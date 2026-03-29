//! Borrowed and owned claim guards for cancellation-safe completion.
use std::time::Duration;

use tokio::runtime::Handle;

use crate::CachedResponse;
use crate::FencedOutcome;
use crate::IdempotencyEntry;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;

/// A borrowed handle to a claimed key.
pub struct ClaimGuard<'a, S: IdempotencyStore + ?Sized> {
    store: &'a S,
    key: &'a IdempotencyKey,
    fencing_token: FencingToken,
    entry: IdempotencyEntry<Processing>,
}

impl<'a, S: IdempotencyStore + ?Sized> ClaimGuard<'a, S> {
    /// Creates a borrowed claim guard owning the claimed entry.
    pub(crate) const fn new(
        store: &'a S,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
        entry: IdempotencyEntry<Processing>,
    ) -> Self {
        Self {
            store,
            key,
            fencing_token,
            entry,
        }
    }
}

impl<S: IdempotencyStore + ?Sized> ClaimGuard<'_, S> {
    /// Returns the fencing token issued for this claim.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Extends the claim's lease by `ttl`, keeping it alive while the side effect runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn touch(&self, ttl: Duration) -> Result<FencedOutcome, S::Error> {
        self.store.touch(self.key, self.fencing_token, ttl).await
    }

    /// Consumes the guard and caches the response as the completed result under `completed_ttl`.
    ///
    /// The completed entry inherits the claim's fingerprint, so the cached response is bound
    /// to the request that was claimed.
    ///
    /// The returned [`FencedOutcome`] reports whether the write applied or was rejected by the
    /// fencing token.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn complete(
        self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, S::Error> {
        let entry = self.entry.complete(response);
        self.store
            .complete(self.key, entry, self.fencing_token, completed_ttl)
            .await
    }
}

/// An owned claim handle that can outlive the current stack frame.
///
/// If it is dropped before [`complete`](Self::complete) runs, a detached task frees the
/// claim so a retry can rerun the side effect.
///
/// The TTL expiry is the fallback when that task cannot be spawned.
pub struct OwnedClaimGuard<S: IdempotencyStore + Clone> {
    store: S,
    key: IdempotencyKey,
    fencing_token: FencingToken,
    entry: IdempotencyEntry<Processing>,
    handle: Handle,
    completed: bool,
}

impl<S: IdempotencyStore + Clone> OwnedClaimGuard<S> {
    /// Creates an owned claim guard.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub(crate) fn new(
        store: S,
        key: IdempotencyKey,
        fencing_token: FencingToken,
        entry: IdempotencyEntry<Processing>,
    ) -> Self {
        Self {
            store,
            key,
            fencing_token,
            entry,
            handle: Handle::current(),
            completed: false,
        }
    }

    /// Returns the fencing token issued for this claim.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Extends the claim's lease by `ttl`, keeping it alive while the side effect runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn touch(&self, ttl: Duration) -> Result<FencedOutcome, S::Error> {
        self.store.touch(&self.key, self.fencing_token, ttl).await
    }

    /// Consumes the guard and caches the response as the completed result under `completed_ttl`.
    ///
    /// The completed entry inherits the claim's fingerprint, so the cached response is bound
    /// to the request that was claimed. The returned [`FencedOutcome`] reports whether the
    /// write applied or was rejected by the fencing token.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn complete(
        mut self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, S::Error> {
        self.completed = true;
        let entry = self.entry.clone().complete(response);
        self.store
            .complete(&self.key, entry, self.fencing_token, completed_ttl)
            .await
    }
}

impl<S: IdempotencyStore + Clone> Drop for OwnedClaimGuard<S> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let store = self.store.clone();
        let key = self.key.clone();
        let token = self.fencing_token;

        #[cfg(feature = "tracing")]
        tracing::warn!(key = %key, "claim dropped without completion, recovering");
        #[cfg(feature = "tracing")]
        let recovery_span = tracing::debug_span!("claim_recovery", key = %key);
        // Correlate the detached recovery back to the span that dropped the guard.
        #[cfg(feature = "tracing")]
        recovery_span.follows_from(tracing::Span::current());

        let recovery = async move {
            let result = store.remove(&key, token).await;
            #[cfg(feature = "tracing")]
            match &result {
                Ok(outcome) => tracing::debug!(?outcome, "recovered dropped claim"),
                Err(error) => tracing::warn!(%error, "failed to recover dropped claim"),
            }
            #[cfg(not(feature = "tracing"))]
            let _ = result;
        };

        #[cfg(feature = "tracing")]
        let recovery = tracing::Instrument::instrument(recovery, recovery_span);

        self.handle.spawn(recovery);
    }
}
