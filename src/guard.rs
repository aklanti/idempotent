//! Borrowed and owned claim guards for cancellation-safe completion.
use std::convert::Infallible;
use std::time::Duration;

use tokio::runtime::Handle;

use crate::CachedResponse;
use crate::FencedOutcome;
use crate::IdempotencyEntry;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::InsertResult;
use crate::ReplayOutcome;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::fencing_token::Rejection;

/// A borrowed handle to a claimed key.
pub struct ClaimGuard<'a, S: IdempotencyStore> {
    store: &'a S,
    key: &'a IdempotencyKey,
    fencing_token: FencingToken,
    entry: IdempotencyEntry<Processing>,
}

impl<'a, S: IdempotencyStore> ClaimGuard<'a, S> {
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

impl<S: IdempotencyStore> ClaimGuard<'_, S> {
    /// Borrows the claim this guard holds.
    const fn claim(&self) -> Claim<'_, S> {
        Claim {
            store: self.store,
            key: self.key,
            entry: &self.entry,
            fencing_token: self.fencing_token,
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
        self.claim().touch(ttl).await
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
        self.claim().complete(response, completed_ttl).await
    }

    /// Renews the lease every half lease until the ceiling or a lost lease, then waits forever.
    ///
    /// A store error is logged and the renewal continues. A zero lease is not renewed. Dropping
    /// the future mid touch is safe.
    pub async fn keep_alive(&self, lease: Duration, ceiling: Duration) -> Infallible {
        self.claim().keep_alive(lease, ceiling).await
    }

    /// Caches the response, and claims the key again if this claim was lost.
    ///
    /// # Errors
    ///
    /// Returns an error if a store operation fails.
    pub(crate) async fn cache(
        self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<CacheOutcome, S::Error> {
        self.claim().cache(response, completed_ttl).await
    }
}

/// An owned claim handle that can outlive the current stack frame.
///
/// If it is dropped before [`complete`](Self::complete) or [`leave`](Self::leave) runs, a
/// detached task frees the claim so a retry can re-run the side effect. Dropped during
/// `complete`, it leaves the claim in place, since the store may or may not have applied the
/// write.
///
/// The TTL expiry is the fallback when that task cannot be spawned.
pub struct OwnedClaimGuard<S: IdempotencyStore + Clone> {
    store: S,
    key: IdempotencyKey,
    fencing_token: FencingToken,
    entry: IdempotencyEntry<Processing>,
    handle: Handle,
    recover_on_drop: bool,
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
            recover_on_drop: true,
        }
    }

    /// Returns the fencing token issued for this claim.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Borrows the claim this guard holds.
    const fn claim(&self) -> Claim<'_, S> {
        Claim {
            store: &self.store,
            key: &self.key,
            entry: &self.entry,
            fencing_token: self.fencing_token,
        }
    }

    /// Extends the claim's lease by `ttl`, keeping it alive while the side effect runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn touch(&self, ttl: Duration) -> Result<FencedOutcome, S::Error> {
        self.claim().touch(ttl).await
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
        self.recover_on_drop = false;
        self.claim().complete(response, completed_ttl).await
    }

    /// Consumes the guard and leaves the claim in place until its lease expires.
    ///
    /// Use it when the side effect's outcome is unknown, so a retry cannot re-run it before
    /// the lease ends.
    pub fn leave(mut self) {
        self.recover_on_drop = false;
    }

    /// Renews the lease every half lease until the ceiling or a lost lease, then waits forever.
    ///
    /// A store error is logged and the renewal continues. A zero lease is not renewed. Dropping
    /// the future mid touch is safe.
    pub async fn keep_alive(&self, lease: Duration, ceiling: Duration) -> Infallible {
        self.claim().keep_alive(lease, ceiling).await
    }

    /// Caches the response, and claims the key again if this claim was lost.
    ///
    /// # Errors
    ///
    /// Returns an error if a store operation fails.
    pub(crate) async fn cache(
        mut self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<CacheOutcome, S::Error> {
        self.recover_on_drop = false;
        self.claim().cache(response, completed_ttl).await
    }
}

impl<S: IdempotencyStore + Clone> Drop for OwnedClaimGuard<S> {
    fn drop(&mut self) {
        if !self.recover_on_drop {
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

/// The outcome of caching a response under a claim.
#[derive(Debug)]
pub(crate) enum CacheOutcome {
    /// The response is cached under the key.
    Cached,
    /// The response is not cached, and the store's rejection says why.
    Uncached {
        /// The reason the store rejected the completion.
        rejection: Rejection,
        /// The outcome a retry gets, when another attempt owns the key.
        #[cfg_attr(
            not(feature = "middleware"),
            expect(dead_code, reason = "only the layer acts on what the key holds")
        )]
        replay: Option<ReplayOutcome>,
    },
}

/// The claim a guard holds, with the store and the key it acts on.
///
/// Both guards hold the same four values and differ only in how they own them, so the store
/// calls live here and each guard keeps what ownership adds.
struct Claim<'a, S> {
    store: &'a S,
    key: &'a IdempotencyKey,
    entry: &'a IdempotencyEntry<Processing>,
    fencing_token: FencingToken,
}

impl<S> Clone for Claim<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Claim<'_, S> {}

impl<S: IdempotencyStore> Claim<'_, S> {
    /// Extends the lease by `ttl`.
    async fn touch(self, ttl: Duration) -> Result<FencedOutcome, S::Error> {
        self.store.touch(self.key, self.fencing_token, ttl).await
    }

    /// Caches `response` as the completed result under `completed_ttl`.
    async fn complete(
        self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, S::Error> {
        let entry = self.entry.clone().complete(response, completed_ttl);
        self.store
            .complete(self.key, entry, self.fencing_token)
            .await
    }

    /// Renews the lease every half lease until the ceiling or a lost lease, then waits forever.
    async fn keep_alive(self, lease: Duration, ceiling: Duration) -> Infallible {
        renew(|ttl| self.touch(ttl), self.key, lease, ceiling).await
    }

    /// Caches `response`, and claims the key again if the store rejects the write.
    ///
    /// A rejection means this claim was lost while the side effect ran. Claiming the key again
    /// shows what holds it now. If the key is free, the response goes in under the new claim. If
    /// another attempt has taken it, that attempt keeps it, and the caller finds out what a
    /// retry gets.
    async fn cache(
        self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> Result<CacheOutcome, S::Error> {
        let FencedOutcome::Rejected(rejection) =
            self.complete(response.clone(), completed_ttl).await?
        else {
            return Ok(CacheOutcome::Cached);
        };
        #[cfg(feature = "tracing")]
        tracing::warn!(
            key = %self.key,
            ?rejection,
            "the store rejected the completion after the side effect ran, claiming the key again"
        );

        match self.store.try_insert(self.key, self.entry.clone()).await? {
            InsertResult::Claimed { fencing_token } => {
                let claimed = Self {
                    fencing_token,
                    ..self
                };
                match claimed.complete(response.clone(), completed_ttl).await? {
                    FencedOutcome::Applied => Ok(CacheOutcome::Cached),
                    FencedOutcome::Rejected(rejection) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            key = %self.key,
                            ?rejection,
                            "the new claim was lost before the response was cached"
                        );
                        Ok(CacheOutcome::Uncached {
                            rejection,
                            replay: None,
                        })
                    }
                }
            }
            InsertResult::Exists(existing) => Ok(CacheOutcome::Uncached {
                rejection,
                replay: Some(existing.replay(self.entry.fingerprint)),
            }),
        }
    }
}

/// Renews the lease every half lease until the ceiling or a lost lease, then waits forever.
async fn renew<F, Fut, E>(
    mut touch: F,
    key: &IdempotencyKey,
    lease: Duration,
    ceiling: Duration,
) -> Infallible
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<FencedOutcome, E>>,
    E: std::fmt::Display,
{
    #[cfg(not(feature = "tracing"))]
    let _ = key;
    let interval = lease / 2;
    if interval.is_zero() {
        return std::future::pending().await;
    }
    let renewals = async {
        loop {
            tokio::time::sleep(interval).await;
            match touch(lease).await {
                Ok(FencedOutcome::Applied) => {}
                Ok(FencedOutcome::Rejected(_lost)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(key = %key, rejection = ?_lost, "the processing lease was lost");
                    return;
                }
                Err(_error) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(key = %key, error = %_error, "failed to renew the processing lease");
                }
            }
        }
    };
    if tokio::time::timeout(ceiling, renewals).await.is_err() {
        #[cfg(feature = "tracing")]
        tracing::warn!(key = %key, "keep-alive ceiling reached, the lease will lapse");
    }
    std::future::pending().await
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::time::Duration;

    use crate::IdempotencyEntry;
    use crate::IdempotencyKey;
    use crate::IdempotencyStore;
    use crate::InsertResult;
    use crate::claim::OwnedClaimOutcome;
    use crate::fingerprint::DefaultFingerprintStrategy;
    use crate::fingerprint::FingerprintStrategy;
    use crate::store::memory::MemoryStore;

    const TTL: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn dropped_owned_guard_frees_the_key() {
        let store = MemoryStore::builder()
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("dropped").expect("valid key");
        let fingerprint = DefaultFingerprintStrategy.compute(&"POST /charges".into(), b"{}");

        let outcome = store
            .claim_owned(key.clone(), TTL)
            .fingerprint("POST /charges", b"{}")
            .try_insert()
            .await
            .expect("claim");
        let OwnedClaimOutcome::Claimed(guard) = outcome else {
            panic!("expected a fresh claim");
        };
        drop(guard);

        // Recovery runs on a detached task; give it a bounded chance to land, well inside the TTL.
        for _ in 0..100 {
            let attempt = store
                .try_insert(&key, IdempotencyEntry::new(fingerprint, TTL))
                .await
                .expect("insert");
            match attempt {
                InsertResult::Claimed { .. } => return,
                InsertResult::Exists(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        panic!("the dropped claim was never freed");
    }
}
