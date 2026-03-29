//! This module define a borrowed and owned guards for cancellation safety.
use std::time::Duration;

use tokio::runtime::Handle;

use crate::FencedOutcome;
use crate::IdempotencyEntry;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::entry::Completed;
use crate::fencing_token::FencingToken;

/// A handle for a claim held for the duration of a borrow.
pub struct ClaimGuard<'a, S: IdempotencyStore> {
    store: &'a S,
    key: &'a IdempotencyKey,
    fencing_token: FencingToken,
}

impl<'a, S: IdempotencyStore> ClaimGuard<'a, S> {
    /// Creates a borrowed claim guard.
    pub(crate) const fn new(
        store: &'a S,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Self {
        Self {
            store,
            key,
            fencing_token,
        }
    }
}

impl<S: IdempotencyStore> ClaimGuard<'_, S> {
    /// Returns the fencing token issued for this claim.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Extends the claim's lease by `ttl`, keeping it alive while the handler runs.
    pub async fn touch(&self, ttl: std::time::Duration) -> Result<FencedOutcome, S::Error> {
        self.store.touch(self.key, self.fencing_token, ttl).await
    }

    /// Consumes the guard, recording `entry` as the competed result under `competed_ttl`.
    pub async fn complete(
        self,
        entry: IdempotencyEntry<Completed>,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, S::Error> {
        self.store
            .complete(self.key, entry, self.fencing_token, completed_ttl)
            .await
    }
}

/// Owned handle for the a claim that must outlive the current stack.
pub struct OwnedClaimGuard<S: IdempotencyStore + Clone> {
    store: S,
    key: IdempotencyKey,
    fencing_token: FencingToken,
    handle: Handle,
    completed: bool,
}

impl<S: IdempotencyStore + Clone> OwnedClaimGuard<S> {
    /// Creates an owned claim guard.
    ///
    /// # Panics
    ///
    /// Must be called from within a Tokio runtime.
    pub(crate) fn new(store: S, key: IdempotencyKey, fencing_token: FencingToken) -> Self {
        Self {
            store,
            key,
            fencing_token,
            handle: Handle::current(),
            completed: false,
        }
    }

    /// Returns the fencing token issued for this claim.
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    ///  Extends the claim's lease by `ttl`, keeping it alive while the handler runs.
    pub async fn touch(&self, ttl: Duration) -> Result<FencedOutcome, S::Error> {
        self.store.touch(&self.key, self.fencing_token, ttl).await
    }

    /// Consumes the guard, recording the entry as the completed result under `completed_ttl`.
    pub async fn complete(
        mut self,
        entry: IdempotencyEntry<Completed>,
        completed_ttl: Duration,
    ) -> Result<FencedOutcome, S::Error> {
        self.completed = true;
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
        let (store, key, token) = (self.store.clone(), self.key.clone(), self.fencing_token);

        self.handle.spawn(async move {
            let _ = store.remove(&key, token).await;
        });
    }
}
