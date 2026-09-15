//! Idempotency store trait and result types.

use std::time::Duration;

use crate::FencedOutcome;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;
#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "valkey")]
pub mod valkey;

use crate::claim::ClaimBuilder;
use crate::claim::NoFingerprint;
use crate::claim::OwnedClaimBuilder;

/// Trait for idempotency entry storage backends.
pub trait IdempotencyStore: Send + Sync + 'static {
    /// The error type returned by store operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Attempts to claim an idempotency key.
    ///
    /// Returns [`InsertResult::Claimed`] when the key was free, or [`InsertResult::Exists`]
    /// with the existing entry when it was already taken.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> impl Future<Output = Result<InsertResult, Self::Error>> + Send;

    /// Creates a builder for a borrowed claim.
    fn claim<'store>(
        &'store self,
        key: &'store IdempotencyKey,
        processing_ttl: Duration,
    ) -> ClaimBuilder<'store, Self, NoFingerprint>
    where
        Self: Sized,
    {
        ClaimBuilder::new(self, key, processing_ttl)
    }

    /// Creates a builder for an owned claim.
    ///
    /// The builder and the futures it returns own a clone of the store and the key, so they
    /// can move across tasks and runtimes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use idempotent::{IdempotencyKey, IdempotencyStore, OwnedClaimOutcome};
    /// # use idempotent::memory::MemoryStore;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = MemoryStore::builder().try_build()?;
    /// let key = IdempotencyKey::new("offer-8f21")?;
    ///
    /// let outcome = store
    ///     .claim_owned(key, Duration::from_secs(30))
    ///     .fingerprint("POST /credentials/issue", b"{}")
    ///     .try_insert()
    ///     .await?;
    /// let OwnedClaimOutcome::Claimed(guard) = outcome else {
    ///     return Ok(());
    /// };
    /// // Drop the guard to free the key, or leave it to the lease.
    /// guard.leave();
    /// # Ok(())
    /// # }
    /// ```
    fn claim_owned(
        &self,
        key: IdempotencyKey,
        processing_ttl: Duration,
    ) -> OwnedClaimBuilder<Self, NoFingerprint>
    where
        Self: Sized + Clone,
    {
        OwnedClaimBuilder::new(self.clone(), key, processing_ttl)
    }

    /// Marks a claimed key as completed and caches its response.
    ///
    /// The entry's `ttl` is the completed lease. The fencing token must match the one returned
    /// by [`Self::try_insert`].
    ///
    /// The returned [`FencedOutcome`] reports whether the write applied or was rejected, and
    /// every store reports it the same way. If another attempt replaced the token, the rejection
    /// is a fencing mismatch, whether that attempt is still running or has already finished. If
    /// no live claim holds the key, or this claim already completed, the key has expired. If the
    /// entry's fingerprint differs from the claimed one, the rejection is a fingerprint mismatch.
    /// A rejected completion writes nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Removes an idempotency entry if the fencing token still owns the claim.
    ///
    /// A matching token removes the entry, a completed one included, so a claim can drop the
    /// response it cached. A token another attempt replaced is a fencing mismatch, and a key
    /// with no entry has expired.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn remove(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Extends the processing lease on a key by `ttl` while the fencing token matches the claim.
    ///
    /// It reports the same outcomes as [`complete`](Self::complete). If another attempt took the
    /// key, the outcome is a fencing mismatch. If the key has already completed, it has expired.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn touch(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Removes a key unconditionally, bypassing the fencing-token check.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn purge(&self, key: &IdempotencyKey) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The result of [`IdempotencyStore::try_insert`].
#[derive(Debug, Clone)]
pub enum InsertResult {
    /// A key is successfully claimed.
    ///
    /// The key was absent or expired and the caller owns the claim and should
    /// execute the handler.
    Claimed {
        /// A fencing token to prevent zombie completion.
        fencing_token: FencingToken,
    },
    /// A key already exists
    Exists(ExistingEntry),
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::time::Duration;

    use crate::CachedResponse;
    use crate::IdempotencyKey;
    use crate::Metadata;
    use crate::claim::ExecutionOutcome;
    use crate::store::IdempotencyStore;
    use crate::store::memory::MemoryStore;

    #[tokio::test]
    async fn executes_then_replays() {
        let store = MemoryStore::builder()
            .buffer(16)
            .sweep_interval(Duration::from_secs(60))
            .try_build()
            .expect("build memory store");

        let key = IdempotencyKey::new("achebe").expect("valid key");
        let response = CachedResponse {
            status_code: 201,
            metadata: Metadata::new(),
            body: b"ok".to_vec().into(),
        };

        let first = store
            .claim(&key, Duration::from_secs(30))
            .fingerprint("POST /charges", b"{}")
            .execute_or_replay(Duration::from_secs(60), |_token| {
                let response = response.clone();
                async move { Ok(response) }
            })
            .await
            .expect("execute");
        assert!(matches!(first, ExecutionOutcome::Executed(_)));

        let second: ExecutionOutcome = store
            .claim(&key, Duration::from_secs(30))
            .fingerprint("POST /charges", b"{}")
            .execute_or_replay(Duration::from_secs(60), |_token| async move {
                Err("the side effect must not re-run on a replay".into())
            })
            .await
            .expect("replay");
        let ExecutionOutcome::Replayed(cached) = second else {
            panic!("expected the cached response to replay");
        };
        assert_eq!(cached.status_code, 201);
    }
}
