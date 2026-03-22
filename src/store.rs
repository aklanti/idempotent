//! Idempotency store trait and result types.

#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "valkey")]
pub mod valkey;

use crate::entry::{Completed, ExistingEntry, FencingToken, IdempotencyEntry, Processing};
use crate::key::IdempotencyKey;

/// Trait for idempotency entry storage backends.
#[async_trait::async_trait]
pub trait IdempotencyStore: Send + Sync + 'static {
    /// The error type returned by store operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Attempt to claim an idempotency key
    ///
    /// It returns [`InsertResult::Claimed`] if claimed, or [`InsertResult::Exists`]
    /// if the key already exists
    async fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Result<InsertResult, Self::Error>;

    /// Marks a claimed key as completed with a cached response.
    ///
    /// The fencing token must match the one returned by [`Self::try_insert`].
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<(), Self::Error>;

    /// Removes an idempotency entry
    async fn remove(&self, key: &IdempotencyKey) -> Result<(), Self::Error>;
}

/// The result of [`IdempotencyStore::try_insert`].
#[derive(Debug, Clone)]
pub enum InsertResult {
    /// A key is successfully claimed
    ///
    /// The key was absent or expired and the caller owns the claim and should
    /// execute the handler
    Claimed {
        /// A fencing token to prevent zombie completion
        fencing_token: FencingToken,
    },
    /// A key already exists
    Exists(ExistingEntry),
}
