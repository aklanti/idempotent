//! Idempotency keys store

use crate::entry::{Completed, FencingToken, IdempotencyEntry, Processing};
use crate::key::IdempotencyKey;

/// A store trait
#[async_trait::async_trait]
pub trait IdempotencyStore: Send + Sync + 'static {
    /// The error when interaction with an idempotency store
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
    /// The fencing token must match the one returns by [`Self::try_insert`]
    async fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> Result<(), Self::Error>;

    /// Removes an idempotency entry
    async fn remove(&mut self, key: &IdempotencyKey) -> Result<(), Self::Error>;
}

/// A result when attempting to claim an idempotency key
#[derive(Debug, Clone)]
pub enum InsertResult {
    /// A key is successfully claimed
    ///
    /// The key was absent or expired and the caller owns the claim and should
    /// execute the handler
    Claimed {
        /// A fencing token to prevent zombie completion
        fencing_token: u64,
    },
    /// A key already exists
    Exists,
}
