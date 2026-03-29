//! Idempotency store trait and result types.

#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "valkey")]
pub mod valkey;

use std::pin::Pin;
use std::time::Duration;

use crate::FencedOutcome;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;

/// Trait for idempotency entry storage backends.
pub trait IdempotencyStore: Send + Sync + 'static {
    /// The error type returned by store operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Attempt to claim an idempotency key
    ///
    /// It returns [`InsertResult::Claimed`] if claimed, or [`InsertResult::Exists`]
    /// if the key already exists
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> impl Future<Output = Result<InsertResult, Self::Error>> + Send;

    /// Marks a claimed key as completed with a cached response.
    ///
    /// The fencing token must match the one returned by [`Self::try_insert`].
    fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Removes an idempotency entry
    fn remove(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Extends the processing lease for an idempotency key by the given `ttl`
    /// if the fencing token still matches the claim.
    fn touch(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Purge key sibling that bypasses the token check.
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

/// Type-erased boxed error.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Object-safe, `dyn`-compatible mirror of [`IdempotencyStore`].
pub trait DynIdempotencyStore: Send + Sync + 'static {
    /// Claims `key`. Erased form of [`IdempotencyStore::try_insert`]
    fn try_insert<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Pin<Box<dyn Future<Output = Result<InsertResult, BoxError>> + Send + 'a>>;

    /// Marks a claimed `key` completed with its cached response. Erased form of
    /// [`IdempotencyStore::complete`].
    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Frees `key` if `fencing_token` still owns the claim. Erased form of
    /// [`IdempotencyStore::remove`]
    fn remove<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Extends the processing lease on `key` by `ttl` while `fencing_token` still matches the claim.
    /// Erased form of [`IdempotencyStore::touch`].
    fn touch<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Deletes `key` unconditionally, bypassing the fencing-token check.
    /// Erased form of [`IdempotencyStore::purge`].
    fn purge<'a>(
        &'a self,
        key: &'a IdempotencyKey,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;
}

impl<S: IdempotencyStore> DynIdempotencyStore for S {
    fn try_insert<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Pin<Box<dyn Future<Output = Result<InsertResult, BoxError>> + Send + 'a>> {
        Box::pin(async move {
            IdempotencyStore::try_insert(self, key, entry)
                .await
                .map_err(Into::into)
        })
    }

    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>> {
        Box::pin(async move {
            IdempotencyStore::complete(self, key, entry, fencing_token, completed_ttl)
                .await
                .map_err(Into::into)
        })
    }

    fn remove<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>> {
        Box::pin(async move {
            IdempotencyStore::remove(self, key, fencing_token)
                .await
                .map_err(Into::into)
        })
    }

    fn touch<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>> {
        Box::pin(async move {
            IdempotencyStore::touch(self, key, fencing_token, ttl)
                .await
                .map_err(Into::into)
        })
    }

    fn purge<'a>(
        &'a self,
        key: &'a IdempotencyKey,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>> {
        Box::pin(async move { IdempotencyStore::purge(self, key).await.map_err(Into::into) })
    }
}
