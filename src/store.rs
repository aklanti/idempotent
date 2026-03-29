//! Idempotency store trait and result types.

use std::pin::Pin;
use std::time::Duration;

use crate::FencedOutcome;
use crate::OwnedClaimGuard;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::key::IdempotencyKey;
mod claim;
#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "valkey")]
pub mod valkey;

use self::claim::ClaimBuilder;
use self::claim::NoFingerprint;
use self::claim::OwnedClaimOutcome;

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

    /// Attempts to claim `key`, returning an owned outcome.
    ///
    /// On success the [`OwnedClaimGuard`] can move across await points and tasks; if it is
    /// dropped before completion it frees the claim so a retry can re-run.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn claim_owned(
        &self,
        key: IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> impl Future<Output = Result<OwnedClaimOutcome<Self>, Self::Error>> + Send
    where
        Self: Clone + Send + Sync + 'static,
    {
        async move {
            let fingerprint = entry.fingerprint;
            let value = match self.try_insert(&key, entry.clone()).await? {
                InsertResult::Claimed { fencing_token } => {
                    let guard = OwnedClaimGuard::new(self.clone(), key, fencing_token, entry);
                    OwnedClaimOutcome::Claimed(guard)
                }
                InsertResult::Exists(existing) => OwnedClaimOutcome::Exists {
                    existing,
                    fingerprint,
                },
            };
            Ok(value)
        }
    }
    /// Marks a claimed key as completed and caches its response.
    ///
    /// The fencing token must match the one returned by [`Self::try_insert`]; the returned
    /// [`FencedOutcome`] reports whether the write applied or was fenced out.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send;

    /// Removes an idempotency entry if the fencing token still owns the claim.
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

/// The boxed error type returned by [`DynIdempotencyStore`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Object-safe, `dyn`-compatible mirror of [`IdempotencyStore`].
pub trait DynIdempotencyStore: Send + Sync + 'static {
    /// Claims `key`.
    fn try_insert<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> Pin<Box<dyn Future<Output = Result<InsertResult, BoxError>> + Send + 'a>>;

    /// Marks a claimed `key` completed with its cached response.
    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
        completed_ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Frees `key` if `fencing_token` still owns the claim.
    fn remove<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Extends the processing lease on `key` by `ttl` while `fencing_token` still matches the
    /// claim.
    fn touch<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FencedOutcome, BoxError>> + Send + 'a>>;

    /// Deletes `key` unconditionally, bypassing the fencing-token check.
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
