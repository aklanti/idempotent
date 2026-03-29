use std::time::Duration;

use super::AnyIdempotencyStore;
use super::InsertResult;
use crate::CachedResponse;
use crate::ClaimGuard;
use crate::FencedOutcome;
use crate::Fingerprint;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::OwnedClaimGuard;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::fencing_token::FencingToken;
use crate::fingerprint::DefaultFingerprintStrategy;
use crate::fingerprint::FingerprintStrategy;

/// The state of the builder without the fingerprint.
pub struct NoFingerprint;

/// The state of the builder with the fingerprint.
pub struct WithFingerprint(Fingerprint);

/// A builder for a claim.
pub struct ClaimBuilder<'store, S: IdempotencyStore + ?Sized, State = NoFingerprint> {
    store: &'store S,
    key: &'store IdempotencyKey,
    processing_ttl: Duration,
    state: State,
}

impl<'store, S: IdempotencyStore + ?Sized> ClaimBuilder<'store, S, NoFingerprint> {
    pub(crate) const fn new(
        store: &'store S,
        key: &'store IdempotencyKey,
        processing_ttl: Duration,
    ) -> Self {
        Self {
            store,
            key,
            processing_ttl,
            state: NoFingerprint,
        }
    }

    /// Fingerprints the request with the default strategy.
    pub fn fingerprint(
        self,
        operation: &str,
        body: &[u8],
    ) -> ClaimBuilder<'store, S, WithFingerprint> {
        self.fingerprint_with(&DefaultFingerprintStrategy, operation, body)
    }

    /// Fingerprints the request with a custom strategy.
    pub fn fingerprint_with(
        self,
        strategy: &dyn FingerprintStrategy,
        operation: &str,
        body: &[u8],
    ) -> ClaimBuilder<'store, S, WithFingerprint> {
        let fingerprint = strategy.compute(operation, body);
        ClaimBuilder {
            store: self.store,
            key: self.key,
            processing_ttl: self.processing_ttl,
            state: WithFingerprint(fingerprint),
        }
    }
}

impl<'store, S: IdempotencyStore + ?Sized> ClaimBuilder<'store, S, WithFingerprint> {
    /// Claims the key, returning a [`ClaimGuard`] on success or the entry that already exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn try_insert(self) -> Result<ClaimOutcome<'store, S>, S::Error> {
        let WithFingerprint(fingerprint) = self.state;
        let entry = IdempotencyEntry::new(fingerprint, self.processing_ttl);
        let outcome = match self.store.try_insert(self.key, entry.clone()).await? {
            InsertResult::Claimed { fencing_token } => {
                ClaimOutcome::Claimed(ClaimGuard::new(self.store, self.key, fencing_token, entry))
            }
            InsertResult::Exists(existing) => ClaimOutcome::Exists {
                existing,
                fingerprint,
            },
        };
        Ok(outcome)
    }

    /// Claims the key and runs the side effect, or replays the cached response on a matching retry.
    ///
    /// On the first request for the key it is claimed, and the side effect runs with the claim's
    /// fencing token which caches the response. A later request with the same fingerprint
    /// replays that response, or returns while the original is still in progress.
    ///
    /// If the store rejects the completion, the response is still considered as executed.
    ///
    /// Dropping the returned future before it completes leaves the claim in place until the
    /// processing TTL expires. For a claim that frees itself when dropped, use
    /// [`claim_owned`](IdempotencyStore::claim_owned).
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails.
    /// When the side effect fails, the claim is left to expire so a later retry re-runs it.
    pub async fn execute_or_replay<F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome, ExecutionError<S::Error>>
    where
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<CachedResponse, Box<dyn std::error::Error + Send + Sync>>>,
    {
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            ClaimOutcome::Claimed(guard) => {
                let response = side_effect(guard.fencing_token())
                    .await
                    .map_err(ExecutionError::SideEffect)?;
                let outcome = guard
                    .complete(response.clone(), completed_ttl)
                    .await
                    .map_err(ExecutionError::Store)?;
                warn_if_rejected(outcome);
                Ok(ExecutionOutcome::Executed(response))
            }
            ClaimOutcome::Exists {
                existing,
                fingerprint,
            } => Ok(replay_outcome(existing, fingerprint)),
        }
    }
}

impl dyn AnyIdempotencyStore {
    /// Creates a builder for a borrowed claim, like [`IdempotencyStore::claim`].
    ///
    /// An inherent method, so it resolves on the trait object itself — including
    /// inside boxed futures where the [`IdempotencyStore`] impl on the `Arc` cannot be named.
    /// Reach it through a deref: `(&*store).claim(…)`.
    pub const fn claim<'store>(
        &'store self,
        key: &'store IdempotencyKey,
        processing_ttl: Duration,
    ) -> ClaimBuilder<'store, Self, NoFingerprint> {
        ClaimBuilder::new(self, key, processing_ttl)
    }
}

/// The outcome of a borrowed claim.
pub enum ClaimOutcome<'store, S: IdempotencyStore + ?Sized> {
    /// The key was claimed.
    Claimed(ClaimGuard<'store, S>),
    /// The key is already taken.
    Exists {
        existing: ExistingEntry,
        fingerprint: Fingerprint,
    },
}
/// Owned [`ClaimOutcome`] returned by `claim_owned`.
pub enum OwnedClaimOutcome<S: IdempotencyStore + Clone> {
    /// The key was claimed.
    Claimed(OwnedClaimGuard<S>),
    /// The key is already taken.
    Exists {
        existing: ExistingEntry,
        fingerprint: Fingerprint,
    },
}

/// Result of [`ClaimBuilder::execute_or_replay`].
#[derive(Debug)]
pub enum ExecutionOutcome {
    /// First time execution of the side effect and its response was cached.
    Executed(CachedResponse),
    /// The cached response was replayed.
    Replayed(CachedResponse),
    /// Another request holds the key mid-flight.
    InFlight,
    /// A different request reused the key.
    FingerprintMismatch,
}

/// Error when executing or replaying the operation.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError<E> {
    /// The store operation failed.
    #[error("store operation failed")]
    Store(#[source] E),
    /// The side effect returned an error.
    #[error("side effect failed")]
    SideEffect(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Maps an existing entry against the request's fingerprint.
fn replay_outcome(existing: ExistingEntry, fingerprint: Fingerprint) -> ExecutionOutcome {
    match existing {
        ExistingEntry::Completed(entry) if entry.fingerprint == fingerprint => {
            ExecutionOutcome::Replayed(entry.into_response())
        }
        ExistingEntry::Processing(entry) if entry.fingerprint == fingerprint => {
            ExecutionOutcome::InFlight
        }
        _ => ExecutionOutcome::FingerprintMismatch,
    }
}

/// Logs a completion the store rejected after the side effect ran.
fn warn_if_rejected(outcome: FencedOutcome) {
    if outcome != FencedOutcome::Applied {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            ?outcome,
            "idempotency completion rejected after the side effect ran"
        );
    }
}
