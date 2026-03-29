use std::time::Duration;

use super::InsertResult;
use crate::ClaimGuard;
use crate::Fingerprint;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::OwnedClaimGuard;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::fingerprint::DefaultFingerprintStrategy;
use crate::fingerprint::FingerprintStrategy;

/// The state of the builder without the fingerprint.
pub struct NoFingerprint;

/// The state of the builder with the fingerprint.
pub struct WithFingerprint(Fingerprint);

/// A builder for a claim.
pub struct ClaimBuilder<'store, S: IdempotencyStore, State = NoFingerprint> {
    store: &'store S,
    key: &'store IdempotencyKey,
    processing_ttl: Duration,
    state: State,
}

impl<'store, S: IdempotencyStore> ClaimBuilder<'store, S, NoFingerprint> {
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

impl<'store, S: IdempotencyStore> ClaimBuilder<'store, S, WithFingerprint> {
    /// Claims the key returning a [`ClaimGuard`].
    pub async fn try_insert(self) -> Result<ClaimOutcome<'store, S>, S::Error> {
        let WithFingerprint(fingerprint) = self.state;
        let entry = IdempotencyEntry::new(fingerprint, self.processing_ttl);
        let outcome = match self.store.try_insert(self.key, entry).await? {
            InsertResult::Claimed { fencing_token } => {
                ClaimOutcome::Claimed(ClaimGuard::new(self.store, self.key, fencing_token))
            }
            InsertResult::Exists(existing) => ClaimOutcome::Exists {
                existing,
                fingerprint,
            },
        };
        Ok(outcome)
    }
}

/// Borrowed outcome.
pub enum ClaimOutcome<'store, S: IdempotencyStore> {
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
