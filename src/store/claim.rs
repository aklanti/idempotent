//! The claim builder and the outcomes of claiming a key.

use std::time::Duration;

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
    /// If the store rejects the completion, the outcome is [`Fenced`](ExecutionOutcome::Fenced).
    /// The side effect has run but its response was not cached.
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
        #[cfg(feature = "tracing")]
        let key = self.key;
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            ClaimOutcome::Claimed(guard) => {
                let response = side_effect(guard.fencing_token())
                    .await
                    .map_err(ExecutionError::SideEffect)?;
                let verdict = guard
                    .complete(response.clone(), completed_ttl)
                    .await
                    .map_err(ExecutionError::Store)?;
                let outcome = ExecutionOutcome::from_completion(verdict, response);
                #[cfg(feature = "tracing")]
                if let ExecutionOutcome::Fenced { rejection, .. } = &outcome {
                    tracing::warn!(
                        key = %key,
                        ?rejection,
                        "completion rejected after the side effect ran"
                    );
                }
                Ok(outcome)
            }
            ClaimOutcome::Exists {
                existing,
                fingerprint,
            } => Ok(replay_outcome(existing, fingerprint)),
        }
    }
}

/// The outcome of a borrowed claim.
pub enum ClaimOutcome<'store, S: IdempotencyStore> {
    /// The key was claimed.
    Claimed(ClaimGuard<'store, S>),
    /// The key is already taken.
    Exists {
        /// The entry that holds the key.
        existing: ExistingEntry,
        /// This request's fingerprint, to compare with the entry's.
        fingerprint: Fingerprint,
    },
}
/// Owned [`ClaimOutcome`] returned by `claim_owned`.
pub enum OwnedClaimOutcome<S: IdempotencyStore + Clone> {
    /// The key was claimed.
    Claimed(OwnedClaimGuard<S>),
    /// The key is already taken.
    Exists {
        /// The entry that holds the key.
        existing: ExistingEntry,
        /// This request's fingerprint, to compare with the entry's.
        fingerprint: Fingerprint,
    },
}

/// Result of [`ClaimBuilder::execute_or_replay`].
#[derive(Debug)]
pub enum ExecutionOutcome {
    /// First time execution of the side effect and its response was cached.
    Executed(CachedResponse),
    /// The side effect ran, but the store rejected the completion and did not cache the response.
    Fenced {
        /// The reason the store rejected the completion.
        rejection: FencedOutcome,
        /// The response the side effect produced.
        response: CachedResponse,
    },
    /// The cached response was replayed.
    Replayed(CachedResponse),
    /// Another request holds the key mid-flight.
    InFlight,
    /// A different request reused the key.
    FingerprintMismatch,
}

impl ExecutionOutcome {
    /// Reads the store's verdict on a completion, keeping the response the side effect produced.
    const fn from_completion(verdict: FencedOutcome, response: CachedResponse) -> Self {
        match verdict {
            FencedOutcome::Applied => Self::Executed(response),
            rejection @ (FencedOutcome::FencingMismatch
            | FencedOutcome::KeyExpired
            | FencedOutcome::FingerprintMismatch) => Self::Fenced {
                rejection,
                response,
            },
        }
    }
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

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::time::Duration;

    use super::ClaimOutcome;
    use super::ExecutionOutcome;
    use crate::CachedResponse;
    use crate::ClaimGuard;
    use crate::FencedOutcome;
    use crate::IdempotencyKey;
    use crate::IdempotencyStore;
    use crate::Metadata;
    use crate::store::memory::MemoryStore;

    const OPERATION: &str = "POST /charges";
    const PROCESSING_TTL: Duration = Duration::from_secs(30);
    const COMPLETED_TTL: Duration = Duration::from_secs(60);

    fn memory_store() -> MemoryStore {
        MemoryStore::builder()
            .buffer(16)
            .sweep_interval(Duration::from_secs(60))
            .try_build()
            .expect("build memory store")
    }

    fn created(body: &'static [u8]) -> CachedResponse {
        CachedResponse {
            status_code: 201,
            metadata: Metadata::new(),
            body: body.into(),
        }
    }

    #[tokio::test]
    async fn execute_or_replay_reports_fenced_after_lease_expiry() {
        let store = memory_store();
        let key = IdempotencyKey::new("expired").expect("valid key");

        let first = store
            .claim(&key, Duration::ZERO)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move { Ok(created(b"first")) })
            .await
            .expect("execute");
        let ExecutionOutcome::Fenced {
            rejection,
            response,
        } = first
        else {
            panic!("expected the completion after lease expiry to be fenced");
        };
        assert_eq!(rejection, FencedOutcome::KeyExpired);
        assert_eq!(response, created(b"first"));

        let second = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(
                COMPLETED_TTL,
                |_token| async move { Ok(created(b"second")) },
            )
            .await
            .expect("execute again");
        let ExecutionOutcome::Executed(response) = second else {
            panic!("expected the retry to re-run the side effect");
        };
        assert_eq!(response, created(b"second"));
    }

    #[tokio::test]
    async fn execute_or_replay_reports_fenced_after_reclaim() {
        let store = memory_store();
        let key = IdempotencyKey::new("reclaimed").expect("valid key");
        let mut reclaimed: Option<ClaimGuard<'_, MemoryStore>> = None;

        let first = store
            .claim(&key, Duration::ZERO)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| {
                let store = &store;
                let key = &key;
                let slot = &mut reclaimed;
                async move {
                    let outcome = store
                        .claim(key, PROCESSING_TTL)
                        .fingerprint(OPERATION, b"{}")
                        .try_insert()
                        .await?;
                    let ClaimOutcome::Claimed(guard) = outcome else {
                        return Err("expected the expired key to be reclaimed".into());
                    };
                    *slot = Some(guard);
                    Ok(created(b"first"))
                }
            })
            .await
            .expect("execute");
        let ExecutionOutcome::Fenced {
            rejection,
            response,
        } = first
        else {
            panic!("expected the completion after a reclaim to be fenced");
        };
        assert_eq!(rejection, FencedOutcome::FencingMismatch);
        assert_eq!(response, created(b"first"));

        let guard = reclaimed
            .take()
            .expect("the reclaiming attempt holds the key");
        let applied = guard
            .complete(created(b"second"), COMPLETED_TTL)
            .await
            .expect("complete");
        assert_eq!(applied, FencedOutcome::Applied);

        let third = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err("the side effect must not re-run on a replay".into())
            })
            .await
            .expect("replay");
        let ExecutionOutcome::Replayed(cached) = third else {
            panic!("expected the winner's response to replay");
        };
        assert_eq!(cached, created(b"second"));
        assert_ne!(cached, created(b"first"));
    }

    #[tokio::test]
    async fn execute_or_replay_reports_in_flight() {
        let store = memory_store();
        let key = IdempotencyKey::new("in-flight").expect("valid key");
        let held = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .try_insert()
            .await
            .expect("claim");
        let ClaimOutcome::Claimed(_guard) = held else {
            panic!("expected a fresh claim");
        };

        let outcome = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err("the side effect must not run while the key is held".into())
            })
            .await
            .expect("execute");
        assert!(matches!(outcome, ExecutionOutcome::InFlight));
    }

    #[tokio::test]
    async fn execute_or_replay_rejects_foreign_body() {
        let store = memory_store();
        let key = IdempotencyKey::new("reused").expect("valid key");

        let first = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{\"amount\": 1}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move { Ok(created(b"first")) })
            .await
            .expect("execute");
        assert!(matches!(first, ExecutionOutcome::Executed(_)));

        let second = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{\"amount\": 2}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err("the side effect must not run for a different request".into())
            })
            .await
            .expect("execute");
        assert!(matches!(second, ExecutionOutcome::FingerprintMismatch));
    }
}
