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
use crate::entry::ReplayOutcome;
use crate::fencing_token::FencingToken;
use crate::fingerprint::DefaultFingerprintStrategy;
use crate::fingerprint::FingerprintStrategy;
use crate::fingerprint::Operation;

/// The state of the builder without the fingerprint.
pub struct NoFingerprint;

/// The state of the builder with the fingerprint.
pub struct WithFingerprint(Fingerprint);

/// A builder for a claim.
pub struct ClaimBuilder<'store, S: IdempotencyStore, State = NoFingerprint> {
    store: &'store S,
    key: &'store IdempotencyKey,
    processing_ttl: Duration,
    keep_alive: Option<Duration>,
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
            keep_alive: None,
            state: NoFingerprint,
        }
    }

    /// Fingerprints the request with the default strategy.
    pub fn fingerprint(
        self,
        operation: impl Into<Operation>,
        body: &[u8],
    ) -> ClaimBuilder<'store, S, WithFingerprint> {
        self.fingerprint_with(&DefaultFingerprintStrategy, operation, body)
    }

    /// Fingerprints the request with a custom strategy.
    pub fn fingerprint_with(
        self,
        strategy: &dyn FingerprintStrategy,
        operation: impl Into<Operation>,
        body: &[u8],
    ) -> ClaimBuilder<'store, S, WithFingerprint> {
        let fingerprint = strategy.compute(&operation.into(), body);
        ClaimBuilder {
            store: self.store,
            key: self.key,
            processing_ttl: self.processing_ttl,
            keep_alive: self.keep_alive,
            state: WithFingerprint(fingerprint),
        }
    }
}

impl<'store, S: IdempotencyStore, State> ClaimBuilder<'store, S, State> {
    /// Renews the processing lease while the side effect runs, for at most `ceiling`.
    ///
    /// After the ceiling the lease lapses, and a completion that comes later is
    /// [`Fenced`](ExecutionOutcome::Fenced).
    pub const fn keep_alive(mut self, ceiling: Duration) -> Self {
        self.keep_alive = Some(ceiling);
        self
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
    /// replays that response, or returns while the original is still in progress. With
    /// [`keep_alive`](Self::keep_alive) set, the processing lease is renewed while the side
    /// effect runs.
    ///
    /// If the store rejects the completion, the outcome is [`Fenced`](ExecutionOutcome::Fenced).
    /// The side effect has run but its response was not cached.
    ///
    /// Dropping the returned future before it completes leaves the claim in place until the
    /// processing TTL expires. For a future that frees the claim when dropped, use
    /// [`claim_owned`](IdempotencyStore::claim_owned); a failed side effect leaves the claim
    /// to expire on both paths.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails.
    /// When the side effect fails, the claim is left to expire so a later retry re-runs it.
    /// When the completion fails, [`Completion`](ExecutionError::Completion) contains the
    /// response and the claim is left to expire.
    pub async fn execute_or_replay<F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome, ExecutionError<S::Error>>
    where
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<CachedResponse, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let keep_alive = self.keep_alive;
        let processing_ttl = self.processing_ttl;
        #[cfg(feature = "tracing")]
        let key = self.key;
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            ClaimOutcome::Claimed(guard) => {
                let response = run_with_renewal(
                    side_effect(guard.fencing_token()),
                    keep_alive.map(|ceiling| guard.keep_alive(processing_ttl, ceiling)),
                )
                .await
                .map_err(ExecutionError::SideEffect)?;
                let verdict = match guard.complete(response.clone(), completed_ttl).await {
                    Ok(verdict) => verdict,
                    Err(source) => return Err(ExecutionError::Completion { source, response }),
                };
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
            } => Ok(existing.replay(fingerprint).into()),
        }
    }
}

/// A builder for an owned claim.
///
/// # Examples
///
/// ```
/// # use std::time::Duration;
/// # use idempotent::{CachedResponse, ExecutionOutcome, IdempotencyKey, IdempotencyStore, Metadata};
/// # use idempotent::memory::MemoryStore;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let store = MemoryStore::builder().try_build()?;
/// let key = IdempotencyKey::new("offer-8f21")?;
///
/// // The future owns its store and key, so it can run on another task.
/// let outcome = tokio::spawn(
///     store
///         .claim_owned(key, Duration::from_secs(30))
///         .fingerprint("POST /credentials/issue", b"{}")
///         .execute_or_replay(Duration::from_secs(60), |_token| async {
///             Ok(CachedResponse::new(201, Metadata::new(), b"issued".to_vec().into()))
///         }),
/// )
/// .await??;
/// assert!(matches!(outcome, ExecutionOutcome::Executed(_)));
/// # Ok(())
/// # }
/// ```
pub struct OwnedClaimBuilder<S: IdempotencyStore + Clone, State = NoFingerprint> {
    store: S,
    key: IdempotencyKey,
    processing_ttl: Duration,
    keep_alive: Option<Duration>,
    state: State,
}

impl<S: IdempotencyStore + Clone> OwnedClaimBuilder<S, NoFingerprint> {
    pub(crate) const fn new(store: S, key: IdempotencyKey, processing_ttl: Duration) -> Self {
        Self {
            store,
            key,
            processing_ttl,
            keep_alive: None,
            state: NoFingerprint,
        }
    }

    /// Fingerprints the request with the default strategy.
    pub fn fingerprint(
        self,
        operation: impl Into<Operation>,
        body: &[u8],
    ) -> OwnedClaimBuilder<S, WithFingerprint> {
        self.fingerprint_with(&DefaultFingerprintStrategy, operation, body)
    }

    /// Fingerprints the request with a custom strategy.
    pub fn fingerprint_with(
        self,
        strategy: &dyn FingerprintStrategy,
        operation: impl Into<Operation>,
        body: &[u8],
    ) -> OwnedClaimBuilder<S, WithFingerprint> {
        let fingerprint = strategy.compute(&operation.into(), body);
        OwnedClaimBuilder {
            store: self.store,
            key: self.key,
            processing_ttl: self.processing_ttl,
            keep_alive: self.keep_alive,
            state: WithFingerprint(fingerprint),
        }
    }
}

impl<S: IdempotencyStore + Clone, State> OwnedClaimBuilder<S, State> {
    /// Renews the processing lease while the side effect runs, for at most `ceiling`.
    ///
    /// After the ceiling the lease lapses, and a completion that comes later is
    /// [`Fenced`](ExecutionOutcome::Fenced).
    pub const fn keep_alive(mut self, ceiling: Duration) -> Self {
        self.keep_alive = Some(ceiling);
        self
    }
}

impl<S: IdempotencyStore + Clone> OwnedClaimBuilder<S, WithFingerprint> {
    /// Claims the key, returning an [`OwnedClaimGuard`] on success or the entry that already
    /// exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    ///
    /// # Panics
    ///
    /// Panics if polled outside a Tokio runtime.
    pub async fn try_insert(self) -> Result<OwnedClaimOutcome<S>, S::Error> {
        let WithFingerprint(fingerprint) = self.state;
        let entry = IdempotencyEntry::new(fingerprint, self.processing_ttl);
        let outcome = match self.store.try_insert(&self.key, entry.clone()).await? {
            InsertResult::Claimed { fencing_token } => OwnedClaimOutcome::Claimed(
                OwnedClaimGuard::new(self.store, self.key, fencing_token, entry),
            ),
            InsertResult::Exists(existing) => OwnedClaimOutcome::Exists {
                existing,
                fingerprint,
            },
        };
        Ok(outcome)
    }

    /// Claims the key and runs the side effect, or replays the cached response on a matching
    /// retry.
    ///
    /// Behaves as [`ClaimBuilder::execute_or_replay`] with the difference that the returned future
    /// owns its store and key, so it can move across tasks and runtimes, and dropping it while
    /// the side effect runs frees the claim at once.
    ///
    /// When the side effect fails, the claim is left to expire, as on the borrowing path.
    /// With [`keep_alive`](Self::keep_alive) set, the processing lease is renewed while the side
    /// effect runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails. When the
    /// completion fails, [`Completion`](ExecutionError::Completion) contains the response and
    /// the claim is left to expire.
    ///
    /// # Panics
    ///
    /// Panics if polled outside a Tokio runtime.
    pub async fn execute_or_replay<F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome, ExecutionError<S::Error>>
    where
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<CachedResponse, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let keep_alive = self.keep_alive;
        let processing_ttl = self.processing_ttl;
        #[cfg(feature = "tracing")]
        let key = self.key.clone();
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            OwnedClaimOutcome::Claimed(guard) => {
                let response = match run_with_renewal(
                    side_effect(guard.fencing_token()),
                    keep_alive.map(|ceiling| guard.keep_alive(processing_ttl, ceiling)),
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        guard.leave();
                        return Err(ExecutionError::SideEffect(error));
                    }
                };
                let verdict = match guard.complete(response.clone(), completed_ttl).await {
                    Ok(verdict) => verdict,
                    Err(source) => return Err(ExecutionError::Completion { source, response }),
                };
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
            OwnedClaimOutcome::Exists {
                existing,
                fingerprint,
            } => Ok(existing.replay(fingerprint).into()),
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
/// Owned [`ClaimOutcome`] returned by [`OwnedClaimBuilder::try_insert`].
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

/// Result of [`ClaimBuilder::execute_or_replay`] and [`OwnedClaimBuilder::execute_or_replay`].
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
    /// The store failed before the side effect ran.
    #[error("store operation failed")]
    Store(#[source] E),
    /// The side effect returned an error.
    #[error("side effect failed")]
    SideEffect(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The store failed after the side effect ran, and nothing was cached.
    #[error("completion failed after the side effect ran")]
    Completion {
        /// The store error.
        #[source]
        source: E,
        /// The response the side effect produced.
        response: CachedResponse,
    },
}

/// Runs the side effect, racing it against the lease renewal when one is set.
async fn run_with_renewal<T, Work, Renewal>(work: Work, renewal: Option<Renewal>) -> T
where
    Work: Future<Output = T>,
    Renewal: Future<Output = std::convert::Infallible>,
{
    match renewal {
        Some(renewal) => tokio::select! {
            output = work => output,
            never = renewal => match never {},
        },
        None => work.await,
    }
}

impl From<ReplayOutcome> for ExecutionOutcome {
    fn from(outcome: ReplayOutcome) -> Self {
        match outcome {
            ReplayOutcome::Replayed(response) => Self::Replayed(response),
            ReplayOutcome::InFlight => Self::InFlight,
            ReplayOutcome::FingerprintMismatch => Self::FingerprintMismatch,
        }
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::time::Duration;

    use super::ClaimOutcome;
    use super::ExecutionError;
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

    #[tokio::test]
    async fn owned_builder_executes_then_replays() {
        let store = memory_store();
        let key = IdempotencyKey::new("owned").expect("valid key");

        let first = store
            .claim_owned(key.clone(), PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move { Ok(created(b"first")) })
            .await
            .expect("execute");
        assert!(matches!(first, ExecutionOutcome::Executed(_)));

        let second = store
            .claim_owned(key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err("the side effect must not re-run on a replay".into())
            })
            .await
            .expect("replay");
        let ExecutionOutcome::Replayed(cached) = second else {
            panic!("expected the cached response to replay");
        };
        assert_eq!(cached, created(b"first"));
    }

    #[tokio::test]
    async fn owned_side_effect_error_leaves_the_claim() {
        let store = memory_store();
        let key = IdempotencyKey::new("failed").expect("valid key");

        let failed = store
            .claim_owned(key.clone(), PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move { Err("boom".into()) })
            .await;
        assert!(matches!(failed, Err(ExecutionError::SideEffect(_))));

        let held = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .try_insert()
            .await
            .expect("claim");
        assert!(matches!(held, ClaimOutcome::Exists { .. }));
    }

    #[tokio::test]
    async fn owned_futures_are_send_and_static() {
        fn assert_send_static<T: Send + 'static>(_: T) {}
        let store = memory_store();
        let key = IdempotencyKey::new("moved").expect("valid key");
        assert_send_static(
            store
                .claim_owned(key.clone(), PROCESSING_TTL)
                .fingerprint(OPERATION, b"{}")
                .try_insert(),
        );
        assert_send_static(
            store
                .claim_owned(key, PROCESSING_TTL)
                .fingerprint(OPERATION, b"{}")
                .execute_or_replay(COMPLETED_TTL, |_token| async move { Ok(created(b"moved")) }),
        );
    }

    #[tokio::test]
    async fn completion_failure_returns_the_response() {
        // The store's task lives on its own runtime, which the side effect shuts down, so the
        // completion finds the task gone.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let store = MemoryStore::builder()
            .runtime(runtime.handle().clone())
            .try_build()
            .expect("build memory store");
        let key = IdempotencyKey::new("lost-store").expect("valid key");

        let failed = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| {
                let store = &store;
                async move {
                    runtime.shutdown_background();
                    while store.is_healthy() {
                        tokio::task::yield_now().await;
                    }
                    Ok(created(b"lost"))
                }
            })
            .await;

        let Err(ExecutionError::Completion { response, .. }) = failed else {
            panic!("expected the completion to fail");
        };
        assert_eq!(response, created(b"lost"));
    }

    #[tokio::test(start_paused = true)]
    async fn execute_or_replay_renews_the_lease_while_the_side_effect_runs() {
        let store = memory_store();
        let key = IdempotencyKey::new("slow").expect("valid key");

        let first = store
            .claim(&key, Duration::from_secs(1))
            .keep_alive(Duration::from_secs(60))
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(created(b"slow"))
            })
            .await
            .expect("execute");
        assert!(matches!(first, ExecutionOutcome::Executed(_)));

        let second = store
            .claim(&key, Duration::from_secs(1))
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err("the side effect must not re-run on a replay".into())
            })
            .await
            .expect("replay");
        assert!(matches!(second, ExecutionOutcome::Replayed(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_stops_at_the_ceiling() {
        let store = memory_store();
        let key = IdempotencyKey::new("too-slow").expect("valid key");

        let outcome = store
            .claim(&key, Duration::from_secs(1))
            .keep_alive(Duration::from_secs(2))
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(created(b"late"))
            })
            .await
            .expect("execute");
        let ExecutionOutcome::Fenced {
            rejection,
            response,
        } = outcome
        else {
            panic!("expected the completion after the ceiling to be fenced");
        };
        assert_eq!(rejection, FencedOutcome::KeyExpired);
        assert_eq!(response, created(b"late"));

        let free = store
            .claim(&key, Duration::from_secs(1))
            .fingerprint(OPERATION, b"{}")
            .try_insert()
            .await
            .expect("claim");
        assert!(matches!(free, ClaimOutcome::Claimed(_)));
    }
}
