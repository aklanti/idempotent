//! The claim builders and the outcomes of claiming a key.

use std::time::Duration;

#[cfg(feature = "json")]
use serde::Serialize;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;

use crate::Cacheable;
use crate::CachedResponse;
use crate::ClaimGuard;
use crate::Fingerprint;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
#[cfg(feature = "json")]
use crate::Json;
use crate::OwnedClaimGuard;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::ReplayOutcome;
use crate::fencing_token::FencingToken;
use crate::fencing_token::Rejection;
use crate::fingerprint::DefaultFingerprintStrategy;
use crate::fingerprint::FingerprintStrategy;
use crate::fingerprint::Operation;
use crate::guard::CacheOutcome;
use crate::store::InsertResult;

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
    /// After the ceiling the lease lapses, so the next attempt can take the key. A completion
    /// that comes later still caches its response while the key is free.
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
    /// The side effect runs with the claim's fencing token, and its response is cached under
    /// the key. A retry with the same fingerprint replays that response.
    ///
    /// With [`keep_alive`](Self::keep_alive) set, the processing lease is renewed while the side
    /// effect runs. If the claim lapsed anyway, the key is claimed again. If another attempt
    /// took it, the outcome is [`Fenced`](ExecutionOutcome::Fenced).
    ///
    /// Dropping the returned future before it completes leaves the claim in place until the
    /// processing TTL expires. For a future that frees the claim when dropped, use
    /// [`claim_owned`](IdempotencyStore::claim_owned). A failed side effect leaves the claim to
    /// expire on both paths.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails. When the store
    /// fails after the side effect ran, the error holds the response it produced.
    pub async fn execute_or_replay<Response, F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome<Response>, ExecutionError<S::Error>>
    where
        Response: Cacheable,
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<Response, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let keep_alive = self.keep_alive;
        let processing_ttl = self.processing_ttl;
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            ClaimOutcome::Claimed(guard) => {
                let value = run_with_renewal(
                    side_effect(guard.fencing_token()),
                    keep_alive.map(|ceiling| guard.keep_alive(processing_ttl, ceiling)),
                )
                .await
                .map_err(ExecutionError::SideEffect)?;
                let encoded = value
                    .to_response()
                    .map_err(|error| ExecutionError::Payload(error.into()))?;
                let cached = guard.cache(encoded.clone(), completed_ttl).await;
                cached_outcome(cached, value, encoded)
            }
            ClaimOutcome::Exists {
                existing,
                fingerprint,
            } => replayed(existing.replay(fingerprint)),
        }
    }
}

#[cfg(feature = "json")]
impl<'store, S: IdempotencyStore> ClaimBuilder<'store, S, WithFingerprint> {
    /// Takes a side effect that returns a value, cached as JSON in the response body.
    ///
    /// The same as returning [`Json`] from the side effect, without the wrapper at either end.
    pub const fn json(self) -> JsonClaimBuilder<'store, S> {
        JsonClaimBuilder { claim: self }
    }
}

/// A builder for a claim whose side effect produces a value rather than a response.
#[cfg(feature = "json")]
pub struct JsonClaimBuilder<'store, S: IdempotencyStore> {
    claim: ClaimBuilder<'store, S, WithFingerprint>,
}

#[cfg(feature = "json")]
impl<'store, S: IdempotencyStore> JsonClaimBuilder<'store, S> {
    /// Renews the processing lease while the side effect runs, for at most `ceiling`.
    ///
    /// After the ceiling the lease lapses, so the next attempt can take the key.
    pub const fn keep_alive(self, ceiling: Duration) -> Self {
        Self {
            claim: self.claim.keep_alive(ceiling),
        }
    }

    /// Claims the key and runs the side effect, or replays the cached value on a matching retry.
    ///
    /// Behaves as [`ClaimBuilder::execute_or_replay`], except that the side effect returns a
    /// value rather than a response, and the value is cached as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails. When the store
    /// fails after the side effect ran, the error holds the response it produced.
    pub async fn execute_or_replay<T, F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome<T>, ExecutionError<S::Error>>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let outcome = self
            .claim
            .execute_or_replay(completed_ttl, |token| async move {
                side_effect(token).await.map(Json)
            })
            .await?;
        Ok(outcome.map(|Json(value)| value))
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
    /// After the ceiling the lease lapses, so the next attempt can take the key. A completion
    /// that comes later still caches its response while the key is free.
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
    /// Behaves as [`ClaimBuilder::execute_or_replay`], except that the returned future owns its
    /// store and key. It can move across tasks and runtimes, and dropping it while the side
    /// effect runs frees the claim at once.
    ///
    /// When the side effect fails, the claim is left to expire, as on the borrowing path.
    /// With [`keep_alive`](Self::keep_alive) set, the processing lease is renewed while the side
    /// effect runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails. When the store
    /// fails after the side effect ran, the error holds the response it produced.
    ///
    /// # Panics
    ///
    /// Panics if polled outside a Tokio runtime.
    pub async fn execute_or_replay<Response, F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome<Response>, ExecutionError<S::Error>>
    where
        Response: Cacheable,
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<Response, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let keep_alive = self.keep_alive;
        let processing_ttl = self.processing_ttl;
        match self.try_insert().await.map_err(ExecutionError::Store)? {
            OwnedClaimOutcome::Claimed(guard) => {
                let value = match run_with_renewal(
                    side_effect(guard.fencing_token()),
                    keep_alive.map(|ceiling| guard.keep_alive(processing_ttl, ceiling)),
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        guard.leave();
                        return Err(ExecutionError::SideEffect(error));
                    }
                };
                let encoded = match value.to_response() {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        guard.leave();
                        return Err(ExecutionError::Payload(error.into()));
                    }
                };
                let cached = guard.cache(encoded.clone(), completed_ttl).await;
                cached_outcome(cached, value, encoded)
            }
            OwnedClaimOutcome::Exists {
                existing,
                fingerprint,
            } => replayed(existing.replay(fingerprint)),
        }
    }
}

#[cfg(feature = "json")]
impl<S: IdempotencyStore + Clone> OwnedClaimBuilder<S, WithFingerprint> {
    /// Takes a side effect that returns a value, cached as JSON in the response body.
    ///
    /// The same as returning [`Json`] from the side effect, without the wrapper at either end.
    pub const fn json(self) -> OwnedJsonClaimBuilder<S> {
        OwnedJsonClaimBuilder { claim: self }
    }
}

/// A builder for an owned claim whose side effect produces a value rather than a response.
#[cfg(feature = "json")]
pub struct OwnedJsonClaimBuilder<S: IdempotencyStore + Clone> {
    claim: OwnedClaimBuilder<S, WithFingerprint>,
}

#[cfg(feature = "json")]
impl<S: IdempotencyStore + Clone> OwnedJsonClaimBuilder<S> {
    /// Renews the processing lease while the side effect runs, for at most `ceiling`.
    ///
    /// After the ceiling the lease lapses, so the next attempt can take the key.
    pub fn keep_alive(self, ceiling: Duration) -> Self {
        Self {
            claim: self.claim.keep_alive(ceiling),
        }
    }

    /// Claims the key and runs the side effect, or replays the cached value on a matching retry.
    ///
    /// Behaves as [`JsonClaimBuilder::execute_or_replay`], except that the returned future owns
    /// its store and key. It can move across tasks and runtimes, and dropping it while the side
    /// effect runs frees the claim at once.
    ///
    /// # Errors
    ///
    /// Returns an error if the side effect fails, or if a store operation fails. When the store
    /// fails after the side effect ran, the error holds the response it produced.
    ///
    /// # Panics
    ///
    /// Panics if polled outside a Tokio runtime.
    pub async fn execute_or_replay<T, F, Fut>(
        self,
        completed_ttl: Duration,
        side_effect: F,
    ) -> Result<ExecutionOutcome<T>, ExecutionError<S::Error>>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(FencingToken) -> Fut,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let outcome = self
            .claim
            .execute_or_replay(completed_ttl, |token| async move {
                side_effect(token).await.map(Json)
            })
            .await?;
        Ok(outcome.map(|Json(value)| value))
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
///
/// `Response` is what the side effect produced, a [`CachedResponse`] unless the side effect
/// returned another [`Cacheable`] payload.
#[derive(Debug)]
pub enum ExecutionOutcome<Response = CachedResponse> {
    /// First time execution of the side effect and its response was cached.
    Executed(Response),
    /// The side effect ran, but the store rejected the completion and did not cache the response.
    Fenced {
        /// The reason the store rejected the completion.
        rejection: Rejection,
        /// The response the side effect produced.
        response: Response,
    },
    /// The cached response was replayed.
    Replayed(Response),
    /// Another request holds the key mid-flight.
    InFlight,
    /// A different request reused the key.
    FingerprintMismatch,
}

impl<Response> ExecutionOutcome<Response> {
    /// Applies `f` to the response the outcome holds.
    pub fn map<T>(self, f: impl FnOnce(Response) -> T) -> ExecutionOutcome<T> {
        match self {
            Self::Executed(response) => ExecutionOutcome::Executed(f(response)),
            Self::Replayed(response) => ExecutionOutcome::Replayed(f(response)),
            Self::Fenced {
                rejection,
                response,
            } => ExecutionOutcome::Fenced {
                rejection,
                response: f(response),
            },
            Self::InFlight => ExecutionOutcome::InFlight,
            Self::FingerprintMismatch => ExecutionOutcome::FingerprintMismatch,
        }
    }
}

/// Error when executing or replaying the operation.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError<StoreError> {
    /// The store failed before the side effect ran.
    #[error("store operation failed")]
    Store(#[source] StoreError),
    /// The side effect returned an error.
    #[error("side effect failed")]
    SideEffect(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The store failed after the side effect ran, and nothing was cached.
    #[error("completion failed after the side effect ran")]
    Completion {
        /// The store error.
        #[source]
        source: StoreError,
        /// The response the side effect produced, as it was cached.
        response: CachedResponse,
    },
    /// The payload could not be encoded for the cache, or a cached one could not be decoded.
    #[error("the payload could not be encoded or decoded")]
    Payload(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl<StoreError> ExecutionError<StoreError> {
    /// Returns the side effect's own error when it has type `T`, or this error unchanged.
    ///
    /// The side effect's error is boxed on the way out, and this takes it back out of the box.
    ///
    /// # Errors
    ///
    /// Returns this error when it came from elsewhere, or when the side effect's error has
    /// another type.
    pub fn into_side_effect_error<T>(self) -> Result<T, Self>
    where
        T: std::error::Error + Send + Sync + 'static,
    {
        let Self::SideEffect(error) = self else {
            return Err(self);
        };
        match error.downcast::<T>() {
            Ok(error) => Ok(*error),
            Err(error) => Err(Self::SideEffect(error)),
        }
    }
}

/// Maps what caching produced to the outcome the caller sees.
fn cached_outcome<Response, StoreError>(
    cached: Result<CacheOutcome, StoreError>,
    value: Response,
    encoded: CachedResponse,
) -> Result<ExecutionOutcome<Response>, ExecutionError<StoreError>> {
    match cached {
        Ok(CacheOutcome::Cached) => Ok(ExecutionOutcome::Executed(value)),
        Ok(CacheOutcome::Uncached { rejection, .. }) => Ok(ExecutionOutcome::Fenced {
            rejection,
            response: value,
        }),
        Err(source) => Err(ExecutionError::Completion {
            source,
            response: encoded,
        }),
    }
}

/// Decodes what the key already holds into the outcome the caller sees.
fn replayed<Response: Cacheable, StoreError>(
    replay: ReplayOutcome,
) -> Result<ExecutionOutcome<Response>, ExecutionError<StoreError>> {
    let outcome = match replay {
        ReplayOutcome::Replayed(cached) => ExecutionOutcome::Replayed(
            Response::from_response(cached)
                .map_err(|error| ExecutionError::Payload(error.into()))?,
        ),
        ReplayOutcome::InFlight => ExecutionOutcome::InFlight,
        ReplayOutcome::FingerprintMismatch => ExecutionOutcome::FingerprintMismatch,
    };
    Ok(outcome)
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
    use crate::fencing_token::Rejection;
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
        assert_eq!(rejection, Rejection::KeyExpired);
        assert_eq!(response, created(b"first"));

        let second: ExecutionOutcome = store
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
        assert_eq!(rejection, Rejection::FencingMismatch);
        assert_eq!(response, created(b"first"));

        let guard = reclaimed
            .take()
            .expect("the reclaiming attempt holds the key");
        let applied = guard
            .complete(created(b"second"), COMPLETED_TTL)
            .await
            .expect("complete");
        assert_eq!(applied, FencedOutcome::Applied);

        let third: ExecutionOutcome = store
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

        let outcome: ExecutionOutcome = store
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

        let second: ExecutionOutcome = store
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

        let second: ExecutionOutcome = store
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
            .execute_or_replay(COMPLETED_TTL, |_token| async move {
                Err::<CachedResponse, _>("boom".into())
            })
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

        let second: ExecutionOutcome = store
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

        let mut reclaimed: Option<ClaimGuard<'_, MemoryStore>> = None;

        let outcome: ExecutionOutcome = store
            .claim(&key, Duration::from_secs(1))
            .keep_alive(Duration::from_secs(2))
            .fingerprint(OPERATION, b"{}")
            .execute_or_replay(COMPLETED_TTL, |_token| {
                let store = &store;
                let key = &key;
                let slot = &mut reclaimed;
                let claim = || {
                    store
                        .claim(key, PROCESSING_TTL)
                        .fingerprint(OPERATION, b"{}")
                        .try_insert()
                };
                async move {
                    // The renewal holds the key past its one second lease, until the ceiling.
                    tokio::time::sleep(Duration::from_millis(1_200)).await;
                    assert!(matches!(claim().await?, ClaimOutcome::Exists { .. }));

                    // Past the ceiling the lease lapses, so the next attempt takes the key.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let ClaimOutcome::Claimed(guard) = claim().await? else {
                        return Err("expected the lapsed key to be free".into());
                    };
                    *slot = Some(guard);
                    Ok(created(b"late"))
                }
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
        assert_eq!(rejection, Rejection::FencingMismatch);
        assert_eq!(response, created(b"late"));
    }
}

#[cfg(all(test, feature = "memory", feature = "json"))]
mod json_tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use serde::Deserialize;
    use serde::Serialize;

    use super::ClaimOutcome;
    use super::ExecutionOutcome;
    use crate::IdempotencyKey;
    use crate::IdempotencyStore;
    use crate::fencing_token::Rejection;
    use crate::store::memory::MemoryStore;

    const OPERATION: &str = "POST /credentials/issue";
    const PROCESSING_TTL: Duration = Duration::from_secs(30);
    const COMPLETED_TTL: Duration = Duration::from_secs(60);

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Issued {
        credential: String,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("signer down")]
    struct SignerDown;

    fn store() -> MemoryStore {
        MemoryStore::builder().try_build().expect("memory store")
    }

    fn issued() -> Issued {
        Issued {
            credential: "signed".to_owned(),
        }
    }

    #[tokio::test]
    async fn json_returns_the_value_then_replays_it() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");
        let runs = AtomicUsize::new(0);

        let first = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(issued())
            })
            .await
            .expect("first run");
        let second: ExecutionOutcome<Issued> = store
            .claim_owned(key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async {
                runs.fetch_add(1, Ordering::SeqCst);
                Err(SignerDown.into())
            })
            .await
            .expect("replay");

        let ExecutionOutcome::Executed(value) = first else {
            panic!("expected the first run to execute");
        };
        assert_eq!(value, issued());
        let ExecutionOutcome::Replayed(value) = second else {
            panic!("expected the retry to replay");
        };
        assert_eq!(value, issued());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn json_reports_in_flight_and_reuse() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");
        let held = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .try_insert()
            .await
            .expect("claim");
        let ClaimOutcome::Claimed(_guard) = held else {
            panic!("expected a fresh claim");
        };

        let in_flight = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async { Ok(issued()) })
            .await
            .expect("execute");
        let reused = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{\"other\": true}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async { Ok(issued()) })
            .await
            .expect("execute");

        assert!(matches!(in_flight, ExecutionOutcome::InFlight));
        assert!(matches!(reused, ExecutionOutcome::FingerprintMismatch));
    }

    #[tokio::test]
    async fn owned_json_leaves_the_claim_when_the_side_effect_fails() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");

        let failed: Result<ExecutionOutcome<Issued>, _> = store
            .claim_owned(key.clone(), PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async { Err(SignerDown.into()) })
            .await;
        let held = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .try_insert()
            .await
            .expect("claim");

        // The side effect's own error comes back out of the box.
        let error = failed.expect_err("the side effect failed");
        assert!(error.into_side_effect_error::<SignerDown>().is_ok());
        assert!(matches!(held, ClaimOutcome::Exists { .. }));
    }

    #[tokio::test]
    async fn json_reports_fenced_when_another_attempt_completed_the_key() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");
        let late = Issued {
            credential: "late".to_owned(),
        };

        let lapsed = store
            .claim(&key, Duration::ZERO)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| {
                let store = &store;
                let key = &key;
                let late = late.clone();
                async move {
                    let winner = store
                        .claim(key, PROCESSING_TTL)
                        .fingerprint(OPERATION, b"{}")
                        .json()
                        .execute_or_replay(COMPLETED_TTL, |_token| async { Ok(issued()) })
                        .await?;
                    assert!(matches!(winner, ExecutionOutcome::Executed(_)));
                    Ok(late)
                }
            })
            .await
            .expect("execute");
        let replay: ExecutionOutcome<Issued> = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async { Err(SignerDown.into()) })
            .await
            .expect("replay");

        let ExecutionOutcome::Fenced {
            rejection,
            response,
        } = lapsed
        else {
            panic!("expected the completion of a key another attempt completed to be fenced");
        };
        assert_eq!(rejection, Rejection::FencingMismatch);
        assert_eq!(response, late);
        let ExecutionOutcome::Replayed(value) = replay else {
            panic!("expected the winner's value to replay");
        };
        assert_eq!(value, issued());
    }

    #[tokio::test]
    async fn json_reports_fenced_when_another_attempt_holds_the_key() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");

        let lapsed = store
            .claim(&key, Duration::ZERO)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async {
                let held = store
                    .claim(&key, PROCESSING_TTL)
                    .fingerprint(OPERATION, b"{}")
                    .try_insert()
                    .await
                    .expect("claim");
                assert!(matches!(held, ClaimOutcome::Claimed(_)));
                Ok(issued())
            })
            .await
            .expect("execute");

        let ExecutionOutcome::Fenced { rejection, .. } = lapsed else {
            panic!("expected the completion of a key another attempt holds to be fenced");
        };
        assert_eq!(rejection, Rejection::FencingMismatch);
    }

    #[tokio::test]
    async fn owned_json_reports_fenced_when_another_attempt_holds_the_key() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");

        let lapsed = store
            .claim_owned(key.clone(), Duration::ZERO)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| {
                let store = store.clone();
                let key = key.clone();
                async move {
                    let held = store
                        .claim(&key, PROCESSING_TTL)
                        .fingerprint(OPERATION, b"{}")
                        .try_insert()
                        .await
                        .expect("claim");
                    assert!(matches!(held, ClaimOutcome::Claimed(_)));
                    Ok(issued())
                }
            })
            .await
            .expect("execute");

        let ExecutionOutcome::Fenced { rejection, .. } = lapsed else {
            panic!("expected the completion of a key another attempt holds to be fenced");
        };
        assert_eq!(rejection, Rejection::FencingMismatch);
    }

    #[tokio::test(start_paused = true)]
    async fn json_caches_the_value_after_its_claim_lapsed() {
        let store = store();
        let key = IdempotencyKey::new("offer-8f21").expect("valid key");
        let runs = AtomicUsize::new(0);

        // The lease lapses while the side effect runs, and no other attempt takes the key.
        let lapsed = store
            .claim(&key, Duration::from_secs(1))
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(issued())
            })
            .await
            .expect("execute");
        let retry: ExecutionOutcome<Issued> = store
            .claim(&key, PROCESSING_TTL)
            .fingerprint(OPERATION, b"{}")
            .json()
            .execute_or_replay(COMPLETED_TTL, |_token| async {
                runs.fetch_add(1, Ordering::SeqCst);
                Err(SignerDown.into())
            })
            .await
            .expect("replay");

        assert!(matches!(lapsed, ExecutionOutcome::Executed(_)));
        let ExecutionOutcome::Replayed(value) = retry else {
            panic!("expected the retry to replay");
        };
        assert_eq!(value, issued());
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}
