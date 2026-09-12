//! The HTTP idempotency middleware.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use http::request::Parts;
use http_body::Body;
use http_body_util::BodyExt;
use http_body_util::LengthLimitError;
use http_body_util::Limited;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tower::Layer;
use tower::Service;

use super::rejection::IdempotencyRejection;
use crate::CachedResponse;
use crate::ClaimOutcome;
use crate::Error;
use crate::FencedOutcome;
use crate::IdempotencyEntry;
use crate::IdempotencyKey;
use crate::IdempotencyStore;
use crate::InsertResult;
use crate::Metadata;
use crate::ReplayOutcome;
use crate::entry::Completed;
use crate::entry::Processing;
use crate::fencing_token::FencingToken;
use crate::fingerprint::DefaultFingerprintStrategy;
use crate::fingerprint::FingerprintStrategy;
use crate::fingerprint::Operation;

/// A boxed error.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The header the key is read from by default.
pub(super) const DEFAULT_HEADER: HeaderName = HeaderName::from_static("idempotency-key");
/// Marks a response served from the cache.
const REPLAYED: HeaderName = HeaderName::from_static("idempotent-replayed");
const DEFAULT_PROCESSING_TTL: Duration = Duration::from_secs(60);
const DEFAULT_COMPLETED_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_MAX_BODY_SIZE: usize = 1 << 20;
const DEFAULT_STORE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(10 * 60);

/// A hook that identifies the caller from a request without its body.
type ScopeHook = dyn Fn(&Parts) -> Option<String> + Send + Sync;

/// The settings shared by the layer and every service built from it.
#[derive(Clone)]
struct Settings<S> {
    store: TimeoutStore<S>,
    header: HeaderName,
    processing_ttl: Duration,
    completed_ttl: Duration,
    strategy: Arc<dyn FingerprintStrategy>,
    max_body_size: usize,
    require_key: bool,
    scope: Option<Arc<ScopeHook>>,
    keep_alive: Duration,
    tracker: TaskTracker,
    max_in_flight: Arc<Semaphore>,
}

impl<S> Settings<S> {
    /// Reads the client's key from the request headers.
    fn client_key(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<IdempotencyKey>, IdempotencyRejection> {
        let Some(value) = headers.get(&self.header) else {
            return if self.require_key {
                Err(IdempotencyRejection::MissingKey)
            } else {
                Ok(None)
            };
        };
        IdempotencyKey::try_from(value)
            .map(Some)
            .map_err(IdempotencyRejection::InvalidKey)
    }

    /// Puts the key under the caller's scope when a hook is set.
    fn scoped_key(
        &self,
        parts: &Parts,
        key: IdempotencyKey,
    ) -> Result<IdempotencyKey, IdempotencyRejection> {
        let Some(hook) = &self.scope else {
            return Ok(key);
        };
        let scope = hook(parts)
            .filter(|scope| !scope.is_empty())
            .ok_or(IdempotencyRejection::MissingScope)?;
        stored_key(&scope, &key).map_err(IdempotencyRejection::InvalidKey)
    }

    /// Admits a request under the cap on requests in flight, or rejects it as overloaded.
    fn admit(&self) -> Result<OwnedSemaphorePermit, IdempotencyRejection> {
        Arc::clone(&self.max_in_flight)
            .try_acquire_owned()
            .map_err(|_| IdempotencyRejection::Overloaded)
    }
}

/// Wraps a service so that a request with an idempotency key is handled once and replays after.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use idempotent::memory::MemoryStore;
/// use idempotent::middleware::IdempotencyLayer;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let layer = IdempotencyLayer::new(MemoryStore::builder().try_build()?)
///     .scope(|parts| {
///         parts
///             .headers
///             .get("x-tenant")
///             .and_then(|tenant| tenant.to_str().ok())
///             .map(String::from)
///     })
///     .max_in_flight(512);
/// let tracker = layer.tracker();
///
/// // Serve with the layer. Once the listener has stopped, drain what is still running.
/// tracker.close();
/// tokio::time::timeout(Duration::from_secs(30), tracker.wait()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct IdempotencyLayer<S> {
    settings: Settings<S>,
}

impl<S> IdempotencyLayer<S> {
    /// Creates a layer over `store` with the default settings.
    pub fn new(store: S) -> Self {
        Self {
            settings: Settings {
                store: TimeoutStore {
                    store,
                    timeout: DEFAULT_STORE_TIMEOUT,
                },
                header: DEFAULT_HEADER,
                processing_ttl: DEFAULT_PROCESSING_TTL,
                completed_ttl: DEFAULT_COMPLETED_TTL,
                strategy: Arc::new(DefaultFingerprintStrategy),
                max_body_size: DEFAULT_MAX_BODY_SIZE,
                require_key: false,
                scope: None,
                keep_alive: DEFAULT_KEEP_ALIVE,
                tracker: TaskTracker::new(),
                max_in_flight: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            },
        }
    }

    /// Reads the key from `header` instead of the idempotency-key header.
    pub fn header(mut self, header: HeaderName) -> Self {
        self.settings.header = header;
        self
    }

    /// Sets the processing lease, the time a claim survives a worker that died holding it.
    ///
    /// Sixty seconds by default. The lease is renewed while the handler runs, so it does not
    /// bound the handler; [`keep_alive`](Self::keep_alive) does. A zero lease fences every
    /// completion.
    pub const fn processing_ttl(mut self, ttl: Duration) -> Self {
        self.settings.processing_ttl = ttl;
        self
    }

    /// Sets the completed lease, how long a cached response replays.
    ///
    /// A day by default. A zero lease replays nothing.
    pub const fn completed_ttl(mut self, ttl: Duration) -> Self {
        self.settings.completed_ttl = ttl;
        self
    }

    /// Fingerprints each request with `strategy` instead of [`DefaultFingerprintStrategy`].
    ///
    /// Every strategy receives the operation, which is the method, the path, and the query,
    /// and the buffered body.
    pub fn fingerprint_strategy(mut self, strategy: impl FingerprintStrategy) -> Self {
        self.settings.strategy = Arc::new(strategy);
        self
    }

    /// Caps the request body the layer buffers and the response body it caches.
    ///
    /// One mebibyte by default. A larger request is rejected with 413. A larger response is
    /// returned uncached, and its claim is left to expire.
    pub const fn max_body_size(mut self, bytes: usize) -> Self {
        self.settings.max_body_size = bytes;
        self
    }

    /// Rejects a request without a key with 400 instead of forwarding it.
    ///
    /// Off by default. Safe methods are forwarded either way.
    pub const fn require_key(mut self, required: bool) -> Self {
        self.settings.require_key = required;
        self
    }

    /// Scopes every key to the caller that `hook` finds in the request.
    ///
    /// The hook sees the request without its body and returns the caller's identity, such as a
    /// tenant id an authentication layer put in the extensions. Without a scope, anyone who
    /// presents a key with the same request gets the cached response, so a service with more
    /// than one client needs one. The stored key is what [`stored_key`] builds from the scope
    /// and the client's key, which limits the client's key to 222 bytes. When the hook returns
    /// nothing, or an empty string, the request is rejected with 400.
    pub fn scope<F>(mut self, hook: F) -> Self
    where
        F: Fn(&Parts) -> Option<String> + Send + Sync + 'static,
    {
        self.settings.scope = Some(Arc::new(hook));
        self
    }

    /// Bounds every store call the layer makes, the touches of the lease renewal included.
    ///
    /// Five seconds by default. A call that times out is a store error. Before the handler
    /// runs that is a 503, and after it the response is returned uncached.
    pub const fn store_timeout(mut self, timeout: Duration) -> Self {
        self.settings.store.timeout = timeout;
        self
    }

    /// Sets how long the processing lease is renewed while the handler runs.
    ///
    /// Ten minutes by default. Past the ceiling the lease lapses, and the completion is fenced
    /// while the response is still returned.
    pub const fn keep_alive(mut self, ceiling: Duration) -> Self {
        self.settings.keep_alive = ceiling;
        self
    }

    /// Caps the requests with a key running at once, answering 503 past the cap.
    ///
    /// No cap by default, and a cap of zero sheds every request with a key. The work for a
    /// request with a key outlives the response future, so this is the one limit that sees it;
    /// a limit outside the layer does not. A request shed here has made no store call.
    pub fn max_in_flight(mut self, limit: usize) -> Self {
        self.settings.max_in_flight = Arc::new(Semaphore::new(limit));
        self
    }

    /// Returns the tracker of the work the layer detached, for the shutdown path.
    ///
    /// Close it once connections are no longer accepted, then wait on it. The layer never
    /// closes it.
    pub fn tracker(&self) -> TaskTracker {
        self.settings.tracker.clone()
    }

    /// Returns the number of requests with a key currently running.
    pub fn in_flight(&self) -> usize {
        self.settings.tracker.len()
    }
}

impl<S> fmt::Debug for IdempotencyLayer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyLayer")
            .field("header", &self.settings.header)
            .field("processing_ttl", &self.settings.processing_ttl)
            .field("completed_ttl", &self.settings.completed_ttl)
            .field("max_body_size", &self.settings.max_body_size)
            .field("require_key", &self.settings.require_key)
            .field("scoped", &self.settings.scope.is_some())
            .field("store_timeout", &self.settings.store.timeout)
            .field("keep_alive", &self.settings.keep_alive)
            .finish_non_exhaustive()
    }
}

impl<S: Clone, Inner> Layer<Inner> for IdempotencyLayer<S> {
    type Service = IdempotencyService<S, Inner>;

    fn layer(&self, inner: Inner) -> Self::Service {
        IdempotencyService {
            inner,
            settings: Arc::new(self.settings.clone()),
        }
    }
}

/// The service [`IdempotencyLayer`] builds around the inner one.
///
/// # Panics
///
/// Calling it outside a Tokio runtime panics, since each request with a key runs in a spawned
/// task.
#[derive(Clone)]
pub struct IdempotencyService<S, Inner> {
    inner: Inner,
    settings: Arc<Settings<S>>,
}

impl<S, Inner: fmt::Debug> fmt::Debug for IdempotencyService<S, Inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<S, Inner, ReqBody, ResBody> Service<Request<ReqBody>> for IdempotencyService<S, Inner>
where
    S: IdempotencyStore,
    Inner: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    Inner::Future: Send + 'static,
    Inner::Error: Send + 'static,
    ReqBody: Body + From<Bytes> + Send + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: Into<BoxError>,
    ResBody: Body + From<Bytes> + Send + 'static,
    ResBody::Data: Send,
    ResBody::Error: Into<BoxError>,
{
    type Error = Inner::Error;
    type Future = ResponseFuture<Inner::Future, ResBody, Inner::Error>;
    type Response = Response<ResBody>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        if request.method().is_safe() {
            return ResponseFuture::passthrough(inner.call(request));
        }
        let key = match self.settings.client_key(request.headers()) {
            Ok(Some(key)) => key,
            Ok(None) => {
                #[cfg(feature = "tracing")]
                tracing::trace!("no idempotency key, passing through");
                return ResponseFuture::passthrough(inner.call(request));
            }
            Err(rejection) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(rejection = rejection.code(), "request rejected");
                return ResponseFuture::rejected(rejection);
            }
        };
        let (parts, body) = request.into_parts();
        let key = match self.settings.scoped_key(&parts, key) {
            Ok(key) => key,
            Err(rejection) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(rejection = rejection.code(), "request rejected");
                return ResponseFuture::rejected(rejection);
            }
        };
        #[cfg(feature = "tracing")]
        let span = tracing::info_span!(
            "idempotent.middleware",
            idempotency_key = %key,
            http.method = %parts.method,
            url.path = parts.uri.path(),
            outcome = tracing::field::Empty,
        );
        let permit = match self.settings.admit() {
            Ok(permit) => permit,
            Err(rejection) => {
                #[cfg(feature = "tracing")]
                {
                    span.record("outcome", "overloaded");
                    tracing::warn!(parent: &span, "max_in_flight reached, shedding");
                }
                return ResponseFuture::rejected(rejection);
            }
        };
        let settings = Arc::clone(&self.settings);
        let future = handle(settings, inner, parts, body, key, permit);
        #[cfg(feature = "tracing")]
        let future = tracing::Instrument::instrument(future, span);
        ResponseFuture::detached(self.settings.tracker.spawn(future))
    }
}

/// Handles a request with a key to completion, detached from the connection.
///
/// The permit under the cap on requests in flight is released when the handling ends.
async fn handle<S, Inner, ReqBody, ResBody>(
    settings: Arc<Settings<S>>,
    mut inner: Inner,
    mut parts: Parts,
    body: ReqBody,
    key: IdempotencyKey,
    _permit: OwnedSemaphorePermit,
) -> Result<Response<ResBody>, Inner::Error>
where
    S: IdempotencyStore,
    Inner: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ReqBody: Body + From<Bytes>,
    ReqBody::Error: Into<BoxError>,
    ResBody: Body + From<Bytes>,
    ResBody::Error: Into<BoxError>,
{
    let bytes = match buffer(body, settings.max_body_size).await {
        Ok(Buffered::Bytes(bytes)) => bytes,
        Ok(Buffered::TooLarge) => {
            outcome("rejected");
            #[cfg(feature = "tracing")]
            tracing::warn!("request body exceeds max_body_size");
            return Ok(IdempotencyRejection::BodyTooLarge.render());
        }
        Err(_error) => {
            outcome("rejected");
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_error, "request body failed");
            return Ok(IdempotencyRejection::RequestBodyFailed.render());
        }
    };
    let claim = settings
        .store
        .claim(&key, settings.processing_ttl)
        .fingerprint_with(&*settings.strategy, Operation::from(&parts), &bytes);
    let guard = match claim.try_insert().await {
        Ok(ClaimOutcome::Claimed(guard)) => guard,
        Ok(ClaimOutcome::Exists {
            existing,
            fingerprint,
        }) => {
            return Ok(match existing.replay(fingerprint) {
                ReplayOutcome::Replayed(cached) => {
                    outcome("replayed");
                    #[cfg(feature = "tracing")]
                    tracing::info!("replaying cached response");
                    replay(cached)
                }
                ReplayOutcome::InFlight => {
                    outcome("in_flight");
                    #[cfg(feature = "tracing")]
                    tracing::warn!("request in flight, returning 409");
                    IdempotencyRejection::InFlight.render()
                }
                ReplayOutcome::FingerprintMismatch => {
                    outcome("mismatch");
                    #[cfg(feature = "tracing")]
                    tracing::warn!("fingerprint mismatch, returning 400");
                    IdempotencyRejection::FingerprintMismatch.render()
                }
            });
        }
        Err(error) => {
            outcome("error");
            store_failed("claim", &error);
            return Ok(IdempotencyRejection::StoreError.render());
        }
    };

    parts.extensions.insert(key.clone());
    let request = Request::from_parts(parts, ReqBody::from(bytes));
    let response = tokio::select! {
        result = inner.call(request) => match result {
            Ok(response) => response,
            Err(error) => {
                outcome("error");
                return Err(error);
            }
        },
        never = guard.keep_alive(settings.processing_ttl, settings.keep_alive) => match never {},
    };

    if declines_caching(response.headers()) {
        outcome("declined");
        match settings.store.remove(&key, guard.fencing_token()).await {
            Ok(FencedOutcome::Applied) => {
                #[cfg(feature = "tracing")]
                tracing::info!("handler declined caching, claim released");
            }
            Ok(_rejection) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    rejection = ?_rejection,
                    "handler declined caching, but the claim was no longer held"
                );
            }
            Err(error) => store_failed("release", &error),
        }
        return Ok(response);
    }
    let (parts, body) = response.into_parts();
    let fits = body
        .size_hint()
        .upper()
        .and_then(|upper| usize::try_from(upper).ok())
        .is_some_and(|upper| upper <= settings.max_body_size);
    if !fits {
        outcome("uncached");
        #[cfg(feature = "tracing")]
        tracing::warn!("response is streaming or over the cache cap, returning it uncached");
        return Ok(Response::from_parts(parts, body));
    }
    let bytes = match Limited::new(body, settings.max_body_size).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_error) => {
            outcome("error");
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_error, "response body failed while it was read for the cache");
            return Ok(IdempotencyRejection::ResponseBodyFailed.render());
        }
    };
    let cached = CachedResponse {
        status_code: parts.status.as_u16(),
        metadata: storable_headers(&parts.headers),
        body: bytes.clone(),
    };
    match guard.complete(cached, settings.completed_ttl).await {
        Ok(FencedOutcome::Applied) => {
            outcome("executed");
            #[cfg(feature = "tracing")]
            tracing::debug!("executed, response cached");
        }
        Ok(_rejection) => {
            outcome("fenced");
            #[cfg(feature = "tracing")]
            tracing::warn!(rejection = ?_rejection, "the store rejected the completion after the handler ran");
        }
        Err(error) => {
            outcome("uncached");
            store_failed("completion", &error);
        }
    }
    Ok(Response::from_parts(parts, ResBody::from(bytes)))
}

/// Records the outcome of the request on its span.
#[cfg(feature = "tracing")]
fn outcome(value: &'static str) {
    tracing::Span::current().record("outcome", value);
}

/// Records the outcome of the request on its span.
#[cfg(not(feature = "tracing"))]
const fn outcome(_value: &'static str) {}

/// Logs a failed store call at error, telling a timeout from a failure.
#[cfg(feature = "tracing")]
fn store_failed<E: std::fmt::Display>(operation: &'static str, error: &TimeoutStoreError<E>) {
    match error {
        TimeoutStoreError::Store(source) => {
            tracing::error!(operation, error = %source, "store operation failed");
        }
        TimeoutStoreError::Elapsed(timeout) => {
            tracing::error!(operation, ?timeout, "store operation timed out");
        }
    }
}

/// Logs a failed store call at error, telling a timeout from a failure.
#[cfg(not(feature = "tracing"))]
const fn store_failed<E>(_operation: &'static str, _error: &TimeoutStoreError<E>) {}

/// Rebuilds a cached response and marks it as replayed.
fn replay<B: From<Bytes>>(cached: CachedResponse) -> Response<B> {
    let mut response = Response::new(B::from(cached.body));
    *response.status_mut() =
        StatusCode::from_u16(cached.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    for (name, value) in cached.metadata.iter() {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value),
        ) {
            headers.append(name, value);
        }
    }
    headers.insert(REPLAYED, HeaderValue::from_static("true"));
    response
}

/// Returns true if the handler set no-store in the cache-control header of its response.
fn declines_caching(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|directive| directive.trim().eq_ignore_ascii_case("no-store"))
}

pin_project_lite::pin_project! {
    /// The future [`IdempotencyService`] returns.
    pub struct ResponseFuture<F, B, E> {
        #[pin]
        kind: Kind<F, B, E>,
    }
}

pin_project_lite::pin_project! {
    #[project = KindProj]
    enum Kind<F, B, E> {
        Passthrough { #[pin] future: F },
        Rejected { response: Option<Response<B>> },
        Detached { task: JoinHandle<Result<Response<B>, E>> },
    }
}

impl<F, B, E> fmt::Debug for ResponseFuture<F, B, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseFuture").finish_non_exhaustive()
    }
}

impl<F, B, E> ResponseFuture<F, B, E> {
    const fn passthrough(future: F) -> Self {
        Self {
            kind: Kind::Passthrough { future },
        }
    }

    fn rejected(rejection: IdempotencyRejection) -> Self
    where
        B: From<Bytes>,
    {
        Self {
            kind: Kind::Rejected {
                response: Some(rejection.render()),
            },
        }
    }

    const fn detached(task: JoinHandle<Result<Response<B>, E>>) -> Self {
        Self {
            kind: Kind::Detached { task },
        }
    }
}

impl<F, B, E> Future for ResponseFuture<F, B, E>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: From<Bytes>,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Passthrough { future } => future.poll(cx),
            KindProj::Rejected { response } => Poll::Ready(Ok(response
                .take()
                .expect("ResponseFuture polled after completion"))),
            KindProj::Detached { task } => match std::task::ready!(Pin::new(task).poll(cx)) {
                Ok(result) => Poll::Ready(result),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_cancelled) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("runtime shut down before the handler finished");
                    Poll::Ready(Ok(IdempotencyRejection::Shutdown.render()))
                }
            },
        }
    }
}

/// A store whose calls are bounded by a timeout.
#[derive(Clone)]
struct TimeoutStore<S> {
    store: S,
    timeout: Duration,
}

/// The error of a store call made under a timeout.
#[derive(Debug, thiserror::Error)]
enum TimeoutStoreError<E> {
    /// The store returned an error.
    #[error("store operation failed")]
    Store(#[source] E),
    /// The timeout passed before the store answered.
    #[error("store operation timed out after {0:?}")]
    Elapsed(Duration),
}

impl<S: IdempotencyStore> TimeoutStore<S> {
    async fn run<T>(
        &self,
        call: impl Future<Output = Result<T, S::Error>> + Send,
    ) -> Result<T, TimeoutStoreError<S::Error>> {
        match tokio::time::timeout(self.timeout, call).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(source)) => Err(TimeoutStoreError::Store(source)),
            Err(_elapsed) => Err(TimeoutStoreError::Elapsed(self.timeout)),
        }
    }
}

impl<S: IdempotencyStore> IdempotencyStore for TimeoutStore<S> {
    type Error = TimeoutStoreError<S::Error>;

    fn try_insert(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Processing>,
    ) -> impl Future<Output = Result<InsertResult, Self::Error>> + Send {
        self.run(self.store.try_insert(key, entry))
    }

    fn complete(
        &self,
        key: &IdempotencyKey,
        entry: IdempotencyEntry<Completed>,
        fencing_token: FencingToken,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send {
        self.run(self.store.complete(key, entry, fencing_token))
    }

    fn remove(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send {
        self.run(self.store.remove(key, fencing_token))
    }

    fn touch(
        &self,
        key: &IdempotencyKey,
        fencing_token: FencingToken,
        ttl: Duration,
    ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send {
        self.run(self.store.touch(key, fencing_token, ttl))
    }

    fn purge(&self, key: &IdempotencyKey) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.run(self.store.purge(key))
    }
}

/// Returns the key the layer stores for a client's `key` when `scope` identifies the caller.
///
/// # Errors
///
/// Returns an error if the stored key exceeds 255 bytes.
pub fn stored_key(scope: &str, key: &IdempotencyKey) -> Result<IdempotencyKey, Error> {
    let digest = Sha256::digest(scope.as_bytes());
    let truncated = digest[..16]
        .iter()
        .fold(0u128, |acc, &byte| (acc << 8) | u128::from(byte));
    IdempotencyKey::new(format!("{truncated:032x}"))?.scoped(key.as_str())
}

/// The keep-alive header, which the http crate has no constant for.
const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");

/// Copies the headers worth replaying into [`Metadata`].
///
/// Hop-by-hop headers describe the connection that carried the response, and the date header
/// is set afresh by the server, so neither is stored.
fn storable_headers(headers: &HeaderMap) -> Metadata {
    headers
        .iter()
        .filter(|(name, _)| !is_connection_specific(name))
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                Bytes::copy_from_slice(value.as_bytes()),
            )
        })
        .collect()
}

/// Returns true if a header belongs to the connection rather than the response.
fn is_connection_specific(name: &HeaderName) -> bool {
    [
        header::CONNECTION,
        KEEP_ALIVE,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        header::DATE,
    ]
    .contains(name)
}

/// The result of buffering a request body under a cap.
enum Buffered {
    /// The body fit within the cap.
    Bytes(Bytes),
    /// The body exceeded the cap and was not buffered.
    TooLarge,
}

/// Collects `body` into memory, refusing to buffer more than `max` bytes.
///
/// # Errors
///
/// Returns an error if the body itself fails.
async fn buffer<B>(body: B, max: usize) -> Result<Buffered, BoxError>
where
    B: http_body::Body,
    B::Error: Into<BoxError>,
{
    match Limited::new(body, max).collect().await {
        Ok(collected) => Ok(Buffered::Bytes(collected.to_bytes())),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => Ok(Buffered::TooLarge),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use http_body_util::Full;

    use super::*;

    #[test]
    fn stored_key_hashes_the_scope() {
        let key = IdempotencyKey::new("cred-offer-123").expect("valid key");
        let alice = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        let again = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        let bob = stored_key("did:web:bob.example", &key).expect("a DID scopes");
        assert_eq!(alice, again);
        assert_ne!(alice, bob);
        assert!(alice.as_str().ends_with("/cred-offer-123"));
        assert_eq!(alice.as_str().len(), 32 + 1 + "cred-offer-123".len());

        let longest = IdempotencyKey::new("k".repeat(222)).expect("valid key");
        assert!(stored_key("tenant", &longest).is_ok());
        let too_long = IdempotencyKey::new("k".repeat(223)).expect("valid key");
        assert!(matches!(
            stored_key("tenant", &too_long),
            Err(Error::KeyTooLong(_))
        ));
    }

    #[test]
    fn storable_headers_drop_connection_headers_and_date() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert("x-charge-id", HeaderValue::from_static("ch_1"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(
            header::DATE,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"),
        );

        let stored = storable_headers(&headers);
        let names: Vec<&str> = stored.iter().map(|(name, _)| name).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"content-type"));
        assert!(names.contains(&"x-charge-id"));
    }

    #[tokio::test]
    async fn buffer_accepts_a_body_at_the_cap_and_rejects_over_it() {
        let Ok(Buffered::Bytes(bytes)) = buffer(Full::new(Bytes::from_static(b"12345")), 5).await
        else {
            panic!("a body at the cap must buffer");
        };
        assert_eq!(bytes.len(), 5);
        assert!(matches!(
            buffer(Full::new(Bytes::from_static(b"123456")), 5).await,
            Ok(Buffered::TooLarge)
        ));
    }
}

#[cfg(all(test, feature = "memory"))]
mod service_tests {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use http::HeaderValue;
    use http_body::Frame;
    use http_body::SizeHint;
    use http_body_util::Full;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use super::*;
    use crate::store::memory::MemoryStore;
    use crate::store::memory::MemoryStoreError;

    const KEY: &str = "cred-offer-123";

    /// A body that is either whole or fails on the first read.
    enum TestBody {
        Full(Full<Bytes>),
        Failing,
    }

    impl From<Bytes> for TestBody {
        fn from(bytes: Bytes) -> Self {
            Self::Full(Full::new(bytes))
        }
    }

    impl From<&'static str> for TestBody {
        fn from(text: &'static str) -> Self {
            Self::from(Bytes::from_static(text.as_bytes()))
        }
    }

    impl Body for TestBody {
        type Data = Bytes;
        type Error = BoxError;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
            match self.get_mut() {
                Self::Full(full) => Pin::new(full).poll_frame(cx).map_err(Into::into),
                Self::Failing => Poll::Ready(Some(Err("body failed".into()))),
            }
        }

        fn size_hint(&self) -> SizeHint {
            match self {
                Self::Full(full) => full.size_hint(),
                Self::Failing => SizeHint::with_exact(4),
            }
        }
    }

    type HandlerFuture =
        Pin<Box<dyn Future<Output = Result<Response<TestBody>, Infallible>> + Send>>;

    /// A handler that counts its runs.
    #[derive(Clone)]
    struct Handler {
        runs: Arc<AtomicUsize>,
        respond: Arc<dyn Fn(Request<TestBody>) -> HandlerFuture + Send + Sync>,
    }

    impl Handler {
        fn new<F, Fut>(respond: F) -> Self
        where
            F: Fn(Request<TestBody>) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Response<TestBody>> + Send + 'static,
        {
            Self {
                runs: Arc::default(),
                respond: Arc::new(move |request| {
                    let response = respond(request);
                    Box::pin(async move { Ok(response.await) })
                }),
            }
        }

        /// A handler that answers `status` with `body`.
        fn answering(status: StatusCode, body: &'static str) -> Self {
            Self::new(move |_| async move { text(status, body) })
        }

        /// A handler that echoes the request body.
        fn echoing() -> Self {
            Self::new(|request| async move {
                let bytes = request
                    .into_body()
                    .collect()
                    .await
                    .expect("request body")
                    .to_bytes();
                Response::new(TestBody::from(bytes))
            })
        }

        /// A handler that reports when it starts and waits to be released.
        fn gated(started: &Arc<Notify>, release: &Arc<Notify>) -> Self {
            let (started, release) = (Arc::clone(started), Arc::clone(release));
            Self::new(move |_| {
                let (started, release) = (Arc::clone(&started), Arc::clone(&release));
                async move {
                    started.notify_one();
                    release.notified().await;
                    text(StatusCode::OK, "done")
                }
            })
        }

        fn runs(&self) -> usize {
            self.runs.load(Ordering::SeqCst)
        }
    }

    impl Service<Request<TestBody>> for Handler {
        type Error = Infallible;
        type Future = HandlerFuture;
        type Response = Response<TestBody>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<TestBody>) -> HandlerFuture {
            self.runs.fetch_add(1, Ordering::SeqCst);
            (self.respond)(request)
        }
    }

    /// A memory store with two switches, one that never answers a claim and one that fails
    /// every completion.
    #[derive(Clone)]
    struct SwitchedStore {
        inner: MemoryStore,
        hang_claims: Arc<AtomicBool>,
        fail_completions: Arc<AtomicBool>,
    }

    impl SwitchedStore {
        fn new() -> Self {
            Self {
                inner: store(),
                hang_claims: Arc::default(),
                fail_completions: Arc::default(),
            }
        }
    }

    impl IdempotencyStore for SwitchedStore {
        type Error = MemoryStoreError;

        async fn try_insert(
            &self,
            key: &IdempotencyKey,
            entry: IdempotencyEntry<Processing>,
        ) -> Result<InsertResult, Self::Error> {
            if self.hang_claims.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            self.inner.try_insert(key, entry).await
        }

        async fn complete(
            &self,
            key: &IdempotencyKey,
            entry: IdempotencyEntry<Completed>,
            fencing_token: FencingToken,
        ) -> Result<FencedOutcome, Self::Error> {
            if self.fail_completions.load(Ordering::SeqCst) {
                return Err(MemoryStoreError::TaskStopped);
            }
            self.inner.complete(key, entry, fencing_token).await
        }

        fn remove(
            &self,
            key: &IdempotencyKey,
            fencing_token: FencingToken,
        ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send {
            self.inner.remove(key, fencing_token)
        }

        fn touch(
            &self,
            key: &IdempotencyKey,
            fencing_token: FencingToken,
            ttl: Duration,
        ) -> impl Future<Output = Result<FencedOutcome, Self::Error>> + Send {
            self.inner.touch(key, fencing_token, ttl)
        }

        fn purge(
            &self,
            key: &IdempotencyKey,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            self.inner.purge(key)
        }
    }

    fn store() -> MemoryStore {
        MemoryStore::builder().try_build().expect("memory store")
    }

    fn text(status: StatusCode, body: &'static str) -> Response<TestBody> {
        let mut response = Response::new(TestBody::from(body));
        *response.status_mut() = status;
        response
    }

    fn post(key: Option<&str>, body: &'static str) -> Request<TestBody> {
        let mut request = Request::post("/credentials");
        if let Some(key) = key {
            request = request.header(DEFAULT_HEADER, key);
        }
        request.body(TestBody::from(body)).expect("valid request")
    }

    async fn body(response: Response<TestBody>) -> Bytes {
        response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes()
    }

    fn rejection(response: &Response<TestBody>) -> Option<&str> {
        response
            .headers()
            .get("idempotent-rejection")
            .and_then(|value| value.to_str().ok())
    }

    fn retry_after(response: &Response<TestBody>) -> Option<&[u8]> {
        response
            .headers()
            .get(header::RETRY_AFTER)
            .map(HeaderValue::as_bytes)
    }

    fn replayed(response: &Response<TestBody>) -> bool {
        response.headers().contains_key(REPLAYED)
    }

    #[tokio::test]
    async fn request_without_key_is_forwarded_untouched() {
        let handler = Handler::echoing();
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(response) = service.oneshot(post(None, "payload")).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(!replayed(&response));
        assert_eq!(&body(response).await[..], b"payload");
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn request_without_key_is_rejected_when_keys_are_required() {
        let handler = Handler::answering(StatusCode::OK, "done");
        let service = IdempotencyLayer::new(store())
            .require_key(true)
            .layer(handler.clone());

        let Ok(response) = service.oneshot(post(None, "{}")).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(rejection(&response), Some("missing-key"));
        assert_eq!(handler.runs(), 0);
    }

    #[tokio::test]
    async fn safe_method_ignores_key() {
        let handler = Handler::answering(StatusCode::OK, "listed");
        let service = IdempotencyLayer::new(store()).layer(handler.clone());
        let request = || {
            Request::get("/credentials")
                .header(DEFAULT_HEADER, KEY)
                .body(TestBody::from(""))
                .expect("valid request")
        };

        let Ok(first) = service.clone().oneshot(request()).await;
        let Ok(second) = service.oneshot(request()).await;

        assert!(!replayed(&first));
        assert!(!replayed(&second));
        assert_eq!(handler.runs(), 2);
    }

    #[tokio::test]
    async fn malformed_key_is_rejected_before_handler_runs() {
        let handler = Handler::answering(StatusCode::OK, "done");
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(response) = service.oneshot(post(Some("bad/key"), "{}")).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(rejection(&response), Some("invalid-key"));
        assert_eq!(handler.runs(), 0);
    }

    #[tokio::test]
    async fn scoped_keys_do_not_collide_across_principals() {
        let store = store();
        let handler = Handler::answering(StatusCode::CREATED, "issued");
        let service = IdempotencyLayer::new(store.clone())
            .scope(|parts| {
                parts
                    .headers
                    .get("x-caller")
                    .and_then(|caller| caller.to_str().ok())
                    .map(String::from)
            })
            .layer(handler.clone());
        let request = |caller: &str| {
            Request::post("/credentials")
                .header(DEFAULT_HEADER, KEY)
                .header("x-caller", caller)
                .body(TestBody::from("{}"))
                .expect("valid request")
        };

        let Ok(alice) = service
            .clone()
            .oneshot(request("did:web:alice.example"))
            .await;
        let Ok(bob) = service
            .clone()
            .oneshot(request("did:web:bob.example"))
            .await;
        let Ok(anonymous) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(alice.status(), StatusCode::CREATED);
        assert_eq!(bob.status(), StatusCode::CREATED);
        assert!(!replayed(&bob));
        assert_eq!(rejection(&anonymous), Some("missing-scope"));
        assert_eq!(handler.runs(), 2);

        let key = IdempotencyKey::new(KEY).expect("valid key");
        let stored = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        let fingerprint = DefaultFingerprintStrategy.compute(&"POST /credentials".into(), b"{}");
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(1));
        assert!(matches!(
            store.try_insert(&stored, entry).await,
            Ok(InsertResult::Exists(_))
        ));
    }

    #[tokio::test]
    async fn request_with_key_reaches_handler_with_body_intact() {
        let handler = Handler::echoing();
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(response) = service.oneshot(post(Some(KEY), "payload")).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(&body(response).await[..], b"payload");
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn request_body_over_cap_is_rejected() {
        let handler = Handler::echoing();
        let service = IdempotencyLayer::new(store())
            .max_body_size(4)
            .layer(handler.clone());

        let Ok(response) = service.oneshot(post(Some(KEY), "12345")).await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(rejection(&response), Some("body-too-large"));
        assert_eq!(handler.runs(), 0);
    }

    #[tokio::test]
    async fn request_body_failure_is_client_error() {
        let handler = Handler::echoing();
        let service = IdempotencyLayer::new(store()).layer(handler.clone());
        let request = Request::post("/credentials")
            .header(DEFAULT_HEADER, KEY)
            .body(TestBody::Failing)
            .expect("valid request");

        let Ok(response) = service.oneshot(request).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(rejection(&response), Some("request-body-failed"));
        assert_eq!(handler.runs(), 0);
    }

    #[tokio::test]
    async fn concurrent_request_on_held_key_is_in_flight() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler = Handler::gated(&started, &release);
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let first = tokio::spawn(service.clone().oneshot(post(Some(KEY), "{}")));
        started.notified().await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;
        release.notify_one();
        let Ok(first) = first.await.expect("first request");

        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(rejection(&second), Some("in-flight"));
        assert_eq!(retry_after(&second), Some(&b"1"[..]));
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn retry_replays_cached_response_without_rerunning_handler() {
        let handler = Handler::new(|_| async {
            Response::builder()
                .status(StatusCode::CREATED)
                .header("x-charge-id", "ch_1")
                .header(header::CONNECTION, "close")
                .header(header::DATE, "Thu, 01 Jan 1970 00:00:00 GMT")
                .body(TestBody::from("issued"))
                .expect("valid response")
        });
        let service = IdempotencyLayer::new(store())
            .header(HeaderName::from_static("x-request-id"))
            .layer(handler.clone());
        let request = || {
            Request::post("/credentials")
                .header("x-request-id", KEY)
                .body(TestBody::from("{}"))
                .expect("valid request")
        };

        let Ok(first) = service.clone().oneshot(request()).await;
        let Ok(second) = service.oneshot(request()).await;

        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(!replayed(&first));
        assert_eq!(second.status(), StatusCode::CREATED);
        assert!(replayed(&second));
        assert_eq!(
            second
                .headers()
                .get("x-charge-id")
                .map(HeaderValue::as_bytes),
            Some(&b"ch_1"[..])
        );
        assert!(!second.headers().contains_key(header::CONNECTION));
        assert!(!second.headers().contains_key(header::DATE));
        assert_eq!(&body(second).await[..], b"issued");
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn key_reused_with_different_body_is_rejected() {
        let handler = Handler::answering(StatusCode::CREATED, "issued");
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(_first) = service
            .clone()
            .oneshot(post(Some(KEY), r#"{"amount": 1}"#))
            .await;
        let Ok(second) = service.oneshot(post(Some(KEY), r#"{"amount": 2}"#)).await;

        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        assert_eq!(rejection(&second), Some("fingerprint-mismatch"));
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn failed_response_is_cached_and_replayed() {
        let handler = Handler::answering(StatusCode::INTERNAL_SERVER_ERROR, "signer down");
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(second.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(replayed(&second));
        assert_eq!(&body(second).await[..], b"signer down");
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn no_store_response_is_not_cached_and_frees_key() {
        let handler = Handler::new(|_| async {
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CACHE_CONTROL, "no-store")
                .body(TestBody::from("invalid offer"))
                .expect("valid response")
        });
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::BAD_REQUEST);
        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        assert!(!replayed(&second));
        assert_eq!(handler.runs(), 2);
    }

    #[tokio::test]
    async fn response_over_cache_cap_is_returned_and_keeps_claim() {
        let handler = Handler::answering(StatusCode::OK, "1234567");
        let service = IdempotencyLayer::new(store())
            .max_body_size(4)
            .layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "")).await;

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(&body(first).await[..], b"1234567");
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn response_body_failure_is_server_error_and_keeps_claim() {
        let handler = Handler::new(|_| async { Response::new(TestBody::Failing) });
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(rejection(&first), Some("response-body-failed"));
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn disconnected_client_does_not_cancel_handler() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler = Handler::gated(&started, &release);
        let service = IdempotencyLayer::new(store()).layer(handler.clone());

        let client = tokio::spawn(service.clone().oneshot(post(Some(KEY), "{}")));
        started.notified().await;
        client.abort();
        release.notify_one();

        let replay = loop {
            let Ok(response) = service.clone().oneshot(post(Some(KEY), "{}")).await;
            if replayed(&response) {
                break response;
            }
            assert_eq!(response.status(), StatusCode::CONFLICT);
            tokio::task::yield_now().await;
        };
        assert_eq!(&body(replay).await[..], b"done");
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_handler_keeps_claim_through_renewal() {
        let handler = Handler::new(|_| async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            text(StatusCode::CREATED, "issued")
        });
        let service = IdempotencyLayer::new(store())
            .processing_ttl(Duration::from_secs(1))
            .layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(replayed(&second));
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn handler_past_the_ceiling_completes_fenced() {
        let handler = Handler::new(|_| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            text(StatusCode::CREATED, "issued")
        });
        let service = IdempotencyLayer::new(store())
            .processing_ttl(Duration::from_secs(1))
            .keep_alive(Duration::from_secs(2))
            .layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(&body(first).await[..], b"issued");
        assert!(!replayed(&second));
        assert_eq!(handler.runs(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn store_timeout_is_service_unavailable() {
        let store = SwitchedStore::new();
        store.hang_claims.store(true, Ordering::SeqCst);
        let handler = Handler::answering(StatusCode::OK, "done");
        let service = IdempotencyLayer::new(store)
            .store_timeout(Duration::from_secs(1))
            .layer(handler.clone());

        let Ok(response) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rejection(&response), Some("store-error"));
        assert_eq!(retry_after(&response), Some(&b"5"[..]));
        assert_eq!(handler.runs(), 0);
    }

    #[tokio::test]
    async fn completion_failure_returns_the_response() {
        let store = SwitchedStore::new();
        store.fail_completions.store(true, Ordering::SeqCst);
        let handler = Handler::answering(StatusCode::CREATED, "issued");
        let service = IdempotencyLayer::new(store).layer(handler.clone());

        let Ok(first) = service.clone().oneshot(post(Some(KEY), "{}")).await;
        let Ok(second) = service.oneshot(post(Some(KEY), "{}")).await;

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(&body(first).await[..], b"issued");
        assert_eq!(second.status(), StatusCode::CONFLICT);
        assert_eq!(handler.runs(), 1);
    }

    #[tokio::test]
    async fn in_flight_cap_sheds_load() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler = Handler::gated(&started, &release);
        let service = IdempotencyLayer::new(store())
            .max_in_flight(1)
            .layer(handler.clone());

        let first = tokio::spawn(service.clone().oneshot(post(Some(KEY), "{}")));
        started.notified().await;
        let Ok(shed) = service
            .clone()
            .oneshot(post(Some("another-key"), "{}"))
            .await;
        release.notify_one();
        let Ok(first) = first.await.expect("first request");
        release.notify_one();
        let Ok(third) = service.oneshot(post(Some("third-key"), "{}")).await;

        assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rejection(&shed), Some("overloaded"));
        assert_eq!(retry_after(&shed), Some(&b"1"[..]));
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(third.status(), StatusCode::OK);
        assert_eq!(handler.runs(), 2);
    }

    #[tokio::test]
    async fn tracker_waits_for_detached_work() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let layer = IdempotencyLayer::new(store());
        let service = layer.layer(Handler::gated(&started, &release));

        let client = tokio::spawn(service.oneshot(post(Some(KEY), "{}")));
        started.notified().await;
        client.abort();
        let tracker = layer.tracker();
        tracker.close();
        let drained = tokio::spawn(async move { tracker.wait().await });
        tokio::task::yield_now().await;

        assert_eq!(layer.in_flight(), 1);
        assert!(!drained.is_finished());

        release.notify_one();
        drained.await.expect("drain");

        assert_eq!(layer.in_flight(), 0);
    }
}
