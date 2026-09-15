//! Tower middleware for HTTP services.
//!
//! [`IdempotencyLayer`] wraps a service so that a request with an idempotency key is handled
//! once and replays after. The key comes from one header, idempotency-key unless the layer is
//! told otherwise. A request without one is forwarded untouched, and so is any request with a
//! safe method, since there is no side effect to protect.
//!
//! The wrapped service sees the request body it was given and returns its own response body.
//! Both are bounded by `From<Bytes>`, which axum's body and `Full<Bytes>` implement. hyper's
//! `Incoming` does not, so a plain hyper server maps its body first. Each request with a key
//! runs in a spawned task, so the service must be called inside a Tokio runtime.
//!
//! # What a request with a key gets
//!
//! The body is buffered under the size cap and fingerprinted with the method, the path, the
//! query, and the body. The key is then claimed. A retry with the same fingerprint gets the
//! cached response with idempotent-replayed set to true. A retry while the first request still
//! runs gets 409 with a retry-after of one second. A key reused with a different request gets
//! 400. Every rejection is plain text with an idempotent-rejection header that holds its code,
//! listed on [`IdempotencyRejection`].
//!
//! The handler runs in its own task, so a client that disconnects cannot cancel it mid side
//! effect, and the processing lease is renewed while it runs. Everything the handler returns is
//! cached and replayed, failures included. The layer cannot tell a failure after a partial side
//! effect from a transient one, and a client that wants a fresh attempt sends a new key. The one
//! exception is a response marked no-store in its cache-control header, which a handler sets
//! when it rejected the request before doing anything. The layer then frees the key, so a
//! corrected retry runs at once. A streaming response, one over the cap, or one whose body fails
//! to read is returned as it came, and the claim is left to expire. A retry then gets 409 until
//! the lease ends rather than a second run.
//!
//! A handler slower than its lease can still lose the key to another request. Sending its
//! response would then give two requests under one key two different responses. The layer sends
//! the cached response of the request that took the key instead, or 409 while that request is
//! still running. If the key is free, the response is cached under a fresh claim and returned as
//! usual. The handler's side effect has run either way, which the layer logs at warn.
//!
//! # Keys and scope
//!
//! The cached response goes to whoever presents the key with the same fingerprint, so keys must
//! be unguessable, UUID v4 or better. A service with more than one client scopes them by the
//! caller with [`IdempotencyLayer::scope`]. Without that, one client can collect another's
//! response by observing a key. The scope is stored as a hash, never as the identifier itself.
//! [`stored_key`] computes the key the store holds. That is the key to
//! [`purge`](crate::IdempotencyStore::purge) when a cached failure must go before its lease
//! ends.
//!
//! # Operating it
//!
//! At steady state the cache holds the request rate times the completed lease times the average
//! cached response size, and eviction is off by requirement. At one request per second, a day's
//! lease, and 10 KiB responses that is about 860 MiB. The completed lease and the body cap are
//! the levers.
//!
//! A concurrency limit or a timeout outside this layer releases its permit when the response
//! future is dropped. The handler keeps running in its task, so such limits bound real work only
//! between this layer and the handler. [`IdempotencyLayer::max_in_flight`] is the one bound that
//! sees the tasks. The memory store deduplicates within one process only.
//!
//! Make the side effect idempotent downstream where you can. Derive the credential identifier,
//! the database row, or the signer's request id from the key. A crash between the side effect
//! and the completion forces a re-run that this layer cannot prevent. Deriving from the key
//! turns that re-run into an upsert rather than a duplicate.
//!
//! # Shutdown
//!
//! Stop accepting connections, then close the [`tracker`](IdempotencyLayer::tracker) and wait
//! on it under a deadline, logging [`in_flight`](IdempotencyLayer::in_flight) while it waits.
//! A deploy that skips this cuts every running handler off and turns every retry into a second
//! run.

#[cfg(feature = "axum")]
mod axum;
mod layer;
pub mod rejection;

#[doc(inline)]
pub use self::layer::IdempotencyLayer;
#[doc(inline)]
pub use self::layer::IdempotencyService;
#[doc(inline)]
pub use self::layer::ResponseFuture;
#[doc(inline)]
pub use self::layer::stored_key;
#[doc(inline)]
pub use self::rejection::IdempotencyRejection;
