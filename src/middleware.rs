//! Tower middleware for HTTP services.
//!
//! [`IdempotencyLayer`] wraps a service so that a request with an idempotency key is handle once
//! and replays after. The key comes from one header, idempotency-key unless the layer is told
//! otherwise. A request without one is forwarded untouched, and so is any request with a safe
//! method, since there is no side effect to protect.
//!
//! # What a request with a key gets
//!
//! The body is buffered under the size cap and fingerprinted with the method, the path, the
//! query, and the body. The key is then claimed.
//! A retry with the same fingerprint gets the cached response with idempotent-replayed set to
//! true. A retry while the first request still runs gets 409 with a retry-after of one second, and
//! a key reused with a different request gets 400. Every rejection is plain text with an
//! idempotent-rejection header carrying its code, listed on [`IdempotencyRejection`].
//!
//! The handler runs in its own task, so a client that disconnects cannot cancel it mid side
//! effect, and the processing lease is renewed while it runs. Everything the handler returns is
//! cached and replayed, failures included, because the layer cannot tell a failure after a
//! partial side effect from a transient one, and a client that wants a fresh attempt sends a
//! new key. The one exception is a response marked no-store in its cache-control header, which
//! a handler sets when it rejected the request before doing anything. The layer then frees the
//! key, so a corrected retry runs at once. A streaming response, one over the cap, or one whose
//! body fails to read is returned as it came and the claim is left to expire, so a retry gets
//! 409 until the lease ends rather than a second run.
//!
//! # Keys and scope
//!
//! The cached response goes to whoever presents the key with the same fingerprint, so keys must
//! be unguessable, UUID v4 or better, and a service with more than one client must scope them
//! by the caller with [`IdempotencyLayer::scope`], or one client can collect another's response
//! by observing a key. The scope is stored as a hash, never as the identifier itself.
//! [`stored_key`] computes the key the store holds, which is the one to
//! [`purge`](crate::IdempotencyStore::purge) when a cached failure must go before its lease
//! ends.
//!
//! # Operating it
//!
//! The cache holds, at steady state, the request rate times the completed lease times the
//! average cached response size, and eviction is off by requirement. At one request per
//! second, a day's lease, and 10 KiB responses that is about 860 MiB. The completed lease and
//! the body cap are the levers.
//!
//! A concurrency limit or a timeout placed outside this layer releases its permit when the
//! response future is dropped while the handler keeps running in its task, so such limits bound
//! real work only between this layer and the handler. [`IdempotencyLayer::max_in_flight`] is
//! the one bound that sees the tasks. The memory store deduplicates within one process only.
//!
//! Make the side effect idempotent downstream where you can. Derive the credential identifier,
//! the database row, or the signer's request id from the key, so the re-run this layer cannot
//! prevent, after a crash between the side effect and the completion, becomes an upsert rather
//! than a duplicate.
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
