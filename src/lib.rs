//! At-most-once execution of side effects.
//!
//! For a given idempotency key the side effect runs at most once,
//! and every retry within the TTL window receives the cached response.
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod config;
pub mod entry;
pub mod error;
pub mod fingerprint;
pub mod key;
pub mod store;

#[doc(inline)]
pub use entry::CachedResponse;
#[doc(inline)]
pub use entry::IdempotencyEntry;
#[doc(inline)]
pub use entry::Metadata;
#[doc(inline)]
pub use fingerprint::Fingerprint;
#[doc(inline)]
pub use key::IdempotencyKey;
#[doc(inline)]
#[cfg(feature = "memory")]
pub use store::memory;
#[doc(inline)]
#[cfg(feature = "valkey")]
pub use store::valkey;
