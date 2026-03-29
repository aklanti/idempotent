//! At-most-once execution of side effects.
//!
//! For a given idempotency key the side effect runs at most once,
//! and every retry within the TTL window receives the cached response.
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod entry;
pub mod error;
mod fencing_token;
pub mod fingerprint;
pub mod key;
pub mod metadata;
pub mod store;

#[doc(inline)]
pub use self::entry::CachedResponse;
#[doc(inline)]
pub use self::entry::IdempotencyEntry;
#[doc(inline)]
pub use self::error::Error;
#[doc(inline)]
pub use self::fencing_token::FencedOutcome;
#[doc(inline)]
pub use self::fingerprint::Fingerprint;
#[doc(inline)]
pub use self::key::IdempotencyKey;
#[doc(inline)]
pub use self::metadata::Metadata;
#[doc(inline)]
pub use self::store::IdempotencyStore;
#[doc(inline)]
pub use self::store::InsertResult;
#[doc(inline)]
#[cfg(feature = "memory")]
pub use self::store::memory;
#[doc(inline)]
#[cfg(feature = "valkey")]
pub use self::store::valkey;
