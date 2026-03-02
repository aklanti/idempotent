//! This crate provides an idempotency abstraction.
//!
//! The fundamental invariant is that for a given idempotency key, the side effect must be executed **at most once**,
//! and the response MUST be returned **at least once** (to every request bearing that key within the TTL window)
#![cfg_attr(docsrs, doc(cfg))]

pub mod config;
pub mod entry;
pub mod error;
pub mod fingerprint;
pub mod key;
pub mod store;

#[doc(inline)]
pub use entry::{CachedResponse, IdempotencyEntry, Metadata};
#[doc(inline)]
pub use fingerprint::Fingerprint;
#[doc(inline)]
pub use key::IdempotencyKey;
