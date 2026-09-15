#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod claim;
pub mod entry;
pub mod error;
pub mod fencing_token;
pub mod fingerprint;
pub mod guard;
pub mod key;
pub mod metadata;
#[cfg(feature = "middleware")]
pub mod middleware;
pub mod store;

#[doc(inline)]
pub use self::claim::ClaimBuilder;
#[doc(inline)]
pub use self::claim::ClaimOutcome;
#[doc(inline)]
pub use self::claim::ExecutionError;
#[doc(inline)]
pub use self::claim::ExecutionOutcome;
#[doc(inline)]
#[cfg(feature = "json")]
pub use self::claim::JsonClaimBuilder;
#[doc(inline)]
pub use self::claim::OwnedClaimBuilder;
#[doc(inline)]
pub use self::claim::OwnedClaimOutcome;
#[doc(inline)]
#[cfg(feature = "json")]
pub use self::claim::OwnedJsonClaimBuilder;
#[doc(inline)]
pub use self::entry::Cacheable;
#[doc(inline)]
pub use self::entry::CachedResponse;
#[doc(inline)]
pub use self::entry::IdempotencyEntry;
#[doc(inline)]
#[cfg(feature = "json")]
pub use self::entry::Json;
#[doc(inline)]
pub use self::entry::ReplayOutcome;
#[doc(inline)]
pub use self::error::Error;
#[doc(inline)]
pub use self::fencing_token::FencedOutcome;
#[doc(inline)]
pub use self::fencing_token::FencingToken;
#[doc(inline)]
pub use self::fencing_token::Rejection;
#[doc(inline)]
pub use self::fingerprint::Fingerprint;
#[doc(inline)]
pub use self::fingerprint::Operation;
#[doc(inline)]
pub use self::guard::ClaimGuard;
#[doc(inline)]
pub use self::guard::OwnedClaimGuard;
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
