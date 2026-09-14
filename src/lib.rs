#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

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
pub use self::entry::CachedResponse;
#[doc(inline)]
pub use self::entry::IdempotencyEntry;
#[doc(inline)]
pub use self::entry::ReplayOutcome;
#[doc(inline)]
pub use self::error::Error;
#[doc(inline)]
pub use self::fencing_token::FencedOutcome;
#[doc(inline)]
pub use self::fencing_token::FencingToken;
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
pub use self::store::claim::ClaimBuilder;
#[doc(inline)]
pub use self::store::claim::ClaimOutcome;
#[doc(inline)]
pub use self::store::claim::ExecutionError;
#[doc(inline)]
pub use self::store::claim::ExecutionOutcome;
#[doc(inline)]
pub use self::store::claim::OwnedClaimBuilder;
#[doc(inline)]
pub use self::store::claim::OwnedClaimOutcome;
#[doc(inline)]
#[cfg(feature = "json")]
pub use self::store::claim::RunError;
#[doc(inline)]
#[cfg(feature = "memory")]
pub use self::store::memory;
#[doc(inline)]
#[cfg(feature = "valkey")]
pub use self::store::valkey;
