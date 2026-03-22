//! Idempotency entry types.
//!
//! These types are storage-agnostic and carry no timestamp or persistence concerns.
use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;

use super::fingerprint::Fingerprint;

/// An idempotency entry, parameterised by [`Processing`] or [`Completed`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IdempotencyEntry<State: EntryState> {
    /// Hash of the original request, used to detect key reuse with a different body.
    pub fingerprint: Fingerprint,
    /// Time to live for this entry
    pub ttl: Duration,
    /// The current processing state of the request with this entry
    state: State,
}

impl IdempotencyEntry<Processing> {
    /// Creates a new idempotency entry in processing state
    #[must_use]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "IdempotencyEntry::new", level=tracing::Level::INFO)
    )]
    pub fn new(fingerprint: Fingerprint, ttl: Duration) -> Self {
        Self {
            state: Processing::new(),
            fingerprint,
            ttl,
        }
    }

    /// Completes this entry, consuming it and returning a `Completed` entry.
    ///
    /// # Examples:
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use idempotent::{CachedResponse, IdempotencyEntry, Metadata};
    /// # use idempotent::fingerprint::{DefaultFingerprintStrategy, FingerprintStrategy};
    /// let fingerprint = DefaultFingerprintStrategy.compute("/get", &[2]);
    /// let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(2));
    /// let response = CachedResponse {
    ///     status_code: 200,
    ///     metadata: Metadata::default(),
    ///     body: vec![].into(),
    /// };
    /// let _ = entry.complete(response);
    /// ```
    #[must_use]
    pub const fn complete(self, response: CachedResponse) -> IdempotencyEntry<Completed> {
        IdempotencyEntry {
            fingerprint: self.fingerprint,
            ttl: self.ttl,
            state: Completed { response },
        }
    }

    /// Returns the fencing token for this entry.
    #[must_use]
    pub const fn fencing_token(&self) -> FencingToken {
        self.state.fencing_token
    }
}

impl IdempotencyEntry<Completed> {
    /// Returns a reference to the cached response.
    pub const fn response(&self) -> &CachedResponse {
        &self.state.response
    }

    /// Consumes the entry, returning the cached response.
    pub fn into_response(self) -> CachedResponse {
        self.state.response
    }
}

impl<State: EntryState> IdempotencyEntry<State> {
    /// Returns `true` if `fingerprint` matches this entry's fingerprint.
    pub fn fingerprint_matches(&self, fingerprint: Fingerprint) -> bool {
        self.fingerprint == fingerprint
    }
}

/// A cached response for a completed idempotency entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CachedResponse {
    /// The status code.
    pub status_code: u16,
    /// Response metadata such as headers.
    pub metadata: Metadata,
    /// The response body.
    pub body: Bytes,
}

/// Response metadata stored as string-keyed byte values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metadata(HashMap<String, Vec<u8>>);

/// The request is currently being processed
///
/// A concurrent request with the same idempotency key will return a response
/// that indicates a conflict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Processing {
    /// A claim fencing token
    pub(crate) fencing_token: FencingToken,
}

impl Processing {
    /// Creates new processing state
    pub fn new() -> Self {
        Self {
            fencing_token: FencingToken::new(),
        }
    }
}

impl Default for Processing {
    fn default() -> Self {
        Self::new()
    }
}

/// An entry that already exists in the store.
#[derive(Debug, Clone)]
pub enum ExistingEntry {
    /// The request is still in flight.
    Processing(IdempotencyEntry<Processing>),
    /// The request has completed and the response is cached.
    Completed(IdempotencyEntry<Completed>),
}

impl EntryState for Processing {}
impl sealed::Sealed for Processing {}

/// A token generated when a key is claimed.
///
/// Prevents zombie completions from overwriting a reclaimed key's result.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FencingToken(pub(crate) u64);

impl FencingToken {
    /// Creates a new fencing token
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name="FencingToken::new", level=tracing::Level::DEBUG, ret)
    )]
    pub fn new() -> Self {
        Self(rand::random())
    }
}

impl Default for FencingToken {
    fn default() -> Self {
        Self::new()
    }
}

/// A completed entry state with a cached response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    response: CachedResponse,
}

impl EntryState for Completed {}
impl sealed::Sealed for Completed {}

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for valid entry states
///
/// This trait is sealed and cannot be implemented outside this crate
pub trait EntryState: sealed::Sealed {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use googletest::matchers::{eq, not, pat};
    use googletest::{assert_that, expect_that, gtest};

    use super::*;

    #[gtest]
    fn new_idempotency_entry_always_in_processing_state() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(1));
        expect_that!(entry.fingerprint, eq(fingerprint));
        expect_that!(entry.state, pat!(Processing { .. }));
    }

    #[gtest]
    fn new_idempotency_preserve_ttl() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(1));
        expect_that!(entry.ttl, eq(Duration::from_nanos(1)));
    }

    #[gtest]
    fn can_complete_processing_idempotency_entry() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(1));
        expect_that!(entry.fingerprint, eq(fingerprint));
        expect_that!(entry.state, pat!(Processing { .. }));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };
        let completed_entry = entry.complete(response.clone());

        let state = Completed { response };
        expect_that!(completed_entry.state, eq(&state));
    }

    #[gtest]
    fn entry_fingerprint_matches() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(1));
        assert_that!(entry.fingerprint_matches(fingerprint), eq(true))
    }

    #[gtest]
    fn complete_idempotency_entry_preserve_fingerprint() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_nanos(1));
        expect_that!(entry.fingerprint, eq(fingerprint));
        expect_that!(entry.state, pat!(Processing { .. }));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };
        let completed_entry = entry.complete(response.clone());
        expect_that!(completed_entry.fingerprint, eq(fingerprint));
    }

    #[gtest]
    fn complete_idempotency_entry_preserve_ttl() {
        let fingerprint = Fingerprint(0x1ab950a);
        let ttl = Duration::from_nanos(1);
        let entry = IdempotencyEntry::new(fingerprint, ttl);
        expect_that!(entry.fingerprint, eq(fingerprint));
        expect_that!(entry.state, pat!(Processing { .. }));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };
        let completed_entry = entry.complete(response.clone());
        expect_that!(completed_entry.ttl, eq(ttl));
    }

    #[gtest]
    fn fencing_token_is_unique() {
        let tok1 = FencingToken::new();
        let tok2 = FencingToken::new();
        expect_that!(tok1, not(eq(tok2)));
    }
}
