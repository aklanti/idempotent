//! This module defines the data structures that track the state of an idempotent request.
//!
//! These types are storage agnostic, and carry no timestamp or persistency concerns.
use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;

use super::fingerprint::Fingerprint;

/// An idempotency tracking entry
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IdempotencyEntry<State: EntryState> {
    /// A hash of the original request
    ///
    /// A fingerprint used to detect a reused key with different request body
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

    /// Creates a new idempotency entry in a completed state
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

    #[must_use]
    /// Return the fencing token
    pub const fn fencing_token(&self) -> FencingToken {
        self.state.fencing_token
    }
}

impl IdempotencyEntry<Completed> {
    /// Returns the response body of a completed request
    pub const fn response(&self) -> &CachedResponse {
        &self.state.response
    }

    /// Consumes `self` and returns inner response
    pub fn into_response(self) -> CachedResponse {
        self.state.response
    }
}

impl<State: EntryState> IdempotencyEntry<State> {
    /// Checks whether a request fingerprint matches this entry
    /// It returns false when a client reuses an idempotency key with a different request body
    pub fn fingerprint_matches(&self, fingerprint: Fingerprint) -> bool {
        self.fingerprint == fingerprint
    }
}

impl IdempotencyEntry<Completed> {}
/// A cached response with a completed idempotency key
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CachedResponse {
    /// Response status code
    pub status_code: u16,
    /// A response metadata
    pub metadata: Metadata,
    /// Raw response body
    pub body: Bytes,
}

/// A data structure representing a response metadata
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
    /// Creates new processing  state
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

/// Existing entry in the  idempotency store
#[derive(Debug, Clone)]
pub enum ExistingEntry {
    /// An in-flight request entry
    Processing(IdempotencyEntry<Processing>),
    /// A completed request entry
    Completed(IdempotencyEntry<Completed>),
}

impl EntryState for Processing {}
impl sealed::Sealed for Processing {}

/// A token generated when a key is claimed
///
/// It prevents the zombie completions from overwriting a reclaimed key's result.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FencingToken(u64);

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

/// The request handler has processed the request and the response is cached
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    /// A cached response for a completed request
    response: CachedResponse,
}

impl EntryState for Completed {}
impl sealed::Sealed for Completed {}

/// A private module for sealed trait
mod sealed {
    /// A sealed trait
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
    use googletest::{expect_that, gtest};

    use super::*;

    #[gtest]
    fn new_idempotency_entry_always_in_processing_state() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint.clone(), Duration::from_nanos(1));
        expect_that!(entry.fingerprint, eq(&fingerprint));
        expect_that!(entry.state, pat!(Processing { .. }));
    }

    #[gtest]
    fn can_complete_processing_idempotency_entry() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint.clone(), Duration::from_nanos(1));
        expect_that!(entry.fingerprint, eq(&fingerprint));
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
    fn fencing_token_is_unique() {
        let tok1 = FencingToken::new();
        let tok2 = FencingToken::new();
        expect_that!(tok1, not(eq(tok2)));
    }
}
