//! Idempotency entry types.
//!
//! These types are storage-agnostic and carry no timestamp or persistence concerns.
use std::time::Duration;

use bytes::Bytes;

use crate::Fingerprint;
use crate::Metadata;

/// An idempotency entry, parameterised by [`Processing`] or [`Completed`].
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IdempotencyEntry<State: EntryState> {
    /// Hash of the original request, used to detect key reuse with a different body.
    pub fingerprint: Fingerprint,
    /// Time to live for this entry.
    pub ttl: Duration,
    /// The current processing state of the request with this entry.
    state: State,
}

impl IdempotencyEntry<Processing> {
    /// Creates a new idempotency entry in processing state.
    #[must_use]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "IdempotencyEntry::new",
            level=tracing::Level::INFO,
        )
    )]
    pub fn new(fingerprint: Fingerprint, ttl: Duration) -> Self {
        Self {
            state: Processing,
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

/// The request is currently being processed
///
/// A concurrent request with the same idempotency key will return a response
/// that indicates a conflict.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Processing;

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

/// A completed entry state with a cached response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    response: CachedResponse,
}

/// Marker trait for valid entry states.
///
/// This trait is sealed and cannot be implemented outside this crate
pub trait EntryState: sealed::Sealed {}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for Completed {}

impl EntryState for Completed {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use googletest::assert_that;
    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::eq;
    use googletest::matchers::pat;

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
        expect_that!(entry.state, pat!(Processing));
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
        expect_that!(entry.state, pat!(Processing));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };
        let completed_entry = entry.complete(response.clone());
        expect_that!(completed_entry.ttl, eq(ttl));
    }
}
