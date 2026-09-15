//! Idempotency entry types.
//!
//! These types are storage-agnostic and hold no timestamp or persistence concerns.
use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "json")]
use serde::Serialize;
#[cfg(feature = "json")]
use serde::de::DeserializeOwned;

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
    pub const fn new(fingerprint: Fingerprint, ttl: Duration) -> Self {
        Self {
            state: Processing,
            fingerprint,
            ttl,
        }
    }

    /// Completes this entry, consuming it and returning a `Completed` entry.
    ///
    /// The completed entry has `completed_ttl` as its replay lease.
    ///
    /// # Examples:
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use idempotent::{CachedResponse, IdempotencyEntry, Metadata};
    /// # use idempotent::fingerprint::{DefaultFingerprintStrategy, FingerprintStrategy};
    /// let fingerprint = DefaultFingerprintStrategy.compute(&"/get".into(), &[2]);
    /// let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(30));
    /// let response = CachedResponse::new(200, Metadata::default(), vec![].into());
    /// let _ = entry.complete(response, Duration::from_secs(86_400));
    /// ```
    #[must_use]
    pub const fn complete(
        self,
        response: CachedResponse,
        completed_ttl: Duration,
    ) -> IdempotencyEntry<Completed> {
        IdempotencyEntry {
            fingerprint: self.fingerprint,
            ttl: completed_ttl,
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

/// A cached response for a completed idempotency entry.
#[non_exhaustive]
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

impl CachedResponse {
    /// Creates a cached response from its status code, metadata, and body.
    pub const fn new(status_code: u16, metadata: Metadata, body: Bytes) -> Self {
        Self {
            status_code,
            metadata,
            body,
        }
    }
}

/// What a side effect produces and how it is cached.
///
/// A [`CachedResponse`] is cached as it stands. [`Json`] puts any serialisable value in the
/// response body, so a side effect can hand back its own type.
pub trait Cacheable: Sized {
    /// The error when converting to or from a cached response.
    type Error: Into<Box<dyn std::error::Error + Send + Sync>>;

    /// Builds the response to cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be encoded.
    fn to_response(&self) -> Result<CachedResponse, Self::Error>;

    /// Reads the value back from a cached response.
    ///
    /// # Errors
    ///
    /// Returns an error if the response cannot be decoded.
    fn from_response(response: CachedResponse) -> Result<Self, Self::Error>;
}

impl Cacheable for CachedResponse {
    type Error = Infallible;

    fn to_response(&self) -> Result<Self, Infallible> {
        Ok(self.clone())
    }

    fn from_response(response: Self) -> Result<Self, Infallible> {
        Ok(response)
    }
}

/// A value cached as JSON in the response body.
#[cfg(feature = "json")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Json<T>(pub T);

#[cfg(feature = "json")]
impl<T: Serialize + DeserializeOwned> Cacheable for Json<T> {
    type Error = serde_json::Error;

    fn to_response(&self) -> Result<CachedResponse, serde_json::Error> {
        let body = serde_json::to_vec(&self.0)?;
        Ok(CachedResponse::new(200, Metadata::new(), body.into()))
    }

    fn from_response(response: CachedResponse) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(&response.body).map(Self)
    }
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

/// A response to a retry from the entry with the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// The cached response of the same request.
    Replayed(CachedResponse),
    /// The same request is still being processed.
    InFlight,
    /// A different request holds the key.
    FingerprintMismatch,
}

impl ExistingEntry {
    /// Answers a retry whose request has `fingerprint`.
    ///
    /// A completed entry with the same fingerprint replays its response, a processing entry
    /// with the same fingerprint is in flight, and any other entry is a mismatch.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use idempotent::{IdempotencyEntry, ReplayOutcome};
    /// # use idempotent::entry::ExistingEntry;
    /// # use idempotent::fingerprint::{DefaultFingerprintStrategy, FingerprintStrategy};
    /// let fingerprint = DefaultFingerprintStrategy.compute(&"POST /issue_credential".into(), b"{}");
    /// let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(30));
    ///
    /// let existing = ExistingEntry::Processing(entry);
    /// assert_eq!(existing.replay(fingerprint), ReplayOutcome::InFlight);
    /// ```
    pub fn replay(self, fingerprint: Fingerprint) -> ReplayOutcome {
        match self {
            Self::Completed(entry) if entry.fingerprint == fingerprint => {
                ReplayOutcome::Replayed(entry.into_response())
            }
            Self::Processing(entry) if entry.fingerprint == fingerprint => ReplayOutcome::InFlight,
            _ => ReplayOutcome::FingerprintMismatch,
        }
    }
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

    use googletest::expect_that;
    use googletest::gtest;
    use googletest::matchers::eq;

    use super::*;

    #[gtest]
    fn complete_keeps_fingerprint_and_takes_completed_ttl() {
        let fingerprint = Fingerprint(0x1ab950a);
        let entry = IdempotencyEntry::new(fingerprint, Duration::from_secs(30));
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };

        let completed = entry.complete(response.clone(), Duration::from_secs(60));

        expect_that!(completed.fingerprint, eq(fingerprint));
        expect_that!(completed.ttl, eq(Duration::from_secs(60)));
        expect_that!(completed.state, eq(&Completed { response }));
    }

    #[gtest]
    fn replay_answers_all_three_cases() {
        let fingerprint = Fingerprint(1);
        let response = CachedResponse {
            status_code: 200,
            metadata: Metadata::default(),
            body: vec![].into(),
        };
        let completed = IdempotencyEntry::new(fingerprint, Duration::from_secs(30))
            .complete(response.clone(), Duration::from_secs(60));
        let processing = IdempotencyEntry::new(fingerprint, Duration::from_secs(30));

        expect_that!(
            ExistingEntry::Completed(completed.clone()).replay(fingerprint),
            eq(&ReplayOutcome::Replayed(response))
        );
        expect_that!(
            ExistingEntry::Processing(processing).replay(fingerprint),
            eq(&ReplayOutcome::InFlight)
        );
        expect_that!(
            ExistingEntry::Completed(completed).replay(Fingerprint(2)),
            eq(&ReplayOutcome::FingerprintMismatch)
        );
    }
}
