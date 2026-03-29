use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use super::error::ValkeyError;
use crate::entry::CachedResponse;
use crate::entry::Completed;
use crate::entry::ExistingEntry;
use crate::entry::IdempotencyEntry;
use crate::entry::Processing;
use crate::fingerprint::Fingerprint;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEntry {
    status: WireStatus,
    fingerprint: Fingerprint,
    ttl: Duration,
    response: Option<CachedResponse>,
}

impl WireEntry {
    /// Serializes the wire entry with a version byte prefix followed by the payload.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ValkeyError> {
        let payload = postcard::to_allocvec(self)?;
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(WIRE_VERSION);
        buf.extend_from_slice(&payload);
        Ok(buf)
    }
}

impl From<&IdempotencyEntry<Processing>> for WireEntry {
    fn from(entry: &IdempotencyEntry<Processing>) -> Self {
        Self {
            status: WireStatus::Processing,
            fingerprint: entry.fingerprint,
            response: None,
            ttl: entry.ttl,
        }
    }
}

impl From<&IdempotencyEntry<Completed>> for WireEntry {
    fn from(entry: &IdempotencyEntry<Completed>) -> Self {
        Self {
            status: WireStatus::Complete,
            fingerprint: entry.fingerprint,
            response: Some(entry.response().clone()),
            ttl: entry.ttl,
        }
    }
}

/// Current wire version
const WIRE_VERSION: u8 = WireVersion::V1 as u8;

impl TryFrom<&[u8]> for WireEntry {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "WireEntry::try_from", skip(bytes), err(Debug))
    )]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (&version, payload) = bytes
            .split_first()
            .ok_or_else(|| ValkeyError::Decode("empty wire data".into()))?;
        match version {
            WIRE_VERSION => Ok(postcard::from_bytes(payload)?),
            v => Err(ValkeyError::Decode(
                format!("unknown wire version: {v}").into(),
            )),
        }
    }
}

impl TryFrom<WireEntry> for ExistingEntry {
    type Error = ValkeyError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "ExistingEntry::try_from", err(Debug))
    )]
    fn try_from(wire: WireEntry) -> Result<Self, ValkeyError> {
        match wire.status {
            WireStatus::Processing => {
                let entry = IdempotencyEntry::new(wire.fingerprint, wire.ttl);
                Ok(ExistingEntry::Processing(entry))
            }
            WireStatus::Complete => {
                let response = wire.response.ok_or_else(|| {
                    ValkeyError::Decode("completed entry missing response".into())
                })?;
                let entry = IdempotencyEntry::new(wire.fingerprint, wire.ttl).complete(response);
                Ok(ExistingEntry::Completed(entry))
            }
        }
    }
}

/// Entry state as persisted in the store.
///
/// It maps the idempotency entry typestate variants reconstructed in [`ExistingEntry`]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
enum WireStatus {
    Complete,
    Processing,
}

/// Schema version for the wire format
///
/// The version is bumped when changing [`WireEntry`] fields to
/// support rolling deploys where old and new nodes coexist.
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
enum WireVersion {
    V1 = 1,
}
