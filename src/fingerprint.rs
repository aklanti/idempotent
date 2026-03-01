//! This module defines the request fingerprint type

use xxhash_rust::xxh3;

///  A request fingerprint computed from the method, route and response's body
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fingerprint(pub(crate) u64);

/// Fingerprint computation strategy
pub trait FingerprintStrategy: Send + Sync + 'static {
    /// Compute fingerprint
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint;
}

/// The default fingerprint  strategy
pub struct DefaultFingerprintStrategy;

impl FingerprintStrategy for DefaultFingerprintStrategy {
    fn compute(&self, operation: &str, body: &[u8]) -> Fingerprint {
        let inner = xxh3::xxh3_64(&[operation.as_bytes(), b":", body].concat());
        Fingerprint(inner)
    }
}
