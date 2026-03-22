//! Idempotency configuration.

use std::sync::Arc;
use std::time::Duration;

use crate::fingerprint::FingerprintStrategy;

/// Configuration for idempotency behaviour.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub struct IdempotencyConfig {
    /// Time-to-live for completed entries.
    pub ttl: Duration,

    /// Custom fingerprint strategy. When `None`, [`DefaultFingerprintStrategy`] is used.
    ///
    /// [`DefaultFingerprintStrategy`]: crate::fingerprint::DefaultFingerprintStrategy
    #[cfg_attr(feature = "serde", serde(skip))]
    pub fingerprint_strategy: Option<Arc<dyn FingerprintStrategy>>,
}
