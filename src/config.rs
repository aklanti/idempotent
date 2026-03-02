//! This module defines the configuration data

use std::sync::Arc;
use std::time::Duration;

use crate::fingerprint::FingerprintStrategy;

/// Configuration type
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub struct IdempotencyConfig {
    /// TTL for idempotency entries
    pub ttl: Duration,

    /// A custom fingerprint algorithm to use
    #[cfg_attr(feature = "serde", serde(skip))]
    pub fingerprint_strategy: Option<Arc<dyn FingerprintStrategy>>,
}
