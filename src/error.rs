//! Idempotency error type

/// Idempotency error
#[derive(Debug, thiserror::Error)]
pub enum IdempotencyError {
    /// The idempotency key is invalid
    #[error("invalid idempotency key: {0}")]
    InvalidKey(String),
}
