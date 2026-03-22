//! Error types for idempotency key validation.

/// Idempotency error
#[derive(Debug, thiserror::Error)]
pub enum IdempotencyError {
    /// The key is empty.
    #[error("idempotency key cannot be empty")]
    EmptyKey,
    /// The key exceeds the 255-byte maximum.
    #[error("idempotency key exceeds 255 bytes (got {0})")]
    KeyTooLong(usize),
}
