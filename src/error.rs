//! Error types for idempotency key validation.

/// Idempotency error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The key is empty.
    #[error("idempotency key cannot be empty")]
    EmptyKey,

    /// A derived-key scope segment is empty.
    #[error("scope segment cannot be empty")]
    EmptyScope,

    /// The key exceeds the 255-byte maximum.
    #[error("idempotency key exceeds 255 bytes (got {0})")]
    KeyTooLong(usize),

    /// Invalid idempotency key.
    #[error("idempotency key contains a control char or a reserved separator (':' or '/')")]
    InvalidKey,

    /// A scope segment contains a control char or a reserved separator.
    #[error("scope segment contains a control char or a reserved separator (':' or '/')")]
    InvalidScope,

    /// The fencing token is invalid.
    #[error("negative fencing token")]
    NegativeFencingToken,

    /// The fencing outcome is unexpected.
    #[error("unexpected fenced outcome: {0}")]
    UnexpectedFencedOutcome(i64),
}
