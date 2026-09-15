//! Errors from validating an idempotency key or a scope.

/// The error returned by fallible idempotency operations.
#[non_exhaustive]
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

    /// The key contains a control character or a reserved separator.
    #[error("idempotency key contains a control character or a reserved separator (':' or '/')")]
    InvalidKey,

    /// A scope segment contains a control character or a reserved separator.
    #[error("scope segment contains a control character or a reserved separator (':' or '/')")]
    InvalidScope,
}
