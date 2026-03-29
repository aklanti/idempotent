use redis::RedisError;

/// Errors returned by [`ValkeyStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ValkeyError {
    /// A connection or network error.
    #[error("connection error")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The stored entry could not be decoded.
    #[error("decode error")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The key prefix is invalid.
    #[error("service-name prefix {0:?} contains a reserved separator (':' or '/')")]
    InvalidPrefix(String),
}

impl From<RedisError> for ValkeyError {
    fn from(error: RedisError) -> Self {
        Self::Connection(Box::new(error))
    }
}

impl From<postcard::Error> for ValkeyError {
    fn from(error: postcard::Error) -> Self {
        Self::Decode(Box::new(error))
    }
}
