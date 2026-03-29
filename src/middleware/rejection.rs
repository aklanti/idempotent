use crate::Error;

/// The rejection the layer returns instead of calling the handler.
#[non_exhaustive]
pub enum IdempotencyRejection {
    /// Malformed idempotency key.
    InvalidKey(Error),

    /// Another request with the same key inflight.
    InFlight,

    /// The key is reused with different request.
    FingerprintMismatch,

    /// The request body exceeds the buffer cap.
    BodyTooLarge,

    /// The store is unavailable
    StoreError,
}
