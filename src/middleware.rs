//! Tower middleware for HTTP services.

#[cfg(feature = "axum")]
mod axum;
mod layer;
pub mod rejection;

#[doc(inline)]
pub use self::layer::IdempotencyLayer;
#[doc(inline)]
pub use self::layer::IdempotencyService;
#[doc(inline)]
pub use self::layer::ResponseFuture;
#[doc(inline)]
pub use self::layer::stored_key;
#[doc(inline)]
pub use self::rejection::IdempotencyRejection;
