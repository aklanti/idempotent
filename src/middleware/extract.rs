use http::HeaderName;
use http::request::Parts;

use crate::Error;
use crate::IdempotencyKey;

/// Extractor of the idempotency key from a request.
pub trait ExtractIdempotencyKey: Send + Sync + 'static {
    /// Extract the idempotency key from request metadata.
    fn extract_from_parts(&self, parts: &Parts) -> Option<Result<IdempotencyKey, Error>>;

    /// Specifies whether to buffer and extract the idempotency key from the body.
    fn body_fallback(&self) -> bool {
        false
    }

    /// Extract the key from the buffered body.
    fn extract_from_body(&self, _body: &[u8]) -> Option<Result<IdempotencyKey, Error>> {
        None
    }
}

/// Idempotency key metadata extractor from a request header.
pub struct HeaderKeyExtractor {
    header: HeaderName,
}

impl HeaderKeyExtractor {
    /// Reads the key from the given header.
    pub const fn new(header: HeaderName) -> Self {
        Self { header }
    }
}

impl Default for HeaderKeyExtractor {
    fn default() -> Self {
        Self::new(HeaderName::from_static("idempotency-key"))
    }
}

impl ExtractIdempotencyKey for HeaderKeyExtractor {
    fn extract_from_parts(&self, parts: &Parts) -> Option<Result<IdempotencyKey, Error>> {
        let value = parts.headers.get(&self.header)?;
        let value = value
            .to_str()
            .map(IdempotencyKey::new)
            .map_err(|_| Error::InvalidKey)
            .flatten();
        Some(value)
    }
}

/// Idempotency key body extractor.
pub struct BodyFieldKeyExtractor<F> {
    accessor: F,
}

impl<F> BodyFieldKeyExtractor<F> {
    /// Creates an extractor from the body accessor.
    pub const fn new(accessor: F) -> Self {
        Self { accessor }
    }
}

impl<F> ExtractIdempotencyKey for BodyFieldKeyExtractor<F>
where
    F: Fn(&[u8]) -> Option<String> + Send + Sync + 'static,
{
    fn extract_from_parts(&self, _: &Parts) -> Option<Result<IdempotencyKey, Error>> {
        None
    }

    fn body_fallback(&self) -> bool {
        true
    }

    fn extract_from_body(&self, body: &[u8]) -> Option<Result<IdempotencyKey, Error>> {
        let value = (self.accessor)(body)?;
        Some(IdempotencyKey::new(value))
    }
}
