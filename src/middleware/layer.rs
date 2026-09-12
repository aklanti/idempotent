//! The HTTP idempotency middleware.

use bytes::Bytes;
use http::HeaderMap;
use http::HeaderName;
use http::header;
use http_body_util::BodyExt;
use http_body_util::LengthLimitError;
use http_body_util::Limited;
use xxhash_rust::xxh3::xxh3_128;

use crate::Error;
use crate::IdempotencyKey;
use crate::Metadata;

/// A boxed error, what the body traits deal in.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Returns the key the layer stores for a client's `key` when `scope` identifies the caller.
///
/// The stored key is the thirty-two hex digits of the scope's 128-bit hash, a slash, and the
/// client's key. Because the scope is hashed, a caller identifier may contain any character,
/// is never written to the store as itself, and prefixes every entry of that caller. The
/// stored key must fit 255 bytes, so `key` may be at most 222 bytes.
///
/// # Errors
///
/// Returns an error if the stored key exceeds 255 bytes.
pub fn stored_key(scope: &str, key: &IdempotencyKey) -> Result<IdempotencyKey, Error> {
    IdempotencyKey::new(format!("{:032x}", xxh3_128(scope.as_bytes())))?.scoped(key.as_str())
}

/// The `keep-alive` header, which has no constant in `http`.
const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");

/// Copies the headers worth replaying into [`Metadata`].
///
/// Hop-by-hop headers describe the connection that carried the response, and `date` is set
/// afresh by the server, so neither is stored.
fn storable_headers(headers: &HeaderMap) -> Metadata {
    headers
        .iter()
        .filter(|(name, _)| !is_connection_specific(name))
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                Bytes::copy_from_slice(value.as_bytes()),
            )
        })
        .collect()
}

/// Returns true if a header belongs to the connection rather than the response.
fn is_connection_specific(name: &HeaderName) -> bool {
    [
        header::CONNECTION,
        KEEP_ALIVE,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        header::DATE,
    ]
    .contains(name)
}

/// The result of buffering a request body under a cap.
enum Buffered {
    /// The body fit within the cap.
    Bytes(Bytes),
    /// The body exceeded the cap and was not buffered.
    TooLarge,
}

/// Collects `body` into memory, refusing to buffer more than `max` bytes.
///
/// # Errors
///
/// Returns an error if the body itself fails.
async fn buffer<B>(body: B, max: usize) -> Result<Buffered, BoxError>
where
    B: http_body::Body,
    B::Error: Into<BoxError>,
{
    match Limited::new(body, max).collect().await {
        Ok(collected) => Ok(Buffered::Bytes(collected.to_bytes())),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => Ok(Buffered::TooLarge),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use http_body_util::Full;

    use super::*;

    #[test]
    fn stored_key_hashes_the_scope() {
        let key = IdempotencyKey::new("cred-offer-123").expect("valid key");
        let alice = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        let again = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        let bob = stored_key("did:web:bob.example", &key).expect("a DID scopes");
        assert_eq!(alice, again);
        assert_ne!(alice, bob);
        assert!(alice.as_str().ends_with("/cred-offer-123"));
        assert_eq!(alice.as_str().len(), 32 + 1 + "cred-offer-123".len());

        let longest = IdempotencyKey::new("k".repeat(222)).expect("valid key");
        assert!(stored_key("tenant", &longest).is_ok());
        let too_long = IdempotencyKey::new("k".repeat(223)).expect("valid key");
        assert!(matches!(
            stored_key("tenant", &too_long),
            Err(Error::KeyTooLong(_))
        ));
    }

    #[test]
    fn storable_headers_drop_connection_headers_and_date() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert("x-charge-id", HeaderValue::from_static("ch_1"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(
            header::DATE,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"),
        );

        let stored = storable_headers(&headers);
        let names: Vec<&str> = stored.iter().map(|(name, _)| name).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"content-type"));
        assert!(names.contains(&"x-charge-id"));
    }

    #[tokio::test]
    async fn buffer_accepts_a_body_at_the_cap_and_rejects_over_it() {
        let Ok(Buffered::Bytes(bytes)) = buffer(Full::new(Bytes::from_static(b"12345")), 5).await
        else {
            panic!("a body at the cap must buffer");
        };
        assert_eq!(bytes.len(), 5);
        assert!(matches!(
            buffer(Full::new(Bytes::from_static(b"123456")), 5).await,
            Ok(Buffered::TooLarge)
        ));
    }
}
