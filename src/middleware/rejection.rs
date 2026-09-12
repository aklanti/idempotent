//! The responses the layer sends on its own.

use bytes::Bytes;
use http::HeaderValue;
use http::Response;
use http::StatusCode;
use http::header;

use crate::Error;

/// A response the layer sends on its own, in place of the handler or after it ran.
#[non_exhaustive]
#[derive(Debug)]
pub enum IdempotencyRejection {
    /// The idempotency key is malformed.
    InvalidKey(Error),
    /// No idempotency key was present on a request that requires one.
    MissingKey,
    /// A scope hook is set and returned nothing for the request.
    MissingScope,
    /// The request body failed while it was being read.
    RequestBodyFailed,
    /// The key was already used with a different request.
    FingerprintMismatch,
    /// The request body exceeds the buffer cap.
    BodyTooLarge,
    /// Another request with the same key is still running.
    InFlight,
    /// The handler's response body failed while it was being read for the cache.
    ResponseBodyFailed,
    /// The cap on requests in flight is reached.
    Overloaded,
    /// The store could not be reached, or did not answer in time.
    StoreError,
    /// The runtime shut down before the handler finished.
    Shutdown,
}

impl IdempotencyRejection {
    /// Returns the status code the rejection is sent with.
    pub const fn status(&self) -> StatusCode {
        match self {
            Self::InvalidKey(_)
            | Self::MissingKey
            | Self::MissingScope
            | Self::RequestBodyFailed
            | Self::FingerprintMismatch => StatusCode::BAD_REQUEST,
            Self::BodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InFlight => StatusCode::CONFLICT,
            Self::ResponseBodyFailed => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Overloaded | Self::StoreError | Self::Shutdown => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// Returns the value of the `idempotent-rejection` header, the variant in kebab case.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidKey(_) => "invalid-key",
            Self::MissingKey => "missing-key",
            Self::MissingScope => "missing-scope",
            Self::RequestBodyFailed => "request-body-failed",
            Self::FingerprintMismatch => "fingerprint-mismatch",
            Self::BodyTooLarge => "body-too-large",
            Self::InFlight => "in-flight",
            Self::ResponseBodyFailed => "response-body-failed",
            Self::Overloaded => "overloaded",
            Self::StoreError => "store-error",
            Self::Shutdown => "shutdown",
        }
    }

    /// Returns the `retry-after` value in seconds, for the rejections a client should retry.
    pub const fn retry_after(&self) -> Option<u32> {
        match self {
            Self::InFlight | Self::Overloaded => Some(1),
            Self::StoreError | Self::Shutdown => Some(5),
            _ => None,
        }
    }

    /// Renders the rejection as a plain-text response.
    pub(crate) fn render<B: From<Bytes>>(self) -> Response<B> {
        let message = match &self {
            Self::InvalidKey(error) => error.to_string(),
            Self::MissingKey => "this endpoint requires an idempotency key".to_owned(),
            Self::MissingScope => "the idempotency key cannot be scoped to a caller".to_owned(),
            Self::RequestBodyFailed => "the request body could not be read".to_owned(),
            Self::FingerprintMismatch => {
                "this idempotency key was already used with a different request".to_owned()
            }
            Self::BodyTooLarge => "request body exceeds the idempotency buffer limit".to_owned(),
            Self::InFlight => {
                "a request with this idempotency key is already being processed".to_owned()
            }
            Self::ResponseBodyFailed => {
                "the response could not be read after the handler ran".to_owned()
            }
            Self::Overloaded => "too many idempotent requests are running".to_owned(),
            Self::StoreError => "the idempotency store is unavailable".to_owned(),
            Self::Shutdown => "the server is shutting down".to_owned(),
        };
        let mut response = Response::new(B::from(Bytes::from(message)));
        *response.status_mut() = self.status();
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        headers.insert(
            "idempotent-rejection",
            HeaderValue::from_static(self.code()),
        );
        if let Some(seconds) = self.retry_after() {
            headers.insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::Full;

    use super::*;

    #[test]
    fn rejections_render_status_code_and_retry_after() {
        let cases = [
            (
                IdempotencyRejection::InvalidKey(Error::EmptyKey),
                StatusCode::BAD_REQUEST,
                "invalid-key",
                None,
            ),
            (
                IdempotencyRejection::MissingKey,
                StatusCode::BAD_REQUEST,
                "missing-key",
                None,
            ),
            (
                IdempotencyRejection::MissingScope,
                StatusCode::BAD_REQUEST,
                "missing-scope",
                None,
            ),
            (
                IdempotencyRejection::RequestBodyFailed,
                StatusCode::BAD_REQUEST,
                "request-body-failed",
                None,
            ),
            (
                IdempotencyRejection::FingerprintMismatch,
                StatusCode::BAD_REQUEST,
                "fingerprint-mismatch",
                None,
            ),
            (
                IdempotencyRejection::BodyTooLarge,
                StatusCode::PAYLOAD_TOO_LARGE,
                "body-too-large",
                None,
            ),
            (
                IdempotencyRejection::InFlight,
                StatusCode::CONFLICT,
                "in-flight",
                Some("1"),
            ),
            (
                IdempotencyRejection::ResponseBodyFailed,
                StatusCode::INTERNAL_SERVER_ERROR,
                "response-body-failed",
                None,
            ),
            (
                IdempotencyRejection::Overloaded,
                StatusCode::SERVICE_UNAVAILABLE,
                "overloaded",
                Some("1"),
            ),
            (
                IdempotencyRejection::StoreError,
                StatusCode::SERVICE_UNAVAILABLE,
                "store-error",
                Some("5"),
            ),
            (
                IdempotencyRejection::Shutdown,
                StatusCode::SERVICE_UNAVAILABLE,
                "shutdown",
                Some("5"),
            ),
        ];
        for (rejection, status, code, retry_after) in cases {
            let response: Response<Full<Bytes>> = rejection.render();
            let headers = response.headers();
            assert_eq!(response.status(), status);
            assert_eq!(
                headers
                    .get("idempotent-rejection")
                    .and_then(|v| v.to_str().ok()),
                Some(code)
            );
            assert_eq!(
                headers
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                retry_after
            );
            assert_eq!(
                headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                Some("text/plain; charset=utf-8")
            );
        }
    }
}
