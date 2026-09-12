//! Integration with axum. The rejection is a response and the key is an extractor.

use axum::body::Body;
use axum::extract::FromRequestParts;
use axum::response::IntoResponse;
use axum::response::Response;
use http::request::Parts;

use super::layer::DEFAULT_HEADER;
use super::rejection::IdempotencyRejection;
use crate::IdempotencyKey;

/// The same status, headers, and message the layer sends.
impl IntoResponse for IdempotencyRejection {
    fn into_response(self) -> Response {
        self.render::<Body>()
    }
}

/// Extracts the key the layer resolved, scope included.
///
/// Without the layer, the key is read from the idempotency-key header. A handler that takes
/// the key makes it required. A request without one is rejected with 400, even when the layer
/// forwards requests without a key.
impl<S: Send + Sync> FromRequestParts<S> for IdempotencyKey {
    type Rejection = IdempotencyRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(key) = parts.extensions.get::<Self>() {
            return Ok(key.clone());
        }
        let value = parts
            .headers
            .get(DEFAULT_HEADER)
            .ok_or(IdempotencyRejection::MissingKey)?;
        IdempotencyKey::try_from(value).map_err(IdempotencyRejection::InvalidKey)
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::Router;
    use axum::routing::post;
    use bytes::Bytes;
    use http::HeaderValue;
    use http::Request;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::middleware::IdempotencyLayer;
    use crate::middleware::stored_key;
    use crate::store::memory::MemoryStore;

    fn store() -> MemoryStore {
        MemoryStore::builder().try_build().expect("memory store")
    }

    fn request(key: Option<&str>) -> Request<Body> {
        let mut request = Request::post("/credentials");
        if let Some(key) = key {
            request = request.header("idempotency-key", key);
        }
        request.body(Body::from("{}")).expect("valid request")
    }

    async fn body(response: Response) -> Bytes {
        response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes()
    }

    #[tokio::test]
    async fn layer_composes_with_router() {
        let runs = Arc::new(AtomicUsize::new(0));
        let handler = {
            let runs = Arc::clone(&runs);
            move |body: String| async move {
                runs.fetch_add(1, Ordering::SeqCst);
                (StatusCode::CREATED, format!("issued for {body}"))
            }
        };
        let router = Router::new()
            .route("/credentials", post(handler))
            .layer(IdempotencyLayer::new(store()));

        let Ok(first) = router
            .clone()
            .oneshot(request(Some("cred-offer-123")))
            .await;
        let Ok(second) = router.oneshot(request(Some("cred-offer-123"))).await;

        assert_eq!(first.status(), StatusCode::CREATED);
        assert!(!first.headers().contains_key("idempotent-replayed"));
        assert_eq!(second.status(), StatusCode::CREATED);
        assert!(second.headers().contains_key("idempotent-replayed"));
        assert_eq!(&body(second).await[..], b"issued for {}");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handler_extracts_key_resolved_by_layer() {
        let router = Router::new()
            .route(
                "/credentials",
                post(|key: IdempotencyKey| async move { key.to_string() }),
            )
            .layer(IdempotencyLayer::new(store()).scope(|parts| {
                parts
                    .headers
                    .get("x-caller")
                    .and_then(|caller| caller.to_str().ok())
                    .map(String::from)
            }));
        let request = Request::post("/credentials")
            .header("idempotency-key", "cred-offer-123")
            .header("x-caller", "did:web:alice.example")
            .body(Body::from("{}"))
            .expect("valid request");

        let Ok(response) = router.oneshot(request).await;

        let key = IdempotencyKey::new("cred-offer-123").expect("valid key");
        let expected = stored_key("did:web:alice.example", &key).expect("a DID scopes");
        assert_eq!(&body(response).await[..], expected.as_str().as_bytes());
    }

    #[tokio::test]
    async fn extractor_reads_the_header_when_no_layer_ran() {
        let router = Router::new().route(
            "/credentials",
            post(|key: IdempotencyKey| async move { key.to_string() }),
        );

        let Ok(present) = router
            .clone()
            .oneshot(request(Some("cred-offer-123")))
            .await;
        let Ok(missing) = router.oneshot(request(None)).await;

        assert_eq!(&body(present).await[..], b"cred-offer-123");
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            missing
                .headers()
                .get("idempotent-rejection")
                .map(HeaderValue::as_bytes),
            Some(&b"missing-key"[..])
        );
    }
}
