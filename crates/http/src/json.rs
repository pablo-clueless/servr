//! A JSON body extractor that does not insist on the header.
//!
//! # Why not `axum::Json`
//!
//! `Json` rejects any request without `Content-Type: application/json`, and
//! every gate in HANDOFF §7 is a bare `curl -d '{...}'` — which sends
//! `application/x-www-form-urlencoded`. So the gates, as written, get
//! ``Expected request with `Content-Type: application/json` `` instead of the
//! documented output, on every phase that posts a body.
//!
//! The gates are the spec, so the server bends, not the gate. Nothing is lost:
//! a client that sets the header still works, and the testbed's control plane
//! is a surface for humans and shell scripts, where refusing a well-formed body
//! over a header nobody typed is friction with no upside.
//!
//! Malformed JSON is still a 400 with the parse error in it. Leniency here is
//! about the envelope, not the contents.

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Deserializes the body as JSON whatever the `Content-Type` says.
pub struct Lenient<T>(pub T);

impl<S, T> FromRequest<S> for Lenient<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = LenientRejection;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|e| LenientRejection(e.to_string()))?;

        // An empty body decodes as `{}`, not `null`: serde will not build a
        // struct from `null` however many fields carry `#[serde(default)]`, so
        // an endpoint whose every field is optional would reject a bodyless
        // request for a reason that has nothing to do with the request.
        let raw: &[u8] = if bytes.is_empty() { b"{}" } else { &bytes };

        serde_json::from_slice(raw)
            .map(Lenient)
            .map_err(|e| LenientRejection(format!("body is not valid JSON for this endpoint: {e}")))
    }
}

#[derive(Debug)]
pub struct LenientRejection(String);

impl IntoResponse for LenientRejection {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "error": self.0 })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::routing::post;
    use axum::Router;
    use serde::Deserialize;
    use tower::ServiceExt;

    use super::*;

    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        topic: String,
    }

    fn app() -> Router {
        Router::new().route(
            "/",
            post(|Lenient(body): Lenient<Body>| async move { body.topic }),
        )
    }

    async fn post_with(content_type: Option<&str>, body: &'static str) -> (StatusCode, String) {
        let mut request = Request::builder().method("POST").uri("/");
        if let Some(ct) = content_type {
            request = request.header("content-type", ct);
        }
        let response = app()
            .oneshot(request.body(axum::body::Body::from(body)).unwrap())
            .await
            .unwrap();

        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    /// What `curl -d '{...}'` actually sends, and what every §7 gate is.
    #[tokio::test]
    async fn accepts_the_form_urlencoded_content_type_curl_defaults_to() {
        let (status, body) = post_with(
            Some("application/x-www-form-urlencoded"),
            r#"{"topic":"demo"}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "demo");
    }

    #[tokio::test]
    async fn accepts_a_missing_content_type() {
        let (status, body) = post_with(None, r#"{"topic":"demo"}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "demo");
    }

    #[tokio::test]
    async fn still_accepts_the_correct_content_type() {
        let (status, body) = post_with(Some("application/json"), r#"{"topic":"demo"}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "demo");
    }

    /// An all-defaults struct posted with no body at all.
    #[tokio::test]
    async fn an_empty_body_decodes_as_defaults() {
        let (status, body) = post_with(None, "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "");
    }

    /// Leniency is about the envelope, not the contents.
    #[tokio::test]
    async fn malformed_json_is_still_a_400_that_says_why() {
        let (status, body) = post_with(None, "{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body.contains("not valid JSON"),
            "the rejection does not explain itself: {body}"
        );
    }
}
