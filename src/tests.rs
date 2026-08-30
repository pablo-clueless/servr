#[cfg(test)]
mod tests {
    use crate::api::create_router;
    use crate::smtp::SmtpService;
    use crate::state::{AppState, Database};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        Router,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    async fn setup_app() -> Router {
        let mailer = Arc::new(SmtpService::new("localhost", 25, None, None).unwrap());
        let (tx, _rx) = mpsc::channel(10);
        let state = Arc::new(AppState {
            mailer,
            job_tx: tx,
            db: Arc::new(Database {
                url: "mock".to_string(),
            }),
        });
        create_router().with_state(state)
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = setup_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
