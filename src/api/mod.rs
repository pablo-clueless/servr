use crate::error::AppError;
use crate::schema::http::Response;
use crate::schema::queue::Job;
use crate::state::{AppState, SharedState};
use axum::{
    Router,
    extract::{ConnectInfo, Json, State},
    http::HeaderMap,
    routing::{get, post},
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[derive(Deserialize)]
pub struct WebhookPayload {
    pub event: String,
    pub data: serde_json::Value,
}

pub fn create_router() -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ping", get(ping_handler))
        .route("/webhook", post(webhook_handler))
        .route("/email", post(email_handler))
}

async fn health_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Json<Response<String>> {
    info!("Health check hit from {}", addr);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    Json(Response::new(
        Some("OK".to_string()),
        "Health check successful",
        200,
        "GET",
        "/health",
        &addr.to_string(),
        ua,
    ))
}

async fn ping_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Json<Response<String>> {
    info!("Ping endpoint hit from {}", addr);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    Json(Response::new(
        Some("Pong".to_string()),
        "Ping successful",
        200,
        "GET",
        "/ping",
        &addr.to_string(),
        ua,
    ))
}

async fn webhook_handler(
    State(state): State<SharedState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<WebhookPayload>,
) -> Result<Json<Response<String>>, AppError> {
    info!("Webhook received: {} from {}", payload.event, addr);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let job = Job::ProcessWebhook {
        id: uuid::Uuid::new_v4().to_string(),
        payload: serde_json::to_value(&payload).map_err(|e| AppError::Internal(e.to_string()))?,
    };

    state
        .job_tx
        .try_send(job)
        .map_err(|e| match e {
            tokio::sync::mpsc::error::TrySendError::Full(_) => AppError::TooManyRequests,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => AppError::QueueError("Queue closed".to_string()),
        })?;

    Ok(Json(Response::new(
        Some("Webhook enqueued".to_string()),
        "Webhook processed",
        200,
        "POST",
        "/webhook",
        &addr.to_string(),
        ua,
    )))
}

async fn email_handler(
    State(state): State<SharedState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(email): Json<crate::schema::email::SendEmail>,
) -> Result<Json<Response<String>>, AppError> {
    info!(
        "Email request received for {} from {}",
        email.to.join(", "),
        addr
    );
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let job = Job::SendEmail(email);
    state
        .job_tx
        .try_send(job)
        .map_err(|e| match e {
            tokio::sync::mpsc::error::TrySendError::Full(_) => AppError::TooManyRequests,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => AppError::QueueError("Queue closed".to_string()),
        })?;

    Ok(Json(Response::new(
        Some("Email enqueued".to_string()),
        "Email request processed",
        200,
        "POST",
        "/email",
        &addr.to_string(),
        ua,
    )))
}
