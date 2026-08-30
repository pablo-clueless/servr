use crate::schema::http::Response;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Internal server error: {0}")]
    Internal(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("SMTP error: {0}")]
    SmtpError(String),

    #[error("Queue error: {0}")]
    QueueError(String),

    #[error("Too many requests: the job queue is full")]
    TooManyRequests,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Internal(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            AppError::BadRequest(ref msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            AppError::NotFound(ref msg) => (StatusCode::NOT_FOUND, msg.clone()),
            AppError::SmtpError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            AppError::QueueError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            AppError::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "Job queue is full".to_string()),
        };


        let body = Response::<()> {
            data: None,
            error: Some(error_message),
            message: "An error occurred".to_string(),
            meta: crate::schema::http::Meta {
                headers: std::collections::HashMap::new(),
                ip: "0.0.0.0".to_string(),
                user_agent: "unknown".to_string(),
            },
            method: "UNKNOWN".to_string(),
            path: "UNKNOWN".to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            status: status.as_u16(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        (status, Json(body)).into_response()
    }
}
