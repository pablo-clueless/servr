use crate::error::AppError;
use crate::schema::http::Response;
use crate::schema::queue::Job;
use crate::state::SharedState;
use axum::{
    extract::{ConnectInfo, Json, Path, State},
    http::HeaderMap,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

#[derive(Deserialize, serde::Serialize, ToSchema)]
pub struct WebhookPayload {
    pub event: String,
    pub data: serde_json::Value,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health_handler,
        ping_handler,
        get_users,
        get_user_by_id,
        get_posts,
        get_post_by_id,
        get_albums,
        get_album_by_id,
        webhook_handler,
        email_handler
    ),
    components(
        schemas(
            WebhookPayload,
            crate::schema::http::Response<String>,
            crate::schema::http::Response<Vec<crate::schema::query::User>>,
            crate::schema::http::Response<crate::schema::query::User>,
            crate::schema::http::Response<Vec<crate::schema::query::Post>>,
            crate::schema::http::Response<crate::schema::query::Post>,
            crate::schema::http::Response<Vec<crate::schema::query::Album>>,
            crate::schema::http::Response<crate::schema::query::Album>,
            crate::schema::email::SendEmail,
            crate::schema::query::User,
            crate::schema::query::Post,
            crate::schema::query::Album,
        )
    ),
    tags(
        (name = "System", description = "System health and connectivity endpoints"),
        (name = "API", description = "Main application API endpoints")
    )
)]
struct ApiDoc;

pub fn create_router() -> Router<SharedState> {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/health", get(health_handler))
        .route("/ping", get(ping_handler))
        .route("/users", get(get_users))
        .route("/users/{id}", get(get_user_by_id))
        .route("/posts", get(get_posts))
        .route("/posts/{id}", get(get_post_by_id))
        .route("/albums", get(get_albums))
        .route("/albums/{id}", get(get_album_by_id))
        .route("/webhook", post(webhook_handler))
        .route("/email", post(email_handler))
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Health check successful", body = Response<String>)
    ),
    tag = "System"
)]
async fn health_handler(headers: HeaderMap) -> Json<Response<String>> {
    info!("Health check hit");
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
        "unknown",
        ua,
    ))
}

#[utoipa::path(
    get,
    path = "/ping",
    responses(
        (status = 200, description = "Ping successful", body = Response<String>)
    ),
    tag = "System"
)]
async fn ping_handler(headers: HeaderMap) -> Json<Response<String>> {
    info!("Ping endpoint hit");
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
        "unknown",
        ua,
    ))
}

#[utoipa::path(
    get,
    path = "/users",
    responses(
        (status = 200, description = "List of users", body = Response<Vec<crate::schema::query::User>>)
    ),
    tag = "API"
)]
async fn get_users(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Json<Response<Vec<crate::schema::query::User>>> {
    info!("Get users hit");
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let users: Vec<_> = state.users.read().unwrap().values().cloned().collect();

    Json(Response::new(
        Some(users),
        "Users retrieved successfully",
        200,
        "GET",
        "/users",
        "unknown",
        ua,
    ))
}

#[utoipa::path(
    get,
    path = "/users/{id}",
    responses(
        (status = 200, description = "User found", body = Response<crate::schema::query::User>),
        (status = 404, description = "User not found")
    ),
    tag = "API"
)]
async fn get_user_by_id(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Response<crate::schema::query::User>>, AppError> {
    info!("Get user by id hit: {}", id);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let user = state.users.read().unwrap().get(&id).cloned();

    match user {
        Some(u) => Ok(Json(Response::new(
            Some(u),
            "User retrieved successfully",
            200,
            "GET",
            &format!("/users/{}", id),
            "unknown",
            ua,
        ))),
        None => Err(AppError::NotFound(format!("User with id {} not found", id))),
    }
}

#[utoipa::path(
    get,
    path = "/posts",
    responses(
        (status = 200, description = "List of posts", body = Response<Vec<crate::schema::query::Post>>)
    ),
    tag = "API"
)]
async fn get_posts(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Json<Response<Vec<crate::schema::query::Post>>> {
    info!("Get posts hit");
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let posts: Vec<_> = state.posts.read().unwrap().values().cloned().collect();

    Json(Response::new(
        Some(posts),
        "Posts retrieved successfully",
        200,
        "GET",
        "/posts",
        "unknown",
        ua,
    ))
}

#[utoipa::path(
    get,
    path = "/posts/{id}",
    responses(
        (status = 200, description = "Post found", body = Response<crate::schema::query::Post>),
        (status = 404, description = "Post not found")
    ),
    tag = "API"
)]
async fn get_post_by_id(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Response<crate::schema::query::Post>>, AppError> {
    info!("Get post by id hit: {}", id);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let post = state.posts.read().unwrap().get(&id).cloned();

    match post {
        Some(p) => Ok(Json(Response::new(
            Some(p),
            "Post retrieved successfully",
            200,
            "GET",
            &format!("/posts/{}", id),
            "unknown",
            ua,
        ))),
        None => Err(AppError::NotFound(format!("Post with id {} not found", id))),
    }
}

#[utoipa::path(
    get,
    path = "/albums",
    responses(
        (status = 200, description = "List of albums", body = Response<Vec<crate::schema::query::Album>>)
    ),
    tag = "API"
)]
async fn get_albums(
    State(state): State<SharedState>,
    headers: HeaderMap,
) -> Json<Response<Vec<crate::schema::query::Album>>> {
    info!("Get albums hit");
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let albums: Vec<_> = state.albums.read().unwrap().values().cloned().collect();

    Json(Response::new(
        Some(albums),
        "Albums retrieved successfully",
        200,
        "GET",
        "/albums",
        "unknown",
        ua,
    ))
}

#[utoipa::path(
    get,
    path = "/albums/{id}",
    responses(
        (status = 200, description = "Album found", body = Response<crate::schema::query::Album>),
        (status = 404, description = "Album not found")
    ),
    tag = "API"
)]
async fn get_album_by_id(
    State(state): State<SharedState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Response<crate::schema::query::Album>>, AppError> {
    info!("Get album by id hit: {}", id);
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");

    let album = state.albums.read().unwrap().get(&id).cloned();

    match album {
        Some(a) => Ok(Json(Response::new(
            Some(a),
            "Album retrieved successfully",
            200,
            "GET",
            &format!("/albums/{}", id),
            "unknown",
            ua,
        ))),
        None => Err(AppError::NotFound(format!(
            "Album with id {} not found",
            id
        ))),
    }
}

#[utoipa::path(
    post,
    path = "/webhook",
    request_body = WebhookPayload,
    responses(
        (status = 200, description = "Webhook enqueued", body = Response<String>),
        (status = 429, description = "Too many requests"),
        (status = 500, description = "Internal server error")
    ),
    tag = "API"
)]
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

    state.job_tx.try_send(job).map_err(|e| match e {
        tokio::sync::mpsc::error::TrySendError::Full(_) => AppError::TooManyRequests,
        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
            AppError::QueueError("Queue closed".to_string())
        }
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

#[utoipa::path(
    post,
    path = "/email",
    request_body = crate::schema::email::SendEmail,
    responses(
        (status = 200, description = "Email enqueued", body = Response<String>),
        (status = 429, description = "Too many requests"),
        (status = 500, description = "Internal server error")
    ),
    tag = "API"
)]
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
    state.job_tx.try_send(job).map_err(|e| match e {
        tokio::sync::mpsc::error::TrySendError::Full(_) => AppError::TooManyRequests,
        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
            AppError::QueueError("Queue closed".to_string())
        }
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
