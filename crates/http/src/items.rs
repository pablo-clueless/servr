//! `/api/items` — the CRUD surface, namespaced per run.
//!
//! Every read and write here goes through the run's own pool (invariant 6).
//! There is no unnamespaced path to the data: a request that names no run gets
//! the process's default run, never "all runs".
//!
//! Trap T9: these use sqlx's **runtime-checked** query API, not the `query!`
//! macros. Compile-time verification needs a live database, which would mean
//! CI could not build the workspace without Postgres running. The alternative
//! is committing a `.sqlx/` directory and remembering to regenerate it.
//!
//! Trap T1: axum 0.8 spells the path parameter `{id}`.

use std::sync::Arc;

use axum::{
    extract::{FromRequestParts, Path, State},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use testbed_core::{RunId, RUN_HEADER};
use uuid::Uuid;

use crate::data::{self, DataError, MaybeData};

/// Shared state for the data-plane routes.
#[derive(Clone)]
pub struct Items {
    pub state: Arc<testbed_core::State>,
    pub data: MaybeData,
}

/// The run a request operates on, from `X-Testbed-Run`.
///
/// Falling back to the process default rather than rejecting keeps single-run
/// use — which is most manual poking at the testbed — free of ceremony, while
/// parallel harnesses always send the header.
pub struct Run(pub RunId);

impl FromRequestParts<Items> for Run {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, items: &Items) -> Result<Self, Self::Rejection> {
        let Some(header) = parts.headers.get(RUN_HEADER) else {
            return Ok(Run(items.state.run()));
        };

        let raw = header
            .to_str()
            .map_err(|_| ApiError::BadRun("header is not valid text".into()))?;

        raw.parse::<RunId>()
            .map(Run)
            .map_err(|e| ApiError::BadRun(format!("{raw:?} is not a run id: {e}")))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Item {
    pub id: Uuid,
    pub name: String,
    /// Virtual time at creation. After a `clock/advance` this legitimately sits
    /// ahead of wall time — that is the point of it.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub struct NewItem {
    pub name: String,
}

pub async fn list(State(items): State<Items>, Run(run): Run) -> Result<Json<Vec<Item>>, ApiError> {
    let pool = data::require(&items.data)?.pool(run).await?;

    let rows = sqlx::query("SELECT id, name, created_at FROM items ORDER BY created_at DESC")
        .fetch_all(&pool)
        .await?;

    Ok(Json(rows.iter().map(row_to_item).collect()))
}

pub async fn get(
    State(items): State<Items>,
    Run(run): Run,
    Path(id): Path<Uuid>,
) -> Result<Json<Item>, ApiError> {
    let pool = data::require(&items.data)?.pool(run).await?;

    let row = sqlx::query("SELECT id, name, created_at FROM items WHERE id = $1")
        .bind(id)
        .fetch_optional(&pool)
        .await?
        .ok_or(ApiError::NotFound(id))?;

    Ok(Json(row_to_item(&row)))
}

pub async fn create(
    State(items): State<Items>,
    Run(run): Run,
    Json(body): Json<NewItem>,
) -> Result<(StatusCode, Json<Item>), ApiError> {
    let pool = data::require(&items.data)?.pool(run).await?;

    let item = Item {
        id: Uuid::new_v4(),
        name: body.name,
        // Virtual, not wall — so a row written after `clock/advance` carries
        // the advanced time and the data plane agrees with the queue.
        created_at: items.state.clock().now(),
    };

    sqlx::query("INSERT INTO items (id, name, created_at) VALUES ($1, $2, $3)")
        .bind(item.id)
        .bind(&item.name)
        .bind(item.created_at)
        .execute(&pool)
        .await?;

    Ok((StatusCode::CREATED, Json(item)))
}

pub async fn delete(
    State(items): State<Items>,
    Run(run): Run,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let pool = data::require(&items.data)?.pool(run).await?;

    let done = sqlx::query("DELETE FROM items WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await?;

    match done.rows_affected() {
        0 => Err(ApiError::NotFound(id)),
        _ => Ok(StatusCode::NO_CONTENT),
    }
}

fn row_to_item(row: &sqlx::postgres::PgRow) -> Item {
    Item {
        id: row.get("id"),
        name: row.get("name"),
        created_at: row.get("created_at"),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("invalid {RUN_HEADER}: {0}")]
    BadRun(String),
    #[error("item {0} not found")]
    NotFound(Uuid),
    #[error(transparent)]
    Data(#[from] DataError),
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::Data(DataError::Sql(e))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRun(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            // Not an internal error: the operator has not started Postgres, and
            // saying so is more useful than a 500.
            Self::Data(DataError::Unconfigured) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Data(DataError::UnknownRun(_)) => StatusCode::NOT_FOUND,
            Self::Data(DataError::Sql(_)) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, "data plane error");
        }

        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    async fn run_of(items: &Items, header: Option<&str>) -> Result<RunId, ApiError> {
        let mut request = Request::builder().uri("/api/items");
        if let Some(value) = header {
            request = request.header(RUN_HEADER, value);
        }
        let (mut parts, _) = request.body(()).unwrap().into_parts();
        Run::from_request_parts(&mut parts, items)
            .await
            .map(|r| r.0)
    }

    fn items() -> Items {
        use testbed_core::{BroadcastBus, Clock, Scenario, State};

        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(8, Arc::clone(&clock), run));
        Items {
            state: Arc::new(State::new(Scenario::default(), clock, bus, run)),
            data: None,
        }
    }

    #[tokio::test]
    async fn a_named_run_is_used() {
        let items = items();
        let wanted = RunId::new();

        let got = run_of(&items, Some(&wanted.header_value())).await.unwrap();
        assert_eq!(got, wanted);
    }

    #[tokio::test]
    async fn no_header_falls_back_to_the_process_run() {
        let items = items();
        let got = run_of(&items, None).await.unwrap();
        assert_eq!(got, items.state.run());
    }

    /// A typo'd run header must fail loudly. Silently falling back to the
    /// default run would let a test read another run's rows and pass.
    #[tokio::test]
    async fn a_malformed_run_header_is_rejected() {
        let items = items();
        let err = run_of(&items, Some("not-a-uuid")).await.unwrap_err();

        assert!(matches!(err, ApiError::BadRun(_)));
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn an_unconfigured_data_plane_is_503_not_500() {
        let status = ApiError::Data(DataError::Unconfigured)
            .into_response()
            .status();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
