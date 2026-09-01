//! REST surface, and the fault layer that sits in front of every route on it.
//!
//! # Still owed — Phase 3
//!
//! - CRUD under `/api`, namespaced by `RunId`, backed by Postgres
//!
//! Trap T1: axum 0.8 path params are `{id}`, not `:id`. The 0.7 form compiles
//! and then 404s at runtime.

pub mod fault;

use std::sync::Arc;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use testbed_core::State;

/// Every route here is behind the fault layer. `/_admin` deliberately is not —
/// see [`fault`] for why.
pub fn router(state: Arc<State>) -> Router {
    Router::new()
        .route("/api/ping", get(ping))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            fault::layer,
        ))
        .with_state(state)
}

async fn ping() -> Json<Value> {
    Json(json!({ "pong": true }))
}
