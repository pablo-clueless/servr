//! REST surface, and the fault layer that sits in front of every route on it.
//!
//! Trap T1: axum 0.8 path params are `{id}`, not `:id`. The 0.7 form compiles
//! and then 404s at runtime.

pub mod data;
pub mod fault;
pub mod items;
pub mod json;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use testbed_core::State;

use crate::data::MaybeData;
use crate::items::Items;

/// Every route here is behind the fault layer. `/_admin` deliberately is not —
/// see [`fault`] for why.
pub fn router(state: Arc<State>) -> Router {
    router_with_data(state, None)
}

/// The full data plane. `data` is `None` when Postgres was not configured, in
/// which case `/api/items` answers 503 and the rest of the surface is unaffected.
pub fn router_with_data(state: Arc<State>, data: MaybeData) -> Router {
    let items = Items {
        state: Arc::clone(&state),
        data,
    };

    let api = Router::new()
        .route("/api/items", get(items::list).post(items::create))
        .route("/api/items/{id}", get(items::get).delete(items::delete))
        .with_state(items);

    let plane = Router::new()
        .route("/api/ping", get(ping))
        .route("/api/echo", post(echo))
        .merge(api)
        .with_state(Arc::clone(&state));

    fault::guard(state, plane)
}

async fn ping() -> Json<Value> {
    Json(json!({ "pong": true }))
}

/// Returns whatever it is sent. Useful for exercising body truncation and
/// connection-drop faults against a payload of known size.
async fn echo(body: Json<Value>) -> Json<Value> {
    body
}
