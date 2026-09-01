//! REST surface, and the fault layer that sits in front of every route.
//!
//! # Not yet built — Phase 2 (HANDOFF §9 task 6)
//!
//! - the `tower` fault layer resolving [`testbed_core::FaultSpec`] per request:
//!   rate, latency, jitter, status override, body truncation, connection drop
//! - CRUD under `/api`, namespaced by `RunId` (Phase 3)
//! - an `EventKind::HttpRequest` per request, naming the faults that fired
//!
//! Faults apply at the layer, always (§5 invariant 8). A handler that reads
//! fault config itself is a bug: it means the fault only exists on the routes
//! someone remembered to wire, which is precisely the routes that get tested.
//!
//! Trap T1: axum 0.8 path params are `{id}`, not `:id`. The 0.7 form compiles
//! and then 404s at runtime.

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};

/// The trivial route the Phase 2 and 2b gates exercise.
pub fn router() -> Router {
    Router::new().route("/api/ping", get(ping))
}

async fn ping() -> Json<Value> {
    Json(json!({ "pong": true }))
}
