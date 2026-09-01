//! The control plane.
//!
//! # This crate performs no Postgres I/O, ever
//!
//! HANDOFF §5 invariant 3. Control-plane state — fault config, scenario
//! registry, event log, clock offset — lives in memory behind `ArcSwap` and is
//! optionally snapshotted to SQLite. It must survive a full data-plane wipe;
//! the moment it depends on Postgres, resetting the data plane resets the
//! testbed's own configuration along with it.
//!
//! # Not yet built — Phase 2 onward (HANDOFF §9 task 7)
//!
//! - `/_admin/reset`, `/_admin/clock/advance`, `/_admin/faults`
//! - `/_admin/events` (SSE) — the typed contract a UI would later consume, so
//!   it stays stable (§10)
//! - `/_admin/runs`, `/_admin/jobs`, `/_admin/ws/*`, `/_admin/mail/send`,
//!   `/_admin/hooks/*`, `/_admin/telemetry/faults`, `/_admin/snapshot`

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use testbed_core::RunId;

/// Mount point for everything in this crate.
pub const PREFIX: &str = "/_admin";

pub fn router(run: RunId) -> Router {
    Router::new().route("/_admin/health", get(move || health(run)))
}

/// Phase 2 gate: `{"status":"ok","run":"<uuid>"}`.
async fn health(run: RunId) -> Json<Value> {
    Json(json!({ "status": "ok", "run": run.to_string() }))
}
