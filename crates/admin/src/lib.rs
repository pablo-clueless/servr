//! The control plane.
//!
//! # This crate performs no Postgres I/O, ever
//!
//! HANDOFF §5 invariant 3. Control-plane state — fault config, scenario
//! registry, event log, clock offset — lives in memory behind `ArcSwap` and is
//! optionally snapshotted to SQLite. It must survive a full data-plane wipe;
//! the moment it depends on Postgres, resetting the data plane resets the
//! testbed's own configuration along with it. CI greps this directory.
//!
//! # These routes are never faulted
//!
//! The fault layer wraps the data-plane router only. A scenario matching `/*`
//! would otherwise put `/_admin/reset` behind the very fault it exists to
//! clear, and the testbed would need a restart to recover.
//!
//! # Still owed
//!
//! `/_admin/runs` (Phase 3), `/_admin/jobs` (4), `/_admin/ws/*` (5),
//! `/_admin/mail/send` (6), `/_admin/hooks/*` (7), `/_admin/telemetry/faults`
//! (8), `/_admin/snapshot` (9).

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use testbed_core::FaultSpec;

/// Mount point for everything in this crate.
pub const PREFIX: &str = "/_admin";

type Shared = Arc<testbed_core::State>;

pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/_admin/health", get(health))
        .route("/_admin/reset", post(reset))
        .route("/_admin/clock", get(clock))
        .route("/_admin/clock/advance", post(advance))
        .route("/_admin/clock/freeze", post(freeze))
        .route("/_admin/clock/resume", post(resume))
        .route(
            "/_admin/faults",
            get(list_faults).post(add_fault).delete(clear_faults),
        )
        .route("/_admin/events", get(events))
        .with_state(state)
}

/// Run lifecycle. Separate from [`router`] because it is the only part of the
/// control plane that reaches the data plane — to create and drop schemas — and
/// keeping that dependency off the main admin state makes the boundary visible.
///
/// This does not violate invariant 3. The control plane stores nothing in
/// Postgres; it issues DDL on request and keeps its own state in memory, so a
/// full data-plane wipe still leaves the testbed configured.
pub fn runs_router(data: testbed_http::data::MaybeData) -> Router {
    Router::new()
        .route("/_admin/runs", get(list_runs).post(create_run))
        .route("/_admin/runs/{id}", axum::routing::delete(drop_run))
        .with_state(data)
}

/// Phase 3 gate: `{"run":"<uuid>"}`.
async fn create_run(
    State(data): State<testbed_http::data::MaybeData>,
) -> Result<Json<Value>, RunError> {
    let plane = testbed_http::data::require(&data)?;
    let run = testbed_core::RunId::new();
    plane.create_run(run).await?;

    Ok(Json(
        json!({ "run": run.to_string(), "schema": run.schema() }),
    ))
}

async fn list_runs(
    State(data): State<testbed_http::data::MaybeData>,
) -> Result<Json<Value>, RunError> {
    let plane = testbed_http::data::require(&data)?;
    let runs: Vec<String> = plane.runs().await.iter().map(|r| r.to_string()).collect();
    Ok(Json(json!({ "runs": runs })))
}

/// Drops the run's schema and everything in it. The control plane is untouched
/// — this is the wipe that control-plane state has to survive (invariant 3).
async fn drop_run(
    State(data): State<testbed_http::data::MaybeData>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Value>, RunError> {
    let plane = testbed_http::data::require(&data)?;
    let run: testbed_core::RunId = id.parse().map_err(|_| RunError::BadId(id.clone()))?;
    plane.drop_run(run).await?;
    Ok(Json(json!({ "ok": true })))
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error("{0:?} is not a run id")]
    BadId(String),
    #[error(transparent)]
    Data(#[from] testbed_http::data::DataError),
}

impl axum::response::IntoResponse for RunError {
    fn into_response(self) -> axum::response::Response {
        use testbed_http::data::DataError;

        let status = match &self {
            Self::BadId(_) => axum::http::StatusCode::BAD_REQUEST,
            Self::Data(DataError::Unconfigured) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Self::Data(DataError::UnknownRun(_)) => axum::http::StatusCode::NOT_FOUND,
            Self::Data(DataError::Sql(_)) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

/// `/metrics`, mounted at the root rather than under `/_admin` because that is
/// where every Prometheus scrape config looks by default.
///
/// Kept separate from [`router`] so it can carry the telemetry handle without
/// putting it in every other handler's state.
pub fn metrics_route(state: Shared, telemetry: Arc<testbed_telemetry::Telemetry>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .with_state((state, telemetry))
}

/// Runtime gauges are sampled here, at scrape time, rather than continuously —
/// so what a scrape reports is the state at the moment it was asked for.
///
/// Trap T14: the clock offset comes from the virtual clock. Anything
/// time-derived that read wall time would disagree with the domain state it
/// describes the instant someone advanced the clock.
async fn metrics(
    State((state, telemetry)): State<(Shared, Arc<testbed_telemetry::Telemetry>)>,
) -> impl axum::response::IntoResponse {
    testbed_telemetry::metrics::observe_runtime(
        state.bus().dropped(),
        state.bus().subscribers(),
        state.clock().offset_ms(),
    );

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        telemetry.render_metrics(),
    )
}

/// Phase 2 gate: `{"status":"ok","run":"<uuid>"}`.
async fn health(State(state): State<Shared>) -> Json<Value> {
    Json(json!({ "status": "ok", "run": state.run().to_string() }))
}

/// Drops the overlay, re-resolves from base, and returns the clock to wall
/// time. The data plane is untouched — dropping run schemas is a separate
/// operation with a separate blast radius.
async fn reset(State(state): State<Shared>) -> Json<Value> {
    state.reset();
    tracing::info!(scenario = %state.base().name, "control plane reset");
    Json(json!({ "ok": true }))
}

async fn clock(State(state): State<Shared>) -> Json<Value> {
    let clock = state.clock();
    Json(json!({
        "now": clock.now(),
        "wall": testbed_core::Clock::wall_now(),
        "offset_ms": clock.offset_ms(),
        "frozen": clock.is_frozen(),
    }))
}

#[derive(Deserialize)]
struct Advance {
    ms: u64,
}

/// Moves virtual time forward. This must not sleep: the Phase 4 gate advances
/// 30 seconds and asserts the call returns in milliseconds.
async fn advance(State(state): State<Shared>, Json(body): Json<Advance>) -> Json<Value> {
    let clock = state.clock();
    clock.advance(Duration::from_millis(body.ms));
    tracing::info!(advanced_ms = body.ms, now = %clock.now(), "clock advanced");
    Json(json!({ "ok": true, "now": clock.now(), "offset_ms": clock.offset_ms() }))
}

async fn freeze(State(state): State<Shared>) -> Json<Value> {
    state.clock().freeze();
    Json(json!({ "ok": true, "now": state.clock().now(), "frozen": true }))
}

async fn resume(State(state): State<Shared>) -> Json<Value> {
    state.clock().resume();
    Json(json!({ "ok": true, "now": state.clock().now(), "frozen": false }))
}

async fn list_faults(State(state): State<Shared>) -> Json<Vec<FaultSpec>> {
    Json(state.resolved().faults.clone())
}

/// Appends to the *effective* fault list, so posting a rule adds to whatever
/// the scenario seeded rather than silently replacing it.
async fn add_fault(State(state): State<Shared>, Json(spec): Json<FaultSpec>) -> Json<Value> {
    let mut faults = state.resolved().faults.clone();
    tracing::info!(route = %spec.route, rate = spec.rate, "fault added");
    faults.push(spec);
    state.mutate(|overlay| overlay.faults = Some(faults));
    Json(json!({ "ok": true }))
}

/// Clears every fault, including those the scenario seeded. `reset` puts the
/// scenario's back; this does not.
async fn clear_faults(State(state): State<Shared>) -> Json<Value> {
    state.mutate(|overlay| overlay.faults = Some(vec![]));
    Json(json!({ "ok": true }))
}

/// The live event tail. This is the contract a UI would later consume
/// (HANDOFF §10), so the shape is `Event` as serialized by `core` and nothing
/// bespoke.
///
/// Trap T8: SSE dies behind proxies without keep-alive.
async fn events(
    State(state): State<Shared>,
) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, Infallible>>> {
    let stream = state.bus().subscribe().map(|event| {
        Ok(SseEvent::default()
            .json_data(&event)
            .unwrap_or_else(|_| SseEvent::default().data("{\"error\":\"unserializable event\"}")))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
