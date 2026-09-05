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
//! Nothing. Every phase's control-plane surface is here.

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
use testbed_http::json::Lenient;

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
        .route("/_admin/snapshot", get(read_snapshot).post(write_snapshot))
        .route(
            "/_admin/telemetry/faults",
            get(telemetry_faults)
                .post(set_telemetry_faults)
                .delete(clear_telemetry_faults),
        )
        .with_state(state)
}

/// What `server` actually managed to wire up, for [`index`].
///
/// Every one of these is optional at boot and degrades rather than failing, so
/// "is Postgres connected" is a question only the process that did the wiring
/// can answer — the control plane deliberately never touches it (invariant 3).
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct Surfaces {
    pub postgres: bool,
    pub redis: bool,
    pub mailpit: bool,
    pub tracing: bool,
}

/// `GET /` — what this is, whether it is healthy, and what can be called.
///
/// # Why this exists
///
/// A deployed testbed with nothing at the root is hostile: the first thing
/// anyone does with a URL is open it, and a 404 there says "broken" when the
/// service is fine. This is the boot log made queryable — the same
/// degradation status, from a machine you can curl instead of a log you have
/// to still have open.
///
/// # Why it is not behind the fault layer
///
/// Same reason `/_admin` is not (see [`crate`] docs): a scenario matching `/*`
/// would make the one page that explains what is happening fail along with
/// everything else, exactly when it is most wanted.
///
/// This is not the UI §10 defers — no HTML, no assets, no state. It is a route
/// listing. The UI, when it happens, consumes `/_admin/events`.
pub fn index_router(state: Shared, surfaces: Surfaces) -> Router {
    Router::new()
        .route("/", get(index))
        .with_state((state, surfaces))
}

async fn index(State((state, surfaces)): State<(Shared, Surfaces)>) -> Json<Value> {
    Json(json!({
        "name": "servr",
        "description": "A fault-injection testbed. Every surface can be told to misbehave on demand.",
        "run": state.run().to_string(),
        "scenario": state.base().name,
        "blast_radius": state.base().blast_radius,

        // The optional dependencies, as wired at boot. `false` is not an error
        // — the matching surface answers 503 or falls back, and the boot log
        // says which.
        "connected": surfaces,

        "surfaces": {
            "http":      ["GET /api/ping", "POST /api/echo", "GET|POST /api/items", "GET|DELETE /api/items/{id}"],
            "websocket": ["GET /ws?topic={topic}"],
            "streaming": ["POST /v1/chat/completions", "GET /_stream/{id}"],
            "webhooks":  ["POST /hooks/in/{id}"],
            "metrics":   ["GET /metrics"],
        },
        "control_plane": {
            "health":    ["GET /_admin/health", "POST /_admin/reset"],
            "clock":     ["GET /_admin/clock", "POST /_admin/clock/advance", "POST /_admin/clock/freeze", "POST /_admin/clock/resume"],
            "faults":    ["GET|POST|DELETE /_admin/faults"],
            "events":    ["GET /_admin/events (SSE)"],
            "runs":      ["GET|POST /_admin/runs", "DELETE /_admin/runs/{id}"],
            "jobs":      ["GET|POST /_admin/jobs", "GET /_admin/jobs/{id}"],
            "websocket": ["GET /_admin/ws", "POST /_admin/ws/publish", "POST /_admin/ws/kill"],
            "mail":      ["GET|DELETE /_admin/mail", "POST /_admin/mail/send"],
            "webhooks":  ["GET /_admin/hooks/in", "GET|DELETE /_admin/hooks/in/{id}", "GET|POST /_admin/hooks/out"],
            "telemetry": ["GET|POST|DELETE /_admin/telemetry/faults"],
            "snapshot":  ["GET|POST /_admin/snapshot"],
        },

        // Said plainly rather than left to be discovered.
        "warning": "/_admin is unauthenticated. Anyone who can reach this URL can inject faults,                     drop run schemas, and make this server send signed requests to any URL.",
    }))
}

/// The catch-all for a path no router claimed.
///
/// axum's default 404 has an **empty body**, so a browser shows its own "page
/// cannot be found" and a client gets a status with nothing to act on — both
/// of which read as "the server is broken" rather than "that route does not
/// exist". Every other error this service produces is JSON with an `error`
/// key; this makes the most common one match.
///
/// Wired with `Router::fallback` on the assembled app, so it sees only paths
/// that matched nothing. A known path with the wrong method still gets axum's
/// 405, which is the more accurate answer and should not be swallowed here.
pub async fn not_found(uri: axum::http::Uri) -> impl axum::response::IntoResponse {
    (
        axum::http::StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("no route for {}", uri.path()),
            "hint": "GET / lists every surface on this testbed",
        })),
    )
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

/// Webhooks: read the capture inbox, queue an outbound delivery.
pub fn hooks_router(hooks: Arc<testbed_hooks::Hooks>) -> Router {
    Router::new()
        .route("/_admin/hooks/in", get(hooks_summary))
        .route(
            "/_admin/hooks/in/{id}",
            get(hooks_captures).delete(hooks_clear),
        )
        .route("/_admin/hooks/out", get(hooks_deliveries).post(hooks_send))
        .with_state(hooks)
}

/// Phase 7 gate: `curl /_admin/hooks/in/abc | jq '.[0].body.x'` → `1`.
///
/// Serves a bare array, not an envelope: the gate indexes straight into `.[0]`,
/// and wrapping it in `{"captures":[…]}` would be a nicer shape that fails the
/// gate as written.
async fn hooks_captures(
    State(hooks): State<Arc<testbed_hooks::Hooks>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<Vec<testbed_hooks::Capture>> {
    Json(hooks.inbox.captures(&id))
}

async fn hooks_summary(State(hooks): State<Arc<testbed_hooks::Hooks>>) -> Json<Value> {
    Json(json!({ "endpoints": hooks.inbox.summary() }))
}

async fn hooks_clear(
    State(hooks): State<Arc<testbed_hooks::Hooks>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<Value> {
    hooks.inbox.clear(Some(&id));
    Json(json!({ "ok": true, "endpoint": id }))
}

#[derive(Deserialize)]
struct NewDelivery {
    url: String,
    #[serde(default)]
    name: Option<String>,
    /// `stripe`, `github` or `none` (Q4). Defaults to Stripe.
    #[serde(default)]
    sign: Option<testbed_core::SigningScheme>,
    #[serde(default)]
    secret: Option<String>,
    /// Retry offsets in **virtual** milliseconds, counted from the enqueue.
    #[serde(default)]
    backoff_ms: Option<Vec<u64>>,
    #[serde(default)]
    fail_first: Option<u32>,
    #[serde(default)]
    body: Option<Value>,
}

/// Phase 7 gate: queues a delivery and returns at once. The attempts follow as
/// the virtual clock reaches them.
async fn hooks_send(
    State(hooks): State<Arc<testbed_hooks::Hooks>>,
    Lenient(body): Lenient<NewDelivery>,
) -> Json<Value> {
    let endpoint = testbed_hooks::outbound::endpoint_from(
        body.name,
        body.url,
        body.sign,
        body.secret,
        body.backoff_ms,
        body.fail_first,
    );

    // Echoed back because the gate has to verify the delivered signature, and
    // an endpoint that defaulted its secret gives the caller no other way to
    // learn which one was used.
    let secret = endpoint
        .secret
        .clone()
        .unwrap_or_else(|| testbed_hooks::sign::DEFAULT_SECRET.to_string());
    let scheme = endpoint.sign;

    let id = hooks
        .sender
        .enqueue(endpoint, body.body.unwrap_or_else(|| json!({ "ok": true })));

    Json(json!({
        "ok": true,
        "id": id.to_string(),
        "sign": scheme,
        "secret": secret,
    }))
}

async fn hooks_deliveries(State(hooks): State<Arc<testbed_hooks::Hooks>>) -> Json<Value> {
    Json(json!({ "deliveries": hooks.sender.deliveries() }))
}

/// Mail: send through Mailpit's SMTP, read back through its REST API.
///
/// `None` when Mailpit was not reachable at boot, in which case these answer
/// 503 — the same shape the data plane uses for Postgres.
pub type MaybeMailer = Option<Arc<testbed_mail::Mailer>>;

pub fn mail_router(mailer: MaybeMailer, state: Shared) -> Router {
    Router::new()
        .route("/_admin/mail", get(mail_list).delete(mail_purge))
        .route("/_admin/mail/send", post(mail_send))
        .with_state((mailer, state))
}

/// The run a mail request acts as, from `X-Testbed-Run`.
///
/// Falls back to the process run rather than rejecting, matching
/// `testbed_http::items::Run` — single-run poking stays free of ceremony while
/// a parallel harness always sends the header. The §7 gate sends it.
fn run_of(
    headers: &axum::http::HeaderMap,
    state: &Shared,
) -> Result<testbed_core::RunId, MailApiError> {
    let Some(raw) = headers.get(testbed_core::RUN_HEADER) else {
        return Ok(state.run());
    };
    raw.to_str()
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| MailApiError::BadRun(format!("{raw:?} is not a run id")))
}

/// Phase 6 gate: `POST /_admin/mail/send` with `x-testbed-run: $RUN_A`.
async fn mail_send(
    State((mailer, state)): State<(MaybeMailer, Shared)>,
    headers: axum::http::HeaderMap,
    Lenient(mail): Lenient<testbed_mail::OutgoingMail>,
) -> Result<Json<Value>, MailApiError> {
    let mailer = mailer.as_ref().ok_or(MailApiError::Unconfigured)?;
    let run = run_of(&headers, &state)?;

    let sent = mailer.send(run, mail).await?;
    Ok(Json(json!({
        "ok": true,
        "message_id": sent.message_id,
        "to": sent.to,
        "subject": sent.subject,
        "run": run.to_string(),
    })))
}

#[derive(Deserialize)]
struct MailQuery {
    /// Passed to Mailpit's own search. A convenience for narrowing, never the
    /// isolation mechanism — see trap T7.
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    /// Read every run's mail rather than this one's. Off by default: the
    /// run-filtered view is the one that holds invariant 7, and a caller has to
    /// ask explicitly to leave it.
    #[serde(default)]
    all: bool,
}

/// The run-filtered inbox. This is the surface invariant 7 lives on.
async fn mail_list(
    State((mailer, state)): State<(MaybeMailer, Shared)>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<MailQuery>,
) -> Result<Json<Value>, MailApiError> {
    let mailer = mailer.as_ref().ok_or(MailApiError::Unconfigured)?;
    let run = run_of(&headers, &state)?;
    let limit = params.limit.unwrap_or(testbed_mail::inbox::DEFAULT_LIMIT);
    let query = params.query.as_deref();

    let messages = if params.all {
        mailer.inbox().all(query, limit).await?
    } else {
        mailer.inbox().for_run(run, query, limit).await?
    };

    Ok(Json(json!({
        "run": if params.all { Value::Null } else { json!(run.to_string()) },
        "count": messages.len(),
        "messages": messages,
    })))
}

/// Deletes every message Mailpit holds, not just this run's.
///
/// Mailpit has no per-run delete for the same reason it has no per-run inbox
/// (T7), so this is deliberately all-or-nothing rather than a filtered delete
/// that would look per-run and not be.
async fn mail_purge(
    State((mailer, _)): State<(MaybeMailer, Shared)>,
) -> Result<Json<Value>, MailApiError> {
    let mailer = mailer.as_ref().ok_or(MailApiError::Unconfigured)?;
    mailer.inbox().purge().await?;
    Ok(Json(json!({ "ok": true, "scope": "all runs" })))
}

#[derive(Debug, thiserror::Error)]
enum MailApiError {
    #[error("Mailpit is not configured; set MAILPIT_SMTP and MAILPIT_API")]
    Unconfigured,
    #[error("{0}")]
    BadRun(String),
    #[error(transparent)]
    Mail(#[from] testbed_mail::MailError),
}

impl axum::response::IntoResponse for MailApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Self::Unconfigured => axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Self::BadRun(_) => axum::http::StatusCode::BAD_REQUEST,
            // A send that fails because Mailpit went away is not the caller's
            // fault, and 502 says so more usefully than 500.
            Self::Mail(_) => axum::http::StatusCode::BAD_GATEWAY,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

/// WebSocket control: inject a frame, disconnect a topic, read presence.
///
/// These are how a test drives the ws surface from the outside — the point
/// being that the client under test does nothing unusual, and the server-side
/// events it has to cope with are triggered here.
pub fn ws_router(hub: Arc<testbed_ws::Hub>) -> Router {
    Router::new()
        .route("/_admin/ws", get(ws_presence))
        .route("/_admin/ws/publish", post(ws_publish))
        .route("/_admin/ws/kill", post(ws_kill))
        .with_state(hub)
}

#[derive(Deserialize)]
struct Publish {
    topic: String,
    body: String,
}

/// Phase 5 gate: the subscriber on `topic` prints `body`.
async fn ws_publish(
    State(hub): State<Arc<testbed_ws::Hub>>,
    Lenient(body): Lenient<Publish>,
) -> Json<Value> {
    // `None`: an admin publish has no originating connection, so it reaches
    // every member including one that happens to be the test's own client.
    let delivered = hub.publish(&body.topic, &body.body, None);
    tracing::info!(topic = %body.topic, delivered, "admin publish");
    Json(json!({ "ok": true, "delivered": delivered }))
}

#[derive(Deserialize)]
struct Kill {
    topic: String,
    /// What the client sees in the close frame.
    #[serde(default = "default_kill_reason")]
    reason: String,
}

fn default_kill_reason() -> String {
    "closed by testbed".to_string()
}

/// Phase 5 gate: the subscriber exits on a clean close, not a read timeout.
///
/// Trap T6 lives on the other side of this call — the hub queues an explicit
/// Close frame rather than dropping the connection's channel, because a drop
/// looks to the client like a network failure and silently invalidates exactly
/// the reconnection tests this endpoint exists for.
async fn ws_kill(
    State(hub): State<Arc<testbed_ws::Hub>>,
    Lenient(body): Lenient<Kill>,
) -> Json<Value> {
    let closed = hub.kill(&body.topic, &body.reason);
    tracing::info!(topic = %body.topic, closed, "admin kill");
    Json(json!({ "ok": true, "closed": closed }))
}

async fn ws_presence(State(hub): State<Arc<testbed_ws::Hub>>) -> Json<Value> {
    Json(json!({
        "connections": hub.connections(),
        "topics": hub.presence(),
    }))
}

/// Job inspection and enqueueing.
pub fn jobs_router(scheduler: Arc<testbed_queue::Scheduler>, state: Shared) -> Router {
    Router::new()
        .route("/_admin/jobs", get(list_jobs).post(enqueue_job))
        .route("/_admin/jobs/{id}", get(get_job))
        .with_state((scheduler, state))
}

#[derive(Deserialize)]
struct NewJob {
    kind: String,
    /// Delay in **virtual** milliseconds. The Phase 4 gate enqueues 30_000 and
    /// then advances the clock rather than waiting.
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    backoff_ms: Option<Vec<u64>>,
}

/// Phase 4 gate: `{"id":"<uuid>"}`.
async fn enqueue_job(
    State((scheduler, state)): State<(Arc<testbed_queue::Scheduler>, Shared)>,
    Lenient(body): Lenient<NewJob>,
) -> Result<Json<Value>, JobError> {
    let due_at = state.clock().now() + chrono::TimeDelta::milliseconds(body.delay_ms as i64);

    let mut job = testbed_queue::Job::new(state.run(), body.kind, due_at);
    if let Some(payload) = body.payload {
        job = job.with_payload(payload);
    }
    if let Some(max) = body.max_attempts {
        job = job.with_max_attempts(max);
    }
    if let Some(backoff) = body.backoff_ms {
        job = job.with_backoff(backoff);
    }

    // T10: the enqueue trace is recorded so the execution span can *link* to
    // it later rather than descend from it.
    if let Some((trace, span)) = testbed_telemetry::propagation::current_ids() {
        job = job.with_trace(trace, span);
    }

    let id = job.id;
    scheduler.store().put(job).await.map_err(JobError::Store)?;

    Ok(Json(json!({ "id": id.to_string(), "due_at": due_at })))
}

async fn get_job(
    State((scheduler, _)): State<(Arc<testbed_queue::Scheduler>, Shared)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Value>, JobError> {
    let uuid: uuid::Uuid = id.parse().map_err(|_| JobError::BadId(id.clone()))?;
    let job = scheduler
        .store()
        .get(testbed_core::JobId(uuid))
        .await
        .map_err(JobError::Store)?;

    Ok(Json(serde_json::to_value(job).unwrap_or(Value::Null)))
}

async fn list_jobs(
    State((scheduler, _)): State<(Arc<testbed_queue::Scheduler>, Shared)>,
) -> Result<Json<Value>, JobError> {
    let jobs = scheduler.store().list().await.map_err(JobError::Store)?;
    Ok(Json(serde_json::to_value(jobs).unwrap_or(Value::Null)))
}

#[derive(Debug, thiserror::Error)]
enum JobError {
    #[error("{0:?} is not a job id")]
    BadId(String),
    #[error(transparent)]
    Store(testbed_queue::StoreError),
}

impl axum::response::IntoResponse for JobError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Self::BadId(_) => axum::http::StatusCode::BAD_REQUEST,
            Self::Store(testbed_queue::StoreError::NotFound(_)) => {
                axum::http::StatusCode::NOT_FOUND
            }
            Self::Store(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
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
async fn advance(State(state): State<Shared>, Lenient(body): Lenient<Advance>) -> Json<Value> {
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

/// Where `POST /_admin/snapshot` writes when the caller names no path.
pub const DEFAULT_SNAPSHOT: &str = "testbed-snapshot.sqlite";

#[derive(Deserialize)]
struct SnapshotRequest {
    #[serde(default)]
    path: Option<String>,
}

/// Phase 9 gate: writes control-plane state to SQLite.
///
/// The data plane is explicitly not written — see `core::snapshot` for why
/// that is a design constraint rather than a missing feature. This route
/// therefore touches no Postgres, which is also why it lives on the plain admin
/// router rather than alongside `/_admin/runs`.
async fn write_snapshot(
    State(state): State<Shared>,
    Lenient(body): Lenient<SnapshotRequest>,
) -> Result<Json<Value>, SnapshotApiError> {
    let path = body.path.unwrap_or_else(|| DEFAULT_SNAPSHOT.to_string());
    let snapshot = testbed_core::Snapshot::capture(&state);
    snapshot.write(&path)?;

    tracing::info!(%path, run = %snapshot.run, "control plane snapshotted");
    Ok(Json(json!({
        "ok": true,
        "path": path,
        "run": snapshot.run.to_string(),
        "clock_offset_ms": snapshot.clock_offset_ms,
        "restore_with": format!("testbed --restore {path}"),
    })))
}

/// Reads a snapshot back without applying it.
///
/// Restoring happens at boot (`--restore`), never here: swapping the control
/// plane under a running server would leave every in-flight request, queued
/// job and open connection referring to a run that no longer matches the
/// state, and `reset` is already the in-process way back to a known state.
async fn read_snapshot(
    axum::extract::Query(params): axum::extract::Query<SnapshotRequest>,
) -> Result<Json<Value>, SnapshotApiError> {
    let path = params.path.unwrap_or_else(|| DEFAULT_SNAPSHOT.to_string());
    let snapshot = testbed_core::Snapshot::read(&path)?;
    Ok(Json(serde_json::to_value(&snapshot).unwrap_or(Value::Null)))
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct SnapshotApiError(#[from] testbed_core::SnapshotError);

impl axum::response::IntoResponse for SnapshotApiError {
    fn into_response(self) -> axum::response::Response {
        use testbed_core::SnapshotError;

        let status = match &self.0 {
            SnapshotError::Missing(_) | SnapshotError::Empty(_) => {
                axum::http::StatusCode::NOT_FOUND
            }
            SnapshotError::Version { .. } => axum::http::StatusCode::CONFLICT,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.0.to_string() }))).into_response()
    }
}

/// Phase 8: the corruption the exporter shim applies.
///
/// This is control-plane config like any other — seeded by the scenario's
/// `[telemetry]` table, overridden here, restored by `reset`. The shim reads
/// the resolved value on every export, so a change takes effect on the next
/// batch without a restart.
async fn telemetry_faults(State(state): State<Shared>) -> Json<testbed_core::TelemetryFault> {
    Json(state.resolved().telemetry.clone())
}

/// Replaces the whole fault, rather than merging: these fields interact (a
/// `rate` of 0 disables every other one), and a partial update would leave
/// callers guessing which of the nine are still set from last time.
async fn set_telemetry_faults(
    State(state): State<Shared>,
    Lenient(fault): Lenient<testbed_core::TelemetryFault>,
) -> Json<Value> {
    tracing::warn!(
        rate = fault.rate,
        orphan_spans = fault.orphan_spans,
        drop_export = fault.drop_export,
        cardinality_bomb = ?fault.cardinality_bomb,
        "telemetry faults set; exported telemetry is now deliberately wrong"
    );
    state.mutate(|overlay| overlay.telemetry = Some(fault.clone()));
    Json(json!({ "ok": true, "telemetry": fault }))
}

/// Back to honest telemetry, including when the *scenario* seeded faults —
/// `reset` puts the scenario's back, this does not.
async fn clear_telemetry_faults(State(state): State<Shared>) -> Json<Value> {
    state.mutate(|overlay| overlay.telemetry = Some(testbed_core::TelemetryFault::default()));
    Json(json!({ "ok": true }))
}

async fn list_faults(State(state): State<Shared>) -> Json<Vec<FaultSpec>> {
    Json(state.resolved().faults.clone())
}

/// Appends to the *effective* fault list, so posting a rule adds to whatever
/// the scenario seeded rather than silently replacing it.
async fn add_fault(State(state): State<Shared>, Lenient(spec): Lenient<FaultSpec>) -> Json<Value> {
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
