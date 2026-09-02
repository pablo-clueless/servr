//! Wires the surfaces together and serves them on one port.
//!
//! The router is assembled in two halves on purpose: the data plane goes
//! through the fault layer, the control plane does not. See
//! `testbed_http::fault` for why that separation is load-bearing.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use testbed_core::{BroadcastBus, Clock, RunId, Scenario, State};
use tokio::signal;

/// Per-subscriber backlog on the event bus. A subscriber falling further behind
/// than this lags, and lagging is reported as `EventKind::Gap`, never hidden.
const BUS_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() {
    let run = RunId::new();
    let telemetry = Arc::new(testbed_telemetry::init(run));

    let scenario_path =
        std::env::var("TESTBED_SCENARIO").unwrap_or_else(|_| "scenarios/default.toml".to_string());
    let scenario = match Scenario::from_path(&scenario_path) {
        Ok(scenario) => scenario,
        Err(e) => {
            // Booting with a silently empty scenario would make every later
            // assertion meaningless, so this is fatal.
            tracing::error!("{e}");
            std::process::exit(1);
        }
    };

    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(BUS_CAPACITY, Arc::clone(&clock), run));
    let state = Arc::new(State::new(scenario, Arc::clone(&clock), bus, run));

    tracing::info!(
        run = %run,
        scenario = %state.base().name,
        schema = %run.schema(),
        faults = state.resolved().faults.len(),
        otlp = %testbed_telemetry::otlp_endpoint(),
        exporting = telemetry.exporting(),
        "testbed starting"
    );
    if !telemetry.exporting() {
        tracing::warn!(
            "no OTLP collector reachable; run `docker compose --profile obs up -d` for traces"
        );
    }
    if let Some(blast) = &state.base().blast_radius {
        tracing::warn!(blast_radius = %blast, "scenario blast radius");
    }

    // The data plane is optional. Without Postgres the HTTP, telemetry and
    // control-plane surfaces all still work, and `/api/items` answers 503 with
    // an explanation — refusing to boot would make phases 0-2b unusable.
    let data = match std::env::var("DATABASE_URL") {
        Ok(url) => match testbed_http::data::DataPlane::connect(&url).await {
            Ok(plane) => {
                let plane = Arc::new(plane);
                if let Err(e) = plane.create_run(run).await {
                    tracing::warn!("could not prepare the default run's schema: {e}");
                }
                tracing::info!(schema = %run.schema(), "data plane ready");
                Some(plane)
            }
            Err(e) => {
                tracing::warn!("Postgres unreachable ({e}); /api/items will answer 503");
                None
            }
        },
        Err(_) => {
            tracing::warn!("DATABASE_URL unset; /api/items will answer 503");
            None
        }
    };

    let app = Router::new()
        .merge(testbed_admin::router(Arc::clone(&state)))
        .merge(testbed_admin::runs_router(data.clone()))
        .merge(testbed_admin::metrics_route(
            Arc::clone(&state),
            Arc::clone(&telemetry),
        ))
        .merge(testbed_http::router_with_data(Arc::clone(&state), data));

    let port: u16 = std::env::var("TESTBED_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let listener = bind(port).unwrap_or_else(|e| panic!("failed to bind port {port}: {e}"));
    tracing::info!(
        "listening on http://localhost:{port} (admin at {})",
        testbed_admin::PREFIX
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("server error");

    // Trap T11: flush the last span batch, or it takes the spans from whatever
    // you were investigating down with it.
    telemetry.shutdown();
}

/// Binds dual-stack, so `localhost` reaches the server over either family.
///
/// This is not incidental tidiness. Every gate in the HANDOFF is written
/// against `localhost`, and a client resolving that to `::1` first will sit
/// through a refused connection before retrying IPv4 — roughly 200ms on
/// Windows. That is invisible in a correctness check and fatal in a timing one:
/// it pushes the Phase 2 gate's 500ms latency assertion past its 600ms ceiling,
/// and it makes the Phase 4 `real 0m0.2xxs` assertion ambiguous.
///
/// Windows defaults `IPV6_V6ONLY` on, unlike Linux, so it is cleared explicitly
/// rather than relying on the platform default. Falls back to IPv4-only where
/// IPv6 is unavailable.
fn bind(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let dual = || -> std::io::Result<std::net::TcpListener> {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_only_v6(false)?;
        socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        Ok(socket.into())
    };

    let std_listener = match dual() {
        Ok(listener) => listener,
        Err(e) => {
            tracing::warn!("dual-stack bind failed ({e}); falling back to IPv4 only");
            let listener =
                std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))?;
            listener.set_nonblocking(true)?;
            listener
        }
    };

    tokio::net::TcpListener::from_std(std_listener)
}

/// Trap T11: once Phase 2b lands, this must call `shutdown_tracer_provider()`.
/// Without it the OTLP batch exporter drops its last batch — which reliably
/// eats exactly the spans from whatever you were investigating.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("shutdown: SIGINT"),
        _ = terminate => tracing::info!("shutdown: SIGTERM"),
    }
}
