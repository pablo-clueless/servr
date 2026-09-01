//! Wires the surfaces together and serves them on one port.
//!
//! Phase 0 scaffold: the router is real, most of what it mounts is not yet.
//! See each crate's module docs for what its phase still owes.

use std::net::SocketAddr;

use axum::Router;
use testbed_core::{Clock, RunId};
use tokio::signal;

#[tokio::main]
async fn main() {
    testbed_telemetry::init_console_subscriber();

    let run = RunId::new();
    let clock = Clock::new();

    tracing::info!(
        run = %run,
        schema = %run.schema(),
        virtual_now = %clock.now(),
        otlp = %testbed_telemetry::otlp_endpoint(),
        "testbed starting"
    );

    let app = Router::new()
        .merge(testbed_admin::router(run))
        .merge(testbed_http::router())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let port: u16 = std::env::var("TESTBED_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));
    tracing::info!(
        "listening on http://{addr} (admin at {})",
        testbed_admin::PREFIX
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("server error");
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
