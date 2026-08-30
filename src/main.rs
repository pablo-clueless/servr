mod api;
mod config;
mod cron;
mod error;
mod queue;
mod schema;
mod smtp;
mod state;
#[cfg(test)]
mod tests;
mod websocket;

use crate::config::Config;
use crate::smtp::SmtpService;
use crate::state::{AppState, Database};
use axum::{routing::get, Router};
use std::sync::Arc;
use tokio::signal;
use tokio::sync::mpsc;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    tracing::info!("Starting Rust Production Server...");

    let cfg = Config::from_env();

    let mailer = Arc::new(
        SmtpService::new(
            &cfg.smtp_host,
            cfg.smtp_port,
            cfg.smtp_user.as_deref(),
            cfg.smtp_pass.as_deref(),
        )
        .expect("Failed to initialize SMTP service"),
    );

    let (job_tx, job_rx) = mpsc::channel(100);

    let db = Arc::new(Database {
        url: cfg.database_url,
    });

    let state = Arc::new(AppState {
        mailer: mailer.clone(),
        job_tx,
        db: db.clone(),
    });

    let worker_mailer = mailer.clone();
    tokio::spawn(async move {
        queue::start_worker(job_rx, worker_mailer).await;
    });

    let ping_url = cfg.self_ping_url.clone();
    tokio::spawn(async move {
        cron::start_self_ping(ping_url).await;
    });

    let app = Router::new()
        .merge(api::create_router())
        .route("/ws", get(websocket::ws_handler))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = format!("0.0.0.0:{}", cfg.server_port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Server listening on http://{}", addr);

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("Shutdown signal received (Ctrl+C)"); },
        _ = terminate => { tracing::info!("Shutdown signal received (SIGTERM)"); },
    }
}
