//! The telemetry spine: subscriber, OTLP export, W3C propagation, metrics, and
//! the exporter shim that corrupts them on demand.
//!
//! Telemetry is a first-class output surface here, not operator debugging
//! (HANDOFF §2 decision 9). The testbed is a telemetry *source* you point real
//! observability tooling at — including a source that can be told to lie.
//!
//! # Q5 — resolved
//!
//! Operator decision: always export OTLP to a collector endpoint; Jaeger and
//! Prometheus sit behind the `obs` compose profile so the base stack stays
//! light. The application only ever speaks OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT`,
//! so what runs behind that is purely a compose-file concern.
//!
//! # Not yet built — Phase 2b (HANDOFF §9 task 8)
//!
//! - `tracing-opentelemetry` layer and the OTLP exporter
//! - W3C `traceparent` extraction (*continued*, never replaced — a fresh trace
//!   id means frontend RUM can never join to backend spans) and injection
//! - `/metrics`, serving RED per surface plus the baseline testbed gauges
//! - the exporter shim applying [`testbed_core::TelemetryFault`] (invariant 11)
//! - `shutdown_tracer_provider()` on the signal handler, or the last batch
//!   vanishes and takes the spans you were investigating with it (trap T11)

pub mod wall;

/// Reads the collector endpoint from the environment, falling back to the
/// local collector that `compose --profile obs` brings up.
pub fn otlp_endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string())
}

/// Service name reported to the collector. The Phase 2b gate queries Jaeger for
/// `service=testbed`, so this string is part of the contract.
pub const SERVICE_NAME: &str = "testbed";

/// Installs a stdout subscriber. Phase 2b replaces this with the full stack;
/// until then the server is not silent.
pub fn init_console_subscriber() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,testbed=debug"));

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();
}

/// Attaching an attribute discovered *after* a span opens — a status code, a
/// job outcome — requires declaring the field as `tracing::field::Empty` up
/// front and `record()`-ing it later (trap T12). Forgetting this yields spans
/// silently missing their most useful attribute, so the field names every
/// surface records late are listed here rather than spelled inline.
pub mod late {
    pub const HTTP_STATUS: &str = "http.status_code";
    pub const HTTP_FAULTS: &str = "testbed.faults";
    pub const JOB_OUTCOME: &str = "testbed.job.outcome";
    pub const JOB_ATTEMPT: &str = "testbed.job.attempt";
    pub const WEBHOOK_STATUS: &str = "testbed.webhook.status";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otlp_endpoint_falls_back_to_the_compose_collector() {
        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
            assert_eq!(otlp_endpoint(), "http://localhost:4317");
        }
    }
}
