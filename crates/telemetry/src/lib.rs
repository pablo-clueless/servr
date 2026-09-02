//! The telemetry spine: subscriber, OTLP export, W3C propagation, metrics.
//!
//! Telemetry is a first-class output surface here, not operator debugging
//! (HANDOFF §2 decision 9). The testbed is a telemetry *source* you point real
//! observability tooling at — including a source that can be told to lie.
//!
//! # Q5 — resolved
//!
//! Always export OTLP to a collector endpoint; Jaeger and Prometheus sit behind
//! the `obs` compose profile so the base stack stays light. The application
//! speaks only OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT`, so what runs behind that
//! is purely a compose-file concern.
//!
//! # Still owed — Phase 8
//!
//! The exporter shim applying [`testbed_core::TelemetryFault`]. It goes in the
//! export path and **only** there (invariant 11): corrupting spans where they
//! are created poisons the testbed's own debuggability, whereas corrupting them
//! on the way out confines the damage to what leaves the process.
//!
//! The export path is also exempt from invariant 4 — it emits no bus events.
//! Export emits an event, the event triggers instrumentation, instrumentation
//! queues a span, export runs again; the batch exporter delays the recursion
//! just long enough to make it puzzling (trap T13).

pub mod metrics;
pub mod propagation;
pub mod wall;

use std::time::Duration;

use metrics_exporter_prometheus::PrometheusHandle;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Service name reported to the collector. The Phase 2b and Phase 4 gates query
/// Jaeger for `service=testbed`, so this string is part of the contract.
pub const SERVICE_NAME: &str = "testbed";

/// Reads the collector endpoint from the environment, falling back to the
/// collector that `compose --profile obs` brings up.
pub fn otlp_endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string())
}

/// Live telemetry. Hold it for the process lifetime and call
/// [`Telemetry::shutdown`] on the way out.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
    prometheus: PrometheusHandle,
}

impl Telemetry {
    /// Renders the Prometheus exposition format for `/metrics`.
    pub fn render_metrics(&self) -> String {
        self.prometheus.render()
    }

    /// Whether spans are actually being exported, or only logged.
    pub fn exporting(&self) -> bool {
        self.provider.is_some()
    }

    /// Flushes the last batch.
    ///
    /// # Trap T11
    ///
    /// Without this on the signal handler, the OTLP batch exporter drops
    /// whatever it was holding — which is reliably the spans from whatever you
    /// were investigating when you hit Ctrl-C.
    pub fn shutdown(&self) {
        if let Some(provider) = &self.provider {
            if let Err(e) = provider.shutdown() {
                tracing::warn!("tracer shutdown failed, last span batch may be lost: {e}");
            }
        }
    }
}

/// Installs the subscriber, the OTLP exporter and the Prometheus recorder.
///
/// A collector that is unreachable is **not** fatal: the testbed is routinely
/// run without the `obs` profile, and refusing to boot would make the base
/// stack useless. Export is skipped and everything else still works.
pub fn init(run: testbed_core::RunId) -> Telemetry {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,testbed=debug"));

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let provider = build_provider(run);
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer());

    match &provider {
        Some(provider) => {
            let tracer = provider.tracer(SERVICE_NAME);
            registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
        }
        None => registry.init(),
    }

    let prometheus = match metrics::install() {
        Ok(handle) => handle,
        Err(e) => panic!("{e}"),
    };

    Telemetry {
        provider,
        prometheus,
    }
}

fn build_provider(run: testbed_core::RunId) -> Option<SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;

    let endpoint = otlp_endpoint();
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(3))
        .build();

    match exporter {
        Ok(exporter) => Some(
            SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    Resource::builder()
                        .with_service_name(SERVICE_NAME)
                        // Every span carries the run, so a trace can be tied
                        // back to the test that produced it.
                        .with_attribute(KeyValue::new(wall::attr::RUN_ID, run.to_string()))
                        .build(),
                )
                .build(),
        ),
        Err(e) => {
            tracing::warn!(
                "OTLP exporter unavailable at {endpoint} ({e}); traces will not be exported"
            );
            None
        }
    }
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

    #[test]
    fn the_service_name_matches_what_the_gates_query() {
        assert_eq!(SERVICE_NAME, "testbed");
    }
}
