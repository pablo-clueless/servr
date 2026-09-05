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
//! # Telemetry chaos — Phase 8
//!
//! The exporter shim applying [`testbed_core::TelemetryFault`] lives in
//! [`chaos`]. It is in the export path and **only** there (invariant 11):
//! corrupting spans where they are created poisons the testbed's own
//! debuggability, whereas corrupting them on the way out confines the damage to
//! what leaves the process.
//!
//! The export path is also exempt from invariant 4 — it emits no bus events.
//! Export emits an event, the event triggers instrumentation, instrumentation
//! queues a span, export runs again; the batch exporter delays the recursion
//! just long enough to make it puzzling (trap T13).

pub mod chaos;
pub mod metrics;
pub mod propagation;
pub mod wall;

use std::sync::Arc;
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

/// Auth headers for the collector, from `OTEL_EXPORTER_OTLP_HEADERS`.
///
/// Every hosted OTLP backend authenticates: Grafana Cloud takes
/// `Authorization=Basic <base64 instance:token>`, Honeycomb takes
/// `x-honeycomb-team=<key>`, Dash0 takes a bearer token. Without this the
/// exporter can reach them and is rejected, which surfaces as the same endless
/// export error as not reaching them at all.
///
/// The format is the OpenTelemetry specification's: comma-separated
/// `key=value`, values may contain `=` (base64 padding does), so the split is
/// on the *first* one only.
fn otlp_headers() -> tonic::metadata::MetadataMap {
    use tonic::metadata::{MetadataKey, MetadataValue};

    let mut metadata = tonic::metadata::MetadataMap::new();
    let Ok(raw) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") else {
        return metadata;
    };

    for pair in raw.split(',') {
        let Some((key, value)) = pair.split_once('=') else {
            tracing::warn!(
                "ignoring malformed OTEL_EXPORTER_OTLP_HEADERS entry {:?}; expected key=value",
                pair
            );
            continue;
        };

        match (
            MetadataKey::from_bytes(key.trim().to_ascii_lowercase().as_bytes()),
            MetadataValue::try_from(value.trim()),
        ) {
            (Ok(k), Ok(v)) => {
                metadata.insert(k, v);
            }
            // Never log the value: these are credentials.
            _ => tracing::warn!("ignoring unusable OTLP header {:?}", key.trim()),
        }
    }

    metadata
}

/// Whether anything is listening at `endpoint`.
///
/// A plain TCP connect, not a gRPC handshake: the question is only "is this an
/// address where a collector could be", and a refused connection answers it.
/// Deliberately short — this runs on the boot path, and a slow DNS lookup for a
/// misconfigured host should not hold the server down.
fn collector_reachable(endpoint: &str) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};

    let secure = endpoint.starts_with("https://");
    let authority = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');

    // A hosted endpoint is usually written without a port —
    // `https://otlp-gateway-prod-us-east-0.grafana.net` — and `to_socket_addrs`
    // cannot resolve a bare host. Inferring it from the scheme is what tonic
    // does, and getting this wrong would disable export against a collector
    // that is perfectly reachable.
    let authority = if authority
        .rsplit(':')
        .next()
        .is_some_and(|p| p.parse::<u16>().is_ok())
    {
        authority.to_string()
    } else if secure {
        format!("{authority}:443")
    } else {
        format!("{authority}:80")
    };

    // `to_socket_addrs` performs the DNS lookup; a name that does not resolve
    // is as unreachable as a port nothing is listening on.
    let Ok(mut addrs) = authority.to_socket_addrs() else {
        return false;
    };

    addrs.any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok())
}

/// Whether span export is switched off entirely.
///
/// # Why this exists
///
/// Building the OTLP exporter succeeds without connecting to anything, so a
/// deployment with no collector boots reporting `exporting=true` and then logs
/// a `BatchSpanProcessor.ExportError` at ERROR level every five seconds,
/// forever. On a hosted platform that is the entire log — the one place an
/// operator looks to find out what the testbed is doing.
///
/// The default endpoint is `localhost:4317`, which is right for
/// `compose --profile obs` and wrong for everywhere else, so "no collector" has
/// to be sayable. `OTEL_SDK_DISABLED` is the OpenTelemetry specification's own
/// switch for it; an explicitly empty `OTEL_EXPORTER_OTLP_ENDPOINT` is accepted
/// too, because setting a variable to nothing is the obvious way to express
/// this and silently falling back to localhost would be a trap.
pub fn export_disabled() -> bool {
    if let Ok(flag) = std::env::var("OTEL_SDK_DISABLED") {
        if matches!(flag.trim().to_ascii_lowercase().as_str(), "true" | "1") {
            return true;
        }
    }
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok_and(|e| e.trim().is_empty())
}

/// Live telemetry. Hold it for the process lifetime and call
/// [`Telemetry::shutdown`] on the way out.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
    prometheus: PrometheusHandle,
    /// Read at scrape time so `cardinality_bomb` and `counter_reset` corrupt the
    /// rendered text rather than the recorder. See [`chaos`].
    faults: Arc<dyn chaos::Faults>,
}

impl Telemetry {
    /// Renders the Prometheus exposition format for `/metrics`, with any
    /// configured metric faults applied to the *text* on the way out.
    pub fn render_metrics(&self) -> String {
        chaos::corrupt_metrics(self.prometheus.render(), &self.faults.current())
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
pub fn init(run: testbed_core::RunId, faults: Arc<dyn chaos::Faults>) -> Telemetry {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,testbed=debug"));

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let provider = build_provider(run, Arc::clone(&faults));
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
        faults,
    }
}

fn build_provider(
    run: testbed_core::RunId,
    faults: Arc<dyn chaos::Faults>,
) -> Option<SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;

    if export_disabled() {
        tracing::info!(
            "span export disabled (OTEL_SDK_DISABLED); metrics and /_admin/events are unaffected"
        );
        return None;
    }

    let endpoint = otlp_endpoint();

    // Probed whether the endpoint was configured or defaulted.
    //
    // An earlier version trusted an explicitly-set endpoint on the grounds that
    // the operator had said where the collector was. That reasoning does not
    // survive contact with a real deployment: the most common wrong value is a
    // *stale* one — `127.0.0.1:4317` copied from a local `.env` into a hosted
    // environment — and trusting it produced exactly the failure the probe
    // exists to prevent, an ERROR every five seconds forever.
    //
    // The case this gives up is a collector that starts *after* the testbed.
    // That is rarer than stale config, recoverable by a restart, and does not
    // arise in this repo's own workflow: `make up-obs` uses `--wait`, so the
    // collector is healthy before the server runs.
    //
    // Same shape as the Postgres, Redis and Mailpit probes: check, degrade, and
    // say so. Telemetry was the one optional dependency that reported
    // `exporting=true` without ever looking.
    if !collector_reachable(&endpoint) {
        tracing::warn!(
            %endpoint,
            "no collector reachable; span export disabled. Unset              OTEL_EXPORTER_OTLP_ENDPOINT to silence this, or point it at a real              collector. Metrics and /_admin/events are unaffected."
        );
        return None;
    }

    let headers = otlp_headers();
    if !headers.is_empty() {
        tracing::debug!(count = headers.len(), "OTLP auth headers configured");
    }

    let exporter = {
        use opentelemetry_otlp::WithTonicConfig;

        opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .with_timeout(Duration::from_secs(3))
            .with_metadata(headers)
            .build()
    };

    match exporter {
        Ok(exporter) => Some(
            SdkTracerProvider::builder()
                // Invariant 11: every span leaves through the shim.
                .with_batch_exporter(chaos::ChaosExporter::new(exporter, faults))
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

/// Span links: "this happened after that", without "that" being its parent.
///
/// # Why linking and not parenting
///
/// Parenting is the intuitive choice and it is wrong wherever the two spans are
/// separated by an arbitrary amount of time. A queue job delayed 30 minutes
/// produces a 30-minute trace (trap T10); a WebSocket connection held open for
/// the length of a test suite produces a trace as long as the suite. Once a few
/// of those exist, every trace-waterfall UI pointed at the testbed becomes
/// unusable — which is the opposite of what a telemetry source is for.
///
/// A link says the same thing without the cost: the new span is a trace root
/// carrying a `FOLLOWS_FROM` reference back to the span that caused it. Jaeger
/// renders it as a reference; the Phase 4 gate counts it.
pub mod link {
    use opentelemetry::trace::{SpanContext, TraceFlags, TraceState};
    use testbed_core::{SpanId, TraceId};

    /// Adds a `FOLLOWS_FROM` reference from `span` back to `(trace, parent)`.
    ///
    /// A no-op when either id is invalid, so a caller with no recorded context
    /// — an unsampled trace, or a surface reached without one — degrades to an
    /// unlinked root span rather than a link pointing at nothing.
    pub fn follows_from(span: &tracing::Span, trace: TraceId, parent: SpanId) {
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        if !trace.is_valid() || !parent.is_valid() {
            return;
        }

        span.add_link(SpanContext::new(
            opentelemetry::trace::TraceId::from_bytes(trace.to_bytes()),
            opentelemetry::trace::SpanId::from_bytes(parent.to_bytes()),
            TraceFlags::SAMPLED,
            // Remote: the linked span was created outside this task's context.
            true,
            TraceState::default(),
        ));
    }

    /// The same, for a caller holding an optional context — the common shape,
    /// since a recorded trace context is always optional.
    pub fn follows_from_opt(span: &tracing::Span, ids: Option<(TraceId, SpanId)>) {
        if let Some((trace, parent)) = ids {
            follows_from(span, trace, parent);
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

    /// A hosted collector is written without a port; inferring it wrongly would
    /// disable export against a collector that is perfectly reachable.
    #[test]
    fn a_bare_https_host_probes_port_443_not_nothing() {
        // Resolvable and listening on 443. If this ever fails for network
        // reasons it fails loudly rather than silently asserting nothing.
        assert!(
            collector_reachable("https://example.com"),
            "a bare https host was treated as unreachable; check the port inference"
        );
    }

    #[test]
    fn an_endpoint_with_nothing_listening_is_unreachable() {
        // Port 1 on loopback: reserved, never bound.
        assert!(!collector_reachable("http://127.0.0.1:1"));
    }

    #[test]
    fn a_hostname_that_does_not_resolve_is_unreachable() {
        assert!(!collector_reachable(
            "http://collector.invalid.testbed.example:4317"
        ));
    }

    #[test]
    fn otlp_headers_parse_the_spec_format() {
        // Base64 values contain `=`, so only the first one separates.
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_HEADERS",
            "Authorization=Basic dXNlcjpwYXNz==,x-scope-orgid=tenant1",
        );
        let headers = otlp_headers();
        std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");

        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers.get("authorization").unwrap(),
            "Basic dXNlcjpwYXNz=="
        );
        assert_eq!(headers.get("x-scope-orgid").unwrap(), "tenant1");
    }

    #[test]
    fn malformed_header_entries_are_skipped_not_fatal() {
        std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", "novalue,good=yes");
        let headers = otlp_headers();
        std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");

        assert_eq!(headers.len(), 1);
        assert_eq!(headers.get("good").unwrap(), "yes");
    }

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
