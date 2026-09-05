//! Helpers shared by the integration tests in this directory.
//!
//! Integration test binaries do not share code except through a module like
//! this one, which is compiled into each `mod common;` that declares it.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;

/// Installs a process-wide tracer with no exporter: spans get real, valid ids
/// and nothing leaves the process.
///
/// Global, not `with_default`, because the spans under test are opened on
/// *other* tasks — a connection task, an admin handler, the webhook sender. A
/// thread-scoped subscriber reaches none of them.
///
/// Without this, anything reading the current trace context silently gets
/// nothing: a bus event with no `trace_id` (reads as an invariant-9 violation)
/// or an outbound request with no `traceparent` (reads as an invariant-10 one).
/// Both are artefacts of the test environment, not the code — the real server
/// installs a subscriber in `testbed_telemetry::init`.
pub fn install_tracing() {
    static PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

    let provider = PROVIDER.get_or_init(|| SdkTracerProvider::builder().build());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("tests")));

    // Every test in a binary shares the process, so all but the first call lose
    // the race. That is fine — they wanted the same subscriber.
    let _ = tracing::subscriber::set_global_default(subscriber);
}
