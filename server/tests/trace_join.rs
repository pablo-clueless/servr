//! The half of the Phase 2b gate that does not need a collector.
//!
//! The gate compares a bus event's `trace_id` against Jaeger's `traceID` for
//! the same request. Two things have to hold for that to work, and only one of
//! them needs a collector:
//!
//! 1. the inbound `traceparent` is **continued**, not replaced — a fresh trace
//!    id means frontend RUM can never join to backend spans;
//! 2. the bus event carries the trace context of the span it happened under
//!    (invariant 9), so the event stream and the trace tree are joinable.
//!
//! Both are asserted here, in-process. The collector comparison stays a manual
//! gate because only a real export can prove what actually left the process.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use futures_util::StreamExt;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use testbed_core::{BroadcastBus, Clock, Event, RunId, Scenario, State};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

/// The traceparent from the HANDOFF §7 phase 2b gate.
const GATE_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const GATE_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const GATE_PARENT_SPAN: &str = "00f067aa0ba902b7";

/// A tracer with no exporter. Spans get real, valid ids; nothing leaves the
/// process. Enough to assert on trace context without the `obs` stack.
fn subscriber() -> impl tracing::Subscriber + Send + Sync {
    let provider = SdkTracerProvider::builder().build();
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")))
}

fn state() -> Arc<State> {
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    Arc::new(State::new(
        Scenario {
            name: "test".into(),
            ..Default::default()
        },
        clock,
        bus,
        run,
    ))
}

async fn ping_with(state: &Arc<State>, traceparent: Option<&str>) -> Event {
    let mut events = state.bus().subscribe();

    let mut request = Request::builder().uri("/api/ping");
    if let Some(tp) = traceparent {
        request = request.header("traceparent", tp);
    }

    testbed_http::router(Arc::clone(state))
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(50), events.next())
        .await
        .expect("no event within 50ms")
        .expect("event stream ended")
}

#[test]
fn an_inbound_traceparent_is_continued_onto_the_bus_event() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let event = tracing::subscriber::with_default(subscriber(), || {
        runtime.block_on(ping_with(&state(), Some(GATE_TRACEPARENT)))
    });

    let trace_id = event.trace_id.expect("event carried no trace id");
    assert_eq!(
        trace_id.to_string(),
        GATE_TRACE_ID,
        "the inbound trace was replaced rather than continued; frontend RUM \
         could never be joined to these spans"
    );

    let span_id = event.span_id.expect("event carried no span id");
    assert_ne!(
        span_id.to_string(),
        GATE_PARENT_SPAN,
        "the event reused the caller's span id instead of its own child span"
    );
    assert!(event.is_joinable());
}

#[test]
fn a_request_without_context_still_gets_a_joinable_trace() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let event = tracing::subscriber::with_default(subscriber(), || {
        runtime.block_on(ping_with(&state(), None))
    });

    assert!(
        event.is_joinable(),
        "a request with no inbound context must still start its own trace"
    );
    assert_ne!(event.trace_id.unwrap().to_string(), GATE_TRACE_ID);
}

#[test]
fn a_malformed_traceparent_does_not_break_the_request() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // What `TelemetryFault::corrupt_inbound_traceparent` will produce in Phase 8.
    let event = tracing::subscriber::with_default(subscriber(), || {
        runtime.block_on(ping_with(&state(), Some("00-garbage-garbage-01")))
    });

    assert!(
        event.is_joinable(),
        "a corrupt inbound context must fall back to a new trace, not no trace"
    );
    assert_ne!(event.trace_id.unwrap().to_string(), GATE_TRACE_ID);
}
