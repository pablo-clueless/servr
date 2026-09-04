//! Invariant 10, the WebSocket half, asserted in CI.
//!
//! A frame span **links** to its connection span; it does not descend from it.
//! The reasoning is trap T10's, applied to a different long-lived parent: a
//! connection held open for the length of a test suite would otherwise produce
//! a trace that long, with every frame nested inside it. That renders in a
//! waterfall UI as one bar and several thousand slivers, which is worse than no
//! trace at all.
//!
//! Asserted against an in-memory exporter so a regression fails the build,
//! rather than waiting for someone to notice it in Jaeger.

use std::sync::Arc;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk_testing::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use testbed_core::{BroadcastBus, Clock, ConnId, EventSink, RunId, SpanId, TraceId};
use testbed_ws::Hub;
use tracing_subscriber::layer::SubscriberExt;

/// The ids from the HANDOFF §7 phase 2b gate, standing in for a connection span.
const CONN_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const CONN_SPAN: &str = "00f067aa0ba902b7";

fn conn_trace() -> Option<(TraceId, SpanId)> {
    Some((CONN_TRACE.parse().unwrap(), CONN_SPAN.parse().unwrap()))
}

/// Publishes one frame to a subscriber holding `trace`, and returns what was
/// exported.
async fn exported_spans(trace: Option<(TraceId, SpanId)>) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();

    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
    let _guard = tracing::subscriber::set_default(subscriber);

    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    let hub = Hub::new(bus as Arc<dyn EventSink>, clock, run);

    // The receiver is held: dropping it would close the channel, and the frame
    // would be counted as undeliverable instead of emitting a span.
    let _rx = hub.join("demo", ConnId::new(), trace);
    assert_eq!(hub.publish("demo", "hello", None), 1);

    drop(_guard);
    provider.force_flush().ok();
    exporter.get_finished_spans().unwrap()
}

/// Matched on an attribute rather than a name: the exported name comes from
/// `otel.name`, so matching a literal name silently finds nothing the moment
/// someone adjusts how frames are labelled in a trace UI.
fn frame_span(spans: &[SpanData]) -> &SpanData {
    spans
        .iter()
        .find(|s| {
            s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "testbed.ws.topic")
        })
        .expect("no frame span was exported")
}

#[tokio::test]
async fn a_frame_span_links_to_its_connection_span() {
    let spans = exported_spans(conn_trace()).await;
    let frame = frame_span(&spans);

    assert_eq!(
        frame.links.iter().count(),
        1,
        "the frame span carries no link back to its connection"
    );

    let link = frame.links.iter().next().unwrap();
    assert_eq!(link.span_context.trace_id().to_string(), CONN_TRACE);
    assert_eq!(link.span_context.span_id().to_string(), CONN_SPAN);
}

/// The half that causes the damage. A frame descending from the connection
/// means the trace lasts as long as the connection does.
#[tokio::test]
async fn a_frame_span_does_not_descend_from_its_connection_span() {
    let spans = exported_spans(conn_trace()).await;
    let frame = frame_span(&spans);

    assert_ne!(
        frame.parent_span_id.to_string(),
        CONN_SPAN,
        "the frame descends from the connection span; a long-lived connection \
         will produce a trace as long as itself"
    );
    assert_ne!(
        frame.span_context.trace_id().to_string(),
        CONN_TRACE,
        "the frame joined the connection's trace instead of linking to it"
    );
}

/// A connection opened without usable trace context still emits frame spans —
/// unlinked, rather than linked to an all-zero id that joins nothing.
#[tokio::test]
async fn a_frame_from_an_untraced_connection_is_an_unlinked_root() {
    let spans = exported_spans(None).await;
    assert_eq!(frame_span(&spans).links.iter().count(), 0);
}

#[tokio::test]
async fn an_invalid_connection_context_is_not_linked_to() {
    let spans = exported_spans(Some((TraceId::INVALID, SpanId::INVALID))).await;
    assert_eq!(
        frame_span(&spans).links.iter().count(),
        0,
        "linked to an all-zero id, which resolves to nothing in any trace UI"
    );
}

/// The direction has to be on the span, not only on the bus event — the two
/// surfaces are read together and a frame that is out on one and unlabelled on
/// the other cannot be reconciled.
#[tokio::test]
async fn a_frame_span_carries_its_topic_and_direction() {
    let spans = exported_spans(conn_trace()).await;
    let frame = frame_span(&spans);

    let attr = |key: &str| {
        frame
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .unwrap_or_else(|| panic!("the frame span has no {key}"))
            .value
            .as_str()
            .to_string()
    };

    assert_eq!(attr("testbed.ws.topic"), "demo");
    assert_eq!(attr("testbed.ws.dir"), "out");
}
