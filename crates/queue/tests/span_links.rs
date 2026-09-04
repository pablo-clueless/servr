//! Trap T10, asserted in CI.
//!
//! A queue job's execution span must **link** to the enqueue span, not descend
//! from it. Parenting is the intuitive choice and it is wrong: a job delayed 30
//! minutes then produces a 30-minute trace, and once a few of those exist every
//! trace-waterfall UI you point at the testbed becomes unusable.
//!
//! The Phase 4 gate checks this by querying Jaeger for a `FOLLOWS_FROM`
//! reference. That needs the `obs` stack; this asserts the same property
//! against an in-memory exporter, so a regression fails the build rather than
//! waiting for someone to run the gate by hand.

use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk_testing::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use testbed_core::{BroadcastBus, Clock, EventSink, RunId, SpanId, TraceId};
use testbed_queue::{Job, JobStore, MemoryStore, Scheduler};
use tracing_subscriber::layer::SubscriberExt;

/// The ids from the HANDOFF §7 phase 2b gate, reused as a stand-in enqueue span.
const ENQUEUE_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const ENQUEUE_SPAN: &str = "00f067aa0ba902b7";

/// Runs one job to completion under an in-memory exporter and returns what it
/// exported. The clock is advanced so the job is due immediately — the delay
/// exists to make the point that a long delay must not become a long trace.
async fn exported_spans(job: Job) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();

    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));

    // `with_default` takes a closure, and the work inside is async, so the
    // guard is held explicitly across the awaits instead.
    let _guard = tracing::subscriber::set_default(subscriber);

    let run = job.run;
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    let store = Arc::new(MemoryStore::new());
    store.put(job).await.unwrap();

    let scheduler = Scheduler::new(
        Arc::clone(&store) as Arc<dyn JobStore>,
        Arc::clone(&clock),
        bus as Arc<dyn EventSink>,
        run,
    );
    clock.advance(Duration::from_secs(1800));
    scheduler.tick().await;
    drop(_guard);

    provider.force_flush().ok();
    exporter.get_finished_spans().unwrap()
}

/// Finds the job-execution span by attribute rather than by name.
///
/// The span's exported name comes from its `otel.name` field (`"job noop"`),
/// not from the `tracing` macro's name — so matching on a literal name silently
/// finds nothing the moment someone adjusts how jobs are labelled in a trace UI.
fn execution_span(spans: &[SpanData]) -> &SpanData {
    spans
        .iter()
        .find(|s| {
            s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "testbed.job.kind")
        })
        .expect("no job execution span was exported")
}

fn delayed_job_with_trace() -> Job {
    Job::new(
        RunId::new(),
        "noop",
        chrono::Utc::now() + TimeDelta::minutes(30),
    )
    .with_trace(
        ENQUEUE_TRACE.parse::<TraceId>().unwrap(),
        ENQUEUE_SPAN.parse::<SpanId>().unwrap(),
    )
}

/// The assertion the Phase 4 gate makes against Jaeger.
#[tokio::test]
async fn the_execution_span_links_to_the_enqueue_span() {
    let spans = exported_spans(delayed_job_with_trace()).await;
    let execution = execution_span(&spans);

    assert_eq!(
        execution.links.iter().count(),
        1,
        "the execution span carries no link to the enqueue span (T10)"
    );

    let link = execution.links.iter().next().unwrap();
    assert_eq!(
        link.span_context.trace_id().to_string(),
        ENQUEUE_TRACE,
        "the link points at the wrong trace"
    );
    assert_eq!(link.span_context.span_id().to_string(), ENQUEUE_SPAN);
}

/// The other half of T10, and the one that actually causes the damage: the
/// execution span must not be a child of the enqueue span. If it descends, a
/// 30-minute delay becomes a 30-minute trace.
#[tokio::test]
async fn the_execution_span_does_not_descend_from_the_enqueue_span() {
    let spans = exported_spans(delayed_job_with_trace()).await;
    let execution = execution_span(&spans);

    assert_ne!(
        execution.parent_span_id.to_string(),
        ENQUEUE_SPAN,
        "the execution span descends from the enqueue span; a delayed job will \
         produce a trace as long as its delay"
    );
    assert_ne!(
        execution.span_context.trace_id().to_string(),
        ENQUEUE_TRACE,
        "the execution span joined the enqueue trace instead of linking to it"
    );
}

#[tokio::test]
async fn a_job_enqueued_without_trace_context_still_runs() {
    let job = Job::new(RunId::new(), "noop", chrono::Utc::now());
    let spans = exported_spans(job).await;

    let execution = execution_span(&spans);
    assert_eq!(execution.links.iter().count(), 0);
}

/// The attempt number has to survive onto the span, which requires declaring
/// the field up front (T12).
#[tokio::test]
async fn the_execution_span_records_its_outcome() {
    let spans = exported_spans(delayed_job_with_trace()).await;
    let execution = execution_span(&spans);

    let outcome = execution
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "testbed.job.outcome")
        .expect("the outcome field was never recorded (T12)");
    assert_eq!(outcome.value.as_str(), "succeeded");
}
