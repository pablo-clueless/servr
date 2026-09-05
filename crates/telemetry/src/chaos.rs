//! The exporter shim: deliberate corruption of what leaves the process.
//!
//! # Invariant 11 — at export, never at instrumentation
//!
//! Every fault here is applied to a batch on its way out, or to the metrics
//! exposition text on its way out. Nothing corrupts a span where it is created.
//!
//! That is not fastidiousness. The testbed is the thing you debug the testbed
//! with: corrupt spans at creation and the first confusing behaviour you chase
//! is chased with a broken trace of itself, and you cannot tell the injected
//! fault from a real bug. Confining the damage to what has already left means
//! `/_admin/events`, the logs and the in-process spans all stay honest while the
//! collector sees garbage — which is exactly the asymmetry that makes this
//! useful for testing observability tooling.
//!
//! # Trap T13 — the export path emits no bus events
//!
//! Nothing in this module calls the bus. An event emitted here would be picked
//! up by instrumentation, which queues a span, which is exported, which emits an
//! event. The batch exporter delays the recursion just long enough to make it
//! puzzling rather than obvious. The export path is exempt from invariant 4.
//!
//! # The metrics faults corrupt the rendered text, not the recorder
//!
//! `cardinality_bomb` and `counter_reset` rewrite the Prometheus exposition
//! output at scrape time rather than registering junk series in the recorder.
//! Same reason: a bomb that went through `metrics::counter!` would poison the
//! testbed's own `/metrics` permanently — there is no un-registering a series —
//! and would survive `POST /_admin/reset`, which is meant to restore a known
//! good state. Rewriting the text means the fault is live exactly as long as it
//! is configured, and the process's own counters are never touched.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::trace::SpanId;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use testbed_core::TelemetryFault;

/// Where the shim reads its current configuration.
///
/// A trait rather than `Arc<State>` directly so the shim is testable without
/// standing up a control plane — and so `telemetry` keeps depending on `core`
/// for types only, not for a particular wiring.
pub trait Faults: Send + Sync + std::fmt::Debug + 'static {
    fn current(&self) -> TelemetryFault;
}

/// The production source: the resolved scenario, as `/_admin/telemetry/faults`
/// leaves it.
#[derive(Debug)]
pub struct FromState(pub Arc<testbed_core::State>);

impl Faults for FromState {
    fn current(&self) -> TelemetryFault {
        self.0.resolved().telemetry.clone()
    }
}

/// Wraps a real exporter and corrupts what passes through it.
#[derive(Debug)]
pub struct ChaosExporter<E> {
    inner: E,
    faults: Arc<dyn Faults>,
}

impl<E> ChaosExporter<E> {
    pub fn new(inner: E, faults: Arc<dyn Faults>) -> Self {
        Self { inner, faults }
    }
}

impl<E: SpanExporter + std::fmt::Debug> SpanExporter for ChaosExporter<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let fault = self.faults.current();

        // `rate` is per batch, matching the field's documented meaning. Rolling
        // per span would make a 0.5 rate mean something different at every batch
        // size, which is not a knob anyone can reason about.
        if !fires(&fault) {
            return self.inner.export(batch).await;
        }

        if let Some(ms) = fault.export_latency_ms {
            stall(Duration::from_millis(ms)).await;
        }

        if fault.drop_export {
            // Reported as success, deliberately: a dropped batch that returned
            // an error would be retried or logged, and the point is telemetry
            // that vanishes without anything noticing.
            return Ok(());
        }

        self.inner.export(corrupt_batch(batch, &fault)).await
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// Whether corruption applies to this export.
fn fires(fault: &TelemetryFault) -> bool {
    fault.rate > 0.0 && rand::random::<f64>() < fault.rate
}

/// Delays an export, from wherever the SDK happens to be driving it.
///
/// # Why this is not just `tokio::time::sleep`
///
/// The batch span processor runs exports on its **own thread**, outside the
/// application's tokio runtime. `tokio::time::sleep` there panics with "there is
/// no reactor running", the processor thread dies, and every span from that
/// moment on is silently dropped — the testbed looks like it stopped emitting
/// telemetry entirely, which reads as a broken collector rather than a bug in a
/// fault nobody enabled on purpose. Found exactly that way on 2026-09-05.
///
/// Blocking the processor thread is the honest simulation anyway: a stalled
/// exporter *does* hold up the batch pipeline, and that thread exists so the
/// stall cannot reach request handling. The tokio path is preferred when a
/// runtime is present so tests and any future async driver stay cooperative.
///
/// Real time, not virtual: a collector's backpressure handling has to actually
/// wait. Nothing is scheduled here, so invariant 1 is untouched.
async fn stall(delay: Duration) {
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::time::sleep(delay).await,
        Err(_) => std::thread::sleep(delay),
    }
}

/// Applies every span-shaped fault to a batch.
///
/// Split out from the trait so it can be tested directly — asserting on what a
/// real OTLP exporter received would need a collector.
pub fn corrupt_batch(batch: Vec<SpanData>, fault: &TelemetryFault) -> Vec<SpanData> {
    batch
        .into_iter()
        .map(|span| corrupt_span(span, fault))
        .collect()
}

fn corrupt_span(mut span: SpanData, fault: &TelemetryFault) -> SpanData {
    if fault.orphan_spans {
        // A parent that appears nowhere in the trace. Every trace UI handles
        // this differently and most handle it badly, which is the test.
        span.parent_span_id = orphan_parent();
    }

    if let Some(ms) = fault.clock_skew_ms {
        span.start_time = shift(span.start_time, ms);
        span.end_time = shift(span.end_time, ms);
    }

    if let Some(bytes) = fault.attribute_bloat_bytes {
        span.attributes.push(opentelemetry::KeyValue::new(
            "testbed.bloat",
            "x".repeat(bytes),
        ));
    }

    span
}

/// A random, non-zero span id. Zero is reserved as "no parent", which would
/// read as a root span rather than as an orphan.
fn orphan_parent() -> SpanId {
    loop {
        let id = SpanId::from_bytes(rand::random::<[u8; 8]>());
        if id != SpanId::INVALID {
            return id;
        }
    }
}

/// Shifts a timestamp, saturating rather than panicking.
///
/// A skew large enough to underflow the epoch is a legitimate thing to ask for
/// — "what does your backend do with a span from 1969" is a fair question — and
/// it must not take the process down to answer it.
fn shift(at: std::time::SystemTime, ms: i64) -> std::time::SystemTime {
    let delta = Duration::from_millis(ms.unsigned_abs());
    let shifted = if ms >= 0 {
        at.checked_add(delta)
    } else {
        at.checked_sub(delta)
    };
    shifted.unwrap_or(std::time::UNIX_EPOCH)
}

/// Rewrites rendered Prometheus exposition text according to `fault`.
///
/// Applied to the output of `PrometheusHandle::render`, so the recorder itself
/// is never touched — see the module docs for why that matters.
pub fn corrupt_metrics(rendered: String, fault: &TelemetryFault) -> String {
    if !fires(fault) {
        return rendered;
    }

    let mut out = if fault.counter_reset {
        reset_counters(&rendered)
    } else {
        rendered
    };

    if let Some(series) = fault.cardinality_bomb {
        out.push_str(&bomb(series));
    }

    out
}

/// Sets every sample to 1, so a backend computing `rate()` over a monotonic
/// counter infers a process restart and discards its accumulated increase.
fn reset_counters(rendered: &str) -> String {
    let mut out = String::with_capacity(rendered.len());
    for line in rendered.lines() {
        // `# HELP`/`# TYPE` lines carry no sample, and a blank line separates
        // metric families. Rewriting either produces exposition text Prometheus
        // rejects outright, which tests the parser rather than the backend.
        if line.starts_with('#') || line.is_empty() {
            out.push_str(line);
        } else {
            match line.rsplit_once(' ') {
                Some((series, _value)) => {
                    out.push_str(series);
                    out.push_str(" 1");
                }
                None => out.push_str(line),
            }
        }
        out.push('\n');
    }
    out
}

/// One synthetic metric with `series` unique label values.
///
/// This is the field that genuinely degrades whatever is scraping — the blast
/// radius `compose.yaml` keeps behind the `obs` profile. Each scrape produces a
/// *fresh* set of label values, so the damage compounds per scrape rather than
/// converging on a fixed set, which is what makes it a real cardinality bomb
/// rather than a large-but-bounded metric.
fn bomb(series: u32) -> String {
    let mut out = String::with_capacity(series as usize * 64);
    out.push_str(
        "# HELP testbed_cardinality_bomb Deliberate cardinality explosion (TelemetryFault)\n\
         # TYPE testbed_cardinality_bomb counter\n",
    );

    let nonce: u64 = rand::random();
    for i in 0..series {
        out.push_str(&format!(
            "testbed_cardinality_bomb{{scrape=\"{nonce:016x}\",series=\"{i}\"}} 1\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    fn fault() -> TelemetryFault {
        TelemetryFault {
            rate: 1.0,
            ..Default::default()
        }
    }

    fn span() -> SpanData {
        use opentelemetry::trace::{
            SpanContext, SpanKind, Status, TraceFlags, TraceId, TraceState,
        };
        use opentelemetry::InstrumentationScope;

        SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes([1; 16]),
                SpanId::from_bytes([2; 8]),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from_bytes([3; 8]),
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: "test".into(),
            start_time: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            end_time: UNIX_EPOCH + Duration::from_secs(1_700_000_001),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Default::default(),
            links: Default::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::builder("test").build(),
        }
    }

    #[test]
    fn an_unconfigured_fault_changes_nothing() {
        let before = span();
        let after = corrupt_span(before.clone(), &TelemetryFault::default());
        assert_eq!(before, after);
    }

    #[test]
    fn orphan_spans_reparents_to_an_id_that_was_not_there() {
        let original = span();
        let orphaned = corrupt_span(
            original.clone(),
            &TelemetryFault {
                orphan_spans: true,
                ..fault()
            },
        );

        assert_ne!(orphaned.parent_span_id, original.parent_span_id);
        assert_ne!(
            orphaned.parent_span_id,
            SpanId::INVALID,
            "an all-zero parent reads as a root span, not as an orphan"
        );
        assert_ne!(
            orphaned.parent_span_id,
            original.span_context.span_id(),
            "the span was reparented to itself"
        );
    }

    /// The gate asks for span start times an hour in the future.
    #[test]
    fn clock_skew_moves_both_timestamps_by_the_configured_amount() {
        let original = span();
        let skewed = corrupt_span(
            original.clone(),
            &TelemetryFault {
                clock_skew_ms: Some(3_600_000),
                ..fault()
            },
        );

        let hour = Duration::from_secs(3_600);
        assert_eq!(skewed.start_time, original.start_time + hour);
        assert_eq!(skewed.end_time, original.end_time + hour);
        assert_eq!(
            skewed.end_time.duration_since(skewed.start_time).unwrap(),
            original
                .end_time
                .duration_since(original.start_time)
                .unwrap(),
            "the skew changed the span's duration; it must only move it"
        );
    }

    /// The gate's actual claim — "exported span start times land an hour in the
    /// future" — is about a span recorded *now*, not about the fixture's fixed
    /// 2023 timestamp. Asserted separately so the two properties cannot be
    /// confused: the test above proves the shift, this one proves the effect.
    #[test]
    fn an_hour_of_skew_puts_a_fresh_span_in_the_future() {
        let mut fresh = span();
        fresh.start_time = crate::wall::system_now();
        fresh.end_time = crate::wall::system_now();

        let skewed = corrupt_span(
            fresh,
            &TelemetryFault {
                clock_skew_ms: Some(3_600_000),
                ..fault()
            },
        );

        assert!(
            skewed.start_time > crate::wall::system_now() + Duration::from_secs(3_500),
            "a fresh span skewed by an hour did not land in the future"
        );
    }

    #[test]
    fn a_negative_skew_moves_timestamps_backwards() {
        let original = span();
        let skewed = corrupt_span(
            original.clone(),
            &TelemetryFault {
                clock_skew_ms: Some(-1_000),
                ..fault()
            },
        );
        assert_eq!(
            skewed.start_time,
            original.start_time - Duration::from_secs(1)
        );
    }

    /// "What does your backend do with a span from before the epoch" is a fair
    /// question, and answering it must not panic the testbed.
    #[test]
    fn an_absurd_negative_skew_saturates_instead_of_panicking() {
        let skewed = corrupt_span(
            span(),
            &TelemetryFault {
                clock_skew_ms: Some(i64::MIN + 1),
                ..fault()
            },
        );
        assert_eq!(skewed.start_time, UNIX_EPOCH);
    }

    #[test]
    fn attribute_bloat_pads_the_span() {
        let bloated = corrupt_span(
            span(),
            &TelemetryFault {
                attribute_bloat_bytes: Some(4096),
                ..fault()
            },
        );

        let padding = bloated
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "testbed.bloat")
            .expect("no padding attribute");
        assert_eq!(padding.value.as_str().len(), 4096);
    }

    #[test]
    fn faults_compose_on_one_span() {
        let corrupted = corrupt_span(
            span(),
            &TelemetryFault {
                orphan_spans: true,
                clock_skew_ms: Some(1_000),
                attribute_bloat_bytes: Some(16),
                ..fault()
            },
        );

        assert_ne!(corrupted.parent_span_id, SpanId::from_bytes([3; 8]));
        assert_eq!(corrupted.attributes.len(), 1);
        assert!(corrupted.start_time > UNIX_EPOCH + Duration::from_secs(1_700_000_000));
    }

    #[test]
    fn a_zero_rate_never_fires() {
        assert!(!fires(&TelemetryFault {
            rate: 0.0,
            orphan_spans: true,
            ..Default::default()
        }));
    }

    #[test]
    fn corrupting_a_batch_touches_every_span() {
        let batch = vec![span(), span(), span()];
        let corrupted = corrupt_batch(
            batch,
            &TelemetryFault {
                orphan_spans: true,
                ..fault()
            },
        );

        assert_eq!(corrupted.len(), 3);
        assert!(corrupted
            .iter()
            .all(|s| s.parent_span_id != SpanId::from_bytes([3; 8])));
    }

    /// A stall must work on a thread with no tokio runtime.
    ///
    /// This is the regression test for the bug that made every span vanish: the
    /// SDK drives exports on its own `BatchProcessor` thread, `tokio::time::sleep`
    /// panicked there, and the dead thread took all span export with it. A
    /// `#[tokio::test]` would *not* catch it — the panic only happens off-runtime,
    /// so this deliberately blocks on the future from a plain thread.
    #[test]
    fn a_stall_outside_a_tokio_runtime_does_not_panic() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        std::thread::spawn(|| {
            let mut future = Box::pin(stall(Duration::from_millis(1)));
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            // The std path completes on the first poll; the tokio path is not
            // reachable here because this thread has no runtime.
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(())));
        })
        .join()
        .expect("stalling off-runtime panicked — the batch processor thread dies here");
    }

    const SCRAPE: &str = "# HELP testbed_http_requests_total Requests\n\
                          # TYPE testbed_http_requests_total counter\n\
                          testbed_http_requests_total{method=\"GET\"} 42\n\
                          testbed_queue_depth 7\n";

    #[test]
    fn metrics_are_untouched_without_a_fault() {
        assert_eq!(
            corrupt_metrics(SCRAPE.to_string(), &TelemetryFault::default()),
            SCRAPE
        );
    }

    #[test]
    fn counter_reset_rewrites_samples_but_keeps_the_metadata() {
        let reset = corrupt_metrics(
            SCRAPE.to_string(),
            &TelemetryFault {
                counter_reset: true,
                ..fault()
            },
        );

        assert!(reset.contains("# TYPE testbed_http_requests_total counter"));
        assert!(
            reset.contains("testbed_http_requests_total{method=\"GET\"} 1"),
            "the sample was not reset: {reset}"
        );
        assert!(!reset.contains(" 42"), "the original value survived");
    }

    #[test]
    fn the_cardinality_bomb_emits_the_requested_number_of_series() {
        let bombed = corrupt_metrics(
            SCRAPE.to_string(),
            &TelemetryFault {
                cardinality_bomb: Some(500),
                ..fault()
            },
        );

        let series = bombed
            .lines()
            .filter(|l| l.starts_with("testbed_cardinality_bomb{"))
            .count();
        assert_eq!(series, 500);
        assert!(
            bombed.contains("testbed_http_requests_total{method=\"GET\"} 42"),
            "the bomb replaced the real scrape instead of adding to it"
        );
    }

    /// The damage has to compound per scrape. A bomb that emitted the same
    /// label set every time is a large metric, not a cardinality problem.
    #[test]
    fn each_scrape_produces_fresh_label_values() {
        let fault = TelemetryFault {
            cardinality_bomb: Some(4),
            ..fault()
        };
        let first = corrupt_metrics(String::new(), &fault);
        let second = corrupt_metrics(String::new(), &fault);
        assert_ne!(first, second, "two scrapes produced identical label values");
    }
}
