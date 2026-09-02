//! W3C trace context in and out.
//!
//! # The inbound context is *continued*, never replaced
//!
//! HANDOFF §7 phase 2b gates this specifically, and it is the single most
//! common reason to point a testbed at anything: a browser sends
//! `traceparent`, and if the server starts a fresh trace instead of joining,
//! frontend RUM can never be correlated with backend spans. Minting a new trace
//! id here looks correct in every trace viewer — each trace is individually
//! well-formed — and destroys the only thing you were trying to test.
//!
//! # Trace context crosses every boundary
//!
//! Invariant 10. Inbound HTTP extracts and continues; outbound webhooks inject
//! (Phase 7); queue jobs carry it as a *link*, not a parent (Phase 4, trap T10);
//! WS frames link to their connection span (Phase 5).

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use testbed_core::{SpanId, TraceId};

/// The standard header. Named here so no surface spells it by hand.
pub const TRACEPARENT: &str = "traceparent";

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// Reads inbound `traceparent`/`tracestate` into a context to parent new spans
/// from. Returns an empty context when the headers are absent or malformed,
/// which starts a fresh trace — the correct fallback, and the reason
/// [`TelemetryFault::corrupt_inbound_traceparent`] is worth testing against.
///
/// [`TelemetryFault::corrupt_inbound_traceparent`]: testbed_core::TelemetryFault::corrupt_inbound_traceparent
pub fn extract(headers: &HeaderMap) -> Context {
    TraceContextPropagator::new().extract(&HeaderExtractor(headers))
}

/// Writes the current context onto outbound headers, so the receiver can
/// continue this trace.
pub fn inject(context: &Context, headers: &mut HeaderMap) {
    TraceContextPropagator::new().inject_context(context, &mut HeaderInjector(headers));
}

/// The trace and span ids of `context`, in the form bus events carry.
///
/// Returns `None` for an invalid or absent span, so a caller never stamps an
/// event with an all-zero id that would silently join nothing.
pub fn ids_of(context: &Context) -> Option<(TraceId, SpanId)> {
    let span = context.span();
    let sc = span.span_context();
    if !sc.is_valid() {
        return None;
    }
    Some((
        TraceId::from_bytes(sc.trace_id().to_bytes()),
        SpanId::from_bytes(sc.span_id().to_bytes()),
    ))
}

/// The ids of the span currently active on this task. This is what the surfaces
/// call to stamp an event (invariant 9).
pub fn current_ids() -> Option<(TraceId, SpanId)> {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let context = tracing::Span::current().context();
    ids_of(&context)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The header from the HANDOFF §7 phase 2b gate.
    const GATE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn headers_with(traceparent: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT, HeaderValue::from_str(traceparent).unwrap());
        headers
    }

    #[test]
    fn the_gate_traceparent_is_continued_not_replaced() {
        let ids = ids_of(&extract(&headers_with(GATE)));
        let (trace, span) = ids.expect("valid traceparent yielded no span context");

        assert_eq!(trace.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.to_string(), "00f067aa0ba902b7");
    }

    #[test]
    fn absent_or_malformed_context_yields_no_ids() {
        assert!(ids_of(&extract(&HeaderMap::new())).is_none());
        assert!(ids_of(&extract(&headers_with("not-a-traceparent"))).is_none());
        assert!(ids_of(&extract(&headers_with("00-0-0-01"))).is_none());
    }

    #[test]
    fn inject_round_trips_through_extract() {
        let context = extract(&headers_with(GATE));

        let mut outbound = HeaderMap::new();
        inject(&context, &mut outbound);

        let header = outbound
            .get(TRACEPARENT)
            .expect("nothing was injected")
            .to_str()
            .unwrap();
        assert!(
            header.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
            "outbound traceparent lost the trace id: {header}"
        );

        let (trace, _) = ids_of(&extract(&outbound)).expect("re-extraction failed");
        assert_eq!(trace.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
