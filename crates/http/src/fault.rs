//! The fault injection layer.
//!
//! # Why this is a layer and not handler code
//!
//! HANDOFF §5 invariant 8. A handler that consults fault config is a bug,
//! because the fault then only exists on the routes someone remembered to wire
//! — which is reliably the routes nobody tests. Here it applies to everything
//! the layer wraps, including routes added later by someone who has never read
//! this file.
//!
//! # Why it is not wrapped around `/_admin`
//!
//! A scenario with `route = "/*"` would otherwise match `/_admin/faults` and
//! `/_admin/reset`, and the testbed would have no way back: the only endpoint
//! that can clear the fault is behind the fault. The control plane stays
//! reachable no matter what the data plane is configured to do.
//!
//! # Latency is real, not virtual
//!
//! The one place virtual time deliberately does not apply. A client measuring
//! a timeout has to actually wait, so this sleeps against real time. Nothing
//! here *schedules* anything, which is what invariant 1 is about.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use testbed_core::{Clock, EventKind, FaultSpec};
use tracing::Instrument;

/// What the matching rules add up to for one request.
#[derive(Debug, Default)]
struct Effects {
    delay: Option<Duration>,
    status: Option<StatusCode>,
    truncate_at: Option<usize>,
    drop_connection: bool,
    /// Names for the event and the span, in the order the rules fired.
    names: Vec<String>,
}

impl Effects {
    /// Resolves the rules matching `path` that actually fire this time.
    ///
    /// `roll` draws in `0.0..1.0`; it is a closure so tests can make the
    /// outcome deterministic without a seeded RNG plumbed through everything.
    fn resolve(specs: &[&FaultSpec], mut roll: impl FnMut() -> f64) -> Self {
        let mut effects = Self::default();

        for spec in specs {
            // Rate is per-rule: two rules at 0.5 are two independent coins.
            if roll() >= spec.rate {
                continue;
            }

            if let Some(ms) = spec.delay_ms(roll()) {
                effects.delay = Some(effects.delay.unwrap_or_default() + Duration::from_millis(ms));
            }
            // Later rules win on scalar fields.
            if let Some(status) = spec.status.and_then(|s| StatusCode::from_u16(s).ok()) {
                effects.status = Some(status);
            }
            if let Some(at) = spec.truncate_body_at {
                effects.truncate_at = Some(at);
            }
            effects.drop_connection |= spec.drop_connection;

            effects
                .names
                .extend(spec.effects().into_iter().map(str::to_string));
        }

        effects
    }

    fn fired(&self) -> bool {
        !self.names.is_empty()
    }
}

/// Wraps `router` in the fault layer.
///
/// Exported because the ws and stream surfaces live in their own crates and
/// `http` must not depend on them (HANDOFF §4). Without this, `server` would
/// have to reach into the layer function and re-derive the wiring, and the two
/// copies would drift — which is how a surface quietly ends up unfaultable, the
/// exact failure invariant 8 exists to prevent.
///
/// # Truncation and unbounded bodies
///
/// A `truncate_body_at` fault buffers the whole body before cutting it, so a
/// route wrapped here must not stream without end. Every current one is finite;
/// `/_admin/events` is unbounded and is deliberately outside the layer anyway.
pub fn guard(state: Arc<testbed_core::State>, router: axum::Router) -> axum::Router {
    router.layer(axum::middleware::from_fn_with_state(state, layer))
}

/// Applies faults, then records the request on the event bus.
///
/// The two belong together: this is the only place that knows which faults
/// fired, so it is the only place that can put them on the event.
pub async fn layer(
    State(state): State<Arc<testbed_core::State>>,
    request: Request,
    next: Next,
) -> Response {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let path = request.uri().path().to_string();
    let method = request.method().to_string();

    // Trap T12: fields discovered later must be declared `Empty` up front, or
    // they can never be recorded.
    let span = tracing::info_span!(
        "http.request",
        otel.name = %format!("{method} {path}"),
        http.request.method = %method,
        url.path = %path,
        { testbed_telemetry::late::HTTP_STATUS } = tracing::field::Empty,
        { testbed_telemetry::late::HTTP_FAULTS } = tracing::field::Empty,
        { testbed_telemetry::wall::attr::VIRTUAL_TIME } = tracing::field::Empty,
        { testbed_telemetry::wall::attr::VIRTUAL_OFFSET_MS } = state.clock().offset_ms(),
    );

    // The inbound context is *continued*, not replaced. A fresh trace id here
    // would make frontend RUM impossible to join to backend spans, which is the
    // single most common reason to point a testbed at anything.
    //
    // A failure means there was no usable inbound context, so this span starts
    // a new trace — the correct fallback, and the observable effect of the
    // `corrupt_inbound_traceparent` telemetry fault.
    if span
        .set_parent(testbed_telemetry::propagation::extract(request.headers()))
        .is_err()
    {
        tracing::trace!("no inbound trace context; starting a new trace");
    }

    let effects = {
        let resolved = state.resolved();
        Effects::resolve(&resolved.faults_for(&path), rand::random::<f64>)
    };

    let started = testbed_telemetry::wall::instant();

    // `.instrument()`, never `span.enter()`: a span guard held across an
    // `.await` stays entered while the task is parked, so it would attribute
    // every concurrently-polled request to whichever one happened to yield last.
    let response = {
        let effects = &effects;
        let request_span = span.clone();
        async move {
            if let Some(delay) = effects.delay {
                tokio::time::sleep(delay).await;
            }

            if effects.drop_connection {
                // Hyper has no "abort this connection" hook reachable from
                // here, so the body errors mid-stream instead. A client sees a
                // truncated transfer, which is the failure mode being simulated.
                Response::builder()
                    .status(effects.status.unwrap_or(StatusCode::OK))
                    .body(Body::from_stream(futures_util::stream::once(async {
                        Err::<axum::body::Bytes, std::io::Error>(std::io::Error::other(
                            "connection dropped by fault injection",
                        ))
                    })))
                    .expect("static response builds")
            } else if let Some(status) = effects.status {
                // A status override short-circuits: the handler never runs,
                // which is what a gateway returning 503 in front of it would do.
                (status, axum::Json(serde_json::json!({ "fault": "status" }))).into_response()
            } else {
                let response = next.run(request).await;
                match effects.truncate_at {
                    Some(at) => truncate(response, at).await,
                    None => response,
                }
            }
        }
        .instrument(request_span)
        .await
    };

    let latency = testbed_telemetry::wall::instant() - started;
    let status = response.status().as_u16();
    let virtual_now = state.clock().now();

    // Trap T12: recording the fields declared `Empty` when the span opened.
    span.record(testbed_telemetry::late::HTTP_STATUS, status);
    span.record(
        testbed_telemetry::late::HTTP_FAULTS,
        effects.names.join(","),
    );
    span.record(
        testbed_telemetry::wall::attr::VIRTUAL_TIME,
        virtual_now.to_rfc3339().as_str(),
    );

    testbed_telemetry::metrics::record_request(&method, status, latency, &effects.names);

    if effects.fired() {
        tracing::debug!(
            status,
            latency_ms = latency.as_millis() as u64,
            faults = %effects.names.join(","),
            "fault applied"
        );
    }

    // Invariant 9: the event carries the trace context of the span it happened
    // under, which is the only thing that lets the event stream and the trace
    // tree be read together.
    let (trace_id, span_id) = match testbed_telemetry::propagation::ids_of(&span.context()) {
        Some((trace, span)) => (Some(trace), Some(span)),
        None => (None, None),
    };

    state.bus().emit(testbed_core::Event {
        id: 0,
        run: state.run(),
        at: virtual_now,
        wall_at: Clock::wall_now(),
        trace_id,
        span_id,
        kind: EventKind::HttpRequest {
            method,
            path,
            status,
            latency_ms: latency.as_millis() as u64,
            faults: effects.names,
        },
    });

    response
}

/// Cuts the body short **without** correcting `content-length`, so the client
/// sees a short read rather than a valid small response. Correcting the header
/// would make this indistinguishable from a handler that returned less.
async fn truncate(response: Response, at: usize) -> Response {
    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let cut = bytes.slice(..at.min(bytes.len()));
    Response::from_parts(parts, Body::from(cut))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(route: &str, rate: f64) -> FaultSpec {
        FaultSpec {
            route: route.into(),
            rate,
            ..Default::default()
        }
    }

    /// A roll sequence that always fires (0.0 < any positive rate).
    fn always() -> impl FnMut() -> f64 {
        || 0.0
    }

    /// A roll sequence that never fires (1.0 >= any rate <= 1.0).
    fn never() -> impl FnMut() -> f64 {
        || 1.0
    }

    #[test]
    fn a_rate_of_zero_never_fires() {
        let spec = FaultSpec {
            status: Some(503),
            ..spec("/api/*", 0.0)
        };
        let effects = Effects::resolve(&[&spec], always());
        assert!(!effects.fired());
        assert_eq!(effects.status, None);
    }

    #[test]
    fn the_phase_2_gate_rule_produces_a_503_after_500ms() {
        let spec = FaultSpec {
            latency_ms: Some(500),
            status: Some(503),
            ..spec("/api/*", 1.0)
        };
        let effects = Effects::resolve(&[&spec], always());

        assert_eq!(effects.delay, Some(Duration::from_millis(500)));
        assert_eq!(effects.status, Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(effects.names, vec!["latency", "status"]);
    }

    #[test]
    fn a_losing_roll_skips_the_rule_entirely() {
        let spec = FaultSpec {
            latency_ms: Some(500),
            status: Some(503),
            ..spec("/api/*", 0.99)
        };
        assert!(!Effects::resolve(&[&spec], never()).fired());
    }

    #[test]
    fn later_rules_win_on_status_but_latency_accumulates() {
        let first = FaultSpec {
            latency_ms: Some(100),
            status: Some(500),
            ..spec("/api/*", 1.0)
        };
        let second = FaultSpec {
            latency_ms: Some(200),
            status: Some(503),
            ..spec("/api/ping", 1.0)
        };

        let effects = Effects::resolve(&[&first, &second], always());
        assert_eq!(effects.status, Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(effects.delay, Some(Duration::from_millis(300)));
    }

    #[test]
    fn drop_connection_survives_one_rule_setting_it() {
        let quiet = spec("/api/*", 1.0);
        let dropper = FaultSpec {
            drop_connection: true,
            ..spec("/api/*", 1.0)
        };
        assert!(Effects::resolve(&[&quiet, &dropper], always()).drop_connection);
    }

    #[test]
    fn no_matching_rules_means_no_effects() {
        let effects = Effects::resolve(&[], always());
        assert!(!effects.fired());
        assert!(effects.delay.is_none());
        assert!(!effects.drop_connection);
    }
}
