//! The Phase 2 gate, as a test.
//!
//! The gate in HANDOFF §7 is a curl script against a running server. This is
//! the same assertions against the router in-process, so a regression fails CI
//! instead of waiting for someone to run the gate by hand. It does not replace
//! the gate — only the real thing measures wall-clock latency over a socket.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use testbed_core::{
    BroadcastBus, Clock, EventKind, FaultSpec, Overlay, RunId, Scenario, State, TelemetryFault,
};
use tower::ServiceExt;

fn state_with(faults: Vec<FaultSpec>) -> Arc<State> {
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    let scenario = Scenario {
        name: "test".into(),
        faults,
        telemetry: TelemetryFault::default(),
        ..Default::default()
    };
    Arc::new(State::new(scenario, clock, bus, run))
}

async fn get(state: &Arc<State>, path: &str) -> axum::response::Response {
    testbed_http::router(Arc::clone(state))
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn an_unfaulted_request_passes_through() {
    let state = state_with(vec![]);
    assert_eq!(get(&state, "/api/ping").await.status(), StatusCode::OK);
}

/// The gate's rule: `{"route":"/api/*","rate":1.0,"latency_ms":500,"status":503}`.
#[tokio::test]
async fn the_gate_rule_returns_503_after_the_configured_latency() {
    let state = state_with(vec![FaultSpec {
        route: "/api/*".into(),
        rate: 1.0,
        latency_ms: Some(500),
        status: Some(503),
        ..Default::default()
    }]);

    // Through the sanctioned accessor: invariant 1's gate greps test code too.
    let started = testbed_telemetry::wall::instant();
    let response = get(&state, "/api/ping").await;
    let elapsed = testbed_telemetry::wall::instant() - started;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        elapsed >= Duration::from_millis(500),
        "latency fault did not delay the response: {elapsed:?}"
    );
}

/// `reset` must make the route healthy again, or the testbed cannot be
/// returned to a known-good state between tests (invariant 2).
#[tokio::test]
async fn reset_clears_a_fault_added_at_runtime() {
    let state = state_with(vec![]);

    state.mutate(|overlay: &mut Overlay| {
        overlay.faults = Some(vec![FaultSpec {
            route: "/api/*".into(),
            rate: 1.0,
            status: Some(503),
            ..Default::default()
        }]);
    });
    assert_eq!(
        get(&state, "/api/ping").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    state.reset();
    assert_eq!(get(&state, "/api/ping").await.status(), StatusCode::OK);
}

/// HANDOFF §9 task 7: the request appears on the event stream with the fault
/// named in `faults`.
#[tokio::test]
async fn a_faulted_request_is_reported_on_the_event_stream() {
    let state = state_with(vec![FaultSpec {
        route: "/api/*".into(),
        rate: 1.0,
        latency_ms: Some(10),
        status: Some(503),
        ..Default::default()
    }]);

    let mut events = state.bus().subscribe();
    get(&state, "/api/ping").await;

    let event = tokio::time::timeout(Duration::from_millis(50), events.next())
        .await
        .expect("event did not arrive within 50ms")
        .expect("event stream ended");

    match event.kind {
        EventKind::HttpRequest {
            method,
            path,
            status,
            latency_ms,
            faults,
        } => {
            assert_eq!(method, "GET");
            assert_eq!(path, "/api/ping");
            assert_eq!(status, 503);
            assert!(latency_ms >= 10, "latency was not recorded: {latency_ms}");
            assert_eq!(faults, vec!["latency", "status"]);
        }
        other => panic!("expected an HttpRequest event, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unfaulted_request_is_still_reported_with_no_faults() {
    let state = state_with(vec![]);
    let mut events = state.bus().subscribe();

    get(&state, "/api/ping").await;

    let event = tokio::time::timeout(Duration::from_millis(50), events.next())
        .await
        .expect("event did not arrive within 50ms")
        .expect("event stream ended");

    match event.kind {
        EventKind::HttpRequest { status, faults, .. } => {
            assert_eq!(status, 200);
            assert!(faults.is_empty());
        }
        other => panic!("expected an HttpRequest event, got {other:?}"),
    }
}

#[tokio::test]
async fn a_truncating_fault_shortens_the_body() {
    let state = state_with(vec![FaultSpec {
        route: "/api/*".into(),
        rate: 1.0,
        truncate_body_at: Some(4),
        ..Default::default()
    }]);

    let response = get(&state, "/api/ping").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.len(), 4, "body was not truncated: {body:?}");
}
