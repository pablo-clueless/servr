//! Cross-surface assertions.
//!
//! These live in `server` because it is the only crate permitted to depend on
//! more than one surface (HANDOFF §4). A test like this inside `crates/http`
//! would create an `http -> admin` edge, which is exactly the coupling the
//! layout rule exists to prevent.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use testbed_core::{BroadcastBus, Clock, FaultSpec, RunId, Scenario, State};
use tower::ServiceExt;

fn state_with(faults: Vec<FaultSpec>) -> Arc<State> {
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    Arc::new(State::new(
        Scenario {
            name: "test".into(),
            faults,
            ..Default::default()
        },
        clock,
        bus,
        run,
    ))
}

async fn get(router: axum::Router, path: &str) -> axum::response::Response {
    router
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// A scenario matching everything must not be able to lock the operator out of
/// the control plane: `/_admin` sits outside the fault layer on purpose. If
/// this ever fails, the only way to clear a `route = "*"` fault is a restart.
#[tokio::test]
async fn a_catch_all_fault_cannot_lock_out_the_control_plane() {
    let state = state_with(vec![FaultSpec {
        route: "*".into(),
        rate: 1.0,
        status: Some(503),
        ..Default::default()
    }]);

    let health = get(testbed_admin::router(Arc::clone(&state)), "/_admin/health").await;
    assert_eq!(
        health.status(),
        StatusCode::OK,
        "the fault layer reached the control plane; recovery would need a restart"
    );

    // ...while the data plane is genuinely faulted.
    let ping = get(testbed_http::router(Arc::clone(&state)), "/api/ping").await;
    assert_eq!(ping.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// The two routers must not collide when merged, or a route added to one
/// silently shadows the other at boot.
#[tokio::test]
async fn the_assembled_router_serves_both_planes() {
    let state = state_with(vec![]);
    let app = axum::Router::new()
        .merge(testbed_admin::router(Arc::clone(&state)))
        .merge(testbed_http::router(Arc::clone(&state)));

    assert_eq!(
        get(app.clone(), "/_admin/health").await.status(),
        StatusCode::OK
    );
    assert_eq!(get(app, "/api/ping").await.status(), StatusCode::OK);
}

/// Phase 8: the exporter shim reads its faults from the resolved scenario, so
/// what `/_admin/telemetry/faults` writes has to be what `FromState` hands the
/// shim. This is the wiring the whole phase rests on and nothing else asserts it
/// — the chaos unit tests take a `TelemetryFault` directly.
#[tokio::test]
async fn telemetry_faults_written_by_admin_reach_the_exporter_shim() {
    use testbed_telemetry::chaos::Faults;

    let state = state_with(vec![]);
    let source = testbed_telemetry::chaos::FromState(Arc::clone(&state));
    assert_eq!(
        source.current(),
        testbed_core::TelemetryFault::default(),
        "a scenario with no [telemetry] table must export honest telemetry"
    );

    let app = testbed_admin::router(Arc::clone(&state));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_admin/telemetry/faults")
                .body(Body::from(
                    r#"{"rate":1.0,"orphan_spans":true,"clock_skew_ms":3600000}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let live = source.current();
    assert_eq!(live.rate, 1.0);
    assert!(live.orphan_spans);
    assert_eq!(live.clock_skew_ms, Some(3_600_000));
}

/// Invariant 2: `reset` reconstructs a known-good state from the scenario
/// alone. Telemetry corruption that survived a reset would leave the next test
/// running against a source that lies, with nothing in its own setup to explain
/// why.
#[tokio::test]
async fn reset_restores_the_scenarios_telemetry_faults() {
    use testbed_telemetry::chaos::Faults;

    let state = state_with(vec![]);
    let source = testbed_telemetry::chaos::FromState(Arc::clone(&state));

    state.mutate(|overlay| {
        overlay.telemetry = Some(testbed_core::TelemetryFault {
            rate: 1.0,
            drop_export: true,
            ..Default::default()
        })
    });
    assert!(source.current().drop_export);

    state.reset();

    assert_eq!(
        source.current(),
        testbed_core::TelemetryFault::default(),
        "telemetry corruption survived a reset"
    );
}
