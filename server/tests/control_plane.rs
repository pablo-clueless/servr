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
