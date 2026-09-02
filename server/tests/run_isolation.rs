//! The Phase 3 gate, as a test.
//!
//! **These tests skip themselves unless `DATABASE_URL` is set.** They are not
//! `#[ignore]`d: the point is that they run automatically the moment Postgres
//! is available, so nobody has to remember a flag. With no database they print
//! a skip line and pass, which keeps `cargo test` green on a machine that only
//! wants to build.
//!
//! ```text
//! docker compose up -d --wait
//! DATABASE_URL=postgres://testbed:testbed@localhost:5432/testbed cargo test -p testbed-server
//! ```
//!
//! What is being gated is invariant 6: two runs writing the same table see none
//! of each other's rows. If this passes, parallel test execution is safe; if it
//! fails, every test that runs concurrently with another is reading someone
//! else's data.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use testbed_core::{BroadcastBus, Clock, RunId, Scenario, State, RUN_HEADER};
use testbed_http::data::DataPlane;
use tower::ServiceExt;

/// `None` means no database is configured, and the caller should skip.
fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty())
}

macro_rules! require_database {
    () => {
        match database_url() {
            Some(url) => url,
            None => {
                eprintln!("skipping: DATABASE_URL unset (start Postgres and re-run)");
                return;
            }
        }
    };
}

struct Harness {
    router: axum::Router,
    data: Arc<DataPlane>,
}

impl Harness {
    async fn new(url: &str) -> Self {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
        let state = Arc::new(State::new(
            Scenario {
                name: "test".into(),
                ..Default::default()
            },
            clock,
            bus,
            run,
        ));

        let data = Arc::new(
            DataPlane::connect(url)
                .await
                .expect("DATABASE_URL is set but Postgres is unreachable"),
        );

        Self {
            router: testbed_http::router_with_data(state, Some(Arc::clone(&data))),
            data,
        }
    }

    async fn create_run(&self) -> RunId {
        let run = RunId::new();
        self.data
            .create_run(run)
            .await
            .expect("run creation failed");
        run
    }

    async fn post_item(&self, run: RunId, name: &str) -> StatusCode {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/items")
                    .header(RUN_HEADER, run.header_value())
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"name":"{name}"}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        response.status()
    }

    async fn list_items(&self, run: RunId) -> Vec<Value> {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/items")
                    .header(RUN_HEADER, run.header_value())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}

/// HANDOFF §7 Phase 3, exactly: write into run A, and run B must see nothing.
#[tokio::test]
async fn two_runs_do_not_see_each_others_rows() {
    let url = require_database!();
    let harness = Harness::new(&url).await;

    let run_a = harness.create_run().await;
    let run_b = harness.create_run().await;

    assert_eq!(harness.post_item(run_a, "a").await, StatusCode::CREATED);

    assert_eq!(
        harness.list_items(run_b).await.len(),
        0,
        "run B saw run A's rows; parallel test execution is unsafe"
    );
    assert_eq!(harness.list_items(run_a).await.len(), 1);

    harness.data.drop_run(run_a).await.unwrap();
    harness.data.drop_run(run_b).await.unwrap();
}

/// T5 in practice: isolation has to survive the pool handing out a connection
/// it did not just open. A single request may well get a fresh connection; many
/// concurrent ones definitely exercise reuse.
#[tokio::test]
async fn isolation_holds_across_pooled_connections() {
    let url = require_database!();
    let harness = Arc::new(Harness::new(&url).await);

    let run_a = harness.create_run().await;
    let run_b = harness.create_run().await;

    // More writes than the pool has connections, so connections are reused.
    for i in 0..20 {
        assert_eq!(
            harness.post_item(run_a, &format!("a{i}")).await,
            StatusCode::CREATED
        );
    }
    assert_eq!(harness.post_item(run_b, "b0").await, StatusCode::CREATED);

    assert_eq!(harness.list_items(run_a).await.len(), 20);
    assert_eq!(
        harness.list_items(run_b).await.len(),
        1,
        "a reused connection leaked across runs; search_path is not per-connection"
    );

    harness.data.drop_run(run_a).await.unwrap();
    harness.data.drop_run(run_b).await.unwrap();
}

/// Dropping a run must take its rows with it, and leave other runs alone.
#[tokio::test]
async fn dropping_a_run_wipes_only_that_run() {
    let url = require_database!();
    let harness = Harness::new(&url).await;

    let keep = harness.create_run().await;
    let wipe = harness.create_run().await;
    harness.post_item(keep, "keep").await;
    harness.post_item(wipe, "wipe").await;

    harness.data.drop_run(wipe).await.unwrap();

    assert_eq!(
        harness.list_items(keep).await.len(),
        1,
        "dropping one run took another run's data with it"
    );

    harness.data.drop_run(keep).await.unwrap();
}

/// Rows carry virtual time, so a row written after `clock/advance` is stamped
/// with the advanced time rather than wall time (T14's reasoning, applied to
/// the data plane).
#[tokio::test]
async fn rows_are_stamped_from_the_virtual_clock() {
    use chrono::{DateTime, Utc};

    let url = require_database!();
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
    let state = Arc::new(State::new(
        Scenario::default(),
        Arc::clone(&clock),
        bus,
        run,
    ));

    let data = Arc::new(DataPlane::connect(&url).await.unwrap());
    data.create_run(run).await.unwrap();
    let router = testbed_http::router_with_data(state, Some(Arc::clone(&data)));

    clock.advance(std::time::Duration::from_secs(3600));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/items")
                .header(RUN_HEADER, run.header_value())
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"future"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let item: Value = serde_json::from_slice(&bytes).unwrap();
    let created: DateTime<Utc> = item["created_at"].as_str().unwrap().parse().unwrap();

    assert!(
        created > Utc::now() + chrono::TimeDelta::minutes(50),
        "row was stamped from wall time, not the virtual clock: {created}"
    );

    data.drop_run(run).await.unwrap();
}
