//! Phase 7, the webhook gate, run in-process.
//!
//! ```text
//! $ curl -s -X POST localhost:8080/hooks/in/abc -d '{"x":1}' >/dev/null
//! $ curl -s localhost:8080/_admin/hooks/in/abc | jq '.[0].body.x'
//! 1
//! $ curl -s -X POST localhost:8080/_admin/hooks/out \
//!     -d '{"url":"...","sign":"stripe","fail_first":2}' >/dev/null
//! $ curl -s -X POST localhost:8080/_admin/clock/advance -d '{"ms":60000}' >/dev/null
//! # WebhookOut attempts: 1, 2, 3
//! ```
//!
//! # Why a real socket
//!
//! The gate points the sender at the testbed's *own* inbox, so the delivery has
//! to leave the process and come back. `oneshot` cannot express that — the
//! sender holds a `reqwest` client, not a `tower` service — and it is the part
//! worth testing: signing, `traceparent` injection and capture all happen
//! across that boundary.

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use testbed_core::{
    BroadcastBus, Clock, EventKind, EventSink, RunId, Scenario, SigningScheme, State,
};
use tokio::net::TcpListener;

/// Bounds every wait. A retry that never fires must fail by name, not by hanging.
const TIMEOUT: Duration = Duration::from_secs(5);

struct Server {
    base: String,
    bus: Arc<BroadcastBus>,
    clock: Arc<Clock>,
    http: reqwest::Client,
}

impl Server {
    async fn post(&self, path: &str, body: serde_json::Value) -> serde_json::Value {
        self.http
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .expect("admin post failed")
            .json()
            .await
            .expect("admin response was not JSON")
    }

    async fn get(&self, path: &str) -> serde_json::Value {
        self.http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("admin get failed")
            .json()
            .await
            .expect("admin response was not JSON")
    }
}

async fn serve() -> (Server, Arc<testbed_hooks::Hooks>) {
    common::install_tracing();

    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(1024, Arc::clone(&clock), run));
    let state = Arc::new(State::new(
        Scenario {
            name: "webhook-gate".into(),
            ..Default::default()
        },
        Arc::clone(&clock),
        Arc::clone(&bus) as Arc<dyn EventSink>,
        run,
    ));

    let hooks = Arc::new(testbed_hooks::Hooks::new(
        Arc::clone(state.bus()),
        Arc::clone(&clock),
        run,
    ));

    let app = axum::Router::new()
        .merge(testbed_admin::router(Arc::clone(&state)))
        .merge(testbed_admin::hooks_router(Arc::clone(&hooks)))
        .merge(testbed_http::fault::guard(
            state,
            testbed_hooks::router(Arc::clone(&hooks.inbox)),
        ));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server stopped");
    });

    // The sender is driven by `tick()` from the tests rather than
    // `run_forever`, so a retry fires because virtual time says so and not
    // because the test waited long enough.
    (
        Server {
            base: format!("http://{addr}"),
            bus,
            clock,
            http: reqwest::Client::new(),
        },
        hooks,
    )
}

/// The gate's first half.
#[tokio::test]
async fn a_posted_webhook_is_captured_and_readable() {
    let (server, _hooks) = serve().await;

    server
        .post("/hooks/in/abc", serde_json::json!({ "x": 1 }))
        .await;

    let captures = server.get("/_admin/hooks/in/abc").await;
    assert_eq!(
        captures[0]["body"]["x"], 1,
        "the gate reads .[0].body.x and got {captures}"
    );
    assert!(
        captures.as_array().is_some(),
        "the captures endpoint must serve a bare array; the gate indexes .[0]"
    );
}

/// The gate's second half: one advance, three attempts.
#[tokio::test]
async fn fail_first_produces_attempts_one_two_three() {
    let (server, hooks) = serve().await;
    let mut events = server.bus.subscribe();

    server
        .post(
            "/_admin/hooks/out",
            serde_json::json!({
                "url": format!("{}/hooks/in/abc", server.base),
                "sign": "stripe",
                "fail_first": 2,
            }),
        )
        .await;

    hooks.sender.tick().await;
    server.clock.advance(Duration::from_secs(60));
    // Two more passes: attempts 2 and 3 are both due after the advance, but the
    // loop makes one attempt per delivery per tick.
    hooks.sender.tick().await;
    hooks.sender.tick().await;

    let attempts = tokio::time::timeout(TIMEOUT, async {
        let mut seen = Vec::new();
        while seen.len() < 3 {
            if let EventKind::WebhookOut { attempt, .. } =
                events.next().await.expect("bus closed").kind
            {
                seen.push(attempt);
            }
        }
        seen
    })
    .await
    .expect("fewer than three attempts reached the bus");

    assert_eq!(attempts, vec![1, 2, 3]);
}

/// "Retries must fire at virtual times matching the configured backoff."
///
/// Asserted on the event itself rather than on the sender's internals, because
/// `next_retry_at` is what a scenario has to read.
#[tokio::test]
async fn retries_are_scheduled_at_the_configured_virtual_backoff() {
    let (server, hooks) = serve().await;
    let mut events = server.bus.subscribe();

    server
        .post(
            "/_admin/hooks/out",
            serde_json::json!({
                "url": format!("{}/hooks/in/abc", server.base),
                "fail_first": 9,
                "backoff_ms": [2_000, 7_000],
            }),
        )
        .await;

    hooks.sender.tick().await;

    let enqueued_at = {
        let deliveries = server.get("/_admin/hooks/out").await;
        deliveries["deliveries"][0]["enqueued_at"]
            .as_str()
            .expect("no enqueued_at")
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    };

    let first = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let EventKind::WebhookOut { next_retry_at, .. } =
                events.next().await.expect("bus closed").kind
            {
                return next_retry_at;
            }
        }
    })
    .await
    .expect("no attempt reached the bus");

    assert_eq!(
        first,
        Some(enqueued_at + chrono::TimeDelta::milliseconds(2_000)),
        "the first retry is not one backoff step from the enqueue"
    );
}

/// A retry must not be brought forward by wall time — only by the clock.
#[tokio::test]
async fn a_retry_does_not_fire_before_its_virtual_backoff() {
    let (server, hooks) = serve().await;

    server
        .post(
            "/_admin/hooks/out",
            serde_json::json!({
                "url": format!("{}/hooks/in/abc", server.base),
                "fail_first": 9,
                "backoff_ms": [30_000],
            }),
        )
        .await;

    hooks.sender.tick().await;
    // Real time passes; virtual time does not.
    tokio::time::sleep(Duration::from_millis(200)).await;
    hooks.sender.tick().await;

    let deliveries = server.get("/_admin/hooks/out").await;
    assert_eq!(
        deliveries["deliveries"][0]["attempt"], 1,
        "a retry fired against wall time instead of the virtual clock"
    );
}

/// "The delivered signature must verify against the endpoint secret."
#[tokio::test]
async fn the_delivered_signature_verifies_under_both_schemes() {
    for (scheme, header, expected) in [
        ("stripe", "stripe-signature", SigningScheme::Stripe),
        ("github", "x-hub-signature-256", SigningScheme::Github),
    ] {
        let (server, hooks) = serve().await;

        let queued = server
            .post(
                "/_admin/hooks/out",
                serde_json::json!({
                    "url": format!("{}/hooks/in/abc", server.base),
                    "sign": scheme,
                    "body": { "x": 1 },
                }),
            )
            .await;
        let secret = queued["secret"].as_str().expect("no secret echoed back");

        hooks.sender.tick().await;

        let captures = server.get("/_admin/hooks/in/abc").await;
        let headers = &captures[0]["headers"];
        let signature = headers[header]
            .as_str()
            .unwrap_or_else(|| panic!("{scheme}: no {header} on the delivery: {headers}"));

        // The exact bytes the sender serialized; signing over a re-encoded body
        // would verify here and fail against a real receiver.
        let body = serde_json::to_vec(&serde_json::json!({ "x": 1 })).unwrap();
        assert!(
            testbed_hooks::sign::verify(expected, secret, &body, signature),
            "{scheme}: the delivered signature does not verify against the secret"
        );
        assert!(
            !testbed_hooks::sign::verify(expected, "wrong-secret", &body, signature),
            "{scheme}: the signature verified under a secret it was not signed with"
        );
    }
}

/// Invariant 10, the last boundary: an outbound webhook carries `traceparent`.
#[tokio::test]
async fn every_delivery_carries_an_injected_traceparent() {
    let (server, hooks) = serve().await;

    server
        .post(
            "/_admin/hooks/out",
            serde_json::json!({ "url": format!("{}/hooks/in/abc", server.base) }),
        )
        .await;
    hooks.sender.tick().await;

    let captures = server.get("/_admin/hooks/in/abc").await;
    let traceparent = captures[0]["headers"]["traceparent"]
        .as_str()
        .expect("the delivery carried no traceparent (invariant 10)");

    assert!(
        traceparent.starts_with("00-"),
        "not a W3C traceparent: {traceparent}"
    );
    assert!(
        traceparent.parse::<Traceparent>().is_ok(),
        "the injected traceparent is malformed: {traceparent}"
    );
}

/// `fail_first` short-circuits before the request, so the receiver sees only
/// the attempt that was actually sent.
#[tokio::test]
async fn fail_first_does_not_reach_the_receiver() {
    let (server, hooks) = serve().await;

    server
        .post(
            "/_admin/hooks/out",
            serde_json::json!({
                "url": format!("{}/hooks/in/abc", server.base),
                "fail_first": 2,
            }),
        )
        .await;

    hooks.sender.tick().await;
    server.clock.advance(Duration::from_secs(60));
    hooks.sender.tick().await;
    hooks.sender.tick().await;

    let captures = server.get("/_admin/hooks/in/abc").await;
    assert_eq!(
        captures.as_array().unwrap().len(),
        1,
        "fail_first delivered to the receiver; it is documented as failing \
         regardless of it"
    );
}

/// Minimal W3C traceparent parser, so the assertion above checks a shape rather
/// than a prefix.
struct Traceparent;

impl std::str::FromStr for Traceparent {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        match parts.as_slice() {
            [version, trace, span, flags]
                if version.len() == 2
                    && trace.len() == 32
                    && span.len() == 16
                    && flags.len() == 2
                    && trace.chars().all(|c| c.is_ascii_hexdigit())
                    && span.chars().all(|c| c.is_ascii_hexdigit())
                    && *trace != "0".repeat(32)
                    && *span != "0".repeat(16) =>
            {
                Ok(Self)
            }
            _ => Err(format!("malformed traceparent: {s}")),
        }
    }
}
