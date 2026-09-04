//! Phase 5, the WebSocket half — the HANDOFF §7 gate, run in-process.
//!
//! ```text
//! $ websocat -t ws://localhost:8080/ws?topic=demo &
//! $ curl -s -X POST localhost:8080/_admin/ws/publish -d '{"topic":"demo","body":"hi"}'
//! # subscriber prints: hi
//! $ curl -s -X POST localhost:8080/_admin/ws/kill -d '{"topic":"demo"}'
//! # subscriber exits with a clean close, not a timeout   <- see T6
//! ```
//!
//! # Why this is over a real socket
//!
//! `tower::ServiceExt::oneshot` cannot express any of it. The upgrade is the
//! point, and T6 — the trap this whole surface is shaped around — is *only*
//! observable to a client that inspects the close frame. A dropped handle and a
//! clean close look identical to anything that merely notices the connection
//! ended, which is precisely why the trap is worth a test.
//!
//! These live in `server` because it is the only crate permitted to depend on
//! more than one surface (HANDOFF §4).

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use testbed_core::{BroadcastBus, Clock, EventKind, EventSink, RunId, Scenario, State};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tracing_subscriber::layer::SubscriberExt;

/// Every read is bounded. An unbounded `recv().await` on a close that never
/// arrives is exactly the hang T6 describes, and a test that hangs reports
/// nothing useful — a timeout names the failure.
const READ_TIMEOUT: Duration = Duration::from_secs(2);

struct Server {
    addr: std::net::SocketAddr,
    bus: Arc<BroadcastBus>,
    client: reqwest_lite::Client,
}

impl Server {
    fn ws_url(&self, topic: &str) -> String {
        format!("ws://{}/ws?topic={topic}", self.addr)
    }

    async fn admin(&self, path: &str, body: serde_json::Value) -> serde_json::Value {
        self.client
            .post_json(&format!("http://{}{path}", self.addr), body)
            .await
    }

    async fn admin_get(&self, path: &str) -> serde_json::Value {
        self.client
            .get_json(&format!("http://{}{path}", self.addr))
            .await
    }
}

/// Installs a process-wide tracer with no exporter: spans get real, valid ids
/// and nothing leaves the process.
///
/// Global, not `with_default`, because the spans under test are opened on
/// *other* tasks — the connection task and the admin handler's. A
/// thread-scoped subscriber reaches neither, and the visible symptom is a bus
/// event with no trace id, which reads as an invariant-9 violation rather than
/// as a missing subscriber.
fn install_tracing() {
    static PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

    let provider = PROVIDER.get_or_init(|| SdkTracerProvider::builder().build());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ws-gate")));

    // Every test in this binary shares the process, so all but the first call
    // lose the race. That is fine — they wanted the same subscriber.
    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Boots the real assembled router on an ephemeral port.
async fn serve() -> Server {
    install_tracing();

    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
    let state = Arc::new(State::new(
        Scenario {
            name: "ws-gate".into(),
            ..Default::default()
        },
        Arc::clone(&clock),
        Arc::clone(&bus) as Arc<dyn EventSink>,
        run,
    ));

    let hub = Arc::new(testbed_ws::Hub::new(
        Arc::clone(state.bus()),
        Arc::clone(&clock),
        run,
    ));

    let app = axum::Router::new()
        .merge(testbed_admin::router(Arc::clone(&state)))
        .merge(testbed_admin::ws_router(Arc::clone(&hub)))
        // Through the fault layer, exactly as `server` mounts it — an upgrade
        // that only works when unwrapped is not the thing being shipped.
        .merge(testbed_http::fault::guard(state, testbed_ws::router(hub)));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind failed");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server stopped");
    });

    Server {
        addr,
        bus,
        client: reqwest_lite::Client::new(),
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(server: &Server, topic: &str) -> Socket {
    let (socket, _) = tokio_tungstenite::connect_async(server.ws_url(topic))
        .await
        .expect("websocket upgrade failed");
    socket
}

async fn next(socket: &mut Socket) -> Option<Message> {
    tokio::time::timeout(READ_TIMEOUT, socket.next())
        .await
        .expect("timed out waiting for a frame")
        .map(|m| m.expect("websocket read failed"))
}

/// The gate's first half: a publish reaches the subscriber.
#[tokio::test]
async fn an_admin_publish_reaches_the_subscriber() {
    let server = serve().await;
    let mut socket = connect(&server, "demo").await;

    // The upgrade completes before the hub records the member, so a publish
    // racing the join would legitimately deliver nothing. Presence is the
    // signal that the join landed.
    wait_for_members(&server, "demo", 1).await;

    let response = server
        .admin(
            "/_admin/ws/publish",
            serde_json::json!({ "topic": "demo", "body": "hi" }),
        )
        .await;
    assert_eq!(response["delivered"], 1);

    match next(&mut socket).await {
        Some(Message::Text(body)) => assert_eq!(body.as_str(), "hi"),
        other => panic!("expected the published frame, got {other:?}"),
    }
}

/// The gate's second half, and trap T6: the subscriber exits on an explicit
/// Close frame rather than sitting on a read until its own timeout fires.
///
/// The distinction is the whole reason this endpoint exists. A client cannot
/// tell a dropped connection from a network partition, so reconnection logic
/// under test would take the wrong branch — and the test asserting it would
/// still pass.
#[tokio::test]
async fn kill_closes_the_connection_cleanly_rather_than_dropping_it() {
    let server = serve().await;
    let mut socket = connect(&server, "demo").await;
    wait_for_members(&server, "demo", 1).await;

    let response = server
        .admin(
            "/_admin/ws/kill",
            serde_json::json!({ "topic": "demo", "reason": "server closed" }),
        )
        .await;
    assert_eq!(response["closed"], 1);

    match next(&mut socket).await {
        Some(Message::Close(Some(frame))) => {
            assert_eq!(
                u16::from(frame.code),
                testbed_ws::CLOSE_CODE,
                "a server-initiated close must be Going Away, not Normal or Abnormal"
            );
            assert_eq!(frame.reason.as_str(), "server closed");
        }
        Some(Message::Close(None)) => panic!("closed without a frame; the reason is lost"),
        None => panic!("the socket ended without a Close frame — T6"),
        other => panic!("expected a Close frame, got {other:?}"),
    }

    // And then the stream really does end, rather than idling.
    assert!(
        next(&mut socket).await.is_none(),
        "the socket stayed open after the close"
    );
}

/// Presence is what `/_admin/ws` serves, and a kill must clear it immediately
/// — not once each connection task happens to notice.
#[tokio::test]
async fn presence_tracks_connections_and_a_kill_clears_it() {
    let server = serve().await;
    let mut a = connect(&server, "demo").await;
    let mut b = connect(&server, "demo").await;
    wait_for_members(&server, "demo", 2).await;

    let presence = server.admin_get("/_admin/ws").await;
    assert_eq!(presence["connections"], 2);
    assert_eq!(presence["topics"]["demo"].as_array().unwrap().len(), 2);

    server
        .admin("/_admin/ws/kill", serde_json::json!({ "topic": "demo" }))
        .await;

    // Both clients drain their close frames so the sockets are not dropped
    // mid-assertion, which would make the emptiness ambiguous.
    for socket in [&mut a, &mut b] {
        assert!(matches!(next(socket).await, Some(Message::Close(_))));
    }

    let presence = server.admin_get("/_admin/ws").await;
    assert_eq!(presence["connections"], 0);
    assert!(presence["topics"].as_object().unwrap().is_empty());
}

/// A topic hub, not a broadcast pipe: a client's own frame goes to the rest of
/// the topic and not back to itself.
#[tokio::test]
async fn a_client_frame_fans_out_to_the_topic_but_not_to_its_sender() {
    let server = serve().await;
    let mut a = connect(&server, "demo").await;
    let mut b = connect(&server, "demo").await;
    wait_for_members(&server, "demo", 2).await;

    a.send(Message::text("from a")).await.unwrap();

    match next(&mut b).await {
        Some(Message::Text(body)) => assert_eq!(body.as_str(), "from a"),
        other => panic!("the other member did not receive the frame: {other:?}"),
    }

    // Nothing comes back to the sender. A short timeout is the only way to
    // assert an absence, so this one is deliberately not `READ_TIMEOUT`.
    assert!(
        tokio::time::timeout(Duration::from_millis(250), a.next())
            .await
            .is_err(),
        "the sender received its own frame back"
    );
}

/// Invariant 4: a frame is a bus event as well as a span. Invariant 9: it
/// carries the trace context to join against.
#[tokio::test]
async fn every_frame_lands_on_the_event_bus_with_a_join_key() {
    let server = serve().await;
    let mut events = server.bus.subscribe();
    let mut socket = connect(&server, "demo").await;
    wait_for_members(&server, "demo", 1).await;

    server
        .admin(
            "/_admin/ws/publish",
            serde_json::json!({ "topic": "demo", "body": "hello" }),
        )
        .await;
    let _ = next(&mut socket).await;

    let frame = tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            let event = events.next().await.expect("bus closed");
            if let EventKind::WsFrame { .. } = event.kind {
                return event;
            }
        }
    })
    .await
    .expect("no WsFrame reached the bus");

    match &frame.kind {
        EventKind::WsFrame {
            topic, dir, bytes, ..
        } => {
            assert_eq!(topic, "demo");
            assert_eq!(*dir, testbed_core::Dir::Out);
            assert_eq!(*bytes, 5);
        }
        other => unreachable!("filtered for WsFrame, got {other:?}"),
    }

    assert!(
        frame.is_joinable(),
        "the frame event carries no trace id; it can be joined to nothing (invariant 9)"
    );
}

/// The upgrade goes through the fault layer like any other request, so a
/// scenario can make a client's reconnect path run for real.
#[tokio::test]
async fn a_fault_on_the_upgrade_is_applied() {
    let server = serve().await;

    server
        .admin(
            "/_admin/faults",
            serde_json::json!({ "route": "/ws", "rate": 1.0, "status": 503 }),
        )
        .await;

    let result = tokio_tungstenite::connect_async(server.ws_url("demo")).await;
    assert!(
        result.is_err(),
        "the upgrade succeeded through a 503 fault; /ws is outside the fault layer"
    );
}

/// Polls presence until the hub has recorded `expected` members.
///
/// The upgrade response returns before the connection task has joined, so
/// every test that publishes needs this. Bounded, so a join that never lands
/// fails here with a clear message rather than as a missing frame later.
///
/// Counted attempts rather than a deadline: invariant 1 permits a monotonic
/// wall-clock read only in `clock.rs` and `wall.rs`, and CI greps test code
/// too — including, as this comment has to be careful of, its comments.
async fn wait_for_members(server: &Server, topic: &str, expected: usize) {
    const POLL: Duration = Duration::from_millis(10);
    let attempts = (READ_TIMEOUT.as_millis() / POLL.as_millis()) as u32;

    let mut count = 0;
    for _ in 0..attempts {
        let presence = server.admin_get("/_admin/ws").await;
        count = presence["topics"][topic]
            .as_array()
            .map_or(0, |members| members.len());
        if count >= expected {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
    panic!("only {count} of {expected} connections joined {topic}");
}

/// A JSON-over-HTTP client, hand-rolled.
///
/// The workspace has no HTTP client dependency and this does not justify
/// adding one: three admin endpoints, request bodies under a hundred bytes,
/// and responses this test fully controls. `hyper` is already in the tree via
/// `axum`, so this is a thin wrapper rather than a protocol implementation.
mod reqwest_lite {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[derive(Default)]
    pub struct Client;

    impl Client {
        pub fn new() -> Self {
            Self
        }

        pub async fn get_json(&self, url: &str) -> serde_json::Value {
            self.request("GET", url, None).await
        }

        pub async fn post_json(&self, url: &str, body: serde_json::Value) -> serde_json::Value {
            self.request("POST", url, Some(body.to_string())).await
        }

        async fn request(
            &self,
            method: &str,
            url: &str,
            body: Option<String>,
        ) -> serde_json::Value {
            let rest = url.strip_prefix("http://").expect("http:// url");
            let (authority, path) = rest.split_once('/').expect("url has a path");
            let path = format!("/{path}");

            let mut stream = TcpStream::connect(authority).await.expect("connect failed");
            let body = body.unwrap_or_default();
            let request = format!(
                "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(request.as_bytes()).await.unwrap();

            let mut raw = Vec::new();
            // `Connection: close` means the server ends the response by closing,
            // so reading to EOF is the framing — no chunked decoding needed.
            tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut raw))
                .await
                .expect("admin request timed out")
                .expect("admin read failed");

            let text = String::from_utf8_lossy(&raw);
            let (_, payload) = text
                .split_once("\r\n\r\n")
                .unwrap_or_else(|| panic!("malformed response: {text}"));
            serde_json::from_str(payload.trim())
                .unwrap_or_else(|e| panic!("response was not JSON ({e}): {payload}"))
        }
    }
}
