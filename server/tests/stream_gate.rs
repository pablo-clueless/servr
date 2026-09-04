//! Phase 5, the streaming half — the HANDOFF §7 gate, run in-process.
//!
//! ```text
//! $ curl -sN localhost:8080/v1/chat/completions \
//!     -d '{"stream":true,"messages":[{"role":"user","content":"hi"}]}' | head -3
//! data: {"choices":[{"delta":{"content":"..."}}]}
//! ```
//!
//! Unlike the WebSocket half these need no socket: the response is an ordinary
//! body, and `oneshot` collects it. What they do need is the assembled router,
//! so the fault layer and the stream routes are exercised together.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use testbed_core::{BroadcastBus, Clock, EventKind, EventSink, RunId, Scenario, State};
use tower::ServiceExt;

struct Harness {
    app: axum::Router,
    state: Arc<State>,
    bus: Arc<BroadcastBus>,
}

fn harness() -> Harness {
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(1024, Arc::clone(&clock), run));
    let state = Arc::new(State::new(
        Scenario {
            name: "stream-gate".into(),
            ..Default::default()
        },
        Arc::clone(&clock),
        Arc::clone(&bus) as Arc<dyn EventSink>,
        run,
    ));

    let streams = testbed_stream::Streams::new(Arc::clone(state.bus()), Arc::clone(&clock), run);

    let app = axum::Router::new()
        .merge(testbed_admin::router(Arc::clone(&state)))
        // Behind the fault layer, exactly as `server` mounts it.
        .merge(testbed_http::fault::guard(
            Arc::clone(&state),
            testbed_stream::router(streams),
        ));

    Harness { app, state, bus }
}

async fn get(app: &axum::Router, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn chat(app: &axum::Router, body: serde_json::Value) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Collects a whole SSE body as text. Every stream under test is finite.
async fn text(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body did not complete");
    String::from_utf8(bytes.to_vec()).expect("body was not utf-8")
}

/// The `data:` payloads of an SSE body, in order, `[DONE]` excluded.
fn payloads(body: &str) -> Vec<serde_json::Value> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|data| *data != testbed_stream::DONE)
        .map(|data| serde_json::from_str(data).expect("chunk was not JSON"))
        .collect()
}

/// The gate verbatim: the first lines of the response are OpenAI-shaped chunks.
#[tokio::test]
async fn the_gate_request_streams_openai_shaped_chunks() {
    let h = harness();
    let response = chat(
        &h.app,
        serde_json::json!({
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }],
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream"),
        "a client will not treat this as a stream"
    );

    let body = text(response).await;
    let head: Vec<&str> = body.lines().take(3).collect();
    assert!(
        head[0].starts_with("data: "),
        "the first line is not an SSE data frame: {head:?}"
    );

    let chunks = payloads(&body);
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "hi");
    assert_eq!(
        chunks.last().unwrap()["choices"][0]["finish_reason"],
        "stop"
    );

    assert!(
        body.contains(&format!("data: {}", testbed_stream::DONE)),
        "the stream never sent [DONE]; a client will wait for one"
    );
}

/// The property a streaming client is most at risk of getting wrong, and the
/// reason the reply is an echo rather than canned text.
#[tokio::test]
async fn the_deltas_reassemble_into_the_prompt() {
    let h = harness();
    let response = chat(
        &h.app,
        serde_json::json!({
            "stream": true,
            "messages": [{ "role": "user", "content": "hello there world" }],
        }),
    )
    .await;

    let assembled: String = payloads(&text(response).await)
        .iter()
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(String::from)
        })
        .collect();

    assert_eq!(assembled, "hello there world");
}

/// `stream` is absent or false: a single JSON completion, not SSE.
#[tokio::test]
async fn a_non_streaming_request_returns_one_completion() {
    let h = harness();
    let response = chat(
        &h.app,
        serde_json::json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let completion: serde_json::Value = serde_json::from_str(&text(response).await).unwrap();

    assert_eq!(completion["object"], "chat.completion");
    assert_eq!(completion["choices"][0]["message"]["content"], "hi");
    assert_eq!(completion["choices"][0]["finish_reason"], "stop");
}

/// The bespoke route (Q3): every knob is a query parameter.
#[tokio::test]
async fn the_scripted_route_emits_the_requested_chunks_in_order() {
    let h = harness();
    let body = text(get(&h.app, "/_stream/demo?chunks=4&body=tick").await).await;

    let chunks = payloads(&body);
    assert_eq!(chunks.len(), 4);
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk["seq"], i as u64);
        assert_eq!(chunk["body"], "tick");
    }
}

/// Invariant 4: one `StreamChunk` per chunk, with a dense `seq`. A consumer
/// detects a lost chunk by the gap, so the numbering has to be exact.
#[tokio::test]
async fn every_chunk_lands_on_the_event_bus_with_its_sequence() {
    let h = harness();
    let mut events = h.bus.subscribe();

    let body = text(get(&h.app, "/_stream/demo?chunks=3").await).await;
    assert_eq!(payloads(&body).len(), 3);

    // The bus carries every surface, so this filters rather than reading the
    // next three events. The fault layer emits its own `HttpRequest` for this
    // request — and emits it *first*, when the handler returns the body, long
    // before the body has finished streaming.
    let seqs = tokio::time::timeout(Duration::from_secs(2), async {
        let mut seqs = Vec::new();
        while seqs.len() < 3 {
            let event = events.next().await.expect("bus closed");
            if let EventKind::StreamChunk { seq, .. } = event.kind {
                seqs.push(seq);
            }
        }
        seqs
    })
    .await
    .expect("fewer than three chunk events reached the bus");

    assert_eq!(seqs, vec![0, 1, 2]);
}

/// Invariant 7, and the reason chunk pacing is a poll loop against the virtual
/// clock: 30 virtual seconds of delay must cost milliseconds of real time.
///
/// This is the streaming counterpart of the Phase 4 timing gate. If it ever
/// starts taking 30 seconds, the pacer has been "simplified" back to
/// `sleep(delay)` and the slow-stream scenario has become untestable.
#[tokio::test]
async fn advancing_the_clock_flushes_a_slow_stream() {
    let h = harness();

    // 3 chunks, 10 virtual seconds apart: 20s of virtual pacing after the
    // first, which is due immediately.
    let response = get(&h.app, "/_stream/slow?chunks=3&delay_ms=10000").await;
    assert_eq!(response.status(), StatusCode::OK);

    let collect = tokio::spawn(text(response));

    // Let the pacer park on its first pending chunk before jumping the clock.
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.state.clock().advance(Duration::from_secs(30));

    let body = tokio::time::timeout(Duration::from_secs(5), collect)
        .await
        .expect("the stream did not finish; pacing is reading wall time")
        .expect("body task panicked");

    assert_eq!(payloads(&body).len(), 3);
}

/// A stream that is *not* released keeps its chunks — otherwise the previous
/// test would pass against a pacer that ignores the delay entirely.
#[tokio::test]
async fn a_slow_stream_withholds_its_chunks_until_the_clock_moves() {
    let h = harness();
    let response = get(&h.app, "/_stream/slow?chunks=3&delay_ms=10000").await;

    assert!(
        tokio::time::timeout(Duration::from_millis(300), text(response))
            .await
            .is_err(),
        "a stream paced 10 virtual seconds apart completed without the clock moving"
    );
}

/// A mid-stream failure: the client gets the chunks written so far and then a
/// truncated transfer. An SSE response cannot retroactively change its status.
#[tokio::test]
async fn a_mid_stream_fault_truncates_the_body() {
    let h = harness();
    let response = get(&h.app, "/_stream/demo?chunks=5&fail_at=2").await;
    assert_eq!(response.status(), StatusCode::OK);

    let result = axum::body::to_bytes(response.into_body(), usize::MAX).await;
    assert!(
        result.is_err(),
        "the body completed cleanly; the client cannot tell the stream broke"
    );
}

/// The stream routes sit behind the fault layer like every other data-plane
/// route, so a scenario can fail the response that carries the stream.
#[tokio::test]
async fn a_fault_on_the_chat_route_is_applied() {
    let h = harness();
    h.state.mutate(|overlay| {
        overlay.faults = Some(vec![testbed_core::FaultSpec {
            route: "/v1/*".into(),
            rate: 1.0,
            status: Some(503),
            ..Default::default()
        }]);
    });

    let response = chat(&h.app, serde_json::json!({ "stream": true })).await;
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "/v1/chat/completions is outside the fault layer"
    );
}
