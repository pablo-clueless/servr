//! `POST /v1/chat/completions` — OpenAI-compatible, streaming or not (Q3).
//!
//! # The reply is an echo, deliberately
//!
//! The testbed has no model behind it and should not pretend to. Echoing the
//! last user message back makes every assertion in a client test deterministic:
//! a test can send `"hello there"` and assert the assembled deltas equal
//! `"hello there"`, which is exactly the property a streaming client is at risk
//! of getting wrong. A canned lorem-ipsum reply would test nothing.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::Event as SseEvent;
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use testbed_core::StreamId;

use crate::chunks;
use crate::{ChunkResult, StreamError, Streams, DONE};

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    /// OpenAI's own field. Absent means a single non-streamed response.
    #[serde(default)]
    pub stream: bool,

    // Testbed extensions. Unknown fields are ignored by real clients, and
    // these are ignored by real servers, so a request carrying them stays
    // valid against both.
    /// Virtual milliseconds between chunks. Advancing the clock past the total
    /// flushes the rest of the stream immediately.
    #[serde(default)]
    pub delay_ms: u64,
    /// Fail the stream after this many chunks — a mid-stream truncation, which
    /// is the failure a streaming client is least likely to handle.
    #[serde(default)]
    pub fail_at: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(default)]
    pub content: String,
}

impl ChatRequest {
    /// The text the completion replies with: the last user message, or the last
    /// message of any role when none is from a user.
    pub fn reply_text(&self) -> String {
        self.messages
            .iter()
            .rev()
            .find(|m| m.role == "user")
            .or_else(|| self.messages.last())
            .map(|m| m.content.clone())
            .unwrap_or_default()
    }

    fn model(&self) -> String {
        self.model
            .clone()
            .unwrap_or_else(|| "testbed-echo".to_string())
    }
}

/// The body is parsed from raw bytes rather than through `axum::Json`.
///
/// `Json` rejects anything without `Content-Type: application/json`, and the
/// §7 gate is a bare `curl -d '{...}'`, which sends form-urlencoded. The
/// control plane solves this with `testbed_http::json::Lenient`; this crate
/// cannot use it — `stream` may depend on `core` and `telemetry` and on no
/// other surface (§4) — and one handler does not justify the edge.
pub async fn completions(State(streams): State<Streams>, body: Bytes) -> Response {
    let body: ChatRequest = match serde_json::from_slice(&body) {
        Ok(body) => body,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("body is not a chat request: {e}") })),
            )
                .into_response()
        }
    };

    let id = StreamId::new();
    let text = body.reply_text();
    let model = body.model();

    // `created` comes from the virtual clock, not wall time: an OpenAI client
    // reading it must see the same timeline as `/_admin/events` and the queue,
    // or a `clock/advance` makes the two disagree in a way that looks like a
    // bug in whatever is being tested.
    let created = streams.clock().now().timestamp();

    let span = tracing::info_span!(
        "stream.chat",
        otel.name = "chat completions",
        testbed.stream.id = %id,
        testbed.stream.model = %model,
        testbed.stream.streaming = body.stream,
        testbed.stream.chunks = tracing::field::Empty,
    );
    let _entered = span.enter();

    if !body.stream {
        // The non-streamed branch still emits one chunk event: from the bus's
        // point of view a completion happened, and a consumer filtering on
        // `StreamChunk` should not have to know which transport carried it.
        streams.emit_chunk(id, 0);
        span.record("testbed.stream.chunks", 1);
        return Json(completion(&id, &model, created, &text)).into_response();
    }

    let pieces = pieces(&id, &model, created, &text);
    span.record("testbed.stream.chunks", pieces.len() as u64);

    // The body outlives this handler, so the stream carries its own clones of
    // everything it needs — including the span, which stays open until the last
    // chunk is written rather than closing when the handler returns.
    let start = streams.clock().now();
    let fail_at = body.fail_at;
    let delay_ms = body.delay_ms;
    let span_for_stream = span.clone();

    let body = stream::unfold(
        (0usize, pieces, streams, span_for_stream),
        move |(index, pieces, streams, span)| async move {
            if index >= pieces.len() {
                return None;
            }
            let seq = index as u32;

            if fail_at.is_some_and(|at| seq >= at) {
                return Some((
                    Err(StreamError::Injected { seq }),
                    (pieces.len(), pieces, streams, span),
                ));
            }

            chunks::wait_until(streams.clock(), chunks::due_at(start, seq, delay_ms)).await;

            let item: ChunkResult = {
                let _entered = span.enter();
                streams.emit_chunk(id, seq);
                Ok(SseEvent::default().data(pieces[index].clone()))
            };

            Some((item, (index + 1, pieces, streams, span)))
        },
    );

    Streams::sse(body).into_response()
}

/// The SSE `data:` payloads for one streamed completion, in order.
///
/// The shape follows OpenAI's: a first chunk carrying only the role, then one
/// chunk per token, then a chunk with `finish_reason`, then `[DONE]`. Clients
/// key off the role chunk to open an assistant message, so a stream that goes
/// straight to content works against some libraries and not others.
fn pieces(id: &StreamId, model: &str, created: i64, text: &str) -> Vec<String> {
    let mut pieces = vec![chunk(
        id,
        model,
        created,
        json!({"role": "assistant"}),
        None,
    )];

    for token in chunks::tokenize(text) {
        pieces.push(chunk(id, model, created, json!({ "content": token }), None));
    }

    pieces.push(chunk(id, model, created, json!({}), Some("stop")));
    pieces.push(DONE.to_string());
    pieces
}

fn chunk(
    id: &StreamId,
    model: &str,
    created: i64,
    delta: Value,
    finish_reason: Option<&str>,
) -> String {
    json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }],
    })
    .to_string()
}

fn completion(id: &StreamId, model: &str, created: i64, text: &str) -> Value {
    json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text },
            "finish_reason": "stop",
        }],
        // Token counts a real client may divide by; character counts stand in,
        // because the alternative is a plausible-looking number that is a lie.
        "usage": {
            "prompt_tokens": text.len(),
            "completion_tokens": text.len(),
            "total_tokens": text.len() * 2,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(messages: &[(&str, &str)]) -> ChatRequest {
        ChatRequest {
            model: None,
            messages: messages
                .iter()
                .map(|(role, content)| ChatMessage {
                    role: role.to_string(),
                    content: content.to_string(),
                })
                .collect(),
            stream: true,
            delay_ms: 0,
            fail_at: None,
        }
    }

    #[test]
    fn the_reply_echoes_the_last_user_message() {
        let req = request(&[
            ("system", "ignore me"),
            ("user", "first"),
            ("assistant", "…"),
            ("user", "second"),
        ]);
        assert_eq!(req.reply_text(), "second");
    }

    #[test]
    fn a_conversation_with_no_user_turn_still_replies() {
        assert_eq!(request(&[("system", "hello")]).reply_text(), "hello");
        assert_eq!(request(&[]).reply_text(), "");
    }

    /// The Phase 5 gate reads the first three lines of the response, so the
    /// role chunk plus one content chunk has to be enough to fill them.
    #[test]
    fn the_gates_request_produces_a_role_chunk_then_content_then_done() {
        let id = StreamId::new();
        let pieces = pieces(&id, "testbed-echo", 0, "hi");
        assert_eq!(pieces.len(), 4, "role, one token, finish, done");

        let first: Value = serde_json::from_str(&pieces[0]).unwrap();
        assert_eq!(first["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(first["object"], "chat.completion.chunk");

        let content: Value = serde_json::from_str(&pieces[1]).unwrap();
        assert_eq!(content["choices"][0]["delta"]["content"], "hi");

        let finish: Value = serde_json::from_str(&pieces[2]).unwrap();
        assert_eq!(finish["choices"][0]["finish_reason"], "stop");

        assert_eq!(pieces[3], DONE);
    }

    #[test]
    fn the_deltas_reassemble_into_the_reply() {
        let id = StreamId::new();
        let pieces = pieces(&id, "m", 0, "hello there world");

        let assembled: String = pieces
            .iter()
            .filter(|p| *p != DONE)
            .filter_map(|p| serde_json::from_str::<Value>(p).ok())
            .filter_map(|c| {
                c["choices"][0]["delta"]["content"]
                    .as_str()
                    .map(String::from)
            })
            .collect();

        assert_eq!(assembled, "hello there world");
    }

    #[test]
    fn every_chunk_in_one_stream_shares_an_id() {
        let id = StreamId::new();
        let expected = format!("chatcmpl-{id}");
        for piece in pieces(&id, "m", 0, "a b") {
            if piece == DONE {
                continue;
            }
            let chunk: Value = serde_json::from_str(&piece).unwrap();
            assert_eq!(chunk["id"], expected);
        }
    }
}
