//! `GET /_stream/{id}` — the bespoke escape hatch (Q3).
//!
//! Everything about the stream is a query parameter, so a scenario can be
//! written as a URL rather than as a scenario file: how many chunks, how far
//! apart in virtual time, and where it breaks.
//!
//! ```text
//! /_stream/demo?chunks=5&delay_ms=1000        five chunks, one virtual second apart
//! /_stream/demo?chunks=5&fail_at=2            two chunks, then a truncated transfer
//! /_stream/demo?chunks=3&body=tick            chunks carrying a chosen payload
//! ```

use axum::extract::{Path, Query, State};
use axum::response::sse::Event as SseEvent;
use axum::response::IntoResponse;
use futures_util::stream;
use serde::Deserialize;
use serde_json::json;
use testbed_core::StreamId;

use crate::chunks;
use crate::{ChunkResult, StreamError, Streams};

#[derive(Debug, Deserialize)]
pub struct Params {
    #[serde(default = "default_chunks")]
    pub chunks: u32,
    /// Virtual milliseconds between chunks.
    #[serde(default)]
    pub delay_ms: u64,
    /// Emit this many chunks, then error the body mid-transfer.
    #[serde(default)]
    pub fail_at: Option<u32>,
    /// Payload for each chunk. The `seq` is added alongside it either way.
    #[serde(default = "default_body")]
    pub body: String,
}

fn default_chunks() -> u32 {
    3
}

fn default_body() -> String {
    "chunk".to_string()
}

pub async fn stream(
    State(streams): State<Streams>,
    Path(name): Path<String>,
    Query(params): Query<Params>,
) -> impl IntoResponse {
    let id = StreamId::new();

    let span = tracing::info_span!(
        "stream.scripted",
        otel.name = %format!("stream {name}"),
        testbed.stream.id = %id,
        testbed.stream.name = %name,
        testbed.stream.chunks = params.chunks,
        testbed.stream.delay_ms = params.delay_ms,
    );

    let start = streams.clock().now();
    let total = params.chunks;
    let fail_at = params.fail_at;
    let delay_ms = params.delay_ms;
    let body = params.body;

    // The span is moved into the stream rather than entered here: the handler
    // returns as soon as the body exists, and a span closed at that point would
    // report a duration of microseconds for a stream that runs for seconds.
    let chunk_stream = stream::unfold(
        (0u32, streams, span, body),
        move |(seq, streams, span, body)| async move {
            if seq >= total {
                return None;
            }

            if fail_at.is_some_and(|at| seq >= at) {
                return Some((
                    Err(StreamError::Injected { seq }),
                    (total, streams, span, body),
                ));
            }

            chunks::wait_until(streams.clock(), chunks::due_at(start, seq, delay_ms)).await;

            let item: ChunkResult = {
                let _entered = span.enter();
                streams.emit_chunk(id, seq);
                Ok(SseEvent::default().data(
                    json!({ "stream": id.to_string(), "seq": seq, "body": body }).to_string(),
                ))
            };

            Some((item, (seq + 1, streams, span, body)))
        },
    );

    Streams::sse(chunk_stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(query: &str) -> Params {
        serde_urlencoded::from_str(query).expect("query did not deserialize")
    }

    #[test]
    fn a_bare_request_still_streams() {
        let p = params("");
        assert_eq!(p.chunks, 3);
        assert_eq!(p.delay_ms, 0);
        assert_eq!(p.body, "chunk");
        assert!(p.fail_at.is_none());
    }

    #[test]
    fn every_knob_is_reachable_from_the_query_string() {
        let p = params("chunks=5&delay_ms=1000&fail_at=2&body=tick");
        assert_eq!(p.chunks, 5);
        assert_eq!(p.delay_ms, 1000);
        assert_eq!(p.fail_at, Some(2));
        assert_eq!(p.body, "tick");
    }
}
