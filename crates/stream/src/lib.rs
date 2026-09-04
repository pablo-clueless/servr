//! SSE and chat streaming.
//!
//! # Q3 — resolved
//!
//! Operator decision: OpenAI-compatible [`CHAT_ROUTE`] for anything pointing a
//! real client at the testbed, plus a bespoke [`SCRIPTED_ROUTE`] escape hatch
//! for scripting arbitrary chunk sequences, delays and mid-stream faults.
//!
//! # Pacing is virtual
//!
//! Inter-chunk delay is scheduling, so it reads the virtual clock (invariant
//! 7): a stream configured to take 30 seconds finishes in milliseconds once the
//! clock is advanced past it. See [`chunks`] for why that is a poll loop.
//!
//! # Trap T8
//!
//! SSE dies behind proxies without keep-alive. Every response here is built
//! through [`Streams::sse`], which applies it, rather than by constructing
//! [`axum::response::sse::Sse`] at each call site.

pub mod chat;
pub mod chunks;
pub mod scripted;

use std::sync::Arc;

use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::{
    routing::{get, post},
    Router,
};
use futures_util::Stream;
use testbed_core::{Clock, Event, EventKind, EventSink, RunId, StreamId};

/// OpenAI-compatible route (Q3).
pub const CHAT_ROUTE: &str = "/v1/chat/completions";
/// Bespoke escape hatch for scripted chunks (Q3). axum 0.8 param syntax (T1).
pub const SCRIPTED_ROUTE: &str = "/_stream/{id}";

/// The sentinel every OpenAI-compatible stream ends with. Clients treat it as
/// end-of-stream rather than waiting for the connection to close, so omitting
/// it makes a correct stream look like a hung one.
pub const DONE: &str = "[DONE]";

/// What a stream handler needs: somewhere to emit, and the clock that paces it.
#[derive(Clone)]
pub struct Streams {
    bus: Arc<dyn EventSink>,
    clock: Arc<Clock>,
    run: RunId,
}

impl Streams {
    pub fn new(bus: Arc<dyn EventSink>, clock: Arc<Clock>, run: RunId) -> Self {
        Self { bus, clock, run }
    }

    pub fn clock(&self) -> &Arc<Clock> {
        &self.clock
    }

    /// Invariant 4, the event half: one [`EventKind::StreamChunk`] per chunk.
    ///
    /// The span half is the stream-level span each handler opens; a span per
    /// chunk would multiply a 500-token completion into 500 spans for no
    /// information a `seq` does not already carry.
    pub fn emit_chunk(&self, stream: StreamId, seq: u32) {
        let (trace_id, span_id) = match testbed_telemetry::propagation::current_ids() {
            Some((t, s)) => (Some(t), Some(s)),
            None => (None, None),
        };

        self.bus.emit(Event {
            id: 0,
            run: self.run,
            at: self.clock.now(),
            wall_at: Clock::wall_now(),
            trace_id,
            span_id,
            kind: EventKind::StreamChunk { stream, seq },
        });
    }

    /// Wraps a chunk stream as SSE with keep-alive (T8).
    pub fn sse<S, E>(stream: S) -> impl IntoResponse
    where
        S: Stream<Item = Result<SseEvent, E>> + Send + 'static,
        E: Into<axum::BoxError>,
    {
        Sse::new(stream).keep_alive(KeepAlive::default())
    }
}

/// Both streaming routes.
///
/// Mounted by `server` behind the fault layer, so a scenario can delay or fail
/// the response that carries the stream. Chunk-level faults are separate and
/// live in the request itself — see [`chat::ChatRequest::fail_at`].
pub fn router(streams: Streams) -> Router {
    Router::new()
        .route(CHAT_ROUTE, post(chat::completions))
        .route(SCRIPTED_ROUTE, get(scripted::stream))
        .with_state(streams)
}

/// A chunk stream's item type. `Infallible` is not usable: a mid-stream fault
/// has to be expressible, and it is expressed as an error on the body.
pub type ChunkResult = Result<SseEvent, StreamError>;

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// A deliberate mid-stream failure. The client sees the bytes written so
    /// far and then a truncated transfer, which is the failure mode being
    /// simulated — an SSE stream cannot retroactively change its status code.
    #[error("stream failed at chunk {seq} by fault injection")]
    Injected { seq: u32 },
}
