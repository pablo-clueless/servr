//! SSE and chat streaming.
//!
//! # Q3 — resolved
//!
//! Operator decision: OpenAI-compatible `/v1/chat/completions` for anything
//! pointing a real client at the testbed, plus a bespoke `/_stream/{id}` escape
//! hatch for scripting arbitrary chunk sequences, delays and mid-stream faults.
//!
//! # Not yet built — Phase 5 (HANDOFF §9 task 11)
//!
//! - token-by-token SSE on both routes
//! - an `EventKind::StreamChunk` per chunk, with `seq`
//!
//! Trap T8: SSE dies behind proxies without keep-alive. Use
//! `axum::response::sse::KeepAlive` on every stream response.

/// OpenAI-compatible route (Q3).
pub const CHAT_ROUTE: &str = "/v1/chat/completions";
/// Bespoke escape hatch for scripted chunks (Q3). axum 0.8 param syntax (T1).
pub const SCRIPTED_ROUTE: &str = "/_stream/{id}";
