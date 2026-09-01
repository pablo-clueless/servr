//! WebSocket hub: topics, presence, and server-initiated disconnects.
//!
//! # Not yet built — Phase 5 (HANDOFF §9 task 11)
//!
//! - a topic hub with fan-out and per-topic presence
//! - `/_admin/ws/publish` and `/_admin/ws/kill`
//! - a connection span, with per-frame children *linked* to it
//! - an `EventKind::WsFrame` per frame, both directions
//!
//! Trap T6: a forced disconnect must send an explicit Close frame. Dropping the
//! handle leaves the client blocked on a read timeout, which is a different
//! failure mode than a disconnect and will silently invalidate exactly the
//! reconnection-logic tests this surface exists to support.

use testbed_core::ConnId;

/// Placeholder so the crate has a compiled surface; replaced in Phase 5.
pub fn new_connection_id() -> ConnId {
    ConnId::new()
}
