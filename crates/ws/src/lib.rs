//! WebSocket hub: topics, presence, and server-initiated disconnects.
//!
//! # The route
//!
//! `GET /ws?topic=demo` upgrades and subscribes. Frames sent by a client fan
//! out to the rest of the topic; `/_admin/ws/publish` injects from the outside;
//! `/_admin/ws/kill` disconnects a whole topic.
//!
//! # Two spans, linked, not nested
//!
//! A connection span covers the session. Each frame gets its own span, a trace
//! *root* carrying a `FOLLOWS_FROM` link back to the connection (invariant 10).
//! Parenting frames to the connection is the intuitive shape and it is wrong
//! for the same reason it is wrong for queue jobs (trap T10): a connection held
//! open for the length of a test suite produces a trace that long, and every
//! trace-waterfall UI pointed at the testbed becomes unusable.
//!
//! # Trap T6
//!
//! A forced disconnect sends an explicit Close frame. Dropping the handle
//! leaves the client blocked on a read timeout, which is a different failure
//! mode than a disconnect and will silently invalidate exactly the
//! reconnection-logic tests this surface exists to support. See
//! [`hub::Outbound::Close`] and its handling in [`conn`].

pub mod conn;
pub mod hub;

use std::sync::Arc;

use axum::{routing::get, Router};

pub use conn::{Params, CLOSE_CODE};
pub use hub::{Hub, Outbound, Subscription};

/// The data-plane WebSocket route.
///
/// Mounted by `server` behind the fault layer, so a scenario can make the
/// upgrade itself slow or failing — the handshake is a request like any other,
/// and a client's reconnect logic is exactly what benefits from that.
pub fn router(hub: Arc<Hub>) -> Router {
    Router::new()
        .route(ROUTE, get(conn::upgrade))
        .with_state(hub)
}

/// Where [`router`] mounts. Named so the gate and the tests agree with the code.
pub const ROUTE: &str = "/ws";
