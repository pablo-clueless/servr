//! Webhooks, both directions: an inbound capture inbox and an outbound sender.
//!
//! # Q4 — resolved
//!
//! Operator decision: support both signing schemes, selected per endpoint in
//! scenario config. See [`testbed_core::SigningScheme`] and [`sign`].
//!
//! # The two halves meet
//!
//! The Phase 7 gate points the sender at the testbed's own inbox, so one
//! `/_admin/hooks/out` exercises signing, retries, `traceparent` injection and
//! capture in a single command — and because the inbound side *continues* the
//! injected context rather than replacing it, a self-delivery shows up as one
//! trace spanning both halves.
//!
//! Trap T1 applies to both routes: axum 0.8 spells the param `{id}`.

pub mod inbound;
pub mod outbound;
pub mod sign;

use std::sync::Arc;

use axum::{routing::post, Router};
use testbed_core::{Clock, EventSink, RunId};

pub use inbound::{Capture, Inbox};
pub use outbound::{Delivery, DeliveryView, Sender};

/// axum 0.8 param syntax (T1).
pub const INBOUND_ROUTE: &str = "/hooks/in/{id}";

/// Both halves, held together so `server` wires one thing.
pub struct Hooks {
    pub inbox: Arc<Inbox>,
    pub sender: Arc<Sender>,
}

impl Hooks {
    pub fn new(bus: Arc<dyn EventSink>, clock: Arc<Clock>, run: RunId) -> Self {
        Self {
            inbox: Arc::new(Inbox::new(Arc::clone(&bus), Arc::clone(&clock), run)),
            sender: Arc::new(Sender::new(bus, clock, run)),
        }
    }
}

/// The data-plane capture route.
///
/// Mounted behind the fault layer by `server`, deliberately: making the
/// receiver flaky is how you find out whether a sender's retry logic works, and
/// that is most of what this surface is for.
pub fn router(inbox: Arc<Inbox>) -> Router {
    Router::new()
        .route(INBOUND_ROUTE, post(inbound::capture))
        .with_state(inbox)
}
