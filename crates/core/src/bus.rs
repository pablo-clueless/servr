//! The event bus.
//!
//! # Q1 — resolved
//!
//! Operator decision: define [`EventSink`] as a trait, ship the in-process
//! `tokio::sync::broadcast` implementation behind it, and leave a Redis pub/sub
//! implementation for a future `distributed` feature. The testbed is
//! single-replica for now; the trait is the escape hatch so that stops being
//! true without touching every call site.
//!
//! The broadcast implementation is Phase 1, task 5 — not yet written. Its
//! acceptance criterion: 1000 events with a deliberately slow subscriber
//! produce [`EventKind::Gap`] events summing to exactly the number dropped
//! (trap T4).

use futures_core::stream::BoxStream;

use crate::event::Event;

/// Where domain events go. One process-wide instance, held by the control-plane
/// state.
///
/// Implementations must handle a lagging subscriber by emitting
/// [`crate::EventKind::Gap`] rather than dropping silently — see trap T4.
pub trait EventSink: Send + Sync + 'static {
    /// Non-blocking. A full or lagging channel must never stall the caller:
    /// this is called from request handlers on the hot path.
    fn emit(&self, event: Event);

    /// A live tail of the bus. Events emitted before subscribing are not
    /// replayed; `/_admin/events` is a tail, not a log query.
    fn subscribe(&self) -> BoxStream<'static, Event>;

    /// Total events dropped for lagging subscribers since boot. Exported as
    /// `testbed_events_dropped_total`, which is how you notice the event log is
    /// lying to you (HANDOFF §7 phase 2b).
    fn dropped(&self) -> u64;
}
