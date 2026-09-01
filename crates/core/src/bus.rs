//! The event bus.
//!
//! # Q1 — resolved
//!
//! Operator decision: define [`EventSink`] as a trait, ship the in-process
//! [`BroadcastBus`] behind it, and leave a Redis pub/sub implementation for a
//! future `distributed` feature. The testbed is single-replica for now; the
//! trait is the escape hatch so that stops being true without touching every
//! call site.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_core::stream::BoxStream;
use futures_util::stream;
use tokio::sync::broadcast;

use crate::clock::Clock;
use crate::event::{Event, EventKind};
use crate::run::RunId;

/// Where domain events go. One process-wide instance, held by the control-plane
/// state.
///
/// Implementations must handle a lagging subscriber by emitting
/// [`EventKind::Gap`] rather than dropping silently — see trap T4.
pub trait EventSink: Send + Sync + 'static {
    /// Non-blocking. A full or lagging channel must never stall the caller:
    /// this is called from request handlers on the hot path.
    ///
    /// The sink assigns [`Event::id`]; whatever the caller put there is
    /// overwritten, so ids are dense and monotonic per process.
    fn emit(&self, event: Event);

    /// A live tail of the bus. Events emitted before subscribing are not
    /// replayed; `/_admin/events` is a tail, not a log query.
    fn subscribe(&self) -> BoxStream<'static, Event>;

    /// Total events dropped for lagging subscribers since boot. Exported as
    /// `testbed_events_dropped_total`, which is how you notice the event log is
    /// lying to you (HANDOFF §7 phase 2b).
    fn dropped(&self) -> u64;
}

/// In-process fan-out over `tokio::sync::broadcast` (Q1).
///
/// # Trap T4
///
/// `broadcast` drops for slow receivers and reports it as
/// `RecvError::Lagged(n)`. Swallowing that yields a silently truncated event
/// log, which is *worse* than no event log: the UI still looks correct, and
/// every conclusion drawn from it is wrong. Every lag is converted into an
/// [`EventKind::Gap`] on the subscriber's stream and added to [`Self::dropped`].
pub struct BroadcastBus {
    tx: broadcast::Sender<Event>,
    clock: Arc<Clock>,
    run: RunId,
    seq: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

impl BroadcastBus {
    /// `capacity` is the per-subscriber backlog. A subscriber that falls more
    /// than this many events behind lags, and lagging is reported, never hidden.
    pub fn new(capacity: usize, clock: Arc<Clock>, run: RunId) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            clock,
            run,
            seq: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Builds a stamped [`Event`] and emits it. The convenient path: it stamps
    /// virtual and wall time from the bus's own clock, so callers cannot
    /// accidentally stamp an event from wall time.
    ///
    /// Trace context is attached by the caller, which is the only place that
    /// knows the active span — see [`Event::with_trace`].
    pub fn publish(&self, kind: EventKind) -> Event {
        let event = Event {
            id: 0, // assigned by `emit`
            run: self.run,
            at: self.clock.now(),
            wall_at: Clock::wall_now(),
            trace_id: None,
            span_id: None,
            kind,
        };
        self.emit(event.clone());
        event
    }

    /// Number of live subscribers. Backs `testbed_event_subscribers`.
    pub fn subscribers(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// Builds the [`EventKind::Gap`] a lagging subscriber receives in place of the
/// events it missed. A free function because [`EventSink::subscribe`] hands it
/// to a `'static` stream that cannot borrow the bus.
fn gap_event(run: RunId, clock: &Clock, id: u64, dropped: u64) -> Event {
    Event {
        id,
        run,
        at: clock.now(),
        wall_at: Clock::wall_now(),
        trace_id: None,
        span_id: None,
        kind: EventKind::Gap { dropped },
    }
}

impl EventSink for BroadcastBus {
    fn emit(&self, mut event: Event) {
        event.id = self.seq.fetch_add(1, Ordering::SeqCst);
        // `send` errors only when there are no subscribers, which is the normal
        // state of a testbed nobody is watching. Not a failure.
        let _ = self.tx.send(event);
    }

    fn subscribe(&self) -> BoxStream<'static, Event> {
        let rx = self.tx.subscribe();
        let clock = Arc::clone(&self.clock);
        let seq = Arc::clone(&self.seq);
        let dropped = Arc::clone(&self.dropped);
        let run = self.run;

        Box::pin(stream::unfold(rx, move |mut rx| {
            let clock = Arc::clone(&clock);
            let seq = Arc::clone(&seq);
            let dropped = Arc::clone(&dropped);
            async move {
                match rx.recv().await {
                    Ok(event) => Some((event, rx)),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        dropped.fetch_add(n, Ordering::SeqCst);
                        let id = seq.load(Ordering::SeqCst);
                        Some((gap_event(run, &clock, id, n), rx))
                    }
                    Err(broadcast::error::RecvError::Closed) => None,
                }
            }
        }))
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for BroadcastBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastBus")
            .field("run", &self.run)
            .field("emitted", &self.seq.load(Ordering::SeqCst))
            .field("dropped", &self.dropped.load(Ordering::SeqCst))
            .field("subscribers", &self.tx.receiver_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::StreamExt;

    use super::*;
    use crate::event::StreamId;

    fn bus(capacity: usize) -> BroadcastBus {
        BroadcastBus::new(capacity, Arc::new(Clock::new()), RunId::new())
    }

    fn chunk(seq: u32) -> EventKind {
        EventKind::StreamChunk {
            stream: StreamId::new(),
            seq,
        }
    }

    /// Drains whatever the stream can yield right now, stopping once it would
    /// block. Bounded by a timeout rather than a fixed count so the test does
    /// not encode `broadcast`'s internal bookkeeping.
    async fn drain(mut s: BoxStream<'static, Event>) -> Vec<Event> {
        let mut out = Vec::new();
        while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), s.next()).await
        {
            out.push(event);
        }
        out
    }

    #[tokio::test]
    async fn ids_are_dense_and_monotonic() {
        let bus = bus(64);
        let sub = bus.subscribe();
        for i in 0..10 {
            bus.publish(chunk(i));
        }

        let ids: Vec<u64> = drain(sub).await.iter().map(|e| e.id).collect();
        assert_eq!(ids, (0..10).collect::<Vec<u64>>());
    }

    #[tokio::test]
    async fn emit_overwrites_a_caller_supplied_id() {
        let bus = bus(8);
        let sub = bus.subscribe();

        let mut forged = gap_event(bus.run, &bus.clock, 0, 0);
        forged.id = 9_999;
        bus.emit(forged);

        assert_eq!(drain(sub).await[0].id, 0);
    }

    /// HANDOFF §9 task 5: 1000 events with a deliberately slow subscriber
    /// produce `Gap` events summing to exactly the number dropped.
    #[tokio::test]
    async fn a_lagging_subscriber_accounts_for_every_dropped_event() {
        const SENT: u64 = 1000;
        const CAPACITY: usize = 16;

        let bus = bus(CAPACITY);
        let sub = bus.subscribe();

        // The subscriber is not polled at all while these are emitted, which is
        // the worst case of "slow".
        for i in 0..SENT {
            bus.publish(chunk(i as u32));
        }

        let received = drain(sub).await;

        let gapped: u64 = received
            .iter()
            .filter_map(|e| match e.kind {
                EventKind::Gap { dropped } => Some(dropped),
                _ => None,
            })
            .sum();
        let delivered = received.len() as u64
            - received
                .iter()
                .filter(|e| matches!(e.kind, EventKind::Gap { .. }))
                .count() as u64;

        assert_eq!(
            gapped + delivered,
            SENT,
            "{gapped} gapped + {delivered} delivered accounts for {} of {SENT} events",
            gapped + delivered
        );
        assert_eq!(
            bus.dropped(),
            gapped,
            "counter disagrees with the Gap events"
        );
        assert!(gapped > 0, "test did not actually induce a lag");
    }

    #[tokio::test]
    async fn a_keeping_up_subscriber_sees_no_gaps() {
        let bus = bus(1024);
        let sub = bus.subscribe();
        for i in 0..1000 {
            bus.publish(chunk(i));
        }

        let received = drain(sub).await;
        assert_eq!(received.len(), 1000);
        assert!(!received
            .iter()
            .any(|e| matches!(e.kind, EventKind::Gap { .. })));
        assert_eq!(bus.dropped(), 0);
    }

    #[tokio::test]
    async fn emitting_with_no_subscribers_is_not_an_error() {
        let bus = bus(8);
        for i in 0..100 {
            bus.publish(chunk(i));
        }
        assert_eq!(bus.dropped(), 0, "unwatched events are not 'dropped'");
        assert_eq!(bus.subscribers(), 0);
    }

    #[tokio::test]
    async fn events_are_stamped_from_the_virtual_clock() {
        let clock = Arc::new(Clock::new());
        let bus = BroadcastBus::new(8, Arc::clone(&clock), RunId::new());

        let before = bus.publish(chunk(0));
        clock.advance(Duration::from_secs(3600));
        let after = bus.publish(chunk(1));

        let virtual_gap = after.at - before.at;
        let wall_gap = after.wall_at - before.wall_at;
        assert!(
            virtual_gap.num_seconds() >= 3600,
            "virtual time did not move with the clock: {virtual_gap}"
        );
        assert!(
            wall_gap.num_seconds() < 5,
            "wall time followed the virtual clock: {wall_gap}"
        );
    }
}
