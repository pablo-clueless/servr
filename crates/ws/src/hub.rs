//! The topic hub: who is subscribed to what, and how a frame reaches them.
//!
//! # Why the hub owns a channel and not the socket
//!
//! `/_admin/ws/publish` runs on an HTTP task; the socket is owned by the
//! connection task. Handing the hub an `mpsc::Sender` per member keeps that
//! boundary one-way — the publisher never touches a socket, and a connection
//! that has gone away is discovered when its channel closes rather than by the
//! hub tracking liveness itself.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use testbed_core::{Clock, ConnId, Dir, Event, EventKind, EventSink, RunId, SpanId, TraceId};
use tokio::sync::mpsc;

/// The trace context of a connection span, as a frame span links back to it.
pub type ConnTrace = Option<(TraceId, SpanId)>;

/// What the hub can push at a connection.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(String),
    /// A server-initiated close, carrying the reason the client will see.
    ///
    /// # Trap T6
    ///
    /// This is its own variant rather than "drop the channel" precisely
    /// because dropping is the wrong thing. A dropped handle leaves the client
    /// blocked on a read until *its own* timeout fires, which is a different
    /// failure mode than a disconnect — and it silently invalidates exactly the
    /// reconnection-logic tests this surface exists to support. The connection
    /// task turns this into an explicit Close frame.
    Close(String),
}

/// One subscriber, from the hub's side.
struct Member {
    tx: mpsc::UnboundedSender<Outbound>,
    /// The connection span's context. A frame span *links* here rather than
    /// descending from it — see [`testbed_telemetry::link`].
    conn_trace: ConnTrace,
}

/// Topics, their members, and the fan-out between them.
pub struct Hub {
    /// A `Mutex`, not an `ArcSwap`: unlike the control-plane config this is
    /// written on every join and leave and read only by the fan-out, so the
    /// read-mostly tradeoff does not apply. No critical section here awaits.
    topics: Mutex<HashMap<String, HashMap<ConnId, Member>>>,
    bus: Arc<dyn EventSink>,
    clock: Arc<Clock>,
    run: RunId,
}

impl Hub {
    pub fn new(bus: Arc<dyn EventSink>, clock: Arc<Clock>, run: RunId) -> Self {
        Self {
            topics: Mutex::new(HashMap::new()),
            bus,
            clock,
            run,
        }
    }

    /// Registers `conn` on `topic` and returns the channel the connection task
    /// reads from.
    pub fn join(&self, topic: &str, conn: ConnId, conn_trace: ConnTrace) -> Subscription {
        let (tx, rx) = mpsc::unbounded_channel();
        self.topics
            .lock()
            .expect("hub lock poisoned")
            .entry(topic.to_string())
            .or_default()
            .insert(conn, Member { tx, conn_trace });
        rx
    }

    /// Removes `conn`, and the topic itself once it is empty — presence must
    /// not accumulate topics nobody is in.
    pub fn leave(&self, topic: &str, conn: ConnId) {
        let mut topics = self.topics.lock().expect("hub lock poisoned");
        if let Some(members) = topics.get_mut(topic) {
            members.remove(&conn);
            if members.is_empty() {
                topics.remove(topic);
            }
        }
    }

    /// Fans `body` out to every member of `topic`, returning how many took it.
    ///
    /// `from` excludes the sender when the frame arrived on a connection, so a
    /// client does not receive its own message back. `None` for an
    /// admin-injected publish, which goes to everyone.
    pub fn publish(&self, topic: &str, body: &str, from: Option<ConnId>) -> usize {
        let targets: Vec<(ConnId, mpsc::UnboundedSender<Outbound>, ConnTrace)> = {
            let topics = self.topics.lock().expect("hub lock poisoned");
            match topics.get(topic) {
                Some(members) => members
                    .iter()
                    .filter(|(id, _)| Some(**id) != from)
                    .map(|(id, m)| (*id, m.tx.clone(), m.conn_trace))
                    .collect(),
                None => return 0,
            }
        };

        let mut delivered = 0;
        for (conn, tx, conn_trace) in targets {
            // A closed channel means the connection task has already exited and
            // `leave` has not caught up yet. Not an error worth reporting.
            if tx.send(Outbound::Text(body.to_string())).is_err() {
                continue;
            }
            delivered += 1;
            self.frame_span(topic, conn, Dir::Out, body.len(), conn_trace);
        }
        delivered
    }

    /// Server-initiated disconnect of every member of `topic`, returning how
    /// many were closed.
    ///
    /// Members are removed here rather than left for each connection task to
    /// call [`Hub::leave`], so presence is accurate the moment this returns
    /// instead of a scheduling hop later.
    pub fn kill(&self, topic: &str, reason: &str) -> usize {
        let members = {
            let mut topics = self.topics.lock().expect("hub lock poisoned");
            match topics.remove(topic) {
                Some(members) => members,
                None => return 0,
            }
        };

        members
            .into_values()
            // T6: an explicit Close frame, which the connection task sends
            // before it lets the socket go.
            .filter(|m| m.tx.send(Outbound::Close(reason.to_string())).is_ok())
            .count()
    }

    /// Presence: every topic with at least one member.
    ///
    /// `BTreeMap` and a sorted member list so the JSON `/_admin/ws` serves is
    /// stable, and two reads of an unchanged hub compare equal.
    pub fn presence(&self) -> BTreeMap<String, Vec<ConnId>> {
        self.topics
            .lock()
            .expect("hub lock poisoned")
            .iter()
            .map(|(topic, members)| {
                let mut conns: Vec<ConnId> = members.keys().copied().collect();
                conns.sort_by_key(|c| c.0);
                (topic.clone(), conns)
            })
            .collect()
    }

    /// Live connections across every topic.
    pub fn connections(&self) -> usize {
        self.topics
            .lock()
            .expect("hub lock poisoned")
            .values()
            .map(HashMap::len)
            .sum()
    }

    /// Invariant 4: a frame is both a bus event and a span.
    ///
    /// The span is a trace *root* linked back to the connection span, never a
    /// child of it — a connection held open for the length of a test suite
    /// would otherwise produce a trace that long. See [`testbed_telemetry::link`].
    pub(crate) fn frame_span(
        &self,
        topic: &str,
        conn: ConnId,
        dir: Dir,
        bytes: usize,
        conn_trace: ConnTrace,
    ) {
        let direction = match dir {
            Dir::In => "in",
            Dir::Out => "out",
        };
        let span = tracing::info_span!(
            parent: None,
            "ws.frame",
            otel.name = %format!("ws frame {direction} {topic}"),
            testbed.ws.topic = %topic,
            testbed.ws.conn = %conn,
            testbed.ws.dir = direction,
            testbed.ws.bytes = bytes,
        );
        testbed_telemetry::link::follows_from_opt(&span, conn_trace);

        // Entered only to stamp the event with this span's own ids, so the
        // frame on `/_admin/events` joins to the frame at the collector
        // (invariant 9). Nothing awaits inside, so the guard is safe here.
        let _entered = span.enter();
        self.emit(EventKind::WsFrame {
            topic: topic.to_string(),
            conn,
            dir,
            bytes,
        });
    }

    pub(crate) fn emit(&self, kind: EventKind) {
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
            kind,
        });
    }
}

/// What a connection task reads to learn what the hub wants sent.
pub type Subscription = mpsc::UnboundedReceiver<Outbound>;

#[cfg(test)]
mod tests {
    use testbed_core::BroadcastBus;

    use super::*;

    fn hub() -> Hub {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
        Hub::new(bus, clock, run)
    }

    #[tokio::test]
    async fn publish_reaches_every_member_of_the_topic() {
        let hub = hub();
        let (a, b) = (ConnId::new(), ConnId::new());
        let mut rx_a = hub.join("demo", a, None);
        let mut rx_b = hub.join("demo", b, None);

        assert_eq!(hub.publish("demo", "hi", None), 2);

        for rx in [&mut rx_a, &mut rx_b] {
            match rx.try_recv().expect("member received nothing") {
                Outbound::Text(body) => assert_eq!(body, "hi"),
                other => panic!("expected text, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_frame_from_a_connection_does_not_echo_back_to_its_sender() {
        let hub = hub();
        let (a, b) = (ConnId::new(), ConnId::new());
        let mut rx_a = hub.join("demo", a, None);
        let mut rx_b = hub.join("demo", b, None);

        assert_eq!(hub.publish("demo", "from a", Some(a)), 1);

        assert!(rx_a.try_recv().is_err(), "sender received its own frame");
        assert!(rx_b.try_recv().is_ok());
    }

    #[tokio::test]
    async fn publishing_to_an_unknown_topic_delivers_nothing() {
        let hub = hub();
        hub.join("demo", ConnId::new(), None);
        assert_eq!(hub.publish("other", "hi", None), 0);
    }

    /// T6: the hub asks for a Close, it does not simply drop the channel.
    #[tokio::test]
    async fn kill_queues_an_explicit_close_and_clears_presence() {
        let hub = hub();
        let mut rx = hub.join("demo", ConnId::new(), None);

        assert_eq!(hub.kill("demo", "server closed"), 1);

        match rx.try_recv().expect("no close was queued") {
            Outbound::Close(reason) => assert_eq!(reason, "server closed"),
            other => panic!("expected a close frame, got {other:?}"),
        }
        assert!(hub.presence().is_empty(), "kill left presence behind");
    }

    #[tokio::test]
    async fn presence_drops_a_topic_once_its_last_member_leaves() {
        let hub = hub();
        let (a, b) = (ConnId::new(), ConnId::new());
        hub.join("demo", a, None);
        hub.join("demo", b, None);
        hub.join("other", a, None);

        assert_eq!(hub.presence()["demo"].len(), 2);
        assert_eq!(hub.connections(), 3);

        hub.leave("demo", a);
        hub.leave("demo", b);

        let presence = hub.presence();
        assert!(!presence.contains_key("demo"), "empty topic still listed");
        assert_eq!(presence.len(), 1);
    }

    #[tokio::test]
    async fn a_departed_connection_is_not_counted_as_delivered() {
        let hub = hub();
        let conn = ConnId::new();
        let rx = hub.join("demo", conn, None);
        drop(rx);

        assert_eq!(hub.publish("demo", "hi", None), 0);
    }

    #[tokio::test]
    async fn every_delivered_frame_lands_on_the_bus() {
        use futures_util::StreamExt;

        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
        let hub = Hub::new(Arc::clone(&bus) as Arc<dyn EventSink>, clock, run);

        let mut events = bus.subscribe();
        let conn = ConnId::new();
        // Held: dropping the receiver closes the channel, and the frame is then
        // counted as undeliverable rather than emitted.
        let _rx = hub.join("demo", conn, None);
        assert_eq!(hub.publish("demo", "hello", None), 1);

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
            .await
            .expect("no event reached the bus")
            .expect("bus closed");
        match event.kind {
            EventKind::WsFrame {
                topic,
                dir,
                bytes,
                conn: on,
            } => {
                assert_eq!(topic, "demo");
                assert_eq!(dir, Dir::Out);
                assert_eq!(bytes, 5);
                assert_eq!(on, conn);
            }
            other => panic!("expected a WsFrame, got {other:?}"),
        }
    }
}
