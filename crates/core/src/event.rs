//! The event bus's payload type.
//!
//! Every domain-significant action emits one of these *and* opens a span
//! (HANDOFF §5 invariant 4). They are two axes, not redundancy: the bus is
//! typed, virtual-clock-stamped, resettable and replayable; the trace tree is
//! wall-clock, sampled and exported. Something appearing in only one of them is
//! a bug.
//!
//! One exception: the OTLP export path emits no events (trap T13). Export emits
//! an event, the event triggers instrumentation, instrumentation queues a span,
//! export runs again — and the batch exporter delays the recursion just long
//! enough to make it non-obvious.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::run::RunId;
use crate::trace::{SpanId, TraceId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Monotonic per-process sequence number. Consumers detect loss by
    /// comparing successive ids, independently of [`EventKind::Gap`].
    pub id: u64,
    pub run: RunId,
    /// Virtual time, from the [`crate::Clock`].
    pub at: DateTime<Utc>,
    /// Real time. Present so the two timelines can be compared after a
    /// `clock/advance`, and for nothing else.
    pub wall_at: DateTime<Utc>,
    /// The join key against the trace tree (HANDOFF §5 invariant 9). Losing it
    /// makes the event stream and the collector uncorrelatable, which is most
    /// of the point of the testbed.
    pub trace_id: Option<TraceId>,
    pub span_id: Option<SpanId>,
    pub kind: EventKind,
}

/// `/_admin/events` is the contract a UI would later consume (HANDOFF §10);
/// this enum is that contract. Tagged representation so consumers can switch on
/// `.kind` without positional decoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EventKind {
    HttpRequest {
        method: String,
        path: String,
        status: u16,
        latency_ms: u64,
        /// Names of the faults that fired on this request, in application order.
        faults: Vec<String>,
    },
    JobTransition {
        job: JobId,
        from: JobState,
        to: JobState,
        attempt: u32,
    },
    MailSent {
        to: String,
        subject: String,
        message_id: String,
    },
    WebhookIn {
        endpoint: String,
        /// Lowercased header names to values. A `HeaderMap` would not
        /// serialize, and this stream is JSON.
        headers: BTreeMap<String, String>,
        body_sha256: String,
    },
    WebhookOut {
        endpoint: String,
        attempt: u32,
        status: Option<u16>,
        /// Virtual time, so a scenario can assert the configured backoff.
        next_retry_at: Option<DateTime<Utc>>,
    },
    WsFrame {
        topic: String,
        conn: ConnId,
        dir: Dir,
        bytes: usize,
    },
    StreamChunk {
        stream: StreamId,
        seq: u32,
    },
    /// Emitted when a subscriber fell behind and the transport dropped events
    /// (trap T4). A silently truncated event log is worse than no event log,
    /// because the UI will look correct. Do not remove this variant.
    Gap {
        dropped: u64,
    },
}

impl EventKind {
    /// Stable discriminant, for metric labels and log filtering.
    pub fn name(&self) -> &'static str {
        match self {
            Self::HttpRequest { .. } => "HttpRequest",
            Self::JobTransition { .. } => "JobTransition",
            Self::MailSent { .. } => "MailSent",
            Self::WebhookIn { .. } => "WebhookIn",
            Self::WebhookOut { .. } => "WebhookOut",
            Self::WsFrame { .. } => "WsFrame",
            Self::StreamChunk { .. } => "StreamChunk",
            Self::Gap { .. } => "Gap",
        }
    }
}

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

uuid_id!(JobId);
uuid_id!(ConnId);
uuid_id!(StreamId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Scheduled,
    Running,
    Succeeded,
    Failed,
    /// Retries exhausted. Terminal.
    Dead,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Dead)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dir {
    In,
    Out,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: EventKind) -> Event {
        Event {
            id: 1,
            run: RunId::new(),
            at: Utc::now(),
            wall_at: Utc::now(),
            trace_id: None,
            span_id: None,
            kind,
        }
    }

    #[test]
    fn serializes_with_a_switchable_kind_tag() {
        let json = serde_json::to_value(event(EventKind::Gap { dropped: 7 })).unwrap();
        assert_eq!(json["kind"]["kind"], "Gap");
        assert_eq!(json["kind"]["dropped"], 7);
    }

    #[test]
    fn trace_ids_land_on_the_wire_as_hex_strings() {
        let mut e = event(EventKind::StreamChunk {
            stream: StreamId::new(),
            seq: 0,
        });
        e.trace_id = Some("4bf92f3577b34da6a3ce929d0e0e4736".parse().unwrap());

        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(json["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn every_kind_round_trips() {
        let kinds = [
            EventKind::HttpRequest {
                method: "GET".into(),
                path: "/api/ping".into(),
                status: 503,
                latency_ms: 500,
                faults: vec!["latency".into(), "status".into()],
            },
            EventKind::JobTransition {
                job: JobId::new(),
                from: JobState::Scheduled,
                to: JobState::Running,
                attempt: 1,
            },
            EventKind::Gap { dropped: 3 },
        ];

        for kind in kinds {
            let name = kind.name();
            let encoded = serde_json::to_string(&event(kind)).unwrap();
            let decoded: Event = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded.kind.name(), name);
        }
    }
}
