//! `POST /hooks/in/{id}` — the capture inbox.
//!
//! Anything posted here is recorded and served back at `/_admin/hooks/in/{id}`,
//! so a test can assert on what its system under test actually sent: headers,
//! body, and the virtual time it arrived.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use testbed_core::{Clock, Event, EventKind, EventSink, RunId};

use crate::sign;

/// Captures kept per endpoint before the oldest are dropped.
///
/// Bounded because `webhook-storm` is a planned scenario (HANDOFF §9 task 16)
/// and an unbounded inbox turns it into an out-of-memory test of the machine
/// rather than of the system under test.
pub const CAPACITY: usize = 500;

/// One received request.
#[derive(Debug, Clone, Serialize)]
pub struct Capture {
    /// Virtual time. After a `clock/advance` this legitimately sits ahead of
    /// wall time — that is the point of it.
    pub at: DateTime<Utc>,
    /// Lowercased header names to values. A `HeaderMap` would not serialize,
    /// and this is read as JSON.
    pub headers: BTreeMap<String, String>,
    /// Parsed when the body is JSON, so the gate's `.body.x` works; otherwise
    /// the raw text as a JSON string.
    pub body: Value,
    pub body_sha256: String,
}

/// Every endpoint's captures.
pub struct Inbox {
    endpoints: Mutex<HashMap<String, Vec<Capture>>>,
    bus: Arc<dyn EventSink>,
    clock: Arc<Clock>,
    run: RunId,
}

impl Inbox {
    pub fn new(bus: Arc<dyn EventSink>, clock: Arc<Clock>, run: RunId) -> Self {
        Self {
            endpoints: Mutex::new(HashMap::new()),
            bus,
            clock,
            run,
        }
    }

    /// Records a request and returns the capture.
    pub fn record(&self, endpoint: &str, headers: &HeaderMap, raw: &[u8]) -> Capture {
        let headers: BTreeMap<String, String> = headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or("<non-utf8>").to_string(),
                )
            })
            .collect();

        let capture = Capture {
            at: self.clock.now(),
            headers,
            body: parse_body(raw),
            body_sha256: sign::body_sha256(raw),
        };

        {
            let mut endpoints = self.endpoints.lock().expect("inbox lock poisoned");
            let captures = endpoints.entry(endpoint.to_string()).or_default();
            captures.push(capture.clone());
            // Oldest first out. A storm should leave the *most recent* traffic
            // readable, which is what anyone debugging one wants.
            if captures.len() > CAPACITY {
                let overflow = captures.len() - CAPACITY;
                captures.drain(..overflow);
            }
        }

        capture
    }

    /// Everything captured for `endpoint`, oldest first.
    pub fn captures(&self, endpoint: &str) -> Vec<Capture> {
        self.endpoints
            .lock()
            .expect("inbox lock poisoned")
            .get(endpoint)
            .cloned()
            .unwrap_or_default()
    }

    /// Endpoint names that have received something, with their counts.
    pub fn summary(&self) -> BTreeMap<String, usize> {
        self.endpoints
            .lock()
            .expect("inbox lock poisoned")
            .iter()
            .map(|(name, captures)| (name.clone(), captures.len()))
            .collect()
    }

    pub fn clear(&self, endpoint: Option<&str>) {
        let mut endpoints = self.endpoints.lock().expect("inbox lock poisoned");
        match endpoint {
            Some(name) => {
                endpoints.remove(name);
            }
            None => endpoints.clear(),
        }
    }

    fn emit(&self, kind: EventKind) {
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

/// Non-JSON bodies are kept as a JSON string rather than rejected: a webhook
/// sender under test may legitimately post form data or XML, and a capture
/// inbox that 400s on it cannot be used to find out that it did.
fn parse_body(raw: &[u8]) -> Value {
    match serde_json::from_slice(raw) {
        Ok(value) => value,
        Err(_) => Value::String(String::from_utf8_lossy(raw).into_owned()),
    }
}

/// `POST /hooks/in/{id}` (T1: axum 0.8 spells the param `{id}`).
///
/// Invariant 4: the capture is a bus event and a span. The inbound trace
/// context is continued, not replaced, so a delivery the testbed sent to itself
/// shows as one trace across both halves.
pub async fn capture(
    State(inbox): State<Arc<Inbox>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Json<Value> {
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::info_span!(
        "webhook.in",
        otel.name = %format!("webhook in {id}"),
        testbed.webhook.endpoint = %id,
        testbed.webhook.bytes = body.len(),
    );
    if span
        .set_parent(testbed_telemetry::propagation::extract(&headers))
        .is_err()
    {
        tracing::trace!("no inbound trace context on the webhook; starting a new trace");
    }

    let _entered = span.enter();
    let capture = inbox.record(&id, &headers, &body);

    inbox.emit(EventKind::WebhookIn {
        endpoint: id.clone(),
        headers: capture.headers.clone(),
        body_sha256: capture.body_sha256.clone(),
    });
    tracing::info!(endpoint = %id, bytes = body.len(), "webhook captured");

    Json(json!({ "ok": true, "endpoint": id, "sha256": capture.body_sha256 }))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use testbed_core::BroadcastBus;

    use super::*;

    fn inbox() -> Inbox {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
        Inbox::new(bus, clock, run)
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    /// The gate reads `.[0].body.x`, so the body has to come back parsed.
    #[test]
    fn the_gate_body_comes_back_as_json() {
        let inbox = inbox();
        inbox.record("abc", &HeaderMap::new(), br#"{"x":1}"#);

        let captures = inbox.captures("abc");
        assert_eq!(captures.len(), 1);
        assert_eq!(captures[0].body["x"], 1);
    }

    #[test]
    fn headers_are_captured_lowercased() {
        let inbox = inbox();
        inbox.record("abc", &headers(&[("X-Custom", "yes")]), b"{}");

        assert_eq!(
            inbox.captures("abc")[0].headers.get("x-custom"),
            Some(&"yes".to_string())
        );
    }

    #[test]
    fn a_non_json_body_is_kept_rather_than_rejected() {
        let inbox = inbox();
        inbox.record("abc", &HeaderMap::new(), b"a=1&b=2");
        assert_eq!(inbox.captures("abc")[0].body, "a=1&b=2");
    }

    #[test]
    fn endpoints_do_not_see_each_others_captures() {
        let inbox = inbox();
        inbox.record("abc", &HeaderMap::new(), b"{}");
        inbox.record("def", &HeaderMap::new(), b"{}");
        inbox.record("def", &HeaderMap::new(), b"{}");

        assert_eq!(inbox.captures("abc").len(), 1);
        assert_eq!(inbox.captures("def").len(), 2);
        assert!(inbox.captures("never-used").is_empty());
    }

    #[test]
    fn the_body_hash_is_recorded_for_each_capture() {
        let inbox = inbox();
        inbox.record("abc", &HeaderMap::new(), br#"{"x":1}"#);
        assert_eq!(
            inbox.captures("abc")[0].body_sha256,
            sign::body_sha256(br#"{"x":1}"#)
        );
    }

    /// A storm must not grow without bound, and must leave the newest traffic
    /// readable rather than the oldest.
    #[test]
    fn the_inbox_is_bounded_and_drops_the_oldest() {
        let inbox = inbox();
        for i in 0..CAPACITY + 10 {
            inbox.record(
                "abc",
                &HeaderMap::new(),
                format!(r#"{{"i":{i}}}"#).as_bytes(),
            );
        }

        let captures = inbox.captures("abc");
        assert_eq!(captures.len(), CAPACITY);
        assert_eq!(
            captures[0].body["i"], 10,
            "the newest captures were dropped instead of the oldest"
        );
        assert_eq!(captures.last().unwrap().body["i"], CAPACITY + 9);
    }

    #[test]
    fn clearing_one_endpoint_leaves_the_others() {
        let inbox = inbox();
        inbox.record("abc", &HeaderMap::new(), b"{}");
        inbox.record("def", &HeaderMap::new(), b"{}");

        inbox.clear(Some("abc"));
        assert!(inbox.captures("abc").is_empty());
        assert_eq!(inbox.captures("def").len(), 1);

        inbox.clear(None);
        assert!(inbox.summary().is_empty());
    }

    #[test]
    fn captures_are_stamped_from_the_virtual_clock() {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
        let inbox = Inbox::new(bus, Arc::clone(&clock), run);

        clock.advance(std::time::Duration::from_secs(3600));
        inbox.record("abc", &HeaderMap::new(), b"{}");

        let at = inbox.captures("abc")[0].at;
        assert!(
            at > Clock::wall_now() + chrono::TimeDelta::minutes(50),
            "the capture was stamped from wall time, not the virtual clock"
        );
    }
}
