//! Fault specifications.
//!
//! These types are *declarative on purpose*. Faults are applied by a `tower`
//! layer in `crates/http`, never by per-handler logic (HANDOFF §5 invariant 8);
//! a handler that reads a `FaultSpec` itself is a bug. Telemetry faults are
//! applied in the exporter shim, never at instrumentation (invariant 11) —
//! corrupting spans where they are created poisons the testbed's own
//! debuggability.

use serde::{Deserialize, Serialize};

/// One fault rule, matched against a request route.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FaultSpec {
    /// Route glob, e.g. `/api/*`. Matched against the request path.
    pub route: String,
    /// Probability in `0.0..=1.0` that this rule fires on a matching request.
    pub rate: f64,
    /// Delay injected before the response. Real sleep — a client measuring
    /// latency must observe it, so this is one of the few places virtual time
    /// deliberately does not apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Uniform jitter added to `latency_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter_ms: Option<u64>,
    /// Status to return instead of the handler's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// Cut the response body off at this many bytes, without adjusting
    /// `content-length` — the point is to break the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncate_body_at: Option<usize>,
    /// Drop the connection mid-response.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub drop_connection: bool,
}

impl FaultSpec {
    /// Whether this rule applies to `path`. `*` matches any run of characters,
    /// so `/api/*` covers `/api/ping` and `/api/items/1` alike.
    pub fn matches(&self, path: &str) -> bool {
        glob_match(&self.route, path)
    }

    /// Total delay to inject, jitter included. Returns `None` when the rule
    /// specifies no latency.
    ///
    /// `roll` is a caller-supplied value in `0.0..1.0` — passed in rather than
    /// drawn here so the jitter distribution stays testable.
    pub fn delay_ms(&self, roll: f64) -> Option<u64> {
        let base = self.latency_ms?;
        let jitter = self.jitter_ms.map_or(0.0, |j| roll * j as f64);
        Some(base + jitter as u64)
    }

    /// Short names of the effects this rule carries, for `EventKind::HttpRequest`
    /// and the `testbed.faults` span field.
    pub fn effects(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.latency_ms.is_some() {
            names.push("latency");
        }
        if self.status.is_some() {
            names.push("status");
        }
        if self.truncate_body_at.is_some() {
            names.push("truncate");
        }
        if self.drop_connection {
            names.push("drop");
        }
        names
    }
}

/// Wildcard match where `*` stands for any run of characters, including none.
///
/// Deliberately not a real glob crate: routes here are short and the semantics
/// need to be obvious to whoever writes a scenario file, not maximally capable.
fn glob_match(pattern: &str, text: &str) -> bool {
    let (pattern, text) = (pattern.as_bytes(), text.as_bytes());
    let (mut p, mut t) = (0, 0);
    // Where to resume if the current `*` turns out to have consumed too little.
    let (mut star, mut resume) = (None, 0);

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'*') {
            star = Some(p);
            resume = t;
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if let Some(s) = star {
            // Backtrack: let the star swallow one more character.
            p = s + 1;
            resume += 1;
            t = resume;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

impl Default for FaultSpec {
    fn default() -> Self {
        Self {
            route: "/*".to_string(),
            rate: 0.0,
            latency_ms: None,
            jitter_ms: None,
            status: None,
            truncate_body_at: None,
            drop_connection: false,
        }
    }
}

/// Deliberate corruption of the telemetry the testbed emits.
///
/// This is the payload for "test dev tools". A well-behaved telemetry source is
/// easy and every observability tool works against one; what nobody can test
/// against is a source that emits *plausibly broken* telemetry on demand.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetryFault {
    /// Probability in `0.0..=1.0` that corruption is applied to a given export.
    #[serde(default)]
    pub rate: f64,
    /// Emit spans referencing a parent that appears nowhere in the trace.
    #[serde(default)]
    pub orphan_spans: bool,
    /// Shift exported span timestamps. Positive values land them in the future.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_skew_ms: Option<i64>,
    /// Unique label values per metric emission. Will genuinely degrade
    /// Prometheus — that is the test, and it is why the obs stack sits behind a
    /// compose profile. Document the blast radius in the scenario file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality_bomb: Option<u32>,
    /// Pad span attributes to this many bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribute_bloat_bytes: Option<usize>,
    /// Silently discard the export batch.
    #[serde(default)]
    pub drop_export: bool,
    /// Stall the exporter, to exercise a collector's backpressure handling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export_latency_ms: Option<u64>,
    /// Mangle inbound `traceparent` headers before extraction, so a client
    /// sending valid context sees its trace broken anyway.
    #[serde(default)]
    pub corrupt_inbound_traceparent: bool,
    /// Reset monotonic counters, which should make a backend infer a restart.
    #[serde(default)]
    pub counter_reset: bool,
}

/// # Q4 — resolved
///
/// Operator decision: support both signing schemes, selected per endpoint in
/// scenario config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SigningScheme {
    /// `Stripe-Signature: t=<unix>,v1=<hmac-sha256 of "t.body">`.
    #[default]
    Stripe,
    /// `X-Hub-Signature-256: sha256=<hmac-sha256 of body>`.
    Github,
    /// Send unsigned.
    None,
}

/// An outbound webhook destination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub sign: SigningScheme,
    #[serde(default)]
    pub secret: Option<String>,
    /// Retry backoff in virtual milliseconds, one entry per retry. Retries fire
    /// at virtual times matching these offsets; the Phase 7 gate asserts it.
    #[serde(default)]
    pub backoff_ms: Vec<u64>,
    /// Fail the first N attempts regardless of the receiver, to exercise retry
    /// logic deterministically.
    #[serde(default)]
    pub fail_first: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fault_needs_only_a_route_and_a_rate() {
        let spec: FaultSpec = serde_json::from_str(r#"{"route":"/api/*","rate":1.0}"#).unwrap();
        assert_eq!(spec.route, "/api/*");
        assert!(!spec.drop_connection);
        assert_eq!(spec.status, None);
    }

    #[test]
    fn the_phase_2_gate_payload_parses() {
        let body = r#"{"route":"/api/*","rate":1.0,"latency_ms":500,"status":503}"#;
        let spec: FaultSpec = serde_json::from_str(body).unwrap();
        assert_eq!(spec.latency_ms, Some(500));
        assert_eq!(spec.status, Some(503));
    }

    #[test]
    fn unset_telemetry_faults_are_omitted_from_the_wire() {
        let json = serde_json::to_value(TelemetryFault::default()).unwrap();
        assert!(json.get("clock_skew_ms").is_none());
        assert_eq!(json["rate"], 0.0);
    }

    #[test]
    fn route_globs_match_the_way_a_scenario_author_expects() {
        let spec = |route: &str| FaultSpec {
            route: route.into(),
            ..Default::default()
        };

        assert!(spec("/api/*").matches("/api/ping"));
        assert!(spec("/api/*").matches("/api/items/1"));
        assert!(spec("/api/*").matches("/api/"));
        assert!(!spec("/api/*").matches("/_admin/health"));

        assert!(spec("/*").matches("/anything/at/all"));
        assert!(spec("*").matches("/_admin/reset"));

        assert!(spec("/api/ping").matches("/api/ping"));
        assert!(!spec("/api/ping").matches("/api/pin"));
        assert!(!spec("/api/ping").matches("/api/pingg"));

        // A star in the middle, and one that has to backtrack.
        assert!(spec("/api/*/items").matches("/api/v1/items"));
        assert!(spec("/api/*items").matches("/api/v1/items"));
        assert!(!spec("/api/*/items").matches("/api/v1/items/1"));
    }

    #[test]
    fn delay_includes_jitter_but_never_without_a_base_latency() {
        let jittery = FaultSpec {
            latency_ms: Some(500),
            jitter_ms: Some(100),
            ..Default::default()
        };
        assert_eq!(jittery.delay_ms(0.0), Some(500));
        assert_eq!(jittery.delay_ms(0.5), Some(550));
        assert_eq!(jittery.delay_ms(0.99), Some(599));

        let jitter_only = FaultSpec {
            jitter_ms: Some(100),
            ..Default::default()
        };
        assert_eq!(jitter_only.delay_ms(0.9), None);
    }

    #[test]
    fn effects_name_every_configured_behaviour() {
        let spec = FaultSpec {
            latency_ms: Some(500),
            status: Some(503),
            drop_connection: true,
            ..Default::default()
        };
        assert_eq!(spec.effects(), vec!["latency", "status", "drop"]);
        assert!(FaultSpec::default().effects().is_empty());
    }

    #[test]
    fn signing_scheme_matches_the_admin_api_spelling() {
        let endpoint: WebhookEndpoint =
            serde_json::from_str(r#"{"name":"a","url":"http://x","sign":"stripe"}"#).unwrap();
        assert_eq!(endpoint.sign, SigningScheme::Stripe);
    }
}
