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
    fn signing_scheme_matches_the_admin_api_spelling() {
        let endpoint: WebhookEndpoint =
            serde_json::from_str(r#"{"name":"a","url":"http://x","sign":"stripe"}"#).unwrap();
        assert_eq!(endpoint.sign, SigningScheme::Stripe);
    }
}
