//! W3C trace identifiers, as carried on every bus event.
//!
//! These are defined here rather than reused from `opentelemetry` for one
//! reason: bus events are serialized to JSON on `/_admin/events`, and otel's
//! id types are not `Serialize`. The wire form is lowercase hex, identical to
//! `traceparent` and to Jaeger's `traceID`, so the Phase 2b gate's comparison
//! between an SSE event and the collector is a plain string equality.
//!
//! `crates/telemetry` provides the conversions to and from the otel types.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

macro_rules! hex_id {
    ($name:ident, $len:literal, $label:literal) => {
        #[doc = concat!("A W3C ", $label, ": ", stringify!($len), " bytes, rendered as lowercase hex.")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            pub const INVALID: Self = Self([0; $len]);

            pub fn from_bytes(bytes: [u8; $len]) -> Self {
                Self(bytes)
            }

            pub fn to_bytes(self) -> [u8; $len] {
                self.0
            }

            /// All-zero ids are reserved as "no id" by the W3C spec.
            pub fn is_valid(&self) -> bool {
                self.0 != [0; $len]
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl FromStr for $name {
            type Err = TraceIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                if s.len() != $len * 2 {
                    return Err(TraceIdError::Length {
                        expected: $len * 2,
                        got: s.len(),
                    });
                }
                let mut bytes = [0u8; $len];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                        .map_err(|_| TraceIdError::NotHex)?;
                }
                Ok(Self(bytes))
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(d)?;
                raw.parse().map_err(de::Error::custom)
            }
        }
    };
}

hex_id!(TraceId, 16, "trace id");
hex_id!(SpanId, 8, "span id");

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TraceIdError {
    #[error("expected {expected} hex characters, got {got}")]
    Length { expected: usize, got: usize },
    #[error("value is not lowercase hex")]
    NotHex,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The id from the HANDOFF §7 Phase 2b gate.
    const GATE_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const GATE_SPAN: &str = "00f067aa0ba902b7";

    #[test]
    fn renders_w3c_lowercase_hex() {
        assert_eq!(
            GATE_TRACE.parse::<TraceId>().unwrap().to_string(),
            GATE_TRACE
        );
        assert_eq!(GATE_SPAN.parse::<SpanId>().unwrap().to_string(), GATE_SPAN);
    }

    #[test]
    fn serializes_as_a_bare_json_string() {
        let id: TraceId = GATE_TRACE.parse().unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{GATE_TRACE}\""));
        assert_eq!(serde_json::from_str::<TraceId>(&json).unwrap(), id);
    }

    #[test]
    fn rejects_wrong_length_and_non_hex() {
        assert!(matches!(
            "abcd".parse::<TraceId>(),
            Err(TraceIdError::Length { .. })
        ));
        assert_eq!(
            "zz".repeat(16).parse::<TraceId>(),
            Err(TraceIdError::NotHex)
        );
    }

    #[test]
    fn all_zero_ids_are_invalid() {
        assert!(!TraceId::INVALID.is_valid());
        assert!(GATE_TRACE.parse::<TraceId>().unwrap().is_valid());
    }
}
