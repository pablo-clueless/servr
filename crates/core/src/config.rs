//! Layered behaviour configuration.
//!
//! TOML scenario files seed [`Scenario`] (`base`) at boot; the admin API mutates
//! an [`Overlay`]; `reset` drops the overlay and re-resolves from base
//! (HANDOFF §2 decision 5).
//!
//! `base` is immutable after boot (§5 invariant 2). That is the whole basis of
//! test isolation: if `reset` cannot reconstruct a known-good state from the
//! scenario file alone, every test that ran before this one is now part of this
//! test's input.

use serde::{Deserialize, Serialize};

use crate::fault::{FaultSpec, TelemetryFault, WebhookEndpoint};

/// The immutable half. Parsed once from `scenarios/*.toml`, then never written.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Free-text note on what running this scenario will do to whatever is
    /// pointed at the testbed. Required in spirit for anything destructive —
    /// `cardinality_bomb` in particular.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blast_radius: Option<String>,
    #[serde(default)]
    pub faults: Vec<FaultSpec>,
    #[serde(default)]
    pub telemetry: TelemetryFault,
    #[serde(default)]
    pub webhooks: Vec<WebhookEndpoint>,
}

impl Scenario {
    pub fn from_toml_str(src: &str) -> Result<Self, ConfigError> {
        toml::from_str(src).map_err(|e| ConfigError::Toml(e.to_string()))
    }
}

/// The mutable half. Everything the admin API writes lands here, and `reset`
/// replaces it with [`Overlay::default`].
///
/// `None` means "defer to base"; `Some` means "override base". A `Vec` field
/// set to `Some(vec![])` therefore clears base's entries rather than inheriting
/// them, which is what `POST /_admin/faults` with an empty list should mean.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Overlay {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub faults: Option<Vec<FaultSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryFault>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<Vec<WebhookEndpoint>>,
}

impl Overlay {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// `base` with `overlay` applied. Recomputed on every mutation and published
/// through an `ArcSwap`, so readers on the request path never take a lock.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resolved {
    pub faults: Vec<FaultSpec>,
    pub telemetry: TelemetryFault,
    pub webhooks: Vec<WebhookEndpoint>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("scenario is not valid TOML: {0}")]
    Toml(String),
    #[error("scenario file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_default_scenario_parses() {
        let src = include_str!("../../../scenarios/default.toml");
        let scenario = Scenario::from_toml_str(src).expect("scenarios/default.toml must parse");
        assert_eq!(scenario.name, "default");
        assert!(
            scenario.faults.is_empty(),
            "the default scenario must inject nothing"
        );
        assert_eq!(scenario.telemetry, TelemetryFault::default());
    }

    #[test]
    fn an_empty_overlay_is_the_reset_state() {
        assert!(Overlay::default().is_empty());
        assert!(!Overlay {
            faults: Some(vec![]),
            ..Default::default()
        }
        .is_empty());
    }
}
