use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifies one test run. Every data-plane write is namespaced by it
/// (HANDOFF §5 invariant 6) — an unnamespaced write breaks parallel test
/// execution for every other run in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Postgres schema holding this run's rows (Q2: schema-per-run).
    ///
    /// Set via `PoolOptions::after_connect`, never once at startup — pooled
    /// connections are handed out fresh and will not carry a `search_path`
    /// set on some earlier connection (trap T5).
    pub fn schema(&self) -> String {
        format!("run_{}", self.0.simple())
    }

    /// Value of the `X-Testbed-Run` header. Mailpit has no native namespacing,
    /// so this header on send plus filtering on read is the entire mail
    /// isolation story (trap T7).
    pub fn header_value(&self) -> String {
        self.0.to_string()
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::from_str(s)?))
    }
}

/// The header carrying a [`RunId`] across every boundary the testbed controls.
pub const RUN_HEADER: &str = "x-testbed-run";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_name_is_a_valid_bare_identifier() {
        let schema = RunId::new().schema();
        assert!(schema.starts_with("run_"));
        assert_eq!(
            schema.len(),
            4 + 32,
            "uuid must be hyphen-free in a schema name"
        );
        assert!(schema[4..].chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn round_trips_through_its_header_value() {
        let run = RunId::new();
        assert_eq!(run.header_value().parse::<RunId>().unwrap(), run);
    }
}
