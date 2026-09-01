//! Wall-clock access for span timestamps.
//!
//! # This file is the second of two sanctioned wall-clock readers
//!
//! The other is `crates/core/src/clock.rs`. CI greps every other file for
//! `SystemTime::now` and `Instant::now` and fails on a hit (HANDOFF §5
//! invariant 1).
//!
//! This exemption exists for exactly one reason: spans must carry honest
//! real-world durations (§2 decision 11). A span cannot claim to last 30
//! virtual seconds — no collector would survive it, and every trace-waterfall
//! UI pointed at the testbed would become unusable. Virtual time rides along as
//! the `testbed.virtual_time` span attribute instead.
//!
//! **Do not call anything here from a scheduler.** If a decision about *when*
//! something happens reads this module, that is the bug invariant 1 exists to
//! catch.

use std::time::{Instant, SystemTime};

use chrono::{DateTime, Utc};

/// Real time, for stamping a span.
pub fn now() -> DateTime<Utc> {
    SystemTime::now().into()
}

/// A real monotonic reference, for measuring how long a span actually took.
pub fn instant() -> Instant {
    Instant::now()
}

/// Span attribute names. Every span carries both (HANDOFF §7 phase 2b).
pub mod attr {
    pub const RUN_ID: &str = "testbed.run_id";
    /// Virtual time as RFC 3339, so a trace viewer can be read against the
    /// event stream after a `clock/advance`.
    pub const VIRTUAL_TIME: &str = "testbed.virtual_time";
    pub const VIRTUAL_OFFSET_MS: &str = "testbed.virtual_offset_ms";
}
