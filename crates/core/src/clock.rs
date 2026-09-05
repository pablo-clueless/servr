//! The virtual clock. Authoritative for all scheduling (HANDOFF §2 decision 7).
//!
//! # This file is one of two sanctioned wall-clock readers
//!
//! `Instant::now()` and `SystemTime::now()` are permitted here and in
//! `crates/telemetry/src/wall.rs`, nowhere else (HANDOFF §5 invariant 1). CI
//! greps for violations. If the queue reads wall time anywhere, time travel is
//! dead and the whole testbed is untestable.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};

/// A clock whose `now()` moves only when the operator advances it, or with real
/// time while running.
///
/// Virtual time is `epoch_wall + elapsed + offset`, where `elapsed` stops
/// accumulating while frozen and `offset` is moved by [`Clock::advance`].
/// Advancing 30 virtual seconds is a single atomic add: it must never sleep,
/// which is what the Phase 4 gate's `real 0m0.2xxs` assertion is really testing.
#[derive(Debug)]
pub struct Clock {
    epoch: Instant,
    epoch_wall: DateTime<Utc>,
    offset_ms: AtomicI64,
    frozen: AtomicBool,
    /// Elapsed-since-epoch captured at the moment of freezing, in milliseconds.
    frozen_elapsed_ms: AtomicI64,
}

impl Clock {
    /// Starts a clock running in step with wall time, at zero offset.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            epoch_wall: Self::wall_now(),
            offset_ms: AtomicI64::new(0),
            frozen: AtomicBool::new(false),
            frozen_elapsed_ms: AtomicI64::new(0),
        }
    }

    /// Starts a clock pinned to `at`, already frozen. Useful for deterministic
    /// scenarios that assert on absolute timestamps.
    pub fn frozen_at(at: DateTime<Utc>) -> Self {
        let clock = Self {
            epoch: Instant::now(),
            epoch_wall: at,
            offset_ms: AtomicI64::new(0),
            frozen: AtomicBool::new(true),
            frozen_elapsed_ms: AtomicI64::new(0),
        };
        clock.freeze();
        clock
    }

    /// Rebuilds a clock carrying `offset_ms`, frozen or not, for a restored
    /// snapshot (HANDOFF §7 phase 9).
    ///
    /// The offset is reproduced, not the absolute virtual time the snapshot was
    /// taken at — see `Snapshot::restore_clock` for why.
    pub fn restore(offset_ms: i64, frozen: bool) -> Self {
        let clock = Self::new();
        clock.offset_ms.store(offset_ms, Ordering::SeqCst);
        if frozen {
            clock.freeze();
        }
        clock
    }

    /// Current virtual time. Every scheduling decision in the testbed compares
    /// against this and never against wall time.
    pub fn now(&self) -> DateTime<Utc> {
        self.epoch_wall + TimeDelta::milliseconds(self.elapsed_ms() + self.offset_ms())
    }

    /// Moves virtual time forward by `d`. Returns the new virtual time.
    ///
    /// This is an atomic add, not a sleep — jobs due at the new time become due
    /// immediately, and the scheduler's next poll picks them up.
    pub fn advance(&self, d: Duration) {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.offset_ms.fetch_add(ms, Ordering::SeqCst);
    }

    /// Stops virtual time. `now()` returns the same instant until [`Clock::resume`],
    /// though [`Clock::advance`] still moves it.
    pub fn freeze(&self) {
        // Capture elapsed before flipping the flag, so a concurrent `now()`
        // either sees the running clock or a fully-written frozen value.
        self.frozen_elapsed_ms
            .store(self.running_elapsed_ms(), Ordering::SeqCst);
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// Restarts virtual time from wherever the freeze left it.
    pub fn resume(&self) {
        if !self.frozen.swap(false, Ordering::SeqCst) {
            return;
        }
        // Roll the time skipped while frozen into the offset, so resuming does
        // not jump forward by the length of the freeze.
        let skipped = self.running_elapsed_ms() - self.frozen_elapsed_ms.load(Ordering::SeqCst);
        self.offset_ms.fetch_sub(skipped, Ordering::SeqCst);
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Accumulated virtual offset, in milliseconds. Snapshotted by
    /// `/_admin/snapshot` (HANDOFF §7 phase 9).
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms.load(Ordering::SeqCst)
    }

    /// Drops the offset and unfreezes. Called by `reset`.
    pub fn reset(&self) {
        self.offset_ms.store(0, Ordering::SeqCst);
        self.frozen.store(false, Ordering::SeqCst);
        self.frozen_elapsed_ms.store(0, Ordering::SeqCst);
    }

    /// Real wall time. **Only** for span timestamps and `Event::wall_at`
    /// (HANDOFF §2 decision 11) — a span may not claim to last 30 virtual
    /// seconds, no collector would survive it. Never schedule against this.
    pub fn wall_now() -> DateTime<Utc> {
        Utc::now()
    }

    fn elapsed_ms(&self) -> i64 {
        if self.frozen.load(Ordering::SeqCst) {
            self.frozen_elapsed_ms.load(Ordering::SeqCst)
        } else {
            self.running_elapsed_ms()
        }
    }

    fn running_elapsed_ms(&self) -> i64 {
        i64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(i64::MAX)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_does_not_sleep() {
        let clock = Clock::new();
        let before = Instant::now();
        clock.advance(Duration::from_secs(30));
        let spent = before.elapsed();

        assert!(
            spent < Duration::from_millis(10),
            "advancing 30 virtual seconds took {spent:?} of real time"
        );
        assert!(clock.offset_ms() >= 30_000);
    }

    #[test]
    fn advance_is_monotonic() {
        let clock = Clock::frozen_at(Utc::now());
        let mut last = clock.now();
        for _ in 0..100 {
            clock.advance(Duration::from_millis(250));
            let next = clock.now();
            assert!(next > last, "{next} did not advance past {last}");
            last = next;
        }
    }

    #[test]
    fn frozen_clock_does_not_drift() {
        let clock = Clock::new();
        clock.freeze();
        let first = clock.now();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(clock.now(), first, "frozen clock drifted with wall time");
    }

    #[test]
    fn advance_still_moves_a_frozen_clock() {
        let clock = Clock::new();
        clock.freeze();
        let first = clock.now();
        clock.advance(Duration::from_secs(60));
        assert_eq!(clock.now() - first, TimeDelta::seconds(60));
    }

    #[test]
    fn resume_does_not_replay_the_freeze() {
        let clock = Clock::new();
        clock.freeze();
        let frozen_at = clock.now();
        std::thread::sleep(Duration::from_millis(30));
        clock.resume();

        let after = clock.now();
        let jumped = after - frozen_at;
        assert!(
            jumped < TimeDelta::milliseconds(20),
            "resume replayed {jumped} of the freeze"
        );
    }

    #[test]
    fn reset_returns_to_wall_time() {
        let clock = Clock::new();
        clock.advance(Duration::from_secs(3600));
        clock.freeze();
        clock.reset();

        assert_eq!(clock.offset_ms(), 0);
        assert!(!clock.is_frozen());
        let drift = clock.now() - Clock::wall_now();
        assert!(
            drift.abs() < TimeDelta::seconds(1),
            "drift after reset: {drift}"
        );
    }
}
