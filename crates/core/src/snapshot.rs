//! Control-plane snapshots, to SQLite.
//!
//! `POST /_admin/snapshot` writes one; `testbed --restore <path>` boots from
//! one. What goes in is exactly the testbed's own state — the scenario it
//! booted from, the overlay the admin API has since written, the clock offset,
//! and the run id. Telemetry fault config rides along inside the overlay,
//! because it *is* control-plane config (HANDOFF §7 phase 9 says so explicitly).
//!
//! # What is deliberately not in here
//!
//! The data plane. No Postgres rows, no schemas, no queued jobs, no captured
//! webhooks, no mail. That is not an omission to fix later: decision 4 says the
//! control plane must survive a full data-plane wipe, and a snapshot that
//! restored both would make the two indistinguishable — you could no longer
//! wipe the data plane and keep your configuration, which is the entire point
//! of the split.
//!
//! The live event bus is not in here either. `/_admin/events` is a tail, not a
//! log (see [`crate::bus`]), so there is no stored history to capture; a
//! subscriber that reconnects after a restore starts from the next event.
//!
//! # Why SQLite and not JSON
//!
//! Locked decision 3. A file the operator can open with `sqlite3` and read
//! without the testbed running is worth more than a marginally simpler writer —
//! debugging "why did this restore come back wrong" should not require the
//! thing being debugged.
//!
//! # This is not the Postgres path
//!
//! `crates/core` depends on `rusqlite`, never on `sqlx`. That is deliberate:
//! invariant 3's grep treats `sqlx` as the marker for Postgres I/O, and keeping
//! the two bindings distinct means the gate stays precise instead of needing an
//! exemption for a file that never touches Postgres at all.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::clock::Clock;
use crate::config::{Overlay, Scenario};
use crate::run::RunId;
use crate::state::State;

/// Bumped when the stored shape changes. A restore refuses a version it does
/// not know rather than silently reading fields that have moved — a snapshot
/// that half-restores is worse than one that will not load.
pub const VERSION: u32 = 1;

/// One captured control plane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u32,
    /// Real time of capture.
    pub created_at: DateTime<Utc>,
    /// Virtual time at capture. Recorded for forensics — a restore does **not**
    /// pin the clock back to it; see [`Snapshot::restore_clock`].
    pub virtual_now: DateTime<Utc>,
    pub run: RunId,
    pub clock_offset_ms: i64,
    pub clock_frozen: bool,
    /// The scenario the testbed booted from. Carried so `reset` still works
    /// after a restore: without it there would be nothing to reset *to*, and
    /// invariant 2 would be unenforceable on a restored process.
    pub base: Scenario,
    pub overlay: Overlay,
}

impl Snapshot {
    /// Captures the current control plane.
    pub fn capture(state: &State) -> Self {
        let clock = state.clock();
        Self {
            version: VERSION,
            created_at: Clock::wall_now(),
            virtual_now: clock.now(),
            run: state.run(),
            clock_offset_ms: clock.offset_ms(),
            clock_frozen: clock.is_frozen(),
            base: state.base().clone(),
            overlay: (**state.overlay()).clone(),
        }
    }

    /// A clock carrying this snapshot's offset.
    ///
    /// The *offset* is restored, not the absolute virtual time. Pinning the
    /// clock back to `virtual_now` would leave a testbed restored an hour later
    /// with a virtual clock an hour behind wall time, which reads as a bug in
    /// the clock rather than as a restore. What the snapshot actually records
    /// is "this control plane was configured with a +N offset", and that is
    /// what comes back.
    pub fn restore_clock(&self) -> Clock {
        Clock::restore(self.clock_offset_ms, self.clock_frozen)
    }

    /// Writes the snapshot, replacing whatever the file held.
    ///
    /// One snapshot per file, enforced by the schema. Appending would make
    /// `--restore` ambiguous about which one it meant.
    pub fn write(&self, path: impl AsRef<Path>) -> Result<(), SnapshotError> {
        let conn = rusqlite::Connection::open(path.as_ref()).map_err(SnapshotError::open)?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS snapshot (
                 id              INTEGER PRIMARY KEY CHECK (id = 1),
                 version         INTEGER NOT NULL,
                 created_at      TEXT    NOT NULL,
                 virtual_now     TEXT    NOT NULL,
                 run             TEXT    NOT NULL,
                 clock_offset_ms INTEGER NOT NULL,
                 clock_frozen    INTEGER NOT NULL,
                 base            TEXT    NOT NULL,
                 overlay         TEXT    NOT NULL
             );",
        )
        .map_err(SnapshotError::sql)?;

        // The scenario and overlay go in as JSON rather than as columns. They
        // are versioned types that will grow fields, and a column per field
        // would turn every addition into a migration of a file whose only job
        // is to be read back by the same binary that wrote it.
        conn.execute(
            "INSERT INTO snapshot
                 (id, version, created_at, virtual_now, run,
                  clock_offset_ms, clock_frozen, base, overlay)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
                 version = excluded.version,
                 created_at = excluded.created_at,
                 virtual_now = excluded.virtual_now,
                 run = excluded.run,
                 clock_offset_ms = excluded.clock_offset_ms,
                 clock_frozen = excluded.clock_frozen,
                 base = excluded.base,
                 overlay = excluded.overlay",
            rusqlite::params![
                self.version,
                self.created_at.to_rfc3339(),
                self.virtual_now.to_rfc3339(),
                self.run.to_string(),
                self.clock_offset_ms,
                self.clock_frozen as i64,
                serde_json::to_string(&self.base).map_err(SnapshotError::encode)?,
                serde_json::to_string(&self.overlay).map_err(SnapshotError::encode)?,
            ],
        )
        .map_err(SnapshotError::sql)?;

        Ok(())
    }

    /// Reads a snapshot back.
    pub fn read(path: impl AsRef<Path>) -> Result<Self, SnapshotError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(SnapshotError::Missing(path.display().to_string()));
        }

        let conn = rusqlite::Connection::open(path).map_err(SnapshotError::open)?;
        let snapshot = conn
            .query_row(
                "SELECT version, created_at, virtual_now, run,
                        clock_offset_ms, clock_frozen, base, overlay
                 FROM snapshot WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, u32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    SnapshotError::Empty(path.display().to_string())
                }
                other => SnapshotError::sql(other),
            })?;

        let (version, created_at, virtual_now, run, offset, frozen, base, overlay) = snapshot;

        if version != VERSION {
            return Err(SnapshotError::Version {
                found: version,
                expected: VERSION,
            });
        }

        Ok(Self {
            version,
            created_at: parse_time(&created_at)?,
            virtual_now: parse_time(&virtual_now)?,
            run: run.parse().map_err(|_| SnapshotError::Field("run"))?,
            clock_offset_ms: offset,
            clock_frozen: frozen != 0,
            base: serde_json::from_str(&base).map_err(SnapshotError::decode)?,
            overlay: serde_json::from_str(&overlay).map_err(SnapshotError::decode)?,
        })
    }
}

fn parse_time(raw: &str) -> Result<DateTime<Utc>, SnapshotError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|_| SnapshotError::Field("timestamp"))
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("no snapshot at {0}")]
    Missing(String),
    #[error("{0} has no snapshot row; it was created but never written")]
    Empty(String),
    #[error("snapshot is version {found}, this build reads version {expected}")]
    Version { found: u32, expected: u32 },
    #[error("could not open the snapshot file: {0}")]
    Open(String),
    #[error("snapshot database error: {0}")]
    Sql(String),
    #[error("snapshot field {0} is not readable")]
    Field(&'static str),
    #[error("could not encode control-plane state: {0}")]
    Encode(String),
    #[error("could not decode control-plane state: {0}")]
    Decode(String),
}

impl SnapshotError {
    fn open(e: rusqlite::Error) -> Self {
        Self::Open(e.to_string())
    }
    fn sql(e: rusqlite::Error) -> Self {
        Self::Sql(e.to_string())
    }
    fn encode(e: serde_json::Error) -> Self {
        Self::Encode(e.to_string())
    }
    fn decode(e: serde_json::Error) -> Self {
        Self::Decode(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::bus::{BroadcastBus, EventSink};
    use crate::fault::{FaultSpec, TelemetryFault};

    fn temp(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("testbed-snapshot-{}-{name}.sqlite", RunId::new()));
        path
    }

    fn state() -> Arc<State> {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(16, Arc::clone(&clock), run));
        Arc::new(State::new(
            Scenario {
                name: "snapshot-test".into(),
                faults: vec![FaultSpec {
                    route: "/api/*".into(),
                    rate: 0.5,
                    ..Default::default()
                }],
                ..Default::default()
            },
            clock,
            bus as Arc<dyn EventSink>,
            run,
        ))
    }

    #[test]
    fn a_snapshot_round_trips_through_a_file() {
        let state = state();
        state.clock().advance(Duration::from_secs(3_600));
        state.mutate(|overlay| {
            overlay.telemetry = Some(TelemetryFault {
                rate: 1.0,
                orphan_spans: true,
                ..Default::default()
            })
        });

        let path = temp("roundtrip");
        let captured = Snapshot::capture(&state);
        captured.write(&path).unwrap();

        let read = Snapshot::read(&path).unwrap();
        assert_eq!(read, captured);
        std::fs::remove_file(&path).ok();
    }

    /// The half the Phase 9 gate is actually about: telemetry fault config is
    /// control plane, so it has to come back.
    #[test]
    fn telemetry_fault_config_survives_the_round_trip() {
        let state = state();
        state.mutate(|overlay| {
            overlay.telemetry = Some(TelemetryFault {
                rate: 1.0,
                cardinality_bomb: Some(50_000),
                clock_skew_ms: Some(3_600_000),
                ..Default::default()
            })
        });

        let path = temp("telemetry");
        Snapshot::capture(&state).write(&path).unwrap();
        let read = Snapshot::read(&path).unwrap();

        let telemetry = read.overlay.telemetry.expect("telemetry config was lost");
        assert_eq!(telemetry.cardinality_bomb, Some(50_000));
        assert_eq!(telemetry.clock_skew_ms, Some(3_600_000));
        std::fs::remove_file(&path).ok();
    }

    /// `reset` has to still work after a restore, so the scenario it resets to
    /// must be in the file.
    #[test]
    fn the_base_scenario_is_carried_so_reset_still_works() {
        let state = state();
        state.mutate(|overlay| overlay.faults = Some(vec![]));

        let path = temp("base");
        Snapshot::capture(&state).write(&path).unwrap();
        let read = Snapshot::read(&path).unwrap();

        assert_eq!(read.base.name, "snapshot-test");
        assert_eq!(
            read.base.faults.len(),
            1,
            "the scenario's own fault was lost"
        );
        assert_eq!(
            read.overlay.faults,
            Some(vec![]),
            "the overlay that cleared it was lost"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn writing_twice_replaces_rather_than_appends() {
        let state = state();
        let path = temp("replace");

        Snapshot::capture(&state).write(&path).unwrap();
        state.clock().advance(Duration::from_secs(60));
        let second = Snapshot::capture(&state);
        second.write(&path).unwrap();

        assert_eq!(Snapshot::read(&path).unwrap(), second);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn the_clock_offset_comes_back_not_the_absolute_time() {
        let state = state();
        state.clock().advance(Duration::from_secs(3_600));

        let snapshot = Snapshot::capture(&state);
        let restored = snapshot.restore_clock();

        assert_eq!(restored.offset_ms(), 3_600_000);
        assert!(!restored.is_frozen());
        // A fresh clock plus the offset, not a clock pinned into the past.
        assert!(restored.now() > Clock::wall_now() + chrono::TimeDelta::minutes(59));
    }

    #[test]
    fn a_frozen_clock_comes_back_frozen() {
        let state = state();
        state.clock().freeze();

        let restored = Snapshot::capture(&state).restore_clock();
        assert!(restored.is_frozen(), "a frozen clock restored running");
    }

    #[test]
    fn reading_a_file_that_is_not_there_says_so() {
        assert!(matches!(
            Snapshot::read(temp("absent")),
            Err(SnapshotError::Missing(_))
        ));
    }

    #[test]
    fn a_snapshot_from_a_future_version_is_refused_rather_than_half_read() {
        let state = state();
        let path = temp("version");
        let mut snapshot = Snapshot::capture(&state);
        snapshot.version = VERSION + 1;
        snapshot.write(&path).unwrap();

        assert!(matches!(
            Snapshot::read(&path),
            Err(SnapshotError::Version { .. })
        ));
        std::fs::remove_file(&path).ok();
    }

    /// The file has to be readable with `sqlite3` and nothing else — that is
    /// most of why decision 3 chose SQLite over a serialized blob.
    #[test]
    fn the_file_is_a_plain_readable_sqlite_database() {
        let state = state();
        let path = temp("readable");
        Snapshot::capture(&state).write(&path).unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        let name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "snapshot");

        let rows: i64 = conn
            .query_row("SELECT count(*) FROM snapshot", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "one snapshot per file");
        std::fs::remove_file(&path).ok();
    }
}
