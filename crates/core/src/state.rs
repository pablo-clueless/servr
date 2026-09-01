//! The control plane's own state.
//!
//! In-memory behind `ArcSwap`, never in Postgres (HANDOFF §5 invariant 3): it
//! must survive a full data-plane wipe. The moment this depends on Postgres,
//! resetting the data plane silently resets the testbed's configuration too.
//!
//! # Layering
//!
//! `base` is parsed from a scenario file at boot and **never written again**
//! (invariant 2). Every runtime mutation lands in `overlay`. [`State::reset`]
//! drops the overlay and re-resolves from base alone. That is the entire basis
//! of test isolation: if `reset` cannot reconstruct a known-good state from the
//! scenario file, every test that ran earlier is part of this test's input.

use std::sync::{Arc, Mutex};

use arc_swap::{ArcSwap, Guard};

use crate::bus::EventSink;
use crate::clock::Clock;
use crate::config::{Overlay, Resolved, Scenario};
use crate::run::RunId;

pub struct State {
    base: Arc<Scenario>,
    overlay: ArcSwap<Overlay>,
    resolved: ArcSwap<Resolved>,
    clock: Arc<Clock>,
    bus: Arc<dyn EventSink>,
    /// The process's default run. Phase 3 adds per-request runs on top; this is
    /// the one reported by `/_admin/health` and used when a request names none.
    run: RunId,
    /// Serializes read-modify-write on the overlay. Readers never touch it —
    /// they go through `ArcSwap` and take no lock at all, which matters because
    /// [`State::resolved`] is on the request path. Without this, two concurrent
    /// `/_admin/faults` calls can lose one another's writes, and a lost
    /// control-plane write surfaces later as an unreproducible flaky test.
    write: Mutex<()>,
}

impl State {
    pub fn new(base: Scenario, clock: Arc<Clock>, bus: Arc<dyn EventSink>, run: RunId) -> Self {
        let base = Arc::new(base);
        let overlay = Overlay::default();
        let resolved = resolve(&base, &overlay);

        Self {
            base,
            overlay: ArcSwap::from_pointee(overlay),
            resolved: ArcSwap::from_pointee(resolved),
            clock,
            bus,
            run,
            write: Mutex::new(()),
        }
    }

    /// Applies `f` to a copy of the overlay, publishes it, and re-resolves.
    pub fn mutate(&self, f: impl FnOnce(&mut Overlay)) {
        let _guard = self.write.lock().unwrap_or_else(|e| e.into_inner());

        let mut next = self.overlay.load().as_ref().clone();
        f(&mut next);

        self.resolved.store(Arc::new(resolve(&self.base, &next)));
        self.overlay.store(Arc::new(next));
    }

    /// Drops the overlay, re-resolves from base, and returns the clock to wall
    /// time. Backs `POST /_admin/reset`.
    ///
    /// The data plane is *not* touched here — dropping run schemas and flushing
    /// Redis is a separate concern with a separate blast radius.
    pub fn reset(&self) {
        let _guard = self.write.lock().unwrap_or_else(|e| e.into_inner());

        let overlay = Overlay::default();
        self.resolved.store(Arc::new(resolve(&self.base, &overlay)));
        self.overlay.store(Arc::new(overlay));
        self.clock.reset();
    }

    /// The effective configuration. Lock-free; safe to call per request.
    pub fn resolved(&self) -> Guard<Arc<Resolved>> {
        self.resolved.load()
    }

    pub fn overlay(&self) -> Guard<Arc<Overlay>> {
        self.overlay.load()
    }

    /// Immutable after boot. There is deliberately no `&mut` accessor.
    pub fn base(&self) -> &Scenario {
        &self.base
    }

    pub fn clock(&self) -> &Arc<Clock> {
        &self.clock
    }

    pub fn bus(&self) -> &Arc<dyn EventSink> {
        &self.bus
    }

    pub fn run(&self) -> RunId {
        self.run
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("scenario", &self.base.name)
            .field("run", &self.run)
            .field("overlay_active", &!self.overlay.load().is_empty())
            .field("clock_offset_ms", &self.clock.offset_ms())
            .finish()
    }
}

/// Pure function of `(base, overlay)`. Keeping it pure is what makes the reset
/// property hold; if this ever reads anything else, `reset` stops being a
/// guarantee and becomes a hope.
fn resolve(base: &Scenario, overlay: &Overlay) -> Resolved {
    Resolved {
        faults: overlay
            .faults
            .clone()
            .unwrap_or_else(|| base.faults.clone()),
        telemetry: overlay
            .telemetry
            .clone()
            .unwrap_or_else(|| base.telemetry.clone()),
        webhooks: overlay
            .webhooks
            .clone()
            .unwrap_or_else(|| base.webhooks.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;
    use crate::bus::BroadcastBus;
    use crate::fault::{FaultSpec, SigningScheme, TelemetryFault, WebhookEndpoint};

    fn state(base: Scenario) -> State {
        let clock = Arc::new(Clock::new());
        let run = RunId::new();
        let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));
        State::new(base, clock, bus, run)
    }

    fn seeded_scenario() -> Scenario {
        Scenario {
            name: "seeded".into(),
            faults: vec![FaultSpec {
                route: "/api/seeded".into(),
                rate: 0.25,
                latency_ms: Some(100),
                ..Default::default()
            }],
            telemetry: TelemetryFault {
                rate: 0.5,
                orphan_spans: true,
                ..Default::default()
            },
            webhooks: vec![WebhookEndpoint {
                name: "seeded".into(),
                url: "http://example.invalid/hook".into(),
                sign: SigningScheme::Github,
                secret: Some("shh".into()),
                backoff_ms: vec![1000, 5000],
                fail_first: 1,
            }],
            ..Default::default()
        }
    }

    // --- strategies ------------------------------------------------------

    fn arb_fault() -> impl Strategy<Value = FaultSpec> {
        (
            "/api/[a-z*]{1,8}",
            0.0f64..=1.0,
            proptest::option::of(0u64..2000),
            proptest::option::of(400u16..600),
            any::<bool>(),
        )
            .prop_map(
                |(route, rate, latency_ms, status, drop_connection)| FaultSpec {
                    route,
                    rate,
                    latency_ms,
                    status,
                    drop_connection,
                    ..Default::default()
                },
            )
    }

    fn arb_telemetry() -> impl Strategy<Value = TelemetryFault> {
        (
            0.0f64..=1.0,
            any::<bool>(),
            proptest::option::of(-3_600_000i64..3_600_000),
            proptest::option::of(1u32..100_000),
            any::<bool>(),
        )
            .prop_map(
                |(rate, orphan_spans, clock_skew_ms, cardinality_bomb, drop_export)| {
                    TelemetryFault {
                        rate,
                        orphan_spans,
                        clock_skew_ms,
                        cardinality_bomb,
                        drop_export,
                        ..Default::default()
                    }
                },
            )
    }

    fn arb_overlay() -> impl Strategy<Value = Overlay> {
        (
            proptest::option::of(proptest::collection::vec(arb_fault(), 0..4)),
            proptest::option::of(arb_telemetry()),
        )
            .prop_map(|(faults, telemetry)| Overlay {
                faults,
                telemetry,
                webhooks: None,
            })
    }

    // --- the task 4 acceptance criterion ---------------------------------

    proptest! {
        /// HANDOFF §9 task 4: arbitrary overlay mutations followed by `reset`
        /// always yield a `Resolved` equal to boot state.
        #[test]
        fn reset_restores_boot_state_after_arbitrary_mutation(
            overlays in proptest::collection::vec(arb_overlay(), 1..8)
        ) {
            let state = state(seeded_scenario());
            let boot = state.resolved().as_ref().clone();

            for overlay in overlays {
                state.mutate(|o| *o = overlay);
            }
            state.reset();

            let after = state.resolved();
            prop_assert_eq!(after.as_ref(), &boot);
        }

        /// Invariant 2, stated directly: nothing a mutation does can reach base.
        #[test]
        fn base_is_immutable_after_boot(
            overlays in proptest::collection::vec(arb_overlay(), 1..8)
        ) {
            let scenario = seeded_scenario();
            let state = state(scenario.clone());

            for overlay in overlays {
                state.mutate(|o| *o = overlay);
            }

            prop_assert_eq!(state.base(), &scenario);
        }
    }

    // --- ordinary cases ---------------------------------------------------

    #[test]
    fn an_unset_overlay_field_defers_to_base() {
        let state = state(seeded_scenario());
        state.mutate(|o| o.telemetry = Some(TelemetryFault::default()));

        let resolved = state.resolved();
        assert_eq!(resolved.telemetry, TelemetryFault::default());
        assert_eq!(
            resolved.faults[0].route, "/api/seeded",
            "faults were left unset and should still come from base"
        );
    }

    #[test]
    fn an_empty_list_clears_base_rather_than_inheriting_it() {
        let state = state(seeded_scenario());
        assert_eq!(state.resolved().faults.len(), 1);

        // What `POST /_admin/faults` with an empty list must mean.
        state.mutate(|o| o.faults = Some(vec![]));
        assert!(state.resolved().faults.is_empty());
    }

    #[test]
    fn reset_returns_the_clock_to_wall_time() {
        let state = state(seeded_scenario());
        state.clock().advance(Duration::from_secs(3600));
        state.clock().freeze();

        state.reset();

        assert_eq!(state.clock().offset_ms(), 0);
        assert!(!state.clock().is_frozen());
    }

    #[test]
    fn concurrent_mutations_do_not_lose_writes() {
        let state = Arc::new(state(Scenario::default()));

        std::thread::scope(|scope| {
            for i in 0..8 {
                let state = Arc::clone(&state);
                scope.spawn(move || {
                    state.mutate(|o| {
                        let mut faults = o.faults.take().unwrap_or_default();
                        faults.push(FaultSpec {
                            route: format!("/api/{i}"),
                            rate: 1.0,
                            ..Default::default()
                        });
                        o.faults = Some(faults);
                    });
                });
            }
        });

        assert_eq!(
            state.resolved().faults.len(),
            8,
            "a concurrent mutation was lost"
        );
    }
}
