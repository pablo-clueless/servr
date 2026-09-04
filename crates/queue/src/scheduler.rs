//! The scheduler: a poll loop comparing due times against **virtual** now.
//!
//! # Why a poll loop and not a timer
//!
//! A `tokio::time::sleep_until(due)` would schedule against wall time, and
//! `clock/advance` could never make it fire early — which is the single
//! capability the whole testbed is built around (invariant 7). So the loop
//! ticks, reads [`Clock::now`], and asks the store what is due.
//!
//! # The tick interval is not a scheduling decision
//!
//! The loop wakes on a real-time interval, because something has to. What it
//! does *when* it wakes is decided entirely by virtual time. That distinction
//! is the point of invariant 1: no code here reads a wall clock to decide
//! whether a job should run, and the Phase 4 gate — advancing 30 virtual
//! seconds and expecting the job to complete in ~0.2 real seconds — is what
//! proves it. The interval bounds that latency, so it is deliberately short.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use testbed_core::{Clock, Event, EventKind, EventSink, JobState};

use crate::job::Job;
use crate::store::JobStore;

/// How often the loop wakes, in real time. Bounds how long after a
/// `clock/advance` a due job takes to start; the Phase 4 gate allows ~200ms.
pub const TICK: Duration = Duration::from_millis(25);

/// What a job kind does. Returning `Err` schedules a retry, or dead-letters the
/// job once attempts are exhausted.
pub type Handler = Arc<dyn Fn(&Job) -> Result<(), String> + Send + Sync>;

pub struct Scheduler {
    store: Arc<dyn JobStore>,
    clock: Arc<Clock>,
    bus: Arc<dyn EventSink>,
    handlers: HashMap<String, Handler>,
    run: testbed_core::RunId,
}

impl Scheduler {
    pub fn new(
        store: Arc<dyn JobStore>,
        clock: Arc<Clock>,
        bus: Arc<dyn EventSink>,
        run: testbed_core::RunId,
    ) -> Self {
        let mut scheduler = Self {
            store,
            clock,
            bus,
            handlers: HashMap::new(),
            run,
        };
        // Always available: the gate enqueues `noop`.
        scheduler.register("noop", |_| Ok(()));
        scheduler.register("fail", |job| {
            Err(format!("deliberate failure on attempt {}", job.attempt))
        });
        scheduler
    }

    pub fn register(
        &mut self,
        kind: impl Into<String>,
        handler: impl Fn(&Job) -> Result<(), String> + Send + Sync + 'static,
    ) {
        self.handlers.insert(kind.into(), Arc::new(handler));
    }

    pub fn store(&self) -> &Arc<dyn JobStore> {
        &self.store
    }

    /// Runs until the process ends.
    pub async fn run_forever(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(tick_ms = TICK.as_millis() as u64, "scheduler started");
        loop {
            ticker.tick().await;
            self.tick().await;
        }
    }

    /// One pass. Separated from the loop so tests drive it directly instead of
    /// sleeping — a test that sleeps to observe the scheduler is testing the
    /// tick interval, not the scheduler.
    pub async fn tick(&self) {
        let now = self.clock.now();

        let due = match self.store.claim_due(now).await {
            Ok(due) => due,
            Err(e) => {
                tracing::error!("claiming due jobs failed: {e}");
                return;
            }
        };

        for job in due {
            self.execute(job).await;
        }
    }

    async fn execute(&self, job: Job) {
        self.emit(&job, JobState::Scheduled, JobState::Running);

        // Trap T10: the execution span *links* to the enqueue span rather than
        // descending from it. A job delayed 30 minutes must not produce a
        // 30-minute trace.
        let span = tracing::info_span!(
            "job.execute",
            otel.name = %format!("job {}", job.kind),
            testbed.job.id = %job.id,
            testbed.job.kind = %job.kind,
            { testbed_telemetry::late::JOB_ATTEMPT } = job.attempt,
            { testbed_telemetry::late::JOB_OUTCOME } = tracing::field::Empty,
        );
        link_to_enqueue(&span, &job);

        let outcome = {
            let _entered = span.enter();
            match self.handlers.get(&job.kind) {
                Some(handler) => handler(&job),
                None => Err(format!("no handler registered for kind {:?}", job.kind)),
            }
        };

        let mut next = job.clone();
        match outcome {
            Ok(()) => {
                span.record(testbed_telemetry::late::JOB_OUTCOME, "succeeded");
                next.state = JobState::Succeeded;
                next.last_error = None;
                self.persist(&next).await;
                self.emit(&next, JobState::Running, JobState::Succeeded);
            }
            Err(error) => {
                span.record(testbed_telemetry::late::JOB_OUTCOME, "failed");
                next.last_error = Some(error.clone());

                if next.can_retry() {
                    // Retries are due in *virtual* time, so a scenario can
                    // assert the configured backoff by advancing the clock.
                    let delay = next.next_backoff_ms();
                    next.state = JobState::Scheduled;
                    next.due_at = self.clock.now() + TimeDelta::milliseconds(delay as i64);
                    self.persist(&next).await;
                    self.emit(&next, JobState::Running, JobState::Scheduled);
                    tracing::debug!(job = %next.id, attempt = next.attempt, delay_ms = delay, %error, "job retrying");
                } else {
                    next.state = JobState::Dead;
                    self.persist(&next).await;
                    self.emit(&next, JobState::Running, JobState::Dead);
                    tracing::warn!(job = %next.id, attempts = next.attempt, %error, "job dead-lettered");
                }
            }
        }
    }

    async fn persist(&self, job: &Job) {
        if let Err(e) = self.store.put(job.clone()).await {
            tracing::error!(job = %job.id, "persisting job state failed: {e}");
        }
    }

    /// Invariant 4: every transition is both an event and a span.
    fn emit(&self, job: &Job, from: JobState, to: JobState) {
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
            kind: EventKind::JobTransition {
                job: job.id,
                from,
                to,
                attempt: job.attempt,
            },
        });
    }
}

/// Adds the `FOLLOWS_FROM` link the Phase 4 gate looks for.
fn link_to_enqueue(span: &tracing::Span, job: &Job) {
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let (Some(trace), Some(parent)) = (job.enqueued_trace, job.enqueued_span) else {
        return;
    };

    span.add_link(SpanContext::new(
        TraceId::from_bytes(trace.to_bytes()),
        SpanId::from_bytes(parent.to_bytes()),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ));
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use testbed_core::{BroadcastBus, RunId};

    use super::*;
    use crate::store::MemoryStore;

    struct Harness {
        scheduler: Arc<Scheduler>,
        clock: Arc<Clock>,
        store: Arc<MemoryStore>,
        run: RunId,
    }

    fn harness() -> Harness {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
        let store = Arc::new(MemoryStore::new());

        let scheduler = Scheduler::new(
            Arc::clone(&store) as Arc<dyn JobStore>,
            Arc::clone(&clock),
            bus,
            run,
        );

        Harness {
            scheduler: Arc::new(scheduler),
            clock,
            store,
            run,
        }
    }

    /// The Phase 4 gate, as a test: a job due in 30 virtual seconds runs the
    /// moment the clock is advanced, and advancing takes no real time.
    ///
    /// If this ever starts taking ~30s, the scheduler is reading wall time and
    /// Phase 4 has failed regardless of the state transition.
    #[tokio::test]
    async fn advancing_the_clock_runs_a_delayed_job_immediately() {
        let h = harness();

        let job = Job::new(h.run, "noop", h.clock.now() + TimeDelta::seconds(30));
        let id = job.id;
        h.store.put(job).await.unwrap();

        h.scheduler.tick().await;
        assert_eq!(
            h.store.get(id).await.unwrap().state,
            JobState::Scheduled,
            "the job ran before it was due"
        );

        // Real elapsed time, through the sanctioned accessor. Invariant 1's
        // grep gate covers test code too, and rightly so — a test that reaches
        // for the wall clock directly is one edit away from a scheduler that
        // does.
        let started = testbed_telemetry::wall::instant();
        h.clock.advance(Duration::from_secs(30));
        h.scheduler.tick().await;
        let elapsed = testbed_telemetry::wall::instant() - started;

        assert_eq!(h.store.get(id).await.unwrap().state, JobState::Succeeded);
        assert!(
            elapsed < Duration::from_millis(200),
            "30 virtual seconds cost {elapsed:?} of real time; the scheduler is \
             reading wall time"
        );
    }

    #[tokio::test]
    async fn a_failing_job_retries_on_the_configured_virtual_backoff() {
        let h = harness();

        let job = Job::new(h.run, "fail", h.clock.now())
            .with_backoff(vec![1_000, 5_000])
            .with_max_attempts(3);
        let id = job.id;
        h.store.put(job).await.unwrap();

        // Attempt 1 fails, retry due 1s later in virtual time.
        h.scheduler.tick().await;
        let after_first = h.store.get(id).await.unwrap();
        assert_eq!(after_first.state, JobState::Scheduled);
        assert_eq!(after_first.attempt, 1);

        // Not yet due: ticking changes nothing.
        h.scheduler.tick().await;
        assert_eq!(h.store.get(id).await.unwrap().attempt, 1);

        h.clock.advance(Duration::from_millis(1_000));
        h.scheduler.tick().await;
        assert_eq!(h.store.get(id).await.unwrap().attempt, 2);

        // Second backoff is longer; 1s is not enough.
        h.clock.advance(Duration::from_millis(1_000));
        h.scheduler.tick().await;
        assert_eq!(h.store.get(id).await.unwrap().attempt, 2);

        h.clock.advance(Duration::from_millis(4_000));
        h.scheduler.tick().await;

        let final_state = h.store.get(id).await.unwrap();
        assert_eq!(final_state.attempt, 3);
        assert_eq!(
            final_state.state,
            JobState::Dead,
            "attempts were exhausted; the job should be dead-lettered"
        );
    }

    #[tokio::test]
    async fn a_dead_job_stays_dead() {
        let h = harness();

        let job = Job::new(h.run, "fail", h.clock.now())
            .with_backoff(vec![0])
            .with_max_attempts(1);
        let id = job.id;
        h.store.put(job).await.unwrap();

        h.scheduler.tick().await;
        assert_eq!(h.store.get(id).await.unwrap().state, JobState::Dead);

        // A terminal job is never claimed again, however far the clock moves.
        h.clock.advance(Duration::from_secs(3600));
        h.scheduler.tick().await;
        assert_eq!(h.store.get(id).await.unwrap().attempt, 1);
    }

    #[tokio::test]
    async fn an_unknown_kind_fails_rather_than_disappearing() {
        let h = harness();

        let job = Job::new(h.run, "does-not-exist", h.clock.now()).with_max_attempts(1);
        let id = job.id;
        h.store.put(job).await.unwrap();

        h.scheduler.tick().await;

        let done = h.store.get(id).await.unwrap();
        assert_eq!(done.state, JobState::Dead);
        assert!(done.last_error.unwrap().contains("no handler"));
    }

    #[tokio::test]
    async fn a_succeeding_job_runs_exactly_once() {
        let h = harness();
        let runs = Arc::new(AtomicU32::new(0));

        let mut scheduler = Scheduler::new(
            Arc::clone(&h.store) as Arc<dyn JobStore>,
            Arc::clone(&h.clock),
            Arc::new(BroadcastBus::new(8, Arc::clone(&h.clock), h.run)),
            h.run,
        );
        let counter = Arc::clone(&runs);
        scheduler.register("count", move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        h.store
            .put(Job::new(h.run, "count", h.clock.now()))
            .await
            .unwrap();

        for _ in 0..5 {
            scheduler.tick().await;
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn every_transition_reaches_the_event_bus() {
        use futures_util::StreamExt;

        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
        let store = Arc::new(MemoryStore::new());
        let scheduler = Scheduler::new(
            Arc::clone(&store) as Arc<dyn JobStore>,
            Arc::clone(&clock),
            Arc::clone(&bus) as Arc<dyn EventSink>,
            run,
        );

        let mut events = bus.subscribe();

        store.put(Job::new(run, "noop", clock.now())).await.unwrap();
        scheduler.tick().await;

        let mut transitions = Vec::new();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(50), events.next()).await
        {
            if let EventKind::JobTransition { from, to, .. } = event.kind {
                transitions.push((from, to));
            }
        }

        assert_eq!(
            transitions,
            vec![
                (JobState::Scheduled, JobState::Running),
                (JobState::Running, JobState::Succeeded),
            ],
            "a state change happened without reaching the bus (invariant 4)"
        );
    }
}
