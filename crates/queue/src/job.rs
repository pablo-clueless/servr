//! What a job is.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use testbed_core::{JobId, JobState, RunId, SpanId, TraceId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub run: RunId,
    /// Names the handler. Unknown kinds fail rather than vanish.
    pub kind: String,
    pub payload: serde_json::Value,
    pub state: JobState,
    /// Attempts made so far. `0` before the first run.
    pub attempt: u32,
    /// Maximum attempts before the job is dead-lettered.
    pub max_attempts: u32,
    /// **Virtual** time the job becomes runnable. The scheduler compares this
    /// against the virtual clock and never against wall time — that comparison
    /// is the whole of invariant 7.
    pub due_at: DateTime<Utc>,
    /// Virtual-millisecond backoff, one entry per retry. Runs off the end by
    /// repeating the last entry.
    pub backoff_ms: Vec<u64>,
    /// The trace the job was *enqueued* under.
    ///
    /// Trap T10: the execution span **links** to this, it does not descend from
    /// it. Parenting is the intuitive choice and it is wrong — a job delayed 30
    /// minutes would produce a 30-minute trace, and a handful of those makes
    /// every trace-waterfall UI pointed at the testbed unusable.
    pub enqueued_trace: Option<TraceId>,
    pub enqueued_span: Option<SpanId>,
    /// Why the last attempt failed, if it did.
    pub last_error: Option<String>,
}

impl Job {
    pub fn new(run: RunId, kind: impl Into<String>, due_at: DateTime<Utc>) -> Self {
        Self {
            id: JobId::new(),
            run,
            kind: kind.into(),
            payload: serde_json::Value::Null,
            state: JobState::Scheduled,
            attempt: 0,
            max_attempts: 3,
            due_at,
            backoff_ms: vec![1_000, 5_000, 30_000],
            enqueued_trace: None,
            enqueued_span: None,
            last_error: None,
        }
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    pub fn with_backoff(mut self, backoff_ms: Vec<u64>) -> Self {
        self.backoff_ms = backoff_ms;
        self
    }

    /// Records the trace context the job was enqueued under, so the execution
    /// span can link back to it (T10).
    pub fn with_trace(mut self, trace: TraceId, span: SpanId) -> Self {
        self.enqueued_trace = Some(trace);
        self.enqueued_span = Some(span);
        self
    }

    /// Whether another attempt is permitted after the one just failed.
    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_attempts
    }

    /// Virtual delay before the next attempt. Indexes by the attempt just made,
    /// repeating the final entry once the list runs out — so a job with more
    /// attempts than backoff entries still slows down rather than hot-looping.
    pub fn next_backoff_ms(&self) -> u64 {
        if self.backoff_ms.is_empty() {
            return 0;
        }
        let index = (self.attempt.saturating_sub(1)) as usize;
        *self
            .backoff_ms
            .get(index)
            .unwrap_or_else(|| self.backoff_ms.last().expect("checked non-empty"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> Job {
        Job::new(RunId::new(), "noop", Utc::now())
    }

    #[test]
    fn backoff_walks_the_list_then_holds_at_the_last_entry() {
        let mut j = job()
            .with_backoff(vec![100, 200, 300])
            .with_max_attempts(10);

        j.attempt = 1;
        assert_eq!(j.next_backoff_ms(), 100);
        j.attempt = 2;
        assert_eq!(j.next_backoff_ms(), 200);
        j.attempt = 3;
        assert_eq!(j.next_backoff_ms(), 300);
        // Past the end: hold, never hot-loop.
        j.attempt = 9;
        assert_eq!(j.next_backoff_ms(), 300);
    }

    #[test]
    fn an_empty_backoff_retries_immediately() {
        let mut j = job().with_backoff(vec![]);
        j.attempt = 1;
        assert_eq!(j.next_backoff_ms(), 0);
    }

    #[test]
    fn retries_stop_at_max_attempts() {
        let mut j = job().with_max_attempts(3);

        j.attempt = 2;
        assert!(j.can_retry());
        j.attempt = 3;
        assert!(!j.can_retry(), "a 4th attempt would exceed max_attempts");
    }

    #[test]
    fn a_job_starts_scheduled_and_unattempted() {
        let j = job();
        assert_eq!(j.state, JobState::Scheduled);
        assert_eq!(j.attempt, 0);
        assert!(j.last_error.is_none());
    }
}
