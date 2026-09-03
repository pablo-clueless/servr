//! Job storage.
//!
//! # Redis is storage, never a scheduler
//!
//! HANDOFF §5 invariant 5. Redis holds jobs and hands back the ones that are
//! due; it never decides *when* due means. That decision belongs to the poll
//! loop in [`crate::scheduler`], comparing against the virtual clock — which is
//! the only reason `clock/advance` can make a 30-minute job run instantly.
//!
//! # Trap T3 — the poll must be atomic
//!
//! `claim_due` is a single atomic operation, not a read followed by a write.
//! `ZRANGEBYSCORE` then `ZREM` is a race: two pollers both see the job and both
//! deliver it. The Redis implementation does both halves in one Lua script.
//! `ZPOPMIN` is *not* a substitute — it ignores the score bound, so it pops
//! jobs that are not due yet.
//!
//! The trait exists so the scheduler, retries and DLQ are all testable without
//! Redis running; [`MemoryStore`] is the in-process implementation and is what
//! the timing gate runs against.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use testbed_core::{JobId, JobState};

use crate::job::Job;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job {0} not found")]
    NotFound(JobId),
    #[error("storage backend: {0}")]
    Backend(String),
}

pub trait JobStore: Send + Sync + 'static {
    /// Adds a job. Overwrites any job with the same id.
    fn put(&self, job: Job) -> Result<(), StoreError>;

    /// Atomically claims every job due at or before `now`, marking each
    /// [`JobState::Running`] in the same operation.
    ///
    /// Atomicity is the contract, not an implementation detail: two schedulers
    /// polling the same store must never both receive the same job.
    fn claim_due(&self, now: DateTime<Utc>) -> Result<Vec<Job>, StoreError>;

    fn get(&self, id: JobId) -> Result<Job, StoreError>;

    fn list(&self) -> Result<Vec<Job>, StoreError>;

    /// Jobs not yet in a terminal state. Backs `testbed_queue_depth`.
    fn depth(&self) -> Result<usize, StoreError> {
        Ok(self
            .list()?
            .iter()
            .filter(|j| !j.state.is_terminal())
            .count())
    }
}

/// In-process store. The default, and what the Phase 4 gate runs against.
#[derive(Default)]
pub struct MemoryStore {
    jobs: Mutex<HashMap<JobId, Job>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobStore for MemoryStore {
    fn put(&self, job: Job) -> Result<(), StoreError> {
        self.lock().insert(job.id, job);
        Ok(())
    }

    fn claim_due(&self, now: DateTime<Utc>) -> Result<Vec<Job>, StoreError> {
        // One lock covers both the selection and the state change, which is
        // this implementation's answer to T3.
        let mut jobs = self.lock();

        let due: Vec<JobId> = jobs
            .values()
            .filter(|j| j.state == JobState::Scheduled && j.due_at <= now)
            .map(|j| j.id)
            .collect();

        Ok(due
            .into_iter()
            .filter_map(|id| {
                let job = jobs.get_mut(&id)?;
                job.state = JobState::Running;
                job.attempt += 1;
                Some(job.clone())
            })
            .collect())
    }

    fn get(&self, id: JobId) -> Result<Job, StoreError> {
        self.lock()
            .get(&id)
            .cloned()
            .ok_or(StoreError::NotFound(id))
    }

    fn list(&self) -> Result<Vec<Job>, StoreError> {
        Ok(self.lock().values().cloned().collect())
    }
}

impl MemoryStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<JobId, Job>> {
        self.jobs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeDelta;
    use testbed_core::RunId;

    use super::*;

    fn at(offset_secs: i64) -> DateTime<Utc> {
        Utc::now() + TimeDelta::seconds(offset_secs)
    }

    #[test]
    fn only_jobs_that_are_due_are_claimed() {
        let store = MemoryStore::new();
        let run = RunId::new();

        let ready = Job::new(run, "ready", at(-10));
        let later = Job::new(run, "later", at(600));
        store.put(ready.clone()).unwrap();
        store.put(later.clone()).unwrap();

        let claimed = store.claim_due(Utc::now()).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, ready.id);

        // The one that is not due must still be Scheduled — this is what
        // ZPOPMIN would get wrong.
        assert_eq!(store.get(later.id).unwrap().state, JobState::Scheduled);
    }

    #[test]
    fn claiming_marks_running_and_counts_the_attempt() {
        let store = MemoryStore::new();
        let job = Job::new(RunId::new(), "noop", at(-1));
        store.put(job.clone()).unwrap();

        let claimed = store.claim_due(Utc::now()).unwrap();
        assert_eq!(claimed[0].state, JobState::Running);
        assert_eq!(claimed[0].attempt, 1);
    }

    /// T3: a second poll must not re-deliver a job the first one took.
    #[test]
    fn a_job_is_claimed_exactly_once() {
        let store = MemoryStore::new();
        store.put(Job::new(RunId::new(), "noop", at(-1))).unwrap();

        assert_eq!(store.claim_due(Utc::now()).unwrap().len(), 1);
        assert_eq!(
            store.claim_due(Utc::now()).unwrap().len(),
            0,
            "the same job was delivered twice"
        );
    }

    /// The same, under real contention rather than in sequence.
    #[test]
    fn concurrent_pollers_never_double_deliver() {
        let store = Arc::new(MemoryStore::new());
        let run = RunId::new();

        for i in 0..100 {
            store.put(Job::new(run, format!("j{i}"), at(-1))).unwrap();
        }

        let claimed: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let store = Arc::clone(&store);
                    scope.spawn(move || store.claim_due(Utc::now()).unwrap().len())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });

        assert_eq!(
            claimed, 100,
            "8 pollers claimed {claimed} of 100 jobs; claims are not atomic"
        );
    }

    #[test]
    fn depth_counts_only_unfinished_jobs() {
        let store = MemoryStore::new();
        let run = RunId::new();

        let mut done = Job::new(run, "done", at(0));
        done.state = JobState::Succeeded;
        let mut dead = Job::new(run, "dead", at(0));
        dead.state = JobState::Dead;

        store.put(Job::new(run, "waiting", at(60))).unwrap();
        store.put(done).unwrap();
        store.put(dead).unwrap();

        assert_eq!(store.depth().unwrap(), 1);
    }
}
