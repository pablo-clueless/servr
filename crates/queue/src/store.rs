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
//! [`JobStore::claim_due`] is one atomic operation, not a read followed by a
//! write. `ZRANGEBYSCORE` then `ZREM` is a race: two pollers both see the job
//! and both deliver it. [`crate::redis_store::RedisStore`] does both halves in a
//! single Lua script. `ZPOPMIN` is *not* a substitute — it ignores the score
//! bound, so it pops jobs that are not due yet.
//!
//! # Why the trait is async
//!
//! Redis I/O cannot be synchronous inside the reactor. The methods return boxed
//! futures rather than using `async fn` because the scheduler holds a
//! `dyn JobStore`, and `async fn` in a trait is not dyn-compatible.
//!
//! [`MemoryStore`] is the in-process implementation and what the Phase 4 timing
//! gate runs against; the trait exists so swapping in Redis changes nothing
//! above it.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use testbed_core::{JobId, JobState};

use crate::job::Job;

/// A future returned by a store method.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job {0} not found")]
    NotFound(JobId),
    #[error("storage backend: {0}")]
    Backend(String),
    #[error("stored job is not readable: {0}")]
    Corrupt(String),
}

pub trait JobStore: Send + Sync + 'static {
    /// Adds a job. Overwrites any job with the same id.
    fn put(&self, job: Job) -> StoreFuture<'_, ()>;

    /// Atomically claims every job due at or before `now`, marking each
    /// [`JobState::Running`] in the same operation.
    ///
    /// Atomicity is the contract, not an implementation detail: two schedulers
    /// polling the same store must never both receive the same job.
    fn claim_due(&self, now: DateTime<Utc>) -> StoreFuture<'_, Vec<Job>>;

    fn get(&self, id: JobId) -> StoreFuture<'_, Job>;

    fn list(&self) -> StoreFuture<'_, Vec<Job>>;

    /// Jobs not yet in a terminal state. Backs `testbed_queue_depth`.
    fn depth(&self) -> StoreFuture<'_, usize> {
        Box::pin(async move {
            Ok(self
                .list()
                .await?
                .iter()
                .filter(|j| !j.state.is_terminal())
                .count())
        })
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

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<JobId, Job>> {
        self.jobs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl JobStore for MemoryStore {
    fn put(&self, job: Job) -> StoreFuture<'_, ()> {
        self.lock().insert(job.id, job);
        Box::pin(std::future::ready(Ok(())))
    }

    fn claim_due(&self, now: DateTime<Utc>) -> StoreFuture<'_, Vec<Job>> {
        // One lock covers both the selection and the state change, which is
        // this implementation's answer to T3.
        let mut jobs = self.lock();

        let due: Vec<JobId> = jobs
            .values()
            .filter(|j| j.state == JobState::Scheduled && j.due_at <= now)
            .map(|j| j.id)
            .collect();

        let claimed = due
            .into_iter()
            .filter_map(|id| {
                let job = jobs.get_mut(&id)?;
                job.state = JobState::Running;
                job.attempt += 1;
                Some(job.clone())
            })
            .collect();

        Box::pin(std::future::ready(Ok(claimed)))
    }

    fn get(&self, id: JobId) -> StoreFuture<'_, Job> {
        let found = self.lock().get(&id).cloned();
        Box::pin(std::future::ready(found.ok_or(StoreError::NotFound(id))))
    }

    fn list(&self) -> StoreFuture<'_, Vec<Job>> {
        let all = self.lock().values().cloned().collect();
        Box::pin(std::future::ready(Ok(all)))
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

    #[tokio::test]
    async fn only_jobs_that_are_due_are_claimed() {
        let store = MemoryStore::new();
        let run = RunId::new();

        let ready = Job::new(run, "ready", at(-10));
        let later = Job::new(run, "later", at(600));
        store.put(ready.clone()).await.unwrap();
        store.put(later.clone()).await.unwrap();

        let claimed = store.claim_due(Utc::now()).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, ready.id);

        // The one that is not due must still be Scheduled — this is what
        // ZPOPMIN would get wrong.
        assert_eq!(
            store.get(later.id).await.unwrap().state,
            JobState::Scheduled
        );
    }

    #[tokio::test]
    async fn claiming_marks_running_and_counts_the_attempt() {
        let store = MemoryStore::new();
        let job = Job::new(RunId::new(), "noop", at(-1));
        store.put(job.clone()).await.unwrap();

        let claimed = store.claim_due(Utc::now()).await.unwrap();
        assert_eq!(claimed[0].state, JobState::Running);
        assert_eq!(claimed[0].attempt, 1);
    }

    /// T3: a second poll must not re-deliver a job the first one took.
    #[tokio::test]
    async fn a_job_is_claimed_exactly_once() {
        let store = MemoryStore::new();
        store
            .put(Job::new(RunId::new(), "noop", at(-1)))
            .await
            .unwrap();

        assert_eq!(store.claim_due(Utc::now()).await.unwrap().len(), 1);
        assert_eq!(
            store.claim_due(Utc::now()).await.unwrap().len(),
            0,
            "the same job was delivered twice"
        );
    }

    /// The same, under real contention rather than in sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_pollers_never_double_deliver() {
        let store = Arc::new(MemoryStore::new());
        let run = RunId::new();

        for i in 0..100 {
            store
                .put(Job::new(run, format!("j{i}"), at(-1)))
                .await
                .unwrap();
        }

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            tasks.spawn(async move { store.claim_due(Utc::now()).await.unwrap().len() });
        }

        let mut claimed = 0;
        while let Some(result) = tasks.join_next().await {
            claimed += result.unwrap();
        }

        assert_eq!(
            claimed, 100,
            "8 pollers claimed {claimed} of 100 jobs; claims are not atomic"
        );
    }

    #[tokio::test]
    async fn depth_counts_only_unfinished_jobs() {
        let store = MemoryStore::new();
        let run = RunId::new();

        let mut done = Job::new(run, "done", at(0));
        done.state = JobState::Succeeded;
        let mut dead = Job::new(run, "dead", at(0));
        dead.state = JobState::Dead;

        store.put(Job::new(run, "waiting", at(60))).await.unwrap();
        store.put(done).await.unwrap();
        store.put(dead).await.unwrap();

        assert_eq!(store.depth().await.unwrap(), 1);
    }
}
