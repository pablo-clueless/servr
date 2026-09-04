//! `RedisStore` against a live Redis.
//!
//! **Skips itself unless `REDIS_URL` is set**, the same way the Phase 3
//! isolation tests skip without `DATABASE_URL`: it runs automatically the
//! moment infra is available rather than needing a remembered flag.
//!
//! ```text
//! docker compose up -d --wait
//! REDIS_URL=redis://localhost:6379 cargo test -p testbed-queue --test redis_store
//! ```
//!
//! The unit tests in `redis_store.rs` assert the Lua script's *shape*. These
//! assert its behaviour, which is the half that matters for T3.

use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use testbed_core::{JobState, RunId};
use testbed_queue::{Job, JobStore, RedisStore};

macro_rules! require_redis {
    () => {
        match std::env::var("REDIS_URL").ok().filter(|u| !u.is_empty()) {
            Some(url) => url,
            None => {
                eprintln!("skipping: REDIS_URL unset (start Redis and re-run)");
                return;
            }
        }
    };
}

/// Each test gets its own run, so they never collide even run in parallel —
/// which is also the isolation property invariant 6 asks for.
async fn store() -> RedisStore {
    let url = std::env::var("REDIS_URL").unwrap();
    RedisStore::connect(&url, RunId::new())
        .await
        .expect("REDIS_URL is set but Redis is unreachable")
}

#[tokio::test]
async fn a_job_round_trips() {
    let _url = require_redis!();
    let store = store().await;
    let run = RunId::new();

    let job = Job::new(run, "noop", Utc::now()).with_payload(serde_json::json!({"x": 1}));
    let id = job.id;
    store.put(job.clone()).await.unwrap();

    let loaded = store.get(id).await.unwrap();
    assert_eq!(loaded, job, "the job did not survive serialization");
}

#[tokio::test]
async fn only_jobs_that_are_due_are_claimed() {
    let _url = require_redis!();
    let store = store().await;
    let run = RunId::new();

    let ready = Job::new(run, "ready", Utc::now() - TimeDelta::seconds(10));
    let later = Job::new(run, "later", Utc::now() + TimeDelta::seconds(600));
    store.put(ready.clone()).await.unwrap();
    store.put(later.clone()).await.unwrap();

    let claimed = store.claim_due(Utc::now()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, ready.id);

    // What ZPOPMIN would get wrong: it would have taken this one too.
    assert_eq!(
        store.get(later.id).await.unwrap().state,
        JobState::Scheduled
    );
}

#[tokio::test]
async fn claiming_marks_running_and_counts_the_attempt() {
    let _url = require_redis!();
    let store = store().await;

    let job = Job::new(RunId::new(), "noop", Utc::now() - TimeDelta::seconds(1));
    let id = job.id;
    store.put(job).await.unwrap();

    let claimed = store.claim_due(Utc::now()).await.unwrap();
    assert_eq!(claimed[0].state, JobState::Running);
    assert_eq!(claimed[0].attempt, 1);

    // The increment is persisted, not just returned.
    assert_eq!(store.get(id).await.unwrap().attempt, 1);
}

/// T3, the real assertion: concurrent pollers against one Redis must never
/// both receive the same job. This is what the Lua script exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_pollers_never_double_deliver() {
    let _url = require_redis!();
    let store = Arc::new(store().await);
    let run = RunId::new();

    for i in 0..100 {
        store
            .put(Job::new(
                run,
                format!("j{i}"),
                Utc::now() - TimeDelta::seconds(1),
            ))
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
        "8 pollers claimed {claimed} of 100 jobs; the Lua claim is not atomic (T3)"
    );
}

/// A terminal job must leave the due set, or it is claimed again forever.
#[tokio::test]
async fn a_finished_job_is_not_claimable() {
    let _url = require_redis!();
    let store = store().await;

    let mut job = Job::new(RunId::new(), "noop", Utc::now() - TimeDelta::seconds(1));
    job.state = JobState::Succeeded;
    store.put(job).await.unwrap();

    assert!(
        store.claim_due(Utc::now()).await.unwrap().is_empty(),
        "a succeeded job was handed back to the scheduler"
    );
}

/// Two runs sharing one Redis must not see each other's jobs (invariant 6).
#[tokio::test]
async fn runs_do_not_share_a_queue() {
    let url = require_redis!();

    let run_a = RunId::new();
    let run_b = RunId::new();
    let store_a = RedisStore::connect(&url, run_a).await.unwrap();
    let store_b = RedisStore::connect(&url, run_b).await.unwrap();

    store_a
        .put(Job::new(run_a, "noop", Utc::now() - TimeDelta::seconds(1)))
        .await
        .unwrap();

    assert_eq!(store_a.claim_due(Utc::now()).await.unwrap().len(), 1);
    assert_eq!(
        store_b.claim_due(Utc::now()).await.unwrap().len(),
        0,
        "run B claimed run A's job"
    );
}
