//! Job registry, scheduler, retries, dead-letter queue.
//!
//! # Not yet built — Phase 4 (HANDOFF §9 task 10)
//!
//! - a poll loop comparing due times against **virtual** now
//! - retry with backoff, and a DLQ for exhausted jobs
//! - an `EventKind::JobTransition` per state change
//!
//! Redis is storage, never the scheduler (§5 invariant 5).
//!
//! Trap T3: the poll must be atomic. `ZRANGEBYSCORE` then `ZREM` is a race and
//! two pollers will double-deliver; do both in one Lua script. `ZPOPMIN` is not
//! a substitute — it ignores the score bound, so it pops jobs that are not due.
//!
//! Trap T10: a job's execution span **links** to the enqueue span, it does not
//! descend from it. Parenting is the intuitive choice and it is wrong: a job
//! delayed 30 minutes then produces a 30-minute trace, and a handful of those
//! makes every trace-waterfall UI pointed at the testbed unusable. Use
//! `FOLLOWS_FROM`. The Phase 4 gate asserts it.

use testbed_core::{JobId, JobState};

/// Placeholder so the crate has a compiled surface; replaced in Phase 4.
pub fn new_job() -> (JobId, JobState) {
    (JobId::new(), JobState::Scheduled)
}
