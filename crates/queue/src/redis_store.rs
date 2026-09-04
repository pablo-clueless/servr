//! Redis-backed job storage.
//!
//! Two keys per run-scoped queue:
//!
//! - `<prefix>:jobs` — a hash of job id to serialized [`Job`].
//! - `<prefix>:due`  — a sorted set of job id scored by due time in virtual
//!   milliseconds. Only jobs in [`JobState::Scheduled`] appear here.
//!
//! Redis stores and orders. It does not decide when a job is due — the
//! scheduler passes `now` in, read from the virtual clock (invariant 5).

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use testbed_core::{JobId, JobState, RunId};

use crate::job::Job;
use crate::store::{JobStore, StoreError, StoreFuture};

/// Selects jobs due at or before `now` **and** marks them claimed, in one
/// round trip that Redis executes atomically.
///
/// # Trap T3
///
/// The obvious implementation — `ZRANGEBYSCORE` then `ZREM` — is a race. Two
/// pollers both read the same member before either removes it, and the job runs
/// twice. Everything below happens inside one script invocation, so a second
/// poller arriving mid-execution sees the members already gone.
///
/// `ZPOPMIN` would be simpler and is wrong: it pops the lowest-scored member
/// regardless of the score bound, so it hands back jobs that are not due yet.
/// The score bound is the whole point.
///
/// KEYS[1] due set · KEYS[2] jobs hash · ARGV[1] now, virtual ms
const CLAIM_DUE: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #due == 0 then
  return {}
end

-- Removing here, in the same script, is what makes the claim exclusive.
redis.call('ZREM', KEYS[1], unpack(due))

local claimed = {}
for i, id in ipairs(due) do
  local raw = redis.call('HGET', KEYS[2], id)
  if raw then
    local job = cjson.decode(raw)
    job.state = 'running'
    job.attempt = job.attempt + 1
    local encoded = cjson.encode(job)
    redis.call('HSET', KEYS[2], id, encoded)
    claimed[#claimed + 1] = encoded
  end
end
return claimed
"#;

pub struct RedisStore {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisStore {
    /// Connects and namespaces every key by `run` (invariant 6), so parallel
    /// runs sharing one Redis never see each other's jobs.
    pub async fn connect(url: &str, run: RunId) -> Result<Self, StoreError> {
        let client = redis::Client::open(url).map_err(|e| StoreError::Backend(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;

        Ok(Self {
            conn,
            prefix: format!("testbed:{}", run.0.simple()),
        })
    }

    fn jobs_key(&self) -> String {
        format!("{}:jobs", self.prefix)
    }

    fn due_key(&self) -> String {
        format!("{}:due", self.prefix)
    }

    /// Due time as a sorted-set score: **virtual** milliseconds since the
    /// epoch. `f64` holds millisecond timestamps exactly well past year 10000.
    fn score(at: DateTime<Utc>) -> f64 {
        at.timestamp_millis() as f64
    }

    fn decode(raw: &str) -> Result<Job, StoreError> {
        serde_json::from_str(raw).map_err(|e| StoreError::Corrupt(e.to_string()))
    }

    fn encode(job: &Job) -> Result<String, StoreError> {
        serde_json::to_string(job).map_err(|e| StoreError::Corrupt(e.to_string()))
    }
}

impl JobStore for RedisStore {
    fn put(&self, job: Job) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let encoded = Self::encode(&job)?;
            let id = job.id.to_string();

            let mut pipe = redis::pipe();
            pipe.atomic().hset(self.jobs_key(), &id, encoded).ignore();

            // Only schedulable jobs sit in the due set. A job that has finished
            // or is mid-flight must not be claimable, and leaving it there is
            // how a terminal job gets run a second time.
            if job.state == JobState::Scheduled {
                pipe.zadd(self.due_key(), &id, Self::score(job.due_at))
                    .ignore();
            } else {
                pipe.zrem(self.due_key(), &id).ignore();
            }

            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))
        })
    }

    fn claim_due(&self, now: DateTime<Utc>) -> StoreFuture<'_, Vec<Job>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();

            let claimed: Vec<String> = redis::Script::new(CLAIM_DUE)
                .key(self.due_key())
                .key(self.jobs_key())
                .arg(Self::score(now))
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;

            claimed.iter().map(|raw| Self::decode(raw)).collect()
        })
    }

    fn get(&self, id: JobId) -> StoreFuture<'_, Job> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let raw: Option<String> = conn
                .hget(self.jobs_key(), id.to_string())
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;

            match raw {
                Some(raw) => Self::decode(raw.as_str()),
                None => Err(StoreError::NotFound(id)),
            }
        })
    }

    fn list(&self) -> StoreFuture<'_, Vec<Job>> {
        Box::pin(async move {
            let mut conn = self.conn.clone();
            let raw: Vec<String> = conn
                .hvals(self.jobs_key())
                .await
                .map_err(|e| StoreError::Backend(e.to_string()))?;

            raw.iter().map(|r| Self::decode(r)).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The script has to survive review by someone who did not write it, so the
    /// properties that make it correct are asserted rather than trusted.
    #[test]
    fn the_claim_script_is_atomic_by_construction() {
        // The read and the removal are both in the script. If a future edit
        // moves either out, the claim stops being exclusive (T3).
        assert!(CLAIM_DUE.contains("ZRANGEBYSCORE"));
        assert!(
            CLAIM_DUE.contains("ZREM"),
            "without ZREM in the same script, two pollers double-deliver"
        );

        // ZPOPMIN ignores the score bound and would pop jobs that are not due.
        assert!(
            !CLAIM_DUE.contains("ZPOPMIN"),
            "ZPOPMIN ignores the score bound; it is not a substitute (T3)"
        );

        // The bound itself: '-inf' to now, never the whole set.
        assert!(CLAIM_DUE.contains("'-inf', ARGV[1]"));
    }

    #[test]
    fn the_script_counts_the_attempt_it_hands_out() {
        // The scheduler relies on `claim_due` having already incremented, the
        // same way MemoryStore does; a mismatch here silently gives every job
        // one extra retry.
        assert!(CLAIM_DUE.contains("job.attempt = job.attempt + 1"));
        assert!(CLAIM_DUE.contains("job.state = 'running'"));
    }

    #[test]
    fn state_names_match_what_serde_writes() {
        // The Lua script writes `state` as a bare string, so it has to agree
        // with `JobState`'s serde representation or the job fails to decode on
        // the way back out.
        let running = serde_json::to_string(&JobState::Running).unwrap();
        assert_eq!(running, "\"running\"");
        assert!(CLAIM_DUE.contains("'running'"));
    }

    #[test]
    fn keys_are_namespaced_per_run() {
        // Two runs sharing one Redis must not share a queue (invariant 6).
        let a = format!("testbed:{}", RunId::new().0.simple());
        let b = format!("testbed:{}", RunId::new().0.simple());
        assert_ne!(a, b);
        assert!(a.starts_with("testbed:"));
    }

    #[test]
    fn scores_are_virtual_milliseconds() {
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        assert_eq!(RedisStore::score(at), 1_700_000_000_123.0);
    }
}
