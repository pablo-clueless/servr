//! Job registry, scheduler, retries, dead-letter queue.
//!
//! # Redis is storage, never a scheduler
//!
//! HANDOFF §5 invariant 5. The scheduler is our own poll loop comparing due
//! times against the virtual clock ([`scheduler`]); Redis only holds jobs and
//! hands back the ones that are due ([`store`]).
//!
//! Two [`store::JobStore`] implementations: [`store::MemoryStore`] for
//! single-process use and tests, and [`redis_store::RedisStore`] for state that
//! survives a restart. Everything above the trait is identical either way.

pub mod job;
pub mod redis_store;
pub mod scheduler;
pub mod store;

pub use job::Job;
pub use redis_store::RedisStore;
pub use scheduler::{Scheduler, TICK};
pub use store::{JobStore, MemoryStore, StoreError};
