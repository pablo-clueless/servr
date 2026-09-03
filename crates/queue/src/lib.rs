//! Job registry, scheduler, retries, dead-letter queue.
//!
//! # Redis is storage, never a scheduler
//!
//! HANDOFF §5 invariant 5. The scheduler is our own poll loop comparing due
//! times against the virtual clock ([`scheduler`]); Redis only holds jobs and
//! hands back the ones that are due ([`store`]).
//!
//! # Still owed
//!
//! The Redis [`store::JobStore`] implementation, with the Lua script that makes
//! `claim_due` atomic (T3). [`store::MemoryStore`] is the working
//! implementation today and is what the Phase 4 timing gate runs against; the
//! trait exists so swapping in Redis changes nothing above it.

pub mod job;
pub mod scheduler;
pub mod store;

pub use job::Job;
pub use scheduler::{Scheduler, TICK};
pub use store::{JobStore, MemoryStore, StoreError};
