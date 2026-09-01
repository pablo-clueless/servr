//! Control-plane primitives shared by every surface.
//!
//! Three rules govern this crate (HANDOFF §4, §5):
//!
//! 1. It depends on no other workspace crate.
//! 2. It performs no Postgres I/O, ever — it must survive a full data-plane wipe.
//! 3. [`clock`] is one of only two files permitted to read wall time. Everything
//!    that schedules reads [`Clock::now`].

pub mod bus;
pub mod clock;
pub mod config;
pub mod event;
pub mod fault;
pub mod run;
pub mod trace;

pub use bus::EventSink;
pub use clock::Clock;
pub use config::{Overlay, Resolved, Scenario};
pub use event::{ConnId, Dir, Event, EventKind, JobId, JobState, StreamId};
pub use fault::{FaultSpec, SigningScheme, TelemetryFault, WebhookEndpoint};
pub use run::RunId;
pub use trace::{SpanId, TraceId};
