//! Webhooks, both directions: an inbound capture inbox and an outbound sender.
//!
//! # Q4 — resolved
//!
//! Operator decision: support both signing schemes, selected per endpoint in
//! scenario config. See [`testbed_core::SigningScheme`].
//!
//! # Not yet built — Phase 7 (HANDOFF §9 task 13)
//!
//! - `POST /hooks/in/{id}` capturing headers and body, readable at
//!   `/_admin/hooks/in/{id}`
//! - an outbound sender with signing, retries on virtual-time backoff, and an
//!   injected `traceparent` on every attempt (§5 invariant 10)
//! - `EventKind::WebhookIn` / `EventKind::WebhookOut`
//!
//! Trap T1 applies to both routes: axum 0.8 spells the param `{id}`.

/// axum 0.8 param syntax (T1).
pub const INBOUND_ROUTE: &str = "/hooks/in/{id}";
