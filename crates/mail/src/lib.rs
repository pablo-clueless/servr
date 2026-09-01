//! Mailpit client facade.
//!
//! Mailpit owns SMTP *and* provides the read API (HANDOFF §2 decision 6). This
//! crate is a thin client over both and nothing more. Do not add
//! `mailin-embedded`, or any other embedded SMTP server.
//!
//! # Not yet built — Phase 6 (HANDOFF §9 task 12)
//!
//! - send over SMTP to Mailpit on 1025
//! - read back over Mailpit's REST API on 8025, filtered by run
//! - an `EventKind::MailSent` per message
//!
//! Trap T7: Mailpit does not namespace. There is no per-run inbox. Setting
//! `X-Testbed-Run` on every send and filtering on it on every read is the
//! entire isolation story — miss it in one place and runs read each other's mail.

/// Mailpit's SMTP port in `compose.yaml`.
pub const SMTP_PORT: u16 = 1025;
/// Mailpit's HTTP/REST port in `compose.yaml`.
pub const HTTP_PORT: u16 = 8025;
