//! Mailpit client facade.
//!
//! Mailpit owns SMTP *and* provides the read API (HANDOFF §2 decision 6). This
//! crate is a thin client over both and nothing more. Do not add
//! `mailin-embedded`, or any other embedded SMTP server.
//!
//! # Trap T7, and what was measured
//!
//! Mailpit does not namespace. There is no per-run inbox, so setting
//! `X-Testbed-Run` on every send and filtering on it on every read is the
//! entire isolation story — miss it in one place and runs read each other's
//! mail.
//!
//! Two things about Mailpit v1.31 make that concrete, both verified against a
//! live container on 2026-09-04 rather than assumed:
//!
//! 1. **The message summary carries no custom headers.** `/api/v1/messages` and
//!    `/api/v1/search` return `From`/`To`/`Subject`/`Snippet` and nothing else,
//!    so a run cannot be identified from a listing alone.
//! 2. **Search does not index custom headers.** Querying either the bare run id
//!    or `X-Testbed-Run:<id>` returns `messages_count: 0` for a message that
//!    demonstrably carries the header.
//!
//! Together those rule out the obvious implementation — pushing the run id into
//! the Mailpit query and letting the server filter. Isolation has to be a
//! client-side filter over headers fetched per message, which is what
//! [`inbox`] does. It is an N+1, deliberately: a bounded one against a local
//! sink, in exchange for isolation that does not depend on another project's
//! indexing behaviour. If that ever regresses to a search filter, runs start
//! silently reading each other's mail and every mail assertion in every
//! scenario becomes meaningless.

pub mod inbox;
pub mod send;

use std::sync::Arc;

use testbed_core::{Clock, EventSink};

pub use inbox::{Inbox, Message};
pub use send::{OutgoingMail, SentMail, RUN_HEADER_NAME};

/// Mailpit's SMTP port in `compose.yaml`.
pub const SMTP_PORT: u16 = 1025;
/// Mailpit's HTTP/REST port in `compose.yaml`.
pub const HTTP_PORT: u16 = 8025;

/// Where Mailpit is.
#[derive(Debug, Clone)]
pub struct MailConfig {
    /// `host:port` for SMTP.
    pub smtp: String,
    /// Base URL for the REST API, no trailing slash.
    pub api: String,
}

impl Default for MailConfig {
    fn default() -> Self {
        Self {
            smtp: format!("localhost:{SMTP_PORT}"),
            api: format!("http://localhost:{HTTP_PORT}"),
        }
    }
}

impl MailConfig {
    /// Reads `MAILPIT_SMTP` and `MAILPIT_API`, falling back to the ports
    /// `compose.yaml` publishes.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            smtp: std::env::var("MAILPIT_SMTP").unwrap_or(default.smtp),
            api: std::env::var("MAILPIT_API").unwrap_or(default.api),
        }
    }

    /// Splits [`Self::smtp`] into host and port.
    pub fn smtp_parts(&self) -> Result<(String, u16), MailError> {
        match self.smtp.rsplit_once(':') {
            Some((host, port)) => {
                let port = port
                    .parse()
                    .map_err(|_| MailError::Config(format!("{:?} has no valid port", self.smtp)))?;
                Ok((host.to_string(), port))
            }
            // A bare host is a reasonable thing to write; assume the standard port.
            None => Ok((self.smtp.clone(), SMTP_PORT)),
        }
    }
}

/// Sends through Mailpit's SMTP and reads back through its REST API.
pub struct Mailer {
    sender: send::Sender,
    inbox: Inbox,
    bus: Arc<dyn EventSink>,
    clock: Arc<Clock>,
    run: testbed_core::RunId,
}

impl Mailer {
    /// Builds the client. Does **not** connect: lettre's transport is lazy and
    /// Mailpit may come up after the testbed does.
    ///
    /// Use [`Mailer::probe`] to decide whether the mail routes should answer or
    /// 503 — the same shape `DataPlane` uses for Postgres.
    pub fn new(
        config: MailConfig,
        bus: Arc<dyn EventSink>,
        clock: Arc<Clock>,
        run: testbed_core::RunId,
    ) -> Result<Self, MailError> {
        Ok(Self {
            sender: send::Sender::new(&config)?,
            inbox: Inbox::new(&config)?,
            bus,
            clock,
            run,
        })
    }

    /// Asks Mailpit for its version. Cheap liveness check for boot.
    pub async fn probe(&self) -> Result<String, MailError> {
        self.inbox.version().await
    }

    pub fn inbox(&self) -> &Inbox {
        &self.inbox
    }

    /// Sends `mail` tagged with `run`, then records it on both surfaces.
    ///
    /// Invariant 4: the send is a bus event *and* a span. Invariant 7: the
    /// `X-Testbed-Run` header is applied here and nowhere else, so there is one
    /// place to get it wrong rather than one per call site.
    pub async fn send(
        &self,
        run: testbed_core::RunId,
        mail: OutgoingMail,
    ) -> Result<SentMail, MailError> {
        let span = tracing::info_span!(
            "mail.send",
            otel.name = "mail send",
            testbed.mail.to = %mail.to,
            testbed.mail.run = %run,
            testbed.mail.message_id = tracing::field::Empty,
        );

        let sent = {
            use tracing::Instrument;
            self.sender
                .send(run, &mail)
                .instrument(span.clone())
                .await?
        };
        span.record("testbed.mail.message_id", sent.message_id.as_str());

        // Entered so the event carries this span's ids and the two surfaces
        // join (invariant 9). Nothing awaits inside.
        let _entered = span.enter();
        let (trace_id, span_id) = match testbed_telemetry::propagation::current_ids() {
            Some((t, s)) => (Some(t), Some(s)),
            None => (None, None),
        };

        self.bus.emit(testbed_core::Event {
            id: 0,
            // The event is stamped with the *process* run, while the mail
            // carries the run it was sent as. They differ whenever a harness
            // drives several runs through one server, and conflating them would
            // make `/_admin/events` disagree with the inbox.
            run: self.run,
            at: self.clock.now(),
            wall_at: Clock::wall_now(),
            trace_id,
            span_id,
            kind: testbed_core::EventKind::MailSent {
                to: mail.to.clone(),
                subject: mail.subject.clone(),
                message_id: sent.message_id.clone(),
            },
        });

        Ok(sent)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MailError {
    #[error("mail configuration: {0}")]
    Config(String),
    #[error("building the message failed: {0}")]
    Build(String),
    #[error("SMTP send to Mailpit failed: {0}")]
    Smtp(String),
    #[error("Mailpit REST API unreachable: {0}")]
    Api(String),
    #[error("Mailpit answered {status} for {path}")]
    Status { status: u16, path: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smtp_parts_splits_host_and_port() {
        let config = MailConfig {
            smtp: "mailpit:2525".into(),
            ..Default::default()
        };
        assert_eq!(config.smtp_parts().unwrap(), ("mailpit".into(), 2525));
    }

    #[test]
    fn a_bare_host_assumes_the_compose_port() {
        let config = MailConfig {
            smtp: "mailpit".into(),
            ..Default::default()
        };
        assert_eq!(config.smtp_parts().unwrap(), ("mailpit".into(), SMTP_PORT));
    }

    #[test]
    fn a_non_numeric_port_is_rejected_rather_than_silently_defaulted() {
        let config = MailConfig {
            smtp: "mailpit:smtp".into(),
            ..Default::default()
        };
        assert!(config.smtp_parts().is_err());
    }

    #[test]
    fn the_defaults_match_the_ports_compose_publishes() {
        let config = MailConfig::default();
        assert_eq!(config.smtp_parts().unwrap().1, SMTP_PORT);
        assert!(config.api.ends_with(&HTTP_PORT.to_string()));
    }
}
