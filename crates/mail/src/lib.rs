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

pub mod allow;
pub mod inbox;
pub mod send;

use std::sync::Arc;

use testbed_core::{Clock, EventSink};

pub use allow::Allowlist;
pub use inbox::{Inbox, Message};
pub use send::{OutgoingMail, SentMail, RUN_HEADER_NAME};

/// Mailpit's SMTP port in `compose.yaml`.
pub const SMTP_PORT: u16 = 1025;
/// Mailpit's HTTP/REST port in `compose.yaml`.
pub const HTTP_PORT: u16 = 8025;

/// An authenticated SMTP relay — Brevo, SES, Postmark, anything real.
///
/// Its presence switches the transport from Mailpit's plaintext socket to
/// STARTTLS with credentials, and switches the read side off: a relay accepts
/// mail and tells you nothing about it afterwards.
#[derive(Debug, Clone)]
pub struct Relay {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// Envelope sender. Relays reject anything from an unverified address, so
    /// this is required rather than defaulted to `testbed@localhost`.
    pub from: String,
}

/// Where mail goes, and who it may go to.
#[derive(Debug, Clone)]
pub struct MailConfig {
    /// `host:port` for Mailpit's plaintext SMTP.
    pub smtp: String,
    /// Base URL for Mailpit's REST API, no trailing slash.
    pub api: String,
    /// When set, mail is relayed for real instead of dropped into Mailpit.
    pub relay: Option<Relay>,
    /// Recipients a *relay* send may reach. Ignored in Mailpit mode, where
    /// nothing leaves the machine.
    pub allowed: Allowlist,
}

impl Default for MailConfig {
    fn default() -> Self {
        Self {
            smtp: format!("localhost:{SMTP_PORT}"),
            api: format!("http://localhost:{HTTP_PORT}"),
            relay: None,
            allowed: Allowlist::default(),
        }
    }
}

impl MailConfig {
    /// Reads `MAILPIT_SMTP` and the REST endpoint, falling back to the ports
    /// `compose.yaml` publishes.
    ///
    /// # Two names for the REST endpoint
    ///
    /// `MAILPIT_HTTP` is what `.env` and `compose.yaml` have always called it
    /// (`MAILPIT_HTTP_PORT` publishes it); `MAILPIT_API` is what this crate
    /// originally read, and what the Makefile and `render.yaml` document. Both
    /// are accepted because the mismatch was a real, silent bug: a deployment
    /// configured from `.env` set `MAILPIT_HTTP`, this read `MAILPIT_API`, got
    /// nothing, fell back to `localhost:8025`, and reported Mailpit as
    /// unreachable — with correct configuration sitting right there. The unit
    /// tests missed it because they set `MAILPIT_API` explicitly.
    pub fn from_env() -> Self {
        let default = Self::default();

        // `SMTP_HOST` is the switch. Its presence means a real relay was
        // configured, and everything else about the mail surface follows from
        // that — including that there is no inbox to read back.
        let relay = std::env::var("SMTP_HOST").ok().map(|host| Relay {
            host,
            port: std::env::var("SMTP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                // 587 (submission, STARTTLS) rather than 25: every hosted relay
                // uses it and outbound 25 is blocked on most platforms anyway.
                .unwrap_or(587),
            user: std::env::var("SMTP_USER").unwrap_or_default(),
            password: std::env::var("SMTP_PASS").unwrap_or_default(),
            from: std::env::var("MAIL_FROM")
                .or_else(|_| std::env::var("SMTP_USER"))
                .unwrap_or_else(|_| send::DEFAULT_FROM.to_string()),
        });

        Self {
            smtp: std::env::var("MAILPIT_SMTP").unwrap_or(default.smtp),
            api: std::env::var("MAILPIT_API")
                .or_else(|_| std::env::var("MAILPIT_HTTP"))
                .unwrap_or(default.api),
            relay,
            allowed: Allowlist::parse(
                &std::env::var("MAIL_ALLOWED_RECIPIENTS").unwrap_or_default(),
            ),
        }
    }

    /// Whether mail actually leaves the machine.
    pub fn is_relay(&self) -> bool {
        self.relay.is_some()
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
    config: MailConfig,
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
            config,
            bus,
            clock,
            run,
        })
    }

    /// Liveness check for boot.
    ///
    /// In Mailpit mode this asks the REST API for its version. In relay mode
    /// there is no REST API, so it opens the SMTP connection instead — which
    /// also verifies the credentials and TLS, the two things most likely to be
    /// wrong.
    pub async fn probe(&self) -> Result<String, MailError> {
        match &self.config.relay {
            Some(relay) => {
                self.sender.test_connection().await?;
                Ok(format!("relay {}:{}", relay.host, relay.port))
            }
            None => self.inbox.version().await,
        }
    }

    /// The read side. `None` in relay mode: a relay accepts mail and tells you
    /// nothing afterwards, so there is no inbox to filter by run.
    pub fn inbox(&self) -> Option<&Inbox> {
        if self.config.is_relay() {
            None
        } else {
            Some(&self.inbox)
        }
    }

    pub fn config(&self) -> &MailConfig {
        &self.config
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
        // The allowlist is checked here and nowhere else. `/_admin` is
        // unauthenticated, so in relay mode this is the only thing standing
        // between a public URL and an open mail relay — see `allow`.
        if self.config.is_relay() && !self.config.allowed.permits(&mail.to) {
            return Err(MailError::RecipientNotAllowed {
                to: mail.to.clone(),
                allowed: self.config.allowed.to_string(),
            });
        }

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
    #[error(
        "refusing to relay to {to}: not in MAIL_ALLOWED_RECIPIENTS ({allowed}).          This endpoint is unauthenticated; without the allowlist it is an open relay."
    )]
    RecipientNotAllowed { to: String, allowed: String },
    #[error(
        "this testbed relays mail and has no inbox; a relay accepts mail and reports nothing back"
    )]
    NoInbox,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc as StdArc;
    use testbed_core::{BroadcastBus, RunId};

    fn relay_config(allowed: &str) -> MailConfig {
        MailConfig {
            relay: Some(Relay {
                host: "smtp.example.test".into(),
                port: 587,
                user: "user".into(),
                password: "pass".into(),
                from: "testbed@example.com".into(),
            }),
            allowed: Allowlist::parse(allowed),
            ..Default::default()
        }
    }

    fn mailer_with(config: MailConfig) -> Mailer {
        let run = RunId::new();
        let clock = StdArc::new(Clock::new());
        let bus = StdArc::new(BroadcastBus::new(8, StdArc::clone(&clock), run));
        Mailer::new(config, bus, clock, run).expect("client builds")
    }

    fn mail_to(to: &str) -> OutgoingMail {
        serde_json::from_value(serde_json::json!({ "to": to, "subject": "x" })).unwrap()
    }

    /// The guard that stands between a public unauthenticated endpoint and an
    /// open mail relay. It has to refuse *before* the transport is touched, so
    /// this asserts on a host that does not resolve: reaching SMTP at all would
    /// be a different error.
    #[tokio::test]
    async fn a_relay_refuses_a_recipient_outside_the_allowlist() {
        let mailer = mailer_with(relay_config("@allowed.test"));

        let err = mailer
            .send(RunId::new(), mail_to("stranger@elsewhere.test"))
            .await
            .expect_err("an unlisted recipient was accepted");

        assert!(
            matches!(err, MailError::RecipientNotAllowed { .. }),
            "refused for the wrong reason: {err}"
        );
        assert!(err.to_string().contains("open relay"));
    }

    /// Fails closed: a relay with no allowlist sends nothing at all.
    #[tokio::test]
    async fn a_relay_with_no_allowlist_refuses_everything() {
        let mailer = mailer_with(relay_config(""));

        let err = mailer
            .send(RunId::new(), mail_to("anyone@anywhere.test"))
            .await
            .expect_err("an unconfigured relay sent mail");
        assert!(matches!(err, MailError::RecipientNotAllowed { .. }));
    }

    /// Mailpit mode is unrestricted on purpose — nothing leaves the machine, so
    /// an allowlist there would be ceremony with no safety value.
    #[tokio::test]
    async fn mailpit_mode_does_not_apply_the_allowlist() {
        let mailer = mailer_with(MailConfig::default());
        assert!(!mailer.config().is_relay());

        // Deliberately tolerant of both outcomes: with Mailpit up this send
        // succeeds, without it the transport errors. Neither is the point — the
        // assertion is only that it was never refused by *policy*, which is the
        // one result that would mean the guard had leaked into Mailpit mode.
        match mailer
            .send(RunId::new(), mail_to("anyone@anywhere.test"))
            .await
        {
            Ok(_) => {}
            Err(e) => assert!(
                !matches!(e, MailError::RecipientNotAllowed { .. }),
                "the allowlist was applied in Mailpit mode: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn a_relay_has_no_inbox_to_read() {
        let mailer = mailer_with(relay_config("@allowed.test"));
        assert!(
            mailer.inbox().is_none(),
            "relay mode offered an inbox; a relay reports nothing back"
        );
    }

    #[tokio::test]
    async fn mailpit_mode_has_an_inbox() {
        assert!(mailer_with(MailConfig::default()).inbox().is_some());
    }

    #[test]
    fn smtp_host_switches_the_transport_and_nothing_else_does() {
        assert!(!MailConfig::default().is_relay());
        assert!(relay_config("@a.test").is_relay());
    }

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
