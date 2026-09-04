//! The SMTP half: build a message, tag it with the run, hand it to Mailpit.

use lettre::message::header::{Header, HeaderName, HeaderValue};
use lettre::transport::smtp::AsyncSmtpTransport;
use lettre::{AsyncTransport, Message, Tokio1Executor};
use serde::{Deserialize, Serialize};
use testbed_core::RunId;

use crate::{MailConfig, MailError};

/// The header that is the entire isolation story (T7).
///
/// Spelled here as an ASCII constant rather than reusing
/// [`testbed_core::RUN_HEADER`] because that one is lowercase for HTTP, where
/// header names are case-insensitive and axum normalises them. A mail header is
/// echoed back by Mailpit exactly as written, and `/api/v1/message/{id}/headers`
/// keys on that spelling — so the canonical `X-Testbed-Run` casing is load
/// bearing on the read side.
pub const RUN_HEADER_NAME: &str = "X-Testbed-Run";

/// Default envelope sender. Mailpit accepts anything; this just has to be a
/// valid address so the message builds.
pub const DEFAULT_FROM: &str = "testbed@localhost";

/// The run tag, as a lettre header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestbedRun(pub String);

impl Header for TestbedRun {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str(RUN_HEADER_NAME)
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(s.to_string()))
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::new(Self::name(), self.0.clone())
    }
}

/// What `/_admin/mail/send` accepts.
#[derive(Debug, Clone, Deserialize)]
pub struct OutgoingMail {
    pub to: String,
    #[serde(default)]
    pub subject: String,
    /// Defaults to something identifiable rather than empty, so a message that
    /// arrives with no body is distinguishable from one whose body was lost.
    #[serde(default = "default_body")]
    pub body: String,
    #[serde(default)]
    pub from: Option<String>,
}

fn default_body() -> String {
    "Sent by the testbed.".to_string()
}

#[derive(Debug, Clone, Serialize)]
pub struct SentMail {
    pub message_id: String,
    pub to: String,
    pub subject: String,
    /// The run the message was tagged with, echoed back so a caller can assert
    /// on it without re-reading the header it just set.
    pub run: RunId,
}

pub struct Sender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
}

impl Sender {
    pub fn new(config: &MailConfig) -> Result<Self, MailError> {
        let (host, port) = config.smtp_parts()?;

        // `builder_dangerous` is the plaintext, no-TLS builder, and that is
        // correct here: Mailpit in `compose.yaml` speaks plain SMTP on 1025 and
        // the testbed never sends mail anywhere else. The workspace does not
        // enable a TLS feature on lettre at all, so there is no encrypted path
        // to fall back to and nothing to negotiate away.
        let transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
            .port(port)
            .build();

        Ok(Self { transport })
    }

    pub async fn send(&self, run: RunId, mail: &OutgoingMail) -> Result<SentMail, MailError> {
        // Set explicitly rather than left to lettre, so the id returned to the
        // caller is the id on the wire. Reading lettre's generated one back off
        // the built message would work and would be one more thing to get
        // subtly wrong.
        let message_id = format!("{}@testbed", uuid::Uuid::new_v4());
        let from = mail.from.as_deref().unwrap_or(DEFAULT_FROM);

        let built = Message::builder()
            .from(
                from.parse()
                    .map_err(|e| MailError::Build(format!("from address {from:?}: {e}")))?,
            )
            .to(mail
                .to
                .parse()
                .map_err(|e| MailError::Build(format!("to address {:?}: {e}", mail.to)))?)
            .subject(&mail.subject)
            .message_id(Some(message_id.clone()))
            // T7: the only thing that makes this message attributable to a run.
            .header(TestbedRun(run.header_value()))
            .body(mail.body.clone())
            .map_err(|e| MailError::Build(e.to_string()))?;

        self.transport
            .send(built)
            .await
            .map_err(|e| MailError::Smtp(e.to_string()))?;

        Ok(SentMail {
            message_id,
            to: mail.to.clone(),
            subject: mail.subject.clone(),
            run,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_header_name_keeps_its_canonical_casing() {
        // Mailpit echoes the header verbatim and `inbox` looks it up by this
        // spelling; lowercasing it here would break the read side only.
        let name = TestbedRun::name();
        let name: &str = name.as_ref();
        assert_eq!(name, "X-Testbed-Run");
    }

    #[test]
    fn the_run_tag_renders_as_the_bare_uuid() {
        let run = RunId::new();
        let header = TestbedRun(run.header_value());
        let rendered = format!("{:?}", header.display());
        assert!(
            rendered.contains(&run.to_string()),
            "the run id did not survive onto the header: {rendered}"
        );
    }

    #[test]
    fn a_parsed_header_round_trips() {
        let raw = RunId::new().header_value();
        assert_eq!(TestbedRun::parse(&raw).unwrap().0, raw);
    }

    /// Every field but `to` is optional, because the §7 gate posts only
    /// `{"to":..., "subject":...}`.
    #[test]
    fn the_gate_body_deserializes() {
        let mail: OutgoingMail =
            serde_json::from_str(r#"{"to":"a@b.c","subject":"hello"}"#).unwrap();

        assert_eq!(mail.to, "a@b.c");
        assert_eq!(mail.subject, "hello");
        assert!(mail.from.is_none());
        assert!(
            !mail.body.is_empty(),
            "an absent body must not send nothing"
        );
    }
}
