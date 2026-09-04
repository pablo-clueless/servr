//! The read half: Mailpit's REST API, filtered to one run.
//!
//! See the crate docs for the two measurements that force the shape of
//! [`Inbox::for_run`] — Mailpit's listing carries no custom headers, and its
//! search does not index them.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use testbed_core::RunId;

use crate::send::RUN_HEADER_NAME;
use crate::{MailConfig, MailError};

/// How many messages a run-filtered read will look at before giving up.
///
/// The filter is an N+1 — one header fetch per candidate — so this bounds the
/// work rather than the result. A run that wants more than this from a local
/// sink is asking the wrong question of a testbed.
pub const DEFAULT_LIMIT: usize = 200;

/// One message, as the testbed reports it.
///
/// Deliberately not a passthrough of Mailpit's shape: this is what
/// `/_admin/mail` serves, and a consumer should not have to learn another
/// project's JSON to read it.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Message {
    pub id: String,
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub subject: String,
    pub snippet: String,
    pub created: String,
    /// The run this message was sent as, read back off the header.
    pub run: Option<String>,
}

pub struct Inbox {
    client: reqwest::Client,
    base: String,
}

impl Inbox {
    pub fn new(config: &MailConfig) -> Result<Self, MailError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| MailError::Api(e.to_string()))?;

        Ok(Self {
            client,
            base: config.api.trim_end_matches('/').to_string(),
        })
    }

    /// Mailpit's version, as a liveness probe.
    pub async fn version(&self) -> Result<String, MailError> {
        #[derive(Deserialize)]
        struct Info {
            #[serde(rename = "Version")]
            version: String,
        }

        let info: Info = self.get("/api/v1/info").await?;
        Ok(info.version)
    }

    /// Every message Mailpit holds, newest first, whatever run sent it.
    ///
    /// `query` is passed to Mailpit's own search when present. It is a
    /// convenience for narrowing — never the isolation mechanism.
    pub async fn all(&self, query: Option<&str>, limit: usize) -> Result<Vec<Message>, MailError> {
        let path = match query {
            Some(q) if !q.is_empty() => {
                format!("/api/v1/search?limit={limit}&query={}", urlencode(q))
            }
            _ => format!("/api/v1/messages?limit={limit}"),
        };

        let listing: Listing = self.get(&path).await?;
        Ok(listing.messages.into_iter().map(Message::from).collect())
    }

    /// The messages belonging to `run`.
    ///
    /// # Trap T7
    ///
    /// The run id is **not** pushed into the Mailpit query, because Mailpit
    /// does not index custom headers — a search for the id returns nothing for
    /// a message that carries it (measured, see the crate docs). So this lists
    /// candidates, fetches each one's headers, and filters here. Moving the
    /// filter into the query would return an empty inbox for every run, and
    /// "no mail" is exactly what a passing isolation assertion looks like.
    pub async fn for_run(
        &self,
        run: RunId,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Message>, MailError> {
        let wanted = run.header_value();
        let mut out = Vec::new();

        for mut message in self.all(query, limit).await? {
            let tag = self.run_header(&message.id).await?;
            if tag.as_deref() == Some(wanted.as_str()) {
                message.run = tag;
                out.push(message);
            }
        }

        Ok(out)
    }

    /// The `X-Testbed-Run` header on one message, if it carries one.
    ///
    /// Mail sent by something other than the testbed legitimately has none;
    /// that is a `None`, not an error.
    pub async fn run_header(&self, id: &str) -> Result<Option<String>, MailError> {
        let headers: HashMap<String, Vec<String>> =
            self.get(&format!("/api/v1/message/{id}/headers")).await?;

        // Mailpit echoes the header as written, but a header name is
        // case-insensitive per RFC 5322 and nothing guarantees a future Mailpit
        // preserves our casing. Compare accordingly.
        Ok(headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(RUN_HEADER_NAME))
            .and_then(|(_, values)| values.first())
            .map(|v| v.trim().to_string()))
    }

    /// Deletes every message. Mailpit has no per-run delete — same reason it
    /// has no per-run inbox (T7) — so this is all or nothing, and a caller
    /// running runs in parallel should not reach for it.
    pub async fn purge(&self) -> Result<(), MailError> {
        let url = format!("{}/api/v1/messages", self.base);
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| MailError::Api(e.to_string()))?;

        if !response.status().is_success() {
            return Err(MailError::Status {
                status: response.status().as_u16(),
                path: "/api/v1/messages".into(),
            });
        }
        Ok(())
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, MailError> {
        let url = format!("{}{path}", self.base);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MailError::Api(e.to_string()))?;

        if !response.status().is_success() {
            return Err(MailError::Status {
                status: response.status().as_u16(),
                path: path.to_string(),
            });
        }

        response
            .json()
            .await
            .map_err(|e| MailError::Api(format!("{path}: {e}")))
    }
}

/// Percent-encodes a search query. Mailpit's query syntax uses `:` and spaces,
/// neither of which survives a raw interpolation into a URL.
fn urlencode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

// --- Mailpit's own shapes, kept private ------------------------------------

#[derive(Debug, Deserialize)]
struct Listing {
    #[serde(rename = "messages", default)]
    messages: Vec<Summary>,
}

#[derive(Debug, Deserialize)]
struct Summary {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "MessageID", default)]
    message_id: String,
    #[serde(rename = "From")]
    from: Option<Address>,
    #[serde(rename = "To", default)]
    to: Option<Vec<Address>>,
    #[serde(rename = "Subject", default)]
    subject: String,
    #[serde(rename = "Snippet", default)]
    snippet: String,
    #[serde(rename = "Created", default)]
    created: String,
}

#[derive(Debug, Deserialize)]
struct Address {
    #[serde(rename = "Address", default)]
    address: String,
}

impl From<Summary> for Message {
    fn from(s: Summary) -> Self {
        Self {
            id: s.id,
            message_id: s.message_id,
            from: s.from.map(|a| a.address).unwrap_or_default(),
            to: s
                .to
                .unwrap_or_default()
                .into_iter()
                .map(|a| a.address)
                .collect(),
            subject: s.subject,
            snippet: s.snippet,
            created: s.created,
            // Filled in by `for_run`; a bare listing does not know it, because
            // Mailpit's summary carries no custom headers.
            run: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured verbatim from Mailpit v1.31 on 2026-09-04. If a future Mailpit
    /// changes these key names the decode breaks here rather than as an empty
    /// inbox in a scenario, which would read as an isolation success.
    const LISTING: &str = r#"{
        "total":1,"unread":1,"count":1,"messages_count":1,
        "messages":[{
            "ID":"3lHV6yeV24cFt8k4zcY7Nz",
            "MessageID":"2Ct1KqbRTSkMMKzff6HMD9@mailpit",
            "Read":false,
            "From":{"Name":"","Address":"testbed@localhost"},
            "To":[{"Name":"","Address":"a@b.c"}],
            "Cc":null,"Bcc":null,"ReplyTo":[],
            "Subject":"hello",
            "Created":"2026-09-04T19:15:50.596Z",
            "Size":294,"Attachments":0,"Snippet":"body"
        }]
    }"#;

    #[test]
    fn decodes_a_real_mailpit_listing() {
        let listing: Listing = serde_json::from_str(LISTING).unwrap();
        let message = Message::from(listing.messages.into_iter().next().unwrap());

        assert_eq!(message.id, "3lHV6yeV24cFt8k4zcY7Nz");
        assert_eq!(message.from, "testbed@localhost");
        assert_eq!(message.to, vec!["a@b.c"]);
        assert_eq!(message.subject, "hello");
        assert_eq!(message.snippet, "body");
        assert!(
            message.run.is_none(),
            "a listing cannot know the run; claiming otherwise hides T7"
        );
    }

    #[test]
    fn an_empty_inbox_decodes_as_no_messages() {
        let listing: Listing =
            serde_json::from_str(r#"{"total":0,"count":0,"messages":[]}"#).unwrap();
        assert!(listing.messages.is_empty());
    }

    #[test]
    fn search_queries_survive_being_put_in_a_url() {
        assert_eq!(urlencode("subject:hello"), "subject%3Ahello");
        assert_eq!(urlencode("to:a@b.c world"), "to%3Aa%40b.c+world");
        assert_eq!(urlencode("plain"), "plain");
    }
}
