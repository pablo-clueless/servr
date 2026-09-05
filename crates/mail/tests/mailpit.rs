//! The Phase 6 gate, against a live Mailpit.
//!
//! **Skips itself unless `MAILPIT_API` is set**, the same way the queue's Redis
//! tests skip without `REDIS_URL`: it runs the moment infra is available rather
//! than needing a remembered flag.
//!
//! ```text
//! docker compose up -d --wait mailpit
//! MAILPIT_API=http://localhost:8025 cargo test -p testbed-mail --test mailpit
//! ```
//!
//! The unit tests in `inbox.rs` assert the decode against a captured payload.
//! These assert the half that cannot be faked: that a message really goes out
//! over SMTP, really comes back over REST, and really only comes back to the
//! run that sent it (invariant 7).
//!
//! # These tests share one inbox
//!
//! Mailpit has no per-run inbox — that is the whole of T7 — so tests here must
//! not purge, and must not assert on totals. Every assertion is scoped to a
//! `RunId` minted by the test itself, which is exactly the isolation property
//! under test and also what lets these run in parallel with each other.

use std::sync::Arc;

use testbed_core::{BroadcastBus, Clock, EventKind, EventSink, RunId};
use testbed_mail::{MailConfig, Mailer, OutgoingMail};

macro_rules! require_mailpit {
    () => {
        match std::env::var("MAILPIT_API").ok().filter(|u| !u.is_empty()) {
            Some(api) => api,
            None => {
                eprintln!("skipping: MAILPIT_API unset (start Mailpit and re-run)");
                return;
            }
        }
    };
}

struct Harness {
    mailer: Mailer,
    bus: Arc<BroadcastBus>,
}

impl Harness {
    /// These tests all run in Mailpit mode, where an inbox is always present.
    /// Relay mode has none — that is what `Mailer::inbox` returning `Option`
    /// encodes, and it is asserted separately in the crate's unit tests.
    fn inbox(&self) -> &testbed_mail::Inbox {
        self.mailer
            .inbox()
            .expect("mailpit mode always has an inbox")
    }
}

fn harness() -> Harness {
    let config = MailConfig::from_env();
    let run = RunId::new();
    let clock = Arc::new(Clock::new());
    let bus = Arc::new(BroadcastBus::new(64, Arc::clone(&clock), run));

    let mailer = Mailer::new(config, Arc::clone(&bus) as Arc<dyn EventSink>, clock, run)
        .expect("building the mail client failed");

    Harness { mailer, bus }
}

fn mail(to: &str, subject: &str) -> OutgoingMail {
    serde_json::from_value(serde_json::json!({ "to": to, "subject": subject }))
        .expect("test mail did not deserialize")
}

/// The gate's first half: a message sent over SMTP is readable over REST.
#[tokio::test]
async fn a_sent_message_comes_back_for_its_own_run() {
    let _api = require_mailpit!();
    let h = harness();
    let run = RunId::new();

    let sent = h
        .mailer
        .send(run, mail("a@b.c", "hello"))
        .await
        .expect("send failed");

    let inbox = h
        .mailer
        .inbox()
        .expect("mailpit mode always has an inbox")
        .for_run(run, None, 50)
        .await
        .expect("read failed");

    assert_eq!(inbox.len(), 1, "the run's own message did not come back");
    assert_eq!(inbox[0].subject, "hello");
    assert_eq!(inbox[0].to, vec!["a@b.c"]);
    assert_eq!(
        inbox[0].run.as_deref(),
        Some(run.header_value().as_str()),
        "the message came back tagged with the wrong run"
    );
    assert!(
        sent.message_id.ends_with("@testbed"),
        "the returned id is not the one that went on the wire: {}",
        sent.message_id
    );
}

/// The gate's second half, and the whole of invariant 7: run B must not see
/// run A's mail.
///
/// Both runs send, so a filter that is simply broken in the "returns nothing"
/// direction cannot pass this — that failure mode is indistinguishable from
/// working isolation if only one side ever has mail.
#[tokio::test]
async fn one_run_cannot_read_another_runs_mail() {
    let _api = require_mailpit!();
    let h = harness();
    let (a, b) = (RunId::new(), RunId::new());

    h.mailer.send(a, mail("a@b.c", "for-a")).await.unwrap();
    h.mailer.send(b, mail("b@b.c", "for-b")).await.unwrap();

    let for_a = h.inbox().for_run(a, None, 100).await.unwrap();
    let for_b = h.inbox().for_run(b, None, 100).await.unwrap();

    assert_eq!(for_a.len(), 1, "run A sees {} messages", for_a.len());
    assert_eq!(for_a[0].subject, "for-a");

    assert_eq!(for_b.len(), 1, "run B sees {} messages", for_b.len());
    assert_eq!(for_b[0].subject, "for-b");
}

/// A run that has sent nothing has an empty inbox, however much mail the
/// shared Mailpit is holding.
#[tokio::test]
async fn a_run_that_sent_nothing_reads_nothing() {
    let _api = require_mailpit!();
    let h = harness();

    h.mailer
        .send(RunId::new(), mail("someone@b.c", "noise"))
        .await
        .unwrap();

    let inbox = h
        .mailer
        .inbox()
        .expect("mailpit mode always has an inbox")
        .for_run(RunId::new(), None, 100)
        .await
        .unwrap();

    assert!(
        inbox.is_empty(),
        "a fresh run read {} messages it never sent",
        inbox.len()
    );
}

/// Mailpit's search narrows; it never isolates.
///
/// This pins the measurement the crate docs rest on: a query that matches
/// another run's message must still not leak it. If Mailpit ever starts
/// indexing custom headers, this keeps passing — the filter does not depend on
/// which way that goes.
#[tokio::test]
async fn a_search_query_narrows_within_a_run_without_crossing_runs() {
    let _api = require_mailpit!();
    let h = harness();
    let (a, b) = (RunId::new(), RunId::new());

    // Same subject on both runs, so the query alone cannot separate them.
    let subject = format!("shared-{}", RunId::new().0.simple());
    h.mailer.send(a, mail("a@b.c", &subject)).await.unwrap();
    h.mailer.send(b, mail("b@b.c", &subject)).await.unwrap();

    let query = format!("subject:{subject}");
    let for_a = h
        .mailer
        .inbox()
        .expect("mailpit mode always has an inbox")
        .for_run(a, Some(&query), 100)
        .await
        .unwrap();

    assert_eq!(
        for_a.len(),
        1,
        "the query matched both runs' messages; the run filter is not applied"
    );
    assert_eq!(for_a[0].to, vec!["a@b.c"]);
}

/// Invariant 4: a send is a bus event as well as a span. Invariant 9: it
/// carries the context to join on.
#[tokio::test]
async fn a_send_lands_on_the_event_bus() {
    use futures_util::StreamExt;

    let _api = require_mailpit!();
    let h = harness();
    let mut events = h.bus.subscribe();

    let sent = h
        .mailer
        .send(RunId::new(), mail("a@b.c", "bussed"))
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let event = events.next().await.expect("bus closed");
            if matches!(event.kind, EventKind::MailSent { .. }) {
                return event;
            }
        }
    })
    .await
    .expect("no MailSent reached the bus");

    match &event.kind {
        EventKind::MailSent {
            to,
            subject,
            message_id,
        } => {
            assert_eq!(to, "a@b.c");
            assert_eq!(subject, "bussed");
            assert_eq!(
                *message_id, sent.message_id,
                "the event's id is not the one that was sent"
            );
        }
        other => unreachable!("filtered for MailSent, got {other:?}"),
    }
}

/// Mail the testbed did not send has no run tag, and must not be attributed to
/// one — a real inbox will contain other people's mail.
#[tokio::test]
async fn untagged_mail_belongs_to_no_run() {
    let api = require_mailpit!();

    // Injected through Mailpit's own send API, so it arrives without the header
    // the SMTP path always sets.
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{api}/api/v1/send"))
        .json(&serde_json::json!({
            "From": { "Email": "outsider@elsewhere.test" },
            "To": [{ "Email": "a@b.c" }],
            "Subject": "untagged",
            "Text": "not from the testbed",
        }))
        .send()
        .await
        .expect("injecting an untagged message failed");
    assert!(response.status().is_success());

    let id: serde_json::Value = response.json().await.unwrap();
    let id = id["ID"].as_str().expect("no message id returned");

    let h = harness();
    assert_eq!(
        h.inbox().run_header(id).await.unwrap(),
        None,
        "a message with no run header was attributed to a run"
    );
}
