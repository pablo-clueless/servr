//! The outbound sender: sign, deliver, retry on virtual-time backoff.
//!
//! # Why another poll loop
//!
//! This is the third virtual-time poll loop in the tree, after
//! `testbed_queue::Scheduler` and `testbed_stream::chunks`. They are deliberately
//! not shared: §4 forbids `hooks` depending on `queue`, and routing deliveries
//! through the job queue would make every webhook a job, which is a bigger lie
//! about the domain than a twenty-line loop is a duplication. The reasoning is
//! identical in all three — sleeping against wall time would make
//! `clock/advance` unable to bring a retry forward, and that capability is the
//! whole point (invariant 7).
//!
//! # Retry offsets are measured from the enqueue, not from the last failure
//!
//! Attempt *n* is due at `enqueued_at + sum(backoff[..n-1])`. Scheduling each
//! retry relative to the moment its predecessor failed looks equivalent and is
//! not: the Phase 7 gate advances the clock **once**, by 60 virtual seconds, and
//! expects attempts 1, 2 and 3 to follow. Under last-failure scheduling the
//! second attempt fires at T+60s and then schedules the third at T+61s — still
//! in the future — so one advance yields one retry and the gate never completes.
//! Offsets from the enqueue make the whole ladder due at once, which is also the
//! more natural reading of "retries fire at virtual times matching the
//! configured backoff".

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use serde_json::Value;
use testbed_core::{
    Clock, Event, EventKind, EventSink, RunId, SigningScheme, SpanId, TraceId, WebhookEndpoint,
};
use uuid::Uuid;

use crate::sign;

/// How often the loop wakes, in real time. Bounds how long after a
/// `clock/advance` a due retry takes to fire.
pub const TICK: Duration = Duration::from_millis(25);

/// Backoff used when an endpoint does not configure one, in virtual ms.
///
/// Three retries, cumulative at +1s, +6s and +36s from the enqueue — all inside
/// the 60 virtual seconds the Phase 7 gate advances, so `fail_first: 2` yields
/// exactly the attempts 1, 2, 3 the gate prints.
pub const DEFAULT_BACKOFF_MS: [u64; 3] = [1_000, 5_000, 30_000];

/// A queued delivery and everything needed to retry it.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub id: Uuid,
    pub endpoint: WebhookEndpoint,
    pub body: Value,
    /// Virtual time the delivery was queued. Every retry offset counts from here.
    pub enqueued_at: DateTime<Utc>,
    /// Attempts already made.
    pub attempt: u32,
    /// Virtual time the next attempt is due.
    pub due_at: DateTime<Utc>,
    /// Trace context at enqueue. Attempts *link* back to it rather than
    /// descending from it — a delivery retried over 30 virtual minutes must not
    /// become a 30-minute trace (T10, same reasoning as the queue).
    pub trace: Option<(TraceId, SpanId)>,
    pub done: bool,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
}

impl Delivery {
    fn backoff(&self) -> Vec<u64> {
        if self.endpoint.backoff_ms.is_empty() {
            DEFAULT_BACKOFF_MS.to_vec()
        } else {
            self.endpoint.backoff_ms.clone()
        }
    }

    /// Total attempts allowed: the first, plus one per backoff entry.
    fn max_attempts(&self) -> u32 {
        self.backoff().len() as u32 + 1
    }

    /// When attempt `n` (1-based) is due, counted from the enqueue.
    fn due_for(&self, n: u32) -> DateTime<Utc> {
        let offset: u64 = self
            .backoff()
            .iter()
            .take(n.saturating_sub(1) as usize)
            .sum();
        self.enqueued_at + TimeDelta::milliseconds(offset as i64)
    }
}

/// What `/_admin/hooks/out` serves back.
#[derive(Debug, Clone, Serialize)]
pub struct DeliveryView {
    pub id: String,
    pub endpoint: String,
    pub url: String,
    pub attempt: u32,
    pub max_attempts: u32,
    /// Virtual time the delivery was queued. Every retry offset counts from
    /// here, so this is what makes `due_at` interpretable.
    pub enqueued_at: DateTime<Utc>,
    pub due_at: DateTime<Utc>,
    pub done: bool,
    pub status: Option<u16>,
    pub error: Option<String>,
}

impl From<&Delivery> for DeliveryView {
    fn from(d: &Delivery) -> Self {
        Self {
            id: d.id.to_string(),
            endpoint: d.endpoint.name.clone(),
            url: d.endpoint.url.clone(),
            attempt: d.attempt,
            max_attempts: d.max_attempts(),
            enqueued_at: d.enqueued_at,
            due_at: d.due_at,
            done: d.done,
            status: d.last_status,
            error: d.last_error.clone(),
        }
    }
}

pub struct Sender {
    client: reqwest::Client,
    queue: Mutex<Vec<Delivery>>,
    clock: Arc<Clock>,
    bus: Arc<dyn EventSink>,
    run: RunId,
}

impl Sender {
    pub fn new(bus: Arc<dyn EventSink>, clock: Arc<Clock>, run: RunId) -> Self {
        Self {
            // A short timeout: a testbed webhook goes to localhost or to
            // whatever the scenario names, and a hung receiver should surface
            // as a failed attempt rather than as a stuck poll loop.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("building a plain reqwest client cannot fail"),
            queue: Mutex::new(Vec::new()),
            clock,
            bus,
            run,
        }
    }

    /// Queues a delivery. The first attempt is made by the next tick rather
    /// than inline, so enqueueing never blocks on the receiver.
    pub fn enqueue(&self, endpoint: WebhookEndpoint, body: Value) -> Uuid {
        let now = self.clock.now();
        let delivery = Delivery {
            id: Uuid::new_v4(),
            endpoint,
            body,
            enqueued_at: now,
            attempt: 0,
            due_at: now,
            trace: testbed_telemetry::propagation::current_ids(),
            done: false,
            last_status: None,
            last_error: None,
        };

        let id = delivery.id;
        self.queue
            .lock()
            .expect("sender lock poisoned")
            .push(delivery);
        id
    }

    pub fn deliveries(&self) -> Vec<DeliveryView> {
        self.queue
            .lock()
            .expect("sender lock poisoned")
            .iter()
            .map(DeliveryView::from)
            .collect()
    }

    pub async fn run_forever(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(tick_ms = TICK.as_millis() as u64, "webhook sender started");
        loop {
            ticker.tick().await;
            self.tick().await;
        }
    }

    /// One pass. Separated from the loop so tests drive it directly instead of
    /// sleeping — a test that sleeps to observe a retry is testing the tick
    /// interval, not the backoff.
    pub async fn tick(&self) {
        let now = self.clock.now();

        let due: Vec<Delivery> = {
            let queue = self.queue.lock().expect("sender lock poisoned");
            queue
                .iter()
                .filter(|d| !d.done && d.due_at <= now)
                .cloned()
                .collect()
        };

        for delivery in due {
            self.attempt(delivery).await;
        }
    }

    async fn attempt(&self, mut delivery: Delivery) {
        delivery.attempt += 1;
        let attempt = delivery.attempt;

        // A trace root linked back to the enqueue, never a child of it.
        let span = tracing::info_span!(
            parent: None,
            "webhook.out",
            otel.name = %format!("webhook out {}", delivery.endpoint.name),
            testbed.webhook.endpoint = %delivery.endpoint.name,
            testbed.webhook.url = %delivery.endpoint.url,
            testbed.webhook.attempt = attempt,
            { testbed_telemetry::late::WEBHOOK_STATUS } = tracing::field::Empty,
        );
        testbed_telemetry::link::follows_from_opt(&span, delivery.trace);

        let outcome = {
            use tracing::Instrument;
            self.deliver(&delivery).instrument(span.clone()).await
        };

        // Trap T12: recording the field declared `Empty` when the span opened.
        match &outcome {
            Ok(status) => {
                span.record(testbed_telemetry::late::WEBHOOK_STATUS, *status);
            }
            Err(e) => {
                tracing::debug!(parent: &span, "attempt {attempt} failed: {e}");
            }
        }

        let succeeded = matches!(outcome, Ok(status) if (200..300).contains(&status));
        delivery.last_status = outcome.as_ref().ok().copied();
        delivery.last_error = outcome.as_ref().err().map(|e| e.to_string());

        let next_retry_at = if succeeded || attempt >= delivery.max_attempts() {
            delivery.done = true;
            None
        } else {
            let due = delivery.due_for(attempt + 1);
            delivery.due_at = due;
            Some(due)
        };

        // Invariant 4: every attempt is an event as well as a span, and the
        // event carries the span's ids so the two join (invariant 9).
        {
            let _entered = span.enter();
            self.emit(EventKind::WebhookOut {
                endpoint: delivery.endpoint.name.clone(),
                attempt,
                status: delivery.last_status,
                next_retry_at,
            });
        }

        if delivery.done && !succeeded {
            tracing::warn!(
                endpoint = %delivery.endpoint.name,
                attempts = attempt,
                "webhook giving up after the last configured retry"
            );
        }

        self.replace(delivery);
    }

    /// One HTTP attempt. `Ok(status)` means the receiver answered, whatever it
    /// said; `Err` means the request never completed.
    async fn deliver(&self, delivery: &Delivery) -> Result<u16, SendError> {
        // `fail_first` short-circuits *before* the request. It is documented as
        // failing "regardless of the receiver", so asking the receiver at all
        // would make the count of deliveries it saw depend on a setting that
        // claims to be about the sender. Point an endpoint at a real 5xx when
        // duplicate deliveries are what you want to exercise.
        if delivery.attempt <= delivery.endpoint.fail_first {
            return Err(SendError::FailFirst {
                attempt: delivery.attempt,
                of: delivery.endpoint.fail_first,
            });
        }

        let body =
            serde_json::to_vec(&delivery.body).map_err(|e| SendError::Body(e.to_string()))?;
        let mut request = self
            .client
            .post(&delivery.endpoint.url)
            .header("content-type", "application/json");

        if let Some((name, value)) = sign::header(
            delivery.endpoint.sign,
            delivery
                .endpoint
                .secret
                .as_deref()
                .unwrap_or(sign::DEFAULT_SECRET),
            &body,
            self.clock.now(),
        ) {
            request = request.header(name, value);
        }

        // Invariant 10: outbound requests carry the current trace context, so a
        // receiver continues this trace instead of starting its own.
        let mut headers = axum::http::HeaderMap::new();
        testbed_telemetry::propagation::inject(
            &tracing_opentelemetry::OpenTelemetrySpanExt::context(&tracing::Span::current()),
            &mut headers,
        );
        for (name, value) in headers.iter() {
            if let Ok(value) = value.to_str() {
                request = request.header(name.as_str(), value);
            }
        }

        let response = request
            .body(body)
            .send()
            .await
            .map_err(|e| SendError::Http(e.to_string()))?;

        Ok(response.status().as_u16())
    }

    fn replace(&self, delivery: Delivery) {
        let mut queue = self.queue.lock().expect("sender lock poisoned");
        if let Some(slot) = queue.iter_mut().find(|d| d.id == delivery.id) {
            *slot = delivery;
        }
    }

    fn emit(&self, kind: EventKind) {
        let (trace_id, span_id) = match testbed_telemetry::propagation::current_ids() {
            Some((t, s)) => (Some(t), Some(s)),
            None => (None, None),
        };

        self.bus.emit(Event {
            id: 0,
            run: self.run,
            at: self.clock.now(),
            wall_at: Clock::wall_now(),
            trace_id,
            span_id,
            kind,
        });
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("failed by fail_first (attempt {attempt} of {of})")]
    FailFirst { attempt: u32, of: u32 },
    #[error("request failed: {0}")]
    Http(String),
    #[error("body is not serializable: {0}")]
    Body(String),
}

/// Builds an endpoint from the loose shape `/_admin/hooks/out` accepts.
pub fn endpoint_from(
    name: Option<String>,
    url: String,
    sign: Option<SigningScheme>,
    secret: Option<String>,
    backoff_ms: Option<Vec<u64>>,
    fail_first: Option<u32>,
) -> WebhookEndpoint {
    WebhookEndpoint {
        // The gate posts only a url, and an unnamed endpoint would emit
        // `EventKind::WebhookOut { endpoint: "" }` — unreadable on the bus.
        name: name.unwrap_or_else(|| url.clone()),
        url,
        sign: sign.unwrap_or_default(),
        secret,
        backoff_ms: backoff_ms.unwrap_or_default(),
        fail_first: fail_first.unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use testbed_core::BroadcastBus;

    use super::*;

    fn sender() -> (Arc<Sender>, Arc<Clock>, Arc<BroadcastBus>) {
        let run = RunId::new();
        let clock = Arc::new(Clock::new());
        let bus = Arc::new(BroadcastBus::new(256, Arc::clone(&clock), run));
        let sender = Arc::new(Sender::new(
            Arc::clone(&bus) as Arc<dyn EventSink>,
            Arc::clone(&clock),
            run,
        ));
        (sender, clock, bus)
    }

    fn endpoint(fail_first: u32) -> WebhookEndpoint {
        WebhookEndpoint {
            name: "test".into(),
            // Port 1 is never listening, so a real attempt fails fast without
            // needing a server. Only tests with `fail_first` covering every
            // attempt avoid reaching it.
            url: "http://127.0.0.1:1/hook".into(),
            sign: SigningScheme::None,
            secret: None,
            backoff_ms: vec![1_000, 5_000],
            fail_first,
        }
    }

    fn view(sender: &Sender) -> DeliveryView {
        sender.deliveries().into_iter().next().expect("no delivery")
    }

    #[tokio::test]
    async fn a_queued_delivery_is_due_immediately() {
        let (sender, _clock, _bus) = sender();
        sender.enqueue(endpoint(9), serde_json::json!({"x":1}));

        let queued = view(&sender);
        assert_eq!(queued.attempt, 0);
        assert!(!queued.done);
        assert_eq!(
            queued.max_attempts, 3,
            "two backoff entries means 3 attempts"
        );
    }

    /// The gate's shape: attempts 1, 2 and 3 all become due from a single
    /// advance, because offsets count from the enqueue.
    #[tokio::test]
    async fn one_advance_releases_the_whole_retry_ladder() {
        let (sender, clock, _bus) = sender();
        sender.enqueue(endpoint(9), serde_json::json!({"x":1}));

        sender.tick().await;
        assert_eq!(view(&sender).attempt, 1);

        clock.advance(Duration::from_secs(60));

        sender.tick().await;
        assert_eq!(view(&sender).attempt, 2);
        sender.tick().await;

        let done = view(&sender);
        assert_eq!(done.attempt, 3);
        assert!(done.done, "the ladder did not terminate at max_attempts");
    }

    /// The property the previous test would still pass without: a retry must
    /// *not* be due before its backoff has elapsed in virtual time.
    #[tokio::test]
    async fn a_retry_is_withheld_until_its_backoff_elapses() {
        let (sender, clock, _bus) = sender();
        sender.enqueue(endpoint(9), serde_json::json!({"x":1}));

        sender.tick().await;
        assert_eq!(view(&sender).attempt, 1);

        // Not yet: the first backoff entry is 1000ms.
        clock.advance(Duration::from_millis(500));
        sender.tick().await;
        assert_eq!(
            view(&sender).attempt,
            1,
            "the retry fired before its backoff elapsed"
        );

        clock.advance(Duration::from_millis(600));
        sender.tick().await;
        assert_eq!(view(&sender).attempt, 2);
    }

    /// Reads `enqueued_at` off the delivery rather than calling `clock.now()`
    /// again: the virtual clock keeps running, so two reads either side of
    /// `enqueue` differ by however long the test took to get between them.
    /// That made this assertion pass alone and fail under a loaded parallel
    /// run — a genuine flake, not a scheduling bug.
    #[tokio::test]
    async fn retry_offsets_are_measured_from_the_enqueue() {
        let (sender, _clock, _bus) = sender();
        sender.enqueue(endpoint(9), serde_json::json!({}));

        sender.tick().await;

        let after_first = view(&sender);
        assert_eq!(
            after_first.due_at,
            after_first.enqueued_at + TimeDelta::milliseconds(1_000),
            "attempt 2 is not one backoff step from the enqueue"
        );
    }

    #[tokio::test]
    async fn a_delivery_stops_after_the_last_configured_retry() {
        let (sender, clock, _bus) = sender();
        sender.enqueue(endpoint(9), serde_json::json!({}));

        for _ in 0..6 {
            clock.advance(Duration::from_secs(60));
            sender.tick().await;
        }

        let done = view(&sender);
        assert!(done.done);
        assert_eq!(done.attempt, 3, "more attempts than max_attempts allows");
    }

    #[tokio::test]
    async fn fail_first_reports_which_attempt_it_failed() {
        let (sender, _clock, _bus) = sender();
        sender.enqueue(endpoint(2), serde_json::json!({}));

        sender.tick().await;
        let after = view(&sender);
        assert_eq!(after.status, None, "fail_first must not report a status");
        assert!(after.error.unwrap().contains("fail_first"));
    }

    #[tokio::test]
    async fn every_attempt_lands_on_the_bus_with_its_number() {
        use futures_util::StreamExt;

        let (sender, clock, bus) = sender();
        let mut events = bus.subscribe();
        sender.enqueue(endpoint(9), serde_json::json!({}));

        sender.tick().await;
        clock.advance(Duration::from_secs(60));
        sender.tick().await;
        sender.tick().await;

        let mut attempts = Vec::new();
        while attempts.len() < 3 {
            match events.next().await.expect("bus closed").kind {
                EventKind::WebhookOut { attempt, .. } => attempts.push(attempt),
                _ => continue,
            }
        }
        assert_eq!(attempts, vec![1, 2, 3]);
    }

    /// The event carries when the next attempt is due, so a scenario can assert
    /// the configured backoff without reading the sender's internals.
    #[tokio::test]
    async fn the_event_carries_the_next_retry_time_until_it_gives_up() {
        use futures_util::StreamExt;

        let (sender, _clock, bus) = sender();
        let mut events = bus.subscribe();
        sender.enqueue(endpoint(9), serde_json::json!({}));

        sender.tick().await;
        let enqueued_at = view(&sender).enqueued_at;
        match events.next().await.unwrap().kind {
            EventKind::WebhookOut { next_retry_at, .. } => assert_eq!(
                next_retry_at,
                Some(enqueued_at + TimeDelta::milliseconds(1_000))
            ),
            other => panic!("expected WebhookOut, got {other:?}"),
        }
    }

    #[test]
    fn an_unnamed_endpoint_is_named_after_its_url() {
        let e = endpoint_from(None, "http://x/hook".into(), None, None, None, None);
        assert_eq!(e.name, "http://x/hook");
        assert_eq!(e.sign, SigningScheme::Stripe, "Q4 default");
    }

    #[test]
    fn an_endpoint_without_backoff_gets_the_default_ladder() {
        let delivery = Delivery {
            id: Uuid::new_v4(),
            endpoint: endpoint_from(None, "http://x".into(), None, None, None, None),
            body: Value::Null,
            enqueued_at: Utc::now(),
            attempt: 0,
            due_at: Utc::now(),
            trace: None,
            done: false,
            last_status: None,
            last_error: None,
        };

        assert_eq!(delivery.backoff(), DEFAULT_BACKOFF_MS.to_vec());
        assert_eq!(delivery.max_attempts(), 4);
        // Every retry has to land inside the 60s the gate advances.
        let last = delivery.due_for(delivery.max_attempts()) - delivery.enqueued_at;
        assert!(
            last < TimeDelta::seconds(60),
            "the default ladder does not fit in the gate's advance: {last}"
        );
    }
}
