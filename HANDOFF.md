# HANDOFF — `testbed`

Written for autonomous continuation. Gates are mechanical: run the command, compare to the stated output. Do not proceed past a failing gate.

---

## 1. What this is

A single Rust server used to exercise frontend and backend features, and to develop dev tools against. Six surfaces:

| Surface | Direction | Purpose |
|---|---|---|
| REST | in | CRUD under fault injection |
| Webhooks | in + out | capture inbox; outbound sender with signing + retries |
| Job queue | internal | delayed/retried/failed jobs under a virtual clock |
| Email | out | SMTP to Mailpit, read back over Mailpit's REST |
| WebSocket | duplex | topic hub, presence, forced disconnects |
| Chat streams | out | token-by-token SSE |
| Telemetry | out | OTLP traces + metrics, deliberately corruptible |

The value is not any one surface. It is that **all of them emit onto one event stream** with one clock and one reset, and that the same execution is simultaneously visible as a real OTLP trace tree. That is the product.

---

## 2. Locked decisions

Do not relitigate these without operator sign-off.

1. Rust, `axum` 0.8 + `tokio`, `tower-http` for the middleware stack.
2. **Docker Compose with real infra.** Postgres, Redis, Mailpit. No embedded/faked backends.
3. **Two planes.**
   - *Data plane* — real infra, the thing under test. Postgres holds domain entities; Redis is queue storage and (maybe) pub/sub; Mailpit is the real SMTP sink.
   - *Control plane* — the testbed's own state: fault config, scenario registry, event log, clock offset. In-memory behind `ArcSwap`, optionally snapshotted to SQLite.
4. **Control plane never touches Postgres.** It must survive a full data-plane wipe.
5. **Behavior is layered.** TOML scenario files seed `base` at boot; the admin API mutates an `overlay`; `reset` drops the overlay and re-resolves from `base`.
6. **No embedded SMTP.** Mailpit owns SMTP *and* provides the read API. `crates/mail` is a thin client facade. Do not add `mailin-embedded`.
7. **The virtual clock is authoritative for all scheduling.** Nothing schedules against wall time.
8. **Fault injection is a `tower` layer**, never per-handler logic.
9. **`tracing` + OTLP is a first-class output surface**, not merely operator debugging. The testbed is a telemetry *source* you point real observability tooling at, and a source that can be told to emit malformed telemetry on demand.
10. **Every bus event carries `trace_id` / `span_id`.** The event stream and the trace tree are two views of one execution and must be joinable on that key.
11. **Span timestamps are wall-clock; virtual time rides as an attribute.** A span cannot claim to last 30 virtual seconds — no collector would survive it.
12. Cargo workspace, crate layout per §4.

---

## 3. Open questions — RESOLVED 2026-09-01

All five were put to the operator and answered. Each took the recommended default. Nothing here is open; do not re-ask.

| # | Question | Blocks | Decision |
|---|---|---|---|
| Q1 | Event bus transport: in-process `tokio::sync::broadcast` vs Redis pub/sub. Does the testbed ever run multi-replica? | **Phase 1** — changes the bus signature, expensive to retrofit | **`EventSink` trait; ship the in-process impl; leave a Redis impl behind a `distributed` feature flag.** Single-replica for now, trait is the escape hatch. → `crates/core/src/bus.rs` |
| Q2 | Postgres isolation: schema-per-run (`search_path`) vs database-per-run vs truncate-between | **Phase 3** | **schema-per-run.** Schema name is `RunId::schema()` → `run_<uuid-simple>`. Set in `PoolOptions::after_connect` (T5) |
| Q3 | Chat stream wire format: OpenAI-compatible `/v1/chat/completions` vs bespoke | **Phase 5** | **Both:** OpenAI-compatible `/v1/chat/completions`, plus a bespoke `/_stream/{id}` escape hatch for arbitrary chunk scripting. → `crates/stream/src/lib.rs` |
| Q4 | Outbound webhook signing: Stripe-style (`t=`,`v1=` HMAC) vs GitHub-style (`X-Hub-Signature-256`) vs both, selectable per endpoint | **Phase 7** | **Both, selected per endpoint in scenario config.** → `SigningScheme` in `crates/core/src/fault.rs` |
| Q5 | Telemetry backend in compose: collector-only with debug exporter, vs Jaeger, vs Tempo + Prometheus + Grafana | **Phase 2b** | **Always export OTLP to a collector**; Jaeger and Prometheus behind the `obs` compose profile so the base stack stays light. → `compose.yaml`, `deploy/otel-collector.yaml` |

The app only ever speaks OTLP to a collector endpoint, so the backend behind it stays a compose-file concern and Q5 remains cheap to revisit.

---

## 4. Workspace layout

```
testbed/
  Cargo.toml                  workspace manifest
  compose.yaml                postgres, redis, mailpit; collector + jaeger + prometheus under profile `obs`
  scenarios/
    default.toml
  crates/
    core/                     clock, event bus, layered config, run scoping, fault spec
    telemetry/                tracing subscriber, OTLP export, propagation, metrics, telemetry faults
    http/                     REST routes + fault layer
    ws/                       hub, topics, presence, forced disconnect
    stream/                   SSE + chat streaming
    queue/                    job registry, scheduler, retries, DLQ
    mail/                     Mailpit client facade
    hooks/                    inbound capture + outbound sender
    admin/                    control plane routes
  server/                     binary; wires everything
```

`core` depends on nothing else in the workspace. `telemetry` depends only on `core`. Everything else depends on both. `server` depends on all. No other cross-crate edges — if `queue` needs to reach `http`, the design is wrong; route it through the event bus.

**Ports:** server `8080` (admin mounted at `/_admin`), Postgres `5432`, Redis `6379`, Mailpit SMTP `1025` / HTTP `8025`.

---

## 5. Invariants

Breaking any of these silently destroys the testbed's usefulness. Several have grep gates in §7.

1. **No wall-clock reads outside two sanctioned files.** `SystemTime::now()` and `Instant::now()` are permitted only in `crates/core/src/clock.rs` and `crates/telemetry/src/wall.rs`. If the queue reads wall time anywhere, time travel is dead and the whole thing is untestable. The telemetry exemption exists solely so spans carry honest real-world durations (invariant 11 in §2) and must not be used for scheduling.
2. **`base` is immutable after boot.** All runtime mutation goes to `overlay`. If `reset` cannot reconstruct a known-good state from the scenario file alone, test isolation is gone.
3. **Control plane performs no Postgres I/O.** Ever.
4. **Every domain-significant action emits a bus event *and* opens a span.** These are two axes, not redundancy: the bus is typed, virtual-clock-stamped, resettable and replayable (the product surface); the trace tree is wall-clock, sampled and exported (the tooling surface). Something that appears in only one of them is a bug.
5. **Redis is queue storage, never a scheduler.** The scheduler is our own poll loop comparing against virtual now.
6. **Every run has a `RunId`; all data-plane writes are namespaced by it.** No exceptions — an unnamespaced write breaks parallel test execution for everyone.
7. **Mailpit isolation is the `X-Testbed-Run` header and nothing else.** Mailpit has no native namespacing. Every outbound message sets it; every read filters on it.
8. **Faults apply at the layer.** A handler that checks fault config itself is a bug.
9. **Bus events and spans are joinable.** Every `Event` carries the `trace_id`/`span_id` active when it was emitted. Losing this join key makes the two surfaces useless together, which is most of the point.
10. **Trace context propagates across every boundary.** Inbound W3C `traceparent` is extracted and continued; outbound webhooks inject it; queue jobs carry it as a *link* (see T10); WS frames link to their connection span.
11. **Telemetry faults are injected at export, not at instrumentation.** Corrupting spans where they are created poisons the testbed's own debuggability. Corrupt them in the exporter shim, where the damage is confined to what leaves the process.

---

## 6. Core types

Sketch, not gospel — but the shape of `State` and `Clock` is load-bearing.

```rust
pub struct RunId(pub Uuid);

pub struct State {
    base: Arc<Scenario>,
    overlay: ArcSwap<Overlay>,
    resolved: ArcSwap<Resolved>,
    clock: Clock,
    bus: Arc<dyn EventSink>,
}

impl State {
    pub fn mutate(&self, f: impl FnOnce(&mut Overlay)) { /* clone, apply, store, re-resolve */ }
    pub fn reset(&self) { /* overlay = default, re-resolve from base */ }
    pub fn resolved(&self) -> Guard<Arc<Resolved>> { self.resolved.load() }
}

pub struct Clock {
    epoch: Instant,
    offset_ms: AtomicI64,
}

impl Clock {
    pub fn now(&self) -> DateTime<Utc>;
    pub fn advance(&self, d: Duration);
    pub fn freeze(&self);
}

pub struct Event {
    pub id: u64,
    pub run: RunId,
    pub at: DateTime<Utc>,
    pub wall_at: DateTime<Utc>,
    pub trace_id: Option<TraceId>,
    pub span_id: Option<SpanId>,
    pub kind: EventKind,
}

pub enum EventKind {
    HttpRequest { method: String, path: String, status: u16, latency_ms: u64, faults: Vec<String> },
    JobTransition { job: JobId, from: JobState, to: JobState, attempt: u32 },
    MailSent { to: String, subject: String, message_id: String },
    WebhookIn { endpoint: String, headers: HeaderMap, body_sha256: String },
    WebhookOut { endpoint: String, attempt: u32, status: Option<u16>, next_retry_at: Option<DateTime<Utc>> },
    WsFrame { topic: String, conn: ConnId, dir: Dir, bytes: usize },
    StreamChunk { stream: StreamId, seq: u32 },
    Gap { dropped: u64 },
}

pub trait EventSink: Send + Sync {
    fn emit(&self, e: Event);
    fn subscribe(&self) -> BoxStream<'static, Event>;
}

pub struct FaultSpec {
    pub route: String,
    pub rate: f64,
    pub latency_ms: Option<u64>,
    pub jitter_ms: Option<u64>,
    pub status: Option<u16>,
    pub truncate_body_at: Option<usize>,
    pub drop_connection: bool,
}

pub struct TelemetryFault {
    pub rate: f64,
    pub orphan_spans: bool,
    pub clock_skew_ms: Option<i64>,
    pub cardinality_bomb: Option<u32>,
    pub attribute_bloat_bytes: Option<usize>,
    pub drop_export: bool,
    pub export_latency_ms: Option<u64>,
    pub corrupt_inbound_traceparent: bool,
    pub counter_reset: bool,
}
```

`EventKind::Gap` exists because of trap T4. Do not remove it.

---

## 7. Phases and gates

### Phase 0 — scaffold + infra

Workspace with all nine crates (stubs fine), `compose.yaml` with healthchecks on all three services.

```
$ docker compose up -d && sleep 15
$ docker compose ps --format '{{.Service}} {{.Health}}'
mailpit healthy
postgres healthy
redis healthy

$ cargo build --workspace 2>&1 | tail -1
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

Healthchecks are not hygiene here — see trap T2.

---

### Phase 1 — core

**Requires Q1 answered.** Clock, event bus, layered config, `RunId`, `FaultSpec` parsing.

```
$ cargo test -p testbed-core 2>&1 | tail -1
test result: ok. 12 passed; 0 failed; 0 ignored

$ rg -n 'SystemTime::now|Instant::now' crates/ server/ \
    --glob '!crates/core/src/clock.rs' --glob '!crates/telemetry/src/wall.rs'
$ echo $?
1
```

Exit 1 means no matches. That grep is invariant #1 and should move into CI.

Required tests: clock advance is monotonic and does not sleep; `reset` restores base exactly after arbitrary overlay mutation; a lagging subscriber yields `Gap` rather than silently losing events.

---

### Phase 2 — http + fault layer + admin

Routes: `/_admin/health`, `/_admin/reset`, `/_admin/clock/advance`, `/_admin/faults`, `/_admin/events` (SSE), plus a trivial `/api/ping`.

```
$ curl -s localhost:8080/_admin/health
{"status":"ok","run":"<uuid>"}

$ curl -s -X POST localhost:8080/_admin/faults \
    -H 'content-type: application/json' \
    -d '{"route":"/api/*","rate":1.0,"latency_ms":500,"status":503}'
{"ok":true}

$ curl -s -o /dev/null -w '%{http_code} %{time_total}\n' localhost:8080/api/ping
503 0.5

$ curl -s -X POST localhost:8080/_admin/reset >/dev/null
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/ping
200
```

`time_total` must be ≥ 0.500 and < 0.600.

---

### Phase 2b — telemetry spine

**Requires Q5 answered.** Do this before Phase 3. Instrumentation retrofitted after six surfaces exist is instrumentation with holes in it, and the holes are always in the interesting places.

Scope: `tracing-subscriber` + `tracing-opentelemetry` + OTLP exporter, W3C propagation in and out, `testbed.run_id` and `testbed.virtual_time` on every span, a Prometheus `/metrics` endpoint, and `trace_id`/`span_id` stamped onto every bus event.

Baseline metrics: RED per surface, plus `testbed_queue_depth`, `testbed_jobs_total{state}`, `testbed_ws_connections`, `testbed_webhook_attempts_total{status}`, `testbed_events_dropped_total`. The last one is the `Gap` counter from T4 and is how you notice the event log is lying to you.

```
$ docker compose --profile obs up -d && sleep 10

$ TP="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
$ curl -s localhost:8080/api/ping -H "traceparent: $TP" >/dev/null

$ curl -s 'localhost:16686/api/traces?service=testbed&limit=1' \
    | jq -r '.data[0].traceID'
4bf92f3577b34da6a3ce929d0e0e4736

$ curl -sN localhost:8080/_admin/events | head -1 | jq -r .trace_id
4bf92f3577b34da6a3ce929d0e0e4736

$ curl -s localhost:8080/metrics | grep -c '^testbed_'
6
```

Two things are being gated here, and both matter. The inbound `traceparent` must be *continued*, not replaced — a fresh trace id means frontend RUM will never join to backend spans, which is the single most common thing people need a testbed for. And the trace id on the bus event must equal the trace id in the collector; if those diverge the two surfaces can never be correlated.

---

### Phase 3 — data plane + run isolation

**Requires Q2 answered.** Postgres and Redis wired, namespaced per `RunId`.

```
$ RUN_A=$(curl -s -X POST localhost:8080/_admin/runs | jq -r .run)
$ RUN_B=$(curl -s -X POST localhost:8080/_admin/runs | jq -r .run)
$ curl -s -X POST localhost:8080/api/items -H "x-testbed-run: $RUN_A" -d '{"name":"a"}' >/dev/null
$ curl -s localhost:8080/api/items -H "x-testbed-run: $RUN_B" | jq 'length'
0
$ curl -s localhost:8080/api/items -H "x-testbed-run: $RUN_A" | jq 'length'
1
```

See trap T5 — `search_path` must be set per pooled connection, not once at startup.

---

### Phase 4 — queue

Job registry, scheduler polling against virtual now, retries with backoff, DLQ.

```
$ JOB=$(curl -s -X POST localhost:8080/_admin/jobs \
    -d '{"kind":"noop","delay_ms":30000}' | jq -r .id)
$ curl -s localhost:8080/_admin/jobs/$JOB | jq -r .state
scheduled

$ time (curl -s -X POST localhost:8080/_admin/clock/advance -d '{"ms":30000}' >/dev/null \
        && sleep 0.2 \
        && curl -s localhost:8080/_admin/jobs/$JOB | jq -r .state)
succeeded

real    0m0.2xxs
```

**The `real` time is the gate.** If it approaches 30s, the scheduler is reading wall time and Phase 4 has failed regardless of the state transition. See trap T3 for the atomicity requirement on the poll.

Then assert the trace shape: the execution span must *link* to the enqueue span, not descend from it. A single trace covering both means T10 was missed.

```
$ curl -s "localhost:16686/api/traces?service=testbed&tags=%7B%22job.id%22%3A%22$JOB%22%7D" \
    | jq '[.data[].spans[] | select(.references[].refType=="FOLLOWS_FROM")] | length'
1
```

---

### Phase 5 — ws + streams

**Requires Q3 answered.** Topic hub, presence, server-initiated close, SSE chat streaming.

```
$ websocat -t ws://localhost:8080/ws?topic=demo &
$ curl -s -X POST localhost:8080/_admin/ws/publish -d '{"topic":"demo","body":"hi"}'
# subscriber prints: hi

$ curl -s -X POST localhost:8080/_admin/ws/kill -d '{"topic":"demo"}'
# subscriber exits with a clean close, not a timeout   <- see T6

$ curl -sN localhost:8080/v1/chat/completions \
    -d '{"stream":true,"messages":[{"role":"user","content":"hi"}]}' | head -3
data: {"choices":[{"delta":{"content":"..."}}]}
```

---

### Phase 6 — mail

Send through Mailpit SMTP, read back through its REST API, filtered by run.

```
$ curl -s -X POST localhost:8080/_admin/mail/send \
    -H "x-testbed-run: $RUN_A" -d '{"to":"a@b.c","subject":"hello"}' >/dev/null
$ curl -s "localhost:8025/api/v1/search?query=subject:hello" | jq '.messages | length'
1
```

Then assert the message carries `X-Testbed-Run: $RUN_A`, and that reading as `$RUN_B` yields 0. This filtering is the *only* isolation mechanism available — trap T7.

---

### Phase 7 — webhooks

**Requires Q4 answered.** Inbound capture inbox, outbound sender with signing and retries.

```
$ curl -s -X POST localhost:8080/hooks/in/abc -d '{"x":1}' >/dev/null
$ curl -s localhost:8080/_admin/hooks/in/abc | jq '.[0].body.x'
1

$ curl -s -X POST localhost:8080/_admin/hooks/out \
    -d '{"url":"http://localhost:8080/hooks/in/abc","sign":"stripe","fail_first":2}' >/dev/null
$ curl -s -X POST localhost:8080/_admin/clock/advance -d '{"ms":60000}' >/dev/null
$ curl -s localhost:8080/_admin/events | jq -c 'select(.kind=="WebhookOut") | .attempt'
1
2
3
```

Retries must fire at virtual times matching the configured backoff, and the delivered signature must verify against the endpoint secret.

---

### Phase 8 — telemetry chaos

This is the payload for "test dev tools." A well-behaved telemetry source is easy; every observability tool works against one. What nobody can test against is a source that emits *plausibly broken* telemetry on demand. That is what this phase builds.

`POST /_admin/telemetry/faults` takes a `TelemetryFault`. Applied in the exporter shim only (invariant 11).

```
$ curl -s -X POST localhost:8080/_admin/telemetry/faults \
    -d '{"rate":1.0,"orphan_spans":true}' >/dev/null
$ curl -s localhost:8080/api/ping >/dev/null && sleep 2
$ curl -s 'localhost:16686/api/traces?service=testbed&limit=1' \
    | jq '.data[0].spans[0].references | length'
1
# ...referencing a parent span id that appears nowhere in the trace

$ curl -s -X POST localhost:8080/_admin/telemetry/faults \
    -d '{"rate":1.0,"clock_skew_ms":3600000}' >/dev/null
# exported span start times land an hour in the future

$ curl -s -X POST localhost:8080/_admin/telemetry/faults \
    -d '{"rate":1.0,"cardinality_bomb":50000}' >/dev/null
# unique label values per metric emission; watch your backend fall over — that is the test
```

`cardinality_bomb` will genuinely degrade Prometheus. That is intentional and it is why the whole obs stack sits behind a compose profile. Document the blast radius in the scenario file.

---

### Phase 9 — SQLite snapshot

`POST /_admin/snapshot` writes control-plane state; `--restore <path>` reloads it. Data plane is explicitly *not* snapshotted. Telemetry fault config is control plane and therefore *is* snapshotted.

---

## 8. Traps

**T1 — `axum` 0.8 path syntax.** Params are `{id}`, not `:id`. The 0.7 form compiles and then 404s at runtime.

**T2 — Compose healthchecks are mandatory.** Use `depends_on: { postgres: { condition: service_healthy } }`. Without it the server races infra on cold boot and CI fails intermittently in a way that looks like flaky tests.

**T3 — The Redis scheduler poll must be atomic.** `ZRANGEBYSCORE` followed by `ZREM` is a race; two pollers double-deliver. Use a Lua script that does both. `ZPOPMIN` is not a substitute — it ignores the score bound, so it will pop jobs that are not due yet.

**T4 — `tokio::sync::broadcast` drops for slow receivers.** `RecvError::Lagged(n)` must be handled by emitting `EventKind::Gap { dropped: n }` downstream. A silently truncated event log is worse than no event log, because the UI will look correct.

**T5 — `search_path` is per-connection.** Pooled connections are handed out fresh and will not carry it. Set it in `PoolOptions::after_connect`, not once at startup.

**T6 — WebSocket close needs an explicit Close frame.** Dropping the handle leaves the client waiting on a read timeout, which is a different failure mode than a disconnect and will silently invalidate reconnection-logic tests.

**T7 — Mailpit does not namespace.** No per-run inbox exists. The `X-Testbed-Run` header on send plus filtering on read is the entire isolation story.

**T8 — SSE dies behind proxies without keep-alive.** Use `axum::response::sse::KeepAlive`.

**T9 — `sqlx` compile-time verification needs a live DB.** Either run `cargo sqlx prepare` and commit `.sqlx/`, or use the runtime-checked query API. Otherwise CI builds fail without infra.

**T10 — A queue job's span links to the enqueue span; it does not descend from it.** Parenting is the intuitive choice and it is wrong: a job delayed 30 minutes produces a 30-minute trace, and once a few of those exist every trace-waterfall UI you point at the testbed becomes unusable. Use `FOLLOWS_FROM` / span links. Gated in Phase 4.

**T11 — The OTLP batch exporter drops spans on shutdown.** Without an explicit `shutdown_tracer_provider()` on a signal handler, the last batch vanishes — which reliably eats exactly the spans from whatever you were investigating.

**T12 — `tracing`'s span fields are fixed at creation.** You cannot attach an attribute discovered later (a status code, a job outcome) without declaring the field as `tracing::field::Empty` up front and `record()`-ing it afterwards. Forgetting this yields spans that are silently missing their most useful attribute.

**T13 — Do not emit a bus event from inside the OTLP export path.** Export emits an event, the event triggers instrumentation, instrumentation queues a span, export runs again. The recursion is not immediately obvious because the batch exporter delays it. The export path is exempt from invariant 4.

**T14 — The metrics endpoint must read the virtual clock for anything time-derived.** Queue depth and job age are domain state, so `job_age_seconds` computed from wall time will disagree with the queue itself the moment the clock is advanced, and the disagreement looks like a queue bug rather than a metrics bug.

---

## 9. Task queue

Ordered. Each item is done when its acceptance criterion passes.

1. ~~**Resolve Q1 with the operator.**~~ Done 2026-09-01 — see §3, all five resolved.
2. **Phase 0 scaffold.** → Phase 0 gate passes. *Done 2026-09-01: workspace builds, all nine crates plus `server`; compose has postgres/redis/mailpit with healthchecks and collector/jaeger/prometheus under `obs`. The `docker compose ps` half of the gate is unverified — Docker was not running on the machine that did the restructure.*
3. **`core::clock`.** → advancing 30s completes in <10ms wall time; grep gate returns exit 1. *Done 2026-09-01: `crates/core/src/clock.rs`, 6 tests. Both halves verified.*
4. **`core::state` layered config.** → property test: arbitrary overlay mutations followed by `reset` always yield a `Resolved` equal to boot state. *Types exist in `crates/core/src/config.rs` (`Scenario`/`Overlay`/`Resolved`); `State`, `mutate`, `reset` and resolution are still to write.*
5. **`core::bus` per Q1.** → 1000 events with a deliberately slow subscriber produce `Gap` events summing to exactly the number dropped.
6. **Fault tower layer.** → Phase 2 gate.
7. **Admin routes + `/_admin/events` SSE.** → an HTTP request through a fault appears on the event stream within 50ms with the fault named in `faults`.
8. **Resolve Q5**, then the telemetry spine. → Phase 2b gate. Both halves matter: inbound `traceparent` continued, and the bus event's `trace_id` equal to the collector's.
9. **Resolve Q2**, then data plane + isolation. → Phase 3 gate.
10. **Queue.** → Phase 4 gate, including both the `real` time assertion and the `FOLLOWS_FROM` link assertion.
11. **Resolve Q3**, then ws + streams. → Phase 5 gate. Connection span with per-frame children linked to it.
12. Mail facade. → Phase 6 gate.
13. **Resolve Q4**, then webhooks. → Phase 7 gate. Outbound requests must carry an injected `traceparent`.
14. Telemetry chaos. → Phase 8 gate. Each `TelemetryFault` field independently verifiable at the collector.
15. Snapshot. → restore reproduces control-plane state including telemetry fault config; data plane untouched.
16. Scenario library in `scenarios/` — at minimum: `flaky-api`, `slow-queue`, `chatty-ws`, `webhook-storm`, `broken-traces`, `cardinality-bomb`.

---

## 10. Non-goals

- Not a production server. No auth hardening, no rate limiting beyond simulated faults.
- Not a recording proxy or general-purpose mock server. Scenarios are authored, not captured.
- No multi-tenancy beyond `RunId`.
- No UI in v1. `/_admin/events` is the contract a UI would later consume; keep it stable and typed.
- **Not an observability backend.** The testbed emits telemetry; it does not store, index, or query it. Jaeger and Prometheus in compose are there to be pointed at, not to be reimplemented.
- **No OTLP logs surface in v1.** Traces and metrics only. Logs are a reasonable v2 addition and the exporter shim is the place they would go.
- Not a load generator. `cardinality_bomb` is a correctness stressor for a backend's cardinality handling, not a throughput benchmark.
- Rust only. No polyglot agent protocol.