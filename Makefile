# Local overrides live in .env (gitignored). Loading it here keeps the gates
# working against whatever credentials and ports compose actually used, rather
# than hardcoding values that silently drift from it.
-include .env

DATABASE_URL ?= postgres://testbed:testbed@localhost:5432/testbed
MAILPIT_API  ?= http://localhost:8025
REDIS_URL    ?= redis://localhost:6379

.PHONY: help
help:
	@echo ""
	@echo "  up            Start the data plane (postgres, redis, mailpit)"
	@echo "  up-obs        Start data plane + collector, jaeger, prometheus"
	@echo "  down          Stop compose and drop volumes"
	@echo "  run           Run the testbed server on :8080"
	@echo "  test          cargo test --workspace"
	@echo "  fmt           cargo fmt --all"
	@echo "  clippy        cargo clippy --workspace --all-targets -D warnings"
	@echo "  invariants    Run the grep gates CI enforces"
	@echo "  gate-0        Phase 0 gate: infra healthy + workspace builds"
	@echo "  gate-5        Phase 5 gate: ws + streams, in-process"
	@echo "  gate-6        Phase 6 gate: mail through Mailpit, run-isolated"
	@echo "  gate-7        Phase 7 gate: webhooks, signing + virtual backoff"
	@echo "  gate-8        Phase 8 gate: telemetry chaos (obs profile for live)"
	@echo ""
	@echo "  Jaeger  http://localhost:16686   Mailpit http://localhost:8025"
	@echo "  Prom    http://localhost:9090    Admin   http://localhost:8080/_admin/health"
	@echo ""

.PHONY: up
up:
	docker compose up -d --wait

.PHONY: up-obs
up-obs:
	docker compose --profile obs up -d --wait

.PHONY: down
down:
	docker compose down -v

.PHONY: run
run:
	DATABASE_URL="$(DATABASE_URL)" REDIS_URL="$(REDIS_URL)" cargo run -p testbed-server

.PHONY: test
test:
	cargo test --workspace --no-fail-fast

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: clippy
clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# Invariant 1 (HANDOFF §5). Exit 0 means no violations.
.PHONY: invariants
invariants:
	@if rg -n 'SystemTime::now|Instant::now' crates/ server/ \
	      --glob '!crates/core/src/clock.rs' \
	      --glob '!crates/telemetry/src/wall.rs'; then \
	  echo "FAIL: wall-clock read outside the two sanctioned files"; exit 1; \
	else echo "ok: no wall-clock reads outside clock.rs and wall.rs"; fi
	@if rg -n 'sqlx|postgres|PgPool' crates/admin/; then \
	  echo "FAIL: control plane touches Postgres"; exit 1; \
	else echo "ok: control plane performs no Postgres I/O"; fi

.PHONY: gate-0
gate-0: up invariants
	docker compose ps --format '{{.Service}} {{.Health}}'
	cargo build --workspace 2>&1 | tail -1

# Phase 2b gate. Needs the obs profile up.
.PHONY: gate-2b
gate-2b:
	@TP="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"; \
	curl -s localhost:8080/api/ping -H "traceparent: $$TP" > /dev/null; \
	sleep 2; \
	echo "jaeger traceID:"; \
	curl -s 'localhost:16686/api/traces?service=testbed&limit=1' | jq -r '.data[0].traceID'; \
	echo "expected:      4bf92f3577b34da6a3ce929d0e0e4736"; \
	echo "testbed_ metrics:"; \
	curl -s localhost:8080/metrics | grep -c '^testbed_'

# Phase 3 gate. Needs Postgres; makes the isolation tests real rather than skipped.
.PHONY: gate-3
gate-3: up
	DATABASE_URL="$(DATABASE_URL)" \
	  cargo test -p testbed-server --test run_isolation -- --nocapture

# Phase 5 gate, in-process. Needs no infra: the ws half boots the assembled
# router on an ephemeral port and drives it with a real WebSocket client (T6 is
# only observable to one that inspects the close frame); the stream half
# collects SSE bodies through the same router.
.PHONY: gate-5
gate-5:
	cargo test -p testbed-server --test ws_gate --test stream_gate -- --nocapture
	cargo test -p testbed-ws --test span_links -- --nocapture

# The same gate as §7 writes it, against a server already running on :8080.
# Needs websocat; the in-process `gate-5` is the one CI runs.
.PHONY: gate-5-live
gate-5-live:
	@echo "=== ws: publish echoes, then kill closes cleanly (T6) ==="
	@websocat -t "ws://localhost:8080/ws?topic=demo" & sleep 0.5; curl -s -X POST localhost:8080/_admin/ws/publish -d '{"topic":"demo","body":"hi"}'; echo; sleep 0.5; curl -s -X POST localhost:8080/_admin/ws/kill -d '{"topic":"demo"}'; echo; sleep 0.5
	@echo "=== stream: openai-compatible chunks ==="
	@curl -sN localhost:8080/v1/chat/completions -d '{"stream":true,"messages":[{"role":"user","content":"hi"}]}' | head -3

# Phase 7 gate, in-process. Needs no infra: the sender delivers to the
# testbed's own capture inbox over a real socket, which is what makes signing,
# traceparent injection and capture testable in one pass.
.PHONY: gate-7
gate-7:
	cargo test -p testbed-server --test webhook_gate -- --nocapture
	cargo test -p testbed-hooks -- --nocapture

# Phase 8 gate, in-process. The shim is asserted against real `SpanData` and
# real exposition text, so it needs no collector; `gate-8-live` is the half that
# proves it at Jaeger.
.PHONY: gate-8
gate-8:
	cargo test -p testbed-telemetry -- --nocapture

# Phase 6 gate. Needs Mailpit; skips itself without MAILPIT_API rather than
# failing, so it runs the moment infra is up.
.PHONY: gate-6
gate-6: up
	MAILPIT_API="$(MAILPIT_API)" cargo test -p testbed-mail -- --nocapture

# RedisStore against live Redis. Proves the Lua claim script (T3) actually runs.
.PHONY: gate-redis
gate-redis: up
	REDIS_URL="$(REDIS_URL)" \
	  cargo test -p testbed-queue --test redis_store -- --nocapture

# Everything the dev container unblocks, in dependency order. Each is also
# runnable on its own; this is the "prove the whole tree" pass.
.PHONY: gates
gates: up invariants
	@echo "=== phase 0: infra healthy ==="
	docker compose ps --format '{{.Service}} {{.Health}}'
	@echo "=== phase 3: run isolation (needs postgres) ==="
	DATABASE_URL="$(DATABASE_URL)" \
	  cargo test -p testbed-server --test run_isolation -- --nocapture
	@echo "=== redis store: the Lua claim script (T3) ==="
	REDIS_URL="$(REDIS_URL)" \
	  cargo test -p testbed-queue --test redis_store -- --nocapture
	@echo "=== phase 5: ws + streams (needs no infra) ==="
	$(MAKE) gate-5
	@echo "=== phase 6: mail isolation (needs mailpit) ==="
	MAILPIT_API="$(MAILPIT_API)" \
	  cargo test -p testbed-mail --test mailpit -- --nocapture
	@echo "=== phase 7: webhooks (needs no infra) ==="
	$(MAKE) gate-7
	@echo "=== phase 8: telemetry chaos (needs no infra) ==="
	$(MAKE) gate-8
	@echo
	@echo "phase 2b still needs the obs profile: make up-obs && make gate-2b"
