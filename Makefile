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
	cargo run -p testbed-server

.PHONY: test
test:
	cargo test --workspace

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
