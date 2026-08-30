.PHONY: help
help:
	@echo ""
	@echo "  env           Copy .env.example -> .env"
	@echo "  run           Build and run the server"
	@echo "  test          Run Rust tests"
	@echo "  fmt           Format code with cargo fmt"
	@echo "  clippy       Run cargo clippy"
	@echo ""
	@echo "  migrate-up    Run database migrations"
	@echo "  migrate-down   Rollback database migrations"
	@echo ""
	@echo "  docker-up     Start server in docker"
	@echo "  docker-down    Stop docker containers"
	@echo ""

.PHONY: env
env:
	if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "created .env"; \
	else \
		echo "skipped .env (already exists)"; \
	fi

.PHONY: run
run:
	cargo run

.PHONY: test
test:
	cargo test

.PHONY: fmt
fmt:
	cargo fmt

.PHONY: clippy
clippy:
	cargo clippy -- -D warnings

.PHONY: migrate-up
migrate-up:
	@echo "Running migrations..."
	# Assuming use of a tool like 'golang-migrate' or similar, or a custom script
	# Example using psql:
	# psql  -f migrations/001_initial_schema.sql

.PHONY: migrate-down
migrate-down:
	@echo "Rolling back migrations..."
	# Example: psql  -c "DROP TABLE email_logs; DROP TABLE jobs;"

.PHONY: docker-up
docker-up:
	docker compose up -d

.PHONY: docker-down
docker-down:
	docker compose down
