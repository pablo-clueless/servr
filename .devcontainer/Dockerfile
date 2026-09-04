# Stable, matching `.github/workflows/ci.yaml`. Deliberately not nightly: if the
# workspace ever stops building on stable, CI should be what says so, not a
# developer discovering it at merge time.
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Tools the HANDOFF §7 gates actually invoke. Missing any of these turns a
# failing gate into a confusing one.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
        jq \
        postgresql-client \
        redis-tools \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

# websocat: the Phase 5 WebSocket gate is written around it, and it is not
# packaged in Debian. Pinned rather than "latest" so a gate never changes
# behaviour because upstream shipped something.
ARG WEBSOCAT_VERSION=1.13.0
RUN ARCH="$(dpkg --print-architecture)" \
    && case "$ARCH" in \
         amd64) WS_ARCH=x86_64 ;; \
         arm64) WS_ARCH=aarch64 ;; \
         *) echo "no websocat build for $ARCH" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /usr/local/bin/websocat \
        "https://github.com/vi/websocat/releases/download/v${WEBSOCAT_VERSION}/websocat.${WS_ARCH}-unknown-linux-musl" \
    && chmod +x /usr/local/bin/websocat

# What the standing checks run. No sqlx-cli: `crates/http/src/items.rs` uses
# the runtime-checked query API precisely so no offline query cache is needed
# (trap T9).
RUN rustup component add clippy rustfmt
