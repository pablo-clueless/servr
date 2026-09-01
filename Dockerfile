# The testbed normally runs on the host against the compose data plane; this
# image exists for CI and for pointing remote tooling at a deployed instance.

FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY . .
ENV RUSTFLAGS="-C target-cpu=generic"
RUN cargo build --release -p testbed-server

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/testbed /app/testbed
COPY --from=builder /app/scenarios /app/scenarios
EXPOSE 8080
CMD ["./testbed"]
