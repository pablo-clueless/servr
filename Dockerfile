# Build stage
FROM rust:nightly-slim-bullseye AS builder

WORKDIR /app
COPY . .
# Use generic target CPU to avoid unstable SIMD feature errors in some crates
ENV RUSTFLAGS="-C target-cpu=generic"
RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

WORKDIR /app
COPY --from=builder /app/target/release/servr /app/servr

# Install runtime dependencies (like openssl and ca-certificates for SMTP/HTTP)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8080
CMD ["./servr"]
