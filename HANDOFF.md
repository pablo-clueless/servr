# Project Handoff: Rust Multi-Service Test Server

## Overview
This project is a specialized Rust-based server designed to serve as a versatile backend for testing various integration scenarios. It provides REST JSON endpoints, webhook handling, WebSocket support, an internal job queue, and SMTP services.

## Core Technical Stack
- **Language:** Rust
- **Web Framework:** `axum` (High-performance, ergonomic web framework)
- **Async Runtime:** `tokio` (Industry standard for async I/O in Rust)
- **Serialization:** `serde` & `serde_json` (For robust JSON handling)
- **Email:** `lettre` (For SMTP service implementation)
- **State Management:** `axum::extract::State` with `Arc` for shared server state

## Architectural Components

### 1. REST API & Webhooks
- **Implementation:** Located in `src/api/`.
- **Schemas:** Leverages existing models in `src/schema/` (e.g., `Response<T>`, `Paginated<T>`).
- **Functionality:** Serves JSON responses and provides endpoints to receive and process external webhooks.

### 2. WebSocket Service
- **Implementation:** Located in `src/websocket/`.
- **Functionality:** Handles real-time bidirectional communication for test clients.

### 3. Internal Job Queue
- **Implementation:** Located in `src/queue/`.
- **Mechanism:** Uses `tokio::sync::mpsc` channels to pass jobs from API handlers to a dedicated background worker task.
- **Workflow:** 
  - Request $\rightarrow$ API Handler $\rightarrow$ Queue Channel $\rightarrow$ Background Worker $\rightarrow$ Execution.

### 4. SMTP Service
- **Implementation:** Located in `src/smtp/`.
- **Schema:** Uses `SendEmail` struct from `src/schema/email.rs`.
- **Library:** Powered by `lettre` for reliable email dispatch.

### 5. Keep-Alive Mechanism (Self-Ping)
- **Implementation:** Located in `src/cron/` or as a spawned task in `main.rs`.
- **Mechanism:** A dedicated `tokio::spawn` task that uses `tokio::time::interval` to send an HTTP GET request to the server's own health check endpoint every 10 minutes.

## Project Structure
```text
src/
├── api/        # REST and Webhook handlers
├── cron/       # Scheduled tasks and Keep-Alive logic
├── queue/      # Job queue worker and logic
├── smtp/       # Email service implementation
├── websocket/ # WebSocket handlers and state
├── schema/     # Data models and API response wrappers
├── state.rs    # Shared application state
├── error.rs    # Custom error types and handling
└── main.rs     # Entry point and service orchestration
```

## Implementation Roadmap
1. **Infrastructure:** Setup `Cargo.toml` with necessary dependencies.
2. **State & Error:** Define global server state and error handling in `state.rs` and `error.rs`.
3. **API Base:** Implement basic Axum routes and JSON responses.
4. **Queue System:** Build the MPSC channel and background worker loop.
5. **SMTP & WebSockets:** Implement the specific service logic.
6. **Keep-Alive:** Add the self-pinging background task.
7. **Verification:** End-to-end testing of the flow: Webhook $\rightarrow$ Queue $\rightarrow$ SMTP/WS.
