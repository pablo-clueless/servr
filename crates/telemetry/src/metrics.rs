//! The Prometheus `/metrics` surface.
//!
//! # Trap T14
//!
//! Anything time-derived here reads the **virtual** clock. Queue depth and job
//! age are domain state, so a `job_age_seconds` computed from wall time
//! disagrees with the queue itself the moment the clock is advanced — and the
//! disagreement reads as a queue bug rather than a metrics bug, which is a
//! genuinely expensive afternoon.
//!
//! The baseline set is fixed by HANDOFF §7 phase 2b: RED per surface plus the
//! testbed's own gauges. `testbed_events_dropped_total` is the `Gap` counter
//! from trap T4, and is how you notice the event log is lying to you.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Names are part of the Phase 2b gate (`grep -c '^testbed_'` must be ≥ 6), so
/// they are declared in one place rather than spelled at each call site.
pub mod name {
    pub const HTTP_REQUESTS: &str = "testbed_http_requests_total";
    pub const HTTP_LATENCY: &str = "testbed_http_request_duration_seconds";
    pub const FAULTS_APPLIED: &str = "testbed_faults_applied_total";
    pub const QUEUE_DEPTH: &str = "testbed_queue_depth";
    pub const JOBS: &str = "testbed_jobs_total";
    pub const WS_CONNECTIONS: &str = "testbed_ws_connections";
    pub const WEBHOOK_ATTEMPTS: &str = "testbed_webhook_attempts_total";
    /// The `Gap` counter (T4).
    pub const EVENTS_DROPPED: &str = "testbed_events_dropped_total";
    pub const EVENT_SUBSCRIBERS: &str = "testbed_event_subscribers";
    /// Virtual minus wall time. Makes an advanced clock visible to anything
    /// scraping, so a surprising graph has an explanation on the same dashboard.
    pub const CLOCK_OFFSET: &str = "testbed_clock_offset_seconds";
}

/// Installs the Prometheus recorder and returns the handle `/metrics` renders.
///
/// Every metric is described and seeded to zero here. An unseeded counter is
/// absent from the scrape until it first fires, which makes `rate()` over it
/// silently wrong for the first interval and makes the gate's `grep -c` depend
/// on traffic having happened.
pub fn install() -> Result<PrometheusHandle, String> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("failed to install Prometheus recorder: {e}"))?;

    describe_and_seed();
    Ok(handle)
}

fn describe_and_seed() {
    use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, Unit};

    describe_counter!(name::HTTP_REQUESTS, "Requests served, by method and status");
    describe_histogram!(
        name::HTTP_LATENCY,
        Unit::Seconds,
        "Wall-clock request duration, injected latency included"
    );
    describe_counter!(name::FAULTS_APPLIED, "Fault effects applied, by effect");
    describe_gauge!(name::QUEUE_DEPTH, "Jobs not yet in a terminal state");
    describe_counter!(name::JOBS, "Job state transitions, by resulting state");
    describe_gauge!(name::WS_CONNECTIONS, "Open WebSocket connections");
    describe_counter!(
        name::WEBHOOK_ATTEMPTS,
        "Outbound webhook attempts, by response status"
    );
    describe_counter!(
        name::EVENTS_DROPPED,
        "Bus events dropped for lagging subscribers (trap T4)"
    );
    describe_gauge!(name::EVENT_SUBSCRIBERS, "Live subscribers on the event bus");
    describe_gauge!(
        name::CLOCK_OFFSET,
        Unit::Seconds,
        "Virtual clock offset from wall time"
    );

    // Seed so the series exist before any traffic.
    counter!(name::EVENTS_DROPPED).absolute(0);
    gauge!(name::QUEUE_DEPTH).set(0.0);
    gauge!(name::WS_CONNECTIONS).set(0.0);
    gauge!(name::EVENT_SUBSCRIBERS).set(0.0);
    gauge!(name::CLOCK_OFFSET).set(0.0);
}

/// Records one served request. Called from the fault layer, which is the only
/// place that knows which faults fired.
pub fn record_request(method: &str, status: u16, latency: std::time::Duration, faults: &[String]) {
    use metrics::{counter, histogram};

    counter!(
        name::HTTP_REQUESTS,
        "method" => method.to_string(),
        "status" => status.to_string(),
    )
    .increment(1);

    histogram!(name::HTTP_LATENCY, "method" => method.to_string()).record(latency.as_secs_f64());

    for fault in faults {
        counter!(name::FAULTS_APPLIED, "effect" => fault.clone()).increment(1);
    }
}

/// Publishes bus and clock state. Called on scrape rather than continuously, so
/// the numbers are read at the moment they are asked for.
///
/// `clock_offset` comes from the virtual clock (T14).
pub fn observe_runtime(dropped: u64, subscribers: usize, clock_offset_ms: i64) {
    use metrics::{counter, gauge};

    counter!(name::EVENTS_DROPPED).absolute(dropped);
    gauge!(name::EVENT_SUBSCRIBERS).set(subscribers as f64);
    gauge!(name::CLOCK_OFFSET).set(clock_offset_ms as f64 / 1000.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_metric_name_carries_the_gate_prefix() {
        let names = [
            name::HTTP_REQUESTS,
            name::HTTP_LATENCY,
            name::FAULTS_APPLIED,
            name::QUEUE_DEPTH,
            name::JOBS,
            name::WS_CONNECTIONS,
            name::WEBHOOK_ATTEMPTS,
            name::EVENTS_DROPPED,
            name::EVENT_SUBSCRIBERS,
            name::CLOCK_OFFSET,
        ];

        for name in names {
            assert!(
                name.starts_with("testbed_"),
                "{name} would not be counted by the phase 2b gate"
            );
        }
        // The gate greps for at least 6 distinct `testbed_` metrics.
        assert!(names.len() >= 6);
    }
}
