//! Chunk pacing, against the virtual clock.
//!
//! # Why a poll loop and not `sleep(delay)`
//!
//! A `tokio::time::sleep(delay)` between chunks would schedule against wall
//! time, and `POST /_admin/clock/advance` could never flush a deliberately slow
//! stream — which makes the `slow-stream` scenario untestable except by
//! actually waiting for it. Invariant 7 says the virtual clock is authoritative
//! for all scheduling, and inter-chunk delay is scheduling.
//!
//! So this mirrors the queue scheduler exactly: the loop wakes on a short real
//! interval because something has to, and what it does when it wakes is decided
//! entirely by comparing [`Clock::now`] against the chunk's due time.
//!
//! The fault layer's latency injection is the deliberate exception in the other
//! direction — see `testbed_http::fault`. That one *is* real, because it exists
//! for a client measuring a timeout, and it schedules nothing.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use testbed_core::Clock;

/// How often the pacer wakes, in real time. Bounds how long after a
/// `clock/advance` the next chunk is written.
pub const TICK: Duration = Duration::from_millis(10);

/// Waits until virtual `due`, or returns immediately if it has already passed.
pub async fn wait_until(clock: &Clock, due: DateTime<Utc>) {
    // The overwhelmingly common case: `delay_ms` is 0, so the whole stream is
    // written as fast as the client can read it.
    if clock.now() >= due {
        return;
    }

    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if clock.now() >= due {
            return;
        }
    }
}

/// The due time of chunk `seq`, counting from `start`.
pub fn due_at(start: DateTime<Utc>, seq: u32, delay_ms: u64) -> DateTime<Utc> {
    start + TimeDelta::milliseconds((delay_ms * seq as u64) as i64)
}

/// Splits `text` into the pieces a token-by-token stream emits.
///
/// Whitespace-delimited, with the separator kept on the *front* of each piece
/// after the first, so concatenating every delta reproduces the input exactly.
/// A client that joins the deltas and gets `helloworld` has found a real bug in
/// its own assembly, and this must not hide it.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    for (i, word) in text.split_whitespace().enumerate() {
        tokens.push(if i == 0 {
            word.to_string()
        } else {
            format!(" {word}")
        });
    }
    tokens
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn deltas_rejoin_into_the_original_text() {
        for text in ["hi", "hello there world", "one"] {
            assert_eq!(tokenize(text).concat(), text);
        }
    }

    #[test]
    fn empty_input_yields_no_tokens() {
        assert!(tokenize("   ").is_empty());
    }

    #[test]
    fn chunk_due_times_are_evenly_spaced_in_virtual_time() {
        let start = Utc::now();
        assert_eq!(due_at(start, 0, 250), start);
        assert_eq!(due_at(start, 3, 250), start + TimeDelta::milliseconds(750));
    }

    /// The point of the whole module: 10 virtual seconds of pacing costs
    /// milliseconds of real time once the clock is advanced past it.
    #[tokio::test]
    async fn advancing_the_clock_releases_a_pending_chunk() {
        let clock = Arc::new(Clock::new());
        let due = due_at(clock.now(), 1, 10_000);

        let waiter = {
            let clock = Arc::clone(&clock);
            tokio::spawn(async move { wait_until(&clock, due).await })
        };

        // Let the pacer reach its first tick, then jump past the due time.
        tokio::time::sleep(TICK * 2).await;
        clock.advance(Duration::from_millis(10_000));

        tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("chunk still pending after the clock passed its due time")
            .unwrap();
    }

    #[tokio::test]
    async fn a_due_chunk_does_not_wait_at_all() {
        let clock = Clock::new();
        let due = clock.now();
        tokio::time::timeout(Duration::from_millis(5), wait_until(&clock, due))
            .await
            .expect("an already-due chunk waited");
    }
}
