//! Wires the surfaces together and serves them on one port.
//!
//! The router is assembled in two halves on purpose: the data plane goes
//! through the fault layer, the control plane does not. See
//! `testbed_http::fault` for why that separation is load-bearing.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use testbed_core::{BroadcastBus, Clock, RunId, Scenario, State};
use tokio::signal;

/// Per-subscriber backlog on the event bus. A subscriber falling further behind
/// than this lags, and lagging is reported as `EventKind::Gap`, never hidden.
const BUS_CAPACITY: usize = 1024;

#[tokio::main]
async fn main() {
    let run = RunId::new();

    // Order matters: the exporter shim reads its faults from `State`, so the
    // control plane has to exist before telemetry is installed. That puts both
    // branches below ahead of the subscriber, which is why their failures are
    // `eprintln!` — there is nothing to log through yet, and a fatal boot error
    // has to be visible regardless.
    //
    // `--restore <path>` boots from a snapshot instead of a scenario file
    // (HANDOFF §7 phase 9). Restoring at boot rather than through the admin API
    // is deliberate: swapping the control plane under a running server would
    // leave in-flight requests, queued jobs and open connections pointing at a
    // run that no longer matches.
    let restore = restore_path();

    let (run, clock, state) = match &restore {
        Some(path) => {
            let snapshot = match testbed_core::Snapshot::read(path) {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            };

            let run = snapshot.run;
            let clock = Arc::new(snapshot.restore_clock());
            let bus = Arc::new(BroadcastBus::new(BUS_CAPACITY, Arc::clone(&clock), run));
            let state = Arc::new(State::new(
                snapshot.base.clone(),
                Arc::clone(&clock),
                bus,
                run,
            ));
            // The overlay is applied after construction so `base` stays exactly
            // what the snapshot recorded — `reset` has to land back there
            // (invariant 2), not on the overlay that was live at capture.
            state.mutate(|overlay| *overlay = snapshot.overlay.clone());
            (run, clock, state)
        }
        None => {
            let scenario_path = std::env::var("TESTBED_SCENARIO")
                .unwrap_or_else(|_| "scenarios/default.toml".to_string());
            let scenario = match Scenario::from_path(&scenario_path) {
                Ok(scenario) => scenario,
                Err(e) => {
                    // Booting with a silently empty scenario would make every
                    // later assertion meaningless, so this is fatal.
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            };

            let clock = Arc::new(Clock::new());
            let bus = Arc::new(BroadcastBus::new(BUS_CAPACITY, Arc::clone(&clock), run));
            let state = Arc::new(State::new(scenario, Arc::clone(&clock), bus, run));
            (run, clock, state)
        }
    };

    let telemetry = Arc::new(testbed_telemetry::init(
        run,
        Arc::new(testbed_telemetry::chaos::FromState(Arc::clone(&state))),
    ));

    if let Some(path) = &restore {
        tracing::info!(
            %path,
            run = %run,
            clock_offset_ms = clock.offset_ms(),
            "restored control plane from snapshot; the data plane was not restored"
        );
    }

    tracing::info!(
        run = %run,
        scenario = %state.base().name,
        schema = %run.schema(),
        faults = state.resolved().faults.len(),
        otlp = %testbed_telemetry::otlp_endpoint(),
        exporting = telemetry.exporting(),
        "testbed starting"
    );
    if !telemetry.exporting() {
        tracing::warn!(
            "no OTLP collector reachable; run `docker compose --profile obs up -d` for traces"
        );
    }
    if let Some(blast) = &state.base().blast_radius {
        tracing::warn!(blast_radius = %blast, "scenario blast radius");
    }
    // A scenario that seeds telemetry corruption has to say so at boot, or the
    // first person to look at the collector spends the afternoon debugging the
    // testbed instead of using it.
    let seeded = &state.base().telemetry;
    if seeded.rate > 0.0 {
        tracing::warn!(
            rate = seeded.rate,
            "scenario seeds telemetry faults; exported telemetry is deliberately wrong"
        );
    }

    // The data plane is optional. Without Postgres the HTTP, telemetry and
    // control-plane surfaces all still work, and `/api/items` answers 503 with
    // an explanation — refusing to boot would make phases 0-2b unusable.
    let data = match std::env::var("DATABASE_URL") {
        Ok(url) => match testbed_http::data::DataPlane::connect(&url).await {
            Ok(plane) => {
                let plane = Arc::new(plane);
                if let Err(e) = plane.create_run(run).await {
                    tracing::warn!("could not prepare the default run's schema: {e}");
                }
                tracing::info!(schema = %run.schema(), "data plane ready");
                Some(plane)
            }
            Err(e) => {
                tracing::warn!("Postgres unreachable ({e}); /api/items will answer 503");
                None
            }
        },
        Err(_) => {
            tracing::warn!("DATABASE_URL unset; /api/items will answer 503");
            None
        }
    };

    // The scheduler polls against the virtual clock either way; Redis only
    // changes where jobs live, and whether they survive a restart.
    let mut redis_backed = false;
    let store: Arc<dyn testbed_queue::JobStore> = match std::env::var("REDIS_URL") {
        Ok(url) => {
            match testbed_queue::RedisStore::connect(&url, run).await {
                Ok(store) => {
                    tracing::info!("queue backed by Redis");
                    redis_backed = true;
                    Arc::new(store)
                }
                Err(e) => {
                    tracing::warn!("Redis unreachable ({e}); queue is in-memory and will not survive a restart");
                    Arc::new(testbed_queue::MemoryStore::new())
                }
            }
        }
        Err(_) => {
            tracing::warn!("REDIS_URL unset; queue is in-memory and will not survive a restart");
            Arc::new(testbed_queue::MemoryStore::new())
        }
    };

    let scheduler = Arc::new(testbed_queue::Scheduler::new(
        store,
        Arc::clone(&clock),
        Arc::clone(state.bus()),
        run,
    ));
    tokio::spawn(Arc::clone(&scheduler).run_forever());

    // Mailpit, like Postgres and Redis, is optional: without it `/_admin/mail/*`
    // answers 503 and every other surface is unaffected. The probe is a real
    // request rather than a lazy transport, because lettre would happily accept
    // sends into a void and the first sign of trouble would be an empty inbox —
    // which is indistinguishable from working isolation (T7).
    let mailer = {
        let config = testbed_mail::MailConfig::from_env();
        match testbed_mail::Mailer::new(
            config.clone(),
            Arc::clone(state.bus()),
            Arc::clone(&clock),
            run,
        ) {
            Ok(mailer) => match mailer.probe().await {
                Ok(version) => {
                    tracing::info!(smtp = %config.smtp, api = %config.api, %version, "mailpit ready");
                    Some(Arc::new(mailer))
                }
                Err(e) => {
                    tracing::warn!(
                        "Mailpit unreachable at {} ({e}); /_admin/mail will answer 503",
                        config.api
                    );
                    None
                }
            },
            Err(e) => {
                tracing::warn!(
                    "mail client could not be built ({e}); /_admin/mail will answer 503"
                );
                None
            }
        }
    };

    // Webhooks. The sender is a virtual-time poll loop of its own — see
    // `testbed_hooks::outbound` for why it is not routed through the queue.
    let hooks = Arc::new(testbed_hooks::Hooks::new(
        Arc::clone(state.bus()),
        Arc::clone(&clock),
        run,
    ));
    tokio::spawn(Arc::clone(&hooks.sender).run_forever());

    let hub = Arc::new(testbed_ws::Hub::new(
        Arc::clone(state.bus()),
        Arc::clone(&clock),
        run,
    ));
    let streams = testbed_stream::Streams::new(Arc::clone(state.bus()), Arc::clone(&clock), run);

    // What actually came up, for `GET /`. Read from the wiring above rather
    // than re-probed, so the index cannot disagree with the boot log.
    let surfaces = testbed_admin::Surfaces {
        postgres: data.is_some(),
        redis: redis_backed,
        mailpit: mailer.is_some(),
        tracing: telemetry.exporting(),
    };

    let app = Router::new()
        .merge(testbed_admin::index_router(Arc::clone(&state), surfaces))
        .merge(testbed_admin::router(Arc::clone(&state)))
        .merge(testbed_admin::jobs_router(
            Arc::clone(&scheduler),
            Arc::clone(&state),
        ))
        .merge(testbed_admin::runs_router(data.clone()))
        .merge(testbed_admin::ws_router(Arc::clone(&hub)))
        .merge(testbed_admin::mail_router(mailer, Arc::clone(&state)))
        .merge(testbed_admin::hooks_router(Arc::clone(&hooks)))
        .merge(testbed_admin::metrics_route(
            Arc::clone(&state),
            Arc::clone(&telemetry),
        ))
        .merge(testbed_http::router_with_data(Arc::clone(&state), data))
        // ws and stream live in their own crates, so `http` cannot mount them
        // (§4 forbids the cross-surface edge). They get the same fault layer
        // through `guard` rather than a second copy of the wiring — a surface
        // that quietly ends up unfaultable is what invariant 8 exists to stop.
        .merge(testbed_http::fault::guard(
            Arc::clone(&state),
            testbed_ws::router(hub),
        ))
        .merge(testbed_http::fault::guard(
            Arc::clone(&state),
            testbed_stream::router(streams),
        ))
        // The capture inbox is faulted too, deliberately: making the receiver
        // flaky is how a sender's retry logic gets tested.
        .merge(testbed_http::fault::guard(
            Arc::clone(&state),
            testbed_hooks::router(Arc::clone(&hooks.inbox)),
        ))
        // Anything no router claimed. axum's default 404 is an empty body,
        // which is indistinguishable from a broken server in a browser.
        .fallback(testbed_admin::not_found);

    // `PORT` is the fallback because that is what every managed host injects
    // (Render, Fly, Heroku); `TESTBED_PORT` still wins so a local gate can move
    // the server off 8080 without competing with whatever set `PORT`.
    let port: u16 = std::env::var("TESTBED_PORT")
        .or_else(|_| std::env::var("PORT"))
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let listener = bind(port).unwrap_or_else(|e| panic!("failed to bind port {port}: {e}"));
    tracing::info!(
        "listening on http://localhost:{port} (admin at {})",
        testbed_admin::PREFIX
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("server error");

    // Trap T11: flush the last span batch, or it takes the spans from whatever
    // you were investigating down with it.
    telemetry.shutdown();
}

/// `--restore <path>`, if given.
///
/// Hand-rolled rather than pulling in an argument parser: the binary has
/// exactly one flag, and every other knob is an environment variable so that
/// `compose.yaml` and the gates can set it without a shell.
fn restore_path() -> Option<String> {
    let mut args = std::env::args().skip(1);
    let arg = args.next()?;

    let path = match arg.as_str() {
        "--restore" => args.next().unwrap_or_else(|| {
            eprintln!("error: --restore needs a path");
            std::process::exit(1);
        }),
        other => match other.strip_prefix("--restore=") {
            Some(path) => path.to_string(),
            None => {
                eprintln!(
                    "error: unknown argument {other:?} (only --restore <path> is understood)"
                );
                std::process::exit(1);
            }
        },
    };

    // Rejected rather than ignored: a mistyped second flag that silently did
    // nothing would be found only by noticing the testbed behaving as though it
    // had never been passed.
    if let Some(extra) = args.next() {
        eprintln!("error: unexpected argument {extra:?}");
        std::process::exit(1);
    }

    Some(path)
}

/// Binds dual-stack, so `localhost` reaches the server over either family.
///
/// This is not incidental tidiness. Every gate in the HANDOFF is written
/// against `localhost`, and a client resolving that to `::1` first will sit
/// through a refused connection before retrying IPv4 — roughly 200ms on
/// Windows. That is invisible in a correctness check and fatal in a timing one:
/// it pushes the Phase 2 gate's 500ms latency assertion past its 600ms ceiling,
/// and it makes the Phase 4 `real 0m0.2xxs` assertion ambiguous.
///
/// Windows defaults `IPV6_V6ONLY` on, unlike Linux, so it is cleared explicitly
/// rather than relying on the platform default. Falls back to IPv4-only where
/// IPv6 is unavailable.
fn bind(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let dual = || -> std::io::Result<std::net::TcpListener> {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_only_v6(false)?;
        socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        Ok(socket.into())
    };

    let std_listener = match dual() {
        Ok(listener) => listener,
        Err(e) => {
            tracing::warn!("dual-stack bind failed ({e}); falling back to IPv4 only");
            let listener =
                std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))?;
            listener.set_nonblocking(true)?;
            listener
        }
    };

    tokio::net::TcpListener::from_std(std_listener)
}

/// Trap T11: once Phase 2b lands, this must call `shutdown_tracer_provider()`.
/// Without it the OTLP batch exporter drops its last batch — which reliably
/// eats exactly the spans from whatever you were investigating.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("shutdown: SIGINT"),
        _ = terminate => tracing::info!("shutdown: SIGTERM"),
    }
}
