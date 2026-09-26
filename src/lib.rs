//! Library crate for busbridge. The binary (main.rs) is a thin
//! wrapper around `cli_main()` here; splitting it this way lets
//! integration tests (tests/) exercise the real dispatch machinery
//! (config loading, `BridgeState`, dispatch.rs, the varlink client/server)
//! directly against a real D-Bus session bus and a fake Varlink backend,
//! rather than only being able to shell out to the compiled binary.
//!
//! See docs/DESIGN_BRIEF_V1.md Section 2.1 for the resident-vs-handoff decision made on
//! every invocation, and Section 6 for the suggested build order.
//!
//! Responsibilities:
//!   1. Parse args/env (config dir override, `stats` subcommand per 3.8).
//!   2. Try connecting to the control socket (control.rs). If that
//!      succeeds, hand off and exit (2.1 step 2).
//!   3. Otherwise become the resident instance: load config, connect to
//!      the bus(es), RequestName, start the varlink listener
//!      (varlink/server.rs), and run the dispatch loop until idle-exit
//!      (idle.rs) fires.

pub mod activation;
pub mod config;
pub mod control;
pub mod convert;
pub mod daemonize;
pub mod dbus;
pub mod errors;
pub mod idle;
pub mod registration;
pub mod telemetry;
pub mod varlink;

use std::path::PathBuf;
use std::sync::Arc;

use tracing::{error, info, warn};

use config::schema::Bus;
use config::DispatchTable;
use control::StartupOutcome;
use dbus::BridgeState;
use telemetry::Telemetry;

/// Shared catch-all error type used across modules for anything that isn't
/// part of the Varlink<->D-Bus wire-protocol mapping proper (that lives in
/// `errors.rs`). This is deliberately not a rich enum: most failures here
/// (I/O, TOML parsing, zbus errors) are handled by logging and moving on
/// (e.g. skip one bad conf.d file) rather than by matching on error kind.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn conf_d_dir() -> PathBuf {
    std::env::var_os("BUSBRIDGE_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/etc/busbridge/conf.d"))
}

fn telemetry_state_path() -> PathBuf {
    std::env::var_os("BUSBRIDGE_STATE_DIR")
        .map(|d| PathBuf::from(d).join("telemetry.jsonl"))
        .or_else(|| {
            std::env::var_os("XDG_STATE_HOME")
                .map(|d| PathBuf::from(d).join("busbridge/telemetry.jsonl"))
        })
        .unwrap_or_else(|| PathBuf::from("/var/lib/busbridge/telemetry.jsonl"))
}

/// The D-Bus name this invocation believes it was activated for, if any -
/// used as the handoff hint (control.rs). Bus-activation conventions for
/// passing this vary by supervisor; we look in two places, both
/// documented, optional, and never required for correctness (worst case:
/// the resident just doesn't get an early RequestName nudge for this
/// specific name, and picks it up on its own from config regardless):
///   - `BUSBRIDGE_ACTIVATED_NAME` env var (settable by whatever
///     `Exec=` line triggered this invocation).
///   - the first CLI argument, for supervisors that pass the activated
///     name positionally.
fn activated_name_hint() -> Option<String> {
    std::env::var("BUSBRIDGE_ACTIVATED_NAME")
        .ok()
        .or_else(|| std::env::args().nth(1).filter(|a| a != "stats"))
}

/// The whole CLI entrypoint, callable both from `main.rs` and from
/// integration tests. Returns the process exit code.
pub fn cli_main() -> i32 {
    let args: Vec<String> = std::env::args().collect();

    // Daemonization, if requested, must happen before anything else in
    // this process - including tracing_subscriber::fmt::init() below,
    // out of an abundance of caution, and definitely before the tokio
    // runtime a few lines down - since forking is only sound while the
    // process is still single-threaded. See daemonize.rs's module doc
    // comment for the full reasoning and docs/DAEMON.md for when to
    // actually use this (the no-supervisor-at-all case) versus not
    // (under systemd/s6/runit/OpenRC, where the supervisor should hold
    // onto this process directly instead).
    if args.iter().any(|a| a == "--daemonize" || a == "-d") {
        let pid_file = args
            .iter()
            .position(|a| a == "--pid-file")
            .and_then(|i| args.get(i + 1))
            .map(std::path::PathBuf::from);
        if let Err(e) = daemonize::daemonize(pid_file.as_deref()) {
            eprintln!("failed to daemonize: {e}");
            return 1;
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    if args.get(1).map(String::as_str) == Some("stats") {
        run_stats_subcommand();
        return 0;
    }

    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start async runtime: {e}");
            return 1;
        }
    };

    rt.block_on(async_main())
}

fn run_stats_subcommand() {
    let telemetry = Telemetry::load(telemetry_state_path());
    println!("{}", telemetry::format_stats_table(&telemetry.snapshot()));
}

async fn async_main() -> i32 {
    let control_paths = control::default_paths();
    let hint = activated_name_hint();

    let resident_lock = match control::resolve_startup(&control_paths, hint.as_deref()).await {
        Ok(StartupOutcome::HandedOff) => {
            info!("handed off to existing resident instance, exiting");
            return 0;
        }
        Ok(StartupOutcome::Resident(lock)) => lock,
        Err(e) => {
            error!(error = %e, "failed to resolve resident-vs-handoff startup");
            return 1;
        }
    };

    let table = match config::load_conf_d(&conf_d_dir()) {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "failed to load conf.d directory");
            return 1;
        }
    };

    let telemetry = Arc::new(Telemetry::load(telemetry_state_path()));
    let idle_timeout = table.max_idle_timeout();
    let (session_table, system_table) = table.partition_by_bus();

    // `BusRegistry` replaces what used to be a fixed `Vec<Arc<BridgeState>>`
    // computed once here at startup. It exists because of one thing
    // conf.d-only operation never needed: an app registering a service
    // (src/registration.rs) can name *either* bus at *any* time, including
    // a bus conf.d had zero services for (and therefore no BridgeState/
    // connection for) at startup. The registry starts seeded with
    // whatever conf.d gave it below, and both hot-reload and registration
    // can grow it later by calling `get_or_start`, which reuses an
    // existing connection for a bus or lazily opens one. See
    // docs/REGISTRATION_PROTOCOL.md's "which bus" section.
    let registry = Arc::new(registration::BusRegistry::new(telemetry.clone()));

    for (bus, tbl) in [(Bus::Session, session_table), (Bus::System, system_table)] {
        if tbl.services.is_empty() {
            continue;
        }
        match start_bus(bus, tbl, telemetry.clone()).await {
            Ok(state) => registry.seed(bus, state).await,
            Err(e) => {
                error!(?bus, error = %e, "failed to start bus connection, this bus's services will be unavailable");
            }
        }
    }

    if registry.snapshot().await.is_empty() {
        warn!("no services configured for either bus; running idle with only the control channel active");
    }

    // The resolver socket is process-wide (one well-known address
    // regardless of how many buses/services are configured, or even if
    // none are - a client can still resolve/reach native Varlink services
    // through it). Registry-aware (not a fixed snapshot) so a service
    // registered - or resolvable-declared via conf.d hot-reload - on a
    // bus that had no connection at all yet when this process started
    // still becomes resolvable once that bus exists, not just the buses
    // that existed at this exact line.
    tokio::spawn(dbus::resolver::start_registry_aware_listener(registry.clone()));

    // The control channel is process-wide (not per-bus): it both routes
    // handoff RequestName calls to whichever running bus actually owns
    // the name, and accepts app registrations for either bus (lazily
    // starting that bus's connection on first use) - see control.rs and
    // registration.rs.
    tokio::spawn(control::run_control_accept_loop(Some(registry.clone()), resident_lock));

    // Hot-reload watcher, also process-wide: reconcile against whichever
    // bus(es) actually changed, starting a bus connection on demand if
    // conf.d adds services to a bus that had none (or that only came
    // into existence via a registration) at startup.
    let registry_for_reload = registry.clone();
    let conf_dir = conf_d_dir();
    tokio::spawn(async move {
        run_hot_reload(conf_dir, registry_for_reload).await;
    });

    // Idle-exit: fires once *every* currently-known bus's activity
    // tracker (and dynamic-object/streaming/registration holds) has been
    // idle past the configured timeout, *and* no new bus appeared while
    // waiting - see `wait_for_registry_idle`'s doc comment for why a
    // one-shot check isn't enough once a registry can grow at runtime.
    // Periodically persist telemetry so counts survive a crash, not just a
    // clean idle-exit (docs/DESIGN_BRIEF_V1.md Section 3.8).
    let telemetry_for_flush = telemetry.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Err(e) = telemetry_for_flush.flush() {
                warn!(error = %e, "failed to flush telemetry state");
            }
        }
    });

    // Shut down on whichever comes first: every bus going idle past its
    // timeout (wait_for_registry_idle), or an external stop request -
    // SIGTERM (the standard, init-system-agnostic way both a supervisor
    // and a plain `kill` ask a process to stop) or SIGINT (Ctrl-C, for
    // running this directly in a terminal, e.g. while testing a conf.d
    // change by hand). Either way, the exact same cleanup runs below:
    // release every name this process owns and flush telemetry, rather
    // than the default "die immediately, run no destructors at all"
    // behavior an unhandled SIGTERM would otherwise get - a background
    // daemon stopped the normal way (a supervisor restarting it, a
    // system shutdown, an operator's explicit `kill`) should leave
    // whatever bus(es) it was serving in a clean, immediately-reusable
    // state, not force every name it held to sit in D-Bus's
    // ownership-queue limbo until something notices the connection died.
    let shutdown_reason = tokio::select! {
        _ = wait_for_registry_idle(&registry, idle_timeout) => "idle timeout reached on all buses",
        _ = wait_for_stop_signal() => "received stop signal",
    };
    info!(reason = shutdown_reason, "shutting down");
    for state in registry.snapshot().await {
        state.release_all_names().await;
    }

    if let Err(e) = telemetry.flush() {
        warn!(error = %e, "failed to flush telemetry state on shutdown");
    }

    0
}

/// Resolves as soon as this process receives SIGTERM or SIGINT - see
/// this function's only caller for why both are handled the same way.
/// Falls back to waiting on SIGINT alone (`tokio::signal::ctrl_c`,
/// infallible) if installing the SIGTERM handler itself fails, which in
/// practice should never happen - `signal()` only fails on a handful of
/// genuinely exceptional conditions (e.g. hitting the OS's signal-handler
/// resource limit), and a process that can't even install one more
/// signal handler has bigger problems than this fallback path, which
/// exists so a failure here degrades gracefully instead of leaving the
/// process with no way to shut down cleanly at all.
async fn wait_for_stop_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to install SIGTERM handler, falling back to SIGINT only");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Connects to `bus` and starts serving `table` on it: `RequestName`s every
/// configured name, then spawns the dispatch loop, varlink listener(s),
/// and outbound subscriptions.
pub async fn start_bus(bus: Bus, table: DispatchTable, telemetry: Arc<Telemetry>) -> Result<Arc<BridgeState>, BoxError> {
    let connection = dbus::connect(bus).await?;
    start_bus_on_connection(connection, table, telemetry).await
}

/// Same as `start_bus`, but against an already-established `Connection`
/// rather than one this function opens itself via `dbus::connect`'s
/// env-var-based `Connection::session()`/`Connection::system()`.
///
/// This exists so integration tests can point the bridge at a specific,
/// private, test-only bus address (`zbus::conn::Builder::address(...)`)
/// without mutating the process-wide `DBUS_SESSION_BUS_ADDRESS` env var -
/// which is unsafe to do from more than one test at a time, since
/// `cargo test` runs `#[test]` functions within a binary concurrently by
/// default, and every test would otherwise be racing to overwrite the
/// same global value out from under each other's `dbus::connect` calls.
pub async fn start_bus_on_connection(
    connection: dbus::Connection,
    table: DispatchTable,
    telemetry: Arc<Telemetry>,
) -> Result<Arc<BridgeState>, BoxError> {
    let state = BridgeState::new(connection, table, telemetry);
    state.request_all_names().await?;

    tokio::spawn(dbus::run_dispatch_loop(state.clone()));
    varlink::server::start_listeners(state.clone()).await;
    dbus::bus_proxy::start_listeners(state.clone()).await;
    tokio::spawn(varlink::streaming::start_subscriptions(state.clone()));

    Ok(state)
}

async fn run_hot_reload(conf_dir: PathBuf, registry: Arc<registration::BusRegistry>) {
    use futures_util::StreamExt;

    let mut watcher = config::build_watcher(&conf_dir);
    while watcher.next().await.is_some() {
        info!("conf.d changed, reloading");
        let new_table = match config::load_conf_d(&conf_dir) {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "hot-reload: failed to reload conf.d, keeping previous config");
                continue;
            }
        };
        let (session_table, system_table) = new_table.partition_by_bus();
        for (bus, tbl) in [(Bus::Session, session_table), (Bus::System, system_table)] {
            match registry.get(bus).await {
                Some(state) => {
                    // Preserve any service registered via the app
                    // registration protocol (registration.rs) on this
                    // bus - conf.d has no idea these exist, so applying
                    // `tbl` (built purely from disk) as-is would silently
                    // un-register every live app on this bus on any
                    // unrelated conf.d edit.
                    let snapshot = registration::snapshot_registered(&state).await;
                    let mut tbl = tbl;
                    registration::merge_registered_into(&mut tbl, &snapshot);
                    if let Err(e) = state.apply_reload(tbl).await {
                        warn!(?bus, error = %e, "hot-reload: failed to apply new dispatch table");
                    }
                    registration::restore_registered_introspection(&state, &snapshot).await;
                }
                // This bus had zero services (so no BridgeState/connection
                // at all) both at startup and at every reload since - skip
                // it unless conf.d just gave it its first service, in
                // which case start it now rather than dropping the new
                // config on the floor. (A bus that already has a
                // BridgeState purely because an app registered a service
                // on it - registration.rs - takes the `Some` branch above
                // exactly like one conf.d started; there's nothing
                // registration-specific to special-case here.)
                None if !tbl.services.is_empty() => match registry.get_or_start(bus).await {
                    Ok(state) => {
                        if let Err(e) = state.apply_reload(tbl).await {
                            warn!(?bus, error = %e, "hot-reload: failed to apply new dispatch table to newly-started bus");
                        }
                    }
                    Err(e) => warn!(?bus, error = %e, "hot-reload: failed to start newly-needed bus connection"),
                },
                None => {}
            }
        }
    }
}

/// Waits until every currently-known bus has been idle past `timeout`,
/// *and* stays true after re-checking - not just a one-shot `join_all`
/// the way this looked before the registration protocol existed. That
/// used to be safe because `states` was a fixed list decided once at
/// startup; now `registry` can grow at any time (an app can register a
/// service on a bus that had no connection at all yet), so a bus that
/// appears *while* this function is waiting on the others must not be
/// missed - otherwise the process could idle-exit out from under a
/// connection that just became active. An empty registry (nothing
/// configured or registered at all, including at startup) polls slowly
/// forever instead of returning immediately, matching the pre-
/// registration behavior of just hanging until something external kills
/// the process, while still noticing if something registers later.
async fn wait_for_registry_idle(registry: &registration::BusRegistry, timeout: std::time::Duration) {
    loop {
        let snapshot = registry.snapshot().await;
        if snapshot.is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        let waiters = snapshot
            .iter()
            .map(|s| idle::wait_for_idle_exit(s.activity.clone(), timeout));
        futures_util::future::join_all(waiters).await;

        let after = registry.snapshot().await;
        if after.len() == snapshot.len() {
            return;
        }
        // Else: at least one new bus was seeded/started while we were
        // waiting on the others - loop again and wait on the larger set
        // too before concluding the whole process is really idle.
    }
}
