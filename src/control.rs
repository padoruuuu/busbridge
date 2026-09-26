//! The control-channel protocol + resident/handoff logic. docs/DESIGN_BRIEF_V1.md
//! Section 2.1. This is the trickiest correctness piece in the whole
//! project - two near-simultaneous D-Bus activations both finding "no one
//! resident yet" and both trying to become resident is a real scenario,
//! and must be handled without a race window where a name request gets
//! lost.
//!
//! Race-safety mechanism: an exclusive, non-blocking `flock()` on a
//! lockfile (not the socket bind itself, which has its own races around
//! stale files) decides who becomes resident. Losing the flock race means
//! someone else is either already resident or is *becoming* resident right
//! now; either way, the correct move is to retry connecting to the control
//! socket (with a short bounded backoff, since there's a brief window
//! between "won the lock" and "finished binding the socket"), not to
//! immediately fall back to becoming resident.
//!
//! Crash resilience is a side effect of using flock(): the kernel releases
//! the lock automatically when the holding process exits for *any*
//! reason, including a crash - no stale-lock cleanup logic needed.

use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tracing::{info, warn};

use crate::dbus::BridgeState;
use crate::varlink::{read_framed, write_framed};
use crate::BoxError;

#[derive(Debug, Clone)]
pub struct ControlPaths {
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
}

/// Resolve the control socket/lockfile paths: `$XDG_RUNTIME_DIR/
/// busbridge/` when set (the common session-scope case), falling
/// back to `/run/busbridge/` for system-scope use (e.g. running as
/// a system-wide daemon with no XDG runtime dir). An explicit
/// `BUSBRIDGE_RUNTIME_DIR` override takes priority over both, for
/// tests and unusual deployments.
pub fn default_paths() -> ControlPaths {
    let dir = std::env::var_os("BUSBRIDGE_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_RUNTIME_DIR").map(|d| PathBuf::from(d).join("busbridge")))
        .unwrap_or_else(|| PathBuf::from("/run/busbridge"));
    paths_in(&dir)
}

pub fn paths_in(dir: &Path) -> ControlPaths {
    ControlPaths {
        socket_path: dir.join("busbridge.sock"),
        lock_path: dir.join("control.lock"),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HandoffRequest {
    /// The D-Bus well-known name this invocation was activated for, if
    /// known (e.g. from `DBUS_STARTER_BUS_TYPE`/activation args - see
    /// main.rs). `None` is valid too: "someone activated us, resident
    /// process, please just make sure you're still alive and serving
    /// whatever you're configured for."
    bus_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HandoffAck {
    ok: bool,
}

pub enum StartupOutcome {
    /// We won the race: bind the control socket, become the resident
    /// process, and (by main.rs) go on to own the bus connection.
    Resident(ResidentLock),
    /// Another process is already resident and acknowledged our handoff
    /// request; this invocation's job is done, exit immediately.
    HandedOff,
}

/// Held for the resident process's entire lifetime. Dropping it releases
/// the flock (also happens automatically on crash) and best-effort removes
/// the socket file so the next invocation starts clean.
pub struct ResidentLock {
    _lock_file: std::fs::File,
    pub listener: UnixListener,
    _cleanup: SocketCleanup,
}

/// Split out from `ResidentLock` itself (rather than a `Drop` impl on
/// `ResidentLock` directly) so callers can still move `listener` out of a
/// `ResidentLock` - a struct with its own `Drop` impl can't have fields
/// partially moved out, but nothing stops one of its *fields* from having
/// one.
struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

enum LockError {
    AlreadyHeld,
    Io(std::io::Error),
}

fn try_acquire_lock(path: &Path) -> Result<std::fs::File, LockError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(LockError::Io)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .map_err(LockError::Io)?;
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret == 0 {
        Ok(file)
    } else {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            Err(LockError::AlreadyHeld)
        } else {
            Err(LockError::Io(err))
        }
    }
}

/// Decide whether this invocation becomes resident or hands off,
/// race-safely. `bus_name_hint` is whatever D-Bus name (if any) this
/// invocation was told it was activated for (see main.rs); it's forwarded
/// to the resident so it can `RequestName` it.
pub async fn resolve_startup(
    paths: &ControlPaths,
    bus_name_hint: Option<&str>,
) -> Result<StartupOutcome, BoxError> {
    // Bounded retries: this is a local race between processes starting at
    // nearly the same instant, not something that should ever need more
    // than a handful of short retries in practice. A generous bound just
    // guards against looping forever if something is deeply wrong (e.g.
    // permissions preventing both locking and connecting).
    const MAX_ATTEMPTS: u32 = 200;
    const RETRY_DELAY: Duration = Duration::from_millis(25);

    for attempt in 0..MAX_ATTEMPTS {
        match try_acquire_lock(&paths.lock_path) {
            Ok(lock_file) => {
                // Try an inherited fd first (e.g. a systemd `.socket`
                // unit socket-activating this control socket
                // specifically - service-templates/systemd/README.md),
                // falling back to binding it ourselves - the same
                // "optional accelerant, never a requirement" pattern
                // varlink/server.rs's own listeners already use, sharing
                // its fd pool (crate::activation::claim_inherited_fd)
                // rather than reading LISTEN_FDS independently. Crucially,
                // an inherited fd is *already bound* at paths.socket_path
                // by whatever handed it to us - removing that path first,
                // the way the "bind fresh" branch needs to, would orphan
                // it (the fd keeps working, but nothing could connect to
                // it by path anymore), so that cleanup only happens in
                // the fallback branch.
                let listener = match crate::activation::claim_inherited_fd() {
                    Some(fd) => match crate::varlink::server::listener_from_fd(fd) {
                        Ok(l) => {
                            info!(fd, "became resident: control socket using inherited fd");
                            l
                        }
                        Err(e) => {
                            warn!(fd, error = %e, "inherited fd invalid for control socket, binding fresh instead");
                            let _ = std::fs::remove_file(&paths.socket_path);
                            UnixListener::bind(&paths.socket_path)?
                        }
                    },
                    None => {
                        let _ = std::fs::remove_file(&paths.socket_path);
                        UnixListener::bind(&paths.socket_path)?
                    }
                };
                info!(socket = %paths.socket_path.display(), "became resident: bound control socket");
                return Ok(StartupOutcome::Resident(ResidentLock {
                    _lock_file: lock_file,
                    listener,
                    _cleanup: SocketCleanup(paths.socket_path.clone()),
                }));
            }
            Err(LockError::Io(e)) => return Err(e.into()),
            Err(LockError::AlreadyHeld) => {
                match try_handoff(&paths.socket_path, bus_name_hint).await {
                    Ok(()) => {
                        info!("handed off to existing resident instance");
                        return Ok(StartupOutcome::HandedOff);
                    }
                    Err(e) => {
                        // The lock holder may not have finished binding
                        // the socket yet (we lost the flock race a moment
                        // before it called bind()) - this is the expected
                        // transient case the retry loop exists for.
                        if attempt + 1 == MAX_ATTEMPTS {
                            return Err(format!(
                                "gave up trying to become resident or hand off after {MAX_ATTEMPTS} attempts: {e}"
                            )
                            .into());
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        }
    }
    unreachable!("loop returns or errors on its last iteration");
}

async fn try_handoff(socket_path: &Path, bus_name_hint: Option<&str>) -> Result<(), BoxError> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let req = HandoffRequest {
        bus_name: bus_name_hint.map(|s| s.to_string()),
    };
    write_framed(&mut stream, &req).await?;
    let mut reader = tokio::io::BufReader::new(&mut stream);
    let ack: Option<HandoffAck> = read_framed(&mut reader).await?;
    match ack {
        Some(a) if a.ok => Ok(()),
        Some(_) => Err("resident declined handoff".into()),
        None => Err("resident closed the connection without acknowledging".into()),
    }
}

/// Runs for the resident process's whole lifetime: accepts connections on
/// the one universal `busbridge.sock` address and routes each to
/// whichever of two protocols it turns out to be (see
/// `docs/REGISTRATION_PROTOCOL.md`):
///   - a legacy handoff request from a freshly-activated invocation of
///     busbridge itself (unchanged since before the registration
///     protocol existed - `HandoffRequest`/`HandoffAck` below), or
///   - an app registering itself as a D-Bus service
///     (`registration::handle_registration_connection`).
///
/// The two are told apart by peeking the first frame: a registration
/// frame always has a `method` field (it's Varlink-shaped -
/// `{"method": "org.busbridge.Registration.Register", ...}`); a handoff
/// frame never does (it's just `{"bus_name": ...}` or `{}`) - the same
/// "does this JSON object have a method key" convention already used to
/// tell a push from a reply in `dbus/dynamic_object.rs`.
///
/// `registry` is `None` only in tests that don't need registration at
/// all (`Option` rather than requiring a `BusRegistry` keeps this
/// function's signature - and every existing call site of it - unchanged
/// from before registration existed). When present, it both resolves a
/// best-effort single `BridgeState` for handoff routing (exactly what a
/// bare `Option<Arc<BridgeState>>` used to be passed in as directly) and
/// lazily starts a bus connection on demand for registration.
pub async fn run_control_accept_loop(registry: Option<std::sync::Arc<crate::registration::BusRegistry>>, lock: ResidentLock) {
    loop {
        match lock.listener.accept().await {
            Ok((stream, _addr)) => {
                let registry = registry.clone();
                tokio::spawn(async move {
                    handle_connection(registry, stream).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "control socket accept() failed, stopping control loop");
                break;
            }
        }
    }
}

async fn handle_connection(registry: Option<std::sync::Arc<crate::registration::BusRegistry>>, mut stream: UnixStream) {
    let handoff_state = match &registry {
        Some(r) => r.best_effort_single().await,
        None => None,
    };
    if let Some(state) = &handoff_state {
        state.activity.touch();
    }

    let raw: Option<serde_json::Value> = {
        let mut reader = tokio::io::BufReader::new(&mut stream);
        match read_framed(&mut reader).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed reading first frame on control socket connection");
                return;
            }
        }
    };
    let Some(raw) = raw else { return };

    if raw.get("method").is_some() {
        let Some(registry) = registry else {
            warn!("registration attempted but this resident has no bus registry (should not happen - report a bug)");
            return;
        };
        crate::registration::handle_registration_connection(registry, stream, raw).await;
        return;
    }

    let req: HandoffRequest = match serde_json::from_value(raw) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "malformed handoff request on control socket");
            return;
        }
    };
    handle_handoff_connection(handoff_state, stream, req).await;
}

async fn handle_handoff_connection(state: Option<std::sync::Arc<BridgeState>>, mut stream: UnixStream, req: HandoffRequest) {
    if let Some(bus_name) = &req.bus_name {
        match &state {
            Some(state) => {
                info!(bus_name, "handoff: requesting name on behalf of newly-activated invocation");
                if let Err(e) = state.connection.request_name(bus_name.as_str()).await {
                    warn!(bus_name, error = %e, "handoff: failed to request name");
                    let _ = write_framed(&mut stream, &HandoffAck { ok: false }).await;
                    return;
                }
            }
            None => {
                // No bus connection at all yet (this resident started with
                // an empty conf.d) - still ack so the activating invocation
                // exits cleanly rather than timing out; a subsequent
                // hot-reload picking up new config is this process's only
                // path to actually serving `bus_name`, which is a pre-
                // existing limitation of starting fully unconfigured, not
                // something this handoff can fix on its own.
                warn!(bus_name, "handoff: acking with no bus connection available (this resident has no configured services)");
            }
        }
    }

    let _ = write_framed(&mut stream, &HandoffAck { ok: true }).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stub "resident" accept loop that doesn't need a real BridgeState -
    /// just enough to prove the race-safety property (exactly one winner)
    /// and that every loser's handoff gets acknowledged.
    async fn stub_resident_loop(listener: UnixListener, received: std::sync::Arc<tokio::sync::Mutex<Vec<Option<String>>>>) {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let received = received.clone();
            tokio::spawn(async move {
                let req: Option<HandoffRequest> = {
                    let mut reader = tokio::io::BufReader::new(&mut stream);
                    read_framed(&mut reader).await.unwrap_or(None)
                };
                if let Some(req) = req {
                    received.lock().await.push(req.bus_name.clone());
                    let _ = write_framed(&mut stream, &HandoffAck { ok: true }).await;
                }
            });
        }
    }

    #[tokio::test]
    async fn near_simultaneous_startup_has_exactly_one_resident() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let received = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Simulate N near-simultaneous "activations" all racing to start.
        // Whichever one wins immediately starts accepting handoffs (a real
        // resident would go on to own a bus connection and do the same via
        // run_control_accept_loop) - otherwise every loser would block
        // forever waiting for an ack nobody sends.
        const N: usize = 12;
        let mut handles = Vec::new();
        for i in 0..N {
            let paths = paths.clone();
            let received = received.clone();
            handles.push(tokio::spawn(async move {
                let outcome = resolve_startup(&paths, Some(&format!("org.example.Name{i}"))).await?;
                let became_resident = matches!(outcome, StartupOutcome::Resident(_));
                if let StartupOutcome::Resident(lock) = outcome {
                    tokio::spawn(async move {
                        // Force whole-value capture of `lock` (not just
                        // `lock.listener`) into this async block. Rust
                        // 2021's disjoint capture would otherwise only
                        // capture the specific field path referenced below
                        // (`lock.listener`), leaving `_lock_file` (the
                        // flock) behind in the *outer* scope - where it
                        // gets dropped, releasing the lock, the instant
                        // this `if let` block ends. Rebinding the whole
                        // variable first is the standard workaround.
                        let lock = lock;
                        stub_resident_loop(lock.listener, received).await;
                    });
                }
                Ok::<bool, BoxError>(became_resident)
            }));
        }

        let mut resident_count = 0;
        let mut handed_off_count = 0;
        for h in handles {
            if h.await.unwrap().unwrap() {
                resident_count += 1;
            } else {
                handed_off_count += 1;
            }
        }

        // Every loser must have successfully handed off (not errored) -
        // resolve_startup() only returns Ok(HandedOff) once its handoff
        // was actually acknowledged, so if we got here at all with N-1
        // HandedOff results, no name request was ever silently dropped.
        assert_eq!(resident_count, 1, "exactly one invocation must become resident");
        assert_eq!(handed_off_count, N - 1);
    }

    #[tokio::test]
    async fn loser_handoff_is_acknowledged_by_the_winner() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());

        let first = resolve_startup(&paths, Some("org.example.First")).await.unwrap();
        let StartupOutcome::Resident(lock) = first else {
            panic!("first invocation should become resident");
        };

        let received = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let listener = lock.listener;
        let received_clone = received.clone();
        let accept_task = tokio::spawn(async move {
            stub_resident_loop(listener, received_clone).await;
        });

        let second = resolve_startup(&paths, Some("org.example.Second")).await.unwrap();
        assert!(matches!(second, StartupOutcome::HandedOff));

        // Give the accept task a moment to record the request.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(received.lock().await.as_slice(), &[Some("org.example.Second".to_string())]);

        accept_task.abort();
    }

    #[tokio::test]
    async fn resident_lock_drop_removes_socket_file() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        let outcome = resolve_startup(&paths, None).await.unwrap();
        let StartupOutcome::Resident(lock) = outcome else {
            panic!("expected Resident");
        };
        assert!(paths.socket_path.exists());
        drop(lock);
        assert!(!paths.socket_path.exists());
    }

    /// Regression test for a real startup crash: a resident process with
    /// no configured services on either bus (e.g. an empty conf.d) has no
    /// `BridgeState` at all, and `run_control_accept_loop` must not assume
    /// one exists - it should still accept and ack handoff connections
    /// instead of panicking or hanging them forever.
    #[tokio::test]
    async fn control_loop_with_no_shim_state_still_acks_handoffs() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());

        let outcome = resolve_startup(&paths, None).await.unwrap();
        let StartupOutcome::Resident(lock) = outcome else {
            panic!("expected Resident");
        };

        // No BridgeState - this is exactly the `None` case that used to
        // reach an `.expect(...)` and panic in `control_router`.
        let accept_task = tokio::spawn(run_control_accept_loop(None, lock));

        let outcome2 = resolve_startup(&paths, Some("org.example.NoConfigYet"))
            .await
            .expect("handoff must succeed (be acked) even with no BridgeState behind it");
        assert!(matches!(outcome2, StartupOutcome::HandedOff));

        accept_task.abort();
    }
}
