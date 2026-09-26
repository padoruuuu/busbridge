//! Optional, init-system-agnostic self-daemonization: detaches busbridge
//! from its controlling terminal via the classic double-fork, for the
//! case where *nothing* is already supervising this process directly (no
//! systemd unit, no s6/runit service, no OpenRC script - just a user
//! wanting to background it from a shell or a login autostart file with
//! no supervisor infrastructure at all). See `docs/DAEMON.md` for the
//! full picture, including why this is the *fallback*, not the
//! recommended default: under any real supervisor, busbridge should run
//! in the foreground and let that supervisor hold onto it directly
//! (that's how it notices a crash and restarts it, tracks its exit
//! status, and - for systemd/s6 specifically - how socket activation's
//! fd handoff already works, entirely independent of anything in this
//! file). This module exists for the one case that story doesn't cover:
//! no supervisor at all.
//!
//! # Why this has to run before the async runtime starts
//!
//! `fork()` in a process that has already spawned other threads is not
//! generally safe: only the calling thread survives into the child, every
//! other thread simply vanishes mid-execution, taking with it anything
//! that thread held a lock on, was in the middle of allocating, or (for a
//! tokio runtime specifically) was scheduled to run on. A daemonized
//! busbridge process would come up with a half-initialized, permanently
//! broken runtime. There is no safe way to daemonize after the runtime
//! exists - `lib.rs::cli_main` calls this, when requested, before
//! building the runtime at all, while the process is still guaranteed
//! single-threaded.

use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

/// Double-fork daemonization. On success, only the final, fully detached
/// grandchild process returns from this function; the original process
/// and the intermediate middle process both call `std::process::exit`
/// from inside this function and never return to the caller. Must be
/// called before anything else in the process has spawned a thread (see
/// this module's doc comment) - `lib.rs::cli_main` is the only caller,
/// and only when `--daemonize` was passed.
pub fn daemonize(pid_file: Option<&Path>) -> io::Result<()> {
    // First fork. The parent's only job was getting here; once it knows
    // the child exists, it exits immediately (rather than, say, waiting
    // for the daemon to finish some startup step) so a shell or
    // autostart script that ran `busbridge --daemonize` sees that
    // command return right away, the same way `some-daemon &`'s
    // backgrounding would look to that shell, rather than hanging until
    // the daemon itself eventually exits.
    //
    // Safety: called at the very start of `cli_main`, before any other
    // thread exists in this process (see this module's doc comment) -
    // the one precondition `fork()` actually needs to be sound here.
    match unsafe { libc::fork() } {
        -1 => return Err(io::Error::last_os_error()),
        0 => {} // child
        _ => std::process::exit(0), // original parent
    }

    // Detach from the controlling terminal and become a new session and
    // process group leader, so signals sent to the terminal's process
    // group (Ctrl-C in the shell that launched us, for one) no longer
    // reach this process.
    if unsafe { libc::setsid() } == -1 {
        return Err(io::Error::last_os_error());
    }

    // Second fork. This process (a session leader, from setsid above) is
    // the only one that could ever re-acquire a controlling terminal;
    // forking again makes the daemon itself a non-leader session member,
    // which can't. As a side effect this also detaches from the
    // intermediate (first-fork) process, which exits immediately below,
    // so init or the nearest subreaper adopts the daemon rather than the
    // now-defunct intermediate process staying responsible for it.
    //
    // Safety: same as the first fork - still single-threaded, nothing
    // has run between the two forks except plain syscalls.
    match unsafe { libc::fork() } {
        -1 => return Err(io::Error::last_os_error()),
        0 => {} // grandchild - the actual daemon
        _ => std::process::exit(0), // intermediate process
    }

    // Don't hold whatever directory launched us busy (and don't
    // interact with any of its assumptions about the current directory).
    let _ = std::env::set_current_dir("/");

    // A daemon shouldn't inherit its launcher's file-creation mask -
    // anything busbridge itself creates (the control socket, its lock
    // file) already sets its own permissions explicitly rather than
    // relying on the umask, but leaving a random inherited mask in place
    // for the process's whole lifetime is still not something a
    // long-running background process should do by accident.
    unsafe { libc::umask(0) };

    redirect_standard_fds()?;

    if let Some(path) = pid_file {
        std::fs::write(path, format!("{}\n", std::process::id()))?;
    }

    Ok(())
}

/// Points stdin at `/dev/null`. A backgrounded daemon should never block
/// waiting on terminal input, so this one is unconditional. stdout/stderr
/// are deliberately left untouched: whatever the launcher already
/// arranged for them - a shell redirect to a log file
/// (`busbridge --daemonize >>/var/log/busbridge.log 2>&1`, set up by the
/// shell *before* this process's own code, and inherited across fork()
/// unchanged), or simply the original terminal if nothing was redirected
/// at all - keeps working exactly as it would for any other backgrounded
/// process. Forcing them to `/dev/null` here unconditionally would
/// silently discard a redirect the operator explicitly asked for, which
/// is worse than the alternative (log output going nowhere once the
/// launching terminal eventually closes, if the operator didn't redirect
/// anything) - that failure mode is at least visible and fixable from the
/// operator's side; a redirect that gets silently overridden isn't.
fn redirect_standard_fds() -> io::Result<()> {
    let dev_null = std::fs::OpenOptions::new().read(true).write(true).open("/dev/null")?;
    if unsafe { libc::dup2(dev_null.as_raw_fd(), libc::STDIN_FILENO) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
