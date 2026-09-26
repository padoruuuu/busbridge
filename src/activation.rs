//! Defensive LISTEN_FDS / LISTEN_PID parsing. docs/DESIGN_BRIEF_V1.md Section 4.
//!
//! HARD CONSTRAINT: this is the ONLY nod to any external/systemd-adjacent
//! convention anywhere in this project, and it must remain read-only and
//! optional - if the env vars are absent or invalid, fall back cleanly to
//! binding the configured socket ourselves (varlink/server.rs). Do not
//! pull in libsystemd or any systemd-specific crate; hand-parse the two
//! env vars (LISTEN_PID must match our own pid, LISTEN_FDS gives the
//! count, fds start at 3).

use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::sync::{Mutex, OnceLock};

/// The fd-inheritance convention's first inherited fd number, per the
/// shared (non-systemd-specific) ABI multiple supervisors implement.
const LISTEN_FDS_START: RawFd = 3;

/// Every inherited fd this process was handed, computed once (from
/// `LISTEN_PID`/`LISTEN_FDS`) the first time anything asks for one, and
/// shared process-wide from then on. Backing store for
/// `claim_inherited_fd` - see that function's doc comment for why a
/// shared, drainable pool exists instead of every caller reading the env
/// vars independently.
static POOL: OnceLock<Mutex<VecDeque<RawFd>>> = OnceLock::new();

fn pool() -> &'static Mutex<VecDeque<RawFd>> {
    POOL.get_or_init(|| {
        let fds = inherited_fds_from(
            std::env::var("LISTEN_PID").ok(),
            std::env::var("LISTEN_FDS").ok(),
            std::process::id(),
        );
        Mutex::new(fds.into_iter().collect())
    })
}

/// Claims and returns the next inherited fd this process was handed, if
/// any are left - `None` once they're all claimed (or if there were
/// never any: absent/malformed env vars, a mismatched `LISTEN_PID`, all
/// "no inherited fds," never an error, per this module's original
/// "optional accelerant, never a requirement" contract).
///
/// More than one thing in this process can end up wanting an inherited
/// fd: `control.rs`'s single control socket, and one per conf.d service
/// with `varlink.listen` configured (`varlink/server.rs`). All of them
/// draw from this one shared, process-wide pool (computed once, on
/// whichever call happens first) rather than each independently
/// re-reading `LISTEN_FDS` - independent reads would each see the *same*
/// full set and could hand the same fd to two different listeners.
/// Which logical socket ends up claiming which positional fd (control
/// socket first, or a particular service first) is a *deployment*
/// convention - whatever order a systemd `.socket` unit's
/// `ListenStream=` lines are declared in - documented in
/// `service-templates/systemd/README.md`, not something this function
/// tracks or enforces itself.
pub fn claim_inherited_fd() -> Option<RawFd> {
    drain_next(pool())
}

/// Split out from `claim_inherited_fd` so the sharing/draining behavior
/// (`draining_a_shared_pool_hands_out_fds_in_order_then_none`, below) can
/// be tested against a pool built directly from known fd numbers, rather
/// than through the process-global `OnceLock` - which, being seeded once
/// from real `LISTEN_PID`/`LISTEN_FDS` env vars, isn't something
/// multiple tests in one process could safely each set up differently
/// (the same reason `inherited_fds_from`, not `claim_inherited_fd`
/// itself, is what the rest of this module's tests exercise directly).
fn drain_next(pool: &Mutex<VecDeque<RawFd>>) -> Option<RawFd> {
    pool.lock().unwrap().pop_front()
}

/// Testable core: takes the two env var values (already read) and our own
/// pid, rather than reading the environment directly, so tests can drive
/// arbitrary combinations without mutating real process env vars.
fn inherited_fds_from(
    listen_pid: Option<String>,
    listen_fds: Option<String>,
    our_pid: u32,
) -> Vec<RawFd> {
    let (Some(pid_str), Some(fds_str)) = (listen_pid, listen_fds) else {
        return Vec::new();
    };

    let Ok(pid) = pid_str.trim().parse::<u32>() else {
        return Vec::new();
    };
    if pid != our_pid {
        // Not meant for us (e.g. inherited across an unrelated exec chain).
        return Vec::new();
    }

    let Ok(count) = fds_str.trim().parse::<u32>() else {
        return Vec::new();
    };

    (0..count)
        .map(|i| LISTEN_FDS_START + i as RawFd)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_vars_yield_no_fds() {
        assert!(inherited_fds_from(None, None, 1234).is_empty());
        assert!(inherited_fds_from(Some("1234".into()), None, 1234).is_empty());
        assert!(inherited_fds_from(None, Some("2".into()), 1234).is_empty());
    }

    #[test]
    fn mismatched_pid_yields_no_fds() {
        assert!(inherited_fds_from(Some("999".into()), Some("2".into()), 1234).is_empty());
    }

    #[test]
    fn malformed_vars_yield_no_fds_not_panic() {
        assert!(inherited_fds_from(Some("not-a-pid".into()), Some("2".into()), 1234).is_empty());
        assert!(inherited_fds_from(Some("1234".into()), Some("not-a-count".into()), 1234).is_empty());
    }

    #[test]
    fn matching_pid_yields_fds_starting_at_3() {
        let fds = inherited_fds_from(Some("1234".into()), Some("2".into()), 1234);
        assert_eq!(fds, vec![3, 4]);
    }

    #[test]
    fn zero_count_yields_empty_vec_not_error() {
        let fds = inherited_fds_from(Some("1234".into()), Some("0".into()), 1234);
        assert!(fds.is_empty());
    }

    #[test]
    fn draining_a_shared_pool_hands_out_fds_in_order_then_none() {
        let pool = Mutex::new(VecDeque::from(vec![3, 4, 5]));
        // Simulates two different listeners (e.g. control.rs's control
        // socket, then varlink/server.rs's per-service listeners) each
        // claiming from the same pool in turn - the scenario
        // `claim_inherited_fd`'s doc comment describes, without needing
        // real env vars or the process-global OnceLock.
        assert_eq!(drain_next(&pool), Some(3));
        assert_eq!(drain_next(&pool), Some(4));
        assert_eq!(drain_next(&pool), Some(5));
        assert_eq!(drain_next(&pool), None);
        assert_eq!(drain_next(&pool), None, "draining an empty pool repeatedly stays None, never panics");
    }
}
