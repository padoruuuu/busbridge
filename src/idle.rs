//! Activity tracking + idle-exit timer. docs/DESIGN_BRIEF_V1.md Section 3.7.
//!
//! Must NOT fire while any streaming Varlink subscription (varlink/streaming.rs)
//! is open, or while any call is in-flight - regardless of elapsed idle time.
//! On exit: release all D-Bus names, close the control socket, close the
//! varlink listen socket cleanly so the next activation starts fresh.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Tracks last-activity time and outstanding "keep alive" holds. Cheap to
/// clone (it's an `Arc` wrapper) so every long-lived task (dispatch loop,
/// varlink accept loop, streaming subscriptions, dynamic-object proxies)
/// can hold its own handle.
#[derive(Clone)]
pub struct ActivityTracker {
    inner: Arc<Inner>,
}

struct Inner {
    /// Unix millis of the last recorded activity.
    last_activity_ms: AtomicI64,
    /// Count of things that must keep the process alive regardless of the
    /// idle timer: in-flight calls, open streaming subscriptions, and live
    /// dynamic-object registrations (docs/DESIGN_BRIEF_V1.md Sections 2.4, 2.5, 3.7).
    active_holds: AtomicUsize,
}

/// RAII guard returned by `ActivityTracker::hold()`. Holding one of these
/// keeps the process from idle-exiting no matter how long it's held, and
/// also counts as activity both when acquired and when released (so a long
/// call that finishes doesn't look "cold" the instant it completes).
#[must_use = "dropping this immediately releases the idle-exit hold"]
pub struct ActivityGuard {
    tracker: ActivityTracker,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.tracker.inner.active_holds.fetch_sub(1, Ordering::SeqCst);
        self.tracker.touch();
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl ActivityTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                last_activity_ms: AtomicI64::new(now_ms()),
                active_holds: AtomicUsize::new(0),
            }),
        }
    }

    /// Record activity: any D-Bus call handled, any inbound Varlink push
    /// connection accepted, any outbound Varlink call completed.
    pub fn touch(&self) {
        self.inner.last_activity_ms.store(now_ms(), Ordering::SeqCst);
    }

    /// Acquire a hold that keeps the process "not idle" until dropped.
    /// Used for in-flight calls, open streaming subscriptions (2.4), and
    /// live dynamic-object registrations (2.5).
    pub fn hold(&self) -> ActivityGuard {
        self.inner.active_holds.fetch_add(1, Ordering::SeqCst);
        self.touch();
        ActivityGuard {
            tracker: self.clone(),
        }
    }

    pub fn has_active_holds(&self) -> bool {
        self.inner.active_holds.load(Ordering::SeqCst) > 0
    }

    pub fn idle_for(&self) -> Duration {
        let last = self.inner.last_activity_ms.load(Ordering::SeqCst);
        let elapsed_ms = (now_ms() - last).max(0);
        Duration::from_millis(elapsed_ms as u64)
    }
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Runs until the process has been idle (no activity, and no active holds)
/// for at least `timeout`, then returns. Callers are expected to treat
/// return as "time to shut down": release D-Bus names, close the control
/// socket, close the Varlink listen socket, then exit.
///
/// Uses a periodic re-check rather than a single sleep so that activity or
/// new holds occurring near the deadline correctly push the deadline out.
pub async fn wait_for_idle_exit(tracker: ActivityTracker, timeout: Duration) {
    // Check on a cadence finer than the timeout so we don't overshoot by
    // much, but never busier than every 250ms (keeps this cheap even for
    // very short test timeouts).
    let poll_interval = (timeout / 8).max(Duration::from_millis(50));
    loop {
        tokio::time::sleep(poll_interval).await;
        if tracker.has_active_holds() {
            continue;
        }
        if tracker.idle_for() >= timeout {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn fires_after_timeout_with_no_activity() {
        let tracker = ActivityTracker::new();
        let start = Instant::now();
        wait_for_idle_exit(tracker, Duration::from_millis(150)).await;
        assert!(start.elapsed() >= Duration::from_millis(150));
    }

    #[tokio::test]
    async fn does_not_fire_while_a_hold_is_active() {
        let tracker = ActivityTracker::new();
        let guard = tracker.hold();
        let fired = tokio::time::timeout(
            Duration::from_millis(300),
            wait_for_idle_exit(tracker.clone(), Duration::from_millis(100)),
        )
        .await;
        assert!(fired.is_err(), "idle-exit must not fire while a hold is active");
        drop(guard);
        // After releasing, it should fire promptly.
        let fired = tokio::time::timeout(
            Duration::from_millis(500),
            wait_for_idle_exit(tracker, Duration::from_millis(100)),
        )
        .await;
        assert!(fired.is_ok());
    }

    #[tokio::test]
    async fn touch_resets_the_clock() {
        let tracker = ActivityTracker::new();
        let t2 = tracker.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            t2.touch();
        });
        let start = Instant::now();
        wait_for_idle_exit(tracker, Duration::from_millis(100)).await;
        // Should take at least ~60ms (the touch) + 100ms (the timeout after it).
        assert!(start.elapsed() >= Duration::from_millis(150));
    }
}
