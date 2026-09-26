//! Per-(bus_name, interface, member) call counters, persisted across
//! idle-exit/restart cycles. docs/DESIGN_BRIEF_V1.md Section 3.8.
//!
//! Exposed via a `busbridge stats` CLI subcommand (see main.rs) so
//! an operator can identify mappings that have gone cold - i.e. every
//! caller has migrated to native Varlink and the compat entry can be
//! deleted. This is the tool's built-in "measure your own obsolescence"
//! feature - don't skip it even though it's polish, it's part of the
//! project's actual purpose (see docs/DESIGN_BRIEF_V1.md Section 1, Mission).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// (bus_name, interface, member)
pub type CounterKey = (String, String, String);

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CounterRecord {
    pub bus_name: String,
    pub interface: String,
    pub member: String,
    pub count: u64,
    /// Unix seconds of the most recent call, so `stats` can show recency,
    /// not just totals (useful when deciding whether "gone cold" really
    /// means migrated vs. just quiet for a day).
    pub last_seen_unix: u64,
}

/// In-memory counters plus the on-disk state file they persist to. Kept
/// deliberately simple (line-delimited JSON, one record per line) per
/// docs/DESIGN_BRIEF_V1.md Section 3.8 ("doesn't need to be fancy").
pub struct Telemetry {
    state_path: PathBuf,
    counters: Mutex<HashMap<CounterKey, CounterRecord>>,
}

impl Telemetry {
    /// Load existing counters from `state_path` if present, starting fresh
    /// otherwise (a missing or corrupt state file is not fatal - telemetry
    /// is observability, not correctness-critical state).
    pub fn load(state_path: PathBuf) -> Self {
        let mut counters = HashMap::new();
        if let Ok(text) = std::fs::read_to_string(&state_path) {
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(record) = serde_json::from_str::<CounterRecord>(line) {
                    let key = (
                        record.bus_name.clone(),
                        record.interface.clone(),
                        record.member.clone(),
                    );
                    counters.insert(key, record);
                }
            }
        }
        Self {
            state_path,
            counters: Mutex::new(counters),
        }
    }

    /// Record one D-Bus-side call actually received for `(bus_name,
    /// interface, member)`.
    pub fn record_call(&self, bus_name: &str, interface: &str, member: &str) {
        let key = (bus_name.to_string(), interface.to_string(), member.to_string());
        let mut counters = self.counters.lock().unwrap();
        let record = counters.entry(key).or_insert_with(|| CounterRecord {
            bus_name: bus_name.to_string(),
            interface: interface.to_string(),
            member: member.to_string(),
            count: 0,
            last_seen_unix: 0,
        });
        record.count += 1;
        record.last_seen_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }

    /// Persist the current counters to `state_path`, atomically (write to a
    /// temp file in the same directory, then rename) so a crash mid-write
    /// never corrupts the previous good state.
    pub fn flush(&self) -> std::io::Result<()> {
        let counters = self.counters.lock().unwrap();
        let mut out = String::new();
        let mut records: Vec<&CounterRecord> = counters.values().collect();
        records.sort_by(|a, b| (&a.bus_name, &a.interface, &a.member).cmp(&(&b.bus_name, &b.interface, &b.member)));
        for record in records {
            out.push_str(&serde_json::to_string(record).unwrap());
            out.push('\n');
        }
        if let Some(parent) = self.state_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = tmp_path_for(&self.state_path);
        std::fs::write(&tmp_path, out)?;
        std::fs::rename(&tmp_path, &self.state_path)?;
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<CounterRecord> {
        let counters = self.counters.lock().unwrap();
        let mut records: Vec<CounterRecord> = counters.values().cloned().collect();
        records.sort_by(|a, b| (&a.bus_name, &a.interface, &a.member).cmp(&(&b.bus_name, &b.interface, &b.member)));
        records
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Render the `busbridge stats` subcommand's output: a simple
/// table an operator can scan for cold mappings (candidates for deletion
/// once every caller has migrated to native Varlink).
pub fn format_stats_table(records: &[CounterRecord]) -> String {
    if records.is_empty() {
        return "No calls recorded yet.".to_string();
    }
    let mut out = String::new();
    out.push_str(&format!(
        "{:<32} {:<32} {:<24} {:>10} {:>20}\n",
        "BUS NAME", "INTERFACE", "MEMBER", "COUNT", "LAST SEEN (unix)"
    ));
    for r in records {
        out.push_str(&format!(
            "{:<32} {:<32} {:<24} {:>10} {:>20}\n",
            r.bus_name, r.interface, r.member, r.count, r.last_seen_unix
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_persists_across_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("stats.jsonl");

        {
            let telemetry = Telemetry::load(path.clone());
            telemetry.record_call("org.kde.StatusNotifierWatcher", "org.kde.StatusNotifierWatcher", "RegisterStatusNotifierItem");
            telemetry.record_call("org.kde.StatusNotifierWatcher", "org.kde.StatusNotifierWatcher", "RegisterStatusNotifierItem");
            telemetry.flush().unwrap();
        }

        let reloaded = Telemetry::load(path);
        let snap = reloaded.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].count, 2);
    }

    #[test]
    fn missing_state_file_starts_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("does-not-exist.jsonl");
        let telemetry = Telemetry::load(path);
        assert!(telemetry.snapshot().is_empty());
    }
}
