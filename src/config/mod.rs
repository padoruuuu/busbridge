//! conf.d directory scanning + hot-reload + merge into a DispatchTable.
//! docs/DESIGN_BRIEF_V1.md Section 3.1.
//!
//! Hot-reload backend must sit behind a `ConfigWatcher` trait (docs/DESIGN_BRIEF_V1.md
//! Section 9, Open Decisions) with an inotify-backed impl for Linux and a
//! polling fallback for portability - don't call inotify directly from
//! generic code.

pub mod schema;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use futures_util::Stream;
use tracing::{info, warn};

use schema::{
    Bus, MethodMapping, PropertyMapping, RegistrableConfig, ServiceFile, SignalMapping,
    VarlinkConfig,
};

use crate::BoxError;

/// One `[service]` block, fully resolved (relative paths made absolute).
#[derive(Debug)]
pub struct LoadedService {
    pub source_path: PathBuf,
    pub bus: Bus,
    pub name: String,
    pub object_path: String,
    pub introspection_xml: Option<PathBuf>,
    pub idle_timeout_secs: u64,
    pub varlink: VarlinkConfig,
    pub passthrough: bool,
    /// Set only for services created through the app registration
    /// protocol (`src/registration.rs`, `docs/REGISTRATION_PROTOCOL.md`)
    /// instead of a `conf.d` file. When `Some`, every place that would
    /// otherwise dial `varlink.backend` per call (dispatch.rs's six
    /// `VarlinkConnection::connect` call sites, routed through the small
    /// `call_backend` helper added alongside this field) instead
    /// multiplexes over this live, app-initiated connection. `None` for
    /// every conf.d-loaded service - the existing dial-per-call behavior
    /// is completely unchanged for those.
    pub peer: Option<Arc<crate::varlink::peer::PeerConnection>>,
}

#[derive(Debug, Clone)]
pub struct MethodEntry {
    pub service: Arc<LoadedService>,
    pub mapping: MethodMapping,
}

#[derive(Debug, Clone)]
pub struct SignalEntry {
    pub service: Arc<LoadedService>,
    pub mapping: SignalMapping,
}

#[derive(Debug, Clone)]
pub struct PropertyEntry {
    pub service: Arc<LoadedService>,
    pub mapping: PropertyMapping,
}

#[derive(Debug, Clone)]
pub struct RegistrableEntry {
    pub service: Arc<LoadedService>,
    pub config: RegistrableConfig,
}

/// (bus_name, interface, member)
pub type MethodKey = (String, String, String);
/// (bus_name, interface, signal_name)
pub type SignalKey = (String, String, String);
/// (bus_name, interface, property_name)
pub type PropertyKey = (String, String, String);
/// (bus_name, interface)
pub type RegistrableKey = (String, String);

/// In-memory dispatch table produced by the config loader. This is the
/// generic lookup structure the D-Bus side (dbus/dispatch.rs) and the
/// Varlink side (varlink/server.rs, varlink/streaming.rs) both consult.
#[derive(Debug, Default, Clone)]
pub struct DispatchTable {
    pub services: HashMap<String, Arc<LoadedService>>,
    pub methods: HashMap<MethodKey, MethodEntry>,
    pub signals: HashMap<SignalKey, SignalEntry>,
    pub properties: HashMap<PropertyKey, PropertyEntry>,
    pub registrables: HashMap<RegistrableKey, RegistrableEntry>,
    /// `[[resolve]]` entries (dbus/resolver.rs) keyed by the Varlink
    /// interface name they resolve, independent of `services` above -
    /// see `schema::ResolveConfig`'s doc comment for why these are never
    /// folded into `services`/RequestName'd.
    pub resolvables: HashMap<String, ResolvableEntry>,
}

/// One `[[resolve]]` entry, ready to route a resolver-forwarded call:
/// which bus to make the outbound D-Bus call on, and where.
#[derive(Debug, Clone)]
pub struct ResolvableEntry {
    pub destination: String,
    pub path: String,
    pub bus: Bus,
}

impl DispatchTable {
    /// All configured D-Bus well-known names, across every loaded service.
    pub fn bus_names(&self) -> impl Iterator<Item = &str> {
        self.services.keys().map(|s| s.as_str())
    }

    /// The most permissive (max) configured idle timeout across every
    /// loaded service, per docs/DESIGN_BRIEF_V1.md Section 3.7 ("enforced at the process
    /// level using the max/most-permissive configured value across all
    /// loaded services, since one process now serves many names").
    pub fn max_idle_timeout(&self) -> Duration {
        self.services
            .values()
            .map(|s| Duration::from_secs(s.idle_timeout_secs))
            .max()
            .unwrap_or(Duration::from_secs(30))
    }

    /// Split into a (session-bus, system-bus) pair of tables. One resident
    /// *process* still serves both (docs/DESIGN_BRIEF_V1.md Section 2.1's "one resident
    /// process" is a process-count invariant, not a connection-count one);
    /// in practice this means two independent `BridgeState`s, each with its
    /// own bus connection, share the process and the control channel.
    pub fn partition_by_bus(self) -> (DispatchTable, DispatchTable) {
        let mut session = DispatchTable::default();
        let mut system = DispatchTable::default();

        for (name, svc) in self.services {
            match svc.bus {
                Bus::Session => session.services.insert(name, svc),
                Bus::System => system.services.insert(name, svc),
            };
        }
        for (k, v) in self.methods {
            match v.service.bus {
                Bus::Session => session.methods.insert(k, v),
                Bus::System => system.methods.insert(k, v),
            };
        }
        for (k, v) in self.signals {
            match v.service.bus {
                Bus::Session => session.signals.insert(k, v),
                Bus::System => system.signals.insert(k, v),
            };
        }
        for (k, v) in self.properties {
            match v.service.bus {
                Bus::Session => session.properties.insert(k, v),
                Bus::System => system.properties.insert(k, v),
            };
        }
        for (k, v) in self.registrables {
            match v.service.bus {
                Bus::Session => session.registrables.insert(k, v),
                Bus::System => system.registrables.insert(k, v),
            };
        }
        for (k, v) in self.resolvables {
            match v.bus {
                Bus::Session => session.resolvables.insert(k, v),
                Bus::System => system.resolvables.insert(k, v),
            };
        }

        (session, system)
    }
}

/// Scan `dir` for `*.toml` files (a stray `*.toml.example` is intentionally
/// ignored - it's a template, not an active config) and merge everything
/// found into a single `DispatchTable`. Per docs/DESIGN_BRIEF_V1.md Section 3.1: "the
/// loader should just merge everything found in the directory."
pub fn load_conf_d(dir: &Path) -> Result<DispatchTable, BoxError> {
    let mut table = DispatchTable::default();
    let mut entries: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "toml").unwrap_or(false))
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!(dir = %dir.display(), "conf.d directory does not exist, starting with empty dispatch table");
            Vec::new()
        }
        Err(e) => return Err(e.into()),
    };
    entries.sort();

    for path in entries {
        if let Err(e) = load_one_file(&path, &mut table) {
            warn!(file = %path.display(), error = %e, "skipping unparseable config file");
        }
    }

    Ok(table)
}

fn load_one_file(path: &Path, table: &mut DispatchTable) -> Result<(), BoxError> {
    let text = std::fs::read_to_string(path)?;
    let file = ServiceFile::parse(&text)?;
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));

    match &file.service {
        Some(service_config) => {
            let name = service_config.name.clone();
            let service = Arc::new(LoadedService {
                source_path: path.to_path_buf(),
                bus: service_config.bus,
                name: name.clone(),
                object_path: service_config.object_path.clone(),
                introspection_xml: service_config
                    .introspection_xml
                    .as_ref()
                    .map(|p| config_dir.join(p)),
                idle_timeout_secs: service_config.idle_timeout_secs,
                varlink: file.varlink.clone(),
                passthrough: service_config.passthrough,
                peer: None,
            });

            for m in file.methods {
                let key = (name.clone(), m.dbus_interface.clone(), m.dbus_method.clone());
                table.methods.insert(
                    key,
                    MethodEntry {
                        service: service.clone(),
                        mapping: m,
                    },
                );
            }
            for s in file.signals {
                let key = (name.clone(), s.dbus_interface.clone(), s.dbus_signal.clone());
                table.signals.insert(
                    key,
                    SignalEntry {
                        service: service.clone(),
                        mapping: s,
                    },
                );
            }
            for p in file.properties {
                let key = (name.clone(), p.dbus_interface.clone(), p.dbus_property.clone());
                table.properties.insert(
                    key,
                    PropertyEntry {
                        service: service.clone(),
                        mapping: p,
                    },
                );
            }
            for r in file.registrables {
                let key = (name.clone(), r.dbus_interface.clone());
                table.registrables.insert(
                    key,
                    RegistrableEntry {
                        service: service.clone(),
                        config: r,
                    },
                );
            }

            table.services.insert(name, service);
        }
        None => {
            if !file.methods.is_empty()
                || !file.signals.is_empty()
                || !file.properties.is_empty()
                || !file.registrables.is_empty()
            {
                warn!(
                    file = %path.display(),
                    "[[method]]/[[signal]]/[[property]]/[[registrable]] entries require a [service] block in the same file to attach to; ignoring them"
                );
            }
        }
    }

    // [[resolve]] entries are always independent of [service] above -
    // see schema::ResolveConfig's doc comment for why.
    for r in file.resolves {
        table.resolvables.insert(
            r.interface.clone(),
            ResolvableEntry {
                destination: r.destination,
                path: r.path,
                bus: r.bus,
            },
        );
    }

    Ok(())
}

/// Result of comparing two successive `DispatchTable`s during hot-reload:
/// names to `RequestName` and names to release. docs/DESIGN_BRIEF_V1.md Section 3.1:
/// "reconcile added/changed/removed mapping files without a full restart -
/// new D-Bus names get RequestName'd, removed ones get released."
#[derive(Debug, Default, Clone)]
pub struct ReloadDiff {
    pub added_names: Vec<String>,
    pub removed_names: Vec<String>,
}

pub fn diff_tables(old: &DispatchTable, new: &DispatchTable) -> ReloadDiff {
    let old_names: std::collections::HashSet<_> = old.services.keys().cloned().collect();
    let new_names: std::collections::HashSet<_> = new.services.keys().cloned().collect();
    ReloadDiff {
        added_names: new_names.difference(&old_names).cloned().collect(),
        removed_names: old_names.difference(&new_names).cloned().collect(),
    }
}

/// A hot-reload trigger. The payload is deliberately not detailed (which
/// file changed, how) - on any change we just do a full rescan-and-diff of
/// `conf.d`, which is simple and cheap enough for a directory of TOML files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReloadEvent;

/// docs/DESIGN_BRIEF_V1.md Section 9 (Open Decisions): hot-reload watcher backend is
/// implemented via a trait, so a portable polling fallback is available
/// wherever the Linux-only `inotify` backend isn't (or isn't desired).
pub trait ConfigWatcher: Stream<Item = ReloadEvent> + Send {}
impl<T> ConfigWatcher for T where T: Stream<Item = ReloadEvent> + Send {}

/// Linux-native hot-reload backend. Only compiled when the `inotify-watch`
/// feature is enabled (see Cargo.toml) - never called directly from generic
/// code, only ever behind the `ConfigWatcher` trait object.
#[cfg(feature = "inotify-watch")]
pub struct InotifyConfigWatcher {
    stream: inotify::EventStream<[u8; 4096]>,
}

#[cfg(feature = "inotify-watch")]
impl InotifyConfigWatcher {
    pub fn new(dir: &Path) -> std::io::Result<Self> {
        use inotify::{Inotify, WatchMask};
        std::fs::create_dir_all(dir).ok();
        let inotify = Inotify::init()?;
        inotify.watches().add(
            dir,
            WatchMask::CREATE | WatchMask::MODIFY | WatchMask::DELETE | WatchMask::MOVE,
        )?;
        let stream = inotify.into_event_stream([0u8; 4096])?;
        Ok(Self { stream })
    }
}

#[cfg(feature = "inotify-watch")]
impl Stream for InotifyConfigWatcher {
    type Item = ReloadEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(_event))) => Poll::Ready(Some(ReloadEvent)),
            Poll::Ready(Some(Err(e))) => {
                warn!(error = %e, "inotify watcher error, stopping hot-reload stream");
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Portable fallback: poll the directory listing (paths + mtimes) on a
/// timer and emit a `ReloadEvent` whenever the snapshot changes. Works
/// anywhere, at the cost of latency bounded by `period`.
pub struct PollingConfigWatcher {
    dir: PathBuf,
    interval: tokio::time::Interval,
    last_snapshot: HashMap<PathBuf, SystemTime>,
}

impl PollingConfigWatcher {
    pub fn new(dir: PathBuf, period: Duration) -> Self {
        let last_snapshot = snapshot(&dir);
        Self {
            dir,
            interval: tokio::time::interval(period),
            last_snapshot,
        }
    }
}

fn snapshot(dir: &Path) -> HashMap<PathBuf, SystemTime> {
    let mut out = HashMap::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().map(|e| e == "toml").unwrap_or(false) {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(mtime) = meta.modified() {
                        out.insert(path, mtime);
                    }
                }
            }
        }
    }
    out
}

impl Stream for PollingConfigWatcher {
    type Item = ReloadEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.interval).poll_tick(cx) {
                Poll::Ready(_) => {
                    let snap = snapshot(&this.dir);
                    if snap != this.last_snapshot {
                        this.last_snapshot = snap;
                        return Poll::Ready(Some(ReloadEvent));
                    }
                    // No change this tick - loop back and wait for the next
                    // tick rather than returning Pending immediately, since
                    // poll_tick already registered the waker.
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Build the best available `ConfigWatcher` for this platform: inotify when
/// the feature is compiled in and initialization succeeds, a polling
/// fallback otherwise. This is the only place that chooses between the two
/// - everything else just holds a boxed `ConfigWatcher` stream.
pub fn build_watcher(dir: &Path) -> Pin<Box<dyn ConfigWatcher>> {
    #[cfg(feature = "inotify-watch")]
    {
        match InotifyConfigWatcher::new(dir) {
            Ok(w) => {
                info!(dir = %dir.display(), "watching conf.d via inotify");
                return Box::pin(w);
            }
            Err(e) => {
                warn!(error = %e, "inotify watcher unavailable, falling back to polling");
            }
        }
    }
    info!(dir = %dir.display(), "watching conf.d via polling fallback");
    Box::pin(PollingConfigWatcher::new(
        dir.to_path_buf(),
        Duration::from_secs(2),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_example_config_directory() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/conf.d/sni-watcher.toml.example"),
            tmp.path().join("sni-watcher.toml"),
        )
        .unwrap();
        std::fs::write(tmp.path().join("sni-watcher.xml"), "<node/>").unwrap();
        std::fs::write(tmp.path().join("sni-item.xml"), "<node/>").unwrap();

        let table = load_conf_d(tmp.path()).unwrap();
        assert_eq!(table.services.len(), 1);
        assert!(table
            .services
            .contains_key("org.kde.StatusNotifierWatcher"));
        assert_eq!(table.methods.len(), 1);
        assert_eq!(table.properties.len(), 1);
        assert_eq!(table.registrables.len(), 1);
    }

    #[test]
    fn diff_detects_added_and_removed_names() {
        let mut old = DispatchTable::default();
        let mut new = DispatchTable::default();
        old.services.insert(
            "org.example.Old".into(),
            Arc::new(LoadedService {
                source_path: "a".into(),
                bus: Bus::Session,
                name: "org.example.Old".into(),
                object_path: "/Old".into(),
                introspection_xml: Some("old.xml".into()),
                idle_timeout_secs: 30,
                varlink: VarlinkConfig::default(),
                passthrough: false,
                peer: None,
            }),
        );
        new.services.insert(
            "org.example.New".into(),
            Arc::new(LoadedService {
                source_path: "b".into(),
                bus: Bus::Session,
                name: "org.example.New".into(),
                object_path: "/New".into(),
                introspection_xml: Some("new.xml".into()),
                idle_timeout_secs: 30,
                varlink: VarlinkConfig::default(),
                passthrough: false,
                peer: None,
            }),
        );
        let diff = diff_tables(&old, &new);
        assert_eq!(diff.added_names, vec!["org.example.New".to_string()]);
        assert_eq!(diff.removed_names, vec!["org.example.Old".to_string()]);
    }
}
