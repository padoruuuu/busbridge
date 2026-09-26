//! The app registration protocol: lets a Varlink-only app become a real
//! D-Bus service without a config file, a separate `varlink.listen`
//! socket, or knowing busbridge exists as anything other than one
//! address it connects to. Full protocol description, wire examples, and
//! design rationale: `docs/REGISTRATION_PROTOCOL.md`. This module is the
//! implementation; that document is the contract.
//!
//! Everything a conf.d `[service]` block can do, a registration frame can
//! also do - `RegisterParams` below deserializes into the exact same
//! `MethodMapping`/`SignalMapping`/`PropertyMapping`/`RegistrableConfig`
//! types `config/schema.rs` already defines for TOML, just carried over
//! JSON instead. The only genuinely new thing is `introspection_xml`
//! being *inline XML text* here instead of a file path, since a
//! registering app has no filesystem convention to point at - see
//! `docs/REGISTRATION_PROTOCOL.md`'s "why inline XML, not a new schema"
//! section for why this reuses D-Bus's own introspection XML rather than
//! inventing a JSON type schema.
//!
//! Once registered, a service's whole "backend" is the live connection
//! the app registered on (`varlink/peer.rs::PeerConnection`) rather than
//! a dialed `varlink.backend` address - dispatch.rs's `call_backend`
//! helper is the only place that distinguishes the two, which is why
//! Introspect, static methods, passthrough, and Properties all work for
//! a registered service exactly as they do for a conf.d one: they all
//! flow through the same lookup code, only the transport differs.
//!
//! Two request types exist, both dispatched from `handle_registration_connection`:
//! `org.busbridge.Registration.Register` (`handle_register` - a whole
//! service) and `org.busbridge.Registration.RegisterDynamicObject`
//! (`handle_register_dynamic_object` - a third party registering one
//! dynamic object under some service's declared `registrables`, on a
//! completely separate connection from that service's own). The latter
//! hands off entirely to `dbus/dynamic_object.rs::register` - the exact
//! same function the conf.d-driven `[[registrable]]` mechanism already
//! uses via its own dedicated per-service socket
//! (`varlink/server.rs`) - so a dynamic object behaves identically
//! either way; only how the registering connection found busbridge
//! differs.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::schema::{Bus, MethodMapping, PropertyMapping, RegistrableConfig, SignalMapping, VarlinkConfig};
use crate::config::{DispatchTable, LoadedService, MethodEntry, PropertyEntry, RegistrableEntry, SignalEntry};
use crate::dbus::introspect::{self, InterfaceDesc};
use crate::dbus::BridgeState;
use crate::telemetry::Telemetry;
use crate::varlink::peer::{Frame, PeerConnection};
use crate::varlink::{read_framed, VarlinkRequest};
use crate::BoxError;

/// Tracks which `BridgeState` (if any) is currently serving each bus.
/// Replaces what used to be a fixed `Vec<Arc<BridgeState>>` computed once
/// at startup - see lib.rs's `async_main` doc comment on why a
/// registration needs this to be able to grow at runtime instead.
pub struct BusRegistry {
    telemetry: Arc<Telemetry>,
    states: RwLock<HashMap<Bus, Arc<BridgeState>>>,
}

impl BusRegistry {
    pub fn new(telemetry: Arc<Telemetry>) -> Self {
        Self {
            telemetry,
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Record a `BridgeState` this process already started (conf.d
    /// startup) as the one serving `bus`.
    pub async fn seed(&self, bus: Bus, state: Arc<BridgeState>) {
        self.states.write().await.insert(bus, state);
    }

    /// The `BridgeState` currently serving `bus`, if this process has
    /// one running yet.
    pub async fn get(&self, bus: Bus) -> Option<Arc<BridgeState>> {
        self.states.read().await.get(&bus).cloned()
    }

    /// Like `get`, but opens a fresh connection and starts serving `bus`
    /// (with an empty dispatch table) if nothing is running for it yet.
    /// Double-checked locking: the common case (already running) only
    /// ever needs a read lock.
    pub async fn get_or_start(&self, bus: Bus) -> Result<Arc<BridgeState>, BoxError> {
        if let Some(state) = self.get(bus).await {
            return Ok(state);
        }
        let mut states = self.states.write().await;
        if let Some(state) = states.get(&bus) {
            return Ok(state.clone());
        }
        info!(?bus, "starting bus connection on demand for an app registration");
        let state = crate::start_bus(bus, DispatchTable::default(), self.telemetry.clone()).await?;
        states.insert(bus, state.clone());
        Ok(state)
    }

    /// Every `BridgeState` currently running, in no particular order.
    pub async fn snapshot(&self) -> Vec<Arc<BridgeState>> {
        self.states.read().await.values().cloned().collect()
    }

    /// One arbitrary running `BridgeState`, for the legacy handoff
    /// protocol's best-effort routing (control.rs) - unrelated to
    /// registration itself, kept here only because this is now the
    /// shared place that knows what's running.
    pub async fn best_effort_single(&self) -> Option<Arc<BridgeState>> {
        self.states.read().await.values().next().cloned()
    }
}

/// The `parameters` of an `org.busbridge.Registration.Register` call.
/// Field-for-field, this is a `[service]` block (`config/schema.rs`'s
/// `ServiceConfig` plus its nested mapping lists) minus the file-specific
/// concepts (no `source_path`; `introspection_xml` is inline text, not a
/// path) and minus `[varlink]` (there is no separate backend/listen
/// address - the registration connection itself is all of those at
/// once).
#[derive(Debug, Deserialize)]
pub struct RegisterParams {
    pub bus: Bus,
    pub name: String,
    pub object_path: String,
    /// Inline D-Bus introspection XML text (not a file path - see this
    /// module's doc comment). Only needed for interfaces this service
    /// declares `methods`/`signals`/`properties` for, or that legacy
    /// D-Bus peers will `Introspect()`; omit entirely for a
    /// `passthrough`-only service with no static mappings.
    #[serde(default)]
    pub introspection_xml: Option<String>,
    #[serde(default)]
    pub passthrough: bool,
    #[serde(default)]
    pub methods: Vec<MethodMapping>,
    #[serde(default)]
    pub signals: Vec<SignalMapping>,
    #[serde(default)]
    pub properties: Vec<PropertyMapping>,
    /// Interfaces third parties can register dynamic objects under via
    /// `org.busbridge.Registration.RegisterDynamicObject` - see this
    /// module's doc comment and `docs/REGISTRATION_PROTOCOL.md`.
    #[serde(default)]
    pub registrables: Vec<RegistrableConfig>,
}

/// Entry point from control.rs once it's peeked a connection's first
/// frame and found a `method` field. `first_frame` is that already-read
/// frame (control.rs can't "unread" it, so it's handed over parsed
/// rather than re-read from `stream`).
pub async fn handle_registration_connection(registry: Arc<BusRegistry>, stream: UnixStream, first_frame: JsonValue) {
    let id = first_frame.get("id").cloned();
    let method = first_frame.get("method").and_then(|m| m.as_str()).unwrap_or_default().to_string();

    let Some(id) = id else {
        warn!("registration attempt with no \"id\" field - every request on this connection needs one, see docs/REGISTRATION_PROTOCOL.md");
        return;
    };

    match method.as_str() {
        "org.busbridge.Registration.Register" => handle_register(registry, stream, id, first_frame).await,
        "org.busbridge.Registration.RegisterDynamicObject" => {
            handle_register_dynamic_object(registry, stream, id, first_frame).await
        }
        other => {
            let mut stream = stream;
            let _ = crate::varlink::write_framed(
                &mut stream,
                &Frame::reply_err(
                    id,
                    "org.busbridge.Registration.UnknownMethod",
                    json!({"message": format!("unknown method {other}, expected org.busbridge.Registration.Register or org.busbridge.Registration.RegisterDynamicObject as the first frame")}),
                ),
            )
            .await;
        }
    }
}

async fn handle_register(registry: Arc<BusRegistry>, stream: UnixStream, id: JsonValue, first_frame: JsonValue) {
    let params: RegisterParams = match first_frame.get("parameters").cloned() {
        Some(p) => match serde_json::from_value(p) {
            Ok(p) => p,
            Err(e) => {
                let mut stream = stream;
                let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.InvalidParameters", json!({"message": e.to_string()}))).await;
                return;
            }
        },
        None => {
            let mut stream = stream;
            let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.InvalidParameters", json!({"message": "missing parameters"}))).await;
            return;
        }
    };

    if !params.registrables.is_empty() {
        info!(
            name = %params.name,
            count = params.registrables.len(),
            "registered service declares registrables - third parties can register dynamic objects under it via org.busbridge.Registration.RegisterDynamicObject"
        );
    }

    let interfaces = match &params.introspection_xml {
        Some(xml) => match introspect::parse(xml) {
            Ok(ifaces) => ifaces,
            Err(e) => {
                let mut stream = stream;
                let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.InvalidIntrospectionXml", json!({"message": e}))).await;
                return;
            }
        },
        None => HashMap::new(),
    };

    let state = match registry.get_or_start(params.bus).await {
        Ok(state) => state,
        Err(e) => {
            let mut stream = stream;
            let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.BusUnavailable", json!({"message": e.to_string()}))).await;
            return;
        }
    };

    if state.dispatch.read().await.services.contains_key(&params.name) {
        let mut stream = stream;
        let _ = crate::varlink::write_framed(
            &mut stream,
            &Frame::reply_err(id, "org.busbridge.Registration.NameAlreadyOwned", json!({"message": format!("{} is already registered on this bus", params.name)})),
        )
        .await;
        return;
    }

    // Confirm actual D-Bus ownership *before* wiring anything up or
    // acking success: a call addressed to a well-known name we don't
    // actually own would never reach us anyway (D-Bus routes by real
    // ownership, not by what's in our own dispatch table), so failing
    // fast here instead of warning-and-proceeding avoids silently
    // registering a service nothing can ever actually reach.
    if let Err(e) = state.connection.request_name(params.name.as_str()).await {
        let mut stream = stream;
        let _ = crate::varlink::write_framed(
            &mut stream,
            &Frame::reply_err(id, "org.busbridge.Registration.NameAlreadyOwned", json!({"message": e.to_string()})),
        )
        .await;
        return;
    }

    // Held for the connection's entire lifetime: as long as it's open,
    // this counts as activity on `state`'s bus, regardless of how much
    // traffic actually flows over it - see docs/REGISTRATION_PROTOCOL.md's
    // "idle-exit" section and dynamic_object.rs's `_hold` field, the same
    // idea applied one level up (a whole service instead of one object).
    let hold = state.activity.hold();

    let (read_half, write_half) = stream.into_split();
    let peer = PeerConnection::new(Box::new(write_half), hold);

    let service = Arc::new(LoadedService {
        source_path: format!("registration:{}", params.name).into(),
        bus: params.bus,
        name: params.name.clone(),
        object_path: params.object_path.clone(),
        introspection_xml: None,
        // Inconsequential for a registered service: idle-exit is
        // governed by the ActivityGuard (`hold`, below) held for the
        // connection's whole lifetime, not by this field - it only ever
        // feeds into the *aggregate* process-wide idle timeout computed
        // once at startup from conf.d (`DispatchTable::max_idle_timeout`),
        // which a service added after startup can no longer influence
        // anyway. Matches config/schema.rs's own TOML default (30s) so
        // there's nothing surprising about the number if it's ever
        // observed (e.g. via a future introspection/debug endpoint).
        idle_timeout_secs: 30,
        varlink: VarlinkConfig::default(),
        passthrough: params.passthrough,
        peer: Some(peer.clone()),
    });

    {
        let mut table = state.dispatch.write().await;
        insert_service(
            &mut table,
            &service,
            &params.methods,
            &params.signals,
            &params.properties,
            &params.registrables,
        );
    }
    if !interfaces.is_empty() {
        state.introspection.write().await.insert(params.name.clone(), interfaces);
    }

    info!(name = %params.name, bus = ?params.bus, object_path = %params.object_path, "app registered as a D-Bus service");

    let ack = Frame::reply_ok(id, json!({"object_path": params.object_path}));
    if peer.reply(ack).await.is_err() {
        // The app vanished between registering and us acking - clean up
        // immediately rather than leaving a dead entry around until some
        // future call happens to notice.
        teardown(&state, &params.name).await;
        return;
    }

    let reader = BufReader::new(read_half);
    run_read_loop(state, service, peer, reader).await;
}

#[derive(Debug, Deserialize)]
struct RegisterDynamicObjectParams {
    /// Which registered (or conf.d) service to register under -
    /// `RegistrableEntry`s are keyed by `(bus_name, dbus_interface)`, and
    /// a bus_name uniquely identifies one bus at a time in practice, so
    /// the caller doesn't also need to say which bus: every currently-
    /// running bus's dispatch table is checked (see the loop below).
    bus_name: String,
    dbus_interface: String,
    #[serde(default)]
    id: Option<String>,
}

/// The complement to `handle_register`, for `docs/REGISTRATION_PROTOCOL.md`'s
/// previously-not-done `registrables` case: a third party registering a
/// *dynamic object* under some other service's declared `registrables`
/// entry - whether that service came from conf.d or from `handle_register`
/// itself doesn't matter, since either way it ends up as an ordinary
/// `RegistrableEntry` in some `BridgeState`'s dispatch table. This
/// function's whole job is finding which one and handing off to
/// `dbus/dynamic_object.rs::register` - the exact same function
/// `varlink/server.rs`'s own dedicated-per-service-socket accept loop
/// already uses for the conf.d case (see that function's doc comment for
/// the one place their behavior differs: the ack's shape).
async fn handle_register_dynamic_object(registry: Arc<BusRegistry>, stream: UnixStream, id: JsonValue, first_frame: JsonValue) {
    let params: RegisterDynamicObjectParams = match first_frame.get("parameters").cloned() {
        Some(p) => match serde_json::from_value(p) {
            Ok(p) => p,
            Err(e) => {
                let mut stream = stream;
                let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.InvalidParameters", json!({"message": e.to_string()}))).await;
                return;
            }
        },
        None => {
            let mut stream = stream;
            let _ = crate::varlink::write_framed(&mut stream, &Frame::reply_err(id, "org.busbridge.Registration.InvalidParameters", json!({"message": "missing parameters"}))).await;
            return;
        }
    };

    let mut found = None;
    for state in registry.snapshot().await {
        let table = state.dispatch.read().await;
        if let Some(entry) = table.registrables.get(&(params.bus_name.clone(), params.dbus_interface.clone())) {
            found = Some((state.clone(), entry.service.clone(), entry.config.clone()));
            break;
        }
    }
    let Some((state, service, registrable_config)) = found else {
        let mut stream = stream;
        let _ = crate::varlink::write_framed(
            &mut stream,
            &Frame::reply_err(
                id,
                "org.busbridge.Registration.UnknownRegistrable",
                json!({"message": format!("no service named {} on any currently-running bus declares {} as a registrable interface", params.bus_name, params.dbus_interface)}),
            ),
        )
        .await;
        return;
    };

    let (read_half, write_half) = stream.into_split();
    let reader: BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>> = BufReader::new(Box::new(read_half));
    let writer: Box<dyn tokio::io::AsyncWrite + Unpin + Send> = Box::new(write_half);

    match crate::dbus::dynamic_object::register(state, service, registrable_config, params.id, Some(id), reader, writer).await {
        Ok(object_path) => {
            info!(
                bus_name = %params.bus_name,
                interface = %params.dbus_interface,
                object_path = %object_path,
                "dynamic object registered via the app registration protocol"
            );
        }
        Err(e) => {
            // register() only ever writes an ack on success - by the time
            // it returns Err, the writer it was given has already been
            // moved into (and dropped along with) its own internals, so
            // there's no reply frame we can send back here. Same
            // limitation the conf.d-driven caller in varlink/server.rs
            // already has; not something this path introduces, and not
            // fixed here - the caller sees the connection simply close
            // rather than an error frame. Worth revisiting if this proves
            // confusing in practice (register() would need to hand back
            // its writer on failure, or take the ack-writing out of its
            // own hands entirely).
            warn!(
                bus_name = %params.bus_name,
                interface = %params.dbus_interface,
                error = %e,
                "dynamic object registration via the app registration protocol failed"
            );
        }
    }
}

/// Insert `service` and its method/signal/property mappings into
/// `table`. Mirrors `config/mod.rs::load_one_file`'s insertion logic for
/// a `[service]` block - kept as a separate, small, from-scratch
/// function here rather than factored out of `load_one_file` and shared,
/// since that function is existing, tested, file-loading-specific code
/// and this one's needs (no file, no config_dir-relative paths) are
/// different enough that sharing would mean threading `Option`s through
/// working code for a second caller's benefit. A few near-identical
/// lines duplicated is a smaller risk than that.
fn insert_service(
    table: &mut DispatchTable,
    service: &Arc<LoadedService>,
    methods: &[MethodMapping],
    signals: &[SignalMapping],
    properties: &[PropertyMapping],
    registrables: &[RegistrableConfig],
) {
    table.services.insert(service.name.clone(), service.clone());

    // Auto-advertise every distinct interface this service declares
    // through `org.varlink.resolver` (dbus/resolver.rs), so a
    // Varlink-native client can find it by interface name alone without
    // needing to know it's D-Bus-backed at all - the same
    // "an app only needs to speak Varlink" property the resolver already
    // gives `[[resolve]]`-configured conf.d services. Unlike
    // `[[resolve]]` (deliberately standalone/not tied to name ownership -
    // see config/schema.rs's `ResolveConfig` doc comment, since a
    // resolvable's destination is very often owned by some *other*
    // process), this one always is: a registered service's declared
    // interfaces are, by construction, exactly what's live on this
    // connection. A `passthrough`-only service with no declared
    // interfaces at all (no methods/signals/properties) has no interface
    // name to advertise, so it gets no resolvable entries - there's
    // nothing wrong with that, just nothing to resolve.
    let mut interfaces_to_resolve = std::collections::HashSet::new();
    interfaces_to_resolve.extend(methods.iter().map(|m| m.dbus_interface.clone()));
    interfaces_to_resolve.extend(signals.iter().map(|s| s.dbus_interface.clone()));
    interfaces_to_resolve.extend(properties.iter().map(|p| p.dbus_interface.clone()));
    for interface in interfaces_to_resolve {
        table.resolvables.insert(
            interface,
            crate::config::ResolvableEntry {
                destination: service.name.clone(),
                path: service.object_path.clone(),
                bus: service.bus,
            },
        );
    }

    for m in methods {
        table.methods.insert(
            (service.name.clone(), m.dbus_interface.clone(), m.dbus_method.clone()),
            MethodEntry {
                service: service.clone(),
                mapping: m.clone(),
            },
        );
    }
    for s in signals {
        table.signals.insert(
            (service.name.clone(), s.dbus_interface.clone(), s.dbus_signal.clone()),
            SignalEntry {
                service: service.clone(),
                mapping: s.clone(),
            },
        );
    }
    for p in properties {
        table.properties.insert(
            (service.name.clone(), p.dbus_interface.clone(), p.dbus_property.clone()),
            PropertyEntry {
                service: service.clone(),
                mapping: p.clone(),
            },
        );
    }
    for r in registrables {
        table.registrables.insert(
            (service.name.clone(), r.dbus_interface.clone()),
            RegistrableEntry {
                service: service.clone(),
                config: r.clone(),
            },
        );
    }
}

/// Everything currently in `state`'s dispatch table (and introspection
/// cache) that came from an app registration rather than conf.d -
/// `run_hot_reload` (lib.rs) snapshots this before applying a freshly-
/// reloaded conf.d table and restores it after, so a conf.d file edit on
/// disk never silently un-registers a live app. conf.d's own loader has
/// no way to know these entries exist (there's no file backing them), so
/// without this, `apply_reload`'s wholesale table replacement would drop
/// them on the very next unrelated hot-reload.
pub struct RegisteredSnapshot {
    table: DispatchTable,
    introspection: HashMap<String, HashMap<String, InterfaceDesc>>,
}

pub async fn snapshot_registered(state: &BridgeState) -> RegisteredSnapshot {
    let current = state.dispatch.read().await;
    let mut table = DispatchTable::default();
    for (name, service) in &current.services {
        if service.peer.is_some() {
            table.services.insert(name.clone(), service.clone());
        }
    }
    for (k, v) in &current.methods {
        if v.service.peer.is_some() {
            table.methods.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in &current.signals {
        if v.service.peer.is_some() {
            table.signals.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in &current.properties {
        if v.service.peer.is_some() {
            table.properties.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in &current.registrables {
        if v.service.peer.is_some() {
            table.registrables.insert(k.clone(), v.clone());
        }
    }
    // Resolvables (registration.rs::insert_service's auto-generated
    // entries) aren't keyed by service name themselves (they're keyed by
    // interface name, and don't carry the service back-reference the
    // other maps do), so identify which ones are "ours" by destination
    // instead - correct as long as two services never legitimately
    // resolve to the very same destination name, which registration.rs
    // already guarantees elsewhere (NameAlreadyOwned).
    let registered_names: std::collections::HashSet<&str> = table.services.keys().map(|s| s.as_str()).collect();
    for (interface, entry) in &current.resolvables {
        if registered_names.contains(entry.destination.as_str()) {
            table.resolvables.insert(interface.clone(), entry.clone());
        }
    }
    drop(current);

    let introspection_all = state.introspection.read().await;
    let mut introspection = HashMap::new();
    for name in table.services.keys() {
        if let Some(ifaces) = introspection_all.get(name) {
            introspection.insert(name.clone(), ifaces.clone());
        }
    }
    RegisteredSnapshot { table, introspection }
}

/// Fold a snapshot's entries into `new_table`, in place - call before
/// `state.apply_reload(new_table)`.
pub fn merge_registered_into(new_table: &mut DispatchTable, snapshot: &RegisteredSnapshot) {
    for (k, v) in &snapshot.table.services {
        new_table.services.insert(k.clone(), v.clone());
    }
    for (k, v) in &snapshot.table.methods {
        new_table.methods.insert(k.clone(), v.clone());
    }
    for (k, v) in &snapshot.table.signals {
        new_table.signals.insert(k.clone(), v.clone());
    }
    for (k, v) in &snapshot.table.properties {
        new_table.properties.insert(k.clone(), v.clone());
    }
    for (k, v) in &snapshot.table.registrables {
        new_table.registrables.insert(k.clone(), v.clone());
    }
    for (k, v) in &snapshot.table.resolvables {
        new_table.resolvables.insert(k.clone(), v.clone());
    }
}

/// Restore a snapshot's introspection entries - call after
/// `state.apply_reload(new_table)`, which otherwise leaves them dropped
/// (it rebuilds the introspection cache purely from `introspection_xml`
/// *file paths*, which registered services don't have - see this
/// module's doc comment).
pub async fn restore_registered_introspection(state: &BridgeState, snapshot: &RegisteredSnapshot) {
    if snapshot.introspection.is_empty() {
        return;
    }
    let mut introspection = state.introspection.write().await;
    for (name, ifaces) in &snapshot.introspection {
        introspection.insert(name.clone(), ifaces.clone());
    }
}

async fn teardown(state: &BridgeState, service_name: &str) {
    info!(name = service_name, "registered app disconnected, releasing name and dispatch entries");
    let mut table = state.dispatch.write().await;
    table.services.remove(service_name);
    table.methods.retain(|k, _| k.0 != service_name);
    table.signals.retain(|k, _| k.0 != service_name);
    table.properties.retain(|k, _| k.0 != service_name);
    table.registrables.retain(|k, _| k.0 != service_name);
    table.resolvables.retain(|_, v| v.destination != service_name);
    let _ = state.connection.release_name(service_name).await;
    state.introspection.write().await.remove(service_name);
}

async fn run_read_loop(
    state: Arc<BridgeState>,
    service: Arc<LoadedService>,
    peer: Arc<PeerConnection>,
    mut reader: BufReader<tokio::net::unix::OwnedReadHalf>,
) {
    loop {
        let frame: Frame = match read_framed(&mut reader).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                warn!(name = %service.name, error = %e, "registration connection read error");
                break;
            }
        };

        if frame.id.is_some() && frame.method.is_some() {
            let state = state.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                handle_peer_request(state, peer, frame).await;
            });
        } else if frame.method.is_some() {
            let state = state.clone();
            let service = service.clone();
            tokio::spawn(async move {
                let req = VarlinkRequest {
                    method: frame.method.unwrap(),
                    parameters: frame.parameters,
                    more: false,
                    oneway: true,
                };
                crate::varlink::server::handle_push_request(&state, &service, req).await;
            });
        } else {
            peer.resolve_bridge_reply(frame).await;
        }
    }

    teardown(&state, &service.name).await;
}

async fn handle_peer_request(state: Arc<BridgeState>, peer: Arc<PeerConnection>, frame: Frame) {
    let id = frame.id.clone().unwrap_or(JsonValue::Null);
    let method = frame.method.clone().unwrap_or_default();
    let mut reply = match method.as_str() {
        "org.busbridge.Peer.Call" => handle_peer_call(&state, frame.parameters).await,
        "org.busbridge.Peer.Subscribe" => handle_peer_subscribe(&state, &peer, frame.parameters).await,
        "org.busbridge.Peer.Unsubscribe" => handle_peer_unsubscribe(&peer, frame.parameters).await,
        "org.busbridge.Peer.FindObjects" => handle_peer_find_objects(&state, frame.parameters).await,
        other => Frame::reply_err(
            JsonValue::Null,
            "org.busbridge.Peer.UnknownMethod",
            json!({"message": format!("unknown method {other}")}),
        ),
    };
    reply.id = Some(id);
    let _ = peer.reply(reply).await;
}

#[derive(Debug, Deserialize)]
struct PeerCallParams {
    destination: String,
    path: String,
    interface: String,
    method: String,
    #[serde(default)]
    args: Vec<JsonValue>,
}

/// `org.busbridge.Peer.Call`: an arbitrary outbound D-Bus call, on behalf
/// of the registered app, exactly like `dbus/bus_proxy.rs`'s `Call` -
/// same reply/error shaping, same auto-introspection-then-inference
/// argument conversion, reusing that module's `perform_call`/
/// `introspect_method` directly rather than re-deriving the logic. Not
/// shared at the handler-function level (unlike `handle_push_request`
/// above) because bus_proxy.rs's handlers write their reply directly to
/// a writer with no `id`, which this protocol's multiplexed replies need
/// - see `varlink/peer.rs`'s module doc comment, point 2.
async fn handle_peer_call(state: &BridgeState, parameters: Option<JsonValue>) -> Frame {
    let params: PeerCallParams = match parameters.and_then(|p| serde_json::from_value(p).ok()) {
        Some(p) => p,
        None => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": "missing or malformed parameters"})),
    };

    let method_desc = crate::dbus::bus_proxy::introspect_method(&state.connection, &params.destination, &params.path, &params.interface, &params.method).await;

    let mut fields: Vec<zvariant::Value<'static>> = Vec::new();
    let conversion: Result<(), String> = 'convert: {
        if let Some(desc) = &method_desc {
            if !desc.in_args.is_empty() || params.args.is_empty() {
                if desc.in_args.len() != params.args.len() {
                    break 'convert Err(format!("{} expects {} argument(s), got {}", params.method, desc.in_args.len(), params.args.len()));
                }
                for (arg, json_val) in desc.in_args.iter().zip(params.args.iter()) {
                    match crate::convert::parse_single_complete_type(&arg.type_sig)
                        .map_err(|e| e.to_string())
                        .and_then(|ty| crate::convert::json_to_dbus(json_val, &ty).map_err(|e| e.to_string()))
                    {
                        Ok(v) => fields.push(v),
                        Err(e) => break 'convert Err(e),
                    }
                }
                break 'convert Ok(());
            }
        }
        for json_val in &params.args {
            match crate::convert::json_to_dbus_inferred(json_val) {
                Ok(v) => fields.push(v),
                Err(e) => break 'convert Err(e.to_string()),
            }
        }
        Ok(())
    };
    if let Err(e) = conversion {
        return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e}));
    }

    let result = crate::dbus::bus_proxy::perform_call(state, &params.destination, &params.path, &params.interface, &params.method, fields).await;
    match result {
        Ok(reply_msg) => match crate::dbus::dispatch::extract_arg_values(&reply_msg) {
            Ok(values) => Frame::reply_ok(JsonValue::Null, json!({"reply": values})),
            Err(e) => Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.CallFailed", json!({"message": e.to_string()})),
        },
        Err(zbus::Error::MethodError(name, detail, _)) => {
            Frame::reply_err(JsonValue::Null, name.to_string(), json!({"message": detail.unwrap_or_default()}))
        }
        Err(other) => Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.CallFailed", json!({"message": other.to_string()})),
    }
}

#[derive(Debug, Default, Deserialize)]
struct PeerSubscribeParams {
    #[serde(default)]
    sender: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    interface: Option<String>,
    #[serde(default)]
    member: Option<String>,
}

/// `org.busbridge.Peer.Subscribe`: matches `dbus/bus_proxy.rs`'s own
/// `Subscribe` (same `MatchRule` fields, same match semantics), but
/// pushes events tagged with a `subscription_id` instead of holding the
/// connection's writer hostage for a `"more": true` stream - this
/// connection has other traffic multiplexed on it, unlike bus_proxy.rs's
/// dedicated one-subscription-per-connection design.
async fn handle_peer_subscribe(state: &BridgeState, peer: &Arc<PeerConnection>, parameters: Option<JsonValue>) -> Frame {
    let params: PeerSubscribeParams = match parameters {
        None => PeerSubscribeParams::default(),
        Some(p) => match serde_json::from_value(p) {
            Ok(p) => p,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e.to_string()})),
        },
    };

    let mut builder = zbus::MatchRule::builder().msg_type(zbus::message::Type::Signal);
    if let Some(sender) = &params.sender {
        builder = match builder.sender(sender.as_str()) {
            Ok(b) => b,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e.to_string()})),
        };
    }
    if let Some(path) = &params.path {
        builder = match builder.path(path.as_str()) {
            Ok(b) => b,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e.to_string()})),
        };
    }
    if let Some(interface) = &params.interface {
        builder = match builder.interface(interface.as_str()) {
            Ok(b) => b,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e.to_string()})),
        };
    }
    if let Some(member) = &params.member {
        builder = match builder.member(member.as_str()) {
            Ok(b) => b,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": e.to_string()})),
        };
    }
    let rule = builder.build();

    let mut stream = match zbus::MessageStream::for_match_rule(rule, &state.connection, None).await {
        Ok(s) => s,
        Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.CallFailed", json!({"message": e.to_string()})),
    };

    let subscription_id = peer.next_subscription_id();
    let peer_for_task = peer.clone();
    let sub_id_for_task = subscription_id.clone();
    let handle = tokio::spawn(async move {
        use futures_util::StreamExt;
        while let Some(Ok(msg)) = stream.next().await {
            let sender = msg.header().sender().map(|s| s.to_string()).unwrap_or_default();
            let path = msg.header().path().map(|p| p.to_string()).unwrap_or_default();
            let interface = msg.header().interface().map(|i| i.to_string()).unwrap_or_default();
            let member = msg.header().member().map(|m| m.to_string()).unwrap_or_default();
            let args = crate::dbus::dispatch::extract_arg_values(&msg).unwrap_or_default();
            let payload = json!({
                "sender": sender,
                "path": path,
                "interface": interface,
                "member": member,
                "args": args,
            });
            if peer_for_task.push(Frame::subscription_event(sub_id_for_task.clone(), payload)).await.is_err() {
                break;
            }
        }
    });
    peer.register_subscription(subscription_id.clone(), handle).await;

    Frame::reply_ok(JsonValue::Null, json!({"subscription_id": subscription_id}))
}

#[derive(Debug, Deserialize)]
struct PeerUnsubscribeParams {
    subscription_id: String,
}

async fn handle_peer_unsubscribe(peer: &Arc<PeerConnection>, parameters: Option<JsonValue>) -> Frame {
    let params: PeerUnsubscribeParams = match parameters.and_then(|p| serde_json::from_value(p).ok()) {
        Some(p) => p,
        None => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": "missing subscription_id"})),
    };
    if peer.cancel_subscription(&params.subscription_id).await {
        Frame::reply_ok(JsonValue::Null, json!({}))
    } else {
        Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.UnknownSubscription", json!({"subscription_id": params.subscription_id}))
    }
}

#[derive(Debug, Deserialize)]
struct PeerFindObjectsParams {
    /// The D-Bus interface to look for.
    interface: String,
    /// Restrict the search to these bus names instead of every currently
    /// connected one. Mostly useful for re-checking a specific name (e.g.
    /// after a `NameOwnerChanged` for it) without repeating a full-bus
    /// scan.
    #[serde(default)]
    bus_names: Option<Vec<String>>,
}

// Per-name walk budget: enough to find an object nested several levels
// deep in a real app's tree without one badly-behaved or enormous bus
// name (of which most connected names, being completely unrelated
// services, will introspect almost nothing at all) blowing out the total
// cost of one FindObjects call, which - unlike a conf.d-configured
// service's own dispatch - runs against however many names ListNames
// happens to return at call time.
const FIND_OBJECTS_MAX_NODES_PER_NAME: usize = 80;
const FIND_OBJECTS_MAX_DEPTH: u32 = 8;
const FIND_OBJECTS_MAX_NAMES: usize = 256;

/// `org.busbridge.Peer.FindObjects`: given a D-Bus interface name, finds
/// every object - on every currently-connected unique bus name, or a
/// caller-supplied subset - that implements it. A bounded breadth-first
/// introspection walk per name, exactly the kind of thing
/// docs/ARCHITECTURE.md's assessment flagged as something apps kept
/// having to reimplement client-side for themselves (each one slightly
/// differently, and each one prone to the same class of bugs: giving up
/// too early, not going deep enough, or both) to find things like a
/// StatusNotifierItem or a DBusMenu object that doesn't sit at any
/// standardized path. Doing it once here means N registered apps don't
/// each redundantly walk the same bus.
///
/// Not cached (yet): every call does a fresh `ListNames` and a fresh
/// walk of whichever names it's given. Worth adding if this turns out to
/// be called often enough for it to matter - invalidated on
/// `NameOwnerChanged` would be the natural approach - but a fresh walk
/// is correct today, just not free, which is exactly why `bus_names` lets
/// a caller narrow the search once it already knows roughly where to
/// look (e.g. just-appeared names from its own `NameOwnerChanged`
/// subscription) instead of re-scanning the whole bus every time.
async fn handle_peer_find_objects(state: &BridgeState, parameters: Option<JsonValue>) -> Frame {
    let params: PeerFindObjectsParams = match parameters.and_then(|p| serde_json::from_value(p).ok()) {
        Some(p) => p,
        None => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.InvalidParameters", json!({"message": "missing interface"})),
    };

    let names = match params.bus_names {
        Some(names) => names,
        None => match list_unique_names(state).await {
            Ok(names) => names,
            Err(e) => return Frame::reply_err(JsonValue::Null, "org.busbridge.Peer.CallFailed", json!({"message": e})),
        },
    };

    let mut items = Vec::new();
    for name in names.into_iter().take(FIND_OBJECTS_MAX_NAMES) {
        if let Some(path) = find_object_implementing(state, &name, &params.interface).await {
            items.push(json!({"bus_name": name, "object_path": path}));
        }
    }
    Frame::reply_ok(JsonValue::Null, json!({"items": items}))
}

async fn list_unique_names(state: &BridgeState) -> Result<Vec<String>, String> {
    let reply = crate::dbus::bus_proxy::perform_call(
        state, "org.freedesktop.DBus", "/org/freedesktop/DBus", "org.freedesktop.DBus", "ListNames", vec![],
    )
    .await
    .map_err(|e| e.to_string())?;
    let values = crate::dbus::dispatch::extract_arg_values(&reply).map_err(|e| e.to_string())?;
    let names = values.into_iter().next().and_then(|v| v.as_array().cloned()).unwrap_or_default();
    Ok(names.into_iter().filter_map(|v| v.as_str().map(String::from)).filter(|n| n.starts_with(':')).collect())
}

async fn introspect_xml(state: &BridgeState, destination: &str, path: &str) -> Option<String> {
    let reply = crate::dbus::bus_proxy::perform_call(
        state, destination, path, "org.freedesktop.DBus.Introspectable", "Introspect", vec![],
    )
    .await
    .ok()?;
    let values = crate::dbus::dispatch::extract_arg_values(&reply).ok()?;
    values.into_iter().next().and_then(|v| v.as_str().map(String::from))
}

/// The actual walk: breadth-first from `/`, bounded by
/// `FIND_OBJECTS_MAX_NODES_PER_NAME`/`FIND_OBJECTS_MAX_DEPTH`, returning
/// the first matching object path found. A failed introspect on any one
/// node just prunes that branch - it's completely unremarkable (most
/// objects on a real app's bus connection aren't the one being searched
/// for, and objects come and go) and must never abandon the whole search
/// for this bus name, only the one branch that failed.
async fn find_object_implementing(state: &BridgeState, bus_name: &str, interface: &str) -> Option<String> {
    let mut visited = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<(String, u32)> = std::collections::VecDeque::new();
    queue.push_back(("/".to_string(), 0));
    let mut budget = FIND_OBJECTS_MAX_NODES_PER_NAME;

    while let Some((path, depth)) = queue.pop_front() {
        if !visited.insert(path.clone()) {
            continue;
        }
        if budget == 0 {
            break;
        }
        budget -= 1;

        let Some(xml) = introspect_xml(state, bus_name, &path).await else {
            continue;
        };
        if crate::dbus::introspect::root_implements_interface(&xml, interface) {
            return Some(path);
        }
        if depth >= FIND_OBJECTS_MAX_DEPTH {
            continue;
        }
        for child in crate::dbus::introspect::child_node_names(&xml) {
            let child_path = if path == "/" { format!("/{child}") } else { format!("{path}/{child}") };
            queue.push_back((child_path, depth + 1));
        }
    }
    None
}
