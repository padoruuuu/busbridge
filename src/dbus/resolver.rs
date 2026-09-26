//! The standard Varlink resolver protocol (`org.varlink.resolver`, see
//! https://varlink.org/Resolver), implemented inside this same daemon
//! process, on one well-known socket. This is the piece that lets a
//! Varlink client find *any* interface - one this bridge translates to
//! D-Bus, or a completely unrelated native Varlink service someone else
//! is running - without needing to know a specific per-service socket
//! path ahead of time. Combined with `dbus/bus_proxy.rs`, this is what
//! makes "an app only needs to speak Varlink" concretely true: it just
//! resolves an interface name against one fixed, standard address and
//! goes from there.
//!
//! ## Wire protocol on the shared socket
//!
//! Every connection is routed by method name:
//!
//!   - `org.varlink.resolver.Resolve` (the real, standard interface -
//!     see link above): `{"interface": "org.example.Foo"} -> {"address": "..."}`,
//!     or error `org.varlink.resolver.InterfaceNotFound {"interface": ...}`.
//!
//!     - If `interface` matches an entry in the file-based native
//!       registry (`registry_dir()`, one file per interface, filename =
//!       interface name, contents = that service's own real Varlink
//!       address), that address is returned and the CLIENT MUST
//!       DISCONNECT and connect there directly - this daemon never
//!       proxies native Varlink traffic (kept fully decoupled: the
//!       resolver's only job is returning a string). Stale entries
//!       (registered but nothing is actually listening any more) are
//!       detected on lookup and treated as not-found, self-healing by
//!       deleting the stale registry file.
//!
//!     - If `interface` matches a `[[resolve]]` entry (config/schema.rs's
//!       `ResolveConfig`, standalone - see that struct's doc comment),
//!       this daemon's OWN address is returned instead - meaning the
//!       client should keep using the SAME connection for its next call.
//!
//!   - Anything else, of the form `"{interface}.{method}"`: only valid
//!     immediately after resolving that exact interface to this daemon's
//!     own address (though not strictly enforced - a client that already
//!     knows the interface it wants can skip the Resolve round trip).
//!     Parameters are a plain JSON object of NAMED arguments (Varlink's
//!     own native convention, unlike `bus_proxy.rs`'s positional-array
//!     `Call`) - auto-introspected against the real D-Bus target
//!     (`[service].name`/`object_path`) for real argument names/types,
//!     falling back to inference if introspection doesn't cover it. The
//!     reply is likewise a named JSON object of the method's real
//!     out-args when known.
//!
//! ## Config
//!
//! A dedicated, standalone `[[resolve]]` table (config/schema.rs's
//! `ResolveConfig`) - NOT nested under `[service]`, and never tied to
//! name ownership (see that struct's own doc comment for why: the
//! destination it resolves to is very often owned by some other,
//! unrelated process entirely). Reads `state.dispatch` fresh on every
//! request, so it stays current across hot-reload automatically, same as
//! everything else in this crate.
//!
//! The native registry is deliberately NOT something this daemon writes
//! to - "decoupled" per its design brief: other Varlink services manage
//! their own registry files directly (e.g. on startup, write
//! `registry_dir()/<their-interface-name>` containing their own address;
//! on clean shutdown, remove it - though even an unclean exit self-heals
//! via stale-detection on the next lookup).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tracing::{info, warn};

use crate::dbus::bus_proxy::{introspect_method, perform_call, reply_dbus_error, reply_err};
use crate::dbus::introspect::MethodDesc;
use crate::dbus::BridgeState;
use crate::varlink::{read_framed, service_get_info, write_framed, VarlinkReply, VarlinkRequest};

/// The single, well-known socket every Varlink client on the system can
/// rely on finding, the same way `$DBUS_SESSION_BUS_ADDRESS` is the one
/// well-known thing a D-Bus client needs to know. Override via
/// `BUSBRIDGE_RESOLVER_SOCKET` (tests use this to avoid needing
/// root / colliding with a real system instance).
pub fn unified_socket_path() -> PathBuf {
    std::env::var_os("BUSBRIDGE_RESOLVER_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/run/varlink/varlink.sock"))
}

/// Where native (non-D-Bus-backed) Varlink services register themselves.
/// Override via `BUSBRIDGE_REGISTRY_DIR`.
pub fn registry_dir() -> PathBuf {
    std::env::var_os("BUSBRIDGE_REGISTRY_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/run/varlink/registry"))
}

/// Starts the resolver's accept loop. Takes every running `BridgeState`
/// (one per bus this process is connected to) since a resolvable
/// interface could be configured on either.
pub async fn start_listener(states: Vec<Arc<BridgeState>>) {
    let socket_path = unified_socket_path();
    // Best-effort: make sure native services have somewhere to register
    // even if nothing else has provisioned `registry_dir()` yet. Not
    // required for the resolver itself to function (a missing directory
    // just means `lookup_native_registry` finds nothing, same as an
    // empty one) - this only helps other processes get started cleanly.
    let _ = std::fs::create_dir_all(registry_dir());

    let listener = match crate::varlink::server::bind_fresh(&format!("unix:{}", socket_path.display())) {
        Ok(l) => l,
        Err(e) => {
            warn!(socket = %socket_path.display(), error = %e, "failed to bind the resolver socket, resolver disabled");
            return;
        }
    };
    info!(socket = %socket_path.display(), "varlink resolver ready");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let states = states.clone();
                tokio::spawn(async move {
                    handle_connection(states, stream).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "resolver accept() failed, stopping resolver");
                break;
            }
        }
    }
}

async fn handle_connection(states: Vec<Arc<BridgeState>>, stream: tokio::net::UnixStream) {
    let (r, w): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) = {
        let (r, w) = tokio::io::split(stream);
        (Box::new(r), Box::new(w))
    };
    let mut reader = BufReader::new(r);
    let mut writer = w;

    loop {
        let req: Option<VarlinkRequest> = match read_framed(&mut reader).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "resolver connection read error");
                break;
            }
        };
        let Some(req) = req else { break };
        for state in &states {
            state.activity.touch();
        }
        dispatch_request(&states, &mut writer, req).await;
    }
}

/// Starts the resolver listener the same way `start_listener` does, but
/// against `docs/REGISTRATION_PROTOCOL.md`'s `BusRegistry` instead of a
/// fixed `Vec<Arc<BridgeState>>` snapshotted once at startup - so a
/// service registered (or hot-reload-added) on a bus that had no
/// connection at all yet when this process started is still resolvable,
/// not just services that existed at the moment this function was
/// called. `start_listener` itself is unchanged and still used by
/// existing callers (`tests/resolver.rs`) that only ever need a fixed
/// set of buses; this is a separate entry point rather than a change to
/// that one specifically so nothing that already depends on its exact
/// signature needs to change.
pub async fn start_registry_aware_listener(registry: Arc<crate::registration::BusRegistry>) {
    let socket_path = unified_socket_path();
    let _ = std::fs::create_dir_all(registry_dir());

    let listener = match crate::varlink::server::bind_fresh(&format!("unix:{}", socket_path.display())) {
        Ok(l) => l,
        Err(e) => {
            warn!(socket = %socket_path.display(), error = %e, "failed to bind the resolver socket, resolver disabled");
            return;
        }
    };
    info!(socket = %socket_path.display(), "varlink resolver ready");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let registry = registry.clone();
                tokio::spawn(async move {
                    handle_connection_registry_aware(registry, stream).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "resolver accept() failed, stopping resolver");
                break;
            }
        }
    }
}

async fn handle_connection_registry_aware(registry: Arc<crate::registration::BusRegistry>, stream: tokio::net::UnixStream) {
    let (r, w): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) = {
        let (r, w) = tokio::io::split(stream);
        (Box::new(r), Box::new(w))
    };
    let mut reader = BufReader::new(r);
    let mut writer = w;

    loop {
        let req: Option<VarlinkRequest> = match read_framed(&mut reader).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "resolver connection read error");
                break;
            }
        };
        let Some(req) = req else { break };
        // Re-snapshotted on every request (not once per connection) since
        // a resolver connection can be long-lived (a client that resolves
        // more than one interface over time) and a bus can start existing
        // at any point in this process's life - see this function's
        // caller's doc comment.
        let states = registry.snapshot().await;
        for state in &states {
            state.activity.touch();
        }
        dispatch_request(&states, &mut writer, req).await;
    }
}

async fn dispatch_request<W: AsyncWrite + Unpin>(states: &[Arc<BridgeState>], writer: &mut W, req: VarlinkRequest) {
    match req.method.as_str() {
        "org.varlink.service.GetInfo" => {
            let _ = write_framed(writer, &VarlinkReply::ok(service_get_info())).await;
        }
        "org.varlink.resolver.Resolve" => {
            handle_resolve(states, writer, req.parameters).await;
        }
        other => {
            handle_direct_call(states, writer, other, req.parameters).await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct ResolveParams {
    interface: String,
}

async fn handle_resolve<W: AsyncWrite + Unpin>(states: &[Arc<BridgeState>], writer: &mut W, params: Option<JsonValue>) {
    let interface = match params.and_then(|p| serde_json::from_value::<ResolveParams>(p).ok()) {
        Some(p) => p.interface,
        None => return reply_err(writer, "org.varlink.resolver.InvalidParameters", "missing 'interface'").await,
    };

    if let Some(address) = lookup_native_registry(&interface) {
        let _ = write_framed(writer, &VarlinkReply::ok(json!({ "address": address }))).await;
        return;
    }

    if find_resolvable_service(states, &interface).await.is_some() {
        let address = format!("unix:{}", unified_socket_path().display());
        let _ = write_framed(writer, &VarlinkReply::ok(json!({ "address": address }))).await;
        return;
    }

    let _ = write_framed(
        writer,
        &VarlinkReply::error("org.varlink.resolver.InterfaceNotFound", Some(json!({"interface": interface}))),
    )
    .await;
}

/// The bus a resolved target lives on, and where to actually call it.
struct ResolvedTarget<'a> {
    state: &'a Arc<BridgeState>,
    destination: String,
    path: String,
}

async fn find_resolvable_service<'a>(states: &'a [Arc<BridgeState>], interface: &str) -> Option<ResolvedTarget<'a>> {
    for state in states {
        let table = state.dispatch.read().await;
        if let Some(entry) = table.resolvables.get(interface) {
            return Some(ResolvedTarget {
                state,
                destination: entry.destination.clone(),
                path: entry.path.clone(),
            });
        }
    }
    None
}

/// Look up `interface` in the file-based native registry, verifying the
/// registered address is actually alive (a stale entry - registered but
/// nothing listening any more, e.g. after an unclean exit - is treated as
/// not-found, and the stale file is removed so the registry self-heals).
fn lookup_native_registry(interface: &str) -> Option<String> {
    lookup_native_registry_in(interface, &registry_dir())
}

/// Pure core of `lookup_native_registry`, factored out so it can be
/// unit-tested against an explicit, isolated directory rather than the
/// process-global `BUSBRIDGE_REGISTRY_DIR` env var - which (like
/// `DBUS_SESSION_BUS_ADDRESS` and `BUSBRIDGE_INTERFACE_XML_DIRS`
/// elsewhere in this crate's tests) is unsafe for concurrently-running
/// tests to each set to a different value; `cargo test` runs `#[test]`
/// functions within a binary on real, concurrent OS threads by default.
fn lookup_native_registry_in(interface: &str, dir: &Path) -> Option<String> {
    // Reject path traversal / separators outright rather than letting an
    // interface name escape `dir` - defense in depth, since this value
    // comes straight from an untrusted Varlink caller.
    if interface.contains('/') || interface.contains("..") {
        return None;
    }
    let path = dir.join(interface);
    let contents = std::fs::read_to_string(&path).ok()?;
    let address = contents.trim().to_string();
    if address.is_empty() {
        return None;
    }
    if socket_seems_alive(&address) {
        Some(address)
    } else {
        let _ = std::fs::remove_file(&path);
        None
    }
}

/// Best-effort liveness check for a registered address. Only meaningful
/// for `unix:` addresses (an `exec:` backend has no persistent state to
/// go stale - it's spawned fresh per connection, same as elsewhere in
/// this crate - so it's always treated as "alive"). Uses a raw
/// (non-async, but effectively instant for a local socket) connect
/// attempt: either something is listening, or the connect fails
/// immediately - there's no real hang risk to guard against here.
fn socket_seems_alive(address: &str) -> bool {
    let Some(path) = address.strip_prefix("unix:") else {
        return true;
    };
    std::os::unix::net::UnixStream::connect(Path::new(path)).is_ok()
}

async fn handle_direct_call<W: AsyncWrite + Unpin>(
    states: &[Arc<BridgeState>],
    writer: &mut W,
    method: &str,
    params: Option<JsonValue>,
) {
    let Some((interface, member)) = method.rsplit_once('.') else {
        return reply_err(writer, "org.varlink.service.MethodNotFound", format!("not a valid method name: {method}")).await;
    };

    let Some(target) = find_resolvable_service(states, interface).await else {
        return reply_err(
            writer,
            "org.varlink.resolver.InterfaceNotFound",
            format!("{interface} is not a resolvable interface on this daemon"),
        )
        .await;
    };
    let state = target.state;

    let method_desc = introspect_method(&state.connection, &target.destination, &target.path, interface, member).await;

    let fields = match named_params_to_dbus_fields(method_desc.as_ref(), &params.unwrap_or(JsonValue::Null)) {
        Ok(f) => f,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };

    match perform_call(state, &target.destination, &target.path, interface, member, fields).await {
        Ok(reply_msg) => match crate::dbus::dispatch::extract_arg_values(&reply_msg) {
            Ok(values) => {
                let payload = dbus_out_values_to_named_json(method_desc.as_ref(), &values);
                let _ = write_framed(writer, &VarlinkReply::ok(payload)).await;
            }
            Err(e) => reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
        },
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

/// Convert a JSON object of named parameters into ordered D-Bus `Value`s
/// using a method's real declared in-args (name -> type, in declared
/// order) - the inverse of dispatch.rs's `zip_named_args`. Falls back to
/// per-field type inference when introspection didn't cover this method,
/// same trade-off `passthrough` mode already makes elsewhere.
fn named_params_to_dbus_fields(
    desc: Option<&MethodDesc>,
    params: &JsonValue,
) -> Result<Vec<zvariant::Value<'static>>, String> {
    match desc {
        Some(m) if !m.in_args.is_empty() => {
            let obj = params
                .as_object()
                .ok_or("expected named parameters (a JSON object)")?;
            let mut fields = Vec::new();
            for (i, arg) in m.in_args.iter().enumerate() {
                let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
                let json_val = obj.get(&key).cloned().unwrap_or(JsonValue::Null);
                let ty = crate::convert::parse_single_complete_type(&arg.type_sig).map_err(|e| e.to_string())?;
                fields.push(crate::convert::json_to_dbus(&json_val, &ty).map_err(|e| e.to_string())?);
            }
            Ok(fields)
        }
        _ => match params {
            JsonValue::Null => Ok(Vec::new()),
            JsonValue::Object(obj) => obj
                .values()
                .map(|v| crate::convert::json_to_dbus_inferred(v).map_err(|e| e.to_string()))
                .collect(),
            _ => Err("expected named parameters (a JSON object)".to_string()),
        },
    }
}

/// Shape a reply's positional D-Bus out-values back into a named JSON
/// object using the method's real declared out-args when known, falling
/// back to generic `arg0`/`arg1` keys otherwise.
fn dbus_out_values_to_named_json(desc: Option<&MethodDesc>, values: &[JsonValue]) -> JsonValue {
    let named = match desc {
        Some(m) if m.out_args.len() == values.len() && !m.out_args.is_empty() => Some(
            m.out_args
                .iter()
                .enumerate()
                .map(|(i, arg)| arg.name.clone().unwrap_or_else(|| format!("arg{i}")))
                .collect::<Vec<_>>(),
        ),
        _ => None,
    };
    let keys = named.unwrap_or_else(|| (0..values.len()).map(|i| format!("arg{i}")).collect());
    let mut obj = serde_json::Map::new();
    for (key, value) in keys.into_iter().zip(values.iter()) {
        obj.insert(key, value.clone());
    }
    JsonValue::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_native_registry_rejects_path_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(lookup_native_registry_in("../../etc/passwd", tmp.path()).is_none());
        assert!(lookup_native_registry_in("org.example/Foo", tmp.path()).is_none());
    }

    #[test]
    fn lookup_native_registry_returns_none_for_missing_entry() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(lookup_native_registry_in("org.example.DoesNotExist", tmp.path()).is_none());
    }

    #[test]
    fn lookup_native_registry_detects_and_heals_stale_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let dead_sock = tmp.path().join("dead.sock");
        let entry_path = tmp.path().join("org.example.Stale");
        std::fs::write(&entry_path, format!("unix:{}", dead_sock.display())).unwrap();

        assert!(lookup_native_registry_in("org.example.Stale", tmp.path()).is_none());
        assert!(!entry_path.exists(), "stale entry should have been removed");
    }

    #[tokio::test]
    async fn lookup_native_registry_finds_a_live_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("live.sock");
        let _listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let address = format!("unix:{}", sock_path.display());
        std::fs::write(tmp.path().join("org.example.Live"), &address).unwrap();

        assert_eq!(lookup_native_registry_in("org.example.Live", tmp.path()), Some(address));
    }

    #[test]
    fn exec_addresses_are_never_treated_as_stale() {
        assert!(socket_seems_alive("exec:/usr/bin/does-not-need-to-exist-for-this-check"));
    }

    #[test]
    fn named_params_to_dbus_fields_uses_introspection_when_available() {
        use crate::dbus::introspect::ArgDesc;
        let desc = MethodDesc {
            in_args: vec![
                ArgDesc { name: Some("name".into()), type_sig: "s".into() },
                ArgDesc { name: Some("count".into()), type_sig: "u".into() },
            ],
            out_args: vec![],
        };
        let params = json!({"count": 3, "name": "hi"});
        let fields = named_params_to_dbus_fields(Some(&desc), &params).unwrap();
        assert_eq!(fields, vec![zvariant::Value::Str("hi".into()), zvariant::Value::U32(3)]);
    }

    #[test]
    fn named_params_to_dbus_fields_infers_without_introspection() {
        let params = json!({"greeting": "hi"});
        let fields = named_params_to_dbus_fields(None, &params).unwrap();
        assert_eq!(fields, vec![zvariant::Value::Str("hi".into())]);
    }

    #[test]
    fn dbus_out_values_to_named_json_uses_real_names() {
        use crate::dbus::introspect::ArgDesc;
        let desc = MethodDesc {
            in_args: vec![],
            out_args: vec![
                ArgDesc { name: Some("greeting".into()), type_sig: "s".into() },
                ArgDesc { name: Some("ok".into()), type_sig: "b".into() },
            ],
        };
        let values = vec![json!("hi"), json!(true)];
        let named = dbus_out_values_to_named_json(Some(&desc), &values);
        assert_eq!(named, json!({"greeting": "hi", "ok": true}));
    }

    #[test]
    fn dbus_out_values_to_named_json_falls_back_to_generic_keys() {
        let values = vec![json!("hi"), json!(true)];
        let named = dbus_out_values_to_named_json(None, &values);
        assert_eq!(named, json!({"arg0": "hi", "arg1": true}));
    }
}
