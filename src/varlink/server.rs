//! Inbound Varlink listener(s): the thing backends connect *to* when they
//! have an event to push. docs/DESIGN_BRIEF_V1.md Section 2.2.
//!
//! One listener per service that configures `varlink.listen`. Startup
//! order per service: try an inherited fd (activation.rs) first, fall
//! back to binding the configured path fresh. Every accepted connection
//! either turns out to be a plain event-push connection (this module, for
//! the lifetime of the connection) or a `registrable` interface's
//! registration handshake, in which case it's handed off to
//! dbus/dynamic_object.rs for the rest of its life.
//!
//! Convention (schema doesn't specify a wire-level distinction, so - state
//! the assumption, proceed): a pushed event's Varlink `method` field is
//! `"{dbus_interface}.{member}"`, where `member` is either a configured
//! `[[signal]]` name (direction = inbound_push) or a `[[property]]` name
//! (interpreted as a `PropertiesChanged` notification for that one
//! property). This is the same convention dynamic_object.rs uses for its
//! own connections, just resolved against the *static* dispatch table
//! instead of one registrable interface.

use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tracing::{info, warn};
use zvariant::{Dict, Signature, StructureBuilder, Value};

use crate::activation;
use crate::config::LoadedService;
use crate::config::schema::SignalDirection;
use crate::dbus::dynamic_object;
use crate::dbus::BridgeState;
use crate::varlink::{
    read_framed, service_get_info, service_get_interface_description, write_framed, VarlinkReply,
    VarlinkRequest,
};
use crate::BoxError;

/// Start one accept loop per configured `varlink.listen` address, each as
/// its own background task. Returns once all listeners are bound (or have
/// failed to bind, which is logged and skipped rather than fatal - one
/// misconfigured service shouldn't take down every other service this
/// resident process serves).
pub async fn start_listeners(state: Arc<BridgeState>) {
    let specs: Vec<Arc<LoadedService>> = {
        let table = state.dispatch.read().await;
        table
            .services
            .values()
            .filter(|s| s.varlink.listen.is_some())
            .cloned()
            .collect()
    };

    for service in specs {
        let listen_addr = service.varlink.listen.clone().unwrap();
        let listener = if let Some(fd) = activation::claim_inherited_fd() {
            match listener_from_fd(fd) {
                Ok(l) => {
                    info!(bus_name = %service.name, fd, "varlink listener using inherited fd");
                    l
                }
                Err(e) => {
                    warn!(bus_name = %service.name, fd, error = %e, "inherited fd invalid, binding fresh instead");
                    match bind_fresh(&listen_addr) {
                        Ok(l) => l,
                        Err(e) => {
                            warn!(bus_name = %service.name, listen = %listen_addr, error = %e, "failed to bind varlink listener, skipping this service");
                            continue;
                        }
                    }
                }
            }
        } else {
            match bind_fresh(&listen_addr) {
                Ok(l) => l,
                Err(e) => {
                    warn!(bus_name = %service.name, listen = %listen_addr, error = %e, "failed to bind varlink listener, skipping this service");
                    continue;
                }
            }
        };

        info!(bus_name = %service.name, listen = %listen_addr, "varlink listener ready");
        let state = state.clone();
        tokio::spawn(async move {
            accept_loop(state, service, listener).await;
        });
    }
}

pub(crate) fn listener_from_fd(fd: std::os::unix::io::RawFd) -> std::io::Result<tokio::net::UnixListener> {
    use std::os::unix::io::FromRawFd;
    // Safety: `fd` came from activation::inherited_fds(), which only
    // returns fds when LISTEN_PID matched our own pid - i.e. this process
    // was specifically handed these fds by whatever exec'd us.
    let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(fd) };
    std_listener.set_nonblocking(true)?;
    tokio::net::UnixListener::from_std(std_listener)
}

pub(crate) fn bind_fresh(addr: &str) -> Result<tokio::net::UnixListener, BoxError> {
    let path = addr.strip_prefix("unix:").unwrap_or(addr);
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Remove a stale socket left behind by a previous, uncleanly-terminated
    // run - otherwise bind() fails with EADDRINUSE forever.
    let _ = std::fs::remove_file(path);
    Ok(tokio::net::UnixListener::bind(path)?)
}

async fn accept_loop(state: Arc<BridgeState>, service: Arc<LoadedService>, listener: tokio::net::UnixListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                state.activity.touch();
                let state = state.clone();
                let service = service.clone();
                tokio::spawn(async move {
                    handle_connection(state, service, stream).await;
                });
            }
            Err(e) => {
                warn!(bus_name = %service.name, error = %e, "varlink accept() failed, stopping this listener");
                break;
            }
        }
    }
}

async fn handle_connection(state: Arc<BridgeState>, service: Arc<LoadedService>, stream: tokio::net::UnixStream) {
    let (r, w): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>) = {
        let (r, w) = tokio::io::split(stream);
        (Box::new(r), Box::new(w))
    };
    let mut reader = BufReader::new(r);
    let mut writer = w;

    let first: Option<VarlinkRequest> = match read_framed(&mut reader).await {
        Ok(v) => v,
        Err(e) => {
            warn!(bus_name = %service.name, error = %e, "failed reading first frame from varlink connection");
            return;
        }
    };
    let Some(first) = first else {
        return; // connected and immediately disconnected - nothing to do.
    };
    state.activity.touch();

    if first.method == "org.varlink.service.GetInfo" {
        let _ = write_framed(&mut writer, &VarlinkReply::ok(service_get_info())).await;
        return;
    }
    if first.method == "org.varlink.service.GetInterfaceDescription" {
        let iface = first
            .parameters
            .as_ref()
            .and_then(|p| p.get("interface"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let reply = match service_get_interface_description(iface) {
            Ok(desc) => VarlinkReply::ok(desc),
            Err(_) => VarlinkReply::error(
                "org.varlink.service.InterfaceNotFound",
                Some(json!({"interface": iface})),
            ),
        };
        let _ = write_framed(&mut writer, &reply).await;
        return;
    }

    let matched_registrable = {
        let table = state.dispatch.read().await;
        table
            .registrables
            .values()
            .find(|r| r.service.name == service.name && r.config.register_via == first.method)
            .map(|r| r.config.clone())
    };

    if let Some(registrable) = matched_registrable {
        let app_id = first
            .parameters
            .as_ref()
            .and_then(|p| p.get("id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        match dynamic_object::register(
            state.clone(),
            service.clone(),
            registrable,
            app_id,
            None,
            reader,
            writer,
        )
        .await
        {
            Ok(object_path) => {
                info!(bus_name = %service.name, object_path = %object_path, "dynamic object registered");
            }
            Err(e) => {
                warn!(bus_name = %service.name, error = %e, "dynamic object registration failed");
            }
        }
        return;
    }

    // Not a registration and not a service-introspection query: treat this
    // connection as a plain inbound-push event source for its whole
    // lifetime (docs/DESIGN_BRIEF_V1.md Section 2.2).
    handle_push_request(&state, &service, first).await;
    loop {
        let req: Option<VarlinkRequest> = match read_framed(&mut reader).await {
            Ok(v) => v,
            Err(e) => {
                warn!(bus_name = %service.name, error = %e, "error reading push connection");
                break;
            }
        };
        let Some(req) = req else { break };
        state.activity.touch();
        handle_push_request(&state, &service, req).await;
    }
}

pub(crate) async fn handle_push_request(state: &BridgeState, service: &LoadedService, req: VarlinkRequest) {
    let Some((interface, member)) = req.method.rsplit_once('.') else {
        warn!(method = %req.method, "pushed event method isn't of the form interface.member, ignoring");
        return;
    };
    let (interface, member) = (interface.to_string(), member.to_string());

    let is_signal = {
        let table = state.dispatch.read().await;
        table
            .signals
            .get(&(service.name.clone(), interface.clone(), member.clone()))
            .map(|entry| matches!(entry.mapping.direction, SignalDirection::InboundPush))
    };
    let is_property = {
        let table = state.dispatch.read().await;
        table
            .properties
            .contains_key(&(service.name.clone(), interface.clone(), member.clone()))
    };

    match is_signal {
        Some(true) => {
            emit_signal_from_push(state, service, &interface, &member, req.parameters).await;
        }
        _ if is_property => {
            emit_properties_changed_from_push(state, service, &interface, &member, req.parameters).await;
        }
        _ if service.passthrough => {
            emit_signal_from_push_generic(state, service, &interface, &member, req.parameters).await;
        }
        _ => {
            warn!(bus_name = %service.name, interface, member, "pushed event doesn't match a configured inbound_push signal or property, ignoring");
        }
    }
}

/// `passthrough` mode's answer to `emit_signal_from_push`: no introspection
/// XML to read declared signal-arg names/types from, so the entire pushed
/// `parameters` object (if any) becomes a single D-Bus signal argument
/// whose type is inferred from its JSON shape (typically `a{sv}` for an
/// object) - see config/schema.rs's `passthrough` doc comment.
/// `passthrough` mode's answer to `emit_signal_from_push`: no introspection
/// XML to read declared signal-arg names/types from ourselves, but a real,
/// already-installed system interface XML might declare this exact signal
/// (introspect::find_system_interface_desc) - if so, use its real arg
/// shape (same as the non-passthrough path). Only when nothing can be
/// found does the whole pushed `parameters` object become a single D-Bus
/// signal argument with its type inferred from its JSON shape - see
/// config/schema.rs's `passthrough` doc comment.
async fn emit_signal_from_push_generic(
    state: &BridgeState,
    service: &LoadedService,
    interface: &str,
    member: &str,
    parameters: Option<serde_json::Value>,
) {
    let discovered_args = crate::dbus::introspect::find_system_interface_desc(interface)
        .and_then(|desc| desc.signals.get(member).map(|s| s.args.clone()));

    if let Some(arg_descs) = discovered_args {
        emit_signal_with_arg_descs(state, service, interface, member, &arg_descs, parameters).await;
        return;
    }

    let result = match parameters {
        None => {
            state
                .connection
                .emit_signal(Option::<&str>::None, service.object_path.as_str(), interface, member, &())
                .await
        }
        Some(params) => match crate::convert::json_to_dbus_inferred(&params) {
            Ok(value) => {
                let mut builder = StructureBuilder::new();
                builder = builder.append_field(value);
                state
                    .connection
                    .emit_signal(
                        Option::<&str>::None,
                        service.object_path.as_str(),
                        interface,
                        member,
                        &builder.build(),
                    )
                    .await
            }
            Err(e) => {
                warn!(error = %e, "passthrough: failed to infer a D-Bus type for pushed event parameters, dropping event");
                return;
            }
        },
    };
    if let Err(e) = result {
        warn!(error = %e, "failed to emit D-Bus signal from passthrough-mode pushed event");
    }
    state.activity.touch();
}

async fn emit_signal_from_push(
    state: &BridgeState,
    service: &LoadedService,
    interface: &str,
    member: &str,
    parameters: Option<serde_json::Value>,
) {
    let arg_descs = {
        let introspection = state.introspection.read().await;
        introspection
            .get(&service.name)
            .and_then(|ifaces| ifaces.get(interface))
            .and_then(|desc| desc.signals.get(member))
            .map(|s| s.args.clone())
    };
    let Some(arg_descs) = arg_descs else {
        warn!(bus_name = %service.name, interface, member, "no introspection entry for pushed signal, cannot type-convert, ignoring");
        return;
    };
    emit_signal_with_arg_descs(state, service, interface, member, &arg_descs, parameters).await;
}

/// Shared by `emit_signal_from_push` (explicit config's own introspection
/// XML) and `emit_signal_from_push_generic` (passthrough's system-XML
/// discovery): given a real signal arg-name/type list from wherever it
/// came from, convert the pushed JSON params and emit the signal.
async fn emit_signal_with_arg_descs(
    state: &BridgeState,
    service: &LoadedService,
    interface: &str,
    member: &str,
    arg_descs: &[crate::dbus::introspect::ArgDesc],
    parameters: Option<serde_json::Value>,
) {
    let mut values = Vec::new();
    if let Some(params) = &parameters {
        for (i, arg) in arg_descs.iter().enumerate() {
            let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
            let field = params.get(&key).cloned().unwrap_or(serde_json::Value::Null);
            match crate::convert::parse_single_complete_type(&arg.type_sig)
                .map_err(BoxError::from)
                .and_then(|ty| crate::convert::json_to_dbus(&field, &ty).map_err(BoxError::from))
            {
                Ok(v) => values.push(v),
                Err(e) => {
                    warn!(error = %e, "failed to convert pushed signal field, dropping event");
                    return;
                }
            }
        }
    }

    let result = if values.is_empty() {
        state
            .connection
            .emit_signal(Option::<&str>::None, service.object_path.as_str(), interface, member, &())
            .await
    } else {
        let mut builder = StructureBuilder::new();
        for v in values {
            builder = builder.append_field(v);
        }
        state
            .connection
            .emit_signal(
                Option::<&str>::None,
                service.object_path.as_str(),
                interface,
                member,
                &builder.build(),
            )
            .await
    };
    if let Err(e) = result {
        warn!(error = %e, "failed to emit D-Bus signal from pushed event");
    }
    state.activity.touch();
}

async fn emit_properties_changed_from_push(
    state: &BridgeState,
    service: &LoadedService,
    interface: &str,
    property: &str,
    parameters: Option<serde_json::Value>,
) {
    let type_sig = {
        let introspection = state.introspection.read().await;
        introspection
            .get(&service.name)
            .and_then(|ifaces| ifaces.get(interface))
            .and_then(|desc| desc.properties.get(property))
            .map(|p| p.type_sig.clone())
    };
    let Some(type_sig) = type_sig else {
        warn!(bus_name = %service.name, interface, property, "no introspection entry for pushed property, cannot type-convert, ignoring");
        return;
    };

    let value_json = match &parameters {
        Some(serde_json::Value::Object(map)) if map.contains_key("value") => map["value"].clone(),
        Some(other) => other.clone(),
        None => serde_json::Value::Null,
    };

    let ty = match crate::convert::parse_single_complete_type(&type_sig) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "bad type signature for pushed property");
            return;
        }
    };
    let value = match crate::convert::json_to_dbus(&value_json, &ty) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "failed to convert pushed property value");
            return;
        }
    };

    let mut changed = Dict::new(
        Signature::from_static_str_unchecked("s"),
        Signature::from_static_str_unchecked("v"),
    );
    // Dict entries with declared val_sig "v" require the explicit
    // Value::Value(..) wrapper - see convert.rs's `DbusType::Variant` arm.
    let _ = changed.append(Value::Str(property.into()), Value::Value(Box::new(value)));

    let mut builder = StructureBuilder::new();
    builder = builder
        .append_field(Value::Str(interface.into()))
        .append_field(Value::Dict(changed))
        .append_field(Value::Array(zvariant::Array::new(Signature::from_static_str_unchecked("s"))));

    let result = state
        .connection
        .emit_signal(
            Option::<&str>::None,
            service.object_path.as_str(),
            "org.freedesktop.DBus.Properties",
            "PropertiesChanged",
            &builder.build(),
        )
        .await;
    if let Err(e) = result {
        warn!(error = %e, "failed to emit PropertiesChanged from pushed event");
    }
    state.activity.touch();
}
