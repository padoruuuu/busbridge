//! (interface, member) -> Mapping lookup and per-call task spawn.
//! docs/DESIGN_BRIEF_V1.md Section 2.3.
//!
//! Each inbound D-Bus call gets its own async task with its own timeout
//! against the target Varlink backend, so one slow/hung backend cannot
//! stall unrelated interfaces served by this same resident process. The
//! caller (`dbus::run_dispatch_loop`) already spawns a fresh task per
//! call; `handle_call` is what runs inside it.
//!
//! Convention notes (docs/DESIGN_BRIEF_V1.md Section 9 spirit: state assumptions,
//! proceed) for pieces the schema leaves underspecified:
//!   - `[[method]].args` names the JSON parameter for each positional
//!     D-Bus in-arg, in signature order (schema.rs). Output arg names/
//!     types come from the method's `introspection_xml` `out` args
//!     instead, since config doesn't restate them.
//!   - A single out-arg method whose Varlink reply isn't a JSON object
//!     containing that arg's name is treated as returning that value
//!     directly (matches Varlink's common "one logical return value"
//!     style without forcing every backend to nest it).
//!   - `[[property]].varlink_method` is used both to fetch (Get/GetAll)
//!     and, for Set, is called again with `{"value": <new value>}` -
//!     schema doesn't provide a separate setter method name.

use std::time::Duration;

use tracing::warn;
use zbus::Message;
use zvariant::{StructureBuilder, Value};

use crate::config::{DispatchTable, MethodEntry, PropertyEntry};
use crate::convert::{self, DbusType};
use crate::dbus::introspect::{ArgDesc, InterfaceDesc};
use crate::dbus::BridgeState;
use crate::errors::{self, VarlinkError};
use crate::varlink::client::{CallError, VarlinkConnection};
use crate::BoxError;

use std::sync::Arc;

const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Route a Varlink call either through a registered service's live,
/// app-initiated connection (`peer` - see `registration.rs`,
/// `varlink/peer.rs`) or, as always, by dialing `backend_addr` fresh.
/// This is the *only* thing that differs between a conf.d-loaded service
/// and one created through the app registration protocol: everything
/// above and below this call in every caller below (arg conversion,
/// timeout wrapping, reply/error shaping) is identical either way, which
/// is exactly the point - registered services get real Introspect,
/// Properties, static-method, and passthrough handling for free by
/// flowing through the same dispatch code conf.d services already use,
/// with only the transport swapped out here.
async fn call_backend(
    peer: Option<&Arc<crate::varlink::peer::PeerConnection>>,
    backend_addr: &str,
    varlink_method: &str,
    parameters: Option<serde_json::Value>,
) -> Result<serde_json::Value, CallError> {
    match peer {
        Some(peer) => peer.call(varlink_method, parameters).await,
        None => {
            let mut conn = VarlinkConnection::connect(backend_addr).await?;
            conn.call(varlink_method, parameters).await
        }
    }
}

pub async fn handle_call(state: Arc<BridgeState>, message: Message) {
    let _hold = state.activity.hold();
    state.activity.touch();

    let path = message.header().path().map(|p| p.to_string());
    let interface = message.header().interface().map(|i| i.to_string());
    let member = message.header().member().map(|m| m.to_string());

    let (Some(path), Some(interface), Some(member)) = (path, interface, member) else {
        // Malformed / not routable - nothing sensible to reply with.
        return;
    };

    // Dynamic objects (docs/DESIGN_BRIEF_V1.md Section 2.5) take priority: a call
    // destined for a synthetic per-app object path is proxied to that
    // object's specific live Varlink connection, not the static table.
    if let Some(obj) = state.dynamic_objects.lookup(&path).await {
        crate::dbus::dynamic_object::handle_dynamic_call(state.clone(), obj, message).await;
        return;
    }

    let bus_name = resolve_bus_name(&state, &message).await;

    if interface == "org.freedesktop.DBus.Introspectable" && member == "Introspect" {
        handle_introspect(&state, bus_name.as_deref(), &path, &message).await;
        return;
    }

    if interface == "org.freedesktop.DBus.Properties" {
        handle_properties_call(&state, bus_name.as_deref(), &member, &message).await;
        return;
    }

    let table = state.dispatch.read().await;
    let found = find_method(&table, bus_name.as_deref(), &interface, &member).cloned_refs();
    let Some((backend_addr, varlink_method, args_names, error_map, bus_name_resolved, peer)) = found else {
        // No explicit [[method]] mapping. If the service this call is
        // addressed to has `passthrough = true`, forward it generically
        // instead of failing - see config/schema.rs's `passthrough` doc
        // comment for exactly what "generically" means.
        let passthrough_service = resolve_service(&table, bus_name.as_deref(), &path)
            .filter(|s| s.passthrough && (s.varlink.backend.is_some() || s.peer.is_some()))
            .cloned();
        drop(table);
        match passthrough_service {
            Some(service) => {
                passthrough_forward_call(&state, &service, &interface, &member, &message).await;
            }
            None => {
                let _ = state
                    .connection
                    .reply_error(
                        &message,
                        "org.freedesktop.DBus.Error.UnknownMethod",
                        &format!("No mapping configured for {interface}.{member}"),
                    )
                    .await;
            }
        }
        return;
    };
    drop(table);

    state
        .telemetry
        .record_call(&bus_name_resolved, &interface, &member);

    let out_args = {
        let introspection = state.introspection.read().await;
        introspection
            .get(&bus_name_resolved)
            .and_then(|ifaces| ifaces.get(&interface))
            .and_then(|desc| desc.methods.get(&member))
            .map(|m| m.out_args.clone())
            .unwrap_or_default()
    };

    let arg_values = match extract_arg_values(&message) {
        Ok(v) => v,
        Err(e) => {
            let _ = state
                .connection
                .reply_error(&message, "org.freedesktop.DBus.Error.InvalidArgs", &e.to_string())
                .await;
            return;
        }
    };

    let mut params = serde_json::Map::new();
    for (name, value) in args_names.iter().zip(arg_values.iter()) {
        params.insert(name.clone(), value.clone());
    }
    insert_dbus_sender(&mut params, &message);
    let parameters = if params.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(params))
    };

    let call_result = tokio::time::timeout(
        DEFAULT_CALL_TIMEOUT,
        call_backend(peer.as_ref(), &backend_addr, &varlink_method, parameters),
    )
    .await;

    match call_result {
        Err(_elapsed) => {
            let _ = state
                .connection
                .reply_error(
                    &message,
                    "org.freedesktop.DBus.Error.Timeout",
                    &format!("varlink backend for {interface}.{member} did not respond in time"),
                )
                .await;
        }
        Ok(Err(CallError::Remote(varlink_err))) => {
            reply_varlink_error(&state, &message, varlink_err, &error_map).await;
        }
        Ok(Err(other)) => {
            let _ = state
                .connection
                .reply_error(&message, "org.freedesktop.DBus.Error.Failed", &other.to_string())
                .await;
        }
        Ok(Ok(reply_json)) => {
            if let Err(e) = reply_success(&state, &message, &reply_json, &out_args).await {
                warn!(error = %e, interface, member, "failed to build D-Bus reply from varlink response");
                let _ = state
                    .connection
                    .reply_error(&message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                    .await;
            }
        }
    }
}

fn resolve_service<'a>(
    table: &'a DispatchTable,
    bus_name: Option<&str>,
    path: &str,
) -> Option<&'a std::sync::Arc<crate::config::LoadedService>> {
    if let Some(bus_name) = bus_name {
        if let Some(service) = table.services.get(bus_name) {
            return Some(service);
        }
    }
    table.services.values().find(|s| s.object_path == path)
}

/// Forward a call that matched no explicit `[[method]]` entry, for a
/// service with `passthrough = true`. See config/schema.rs's
/// `ServiceConfig::passthrough` doc comment for the conventions used.
async fn passthrough_forward_call(
    state: &BridgeState,
    service: &crate::config::LoadedService,
    interface: &str,
    member: &str,
    message: &Message,
) {
    state.telemetry.record_call(&service.name, interface, member);

    // A registered service (service.peer.is_some() - see registration.rs)
    // has no varlink.backend to dial at all; its whole "backend" is the
    // live connection in `service.peer`. Only require a configured
    // backend address when there's no peer to fall back to.
    let backend_addr = match (&service.varlink.backend, &service.peer) {
        (Some(addr), _) => addr.clone(),
        (None, Some(_)) => String::new(),
        (None, None) => {
            let _ = state
                .connection
                .reply_error(
                    message,
                    "org.freedesktop.DBus.Error.Failed",
                    &"passthrough is enabled but [varlink].backend is not configured".to_string(),
                )
                .await;
            return;
        }
    };

    let arg_values = match extract_arg_values(message) {
        Ok(v) => v,
        Err(e) => {
            let _ = state
                .connection
                .reply_error(message, "org.freedesktop.DBus.Error.InvalidArgs", &e.to_string())
                .await;
            return;
        }
    };

    // Prefer a real, already-installed interface XML for this interface
    // name if one can be found (introspect::find_system_interface_desc) -
    // it gives us the method's true in/out arg names and types with zero
    // config, recovering named parameters and correctly-typed,
    // correctly-arity'd replies. Falls back to fully generic arg0/arg1
    // naming and inferred-from-JSON reply typing when no such XML exists.
    let method_desc = crate::dbus::introspect::find_system_interface_desc(interface)
        .and_then(|desc| desc.methods.get(member).cloned());

    let mut params = match &method_desc {
        Some(m) if !m.in_args.is_empty() => match zip_named_args(&m.in_args, &arg_values) {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        },
        _ => {
            let mut map = serde_json::Map::new();
            for (i, value) in arg_values.iter().enumerate() {
                map.insert(format!("arg{i}"), value.clone());
            }
            map
        }
    };
    insert_dbus_sender(&mut params, message);
    let parameters = if params.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(params))
    };

    let varlink_method = format!("{interface}.{member}");
    let call_result = tokio::time::timeout(
        DEFAULT_CALL_TIMEOUT,
        call_backend(service.peer.as_ref(), &backend_addr, &varlink_method, parameters),
    )
    .await;

    match call_result {
        Err(_elapsed) => {
            let _ = state
                .connection
                .reply_error(
                    message,
                    "org.freedesktop.DBus.Error.Timeout",
                    &format!("varlink backend for {interface}.{member} did not respond in time"),
                )
                .await;
        }
        Ok(Err(CallError::Remote(varlink_err))) => {
            reply_varlink_error(state, message, varlink_err, &std::collections::HashMap::new()).await;
        }
        Ok(Err(other)) => {
            let _ = state
                .connection
                .reply_error(message, "org.freedesktop.DBus.Error.Failed", &other.to_string())
                .await;
        }
        Ok(Ok(reply_json)) => {
            let build_result = match &method_desc {
                // A real discovered method description: build the exact
                // out-arg shape it declares, same as non-passthrough
                // dispatch does.
                Some(m) if !m.out_args.is_empty() => json_to_out_values(&reply_json, &m.out_args),
                // A discovered description that simply declares no
                // out-args at all (e.g. a fire-and-forget method).
                Some(_) if reply_json.is_null() => Ok(Vec::new()),
                // No discovered description (or it disagrees with a
                // non-null reply) - fall back to one inferred value.
                _ if reply_json.is_null() => Ok(Vec::new()),
                _ => convert::json_to_dbus_inferred(&reply_json)
                    .map(|v| vec![v])
                    .map_err(BoxError::from),
            };
            match build_result {
                Ok(values) if values.is_empty() => {
                    let _ = state.connection.reply(message, &()).await;
                }
                Ok(values) => {
                    let mut builder = StructureBuilder::new();
                    for v in values {
                        builder = builder.append_field(v);
                    }
                    let _ = state.connection.reply(message, &builder.build()).await;
                }
                Err(e) => {
                    let _ = state
                        .connection
                        .reply_error(message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                        .await;
                }
            }
        }
    }
}

async fn reply_varlink_error(
    state: &BridgeState,
    message: &Message,
    varlink_err: VarlinkError,
    error_map: &std::collections::HashMap<String, String>,
) {
    let dbus_err = errors::varlink_error_to_dbus(&varlink_err, error_map);
    let name = dbus_err
        .name
        .clone()
        .try_into()
        .unwrap_or_else(|_| zbus::names::ErrorName::from_static_str_unchecked("org.freedesktop.DBus.Error.Failed"));
    let _ = state
        .connection
        .reply_error(message, name, &dbus_err.message)
        .await;
}

async fn reply_success(
    state: &BridgeState,
    message: &Message,
    reply_json: &serde_json::Value,
    out_args: &[ArgDesc],
) -> Result<(), BoxError> {
    let mut values: Vec<Value<'static>> = Vec::new();
    if out_args.is_empty() {
        // No declared out-args: nothing to send back beyond an empty reply.
    } else if out_args.len() == 1 && !reply_json_has_key(reply_json, &out_args[0]) {
        let ty = convert::parse_single_complete_type(&out_args[0].type_sig)?;
        values.push(convert::json_to_dbus(reply_json, &ty)?);
    } else {
        for (i, arg) in out_args.iter().enumerate() {
            let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
            let field = reply_json
                .get(&key)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let ty = convert::parse_single_complete_type(&arg.type_sig)?;
            values.push(convert::json_to_dbus(&field, &ty)?);
        }
    }

    if values.is_empty() {
        state.connection.reply(message, &()).await?;
    } else {
        let mut builder = StructureBuilder::new();
        for v in values {
            builder = builder.append_field(v);
        }
        state.connection.reply(message, &builder.build()).await?;
    }
    Ok(())
}

/// Every inbound-call-forwarding path (static `[[method]]` mapping,
/// `passthrough` mode, and dynamic-object/registry forwarding) includes
/// the caller's D-Bus sender (their unique bus name, e.g. `:1.234`) in the
/// JSON parameters sent to the Varlink backend automatically, under the
/// reserved key `_dbus_sender` - no config needed to opt in. This is what
/// lets a backend reconstruct caller identity in the (real but uncommon)
/// case where the call's own declared arguments don't already carry it -
/// see docs/DESIGN_BRIEF_V1.md-adjacent discussion: most SNI-style registrations pass
/// the caller's own bus name as an explicit argument and don't need this,
/// but the spec allows an empty/relative argument, in which case the
/// sender is the only way to know who's calling.
///
/// Absent (key omitted entirely, not `null`) if the message has no
/// sender header, which practically shouldn't happen for method calls
/// arriving over a real bus connection, but isn't assumed.
pub(crate) fn insert_dbus_sender(params: &mut serde_json::Map<String, serde_json::Value>, message: &Message) {
    if let Some(sender) = message.header().sender() {
        params.insert("_dbus_sender".to_string(), serde_json::Value::String(sender.to_string()));
    }
}

fn reply_json_has_key(json: &serde_json::Value, arg: &ArgDesc) -> bool {
    match (&arg.name, json) {
        (Some(name), serde_json::Value::Object(map)) => map.contains_key(name),
        _ => false,
    }
}

/// Which of our configured well-known names this call was addressed to,
/// if determinable from the message's destination header. Falls back to
/// `None` (meaning: search across all configured services by
/// interface+member alone) when the caller addressed our unique name
/// directly rather than a well-known name.
async fn resolve_bus_name(state: &BridgeState, message: &Message) -> Option<String> {
    let dest = message.header().destination()?.to_string();
    let table = state.dispatch.read().await;
    if table.services.contains_key(&dest) {
        Some(dest)
    } else {
        None
    }
}

/// (backend_addr, varlink_method, arg_names, error_map, resolved_bus_name, peer)
type FoundMethod = (
    String,
    String,
    Vec<String>,
    std::collections::HashMap<String, String>,
    String,
    Option<Arc<crate::varlink::peer::PeerConnection>>,
);

trait ClonedRefs {
    fn cloned_refs(self) -> Option<FoundMethod>;
}

impl ClonedRefs for Option<&MethodEntry> {
    fn cloned_refs(self) -> Option<FoundMethod> {
        self.map(|entry| {
            (
                entry.service.varlink.backend.clone().unwrap_or_default(),
                entry.mapping.varlink_method.clone(),
                entry.mapping.args.clone(),
                entry.mapping.error_map.clone(),
                entry.service.name.clone(),
                entry.service.peer.clone(),
            )
        })
    }
}

fn find_method<'a>(
    table: &'a DispatchTable,
    bus_name: Option<&str>,
    interface: &str,
    member: &str,
) -> Option<&'a MethodEntry> {
    if let Some(bus_name) = bus_name {
        let key = (bus_name.to_string(), interface.to_string(), member.to_string());
        if let Some(entry) = table.methods.get(&key) {
            return Some(entry);
        }
    }
    table.methods.values().find(|entry| {
        entry.mapping.dbus_interface == interface && entry.mapping.dbus_method == member
    })
}

fn find_property<'a>(
    table: &'a DispatchTable,
    bus_name: Option<&str>,
    interface: &str,
    property: &str,
) -> Option<&'a PropertyEntry> {
    if let Some(bus_name) = bus_name {
        let key = (bus_name.to_string(), interface.to_string(), property.to_string());
        if let Some(entry) = table.properties.get(&key) {
            return Some(entry);
        }
    }
    table.properties.values().find(|entry| {
        entry.mapping.dbus_interface == interface && entry.mapping.dbus_property == property
    })
}

async fn handle_introspect(state: &BridgeState, bus_name: Option<&str>, path: &str, message: &Message) {
    let table = state.dispatch.read().await;
    let resolved = bus_name
        .map(|s| s.to_string())
        .or_else(|| {
            table
                .services
                .values()
                .find(|svc| svc.object_path == path)
                .map(|svc| svc.name.clone())
        });
    drop(table);

    let Some(bus_name) = resolved else {
        let _ = state
            .connection
            .reply_error(
                message,
                "org.freedesktop.DBus.Error.UnknownObject",
                &format!("no service configured for object path {path}"),
            )
            .await;
        return;
    };

    let introspection = state.introspection.read().await;
    let empty = std::collections::HashMap::new();
    let ifaces: &std::collections::HashMap<String, InterfaceDesc> =
        introspection.get(&bus_name).unwrap_or(&empty);

    let mut xml = String::from("<!DOCTYPE node PUBLIC \"-//freedesktop//DTD D-BUS Object Introspection 1.0//EN\"\n\"http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd\">\n<node>\n");
    xml.push_str("  <interface name=\"org.freedesktop.DBus.Introspectable\">\n    <method name=\"Introspect\"><arg name=\"xml_data\" type=\"s\" direction=\"out\"/></method>\n  </interface>\n");
    xml.push_str("  <interface name=\"org.freedesktop.DBus.Properties\">\n    <method name=\"Get\"><arg name=\"interface_name\" type=\"s\" direction=\"in\"/><arg name=\"property_name\" type=\"s\" direction=\"in\"/><arg name=\"value\" type=\"v\" direction=\"out\"/></method>\n    <method name=\"GetAll\"><arg name=\"interface_name\" type=\"s\" direction=\"in\"/><arg name=\"properties\" type=\"a{sv}\" direction=\"out\"/></method>\n    <method name=\"Set\"><arg name=\"interface_name\" type=\"s\" direction=\"in\"/><arg name=\"property_name\" type=\"s\" direction=\"in\"/><arg name=\"value\" type=\"v\" direction=\"in\"/></method>\n  </interface>\n");
    let mut names: Vec<_> = ifaces.keys().collect();
    names.sort();
    for name in names {
        xml.push_str(&ifaces[name].to_xml_fragment());
    }
    xml.push_str("</node>\n");

    let _ = state.connection.reply(message, &xml).await;
}

async fn handle_properties_call(state: &BridgeState, bus_name: Option<&str>, member: &str, message: &Message) {
    let path = message.header().path().map(|p| p.to_string()).unwrap_or_default();
    let arg_values = match extract_arg_values(message) {
        Ok(v) => v,
        Err(e) => {
            let _ = state
                .connection
                .reply_error(message, "org.freedesktop.DBus.Error.InvalidArgs", &e.to_string())
                .await;
            return;
        }
    };

    match member {
        "Get" => {
            let (Some(interface), Some(property)) = (
                arg_values.first().and_then(|v| v.as_str()),
                arg_values.get(1).and_then(|v| v.as_str()),
            ) else {
                let _ = state
                    .connection
                    .reply_error(message, "org.freedesktop.DBus.Error.InvalidArgs", &"Get(interface, property) expected".to_string())
                    .await;
                return;
            };
            match fetch_property_value(state, bus_name, &path, interface, property).await {
                Ok(value) => {
                    // `value` is already a `zvariant::Value`, which
                    // self-describes as a variant when serialized -
                    // do NOT wrap it again in `Value::Value(..)`, that
                    // would double the variant framing on the wire.
                    let _ = state.connection.reply(message, &value).await;
                }
                Err(e) => {
                    let _ = state
                        .connection
                        .reply_error(message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                        .await;
                }
            }
        }
        "GetAll" => {
            let Some(interface) = arg_values.first().and_then(|v| v.as_str()) else {
                let _ = state
                    .connection
                    .reply_error(message, "org.freedesktop.DBus.Error.InvalidArgs", &"GetAll(interface) expected".to_string())
                    .await;
                return;
            };
            let table = state.dispatch.read().await;
            let mut prop_names: Vec<String> = table
                .properties
                .keys()
                .filter(|(bn, iface, _)| iface == interface && bus_name.map(|b| b == bn).unwrap_or(true))
                .map(|(_, _, p)| p.clone())
                .collect();
            drop(table);

            // No explicit [[property]] entries for this interface - in
            // passthrough mode, fall back to whatever a discovered system
            // interface XML declares (see introspect::find_system_interface_desc).
            // Recovers GetAll's property *list* automatically; still no
            // config needed. If nothing declares this interface at all,
            // GetAll degrades to an empty dict rather than an error - a
            // caller asking for a specific property via Get() still works
            // either way, since that path doesn't need an enumerated list.
            if prop_names.is_empty() {
                if let Some(desc) = crate::dbus::introspect::find_system_interface_desc(interface) {
                    prop_names = desc.properties.keys().cloned().collect();
                }
            }

            let mut dict = zvariant::Dict::new(
                zvariant::Signature::from_static_str_unchecked("s"),
                zvariant::Signature::from_static_str_unchecked("v"),
            );
            for prop_name in prop_names {
                if let Ok(value) = fetch_property_value(state, bus_name, &path, interface, &prop_name).await {
                    // Dict entries with declared val_sig "v" require the
                    // explicit Value::Value(..) wrapper - see the long
                    // comment on convert.rs's `DbusType::Variant` arm for
                    // why this differs from the bare top-level Get() reply
                    // just above.
                    let _ = dict.append(Value::Str(prop_name.into()), Value::Value(Box::new(value)));
                }
            }
            let _ = state.connection.reply(message, &Value::Dict(dict)).await;
        }
        "Set" => {
            let (Some(interface), Some(property)) = (
                arg_values.first().and_then(|v| v.as_str()),
                arg_values.get(1).and_then(|v| v.as_str()),
            ) else {
                let _ = state
                    .connection
                    .reply_error(message, "org.freedesktop.DBus.Error.InvalidArgs", &"Set(interface, property, value) expected".to_string())
                    .await;
                return;
            };
            let new_value = arg_values.get(2).cloned().unwrap_or(serde_json::Value::Null);

            let table = state.dispatch.read().await;
            let explicit = find_property(&table, bus_name, interface, property).cloned_prop();
            let passthrough_fallback = explicit.is_none().then(|| resolve_service(&table, bus_name, &path).cloned()).flatten();
            drop(table);

            let (backend_addr, varlink_method, peer) = match explicit {
                Some((addr, method, peer)) => (addr, method, peer),
                None => match passthrough_fallback.filter(|s| s.passthrough) {
                    Some(service) => {
                        if service.varlink.backend.is_none() && service.peer.is_none() {
                            let _ = state
                                .connection
                                .reply_error(message, "org.freedesktop.DBus.Error.Failed", &"passthrough is enabled but [varlink].backend is not configured".to_string())
                                .await;
                            return;
                        }
                        (
                            service.varlink.backend.clone().unwrap_or_default(),
                            format!("{interface}.{property}"),
                            service.peer.clone(),
                        )
                    }
                    None => {
                        let _ = state
                            .connection
                            .reply_error(message, "org.freedesktop.DBus.Error.UnknownProperty", &property.to_string())
                            .await;
                        return;
                    }
                },
            };

            let params = serde_json::json!({ "value": new_value });
            let result = tokio::time::timeout(
                DEFAULT_CALL_TIMEOUT,
                call_backend(peer.as_ref(), &backend_addr, &varlink_method, Some(params)),
            )
            .await;
            match result {
                Ok(Ok(_)) => {
                    let _ = state.connection.reply(message, &()).await;
                }
                Ok(Err(e)) => {
                    let _ = state
                        .connection
                        .reply_error(message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                        .await;
                }
                Err(_) => {
                    let _ = state
                        .connection
                        .reply_error(message, "org.freedesktop.DBus.Error.Timeout", &"varlink backend timed out".to_string())
                        .await;
                }
            }
        }
        _ => {
            let _ = state
                .connection
                .reply_error(message, "org.freedesktop.DBus.Error.UnknownMethod", &member.to_string())
                .await;
        }
    }
}

trait ClonedProp {
    fn cloned_prop(self) -> Option<(String, String, Option<Arc<crate::varlink::peer::PeerConnection>>)>;
}
impl ClonedProp for Option<&PropertyEntry> {
    fn cloned_prop(self) -> Option<(String, String, Option<Arc<crate::varlink::peer::PeerConnection>>)> {
        self.map(|entry| {
            (
                entry.service.varlink.backend.clone().unwrap_or_default(),
                entry.mapping.varlink_method.clone(),
                entry.service.peer.clone(),
            )
        })
    }
}

/// Fetch and convert one property's value, trying in order:
///   1. An explicit `[[property]]` mapping (real declared type from this
///      service's own `introspection_xml`).
///   2. `passthrough` mode: forward to `"{interface}.{property}"` on the
///      service's backend (same convention `[[property]].varlink_method`
///      already uses for both Get and Set), typed via a discovered system
///      interface XML if one exists, else inferred from the JSON reply.
async fn fetch_property_value(
    state: &BridgeState,
    bus_name: Option<&str>,
    path: &str,
    interface: &str,
    property: &str,
) -> Result<Value<'static>, BoxError> {
    let table = state.dispatch.read().await;
    if let Some(entry) = find_property(&table, bus_name, interface, property) {
        let backend_addr = entry.service.varlink.backend.clone().unwrap_or_default();
        let varlink_method = entry.mapping.varlink_method.clone();
        let resolved_bus_name = entry.service.name.clone();
        let peer = entry.service.peer.clone();
        drop(table);

        let type_sig = {
            let introspection = state.introspection.read().await;
            introspection
                .get(&resolved_bus_name)
                .and_then(|ifaces| ifaces.get(interface))
                .and_then(|desc| desc.properties.get(property))
                .map(|p| p.type_sig.clone())
        };

        let reply_json = tokio::time::timeout(
            DEFAULT_CALL_TIMEOUT,
            call_backend(peer.as_ref(), &backend_addr, &varlink_method, None),
        )
        .await
        .map_err(|_| "varlink backend timed out fetching property".to_string())??;

        let value_json = match &reply_json {
            serde_json::Value::Object(map) if map.contains_key(property) => map[property].clone(),
            other => other.clone(),
        };

        return match type_sig {
            Some(sig) => {
                let ty = convert::parse_single_complete_type(&sig)?;
                convert::json_to_dbus(&value_json, &ty).map_err(BoxError::from)
            }
            None => convert::json_to_dbus_inferred(&value_json).map_err(BoxError::from),
        };
    }

    // No explicit [[property]] entry - try passthrough.
    let service = resolve_service(&table, bus_name, path).cloned();
    drop(table);
    let service = service
        .filter(|s| s.passthrough)
        .ok_or_else(|| format!("no property mapping for {interface}.{property}"))?;
    let peer = service.peer.clone();
    let backend_addr = if peer.is_some() {
        String::new()
    } else {
        service
            .varlink
            .backend
            .clone()
            .ok_or("passthrough is enabled but [varlink].backend is not configured")?
    };
    let varlink_method = format!("{interface}.{property}");

    let reply_json = tokio::time::timeout(
        DEFAULT_CALL_TIMEOUT,
        call_backend(peer.as_ref(), &backend_addr, &varlink_method, None),
    )
    .await
    .map_err(|_| "varlink backend timed out fetching property".to_string())??;

    let value_json = match &reply_json {
        serde_json::Value::Object(map) if map.contains_key(property) => map[property].clone(),
        other => other.clone(),
    };

    let discovered_type = crate::dbus::introspect::find_system_interface_desc(interface)
        .and_then(|desc| desc.properties.get(property).map(|p| p.type_sig.clone()));
    match discovered_type {
        Some(sig) => {
            let ty = convert::parse_single_complete_type(&sig)?;
            convert::json_to_dbus(&value_json, &ty).map_err(BoxError::from)
        }
        None => convert::json_to_dbus_inferred(&value_json).map_err(BoxError::from),
    }
}

pub(crate) fn extract_arg_values(message: &Message) -> Result<Vec<serde_json::Value>, BoxError> {
    let body = message.body();
    match body.signature() {
        Some(sig) if !sig.as_str().is_empty() => {
            let structure: zvariant::Structure = body
                .deserialize()
                .map_err(|e| format!("failed to deserialize D-Bus body: {e}"))?;
            Ok(structure.fields().iter().map(convert::dbus_to_json).collect())
        }
        _ => Ok(Vec::new()),
    }
}

/// Shared by dynamic_object.rs: build a JSON object out of positionally
/// zipped introspection arg names and already-converted JSON values.
pub(crate) fn zip_named_args(args: &[ArgDesc], values: &[serde_json::Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (i, (arg, value)) in args.iter().zip(values.iter()).enumerate() {
        let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
        map.insert(key, value.clone());
    }
    serde_json::Value::Object(map)
}

/// Shared by dynamic_object.rs: convert a JSON reply back into D-Bus
/// `Value`s given a list of expected out-args, same convention as the
/// static-dispatch `reply_success` above.
pub(crate) fn json_to_out_values(
    reply_json: &serde_json::Value,
    out_args: &[ArgDesc],
) -> Result<Vec<Value<'static>>, BoxError> {
    let mut values = Vec::new();
    if out_args.is_empty() {
        return Ok(values);
    }
    if out_args.len() == 1 && !reply_json_has_key(reply_json, &out_args[0]) {
        let ty = convert::parse_single_complete_type(&out_args[0].type_sig)?;
        values.push(convert::json_to_dbus(reply_json, &ty)?);
        return Ok(values);
    }
    for (i, arg) in out_args.iter().enumerate() {
        let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
        let field = reply_json.get(&key).cloned().unwrap_or(serde_json::Value::Null);
        let ty = convert::parse_single_complete_type(&arg.type_sig)?;
        values.push(convert::json_to_dbus(&field, &ty)?);
    }
    Ok(values)
}

#[allow(dead_code)]
fn unused_type_hint(_: DbusType) {}
