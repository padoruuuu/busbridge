//! Generic outbound D-Bus *client* proxy, exposed over Varlink.
//!
//! Everything else in this crate is about the bridge OWNING a D-Bus name
//! and forwarding calls it RECEIVES to one preconfigured Varlink backend.
//! This module is the complement: it lets a Varlink app ask the bridge to
//! make ARBITRARY outbound D-Bus calls, read/write arbitrary properties,
//! introspect arbitrary objects, and subscribe to arbitrary signals - on
//! any bus name, not just ones this bridge itself owns. Together with the
//! rest of the crate, this is what lets an app be rewritten to speak only
//! Varlink and never link a D-Bus library at all, even when its job
//! requires acting as a full D-Bus client against other, unrelated,
//! D-Bus-only third-party processes (the canonical example: a
//! StatusNotifierWatcher-style tray host, which must query whichever
//! tray application just registered - a bus name it can't know ahead of
//! time and therefore can't express as static config).
//!
//! Enabled per-service via `[varlink].bus_proxy_listen` (config/schema.rs)
//! - opt-in, since it grants whoever can reach the socket the same
//! practical trust as a process connected directly to that bus.
//!
//! Wire protocol (same NUL-delimited-JSON framing as everywhere else in
//! varlink/mod.rs), five methods:
//!
//!   `org.busbridge.BusProxy.Call`
//!     in:  {"destination", "path", "interface", "method", "args": [..]}
//!     out: {"reply": [..]}
//!   Auto-introspects the target first to convert `args`/the reply using
//!   its REAL declared types (dbus/introspect.rs::parse on a live
//!   Introspect() response - always correct when the target implements
//!   introspection, which almost everything does). Falls back to
//!   positional type inference (convert::json_to_dbus_inferred) only if
//!   introspection fails or doesn't declare the method.
//!
//!   `org.busbridge.BusProxy.Introspect`
//!     in: {"destination", "path"} out: {"xml": "<node>...</node>"}
//!
//!   `org.busbridge.BusProxy.GetProperty` /
//!   `org.busbridge.BusProxy.GetAllProperties` /
//!   `org.busbridge.BusProxy.SetProperty`
//!   Plain forwards to the target's own `org.freedesktop.DBus.Properties`
//!   - always safe to convert generically since property values are
//!   always variant-wrapped on the wire regardless of their real type.
//!
//!   `org.busbridge.BusProxy.Subscribe`
//!     in: {"sender"?, "path"?, "interface"?, "member"?} (all optional)
//!     more: true - one streamed reply per matching signal:
//!     {"sender", "path", "interface", "member", "args": [..]}, until the
//!     client disconnects. Holds an idle-exit activity guard for its
//!     whole lifetime (docs/DESIGN_BRIEF_V1.md Section 3.7's "held-open streaming call
//!     must be treated as not idle", same as varlink/streaming.rs).

use std::sync::Arc;

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tracing::{info, warn};
use zbus::message::Type as MessageType;
use zvariant::{StructureBuilder, Value};

use crate::config::LoadedService;
use crate::convert;
use crate::dbus::dispatch::extract_arg_values;
use crate::dbus::introspect::{self, MethodDesc};
use crate::dbus::BridgeState;
use crate::varlink::{read_framed, service_get_info, write_framed, VarlinkReply, VarlinkRequest};

/// Start one accept loop per configured `[varlink].bus_proxy_listen`
/// address.
pub async fn start_listeners(state: Arc<BridgeState>) {
    let specs: Vec<Arc<LoadedService>> = {
        let table = state.dispatch.read().await;
        table
            .services
            .values()
            .filter(|s| s.varlink.bus_proxy_listen.is_some())
            .cloned()
            .collect()
    };

    for service in specs {
        let listen_addr = service.varlink.bus_proxy_listen.clone().unwrap();
        match crate::varlink::server::bind_fresh(&listen_addr) {
            Ok(listener) => {
                info!(bus_name = %service.name, listen = %listen_addr, "generic D-Bus bus-proxy listener ready");
                let state = state.clone();
                tokio::spawn(async move {
                    accept_loop(state, listener).await;
                });
            }
            Err(e) => {
                warn!(bus_name = %service.name, listen = %listen_addr, error = %e, "failed to bind bus-proxy listener, skipping");
            }
        }
    }
}

async fn accept_loop(state: Arc<BridgeState>, listener: tokio::net::UnixListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                state.activity.touch();
                let state = state.clone();
                tokio::spawn(async move {
                    handle_connection(state, stream).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "bus-proxy accept() failed, stopping this listener");
                break;
            }
        }
    }
}

async fn handle_connection(state: Arc<BridgeState>, stream: tokio::net::UnixStream) {
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
                warn!(error = %e, "bus-proxy connection read error");
                break;
            }
        };
        let Some(req) = req else { break };
        state.activity.touch();

        match req.method.as_str() {
            "org.varlink.service.GetInfo" => {
                let _ = write_framed(&mut writer, &VarlinkReply::ok(service_get_info())).await;
            }
            "org.busbridge.BusProxy.Call" => {
                handle_call(&state, &mut writer, req.parameters).await;
            }
            "org.busbridge.BusProxy.Introspect" => {
                handle_introspect(&state, &mut writer, req.parameters).await;
            }
            "org.busbridge.BusProxy.GetProperty" => {
                handle_get_property(&state, &mut writer, req.parameters).await;
            }
            "org.busbridge.BusProxy.GetAllProperties" => {
                handle_get_all_properties(&state, &mut writer, req.parameters).await;
            }
            "org.busbridge.BusProxy.SetProperty" => {
                handle_set_property(&state, &mut writer, req.parameters).await;
            }
            "org.busbridge.BusProxy.Subscribe" => {
                handle_subscribe(&state, &mut writer, req.parameters).await;
                // Subscribe consumes the rest of the connection's
                // lifetime (it's a long-lived "more: true" stream); the
                // client is expected to just disconnect to unsubscribe.
                break;
            }
            other => {
                let _ = write_framed(
                    &mut writer,
                    &VarlinkReply::error("org.varlink.service.MethodNotFound", Some(json!({"method": other}))),
                )
                .await;
            }
        }
    }
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: Option<JsonValue>) -> Result<T, String> {
    let params = params.ok_or("missing parameters")?;
    serde_json::from_value(params).map_err(|e| e.to_string())
}

pub(crate) async fn reply_err<W: AsyncWrite + Unpin>(writer: &mut W, name: &str, message: impl Into<String>) {
    let _ = write_framed(writer, &VarlinkReply::error(name, Some(json!({"message": message.into()})))).await;
}

/// Shared call primitive: given already-converted D-Bus argument
/// `Value`s, make the actual outbound call. Both this module's own `Call`
/// (positional-array convention) and `dbus/resolver.rs`'s directly-routed
/// calls (named-object convention, Varlink's own native style) build
/// their `fields` differently but share this exact tail - and, deliberately,
/// do NOT share how the result gets shaped into a Varlink reply, since the
/// two conventions want different reply shapes.
pub(crate) async fn perform_call(
    state: &BridgeState,
    destination: &str,
    path: &str,
    interface: &str,
    method: &str,
    fields: Vec<Value<'static>>,
) -> Result<zbus::Message, zbus::Error> {
    if fields.is_empty() {
        state.connection.call_method(Some(destination), path, Some(interface), method, &()).await
    } else {
        let mut builder = StructureBuilder::new();
        for f in fields {
            builder = builder.append_field(f);
        }
        state
            .connection
            .call_method(Some(destination), path, Some(interface), method, &builder.build())
            .await
    }
}

/// Map a real D-Bus call failure onto a Varlink error reply: a genuine
/// `MethodError` from the target passes its D-Bus error name straight
/// through (mirrors errors.rs's opposite-direction default convention),
/// anything else (connection trouble, bad destination, etc.) becomes a
/// generic `CallFailed`. `pub(crate)` so `dbus/resolver.rs` can reuse it.
pub(crate) async fn reply_dbus_error<W: AsyncWrite + Unpin>(writer: &mut W, error: zbus::Error) {
    match error {
        zbus::Error::MethodError(name, detail, _msg) => {
            let _ = write_framed(
                writer,
                &VarlinkReply::error(name.to_string(), Some(json!({"message": detail.unwrap_or_default()}))),
            )
            .await;
        }
        other => reply_err(writer, "org.busbridge.BusProxy.CallFailed", other.to_string()).await,
    }
}

#[derive(Debug, Deserialize)]
struct CallParams {
    destination: String,
    path: String,
    interface: String,
    method: String,
    #[serde(default)]
    args: Vec<JsonValue>,
}

/// Auto-introspect `destination`/`path` and look up one specific method's
/// declared description. `None` on any failure (target doesn't implement
/// introspection, doesn't declare this interface/method, XML didn't
/// parse, etc.) - callers treat that as "fall back to inference", not as
/// an error; plenty of legitimate D-Bus services have incomplete or
/// absent introspection.
///
/// `pub(crate)` so `dbus/resolver.rs` can reuse the exact same
/// auto-discovery this module's own `Call` uses.
pub(crate) async fn introspect_method(
    connection: &zbus::Connection,
    destination: &str,
    path: &str,
    interface: &str,
    method: &str,
) -> Option<MethodDesc> {
    let reply = connection
        .call_method(
            Some(destination),
            path,
            Some("org.freedesktop.DBus.Introspectable"),
            "Introspect",
            &(),
        )
        .await
        .ok()?;
    let xml: String = reply.body().deserialize().ok()?;
    let ifaces = introspect::parse(&xml).ok()?;
    ifaces.get(interface)?.methods.get(method).cloned()
}

async fn handle_call<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: CallParams = match parse_params(params) {
        Ok(p) => p,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };

    let method_desc = introspect_method(
        &state.connection,
        &params.destination,
        &params.path,
        &params.interface,
        &params.method,
    )
    .await;

    let mut fields: Vec<Value<'static>> = Vec::new();
    let conversion: Result<(), String> = 'convert: {
        if let Some(desc) = &method_desc {
            if !desc.in_args.is_empty() || params.args.is_empty() {
                if desc.in_args.len() != params.args.len() {
                    break 'convert Err(format!(
                        "{} expects {} argument(s), got {}",
                        params.method,
                        desc.in_args.len(),
                        params.args.len()
                    ));
                }
                for (arg, json_val) in desc.in_args.iter().zip(params.args.iter()) {
                    match convert::parse_single_complete_type(&arg.type_sig)
                        .map_err(|e| e.to_string())
                        .and_then(|ty| convert::json_to_dbus(json_val, &ty).map_err(|e| e.to_string()))
                    {
                        Ok(v) => fields.push(v),
                        Err(e) => break 'convert Err(e),
                    }
                }
                break 'convert Ok(());
            }
        }
        // No usable introspection - fall back to per-argument inference.
        for json_val in &params.args {
            match convert::json_to_dbus_inferred(json_val) {
                Ok(v) => fields.push(v),
                Err(e) => break 'convert Err(e.to_string()),
            }
        }
        Ok(())
    };
    if let Err(e) = conversion {
        return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await;
    }

    let result = perform_call(
        state,
        &params.destination,
        &params.path,
        &params.interface,
        &params.method,
        fields,
    )
    .await;

    match result {
        Ok(reply_msg) => match extract_arg_values(&reply_msg) {
            Ok(values) => {
                let _ = write_framed(writer, &VarlinkReply::ok(json!({ "reply": values }))).await;
            }
            Err(e) => reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
        },
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

#[derive(Debug, Deserialize)]
struct IntrospectParams {
    destination: String,
    path: String,
}

async fn handle_introspect<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: IntrospectParams = match parse_params(params) {
        Ok(p) => p,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };
    let result = state
        .connection
        .call_method(
            Some(params.destination.as_str()),
            params.path.as_str(),
            Some("org.freedesktop.DBus.Introspectable"),
            "Introspect",
            &(),
        )
        .await;
    match result {
        Ok(reply_msg) => match reply_msg.body().deserialize::<String>() {
            Ok(xml) => {
                let _ = write_framed(writer, &VarlinkReply::ok(json!({ "xml": xml }))).await;
            }
            Err(e) => reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
        },
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

#[derive(Debug, Deserialize)]
struct PropertyParams {
    destination: String,
    path: String,
    interface: String,
    property: String,
}

async fn handle_get_property<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: PropertyParams = match parse_params(params) {
        Ok(p) => p,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };
    let result = state
        .connection
        .call_method(
            Some(params.destination.as_str()),
            params.path.as_str(),
            Some("org.freedesktop.DBus.Properties"),
            "Get",
            &(params.interface.as_str(), params.property.as_str()),
        )
        .await;
    match result {
        Ok(reply_msg) => match reply_msg.body().deserialize::<Value>() {
            // dbus_to_json already unwraps one variant layer, matching
            // how a real deserialized "v" slot naturally arrives.
            Ok(value) => {
                let _ = write_framed(writer, &VarlinkReply::ok(json!({ "value": convert::dbus_to_json(&value) }))).await;
            }
            Err(e) => reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
        },
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

#[derive(Debug, Deserialize)]
struct GetAllPropertiesParams {
    destination: String,
    path: String,
    interface: String,
}

async fn handle_get_all_properties<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: GetAllPropertiesParams = match parse_params(params) {
        Ok(p) => p,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };
    let result = state
        .connection
        .call_method(
            Some(params.destination.as_str()),
            params.path.as_str(),
            Some("org.freedesktop.DBus.Properties"),
            "GetAll",
            &(params.interface.as_str(),),
        )
        .await;
    match result {
        Ok(reply_msg) => match reply_msg.body().deserialize::<Value>() {
            Ok(value) => {
                let _ = write_framed(writer, &VarlinkReply::ok(json!({ "values": convert::dbus_to_json(&value) }))).await;
            }
            Err(e) => reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
        },
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

#[derive(Debug, Deserialize)]
struct SetPropertyParams {
    destination: String,
    path: String,
    interface: String,
    property: String,
    value: JsonValue,
}

async fn handle_set_property<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: SetPropertyParams = match parse_params(params) {
        Ok(p) => p,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e).await,
    };
    let value = match convert::json_to_dbus_inferred(&params.value) {
        Ok(v) => v,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
    };
    // Properties.Set's third argument is declared type "v" - building it
    // via a container (StructureBuilder) requires the explicit
    // Value::Value(..) wrap to get that declared signature (verified
    // empirically; see convert.rs's `DbusType::Variant` arm for the full
    // explanation of when this wrap is/isn't needed).
    let body = StructureBuilder::new()
        .append_field(Value::Str(params.interface.clone().into()))
        .append_field(Value::Str(params.property.clone().into()))
        .append_field(Value::Value(Box::new(value)))
        .build();
    let result = state
        .connection
        .call_method(
            Some(params.destination.as_str()),
            params.path.as_str(),
            Some("org.freedesktop.DBus.Properties"),
            "Set",
            &body,
        )
        .await;
    match result {
        Ok(_) => {
            let _ = write_framed(writer, &VarlinkReply::ok(json!({}))).await;
        }
        Err(e) => reply_dbus_error(writer, e).await,
    }
}

#[derive(Debug, Default, Deserialize)]
struct SubscribeParams {
    #[serde(default)]
    sender: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    interface: Option<String>,
    #[serde(default)]
    member: Option<String>,
}

async fn handle_subscribe<W: AsyncWrite + Unpin>(state: &BridgeState, writer: &mut W, params: Option<JsonValue>) {
    let params: SubscribeParams = match params {
        None => SubscribeParams::default(),
        Some(p) => match serde_json::from_value(p) {
            Ok(p) => p,
            Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
        },
    };

    let mut builder = zbus::MatchRule::builder().msg_type(MessageType::Signal);
    if let Some(sender) = &params.sender {
        builder = match builder.sender(sender.as_str()) {
            Ok(b) => b,
            Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
        };
    }
    if let Some(path) = &params.path {
        builder = match builder.path(path.as_str()) {
            Ok(b) => b,
            Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
        };
    }
    if let Some(interface) = &params.interface {
        builder = match builder.interface(interface.as_str()) {
            Ok(b) => b,
            Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
        };
    }
    if let Some(member) = &params.member {
        builder = match builder.member(member.as_str()) {
            Ok(b) => b,
            Err(e) => return reply_err(writer, "org.busbridge.BusProxy.InvalidParameters", e.to_string()).await,
        };
    }
    let rule = builder.build();

    let mut stream = match zbus::MessageStream::for_match_rule(rule, &state.connection, None).await {
        Ok(s) => s,
        Err(e) => return reply_err(writer, "org.busbridge.BusProxy.CallFailed", e.to_string()).await,
    };

    // A subscription is exactly the kind of held-open call docs/DESIGN_BRIEF_V1.md
    // Section 3.7 says must suppress idle-exit for its whole lifetime.
    let _hold = state.activity.hold();

    while let Some(msg) = stream.next().await {
        let Ok(msg) = msg else { break };
        let sender = msg.header().sender().map(|s| s.to_string()).unwrap_or_default();
        let path = msg.header().path().map(|p| p.to_string()).unwrap_or_default();
        let interface = msg.header().interface().map(|i| i.to_string()).unwrap_or_default();
        let member = msg.header().member().map(|m| m.to_string()).unwrap_or_default();
        let args = extract_arg_values(&msg).unwrap_or_default();
        let payload = json!({
            "sender": sender,
            "path": path,
            "interface": interface,
            "member": member,
            "args": args,
        });
        if write_framed(writer, &VarlinkReply::ok_continuing(payload)).await.is_err() {
            break;
        }
        state.activity.touch();
    }
}
