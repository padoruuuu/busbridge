//! Runtime D-Bus object creation/proxy/teardown for `registrable`
//! interfaces. docs/DESIGN_BRIEF_V1.md Section 2.5.
//!
//! Convention notes (schema doesn't specify these, so - state the
//! assumption, proceed):
//!   - The Varlink method name a legacy D-Bus call is forwarded as is
//!     `"{dbus_interface}.{member}"` (mirrors the static registrable
//!     interface's own D-Bus naming, so `Activate` on
//!     `org.kde.StatusNotifierItem` forwards as
//!     `"org.kde.StatusNotifierItem.Activate"`). `org.freedesktop.DBus.
//!     Properties.Get/Set/GetAll` forward the same way, with
//!     `org.freedesktop.DBus.Properties` as the prefix, so the app can
//!     distinguish them from ordinary interface methods.
//!   - The other direction (a push *from* the app, `handle_push` below)
//!     uses the bare member name instead (`"NewStatus"`, not
//!     `"org.kde.StatusNotifierItem.NewStatus"`) - deliberately
//!     asymmetric with the call-forwarding convention above. A
//!     registrable connection is already scoped to exactly one
//!     interface (the one it registered for), so there's nothing to
//!     disambiguate; this is unlike the *static* `[[signal]]
//!     direction=inbound_push]` convention in varlink/server.rs, which
//!     does require the `"{interface}.{member}"` prefix, because one
//!     shared push connection there can carry events for several
//!     different interfaces. Getting this backwards (as an early draft
//!     of tests/sni_tray.rs did) fails silently: `handle_push` just
//!     doesn't find a matching signal and drops the event, so the app
//!     appears to have pushed nothing.
//!   - Named args (both directions) come from the `introspection_xml`
//!     for the `registrable` interface, keyed by arg name (or `argN` if
//!     unnamed).
//!   - The single Varlink connection the app opened to register is reused
//!     bidirectionally for its entire lifetime: our bridge also acts as a
//!     Varlink *client* on that same connection to forward D-Bus calls
//!     to the app, multiplexed against the app's own unsolicited pushes
//!     (event notifications) by the standard Varlink wire distinction -
//!     a frame with a `"method"` key is a push *from* the app; a frame
//!     without one is a reply *to* a call *we* made. Outbound calls are
//!     serialized (one in flight at a time per connection) so replies can
//!     be matched to requests purely by FIFO order without a serial-id
//!     field (which Varlink's wire format doesn't have).
//!
//! Isolation (docs/DESIGN_BRIEF_V1.md Section 2.5): every inbound D-Bus call for a
//! dynamic object is dispatched (by dispatch.rs, which owns the shared
//! message loop) onto its own task before reaching here, and the routing
//! table below (`DynamicObjectRegistry`) is a plain `RwLock<HashMap>` -
//! a fast lookup, never held across an await that talks to a backend.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::{info, warn};
use zbus::Message;
use zvariant::{StructureBuilder, Value};

use crate::config::schema::RegistrableConfig;
use crate::config::LoadedService;
use crate::dbus::dispatch::{extract_arg_values, json_to_out_values, zip_named_args};
use crate::dbus::introspect::{self, InterfaceDesc};
use crate::dbus::BridgeState;
use crate::errors::VarlinkError;
use crate::idle::ActivityGuard;
use crate::varlink::{read_framed, write_framed, VarlinkRequest};
use crate::BoxError;

type PendingReply = oneshot::Sender<Result<JsonValue, VarlinkError>>;

struct Outbound {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    pending: VecDeque<PendingReply>,
}

pub struct DynamicObject {
    pub object_path: String,
    pub dbus_interface: String,
    pub bus_name: String,
    /// The registrable interface's own introspection XML, loaded once at
    /// registration time. `handle_dynamic_call` looks arg names/types up
    /// here - deliberately *not* `BridgeState::introspection`, which only
    /// ever holds each `[service]` block's *static* `introspection_xml`
    /// (populated by `load_all_introspection` in `dbus/mod.rs`) and knows
    /// nothing about `[[registrable]]` interfaces, which are typically
    /// the only interface a purely-dynamic service has. Using the wrong
    /// map here silently drops every argument name/type for dynamic
    /// objects (in_args/out_args both come back empty), which in turn
    /// drops every positional D-Bus argument on the floor in both
    /// directions - a real bug, not a hypothetical one; see
    /// `tests/sni_tray.rs`.
    pub interfaces: HashMap<String, InterfaceDesc>,
    outbound: Mutex<Outbound>,
    _hold: ActivityGuard,
}

impl DynamicObject {
    /// Forward a D-Bus-originated call to the app over this object's
    /// backing Varlink connection.
    pub async fn call(&self, method: &str, parameters: Option<JsonValue>) -> Result<JsonValue, BoxError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut ob = self.outbound.lock().await;
            let req = VarlinkRequest {
                method: method.to_string(),
                parameters,
                more: false,
                oneway: false,
            };
            write_framed(&mut ob.writer, &req).await?;
            ob.pending.push_back(tx);
        }
        match rx.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(format!("app returned varlink error {}", e.name).into()),
            Err(_) => Err("connection to app closed before it replied".into()),
        }
    }
}

#[derive(Default)]
pub struct DynamicObjectRegistry {
    objects: RwLock<HashMap<String, Arc<DynamicObject>>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl DynamicObjectRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn lookup(&self, path: &str) -> Option<Arc<DynamicObject>> {
        self.objects.read().await.get(path).cloned()
    }

    async fn insert(&self, path: String, obj: Arc<DynamicObject>) {
        self.objects.write().await.insert(path, obj);
    }

    async fn remove(&self, path: &str) {
        self.objects.write().await.remove(path);
    }
}

fn sanitize_path_segment(id: &str) -> String {
    let cleaned: String = id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "_".to_string()
    } else {
        cleaned
    }
}

/// Register a newly-connected Varlink app as a dynamic D-Bus object.
/// `reader`/`writer` are the split halves of the connection the app used
/// to send its registration request (varlink/server.rs owns accepting
/// that connection; this function takes over from there for its entire
/// lifetime). Spawns the background task that both proxies pushed events
/// as D-Bus signals and completes any outbound calls we issue.
///
/// `reply_id` is `None` for the original conf.d-driven caller
/// (varlink/server.rs), whose ack has never needed one (a bare
/// `{"parameters": {"object_path": ...}}`, matching plain Varlink
/// framing). `registration.rs`'s `RegisterDynamicObject` - the same
/// mechanism, reachable through the app registration protocol's
/// universal socket instead of a service's own dedicated
/// `varlink.listen` - passes `Some(id)` so its ack matches that
/// protocol's id-tagged reply convention
/// (docs/REGISTRATION_PROTOCOL.md). Either way this is the only line
/// that differs; everything else about registering a dynamic object is
/// identical regardless of which socket the connection arrived on.
pub async fn register(
    state: Arc<BridgeState>,
    service: Arc<LoadedService>,
    registrable: RegistrableConfig,
    app_supplied_id: Option<String>,
    reply_id: Option<serde_json::Value>,
    reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
) -> Result<String, BoxError> {
    let id = match registrable.id_source {
        crate::config::schema::IdSource::Generated => state
            .dynamic_objects
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .to_string(),
        crate::config::schema::IdSource::AppSupplied => app_supplied_id
            .ok_or("registrable interface requires id_source=app_supplied but the app didn't supply one")?,
    };
    let object_path = format!(
        "{}/{}",
        registrable.path_prefix.trim_end_matches('/'),
        sanitize_path_segment(&id)
    );

    let introspection_xml_path = service
        .source_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(&registrable.introspection_xml);
    // A service created through the app registration protocol
    // (registration.rs, service.peer.is_some()) has no real filesystem
    // path behind it - `registrable.introspection_xml` is inline XML
    // text in that case, the same reasoning as the top-level service's
    // own `introspection_xml` field (docs/REGISTRATION_PROTOCOL.md's
    // "why inline XML, not a new schema" section, applied one level
    // down). conf.d-loaded services (service.peer.is_none()) are
    // completely unaffected - this branch is new, the other one is the
    // exact code that was already here.
    let interfaces = if service.peer.is_some() {
        introspect::parse(&registrable.introspection_xml)?
    } else {
        introspect::load_from_path(&introspection_xml_path)?
    };
    let _desc: &InterfaceDesc = interfaces
        .get(&registrable.dbus_interface)
        .ok_or_else(|| format!("{} not found in its own introspection_xml", registrable.dbus_interface))?;

    let hold = state.activity.hold();
    let mut writer = writer;
    let object_path_json = serde_json::json!({ "object_path": object_path });
    let ack: serde_json::Value = match &reply_id {
        Some(id) => serde_json::json!({ "id": id, "parameters": object_path_json }),
        None => serde_json::json!({ "parameters": object_path_json }),
    };
    write_framed(&mut writer, &ack).await?;

    let obj = Arc::new(DynamicObject {
        object_path: object_path.clone(),
        dbus_interface: registrable.dbus_interface.clone(),
        bus_name: service.name.clone(),
        interfaces: interfaces.clone(),
        outbound: Mutex::new(Outbound {
            writer,
            pending: VecDeque::new(),
        }),
        _hold: hold,
    });

    state.dynamic_objects.insert(object_path.clone(), obj.clone()).await;
    info!(object_path = %object_path, interface = %registrable.dbus_interface, "registered dynamic D-Bus object");

    let interfaces_for_task = interfaces;
    let registrable_for_task = registrable;
    tokio::spawn(async move {
        run_push_reader(state, obj, reader, interfaces_for_task, registrable_for_task).await;
    });

    Ok(object_path)
}

/// Background task: reads every frame from the app. A frame with a
/// `"method"` key is an unsolicited push (translated into a D-Bus signal);
/// otherwise it's the reply to our oldest outstanding outbound call.
async fn run_push_reader(
    state: Arc<BridgeState>,
    obj: Arc<DynamicObject>,
    mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    interfaces: HashMap<String, InterfaceDesc>,
    registrable: RegistrableConfig,
) {
    loop {
        let raw: Option<JsonValue> = match read_framed(&mut reader).await {
            Ok(v) => v,
            Err(e) => {
                warn!(object_path = %obj.object_path, error = %e, "dynamic object connection read error, tearing down");
                break;
            }
        };
        let Some(raw) = raw else {
            break; // clean EOF - app disconnected.
        };

        if raw.get("method").is_some() {
            if let Ok(req) = serde_json::from_value::<VarlinkRequest>(raw) {
                handle_push(&state, &obj, &interfaces, &registrable, req).await;
            }
            continue;
        }

        #[derive(serde::Deserialize)]
        struct RawReply {
            #[serde(default)]
            parameters: Option<JsonValue>,
            #[serde(default)]
            error: Option<String>,
        }
        if let Ok(reply) = serde_json::from_value::<RawReply>(raw) {
            let mut ob = obj.outbound.lock().await;
            if let Some(tx) = ob.pending.pop_front() {
                drop(ob);
                let result = match reply.error {
                    Some(name) => Err(VarlinkError {
                        name,
                        parameters: reply.parameters,
                    }),
                    None => Ok(reply.parameters.unwrap_or(JsonValue::Null)),
                };
                let _ = tx.send(result);
            }
        }
    }

    state.dynamic_objects.remove(&obj.object_path).await;
    info!(object_path = %obj.object_path, "dynamic D-Bus object torn down (app disconnected)");
}

/// A push from the app: forward as a D-Bus signal from the synthetic
/// object path. The push's `method` field is the bare signal name (see
/// module doc comment on convention).
async fn handle_push(
    state: &BridgeState,
    obj: &DynamicObject,
    interfaces: &HashMap<String, InterfaceDesc>,
    registrable: &RegistrableConfig,
    req: VarlinkRequest,
) {
    let Some(desc) = interfaces.get(&registrable.dbus_interface) else {
        return;
    };
    let Some(signal_desc) = desc.signals.get(&req.method) else {
        warn!(object_path = %obj.object_path, method = %req.method, "push from dynamic object doesn't match a declared signal, ignoring");
        return;
    };

    let mut values = Vec::new();
    if let Some(params) = &req.parameters {
        for (i, arg) in signal_desc.args.iter().enumerate() {
            let key = arg.name.clone().unwrap_or_else(|| format!("arg{i}"));
            let field = params.get(&key).cloned().unwrap_or(JsonValue::Null);
            match crate::convert::parse_single_complete_type(&arg.type_sig)
                .map_err(BoxError::from)
                .and_then(|ty| crate::convert::json_to_dbus(&field, &ty).map_err(BoxError::from))
            {
                Ok(v) => values.push(v),
                Err(e) => {
                    warn!(error = %e, "failed to convert pushed event field to D-Bus type");
                    return;
                }
            }
        }
    }

    let result = if values.is_empty() {
        state
            .connection
            .emit_signal(
                Option::<&str>::None,
                obj.object_path.as_str(),
                registrable.dbus_interface.as_str(),
                req.method.as_str(),
                &(),
            )
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
                obj.object_path.as_str(),
                registrable.dbus_interface.as_str(),
                req.method.as_str(),
                &builder.build(),
            )
            .await
    };
    if let Err(e) = result {
        warn!(error = %e, object_path = %obj.object_path, "failed to emit D-Bus signal for dynamic object push");
    }
    state.activity.touch();
}

/// Forward an inbound D-Bus call destined for a dynamic object to the
/// app behind it. Called from dispatch.rs once it's resolved the object.
pub async fn handle_dynamic_call(state: Arc<BridgeState>, obj: Arc<DynamicObject>, message: Message) {
    let interface = message.header().interface().map(|i| i.to_string()).unwrap_or_default();
    let member = message.header().member().map(|m| m.to_string()).unwrap_or_default();

    state.telemetry.record_call(&obj.bus_name, &interface, &member);

    // Reload the interface description fresh from disk isn't necessary
    // here - dispatch.rs already validated the object exists; arg naming
    // for the forwarded call is looked up from *this object's own*
    // introspection (loaded once at registration time from the
    // `[[registrable]]`'s `introspection_xml`), which is stable for the
    // object's whole lifetime. This must not be `state.introspection` -
    // see the doc comment on `DynamicObject::interfaces`.
    let (in_args, out_args, varlink_prefix) = {
        let desc = obj.interfaces.get(&interface);
        match desc.and_then(|d| d.methods.get(&member)) {
            Some(m) => (m.in_args.clone(), m.out_args.clone(), interface.clone()),
            None => (Vec::new(), Vec::new(), interface.clone()),
        }
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
    let mut params = match zip_named_args(&in_args, &arg_values) {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    crate::dbus::dispatch::insert_dbus_sender(&mut params, &message);
    let parameters = if params.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(params))
    };

    let varlink_method = format!("{varlink_prefix}.{member}");
    let timeout = std::time::Duration::from_secs(30);
    match tokio::time::timeout(timeout, obj.call(&varlink_method, parameters)).await {
        Err(_) => {
            let _ = state
                .connection
                .reply_error(&message, "org.freedesktop.DBus.Error.Timeout", &"app did not respond in time".to_string())
                .await;
        }
        Ok(Err(e)) => {
            let _ = state
                .connection
                .reply_error(&message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                .await;
        }
        Ok(Ok(reply_json)) => match json_to_out_values(&reply_json, &out_args) {
            Ok(values) if values.is_empty() => {
                let _ = state.connection.reply(&message, &()).await;
            }
            Ok(values) => {
                let mut builder = StructureBuilder::new();
                for v in values {
                    builder = builder.append_field(v);
                }
                let _ = state.connection.reply(&message, &builder.build()).await;
            }
            Err(e) => {
                let _ = state
                    .connection
                    .reply_error(&message, "org.freedesktop.DBus.Error.Failed", &e.to_string())
                    .await;
            }
        },
    }
}

#[allow(dead_code)]
fn unused_value_hint(_: Value<'static>) {}
