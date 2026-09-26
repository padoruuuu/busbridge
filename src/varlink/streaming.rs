//! Outbound `"more": true` subscriptions: the bridge as a Varlink *client*
//! holding a call open to receive a stream of pushed replies, translated
//! into D-Bus signals. docs/DESIGN_BRIEF_V1.md Section 2.4.
//!
//! Distinct from varlink/server.rs's inbound-push model (2.2): here *we*
//! connect out and keep asking; there, backends connect *to* us. Config
//! chooses per-signal via `[[signal]].direction`.
//!
//! Every open subscription holds an idle-exit guard for its entire
//! lifetime (docs/DESIGN_BRIEF_V1.md Section 3.7: "a held-open streaming call ... must
//! be treated as not idle"), and reconnects with capped exponential
//! backoff if the backend drops the connection or is briefly unavailable,
//! rather than giving up permanently - a backend restarting is a normal
//! event, not a fatal one.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};
use zvariant::{StructureBuilder, Value};

use crate::config::schema::SignalDirection;
use crate::config::LoadedService;
use crate::dbus::BridgeState;
use crate::varlink::client::VarlinkConnection;
use crate::BoxError;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct SubscriptionSpec {
    service: Arc<LoadedService>,
    interface: String,
    signal_name: String,
    varlink_method: String,
}

/// Start one background task per configured `outbound_subscribe` signal.
pub async fn start_subscriptions(state: Arc<BridgeState>) {
    let specs: Vec<SubscriptionSpec> = {
        let table = state.dispatch.read().await;
        table
            .signals
            .values()
            .filter_map(|entry| match &entry.mapping.direction {
                SignalDirection::OutboundSubscribe { varlink_method } => Some(SubscriptionSpec {
                    service: entry.service.clone(),
                    interface: entry.mapping.dbus_interface.clone(),
                    signal_name: entry.mapping.dbus_signal.clone(),
                    varlink_method: varlink_method.clone(),
                }),
                SignalDirection::InboundPush => None,
            })
            .collect()
    };

    for spec in specs {
        let state = state.clone();
        tokio::spawn(async move {
            run_subscription(state, spec).await;
        });
    }
}

async fn run_subscription(state: Arc<BridgeState>, spec: SubscriptionSpec) {
    let Some(backend_addr) = spec.service.varlink.backend.clone() else {
        warn!(
            bus_name = %spec.service.name,
            interface = %spec.interface,
            signal = %spec.signal_name,
            "outbound_subscribe signal has no [varlink].backend configured, cannot subscribe"
        );
        return;
    };

    let mut backoff = INITIAL_BACKOFF;
    loop {
        let hold = state.activity.hold();
        info!(bus_name = %spec.service.name, interface = %spec.interface, signal = %spec.signal_name, "starting outbound varlink subscription");
        let outcome = run_once(&state, &spec, &backend_addr).await;
        drop(hold);

        match outcome {
            Ok(true) => {
                // Backend told us the stream ended normally
                // (`continues: false`) or closed the connection cleanly -
                // reset backoff and try again immediately, since this
                // isn't a failure.
                backoff = INITIAL_BACKOFF;
            }
            Ok(false) => {
                backoff = INITIAL_BACKOFF;
            }
            Err(e) => {
                warn!(
                    bus_name = %spec.service.name,
                    interface = %spec.interface,
                    signal = %spec.signal_name,
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "outbound subscription failed, retrying after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        }
        // Brief pause even on a clean end, to avoid a hot reconnect loop
        // against a backend that immediately closes every connection.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Runs one subscription attempt to completion. Returns `Ok(true)` if the
/// stream ended normally (backend said `continues: false` or closed
/// cleanly), `Err` on a connection-level failure that should be retried
/// after backoff.
async fn run_once(state: &BridgeState, spec: &SubscriptionSpec, backend_addr: &str) -> Result<bool, BoxError> {
    let conn = VarlinkConnection::connect(backend_addr).await?;
    let mut conn = conn.call_streaming(&spec.varlink_method, None).await?;

    loop {
        match conn.next_reply().await? {
            None => return Ok(true),
            Some(reply) => {
                if let Some(err_name) = reply.error {
                    warn!(
                        bus_name = %spec.service.name,
                        interface = %spec.interface,
                        signal = %spec.signal_name,
                        varlink_error = %err_name,
                        "outbound subscription backend returned an error, ending this attempt"
                    );
                    return Ok(true);
                }
                emit_from_streaming_reply(state, spec, reply.parameters).await;
                state.activity.touch();
                if !reply.continues {
                    return Ok(true);
                }
            }
        }
    }
}

async fn emit_from_streaming_reply(
    state: &BridgeState,
    spec: &SubscriptionSpec,
    parameters: Option<serde_json::Value>,
) {
    let arg_descs = {
        let introspection = state.introspection.read().await;
        introspection
            .get(&spec.service.name)
            .and_then(|ifaces| ifaces.get(&spec.interface))
            .and_then(|desc| desc.signals.get(&spec.signal_name))
            .map(|s| s.args.clone())
    };
    let Some(arg_descs) = arg_descs else {
        warn!(
            bus_name = %spec.service.name,
            interface = %spec.interface,
            signal = %spec.signal_name,
            "no introspection entry for subscribed signal, cannot type-convert, dropping event"
        );
        return;
    };

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
                    warn!(error = %e, "failed to convert streamed event field, dropping event");
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
                spec.service.object_path.as_str(),
                spec.interface.as_str(),
                spec.signal_name.as_str(),
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
                spec.service.object_path.as_str(),
                spec.interface.as_str(),
                spec.signal_name.as_str(),
                &builder.build(),
            )
            .await
    };
    if let Err(e) = result {
        warn!(error = %e, "failed to emit D-Bus signal from subscribed stream event");
    }
}

#[allow(dead_code)]
fn unused_value_hint(_: Value<'static>) {}
