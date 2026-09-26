//! D-Bus connection setup via zbus's LOW-LEVEL Message API (not the
//! #[interface]/#[proxy] macros - those assume static signatures, which
//! is incompatible with generic config-driven dispatch). docs/DESIGN_BRIEF_V1.md
//! Section 3.3.
//!
//! Responsibilities: connect, RequestName for every configured name on
//! this one connection (docs/DESIGN_BRIEF_V1.md Section 2.1), intercept raw messages,
//! hand off to dispatch.rs.

pub mod bus_proxy;
pub mod dispatch;
pub mod dynamic_object;
pub mod introspect;
pub mod resolver;

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::sync::RwLock;
use tracing::{info, warn};
use zbus::message::Type as MessageType;
pub use zbus::Connection;
use zbus::MessageStream;

use crate::config::schema::Bus;
use crate::config::DispatchTable;
use crate::idle::ActivityTracker;
use crate::telemetry::Telemetry;
use crate::BoxError;

use introspect::InterfaceDesc;

/// The one shared piece of state every subsystem (dispatch.rs,
/// dynamic_object.rs, varlink/server.rs, varlink/streaming.rs) holds a
/// clone of, per docs/DESIGN_BRIEF_V1.md Section 2.1 ("exactly one connection no matter
/// how many objects are registered" / 2.5) and Section 2.3's per-call
/// isolation requirement (this struct is `Arc`'d and cheap to clone-share;
/// the actual dispatch table is behind its own `RwLock` so hot-reload
/// never blocks in-flight calls for long).
pub struct BridgeState {
    pub connection: Connection,
    pub dispatch: RwLock<DispatchTable>,
    /// Parsed `introspection_xml` per bus_name -> per D-Bus interface.
    /// Rebuilt whenever `dispatch` is hot-reloaded.
    pub introspection: RwLock<HashMap<String, HashMap<String, InterfaceDesc>>>,
    pub activity: ActivityTracker,
    pub telemetry: Arc<Telemetry>,
    pub dynamic_objects: dynamic_object::DynamicObjectRegistry,
}

pub async fn connect(bus: Bus) -> zbus::Result<Connection> {
    match bus {
        Bus::Session => Connection::session().await,
        Bus::System => Connection::system().await,
    }
}

fn load_all_introspection(table: &DispatchTable) -> HashMap<String, HashMap<String, InterfaceDesc>> {
    let mut out = HashMap::new();
    for (name, service) in &table.services {
        let Some(xml_path) = &service.introspection_xml else {
            // No introspection XML configured at all - fine in
            // `passthrough` mode (dispatch.rs infers types generically
            // instead), and just means Introspect() won't describe this
            // service's interfaces beyond the built-in ones.
            continue;
        };
        match introspect::load_from_path(xml_path) {
            Ok(ifaces) => {
                out.insert(name.clone(), ifaces);
            }
            Err(e) => {
                warn!(bus_name = %name, error = %e, "failed to load introspection XML, this service's non-passthrough calls will fail type conversion");
            }
        }
    }
    out
}

impl BridgeState {
    pub fn new(connection: Connection, dispatch: DispatchTable, telemetry: Arc<Telemetry>) -> Arc<Self> {
        let introspection = load_all_introspection(&dispatch);
        Arc::new(Self {
            connection,
            dispatch: RwLock::new(dispatch),
            introspection: RwLock::new(introspection),
            activity: ActivityTracker::new(),
            telemetry,
            dynamic_objects: dynamic_object::DynamicObjectRegistry::new(),
        })
    }

    /// `RequestName` for every configured D-Bus name. Called once at
    /// startup (docs/DESIGN_BRIEF_V1.md Section 2.1: "claims whatever name(s) apply").
    pub async fn request_all_names(&self) -> Result<(), BoxError> {
        let table = self.dispatch.read().await;
        for name in table.bus_names() {
            info!(bus_name = name, "requesting D-Bus name");
            self.connection.request_name(name).await?;
        }
        Ok(())
    }

    /// Reconcile a freshly-loaded `DispatchTable` against the current one:
    /// RequestName newly-added names, ReleaseName removed ones, and swap
    /// in the new table + introspection cache. docs/DESIGN_BRIEF_V1.md Section 3.1.
    pub async fn apply_reload(&self, new_table: DispatchTable) -> Result<(), BoxError> {
        let diff = {
            let table = self.dispatch.read().await;
            crate::config::diff_tables(&table, &new_table)
        };
        for name in &diff.added_names {
            info!(bus_name = name, "hot-reload: requesting newly configured name");
            if let Err(e) = self.connection.request_name(name.as_str()).await {
                warn!(bus_name = name, error = %e, "hot-reload: failed to request name");
            }
        }
        for name in &diff.removed_names {
            info!(bus_name = name, "hot-reload: releasing removed name");
            if let Err(e) = self.connection.release_name(name.as_str()).await {
                warn!(bus_name = name, error = %e, "hot-reload: failed to release name");
            }
        }
        let new_introspection = load_all_introspection(&new_table);
        *self.introspection.write().await = new_introspection;
        *self.dispatch.write().await = new_table;
        self.activity.touch();
        Ok(())
    }

    /// Release every configured name and any dynamically-registered ones,
    /// used during clean shutdown (docs/DESIGN_BRIEF_V1.md Section 3.7).
    pub async fn release_all_names(&self) {
        let table = self.dispatch.read().await;
        for name in table.bus_names() {
            let _ = self.connection.release_name(name).await;
        }
    }
}

/// The main D-Bus message intercept loop. Every inbound method call is
/// handed to its own spawned task (dispatch.rs / dynamic_object.rs), so a
/// slow backend for one interface never blocks unrelated traffic
/// (docs/DESIGN_BRIEF_V1.md Sections 2.3, 2.5).
pub async fn run_dispatch_loop(state: Arc<BridgeState>) {
    let mut stream = MessageStream::from(&state.connection);
    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                if message.message_type() != MessageType::MethodCall {
                    // This bridge emits signals (outbound) but does not
                    // currently subscribe to or forward *inbound* D-Bus
                    // signals from other peers - out of scope per
                    // docs/DESIGN_BRIEF_V1.md's Mission (translating calls/signals for
                    // configured interfaces this bridge itself owns).
                    continue;
                }
                let state = state.clone();
                tokio::spawn(async move {
                    dispatch::handle_call(state, message).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "D-Bus connection error, stopping dispatch loop");
                break;
            }
        }
    }
}
