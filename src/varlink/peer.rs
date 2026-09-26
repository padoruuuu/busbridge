//! `PeerConnection`: one live, app-initiated Varlink connection acting as
//! the entire "backend" for a service created through the app
//! registration protocol (`src/registration.rs`,
//! `docs/REGISTRATION_PROTOCOL.md`), instead of `varlink.backend` being
//! dialed fresh per call the way conf.d-loaded services work
//! (`varlink/client.rs::VarlinkConnection`).
//!
//! This is deliberately a *separate* type from
//! `dbus/dynamic_object.rs::DynamicObject`'s own `Outbound`/`call()`,
//! even though the bridge->app direction is structurally almost
//! identical (write a request frame, match the next reply-shaped frame
//! FIFO). Two reasons for not sharing:
//!   1. `dynamic_object.rs` is existing, tested code with real production
//!      behavior riding on it; touching it to generalize for a second use
//!      case is a correctness risk for zero behavioral gain, given this
//!      module can't share much of it anyway (see point 2).
//!   2. A registered service's connection is genuinely bidirectionally
//!      multiplexed in a way a dynamic object's never is: the *app* can
//!      also initiate requests that need a reply (arbitrary outbound
//!      D-Bus calls, subscribe, unsubscribe - see
//!      `docs/REGISTRATION_PROTOCOL.md`), concurrently with the bridge
//!      forwarding inbound D-Bus calls the other way. `DynamicObject`
//!      only ever needs the bridge->app direction. Modeling both directions
//!      in one type, with one shared frame shape, is simpler than trying
//!      to retrofit that onto `DynamicObject`.
//!
//! Wire-level frame shape (`Frame` below) and the full protocol this
//! implements are documented in `docs/REGISTRATION_PROTOCOL.md` - this
//! module is the mechanism, that document is the contract.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{oneshot, Mutex};

use crate::errors::VarlinkError;
use crate::idle::ActivityGuard;
use crate::varlink::client::CallError;

/// The one frame shape used for every message on a registration
/// connection, in both directions, after the initial registration
/// handshake. Which fields are present (not their mere JSON type, since
/// `parameters` is present in almost every case) is what distinguishes
/// the four kinds of traffic that can appear on this connection - see
/// `docs/REGISTRATION_PROTOCOL.md`'s "frame shapes" section for the
/// worked examples this table summarizes:
///
/// | id  | method | subscription_id | meaning                                          |
/// |-----|--------|------------------|---------------------------------------------------|
/// | yes | yes    | no               | app -> bridge: a `org.busbridge.Peer.*` request   |
/// | yes | no     | no               | bridge -> app: reply to the above                 |
/// | no  | yes    | no               | app -> bridge: a push (signal/property event)     |
/// | no  | yes    | no               | bridge -> app: an inbound D-Bus call to forward    |
/// | no  | no     | no               | app -> bridge: reply to the row above (FIFO)       |
/// | no  | no     | yes              | bridge -> app: a subscription event                |
///
/// The two "no/yes/no" rows are only ambiguous in the abstract - in
/// practice each direction only ever produces one of them (an app never
/// receives a push, a bridge never forwards a reply-shaped push), so
/// each side's read loop only needs to handle the three rows real for
/// its own direction. `PeerConnection` (the bridge side) implements the
/// first, third, and fifth rows; `examples/registration_client.rs` (the
/// reference client, the app side) implements the second, fourth, and
/// sixth.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Frame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Frame {
    pub fn push(method: impl Into<String>, parameters: Option<JsonValue>) -> Self {
        Self {
            method: Some(method.into()),
            parameters,
            ..Default::default()
        }
    }

    pub fn subscription_event(subscription_id: impl Into<String>, parameters: JsonValue) -> Self {
        Self {
            subscription_id: Some(subscription_id.into()),
            parameters: Some(parameters),
            ..Default::default()
        }
    }

    pub fn reply_ok(id: JsonValue, parameters: JsonValue) -> Self {
        Self {
            id: Some(id),
            parameters: Some(parameters),
            ..Default::default()
        }
    }

    pub fn reply_err(id: JsonValue, error: impl Into<String>, parameters: JsonValue) -> Self {
        Self {
            id: Some(id),
            error: Some(error.into()),
            parameters: Some(parameters),
            ..Default::default()
        }
    }
}

struct Outbound {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    /// FIFO queue for the bridge->app direction (row 1/row 5 above are
    /// handled elsewhere; this is specifically "we sent a `method`+no-id
    /// call, who do we wake when the next no-method-no-id reply frame
    /// arrives"). Exactly `dynamic_object.rs::Outbound`'s `pending`
    /// field, same reasoning: only one bridge->app call is ever in
    /// flight at a time per connection, so FIFO position alone
    /// disambiguates without needing an id on this side too.
    pending_bridge_calls: VecDeque<oneshot::Sender<Frame>>,
}

/// One registered service's live connection. Constructed by
/// `registration.rs` right after a successful `Register` call; the read
/// loop that feeds `pending_bridge_calls` and dispatches pushes/app-
/// initiated requests also lives in `registration.rs` (it needs access
/// to `BridgeState` for pushes and outbound calls, which this type
/// deliberately doesn't hold, to keep it a pure write+correlate
/// primitive that doesn't need to know about D-Bus at all).
pub struct PeerConnection {
    outbound: Mutex<Outbound>,
    next_subscription_id: AtomicU64,
    /// subscription_id -> the background task streaming that
    /// subscription's events (see `registration.rs::handle_peer_subscribe`).
    /// Tracked here (not just left to run until the connection itself
    /// closes) so `org.busbridge.Peer.Unsubscribe` can cancel one
    /// subscription without tearing down the whole connection.
    subscriptions: Mutex<std::collections::HashMap<String, tokio::task::JoinHandle<()>>>,
    _hold: ActivityGuard,
}

impl std::fmt::Debug for PeerConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnection").finish_non_exhaustive()
    }
}

const DEFAULT_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl PeerConnection {
    pub fn new(writer: Box<dyn AsyncWrite + Unpin + Send>, hold: ActivityGuard) -> Arc<Self> {
        Arc::new(Self {
            outbound: Mutex::new(Outbound {
                writer,
                pending_bridge_calls: VecDeque::new(),
            }),
            next_subscription_id: AtomicU64::new(0),
            subscriptions: Mutex::new(std::collections::HashMap::new()),
            _hold: hold,
        })
    }

    pub fn next_subscription_id(&self) -> String {
        format!("sub-{}", self.next_subscription_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Track a subscription's background event-streaming task so
    /// `cancel_subscription` can abort it later. Called right after
    /// spawning it.
    pub async fn register_subscription(&self, id: String, handle: tokio::task::JoinHandle<()>) {
        self.subscriptions.lock().await.insert(id, handle);
    }

    /// Aborts and forgets a subscription's background task. Returns
    /// `false` if `id` wasn't a live subscription on this connection
    /// (already cancelled, or never existed) - callers use this to shape
    /// `Unsubscribe`'s reply/error.
    pub async fn cancel_subscription(&self, id: &str) -> bool {
        match self.subscriptions.lock().await.remove(id) {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
    }

    async fn write_frame(&self, frame: &Frame) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(frame)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        bytes.push(0);
        let mut outbound = self.outbound.lock().await;
        outbound.writer.write_all(&bytes).await?;
        outbound.writer.flush().await
    }

    /// Forward an inbound D-Bus call (or a `org.freedesktop.DBus.
    /// Properties.Get/Set/GetAll`, using the same
    /// `"{interface}.{member}"` convention as everywhere else in this
    /// crate) to the app and wait for its reply. Mirrors
    /// `VarlinkConnection::call`'s signature exactly (same `CallError`
    /// type) so dispatch.rs's `call_backend` helper can switch between
    /// dialing a backend and calling through a peer with no other code
    /// changes - see dispatch.rs.
    pub async fn call(&self, method: &str, parameters: Option<JsonValue>) -> Result<JsonValue, CallError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut outbound = self.outbound.lock().await;
            outbound.pending_bridge_calls.push_back(tx);
            let frame = Frame::push(method, parameters);
            let bytes = serde_json::to_vec(&frame).map_err(|e| CallError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
            let mut bytes = bytes;
            bytes.push(0);
            if let Err(e) = outbound.writer.write_all(&bytes).await {
                outbound.pending_bridge_calls.pop_back();
                return Err(CallError::Io(e));
            }
            if let Err(e) = outbound.writer.flush().await {
                outbound.pending_bridge_calls.pop_back();
                return Err(CallError::Io(e));
            }
        }
        let reply = tokio::time::timeout(DEFAULT_CALL_TIMEOUT, rx)
            .await
            .map_err(|_| CallError::ProtocolClosed)?
            .map_err(|_| CallError::ProtocolClosed)?;
        match reply.error {
            Some(name) => Err(CallError::Remote(VarlinkError {
                name,
                parameters: reply.parameters,
            })),
            None => Ok(reply.parameters.unwrap_or_else(|| serde_json::json!({}))),
        }
    }

    /// Called by the read loop (`registration.rs`) when a no-method,
    /// no-subscription-id frame arrives: it's a reply to whichever
    /// bridge->app call is longest-pending. Silently drops it (with the
    /// caller expected to have already logged) if nothing is pending -
    /// a reply to a call that already timed out, most likely.
    pub async fn resolve_bridge_reply(&self, frame: Frame) {
        let sender = {
            let mut outbound = self.outbound.lock().await;
            outbound.pending_bridge_calls.pop_front()
        };
        if let Some(sender) = sender {
            let _ = sender.send(frame);
        }
    }

    /// Send an app->bridge request's reply back with the same `id` the
    /// app used - called from `registration.rs` once it's finished
    /// handling a `Call`/`Subscribe`/`Unsubscribe` request.
    pub async fn reply(&self, frame: Frame) -> std::io::Result<()> {
        self.write_frame(&frame).await
    }

    /// Push a fire-and-forget frame (a subscription event) with no reply
    /// expected.
    pub async fn push(&self, frame: Frame) -> std::io::Result<()> {
        self.write_frame(&frame).await
    }
}
