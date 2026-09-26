//! The direct answer to "can an app move entirely to Varlink and still
//! act as a D-Bus client against arbitrary third-party apps": this test
//! builds a fake third-party D-Bus service (standing in for something
//! like Discord's or Steam's own StatusNotifierItem object - a real app
//! this bridge's operator doesn't control and can't add config for ahead of
//! time) using ordinary zbus, then drives it entirely from a "client"
//! that speaks NOTHING but the raw Varlink wire protocol - no zbus, no
//! D-Bus types, not even the bridge's own D-Bus-facing types - through
//! `dbus/bus_proxy.rs`'s generic Call/GetProperty/SetProperty/Subscribe
//! API.
//!
//! If this test passes, an app rewritten to only ever speak Varlink can
//! still: call an arbitrary method on an arbitrary bus name with
//! correctly-typed arguments (recovered via live introspection, not
//! config), read and write an arbitrary property, and receive an
//! arbitrary signal - the complete D-Bus client surface `sni.rs`-style
//! code needs.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value as JsonValue};
use tokio::io::BufReader;
use zbus::interface;

use busbridge::config::load_conf_d;
use busbridge::telemetry::Telemetry;
use busbridge::varlink::{read_framed, write_framed, VarlinkReply, VarlinkRequest};

struct TestBus {
    child: std::process::Child,
}
impl Drop for TestBus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn spawn_test_bus() -> Option<(TestBus, String)> {
    let mut child = Command::new("dbus-daemon")
        .args(["--session", "--print-address", "--nofork"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let mut reader = StdBufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let address = line.trim().to_string();
    if address.is_empty() {
        return None;
    }
    Some((TestBus { child }, address))
}

/// Stands in for a real, unrelated third-party app's D-Bus object (think:
/// a tray application's own `org.kde.StatusNotifierItem`) - built with
/// ordinary zbus, completely independent of this crate, to make sure the
/// test proves something real rather than exercising this crate's own
/// conventions against itself.
struct FakeTrayItem {
    icon_name: Mutex<String>,
}

#[interface(name = "org.example.FakeTrayItem")]
impl FakeTrayItem {
    /// A method with a real, precisely-typed multi-arg signature -
    /// (s, u) in, (s, b) out - deliberately nothing generic inference
    /// could reconstruct on its own, to prove live introspection is what
    /// makes this work.
    async fn activate(&self, action: String, x: u32) -> (String, bool) {
        (format!("did {action} at {x}"), true)
    }

    #[zbus(property)]
    async fn icon_name(&self) -> String {
        self.icon_name.lock().unwrap().clone()
    }

    #[zbus(property)]
    async fn set_icon_name(&self, value: String) {
        *self.icon_name.lock().unwrap() = value;
    }

    #[zbus(signal)]
    async fn new_icon(ctxt: &zbus::SignalContext<'_>, icon_name: String) -> zbus::Result<()>;
}

/// A minimal *pure Varlink* client for the bus-proxy protocol - written
/// using nothing but the same generic NUL-delimited-JSON framing helpers
/// varlink/mod.rs already exposes, deliberately not touching zbus or any
/// D-Bus type at all. This is the shape `sni.rs`, fully ported, would
/// actually use.
struct BusProxyClient {
    stream: tokio::net::UnixStream,
}

impl BusProxyClient {
    async fn connect(addr: &std::path::Path) -> Self {
        Self {
            stream: tokio::net::UnixStream::connect(addr).await.unwrap(),
        }
    }

    async fn call(&mut self, method: &str, parameters: JsonValue) -> VarlinkReply {
        let req = VarlinkRequest {
            method: method.to_string(),
            parameters: Some(parameters),
            more: false,
            oneway: false,
        };
        write_framed(&mut self.stream, &req).await.unwrap();
        let mut reader = BufReader::new(&mut self.stream);
        read_framed(&mut reader).await.unwrap().unwrap()
    }
}

#[tokio::test]
async fn pure_varlink_client_acts_as_a_full_dbus_client_via_bus_proxy() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The fake third-party app: a real, independent D-Bus service the
    // bridge's operator has no config for and no control over.
    let app_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    app_connection
        .object_server()
        .at(
            "/org/example/FakeTrayItem",
            FakeTrayItem {
                icon_name: Mutex::new("initial-icon".to_string()),
            },
        )
        .await
        .unwrap();
    app_connection.request_name("org.example.FakeTrayApp").await.unwrap();

    // The bridge itself, with bus_proxy_listen enabled and nothing else -
    // it doesn't even need to own any D-Bus name for this test, since
    // Call/GetProperty/etc. address arbitrary destinations directly.
    let conf_dir = tempfile::tempdir().unwrap();
    let proxy_sock = conf_dir.path().join("busproxy.sock");
    std::fs::write(
        conf_dir.path().join("proxy.toml"),
        format!(
            r#"
[service]
bus = "session"
name = "org.example.ShimForProxyTest"
object_path = "/org/example/ShimForProxyTest"
passthrough = true

[varlink]
bus_proxy_listen = "unix:{}"
"#,
            proxy_sock.display()
        ),
    )
    .unwrap();

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let shim_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(shim_connection, table, telemetry)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut client = BusProxyClient::connect(&proxy_sock).await;

    // 1. Call - a real, precisely-typed multi-arg method, resolved via
    // live introspection of the fake app, not any config this test wrote.
    let reply = client
        .call(
            "org.busbridge.BusProxy.Call",
            json!({
                "destination": "org.example.FakeTrayApp",
                "path": "/org/example/FakeTrayItem",
                "interface": "org.example.FakeTrayItem",
                "method": "Activate",
                "args": ["click", 42],
            }),
        )
        .await;
    assert!(!reply.is_error(), "Call failed: {reply:?}");
    assert_eq!(
        reply.parameters,
        Some(json!({"reply": ["did click at 42", true]})),
        "Call should recover the real (s, u) in / (s, b) out signature via introspection"
    );

    // 2. GetProperty - reading the fake app's own property, no config.
    let reply = client
        .call(
            "org.busbridge.BusProxy.GetProperty",
            json!({
                "destination": "org.example.FakeTrayApp",
                "path": "/org/example/FakeTrayItem",
                "interface": "org.example.FakeTrayItem",
                "property": "IconName",
            }),
        )
        .await;
    assert!(!reply.is_error(), "GetProperty failed: {reply:?}");
    assert_eq!(reply.parameters, Some(json!({"value": "initial-icon"})));

    // 3. SetProperty - writing it back.
    let reply = client
        .call(
            "org.busbridge.BusProxy.SetProperty",
            json!({
                "destination": "org.example.FakeTrayApp",
                "path": "/org/example/FakeTrayItem",
                "interface": "org.example.FakeTrayItem",
                "property": "IconName",
                "value": "changed-icon",
            }),
        )
        .await;
    assert!(!reply.is_error(), "SetProperty failed: {reply:?}");

    let reply = client
        .call(
            "org.busbridge.BusProxy.GetProperty",
            json!({
                "destination": "org.example.FakeTrayApp",
                "path": "/org/example/FakeTrayItem",
                "interface": "org.example.FakeTrayItem",
                "property": "IconName",
            }),
        )
        .await;
    assert_eq!(reply.parameters, Some(json!({"value": "changed-icon"})));

    // 4. Introspect - raw XML passthrough of the fake app's real, live
    // introspection data.
    let reply = client
        .call(
            "org.busbridge.BusProxy.Introspect",
            json!({
                "destination": "org.example.FakeTrayApp",
                "path": "/org/example/FakeTrayItem",
            }),
        )
        .await;
    assert!(!reply.is_error());
    let xml = reply.parameters.unwrap()["xml"].as_str().unwrap().to_string();
    assert!(xml.contains("org.example.FakeTrayItem"));
    assert!(xml.contains("NewIcon"));

    // 5. Subscribe - a real D-Bus signal from the fake app, observed
    // purely over Varlink.
    let mut sub_client = BusProxyClient::connect(&proxy_sock).await;
    let sub_req = VarlinkRequest {
        method: "org.busbridge.BusProxy.Subscribe".to_string(),
        parameters: Some(json!({
            "sender": "org.example.FakeTrayApp",
            "interface": "org.example.FakeTrayItem",
            "member": "NewIcon",
        })),
        more: true,
        oneway: false,
    };
    write_framed(&mut sub_client.stream, &sub_req).await.unwrap();
    // Give the subscription a moment to actually register on the bus
    // before the fake app emits, to avoid a race against AddMatch.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let ctxt = zbus::SignalContext::new(&app_connection, "/org/example/FakeTrayItem").unwrap();
    FakeTrayItem::new_icon(&ctxt, "pushed-icon".to_string()).await.unwrap();

    let mut sub_reader = BufReader::new(&mut sub_client.stream);
    let event: VarlinkReply = tokio::time::timeout(Duration::from_secs(5), read_framed(&mut sub_reader))
        .await
        .expect("timed out waiting for the subscribed signal")
        .unwrap()
        .unwrap();
    assert!(event.continues);
    let params = event.parameters.unwrap();
    assert_eq!(params["interface"], "org.example.FakeTrayItem");
    assert_eq!(params["member"], "NewIcon");
    assert_eq!(params["args"], json!(["pushed-icon"]));
}
