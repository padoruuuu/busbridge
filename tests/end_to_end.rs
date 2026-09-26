//! End-to-end integration test: proves the D-Bus <-> Varlink translation
//! actually works in both directions, against a real (private, throwaway)
//! D-Bus session bus and a fake Varlink backend/app - not mocks of our own
//! code. This is the test docs/DESIGN_BRIEF_V1.md Section 7 calls for beyond unit tests:
//! "an integration test using a real (but test-only) D-Bus session bus
//! (dbus-launch or dbus-run-session) and a fake Varlink backend."
//!
//! Covers:
//!   1. D-Bus call -> Varlink call -> D-Bus reply (docs/DESIGN_BRIEF_V1.md Section 2.3).
//!   2. Varlink push -> D-Bus signal (docs/DESIGN_BRIEF_V1.md Section 2.2).
//!
//! Skips cleanly (rather than failing) if `dbus-daemon` isn't available in
//! the environment running the tests, since that's an environment
//! precondition, not something this crate controls.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::json;
use tokio::io::BufReader;
use tokio::net::UnixListener;

use busbridge::config::load_conf_d;
use busbridge::telemetry::Telemetry;
use busbridge::varlink::{read_framed, write_framed, VarlinkReply, VarlinkRequest};

/// Spawns a private `dbus-daemon --session` for the duration of the test
/// and returns (its address, a handle that kills it on drop).
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
    // Leak the reader's stdout handle by not holding onto `reader` (the
    // child keeps running with its stdout pipe now unread-from, which is
    // fine - dbus-daemon doesn't need us to keep draining it).
    Some((TestBus { child }, address))
}

const SNI_WATCHER_XML: &str = include_str!("../docs/sni-watcher.xml.example");

fn write_test_config(dir: &std::path::Path, varlink_listen: &str, varlink_backend: &str) {
    std::fs::write(dir.join("sni-watcher.xml"), SNI_WATCHER_XML).unwrap();
    let toml = format!(
        r#"
[service]
bus = "session"
name = "org.example.TestWatcher"
object_path = "/StatusNotifierWatcher"
introspection_xml = "sni-watcher.xml"
idle_timeout_secs = 300

[varlink]
listen = "{varlink_listen}"
backend = "{varlink_backend}"

[[method]]
dbus_interface = "org.kde.StatusNotifierWatcher"
dbus_method = "RegisterStatusNotifierItem"
varlink_method = "org.example.tray.Register"
args = ["service"]

[[signal]]
dbus_interface = "org.kde.StatusNotifierWatcher"
dbus_signal = "StatusNotifierItemRegistered"
direction = "inbound_push"
"#
    );
    std::fs::write(dir.join("sni-watcher.toml"), toml).unwrap();
}

#[tokio::test]
async fn dbus_call_is_translated_to_varlink_and_back() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    // Give the private bus a moment to be ready to accept connections.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend.sock");
    let listen_sock = conf_dir.path().join("push.sock");
    write_test_config(
        conf_dir.path(),
        &format!("unix:{}", listen_sock.display()),
        &format!("unix:{}", backend_sock.display()),
    );

    // Fake Varlink backend: accept exactly one call, assert its shape,
    // reply successfully.
    let backend_listener = UnixListener::bind(&backend_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (stream, _) = backend_listener.accept().await.unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let req: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(req.method, "org.example.tray.Register");
        let params = req.parameters.unwrap();
        assert_eq!(params["service"], "org.example.MyTrayApp");
        // `_dbus_sender` is also present automatically - not asserted
        // here (see dbus_sender_is_automatically_included_in_call_params
        // for that), just tolerated so this test isn't coupled to it.
        write_framed(&mut w, &VarlinkReply::ok(json!({}))).await.unwrap();
    });

    // Start the bridge itself against the private test bus - connecting by
    // explicit address (not via dbus::connect's env-var-based
    // Connection::session()) so this test never touches process-global
    // state that a concurrently-running test could race on.
    let table = load_conf_d(conf_dir.path()).unwrap();
    assert_eq!(table.services.len(), 1, "test config should have loaded exactly one service");
    let telemetry = Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let shim_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let state = busbridge::start_bus_on_connection(shim_connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");

    // Give the varlink listener + name registration a moment to settle.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Act as an ordinary D-Bus client: call the real D-Bus method.
    let client = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let reply = client
        .call_method(
            Some("org.example.TestWatcher"),
            "/StatusNotifierWatcher",
            Some("org.kde.StatusNotifierWatcher"),
            "RegisterStatusNotifierItem",
            &("org.example.MyTrayApp",),
        )
        .await;

    assert!(
        reply.is_ok(),
        "D-Bus call should succeed via the varlink round trip, got: {reply:?}"
    );

    backend_task.await.unwrap();
    state.activity.touch();
}

#[tokio::test]
async fn varlink_push_is_translated_to_a_dbus_signal() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend2.sock");
    let listen_sock = conf_dir.path().join("push2.sock");
    write_test_config(
        conf_dir.path(),
        &format!("unix:{}", listen_sock.display()),
        &format!("unix:{}", backend_sock.display()),
    );
    // No outbound calls happen in this test, but bind the backend socket's
    // parent regardless isn't necessary since nothing connects to it here.

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let shim_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(shim_connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");

    tokio::time::sleep(Duration::from_millis(150)).await;

    // A second, independent client subscribes to the signal before the
    // push happens.
    let monitor = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    monitor
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "AddMatch",
            &("type='signal',interface='org.kde.StatusNotifierWatcher',member='StatusNotifierItemRegistered'",),
        )
        .await
        .unwrap();
    let mut stream = zbus::MessageStream::from(&monitor);

    // Act as a Varlink backend pushing an event over the listen socket.
    let mut push_conn = tokio::net::UnixStream::connect(&listen_sock).await.unwrap();
    let push_req = VarlinkRequest {
        method: "org.kde.StatusNotifierWatcher.StatusNotifierItemRegistered".to_string(),
        parameters: Some(json!({"service": "org.example.PushedApp"})),
        more: false,
        oneway: false,
    };
    write_framed(&mut push_conn, &push_req).await.unwrap();

    // Wait for the signal to arrive on the monitor connection.
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = stream.next().await {
            if msg.header().member().map(|m| m.as_str()) == Some("StatusNotifierItemRegistered") {
                let body: (String,) = msg.body().deserialize().unwrap();
                return body.0;
            }
        }
        panic!("stream ended before signal arrived");
    })
    .await
    .expect("timed out waiting for the translated D-Bus signal");

    assert_eq!(found, "org.example.PushedApp");
}

#[tokio::test]
async fn dbus_sender_is_automatically_included_in_call_params() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend3.sock");
    let listen_sock = conf_dir.path().join("push3.sock");
    write_test_config(
        conf_dir.path(),
        &format!("unix:{}", listen_sock.display()),
        &format!("unix:{}", backend_sock.display()),
    );

    let backend_listener = UnixListener::bind(&backend_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (stream, _) = backend_listener.accept().await.unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let req: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        let params = req.parameters.unwrap();
        // The regular declared arg is still there...
        assert_eq!(params["service"], "org.example.MyTrayApp");
        // ...and so is the caller's D-Bus sender, with no config needed to
        // opt in - unique bus names always start with ':'.
        let sender = params["_dbus_sender"].as_str().expect("_dbus_sender should be present");
        assert!(sender.starts_with(':'), "expected a unique bus name, got {sender:?}");
        write_framed(&mut w, &VarlinkReply::ok(serde_json::json!({}))).await.unwrap();
    });

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let shim_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(shim_connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let reply = client
        .call_method(
            Some("org.example.TestWatcher"),
            "/StatusNotifierWatcher",
            Some("org.kde.StatusNotifierWatcher"),
            "RegisterStatusNotifierItem",
            &("org.example.MyTrayApp",),
        )
        .await;
    assert!(reply.is_ok(), "call should succeed: {reply:?}");

    backend_task.await.unwrap();
}
