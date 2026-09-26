//! Integration test for the *tray item* half of SNI support: a
//! Varlink-only app registering itself as a `[[registrable]]` dynamic
//! D-Bus object (docs/ARCHITECTURE.md Section 4), as opposed to the
//! watcher-side static mapping already covered by `tests/end_to_end.rs`.
//! This is the direction the project's own name change is about: a
//! Varlink-native tray app that appears to legacy D-Bus panels exactly
//! like a normal D-Bus tray application.
//!
//! Exercises `src/dbus/dynamic_object.rs` end to end, against a real
//! (private, throwaway) D-Bus session bus and a fake Varlink-native tray
//! item app - not mocks of our own code:
//!
//!   1. `dbus_call_is_forwarded_to_the_registered_tray_item` - the app
//!      registers, then a real D-Bus client calls `Activate(x, y)` on
//!      the resulting object; the bridge forwards it to the app over the
//!      app's own registration connection and relays its reply back as
//!      a real D-Bus method reply.
//!   2. `varlink_push_from_tray_item_becomes_a_dbus_signal` - the app
//!      pushes a `NewStatus` event over that connection; a real D-Bus
//!      signal subscriber sees a genuine
//!      `org.kde.StatusNotifierItem.NewStatus` signal.
//!   3. `dbus_properties_get_on_tray_item_forwards_to_the_app` - a real
//!      D-Bus client calls `org.freedesktop.DBus.Properties.Get` on the
//!      object; the bridge forwards it using the
//!      `org.freedesktop.DBus.Properties.{member}` naming convention
//!      documented in `dynamic_object.rs`'s module doc comment, and the
//!      app's answer comes back as a correctly-typed D-Bus variant.
//!
//! Skips cleanly (rather than failing) if `dbus-daemon` isn't available,
//! same convention as `tests/end_to_end.rs`.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::json;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::oneshot;

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

const SNI_ITEM_XML: &str = include_str!("../docs/sni-item.xml.example");

/// One `[service]` + one `[[registrable]]` block and nothing else - no
/// static `[[method]]`/`[[signal]]`/`[[property]]` entries at all. This
/// is exactly the shape a real "Varlink-native tray host" deployment
/// uses: the service exists purely so dynamically-registered items have
/// somewhere to live (docs/ARCHITECTURE.md Section 4, direction 3).
fn write_test_config(dir: &std::path::Path, registration_listen: &str, bus_name: &str) {
    std::fs::write(dir.join("sni-item.xml"), SNI_ITEM_XML).unwrap();
    let toml = format!(
        r#"
[service]
bus = "session"
name = "{bus_name}"
object_path = "/unused"
idle_timeout_secs = 300

[varlink]
listen = "{registration_listen}"

[[registrable]]
dbus_interface = "org.kde.StatusNotifierItem"
path_prefix = "/StatusNotifierItem"
introspection_xml = "sni-item.xml"
register_via = "org.example.tray.RegisterItem"
id_source = "generated"
"#
    );
    std::fs::write(dir.join("sni-item.toml"), toml).unwrap();
}

#[tokio::test]
async fn dbus_call_is_forwarded_to_the_registered_tray_item() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    const BUS_NAME: &str = "org.example.TestTrayItemHost1";
    let conf_dir = tempfile::tempdir().unwrap();
    let registration_sock = conf_dir.path().join("register.sock");
    write_test_config(conf_dir.path(), &format!("unix:{}", registration_sock.display()), BUS_NAME);

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = std::sync::Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (path_tx, path_rx) = oneshot::channel::<String>();

    // Fake Varlink-native tray item app: register, hand back the
    // allocated object path, then wait for exactly one forwarded call
    // and answer it.
    let app_task = tokio::spawn(async move {
        let mut conn = UnixStream::connect(&registration_sock).await.unwrap();
        let register_req = VarlinkRequest {
            method: "org.example.tray.RegisterItem".to_string(),
            parameters: None,
            more: false,
            oneway: false,
        };
        write_framed(&mut conn, &register_req).await.unwrap();

        let (r, mut w) = conn.into_split();
        let mut reader = BufReader::new(r);
        let ack: VarlinkReply = read_framed(&mut reader).await.unwrap().unwrap();
        assert!(!ack.is_error(), "registration should succeed: {ack:?}");
        let object_path = ack.parameters.unwrap()["object_path"]
            .as_str()
            .expect("ack should carry the allocated object_path")
            .to_string();
        path_tx.send(object_path).unwrap();

        let call: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(call.method, "org.kde.StatusNotifierItem.Activate");
        let params = call.parameters.unwrap();
        assert_eq!(params["x"], 10);
        assert_eq!(params["y"], 20);
        assert!(
            params["_dbus_sender"].as_str().unwrap().starts_with(':'),
            "forwarded call should carry the caller's D-Bus sender"
        );
        write_framed(&mut w, &VarlinkReply::ok(json!({}))).await.unwrap();
    });

    let object_path = path_rx.await.expect("app task should report its object path");
    assert!(
        object_path.starts_with("/StatusNotifierItem/"),
        "object path should live under the registrable's path_prefix, got {object_path}"
    );
    // The registration ack is written slightly before the bridge finishes
    // inserting the object into its dynamic-object registry
    // (dynamic_object.rs's `register()`); give it a moment so this test
    // isn't racing that insert.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let reply = client
        .call_method(
            Some(BUS_NAME),
            object_path.as_str(),
            Some("org.kde.StatusNotifierItem"),
            "Activate",
            &(10i32, 20i32),
        )
        .await;
    assert!(reply.is_ok(), "Activate should round-trip through the registered tray item: {reply:?}");

    app_task.await.unwrap();
}

#[tokio::test]
async fn varlink_push_from_tray_item_becomes_a_dbus_signal() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    const BUS_NAME: &str = "org.example.TestTrayItemHost2";
    let conf_dir = tempfile::tempdir().unwrap();
    let registration_sock = conf_dir.path().join("register.sock");
    write_test_config(conf_dir.path(), &format!("unix:{}", registration_sock.display()), BUS_NAME);

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = std::sync::Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (path_tx, path_rx) = oneshot::channel::<String>();
    // Lets the main test tell the app "the monitor is subscribed now, go
    // ahead and push" - precise instead of racing on a fixed sleep.
    let (go_tx, go_rx) = oneshot::channel::<()>();

    let app_task = tokio::spawn(async move {
        let mut conn = UnixStream::connect(&registration_sock).await.unwrap();
        let register_req = VarlinkRequest {
            method: "org.example.tray.RegisterItem".to_string(),
            parameters: None,
            more: false,
            oneway: false,
        };
        write_framed(&mut conn, &register_req).await.unwrap();

        let (r, mut w) = conn.into_split();
        let mut reader = BufReader::new(r);
        let ack: VarlinkReply = read_framed(&mut reader).await.unwrap().unwrap();
        let object_path = ack.parameters.unwrap()["object_path"].as_str().unwrap().to_string();
        path_tx.send(object_path).unwrap();

        go_rx.await.unwrap();
        let push = VarlinkRequest {
            // Bare member name, NOT "{interface}.{member}" - that prefixed
            // form is the convention for calls forwarded *to* the app
            // (handle_dynamic_call), not for pushes *from* it. A
            // registrable connection is already scoped to exactly one
            // interface, so handle_push (dynamic_object.rs) looks pushed
            // events up in that interface's signal table by bare name -
            // see its doc comment. Using the prefixed form here (as a
            // first draft of this test did) makes the lookup miss
            // silently and the signal never gets emitted.
            method: "NewStatus".to_string(),
            parameters: Some(json!({"status": "Active"})),
            more: false,
            oneway: false,
        };
        write_framed(&mut w, &push).await.unwrap();
        // Keep the reader alive for a moment so the bridge doesn't see a
        // disconnect and tear the object down before the test finishes
        // asserting on the signal it just caused.
        let _ = tokio::time::timeout(Duration::from_millis(500), read_framed::<_, VarlinkRequest>(&mut reader)).await;
    });

    let object_path = path_rx.await.expect("app task should report its object path");

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
            &(format!(
                "type='signal',interface='org.kde.StatusNotifierItem',member='NewStatus',path='{object_path}'"
            ),),
        )
        .await
        .unwrap();
    let mut stream = zbus::MessageStream::from(&monitor);

    go_tx.send(()).unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = stream.next().await {
            if msg.header().member().map(|m| m.as_str()) == Some("NewStatus") {
                let body: (String,) = msg.body().deserialize().unwrap();
                return body.0;
            }
        }
        panic!("stream ended before the signal arrived");
    })
    .await
    .expect("timed out waiting for the translated D-Bus signal");

    assert_eq!(found, "Active");
    app_task.await.unwrap();
}

#[tokio::test]
async fn dbus_properties_get_on_tray_item_forwards_to_the_app() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    const BUS_NAME: &str = "org.example.TestTrayItemHost3";
    let conf_dir = tempfile::tempdir().unwrap();
    let registration_sock = conf_dir.path().join("register.sock");
    write_test_config(conf_dir.path(), &format!("unix:{}", registration_sock.display()), BUS_NAME);

    let table = load_conf_d(conf_dir.path()).unwrap();
    let telemetry = std::sync::Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let _state = busbridge::start_bus_on_connection(connection, table, telemetry)
        .await
        .expect("bridge should start against the private test bus");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (path_tx, path_rx) = oneshot::channel::<String>();

    let app_task = tokio::spawn(async move {
        let mut conn = UnixStream::connect(&registration_sock).await.unwrap();
        let register_req = VarlinkRequest {
            method: "org.example.tray.RegisterItem".to_string(),
            parameters: None,
            more: false,
            oneway: false,
        };
        write_framed(&mut conn, &register_req).await.unwrap();

        let (r, mut w) = conn.into_split();
        let mut reader = BufReader::new(r);
        let ack: VarlinkReply = read_framed(&mut reader).await.unwrap().unwrap();
        let object_path = ack.parameters.unwrap()["object_path"].as_str().unwrap().to_string();
        path_tx.send(object_path).unwrap();

        let call: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        // Same naming convention as an ordinary interface method, with
        // org.freedesktop.DBus.Properties as the prefix - see the module
        // doc comment on src/dbus/dynamic_object.rs.
        assert_eq!(call.method, "org.freedesktop.DBus.Properties.Get");
        let params = call.parameters.unwrap();
        assert_eq!(params["interface_name"], "org.kde.StatusNotifierItem");
        assert_eq!(params["property_name"], "Status");
        write_framed(&mut w, &VarlinkReply::ok(json!({"value": "Active"}))).await.unwrap();
    });

    let object_path = path_rx.await.expect("app task should report its object path");
    // See the identical comment in dbus_call_is_forwarded_to_the_registered_tray_item.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let reply = client
        .call_method(
            Some(BUS_NAME),
            object_path.as_str(),
            Some("org.freedesktop.DBus.Properties"),
            "Get",
            &("org.kde.StatusNotifierItem", "Status"),
        )
        .await
        .expect("Properties.Get should round-trip through the registered tray item");

    let value: zbus::zvariant::OwnedValue = reply.body().deserialize().unwrap();
    let status: String = value.try_into().unwrap();
    assert_eq!(status, "Active");

    app_task.await.unwrap();
}
