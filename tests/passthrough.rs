//! Proves `passthrough = true` (config/schema.rs) actually works, end to
//! end, against a real private D-Bus session bus and a fake Varlink
//! backend/app - with NO `[[method]]`, `[[signal]]`, `[[property]]`, or
//! `introspection_xml` in the config at all. This is the "you shouldn't
//! have to hand-write a mapping for every method" mode.
//!
//! Skips cleanly (not a failure) if `dbus-daemon` isn't available.

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

/// The whole config for this test: no [[method]]/[[signal]]/[[property]],
/// no introspection_xml - just enough to say "this D-Bus name, this
/// Varlink backend, translate everything automatically."
fn write_passthrough_config(dir: &std::path::Path, varlink_listen: &str, varlink_backend: &str) {
    let toml = format!(
        r#"
[service]
bus = "session"
name = "org.example.PassthroughTest"
object_path = "/org/example/PassthroughTest"
passthrough = true

[varlink]
listen = "{varlink_listen}"
backend = "{varlink_backend}"
"#
    );
    std::fs::write(dir.join("passthrough.toml"), toml).unwrap();
}

#[tokio::test]
async fn passthrough_forwards_dbus_calls_with_zero_mapping_config() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend.sock");
    let listen_sock = conf_dir.path().join("push.sock");
    write_passthrough_config(
        conf_dir.path(),
        &format!("unix:{}", listen_sock.display()),
        &format!("unix:{}", backend_sock.display()),
    );

    // Fake Varlink backend: no config told it anything about this method -
    // it just receives whatever the D-Bus call becomes generically.
    let backend_listener = UnixListener::bind(&backend_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (stream, _) = backend_listener.accept().await.unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let req: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        // Method name is "{interface}.{member}", args are generic arg0, arg1, ...
        assert_eq!(req.method, "org.example.Greeter.SayHello");
        let params = req.parameters.unwrap();
        assert_eq!(params["arg0"], "World");
        write_framed(&mut w, &VarlinkReply::ok(json!("Hello, World!"))).await.unwrap();
    });

    let table = load_conf_d(conf_dir.path()).unwrap();
    assert_eq!(table.services.len(), 1);
    // No [[method]] entries at all - this is the whole point.
    assert_eq!(table.methods.len(), 0);

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
            Some("org.example.PassthroughTest"),
            "/org/example/PassthroughTest",
            Some("org.example.Greeter"),
            "SayHello",
            &("World",),
        )
        .await
        .expect("passthrough call should succeed with zero method mapping config");

    let body: (String,) = reply.body().deserialize().unwrap();
    assert_eq!(body.0, "Hello, World!");

    backend_task.await.unwrap();
}

#[tokio::test]
async fn passthrough_translates_pushed_events_with_zero_mapping_config() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend2.sock");
    let listen_sock = conf_dir.path().join("push2.sock");
    write_passthrough_config(
        conf_dir.path(),
        &format!("unix:{}", listen_sock.display()),
        &format!("unix:{}", backend_sock.display()),
    );

    let table = load_conf_d(conf_dir.path()).unwrap();
    assert_eq!(table.signals.len(), 0); // no [[signal]] entries either.
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
            &("type='signal',interface='org.example.Notifier',member='Pinged'",),
        )
        .await
        .unwrap();
    let mut stream = zbus::MessageStream::from(&monitor);

    let mut push_conn = tokio::net::UnixStream::connect(&listen_sock).await.unwrap();
    let push_req = VarlinkRequest {
        method: "org.example.Notifier.Pinged".to_string(),
        parameters: Some(json!({"from": "somewhere", "count": 3})),
        more: false,
        oneway: false,
    };
    write_framed(&mut push_conn, &push_req).await.unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(msg)) = stream.next().await {
            if msg.header().member().map(|m| m.as_str()) == Some("Pinged") {
                // Passthrough wraps the whole pushed params object as a
                // single inferred a{sv} argument.
                let body: (std::collections::HashMap<String, zbus::zvariant::OwnedValue>,) =
                    msg.body().deserialize().unwrap();
                return body.0;
            }
        }
        panic!("stream ended before signal arrived");
    })
    .await
    .expect("timed out waiting for the passthrough-translated D-Bus signal");

    let from: String = found.get("from").unwrap().try_clone().unwrap().try_into().unwrap();
    assert_eq!(from, "somewhere");
}
