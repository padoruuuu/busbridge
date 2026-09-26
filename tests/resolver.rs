//! Proves the standard `org.varlink.resolver` protocol (dbus/resolver.rs)
//! actually works end to end, for both halves it's supposed to cover:
//!
//!   1. A native (non-D-Bus) Varlink service, registered via the
//!      file-based registry - resolving it returns ITS OWN address, and
//!      the client is expected to disconnect and reconnect there (the
//!      daemon never proxies native traffic).
//!   2. A D-Bus-backed interface (`[[resolve]]` config) - resolving it
//!      returns the DAEMON's own address, and the same connection can be
//!      reused to call it directly with named JSON parameters, typed via
//!      live introspection of the real target.
//!
//! Only one `#[tokio::test]` in this file, on purpose: it's the one place
//! besides tests/passthrough_system_xml.rs that sets process-global env
//! vars (`BUSBRIDGE_RESOLVER_SOCKET`, `BUSBRIDGE_REGISTRY_DIR`),
//! and isolating that to a single test in its own file/process avoids any
//! risk of racing a concurrently-running test - see this crate's other
//! integration tests' own comments about why `std::env::set_var` is
//! otherwise avoided.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
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

/// Stands in for a real, independent third-party D-Bus service - same
/// pattern as tests/bus_proxy.rs, built with plain zbus.
struct FakeGreeterApp;
#[interface(name = "org.example.Greeter")]
impl FakeGreeterApp {
    async fn say_hello(&self, name: String) -> (String, u32) {
        (format!("Hello, {name}!"), name.len() as u32)
    }
}

async fn call(stream: &mut tokio::net::UnixStream, method: &str, parameters: serde_json::Value) -> VarlinkReply {
    let req = VarlinkRequest {
        method: method.to_string(),
        parameters: Some(parameters),
        more: false,
        oneway: false,
    };
    write_framed(stream, &req).await.unwrap();
    let mut reader = BufReader::new(stream);
    read_framed(&mut reader).await.unwrap().unwrap()
}

#[tokio::test]
async fn resolver_handles_both_native_and_dbus_backed_interfaces() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resolver_dir = tempfile::tempdir().unwrap();
    let resolver_sock = resolver_dir.path().join("varlink.sock");
    let registry_dir = tempfile::tempdir().unwrap();
    std::env::set_var("BUSBRIDGE_RESOLVER_SOCKET", &resolver_sock);
    std::env::set_var("BUSBRIDGE_REGISTRY_DIR", registry_dir.path());

    // --- Set up the native service side: a fake Varlink-only service
    // that registers itself in the file-based registry directly (no
    // bridge involvement at all - "decoupled": the resolver only reads).
    let native_sock = registry_dir.path().join("native-service.sock");
    let native_listener = tokio::net::UnixListener::bind(&native_sock).unwrap();
    std::fs::write(
        registry_dir.path().join("org.example.NativeThing"),
        format!("unix:{}", native_sock.display()),
    )
    .unwrap();
    let native_task = tokio::spawn(async move {
        // A real native service persistently accepts connections - the
        // resolver's own stale-liveness check (a real connect() probe,
        // see resolver.rs's socket_seems_alive) is itself one such
        // connection, separate from the actual client call that follows.
        loop {
            let (stream, _) = native_listener.accept().await.unwrap();
            let (r, mut w) = tokio::io::split(stream);
            let mut reader = BufReader::new(r);
            let Some(req) = read_framed::<_, VarlinkRequest>(&mut reader).await.unwrap() else {
                continue; // an empty probe connection (e.g. the stale check) - nothing to reply to.
            };
            assert_eq!(req.method, "org.example.NativeThing.Ping");
            write_framed(&mut w, &VarlinkReply::ok(json!({"pong": true}))).await.unwrap();
            break;
        }
    });

    // --- Set up the D-Bus-backed side: a real fake D-Bus app + a
    // [[resolve]] config entry pointing at it.
    let app_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    app_connection
        .object_server()
        .at("/org/example/Greeter", FakeGreeterApp)
        .await
        .unwrap();
    app_connection.request_name("org.example.GreeterApp").await.unwrap();

    let conf_dir = tempfile::tempdir().unwrap();
    std::fs::write(
        conf_dir.path().join("resolve.toml"),
        r#"
[[resolve]]
interface = "org.example.Greeter"
destination = "org.example.GreeterApp"
path = "/org/example/Greeter"
bus = "session"
"#,
    )
    .unwrap();

    let table = load_conf_d(conf_dir.path()).unwrap();
    assert_eq!(table.resolvables.len(), 1);
    assert!(table.services.is_empty(), "a resolve-only file must not register any owned service");

    let telemetry = Arc::new(Telemetry::load(conf_dir.path().join("telemetry.jsonl")));
    let shim_connection = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .build()
        .await
        .unwrap();
    let state = busbridge::start_bus_on_connection(shim_connection, table, telemetry)
        .await
        .unwrap();
    tokio::spawn(busbridge::dbus::resolver::start_listener(vec![state]));
    tokio::time::sleep(Duration::from_millis(150)).await;

    // === 1. Resolve the native interface ===
    let mut client = tokio::net::UnixStream::connect(&resolver_sock).await.unwrap();
    let reply = call(
        &mut client,
        "org.varlink.resolver.Resolve",
        json!({"interface": "org.example.NativeThing"}),
    )
    .await;
    assert!(!reply.is_error(), "resolve should succeed: {reply:?}");
    let native_address = reply.parameters.unwrap()["address"].as_str().unwrap().to_string();
    assert_eq!(
        native_address,
        format!("unix:{}", native_sock.display()),
        "a native service must resolve to ITS OWN address, not the daemon's"
    );

    // Per protocol: disconnect from the daemon, connect directly to the
    // resolved native address, and call it there - the daemon never
    // proxies this traffic.
    drop(client);
    let native_path = native_address.strip_prefix("unix:").unwrap();
    let mut native_client = tokio::net::UnixStream::connect(native_path).await.unwrap();
    let reply = call(&mut native_client, "org.example.NativeThing.Ping", json!({})).await;
    assert!(!reply.is_error());
    assert_eq!(reply.parameters, Some(json!({"pong": true})));
    native_task.await.unwrap();

    // === 2. Resolve the D-Bus-backed interface ===
    let mut client = tokio::net::UnixStream::connect(&resolver_sock).await.unwrap();
    let reply = call(
        &mut client,
        "org.varlink.resolver.Resolve",
        json!({"interface": "org.example.Greeter"}),
    )
    .await;
    assert!(!reply.is_error(), "resolve should succeed: {reply:?}");
    let resolved_address = reply.parameters.unwrap()["address"].as_str().unwrap().to_string();
    assert_eq!(
        resolved_address,
        format!("unix:{}", resolver_sock.display()),
        "a D-Bus-backed interface must resolve to the DAEMON's own address"
    );

    // Per protocol: reuse the SAME connection to call it directly, named
    // JSON parameters, real (s, u) reply shape recovered via live
    // introspection of the fake app - no config beyond the interface ->
    // destination/path mapping above.
    let reply = call(&mut client, "org.example.Greeter.SayHello", json!({"name": "Ada"})).await;
    assert!(!reply.is_error(), "direct call should succeed: {reply:?}");
    // zbus's own auto-generated introspection doesn't name tuple out-args
    // without explicit doc attributes, so these fall back to the generic
    // arg0/arg1 convention (dbus_out_values_to_named_json) - still a real,
    // precisely-TYPED (s, u) reply recovered purely via live introspection,
    // which is what actually matters here.
    assert_eq!(reply.parameters, Some(json!({"arg0": "Hello, Ada!", "arg1": 3})));

    // === 3. Unknown interface -> InterfaceNotFound ===
    let reply = call(
        &mut client,
        "org.varlink.resolver.Resolve",
        json!({"interface": "org.example.NoSuchThing"}),
    )
    .await;
    assert!(reply.is_error());
    assert_eq!(reply.error.as_deref(), Some("org.varlink.resolver.InterfaceNotFound"));
}
