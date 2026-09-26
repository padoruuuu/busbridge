//! Proves a service registered through the app registration protocol
//! (docs/REGISTRATION_PROTOCOL.md) becomes resolvable through the
//! standard `org.varlink.resolver` protocol (dbus/resolver.rs) too - not
//! just reachable as an ordinary D-Bus destination
//! (tests/registration.rs already covers that). A Varlink-native client
//! should be able to find this service by interface name alone, exactly
//! as it would a `[[resolve]]`-configured conf.d service
//! (tests/resolver.rs), with no config file involved at all on either
//! side.
//!
//! Only one `#[tokio::test]` in this file, on purpose - same reasoning as
//! tests/resolver.rs: `BUSBRIDGE_RESOLVER_SOCKET` is a process-global env
//! var, and isolating that to a single test in its own file avoids racing
//! a concurrently-running one.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::io::BufReader;
use tokio::net::UnixStream;

use busbridge::config::schema::Bus;
use busbridge::config::DispatchTable;
use busbridge::control::{self, paths_in, resolve_startup, StartupOutcome};
use busbridge::registration::BusRegistry;
use busbridge::telemetry::Telemetry;
use busbridge::varlink::peer::Frame;
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

async fn call(stream: &mut UnixStream, method: &str, parameters: serde_json::Value) -> VarlinkReply {
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

const ECHO_XML: &str = r#"<node>
  <interface name="org.example.EchoService">
    <method name="Ping">
      <arg name="message" type="s" direction="in"/>
      <arg name="reply" type="s" direction="out"/>
    </method>
  </interface>
</node>"#;

#[tokio::test]
async fn registered_service_is_resolvable_and_callable_through_the_resolver() {
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

    // Same resident setup as tests/registration.rs: a private control
    // socket, a BusRegistry pre-seeded with a BridgeState already
    // connected to the private test bus (sidesteps
    // $DBUS_SESSION_BUS_ADDRESS races - see that file's own comment on
    // why), and both the registration accept loop and the
    // registry-aware resolver listener running against it, exactly as
    // lib.rs's async_main wires them together in production.
    let control_dir = tempfile::tempdir().unwrap();
    let paths = paths_in(control_dir.path());
    let StartupOutcome::Resident(lock) = resolve_startup(&paths, None).await.unwrap() else {
        panic!("expected to become the resident in a fresh control dir");
    };
    let telemetry = Arc::new(Telemetry::load(control_dir.path().join("telemetry.jsonl")));
    let registry = Arc::new(BusRegistry::new(telemetry));
    let connection = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    let state = busbridge::start_bus_on_connection(connection, DispatchTable::default(), Arc::new(Telemetry::load(control_dir.path().join("unused.jsonl"))))
        .await
        .unwrap();
    registry.seed(Bus::Session, state).await;
    tokio::spawn(control::run_control_accept_loop(Some(registry.clone()), lock));
    tokio::spawn(busbridge::dbus::resolver::start_registry_aware_listener(registry));
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Register the service - no conf.d file, no [[resolve]] entry, just
    // the registration frame itself.
    let mut app_conn = UnixStream::connect(&paths.socket_path).await.unwrap();
    let register_req = json!({
        "id": "1",
        "method": "org.busbridge.Registration.Register",
        "parameters": {
            "bus": "session",
            "name": "org.example.EchoService",
            "object_path": "/org/example/EchoService",
            "introspection_xml": ECHO_XML,
            "methods": [{
                "dbus_interface": "org.example.EchoService",
                "dbus_method": "Ping",
                "varlink_method": "org.example.EchoService.Ping",
                "args": ["message"]
            }]
        }
    });
    write_framed(&mut app_conn, &register_req).await.unwrap();
    let ack: Frame = {
        let mut reader = BufReader::new(&mut app_conn);
        read_framed(&mut reader).await.unwrap().unwrap()
    };
    assert!(ack.error.is_none(), "registration should succeed: {ack:?}");

    let (r, mut w) = app_conn.into_split();
    let mut app_reader = BufReader::new(r);
    let app_task = tokio::spawn(async move {
        let call: Frame = read_framed(&mut app_reader).await.unwrap().unwrap();
        assert_eq!(call.method.as_deref(), Some("org.example.EchoService.Ping"));
        assert_eq!(call.parameters.unwrap()["message"], "hi");
        write_framed(&mut w, &Frame { parameters: Some(json!({"reply": "hi back"})), ..Default::default() }).await.unwrap();
    });

    // === Resolve it ===
    let mut client = UnixStream::connect(&resolver_sock).await.unwrap();
    let reply = call(&mut client, "org.varlink.resolver.Resolve", json!({"interface": "org.example.EchoService"})).await;
    assert!(!reply.is_error(), "resolve should succeed: {reply:?}");
    let resolved_address = reply.parameters.unwrap()["address"].as_str().unwrap().to_string();
    assert_eq!(
        resolved_address,
        format!("unix:{}", resolver_sock.display()),
        "a registered service must resolve to the daemon's own address, same as a [[resolve]]-configured one"
    );

    // === Call it, on the same connection, through the resolver's
    // generic "{interface}.{method}" fallback ===
    let reply = call(&mut client, "org.example.EchoService.Ping", json!({"message": "hi"})).await;
    assert!(!reply.is_error(), "direct call through the resolver should succeed: {reply:?}");
    assert_eq!(reply.parameters, Some(json!({"reply": "hi back"})));

    app_task.await.unwrap();
}
