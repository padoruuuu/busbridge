//! Integration test for the app registration protocol
//! (docs/REGISTRATION_PROTOCOL.md): a Varlink-only app connecting to the
//! one universal `busbridge.sock` address and becoming a real D-Bus
//! service, with no conf.d file and no separate `varlink.listen` socket
//! of its own - the thing `src/registration.rs`/`src/varlink/peer.rs`
//! exist for, and genuinely different from both `tests/end_to_end.rs`
//! (a conf.d-configured static mapping) and `tests/sni_tray.rs` (a
//! `[[registrable]]` *dynamic object* under an already-conf.d-configured
//! service).
//!
//! Drives the real `control::run_control_accept_loop` on the real
//! (renamed) control socket, exactly as a real busbridge process would,
//! rather than calling `registration::handle_registration_connection`
//! directly - the "one address, both protocols multiplexed on it"
//! behavior is itself part of what needs proving.
//!
//! Skips cleanly (rather than failing) if `dbus-daemon` isn't available,
//! same convention as every other integration test in this crate.

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
use busbridge::varlink::{read_framed, write_framed};

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
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    std::io::BufRead::read_line(&mut reader, &mut line).ok()?;
    let address = line.trim().to_string();
    if address.is_empty() {
        return None;
    }
    Some((TestBus { child }, address))
}

/// Starts the control accept loop on a fresh, private control socket
/// (its own tempdir, never the real `$XDG_RUNTIME_DIR`), with a
/// `BusRegistry` pre-seeded with a `BridgeState` already connected to the
/// private test bus and an empty dispatch table - exactly what a real
/// conf.d-started resident with no configured services yet looks like.
/// Pre-seeding (rather than letting an app registration lazily start the
/// bus via `BusRegistry::get_or_start`) sidesteps `dbus::connect`'s
/// reliance on `$DBUS_SESSION_BUS_ADDRESS`, a process-global env var that
/// would otherwise race across this file's concurrently-run tests.
async fn start_test_resident(bus_address: &str) -> (tempfile::TempDir, String) {
    let control_dir = tempfile::tempdir().unwrap();
    let paths = paths_in(control_dir.path());
    let StartupOutcome::Resident(lock) = resolve_startup(&paths, None).await.unwrap() else {
        panic!("expected to become the resident in a fresh control dir");
    };

    let telemetry = Arc::new(Telemetry::load(control_dir.path().join("telemetry.jsonl")));
    let registry = Arc::new(BusRegistry::new(telemetry));

    let connection = zbus::conn::Builder::address(bus_address).unwrap().build().await.unwrap();
    let state = busbridge::start_bus_on_connection(connection, DispatchTable::default(), Arc::new(Telemetry::load(control_dir.path().join("unused.jsonl"))))
        .await
        .expect("bridge should start against the private test bus");
    registry.seed(Bus::Session, state).await;

    tokio::spawn(control::run_control_accept_loop(Some(registry), lock));
    tokio::time::sleep(Duration::from_millis(100)).await;

    (control_dir, paths.socket_path.display().to_string())
}

const ECHO_XML: &str = r#"<node>
  <interface name="org.example.EchoService">
    <method name="Ping">
      <arg name="message" type="s" direction="in"/>
      <arg name="reply" type="s" direction="out"/>
    </method>
    <property name="Counter" type="u" access="read"/>
  </interface>
</node>"#;

async fn register(socket_path: &str, bus_name: &str) -> (UnixStream, String) {
    let mut conn = UnixStream::connect(socket_path).await.unwrap();
    let register_req = json!({
        "id": "1",
        "method": "org.busbridge.Registration.Register",
        "parameters": {
            "bus": "session",
            "name": bus_name,
            "object_path": "/org/example/EchoService",
            "introspection_xml": ECHO_XML,
            "methods": [{
                "dbus_interface": "org.example.EchoService",
                "dbus_method": "Ping",
                "varlink_method": "org.example.EchoService.Ping",
                "args": ["message"]
            }],
            "properties": [{
                "dbus_interface": "org.example.EchoService",
                "dbus_property": "Counter",
                "varlink_method": "org.example.EchoService.Counter"
            }]
        }
    });
    write_framed(&mut conn, &register_req).await.unwrap();

    let ack: Frame = {
        let mut reader = BufReader::new(&mut conn);
        read_framed(&mut reader).await.unwrap().unwrap()
    };
    assert!(ack.error.is_none(), "registration should succeed: {ack:?}");
    let object_path = ack.parameters.unwrap()["object_path"].as_str().unwrap().to_string();
    (conn, object_path)
}

#[tokio::test]
async fn registered_app_answers_a_real_dbus_call() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_control_dir, socket_path) = start_test_resident(&bus_address).await;

    let (conn, object_path) = register(&socket_path, "org.example.EchoService1").await;
    let (r, mut w) = conn.into_split();
    let mut reader = BufReader::new(r);

    let app_task = tokio::spawn(async move {
        let call: Frame = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(call.method.as_deref(), Some("org.example.EchoService.Ping"));
        let params = call.parameters.unwrap();
        assert_eq!(params["message"], "hello");
        assert!(params["_dbus_sender"].as_str().unwrap().starts_with(':'));
        write_framed(&mut w, &Frame { parameters: Some(json!({"reply": "hello back"})), ..Default::default() }).await.unwrap();
    });

    let client = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    let reply = client
        .call_method(Some("org.example.EchoService1"), object_path.as_str(), Some("org.example.EchoService"), "Ping", &("hello",))
        .await
        .expect("Ping should round-trip through the registered app");
    let body: (String,) = reply.body().deserialize().unwrap();
    assert_eq!(body.0, "hello back");

    app_task.await.unwrap();
}

#[tokio::test]
async fn registered_app_property_get_round_trips() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_control_dir, socket_path) = start_test_resident(&bus_address).await;

    let (conn, object_path) = register(&socket_path, "org.example.EchoService2").await;
    let (r, mut w) = conn.into_split();
    let mut reader = BufReader::new(r);

    let app_task = tokio::spawn(async move {
        // A declared property's Get forwards as a *bare* call to its own
        // varlink_method - no interface/property wrapper, no parameters
        // at all. See docs/REGISTRATION_PROTOCOL.md's "Properties"
        // section (this is the exact thing that was wrong in this
        // module's first draft - see the commit history / prior
        // conversation turn for the correction).
        let call: Frame = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(call.method.as_deref(), Some("org.example.EchoService.Counter"));
        assert!(call.parameters.is_none() || call.parameters == Some(json!({})));
        write_framed(&mut w, &Frame { parameters: Some(json!(7)), ..Default::default() }).await.unwrap();
    });

    let client = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    let reply = client
        .call_method(
            Some("org.example.EchoService2"),
            object_path.as_str(),
            Some("org.freedesktop.DBus.Properties"),
            "Get",
            &("org.example.EchoService", "Counter"),
        )
        .await
        .expect("Properties.Get should round-trip through the registered app");
    let value: zbus::zvariant::OwnedValue = reply.body().deserialize().unwrap();
    let counter: u32 = value.try_into().unwrap();
    assert_eq!(counter, 7);

    app_task.await.unwrap();
}

#[tokio::test]
async fn disconnecting_releases_the_name() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_control_dir, socket_path) = start_test_resident(&bus_address).await;

    let (conn, _object_path) = register(&socket_path, "org.example.EchoService3").await;

    let client = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    let owner_reply = client
        .call_method(Some("org.freedesktop.DBus"), "/org/freedesktop/DBus", Some("org.freedesktop.DBus"), "GetNameOwner", &("org.example.EchoService3",))
        .await;
    assert!(owner_reply.is_ok(), "name should be owned while the app is connected: {owner_reply:?}");

    drop(conn);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let owner_reply_after = client
        .call_method(Some("org.freedesktop.DBus"), "/org/freedesktop/DBus", Some("org.freedesktop.DBus"), "GetNameOwner", &("org.example.EchoService3",))
        .await;
    assert!(owner_reply_after.is_err(), "name should have been released after the app disconnected, got: {owner_reply_after:?}");
}

const SNI_ITEM_XML: &str = r#"<node>
  <interface name="org.kde.StatusNotifierItem">
    <method name="Activate">
      <arg name="x" type="i" direction="in"/>
      <arg name="y" type="i" direction="in"/>
    </method>
  </interface>
</node>"#;

/// A third party (a completely separate connection, distinct from the
/// registered "watcher" service's own connection) registers a *dynamic
/// object* under a service created through `Register` - the previously-
/// "not done yet" case (docs/REGISTRATION_PROTOCOL.md). Proves the whole
/// chain: `org.busbridge.Registration.RegisterDynamicObject` finds the
/// parent's `RegistrableEntry` across running buses, hands off to the
/// exact same `dbus/dynamic_object.rs::register` the conf.d-driven path
/// already uses, and a real D-Bus call to the resulting object reaches
/// the *third party's* connection - not the parent service's.
#[tokio::test]
async fn third_party_registers_a_dynamic_object_under_a_registered_service() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_control_dir, socket_path) = start_test_resident(&bus_address).await;

    // The "watcher": a registered service that declares one registrable
    // interface, and otherwise does nothing itself.
    let mut watcher_conn = UnixStream::connect(&socket_path).await.unwrap();
    let register_req = json!({
        "id": "1",
        "method": "org.busbridge.Registration.Register",
        "parameters": {
            "bus": "session",
            "name": "org.example.TestWatcher",
            "object_path": "/org/example/TestWatcher",
            "registrables": [{
                "dbus_interface": "org.kde.StatusNotifierItem",
                "path_prefix": "/StatusNotifierItem",
                "introspection_xml": SNI_ITEM_XML,
                "register_via": "unused_for_this_path",
                "id_source": "generated"
            }]
        }
    });
    write_framed(&mut watcher_conn, &register_req).await.unwrap();
    let watcher_ack: Frame = {
        let mut reader = BufReader::new(&mut watcher_conn);
        read_framed(&mut reader).await.unwrap().unwrap()
    };
    assert!(watcher_ack.error.is_none(), "watcher registration should succeed: {watcher_ack:?}");

    // The "item": an unrelated third-party connection registering a
    // dynamic object under the watcher's declared registrable.
    let mut item_conn = UnixStream::connect(&socket_path).await.unwrap();
    let register_item_req = json!({
        "id": "1",
        "method": "org.busbridge.Registration.RegisterDynamicObject",
        "parameters": {
            "bus_name": "org.example.TestWatcher",
            "dbus_interface": "org.kde.StatusNotifierItem"
        }
    });
    write_framed(&mut item_conn, &register_item_req).await.unwrap();
    let item_ack: Frame = {
        let mut reader = BufReader::new(&mut item_conn);
        read_framed(&mut reader).await.unwrap().unwrap()
    };
    assert!(item_ack.error.is_none(), "dynamic object registration should succeed: {item_ack:?}");
    let object_path = item_ack.parameters.unwrap()["object_path"].as_str().unwrap().to_string();
    assert!(object_path.starts_with("/StatusNotifierItem/"), "got {object_path}");

    let (item_read, mut item_write) = item_conn.into_split();
    let mut item_reader = BufReader::new(item_read);
    let item_task = tokio::spawn(async move {
        let call: Frame = read_framed(&mut item_reader).await.unwrap().unwrap();
        assert_eq!(call.method.as_deref(), Some("org.kde.StatusNotifierItem.Activate"));
        let params = call.parameters.unwrap();
        assert_eq!(params["x"], 5);
        assert_eq!(params["y"], 6);
        write_framed(&mut item_write, &Frame { parameters: Some(json!({})), ..Default::default() }).await.unwrap();
    });

    let client = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reply = client
        .call_method(
            Some("org.example.TestWatcher"),
            object_path.as_str(),
            Some("org.kde.StatusNotifierItem"),
            "Activate",
            &(5i32, 6i32),
        )
        .await;
    assert!(reply.is_ok(), "Activate should reach the third party's connection: {reply:?}");

    item_task.await.unwrap();
    drop(watcher_conn); // keep the watcher connection alive until here
}

struct FindTarget;

#[zbus::interface(name = "org.example.FindTarget")]
impl FindTarget {
    async fn ping(&self) -> &str {
        "pong"
    }
}

/// Proves `org.busbridge.Peer.FindObjects` actually walks the tree -
/// not just checks a fixed set of well-known paths - by putting the
/// target object four levels deep under an arbitrary, made-up path no
/// convention would predict, exactly the shape of bug this feature
/// exists to not have (docs/ARCHITECTURE.md's "server-side discovery"
/// rationale).
#[tokio::test]
async fn find_objects_locates_a_deeply_nested_interface() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A real D-Bus service, unrelated to busbridge, exposing the target
    // interface at a deliberately deep, arbitrary path.
    const TARGET_PATH: &str = "/a/b/c/d/TargetObject";
    let _target_conn = zbus::conn::Builder::address(bus_address.as_str())
        .unwrap()
        .name("org.example.FindTargetOwner")
        .unwrap()
        .serve_at(TARGET_PATH, FindTarget)
        .unwrap()
        .build()
        .await
        .unwrap();

    let (_control_dir, socket_path) = start_test_resident(&bus_address).await;

    // Any registered app can call FindObjects - register a minimal one
    // purely to get onto busbridge.sock.
    let (mut conn, _object_path) = register(&socket_path, "org.example.FindObjectsCaller").await;

    let find_req = json!({
        "id": "2",
        "method": "org.busbridge.Peer.FindObjects",
        "parameters": { "interface": "org.example.FindTarget" }
    });
    write_framed(&mut conn, &find_req).await.unwrap();
    let reply: Frame = {
        let mut reader = BufReader::new(&mut conn);
        read_framed(&mut reader).await.unwrap().unwrap()
    };
    assert!(reply.error.is_none(), "FindObjects should succeed: {reply:?}");
    let items = reply.parameters.unwrap()["items"].as_array().unwrap().clone();
    // FindObjects reports the *unique* connection name (":1.x"), not
    // whichever well-known name(s) that connection happens to also own -
    // deliberately (querying by unique identity avoids a race if a
    // well-known name changes hands between the search and using the
    // result), so the useful thing to assert is that the returned
    // (bus_name, object_path) pair actually reaches the target object,
    // not that bus_name string-matches "org.example.FindTargetOwner".
    let found = items.iter().find(|item| item["object_path"] == TARGET_PATH);
    let Some(found) = found else {
        panic!("expected to find {TARGET_PATH} somewhere, got: {items:?}");
    };
    let found_bus_name = found["bus_name"].as_str().unwrap();
    let client = zbus::conn::Builder::address(bus_address.as_str()).unwrap().build().await.unwrap();
    let ping_reply = client
        .call_method(Some(found_bus_name), TARGET_PATH, Some("org.example.FindTarget"), "Ping", &())
        .await
        .expect("the discovered (bus_name, object_path) pair should actually reach the target");
    let body: (String,) = ping_reply.body().deserialize().unwrap();
    assert_eq!(body.0, "pong");
}
