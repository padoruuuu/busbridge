//! Proves that `passthrough` mode's automatic system-interface-XML
//! discovery (dbus/introspect.rs's `find_system_interface_desc`) actually
//! recovers precise types, multi-value replies, and Properties support -
//! with zero `[[method]]`/`[[property]]` config, by making a real, already
//! -installed-looking interface XML available and confirming the D-Bus
//! call/property access on the other side comes back correctly shaped
//! instead of generically-inferred.
//!
//! This lives in its own file (its own test binary/process) because it's
//! the one place in the test suite that sets the
//! `BUSBRIDGE_INTERFACE_XML_DIRS` env var - keeping it isolated
//! avoids any chance of interaction with concurrently-running `#[test]`
//! functions elsewhere that might also touch process-global state (see
//! tests/end_to_end.rs and tests/passthrough.rs's own comments about why
//! they deliberately avoid `std::env::set_var` for exactly this reason).
//! Only one `#[tokio::test]` function exists in this file, so there is no
//! risk of two tests racing on that env var within this process either.

use std::io::{BufRead, BufReader as StdBufReader};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

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

/// A stand-in for what a real system package would install under
/// `/usr/share/dbus-1/interfaces/`. Declares a method with two distinct
/// typed out-args (something generic inference could never produce) and
/// one property, for an interface name unique to this test so it can
/// never collide with anything else that might get cached during the
/// test run.
const SYSTEM_XML: &str = r#"
<node>
  <interface name="org.example.DiscoveredGreeterXYZ">
    <method name="Greet">
      <arg name="name" type="s" direction="in"/>
      <arg name="greeting" type="s" direction="out"/>
      <arg name="length" type="u" direction="out"/>
    </method>
    <property name="Language" type="s" access="readwrite"/>
  </interface>
</node>
"#;

#[tokio::test]
async fn passthrough_recovers_real_types_via_system_xml_discovery() {
    let Some((_bus_guard, bus_address)) = spawn_test_bus() else {
        eprintln!("skipping: dbus-daemon not available in this environment");
        return;
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stand in for "/usr/share/dbus-1/interfaces/" with a temp dir - the
    // only env var this whole test suite sets, isolated to this one-test
    // file/process (see module doc comment).
    let xml_dir = tempfile::tempdir().unwrap();
    std::fs::write(xml_dir.path().join("org.example.DiscoveredGreeterXYZ.xml"), SYSTEM_XML).unwrap();
    std::env::set_var("BUSBRIDGE_INTERFACE_XML_DIRS", xml_dir.path());

    let conf_dir = tempfile::tempdir().unwrap();
    let backend_sock = conf_dir.path().join("backend.sock");
    std::fs::write(
        conf_dir.path().join("discovered.toml"),
        format!(
            r#"
[service]
bus = "session"
name = "org.example.DiscoveredTest"
object_path = "/org/example/DiscoveredTest"
passthrough = true

[varlink]
backend = "unix:{}"
"#,
            backend_sock.display()
        ),
    )
    .unwrap();

    // Fake Varlink backend: proves the call arrived with REAL named
    // parameters ("name", not "arg0") - only possible because the bridge
    // found the system XML's declared in-arg name.
    let backend_listener = UnixListener::bind(&backend_sock).unwrap();
    let backend_task = tokio::spawn(async move {
        let (stream, _) = backend_listener.accept().await.unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let req: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(req.method, "org.example.DiscoveredGreeterXYZ.Greet");
        let params = req.parameters.unwrap();
        assert_eq!(params["name"], "Ada");
        // Two named return values, matching the system XML's declared
        // out-args exactly.
        write_framed(&mut w, &VarlinkReply::ok(json!({"greeting": "Hello, Ada!", "length": 11})))
            .await
            .unwrap();
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

    // The call itself: expect the exact two-value, precisely-typed reply
    // the system XML declares - (s, u) - not one generically-inferred blob.
    let reply = client
        .call_method(
            Some("org.example.DiscoveredTest"),
            "/org/example/DiscoveredTest",
            Some("org.example.DiscoveredGreeterXYZ"),
            "Greet",
            &("Ada",),
        )
        .await
        .expect("call should succeed with types recovered from the discovered system XML");
    let body: (String, u32) = reply
        .body()
        .deserialize()
        .expect("reply should deserialize as the exact (s, u) the system XML declares");
    assert_eq!(body, ("Hello, Ada!".to_string(), 11));

    backend_task.await.unwrap();

    // Properties.Get, via the naming convention ("{interface}.{property}")
    // - no [[property]] entry anywhere in config.
    std::fs::remove_file(&backend_sock).ok();
    let backend_listener2 = UnixListener::bind(&backend_sock).unwrap();
    let backend_task2 = tokio::spawn(async move {
        let (stream, _) = backend_listener2.accept().await.unwrap();
        let (r, mut w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let req: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(req.method, "org.example.DiscoveredGreeterXYZ.Language");
        write_framed(&mut w, &VarlinkReply::ok(json!({"Language": "en"}))).await.unwrap();
    });

    let prop_reply = client
        .call_method(
            Some("org.example.DiscoveredTest"),
            "/org/example/DiscoveredTest",
            Some("org.freedesktop.DBus.Properties"),
            "Get",
            &("org.example.DiscoveredGreeterXYZ", "Language"),
        )
        .await
        .expect("Properties.Get should work via passthrough's naming convention");
    let value: zbus::zvariant::OwnedValue = prop_reply.body().deserialize().unwrap();
    let language: String = value.try_into().unwrap();
    assert_eq!(language, "en");

    backend_task2.await.unwrap();
}
