//! Reference client for the busbridge app registration protocol
//! (docs/REGISTRATION_PROTOCOL.md). Deliberately minimal and
//! dependency-light (std + serde_json, blocking I/O, no async runtime):
//! the point of this file is to be short enough to read start to finish
//! and port to another language, not to be a production-quality client
//! library. Every protocol-relevant line has a comment; general Rust
//! boilerplate doesn't.
//!
//! Run with `cargo run --example registration_client` against a running
//! busbridge instance. Registers `org.example.EchoService` with one
//! method (`Ping`, echoes its argument back) and one property
//! (`Counter`, incrementing on each Ping), then serves calls until
//! Ctrl-C - closing the connection is the entire teardown (see the
//! protocol doc's "Teardown" section), so there's nothing else to clean
//! up on exit.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

/// Every frame on the wire is a JSON object followed by a single NUL
/// byte - see docs/REGISTRATION_PROTOCOL.md's "Wire format" section.
/// This is the entire framing protocol; there is no length prefix.
fn write_frame(stream: &mut UnixStream, value: &Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(0);
    stream.write_all(&bytes)
}

fn read_frame(reader: &mut BufReader<UnixStream>) -> std::io::Result<Option<Value>> {
    let mut bytes = Vec::new();
    // read_until keeps the delimiter, so strip it before parsing.
    let n = reader.read_until(0, &mut bytes)?;
    if n == 0 {
        return Ok(None); // clean EOF - the peer closed the connection
    }
    bytes.pop(); // drop the trailing NUL
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn socket_address() -> String {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("{dir}/busbridge/busbridge.sock")
    } else {
        "/run/busbridge/busbridge.sock".to_string()
    }
}

/// The one interface this example implements, as D-Bus introspection
/// XML - see docs/REGISTRATION_PROTOCOL.md's "Why inline XML, not a new
/// schema" section for why this is XML text rather than some JSON type
/// description.
const INTROSPECTION_XML: &str = r#"<node>
  <interface name="org.example.EchoService">
    <method name="Ping">
      <arg name="message" type="s" direction="in"/>
      <arg name="reply" type="s" direction="out"/>
    </method>
    <property name="Counter" type="u" access="read"/>
  </interface>
</node>"#;

fn main() -> std::io::Result<()> {
    let mut stream = UnixStream::connect(socket_address())?;

    // Frame shape 1 (docs/REGISTRATION_PROTOCOL.md): register once, as
    // the very first frame on this connection. `id` is required on every
    // request that expects a reply, including this one.
    write_frame(
        &mut stream,
        &json!({
            "id": "register",
            "method": "org.busbridge.Registration.Register",
            "parameters": {
                "bus": "session",
                "name": "org.example.EchoService",
                "object_path": "/org/example/EchoService",
                "introspection_xml": INTROSPECTION_XML,
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
        }),
    )?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let ack = read_frame(&mut reader)?.expect("connection closed before registering");
    if let Some(err) = ack.get("error") {
        eprintln!("registration failed: {err} {:?}", ack.get("parameters"));
        std::process::exit(1);
    }
    println!("registered as {:?}", ack["parameters"]["object_path"]);
    println!("try: busctl --user call org.example.EchoService /org/example/EchoService org.example.EchoService Ping s hello");

    let counter = AtomicU64::new(0);

    // Frame shapes 2 and 3 arrive on this same connection from here on:
    // inbound D-Bus calls to answer (shape 2) and, in a real app,
    // whatever this app itself wants to push (shape 3 - this example
    // never has anything to push unprompted, so it only ever reads).
    loop {
        let frame = match read_frame(&mut reader)? {
            Some(f) => f,
            None => {
                println!("busbridge closed the connection, exiting");
                return Ok(());
            }
        };

        // Everything busbridge sends this connection unprompted (as
        // opposed to a reply to something we sent, which we never do in
        // this example) has a "method" field - see the Frame shape table
        // in docs/REGISTRATION_PROTOCOL.md.
        let Some(method) = frame.get("method").and_then(Value::as_str) else {
            continue;
        };
        let params = frame.get("parameters").cloned().unwrap_or(json!({}));

        let reply = match method {
            "org.example.EchoService.Ping" => {
                let message = params.get("message").and_then(Value::as_str).unwrap_or("");
                counter.fetch_add(1, Ordering::Relaxed);
                println!("Ping({message:?}) from {:?}", params.get("_dbus_sender"));
                json!({"parameters": {"reply": message}})
            }
            // A declared [[property]] entry's Get/Set forwards as a bare
            // call to that property's own `varlink_method` - not wrapped
            // in an "org.freedesktop.DBus.Properties.*" envelope the way
            // a *dynamically registered object*'s properties are (that's
            // a different mechanism - dynamic_object.rs - that this
            // example doesn't use). Get carries no parameters at all;
            // Set would arrive as {"value": <new value>}. See
            // docs/REGISTRATION_PROTOCOL.md's "Properties" section.
            "org.example.EchoService.Counter" => {
                json!({"parameters": counter.load(Ordering::Relaxed)})
            }
            other => {
                eprintln!("no handler for {other}, replying with an error");
                json!({"error": "org.example.EchoService.NotImplemented", "parameters": {"message": format!("{other} is not implemented")}})
            }
        };
        write_frame(&mut stream, &reply)?;
    }
}
