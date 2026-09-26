# busbridge

A bidirectional D-Bus ↔ Varlink compatibility and migration layer for
Linux, designed to be init-system agnostic (no systemd-specific
dependencies).

**Start here:** [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) is the
current, authoritative design document — an assessment of where the
project stood, ecosystem research (systemd's own Varlink migration,
StatusNotifierItem, `ObjectManager`, existing bridge attempts), the
recommended semantic-model architecture, and a phased roadmap from the
code in this repository to that architecture.

[`docs/DESIGN_BRIEF_V1.md`](./docs/DESIGN_BRIEF_V1.md) is the original
("phase 0") design brief this codebase was first built from. It's kept as
a historical record — the process model, activation story, and
passthrough/system-XML discovery mechanism it describes are still sound
and are carried forward — but `ARCHITECTURE.md` supersedes its
conversion-relay framing. The rest of this README describes what's
actually implemented in the phase-0 code today and how to build/run/test
it; treat it as documentation of the current baseline, not the end state.

## Why you still have to write *some* config

D-Bus and Varlink share no registry, no naming convention, and no
discovery protocol - there is no way for any program to look at a running
system and correctly guess "this D-Bus name should map to that Varlink
socket." Two pieces of information are therefore unavoidable, no matter
how this tool is built: **which D-Bus name to own**, and **where the
Varlink backend actually lives**. Nothing below removes that; what it
does remove is having to hand-write a mapping for every single method,
signal, and property.

## Zero-mapping passthrough mode

Set `passthrough = true` on a `[service]` block and drop the
`introspection_xml`, `[[method]]`, `[[signal]]`, and `[[property]]`
entries entirely:

```toml
[service]
bus = "session"
name = "org.example.MyService"
object_path = "/org/example/MyService"
passthrough = true

[varlink]
listen = "unix:/run/user/1000/busbridge/my-service-push.sock"
backend = "unix:/run/user/1000/my-varlink-backend.sock"
```

That's the entire config. With it:

- Any D-Bus call on `org.example.MyService` is automatically forwarded to
  the backend as a Varlink call named `"{interface}.{member}"`.
- Symmetrically, anything a backend pushes to the `listen` socket as
  `"{interface}.{member}"` is emitted as a real D-Bus signal.
- `org.freedesktop.DBus.Properties.Get/Set` work too, via the same
  `"{interface}.{property}"` naming convention (`Set` sends
  `{"value": ...}`, matching `[[property]].varlink_method`'s own
  convention exactly).

### Recovering precise types for free: automatic system-XML discovery

Many real D-Bus interfaces already have machine-readable XML specs
installed by *other* packages - conventionally under
`/usr/share/dbus-1/interfaces/*.xml` (portals,
`org.freedesktop.Notifications`, and similar commonly ship these).
`passthrough` mode looks there automatically
(`dbus/introspect.rs::find_system_interface_desc`, extra directories via
`BUSBRIDGE_INTERFACE_XML_DIRS`) before falling back to generic
inference. When a match is found, for free, with no config at all:

- Call arguments use their **real declared names**, not `arg0`/`arg1`.
- Replies are split into their **real declared out-args with real types**
  (`u`, `y`, object paths, ...), not one inferred blob.
- `Properties.GetAll` can **enumerate the real property list** instead of
  returning empty.
- Pushed signals use their **real declared positional arguments**, not one
  wrapped `a{sv}` blob.

Only when nothing declares the interface at all does passthrough fall back
to: generic `arg0`/`arg1` parameter names, one JSON-shape-inferred D-Bus
value as the whole reply (a JSON number always becomes `x`/int64, never
`u32` or `y`; a JSON object becomes `a{sv}`), `Properties.GetAll` returning
empty, and pushed signals wrapped as one `a{sv}` argument. That fallback
is the actual, honest floor - the system-XML path is a bonus for
interfaces someone's already documented this way, not something you can
rely on for a bespoke/private interface.

Use the explicit `[[method]]`/`[[signal]]`/`[[property]]` form (see
`conf.d/sni-watcher.toml.example`) when you need guaranteed exact types
regardless of what's installed on the target system, or documented
`Introspect()` output. Both forms can be mixed in the same `[service]`
block: an explicit `[[method]]` entry always wins over passthrough for
that specific method.

Verified end-to-end (not just unit-tested):
- `cargo test --test passthrough` - zero-mapping mode's fallback path
  (no system XML available), both directions.
- `cargo test --test passthrough_system_xml` - the auto-discovery path:
  a fake "installed" interface XML recovers real multi-value typed
  replies and working `Properties.Get`, with zero method/property config.

## Full migration off D-Bus: the generic bus-proxy

Passthrough and explicit mappings above both cover the same *shape* of
problem: your app owns a D-Bus name and wants to *receive* calls/emit
signals for it over Varlink instead of linking a D-Bus library. But some
apps also need to act as a D-Bus **client** against arbitrary *other*
processes whose bus names aren't known ahead of time - the canonical
example is a StatusNotifierWatcher-style tray host: once some app
registers with it, the host has to query and interact with *that specific
app's own* D-Bus object, and there's no way to write static config for a
bus name you'll only learn about at runtime.

`[varlink].bus_proxy_listen` (`dbus/bus_proxy.rs`) solves this half: a
generic, opt-in outbound D-Bus client proxy, exposed over Varlink. A
Varlink app connects to it and gets six methods:

- `Call` - an arbitrary method call on an arbitrary destination/path/
  interface. Auto-introspects the target first to recover its *real*
  declared argument types and names (parses a live `Introspect()`
  response - the same technique `passthrough`'s system-XML discovery
  uses, just against the live target instead of a file on disk), falling
  back to type inference only if the target has no/incomplete
  introspection.
- `Introspect` - raw XML passthrough.
- `GetProperty` / `GetAllProperties` / `SetProperty` - forwards to the
  target's own `Properties` interface.
- `Subscribe` - an arbitrary signal match rule (sender/path/interface/
  member, all optional), streamed back as a sequence of Varlink replies
  for as long as the connection stays open.

Together with the rest of this crate, this means an app like a tray host
can be rewritten to speak **only** Varlink - no D-Bus library linked at
all - even though its job requires full, unrestricted D-Bus client
behavior against processes it has no config for. See
`conf.d/sni-watcher-with-bus-proxy.toml.example` for what that config
actually looks like end to end.

**Security note, stated plainly:** this grants whoever can reach the
socket the same practical trust as a process connected directly to that
bus - they can call *any* method on *any* name reachable there, not just
ones related to your app. Only enable it for a backend you'd trust with
that; leave it unset (the default) otherwise.

### Wire protocol, exactly

All six method names live under the `org.busbridge.BusProxy` prefix,
same NUL-delimited-JSON framing as everywhere else. `bus_proxy_listen`'s
`Call` uses a **positional JSON array** for arguments (unlike the
resolver's own directly-routed calls above, which use named objects - see
that section for why they intentionally differ).

- **`org.busbridge.BusProxy.Call`**
  - Request: `{"destination": "org.example.App", "path": "/org/example/App", "interface": "org.example.App", "method": "DoThing", "args": [<json>, ...]}` (`args` optional, defaults to `[]`)
  - Reply: `{"reply": [<json>, ...]}` - one array entry per real out-arg, in declared order
  - Errors: a genuine D-Bus `MethodError` passes its real D-Bus error name straight through, with `{"message": "..."}`; anything else (bad destination, connection trouble) becomes `org.busbridge.BusProxy.CallFailed`; malformed parameters become `org.busbridge.BusProxy.InvalidParameters`

- **`org.busbridge.BusProxy.Introspect`**
  - Request: `{"destination": "...", "path": "..."}`
  - Reply: `{"xml": "<node>...</node>"}` (raw passthrough of the target's own `Introspect()`)

- **`org.busbridge.BusProxy.GetProperty`**
  - Request: `{"destination": "...", "path": "...", "interface": "...", "property": "..."}`
  - Reply: `{"value": <json>}`

- **`org.busbridge.BusProxy.GetAllProperties`**
  - Request: `{"destination": "...", "path": "...", "interface": "..."}`
  - Reply: `{"values": {"PropName": <json>, ...}}`

- **`org.busbridge.BusProxy.SetProperty`**
  - Request: `{"destination": "...", "path": "...", "interface": "...", "property": "...", "value": <json>}`
  - Reply: `{}` on success

- **`org.busbridge.BusProxy.Subscribe`** (`"more": true`)
  - Request: `{"sender": "org.example.App", "path": "/org/example/App", "interface": "org.example.App", "member": "SomeSignal"}` - **all four fields optional**, omit any to match anything for that field
  - Each streamed reply (`"continues": true`): `{"sender": "...", "path": "...", "interface": "...", "member": "...", "args": [<json>, ...]}`
  - Ends when the client disconnects (there's no explicit unsubscribe call - closing the connection is how you stop the stream)

### Caller identity, automatically

Every inbound call this bridge forwards to a Varlink backend - whether via
an explicit `[[method]]` mapping, `passthrough` mode, a dynamic/registrable
object, or the resolver's directly-routed calls - automatically includes
the D-Bus caller's sender (their unique bus name, e.g. `:1.234`) under the
reserved parameter key **`_dbus_sender`**, alongside the call's normal
declared/named arguments. No config needed to opt in. This exists for the
(real but less common) case where a call's own arguments don't already
carry enough to reconstruct caller identity - most callers just don't need
to read this key at all.

Verified end-to-end in `tests/bus_proxy.rs`: a fake third-party D-Bus
service is built with ordinary zbus (standing in for something like
Discord's own tray-icon object, which this bridge's operator doesn't
control), and driven entirely by a client that speaks nothing but the raw
Varlink wire protocol - no zbus, no D-Bus types at all - exercising all
six methods, including recovering a real `(s, u) -> (s, bool)` method
signature purely from live introspection and receiving a real D-Bus
signal purely over Varlink.

## The standard Varlink resolver, on one well-known socket

`src/dbus/resolver.rs` implements the real, standard `org.varlink.resolver`
protocol (https://varlink.org/Resolver) on a single well-known socket:
`/run/varlink/varlink.sock` by default (override with
`BUSBRIDGE_RESOLVER_SOCKET`). This is the Varlink-world equivalent
of `$DBUS_SESSION_BUS_ADDRESS` - one fixed, standard address every client
can rely on, covering **both** kinds of interface a client might want:

- **Native Varlink services** unrelated to D-Bus at all, discovered via a
  file-based registry (`/run/varlink/registry/` by default, override with
  `BUSBRIDGE_REGISTRY_DIR`): one file per interface, filename =
  interface name, contents = that service's own real address. This bridge
  never writes to this registry itself - other services manage their own
  entries directly (write on startup, remove on clean shutdown). Stale
  entries (registered, but nothing listening any more - e.g. after an
  unclean exit) are detected on lookup via a real connect probe and
  self-heal by deleting the stale file.
- **D-Bus-backed interfaces**, routed via a standalone `[[resolve]]` config
  entry (see `conf.d/resolve-only.toml.example`) - deliberately NOT nested
  under `[service]` and never tied to name ownership, since the
  destination is very often a name owned by a completely different,
  unrelated process.

### Wire protocol, exactly

Every connection to the resolver socket is routed by method name (same
NUL-delimited-JSON framing as everywhere else in this crate):

- **`org.varlink.resolver.Resolve`** (the real, standard method):
  - Request: `{"interface": "org.example.Foo"}`
  - Reply: `{"address": "unix:/path/to/something"}`
  - Error: `org.varlink.resolver.InterfaceNotFound` with
    `{"interface": "org.example.Foo"}`
  - For a **native** match: `address` is that service's own real address.
    **The client must disconnect and connect there directly - this bridge
    never proxies native Varlink traffic.**
  - For a **D-Bus-backed** match: `address` is this same daemon's own
    resolver socket address. **The client should reuse the same
    connection** for its next call.

- **Any other method name**, of the form `"{interface}.{method}"` - only
  meaningful for a D-Bus-backed interface, and only valid on the same
  connection that just resolved it (though a client that already knows
  which interface it wants can skip the `Resolve` round trip entirely):
  - Request parameters: a plain **JSON object of named arguments**
    (Varlink's own native calling convention - e.g. calling
    `org.example.Greeter.SayHello` with `{"name": "Ada"}`). This is
    auto-introspected against the real D-Bus target for real argument
    names/types (same mechanism as the bus-proxy's `Call` above), falling
    back to inference only if introspection doesn't cover it.
  - Reply: a named JSON object of the method's real out-args when known
    (e.g. `{"greeting": "Hello, Ada!"}`), falling back to generic
    `arg0`/`arg1` keys if introspection didn't declare names for them.
  - Errors: a genuine D-Bus `MethodError` passes its real D-Bus error name
    straight through; anything else becomes
    `org.busbridge.BusProxy.CallFailed`.

Note this is a **different** wire convention from `bus_proxy_listen`'s own
`Call` method (positional JSON array, explicit destination/path every
time) - the two serve different use cases (bus-proxy: arbitrary,
unconfigured destinations; resolver: pre-configured, ergonomic,
interface-name-based routing) and intentionally aren't unified, so
existing bus-proxy integrations are unaffected by this addition.

Verified end-to-end in `tests/resolver.rs`: both a fake native Varlink
service (registered via the file registry) and a fake D-Bus-backed
service (via `[[resolve]]`, built with plain zbus) are resolved and
called correctly, including the native-service liveness/staleness check
and the D-Bus-backed path's real multi-value typed reply recovered purely
from live introspection.

## Running in the background

Under a supervisor (systemd, s6, runit, ...) just run `busbridge` in the
foreground and let it hold the process directly - that's the normal
case, and how `service-templates/systemd/` already works. With no
supervisor at all, `busbridge --daemonize` detaches and backgrounds
itself the classic way. Either way, SIGTERM/SIGINT now trigger a clean
shutdown (every held D-Bus name released, telemetry flushed) instead of
dying with no cleanup. See `docs/DAEMON.md` for the full picture.

## Layout

```
docs/ARCHITECTURE.md                 # current design + roadmap - read this first
docs/DESIGN_BRIEF_V1.md              # historical phase-0 design brief
docs/REGISTRATION_PROTOCOL.md        # the app registration protocol spec
docs/DAEMON.md                       # running in the background, init-agnostically
Cargo.toml              # dependencies (zbus 4, tokio, serde, quick-xml, inotify, ...)
src/
  lib.rs                # module declarations + cli_main(); all real logic lives here
  main.rs                # thin binary wrapper around busbridge::cli_main()
  config/                # conf.d loading, schema, hot-reload watcher
  dbus/                  # low-level zbus connection, dispatch, introspection, dynamic objects, generic bus-proxy, varlink resolver
  varlink/               # wire framing, outbound client, inbound server, streaming subscriptions, peer.rs (registered-service multiplexing)
  registration.rs        # the app registration protocol: BusRegistry, Register/RegisterDynamicObject/Peer.* handling
  control.rs             # resident/handoff race-safe control channel + registration socket dispatch
  daemonize.rs           # optional init-agnostic self-daemonization (--daemonize)
  convert.rs             # zvariant::Value <-> serde_json::Value, signature parser
  errors.rs              # Varlink error <-> D-Bus error mapping
  activation.rs          # LISTEN_FDS/LISTEN_PID parsing (optional, never required), shared inherited-fd pool
  idle.rs                # activity tracking + idle-exit
  telemetry.rs           # per-mapping call counters, `stats` subcommand
conf.d/                 # example service mapping configs (docs/DESIGN_BRIEF_V1.md §3.2)
examples/
  registration_client.rs # minimal reference client for the app registration protocol
service-templates/
  dbus-1/               # example D-Bus bus-activation .service file (init-agnostic)
  systemd/              # OPTIONAL systemd convenience drop-ins (native socket activation)
docs/                   # supplementary notes, introspection XML examples
tests/
  end_to_end.rs         # real integration test: private dbus-daemon + fake varlink backend
```

## Status

Fully implemented per docs/DESIGN_BRIEF_V1.md's build order (§6). Every module listed in
§5 is real code, not a stub - `grep -r "todo!" src/` returns nothing.

- **46 unit tests** across every module (`cargo test --lib`), including a
  concurrency test for the resident/handoff race (§2.1) that spins up 12
  simultaneous "activations" against one control socket and asserts
  exactly one becomes resident and every loser's handoff is acknowledged,
  and a regression test for a real crash a user hit (a resident process
  with zero configured services on either bus must not panic - it now
  runs the control channel with no `BridgeState` behind it and acks
  handoffs anyway).
- **7 end-to-end integration tests** against a real, private
  `dbus-daemon --session` and a fake Varlink backend/app (no mocks of this
  crate's own code):
  - `cargo test --test end_to_end` - explicit `[[method]]`/`[[signal]]`
    mappings: a D-Bus call is forwarded and replied to correctly; a
    pushed Varlink event is observed as a real D-Bus signal; the caller's
    `_dbus_sender` arrives automatically alongside the declared args.
  - `cargo test --test passthrough` - the zero-mapping mode's generic
    fallback path, proving the same two directions work with no
    per-method config and no system interface XML available.
  - `cargo test --test passthrough_system_xml` - the auto-discovery path:
    with a fake "installed" interface XML present, passthrough recovers
    a real multi-value, precisely-typed reply and a working
    `Properties.Get` - catching, incidentally, a real pre-existing
    double-variant-wrapping bug in `Properties.Get` that no earlier test
    had ever exercised.
  - `cargo test --test bus_proxy` - the generic outbound-client proxy: a
    client speaking nothing but raw Varlink drives a fake third-party
    D-Bus service (built with ordinary zbus) through `Call`,
    `GetProperty`/`SetProperty`, `Introspect`, and `Subscribe`, recovering
    a real multi-arg method signature purely via live introspection.
  - `cargo test --test resolver` - the standard `org.varlink.resolver`
    protocol: a fake native Varlink service (file-registry-registered)
    resolves to its own address and is called directly; a fake D-Bus-backed
    service (`[[resolve]]`-configured) resolves to this daemon's own
    address and is called on the same connection with named parameters.

  These tests skip cleanly (with a message, not a failure) if `dbus-daemon`
  isn't installed in the environment running them.

  Each test spawns its own private `dbus-daemon` and connects the bridge to
  it via an explicit address (`busbridge::start_bus_on_connection`,
  `zbus::conn::Builder::address(...)`) rather than the
  `DBUS_SESSION_BUS_ADDRESS` env var - `cargo test` runs `#[test]`
  functions within one binary concurrently by default, and that env var is
  process-global, so two tests both setting it would race. If you're
  adding a new integration test, follow the same pattern rather than
  `std::env::set_var`.

### Building and testing

```sh
cargo build                          # debug build
cargo build --release                # release build
cargo test                           # unit tests + integration tests
cargo test --lib                     # unit tests only (no dbus-daemon needed)
cargo test --test end_to_end         # just the end-to-end tests
```

### Running

```sh
# Copy an example config into place:
cp conf.d/sni-watcher.toml.example conf.d/sni-watcher.toml
# (edit the varlink listen/backend addresses for your actual tray backend)

BUSBRIDGE_CONFIG_DIR=./conf.d cargo run
```

Environment variables (all optional, all documented in `src/lib.rs` and
`src/control.rs`):

- `BUSBRIDGE_CONFIG_DIR` - where to look for `*.toml` mapping
  files. Defaults to `/etc/busbridge/conf.d`.
- `BUSBRIDGE_RUNTIME_DIR` - where the control socket/lockfile live.
  Defaults to `$XDG_RUNTIME_DIR/busbridge`, falling back to
  `/run/busbridge`.
- `BUSBRIDGE_STATE_DIR` / `XDG_STATE_HOME` - where telemetry is
  persisted. Defaults to `/var/lib/busbridge/telemetry.jsonl`.
- `BUSBRIDGE_ACTIVATED_NAME` - the D-Bus name this invocation was
  activated for, if the supervisor can pass it (used only as an early
  handoff hint; never required for correctness).

```sh
busbridge stats   # print per-(bus_name, interface, member) call counts
```

### A note on the committed `Cargo.lock`

This was developed and tested against a fairly old toolchain (rustc/cargo
1.75). Several transitive dependencies (`indexmap`, `hashbrown`, `toml`,
`tempfile`, `proc-macro-crate`, and friends) have since moved to edition
2024 or bumped their MSRV past 1.75, which breaks resolution on older
toolchains if you regenerate the lockfile with `cargo update`. The
committed `Cargo.lock` pins working versions for that toolchain. If you're
building with a current stable Rust, feel free to `cargo update` freely -
you almost certainly won't hit this at all.

### Documented assumptions

Several places in the config schema are intentionally open-ended (per
docs/DESIGN_BRIEF_V1.md §9). Where the code had to pick a concrete convention, it's
called out in a doc comment at the point of decision - search for "state
the assumption" across `src/` to find them all. The main ones:

- Inbound Varlink pushes (§2.2) and dynamic-object pushes (§2.5) use
  `"{dbus_interface}.{member}"` as the Varlink method name.
- `[[method]].args` names positional D-Bus in-args; out-args and their
  types come from `introspection_xml` instead.
- `[[property]].varlink_method` is used for both Get/GetAll and Set (with
  `{"value": ...}` params for Set).
- One D-Bus resident process can serve both session- and system-bus
  services simultaneously via two independent `BridgeState`s sharing one
  control channel and one telemetry store.
