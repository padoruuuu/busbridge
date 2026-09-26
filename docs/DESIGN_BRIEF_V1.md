# Project Brief: `busbridge` (v1, historical)

> **Status: superseded.** This is the original design brief for the project
> when it was scoped as a config-driven translation relay. It is kept here
> as a historical record of the "phase 0" architecture and the reasoning
> behind it — much of which (the init-agnostic activation model, the
> resident/handoff control channel, the passthrough/system-XML discovery
> mechanism) is still sound and carried forward. It has been **superseded**
> by [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md), which reassesses the
> mission against current D-Bus/Varlink ecosystem research and lays out
> where the project is going next (a semantic bus model, stateful protocol
> adapters, first-class object lifecycles). Read `ARCHITECTURE.md` first;
> come back here only for the "why" behind existing phase-0 code that
> `ARCHITECTURE.md`'s roadmap hasn't replaced yet.

Paste this entire document as the opening message to a fresh Claude session
to begin implementation. It contains the full design context; you should not
need the original planning conversation. Where a decision was deliberately
left open, it's flagged explicitly in "Open Decisions" at the end — make a
reasonable choice, state the assumption, and proceed.

## 1. Mission

Write a generic, config-driven Rust daemon that acts as a translation relay
between D-Bus and Varlink. It lets programs keep speaking D-Bus (for
compatibility with the existing ecosystem) while the actual service logic
lives behind Varlink, so that:

- The D-Bus-facing surface only runs when something is actually using it
  (bus activation), instead of a permanent resident service per D-Bus name.
- The Varlink-facing backends only run when actually invoked (Varlink's own
  `exec:` transport, or socket activation on their own socket).
- Over time, as more of an ecosystem's programs speak Varlink natively, this
  bridge becomes the **single remaining long-lived D-Bus client/service
  process** on the system — a consolidation point, not one bridge per
  interface. Every other former D-Bus service disappears as a resident
  process and either speaks Varlink directly or lives behind this bridge as a
  passive Varlink backend.
- The design must work under **any init system**, not just systemd. See
  Section 4 (Hard Constraints) — this is non-negotiable and shapes several
  decisions below that would otherwise be "just use systemd."

This is explicitly a bridge/migration tool, not a permanent architecture —
its usefulness should visibly shrink as native Varlink adoption grows, and
it should make that shrinkage *measurable* (see Telemetry, Section 3.8).

Critically, this needs to work in **both directions**: not just letting
legacy D-Bus-only programs be consumed via Varlink, but also letting a
**brand-new application be written Varlink-only** and still be usable by
old D-Bus-only consumers that haven't migrated yet (e.g. a new tray item
app written purely against Varlink, still shown correctly by an old
D-Bus-only tray host panel). The static per-interface config in Section
3 handles the first direction natively; the second direction additionally
needs runtime-created D-Bus objects, since a Varlink-only app has no D-Bus
identity of its own — see Section 2.5.

## 2. Core Architecture

### 2.1 Process model: one resident process, many D-Bus names

Rather than one exec'd process per D-Bus interface, this bridge is designed to
end up being **a single long-lived process that owns an arbitrary number of
D-Bus well-known names**, configured via a `conf.d`-style directory. Adding
support for a new interface is "drop in a new TOML file," not "write and
deploy a new binary."

Because a D-Bus connection can call `RequestName` for as many well-known
names as it likes, and because bus activation is a feature of the message
bus daemon (dbus-daemon/dbus-broker) and *not* of any init system, this part
of the design is inherently init-agnostic already — lean on it.

**The multi-name-single-process problem:** D-Bus activation fundamentally
works by exec'ing a fresh process per activation event. If name A and name B
are both configured, and both get activated independently (possibly by two
different callers at nearly the same time), naively you'd get two competing
processes both trying to become "the" bridge. Solve this with a **control
channel**, not an init-specific trick (systemd's `SystemdService=` was
considered and explicitly rejected — it's a systemd-only `.service` file
extension and violates the init-agnostic requirement):

1. On startup, every invocation (whether freshly exec'd by bus activation or
   otherwise) first tries to connect to a private control socket at a fixed,
   well-known path (e.g. `$XDG_RUNTIME_DIR/busbridge/control.sock`
   for session scope, an analogous `/run/...` path for system scope).
2. **Connect succeeds** → another instance is already the resident bridge.
   Send it a small message over the control channel identifying which
   D-Bus name this invocation was activated for; that resident instance
   calls `RequestName` for it on its own existing bus connection. The
   newly-exec'd process then exits immediately (it was only ever a wake-up
   knock — the bus daemon needed *something* to exec, and this was it).
3. **Connect fails** (nothing listening) → this invocation becomes the
   resident instance. It binds the control socket, connects to the bus,
   claims whatever name(s) apply, and enters the dispatch loop.

This must be race-safe: two near-simultaneous activations both finding "no
one's listening" and both trying to become resident is a real scenario.
Use an exclusive-create/lock primitive (e.g. bind with a preceding
`flock()`'d lockfile, or rely on `bind()` failing with `EADDRINUSE` for an
already-bound abstract/unix socket) to make "become resident" atomic, and
have the loser fall back to path (2) — connect and hand off — rather than
erroring out.

### 2.2 Varlink-side activation: built into the bridge (per current decision)

Earlier iterations of this design considered a separate standalone
"activation supervisor" binary. **Current decision: fold that
responsibility into the bridge itself** rather than splitting it into its own
crate/binary. Keep the code modular internally (a self-contained module,
see Section 5) so it *could* be extracted later, but ship it as part of
this one binary for now.

Responsibility of this subsystem: be the thing a Varlink backend connects
*to* when it has an event to push (e.g. a tray icon changed and needs to
become a D-Bus signal). Concretely:

1. On startup, check for inherited-fd activation (see Section 4.2 — read
   `LISTEN_FDS`/`LISTEN_PID` defensively, no library dependency). If
   present and valid, use the inherited listening socket.
2. If absent, bind and listen on the configured Varlink socket path itself.
3. Accept loop: incoming connections are Varlink backends delivering
   events. Decode, look up the config mapping, translate to a D-Bus signal
   (or property-changed notification) using the type-conversion rules in
   Section 3.5, and emit it on the bus connection.
4. This accept loop's activity also feeds the idle-exit timer (Section
   3.7) — an incoming connection counts as activity even though it's not a
   D-Bus call.

### 2.3 Outbound Varlink calls (D-Bus → Varlink direction)

When a D-Bus method call arrives for a configured interface/member, look up
its mapping and issue the corresponding Varlink call to the configured
backend address. Support at least two Varlink transport forms for this
outbound direction:

- `unix:/path/to/socket` — connect directly; if the backend is itself
  socket-activated (by its own init-native mechanism or its own future
  bridge), the `connect()` call is what triggers its activation — no special
  handling needed on our side.
- `exec:/path/to/program [args...]` — Varlink's own standard
  activation-on-connect convention (spawn, talk over stdin/stdout). Prefer
  this where it fits (stateless/short-lived backends) since it requires
  zero assumptions about what supervises the backend.

Each inbound D-Bus call should be dispatched onto its own async task with
its own timeout against the target Varlink backend, so a slow or hung
backend for interface A cannot stall unrelated traffic for interface B.

### 2.4 Signal / streaming forwarding

Varlink's `"more": true` continuation mechanism (a call that receives
multiple pushed replies over time) is the natural analog of a D-Bus signal
stream and should be used for the *bridge calling out to subscribe* case
(e.g. "watch for property changes on this backend"), distinct from the
inbound-connection push model in 2.2 (which is for the backend initiating
contact, better suited to activation-based wake-ups). Config should be able
to express either direction per-signal — see Section 3.3.

A held-open streaming call (2.4) or an accepted inbound connection with
further expected traffic (2.2) must both be treated as "not idle" for
purposes of Section 3.7.

### 2.5 Dynamic object proxying (Varlink-native clients appearing on D-Bus)

Everything in 2.1-2.4 assumes a **static** dispatch table: fixed interfaces
and object paths known in advance from `conf.d`. That covers the common
case of "translate calls aimed at a well-known service name." It does
**not** cover interfaces like Status Notifier Item, where each individual
*client application* (not just the bridge's own well-known service) needs
its own independently-addressable D-Bus object with ongoing two-way
traffic — property gets, method calls, and signals — for as long as that
application runs.

This matters specifically for the goal of letting a brand-new application
be written **Varlink-only** and still be picked up by legacy D-Bus-only
consumers (e.g. a Varlink-native tray item app, discovered by an
old D-Bus-only tray host panel). Without this section, the bridge can only
translate calls *into* a Varlink-speaking backend for a fixed set of
pre-configured names — it can't manufacture a new *outward-facing* D-Bus
identity on behalf of an app that never opens a D-Bus connection itself.

**Design:**

1. Config marks certain interfaces as `registrable` (see schema addition
   below) rather than fixed-path — meaning: object paths for this
   interface are created at runtime, not known at startup.
2. When a Varlink-only application connects to the bridge's Varlink listener
   (the same inbound-push listener from 2.2) and sends a registration
   request for a `registrable` interface, the bridge:
   - allocates a fresh D-Bus object path (e.g. `/StatusNotifierItem/<id>`,
     `<id>` generated — sequential counter or the app-supplied identifier,
     config should allow either),
   - registers that object on the existing bus connection using the
     interface shape declared in the relevant `introspection_xml`,
   - keeps a live mapping from that object path back to *this specific*
     Varlink connection (not to a fixed backend address — the backend
     *is* the live connection) for as long as it stays open.
3. For the lifetime of that connection, the bridge proxies bidirectionally:
   - **legacy D-Bus caller → app**: any call a D-Bus peer makes on the
     synthetic object (property `Get`/`GetAll`, `Activate`, `Scroll`,
     etc.) is forwarded as a Varlink call over that specific app
     connection, response routed back to the original D-Bus caller.
   - **app → legacy D-Bus caller(s)**: any event the app pushes over its
     Varlink connection (e.g. `NewIcon`, `NewStatus`) is emitted as a real
     D-Bus signal from that synthetic object path.

   **Isolation requirement:** all of this runs on the bridge's single shared
   D-Bus connection (Section 2.1) — there is exactly one connection no
   matter how many objects are registered. This means the same per-call
   isolation principle from Section 2.3 applies here too, and matters more
   as the registered-object count grows: dispatch each inbound D-Bus call
   destined for a dynamic object onto its own async task with its own
   timeout against that object's specific backing Varlink connection. A
   slow or hung app behind object path A must never stall a call destined
   for object path B, even though both are proxied through the same
   underlying D-Bus connection. The routing table (path → connection) is
   purely a lookup; it must not become a shared lock that serializes
   unrelated calls.
4. On disconnect (app exits or closes its Varlink connection): tear the
   synthetic object down, release its path, and — where the interface has
   a "goodbye" convention (e.g. telling
   `org.kde.StatusNotifierWatcher.UnregisterStatusNotifierItem` if/when
   such a thing is used, or just letting `NameOwnerChanged`-equivalent
   absence speak for itself) — perform that cleanup too.

**Interaction with idle-exit (3.7):** a live proxied object with an open
Varlink connection backing it counts as "not idle," same as an open
streaming subscription (2.4) — the bridge must not exit while any registered
dynamic object still has an app connected behind it, even with zero recent
D-Bus traffic, since that object could receive a call at any moment and
there is now no way to "re-activate into" it (unlike the static case,
where bus activation itself is the wake-up mechanism — a synthetic object
that no longer has a live process backing it should simply be torn down,
not preserved across a restart).

**Config schema addition:**

```toml
[[registrable]]
dbus_interface = "org.kde.StatusNotifierItem"
path_prefix = "/StatusNotifierItem"           # synthetic paths allocated under this
introspection_xml = "sni-item.xml"            # authoritative types for THIS interface
register_via = "org.example.tray.RegisterItem" # varlink method the app calls to register
id_source = "generated"                        # "generated" | "app_supplied"
```

This is additive to the existing method/signal/property config shapes,
not a replacement — a single `conf.d` file for SNI would plausibly define
both the fixed `org.kde.StatusNotifierWatcher` service (static, 3.2) *and*
a `[[registrable]]` block for `org.kde.StatusNotifierItem` (dynamic, this
section), since real-world SNI involves both a well-known-named watcher
and per-app items.

**Note on when this machinery stops mattering:** if *both* sides of a
given interaction (item and host) eventually speak Varlink natively, they
should talk to each other directly with no D-Bus and no bridge involved at
all — this dynamic-object-proxy path only exists for as long as at least
one side is D-Bus-only. Its usage should show up in the telemetry counters
(3.8) the same as any other mapping, and going quiet is exactly the signal
that it's safe to delete.

## 3. Components (map to modules in Section 5)

### 3.1 Config loader
- Scans a `conf.d`-style directory (default e.g.
  `/etc/busbridge/conf.d/*.toml`, overridable) at startup.
- Supports hot-reload: watch the directory (inotify on Linux, but keep this
  behind a trait/abstraction so a polling fallback works anywhere) and
  reconcile added/changed/removed mapping files without a full restart —
  new D-Bus names get `RequestName`'d, removed ones get released.
- Produces an in-memory dispatch table: effectively
  `HashMap<(BusName, Interface, Member), Mapping>` for methods, plus
  parallel structures for signals and properties.

### 3.2 Config schema (TOML)

One file per D-Bus name is the expected convention, though the loader
should just merge everything found in the directory. Example:

```toml
[service]
bus = "session"                       # "session" | "system"
name = "org.kde.StatusNotifierWatcher"
object_path = "/StatusNotifierWatcher"
introspection_xml = "sni-watcher.xml" # relative to this config file; authoritative
                                       # source of D-Bus-side types for this interface
idle_timeout_secs = 30

[varlink]
listen = "unix:/run/user/1000/busbridge/sni.sock"  # inbound push (2.2)
backend = "unix:/run/user/1000/tray-backend.sock"          # outbound calls (2.3)

[[method]]
dbus_interface = "org.kde.StatusNotifierWatcher"
dbus_method = "RegisterStatusNotifierItem"
varlink_method = "org.example.tray.Register"
# maps positional dbus args (in signature order) to named varlink params
args = ["service"]

[[signal]]
dbus_interface = "org.kde.StatusNotifierWatcher"
dbus_signal = "StatusNotifierItemRegistered"
direction = "inbound_push"            # backend connects to us (2.2)
# OR: direction = "outbound_subscribe" with a varlink_method that streams (2.4)

[[property]]
dbus_interface = "org.kde.StatusNotifierWatcher"
dbus_property = "RegisteredStatusNotifierItems"
varlink_method = "org.example.tray.ListRegistered"
```

Treat `org.freedesktop.DBus.Properties` (`Get`/`Set`/`GetAll` +
`PropertiesChanged`) as ordinary entries in the same method/signal tables
rather than special-cased code paths — keeps the dispatch engine uniform.

### 3.3 D-Bus side (zbus, low-level)

Use `zbus`, but **do not** use the `#[interface]` proc-macro or
`#[proxy]` codegen — both assume compile-time known method signatures,
which is incompatible with a config-driven generic dispatcher. Instead:

- Open a connection, register for the object paths named in config.
- Intercept raw `zbus::Message`s (or the lowest-level API zbus's current
  version exposes for this — check current `zbus` docs, the API has
  changed across major versions) and dispatch by `(interface, member)`
  against the table from 3.1.
- Construct replies/signals manually via zbus's message-building API using
  `zvariant::Value`, with the target signature sourced from the
  `introspection_xml` referenced in config (see 3.5).
- `RequestName` for every configured name on this one connection.

### 3.4 Varlink side (hand-rolled, not the `varlink` crate)

The official `varlink` crate is codegen-first (`.varlink` interface files →
generated Rust structs) and assumes static types — also incompatible with
generic dispatch. Hand-roll a minimal async client/server:

- Wire format: JSON object + `\0` (NUL) delimiter, one object per
  request/reply, over a `tokio::net::UnixStream` (or the exec'd child's
  stdio pipes for `exec:` transport).
- Client (outbound calls, 2.3): send `{"method": "...", "parameters": {...}}`,
  read one or more replies, respecting `"continues": true` for streaming.
- Server (inbound push, 2.2 / accept loop): accept connections, parse
  incoming request objects, look up config mapping, translate to D-Bus.
- Implement enough of Varlink's `org.varlink.service.GetInfo` /
  `GetInterfaceDescription` introspection methods for basic compatibility
  with generic Varlink tooling, even though our own dispatch doesn't need
  them internally.

### 3.5 Type conversion layer

- **D-Bus → JSON** (needed for 2.3 outbound calls and encoding push
  payloads): purely mechanical from the known incoming signature —
  `s`→string, integer types→number, `b`→bool, arrays→JSON arrays,
  `a{sv}`→JSON object, structs→JSON array or object (pick one convention
  and document it), variants→unwrap one level. Needs no per-interface
  config; write this once as a generic recursive converter.
- **JSON → D-Bus** (needed for inbound push → signal emission, and for
  translating Varlink replies back into D-Bus method replies): JSON alone
  is underspecified (can't tell `i32` from `u64` from `d` from a bare
  number). Resolve this using the **target D-Bus signature**, which is
  always statically knowable here because we control the introspection XML
  for everything we expose on the D-Bus side. Write a converter that takes
  `(serde_json::Value, &zvariant::Signature)` and produces the coerced
  `zvariant::Value`, erroring clearly on mismatches rather than guessing.

### 3.6 Error mapping

- Varlink errors are named JSON objects:
  `{"error": "org.example.NotFound", "parameters": {...}}`.
- D-Bus errors are an error name (`org.freedesktop.DBus.Error.*`-style
  convention, but can be any reverse-DNS name) plus a human string.
- Define a small, explicit mapping table (in config, per interface, or a
  sensible convention like "pass the Varlink error name straight through as
  the D-Bus error name, stringify `parameters` as the message") — document
  whichever convention you pick clearly since it's currently unresolved
  (see Open Decisions).

### 3.7 Idle-exit

- Maintain a last-activity timestamp updated on: any D-Bus call handled,
  any inbound Varlink push connection accepted, any outbound Varlink call
  completed.
- A background timer checks against `idle_timeout_secs` (per-service
  config, but enforced at the process level using the max/most-permissive
  configured value across all loaded services, since one process now
  serves many names).
- Do **not** exit while any streaming Varlink subscription (2.4) is
  currently open, any call is in-flight, or any dynamically registered
  object (2.5) still has a live app connection backing it — regardless of
  the idle timer.
- On exit: release all held D-Bus names, close the control socket, close
  the Varlink listen socket cleanly (so the next activation — bus-side or
  Varlink-side — starts fresh, per the init-agnostic activation model).

### 3.8 Migration telemetry

- Maintain simple counters keyed by `(bus_name, interface, member)` for
  every D-Bus-side call actually received, persisted to a small local
  state file (plain JSON or line-delimited, doesn't need to be fancy) so
  counts survive idle-exit/restart cycles.
- Provide a way to dump this (a CLI subcommand, e.g.
  `busbridge stats`) so an operator can identify mappings that
  have gone cold — evidence that every caller has migrated to native
  Varlink and that config entry (and the compatibility bridge for it) can be
  deleted entirely.

## 4. Hard Constraints (init-agnosticism)

These are non-negotiable design rules, not preferences:

- **No linkage against `libsystemd`/`libsystemd-sys`.** No exceptions.
- **No `sd_notify`/`NOTIFY_SOCKET` readiness protocol.** Readiness is
  implicit: successfully owning a D-Bus name *is* the readiness signal to
  D-Bus peers; the Varlink socket accepting a connection *is* the readiness
  signal to Varlink peers. Don't add an explicit readiness handshake on top
  — it would be the one thing that actually re-couples this to systemd for
  no benefit.
- **No `SystemdService=` or any other systemd-only `.service` file key.**
  Multi-name-single-process consolidation is handled entirely by the
  userspace control channel (Section 2.1), not by an init-side alias
  mechanism.
- **`LISTEN_FDS`/`LISTEN_PID` env-var inheritance is read defensively as an
  optional accelerant, never a requirement.** This convention (fds start
  at fd 3, `LISTEN_PID` must match our own pid, `LISTEN_FDS` gives the
  count) is implemented independently by multiple non-systemd supervisors
  (s6, dinit, some OpenRC setups) — treat it as a shared, load-bearing-only
  when-present ABI, not a systemd dependency. Parse it by hand (a dozen
  lines); do not pull in a crate whose only purpose is talking to systemd
  specifically.
- **The only thing ever assumed of the init system is: "start this
  executable, and restart it if it exits."** That is the universal
  supervisor contract satisfied by every init in existence (systemd,
  sysvinit + a respawn wrapper, runit, s6, dinit, OpenRC, even a `cron
  @reboot` restart loop). D-Bus's own bus-activation `Exec=` mechanism
  covers "start me on demand" for the D-Bus-triggered case without any
  init involvement at all, since that's a message-bus-daemon feature.
  Ship systemd `.socket`/`.service` unit files as **optional, clearly
  labeled convenience drop-ins** under `service-templates/systemd/` for
  users on inits sophisticated enough to do native socket-activation
  hand-off — the bridge's own fd-detection code must work identically
  whether that fd came from a native unit, is bound by the bridge itself,
  or (not applicable here per the current scope decision, but keep this
  extensible) from a separate supervisor stub in the future.

## 5. Suggested Module Layout

Matches the skeleton already created alongside this prompt:

```
src/
  main.rs              # entrypoint: parse args/env, decide resident-vs-handoff (2.1), run
  config/
    mod.rs             # conf.d scanning, hot-reload, merges into DispatchTable
    schema.rs          # serde structs for the TOML shape in 3.2
  control.rs           # the control-channel protocol + resident/handoff logic (2.1)
  dbus/
    mod.rs             # zbus connection setup, RequestName handling, message intercept
    dispatch.rs         # (interface, member) -> Mapping lookup + per-call task spawn
    introspect.rs        # loading/parsing the introspection_xml referenced in config
    dynamic_object.rs    # runtime object creation/proxy/teardown for `registrable`
                          # interfaces (2.5) - path allocation, live-connection
                          # routing table, bidirectional proxying, idle-exit hook
  varlink/
    mod.rs             # shared framing (JSON + NUL) read/write helpers
    client.rs          # outbound calls (2.3): unix: and exec: transports
    server.rs          # inbound accept loop / activation stub logic (2.2)
    streaming.rs        # "more":true subscribe handling (2.4)
  convert.rs            # zvariant::Value <-> serde_json::Value (3.5)
  errors.rs             # varlink-error <-> dbus-error mapping (3.6)
  idle.rs               # activity tracking + idle-exit timer (3.7)
  telemetry.rs          # call counters + `stats` subcommand support (3.8)
  activation.rs         # LISTEN_FDS/LISTEN_PID defensive parsing (Section 4)
```

## 6. Suggested Build Order

1. Config loader + schema, with a static test fixture — no networking yet.
2. D-Bus side: connect, `RequestName` a single hardcoded test name, reply
   to one hardcoded method with a canned value. Proves the zbus
   low-level-message approach works before generic dispatch is built.
3. Generic dispatch table wired to config from step 1, still with a fake/
   stub Varlink backend (e.g. a shell script using `socat` for manual
   testing).
4. Real hand-rolled Varlink client (outbound, 2.3) replacing the stub.
5. Type conversion layer (3.5) done properly, replacing any temporary
   hardcoded conversions from steps 2-4.
6. Idle-exit (3.7) and `LISTEN_FDS`-aware Varlink listener (2.2).
7. Control channel + multi-name single-process consolidation (2.1) — this
   is the trickiest correctness piece (race safety); write it with tests
   that simulate near-simultaneous activation.
8. Signal/streaming forwarding (2.4), error mapping (3.6).
9. Dynamic object proxying (2.5) — build this only after the static path
   (steps 1-8) is solid, since it reuses the same conversion/idle/control
   infrastructure but adds runtime object lifecycle management on top.
   This is what makes a Varlink-only client app (e.g. a Varlink-native SNI
   tray item) transparently visible to legacy D-Bus-only consumers.
10. Hot-reload (3.1) and telemetry (3.8) last — genuinely optional polish,
    don't let them block a working end-to-end path.

Prefer getting one real interface (Status Notifier Item/Watcher is the
running example throughout this brief and is a reasonable first target —
it's small, well-documented, and broadcast-heavy enough to exercise both
the method and signal paths) working fully end-to-end before generalizing
further, rather than building all the abstraction layers against no real
test case.

## 7. Testing Expectations

- Unit tests for the type conversion layer (3.5) covering at minimum:
  primitives, nested `a{sv}`, arrays of structs, variant unwrapping, and at
  least one deliberately malformed/mismatched-signature case per direction.
- An integration test that exercises the control-channel race (two
  processes started concurrently, exactly one should become resident).
- An integration test using a fake Varlink backend (a small test-only
  Unix-socket echo/script) driven end-to-end through a real D-Bus session
  bus (dbus-daemon or dbus-broker under test, e.g. via `dbus-run-session`)
  to prove a full method call round-trips correctly.
- Idle-exit behavior should be tested with a short configurable timeout in
  test builds rather than waiting out a real 30s+ timer.

## 8. Explicit Non-Goals

- This is not a general-purpose D-Bus-to-anything bridge; Varlink is the
  only target protocol.
- Not building a GUI or TUI config editor — TOML files only.
- Not implementing D-Bus's full type system exhaustively on day one
  (e.g. exotic struct nesting, file-descriptor-passing messages) — get the
  common cases solid first and document known gaps.
- Not attempting to replace or reimplement the message bus daemon itself
  (dbus-daemon/dbus-broker) — this bridge is a client/service on the bus,
  not a bus implementation.
- Not shipping a separate activation-supervisor binary at this stage (that
  idea was considered and explicitly deferred — see project history notes
  in the prompt above, Section 2.2). Keep the code modular enough that
  splitting it out later remains easy, but don't build the split now.

## 9. Open Decisions (use judgment, state your assumption, proceed)

- **Struct encoding convention** for D-Bus structs `(a, b, c)` as JSON:
  array vs. object-with-numbered-or-named-keys. Pick array (simplest,
  matches Varlink's own convention of using arrays for ordered data) unless
  a specific interface's config wants to name fields explicitly.
- **Error-mapping convention** (3.6): default to "Varlink error name passes
  straight through as the D-Bus error name; `parameters` is JSON-stringified
  into the D-Bus error message" unless config specifies an explicit mapping
  table for an interface that needs nicer error names.
- **Control channel wire format**: keep it minimal — newline-delimited JSON
  is fine, doesn't need to match the Varlink framing used elsewhere, since
  it's a private implementation detail never exposed externally.
- **System vs. session bus default**: config specifies explicitly per
  service (see `bus = "session" | "system"` in 3.2); don't assume a global
  default.
- **Hot-reload watcher backend**: implement via a trait
  (`ConfigWatcher: Stream<Item = ReloadEvent>`) with an `inotify`-backed
  impl for Linux and a dumb polling impl as the portable fallback, so this
  doesn't become an unstated Linux-only (or Linux-inotify-only) dependency
  by accident.

## 10. Recommended Crates (verify current versions/APIs before use — check
docs.rs, since zbus's low-level message API in particular has shifted
across major versions)

- `zbus` (low-level `Message` API, not the `#[proxy]`/`#[interface]` macros)
- `zvariant` (comes with zbus, for `Value`/`Signature`)
- `tokio` (async runtime, `UnixStream`/`UnixListener` for Varlink)
- `serde` + `serde_json` (config parsing target structs; Varlink wire format)
- `toml` (config file parsing)
- `inotify` (Linux hot-reload backend; must sit behind the trait from
  Section 9, not called directly from generic code)
- Avoid: `varlink` crate (codegen-first, wrong fit — see 3.4), any
  `libsystemd`/`systemd`-named crate (see Section 4).

---

Deliverable for this session: a working Rust workspace implementing the
above, following the module layout in Section 5, with the build order in
Section 6 as a rough milestone guide. Ask clarifying questions only where
Section 9 doesn't already give you a default to proceed with.
