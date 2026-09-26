# The busbridge app registration protocol

This document is the contract; `src/registration.rs`, `src/varlink/peer.rs`,
and `src/control.rs` are the implementation. If they disagree, that's a bug
in the implementation.

## Why this exists

Before this protocol, a Varlink app that wanted to appear as a D-Bus
service needed: a conf.d TOML file, a filesystem path for its
introspection XML, and its own dedicated `varlink.listen` socket that
busbridge would dial - three things to set up before the first line of
"be a D-Bus service" code could run, and none of them things a Varlink
app should need to know or care about. This protocol collapses all of
that into one thing: connect to one address, send one request.

## The address

```
$XDG_RUNTIME_DIR/busbridge/busbridge.sock
```

falling back to `/run/busbridge/busbridge.sock` if `$XDG_RUNTIME_DIR`
isn't set (matching `control.rs::default_paths`'s existing fallback for
the same reasons - see its doc comment). This is the *same* socket
busbridge's own resident/handoff protocol already used (previously named
`control.sock`); a connection to it is either a legacy handoff request or
a registration, told apart by whether the first frame has a `method`
field (see "Frame shapes" below). An app never needs to know the handoff
protocol exists.

This socket can also be systemd socket-activated
(`service-templates/systemd/busbridge.socket`), so busbridge doesn't
need to be started ahead of time at all - the very first app registration
(or handoff) brings it up, the same "optional accelerant, never a
requirement" fd-inheritance mechanism `varlink/server.rs`'s per-service
listeners already used (`src/activation.rs`), now shared correctly
between both kinds of listener - see that module's doc comment.

## Wire format

Every frame is a JSON object followed by a single NUL (`\0`) byte - the
same framing every other Varlink connection in this project already uses
(`varlink/mod.rs::read_framed`/`write_framed`). There is no length
prefix; NUL is the only frame boundary.

Every frame is one JSON object with up to five fields, all optional
except where a specific frame shape requires them:

| Field             | Type          | Meaning |
|-------------------|---------------|---------|
| `id`              | any JSON scalar | Present on a request that needs a reply, and echoed back unchanged on that reply. Absent on anything that doesn't need one. |
| `method`          | string        | Present on a call/push; absent on a reply. |
| `subscription_id` | string        | Present only on a subscription event push (never combined with `method` or `id`). |
| `parameters`      | any JSON value (usually an object) | The call's arguments, or the reply's return value(s). |
| `error`           | string        | Present on a failed reply instead of (well-formed) success; `parameters` on an error frame is that error's own payload, conventionally `{"message": "..."}`. |

## Frame shapes

Six distinct shapes exist, three used by the app, three used by
busbridge. This table is `varlink/peer.rs`'s `Frame` doc comment,
repeated here with the worked examples:

### 1. App -> busbridge: `Register` (the app sends this once, first)

```json
{"id": "1", "method": "org.busbridge.Registration.Register", "parameters": {
  "bus": "session",
  "name": "org.example.MyService",
  "object_path": "/org/example/MyService",
  "introspection_xml": "<node><interface name=\"org.example.MyService\">...</interface></node>",
  "passthrough": false,
  "methods": [
    {"dbus_interface": "org.example.MyService", "dbus_method": "DoThing",
     "varlink_method": "org.example.MyService.DoThing", "args": ["x", "y"]}
  ],
  "signals": [
    {"dbus_interface": "org.example.MyService", "dbus_signal": "SomethingHappened",
     "direction": "inbound_push"}
  ],
  "properties": [
    {"dbus_interface": "org.example.MyService", "dbus_property": "Status",
     "varlink_method": "org.example.MyService.Status"}
  ]
}}
```

`bus`, `name`, and `object_path` are the only required fields.
`methods`/`signals`/`properties` are exactly `config/schema.rs`'s
`MethodMapping`/`SignalMapping`/`PropertyMapping` shapes, carried over
JSON instead of TOML - see "Why inline XML, not a new schema" below for
why `introspection_xml` is the one field that isn't a straight TOML->JSON
translation. Everything here is optional except the three required
fields: a `passthrough: true` service with no `methods` at all is
completely valid (and often the simplest thing to write - see "Which
style should my app use" below).

Reply, success:
```json
{"id": "1", "parameters": {"object_path": "/org/example/MyService"}}
```
(`object_path` is echoed back rather than left implicit, since a future
version of this protocol may let busbridge allocate one, the way
`[[registrable]]`'s dynamic objects already do.)

Reply, failure:
```json
{"id": "1", "error": "org.busbridge.Registration.NameAlreadyOwned", "parameters": {"message": "org.example.MyService is already registered on this bus"}}
```

Defined error names: `org.busbridge.Registration.NameAlreadyOwned` (the
name is already owned - by another registration, a conf.d service, or a
completely unrelated D-Bus client), `org.busbridge.Registration.InvalidParameters`,
`org.busbridge.Registration.InvalidIntrospectionXml`,
`org.busbridge.Registration.BusUnavailable` (couldn't connect to the
requested bus at all), `org.busbridge.Registration.UnknownMethod` (the
first frame wasn't a `Register` call).

### Frame shape 1b: a third party registering a dynamic object under a `registrables` entry

Complementary to `Register`, and usable by a completely different
connection than the one that registered the parent service: if a
service (registered through `Register`, or a conf.d `[service]` with
`[[registrable]]` entries - it makes no difference which) declares
`registrables`, any app can register a dynamic object under one of those
entries the same way `[[registrable]]`'s dedicated-socket mechanism
(`dbus/dynamic_object.rs`) already lets it, just reachable through this
same universal address instead of a socket specific to that one service.

```json
{"id": "5", "method": "org.busbridge.Registration.RegisterDynamicObject", "parameters": {
  "bus_name": "org.freedesktop.StatusNotifierWatcher",
  "dbus_interface": "org.kde.StatusNotifierItem",
  "id": "my-app-chosen-id"
}}
```

`bus_name` + `dbus_interface` identify which `registrables` entry to use
(`id` is only read if that entry's `id_source` is `app_supplied` - see
`config/schema.rs`'s `RegistrableConfig`). busbridge checks every
currently-running bus's dispatch table for a matching entry, so the
caller doesn't need to know which bus the parent service is actually on.

Reply:
```json
{"id": "5", "parameters": {"object_path": "/StatusNotifierItem/0"}}
```
or
```json
{"id": "5", "error": "org.busbridge.Registration.UnknownRegistrable", "parameters": {"message": "..."}}
```

Once acked, this connection *is* the dynamic object for its whole
lifetime - frame shapes 2 and 3 below apply to it exactly as they do to
a `Register`ed service, because it's the same underlying code
(`dbus/dynamic_object.rs::register`) either way. One asymmetry worth
knowing: `register()` only ever writes an ack on success, so a failure
this deep in (introspection XML that doesn't declare the requested
interface, `id_source=app_supplied` with no `id` given) closes the
connection rather than sending back a `Frame`-shaped error - the same
pre-existing behavior the conf.d-driven path has always had.

### 2. Busbridge -> app: an inbound D-Bus call, forwarded

```json
{"method": "org.example.MyService.DoThing", "parameters": {"x": 1, "y": 2, "_dbus_sender": ":1.234"}}
```

No `id` - only one of these is ever in flight at a time per connection
(exactly `dbus/dynamic_object.rs`'s existing, proven convention: FIFO
position alone disambiguates, since there's never more than one
outstanding). `_dbus_sender` is added the same way it already is
everywhere else in this project (`dispatch.rs::insert_dbus_sender`).

App's reply:
```json
{"parameters": {"result": 42}}
```
or
```json
{"error": "org.example.MyError", "parameters": {"message": "..."}}
```

### 3. App -> busbridge: a push (a signal, or a property change)

```json
{"method": "org.example.MyService.SomethingHappened", "parameters": {"detail": "..."}}
```

No `id`, no reply. The `"{interface}.{member}"` method name is matched
against the `signals` this service declared at registration (or, for a
`passthrough: true` service, forwarded generically the same way
`varlink/server.rs`'s existing static push handling already does -
`handle_push_request`, reused verbatim here). A push that matches
nothing is dropped with a `warn!` log, not an error - there's no reply
channel for one to travel back on.

### Properties: a different, simpler convention than signals/calls

A declared `properties` entry's Get/Set does *not* use the
`"{interface}.{member}"` convention frame shapes 2 and 3 use - it
forwards as a **bare call to that property's own `varlink_method`**,
exactly as declared in `Register`'s `properties` list, with no
`interface`/`property` fields added:

- Get: busbridge sends `{"method": "<varlink_method>"}` (no
  `parameters` at all) and expects `{"parameters": <the value>}` back.
- Set: busbridge sends `{"method": "<varlink_method>", "parameters": {"value": <new value>}}`.
- GetAll: busbridge issues one such Get per declared property for the
  requested interface and assembles the results itself - the app never
  sees a single combined "GetAll" call.

This is `dispatch.rs::fetch_property_value`'s existing convention
(shared with conf.d services - registration doesn't add or change
anything property-specific, it flows through the exact same code), and
is worth calling out explicitly because it's easy to assume properties
work like the SNI-style `org.freedesktop.DBus.Properties.Get` forwarding
`dbus/dynamic_object.rs` uses for *dynamically registered objects*
(`[[registrable]]`) - a different, XML-driven mechanism this protocol
doesn't use. See `examples/registration_client.rs` for a worked example
of both a method and a property.

### 4. App -> busbridge: an `org.busbridge.Peer.*` request

The complement to frame shape 1: after registering, the *app* can also
ask busbridge to do something on its behalf, over the same connection.
Three methods exist:

**`org.busbridge.Peer.Call`** - an arbitrary outbound D-Bus call, exactly
like `dbus/bus_proxy.rs`'s standalone `Call` (same reply/error shaping;
this is that same code, reused):
```json
{"id": "2", "method": "org.busbridge.Peer.Call", "parameters": {
  "destination": "org.freedesktop.Notifications",
  "path": "/org/freedesktop/Notifications",
  "interface": "org.freedesktop.Notifications",
  "method": "GetCapabilities",
  "args": []
}}
```
Reply: `{"id": "2", "parameters": {"reply": [...]}}` or
`{"id": "2", "error": "...", "parameters": {"message": "..."}}` - the
D-Bus target's own error name passes straight through where there is
one, the same default convention `errors.rs` uses everywhere else.

**`org.busbridge.Peer.Subscribe`** - matches `dbus/bus_proxy.rs`'s own
`Subscribe` (same fields, same D-Bus match-rule semantics), but since
this connection carries other traffic too, it can't hold the connection
open the way that dedicated one does - events arrive as separate
frame-shape-6 pushes instead:
```json
{"id": "3", "method": "org.busbridge.Peer.Subscribe", "parameters": {
  "sender": null, "path": null,
  "interface": "org.freedesktop.Notifications", "member": "ActionInvoked"
}}
```
Reply: `{"id": "3", "parameters": {"subscription_id": "sub-0"}}`.

**`org.busbridge.Peer.Unsubscribe`**:
```json
{"id": "4", "method": "org.busbridge.Peer.Unsubscribe", "parameters": {"subscription_id": "sub-0"}}
```
Reply: `{"id": "4", "parameters": {}}` or
`{"id": "4", "error": "org.busbridge.Peer.UnknownSubscription", ...}`.

**`org.busbridge.Peer.FindObjects`** - given a D-Bus interface name, finds
every object implementing it, across every currently-connected bus name
(or a caller-supplied subset), by walking each name's object tree with a
bounded breadth-first introspection search:
```json
{"id": "5", "method": "org.busbridge.Peer.FindObjects", "parameters": {
  "interface": "org.kde.StatusNotifierItem",
  "bus_names": [":1.42"]
}}
```
`bus_names` is optional; omitting it searches every currently-connected
unique bus name (via `ListNames`). Reply:
```json
{"id": "5", "parameters": {"items": [
  {"bus_name": ":1.42", "object_path": "/org/example/App/TrayIcon"}
]}}
```
This exists because "find the object on this bus implementing interface
X" has no reliable path convention to rely on - real applications place
these objects at wildly different depths and names - and every app that
needed this ended up hand-rolling its own version of the same walk,
each slightly differently, and each prone to the same kinds of bugs: not
going deep enough, or giving up on the whole search after the first
failed introspection instead of just pruning that one branch. Since
busbridge already has the bus connection open, doing this walk once here
means N registered apps don't each redundantly re-walk the same bus on
their own. `bus_name` in each result item is always the *unique*
connection name (`:1.x`), not a well-known name that connection might
also own, since a well-known name can change hands between the search
and using the result and a unique name can't.

Not cached: every call does a fresh `ListNames` and a fresh walk. A
caller that already has a good guess at where to look (e.g. a bus name
it just saw appear via its own `NameOwnerChanged` subscription) should
pass `bus_names` to scope the search rather than re-scanning the whole
bus each time.

### 5. Busbridge -> app: reply to an `org.busbridge.Peer.*` request

Covered above alongside each request - always `{"id": <same id>, ...}`,
never a `method` field.

### 6. Busbridge -> app: a subscription event

```json
{"subscription_id": "sub-0", "parameters": {
  "sender": ":1.9", "path": "/org/freedesktop/Notifications",
  "interface": "org.freedesktop.Notifications", "member": "ActionInvoked",
  "args": [3, "default"]
}}
```
No `id`, no `method`. Keeps arriving until the app sends `Unsubscribe`
for that `subscription_id` or the connection closes.

## Which bus

`bus` in the `Register` call is `"session"` or `"system"`, independent of
whatever conf.d has configured. If this busbridge process doesn't
already have a connection open to the requested bus (e.g. it started
with an empty or session-only conf.d, and an app now registers a
`system`-bus service), it opens one on demand - see `lib.rs`'s
`BusRegistry::get_or_start`. This is deliberately different from conf.d
services, which only ever run on buses conf.d actually configured
something for at startup: a registration is a live, momentary request,
not a static description of the world, so "the bus this process happened
to be started for" isn't a reasonable restriction to put on it.

## Idle-exit

A registered service's connection holds an `ActivityGuard`
(`idle.rs::ActivityTracker::hold`) for its entire lifetime, the same
mechanism `dbus/dynamic_object.rs` already uses for one dynamically
registered *object* - here it's one level up, covering a whole service.
As long as the connection is open, this bus counts as non-idle regardless
of how much or little traffic flows over it. `lib.rs::wait_for_registry_idle`
also has to account for a bus that comes into existence *after* the
process's idle-wait has already started (see that function's doc
comment) - a one-shot check computed once at startup, which was sufficient
before this protocol existed, would otherwise be able to idle-exit out
from under a connection that registered moments later.

## Teardown

When the app closes the connection (or busbridge's read loop otherwise
errors out): busbridge releases the D-Bus name, removes every method/
signal/property/registrable entry that came from this registration, and
drops the cached introspection for it. This is unconditional and
immediate - there's no separate "goodbye" message, because the
connection closing *is* the goodbye message. (This mirrors a lesson from
StatusNotifierItem's own teardown convention - see
`docs/ARCHITECTURE.md`'s addendum on that - and generalizes it: closing a
connection is a sufficient and sufficiently detectable signal on its own,
without needing a paired explicit unregister call.)

## Why inline XML, not a new schema

`methods`/`signals`/`properties` reuse `config/schema.rs`'s existing
types directly - JSON instead of TOML, same fields. The one place this
protocol needed something new is `introspection_xml`: conf.d services
point at a file on disk; a registering app has no filesystem convention
to point at, so it sends the XML text itself instead.

The alternative would be inventing a JSON type schema (something like
`{"name": "DoThing", "in_args": [{"name": "x", "type": "i"}], ...}`) and
parsing *that* instead of XML. This was deliberately rejected: D-Bus
introspection XML is a format every D-Bus tool, binding, and human
already knows how to read and write, it's exactly as expressive as this
protocol needs (arg names and D-Bus type signatures, nothing more), and
`dbus/introspect.rs::parse` already exists, is already tested, and is
already exactly what a conf.d service's `introspection_xml` file gets
parsed with (`introspect::load_from_path` is a two-line wrapper around
the same `parse` function this protocol calls directly on inline text).
Inventing a parallel JSON schema for the same information would be new
surface for zero benefit - see `docs/ARCHITECTURE.md`'s stance on not
inventing new representations where an existing, adequate one is right
there.

## Which style should my app use

Three ways to describe a service, in increasing order of how much the app
needs to know about its own D-Bus-facing shape:

1. **`passthrough: true`, no `methods`/`signals`/`properties` at all.**
   Every call gets forwarded generically (the same generic conversion
   `dbus/dispatch.rs::passthrough_forward_call` already does for conf.d
   passthrough services - arg values by position, no names). Simplest to
   write; least precise types, least helpful errors for a caller who gets
   something wrong.
2. **`passthrough: true` plus `introspection_xml`.** Passthrough calls
   still forward generically at the D-Bus wire level, but a legacy D-Bus
   client's `Introspect()` gets a real, accurate answer, and Get/GetAll
   on a declared property get real types instead of `int64`-collapsed
   guesses. The pragmatic middle ground for most services.
3. **Explicit `methods`/`signals`/`properties` (with or without
   `introspection_xml` alongside them for accurate types).** Full control
   over the Varlink-facing method names and D-Bus error mapping per
   method - the same thing a hand-written conf.d file gives a conf.d
   service.

## Resolver discoverability

Every interface a registered service declares `methods`/`signals`/
`properties` for is automatically resolvable through
`org.varlink.resolver` (`dbus/resolver.rs`), exactly as if it were a
`[[resolve]]`-configured conf.d entry - `registration.rs::insert_service`
generates one on registration, and removes it on teardown. A Varlink-
native client can find and call a registered service purely by interface
name, with no `[[resolve]]` config anywhere, on either side. (A
`passthrough`-only service with no declared interfaces at all has
nothing to advertise a name for, so it doesn't get a resolvable entry -
it's still reachable as an ordinary D-Bus destination, e.g. through
`org.busbridge.BusProxy` - `dbus/bus_proxy.rs`.)

This works even for a bus that had no connection at all when this
process started: the resolver listener is itself registry-aware
(`dbus/resolver.rs::start_registry_aware_listener`), re-checking every
currently-running bus on each request rather than a fixed snapshot taken
once at startup - the same fix `lib.rs`'s hot-reload and idle-exit
needed for the same underlying reason (a `BusRegistry` that can grow at
runtime - see this document's "Which bus" section and `lib.rs`'s
`async_main` doc comment).

## What this doesn't do (yet)

- **File-descriptor passing** - not supported anywhere in this project
  yet (`docs/ARCHITECTURE.md` covers this gap in general terms); this
  protocol doesn't change that.
- **`RegisterDynamicObject`'s failure path has no reply frame** - see
  that section above; `dynamic_object.rs::register` was written for a
  caller that never needed to send an error reply, and still doesn't in
  the one case (introspection/id-source problems) where this protocol's
  general convention would want one.
