# busbridge — Architecture

This document supersedes `docs/DESIGN_BRIEF_V1.md`. It reassesses the
project against current D-Bus/Varlink ecosystem research, rather than
taking the original brief's ideas as given, and lays out where the
project should go next: a bidirectional D-Bus ↔ Varlink **compatibility
and migration layer**, not a message-conversion shim.

Contents:
1. [Assessment of the current codebase](#1-assessment-of-the-current-codebase)
2. [Research-backed recommended architecture](#2-research-backed-recommended-architecture)
3. [What to keep, change, or discard](#3-what-to-keep-change-or-discard)
4. [Tray applications and other stateful D-Bus APIs](#4-tray-applications-and-other-stateful-d-bus-apis)
5. [Varlink-facing API philosophy](#5-varlink-facing-api-philosophy)
6. [Recommended project structure](#6-recommended-project-structure)
7. [Implementation roadmap](#7-implementation-roadmap)
8. [Compatibility, performance, security, correctness](#8-compatibility-performance-security-correctness)

---

## 1. Assessment of the current codebase

The phase-0 codebase (~8,800 lines of Rust, `docs/DESIGN_BRIEF_V1.md`'s
brief) is a competently engineered **config-driven message relay**. Its
foundational engineering choices are sound and should not be revisited:

- **zbus's low-level `Message` API instead of `#[proxy]`/`#[interface]`
  macros.** Correct — those macros assume compile-time-known signatures,
  incompatible with generic dispatch.
- **A hand-rolled Varlink client/server instead of the `varlink` crate.**
  Correct — that crate is codegen-first (`.varlink` → generated structs),
  the wrong fit for a generic bridge.
- **Strict init-agnosticism** (no `libsystemd`, no `sd_notify`, no
  `SystemdService=`, defensive `LISTEN_FDS` parsing). This is a genuine
  strength worth preserving deliberately, not just historically — see
  §8 for why it stays relevant even as systemd itself leans harder into
  Varlink.
- **Resident/handoff control channel** solving the "N bus-activated
  names, one process" problem without an init-specific trick. Real,
  race-tested, and still the right design.
- **Passthrough mode with system-XML auto-discovery.** Genuinely useful:
  many real interfaces (portals, `org.freedesktop.Notifications`) already
  ship machine-readable introspection XML under
  `/usr/share/dbus-1/interfaces/`, and recovering real types from that,
  for free, is a good pragmatic floor above pure guesswork.
- **The standard `org.varlink.resolver` protocol on a well-known socket.**
  This is a real, valuable point of integration with the existing Varlink
  ecosystem (see §5) and should become the primary discovery path.

Where it falls short of "a serious compatibility and migration layer,"
per the brief's own framing:

**It has no persistent object/service model.** The dispatch table is
`HashMap<(BusName, Interface, Member), Mapping>` — a lookup from a
message shape to an action, not a representation of a D-Bus object as a
stateful thing with a lifecycle, a set of implemented interfaces, and a
current property state. `DynamicObject` (the one place with any runtime
lifecycle) is a special-cased bag bolted on for the SNI case, not a
general primitive. Concretely, this means:

- **No property caching anywhere.** Every `Get`/`GetAll` round-trips to
  the backend, even for properties a well-behaved interface declares as
  change-notified (`org.freedesktop.DBus.Property.EmitsChangedSignal`).
  For a tray host that polls several items' `IconPixmap` on every panel
  repaint, this is a real, avoidable performance cost.
- **No generic `org.freedesktop.DBus.ObjectManager` support**, in either
  direction. This is a widely used convention (BlueZ, UDisks2,
  NetworkManager, most portals) for "here is a dynamic, enumerable,
  add/remove-notified set of objects," and it's exactly the shape SNI's
  own registration problem is a *special case* of. The current design
  solved SNI specifically (`[[registrable]]`) rather than solving the
  general pattern SNI happens to instantiate.
- **JSON is the actual internal data model**, not just the wire format at
  the Varlink edge. `convert.rs` converts `zvariant::Value` straight to
  `serde_json::Value` and back; nothing else in the crate operates on a
  representation that remembers "this is a dict of variants because it's
  a property bag" versus "this is just a dict." That's an acceptable
  *wire* choice (see §5) but an inadequate *internal* one for a tool that
  wants to reason about caching, ownership, and object identity.
- **No name-ownership modeling.** The bridge calls `RequestName` and
  otherwise doesn't participate in D-Bus's ownership semantics —
  `NameOwnerChanged`, activation retries, or reacting to a watched
  service's restart. §4 shows why this specifically breaks a correct SNI
  implementation.

**A concrete protocol-correctness issue in the existing SNI design:**
`RegisterStatusNotifierItem` registers the item's *own D-Bus service
name* (per the freedesktop spec, a string like
`org.freedesktop.StatusNotifierItem-4077-1`), not merely an object path.
Hosts subsequently talk to that name's own `/StatusNotifierItem` object
directly. The phase-0 `[[registrable]]` design allocates a path under
`path_prefix` on the bridge's *single, shared* bus connection/name. Some
newer hosts accept a `"busname,objectpath"` convention to disambiguate
multiple items sharing one name, but this is **not universally
implemented** — plenty of older/simpler hosts assume exactly one item at
the fixed path `/StatusNotifierItem` on the given name. Two Varlink-native
items registered concurrently behind one shared bridge-owned bus name
would silently collide for such hosts. See §4 for the fix (dedicated bus
name per registered item, by default).

**Other real gaps, not fatal but worth naming honestly:**

- **No file-descriptor passing.** Explicitly out of scope in the phase-0
  brief. This matters more than it might seem: much of the modern
  desktop stack (xdg-desktop-portal's `OpenURI`/`ScreenCast`/`Camera`,
  PipeWire negotiation) relies on D-Bus's UNIX-fd message type, and
  Varlink has its own SCM_RIGHTS-based convention for the same thing. Not
  needed for SNI/MPRIS/Notifications, but a real ceiling on how far
  "compatibility layer" can be taken without it.
- **Streaming is ad hoc, not a first-class concept.** Outbound
  subscriptions (`direction = "outbound_subscribe"`) and inbound pushes
  are both real, but neither has a defined backpressure or overflow
  policy. A slow Varlink consumer of a `"more": true` stream can, in the
  current design, exert backpressure onto the loop reading D-Bus
  signals, which risks stalling delivery to *unrelated* subscribers.
- **Error mapping is a single global default** (Varlink error name
  passes through as the D-Bus error name), with no attempt at the small
  set of well-known D-Bus error categories (`UnknownMethod`,
  `UnknownProperty`, `InvalidArgs`, `NameHasNoOwner`, `ServiceUnknown`)
  that many D-Bus client libraries pattern-match on by name.

None of this means starting over. It means the static
config-driven-relay machinery (§3.2 in the old brief) should become one
*adapter* onto a real object model, rather than being the whole system.

**Addendum, found while adding `tests/sni_tray.rs`:** the phase-0 code
had no test at all for a Varlink-native app registering as a dynamic
object and then actually receiving a forwarded D-Bus call — the only
existing SNI test (`tests/end_to_end.rs`) exercises the *watcher's*
static mapping, not an *item's* dynamic registration. Writing that test
surfaced a real bug, now fixed: `handle_dynamic_call` looked up argument
names/types in `BridgeState::introspection`, which is populated only
from each `[service]` block's *static* `introspection_xml` — never from
a `[[registrable]]`'s own `introspection_xml`. Since a purely dynamic
service typically has no static introspection XML at all, every call
into a dynamically registered object (`Activate`, `Properties.Get`, ...)
silently got empty in/out-arg descriptors, which drops every positional
argument on the floor in both directions. `DynamicObject` now carries its
own interface descriptor map, loaded once at registration time from the
registrable's own XML, and `handle_dynamic_call` reads from there
instead. This is exactly the kind of gap a config-driven-relay's own test
suite is prone to leaving open: the static and dynamic paths look similar
enough in the code that "we already tested SNI" was true and misleading
at the same time.

The same test-writing pass also surfaced a real (if smaller) sharp edge,
not a bug: pushed-event method names use **two different conventions**
depending on path. The static, config-driven push path
(`varlink/server.rs`, `[[signal]] direction = "inbound_push"`) requires
`"{interface}.{member}"`, because one shared push connection there can
carry events for several different interfaces. The dynamic/registrable
push path (`dynamic_object.rs::handle_push`) expects the **bare** member
name instead, since a registrable connection is already scoped to
exactly one interface. Both are individually reasonable, but the
asymmetry is easy to trip over — the first draft of `tests/sni_tray.rs`
did, silently: `handle_push` just doesn't find a matching signal and
drops the event with no error surfaced to the app, so a real backend
integrating against this would see nothing and have no indication why.
Worth a warning-level log line for exactly this "push method didn't
match any declared signal" case if one doesn't already fire (it does:
`handle_push` logs a `warn!` on the no-match branch — but it's easy to
miss in practice, which is itself evidence for §5's push toward more
self-describing, less convention-dependent wire shapes for known
interfaces).

---

## 2. Research-backed recommended architecture

### What the ecosystem actually looks like right now

A few things worth being explicit about, since they shape the
architecture more than the original brief's framing did:

- **systemd itself has been moving hard toward Varlink.** Lennart
  Poettering has spoken publicly (All Systems Go, 2024) about D-Bus's
  long-standing difficulties as an IPC mechanism for systemd's own needs,
  and recent systemd release cycles have migrated an increasing number of
  internal APIs (`hostnamed`, `networkd`, `resolved`, `udev`,
  `userdbd`, sysupdate/repart/pcrextend and others) to Varlink,
  accessible uniformly via `varlinkctl`. This is direct, independent
  validation of this project's premise: Varlink is becoming a real
  citizen of the Linux system layer, not a niche curiosity, and a serious
  D-Bus↔Varlink bridge is a legitimate, useful thing to build. It's also
  a reason **not** to assume systemd will make this project redundant:
  systemd's own Varlink work is systemd-internal-API-shaped (its own
  daemons talking to each other and to `systemctl`-adjacent tools), not a
  general bridge for arbitrary third-party D-Bus session/system services
  like SNI, MPRIS, or Notifications. Someone (in a public systemd issue
  thread) directly asked Poettering whether it would be "feasible to rip
  out pre-existing native D-Bus APIs and provide them via a small bridge
  which does translation to/from Varlink" — a fair description of half of
  what this project does — and nothing has shipped to do that generally.
  busbridge, being strictly init-agnostic, is complementary to that
  trend rather than competing with it, and its value doesn't depend on
  systemd shipping one itself.
- **No existing general-purpose D-Bus↔Varlink bridge exists.** What does
  exist: `systemd`'s own `varlink-http-bridge` (a *transparent byte proxy*
  from Varlink to HTTP/WebSocket for remote access — a transport bridge,
  not a protocol translator), `varlinkctl`'s bridge-executable convention
  for pluggable transports, and various from-scratch Varlink client
  libraries (`vali`, `varlink-glib`, `zlink`/`kirmes`). None of them
  cross the D-Bus/Varlink protocol boundary itself. This is a genuinely
  open niche.
- **Varlink has no native concept of "property" or "signal."** This is
  the single most important protocol-mismatch fact to internalize, and
  the original brief's "D-Bus ↔ semantic model ↔ Varlink" idea only works
  if this is respected rather than paved over. Varlink's primitives are:
  a method call, an optional single streamed reply sequence
  (`"more": true`), and a service-level introspection convention
  (`org.varlink.service.GetInfo`/`GetInterfaceDescription`). "Properties"
  and "signals" are D-Bus/desktop conventions layered *on top* of a
  request/reply protocol by every D-Bus service's own interface design —
  there is no wire-level primitive to detect or infer them from an
  arbitrary Varlink interface. Concretely: **do not** attempt to guess
  "this Varlink method looks like a property getter" or "this streaming
  method is secretly a signal" from shape or naming heuristics for the
  Varlink→D-Bus direction. Require a small, explicit interface
  descriptor (the same kind of authoritative XML/manifest already used
  for the D-Bus→Varlink direction) that states which methods are
  properties, which streaming calls represent signal classes, and what
  their D-Bus-side names and types are. A "smart" auto-mapper here isn't
  a shortcut, it's a source of protocol-incorrect behavior.
- **`org.freedesktop.DBus.ObjectManager` is the right general primitive
  for "a dynamic, add/remove-notified set of objects,"** and it already
  generalizes what `[[registrable]]` was special-cased for. BlueZ,
  UDisks2, NetworkManager, and most portals use it. Building generic
  ObjectManager support (both directions) once buys SNI's dynamic-item
  registration *and* every future "watch a changing set of objects" case,
  instead of one bespoke mechanism per interface that needs it.

### The core architectural decision: a semantic model, but a small, D-Bus-shaped one

Introduce an internal, protocol-independent layer with four concepts,
directly mirroring D-Bus's own object model (not a broader invention):

```
Bus        session | system
Service    a D-Bus name (well-known and/or unique), with ownership state
Object     a path, holding an ordered set of implemented Interfaces
Interface  name + Methods + Properties + Signals, sourced from an
           authoritative descriptor (introspection XML for D-Bus-side
           interfaces; a small manifest for Varlink-side ones)
```

Plus two supporting entities:

```
Subscription   a live signal/event stream (D-Bus match rule <-> a
               persistent Varlink "more" call), with a defined
               backpressure/overflow policy
Registration   a Varlink connection that has registered a dynamic Object
               (the generalization of DynamicObject/`[[registrable]]`)
```

Internally, values are represented in a `model::Value` type that is
**lossless relative to `zvariant::Value`** (keeps the signature alongside
the value; knows the difference between a dict-of-variants that's a
property bag and one that's "just a dict"; distinguishes object paths,
signatures, and byte arrays from plain strings/int-arrays). `serde_json`
is used only at the two wire edges (Varlink-in, Varlink-out) — not as the
thing dispatch, caching, or the model itself operate on. This is a
narrowing of scope relative to today's `convert.rs` (which already does
the hard part — signature-guided JSON→D-Bus conversion — correctly), not
a rewrite of it: the existing signature parser and conversion logic
become the *codec at the model boundary* instead of being called ad hoc
from `dispatch.rs` and `dynamic_object.rs` independently.

This model is deliberately **not** an attempt at a protocol-independent
IPC abstraction in the abstract sense — it doesn't try to also fit, say, a
future Wayland-protocol or COM-style bridge. It's scoped tightly to what
D-Bus's actual object model requires. That scoping is a feature: the
brief's own list of "ideas worth investigating" includes "should busbridge
eventually support other IPC abstractions" — the honest answer, given
the constraint to prioritize something *implementable and useful*, is
**not now, and don't design for it speculatively.** Keep the D-Bus and
Varlink adapters cleanly separated from the model (already mostly true in
the existing `dbus/` vs `varlink/` module split) so that *if* a third
protocol ever became a real, funded reason to extend this, the model
wouldn't need to be redesigned — but do not build abstraction for a
protocol that doesn't exist yet. That's the unnecessary-abstraction trap
the brief explicitly warned against, and the biggest failure mode for a
project like this.

### Two shapes of interaction — keep both, don't unify them

The existing codebase already has this insight half-formed (static
mappings vs. `bus_proxy`'s generic client); make it explicit and
first-class rather than two features that happened to both get built:

1. **Known interfaces** (SNI, MPRIS, Notifications, and whatever else
   busbridge ships descriptors for). For these, busbridge behaves like a
   real protocol gateway: it maintains model Objects, generates an
   idiomatic per-interface Varlink API (§5), caches properties correctly,
   and validates types in both directions using the authoritative
   descriptor. This is the primary, "serious" path.
2. **Arbitrary/unknown destinations** — the existing `bus_proxy` module
   and the passthrough JSON-shape-inference fallback. These remain
   necessary and valuable (there is no way to have a typed model for an
   interface nobody described), but they are explicitly the **fallback**,
   not the model for how the primary interfaces work. Keep them, keep the
   README's honest framing of passthrough's inference floor, but don't
   let their generic-JSON-blob shape leak into how known interfaces are
   exposed.

---

## 3. What to keep, change, or discard

**Keep as-is:**
- Resident/handoff control channel and the whole init-agnostic activation
  story (§4 of the old brief). No changes needed; this is orthogonal to
  the object-model work.
- `zbus`'s low-level Message API, hand-rolled Varlink wire framing.
- `org.varlink.resolver` support — becomes more important, not less (§5).
- Passthrough mode + system-XML auto-discovery, repositioned explicitly
  as the fallback path for undescribed interfaces.
- `bus_proxy`'s generic outbound client — the right tool for "arbitrary,
  unconfigured destination," e.g. the SNI-host-querying-arbitrary-items
  case in the *legacy-item, Varlink-host* direction.
- Idle-exit and telemetry — sound, need only small extensions (per-object
  and per-subscription activity hooks, which the existing `ActivityGuard`
  pattern already generalizes to cleanly).

**Change substantially:**
- `convert.rs` becomes the **codec at the model boundary**
  (`model::Value <-> zvariant::Value` and `model::Value <-> JSON`)
  instead of being called directly, ad hoc, from `dispatch.rs` and
  `dynamic_object.rs`. The signature-parsing and JSON-coercion logic
  already there is correct and is reused, not rewritten.
- The config schema's `[[method]]`/`[[signal]]`/`[[property]]` entries
  become declarations that populate an `Interface` in the model, rather
  than direct rows in a dispatch `HashMap`. Behaviourally very similar at
  the config-authoring level; structurally, this is what makes caching,
  ownership tracking, and generic `ObjectManager` support fall out for
  free instead of needing bespoke code per feature (as `[[registrable]]`
  currently is).
- Dynamic object registration: give each registered item its **own
  dedicated D-Bus well-known name by default** (see §4), not a path under
  a name shared with other registered items. Make shared-name mode
  available for interfaces/hosts confirmed to support the
  `"busname,objectpath"` convention, but don't make it the default.
- Error mapping: add a small, built-in table for the well-known D-Bus
  error categories (`UnknownMethod`, `UnknownProperty`, `InvalidArgs`,
  `AccessDenied`, `NameHasNoOwner`, `ServiceUnknown`, `Timeout`) on top
  of — not replacing — the current "pass the name straight through"
  default for anything not in that table.
- Streaming/subscriptions: formalize as `Subscription` entities with a
  bounded buffer and an explicit "you missed N events" overflow signal
  delivered to the lagging consumer, rather than letting a slow reader's
  backpressure propagate into the shared D-Bus message loop.

**Discard / explicitly avoid building:**
- Any attempt to auto-detect "this is a property" or "this is a signal"
  from an arbitrary Varlink interface's shape (§2) — this is a
  misconception, not a shortcut. Require an explicit descriptor.
- Any move toward a protocol-independent-in-the-abstract IPC framework.
  Scope the model to D-Bus's actual object model; don't design in
  hypothetical support for a third protocol.
- Treating FD-passing as something to fake via base64/paths in JSON. If
  and when it's built, it belongs at the `model::Value` layer as a
  distinct variant with real SCM_RIGHTS handling on both wire adapters —
  otherwise, document it as an explicit, honest gap (see §7, §8) rather
  than a silent partial implementation.
- The old brief's assumption that SNI needs a bespoke "goodbye" RPC on
  disconnect (`docs/DESIGN_BRIEF_V1.md` §2.5 speculates about telling the
  watcher `UnregisterStatusNotifierItem`). **No such method exists in the
  StatusNotifierWatcher spec.** Per spec, watchers detect item
  disappearance purely from the item's bus name disappearing off the bus
  (the D-Bus equivalent of `NameOwnerChanged`) — releasing the name *is*
  the correct, complete teardown signal. Don't build machinery for a
  callback that isn't part of the protocol.

---

## 4. Tray applications and other stateful D-Bus APIs

StatusNotifierItem/Watcher is a good primary test case for exactly the
reason the brief says (small, well-documented, exercises both methods and
signals) — but its *registration handshake* is also the single most
stateful, hardest-to-get-right part of the whole system, which argues for
validating the new model/codegen pipeline on a **simpler** interface
first (see §7's roadmap reasoning) and applying it to SNI once proven.

### The four directions, and which ones need what

1. **Legacy D-Bus item → legacy D-Bus host.** busbridge is not involved.
2. **Legacy D-Bus item → Varlink-native host.** Handled by the existing
   `bus_proxy` design: the host calls busbridge's generic Varlink client
   proxy to introspect and call the item's real D-Bus object, and
   subscribes to its signals via `Subscribe`. No static config needed —
   this is the "arbitrary unconfigured destination" case from §2 and is
   already correctly designed for. Keep it, but layer the model's
   property cache underneath it for popular targets to cut down on
   repeated `GetAll` chatter from a host polling many items.
3. **Varlink-native item → legacy D-Bus host.** The interesting, harder
   direction, and where the fixes below matter:
   1. The app connects to busbridge's Varlink registration endpoint and
      declares which known interface it implements (`org.kde.
      StatusNotifierItem`) plus its initial property values.
   2. busbridge allocates a **dedicated D-Bus well-known name** for this
      item (default; e.g. following the spec's own convention,
      `org.freedesktop.StatusNotifierItem-<pid-or-counter>-<id>`), calls
      `RequestName` for it, and creates a model `Object` at
      `/StatusNotifierItem` on that name implementing the interface from
      the shipped descriptor.
   3. busbridge calls `RegisterStatusNotifierItem` on
      `org.kde.StatusNotifierWatcher`. The watcher is very likely
      bus-activatable and may not be running yet — this call should
      trigger its activation transparently; busbridge must handle
      `ServiceUnknown`/timeout with a bounded retry, since a
      brand-new session may not have a watcher available for a moment.
   4. busbridge **must track the watcher's own ownership** (watch
      `NameOwnerChanged` for `org.kde.StatusNotifierWatcher`) and
      re-issue `RegisterStatusNotifierItem` if the watcher restarts —
      real SNI watcher implementations commonly require this, and it's
      exactly the kind of thing a plain "translate this call" mapping
      can't express, because it's a reaction to a *third party's*
      lifecycle event, not to any call the item or a host made.
   5. Property `Get`/`GetAll`/`Set` on the synthetic object translate to
      Varlink calls on the app's live connection, through the model's
      property cache (populated from the registration payload and
      subsequent push updates; invalidated on the interface's own
      `EmitsChangedSignal` annotation, honored exactly as D-Bus's own
      introspection convention already defines it — don't invent a new
      annotation for this).
   6. Events the app pushes (`NewIcon`, `NewStatus`, `NewTitle`, ...) over
      its persistent Varlink connection update the cache and emit the
      **real, bespoke D-Bus signal** SNI defines for each (SNI mixes
      dedicated signals and would-be-`PropertiesChanged`-style updates —
      model both, don't fold everything into `PropertiesChanged` just
      because that's the generic D-Bus convention).
   7. On disconnect: release the object and the dedicated bus name.
      **That's the entire teardown.** No goodbye RPC (see §3) — releasing
      the name is the correct signal per spec.
   8. Idle-exit: exactly as the old brief already got right — a live
      registration is "not idle" regardless of the timer, and busbridge
      cannot exit while it holds a dynamically-registered name, since
      there's no way to "reactivate into" a synthetic identity the way
      bus activation reactivates into a static one.

4. **Varlink-native item → Varlink-native host.** No D-Bus, no busbridge,
   by design — this is the migration end-state and should show up as
   "usage went quiet" in telemetry (already a stated goal; keep it).

### Generalizing beyond SNI

The same "an app registers itself as a provider and gets an ongoing
two-way callback surface" pattern shows up elsewhere, and is worth
validating against a couple of different shapes rather than assuming SNI
proves the general case:

- **MPRIS** (`org.mpris.MediaPlayer2.*`): apps already register under
  their own bus name (`org.mpris.MediaPlayer2.spotify`), no
  watcher/registration handshake at all — mostly properties plus a
  `Seeked` signal. Good *first* validation target for the model +
  per-interface Varlink codegen pipeline precisely because it has none of
  SNI's registration statefulness.
- **`org.freedesktop.Notifications`**: mostly stateless RPC (`Notify`),
  but the eventual `ActionInvoked`/`NotificationClosed` signals must be
  routed back to whichever specific caller sent the original `Notify`
  with that notification ID — a stateful correlation problem that plain
  call-mapping can't express, and a good concrete illustration of why
  "mechanical translation isn't sufficient" for some interfaces even
  without a registration handshake like SNI's.
- **`org.freedesktop.DBus.ObjectManager`-based services** (BlueZ,
  UDisks2, NetworkManager): the general case SNI's dynamic registration
  is a special case of. Building generic ObjectManager support (a
  `Registration` can declare a *set* of objects rather than exactly one)
  subsumes SNI's mechanism rather than sitting beside it.

---

## 5. Recommended Varlink-facing API philosophy

- **One idiomatic Varlink interface per bridged D-Bus interface** for
  known interfaces (`org.kde.StatusNotifierItem`, `org.mpris.
  MediaPlayer2.Player`, ...), not one generic `Call(interface, method,
  args)` surface. This is both more Varlink-idiomatic (interfaces are
  meant to be self-describing, single-purpose, introspectable units) and
  gives a Varlink-native client an experience indistinguishable from
  talking to a real, hand-written Varlink service — which is the whole
  point of calling this a migration layer rather than a proxy.
- Every generated interface implements `org.varlink.service` for real
  (`GetInfo`, and a truthful `GetInterfaceDescription` generated from the
  model's `Interface`, not the current placeholder that just states no
  static schema exists). This is table stakes for working with generic
  Varlink tooling (`varlinkctl introspect`, etc.).
- **Properties**: prefer a Varlink-native shape (e.g. a single
  `GetState`-style call returning the whole property set as a typed
  struct, `SetX`/`SetY` methods per writable property) over mechanically
  mirroring D-Bus's three-method `Properties` interface — but keep the
  literal `org.freedesktop.DBus.Properties`-shaped mapping available
  for the passthrough/generic-fallback path, where no nicer shape can be
  synthesized without a descriptor.
- **Signals**: model as a single `Subscribe`/`WatchEvents`-style call per
  object (or per interface) that streams a tagged union of event kinds
  via `"more": true`, rather than one long-lived connection per signal
  type. This avoids a proliferation of held-open connections for a
  client that wants several signal classes from one object, and matches
  the shape of systemd's own Varlink notification-style APIs.
- **Discovery**: `org.varlink.resolver` is the client-facing contract.
  Varlink-native clients shouldn't need to know busbridge exists, or what
  socket path it listens on — they resolve an interface name and get
  routed. Config-level `unix:`/`exec:` addresses are an implementation
  detail, not something clients should hardcode. This is already
  implemented and correct; lean on it more, not less.
- **Errors**: known interfaces declare their own Varlink error types in
  their IDL, matching the D-Bus error names/semantics documented for that
  interface (reusing §3's built-in error-category table where the
  D-Bus-side error isn't interface-specific). The generic
  passthrough/`BusProxy` surface keeps its current "pass the raw D-Bus
  error name through" behavior, since it can't know errors in advance.
- **Versioning**: since generated interfaces are derived from descriptors
  busbridge itself authors (for known interfaces) or discovers (for
  passthrough), pin a version per shipped descriptor and only change a
  generated interface's shape on a deliberate, documented bump — a
  long-lived Varlink client's contract with `org.kde.StatusNotifierItem`
  shouldn't shift silently under a busbridge update.

---

## 6. Recommended project structure

```
busbridge/
  Cargo.toml
  README.md
  docs/
    ARCHITECTURE.md              # this document
    DESIGN_BRIEF_V1.md            # historical phase-0 brief
    interfaces/                   # shipped, authoritative descriptors for
                                   # known interfaces (D-Bus introspection
                                   # XML + the small property/signal
                                   # manifest needed for the reverse
                                   # direction, per §2)
      org.kde.StatusNotifierItem.xml
      org.kde.StatusNotifierWatcher.xml
      org.mpris.MediaPlayer2.Player.xml
      org.freedesktop.Notifications.xml
  src/
    main.rs
    lib.rs
    model/                       # NEW - the protocol-independent
                                  # semantic model (§2)
      mod.rs                      # Bus/Service/Object/Interface types
      value.rs                    # internal Value: lossless relative to
                                   # zvariant::Value, signature-aware
      cache.rs                    # property cache; EmitsChangedSignal-
                                   # aware invalidation
      subscription.rs             # Subscription entity: bounded buffer,
                                   # overflow signaling
      registry.rs                 # Registration entity: the
                                   # generalization of DynamicObject
    dbus/
      mod.rs                       # connection, RequestName, message
                                    # intercept (existing, unchanged)
      dispatch.rs                  # now dispatches into model:: instead
                                    # of building JSON directly
      introspect.rs                # existing: introspection XML loading
      dynamic_object.rs            # reworked onto model::registry;
                                    # dedicated-bus-name-by-default (§4)
      bus_proxy.rs                 # unchanged: generic outbound-client
                                    # escape hatch (§2, §3)
      resolver.rs                  # unchanged: org.varlink.resolver
      ownership.rs                 # NEW - NameOwnerChanged tracking,
                                    # watcher-restart re-registration (§4)
      object_manager.rs            # NEW - generic ObjectManager producer
                                    # + consumer support (§2, §4)
    varlink/
      mod.rs
      client.rs
      server.rs
      streaming.rs                 # reworked onto model::subscription
      idl_gen.rs                   # NEW - generates per-interface Varlink
                                    # IDL + dispatch from model::Interface
      service_info.rs              # NEW - real GetInfo/
                                    # GetInterfaceDescription per interface
    convert.rs                     # narrowed: model::Value <->
                                    # zvariant::Value and model::Value <->
                                    # JSON, used only at the two edges
    errors.rs                      # existing passthrough default +
                                    # built-in D-Bus error-category table
    config/
      mod.rs
      schema.rs                    # [[method]]/[[signal]]/[[property]]
                                    # entries now populate model::Interface
    control.rs                     # unchanged
    activation.rs                  # unchanged
    idle.rs                        # unchanged (generalizes to
                                    # subscriptions/registrations already)
    telemetry.rs                   # unchanged
  conf.d/
    *.toml.example
  service-templates/
    dbus-1/
    systemd/
  tests/
    *.rs
```

Everything under `model/`, `ownership.rs`, `object_manager.rs`,
`idl_gen.rs`, and `service_info.rs` is new. Everything else is the
existing, renamed phase-0 code, migrated onto the model incrementally
(§7) rather than rewritten wholesale.

---

## 7. Implementation roadmap

The phases below are ordered to de-risk the highest-uncertainty piece
(the model + Varlink-codegen pipeline) against the *simplest* real
interface first, then apply it to progressively more stateful cases —
deliberately **not** SNI-first, unlike the old brief's build order. SNI's
registration/re-registration/dedicated-name handling (§4) is the hardest,
most stateful part of the system; proving the pipeline against MPRIS
first means SNI-specific problems don't get tangled up with
pipeline-level bugs.

1. **Rename and re-baseline** (done in this pass): project renamed to
   `busbridge` throughout; old brief preserved as history; this document
   established as the live architecture reference. No behavior change.
2. **Introduce `model::Value` and `model::Object`/`Interface` as an
   internal layer, non-breaking.** Migrate `dispatch.rs`'s and
   `dynamic_object.rs`'s direct `convert.rs` calls to go through it.
   Verify against the existing test suite (`tests/end_to_end.rs`,
   `tests/passthrough*.rs`) with no behavior change — this phase should
   be invisible from the outside.
3. **Property caching**, gated on the interface descriptor's
   `EmitsChangedSignal` annotation (already a standard D-Bus introspection
   concept — read it, don't invent a new one). Validate against a
   synthetic multi-reader test (several concurrent `GetAll` callers,
   one cache miss, N cache hits).
4. **Ship a first known-interface descriptor and the Varlink codegen path
   (`varlink/idl_gen.rs`, `varlink/service_info.rs`) against MPRIS**,
   chosen specifically for its lack of registration statefulness. Prove:
   real per-interface Varlink IDL, real `GetInterfaceDescription`,
   properties served from cache, `Seeked` as a modeled signal via
   `Subscribe`. Validate against a real MPRIS-speaking app and a
   real Varlink-side test client, not just unit tests.
5. **Notifications**, chosen for its call-ID-keyed callback correlation —
   validates the "stateful adapter, not mechanical translation" case
   without a registration handshake.
6. **Generic `org.freedesktop.DBus.ObjectManager` support**
   (`dbus/object_manager.rs`), both directions. This subsumes the
   SNI-shaped "dynamic registration" problem as a special case rather
   than solving it twice.
7. **SNI**, now built on top of steps 4–6's proven machinery:
   `dbus/ownership.rs` for watcher-restart re-registration,
   dedicated-bus-name-per-item by default, real signal modeling
   (`NewIcon`/`NewStatus`/etc., not everything folded into
   `PropertiesChanged`). Validate against at least two independent real
   hosts (e.g. a Plasma panel and `waybar` or `xfce4-panel`) to catch the
   shared-name-convention incompatibility called out in §1/§4 empirically,
   not just per spec-reading.
8. **Subscription hardening**: bounded buffers and explicit overflow
   signaling for all streaming paths (`varlink/streaming.rs`,
   `dbus/object_manager.rs`'s change notifications), replacing the
   current unmoderated forwarding loops.
9. **Error-category table** (`errors.rs` extension) — small, independent,
   can land any time after step 2.
10. **(Exploratory, only if telemetry/demand justifies it) FD-passing**
    at the `model::Value` layer. Treat as a real, scoped feature with its
    own design pass when undertaken — not a partial hack layered onto
    JSON.

Non-goal, at every phase: generalizing the model to protocols other than
D-Bus/Varlink. Keep the module boundary clean; don't build the
abstraction.

---

## 8. Compatibility, performance, security, correctness

**Security.** `bus_proxy`'s generic outbound-client proxy remains the
single largest risk surface in the system: whoever can reach that socket
has the same practical trust as a process connected directly to the
target bus. This is unavoidable if the tool is to forward genuinely
arbitrary calls on behalf of a Varlink-native client — the mitigation is
Unix-socket file permissions/placement (already the documented model),
not an internal allow/deny flag that would just move the same trust
decision one layer down. A future per-destination allow/deny list is a
reasonable defense-in-depth addition, but socket-level access control is
and should remain the real boundary. Dedicated-bus-name-per-registered-
item (§4) also opens a small new surface: a malicious or buggy Varlink
client could churn name registrations. A per-connection/per-uid
registration rate limit is worth adding alongside that feature, not
after.

**Correctness — ordering.** A single object's events must be delivered in
order (a rapid `NewIcon` then `NewStatus` must not reorder on the wire).
The existing per-call task-spawn isolation (correctly designed for
*cross-object* isolation, so a hung backend for object A can't stall
object B) must not be allowed to reorder events *within* one object's own
stream — sequence dispatch per-object even while keeping isolation
across objects.

**Correctness — property staleness.** Caching (§7 step 3) must never
serve a stale value for a property that doesn't declare
`EmitsChangedSignal`. Default to no caching for such properties rather
than inventing a TTL heuristic; a wrong guess here produces silent,
hard-to-diagnose bugs in consumers.

**Correctness — the honest limits of type recovery.** The existing
signature-guided JSON→D-Bus conversion is sound *whenever a target
signature is known* (i.e., for busbridge-authored/known interfaces).
For the passthrough/generic-fallback path, the README's own documented
floor (numeric types collapse to `int64`, structs can't be told from
arrays, `GetAll` on an undescribed interface returns empty) is real and
should stay explicitly, honestly documented rather than papered over —
it is not a bug, it's what's actually possible without a descriptor.

**Performance.** Property caching (§7 step 3) is very likely the single
largest available performance win — SNI-style tray hosts and MPRIS
controllers both commonly poll properties on a timer or on every repaint.
Per-object event sequencing (above) should be implemented without a
global lock: the routing table from object path to backing
connection/cache should be a fast lookup, never held across an `await`
that talks to a backend, exactly as the existing `dynamic_object.rs`
design already gets right for its one special case — generalize that
principle, don't lose it.

**Compatibility — dbus-daemon vs. dbus-broker.** Both implement the same
D-Bus wire protocol and bus semantics; busbridge as an ordinary bus
client/service shouldn't need to special-case either. `dbus-broker`
enforces XML policy and quota limits somewhat more strictly than the
classic `dbus-daemon` in some edge cases — worth a compatibility test
matrix entry (run the integration tests against both), not a code
difference.

**Compatibility — the SNI shared-name convention, empirically.** As
flagged in §1 and §4, don't trust the newer `"busname,objectpath"`
registration convention to be universally supported. Validate the
dedicated-name-by-default choice against at least two real, independently
maintained host implementations before considering SNI support "done."

**Gaps to keep documenting honestly rather than silently limiting:**
no file-descriptor passing (until/unless §7's exploratory phase lands);
struct fields lose their names across the JSON boundary in the
generic/passthrough path (arrays, not named objects — sound, documented,
matches Varlink's own preference for ordered data, but worth restating
here since it's a real, permanent limitation of the fallback path
specifically, not of the known-interface codegen path, which can name
fields from the descriptor).
