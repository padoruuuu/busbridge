# Integration tests

All of these spawn a private, throwaway `dbus-daemon --session` and skip
cleanly (rather than failing) if one isn't available in the environment
running the tests.

- `end_to_end.rs` - static `[[method]]`/`[[signal]]` mapping: a real
  D-Bus call round-trips to a fake Varlink backend and back, and a
  Varlink push becomes a real D-Bus signal. Uses
  `org.kde.StatusNotifierWatcher` as the running example, but only
  exercises the *watcher* side (a fixed object path, config-declared
  method/signal mappings).
- `sni_tray.rs` - the *tray item* side: a Varlink-only app registering
  itself as a `[[registrable]]` dynamic object
  (`src/dbus/dynamic_object.rs`), which `end_to_end.rs` doesn't cover.
  Proves a forwarded method call (`Activate`), a pushed event becoming a
  real D-Bus signal (`NewStatus`), and `org.freedesktop.DBus.Properties`
  forwarding all work against a real bus. See docs/ARCHITECTURE.md
  Section 4 for why this direction (Varlink-native app, legacy D-Bus
  host) is the interesting one worth testing directly.
- `passthrough.rs` / `passthrough_system_xml.rs` - zero-config
  passthrough mode, including recovering real D-Bus types from
  auto-discovered system introspection XML instead of generic inference.
- `bus_proxy.rs` - the generic outbound D-Bus *client* proxy
  (`src/dbus/bus_proxy.rs`): a Varlink app making arbitrary calls,
  reading properties, and subscribing to signals on names it doesn't own.
- `resolver.rs` - the standard `org.varlink.resolver` protocol
  (`src/dbus/resolver.rs`).

Unit tests (type conversion, config schema parsing, control-channel race
handling) live alongside their modules under `src/` rather than here.
