# Optional systemd convenience units

**Optional drop-ins for systemd users only.** Nothing in `busbridge`
requires systemd, and none of these files use systemd-only D-Bus
`.service` extensions like `SystemdService=` (see
docs/DESIGN_BRIEF_V1.md Section 4, Hard Constraints). The bridge falls
back to binding every socket itself if none of these units are
installed, with identical resulting behavior either way.

What these buy you *on systemd specifically*: native socket activation,
so the very first connection (to `busbridge.sock`, or to a conf.d
service's own `varlink.listen` address) is handled without busbridge
needing to bind the socket itself at cold-start - including, for
`busbridge.sock`, starting busbridge itself on demand from the very
first app registration (docs/REGISTRATION_PROTOCOL.md). The bridge's own
`LISTEN_PID`/`LISTEN_FDS` detection (`src/activation.rs`) works
identically regardless of which fd(s) came from a unit like these versus
being bound by the bridge itself - see docs/DESIGN_BRIEF_V1.md Section 4
for why this must remain true, and `src/activation.rs`'s own doc comment
for how multiple listeners (the control socket, and one per conf.d
service with `varlink.listen` set) safely share one `LISTEN_FDS` fd list
without needing to coordinate with each other directly.

## Files

- **`busbridge.socket`** / **`busbridge.service`** - the app registration
  protocol's one universal address
  (`docs/REGISTRATION_PROTOCOL.md`). Install one pair per deployment
  scope (session or system - `%t` in the `.socket` unit already expands
  correctly for either). This is almost certainly the pair you want if
  you're deploying busbridge specifically to serve Varlink-native apps
  registering themselves as D-Bus services, since it means busbridge
  doesn't need to be started ahead of time at all - the first app to
  register brings it up.

- **`../dbus-1/org.kde.StatusNotifierWatcher.service.example`** - the
  *other* activation path, for conf.d-configured services: ordinary
  D-Bus bus activation, owned by the message bus daemon rather than
  systemd, and working identically under any init. A systemd `.socket`
  unit for a conf.d service's own `varlink.listen` address is also
  possible (same pairing convention as `busbridge.socket`/
  `busbridge.service` above, just `ListenStream=` pointed at that
  service's own configured path instead) but isn't shipped as a template
  here, since the path is different for every service and every
  deployment's `conf.d/` layout.

## Installing

Pick a scope and install both files from this directory there:

```
~/.config/systemd/user/busbridge.socket       # session bus
~/.config/systemd/user/busbridge.service
systemctl --user enable --now busbridge.socket
```

or, for the system bus:

```
/etc/systemd/system/busbridge.socket
/etc/systemd/system/busbridge.service
systemctl enable --now busbridge.socket
```
