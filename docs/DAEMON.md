# Running busbridge in the background

busbridge is meant to run continuously in the background, the same way
`dbus-broker`/`dbus-daemon` do, but it doesn't assume any particular init
system to make that happen (`docs/DESIGN_BRIEF_V1.md` Section 4's hard
constraints). There are two genuinely different situations, and the right
answer is different for each:

## You have a supervisor (systemd, s6, runit, OpenRC, ...)

**Run busbridge in the foreground and let the supervisor hold onto it
directly. Do not pass `--daemonize`.**

This is the normal, recommended case, and nothing about it changed in
this pass - it's already how `service-templates/systemd/busbridge.service`
works, and the same idea applies verbatim under any other supervisor:
`ExecStart=/path/to/busbridge` with no forking, no `Type=forking`. The
supervisor:

- notices if busbridge crashes and can restart it,
- can send it SIGTERM to stop it cleanly (busbridge now handles this -
  see "Stopping busbridge" below - releasing every D-Bus name it holds
  and flushing telemetry before exiting, rather than dying with no
  cleanup the way an unhandled SIGTERM would),
- captures its stdout/stderr (journald, a log file, whatever that
  supervisor does) without busbridge needing to know or care,
- can hand it an already-bound listening socket via the standard
  `LISTEN_PID`/`LISTEN_FDS` fd-inheritance convention
  (`src/activation.rs`) for `busbridge.sock`
  (`service-templates/systemd/busbridge.socket`) and/or individual
  conf.d services' own `varlink.listen` sockets - this is a generic,
  non-systemd-specific mechanism (several supervisors implement the same
  fd-passing ABI), not something unique to the systemd templates that
  happen to be shipped.

Daemonizing (forking away from the process the supervisor started) would
actively break this: the supervisor would lose track of the real process,
notice it "exited" immediately, and either give up or restart it into a
duplicate.

## You have no supervisor at all

For a plain shell, a login autostart file, or anywhere else with no
process-supervision infrastructure at all - `--daemonize` (or `-d`) makes
busbridge detach itself from the terminal and background itself, the
classic double-fork:

```
busbridge --daemonize
# or, keeping a PID file around to signal it later:
busbridge --daemonize --pid-file /run/user/1000/busbridge/busbridge.pid
```

This returns immediately (the shell gets its prompt back right away, the
same as `some-command &` would look), with the real daemon now detached
and running in the background. It:

- closes its session/controlling terminal association (`setsid` plus a
  second fork - signals sent to the terminal's process group, Ctrl-C
  included, no longer reach it),
- redirects stdin to `/dev/null` (a background process should never
  block waiting on terminal input),
- leaves stdout/stderr untouched - if you want daemonized output logged
  somewhere, redirect it yourself before backgrounding
  (`busbridge --daemonize >>/var/log/busbridge.log 2>&1`), since the
  shell sets that redirect up before busbridge's own code ever runs, and
  it survives the internal fork() calls unchanged. Forcing stdout/stderr
  to `/dev/null` unconditionally here would silently discard a redirect
  you explicitly asked for, which is worse than output simply going
  nowhere if you didn't redirect anything - see `daemonize.rs`'s doc
  comment on `redirect_standard_fds` for the reasoning in full,
- changes its working directory to `/` and resets its umask, the usual
  daemon hygiene so it doesn't hold an arbitrary directory busy or create
  files with a surprising inherited permission mask.

`--pid-file <path>` is optional; if given, the final daemon process
writes its own pid there (one line, newline-terminated) once fully
detached - useful for a simple `kill -TERM "$(cat /path/to/pidfile)"`
stop script when nothing fancier is managing the process. Without a
supervisor, this is on you; busbridge doesn't create or clean up a
default pid-file path on its own.

Everything else - the resident/handoff control channel, hot-reload,
idle-exit, socket activation if you also set up the optional systemd
units anyway - works completely unchanged whether or not `--daemonize`
was used. Daemonizing only changes how the *process itself* starts; it's
not a different mode of operation from that point on.

## Stopping busbridge

Send SIGTERM (what `kill`, and every supervisor's normal "stop" action,
sends by default) or SIGINT (Ctrl-C, for a foreground/interactive run).
Either one now triggers the same graceful shutdown idle-exit already
uses: release every D-Bus name this process holds, flush telemetry, then
exit - rather than the default disposition for an unhandled SIGTERM
(die immediately, no cleanup at all), which used to be the only way this
process could stop other than waiting for its own idle timeout.
