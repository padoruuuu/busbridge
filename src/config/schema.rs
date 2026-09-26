//! Serde structs matching the TOML shape defined in docs/DESIGN_BRIEF_V1.md Section 3.2.
//! Covers [service], [varlink], [[method]], [[signal]], [[property]], and
//! the [[registrable]] addition from Section 2.5.
//!
//! Treat org.freedesktop.DBus.Properties Get/Set/GetAll as ordinary
//! [[method]] entries rather than a special case - see docs/DESIGN_BRIEF_V1.md Section 3.2.
//! (The config loader in `mod.rs` synthesizes those entries automatically
//! for every configured `[[property]]`, so authors don't have to hand-write
//! them - see `synth_property_methods`.)

use std::collections::HashMap;

use serde::Deserialize;

/// Which bus a `[service]` block should be published on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Bus {
    #[default]
    Session,
    System,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub bus: Bus,
    pub name: String,
    pub object_path: String,
    /// Relative to the config file that referenced it; authoritative source
    /// of D-Bus-side types for this interface (docs/DESIGN_BRIEF_V1.md Section 3.5).
    /// Optional: required for [[method]]/[[signal]]/[[property]] entries
    /// (their type conversion depends on it), but not needed at all in
    /// `passthrough` mode, which infers types generically instead - see
    /// `passthrough` below.
    #[serde(default)]
    pub introspection_xml: Option<String>,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// When true, any D-Bus call on this name that doesn't match an
    /// explicit `[[method]]` entry is forwarded to `[varlink].backend`
    /// automatically, using generic conventions instead of per-method
    /// config: the Varlink method name is `"{interface}.{member}"`, the
    /// D-Bus call's actual argument values (read straight off the wire,
    /// no introspection needed) become a JSON object keyed `arg0`,
    /// `arg1`, ..., and the reply is converted back with its D-Bus type
    /// *inferred* from the JSON shape (see convert.rs's
    /// `json_to_dbus_inferred`) rather than declared up front.
    ///
    /// Symmetrically, any inbound Varlink push (docs/DESIGN_BRIEF_V1.md Section 2.2)
    /// whose `"{interface}.{member}"` doesn't match a configured
    /// `[[signal]]`/`[[property]]` is emitted as a D-Bus signal the same
    /// generic way.
    ///
    /// Not covered by passthrough: `org.freedesktop.DBus.Properties`
    /// Get/Set/GetAll still require explicit `[[property]]` entries -
    /// there's no generic convention for "which Varlink call fetches this
    /// property" to fall back to, unlike methods and signals.
    ///
    /// This is the tool's answer to "I don't want to hand-write a mapping
    /// for every method" - it trades precise, declared D-Bus types for
    /// zero per-method config. A `[service]` + `[varlink]` block (still
    /// naming a D-Bus name and a Varlink address - nothing can guess
    /// those) is the entire config needed. See README.md's "Zero-config
    /// passthrough mode" section.
    #[serde(default)]
    pub passthrough: bool,
}

fn default_idle_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct VarlinkConfig {
    /// Inbound push / activation-stub listen address (docs/DESIGN_BRIEF_V1.md Section 2.2).
    pub listen: Option<String>,
    /// Outbound-call backend address (docs/DESIGN_BRIEF_V1.md Section 2.3): `unix:...` or
    /// `exec:...`.
    pub backend: Option<String>,
    /// Opt-in generic outbound D-Bus *client* proxy, listening on this
    /// address (see dbus/bus_proxy.rs). Lets a Varlink app make arbitrary
    /// D-Bus method calls, read/write arbitrary properties, introspect
    /// arbitrary objects, and subscribe to arbitrary signals on this
    /// service's bus - entirely without linking a D-Bus library itself.
    /// This is the complement to the rest of this schema (which lets a
    /// Varlink app receive D-Bus calls / emit D-Bus signals for names IT
    /// owns): together, the two let an app be moved to Varlink-only with
    /// no D-Bus code on either the serving or the calling side.
    ///
    /// SECURITY: this grants whoever can reach the socket the same
    /// practical trust as a process connected directly to this service's
    /// bus - they can call any method on any name reachable there, not
    /// just this service's own. Leave unset unless you actually need a
    /// Varlink app to act as a general D-Bus client (e.g. a status-tray
    /// host that must query whichever third-party apps register with it,
    /// whose bus names aren't known ahead of time).
    #[serde(default)]
    pub bus_proxy_listen: Option<String>,
}

fn default_args() -> Vec<String> {
    Vec::new()
}

#[derive(Debug, Clone, Deserialize)]
pub struct MethodMapping {
    pub dbus_interface: String,
    pub dbus_method: String,
    pub varlink_method: String,
    /// Maps positional D-Bus args (in signature order) to named Varlink
    /// parameters.
    #[serde(default = "default_args")]
    pub args: Vec<String>,
    /// Explicit Varlink-error-name -> D-Bus-error-name mapping table for
    /// this method (docs/DESIGN_BRIEF_V1.md Section 3.6, Open Decisions). If a Varlink
    /// error name isn't present here, the default convention applies: pass
    /// the Varlink error name straight through as the D-Bus error name.
    #[serde(default)]
    pub error_map: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", tag = "direction")]
pub enum SignalDirection {
    /// Backend connects to us and pushes the event (docs/DESIGN_BRIEF_V1.md Section 2.2).
    InboundPush,
    /// We call out and hold a `"more": true` subscription open (docs/DESIGN_BRIEF_V1.md
    /// Section 2.4).
    OutboundSubscribe { varlink_method: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct SignalMapping {
    pub dbus_interface: String,
    pub dbus_signal: String,
    #[serde(flatten)]
    pub direction: SignalDirection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PropertyMapping {
    pub dbus_interface: String,
    pub dbus_property: String,
    pub varlink_method: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdSource {
    Generated,
    AppSupplied,
}

fn default_id_source() -> IdSource {
    IdSource::Generated
}

/// docs/DESIGN_BRIEF_V1.md Section 2.5: interfaces whose object paths are allocated at
/// runtime rather than fixed in config.
#[derive(Debug, Clone, Deserialize)]
pub struct RegistrableConfig {
    pub dbus_interface: String,
    pub path_prefix: String,
    pub introspection_xml: String,
    /// The Varlink method a Varlink-only app calls to register itself.
    pub register_via: String,
    #[serde(default = "default_id_source")]
    pub id_source: IdSource,
}

/// docs/DESIGN_BRIEF_V1.md-adjacent addition: a standalone routing entry for the
/// standard Varlink resolver protocol (dbus/resolver.rs) - "resolve this
/// Varlink interface name to this D-Bus destination/path". Deliberately
/// NOT nested under `[service]` and NOT tied to name ownership: the
/// destination this resolves to is very often a name owned by some
/// completely different, unrelated process (the whole point - see
/// dbus/resolver.rs's module doc comment), so it must never cause this
/// bridge to `RequestName` anything. A `[[resolve]]` entry is fully
/// self-contained; it does not default any field from a `[service]`
/// block even if one happens to be present in the same file.
#[derive(Debug, Clone, Deserialize)]
pub struct ResolveConfig {
    /// The Varlink-resolvable interface name a client asks
    /// `org.varlink.resolver.Resolve` for.
    pub interface: String,
    /// The real D-Bus destination (well-known or unique name) that
    /// implements it.
    pub destination: String,
    pub path: String,
    #[serde(default)]
    pub bus: Bus,
}

/// The full parsed shape of one `conf.d/*.toml` file. One file per D-Bus
/// name is the expected convention (docs/DESIGN_BRIEF_V1.md Section 3.2).
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceFile {
    /// Optional: a file can contain ONLY `[[resolve]]` entries (routing
    /// to interfaces this bridge doesn't own or translate at all, just
    /// helps clients find), with no `[service]`/name-ownership at all.
    #[serde(default)]
    pub service: Option<ServiceConfig>,
    #[serde(default)]
    pub varlink: VarlinkConfig,
    #[serde(default, rename = "method")]
    pub methods: Vec<MethodMapping>,
    #[serde(default, rename = "signal")]
    pub signals: Vec<SignalMapping>,
    #[serde(default, rename = "property")]
    pub properties: Vec<PropertyMapping>,
    #[serde(default, rename = "registrable")]
    pub registrables: Vec<RegistrableConfig>,
    #[serde(default, rename = "resolve")]
    pub resolves: Vec<ResolveConfig>,
}

impl ServiceFile {
    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_sni_watcher_config() {
        let text = include_str!("../../conf.d/sni-watcher.toml.example");
        let file = ServiceFile::parse(text).expect("example config should parse");
        assert_eq!(file.service.as_ref().unwrap().name, "org.kde.StatusNotifierWatcher");
        assert_eq!(file.methods.len(), 1);
        assert_eq!(file.signals.len(), 1);
        assert_eq!(file.properties.len(), 1);
        assert_eq!(file.registrables.len(), 1);
        match &file.signals[0].direction {
            SignalDirection::InboundPush => {}
            other => panic!("expected inbound_push, got {other:?}"),
        }
        assert_eq!(file.registrables[0].id_source, IdSource::Generated);
    }

    #[test]
    fn parses_resolve_only_file_with_no_service_block() {
        let text = r#"
            [[resolve]]
            interface = "org.example.Greeter"
            destination = "org.example.GreeterApp"
            path = "/org/example/Greeter"
        "#;
        let file = ServiceFile::parse(text).expect("resolve-only config should parse");
        assert!(file.service.is_none());
        assert_eq!(file.resolves.len(), 1);
        assert_eq!(file.resolves[0].interface, "org.example.Greeter");
        assert_eq!(file.resolves[0].bus, Bus::Session);
    }
}
