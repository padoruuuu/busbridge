//! Loads/parses the `introspection_xml` referenced per-service in config
//! (docs/DESIGN_BRIEF_V1.md Section 3.2). This XML is the authoritative source of
//! D-Bus-side types used by convert.rs's JSON -> D-Bus direction
//! (docs/DESIGN_BRIEF_V1.md Section 3.5) since we author it ourselves for everything
//! this bridge exposes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use quick_xml::reader::Reader;

use crate::BoxError;

#[derive(Debug, Clone, Default)]
pub struct ArgDesc {
    pub name: Option<String>,
    pub type_sig: String,
}

#[derive(Debug, Clone, Default)]
pub struct MethodDesc {
    pub in_args: Vec<ArgDesc>,
    pub out_args: Vec<ArgDesc>,
}

#[derive(Debug, Clone, Default)]
pub struct SignalDesc {
    pub args: Vec<ArgDesc>,
}

#[derive(Debug, Clone, Default)]
pub struct PropertyDesc {
    pub type_sig: String,
    /// "read", "write", or "readwrite" per the D-Bus introspection schema.
    pub access: String,
}

#[derive(Debug, Clone, Default)]
pub struct InterfaceDesc {
    pub name: String,
    pub methods: HashMap<String, MethodDesc>,
    pub signals: HashMap<String, SignalDesc>,
    pub properties: HashMap<String, PropertyDesc>,
}

impl InterfaceDesc {
    /// The raw `Introspectable.Introspect` XML fragment for this one
    /// interface (used to build the full node XML the dispatch engine
    /// answers `org.freedesktop.DBus.Introspectable.Introspect` with).
    pub fn to_xml_fragment(&self) -> String {
        let mut out = format!("  <interface name=\"{}\">\n", xml_escape(&self.name));
        let mut methods: Vec<_> = self.methods.iter().collect();
        methods.sort_by_key(|(k, _)| (*k).clone());
        for (name, m) in methods {
            out.push_str(&format!("    <method name=\"{}\">\n", xml_escape(name)));
            for arg in &m.in_args {
                out.push_str(&arg_xml(arg, Some("in")));
            }
            for arg in &m.out_args {
                out.push_str(&arg_xml(arg, Some("out")));
            }
            out.push_str("    </method>\n");
        }
        let mut signals: Vec<_> = self.signals.iter().collect();
        signals.sort_by_key(|(k, _)| (*k).clone());
        for (name, s) in signals {
            out.push_str(&format!("    <signal name=\"{}\">\n", xml_escape(name)));
            for arg in &s.args {
                out.push_str(&arg_xml(arg, None));
            }
            out.push_str("    </signal>\n");
        }
        let mut props: Vec<_> = self.properties.iter().collect();
        props.sort_by_key(|(k, _)| (*k).clone());
        for (name, p) in props {
            out.push_str(&format!(
                "    <property name=\"{}\" type=\"{}\" access=\"{}\"/>\n",
                xml_escape(name),
                xml_escape(&p.type_sig),
                xml_escape(&p.access)
            ));
        }
        out.push_str("  </interface>\n");
        out
    }
}

fn arg_xml(arg: &ArgDesc, direction: Option<&str>) -> String {
    let name_attr = arg
        .name
        .as_ref()
        .map(|n| format!(" name=\"{}\"", xml_escape(n)))
        .unwrap_or_default();
    let dir_attr = direction
        .map(|d| format!(" direction=\"{d}\""))
        .unwrap_or_default();
    format!(
        "      <arg{name_attr} type=\"{}\"{dir_attr}/>\n",
        xml_escape(&arg.type_sig)
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn load_from_path(path: &Path) -> Result<HashMap<String, InterfaceDesc>, BoxError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("reading introspection XML {}: {e}", path.display()))?;
    parse(&text).map_err(|e| format!("parsing introspection XML {}: {e}", path.display()).into())
}

/// Directories where OTHER packages commonly install standalone D-Bus
/// interface XML files (portals, `org.freedesktop.Notifications`, etc.),
/// separate from any given service's own `.service` file. Checked, in
/// order, by `find_system_interface_desc` below.
///
/// Override/extend via the `BUSBRIDGE_INTERFACE_XML_DIRS` env var
/// (colon-separated, checked first) - useful for a Varlink backend
/// package that ships its own interface XML somewhere non-standard, or
/// for tests. This is deliberately NOT per-service config: it's a
/// process-wide search path, the same kind of environment-driven
/// discovery `activation.rs` already does for `LISTEN_FDS`.
fn system_interface_xml_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(extra) = std::env::var("BUSBRIDGE_INTERFACE_XML_DIRS") {
        dirs.extend(std::env::split_paths(&extra));
    }
    dirs.push(PathBuf::from("/usr/share/dbus-1/interfaces"));
    dirs.push(PathBuf::from("/usr/local/share/dbus-1/interfaces"));
    dirs
}

/// Per-process cache of interface-name -> discovered description (or
/// `None` for a confirmed miss), so `passthrough` mode's automatic type
/// upgrade (dispatch.rs, varlink/server.rs) doesn't re-scan the
/// filesystem on every single call.
static SYSTEM_INTERFACE_CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Option<InterfaceDesc>>>> =
    std::sync::OnceLock::new();

/// Look for a real, already-installed D-Bus interface XML declaring
/// `interface_name`, across `system_interface_xml_dirs()`. This is how
/// `passthrough` mode (config/schema.rs) recovers precise types, real
/// argument names, correctly-arity'd multi-value replies, and correctly
/// shaped signals *without* the user writing any XML themselves - it only
/// helps for interfaces some other installed package already documents
/// this way; for a bespoke/private interface, there's nothing to find and
/// passthrough falls back to generic inference (see convert.rs's
/// `json_to_dbus_inferred`).
pub fn find_system_interface_desc(interface_name: &str) -> Option<InterfaceDesc> {
    let cache = SYSTEM_INTERFACE_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    {
        let cache = cache.lock().unwrap();
        if let Some(hit) = cache.get(interface_name) {
            return hit.clone();
        }
    }

    let found = find_interface_in_dirs(interface_name, &system_interface_xml_dirs());

    cache.lock().unwrap().insert(interface_name.to_string(), found.clone());
    found
}

/// Pure directory-scanning core of `find_system_interface_desc`, factored
/// out so it can be unit-tested against an explicit, isolated list of
/// directories - deliberately NOT via the
/// `BUSBRIDGE_INTERFACE_XML_DIRS` env var, which (like
/// `DBUS_SESSION_BUS_ADDRESS` elsewhere in this crate's tests) is
/// process-global and therefore unsafe for concurrently-running tests to
/// each set to a different value.
fn find_interface_in_dirs(interface_name: &str, dirs: &[PathBuf]) -> Option<InterfaceDesc> {
    dirs.iter().find_map(|dir| {
        let entries = std::fs::read_dir(dir).ok()?;
        entries.filter_map(|e| e.ok()).find_map(|entry| {
            let path = entry.path();
            if path.extension().map(|e| e == "xml").unwrap_or(false) {
                let text = std::fs::read_to_string(&path).ok()?;
                let ifaces = parse(&text).ok()?;
                ifaces.get(interface_name).cloned()
            } else {
                None
            }
        })
    })
}
/// (`<node><interface>...</interface></node>`) into a map of interface
/// name -> `InterfaceDesc`.
/// Direct child object-path *segments* named by a `<node name="...">`
/// element at the top level of an introspection XML document - i.e. what
/// `org.freedesktop.DBus.Introspectable.Introspect` returns for the
/// children of the object it was called on (not to be confused with the
/// `<node>` element the whole document is wrapped in, which normally has
/// no `name` attribute, or has the queried object's own full path -
/// either way not a *child* name, so it's skipped by the `!name.is_empty()`
/// check below).
///
/// Used by `registration.rs::find_object_implementing` (and mirrors, on
/// the busbridge side, the same recursive-discovery walk apps used to
/// have to implement for themselves client-side - see
/// docs/REGISTRATION_PROTOCOL.md's `FindObjects` section) to walk a bus
/// name's object tree without knowing its shape ahead of time.
pub fn child_node_names(xml: &str) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut names = Vec::new();
    let mut buf = Vec::new();
    // Depth-scoped for the same reason `root_implements_interface` is
    // (see its doc comment): some real D-Bus servers' `Introspect()`
    // recursively embed full descendant data inside `<node>` elements
    // instead of the minimal "empty marker" convention, so a depth-blind
    // scan collects every descendant's name at every level, not just the
    // *direct* children of the queried object - which, fed back into a
    // path-building BFS one `path/child` segment at a time, still
    // eventually reconstructs a deeply-nested correct path (each wrong
    // guess just fails to introspect and gets pruned), but only by
    // brute-forcing many more combinations than necessary at every
    // level. That went unnoticed against a single linear test chain
    // (a -> b -> c -> d -> target, no branching) - it re-derives the
    // right combination on essentially the first attempt either way -
    // but a real, wider tree could burn through the whole node/depth
    // budget on bogus combinations before ever trying the real path.
    let mut node_depth: u32 = 0;
    loop {
        let event = match reader.read_event_into(&mut buf) {
            Ok(e) => e,
            Err(_) => break,
        };
        match event {
            Event::Eof => break,
            Event::Start(e) => {
                if e.local_name().as_ref() == b"node" {
                    node_depth += 1;
                    // Depth 2 here means "direct child of the root
                    // (depth 1)" - the root's own Start event already
                    // incremented depth to 1 above, so a child's Start
                    // event is what brings it to 2.
                    if node_depth == 2 {
                        if let Some(name) = collect_attrs(&e).get("name") {
                            if !name.is_empty() {
                                names.push(name.clone());
                            }
                        }
                    }
                }
            }
            Event::End(e) => {
                if e.local_name().as_ref() == b"node" {
                    node_depth = node_depth.saturating_sub(1);
                }
            }
            Event::Empty(e) => {
                // A self-closing <node name="child"/> is a direct child
                // exactly when it appears at depth 1 (inside the root,
                // not inside some other child's own embedded block) -
                // note this checks the *current* depth as of entering
                // the root, unlike Start above, since an Empty node
                // never nests anything and so never increments depth
                // itself.
                if node_depth == 1 && e.local_name().as_ref() == b"node" {
                    if let Some(name) = collect_attrs(&e).get("name") {
                        if !name.is_empty() {
                            names.push(name.clone());
                        }
                    }
                }
            }
            _ => {}
        }
        buf.clear();
    }
    names
}

/// Whether the object an introspection document is *itself* describing
/// (as opposed to any of its children) implements `interface` -
/// correctly scoped to the outermost `<node>`'s direct `<interface>`
/// children, unlike `parse` (see that function's doc comment for why it
/// doesn't need this distinction for its own, different purpose).
///
/// This distinction matters because real D-Bus implementations vary in
/// how much they put in one `Introspect()` reply: some literally follow
/// the introspection DTD's minimal convention (child `<node>` elements
/// are empty markers, `<node name="child"/>`, carrying no interface data
/// of their own - the caller is expected to `Introspect()` the child
/// separately if it wants to know more), but others (zbus's own
/// `ObjectServer`, observed directly - not merely suspected - while
/// building this function, via `tests/registration.rs`'s
/// `find_objects_locates_a_deeply_nested_interface`) recursively embed
/// each descendant's *full* interface data inside its `<node>` element
/// instead. A flat, nesting-unaware scan (like `parse`'s) sees every
/// `<interface>` anywhere in a document like that and can't tell "the
/// root object has this" from "some object four levels down has this" -
/// which silently made `registration.rs::find_object_implementing`
/// report every match as if it were at `/`, regardless of the real
/// depth, the first time this was tested against a real D-Bus server
/// rather than only against hand-authored, single-object XML.
pub fn root_implements_interface(xml: &str, interface: &str) -> bool {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    // Depth of `<node>` elements only (not all elements) - 0 outside any
    // node, 1 while inside the outermost (root) node, 2+ while inside a
    // nested child node's own embedded data, if a server writes it that
    // way. `<interface>` only counts at depth 1.
    let mut node_depth: u32 = 0;
    loop {
        let event = match reader.read_event_into(&mut buf) {
            Ok(e) => e,
            Err(_) => break,
        };
        match event {
            Event::Eof => break,
            Event::Start(e) => match e.local_name().as_ref() {
                b"node" => node_depth += 1,
                b"interface" if node_depth == 1 => {
                    let attrs = collect_attrs(&e);
                    if attrs.get("name").map(String::as_str) == Some(interface) {
                        return true;
                    }
                }
                _ => {}
            },
            Event::End(e) => {
                if e.local_name().as_ref() == b"node" {
                    node_depth = node_depth.saturating_sub(1);
                }
            }
            Event::Empty(e) => {
                // A self-closing <interface .../> at the root would be
                // unusual (interfaces always have at least methods/
                // signals/properties in practice) but handled anyway for
                // correctness; self-closing <node/> elements (the normal
                // "here's a child, nothing more" marker) never contain
                // nested interfaces and need no depth bookkeeping.
                if node_depth == 1 && e.local_name().as_ref() == b"interface" {
                    let attrs = collect_attrs(&e);
                    if attrs.get("name").map(String::as_str) == Some(interface) {
                        return true;
                    }
                }
            }
            _ => {}
        }
        buf.clear();
    }
    false
}

pub fn parse(xml: &str) -> Result<HashMap<String, InterfaceDesc>, String> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut interfaces: HashMap<String, InterfaceDesc> = HashMap::new();
    let mut ctx = Ctx::None;

    let mut buf = Vec::new();
    loop {
        let event = reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("XML parse error at position {}: {e}", reader.buffer_position()))?;
        match event {
            Event::Eof => break,
            Event::Start(e) => {
                handle_open(&mut interfaces, &mut ctx, &e, false);
            }
            Event::Empty(e) => {
                handle_open(&mut interfaces, &mut ctx, &e, true);
            }
            Event::End(e) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                match local.as_str() {
                    "method" => {
                        if let Ctx::Method(iface, _) = &ctx {
                            ctx = Ctx::Interface(iface.clone());
                        }
                    }
                    "signal" => {
                        if let Ctx::Signal(iface, _) = &ctx {
                            ctx = Ctx::Interface(iface.clone());
                        }
                    }
                    "interface" => {
                        ctx = Ctx::None;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(interfaces)
}

fn handle_open(
    interfaces: &mut HashMap<String, InterfaceDesc>,
    ctx: &mut Ctx,
    e: &quick_xml::events::BytesStart<'_>,
    is_empty: bool,
) {
    let local = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
    let attrs = collect_attrs(e);
    match local.as_str() {
        "interface" => {
            let name = attrs.get("name").cloned().unwrap_or_default();
            interfaces.entry(name.clone()).or_insert_with(|| InterfaceDesc {
                name: name.clone(),
                ..Default::default()
            });
            *ctx = Ctx::Interface(name);
        }
        "method" => {
            if let Ctx::Interface(iface) = ctx {
                let name = attrs.get("name").cloned().unwrap_or_default();
                interfaces
                    .get_mut(iface)
                    .unwrap()
                    .methods
                    .entry(name.clone())
                    .or_default();
                if !is_empty {
                    *ctx = Ctx::Method(iface.clone(), name);
                }
            }
        }
        "signal" => {
            if let Ctx::Interface(iface) = ctx {
                let name = attrs.get("name").cloned().unwrap_or_default();
                interfaces
                    .get_mut(iface)
                    .unwrap()
                    .signals
                    .entry(name.clone())
                    .or_default();
                if !is_empty {
                    *ctx = Ctx::Signal(iface.clone(), name);
                }
            }
        }
        "property" => {
            if let Ctx::Interface(iface) = ctx {
                let name = attrs.get("name").cloned().unwrap_or_default();
                let type_sig = attrs.get("type").cloned().unwrap_or_default();
                let access = attrs.get("access").cloned().unwrap_or_else(|| "read".to_string());
                interfaces
                    .get_mut(iface)
                    .unwrap()
                    .properties
                    .insert(name, PropertyDesc { type_sig, access });
            }
        }
        "arg" => {
            let name = attrs.get("name").cloned();
            let type_sig = attrs.get("type").cloned().unwrap_or_default();
            let direction = attrs.get("direction").cloned().unwrap_or_else(|| "in".to_string());
            match ctx {
                Ctx::Method(iface, method) => {
                    let m = interfaces.get_mut(iface).unwrap().methods.get_mut(method).unwrap();
                    let arg = ArgDesc { name, type_sig };
                    if direction == "out" {
                        m.out_args.push(arg);
                    } else {
                        m.in_args.push(arg);
                    }
                }
                Ctx::Signal(iface, signal) => {
                    let s = interfaces.get_mut(iface).unwrap().signals.get_mut(signal).unwrap();
                    s.args.push(ArgDesc { name, type_sig });
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// Parse context: which element we're inside, so a bare <arg> can be
// attributed to the right method/signal.
enum Ctx {
    None,
    Interface(String),
    Method(String, String), // (interface, method)
    Signal(String, String), // (interface, signal)
}

fn collect_attrs(e: &quick_xml::events::BytesStart<'_>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for attr in e.attributes().flatten() {
        let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
        let value = attr.unescape_value().unwrap_or_default().to_string();
        out.insert(key, value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
        <node>
          <interface name="org.kde.StatusNotifierWatcher">
            <method name="RegisterStatusNotifierItem">
              <arg name="service" type="s" direction="in"/>
            </method>
            <signal name="StatusNotifierItemRegistered">
              <arg name="service" type="s"/>
            </signal>
            <property name="RegisteredStatusNotifierItems" type="as" access="read"/>
          </interface>
        </node>
    "#;

    #[test]
    fn parses_methods_signals_and_properties() {
        let ifaces = parse(SAMPLE).unwrap();
        let iface = ifaces.get("org.kde.StatusNotifierWatcher").unwrap();
        let m = iface.methods.get("RegisterStatusNotifierItem").unwrap();
        assert_eq!(m.in_args.len(), 1);
        assert_eq!(m.in_args[0].type_sig, "s");
        assert_eq!(m.out_args.len(), 0);

        let s = iface.signals.get("StatusNotifierItemRegistered").unwrap();
        assert_eq!(s.args.len(), 1);
        assert_eq!(s.args[0].type_sig, "s");

        let p = iface.properties.get("RegisteredStatusNotifierItems").unwrap();
        assert_eq!(p.type_sig, "as");
        assert_eq!(p.access, "read");
    }

    #[test]
    fn round_trips_to_xml_fragment_and_reparses() {
        let ifaces = parse(SAMPLE).unwrap();
        let iface = ifaces.get("org.kde.StatusNotifierWatcher").unwrap();
        let fragment = iface.to_xml_fragment();
        let wrapped = format!("<node>\n{fragment}</node>");
        let reparsed = parse(&wrapped).unwrap();
        let iface2 = reparsed.get("org.kde.StatusNotifierWatcher").unwrap();
        assert_eq!(iface2.methods.len(), iface.methods.len());
        assert_eq!(iface2.signals.len(), iface.signals.len());
        assert_eq!(iface2.properties.len(), iface.properties.len());
    }

    #[test]
    fn find_interface_in_dirs_locates_a_matching_interface() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("org.example.Greeter.xml"),
            r#"<node>
                <interface name="org.example.Greeter">
                    <method name="SayHello">
                        <arg name="name" type="s" direction="in"/>
                        <arg name="greeting" type="s" direction="out"/>
                    </method>
                    <property name="DefaultLanguage" type="s" access="read"/>
                </interface>
            </node>"#,
        )
        .unwrap();

        let dirs = vec![tmp.path().to_path_buf()];
        let found = find_interface_in_dirs("org.example.Greeter", &dirs).unwrap();
        assert_eq!(found.methods.get("SayHello").unwrap().in_args[0].type_sig, "s");
        assert_eq!(found.methods.get("SayHello").unwrap().out_args[0].name.as_deref(), Some("greeting"));
        assert!(found.properties.contains_key("DefaultLanguage"));

        assert!(find_interface_in_dirs("org.example.NoSuchInterface", &dirs).is_none());
    }

    #[test]
    fn find_interface_in_dirs_skips_nonexistent_and_non_xml() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("notes.txt"), "not xml").unwrap();
        let dirs = vec![PathBuf::from("/does/not/exist"), tmp.path().to_path_buf()];
        assert!(find_interface_in_dirs("org.example.Anything", &dirs).is_none());
    }
}
