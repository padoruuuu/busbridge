//! Varlink error <-> D-Bus error mapping. docs/DESIGN_BRIEF_V1.md Section 3.6.
//!
//! Default convention (Section 9, Open Decisions): Varlink error name
//! passes straight through as the D-Bus error name; `parameters` is
//! JSON-stringified into the D-Bus error message, unless a specific
//! interface's config supplies an explicit mapping table.

use std::collections::HashMap;

use serde_json::Value as JsonValue;

/// A Varlink error reply: `{"error": "org.example.NotFound", "parameters": {...}}`.
#[derive(Debug, Clone, PartialEq)]
pub struct VarlinkError {
    pub name: String,
    pub parameters: Option<JsonValue>,
}

/// A D-Bus error: an error name (conventionally
/// `org.freedesktop.DBus.Error.*`-style, but any reverse-DNS name is legal)
/// plus a human-readable message.
#[derive(Debug, Clone, PartialEq)]
pub struct DBusError {
    pub name: String,
    pub message: String,
}

/// Translate a Varlink error reply into a D-Bus error, per docs/DESIGN_BRIEF_V1.md
/// Section 3.6 / Section 9.
///
/// - If `error_map` (the per-method `[[method]].error_map` table from
///   config/schema.rs) has an entry for `varlink_err.name`, that entry is
///   used as the D-Bus error name.
/// - Otherwise the Varlink error name passes straight through unchanged.
/// - In both cases, `parameters` (if present) is JSON-stringified into the
///   D-Bus error message; if absent, the D-Bus error name itself is reused
///   as a minimal, non-empty message (D-Bus errors require some text).
pub fn varlink_error_to_dbus(
    varlink_err: &VarlinkError,
    error_map: &HashMap<String, String>,
) -> DBusError {
    let name = error_map
        .get(&varlink_err.name)
        .cloned()
        .unwrap_or_else(|| varlink_err.name.clone());

    let message = match &varlink_err.parameters {
        Some(params) => serde_json::to_string(params).unwrap_or_else(|_| varlink_err.name.clone()),
        None => varlink_err.name.clone(),
    };

    DBusError { name, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_unmapped() {
        let err = VarlinkError {
            name: "org.example.tray.NotFound".into(),
            parameters: Some(serde_json::json!({"id": "abc"})),
        };
        let mapped = varlink_error_to_dbus(&err, &HashMap::new());
        assert_eq!(mapped.name, "org.example.tray.NotFound");
        assert_eq!(mapped.message, r#"{"id":"abc"}"#);
    }

    #[test]
    fn explicit_mapping_overrides_name() {
        let err = VarlinkError {
            name: "org.example.tray.NotFound".into(),
            parameters: None,
        };
        let mut map = HashMap::new();
        map.insert(
            "org.example.tray.NotFound".to_string(),
            "org.freedesktop.DBus.Error.UnknownObject".to_string(),
        );
        let mapped = varlink_error_to_dbus(&err, &map);
        assert_eq!(mapped.name, "org.freedesktop.DBus.Error.UnknownObject");
        assert_eq!(mapped.message, "org.example.tray.NotFound");
    }
}
