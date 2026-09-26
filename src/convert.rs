//! zvariant::Value <-> serde_json::Value conversion. docs/DESIGN_BRIEF_V1.md Section 3.5.
//!
//! D-Bus -> JSON: mechanical, no config needed, signature is always known
//! from the incoming message.
//!
//! JSON -> D-Bus: needs the TARGET signature (from the introspection XML
//! we author for our own exposed interfaces) to disambiguate numeric types
//! and D-Bus-only types (object paths, signatures, bytes). Error clearly
//! on mismatch rather than guessing.
//!
//! Struct-encoding convention (docs/DESIGN_BRIEF_V1.md Section 9, Open Decisions):
//! structs `(a, b, c)` are encoded as JSON arrays, not objects - simplest,
//! and matches Varlink's own convention of using arrays for ordered data.

use std::fmt;

use serde_json::Value as JsonValue;
use zvariant::{Array, Dict, ObjectPath, Signature, StructureBuilder, Value};

/// A parsed D-Bus type - the tree form of a single complete type from a
/// D-Bus signature string (e.g. `a{sv}`, `(sii)`). We parse signatures
/// ourselves (rather than pulling in a signature-AST crate) since the
/// grammar is small and we need it in both directions: to walk a target
/// signature during JSON -> D-Bus, and to reconstruct signature strings for
/// `Array::new`/`Dict::new` while building D-Bus values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbusType {
    Byte,
    Bool,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    Double,
    String,
    ObjectPath,
    Signature,
    Variant,
    Array(Box<DbusType>),
    /// `a{kv}` - the D-Bus dict-entry-array convention. Key is always a
    /// basic (non-container) type in real D-Bus signatures; we don't
    /// enforce that here, since our own introspection XML is authoritative
    /// and trusted.
    Dict(Box<DbusType>, Box<DbusType>),
    Struct(Vec<DbusType>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConvertError {
    TypeMismatch { expected: String, json_kind: String },
    FieldCountMismatch { expected: usize, got: usize },
    UnsupportedSignatureChar(char),
    EmptySignature,
    TrailingSignature(String),
    UnbalancedContainer,
    CannotInferVariantType,
    InvalidObjectPath(String),
    InvalidSignature(String),
    ZvariantBuild(String),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::TypeMismatch { expected, json_kind } => {
                write!(f, "expected D-Bus type {expected}, got JSON {json_kind}")
            }
            ConvertError::FieldCountMismatch { expected, got } => {
                write!(f, "struct expects {expected} fields, JSON array has {got}")
            }
            ConvertError::UnsupportedSignatureChar(c) => {
                write!(f, "unsupported D-Bus signature character '{c}'")
            }
            ConvertError::EmptySignature => write!(f, "empty D-Bus signature"),
            ConvertError::TrailingSignature(s) => {
                write!(f, "trailing characters after complete type: {s:?}")
            }
            ConvertError::UnbalancedContainer => write!(f, "unbalanced container in signature"),
            ConvertError::CannotInferVariantType => {
                write!(f, "cannot infer a D-Bus type for this JSON value inside a variant")
            }
            ConvertError::InvalidObjectPath(s) => write!(f, "invalid object path: {s}"),
            ConvertError::InvalidSignature(s) => write!(f, "invalid signature string: {s}"),
            ConvertError::ZvariantBuild(s) => write!(f, "failed to build D-Bus value: {s}"),
        }
    }
}

impl std::error::Error for ConvertError {}

fn json_kind(json: &JsonValue) -> &'static str {
    match json {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn mismatch(ty: &DbusType, json: &JsonValue) -> ConvertError {
    ConvertError::TypeMismatch {
        expected: dbus_type_sig_string(ty),
        json_kind: json_kind(json).to_string(),
    }
}

// ---------------------------------------------------------------------
// Signature parsing (string -> DbusType tree)
// ---------------------------------------------------------------------

/// Parse a single complete type from the start of `sig`, returning the
/// parsed type and whatever's left over.
fn parse_type(sig: &str) -> Result<(DbusType, &str), ConvertError> {
    let mut chars = sig.chars();
    let c = chars.next().ok_or(ConvertError::EmptySignature)?;
    let rest = chars.as_str();
    match c {
        'y' => Ok((DbusType::Byte, rest)),
        'b' => Ok((DbusType::Bool, rest)),
        'n' => Ok((DbusType::I16, rest)),
        'q' => Ok((DbusType::U16, rest)),
        'i' => Ok((DbusType::I32, rest)),
        'u' => Ok((DbusType::U32, rest)),
        'x' => Ok((DbusType::I64, rest)),
        't' => Ok((DbusType::U64, rest)),
        'd' => Ok((DbusType::Double, rest)),
        's' => Ok((DbusType::String, rest)),
        'o' => Ok((DbusType::ObjectPath, rest)),
        'g' => Ok((DbusType::Signature, rest)),
        'v' => Ok((DbusType::Variant, rest)),
        'a' => {
            if let Some(after_brace) = rest.strip_prefix('{') {
                let (key, after_key) = parse_type(after_brace)?;
                let (val, after_val) = parse_type(after_key)?;
                let after_close = after_val
                    .strip_prefix('}')
                    .ok_or(ConvertError::UnbalancedContainer)?;
                Ok((DbusType::Dict(Box::new(key), Box::new(val)), after_close))
            } else {
                let (elem, after) = parse_type(rest)?;
                Ok((DbusType::Array(Box::new(elem)), after))
            }
        }
        '(' => {
            let mut fields = Vec::new();
            let mut remaining = rest;
            loop {
                if let Some(after) = remaining.strip_prefix(')') {
                    remaining = after;
                    break;
                }
                if remaining.is_empty() {
                    return Err(ConvertError::UnbalancedContainer);
                }
                let (field, after) = parse_type(remaining)?;
                fields.push(field);
                remaining = after;
            }
            Ok((DbusType::Struct(fields), remaining))
        }
        other => Err(ConvertError::UnsupportedSignatureChar(other)),
    }
}

/// Parse a D-Bus signature string that contains exactly one complete type
/// (the common case for a single method argument, property, or struct
/// field - see docs/DESIGN_BRIEF_V1.md Section 3.5).
pub fn parse_single_complete_type(sig: &str) -> Result<DbusType, ConvertError> {
    let (ty, rest) = parse_type(sig)?;
    if !rest.is_empty() {
        return Err(ConvertError::TrailingSignature(rest.to_string()));
    }
    Ok(ty)
}

fn dbus_type_sig_string(ty: &DbusType) -> String {
    match ty {
        DbusType::Byte => "y".to_string(),
        DbusType::Bool => "b".to_string(),
        DbusType::I16 => "n".to_string(),
        DbusType::U16 => "q".to_string(),
        DbusType::I32 => "i".to_string(),
        DbusType::U32 => "u".to_string(),
        DbusType::I64 => "x".to_string(),
        DbusType::U64 => "t".to_string(),
        DbusType::Double => "d".to_string(),
        DbusType::String => "s".to_string(),
        DbusType::ObjectPath => "o".to_string(),
        DbusType::Signature => "g".to_string(),
        DbusType::Variant => "v".to_string(),
        DbusType::Array(elem) => format!("a{}", dbus_type_sig_string(elem)),
        DbusType::Dict(k, v) => {
            format!("a{{{}{}}}", dbus_type_sig_string(k), dbus_type_sig_string(v))
        }
        DbusType::Struct(fields) => {
            let inner: String = fields.iter().map(dbus_type_sig_string).collect();
            format!("({inner})")
        }
    }
}

fn dbus_type_signature(ty: &DbusType) -> Result<Signature<'static>, ConvertError> {
    let s = dbus_type_sig_string(ty);
    Signature::try_from(s.clone())
        .map(|sig| sig.into_owned())
        .map_err(|_| ConvertError::InvalidSignature(s))
}

// ---------------------------------------------------------------------
// D-Bus -> JSON (mechanical, signature always known from the message)
// ---------------------------------------------------------------------

pub fn dbus_to_json(value: &Value<'_>) -> JsonValue {
    match value {
        Value::U8(v) => JsonValue::from(*v),
        Value::Bool(v) => JsonValue::from(*v),
        Value::I16(v) => JsonValue::from(*v),
        Value::U16(v) => JsonValue::from(*v),
        Value::I32(v) => JsonValue::from(*v),
        Value::U32(v) => JsonValue::from(*v),
        Value::I64(v) => JsonValue::from(*v),
        Value::U64(v) => JsonValue::from(*v),
        Value::F64(v) => JsonValue::from(*v),
        Value::Str(v) => JsonValue::from(v.as_str()),
        Value::Signature(v) => JsonValue::from(v.as_str()),
        Value::ObjectPath(v) => JsonValue::from(v.as_str()),
        // Variants unwrap one level, per docs/DESIGN_BRIEF_V1.md Section 3.5.
        Value::Value(inner) => dbus_to_json(inner),
        Value::Array(arr) => JsonValue::Array(arr.iter().map(dbus_to_json).collect()),
        Value::Dict(dict) => {
            let mut map = serde_json::Map::new();
            for (k, v) in dict.iter() {
                let key = match k {
                    Value::Str(s) => s.to_string(),
                    Value::ObjectPath(p) => p.to_string(),
                    // Non-string dict keys are rare in practice (D-Bus
                    // convention is overwhelmingly a{sv}/a{ss}); fall back
                    // to a debug rendering rather than losing the entry.
                    // Known gap, documented per docs/DESIGN_BRIEF_V1.md Section 8.
                    other => format!("{other:?}"),
                };
                map.insert(key, dbus_to_json(v));
            }
            JsonValue::Object(map)
        }
        Value::Structure(s) => JsonValue::Array(s.fields().iter().map(dbus_to_json).collect()),
        // `Value::Maybe` only exists when zvariant's own "gvariant" cargo
        // feature is enabled, which this crate doesn't turn on (we only
        // speak classic D-Bus wire format, not the GVariant extensions) -
        // so there's no variant to match here at all in our build.
        #[cfg(unix)]
        Value::Fd(_) => JsonValue::Null,
    }
}

// ---------------------------------------------------------------------
// JSON -> D-Bus (needs the target signature to disambiguate)
// ---------------------------------------------------------------------

pub fn json_to_dbus(json: &JsonValue, ty: &DbusType) -> Result<Value<'static>, ConvertError> {
    match ty {
        DbusType::Byte => json
            .as_u64()
            .and_then(|v| u8::try_from(v).ok())
            .map(Value::U8)
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::Bool => json.as_bool().map(Value::Bool).ok_or_else(|| mismatch(ty, json)),
        DbusType::I16 => json
            .as_i64()
            .and_then(|v| i16::try_from(v).ok())
            .map(Value::I16)
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::U16 => json
            .as_u64()
            .and_then(|v| u16::try_from(v).ok())
            .map(Value::U16)
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::I32 => json
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(Value::I32)
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::U32 => json
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .map(Value::U32)
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::I64 => json.as_i64().map(Value::I64).ok_or_else(|| mismatch(ty, json)),
        DbusType::U64 => json.as_u64().map(Value::U64).ok_or_else(|| mismatch(ty, json)),
        DbusType::Double => json.as_f64().map(Value::F64).ok_or_else(|| mismatch(ty, json)),
        DbusType::String => json
            .as_str()
            .map(|s| Value::Str(s.to_string().into()))
            .ok_or_else(|| mismatch(ty, json)),
        DbusType::ObjectPath => {
            let s = json.as_str().ok_or_else(|| mismatch(ty, json))?;
            let owned = zvariant::OwnedObjectPath::try_from(s.to_string())
                .map_err(|_| ConvertError::InvalidObjectPath(s.to_string()))?;
            Ok(Value::ObjectPath(ObjectPath::from(owned)))
        }
        DbusType::Signature => {
            let s = json.as_str().ok_or_else(|| mismatch(ty, json))?;
            let sig = Signature::try_from(s.to_string())
                .map_err(|_| ConvertError::InvalidSignature(s.to_string()))?;
            Ok(Value::Signature(sig.into_owned()))
        }
        DbusType::Variant => {
            let inferred = infer_type_from_json(json)?;
            let inner = json_to_dbus(json, &inferred)?;
            // NOTE: this Value::Value(..) wrap is required here (unlike
            // the *top-level* message-body case in dispatch.rs's
            // Properties.Get handler, which must NOT wrap) because this
            // value is always headed into a Dict/Structure builder API
            // (Dict::append, StructureBuilder::append_field) whose own
            // internal signature check validates against the declared
            // slot type ("v") using the value's *concrete* signature
            // (zvariant's `value_signature()`, not `dynamic_signature()`)
            // - only `Value::Value(inner)` itself reports "v" there;
            // a bare `Value::Str(..)` reports "s" and gets rejected.
            // Verified empirically: wrapped entries round-trip through a
            // real Dict as exactly one variant layer on the wire, not two
            // - Dict's own (de)serialization already expects this shape.
            Ok(Value::Value(Box::new(inner)))
        }
        DbusType::Array(elem_ty) => {
            let items = json.as_array().ok_or_else(|| mismatch(ty, json))?;
            let elem_sig = dbus_type_signature(elem_ty)?;
            let mut arr = Array::new(elem_sig);
            for item in items {
                let v = json_to_dbus(item, elem_ty)?;
                arr.append(v).map_err(|e| ConvertError::ZvariantBuild(e.to_string()))?;
            }
            Ok(Value::Array(arr))
        }
        DbusType::Dict(key_ty, val_ty) => {
            let obj = json.as_object().ok_or_else(|| mismatch(ty, json))?;
            let key_sig = dbus_type_signature(key_ty)?;
            let val_sig = dbus_type_signature(val_ty)?;
            let mut dict = Dict::new(key_sig, val_sig);
            for (k, v) in obj {
                let key_json = JsonValue::String(k.clone());
                let key_val = json_to_dbus(&key_json, key_ty)?;
                let val_val = json_to_dbus(v, val_ty)?;
                dict.append(key_val, val_val)
                    .map_err(|e| ConvertError::ZvariantBuild(e.to_string()))?;
            }
            Ok(Value::Dict(dict))
        }
        DbusType::Struct(fields) => {
            let items = json.as_array().ok_or_else(|| mismatch(ty, json))?;
            if items.len() != fields.len() {
                return Err(ConvertError::FieldCountMismatch {
                    expected: fields.len(),
                    got: items.len(),
                });
            }
            let mut builder = StructureBuilder::new();
            for (item, field_ty) in items.iter().zip(fields.iter()) {
                let v = json_to_dbus(item, field_ty)?;
                builder = builder.append_field(v);
            }
            Ok(Value::Structure(builder.build()))
        }
    }
}

/// Convert a JSON value straight to a D-Bus `Value` with NO target
/// signature at all - the D-Bus type is inferred mechanically from the
/// JSON's own shape (see `infer_type_from_json`'s doc comment for exactly
/// what that mapping is). This is what `passthrough` mode
/// (config/schema.rs's `ServiceConfig::passthrough`) uses for method
/// replies and pushed-event values it has no introspection XML to look
/// declared types up in - it's strictly less precise than
/// `json_to_dbus(json, &ty)` with a real target type (a JSON integer
/// always becomes D-Bus `x`/i64 here, never `u32` or `y`, for instance),
/// so prefer a real signature wherever one is available.
pub fn json_to_dbus_inferred(json: &JsonValue) -> Result<Value<'static>, ConvertError> {
    let ty = infer_type_from_json(json)?;
    json_to_dbus(json, &ty)
}

/// JSON alone can't distinguish `i32` from `u64` from `d` from a bare
/// number, so when the target is a bare variant (e.g. the value side of
/// `a{sv}`), we make a mechanical best-effort guess from the JSON shape
/// rather than erroring outright. This is a documented, deliberate gap
/// (docs/DESIGN_BRIEF_V1.md Section 8): integers become `x` (i64), floats become `d`,
/// objects become `a{sv}`, and arrays infer their element type from the
/// first element (defaulting to `s` when empty).
fn infer_type_from_json(json: &JsonValue) -> Result<DbusType, ConvertError> {
    match json {
        JsonValue::Null => Err(ConvertError::CannotInferVariantType),
        JsonValue::Bool(_) => Ok(DbusType::Bool),
        JsonValue::Number(n) => {
            if n.is_f64() && n.as_i64().is_none() && n.as_u64().is_none() {
                Ok(DbusType::Double)
            } else {
                Ok(DbusType::I64)
            }
        }
        JsonValue::String(_) => Ok(DbusType::String),
        JsonValue::Array(items) => match items.first() {
            Some(first) => Ok(DbusType::Array(Box::new(infer_type_from_json(first)?))),
            None => Ok(DbusType::Array(Box::new(DbusType::String))),
        },
        JsonValue::Object(_) => Ok(DbusType::Dict(
            Box::new(DbusType::String),
            Box::new(DbusType::Variant),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signature_parsing_primitives_and_containers() {
        assert_eq!(parse_single_complete_type("s").unwrap(), DbusType::String);
        assert_eq!(
            parse_single_complete_type("a{sv}").unwrap(),
            DbusType::Dict(Box::new(DbusType::String), Box::new(DbusType::Variant))
        );
        assert_eq!(
            parse_single_complete_type("(sii)").unwrap(),
            DbusType::Struct(vec![DbusType::String, DbusType::I32, DbusType::I32])
        );
        assert_eq!(
            parse_single_complete_type("a(si)").unwrap(),
            DbusType::Array(Box::new(DbusType::Struct(vec![DbusType::String, DbusType::I32])))
        );
    }

    #[test]
    fn signature_parsing_rejects_trailing_and_unbalanced() {
        assert!(matches!(
            parse_single_complete_type("ss"),
            Err(ConvertError::TrailingSignature(_))
        ));
        assert!(matches!(
            parse_single_complete_type("(si"),
            Err(ConvertError::UnbalancedContainer)
        ));
        assert!(matches!(
            parse_single_complete_type("Q"),
            Err(ConvertError::UnsupportedSignatureChar('Q'))
        ));
    }

    #[test]
    fn primitives_round_trip_both_directions() {
        let ty = DbusType::U32;
        let v = json_to_dbus(&json!(42), &ty).unwrap();
        assert_eq!(v, Value::U32(42));
        assert_eq!(dbus_to_json(&v), json!(42));

        let ty = DbusType::String;
        let v = json_to_dbus(&json!("hello"), &ty).unwrap();
        assert_eq!(v, Value::Str("hello".into()));
        assert_eq!(dbus_to_json(&v), json!("hello"));

        let ty = DbusType::Bool;
        let v = json_to_dbus(&json!(true), &ty).unwrap();
        assert_eq!(dbus_to_json(&v), json!(true));
    }

    #[test]
    fn nested_a_sv_round_trips() {
        let ty = DbusType::Dict(Box::new(DbusType::String), Box::new(DbusType::Variant));
        let input = json!({"title": "hi", "count": 3});
        let v = json_to_dbus(&input, &ty).unwrap();
        let back = dbus_to_json(&v);
        assert_eq!(back, input);
    }

    #[test]
    fn array_of_structs_round_trips() {
        let ty = DbusType::Array(Box::new(DbusType::Struct(vec![DbusType::String, DbusType::I32])));
        let input = json!([["a", 1], ["b", 2]]);
        let v = json_to_dbus(&input, &ty).unwrap();
        let back = dbus_to_json(&v);
        assert_eq!(back, input);
    }

    #[test]
    fn variant_unwraps_one_level_on_the_way_out() {
        let inner = json_to_dbus(&json!(7), &DbusType::I64).unwrap();
        let variant = Value::Value(Box::new(inner));
        assert_eq!(dbus_to_json(&variant), json!(7));
    }

    #[test]
    fn malformed_json_for_target_signature_errors_json_to_dbus() {
        let ty = DbusType::U32;
        let err = json_to_dbus(&json!("not a number"), &ty).unwrap_err();
        assert!(matches!(err, ConvertError::TypeMismatch { .. }));

        let ty = DbusType::Struct(vec![DbusType::String, DbusType::I32]);
        let err = json_to_dbus(&json!(["only one"]), &ty).unwrap_err();
        assert!(matches!(err, ConvertError::FieldCountMismatch { expected: 2, got: 1 }));
    }

    #[test]
    fn json_to_dbus_inferred_covers_common_shapes() {
        assert_eq!(json_to_dbus_inferred(&json!(true)).unwrap(), Value::Bool(true));
        assert_eq!(json_to_dbus_inferred(&json!(42)).unwrap(), Value::I64(42));
        assert_eq!(json_to_dbus_inferred(&json!(1.5)).unwrap(), Value::F64(1.5));
        assert_eq!(
            json_to_dbus_inferred(&json!("hi")).unwrap(),
            Value::Str("hi".into())
        );
        let dict = json_to_dbus_inferred(&json!({"a": 1, "b": "x"})).unwrap();
        assert_eq!(dbus_to_json(&dict), json!({"a": 1, "b": "x"}));
        assert!(json_to_dbus_inferred(&JsonValue::Null).is_err());
    }
}
