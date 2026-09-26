//! Shared Varlink wire framing: JSON object + NUL (\0) delimiter, one
//! object per request/reply. docs/DESIGN_BRIEF_V1.md Section 3.4.
//!
//! Hand-rolled deliberately - the official `varlink` crate is
//! codegen-first and assumes static types, incompatible with generic
//! dispatch. Read/write helpers here are shared by client.rs and server.rs.

pub mod client;
pub mod peer;
pub mod server;
pub mod streaming;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

/// One Varlink method-call request, per the wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VarlinkRequest {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub more: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub oneway: bool,
}

/// One Varlink reply, either a success (`parameters`) or an error
/// (`error` + `parameters` as the error's own payload).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VarlinkReply {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub continues: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl VarlinkReply {
    pub fn ok(parameters: JsonValue) -> Self {
        Self {
            parameters: Some(parameters),
            error: None,
            continues: false,
        }
    }

    pub fn ok_continuing(parameters: JsonValue) -> Self {
        Self {
            parameters: Some(parameters),
            error: None,
            continues: true,
        }
    }

    pub fn error(name: impl Into<String>, parameters: Option<JsonValue>) -> Self {
        Self {
            parameters,
            error: Some(name.into()),
            continues: false,
        }
    }

    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Write one JSON value followed by the NUL delimiter.
pub async fn write_framed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: &impl Serialize,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    bytes.push(0);
    writer.write_all(&bytes).await?;
    writer.flush().await
}

/// Read one NUL-delimited JSON value. Returns `Ok(None)` cleanly on EOF
/// (the peer closed the connection between messages, which is a normal
/// occurrence, not an error).
pub async fn read_framed<R, T>(reader: &mut R) -> std::io::Result<Option<T>>
where
    R: AsyncBufRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut buf = Vec::new();
    let n = reader.read_until(0, &mut buf).await?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&0) {
        buf.pop();
    }
    if buf.is_empty() {
        return Ok(None);
    }
    let value = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(value))
}

/// Minimal `org.varlink.service.GetInfo` reply, so generic Varlink tooling
/// (e.g. `varlinkctl`) can at least identify this process, per docs/DESIGN_BRIEF_V1.md
/// Section 3.4 ("basic compatibility ... even though our own dispatch
/// doesn't need them internally"). Not a full interface-description
/// generator - `GetInterfaceDescription` returns a stub description noting
/// that mappings are config-driven rather than a static `.varlink` schema.
pub fn service_get_info() -> JsonValue {
    serde_json::json!({
        "vendor": "busbridge",
        "product": "busbridge",
        "version": env!("CARGO_PKG_VERSION"),
        "url": "https://github.com/example/busbridge",
        "interfaces": ["org.varlink.service"],
    })
}

pub fn service_get_interface_description(interface: &str) -> Result<JsonValue, String> {
    if interface == "org.varlink.service" {
        Ok(serde_json::json!({
            "description": "interface org.varlink.service\n\nmethod GetInfo() -> (vendor: string, product: string, version: string, url: string, interfaces: []string)\nmethod GetInterfaceDescription(interface: string) -> (description: string)\n"
        }))
    } else {
        Err(format!(
            "interface descriptions are config-driven for this bridge; no static schema for {interface}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip_over_a_duplex_pipe() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = VarlinkRequest {
            method: "org.example.Foo".into(),
            parameters: Some(serde_json::json!({"x": 1})),
            more: false,
            oneway: false,
        };
        write_framed(&mut a, &req).await.unwrap();

        let mut reader = tokio::io::BufReader::new(&mut b);
        let got: VarlinkRequest = read_framed(&mut reader).await.unwrap().unwrap();
        assert_eq!(got.method, "org.example.Foo");
        assert_eq!(got.parameters, Some(serde_json::json!({"x": 1})));
    }

    #[tokio::test]
    async fn read_framed_returns_none_on_clean_eof() {
        let (a, mut b) = tokio::io::duplex(64);
        drop(a);
        let mut reader = tokio::io::BufReader::new(&mut b);
        let got: Option<VarlinkRequest> = read_framed(&mut reader).await.unwrap();
        assert!(got.is_none());
    }
}
