//! Outbound Varlink calls to a configured backend. docs/DESIGN_BRIEF_V1.md Section 2.3.
//!
//! Backend address is `unix:/path/to/socket` or `exec:/path/to/binary arg...`.
//! For `exec:`, the connect() call itself is what triggers activation (spawn
//! the process, talk to it over its stdin/stdout) - no special handling
//! needed on our side beyond spawning correctly.
//!
//! Simplification (documented, docs/DESIGN_BRIEF_V1.md Section 9 spirit - state the
//! assumption and proceed): we open a fresh connection per outbound call
//! rather than pooling/reusing connections. This keeps call isolation
//! trivial (docs/DESIGN_BRIEF_V1.md Section 3.3: "each inbound D-Bus call dispatched onto
//! its own async task") and matches `exec:`'s natural "spawn, talk, reap"
//! shape; for `unix:` backends this costs a connect() per call, which is
//! cheap on a local socket and avoids a class of connection-reuse bugs
//! (interleaved `more`-streams, stale connections after backend restarts).

use std::path::PathBuf;
use std::process::Stdio;

use serde_json::Value as JsonValue;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};

use super::{read_framed, write_framed, VarlinkReply, VarlinkRequest};
use crate::errors::VarlinkError;

#[derive(Debug)]
pub enum Transport {
    Unix(PathBuf),
    Exec(String, Vec<String>),
}

/// Parse a `unix:...` or `exec:...` backend address.
pub fn parse_address(addr: &str) -> Result<Transport, String> {
    if let Some(path) = addr.strip_prefix("unix:") {
        Ok(Transport::Unix(PathBuf::from(path)))
    } else if let Some(rest) = addr.strip_prefix("exec:") {
        let mut parts = rest.split_whitespace();
        let program = parts
            .next()
            .ok_or_else(|| format!("exec: address missing a program: {addr:?}"))?
            .to_string();
        let args = parts.map(|s| s.to_string()).collect();
        Ok(Transport::Exec(program, args))
    } else {
        Err(format!("unsupported varlink address (want unix: or exec:): {addr:?}"))
    }
}

#[derive(Debug)]
pub enum CallError {
    Io(std::io::Error),
    /// Connection closed / EOF before a reply arrived.
    ProtocolClosed,
    /// The backend replied with `{"error": ...}`.
    Remote(VarlinkError),
    Address(String),
    Spawn(std::io::Error),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Io(e) => write!(f, "I/O error talking to varlink backend: {e}"),
            CallError::ProtocolClosed => {
                write!(f, "varlink backend closed the connection before replying")
            }
            CallError::Remote(e) => write!(f, "varlink backend returned error {}", e.name),
            CallError::Address(s) => write!(f, "{s}"),
            CallError::Spawn(e) => write!(f, "failed to spawn exec: backend: {e}"),
        }
    }
}

impl std::error::Error for CallError {}

impl From<std::io::Error> for CallError {
    fn from(e: std::io::Error) -> Self {
        CallError::Io(e)
    }
}

/// A single outbound connection to a Varlink backend. Only one call (or one
/// streaming subscription's worth of calls) should be made per instance -
/// see the module-level doc comment on connection reuse.
pub struct VarlinkConnection {
    reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    /// Kept alive so the child isn't reaped/killed while we're still
    /// talking to it over its piped stdio.
    _child: Option<tokio::process::Child>,
}

impl VarlinkConnection {
    pub async fn connect(addr: &str) -> Result<Self, CallError> {
        match parse_address(addr).map_err(CallError::Address)? {
            Transport::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(&path).await?;
                let (r, w) = tokio::io::split(stream);
                Ok(Self {
                    reader: BufReader::new(Box::new(r)),
                    writer: Box::new(w),
                    _child: None,
                })
            }
            Transport::Exec(program, args) => {
                let mut cmd = tokio::process::Command::new(program);
                cmd.args(args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null());
                let mut child = cmd.spawn().map_err(CallError::Spawn)?;
                let stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| CallError::Address("exec: backend has no stdin".into()))?;
                let stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| CallError::Address("exec: backend has no stdout".into()))?;
                Ok(Self {
                    reader: BufReader::new(Box::new(stdout)),
                    writer: Box::new(stdin),
                    _child: Some(child),
                })
            }
        }
    }

    /// Send a single request, expecting exactly one (non-continuing) reply.
    pub async fn call(
        &mut self,
        method: &str,
        parameters: Option<JsonValue>,
    ) -> Result<JsonValue, CallError> {
        let request = VarlinkRequest {
            method: method.to_string(),
            parameters,
            more: false,
            oneway: false,
        };
        write_framed(&mut self.writer, &request).await?;
        let reply: VarlinkReply = read_framed(&mut self.reader)
            .await?
            .ok_or(CallError::ProtocolClosed)?;
        reply_to_result(reply)
    }

    /// Send a request with `"more": true` and return the connection so the
    /// caller (varlink/streaming.rs) can read a sequence of `continues:
    /// true` replies followed by a final one.
    pub async fn call_streaming(
        mut self,
        method: &str,
        parameters: Option<JsonValue>,
    ) -> Result<Self, CallError> {
        let request = VarlinkRequest {
            method: method.to_string(),
            parameters,
            more: true,
            oneway: false,
        };
        write_framed(&mut self.writer, &request).await?;
        Ok(self)
    }

    /// Read the next reply from an open streaming call. Returns `Ok(None)`
    /// once the peer closes the connection (treated as stream end, not an
    /// error - see varlink/streaming.rs for the reconnect/backoff policy
    /// layered on top of this).
    pub async fn next_reply(&mut self) -> Result<Option<VarlinkReply>, CallError> {
        let reply: Option<VarlinkReply> = read_framed(&mut self.reader).await?;
        Ok(reply)
    }
}

fn reply_to_result(reply: VarlinkReply) -> Result<JsonValue, CallError> {
    if let Some(name) = reply.error {
        return Err(CallError::Remote(VarlinkError {
            name,
            parameters: reply.parameters,
        }));
    }
    Ok(reply.parameters.unwrap_or(JsonValue::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn parses_unix_and_exec_addresses() {
        match parse_address("unix:/run/foo.sock").unwrap() {
            Transport::Unix(p) => assert_eq!(p, PathBuf::from("/run/foo.sock")),
            _ => panic!("expected Unix"),
        }
        match parse_address("exec:/usr/bin/foo --flag bar").unwrap() {
            Transport::Exec(prog, args) => {
                assert_eq!(prog, "/usr/bin/foo");
                assert_eq!(args, vec!["--flag", "bar"]);
            }
            _ => panic!("expected Exec"),
        }
        assert!(parse_address("tcp:127.0.0.1:1234").is_err());
    }

    #[tokio::test]
    async fn call_over_a_unix_socket_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, mut w) = tokio::io::split(stream);
            let mut reader = BufReader::new(r);
            let req: VarlinkRequest = super::super::read_framed(&mut reader).await.unwrap().unwrap();
            assert_eq!(req.method, "org.example.Ping");
            let reply = VarlinkReply::ok(serde_json::json!({"pong": true}));
            super::super::write_framed(&mut w, &reply).await.unwrap();
            w.shutdown().await.unwrap();
        });

        let mut conn = VarlinkConnection::connect(&format!("unix:{}", path.display()))
            .await
            .unwrap();
        let result = conn.call("org.example.Ping", None).await.unwrap();
        assert_eq!(result, serde_json::json!({"pong": true}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remote_error_reply_surfaces_as_call_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, mut w) = tokio::io::split(stream);
            let mut reader = BufReader::new(r);
            let _req: VarlinkRequest = super::super::read_framed(&mut reader).await.unwrap().unwrap();
            let reply = VarlinkReply::error("org.example.NotFound", Some(serde_json::json!({"id": "x"})));
            super::super::write_framed(&mut w, &reply).await.unwrap();
        });

        let mut conn = VarlinkConnection::connect(&format!("unix:{}", path.display()))
            .await
            .unwrap();
        let err = conn.call("org.example.Get", None).await.unwrap_err();
        match err {
            CallError::Remote(e) => assert_eq!(e.name, "org.example.NotFound"),
            other => panic!("expected Remote error, got {other:?}"),
        }
    }
}
