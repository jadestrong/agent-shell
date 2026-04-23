use std::fmt;
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// RequestId
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(untagged)]
enum IdRepr {
    I64(i64),
    String(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RequestId(IdRepr);

impl From<i64> for RequestId {
    fn from(value: i64) -> Self {
        RequestId(IdRepr::I64(value))
    }
}

impl From<String> for RequestId {
    fn from(value: String) -> Self {
        RequestId(IdRepr::String(value))
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            IdRepr::I64(n) => write!(f, "{n}"),
            IdRepr::String(s) => write!(f, "\"{s}\""),
        }
    }
}

// ---------------------------------------------------------------------------
// ResponseError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error code {}: {}", self.code, self.message)
    }
}

impl std::error::Error for ResponseError {}

// Standard JSON-RPC 2.0 error codes
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

// ACP-specific error codes
pub const AUTH_REQUIRED: i64 = -32000;

// ---------------------------------------------------------------------------
// Request / Response / Notification
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Response {
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Notification {
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
}

impl Response {
    pub fn new_ok<R: Serialize>(id: RequestId, result: R) -> Response {
        Response {
            id,
            result: Some(serde_json::to_value(result).unwrap()),
            error: None,
        }
    }

    pub fn new_err(id: RequestId, code: i64, message: String) -> Response {
        Response {
            id,
            result: None,
            error: Some(ResponseError {
                code,
                message,
                data: None,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Message enum (untagged for JSON-RPC 2.0 dispatch)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

impl From<Request> for Message {
    fn from(value: Request) -> Self {
        Message::Request(value)
    }
}

impl From<Response> for Message {
    fn from(value: Response) -> Self {
        Message::Response(value)
    }
}

impl From<Notification> for Message {
    fn from(value: Notification) -> Self {
        Message::Notification(value)
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Wire format: Content-Length framed JSON-RPC
// ---------------------------------------------------------------------------

impl Message {
    /// Read a JSON-RPC message from a `BufRead` source.
    ///
    /// Supports two framing modes:
    /// 1. Newline-delimited JSON: one JSON object per line
    /// 2. Content-Length framed (LSP-style): `Content-Length: N\r\n\r\n<body>`
    ///
    /// The reader auto-detects the mode by peeking at the first bytes.
    /// Returns `Ok(None)` on EOF.
    pub fn read(r: &mut impl BufRead) -> io::Result<Option<Message>> {
        Self::_read(r)
    }

    fn _read(r: &mut dyn BufRead) -> io::Result<Option<Message>> {
        loop {
            // Peek to decide framing mode
            let buf = match r.fill_buf() {
                Ok(buf) if buf.is_empty() => return Ok(None),
                Ok(buf) => buf,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            };

            if buf.starts_with(b"Content-Length") {
                // Content-Length framed (LSP-style) — used by Emacs sender
                let text = match read_msg_text(r)? {
                    None => return Ok(None),
                    Some(text) => text,
                };
                let msg: Message = serde_json::from_str(&text)?;
                return Ok(Some(msg));
            } else {
                // Newline-delimited JSON
                let mut line = String::new();
                if r.read_line(&mut line)? == 0 {
                    return Ok(None);
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue; // skip blank lines
                }
                let msg: Message = serde_json::from_str(trimmed)?;
                return Ok(Some(msg));
            }
        }
    }

    /// Write a JSON-RPC message to a `Write` sink.
    ///
    /// Uses Content-Length framing (LSP-style):
    /// `Content-Length: N\r\n\r\n<body>`.
    /// Includes the mandatory `jsonrpc: "2.0"` field.
    pub fn write(self, w: &mut impl Write) -> io::Result<()> {
        Self::_write(self, w)
    }

    fn _write(msg: Message, w: &mut dyn Write) -> io::Result<()> {
        /// Helper that flattens the inner message and adds `jsonrpc: "2.0"`.
        #[derive(Serialize)]
        struct JsonRpc {
            jsonrpc: &'static str,
            #[serde(flatten)]
            msg: Message,
        }

        let text = serde_json::to_string(&JsonRpc {
            jsonrpc: "2.0",
            msg,
        })?;
        let header = format!("Content-Length: {}\r\n\r\n", text.len());
        w.write_all(header.as_bytes())?;
        w.write_all(text.as_bytes())?;
        w.flush()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Low-level read / write helpers
// ---------------------------------------------------------------------------

fn read_msg_text(inp: &mut dyn BufRead) -> io::Result<Option<String>> {
    fn invalid_data(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
    macro_rules! invalid_data {
        ($($tt:tt)*) => (invalid_data(format!($($tt)*)))
    }

    let mut size = None;
    let mut buf = String::new();

    loop {
        buf.clear();
        if inp.read_line(&mut buf)? == 0 {
            return Ok(None);
        }
        if !buf.ends_with("\r\n") {
            return Err(invalid_data!("malformed header: {:?}", buf));
        }
        let buf = &buf[..buf.len() - 2];
        if buf.is_empty() {
            break;
        }
        let mut parts = buf.splitn(2, ": ");
        let header_name = parts.next().unwrap();
        let header_value = parts
            .next()
            .ok_or_else(|| invalid_data!("malformed header: {:?}", buf))?;
        if header_name.eq_ignore_ascii_case("Content-Length") {
            size = Some(header_value.parse::<usize>().map_err(invalid_data)?);
        }
    }

    let size: usize = size.ok_or_else(|| invalid_data!("no Content-Length"))?;
    let mut buf = buf.into_bytes();
    buf.resize(size, 0);
    inp.read_exact(&mut buf)?;
    let buf = String::from_utf8(buf).map_err(invalid_data)?;
    Ok(Some(buf))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Helper: write a message to bytes, then read it back.
    fn round_trip(msg: Message) -> Message {
        let mut buf = Vec::new();
        msg.write(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        Message::read(&mut cursor).unwrap().unwrap()
    }

    // -- RequestId ----------------------------------------------------------

    #[test]
    fn request_id_from_i64() {
        let id = RequestId::from(42i64);
        assert_eq!(serde_json::to_string(&id).unwrap(), "42");
    }

    #[test]
    fn request_id_from_string() {
        let id = RequestId::from("abc".to_string());
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"abc\"");
    }

    #[test]
    fn request_id_display() {
        assert_eq!(RequestId::from(1i64).to_string(), "1");
        assert_eq!(RequestId::from("x".to_string()).to_string(), "\"x\"");
    }

    // -- Serialization round-trip -------------------------------------------

    #[test]
    fn request_round_trip() {
        let msg = Message::Request(Request {
            id: RequestId::from(1i64),
            method: "textDocument/completion".into(),
            params: serde_json::json!({"textDocument": {"uri": "file:///a.rs"}}),
        });
        match round_trip(msg) {
            Message::Request(req) => {
                assert_eq!(req.id, RequestId::from(1i64));
                assert_eq!(req.method, "textDocument/completion");
            }
            other => panic!("expected Request, got {other:?}"),
        }
    }

    #[test]
    fn response_ok_round_trip() {
        let msg = Message::Response(Response::new_ok(
            RequestId::from(2i64),
            serde_json::json!({"items": []}),
        ));
        match round_trip(msg) {
            Message::Response(resp) => {
                assert_eq!(resp.id, RequestId::from(2i64));
                assert!(resp.result.is_some());
                assert!(resp.error.is_none());
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn response_err_round_trip() {
        let msg = Message::Response(Response::new_err(
            RequestId::from(3i64),
            METHOD_NOT_FOUND,
            "method not found".into(),
        ));
        match round_trip(msg) {
            Message::Response(resp) => {
                assert_eq!(resp.id, RequestId::from(3i64));
                assert!(resp.result.is_none());
                let err = resp.error.unwrap();
                assert_eq!(err.code, METHOD_NOT_FOUND);
                assert_eq!(err.message, "method not found");
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn notification_round_trip() {
        let msg = Message::Notification(Notification {
            method: "initialized".into(),
            params: serde_json::json!({}),
        });
        match round_trip(msg) {
            Message::Notification(notif) => {
                assert_eq!(notif.method, "initialized");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
    }

    // -- Wire format --------------------------------------------------------

    #[test]
    fn write_includes_jsonrpc_field() {
        let msg = Message::Notification(Notification {
            method: "test".into(),
            params: serde_json::Value::Null,
        });
        let mut buf = Vec::new();
        msg.write(&mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        // The body should contain "jsonrpc":"2.0"
        assert!(text.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn write_uses_content_length_format() {
        let msg = Message::Notification(Notification {
            method: "test".into(),
            params: serde_json::Value::Null,
        });
        let mut buf = Vec::new();
        msg.write(&mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("Content-Length: "));
        assert!(text.contains("\r\n\r\n"));
    }

    // -- Error cases --------------------------------------------------------

    #[test]
    fn read_eof_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = Message::read(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn read_malformed_header_returns_error() {
        // Missing \r\n termination
        let data = b"Content-Length: 2\n\n{}";
        let mut cursor = Cursor::new(data.as_slice());
        let result = Message::read(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn read_missing_content_length_returns_error() {
        let data = b"X-Custom: foo\r\n\r\n{}";
        let mut cursor = Cursor::new(data.as_slice());
        let result = Message::read(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn read_invalid_json_returns_error() {
        let body = "not valid json";
        let header = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(header.into_bytes());
        let result = Message::read(&mut cursor);
        assert!(result.is_err());
    }

    // -- Response convenience constructors ----------------------------------

    #[test]
    fn new_ok_sets_result_and_no_error() {
        let resp = Response::new_ok(RequestId::from(1i64), "hello");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
        assert_eq!(resp.result.unwrap(), serde_json::json!("hello"));
    }

    #[test]
    fn new_err_sets_error_and_no_result() {
        let resp = Response::new_err(RequestId::from(1i64), INVALID_REQUEST, "bad".into());
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, INVALID_REQUEST);
        assert_eq!(err.message, "bad");
        assert!(err.data.is_none());
    }

    // -- String request id --------------------------------------------------

    #[test]
    fn request_with_string_id_round_trip() {
        let msg = Message::Request(Request {
            id: RequestId::from("uuid-123".to_string()),
            method: "test/method".into(),
            params: serde_json::json!(null),
        });
        match round_trip(msg) {
            Message::Request(req) => {
                assert_eq!(req.id, RequestId::from("uuid-123".to_string()));
            }
            other => panic!("expected Request, got {other:?}"),
        }
    }

    // -- Content-Length backward compatibility (reader) ----------------------

    #[test]
    fn read_content_length_framed_input() {
        // Emacs sends Content-Length framed messages; the reader must handle them.
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"test","params":{}}"#;
        let frame = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(frame.into_bytes());
        let msg = Message::read(&mut cursor).unwrap().unwrap();
        match msg {
            Message::Request(req) => {
                assert_eq!(req.method, "test");
            }
            other => panic!("expected Request, got {other:?}"),
        }
    }

    #[test]
    fn read_newline_delimited_input() {
        let line = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test\",\"params\":{}}\n";
        let mut cursor = Cursor::new(line.as_bytes());
        let msg = Message::read(&mut cursor).unwrap().unwrap();
        match msg {
            Message::Request(req) => {
                assert_eq!(req.method, "test");
            }
            other => panic!("expected Request, got {other:?}"),
        }
    }
}
