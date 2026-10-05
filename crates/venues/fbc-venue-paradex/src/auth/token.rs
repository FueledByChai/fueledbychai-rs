//! The session token Paradex's login answers with: read from the answer, named for the journal,
//! and carried in the WebSocket's auth frame and REST's header only where they are marked as
//! credentials (decisions 0009, 0028).

use core::fmt;
use core::ops::Range;

use fbc_core::{DecodeError, Header, HttpResponse, InboundSpans, Secret, WireSlice};
use serde_json::Value;

/// The field of the login answer that carries the token (docs.paradex.trade, "Get JWT").
const FIELD: &str = "jwt_token";

/// The token a login gave. It is held as a [`Secret`] (zeroed when dropped, never `Clone`), and
/// its `Debug` shows its length only. It is checked to be text in the JWT alphabet (letters,
/// digits, `-`, `_` and `.`), so it stands verbatim inside a JSON string and an HTTP header
/// value, and nothing that holds it needs escaping.
pub struct SessionToken(Secret);

impl fmt::Debug for SessionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionToken(<{} bytes>)", self.len())
    }
}

/// Whether `token` is non-empty text in the JWT alphabet.
fn in_alphabet(token: &str) -> bool {
    !token.is_empty()
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

impl SessionToken {
    /// The token in a login answer's body: a JSON object whose `jwt_token` is a string in the
    /// JWT alphabet. Refused naming the part, never with a byte of what was sent.
    pub fn read(body: &[u8]) -> Result<SessionToken, DecodeError> {
        let doc: Value = serde_json::from_slice(body)
            .map_err(|_| DecodeError::Malformed("login answer is not JSON"))?;
        // The token is moved out of the parsed answer into the Secret, not copied.
        let field = match doc {
            Value::Object(mut map) => map.remove(FIELD),
            _ => None,
        };
        match field {
            Some(Value::String(token)) if in_alphabet(&token) => {
                Ok(SessionToken(Secret::new(token)))
            }
            _ => Err(DecodeError::Malformed(FIELD)),
        }
    }

    /// The token's length in bytes.
    pub fn len(&self) -> usize {
        self.0.expose().len()
    }

    /// Never true: a token read from an answer holds at least one byte.
    pub fn is_empty(&self) -> bool {
        self.0.expose().is_empty()
    }

    /// The WebSocket's JSON-RPC `auth` request as request `id`, the token as `params.bearer`
    /// inside the frame's one redaction span.
    pub fn ws_frame(&self, id: u64) -> WireSlice {
        let head = r#"{"jsonrpc":"2.0","method":"auth","params":{"bearer":""#;
        let token = self.0.expose();
        let text = format!(r#"{head}{token}"}},"id":{id}}}"#);
        let span = span(head.len(), token.len());
        WireSlice::redacted(text.into_bytes(), vec![span])
            .expect("the token's span lies inside the frame")
    }

    /// The REST requests' `Authorization` header, a bearer token, marked redacted.
    pub fn header(&self) -> Header {
        Header {
            name: "Authorization",
            value: format!("Bearer {}", self.0.expose()),
            redact: true,
        }
    }
}

/// The span of `len` bytes from `start`, as a redaction span's `u32` ends. A frame or body the
/// runtime handles is far below 4 GiB, so the ends fit.
fn span(start: usize, len: usize) -> Range<u32> {
    let at = |n: usize| u32::try_from(n).expect("a span within 4 GiB");
    at(start)..at(start + len)
}

/// The credentials in a login answer, for the codec's `redact_inbound` (0028): every copy of
/// the token its `jwt_token` field holds, wherever in the body it stands (overlapping copies
/// as one span), when that is the body's one mention of `jwt_token` and the body holds no
/// escape. Nothing for a body that is empty, or JSON that neither mentions `jwt_token` nor
/// escapes anything (a refusal). Otherwise the whole body: where a token stands cannot be
/// told in an answer that is not JSON, holds a `jwt_token` that is not a non-empty string,
/// names it more than once (a parsed object keeps only the last of repeated keys, Codex
/// r4184896990), nests it, or holds an escape that could spell the key or the token another
/// way.
pub fn token_spans(resp: &HttpResponse<'_>) -> InboundSpans {
    let body = resp.body;
    let whole = || InboundSpans::response(vec![], vec![span(0, body.len())]);
    if body.is_empty() {
        return InboundSpans::NONE;
    }
    let Ok(doc) = serde_json::from_slice::<Value>(body) else {
        return whole();
    };
    let mentions = body
        .windows(FIELD.len())
        .filter(|window| *window == FIELD.as_bytes())
        .count();
    let plain = !body.contains(&b'\\');
    let token = match doc.get(FIELD) {
        None if mentions == 0 && plain => return InboundSpans::NONE,
        Some(Value::String(token)) if !token.is_empty() && mentions == 1 && plain => {
            token.as_bytes()
        }
        _ => return whole(),
    };
    let mut spans: Vec<Range<u32>> = Vec::new();
    let starts = body.windows(token.len()).enumerate();
    for (at, _) in starts.filter(|(_, window)| *window == token) {
        let copy = span(at, token.len());
        match spans.last_mut() {
            Some(last) if last.end >= copy.start => last.end = copy.end,
            _ => spans.push(copy),
        }
    }
    // The token's bytes are in the body: no escape, so the string stands as written.
    InboundSpans::response(vec![], spans)
}
