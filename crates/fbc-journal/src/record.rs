//! The records the runtime writes: everything that crosses the shard boundary (0006), in the
//! shapes design §4.8 gives them as 0014 refines it.
//!
//! A record is owned, so the reader can hand one back. Its `Debug` shows no credential (0009):
//! inbound bytes and response bodies by length only (a venue can echo a key), a redaction span
//! or a redacted header value by length, and a response's headers by count, as `fbc-core`'s
//! own types do.

use core::fmt;
use core::ops::Range;

use fbc_core::{
    ConnKey, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, MonoNs,
    NotSentReason, RawFrame, RpcId, Stamp, Subscription, TimerTag, WireSlice, WireUrl,
};

/// The byte a redaction span reads back as. A span's bytes are never written (its keyed hash
/// is, [`Record::digests`]); the record keeps the span's place and length, and the reader
/// fills it with this byte. It is ASCII, so a blanked URL or header value stays text.
pub const BLANK: u8 = 0;

/// The headers whose values are credentials whatever the codec marked: their values are
/// written as keyed hashes and read back blanked, in requests and results alike (design §9; a venue behind a CDN sets
/// cookies on responses). Matched without regard to case, as HTTP names are.
pub const SECRET_HEADERS: [&str; 4] = [
    "Authorization",
    "Proxy-Authorization",
    "Cookie",
    "Set-Cookie",
];

/// Whether a header of this name holds a credential by name ([`SECRET_HEADERS`]).
pub fn is_secret_header(name: &str) -> bool {
    SECRET_HEADERS.iter().any(|s| name.eq_ignore_ascii_case(s))
}

/// Bytes a record keeps verbatim but never shows: its `Debug` gives the length only.
#[derive(Clone, Eq, PartialEq, Hash, Default)]
pub struct Opaque(pub Vec<u8>);

impl fmt::Debug for Opaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Opaque")
            .field("len", &self.0.len())
            .finish()
    }
}

/// A WebSocket frame's kind, as a codec sees it ([`RawFrame`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Opcode {
    Text,
    Binary,
}

/// What became of writing an outbound frame.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum WriteRes {
    /// The frame reached the socket.
    Written,
    /// No byte of it was written (backpressure, a closed stream).
    NotSent(NotSentReason),
}

/// A connection change or a subscription call: what replay needs to rebuild a session's
/// codecs (one per epoch) and call them where the live session did.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ControlEvent {
    /// The connection opened, under this epoch.
    Opened(ConnKey),
    /// The connection closed.
    Closed(ConnKey),
    /// The codec's `subscribe` was called with these differences.
    Subscribe {
        conn: ConnKey,
        add: Vec<Subscription>,
        remove: Vec<Subscription>,
    },
}

/// A marker in the record stream.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Marker {
    /// A session began. The header is the consumer's, as bytes: what it holds (schema, build,
    /// decoders, spec table, book channels) is the consumer's choice.
    SessionStart { header: Opaque },
    /// Records were dropped: `dropped` of them, from sequence `from_seq` (0006).
    Degraded { from_seq: u64, dropped: u64 },
    /// Records are no longer being dropped.
    Recovered,
}

/// One HTTP header as journaled. `redact` says its value is a credential: the codec marked it,
/// or its name is one of [`SECRET_HEADERS`]. A read-back header with `redact` has its value
/// blanked, at its length. `Debug` shows a redacted value by length only.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct HeaderRec {
    pub name: String,
    pub value: String,
    pub redact: bool,
}

impl HeaderRec {
    /// Whether the journal blanks this header's value.
    pub fn secret(&self) -> bool {
        self.redact || is_secret_header(&self.name)
    }

    fn blanked(&self) -> HeaderRec {
        if self.secret() {
            HeaderRec {
                name: self.name.clone(),
                value: blank_text(self.value.len()),
                redact: true,
            }
        } else {
            self.clone()
        }
    }
}

impl From<&Header> for HeaderRec {
    fn from(h: &Header) -> HeaderRec {
        HeaderRec {
            name: h.name.to_owned(),
            value: h.value.clone(),
            redact: h.redact,
        }
    }
}

impl fmt::Debug for HeaderRec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("HeaderRec");
        d.field("name", &self.name);
        if self.secret() {
            d.field(
                "value",
                &format_args!("<redacted {} bytes>", self.value.len()),
            );
        } else {
            d.field("value", &self.value);
        }
        d.field("redact", &self.redact).finish()
    }
}

/// An HTTP request as journaled: the codec's [`HttpRequest`] with owned header names.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct HttpRequestRec {
    pub method: HttpMethod,
    pub url: WireUrl,
    pub headers: Vec<HeaderRec>,
    pub body: WireSlice,
}

impl HttpRequestRec {
    fn blanked(&self) -> HttpRequestRec {
        HttpRequestRec {
            method: self.method,
            url: blank_url(&self.url),
            headers: self.headers.iter().map(HeaderRec::blanked).collect(),
            body: blank_slice(&self.body),
        }
    }
}

impl From<&HttpRequest> for HttpRequestRec {
    fn from(req: &HttpRequest) -> HttpRequestRec {
        HttpRequestRec {
            method: req.method,
            url: req.url.clone(),
            headers: req.headers.iter().map(HeaderRec::from).collect(),
            body: req.body.clone(),
        }
    }
}

/// An HTTP response as journaled, with its headers. Response headers carry no codec mark
/// (FBC-7lm adds one), so only [`SECRET_HEADERS`] are blanked; the body is kept verbatim.
/// `Debug` shows the status, the number of headers and the body's length only, as
/// [`HttpResponse`]'s does.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct HttpResponseRec {
    pub status: u16,
    pub headers: Vec<HeaderRec>,
    pub body: Opaque,
}

impl HttpResponseRec {
    fn blanked(&self) -> HttpResponseRec {
        HttpResponseRec {
            status: self.status,
            headers: self.headers.iter().map(HeaderRec::blanked).collect(),
            body: self.body.clone(),
        }
    }
}

impl From<&HttpResponse<'_>> for HttpResponseRec {
    fn from(resp: &HttpResponse<'_>) -> HttpResponseRec {
        HttpResponseRec {
            status: resp.status,
            headers: resp
                .headers
                .iter()
                .map(|(name, value)| HeaderRec {
                    name: (*name).to_owned(),
                    value: (*value).to_owned(),
                    redact: false,
                })
                .collect(),
            body: Opaque(resp.body.to_vec()),
        }
    }
}

impl fmt::Debug for HttpResponseRec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpResponseRec")
            .field("status", &self.status)
            .field("headers", &self.headers.len())
            .field("body_len", &self.body.0.len())
            .finish()
    }
}

/// One journal record.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Record {
    /// A frame as it came off a stream, with its stamp, before decoding. Its bytes are
    /// written verbatim: a codec marks no span in an inbound frame until FBC-7lm.
    Inbound {
        stamp: Stamp,
        opcode: Opcode,
        bytes: Opaque,
    },
    /// A frame written to a connection, with its redaction spans.
    Outbound {
        at: MonoNs,
        conn: ConnKey,
        rpc: Option<RpcId>,
        frame: WireSlice,
    },
    /// What became of writing the outbound frame before it on `conn`.
    WriteResult {
        at: MonoNs,
        conn: ConnKey,
        rpc: Option<RpcId>,
        result: WriteRes,
    },
    /// An HTTP request a codec asked for, by the connection epoch whose codec asked.
    HttpRequest {
        at: MonoNs,
        conn: ConnKey,
        tag: HttpTag,
        rpc: Option<RpcId>,
        req: HttpRequestRec,
    },
    /// An HTTP request's response with its headers, or why none came. `stamp` is the one the
    /// runtime gave the events the codec pushed for it: its ingest sequence, when the result
    /// came (both clocks), and the connection epoch whose codec asked.
    HttpResult {
        stamp: Stamp,
        tag: HttpTag,
        result: Result<HttpResponseRec, HttpFailure>,
    },
    /// A codec's timer fired. `stamp` is the one the runtime gave the events the codec pushed
    /// for it: its ingest sequence, the `now` and `wall` the codec's `on_timer` was called
    /// with, and the connection epoch whose codec set the timer.
    Timer { stamp: Stamp, tag: TimerTag },
    /// A connection change or a subscription call.
    Control { at: MonoNs, ev: ControlEvent },
    /// A marker.
    Marker(Marker),
}

impl Record {
    /// An inbound frame as it came off a stream.
    pub fn inbound(stamp: Stamp, frame: RawFrame<'_>) -> Record {
        let opcode = match frame {
            RawFrame::Text(_) => Opcode::Text,
            RawFrame::Binary(_) => Opcode::Binary,
        };
        Record::Inbound {
            stamp,
            opcode,
            bytes: Opaque(frame.bytes().to_vec()),
        }
    }

    /// The record as the reader returns it: every redaction span's bytes and every secret
    /// header's value replaced by [`BLANK`] at the same place and length, and those headers
    /// marked `redact`. A record with nothing to redact is returned as it is.
    pub fn blanked(&self) -> Record {
        match self {
            Record::Outbound {
                at,
                conn,
                rpc,
                frame,
            } => Record::Outbound {
                at: *at,
                conn: *conn,
                rpc: *rpc,
                frame: blank_slice(frame),
            },
            Record::HttpRequest {
                at,
                conn,
                tag,
                rpc,
                req,
            } => Record::HttpRequest {
                at: *at,
                conn: *conn,
                tag: *tag,
                rpc: *rpc,
                req: req.blanked(),
            },
            Record::HttpResult { stamp, tag, result } => Record::HttpResult {
                stamp: *stamp,
                tag: *tag,
                result: result
                    .as_ref()
                    .map(HttpResponseRec::blanked)
                    .map_err(|e| *e),
            },
            other => other.clone(),
        }
    }
}

/// `len` blank characters.
fn blank_text(len: usize) -> String {
    String::from_utf8(vec![BLANK; len]).expect("BLANK is ASCII")
}

/// `bytes` with every span's bytes blanked.
fn blank_spans(bytes: &[u8], spans: &[Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        out[span.start as usize..span.end as usize].fill(BLANK);
    }
    out
}

fn blank_slice(slice: &WireSlice) -> WireSlice {
    let bytes = blank_spans(slice.bytes(), slice.redactions());
    WireSlice::redacted(bytes, slice.redactions().to_vec()).expect("spans already checked")
}

fn blank_url(url: &WireUrl) -> WireUrl {
    // Spans lie on character boundaries, so blanking whole characters with ASCII keeps UTF-8.
    let text = String::from_utf8(blank_spans(url.as_str().as_bytes(), url.redactions()))
        .expect("spans cover whole characters");
    WireUrl::redacted(text, url.redactions().to_vec()).expect("spans already checked")
}
