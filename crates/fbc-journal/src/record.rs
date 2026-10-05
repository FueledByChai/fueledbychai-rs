//! The records the runtime writes: everything that crosses the shard boundary (0006), in the
//! shapes design §4.8 gives them as 0014 refines it.
//!
//! A record is owned, so the reader can hand one back. Its `Debug` shows no credential (0009):
//! inbound bytes and response bodies by length only (a venue can echo a key a codec did not
//! mark), a redaction span or a redacted header name or value by length, and a response's
//! headers by count, as `fbc-core`'s own types do.
//!
//! Inbound bytes carry the spans the codec named in them ([`InboundSpans`], decision 0028):
//! [`Record::inbound_redacted`] and [`HttpResponseRec::redacted`] build an inbound frame's or a
//! response's record with them, and the journal blanks them as it does an outbound frame's.

use core::fmt;
use core::ops::Range;

use fbc_core::{
    ConnKey, EncodeCtx, Header, HeaderMark, HttpFailure, HttpMethod, HttpRequest, HttpResponse,
    HttpTag, Inbound, InboundSpans, InstrumentId, MonoNs, NotSentReason, RawFrame, RedactError,
    RpcId, Stamp, Subscription, TimerTag, WireSlice, WireUrl,
};

/// The byte a redaction span reads back as. A span's bytes are never written (its keyed hash
/// is, [`Record::digests`]); the record keeps the span's place and length, and the reader
/// fills it with this byte. It is the ASCII digit `2`, which every text form a credential
/// takes accepts in its place: a JSON string or number, an HTTP header value, a URL, and the
/// decimal, hex, base32, base58 and base64 alphabets. So a codec that parses a replayed
/// response or frame (with `serde_json`, a header parser) accepts what it accepted live
/// (decision 0028). It is not distinctive: the record's spans, not its bytes, say what was
/// blanked.
pub const BLANK: u8 = b'2';

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

impl Opcode {
    /// The kind a frame of these bytes is sent as: text when they are UTF-8, binary otherwise.
    /// A session chooses an outbound frame's kind this way; a journal of format version 4 or
    /// earlier, which did not keep it, reads an outbound frame back with the kind its blanked
    /// bytes imply.
    pub fn of(bytes: &[u8]) -> Opcode {
        match core::str::from_utf8(bytes) {
            Ok(_) => Opcode::Text,
            Err(_) => Opcode::Binary,
        }
    }
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

/// A nonce source as the runtime numbers it: one per scope the venue's
/// [`NonceScope`](fbc_core::NonceScope) names (an account, a signing key). Which number names
/// which source is the runtime's choice.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NonceSourceId(pub u32);

/// One HTTP header as journaled. `redact` says its value is a credential: the codec marked it,
/// or its name is one of [`SECRET_HEADERS`]; `redact_name` says its name is one too (a key a
/// proxy echoes in a header name, [`HeaderMark::NameAndValue`]), and its value with it. A
/// read-back header has what was secret blanked, at its length, and `redact` set. `Debug`
/// shows a redacted name or value by length only.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct HeaderRec {
    pub name: String,
    pub value: String,
    pub redact: bool,
    pub redact_name: bool,
}

impl HeaderRec {
    /// Whether the journal blanks this header's value.
    pub fn secret(&self) -> bool {
        self.redact || self.redact_name || is_secret_header(&self.name)
    }

    fn blanked(&self) -> HeaderRec {
        if self.secret() {
            HeaderRec {
                name: if self.redact_name {
                    blank_text(self.name.len())
                } else {
                    self.name.clone()
                },
                value: blank_text(self.value.len()),
                redact: true,
                redact_name: self.redact_name,
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
            redact_name: false,
        }
    }
}

impl fmt::Debug for HeaderRec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("HeaderRec");
        if self.redact_name {
            d.field(
                "name",
                &format_args!("<redacted {} bytes>", self.name.len()),
            );
        } else {
            d.field("name", &self.name);
        }
        if self.secret() {
            d.field(
                "value",
                &format_args!("<redacted {} bytes>", self.value.len()),
            );
        } else {
            d.field("value", &self.value);
        }
        d.field("redact", &self.redact)
            .field("redact_name", &self.redact_name)
            .finish()
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

/// An HTTP response as journaled, with its headers and the spans of its body that hold
/// credentials. The headers the codec marked ([`HttpResponseRec::redacted`]) and the
/// [`SECRET_HEADERS`] are blanked, and so are `body_redact`'s spans; the rest of the body is
/// kept verbatim. `Debug` shows the status, the number of headers and the body's length only,
/// as [`HttpResponse`]'s does.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct HttpResponseRec {
    pub status: u16,
    pub headers: Vec<HeaderRec>,
    pub body: Opaque,
    /// The body's credential spans: non-empty, inside it, in ascending order.
    pub body_redact: Vec<Range<u32>>,
}

impl HttpResponseRec {
    /// `resp` as journaled with the credentials its codec named in it
    /// ([`ExecCodec::redact_inbound`](fbc_core::ExecCodec::redact_inbound)): the marked
    /// headers and body spans are blanked. `Err` when `spans` do not fit `resp`
    /// ([`InboundSpans::check`]).
    pub fn redacted(
        resp: &HttpResponse<'_>,
        spans: &InboundSpans,
    ) -> Result<HttpResponseRec, RedactError> {
        spans.check(Inbound::Http(HttpTag(0), *resp))?;
        let mut rec = HttpResponseRec::from(resp);
        for &(at, mark) in spans.headers() {
            // Checked: every mark names a header.
            let h = &mut rec.headers[at as usize];
            h.redact = true;
            h.redact_name = mark == HeaderMark::NameAndValue;
        }
        rec.body_redact = spans.body().to_vec();
        Ok(rec)
    }

    fn blanked(&self) -> HttpResponseRec {
        HttpResponseRec {
            status: self.status,
            headers: self.headers.iter().map(HeaderRec::blanked).collect(),
            body: Opaque(blank_spans(&self.body.0, &self.body_redact)),
            body_redact: self.body_redact.clone(),
        }
    }
}

impl From<&HttpResponse<'_>> for HttpResponseRec {
    /// `resp` with nothing marked: only the [`SECRET_HEADERS`] are blanked.
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
                    redact_name: false,
                })
                .collect(),
            body: Opaque(resp.body.to_vec()),
            body_redact: Vec::new(),
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
    /// A frame as it came off a stream, with its stamp, before decoding, and the spans of its
    /// bytes its codec named as credentials ([`Record::inbound_redacted`]): non-empty, inside
    /// the bytes, in ascending order, and on character boundaries in a text frame. The spans
    /// are blanked; the rest is written verbatim.
    Inbound {
        stamp: Stamp,
        opcode: Opcode,
        bytes: Opaque,
        redact: Vec<Range<u32>>,
    },
    /// A frame written to a connection, with the kind it was sent as and its redaction spans.
    /// A text frame's bytes are UTF-8 as sent; a span may split a character, so blanked they
    /// need not be. The kind is kept rather than read off the bytes (FBC-q7b): a binary frame
    /// whose only bytes that are not UTF-8 lie in its spans is UTF-8 once blanked.
    Outbound {
        at: MonoNs,
        conn: ConnKey,
        rpc: Option<RpcId>,
        opcode: Opcode,
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
    /// A nonce `source` reserved: one record per value, in the order reserved, so replay and a
    /// restarted source carry on from exactly where the live one stood (0006).
    Nonce { source: NonceSourceId, value: u64 },
    /// The context one call was given (0014 item 1): its wall and monotonic time and the
    /// nonces reserved for it, which replay hands the same call. `rpc` names the request of an
    /// `encode`; it is `None` for the other calls that take a context (`on_open`, `on_timer`,
    /// `resync`), which replay matches by their place in the journal.
    EncodeCtx { rpc: Option<RpcId>, ctx: EncodeCtx },
    /// A decide cycle began (design §4.8): every input up to ingest sequence
    /// `last_ingest_seq` had been drained, and the shard ran its strategy and planner for
    /// `instruments`, in that order. Written for every pass, a pass that decides nothing
    /// included, so replay decides exactly where the live shard did.
    Cycle {
        last_ingest_seq: u64,
        instruments: Vec<InstrumentId>,
    },
}

impl Record {
    /// An inbound frame as it came off a stream, with nothing marked: for a codec whose
    /// frames hold no credential ([`InboundSpans::NONE`]).
    pub fn inbound(stamp: Stamp, frame: RawFrame<'_>) -> Record {
        Record::inbound_with(stamp, frame, Vec::new())
    }

    fn inbound_with(stamp: Stamp, frame: RawFrame<'_>, redact: Vec<Range<u32>>) -> Record {
        let opcode = match frame {
            RawFrame::Text(_) => Opcode::Text,
            RawFrame::Binary(_) => Opcode::Binary,
        };
        Record::Inbound {
            stamp,
            opcode,
            bytes: Opaque(frame.bytes().to_vec()),
            redact,
        }
    }

    /// An inbound frame with the credentials its codec named in it
    /// ([`MdCodec::redact_inbound`](fbc_core::MdCodec::redact_inbound),
    /// [`ExecCodec::redact_inbound`](fbc_core::ExecCodec::redact_inbound)), which the journal
    /// blanks. `Err` when `spans` do not fit `frame` ([`InboundSpans::check`]).
    pub fn inbound_redacted(
        stamp: Stamp,
        frame: RawFrame<'_>,
        spans: &InboundSpans,
    ) -> Result<Record, RedactError> {
        spans.check(Inbound::Frame(frame))?;
        Ok(Record::inbound_with(stamp, frame, spans.body().to_vec()))
    }

    /// The record as the reader returns it: every redaction span's bytes and every secret
    /// header's value replaced by [`BLANK`] at the same place and length, and those headers
    /// marked `redact`. A record with nothing to redact is returned as it is.
    pub fn blanked(&self) -> Record {
        match self {
            Record::Inbound {
                stamp,
                opcode,
                bytes,
                redact,
            } => Record::Inbound {
                stamp: *stamp,
                opcode: *opcode,
                bytes: Opaque(blank_spans(&bytes.0, redact)),
                redact: redact.clone(),
            },
            Record::Outbound {
                at,
                conn,
                rpc,
                opcode,
                frame,
            } => Record::Outbound {
                at: *at,
                conn: *conn,
                rpc: *rpc,
                opcode: *opcode,
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

/// A record offered to a sink with its large contents borrowed from where they lie (FBC-f3w):
/// a frame, a request, a response or a subscribe call's sets. A sink with no room for one
/// refuses it before a byte of its payload is copied, and one it admits is encoded straight
/// from what it borrows, so no size is worked out apart from the encoding itself. Each encodes
/// exactly as the owned [`Record`] [`to_record`](RecordRef::to_record) gives, which is what the
/// reader returns. Every other kind is offered [`Owned`](RecordRef::Owned).
///
/// Its `Debug` is the owned record's, which shows no credential.
#[derive(Copy, Clone)]
pub enum RecordRef<'a> {
    /// Any record, owned.
    Owned(&'a Record),
    /// A [`Record::Inbound`]: the frame as it came off a stream.
    Inbound { stamp: Stamp, frame: RawFrame<'a> },
    /// A [`Record::HttpRequest`]: the request a codec asked for.
    HttpRequest {
        at: MonoNs,
        conn: ConnKey,
        tag: HttpTag,
        rpc: Option<RpcId>,
        req: &'a HttpRequest,
    },
    /// A [`Record::HttpResult`]: the response as the transport handed it, or why none came.
    HttpResult {
        stamp: Stamp,
        tag: HttpTag,
        result: Result<ResponseRef<'a>, HttpFailure>,
    },
    /// A [`Record::Control`] of a [`ControlEvent::Subscribe`].
    Subscribe {
        at: MonoNs,
        conn: ConnKey,
        add: &'a [Subscription],
        remove: &'a [Subscription],
    },
}

/// An HTTP response as the transport handed it, borrowed: its status, its headers in order
/// with their values as raw bytes, and its body. The journal reads each header value lossily as
/// UTF-8 (each invalid sequence as U+FFFD), as a codec is handed it, and only once the record
/// is admitted. `Debug` shows the status, the number of headers and the body's length only.
#[derive(Copy, Clone)]
pub struct ResponseRef<'a> {
    pub status: u16,
    pub headers: &'a [(&'a str, &'a [u8])],
    pub body: &'a [u8],
}

impl fmt::Debug for ResponseRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseRef")
            .field("status", &self.status)
            .field("headers", &self.headers.len())
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl From<&ResponseRef<'_>> for HttpResponseRec {
    fn from(resp: &ResponseRef<'_>) -> HttpResponseRec {
        HttpResponseRec {
            status: resp.status,
            headers: resp
                .headers
                .iter()
                .map(|(name, value)| HeaderRec {
                    name: (*name).to_owned(),
                    value: String::from_utf8_lossy(value).into_owned(),
                    redact: false,
                    redact_name: false,
                })
                .collect(),
            body: Opaque(resp.body.to_vec()),
            body_redact: Vec::new(),
        }
    }
}

impl fmt::Debug for RecordRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The owned record's `Debug` is the one reviewed for credentials (0009); this is for
        // diagnostics, not a hot path, so building it is fine.
        f.debug_tuple("RecordRef").field(&self.to_record()).finish()
    }
}

impl<'a> From<&'a Record> for RecordRef<'a> {
    fn from(record: &'a Record) -> RecordRef<'a> {
        RecordRef::Owned(record)
    }
}

impl RecordRef<'_> {
    /// The record this stands for, owned: what it encodes as.
    pub fn to_record(&self) -> Record {
        match *self {
            RecordRef::Owned(record) => record.clone(),
            RecordRef::Inbound { stamp, frame } => Record::inbound(stamp, frame),
            RecordRef::HttpRequest {
                at,
                conn,
                tag,
                rpc,
                req,
            } => Record::HttpRequest {
                at,
                conn,
                tag,
                rpc,
                req: HttpRequestRec::from(req),
            },
            RecordRef::HttpResult { stamp, tag, result } => Record::HttpResult {
                stamp,
                tag,
                result: result.as_ref().map(HttpResponseRec::from).map_err(|e| *e),
            },
            RecordRef::Subscribe {
                at,
                conn,
                add,
                remove,
            } => Record::Control {
                at,
                ev: ControlEvent::Subscribe {
                    conn,
                    add: add.to_vec(),
                    remove: remove.to_vec(),
                },
            },
        }
    }
}

/// `len` blank characters.
fn blank_text(len: usize) -> String {
    String::from_utf8(vec![BLANK; len]).expect("BLANK is ASCII")
}

/// `bytes` with every span's bytes blanked. A span reaching past the end (in a record built
/// field by field, which the writer refuses) is blanked as far as the bytes go.
fn blank_spans(bytes: &[u8], spans: &[Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        let end = (span.end as usize).min(out.len());
        if let Some(part) = out.get_mut(span.start as usize..end) {
            part.fill(BLANK);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Codex r4179379939: a borrowed record shows no credential, a header secret by name
    /// included, as the owned record it stands for does not.
    #[test]
    fn a_borrowed_record_shows_no_credential() {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain("https://toy/x"),
            headers: vec![Header {
                name: "Authorization",
                value: "live-secret-token".into(),
                redact: false,
            }],
            body: WireSlice::plain(Vec::new()),
        };
        let stamp = Stamp {
            ingest_seq: 1,
            kernel_rx: None,
            recv_mono: MonoNs(1),
            recv_wall: fbc_core::WallNs(1),
            conn: ConnKey { conn: 1, epoch: 1 },
        };
        let request = RecordRef::HttpRequest {
            at: MonoNs(1),
            conn: stamp.conn,
            tag: HttpTag(1),
            rpc: None,
            req: &req,
        };
        let headers = [("set-cookie", &b"sid=live-cookie"[..])];
        let result = RecordRef::HttpResult {
            stamp,
            tag: HttpTag(1),
            result: Ok(ResponseRef {
                status: 200,
                headers: &headers,
                body: b"live-body",
            }),
        };
        let response = ResponseRef {
            status: 200,
            headers: &headers,
            body: b"live-body",
        };
        let shown = [
            format!("{request:?}"),
            format!("{result:?}"),
            format!("{response:?}"),
        ];
        for shown in &shown {
            assert!(!shown.contains("live-"), "{shown}");
        }
        assert_eq!(
            shown[2],
            "ResponseRef { status: 200, headers: 1, body_len: 9 }"
        );
    }
}
