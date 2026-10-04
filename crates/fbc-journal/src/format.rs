//! The on-disk format, hand-written little-endian with no serialization dependency.
//!
//! A segment is [`MAGIC`], the format [`VERSION`] (`u16`), then records. A record is its body's
//! length (`u32`) and its body: a kind byte and the kind's fields in order. Integers are
//! little-endian; an `Option` is a byte (0 none, 1 some) and the value; a byte string or text
//! is its length (`u32`) and its bytes; an enum is a tag byte and its fields.
//!
//! Redacted content is never written (FBC-apz, decision 0024). A [`WireSlice`] or [`WireUrl`] is
//! its total length, its span count, each span's start and end (`u32`) and the span's
//! HMAC-SHA-256 under the [`RedactionKey`] (32 bytes), and then only the bytes outside the
//! spans; a secret header is its name, a flag byte, its value's length and the value's
//! HMAC-SHA-256. The reader puts [`BLANK`] where the spans were and returns the hashes beside
//! the record ([`Record::digests`] gives their order). A record redacts at most
//! [`MAX_REDACTED`] bytes, so a damaged length cannot make the reader allocate more than that
//! for blanks.
//!
//! Version 3 (FBC-ec9) adds three kinds and changes none: `Nonce`, `EncodeCtx` and `Cycle`.
//! A version 2 segment holding one of them is malformed. A reader of version 2 refuses a
//! version 3 segment at its header, rather than part way through.
//!
//! Version 4 (FBC-7lm, decision 0028) writes the credentials a codec named in inbound bytes as
//! keyed hashes: an inbound frame's bytes and a response's body are written as a [`WireSlice`]
//! is (their spans hashed, the rest verbatim), and a header is a flag byte (0 plain, 1 its
//! value secret, 2 its name and value secret) then its name and its value, each secret one as
//! its length and its hash. Versions 2 and 3 wrote a header's name before its flag, and an
//! inbound frame's bytes and a response's body whole. The reader reads versions 2, 3 and 4, so
//! a journal written before reads back unchanged.

use core::ops::Range;

use fbc_core::{
    BookId, ConnKey, EncodeCtx, Feed, Header, HttpFailure, HttpMethod, HttpTag, InstrumentId,
    KernelRxNs, MonoNs, NonceBlock, NotSentReason, RawFrame, RpcId, Stamp, Subscription, TimerTag,
    TouchSourceId, WallNs, WireSlice, WireUrl, check_redactions,
};

use crate::JournalError;
use crate::record::{
    BLANK, ControlEvent, HeaderRec, HttpRequestRec, HttpResponseRec, Marker, NonceSourceId, Opaque,
    Opcode, Record, RecordRef, WriteRes, is_secret_header,
};
use crate::redact::{DIGEST_LEN, RedactionKey, SpanDigest};

/// The first bytes of every segment.
pub const MAGIC: [u8; 4] = *b"FBCJ";
/// The format version this crate writes. Version 1 (FBC-aen) wrote no hash for a span;
/// version 2 writes each span's keyed hash (FBC-apz); version 3 adds the `Nonce`, `EncodeCtx`
/// and `Cycle` kinds (FBC-ec9); version 4 hashes the spans a codec names in inbound frames,
/// response bodies and response header names (FBC-7lm).
pub const VERSION: u16 = 4;
/// The oldest format version this crate reads: version 1 is refused (0024).
pub const OLDEST_READABLE: u16 = 2;
/// The most bytes one record may redact, its spans and secret header values together.
/// Credentials are short; the bound keeps a damaged length from making the reader allocate
/// gigabytes of blanks. The writer refuses a record over it ([`JournalError::TooLarge`]).
pub const MAX_REDACTED: u64 = 1 << 20;

const INBOUND: u8 = 1;
const OUTBOUND: u8 = 2;
const WRITE_RESULT: u8 = 3;
const HTTP_REQUEST: u8 = 4;
const HTTP_RESULT: u8 = 5;
const TIMER: u8 = 6;
const CONTROL: u8 = 7;
const MARKER: u8 = 8;
// Version 3.
const NONCE: u8 = 9;
const ENCODE_CTX: u8 = 10;
const CYCLE: u8 = 11;

// Field-less enums are a byte: the value's place in its table. Encoding matches exhaustively,
// so a new variant fails to compile until it has a byte; decoding indexes the table.
const OPCODES: [Opcode; 2] = [Opcode::Text, Opcode::Binary];
const METHODS: [HttpMethod; 4] = [
    HttpMethod::Get,
    HttpMethod::Post,
    HttpMethod::Put,
    HttpMethod::Delete,
];
const FAILURES: [HttpFailure; 3] = [
    HttpFailure::NotSent,
    HttpFailure::TimedOut,
    HttpFailure::Lost,
];
const NOT_SENT: [NotSentReason; 7] = [
    NotSentReason::Disconnected,
    NotSentReason::Backpressure,
    NotSentReason::RateBudget,
    NotSentReason::Unsupported,
    NotSentReason::FlagConflict,
    NotSentReason::Unencodable,
    NotSentReason::SignFailed,
];

fn opcode_byte(v: Opcode) -> u8 {
    match v {
        Opcode::Text => 0,
        Opcode::Binary => 1,
    }
}

fn method_byte(v: HttpMethod) -> u8 {
    match v {
        HttpMethod::Get => 0,
        HttpMethod::Post => 1,
        HttpMethod::Put => 2,
        HttpMethod::Delete => 3,
    }
}

fn failure_byte(v: HttpFailure) -> u8 {
    match v {
        HttpFailure::NotSent => 0,
        HttpFailure::TimedOut => 1,
        HttpFailure::Lost => 2,
    }
}

fn not_sent_byte(v: NotSentReason) -> u8 {
    match v {
        NotSentReason::Disconnected => 0,
        NotSentReason::Backpressure => 1,
        NotSentReason::RateBudget => 2,
        NotSentReason::Unsupported => 3,
        NotSentReason::FlagConflict => 4,
        NotSentReason::Unencodable => 5,
        NotSentReason::SignFailed => 6,
    }
}

/// A length that must fit the format's `u32`.
fn len32(n: usize) -> Result<u32, JournalError> {
    u32::try_from(n).map_err(|_| JournalError::TooLarge)
}

/// Appends a record's body (kind and fields) to `out`, each redacted span hashed under `key`.
pub(crate) fn encode(
    record: &Record,
    key: &RedactionKey,
    out: &mut Vec<u8>,
) -> Result<(), JournalError> {
    encode_within(record, key, out, usize::MAX)
}

/// [`encode`], refusing a body longer than `limit` bytes with [`JournalError::TooLarge`]
/// before copying the bytes that would pass it, so a record too large for the room left
/// costs no copy of its payload. A refused record leaves `out` as it was. A borrowed record
/// ([`RecordRef`]) is encoded from what it borrows, by the same code as the owned record it
/// stands for, so the room it is admitted to is checked at its exact encoded length.
pub(crate) fn encode_within<'r>(
    record: impl Into<RecordRef<'r>>,
    key: &RedactionKey,
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<(), JournalError> {
    let record = record.into();
    let start = out.len();
    // A first pass measures the body, hashing and copying nothing, so a record that cannot fit
    // is refused before any of its redactions is hashed (Codex r4179465515); the same code then
    // writes it.
    let encoded = encode_body(record, key, out, limit, false)
        .and_then(|()| encode_body(record, key, out, limit, true));
    // A refused record leaves `out` as it found it.
    if encoded.is_err() {
        out.truncate(start);
    }
    encoded
}

/// Encodes `record` into `out` within `limit` bytes, or with `write` false only measures it:
/// nothing is copied or hashed, and only a record too large is refused.
fn encode_body(
    record: RecordRef<'_>,
    key: &RedactionKey,
    out: &mut Vec<u8>,
    limit: usize,
    write: bool,
) -> Result<(), JournalError> {
    let mut e = Enc {
        end: body_end(out.len(), limit),
        at: out.len(),
        write,
        out,
        key,
        redacted: 0,
        over: false,
    };
    // A borrowed record goes through the same field writers as the owned record it stands for.
    let record = match record {
        RecordRef::Owned(record) => record,
        RecordRef::Inbound { stamp, frame } => {
            let opcode = match frame {
                RawFrame::Text(_) => Opcode::Text,
                RawFrame::Binary(_) => Opcode::Binary,
            };
            e.inbound(&stamp, opcode, frame.bytes(), &[])?;
            return e.within();
        }
        RecordRef::HttpRequest {
            at,
            conn,
            tag,
            rpc,
            req,
        } => {
            let hide = |h: &Header| Hide::value_if(h.redact || is_secret_header(h.name));
            let headers = req
                .headers
                .iter()
                .map(|h| (h.name, Value::Text(&h.value), hide(h)));
            e.http_request(
                (at, conn, tag, rpc),
                req.method,
                &req.url,
                headers,
                &req.body,
            )?;
            return e.within();
        }
        RecordRef::HttpResult { stamp, tag, result } => {
            let result = result.map(|r| {
                let headers = r.headers.iter();
                let headers = headers.map(|(name, raw)| {
                    (
                        *name,
                        Value::Lossy(raw),
                        Hide::value_if(is_secret_header(name)),
                    )
                });
                // A borrowed response carries no spans: a codec names them (decision 0028)
                // only once the session asks it (FBC-s69).
                (r.status, headers, (r.body, &[][..]))
            });
            e.http_result(&stamp, tag, result)?;
            return e.within();
        }
        RecordRef::Subscribe {
            at,
            conn,
            add,
            remove,
        } => {
            e.subscribe(at, conn, add, remove)?;
            return e.within();
        }
    };
    match record {
        Record::Inbound {
            stamp,
            opcode,
            bytes,
            redact,
        } => e.inbound(stamp, *opcode, &bytes.0, redact)?,
        Record::Outbound {
            at,
            conn,
            rpc,
            frame,
        } => {
            e.u8(OUTBOUND);
            e.u64(at.0);
            e.conn(*conn);
            e.rpc(*rpc);
            e.spanned(frame.bytes(), frame.redactions())?;
        }
        Record::WriteResult {
            at,
            conn,
            rpc,
            result,
        } => {
            e.u8(WRITE_RESULT);
            e.u64(at.0);
            e.conn(*conn);
            e.rpc(*rpc);
            match result {
                WriteRes::Written => e.u8(0),
                WriteRes::NotSent(why) => {
                    e.u8(1);
                    e.u8(not_sent_byte(*why));
                }
            }
        }
        Record::HttpRequest {
            at,
            conn,
            tag,
            rpc,
            req,
        } => {
            let headers = rec_headers(&req.headers);
            e.http_request(
                (*at, *conn, *tag, *rpc),
                req.method,
                &req.url,
                headers,
                &req.body,
            )?;
        }
        Record::HttpResult { stamp, tag, result } => {
            let result = result
                .as_ref()
                .map(|r| {
                    let body = (r.body.0.as_slice(), r.body_redact.as_slice());
                    (r.status, rec_headers(&r.headers), body)
                })
                .map_err(|e| *e);
            e.http_result(stamp, *tag, result)?;
        }
        Record::Timer { stamp, tag } => {
            e.u8(TIMER);
            e.stamp(stamp);
            e.u64(tag.0);
        }
        Record::Control { at, ev } => {
            e.u8(CONTROL);
            e.u64(at.0);
            match ev {
                ControlEvent::Opened(conn) => {
                    e.u8(0);
                    e.conn(*conn);
                }
                ControlEvent::Closed(conn) => {
                    e.u8(1);
                    e.conn(*conn);
                }
                ControlEvent::Subscribe { conn, add, remove } => {
                    e.subscription(*conn, add, remove)?;
                }
            }
        }
        Record::Marker(marker) => {
            e.u8(MARKER);
            match marker {
                Marker::SessionStart { header } => {
                    e.u8(0);
                    e.bytes(&header.0)?;
                }
                Marker::Degraded { from_seq, dropped } => {
                    e.u8(1);
                    e.u64(*from_seq);
                    e.u64(*dropped);
                }
                Marker::Recovered => e.u8(2),
            }
        }
        Record::Nonce { source, value } => {
            e.u8(NONCE);
            e.u32(source.0);
            e.u64(*value);
        }
        Record::EncodeCtx { rpc, ctx } => {
            e.u8(ENCODE_CTX);
            e.rpc(*rpc);
            e.i64(ctx.wall.0);
            e.u64(ctx.mono.0);
            let nonces = ctx.nonces.as_slice();
            e.u32(len32(nonces.len())?);
            for n in nonces {
                e.u64(*n);
                e.within()?;
            }
        }
        Record::Cycle {
            last_ingest_seq,
            instruments,
        } => {
            e.u8(CYCLE);
            e.u64(*last_ingest_seq);
            e.u32(len32(instruments.len())?);
            for inst in instruments {
                e.u32(inst.get());
                e.within()?;
            }
        }
    }
    e.within()
}

impl Enc<'_> {
    /// An inbound frame: its stamp, its opcode and its bytes with the spans a codec named in
    /// them.
    fn inbound(
        &mut self,
        stamp: &Stamp,
        opcode: Opcode,
        bytes: &[u8],
        spans: &[Range<u32>],
    ) -> Result<(), JournalError> {
        self.u8(INBOUND);
        self.stamp(stamp);
        self.u8(opcode_byte(opcode));
        // Checked before the payload is copied, and only for a payload that fits after the
        // fields before it: one that cannot is refused for its size, unscanned.
        if self.write
            && opcode == Opcode::Text
            && bytes.len() <= self.room()
            && core::str::from_utf8(bytes).is_err()
        {
            return Err(JournalError::Unencodable("a text frame that is not UTF-8"));
        }
        self.inbound_spanned(bytes, spans)
    }

    /// An HTTP request: when and by whom it was asked for (`at`, `conn`, `tag`, `rpc`), its
    /// method, URL, headers (name, value, what of it is secret) and body.
    fn http_request<'h>(
        &mut self,
        (at, conn, tag, rpc): (MonoNs, ConnKey, HttpTag, Option<RpcId>),
        method: HttpMethod,
        url: &WireUrl,
        headers: impl ExactSizeIterator<Item = (&'h str, Value<'h>, Hide)>,
        body: &WireSlice,
    ) -> Result<(), JournalError> {
        self.u8(HTTP_REQUEST);
        self.u64(at.0);
        self.conn(conn);
        self.u64(tag.0);
        self.rpc(rpc);
        self.u8(method_byte(method));
        self.spanned(url.as_str().as_bytes(), url.redactions())?;
        self.headers(headers)?;
        self.spanned(body.bytes(), body.redactions())
    }

    /// An HTTP result: the response's status, headers (name, value, what of it is secret) and
    /// body with the spans a codec named in it, or why none came.
    fn http_result<'h>(
        &mut self,
        stamp: &Stamp,
        tag: HttpTag,
        result: Result<
            (
                u16,
                impl ExactSizeIterator<Item = (&'h str, Value<'h>, Hide)>,
                Spanned<'_>,
            ),
            HttpFailure,
        >,
    ) -> Result<(), JournalError> {
        self.u8(HTTP_RESULT);
        self.stamp(stamp);
        self.u64(tag.0);
        match result {
            Ok((status, headers, (body, spans))) => {
                self.u8(0);
                self.u16(status);
                self.headers(headers)?;
                self.inbound_spanned(body, spans)
            }
            Err(failure) => {
                self.u8(1);
                self.u8(failure_byte(failure));
                Ok(())
            }
        }
    }

    /// A control record of a subscribe call.
    fn subscribe(
        &mut self,
        at: MonoNs,
        conn: ConnKey,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<(), JournalError> {
        self.u8(CONTROL);
        self.u64(at.0);
        self.subscription(conn, add, remove)
    }

    /// A subscribe call's event: its connection and the subscriptions added and removed.
    fn subscription(
        &mut self,
        conn: ConnKey,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<(), JournalError> {
        self.u8(2);
        self.conn(conn);
        self.subs(add)?;
        self.subs(remove)
    }
}

/// Journaled headers as (name, value, what of it is secret).
fn rec_headers(headers: &[HeaderRec]) -> impl ExactSizeIterator<Item = (&str, Value<'_>, Hide)> {
    headers.iter().map(|h| {
        let hide = if h.redact_name {
            Hide::NameAndValue
        } else {
            Hide::value_if(h.secret())
        };
        (h.name.as_str(), Value::Text(&h.value), hide)
    })
}

/// Bytes with the redaction spans named in them.
type Spanned<'b> = (&'b [u8], &'b [Range<u32>]);

/// What of a header the journal keeps only as keyed hashes.
#[derive(Copy, Clone)]
enum Hide {
    Nothing,
    Value,
    NameAndValue,
}

impl Hide {
    /// Its value when `secret`, else nothing.
    fn value_if(secret: bool) -> Hide {
        if secret { Hide::Value } else { Hide::Nothing }
    }
}

/// The UTF-8 a lossily read value is written as: [`String::from_utf8_lossy`]'s, without
/// building it.
const REPLACEMENT: &[u8] = "\u{FFFD}".as_bytes();

/// A header value as journaled: text, or raw bytes read lossily as UTF-8.
#[derive(Copy, Clone)]
enum Value<'a> {
    Text(&'a str),
    Lossy(&'a [u8]),
}

impl<'a> Value<'a> {
    /// The value's UTF-8 in pieces: a lossy one's valid runs, each invalid sequence as U+FFFD.
    fn chunks(self) -> impl Iterator<Item = &'a [u8]> {
        let (text, raw): (&[u8], &[u8]) = match self {
            Value::Text(text) => (text.as_bytes(), &[]),
            Value::Lossy(raw) => (&[], raw),
        };
        let lossy = raw.utf8_chunks().flat_map(|c| {
            let bad: &[u8] = if c.invalid().is_empty() {
                &[]
            } else {
                REPLACEMENT
            };
            [c.valid().as_bytes(), bad]
        });
        core::iter::once(text).chain(lossy)
    }

    /// The length of the value's UTF-8.
    fn len(self) -> usize {
        self.chunks().map(<[u8]>::len).sum()
    }
}

/// A span's descriptor: its start and end (`u32`) and its keyed hash.
const SPAN_DESCRIPTOR: usize = 8 + DIGEST_LEN;

/// Where a body that starts at `start` in its buffer must end: within `limit` bytes, and never
/// past the format's `u32` body length.
fn body_end(start: usize, limit: usize) -> usize {
    start.saturating_add(limit.min(u32::MAX as usize))
}

/// The output, the key spans are hashed under, the bytes redacted so far, where the body must
/// end by, and whether it has passed that end, after which nothing more is copied. `at` is
/// where the body has reached in `out`; with `write` false the body is only measured, nothing
/// copied into `out` and nothing hashed.
struct Enc<'a> {
    out: &'a mut Vec<u8>,
    key: &'a RedactionKey,
    redacted: u64,
    end: usize,
    over: bool,
    at: usize,
    write: bool,
}

impl Enc<'_> {
    /// The bytes left before the limit.
    fn room(&self) -> usize {
        self.end.saturating_sub(self.at)
    }

    /// Every byte the encoder writes goes through here.
    fn put(&mut self, v: &[u8]) {
        self.over = self.over || self.at.saturating_add(v.len()) > self.end;
        if !self.over {
            if self.write {
                self.out.extend_from_slice(v);
            }
            self.at += v.len();
        }
    }

    /// The keyed hash of `chunks`, concatenated; a measuring pass hashes nothing and counts
    /// the hash's length.
    fn hash<'c>(&mut self, chunks: impl IntoIterator<Item = &'c [u8]>) {
        if self.write {
            let digest = self.key.digest_chunks(chunks);
            self.put(&digest.0);
        } else {
            self.put(&[0; DIGEST_LEN]);
        }
    }

    fn u8(&mut self, v: u8) {
        self.put(&[v]);
    }

    fn u16(&mut self, v: u16) {
        self.put(&v.to_le_bytes());
    }

    fn u32(&mut self, v: u32) {
        self.put(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.put(&v.to_le_bytes());
    }

    fn i64(&mut self, v: i64) {
        self.put(&v.to_le_bytes());
    }

    fn bytes(&mut self, v: &[u8]) -> Result<(), JournalError> {
        self.u32(len32(v.len())?);
        self.put(v);
        self.within()
    }

    /// Fails once the body has passed its limit, so a loop over a record's elements stops
    /// there rather than walking the rest.
    fn within(&self) -> Result<(), JournalError> {
        if self.over {
            return Err(JournalError::TooLarge);
        }
        Ok(())
    }

    /// Counts `n` more redacted bytes against [`MAX_REDACTED`].
    fn redact(&mut self, n: u64) -> Result<(), JournalError> {
        self.redacted += n;
        if self.redacted > MAX_REDACTED {
            return Err(JournalError::TooLarge);
        }
        Ok(())
    }

    fn conn(&mut self, c: ConnKey) {
        self.u16(c.conn);
        self.u32(c.epoch);
    }

    fn rpc(&mut self, rpc: Option<RpcId>) {
        match rpc {
            None => self.u8(0),
            Some(id) => {
                self.u8(1);
                self.u64(id.0);
            }
        }
    }

    fn stamp(&mut self, s: &Stamp) {
        self.u64(s.ingest_seq);
        match s.kernel_rx {
            None => self.u8(0),
            Some(k) => {
                self.u8(1);
                self.i64(k.0);
            }
        }
        self.u64(s.recv_mono.0);
        self.i64(s.recv_wall.0);
        self.conn(s.conn);
    }

    /// Content with redaction spans: lengths, each span with its keyed hash, then only the
    /// bytes outside the spans.
    fn spanned(&mut self, bytes: &[u8], spans: &[Range<u32>]) -> Result<(), JournalError> {
        self.u32(len32(bytes.len())?);
        self.u32(len32(spans.len())?);
        // Descriptors that cannot fit stop the record before the spans are walked. Then the
        // spans are counted, stopping at the first one past MAX_REDACTED, so a record that
        // redacts too much writes no descriptor and hashes no span.
        if spans.len().saturating_mul(SPAN_DESCRIPTOR) > self.room() {
            self.over = true;
            return Err(JournalError::TooLarge);
        }
        for span in spans {
            self.redact(u64::from(span.end - span.start))?;
        }
        for span in spans {
            self.u32(span.start);
            self.u32(span.end);
            self.hash([&bytes[span.start as usize..span.end as usize]]);
            self.within()?;
        }
        let mut at = 0;
        for span in spans {
            self.put(&bytes[at..span.start as usize]);
            self.within()?;
            at = span.end as usize;
        }
        self.put(&bytes[at..]);
        self.within()
    }

    /// Inbound bytes with the spans a codec named in them, written as [`Enc::spanned`] writes
    /// a [`WireSlice`]. The spans are checked first ([`check_redactions`]), since a record
    /// built field by field may hold any; spans on bytes that cannot fit are refused for
    /// their size, unscanned.
    fn inbound_spanned(&mut self, bytes: &[u8], spans: &[Range<u32>]) -> Result<(), JournalError> {
        if !spans.is_empty() {
            if bytes.len() > self.room() {
                self.over = true;
                return Err(JournalError::TooLarge);
            }
            if check_redactions(bytes, spans).is_err() {
                return Err(JournalError::Unencodable("inbound redaction spans"));
            }
        }
        self.spanned(bytes, spans)
    }

    /// A secret header's name or value: its length and its keyed hash. The length and the
    /// digest must fit before the value is counted or hashed (Codex r4179310271).
    fn hashed(&mut self, v: Value<'_>) -> Result<(), JournalError> {
        if 4 + DIGEST_LEN > self.room() {
            self.over = true;
            return Err(JournalError::TooLarge);
        }
        let len = v.len();
        self.redact(len as u64)?;
        self.u32(len32(len)?);
        self.hash(v.chunks());
        self.within()
    }

    /// Headers as (name, value, what of it is secret): each a flag byte (0 plain, 1 its value
    /// secret, 2 its name and value secret), its name and its value, a secret one written as
    /// its length and its keyed hash.
    fn headers<'h>(
        &mut self,
        headers: impl ExactSizeIterator<Item = (&'h str, Value<'h>, Hide)>,
    ) -> Result<(), JournalError> {
        self.u32(len32(headers.len())?);
        for (name, value, hide) in headers {
            match hide {
                Hide::NameAndValue => {
                    self.u8(2);
                    self.hashed(Value::Text(name))?;
                    self.hashed(value)?;
                }
                Hide::Value => {
                    self.u8(1);
                    self.bytes(name.as_bytes())?;
                    self.hashed(value)?;
                }
                Hide::Nothing => {
                    self.u8(0);
                    self.bytes(name.as_bytes())?;
                    let len = value.len();
                    self.u32(len32(len)?);
                    for chunk in value.chunks() {
                        self.put(chunk);
                    }
                    self.within()?;
                }
            }
        }
        Ok(())
    }

    fn subs(&mut self, subs: &[Subscription]) -> Result<(), JournalError> {
        self.u32(len32(subs.len())?);
        for s in subs {
            self.u32(s.inst.get());
            match s.feed {
                Feed::Touch(TouchSourceId(id)) => {
                    self.u8(0);
                    self.u8(id);
                }
                Feed::Book(BookId(id)) => {
                    self.u8(1);
                    self.u8(id);
                }
                Feed::Trades => self.u8(2),
                Feed::Mark => self.u8(3),
                Feed::Index => self.u8(4),
                Feed::Funding => self.u8(5),
                Feed::Stats => self.u8(6),
            }
            self.within()?;
        }
        Ok(())
    }
}

/// What in a record body could not be read; the reader names the segment.
pub(crate) type Bad = &'static str;

/// Reads one record body of the current [`VERSION`]: [`decode_version`].
#[cfg(test)]
pub(crate) fn decode(body: &[u8]) -> Result<(Record, Vec<SpanDigest>), Bad> {
    decode_version(body, VERSION)
}

/// Reads one record body of a segment in format `version` (one this crate reads), which must
/// be consumed exactly: the record with its spans blanked, and the keyed hashes written in
/// their place, in the order [`Record::digests`] gives. A kind newer than `version` is
/// malformed.
pub(crate) fn decode_version(body: &[u8], version: u16) -> Result<(Record, Vec<SpanDigest>), Bad> {
    let mut d = Dec {
        buf: body,
        at: 0,
        blanked: 0,
        digests: Vec::new(),
        version,
    };
    let kind = d.u8()?;
    if kind >= NONCE && version < 3 {
        return Err("record kind");
    }
    let record = match kind {
        INBOUND => {
            let stamp = d.stamp()?;
            let opcode = d.pick(&OPCODES, "opcode")?;
            let (bytes, redact) = d.inbound()?;
            // Spans on character boundaries keep a blanked text frame UTF-8.
            if opcode == Opcode::Text && core::str::from_utf8(&bytes).is_err() {
                return Err("text frame");
            }
            Record::Inbound {
                stamp,
                opcode,
                bytes: Opaque(bytes),
                redact,
            }
        }
        OUTBOUND => Record::Outbound {
            at: MonoNs(d.u64()?),
            conn: d.conn()?,
            rpc: d.rpc()?,
            frame: {
                let (bytes, spans) = d.spanned()?;
                WireSlice::redacted(bytes, spans).map_err(|_| "redaction spans")?
            },
        },
        WRITE_RESULT => Record::WriteResult {
            at: MonoNs(d.u64()?),
            conn: d.conn()?,
            rpc: d.rpc()?,
            result: match d.u8()? {
                0 => WriteRes::Written,
                1 => WriteRes::NotSent(d.pick(&NOT_SENT, "write result")?),
                _ => return Err("write result"),
            },
        },
        HTTP_REQUEST => Record::HttpRequest {
            at: MonoNs(d.u64()?),
            conn: d.conn()?,
            tag: HttpTag(d.u64()?),
            rpc: d.rpc()?,
            req: HttpRequestRec {
                method: d.pick(&METHODS, "method")?,
                url: {
                    let (bytes, spans) = d.spanned()?;
                    let text = String::from_utf8(bytes).map_err(|_| "url text")?;
                    WireUrl::redacted(text, spans).map_err(|_| "redaction spans")?
                },
                headers: d.headers()?,
                body: {
                    let (bytes, spans) = d.spanned()?;
                    WireSlice::redacted(bytes, spans).map_err(|_| "redaction spans")?
                },
            },
        },
        HTTP_RESULT => Record::HttpResult {
            stamp: d.stamp()?,
            tag: HttpTag(d.u64()?),
            result: match d.u8()? {
                0 => {
                    let (status, headers) = (d.u16()?, d.headers()?);
                    let (body, body_redact) = d.inbound()?;
                    Ok(HttpResponseRec {
                        status,
                        headers,
                        body: Opaque(body),
                        body_redact,
                    })
                }
                1 => Err(d.pick(&FAILURES, "http failure")?),
                _ => return Err("http result"),
            },
        },
        TIMER => Record::Timer {
            stamp: d.stamp()?,
            tag: TimerTag(d.u64()?),
        },
        CONTROL => Record::Control {
            at: MonoNs(d.u64()?),
            ev: match d.u8()? {
                0 => ControlEvent::Opened(d.conn()?),
                1 => ControlEvent::Closed(d.conn()?),
                2 => ControlEvent::Subscribe {
                    conn: d.conn()?,
                    add: d.subs()?,
                    remove: d.subs()?,
                },
                _ => return Err("control event"),
            },
        },
        MARKER => Record::Marker(match d.u8()? {
            0 => Marker::SessionStart {
                header: Opaque(d.bytes()?.to_vec()),
            },
            1 => Marker::Degraded {
                from_seq: d.u64()?,
                dropped: d.u64()?,
            },
            2 => Marker::Recovered,
            _ => return Err("marker"),
        }),
        NONCE => Record::Nonce {
            source: NonceSourceId(d.u32()?),
            value: d.u64()?,
        },
        ENCODE_CTX => Record::EncodeCtx {
            rpc: d.rpc()?,
            ctx: EncodeCtx {
                wall: WallNs(d.i64()?),
                mono: MonoNs(d.u64()?),
                nonces: NonceBlock::new(d.nonces()?),
            },
        },
        CYCLE => Record::Cycle {
            last_ingest_seq: d.u64()?,
            instruments: d.instruments()?,
        },
        _ => return Err("record kind"),
    };
    if d.at != body.len() {
        return Err("bytes after the record");
    }
    Ok((record, d.digests))
}

struct Dec<'a> {
    buf: &'a [u8],
    at: usize,
    /// Blank bytes put in so far.
    blanked: u64,
    /// The keyed hashes read so far, in record order.
    digests: Vec<SpanDigest>,
    /// The segment's format version.
    version: u16,
}

impl<'a> Dec<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Bad> {
        let end = self.at.checked_add(n).filter(|&end| end <= self.buf.len());
        let end = end.ok_or("record ends early")?;
        let out = &self.buf[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Bad> {
        Ok(self.take(N)?.try_into().expect("took N bytes"))
    }

    fn u8(&mut self) -> Result<u8, Bad> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, Bad> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, Bad> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, Bad> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, Bad> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    /// Counts `n` more blank bytes against [`MAX_REDACTED`], before they are allocated.
    fn blank(&mut self, n: u64) -> Result<(), Bad> {
        self.blanked += n;
        if self.blanked > MAX_REDACTED {
            return Err("redacted length");
        }
        Ok(())
    }

    fn pick<T: Copy>(&mut self, table: &[T], what: Bad) -> Result<T, Bad> {
        table.get(usize::from(self.u8()?)).copied().ok_or(what)
    }

    fn flag(&mut self, what: Bad) -> Result<bool, Bad> {
        self.pick(&[false, true], what)
    }

    fn bytes(&mut self) -> Result<&'a [u8], Bad> {
        let len = self.u32()?;
        self.take(len as usize)
    }

    fn text(&mut self) -> Result<String, Bad> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| "text")
    }

    fn conn(&mut self) -> Result<ConnKey, Bad> {
        Ok(ConnKey {
            conn: self.u16()?,
            epoch: self.u32()?,
        })
    }

    fn rpc(&mut self) -> Result<Option<RpcId>, Bad> {
        Ok(match self.flag("rpc")? {
            false => None,
            true => Some(RpcId(self.u64()?)),
        })
    }

    fn stamp(&mut self) -> Result<Stamp, Bad> {
        Ok(Stamp {
            ingest_seq: self.u64()?,
            kernel_rx: match self.flag("kernel_rx")? {
                false => None,
                true => Some(KernelRxNs(self.i64()?)),
            },
            recv_mono: MonoNs(self.u64()?),
            recv_wall: WallNs(self.i64()?),
            conn: self.conn()?,
        })
    }

    /// A span's keyed hash.
    fn digest(&mut self) -> Result<(), Bad> {
        let digest = SpanDigest(self.array()?);
        self.digests.push(digest);
        Ok(())
    }

    /// Content written with [`Enc::spanned`], each span filled with [`BLANK`] and its hash
    /// kept.
    fn spanned(&mut self) -> Result<(Vec<u8>, Vec<Range<u32>>), Bad> {
        let len = self.u32()?;
        let count = self.u32()?;
        let mut spans = Vec::new();
        let mut out = Vec::new();
        let mut at = 0;
        for _ in 0..count {
            let span = self.u32()?..self.u32()?;
            if span.start < at || span.start >= span.end || span.end > len {
                return Err("redaction spans");
            }
            // Counted before any blank is allocated.
            self.blank(u64::from(span.end - span.start))?;
            self.digest()?;
            spans.push(span.clone());
            at = span.end;
        }
        let mut at = 0;
        for span in &spans {
            out.extend_from_slice(self.take((span.start - at) as usize)?);
            out.resize(span.end as usize, BLANK);
            at = span.end;
        }
        out.extend_from_slice(self.take((len - at) as usize)?);
        Ok((out, spans))
    }

    /// Inbound bytes: in version 4 written with their spans ([`Dec::spanned`]), before it
    /// whole, with none.
    fn inbound(&mut self) -> Result<(Vec<u8>, Vec<Range<u32>>), Bad> {
        if self.version >= 4 {
            self.spanned()
        } else {
            Ok((self.bytes()?.to_vec(), Vec::new()))
        }
    }

    /// A secret header's name or value, written with [`Enc::hashed`]: blanks at its length.
    fn hashed(&mut self) -> Result<String, Bad> {
        let len = self.u32()?;
        self.blank(u64::from(len))?;
        self.digest()?;
        Ok(String::from_utf8(vec![BLANK; len as usize]).expect("BLANK is ASCII"))
    }

    fn headers(&mut self) -> Result<Vec<HeaderRec>, Bad> {
        let count = self.u32()?;
        let mut headers = Vec::new();
        for _ in 0..count {
            // Version 4 puts the flag first and may hash the name; before it, the name came
            // first and only a value was secret.
            let (flag, name) = if self.version >= 4 {
                match self.pick(&[0u8, 1, 2], "header flag")? {
                    2 => (2, self.hashed()?),
                    flag => (flag, self.text()?),
                }
            } else {
                let name = self.text()?;
                (u8::from(self.flag("header flag")?), name)
            };
            let value = if flag > 0 {
                self.hashed()?
            } else {
                self.text()?
            };
            headers.push(HeaderRec {
                name,
                value,
                redact: flag > 0,
                redact_name: flag == 2,
            });
        }
        Ok(headers)
    }

    fn nonces(&mut self) -> Result<Vec<u64>, Bad> {
        let count = self.u32()?;
        let mut nonces = Vec::new();
        for _ in 0..count {
            nonces.push(self.u64()?);
        }
        Ok(nonces)
    }

    fn instruments(&mut self) -> Result<Vec<InstrumentId>, Bad> {
        let count = self.u32()?;
        let mut instruments = Vec::new();
        for _ in 0..count {
            instruments.push(InstrumentId::new(self.u32()?));
        }
        Ok(instruments)
    }

    fn subs(&mut self) -> Result<Vec<Subscription>, Bad> {
        let count = self.u32()?;
        let mut subs = Vec::new();
        for _ in 0..count {
            let inst = InstrumentId::new(self.u32()?);
            let feed = match self.u8()? {
                0 => Feed::Touch(TouchSourceId(self.u8()?)),
                1 => Feed::Book(BookId(self.u8()?)),
                2 => Feed::Trades,
                3 => Feed::Mark,
                4 => Feed::Index,
                5 => Feed::Funding,
                6 => Feed::Stats,
                _ => return Err("feed"),
            };
            subs.push(Subscription { inst, feed });
        }
        Ok(subs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::ResponseRef;
    use fbc_core::HttpRequest;

    fn round_trip(record: &Record) -> Record {
        let mut body = Vec::new();
        encode(record, &key(), &mut body).unwrap();
        decode(&body).unwrap().0
    }

    fn key() -> RedactionKey {
        RedactionKey::new(&[3; 32]).unwrap()
    }

    fn conn() -> ConnKey {
        ConnKey { conn: 1, epoch: 2 }
    }

    fn stamp() -> Stamp {
        Stamp {
            ingest_seq: 5,
            kernel_rx: None,
            recv_mono: MonoNs(6),
            recv_wall: WallNs(7),
            conn: conn(),
        }
    }

    #[test]
    fn a_record_over_the_limit_is_refused_before_its_payload_is_copied() {
        // Codex r4176799800: the sink must not copy a payload it is about to drop.
        let big = vec![b'a'; 1 << 20];
        let records = [
            Record::Inbound {
                stamp: stamp(),
                opcode: Opcode::Text,
                bytes: Opaque(big.clone()),
                redact: Vec::new(),
            },
            Record::Outbound {
                at: MonoNs(1),
                conn: conn(),
                rpc: None,
                frame: WireSlice::redacted(big.clone(), vec![0..1, 2..3]).unwrap(),
            },
            Record::HttpResult {
                stamp: stamp(),
                tag: HttpTag(1),
                result: Ok(HttpResponseRec {
                    status: 200,
                    headers: Vec::new(),
                    body: Opaque(big.clone()),
                    body_redact: Vec::new(),
                }),
            },
        ];
        for record in &records {
            let mut body = Vec::new();
            assert!(matches!(
                encode_within(record, &key(), &mut body, 256),
                Err(JournalError::TooLarge)
            ));
            assert!(body.capacity() <= 512, "{}", body.capacity());
            // Within a limit it fits, it encodes as without one.
            let mut unlimited = Vec::new();
            encode(record, &key(), &mut unlimited).unwrap();
            let mut limited = Vec::new();
            encode_within(record, &key(), &mut limited, unlimited.len()).unwrap();
            assert_eq!(limited, unlimited);
        }
        // Codex r4176868162: a text frame that fits but is not UTF-8 is refused before its
        // payload is copied.
        let mut body = Vec::new();
        let not_utf8 = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(vec![0xff; 1 << 20]),
            redact: Vec::new(),
        };
        assert!(matches!(
            encode(&not_utf8, &key(), &mut body),
            Err(JournalError::Unencodable(_))
        ));
        assert!(body.capacity() < 1 << 10, "{}", body.capacity());
        // A text frame too large to copy is refused for its size, before its UTF-8 is checked.
        let not_utf8 = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(vec![0xff; 1 << 20]),
            redact: Vec::new(),
        };
        assert!(matches!(
            encode_within(&not_utf8, &key(), &mut Vec::new(), 256),
            Err(JournalError::TooLarge)
        ));
    }

    #[test]
    fn a_text_frame_must_be_utf8() {
        let frame = |bytes: &[u8]| Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(bytes.to_vec()),
            redact: Vec::new(),
        };
        assert_eq!(round_trip(&frame(b"ok")), frame(b"ok"));
        let mut body = Vec::new();
        assert!(matches!(
            encode(&frame(&[0xff]), &key(), &mut body),
            Err(JournalError::Unencodable("a text frame that is not UTF-8"))
        ));
        // A damaged segment that labels other bytes as text.
        encode(&frame(b"x"), &key(), &mut body).unwrap();
        *body.last_mut().unwrap() = 0xff;
        assert_eq!(decode(&body), Err("text frame"));
        // Binary frames are any bytes.
        let binary = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Binary,
            bytes: Opaque(vec![0xff]),
            redact: Vec::new(),
        };
        assert_eq!(round_trip(&binary), binary);
    }

    #[test]
    fn inbound_spans_are_hashed_and_bytes_that_cannot_fit_are_refused_unscanned() {
        let frame = |opcode, bytes: &[u8], redact| Record::Inbound {
            stamp: stamp(),
            opcode,
            bytes: Opaque(bytes.to_vec()),
            redact,
        };
        // A text frame's span on whole characters reads back blanked and still UTF-8.
        let text = frame(Opcode::Text, "kéy=é|".as_bytes(), vec![1..3, 5..7]);
        let mut body = Vec::new();
        encode(&text, &key(), &mut body).unwrap();
        let (back, digests) = decode(&body).unwrap();
        assert_eq!(back, text.blanked());
        assert_eq!(digests, text.digests(&key()));
        assert_eq!(digests, [key().digest("é".as_bytes()); 2]);
        // A frame with spans that cannot fit is refused for its size before its spans are
        // checked (these would be refused as damaged), and leaves the body as it was.
        let big = frame(
            Opcode::Binary,
            &[1; 512],
            std::iter::once(600..700).collect(),
        );
        let mut body = vec![9];
        let refused = encode_within(&big, &key(), &mut body, 256);
        assert!(matches!(refused, Err(JournalError::TooLarge)));
        assert_eq!(body, [9]);
        // A text frame whose blanked bytes are not UTF-8 is refused by the reader.
        let mut body = Vec::new();
        encode(
            &frame(Opcode::Text, b"ab", std::iter::once(0..1).collect()),
            &key(),
            &mut body,
        )
        .unwrap();
        let at = body.len() - 1;
        body[at] = 0xff;
        assert_eq!(decode(&body), Err("text frame"));
    }

    #[test]
    fn every_table_value_has_its_own_byte_and_round_trips() {
        for (i, v) in OPCODES.iter().enumerate() {
            assert_eq!(usize::from(opcode_byte(*v)), i);
        }
        for (i, v) in METHODS.iter().enumerate() {
            assert_eq!(usize::from(method_byte(*v)), i);
        }
        for (i, v) in FAILURES.iter().enumerate() {
            assert_eq!(usize::from(failure_byte(*v)), i);
            let r = Record::HttpResult {
                stamp: stamp(),
                tag: HttpTag(1),
                result: Err(*v),
            };
            assert_eq!(round_trip(&r), r);
        }
        for (i, v) in NOT_SENT.iter().enumerate() {
            assert_eq!(usize::from(not_sent_byte(*v)), i);
            let r = Record::WriteResult {
                at: MonoNs(1),
                conn: conn(),
                rpc: None,
                result: WriteRes::NotSent(*v),
            };
            assert_eq!(round_trip(&r), r);
        }
    }

    #[test]
    fn every_feed_round_trips() {
        let feeds = [
            Feed::Touch(TouchSourceId(3)),
            Feed::Book(BookId(2)),
            Feed::Trades,
            Feed::Mark,
            Feed::Index,
            Feed::Funding,
            Feed::Stats,
        ];
        let subs: Vec<Subscription> = feeds
            .iter()
            .map(|&feed| Subscription {
                inst: InstrumentId::new(9),
                feed,
            })
            .collect();
        let r = Record::Control {
            at: MonoNs(1),
            ev: ControlEvent::Subscribe {
                conn: conn(),
                add: subs,
                remove: Vec::new(),
            },
        };
        assert_eq!(round_trip(&r), r);
    }

    fn good_outbound() -> Vec<u8> {
        let frame = WireSlice::redacted(b"abcdef".to_vec(), vec![1..2, 3..5]).unwrap();
        let mut body = Vec::new();
        let r = Record::Outbound {
            at: MonoNs(1),
            conn: conn(),
            rpc: Some(RpcId(4)),
            frame,
        };
        encode(&r, &key(), &mut body).unwrap();
        body
    }

    /// Bytes of an Outbound record up to its span list: kind, at, conn, rpc.
    const OUTBOUND_HEAD: usize = 1 + 8 + 6 + 1 + 8;

    /// An Outbound body with these content fields after its head.
    fn outbound_with(tail: &[u8]) -> Vec<u8> {
        let mut body = good_outbound()[..OUTBOUND_HEAD].to_vec();
        body.extend_from_slice(tail);
        body
    }

    fn words(ws: &[u32]) -> Vec<u8> {
        ws.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    #[test]
    fn a_damaged_body_is_refused_with_what_was_wrong() {
        let good = good_outbound();
        assert!(decode(&good).is_ok());
        let cases: Vec<(Vec<u8>, Bad)> = vec![
            (vec![], "record ends early"),
            (vec![99], "record kind"),
            (good[..good.len() - 1].to_vec(), "record ends early"),
            ([good.clone(), vec![0]].concat(), "bytes after the record"),
            // An rpc flag that is neither 0 nor 1.
            (
                {
                    let mut b = good.clone();
                    b[15] = 2;
                    b
                },
                "rpc",
            ),
            // Spans: reversed, empty, past the end, out of order.
            (outbound_with(&words(&[6, 1, 3, 1])), "redaction spans"),
            (outbound_with(&words(&[6, 1, 2, 2])), "redaction spans"),
            (outbound_with(&words(&[6, 1, 5, 7])), "redaction spans"),
            (
                outbound_with(&[&words(&[6, 2, 3, 4])[..], &[0; 32], &words(&[1, 2])].concat()),
                "redaction spans",
            ),
            // A write result, an HTTP result, a control event and a marker of no known tag.
            (
                [&[WRITE_RESULT][..], &[0; 8], &[0; 6], &[0], &[7]].concat(),
                "write result",
            ),
            (
                [&[WRITE_RESULT][..], &[0; 8], &[0; 6], &[0], &[1, 9]].concat(),
                "write result",
            ),
            (
                [&[HTTP_RESULT][..], &[0; 31], &[0; 8], &[2]].concat(),
                "http result",
            ),
            (
                [&[HTTP_RESULT][..], &[0; 31], &[0; 8], &[1, 3]].concat(),
                "http failure",
            ),
            ([&[CONTROL][..], &[0; 8], &[3]].concat(), "control event"),
            (
                [
                    &[CONTROL][..],
                    &[0; 8],
                    &[2],
                    &[0; 6],
                    &words(&[1, 0]),
                    &[7],
                ]
                .concat(),
                "feed",
            ),
            ([MARKER, 3].to_vec(), "marker"),
            // An inbound frame with an unknown opcode or kernel_rx flag.
            (
                [
                    &[INBOUND][..],
                    &[0; 8],
                    &[0],
                    &[0; 8],
                    &[0; 8],
                    &[0; 6],
                    &[2],
                ]
                .concat(),
                "opcode",
            ),
            ([&[INBOUND][..], &[0; 8], &[5]].concat(), "kernel_rx"),
        ];
        for (body, want) in cases {
            assert_eq!(decode(&body), Err(want), "{body:?}");
        }
    }

    #[test]
    fn a_record_redacts_at_most_max_redacted_bytes() {
        let outbound = |len: u64| Record::Outbound {
            at: MonoNs(1),
            conn: conn(),
            rpc: None,
            frame: WireSlice::redacted(vec![7; len as usize + 2], vec![0..1, 2..len as u32 + 1])
                .unwrap(),
        };
        let at_limit = outbound(MAX_REDACTED);
        assert_eq!(round_trip(&at_limit), at_limit.blanked());
        let mut body = Vec::new();
        assert!(matches!(
            encode(&outbound(MAX_REDACTED + 1), &key(), &mut body),
            Err(JournalError::TooLarge)
        ));
        // Spans and secret header values count together.
        let mut headers = vec![HeaderRec {
            name: "Cookie".into(),
            value: "c".repeat(MAX_REDACTED as usize),
            redact: false,
            redact_name: false,
        }];
        let result = |headers: Vec<HeaderRec>| Record::HttpResult {
            stamp: stamp(),
            tag: HttpTag(1),
            result: Ok(HttpResponseRec {
                status: 200,
                headers,
                body: Opaque(Vec::new()),
                body_redact: Vec::new(),
            }),
        };
        assert_eq!(
            round_trip(&result(headers.clone())),
            result(headers.clone()).blanked()
        );
        headers.push(HeaderRec {
            name: "X-Key".into(),
            value: "k".into(),
            redact: true,
            redact_name: false,
        });
        assert!(matches!(
            encode(&result(headers), &key(), &mut body),
            Err(JournalError::TooLarge)
        ));
    }

    fn encode_ctx(nonces: Vec<u64>) -> Record {
        Record::EncodeCtx {
            rpc: Some(RpcId(3)),
            ctx: EncodeCtx {
                wall: WallNs(-5),
                mono: MonoNs(6),
                nonces: NonceBlock::new(nonces),
            },
        }
    }

    fn cycle(n: u32) -> Record {
        Record::Cycle {
            last_ingest_seq: 9,
            instruments: (0..n).map(InstrumentId::new).collect(),
        }
    }

    #[test]
    fn the_version_3_kinds_round_trip_and_are_refused_in_a_version_2_body() {
        let nonce = Record::Nonce {
            source: NonceSourceId(2),
            value: 7,
        };
        for record in [nonce, encode_ctx(vec![1, 2]), cycle(3)] {
            assert_eq!(round_trip(&record), record);
            let mut body = Vec::new();
            encode(&record, &key(), &mut body).unwrap();
            assert_eq!(decode_version(&body, 3).unwrap().0, record);
            assert_eq!(decode_version(&body, 2), Err("record kind"));
        }
        // The version 2 kinds read the same in either version.
        let body = good_outbound();
        assert_eq!(decode_version(&body, 2), decode_version(&body, 3));
    }

    #[test]
    fn a_damaged_version_3_body_is_refused_with_what_was_wrong() {
        let mut ctx = Vec::new();
        encode(&encode_ctx(vec![1, 2]), &key(), &mut ctx).unwrap();
        let mut cyc = Vec::new();
        encode(&cycle(2), &key(), &mut cyc).unwrap();
        let cases: Vec<(Vec<u8>, Bad)> = vec![
            ([NONCE, 0, 0, 0, 0].to_vec(), "record ends early"),
            // An rpc flag that is neither 0 nor 1.
            ([&[ENCODE_CTX][..], &[2]].concat(), "rpc"),
            // A nonce or an instrument cut short, and a count past the body.
            (ctx[..ctx.len() - 1].to_vec(), "record ends early"),
            (cyc[..cyc.len() - 1].to_vec(), "record ends early"),
            (
                [&[CYCLE][..], &[0; 8], &words(&[u32::MAX]), &[0; 4]].concat(),
                "record ends early",
            ),
            ([ctx.clone(), vec![0]].concat(), "bytes after the record"),
        ];
        for (body, want) in cases {
            assert_eq!(decode(&body), Err(want), "{body:?}");
        }
    }

    #[test]
    fn a_long_nonce_or_instrument_list_stops_at_the_limit() {
        for record in [encode_ctx((0..4_096).collect()), cycle(4_096)] {
            let mut body = Vec::new();
            assert!(matches!(
                encode_within(&record, &key(), &mut body, 64),
                Err(JournalError::TooLarge)
            ));
            assert!(body.is_empty());
            let mut unlimited = Vec::new();
            encode(&record, &key(), &mut unlimited).unwrap();
            assert_eq!(decode(&unlimited).unwrap().0, record);
        }
    }

    #[test]
    fn a_damaged_redacted_length_is_refused_before_it_is_allocated() {
        // A span over the whole of a u32::MAX-byte frame, in a record of a few bytes.
        let huge = outbound_with(&words(&[u32::MAX, 1, 0, u32::MAX]));
        assert_eq!(decode(&huge), Err("redacted length"));
        // A secret response header claiming a u32::MAX-byte value: in version 4 its flag
        // comes first, and a secret name may claim it too; before, the name came first.
        let head = [&[HTTP_RESULT][..], &[0; 31], &[0; 8], &[0, 200, 0]].concat();
        let result_with = |tail: &[u8]| [&head[..], &words(&[1]), tail].concat();
        let value = result_with(&[&[1][..], &words(&[1]), b"a", &words(&[u32::MAX])].concat());
        assert_eq!(decode(&value), Err("redacted length"));
        let name = result_with(&[&[2][..], &words(&[u32::MAX])].concat());
        assert_eq!(decode(&name), Err("redacted length"));
        let old = result_with(&[&words(&[1]), &b"a"[..], &[1], &words(&[u32::MAX])].concat());
        assert_eq!(decode_version(&old, 3), Err("redacted length"));
        // A version 4 inbound frame or response body is spanned: a span over u32::MAX bytes.
        let span = words(&[u32::MAX, 1, 0, u32::MAX]);
        let frame = [&[INBOUND][..], &[0; 31], &[0], &span].concat();
        assert_eq!(decode(&frame), Err("redacted length"));
        let body = [&head[..], &words(&[0]), &span].concat();
        assert_eq!(decode(&body), Err("redacted length"));
    }

    /// An HTTP request body from its method on.
    fn request_with(tail: &[u8]) -> Vec<u8> {
        [&[HTTP_REQUEST][..], &[0; 8], &[0; 6], &[0; 8], &[0], tail].concat()
    }

    #[test]
    fn a_damaged_request_is_refused() {
        // A URL that is not UTF-8, then a span that splits a character of it.
        let not_utf8 = [&[0][..], &words(&[1, 0]), &[0xff]].concat();
        assert_eq!(decode(&request_with(&not_utf8)), Err("url text"));
        let split = [&[0][..], &words(&[2, 1, 1, 2]), &[0; 32], &[0xc3]].concat();
        assert_eq!(decode(&request_with(&split)), Err("url text"));
        // An unknown method.
        assert_eq!(decode(&request_with(&[9])), Err("method"));
        // A header name that is not UTF-8, and a header flag that is not 0, 1 or 2 (version
        // 4, flag first) or not 0 or 1 (before, after the name).
        let url = [&[0][..], &words(&[0, 0])].concat();
        let bad_name = [&url[..], &words(&[1]), &[0], &words(&[1]), &[0xff]].concat();
        assert_eq!(decode(&request_with(&bad_name)), Err("text"));
        let bad_flag = [&url[..], &words(&[1]), &[3]].concat();
        assert_eq!(decode(&request_with(&bad_flag)), Err("header flag"));
        let old_name = [&url[..], &words(&[1, 1]), &[0xff]].concat();
        assert_eq!(decode_version(&request_with(&old_name), 3), Err("text"));
        let old_flag = [&url[..], &words(&[1, 1]), b"a", &[2]].concat();
        assert_eq!(
            decode_version(&request_with(&old_flag), 3),
            Err("header flag")
        );
    }

    /// Codex r4179465515: a record that cannot fit its limit is refused before any of its
    /// redactions is hashed, however much of it would fit before the field that does not: a
    /// request whose redacted URL fits but whose body does not hashes nothing.
    #[test]
    fn a_record_that_cannot_fit_is_refused_before_any_redaction_is_hashed() {
        let url = "k".repeat(4096);
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::redacted(url, vec![0..2048, 2048..4096]).unwrap(),
            headers: vec![Header {
                name: "Authorization",
                value: "token".into(),
                redact: false,
            }],
            body: WireSlice::plain(vec![b'b'; 1 << 16]),
        };
        let view = RecordRef::HttpRequest {
            at: MonoNs(1),
            conn: conn(),
            tag: HttpTag(1),
            rpc: None,
            req: &req,
        };
        let before = crate::redact::digests_taken();
        let mut out = Vec::new();
        assert!(matches!(
            encode_within(view, &key(), &mut out, 1024),
            Err(JournalError::TooLarge)
        ));
        assert_eq!(
            crate::redact::digests_taken(),
            before,
            "hashed a refused record"
        );
        assert!(out.is_empty());
        encode_within(view, &key(), &mut out, usize::MAX).unwrap();
        assert_eq!(crate::redact::digests_taken(), before + 3);
    }

    /// Codex r4179310271: a secret header whose length and digest cannot fit after its flag
    /// and name is refused before its value is counted or hashed, so a record bound to be
    /// dropped costs no scan of a long credential; a secret name likewise.
    #[test]
    fn a_secret_header_with_no_room_for_its_digest_is_refused_unhashed() {
        let k = key();
        let value = "s".repeat(4096);
        // Room for the count, the flag, the name's length and the name, and the value's length
        // and digest but one byte.
        let mut out = Vec::new();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 4 + 1 + 4 + "cookie".len() + 4 + DIGEST_LEN - 1,
            over: false,
            at: 0,
            write: true,
        };
        let headers = [("cookie", Value::Text(&value), Hide::Value)].into_iter();
        assert!(matches!(e.headers(headers), Err(JournalError::TooLarge)));
        assert_eq!(e.redacted, 0, "counted before its digest was known to fit");
        assert_eq!(e.out.len(), 4 + 1 + 4 + "cookie".len());

        // A secret name: room for the count, the flag and the name's length and digest but one
        // byte.
        let mut out = Vec::new();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 4 + 1 + 4 + DIGEST_LEN - 1,
            over: false,
            at: 0,
            write: true,
        };
        let headers = [("x-echo-k", Value::Text(&value), Hide::NameAndValue)].into_iter();
        assert!(matches!(e.headers(headers), Err(JournalError::TooLarge)));
        assert_eq!(e.redacted, 0, "counted before its digest was known to fit");
        assert_eq!(e.out.len(), 4 + 1);
    }

    #[test]
    fn encoding_stops_at_the_limit_instead_of_walking_the_rest() {
        // Codex r4176902248: past the limit, no later element is written. A secret header
        // after the limit would count redacted bytes if it were visited.
        let mut out = Vec::new();
        let k = key();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 16,
            over: false,
            at: 0,
            write: true,
        };
        let headers = [
            HeaderRec {
                name: "x-long".into(),
                value: "v".repeat(64),
                redact: false,
                redact_name: false,
            },
            HeaderRec {
                name: "x-secret".into(),
                value: "s".repeat(10),
                redact: true,
                redact_name: false,
            },
        ];
        assert!(matches!(
            e.headers(rec_headers(&headers)),
            Err(JournalError::TooLarge)
        ));
        assert_eq!(e.redacted, 0);

        let mut out = Vec::new();
        let k = key();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 16,
            over: false,
            at: 0,
            write: true,
        };
        let spans: Vec<Range<u32>> = (0..64).map(|i| i * 2..i * 2 + 1).collect();
        assert!(matches!(
            e.spanned(&[b'a'; 128], &spans),
            Err(JournalError::TooLarge)
        ));
        // Descriptors that cannot fit stop it before the spans are counted.
        assert_eq!(e.redacted, 0);
        assert!(e.out.len() <= 16);

        let mut out = Vec::new();
        let k = key();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 16,
            over: false,
            at: 0,
            write: true,
        };
        let subs: Vec<Subscription> = (1..=64)
            .map(|i| Subscription {
                inst: InstrumentId::new(i),
                feed: Feed::Trades,
            })
            .collect();
        assert!(matches!(e.subs(&subs), Err(JournalError::TooLarge)));
        assert!(e.out.len() <= 16);
    }

    #[test]
    fn a_text_frame_that_cannot_fit_with_its_fields_is_not_scanned() {
        // Codex r4176932550: the payload alone fits the limit, the record with its fixed
        // fields does not; it is refused for its size, not after a UTF-8 scan.
        let mut payload = vec![b'a'; 63];
        payload.push(0xff);
        let record = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(payload),
            redact: Vec::new(),
        };
        assert!(matches!(
            encode_within(&record, &key(), &mut Vec::new(), 64),
            Err(JournalError::TooLarge)
        ));
        let mut unlimited = Vec::new();
        assert!(matches!(
            encode(&record, &key(), &mut unlimited),
            Err(JournalError::Unencodable(_))
        ));
    }

    #[test]
    fn no_limit_lets_a_body_past_the_format_s_u32_length() {
        // Codex r4176932553: a queue with more than 4 GiB of room must still refuse at
        // encoding a body the format cannot frame.
        assert_eq!(body_end(0, usize::MAX), u32::MAX as usize);
        assert_eq!(body_end(10, usize::MAX), 10 + u32::MAX as usize);
        assert_eq!(body_end(10, 64), 74);
    }

    #[test]
    fn too_much_redaction_is_refused_before_any_span_is_written() {
        // Codex r4176960480: spans redacting more than MAX_REDACTED in all are refused before
        // their descriptors are written, however much room the limit leaves.
        let mut out = Vec::new();
        let k = key();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: usize::MAX,
            over: false,
            at: 0,
            write: true,
        };
        let span = MAX_REDACTED as u32 / 4;
        let bytes = vec![b'a'; span as usize * 8];
        let spans: Vec<Range<u32>> = (0..8).map(|i| i * span..(i + 1) * span).collect();
        assert!(matches!(
            e.spanned(&bytes, &spans),
            Err(JournalError::TooLarge)
        ));
        // Only the two lengths: no span descriptor.
        assert_eq!(e.out.len(), 8);
    }

    #[test]
    fn span_descriptors_that_cannot_fit_are_refused_before_the_spans_are_walked() {
        // Codex r4176986652: with no room for the descriptors, the spans are not counted.
        let mut out = Vec::new();
        let k = key();
        let mut e = Enc {
            out: &mut out,
            key: &k,
            redacted: 0,
            end: 64,
            over: false,
            at: 0,
            write: true,
        };
        let spans: Vec<Range<u32>> = (0..64).map(|i| i * 2..i * 2 + 1).collect();
        assert!(matches!(
            e.spanned(&[b'a'; 128], &spans),
            Err(JournalError::TooLarge)
        ));
        assert_eq!(e.redacted, 0);
        assert!(e.out.len() <= 64);
    }

    /// A small deterministic generator (xorshift64*), so the property test below needs no
    /// dependency and every run checks the same cases.
    struct Gen(u64);

    impl Gen {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn flip(&mut self) -> bool {
            self.below(2) == 0
        }

        /// A length: empty, small, or large (past what a small sink holds).
        fn len(&mut self) -> usize {
            match self.below(8) {
                0 => 0,
                1 => 70_000 + self.below(4096),
                _ => self.below(64),
            }
        }

        fn bytes(&mut self, n: usize) -> Vec<u8> {
            (0..n).map(|_| self.next() as u8).collect()
        }

        fn any_bytes(&mut self) -> Vec<u8> {
            let n = self.len();
            self.bytes(n)
        }

        fn any_text(&mut self) -> String {
            let n = self.len();
            self.text(n)
        }

        fn ascii(&mut self, n: usize) -> String {
            (0..n)
                .map(|_| char::from(b'a' + self.below(26) as u8))
                .collect()
        }

        /// Random bytes read lossily, as the runtime reads a header value: invalid sequences
        /// become U+FFFD, valid multi-byte ones stay.
        fn text(&mut self, n: usize) -> String {
            String::from_utf8_lossy(&self.bytes(n)).into_owned()
        }

        /// Ordered, disjoint, non-empty spans within `len` bytes.
        fn spans(&mut self, len: usize) -> Vec<Range<u32>> {
            let mut spans = Vec::new();
            let mut at = 0;
            while at < len && self.below(3) != 0 {
                // Below `len`, as `at` is.
                let start = at + self.below((len - at).min(16));
                let end = start + 1 + self.below((len - start).min(40));
                spans.push(start as u32..end as u32);
                at = end;
            }
            spans
        }

        fn slice(&mut self) -> WireSlice {
            let bytes = self.any_bytes();
            let spans = self.spans(bytes.len());
            WireSlice::redacted(bytes, spans).unwrap()
        }

        fn stamp(&mut self) -> Stamp {
            Stamp {
                ingest_seq: self.next(),
                kernel_rx: self.flip().then(|| KernelRxNs(self.next() as i64)),
                recv_mono: MonoNs(self.next()),
                recv_wall: WallNs(self.next() as i64),
                conn: conn(),
            }
        }

        fn rpc(&mut self) -> Option<RpcId> {
            self.flip().then(|| RpcId(self.next()))
        }

        fn subs(&mut self) -> Vec<Subscription> {
            let feeds = [
                Feed::Touch(TouchSourceId(3)),
                Feed::Book(BookId(2)),
                Feed::Trades,
                Feed::Mark,
                Feed::Index,
                Feed::Funding,
                Feed::Stats,
            ];
            (0..self.below(5))
                .map(|_| Subscription {
                    inst: InstrumentId::new(self.next() as u32),
                    feed: feeds[self.below(feeds.len())],
                })
                .collect()
        }

        fn request(&mut self) -> HttpRequest {
            const NAMES: [&str; 5] = ["Authorization", "cookie", "x-sig", "accept", "X-Api-Key"];
            let url = {
                let n = self.len();
                self.ascii(n)
            };
            let spans = self.spans(url.len());
            HttpRequest {
                method: METHODS[self.below(METHODS.len())],
                url: WireUrl::redacted(url, spans).unwrap(),
                headers: (0..self.below(4))
                    .map(|_| Header {
                        name: NAMES[self.below(NAMES.len())],
                        value: self.any_text(),
                        redact: self.flip(),
                    })
                    .collect(),
                body: self.slice(),
            }
        }

        /// A response's headers as the transport hands them on, raw (often not UTF-8), secret
        /// ones by name among them.
        fn response_headers(&mut self) -> Vec<(&'static str, Vec<u8>)> {
            const NAMES: [&str; 4] = ["set-cookie", "Cookie", "content-type", "x-raw"];
            (0..self.below(4))
                .map(|_| (NAMES[self.below(NAMES.len())], self.any_bytes()))
                .collect()
        }

        /// One owned record of a kind no [`RecordRef`] borrows.
        fn owned(&mut self) -> Record {
            match self.below(9) {
                0 => Record::Outbound {
                    at: MonoNs(self.next()),
                    conn: conn(),
                    rpc: self.rpc(),
                    frame: self.slice(),
                },
                1 => Record::WriteResult {
                    at: MonoNs(self.next()),
                    conn: conn(),
                    rpc: self.rpc(),
                    result: if self.flip() {
                        WriteRes::Written
                    } else {
                        WriteRes::NotSent(NOT_SENT[self.below(NOT_SENT.len())])
                    },
                },
                2 => Record::Timer {
                    stamp: self.stamp(),
                    tag: TimerTag(self.next()),
                },
                3 => Record::Control {
                    at: MonoNs(self.next()),
                    ev: if self.flip() {
                        ControlEvent::Opened(conn())
                    } else {
                        ControlEvent::Closed(conn())
                    },
                },
                4 => Record::Marker(match self.below(3) {
                    0 => Marker::SessionStart {
                        header: Opaque(self.any_bytes()),
                    },
                    1 => Marker::Degraded {
                        from_seq: self.next(),
                        dropped: self.next(),
                    },
                    _ => Marker::Recovered,
                }),
                5 => Record::Nonce {
                    source: NonceSourceId(self.next() as u32),
                    value: self.next(),
                },
                6 => encode_ctx((0..self.below(6)).map(|_| self.next()).collect()),
                7 => cycle(self.below(6) as u32),
                _ => Record::HttpResult {
                    stamp: self.stamp(),
                    tag: HttpTag(self.next()),
                    result: Err(FAILURES[self.below(FAILURES.len())]),
                },
            }
        }
    }

    /// Checks the one property every offer relies on for one record: the borrowed record
    /// encodes byte for byte as the owned record it stands for, reads back as that record
    /// blanked, and a sink's room for it is checked at exactly that length: it fits a limit of
    /// its encoded length and is refused, leaving the buffer as it was, one byte short of it.
    fn check(view: RecordRef<'_>) {
        let owned = view.to_record();
        let mut borrowed = Vec::new();
        encode_within(view, &key(), &mut borrowed, usize::MAX).unwrap();
        let mut built = Vec::new();
        encode(&owned, &key(), &mut built).unwrap();
        assert_eq!(borrowed, built, "{owned:?}");
        assert_eq!(decode(&borrowed).unwrap().0, owned.blanked());
        let mut exact = vec![7];
        encode_within(view, &key(), &mut exact, borrowed.len()).unwrap();
        assert_eq!(exact[1..], borrowed[..]);
        let mut short = vec![7];
        assert!(matches!(
            encode_within(view, &key(), &mut short, borrowed.len() - 1),
            Err(JournalError::TooLarge)
        ));
        assert_eq!(short, [7]);
    }

    /// FBC-f3w: the size a record is offered to a sink at is the size it encodes to, for every
    /// kind, borrowed or owned: with redacted spans, secret and plain headers (secret by name
    /// or by mark), lossily read header values, binary frames that are not UTF-8, and empty and
    /// large payloads. A borrowed record has no size of its own to drift from its encoding.
    #[test]
    fn every_record_is_offered_at_exactly_the_length_it_encodes_to() {
        let mut g = Gen(0x9e37_79b9_7f4a_7c15);
        for _ in 0..400 {
            let stamp = g.stamp();
            let (text, binary) = (g.any_text(), g.any_bytes());
            check(RecordRef::Inbound {
                stamp,
                frame: RawFrame::Text(&text),
            });
            check(RecordRef::Inbound {
                stamp,
                frame: RawFrame::Binary(&binary),
            });
            let req = g.request();
            check(RecordRef::HttpRequest {
                at: MonoNs(g.next()),
                conn: conn(),
                tag: HttpTag(g.next()),
                rpc: g.rpc(),
                req: &req,
            });
            let headers = g.response_headers();
            let pairs: Vec<(&str, &[u8])> = headers.iter().map(|(n, v)| (*n, &v[..])).collect();
            let body = g.any_bytes();
            let result = if g.below(4) == 0 {
                Err(FAILURES[g.below(FAILURES.len())])
            } else {
                Ok(ResponseRef {
                    status: g.next() as u16,
                    headers: &pairs,
                    body: &body,
                })
            };
            check(RecordRef::HttpResult {
                stamp,
                tag: HttpTag(g.next()),
                result,
            });
            let (add, remove) = (g.subs(), g.subs());
            check(RecordRef::Subscribe {
                at: MonoNs(g.next()),
                conn: conn(),
                add: &add,
                remove: &remove,
            });
            let owned = g.owned();
            check(RecordRef::from(&owned));
        }
    }

    /// FBC-f3w: a borrowed record the format refuses is refused as the owned record it stands
    /// for is, for the same reason.
    #[test]
    fn a_borrowed_record_is_refused_as_its_owned_record_is() {
        let over = MAX_REDACTED as usize + 1;
        let req = HttpRequest {
            method: HttpMethod::Post,
            url: WireUrl::redacted("https://toy/x".into(), Vec::new()).unwrap(),
            headers: Vec::new(),
            body: WireSlice::redacted(
                vec![b'k'; over + 1],
                vec![0..over as u32, over as u32..over as u32 + 1],
            )
            .unwrap(),
        };
        let secret = vec![b's'; over];
        let pairs = [("Set-Cookie", &secret[..])];
        let views = [
            RecordRef::HttpRequest {
                at: MonoNs(1),
                conn: conn(),
                tag: HttpTag(1),
                rpc: None,
                req: &req,
            },
            RecordRef::HttpResult {
                stamp: stamp(),
                tag: HttpTag(1),
                result: Ok(ResponseRef {
                    status: 200,
                    headers: &pairs,
                    body: b"",
                }),
            },
        ];
        for view in views {
            let mut out = Vec::new();
            assert!(matches!(
                encode_within(view, &key(), &mut out, usize::MAX),
                Err(JournalError::TooLarge)
            ));
            assert!(out.is_empty());
            assert!(matches!(
                encode(&view.to_record(), &key(), &mut out),
                Err(JournalError::TooLarge)
            ));
        }
    }
}
