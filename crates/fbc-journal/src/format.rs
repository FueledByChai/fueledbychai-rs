//! The on-disk format, hand-written little-endian with no serialization dependency.
//!
//! A segment is [`MAGIC`], the format [`VERSION`] (`u16`), then records. A record is its body's
//! length (`u32`) and its body: a kind byte and the kind's fields in order. Integers are
//! little-endian; an `Option` is a byte (0 none, 1 some) and the value; a byte string or text
//! is its length (`u32`) and its bytes; an enum is a tag byte and its fields.
//!
//! Redacted content is never written. A [`WireSlice`] or [`WireUrl`] is its total length, its
//! span count, each span's start and end (`u32`), and then only the bytes outside the spans;
//! a secret header is its name, a flag byte, and its value's length alone. The reader puts
//! [`BLANK`] where the spans were. A record redacts at most [`MAX_REDACTED`] bytes, so a
//! damaged length cannot make the reader allocate more than that for blanks.

use core::ops::Range;

use fbc_core::{
    BookId, ConnKey, Feed, HttpFailure, HttpMethod, HttpTag, InstrumentId, KernelRxNs, MonoNs,
    NotSentReason, RpcId, Stamp, Subscription, TimerTag, TouchSourceId, WallNs, WireSlice, WireUrl,
};

use crate::JournalError;
use crate::record::{
    BLANK, ControlEvent, HeaderRec, HttpRequestRec, HttpResponseRec, Marker, Opaque, Opcode,
    Record, WriteRes,
};

/// The first bytes of every segment.
pub const MAGIC: [u8; 4] = *b"FBCJ";
/// The format version this crate writes and reads.
pub const VERSION: u16 = 1;
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

/// Appends a record's body (kind and fields) to `out`.
pub(crate) fn encode(record: &Record, out: &mut Vec<u8>) -> Result<(), JournalError> {
    encode_within(record, out, usize::MAX)
}

/// [`encode`], refusing a body longer than `limit` bytes with [`JournalError::TooLarge`]
/// before copying the bytes that would pass it, so a record too large for the room left
/// costs no copy of its payload. A refused record leaves `out` as it was.
pub(crate) fn encode_within(
    record: &Record,
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<(), JournalError> {
    let start = out.len();
    let encoded = encode_body(record, out, limit);
    // A refused record leaves `out` as it found it.
    if encoded.is_err() {
        out.truncate(start);
    }
    encoded
}

fn encode_body(record: &Record, out: &mut Vec<u8>, limit: usize) -> Result<(), JournalError> {
    let mut e = Enc {
        end: body_end(out.len(), limit),
        out,
        redacted: 0,
        over: false,
    };
    match record {
        Record::Inbound {
            stamp,
            opcode,
            bytes,
        } => {
            e.u8(INBOUND);
            e.stamp(stamp);
            e.u8(opcode_byte(*opcode));
            e.u32(len32(bytes.0.len())?);
            // Checked before the payload is copied, and only for a payload that fits after
            // the fields before it: one that cannot is refused for its size, unscanned.
            if *opcode == Opcode::Text
                && bytes.0.len() <= e.room()
                && core::str::from_utf8(&bytes.0).is_err()
            {
                return Err(JournalError::Unencodable("a text frame that is not UTF-8"));
            }
            e.put(&bytes.0);
            e.within()?;
        }
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
            e.u8(HTTP_REQUEST);
            e.u64(at.0);
            e.conn(*conn);
            e.u64(tag.0);
            e.rpc(*rpc);
            e.u8(method_byte(req.method));
            e.spanned(req.url.as_str().as_bytes(), req.url.redactions())?;
            e.headers(&req.headers)?;
            e.spanned(req.body.bytes(), req.body.redactions())?;
        }
        Record::HttpResult { stamp, tag, result } => {
            e.u8(HTTP_RESULT);
            e.stamp(stamp);
            e.u64(tag.0);
            match result {
                Ok(resp) => {
                    e.u8(0);
                    e.u16(resp.status);
                    e.headers(&resp.headers)?;
                    e.bytes(&resp.body.0)?;
                }
                Err(failure) => {
                    e.u8(1);
                    e.u8(failure_byte(*failure));
                }
            }
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
                    e.u8(2);
                    e.conn(*conn);
                    e.subs(add)?;
                    e.subs(remove)?;
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
    }
    if e.over {
        return Err(JournalError::TooLarge);
    }
    Ok(())
}

/// Where a body that starts at `start` in its buffer must end: within `limit` bytes, and never
/// past the format's `u32` body length.
fn body_end(start: usize, limit: usize) -> usize {
    start.saturating_add(limit.min(u32::MAX as usize))
}

/// The output, the bytes redacted so far, where the body must end by, and whether it has
/// passed that end, after which nothing more is copied.
struct Enc<'a> {
    out: &'a mut Vec<u8>,
    redacted: u64,
    end: usize,
    over: bool,
}

impl Enc<'_> {
    /// The bytes left before the limit.
    fn room(&self) -> usize {
        self.end.saturating_sub(self.out.len())
    }

    /// Every byte the encoder writes goes through here.
    fn put(&mut self, v: &[u8]) {
        self.over = self.over || self.out.len().saturating_add(v.len()) > self.end;
        if !self.over {
            self.out.extend_from_slice(v);
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

    /// Content with redaction spans: lengths and spans, then only the bytes outside them.
    fn spanned(&mut self, bytes: &[u8], spans: &[Range<u32>]) -> Result<(), JournalError> {
        self.u32(len32(bytes.len())?);
        self.u32(len32(spans.len())?);
        // Counted first, stopping at the first span past MAX_REDACTED, so a record that
        // redacts too much writes no descriptor.
        for span in spans {
            self.redact(u64::from(span.end - span.start))?;
        }
        for span in spans {
            self.u32(span.start);
            self.u32(span.end);
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

    /// Headers: a secret one's value is written as its length alone.
    fn headers(&mut self, headers: &[HeaderRec]) -> Result<(), JournalError> {
        self.u32(len32(headers.len())?);
        for h in headers {
            self.bytes(h.name.as_bytes())?;
            if h.secret() {
                self.redact(h.value.len() as u64)?;
                self.u8(1);
                self.u32(len32(h.value.len())?);
            } else {
                self.u8(0);
                self.bytes(h.value.as_bytes())?;
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

/// Reads one record body, which must be consumed exactly.
pub(crate) fn decode(body: &[u8]) -> Result<Record, Bad> {
    let mut d = Dec {
        buf: body,
        at: 0,
        blanked: 0,
    };
    let record = match d.u8()? {
        INBOUND => {
            let stamp = d.stamp()?;
            let opcode = d.pick(&OPCODES, "opcode")?;
            let bytes = d.bytes()?;
            if opcode == Opcode::Text && core::str::from_utf8(bytes).is_err() {
                return Err("text frame");
            }
            Record::Inbound {
                stamp,
                opcode,
                bytes: Opaque(bytes.to_vec()),
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
                0 => Ok(HttpResponseRec {
                    status: d.u16()?,
                    headers: d.headers()?,
                    body: Opaque(d.bytes()?.to_vec()),
                }),
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
        _ => return Err("record kind"),
    };
    if d.at != body.len() {
        return Err("bytes after the record");
    }
    Ok(record)
}

struct Dec<'a> {
    buf: &'a [u8],
    at: usize,
    /// Blank bytes put in so far.
    blanked: u64,
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

    /// Content written with [`Enc::spanned`], each span filled with [`BLANK`].
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
            spans.push(span.clone());
            at = span.end;
        }
        let mut at = 0;
        for span in &spans {
            out.extend_from_slice(self.take((span.start - at) as usize)?);
            self.blank(u64::from(span.end - span.start))?;
            out.resize(span.end as usize, BLANK);
            at = span.end;
        }
        out.extend_from_slice(self.take((len - at) as usize)?);
        Ok((out, spans))
    }

    fn headers(&mut self) -> Result<Vec<HeaderRec>, Bad> {
        let count = self.u32()?;
        let mut headers = Vec::new();
        for _ in 0..count {
            let name = self.text()?;
            let redact = self.flag("header flag")?;
            let value = if redact {
                let len = self.u32()?;
                self.blank(u64::from(len))?;
                String::from_utf8(vec![BLANK; len as usize]).expect("BLANK is ASCII")
            } else {
                self.text()?
            };
            headers.push(HeaderRec {
                name,
                value,
                redact,
            });
        }
        Ok(headers)
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

    fn round_trip(record: &Record) -> Record {
        let mut body = Vec::new();
        encode(record, &mut body).unwrap();
        decode(&body).unwrap()
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
                }),
            },
        ];
        for record in &records {
            let mut body = Vec::new();
            assert!(matches!(
                encode_within(record, &mut body, 256),
                Err(JournalError::TooLarge)
            ));
            assert!(body.capacity() <= 512, "{}", body.capacity());
            // Within a limit it fits, it encodes as without one.
            let mut unlimited = Vec::new();
            encode(record, &mut unlimited).unwrap();
            let mut limited = Vec::new();
            encode_within(record, &mut limited, unlimited.len()).unwrap();
            assert_eq!(limited, unlimited);
        }
        // Codex r4176868162: a text frame that fits but is not UTF-8 is refused before its
        // payload is copied.
        let mut body = Vec::new();
        let not_utf8 = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(vec![0xff; 1 << 20]),
        };
        assert!(matches!(
            encode(&not_utf8, &mut body),
            Err(JournalError::Unencodable(_))
        ));
        assert!(body.capacity() < 1 << 10, "{}", body.capacity());
        // A text frame too large to copy is refused for its size, before its UTF-8 is checked.
        let not_utf8 = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(vec![0xff; 1 << 20]),
        };
        assert!(matches!(
            encode_within(&not_utf8, &mut Vec::new(), 256),
            Err(JournalError::TooLarge)
        ));
    }

    #[test]
    fn a_text_frame_must_be_utf8() {
        let frame = |bytes: &[u8]| Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Text,
            bytes: Opaque(bytes.to_vec()),
        };
        assert_eq!(round_trip(&frame(b"ok")), frame(b"ok"));
        let mut body = Vec::new();
        assert!(matches!(
            encode(&frame(&[0xff]), &mut body),
            Err(JournalError::Unencodable("a text frame that is not UTF-8"))
        ));
        // A damaged segment that labels other bytes as text.
        encode(&frame(b"x"), &mut body).unwrap();
        *body.last_mut().unwrap() = 0xff;
        assert_eq!(decode(&body), Err("text frame"));
        // Binary frames are any bytes.
        let binary = Record::Inbound {
            stamp: stamp(),
            opcode: Opcode::Binary,
            bytes: Opaque(vec![0xff]),
        };
        assert_eq!(round_trip(&binary), binary);
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
        encode(&r, &mut body).unwrap();
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
                outbound_with(&words(&[6, 2, 3, 4, 1, 2])),
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
            encode(&outbound(MAX_REDACTED + 1), &mut body),
            Err(JournalError::TooLarge)
        ));
        // Spans and secret header values count together.
        let mut headers = vec![HeaderRec {
            name: "Cookie".into(),
            value: "c".repeat(MAX_REDACTED as usize),
            redact: false,
        }];
        let result = |headers: Vec<HeaderRec>| Record::HttpResult {
            stamp: stamp(),
            tag: HttpTag(1),
            result: Ok(HttpResponseRec {
                status: 200,
                headers,
                body: Opaque(Vec::new()),
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
        });
        assert!(matches!(
            encode(&result(headers), &mut body),
            Err(JournalError::TooLarge)
        ));
    }

    #[test]
    fn a_damaged_redacted_length_is_refused_before_it_is_allocated() {
        // A span over the whole of a u32::MAX-byte frame, in a record of a few bytes.
        let huge = outbound_with(&words(&[u32::MAX, 1, 0, u32::MAX]));
        assert_eq!(decode(&huge), Err("redacted length"));
        // A secret response header claiming a u32::MAX-byte value.
        let header = [
            &[HTTP_RESULT][..],
            &[0; 31], // a stamp with no kernel time
            &[0; 8],
            &[0, 200, 0],
            &words(&[1, 1]),
            b"a",
            &[1],
            &words(&[u32::MAX]),
        ]
        .concat();
        assert_eq!(decode(&header), Err("redacted length"));
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
        let split = [&[0][..], &words(&[2, 1, 1, 2]), &[0xc3]].concat();
        assert_eq!(decode(&request_with(&split)), Err("url text"));
        // An unknown method.
        assert_eq!(decode(&request_with(&[9])), Err("method"));
        // A header name that is not UTF-8, and a header flag that is neither 0 nor 1.
        let url = [&[0][..], &words(&[0, 0])].concat();
        let bad_name = [&url[..], &words(&[1, 1]), &[0xff]].concat();
        assert_eq!(decode(&request_with(&bad_name)), Err("text"));
        let bad_flag = [&url[..], &words(&[1, 1]), b"a", &[2]].concat();
        assert_eq!(decode(&request_with(&bad_flag)), Err("header flag"));
    }

    #[test]
    fn encoding_stops_at_the_limit_instead_of_walking_the_rest() {
        // Codex r4176902248: past the limit, no later element is written. A secret header
        // after the limit would count redacted bytes if it were visited.
        let mut out = Vec::new();
        let mut e = Enc {
            out: &mut out,
            redacted: 0,
            end: 16,
            over: false,
        };
        let headers = [
            HeaderRec {
                name: "x-long".into(),
                value: "v".repeat(64),
                redact: false,
            },
            HeaderRec {
                name: "x-secret".into(),
                value: "s".repeat(10),
                redact: true,
            },
        ];
        assert!(matches!(e.headers(&headers), Err(JournalError::TooLarge)));
        assert_eq!(e.redacted, 0);

        let mut out = Vec::new();
        let mut e = Enc {
            out: &mut out,
            redacted: 0,
            end: 16,
            over: false,
        };
        let spans: Vec<Range<u32>> = (0..64).map(|i| i * 2..i * 2 + 1).collect();
        assert!(matches!(
            e.spanned(&[b'a'; 128], &spans),
            Err(JournalError::TooLarge)
        ));
        // The spans are counted against MAX_REDACTED first (no copy); their descriptors stop
        // at the limit.
        assert_eq!(e.redacted, 64);
        assert!(e.out.len() <= 16);

        let mut out = Vec::new();
        let mut e = Enc {
            out: &mut out,
            redacted: 0,
            end: 16,
            over: false,
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
        };
        assert!(matches!(
            encode_within(&record, &mut Vec::new(), 64),
            Err(JournalError::TooLarge)
        ));
        let mut unlimited = Vec::new();
        assert!(matches!(
            encode(&record, &mut unlimited),
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
        let mut e = Enc {
            out: &mut out,
            redacted: 0,
            end: usize::MAX,
            over: false,
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
}
