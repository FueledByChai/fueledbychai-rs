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
//! [`BLANK`] where the spans were.

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
    let mut e = Enc(out);
    match record {
        Record::Inbound {
            stamp,
            opcode,
            bytes,
        } => {
            e.u8(INBOUND);
            e.stamp(stamp);
            e.u8(opcode_byte(*opcode));
            e.bytes(&bytes.0)?;
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
        Record::HttpResult {
            at,
            conn,
            tag,
            result,
        } => {
            e.u8(HTTP_RESULT);
            e.u64(at.0);
            e.conn(*conn);
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
        Record::Timer { fired, conn, tag } => {
            e.u8(TIMER);
            e.u64(fired.0);
            e.conn(*conn);
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
    Ok(())
}

struct Enc<'a>(&'a mut Vec<u8>);

impl Enc<'_> {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }

    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }

    fn bytes(&mut self, v: &[u8]) -> Result<(), JournalError> {
        self.u32(len32(v.len())?);
        self.0.extend_from_slice(v);
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
        for span in spans {
            self.u32(span.start);
            self.u32(span.end);
        }
        let mut at = 0;
        for span in spans {
            self.0.extend_from_slice(&bytes[at..span.start as usize]);
            at = span.end as usize;
        }
        self.0.extend_from_slice(&bytes[at..]);
        Ok(())
    }

    /// Headers: a secret one's value is written as its length alone.
    fn headers(&mut self, headers: &[HeaderRec]) -> Result<(), JournalError> {
        self.u32(len32(headers.len())?);
        for h in headers {
            self.bytes(h.name.as_bytes())?;
            if h.secret() {
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
        }
        Ok(())
    }
}

/// What in a record body could not be read; the reader names the segment.
pub(crate) type Bad = &'static str;

/// Reads one record body, which must be consumed exactly.
pub(crate) fn decode(body: &[u8]) -> Result<Record, Bad> {
    let mut d = Dec { buf: body, at: 0 };
    let record = match d.u8()? {
        INBOUND => Record::Inbound {
            stamp: d.stamp()?,
            opcode: d.pick(&OPCODES, "opcode")?,
            bytes: Opaque(d.bytes()?.to_vec()),
        },
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
            at: MonoNs(d.u64()?),
            conn: d.conn()?,
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
            fired: MonoNs(d.u64()?),
            conn: d.conn()?,
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
                let len = self.u32()? as usize;
                String::from_utf8(vec![BLANK; len]).expect("BLANK is ASCII")
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
                at: MonoNs(1),
                conn: conn(),
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
                [&[HTTP_RESULT][..], &[0; 8], &[0; 6], &[0; 8], &[2]].concat(),
                "http result",
            ),
            (
                [&[HTTP_RESULT][..], &[0; 8], &[0; 6], &[0; 8], &[1, 3]].concat(),
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
}
