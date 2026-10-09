//! FBC-ec9's done line: Nonce, EncodeCtx and Cycle records, written interleaved with the other
//! kinds, read back equal and in write order; and a journal written before these kinds existed
//! (format version 2, `fixtures/journal/v2`) still reads back unchanged (0006, 0014 item 1).
//! So does a journal written in format version 3 (`fixtures/journal/v3`), before inbound
//! redaction spans (FBC-7lm, decision 0028), and one in format version 4
//! (`fixtures/journal/v4`), before outbound frames kept the kind they were sent as (FBC-q7b),
//! and one in format version 5 (`fixtures/journal/v5`), before a write result could be not sent
//! for a stale authorization (FBC-j5bw, decision 0062), and one in format version 6
//! (`fixtures/journal/v6`), before a request deadline's firing was a record (FBC-0hfl).
//!
//! Every secret here is synthetic, and each is assembled at run time so no credential-shaped
//! literal sits in the source.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{
    ConnKey, EncodeCtx, Feed, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag,
    InboundSpans, InstrumentId, KernelRxNs, MonoNs, NonceBlock, NotSentReason, RawFrame, RpcId,
    Stamp, Subscription, TimerTag, WallNs, WireSlice, WireUrl,
};
use fbc_journal::format::{MAGIC, VERSION};
use fbc_journal::{
    CloseRec, ControlEvent, Entry, HttpRequestRec, HttpResponseRec, JournalError, JournalReader,
    JournalWriter, Marker, NonceSourceId, Opaque, Opcode, Record, RedactionKey, WriteRes,
    WsControl,
};

const SEC: i64 = 1_000_000_000;
/// 2026-10-03T12:00:00Z and an hour later: the version 2 fixture's two segments.
const NOON: WallNs = WallNs(1_791_028_800 * SEC);
const ONE_PM: WallNs = WallNs((1_791_028_800 + 3_600) * SEC);

/// The key the version 2 fixture's spans were hashed under.
fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

/// A synthetic secret, distinctive enough that finding it in a file means it leaked.
fn secret(what: &str) -> String {
    format!("SYNTH{}{}SECRET", what.to_uppercase(), "-v2k")
}

fn span_of(hay: &[u8], needle: &str) -> std::ops::Range<u32> {
    let at = hay
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())
        .unwrap();
    at as u32..(at + needle.len()) as u32
}

fn conn() -> ConnKey {
    ConnKey { conn: 2, epoch: 1 }
}

fn stamp(ingest_seq: u64, wall: WallNs) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: Some(KernelRxNs(wall.0 - 3)),
        recv_mono: MonoNs(ingest_seq * 10),
        recv_wall: wall,
        conn: conn(),
    }
}

/// One record of every kind format version 2 has, with the wall time each was filed under:
/// what `fixtures/journal/v2` holds (its README says how it was written).
fn v2_records() -> Vec<(WallNs, Record)> {
    let token = secret("frame-token");
    let frame = format!("{{\"op\":\"auth\",\"token\":\"{token}\",\"sig\":\"0x51\"}}");
    let frame_span = span_of(frame.as_bytes(), &token);
    let query_key = secret("query-key");
    let url = format!("https://api.example.test/v1/orders?key={query_key}");
    let url_span = span_of(url.as_bytes(), &query_key);
    let body_key = secret("body-key");
    let body = format!("{{\"market\":\"BTC-USD-PERP\",\"key\":\"{body_key}\"}}");
    let body_span = span_of(body.as_bytes(), &body_key);
    let req = HttpRequest {
        method: HttpMethod::Post,
        url: WireUrl::redacted(url, vec![url_span]).unwrap(),
        headers: vec![
            Header {
                name: "Authorization",
                value: format!("Bearer {}", secret("bearer")),
                redact: false,
            },
            Header {
                name: "X-Api-Key",
                value: secret("api-key"),
                redact: true,
            },
            Header {
                name: "Content-Type",
                value: "application/json".into(),
                redact: false,
            },
        ],
        body: WireSlice::redacted(body.into_bytes(), vec![body_span]).unwrap(),
    };
    let set_cookie = format!("cdn={}", secret("set-cookie"));
    let resp_headers = [
        ("Set-Cookie", set_cookie.as_str()),
        ("X-Request-Id", "req-9"),
    ];
    let resp = HttpResponse {
        status: 201,
        headers: &resp_headers,
        body: b"{\"status\":\"NEW\"}",
    };
    let text = "{\"channel\":\"bbo\",\"px\":\"101.5\"}";
    vec![
        (
            NOON,
            Record::Marker(Marker::SessionStart {
                header: Opaque(b"consumer header".to_vec()),
            }),
        ),
        (
            NOON,
            Record::Control {
                at: MonoNs(1),
                ev: ControlEvent::Opened(conn()),
            },
        ),
        (
            NOON,
            Record::Control {
                at: MonoNs(2),
                ev: ControlEvent::Subscribe {
                    conn: conn(),
                    add: vec![Subscription {
                        inst: InstrumentId::new(7),
                        feed: Feed::Trades,
                    }],
                    remove: Vec::new(),
                },
            },
        ),
        (NOON, Record::inbound(stamp(1, NOON), RawFrame::Text(text))),
        (
            NOON,
            Record::inbound(stamp(2, NOON), RawFrame::Binary(&[0, 1, 0xfe, 0xff])),
        ),
        (
            NOON,
            Record::Outbound {
                at: MonoNs(30),
                conn: conn(),
                rpc: Some(RpcId(5)),
                opcode: Opcode::Text,
                frame: WireSlice::redacted(frame.into_bytes(), vec![frame_span]).unwrap(),
            },
        ),
        (
            NOON,
            Record::WriteResult {
                at: MonoNs(31),
                conn: conn(),
                rpc: Some(RpcId(5)),
                result: WriteRes::Written,
            },
        ),
        // The next hour: the segment above is closed and compressed.
        (
            ONE_PM,
            Record::HttpRequest {
                at: MonoNs(40),
                conn: conn(),
                tag: HttpTag(3),
                rpc: Some(RpcId(6)),
                req: HttpRequestRec::from(&req),
            },
        ),
        (
            ONE_PM,
            Record::HttpResult {
                stamp: stamp(3, ONE_PM),
                tag: HttpTag(3),
                result: Ok(HttpResponseRec::from(&resp)),
            },
        ),
        (
            ONE_PM,
            Record::HttpResult {
                stamp: stamp(4, ONE_PM),
                tag: HttpTag(4),
                result: Err(HttpFailure::Lost),
            },
        ),
        (
            ONE_PM,
            Record::Timer {
                stamp: stamp(5, ONE_PM),
                tag: TimerTag(8),
            },
        ),
        (
            ONE_PM,
            Record::WriteResult {
                at: MonoNs(60),
                conn: conn(),
                rpc: None,
                result: WriteRes::NotSent(NotSentReason::Backpressure),
            },
        ),
        (
            ONE_PM,
            Record::Marker(Marker::Degraded {
                from_seq: 6,
                dropped: 2,
            }),
        ),
        (ONE_PM, Record::Marker(Marker::Recovered)),
        (
            ONE_PM,
            Record::Control {
                at: MonoNs(70),
                ev: ControlEvent::Closed(conn()),
            },
        ),
    ]
}

/// What `fixtures/journal/v3` holds: the version 2 records with one decide cycle's records
/// (its boundary, the nonces reserved, the encode's context and frame) after the second
/// inbound frame, all written by the version 3 writer (its README says how).
fn v3_records() -> Vec<(WallNs, Record)> {
    let mut out = v2_records();
    let at = out
        .iter()
        .position(|(_, r)| matches!(r, Record::Outbound { .. }))
        .unwrap();
    out.splice(at..at, cycle(1, &[500, 501]).into_iter().map(|r| (NOON, r)));
    out
}

/// What `fixtures/journal/v4` holds: the version 3 records with, after the decide cycle's
/// frame, an inbound frame with a credential its codec named, a binary outbound frame whose
/// bytes outside its redaction span are not UTF-8, and one whose only bytes that are not UTF-8
/// lie inside its span, all written by the version 4 writer (its README says how). Version 4
/// kept no outbound frame's kind, so each reads back with the kind its blanked bytes imply
/// (FBC-q7b): the first binary, the second text, with `inside` its kind.
fn v4_records(inside_kind: Opcode) -> Vec<(WallNs, Record)> {
    let mut out = v3_records();
    let at = out
        .iter()
        .position(|(_, r)| matches!(r, Record::WriteResult { .. }))
        .unwrap();
    let echo = secret("echo");
    let text = format!("{{\"channel\":\"fills\",\"echo\":\"{echo}\"}}");
    let spans = InboundSpans::frame(vec![span_of(text.as_bytes(), &echo)]);
    let inbound = Record::inbound_redacted(stamp(3, NOON), RawFrame::Text(&text), &spans).unwrap();
    let mut outside = b"bin|".to_vec();
    outside.extend_from_slice(&[0xc3, 0x28]);
    outside.extend_from_slice(secret("bin-outside").as_bytes());
    let outside_span = (outside.len() - secret("bin-outside").len()) as u32..outside.len() as u32;
    let mut inside = b"bin|".to_vec();
    inside.extend_from_slice(&[0xff, 0xfe]);
    inside.extend_from_slice(secret("bin-inside").as_bytes());
    let inside_span = 4..inside.len() as u32;
    let new = [
        inbound,
        binary_outbound(80, outside, outside_span, Opcode::Binary),
        binary_outbound(81, inside, inside_span, inside_kind),
    ];
    out.splice(at..at, new.into_iter().map(|r| (NOON, r)));
    out
}

/// What `fixtures/journal/v5` holds: the version 4 records, the binary frame whose only bytes
/// that are not UTF-8 lie inside its span kept as binary as version 5 keeps it, with, after the
/// first write result, a ping and a close frame received, each payload a credential, and a
/// write result not sent for the last reason version 5 has, all written by the version 5 writer
/// (its README says how).
fn v5_records() -> Vec<(WallNs, Record)> {
    let mut out = v4_records(Opcode::Binary);
    let at = 1 + out
        .iter()
        .position(|(_, r)| matches!(r, Record::WriteResult { .. }))
        .unwrap();
    let new = [
        Record::InboundControl {
            stamp: stamp(4, NOON),
            frame: WsControl::Ping(Opaque(secret("ping").into_bytes())),
        },
        Record::InboundControl {
            stamp: stamp(5, NOON),
            frame: WsControl::Close(Some(CloseRec {
                code: 1001,
                reason: secret("close"),
            })),
        },
        Record::WriteResult {
            at: MonoNs(90),
            conn: conn(),
            rpc: Some(RpcId(9)),
            result: WriteRes::NotSent(NotSentReason::SignFailed),
        },
    ];
    out.splice(at..at, new.into_iter().map(|r| (NOON, r)));
    out
}

/// What `fixtures/journal/v6` holds: the version 5 records with, after the `SignFailed` write
/// result, a write result not sent for the stale-authorization reason version 6 added, all
/// written by the version 6 writer (its README says how).
fn v6_records() -> Vec<(WallNs, Record)> {
    let mut out = v5_records();
    let at = 1 + out
        .iter()
        .position(|(_, r)| {
            matches!(
                r,
                Record::WriteResult {
                    result: WriteRes::NotSent(NotSentReason::SignFailed),
                    ..
                }
            )
        })
        .unwrap();
    let stale = Record::WriteResult {
        at: MonoNs(91),
        conn: conn(),
        rpc: Some(RpcId(10)),
        result: WriteRes::NotSent(NotSentReason::StaleAuthorization),
    };
    out.insert(at, (NOON, stale));
    out
}

/// A frame written at `at` as `opcode`, with its credential span.
fn binary_outbound(at: u64, bytes: Vec<u8>, span: std::ops::Range<u32>, opcode: Opcode) -> Record {
    Record::Outbound {
        at: MonoNs(at),
        conn: conn(),
        rpc: None,
        opcode,
        frame: WireSlice::redacted(bytes, vec![span]).unwrap(),
    }
}

fn fixture_root(version: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/journal")
        .join(version)
}

/// The format version in a segment's header.
fn version_of(segment: &[u8]) -> u16 {
    assert_eq!(segment[..4], MAGIC);
    u16::from_le_bytes([segment[4], segment[5]])
}

#[test]
fn a_journal_written_before_these_kinds_existed_reads_back_unchanged() {
    reads_back_unchanged("v2", 2, v2_records());
}

#[test]
fn a_journal_written_before_inbound_redaction_spans_reads_back_unchanged() {
    reads_back_unchanged("v3", 3, v3_records());
}

/// FBC-q7b: a journal written before outbound frames kept their kind reads back, each such
/// frame with the kind its blanked bytes imply.
#[test]
fn a_journal_written_before_outbound_opcodes_reads_back_with_the_kind_its_bytes_imply() {
    reads_back_unchanged("v4", 4, v4_records(Opcode::Text));
}

/// FBC-j5bw (DeepSeek's DS-1 on PR #100): a journal written in format version 5, before the
/// stale-authorization reason, reads back unchanged, its received control frames and its last
/// not-sent reason included.
#[test]
fn a_journal_written_before_the_stale_authorization_reason_reads_back_unchanged() {
    reads_back_unchanged("v5", 5, v5_records());
}

/// FBC-0hfl: a journal written in format version 6, before the request deadline firing's kind,
/// reads back unchanged, its stale-authorization write result included.
#[test]
fn a_journal_written_before_the_rpc_timeout_kind_reads_back_unchanged() {
    reads_back_unchanged("v6", 6, v6_records());
}

/// FBC-q7b's done line: a binary frame whose only bytes that are not UTF-8 lie inside its
/// redaction span, which is UTF-8 once blanked, reads back as the binary frame it was sent as.
#[test]
fn a_binary_frame_utf8_once_blanked_reads_back_binary() {
    let root = fresh_dir("outbound_opcode");
    let written = v4_records(Opcode::Binary);
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    for (now, record) in &written {
        writer.append(*now, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let open = fs::read(root.join("20261003/1-000001.fbcj")).unwrap();
    assert_eq!(version_of(&open), VERSION);
    let read: Vec<Record> = JournalReader::open(&root, 1)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let expected: Vec<Record> = written.iter().map(|(_, r)| r.blanked()).collect();
    assert_eq!(read, expected);
    let at_81 = |r: &&Record| matches!(r, Record::Outbound { at: MonoNs(81), .. });
    let Some(Record::Outbound { opcode, frame, .. }) = read.iter().find(at_81) else {
        panic!("{read:?}");
    };
    let sent = written.iter().map(|(_, r)| r).find(at_81);
    let Some(Record::Outbound { frame: sent, .. }) = sent else {
        panic!("{written:?}");
    };
    assert_eq!(*opcode, Opcode::Binary);
    // What was sent was not UTF-8; what reads back is, so only the kept kind says binary.
    assert!(std::str::from_utf8(sent.bytes()).is_err());
    assert!(std::str::from_utf8(frame.bytes()).is_ok());
}

/// The fixture in `fixtures/journal/<dir>`, which the format `version` writer left (a closed,
/// compressed segment and the open one), reads back as `written`, hashes included.
fn reads_back_unchanged(dir: &str, version: u16, written: Vec<(WallNs, Record)>) {
    let root = fixture_root(dir);
    let day = root.join("20261003");
    let closed = zstd::decode_all(&fs::read(day.join("1-000000.fbcj.zst")).unwrap()[..]).unwrap();
    let open = fs::read(day.join("1-000001.fbcj")).unwrap();
    assert_eq!(version_of(&closed), version);
    assert_eq!(version_of(&open), version);
    assert!(VERSION > version);

    let entries: Vec<Entry> = JournalReader::open(&root, 1)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), written.len());
    let key = key();
    for (entry, (_, record)) in entries.iter().zip(&written) {
        assert_eq!(entry.record, record.blanked());
        assert_eq!(entry.digests, record.digests(&key));
    }
    // Some records had spans and secret headers: the comparison covered hashes too.
    assert!(entries.iter().map(|e| e.digests.len()).sum::<usize>() >= 5);
}

const MIDNIGHT: i64 = 1_791_072_000;
const BEFORE_MIDNIGHT: WallNs = WallNs((MIDNIGHT - 1) * SEC);
const AFTER_MIDNIGHT: WallNs = WallNs((MIDNIGHT + 1) * SEC);

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// What one decide cycle journals around an encode: its boundary, the nonces reserved from a
/// source, the context the encode was given, and the encode's frame.
fn cycle(n: u64, nonces: &[u64]) -> Vec<Record> {
    let mut out = vec![Record::Cycle {
        last_ingest_seq: n,
        instruments: vec![InstrumentId::new(7), InstrumentId::new(n as u32 + 100)],
    }];
    out.extend(nonces.iter().map(|&value| Record::Nonce {
        source: NonceSourceId(4),
        value,
    }));
    out.push(Record::EncodeCtx {
        rpc: Some(RpcId(n)),
        ctx: EncodeCtx {
            wall: WallNs(1_791_071_000 * SEC + n as i64),
            mono: MonoNs(n * 1_000),
            nonces: NonceBlock::new(nonces.to_vec()),
        },
    });
    out.push(Record::Outbound {
        at: MonoNs(n * 1_000 + 1),
        conn: conn(),
        rpc: Some(RpcId(n)),
        opcode: Opcode::Text,
        frame: WireSlice::plain(format!("{{\"order\":{n}}}").into_bytes()),
    });
    out
}

#[test]
fn nonce_encode_ctx_and_cycle_records_round_trip_in_write_order_among_the_other_kinds() {
    let root = fresh_dir("cycle_records");
    // The version 2 kinds, with cycles, nonces and contexts between them, across two UTC
    // days (so across a roll into a new day's directory and a compressed closed segment).
    let mut records: Vec<(WallNs, Record)> = Vec::new();
    for (i, (_, r)) in v2_records().into_iter().enumerate() {
        let now = if i < 8 {
            BEFORE_MIDNIGHT
        } else {
            AFTER_MIDNIGHT
        };
        records.push((now, r));
        let new = match i {
            3 => cycle(1, &[500, 501, 502]),
            4 => vec![Record::Cycle {
                // A pass that decided nothing is a boundary too.
                last_ingest_seq: 2,
                instruments: Vec::new(),
            }],
            8 => cycle(3, &[503]),
            // A context for a call that is not an encode (on_open), taking no nonce, and one
            // for a call that took a random scope's nonces.
            10 => vec![
                Record::EncodeCtx {
                    rpc: None,
                    ctx: EncodeCtx {
                        wall: WallNs(-1),
                        mono: MonoNs(u64::MAX),
                        nonces: NonceBlock::EMPTY,
                    },
                },
                Record::Nonce {
                    source: NonceSourceId(u32::MAX),
                    value: u64::MAX,
                },
                Record::EncodeCtx {
                    rpc: None,
                    ctx: EncodeCtx {
                        wall: WallNs(i64::MIN),
                        mono: MonoNs(0),
                        nonces: NonceBlock::new(vec![u64::MAX, 0, 9]),
                    },
                },
            ],
            _ => Vec::new(),
        };
        records.extend(new.into_iter().map(|r| (now, r)));
    }
    let kinds = |pred: fn(&Record) -> bool| records.iter().filter(|(_, r)| pred(r)).count();
    assert_eq!(kinds(|r| matches!(r, Record::Nonce { .. })), 5);
    assert_eq!(kinds(|r| matches!(r, Record::EncodeCtx { .. })), 4);
    assert_eq!(kinds(|r| matches!(r, Record::Cycle { .. })), 3);

    let mut writer = JournalWriter::create(&root, 6, key()).unwrap();
    for (now, record) in &records {
        writer.append(*now, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let mut days: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    days.sort();
    assert_eq!(days, ["20261003", "20261004"]);
    // Each segment is in the current version.
    let open = fs::read(root.join("20261004/6-000000.fbcj")).unwrap();
    assert_eq!(version_of(&open), VERSION);

    let read: Vec<Record> = JournalReader::open(&root, 6)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let expected: Vec<Record> = records.iter().map(|(_, r)| r.blanked()).collect();
    assert_eq!(read, expected);
    // The new kinds carry nothing to redact: they come back exactly as written.
    for ((_, written), back) in records.iter().zip(&read) {
        if matches!(
            written,
            Record::Nonce { .. } | Record::EncodeCtx { .. } | Record::Cycle { .. }
        ) {
            assert_eq!(back, written);
            assert!(written.digests(&key()).is_empty());
        }
    }
}

#[test]
fn a_version_2_segment_holding_a_new_kind_is_malformed() {
    // A version 2 writer could not have written one: the segment is damaged, not newer.
    let root = fresh_dir("v2_with_new_kind");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(
        NOON,
        &Record::Nonce {
            source: NonceSourceId(1),
            value: 2,
        },
    )
    .unwrap();
    w.append(
        NOON,
        &Record::Timer {
            stamp: stamp(1, NOON),
            tag: TimerTag(1),
        },
    )
    .unwrap();
    drop(w);
    let path = root.join("20261003/1-000000.fbcj");
    let mut bytes = fs::read(&path).unwrap();
    // Read in version 3, then relabelled as version 2.
    let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
    assert!(back.iter().all(Result::is_ok), "{back:?}");
    bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
    fs::write(&path, &bytes).unwrap();
    let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
    assert_eq!(back.len(), 1);
    match &back[0] {
        Err(JournalError::Malformed { segment, what }) => {
            assert_eq!(*what, "record kind");
            assert_eq!(*segment, path);
        }
        other => panic!("{other:?}"),
    }
    // Version 1 is still refused (0024), and so is a version newer than this reader's.
    for version in [1, VERSION + 1] {
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
        assert!(
            matches!(
                back[..],
                [Err(JournalError::UnsupportedVersion { version: v, .. })] if v == version
            ),
            "{back:?}"
        );
    }
}
