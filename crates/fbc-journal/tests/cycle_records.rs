//! FBC-ec9's done line: Nonce, EncodeCtx and Cycle records, written interleaved with the other
//! kinds, read back equal and in write order; and a journal written before these kinds existed
//! (format version 2, `fixtures/journal/v2`) still reads back unchanged (0006, 0014 item 1).
//!
//! Every secret here is synthetic, and each is assembled at run time so no credential-shaped
//! literal sits in the source.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{
    ConnKey, EncodeCtx, Feed, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag,
    InstrumentId, KernelRxNs, MonoNs, NonceBlock, NotSentReason, RawFrame, RpcId, Stamp,
    Subscription, TimerTag, WallNs, WireSlice, WireUrl,
};
use fbc_journal::format::{MAGIC, VERSION};
use fbc_journal::{
    ControlEvent, Entry, HttpRequestRec, HttpResponseRec, JournalError, JournalReader,
    JournalWriter, Marker, NonceSourceId, Opaque, Record, RedactionKey, WriteRes,
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

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/journal/v2")
}

/// The format version in a segment's header.
fn version_of(segment: &[u8]) -> u16 {
    assert_eq!(segment[..4], MAGIC);
    u16::from_le_bytes([segment[4], segment[5]])
}

#[test]
fn a_journal_written_before_these_kinds_existed_reads_back_unchanged() {
    let root = fixture_root();
    let day = root.join("20261003");
    // The fixture is what the version 2 writer left: a closed, compressed segment and the open
    // one, both in version 2.
    let closed = zstd::decode_all(&fs::read(day.join("1-000000.fbcj.zst")).unwrap()[..]).unwrap();
    let open = fs::read(day.join("1-000001.fbcj")).unwrap();
    assert_eq!(version_of(&closed), 2);
    assert_eq!(version_of(&open), 2);
    const { assert!(VERSION > 2) };

    let entries: Vec<Entry> = JournalReader::open(&root, 1)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    let written = v2_records();
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
