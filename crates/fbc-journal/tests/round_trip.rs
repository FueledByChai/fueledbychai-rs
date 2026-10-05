//! FBC-aen's done line: one record of every kind, written across two UTC days, reads back equal
//! (with its redaction spans blanked) and in write order, and the written files hold no byte of
//! any redaction span and no value of an Authorization, Proxy-Authorization, Cookie or
//! Set-Cookie header (0006, 0009).
//!
//! Every secret here is synthetic, and each is assembled at run time so no credential-shaped
//! literal sits in the source.

use std::fs;
use std::path::{Path, PathBuf};

use fbc_core::{
    BookId, ConnKey, Feed, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag,
    InstrumentId, KernelRxNs, MonoNs, NotSentReason, RawFrame, RpcId, Stamp, Subscription,
    TimerTag, TouchSourceId, WallNs, WireSlice, WireUrl,
};
use fbc_journal::{
    BLANK, ControlEvent, HeaderRec, HttpRequestRec, HttpResponseRec, JournalReader, JournalWriter,
    Marker, Opaque, Opcode, Record, WriteRes,
};

/// The key the journal hashes redaction spans under in these tests.
fn key() -> std::sync::Arc<fbc_journal::RedactionKey> {
    std::sync::Arc::new(fbc_journal::RedactionKey::new(&[9; 32]).unwrap())
}

const SEC: i64 = 1_000_000_000;
/// 2026-10-03T23:59:59Z and 2026-10-04T00:00:01Z.
const BEFORE_MIDNIGHT: WallNs = WallNs(1_791_071_999 * SEC);
const AFTER_MIDNIGHT: WallNs = WallNs(1_791_072_001 * SEC);

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A synthetic secret, distinctive enough that finding it in a file means it leaked.
fn secret(what: &str) -> String {
    format!("SYNTH{}{}SECRET", what.to_uppercase(), "-x9q")
}

/// A span over `needle` inside `hay`.
fn span_of(hay: &[u8], needle: &str) -> std::ops::Range<u32> {
    let at = hay
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())
        .unwrap();
    at as u32..(at + needle.len()) as u32
}

fn conn(epoch: u32) -> ConnKey {
    ConnKey { conn: 3, epoch }
}

/// The stamp of a result or timer firing after midnight: ingest sequence and mono time.
fn after(ingest_seq: u64, mono: u64) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(mono),
        recv_wall: WallNs(AFTER_MIDNIGHT.0 + mono as i64),
        conn: conn(1),
    }
}

struct Session {
    /// (wall time of the append, record), in write order.
    records: Vec<(WallNs, Record)>,
    /// Every value that must not appear in a written file.
    secrets: Vec<String>,
    /// Values that are not secret and must appear, so the search is known to work.
    plain: Vec<String>,
}

fn session() -> Session {
    let mut secrets = Vec::new();
    let mut plain = Vec::new();
    let mut records = Vec::new();

    // Inbound frames, with their stamps.
    let text = String::from("{\"channel\":\"bbo\",\"px\":\"101.5\"}");
    plain.push(text.clone());
    let stamp = Stamp {
        ingest_seq: 1,
        kernel_rx: Some(KernelRxNs(1_791_071_999 * SEC - 5)),
        recv_mono: MonoNs(10),
        recv_wall: WallNs(1_791_071_999 * SEC),
        conn: conn(1),
    };
    records.push((
        BEFORE_MIDNIGHT,
        Record::inbound(stamp, RawFrame::Text(&text)),
    ));
    let binary = vec![0u8, 1, 2, 0xff, 0xfe];
    records.push((
        BEFORE_MIDNIGHT,
        Record::inbound(
            Stamp {
                ingest_seq: 2,
                kernel_rx: None,
                ..stamp
            },
            RawFrame::Binary(&binary),
        ),
    ));

    // A connection opened, and its subscription call.
    records.push((
        BEFORE_MIDNIGHT,
        Record::Control {
            at: MonoNs(5),
            ev: ControlEvent::Opened(conn(1)),
        },
    ));
    records.push((
        BEFORE_MIDNIGHT,
        Record::Control {
            at: MonoNs(6),
            ev: ControlEvent::Subscribe {
                conn: conn(1),
                add: vec![
                    Subscription {
                        inst: InstrumentId::new(7),
                        feed: Feed::Book(BookId(1)),
                    },
                    Subscription {
                        inst: InstrumentId::new(7),
                        feed: Feed::Touch(TouchSourceId(0)),
                    },
                ],
                remove: vec![Subscription {
                    inst: InstrumentId::new(8),
                    feed: Feed::Trades,
                }],
            },
        },
    ));

    // An outbound frame with two redaction spans, and its write result.
    let token = secret("frame-token");
    let key = secret("frame-key");
    secrets.extend([token.clone(), key.clone()]);
    let body =
        format!("{{\"op\":\"auth\",\"token\":\"{token}\",\"key\":\"{key}\",\"sig\":\"0x51\"}}");
    plain.push("\"op\":\"auth\"".into());
    let spans = vec![
        span_of(body.as_bytes(), &token),
        span_of(body.as_bytes(), &key),
    ];
    let frame = WireSlice::redacted(body.into_bytes(), spans).unwrap();
    records.push((
        BEFORE_MIDNIGHT,
        Record::Outbound {
            at: MonoNs(11),
            conn: conn(1),
            rpc: Some(RpcId(40)),
            opcode: Opcode::Text,
            frame,
        },
    ));
    records.push((
        BEFORE_MIDNIGHT,
        Record::WriteResult {
            at: MonoNs(12),
            conn: conn(1),
            rpc: Some(RpcId(40)),
            result: WriteRes::Written,
        },
    ));

    // The session's header marker, written after midnight: the second day starts here.
    records.push((
        AFTER_MIDNIGHT,
        Record::Marker(Marker::SessionStart {
            header: Opaque(b"consumer header v1".to_vec()),
        }),
    ));

    // An HTTP request with every kind of credential the journal must blank.
    let bearer = format!("Bearer {}", secret("bearer"));
    let proxy = format!("Basic {}", secret("proxy"));
    let cookie = format!("session={}", secret("cookie"));
    let api_key = secret("api-key");
    let query_key = secret("query-key");
    let body_key = secret("body-key");
    secrets.extend([
        bearer.clone(),
        proxy.clone(),
        cookie.clone(),
        api_key.clone(),
        query_key.clone(),
        body_key.clone(),
    ]);
    let url = format!("https://api.example.test/v1/orders?key={query_key}&x=1");
    plain.push("https://api.example.test/v1/orders?key=".into());
    let url_span = span_of(url.as_bytes(), &query_key);
    let req_body = format!("{{\"market\":\"BTC-USD-PERP\",\"key\":\"{body_key}\"}}");
    plain.push("BTC-USD-PERP".into());
    let body_span = span_of(req_body.as_bytes(), &body_key);
    let req = HttpRequest {
        method: HttpMethod::Post,
        url: WireUrl::redacted(url, vec![url_span]).unwrap(),
        headers: vec![
            Header {
                name: "Authorization",
                value: bearer,
                redact: false,
            },
            Header {
                name: "proxy-authorization",
                value: proxy,
                redact: false,
            },
            Header {
                name: "Cookie",
                value: cookie,
                redact: false,
            },
            Header {
                name: "X-Api-Key",
                value: api_key,
                redact: true,
            },
            Header {
                name: "Content-Type",
                value: "application/json".into(),
                redact: false,
            },
        ],
        body: WireSlice::redacted(req_body.into_bytes(), vec![body_span]).unwrap(),
    };
    plain.push("application/json".into());
    records.push((
        AFTER_MIDNIGHT,
        Record::HttpRequest {
            at: MonoNs(20),
            conn: conn(1),
            tag: HttpTag(9),
            rpc: Some(RpcId(41)),
            req: HttpRequestRec::from(&req),
        },
    ));

    // Its result, with headers: a CDN cookie set on the response.
    let set_cookie = format!("cdn={}; Path=/", secret("set-cookie"));
    let echoed = format!("session={}", secret("echoed-cookie"));
    secrets.extend([set_cookie.clone(), echoed.clone()]);
    let headers = [
        ("Set-Cookie", set_cookie.as_str()),
        ("COOKIE", echoed.as_str()),
        ("X-Request-Id", "req-77"),
    ];
    plain.push("req-77".into());
    let resp = HttpResponse {
        status: 200,
        headers: &headers,
        body: b"{\"status\":\"OPEN\"}",
    };
    records.push((
        AFTER_MIDNIGHT,
        Record::HttpResult {
            stamp: after(3, 21),
            tag: HttpTag(9),
            result: Ok(HttpResponseRec::from(&resp)),
        },
    ));
    records.push((
        AFTER_MIDNIGHT,
        Record::HttpResult {
            stamp: after(4, 22),
            tag: HttpTag(10),
            result: Err(HttpFailure::TimedOut),
        },
    ));

    // A plain request (no span, no secret header) on a poll endpoint.
    records.push((
        AFTER_MIDNIGHT,
        Record::HttpRequest {
            at: MonoNs(23),
            conn: ConnKey { conn: 4, epoch: 1 },
            tag: HttpTag(11),
            rpc: None,
            req: HttpRequestRec::from(&HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain("https://api.example.test/v1/orderbook/BTC-USD-PERP"),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            }),
        },
    ));

    // A timer, a failed write, the markers and the connection closing.
    records.push((
        AFTER_MIDNIGHT,
        Record::Timer {
            stamp: after(5, 30),
            tag: TimerTag(2),
        },
    ));
    records.push((
        AFTER_MIDNIGHT,
        Record::Outbound {
            at: MonoNs(31),
            conn: conn(1),
            rpc: None,
            opcode: Opcode::Text,
            frame: WireSlice::plain(b"{\"op\":\"ping\"}".to_vec()),
        },
    ));
    records.push((
        AFTER_MIDNIGHT,
        Record::WriteResult {
            at: MonoNs(31),
            conn: conn(1),
            rpc: None,
            result: WriteRes::NotSent(NotSentReason::Backpressure),
        },
    ));
    records.push((
        AFTER_MIDNIGHT,
        Record::Marker(Marker::Degraded {
            from_seq: 1_000,
            dropped: 17,
        }),
    ));
    records.push((AFTER_MIDNIGHT, Record::Marker(Marker::Recovered)));
    records.push((
        AFTER_MIDNIGHT,
        Record::Control {
            at: MonoNs(40),
            ev: ControlEvent::Closed(conn(1)),
        },
    ));

    Session {
        records,
        secrets,
        plain,
    }
}

fn files_under(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files_under(&path, found);
        } else {
            found.push(path);
        }
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn every_kind_round_trips_across_two_days_in_order_with_no_secret_written() {
    let root = fresh_dir("round_trip");
    let s = session();

    let mut writer = JournalWriter::create(&root, 2, key()).unwrap();
    for (now, record) in &s.records {
        writer.append(*now, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    // One subdirectory per UTC day, one segment in each.
    let mut days: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    days.sort();
    assert_eq!(days, ["20261003", "20261004"]);
    for day in &days {
        let names: Vec<String> = fs::read_dir(root.join(day))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 1, "{day}: {names:?}");
        assert!(names[0].starts_with("2-"), "{names:?}");
    }

    // Read back equal, with spans blanked, and in write order.
    let read: Vec<Record> = JournalReader::open(&root, 2)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let expected: Vec<Record> = s.records.iter().map(|(_, r)| r.blanked()).collect();
    assert_eq!(read, expected);

    // Records with nothing to redact come back exactly as written.
    for ((_, written), back) in s.records.iter().zip(&read) {
        match written {
            Record::Outbound { frame, .. } if !frame.redactions().is_empty() => {}
            Record::HttpRequest { .. } | Record::HttpResult { result: Ok(_), .. } => {}
            _ => assert_eq!(back, written),
        }
    }

    // A blanked span keeps its place and length.
    let Record::Outbound { frame, .. } = &read[4] else {
        panic!("{:?}", read[4])
    };
    let Record::Outbound { frame: sent, .. } = &s.records[4].1 else {
        unreachable!()
    };
    assert_eq!(frame.bytes().len(), sent.bytes().len());
    assert_eq!(frame.redactions(), sent.redactions());
    for span in frame.redactions() {
        let (a, b) = (span.start as usize, span.end as usize);
        assert!(frame.bytes()[a..b].iter().all(|&byte| byte == BLANK));
    }

    // Secret headers come back blanked and marked, by name whatever their case.
    let Record::HttpRequest { req, .. } = &read[7] else {
        panic!("{:?}", read[7])
    };
    let marked: Vec<(&str, bool)> = req
        .headers
        .iter()
        .map(|h| (h.name.as_str(), h.redact))
        .collect();
    assert_eq!(
        marked,
        [
            ("Authorization", true),
            ("proxy-authorization", true),
            ("Cookie", true),
            ("X-Api-Key", true),
            ("Content-Type", false),
        ]
    );
    let Record::HttpResult {
        result: Ok(resp), ..
    } = &read[8]
    else {
        panic!("{:?}", read[8])
    };
    assert_eq!(resp.status, 200);
    let set_cookie: &HeaderRec = &resp.headers[0];
    assert!(set_cookie.redact);
    assert!(set_cookie.value.bytes().all(|b| b == BLANK));
    assert!(!resp.headers[2].redact);
    assert_eq!(resp.headers[2].value, "req-77");

    // No secret in any written file; the plain parts are there, so the search works.
    let mut files = Vec::new();
    files_under(&root, &mut files);
    let bytes: Vec<u8> = files.iter().flat_map(|f| fs::read(f).unwrap()).collect();
    for secret in &s.secrets {
        assert!(!contains(&bytes, secret.as_bytes()), "{secret} was written");
        // Nor any distinctive part of one (the synthetic core of each).
        let core = &secret[secret.find("SYNTH").unwrap()..];
        assert!(!contains(&bytes, core.as_bytes()), "{core} was written");
    }
    for plain in &s.plain {
        assert!(contains(&bytes, plain.as_bytes()), "{plain} is missing");
    }
}

#[test]
fn a_record_s_debug_shows_no_secret() {
    let s = session();
    for (_, record) in &s.records {
        let shown = format!("{record:?}");
        for secret in &s.secrets {
            let core = &secret[secret.find("SYNTH").unwrap()..];
            assert!(!shown.contains(core), "{shown}");
        }
    }
    // Inbound bytes and response bodies show by length only: a venue can echo a key.
    let Record::Inbound { opcode, bytes, .. } = &s.records[0].1 else {
        unreachable!()
    };
    assert_eq!(*opcode, Opcode::Text);
    assert_eq!(format!("{bytes:?}"), "Opaque { len: 30 }");
}
