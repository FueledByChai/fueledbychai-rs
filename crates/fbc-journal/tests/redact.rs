//! FBC-apz's done line: records carrying a bearer token in an Authorization header, a cookie in
//! a request, a Set-Cookie in a response, a key in a header the codec marked redacted, a key in
//! a URL query span and a signed payload are journaled; the files hold none of the synthetic
//! secrets, each span holds its HMAC-SHA-256 under the test key (equal secrets hash equally),
//! the signature bytes are kept verbatim, and the key's `Debug` shows none of its bytes
//! (0006, 0009).
//!
//! Every secret here is synthetic and assembled at run time, so no credential-shaped literal
//! sits in the source. The expected hashes are computed here with `hmac` and `sha2` directly,
//! not through the journal's own code.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{
    ConnKey, Header, HttpMethod, HttpRequest, HttpResponse, HttpTag, MonoNs, RpcId, Stamp,
    TrafficClass, WallNs, WireSlice, WireUrl,
};
use fbc_journal::{
    BLANK, Entry, HttpRequestRec, HttpResponseRec, JournalReader, JournalSink, JournalWriter,
    Record, RedactionKey, SinkConfig, SpanDigest, journal_queue,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const NOW: WallNs = WallNs(1_791_072_001 * 1_000_000_000);

/// The test key: synthetic, and distinctive enough that finding it means it leaked.
fn key_bytes() -> Vec<u8> {
    format!("SYNTHKEY-{}-journal-redaction-test", "q7z").into_bytes()
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&key_bytes()).unwrap())
}

/// HMAC-SHA-256 of `bytes` under the test key, computed without the journal.
fn hmac(bytes: &[u8]) -> SpanDigest {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes()).unwrap();
    mac.update(bytes);
    SpanDigest(mac.finalize().into_bytes().into())
}

fn secret(what: &str) -> String {
    format!("SYNTH{}{}SECRET", what.to_uppercase(), "-k4w")
}

fn span_of(hay: &[u8], needle: &str) -> std::ops::Range<u32> {
    let at = hay
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())
        .unwrap();
    at as u32..(at + needle.len()) as u32
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn written_bytes(dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(written_bytes(&path));
        } else {
            out.extend(fs::read(&path).unwrap());
        }
    }
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

const CONN: ConnKey = ConnKey { conn: 2, epoch: 1 };

struct Session {
    records: Vec<Record>,
    /// Each record's span values in the order the journal hashes them.
    span_values: Vec<Vec<String>>,
    secrets: Vec<String>,
    /// The signature, which must be written verbatim.
    signature: String,
}

fn request(tag: u64, bearer: &str, cookie: &str, api_key: &str) -> (Record, Vec<String>) {
    let url = format!("https://api.example.test/v1/account?api_key={api_key}&page=1");
    let url_span = span_of(url.as_bytes(), api_key);
    let req = HttpRequest {
        method: HttpMethod::Get,
        url: WireUrl::redacted(url, vec![url_span]).unwrap(),
        headers: vec![
            Header {
                name: "Authorization",
                value: bearer.to_owned(),
                redact: false,
            },
            Header {
                name: "Cookie",
                value: cookie.to_owned(),
                redact: false,
            },
            Header {
                name: "X-Api-Key",
                value: api_key.to_owned(),
                redact: true,
            },
            Header {
                name: "Accept",
                value: "application/json".into(),
                redact: false,
            },
        ],
        body: WireSlice::plain(Vec::new()),
    };
    let record = Record::HttpRequest {
        at: MonoNs(tag),
        conn: CONN,
        tag: HttpTag(tag),
        rpc: None,
        req: HttpRequestRec::from(&req),
    };
    // The URL's span, then the secret headers in header order, then the body's spans.
    let values = vec![
        api_key.to_owned(),
        bearer.to_owned(),
        cookie.to_owned(),
        api_key.to_owned(),
    ];
    (record, values)
}

fn session() -> Session {
    let token = secret("bearer");
    let bearer = format!("Bearer {token}");
    let cookie = format!("session={}", secret("cookie"));
    let other_cookie = format!("session={}", secret("other-cookie"));
    let api_key = secret("api-key");
    let set_cookie = format!("cdn={}; Path=/", secret("set-cookie"));
    let jwt = secret("jwt");
    // A Stark signature's two felts, in decimal as Paradex sends them.
    let signature = format!(
        "[\"{}\",\"{}\"]",
        "31415926535897932384", "27182818284590452353"
    );

    let mut records = Vec::new();
    let mut span_values = Vec::new();

    let (r, v) = request(1, &bearer, &cookie, &api_key);
    records.push(r);
    span_values.push(v);

    let headers = [
        ("Set-Cookie", set_cookie.as_str()),
        ("Content-Type", "application/json"),
    ];
    records.push(Record::HttpResult {
        stamp: Stamp {
            ingest_seq: 1,
            kernel_rx: None,
            recv_mono: MonoNs(2),
            recv_wall: NOW,
            conn: CONN,
        },
        tag: HttpTag(1),
        result: Ok(HttpResponseRec::from(&HttpResponse {
            status: 200,
            headers: &headers,
            body: b"{\"ok\":true}",
        })),
    });
    span_values.push(vec![set_cookie.clone()]);

    // The same bearer token and key again, with another cookie: equal secrets hash equally.
    let (r, v) = request(3, &bearer, &other_cookie, &api_key);
    records.push(r);
    span_values.push(v);

    // A signed order: the JWT is a span, the signature is not.
    let order = format!(
        "{{\"op\":\"order\",\"jwt\":\"{jwt}\",\"market\":\"BTC-USD-PERP\",\"signature\":{signature}}}"
    );
    let frame = WireSlice::redacted(
        order.clone().into_bytes(),
        vec![span_of(order.as_bytes(), &jwt)],
    )
    .unwrap();
    records.push(Record::Outbound {
        at: MonoNs(4),
        conn: CONN,
        rpc: Some(RpcId(9)),
        frame,
    });
    span_values.push(vec![jwt.clone()]);

    Session {
        records,
        span_values,
        secrets: vec![token, cookie, other_cookie, api_key, set_cookie, jwt],
        signature,
    }
}

fn read_entries(root: &Path, shard: u16) -> Vec<Entry> {
    JournalReader::open(root, shard)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Every check of the done line on what one journal directory holds.
fn check_journal(root: &Path, shard: u16, s: &Session) {
    let bytes = written_bytes(root);

    // No synthetic secret, nor the key, is in any file.
    for secret in &s.secrets {
        let core = &secret[secret.find("SYNTH").unwrap()..];
        assert!(!contains(&bytes, core.as_bytes()), "{core} was written");
    }
    assert!(!contains(&bytes, &key_bytes()), "the key was written");

    // Each span holds its HMAC-SHA-256 under the test key: in the file, and read back in
    // the journal's order.
    let entries = read_entries(root, shard);
    assert_eq!(entries.len(), s.records.len());
    for ((entry, written), values) in entries.iter().zip(&s.records).zip(&s.span_values) {
        let expected: Vec<SpanDigest> = values.iter().map(|v| hmac(v.as_bytes())).collect();
        assert_eq!(entry.digests, expected, "{written:?}");
        for d in &expected {
            assert!(contains(&bytes, &d.0), "a span's hash is not in the file");
        }
        // The record reads back with its spans blanked, as the journal always returned it.
        assert_eq!(entry.record, written.blanked());
    }

    // Equal secrets hash equally, across records and places; different ones do not.
    let first = &entries[0].digests;
    let again = &entries[2].digests;
    assert_eq!(first[0], first[3], "the key in the URL and in X-Api-Key");
    assert_eq!(first[1], again[1], "the bearer token in both requests");
    assert_eq!(first[0], again[0]);
    assert_ne!(first[2], again[2], "two different cookies");

    // The signature is kept verbatim, in the file and read back.
    assert!(contains(&bytes, s.signature.as_bytes()));
    let Record::Outbound { frame, .. } = &entries[3].record else {
        panic!("{:?}", entries[3].record)
    };
    assert!(contains(frame.bytes(), s.signature.as_bytes()));
    let span = &frame.redactions()[0];
    assert!(
        frame.bytes()[span.start as usize..span.end as usize]
            .iter()
            .all(|&b| b == BLANK)
    );
}

#[test]
fn spans_are_written_as_keyed_hashes_and_signatures_verbatim() {
    let root = fresh_dir("redact_writer");
    let s = session();
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    for record in &s.records {
        writer.append(NOW, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    check_journal(&root, 1, &s);

    // What the reader returns is what `Record::digests` says it will.
    let k = key();
    for (entry, written) in read_entries(&root, 1).iter().zip(&s.records) {
        assert_eq!(entry.digests, written.digests(&k));
    }
}

#[test]
fn the_sink_hashes_spans_on_the_caller_s_thread_the_same_way() {
    let root = fresh_dir("redact_sink");
    let s = session();
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 1 << 16,
            soft_limit_pct: 80,
        },
        key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    for record in &s.records {
        assert_eq!(
            sink.record(TrafficClass::Normal, NOW, record),
            fbc_journal::Recorded::Ok
        );
    }
    drop(sink);
    writer.close().unwrap();
    check_journal(&root, 1, &s);
}

#[test]
fn another_key_hashes_the_same_secret_differently() {
    let s = session();
    let other = RedactionKey::new(&[7u8; 32]).unwrap();
    let ours = s.records[0].digests(&key());
    let theirs = s.records[0].digests(&other);
    assert_eq!(ours.len(), theirs.len());
    for (a, b) in ours.iter().zip(&theirs) {
        assert_ne!(a, b);
    }
}

#[test]
fn the_key_shows_none_of_its_bytes() {
    let k = key();
    let shown = [format!("{k:?}"), format!("{k:#?}"), format!("{k}")];
    let raw = key_bytes();
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let listed = format!("{raw:?}");
    for text in &shown {
        assert_eq!(
            text,
            if text.starts_with('<') {
                "<redaction key>"
            } else {
                "RedactionKey(..)"
            }
        );
        assert!(!text.contains("SYNTHKEY"), "{text}");
        assert!(!text.contains(&hex[..8]), "{text}");
        assert!(!text.contains(&listed[..8]), "{text}");
    }
}

#[test]
fn a_short_key_is_refused() {
    assert!(RedactionKey::new(&[1u8; 31]).is_err());
    assert!(RedactionKey::new(&[]).is_err());
    assert!(RedactionKey::new(&[1u8; 32]).is_ok());
}
