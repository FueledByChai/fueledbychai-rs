//! FBC-7lm's done line, fbc-journal's half (0006, 0009, decision 0028): fbc-core's toy venue
//! (`auth_toy`, shared by path) marks the token in a synthetic authentication response's body,
//! its `Set-Cookie` header, a header whose name echoes a key, and a key a frame echoes. The
//! journal round trip shows the written files hold only the keyed hash of each of those spans
//! and none of the synthetic credentials, while the bytes around them are kept; and the records
//! read back, with the spans blanked, replay through the toy to the same events and effects.
//!
//! Every credential is synthetic and assembled at run time, so no credential-shaped literal
//! sits in the source. The expected hashes are computed with `hmac` and `sha2` directly, not
//! through the journal's own code.

#[path = "../../fbc-core/tests/auth_toy/mod.rs"]
mod auth_toy;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use auth_toy::{AUTH_TAG, AuthToy, decode_frame, decode_http};
use fbc_core::{
    ConnKey, ExecCodec, HeaderMark, HttpResponse, Inbound, InboundSpans, MonoNs, RawFrame,
    RedactError, Stamp, WallNs,
};
use fbc_journal::{
    BLANK, Entry, HeaderRec, HttpResponseRec, JournalError, JournalReader, JournalWriter, Opaque,
    Opcode, Record, RedactionKey, SpanDigest,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const NOW: WallNs = WallNs(1_791_072_001 * 1_000_000_000);

fn key_bytes() -> Vec<u8> {
    format!("SYNTHKEY-{}-inbound-redaction-test", "m3d").into_bytes()
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
    format!("SYNTH{}{}SECRET", what.to_uppercase(), "-n8q")
}

fn stamp(ingest_seq: u64) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq),
        recv_wall: NOW,
        conn: ConnKey { conn: 3, epoch: 1 },
    }
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

/// A response as the toy is handed it, borrowed from a journaled one.
fn response(rec: &HttpResponseRec) -> (Vec<(&str, &str)>, &[u8]) {
    let headers = rec.headers.iter();
    let headers = headers.map(|h| (h.name.as_str(), h.value.as_str()));
    (headers.collect(), &rec.body.0)
}

#[test]
fn credential_spans_are_journaled_only_as_keyed_hashes_and_replay_decodes_the_same() {
    let token = secret("token");
    let cookie = secret("cookie");
    let echo = secret("echo-key");
    let frame_key = secret("frame-key");
    let body = format!("auth|token={token}|refresh=300");
    let set_cookie = format!("sid={cookie}; Path=/");
    let echo_name = format!("X-Echo-{echo}");
    let headers = [
        ("Content-Type", "text/plain"),
        ("Set-Cookie", set_cookie.as_str()),
        (echo_name.as_str(), "1"),
        ("X-Request-Id", "req-7"),
    ];
    let resp = HttpResponse {
        status: 200,
        headers: &headers,
        body: body.as_bytes(),
    };
    let frame = format!("hello|key={frame_key}|v=2");

    // The toy names the spans; the records carry them.
    let resp_spans = AuthToy.redact_inbound(Inbound::Http(AUTH_TAG, resp));
    let marks = [(1, HeaderMark::Value), (2, HeaderMark::NameAndValue)];
    assert_eq!(resp_spans.headers(), marks);
    let frame_spans = AuthToy.redact_inbound(Inbound::Frame(RawFrame::Text(&frame)));
    let records = [
        Record::HttpResult {
            stamp: stamp(1),
            tag: AUTH_TAG,
            result: Ok(HttpResponseRec::redacted(&resp, &resp_spans).unwrap()),
        },
        Record::inbound_redacted(stamp(2), RawFrame::Text(&frame), &frame_spans).unwrap(),
    ];

    let root = fresh_dir("inbound_redaction");
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    for record in &records {
        writer.append(NOW, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    // No synthetic credential reaches a file; each span's keyed hash does, and the bytes around
    // the spans are kept.
    let files = written_bytes(&root);
    for leaked in [&token, &cookie, &echo, &frame_key] {
        assert!(!contains(&files, leaked.as_bytes()), "{leaked} was written");
    }
    let hashes = [
        hmac(set_cookie.as_bytes()),
        hmac(echo_name.as_bytes()),
        hmac(b"1"),
        hmac(token.as_bytes()),
    ];
    for hash in &hashes {
        assert!(contains(&files, &hash.0));
    }
    for kept in [
        "auth|token=",
        "|refresh=300",
        "X-Request-Id",
        "req-7",
        "hello|key=",
        "|v=2",
    ] {
        assert!(contains(&files, kept.as_bytes()), "{kept} was not kept");
    }

    // Read back: each record blanked at its spans, with the hashes beside it in format order.
    let entries: Vec<Entry> = JournalReader::open(&root, 1)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    for (entry, record) in entries.iter().zip(&records) {
        assert_eq!(entry.record, record.blanked());
        assert_eq!(entry.digests, record.digests(&key()));
        let shown = format!("{:?} {:?}", entry.record, record);
        for leaked in [&token, &cookie, &echo, &frame_key] {
            assert!(!shown.contains(leaked.as_str()), "{shown}");
        }
    }
    assert_eq!(entries[0].digests, hashes);
    assert_eq!(entries[1].digests, [hmac(frame_key.as_bytes())]);

    // Replay: the toy decodes the read-back response and frame to what it decoded live.
    let Record::HttpResult {
        result: Ok(back), ..
    } = &entries[0].record
    else {
        panic!("{:?}", entries[0].record)
    };
    let blank = |n: usize| String::from_utf8(vec![BLANK; n]).unwrap();
    assert_eq!(back.headers[1].value, blank(set_cookie.len()));
    assert_eq!(
        back.headers[2],
        HeaderRec {
            name: blank(echo_name.len()),
            value: blank(1),
            redact: true,
            redact_name: true,
        }
    );
    // A header whose name is redacted shows neither its name nor its value.
    let original = HttpResponseRec::redacted(&resp, &resp_spans).unwrap();
    let shown = format!("{:?}", original.headers[2]);
    assert!(!shown.contains(&echo), "{shown}");
    assert!(shown.contains(&format!("<redacted {} bytes>", echo_name.len())));
    let (back_headers, back_body) = response(back);
    let replayed = HttpResponse {
        status: back.status,
        headers: &back_headers,
        body: back_body,
    };
    let live = decode_http(AUTH_TAG, Ok(resp));
    assert_eq!(live.result, Ok(()));
    assert_eq!(live.events.len(), 1);
    assert_eq!(decode_http(AUTH_TAG, Ok(replayed)), live);
    // Modulo the spans: the replayed body differs from the live one only inside them.
    let token_span = &resp_spans.body()[0];
    let (start, end) = (token_span.start as usize, token_span.end as usize);
    assert_eq!(back_body[..start], body.as_bytes()[..start]);
    assert_eq!(back_body[end..], body.as_bytes()[end..]);
    assert!(back_body[start..end].iter().all(|&b| b == BLANK));

    let Record::Inbound { bytes, .. } = &entries[1].record else {
        panic!("{:?}", entries[1].record)
    };
    let back_frame = std::str::from_utf8(&bytes.0).unwrap();
    let live = decode_frame(RawFrame::Text(&frame));
    assert_eq!(live.events.len(), 1);
    assert_eq!(decode_frame(RawFrame::Text(back_frame)), live);
}

#[test]
fn a_redacted_json_body_still_parses_when_read_back() {
    // Codex r4179231677: a credential inside a JSON string, or standing as a JSON number, is
    // blanked with bytes JSON accepts there, so a codec that parses the live body with
    // serde_json parses the replayed one too, to the same document outside the spans.
    let token = secret("token");
    let id = "4071";
    let body = format!("{{\"jwt_token\":\"{token}\",\"key_id\":{id},\"ttl\":300}}");
    let span = |needle: &str| {
        let at = body.find(needle).unwrap() as u32;
        at..at + needle.len() as u32
    };
    let headers = [("Set-Cookie", "sid=abc; Path=/")];
    let resp = HttpResponse {
        status: 200,
        headers: &headers,
        body: body.as_bytes(),
    };
    let spans = InboundSpans::response(vec![(0, HeaderMark::Value)], vec![span(&token), span(id)]);
    let record = Record::HttpResult {
        stamp: stamp(1),
        tag: AUTH_TAG,
        result: Ok(HttpResponseRec::redacted(&resp, &spans).unwrap()),
    };
    let root = fresh_dir("inbound_redaction_json");
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    writer.append(NOW, &record).unwrap();
    writer.flush().unwrap();
    drop(writer);
    let back: Vec<Record> = JournalReader::open(&root, 1)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let [
        Record::HttpResult {
            result: Ok(back), ..
        },
    ] = &back[..]
    else {
        panic!("{back:?}")
    };
    let live: serde_json::Value = serde_json::from_slice(body.as_bytes()).unwrap();
    let replayed: serde_json::Value = serde_json::from_slice(&back.body.0).unwrap();
    assert_eq!(replayed["ttl"], live["ttl"]);
    assert_eq!(replayed["jwt_token"].as_str().unwrap().len(), token.len());
    assert_ne!(replayed["jwt_token"], live["jwt_token"]);
    assert!(replayed["key_id"].is_u64());
    // The blanked cookie is still a header value a parser takes: printable, no separator.
    let cookie = &back.headers[0].value;
    assert!(
        cookie
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b';' && b != b',')
    );
}

#[test]
fn spans_that_do_not_fit_their_input_are_refused() {
    let resp = HttpResponse {
        status: 200,
        headers: &[("Set-Cookie", "a")],
        body: b"auth|token=t",
    };
    let header = InboundSpans::response(vec![(1, HeaderMark::Value)], Vec::new());
    assert_eq!(
        HttpResponseRec::redacted(&resp, &header),
        Err(RedactError::NoSuchHeader)
    );
    let past = InboundSpans::frame(std::iter::once(5..40).collect());
    let frame = RawFrame::Text("hello|key=k");
    assert_eq!(
        Record::inbound_redacted(stamp(1), frame, &past),
        Err(RedactError::OutOfBounds)
    );
    // A record built field by field with such spans is refused by the writer, which writes
    // nothing for it, and a record with nothing marked is written as it came.
    let root = fresh_dir("inbound_redaction_refused");
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    let inbound = |opcode, bytes: &[u8], redact| Record::Inbound {
        stamp: stamp(1),
        opcode,
        bytes: Opaque(bytes.to_vec()),
        redact,
    };
    let refused = [
        inbound(Opcode::Binary, b"key", std::iter::once(1..9).collect()),
        // A span that splits a character of a text frame.
        inbound(
            Opcode::Text,
            "kéy".as_bytes(),
            std::iter::once(1..2).collect(),
        ),
        Record::HttpResult {
            stamp: stamp(2),
            tag: AUTH_TAG,
            result: Ok(HttpResponseRec {
                body_redact: vec![3..4, 1..2],
                ..HttpResponseRec::from(&resp)
            }),
        },
    ];
    for record in &refused {
        assert!(
            matches!(
                writer.append(NOW, record),
                Err(JournalError::Unencodable(_))
            ),
            "{record:?}"
        );
        // Blanking and hashing such a record stays inside its bytes.
        let _ = (record.blanked(), record.digests(&key()));
    }
    let plain = Record::inbound(stamp(3), frame);
    writer.append(NOW, &plain).unwrap();
    writer.flush().unwrap();
    drop(writer);
    let back: Vec<Record> = JournalReader::open(&root, 1)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(back, [plain]);
}
