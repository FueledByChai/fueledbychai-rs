//! FBC-s69's done line (0006, 0009, 0024, decision 0028): a toy-venue session whose codec names
//! a credential in an inbound frame, in an HTTP response body and in response headers (one by
//! value, one by name and value) journals each of those only as its HMAC-SHA-256 under the
//! test key: the journal files hold none of the synthetic credentials, the bytes outside the
//! spans are kept verbatim, and no inbound record is withheld. What the session offers the
//! journal is the borrowed record it encodes, at the length it encodes to: the files are byte
//! for byte those the owned records write. A codec whose spans do not fit what it named them in
//! is a defect: that input is journaled with its whole body and every header hashed, never
//! verbatim, and counted.
//!
//! Every credential is synthetic and assembled at run time, so no credential-shaped literal
//! sits in the source. The expected hashes are computed with `hmac` and `sha2` directly, not
//! through the journal's own code.

mod common;

use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use common::toy::{self, ToyVenue};
use common::{ScriptedHttp, ScriptedWs};
use fbc_core::{
    EndpointPlan, Envelope, MdEvent, MdTransport, TrafficClass, VenueConfig, WallNs, WireUrl,
};
use fbc_journal::{
    BLANK, Entry, HeaderRec, JournalReader, JournalSink, JournalWriter, QueueSink, Record,
    RecordRef, Recorded, RedactionKey, SinkConfig, SpanDigest, journal_queue,
};
use fbc_runtime::{
    Connector, IngestClock, Journal, Liveness, MdSession, MdSessionConfig, ProxyConfig,
    ReconnectPacing, WriteStall,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const SHARD: u16 = 4;

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key_bytes() -> Vec<u8> {
    format!("SYNTHKEY-{}-session-redaction-test", "r7k").into_bytes()
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

/// A synthetic credential: lower case, as an HTTP header name arrives, and free of `|`.
fn secret(what: &str) -> String {
    format!("synth{what}{}secret", "-v2m")
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Every byte under `dir` as written, in path order, a closed segment decompressed.
fn written_bytes(dir: &Path) -> Vec<u8> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            out.extend(written_bytes(&path));
        } else if path.extension().is_some_and(|e| e == "zst") {
            out.extend(zstd::decode_all(fs::File::open(&path).unwrap()).unwrap());
        } else {
            out.extend(fs::read(&path).unwrap());
        }
    }
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// A session of `venue` at `url`, wanting trades on instrument 1.
fn session(venue: &'static ToyVenue, url: String) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 64 * 1024,
        conn: 6,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

/// The journal queue's sink, keeping what each offer stands for as an owned record, and
/// whether it was offered borrowed.
struct Tee {
    queue: QueueSink,
    offered: Vec<(WallNs, Record, bool)>,
}

impl JournalSink for Tee {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.offered.push((now, record.clone(), false));
        self.queue.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        let borrowed = !matches!(record, RecordRef::Owned(_));
        self.offered.push((now, record.to_record(), borrowed));
        self.queue.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.queue.omit(class, now)
    }
}

/// What a session of the toy journaled when the script `play` ran against it, read back, with
/// every byte of its files and the session's counter of codec span defects; `owned` gets the
/// files the records offered would write as owned records.
async fn journal_of<F, Fut>(name: &str, play: F) -> (Vec<Entry>, Vec<u8>, Tee, u64)
where
    F: FnOnce(ScriptedWs, ScriptedHttp, Seen, fbc_runtime::MdControl) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let root = fresh_dir(name);
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let tee = Rc::new(RefCell::new(Tee {
        queue,
        offered: Vec::new(),
    }));
    let (ws, http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let seen = Seen::default();
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), keep(&seen)).unwrap();
    session.set_journal(Journal::new(tee.clone()));
    let (run, ()) = tokio::join!(session.run(), play(ws, http, seen.clone(), control));
    run.unwrap();
    writer.close().unwrap();
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    let files = written_bytes(&root);
    fs::remove_dir_all(&root).unwrap();
    let defects = session.counters().refused_redactions;
    drop(session);
    let tee = Rc::try_unwrap(tee).ok().unwrap().into_inner();
    (entries, files, tee, defects)
}

/// The bytes the owned records `offered` write, as a journal of their own.
fn owned_files(name: &str, offered: &[(WallNs, Record, bool)]) -> Vec<u8> {
    let root = fresh_dir(name);
    let mut writer = JournalWriter::create(&root, SHARD, key()).unwrap();
    for (now, record, _) in offered {
        writer.append(*now, record).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let files = written_bytes(&root);
    fs::remove_dir_all(&root).unwrap();
    files
}

fn blank(n: usize) -> String {
    String::from_utf8(vec![BLANK; n]).unwrap()
}

#[tokio::test]
async fn a_session_journals_what_its_codec_names_in_frames_and_responses_only_as_keyed_hashes() {
    let in_frame = secret("frame");
    let in_body = secret("body");
    let in_value = secret("value");
    let in_name = format!("x-key-{}", secret("name"));
    let frame = format!("trade|sym=A|px=1|qty=1|seq=1|echo={in_frame}|v=2");
    let body = format!("trade|sym=A|px=2|qty=1|seq=2|echo={in_body}\n");
    let head = format!("HTTP/1.1 200 OK\r\nX-Echo: {in_value}\r\n{in_name}: 1\r\nX-Toy: yes");
    let (live_frame, live_body) = (frame.clone(), body.clone());
    let (entries, files, tee, defects) = journal_of(
        "md_inbound_redaction",
        |mut ws, mut http, seen, control| async move {
            let mut peer = ws.accept().await;
            assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
            assert_eq!(peer.recv().await, "sub|add=A");
            peer.send(&live_frame);
            until(|| seen.borrow().len() == 1).await;
            let snap = http.url("/snap");
            peer.send(&format!("get|tag=1|ms=5000|url={snap}"));
            http.request().await.answer(&head, &live_body).await;
            until(|| seen.borrow().len() == 2).await;
            drop(control);
            assert_eq!(peer.next().await, None);
        },
    )
    .await;
    assert_eq!(defects, 0);

    // No synthetic credential reaches a file; each marked span's keyed hash does.
    let secrets = [&in_frame, &in_body, &in_value, &in_name];
    for leaked in secrets {
        assert!(!contains(&files, leaked.as_bytes()), "{leaked} was written");
    }
    let hashes = [
        hmac(in_frame.as_bytes()),
        hmac(in_value.as_bytes()),
        hmac(in_name.as_bytes()),
        hmac(b"1"),
        hmac(in_body.as_bytes()),
    ];
    for hash in &hashes {
        assert!(contains(&files, &hash.0));
    }

    // No inbound record is withheld: both frames and the response are journaled, each kept
    // verbatim outside its spans, which read back blanked.
    let inbound: Vec<&Entry> = entries
        .iter()
        .filter(|e| matches!(e.record, Record::Inbound { .. }))
        .collect();
    assert_eq!(inbound.len(), 2, "{entries:#?}");
    let Record::Inbound { bytes, redact, .. } = &inbound[0].record else {
        unreachable!()
    };
    let read_frame = frame.replace(&in_frame, &blank(in_frame.len()));
    assert_eq!(bytes.0, read_frame.as_bytes());
    assert_eq!(redact.len(), 1);
    assert_eq!(inbound[0].digests, [hashes[0]]);
    let Record::Inbound { bytes, redact, .. } = &inbound[1].record else {
        unreachable!()
    };
    assert!(
        std::str::from_utf8(&bytes.0)
            .unwrap()
            .starts_with("get|tag=1")
    );
    assert!(redact.is_empty());

    let results: Vec<&Entry> = entries
        .iter()
        .filter(|e| matches!(e.record, Record::HttpResult { .. }))
        .collect();
    assert_eq!(results.len(), 1);
    let Record::HttpResult {
        result: Ok(resp), ..
    } = &results[0].record
    else {
        panic!("{:?}", results[0].record)
    };
    let read_body = body.replace(&in_body, &blank(in_body.len()));
    assert_eq!(resp.body.0, read_body.as_bytes());
    assert_eq!(
        resp.headers[..3],
        [
            HeaderRec {
                name: "x-echo".into(),
                value: blank(in_value.len()),
                redact: true,
                redact_name: false,
            },
            HeaderRec {
                name: blank(in_name.len()),
                value: blank(1),
                redact: true,
                redact_name: true,
            },
            HeaderRec {
                name: "x-toy".into(),
                value: "yes".into(),
                redact: false,
                redact_name: false,
            },
        ]
    );
    assert_eq!(results[0].digests, hashes[1..]);

    // The session offered the frames and the response borrowed, and the journal encoded each
    // at exactly the length the owned record it stands for encodes to: the files are byte for
    // byte what the owned records write.
    let borrowed: Vec<&Record> = tee
        .offered
        .iter()
        .filter(|(_, _, borrowed)| *borrowed)
        .map(|(_, r, _)| r)
        .collect();
    assert!(
        borrowed
            .iter()
            .any(|r| matches!(r, Record::HttpResult { .. }))
    );
    let inputs = borrowed
        .iter()
        .filter(|r| matches!(r, Record::Inbound { .. }));
    assert_eq!(inputs.count(), 2);
    assert_eq!(
        files,
        owned_files("md_inbound_redaction_owned", &tee.offered)
    );
}

/// A codec whose spans do not fit what it named them in is a defect: the input is journaled
/// with its whole body and every header, name and value, as keyed hashes, never verbatim, and
/// counted; the codec still decodes it.
#[tokio::test]
async fn spans_that_do_not_fit_hash_the_whole_input_and_are_counted() {
    let in_frame = secret("frame");
    let in_body = secret("body");
    let frame = format!("trade|sym=A|px=1|qty=1|seq=1|echo={in_frame}|oops=1");
    let body = format!("trade|sym=A|px=2|qty=1|seq=2|echo={in_body}\n");
    let head = "HTTP/1.1 200 OK\r\nX-Oops: 1\r\nX-Toy: yes".to_owned();
    let (live_frame, live_body) = (frame.clone(), body.clone());
    let (entries, files, _tee, defects) = journal_of(
        "md_inbound_redaction_defect",
        |mut ws, mut http, seen, control| async move {
            let mut peer = ws.accept().await;
            assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
            assert_eq!(peer.recv().await, "sub|add=A");
            peer.send(&live_frame);
            until(|| seen.borrow().len() == 1).await;
            let snap = http.url("/snap");
            peer.send(&format!("get|tag=1|ms=5000|url={snap}"));
            http.request().await.answer(&head, &live_body).await;
            until(|| seen.borrow().len() == 2).await;
            drop(control);
            assert_eq!(peer.next().await, None);
        },
    )
    .await;
    assert_eq!(defects, 2);
    for leaked in [&in_frame, &in_body, &frame, &body] {
        assert!(!contains(&files, leaked.as_bytes()), "{leaked} was written");
    }
    assert!(!contains(&files, b"x-toy"));
    let entry = entries
        .iter()
        .find(|e| matches!(e.record, Record::Inbound { ref redact, .. } if !redact.is_empty()))
        .unwrap();
    let Record::Inbound { bytes, .. } = &entry.record else {
        unreachable!()
    };
    assert_eq!(bytes.0, blank(frame.len()).as_bytes());
    assert_eq!(entry.digests, [hmac(frame.as_bytes())]);
    let Some(Record::HttpResult {
        result: Ok(resp), ..
    }) = entries
        .iter()
        .map(|e| &e.record)
        .find(|r| matches!(r, Record::HttpResult { .. }))
    else {
        panic!("{entries:#?}")
    };
    assert_eq!(resp.body.0, blank(body.len()).as_bytes());
    assert!(resp.headers.iter().all(|h| h.redact_name && h.redact));
    assert!(
        resp.headers
            .iter()
            .all(|h| h.name.bytes().all(|b| b == BLANK))
    );
}
