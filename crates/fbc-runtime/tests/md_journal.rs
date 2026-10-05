//! FBC-f3w's done line: a journaled toy-venue session against local servers reads back, in
//! order, every inbound frame with its stamp, outbound frame, HTTP request and result with
//! headers, timer firing and connection change it saw; and with the journal filled past its
//! soft limit and then its reserve, a Safety-class frame the toy codec emits is still written
//! to the server, the dropped records are counted, and a `Degraded` marker is written once
//! space returns (0006). A venue's endpoints all record into its one journal.

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
    ConnKey, EndpointPlan, Envelope, MdEvent, MdTransport, Stamp, TrafficClass, VenueConfig,
    WallNs, WireUrl,
};
use fbc_journal::{
    BLANK, ControlEvent, HeaderRec, JournalReader, JournalSink, JournalWriter, Marker, QueueSink,
    Record, RecordRef, Recorded, RedactionKey, SinkConfig, journal_queue,
};
use fbc_runtime::{
    Connector, IngestClock, Input, Journal, Liveness, MdSession, MdSessionConfig, MdVenue,
    MdVenueConfig, ProxyConfig, ReconnectPacing, WriteStall,
};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

const CONN: u16 = 5;
const SHARD: u16 = 2;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[3; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn read_all(root: &Path) -> Vec<Record> {
    JournalReader::open(root, SHARD)
        .unwrap()
        .map(Result::unwrap)
        .collect()
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
        conn: CONN,
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

fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

/// A record as one line, its stamp and times left out: what the session did, in order.
fn line(record: &Record) -> String {
    match record {
        Record::Control { ev, .. } => match ev {
            ControlEvent::Opened(k) => format!("open {}", k.epoch),
            ControlEvent::Closed(k) => format!("close {}", k.epoch),
            ControlEvent::Subscribe { conn, add, remove } => {
                let ids = |s: &[fbc_core::Subscription]| {
                    s.iter()
                        .map(|s| s.inst.get().to_string())
                        .collect::<Vec<_>>()
                };
                format!(
                    "subscribe {} +{:?} -{:?}",
                    conn.epoch,
                    ids(add),
                    ids(remove)
                )
            }
        },
        Record::Inbound { stamp, bytes, .. } => {
            format!("in {} {}", stamp.conn.epoch, text(&bytes.0))
        }
        Record::Outbound { conn, frame, .. } => {
            format!("out {} {}", conn.epoch, text(frame.bytes()))
        }
        Record::WriteResult { conn, result, .. } => format!("write {} {result:?}", conn.epoch),
        Record::HttpRequest { conn, tag, req, .. } => {
            let path = req.url.as_str().rsplit_once('/').unwrap().1;
            format!("request {} {} {:?} /{path}", conn.epoch, tag.0, req.method)
        }
        Record::HttpResult { stamp, tag, result } => match result {
            Ok(resp) => format!("result {} {} {}", stamp.conn.epoch, tag.0, resp.status),
            Err(failure) => format!("result {} {} {failure:?}", stamp.conn.epoch, tag.0),
        },
        Record::Timer { stamp, tag } => format!("timer {} {}", stamp.conn.epoch, tag.0),
        Record::Marker(Marker::Degraded { .. }) => "degraded".into(),
        other => format!("{other:?}"),
    }
}

/// The stamp of a record that carries one.
fn stamp_of(record: &Record) -> Option<Stamp> {
    match record {
        Record::Inbound { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::Timer { stamp, .. } => Some(*stamp),
        _ => None,
    }
}

#[tokio::test]
async fn a_journaled_session_reads_back_every_input_output_and_connection_change_in_order() {
    let root = fresh_dir("md_journal_session");
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let sink = Rc::new(RefCell::new(sink));
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let seen = Seen::default();
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), keep(&seen)).unwrap();
    session.set_journal(Journal::new(sink.clone()));
    let (snap, late) = (http.url("/snap"), http.url("/late"));
    let watch = seen.clone();
    let script = async move {
        let mut first = ws.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1");
        assert_eq!(first.recv().await, "sub|add=A");
        first.send("trade|sym=A|px=100|qty=1|seq=1");
        until(|| watch.borrow().len() == 1).await;
        first.send("arm|sym=A|ms=20");
        until(|| watch.borrow().len() == 2).await;
        first.send(&format!("get|tag=1|ms=5000|url={snap}"));
        let head = "HTTP/1.1 200 OK\r\nX-Toy: yes\r\nSet-Cookie: s=secret";
        http.request().await.answer(head, "say").await;
        assert_eq!(first.recv().await, "said");
        // A request with no deadline is not sent: its failure is journaled as its result.
        first.send(&format!("get|tag=3|ms=max|url={snap}"));
        until(|| venue.http_log().len() == 2).await;
        // A request whose answer comes after its stream reconnected.
        first.send(&format!("get|tag=2|ms=5000|url={late}|bye=1"));
        let held = http.request().await;
        assert_eq!(first.next().await, None);
        let mut second = ws.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1");
        assert_eq!(second.recv().await, "sub|add=A");
        held.answer("HTTP/1.1 200 OK", "").await;
        second.send("trade|sym=A|px=101|qty=1|seq=2");
        until(|| watch.borrow().len() == 3).await;
        drop(control);
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(session.stale(Input::Http), 1);
    writer.close().unwrap();
    assert_eq!(sink.borrow().dropped(TrafficClass::Normal), 0);

    let records = read_all(&root);
    let lines: Vec<String> = records.iter().map(line).collect();
    // The late result was answered after the second epoch subscribed and taken before it closed;
    // where it falls among the second epoch's frames is the scheduler's.
    let late_at = lines.iter().position(|l| l == "result 0 2 200").unwrap();
    let subscribed = lines.iter().position(|l| l == "out 1 sub|add=A").unwrap();
    assert!(
        subscribed < late_at && late_at < lines.len() - 1,
        "{lines:#?}"
    );
    let mut rest = lines.clone();
    rest.remove(late_at);
    let snap_get = "in 0 get|tag=1|ms=5000|url=".to_owned() + &http_url(&records, "/snap");
    let unsent_get = "in 0 get|tag=3|ms=max|url=".to_owned() + &http_url(&records, "/snap");
    let late_get =
        "in 0 get|tag=2|ms=5000|url=".to_owned() + &http_url(&records, "/late") + "|bye=1";
    let expected = [
        "open 0",
        "out 0 hello|codec=0|plan=1",
        "write 0 Written",
        "subscribe 0 +[\"1\"] -[]",
        "out 0 sub|add=A",
        "write 0 Written",
        "in 0 trade|sym=A|px=100|qty=1|seq=1",
        "in 0 arm|sym=A|ms=20",
        "timer 0 1",
        &snap_get,
        "request 0 1 Get /snap",
        "result 0 1 200",
        "out 0 said",
        "write 0 Written",
        &unsent_get,
        "request 0 3 Get /snap",
        "result 0 3 NotSent",
        &late_get,
        "request 0 2 Get /late",
        "close 0",
        "open 1",
        "out 1 hello|codec=1|plan=1",
        "write 1 Written",
        "subscribe 1 +[\"1\"] -[]",
        "out 1 sub|add=A",
        "write 1 Written",
        "in 1 trade|sym=A|px=101|qty=1|seq=2",
        "close 1",
    ];
    assert_eq!(rest, expected);

    // Every stamped input took its place in ingest order, and the events the handler got carry
    // the stamps of the inputs journaled for them.
    let stamps: Vec<Stamp> = records.iter().filter_map(stamp_of).collect();
    assert!(stamps.windows(2).all(|w| w[0].ingest_seq < w[1].ingest_seq));
    let seen = seen.borrow();
    for env in seen.iter() {
        assert!(stamps.contains(&env.stamp), "{:?}", env.stamp);
    }
    let conns: Vec<ConnKey> = stamps.iter().map(|s| s.conn).collect();
    assert_eq!(
        conns.first(),
        Some(&ConnKey {
            conn: CONN,
            epoch: 0
        })
    );
    // The result carries its headers; a secret one reads back blanked at its length.
    let Some(Record::HttpResult {
        result: Ok(resp), ..
    }) = records.iter().find(|r| line(r) == "result 0 1 200")
    else {
        panic!("no result");
    };
    let header = |name: &str| resp.headers.iter().find(|h| h.name == name).cloned();
    let toy = HeaderRec {
        name: "x-toy".into(),
        value: "yes".into(),
        redact: false,
        redact_name: false,
    };
    assert_eq!(header("x-toy"), Some(toy));
    let cookie = header("set-cookie").unwrap();
    assert!(cookie.redact && cookie.value.bytes().all(|b| b == BLANK));
    assert_eq!(cookie.value.len(), "s=secret".len());
    assert_eq!(text(&resp.body.0), "say");
    fs::remove_dir_all(&root).unwrap();
}

/// The URL of the journaled request whose path is `path`.
fn http_url(records: &[Record], path: &str) -> String {
    records
        .iter()
        .find_map(|r| match r {
            Record::HttpRequest { req, .. } if req.url.as_str().ends_with(path) => {
                Some(req.url.as_str().to_owned())
            }
            _ => None,
        })
        .unwrap()
}

/// How many records of `class` the sink dropped.
fn dropped(sink: &RefCell<QueueSink>, class: TrafficClass) -> u64 {
    sink.borrow().dropped(class)
}

#[tokio::test]
async fn a_full_journal_never_holds_back_a_safety_frame_and_marks_the_gap_once_space_returns() {
    let root = fresh_dir("md_journal_reserve");
    // Nothing drains the queue until the writer is spawned: a stalled writer.
    let config = SinkConfig {
        budget_bytes: 2048,
        soft_limit_pct: 50,
    };
    let (sink, drain) = journal_queue(config, key()).unwrap();
    let sink = Rc::new(RefCell::new(sink));
    let mut ws = ScriptedWs::start().await;
    let seen = Seen::default();
    let (mut session, control) =
        MdSession::new(session(ToyVenue::leak(), ws.url()), keep(&seen)).unwrap();
    session.set_journal(Journal::new(sink.clone()));
    let (watch, queue) = (seen.clone(), sink.clone());
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // Each cancel's inbound frame is Normal and its outbound frame and write result Safety:
        // the Normal records fill the soft limit, then the Safety ones the reserve.
        let mut n = 0;
        while dropped(&queue, TrafficClass::Safety) == 0 {
            n += 1;
            assert!(n < 1_000, "the reserve was never spent");
            peer.send(&format!("cancel|id={n}"));
            assert_eq!(peer.recv().await, format!("cancel|id={n}"));
        }
        assert!(dropped(&queue, TrafficClass::Normal) > 0);
        // The reserve is spent: the next cancel's records are dropped, and it is still sent.
        let before = dropped(&queue, TrafficClass::Safety);
        peer.send("cancel|id=999");
        assert_eq!(peer.recv().await, "cancel|id=999");
        assert_eq!(dropped(&queue, TrafficClass::Safety), before + 2);
        // The writer resumes and drains the queue; the next record is preceded by the marker.
        let writer = drain
            .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
            .unwrap();
        until(|| queue.borrow().queued_bytes() == 0).await;
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        until(|| watch.borrow().len() == 1).await;
        drop(control);
        assert_eq!(peer.next().await, None);
        (root, writer)
    };
    let (run, (root, writer)) = tokio::join!(session.run(), script);
    run.unwrap();
    writer.close().unwrap();
    let drops = dropped(&sink, TrafficClass::Normal) + dropped(&sink, TrafficClass::Safety);

    let records = read_all(&root);
    let lines: Vec<String> = records.iter().map(line).collect();
    let markers: Vec<(usize, u64, u64)> = records
        .iter()
        .enumerate()
        .filter_map(|(i, r)| match r {
            Record::Marker(Marker::Degraded { from_seq, dropped }) => {
                Some((i, *from_seq, *dropped))
            }
            _ => None,
        })
        .collect();
    let [(at, from_seq, count)] = markers[..] else {
        panic!("{lines:#?}");
    };
    assert_eq!(count, drops);
    assert!(from_seq as usize <= at);
    assert_eq!(lines[at + 1], "in 0 trade|sym=A|px=100|qty=1|seq=1");
    assert_eq!(lines[at + 2..], ["close 0"]);
    // Past the soft limit, a cancel's Safety records were kept from the reserve while its
    // Normal inbound frame was dropped; the last cancel's were dropped too.
    let kept_from_reserve = (1..1_000).any(|k| {
        let out = format!("out 0 cancel|id={k}");
        lines.contains(&out) && !lines.contains(&format!("in 0 cancel|id={k}"))
    });
    assert!(kept_from_reserve, "{lines:#?}");
    assert!(!lines.iter().any(|l| l.ends_with("cancel|id=999")));
    fs::remove_dir_all(&root).unwrap();
}

/// A sink that keeps every record it is offered, in memory.
#[derive(Default)]
struct Kept(Vec<Record>);

impl JournalSink for Kept {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.0.push(record.clone());
        Recorded::Ok
    }
    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

#[tokio::test]
async fn every_endpoint_a_venue_opens_records_into_its_one_journal() {
    let (mut s0, mut s1) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let mut cfg = VenueConfig::new();
    cfg.insert("toy.url.0", &s0.url());
    cfg.insert("toy.url.1", &s1.url());
    let toy_venue = ToyVenue::leak();
    let config = MdVenueConfig {
        venue: toy_venue,
        cfg,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conns: 10..20,
        limiter: toy_venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let (mut venue, control) = MdVenue::new(config, |_| {}).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    venue.set_journal(Journal::new(kept.clone()));
    // Two per endpoint: three subscriptions open two.
    control.set_desired([1, 2, 3].map(toy::sub)).unwrap();
    let script = async move {
        let (mut a, mut b) = (s0.accept().await, s1.accept().await);
        // Which endpoint's codec is built first depends on which connects first: only the
        // plans are fixed.
        let plan = |hello: String| hello.split_once("|plan=").map(|(_, p)| p.to_owned());
        assert_eq!(plan(a.recv().await).as_deref(), Some("1,2"));
        assert_eq!(plan(b.recv().await).as_deref(), Some("3"));
        assert_eq!(a.recv().await, "sub|add=A,B");
        assert_eq!(b.recv().await, "sub|add=C");
        drop(control);
        assert_eq!((a.next().await, b.next().await), (None, None));
    };
    let (run, ()) = tokio::join!(venue.run(), script);
    run.unwrap();
    let lines: Vec<String> = kept
        .borrow()
        .0
        .iter()
        .filter_map(|r| match r {
            Record::Control { ev, .. } => Some(format!("{ev:?}")),
            _ => None,
        })
        .collect();
    for conn in [10, 11] {
        let key = ConnKey { conn, epoch: 0 };
        for ev in [ControlEvent::Opened(key), ControlEvent::Closed(key)] {
            assert!(lines.contains(&format!("{ev:?}")), "{lines:#?}");
        }
    }
}

/// Codex r4178197275: until a codec can mark the credential spans of what comes back (FBC-7lm),
/// a session that has sent a credential journals none of what it receives verbatim.
#[tokio::test]
async fn a_session_that_sent_a_credential_journals_nothing_it_receives_verbatim() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let seen = Seen::default();
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), keep(&seen)).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(kept.clone()));
    let snap = http.url("/snap");
    let watch = seen.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("trade|sym=A|px=1|qty=1|seq=1");
        until(|| watch.borrow().len() == 1).await;
        peer.send("auth");
        assert_eq!(peer.recv().await, "auth|key=toy-secret");
        // The venue echoes the key, in a frame and in a response.
        peer.send("trade|sym=A|px=2|qty=1|seq=2|echo=toy-secret");
        until(|| watch.borrow().len() == 2).await;
        peer.send(&format!("get|tag=1|ms=5000|url={snap}"));
        let head = "HTTP/1.1 200 OK\r\nX-Echo: toy-secret";
        http.request().await.answer(head, "say").await;
        assert_eq!(peer.recv().await, "said");
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let kept = kept.borrow();
    let lines: Vec<String> = kept.0.iter().map(line).collect();
    assert!(lines.contains(&"in 0 trade|sym=A|px=1|qty=1|seq=1".to_owned()));
    // The credential frame is journaled with its span; nothing received after it is.
    assert!(lines.contains(&"out 0 auth|key=toy-secret".to_owned()));
    assert!(lines.contains(&"request 0 1 Get /snap".to_owned()));
    assert!(lines.contains(&"out 0 said".to_owned()));
    let inbound = lines.iter().filter(|l| l.starts_with("in ")).count();
    let results = lines.iter().filter(|l| l.starts_with("result ")).count();
    assert_eq!((inbound, results), (2, 0), "{lines:#?}");
    assert!(
        !lines
            .iter()
            .any(|l| l.starts_with("in ") && l.contains("toy-secret"))
    );
    assert_eq!(session.counters().journal_withheld, 3);
}

/// A session whose endpoint URL carries a credential journals nothing it receives verbatim from
/// its first frame on.
#[tokio::test]
async fn a_session_whose_url_carries_a_credential_journals_nothing_it_receives_verbatim() {
    let mut ws = ScriptedWs::start().await;
    let seen = Seen::default();
    let mut config = session(ToyVenue::leak(), ws.url());
    let url = ws.url();
    let span = (url.len() - "md".len()) as u32..url.len() as u32;
    config.plan.transport = MdTransport::Socket {
        url: WireUrl::redacted(url, vec![span]).unwrap(),
    };
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(kept.clone()));
    let watch = seen.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("trade|sym=A|px=1|qty=1|seq=1");
        until(|| watch.borrow().len() == 1).await;
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let lines: Vec<String> = kept.borrow().0.iter().map(line).collect();
    assert!(!lines.iter().any(|l| l.starts_with("in ")), "{lines:#?}");
    assert!(lines.contains(&"out 0 sub|add=A".to_owned()));
    assert_eq!(session.counters().journal_withheld, 1);
}

/// A sink that takes `pause` to record an HTTP request, as a slow encode would.
struct SlowRequests {
    pause: Duration,
}

impl JournalSink for SlowRequests {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        if let Record::HttpRequest { .. } = record {
            std::thread::sleep(self.pause);
        }
        Recorded::Ok
    }
    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

/// Codex r4178197281: a request's timeout runs from when the codec asked, so time spent
/// journaling it counts against it.
#[tokio::test]
async fn journaling_a_request_counts_against_its_timeout() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let slow = SlowRequests { pause: ms(1_000) };
    session.set_journal(Journal::new(Rc::new(RefCell::new(slow))));
    let slow_url = http.url("/slow");
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        let start = std::time::Instant::now();
        peer.send(&format!("get|tag=2|ms=1500|url={slow_url}"));
        let _held = http.request().await;
        until(|| venue.http_log().len() == 1).await;
        let took = start.elapsed();
        drop(control);
        took
    };
    let (run, took) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(venue.http_log(), ["0/2:TimedOut"]);
    // 1.5 s from the ask, not 1 s of journaling and then 1.5 s more.
    assert!(took >= ms(1_500) && took < ms(2_200), "{took:?}");
}

/// A request carrying a secret header makes the session credentialed: its response is withheld.
#[tokio::test]
async fn a_request_with_a_secret_header_withholds_its_response_from_the_journal() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(kept.clone()));
    let snap = http.url("/snap");
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&format!("get|tag=4|ms=5000|url={snap}|auth=1"));
        let head = "HTTP/1.1 200 OK\r\nX-Echo: toy-token";
        http.request().await.answer(head, "").await;
        until(|| venue.http_log().len() == 1).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let lines: Vec<String> = kept.borrow().0.iter().map(line).collect();
    assert!(lines.contains(&"request 0 4 Get /snap".to_owned()));
    assert!(
        !lines.iter().any(|l| l.starts_with("result ")),
        "{lines:#?}"
    );
    // The request's own frame was journaled before it made the session credentialed.
    assert_eq!(session.counters().journal_withheld, 1);
}

/// A sink that keeps each record's kind only, for frames too large to keep.
#[derive(Default)]
struct Kinds(Vec<String>);

impl JournalSink for Kinds {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        let kind = match record {
            Record::Outbound { frame, .. } => format!("out {}", frame.bytes().len()),
            Record::WriteResult { .. } => "write".into(),
            Record::Control {
                ev: ControlEvent::Closed(_),
                ..
            } => "close".into(),
            _ => "other".into(),
        };
        self.0.push(kind);
        Recorded::Ok
    }
    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

/// A write the control's drop interrupts has no write result: whether it reached the venue is
/// unknown, and the connection's close follows its frame.
#[tokio::test]
async fn an_interrupted_write_is_journaled_without_a_write_result() {
    let mut ws = ScriptedWs::start().await;
    let (mut session, control) =
        MdSession::new(session(ToyVenue::leak(), ws.url()), |_| {}).unwrap();
    let kinds = Rc::new(RefCell::new(Kinds::default()));
    session.set_journal(Journal::new(kinds.clone()));
    let big = 64 * 1024 * 1024;
    let watch = kinds.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // 64 MiB is far more than the loopback socket buffers hold.
        peer.send("big|kb=65536");
        peer.stall();
        until(|| watch.borrow().0.contains(&format!("out {big}"))).await;
        tokio::time::sleep(ms(50)).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let kinds = kinds.borrow();
    assert_eq!(
        kinds.0[kinds.0.len() - 2..],
        [format!("out {big}"), "close".into()]
    );
}

/// A sink that refuses every record offered borrowed, keeping what it stands for, and keeps the
/// kind of every record offered built.
#[derive(Default)]
struct Unbuilt {
    borrowed: Vec<String>,
    built: Vec<String>,
    /// Each response's headers as offered: name and raw value.
    raw: Vec<(String, Vec<u8>)>,
}

impl JournalSink for Unbuilt {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.built.push(line(record));
        Recorded::Ok
    }

    fn record_ref(&mut self, _: TrafficClass, _: WallNs, record: RecordRef<'_>) -> Recorded {
        if let RecordRef::HttpResult { result: Ok(r), .. } = record {
            let raw = r.headers.iter().map(|(n, v)| ((*n).to_owned(), v.to_vec()));
            self.raw.extend(raw);
        }
        match record {
            RecordRef::Owned(record) => self.built.push(line(record)),
            borrowed => self.borrowed.push(line(&borrowed.to_record())),
        }
        Recorded::DroppedCounted
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

/// Codex r4178252055, r4178287660, r4178567381: subscribe calls, inbound frames, HTTP requests
/// and their results are offered borrowed, so a sink with no room refuses them before they are
/// copied, at the length the journal's own encoding of them takes (FBC-f3w: no size is worked
/// out apart from the encoding).
#[tokio::test]
async fn subscribes_frames_requests_and_results_are_offered_borrowed() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let sink = Rc::new(RefCell::new(Unbuilt::default()));
    session.set_journal(Journal::new(sink.clone()));
    let snap = http.url("/snap");
    let get = format!("get|tag=1|ms=5000|url={snap}");
    let frame = get.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&frame);
        http.request().await.answer("HTTP/1.1 200 OK", "say").await;
        assert_eq!(peer.recv().await, "said");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let sink = sink.borrow();
    let borrowed = [
        r#"subscribe 0 +["1"] -[]"#.to_owned(),
        format!("in 0 {get}"),
        "request 0 1 Get /snap".into(),
        "result 0 1 200".into(),
    ];
    assert_eq!(sink.borrowed, borrowed);
    // The response's header values are offered raw, read only once admitted (Codex
    // r4179379938).
    let raw = [
        ("content-length".to_owned(), b"3".to_vec()),
        ("connection".to_owned(), b"close".to_vec()),
    ];
    assert_eq!(sink.raw, raw);
    assert!(!sink.built.iter().any(|l| {
        l.starts_with("in ")
            || l.starts_with("request ")
            || l.starts_with("result ")
            || l.starts_with("subscribe ")
    }));
}

/// Codex r4178287664: an input a credentialed session withholds leaves a gap the journal marks,
/// so a recording of it never reads as complete.
#[tokio::test]
async fn withheld_inputs_leave_a_gap_the_journal_marks() {
    let root = fresh_dir("md_journal_withheld");
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let sink = Rc::new(RefCell::new(sink));
    let mut ws = ScriptedWs::start().await;
    let seen = Seen::default();
    let (mut session, control) =
        MdSession::new(session(ToyVenue::leak(), ws.url()), keep(&seen)).unwrap();
    session.set_journal(Journal::new(sink.clone()));
    let watch = seen.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("auth");
        assert_eq!(peer.recv().await, "auth|key=toy-secret");
        peer.send("trade|sym=A|px=2|qty=1|seq=2|echo=toy-secret");
        until(|| watch.borrow().len() == 1).await;
        peer.send("say");
        assert_eq!(peer.recv().await, "said");
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    writer.close().unwrap();
    assert_eq!(session.counters().journal_withheld, 2);
    let records = read_all(&root);
    let lines: Vec<String> = records.iter().map(line).collect();
    let tail: Vec<&str> = lines[lines.len() - 5..]
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(
        tail,
        [
            "write 0 Written",
            "degraded",
            "out 0 said",
            "write 0 Written",
            "close 0"
        ],
        "{lines:#?}"
    );
    let marker = &records[records.len() - 4];
    assert!(matches!(
        marker,
        Record::Marker(Marker::Degraded { dropped: 2, .. })
    ));
    fs::remove_dir_all(&root).unwrap();
}

/// Codex r4178287660: a credentialed HTTP request is offered borrowed too, so a full journal
/// refuses it before it is cloned; the journal hashes its Authorization value as it encodes it
/// (Codex r4178567377, r4178860509). Its result is withheld unoffered.
#[tokio::test]
async fn a_credentialed_request_is_offered_borrowed_and_its_result_withheld() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let sink = Rc::new(RefCell::new(Unbuilt::default()));
    session.set_journal(Journal::new(sink.clone()));
    let url = http.url("/snap");
    let get = format!("get|tag=1|ms=5000|auth=1|url={url}");
    let frame = get.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&frame);
        http.request().await.answer("HTTP/1.1 200 OK", "").await;
        until(|| venue.http_log().len() == 1).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let borrowed = [
        r#"subscribe 0 +["1"] -[]"#.to_owned(),
        format!("in 0 {get}"),
        "request 0 1 Get /snap".into(),
    ];
    assert_eq!(sink.borrow().borrowed, borrowed);
    assert_eq!(session.counters().journal_withheld, 1);
}

/// Codex r4178646794: a data frame read in the same poll as the control's drop reaches no codec,
/// but the journal still records it, so a recording never silently misses an input the session
/// read. The session's select takes either ready branch at random, so the race is run many
/// times: some runs read the frame first, and each of those must journal it.
#[tokio::test]
async fn a_frame_read_as_the_control_drops_is_journaled_but_reaches_no_codec() {
    const RUNS: usize = 96;
    // The peers run on a runtime of their own, so the session's thread can be blocked while a
    // peer writes.
    let (peers, ready) = std::sync::mpsc::channel();
    let (done, finish) = tokio::sync::oneshot::channel::<()>();
    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            for _ in 0..RUNS {
                peers.send(ScriptedWs::start().await).unwrap();
            }
            let _ = finish.await;
        });
    });
    let mut journaled = 0;
    for _ in 0..RUNS {
        let mut ws = ready.recv().unwrap();
        let seen = Seen::default();
        let (mut session, control) =
            MdSession::new(session(ToyVenue::leak(), ws.url()), keep(&seen)).unwrap();
        let kept = Rc::new(RefCell::new(Kept::default()));
        session.set_journal(Journal::new(kept.clone()));
        let script = async move {
            let mut peer = ws.accept().await;
            assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
            assert_eq!(peer.recv().await, "sub|add=A");
            peer.send("trade|sym=A|px=1|qty=1|seq=1");
            // Blocks the session's thread, so the frame lands unread before the drop.
            std::thread::sleep(ms(25));
            drop(control);
        };
        let (run, ()) = tokio::join!(session.run(), script);
        run.unwrap();
        assert!(seen.borrow().is_empty());
        let kept = kept.borrow();
        journaled += kept.0.iter().filter(|r| line(r).starts_with("in ")).count();
    }
    done.send(()).unwrap();
    server.join().unwrap();
    assert!(journaled > 0, "no run journaled the frame it read");
}

/// Codex r4179310275: a credentialed request's failure holds no byte of the response, so it is
/// journaled like any other, and replay can make the `on_http` call the live codec got; only a
/// response is withheld.
#[tokio::test]
async fn a_credentialed_requests_failure_is_journaled() {
    let mut ws = ScriptedWs::start().await;
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let sink = Rc::new(RefCell::new(Unbuilt::default()));
    session.set_journal(Journal::new(sink.clone()));
    let url = format!("http://127.0.0.1:{}/snap", common::closed_port().await);
    let get = format!("get|tag=1|ms=5000|auth=1|url={url}");
    let frame = get.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&frame);
        until(|| venue.http_log().len() == 1).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let borrowed = sink.borrow().borrowed.clone();
    assert_eq!(borrowed.len(), 4, "{borrowed:?}");
    assert_eq!(borrowed[2], "request 0 1 Get /snap");
    assert!(
        borrowed[3].starts_with("result 0 1 ") && !borrowed[3].ends_with(" 200"),
        "{borrowed:?}"
    );
    assert_eq!(session.counters().journal_withheld, 0);
}

/// A sink that keeps each record's line and drops the session's control as the first
/// connection's opening is journaled.
struct DropsOnOpen {
    control: Option<fbc_runtime::MdControl>,
    lines: Vec<String>,
}

impl JournalSink for DropsOnOpen {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        if let Record::Control {
            ev: ControlEvent::Opened(_),
            ..
        } = record
        {
            self.control = None;
        }
        self.lines.push(line(record));
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

/// Codex r4179310270: a socket the session holds is journaled opened and closed even when the
/// control drops as it opens; the session then builds no codec and sends nothing on it. (The
/// control can be dropped from another thread at any instant; dropping it from inside the
/// journal, as the opening is recorded, puts that instant exactly here.)
#[tokio::test]
async fn a_socket_opened_as_the_control_drops_is_journaled_opened_and_closed() {
    let mut ws = ScriptedWs::start().await;
    let venue = ToyVenue::leak();
    let (mut session, control) = MdSession::new(session(venue, ws.url()), |_| {}).unwrap();
    let sink = Rc::new(RefCell::new(DropsOnOpen {
        control: Some(control),
        lines: Vec::new(),
    }));
    session.set_journal(Journal::new(sink.clone()));
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(sink.borrow().lines, ["open 0", "close 0"]);
    assert_eq!(venue.codecs(), 0);
}

/// Codex r4179379935: a timer that wins the session's select as the control drops reaches no
/// codec, but the journal still records it, so a recording never silently misses an input the
/// session took. The select takes either ready branch at random, so the race is run many
/// times: some runs take the timer, and each of those must journal it.
#[tokio::test]
async fn a_timer_that_fires_as_the_control_drops_is_journaled_but_reaches_no_codec() {
    const RUNS: usize = 64;
    let mut raced = 0;
    for _ in 0..RUNS {
        let mut ws = ScriptedWs::start().await;
        let seen = Seen::default();
        let (mut session, control) =
            MdSession::new(session(ToyVenue::leak(), ws.url()), keep(&seen)).unwrap();
        let kept = Rc::new(RefCell::new(Kept::default()));
        session.set_journal(Journal::new(kept.clone()));
        let watch = kept.clone();
        let script = async move {
            let mut peer = ws.accept().await;
            assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
            assert_eq!(peer.recv().await, "sub|add=A");
            peer.send("arm|sym=A|ms=20");
            // Journaled before it is decoded; decoding arms the timer in the same turn.
            while !watch.borrow().0.iter().any(|r| line(r).starts_with("in ")) {
                tokio::task::yield_now().await;
            }
            // Blocks the session's thread past the timer's due time, so the timer and the
            // drop are both ready when the session next runs.
            std::thread::sleep(ms(60));
            drop(control);
        };
        let (run, ()) = tokio::join!(session.run(), script);
        run.unwrap();
        let kept = kept.borrow();
        let timers = kept
            .0
            .iter()
            .filter(|r| line(r).starts_with("timer "))
            .count();
        let delivered = !seen.borrow().is_empty();
        assert!(
            timers == 1 || !delivered,
            "a timer reached the codec unjournaled"
        );
        if timers == 1 && !delivered {
            raced += 1;
        }
    }
    assert!(
        raced > 0,
        "no run journaled a timer that fired as the control dropped"
    );
}
