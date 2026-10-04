//! FBC-3kz's done line: decoder replay feeds a journal the runtime wrote to the decoders through
//! the same calls the live session made (0006, design §10.1), so a toy-venue session spanning two
//! epochs and an HTTP response replays to the same envelopes (stamps, venue meta and bodies), on
//! every replay. The caller supplies the venue configuration and spec table the session ran with.
//! The rest are its error paths, on hand-built records.

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
    BookId, ConnKey, EndpointPlan, Envelope, FeedHealth, HttpFailure, HttpTag, MdEvent,
    MdTransport, MonoNs, Stamp, Subscription, TimerTag, VenueConfig, WallNs, WireSlice, WireUrl,
};
use fbc_journal::{
    ControlEvent, HeaderRec, HttpResponseRec, JournalError, JournalReader, JournalWriter, Marker,
    Opaque, Opcode, Record, RedactionKey, SinkConfig, WriteRes, journal_queue,
};
use fbc_runtime::{
    Connector, IngestClock, Journal, MdReplay, MdReplayConfig, MdReplayCounters, MdSession,
    MdSessionConfig, ProxyConfig, ReconnectPacing, ReplayError,
};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

const CONN: u16 = 5;
const SHARD: u16 = 3;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[4; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// The endpoint the session ran: trades on instrument 1 at `url`.
fn plan(url: &str) -> EndpointPlan {
    EndpointPlan {
        stream: toy::STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain(url.to_owned()),
        },
        subs: vec![toy::sub(1)],
    }
}

fn session(venue: &'static ToyVenue, url: &str) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: plan(url),
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 64 * 1024,
        conn: CONN,
        limiter: venue.limiter(0),
    }
}

/// What a replay of the session needs from the caller: what the session ran with.
fn replay_config(venue: &'static ToyVenue) -> MdReplayConfig {
    MdReplayConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: plan("ws://127.0.0.1:1/replayed"),
        specs: toy::specs(),
        conn: CONN,
    }
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

/// Replays the journal at `root` into a fresh toy venue: the envelopes, the counts, and the
/// venue (its codecs' plans and `on_http` calls).
fn replay(root: &Path) -> (Vec<Envelope<MdEvent>>, MdReplayCounters, &'static ToyVenue) {
    let venue = ToyVenue::leak();
    let seen = Seen::default();
    let mut replay = MdReplay::new(replay_config(venue), keep(&seen)).unwrap();
    replay
        .run(JournalReader::open(root, SHARD).unwrap())
        .unwrap();
    let counters = replay.counters();
    drop(replay);
    (seen.take(), counters, venue)
}

#[tokio::test]
async fn a_journaled_session_replays_to_the_same_envelopes_every_time() {
    let root = fresh_dir("md_replay_session");
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
    let (mut session, control) = MdSession::new(session(venue, &ws.url()), keep(&seen)).unwrap();
    session.set_journal(Journal::new(sink.clone()));
    let (snap, late) = (http.url("/snap"), http.url("/late"));
    let watch = seen.clone();
    let script = async move {
        let mut first = ws.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1");
        assert_eq!(first.recv().await, "sub|add=A");
        first.send("trade|sym=A|px=100|qty=1|seq=1");
        first.send("arm|sym=A|ms=20");
        until(|| watch.borrow().len() == 2).await;
        // An HTTP response whose body the codec reads as two records.
        first.send(&format!("get|tag=1|ms=5000|url={snap}"));
        let body = "trade|sym=A|px=102|qty=2|seq=2\ntrade|sym=A|px=103|qty=1|seq=3";
        let head = "HTTP/1.1 200 OK\r\nX-Toy: yes\r\nSet-Cookie: s=secret";
        http.request().await.answer(head, body).await;
        until(|| watch.borrow().len() == 4).await;
        // A request answered after its stream reconnected: dropped live, so not replayed.
        first.send(&format!("get|tag=2|ms=5000|url={late}|bye=1"));
        let held = http.request().await;
        assert_eq!(first.next().await, None);
        let mut second = ws.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1");
        assert_eq!(second.recv().await, "sub|add=A");
        held.answer("HTTP/1.1 200 OK", "trade|sym=A|px=1|qty=1|seq=99")
            .await;
        second.send("begin|sym=A|book=0|epoch=1|seq=10");
        second.send("lvl|sym=A|book=0|side=bid|px=99|qty=4|seq=11");
        second.send("end|sym=A|book=0|seq=12");
        second.send("trade|sym=A|px=104|qty=3|seq=4");
        until(|| watch.borrow().len() == 8).await;
        drop(control);
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    writer.close().unwrap();
    let live = seen.take();
    assert_eq!(live.len(), 8);
    assert_eq!(
        live[7].stamp.conn,
        ConnKey {
            conn: CONN,
            epoch: 1
        }
    );

    let (first, counters, replayed) = replay(&root);
    assert_eq!(first, live);
    let (second, again, twice) = replay(&root);
    assert_eq!(second, live);
    assert_eq!(again, counters);
    // Each epoch's codec was built for the plan the live one was, and was handed the HTTP
    // results the live one was, with their headers.
    assert_eq!(replayed.plans(), venue.plans());
    assert_eq!(replayed.plans(), [vec![1], vec![1]]);
    assert_eq!(replayed.http_log(), venue.http_log());
    assert_eq!(twice.http_log(), ["0/1:200:yes"]);
    assert_eq!(
        counters,
        MdReplayCounters {
            epochs: 2,
            frames: 8,
            http: 1,
            timers: 1,
            stale: 1,
            ..MdReplayCounters::default()
        }
    );
    fs::remove_dir_all(&root).unwrap();
}

fn conn(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn stamp(ingest_seq: u64, epoch: u32) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq * 10),
        recv_wall: WallNs(1_000 + ingest_seq as i64),
        conn: conn(epoch),
    }
}

fn opened(epoch: u32) -> Record {
    Record::Control {
        at: MonoNs(0),
        ev: ControlEvent::Opened(conn(epoch)),
    }
}

fn closed(epoch: u32) -> Record {
    Record::Control {
        at: MonoNs(0),
        ev: ControlEvent::Closed(conn(epoch)),
    }
}

fn subscribe(epoch: u32, add: &[u32], remove: &[u32]) -> Record {
    let subs = |ids: &[u32]| {
        ids.iter()
            .copied()
            .map(toy::sub)
            .collect::<Vec<Subscription>>()
    };
    Record::Control {
        at: MonoNs(0),
        ev: ControlEvent::Subscribe {
            conn: conn(epoch),
            add: subs(add),
            remove: subs(remove),
        },
    }
}

fn frame(seq: u64, epoch: u32, text: &str) -> Record {
    Record::Inbound {
        stamp: stamp(seq, epoch),
        opcode: Opcode::Text,
        bytes: Opaque(text.as_bytes().to_vec()),
        redact: Vec::new(),
    }
}

fn result(seq: u64, epoch: u32, tag: u64, body: &str) -> Record {
    Record::HttpResult {
        stamp: stamp(seq, epoch),
        tag: HttpTag(tag),
        result: Ok(HttpResponseRec {
            status: 200,
            headers: vec![HeaderRec {
                name: "x-toy".into(),
                value: format!("r{seq}"),
                redact: false,
                redact_name: false,
            }],
            body: Opaque(body.as_bytes().to_vec()),
            body_redact: Vec::new(),
        }),
    }
}

fn trade(seq: u64) -> String {
    format!("trade|sym=A|px=100|qty=1|seq={seq}")
}

/// Replays `records` into a fresh toy venue.
fn replay_records(
    records: Vec<Record>,
) -> (Vec<Envelope<MdEvent>>, MdReplayCounters, &'static ToyVenue) {
    let venue = ToyVenue::leak();
    let seen = Seen::default();
    let mut replay = MdReplay::new(replay_config(venue), keep(&seen)).unwrap();
    replay.run(records.into_iter().map(Ok)).unwrap();
    let counters = replay.counters();
    drop(replay);
    (seen.take(), counters, venue)
}

/// The venue sequence numbers of `seen`, in order.
fn seqs(seen: &[Envelope<MdEvent>]) -> Vec<Option<u64>> {
    seen.iter().map(|e| e.venue_seq).collect()
}

#[test]
fn an_epochs_codec_is_built_for_its_opening_subscription_and_answers_what_came_before_it() {
    // A result that came back while the codec's opening writes waited is answered by the codec
    // built for the opening call's subscriptions, before that call.
    let (seen, counters, venue) = replay_records(vec![
        opened(0),
        Record::Outbound {
            at: MonoNs(1),
            conn: conn(0),
            rpc: None,
            frame: WireSlice::plain(b"hello".to_vec()),
        },
        result(1, 0, 7, &trade(1)),
        Record::WriteResult {
            at: MonoNs(2),
            conn: conn(0),
            rpc: None,
            result: WriteRes::Written,
        },
        subscribe(0, &[1, 2], &[]),
        frame(2, 0, &trade(2)),
        subscribe(0, &[3], &[2]),
        closed(0),
    ]);
    assert_eq!(seqs(&seen), [Some(1), Some(2)]);
    assert_eq!(seen[0].stamp, stamp(1, 0));
    assert_eq!(venue.plans(), [vec![1, 2]]);
    assert_eq!(venue.http_log(), ["0/7:200:r1"]);
    assert_eq!(counters.epochs, 1);
    assert_eq!(counters.refused_subscribes, 0);
}

#[test]
fn an_epoch_whose_first_input_comes_before_any_subscription_is_built_for_none() {
    let (seen, _, venue) = replay_records(vec![
        opened(0),
        frame(1, 0, &trade(1)),
        subscribe(0, &[1], &[]),
        closed(0),
        // A journal that ends while an epoch's codec waits for its opening call: the results it
        // held are still answered, a failure included.
        opened(1),
        Record::HttpResult {
            stamp: stamp(2, 1),
            tag: HttpTag(3),
            result: Err(HttpFailure::TimedOut),
        },
        result(3, 1, 4, &trade(3)),
    ]);
    assert_eq!(seqs(&seen), [Some(1), Some(3)]);
    assert_eq!(venue.plans(), [vec![], vec![]]);
    assert_eq!(venue.http_log(), ["1/3:TimedOut", "1/4:200:r3"]);
}

#[test]
fn an_ended_epochs_inputs_and_other_connections_records_reach_no_codec() {
    let other = ConnKey {
        conn: CONN + 1,
        epoch: 0,
    };
    let (seen, counters, venue) = replay_records(vec![
        // Before any epoch opened.
        frame(1, 0, &trade(1)),
        subscribe(0, &[1], &[]),
        opened(0),
        subscribe(0, &[1], &[]),
        Record::Timer {
            stamp: stamp(2, 0),
            tag: TimerTag(1),
        },
        // Another connection's records are another session's.
        Record::Control {
            at: MonoNs(0),
            ev: ControlEvent::Opened(other),
        },
        Record::Inbound {
            stamp: Stamp {
                conn: other,
                ..stamp(3, 0)
            },
            opcode: Opcode::Text,
            bytes: Opaque(trade(3).into_bytes()),
            redact: Vec::new(),
        },
        Record::Marker(Marker::Degraded {
            from_seq: 4,
            dropped: 2,
        }),
        Record::Marker(Marker::Recovered),
        Record::Nonce {
            source: fbc_journal::NonceSourceId(0),
            value: 1,
        },
        // A close of an epoch that is not the open one changes nothing.
        closed(7),
        frame(4, 0, &trade(4)),
        // An opening with no close before it ends the open epoch.
        opened(1),
        result(5, 0, 1, &trade(5)),
        Record::Timer {
            stamp: stamp(6, 0),
            tag: TimerTag(1),
        },
        frame(7, 1, &trade(7)),
        closed(1),
        frame(8, 1, &trade(8)),
        subscribe(1, &[2], &[]),
        result(9, 1, 2, &trade(9)),
    ]);
    assert_eq!(seqs(&seen), [None, Some(4), Some(7)]);
    assert_eq!(
        seen[0].body,
        MdEvent::Health {
            inst: fbc_core::InstrumentId::new(1),
            feed: fbc_core::Feed::Trades,
            h: FeedHealth::Stale,
        }
    );
    assert_eq!(venue.plans(), [vec![1], vec![]]);
    assert!(venue.http_log().is_empty());
    assert_eq!(
        counters,
        MdReplayCounters {
            epochs: 2,
            frames: 2,
            http: 0,
            timers: 1,
            stale: 7,
            decode_errors: 0,
            refused_subscribes: 0,
            gaps: 1,
        }
    );
}

#[test]
fn what_the_codec_refuses_is_counted_as_it_was_live() {
    let (seen, counters, _) = replay_records(vec![
        opened(0),
        // Instrument 9 is not in the spec table: the toy refuses the call.
        subscribe(0, &[9], &[]),
        frame(1, 0, "nonsense"),
        Record::Inbound {
            stamp: stamp(2, 0),
            opcode: Opcode::Binary,
            bytes: Opaque(vec![0xFF]),
            redact: Vec::new(),
        },
        result(3, 0, 1, "nonsense"),
        frame(4, 0, "begin|sym=A|book=0|epoch=1|seq=1"),
        closed(0),
    ]);
    assert_eq!(seen.len(), 1);
    assert!(matches!(
        seen[0].body,
        MdEvent::BookSnapshotBegin {
            book: BookId(0),
            ..
        }
    ));
    assert_eq!(counters.refused_subscribes, 1);
    assert_eq!(counters.decode_errors, 3);
    assert_eq!((counters.frames, counters.http), (3, 1));
}

#[test]
fn a_replay_stops_at_a_journal_error_or_a_text_frame_that_is_not_utf8() {
    let venue = ToyVenue::leak();
    let mut replay = MdReplay::new(replay_config(venue), |_: Envelope<MdEvent>| {}).unwrap();
    let records = vec![Ok(opened(0)), Err(JournalError::TooLarge), Ok(closed(0))];
    let err = replay.run(records).unwrap_err();
    assert!(matches!(err, ReplayError::Journal(JournalError::TooLarge)));
    assert_eq!(
        err.to_string(),
        "the journal cannot be read: a journal record is too large for its format"
    );
    assert!(std::error::Error::source(&err).is_some());

    let bad = Record::Inbound {
        stamp: stamp(9, 0),
        opcode: Opcode::Text,
        bytes: Opaque(vec![b'a', 0xFF]),
        redact: Vec::new(),
    };
    let err = replay.feed(&bad).unwrap_err();
    assert!(matches!(err, ReplayError::NotUtf8 { ingest_seq: 9 }));
    assert_eq!(
        err.to_string(),
        "the text frame of ingest sequence 9 is not UTF-8"
    );
    assert!(std::error::Error::source(&err).is_none());
}

#[test]
fn a_replay_refuses_a_configuration_the_venue_refuses() {
    let mut config = replay_config(ToyVenue::leak());
    config.cfg.insert(toy::REFUSE, "1");
    let Err(err) = MdReplay::new(config, |_: Envelope<MdEvent>| {}) else {
        panic!("the configuration was taken");
    };
    assert!(matches!(err, ReplayError::Config(_)));
    assert!(err.to_string().starts_with("venue configuration refused: "));
    assert!(std::error::Error::source(&err).is_some());
}
