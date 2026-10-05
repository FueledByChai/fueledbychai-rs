//! FBC-53c's done line: fbc-runtime's market-data session, with the runtime's toy venue, played
//! through the stub's duplicate-ack and silence-after-subscription scripts.
//!
//! The toy codec reacts to each acknowledgement as a real one does: the first marks the
//! subscription live and asks the stub's REST path for a snapshot. The runtime never parses an
//! acknowledgement, so the duplicate-ack test proves the codec-plus-runtime path.
//!
//! Both tests run on a paused clock that moves only when the test moves it: a blocking task that
//! never ends keeps tokio from jumping the clock while socket I/O is under way. Every wait is for
//! something the test can observe, bounded by wall-clock time from another thread so that a
//! regression fails instead of hanging; no assertion depends on how long a wait took.

#[allow(dead_code)]
#[path = "../../fbc-runtime/tests/common/toy.rs"]
mod toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fbc_conformance::{
    Frame, HttpReply, HttpRoutes, StubServer, duplicate_acks, silence_after_ack,
};
use fbc_core::{
    ConnKey, EndpointPlan, Envelope, Feed, FeedHealth, InstrumentId, MdEvent, MdTransport,
    VenueConfig, WireUrl,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdSession, MdSessionConfig, ProxyConfig, ReconnectPacing,
    WriteStall,
};
use tokio::time::{Instant, advance};
use toy::{SNAPSHOT, ToyVenue};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

const CONN: u16 = 3;
/// The consumer's silence window in the silence test.
const SILENCE: Duration = Duration::from_secs(5);

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn trade(sym: &str, px: u32, seq: u64) -> String {
    format!("trade|sym={sym}|px={px}|qty=1|seq={seq}")
}

/// The acknowledgement of instrument `sym`'s subscription.
fn ack(sym: &str) -> Frame {
    Frame::text(format!("ack|sym={sym}"))
}

/// The stub's snapshots: each instrument's last trade, A's seq 1 and B's seq 2.
fn snapshots() -> HttpRoutes {
    let reply = |body: String| HttpReply {
        status: 200,
        body: body.into_bytes(),
    };
    HttpRoutes::from([
        ("/snapshot/A".to_owned(), reply(trade("A", 100, 1))),
        ("/snapshot/B".to_owned(), reply(trade("B", 200, 2))),
    ])
}

/// A session of `venue` on the stub, wanting trades on A and B, its snapshots under the stub's
/// `/snapshot`. A 1 s floor between attempts, and no attempt deadline: a pending deadline would
/// be a timer the test has to step past while the connection opens.
fn session(venue: &'static ToyVenue, server: &StubServer, liveness: Liveness) -> MdSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(SNAPSHOT, &server.http_url("/snapshot"));
    MdSessionConfig {
        venue,
        cfg,
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.ws_url("/md")),
            },
            subs: vec![toy::sub(1), toy::sub(2)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        liveness,
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// A handler that keeps every envelope.
fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets every task run, without moving the clock, until `done`; panics if that takes a minute
/// of wall-clock time, so a regression fails instead of hanging.
async fn until(done: impl Fn() -> bool) {
    let expired = Arc::new(AtomicBool::new(false));
    let flag = expired.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(60));
        flag.store(true, Ordering::SeqCst);
    });
    while !done() {
        assert!(!expired.load(Ordering::SeqCst), "not settled within 60 s");
        tokio::task::yield_now().await;
    }
}

/// The venue sequence numbers of the trades `seen` holds, in order.
fn trade_seqs(seen: &[Envelope<MdEvent>]) -> Vec<u64> {
    let trades = seen
        .iter()
        .filter(|e| matches!(e.body, MdEvent::Trade { .. }));
    trades.map(|e| e.venue_seq.unwrap()).collect()
}

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

#[tokio::test(start_paused = true)]
async fn under_duplicate_acks_the_codec_asks_one_snapshot_each_and_nothing_is_sent_or_delivered_twice()
 {
    let frozen = freeze();
    // The toy's hello and subscribe are read; A and B are each acknowledged twice; then one
    // trade on each is pushed once.
    let data = [
        Frame::text(trade("A", 101, 3)),
        Frame::text(trade("B", 201, 4)),
    ];
    let script = duplicate_acks(2, &[ack("A"), ack("B")], &data);
    let server = StubServer::start(script, snapshots()).await.unwrap();
    let venue = ToyVenue::leak();
    let lax = Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap();
    let seen = Seen::default();
    let (mut session, control) = MdSession::new(session(venue, &server, lax), keep(&seen)).unwrap();

    let watch = seen.clone();
    let drive = async {
        server.finished().await.unwrap();
        // The trades were pushed after the second acknowledgements and the codec reads a
        // connection's frames in order, so once both have reached the handler every
        // acknowledgement has been read and every snapshot it caused asked for; then each
        // snapshot asked for is let land.
        until(|| trade_seqs(&watch.borrow()).ends_with(&[3, 4])).await;
        until(|| venue.http_log().len() == venue.snapshots() as usize).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), drive);
    run.unwrap();
    drop(frozen);

    // Exactly one snapshot request per instrument, asked for and answered.
    assert_eq!(venue.snapshots(), 2);
    assert_eq!(
        sorted(server.http_requests()),
        ["GET /snapshot/A", "GET /snapshot/B"]
    );
    assert_eq!(sorted(venue.http_log()), ["0/1:200:-", "0/2:200:-"]);

    // No second subscribe: the one connection received the hello and one subscribe, from one
    // codec that had one subscribe call.
    let conns = server.connections();
    assert_eq!(conns.len(), 1);
    let sent = [
        Frame::text("hello|codec=0|plan=1,2"),
        Frame::text("sub|add=A,B"),
    ];
    assert_eq!(conns[0].received, sent);
    assert_eq!((venue.codecs(), venue.subscribe_calls()), (1, 1));

    // No duplicated event: each snapshot's trade and each pushed trade once, all on epoch 0.
    let seen = seen.borrow();
    assert_eq!(sorted(trade_seqs(&seen)), [1, 2, 3, 4]);
    assert_eq!(seen.len(), 4);
    assert!(seen.iter().all(|e| e.stamp.conn == key(0)));
    assert_eq!(session.current(), key(0));
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.decode_errors), (1, 0));
    assert_eq!((counters.silences, counters.refused_effects), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_stream_silent_after_its_acks_is_reported_stale_at_the_window_and_reconnected_under_a_new_epoch()
 {
    let frozen = freeze();
    let script = silence_after_ack(2, &[ack("A"), ack("B")]);
    let server = StubServer::start(script, snapshots()).await.unwrap();
    let venue = ToyVenue::leak();
    let liveness = Liveness::new(SILENCE, ms(1)).unwrap();
    let seen = Seen::default();
    let (mut session, control) =
        MdSession::new(session(venue, &server, liveness), keep(&seen)).unwrap();

    let watch = seen.clone();
    let drive = async {
        // Both acknowledgements were read and their snapshots landed, with the clock still.
        until(|| watch.borrow().len() == 2).await;
        // Then nothing arrives: not a moment before the window has run out, the stream is
        // reported stale and its connection let go.
        advance(SILENCE - ms(1)).await;
        churn().await;
        assert_eq!(watch.borrow().len(), 2);
        advance(ms(1)).await;
        until(|| watch.borrow().len() == 4).await;
        let stale = Instant::now();
        // It reconnects through the pacing, the floor after the drop, under epoch 1, whose
        // codec subscribes once and asks for its own snapshots on the new acknowledgements.
        advance(ms(999)).await;
        churn().await;
        assert_eq!(server.connections().len(), 1);
        advance(ms(1)).await;
        until(|| watch.borrow().len() == 6).await;
        server.finished().await.unwrap();
        drop(control);
        stale
    };
    let (run, stale) = tokio::join!(session.run(), drive);
    run.unwrap();
    drop(frozen);

    let seen = seen.borrow();
    let silent = |inst| MdEvent::Health {
        inst: InstrumentId::new(inst),
        feed: Feed::Trades,
        h: FeedHealth::Stale,
    };
    // Epoch 0: both snapshots, then both subscriptions reported stale, one window after them.
    assert_eq!(sorted(trade_seqs(&seen[..2])), [1, 2]);
    assert_eq!([seen[2].body, seen[3].body], [silent(1), silent(2)]);
    assert!(seen[..4].iter().all(|e| e.stamp.conn == key(0)));
    let gap = seen[2].stamp.recv_mono.0 - seen[1].stamp.recv_mono.0;
    assert_eq!(Duration::from_nanos(gap), SILENCE);
    // Epoch 1: its own snapshots, and nothing else.
    assert_eq!(sorted(trade_seqs(&seen[4..])), [1, 2]);
    assert!(seen[4..].iter().all(|e| e.stamp.conn == key(1)));
    assert_eq!(session.current(), key(1));

    // The new connection opened the floor after the alarm and was subscribed once.
    let conns = server.connections();
    assert_eq!(conns.len(), 2);
    assert_eq!(conns[1].accepted_at.duration_since(stale), ms(1_000));
    for (n, conn) in conns.iter().enumerate() {
        let hello = Frame::text(format!("hello|codec={n}|plan=1,2"));
        assert_eq!(
            conn.received,
            [hello, Frame::text("sub|add=A,B")],
            "conn {n}"
        );
    }
    assert_eq!((venue.codecs(), venue.subscribe_calls()), (2, 2));
    assert_eq!(venue.snapshots(), 4);
    let counters = session.counters();
    assert_eq!((counters.silences, counters.attempts), (1, 2));
    assert_eq!((counters.failed_attempts, counters.rotations), (0, 0));
}
