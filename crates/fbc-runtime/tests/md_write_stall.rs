//! FBC-ha3's done line: a session whose peer stops reading while a frame is being written ends
//! that epoch as a drop within the consumer's write-stall window (a required setting with no
//! default in code, distinct from the attempt deadline and the silence window) and reconnects
//! through the pacing; and a current-epoch timer that falls due during the stalled write fires,
//! stamped in ingest order, no later than that window.
//!
//! Every test runs on a paused clock that moves only when the test moves it (FBC-3dj), so no
//! assertion depends on wall-clock headroom.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use common::ScriptedWs;
use common::toy::{self, ToyVenue};
use fbc_core::{
    ConnKey, EndpointPlan, Envelope, Feed, FeedHealth, MdEvent, MdTransport, VenueConfig, WireUrl,
};
use fbc_runtime::{
    Connector, IngestClock, Input, Liveness, MdSession, MdSessionConfig, ProxyConfig,
    ReconnectPacing, WriteStall, WriteStallError,
};
use tokio::time::{Instant, advance};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

const CONN: u16 = 5;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

/// A session of a fresh toy venue at `url`, wanting trades on A and B, whose writes may stall
/// for `stall`. A 1 s floor and no attempt deadline, and a silence window and rotation margin
/// too long to matter: none of them is the bound under test.
fn toy_session(url: String, stall: Duration) -> MdSessionConfig {
    let venue = ToyVenue::leak();
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
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
        liveness: Liveness::new(ms(3_600_000), ms(1)).unwrap(),
        write_stall: WriteStall::new(stall).unwrap(),
    }
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

/// Lets the session run, without moving the clock, until `done`.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("not settled");
}

#[tokio::test(start_paused = true)]
async fn a_write_the_peer_stops_reading_ends_its_epoch_at_the_window_and_a_timer_due_meanwhile_fires()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let config = toy_session(server.url(), ms(3_000));
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        // A trade, a timer due in 500 ms, then a 64 MiB frame the peer never reads: far more
        // than the loopback socket buffers hold, so the write waits on the peer.
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        peer.send("arm|sym=A|ms=500");
        peer.send("big|kb=65536");
        let release = peer.hold();
        settle(|| watch.borrow().len() == 1).await;
        churn().await;
        let start = Instant::now();
        // The timer fires while the write still waits: at its own time, not when the write
        // ends.
        advance(ms(499)).await;
        churn().await;
        assert_eq!(watch.borrow().len(), 1);
        advance(ms(1)).await;
        settle(|| watch.borrow().len() == 2).await;
        // The write is abandoned at the window, 3 s after it began, and the epoch ends as a
        // drop: the next attempt waits the pacing's 1 s floor, so it starts 4 s in.
        // Each step lets the session see the clock where the step leaves it.
        for step in [2_499, 1, 999] {
            advance(ms(step)).await;
            churn().await;
            assert!(server.try_accept().is_none());
        }
        advance(ms(1)).await;
        let mut next = None;
        for _ in 0..100_000 {
            next = server.try_accept();
            if next.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let mut next = next.expect("the session reconnected");
        assert_eq!(start.elapsed(), ms(4_000));
        assert_eq!(next.recv().await, "hello|codec=1|plan=1,2");
        assert_eq!(next.recv().await, "sub|add=A,B");
        drop((release, control));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let seen = seen.borrow();
    assert_eq!(seen.len(), 2);
    let (trade, fired) = (&seen[0], &seen[1]);
    assert!(matches!(trade.body, MdEvent::Trade { .. }));
    let stale = MdEvent::Health {
        inst: toy::sub(1).inst,
        feed: Feed::Trades,
        h: FeedHealth::Stale,
    };
    assert_eq!(fired.body, stale);
    // Stamped in ingest order under the epoch that set it: after the trade, the arm and the big
    // frame, 500 ms after they were read.
    assert_eq!(fired.stamp.conn, key(0));
    assert_eq!(fired.stamp.ingest_seq, trade.stamp.ingest_seq + 3);
    let elapsed = fired.stamp.recv_mono.0 - trade.stamp.recv_mono.0;
    assert_eq!(Duration::from_nanos(elapsed), ms(500));
    let counters = session.counters();
    assert_eq!((counters.write_stalls, counters.attempts), (1, 2));
    assert_eq!((counters.silences, counters.failed_attempts), (0, 0));
    assert_eq!(session.stale(Input::Timer), 0);
    assert_eq!(session.current(), key(1));
}

/// Codex r4180583622: a session that first runs again after both a timer and the window are
/// past (a paused clock's jump, a starved executor) honours whichever came first. A timer due
/// after the window does not reach the codec: the epoch ended at the window, and the timer
/// fires into nothing.
#[tokio::test(start_paused = true)]
async fn a_timer_due_after_the_window_does_not_fire_into_the_codec_when_both_are_past() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let config = toy_session(server.url(), ms(3_000));
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        peer.send("arm|sym=A|ms=4000");
        peer.send("big|kb=65536");
        let release = peer.hold();
        churn().await;
        // One step past both the window (3 s) and the timer (4 s).
        advance(ms(5_000)).await;
        churn().await;
        advance(ms(1_000)).await;
        let mut next = server.accept().await;
        assert_eq!(next.recv().await, "hello|codec=1|plan=1,2");
        drop((release, control));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert!(seen.borrow().is_empty());
    assert_eq!(session.counters().write_stalls, 1);
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.current(), key(1));
}

#[tokio::test(start_paused = true)]
async fn a_write_that_completes_within_the_window_keeps_its_epoch() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let config = toy_session(server.url(), ms(3_000));
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        peer.send("big|kb=65536");
        let release = peer.hold();
        churn().await;
        // Released a moment before the window: the frame arrives and the epoch goes on.
        advance(ms(2_999)).await;
        churn().await;
        release.send(()).unwrap();
        assert_eq!(peer.recv().await.len(), 65536 * 1024);
        peer.send("say");
        assert_eq!(peer.recv().await, "said");
        advance(ms(10_000)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(session.counters().write_stalls, 0);
    assert_eq!(session.current(), key(0));
}

/// A timer the ended epoch set that falls due while the next epoch's write waits fires into
/// nothing: stamped, journaled and counted, never handed to the new codec.
#[tokio::test(start_paused = true)]
async fn an_ended_epochs_timer_due_during_a_stalled_write_fires_into_nothing() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let config = toy_session(server.url(), ms(3_000));
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        first.send("arm|sym=A|ms=1500");
        first.send("bye");
        // The reconnect waits the 1 s floor.
        churn().await;
        advance(ms(1_000)).await;
        let mut second = server.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1,2");
        assert_eq!(second.recv().await, "sub|add=A,B");
        second.send("big|kb=65536");
        let release = second.hold();
        churn().await;
        advance(ms(500)).await;
        churn().await;
        release.send(()).unwrap();
        assert_eq!(second.recv().await.len(), 65536 * 1024);
        drop((first, control));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert!(seen.borrow().is_empty());
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.counters().write_stalls, 0);
}

#[test]
fn a_write_stall_window_is_the_consumers_and_zero_is_refused() {
    assert_eq!(WriteStall::new(ms(250)).unwrap().window(), ms(250));
    assert_eq!(WriteStall::new(ms(0)).unwrap_err(), WriteStallError::Zero);
    assert_eq!(
        WriteStallError::Zero.to_string(),
        "the write-stall window is zero"
    );
}
