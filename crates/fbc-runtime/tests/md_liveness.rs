//! FBC-djl's done line: the toy codec's keepalive reaches a local server at its declared
//! interval; a toy venue declaring a short `max_conn_lifetime` is rotated to its next epoch
//! before the limit with its desired subscriptions sent exactly once; and a stream whose server
//! goes silent after acknowledging a subscription is reported stale within the configured window
//! and reconnected under a new epoch.
//!
//! Every test runs on a paused clock that moves only when the test moves it (FBC-3dj): a
//! blocking task that never ends keeps tokio from jumping the clock while socket I/O is under
//! way, so no assertion depends on wall-clock headroom.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use common::toy::{self, KEEPALIVE, LIFETIME_MS, ToyVenue};
use common::{PING, PONG, ScriptedHttp, ScriptedWs};
use fbc_core::{
    ConnKey, EndpointPlan, Envelope, FeedHealth, LimitScope, MdEvent, MdTransport, OpKind,
    RateCharge, RateLimit, TagSet, TrafficClass, VenueConfig, WireSlice, WireUrl,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, LivenessError, MdControl, MdHandler, MdSession,
    MdSessionConfig, MdVenue, MdVenueConfig, Outbox, ProxyConfig, ReconnectPacing, SessionError,
};
use tokio::time::{Instant, advance};

type Seen = Rc<RefCell<Vec<Envelope<MdEvent>>>>;

/// A handler that keeps every envelope; every session here takes one, so the session's code is
/// built for one handler type.
fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push(env)
}

const CONN: u16 = 3;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

/// A 1 s floor, and no attempt deadline: a pending deadline would be a timer the test has to
/// step past while the connection opens.
fn pacing() -> ReconnectPacing {
    ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX).unwrap()
}

/// A session of a fresh toy venue at `url`, configured by `cfg`, wanting trades on A and B.
fn toy_session(url: String, cfg: &[(&'static str, &str)], liveness: Liveness) -> MdSessionConfig {
    let mut venue_cfg = VenueConfig::new();
    for (k, v) in cfg {
        venue_cfg.insert(k, v);
    }
    let venue = ToyVenue::leak();
    MdSessionConfig {
        venue,
        cfg: venue_cfg,
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
            },
            subs: vec![toy::sub(1), toy::sub(2)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: pacing(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        liveness,
    }
}

/// A silence window and rotation margin too long to matter.
fn lax() -> Liveness {
    Liveness::new(ms(3_600_000), ms(1)).unwrap()
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
async fn the_codecs_ping_keepalive_reaches_the_server_at_its_declared_interval() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let config = toy_session(server.url(), &[(KEEPALIVE, "ping:1000")], lax());
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        let (opened, sub) = peer.next_at().await.unwrap();
        assert_eq!(sub, "sub|add=A,B");
        let mut at = Vec::new();
        for _ in 0..3 {
            // Not a moment before the interval.
            advance(ms(999)).await;
            churn().await;
            assert!(peer.quiet());
            advance(ms(1)).await;
            let (when, what) = peer.next_at().await.unwrap();
            assert_eq!(what, PING);
            at.push(when.duration_since(opened));
        }
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(at, [1_000, 2_000, 3_000].map(ms));
    let counters = session.counters();
    assert_eq!((counters.keepalives, counters.silences), (3, 0));
}

#[tokio::test(start_paused = true)]
async fn a_frame_keepalive_is_the_codecs_own_frame_and_a_zero_interval_is_refused() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let config = toy_session(server.url(), &[(KEEPALIVE, "frame:500")], lax());
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        let (opened, _) = peer.next_at().await.unwrap();
        advance(ms(500)).await;
        let (when, what) = peer.next_at().await.unwrap();
        assert_eq!(
            (what.as_str(), when.duration_since(opened)),
            ("ka", ms(500))
        );
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(session.counters().keepalives, 1);

    // A keepalive every 0 ms would never let the session read: a codec defect, refused and
    // counted, and the epoch runs without one.
    let mut server = ScriptedWs::start().await;
    let config = toy_session(server.url(), &[(KEEPALIVE, "ping:0")], lax());
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        advance(ms(10_000)).await;
        peer.send("say");
        assert_eq!(peer.recv().await, "said");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let counters = session.counters();
    assert_eq!((counters.keepalives, counters.refused_effects), (0, 1));
}

#[tokio::test(start_paused = true)]
async fn a_ping_is_charged_its_keepalives_charge_and_one_the_buckets_refuse_is_not_sent() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    // Three control frames a minute on a connection: the hello and two pings.
    let venue = ToyVenue::with_limits(vec![RateLimit {
        scope: LimitScope::Connection,
        ops: TagSet::of(&[OpKind::Control]),
        per: ms(60_000),
        units: 3,
    }]);
    let rates = venue.limiter(0);
    let mut config = toy_session(server.url(), &[(KEEPALIVE, "ping:1000")], lax());
    (config.venue, config.limiter) = (venue, rates.clone());
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        for _ in 0..2 {
            advance(ms(1_000)).await;
            assert_eq!(peer.recv().await, PING);
        }
        advance(ms(1_000)).await;
        churn().await;
        assert!(peer.quiet());
        assert_eq!(rates.counts().refused.connection, 1);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(session.counters().keepalives, 3);
}

#[tokio::test(start_paused = true)]
async fn a_venue_with_a_short_lifetime_is_rotated_before_it_with_each_subscription_sent_once() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    // The venue closes a connection 10 s after it opens; the consumer's margin is 2 s.
    let liveness = Liveness::new(ms(3_600_000), ms(2_000)).unwrap();
    let config = toy_session(server.url(), &[(LIFETIME_MS, "10000")], liveness);
    let (mut session, control) = MdSession::new(config, keep(&Seen::default())).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let (opened, hello) = first.next_at().await.unwrap();
        assert_eq!(hello, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        advance(ms(7_999)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        assert!(first.quiet());
        // 8 s after it opened, the next epoch opens at once, without waiting the 1 s floor of
        // a drop, and the old connection is closed.
        advance(ms(1)).await;
        let mut second = server.accept().await;
        let (rotated, hello) = second.next_at().await.unwrap();
        assert_eq!(hello, "hello|codec=1|plan=1,2");
        assert_eq!(rotated.duration_since(opened), ms(8_000));
        assert_eq!(first.next().await, None);
        assert_eq!(second.recv().await, "sub|add=A,B");
        churn().await;
        drop(control);
        // Nothing more was sent: each subscription once on the new epoch.
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(session.current(), key(1));
    let counters = session.counters();
    assert_eq!((counters.rotations, counters.attempts), (1, 2));
    assert_eq!((counters.silences, counters.failed_attempts), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_stream_silent_after_its_subscription_is_reported_stale_and_reconnected_as_paced() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let liveness = Liveness::new(ms(5_000), ms(1)).unwrap();
    let config = toy_session(server.url(), &[], liveness);
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        // The server acknowledges with a trade, then sends only pings, each within the window:
        // a heartbeat keeps the stream alive.
        first.send("trade|sym=A|px=100|qty=1|seq=1");
        settle(|| watch.borrow().len() == 1).await;
        for _ in 0..2 {
            advance(ms(4_000)).await;
            first.ping();
            assert_eq!(first.recv().await, PONG);
        }
        // Then nothing: 5 s after the last ping, not a moment before, the stream is reported
        // stale and closed.
        advance(ms(4_999)).await;
        churn().await;
        assert_eq!(watch.borrow().len(), 1);
        assert!(first.quiet());
        advance(ms(1)).await;
        settle(|| watch.borrow().len() == 3).await;
        assert_eq!(first.next().await, None);
        let stale = Instant::now();
        // It reconnects through the pacing: the floor after a drop.
        advance(ms(999)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        advance(ms(1)).await;
        let mut second = server.accept().await;
        let (when, hello) = second.next_at().await.unwrap();
        assert_eq!(hello, "hello|codec=1|plan=1,2");
        assert_eq!(when.duration_since(stale), ms(1_000));
        assert_eq!(second.recv().await, "sub|add=A,B");
        drop(control);
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let seen = seen.borrow();
    assert_eq!(seen.len(), 3);
    let silent = |inst| MdEvent::Health {
        inst: fbc_core::InstrumentId::new(inst),
        feed: fbc_core::Feed::Trades,
        h: FeedHealth::Stale,
    };
    assert_eq!([seen[1].body, seen[2].body], [silent(1), silent(2)]);
    // One input, the alarm, stamped on the silent epoch 13 s after the trade: 4 + 4 + 5.
    assert!(seen.iter().all(|e| e.stamp.conn == key(0)));
    assert_eq!(seen[1].stamp, seen[2].stamp);
    assert!(seen[1].stamp.ingest_seq > seen[0].stamp.ingest_seq);
    let gap = seen[1].stamp.recv_mono.0 - seen[0].stamp.recv_mono.0;
    assert_eq!(Duration::from_nanos(gap), ms(13_000));
    assert_eq!(session.current(), key(1));
    let counters = session.counters();
    assert_eq!((counters.silences, counters.attempts), (1, 2));
    assert_eq!(counters.rotations, 0);
}

#[tokio::test(start_paused = true)]
async fn only_a_frame_heard_restarts_the_window_and_each_epoch_starts_its_own() {
    let frozen = freeze();
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let seen = Seen::default();
    let liveness = Liveness::new(ms(5_000), ms(1)).unwrap();
    let config = toy_session(server.url(), &[], liveness);
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        // The last frames heard: the codec arms a timer, which fires 1 s later, and asks for a
        // GET, answered 2 s later.
        first.send("arm|sym=A|ms=1000");
        first.send(&format!("get|tag=1|ms=60000|url={}", http.url("/snap")));
        let asked = http.request().await;
        advance(ms(1_000)).await;
        settle(|| watch.borrow().len() == 1).await;
        advance(ms(1_000)).await;
        asked.answer("HTTP/1.1 200 OK", "").await;
        // The session writes a subscribe of its own. Neither that, the timer nor the answer
        // was heard.
        control.set_desired([1, 2, 3].map(toy::sub));
        assert_eq!(first.recv().await, "sub|add=C");
        advance(ms(2_999)).await;
        churn().await;
        assert_eq!(watch.borrow().len(), 1);
        advance(ms(1)).await;
        settle(|| watch.borrow().len() == 4).await;
        assert_eq!(first.next().await, None);
        advance(ms(1_000)).await;
        let mut second = server.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1,2,3");
        assert_eq!(second.recv().await, "sub|add=A,B,C");
        // The new epoch's window counts from its own open.
        advance(ms(4_999)).await;
        churn().await;
        assert_eq!(watch.borrow().len(), 4);
        advance(ms(1)).await;
        settle(|| watch.borrow().len() == 7).await;
        assert_eq!(second.next().await, None);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let seen = seen.borrow();
    // The codec's own report on the first epoch, then the alarm's on each, for every
    // subscription wanted.
    let conns: Vec<_> = seen.iter().map(|e| e.stamp.conn).collect();
    assert_eq!(conns, [0, 0, 0, 0, 1, 1, 1].map(key));
    let insts: Vec<_> = seen
        .iter()
        .map(|e| match e.body {
            MdEvent::Health {
                inst,
                h: FeedHealth::Stale,
                ..
            } => inst.get(),
            _ => 0,
        })
        .collect();
    assert_eq!(insts, [1, 1, 2, 3, 1, 2, 3]);
    assert_eq!(session.counters().silences, 2);
}

/// Codex r4179959341: a change of the desired set that arrives as the window runs out is what
/// the alarm reports, whichever the session sees first.
#[tokio::test(start_paused = true)]
async fn the_alarm_reports_the_set_wanted_when_it_rings() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let liveness = Liveness::new(ms(5_000), ms(1)).unwrap();
    let config = toy_session(server.url(), &[], liveness);
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        // A write the peer holds back keeps the session busy while A is dropped and C added,
        // and the window runs out: both are due when the write ends.
        peer.send("big|kb=65536");
        let release = peer.hold();
        churn().await;
        control.set_desired([2, 3].map(toy::sub));
        advance(ms(6_000)).await;
        churn().await;
        release.send(()).unwrap();
        assert_eq!(peer.recv().await.len(), 65536 * 1024);
        settle(|| watch.borrow().len() == 2).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let insts: Vec<_> = seen
        .borrow()
        .iter()
        .map(|e| match e.body {
            MdEvent::Health { inst, .. } => inst.get(),
            _ => 0,
        })
        .collect();
    assert_eq!(insts, [2, 3]);
    assert_eq!(session.counters().silences, 1);
}

/// Codex r4180000633: a handler that answers a stale report by changing the desired set, as a
/// consumer that resubscribes elsewhere would, does so while the alarm is still reporting. A
/// session that held the set's lock meanwhile would deadlock: the test runs it on a thread of
/// its own and fails, rather than hangs, if it does not finish.
#[test]
fn a_handler_may_change_the_desired_set_from_a_stale_report() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(resubscribe_on_stale());
        let _ = done.send(());
    });
    finished
        .recv_timeout(Duration::from_secs(60))
        .expect("the session finished");
}

/// A consumer's handler that answers each stale report by wanting C instead, and by issuing a
/// write, which the session must send on neither the closing connection nor the next.
struct Resubscribe {
    control: Rc<RefCell<Option<MdControl>>>,
    reports: Rc<RefCell<u32>>,
}

impl MdHandler for Resubscribe {
    fn on_md(&mut self, _: Envelope<MdEvent>) {}

    fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
        if let (MdEvent::Health { .. }, Some(control)) = (env.body, self.control.borrow().as_ref())
        {
            *self.reports.borrow_mut() += 1;
            control.set_desired([toy::sub(3)]);
            let charge = RateCharge::one(OpKind::Control, None);
            out.send(
                WireSlice::plain(b"resub".to_vec()),
                TrafficClass::Normal,
                charge,
            );
        }
    }
}

async fn resubscribe_on_stale() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let liveness = Liveness::new(ms(5_000), ms(1)).unwrap();
    let config = toy_session(server.url(), &[], liveness);
    let slot: Rc<RefCell<Option<MdControl>>> = Rc::default();
    let reports = Rc::new(RefCell::new(0));
    let handler = Resubscribe {
        control: slot.clone(),
        reports: reports.clone(),
    };
    let (mut session, control) = MdSession::new(config, handler).unwrap();
    *slot.borrow_mut() = Some(control);
    let script = async {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        churn().await;
        advance(ms(5_000)).await;
        // Nothing more on the silent connection.
        assert_eq!(first.next().await, None);
        churn().await;
        advance(ms(1_000)).await;
        let mut second = server.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=3");
        assert_eq!(second.recv().await, "sub|add=C");
        // Nor on the next, when the session next writes for an input.
        second.send("say|id=1");
        assert_eq!(second.recv().await, "said|id=1");
        slot.borrow_mut().take();
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    // Both of the silent epoch's subscriptions were reported, and each write refused.
    assert_eq!(*reports.borrow(), 2);
    assert_eq!(session.counters().refused_effects, 2);
}

/// Codex r4180246768: a window that runs out as the connection is due to rotate is a silence,
/// reported stale and reconnected as paced, never a planned rotation. Which of two deadlines due
/// at once the session sees first is chance, so it is tried on many epochs.
#[tokio::test(start_paused = true)]
async fn a_window_that_runs_out_as_the_connection_rotates_is_silence() {
    const EPOCHS: u32 = 24;
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    // The venue closes a connection 10 s after it opens and the margin is 2 s: rotation is due
    // 8 s after the open, as the 8 s window runs out.
    let liveness = Liveness::new(ms(8_000), ms(2_000)).unwrap();
    let config = toy_session(server.url(), &[(LIFETIME_MS, "10000")], liveness);
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        for epoch in 0..EPOCHS {
            assert_eq!(peer.recv().await, format!("hello|codec={epoch}|plan=1,2"));
            assert_eq!(peer.recv().await, "sub|add=A,B");
            advance(ms(8_000)).await;
            let reports = 2 * (epoch as usize + 1);
            settle(|| watch.borrow().len() == reports).await;
            assert_eq!(peer.next().await, None);
            // A drop waits the pacing's floor; a rotation would have reconnected at once.
            churn().await;
            assert!(server.try_accept().is_none());
            advance(ms(1_000)).await;
            peer = server.accept().await;
        }
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert!(seen.borrow().iter().all(|e| matches!(
        e.body,
        MdEvent::Health {
            h: FeedHealth::Stale,
            ..
        }
    )));
    let counters = session.counters();
    assert_eq!(
        (counters.silences, counters.rotations),
        (u64::from(EPOCHS), 0)
    );
}

#[tokio::test(start_paused = true)]
async fn a_frame_that_waited_behind_a_held_write_is_heard_not_silence() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let liveness = Liveness::new(ms(5_000), ms(1)).unwrap();
    let config = toy_session(server.url(), &[], liveness);
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(peer.recv().await, "sub|add=A,B");
        // A 64 MiB frame the peer holds back, and a trade that arrives behind it while the
        // session's write waits; the window runs out meanwhile.
        peer.send("big|kb=65536");
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        let release = peer.hold();
        churn().await;
        advance(ms(6_000)).await;
        churn().await;
        release.send(()).unwrap();
        assert_eq!(peer.recv().await.len(), 65536 * 1024);
        settle(|| !watch.borrow().is_empty()).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let seen = seen.borrow();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].venue_seq, Some(1));
    assert_eq!(session.counters().silences, 0);
    assert_eq!(session.current(), key(0));
}

#[test]
fn a_liveness_needs_a_window_and_a_margin_and_the_margin_must_fit_the_venues_lifetime() {
    assert_eq!(
        Liveness::new(ms(0), ms(1)).unwrap_err(),
        LivenessError::ZeroSilence
    );
    assert_eq!(
        LivenessError::ZeroSilence.to_string(),
        "the silence window is zero"
    );
    assert_eq!(
        Liveness::new(ms(1), ms(0)).unwrap_err(),
        LivenessError::ZeroMargin
    );
    assert_eq!(
        LivenessError::ZeroMargin.to_string(),
        "the rotation margin is zero"
    );
    let liveness = Liveness::new(ms(7), ms(3)).unwrap();
    assert_eq!(
        (liveness.silence(), liveness.rotation_margin()),
        (ms(7), ms(3))
    );

    // A margin as long as the venue's lifetime would rotate the moment a connection opens.
    let tight = Liveness::new(ms(1_000), ms(10_000)).unwrap();
    let url = "ws://127.0.0.1:1/md".to_owned();
    let config = toy_session(url.clone(), &[(LIFETIME_MS, "10000")], tight);
    let err = MdSession::new(config, keep(&Seen::default()))
        .err()
        .unwrap();
    assert_eq!(
        err,
        SessionError::Liveness(LivenessError::MarginNotBelowLifetime)
    );
    assert_eq!(
        err.to_string(),
        "the rotation margin is not below the venue's connection lifetime"
    );
    let config = toy_session(url.clone(), &[(LIFETIME_MS, "10001")], tight);
    assert!(MdSession::new(config, keep(&Seen::default())).is_ok());
    // A venue with no lifetime takes any margin.
    assert!(MdSession::new(toy_session(url.clone(), &[], tight), keep(&Seen::default())).is_ok());
    // A poll endpoint opens no connection to rotate, so the margin is not checked against the
    // lifetime (Codex r4180333662).
    let mut poll = toy_session(url, &[(LIFETIME_MS, "10000")], tight);
    poll.plan.transport = MdTransport::Poll {
        base_url: WireUrl::plain("http://127.0.0.1:1".to_owned()),
    };
    assert!(MdSession::new(poll, keep(&Seen::default())).is_ok());

    let venue = |lifetime: &str| {
        let mut cfg = VenueConfig::new();
        cfg.insert(LIFETIME_MS, lifetime);
        let venue = ToyVenue::leak();
        MdVenueConfig {
            venue,
            cfg,
            specs: toy::specs(),
            connector: Connector::new(ProxyConfig::Direct),
            pacing: pacing(),
            clock: IngestClock::new(),
            http_max_body: 1024,
            conns: 0..4,
            limiter: venue.limiter(0),
            liveness: tight,
        }
    };
    let err = MdVenue::new(venue("10000"), keep(&Seen::default()))
        .err()
        .unwrap();
    assert_eq!(
        err,
        SessionError::Liveness(LivenessError::MarginNotBelowLifetime)
    );
    assert!(MdVenue::new(venue("20000"), keep(&Seen::default())).is_ok());
}
