//! FBC-ku8's done line: a market-data session drives the toy venue's codec against a local
//! WebSocket server, delivers stamped events in ingest order, opens a fresh epoch after a drop
//! with each subscription sent once, fires an old epoch's timer into nothing, sends only the
//! difference when the desired set changes, and paces attempts against a refusing server.

mod common;

use std::cell::RefCell;
use std::rc::Rc;
use std::thread::{self, ThreadId};
use std::time::Duration;

use common::toy::{self, REFUSE, ToyVenue};
use common::{ScriptedWs, hanging, once_then_hanging, refusing};
use fbc_core::{ConnKey, Envelope, Feed, FeedHealth, InstrumentId, MdEvent, MdTransport, WireUrl};
use fbc_core::{EndpointPlan, VenueConfig};
use fbc_runtime::{
    Connector, IngestClock, Input, MdCounters, MdSession, MdSessionConfig, ProxyConfig,
    ReconnectPacing, SessionError, Step,
};

type Seen = Rc<RefCell<Vec<(ThreadId, Envelope<MdEvent>)>>>;

/// A handler that keeps every envelope and the thread it arrived on.
fn keep(seen: &Seen) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let seen = seen.clone();
    move |env| seen.borrow_mut().push((thread::current().id(), env))
}

/// The connection number the tests stamp the toy's session with.
const CONN: u16 = 7;

/// A session of `venue` at `url` wanting trades on `subs`, directly, paced by `pacing`.
fn session(
    venue: &'static ToyVenue,
    url: String,
    subs: &[u32],
    pacing: ReconnectPacing,
) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
            },
            subs: subs.iter().copied().map(toy::sub).collect(),
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
    }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A fast floor, so a drop reconnects at once in real time.
fn quick() -> ReconnectPacing {
    ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000)).unwrap()
}

/// Waits, in real time, until `done` holds.
async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

#[tokio::test]
async fn events_arrive_stamped_in_ingest_order_and_a_drop_opens_a_fresh_epoch_subscribed_once() {
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let config = session(ToyVenue::leak(), server.url(), &[1, 2], quick());
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,2");
        assert_eq!(first.recv().await, "sub|add=A,B");
        first.send("trade|sym=A|px=100|qty=1|seq=1");
        first.send("arm|sym=A|ms=150");
        first.send_binary(b"\xff");
        first.send("trade|sym=B|px=200|qty=2|seq=2");
        until(|| watch.borrow().len() == 2).await;
        first.drop_conn();

        let mut second = server.accept().await;
        assert_eq!(second.recv().await, "hello|codec=1|plan=1,2");
        assert_eq!(second.recv().await, "sub|add=A,B");
        second.send("arm|sym=B|ms=300");
        second.send("trade|sym=A|px=101|qty=3|seq=3");
        until(|| watch.borrow().len() == 4).await;
        drop(control);
        // The session closed the connection having sent nothing more: each subscription once.
        assert_eq!(second.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();

    let seen = seen.borrow();
    let here = thread::current().id();
    assert!(seen.iter().all(|(thread, _)| *thread == here));
    let envs: Vec<_> = seen.iter().map(|(_, env)| env).collect();
    let conns: Vec<_> = envs.iter().map(|e| e.stamp.conn).collect();
    assert_eq!(conns, [key(0), key(0), key(1), key(1)]);
    let ingest: Vec<_> = envs.iter().map(|e| e.stamp.ingest_seq).collect();
    assert!(ingest.windows(2).all(|w| w[0] < w[1]));
    // Every input took a place in ingest order: four frames and the close frame, then (in either
    // order) two frames and the old timer's dropped firing, then the current timer's firing
    // (Codex r4177068885, r4177205689).
    assert_eq!((ingest[0], ingest[1], ingest[3]), (0, 3, 8));
    assert!(
        envs.windows(2)
            .all(|w| w[0].stamp.recv_mono <= w[1].stamp.recv_mono)
    );
    assert!(envs.iter().all(|e| e.stamp.kernel_rx.is_none()));
    let seqs: Vec<_> = envs.iter().map(|e| e.venue_seq).collect();
    assert_eq!(seqs, [Some(1), Some(2), Some(3), None]);
    // The old epoch's timer (instrument A) fired into nothing; only the current one's reported.
    let stale_b = MdEvent::Health {
        inst: InstrumentId::new(2),
        feed: Feed::Trades,
        h: FeedHealth::Stale,
    };
    assert_eq!(envs[3].body, stale_b);
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.stale(Input::Event), 0);
    assert_eq!(session.current(), key(1));
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.failed_attempts), (2, 0));
    assert_eq!(counters.decode_errors, 1);
}

#[tokio::test]
async fn adding_and_removing_subscriptions_while_connected_sends_only_the_difference() {
    let mut server = ScriptedWs::start().await;
    let config = session(ToyVenue::leak(), server.url(), &[1], quick());
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        control.set_desired([1, 2].map(toy::sub));
        assert_eq!(peer.recv().await, "sub|add=B");
        control.set_desired([2].map(toy::sub));
        assert_eq!(peer.recv().await, "sub|remove=A");
        // Re-adding what is active sends nothing; the next change is the next frame.
        control.set_desired([2].map(toy::sub));
        control.set_desired([2, 3].map(toy::sub));
        assert_eq!(peer.recv().await, "sub|add=C");
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(session.counters().refused_subscribes, 0);
}

#[tokio::test]
async fn a_refused_subscribe_stays_pending_stray_effects_are_refused_and_bye_reconnects() {
    let mut server = ScriptedWs::start().await;
    // Instrument 9 is not in the spec table, so the toy refuses the whole call.
    let slow = ReconnectPacing::new(ms(200), ms(200), 100, Duration::from_secs(60), ms(5_000));
    let slow = slow.unwrap();
    let config = session(ToyVenue::leak(), server.url(), &[1, 9], slow);
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        assert_eq!(first.recv().await, "hello|codec=0|plan=1,9");
        control.set_desired([1].map(toy::sub));
        assert_eq!(first.recv().await, "sub|add=A");
        first.send("odd");
        first.send("arm|sym=A|ms=50");
        first.send("bye");
        assert_eq!(first.next().await, None);
        // While disconnected, the timer fires into nothing and a change waits for the epoch.
        control.set_desired([1, 2].map(toy::sub));
        let mut second = server.accept().await;
        // The new epoch's codec is built for the set wanted now (Codex r4177068881).
        assert_eq!(second.recv().await, "hello|codec=1|plan=1,2");
        assert_eq!(second.recv().await, "sub|add=A,B");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let counters = session.counters();
    assert_eq!(counters.refused_subscribes, 1);
    assert_eq!(counters.refused_effects, 2);
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.current(), key(1));
}

#[tokio::test]
async fn an_attempt_that_does_not_open_by_its_deadline_fails_and_the_control_stops_a_hung_one() {
    let (addr, mut accepts) = hanging().await;
    let pacing = ReconnectPacing::new(ms(50), ms(50), 100, ms(60_000), ms(100)).unwrap();
    let config = session(ToyVenue::leak(), format!("ws://{addr}/md"), &[1], pacing);
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut at = Vec::new();
        while at.len() < 3 {
            at.push(accepts.recv().await.unwrap());
        }
        control.set_desired([1, 2].map(toy::sub));
        tokio::task::yield_now().await;
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    // Each attempt hangs until its 100 ms deadline, then waits the 50 ms floor (Codex
    // r4177068887); real time, so only the lower bound is exact.
    assert!(at.windows(2).all(|w| w[1].duration_since(w[0]) >= ms(150)));
    assert_eq!(session.counters().attempts, 3);
    assert_eq!(session.counters().failed_attempts, 2);
}

#[tokio::test]
async fn dropping_the_control_stops_a_session_whose_write_waits_on_a_peer_that_stopped_reading() {
    let mut server = ScriptedWs::start().await;
    let config = session(ToyVenue::leak(), server.url(), &[1], quick());
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // 64 MiB is far more than the loopback socket buffers hold (Codex r4177113779).
        peer.send("big|kb=65536");
        peer.stall();
        tokio::time::sleep(ms(200)).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // A stop, not a drop: no new epoch was opened for it (Codex r4177205693).
    assert_eq!(session.current(), key(0));
}

#[tokio::test]
async fn control_frames_take_their_place_in_ingest_order() {
    let mut server = ScriptedWs::start().await;
    let seen = Seen::default();
    let config = session(ToyVenue::leak(), server.url(), &[1], quick());
    let (mut session, control) = MdSession::new(config, keep(&seen)).unwrap();
    let watch = seen.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        peer.ping();
        peer.send("trade|sym=A|px=101|qty=1|seq=2");
        until(|| watch.borrow().len() == 2).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // The ping consumed an ingest number though it carried no event (Codex r4177205689).
    let ingest: Vec<_> = seen
        .borrow()
        .iter()
        .map(|(_, e)| e.stamp.ingest_seq)
        .collect();
    assert_eq!(ingest, [0, 2]);
}

#[tokio::test]
async fn an_ended_epochs_timer_fires_into_nothing_while_an_attempt_hangs() {
    let (addr, mut accepts) = once_then_hanging(&["arm|sym=A|ms=200", "bye"]).await;
    let config = session(ToyVenue::leak(), format!("ws://{addr}/md"), &[1], quick());
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        // The second attempt hangs until its 5 s deadline; the timer falls due 200 ms after the
        // first connection asked for it.
        accepts.recv().await.unwrap();
        tokio::time::sleep(ms(600)).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // It fired, stamped and counted, during the attempt (Codex r4177205690).
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.counters().attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn a_refusing_server_sees_attempts_spaced_by_the_backoff_and_within_the_budget() {
    let (addr, mut accepts) = refusing().await;
    // No attempt deadline: on paused time a pending deadline would let the clock jump ahead
    // while the attempt's socket I/O is still under way.
    let pacing = ReconnectPacing::new(ms(1_000), ms(4_000), 3, ms(10_000), Duration::MAX);
    let pacing = pacing.unwrap();
    let config = session(ToyVenue::leak(), format!("ws://{addr}/md"), &[1], pacing);
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut at = Vec::new();
        while at.len() < 7 {
            at.push(accepts.recv().await.unwrap());
        }
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    let since: Vec<_> = at.iter().map(|t| t.duration_since(at[0])).collect();
    // Backoff 1, 2, then 4 s, held to 10 s by a budget of 3 per 10 s, then 4 s at the ceiling.
    assert_eq!(
        since,
        [0, 1_000, 3_000, 10_000, 14_000, 18_000, 22_000].map(ms)
    );
    assert!(
        at.windows(4)
            .all(|w| w[3].duration_since(w[0]) >= ms(10_000))
    );
    let MdCounters {
        attempts,
        failed_attempts,
        ..
    } = session.counters();
    // The seventh may still be failing when the control drops.
    assert_eq!(attempts, 7);
    assert!(failed_attempts >= 6);
}

#[test]
fn a_session_needs_a_socket_url_it_can_open_and_a_configuration_the_venue_accepts() {
    let config = session(ToyVenue::leak(), "http://127.0.0.1:1/".into(), &[], quick());
    let err = MdSession::new(config, |_| {}).err().unwrap();
    assert!(matches!(&err, SessionError::Url(e) if e.step() == Step::Url));
    assert!(
        err.to_string()
            .starts_with("the endpoint cannot be opened: URL failed: ")
    );
    let config = session(
        ToyVenue::leak(),
        "wss://toy.invalid/md".into(),
        &[],
        quick(),
    );
    assert!(MdSession::new(config, |_| {}).is_ok());

    let mut config = session(ToyVenue::leak(), "ws://127.0.0.1:1/".into(), &[], quick());
    config.cfg.insert(REFUSE, "yes");
    let err = MdSession::new(config, |_| {}).err().unwrap();
    assert!(matches!(err, SessionError::Config(_)));
    assert_eq!(
        err.to_string(),
        "venue configuration refused: invalid configuration key toy.refuse: refused"
    );
}
