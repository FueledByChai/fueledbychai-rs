//! FBC-u1d's done line, its last clause: the conformance toy's `MdCodec` declares a keepalive
//! (`MdCodec::keepalive`), and fbc-runtime's market-data session, built from `ToyFactory`,
//! sends it to the stub venue on 127.0.0.1 at its interval, not a moment before.
//!
//! The test runs on a paused clock that moves only when the test moves it: a blocking task that
//! never ends keeps tokio from jumping the clock while socket I/O is under way. Every wait is
//! for something the test can observe, bounded by wall-clock time from another thread so that a
//! regression fails instead of hanging.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{self, BOOK, INST_A, KEEPALIVE_EVERY, ToyFactory};
use fbc_conformance::{Frame, HttpRouter, Step, StubServer, WsScript};
use fbc_core::{
    EndpointPlan, Envelope, Feed, MdEvent, MdTransport, StreamId, Subscription, VenueConfig,
    WireUrl,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdSession, MdSessionConfig, ProxyConfig, RateLimiter,
    ReconnectPacing, SafetyReserve, WriteStall,
};
use tokio::time::advance;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
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

#[tokio::test(start_paused = true)]
async fn the_toys_keepalive_reaches_a_local_server_at_its_declared_interval() {
    const PINGS: usize = 3;
    let frozen = freeze();
    // The subscribe and three keepalives are read.
    let mut steps = vec![Step::Accept];
    steps.extend((0..=PINGS).map(|_| Step::Read { conn: 0 }));
    let server = StubServer::start(WsScript::new(steps), HttpRouter::new())
        .await
        .unwrap();
    let caps = toy::caps();
    let reserve = SafetyReserve::percent(0).unwrap();
    let config = MdSessionConfig {
        venue: &ToyFactory,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: StreamId(1),
            transport: MdTransport::Socket {
                url: WireUrl::plain(server.ws_url("/md")),
            },
            subs: vec![Subscription {
                inst: INST_A,
                feed: Feed::Book(BOOK),
            }],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX).unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: 3,
        limiter: RateLimiter::new(&caps.limits, reserve).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let seen = Rc::new(RefCell::new(Vec::<Envelope<MdEvent>>::new()));
    let keep = seen.clone();
    let (mut session, control) =
        MdSession::new(config, move |env| keep.borrow_mut().push(env)).unwrap();

    let received = || server.connections().first().map_or(0, |c| c.received.len());
    let drive = async {
        until(|| received() == 1).await;
        for n in 1..=PINGS {
            // Not a moment before the interval.
            advance(KEEPALIVE_EVERY - ms(1)).await;
            churn().await;
            assert_eq!(received(), n, "keepalive {n} early");
            advance(ms(1)).await;
            until(|| received() == n + 1).await;
        }
        server.finished().await.unwrap();
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), drive);
    run.unwrap();
    drop(frozen);

    let conns = server.connections();
    assert_eq!(conns.len(), 1);
    let mut sent = vec![Frame::text("sub|add=TOYA-PERP@0")];
    sent.extend((0..PINGS).map(|_| Frame::text("ping")));
    assert_eq!(conns[0].received, sent);
    let counters = session.counters();
    assert_eq!((counters.keepalives, counters.attempts), (PINGS as u64, 1));
    assert_eq!(counters.refused_effects, 0);
    assert!(seen.borrow().is_empty());
}
