//! FBC-z2s's done line, over the loopback: fbc-runtime's market-data session runs the poll
//! endpoint `ToyFactory` plans for stats (an `MdTransport::Poll` endpoint, record 0014 item 9,
//! decision 0027), opens no socket, and the toy's poll codec asks the stub venue's HTTP endpoint
//! for each subscribed instrument's stats at once and again every `STATS_EVERY` under its own
//! timer, not a moment before, its answers reaching the handler as `MdEvent::Stats`, until the
//! last stats subscription is removed: then it asks for nothing more (Codex r4172917328).
//!
//! The test runs on a paused clock that moves only when the test moves it: a blocking task that
//! never ends keeps tokio from jumping the clock while socket I/O is under way. Every wait is
//! for something the test can observe, bounded by wall-clock time from another thread so that a
//! regression fails instead of hanging.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fbc_conformance::toy::{
    self, INST_A, INST_B, STATS_EVERY, STATS_STREAM, STATS_URL_KEY, ToyFactory,
};
use fbc_conformance::{HttpReply, HttpRouter, PathPattern, StubServer, WsScript};
use fbc_core::{
    AssetSym, Envelope, Feed, InstrumentId, Lots, MdEvent, MdTransport, Money, Subscription,
    VenueConfig, VenueFactory,
};
use fbc_runtime::http::Method;
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

fn stats(inst: InstrumentId) -> Subscription {
    Subscription {
        inst,
        feed: Feed::Stats,
    }
}

/// The stats the stub answers for `sym`: its volume in nanos of USDC and its open interest.
fn answered(sym: &str) -> (i128, i64) {
    match sym {
        "TOYA-PERP" => (2_500_000_000_000, 40),
        _ => (750_000_000_000, 9),
    }
}

fn event(inst: InstrumentId, sym: &str) -> MdEvent {
    let (vol, oi) = answered(sym);
    MdEvent::Stats {
        inst,
        volume_24h_quote: Some(Money::new(vol, AssetSym::new("USDC").unwrap())),
        oi: Lots::new(oi),
    }
}

#[tokio::test(start_paused = true)]
async fn the_toys_stats_are_polled_under_its_own_timer_until_the_last_subscription_is_removed() {
    let frozen = freeze();
    let stats_route = PathPattern::template("/md/stats/{sym}").unwrap();
    let router = HttpRouter::new().route_fn(Method::GET, stats_route, |req| {
        let sym = req.param("sym").unwrap_or("");
        let (vol, oi) = answered(sym);
        HttpReply {
            status: 200,
            body: format!("stats|sym={sym}|vol={vol}|oi={oi}").into_bytes(),
        }
    });
    let server = StubServer::start(WsScript::new(Vec::new()), router)
        .await
        .unwrap();
    let mut cfg = VenueConfig::new();
    cfg.insert(STATS_URL_KEY, &server.http_url("/md"));
    let specs = toy::specs();
    let subs = BTreeSet::from([stats(INST_A), stats(INST_B)]);
    // The factory plans stats alone on one poll endpoint under the configured base.
    let mut plans = ToyFactory.plan_md(&cfg, &specs, &subs).unwrap();
    assert_eq!(plans.len(), 1, "{plans:?}");
    let plan = plans.remove(0);
    assert_eq!(plan.stream, STATS_STREAM);
    assert!(matches!(plan.transport, MdTransport::Poll { .. }));

    let caps = toy::caps();
    let reserve = SafetyReserve::percent(0).unwrap();
    let config = MdSessionConfig {
        venue: &ToyFactory,
        cfg,
        plan,
        specs,
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

    let requests = || server.http_requests().len();
    let events = || seen.borrow().len();
    let (get_a, get_b) = ("GET /md/stats/TOYA-PERP", "GET /md/stats/TOYB-PERP");
    let drive = async {
        // Both asked for at once.
        until(|| events() == 2).await;
        assert_eq!(requests(), 2);
        for round in 1..=2 {
            // Not a moment before the interval, then both again.
            advance(STATS_EVERY - ms(1)).await;
            churn().await;
            assert_eq!(requests(), 2 * round, "round {round} early");
            advance(ms(1)).await;
            until(|| events() == 2 * (round + 1)).await;
        }
        // One removed: the next round asks for the other alone.
        control.set_desired([stats(INST_B)]);
        churn().await;
        advance(STATS_EVERY).await;
        until(|| events() == 7).await;
        assert_eq!(requests(), 7);
        // The last removed: polling stops, however long the session runs on.
        control.set_desired([]);
        churn().await;
        for _ in 0..4 {
            advance(STATS_EVERY).await;
            churn().await;
        }
        assert_eq!((requests(), events()), (7, 7));
        // Subscribed again: asked for at once.
        control.set_desired([stats(INST_A)]);
        until(|| events() == 8).await;
        drop(control);
    };
    let ((), run) = tokio::join!(drive, session.run());
    run.unwrap();
    drop(frozen);

    let mut asked = server.http_requests();
    // Each round's two requests may arrive in either order.
    for round in asked[..6].chunks_mut(2) {
        round.sort();
        assert_eq!(round, [get_a, get_b]);
    }
    assert_eq!(&asked[6..], [get_b, get_a]);
    let seen = seen.borrow();
    let mut bodies: Vec<MdEvent> = seen.iter().map(|e| e.body).collect();
    let (a, b) = (event(INST_A, "TOYA-PERP"), event(INST_B, "TOYB-PERP"));
    for round in bodies[..6].chunks_mut(2) {
        round.sort_by_key(|ev| matches!(ev, MdEvent::Stats { inst, .. } if *inst == INST_B));
        assert_eq!(round, [a, b]);
    }
    assert_eq!(&bodies[6..], [b, a]);
    // No socket was opened, and the codec asked for nothing the runtime refused.
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.refused_effects), (0, 0));
    assert_eq!(counters.decode_errors, 0);
    assert!(seen.iter().all(|e| e.stamp.conn == session.current()));
}
