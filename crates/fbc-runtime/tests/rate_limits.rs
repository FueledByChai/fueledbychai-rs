//! FBC-bel's done line: with the toy venue declaring Account, Ip, Pair, Connection and Connect
//! limits, each tagged effect is charged its weight to the matching buckets only; normal
//! requests stop at the configured reserve while safety requests pass until the bucket is
//! empty; refusals and 429 and 418 responses are counted under the scope charged; a refused
//! HTTP request comes back to `on_http` as NotSent and a refused subscription goes once its
//! bucket refills; connection attempts beyond a Connect limit wait for the bucket; and an
//! AddressVolume limit is refused at start.

mod common;

use std::num::NonZeroU32;
use std::ops::Range;
use std::time::Duration;

use common::toy::{self, ToyVenue};
use common::{PONG, ScriptedHttp, ScriptedWs, refusing};
use fbc_core::{
    ConnKey, EndpointPlan, InstrumentId, LimitScope, MdTransport, OpKind, RateCharge, RateLimit,
    TagSet, VenueConfig, WireUrl,
};
use fbc_runtime::{
    BucketKey, Connector, IngestClock, Liveness, MdSession, MdSessionConfig, MdVenue,
    MdVenueConfig, ProxyConfig, RateCounts, RateError, RateLimiter, ReconnectPacing, SafetyReserve,
    ScopeCounts, SessionError,
};
use tokio::time::Instant;

const CONN: u16 = 5;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn limit(scope: LimitScope, ops: &[OpKind], units: u32, per: Duration) -> RateLimit {
    RateLimit {
        scope,
        ops: TagSet::of(ops),
        per,
        units,
    }
}

/// The toy's limits, each over a minute, so none refills while a test runs.
const ACCOUNT: usize = 0;
const IP_REST: usize = 1;
const PAIR: usize = 2;
const CONNECTION: usize = 3;
const IP_CONNECT: usize = 4;

fn limits() -> Vec<RateLimit> {
    let minute = Duration::from_secs(60);
    vec![
        limit(
            LimitScope::Account,
            &[OpKind::Place, OpKind::Cancel],
            10,
            minute,
        ),
        limit(LimitScope::Ip, &[OpKind::Rest], 6, minute),
        limit(
            LimitScope::Pair,
            &[OpKind::Place, OpKind::Subscribe],
            5,
            minute,
        ),
        limit(
            LimitScope::Connection,
            &[OpKind::Subscribe, OpKind::Control, OpKind::Place],
            50,
            minute,
        ),
        limit(LimitScope::Ip, &[OpKind::Connect], 5, minute),
    ]
}

fn pacing() -> ReconnectPacing {
    ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap()
}

/// A session of `venue` at `url` wanting trades on instrument 1, charging `limiter`.
fn session(venue: &'static ToyVenue, url: String, limiter: RateLimiter) -> MdSessionConfig {
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
        pacing: pacing(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter,
        liveness: no_alarm(),
    }
}

/// No silence window: on paused time a pending window would let the clock jump ahead during
/// socket I/O (FBC-djl).
fn no_alarm() -> Liveness {
    Liveness::new(Duration::MAX, ms(1)).unwrap()
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

/// Holds tokio's paused clock still until dropped: a paused clock otherwise jumps to its next
/// timer whenever the runtime waits on socket I/O, and does not while a blocking task runs
/// (tokio's test-util). Time then moves only by `tokio::time::advance`, so no machine load can
/// let a bucket refill early or late.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets the session run, without moving the clock, until `done`; what it waits for needs no
/// socket I/O, so it comes within a few turns of the runtime or not at all.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("not settled");
}

fn inst(n: u32) -> InstrumentId {
    InstrumentId::new(n)
}

#[tokio::test]
async fn each_tagged_effect_is_charged_its_weight_to_the_matching_buckets_only() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::with_limits(limits());
    let rates = venue.limiter(0);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let conn = ConnKey {
        conn: CONN,
        epoch: 0,
    };
    let used = |limit, key| rates.used(Instant::now(), limit, key);
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // A place of weight 3 on B; a REST request of weight 5; a cancel naming no instrument.
        peer.send("say|id=1|op=place|sym=B|w=3");
        assert_eq!(peer.recv().await, "said|id=1");
        peer.send(&format!(
            "get|tag=1|ms=5000|url={}|op=rest|w=5",
            http.url("/x")
        ));
        http.request().await.answer("HTTP/1.1 200 OK", "").await;
        until(|| venue.http_log().len() == 1).await;
        peer.send("say|id=2|op=cancel");
        assert_eq!(peer.recv().await, "said|id=2");
        // A request the runtime cannot make opens no connection and is charged nothing (Codex
        // r4179682244).
        peer.send("get|tag=2|ms=5000|url=ftp://127.0.0.1/x|op=rest|w=1");
        until(|| venue.http_log().len() == 2).await;
        assert_eq!(venue.http_log()[1], "0/2:NotSent");

        // The account: the place and the cancel; the IP: the REST request only.
        assert_eq!(used(ACCOUNT, BucketKey::Shared), 4);
        assert_eq!(used(IP_REST, BucketKey::Shared), 5);
        // Per pair: the subscription on A, the place on B, nothing on C or unkeyed.
        assert_eq!(used(PAIR, BucketKey::Pair(inst(1))), 1);
        assert_eq!(used(PAIR, BucketKey::Pair(inst(2))), 3);
        assert_eq!(used(PAIR, BucketKey::Pair(inst(3))), 0);
        assert_eq!(used(PAIR, BucketKey::Shared), 0);
        // The connection: hello, subscription and place, never the HTTP request or the cancel.
        assert_eq!(used(CONNECTION, BucketKey::Connection(conn)), 5);
        // The socket's connection and the HTTP request's own (Codex r4179474175).
        assert_eq!(used(IP_CONNECT, BucketKey::Shared), 2);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(rates.counts(), RateCounts::default());
    // The ended connection's bucket is forgotten.
    assert_eq!(used(CONNECTION, BucketKey::Connection(conn)), 0);
}

#[tokio::test]
async fn normal_traffic_stops_at_the_reserve_and_safety_traffic_passes_until_the_bucket_is_empty() {
    let mut ws = ScriptedWs::start().await;
    let venue = ToyVenue::with_limits(limits());
    // 20% of the account's 10 units are kept: normal places stop at 8.
    let rates = venue.limiter(20);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        for n in 1..=9 {
            peer.send(&format!("say|id={n}|op=place"));
        }
        for n in 1..=3 {
            peer.send(&format!("say|id=s{n}|op=place|class=safety"));
        }
        // Not an account operation: always written, after everything before it.
        peer.send("say|id=end");
        let mut heard = Vec::new();
        loop {
            let frame = peer.recv().await;
            if frame == "said|id=end" {
                break;
            }
            heard.push(frame);
        }
        drop(control);
        heard
    };
    let (run, heard) = tokio::join!(session.run(), script);
    run.unwrap();
    let ids: Vec<_> = heard
        .iter()
        .map(|f| f.trim_start_matches("said|id="))
        .collect();
    assert_eq!(ids, ["1", "2", "3", "4", "5", "6", "7", "8", "s1", "s2"]);
    let refused = ScopeCounts {
        account: 2,
        ..ScopeCounts::default()
    };
    assert_eq!(
        rates.counts(),
        RateCounts {
            refused,
            ..RateCounts::default()
        }
    );
    assert_eq!(rates.counts().refused.get(LimitScope::Account), 2);
}

#[tokio::test]
async fn a_refused_request_comes_back_not_sent_and_429_and_418_are_counted_under_its_scope() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ToyVenue::with_limits(limits());
    // 20% of the IP's 6 REST units are kept: normal requests stop at 5.
    let rates = venue.limiter(20);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        let get = |tag: u32, rest: &str| {
            format!("get|tag={tag}|ms=5000|url={}|op=rest{rest}", http.url("/x"))
        };
        let (first, second) = (get(1, "|w=3"), get(2, "|w=2"));
        let (third, fourth) = (get(3, ""), get(4, "|class=safety"));
        peer.send(&first);
        let head = "HTTP/1.1 429 Too Many Requests";
        http.request().await.answer(head, "").await;
        until(|| venue.http_log().len() == 1).await;
        peer.send(&second);
        http.request()
            .await
            // A body over the session's limit loses the response, but not its status.
            .answer("HTTP/1.1 418 I'm a teapot", &"x".repeat(2048))
            .await;
        until(|| venue.http_log().len() == 2).await;
        // At 5 of 6: a normal request is refused without a connection, a safety one passes.
        peer.send(&third);
        until(|| venue.http_log().len() == 3).await;
        assert_eq!(http.connections(), 2);
        peer.send(&fourth);
        http.request().await.answer("HTTP/1.1 200 OK", "").await;
        until(|| venue.http_log().len() == 4).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(
        venue.http_log(),
        ["0/1:429:-", "0/2:Lost", "0/3:NotSent", "0/4:200:-"]
    );
    let ip = ScopeCounts {
        ip: 1,
        ..ScopeCounts::default()
    };
    let counts = rates.counts();
    assert_eq!(counts.refused, ip);
    assert_eq!(counts.rejected, ScopeCounts { ip: 2, ..ip });
}

#[tokio::test]
async fn a_429_counts_under_the_requests_own_scopes_not_its_connections() {
    let (mut ws, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    // REST requests are counted per account, new connections per IP (Codex r4179558360).
    let minute = Duration::from_secs(60);
    let venue = ToyVenue::with_limits(vec![
        limit(LimitScope::Account, &[OpKind::Rest], 6, minute),
        limit(LimitScope::Ip, &[OpKind::Connect], 5, minute),
    ]);
    let rates = venue.limiter(0);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send(&format!("get|tag=1|ms=5000|url={}|op=rest", http.url("/x")));
        let head = "HTTP/1.1 429 Too Many Requests";
        http.request().await.answer(head, "").await;
        until(|| venue.http_log().len() == 1).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(venue.http_log(), ["0/1:429:-"]);
    // The request and its connection were both charged; the 429 answers the request alone.
    assert_eq!(rates.used(Instant::now(), 1, BucketKey::Shared), 2);
    let account = ScopeCounts {
        account: 1,
        ..ScopeCounts::default()
    };
    assert_eq!(
        rates.counts(),
        RateCounts {
            rejected: account,
            ..RateCounts::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_refused_subscription_stays_pending_and_is_sent_once_its_bucket_refills() {
    // The codec is asked once per call: a refused call's frames wait, with the call
    // outstanding, so the codec's state never runs ahead of what the venue was sent (Codex
    // r4179266579). The clock moves only when the test moves it.
    let frozen = freeze();
    let mut ws = ScriptedWs::start().await;
    // One subscribe call per instrument per 300 ms.
    let window = ms(300);
    let one_per_pair = limit(LimitScope::Pair, &[OpKind::Subscribe], 1, window);
    let venue = ToyVenue::with_limits(vec![one_per_pair]);
    let rates = venue.limiter(0);
    let started = Instant::now();
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // Removing A charges A's bucket again within its window: refused, nothing sent. A is
        // wanted again before the bucket refills.
        control.set_desired([]);
        settle(|| rates.counts().refused.pair == 1).await;
        control.set_desired([toy::sub(1)]);
        // The change tries the waiting call again: refused again, nothing sent.
        settle(|| rates.counts().refused.pair == 2).await;
        // The session keeps reading and writing while the call waits.
        peer.send("say|id=1");
        assert_eq!(peer.recv().await, "said|id=1");
        // The call goes once A's bucket has room, not a moment before.
        tokio::time::advance(window - ms(1)).await;
        peer.send("say|id=2");
        assert_eq!(peer.recv().await, "said|id=2");
        tokio::time::advance(ms(1)).await;
        assert_eq!(peer.recv().await, "sub|remove=A");
        assert_eq!(started.elapsed(), window);
        // The removal went, so adding A again is a call of its own, once A's bucket refills.
        settle(|| rates.counts().refused.pair == 3).await;
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|add=A");
        assert_eq!(started.elapsed(), window * 2);
        // Nothing more of either call: the next frame is the next one asked for.
        peer.send("say|id=3");
        assert_eq!(peer.recv().await, "said|id=3");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(rates.counts().refused.pair, 3);
    assert_eq!(venue.subscribe_calls(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_subscribe_call_whose_frames_can_fit_together_waits_until_they_all_do() {
    // A call's frames go together when they ever can, so no frame of it goes while another
    // waits and the codec, which changed its state for the whole call, never runs ahead of the
    // venue (Codex r4179558357). The clock moves only when the test moves it.
    let frozen = freeze();
    let mut ws = ScriptedWs::start().await;
    // Two subscribe frames per 300 ms on a connection.
    let window = ms(300);
    let per_connection = limit(LimitScope::Connection, &[OpKind::Subscribe], 2, window);
    let venue = ToyVenue::with_limits(vec![per_connection]);
    let rates = venue.limiter(0);
    let started = Instant::now();
    let mut config = session(venue, ws.url(), rates.clone());
    config.cfg.insert(toy::SPLIT, "yes");
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // Adding B and removing A takes two frames; one is free until A's add leaves the
        // window.
        control.set_desired([toy::sub(2)]);
        settle(|| rates.counts().refused.connection == 1).await;
        peer.send("say|id=1");
        assert_eq!(peer.recv().await, "said|id=1");
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|add=B");
        assert_eq!(peer.recv().await, "sub|remove=A");
        assert_eq!(started.elapsed(), window);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(rates.counts().refused.connection, 1);
    assert_eq!(venue.subscribe_calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_subscribe_call_whose_frames_never_fit_together_sends_them_one_by_one() {
    // The clock moves only when the test moves it.
    let frozen = freeze();
    let mut ws = ScriptedWs::start().await;
    // Two subscribe frames per 300 ms on a connection, one kept for safety traffic: a call of
    // two frames never fits at once (Codex r4179474176), each fits on its own in turn.
    let window = ms(300);
    let per_connection = limit(LimitScope::Connection, &[OpKind::Subscribe], 2, window);
    let venue = ToyVenue::with_limits(vec![per_connection]);
    let rates = venue.limiter(50);
    let started = Instant::now();
    let mut config = session(venue, ws.url(), rates.clone());
    config.cfg.insert(toy::SPLIT, "yes");
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let refused = || rates.counts().refused.connection;
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        control.set_desired([toy::sub(2)]);
        // Together, then on its own: B's add waits for A's to leave the window.
        settle(|| refused() == 2).await;
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|add=B");
        // Then A's removal waits for B's add, alone too.
        settle(|| refused() == 4).await;
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|remove=A");
        assert_eq!(started.elapsed(), window * 2);
        // The next change is not held behind it.
        control.set_desired([toy::sub(3)]);
        settle(|| refused() == 6).await;
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|add=C");
        settle(|| refused() == 8).await;
        tokio::time::advance(window).await;
        assert_eq!(peer.recv().await, "sub|remove=B");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(venue.subscribe_calls(), 3);
}

#[tokio::test]
async fn a_subscribe_frame_that_never_fits_ends_the_session_and_its_connection_is_forgotten() {
    let mut ws = ScriptedWs::start().await;
    // Two subscribe or control units per minute on a connection, one kept for safety traffic:
    // a subscribe frame of weight 2 never fits (Codex r4179682238).
    let minute = Duration::from_secs(60);
    let ops = [OpKind::Subscribe, OpKind::Control];
    let venue = ToyVenue::with_limits(vec![limit(LimitScope::Connection, &ops, 2, minute)]);
    let rates = venue.limiter(50);
    let mut config = session(venue, ws.url(), rates.clone());
    config.cfg.insert(toy::SPLIT, "2");
    let (mut session, _control) = MdSession::new(config, |_| {}).unwrap();
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        peer
    };
    let (run, _peer) = tokio::join!(session.run(), script);
    let one = RateCharge::one(OpKind::Subscribe, Some(inst(1)));
    let charge = RateCharge {
        weight: NonZeroU32::new(2).unwrap(),
        ..one
    };
    assert_eq!(
        run.unwrap_err(),
        SessionError::Rates(RateError::NeverFits(charge))
    );
    // The hello was charged to the connection, whose bucket is forgotten all the same (Codex
    // r4179720972).
    let conn = BucketKey::Connection(ConnKey {
        conn: CONN,
        epoch: 0,
    });
    assert_eq!(rates.used(Instant::now(), 0, conn), 0);
}

#[tokio::test]
async fn close_frames_are_charged_as_control_frames() {
    let mut ws = ScriptedWs::start().await;
    // Three control frames per minute from this IP, none kept for safety traffic.
    let minute = Duration::from_secs(60);
    let venue = ToyVenue::with_limits(vec![limit(LimitScope::Ip, &[OpKind::Control], 3, minute)]);
    let rates = venue.limiter(0);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let used = || rates.used(Instant::now(), 0, BucketKey::Shared);
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // The WebSocket layer answers the venue's close with one of its own (Codex
        // r4179720976): charged like a pong.
        peer.drop_conn();
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=1|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        assert_eq!(used(), 3);
        // The bucket is full: the session's own close is refused and not written; the socket
        // closes all the same.
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(used(), 3);
    assert_eq!(rates.counts().refused.ip, 1);
}

#[tokio::test]
async fn a_pong_the_websocket_layer_sends_on_its_own_is_charged_to_its_connection() {
    let mut ws = ScriptedWs::start().await;
    // Three control frames per minute on a connection, none kept for safety traffic.
    let minute = Duration::from_secs(60);
    let per_connection = limit(LimitScope::Connection, &[OpKind::Control], 3, minute);
    let venue = ToyVenue::with_limits(vec![per_connection]);
    let rates = venue.limiter(0);
    let (mut session, control) =
        MdSession::new(session(venue, ws.url(), rates.clone()), |_| {}).unwrap();
    let conn = BucketKey::Connection(ConnKey {
        conn: CONN,
        epoch: 0,
    });
    let script = async {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        // The hello and the ping's pong (Codex r4179266588) leave room for one more frame.
        peer.ping();
        peer.send("say|id=1");
        // The scripted server hears the pong first (FBC-djl).
        assert_eq!(peer.recv().await, PONG);
        assert_eq!(peer.recv().await, "said|id=1");
        peer.send("say|id=2");
        until(|| rates.counts().refused.connection == 1).await;
        assert_eq!(rates.used(Instant::now(), 0, conn), 3);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
}

#[tokio::test(start_paused = true)]
async fn connection_attempts_beyond_a_connect_limit_wait_for_the_bucket() {
    let (addr, mut accepts) = refusing().await;
    let per = Duration::from_secs(10);
    let connects = limit(LimitScope::Ip, &[OpKind::Connect], 2, per);
    let venue = ToyVenue::with_limits(vec![connects]);
    let rates = venue.limiter(0);
    let mut config = session(venue, format!("ws://{addr}/md"), rates.clone());
    // No attempt deadline: on paused time a pending deadline would let the clock jump ahead
    // while the attempt's socket I/O is still under way.
    config.pacing = ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), Duration::MAX).unwrap();
    let (mut session, control) = MdSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut at = Vec::new();
        while at.len() < 5 {
            at.push(accepts.recv().await.unwrap());
        }
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    // The backoff alone would start all five within a second; at most two start in any 10 s.
    assert!(at.windows(3).all(|w| w[2].duration_since(w[0]) >= per));
    assert_eq!(at[1].duration_since(at[0]), ms(10));
    assert_eq!(at[2].duration_since(at[0]), per);
    assert_eq!(session.counters().attempts, 5);
    assert!(rates.counts().refused.ip >= 2);
}

#[test]
fn a_venue_declaring_an_address_volume_limit_is_refused_at_start() {
    let volume = LimitScope::AddressVolume { usdc_per_req: 10 };
    let declared = vec![limit(volume, &[OpKind::Place], 1, ms(1_000))];
    let venue = ToyVenue::with_limits(declared.clone());
    let reserve = SafetyReserve::percent(15).unwrap();
    let err = RateLimiter::new(&declared, reserve).unwrap_err();
    assert_eq!(err, RateError::Unsupported(volume));
    assert!(err.to_string().contains("AddressVolume"));
    // A limiter built for any other limits is not taken, so no session of it starts.
    let other = ToyVenue::leak().limiter(0);
    let url = "ws://127.0.0.1:1/md".to_owned();
    let err = MdSession::new(session(venue, url, other.clone()), |_| {})
        .err()
        .unwrap();
    assert_eq!(err, SessionError::Rates(RateError::OtherLimits));
    assert_eq!(
        err.to_string(),
        "the rate limiter was built for other limits than the venue's"
    );
    let config = MdVenueConfig {
        venue,
        cfg: VenueConfig::new(),
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: pacing(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conns: Range { start: 0, end: 4 },
        limiter: other,
        liveness: no_alarm(),
    };
    let err = MdVenue::new(config, |_| {}).err().unwrap();
    assert_eq!(err, SessionError::from(RateError::OtherLimits));
}
