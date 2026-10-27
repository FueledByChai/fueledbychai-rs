//! FBC-2kmp's done line, the stub test: fbc-runtime's market-data session runs the Hibachi codec
//! against the conformance kit's stub server on a paused clock, ETH/USDT-P's and SOL/USDT-P's
//! orderbooks (and ETH's other topics) on one connection, the resubscribe interval from the
//! required `hibachi.orderbook_resubscribe`. The stub plays the script below; the test moves the
//! clock by the interval at each point the script waits for the codec, and reads the books a
//! consumer's `MdBooks` builds (decisions 0023, 0039, 0129):
//!
//! - an empty Snapshot on the first subscription, on a resubscribe and after a reconnect reports
//!   a gap and pushes no level or window, the book not valid through the Updates after it while
//!   SOL's stays valid; after the interval the codec writes an unsubscribe and then a subscribe of
//!   ETH's orderbook with the same granularity and depth, again at each interval while Snapshots
//!   come back empty or thin;
//! - an Update whose bounds disagree with the book (a frame lost) gaps that book only and brings
//!   the same resubscribe; a populated Snapshot then makes the book equal to it and the Updates
//!   after it;
//! - a resubscribe answered by no Snapshot within the interval brings one reconnect, paced by
//!   `ReconnectPacing`, after which every subscription is replayed and the populated Snapshot
//!   makes the book valid; so does a close by the venue;
//! - a book whose subscription the consumer removes is never resubscribed;
//! - decoder replay of the session's journal gives the same events, so the same book states at
//!   every point, and rebuilds both books byte for byte (decision 0006).
//!
//! The clock moves only when the test moves it: a blocking task that never ends keeps tokio from
//! jumping it while socket I/O is under way, and every wait is for something the test observes,
//! bounded by wall-clock time from another thread (as `fbc-conformance`'s `acks_and_silence.rs`).

mod common;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fbc_book::{BookState, L2Book};
use fbc_conformance::{Frame, HttpRoutes, Step, StubServer, WsScript};
use fbc_core::{
    ConnKey, EndpointPlan, Envelope, Feed, InstrumentId, Lvl, MdEvent, Subscription, VenueConfig,
    VenueFactory,
};
use fbc_journal::{JournalReader, JournalWriter, RedactionKey, SinkConfig, journal_queue};
use fbc_runtime::{
    Connector, IngestClock, Journal, Liveness, MdBooks, MdHandler, MdReplay, MdReplayConfig,
    MdSession, MdSessionConfig, ProxyConfig, RateLimiter, ReconnectPacing, SafetyReserve,
    TradingBooks, WriteStall,
};
use fbc_venue_hibachi::{BOOK_ORDERBOOK, Hibachi, KEY_DATA_WS_URL};
use tokio::time::advance;

use common::{ETH, INTERVAL, SOL, book_sub, config, eth_lvl, every_feed, frame, specs};

const CONN: u16 = 4;
const SHARD: u16 = 1;
/// The pacing's floor: the wait after a connection that opened drops.
const FLOOR: Duration = Duration::from_millis(100);

/// A book as a gap of it found it.
type BeforeGap = (InstrumentId, Option<L2Book>);

/// What the consumer sees: every event, each epoch's end, and the books `MdBooks` builds.
///
/// A script cannot wait for the test, only for the client, so a point the stub passes straight
/// through (a Snapshot followed at once by a lost frame, a connection the venue closes) is read
/// afterwards from what the books were just before: each book as each gap of it found it, and
/// every book as each epoch's end found it.
#[derive(Clone, Default)]
struct Seen {
    events: Rc<RefCell<Vec<Envelope<MdEvent>>>>,
    ends: Rc<RefCell<Vec<ConnKey>>>,
    books: Rc<RefCell<Option<MdBooks>>>,
    before_gap: Rc<RefCell<Vec<BeforeGap>>>,
    before_end: Rc<RefCell<Vec<MdBooks>>>,
}

impl Seen {
    fn new() -> Seen {
        let seen = Seen::default();
        *seen.books.borrow_mut() = Some(MdBooks::new(TradingBooks::default()));
        seen
    }

    fn book(&self, inst: InstrumentId) -> Option<L2Book> {
        let books = self.books.borrow();
        books.as_ref().unwrap().book(inst, BOOK_ORDERBOOK).cloned()
    }

    fn valid(&self, inst: InstrumentId) -> bool {
        self.book(inst)
            .is_some_and(|b| matches!(b.state(), BookState::Valid { .. }))
    }

    /// `inst`'s book as its last gap found it: valid, and its levels best first.
    fn before_last_gap(&self, inst: InstrumentId) -> (Vec<Lvl>, Vec<Lvl>) {
        let gaps = self.before_gap.borrow();
        let (_, book) = gaps.iter().rev().find(|(i, _)| *i == inst).unwrap();
        let book = book.as_ref().unwrap();
        assert!(matches!(book.state(), BookState::Valid { .. }));
        let top = book.top(10).unwrap();
        (top.bids, top.asks)
    }

    fn len(&self) -> usize {
        self.events.borrow().len()
    }

    /// ETH's orderbook events from position `from` on.
    fn eth_book_events(&self, from: usize) -> Vec<MdEvent> {
        let events = self.events.borrow();
        events[from..]
            .iter()
            .map(|e| e.body)
            .filter(|ev| match *ev {
                MdEvent::BookSnapshotBegin { inst, .. }
                | MdEvent::BookSnapshotEnd { inst, .. }
                | MdEvent::Level { inst, .. }
                | MdEvent::Window { inst, .. }
                | MdEvent::SidePrune { inst, .. }
                | MdEvent::SideClear { inst, .. } => inst == ETH,
                MdEvent::Health {
                    inst,
                    feed: Feed::Book(_),
                    ..
                } => inst == ETH,
                _ => false,
            })
            .collect()
    }
}

impl MdHandler for Seen {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        let mut books = self.books.borrow_mut();
        let books = books.as_mut().unwrap();
        if let MdEvent::Health {
            inst,
            feed: Feed::Book(book),
            ..
        } = env.body
        {
            let before = books.book(inst, book).cloned();
            self.before_gap.borrow_mut().push((inst, before));
        }
        books.apply(env.stamp.conn, &env.body).unwrap();
        self.events.borrow_mut().push(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        let mut books = self.books.borrow_mut();
        let books = books.as_mut().unwrap();
        self.before_end.borrow_mut().push(books.clone());
        books.end_epoch(key);
        self.ends.borrow_mut().push(key);
    }
}

/// ETH's six topics and SOL's orderbook.
fn subs() -> BTreeSet<Subscription> {
    let mut subs: BTreeSet<Subscription> = every_feed(ETH).into_iter().collect();
    subs.insert(book_sub(SOL));
    subs
}

/// The subscribe frames every connection is sent, sorted.
fn subscribe_frames() -> Vec<Frame> {
    let one = |body: &str| {
        Frame::text(format!(
            r#"{{"method":"subscribe","parameters":{{"subscriptions":[{body}]}}}}"#
        ))
    };
    let mut frames: Vec<Frame> = [
        r#"{"depth":3,"granularity":"0.01","symbol":"ETH/USDT-P","topic":"orderbook"}"#,
        r#"{"symbol":"ETH/USDT-P","topic":"ask_bid_price"}"#,
        r#"{"symbol":"ETH/USDT-P","topic":"trades"}"#,
        r#"{"symbol":"ETH/USDT-P","topic":"mark_price"}"#,
        r#"{"symbol":"ETH/USDT-P","topic":"spot_price"}"#,
        r#"{"symbol":"ETH/USDT-P","topic":"funding_rate_estimation"}"#,
        r#"{"depth":3,"granularity":"0.001","symbol":"SOL/USDT-P","topic":"orderbook"}"#,
    ]
    .into_iter()
    .map(one)
    .collect();
    frames.sort_by_key(|f| format!("{f:?}"));
    frames
}

const ETH_UNSUBSCRIBE: &str = r#"{"method":"unsubscribe","parameters":{"subscriptions":[{"symbol":"ETH/USDT-P","topic":"orderbook"}]}}"#;
const ETH_SUBSCRIBE: &str = r#"{"method":"subscribe","parameters":{"subscriptions":[{"depth":3,"granularity":"0.01","symbol":"ETH/USDT-P","topic":"orderbook"}]}}"#;

/// The script, with the step count after each point the test acts at.
struct Script {
    steps: Vec<Step>,
    marks: Vec<usize>,
}

impl Script {
    fn accept(&mut self, conn: usize) {
        self.steps.push(Step::Accept);
        for _ in 0..subscribe_frames().len() {
            self.steps.push(Step::Read { conn });
        }
    }

    fn push(&mut self, conn: usize, names: &[&str]) {
        for name in names {
            let frame = Frame::text(frame(name));
            self.steps.push(Step::Push { conn, frame });
        }
    }

    fn read(&mut self, conn: usize, n: usize) {
        for _ in 0..n {
            self.steps.push(Step::Read { conn });
        }
    }

    /// A barrier, after which the test acts: its position.
    fn mark(&mut self, conn: usize) -> usize {
        self.steps.push(Step::Barrier { conn });
        self.marks.push(self.steps.len());
        self.steps.len()
    }
}

/// The timeline. Each `mark` is a point the stub then waits at for the client (the codec's next
/// frames), where the test checks and moves the clock.
fn script() -> Script {
    let mut s = Script {
        steps: Vec::new(),
        marks: Vec::new(),
    };
    // Connection 0: an empty Snapshot on the first subscription; SOL's populated; an Update
    // for each; a trade and a touch.
    s.accept(0);
    s.push(
        0,
        &[
            "eth_snapshot_empty",
            "sol_s0",
            "eth_unchanged",
            "sol_u1",
            "trade_single",
            "abp_sized",
        ],
    );
    s.mark(0); // 0: ETH not valid, SOL valid; then the interval.
    s.read(0, 2);
    // The resubscribe answered: the acknowledgement, the empty Update an unsubscribe brings,
    // an empty Snapshot again, and an Update after it.
    s.push(
        0,
        &[
            "ack",
            "eth_unchanged",
            "eth_snapshot_empty",
            "eth_unchanged",
        ],
    );
    s.mark(0); // 1: still not valid; then the interval.
    s.read(0, 2);
    s.push(0, &["ack", "eth_snapshot_thin"]);
    s.mark(0); // 2: a thin side fails too; then the interval.
    s.read(0, 2);
    // The populated Snapshot and an Update, then a frame lost (U2): U3's bid window removes
    // 1999.80 unstated.
    s.push(0, &["ack", "eth_s0", "eth_u1", "eth_u3"]);
    s.mark(0); // 3: ETH was S0 and U1 until the gap; then the interval.
    s.read(0, 2);
    s.push(0, &["ack", "eth_s0", "eth_u2"]);
    s.mark(0); // 4: ETH was S0 until U2 (U1 lost) gapped it; then the interval.
    s.read(0, 2);
    s.mark(0); // 5: no Snapshot answers; then the interval: the codec's reconnect.
    // Connection 1: every subscription replayed; populated Snapshots; the venue closes it.
    s.accept(1);
    s.push(1, &["eth_s0", "sol_s0"]);
    s.steps.push(Step::Close { conn: 1 });
    // Connection 2: every subscription replayed; an empty Snapshot on the reconnect.
    s.accept(2);
    s.push(2, &["eth_snapshot_empty", "sol_s0", "eth_unchanged"]);
    s.mark(2); // 6: ETH not valid, SOL valid; then the interval.
    s.read(2, 2);
    s.push(2, &["ack", "eth_s0", "eth_u1", "eth_u3"]);
    s.mark(2); // 7: ETH was S0 and U1 until the gap; the consumer removes ETH's orderbook.
    s.read(2, 1);
    s.mark(2); // 8: the interval passes: nothing is written.
    s
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets every task run, without moving the clock, until `done`; panics after a minute of
/// wall-clock time, so a regression fails instead of hanging.
async fn until(what: &str, done: impl Fn() -> bool) {
    let expired = Arc::new(AtomicBool::new(false));
    let flag = expired.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(60));
        flag.store(true, Ordering::SeqCst);
    });
    while !done() {
        assert!(!expired.load(Ordering::SeqCst), "{what}: not within 60 s");
        tokio::task::yield_now().await;
    }
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

fn plan(cfg: &VenueConfig) -> EndpointPlan {
    let mut plans = Hibachi.plan_md(cfg, &specs(), &subs()).unwrap();
    assert_eq!(plans.len(), 1);
    plans.remove(0)
}

fn session_config(cfg: &VenueConfig) -> MdSessionConfig {
    let caps = Hibachi.caps(cfg).unwrap();
    MdSessionConfig {
        venue: &Hibachi,
        cfg: cfg.clone(),
        plan: plan(cfg),
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        // No attempt deadline: a pending deadline would be a timer the test has to step past.
        pacing: ReconnectPacing::new(
            FLOOR,
            FLOOR * 8,
            100,
            Duration::from_secs(60),
            Duration::MAX,
        )
        .unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// S0's book and S0 then U1's, best first.
fn s0() -> (Vec<Lvl>, Vec<Lvl>) {
    (
        vec![
            eth_lvl(200_000, 1_250),
            eth_lvl(199_990, 3_000),
            eth_lvl(199_980, 500),
        ],
        vec![
            eth_lvl(200_010, 1_500),
            eth_lvl(200_020, 2_000),
            eth_lvl(200_030, 750),
        ],
    )
}

fn s0_u1() -> (Vec<Lvl>, Vec<Lvl>) {
    let (bids, _) = s0();
    let asks = vec![
        eth_lvl(200_020, 2_000),
        eth_lvl(200_030, 750),
        eth_lvl(200_040, 1_100),
    ];
    (bids, asks)
}

fn is_gap(ev: &MdEvent) -> bool {
    matches!(ev, MdEvent::Health { .. })
}

#[tokio::test(start_paused = true)]
async fn the_codec_recovers_broken_books_by_resubscribing_then_reconnecting_and_replay_agrees() {
    let frozen = freeze();
    let Script { steps, marks } = script();
    let server = StubServer::start(WsScript::new(steps), HttpRoutes::new())
        .await
        .unwrap();
    let mut cfg = config();
    cfg.insert(KEY_DATA_WS_URL, &server.ws_url(""));
    let root = fresh_dir("hibachi_md_stub");
    let sink_config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(sink_config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let seen = Seen::new();
    let (mut session, control) = MdSession::new(session_config(&cfg), seen.clone()).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(sink))));

    let watch = seen.clone();
    let (server, marks) = (&server, &marks);
    let drive = async {
        let at = |n: usize| async move {
            until(&format!("mark {n}"), || server.played() >= marks[n]).await
        };
        // 0: the empty Snapshot on the first subscription gapped ETH's book, which stays not
        // valid through the Update after it; SOL's is valid; no level or window of ETH's.
        at(0).await;
        assert!(!watch.valid(ETH));
        assert!(watch.valid(SOL));
        let eth = watch.eth_book_events(0);
        assert!(eth.iter().all(is_gap), "{eth:?}");
        assert_eq!(eth.len(), 1);
        let mut from = watch.len();
        // The interval: an unsubscribe and a subscribe, read by the script.
        advance(INTERVAL).await;
        // 1: the resubscribe's empty Snapshot gaps it again, and it stays not valid through
        // the Update after it, no level or window of it pushed.
        at(1).await;
        assert!(!watch.valid(ETH));
        assert!(watch.valid(SOL));
        let eth = watch.eth_book_events(from);
        assert_eq!(eth.len(), 1, "{eth:?}");
        assert!(eth.iter().all(is_gap));
        from = watch.len();
        advance(INTERVAL).await;
        // 2: a thin Snapshot fails the check too.
        at(2).await;
        assert!(!watch.valid(ETH));
        assert!(watch.eth_book_events(from).iter().all(is_gap));
        advance(INTERVAL).await;
        // 3: the populated Snapshot made ETH's book equal to it and the Update after it, until
        // a lost frame gapped ETH's book only.
        at(3).await;
        assert_eq!(watch.before_last_gap(ETH), s0_u1());
        assert!(!watch.valid(ETH));
        assert!(watch.valid(SOL));
        advance(INTERVAL).await;
        // 4: the resubscribe's Snapshot: the book was that Snapshot until the next lost frame.
        at(4).await;
        assert_eq!(watch.before_last_gap(ETH), s0());
        assert!(!watch.valid(ETH));
        advance(INTERVAL).await;
        // 5: resubscribed; no Snapshot answers within the interval.
        at(5).await;
        advance(INTERVAL).await;
        // The codec's reconnect: the session closes connection 0 and, after the floor, opens
        // connection 1, which the venue closes once both Snapshots are in.
        until("connection 0 closed", || watch.ends.borrow().len() == 1).await;
        churn().await;
        assert_eq!(server.connections().len(), 1, "no attempt before the floor");
        while watch.ends.borrow().len() < 2 {
            advance(FLOOR / 10).await;
            churn().await;
        }
        // Every subscription replayed; the populated Snapshots made both books valid, ETH's
        // equal to its Snapshot, until the venue's close gapped both at the epoch's end.
        let ended = watch.before_end.borrow()[1].clone();
        let book = |inst| ended.book(inst, BOOK_ORDERBOOK).unwrap().clone();
        assert_eq!(book(ETH).state(), BookState::Valid { epoch: 1 });
        assert_eq!(book(SOL).state(), BookState::Valid { epoch: 1 });
        let top = book(ETH).top(10).unwrap();
        assert_eq!((top.bids, top.asks), s0());
        // Byte for byte the book a fresh codec builds from that Snapshot alone.
        let mut fresh = common::Driver::new(&[book_sub(ETH)]);
        fresh.feed("eth_s0");
        assert_eq!(
            book(ETH).canonical_bytes(),
            fresh.book(ETH).canonical_bytes()
        );
        assert!(!watch.valid(ETH));
        assert!(!watch.valid(SOL));
        let from = watch.len();
        while server.connections().len() < 3 {
            advance(FLOOR / 10).await;
            churn().await;
        }
        // 6: an empty Snapshot on the reconnect: ETH not valid through the Update after it, no
        // level or window of it; SOL valid.
        at(6).await;
        assert!(!watch.valid(ETH));
        assert!(watch.valid(SOL));
        let eth = watch.eth_book_events(from);
        assert_eq!(eth.len(), 1, "{eth:?}");
        assert!(eth.iter().all(is_gap));
        advance(INTERVAL).await;
        // 7: resubscribed, the populated Snapshot and its Update, until a lost frame; the
        // consumer then removes ETH's orderbook.
        at(7).await;
        assert_eq!(watch.before_last_gap(ETH), s0_u1());
        assert!(!watch.valid(ETH));
        let mut kept = subs();
        kept.remove(&book_sub(ETH));
        control.set_desired(kept);
        // 8: the unsubscribe was read; the interval passes and nothing more is written.
        at(8).await;
        advance(INTERVAL).await;
        churn().await;
        server.finished().await.unwrap();
        churn().await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), drive);
    run.unwrap();
    drop(frozen);
    writer.close().unwrap();
    assert_eq!(session.counters().decode_errors, 0);
    assert_eq!(session.counters().attempts, 3);

    // What each connection was sent: every subscription, replayed after each reconnect, and the
    // codec's own unsubscribe and subscribe of ETH's orderbook, with the same granularity and
    // depth, at each interval.
    let conns = server.connections();
    assert_eq!(conns.len(), 3);
    let resubscribe = [Frame::text(ETH_UNSUBSCRIBE), Frame::text(ETH_SUBSCRIBE)];
    for (n, conn) in conns.iter().enumerate() {
        let mut first = conn.received[..7].to_vec();
        first.sort_by_key(|f| format!("{f:?}"));
        assert_eq!(first, subscribe_frames(), "connection {n}");
    }
    assert_eq!(conns[0].received.len(), 7 + 5 * 2);
    for pair in conns[0].received[7..].chunks(2) {
        assert_eq!(pair, resubscribe);
    }
    assert_eq!(conns[1].received.len(), 7);
    assert_eq!(
        conns[2].received[7..],
        [
            resubscribe[0].clone(),
            resubscribe[1].clone(),
            Frame::text(ETH_UNSUBSCRIBE),
        ]
    );

    // Decoder replay of the journal: the same events, epoch ends and books, byte for byte.
    let live_events = seen.events.borrow().clone();
    let live_books = seen.books.borrow().clone().unwrap();
    for _ in 0..2 {
        let replayed = Seen::new();
        let config = MdReplayConfig {
            venue: &Hibachi,
            cfg: cfg.clone(),
            plan: plan(&cfg),
            specs: specs(),
            conn: CONN,
        };
        let mut replay = MdReplay::new(config, replayed.clone()).unwrap();
        replay
            .run(JournalReader::open(&root, SHARD).unwrap())
            .unwrap();
        assert_eq!(replay.counters().epochs, 3);
        drop(replay);
        assert_eq!(*replayed.events.borrow(), live_events);
        assert_eq!(*replayed.ends.borrow(), *seen.ends.borrow());
        assert_eq!(*replayed.before_gap.borrow(), *seen.before_gap.borrow());
        assert_eq!(*replayed.before_end.borrow(), *seen.before_end.borrow());
        let books = replayed.books.borrow().clone().unwrap();
        assert_eq!(books, live_books);
        // Byte for byte, each book valid when an epoch ended: SOL's at each end, and ETH's at
        // the venue's close (the final books are all gapped, so compare these).
        let (live_ends, replayed_ends) = (seen.before_end.borrow(), replayed.before_end.borrow());
        for (end, inst) in [(0, SOL), (1, ETH), (1, SOL), (2, SOL)] {
            let live = live_ends[end].book(inst, BOOK_ORDERBOOK).unwrap();
            let again = replayed_ends[end].book(inst, BOOK_ORDERBOOK).unwrap();
            assert!(
                matches!(live.state(), BookState::Valid { .. }),
                "{end} {inst:?}"
            );
            assert_eq!(
                again.canonical_bytes(),
                live.canonical_bytes(),
                "{end} {inst:?}"
            );
        }
    }
    fs::remove_dir_all(&root).unwrap();
}
