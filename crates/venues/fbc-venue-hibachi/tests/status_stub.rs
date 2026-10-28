//! FBC-1632's done line, the stub tests (decisions 0006, 0027, 0111 and 0130): fbc-runtime's
//! market-data session runs the status poll endpoint `plan_md` plans against the conformance
//! kit's stub server, on a paused clock the test moves, on the hand-built SYNTHETIC answers of
//! `fixtures/hibachi/rest/status/`. The stub runs on a runtime of its own on another thread and
//! holds each read until the test hands it its answer, so a read can be in flight while the
//! clock passes a halt. The session's clock reads its wall time from the paused clock
//! (`IngestClock::replayed`), so every instant below is exact. Each test then replays the
//! session's journal (its HTTP answers and timer firings) through decoder replay and gets the
//! same events, stamps included.
//!
//! - a contract going LIVE to PAUSED and back, the exchange entering and leaving
//!   SCHEDULED_MAINTENANCE, a failed read and a refused answer, each reported once at the poll
//!   that sees it, and a consumer's handler passing each status it is told into
//!   `PlanVenue::status`, the execution planner then planning no place or amend on the halted
//!   market and still its cancel;
//! - a maintenance window starting between two reads and an FX session ending between two reads,
//!   each reported halted at its time, a read sent before each and answered after its halt LIVE
//!   and NORMAL still halted, and none trading again before a read shows it, the window's bounds
//!   read alike as numbers and as strings;
//! - the window after the FX reopening, pending FBC-vg2n, and a read sent before the window
//!   after a reopening (the FX pair's, and an advertised open) ends, answered after it, halted;
//! - a close between two reads reported at that instant, a read sent before it and the next
//!   read, both showing the contract LIVE with that close, still halted, a read answered after a
//!   close it is the first to advertise halted, and a market a read leaves out halted;
//! - a close in milliseconds, one not a number and a window bound in milliseconds each refused,
//!   every market halted at the poll that refused it.

mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use common::status::{
    AFTER, ETH, EUR, FRIDAY, FX_CLOSE, FX_REOPEN, INTERVAL, MARKETS, NS, WEDNESDAY, WIF, answer,
    each, poll_plan, specs, subs,
};
use fbc_conformance::{HttpReply, HttpRouter, PathPattern, StubServer, WsScript};
use fbc_core::{
    AccountKey, AccountLease, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, Channel, CidMint,
    EndpointPlan, Envelope, InstrumentId, ItemRef, Lots, MarketLease, MdEvent, MonoNs, Namespace,
    NamespaceLease, OrderCaps, RefKind, Side, SignedLots, SubmitOutcome, TagSet, Ticks, Tif,
    TradingStatus, TrafficClass, VenueCommand, VenueConfig, VenueFactory, VenueMode, WallNs,
    dispatch_market_data,
};
use fbc_journal::{
    JournalReader, JournalSink, JournalWriter, QueueSink, Record, RecordRef, Recorded,
    RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::{
    Batching, DesiredBook, DesiredQuote, ExecutionPlanner, LadderConfig, LeaseKeys, Leases,
    MarketCapsConfig, OrderKey, OrderOp, PlanRefusal, PlanVenue, PlannerConfig, PreTradeCaps,
    RateView, ReduceFallback, Registry, ResyncSnapshot, Stage,
};
use fbc_runtime::http::Method;
use fbc_runtime::{
    Connector, IngestClock, Journal, Liveness, MdHandler, MdReplay, MdReplayConfig, MdSession,
    MdSessionConfig, ProxyConfig, RateLimiter, ReconnectPacing, SafetyReserve, WriteStall,
};
use fbc_venue_hibachi::discover::EXCHANGE_INFO_PATH;
use fbc_venue_hibachi::{Hibachi, KEY_DATA_REST_URL, KEY_REST_TIMEOUT};
use tokio::time::advance;

const CONN: u16 = 6;
const SHARD: u16 = 2;
const TRADING: TradingStatus = TradingStatus::Trading;
const HALTED: TradingStatus = TradingStatus::Halted;

type Seen = Vec<(f64, InstrumentId, TradingStatus)>;

/// The conformance stub on a thread and runtime of its own: its HTTP endpoint answers each
/// `GET /market/exchange-info` with the reply the test hands it once the read is in, holding the
/// read meanwhile. Dropping it answers a read still held 503 and stops the stub.
struct Venue {
    url: String,
    asked: mpsc::Receiver<()>,
    replies: Option<mpsc::Sender<HttpReply>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Venue {
    fn start() -> Venue {
        let (url_tx, url_rx) = mpsc::channel();
        let (asked_tx, asked) = mpsc::channel::<()>();
        let (replies, reply_rx) = mpsc::channel::<HttpReply>();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let held = Mutex::new((asked_tx, reply_rx));
                let pattern = PathPattern::exact(EXCHANGE_INFO_PATH);
                let router = HttpRouter::new().route_fn(Method::GET, pattern, move |_| {
                    let held = held.lock().unwrap();
                    let _ = held.0.send(());
                    held.1.recv().unwrap_or(HttpReply {
                        status: 503,
                        body: Vec::new(),
                    })
                });
                let server = StubServer::start(WsScript::new(Vec::new()), router)
                    .await
                    .unwrap();
                url_tx.send(server.http_url("")).unwrap();
                let _ = stopped.await;
                drop(server);
            });
        });
        Venue {
            url: url_rx.recv().unwrap(),
            asked,
            replies: Some(replies),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn reply(&self, status: u16, body: Vec<u8>) {
        let replies = self.replies.as_ref().unwrap();
        replies.send(HttpReply { status, body }).unwrap();
    }
}

impl Drop for Venue {
    fn drop(&mut self) {
        self.replies.take();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A journal sink that keeps a copy of every record it is offered, so the test sees what the
/// session took as it takes it, and hands each to the journal queue.
struct Tee {
    sink: QueueSink,
    kept: Rc<RefCell<Vec<Record>>>,
}

impl JournalSink for Tee {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.kept.borrow_mut().push(record.clone());
        self.sink.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        self.kept.borrow_mut().push(record.to_record());
        self.sink.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.sink.omit(class, now)
    }
}

/// What the consumer's handler saw, and what it did with each status.
#[derive(Clone, Default)]
struct Consumer {
    events: Rc<RefCell<Vec<Envelope<MdEvent>>>>,
    /// Each market's status as last reported: what the consumer passes the planner.
    statuses: Rc<RefCell<BTreeMap<InstrumentId, TradingStatus>>>,
    /// Whether it plans ETH at each status it is told.
    quotes: bool,
    /// What the planner planned for ETH at each status passed to it.
    planned: Rc<RefCell<Vec<Quoted>>>,
}

impl MdHandler for Consumer {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        if let MdEvent::Status { inst, status } = env.body {
            self.statuses.borrow_mut().insert(inst, status);
            if self.quotes && inst == ETH {
                // The status as the venue's market data last reported it, passed to the
                // planner's PlanVenue.
                let status = self.statuses.borrow()[&ETH];
                self.planned.borrow_mut().push(quote_eth(status));
            }
        }
        self.events.borrow_mut().push(env);
    }
}

impl Consumer {
    /// Every status reported, as (seconds after `origin`, market, status).
    fn statuses(&self, origin: i64) -> Seen {
        let events = self.events.borrow();
        events
            .iter()
            .filter_map(|env| match env.body {
                MdEvent::Status { inst, status } => {
                    let at = (env.stamp.recv_wall.0 - origin * NS) as f64 / NS as f64;
                    Some((at, inst, status))
                }
                _ => None,
            })
            .collect()
    }

    fn count(&self) -> usize {
        self.events.borrow().len()
    }
}

/// One timeline against the stub: the session, its journal and the test's hold on the clock.
struct Run {
    venue: Venue,
    kept: Rc<RefCell<Vec<Record>>>,
    consumer: Consumer,
    /// Seconds after the origin the clock stands at.
    at: RefCell<i64>,
}

impl Run {
    /// Moves the clock to `secs` after the origin, a step at a time so every timer fires at its
    /// own instant, and lets the session take what fell due.
    async fn to(&self, secs: i64) {
        let from = *self.at.borrow();
        assert!(secs >= from);
        advance(Duration::from_secs(u64::try_from(secs - from).unwrap())).await;
        *self.at.borrow_mut() = secs;
        churn().await;
    }

    /// Waits for a read to reach the stub.
    async fn asked(&self) {
        until("a read at the stub", || self.venue.asked.try_recv().is_ok()).await;
    }

    fn http_results(&self) -> usize {
        let kept = self.kept.borrow();
        kept.iter()
            .filter(|r| matches!(r, Record::HttpResult { .. }))
            .count()
    }

    /// Whether a timer fired after the last HTTP result: the one its answer was judged at.
    fn judged(&self) -> bool {
        let kept = self.kept.borrow();
        let last = |f: &dyn Fn(&Record) -> bool| kept.iter().rposition(f);
        let result = last(&|r| matches!(r, Record::HttpResult { .. }));
        let timer = last(&|r| matches!(r, Record::Timer { .. }));
        matches!((result, timer), (Some(r), Some(t)) if t > r)
    }

    /// Hands the read held at the stub fixture `name`, 200, and waits until it is judged.
    async fn answer(&self, name: &str) {
        self.answer_body(name, answer(name)).await;
    }

    /// Hands the read held at the stub `body`, 200, and waits until it is judged.
    async fn answer_body(&self, what: &str, body: Vec<u8>) {
        let before = self.http_results();
        self.venue.reply(200, body);
        until(what, || self.http_results() > before && self.judged()).await;
        churn().await;
    }

    /// Hands the held read a failure (`status`) or an answer the codec refuses, and waits until
    /// the session took it.
    async fn refuse(&self, status: u16, body: Vec<u8>) {
        let before = self.http_results();
        self.venue.reply(status, body);
        until("a refused read", || self.http_results() > before).await;
        churn().await;
    }

    /// The read due at `secs`: the clock moved to it, the read at the stub, answered `name`.
    async fn read(&self, secs: i64, name: &str) {
        self.to(secs).await;
        self.asked().await;
        self.answer(name).await;
    }
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets every task run, without moving the clock, until `done`; panics after a minute of
/// wall-clock time, so a regression fails instead of hanging.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
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

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> mpsc::Sender<()> {
    let (thaw, frozen) = mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[9; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// The tests' configuration with the stub as the REST origin, and a REST timeout no hold of a
/// read in these tests reaches.
fn config(venue: &Venue) -> VenueConfig {
    let mut cfg = common::config();
    cfg.insert(KEY_DATA_REST_URL, &venue.url);
    cfg.insert(KEY_REST_TIMEOUT, "600000ms");
    cfg
}

fn session_config(cfg: &VenueConfig, plan: EndpointPlan, clock: IngestClock) -> MdSessionConfig {
    let caps = Hibachi.caps(cfg).unwrap();
    MdSessionConfig {
        venue: &Hibachi,
        cfg: cfg.clone(),
        plan,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(100),
            Duration::from_millis(800),
            100,
            Duration::from_secs(60),
            Duration::MAX,
        )
        .unwrap(),
        clock,
        http_max_body: 1 << 16,
        conn: CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// Runs the poll endpoint from `origin` (Unix seconds) through `drive`, journaled under `name`;
/// then replays the journal twice, each time giving the same events. What the consumer saw, and
/// the session's decode errors.
async fn timeline<F>(name: &str, origin: i64, quotes: bool, drive: F) -> (Consumer, u64)
where
    F: AsyncFnOnce(&Run),
{
    let frozen = freeze();
    let venue = Venue::start();
    let cfg = config(&venue);
    let plan = poll_plan(&cfg, &subs());
    let root = fresh_dir(name);
    let sink_config = SinkConfig {
        budget_bytes: 1 << 22,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(sink_config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let kept = Rc::new(RefCell::new(Vec::new()));
    let tee = Tee {
        sink,
        kept: kept.clone(),
    };
    let consumer = Consumer {
        quotes,
        ..Consumer::default()
    };
    let clock = IngestClock::replayed(WallNs(origin * NS));
    let config = session_config(&cfg, plan.clone(), clock);
    let (mut session, control) = MdSession::new(config, consumer.clone()).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(tee))));
    let run = Run {
        venue,
        kept,
        consumer: consumer.clone(),
        at: RefCell::new(0),
    };
    let script = async {
        drive(&run).await;
        drop(control);
    };
    let (ran, ()) = tokio::join!(session.run(), script);
    ran.unwrap();
    drop(frozen);
    let decode_errors = session.counters().decode_errors;
    drop(session);
    drop(run);
    writer.close().unwrap();

    // Decoder replay of the journal: the same events, stamps included, every time.
    let live = consumer.events.borrow().clone();
    for _ in 0..2 {
        let replayed = Consumer::default();
        let config = MdReplayConfig {
            venue: &Hibachi,
            cfg: cfg.clone(),
            plan: plan.clone(),
            specs: specs(),
            conn: CONN,
        };
        let mut replay = MdReplay::new(config, replayed.clone()).unwrap();
        replay
            .run(JournalReader::open(&root, SHARD).unwrap())
            .unwrap();
        drop(replay);
        assert_eq!(*replayed.events.borrow(), live, "{name}: replay");
    }
    fs::remove_dir_all(&root).unwrap();
    (consumer, decode_errors)
}

/// What the planner built for ETH under a status: the kinds of command, and why it refused
/// the rest.
#[derive(Clone, Debug, PartialEq)]
struct Quoted {
    status: TradingStatus,
    commands: Vec<&'static str>,
    refused: Vec<(Side, u16, PlanRefusal)>,
}

/// The order caps the planner is given: an amending venue (no Hibachi order entry exists yet;
/// BT-604), one command at a time.
fn order_caps() -> OrderCaps {
    use fbc_core::{
        AckModel, CancelOnDisconnect, ClientIdFormat, NonceScope, OrderKindTag, OrderingKey,
        SnapshotReads, SnapshotSource, Support, TifTag,
    };
    OrderCaps {
        kinds: TagSet::of(&[OrderKindTag::Limit]),
        tifs: TagSet::of(&[TifTag::Gtc]),
        channels: TagSet::of(&[Channel::Public]),
        post_only: true,
        reduce_only: true,
        flag_conflicts: vec![],
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: true,
            ack: AmendAck::ReplacedEvent,
            qty_semantics: AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        cancel_refs: TagSet::of(&[RefKind::Venue]),
        query_refs: TagSet::of(&[RefKind::Venue]),
        cancel_before_ack: false,
        cancel_is_signed: false,
        batch_place: None,
        batch_cancel: None,
        cancel_all_account: Support::Unsupported,
        cancel_all_instrument: Support::Unsupported,
        cancel_on_disconnect: CancelOnDisconnect::PerConnection {
            rearm_on_reconnect: true,
            covers_open_orders: false,
        },
        ack: AckModel::SinglePhase,
        client_id: ClientIdFormat::Uuid,
        cid_echoed_on_events: true,
        nonce_scope: NonceScope::PerAccountMonotonic,
        ordering_key: OrderingKey::VenueSeq,
        snapshot_source: SnapshotSource::Trustworthy,
        snapshot_reads: SnapshotReads::OneInstant,
        snapshot_lag_bound: Duration::ZERO,
        events_echo_flags: false,
        sign_cost_hint_us: 0,
    }
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// A post-only GTC bid of one lot at `px`, tagged `tag`.
fn bid(px: i64, tag: u16) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(1),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        tag,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    }
}

/// A consumer's quoter on ETH, armed and Quoting on a registry of its own, holding two
/// acknowledged bids at levels 0 and 1, planned while ETH traded; then one pass with `status`
/// as the venue's market data last reported it, wanting level 0 moved (an amend), level 1 gone
/// (a cancel) and a new level 2 (a place).
fn quote_eth(status: TradingStatus) -> Quoted {
    static NEXT: AtomicU16 = AtomicU16::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-hibachi-status-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let caps = order_caps();
    let account = format!("acct-{n}");
    let hibachi = Hibachi.caps(&common::config()).unwrap();
    let symbol = dispatch_market_data(&hibachi, |s| s.venue_symbol("ETH/USDT-P")).unwrap();
    let market = MarketCapsConfig {
        inventory: Some(lots(50)),
        resting: Some(lots(1_000_000)),
    };
    let keys = LeaseKeys::new("synthetic", &account, &caps).with_market(ETH, symbol.clone());
    let mut reg = Registry::with_caps(PreTradeCaps::new().with_market(ETH, market).unwrap())
        .with_lease_keys(keys);
    let ladder = LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap();
    let snapshot = ResyncSnapshot {
        watermark: WallNs(1_000),
        requested_at: MonoNs(1_000),
        orders: vec![],
        positions: vec![(ETH, SignedLots(0))],
    };
    let order_key = OrderKey {
        venue: None,
        ingest: 1,
    };
    reg.resync(&ladder, &caps, &snapshot, order_key).unwrap();
    let market_lease = MarketLease::acquire(&dir, "synthetic", &account, &symbol).unwrap();
    let account_lease = AccountLease::acquire(&dir, "synthetic", &account).unwrap();
    reg.start(
        ETH,
        Leases::market(market_lease).with_account(account_lease),
    )
    .unwrap();
    let lease = NamespaceLease::acquire(&dir, AccountKey::new(n), Namespace::new(n)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let config = PlannerConfig::new(Bps(2.0), lots(2), Duration::ZERO).unwrap();
    let mut planner = ExecutionPlanner::new(config);
    let acct = AccountKey::new(1);
    let venue = |status| PlanVenue {
        caps: &caps,
        account_mode: VenueMode::Normal,
        market_mode: VenueMode::Normal,
        status,
        rates: RateView::new(Vec::new()),
        batching: Batching::One,
    };
    let resting = DesiredBook::new(ETH)
        .with(Side::Buy, 0, bid(9_900, 0))
        .with(Side::Buy, 1, bid(9_890, 1));
    let open = planner
        .plan(
            &resting,
            &mut reg,
            &venue(TRADING),
            acct,
            &mut mint,
            MonoNs(0),
        )
        .unwrap();
    for (k, p) in open.commands.iter().enumerate() {
        let vid = dispatch_market_data(&hibachi, |s| s.venue_order_id(&format!("v-{k}"))).unwrap();
        let item = ItemRef {
            idx: 0,
            cid: None,
            vid: Some(vid),
        };
        let accepted = SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        };
        reg.on_outcome(p.cid, OrderOp::Place, &item, &accepted, MonoNs(1))
            .unwrap();
    }
    let changed = DesiredBook::new(ETH)
        .with(Side::Buy, 0, bid(9_950, 0))
        .with(Side::Buy, 2, bid(9_880, 2));
    let plan = planner
        .plan(
            &changed,
            &mut reg,
            &venue(status),
            acct,
            &mut mint,
            MonoNs(2),
        )
        .unwrap();
    let commands = plan
        .commands
        .iter()
        .map(|p| match (p.stage, p.auth.command()) {
            (Stage::Cancel, VenueCommand::Cancel(_)) => "cancel",
            (_, VenueCommand::Amend(_)) => "amend",
            (_, VenueCommand::Place(_)) => "place",
            other => panic!("{other:?}"),
        })
        .collect();
    let refused = plan
        .refused
        .iter()
        .map(|r| (r.side, r.level, r.why.clone()))
        .collect();
    Quoted {
        status,
        commands,
        refused,
    }
}

#[tokio::test(start_paused = true)]
async fn each_change_is_reported_once_at_the_poll_that_sees_it_and_the_planner_follows_it() {
    let t = WEDNESDAY;
    let (consumer, errors) = timeline("hibachi_status_changes", t, true, async |run: &Run| {
        run.asked().await;
        run.answer("live").await;
        run.read(60, "live").await;
        // LIVE to PAUSED and back.
        run.read(120, "eth_paused").await;
        run.read(180, "eth_paused").await;
        run.read(240, "live").await;
        // The exchange enters SCHEDULED_MAINTENANCE (its window, from t+290 to t+330, first
        // published now) and leaves it once the window and the window after it have passed.
        run.read(300, "scheduled_maintenance").await;
        run.read(360, "scheduled_maintenance").await;
        run.read(420, "scheduled_maintenance").await;
        run.read(480, "live").await;
        // A failed read, twice, then one that succeeds.
        run.to(540).await;
        run.asked().await;
        run.refuse(503, Vec::new()).await;
        run.to(600).await;
        run.asked().await;
        run.refuse(503, Vec::new()).await;
        run.read(660, "live").await;
        // A refused answer (ETH's status null), then one taken.
        run.to(720).await;
        run.asked().await;
        run.refuse(200, answer("status_null")).await;
        run.read(780, "live").await;
        assert_eq!(run.consumer.count(), 23);
    })
    .await;
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((120.0, ETH, HALTED));
    expected.push((240.0, ETH, TRADING));
    for (halted, lifted) in [(300.0, 480.0), (540.0, 660.0), (720.0, 780.0)] {
        expected.extend(each(halted, &MARKETS, HALTED));
        expected.extend(each(lifted, &MARKETS, TRADING));
    }
    assert_eq!(consumer.statuses(t), expected);
    // The refused answer is the session's one decode error.
    assert_eq!(errors, 1);
    // Every status the handler passed the planner: on a halted ETH it builds no place or amend
    // and still the cancel; trading, all three.
    let trading = Quoted {
        status: TRADING,
        commands: vec!["cancel", "amend", "place"],
        refused: vec![],
    };
    let halted = Quoted {
        status: HALTED,
        commands: vec!["cancel"],
        refused: vec![
            (Side::Buy, 0, PlanRefusal::Status(HALTED)),
            (Side::Buy, 2, PlanRefusal::Status(HALTED)),
        ],
    };
    let mut quoted = vec![trading.clone()];
    for _ in 0..4 {
        quoted.push(halted.clone());
        quoted.push(trading.clone());
    }
    assert_eq!(*consumer.planned.borrow(), quoted);
}

/// The Friday timeline under the window fixture `window`: what was reported.
async fn friday(name: &str, window: &'static str) -> Seen {
    let t = FRIDAY;
    let (consumer, errors) = timeline(name, t, false, async move |run: &Run| {
        run.asked().await;
        run.answer(window).await;
        // A read sent at t+60, before the window's begin (t+90), held; the begin halts every
        // market; the read, answered LIVE and NORMAL, lifts nothing.
        run.to(60).await;
        run.asked().await;
        let before = run.consumer.count();
        run.to(90).await;
        until("the window's halt", || run.consumer.count() == before + 3).await;
        run.answer(window).await;
        run.read(120, window).await;
        // The window ends at t+150; the window after it runs to t+270.
        run.read(180, "live").await;
        run.read(240, "live").await;
        run.read(300, "live").await;
        for k in 6..=8 {
            run.read(k * INTERVAL, "live").await;
        }
        // A read sent at t+540, before the FX weekend closes at t+600, held; the close halts
        // EUR; the read, and the one sent at the close, answered LIVE: EUR stays halted.
        assert_eq!(FX_CLOSE, t + 600);
        run.to(540).await;
        run.asked().await;
        let before = run.consumer.count();
        run.to(600).await;
        until("the session's halt", || run.consumer.count() == before + 1).await;
        run.answer("live").await;
        run.asked().await;
        run.answer("live").await;
        run.read(660, "live").await;
    })
    .await;
    assert_eq!(errors, 0);
    consumer.statuses(t)
}

#[tokio::test(start_paused = true)]
async fn a_window_and_an_fx_session_end_between_reads_halt_at_their_time_and_reads_in_flight_stay_halted()
 {
    let numbers = friday("hibachi_status_window_number", "window_number").await;
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(90.0, &MARKETS, HALTED));
    assert_eq!(150 + AFTER, 270);
    expected.extend(each(300.0, &MARKETS, TRADING));
    expected.push((600.0, EUR, HALTED));
    assert_eq!(numbers, expected);
    // The window's bounds as strings: the same.
    let strings = friday("hibachi_status_window_string", "window_string").await;
    assert_eq!(strings, numbers);
}

#[tokio::test(start_paused = true)]
async fn the_fx_reopening_holds_the_pair_halted_for_the_configured_window() {
    let t = FX_REOPEN - 120;
    let (consumer, _) = timeline("hibachi_status_reopening", t, false, async |run: &Run| {
        run.asked().await;
        run.answer("live").await;
        for k in 1..=4 {
            run.read(k * INTERVAL, "live").await;
        }
    })
    .await;
    // Halted by the weekend at the first timer; trading only from a read sent the configured
    // window after the reopening (t+120 + 120).
    let expected = vec![
        (0.0, EUR, HALTED),
        (0.0, ETH, TRADING),
        (0.0, WIF, TRADING),
        (240.0, EUR, TRADING),
    ];
    assert_eq!(consumer.statuses(t), expected);
}

#[tokio::test(start_paused = true)]
async fn a_read_sent_before_a_reopening_window_ends_and_answered_after_it_is_halted() {
    // EUR's FX reopening and WIF's advertised open both at t+150, the window after them to
    // t+270.
    let t = FX_REOPEN - 150;
    assert_eq!(FX_REOPEN + AFTER, t + 270);
    let mut doc: serde_json::Value = serde_json::from_slice(&answer("live")).unwrap();
    let contracts = doc["futureContracts"].as_array_mut().unwrap();
    let wif = contracts
        .iter_mut()
        .find(|c| c["symbol"] == "WIF/USDT-P")
        .unwrap();
    wif["marketOpenTimestamp"] = serde_json::json!(FX_REOPEN.to_string());
    let opens = doc.to_string().into_bytes();
    let opens = &opens;
    let (consumer, errors) = timeline("hibachi_status_held", t, false, async move |run: &Run| {
        run.asked().await;
        run.answer_body("opens", opens.clone()).await;
        for k in 1..=3 {
            run.to(k * INTERVAL).await;
            run.asked().await;
            run.answer_body("opens", opens.clone()).await;
        }
        // The read sent at t+240, inside the window, held until t+280, after it: halted.
        run.to(240).await;
        run.asked().await;
        run.to(280).await;
        run.answer_body("opens", opens.clone()).await;
        // The read sent at t+300: trading.
        run.to(300).await;
        run.asked().await;
        run.answer_body("opens", opens.clone()).await;
    })
    .await;
    assert_eq!(errors, 0);
    let expected = vec![
        (0.0, EUR, HALTED),
        (0.0, ETH, TRADING),
        (0.0, WIF, HALTED),
        (300.0, EUR, TRADING),
        (300.0, WIF, TRADING),
    ];
    assert_eq!(consumer.statuses(t), expected);
}

#[tokio::test(start_paused = true)]
async fn a_close_between_reads_and_a_market_a_read_leaves_out_are_halted() {
    let t = WEDNESDAY;
    let (consumer, _) = timeline("hibachi_status_closes", t, false, async |run: &Run| {
        run.asked().await;
        run.answer("live").await;
        // WIF's close, t+150, advertised from t+60.
        run.read(60, "wif_close").await;
        // A read sent at t+120, before the close, held past it: the close halts WIF; the read
        // and the next, both listing it LIVE with that close, leave it halted.
        run.to(120).await;
        run.asked().await;
        let before = run.consumer.count();
        run.to(150).await;
        until("the close's halt", || run.consumer.count() == before + 1).await;
        run.answer("wif_close").await;
        run.read(180, "wif_close").await;
        // A read sent at t+240, answered at t+260, after a close (ETH's, t+250) it is the first
        // to advertise: ETH halted.
        run.to(240).await;
        run.asked().await;
        run.to(260).await;
        run.answer("eth_close_first").await;
        // A read leaving EUR out.
        run.read(300, "eur_left_out").await;
    })
    .await;
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, WIF, HALTED));
    expected.push((260.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    expected.push((300.0, EUR, HALTED));
    assert_eq!(consumer.statuses(t), expected);
}

#[tokio::test(start_paused = true)]
async fn a_close_in_milliseconds_or_not_a_number_and_a_window_bound_in_milliseconds_are_refused() {
    let t = WEDNESDAY;
    let refused = ["close_ms", "close_not_number", "window_ms"];
    let (consumer, errors) = timeline("hibachi_status_refused", t, false, async |run: &Run| {
        run.asked().await;
        run.answer("live").await;
        for (k, name) in refused.iter().enumerate() {
            let at = 120 * k as i64 + 60;
            run.to(at).await;
            run.asked().await;
            run.refuse(200, answer(name)).await;
            run.read(at + 60, "live").await;
        }
    })
    .await;
    let mut expected = each(0.0, &MARKETS, TRADING);
    for k in 0..3 {
        let at = (120 * k + 60) as f64;
        expected.extend(each(at, &MARKETS, HALTED));
        expected.extend(each(at + 60.0, &MARKETS, TRADING));
    }
    assert_eq!(consumer.statuses(t), expected);
    assert_eq!(errors, 3);
}

#[test]
fn the_tests_markets_and_subscriptions_are_the_poll_endpoints() {
    // Every market a subscription names, and only those, is one the poll reports.
    let markets: BTreeSet<InstrumentId> = subs().iter().map(|s| s.inst).collect();
    assert_eq!(markets, BTreeSet::from(MARKETS));
    assert_eq!(INTERVAL, 60);
}
