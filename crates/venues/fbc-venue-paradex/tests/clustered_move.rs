//! FBC-vgm's done line (BT-402; design §5.3, restated: a clustered reference move arriving
//! during a ladder rebuild; decisions 0002, 0100): the clustered-move benchmark. One shard runs
//! two sessions on fbc-runtime's shard host: Paradex's market data from a reference server on a
//! thread of its own (`clustered_move/reference.rs`), and Paradex's order entry, this crate's
//! codec signing every place, against fbc-conformance's stub server standing in for Paradex
//! (`rehearsal/venue.rs`), wired to fbc-oms as a consumer wires it (`rehearsal/glue.rs`).
//!
//! **The fixture.** Once the market is started, the decide pass puts eight resting buys on the
//! host's ladder, then, round after round, a ladder of eight more. Each time the stub reads the
//! first, third, fifth or seventh place of a round, the reference server sends a burst of two
//! bbo frames, the market moving down a tick a frame, so they arrive while the shard is
//! signing the round's next place. The market-data handler answers each frame with a protective
//! cancel of the oldest order still resting, built and authorized by fbc-oms. A round ends once
//! its places rest and its eight cancels have ended their orders; the next round's ladder
//! replaces them. Eight rounds: 72 places signed, 64 protective cancels.
//!
//! **What it shows.** Every protective cancel is written, after at most one place the host had
//! submitted when the handler submitted the cancel (the host holds the rest of the ladder). On
//! Linux, as CI runs it, every cancel's frame has a kernel receive time and its tick-to-wire is
//! reported on the order-entry session (FBC-qfm): every cancel is written after at most one
//! place whose signature was still in progress when its frame was received, and the benchmark
//! reports the p99 of their kernel-receive-to-write-done times. The 0.75 ms target is the
//! consumer's deployment trigger on the shared box, not this test's. Elsewhere (developer Macs;
//! not run in CI) no frame has a kernel receive time: the benchmark says so and reports none.
//!
//! The run is on real time, the servers answering on the loopback at once; a bound fails a hang.

mod common;
#[allow(dead_code)]
#[path = "rehearsal/glue.rs"]
mod glue;
mod md;
#[path = "clustered_move/reference.rs"]
mod reference;
#[allow(dead_code)]
#[path = "rehearsal/venue.rs"]
mod venue;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::Vectors;
use common::secrets::synthetic;
use fbc_core::{
    AccountKey, Channel, CidMint, ClientOrderId, ConnKey, Envelope, ExecEvent, Feed, InstrumentId,
    Lots, MarketLease, MdEvent, Namespace, NamespaceLease, NewOrder, NonceBlock, NonceSource,
    OrderCaps, OrderKind, RpcId, Secret, Secrets, Side, SizeStep, SpecTable, Stamp, SubmitHandle,
    Subscription, Ticks, Tif, VenueConfig, VenueFactory, VenueSymbol, WallNs,
};
use fbc_journal::NonceSourceId;
use fbc_oms::{
    CancelChoice, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig,
    OrdState, PreTradeCaps, Registry,
};
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, InFlight, IngestClock,
    Ladder, Liveness, MdSession, MdSessionConfig, Member, ProxyConfig, RateLimiter,
    ReconnectPacing, ReservedBound, RpcIds, SafetyReserve, ShardHost, TickToWire, WriteStall,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::exec_caps;
use fbc_venue_paradex::factory::{EXEC_MODE, EXEC_URL, MD_URL, RPC_TIMEOUT};
use fbc_venue_paradex::md::BBO;
use glue::{Glue, GlueHandler, wall_now};
use reference::Reference;
use rust_decimal::Decimal;
use tokio::sync::Notify;
use venue::{Book, Fate, MARKET, Shared, Stub, lock};

const INST: InstrumentId = md::BTC;
const ACCT: AccountKey = AccountKey::new(1);
const NS: Namespace = Namespace::new(1);
/// The lease files' names: the venue's id and a label for the account, never its address.
const VENUE: &str = "PARADEX";
const ACCOUNT_LABEL: &str = "clustered-move";
/// The owner's $11 resting per side and $50 inventory, in lots of 0.00001 at about 60000.
const RESTING_CAP: i64 = 18;
const INVENTORY_CAP: i64 = 83;
/// The order-entry and market-data connections' numbers (a write is attributed to an input by
/// its connection).
const EXEC_CONN: u16 = 0;
const MD_CONN: u16 = 1;

/// The resting buys placed before the first round, which its frames cancel.
const TARGETS: u64 = 8;
/// The places of each round's ladder.
const LADDER: u64 = 8;
/// The rounds.
const ROUNDS: u64 = 8;
/// The frames of a burst.
const BURST: usize = 2;
/// The frames, and so the protective cancels, of each round: a burst each time the stub reads
/// the round's first, third, fifth or seventh place.
const FRAMES_PER_ROUND: u64 = LADDER / 2 * BURST as u64;
/// What the stub answers on the order connection: the opening six (the auth frame, four
/// private subscriptions and the cancel-on-disconnect arm), every place and every cancel.
const EXEC_FRAMES: u64 = 6 + TARGETS + ROUNDS * LADDER + ROUNDS * FRAMES_PER_ROUND;
/// How long the whole run may take before the test fails.
const BOUND: Duration = Duration::from_secs(120);

/// Whether the stub's `created`th create (from 1) is a round's first, third, fifth or seventh
/// place, so the reference server sends a burst then.
fn bursts_after(created: u64) -> bool {
    created
        .checked_sub(TARGETS + 1)
        .is_some_and(|at| (at % LADDER).is_multiple_of(2))
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// BTC-USD-PERP on its 0.1 tick and 0.00001 size step.
fn specs() -> SpecTable {
    let mut spec = md::spec(INST, MARKET);
    spec.size_step = SizeStep::new(Decimal::from_str("0.00001").unwrap()).unwrap();
    let mut table = SpecTable::new();
    table.insert(spec);
    table
}

fn symbol() -> VenueSymbol {
    specs().get(INST).unwrap().venue_symbol.clone()
}

fn order_caps() -> OrderCaps {
    exec_caps().order
}

fn secrets() -> Secrets {
    let (account, key) = synthetic();
    let mut creds = Secrets::new();
    creds.insert(ACCOUNT_ADDRESS, Secret::new(account));
    creds.insert(SIGNING_KEY, Secret::new(key));
    creds
}

/// Paradex's configuration: order entry on the stub, market data from the reference server.
fn config(stub: &Stub, reference: &Reference) -> VenueConfig {
    let chain = Vectors::read().header["chain_id"].clone();
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (EXEC_URL, stub.ws_url().as_str()),
        (EXEC_MODE, "orders"),
        (RPC_TIMEOUT, "5000ms"),
        (REST_URL, stub.rest_url().as_str()),
        (CHAIN_ID, chain.as_str()),
        (SIGNATURE_LIFETIME, "600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
        (MD_URL, reference.url()),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// A directory of the test's own under the system's temporary directory, removed with
/// everything in it when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TempDir {
        let dir =
            std::env::temp_dir().join(format!("fbc-paradex-clustered-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Nonces counted up from 1 (Paradex asks for none).
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// A registry under the owner's caps, its lease names given: a trustworthy resync seeds it
/// (decision 0094).
fn registry() -> Registry {
    let limits = MarketCapsConfig {
        inventory: Some(lots(INVENTORY_CAP)),
        resting: Some(lots(RESTING_CAP)),
    };
    let caps = PreTradeCaps::new().with_market(INST, limits).unwrap();
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps()).with_market(INST, symbol());
    Registry::with_caps(caps).with_lease_keys(keys)
}

fn glue() -> Glue {
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3_600),
            max_entries: 10_000,
        },
        wall_now(),
    )
    .unwrap();
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    Glue::new(registry(), ledger, order_caps(), ladder)
}

fn pacing() -> ReconnectPacing {
    ReconnectPacing::new(
        Duration::from_millis(10),
        Duration::from_millis(100),
        20,
        Duration::from_secs(60),
        Duration::from_secs(5),
    )
    .unwrap()
}

/// The order-entry session's configuration, stamping from `clock`.
fn exec_config(cfg: &VenueConfig, clock: &IngestClock) -> ExecSessionConfig {
    let venue: &'static ParadexFactory = &ParadexFactory;
    let caps = venue.caps(cfg).unwrap();
    ExecSessionConfig {
        venue,
        cfg: cfg.clone(),
        creds: secrets(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: pacing(),
        clock: clock.clone(),
        nonces: Box::new(Counting(1)),
        nonce_source: NonceSourceId(1),
        conn: EXEC_CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(10).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(10)).unwrap(),
        http_max_body: 1 << 20,
        outbound: ReservedBound::new(64, 64).unwrap(),
        in_flight: InFlight::new(ReservedBound::new(64, 64).unwrap()),
    }
}

/// The market-data session's configuration, BTC-USD-PERP's bbo, stamping from `clock`.
fn md_config(cfg: &VenueConfig, clock: &IngestClock) -> MdSessionConfig {
    let subs = BTreeSet::from([Subscription {
        inst: INST,
        feed: Feed::Touch(BBO),
    }]);
    let mut plans = ParadexFactory.plan_md(cfg, &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1);
    let caps = ParadexFactory.caps(cfg).unwrap();
    MdSessionConfig {
        venue: &ParadexFactory,
        cfg: cfg.clone(),
        plan: plans.remove(0),
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: pacing(),
        clock: clock.clone(),
        http_max_body: 64 * 1024,
        conn: MD_CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// What the order-entry session reported beyond what the glue keeps.
#[derive(Default)]
struct Reported {
    /// The request ids of the protective cancels, as the market-data handler submitted them.
    cancels: HashSet<RpcId>,
    /// Each place reported encoded (its signature done; reported before its write), with the
    /// wall clock then.
    places: Vec<(RpcId, WallNs)>,
    /// Every submission reported not sent.
    not_sent: Vec<SubmitHandle>,
    /// The tick-to-wire samples (Linux only).
    ticks: Vec<TickToWire>,
}

/// The order-entry handler: the glue's, keeping what [`Reported`] holds as well.
struct Exec {
    glue: GlueHandler,
    reported: Rc<RefCell<Reported>>,
}

impl ExecHandler for Exec {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.glue.on_exec(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.glue.on_epoch_end(key);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        {
            let mut reported = self.reported.borrow_mut();
            if handle.receipt.is_err() {
                reported.not_sent.push(handle.clone());
            } else if !reported.cancels.contains(&handle.rpc) {
                reported.places.push((handle.rpc, wall_now()));
            }
        }
        self.glue.on_submitted(handle);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.reported.borrow_mut().ticks.push(sample);
    }
}

/// One protective cancel, as the market-data handler submitted it.
#[derive(Clone, Debug)]
struct Cancel {
    cid: ClientOrderId,
    rpc: RpcId,
    /// The stamp of the frame it answered.
    frame: Stamp,
    /// The places reported encoded when it was submitted.
    places_encoded: usize,
}

/// Where the benchmark stands, as the decide pass moves it on.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Phase {
    /// Waiting until the session takes places and the reference feed's warm-up was handled.
    Setup,
    /// The resting buys the first round cancels are being placed.
    Targets,
    /// Round `n` (from 0) is going out.
    Round(u64),
    Done,
}

/// The benchmark's state, shared by the decide pass and the market-data handler.
struct Bench {
    mint: CidMint,
    dir: TempDir,
    phase: Phase,
    /// The orders the next frames cancel, oldest first.
    pool: VecDeque<ClientOrderId>,
    /// The places of the targets or the current round.
    placing: Vec<ClientOrderId>,
    /// The bbo frames handed to the market-data handler, the warm-up first.
    frames: u64,
    /// The places put on the ladder, for their prices.
    planned: u64,
    cancels: Vec<Cancel>,
    done: Rc<Notify>,
}

fn is_open(g: &Glue, cid: ClientOrderId) -> bool {
    matches!(g.reg.get(cid).map(|r| r.state()), Some(OrdState::Open))
}

fn is_terminal(g: &Glue, cid: ClientOrderId) -> bool {
    g.reg.get(cid).is_some_and(|r| r.state().is_terminal())
}

impl Bench {
    /// The decide pass: moves the benchmark on once what the phase waits for has happened.
    fn decide(&mut self, glue: &RefCell<Glue>, orders: &ExecOrders, ladder: &mut Ladder) {
        let next = match self.phase {
            Phase::Setup => {
                if !(orders.may_place() && self.frames >= 1) {
                    return;
                }
                let mut g = glue.borrow_mut();
                let lease = MarketLease::acquire(self.dir.path(), VENUE, ACCOUNT_LABEL, &symbol());
                g.reg.start(INST, Leases::market(lease.unwrap())).unwrap();
                self.placing = self.ladder(&mut g, orders, ladder, TARGETS);
                self.phase = Phase::Targets;
                return;
            }
            Phase::Targets => 0,
            Phase::Round(n) => n + 1,
            Phase::Done => return,
        };
        let g = glue.borrow();
        let placed = self.placing.iter().all(|&cid| is_open(&g, cid));
        let cancelled = self.cancels.len() == usize::try_from(next * FRAMES_PER_ROUND).unwrap()
            && self.cancels.iter().all(|c| is_terminal(&g, c.cid));
        drop(g);
        if !(placed && cancelled) {
            return;
        }
        if next == ROUNDS {
            self.phase = Phase::Done;
            self.done.notify_one();
            return;
        }
        assert!(self.pool.is_empty(), "{:?}", self.pool);
        self.pool = std::mem::take(&mut self.placing).into();
        let mut g = glue.borrow_mut();
        self.placing = self.ladder(&mut g, orders, ladder, LADDER);
        self.phase = Phase::Round(next);
    }

    /// Puts `n` resting buys of a lot each on `ladder`, built and authorized by fbc-oms.
    fn ladder(
        &mut self,
        g: &mut Glue,
        orders: &ExecOrders,
        ladder: &mut Ladder,
        n: u64,
    ) -> Vec<ClientOrderId> {
        (0..n)
            .map(|_| {
                self.planned += 1;
                let px = 599_000 - i64::try_from(self.planned % 50).unwrap();
                let order = NewOrder {
                    cid: self.mint.mint().unwrap(),
                    inst: INST,
                    side: Side::Buy,
                    kind: OrderKind::Limit { px: Ticks(px) },
                    qty: lots(1),
                    tif: Tif::Gtc,
                    channel: Channel::Public,
                    post_only: true,
                    reduce_only: false,
                    reducing: false,
                };
                let cid = order.cid;
                let cmd = g.reg.place(order).unwrap();
                let auth = g.reg.authorize(ACCT, cmd).unwrap();
                g.planned(&auth);
                ladder.push(orders, auth).unwrap();
                cid
            })
            .collect()
    }

    /// The market-data handler's answer to a bbo: after the warm-up, a protective cancel of the
    /// oldest order still resting, submitted at once.
    fn on_bbo(
        &mut self,
        frame: Stamp,
        glue: &RefCell<Glue>,
        orders: &ExecOrders,
        reported: &RefCell<Reported>,
    ) {
        self.frames += 1;
        if self.frames == 1 {
            return;
        }
        let cid = self
            .pool
            .pop_front()
            .expect("a frame for each order resting");
        let mut g = glue.borrow_mut();
        let permit = g.reg.cancellable(cid).unwrap();
        let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
            panic!("a cancel of a resting order is sent");
        };
        let auth = g.reg.authorize(ACCT, cmd).unwrap();
        let rpc = g.submit(orders, auth);
        let mut reported = reported.borrow_mut();
        reported.cancels.insert(rpc);
        self.cancels.push(Cancel {
            cid,
            rpc,
            frame,
            places_encoded: reported.places.len(),
        });
    }
}

/// What one run showed.
struct Shown {
    cancels: Vec<Cancel>,
    reported: Rc<RefCell<Reported>>,
    /// The JSON-RPC requests the stub read on the order connection, in order.
    requests: Vec<serde_json::Value>,
}

/// Runs the benchmark.
async fn run() -> Shown {
    let reference = Reference::start();
    let book: Shared = Arc::new(Mutex::new(Book::default()));
    {
        let mut book = lock(&book);
        let fates = TARGETS + ROUNDS * LADDER;
        book.fates.extend((0..fates).map(|_| Fate::Rest));
        let bursts = reference.bursts();
        book.on_create = Some(Box::new(move |created| {
            if bursts_after(created) {
                // The reference server is gone only once the market-data session closed.
                let _ = bursts.send(BURST);
            }
        }));
    }
    let stub = Stub::start(&book, &[usize::try_from(EXEC_FRAMES).unwrap()]);
    let cfg = config(&stub, &reference);
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());

    let glue = Rc::new(RefCell::new(glue()));
    let reported: Rc<RefCell<Reported>> = Rc::default();
    let handler = Exec {
        glue: GlueHandler(Rc::clone(&glue)),
        reported: Rc::clone(&reported),
    };
    let (mut exec, exec_control) = ExecSession::new(exec_config(&cfg, &clock), handler).unwrap();
    let orders = exec.orders();
    glue.borrow_mut().watch(orders.clone());

    let dir = TempDir::new();
    let ns = NamespaceLease::acquire(dir.path(), ACCT, NS).unwrap();
    let done = Rc::new(Notify::new());
    let bench = Rc::new(RefCell::new(Bench {
        mint: CidMint::new(ns, 0, 0, wall_now()),
        dir,
        phase: Phase::Setup,
        pool: VecDeque::new(),
        placing: Vec::new(),
        frames: 0,
        planned: 0,
        cancels: Vec::new(),
        done: Rc::clone(&done),
    }));

    let (b, g, o, r) = (
        Rc::clone(&bench),
        Rc::clone(&glue),
        orders.clone(),
        Rc::clone(&reported),
    );
    let md_handler = move |env: Envelope<MdEvent>| {
        if let MdEvent::Touch { .. } = env.body {
            b.borrow_mut().on_bbo(env.stamp, &g, &o, &r);
        }
    };
    let (mut md, md_control) = MdSession::new(md_config(&cfg, &clock), md_handler).unwrap();

    let (b, g) = (Rc::clone(&bench), Rc::clone(&glue));
    let mut decide = move |_: &[InstrumentId], ladder: &mut Ladder| {
        b.borrow_mut().decide(&g, &orders, ladder);
    };
    let script = async {
        done.notified().await;
        // Every frame the stub was to answer has been answered: the last round's cancels have
        // ended their orders.
        stub.finished().await.unwrap();
        drop((md_control, exec_control));
    };
    let members = vec![Member::md(&mut md), Member::exec(&mut exec)];
    let both = async { tokio::join!(host.run(members, &mut decide), script) };
    let (ran, ()) = tokio::time::timeout(BOUND, both)
        .await
        .expect("the benchmark did not end within its bound");
    ran.unwrap();
    assert_eq!(md.counters().decode_errors, 0);
    assert_eq!(exec.counters().decode_errors, 0);
    drop((md, exec, decide));
    reference.join();
    let requests = stub.requests(0);
    assert_eq!(stub.connections().len(), 1);
    let cancels = bench.borrow().cancels.clone();
    assert_eq!(bench.borrow().phase, Phase::Done);
    Shown {
        cancels,
        reported,
        requests,
    }
}

/// Every place and protective cancel was written, nothing refused; each cancel was written
/// after at most one place the host had submitted when the handler submitted it: the places
/// written before it number at most one more than those encoded by then. The places written
/// before each cancel, in the order the cancels were submitted.
fn every_cancel_written_behind_at_most_one_place(shown: &Shown) -> Vec<usize> {
    let reported = shown.reported.borrow();
    assert!(reported.not_sent.is_empty(), "{:?}", reported.not_sent);
    let total = usize::try_from(ROUNDS * FRAMES_PER_ROUND).unwrap();
    assert_eq!(shown.cancels.len(), total);
    let places = usize::try_from(TARGETS + ROUNDS * LADDER).unwrap();
    assert_eq!(reported.places.len(), places);
    let method = |v: &serde_json::Value| v["method"].as_str().unwrap_or("").to_owned();
    let methods: Vec<String> = shown.requests.iter().map(method).collect();
    let count = |m: &str| methods.iter().filter(|x| *x == m).count();
    assert_eq!(
        (count("order.create"), count("order.cancel")),
        (places, total)
    );
    // Every frame a cancel answered is a market-data frame, each cancel its own.
    let frames: HashSet<u64> = shown.cancels.iter().map(|c| c.frame.ingest_seq).collect();
    assert_eq!(frames.len(), total);
    shown
        .cancels
        .iter()
        .map(|c| {
            assert_eq!(c.frame.conn.conn, MD_CONN);
            let at = shown
                .requests
                .iter()
                .position(|v| v["method"] == "order.cancel" && v["id"] == c.rpc.0)
                .unwrap_or_else(|| panic!("cancel {c:?} written"));
            let before = methods[..at]
                .iter()
                .filter(|m| *m == "order.create")
                .count();
            assert!(
                c.places_encoded <= before && before <= c.places_encoded + 1,
                "{c:?}: {before} places written before it"
            );
            before
        })
        .collect()
}

/// The `p`th percentile (nearest rank) of `sorted`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn percentile(sorted: &[i64], p: usize) -> i64 {
    let rank = (sorted.len() * p).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Writes `line` where a passing test's output is seen: the test's standard error, past the
/// harness's capture, and the job summary when the CI step gives one.
fn report(line: &str) {
    let _ = writeln!(std::io::stderr(), "{line}");
    if let Some(path) = std::env::var_os("GITHUB_STEP_SUMMARY")
        && let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(path)
    {
        let _ = writeln!(f, "{line}\n");
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_clustered_move_during_a_ladder_has_every_protective_cancel_written_after_at_most_one_signature_in_progress_and_reports_its_p99()
 {
    let shown = run().await;
    let written = every_cancel_written_behind_at_most_one_place(&shown);
    let reported = shown.reported.borrow();
    let mut nanos = Vec::new();
    let mut waited = 0;
    for (c, before) in shown.cancels.iter().zip(written) {
        let rx = c
            .frame
            .kernel_rx
            .expect("a frame read on Linux has a kernel receive time")
            .0;
        // Its one sample, on the order-entry session, from the frame's kernel receive.
        let samples: Vec<&TickToWire> = reported
            .ticks
            .iter()
            .filter(|t| t.frame == c.frame)
            .collect();
        let [tick] = samples.as_slice() else {
            panic!("{c:?}: {samples:?}");
        };
        assert_eq!(tick.stream, fbc_venue_paradex::factory::EXEC_STREAM);
        assert!(tick.nanos >= 0, "{tick:?}");
        // The places whose signatures were done when the frame was received; of those written
        // before the cancel, the rest were signed after it: at most one, the signature then in
        // progress.
        let signed = reported.places.iter().filter(|(_, at)| at.0 <= rx).count();
        let in_progress = before.saturating_sub(signed);
        assert!(
            in_progress <= 1,
            "{c:?}: {in_progress} signatures after its frame was received"
        );
        waited += in_progress;
        nanos.push(tick.nanos);
    }
    // How many cancels waited for a signature is reported, not asserted: whether a burst lands
    // inside a signature or in the poll between two is the scheduler's (Codex r4315689623 on
    // PR #179).
    // No sample is for a write attributed to another market-data frame.
    let md_ticks = reported
        .ticks
        .iter()
        .filter(|t| t.frame.conn.conn == MD_CONN)
        .count();
    assert_eq!(md_ticks, nanos.len());
    nanos.sort_unstable();
    let us = |n: i64| n as f64 / 1_000.0;
    report(&format!(
        "FBC-vgm clustered move (Linux): {} protective cancels during {} signed places; \
         kernel receive to write done p50 {:.1} us, p99 {:.1} us, max {:.1} us; {} waited \
         for a signature in progress, none for more than one",
        nanos.len(),
        reported.places.len(),
        us(percentile(&nanos, 50)),
        us(percentile(&nanos, 99)),
        us(*nanos.last().unwrap()),
        waited,
    ));
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn off_linux_a_clustered_move_during_a_ladder_has_every_protective_cancel_written_and_reports_that_kernel_rx_is_absent()
 {
    let shown = run().await;
    every_cancel_written_behind_at_most_one_place(&shown);
    assert!(shown.cancels.iter().all(|c| c.frame.kernel_rx.is_none()));
    let reported = shown.reported.borrow();
    assert!(reported.ticks.is_empty(), "{:?}", reported.ticks);
    report(&format!(
        "FBC-vgm clustered move ({}): kernel_rx is absent on this system, so no \
         kernel-receive-to-write-done time is reported; {} protective cancels during {} \
         signed places, each written behind at most one place the host had submitted",
        std::env::consts::OS,
        shown.cancels.len(),
        reported.places.len(),
    ));
}
