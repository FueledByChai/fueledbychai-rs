//! FBC-d8uk's done line (BT-402; design §5.3, restated: a clustered reference move arriving
//! during a ladder rebuild; decisions 0002, 0031, 0100): QFEX's clustered-move benchmark, the
//! kernel-receive-to-write-done tick-to-trade of protective cancels, measured as Paradex's
//! (FBC-vgm, `crates/venues/fbc-venue-paradex/tests/clustered_move.rs`) measures it. One shard
//! runs two sessions on fbc-runtime's shard host: QFEX's public market data (`bbo`, this crate's
//! MDS codec) from a reference server on a thread of its own (`clustered_move/reference.rs`),
//! and QFEX's order entry, an unchanged `ExecSession` on this crate's codec under
//! `preprod_probe` with every origin loopback (decision 0132 item 6), against fbc-conformance's
//! stub server answering the arm, the resync, `add_order` and `cancel_order` in QFEX's shapes
//! (`clustered_move/venue.rs`), wired to fbc-oms as a consumer wires it
//! (`clustered_move/glue.rs`). Nothing connects to a venue, and every key, id and value is
//! synthetic (`tests/probe_harness/`).
//!
//! **The fixture.** Once the arm is accepted and the resync has ended (decision 0058), the
//! decide pass starts the market, seeded flat by hand under a declared testnet run (decision
//! 0067; the probe caps' snapshot is untrustworthy, 0132 item 5), and puts eight resting buys on
//! the host's ladder, then, round after round, a ladder of eight more, each a post-only GTC of
//! one lot built and authorized by fbc-oms. Each time the stub reads the first, third, fifth or
//! seventh place of a round, the reference server sends a burst of two bbo frames, the market
//! moving down a tick a frame, so they arrive while the shard is placing the round's next
//! orders. The market-data handler answers each frame with a protective cancel of the oldest
//! order still resting, built and authorized by fbc-oms and submitted at once; each is Safety
//! traffic attributed to the frame it answers, so on Linux the order-entry session reports one
//! tick-to-wire sample for it (FBC-qfm). A round ends once its places rest (their `ACK`s) and
//! its eight cancels have ended their orders (their `CANCELLED`s); the next round's ladder
//! replaces them. Eight rounds: 72 places, 64 protective cancels.
//!
//! **No signature.** QFEX signs no order frame (`src/exec/order.rs`; the HMAC signs only the
//! authentication), so where every Paradex cancel waited for a STARK signature in progress, a
//! QFEX cancel waits at most for one place's encode and write: the runtime's and the codec's own
//! path. "Submitted while a place was in progress" is counted as Paradex's benchmark counts a
//! signature in progress: a cancel waited for a place when the stub read that place before it
//! and the session reported the place encoded (`on_submitted`, after the encode and before the
//! write) after the cancel's frame arrived, on the wall clock: at its kernel receive on Linux,
//! and at its ingest stamp elsewhere. A place reported encoded before the frame arrived whose
//! write was still under way is not counted, though the cancel waited for the rest of that
//! write. Off Linux a cancel waits for a place encoded after its frame was read only when the
//! host had handed the session that place before the market-data session's poll read the frame;
//! a place's write that waits on the socket lets the market-data session read a frame meanwhile,
//! but that place was encoded first, so it is not counted. On Linux the kernel may receive the
//! frame during any place's encode or write. The host hands its ladder one command at a time
//! and reads every frame delivered between two (decision 0100), so at most one place is encoded
//! after a frame arrived and written before its cancel, which the benchmark asserts (a frame the
//! kernel stamped before its data was readable could break it only if it stayed unread through
//! a whole place and the poll after it); how many cancels wait for one is the scheduler's, and
//! is reported, not asserted.
//!
//! **What it reports.** On Linux, as CI runs it, every cancel's frame has a kernel receive time,
//! and the benchmark prints one line beginning "QFEX clustered move (Linux)" with the protective
//! cancels, the places, the kernel-receive-to-write-done p50, p99 and max in microseconds, and
//! how many cancels waited for a place in progress. Each sample is held to its cancel's write:
//! its write done, on the wall clock, falls after its cancel's encode and before the next
//! request's. The order-entry session sets no journal there, as Paradex's does not, so nothing
//! but the session's own path lies between a frame and its cancel's write. Elsewhere (developer
//! Macs) no frame has a kernel receive time: the line says so and reports none. On every
//! platform it also reports the time from each cancel's frame's ingest stamp (user-space frame
//! completion, before decode) to its write done, which excludes the kernel-to-user hop: on
//! Linux from each sample (its write done on the wall clock, the frame's kernel receive plus the
//! sample, less the stamp's wall time); elsewhere from the order-entry session's journal
//! (decision 0006), whose `WriteResult` records carry the write-done instant on the shard clock
//! the stamps come from. Off Linux that figure is therefore measured with the session
//! journaling, and the line says so: before each cancel's write the session pushes its nonce,
//! its encode context and its outbound frame onto the journal's queue, as it does for each place
//! ahead of it, it journals every frame it reads on the shard's one task, and the journal's
//! writer thread competes for the CPUs. The percentiles are nearest rank, so at 64 cancels the
//! p99 is the 64th sample, the max, and the line says so.
//!
//! **Running it.** CI runs it in the dev profile on a shared runner, which overstates the
//! runtime's share. On the owner's Linux box, for figures to set against the Java library's:
//!
//! ```text
//! cargo test --release -p fbc-venue-qfex --test clustered_move -- --nocapture
//! ```
//!
//! The run is on real time, the servers answering on the loopback at once. Every step waits on
//! an event (a burst on the stub's reading a place, the next round on the registry's state, the
//! end on the stub's last answer), and a bound fails a hang; a stub script that fails, or a
//! host run that ends early, fails the run at once with its cause.

#[allow(dead_code)]
#[path = "clustered_move/glue.rs"]
mod glue;
#[allow(dead_code)]
mod probe_harness;
#[path = "clustered_move/reference.rs"]
mod reference;
#[path = "clustered_move/venue.rs"]
mod venue;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_core::{
    AccountKey, Channel, CidMint, ClientOrderId, ConnKey, Envelope, ExecEvent, Feed, InstrumentId,
    Lots, MarketLease, MdEvent, NamespaceLease, NewOrder, OrderCaps, OrderKind, RpcId, Side,
    SignedLots, SpecTable, Stamp, SubmitHandle, Subscription, Ticks, Tif, VenueConfig,
    VenueFactory, VenueSymbol, WallNs, encode_cid,
};
use fbc_journal::{
    JournalReader, JournalWriter, NonceSourceId, Record, RedactionKey, SinkConfig, WriteRes,
    journal_queue,
};
use fbc_oms::{
    CancelChoice, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrdState, PreTradeCaps,
    Registry, TestnetRun,
};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig,
    InFlight, IngestClock, Journal, Ladder, Liveness, MdSession, MdSessionConfig, Member,
    ProxyConfig, RateLimiter, RateUserKey, ReconnectPacing, ReservedBound, RpcIds, SafetyReserve,
    ShardHost, TickToWire, WriteStall,
};
use fbc_venue_qfex::exec::{CLIENT_ID_FORMAT, probe_exec_caps};
use fbc_venue_qfex::probe::KEY_RPC_TIMEOUT;
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{Qfex, TOUCH_BBO};
use glue::{Glue, GlueHandler, Item, wall_now};
use reference::Reference;
use tokio::sync::Notify;
use venue::{Book, Shared, Stub, lock};

const INST: InstrumentId = probe_harness::AAPL;
/// The consumer's account key, which the configuration binds the session to (decision 0122):
/// the harness's primary account's.
const ACCT: AccountKey = AccountKey::new(4);
/// The lease files' names: the venue's id and a label for the account, never its id.
const VENUE: &str = "QFEX";
const ACCOUNT_LABEL: &str = "clustered-move";
/// Made-up caps in lots of 0.01: room for the sixteen buys that rest at once.
const RESTING_CAP: i64 = 18;
const INVENTORY_CAP: i64 = 83;
/// The order-entry and market-data connections' numbers (a write is attributed to an input by
/// its connection).
const EXEC_CONN: u16 = 0;
const MD_CONN: u16 = 1;
/// The journal's shard.
const SHARD: u16 = 0;

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
/// The places and the protective cancels of the whole run.
const PLACES: usize = (TARGETS + ROUNDS * LADDER) as usize;
const CANCELS: usize = (ROUNDS * FRAMES_PER_ROUND) as usize;
/// What the stub answers on the trade socket: the opening six (the authentication, the three
/// private subscriptions, the cancel-on-disconnect arm and the resync's one `get_user_orders`
/// page), every place and every cancel.
const EXEC_FRAMES: usize = 6 + PLACES + CANCELS;
/// How long the whole run may take before the test fails.
const BOUND: Duration = Duration::from_secs(120);

/// Whether the stub's `created`th place (from 1) is a round's first, third, fifth or seventh,
/// so the reference server sends a burst then.
fn bursts_after(created: u64) -> bool {
    created
        .checked_sub(TARGETS + 1)
        .is_some_and(|at| (at % LADDER).is_multiple_of(2))
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// `AAPL-USD` on its 0.01 grid, in lots of 0.01, listing post-only GTC (decision 0136).
fn specs() -> SpecTable {
    probe_harness::specs()
}

fn symbol() -> VenueSymbol {
    specs().get(INST).unwrap().venue_symbol.clone()
}

fn order_caps() -> OrderCaps {
    probe_exec_caps().order
}

/// The pre-production probe on the stub's trade socket and REST route and the reference
/// server's MDS: every origin loopback, which the probe's guard admits. No request's deadline
/// falls within the run: nothing settles a place, which ends `Unknown` only at its deadline
/// (decision 0132 item 8).
fn config(stub: &Stub, reference: &Reference) -> VenueConfig {
    let cfg = probe_harness::on("primary", &stub.ws_url(), &stub.rest_url(), reference.url());
    probe_harness::with(cfg, KEY_RPC_TIMEOUT, "3600s")
}

/// A directory of the test's own under the target's temporary directory, removed with
/// everything in it when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("fbc-qfex-clustered-{name}-{}", std::process::id()));
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

/// A registry under the made-up caps, its lease names given, declared an owner-assisted testnet
/// run and seeded flat by hand: the probe caps declare the snapshot untrustworthy, so no resync
/// seeds the position (decisions 0055, 0067, 0132 item 5).
fn registry() -> Registry {
    let limits = MarketCapsConfig {
        inventory: Some(lots(INVENTORY_CAP)),
        resting: Some(lots(RESTING_CAP)),
    };
    let caps = PreTradeCaps::new().with_market(INST, limits).unwrap();
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps()).with_market(INST, symbol());
    let mut reg = Registry::with_caps(caps)
        .with_lease_keys(keys)
        .for_testnet_run(TestnetRun::owner_assisted());
    reg.seed_position(INST, SignedLots(0)).unwrap();
    reg
}

fn glue() -> Glue {
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    Glue::new(registry(), order_caps(), ladder)
}

/// One connection attempt: a reconnect would fail the run, never hide in it.
fn once() -> ReconnectPacing {
    let day = Duration::from_secs(86_400);
    ReconnectPacing::new(day, day, 1, day, Duration::from_secs(10)).unwrap()
}

/// The order-entry session's configuration, stamping from `clock`.
fn exec_config(cfg: &VenueConfig, clock: &IngestClock) -> ExecSessionConfig {
    let caps = Qfex.caps(cfg).unwrap();
    let limits = caps
        .exec_limits
        .expect("the probe's caps declare its limits");
    let reserve = SafetyReserve::percent(10).unwrap();
    // Every place stays in flight until its deadline (no reply settles one), so the table holds
    // all of them.
    let bound = ReservedBound::new(128, 128).unwrap();
    ExecSessionConfig {
        venue: &Qfex,
        cfg: cfg.clone(),
        creds: probe_harness::secrets(),
        acct: ACCT,
        account_wide_cod_authority: Some(AccountWideCodAuthority::new(ACCT)),
        rpc_ids: RpcIds::default(),
        ns: probe_harness::NS,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: once(),
        clock: clock.clone(),
        nonces: Box::new(QfexNonces::os()),
        nonce_source: NonceSourceId(1),
        conn: EXEC_CONN,
        limiter: RateLimiter::for_user(RateUserKey::new(0xd8c4_0001), &limits, reserve).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(10)).unwrap(),
        http_max_body: 1 << 20,
        outbound: bound,
        in_flight: InFlight::new(bound),
    }
}

/// The market-data session's configuration, `AAPL-USD`'s bbo, stamping from `clock`.
fn md_config(cfg: &VenueConfig, clock: &IngestClock) -> MdSessionConfig {
    let subs = BTreeSet::from([Subscription {
        inst: INST,
        feed: Feed::Touch(TOUCH_BBO),
    }]);
    let mut plans = Qfex.plan_md(cfg, &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1);
    let caps = Qfex.caps(cfg).unwrap();
    MdSessionConfig {
        venue: &Qfex,
        cfg: cfg.clone(),
        plan: plans.remove(0),
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: once(),
        clock: clock.clone(),
        http_max_body: 64 * 1024,
        conn: MD_CONN,
        limiter: RateLimiter::new(&caps.limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[0xd8; 32]).unwrap())
}

/// What the order-entry session reported beyond what the glue keeps.
#[derive(Default)]
struct Reported {
    /// Every request reported sent, in the order reported: each once encoded and before its
    /// write, so in the order written, with the wall clock then.
    sent: Vec<(RpcId, WallNs)>,
    /// The places among them, by client id, with the wall clock when each was reported encoded.
    places: Vec<(ClientOrderId, WallNs)>,
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
        let at = wall_now();
        let (rpc, sent) = (handle.rpc, handle.receipt.is_ok());
        if !sent {
            self.reported.borrow_mut().not_sent.push(handle.clone());
        }
        let item = self.glue.submitted(handle);
        let mut reported = self.reported.borrow_mut();
        if sent {
            reported.sent.push((rpc, at));
        }
        if let (true, Some(Item::Place(cid))) = (sent, item) {
            reported.places.push((cid, at));
        }
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
    planned: i64,
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

    /// Puts `n` resting buys of a lot each on `ladder`, post-only GTC, built and authorized by
    /// fbc-oms.
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
                let order = NewOrder {
                    cid: self.mint.mint().unwrap(),
                    inst: INST,
                    side: Side::Buy,
                    kind: OrderKind::Limit {
                        px: Ticks(19_000 - self.planned % 50),
                    },
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
    /// oldest order still resting, by its venue order id (the probe caps' one cancel reference),
    /// submitted at once.
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
        self.cancels.push(Cancel {
            cid,
            rpc,
            frame,
            places_encoded: reported.borrow().places.len(),
        });
    }
}

/// What one run showed.
struct Shown {
    cancels: Vec<Cancel>,
    reported: Rc<RefCell<Reported>>,
    glue: Rc<RefCell<Glue>>,
    /// The JSON requests the stub read on the trade socket, in order.
    requests: Vec<serde_json::Value>,
    /// Each request's write done on the shard clock, from the order-entry session's journal,
    /// when the run journaled.
    written: Option<HashMap<RpcId, u64>>,
}

/// Each request's write done on the order-entry connection, on the shard clock, from the
/// journal's `WriteResult` records.
fn written(root: &Path) -> HashMap<RpcId, u64> {
    let mut done = HashMap::new();
    for entry in JournalReader::open(root, SHARD).unwrap().entries() {
        if let Record::WriteResult {
            at,
            conn,
            rpc: Some(rpc),
            result,
        } = entry.unwrap().record
            && conn.conn == EXEC_CONN
        {
            assert_eq!(result, WriteRes::Written, "request {}", rpc.0);
            assert!(done.insert(rpc, at.0).is_none(), "request {} twice", rpc.0);
        }
    }
    done
}

/// Runs the benchmark; the order-entry session journals only when `journaled` (off Linux,
/// where no tick-to-wire sample gives a write's done instant).
async fn run(journaled: bool) -> Shown {
    let reference = Reference::start();
    let book: Shared = Arc::new(Mutex::new(Book::default()));
    {
        let bursts = reference.bursts();
        lock(&book).on_create = Some(Box::new(move |created| {
            if bursts_after(created) {
                // The reference server is gone only once the market-data session closed.
                let _ = bursts.send(BURST);
            }
        }));
    }
    let stub = Stub::start(&book, EXEC_FRAMES);
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
    let journal_dir = TempDir::new("journal");
    let writer = journaled.then(|| {
        let sink = SinkConfig {
            budget_bytes: 16 << 20,
            soft_limit_pct: 85,
        };
        let (sink, drain) = journal_queue(sink, key()).unwrap();
        let writer = drain
            .spawn(JournalWriter::create(journal_dir.path(), SHARD, key()).unwrap())
            .unwrap();
        exec.set_journal(Journal::new(Rc::new(RefCell::new(sink))));
        writer
    });
    let orders = exec.orders();

    let dir = TempDir::new("leases");
    let ns = NamespaceLease::acquire(dir.path(), ACCT, probe_harness::NS).unwrap();
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
    // The host's end, so a run that ends early fails at once with its error, not at the bound.
    let ended = Notify::new();
    let script = async {
        tokio::select! {
            () = done.notified() => {}
            // The stub's script may end with the last round's last answer before the decide
            // pass sees it; a script that failed fails the run at once with its cause.
            outcome = stub.finished() => {
                outcome.expect("the stub's script failed");
                tokio::select! {
                    () = done.notified() => {}
                    () = ended.notified() => return,
                }
            }
            () = ended.notified() => return,
        }
        // Every frame the stub was to answer has been answered: the last round's cancels have
        // ended their orders.
        stub.finished().await.unwrap();
        drop((md_control, exec_control));
    };
    let members = vec![Member::md(&mut md), Member::exec(&mut exec)];
    let hosted = async {
        let ran = host.run(members, &mut decide).await;
        ended.notify_one();
        ran
    };
    let both = async { tokio::join!(hosted, script) };
    let (ran, ()) = tokio::time::timeout(BOUND, both)
        .await
        .expect("the benchmark did not end within its bound");
    ran.unwrap();
    assert_eq!(md.counters().decode_errors, 0);
    assert_eq!(exec.counters().decode_errors, 0);
    drop((md, exec, decide));
    if let Some(writer) = writer {
        writer.close().unwrap();
    }
    reference.join();
    assert_eq!(stub.connections(), 1);
    let requests = stub.requests();
    let cancels = bench.borrow().cancels.clone();
    assert_eq!(bench.borrow().phase, Phase::Done);
    Shown {
        cancels,
        reported,
        glue,
        requests,
        written: journaled.then(|| written(journal_dir.path())),
    }
}

/// Every place and protective cancel was authorized by fbc-oms and written, nothing refused,
/// each cancel answering a market-data frame of its own and written after at most one place the
/// host had submitted when the handler submitted it: the places the stub read before it number
/// at most one more than those encoded by then. Of those places, at most one was reported
/// encoded after its frame arrived, at the wall-clock instant `arrived` gives (its frame's
/// kernel receive on Linux, its ingest stamp elsewhere). For each cancel, in the order
/// submitted, whether it waited for such a place.
fn every_cancel_written_behind_at_most_one_place(
    shown: &Shown,
    arrived: impl Fn(&Cancel) -> i64,
) -> Vec<bool> {
    let reported = shown.reported.borrow();
    let glue = shown.glue.borrow();
    assert!(reported.not_sent.is_empty(), "{:?}", reported.not_sent);
    assert!(glue.problems.is_empty(), "{:?}", glue.problems);
    assert_eq!(glue.resyncs, 1);
    assert_eq!(shown.cancels.len(), CANCELS);
    assert_eq!(reported.places.len(), PLACES);
    assert_eq!(reported.sent.len(), PLACES + CANCELS);

    // Every place and cancel the stub read is one fbc-oms authorized: the places by their
    // client ids, the cancels by the orders they name.
    assert_eq!(shown.requests.len(), EXEC_FRAMES);
    let kind = |v: &serde_json::Value| v["type"].as_str().unwrap_or("").to_owned();
    let of = |t: &'static str| shown.requests.iter().filter(move |v| kind(v) == t);
    let placed: BTreeSet<String> = of("add_order")
        .map(|v| v["params"]["client_order_id"].as_str().unwrap().to_owned())
        .collect();
    let wire = |cid: ClientOrderId| {
        encode_cid(&CLIENT_ID_FORMAT, cid)
            .unwrap()
            .as_str()
            .to_owned()
    };
    let authorized_places: BTreeSet<String> = (glue.authorized.iter())
        .filter_map(|item| match item {
            Item::Place(cid) => Some(wire(*cid)),
            Item::Cancel(_) => None,
        })
        .collect();
    assert_eq!(of("add_order").count(), PLACES);
    assert_eq!((placed.len(), &placed), (PLACES, &authorized_places));
    let cancelled_of = |v: &serde_json::Value| {
        let vid = probe_harness::venue_id(v["params"]["order_id"].as_str().unwrap());
        glue.reg.cid_of(&vid).expect("a cancel of an order of ours")
    };
    let cancelled: Vec<ClientOrderId> = of("cancel_order").map(cancelled_of).collect();
    let authorized_cancels: Vec<ClientOrderId> = (glue.authorized.iter())
        .filter_map(|item| match item {
            Item::Cancel(cid) => Some(*cid),
            Item::Place(_) => None,
        })
        .collect();
    let submitted: Vec<ClientOrderId> = shown.cancels.iter().map(|c| c.cid).collect();
    assert_eq!(authorized_cancels, submitted);
    let distinct: HashSet<&ClientOrderId> = cancelled.iter().collect();
    assert_eq!(distinct.len(), CANCELS);
    assert_eq!(
        cancelled.iter().collect::<HashSet<_>>(),
        submitted.iter().collect()
    );
    // Every order the run cancelled has ended, and the last round's eight rest.
    assert!(submitted.iter().all(|&cid| is_terminal(&glue, cid)));
    let resting = (glue.authorized.iter())
        .filter(|item| matches!(item, Item::Place(cid) if is_open(&glue, *cid)))
        .count();
    assert_eq!(resting, PLACES - CANCELS);

    // Every frame a cancel answered is a market-data frame, each cancel its own.
    let frames: HashSet<u64> = shown.cancels.iter().map(|c| c.frame.ingest_seq).collect();
    assert_eq!(frames.len(), CANCELS);

    // Where the stub read each cancel on the one connection, which is the order written.
    let read_at: HashMap<ClientOrderId, usize> = (shown.requests.iter().enumerate())
        .filter(|(_, v)| kind(v) == "cancel_order")
        .map(|(at, v)| (cancelled_of(v), at))
        .collect();
    let encoded: HashMap<String, WallNs> = (reported.places.iter())
        .map(|(cid, at)| (wire(*cid), *at))
        .collect();
    shown
        .cancels
        .iter()
        .map(|c| {
            assert_eq!(c.frame.conn.conn, MD_CONN);
            let at = read_at[&c.cid];
            let before: Vec<WallNs> = (shown.requests[..at].iter())
                .filter(|v| kind(v) == "add_order")
                .map(|v| encoded[v["params"]["client_order_id"].as_str().unwrap()])
                .collect();
            assert!(
                c.places_encoded <= before.len() && before.len() <= c.places_encoded + 1,
                "{c:?}: {} places written before it",
                before.len()
            );
            let arrival = arrived(c);
            let waited = before.iter().filter(|at| at.0 > arrival).count();
            assert!(
                waited <= 1,
                "{c:?}: {waited} places written before it were encoded after its frame arrived"
            );
            waited == 1
        })
        .collect()
}

/// The nearest rank of the `p`th percentile of `len` samples, from 1.
fn rank(len: usize, p: usize) -> usize {
    (len * p).div_ceil(100).max(1)
}

fn us(nanos: f64) -> f64 {
    nanos / 1_000.0
}

/// The p50, p99 and max of `nanos` (nearest rank), in microseconds, as the report gives them,
/// saying so when the p99's rank is the last: below 101 samples the p99 is the max.
fn spread(mut nanos: Vec<f64>) -> String {
    nanos.sort_unstable_by(f64::total_cmp);
    let len = nanos.len();
    let note = if rank(len, 99) == len {
        format!(" (at {len} samples the nearest-rank p99 is the max)")
    } else {
        String::new()
    };
    format!(
        "p50 {:.1} us, p99 {:.1} us, max {:.1} us{note}",
        us(nanos[rank(len, 50) - 1]),
        us(nanos[rank(len, 99) - 1]),
        us(nanos[len - 1]),
    )
}

/// The report's tail: how many cancels waited for a place encoded after their frame arrived,
/// and the ingest-stamp-to-write-done spread of `ingest`, in nanoseconds, labelled `how`.
fn tail(waited: &[bool], ingest: &[u64], how: &str) -> String {
    let behind = waited.iter().filter(|w| **w).count();
    let ingest = ingest.iter().map(|n| *n as f64).collect();
    format!(
        "{behind} waited for a place encoded after their frame arrived, none for more than one; \
         ingest stamp to write done, excluding the kernel-to-user hop, {how}, {}",
        spread(ingest)
    )
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
async fn a_clustered_move_during_a_ladder_has_every_protective_cancel_written_behind_at_most_one_place_and_reports_its_kernel_receive_to_write_done()
 {
    // No journal, as Paradex's benchmark runs: nothing but the session's own path lies between
    // a frame and its cancel's write.
    let shown = run(false).await;
    assert!(shown.written.is_none());
    let reported = shown.reported.borrow();
    // Each request's encode on the wall clock, and the next request's after it.
    let encode: HashMap<RpcId, (i64, Option<i64>)> = (reported.sent.iter().enumerate())
        .map(|(i, (rpc, at))| (*rpc, (at.0, reported.sent.get(i + 1).map(|(_, n)| n.0))))
        .collect();
    let mut kernel = Vec::new();
    let mut ingest = Vec::new();
    for c in &shown.cancels {
        let rx = c
            .frame
            .kernel_rx
            .expect("a frame read on Linux has a kernel receive time")
            .0;
        // Its one sample, on the order-entry session, from the frame's kernel receive: the
        // cancel is Safety traffic attributed to the frame it answered.
        let samples: Vec<&TickToWire> = (reported.ticks.iter())
            .filter(|t| t.frame == c.frame)
            .collect();
        let [tick] = samples.as_slice() else {
            panic!("{c:?}: {samples:?}");
        };
        assert_eq!(tick.stream, fbc_venue_qfex::exec::TRADE);
        assert!(tick.nanos >= 0, "{tick:?}");
        // The sample's write done on the wall clock is this cancel's write: after its encode
        // and before the next request's, the one task writing each request between the two.
        let done = rx + tick.nanos;
        let (encoded, next) = encode[&c.rpc];
        assert!(
            encoded <= done && next.is_none_or(|next| done <= next),
            "{c:?}: {tick:?} written at {done}, encoded at {encoded}, the next at {next:?}"
        );
        kernel.push(tick.nanos.unsigned_abs());
        let from_ingest = done - c.frame.recv_wall.0;
        ingest.push(u64::try_from(from_ingest).expect("a frame read before its cancel's write"));
    }
    // No sample is for a write attributed to another market-data frame: one per cancel.
    let md_ticks = (reported.ticks.iter())
        .filter(|t| t.frame.conn.conn == MD_CONN)
        .count();
    assert_eq!(md_ticks, CANCELS);
    let waited =
        every_cancel_written_behind_at_most_one_place(&shown, |c| c.frame.kernel_rx.unwrap().0);
    report(&format!(
        "QFEX clustered move (Linux): {} protective cancels during {} places, none signed; \
         kernel receive to write done {}; {}",
        kernel.len(),
        reported.places.len(),
        spread(kernel.iter().map(|n| *n as f64).collect()),
        tail(&waited, &ingest, "from the samples"),
    ));
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn off_linux_a_clustered_move_during_a_ladder_has_every_protective_cancel_written_and_reports_that_kernel_rx_is_absent()
 {
    let shown = run(true).await;
    let waited = every_cancel_written_behind_at_most_one_place(&shown, |c| c.frame.recv_wall.0);
    assert!(shown.cancels.iter().all(|c| c.frame.kernel_rx.is_none()));
    let reported = shown.reported.borrow();
    assert!(reported.ticks.is_empty(), "{:?}", reported.ticks);
    // Each cancel's write done on the shard clock, from the journal, less its frame's stamp.
    let written = shown.written.as_ref().expect("the run journaled");
    let ingest: Vec<u64> = (shown.cancels.iter())
        .map(|c| {
            let done = written[&c.rpc];
            let stamped = c.frame.recv_mono.0;
            assert!(done >= stamped, "{c:?}: written at {done}");
            done - stamped
        })
        .collect();
    for (rpc, _) in &reported.sent {
        assert!(written.contains_key(rpc), "request {} written", rpc.0);
    }
    report(&format!(
        "QFEX clustered move ({}): kernel_rx is absent on this system, so no \
         kernel-receive-to-write-done time is reported; {} protective cancels during {} \
         places, none signed; {}",
        std::env::consts::OS,
        shown.cancels.len(),
        reported.places.len(),
        tail(&waited, &ingest, "with the order-entry session journaling"),
    ));
}
