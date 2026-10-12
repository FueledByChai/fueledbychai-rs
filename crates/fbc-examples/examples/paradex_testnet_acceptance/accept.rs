//! paradex_testnet_acceptance's run: FBC-8xr's script on Paradex TESTNET through fbc-oms, in
//! three phases, each one order-entry session (fbc-runtime's `ExecSession`) over a registry of
//! its own declared an owner-assisted testnet run (decision 0067), its events routed into the
//! registry by testnet_trade's [`Link`]:
//!
//! 1. **main**: place, amend, cancel, batch create, cancel-many, an IOC that expires, then a
//!    post-only order left resting and the socket DROPPED with no close frame (`cod-dropped`).
//! 2. **graceful**: a new connection (its resync shows whether the dropped socket's order
//!    survived, and Safety-cancels it if so, decision 0080), one post-only order, and the
//!    socket CLOSED gracefully (`cod-graceful`).
//! 3. **stop**: a last connection, the kill switch and a cancel of everything of ours on the
//!    market, and the last comparison.
//!
//! A registry per phase is a restart as the owner's runs have them: each phase logs in, arms
//! cancel-on-disconnect, resyncs, seeds the market by hand from the resync's REST position and
//! Starts it under the caps; nothing carries an earlier phase's orders but the venue. Between
//! the phases, the account's GET /orders is read over REST until the order the disconnect
//! should have cancelled is gone, or `--cod-wait` passes, before any reconnect could cancel it.
//!
//! Every command goes through an fbc-oms authorization, re-checked at submit (decisions 0005,
//! 0060): nothing bypasses the registry. Every order is tiny (`--order-usd`, at most the
//! resting cap per side), post-only (but the IOC, which is priced where it can fill nothing),
//! and at least `--away-bps` behind a touch read just before it is built.
//!
//! After every step, [`Phase::compare`] reads GET /orders and GET /positions until they agree
//! with what the order stream showed or `--rest-wait` passes, and a new order's
//! GET /orders-history?client_id= until it lists it. Every state the registry holds each order
//! in after each event is traced ([`Tap`]). The run's journal is written under the evidence
//! directory and its scrubbed slice becomes the fixtures (`evidence.rs`).

use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fbc_core::{
    AccountKey, Channel, CidMatch, CidMint, ClientIdFormat, ClientOrderId, ConnKey, ConnState,
    Envelope, ExecEvent, Lots, MarketLease, NewOrder, NonceBlock, NonceScope, NonceSource, OpKind,
    OrderKind, Secrets, Side, SignedLots, SpecTable, SubmitHandle, SubmitOutcome, Ticks, Tif,
    VenueFactory, VenueOrderSnapshot, encode_cid,
};
use fbc_journal::{JournalWriter, NonceSourceId, RedactionKey, SinkConfig, journal_queue};
use fbc_oms::{
    CancelChoice, CancelEverything, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, OrdState, PermittedCommand, PreTradeCaps, Registry, TestnetRun,
};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, Journal,
    RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::exec::{open_orders_request, order_history_request, positions_request};
use fbc_venue_paradex::md::rest::{OrderbookSnapshot, decode_orderbook, orderbook_path};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::Value;
use tokio::sync::Notify;

use crate::accept_args::AcceptOptions;
use crate::args::{
    Options, OrderSide, TESTNET_CHAIN, Target, direct_to_stub, hosts, sole, testnet_guard,
};
use crate::auth_rest::{RestAnswer, RestReader};
use crate::evidence::{
    self, Comparison, Disconnect, Evidence, History, Ran, RestPosition, Sent, Trace,
};
use crate::link::{Link, LinkHandler, Note, shown};
use crate::trade::{self, HighWater, INST, LOGIN_REFUSALS, Out, Priced, off_thread_holding};

const ACCT: AccountKey = AccountKey::new(1);
/// The lease files' venue and account labels, as testnet_trade's, so the two never trade the
/// same market of the account at once and share the client-id high-water mark.
const VENUE: &str = "PARADEX";
const ACCOUNT_LABEL: &str = "testnet";
/// The journal's shard number.
const SHARD: u16 = 0;
/// The pause between two REST reads of one comparison.
const POLL: Duration = Duration::from_millis(250);

/// Where the credentials come from: the environment when run, synthetic ones under test. Each
/// session and the REST reader take their own copy, moved into the adapter's src/auth.
pub type Creds<'a> = &'a dyn Fn() -> Result<Secrets, String>;

/// What a run wrote, and whether every step passed (the paths and the evidence are read by
/// the integration test; the program prints them on its DONE line).
#[allow(dead_code)]
#[derive(Debug)]
pub struct Outcome {
    pub ok: bool,
    pub report: PathBuf,
    pub json: PathBuf,
    pub fixtures: PathBuf,
    pub evidence: Evidence,
    /// The fixture files written.
    pub fixture_files: Vec<String>,
}

/// Runs the acceptance program: the guard first, before anything connects (the URLs, the chain
/// id, the proxy, `--sole-trader`), then the phases, then the files. An error is a refusal
/// before anything was sent; once a session started, the outcome is in the report.
pub async fn run(
    opts: &AcceptOptions,
    chain: Option<&str>,
    creds: Creds<'_>,
    out: Out,
) -> Result<Outcome, String> {
    let t = &opts.trade;
    let target = testnet_guard(&t.rest_url, &t.ws_url, chain)?;
    direct_to_stub(target, &t.proxy)?;
    sole(t)?;
    let dir = &opts.evidence_dir;
    if fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some()) {
        return Err(format!(
            "--evidence-dir {}: not empty; give a new directory for each run",
            dir.display()
        ));
    }
    fs::create_dir_all(dir).map_err(|e| format!("--evidence-dir {}: {e}", dir.display()))?;
    let lines = Lines {
        out,
        t0: Instant::now(),
    };
    let (rest_host, ws_host) = hosts(&t.rest_url, &t.ws_url)?;
    let what = match target {
        Target::Testnet => "Paradex testnet",
        Target::LoopbackStub => "a loopback test stub, not Paradex",
    };
    lines.line(format_args!(
        "TESTNET {what}: REST host {rest_host} WS host {ws_host} chain {TESTNET_CHAIN}"
    ));
    lines.line(format_args!(
        "OWNER-ASSISTED testnet run (decision 0067): each phase's market position is seeded by \
         hand from the venue's REST position; never done outside a declared testnet run"
    ));

    let factory = &ParadexFactory;
    let cfg = trade::config(t);
    let specs = trade::specs(factory, &cfg, t)?;
    // One limiter for everything the run sends Paradex: the sessions of all three phases, the
    // REST reader and the order-book reads.
    let limiter = RateLimiter::new(
        &factory.caps(&cfg).map_err(|e| e.to_string())?.limits,
        SafetyReserve::percent(10).expect("a valid reserve"),
    )
    .map_err(|e| format!("{e:?}"))?;
    let book = touch(t, &specs, &limiter).await?;
    let mine = trade::price(t, &book)?;
    let other_opts = with_side(t, opposite(t.side));
    let other = trade::price(&other_opts, &book).ok();
    let resting = [Some(&mine), other.as_ref()]
        .into_iter()
        .flatten()
        .map(|p| p.resting)
        .min()
        .expect("the order's side is priced");
    let inventory = [Some(&mine), other.as_ref()]
        .into_iter()
        .flatten()
        .map(|p| p.inventory)
        .min()
        .expect("the order's side is priced");
    let caps_text = format!(
        "caps: resting ${} per side ({} lots), inventory ${} ({} lots); each order ${}",
        t.resting_cap_usd,
        resting.get(),
        t.inventory_cap_usd,
        inventory.get(),
        t.order_usd
    );
    lines.line(format_args!("CAPS {caps_text}"));
    let order_caps = factory
        .caps(&cfg)
        .map_err(|e| e.to_string())?
        .exec
        .ok_or("Paradex declares no order entry")?
        .order;
    let symbol = specs
        .get(INST)
        .expect("the one market")
        .venue_symbol
        .clone();
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps).with_market(INST, symbol.clone());
    fs::create_dir_all(&t.lease_dir)
        .map_err(|e| format!("--lease-dir {}: {e}", t.lease_dir.display()))?;
    let ns = fbc_core::Namespace::new(t.namespace);
    let ns_lease = fbc_core::NamespaceLease::acquire(&t.lease_dir, ACCT, ns)
        .map_err(|e| format!("the client-id namespace's lease is held (another run?): {e:?}"))?;
    let hwm = HighWater::at(&t.lease_dir, ns);
    let persisted = hwm.read()?;
    let rest = RestReader::new(
        &cfg,
        creds()?,
        Connector::new(t.proxy.clone()),
        limiter.clone(),
    )?;

    // The journal: a queue the sessions record into, drained by a writer thread into
    // <evidence>/journal, its spans hashed under a key of this run's (never written).
    let journal_dir = dir.join("journal");
    let key = Arc::new(RedactionKey::new(&run_key()?).map_err(|e| format!("the journal: {e}"))?);
    let (sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 64 << 20,
            soft_limit_pct: 90,
        },
        Arc::clone(&key),
    )
    .map_err(|e| format!("the journal: {e}"))?;
    let writer =
        JournalWriter::create(&journal_dir, SHARD, key).map_err(|e| format!("the journal: {e}"))?;
    let writing = drain
        .spawn(writer)
        .map_err(|e| format!("the journal: {e}"))?;
    let journal = Journal::new(Rc::new(RefCell::new(sink)));

    let started = evidence::utc(wall_secs());
    let mut run = Run {
        opts: opts.clone(),
        lines: lines.clone(),
        cfg,
        specs,
        order_caps,
        resting,
        inventory,
        keys,
        hwm,
        mint: None,
        ns_lease: Some(ns_lease),
        persisted,
        rest,
        journal,
        rpc_ids: RpcIds::default(),
        tracker: Rc::new(RefCell::new(Tracker::default())),
        ev: Evidence {
            started: started.clone(),
            target: what.to_owned(),
            market: t.market.clone(),
            caps: caps_text,
            ..Evidence::default()
        },
        rest_bodies: Vec::new(),
        fmt: order_caps_fmt(),
        undecoded: false,
        book_reads: 1,
        sampled: (0, 0, 0),
        ended_counts: (0, 0),
        live: None,
        troubled: false,
        clock: IngestClock::new(),
        limiter,
    };
    for name in [
        "place",
        "amend",
        "cancel",
        "batch",
        "cancel-many",
        "ioc",
        "cod-dropped",
        "cod-graceful",
        "stop",
    ] {
        run.ev.step(name);
    }

    // Phase 1, then what the dropped socket left; phase 2, then what the close left; phase 3.
    let main = run.phase(PhaseKind::Main, creds).await;
    if let Some(cod) = main {
        run.after_disconnect("cod-dropped", cod).await;
        if let Some(cod) = run.phase(PhaseKind::Graceful, creds).await {
            run.after_disconnect("cod-graceful", cod).await;
        }
    }
    run.phase(PhaseKind::Stop, creds).await;

    // Every session has dropped its handle on the journal; this one goes too, and the writer
    // thread writes what is queued and closes its segment.
    let Run {
        journal,
        mut ev,
        rest_bodies,
        ..
    } = run;
    drop(journal);
    if let Err(e) = writing.close() {
        ev.artifact_errors
            .push(format!("the journal's writer ended with an error: {e}"));
    }
    let fixtures = dir.join("fixtures");
    let date = started.get(..10).unwrap_or("").to_owned();
    let mut fixture_files = Vec::new();
    match evidence::write_fixtures(
        &journal_dir,
        SHARD,
        &fixtures,
        &date,
        what,
        &t.market,
        &rest_bodies,
    ) {
        Ok(written) => {
            ev.frames = written.order_rows;
            ev.withheld = written.withheld;
            fixture_files = written.files;
        }
        Err(e) => ev.artifact_errors.push(format!("no fixtures written: {e}")),
    }
    // The repository's own privacy check over what was written: fixtures it refuses are
    // removed, never left for the owner to copy.
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    if fixtures.exists()
        && let Err(e) = evidence::privacy_check(&script, &fixtures)
    {
        let _ = fs::remove_dir_all(&fixtures);
        fixture_files.clear();
        ev.artifact_errors
            .push(format!("{e}; the fixtures directory was removed"));
    }
    let report = dir.join("report.md");
    let json = dir.join("evidence.json");
    fs::write(&report, evidence::markdown(&ev))
        .map_err(|e| format!("{}: {e}", report.display()))?;
    let text = serde_json::to_string_pretty(&evidence::to_json(&ev)).map_err(|e| e.to_string())?;
    fs::write(&json, text).map_err(|e| format!("{}: {e}", json.display()))?;
    let ok = ev.ok();
    lines.line(format_args!(
        "DONE {} report {} evidence {} fixtures {} ({} files)",
        if ok { "ok" } else { "failed" },
        report.display(),
        json.display(),
        fixtures.display(),
        fixture_files.len()
    ));
    Ok(Outcome {
        ok,
        report,
        json,
        fixtures,
        evidence: ev,
        fixture_files,
    })
}

/// This run's journal key: 32 bytes from the operating system's random source. It is never
/// written; the journal keeps only the keyed hashes of the spans it hides.
fn run_key() -> Result<Vec<u8>, String> {
    use std::io::Read as _;
    let mut key = vec![0u8; 32];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut key))
        .map_err(|e| format!("no journal key from /dev/urandom: {e}"))?;
    Ok(key)
}

/// The seconds since the epoch now.
fn wall_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Paradex's client-id format, for naming our ids as they go on the wire.
fn order_caps_fmt() -> ClientIdFormat {
    fbc_venue_paradex::exec::exec_caps().order.client_id
}

fn opposite(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

fn with_side(opts: &Options, side: OrderSide) -> Options {
    Options {
        side,
        ..opts.clone()
    }
}

fn side_of(side: OrderSide) -> Side {
    match side {
        OrderSide::Buy => Side::Buy,
        OrderSide::Sell => Side::Sell,
    }
}

/// Lines with the milliseconds since the start.
#[derive(Clone)]
struct Lines {
    out: Out,
    t0: Instant,
}

impl Lines {
    fn line(&self, text: std::fmt::Arguments<'_>) {
        let _ = writeln!(self.out.borrow_mut(), "{text}");
    }

    fn step(&self, step: &str, text: std::fmt::Arguments<'_>) {
        let ms = self.t0.elapsed().as_millis();
        self.line(format_args!("STEP {step:<12} +{ms}ms  {text}"));
    }
}

/// The market's REST order book (`GET /orderbook`), as testnet_trade reads it.
async fn touch(
    opts: &Options,
    specs: &SpecTable,
    limiter: &RateLimiter,
) -> Result<OrderbookSnapshot, String> {
    let origin = opts
        .rest_url
        .trim_end_matches('/')
        .strip_suffix("/v1")
        .ok_or("--rest-url: not a base ending in /v1")?;
    let url = format!("{origin}{}", orderbook_path(&opts.market));
    let request = Request::builder()
        .method(Method::GET)
        .uri(&url)
        .body(Bytes::new())
        .map_err(|e| format!("GET /orderbook: {e}"))?;
    let connector = Connector::new(opts.proxy.clone());
    let wait = Duration::from_secs(opts.step_timeout_secs);
    // A public GET: Paradex's per-IP limits count it.
    crate::auth_rest::admit(
        limiter,
        OpKind::Rest,
        Instant::now() + wait,
        "GET /orderbook",
    )
    .await?;
    let response = tokio::time::timeout(wait, connector.http(request, 1 << 20))
        .await
        .map_err(|_| "GET /orderbook: no answer in time".to_owned())?
        .map_err(|e| format!("GET /orderbook: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("GET /orderbook: status {}", response.status()));
    }
    decode_orderbook(response.body(), specs).map_err(|e| format!("GET /orderbook: {e:?}"))
}

/// Nonces counted up from 1: Paradex asks for none, but the session reserves one per item.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).expect("a block of nonces");
        self.0 += u64::from(len);
        block
    }
}

/// One order of ours the run follows: the registry's state, price and total after each event.
#[derive(Debug)]
struct Tracked {
    label: String,
    cid: ClientOrderId,
    wire: String,
    phase: usize,
    vid: Option<String>,
    /// Each state, with when the stream showed it.
    states: Vec<(String, Instant)>,
}

/// The orders the run follows, sampled after every event the session hands the link.
#[derive(Debug, Default)]
struct Tracker {
    phase: usize,
    orders: Vec<Tracked>,
    /// The market's grid, for showing a price and a total.
    tick: Decimal,
    step: Decimal,
}

impl Tracker {
    fn sample(&mut self, reg: &Registry) {
        let (tick, step, phase) = (self.tick, self.step, self.phase);
        for o in self.orders.iter_mut().filter(|o| o.phase == phase) {
            let Some(rec) = reg.get(o.cid) else { continue };
            let px = rec.px().map_or_else(
                || "-".to_owned(),
                |p| (Decimal::from(p.0) * tick).normalize().to_string(),
            );
            let qty = (Decimal::from(rec.qty().get()) * step).normalize();
            let state = format!("{:?} @{px} x {qty}", rec.state());
            if o.states.last().is_none_or(|(s, _)| *s != state) {
                o.states.push((state, Instant::now()));
            }
            if o.vid.is_none() {
                o.vid = rec.vid().map(|v| v.as_str().to_owned());
            }
        }
    }

    fn get(&self, cid: ClientOrderId) -> Option<&Tracked> {
        self.orders.iter().find(|o| o.cid == cid)
    }

    fn trace(&self, cid: ClientOrderId) -> Trace {
        self.get(cid).map_or_else(Trace::default, |o| Trace {
            label: o.label.clone(),
            client_id: o.wire.clone(),
            venue_id: o.vid.clone(),
            states: o.states.iter().map(|(s, _)| s.clone()).collect(),
        })
    }
}

/// The session's handler: testnet_trade's [`LinkHandler`], then a sample of the orders the run
/// follows, so every state each passes through is traced.
struct Tap {
    inner: LinkHandler,
    tracker: Rc<RefCell<Tracker>>,
}

impl Tap {
    fn sample(&self) {
        let link = self.inner.link.borrow();
        self.tracker.borrow_mut().sample(link.registry());
    }
}

impl ExecHandler for Tap {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.inner.on_exec(env);
        self.sample();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.inner.on_epoch_end(key);
        self.sample();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.inner.on_submitted(handle);
        self.sample();
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.inner.on_unprotected(orders);
        self.sample();
    }
}

/// The three phases (module documentation).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PhaseKind {
    Main,
    Graceful,
    Stop,
}

/// How a phase ends its session: dropping the socket with no close frame, or closing it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SocketEnd {
    Drop,
    Close,
    /// A close whose session did not finish within 5 s: its socket was then dropped.
    CloseTimedOut,
}

/// The order a disconnect should cancel, for [`Run::after_disconnect`].
struct CodOrder {
    label: String,
    cid: ClientOrderId,
    wire: String,
    how: SocketEnd,
    /// When the session ended.
    at: Instant,
    /// The market's position in lots as the phase's stream last showed it.
    lots: i64,
}

/// The run's state across its phases.
struct Run {
    opts: AcceptOptions,
    lines: Lines,
    cfg: fbc_core::VenueConfig,
    specs: SpecTable,
    order_caps: fbc_core::OrderCaps,
    /// The caps in lots: the resting cap per side at the dearer side's price, the inventory cap
    /// at the highest of the prices and the touch.
    resting: Lots,
    inventory: Lots,
    keys: LeaseKeys,
    hwm: HighWater,
    mint: Option<CidMint>,
    ns_lease: Option<fbc_core::NamespaceLease>,
    persisted: u64,
    rest: RestReader,
    journal: Journal,
    rpc_ids: RpcIds,
    tracker: Rc<RefCell<Tracker>>,
    ev: Evidence,
    /// The REST answers kept for the fixtures: what was read, its body.
    rest_bodies: Vec<(String, Vec<u8>)>,
    fmt: ClientIdFormat,
    /// Whether a session could not decode a frame or answer: the run cannot say it saw all.
    undecoded: bool,
    /// The order-book reads made, the connections opened, and what had been sent when the
    /// last comparison was made (REST requests, WebSocket order methods, connections): the
    /// per-IP limit check's counts.
    book_reads: u64,
    sampled: (u64, u64, u64),
    /// The WebSocket order methods and connections of the phases that ended, and the running
    /// phase's link ([`Run::session_totals`]).
    ended_counts: (u64, u64),
    live: Option<Rc<RefCell<Link>>>,
    /// Whether a phase's link noted something that leaves the order truth incomplete
    /// ([`link_problems`]).
    troubled: bool,
    /// The ingest clock every phase's session stamps with: one ingest sequence and one
    /// monotonic origin for the run's one journal shard.
    clock: IngestClock,
    /// The limiter every phase's session and the REST reader charge: one count of what this
    /// program sends Paradex.
    limiter: RateLimiter,
}

/// One phase's session, registry and link (module documentation).
struct Phase {
    link: Rc<RefCell<Link>>,
    wake: Rc<Notify>,
    orders: ExecOrders,
    timeout: Duration,
    /// Whether this phase's resync showed an order of ours resting (an earlier phase's).
    restored: usize,
}

impl Phase {
    /// Waits until `found` finds what it looks for in the link, or `within` passes.
    async fn wait_for<T>(
        &self,
        within: Duration,
        mut found: impl FnMut(&mut Link) -> Option<T>,
    ) -> Option<T> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let woken = self.wake.notified();
            if let Some(t) = found(&mut self.link.borrow_mut()) {
                return Some(t);
            }
            tokio::select! {
                _ = woken => {}
                _ = tokio::time::sleep_until(deadline) => return None,
            }
        }
    }

    async fn wait<T>(&self, found: impl FnMut(&mut Link) -> Option<T>) -> Option<T> {
        self.wait_for(self.timeout, found).await
    }

    /// Authorizes `cmd` and hands it to the session: its request id.
    fn submit(&self, cmd: PermittedCommand) -> Result<fbc_core::RpcId, String> {
        let mut link = self.link.borrow_mut();
        let auth = link
            .reg()
            .authorize(ACCT, cmd)
            .map_err(|e| format!("{e:?}"))?;
        link.submit(&self.orders, auth)
    }

    /// Every outcome the session reported for request `rpc`, in order.
    fn outcomes(&self, rpc: fbc_core::RpcId) -> Vec<String> {
        let link = self.link.borrow();
        link.notes()
            .iter()
            .filter_map(|n| match n {
                Note::Outcome {
                    rpc: r,
                    item,
                    outcome,
                    ..
                // A refusal's venue text goes through testnet_trade's `shown`, which withholds
                // any word that could hold an echoed credential.
                } if *r == rpc => Some(match item {
                    Some(i) => format!("item {i} {}", shown(&format!("{outcome:?}"))),
                    None => shown(&format!("{outcome:?}")),
                }),
                Note::Submitted {
                    rpc: r,
                    sent: Err(why),
                } if *r == rpc => Some(format!("NotSent({why:?})")),
                _ => None,
            })
            .collect()
    }

    /// What became of request `rpc` once every item was answered, within the step timeout.
    async fn answered(&self, rpc: fbc_core::RpcId) -> Option<SubmitOutcome> {
        self.wait(|l| l.outcome_of(rpc)).await
    }

    /// Waits until every order of `cids` satisfies `done`; true when they did in time.
    async fn until(&self, cids: &[ClientOrderId], done: impl Fn(OrdState) -> bool) -> bool {
        self.wait(|l| {
            cids.iter()
                .all(|c| l.registry().get(*c).is_some_and(|r| done(r.state())))
                .then_some(())
        })
        .await
        .is_some()
    }

    fn state(&self, cid: ClientOrderId) -> Option<OrdState> {
        self.link.borrow().registry().get(cid).map(|r| r.state())
    }
}

/// Whether an order rests, or has ended.
fn resting_or_ended(s: OrdState) -> bool {
    matches!(s, OrdState::Open | OrdState::PartiallyFilled) || s.is_terminal()
}

impl Run {
    fn t(&self) -> &Options {
        &self.opts.trade
    }

    /// A step that failed: why, noted on it.
    fn fail(&mut self, step: &str, why: impl Into<String>) {
        let why = why.into();
        let ms = self.lines.t0.elapsed().as_millis();
        self.lines.line(format_args!("FAIL {step} +{ms}ms  {why}"));
        let s = self.ev.step(step);
        s.ran = Ran::Yes;
        s.pass = false;
        s.notes.push(why);
    }

    /// Runs one phase: its session beside its steps, until the steps end it by dropping or
    /// closing the socket. The order a disconnect should cancel, for a phase that left one.
    async fn phase(&mut self, kind: PhaseKind, creds: Creds<'_>) -> Option<CodOrder> {
        self.tracker.borrow_mut().phase += 1;
        {
            let mut tr = self.tracker.borrow_mut();
            tr.tick = self.t().tick;
            tr.step = self.t().step;
        }
        let built = self.build_session(creds);
        let (link, wake, mut session, control) = match built {
            Ok(b) => b,
            Err(e) => {
                let step = match kind {
                    PhaseKind::Main => "place",
                    PhaseKind::Graceful => "cod-graceful",
                    PhaseKind::Stop => "stop",
                };
                self.fail(step, format!("the session could not be built: {e}"));
                return None;
            }
        };
        self.live = Some(Rc::clone(&link));
        let phase = Phase {
            link: Rc::clone(&link),
            wake,
            orders: session.orders(),
            timeout: Duration::from_secs(self.t().step_timeout_secs),
            restored: 0,
        };
        let mut control = Some(control);
        let (end, ended_first) = {
            let running = session.run();
            tokio::pin!(running);
            let steps = self.steps(kind, phase);
            tokio::pin!(steps);
            let mut ended_first = None;
            let end = tokio::select! {
                ended = &mut running => {
                    ended_first = Some(match ended {
                        Ok(()) => "stopped".to_owned(),
                        Err(e) => e.to_string(),
                    });
                    (SocketEnd::Close, None)
                }
                end = &mut steps => end,
            };
            let mut end = end;
            if end.0 == SocketEnd::Close && ended_first.is_none() {
                // A graceful close: the control's drop sends the close frame. A session that
                // does not finish in time has its socket dropped below, so the close is not
                // recorded as a clean one.
                drop(control.take());
                let closed = tokio::time::timeout(Duration::from_secs(5), &mut running).await;
                if closed.is_err() {
                    end.0 = SocketEnd::CloseTimedOut;
                }
            }
            // A drop: the run's future, and with it the socket, is dropped here with no close
            // frame sent, as when a process dies.
            (end, ended_first)
        };
        let at = Instant::now();
        let lots = link.borrow().registry().inventory(INST).0;
        let undecoded = session.counters().decode_errors;
        drop(session);
        drop(control);
        let (ws, conns) = session_counts(link.borrow().notes());
        self.ended_counts.0 += ws;
        self.ended_counts.1 += conns;
        self.live = None;
        // What the link noted that leaves the run's order truth incomplete: the steps may all
        // have passed, but the run cannot claim a clean result (as testnet_trade fails on
        // them).
        let problems = link_problems(link.borrow().notes());
        drop(link);
        if !problems.is_empty() {
            for what in &problems {
                let note = format!("the {kind:?} phase: {what}");
                self.lines.line(format_args!("NOTE {note}"));
                self.ev.notes.push(note);
            }
            self.troubled = true;
            let s = self.ev.step("stop");
            s.notes.push(format!(
                "the {kind:?} phase's link noted {} problems (the run's notes): the run cannot \
                 say its order truth is complete",
                problems.len()
            ));
            s.pass = false;
        }
        if let Some(why) = ended_first {
            let step = match kind {
                PhaseKind::Main => "place",
                PhaseKind::Graceful => "cod-graceful",
                PhaseKind::Stop => "stop",
            };
            self.ev.notes.push(format!(
                "the {kind:?} phase's session ended before its steps: {why}"
            ));
            if self.ev.step(step).ran == Ran::NotRun {
                self.fail(step, format!("the session ended first: {why}"));
            }
        }
        if undecoded > 0 {
            self.ev.notes.push(format!(
                "{undecoded} frames or HTTP answers of the {kind:?} phase could not be decoded"
            ));
            self.undecoded = true;
            let s = self.ev.step("stop");
            s.notes.push(
                "an undecodable frame or answer: the run cannot say it saw everything".to_owned(),
            );
            s.pass = false;
        }
        end.1.map(|(label, cid, wire)| CodOrder {
            label,
            cid,
            wire,
            how: end.0,
            at,
            lots,
        })
    }

    /// A phase's registry, link and session.
    #[allow(clippy::type_complexity)]
    fn build_session(
        &mut self,
        creds: Creds<'_>,
    ) -> Result<
        (
            Rc<RefCell<Link>>,
            Rc<Notify>,
            ExecSession<Tap>,
            fbc_runtime::ExecControl,
        ),
        String,
    > {
        let limits = MarketCapsConfig {
            inventory: Some(self.inventory),
            resting: Some(self.resting),
        };
        let caps = PreTradeCaps::new()
            .with_market(INST, limits)
            .map_err(|e| format!("caps: {e:?}"))?;
        let reg = Registry::with_caps(caps)
            .for_testnet_run(TestnetRun::owner_assisted())
            .with_lease_keys(self.keys.clone());
        let ledger = FillLedger::new(
            LedgerConfig {
                max_age: Duration::from_secs(3_600),
                max_entries: 10_000,
            },
            trade_wall_now(),
        )
        .map_err(|e| e.to_string())?;
        let ladder = LadderConfig::new(
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(60),
            2,
        )
        .map_err(|e| format!("{e:?}"))?;
        let link = Rc::new(RefCell::new(Link::new(
            reg,
            ledger,
            self.order_caps.clone(),
            ladder,
        )));
        let wake = Rc::new(Notify::new());
        let handler = Tap {
            inner: LinkHandler {
                link: Rc::clone(&link),
                wake: Rc::clone(&wake),
            },
            tracker: Rc::clone(&self.tracker),
        };
        let cfg = self.session_config(creds()?)?;
        let (mut session, control) =
            ExecSession::new(cfg, handler).map_err(|e| format!("session: {e}"))?;
        session.set_journal(self.journal.clone());
        link.borrow_mut().attach(session.orders(), ACCT);
        Ok((link, wake, session, control))
    }

    /// The session's configuration, as testnet_trade's, the request ids shared by the phases.
    fn session_config(&self, creds: Secrets) -> Result<ExecSessionConfig, String> {
        let venue = &ParadexFactory;
        Ok(ExecSessionConfig {
            venue,
            cfg: self.cfg.clone(),
            creds,
            acct: ACCT,
            rpc_ids: self.rpc_ids.clone(),
            ns: fbc_core::Namespace::new(self.t().namespace),
            specs: self.specs.clone(),
            connector: Connector::new(self.t().proxy.clone()),
            pacing: ReconnectPacing::new(
                Duration::from_millis(250),
                Duration::from_secs(5),
                5,
                Duration::from_secs(60),
                Duration::from_secs(10),
            )
            .expect("valid pacing"),
            clock: self.clock.clone(),
            nonces: Box::new(Counting(1)),
            nonce_source: NonceSourceId(u32::from(ACCT.get())),
            // Each phase its own connection number in the run's one journal.
            conn: u16::try_from(self.tracker.borrow().phase).unwrap_or(u16::MAX),
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a non-zero window"),
            http_max_body: 1 << 20,
        })
    }

    /// The phase's steps; how its socket ends, and the order its disconnect should cancel.
    async fn steps(
        &mut self,
        kind: PhaseKind,
        mut p: Phase,
    ) -> (SocketEnd, Option<(String, ClientOrderId, String)>) {
        let first = match kind {
            PhaseKind::Main => "place",
            PhaseKind::Graceful => "cod-graceful",
            PhaseKind::Stop => "stop",
        };
        if let Err(e) = self.open(kind, &mut p).await {
            self.fail(first, e);
            if kind == PhaseKind::Main {
                self.lines.line(format_args!(
                    "NOTE the first phase did not start: its steps and the cancel-on-disconnect \
                     steps are not run; the stop phase still runs"
                ));
            }
            return (SocketEnd::Close, None);
        }
        match kind {
            PhaseKind::Main => self.main(&p).await,
            PhaseKind::Graceful => {
                let made = self.cod_order(&p, "cod-graceful", "B4").await;
                match made {
                    Some(order) => (SocketEnd::Close, Some(order)),
                    None => {
                        self.stop(&p, "cod-graceful").await;
                        (SocketEnd::Close, None)
                    }
                }
            }
            PhaseKind::Stop => {
                let stopped = self.stop(&p, "stop").await;
                let c = self.compare(&p, "stop", &[]).await;
                let clean = c.orders_agreed && c.positions_agreed;
                let undecoded = self.undecoded || self.troubled;
                let s = self.ev.step("stop");
                s.ran = Ran::Yes;
                s.pass = stopped && clean && !undecoded;
                s.comparisons.push(c);
                (SocketEnd::Close, None)
            }
        }
    }

    /// Login, arm, resync, the hand seed and Start: why it did not happen, if it did not.
    async fn open(&mut self, kind: PhaseKind, p: &mut Phase) -> Result<(), String> {
        let login = p
            .wait(|l| {
                if l.notes()
                    .iter()
                    .any(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
                {
                    return Some(Ok(()));
                }
                match l.login_refusals() {
                    Some((n, why)) if n >= LOGIN_REFUSALS => Some(Err(format!(
                        "login refused {n} times: {why} (testnet_trade's --help lists the usual causes)"
                    ))),
                    _ => None,
                }
            })
            .await
            .unwrap_or_else(|| Err("no login within the step timeout".to_owned()));
        login?;
        self.lines.step(
            "login",
            format_args!("{kind:?} phase: the order socket is authenticated"),
        );
        p.wait(|l| {
            l.notes().iter().find_map(|n| match n {
                Note::Outcome {
                    outcome: SubmitOutcome::Accepted { .. },
                    ours: false,
                    ..
                } => Some(()),
                _ => None,
            })
        })
        .await
        .ok_or("Paradex did not accept cancel-on-disconnect within the step timeout")?;
        self.lines
            .step("arm", format_args!("cancel-on-disconnect accepted"));
        let orders = p.orders.clone();
        let snapshot = p
            .wait(|l| {
                if !orders.may_place() {
                    return None;
                }
                l.notes().iter().rev().find_map(|n| match n {
                    Note::Resynced { snapshot, .. } => Some(Ok(snapshot.clone())),
                    Note::ResyncRefused(why) => Some(Err(why.clone())),
                    _ => None,
                })
            })
            .await
            .ok_or("the resync did not end within the step timeout")?
            .map_err(|why| format!("the registry refused the resync: {why}"))?;
        let on_market: Vec<&VenueOrderSnapshot> =
            snapshot.orders.iter().filter(|o| o.inst == INST).collect();
        let not_ours = on_market
            .iter()
            .filter(|o| !matches!(o.cid, Some(CidMatch::Ours(_))))
            .count();
        p.restored = on_market.len() - not_ours;
        let position = snapshot
            .positions
            .iter()
            .find(|(inst, _)| *inst == INST)
            .map_or(SignedLots(0), |(_, q)| *q);
        self.lines.step(
            "resync",
            format_args!(
                "{} open orders on the market ({} ours, {not_ours} not ours), position {} lots",
                on_market.len(),
                p.restored,
                position.0
            ),
        );
        if not_ours > 0 {
            return Err(format!(
                "{not_ours} open orders on the market are not ours: cancel them first; the run \
                 neither cancels nor counts another trader's orders"
            ));
        }
        // What the disconnect before this phase left resting, as this resync shows it.
        let before = match kind {
            PhaseKind::Main => None,
            PhaseKind::Graceful => Some("cod-dropped"),
            PhaseKind::Stop => Some("cod-graceful"),
        };
        if let Some(d) = before.and_then(|b| self.ev.step(b).disconnect.as_mut()) {
            d.resting_at_reconnect = Some(p.restored > 0);
        }
        if p.restored > 0 {
            // An earlier phase's (or run's) order: the session told it unprotected and the link
            // Safety-cancelled it as the connection opened (decision 0080).
            let orders = p.orders.clone();
            let ended = p
                .wait(|_| orders.may_place_on(INST).then_some(()))
                .await
                .is_some();
            let note = format!(
                "{} orders of ours rested from an earlier connection at this {kind:?} phase's \
                 resync: Safety-cancelled (decision 0080), {}",
                p.restored,
                if ended {
                    "shown ended"
                } else {
                    "NOT shown ended in time"
                }
            );
            self.lines.line(format_args!("NOTE {note}"));
            self.ev.notes.push(note);
            if !ended {
                return Err("an earlier connection's order of ours did not end".to_owned());
            }
        }
        if self.mint.is_none() {
            let ours_max = trade::snapshot_max(&snapshot);
            let lease = self.ns_lease.take().ok_or("the namespace lease is gone")?;
            self.mint = Some(CidMint::new(
                lease,
                self.persisted,
                ours_max,
                trade_wall_now(),
            ));
        }
        let leases = {
            let symbol = self.keys_symbol();
            let market =
                MarketLease::acquire(&self.t().lease_dir, VENUE, ACCOUNT_LABEL, &symbol)
                    .map_err(|e| format!("the market's lease is held (another run?): {e:?}"))?;
            let mut leases = Leases::market(market);
            if self.keys.nonce_scope() == NonceScope::PerAccountMonotonic {
                let acct =
                    fbc_core::AccountLease::acquire(&self.t().lease_dir, VENUE, ACCOUNT_LABEL)
                        .map_err(|e| format!("the account's lease is held: {e:?}"))?;
                leases = leases.with_account(acct);
            }
            leases
        };
        let mut link = p.link.borrow_mut();
        let reg = link.reg();
        if !reg.position_known(INST) {
            reg.seed_position(INST, position)
                .map_err(|e| format!("the hand seed was refused: {e:?}"))?;
        }
        reg.start(INST, leases)
            .map_err(|e| format!("Start was refused: {e:?}"))?;
        drop(link);
        self.lines.step(
            "start",
            format_args!(
                "position {} lots seeded by hand (decision 0067); the market is Started",
                position.0
            ),
        );
        Ok(())
    }

    fn keys_symbol(&self) -> fbc_core::VenueSymbol {
        self.specs
            .get(INST)
            .expect("the one market")
            .venue_symbol
            .clone()
    }

    /// Mints `n` client ids and keeps the high-water mark before any goes out.
    async fn mint(&mut self, n: usize) -> Result<Vec<ClientOrderId>, String> {
        let mut mint = self.mint.take().ok_or("no client-id mint")?;
        let mut cids = Vec::new();
        for _ in 0..n {
            match mint.mint() {
                Ok(c) => cids.push(c),
                Err(e) => {
                    self.mint = Some(mint);
                    return Err(format!("no client id: {e:?}"));
                }
            }
        }
        let mark = mint.high_water();
        let hwm = self.hwm.clone();
        let timeout = Duration::from_secs(self.t().step_timeout_secs);
        match off_thread_holding(timeout, mint, move || hwm.write(mark)).await {
            Some((Ok(()), mint)) => {
                self.mint = Some(mint);
                Ok(cids)
            }
            Some((Err(e), mint)) => {
                self.mint = Some(mint);
                Err(e)
            }
            None => Err("the client-id high-water mark's write did not finish in time".to_owned()),
        }
    }

    /// The order on `side`, `--away-bps` behind a touch read now (and `extra_bps` more): its
    /// price and size, the size no more than the resting cap holds.
    async fn priced(&mut self, side: OrderSide, extra_bps: u32) -> Result<Priced, String> {
        self.book_reads += 1;
        let book = touch(self.t(), &self.specs, &self.limiter).await?;
        self.priced_on(&book, side, extra_bps)
    }

    /// [`Run::priced`] on `book`.
    fn priced_on(
        &self,
        book: &OrderbookSnapshot,
        side: OrderSide,
        extra_bps: u32,
    ) -> Result<Priced, String> {
        let mut opts = with_side(self.t(), side);
        opts.away_bps += extra_bps;
        let mut priced = trade::price(&opts, book)?;
        if priced.qty > self.resting {
            // The resting cap in lots is the dearer side's: the order is cut to it, and must
            // still meet the market's minimum notional.
            priced.qty = self.resting;
            priced.size = Decimal::from(priced.qty.get()) * self.t().step;
            priced.notional = priced.size * priced.px;
            if priced.notional < self.t().min_notional {
                return Err(format!(
                    "the order cut to the resting cap ({} lots) is ${}, below the market's \
                     minimum ${}; raise --resting-cap-usd",
                    priced.qty.get(),
                    priced.notional.round_dp(4),
                    self.t().min_notional
                ));
            }
        }
        Ok(priced)
    }

    /// Follows `cid` under `label` in this phase.
    fn track(&self, cid: ClientOrderId, label: &str) -> String {
        let wire = encode_cid(&self.fmt, cid).map_or_else(|_| "?".to_owned(), |w| w.to_string());
        let mut tr = self.tracker.borrow_mut();
        let phase = tr.phase;
        tr.orders.push(Tracked {
            label: label.to_owned(),
            cid,
            wire: wire.clone(),
            phase,
            vid: None,
            states: Vec::new(),
        });
        wire
    }

    /// The order to build for a place.
    fn new_order(
        cid: ClientOrderId,
        side: Side,
        priced: &Priced,
        tif: Tif,
        post_only: bool,
    ) -> NewOrder {
        NewOrder {
            cid,
            inst: INST,
            side,
            kind: OrderKind::Limit { px: priced.ticks },
            qty: priced.qty,
            tif,
            channel: Channel::Public,
            post_only,
            reduce_only: false,
            reducing: false,
        }
    }

    /// Records request `rpc` of `kind` with `items` items on `step`, its outcomes as reported.
    fn sent(&mut self, p: &Phase, step: &str, kind: &str, rpc: fbc_core::RpcId, items: usize) {
        let outcomes = p.outcomes(rpc);
        let s = self.ev.step(step);
        s.ran = Ran::Yes;
        match s.sent.iter_mut().find(|c| c.rpc == rpc.0) {
            Some(c) => c.outcomes = outcomes,
            None => s.sent.push(Sent {
                kind: kind.to_owned(),
                rpc: rpc.0,
                items,
                outcomes,
            }),
        }
    }

    /// Every outcome the session has reported so far for each request `step` sent: a
    /// placement's final phase (0092) comes on its order event, after the reply's provisional
    /// acceptance the step first recorded.
    fn refresh_outcomes(&mut self, p: &Phase, step: &str) {
        let rpcs: Vec<u64> = self.ev.step(step).sent.iter().map(|c| c.rpc).collect();
        for rpc in rpcs {
            let outcomes = p.outcomes(fbc_core::RpcId(rpc));
            if let Some(c) = self.ev.step(step).sent.iter_mut().find(|c| c.rpc == rpc) {
                c.outcomes = outcomes;
            }
        }
    }

    /// Places one order of ours on `side`; its client id and wire id, or why not.
    async fn place_one(
        &mut self,
        p: &Phase,
        step: &str,
        label: &str,
        side: OrderSide,
        tif: Tif,
        post_only: bool,
    ) -> Result<(ClientOrderId, String), String> {
        let priced = self.priced(side, 0).await?;
        let cid = self.mint(1).await?[0];
        let wire = self.track(cid, label);
        let new = Run::new_order(cid, side_of(side), &priced, tif, post_only);
        let cmd = p
            .link
            .borrow_mut()
            .reg()
            .place(new)
            .map_err(|e| format!("{e:?}"))?;
        let rpc = p.submit(cmd)?;
        self.lines.step(
            step,
            format_args!(
                "request {}: {label} {} {} @ {} {tif:?}{}",
                rpc.0,
                if side == OrderSide::Buy {
                    "buy"
                } else {
                    "sell"
                },
                priced.size,
                priced.px,
                if post_only { " post-only" } else { "" }
            ),
        );
        let answer = p.answered(rpc).await;
        self.sent(p, step, "place", rpc, 1);
        match answer {
            Some(SubmitOutcome::Accepted { .. }) => Ok((cid, wire)),
            Some(other) => Err(format!("the place came back {other:?}")),
            None => Err("no answer to the place within the step timeout".to_owned()),
        }
    }

    /// The main phase's steps (module documentation).
    async fn main(&mut self, p: &Phase) -> (SocketEnd, Option<(String, ClientOrderId, String)>) {
        let side = self.t().side;
        // place
        let b1 = match self.place_one(p, "place", "B1", side, Tif::Gtc, true).await {
            Ok(b1) => b1,
            Err(e) => {
                self.fail("place", e);
                self.stop(p, "place").await;
                return (SocketEnd::Close, None);
            }
        };
        let open = p.until(&[b1.0], resting_or_ended).await;
        self.finish_step(
            p,
            "place",
            &[b1.0],
            open && p.state(b1.0) == Some(OrdState::Open),
        )
        .await;
        self.history("place", b1.0, &["NEW", "OPEN"]).await;
        if !self.ev.step("place").pass {
            self.stop(p, "place").await;
            return (SocketEnd::Close, None);
        }

        // amend
        if !self.amend(p, b1.0).await {
            self.stop(p, "amend").await;
            return (SocketEnd::Close, None);
        }

        // cancel
        if !self.cancel(p, "cancel", &[b1.0]).await {
            self.stop(p, "cancel").await;
            return (SocketEnd::Close, None);
        }
        self.history("cancel", b1.0, &["CLOSED"]).await;

        // batch, cancel-many
        match self.batch(p).await {
            Some(Some(pair)) => {
                if !self.cancel(p, "cancel-many", &pair).await {
                    self.stop(p, "cancel-many").await;
                    return (SocketEnd::Close, None);
                }
            }
            Some(None) => {}
            None => {
                self.stop(p, "batch").await;
                return (SocketEnd::Close, None);
            }
        }

        // ioc
        match self.place_one(p, "ioc", "IOC", side, Tif::Ioc, false).await {
            Ok((cid, _)) => {
                let ended = p.until(&[cid], OrdState::is_terminal).await;
                // An expiry: ended cancelled or expired with nothing filled. One that filled
                // (the touch moved under it) exercised no expiry.
                let expired = ended && {
                    let link = p.link.borrow();
                    link.registry().get(cid).is_some_and(|r| {
                        r.filled() == Lots::ZERO
                            && matches!(
                                r.state(),
                                OrdState::Terminal(
                                    fbc_oms::TerminalKind::Canceled(_)
                                        | fbc_oms::TerminalKind::Expired
                                )
                            )
                    })
                };
                if ended && !expired {
                    self.ev.step("ioc").notes.push(
                        "the IOC did not expire unfilled (it filled, or ended another way): no \
                         expiry was exercised"
                            .to_owned(),
                    );
                }
                let ended = expired;
                self.finish_step(p, "ioc", &[cid], ended).await;
                self.history("ioc", cid, &["CLOSED"]).await;
            }
            // An IOC rests on nothing: one refused or unanswered is recorded, and the run goes
            // on (the registry's caps count an unanswered one until it is resolved).
            Err(e) => self.fail("ioc", e),
        }

        // cod-dropped
        match self.cod_order(p, "cod-dropped", "B3").await {
            Some(order) => (SocketEnd::Drop, Some(order)),
            None => {
                self.stop(p, "cod-dropped").await;
                (SocketEnd::Close, None)
            }
        }
    }

    /// Places the order a disconnect should cancel and confirms it rests, on the stream and
    /// over REST: it, or `None` (the step failed).
    async fn cod_order(
        &mut self,
        p: &Phase,
        step: &str,
        label: &str,
    ) -> Option<(String, ClientOrderId, String)> {
        let side = self.t().side;
        match self.place_one(p, step, label, side, Tif::Gtc, true).await {
            Ok((cid, wire)) => {
                let open = p.until(&[cid], resting_or_ended).await;
                let rests = open && p.state(cid) == Some(OrdState::Open);
                let c = self
                    .compare(p, &format!("{step}: {label} resting"), &[cid])
                    .await;
                let listed = c.orders_agreed;
                self.refresh_outcomes(p, step);
                let s = self.ev.step(step);
                s.comparisons.push(c);
                if !(rests && listed) {
                    self.fail(
                        step,
                        format!("{label} did not rest, or REST never listed it"),
                    );
                    return None;
                }
                let how = if step == "cod-dropped" {
                    "dropping"
                } else {
                    "closing"
                };
                self.lines.step(
                    step,
                    format_args!(
                        "{label} rests; {how} the order socket with cancel-on-disconnect armed"
                    ),
                );
                Some((label.to_owned(), cid, wire))
            }
            Err(e) => {
                self.fail(step, e);
                None
            }
        }
    }

    /// Ends a step whose orders `cids` reached their state (`reached`): their traces, a REST
    /// comparison, and its verdict.
    async fn finish_step(&mut self, p: &Phase, step: &str, cids: &[ClientOrderId], reached: bool) {
        let c = self.compare(p, step, cids).await;
        let traces: Vec<Trace> = {
            let tr = self.tracker.borrow();
            cids.iter().map(|c| tr.trace(*c)).collect()
        };
        let agreed = c.orders_agreed && c.positions_agreed;
        let states: Vec<String> = traces
            .iter()
            .map(|t| t.states.last().cloned().unwrap_or_default())
            .collect();
        self.lines.step(
            step,
            format_args!(
                "{} [{}]; REST agreed: orders {} positions {}",
                if reached { "reached" } else { "NOT reached" },
                states.join("; "),
                c.orders_agreed,
                c.positions_agreed
            ),
        );
        self.refresh_outcomes(p, step);
        let s = self.ev.step(step);
        s.ran = Ran::Yes;
        s.orders.extend(traces);
        s.comparisons.push(c);
        s.pass = reached && agreed;
        if !reached {
            s.notes.push(
                "an order did not reach the state the step expects within the step timeout"
                    .to_owned(),
            );
        }
    }

    /// The amend: further from the touch and, where the minimum notional allows, one step
    /// smaller (order.modify's size is the total). True when it was confirmed.
    async fn amend(&mut self, p: &Phase, cid: ClientOrderId) -> bool {
        let side = self.t().side;
        let (qty, px) = {
            let link = p.link.borrow();
            let rec = link.registry().get(cid).expect("the placed order");
            (rec.qty(), rec.px())
        };
        let Some(px) = px else {
            self.fail("amend", "the order has no price");
            return false;
        };
        // Ten basis points further, at least one tick.
        let tick = self.t().tick;
        let price = Decimal::from(px.0) * tick;
        let ticks = (price * Decimal::new(10, 4) / tick)
            .ceil()
            .to_i64()
            .unwrap_or(1)
            .max(1);
        let new_px = match side {
            OrderSide::Buy => Ticks(px.0 - ticks),
            OrderSide::Sell => Ticks(px.0 + ticks),
        };
        let smaller = Lots::new(qty.get() - 1).filter(|l| l.get() > 0);
        let notional =
            |l: Lots| Decimal::from(l.get()) * self.t().step * Decimal::from(new_px.0) * tick;
        let new_qty = smaller
            .filter(|l| notional(*l) >= self.t().min_notional)
            .unwrap_or(qty);
        let built = {
            let mut link = p.link.borrow_mut();
            let caps = link.caps().clone();
            link.reg()
                .live(cid)
                .map_err(|e| format!("{e:?}"))
                .and_then(|live| {
                    live.amend(&caps, new_px, new_qty, false)
                        .map_err(|e| format!("{e:?}"))
                })
        };
        let rpc = match built.and_then(|cmd| p.submit(cmd)) {
            Ok(rpc) => rpc,
            Err(e) => {
                self.fail(
                    "amend",
                    format!("the amend was not built or submitted: {e}"),
                );
                return false;
            }
        };
        let shown = (Decimal::from(new_px.0) * tick).normalize();
        let size = (Decimal::from(new_qty.get()) * self.t().step).normalize();
        self.lines.step(
            "amend",
            format_args!("request {}: B1 to {shown} x {size} (the total)", rpc.0),
        );
        let answer = p.answered(rpc).await;
        self.sent(p, "amend", "amend", rpc, 1);
        if !matches!(answer, Some(SubmitOutcome::Accepted { .. })) {
            self.fail("amend", format!("the amend came back {answer:?}"));
            return false;
        }
        let confirmed = p
            .wait(|l| {
                let rec = l.registry().get(cid)?;
                let done = rec.state().is_terminal()
                    || (!rec.amend_unconfirmed()
                        && rec.px() == Some(new_px)
                        && rec.qty() == new_qty);
                done.then_some(())
            })
            .await
            .is_some();
        let rests = p.state(cid) == Some(OrdState::Open);
        self.sent(p, "amend", "amend", rpc, 1);
        self.finish_step(p, "amend", &[cid], confirmed && rests)
            .await;
        self.ev.step("amend").pass
    }

    /// Cancels `cids` (one: a cancel; more: a cancel-many) and waits for them to end. True when
    /// each ended cancelled and REST agreed.
    async fn cancel(&mut self, p: &Phase, step: &str, cids: &[ClientOrderId]) -> bool {
        let built: Result<Vec<PermittedCommand>, String> = {
            let mut link = p.link.borrow_mut();
            let caps = link.caps().clone();
            if cids.len() == 1 {
                match link
                    .reg()
                    .cancellable(cids[0])
                    .map(|permit| permit.cancel(&caps))
                {
                    Ok(CancelChoice::Send(cmd)) => Ok(vec![cmd]),
                    Ok(CancelChoice::AwaitAck) => {
                        Err("the cancel waits for an acknowledgement".to_owned())
                    }
                    Err(e) => Err(format!("{e:?}")),
                }
            } else {
                let plan = link.reg().cancel_many(cids, &caps);
                if plan.refused.is_empty() && plan.awaiting_ack.is_empty() {
                    Ok(plan.commands)
                } else {
                    Err(format!(
                        "cancel-many: refused {:?}, awaiting acknowledgement {:?}",
                        plan.refused, plan.awaiting_ack
                    ))
                }
            }
        };
        let cmds = match built {
            Ok(cmds) => cmds,
            Err(e) => {
                self.fail(step, e);
                return false;
            }
        };
        let kind = if cids.len() == 1 {
            "cancel"
        } else {
            "cancel-many"
        };
        let mut rpcs = Vec::new();
        for cmd in cmds {
            match p.submit(cmd) {
                Ok(rpc) => rpcs.push(rpc),
                Err(e) => {
                    self.fail(step, format!("a cancel was not submitted: {e}"));
                    return false;
                }
            }
        }
        let shown: Vec<String> = rpcs.iter().map(|r| r.0.to_string()).collect();
        self.lines.step(
            step,
            format_args!(
                "requests [{}]: {kind} of {} orders",
                shown.join(", "),
                cids.len()
            ),
        );
        let mut accepted = true;
        for rpc in &rpcs {
            let answer = p.answered(*rpc).await;
            self.sent(p, step, kind, *rpc, cids.len());
            accepted &= matches!(answer, Some(SubmitOutcome::Accepted { .. }));
        }
        let ended = p.until(cids, OrdState::is_terminal).await;
        let cancelled = cids.iter().all(|c| {
            matches!(
                p.state(*c),
                Some(OrdState::Terminal(fbc_oms::TerminalKind::Canceled(_)))
            )
        });
        for rpc in &rpcs {
            self.sent(p, step, kind, *rpc, cids.len());
        }
        self.finish_step(p, step, cids, accepted && ended && cancelled)
            .await;
        self.ev.step(step).pass
    }

    /// The batch of two: a buy below the bid and a sell above the ask. `Some(Some(ids))` when
    /// both rest, `Some(None)` when skipped (an empty side of the book), `None` when it failed.
    async fn batch(&mut self, p: &Phase) -> Option<Option<Vec<ClientOrderId>>> {
        self.book_reads += 1;
        let book = match touch(self.t(), &self.specs, &self.limiter).await {
            Ok(book) => book,
            Err(e) => {
                self.fail("batch", e);
                return None;
            }
        };
        // Only the venue's state skips the pair: a side of the book with nothing resting, so
        // no order can rest behind it. Anything else (a read that failed, a size the caps or
        // the minimum notional refuse) fails the step.
        if book.bids.is_empty() || book.asks.is_empty() {
            let why = format!(
                "the order book has no {} resting, so no order rests behind that side",
                if book.bids.is_empty() { "bid" } else { "ask" }
            );
            for step in ["batch", "cancel-many"] {
                self.ev.step(step).ran = Ran::Skipped(why.clone());
            }
            self.lines
                .line(format_args!("NOTE batch and cancel-many skipped: {why}"));
            return Some(None);
        }
        let priced = (
            self.priced_on(&book, OrderSide::Buy, 0),
            self.priced_on(&book, OrderSide::Sell, 0),
        );
        let (buy, sell) = match priced {
            (Ok(b), Ok(s)) => (b, s),
            (b, s) => {
                self.fail("batch", b.err().or(s.err()).unwrap_or_default());
                return None;
            }
        };
        let cids = match self.mint(2).await {
            Ok(c) => c,
            Err(e) => {
                self.fail("batch", e);
                return None;
            }
        };
        self.track(cids[0], "B2 buy");
        self.track(cids[1], "S2 sell");
        let orders = vec![
            Run::new_order(cids[0], Side::Buy, &buy, Tif::Gtc, true),
            Run::new_order(cids[1], Side::Sell, &sell, Tif::Gtc, true),
        ];
        let plan = p.link.borrow_mut().reg().place_batch(orders);
        let plan = match plan {
            Ok(plan) if plan.refused.is_empty() => plan,
            Ok(plan) => {
                self.fail("batch", format!("items refused: {:?}", plan.refused));
                return None;
            }
            Err(e) => {
                self.fail("batch", format!("{e:?}"));
                return None;
            }
        };
        let Some(cmd) = plan.command else {
            self.fail("batch", "no batch was built");
            return None;
        };
        let rpc = match p.submit(cmd) {
            Ok(rpc) => rpc,
            Err(e) => {
                self.fail("batch", e);
                return None;
            }
        };
        self.lines.step(
            "batch",
            format_args!(
                "request {}: buy {} @ {} and sell {} @ {}, post-only",
                rpc.0, buy.size, buy.px, sell.size, sell.px
            ),
        );
        let answer = p.answered(rpc).await;
        self.sent(p, "batch", "batch", rpc, 2);
        if !matches!(answer, Some(SubmitOutcome::Accepted { .. })) {
            self.fail("batch", format!("the batch came back {answer:?}"));
            return None;
        }
        let rest = p.until(&cids, resting_or_ended).await;
        let both_open = cids.iter().all(|c| p.state(*c) == Some(OrdState::Open));
        self.sent(p, "batch", "batch", rpc, 2);
        self.finish_step(p, "batch", &cids, rest && both_open).await;
        for cid in &cids {
            self.history("batch", *cid, &["NEW", "OPEN"]).await;
        }
        if self.ev.step("batch").pass {
            Some(Some(cids))
        } else {
            None
        }
    }

    /// Stop: the kill switch, then the cancel of everything of ours on the market, each
    /// acknowledged and each order ended. True when it all happened. Notes go on `step`.
    async fn stop(&mut self, p: &Phase, step: &str) -> bool {
        let (cmds, mut waiting, refused) = {
            let mut link = p.link.borrow_mut();
            let caps = link.caps().clone();
            let reg = link.reg();
            reg.kill(INST);
            match reg.cancel_everything(INST, &caps) {
                CancelEverything::CancelAll {
                    command,
                    unanswered,
                } => {
                    let mut cmds = vec![command];
                    cmds.extend(unanswered.commands);
                    (cmds, unanswered.awaiting_ack, Vec::new())
                }
                CancelEverything::Explicit { plan, .. } => (
                    plan.commands,
                    plan.awaiting_ack,
                    plan.refused
                        .iter()
                        .map(|(c, r)| format!("{c:?}: {r:?}"))
                        .collect::<Vec<_>>(),
                ),
            }
        };
        let mut ok = refused.is_empty();
        let mut rpcs = Vec::new();
        for cmd in cmds {
            match p.submit(cmd) {
                Ok(rpc) => rpcs.push(rpc),
                Err(e) => {
                    ok = false;
                    self.ev
                        .step(step)
                        .notes
                        .push(format!("a Stop cancel was not submitted: {e}"));
                }
            }
        }
        // Cancels that wait for their order's acknowledgement go out as each lands.
        while !waiting.is_empty() {
            let due = p
                .wait(|l| {
                    let caps = l.caps().clone();
                    let due: Vec<ClientOrderId> = l
                        .reg()
                        .cancels_due(&caps)
                        .into_iter()
                        .filter(|c| waiting.contains(c))
                        .collect();
                    let ended: Vec<ClientOrderId> = waiting
                        .iter()
                        .copied()
                        .filter(|c| l.registry().get(*c).is_none_or(|r| r.state().is_terminal()))
                        .collect();
                    (!due.is_empty() || !ended.is_empty()).then_some((due, ended))
                })
                .await;
            let Some((due, ended)) = due else {
                ok = false;
                self.ev.step(step).notes.push(format!(
                    "{} orders stayed unacknowledged: Stop could not cancel them",
                    waiting.len()
                ));
                break;
            };
            waiting.retain(|c| !due.contains(c) && !ended.contains(c));
            for cid in due {
                let built = {
                    let mut link = p.link.borrow_mut();
                    let caps = link.caps().clone();
                    link.reg()
                        .cancellable(cid)
                        .map(|permit| permit.cancel(&caps))
                };
                match built {
                    Ok(CancelChoice::Send(cmd)) => match p.submit(cmd) {
                        Ok(rpc) => rpcs.push(rpc),
                        Err(_) => ok = false,
                    },
                    Ok(CancelChoice::AwaitAck) => waiting.push(cid),
                    Err(_) => ok = false,
                }
            }
        }
        for rpc in &rpcs {
            let answer = p.answered(*rpc).await;
            self.sent(
                p,
                step,
                "stop cancel",
                *rpc,
                p.link.borrow().cids_of(*rpc).len(),
            );
            ok &= matches!(answer, Some(SubmitOutcome::Accepted { .. }));
        }
        let named: Vec<ClientOrderId> = {
            let link = p.link.borrow();
            rpcs.iter().flat_map(|r| link.cids_of(*r)).collect()
        };
        if !named.is_empty() {
            ok &= p.until(&named, OrdState::is_terminal).await;
        }
        p.link.borrow_mut().reg().disarm(INST);
        self.lines.step(
            "stop",
            format_args!(
                "kill switch on; {} cancels sent; market disarmed{}",
                rpcs.len(),
                if ok {
                    ""
                } else {
                    "; NOT everything was cancelled"
                }
            ),
        );
        if !ok {
            self.ev
                .step(step)
                .notes
                .push("Stop did not cancel everything of ours".to_owned());
        }
        ok
    }

    /// After a phase ended its socket with `cod` resting: GET /orders until it is gone or
    /// `--cod-wait` passes, then its history (did it end cancelled with nothing filled?), then
    /// GET /orders and GET /positions against what the phase's stream last showed (the order
    /// gone if cancel-on-disconnect worked; the position the fills imply).
    async fn after_disconnect(&mut self, step: &str, cod: CodOrder) {
        let how = match cod.how {
            SocketEnd::Drop => "dropped (no close frame)",
            SocketEnd::Close => {
                "graceful close (the session's close frame; fbc-runtime does not yet report \
                 whether the frame was written, FBC-q40h)"
            }
            SocketEnd::CloseTimedOut => {
                "closed, but the session did not finish within 5 s and its socket was then \
                 dropped: neither a clean graceful close nor a clean drop"
            }
        };
        let wait = Duration::from_secs(self.opts.cod_wait_secs);
        let deadline = Instant::now() + wait;
        let mut gone_at = None;
        // Whether an answer read after the disconnect listed the order: "not cancelled" is
        // said only of an order seen listed, never of one no answer showed.
        let mut still_listed = false;
        let mut errors = Vec::new();
        // Every read is bounded by the wait; one the wait cut short is no error once an answer
        // was read, and the step's error when none was.
        let mut first = true;
        loop {
            if !std::mem::take(&mut first) && Instant::now() >= deadline {
                break;
            }
            let read = self
                .rest
                .get(open_orders_request(&self.t().rest_url), deadline)
                .await;
            if read.is_err() && Instant::now() >= deadline {
                if !still_listed {
                    errors.push(format!(
                        "no GET /orders answered within --cod-wait ({} s): the disconnect was \
                         not measured",
                        wait.as_secs()
                    ));
                }
                break;
            }
            match read {
                Ok(answer) if answer.status == 200 => match parse_results(&answer.body) {
                    Ok(rows) => {
                        let listed = rows.iter().any(|o| {
                            o.get("client_id").and_then(Value::as_str) == Some(cod.wire.as_str())
                        });
                        if !listed {
                            gone_at = Some(Instant::now());
                            still_listed = false;
                            self.rest_bodies
                                .push((format!("orders-after-{step}"), answer.body));
                            break;
                        }
                        still_listed = true;
                    }
                    Err(e) => errors.push(format!("GET /orders: {e}")),
                },
                Ok(answer) => errors.push(format!("GET /orders status {}", answer.status)),
                Err(e) => errors.push(e),
            }
            tokio::time::sleep(POLL).await;
        }
        let history = self
            .history_read(&cod.label, &cod.wire, &["CLOSED"], cod.at, wait)
            .await;
        // Gone from GET /orders proves only that it no longer rests: cancelled by the
        // disconnect means its history shows it CLOSED with quantities read and nothing filled.
        let filled = history.filled == Some(true);
        let cancelled = disconnect_cancelled(gone_at.is_some(), &history);
        // The stream saw nothing after the socket ended, so no order of it is compared: the
        // order's fate is this step's disconnect row (REST only); the position is compared
        // with what the phase's fills imply.
        let c = self
            .compare_rest(
                &format!("{step}: after the disconnect (position only)"),
                &[],
                cod.at,
                cod.lots,
                false,
            )
            .await;
        self.lines.step(
            step,
            format_args!(
                "{how}: {} {}",
                cod.label,
                if cancelled {
                    format!(
                        "cancelled by Paradex (GET /orders stopped listing it after {} ms; \
                         history CLOSED, nothing filled, cancel_reason {})",
                        gone_at.map_or(0, |g| g.duration_since(cod.at).as_millis()),
                        history.cancel_reason.as_deref().unwrap_or("-")
                    )
                } else if filled {
                    "FILLED before or during the disconnect: the experiment says nothing of \
                     cancel-on-disconnect"
                        .to_owned()
                } else if gone_at.is_some() {
                    "gone from GET /orders, but its history never showed it CLOSED with readable \
                     quantities: not judged"
                        .to_owned()
                } else if !still_listed {
                    "not measured: no GET /orders answer was read after the disconnect".to_owned()
                } else {
                    format!(
                        "still listed after {} s: NOT cancelled on this disconnect",
                        wait.as_secs()
                    )
                }
            ),
        );
        let trace = self.tracker.borrow().trace(cod.cid);
        let s = self.ev.step(step);
        s.ran = Ran::Yes;
        s.orders.push(trace);
        // The step records what Paradex did; either answer is evidence, so it passes once it
        // was measured cleanly: the socket ended as intended, the order neither filled nor
        // went unexplained, every REST read answered, and the position is the fills'.
        let measured = disconnect_measured(cancelled, still_listed, !errors.is_empty(), filled);
        s.pass = measured
            && cod.how != SocketEnd::CloseTimedOut
            && !filled
            && c.positions_agreed
            && history.error.is_none();
        if filled {
            s.notes.push(format!(
                "{} filled: no cancel-on-disconnect verdict",
                cod.label
            ));
        }
        if cod.how == SocketEnd::CloseTimedOut {
            s.notes.push(how.to_owned());
        }
        s.notes.extend(errors);
        s.comparisons.push(c);
        s.disconnect = Some(Disconnect {
            how: how.to_owned(),
            cancelled,
            still_listed,
            after_ms: gone_at.map(|g| g.duration_since(cod.at).as_millis()),
            waited_s: wait.as_secs(),
            history: Some(history),
            // The next phase's resync says ([`Run::open`]).
            resting_at_reconnect: None,
        });
    }

    /// GET /orders-history?client_id= for our order `cid` (step `step`) until it lists the
    /// order with one of the `wanted` statuses, noted on the step.
    async fn history(&mut self, step: &str, cid: ClientOrderId, wanted: &[&str]) {
        let (label, wire, since) = {
            let tr = self.tracker.borrow();
            let Some(o) = tr.get(cid) else { return };
            let since = o.states.last().map_or_else(Instant::now, |(_, at)| *at);
            (o.label.clone(), o.wire.clone(), since)
        };
        let wait = Duration::from_secs(self.opts.rest_wait_secs);
        let h = self.history_read(&label, &wire, wanted, since, wait).await;
        self.lines.step(
            step,
            format_args!(
                "orders-history {label}: {} after {} ms ({} reads)",
                if h.listed {
                    h.status.clone().unwrap_or_default()
                } else {
                    "NOT listed".to_owned()
                },
                h.lag_ms.unwrap_or(0),
                h.reads
            ),
        );
        let s = self.ev.step(step);
        s.histories.push(h);
    }

    /// The history read itself.
    async fn history_read(
        &mut self,
        label: &str,
        wire: &str,
        wanted: &[&str],
        since: Instant,
        wait: Duration,
    ) -> History {
        let deadline = Instant::now() + wait;
        let mut h = History {
            label: label.to_owned(),
            wanted: wanted.join("/"),
            ..History::default()
        };
        loop {
            if h.reads > 0 && Instant::now() >= deadline {
                return h;
            }
            let read = self
                .rest
                .get(order_history_request(&self.t().rest_url, wire), deadline)
                .await;
            // A read the wait cut short is no answer, and no error.
            if h.reads > 0 && read.is_err() && Instant::now() >= deadline {
                return h;
            }
            h.reads += 1;
            match read {
                Ok(answer) if answer.status == 200 => match parse_results(&answer.body) {
                    Ok(rows) => {
                        h.error = None;
                        let row = rows
                            .iter()
                            .find(|o| o.get("client_id").and_then(Value::as_str) == Some(wire));
                        if let Some(row) = row {
                            let text = |k: &str| row.get(k).and_then(Value::as_str);
                            let status = text("status").unwrap_or("").to_owned();
                            h.status = Some(status.clone());
                            h.cancel_reason = text("cancel_reason").map(str::to_owned);
                            // Filled when what remains is less than its size.
                            let num = |k: &str| text(k).and_then(|v| v.parse::<Decimal>().ok());
                            h.filled = match (num("size"), num("remaining_size")) {
                                (Some(size), Some(left)) => Some(left < size),
                                _ => None,
                            };
                            if wanted.contains(&status.as_str()) {
                                h.listed = true;
                                h.lag_ms = Some(since.elapsed().as_millis());
                                self.rest_bodies.push((
                                    format!("orders-history-{}", label.replace(' ', "-")),
                                    answer.body,
                                ));
                                return h;
                            }
                        }
                    }
                    Err(e) => h.error = Some(e),
                },
                Ok(answer) => h.error = Some(format!("status {}", answer.status)),
                Err(e) => h.error = Some(e),
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Reads GET /orders and GET /positions until they agree with what the stream shows of
    /// `cids` (and the market's position), or `--rest-wait` passes. With no `cids` (the last
    /// comparison), nothing at all may rest on the market.
    async fn compare(&mut self, p: &Phase, after: &str, cids: &[ClientOrderId]) -> Comparison {
        let tick = self.t().tick;
        let step = self.t().step;
        // What the stream shows: present at a price and total, or absent; none while it awaits
        // an answer (it is then not compared).
        let (expected, since, stream_lots) = {
            let link = p.link.borrow();
            let tr = self.tracker.borrow();
            let mut since = None::<Instant>;
            let mut expected = Vec::new();
            for cid in cids {
                let Some(rec) = link.registry().get(*cid) else {
                    continue;
                };
                let Some(o) = tr.get(*cid) else { continue };
                if let Some((_, at)) = o.states.last() {
                    since = Some(since.map_or(*at, |s| s.max(*at)));
                }
                let want = match rec.state() {
                    OrdState::Open | OrdState::PartiallyFilled => Some((
                        rec.px().map(|p| Decimal::from(p.0) * tick),
                        Decimal::from(rec.qty().get()) * step,
                    )),
                    s if s.is_terminal() => None,
                    _ => continue,
                };
                expected.push((o.label.clone(), o.wire.clone(), want));
            }
            let lots = link.registry().inventory(INST).0;
            (expected, since.unwrap_or_else(Instant::now), lots)
        };
        self.compare_rest(after, &expected, since, stream_lots, cids.is_empty())
            .await
    }

    /// [`Run::compare`]'s reads: `expected` (each order's label, client id as sent, and its
    /// price and total while it rests, `None` once ended) and the market's position in lots,
    /// from the stream's last change at `since`; `empty`: nothing may rest on the market.
    async fn compare_rest(
        &mut self,
        after: &str,
        expected: &[Expected],
        since: Instant,
        stream_lots: i64,
        empty: bool,
    ) -> Comparison {
        let step = self.t().step;
        let stream_position = (Decimal::from(stream_lots) * step).normalize();
        // What was sent since the last comparison, for the per-IP limit check: the program's
        // own REST requests (this reader's, logins included, and the order-book reads), the
        // WebSocket order methods, and the connections opened (each a login and two reads).
        let mut c = Comparison {
            after: after.to_owned(),
            expected: expected
                .iter()
                .map(|(label, _, want)| {
                    (
                        label.clone(),
                        match want {
                            Some((px, qty)) => format!(
                                "present {} x {}",
                                px.map_or("-".to_owned(), |p| p.normalize().to_string()),
                                qty.normalize()
                            ),
                            None => "absent".to_owned(),
                        },
                    )
                })
                .collect(),
            stream_position: stream_position.to_string(),
            orders_compared: !expected.is_empty() || empty,
            ..Comparison::default()
        };
        let wait = Duration::from_secs(self.opts.rest_wait_secs);
        let deadline = Instant::now() + wait;
        let market = self.t().market.clone();
        // The answers of the last round after the first, kept for the fixtures beside the first
        // round's: the ones that agreed, or the last read when none did.
        let (mut later_orders, mut later_positions) = (None::<Vec<u8>>, None::<Vec<u8>>);
        loop {
            if c.reads > 0 && Instant::now() >= deadline {
                break;
            }
            // Every read is bounded by the wait. A round the wait cut short is no answer: the
            // round before it stands (the first round's failures are recorded).
            let orders = self
                .rest
                .get(open_orders_request(&self.t().rest_url), deadline)
                .await;
            if c.reads == 0 {
                // The per-IP check's sample: everything sent since the last sample, up to and
                // including this read (and any login it made), is counted against the
                // rate-limit headers it carries.
                let rest_now = self.rest.requests() + self.book_reads;
                let (ws_now, conns_now) = self.session_totals();
                let (rest_before, ws_before, conns_before) = self.sampled;
                c.rest_since = rest_now - rest_before;
                c.ws_since = ws_now - ws_before;
                c.connections_since = conns_now - conns_before;
                self.sampled = (rest_now, ws_now, conns_now);
            }
            let positions = self
                .rest
                .get(positions_request(&self.t().rest_url), deadline)
                .await;
            if c.reads > 0 && (orders.is_err() || positions.is_err()) && Instant::now() >= deadline
            {
                break;
            }
            c.reads += 1;
            let (mut mismatches, mut agreed_orders, mut agreed_positions) =
                (Vec::new(), false, false);
            match &orders {
                Ok(RestAnswer {
                    status: 200,
                    body,
                    limits,
                }) => {
                    if c.reads == 1 {
                        c.limits = limits.clone();
                        c.page_keys = top_keys(body);
                        self.rest_bodies
                            .push((format!("orders-{}", slug(after)), body.clone()));
                    } else {
                        later_orders = Some(body.clone());
                    }
                    match parse_results(body) {
                        Ok(rows) => {
                            let on_market: Vec<&Value> = rows
                                .iter()
                                .filter(|o| {
                                    o.get("market").and_then(Value::as_str) == Some(market.as_str())
                                })
                                .collect();
                            mismatches.extend(order_mismatches(expected, &on_market));
                            if empty && !on_market.is_empty() {
                                mismatches.push(format!(
                                    "{} orders still listed on the market",
                                    on_market.len()
                                ));
                            }
                            agreed_orders = mismatches.is_empty();
                        }
                        Err(e) => c.errors.push(format!("GET /orders: {e}")),
                    }
                }
                Ok(other) => c
                    .errors
                    .push(format!("GET /orders status {}", other.status)),
                Err(e) => c.errors.push(e.clone()),
            }
            match &positions {
                Ok(RestAnswer {
                    status: 200, body, ..
                }) => {
                    if c.reads == 1 {
                        self.rest_bodies
                            .push((format!("positions-{}", slug(after)), body.clone()));
                    } else {
                        later_positions = Some(body.clone());
                    }
                    match parse_results(body) {
                        Ok(rows) => {
                            let row = rows.iter().find(|o| {
                                o.get("market").and_then(Value::as_str) == Some(market.as_str())
                            });
                            let field = |o: &Value, k: &str| {
                                o.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
                            };
                            c.rest_position = row.map(|o| RestPosition {
                                side: field(o, "side"),
                                size: field(o, "size"),
                                status: field(o, "status"),
                            });
                            match position_mismatch(c.rest_position.as_ref(), stream_position) {
                                None => agreed_positions = true,
                                Some(why) => mismatches.push(why),
                            }
                        }
                        Err(e) => c.errors.push(format!("GET /positions: {e}")),
                    }
                }
                Ok(other) => c
                    .errors
                    .push(format!("GET /positions status {}", other.status)),
                Err(e) => c.errors.push(e.clone()),
            }
            if c.reads == 1 {
                // Each verdict on its own: question (a) is of GET /orders alone.
                c.first_read_orders_agreed = agreed_orders;
                c.first_read_positions_agreed = agreed_positions;
                c.first_mismatches = mismatches.clone();
            }
            if agreed_orders && agreed_positions {
                c.orders_agreed = true;
                c.positions_agreed = true;
                // A disconnect's position-only comparison reads only after the disconnect's
                // own wait: its time to agree is no lag behind the stream, so it has none.
                if c.orders_compared {
                    c.lag_ms = Some(since.elapsed().as_millis());
                }
                break;
            }
            // The last read's verdict stands when the wait passes.
            c.orders_agreed = agreed_orders;
            c.positions_agreed = agreed_positions;
            c.last_mismatches = mismatches;
            tokio::time::sleep(POLL).await;
        }
        let outcome = if c.orders_agreed && c.positions_agreed {
            "agreed"
        } else {
            "last"
        };
        for (what, body) in [("orders", later_orders), ("positions", later_positions)] {
            if let Some(body) = body {
                self.rest_bodies
                    .push((format!("{what}-{}-{outcome}", slug(after)), body));
            }
        }
        c
    }

    /// The WebSocket order methods the run's sessions wrote so far, and the connections they
    /// opened ([`session_counts`] of every phase's link).
    fn session_totals(&self) -> (u64, u64) {
        let (ws, conns) = self
            .live
            .as_ref()
            .map_or((0, 0), |link| session_counts(link.borrow().notes()));
        (self.ended_counts.0 + ws, self.ended_counts.1 + conns)
    }
}

/// Whether a disconnect's cancel-on-disconnect was measured: the order `cancelled` by it, or
/// `still_listed` by the last GET /orders answer read after it, with no read failed
/// (`read_errors`) and nothing `filled`. No answer read is no "not cancelled".
pub fn disconnect_measured(
    cancelled: bool,
    still_listed: bool,
    read_errors: bool,
    filled: bool,
) -> bool {
    cancelled || (still_listed && !read_errors && !filled)
}

/// What one session's link `notes` say it sent, for the per-IP check: the WebSocket order
/// methods (every request of ours the session reported sent: the steps', the Stop cancels and
/// the Safety cancels at a reconnect; and each connection's own cancel-on-disconnect arm, the
/// session's request the link notes as not ours, counted once its answer came) and the
/// connections it opened (each epoch that authenticated, and each whose login was refused:
/// every one made a login, and every authenticated one its two resync reads).
pub fn session_counts(notes: &[Note]) -> (u64, u64) {
    let (mut ours, mut conns) = (0u64, 0u64);
    let mut arms = std::collections::BTreeSet::new();
    for note in notes {
        match note {
            Note::Submitted { sent: Ok(()), .. } => ours += 1,
            Note::Outcome {
                rpc, ours: false, ..
            } => {
                arms.insert(rpc.0);
            }
            Note::Conn(ConnState::Authenticated) | Note::LoginRefused(_) => conns += 1,
            _ => {}
        }
    }
    (ours + arms.len() as u64, conns)
}

/// What a phase's link `notes` hold that leaves the run's order truth incomplete: a problem
/// the link noted (a venue error after login, an asynchronous reject, an event or outcome the
/// registry could not apply), an order in our namespace the registry does not hold (an order
/// event's, or a resync's), a fill not of our orders, or a refused resync. Empty when none.
pub fn link_problems(notes: &[Note]) -> Vec<String> {
    let mut problems = Vec::new();
    for note in notes {
        match note {
            Note::Problem(what) => problems.push(format!("the link noted a problem: {what}")),
            Note::Orphan { cid, .. } => problems.push(format!(
                "an order event of client id sequence {} in our namespace, which the registry \
                 does not hold",
                cid.seq()
            )),
            Note::Resynced { report, .. } => {
                for (cid, _) in &report.untracked {
                    problems.push(format!(
                        "a resync showed client id sequence {} in our namespace, which the \
                         registry does not hold",
                        cid.seq()
                    ));
                }
            }
            Note::Fill {
                seen,
                unexplained: true,
            } => problems.push(format!("a fill not of our orders: {seen}")),
            Note::ResyncRefused(why) => problems.push(format!("a resync was refused: {why}")),
            _ => {}
        }
    }
    problems
}

/// Whether an order gone (`gone`) from GET /orders after a disconnect was cancelled by it: its
/// history lists it CLOSED with its quantities read and nothing filled. Unreadable quantities
/// are not "nothing filled".
pub fn disconnect_cancelled(gone: bool, history: &History) -> bool {
    gone && history.listed && history.filled == Some(false)
}

/// One order a comparison expects: its label, client id as sent, and its price and total
/// while it rests (`None` once it ended).
type Expected = (String, String, Option<(Option<Decimal>, Decimal)>);

/// What GET /orders' rows on the market show other than `expected`.
fn order_mismatches(expected: &[Expected], on_market: &[&Value]) -> Vec<String> {
    let mut mismatches = Vec::new();
    for (label, wire, want) in expected {
        let row = on_market
            .iter()
            .find(|o| o.get("client_id").and_then(Value::as_str) == Some(wire.as_str()));
        match (want, row) {
            (None, None) => {}
            (None, Some(_)) => {
                mismatches.push(format!("{label}: ended on the stream, still listed"))
            }
            (Some(_), None) => mismatches.push(format!("{label}: open on the stream, not listed")),
            (Some((px, qty)), Some(row)) => {
                let num = |k: &str| {
                    row.get(k)
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<Decimal>().ok())
                };
                if num("price") != *px || num("size") != Some(*qty) {
                    mismatches.push(format!(
                        "{label}: listed at {} x {}, the stream shows {} x {}",
                        row.get("price").and_then(Value::as_str).unwrap_or("-"),
                        row.get("size").and_then(Value::as_str).unwrap_or("-"),
                        px.map_or("-".to_owned(), |p| p.normalize().to_string()),
                        qty.normalize()
                    ));
                }
            }
        }
    }
    mismatches
}

/// Why GET /positions' row for the market (`None`: not listed) disagrees with `stream` (the
/// position the fills imply, signed), or `None` when it agrees: a signed size equal to the
/// stream's, LONG for a positive one and SHORT for a negative one, OPEN; and at size 0, not
/// listed or CLOSED (FBC-0sc's (b)).
pub fn position_mismatch(rest: Option<&RestPosition>, stream: Decimal) -> Option<String> {
    let Some(p) = rest else {
        return (!stream.is_zero())
            .then(|| format!("position: GET /positions lists none, the fills {stream}"));
    };
    let Ok(size) = p.size.parse::<Decimal>() else {
        return Some("position: GET /positions' size is not a decimal".to_owned());
    };
    if size != stream {
        return Some(format!(
            "position: GET /positions {size}, the fills {stream}"
        ));
    }
    let (side, status) = (p.side.as_str(), p.status.as_str());
    let shaped = if size.is_zero() {
        status == "CLOSED"
    } else {
        status == "OPEN"
            && side
                == if size > Decimal::ZERO {
                    "LONG"
                } else {
                    "SHORT"
                }
    };
    (!shaped).then(|| {
        format!("position: GET /positions shows size {size} as side {side:?} status {status:?}")
    })
}

/// The wall clock now, as testnet_trade reads it.
fn trade_wall_now() -> fbc_core::WallNs {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    fbc_core::WallNs(i64::try_from(ns).unwrap_or(i64::MAX))
}

/// The `results` of a Paradex list answer, or why the answer holds none: a 200 answer that is
/// not JSON, or has no `results` list, is no empty snapshot.
pub fn parse_results(body: &[u8]) -> Result<Vec<Value>, String> {
    let v: Value = serde_json::from_slice(body).map_err(|_| "the answer is not JSON".to_owned())?;
    v.get("results")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| "the answer has no results list".to_owned())
}

/// A list answer's top-level keys besides `results`.
fn top_keys(body: &[u8]) -> Vec<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| {
            v.as_object()
                .map(|m| m.keys().filter(|k| *k != "results").cloned().collect())
        })
        .unwrap_or_default()
}

/// `text` as a file name's part.
fn slug(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}
