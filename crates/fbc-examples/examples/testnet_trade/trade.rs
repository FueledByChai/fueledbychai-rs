//! testnet_trade's wiring and its one round trip: the testnet guard, the touch read over REST,
//! the order priced and sized under the caps, an fbc-oms registry declared an owner-assisted
//! testnet run (decision 0067), one Paradex order-entry session (fbc-runtime's `ExecSession`)
//! handing its events to the registry through [`Link`], and the driver that waits for each step
//! under the step timeout, places one post-only order, cancels it, and Stops.
//!
//! **Never a crossing order.** The order is a post-only limit (Paradex's `POST_ONLY`
//! instruction, which the venue cancels rather than let it take), priced `--away-bps` below the
//! best bid for a buy (above the best ask for a sell) read from `GET /orderbook` just before,
//! floored (ceiled) onto the tick, and refused unless it lies strictly behind the touch.
//!
//! **Its size.** The resting cap per side (`--resting-cap-usd`, required, no default) at the
//! order's price, floored onto the size step: the largest order the cap admits. Refused when
//! that is no lot, or its notional is below the market's `--min-notional` (the encoder does not
//! check a minimum, FBC-98fc). The inventory cap (`--inventory-cap-usd`, required) is converted
//! at the same price.
//!
//! **What is not built here.** No order query on the Unknown ladder yet (FBC-m8vm): an order
//! whose acknowledgement times out is `Unknown`, and Stop's cancel of every order of ours on the
//! market, by client id, is what ends it. No instrument discovery (FBC-l5o): the tick, size step
//! and minimum notional come from the command line.

use std::cell::RefCell;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::rc::Rc;
use std::time::{Duration, Instant};

use fbc_core::{
    AccountKey, AssetSym, Channel, CidMatch, CidMint, ClientOrderId, ConnState, FundingSpec,
    InstrumentId, InstrumentKind, InstrumentSpec, Lots, MarketLease, Namespace, NamespaceLease,
    NewOrder, NonceBlock, NonceScope, NonceSource, OrderKind, PriceGrid, RpcId, Secrets, Side,
    SignedLots, SizeStep, SpecTable, SubmitOutcome, Ticks, Tif, TradingStatus, UnderlyingId,
    VenueConfig, VenueFactory, VenueId, WallNs, dispatch_market_data,
};
use fbc_oms::{
    CancelChoice, CancelEverything, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, OrdState, PreTradeCaps, Registry, ResyncReport, ResyncSnapshot, Routed,
    TerminalKind, TestnetRun,
};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::{
    Connector, ExecControl, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, RateLimiter,
    ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, TIMEOUT};
use fbc_venue_paradex::factory::{EXEC_MODE, EXEC_URL, RPC_TIMEOUT};
use fbc_venue_paradex::md::rest::{OrderbookSnapshot, decode_orderbook, orderbook_path};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Notify;

use crate::args::{Options, OrderSide, TESTNET_CHAIN, Target, direct_to_stub, sole, testnet_guard};
use crate::link::{Link, LinkHandler, Note};

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The one market's instrument id, the account's number and the engine namespace the client
/// ids are minted under.
pub const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
const NS: Namespace = Namespace::new(1);

/// The venue's id as the lease files name it, and the account's name in them: a label, never
/// the account's address.
const VENUE: &str = "PARADEX";
const ACCOUNT_LABEL: &str = "testnet";

/// How long an order request awaits its reply before it is `Unknown`.
const RPC_WAIT: &str = "5000ms";

/// What a run did: whether every step happened, and how many places and cancels the session
/// reported sent.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub ok: bool,
    pub places_sent: usize,
    pub cancels_sent: usize,
}

/// Lines with the milliseconds since the start.
#[derive(Clone)]
struct Lines {
    out: Out,
    t0: Instant,
}

impl Lines {
    fn line(&self, text: std::fmt::Arguments<'_>) {
        // A line that cannot be written is lost; the run goes on to its Stop regardless.
        let _ = writeln!(self.out.borrow_mut(), "{text}");
    }

    fn step(&self, step: &str, text: std::fmt::Arguments<'_>) {
        let ms = self.t0.elapsed().as_millis();
        self.line(format_args!("STEP {step:<10} +{ms}ms  {text}"));
    }
}

/// Runs testnet_trade: the guard first, before anything connects; then the touch, the order,
/// the session and the driver. `chain` is `PARADEX_CHAIN_ID` when set; `creds` the account's
/// credentials, handed to the session's codec unread. An error is a refusal before the session
/// started; once it started the outcome is in the [`Report`].
pub async fn run(
    opts: &Options,
    chain: Option<&str>,
    creds: Secrets,
    out: Out,
) -> Result<Report, String> {
    let target = testnet_guard(&opts.rest_url, &opts.ws_url, chain)?;
    direct_to_stub(target, &opts.proxy)?;
    sole(opts)?;
    let lines = Lines {
        out,
        t0: Instant::now(),
    };
    let what = match target {
        Target::Testnet => "Paradex testnet",
        Target::LoopbackStub => "a loopback test stub, not Paradex",
    };
    lines.line(format_args!(
        "TESTNET {what}: REST {} WS {} chain {TESTNET_CHAIN}",
        opts.rest_url, opts.ws_url
    ));
    lines.line(format_args!(
        "OWNER-ASSISTED testnet run (decision 0067): Paradex's snapshot source is untrustworthy, \
         so the market's position is seeded by hand from the venue's REST position; never done \
         outside a declared testnet run"
    ));

    let factory = &ParadexFactory;
    let cfg = config(opts);
    let specs = specs(factory, &cfg, opts)?;
    let book = touch(opts, &specs).await?;
    let order = price(opts, &book)?;
    lines.line(format_args!(
        "BBO bid {} ask {} (GET /orderbook seq {})",
        order.bid, order.ask, book.seq_no
    ));
    lines.line(format_args!(
        "ORDER {} {} @ {} post-only, notional ${}, {} bps behind the touch; caps: resting {} \
         lots per side, inventory {} lots",
        match opts.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        },
        order.size,
        order.px,
        order.notional.round_dp(4),
        opts.away_bps,
        order.qty.get(),
        order.inventory.get(),
    ));

    // The registry: caps, the testnet-run declaration (decision 0067), then the lease names.
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
    let limits = MarketCapsConfig {
        inventory: Some(order.inventory),
        resting: Some(order.qty),
    };
    let caps = PreTradeCaps::new()
        .with_market(INST, limits)
        .map_err(|e| format!("caps: {e:?}"))?;
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps).with_market(INST, symbol.clone());
    let needs_account_lease = keys.nonce_scope() == NonceScope::PerAccountMonotonic;
    let reg = Registry::with_caps(caps)
        .for_testnet_run(TestnetRun::owner_assisted())
        .with_lease_keys(keys);

    // The leases: the client-id namespace, the market (and the account where nonces need it).
    fs::create_dir_all(&opts.lease_dir)
        .map_err(|e| format!("--lease-dir {}: {e}", opts.lease_dir.display()))?;
    let held = |e: &dyn std::fmt::Debug| {
        format!(
            "a lease in {} is held (another testnet_trade running?): {e:?}",
            opts.lease_dir.display()
        )
    };
    let ns_lease = NamespaceLease::acquire(&opts.lease_dir, ACCT, NS).map_err(|e| held(&e))?;
    let hwm = HighWater::at(&opts.lease_dir);
    // Read now, so a mark that cannot be read refuses the run before anything connects; the
    // mint is built after the resync, which also floors it (the highest of our ids it shows).
    let persisted = hwm.read()?;
    let market = MarketLease::acquire(&opts.lease_dir, VENUE, ACCOUNT_LABEL, &symbol)
        .map_err(|e| held(&e))?;
    let mut leases = Leases::market(market);
    if needs_account_lease {
        let acct = fbc_core::AccountLease::acquire(&opts.lease_dir, VENUE, ACCOUNT_LABEL)
            .map_err(|e| held(&e))?;
        leases = leases.with_account(acct);
    }

    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3_600),
            max_entries: 10_000,
        },
        wall_now(),
    )
    .map_err(|e| e.to_string())?;
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .map_err(|e| format!("{e:?}"))?;
    let link = Rc::new(RefCell::new(Link::new(reg, ledger, order_caps, ladder)));
    let wake = Rc::new(Notify::new());
    let handler = LinkHandler {
        link: Rc::clone(&link),
        wake: Rc::clone(&wake),
    };
    let session_cfg = session_config(factory, cfg, creds, specs, opts)?;
    let (mut session, control) =
        ExecSession::new(session_cfg, handler).map_err(|e| format!("session: {e}"))?;
    let driver = Driver {
        link: Rc::clone(&link),
        wake,
        orders: session.orders(),
        lines: lines.clone(),
        timeout: Duration::from_secs(opts.step_timeout_secs),
        hold: Duration::from_secs(opts.hold_secs),
        hwm,
        told: std::cell::Cell::new(0),
        start_position: None,
        owned: Vec::new(),
    };
    let order = Planned {
        side: match opts.side {
            OrderSide::Buy => Side::Buy,
            OrderSide::Sell => Side::Sell,
        },
        px: order.ticks,
        qty: order.qty,
    };
    let ran = drive_with(
        &mut session,
        driver.run(order, ns_lease, persisted, leases, control),
    )
    .await;
    let ok = ran.unwrap_or_else(|ended| {
        lines.line(format_args!(
            "NOTE the session ended before the run did: {ended}; nothing more can be sent, and \
             Paradex's cancel-on-disconnect cancels what the closed socket left resting"
        ));
        false
    });
    drop(session);
    let (places_sent, cancels_sent) = link.borrow().sent_counts();
    let report = Report {
        ok,
        places_sent,
        cancels_sent,
    };
    lines.line(format_args!(
        "DONE {} places sent {} cancels sent {}",
        if report.ok { "ok" } else { "failed" },
        report.places_sent,
        report.cancels_sent
    ));
    Ok(report)
}

/// Runs the session beside `driver` until the driver is done (it drops the session's control,
/// so the session then stops); the session's end first is an error.
async fn drive_with<H: fbc_runtime::ExecHandler>(
    session: &mut ExecSession<H>,
    driver: impl Future<Output = bool>,
) -> Result<bool, String> {
    let running = session.run();
    tokio::pin!(running);
    tokio::pin!(driver);
    tokio::select! {
        ended = &mut running => Err(match ended {
            Ok(()) => "stopped".to_owned(),
            Err(e) => e.to_string(),
        }),
        ok = &mut driver => {
            // The control is dropped: the session closes its socket and returns.
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut running).await;
            Ok(ok)
        }
    }
}

/// The wall clock now.
fn wall_now() -> WallNs {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    WallNs(i64::try_from(ns).unwrap_or(i64::MAX))
}

/// The Paradex configuration: order entry on the WebSocket, the REST base for the login and
/// the resync, the testnet chain id, and the timings.
fn config(opts: &Options) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (EXEC_URL, opts.ws_url.as_str()),
        (EXEC_MODE, "orders"),
        (RPC_TIMEOUT, RPC_WAIT),
        (REST_URL, opts.rest_url.as_str()),
        (CHAIN_ID, TESTNET_CHAIN),
        (SIGNATURE_LIFETIME, "600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// The one market's spec, from the command line: its tick, size step and minimum size of one
/// lot. The minimum notional is checked by [`price`], not here.
fn specs(venue: &dyn VenueFactory, cfg: &VenueConfig, opts: &Options) -> Result<SpecTable, String> {
    let caps = venue.caps(cfg).map_err(|e| e.to_string())?;
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(&opts.market))
        .map_err(|e| format!("--market {}: {e:?}", opts.market))?;
    let price_grid = PriceGrid::fixed(opts.tick).map_err(|e| format!("--tick: {e:?}"))?;
    let size_step = SizeStep::new(opts.step).ok_or("--step: not a valid size step")?;
    let usd = AssetSym::new("USD").expect("an asset symbol");
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        id: INST,
        venue: VenueId::new(1),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(INST.get()),
        kind: InstrumentKind::Perpetual,
        price_grid,
        quote_grid: None,
        size_step,
        min_size: Lots::new(1).expect("one lot"),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    });
    Ok(table)
}

/// The market's REST order book (`GET /orderbook`), read once before the order is priced.
async fn touch(opts: &Options, specs: &SpecTable) -> Result<OrderbookSnapshot, String> {
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
    let response = tokio::time::timeout(wait, connector.http(request, 1 << 20))
        .await
        .map_err(|_| "GET /orderbook: no answer in time".to_owned())?
        .map_err(|e| format!("GET /orderbook: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("GET /orderbook: status {}", response.status()));
    }
    decode_orderbook(response.body(), specs).map_err(|e| format!("GET /orderbook: {e:?}"))
}

/// The order as priced and sized, and the caps at its price.
struct Priced {
    bid: Decimal,
    ask: Decimal,
    ticks: Ticks,
    px: Decimal,
    qty: Lots,
    size: Decimal,
    notional: Decimal,
    inventory: Lots,
}

/// The order `--away-bps` behind the touch on the tick and the resting cap's size on the size
/// step, or why it cannot be placed: an empty side, a price that would not rest strictly behind
/// the touch, no lot within the cap, or a notional below the market's minimum.
fn price(opts: &Options, book: &OrderbookSnapshot) -> Result<Priced, String> {
    let (best_bid, best_ask) = match (book.bids.first(), book.asks.first()) {
        (Some(b), Some(a)) => (b.px, a.px),
        _ => return Err("GET /orderbook: a side is empty; no touch to stay away from".to_owned()),
    };
    let tick = opts.tick;
    let of = |t: Ticks| Decimal::from(t.0) * tick;
    let (bid, ask) = (of(best_bid), of(best_ask));
    let away = Decimal::from(opts.away_bps) / Decimal::from(10_000);
    let ticks = match opts.side {
        OrderSide::Buy => (bid * (Decimal::ONE - away) / tick).floor(),
        OrderSide::Sell => (ask * (Decimal::ONE + away) / tick).ceil(),
    };
    let ticks = Ticks(ticks.to_i64().ok_or("the price does not fit")?);
    // Never a crossing order: strictly behind the touch on its own side, so also behind the
    // other side's.
    let behind = match opts.side {
        OrderSide::Buy => ticks.0 > 0 && ticks < best_bid && ticks < best_ask,
        OrderSide::Sell => ticks > best_ask && ticks > best_bid,
    };
    if !behind {
        return Err(format!(
            "the order's price {} would not rest strictly behind the touch (bid {bid}, ask {ask}); \
             refused",
            of(ticks)
        ));
    }
    let px = of(ticks);
    let lots_for = |usd: Decimal| -> Result<Lots, String> {
        let n = (usd / (px * opts.step)).floor();
        Lots::new(n.to_i64().ok_or("a cap does not fit")?)
            .ok_or_else(|| "a negative cap".to_owned())
    };
    let resting_usd = opts.resting_cap_usd;
    let qty = lots_for(resting_usd)?;
    let inventory = lots_for(opts.inventory_cap_usd)?;
    let size = Decimal::from(qty.get()) * opts.step;
    let notional = size * px;
    if qty.get() == 0 {
        return Err(format!(
            "${resting_usd} at {px} is less than one size step ({}); refused",
            opts.step
        ));
    }
    if notional < opts.min_notional {
        return Err(format!(
            "the order's notional ${} (the ${resting_usd} resting cap at {px}, {size} on the \
             {} step) is below the market's minimum ${}; refused",
            notional.round_dp(4),
            opts.step,
            opts.min_notional
        ));
    }
    Ok(Priced {
        bid,
        ask,
        ticks,
        px,
        qty,
        size,
        notional,
        inventory,
    })
}

/// Nonces counted up from 1: Paradex asks for none, but the session reserves one per item and
/// per arm as the contract says.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).expect("a block of nonces");
        self.0 += u64::from(len);
        block
    }
}

/// The order-entry session's configuration: reconnects paced from 250 ms to 5 s, at most 5
/// attempts a minute, each given 10 s; normal traffic stopping at 90% of each declared limit; a
/// write a peer stopped reading ends its connection after 10 s.
fn session_config(
    venue: &'static ParadexFactory,
    cfg: VenueConfig,
    creds: Secrets,
    specs: SpecTable,
    opts: &Options,
) -> Result<ExecSessionConfig, String> {
    let caps = venue.caps(&cfg).map_err(|e| e.to_string())?;
    let limiter = RateLimiter::new(
        &caps.limits,
        SafetyReserve::percent(10).expect("a valid reserve"),
    )
    .map_err(|e| format!("{e:?}"))?;
    Ok(ExecSessionConfig {
        venue,
        cfg,
        creds,
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs,
        connector: Connector::new(opts.proxy.clone()),
        pacing: ReconnectPacing::new(
            Duration::from_millis(250),
            Duration::from_secs(5),
            5,
            Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .expect("valid pacing"),
        clock: IngestClock::new(),
        nonces: Box::new(Counting(1)),
        conn: 0,
        limiter,
        write_stall: WriteStall::new(Duration::from_secs(10)).expect("a non-zero window"),
        http_max_body: 1 << 20,
    })
}

/// The latest resync applied: once the session takes places, the one whose end opened the
/// current epoch's gate, never an earlier epoch's.
pub fn latest_resync(notes: &[Note]) -> Option<(&ResyncReport, &ResyncSnapshot)> {
    notes.iter().rev().find_map(|n| match n {
        Note::Resynced { report, snapshot } => Some((report, snapshot)),
        _ => None,
    })
}

/// The highest sequence of our namespace's client ids among the open orders `snapshot` shows, 0
/// when it shows none.
pub fn snapshot_max(snapshot: &ResyncSnapshot) -> u64 {
    snapshot
        .orders
        .iter()
        .filter_map(|o| match o.cid {
            Some(fbc_core::CidMatch::Ours(cid)) => Some(cid.seq()),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// The client-id mint's high-water mark, kept in a file of the lease directory across runs, so
/// a run whose wall clock stepped back never mints an id an earlier run used (the namespace
/// lease keeps two minters apart, not two runs).
pub struct HighWater {
    path: std::path::PathBuf,
}

impl HighWater {
    /// The mark kept in `dir`.
    pub fn at(dir: &std::path::Path) -> HighWater {
        let name = format!("cid-high-water-account-{}-ns-{}", ACCT.get(), NS.get());
        HighWater {
            path: dir.join(name),
        }
    }

    /// The mark kept, 0 when none was.
    pub fn read(&self) -> Result<u64, String> {
        match fs::read_to_string(&self.path) {
            Ok(text) => text
                .trim()
                .parse()
                .map_err(|_| format!("{}: not a high-water mark", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(format!("{}: {e}", self.path.display())),
        }
    }

    /// Keeps `mark`: written whole to a file beside it and synced, renamed over the mark, and
    /// the directory synced, so the mark is on the disk when this returns.
    pub fn write(&self, mark: u64) -> Result<(), String> {
        use std::io::Write as _;
        let tmp = self.path.with_extension("tmp");
        let durable = || -> std::io::Result<()> {
            // The new mark reaches the disk before it replaces the old, and the rename does
            // before the order goes out: a power loss leaves the old mark or the new, never none.
            let mut file = fs::File::create(&tmp)?;
            file.write_all(mark.to_string().as_bytes())?;
            file.sync_all()?;
            fs::rename(&tmp, &self.path)?;
            let dir = self.path.parent().unwrap_or(std::path::Path::new("."));
            fs::File::open(dir)?.sync_all()
        };
        durable().map_err(|e| {
            format!(
                "{}: cannot keep the client-id high-water mark: {e}",
                self.path.display()
            )
        })
    }
}

/// The order to place: its side, price and size.
struct Planned {
    side: Side,
    px: Ticks,
    qty: Lots,
}

/// Waits for each step and makes the next (module documentation).
struct Driver {
    link: Rc<RefCell<Link>>,
    wake: Rc<Notify>,
    orders: ExecOrders,
    lines: Lines,
    timeout: Duration,
    hold: Duration,
    /// Where the client-id high-water mark is kept.
    hwm: HighWater,
    /// How many of the link's notes were looked at for [`Driver::tell`].
    told: std::cell::Cell<usize>,
    /// The market's inventory once Start armed it: a round trip leaves it unchanged.
    start_position: Option<SignedLots>,
    /// Our orders on the market (the resync's and the one placed), each with what of it had
    /// filled when the run took it on: a fill of any of them during the run fails it.
    owned: Vec<(ClientOrderId, Lots)>,
}

impl Driver {
    /// Waits until `found` finds what it looks for in the link, or the step timeout passes.
    async fn wait<T>(&self, mut found: impl FnMut(&mut Link) -> Option<T>) -> Option<T> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            // Created before the check, so a wake between the check and the await is kept.
            let woken = self.wake.notified();
            self.tell();
            if let Some(t) = found(&mut self.link.borrow_mut()) {
                return Some(t);
            }
            tokio::select! {
                _ = woken => {}
                _ = tokio::time::sleep_until(deadline) => return None,
            }
        }
    }

    /// Prints a `NOTE` for each note not printed yet that the steps do not report themselves:
    /// a refusal or problem, a fill, an order event of an order not ours, a connection's end.
    fn tell(&self) {
        let link = self.link.borrow();
        let notes = &link.notes()[self.told.get()..];
        self.told.set(link.notes().len());
        for note in notes {
            match note {
                Note::Problem(what) => self.note(format_args!("{what}")),
                Note::Fill(routed) => self.note(format_args!("a fill: {routed}")),
                Note::Order(routed) if !matches!(routed, Routed::Ours(..)) => {
                    self.note(format_args!(
                        "an order event of an order not ours: {routed:?}"
                    ));
                }
                Note::EpochEnd(key) => self.note(format_args!(
                    "the order socket's connection {key:?} ended; the session reconnects and \
                     arms again"
                )),
                _ => {}
            }
        }
    }

    fn timed_out(&self, step: &str, what: &str) {
        let ms = self.lines.t0.elapsed().as_millis();
        self.lines.line(format_args!(
            "TIMEOUT {step} +{ms}ms  {what} did not happen within {} s; stopping",
            self.timeout.as_secs()
        ));
    }

    fn note(&self, text: std::fmt::Arguments<'_>) {
        self.lines.line(format_args!("NOTE {text}"));
    }

    /// What became of request `rpc` once every item is answered ([`Link::outcome_of`]).
    fn outcome_of(link: &Link, rpc: RpcId) -> Option<SubmitOutcome> {
        link.outcome_of(rpc)
    }

    /// The run: every step, then Stop whatever happened; true when every step happened, Stop
    /// cancelled what was left, and no fill moved the inventory.
    async fn run(
        mut self,
        order: Planned,
        ns_lease: NamespaceLease,
        persisted: u64,
        leases: Leases,
        control: ExecControl,
    ) -> bool {
        let ok = self.steps(order, ns_lease, persisted, leases).await;
        let stopped = self.stop().await;
        let unmoved = self.inventory_unmoved();
        let untraded = self.owned_untraded();
        drop(control);
        ok && stopped && unmoved && untraded
    }

    /// True unless an order of ours on the market filled during the run, the resync's
    /// included, even one that ended before Stop or whose fills left the inventory where it was.
    fn owned_untraded(&self) -> bool {
        let traded: Vec<(ClientOrderId, Lots)> = {
            let mut link = self.link.borrow_mut();
            let reg = link.reg();
            self.owned
                .iter()
                .filter_map(|(cid, before)| {
                    let now = reg.get(*cid).map_or(*before, |r| r.filled());
                    (now > *before).then_some((*cid, now))
                })
                .collect()
        };
        for (cid, filled) in &traded {
            self.note(format_args!(
                "an order of ours traded during the run: venue order {} has {} lots filled",
                self.vid_of(*cid),
                filled.get()
            ));
        }
        traded.is_empty()
    }

    /// True unless the market's inventory now differs from what it was at Start: a backstop
    /// for a fill the order checks did not see.
    fn inventory_unmoved(&self) -> bool {
        let Some(start) = self.start_position else {
            return true;
        };
        let now = self.link.borrow_mut().reg().inventory(INST);
        if now == start {
            return true;
        }
        self.note(format_args!(
            "the inventory moved from {} to {} lots during the run: a fill happened",
            start.0, now.0
        ));
        false
    }

    /// Login, arm, resync, seed, Start, place, ack, cancel, cancel ack, closed: false at the
    /// first step that does not happen.
    async fn steps(
        &mut self,
        order: Planned,
        ns_lease: NamespaceLease,
        persisted: u64,
        leases: Leases,
    ) -> bool {
        let authenticated = self
            .wait(|l| {
                l.notes()
                    .iter()
                    .any(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
                    .then_some(())
            })
            .await;
        if authenticated.is_none() {
            self.timed_out("login", "the login and the socket's authentication");
            return false;
        }
        self.lines.step(
            "login",
            format_args!("logged in over REST; the order socket is authenticated"),
        );

        let arm = self
            .wait(|l| {
                l.notes().iter().find_map(|n| match n {
                    Note::Outcome {
                        outcome: outcome @ SubmitOutcome::Accepted { .. },
                        ours: false,
                        ..
                    } => Some(outcome.clone()),
                    _ => None,
                })
            })
            .await;
        let Some(arm) = arm else {
            self.timed_out("arm", "Paradex's acceptance of cancel-on-disconnect");
            return false;
        };
        self.lines.step(
            "arm",
            format_args!(
                "cancel-on-disconnect accepted ({arm:?}): a dropped socket cancels our orders"
            ),
        );

        let orders = self.orders.clone();
        let resynced = self
            .wait(|l| {
                if !orders.may_place() {
                    return None;
                }
                latest_resync(l.notes()).map(|(report, snapshot)| {
                    (
                        snapshot_max(snapshot),
                        report.untrustworthy,
                        snapshot.orders.iter().filter(|o| o.inst == INST).count(),
                        snapshot.orders.len(),
                        // Orders on the market the registry cannot own: another namespace's,
                        // a non-canonical client id's, or one with none.
                        snapshot
                            .orders
                            .iter()
                            .filter(|o| o.inst == INST && !matches!(o.cid, Some(CidMatch::Ours(_))))
                            .count(),
                        snapshot
                            .orders
                            .iter()
                            .filter(|o| o.inst == INST)
                            .filter_map(|o| match o.cid {
                                Some(CidMatch::Ours(cid)) => Some(cid),
                                _ => None,
                            })
                            .collect::<Vec<ClientOrderId>>(),
                        snapshot
                            .positions
                            .iter()
                            .find(|(inst, _)| *inst == INST)
                            .map_or(SignedLots(0), |(_, qty)| *qty),
                    )
                })
            })
            .await;
        let Some((ours_max, untrustworthy, on_market, open, not_ours, ours, position)) = resynced
        else {
            self.timed_out(
                "resync",
                "the REST resync (an open order or position on a market other than --market \
                 refuses it whole)",
            );
            return false;
        };
        self.lines.step(
            "resync",
            format_args!(
                "{open} open orders ({on_market} on the market), position {} lots; the snapshot \
                 is {}",
                position.0,
                if untrustworthy {
                    "untrustworthy, so it seeded nothing"
                } else {
                    "trustworthy"
                }
            ),
        );

        {
            let mut link = self.link.borrow_mut();
            let reg = link.reg();
            self.owned = ours
                .into_iter()
                .map(|cid| (cid, reg.get(cid).map_or(Lots::ZERO, |r| r.filled())))
                .collect();
        }
        if not_ours > 0 {
            // The registry neither cancels nor counts the fills of an order it cannot own: one
            // could fill during the run and stay open after Stop, so the run is refused before
            // Start.
            self.note(format_args!(
                "{not_ours} open orders on the market are not ours (another namespace's, a \
                 non-canonical client id's, or one with none): Stop could not cancel them nor \
                 the run count their fills, so it is refused before Start; cancel them first"
            ));
            return false;
        }

        {
            let mut link = self.link.borrow_mut();
            let reg = link.reg();
            if !reg.position_known(INST) {
                if let Err(e) = reg.seed_position(INST, position) {
                    drop(link);
                    self.note(format_args!("the hand seed was refused: {e:?}"));
                    return false;
                }
                self.lines.line(format_args!(
                    "SEED position {} lots seeded by hand from the resync's REST position: an \
                     owner-assisted testnet run (decision 0067)",
                    position.0
                ));
            }
            match reg.start(INST, leases) {
                Ok(entry) => {
                    self.lines.step(
                        "start",
                        format_args!(
                            "armed: {:?}, generation {}",
                            entry.state(),
                            entry.generation().get()
                        ),
                    );
                    self.start_position = Some(reg.inventory(INST));
                }
                Err(refusal) => {
                    drop(link);
                    self.note(format_args!("Start was refused: {refusal:?}"));
                    return false;
                }
            }
        }

        // The place: built and authorized by fbc-oms, then handed to the session.
        // The mint, floored by the mark kept from earlier runs, the highest of our ids the
        // resync shows, and the wall clock.
        let mut mint = CidMint::new(ns_lease, persisted, ours_max, wall_now());
        let cid = match mint.mint() {
            Ok(cid) => cid,
            Err(e) => {
                self.note(format_args!("no client id: {e:?}"));
                return false;
            }
        };
        self.owned.push((cid, Lots::ZERO));
        // Kept before the id goes out, so no later run mints it again.
        if let Err(e) = self.hwm.write(mint.high_water()) {
            self.note(format_args!("{e}; nothing placed"));
            return false;
        }
        let placed = {
            let mut link = self.link.borrow_mut();
            let new = NewOrder {
                cid,
                inst: INST,
                side: order.side,
                kind: OrderKind::Limit { px: order.px },
                qty: order.qty,
                tif: Tif::Gtc,
                channel: Channel::Public,
                post_only: true,
                reduce_only: false,
                reducing: false,
            };
            let built = link.reg().place(new).map_err(|e| format!("{e:?}"));
            let auth = built.and_then(|cmd| {
                link.reg()
                    .authorize(ACCT, cmd)
                    .map_err(|e| format!("{e:?}"))
            });
            auth.and_then(|auth| link.submit(&self.orders, auth))
        };
        let place = match placed {
            Ok(rpc) => rpc,
            Err(e) => {
                self.note(format_args!("the place was refused: {e}"));
                return false;
            }
        };
        self.lines.step(
            "place",
            format_args!(
                "request {}: one post-only limit order handed to the session",
                place.0
            ),
        );

        let ack = self.wait(|l| Driver::outcome_of(l, place)).await;
        match ack {
            Some(SubmitOutcome::Accepted { ack }) => {
                let vid = self.vid_of(cid);
                self.lines.step(
                    "ack",
                    format_args!("accepted ({ack:?}), venue order id {vid}"),
                );
            }
            Some(other) => {
                self.note(format_args!("the place came back {other:?}"));
                if other == SubmitOutcome::Unknown {
                    self.unknown_note();
                }
                return false;
            }
            None => {
                self.timed_out("ack", "the place's acknowledgement");
                self.unknown_note();
                return false;
            }
        }

        if !self.hold.is_zero() {
            tokio::time::sleep(self.hold).await;
        }

        let cancelled = {
            let mut link = self.link.borrow_mut();
            let caps = link.caps().clone();
            let choice = link
                .reg()
                .cancellable(cid)
                .map(|permit| permit.cancel(&caps));
            match choice {
                Ok(CancelChoice::Send(cmd)) => {
                    let auth = link
                        .reg()
                        .authorize(ACCT, cmd)
                        .map_err(|e| format!("{e:?}"));
                    auth.and_then(|auth| link.submit(&self.orders, auth))
                }
                Ok(CancelChoice::AwaitAck) => {
                    Err("the cancel waits for an acknowledgement".to_owned())
                }
                Err(refusal) => Err(format!("{refusal:?}")),
            }
        };
        let cancel = match cancelled {
            Ok(rpc) => rpc,
            Err(e) => {
                // The order ended meanwhile (a post-only order the venue would not rest), or
                // cannot be cancelled now: Stop's cancel everything sees what is left.
                self.note(format_args!("no cancel was built: {e}"));
                return false;
            }
        };
        self.lines.step(
            "cancel",
            format_args!("request {}: the cancel handed to the session", cancel.0),
        );

        match self.wait(|l| Driver::outcome_of(l, cancel)).await {
            Some(SubmitOutcome::Accepted { ack }) => self.lines.step(
                "cancel-ack",
                format_args!("accepted ({ack:?}): queued for cancellation"),
            ),
            Some(other) => {
                self.note(format_args!("the cancel came back {other:?}"));
                return false;
            }
            None => {
                self.timed_out("cancel-ack", "the cancel's acknowledgement");
                return false;
            }
        }

        let closed = self
            .wait(
                |l| match l.reg().get(cid).map(|r| (r.state(), r.filled())) {
                    Some((OrdState::Terminal(kind), filled)) => Some((kind, filled)),
                    _ => None,
                },
            )
            .await;
        match closed {
            Some((kind @ TerminalKind::Canceled(_), Lots::ZERO)) => {
                self.lines.step(
                    "closed",
                    format_args!("the order event reports it ended: {kind:?}"),
                );
                true
            }
            Some((TerminalKind::Canceled(_), filled)) => {
                // Cancelled after a maker fill: a trade happened and left a position, so the
                // round trip did not.
                let position = self.link.borrow_mut().reg().inventory(INST).0;
                self.note(format_args!(
                    "the order was cancelled after {} lots of it filled; inventory now \
                     {position} lots",
                    filled.get()
                ));
                false
            }
            Some((kind, _)) => {
                // A fill (or a reject or expiry) is not the round trip asked for: a filled
                // order leaves a position, so the run fails.
                let position = self.link.borrow_mut().reg().inventory(INST).0;
                self.note(format_args!(
                    "the order ended without being cancelled: {kind:?}; inventory now {position} \
                     lots"
                ));
                false
            }
            None => {
                self.timed_out("closed", "the order event reporting the order ended");
                self.note(format_args!(
                    "Stop's cancel of every order of ours on the market names it again"
                ));
                false
            }
        }
    }

    /// The venue order id of `cid`, or `none`.
    fn vid_of(&self, cid: ClientOrderId) -> String {
        let mut link = self.link.borrow_mut();
        let vid = link
            .reg()
            .get(cid)
            .and_then(|r| r.vid().map(|v| v.as_str().to_owned()));
        vid.unwrap_or_else(|| "none".to_owned())
    }

    fn unknown_note(&self) {
        self.note(format_args!(
            "the order's fate is not known: no order query is built for the Unknown ladder yet \
             (FBC-m8vm), so Stop cancels it once Paradex acknowledges it, and otherwise the \
             socket's close leaves it to Paradex's cancel-on-disconnect"
        ));
    }

    /// Stop: the kill switch, then the cancel of everything of ours on the market (an
    /// instrument cancel-all only under 0005's I7 guard, which Paradex's untrustworthy resync
    /// never meets, so explicit cancels). Paradex takes no cancel by client id before the
    /// venue acknowledged the order, so an unacknowledged order's cancel waits for that
    /// acknowledgement, built the moment it lands ([`Registry::cancels_due`]), for at most the
    /// step timeout; one still waiting then is left to cancel-on-disconnect, which the socket's
    /// close sets off. Then the market is disarmed and its leases released. True when every
    /// order was cancelled or ended and every cancel sent was accepted.
    async fn stop(&mut self) -> bool {
        let mut refusals = Vec::new();
        let (cmds, waiting, how) = {
            let mut link = self.link.borrow_mut();
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
                    let how = "an instrument cancel-all".to_owned();
                    (cmds, unanswered.awaiting_ack, how)
                }
                CancelEverything::Explicit { plan, why } => {
                    for (cid, refusal) in &plan.refused {
                        refusals.push(format!("{cid:?}: {refusal:?}"));
                    }
                    let how =
                        format!("explicit cancels of our orders, no instrument cancel-all: {why}");
                    (plan.commands, plan.awaiting_ack, how)
                }
            }
        };
        for refused in refusals {
            self.note(format_args!("no Stop cancel for {refused}"));
        }
        let mut rpcs = Vec::new();
        for cmd in cmds {
            self.submit_stop_cancel(cmd, &mut rpcs);
        }
        let mut waiting = waiting;
        // Orders that ended while their cancel waited for an acknowledgement: no cancel was
        // sent for them, so how they ended is checked with the cancelled ones.
        let mut ended_waiting = Vec::new();
        if !waiting.is_empty() {
            self.note(format_args!(
                "{} orders not yet acknowledged: their cancels wait for the acknowledgement \
                 (Paradex takes no cancel by client id before it), at most {} s",
                waiting.len(),
                self.timeout.as_secs()
            ));
        }
        while !waiting.is_empty() {
            let due = self
                .wait(|l| {
                    let caps = l.caps().clone();
                    let reg = l.reg();
                    let ended: Vec<ClientOrderId> = waiting
                        .iter()
                        .copied()
                        .filter(|cid| reg.get(*cid).is_none_or(|r| r.state().is_terminal()))
                        .collect();
                    let due: Vec<ClientOrderId> = reg
                        .cancels_due(&caps)
                        .into_iter()
                        .filter(|cid| waiting.contains(cid))
                        .collect();
                    (!ended.is_empty() || !due.is_empty()).then_some((ended, due))
                })
                .await;
            let Some((ended, due)) = due else {
                self.note(format_args!(
                    "{} orders still unacknowledged: the socket's close leaves Paradex's \
                     cancel-on-disconnect to cancel them",
                    waiting.len()
                ));
                break;
            };
            waiting.retain(|cid| !ended.contains(cid) && !due.contains(cid));
            ended_waiting.extend(ended);
            for cid in due {
                let built = {
                    let mut link = self.link.borrow_mut();
                    let caps = link.caps().clone();
                    link.reg().cancellable(cid).map(|p| p.cancel(&caps))
                };
                match built {
                    Ok(CancelChoice::Send(cmd)) => self.submit_stop_cancel(cmd, &mut rpcs),
                    Ok(CancelChoice::AwaitAck) => waiting.push(cid),
                    Err(refusal) => self.note(format_args!("no Stop cancel: {refusal:?}")),
                }
            }
        }
        let mut accepted = waiting.is_empty();
        for rpc in &rpcs {
            match self.wait(|l| Driver::outcome_of(l, *rpc)).await {
                Some(SubmitOutcome::Accepted { .. }) => {}
                Some(other) => {
                    accepted = false;
                    self.note(format_args!("Stop's cancel {} came back {other:?}", rpc.0));
                }
                None => {
                    accepted = false;
                    self.timed_out("stop", "a Stop cancel's acknowledgement");
                }
            }
        }
        // A queued cancel is not a done one: every order a Stop cancel named must be reported
        // cancelled, with nothing of it filled, before Stop counts as done; a fill racing the
        // cancel leaves a position.
        let named: Vec<ClientOrderId> = {
            let link = self.link.borrow();
            rpcs.iter()
                .flat_map(|rpc| link.cids_of(*rpc))
                .chain(ended_waiting)
                .collect()
        };
        if !named.is_empty() {
            let ended = self
                .wait(|l| {
                    let reg = l.reg();
                    named
                        .iter()
                        .all(|cid| reg.get(*cid).is_none_or(|r| r.state().is_terminal()))
                        .then_some(())
                })
                .await;
            if ended.is_none() {
                accepted = false;
                self.timed_out(
                    "stop",
                    "the order events reporting Stop's cancelled orders ended",
                );
            } else {
                let ends: Vec<(TerminalKind, Lots)> = {
                    let mut link = self.link.borrow_mut();
                    let reg = link.reg();
                    named
                        .iter()
                        .filter_map(|cid| reg.get(*cid))
                        .filter_map(|r| match r.state() {
                            OrdState::Terminal(kind) => Some((kind, r.filled())),
                            _ => None,
                        })
                        .collect()
                };
                for end in ends {
                    match end {
                        (TerminalKind::Canceled(_), Lots::ZERO) => {}
                        (TerminalKind::Canceled(_), filled) => {
                            accepted = false;
                            self.note(format_args!(
                                "a Stop cancel's order was cancelled after {} lots of it filled",
                                filled.get()
                            ));
                        }
                        (kind, _) => {
                            accepted = false;
                            self.note(format_args!(
                                "a Stop cancel's order ended without being cancelled: {kind:?}"
                            ));
                        }
                    }
                }
            }
        }
        self.link.borrow_mut().reg().disarm(INST);
        self.lines.step(
            "stop",
            format_args!(
                "kill switch on; cancel all: {} cancels sent ({how}); market disarmed, leases \
                 released, session closing",
                rpcs.len()
            ),
        );
        accepted
    }

    /// Authorizes and submits one of Stop's cancels, adding its request to `rpcs`.
    fn submit_stop_cancel(&mut self, cmd: fbc_oms::PermittedCommand, rpcs: &mut Vec<RpcId>) {
        let submitted = {
            let mut link = self.link.borrow_mut();
            let auth = link
                .reg()
                .authorize(ACCT, cmd)
                .map_err(|e| format!("{e:?}"));
            auth.and_then(|auth| link.submit(&self.orders, auth))
        };
        match submitted {
            Ok(rpc) => rpcs.push(rpc),
            Err(e) => self.note(format_args!("a Stop cancel was not submitted: {e}")),
        }
    }
}
