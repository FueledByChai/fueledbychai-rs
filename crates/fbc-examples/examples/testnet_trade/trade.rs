//! testnet_trade's wiring and its one round trip: the testnet guard, the touch read over REST,
//! the order priced and sized under the caps, an fbc-oms registry declared an owner-assisted
//! testnet run (decision 0067), one Paradex order-entry session (fbc-runtime's `ExecSession`)
//! handing its events to the registry through [`Link`], and the driver that waits for each step
//! under the step timeout, places one post-only order, cancels it, and Stops.
//!
//! **Never a crossing order.** The order is a post-only limit (Paradex's `POST_ONLY`
//! instruction, which the venue cancels rather than let it take), priced `--away-bps` below the
//! best bid for a buy (above the best ask for a sell) read from `GET /orderbook` before the
//! session starts, floored (ceiled) onto the tick, and refused unless it lies strictly behind
//! the touch. Only the side the order rests behind is needed: a buy on a book with no asks (a
//! testnet market nobody sells on) rests behind the bid all the same. The touch is read again
//! once the market is Started, just before the place: the order goes out only if it is still
//! at least `--away-bps` behind it, and the inventory cap at the fresh touch is still no fewer
//! lots than the caps hold.
//!
//! **Its size.** `--order-usd` (required, no default, at most the resting cap) at the order's
//! price, floored onto the size step. Refused when that is no lot, or its notional is below the
//! market's `--min-notional` (the encoder does not check a minimum, FBC-98fc). The resting cap
//! per side (`--resting-cap-usd`) is converted to lots at the same price, and the inventory cap
//! (`--inventory-cap-usd`), both required, at the highest of that price, the bid and the ask
//! (those that rest), so a position is never worth more than the cap at the market.
//!
//! **Orders of ours from an earlier connection or run** (decision 0080). This connection's
//! cancel-on-disconnect may not cover an order of ours its resync shows resting, so the session
//! tells it unprotected and takes no place on its market until it ends, and the link cancels it
//! at once, a Safety cancel fbc-oms builds and authorizes ([`Link`]). An earlier run's: the run
//! places nothing beside it, waits for it to end (Stop cancels it when it does not, within the
//! step timeout), and says why; a run after that places. The run's own, after a reconnect: its
//! Safety cancel ends the hold and is the round trip's cancel. The registry sums resting lots
//! on a side, so one still resting on the order's side at the place (perhaps at a price nearer
//! the touch) would count at the new order's price, not its own: nothing is placed then
//! either.
//!
//! **Its client ids** are minted, decoded as ours, leased and kept under the namespace the
//! consumer allocates (`--namespace`, required): an order on the market under another
//! namespace's id refuses the run before Start.
//!
//! **A refused login** stops the run at once once [`LOGIN_REFUSALS`] logins in a row were
//! refused, naming the last refusal (its HTTP status, Paradex's code and message, as the codec
//! reports it) and the usual causes, instead of reconnecting until the step timeout; each
//! connection that ends says why, as far as the session reports it ([`Note::EpochEnd`]).
//!
//! **Its output** goes through [`detached`] when run: a thread of its own writes it, so a stalled
//! standard output never blocks the one-thread runtime the session, the timeouts and the
//! cancels run on.
//!
//! **The position check** (FBC-hpg6, the owner's condition in decision 0094: positions follow
//! fills). After the first fill the session sees, the run reads `GET /positions` and compares the
//! market's size with the registry's position, exactly in lots ([`PositionCheck`]); a mismatch is
//! read once more after the step timeout, both readings printed, and one the re-read clears lets
//! the run go on, while one that persists Stops it at once (no step waits on, so nothing is
//! placed after) and fails it, saying to check the position by hand. At Stop, once Stop's cancels
//! are answered, the comparison runs again on every run, fills or not; a mismatch that persists
//! there fails the run with both readings.
//!
//! **What is not built here.** No order query on the Unknown ladder yet (FBC-m8vm): an order
//! whose acknowledgement times out is `Unknown`, and Stop's cancel of every order of ours on the
//! market, by client id, is what ends it. No instrument discovery (FBC-l5o): the tick, size step
//! and minimum notional come from the command line.

use std::cell::{Cell, RefCell};
use std::fs;
use std::rc::Rc;
use std::time::{Duration, Instant};

use fbc_core::{
    Channel, CidMatch, CidMint, ClientOrderId, ConnState, Lots, MarketLease, Namespace,
    NamespaceLease, NewOrder, NonceScope, OrderKind, RpcId, Secrets, Side, SignedLots, SpecTable,
    SubmitOutcome, Ticks, Tif, VenueConfig, VenueFactory,
};
use fbc_oms::{
    CancelChoice, CancelEverything, FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig,
    MarketCapsConfig, OrdState, PreTradeCaps, Registry, ResyncSnapshot, Routed, TerminalKind,
    TestnetRun,
};
use fbc_runtime::{Connector, ExecControl, ExecOrders, ExecSession};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::md::rest::OrderbookSnapshot;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Notify;

use crate::args::{
    Options, OrderSide, TESTNET_CHAIN, Target, direct_to_stub, hosts, sole, testnet_guard,
};
use crate::link::{Link, LinkHandler, Note, async_reject};
use crate::positions::{PositionCheck, fill_seen, fills_in};
use crate::testnet::{
    ACCOUNT_LABEL, ACCT, INST, LOGIN_REFUSALS, Lines, REFUSAL_CAUSES, VENUE, drive_with,
    px_or_none, wall_now,
};
use crate::testnet::{
    HighWater, Out, cap_lots, inventory_price, latest_resync, off_thread_holding,
    resync_disagreements, snapshot_max,
};

/// The Paradex configuration for `opts`' URLs ([`crate::testnet::config`]).
pub fn config(opts: &Options) -> VenueConfig {
    crate::testnet::config(&opts.rest_url, &opts.ws_url)
}

/// The one market's spec from `opts` ([`crate::testnet::specs`]).
pub fn specs(
    venue: &dyn VenueFactory,
    cfg: &VenueConfig,
    opts: &Options,
) -> Result<SpecTable, String> {
    crate::testnet::specs(venue, cfg, &opts.market, opts.tick, opts.step)
}

/// The market's REST order book under `opts` ([`crate::testnet::touch`]).
async fn touch(opts: &Options, specs: &SpecTable) -> Result<OrderbookSnapshot, String> {
    let wait = Duration::from_secs(opts.step_timeout_secs);
    crate::testnet::touch(&opts.rest_url, &opts.market, &opts.proxy, wait, specs).await
}

/// The order-entry session's configuration under `opts` ([`crate::testnet::session_config`]).
fn session_config(
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    creds: Secrets,
    specs: SpecTable,
    opts: &Options,
) -> Result<fbc_runtime::ExecSessionConfig, String> {
    crate::testnet::session_config(venue, cfg, creds, specs, &opts.proxy, opts.namespace)
}

/// What a run did: whether every step happened, and how many places and cancels the session
/// reported sent.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub ok: bool,
    pub places_sent: usize,
    pub cancels_sent: usize,
}

/// Where a round trip trades, and how its market's position comes to be known.
pub struct Network {
    /// The venue (Paradex; under test, a stand-in that declares other caps).
    pub factory: &'static dyn VenueFactory,
    /// The account's name in the lease files: a label, never the address.
    pub label: &'static str,
    /// The program, as a held lease's refusal names it.
    pub program: &'static str,
    /// True: an owner-assisted testnet run, the market seeded by hand from the resync's REST
    /// position (decision 0067). False: never seeded by hand; Start only once a trustworthy
    /// resync seeded the market, and only flat (`mainnet_smoke`).
    pub owner_assisted: bool,
    /// The market's mark price, read before the run: Paradex judges an order's minimum notional
    /// at it (the owner's 2026-10-16 testnet run), so the order must reach the minimum there,
    /// and the inventory cap is converted at no less than it. `None`: not read (testnet_trade).
    pub mark: Option<Decimal>,
    /// The usual causes of a refused login, printed with the refusal.
    pub refusal_causes: &'static str,
}

/// Runs testnet_trade: the guard first, before anything connects; then the touch, the order,
/// the session and the driver. `chain` is `PARADEX_CHAIN_ID` when set; `creds` the account's
/// credentials, twice: the session's, handed to its codec unread, and the position check's own
/// login's, each moved on unread (no type here holds them). An error is a refusal before the session started; once it started the
/// outcome is in the [`Report`].
pub async fn run(
    opts: &Options,
    chain: Option<&str>,
    creds: (Secrets, Secrets),
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
    // The hosts only: an accepted URL's path may still carry a token.
    let (rest_host, ws_host) = hosts(&opts.rest_url, &opts.ws_url)?;
    lines.line(format_args!(
        "TESTNET {what}: REST host {rest_host} WS host {ws_host} chain {TESTNET_CHAIN}"
    ));
    lines.line(format_args!(
        "OWNER-ASSISTED testnet run (decision 0067): Paradex's snapshot source is trustworthy \
         (decision 0094), so the resync seeds the market's position; a market it left unknown \
         is seeded by hand from the venue's REST position, never outside a declared testnet run"
    ));
    let net = Network {
        factory: &ParadexFactory,
        label: ACCOUNT_LABEL,
        program: "testnet_trade",
        owner_assisted: true,
        mark: None,
        refusal_causes: REFUSAL_CAUSES,
    };
    run_on(opts, &net, config(opts), creds, lines).await
}

/// The round trip on `net` under the Paradex configuration `cfg` (its login signing for
/// `net.chain`), once its guard passed and its first lines are printed: the touch, the order,
/// the session and the driver ([`run`] for its other arguments).
pub async fn run_on(
    opts: &Options,
    net: &Network,
    cfg: VenueConfig,
    creds: (Secrets, Secrets),
    lines: Lines,
) -> Result<Report, String> {
    sole(opts)?;
    let factory = net.factory;
    let specs = specs(factory, &cfg, opts)?;
    let book = touch(opts, &specs).await?;
    let order = at_mark(opts, price(opts, &book)?, net.mark)?;
    lines.line(format_args!(
        "BBO bid {} ask {} (GET /orderbook seq {})",
        px_or_none(order.bid),
        px_or_none(order.ask),
        book.seq_no
    ));
    lines.line(format_args!(
        "ORDER {} {} @ {} post-only, notional ${}, {} bps behind the touch, {} lots; caps: \
         resting {} lots per side, inventory {} lots",
        match opts.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        },
        order.size,
        order.px,
        order.notional.round_dp(4),
        opts.away_bps,
        order.qty.get(),
        order.resting.get(),
        order.inventory.get(),
    ));
    if let (Some(mark), Some(at_mark)) = (net.mark, order.mark_notional) {
        lines.line(format_args!(
            "ORDER notional ${} at the mark {mark}, where Paradex judges the minimum ${}",
            at_mark.round_dp(4),
            opts.min_notional
        ));
    }

    // The registry: caps, the testnet-run declaration (decision 0067) when the run is one, then
    // the lease names.
    let venue_caps = factory.caps(&cfg).map_err(|e| e.to_string())?;
    let order_caps = venue_caps
        .exec
        .as_ref()
        .ok_or("Paradex declares no order entry")?
        .order
        .clone();
    let symbol = specs
        .get(INST)
        .expect("the one market")
        .venue_symbol
        .clone();
    let limits = MarketCapsConfig {
        inventory: Some(order.inventory),
        resting: Some(order.resting),
    };
    let caps = PreTradeCaps::new()
        .with_market(INST, limits)
        .map_err(|e| format!("caps: {e:?}"))?;
    let keys = LeaseKeys::new(VENUE, net.label, &order_caps).with_market(INST, symbol.clone());
    let needs_account_lease = keys.nonce_scope() == NonceScope::PerAccountMonotonic;
    let reg = Registry::with_caps(caps);
    let reg = if net.owner_assisted {
        reg.for_testnet_run(TestnetRun::owner_assisted())
    } else {
        reg
    };
    let reg = reg.with_lease_keys(keys);

    // The leases: the client-id namespace, the market (and the account where nonces need it).
    fs::create_dir_all(&opts.lease_dir)
        .map_err(|e| format!("--lease-dir {}: {e}", opts.lease_dir.display()))?;
    let held = |e: &dyn std::fmt::Debug| {
        format!(
            "a lease in {} is held (another {} running?): {e:?}",
            opts.lease_dir.display(),
            net.program
        )
    };
    let ns = Namespace::new(opts.namespace);
    let ns_lease = NamespaceLease::acquire(&opts.lease_dir, ACCT, ns).map_err(|e| held(&e))?;
    let hwm = HighWater::at(&opts.lease_dir, ns);
    // Read now, so a mark that cannot be read refuses the run before anything connects; the
    // mint is built after the resync, which also floors it (the highest of our ids it shows).
    let persisted = hwm.read()?;
    let market =
        MarketLease::acquire(&opts.lease_dir, VENUE, net.label, &symbol).map_err(|e| held(&e))?;
    let mut leases = Leases::market(market);
    if needs_account_lease {
        let acct = fbc_core::AccountLease::acquire(&opts.lease_dir, VENUE, net.label)
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
        crate::testnet::SNAPSHOT_SETTLE,
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
    let recheck = Recheck {
        opts: opts.clone(),
        specs: specs.clone(),
        first_seq: book.seq_no,
        inventory: order.inventory,
        mark: net.mark,
    };
    let timeout = Duration::from_secs(opts.step_timeout_secs);
    let (session_creds, position_creds) = creds;
    let session_cfg = session_config(factory, cfg.clone(), session_creds, specs.clone(), opts)?;
    // The position check reads over REST through the limiter the session charges, so its reads
    // and the session's traffic share one count (FBC-hpg6).
    let positions = PositionCheck::new(
        &cfg,
        position_creds,
        Connector::new(opts.proxy.clone()),
        session_cfg.limiter.clone(),
        &opts.rest_url,
        specs,
        venue_caps,
        timeout,
    )?;
    let (mut session, control) =
        ExecSession::new(session_cfg, handler).map_err(|e| format!("session: {e}"))?;
    // Where the link's Safety cancels of unprotected orders go (decision 0080).
    link.borrow_mut().attach(session.orders(), ACCT);
    let driver = Driver {
        link: Rc::clone(&link),
        wake,
        orders: session.orders(),
        lines: lines.clone(),
        timeout,
        hold: Duration::from_secs(opts.hold_secs),
        hwm,
        told: std::cell::Cell::new(0),
        start_position: None,
        owned: Vec::new(),
        recheck,
        mint: None,
        owner_assisted: net.owner_assisted,
        refusal_causes: net.refusal_causes,
        positions: Cell::new(Some(positions)),
        fill_checked: Cell::new(false),
        halt: RefCell::new(None),
        stopping: Cell::new(false),
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
    let mut ok = ran.unwrap_or_else(|ended| {
        lines.line(format_args!(
            "NOTE the session ended before the run did: {ended}; nothing more can be sent, and \
             Paradex's cancel-on-disconnect cancels what the closed socket left resting"
        ));
        false
    });
    // A frame or HTTP answer the codec could not decode may have held a fill, a position or
    // an order event the run never saw: the run cannot say the account did not move.
    let undecoded = session.counters().decode_errors;
    if undecoded > 0 {
        lines.line(format_args!(
            "NOTE {undecoded} frames or HTTP answers could not be decoded: one may have held a \
             fill, a position or an order event the run never saw"
        ));
        ok = false;
    }
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

/// The order as priced and sized, and the caps at its price.
pub struct Priced {
    /// The touch: the best bid and ask, `None` for a side with nothing resting.
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub ticks: Ticks,
    pub px: Decimal,
    pub qty: Lots,
    pub size: Decimal,
    pub notional: Decimal,
    /// The notional at the mark price, when one was read.
    pub mark_notional: Option<Decimal>,
    pub resting: Lots,
    pub inventory: Lots,
}

/// The touch `book` shows on the tick, as prices: its best bid and ask, `None` for a side with
/// nothing resting.
struct Touch {
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    /// The price `--away-bps` behind it on the order's side, floored (a buy) or ceiled (a sell)
    /// onto the tick: an order there or further away rests at least that far behind it.
    away: Ticks,
}

/// `book`'s touch and the price `--away-bps` behind it, or why there is none to stay away from:
/// no price on the side the order rests behind (a buy needs a bid, a sell an ask; the other
/// side may be empty), or a price that would not rest strictly behind the touch.
fn away_from(opts: &Options, book: &OrderbookSnapshot) -> Result<Touch, String> {
    let (best_bid, best_ask) = (
        book.bids.first().map(|b| b.px),
        book.asks.first().map(|a| a.px),
    );
    let tick = opts.tick;
    let of = |t: Ticks| Decimal::from(t.0) * tick;
    let (bid, ask) = (best_bid.map(of), best_ask.map(of));
    let away = Decimal::from(opts.away_bps) / Decimal::from(10_000);
    let ticks = match opts.side {
        OrderSide::Buy => {
            let bid = bid.ok_or(
                "GET /orderbook: no bid rests; a buy is priced behind the best bid, so there is \
                 no touch to stay away from",
            )?;
            (bid * (Decimal::ONE - away) / tick).floor()
        }
        OrderSide::Sell => {
            let ask = ask.ok_or(
                "GET /orderbook: no ask rests; a sell is priced behind the best ask, so there is \
                 no touch to stay away from",
            )?;
            (ask * (Decimal::ONE + away) / tick).ceil()
        }
    };
    let ticks = Ticks(ticks.to_i64().ok_or("the price does not fit")?);
    // Never a crossing order: strictly behind the touch on its own side, so also behind the
    // other side's, when one rests there.
    let behind = match opts.side {
        OrderSide::Buy => {
            ticks.0 > 0 && best_bid.is_some_and(|b| ticks < b) && best_ask.is_none_or(|a| ticks < a)
        }
        OrderSide::Sell => {
            best_ask.is_some_and(|a| ticks > a) && best_bid.is_none_or(|b| ticks > b)
        }
    };
    if !behind {
        return Err(format!(
            "the order's price {} would not rest strictly behind the touch (bid {}, ask {}); \
             refused",
            of(ticks),
            px_or_none(bid),
            px_or_none(ask)
        ));
    }
    Ok(Touch {
        bid,
        ask,
        away: ticks,
    })
}

/// The order `--away-bps` behind the touch on the tick and `--order-usd`'s size on the size
/// step, with the caps in lots (the resting cap at its price, the inventory cap at
/// [`inventory_price`]), or why it cannot be placed: no touch to stay away from
/// ([`away_from`]), no lot in the size, or a notional below the market's minimum.
pub fn price(opts: &Options, book: &OrderbookSnapshot) -> Result<Priced, String> {
    let Touch { bid, ask, away } = away_from(opts, book)?;
    let ticks = away;
    let px = Decimal::from(ticks.0) * opts.tick;
    let lots_for = |usd: Decimal, at: Decimal| -> Result<Lots, String> {
        let n = usd
            .checked_div(at * opts.step)
            .ok_or("an amount does not fit")?
            .floor();
        Lots::new(n.to_i64().ok_or("an amount does not fit")?)
            .ok_or_else(|| "a negative amount".to_owned())
    };
    let order_usd = opts.order_usd;
    let qty = lots_for(order_usd, px)?;
    // The resting cap at the order's price, where it rests; the inventory cap at the highest
    // of that, the bid and the ask, so a position is never worth more than the cap at the
    // market.
    // The caps have no ceiling: one past a lot count is the largest ([`cap_lots`]).
    let resting = cap_lots(opts.resting_cap_usd, px, opts.step);
    let inventory = cap_lots(
        opts.inventory_cap_usd,
        inventory_price(px, bid, ask),
        opts.step,
    );
    let size = Decimal::from(qty.get()) * opts.step;
    let notional = size * px;
    if qty.get() == 0 {
        return Err(format!(
            "${order_usd} at {px} is less than one size step ({}); refused",
            opts.step
        ));
    }
    if notional < opts.min_notional {
        return Err(format!(
            "the order's notional ${} (--order-usd ${order_usd} at {px}, {size} on the {} step) \
             is below the market's minimum ${}; refused",
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
        mark_notional: None,
        resting,
        inventory,
    })
}

/// `order` as [`price`] priced it, judged at the market's `mark` price when one was read: its
/// notional at the mark must reach the market's minimum too (Paradex judges it there, and
/// refuses an order below it only after the place was accepted), and the inventory cap is
/// converted at no less than the mark ([`at_market`]). Unchanged when `mark` is `None`.
pub fn at_mark(opts: &Options, order: Priced, mark: Option<Decimal>) -> Result<Priced, String> {
    let Some(mark) = mark else {
        return Ok(order);
    };
    let at_mark = order.size * mark;
    if at_mark < opts.min_notional {
        return Err(format!(
            "the order's notional at the mark price {mark} is ${} ({} on the {} step), below \
             the market's minimum ${}: Paradex judges the minimum at the mark and refuses the \
             order after accepting it; refused (raise --order-usd within the resting cap, or \
             bring --away-bps nearer)",
            at_mark.round_dp(4),
            order.size,
            opts.step,
            opts.min_notional
        ));
    }
    let at = at_market(order.px, order.bid, order.ask, Some(mark));
    let inventory = cap_lots(opts.inventory_cap_usd, at, opts.step);
    Ok(Priced {
        mark_notional: Some(at_mark),
        inventory,
        ..order
    })
}

/// The price the inventory cap is converted at: [`inventory_price`] of the order's price and
/// the touch, or the mark when it is higher.
pub fn at_market(
    px: Decimal,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    mark: Option<Decimal>,
) -> Decimal {
    mark.into_iter()
        .fold(inventory_price(px, bid, ask), Decimal::max)
}

/// What the touch's second read needs: the command line and the market's spec.
struct Recheck {
    opts: Options,
    specs: SpecTable,
    /// The first read's book sequence: the second must not be older.
    first_seq: u64,
    /// The inventory cap in lots the registry holds, from the first read.
    inventory: Lots,
    /// The mark price read before the run, if any ([`at_market`]).
    mark: Option<Decimal>,
}

impl Recheck {
    /// Reads the touch again and checks the order at `px` is still at least `--away-bps` behind
    /// it, and the inventory cap at [`inventory_price`] of `px` and the fresh touch is no fewer
    /// lots than the caps hold: the touch it reports, or why the order must not go out.
    async fn still_away(&self, px: Ticks) -> Result<(Touch, u64), String> {
        let book = touch(&self.opts, &self.specs).await?;
        if book.seq_no < self.first_seq {
            return Err(format!(
                "the second order book (seq {}) is older than the first (seq {}): a stale \
                 answer shows no current touch",
                book.seq_no, self.first_seq
            ));
        }
        let now = away_from(&self.opts, &book)?;
        let far_enough = match self.opts.side {
            OrderSide::Buy => px <= now.away,
            OrderSide::Sell => px >= now.away,
        };
        if !far_enough {
            return Err(format!(
                "the touch moved toward the order (bid {}, ask {}): at {} it would be less than \
                 {} bps behind it",
                px_or_none(now.bid),
                px_or_none(now.ask),
                Decimal::from(px.0) * self.opts.tick,
                self.opts.away_bps
            ));
        }
        // The inventory cap in lots at the fresh market: never fewer than the registry holds,
        // or its check would admit a position worth more than the cap now.
        let price = Decimal::from(px.0) * self.opts.tick;
        let at = at_market(price, now.bid, now.ask, self.mark);
        let usd = self.opts.inventory_cap_usd;
        let lots = cap_lots(usd, at, self.opts.step).get();
        if lots < self.inventory.get() {
            return Err(format!(
                "the inventory cap of ${usd} is now {lots} lots at {at}, the highest of the \
                 order's price and the touch (bid {}, ask {}), fewer than the {} lots the caps \
                 hold from the first read",
                px_or_none(now.bid),
                px_or_none(now.ask),
                self.inventory.get()
            ));
        }
        Ok((now, book.seq_no))
    }
}

/// What shows the account changed since the seed, or might hold what the run cannot see, in
/// `notes` and `reg`: a reconnect's resync disagreeing with the inventory
/// ([`resync_disagreements`]) or refused by the registry, an order in our namespace that the
/// registry does not hold (shown by a resync or an order event), a fill not of our orders, a
/// position event other than `start` (the market's position seeded at Start; flat on any other
/// market), an order not ours that an order event reported (even one ended since), an order of
/// `owned` that filled since the run took it on ([`traded`]: the round
/// trip can no longer be clean), an amend of an order of ours (the sample never amends), a
/// problem the link noted ([`Note::Problem`]: a venue error after login, an event or outcome
/// the registry could not apply), an asynchronous reject ([`Note::AsyncReject`]), or an order
/// not ours in view on the market. Empty when nothing does.
pub fn account_changes(
    notes: &[Note],
    reg: &Registry,
    start: Option<SignedLots>,
    owned: &[(ClientOrderId, Lots)],
) -> Vec<String> {
    let mut changes: Vec<String> = resync_disagreements(notes)
        .into_iter()
        .map(|what| format!("a resync found the position other than the registry's: {what}"))
        .collect();
    let orphan = |cid: ClientOrderId, vid: Option<&fbc_core::VenueOrderId>| {
        format!(
            "an order in our namespace that the registry does not hold was reported (client \
             id sequence {}, venue order {}): Stop cannot cancel it nor the run count its \
             fills",
            cid.seq(),
            vid.map_or("unknown".to_owned(), |v| format!("{v:?}"))
        )
    };
    for note in notes {
        match note {
            Note::ResyncRefused(why) => changes.push(format!(
                "a resync was refused ({why}): what the registry holds may no longer be the \
                 account's"
            )),
            Note::Resynced { report, .. } => {
                for (cid, vid) in &report.untracked {
                    changes.push(orphan(*cid, Some(vid)));
                }
            }
            Note::Orphan { cid, vid } => changes.push(orphan(*cid, vid.as_ref())),
            // Every order not ours an order event reported, not only those still in view: the
            // registry's view forgets one once it ends, but another trader still operated on
            // the account during the run. (An untracked order in our namespace is an Orphan.)
            Note::Order(
                routed @ (Routed::Foreign(_) | Routed::NotCanonical | Routed::Untracked),
            ) => {
                let whose = match routed {
                    Routed::Foreign(ns) => format!("namespace {}'s", ns.get()),
                    Routed::NotCanonical => "a non-canonical client id's".to_owned(),
                    _ => "one with no client id".to_owned(),
                };
                changes.push(format!(
                    "an order not ours was reported during the run ({whose}): another trader \
                     operated on the account, so Stop could not cancel it nor the run count its \
                     fills"
                ));
            }
            Note::Fill {
                seen,
                unexplained: true,
                ..
            } => changes.push(format!("a fill not of our orders came in: {seen}")),
            // The sample never amends: an amend of ours was another process's (Codex
            // r4226570452 on PR #114).
            Note::Order(Routed::Ours(_, fbc_oms::Applied::Amended)) => changes.push(
                "an order of ours was amended during the run, which this sample never does: \
                 another process operated on it"
                    .to_owned(),
            ),
            // A venue error after login, an asynchronous reject, or what the glue could not
            // apply leaves the run uncertain (Codex r4226570456 on PR #114).
            Note::Problem(what) => changes.push(format!("the run noted a problem: {what}")),
            Note::AsyncReject { op, reject, .. } => changes.push(format!(
                "the run noted a problem: {}",
                async_reject(*op, reject)
            )),
            Note::Position { inst, qty } => {
                // The resync refuses an account with a position on another market.
                let expected = if *inst == INST {
                    start
                } else {
                    Some(SignedLots(0))
                };
                if expected != Some(*qty) {
                    changes.push(format!(
                        "the venue reported the account's position in instrument {} as {} \
                         lots, not the {} lots the run expects",
                        inst.get(),
                        qty.0,
                        expected.map_or("(no Start)".to_owned(), |e| e.0.to_string())
                    ));
                }
            }
            _ => {}
        }
    }
    for (cid, filled) in traded(reg, owned) {
        let vid = reg
            .get(cid)
            .and_then(|r| r.vid().map(|v| v.as_str().to_owned()));
        changes.push(format!(
            "an order of ours traded during the run: venue order {} has {} lots filled",
            vid.unwrap_or_else(|| "none".to_owned()),
            filled.get()
        ));
    }
    if reg.foreign_in_view(INST) {
        changes.push(
            "an order not ours is in view on the market (an order event showed it open): \
             Stop cannot cancel it nor the run count its fills"
                .to_owned(),
        );
    }
    changes
}

/// Those of `owned` (our orders on the market, each with what of it had filled when the run
/// took it on) that filled more since, each with what it has filled now.
pub fn traded(reg: &Registry, owned: &[(ClientOrderId, Lots)]) -> Vec<(ClientOrderId, Lots)> {
    owned
        .iter()
        .filter_map(|(cid, before)| {
            let now = reg.get(*cid).map_or(*before, |r| r.filled());
            (now > *before).then_some((*cid, now))
        })
        .collect()
}

/// Our orders on the market that `snapshot` shows, each with what of it had filled then: the
/// baseline [`traded`] counts from.
pub fn restored_baseline(snapshot: &ResyncSnapshot) -> Vec<(ClientOrderId, Lots)> {
    snapshot
        .orders
        .iter()
        .filter(|o| o.inst == INST)
        .filter_map(|o| match o.cid {
            Some(CidMatch::Ours(cid)) => Some((cid, o.cum_filled)),
            _ => None,
        })
        .collect()
}

/// Why the place must not go out, judged just before it with nothing awaited in between, or
/// `None`:
/// - `gate_open` false: the order socket's current connection does not take places yet (a
///   reconnect since the resync step, its own resync still to end). That resync has left no
///   note for the audit to judge, and a place queued now would go out once it ends, against
///   an inventory it may contradict.
/// - `changes` ([`account_changes`]) not empty: the account changed since the seed (a
///   reconnect's resync disagreeing or refused, a position event, a fill not of ours, an order
///   of ours the registry does not hold, an order of ours that filled since the run took it
///   on) or an order not ours was reported, so the caps would judge the place against an
///   inventory that is no longer the account's. The run's end reports what it was.
/// - `same_side` ([`resting_on_side`]) not empty: an order of ours (an earlier run's, restored
///   by the resync) still rests on the order's side. The registry sums resting lots on a side
///   and the resting cap is in lots at the new order's price, so one resting nearer the touch
///   would count at less than its own price and the dollars resting could exceed
///   --resting-cap-usd. Stop cancels them.
/// - `held` not 0: that many orders of ours a resync showed resting from an earlier connection
///   rest still on the market, where this connection's cancel-on-disconnect may not cover them,
///   so the session takes no place on the market until they end (decision 0080: it would
///   refuse the place as `NotSent(Disconnected)`). Their Safety cancels went out as the
///   connection opened; Stop cancels what is left.
pub fn place_refusal(
    gate_open: bool,
    held: usize,
    changes: &[String],
    same_side: &[(ClientOrderId, Lots)],
) -> Option<String> {
    if !gate_open {
        return Some(
            "the order socket's current connection takes no place yet (a reconnect since the \
             resync step, its resync not ended): the audit cannot judge what that resync finds"
                .to_owned(),
        );
    }
    if !changes.is_empty() {
        return Some(
            "the account changed since the seed, an order of ours traded, or an order not ours \
             is in view"
                .to_owned(),
        );
    }
    if !same_side.is_empty() {
        let lots: i64 = same_side.iter().map(|(_, l)| l.get()).sum();
        return Some(format!(
            "{} orders of ours ({lots} lots) rest on the order's side: the resting cap is \
             counted in lots at the order's price, not theirs; cancel them first",
            same_side.len()
        ));
    }
    if held > 0 {
        return Some(format!(
            "{held} orders of ours rest on the market from an earlier connection, where this \
             connection's cancel-on-disconnect may not cover them, so the session takes no place \
             on it until they end (decision 0080); their Safety cancels went out as it opened, \
             and Stop cancels what is left"
        ));
    }
    None
}

/// Why a run that is not an owner-assisted testnet run (`owner_assisted` false: never seeded by
/// hand) must not Start on the resync it applied, or `None`: the resync was `untrustworthy`, so
/// it seeded no position and nothing may seed one by hand (owner answer C to RB-olg-3: a
/// mainnet market is armed only after a trustworthy resync; Paradex's snapshot source is
/// trustworthy, decision 0094, so this guards a venue declaring otherwise), or the account
/// holds a `position` on the market (the smoke runs on a flat account). Always `None` for an
/// owner-assisted run, which seeds by hand.
pub fn start_refusal(
    owner_assisted: bool,
    untrustworthy: bool,
    position: SignedLots,
) -> Option<String> {
    if owner_assisted {
        return None;
    }
    if untrustworthy {
        return Some(
            "the resync's snapshot source is untrustworthy, so it seeded no position, and this \
             run never seeds one by hand (only an owner-assisted testnet run may, decision \
             0067); it runs only on a venue whose snapshot source is trustworthy (Paradex's is, \
             decision 0094)"
                .to_owned(),
        );
    }
    if position.0 != 0 {
        return Some(format!(
            "the account holds {} lots on the market: the smoke runs on a flat account; close \
             the position first",
            position.0
        ));
    }
    None
}

/// Those of `owned` (our orders on the market) resting on `side`, each with what of it rests
/// ([`OrderRecord::resting`](fbc_oms::OrderRecord::resting)); one the registry does not hold
/// is not among them.
pub fn resting_on_side(
    reg: &Registry,
    owned: &[(ClientOrderId, Lots)],
    side: Side,
) -> Vec<(ClientOrderId, Lots)> {
    owned
        .iter()
        .filter_map(|(cid, _)| {
            let rec = reg.get(*cid)?;
            let rests = rec.resting();
            (rec.placed().side == side && rests > Lots::ZERO).then_some((*cid, rests))
        })
        .collect()
}

/// Those of `owned` (our orders on the market) that did not end cancelled, each with its state
/// (`None`: the registry holds no record of it): still open, or ended another way.
pub fn not_cancelled(
    reg: &Registry,
    owned: &[(ClientOrderId, Lots)],
) -> Vec<(ClientOrderId, Option<OrdState>)> {
    owned
        .iter()
        .filter_map(|(cid, _)| {
            let state = reg.get(*cid).map(|r| r.state());
            match state {
                Some(OrdState::Terminal(TerminalKind::Canceled(_))) => None,
                other => Some((*cid, other)),
            }
        })
        .collect()
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
    /// filled when the run took it on: a fill of any of them during the run fails it, and so
    /// does one not cancelled when the run ends.
    owned: Vec<(ClientOrderId, Lots)>,
    /// The touch's second read, just before the place.
    recheck: Recheck,
    /// The client-id mint once its mark was kept, holding the namespace lease: dropped with
    /// the driver, when the run ends, Stop included (Reviewer B's RB114-7), so no other
    /// testnet_trade takes the namespace while this one trades.
    mint: Option<CidMint>,
    /// [`Network::owner_assisted`].
    owner_assisted: bool,
    /// [`Network::refusal_causes`].
    refusal_causes: &'static str,
    /// The position check (module documentation), lent out while it reads.
    positions: Cell<Option<PositionCheck>>,
    /// Whether the check after the first fill has run.
    fill_checked: Cell<bool>,
    /// Why the run must Stop at once: a mismatch after the first fill that the re-read
    /// confirmed. Every step's wait then ends, so nothing is placed after it.
    halt: RefCell<Option<String>>,
    /// Stop has begun: its waits are not cut short, and the check after the first fill is left
    /// to Stop's own comparison.
    stopping: Cell<bool>,
}

impl Driver {
    /// Waits until `found` finds what it looks for in the link, or the step timeout passes.
    async fn wait<T>(&self, found: impl FnMut(&mut Link) -> Option<T>) -> Option<T> {
        self.wait_for(self.timeout, found).await
    }

    /// Waits until `found` finds what it looks for in the link, or `within` passes. Outside
    /// Stop, the first fill the session sees is checked against `GET /positions` first
    /// ([`Driver::check_after_first_fill`]); a mismatch that persists ends the wait at once
    /// (`None`, the run's halt set), so no step goes on to place.
    async fn wait_for<T>(
        &self,
        within: Duration,
        mut found: impl FnMut(&mut Link) -> Option<T>,
    ) -> Option<T> {
        let mut deadline = tokio::time::Instant::now() + within;
        loop {
            // Created before the check, so a wake between the check and the await is kept.
            let woken = self.wake.notified();
            self.tell();
            if self.check_after_first_fill().await {
                // The check's reads (and a re-read's wait) are not the step's: its time starts
                // again, and what came in meanwhile is read first.
                deadline = tokio::time::Instant::now() + within;
                continue;
            }
            if self.halted() {
                return None;
            }
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
    /// a refusal or problem, a fill, an order event of an order not ours, a connection's end;
    /// and a `WARN` line for an order of ours ended by a cancel the run did not send (FBC-aoh5:
    /// another program on the account, or the Paradex UI; with one order, no repeat follows).
    fn tell(&self) {
        let link = self.link.borrow();
        let notes = &link.notes()[self.told.get()..];
        self.told.set(link.notes().len());
        for note in notes {
            match note {
                Note::Problem(what) => self.note(format_args!("{what}")),
                Note::AsyncReject { op, reject, .. } => {
                    self.note(format_args!("{}", async_reject(*op, reject)));
                }
                Note::Fill { seen, .. } => self.note(format_args!("a fill: {seen}")),
                Note::Position { inst, qty } => self.note(format_args!(
                    "the account's position in instrument {} is {} lots",
                    inst.get(),
                    qty.0
                )),
                Note::ResyncRefused(why) => {
                    self.note(format_args!("the resync was refused: {why}"));
                }
                Note::Orphan { cid, .. } => self.note(format_args!(
                    "an order event of an order in our namespace that the registry does not \
                     hold (client id sequence {})",
                    cid.seq()
                )),
                Note::Order(routed) if !matches!(routed, Routed::Ours(..)) => {
                    self.note(format_args!(
                        "an order event of an order not ours: {routed:?}"
                    ));
                }
                Note::EpochEnd { key, why } => self.note(format_args!(
                    "the order socket's connection {key:?} ended: {why}; the session reconnects, \
                     logs in and arms again"
                )),
                Note::CancelNotOurs { cid, .. } => self.lines.line(format_args!(
                    "WARN our order (client id sequence {}) ended by a user cancel this program \
                     did not send (another program on the account, or the Paradex UI): one \
                     quoter per account",
                    cid.seq()
                )),
                Note::Unprotected { told, rpcs, left } => {
                    let requests: Vec<String> = rpcs.iter().map(|r| r.0.to_string()).collect();
                    self.note(format_args!(
                        "{} orders of ours rest from an earlier connection or run, which this \
                         connection's cancel-on-disconnect may not cover (decision 0080): Safety \
                         cancel requests [{}] sent for them at once{}",
                        told.len(),
                        requests.join(", "),
                        left.iter().map(|l| format!("; {l}")).collect::<String>()
                    ));
                }
                _ => {}
            }
        }
    }

    /// Whether a persisting mismatch after the first fill halts the run's steps (never Stop's).
    fn halted(&self) -> bool {
        !self.stopping.get() && self.halt.borrow().is_some()
    }

    /// The check after the first fill (module documentation), once, outside Stop and once the
    /// market's position is known: true when it ran. A mismatch the re-read confirms sets the
    /// run's halt.
    async fn check_after_first_fill(&self) -> bool {
        if self.stopping.get() || self.fill_checked.get() {
            return false;
        }
        let due = {
            let link = self.link.borrow();
            fill_seen(link.notes()) && link.registry().position_known(INST)
        };
        if !due {
            return false;
        }
        self.fill_checked.set(true);
        let Some(mut check) = self.positions.take() else {
            return false;
        };
        let when = "after the first fill";
        let verdict = check.check(when, &self.lines, self.registry_now()).await;
        self.positions.set(Some(check));
        if let Some(why) = verdict.failure(when) {
            self.halt.replace(Some(why));
        }
        true
    }

    /// The registry's position on the market and the fills of ours applied, read when called.
    fn registry_now(&self) -> impl Fn() -> (SignedLots, usize) + use<> {
        let link = Rc::clone(&self.link);
        move || {
            let link = link.borrow();
            (link.registry().inventory(INST), fills_in(link.notes()))
        }
    }

    /// The comparison at Stop (module documentation), once Stop's cancels are answered: true
    /// unless a mismatch persists. A market whose position no resync seeded has nothing to
    /// compare.
    async fn check_at_stop(&self) -> bool {
        let when = "at Stop";
        if !self.link.borrow().registry().position_known(INST) {
            self.lines.line(format_args!(
                "POSITION {when}: the registry holds no position on the market (no resync seeded \
                 it): nothing to compare"
            ));
            return true;
        }
        let Some(mut check) = self.positions.take() else {
            return true;
        };
        let verdict = check.check(when, &self.lines, self.registry_now()).await;
        self.positions.set(Some(check));
        match verdict.failure(when) {
            Some(why) => {
                self.note(format_args!("{why}"));
                false
            }
            None => true,
        }
    }

    fn timed_out(&self, step: &str, what: &str) {
        let ms = self.lines.t0.elapsed().as_millis();
        if self.halted() {
            // The wait ended on the position check, not the step's time.
            self.lines.line(format_args!(
                "HALT {step} +{ms}ms  not waited for: the position check after the first fill \
                 failed; stopping"
            ));
            return;
        }
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
    /// sent every cancel and each was accepted, every order of ours on the market ended
    /// cancelled, none of them filled during the run, no fill moved the inventory, and no fill
    /// not of our orders or position change came in.
    async fn run(
        mut self,
        order: Planned,
        ns_lease: NamespaceLease,
        persisted: u64,
        leases: Leases,
        control: ExecControl,
    ) -> bool {
        let ok = self.steps(order, ns_lease, persisted, leases).await;
        // What came in since the last wait (a connection's end, say) before Stop's lines.
        self.tell();
        self.stopping.set(true);
        let halted = match self.halt.borrow_mut().take() {
            Some(why) => {
                self.note(format_args!("{why}"));
                true
            }
            None => false,
        };
        // Stop, with GET /positions compared against the registry once Stop's cancels are
        // answered, every run, before the market is disarmed and its leases released.
        let (stopped, positions) = self.stop().await;
        let unmoved = self.inventory_unmoved();
        let cancelled = self.owned_cancelled();
        let account = self.account_unmoved();
        drop(control);
        ok && !halted && stopped && positions && unmoved && cancelled && account
    }

    /// True when every order of ours on the market ended cancelled: one still open, or ended
    /// otherwise, after Stop fails the run, whatever Stop managed to send.
    fn owned_cancelled(&self) -> bool {
        let left = {
            let mut link = self.link.borrow_mut();
            not_cancelled(link.reg(), &self.owned)
        };
        for (cid, state) in &left {
            self.note(format_args!(
                "an order of ours did not end cancelled: venue order {} is {}",
                self.vid_of(*cid),
                match state {
                    Some(state) => format!("{state:?}"),
                    None => "not held by the registry".to_owned(),
                }
            ));
        }
        left.is_empty()
    }

    /// True unless anything [`account_changes`] reports happened: a fill not of our orders, a
    /// position other than the seeded one, a reconnect's resync that disagreed or was refused,
    /// an order of ours the registry does not hold, or an order not ours in view.
    fn account_unmoved(&self) -> bool {
        let changes = self.account_changes();
        for what in &changes {
            self.note(format_args!("{what}"));
        }
        changes.is_empty()
    }

    /// [`account_changes`] of the link's notes and registry, against the position seeded at
    /// Start.
    fn account_changes(&self) -> Vec<String> {
        let link = self.link.borrow();
        account_changes(
            link.notes(),
            link.registry(),
            self.start_position,
            &self.owned,
        )
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
        // Authenticated, or refused LOGIN_REFUSALS times in a row: reconnecting does not mend a
        // key, an address or an account the venue refuses.
        let login = self
            .wait(|l| {
                if l.notes()
                    .iter()
                    .any(|n| matches!(n, Note::Conn(ConnState::Authenticated)))
                {
                    return Some(Ok(()));
                }
                match l.login_refusals() {
                    Some((n, why)) if n >= LOGIN_REFUSALS => Some(Err((n, why.to_owned()))),
                    _ => None,
                }
            })
            .await;
        match login {
            Some(Ok(())) => {}
            Some(Err((n, why))) => {
                self.note(format_args!(
                    "login refused {n} times: {why}; stopping ({}; --help lists them)",
                    self.refusal_causes
                ));
                return false;
            }
            None => {
                self.timed_out("login", "the login and the socket's authentication");
                return false;
            }
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
                let latest = latest_resync(l.notes())?;
                Some(latest.map_err(str::to_owned).map(|(report, snapshot)| {
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
                        // Each with what the snapshot showed filled: an order event may have
                        // raised the registry's count since, and that fill is this run's.
                        restored_baseline(snapshot),
                        snapshot
                            .positions
                            .iter()
                            .find(|(inst, _)| *inst == INST)
                            .map_or(SignedLots(0), |(_, qty)| *qty),
                    )
                }))
            })
            .await;
        let resynced = match resynced {
            Some(Ok(resynced)) => resynced,
            Some(Err(_)) => {
                // The session opened its gate at the resync's end, but the registry applied
                // nothing of it (the NOTE above says why): nothing is built on it.
                self.note(format_args!(
                    "the registry applied no resync behind the session's open gate; nothing \
                     placed"
                ));
                return false;
            }
            None => {
                self.timed_out(
                    "resync",
                    "the REST resync (an open order or position on a market other than \
                     --market refuses it whole)",
                );
                return false;
            }
        };
        let (ours_max, untrustworthy, on_market, open, not_ours, ours, position) = resynced;
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

        self.owned = ours;
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

        if let Some(why) = start_refusal(self.owner_assisted, untrustworthy, position) {
            self.note(format_args!("Start refused: {why}; nothing placed"));
            return false;
        }
        {
            let mut link = self.link.borrow_mut();
            let reg = link.reg();
            if !reg.position_known(INST) {
                if !self.owner_assisted {
                    // A trustworthy resync seeds every market the caps configure: this one
                    // was not, so a fill it could not place is unsettled.
                    drop(link);
                    self.note(format_args!(
                        "Start refused: the trustworthy resync did not seed the market (a fill \
                         it could not place); nothing placed"
                    ));
                    return false;
                }
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
            } else {
                self.lines.line(format_args!(
                    "SEED position {} lots seeded by the trustworthy resync (decision 0094)",
                    reg.inventory(INST).0
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

        // Orders of ours an earlier run left on the market: the session told them unprotected
        // when the epoch opened and the link sent their Safety cancels then (decision 0080).
        // The run places nothing beside them; it waits, for at most the step timeout, until
        // the session shows them ended, so Stop finds them so.
        let told = self.link.borrow().told_on(INST);
        if told > 0 {
            let orders = self.orders.clone();
            let ended = self
                .wait(|_| orders.may_place_on(INST).then_some(()))
                .await
                .is_some();
            self.note(format_args!(
                "{told} orders of ours an earlier run left rest on the market, where this \
                 connection's cancel-on-disconnect may not cover them: Safety-cancelled as it \
                 opened (decision 0080), {}; nothing placed, run again",
                if ended {
                    "and shown ended"
                } else {
                    "not shown ended within the step timeout, so Stop cancels them"
                }
            ));
            return false;
        }

        // The client id, and its mark kept, first: the write is the one wait left before the
        // place, so the touch's second read and the account audit come after it, and nothing
        // waits between the audit and the place.
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
        // Kept before the id goes out, so no later run mints it again: written and synced on
        // a thread of its own ([`off_thread`]), so a slow disk never stalls the runtime the
        // session runs on, for at most the step timeout, nor holds up the exit after it.
        let hwm = self.hwm.clone();
        let mark = mint.high_water();
        // The mint, holding the namespace lease, goes with the write: the lease is held until
        // the write ends, even one that outlives the timeout, so no other run takes the
        // namespace and keeps a newer mark that this late write would then replace. A write
        // that ends in time hands the mint back, kept until the run ends ([`Driver::mint`]).
        let kept = match off_thread_holding(self.timeout, mint, move || hwm.write(mark)).await {
            Some((kept, mint)) => {
                self.mint = Some(mint);
                kept
            }
            None => Err("the high-water mark's write did not finish in time".to_owned()),
        };
        if let Err(e) = kept {
            self.note(format_args!("{e}; nothing placed"));
            return false;
        }
        self.lines.line(format_args!(
            "MARK client-id high-water mark {mark} kept in the lease directory"
        ));

        // The touch again, just before the place: the login, the arm, the resync and the
        // mark's write took time, and the order goes out only if it is still --away-bps
        // behind it.
        match self.recheck.still_away(order.px).await {
            Ok((now, seq)) => self.lines.line(format_args!(
                "BBO again bid {} ask {} (GET /orderbook seq {seq}): the order is still {} bps or \
                 more behind the touch",
                px_or_none(now.bid),
                px_or_none(now.ask),
                self.recheck.opts.away_bps
            )),
            Err(e) => {
                self.note(format_args!("{e}; nothing placed"));
                return false;
            }
        }
        // The last checks, with nothing awaited between them and the place below
        // ([`place_refusal`]): the current connection's gate, the account audit, and an
        // order of ours resting on the order's side.
        let refusal = {
            let link = self.link.borrow();
            let changes = account_changes(
                link.notes(),
                link.registry(),
                self.start_position,
                &self.owned,
            );
            let same_side = resting_on_side(link.registry(), &self.owned, order.side);
            // The orders of ours that hold the market (decision 0080): at least one while the
            // session takes places but not on the market.
            let held = if self.orders.may_place() && !self.orders.may_place_on(INST) {
                let unprotected = self.orders.unprotected();
                unprotected.iter().filter(|o| o.inst == INST).count().max(1)
            } else {
                0
            };
            place_refusal(self.orders.may_place(), held, &changes, &same_side)
        };
        if let Some(why) = refusal {
            self.note(format_args!("{why}; nothing placed"));
            return false;
        }

        // The place: built and authorized by fbc-oms, then handed to the session.
        self.owned.push((cid, Lots::ZERO));
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

        // The hold, ended early by a reconnect whose resync showed the order resting: the
        // session told it unprotected and the link Safety-cancelled it (decision 0080), and that
        // cancel is the round trip's.
        if !self.hold.is_zero() {
            self.wait_for(self.hold, |l| l.safety_cancel_of(cid)).await;
        }

        let cancelled = {
            let mut link = self.link.borrow_mut();
            let safety = link.safety_cancel_of(cid);
            let caps = link.caps().clone();
            match safety {
                Some(rpc) => Ok((rpc, true)),
                None => match link
                    .reg()
                    .cancellable(cid)
                    .map(|permit| permit.cancel(&caps))
                {
                    Ok(CancelChoice::Send(cmd)) => {
                        let auth = link
                            .reg()
                            .authorize(ACCT, cmd)
                            .map_err(|e| format!("{e:?}"));
                        auth.and_then(|auth| link.submit(&self.orders, auth))
                            .map(|rpc| (rpc, false))
                    }
                    Ok(CancelChoice::AwaitAck) => {
                        Err("the cancel waits for an acknowledgement".to_owned())
                    }
                    Err(refusal) => Err(format!("{refusal:?}")),
                },
            }
        };
        let (cancel, safety) = match cancelled {
            Ok(sent) => sent,
            Err(e) => {
                // The order ended meanwhile (a post-only order the venue would not rest), or
                // cannot be cancelled now: Stop's cancel everything sees what is left.
                self.note(format_args!("no cancel was built: {e}"));
                return false;
            }
        };
        self.lines.step(
            "cancel",
            format_args!(
                "request {}: {}",
                cancel.0,
                if safety {
                    "the Safety cancel sent when a reconnect's resync showed the order resting \
                     (decision 0080)"
                } else {
                    "the cancel handed to the session"
                }
            ),
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
                if !self.halted() {
                    self.note(format_args!(
                        "Stop's cancel of every order of ours on the market names it again"
                    ));
                }
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
    /// instrument cancel-all under 0005's I7 guard, which Paradex's trustworthy resync can meet
    /// (decision 0094), otherwise explicit cancels). Paradex takes no cancel by client id before the
    /// venue acknowledged the order, so an unacknowledged order's cancel waits for that
    /// acknowledgement, built the moment it lands ([`Registry::cancels_due`]), for at most the
    /// step timeout; one still waiting then is left to cancel-on-disconnect, which the socket's
    /// close sets off. Then the comparison at Stop ([`Driver::check_at_stop`]) while the
    /// market's lease is still held, and then the market is disarmed and its leases released.
    /// True when every order was cancelled or ended and every cancel was built, sent and
    /// accepted: a cancel refused by the registry or not handed to the session fails it; and
    /// true unless the comparison's mismatch persists.
    async fn stop(&mut self) -> (bool, bool) {
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
        // Every cancel built and handed to the session, or Stop fails.
        let mut sent_all = refusals.is_empty();
        for refused in refusals {
            self.note(format_args!("no Stop cancel for {refused}"));
        }
        let mut rpcs = Vec::new();
        for cmd in cmds {
            sent_all &= self.submit_stop_cancel(cmd, &mut rpcs);
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
                    Ok(CancelChoice::Send(cmd)) => {
                        sent_all &= self.submit_stop_cancel(cmd, &mut rpcs);
                    }
                    Ok(CancelChoice::AwaitAck) => waiting.push(cid),
                    Err(refusal) => {
                        sent_all = false;
                        self.note(format_args!("no Stop cancel: {refusal:?}"));
                    }
                }
            }
        }
        let mut accepted = sent_all && waiting.is_empty();
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
                // What of each had filled when the run took it on (an earlier run's order may
                // have filled in part before): only a fill since then is this run's.
                let before = |cid: ClientOrderId| {
                    self.owned
                        .iter()
                        .find(|(c, _)| *c == cid)
                        .map_or(Lots::ZERO, |(_, b)| *b)
                };
                let ends: Vec<(TerminalKind, Lots, Lots)> = {
                    let mut link = self.link.borrow_mut();
                    let reg = link.reg();
                    named
                        .iter()
                        .filter_map(|cid| reg.get(*cid).map(|r| (r, before(*cid))))
                        .filter_map(|(r, before)| match r.state() {
                            OrdState::Terminal(kind) => Some((kind, r.filled(), before)),
                            _ => None,
                        })
                        .collect()
                };
                for end in ends {
                    match end {
                        (TerminalKind::Canceled(_), filled, before) if filled <= before => {}
                        (TerminalKind::Canceled(_), filled, before) => {
                            accepted = false;
                            self.note(format_args!(
                                "a Stop cancel's order was cancelled after {} lots of it filled \
                                 during the run",
                                filled.get() - before.get()
                            ));
                        }
                        (kind, _, _) => {
                            accepted = false;
                            self.note(format_args!(
                                "a Stop cancel's order ended without being cancelled: {kind:?}"
                            ));
                        }
                    }
                }
            }
        }
        // The comparison while the market's lease is still held, so no other process takes
        // the market while the run compares against its registry (Codex r4305979062 on PR
        // #157); then the market is disarmed and its leases released at once.
        let compared = self.check_at_stop().await;
        self.link.borrow_mut().reg().disarm(INST);
        self.lines.step(
            "stop",
            format_args!(
                "kill switch on; cancel all: {} cancels sent ({how}); market disarmed, leases \
                 released, session closing",
                rpcs.len()
            ),
        );
        (accepted, compared)
    }

    /// Authorizes and submits one of Stop's cancels, adding its request to `rpcs`: false when it
    /// was not handed to the session.
    fn submit_stop_cancel(
        &mut self,
        cmd: fbc_oms::PermittedCommand,
        rpcs: &mut Vec<RpcId>,
    ) -> bool {
        let submitted = {
            let mut link = self.link.borrow_mut();
            let auth = link
                .reg()
                .authorize(ACCT, cmd)
                .map_err(|e| format!("{e:?}"));
            auth.and_then(|auth| link.submit(&self.orders, auth))
        };
        match submitted {
            Ok(rpc) => {
                rpcs.push(rpc);
                true
            }
            Err(e) => {
                self.note(format_args!("a Stop cancel was not submitted: {e}"));
                false
            }
        }
    }
}
