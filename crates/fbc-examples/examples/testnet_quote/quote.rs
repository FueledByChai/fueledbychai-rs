//! testnet_quote's loop: a PLUMBING TEST, NOT A STRATEGY. One market, one post-only order of a
//! fixed size on each side, a fixed offset behind the touch, requoted as the touch moves, through
//! fbc-oms's [`ExecutionPlanner`] and caps and fbc-runtime's order-entry session on Paradex
//! TESTNET. The offsets are the command line's: nothing here prices from a model.
//!
//! **One cycle.** The touch is read over REST (`GET /orderbook`); each side whose book side rests
//! gets a [`DesiredQuote`] behind it ([`desired`]), and the planner closes the difference
//! between that [`DesiredBook`] and the orders it placed, pass after pass, until nothing is left
//! to send, held or awaited ([`Driver::settle`]), each pass's commands authorized by fbc-oms and
//! handed to the session. The planner is given Paradex's [`OrderCaps`] with `amend` set to
//! `None`, so every change is a cancel and, once the order ended, a new place
//! (cancel-and-replace, never an amend). Paradex takes no cancel before an order's
//! acknowledgement (`cancel_before_ack` false), so one order never has two commands in flight.
//!
//! **The caps.** The quote size is a fixed number of lots, `--order-usd` at the highest quote
//! price of the first touch ([`size`]); fbc-oms's caps are the USD caps in lots at that same
//! price ([`Sizing`]). Each cycle a side is left unquoted when its notional at its price would
//! exceed `--resting-cap-usd` or fall below `--min-notional`, or when a fill would take the
//! position past `--inventory-cap-usd` at the highest of the quote prices and the touch, so every
//! place stays inside the USD caps however the price moved since the start; fbc-oms checks the
//! caps in lots on every place as well.
//!
//! **Reconnects** (and the start). The session arms cancel-on-disconnect on every connection
//! and resyncs before it takes a place (FBC-w19); the orders of ours the resync shows resting are
//! told unprotected and Safety-cancelled by the [`Link`] at once (decision 0080). Paradex does
//! not say an arm covers orders already open, so after every reconnect, once the arm is
//! accepted and the resync has ended, the loop cancels every other order of ours still open on
//! the market and waits until all of them ended before it places ([`Driver::sweep`]): an
//! instrument cancel-all when fbc-oms builds one under 0005's I7 guard, which needs a trustworthy
//! resync (Paradex's is, decision 0094), otherwise explicit cancels of each order of ours, never
//! an account cancel-all.
//!
//! **Stop** (kill switch on, every order of ours on the market cancelled, the session closed):
//! on the interrupt (Ctrl-C), at `--duration` or `--cycles`, and on anything the loop does not
//! expect ([`Driver::absorb`]): a refused login, a resync that disagrees with the inventory or
//! that the registry refuses, an order of ours gone Unknown (the loop does not drive the Unknown
//! ladder: no Paradex order query is wired here), an order or a fill not ours, an amend of ours,
//! a problem the link noted, an asynchronous reject other than one of our own post-only places
//! closed POST_ONLY_WOULD_CROSS (that side is left out of the cycle and placed again the next),
//! an order book older than one already read, the client-id sequence space running out, a book
//! that does not settle within the step timeout (a position the venue reports that the inventory
//! does not hold holds every pass until they agree), and the session's end. Each is taken as
//! it comes: a touch being read is not waited for ([`Driver::touch_unless_ended`]), and a pass
//! reads the notes again after its waits, before it plans ([`Driver::halted`]).
//!
//! **The position check** (FBC-hpg6, the owner's condition in decision 0094: positions follow
//! fills). After the first fill the loop sees, the next pass reads `GET /positions` before it
//! plans and compares the market's size with the registry's position, exactly in lots
//! ([`PositionCheck`]); a mismatch is read once more after the step timeout, both readings
//! printed. The check is raced against what ends a cycle, as a touch is, and the pass after it
//! reads a fresh touch before it plans. One the re-read clears lets the loop go on; one that
//! persists Stops it (the kill switch and the cancel of everything of ours), with nothing
//! planned or placed after the fill, and fails the run, saying to check the position by hand.
//! At Stop, once Stop's cancels are answered, the comparison runs again on every run, fills or
//! not; a mismatch that persists there fails the run with both readings.
//!
//! **Memory.** The link's notes are read once each ([`Driver::absorb`]) and those read are
//! forgotten every cycle ([`Link::forget`]), so a long run keeps no growing note list.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use fbc_core::{
    Bps, Channel, CidMatch, CidMint, ClientOrderId, ConnState, Lots, MarketLease, Namespace,
    NamespaceLease, NonceScope, OpKind, OrderCaps, OrderKindTag, RejectKind, RpcId, Secrets, Side,
    SignedLots, SpecTable, SubmitOutcome, Ticks, Tif, TifTag, VenueFactory,
};
use fbc_oms::{
    Applied, CancelChoice, CancelEverything, DesiredBook, DesiredQuote, ExecutionPlanner,
    FillLedger, Intent, LadderConfig, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, OrdState,
    PermittedCommand, PlanRefusal, PlannerConfig, PreTradeCaps, Registry, Routed, Stage,
    TestnetRun,
};
use fbc_runtime::{Connector, ExecControl, ExecOrders, ExecSession};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::md::rest::OrderbookSnapshot;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Notify;

use crate::args::{Options, Target, direct_to_stub, hosts, sole, testnet_guard};
use crate::link::{Link, LinkHandler, Note, async_reject};
use crate::positions::{PositionCheck, Verdict, fills_in};
use crate::testnet::{
    ACCOUNT_LABEL, ACCT, HighWater, INST, Lines, Out, TESTNET_CHAIN, VENUE, config, drive_with,
    inventory_price, off_thread_holding, px_or_none, resync_disagreements, session_config,
    snapshot_max, specs, touch, wall_now,
};

/// How many client ids each kept high-water mark reserves ahead of the mint: the mark is written
/// before the ids go out, and again only once the mint is within one pass (two places) of it.
pub const RESERVE: u64 = 1_024;

/// A high-water mark's write off the runtime's thread: whether it was kept, and the mint it
/// held, once it ends within the step timeout.
type MarkWrite = Pin<Box<dyn Future<Output = Option<(Result<(), String>, Option<CidMint>)>>>>;

/// What a run did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// The loop ended as asked (`--duration`, `--cycles` or the interrupt), every Stop cancel
    /// was accepted and every order of ours ended.
    pub ok: bool,
    /// Why the loop ended.
    pub ended: String,
    /// Requote cycles completed.
    pub cycles: u64,
    /// Places and cancels (every other request: cancels, cancel-manys, cancel-alls) the session
    /// reported sent.
    pub places_sent: usize,
    pub cancels_sent: usize,
    /// Orders of ours that ended cancelled.
    pub cancelled: usize,
    /// Fills of orders of ours.
    pub fills: usize,
    /// The market's inventory at the end, in lots.
    pub inventory: i64,
    /// The requests whose items the loop and the link still hold at the end (in flight, or
    /// unanswered): what a long run keeps.
    pub requests_held: usize,
}

/// The quote size and fbc-oms's caps, in lots, at the first touch.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Sizing {
    /// Each quote's size.
    pub qty: Lots,
    /// The resting cap per side and the inventory cap.
    pub resting: Lots,
    pub inventory: Lots,
    /// The price they were converted at: the highest of the quote prices and the touch.
    pub at: Decimal,
}

/// A side's quote price behind the touch, or why there is none.
type Behind = Result<Ticks, String>;

/// The quote prices behind `book`'s touch: the bid `--bid-bps` below the best bid (floored onto
/// the tick), the ask `--ask-bps` above the best ask (ceiled), each strictly behind the touch on
/// both sides; a side whose book side is empty has none.
pub fn quote_prices(opts: &Options, book: &OrderbookSnapshot) -> (Behind, Behind) {
    let best_bid = book.bids.first().map(|b| b.px);
    let best_ask = book.asks.first().map(|a| a.px);
    let tick = opts.tick;
    let bps = |n: u32| Decimal::from(n) / Decimal::from(10_000);
    let to_ticks = |d: Decimal| -> Behind {
        d.to_i64()
            .map(Ticks)
            .ok_or_else(|| "the price does not fit".to_owned())
    };
    let behind = |ok: bool, t: Ticks| -> Behind {
        if ok {
            Ok(t)
        } else {
            Err("the quote would not rest strictly behind the touch".to_owned())
        }
    };
    let bid = best_bid
        .ok_or_else(|| "its book side is empty".to_owned())
        .and_then(|b| {
            let px = Decimal::from(b.0) * tick * (Decimal::ONE - bps(opts.bid_bps));
            let t = to_ticks((px / tick).floor())?;
            behind(t.0 > 0 && t < b && best_ask.is_none_or(|a| t < a), t)
        });
    let ask = best_ask
        .ok_or_else(|| "its book side is empty".to_owned())
        .and_then(|a| {
            let px = Decimal::from(a.0) * tick * (Decimal::ONE + bps(opts.ask_bps));
            let t = to_ticks((px / tick).ceil())?;
            behind(t > a && best_bid.is_none_or(|b| t > b), t)
        });
    (bid, ask)
}

/// `t` as a price.
fn px_of(opts: &Options, t: Ticks) -> Decimal {
    Decimal::from(t.0) * opts.tick
}

/// The touch's best bid and ask as prices.
fn touch_px(opts: &Options, book: &OrderbookSnapshot) -> (Option<Decimal>, Option<Decimal>) {
    (
        book.bids.first().map(|b| px_of(opts, b.px)),
        book.asks.first().map(|a| px_of(opts, a.px)),
    )
}

/// The highest of the quote prices and the touch: where a position's worth is judged.
fn market_px(opts: &Options, book: &OrderbookSnapshot, quotes: &[&Behind]) -> Option<Decimal> {
    let (bid, ask) = touch_px(opts, book);
    let top = quotes
        .iter()
        .filter_map(|q| q.as_ref().ok())
        .map(|t| px_of(opts, *t))
        .chain(bid)
        .chain(ask)
        .max()?;
    Some(inventory_price(top, bid, ask))
}

/// The quote size and the caps in lots at the first touch `book` ([`Sizing`]), or why the run
/// cannot quote: no side of the book rests, no lot in `--order-usd`, or a quote's notional below
/// the market's minimum at the lowest quote price.
pub fn size(opts: &Options, book: &OrderbookSnapshot) -> Result<Sizing, String> {
    let (bid, ask) = quote_prices(opts, book);
    if bid.is_err() && ask.is_err() {
        return Err(format!(
            "no side can be quoted at the first touch (bid: {}; ask: {})",
            bid.err().unwrap_or_default(),
            ask.err().unwrap_or_default()
        ));
    }
    let at = market_px(opts, book, &[&bid, &ask]).ok_or("no price to size the quotes at")?;
    let lots_for = |usd: Decimal| -> Lots {
        let n = (usd / (at * opts.step)).floor().to_i64().unwrap_or(0);
        Lots::new(n).unwrap_or(Lots::ZERO)
    };
    let qty = lots_for(opts.order_usd);
    if qty == Lots::ZERO {
        return Err(format!(
            "--order-usd ${} at {at} is less than one size step ({}); refused",
            opts.order_usd, opts.step
        ));
    }
    let lowest = [&bid, &ask]
        .into_iter()
        .filter_map(|q| q.as_ref().ok())
        .map(|t| px_of(opts, *t))
        .min()
        .unwrap_or(at);
    let notional = Decimal::from(qty.get()) * opts.step * lowest;
    if notional < opts.min_notional {
        return Err(format!(
            "a quote's notional ${} ({} lots at {lowest}) is below the market's minimum ${}; \
             raise --order-usd",
            notional.round_dp(4),
            qty.get(),
            opts.min_notional
        ));
    }
    Ok(Sizing {
        qty,
        resting: lots_for(opts.resting_cap_usd),
        inventory: lots_for(opts.inventory_cap_usd),
        at,
    })
}

/// The requests [`Driver::compact`] released most recently. A place is released once its first
/// answer (Paradex's `Accepted{Provisional}`) is in, and its `Accepted{Final}` (decision 0092)
/// can come after, when the link no longer holds the request: remembered here, such an answer
/// is told from one to the session's own request, its cancel-on-disconnect arm (RB-elg7-5 on
/// PR #149). The oldest are forgotten past [`Released::KEPT`], so it stays small.
#[derive(Debug, Default)]
pub struct Released(VecDeque<RpcId>);

/// Whose an answer to a request the link did not hold is ([`Released::answer`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unheld {
    /// The session's own request: its cancel-on-disconnect arm.
    Arm,
    /// A later answer to a request of ours already released.
    Late,
}

impl Released {
    /// How many released requests are remembered: a late answer comes within a cycle or two.
    pub const KEPT: usize = 256;

    /// Remembers `rpc` as released, forgetting the oldest past [`Released::KEPT`].
    pub fn release(&mut self, rpc: RpcId) {
        if self.0.len() == Self::KEPT {
            self.0.pop_front();
        }
        self.0.push_back(rpc);
    }

    /// Whose an answer to `rpc`, a request the link did not hold, is.
    pub fn answer(&self, rpc: RpcId) -> Unheld {
        if self.0.contains(&rpc) {
            Unheld::Late
        } else {
            Unheld::Arm
        }
    }
}

/// One cycle's desired book, the quotes in it, and the sides left out and why.
#[derive(Clone, Debug)]
pub struct Wanted {
    pub book: DesiredBook,
    pub bid: Option<Ticks>,
    pub ask: Option<Ticks>,
    pub skipped: Vec<String>,
}

/// The desired book for the touch `book` with the position at `position` ([`quote_prices`]),
/// each quote `qty` lots: a side is left out when it has no price behind the touch, when its
/// notional at its price would exceed `--resting-cap-usd` or fall below `--min-notional`, or
/// when a fill of it would take the position past `--inventory-cap-usd` at the highest of the
/// quote prices and the touch.
pub fn desired(
    opts: &Options,
    qty: Lots,
    position: SignedLots,
    book: &OrderbookSnapshot,
) -> Wanted {
    let (bid, ask) = quote_prices(opts, book);
    let mkt = market_px(opts, book, &[&bid, &ask]);
    let mut wanted = Wanted {
        book: DesiredBook::new(INST),
        bid: None,
        ask: None,
        skipped: Vec::new(),
    };
    for (side, name, quote) in [(Side::Buy, "bid", bid), (Side::Sell, "ask", ask)] {
        let px = match quote {
            Ok(t) => t,
            Err(why) => {
                wanted.skipped.push(format!("{name}: {why}"));
                continue;
            }
        };
        let price = px_of(opts, px);
        let notional = Decimal::from(qty.get()) * opts.step * price;
        let after = match side {
            Side::Buy => position.0.saturating_add(qty.get()),
            Side::Sell => position.0.saturating_sub(qty.get()),
        };
        let worth = Decimal::from(after.unsigned_abs()) * opts.step * mkt.unwrap_or(price);
        let why = if notional > opts.resting_cap_usd {
            Some(format!(
                "its notional ${} at {price} is above --resting-cap-usd ${}",
                notional.round_dp(4),
                opts.resting_cap_usd
            ))
        } else if notional < opts.min_notional {
            Some(format!(
                "its notional ${} at {price} is below the market's minimum ${}",
                notional.round_dp(4),
                opts.min_notional
            ))
        } else if worth > opts.inventory_cap_usd {
            Some(format!(
                "a fill would take the position to {after} lots, ${} at {}, past \
                 --inventory-cap-usd ${}",
                worth.round_dp(4),
                mkt.unwrap_or(price),
                opts.inventory_cap_usd
            ))
        } else {
            None
        };
        if let Some(why) = why {
            wanted.skipped.push(format!("{name}: {why}"));
            continue;
        }
        let quote = DesiredQuote {
            px,
            qty,
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        wanted.book.set(side, 0, quote);
        match side {
            Side::Buy => wanted.bid = Some(px),
            Side::Sell => wanted.ask = Some(px),
        }
    }
    wanted
}

/// Refuses order caps that do not admit the loop's one order shape: a post-only GTC limit on
/// the public book.
pub fn shape_admitted(caps: &OrderCaps) -> Result<(), String> {
    if caps.kinds.contains(OrderKindTag::Limit)
        && caps.tifs.contains(TifTag::Gtc)
        && caps.channels.contains(Channel::Public)
        && caps.post_only
    {
        Ok(())
    } else {
        Err(
            "the venue's order caps do not admit a post-only GTC limit order on the public book, \
             the only order this loop places"
                .to_owned(),
        )
    }
}

/// The caps the planner is given: the venue's with no amend, so every change it builds is a
/// cancel and then a place (cancel-and-replace only).
pub fn cancel_and_replace(caps: &OrderCaps) -> OrderCaps {
    OrderCaps {
        amend: None,
        ..caps.clone()
    }
}

/// Runs testnet_quote: the guard first, before anything connects; then the first touch and the
/// sizing, the registry, the session and the loop, until `interrupt` completes, `--duration` or
/// `--cycles` is reached, or anything Stops it. `chain` is `PARADEX_CHAIN_ID` when set; `creds`
/// the account's credentials, twice: the session's, handed to its codec unread, and the position
/// check's own login's, each moved on unread (no type here holds them). An error is a refusal before the session started; once it
/// started the outcome is in the [`Report`].
pub async fn run(
    opts: &Options,
    chain: Option<&str>,
    creds: (Secrets, Secrets),
    out: Out,
    interrupt: impl Future<Output = ()>,
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
    let (rest_host, ws_host) = hosts(&opts.rest_url, &opts.ws_url)?;
    lines.line(format_args!(
        "TESTNET {what}: REST host {rest_host} WS host {ws_host} chain {TESTNET_CHAIN}"
    ));
    lines.line(format_args!(
        "OWNER-ASSISTED testnet run (decision 0067): Paradex's snapshot source is trustworthy \
         (decision 0094), so the resync seeds the market's position; a market it left unknown \
         is seeded by hand from the venue's REST position, never outside a declared testnet run"
    ));
    lines.line(format_args!(
        "PLUMBING TEST, not a strategy: fixed offsets of {} bps under the bid and {} bps over \
         the ask, no model",
        opts.bid_bps, opts.ask_bps
    ));

    let factory = &ParadexFactory;
    let cfg = config(&opts.rest_url, &opts.ws_url);
    let specs = specs(factory, &cfg, &opts.market, opts.tick, opts.step)?;
    let wait = Duration::from_secs(opts.step_timeout_secs);
    let book = touch(&opts.rest_url, &opts.market, &opts.proxy, wait, &specs).await?;
    let sizing = size(opts, &book)?;
    let (bid, ask) = touch_px(opts, &book);
    lines.line(format_args!(
        "QUOTE {} lots ({} at the size step) per side; caps at {}: resting {} lots per side, \
         inventory {} lots (first touch: bid {} ask {})",
        sizing.qty.get(),
        Decimal::from(sizing.qty.get()) * opts.step,
        sizing.at,
        sizing.resting.get(),
        sizing.inventory.get(),
        px_or_none(bid),
        px_or_none(ask)
    ));

    let venue_caps = factory.caps(&cfg).map_err(|e| e.to_string())?;
    let order_caps = venue_caps
        .exec
        .as_ref()
        .ok_or("Paradex declares no order entry")?
        .order
        .clone();
    shape_admitted(&order_caps)?;
    let symbol = specs
        .get(INST)
        .ok_or("the market is not in the spec table")?
        .venue_symbol
        .clone();
    let limits = MarketCapsConfig {
        inventory: Some(sizing.inventory),
        resting: Some(sizing.resting),
    };
    let caps = PreTradeCaps::new()
        .with_market(INST, limits)
        .map_err(|e| format!("caps: {e:?}"))?;
    let keys = LeaseKeys::new(VENUE, ACCOUNT_LABEL, &order_caps).with_market(INST, symbol.clone());
    let needs_account_lease = keys.nonce_scope() == NonceScope::PerAccountMonotonic;
    let reg = Registry::with_caps(caps)
        .for_testnet_run(TestnetRun::owner_assisted())
        .with_lease_keys(keys);

    fs::create_dir_all(&opts.lease_dir)
        .map_err(|e| format!("--lease-dir {}: {e}", opts.lease_dir.display()))?;
    let held = |e: &dyn std::fmt::Debug| {
        format!(
            "a lease in {} is held (another sample running?): {e:?}",
            opts.lease_dir.display()
        )
    };
    let ns = Namespace::new(opts.namespace);
    let ns_lease = NamespaceLease::acquire(&opts.lease_dir, ACCT, ns).map_err(|e| held(&e))?;
    let hwm = HighWater::at(&opts.lease_dir, ns);
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
        crate::testnet::SNAPSHOT_SETTLE,
        Duration::from_secs(60),
        2,
    )
    .map_err(|e| format!("{e:?}"))?;
    let planner = ExecutionPlanner::new(
        PlannerConfig::new(Bps(0.0), Lots::ZERO, Duration::ZERO).map_err(|e| e.to_string())?,
    );
    let plan_caps = cancel_and_replace(&order_caps);
    let link = Rc::new(RefCell::new(Link::new(
        reg,
        ledger,
        order_caps.clone(),
        ladder,
    )));
    let wake = Rc::new(Notify::new());
    let handler = LinkHandler {
        link: Rc::clone(&link),
        wake: Rc::clone(&wake),
    };
    let (session_creds, position_creds) = creds;
    let session_cfg = session_config(
        factory,
        cfg.clone(),
        session_creds,
        specs.clone(),
        &opts.proxy,
        opts.namespace,
    )?;
    // The position check reads over REST through the limiter the session charges, so its reads
    // and the session's traffic share one count (FBC-hpg6).
    let positions = PositionCheck::new(
        &cfg,
        position_creds,
        Connector::new(opts.proxy.clone()),
        session_cfg.limiter.clone(),
        &opts.rest_url,
        specs.clone(),
        venue_caps,
        wait,
    )?;
    let (mut session, control) =
        ExecSession::new(session_cfg, handler).map_err(|e| format!("session: {e}"))?;
    link.borrow_mut().attach(session.orders(), ACCT);
    let interrupted = Rc::new(Cell::new(false));
    let mut driver = Driver {
        link: Rc::clone(&link),
        wake: Rc::clone(&wake),
        orders: session.orders(),
        lines: lines.clone(),
        opts: opts.clone(),
        specs,
        qty: sizing.qty,
        timeout: wait,
        interrupted: Rc::clone(&interrupted),
        deadline: tokio::time::Instant::now() + Duration::from_secs(opts.duration_secs),
        planner,
        plan_caps,
        caps: order_caps,
        hwm,
        mint: None,
        reserved: 0,
        writing: None,
        released: Released::default(),
        authenticated: false,
        armed: 0,
        resynced: None,
        epochs: 0,
        swept: 0,
        resync_due: false,
        trouble: None,
        answers: HashMap::new(),
        submitted: HashSet::new(),
        ours: HashMap::new(),
        owned: Vec::new(),
        cancelled: 0,
        fills: Rc::new(Cell::new(0)),
        cycles: 0,
        last: None,
        book_seq: book.seq_no,
        venue_position: None,
        unfilled: Vec::new(),
        positions: Some(positions),
        fill_check_due: false,
        fill_checked: false,
    };
    // The interrupt sets a flag the driver reads at each wake, and wakes it.
    let watch = {
        let (flag, wake) = (Rc::clone(&interrupted), Rc::clone(&wake));
        async move {
            interrupt.await;
            flag.set(true);
            wake.notify_waiters();
            std::future::pending::<()>().await;
        }
    };
    let driving = async {
        tokio::pin!(watch);
        let quoting = driver.run(ns_lease, persisted, leases, control);
        tokio::pin!(quoting);
        tokio::select! {
            () = &mut watch => unreachable!("the watch never ends"),
            done = &mut quoting => done,
        }
    };
    let ran = drive_with(&mut session, driving).await;
    let (mut ok, ended) = match ran {
        Ok(done) => done,
        Err(why) => {
            let why = format!(
                "the session ended before the loop did: {why}; nothing more can be sent, and \
                 Paradex's cancel-on-disconnect cancels what the closed socket left resting"
            );
            lines.line(format_args!("STOP {why}"));
            (false, why)
        }
    };
    let undecoded = session.counters().decode_errors;
    if undecoded > 0 {
        lines.line(format_args!(
            "NOTE {undecoded} frames or HTTP answers could not be decoded: one may have held a \
             fill, a position or an order event the run never saw"
        ));
        ok = false;
    }
    drop(session);
    let report = driver.report(ok, ended);
    lines.line(format_args!(
        "SUMMARY cycles {} orders placed {} cancels sent {} orders cancelled {} fills {} \
         inventory {} lots",
        report.cycles,
        report.places_sent,
        report.cancels_sent,
        report.cancelled,
        report.fills,
        report.inventory
    ));
    lines.line(format_args!(
        "DONE {} ({})",
        if report.ok { "ok" } else { "failed" },
        report.ended
    ));
    Ok(report)
}

/// What one requote cycle came to.
enum Settled {
    /// Nothing left to send, hold or await.
    Done,
    /// The connection changed, or the session takes no place now: the reconnect's sweep first.
    Reconnected,
    /// The interrupt came.
    Interrupted,
    /// `--duration` passed: nothing more is handed over (Codex r4243908421).
    Ended,
}

/// What each request of ours is, for its lines and its answer: a place or a cancel, the side
/// and order of its first item (none for an instrument cancel-all), and how many items it has.
#[derive(Copy, Clone, Debug)]
struct Request {
    place: bool,
    first: Option<(Side, ClientOrderId)>,
    items: usize,
}

/// The loop (module documentation).
struct Driver {
    link: Rc<RefCell<Link>>,
    wake: Rc<Notify>,
    orders: ExecOrders,
    lines: Lines,
    opts: Options,
    specs: SpecTable,
    qty: Lots,
    timeout: Duration,
    interrupted: Rc<Cell<bool>>,
    deadline: tokio::time::Instant,
    planner: ExecutionPlanner,
    /// The venue's caps with no amend, for the planner ([`cancel_and_replace`]).
    plan_caps: OrderCaps,
    /// The venue's caps.
    caps: OrderCaps,
    hwm: HighWater,
    /// The client-id mint, holding the namespace lease, once the first resync floored it.
    mint: Option<CidMint>,
    /// The high-water mark kept: no id above it is minted before a higher one is kept.
    reserved: u64,
    /// The mark's write still in flight when a cycle ended during it, holding the mint, and the
    /// mark it keeps: the next pass waits for it and takes the mint back (Codex r4244206466).
    writing: Option<(u64, MarkWrite)>,
    /// The requests released most recently, so a later answer to one is not taken for an arm.
    released: Released,
    /// A connection authenticated; arms the venue accepted; the latest resync applied: the
    /// highest of our client ids it showed, how many open orders, how many of them on the market
    /// not ours, and the market's position.
    authenticated: bool,
    armed: usize,
    resynced: Option<(u64, usize, usize, SignedLots)>,
    /// Connections that ended, and how many of them the reconnect's sweep has handled.
    epochs: usize,
    swept: usize,
    /// A connection ended and its successor's resync has not ended yet.
    resync_due: bool,
    /// The first thing that Stops the loop, as the notes showed it.
    trouble: Option<String>,
    /// The answers to each request of ours, item by item (`None`: the whole request's, a not
    /// sent included), until [`Driver::answer`] has them all.
    answers: HashMap<RpcId, Vec<(Option<u16>, SubmitOutcome)>>,
    submitted: HashSet<RpcId>,
    ours: HashMap<RpcId, Request>,
    /// The orders of ours on the market the run knows that have not ended (the resyncs' and the
    /// planner's): each leaves once it ended, its end printed ([`Driver::prune`]), so the list
    /// holds what rests, never the run's history.
    owned: Vec<ClientOrderId>,
    /// Orders of ours that ended cancelled.
    cancelled: usize,
    /// Fills of ours read from the notes: shared with the position check's registry reading,
    /// which may land after more notes were read ([`Driver::registry_now`]).
    fills: Rc<Cell<usize>>,
    cycles: u64,
    /// The last cycle's quotes and skipped sides, to print a cycle only when they change.
    last: Option<(Option<Ticks>, Option<Ticks>, Vec<String>)>,
    /// The highest `seq_no` of the order books read: an older one Stops the loop (Codex
    /// r4244080123 on PR #149).
    book_seq: u64,
    /// The market's position as the venue's position stream last reported it, since the last
    /// resync: no pass plans while it differs from the inventory (Codex r4244080127).
    venue_position: Option<SignedLots>,
    /// Orders of ours that ended with fills the venue reported on an order update that the
    /// fill stream has not brought yet: no pass plans until it has (Codex r4244145969).
    unfilled: Vec<ClientOrderId>,
    /// The position check (module documentation), lent out while it reads.
    positions: Option<PositionCheck>,
    /// A fill was seen and the check after the first fill has not run: no pass plans until it
    /// has.
    fill_check_due: bool,
    /// The check after the first fill has run (or was passed over at Stop).
    fill_checked: bool,
}

impl Driver {
    /// The loop, then Stop whatever ended it: whether it ended as asked and Stop did all it
    /// must, and why it ended.
    async fn run(
        &mut self,
        ns_lease: NamespaceLease,
        persisted: u64,
        leases: Leases,
        control: ExecControl,
    ) -> (bool, String) {
        let (mut asked, mut why) = match self.quote(ns_lease, persisted, leases).await {
            Ok(why) => (true, why),
            Err(why) => (false, why),
        };
        self.absorb();
        // Something unexpected noted as the loop ended still fails the run.
        if let Some(late) = self.trouble.clone()
            && asked
        {
            asked = false;
            why = late;
        }
        self.lines.line(format_args!("STOP {why}"));
        // Stop, with GET /positions compared against the registry once Stop's cancels are
        // answered, every run, before the market is disarmed and its leases released.
        let (stopped, compared) = self.stop().await;
        if let Some(failed) = compared
            && asked
        {
            asked = false;
            why = failed;
        }
        self.compact();
        drop(control);
        (asked && stopped, why)
    }

    /// The registry's position on the market and the fills of ours applied (those read and
    /// those still in the link's notes), read when called.
    fn registry_now(&self) -> impl Fn() -> (SignedLots, usize) + use<> {
        let (link, read) = (Rc::clone(&self.link), Rc::clone(&self.fills));
        move || {
            let link = link.borrow();
            let fills = read.get() + fills_in(link.notes());
            (link.registry().inventory(INST), fills)
        }
    }

    /// The check after the first fill (module documentation), when one is due: true when it
    /// ran. A mismatch the re-read confirms is a reason to Stop. One due on a market whose
    /// position is not known (never, once the loop Started) is left to the Stop comparison.
    ///
    /// The check is raced against what ends a cycle ([`Driver::ending`]: the interrupt, a
    /// reason to Stop, a connection's end, `--duration`), read as the notes come, so none of
    /// them waits for its reads or its re-read's wait (up to three step timeouts) while the
    /// quotes rest (RA157-1 / RB157-2 on PR #157, the rule of Codex r4243989987 on PR #149).
    /// A check cut short is due again: the Stop comparison takes it, or the next pass after a
    /// reconnect's sweep.
    async fn check_after_first_fill(&mut self) -> bool {
        if !self.fill_check_due {
            return false;
        }
        self.fill_check_due = false;
        self.fill_checked = true;
        if !self.link.borrow().registry().position_known(INST) {
            return false;
        }
        let Some(mut check) = self.positions.take() else {
            return false;
        };
        let when = "after the first fill";
        let verdict = self.check_unless_ended(&mut check, when).await;
        self.positions = Some(check);
        let Some(verdict) = verdict else {
            self.fill_check_due = true;
            return false;
        };
        if let Some(why) = verdict.failure(when) {
            self.trouble(why);
        }
        true
    }

    /// `check` `when`, or `None` once the cycle must end before it comes to a verdict
    /// ([`Driver::ending`], as [`Driver::touch_unless_ended`] reads the touch); the check is
    /// then dropped, its read or its re-read's wait abandoned.
    async fn check_unless_ended(
        &mut self,
        check: &mut PositionCheck,
        when: &str,
    ) -> Option<Verdict> {
        let lines = self.lines.clone();
        let checking = check.check(when, &lines, self.registry_now());
        tokio::pin!(checking);
        loop {
            let wake = Rc::clone(&self.wake);
            // Created before the check, so a wake between the check and the await is kept.
            let woken = wake.notified();
            self.absorb();
            if self.ending() {
                return None;
            }
            tokio::select! {
                verdict = &mut checking => return Some(verdict),
                () = woken => {}
                () = tokio::time::sleep_until(self.deadline) => return None,
            }
        }
    }

    /// The comparison at Stop (module documentation): why the run fails when a mismatch
    /// persists, `None` otherwise. A market whose position no resync seeded has nothing to
    /// compare.
    async fn check_at_stop(&mut self) -> Option<String> {
        // A fill seen too late for its own check is compared here.
        self.fill_check_due = false;
        let when = "at Stop";
        if !self.link.borrow().registry().position_known(INST) {
            self.lines.line(format_args!(
                "POSITION {when}: the registry holds no position on the market (no resync seeded \
                 it): nothing to compare"
            ));
            return None;
        }
        let mut check = self.positions.take()?;
        let verdict = check.check(when, &self.lines, self.registry_now()).await;
        self.positions = Some(check);
        let failed = verdict.failure(when)?;
        self.note(format_args!("{failed}"));
        Some(failed)
    }

    /// The report of the run.
    fn report(&self, ok: bool, ended: String) -> Report {
        let link = self.link.borrow();
        let reg = link.registry();
        let (places_sent, cancels_sent) = link.sent_counts();
        Report {
            ok,
            ended,
            cycles: self.cycles,
            places_sent,
            cancels_sent,
            cancelled: self.cancelled,
            fills: self.fills.get(),
            inventory: reg.inventory(INST).0,
            requests_held: link.requests_held().max(self.ours.len()),
        }
    }

    /// Waits until `found` finds what it looks for, or `within` passes (`None`), reading the
    /// notes at each wake ([`Driver::absorb`]); `interruptible`: also `None` once the interrupt
    /// came.
    async fn wait_for<T>(
        &mut self,
        within: Duration,
        interruptible: bool,
        mut found: impl FnMut(&mut Driver) -> Option<T>,
    ) -> Option<T> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let wake = Rc::clone(&self.wake);
            // Created before the check, so a wake between the check and the await is kept.
            let woken = wake.notified();
            self.absorb();
            if let Some(t) = found(self) {
                return Some(t);
            }
            if interruptible && self.interrupted.get() {
                return None;
            }
            tokio::select! {
                () = woken => {}
                () = tokio::time::sleep_until(deadline) => return None,
            }
        }
    }

    /// A line of the loop's: its tag, the milliseconds since the start, and `text`.
    fn event(&self, tag: &str, text: std::fmt::Arguments<'_>) {
        let ms = self.lines.t0.elapsed().as_millis();
        self.lines.line(format_args!("{tag:<10} +{ms}ms  {text}"));
    }

    fn note(&self, text: std::fmt::Arguments<'_>) {
        self.lines.line(format_args!("NOTE {text}"));
    }

    /// The first reason to Stop, kept.
    fn trouble(&mut self, why: String) {
        self.trouble.get_or_insert(why);
    }

    /// Reads the notes the link appended since the last read, once each: prints what the owner
    /// should see, keeps what the loop needs (outcomes, sends, connection ends), and keeps the
    /// first reason to Stop ([`Driver::trouble`]). Then the orders of ours: one gone Unknown
    /// Stops the loop, as does an order not ours in view on the market.
    fn absorb(&mut self) {
        let notes: Vec<Note> = {
            let mut link = self.link.borrow_mut();
            let read = link.notes().len();
            link.forget(read)
        };
        for note in notes {
            self.absorb_one(note);
        }
        self.prune();
        let unknown: Vec<ClientOrderId> = {
            let link = self.link.borrow();
            let reg = link.registry();
            self.owned
                .iter()
                .copied()
                .filter(|cid| {
                    reg.get(*cid).is_some_and(|r| {
                        r.state() == OrdState::Unknown || r.unknown_since().is_some()
                    })
                })
                .collect()
        };
        if let Some(cid) = unknown.first() {
            self.trouble(format!(
                "an order of ours (client id sequence {}) went Unknown: the loop does not drive \
                 the Unknown ladder (no Paradex order query is wired here)",
                cid.seq()
            ));
        }
        if self.link.borrow().registry().foreign_in_view(INST) {
            self.trouble("an order not ours is in view on the market".to_owned());
        }
    }

    fn absorb_one(&mut self, note: Note) {
        match note {
            Note::Outcome {
                rpc,
                item,
                outcome,
                ours: true,
            } => self.on_outcome(rpc, item, outcome),
            Note::Outcome {
                rpc,
                outcome,
                ours: false,
                ..
            } => {
                if self.released.answer(rpc) == Unheld::Late {
                    // A place's Accepted{Final} after its provisional answer: the registry has
                    // it from the order event. Anything else is printed (RB-elg7-5).
                    if !matches!(outcome, SubmitOutcome::Accepted { .. }) {
                        self.note(format_args!(
                            "a later answer to request {}, released once its first answer \
                             came: {outcome:?}",
                            rpc.0
                        ));
                    }
                } else if matches!(outcome, SubmitOutcome::Accepted { .. }) {
                    self.armed += 1;
                } else {
                    self.note(format_args!(
                        "the session's own request (its cancel-on-disconnect arm) came back \
                         {outcome:?}"
                    ));
                }
            }
            Note::Submitted { rpc, sent } => {
                self.submitted.insert(rpc);
                if let Err(reason) = sent {
                    self.note(format_args!("request {} was not sent: {reason:?}", rpc.0));
                    let answers = self.answers.entry(rpc).or_default();
                    answers.push((None, SubmitOutcome::NotSent(reason)));
                }
            }
            Note::Order(Routed::Ours(cid, Applied::Amended)) => self.trouble(format!(
                "an order of ours (client id sequence {}) was amended, which this loop never \
                 does: another process operated on it",
                cid.seq()
            )),
            Note::Order(Routed::Ours(cid, _)) => self.prune_one(cid),
            Note::Order(routed) => self.trouble(format!(
                "an order event of an order not ours came in ({routed:?}): another trader \
                 operates on the account"
            )),
            Note::Orphan { cid, .. } => self.trouble(format!(
                "an order in our namespace that the registry does not hold was reported (client \
                 id sequence {})",
                cid.seq()
            )),
            Note::Fill {
                seen,
                unexplained,
                ours,
            } => {
                // The first fill seen: GET /positions is compared before the next plan.
                if !self.fill_checked {
                    self.fill_check_due = true;
                }
                if unexplained {
                    self.trouble(format!("a fill not of our orders came in: {seen}"));
                } else if !ours {
                    // Held by the ledger (a duplicate, or before its watermark): not applied.
                    self.note(format_args!("a fill not applied: {seen}"));
                } else {
                    self.fills.set(self.fills.get() + 1);
                    let inventory = self.link.borrow().registry().inventory(INST).0;
                    self.event(
                        "FILL",
                        format_args!("{seen}; inventory now {inventory} lots"),
                    );
                }
            }
            Note::Position { inst, qty } => {
                if inst == INST {
                    self.venue_position = Some(qty);
                }
                self.note(format_args!(
                    "the account's position in instrument {} is {} lots",
                    inst.get(),
                    qty.0
                ));
            }
            Note::Resynced { report, snapshot } => {
                // The resync's position supersedes any reported before it.
                self.venue_position = None;
                self.resync_due = false;
                let ours: Vec<ClientOrderId> = snapshot
                    .orders
                    .iter()
                    .filter(|o| o.inst == INST)
                    .filter_map(|o| match o.cid {
                        Some(CidMatch::Ours(cid)) => Some(cid),
                        _ => None,
                    })
                    .collect();
                self.own(&ours);
                let not_ours = snapshot
                    .orders
                    .iter()
                    .filter(|o| o.inst == INST && !matches!(o.cid, Some(CidMatch::Ours(_))))
                    .count();
                let position = snapshot
                    .positions
                    .iter()
                    .find(|(inst, _)| *inst == INST)
                    .map_or(SignedLots(0), |(_, qty)| *qty);
                self.resynced = Some((
                    snapshot_max(&snapshot),
                    snapshot.orders.len(),
                    not_ours,
                    position,
                ));
                let wrong = resync_disagreements(&[Note::Resynced {
                    report: report.clone(),
                    snapshot,
                }]);
                if !wrong.is_empty() {
                    self.trouble(format!(
                        "a resync found the position other than the registry's: {}",
                        wrong.join("; ")
                    ));
                }
                if let Some((cid, _)) = report.untracked.first() {
                    self.trouble(format!(
                        "a resync showed an order in our namespace that the registry does not \
                         hold (client id sequence {})",
                        cid.seq()
                    ));
                }
            }
            Note::ResyncRefused(why) => {
                self.resync_due = false;
                self.trouble(format!("a resync was refused: {why}"));
            }
            Note::AsyncReject { target, op, reject } => {
                let what = async_reject(op, &reject);
                // One of our own post-only places the venue would not rest because the touch
                // moved through it: an ordinary end of a quote, its side left out of the cycle
                // and placed again the next (RB-elg7-1 on PR #149). Anything else Stops.
                let ours = target
                    .client()
                    .filter(|cid| self.link.borrow().registry().get(*cid).is_some());
                match ours {
                    Some(cid)
                        if op == OpKind::Place && reject.kind == RejectKind::PostOnlyWouldCross =>
                    {
                        self.note(format_args!(
                            "the {} #{} was not rested: {what}; its side is left out of this \
                             cycle and placed again the next",
                            side_name(self.side_of(cid)),
                            cid.seq()
                        ));
                    }
                    _ => self.trouble(format!("the link noted a problem: {what}")),
                }
            }
            Note::Problem(what) => self.trouble(format!("the link noted a problem: {what}")),
            Note::LoginRefused(why) => self.trouble(format!("a login was refused: {why}")),
            Note::EpochEnd { key, why } => {
                self.epochs += 1;
                self.resync_due = true;
                self.note(format_args!(
                    "the order socket's connection {key:?} ended: {why}; the session \
                     reconnects, logs in, arms and resyncs"
                ));
            }
            Note::Unprotected { told, rpcs, left } => {
                let cids: Vec<ClientOrderId> = told.iter().map(|(cid, _)| *cid).collect();
                self.own(&cids);
                for rpc in &rpcs {
                    let named = self.link.borrow().cids_of(*rpc);
                    let request = Request {
                        place: false,
                        first: named.first().map(|cid| (self.side_of(*cid), *cid)),
                        items: named.len().max(1),
                    };
                    self.ours.insert(*rpc, request);
                }
                let requests: Vec<String> = rpcs.iter().map(|r| r.0.to_string()).collect();
                self.note(format_args!(
                    "{} orders of ours rest from an earlier connection or run, which this \
                     connection's cancel-on-disconnect may not cover (decision 0080): Safety \
                     cancel requests [{}] sent for them at once",
                    told.len(),
                    requests.join(", ")
                ));
                if !left.is_empty() {
                    self.trouble(format!(
                        "orders of ours told unprotected were not cancelled: {}",
                        left.join("; ")
                    ));
                }
            }
            Note::Conn(ConnState::Authenticated) => self.authenticated = true,
            Note::Conn(_) => {}
        }
    }

    /// Adds `cids` to the orders of ours the run knows.
    fn own(&mut self, cids: &[ClientOrderId]) {
        for cid in cids {
            if !self.owned.contains(cid) {
                self.owned.push(*cid);
            }
        }
    }

    /// The side of our order `cid` (a buy when the registry does not hold it).
    fn side_of(&self, cid: ClientOrderId) -> Side {
        let link = self.link.borrow();
        link.registry()
            .get(cid)
            .map_or(Side::Buy, |r| r.placed().side)
    }

    /// An answer to request `rpc` of ours (item `item`, or the whole request): kept for
    /// [`Driver::answer`], an Unknown one a reason to Stop, the first printed, and any not
    /// accepted noted.
    fn on_outcome(&mut self, rpc: RpcId, item: Option<u16>, outcome: SubmitOutcome) {
        if outcome == SubmitOutcome::Unknown {
            self.trouble(format!(
                "request {} came back Unknown: the loop does not drive the Unknown ladder (no \
                 Paradex order query is wired here)",
                rpc.0
            ));
        }
        let answers = self.answers.entry(rpc).or_default();
        let first = answers.is_empty();
        answers.push((item, outcome.clone()));
        let request = self.ours.get(&rpc).copied();
        let (place, named) = match request {
            Some(Request {
                place,
                first: Some((side, cid)),
                ..
            }) => (place, format!("{} #{}", side_name(side), cid.seq())),
            Some(Request { place, .. }) => (place, "every order of ours".to_owned()),
            None => return,
        };
        match (&outcome, place) {
            (SubmitOutcome::Accepted { ack }, true) if first => {
                let vid = request
                    .and_then(|r| r.first)
                    .map_or_else(|| "none".to_owned(), |(_, cid)| self.vid_of(cid));
                self.event(
                    "ACK",
                    format_args!("{named} accepted ({ack:?}), venue order id {vid}"),
                );
            }
            (SubmitOutcome::Accepted { ack }, false) if first => self.event(
                "CANCEL-ACK",
                format_args!("{named} accepted ({ack:?}): queued for cancellation"),
            ),
            (SubmitOutcome::Accepted { .. }, _) => {}
            (other, true) => self.note(format_args!("the place {named} came back {other:?}")),
            (other, false) => self.note(format_args!(
                "request {}'s cancel (item {}) came back {other:?}",
                rpc.0,
                item.map_or_else(|| "all".to_owned(), |i| i.to_string())
            )),
        }
    }

    /// What came of request `rpc` of ours once every item is answered: the whole request's
    /// answer, or, item by item, the first not accepted, else accepted. `None` while an item is
    /// unanswered, so a batch is never judged by its first item alone.
    fn answer(&self, rpc: RpcId) -> Option<SubmitOutcome> {
        let answers = self.answers.get(&rpc)?;
        if let Some((_, whole)) = answers.iter().find(|(item, _)| item.is_none()) {
            return Some(whole.clone());
        }
        let items = self.ours.get(&rpc).map_or(1, |r| r.items);
        let mut by_item: Vec<(u16, &SubmitOutcome)> = Vec::new();
        for (item, outcome) in answers {
            if let Some(i) = item
                && !by_item.iter().any(|(seen, _)| seen == i)
            {
                by_item.push((*i, outcome));
            }
        }
        if by_item.len() < items {
            return None;
        }
        by_item.sort_by_key(|(i, _)| *i);
        let refused = by_item
            .iter()
            .find(|(_, o)| !matches!(o, SubmitOutcome::Accepted { .. }));
        Some(refused.unwrap_or(&by_item[0]).1.clone())
    }

    /// The orders of ours that ended leave [`Driver::owned`] ([`Driver::prune_one`]).
    fn prune(&mut self) {
        for cid in self.owned.clone() {
            self.prune_one(cid);
        }
    }

    /// Our order `cid` leaves [`Driver::owned`] once it ended, its end printed once (or once the
    /// registry does not hold it, which a resync already reported).
    fn prune_one(&mut self, cid: ClientOrderId) {
        if !self.owned.contains(&cid) {
            return;
        }
        let end = {
            let link = self.link.borrow();
            match link.registry().get(cid) {
                None => None,
                Some(r) => match r.state() {
                    OrdState::Terminal(kind) => Some((
                        kind,
                        r.filled(),
                        r.placed().side,
                        r.cum_venue() > r.cum_fills(),
                    )),
                    _ => return,
                },
            }
        };
        self.owned.retain(|c| *c != cid);
        if end.is_some_and(|(.., behind)| behind) {
            self.unfilled.push(cid);
        }
        let end = end.map(|(kind, filled, side, _)| (kind, filled, side));
        let Some((kind, filled, side)) = end else {
            return;
        };
        if matches!(kind, fbc_oms::TerminalKind::Canceled(_)) {
            self.cancelled += 1;
        }
        self.event(
            "CLOSED",
            format_args!(
                "{} #{} ended {kind:?}, {} lots of it filled",
                side_name(side),
                cid.seq(),
                filled.get()
            ),
        );
    }

    /// Drops what the loop kept of the requests that were sent and answered: nothing waits on
    /// them any more (called between cycles), so the maps hold what is in flight.
    fn compact(&mut self) {
        let gone: Vec<RpcId> = self
            .answers
            .keys()
            .copied()
            .filter(|rpc| self.submitted.contains(rpc) && self.answer(*rpc).is_some())
            .collect();
        for rpc in gone {
            self.answers.remove(&rpc);
            self.submitted.remove(&rpc);
            self.ours.remove(&rpc);
            self.link.borrow_mut().release(rpc);
            self.released.release(rpc);
        }
    }

    /// The venue order id of `cid`, or `none`.
    fn vid_of(&self, cid: ClientOrderId) -> String {
        let link = self.link.borrow();
        let vid = link
            .registry()
            .get(cid)
            .and_then(|r| r.vid().map(|v| v.as_str().to_owned()));
        vid.unwrap_or_else(|| "none".to_owned())
    }

    /// Login, arm, resync, seed, Start, the start's sweep, then the cycles until the loop ends
    /// as asked (`Ok`, why) or must Stop (`Err`, why).
    async fn quote(
        &mut self,
        ns_lease: NamespaceLease,
        persisted: u64,
        leases: Leases,
    ) -> Result<String, String> {
        let timeout = self.timeout;
        let login = self
            .wait_for(timeout, true, |d| {
                (d.authenticated || d.trouble.is_some()).then_some(())
            })
            .await;
        self.ended_unless(login, "login", "the login and the socket's authentication")?;
        self.lines.step(
            "login",
            format_args!("logged in over REST; the order socket is authenticated"),
        );
        let arm = self
            .wait_for(timeout, true, |d| {
                (d.armed > 0 || d.trouble.is_some()).then_some(())
            })
            .await;
        self.ended_unless(arm, "arm", "Paradex's acceptance of cancel-on-disconnect")?;
        self.lines.step(
            "arm",
            format_args!("cancel-on-disconnect accepted: a dropped socket cancels our orders"),
        );
        let orders = self.orders.clone();
        let resynced = self
            .wait_for(timeout, true, |d| {
                if d.trouble.is_some() {
                    return Some(None);
                }
                (orders.may_place() && d.resynced.is_some()).then_some(d.resynced)
            })
            .await;
        let resynced = self.ended_unless(
            resynced,
            "resync",
            "the REST resync (an open order or position on another market refuses it whole)",
        )?;
        let Some((ours_max, open, not_ours, position)) = resynced else {
            return Err(self.trouble.clone().unwrap_or_default());
        };
        self.lines.step(
            "resync",
            format_args!(
                "{open} open orders, position {} lots; the snapshot is trustworthy \
                 (decision 0094)",
                position.0
            ),
        );
        if not_ours > 0 {
            return Err(format!(
                "{not_ours} open orders on the market are not ours (another namespace's, a \
                 non-canonical client id's, or one with none): Stop could not cancel them nor \
                 the loop count their fills, so it is refused before Start; cancel them first"
            ));
        }
        {
            let mut link = self.link.borrow_mut();
            let reg = link.reg();
            if reg.position_known(INST) {
                self.lines.line(format_args!(
                    "SEED position {} lots seeded by the trustworthy resync (decision 0094)",
                    reg.inventory(INST).0
                ));
            } else {
                reg.seed_position(INST, position)
                    .map_err(|e| format!("the hand seed was refused: {e:?}"))?;
                self.lines.line(format_args!(
                    "SEED position {} lots seeded by hand from the resync's REST position: an \
                     owner-assisted testnet run (decision 0067)",
                    position.0
                ));
            }
            let entry = reg
                .start(INST, leases)
                .map_err(|e| format!("Start was refused: {e:?}"))?;
            self.lines.step(
                "start",
                format_args!(
                    "armed: {:?}, generation {}",
                    entry.state(),
                    entry.generation().get()
                ),
            );
        }
        self.mint = Some(CidMint::new(ns_lease, persisted, ours_max, wall_now()));
        self.sweep("START").await?;
        self.swept = self.epochs;
        self.cycles().await
    }

    /// `found`, or why the step did not happen: what the notes showed, the interrupt, or a
    /// timeout (printed).
    fn ended_unless<T>(&self, found: Option<T>, step: &str, what: &str) -> Result<T, String> {
        if let Some(why) = &self.trouble {
            return Err(why.clone());
        }
        if self.interrupted.get() {
            return Err("interrupted (Ctrl-C)".to_owned());
        }
        found.ok_or_else(|| {
            let ms = self.lines.t0.elapsed().as_millis();
            self.lines.line(format_args!(
                "TIMEOUT {step} +{ms}ms  {what} did not happen within {} s; stopping",
                self.timeout.as_secs()
            ));
            format!("{what} did not happen in time")
        })
    }

    /// The requote cycles: until the interrupt, `--duration` or `--cycles` (`Ok`), or anything
    /// that Stops the loop (`Err`).
    async fn cycles(&mut self) -> Result<String, String> {
        loop {
            self.absorb();
            if let Some(why) = &self.trouble {
                return Err(why.clone());
            }
            if self.interrupted.get() {
                return Ok("interrupted (Ctrl-C)".to_owned());
            }
            if tokio::time::Instant::now() >= self.deadline {
                return Ok(format!("--duration {} s reached", self.opts.duration_secs));
            }
            if self.opts.cycles.is_some_and(|n| self.cycles >= n) {
                return Ok(format!("--cycles {} reached", self.cycles));
            }
            if self.epochs != self.swept {
                self.reconnect().await?;
                continue;
            }
            self.compact();
            // The inventory before the touch is read: a fill while it is in flight makes the
            // first pass read the touch again (Codex r4244145956).
            let read_at = self.link.borrow().registry().inventory(INST);
            // Raced against the interrupt, a reason to Stop, a connection's end and
            // --duration, each taken at the top of the loop (Codex r4243989987).
            let Some(book) = self.touch_unless_ended(self.timeout).await? else {
                continue;
            };
            match self.settle(book, read_at).await? {
                Settled::Done => self.cycles += 1,
                Settled::Reconnected | Settled::Interrupted | Settled::Ended => continue,
            }
            self.pause().await;
        }
    }

    /// Whether the cycle must end before anything more is handed over: the interrupt came, a
    /// reason to Stop was noted, a connection ended, or `--duration` passed.
    fn ending(&self) -> bool {
        self.trouble.is_some()
            || self.interrupted.get()
            || self.epochs != self.swept
            || tokio::time::Instant::now() >= self.deadline
    }

    /// The touch (`GET /orderbook`), read as the notes come: `None` once the cycle must end
    /// ([`Driver::ending`]) before it answers, so neither Ctrl-C, a reason to Stop, a
    /// connection's end nor `--duration` waits up to `--step-timeout` for it while the quotes
    /// rest (Codex r4243989987 on PR #149).
    ///
    /// The read waits at most `within`. A book older than one already read (a lower `seq_no`:
    /// a stale cache or replica) is a reason to Stop, never quoted from (Codex r4244080123).
    async fn touch_unless_ended(
        &mut self,
        within: Duration,
    ) -> Result<Option<OrderbookSnapshot>, String> {
        let (rest_url, market) = (self.opts.rest_url.clone(), self.opts.market.clone());
        let (proxy, specs) = (self.opts.proxy.clone(), self.specs.clone());
        let read = touch(&rest_url, &market, &proxy, within, &specs);
        tokio::pin!(read);
        loop {
            let wake = Rc::clone(&self.wake);
            let woken = wake.notified();
            self.absorb();
            if self.ending() {
                return Ok(None);
            }
            tokio::select! {
                book = &mut read => {
                    let book = book?;
                    if book.seq_no < self.book_seq {
                        return Err(format!(
                            "GET /orderbook answered seq {} after seq {}: an order book older \
                             than one already read is never quoted from",
                            book.seq_no, self.book_seq
                        ));
                    }
                    self.book_seq = book.seq_no;
                    return Ok(Some(book));
                }
                () = woken => {}
                () = tokio::time::sleep_until(self.deadline) => return Ok(None),
            }
        }
    }

    /// Prints the cycle when its quotes or skipped sides differ from the last cycle's.
    fn print_cycle(&mut self, book: &OrderbookSnapshot, wanted: &Wanted) {
        let now = (wanted.bid, wanted.ask, wanted.skipped.clone());
        if self.last.as_ref() == Some(&now) {
            return;
        }
        let (bid, ask) = touch_px(&self.opts, book);
        let quote = |t: Option<Ticks>| {
            t.map_or_else(
                || "none".to_owned(),
                |t| format!("{} lots @ {}", self.qty.get(), px_of(&self.opts, t)),
            )
        };
        self.lines.line(format_args!(
            "CYCLE {} touch bid {} ask {} (GET /orderbook seq {}): bid {}, ask {}",
            self.cycles + 1,
            px_or_none(bid),
            px_or_none(ask),
            book.seq_no,
            quote(wanted.bid),
            quote(wanted.ask)
        ));
        for skipped in &wanted.skipped {
            self.lines.line(format_args!("SKIP {skipped}"));
        }
        self.last = Some(now);
    }

    /// The pause between cycles: the interval, ended early by the interrupt, `--duration`, a
    /// reason to Stop or a connection's end, read as the notes come.
    async fn pause(&mut self) {
        let until = std::cmp::min(
            tokio::time::Instant::now() + Duration::from_millis(self.opts.interval_ms),
            self.deadline,
        );
        loop {
            let wake = Rc::clone(&self.wake);
            let woken = wake.notified();
            self.absorb();
            if self.interrupted.get()
                || self.trouble.is_some()
                || self.epochs != self.swept
                || tokio::time::Instant::now() >= until
            {
                return;
            }
            tokio::select! {
                () = woken => {}
                () = tokio::time::sleep_until(until) => return,
            }
        }
    }

    /// Keeps a high-water mark [`RESERVE`] ids above the mint before a pass may mint past the one
    /// kept: written and synced off the runtime's thread, the mint (and its namespace lease)
    /// held until the write ends even past the timeout. The write is raced against what ends a
    /// cycle ([`Driver::ending`], Codex r4244145961): `Ok(false)` when it came first. The write
    /// is then kept, holding the mint, and the next pass (after a reconnect's sweep) waits for
    /// it and takes the mint back, so a connection's end never loses it and no second write of
    /// the mark starts beside it (Codex r4244206466); when the run ends instead, the mint is
    /// dropped on the write's thread once the write ends (nothing is minted after).
    async fn reserve(&mut self) -> Result<bool, String> {
        let (mark, mut write) = match self.writing.take() {
            Some(writing) => writing,
            None => {
                let high = self.mint.as_ref().map_or(0, CidMint::high_water);
                // Near u64::MAX no mark can be kept above the mint: refused, the mark kept
                // unchanged, never wrapped (Codex r4244080134).
                let (Some(next), Some(mark)) = (high.checked_add(2), high.checked_add(RESERVE))
                else {
                    return Err(format!(
                        "the client-id sequence space is exhausted: no high-water mark \
                         {RESERVE} ids above {high} can be kept, so nothing more is minted"
                    ));
                };
                if next <= self.reserved {
                    return Ok(true);
                }
                let hwm = self.hwm.clone();
                let mint = self.mint.take();
                let write: MarkWrite =
                    Box::pin(off_thread_holding(self.timeout, mint, move || {
                        hwm.write(mark)
                    }));
                (mark, write)
            }
        };
        let written = loop {
            let wake = Rc::clone(&self.wake);
            let woken = wake.notified();
            self.absorb();
            if self.ending() {
                self.writing = Some((mark, write));
                return Ok(false);
            }
            tokio::select! {
                written = &mut write => break written,
                () = woken => {}
                () = tokio::time::sleep_until(self.deadline) => {
                    self.writing = Some((mark, write));
                    return Ok(false);
                }
            }
        };
        match written {
            Some((kept, mint)) => {
                self.mint = mint;
                kept?;
                self.reserved = mark;
                self.lines.line(format_args!(
                    "MARK client-id high-water mark {mark} kept in the lease directory"
                ));
                Ok(true)
            }
            None => Err("the high-water mark's write did not finish in time".to_owned()),
        }
    }

    /// Drives the planner to the desired book at the touch `book`: a pass, its commands handed
    /// to the session, then the next pass at the next wake, until a pass has nothing to send,
    /// hold or await. Each pass judges the quotes again ([`desired`]) at the position as it is
    /// then, and at a fresh touch once a fill moved it since the touch was read (Codex
    /// r4243853260): a replacement is never placed on a position the cycle did not see. The
    /// notes are read again after the pass's waits and before it plans ([`Driver::halted`]), so
    /// nothing noted during them (the interrupt, a resync mismatch, a connection's end) lets a
    /// pass place (Codex r4243989985). A side
    /// whose order placed this cycle ended without a cancel of ours (a post-only order the venue
    /// would not rest, a fill) is not placed again before the next cycle; a place fbc-oms's caps
    /// refuse leaves its side out of the cycle.
    async fn settle(
        &mut self,
        mut book: OrderbookSnapshot,
        mut read_at: SignedLots,
    ) -> Result<Settled, String> {
        let mut deadline = tokio::time::Instant::now() + self.timeout;
        let mut left_out: Vec<Side> = Vec::new();
        let mut placed: Vec<(Side, ClientOrderId)> = Vec::new();
        let mut holding = false;
        // A check ran since the touch was read: the touch is read again before anything is
        // planned.
        let mut aged = false;
        loop {
            let wake = Rc::clone(&self.wake);
            let woken = wake.notified();
            // After the first fill, GET /positions against the registry before anything is
            // planned (FBC-hpg6): its reads (and a re-read's wait) are not the book's settling
            // time, which starts again, and the touch held is as old as the check, so the pass
            // reads a fresh one before it plans (RA157-2 / RB157-1 on PR #157): no quote is
            // priced from a book read before the check.
            if self.check_after_first_fill().await {
                deadline = tokio::time::Instant::now() + self.timeout;
                aged = true;
            }
            if let Some(ended) = self.halted()? {
                return Ok(ended);
            }
            // The mark's write and a fresh touch for a moved position are this pass's waits;
            // the notes are read again after them, before anything is planned (below).
            let mut cut = !self.reserve().await?;
            let inventory = |d: &Driver| d.link.borrow().registry().inventory(INST);
            let mut position = inventory(self);
            // A fresh touch for a moved position, read again until no fill moved it while the
            // touch was being read (Codex r4243908416), and one after a position check.
            while !cut && (aged || position != read_at) {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                let Some(fresh) = self.touch_unless_ended(left).await? else {
                    cut = true;
                    break;
                };
                book = fresh;
                aged = false;
                read_at = position;
                position = inventory(self);
            }
            // Whatever the notes showed during those waits (the interrupt, a reason to Stop, a
            // connection's end, --duration) is taken now, with nothing awaited between this
            // read and the commands' hand-over: no pass places after any of them, nor before
            // a reconnect's sweep (Codex r4243989985 on PR #149).
            if let Some(ended) = self.halted()? {
                return Ok(ended);
            }
            // A fill read during those waits: its position check comes first, at the top of the
            // pass, before anything is planned.
            if cut || self.fill_check_due {
                continue;
            }
            // The waits may have used the rest of the book's settling time (Codex r4244080131).
            if tokio::time::Instant::now() >= deadline {
                return Err(self.timed_out("settle", "the book settling"));
            }
            // A position the venue reported that the inventory does not hold yet (its fill not
            // read yet, or a change not ours), or an order's fills the venue reported on an
            // order update that the fill stream has not brought yet: nothing is planned until
            // they agree, or Stop at the settling time (Codex r4244080127, r4244145969).
            if let Some(what) = self.unreconciled(position) {
                if !holding {
                    holding = true;
                    self.note(format_args!(
                        "{what} differ: nothing is planned until they agree"
                    ));
                }
                tokio::select! {
                    () = woken => {}
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(self.timed_out("settle", &format!("{what} agreeing")));
                    }
                }
                continue;
            }
            placed.retain(|(side, cid)| {
                let link = self.link.borrow();
                let ended = link
                    .registry()
                    .get(*cid)
                    .filter(|r| r.state().is_terminal() && r.cancels_sent() == 0);
                match ended {
                    Some(r) => {
                        self.lines.line(format_args!(
                            "NOTE the {} #{} placed this cycle ended {:?} without a cancel of \
                             ours: not placed again before the next cycle",
                            side_name(*side),
                            cid.seq(),
                            r.state()
                        ));
                        left_out.push(*side);
                        false
                    }
                    None => true,
                }
            });
            let mut wanted = desired(&self.opts, self.qty, position, &book);
            for side in &left_out {
                wanted.book.remove(*side, 0);
                match side {
                    Side::Buy => wanted.bid = None,
                    Side::Sell => wanted.ask = None,
                }
            }
            self.print_cycle(&book, &wanted);
            let now = self.link.borrow().now().0;
            let plan = {
                let mut link = self.link.borrow_mut();
                let mint = self.mint.as_mut().ok_or("no client-id mint")?;
                self.planner
                    .plan(&wanted.book, link.reg(), &self.plan_caps, ACCT, mint, now)
                    .map_err(|e| format!("the planner refused the pass: {e}"))?
            };
            let mut refused_place = false;
            for refused in &plan.refused {
                match &refused.why {
                    PlanRefusal::Place(e) => {
                        refused_place = true;
                        left_out.push(refused.side);
                        self.note(format_args!(
                            "fbc-oms refused the {} place: {e:?}; that side is left out of this \
                             cycle",
                            side_name(refused.side)
                        ));
                    }
                    other => {
                        return Err(format!(
                            "the planner built nothing for the {} level: {other:?}",
                            side_name(refused.side)
                        ));
                    }
                }
            }
            if plan.commands.is_empty()
                && plan.awaiting_ack.is_empty()
                && plan.held.is_empty()
                && !refused_place
            {
                return Ok(Settled::Done);
            }
            let mut rpcs = Vec::new();
            for planned in plan.commands {
                let place = planned.stage != Stage::Cancel;
                let rpc = self.hand_over(planned.auth, place, planned.side, planned.cid)?;
                if place {
                    placed.push((planned.side, planned.cid));
                }
                rpcs.push(rpc);
            }
            if !rpcs.is_empty() {
                // Each sent before the next pass, so the registry knows its cancels in flight
                // and builds none again.
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                let sent = self
                    .wait_for(left, false, |d| {
                        rpcs.iter().all(|r| d.submitted.contains(r)).then_some(())
                    })
                    .await;
                if sent.is_none() {
                    return Err(self.timed_out("settle", "the session's sends of a pass"));
                }
                continue;
            }
            if refused_place {
                continue;
            }
            tokio::select! {
                () = woken => {}
                () = tokio::time::sleep_until(deadline) => {
                    return Err(self.timed_out(
                        "settle",
                        &format!("the book settling (held: {:?})", plan.held),
                    ));
                }
            }
        }
    }

    /// What the venue reported that the inventory does not hold yet, if anything: a position
    /// other than `position`, or fills of an order of ours that the fill stream has not
    /// brought (its cumulative fill on an order update above the fills read).
    fn unreconciled(&mut self, position: SignedLots) -> Option<String> {
        if let Some(venue) = self.venue_position.filter(|v| *v != position) {
            return Some(format!(
                "the venue's position ({} lots) and the inventory ({} lots)",
                venue.0, position.0
            ));
        }
        let link = Rc::clone(&self.link);
        let link = link.borrow();
        let reg = link.registry();
        let behind =
            |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| r.cum_venue() > r.cum_fills());
        self.unfilled.retain(behind);
        let cid = self
            .owned
            .iter()
            .chain(&self.unfilled)
            .find(|c| behind(c))?;
        let r = reg.get(*cid)?;
        Some(format!(
            "the venue's fills of #{} ({} lots) and the fills read ({} lots)",
            cid.seq(),
            r.cum_venue().get(),
            r.cum_fills().get()
        ))
    }

    /// Reads the notes and says whether a pass must not plan: `Err` for a reason to Stop, or
    /// how the cycle ends (the interrupt, a connection's end or a session that takes no place,
    /// `--duration`).
    fn halted(&mut self) -> Result<Option<Settled>, String> {
        self.absorb();
        if let Some(why) = &self.trouble {
            return Err(why.clone());
        }
        if self.interrupted.get() {
            return Ok(Some(Settled::Interrupted));
        }
        if self.epochs != self.swept || !self.orders.may_place() {
            return Ok(Some(Settled::Reconnected));
        }
        if tokio::time::Instant::now() >= self.deadline {
            return Ok(Some(Settled::Ended));
        }
        Ok(None)
    }

    fn timed_out(&self, step: &str, what: &str) -> String {
        let ms = self.lines.t0.elapsed().as_millis();
        self.lines.line(format_args!(
            "TIMEOUT {step} +{ms}ms  {what} did not happen within {} s; stopping",
            self.timeout.as_secs()
        ));
        format!("{what} did not happen in time")
    }

    /// Authorizes nothing: hands an authorization fbc-oms issued to the session, printing it,
    /// and remembers what the request is.
    fn hand_over(
        &mut self,
        auth: fbc_oms::Authorization,
        place: bool,
        side: Side,
        cid: ClientOrderId,
    ) -> Result<RpcId, String> {
        let shown = match auth.command() {
            fbc_core::VenueCommand::Place(o) => {
                let px = match o.kind {
                    fbc_core::OrderKind::Limit { px } => px_of(&self.opts, px).to_string(),
                    fbc_core::OrderKind::Market => "market".to_owned(),
                };
                format!(
                    "{} lots ({}) @ {px} post-only",
                    o.qty.get(),
                    Decimal::from(o.qty.get()) * self.opts.step
                )
            }
            _ => String::new(),
        };
        let rpc = {
            let mut link = self.link.borrow_mut();
            link.submit(&self.orders, auth)
                .map_err(|e| format!("the session refused a command: {e}"))?
        };
        let request = Request {
            place,
            first: Some((side, cid)),
            items: 1,
        };
        self.ours.insert(rpc, request);
        if place {
            self.own(&[cid]);
            self.event(
                "PLACE",
                format_args!(
                    "{} #{} {shown} (request {})",
                    side_name(side),
                    cid.seq(),
                    rpc.0
                ),
            );
        } else {
            self.event(
                "CANCEL",
                format_args!("{} #{} (request {})", side_name(side), cid.seq(), rpc.0),
            );
        }
        Ok(rpc)
    }

    /// After a reconnect: waits for the new connection's arm and resync (the session takes
    /// places again), then [`Driver::sweep`]s.
    async fn reconnect(&mut self) -> Result<(), String> {
        let orders = self.orders.clone();
        let timeout = self.timeout;
        let ready = self
            .wait_for(timeout, true, |d| {
                (d.trouble.is_some() || (!d.resync_due && orders.may_place())).then_some(())
            })
            .await;
        self.ended_unless(ready, "reconnect", "the new connection's arm and resync")?;
        let epochs = self.epochs;
        self.sweep("RECONNECT").await?;
        self.swept = epochs;
        Ok(())
    }

    /// Cancels every order of ours still open on the market and waits until all of them ended
    /// and the session takes places on the market, before anything is placed (module
    /// documentation).
    async fn sweep(&mut self, label: &str) -> Result<(), String> {
        let (rpcs, how) = self.cancel_ours()?;
        self.lines.line(format_args!(
            "{label} every order of ours still open on the market is cancelled before any place: \
             {} cancels sent ({how})",
            rpcs.len()
        ));
        let orders = self.orders.clone();
        let timeout = self.timeout;
        let done = self
            .wait_for(timeout, true, |d| {
                if d.trouble.is_some() {
                    return Some(());
                }
                d.cancel_due();
                (d.all_ended() && orders.may_place_on(INST)).then_some(())
            })
            .await;
        self.ended_unless(
            done,
            "sweep",
            "the end of every order of ours on the market",
        )?;
        Ok(())
    }

    /// Whether every order of ours the run knows ended ([`Driver::prune`] then holds none).
    fn all_ended(&self) -> bool {
        let link = self.link.borrow();
        let reg = link.registry();
        self.owned
            .iter()
            .all(|cid| reg.get(*cid).is_none_or(|r| r.state().is_terminal()))
    }

    /// The cancels of every order of ours still open on the market that has none in flight,
    /// handed to the session: an instrument cancel-all when fbc-oms builds one (0005's I7),
    /// otherwise explicit cancels. The requests, and how it was done.
    fn cancel_ours(&mut self) -> Result<(Vec<RpcId>, String), String> {
        let (cmds, how) = {
            let mut link = self.link.borrow_mut();
            let caps = self.caps.clone();
            let reg = link.reg();
            match reg.cancel_everything(INST, &caps) {
                CancelEverything::CancelAll {
                    command,
                    unanswered,
                } => {
                    let mut cmds = vec![command];
                    cmds.extend(unanswered.commands);
                    (cmds, "an instrument cancel-all".to_owned())
                }
                CancelEverything::Explicit { why, .. } => {
                    let open: Vec<ClientOrderId> = self
                        .owned
                        .iter()
                        .copied()
                        .filter(|cid| {
                            // One whose cancel is in flight is left to it, unless it went
                            // Unknown (a cancel answered so): cancelled again.
                            reg.get(*cid).is_some_and(|r| {
                                let unknown =
                                    r.state() == OrdState::Unknown || r.unknown_since().is_some();
                                !r.state().is_terminal()
                                    && !r.cancel_awaits_ack()
                                    && (r.intent() == Intent::None || unknown)
                            })
                        })
                        .collect();
                    let plan = reg.cancel_many(&open, &caps);
                    if let Some((cid, refusal)) = plan.refused.first() {
                        return Err(format!(
                            "no cancel could be built for client id sequence {}: {refusal:?}",
                            cid.seq()
                        ));
                    }
                    let how = format!(
                        "explicit cancels of our orders with none in flight or gone Unknown, no \
                         instrument cancel-all: {why}"
                    );
                    (plan.commands, how)
                }
            }
        };
        let mut rpcs = Vec::new();
        for cmd in cmds {
            rpcs.push(self.submit_cancel(cmd)?);
        }
        Ok((rpcs, how))
    }

    /// Authorizes and hands over one cancel fbc-oms built, printing the orders it names.
    fn submit_cancel(&mut self, cmd: PermittedCommand) -> Result<RpcId, String> {
        let auth = {
            let mut link = self.link.borrow_mut();
            link.reg()
                .authorize(ACCT, cmd)
                .map_err(|e| format!("a cancel was not authorized: {e:?}"))?
        };
        let cids: Vec<ClientOrderId> = {
            let link = self.link.borrow();
            let reg = link.registry();
            let of = |c: &fbc_core::CancelOrder| {
                c.target
                    .client()
                    .or_else(|| c.target.venue().and_then(|v| reg.cid_of(v)))
            };
            match auth.command() {
                fbc_core::VenueCommand::Cancel(c) => of(c).into_iter().collect(),
                fbc_core::VenueCommand::CancelMany(cs) => cs.iter().filter_map(of).collect(),
                _ => Vec::new(),
            }
        };
        let rpc = {
            let mut link = self.link.borrow_mut();
            link.submit(&self.orders, auth)
                .map_err(|e| format!("the session refused a command: {e}"))?
        };
        let named: Vec<String> = cids
            .iter()
            .map(|cid| format!("{} #{}", side_name(self.side_of(*cid)), cid.seq()))
            .collect();
        let request = Request {
            place: false,
            first: cids.first().map(|cid| (self.side_of(*cid), *cid)),
            items: cids.len().max(1),
        };
        self.ours.insert(rpc, request);
        let what = if named.is_empty() {
            "every order of ours on the market (an instrument cancel-all)".to_owned()
        } else {
            named.join(", ")
        };
        self.event("CANCEL", format_args!("{what} (request {})", rpc.0));
        Ok(rpc)
    }

    /// Sends the cancels that waited for their order's acknowledgement once it landed
    /// ([`Registry::cancels_due`]): an order of ours placed and not yet acknowledged when a
    /// sweep or Stop came.
    fn cancel_due(&mut self) -> Vec<RpcId> {
        let mut rpcs = Vec::new();
        let due = {
            let link = self.link.borrow();
            link.registry().cancels_due(&self.caps)
        };
        for cid in due {
            let built = {
                let mut link = self.link.borrow_mut();
                let caps = self.caps.clone();
                link.reg().cancellable(cid).map(|p| p.cancel(&caps))
            };
            match built {
                Ok(CancelChoice::Send(cmd)) => match self.submit_cancel(cmd) {
                    Ok(rpc) => rpcs.push(rpc),
                    Err(why) => self.trouble(why),
                },
                Ok(CancelChoice::AwaitAck) => {}
                Err(refusal) => self.trouble(format!("no cancel: {refusal:?}")),
            }
        }
        rpcs
    }

    /// Stop: the kill switch, then the cancel of every order of ours on the market
    /// ([`Driver::cancel_ours`]; those not yet acknowledged once they are), each answered, and
    /// every order of ours ended, within the step timeout; then the comparison at Stop
    /// ([`Driver::check_at_stop`]) while the market's lease is held, and then the market is
    /// disarmed and its leases released. True when every cancel was built, sent and accepted
    /// and every order of ours ended; and why the run fails when the comparison's mismatch
    /// persists.
    async fn stop(&mut self) -> (bool, Option<String>) {
        self.link.borrow_mut().reg().kill(INST);
        // Stop's waits are not cut short by the interrupt: it is what started them.
        self.trouble = None;
        let (mut rpcs, how, mut ok) = match self.cancel_ours() {
            Ok((rpcs, how)) => (rpcs, how, true),
            Err(why) => {
                self.note(format_args!("{why}"));
                (Vec::new(), "none built".to_owned(), false)
            }
        };
        let timeout = self.timeout;
        let ended = self
            .wait_for(timeout, false, |d| {
                rpcs.extend(d.cancel_due());
                let answered = rpcs.iter().all(|r| d.answer(*r).is_some());
                (answered && d.all_ended()).then_some(())
            })
            .await;
        if ended.is_none() {
            ok = false;
            self.timed_out(
                "stop",
                "the answers to Stop's cancels and the end of every order of ours",
            );
        }
        for rpc in &rpcs {
            match self.answer(*rpc).as_ref() {
                Some(SubmitOutcome::Accepted { .. }) => {}
                Some(other) => {
                    ok = false;
                    self.note(format_args!("Stop's cancel {} came back {other:?}", rpc.0));
                }
                None => ok = false,
            }
        }
        if let Some(why) = self.trouble.take() {
            ok = false;
            self.note(format_args!("during Stop: {why}"));
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
        (ok, compared)
    }
}

/// `bid` for a buy, `ask` for a sell.
fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    }
}
