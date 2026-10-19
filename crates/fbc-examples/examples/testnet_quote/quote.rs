//! testnet_quote's loop: a PLUMBING TEST, NOT A STRATEGY. One market, one post-only order of a
//! fixed size on each side, a fixed offset behind the touch, requoted as the touch moves, through
//! fbc-oms's [`ExecutionPlanner`] and caps and fbc-runtime's order-entry session on Paradex
//! TESTNET, or with `--mainnet` on Paradex MAINNET (below). The offsets are the command line's:
//! nothing here prices from a model.
//!
//! **Mainnet mode** (`--mainnet`, FBC-tyft: the owner's "small and supervised" sessions). The
//! same loop, refused before anything connects unless both `--mainnet` and `--sole-trader` are
//! given, the URLs are exactly mainnet's (or both a loopback stub's), `PARADEX_CHAIN_ID` (when
//! set) is mainnet's, the caps are at most $11 resting per side and $50 inventory, both offsets
//! are at least 20 bps and `--duration` at most an hour ([`crate::args::mainnet_limits`]); the
//! credentials are the mainnet-only variables (`src/auth_mainnet.rs`, [`credentials`]); the
//! venue must declare its snapshot trustworthy ([`crate::mainnet::trusted`]). Then, before the
//! order socket connects, `GET /system/config` must name the mainnet chain and `GET /positions`
//! must show no position on another market (`--sole-trader`). The resync must seed the market:
//! nothing is ever seeded by hand on mainnet, so one that seeds nothing refuses Start. A
//! permanent refusal of a place or a cancel Stops the run and fails it (a person is watching).
//!
//! **The mark.** Paradex judges an order's minimum notional at the MARK price, and refuses one
//! below it only after accepting it (`ORDER_SIZE_BELOW_MIN`, the owner's 2026-10-16 testnet
//! run). Every read of the touch reads the mark with it (`GET /markets/summary`,
//! [`touch_and_mark`]), so the mark a pass judges by is never older than its touch, at least
//! once per cycle; a side whose size at the mark is below the minimum is left out and printed,
//! never sent ([`desired`]), and an asynchronous `ORDER_SIZE_BELOW_MIN` (the adapter's
//! `RejectKind::MinNotional`, decision 0096) is a sizing error that Stops the run. The inventory
//! cap is judged at no less than the mark. Both reads are charged to the session's rate limiter
//! ([`crate::testnet::admit_reads`]).
//!
//! **Refusals of ours.** A place or cancel the venue refuses for its rate (`RateLimited`)
//! paces the loop: nothing more is planned until its retry time, or [`RATE_LIMITED_WAIT`] when
//! the venue states none. A post-only place that would cross leaves its side out of the cycle.
//! Any other refusal of a place or a cancel of ours is permanent and Stops the run, failing it
//! (Codex r4277631500, r4289363361 on PR #149): nothing is placed or cancelled again in a loop.
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
//! **Cancels not ours** (FBC-aoh5, the owner's 2026-10-18 mainnet session: a Java instance on
//! the same account cancelled every order on its ticker it did not place, every 3 s, and the
//! loop just requoted). Paradex does not say who sent a user cancel; the [`Link`] notes an order
//! of ours ended by one that no cancel of ours covers ([`Note::CancelNotOurs`]), and the loop
//! applies the owner's rule ("Warn once, Stop on repeat") through [`CancelsNotOurs`]: those of
//! one action of the other program's are one event (two are one when the venue stamped the
//! second within [`EVENT_SPAN`] of the event's first and its order was placed before the event
//! was seen; a requote cancelled again, or a cancel seconds later of any order, however old, is
//! another); the first prints a `WARN` line and the loop requotes; with
//! `--mainnet`, a second on a later cycle within [`Options::cancel_window`] (30 s) of the first,
//! or one of the loop's requote (the first order it placed on a side after the event before,
//! a place that never rested aside: RB-aoh5-10), or of a replace of it, within the window of that requote's place, however long the cycle that placed it (Codex
//! r4309154208 on PR #159), Stops the run and fails it ("another program cancelled our orders:
//! one quoter per account"); past the window the next one starts a new window; a testnet run
//! only warns. Once one was
//! seen, another program may be on the account, so the run's cancels of everything (a
//! reconnect's sweep, Stop) name our orders explicitly and never send an instrument cancel-all,
//! which would cancel its orders too ([`Driver::cancel_ours`]). The summary counts them.
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
    Bps, Channel, CidMatch, CidMint, ClientOrderId, ConnState, ExchNs, Lots, MarketLease,
    Namespace, NamespaceLease, NonceScope, OpKind, OrderCaps, OrderKindTag, Reject, RejectKind,
    RpcId, Secrets, Side, SignedLots, SpecTable, SubmitOutcome, Ticks, Tif, TifTag, TradingStatus,
    VenueFactory, VenueMode,
};
use fbc_oms::{
    Applied, Batching, CancelChoice, CancelEverything, DesiredBook, DesiredQuote, ExecutionPlanner,
    FillLedger, Intent, LadderConfig, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, OrdState,
    PermittedCommand, PlanRefusal, PlanVenue, PlannerConfig, PreTradeCaps, RateView,
    ReduceFallback, Registry, Routed, Stage, TestnetRun,
};
use fbc_runtime::{Connector, ExecControl, ExecOrders, ExecSession, RateLimiter};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::md::rest::OrderbookSnapshot;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Notify;

use crate::args::{Options, confirmed, hosts, testnet_guard};
use crate::link::{Link, LinkHandler, Note, async_reject, described};
use crate::mainnet::{MAINNET_CHAIN, chain_refusal, decode_chain, mainnet_guard, trusted};
use crate::positions::{PositionCheck, Verdict, fills_in};
use crate::testnet::{
    ACCOUNT_LABEL, ACCT, HighWater, INST, Lines, Out, TESTNET_CHAIN, VENUE, admit_reads, config,
    drive_with, inventory_price, off_thread_holding, px_or_none, rest_get, resync_disagreements,
    session_config, snapshot_max, specs, touch_and_mark, wall_now,
};

/// The program the refusals name.
pub const PROGRAM: &str = "testnet_quote";

/// How long a request of ours the venue refused for its rate (`RateLimited`) holds the loop
/// when the venue states no retry time (Paradex states none for its code 42901): twice the
/// window of its tightest limit (the per-account order limit's one second). Nothing more is
/// planned before it passes.
pub const RATE_LIMITED_WAIT: Duration = Duration::from_secs(2);

/// How long a pass waits before reading the touch again when `GET /orderbook` answered a book no
/// newer than the one it held after a fill moved the position (a stale cache or replica, Codex
/// r4289363354 on PR #149): within the book's settling time.
pub const STALE_BOOK_PAUSE: Duration = Duration::from_millis(100);

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
    /// Orders of ours ended by a cancel this program did not send (FBC-aoh5).
    pub cancels_not_ours: usize,
}

/// How far after an event's first cancel not ours, by the venue's times, another is still the
/// same event: one reconcile, or one cancel-all in the venue's UI, ends what rested at nearly one
/// venue time, and a second action comes later (Java's orphan reconcile runs every 3 s; the
/// loop's cycle is 1 s by default).
pub const EVENT_SPAN: Duration = Duration::from_secs(1);

/// What a cancel not ours is to the owner's rule ([`CancelsNotOurs::on_cancel`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NotOurs {
    /// Of an order placed before the event before it was seen, stamped by the venue within
    /// [`EVENT_SPAN`] of that event's first: the same event (one reconcile, or one cancel-all in
    /// the venue's UI, cancels what rested when it ran: both sides).
    SameEvent,
    /// A new event with no other within the window before it: it starts the window (a WARN).
    First,
    /// A new event within the window of the window's first, or of the loop's requote within
    /// the window of its place: a mainnet run Stops.
    Repeat,
}

/// The cancels not ours a run saw and the owner's window over them (module documentation): when
/// the window's first event was seen, and the latest event.
///
/// Two are one event only when the second's order was placed before the first was seen and the
/// venue stamped the second within [`EVENT_SPAN`] of the event's first. The owner's "a second
/// one on a later cycle" is so judged by the venue's times and the order, not by when an order
/// event happens to be read (between two cycles, or during the next: one reconcile's events
/// read apart stay one event): a cancel not ours of a quote the loop placed after it saw one is
/// another action, as is one seconds later of an order that rested since before it (Codex
/// r4308625185, RB-aoh5-1 on PR #159). An event the venue stamped no time on is judged by when
/// it was seen instead.
///
/// A new event is a repeat when it comes within the window of the window's first event, or when
/// it cancels the loop's requote of a side (the first order of ours placed on that side after
/// the latest event was seen, a place refused, not sent or that would cross aside: it never
/// rested, [`Self::unrested`]), or a replace of it, within the window of that requote's place. The second is measured
/// from our place, not from the event before: the loop requotes a side another program
/// cancelled only on the next cycle, and its reads, settle waits and rate-limit waits can make
/// that cycle longer than the window, so by the first measure alone a co-tenant cancelling every
/// requote would only warn each cycle (Codex r4309154208, RB-aoh5-9 on PR #159). A replace of
/// the requote is timed from the requote's place too, so the loop replacing its requote before
/// another program acts keeps the rule (Codex r4310042604). One that cancels a requote or its
/// replace more than the window after the requote's place is judged by the first measure.
#[derive(Clone, Debug)]
pub struct CancelsNotOurs {
    window: Duration,
    orders: usize,
    first: Option<tokio::time::Instant>,
    latest: Option<Event>,
    /// When each order of ours not ended was placed by this run.
    placed: HashMap<ClientOrderId, tokio::time::Instant>,
    /// The loop's requote of each side (bid, ask) since the latest event ([`Requote`]).
    requotes: [Option<Requote>; 2],
}

/// The loop's requote of a side since the latest event: the first order of ours placed on it
/// after that event was seen that did not end unrested ([`CancelsNotOurs::unrested`]), timed
/// from its place, and every order placed on the side since, not ended: our replaces of it
/// (Codex r4310042604 on PR #159), so the loop replacing its requote before another program
/// acts does not take the requote rule away, and a fill of it does not either.
#[derive(Clone, Debug)]
struct Requote {
    since: tokio::time::Instant,
    founder: ClientOrderId,
    chain: Vec<ClientOrderId>,
}

/// The index of `side` in [`CancelsNotOurs::requotes`].
fn side_index(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

/// An event of cancels not ours: when it was seen, and the venue's time on its first cancel.
#[derive(Copy, Clone, Debug)]
struct Event {
    seen: tokio::time::Instant,
    at: Option<ExchNs>,
}

impl Event {
    /// Whether a cancel stamped `at` by the venue, seen at `now`, is within [`EVENT_SPAN`] of
    /// this event's first, by the venue's times when both have one.
    fn spans(&self, at: Option<ExchNs>, now: tokio::time::Instant) -> bool {
        let span = i128::try_from(EVENT_SPAN.as_nanos()).unwrap_or(i128::MAX);
        match (self.at, at) {
            (Some(first), Some(at)) => (i128::from(at.0) - i128::from(first.0)).abs() <= span,
            _ => now.saturating_duration_since(self.seen) <= EVENT_SPAN,
        }
    }
}

impl CancelsNotOurs {
    /// None seen yet; a second within `window` of the first is a repeat.
    pub fn new(window: Duration) -> CancelsNotOurs {
        CancelsNotOurs {
            window,
            orders: 0,
            first: None,
            latest: None,
            placed: HashMap::new(),
            requotes: [None, None],
        }
    }

    /// This run placed our order `cid` on `side` at `now`: the loop's requote of that side when
    /// it is the first placed on it since the latest event, otherwise a replace of it, judged
    /// from the requote's place ([`Requote`]).
    pub fn placed(&mut self, cid: ClientOrderId, side: Side, now: tokio::time::Instant) {
        self.placed.insert(cid, now);
        if self.latest.is_none() {
            return;
        }
        match &mut self.requotes[side_index(side)] {
            Some(requote) => requote.chain.push(cid),
            slot @ None => {
                *slot = Some(Requote {
                    since: now,
                    founder: cid,
                    chain: vec![cid],
                });
            }
        }
    }

    /// Our place `cid` ended without resting: refused (for its rate, or a post-only place that
    /// would cross), or never sent. It held its side's requote only as the first place after
    /// the latest event, and another program cannot cancel it, so the side's requote is the next
    /// order placed on it: the one that rests (RB-aoh5-10 on PR #159). A replace of a requote
    /// that rested frees nothing: the requote stays timed from its place.
    pub fn unrested(&mut self, cid: ClientOrderId) {
        for slot in &mut self.requotes {
            if let Some(requote) = slot {
                requote.chain.retain(|c| *c != cid);
                if requote.founder == cid && requote.chain.is_empty() {
                    *slot = None;
                }
            }
        }
    }

    /// Keeps the placing times of the orders `keep` takes (those not ended).
    pub fn retain(&mut self, keep: impl Fn(&ClientOrderId) -> bool) {
        self.placed.retain(|cid, _| keep(cid));
        for requote in self.requotes.iter_mut().flatten() {
            requote.chain.retain(|cid| keep(cid));
        }
    }

    /// Our order `cid` on `side` (placed by this run, [`Self::placed`], or an earlier run's),
    /// ended by a cancel not ours the venue stamped `at`, seen at `now`: the same event as the
    /// one before it when its order was placed before that one was seen and the venue stamped
    /// it within [`EVENT_SPAN`] of that one's first; otherwise a repeat when within the window
    /// of the window's first event or of its place as the loop's requote of `side` (type
    /// documentation), or the first of a new window.
    pub fn on_cancel(
        &mut self,
        cid: ClientOrderId,
        side: Side,
        at: Option<ExchNs>,
        now: tokio::time::Instant,
    ) -> NotOurs {
        self.orders += 1;
        let placed = self.placed.remove(&cid);
        if let Some(latest) = self.latest
            && placed.is_none_or(|placed| placed <= latest.seen)
            && latest.spans(at, now)
        {
            return NotOurs::SameEvent;
        }
        let requote = self.requotes[side_index(side)]
            .as_ref()
            .is_some_and(|requote| {
                requote.chain.contains(&cid)
                    && now.saturating_duration_since(requote.since) <= self.window
            });
        self.latest = Some(Event { seen: now, at });
        self.requotes = [None, None];
        match self.first {
            _ if requote => NotOurs::Repeat,
            Some(first) if now.saturating_duration_since(first) <= self.window => NotOurs::Repeat,
            _ => {
                self.first = Some(now);
                NotOurs::First
            }
        }
    }

    /// How many orders of ours cancels not ours ended.
    pub fn orders(&self) -> usize {
        self.orders
    }

    /// Whether any was seen: another program may be on the account.
    pub fn seen(&self) -> bool {
        self.orders > 0
    }

    /// The window as the lines print it.
    fn shown(&self) -> String {
        if self.window.subsec_nanos() == 0 {
            format!("{} s", self.window.as_secs())
        } else {
            format!("{} ms", self.window.as_millis())
        }
    }
}

/// The quote size and fbc-oms's caps, in lots, at the first touch and mark.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Sizing {
    /// Each quote's size.
    pub qty: Lots,
    /// The resting cap per side (at `at`) and the inventory cap (at no less than the mark).
    pub resting: Lots,
    pub inventory: Lots,
    /// The price the size and the resting cap were converted at: the highest of the quote
    /// prices and the touch.
    pub at: Decimal,
    /// The mark price read with the first touch.
    pub mark: Decimal,
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

/// The quote size and the caps in lots at the first touch `book` and `mark` ([`Sizing`]), or why
/// the run cannot quote: no side of the book rests, no lot in `--order-usd`, or a quote's
/// notional below the market's minimum at the lowest quote price or at the mark (where Paradex
/// judges it).
pub fn size(opts: &Options, book: &OrderbookSnapshot, mark: Decimal) -> Result<Sizing, String> {
    let (bid, ask) = quote_prices(opts, book);
    if bid.is_err() && ask.is_err() {
        return Err(format!(
            "no side can be quoted at the first touch (bid: {}; ask: {})",
            bid.err().unwrap_or_default(),
            ask.err().unwrap_or_default()
        ));
    }
    let at = market_px(opts, book, &[&bid, &ask]).ok_or("no price to size the quotes at")?;
    let lots_at = |usd: Decimal, px: Decimal| -> Lots {
        let n = (usd / (px * opts.step)).floor().to_i64().unwrap_or(0);
        Lots::new(n).unwrap_or(Lots::ZERO)
    };
    let lots_for = |usd: Decimal| lots_at(usd, at);
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
    let at_mark = Decimal::from(qty.get()) * opts.step * mark;
    if at_mark < opts.min_notional {
        return Err(format!(
            "a quote's notional at the mark price {mark} is ${} ({} lots), below the market's \
             minimum ${}: Paradex judges the minimum at the mark and refuses the order after \
             accepting it; raise --order-usd",
            at_mark.round_dp(4).normalize(),
            qty.get(),
            opts.min_notional
        ));
    }
    Ok(Sizing {
        qty,
        resting: lots_for(opts.resting_cap_usd),
        inventory: lots_at(opts.inventory_cap_usd, at.max(mark)),
        at,
        mark,
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

/// The desired book for the touch `book` and the mark price `mark` with the position at
/// `position` ([`quote_prices`]), each quote `qty` lots: a side is left out when it has no price
/// behind the touch, when its notional at its price would exceed `--resting-cap-usd` or fall
/// below `--min-notional`, when its notional at the mark would fall below `--min-notional`
/// (where Paradex judges it, refusing the order only after accepting it), or when a fill of it
/// would take the position past `--inventory-cap-usd` at the highest of the quote prices, the
/// touch and the mark.
pub fn desired(
    opts: &Options,
    qty: Lots,
    position: SignedLots,
    book: &OrderbookSnapshot,
    mark: Decimal,
) -> Wanted {
    let (bid, ask) = quote_prices(opts, book);
    let mkt = market_px(opts, book, &[&bid, &ask]).map(|px| px.max(mark));
    let at_mark = Decimal::from(qty.get()) * opts.step * mark;
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
        } else if at_mark < opts.min_notional {
            Some(format!(
                "its notional ${} at the mark {mark} is below the market's minimum ${} \
                 (Paradex judges the minimum at the mark)",
                at_mark.round_dp(4).normalize(),
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
            tag: 0,
            reduce_fallback: ReduceFallback::KeepReduceOnly,
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

/// The guard, run before anything connects (by `main` before it reads the credentials, and
/// again by [`run_venue`]): every check of the command line ([`confirmed`]), and the chain id
/// (`PARADEX_CHAIN_ID` when set, `chain`) the testnet's, or with `--mainnet` mainnet's. Whether
/// the URLs are a loopback stub's.
pub fn guard(opts: &Options, chain: Option<&str>) -> Result<bool, String> {
    let stub = confirmed(opts)?;
    if opts.mainnet {
        mainnet_guard(&opts.rest_url, &opts.ws_url, chain, PROGRAM)?;
    } else {
        testnet_guard(&opts.rest_url, &opts.ws_url, chain)?;
    }
    Ok(stub)
}

/// The account's credentials from `lookup` (the process environment when run; a map under
/// test), read twice: the session's and the position check's own login's (a credential set is
/// never copied). The testnet samples' variables (`src/auth.rs`), or with `--mainnet` the
/// mainnet-only ones (`src/auth_mainnet.rs`, which refuses a run whose environment holds a
/// testnet one). Refused naming variables, never a value.
pub fn credentials(
    opts: &Options,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<(Secrets, Secrets), String> {
    let read = || {
        if opts.mainnet {
            crate::auth_mainnet::paradex_mainnet_secrets(&lookup).map_err(|e| {
                format!(
                    "{e} (testnet_quote --mainnet reads the same mainnet variables as \
                     mainnet_smoke)"
                )
            })
        } else {
            crate::auth::paradex_secrets(&lookup)
        }
    };
    Ok((read()?, read()?))
}

/// The Paradex configuration for `opts`: the login signing for the testnet's chain, or with
/// `--mainnet` mainnet's.
pub fn venue_config(opts: &Options) -> fbc_core::VenueConfig {
    if opts.mainnet {
        crate::mainnet::config(&opts.rest_url, &opts.ws_url)
    } else {
        config(&opts.rest_url, &opts.ws_url)
    }
}

/// Runs testnet_quote on Paradex ([`run_venue`]).
pub async fn run(
    opts: &Options,
    chain: Option<&str>,
    creds: (Secrets, Secrets),
    out: Out,
    interrupt: impl Future<Output = ()>,
) -> Result<Report, String> {
    run_venue(&ParadexFactory, opts, chain, creds, out, interrupt).await
}

/// Runs testnet_quote on `factory` (Paradex's; a stand-in under test): the guard first, before
/// anything connects ([`guard`]; with `--mainnet` also the venue's snapshot source trustworthy,
/// [`trusted`]); then the loop ([`run_on`]) until `interrupt` completes, `--duration` or
/// `--cycles` is reached, or anything Stops it. `chain` is `PARADEX_CHAIN_ID` when set; `creds`
/// the account's credentials, twice ([`credentials`]): the session's, handed to its codec
/// unread, and the position check's own login's, each moved on unread (no type here holds
/// them). An error is a refusal before the session started; once it started the outcome is in
/// the [`Report`].
pub async fn run_venue(
    factory: &'static dyn VenueFactory,
    opts: &Options,
    chain: Option<&str>,
    creds: (Secrets, Secrets),
    out: Out,
    interrupt: impl Future<Output = ()>,
) -> Result<Report, String> {
    let stub = guard(opts, chain)?;
    let cfg = venue_config(opts);
    if opts.mainnet {
        trusted(factory, &cfg, PROGRAM)?;
    }
    let lines = Lines {
        out,
        t0: Instant::now(),
    };
    let (rest_host, ws_host) = hosts(&opts.rest_url, &opts.ws_url)?;
    if opts.mainnet {
        let what = if stub {
            "a loopback test stub, not Paradex"
        } else {
            "Paradex MAINNET, real money"
        };
        lines.line(format_args!(
            "MAINNET {what}: REST host {rest_host} WS host {ws_host} chain {MAINNET_CHAIN}"
        ));
        lines.line(format_args!(
            "SUPERVISED mainnet run: never seeded by hand (the trustworthy resync seeds the \
             market, decision 0094); caps ${} resting per side, ${} inventory; --duration {} s",
            opts.resting_cap_usd, opts.inventory_cap_usd, opts.duration_secs
        ));
    } else {
        let what = if stub {
            "a loopback test stub, not Paradex"
        } else {
            "Paradex testnet"
        };
        lines.line(format_args!(
            "TESTNET {what}: REST host {rest_host} WS host {ws_host} chain {TESTNET_CHAIN}"
        ));
        lines.line(format_args!(
            "OWNER-ASSISTED testnet run (decision 0067): Paradex's snapshot source is \
             trustworthy (decision 0094), so the resync seeds the market's position; a market \
             it left unknown is seeded by hand from the venue's REST position, never outside a \
             declared testnet run"
        ));
    }
    lines.line(format_args!(
        "PLUMBING TEST, not a strategy: fixed offsets of {} bps under the bid and {} bps over \
         the ask, no model",
        opts.bid_bps, opts.ask_bps
    ));
    run_on(factory, opts, creds, lines, interrupt).await
}

/// The loop on `factory` once [`run_venue`]'s guard passed and its first lines are printed
/// ([`run_venue`] for its arguments): the command line is checked again ([`confirmed`]); the
/// venue's declared snapshot source is not (a test runs a venue declaring it untrustworthy past
/// [`run_venue`]'s check, to show that a mainnet resync seeding nothing refuses Start). Then the
/// chain (with `--mainnet`), the first touch and mark and the sizing, the registry, the leases,
/// the other markets' positions (with `--mainnet`), the session and the loop.
pub async fn run_on(
    factory: &'static dyn VenueFactory,
    opts: &Options,
    creds: (Secrets, Secrets),
    lines: Lines,
    interrupt: impl Future<Output = ()>,
) -> Result<Report, String> {
    confirmed(opts)?;
    let cfg = venue_config(opts);
    let specs = specs(factory, &cfg, &opts.market, opts.tick, opts.step)?;
    let wait = Duration::from_secs(opts.step_timeout_secs);
    let venue_caps = factory.caps(&cfg).map_err(|e| e.to_string())?;
    let order_caps = venue_caps
        .exec
        .as_ref()
        .ok_or("Paradex declares no order entry")?
        .order
        .clone();
    shape_admitted(&order_caps)?;
    let (session_creds, position_creds) = creds;
    let session_cfg = session_config(
        factory,
        cfg.clone(),
        session_creds,
        specs.clone(),
        &opts.proxy,
        opts.namespace,
    )?;
    // Every REST read of the run is charged to the limiter the session charges, so they and
    // the session's traffic share one count (Codex r4289363370 on PR #149; FBC-hpg6).
    let limiter = session_cfg.limiter.clone();
    if opts.mainnet {
        let by = tokio::time::Instant::now() + wait;
        admit_reads(&limiter, 1, by, "GET /system/config").await?;
        let left = by.saturating_duration_since(tokio::time::Instant::now());
        let body = rest_get(
            &opts.rest_url,
            "/v1/system/config",
            "GET /system/config",
            &opts.proxy,
            left,
        )
        .await?;
        if let Some(refused) = chain_refusal(&decode_chain(&body)?) {
            return Err(refused);
        }
        lines.step(
            "chain",
            format_args!("GET /system/config names {MAINNET_CHAIN}, the chain the login signs for"),
        );
    }
    let (book, mark) = touch_and_mark(
        &opts.rest_url,
        &opts.market,
        &opts.proxy,
        wait,
        &specs,
        &limiter,
    )
    .await?;
    let sizing = size(opts, &book, mark)?;
    let (bid, ask) = touch_px(opts, &book);
    lines.line(format_args!(
        "QUOTE {} lots ({} at the size step) per side; caps at {}: resting {} lots per side, \
         inventory {} lots (first touch: bid {} ask {}; mark {}, where Paradex judges the \
         minimum ${})",
        sizing.qty.get(),
        Decimal::from(sizing.qty.get()) * opts.step,
        sizing.at,
        sizing.resting.get(),
        sizing.inventory.get(),
        px_or_none(bid),
        px_or_none(ask),
        sizing.mark,
        opts.min_notional
    ));

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
    let label = if opts.mainnet {
        crate::mainnet::LABEL
    } else {
        ACCOUNT_LABEL
    };
    let keys = LeaseKeys::new(VENUE, label, &order_caps).with_market(INST, symbol.clone());
    let needs_account_lease = keys.nonce_scope() == NonceScope::PerAccountMonotonic;
    let reg = Registry::with_caps(caps);
    // Only a testnet run is a declared owner-assisted run (decision 0067); a mainnet market is
    // seeded by its trustworthy resync or not at all.
    let reg = if opts.mainnet {
        reg
    } else {
        reg.for_testnet_run(TestnetRun::owner_assisted())
    };
    let reg = reg.with_lease_keys(keys);

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
    let market =
        MarketLease::acquire(&opts.lease_dir, VENUE, label, &symbol).map_err(|e| held(&e))?;
    let mut leases = Leases::market(market);
    if needs_account_lease {
        let acct =
            fbc_core::AccountLease::acquire(&opts.lease_dir, VENUE, label).map_err(|e| held(&e))?;
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
    // The position check reads over REST through the limiter the session charges, so its reads
    // and the session's traffic share one count (FBC-hpg6).
    let mut positions = PositionCheck::new(
        &cfg,
        position_creds,
        Connector::new(opts.proxy.clone()),
        limiter.clone(),
        &opts.rest_url,
        specs.clone(),
        venue_caps,
        wait,
    )?;
    if opts.mainnet {
        // The account's positions on every other market, before the order socket connects: one
        // there breaks --sole-trader's premise (Codex r4277631493 on PR #149).
        let elsewhere = positions.elsewhere().await?;
        if !elsewhere.is_empty() {
            return Err(format!(
                "GET /positions shows the account holding a position on another market ({}): \
                 --sole-trader says no other process trades this account, and this run trades \
                 one market only; close it first; refused before the order socket connected",
                elsewhere.join(", ")
            ));
        }
        lines.step(
            "positions",
            format_args!("GET /positions shows no position on another market"),
        );
    }
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
        limiter,
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
        retry_at: None,
        recancel_due: false,
        not_ours: CancelsNotOurs::new(opts.cancel_window),
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
         inventory {} lots cancels not ours {}",
        report.cycles,
        report.places_sent,
        report.cancels_sent,
        report.cancelled,
        report.fills,
        report.inventory,
        report.cancels_not_ours
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
    /// A request of ours was refused for its rate: nothing is planned before its retry time
    /// ([`Driver::retry_at`]).
    Paced,
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
    /// The session's rate limiter, which every REST read of the loop is charged to.
    limiter: RateLimiter,
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
    /// The market's position as the venue's position stream last reported it, and the
    /// connection it came on (the count of connections ended before it, [`Driver::epochs`]): no
    /// pass plans while it differs from the inventory (Codex r4244080127). A resync supersedes
    /// one reported on an earlier connection, never one reported on its own, which may be newer
    /// than its snapshot (Codex r4277631505 on PR #149).
    venue_position: Option<(SignedLots, usize)>,
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
    /// Until when nothing is planned: a request of ours refused for its rate (`RateLimited`)
    /// waits its retry time, or [`RATE_LIMITED_WAIT`] (Codex r4277631500, r4289363361).
    retry_at: Option<tokio::time::Instant>,
    /// A cancel of ours was refused for its rate since the last cancel of everything: a sweep
    /// or Stop sends the cancel of everything of ours again once its retry time passed
    /// ([`Driver::cancel_again`], Codex r4306449947 on PR #158).
    recancel_due: bool,
    /// The cancels not ours seen, under the owner's rule (module documentation, FBC-aoh5),
    /// with when each order of ours the loop placed that has not ended was handed over (with
    /// the venue's times, what tells one event from the next, and each side's requote since
    /// the latest event, [`CancelsNotOurs`]).
    not_ours: CancelsNotOurs,
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
            cancels_not_ours: self.not_ours.orders(),
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
            // A refusal for its rate's retry time is a wake too: what waits for it (a sweep's or
            // Stop's cancel sent again) is read then, whether or not anything else comes.
            let retry = self
                .retry_at
                .filter(|at| *at > tokio::time::Instant::now() && *at < deadline);
            tokio::select! {
                () = woken => {}
                // Past the deadline when there is no retry time to wake at: nothing found.
                () = tokio::time::sleep_until(retry.unwrap_or(deadline)) => {
                    retry?;
                }
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
                    self.unrested(rpc);
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
                    self.venue_position = Some((qty, self.epochs));
                } else if qty.0 != 0 {
                    // --sole-trader: a position on another market is another trader's (Codex
                    // r4277631493 on PR #149). (With one market in the spec table the adapter
                    // refuses such a frame first, a decode error that Stops the loop too.)
                    self.trouble(format!(
                        "the venue reported a position of {} lots on another market (instrument \
                         {}): --sole-trader says no other process trades this account",
                        qty.0,
                        inst.get()
                    ));
                }
                self.note(format_args!(
                    "the account's position in instrument {} is {} lots",
                    inst.get(),
                    qty.0
                ));
            }
            Note::Resynced { report, snapshot } => {
                // The resync's position supersedes one the position stream reported on an
                // earlier connection, never one reported on this one, before or during the
                // resync: it may be newer than the snapshot's read, so it is kept until the
                // inventory agrees with it (Codex r4277631505 on PR #149).
                if self
                    .venue_position
                    .is_some_and(|(_, epoch)| epoch != self.epochs)
                {
                    self.venue_position = None;
                }
                self.resync_due = false;
                if let Some((inst, qty)) = snapshot
                    .positions
                    .iter()
                    .find(|(inst, qty)| *inst != INST && qty.0 != 0)
                {
                    // --sole-trader (Codex r4277631493 on PR #149).
                    self.trouble(format!(
                        "a resync showed a position of {} lots on another market (instrument \
                         {}): --sole-trader says no other process trades this account",
                        qty.0,
                        inst.get()
                    ));
                }
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
                        self.not_ours.unrested(cid);
                        self.note(format_args!(
                            "the {} #{} was not rested: {what}; its side is left out of this \
                             cycle and placed again the next",
                            side_name(self.side_of(cid)),
                            cid.seq()
                        ));
                    }
                    // Paradex judges the minimum notional at the mark and refuses an order
                    // below it after accepting it (ORDER_SIZE_BELOW_MIN, which its adapter
                    // reads as MinNotional, decision 0096): a sizing error, never a venue
                    // fault (the owner's 2026-10-16 testnet run), and permanent.
                    _ if reject.kind == RejectKind::MinNotional => self.trouble(format!(
                        "a sizing error: {what}: Paradex judged the order's notional at the mark \
                         price below the market's minimum, though every place is sized at the \
                         mark read with its touch; check --min-notional and --order-usd against \
                         GET /markets; nothing more is placed"
                    )),
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
            Note::CancelNotOurs { cid, side, at } => self.cancel_not_ours(cid, side, at),
            Note::Conn(ConnState::Authenticated) => self.authenticated = true,
            Note::Conn(_) => {}
        }
    }

    /// Our order `cid` on `side` was ended by a cancel this program did not send, stamped `at`
    /// by the venue (module documentation, FBC-aoh5): the owner's rule applied, printed as a
    /// `WARN` line once per event; with `--mainnet` a repeat is a reason to Stop.
    fn cancel_not_ours(&mut self, cid: ClientOrderId, side: Side, at: Option<ExchNs>) {
        let order = format!("the {} #{}", side_name(side), cid.seq());
        let window = self.not_ours.shown();
        let what = format!(
            "{order} ended by a user cancel this program did not send (another program on the \
             account, or the Paradex UI): one quoter per account"
        );
        match self
            .not_ours
            .on_cancel(cid, side, at, tokio::time::Instant::now())
        {
            NotOurs::SameEvent => self.note(format_args!(
                "{order} ended by a user cancel this program did not send too: placed before the \
                 last one was seen and cancelled with it, the same event"
            )),
            NotOurs::First if self.opts.mainnet => self.event(
                "WARN",
                format_args!(
                    "{what}; a second on a later cycle within {window} Stops the run, as does \
                     one of the loop's requote within {window} of its place; the loop requotes, \
                     and cancels everything of ours explicitly from now on (never an instrument \
                     cancel-all)"
                ),
            ),
            NotOurs::First => self.event(
                "WARN",
                format_args!(
                    "{what}; testnet mode only warns; the loop requotes, and cancels everything \
                     of ours explicitly from now on (never an instrument cancel-all)"
                ),
            ),
            NotOurs::Repeat if self.opts.mainnet => {
                self.event(
                    "WARN",
                    format_args!(
                        "{what}; the second on a later cycle within {window} (of the first, or of \
                         placing the requote it cancelled): Stop"
                    ),
                );
                self.trouble(format!(
                    "another program cancelled our orders: one quoter per account (a second \
                     cancel this program did not send on a later cycle within {window} of the \
                     first or of placing the requote it cancelled, of {order}); stop the other program or give each its own account"
                ));
            }
            NotOurs::Repeat => self.event(
                "WARN",
                format_args!(
                    "{what}; a second on a later cycle within {window} (of the first, or of \
                     placing the requote it cancelled): testnet mode only warns"
                ),
            ),
        }
    }

    /// Our place request `rpc` came back refused or was not sent: its order never rested
    /// ([`CancelsNotOurs::unrested`]). A cancel request frees nothing.
    fn unrested(&mut self, rpc: RpcId) {
        if let Some(Request {
            place: true,
            first: Some((_, cid)),
            ..
        }) = self.ours.get(&rpc).copied()
        {
            self.not_ours.unrested(cid);
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
        if let SubmitOutcome::Rejected(reject) = &outcome {
            self.refused(place, &named, reject);
        }
        if matches!(
            outcome,
            SubmitOutcome::Rejected(_) | SubmitOutcome::NotSent(_)
        ) {
            self.unrested(rpc);
        }
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

    /// A refusal of a place (`place`) or a cancel of ours, `named`: one for its rate paces the
    /// loop ([`Driver::pace`]); a post-only place that would cross leaves its side out of the
    /// cycle (its order ended); a cancel of an order that already ended, or that the venue does
    /// not know (the registry then holds it Unknown, which Stops the loop), awaits that end; any
    /// other is permanent, and Stops the loop, failing the run: nothing is sent again in a loop
    /// (Codex r4277631500, r4289363361 on PR #149).
    fn refused(&mut self, place: bool, named: &str, reject: &Reject) {
        let what = if place {
            format!("the place {named}")
        } else {
            format!("the cancel of {named}")
        };
        match reject.kind {
            RejectKind::RateLimited { retry_after } => {
                self.pace(&what, retry_after);
                // A sweep or Stop sends it again once the retry time passed; a cycle's planner
                // builds it again itself (Codex r4306449947 on PR #158).
                self.recancel_due |= !place;
            }
            RejectKind::PostOnlyWouldCross if place => {}
            RejectKind::AlreadyTerminal(_) | RejectKind::NotFound if !place => {}
            kind => self.trouble(format!(
                "{what} was refused permanently ({kind:?}: {}): no retry mends it, so nothing \
                 more is sent{}",
                described(reject),
                if kind == RejectKind::MinNotional {
                    "; a sizing error: check --min-notional and --order-usd against GET /markets"
                } else {
                    ""
                }
            )),
        }
    }

    /// Nothing is planned until `retry_after` from now (or [`RATE_LIMITED_WAIT`] when the venue
    /// states none): `what` was refused for its rate.
    fn pace(&mut self, what: &str, retry_after: Option<Duration>) {
        let wait = retry_after.unwrap_or(RATE_LIMITED_WAIT);
        let until = tokio::time::Instant::now() + wait;
        self.retry_at = Some(self.retry_at.map_or(until, |at| at.max(until)));
        self.event(
            "PACE",
            format_args!(
                "{what} was refused for its rate: nothing more is planned for {} ms (the venue's \
                 retry time, or {} ms when it states none)",
                wait.as_millis(),
                RATE_LIMITED_WAIT.as_millis()
            ),
        );
    }

    /// Whether a refusal for its rate holds the loop now ([`Driver::retry_at`]).
    fn paced(&self) -> bool {
        self.retry_at
            .is_some_and(|at| tokio::time::Instant::now() < at)
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
        // Only the orders not ended need their placing time (FBC-aoh5).
        let owned = &self.owned;
        self.not_ours.retain(|cid| owned.contains(cid));
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
        let source = match self.caps.snapshot_source {
            fbc_core::SnapshotSource::Trustworthy => "the snapshot is trustworthy (decision 0094)",
            fbc_core::SnapshotSource::Untrustworthy => "the snapshot is declared untrustworthy",
            fbc_core::SnapshotSource::None => "the venue declares no snapshot",
        };
        self.lines.step(
            "resync",
            format_args!("{open} open orders, position {} lots; {source}", position.0),
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
            } else if self.opts.mainnet {
                // A mainnet market is armed only after a trustworthy resync seeded it, never by
                // hand (owner answer C to RB-olg-3; decision 0067 is for testnet runs only).
                return Err(
                    "Start refused: the resync seeded no position (the venue's snapshot source is \
                     not trustworthy), and a mainnet market is never seeded by hand (only an \
                     owner-assisted testnet run may be, decision 0067); nothing placed"
                        .to_owned(),
                );
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
            if self.paced() {
                // A refusal for its rate: nothing is read or planned before its retry time.
                let until = self.retry_at.unwrap_or_else(tokio::time::Instant::now);
                self.pause_until(until).await;
                continue;
            }
            self.compact();
            // The inventory before the touch is read: a fill while it is in flight makes the
            // first pass read the touch again (Codex r4244145956).
            let read_at = self.link.borrow().registry().inventory(INST);
            // Raced against the interrupt, a reason to Stop, a connection's end and
            // --duration, each taken at the top of the loop (Codex r4243989987).
            let Some((book, mark)) = self.touch_unless_ended(self.timeout).await? else {
                continue;
            };
            match self.settle(book, mark, read_at).await? {
                Settled::Done => self.cycles += 1,
                Settled::Reconnected | Settled::Interrupted | Settled::Ended | Settled::Paced => {
                    continue;
                }
            }
            // The last cycle asked for ends the loop at once: its quotes never rest for the
            // pause (Codex r4289363377 on PR #149).
            if self.opts.cycles.is_some_and(|n| self.cycles >= n) {
                return Ok(format!("--cycles {} reached", self.cycles));
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

    /// The touch (`GET /orderbook`) and the mark price (`GET /markets/summary`), read together
    /// and charged to the session's rate limiter ([`touch_and_mark`]), as the notes come: `None`
    /// once the cycle must end ([`Driver::ending`]) before they answer, so neither Ctrl-C, a
    /// reason to Stop, a connection's end nor `--duration` waits up to `--step-timeout` for them
    /// while the quotes rest (Codex r4243989987 on PR #149).
    ///
    /// The reads wait at most `within`, the wait for the rate limit's room included. A book older
    /// than one already read (a lower `seq_no`: a stale cache or replica) is a reason to Stop,
    /// never quoted from (Codex r4244080123).
    async fn touch_unless_ended(
        &mut self,
        within: Duration,
    ) -> Result<Option<(OrderbookSnapshot, Decimal)>, String> {
        let (rest_url, market) = (self.opts.rest_url.clone(), self.opts.market.clone());
        let (proxy, specs) = (self.opts.proxy.clone(), self.specs.clone());
        let limiter = self.limiter.clone();
        let read = touch_and_mark(&rest_url, &market, &proxy, within, &specs, &limiter);
        tokio::pin!(read);
        loop {
            let wake = Rc::clone(&self.wake);
            let woken = wake.notified();
            self.absorb();
            if self.ending() {
                return Ok(None);
            }
            tokio::select! {
                read = &mut read => {
                    let (book, mark) = read?;
                    if book.seq_no < self.book_seq {
                        return Err(format!(
                            "GET /orderbook answered seq {} after seq {}: an order book older \
                             than one already read is never quoted from",
                            book.seq_no, self.book_seq
                        ));
                    }
                    self.book_seq = book.seq_no;
                    return Ok(Some((book, mark)));
                }
                () = woken => {}
                () = tokio::time::sleep_until(self.deadline) => return Ok(None),
            }
        }
    }

    /// Prints the cycle when its quotes or skipped sides differ from the last cycle's.
    fn print_cycle(&mut self, book: &OrderbookSnapshot, mark: Decimal, wanted: &Wanted) {
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
            "CYCLE {} touch bid {} ask {} (GET /orderbook seq {}), mark {}: bid {}, ask {}",
            self.cycles + 1,
            px_or_none(bid),
            px_or_none(ask),
            book.seq_no,
            mark,
            quote(wanted.bid),
            quote(wanted.ask)
        ));
        for skipped in &wanted.skipped {
            self.lines.line(format_args!("SKIP {skipped}"));
        }
        self.last = Some(now);
    }

    /// The pause between cycles: the interval ([`Driver::pause_until`]).
    async fn pause(&mut self) {
        let until = tokio::time::Instant::now() + Duration::from_millis(self.opts.interval_ms);
        self.pause_until(until).await;
    }

    /// Waits until `until` (never past `--duration`), ended early by the interrupt, `--duration`,
    /// a reason to Stop or a connection's end, read as the notes come.
    async fn pause_until(&mut self, until: tokio::time::Instant) {
        let until = std::cmp::min(until, self.deadline);
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
    ///
    /// The wait for the write is bounded by the book's settling time, `settle` (the pass's
    /// deadline), as every other wait of a pass is: a write that stalls past it Stops the loop
    /// then, never a whole step timeout after a late pass started it (Codex r4289363366 on PR
    /// #149); the write is kept, holding the mint, as when a cycle ends during it.
    async fn reserve(&mut self, settle: tokio::time::Instant) -> Result<bool, String> {
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
                // The write's own bound is past every deadline that cuts its wait short (the
                // settling time, --duration): the wait below decides.
                let bound = self.timeout.saturating_mul(2);
                let write: MarkWrite =
                    Box::pin(off_thread_holding(bound, mint, move || hwm.write(mark)));
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
                biased;
                () = tokio::time::sleep_until(self.deadline) => {
                    self.writing = Some((mark, write));
                    return Ok(false);
                }
                () = tokio::time::sleep_until(settle) => {
                    self.writing = Some((mark, write));
                    return Err(self.timed_out("settle", "the client-id high-water mark's write"));
                }
                written = &mut write => break written,
                () = woken => {}
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
    ///
    /// The fresh touch read after a fill moved the position must be a newer book than the one
    /// held (a higher `seq_no`): one no newer is read again, never quoted from (Codex
    /// r4289363354 on PR #149). No pass plans while a cancel of ours is unanswered, even once
    /// its order ended (the venue's order event can come before the cancel's reply), so a
    /// replacement is placed only after its cancel's acknowledgement (Codex r4277631497). The
    /// mark read with each touch judges the quotes with it (`mark`).
    async fn settle(
        &mut self,
        mut book: OrderbookSnapshot,
        mut mark: Decimal,
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
            let mut cut = !self.reserve(deadline).await?;
            let inventory = |d: &Driver| d.link.borrow().registry().inventory(INST);
            let mut position = inventory(self);
            // A fresh touch for a moved position, read again until no fill moved it while the
            // touch was being read (Codex r4243908416), and one after a position check.
            while !cut && (aged || position != read_at) {
                // The position moved since the book held was read: only a newer book shows the
                // market after the move (Codex r4289363354 on PR #149).
                let moved = position != read_at;
                if tokio::time::Instant::now() >= deadline {
                    return Err(self.timed_out(
                        "settle",
                        "an order book newer than the one held when the position moved",
                    ));
                }
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                let Some((fresh, fresh_mark)) = self.touch_unless_ended(left).await? else {
                    cut = true;
                    break;
                };
                if moved && fresh.seq_no <= book.seq_no {
                    self.note(format_args!(
                        "GET /orderbook answered seq {} after the position moved, no newer than \
                         the book held (seq {}): read again, never quoted from",
                        fresh.seq_no, book.seq_no
                    ));
                    let until =
                        std::cmp::min(tokio::time::Instant::now() + STALE_BOOK_PAUSE, deadline);
                    self.pause_until(until).await;
                    if self.ending() {
                        cut = true;
                    }
                    continue;
                }
                book = fresh;
                mark = fresh_mark;
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
            // An order's end can come before its cancel's reply: nothing is planned until every
            // cancel of ours is answered, so no replacement goes out before its cancel's
            // acknowledgement (Codex r4277631497 on PR #149).
            if let Some(rpc) = self.cancel_unanswered() {
                tokio::select! {
                    () = woken => {}
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(self.timed_out(
                            "settle",
                            &format!("the answer to cancel request {}", rpc.0),
                        ));
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
            let mut wanted = desired(&self.opts, self.qty, position, &book, mark);
            for side in &left_out {
                wanted.book.remove(*side, 0);
                match side {
                    Side::Buy => wanted.bid = None,
                    Side::Sell => wanted.ask = None,
                }
            }
            self.print_cycle(&book, mark, &wanted);
            let now = self.link.borrow().now().0;
            let plan = {
                let mut link = self.link.borrow_mut();
                let mint = self.mint.as_mut().ok_or("no client-id mint")?;
                // This program reads no venue mode and no limiter room, and sends one order
                // per command: the venue refuses what its mode refuses, and the runtime's
                // limiter holds normal traffic at its reserve (decision 0111).
                let venue = PlanVenue {
                    caps: &self.plan_caps,
                    account_mode: VenueMode::Normal,
                    market_mode: VenueMode::Normal,
                    status: TradingStatus::Trading,
                    rates: RateView::new(Vec::new()),
                    batching: Batching::One,
                };
                self.planner
                    .plan(&wanted.book, link.reg(), &venue, ACCT, mint, now)
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
        let venue = self.venue_position.map(|(qty, _)| qty);
        if let Some(venue) = venue.filter(|v| *v != position) {
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
    /// `--duration`, a refusal for its rate whose retry time has not come).
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
        if self.paced() {
            return Ok(Some(Settled::Paced));
        }
        Ok(None)
    }

    /// A cancel request of ours naming an order with no answer yet (one not sent included), if
    /// any: a replace's or a side's cancel, or a Safety cancel the session's link sent. An
    /// instrument cancel-all names none: a sweep's is sent before any place, and its order ends
    /// are what the sweep waits for.
    fn cancel_unanswered(&self) -> Option<RpcId> {
        self.ours
            .iter()
            .filter(|(_, request)| !request.place && request.first.is_some())
            .map(|(rpc, _)| *rpc)
            .find(|rpc| self.answer(*rpc).is_none())
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
            self.not_ours.placed(cid, side, tokio::time::Instant::now());
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
        // The sweep's cancel covers every cancel refused for its rate before it.
        self.recancel_due = false;
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
                d.cancel_again();
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

    /// The cancel of everything of ours sent again ([`Driver::cancel_ours`]) once a cancel was
    /// refused for its rate during a sweep or Stop and its retry time passed: what they wait for
    /// is sent again, not waited out while the quotes rest (Codex r4306449947 on PR #158). The
    /// requests sent.
    fn cancel_again(&mut self) -> Vec<RpcId> {
        if !self.recancel_due || self.paced() {
            return Vec::new();
        }
        self.recancel_due = false;
        match self.cancel_ours() {
            Ok((rpcs, how)) => {
                self.note(format_args!(
                    "a cancel refused for its rate, sent again after its retry time: {} cancels \
                     sent ({how})",
                    rpcs.len()
                ));
                rpcs
            }
            Err(why) => {
                self.trouble(why);
                Vec::new()
            }
        }
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
    /// handed to the session: an instrument cancel-all when fbc-oms builds one (0005's I7) and
    /// no cancel not ours was seen, otherwise explicit cancels: once one was seen another
    /// program may be on the account, and Paradex's instrument cancel-all would cancel its
    /// orders too (FBC-aoh5). The requests, and how it was done.
    fn cancel_ours(&mut self) -> Result<(Vec<RpcId>, String), String> {
        let not_ours = self.not_ours.seen();
        let (cmds, how) = {
            let mut link = self.link.borrow_mut();
            let caps = self.caps.clone();
            let reg = link.reg();
            let everything = (!not_ours).then(|| reg.cancel_everything(INST, &caps));
            let all = match everything {
                Some(CancelEverything::CancelAll {
                    command,
                    unanswered,
                }) => {
                    let mut cmds = vec![command];
                    cmds.extend(unanswered.commands);
                    Ok((cmds, "an instrument cancel-all".to_owned()))
                }
                Some(CancelEverything::Explicit { why, .. }) => Err(why.to_string()),
                None => Err(
                    "another program cancelled our orders (a cancel this program did \
                             not send), and an instrument cancel-all would cancel its orders too"
                        .to_owned(),
                ),
            };
            match all {
                Ok(all) => all,
                Err(why) => {
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
        // Stop's cancel covers every cancel refused for its rate before it.
        self.recancel_due = false;
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
                rpcs.extend(d.cancel_again());
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
                // Refused for its rate and sent again, and every order of ours ended: paced, not
                // failed (Codex r4306449947 on PR #158).
                Some(SubmitOutcome::Rejected(r))
                    if matches!(r.kind, RejectKind::RateLimited { .. }) && ended.is_some() => {}
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
