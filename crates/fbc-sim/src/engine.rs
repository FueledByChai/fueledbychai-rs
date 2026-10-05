//! SimVenue's matching engine: a pure state machine over the shard's market-data envelopes and
//! the frames its codec writes, answering with frames after the configured latency (decision
//! 0043).

use core::fmt;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_book::{BookError, Books, L2Book};
use fbc_core::{
    AccountKey, Aggressor, AssetSym, BookId, BookSide, CancelReason, Channel, Envelope, FeeBook,
    FeeKey, InstrumentId, Liquidity, Lots, MdEvent, MonoNs, Side, SpecTable, TerminalHint, Ticks,
    TifTag, VenueFeeSign, WallNs,
};

use crate::config::{SimConfig, SimLatency};
use crate::queue::{NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, SimFill, TradeView};
use crate::wire::{
    Cancel, Command, FillRecord, OrderEvent, Place, Refusal, Reply, SimState, Target, WireError,
};

/// One frame the engine answers with, due at the codec at `at`: the instant the venue acted
/// plus [`SimLatency::to_client`]. The host hands it to the codec's `on_frame` then.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Answer {
    pub at: MonoNs,
    pub frame: Vec<u8>,
}

/// An engine call that was refused.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum SimError {
    /// An order-entry frame the engine cannot read, naming the part; nothing was scheduled.
    Malformed(&'static str),
    /// A market-data event the book refused; the book is as `fbc-book` leaves it.
    Book(BookError),
    /// The queue model refused a level cancel.
    Queue(QueueError),
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::Malformed(what) => write!(f, "simulated order-entry frame: bad {what}"),
            SimError::Book(e) => write!(f, "simulated venue's book: {e}"),
            SimError::Queue(e) => write!(f, "simulated venue's queue model: {e}"),
        }
    }
}

impl std::error::Error for SimError {}

/// A resting order the venue holds.
#[derive(Clone, Debug)]
struct Resting {
    cid: String,
    inst: InstrumentId,
    side: Side,
    px: Ticks,
    qty: Lots,
    cum: Lots,
    post_only: bool,
    reduce_only: bool,
}

/// An instant on both of the host's clocks, from a stamp or a frame's encode time.
#[derive(Copy, Clone, Debug)]
struct At {
    mono: MonoNs,
    wall: WallNs,
}

/// SimVenue's engine. It holds its own `fbc-book` books, built from the envelopes it is fed
/// (never the runtime's), one [`QueueModel`] per instrument, and the orders its codec placed.
/// It reads no clock: a command acts at its encode time (which the codec writes in its frame)
/// plus [`SimLatency::to_venue`], an envelope at its stamp, and answers are due
/// [`SimLatency::to_client`] after the venue acted. Commands act in arrival order, each before
/// any envelope stamped at or after its arrival, and are held until such an envelope (or
/// [`advance`](SimEngine::advance)) passes them.
///
/// - A place crosses the trading book's displayed levels, each fill at a level's price up to
///   its size, as the taker; a post-only order that would cross is refused
///   (`PostOnlyWouldCross`). The rest of a good-till-cancelled limit order rests and queues
///   (decision 0038); the rest of an immediate-or-cancel, fill-or-kill or market order is
///   cancelled unfilled, and a fill-or-kill order that cannot fill whole fills nothing.
/// - A cancel ends a resting order; one the venue has ended is refused `AlreadyTerminal`, one
///   it never had `NotFound`.
/// - A public trade on a trading book fills resting orders through the queue model, as the
///   maker. A trade the venue gives no aggressor for is classified against the touch (at or
///   above the offer a buy, at or below the bid a sell), and ignored inside the spread.
/// - A trading book's level that shrinks by more than the trades at its price since it last
///   changed is a level cancel for the queue model, whether a delta or a replacement snapshot
///   shrinks it; each change ends what those trades explain.
/// - Every fill's fee is the fee book's rate for the account, instrument, public channel and
///   liquidity at the fill's wall time, times its notional, rounded to the nano, written in the
///   stood-in venue's fee sign so the codec's [`DecodeScope`](fbc_core::DecodeScope) reads it
///   back as a cost (0004); a fee that is not a finite number of nanos strictly inside `i128`'s
///   range is no fee. A place that would take a fill without one is refused
///   (`no_fee`); a resting order a trade would fill without a rate is cancelled by the venue
///   instead.
#[derive(Clone, Debug)]
pub struct SimEngine {
    fee_sign: VenueFeeSign,
    latency: SimLatency,
    queue: QueueConfig,
    account: AccountKey,
    fees: FeeBook,
    specs: SpecTable,
    trading: BTreeMap<InstrumentId, BookId>,
    books: Books,
    queues: BTreeMap<InstrumentId, QueueModel>,
    live: BTreeMap<u64, Resting>,
    ended: BTreeMap<u64, TerminalHint>,
    by_cid: BTreeMap<String, u64>,
    in_flight: BTreeMap<(MonoNs, u64), (WallNs, Command)>,
    /// Public size traded at a level since it last changed, by (instrument, bid side, price).
    traded: BTreeMap<(InstrumentId, bool, Ticks), Lots>,
    out: Vec<Answer>,
    frames: u64,
    orders: u64,
    fills: u64,
    seq: u64,
}

impl SimEngine {
    /// An engine for the venue `config` stands in for, with no books and no orders.
    pub fn new(config: &SimConfig) -> SimEngine {
        SimEngine {
            fee_sign: config.exec.fills.fee_sign,
            latency: config.latency,
            queue: config.queue,
            account: config.account,
            fees: config.fees.clone(),
            specs: config.specs.clone(),
            trading: config.books.clone(),
            books: Books::new(),
            queues: BTreeMap::new(),
            live: BTreeMap::new(),
            ended: BTreeMap::new(),
            by_cid: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            traded: BTreeMap::new(),
            out: Vec::new(),
            frames: 0,
            orders: 0,
            fills: 0,
            seq: 0,
        }
    }

    /// A frame the codec wrote: the command acts at its encode time plus
    /// [`SimLatency::to_venue`]. Refused, scheduling nothing, when it cannot be read.
    pub fn on_order_frame(&mut self, frame: &[u8]) -> Result<(), SimError> {
        let command =
            Command::decode(frame).map_err(|WireError(what)| SimError::Malformed(what))?;
        let sent = command.sent();
        let mono = sent.mono + self.latency.to_venue;
        let wall = after(sent.wall, self.latency.to_venue);
        self.in_flight.insert((mono, self.frames), (wall, command));
        self.frames += 1;
        Ok(())
    }

    /// One of the shard's market-data envelopes, in ingest order: the commands that arrived
    /// by its stamp act first, then the event. Book events build the engine's books; a trade
    /// fills resting orders. Refused when the book refuses the event.
    pub fn on_market(&mut self, env: &Envelope<MdEvent>) -> Result<(), SimError> {
        self.advance(env.stamp.recv_mono);
        let at = At {
            mono: env.stamp.recv_mono,
            wall: env.stamp.recv_wall,
        };
        match env.body {
            MdEvent::Trade {
                inst,
                aggressor,
                px,
                qty,
                ..
            } => {
                self.trade(inst, aggressor, px, qty, at);
                Ok(())
            }
            MdEvent::Level {
                inst,
                book,
                side,
                px,
                qty,
            } if self.trading.get(&inst) == Some(&book) => {
                self.level(&env.body, inst, side, px, qty)
            }
            MdEvent::BookSnapshotEnd { inst, book } if self.trading.get(&inst) == Some(&book) => {
                self.snapshot_end(&env.body, inst)
            }
            ref body => self.books.apply(body).map(drop).map_err(SimError::Book),
        }
    }

    /// Acts on every command that arrived by `now`, a stamp the host read (decision 0043).
    pub fn advance(&mut self, now: MonoNs) {
        while let Some(entry) = self.in_flight.first_entry() {
            let (mono, _) = *entry.key();
            if mono > now {
                break;
            }
            let (wall, command) = entry.remove();
            let at = At { mono, wall };
            match command {
                Command::Place(p) => self.place(p, at),
                Command::Cancel(c) => self.cancel(c, at),
            }
        }
    }

    /// The answers produced since the last call, in the order the venue gave them.
    pub fn take_answers(&mut self) -> Vec<Answer> {
        core::mem::take(&mut self.out)
    }

    fn answer(&mut self, at: At, reply: Reply) {
        let frame = reply.encode(self.seq);
        self.seq += 1;
        let at = at.mono + self.latency.to_client;
        self.out.push(Answer { at, frame });
    }

    fn trading_book(&self, inst: InstrumentId) -> Option<&L2Book> {
        let book = *self.trading.get(&inst)?;
        self.books.get(inst, book)
    }

    /// The fee of a fill in the venue's fee sign, and its asset; `None` without a current
    /// rate, or when the notional or fee does not fit.
    fn fee(
        &self,
        inst: InstrumentId,
        liquidity: Liquidity,
        px: Ticks,
        qty: Lots,
        wall: WallNs,
    ) -> Option<(i128, AssetSym)> {
        let key = FeeKey {
            account: self.account,
            instrument: inst,
            channel: Channel::Public,
            liquidity,
        };
        let rate = self.fees.rate(&key, wall)?.0.0;
        let notional = self.specs.get(inst)?.notional(px, qty)?;
        // A model's f64 (0004): the rate is in basis points of the notional.
        let cost = (notional.nanos as f64 * rate / 10_000.0).round();
        // Never a saturated or NaN-as-zero fee (Codex r4182154747): it must be a finite number
        // of nanos strictly inside i128's range, since the decode scope refuses i128::MIN
        // (Codex r4182342652) and 2^127 is the first value past i128::MAX.
        let fits = cost.is_finite() && cost > i128::MIN as f64 && cost < i128::MAX as f64;
        let cost = fits.then_some(cost as i128)?;
        let raw = match self.fee_sign {
            VenueFeeSign::PositiveIsCost => Some(cost),
            VenueFeeSign::PositiveIsRebate => cost.checked_neg(),
        };
        Some((raw?, notional.asset))
    }

    /// The size of the modelled orders resting at `side` and `px` of `inst`.
    fn own(&self, inst: InstrumentId, side: BookSide, px: Ticks) -> Lots {
        let at = self
            .live
            .values()
            .filter(|o| o.inst == inst && o.side.book_side() == side && o.px == px);
        at.fold(Lots::ZERO, |sum, o| {
            let left = o.qty.checked_sub(o.cum).unwrap_or(Lots::ZERO);
            sum.checked_add(left).unwrap_or(sum)
        })
    }

    fn place(&mut self, p: Place, at: At) {
        match self.match_place(&p, at) {
            Ok(fill) => self.commit_place(p, fill, at),
            Err(refusal) => self.answer(
                at,
                Reply::Rejected {
                    rpc: p.rpc,
                    refusal,
                },
            ),
        }
    }

    /// What a placement does, decided before anything changes: the fills it takes, each with
    /// its fee, and the level size it rests behind, if it rests.
    fn match_place(&mut self, p: &Place, at: At) -> Result<Taken, Refusal> {
        if self.by_cid.contains_key(&p.cid) {
            return Err(Refusal::DuplicateClientId);
        }
        let book = self.trading_book(p.inst).ok_or(Refusal::NoBook)?;
        let top = book.top(usize::MAX).map_err(|_| Refusal::NoBook)?;
        let opposite = match p.side {
            Side::Buy => top.asks,
            Side::Sell => top.bids,
        };
        let within = |lvl: &&fbc_core::Lvl| match (p.px, p.side) {
            (None, _) => true,
            (Some(px), Side::Buy) => lvl.px <= px,
            (Some(px), Side::Sell) => lvl.px >= px,
        };
        if p.post_only && opposite.first().is_some_and(|l| within(&l)) {
            return Err(Refusal::PostOnlyWouldCross);
        }
        let mut crossed = Vec::new();
        let mut left = p.qty;
        for lvl in opposite.iter().filter(within) {
            let qty = lvl.qty.min(left);
            if qty == Lots::ZERO {
                break;
            }
            left = left.checked_sub(qty).unwrap_or(Lots::ZERO);
            crossed.push((lvl.px, qty));
        }
        // A fill-or-kill order that cannot fill whole takes nothing, so it needs no fee
        // (Codex r4182154743): fees are looked up only for the fills that will happen.
        if p.tif == TifTag::Fok && left > Lots::ZERO {
            crossed.clear();
            left = p.qty;
        }
        let mut takes = Vec::new();
        for (px, qty) in crossed {
            let fee = self.fee(p.inst, Liquidity::Taker, px, qty, at.wall);
            takes.push((px, qty, fee.ok_or(Refusal::NoFee)?));
        }
        let rests = match p.px {
            Some(px) if p.tif == TifTag::Gtc && left > Lots::ZERO => {
                let side = p.side.book_side();
                let book = self.trading_book(p.inst).ok_or(Refusal::NoBook)?;
                let shown = book.level(side, px).ok().flatten().ok_or(Refusal::NoBook)?;
                let shown = shown
                    .checked_add(self.own(p.inst, side, px))
                    .ok_or(Refusal::NoBook)?;
                let order = NewOrder {
                    side: p.side,
                    px,
                    channel: Channel::Public,
                    qty: left,
                };
                let queue = self
                    .queues
                    .entry(p.inst)
                    .or_insert_with(|| QueueModel::new(self.queue));
                queue
                    .accept_shown(OrderKey(self.orders), order, shown)
                    .map_err(|_| Refusal::NoBook)?;
                true
            }
            _ => false,
        };
        Ok(Taken { takes, rests })
    }

    fn commit_place(&mut self, p: Place, taken: Taken, at: At) {
        let n = self.orders;
        self.orders += 1;
        let vid = vid(n);
        self.by_cid.insert(p.cid.clone(), n);
        let accepted = Reply::Accepted {
            rpc: p.rpc,
            cid: p.cid.clone(),
            vid: vid.clone(),
        };
        self.answer(at, accepted);
        let mut cum = Lots::ZERO;
        for (px, qty, (fee, asset)) in taken.takes {
            cum = cum.checked_add(qty).unwrap_or(cum);
            let fill = self.fill_record(
                &p.cid,
                &vid,
                p.inst,
                p.side,
                px,
                qty,
                cum,
                Liquidity::Taker,
                fee,
                asset,
            );
            self.answer(at, Reply::Fill(fill));
        }
        let state = if taken.rests {
            SimState::Open
        } else if cum == p.qty {
            SimState::Filled
        } else {
            SimState::Canceled(CancelReason::Unfilled)
        };
        let order = Resting {
            cid: p.cid,
            inst: p.inst,
            side: p.side,
            px: p.px.unwrap_or(Ticks(0)),
            qty: p.qty,
            cum,
            post_only: p.post_only,
            reduce_only: p.reduce_only,
        };
        let event = order_event(&order, &vid, state, p.px);
        self.answer(at, Reply::Order(event));
        match state {
            SimState::Open => {
                self.live.insert(n, order);
            }
            SimState::Filled => {
                self.ended.insert(n, TerminalHint::Filled);
            }
            SimState::Canceled(_) => {
                self.ended.insert(n, TerminalHint::Canceled);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_record(
        &mut self,
        cid: &str,
        vid: &str,
        inst: InstrumentId,
        side: Side,
        px: Ticks,
        qty: Lots,
        cum: Lots,
        liquidity: Liquidity,
        fee: i128,
        asset: AssetSym,
    ) -> FillRecord {
        let fid = format!("F{}", self.fills);
        self.fills += 1;
        FillRecord {
            fid,
            cid: cid.to_owned(),
            vid: vid.to_owned(),
            inst,
            side,
            px,
            qty,
            cum,
            liquidity,
            fee,
            asset,
        }
    }

    fn cancel(&mut self, c: Cancel, at: At) {
        let n = match &c.target {
            Target::Venue(vid) => vid.strip_prefix('S').and_then(|n| n.parse().ok()),
            Target::Client(cid) => self.by_cid.get(cid).copied(),
        };
        let Some(order) = n.and_then(|n| self.live.remove(&n).map(|o| (n, o))) else {
            let refusal = match n.and_then(|n| self.ended.get(&n)) {
                Some(hint) => Refusal::Terminal(*hint),
                None => Refusal::NotFound,
            };
            return self.answer(
                at,
                Reply::Rejected {
                    rpc: c.rpc,
                    refusal,
                },
            );
        };
        let (n, order) = order;
        self.end(n, &order, TerminalHint::Canceled);
        let vid = vid(n);
        let accepted = Reply::Accepted {
            rpc: c.rpc,
            cid: order.cid.clone(),
            vid: vid.clone(),
        };
        self.answer(at, accepted);
        let state = SimState::Canceled(CancelReason::Requested);
        let event = order_event(&order, &vid, state, Some(order.px));
        self.answer(at, Reply::Order(event));
    }

    /// Takes order `n` out of its queue and records how it ended.
    fn end(&mut self, n: u64, order: &Resting, hint: TerminalHint) {
        if let Some(queue) = self.queues.get_mut(&order.inst) {
            queue.remove(OrderKey(n));
        }
        self.ended.insert(n, hint);
    }

    fn level(
        &mut self,
        ev: &MdEvent,
        inst: InstrumentId,
        side: BookSide,
        px: Ticks,
        qty: Lots,
    ) -> Result<(), SimError> {
        let before = self
            .trading_book(inst)
            .filter(|b| b.snapshot_in_progress().is_none())
            .and_then(|b| b.level(side, px).ok().flatten());
        self.books.apply(ev).map_err(SimError::Book)?;
        match before {
            Some(before) => self.level_changed(inst, side, px, before, qty),
            None => Ok(()),
        }
    }

    /// A replacement snapshot of `inst`'s trading book completed: each level a resting order
    /// sits at changed from its size in the book replaced to its size in the snapshot, as a
    /// delta would have changed it (Codex r4182154723), and the trades printed before it
    /// explain no later change.
    fn snapshot_end(&mut self, ev: &MdEvent, inst: InstrumentId) -> Result<(), SimError> {
        let held: BTreeSet<(bool, Ticks)> = self
            .live
            .values()
            .filter(|o| o.inst == inst)
            .map(|o| (o.side == Side::Buy, o.px))
            .collect();
        let side = |bid| if bid { BookSide::Bid } else { BookSide::Ask };
        let size = |engine: &SimEngine, bid, px| {
            let book = engine.trading_book(inst);
            book.and_then(|b| b.level(side(bid), px).ok().flatten())
        };
        let before: Vec<_> = held.iter().map(|&(bid, px)| size(self, bid, px)).collect();
        self.books.apply(ev).map_err(SimError::Book)?;
        let mut changed = Ok(());
        for (&(bid, px), before) in held.iter().zip(before) {
            if let (Some(before), Some(after)) = (before, size(self, bid, px)) {
                changed = changed.and(self.level_changed(inst, side(bid), px, before, after));
            }
        }
        self.traded.retain(|key, _| key.0 != inst);
        changed
    }

    /// The trading book's level at `side` and `px` changed from `before` to `after`. The
    /// trades printed at it since its last change explain a shrink up to their size, and the
    /// rest of the shrink is a level cancel; the change ends what those trades explain, so
    /// none is carried to a later change (Codex r4182154731).
    fn level_changed(
        &mut self,
        inst: InstrumentId,
        side: BookSide,
        px: Ticks,
        before: Lots,
        after: Lots,
    ) -> Result<(), SimError> {
        let traded = self.traded.remove(&(inst, side == BookSide::Bid, px));
        let shrunk = before.checked_sub(after).unwrap_or(Lots::ZERO);
        let cancelled = shrunk
            .checked_sub(traded.unwrap_or(Lots::ZERO))
            .unwrap_or(Lots::ZERO);
        let level_before = before
            .checked_add(self.own(inst, side, px))
            .unwrap_or(before);
        match self.queues.get_mut(&inst) {
            Some(queue) if cancelled > Lots::ZERO => queue
                .level_cancel(side, px, cancelled, level_before)
                .map_err(SimError::Queue),
            _ => Ok(()),
        }
    }

    fn trade(&mut self, inst: InstrumentId, aggressor: Aggressor, px: Ticks, qty: Lots, at: At) {
        let Some(book) = self.trading_book(inst) else {
            return;
        };
        let taker = match aggressor {
            Aggressor::Buyer => Side::Buy,
            Aggressor::Seller => Side::Sell,
            Aggressor::Unknown => {
                let Ok(touch) = book.touch() else {
                    return;
                };
                if touch.ask.is_some_and(|a| px >= a.px) {
                    Side::Buy
                } else if touch.bid.is_some_and(|b| px <= b.px) {
                    Side::Sell
                } else {
                    return;
                }
            }
        };
        let key = (inst, taker == Side::Sell, px);
        let traded = self.traded.get(&key).copied().unwrap_or(Lots::ZERO);
        self.traded
            .insert(key, traded.checked_add(qty).unwrap_or(traded));
        let Some(queue) = self.queues.get_mut(&inst) else {
            return;
        };
        let view = TradeView {
            taker,
            px,
            qty,
            channel: Channel::Public,
        };
        // The queue model holds only resting orders, so each fill names one.
        for fill in queue.trade(view) {
            if let Some(order) = self.live.remove(&fill.key.0) {
                self.maker_fill(fill, order, at);
            }
        }
    }

    fn maker_fill(&mut self, fill: SimFill, mut order: Resting, at: At) {
        let n = fill.key.0;
        let vid = vid(n);
        let Some((fee, asset)) = self.fee(order.inst, Liquidity::Maker, fill.px, fill.qty, at.wall)
        else {
            // No rate to charge it at: the venue cancels the order rather than fill it.
            self.end(n, &order, TerminalHint::Canceled);
            let state = SimState::Canceled(CancelReason::Venue);
            let event = order_event(&order, &vid, state, Some(order.px));
            return self.answer(at, Reply::Order(event));
        };
        order.cum = order.cum.checked_add(fill.qty).unwrap_or(order.cum);
        let record = self.fill_record(
            &order.cid,
            &vid,
            order.inst,
            order.side,
            fill.px,
            fill.qty,
            order.cum,
            Liquidity::Maker,
            fee,
            asset,
        );
        self.answer(at, Reply::Fill(record));
        let state = if fill.remaining == Lots::ZERO {
            self.ended.insert(n, TerminalHint::Filled);
            SimState::Filled
        } else {
            SimState::Open
        };
        let event = order_event(&order, &vid, state, Some(order.px));
        self.answer(at, Reply::Order(event));
        if state == SimState::Open {
            self.live.insert(n, order);
        }
    }
}

/// What a placement takes before it changes anything: its fills, `(px, qty, (fee, asset))`,
/// and whether its rest was queued.
struct Taken {
    takes: Vec<(Ticks, Lots, (i128, AssetSym))>,
    rests: bool,
}

/// The venue's id for its `n`th order.
fn vid(n: u64) -> String {
    format!("S{n}")
}

fn order_event(order: &Resting, vid: &str, state: SimState, px: Option<Ticks>) -> OrderEvent {
    OrderEvent {
        cid: order.cid.clone(),
        vid: vid.to_owned(),
        inst: order.inst,
        side: order.side,
        state,
        cum: order.cum,
        px,
        qty: order.qty,
        post_only: order.post_only,
        reduce_only: order.reduce_only,
    }
}

/// `wall` plus `d`, saturating.
fn after(wall: WallNs, d: Duration) -> WallNs {
    let nanos = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
    WallNs(wall.0.saturating_add(nanos))
}
