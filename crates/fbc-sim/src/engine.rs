//! SimVenue's matching engine: a pure state machine over the shard's market-data envelopes and
//! the frames its codec writes, answering with frames after the configured latency (decision
//! 0046).

use core::fmt;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_book::{BookError, Books, L2Book};
use fbc_core::{
    AccountKey, Aggressor, AmendCaps, AmendQty, AssetSym, BookId, BookSide, CancelReason, Channel,
    Envelope, FeeBook, FeeKey, InstrumentId, Liquidity, Lots, MdEvent, MonoNs, NotAmendable, Side,
    SpecTable, TerminalHint, Ticks, TifTag, VenueFeeSign, VenueOrderId, WallNs,
};

use crate::config::{SimConfig, SimLatency};
use crate::queue::{
    NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, QueuePos, SimFill, TradeView,
};
use crate::wire::{
    Amend, Command, FillRecord, ItemResult, OrderEvent, Place, Query, Refusal, Reply, SimState,
    Target, WireError,
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
    /// An injected order on an instrument the consumer configured no trading book for, or
    /// whose book the engine has not built yet; nothing was injected.
    NoBook(InstrumentId),
    /// An injected order the queue model refused (no size, a level the book does not know, or
    /// lots past an `i64` there); nothing was injected.
    Queue(QueueError),
    /// A withdrawal naming no order the engine holds as injected.
    NotInjected(OrderKey),
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimError::Malformed(what) => write!(f, "simulated order-entry frame: bad {what}"),
            SimError::Book(e) => write!(f, "simulated venue's book: {e}"),
            SimError::NoBook(inst) => {
                write!(f, "no simulated trading book for instrument {}", inst.get())
            }
            SimError::Queue(e) => write!(f, "injected order: {e}"),
            SimError::NotInjected(key) => write!(f, "order {} is not injected", key.0),
        }
    }
}

impl std::error::Error for SimError {}

/// An order another process placed on the venue SimVenue stands in for (design §10.2 step 2:
/// Java's, in calibration), which the consumer injects so the engine models it as its own
/// ([`SimEngine::inject`]). It rests in the real book, so the engine's books show it: unlike
/// SimVenue's own orders, its size is never added to the level the book shows.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct InjectedOrder {
    pub inst: InstrumentId,
    pub side: Side,
    pub px: Ticks,
    pub qty: Lots,
}

/// An injected order the engine holds, and how much of it the engine's book has not shown yet
/// (Codex r4186502479): all of it when injected, as it arrives at the venue; the level's
/// growths show it, in injection order, and its fills come out of what is not shown first,
/// since the book shows an order net of its fills.
#[derive(Copy, Clone, Debug)]
struct Injected {
    order: InjectedOrder,
    unshown: Lots,
}

impl Injected {
    fn at(&self, inst: InstrumentId, side: BookSide, px: Ticks) -> bool {
        let o = &self.order;
        o.inst == inst && o.side.book_side() == side && o.px == px
    }
}

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

/// An order the venue ended, as a query reports it: its last values, its price (`None` for a
/// market order) and its final state.
#[derive(Clone, Debug)]
struct Ended {
    order: Resting,
    px: Option<Ticks>,
    state: SimState,
}

impl Ended {
    fn hint(&self) -> TerminalHint {
        match self.state {
            SimState::Filled => TerminalHint::Filled,
            _ => TerminalHint::Canceled,
        }
    }
}

/// An instant on both of the host's clocks, from a stamp or a frame's encode time.
#[derive(Copy, Clone, Debug)]
struct At {
    mono: MonoNs,
    wall: WallNs,
}

/// An order to match: a placement, or the rest of an amended order.
struct Match {
    inst: InstrumentId,
    side: Side,
    px: Option<Ticks>,
    qty: Lots,
    tif: TifTag,
    post_only: bool,
}

/// What one item of a request did: its outcome, and the events that follow it.
struct Item {
    result: ItemResult,
    events: Vec<Reply>,
}

impl Item {
    fn refused(refusal: Refusal) -> Item {
        Item {
            result: ItemResult::Rejected(refusal),
            events: Vec::new(),
        }
    }

    fn accepted(cid: String, n: u64, events: Vec<Reply>) -> Item {
        let vid = vid(n);
        Item {
            result: ItemResult::Accepted { cid, vid },
            events,
        }
    }
}

/// SimVenue's engine. It holds its own `fbc-book` books, built from the envelopes it is fed
/// (never the runtime's), one [`QueueModel`] per instrument, the orders its codec placed and
/// those the consumer injected. It reads no clock: a command acts at its encode time (which the
/// codec writes in its frame) plus [`SimLatency::to_venue`], an envelope at its stamp, and
/// answers are due [`SimLatency::to_client`] after the venue acted. Commands act in arrival
/// order, each before any envelope stamped at or after its arrival, and are held until such an
/// envelope (or [`advance`](SimEngine::advance)) passes them.
///
/// - A place for an instrument the spec table does not list is refused (`no_book`), since its
///   size cannot be judged. A place of zero lots is refused (`InvalidQty`), since zero lots is
///   never an order, and so is one below the spec's minimum size or above its largest order,
///   and one that would rest where the level's size with the simulated orders there and its
///   own would not fit in lots. A limit price the spec's grid does not accept is refused
///   (`InvalidPrice`).
/// - A place crosses the trading book's displayed levels, each fill at a level's price up to
///   its size, as the taker; a post-only order that would cross is refused
///   (`PostOnlyWouldCross`). The rest of a good-till-cancelled limit order rests and queues
///   (decision 0038); the rest of an immediate-or-cancel, fill-or-kill or market order is
///   cancelled unfilled, and a fill-or-kill order that cannot fill whole fills nothing. A
///   good-till-cancelled order that would rest where the book does not know the size (past
///   its depth or window) is refused whole (`no_book`), the part that would cross included,
///   since its queue position cannot be set. A resting order queues behind the level's size
///   less the trades printed at its price whose shrink the level has not shown yet; a trade
///   printed through a level counts as taking all of it.
/// - A batch of places is each item placed in turn, at the same instant, and answered with
///   one outcome per item in one frame, before the items' fills and order events.
/// - An amend changes a resting order's price, size and flags as the stood-in venue's
///   [`AmendCaps`] allow (an instrument, side or time in force it would change, or a field
///   they do not amend, is `NotAmendable(Unsupported)`; a partially filled order where they do
///   not amend one, `NotAmendable(PartiallyFilled)`), its quantity read under their
///   [`AmendQty`]: a total at or below what the order has filled, or a remaining quantity of
///   zero, is `InvalidQty`. The amended order keeps its queue position only where the caps
///   declare `keeps_priority: Some(true)` and its price is unchanged; otherwise (design §4.5:
///   `None` is not measured yet, and the simulator resets) it is matched again as a new order
///   at its price and queues behind the level's size there, crossing the book as a placement
///   would. Where the caps say a rejected amend does not keep the original, the venue cancels
///   the order it refused to amend.
/// - A cancel ends a resting order; one the venue has ended is refused `AlreadyTerminal`, one
///   it never had `NotFound`. A batch of cancels is each cancelled in turn and answered with
///   one outcome per item in one frame. A cancel-all of an instrument ends every resting order
///   SimVenue placed on that instrument and no other; injected orders are another process's
///   and never end.
/// - A query answers with the order as the venue holds it, or as it ended, or with none.
/// - A public trade on a trading book fills resting orders, SimVenue's own and injected ones,
///   through the queue model, as the maker. A trade the venue gives no aggressor for is
///   classified against the touch (at or above the offer a buy, at or below the bid a sell),
///   and ignored inside the spread. An injected order's fills are never answered on the
///   order-entry stream ([`take_injected_fills`](SimEngine::take_injected_fills)). An
///   injected order is in the real book, but the engine's book shows it only from the growth
///   of its level that follows its injection (in injection order, net of its fills); until
///   then its size is added to the level, as SimVenue's own orders' always is.
/// - A trading book's level that shrinks by more than the trades at its price since it last
///   changed (and the injected orders withdrawn from it) is a level cancel for the queue
///   model, whether a delta or a replacement snapshot shrinks it; each change ends what those
///   trades explain, and so does every replacement snapshot, which restates the level even
///   where it repeats its size or cannot compare sizes, and a delta that first shows a level
///   the book did not reach.
/// - Every fill's fee is the fee book's rate for the account, instrument, public channel and
///   liquidity at the fill's wall time, times its notional, rounded to the nano, written in the
///   stood-in venue's fee sign so the codec's [`DecodeScope`](fbc_core::DecodeScope) reads it
///   back as a cost (0004); a fee that is not a finite number of nanos strictly inside `i128`'s
///   range is no fee. A place or amend that would take a fill without one is refused
///   (`no_fee`); a resting order a trade would fill without a rate is cancelled by the venue
///   instead, and the trade's size it would have taken goes to the orders behind it. An
///   injected order's fills need no fee.
#[derive(Clone, Debug)]
pub struct SimEngine {
    fee_sign: VenueFeeSign,
    amend: Option<AmendCaps>,
    latency: SimLatency,
    queue: QueueConfig,
    account: AccountKey,
    fees: FeeBook,
    specs: SpecTable,
    trading: BTreeMap<InstrumentId, BookId>,
    books: Books,
    queues: BTreeMap<InstrumentId, QueueModel>,
    live: BTreeMap<u64, Resting>,
    ended: BTreeMap<u64, Ended>,
    by_cid: BTreeMap<String, u64>,
    injected: BTreeMap<u64, Injected>,
    injected_fills: Vec<SimFill>,
    in_flight: BTreeMap<(MonoNs, u64), (WallNs, Command)>,
    /// Public size a level lost to trades printed at it, and to injected orders withdrawn from
    /// it, since it last changed, by (instrument, bid side, price).
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
            amend: config.exec.order.amend,
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
            injected: BTreeMap::new(),
            injected_fills: Vec::new(),
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

    /// Acts on every command that arrived by `now`, a stamp the host read (decision 0046).
    pub fn advance(&mut self, now: MonoNs) {
        while let Some(entry) = self.in_flight.first_entry() {
            let (mono, _) = *entry.key();
            if mono > now {
                break;
            }
            let (wall, command) = entry.remove();
            let at = At { mono, wall };
            match command {
                Command::Place(p) => {
                    let rpc = p.rpc;
                    let item = self.place(p, at);
                    self.answer_item(rpc, item, at);
                }
                Command::Batch(head, places) => {
                    let items = places.into_iter().map(|p| self.place(p, at)).collect();
                    self.answer_items(head.rpc, items, at);
                }
                Command::Amend(a) => {
                    let rpc = a.head.rpc;
                    let item = self.amend(a, at);
                    self.answer_item(rpc, item, at);
                }
                Command::Cancel(c) => {
                    let item = self.cancel(&c.target);
                    self.answer_item(c.rpc, item, at);
                }
                Command::Cancels(head, targets) => {
                    let items = targets.iter().map(|t| self.cancel(t)).collect();
                    self.answer_items(head.rpc, items, at);
                }
                Command::CancelAll(head, inst) => self.cancel_all(head.rpc, inst, at),
                Command::Query(q) => self.query(q, at),
            }
        }
    }

    /// The answers produced since the last call, in the order the venue gave them.
    pub fn take_answers(&mut self) -> Vec<Answer> {
        core::mem::take(&mut self.out)
    }

    /// Injects an order another process placed (design §10.2 step 2), arriving at the venue at
    /// `now`: the commands that arrived by then act first. It queues behind its level's size as
    /// the engine's book shows it then, before any update that shows the injected order itself,
    /// less the trades printed there whose shrink the level has not shown yet, as SimVenue's
    /// own orders queue, and it is a modelled order like them: never counted in a later
    /// order's size ahead, matched in arrival order with them, and filled by trades through the
    /// queue model. It is never answered on the order-entry stream, so it never reaches the
    /// consumer's handler as one of its orders, and no command of the codec's can name it.
    /// Gives the key it is modelled under. Refused, injecting nothing, when the instrument has
    /// no trading book yet, or when the queue model refuses it.
    pub fn inject(&mut self, now: MonoNs, order: InjectedOrder) -> Result<OrderKey, SimError> {
        self.advance(now);
        let (side, px) = (order.side.book_side(), order.px);
        let book = self.trading_book(order.inst);
        let book = book.ok_or(SimError::NoBook(order.inst))?;
        let shown = book
            .level(side, px)
            .map_err(|e| SimError::Queue(QueueError::Book(e)))?
            .ok_or(SimError::Queue(QueueError::UnknownLevel { side, px }))?;
        // SimVenue's own orders are not in the real book, so they are added, and so are the
        // injected ones it does not show yet; the queue model takes every modelled order out.
        // The level, they and this order must fit in lots together (Codex r4186502501).
        let shown = self
            .unshown(order.inst, side, px, shown)
            .checked_add(self.own(order.inst, side, px))
            .filter(|shown| shown.checked_add(order.qty).is_some())
            .ok_or(SimError::Queue(QueueError::Overflow { side, px }))?;
        let key = OrderKey(self.orders);
        let modelled = NewOrder {
            side: order.side,
            px,
            channel: Channel::Public,
            qty: order.qty,
        };
        let queue = self.queue_of(order.inst);
        queue
            .accept_shown(key, modelled, shown)
            .map_err(SimError::Queue)?;
        self.orders += 1;
        let unshown = order.qty;
        self.injected.insert(key.0, Injected { order, unshown });
        Ok(key)
    }

    /// The process that placed injected order `key` cancelled it at `now`: the commands that
    /// arrived by then act first, and the order leaves its queue. The size it had left that
    /// the book shows is taken from its level as a trade's is, so the book's shrink when it
    /// shows the cancellation moves no other order, which never had that size ahead of it;
    /// what the book never showed never leaves it. Refused for a key the engine does not hold
    /// as injected (one never injected, or since filled whole).
    pub fn withdraw(&mut self, now: MonoNs, key: OrderKey) -> Result<(), SimError> {
        self.advance(now);
        let held = self
            .injected
            .remove(&key.0)
            .ok_or(SimError::NotInjected(key))?;
        let order = held.order;
        let pos = self.queue_of(order.inst).remove(key);
        let left = pos.map_or(Lots::ZERO, |pos| pos.remaining);
        let left = left.checked_sub(held.unshown).unwrap_or(Lots::ZERO);
        let level = (order.inst, order.side == Side::Buy, order.px);
        let taken = self.traded.entry(level).or_insert(Lots::ZERO);
        *taken = taken.checked_add(left).unwrap_or(MAX_LOTS);
        Ok(())
    }

    /// Where injected order `key` sits, while the engine holds it.
    pub fn injected(&self, key: OrderKey) -> Option<QueuePos> {
        let held = self.injected.get(&key.0)?;
        self.queues.get(&held.order.inst)?.position(key)
    }

    /// The injected orders' fills since the last call, in the order trades gave them.
    pub fn take_injected_fills(&mut self) -> Vec<SimFill> {
        core::mem::take(&mut self.injected_fills)
    }

    /// Where the resting order the venue named `vid` sits in its queue; `None` for an order it
    /// does not hold resting.
    pub fn queue_position(&self, vid: &VenueOrderId) -> Option<QueuePos> {
        let n = self.lookup(&Target::Venue(vid.as_str().to_owned()))?;
        let order = self.live.get(&n)?;
        self.queues.get(&order.inst)?.position(OrderKey(n))
    }

    fn answer(&mut self, at: At, reply: Reply) {
        let frame = reply.encode(self.seq);
        self.seq += 1;
        let at = at.mono + self.latency.to_client;
        self.out.push(Answer { at, frame });
    }

    /// A single command's outcome, then its events.
    fn answer_item(&mut self, rpc: u64, item: Item, at: At) {
        let reply = match item.result {
            ItemResult::Accepted { cid, vid } => Reply::Accepted { rpc, cid, vid },
            ItemResult::Rejected(refusal) => Reply::Rejected { rpc, refusal },
        };
        self.answer(at, reply);
        for event in item.events {
            self.answer(at, event);
        }
    }

    /// A batch's outcomes, every item's in one frame, then each item's events in turn.
    fn answer_items(&mut self, rpc: u64, items: Vec<Item>, at: At) {
        let (results, events): (Vec<_>, Vec<_>) =
            items.into_iter().map(|i| (i.result, i.events)).unzip();
        self.answer(
            at,
            Reply::Items {
                rpc,
                items: results,
            },
        );
        for event in events.into_iter().flatten() {
            self.answer(at, event);
        }
    }

    fn trading_book(&self, inst: InstrumentId) -> Option<&L2Book> {
        let book = *self.trading.get(&inst)?;
        self.books.get(inst, book)
    }

    fn queue_of(&mut self, inst: InstrumentId) -> &mut QueueModel {
        let config = self.queue;
        self.queues
            .entry(inst)
            .or_insert_with(|| QueueModel::new(config))
    }

    /// `shown`, the size the book shows at a level, less what trades printed there (or
    /// injected orders withdrawn from it) took before the book showed it.
    fn unshown(&self, inst: InstrumentId, side: BookSide, px: Ticks, shown: Lots) -> Lots {
        let credit = self.traded.get(&(inst, side == BookSide::Bid, px));
        let credit = credit.copied().unwrap_or(Lots::ZERO);
        shown.checked_sub(credit).unwrap_or(Lots::ZERO)
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
        // `raw` is the venue's wire number in the venue's own sign, not a `Fee`: the codec
        // turns it into one through `DecodeScope::fee`, the only place a fee's sign is set.
        let raw = match self.fee_sign {
            VenueFeeSign::PositiveIsCost => Some(cost),
            VenueFeeSign::PositiveIsRebate => cost.checked_neg(),
        };
        Some((raw?, notional.asset))
    }

    /// The modelled size at `side` and `px` of `inst` that the real book does not show:
    /// SimVenue's own resting orders, which it never shows, and the injected orders' size it
    /// has not shown yet. It fits in lots, since a placement or injection that would push its
    /// level past them is refused; it would saturate.
    fn own(&self, inst: InstrumentId, side: BookSide, px: Ticks) -> Lots {
        let at = self
            .live
            .values()
            .filter(|o| o.inst == inst && o.side.book_side() == side && o.px == px)
            .map(|o| o.qty.checked_sub(o.cum).unwrap_or(Lots::ZERO));
        let unshown = self.injected.values().filter(|i| i.at(inst, side, px));
        let at = at.chain(unshown.map(|i| i.unshown));
        at.fold(Lots::ZERO, |sum, left| {
            sum.checked_add(left).unwrap_or(MAX_LOTS)
        })
    }

    /// The order the venue knows by `target`, live or ended.
    fn lookup(&self, target: &Target) -> Option<u64> {
        match target {
            // Only the exact spelling the venue gave (Codex r4182991987): "S00" is not "S0".
            Target::Venue(v) => {
                let n = v.strip_prefix('S').and_then(|n| n.parse().ok());
                n.filter(|&n| vid(n) == *v)
            }
            Target::Client(cid) => self.by_cid.get(cid).copied(),
        }
    }

    /// The refusal of a command naming order `n`, which does not rest: it ended, or the venue
    /// never had it.
    fn missing(&self, n: Option<u64>) -> Refusal {
        match n.and_then(|n| self.ended.get(&n)) {
            Some(ended) => Refusal::Terminal(ended.hint()),
            None => Refusal::NotFound,
        }
    }

    fn place(&mut self, p: Place, at: At) -> Item {
        let key = OrderKey(self.orders);
        let m = Match {
            inst: p.inst,
            side: p.side,
            px: p.px,
            qty: p.qty,
            tif: p.tif,
            post_only: p.post_only,
        };
        let taken = self
            .check_place(&p)
            .and_then(|()| self.match_order(&m, key, at));
        match taken {
            Ok(taken) => self.commit_place(p, taken),
            Err(refusal) => Item::refused(refusal),
        }
    }

    fn check_place(&self, p: &Place) -> Result<(), Refusal> {
        if self.by_cid.contains_key(&p.cid) {
            return Err(Refusal::DuplicateClientId);
        }
        self.check_order(p.inst, p.qty, p.px)
    }

    /// An order of `qty` lots in total at `px` judged by its instrument's spec.
    fn check_order(&self, inst: InstrumentId, qty: Lots, px: Option<Ticks>) -> Result<(), Refusal> {
        // The venue judges an order's size by its spec, so with none it places nothing
        // (Codex r4183669448).
        let spec = self.specs.get(inst).ok_or(Refusal::NoBook)?;
        // Zero lots is never an order (Codex r4182678519; `InstrumentSpec::floor_qty`), nor is
        // one below the spec's minimum or above its largest order (Codex r4183669448).
        let too_big = spec.max_order_size.is_some_and(|max| qty > max);
        if qty == Lots::ZERO || qty < spec.min_size || too_big {
            return Err(Refusal::InvalidQty);
        }
        // A limit price must be one the grid accepts (Codex r4184245564).
        if px.is_some_and(|px| !spec.price_grid.valid_at(px)) {
            return Err(Refusal::InvalidPrice);
        }
        Ok(())
    }

    /// What an order does, decided before anything but its queue changes: the fills it takes,
    /// each with its fee, and whether its rest was queued under `key`.
    fn match_order(&mut self, p: &Match, key: OrderKey, at: At) -> Result<Taken, Refusal> {
        let book = self.trading_book(p.inst).ok_or(Refusal::NoBook)?;
        let top = book.top(usize::MAX).map_err(|_| Refusal::NoBook)?;
        let (opposite, maker_bid) = match p.side {
            Side::Buy => (top.asks, false),
            Side::Sell => (top.bids, true),
        };
        // What each opposite level still holds: trades printed there, or through it, whose
        // shrink the book has not shown yet took their size first (Codex r4185186386), so a
        // level they emptied is neither crossed nor taken from.
        let opposite: Vec<(Ticks, Lots)> = opposite
            .iter()
            .map(|lvl| {
                let credit = self.traded.get(&(p.inst, maker_bid, lvl.px)).copied();
                let left = lvl.qty.checked_sub(credit.unwrap_or(Lots::ZERO));
                (lvl.px, left.unwrap_or(Lots::ZERO))
            })
            .filter(|&(_, qty)| qty > Lots::ZERO)
            .collect();
        let within = |&&(lvl_px, _): &&(Ticks, Lots)| match (p.px, p.side) {
            (None, _) => true,
            (Some(px), Side::Buy) => lvl_px <= px,
            (Some(px), Side::Sell) => lvl_px >= px,
        };
        if p.post_only && opposite.first().is_some_and(|l| within(&l)) {
            return Err(Refusal::PostOnlyWouldCross);
        }
        let mut crossed = Vec::new();
        let mut left = p.qty;
        for &(px, size) in opposite.iter().filter(within) {
            let qty = size.min(left);
            if qty == Lots::ZERO {
                break;
            }
            left = left.checked_sub(qty).unwrap_or(Lots::ZERO);
            crossed.push((px, qty));
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
                // Trades printed at the price whose shrink the level has not shown yet took
                // their size before the order arrived (Codex r4184245578): it queues behind
                // what they left, and that shrink, which they explain, moves nothing.
                let shown = self.unshown(p.inst, side, px, shown);
                // Codex r4183438501: the size at the price, the public size, every simulated
                // order resting there and this one together, must fit in lots, so the venue's
                // own size there is never truncated.
                let shown = shown
                    .checked_add(self.own(p.inst, side, px))
                    .filter(|shown| shown.checked_add(left).is_some())
                    .ok_or(Refusal::InvalidQty)?;
                let order = NewOrder {
                    side: p.side,
                    px,
                    channel: Channel::Public,
                    qty: left,
                };
                self.queue_of(p.inst)
                    .accept_shown(key, order, shown)
                    .map_err(|_| Refusal::NoBook)?;
                true
            }
            _ => false,
        };
        Ok(Taken { takes, rests })
    }

    fn commit_place(&mut self, p: Place, taken: Taken) -> Item {
        let n = self.orders;
        self.orders += 1;
        self.by_cid.insert(p.cid.clone(), n);
        let order = Resting {
            cid: p.cid.clone(),
            inst: p.inst,
            side: p.side,
            px: p.px.unwrap_or(Ticks(0)),
            qty: p.qty,
            cum: Lots::ZERO,
            post_only: p.post_only,
            reduce_only: p.reduce_only,
        };
        let events = self.commit(n, order, p.px, taken);
        Item::accepted(p.cid, n, events)
    }

    /// Order `n`, as `order` with its price `px`, takes `taken`'s fills as the taker and rests
    /// or ends: its fill and order events.
    fn commit(
        &mut self,
        n: u64,
        mut order: Resting,
        px: Option<Ticks>,
        taken: Taken,
    ) -> Vec<Reply> {
        let vid = vid(n);
        let mut events = Vec::new();
        for (fill_px, qty, fee) in taken.takes {
            order.cum = order.cum.checked_add(qty).unwrap_or(order.cum);
            let fill = self.fill_record(&order, &vid, fill_px, qty, Liquidity::Taker, fee);
            events.push(Reply::Fill(fill));
        }
        let state = if taken.rests {
            SimState::Open
        } else if order.cum == order.qty {
            SimState::Filled
        } else {
            SimState::Canceled(CancelReason::Unfilled)
        };
        events.push(Reply::Order(order_event(&order, &vid, state, px)));
        if state == SimState::Open {
            self.live.insert(n, order);
        } else {
            self.ended.insert(n, Ended { order, px, state });
        }
        events
    }

    fn fill_record(
        &mut self,
        order: &Resting,
        vid: &str,
        px: Ticks,
        qty: Lots,
        liquidity: Liquidity,
        fee: (i128, AssetSym),
    ) -> FillRecord {
        let fid = format!("F{}", self.fills);
        self.fills += 1;
        FillRecord {
            fid,
            cid: order.cid.clone(),
            vid: vid.to_owned(),
            inst: order.inst,
            side: order.side,
            px,
            qty,
            cum: order.cum,
            liquidity,
            fee: fee.0,
            asset: fee.1,
        }
    }

    fn amend(&mut self, a: Amend, at: At) -> Item {
        let n = self.lookup(&a.target);
        let found = n.and_then(|n| self.live.get(&n).map(|o| (n, o.clone())));
        let Some((n, order)) = found else {
            return Item::refused(self.missing(n));
        };
        match self.match_amend(n, &order, &a, at) {
            Ok((amended, taken)) => {
                let events = self.commit(n, amended, Some(a.px), taken);
                Item::accepted(order.cid, n, events)
            }
            // A venue whose refused amend does not keep the original cancels it.
            Err(refusal) if self.amend.is_some_and(|caps| !caps.reject_keeps_original) => {
                self.live.remove(&n);
                let event = self.end(n, order, SimState::Canceled(CancelReason::Venue));
                Item {
                    result: ItemResult::Rejected(refusal),
                    events: vec![event],
                }
            }
            Err(refusal) => Item::refused(refusal),
        }
    }

    /// What amending resting order `n` (`order`) as `a` does, decided before anything but its
    /// queue changes: the order as amended, and what it takes and whether it rests.
    fn match_amend(
        &mut self,
        n: u64,
        order: &Resting,
        a: &Amend,
        at: At,
    ) -> Result<(Resting, Taken), Refusal> {
        let unsupported = Refusal::NotAmendable(NotAmendable::Unsupported);
        let caps = self.amend.ok_or(unsupported)?;
        // Every resting order is a good-till-cancelled limit order; an amend changes its
        // price, size and flags, never its instrument, side or time in force.
        if a.inst != order.inst || a.side != order.side || a.tif != TifTag::Gtc {
            return Err(unsupported);
        }
        let rest = match caps.qty_semantics {
            AmendQty::TotalIncludingFilled => a.qty.checked_sub(order.cum),
            AmendQty::Remaining => Some(a.qty),
        };
        let rest = rest
            .filter(|rest| *rest > Lots::ZERO)
            .ok_or(Refusal::InvalidQty)?;
        let total = order.cum.checked_add(rest).ok_or(Refusal::InvalidQty)?;
        let flags = (a.post_only, a.reduce_only) != (order.post_only, order.reduce_only);
        let priced = a.px != order.px;
        // The size changes as the venue reads its own quantity: the total, or what remains.
        let sized = match caps.qty_semantics {
            AmendQty::TotalIncludingFilled => total != order.qty,
            AmendQty::Remaining => Some(rest) != order.qty.checked_sub(order.cum),
        };
        if (priced && !caps.price) || (sized && !caps.qty) || (flags && !caps.flags) {
            return Err(unsupported);
        }
        if order.cum > Lots::ZERO && !caps.when_partially_filled {
            return Err(Refusal::NotAmendable(NotAmendable::PartiallyFilled));
        }
        self.check_order(a.inst, total, Some(a.px))?;
        let amended = Resting {
            px: a.px,
            qty: total,
            post_only: a.post_only,
            reduce_only: a.reduce_only,
            ..order.clone()
        };
        let (key, side) = (OrderKey(n), a.side.book_side());
        // Design §4.5: the order keeps its place only where the venue is known to keep it, and
        // only at its price; a place at another price is no place to keep.
        if caps.keeps_priority == Some(true) && !priced {
            let left = order.qty.checked_sub(order.cum).unwrap_or(Lots::ZERO);
            // As a placement (Codex r4183438501): a larger rest must fit in lots with the
            // level's size and the other modelled orders there, so the level must be known
            // (Codex r4186502494); a smaller one fits where the order did.
            if rest > left {
                let book = self.trading_book(a.inst);
                let shown = book.and_then(|b| b.level(side, a.px).ok().flatten());
                let shown = shown.ok_or(Refusal::NoBook)?;
                let others = self.own(a.inst, side, a.px).checked_sub(left);
                let fits = others
                    .and_then(|o| o.checked_add(shown))
                    .and_then(|o| o.checked_add(rest));
                fits.ok_or(Refusal::InvalidQty)?;
            }
            self.queue_of(a.inst).set_remaining(key, rest);
            let taken = Taken {
                takes: Vec::new(),
                rests: true,
            };
            return Ok((amended, taken));
        }
        // Otherwise it is matched as a new order at its price, without its old place or its
        // old size at the venue, both put back if the venue refuses it.
        let queue = self.queues.get(&a.inst).cloned();
        let resting = self.live.remove(&n);
        self.queue_of(a.inst).remove(key);
        let m = Match {
            inst: a.inst,
            side: a.side,
            px: Some(a.px),
            qty: rest,
            tif: TifTag::Gtc,
            post_only: a.post_only,
        };
        let taken = self.match_order(&m, key, at);
        if taken.is_err() {
            self.queues.extend(queue.map(|q| (a.inst, q)));
            self.live.extend(resting.map(|o| (n, o)));
        }
        Ok((amended, taken?))
    }

    fn cancel(&mut self, target: &Target) -> Item {
        let n = self.lookup(target);
        let found = n.and_then(|n| self.live.remove(&n).map(|o| (n, o)));
        let Some((n, order)) = found else {
            return Item::refused(self.missing(n));
        };
        let cid = order.cid.clone();
        let event = self.end(n, order, SimState::Canceled(CancelReason::Requested));
        Item::accepted(cid, n, vec![event])
    }

    /// Ends every resting order SimVenue placed on `inst`, and nothing else (an instrument
    /// cancel-all is never widened to the account, and injected orders are not SimVenue's).
    fn cancel_all(&mut self, rpc: u64, inst: InstrumentId, at: At) {
        let (covered, rest): (BTreeMap<_, _>, _) = core::mem::take(&mut self.live)
            .into_iter()
            .partition(|(_, o)| o.inst == inst);
        self.live = rest;
        self.answer(at, Reply::Done { rpc });
        for (n, order) in covered {
            let event = self.end(n, order, SimState::Canceled(CancelReason::Requested));
            self.answer(at, event);
        }
    }

    fn query(&mut self, q: Query, at: At) {
        let n = self.lookup(&q.target);
        let live = n.and_then(|n| {
            let o = self.live.get(&n)?;
            Some(order_event(o, &vid(n), SimState::Open, Some(o.px)))
        });
        let ended = n.and_then(|n| {
            let e = self.ended.get(&n)?;
            Some(order_event(&e.order, &vid(n), e.state, e.px))
        });
        let reply = Reply::Query {
            rpc: q.head.rpc,
            vid: q.vid,
            cid: q.cid,
            found: live.or(ended),
        };
        self.answer(at, reply);
    }

    /// Takes resting order `n` out of its queue and records it as `state` ended it: its order
    /// event.
    fn end(&mut self, n: u64, order: Resting, state: SimState) -> Reply {
        if let Some(queue) = self.queues.get_mut(&order.inst) {
            queue.remove(OrderKey(n));
        }
        let px = Some(order.px);
        let event = Reply::Order(order_event(&order, &vid(n), state, px));
        self.ended.insert(n, Ended { order, px, state });
        event
    }

    fn level(
        &mut self,
        ev: &MdEvent,
        inst: InstrumentId,
        side: BookSide,
        px: Ticks,
        qty: Lots,
    ) -> Result<(), SimError> {
        let book = self.trading_book(inst);
        let in_snapshot = book.is_some_and(|b| b.snapshot_in_progress().is_some());
        let before = book
            .filter(|_| !in_snapshot)
            .and_then(|b| b.level(side, px).ok().flatten());
        self.books.apply(ev).map_err(SimError::Book)?;
        match before {
            Some(before) => {
                self.level_changed(inst, side, px, before, qty);
                Ok(())
            }
            // A snapshot's levels are compared at its end. Outside one, a level the book did
            // not know is a change too (Codex r4183669454): its size already reflects the
            // trades printed there, so they explain none of its next shrink.
            None if !in_snapshot => {
                self.traded.remove(&(inst, side == BookSide::Bid, px));
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// A replacement snapshot of `inst`'s trading book completed: each level a resting or
    /// injected order sits at, or trades printed at, changed from its size in the book replaced
    /// to its size in the snapshot, as a delta would have changed it (Codex r4182154723), and
    /// no trade before the snapshot explains a later change.
    fn snapshot_end(&mut self, ev: &MdEvent, inst: InstrumentId) -> Result<(), SimError> {
        // The levels a modelled order sits at, and those trades printed at since they changed.
        let orders = self.live.values().filter(|o| o.inst == inst);
        let mut held: BTreeSet<(bool, Ticks)> =
            orders.map(|o| (o.side == Side::Buy, o.px)).collect();
        let injected = self.injected.values().filter(|i| i.order.inst == inst);
        held.extend(injected.map(|i| (i.order.side == Side::Buy, i.order.px)));
        let traded = self.traded.keys().filter(|key| key.0 == inst);
        held.extend(traded.map(|&(_, bid, px)| (bid, px)));
        let side = |bid| if bid { BookSide::Bid } else { BookSide::Ask };
        let size = |engine: &SimEngine, bid, px| {
            let book = engine.trading_book(inst);
            book.and_then(|b| b.level(side(bid), px).ok().flatten())
        };
        let before: Vec<_> = held.iter().map(|&(bid, px)| size(self, bid, px)).collect();
        self.books.apply(ev).map_err(SimError::Book)?;
        for (&(bid, px), before) in held.iter().zip(before) {
            if let (Some(before), Some(after)) = (before, size(self, bid, px)) {
                self.level_changed(inst, side(bid), px, before, after);
            }
            // The snapshot restates every level, so it ends what the trades printed there
            // explain whatever it shows: a size equal to the one it replaced (Codex
            // r4184026666), or sizes that cannot be compared, at a level the old or the new
            // book does not reach (Codex r4182678488).
            self.traded.remove(&(inst, bid, px));
        }
        Ok(())
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
    ) {
        // A repeat of the level's size is no change (Codex r4182448165): the trades still
        // explain its next one.
        if before == after {
            return;
        }
        // A growth shows the injected orders there it has not shown yet, in injection order.
        let mut grown = after.checked_sub(before).unwrap_or(Lots::ZERO);
        for held in self.injected.values_mut().filter(|i| i.at(inst, side, px)) {
            let shown = held.unshown.min(grown);
            held.unshown = held.unshown.checked_sub(shown).unwrap_or(Lots::ZERO);
            grown = grown.checked_sub(shown).unwrap_or(Lots::ZERO);
        }
        let traded = self.traded.remove(&(inst, side == BookSide::Bid, px));
        let shrunk = before.checked_sub(after).unwrap_or(Lots::ZERO);
        let cancelled = shrunk
            .checked_sub(traded.unwrap_or(Lots::ZERO))
            .unwrap_or(Lots::ZERO);
        // The public size and the simulated orders there each fit in lots, but together may
        // not (Codex r4185186401): the level's size before is counted in an i128, so the
        // Middle bracket's share is never taken of a truncated level.
        let own = self.own(inst, side, px);
        let level_before = i128::from(before.get()) + i128::from(own.get());
        if let Some(queue) = self.queues.get_mut(&inst) {
            // `cancelled` is at most the public shrink, so at most `level_before`.
            queue.level_cancel_wide(side, px, cancelled, level_before);
        }
    }

    fn trade(&mut self, inst: InstrumentId, aggressor: Aggressor, px: Ticks, qty: Lots, at: At) {
        // A trade of no size says nothing of any level, the ones it printed through included
        // (Codex r4184778419), as the queue model holds for its own levels.
        if qty == Lots::ZERO {
            return;
        }
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
        // A print through a level emptied it first (Codex r4184546701): each known level the
        // taker's side shows at a better price than the print's is credited with its whole
        // size, so its removal is explained and an order arriving there queues behind none of
        // it.
        let top = book.top(usize::MAX).map(|top| match taker {
            Side::Buy => top.asks,
            Side::Sell => top.bids,
        });
        let through = top
            .unwrap_or_default()
            .into_iter()
            .filter(|lvl| match taker {
                Side::Buy => lvl.px < px,
                Side::Sell => lvl.px > px,
            });
        let bid = taker == Side::Sell;
        let emptied: Vec<_> = through.map(|lvl| ((inst, bid, lvl.px), lvl.qty)).collect();
        for (key, size) in emptied {
            let traded = self.traded.entry(key).or_insert(Lots::ZERO);
            *traded = (*traded).max(size);
        }
        let key = (inst, bid, px);
        let traded = self.traded.get(&key).copied().unwrap_or(Lots::ZERO);
        // Saturated, never dropped (Codex r4182448176): past an i64 of lots the trades
        // explain any shrink a level can show.
        self.traded
            .insert(key, traded.checked_add(qty).unwrap_or(MAX_LOTS));
        let view = TradeView {
            taker,
            px,
            qty,
            channel: Channel::Public,
        };
        // The trade is matched on a copy of the queues first (Codex r4183669469): an order it
        // would fill with no rate to charge is cancelled by the venue and leaves the queue, and
        // the trade is matched again, so the size it would have taken reaches the orders
        // behind it. Each round takes at least one order out of the queue, so this ends. An
        // injected order's fill needs no rate: it is never answered.
        loop {
            let Some(mut queue) = self.queues.get(&inst).cloned() else {
                return;
            };
            let fills = queue.trade(view);
            let priced: Vec<Option<Option<_>>> = fills
                .iter()
                .map(|f| {
                    let fee = || self.fee(inst, Liquidity::Maker, f.px, f.qty, at.wall);
                    let injected = self.injected.contains_key(&f.key.0);
                    if injected {
                        Some(None)
                    } else {
                        fee().map(Some)
                    }
                })
                .collect();
            if priced.iter().all(Option::is_some) {
                self.queues.insert(inst, queue);
                // The queue model holds only resting and injected orders, so each fill names
                // one.
                for (fill, fee) in fills.into_iter().zip(priced.into_iter().flatten()) {
                    match fee {
                        None => self.injected_fill(fill),
                        Some(fee) => {
                            if let Some(order) = self.live.remove(&fill.key.0) {
                                self.maker_fill(fill, order, fee, at);
                            }
                        }
                    }
                }
                return;
            }
            for (fill, _) in fills.iter().zip(&priced).filter(|(_, fee)| fee.is_none()) {
                let n = fill.key.0;
                // Out of the queue whatever the venue holds, so the next round cannot meet it.
                if let Some(queue) = self.queues.get_mut(&inst) {
                    queue.remove(fill.key);
                }
                if let Some(order) = self.live.remove(&n) {
                    // No rate to charge it at: the venue cancels the order rather than fill it.
                    let event = self.end(n, order, SimState::Canceled(CancelReason::Venue));
                    self.answer(at, event);
                }
            }
        }
    }

    fn injected_fill(&mut self, fill: SimFill) {
        if let Some(held) = self.injected.get_mut(&fill.key.0) {
            held.unshown = held.unshown.checked_sub(fill.qty).unwrap_or(Lots::ZERO);
        }
        if fill.remaining == Lots::ZERO {
            self.injected.remove(&fill.key.0);
        }
        self.injected_fills.push(fill);
    }

    fn maker_fill(&mut self, fill: SimFill, mut order: Resting, fee: (i128, AssetSym), at: At) {
        let n = fill.key.0;
        let vid = vid(n);
        order.cum = order.cum.checked_add(fill.qty).unwrap_or(order.cum);
        let record = self.fill_record(&order, &vid, fill.px, fill.qty, Liquidity::Maker, fee);
        self.answer(at, Reply::Fill(record));
        let px = Some(order.px);
        let state = if fill.remaining == Lots::ZERO {
            SimState::Filled
        } else {
            SimState::Open
        };
        self.answer(at, Reply::Order(order_event(&order, &vid, state, px)));
        if state == SimState::Open {
            self.live.insert(n, order);
        } else {
            self.ended.insert(n, Ended { order, px, state });
        }
    }
}

/// What an order takes before it changes anything: its fills, `(px, qty, (fee, asset))`, and
/// whether its rest was queued.
struct Taken {
    takes: Vec<(Ticks, Lots, (i128, AssetSym))>,
    rests: bool,
}

/// The most lots a `Lots` holds.
const MAX_LOTS: Lots = match Lots::new(i64::MAX) {
    Some(lots) => lots,
    None => Lots::ZERO,
};

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
