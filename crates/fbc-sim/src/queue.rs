//! The queue-position fill model (design §10.2, decision 0038).

use core::fmt;
use std::collections::BTreeMap;

use fbc_book::{BookError, L2Book};
use fbc_core::{BookSide, Channel, Lots, Side, Ticks};

/// How much of a level's cancelled size was ahead of a simulated order. The venue's true
/// queue is not observed, so a study runs a model per bracket and reads the band they span.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Bracket {
    /// None of it: the order advances only when trades consume the size ahead.
    Pessimistic,
    /// Its proportional share: `floor(cancelled × ahead / the level's size before)`.
    Middle,
    /// All of it, up to the size ahead.
    Optimistic,
}

/// A queue model's configuration. It has no `Default`: the bracket is the caller's choice
/// (0009), and a compile-fail case in `tests/ui/` keeps it so.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct QueueConfig {
    pub bracket: Bracket,
}

/// The caller's name for one modelled order, the simulation's own or an injected one.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct OrderKey(pub u64);

/// An order the model is asked to queue.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct NewOrder {
    pub side: Side,
    pub px: Ticks,
    pub channel: Channel,
    pub qty: Lots,
}

/// Where a modelled order sits: its side, price and channel, the size it has left, and the
/// size queued ahead of it at its price that is not a modelled order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct QueuePos {
    pub side: Side,
    pub px: Ticks,
    pub channel: Channel,
    pub remaining: Lots,
    pub ahead: Lots,
}

/// A public trade as the model reads it. The caller classifies a trade whose aggressor the
/// venue does not give, and says which flow it was (0038): `Channel::Public` flow meets the
/// public book only, so it never fills an RPI order; `Channel::Rpi` is retail flow, which meets
/// every public order at a price before any RPI order there.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TradeView {
    /// The side that took liquidity; the trade reaches resting orders on the other side.
    pub taker: Side,
    pub px: Ticks,
    pub qty: Lots,
    pub channel: Channel,
}

/// One fill of a modelled order, at the order's price, reported once.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SimFill {
    pub key: OrderKey,
    pub side: Side,
    pub px: Ticks,
    pub qty: Lots,
    /// What the order has left; at zero the model no longer holds it.
    pub remaining: Lots,
}

/// A queue-model call that was refused; the model is unchanged.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum QueueError {
    /// The model already holds an order under this key.
    DuplicateOrder(OrderKey),
    /// An order of no size.
    ZeroQuantity(OrderKey),
    /// The book cannot be read.
    Book(BookError),
    /// The book does not know the level's size at the order's price: outside its window, or
    /// past the deepest level a capped book shows ([`L2Book::level`]).
    UnknownLevel { side: BookSide, px: Ticks },
    /// Lots at a level (the modelled orders there, or the size ahead of one) past an `i64`.
    Overflow { side: BookSide, px: Ticks },
    /// A level said to shrink by more than its size before.
    CancelExceedsLevel { cancelled: Lots, level_before: Lots },
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::DuplicateOrder(key) => write!(f, "order {} is already queued", key.0),
            QueueError::ZeroQuantity(key) => write!(f, "order {} has no size", key.0),
            QueueError::Book(e) => write!(f, "queue model cannot read the book: {e}"),
            QueueError::UnknownLevel { side, px } => {
                write!(
                    f,
                    "the book does not know the size at {side:?} level {}",
                    px.0
                )
            }
            QueueError::Overflow { side, px } => {
                write!(f, "lots at {side:?} level {} overflow", px.0)
            }
            QueueError::CancelExceedsLevel {
                cancelled,
                level_before,
            } => write!(
                f,
                "a level of {} lots cannot shrink by {}",
                level_before.get(),
                cancelled.get()
            ),
        }
    }
}

impl std::error::Error for QueueError {}

/// A held order and its arrival number, which orders it behind earlier ones at its price.
#[derive(Copy, Clone, Debug)]
struct Held {
    pos: QueuePos,
    seq: u64,
}

/// The queue positions of every modelled order (the simulation's own and the injected ones)
/// under one [`Bracket`].
///
/// The caller drives it: it queues each order as the order arrives ([`accept`]), reports each
/// level that shrank with no trade at its price ([`level_cancel`]), the public size that joins
/// a level ([`public_join`]) and each public trade ([`trade`]), and removes an order it
/// cancels ([`remove`]). An order the model fills completely leaves it.
///
/// The book it reads is the public book: the public channel's levels, every modelled public
/// order included once it shows. A modelled RPI order is not in it, and its size ahead is the
/// public size at its price only (0038).
///
/// [`accept`]: QueueModel::accept
/// [`level_cancel`]: QueueModel::level_cancel
/// [`public_join`]: QueueModel::public_join
/// [`trade`]: QueueModel::trade
/// [`remove`]: QueueModel::remove
#[derive(Clone, Debug)]
pub struct QueueModel {
    bracket: Bracket,
    orders: BTreeMap<OrderKey, Held>,
    next_seq: u64,
}

impl QueueModel {
    /// An empty model under `config`'s bracket.
    pub fn new(config: QueueConfig) -> QueueModel {
        QueueModel {
            bracket: config.bracket,
            orders: BTreeMap::new(),
            next_seq: 0,
        }
    }

    /// Its bracket.
    pub fn bracket(&self) -> Bracket {
        self.bracket
    }

    /// The order under `key`, while the model holds it.
    pub fn position(&self, key: OrderKey) -> Option<QueuePos> {
        self.orders.get(&key).map(|held| held.pos)
    }

    /// Stops modelling the order under `key` (a cancel), giving its last position.
    pub fn remove(&mut self, key: OrderKey) -> Option<QueuePos> {
        self.orders.remove(&key).map(|held| held.pos)
    }

    /// Queues a new order behind its level's size on the public `book`, less every modelled
    /// public order the model already holds at its side and price (they are queued themselves,
    /// so they never count as size ahead). `book` is the level as the simulated venue shows it
    /// when the order arrives: with the modelled public orders it holds, and before any update
    /// that shows the new order itself. A level smaller than the modelled orders at it (they
    /// show late) leaves nothing ahead. Refused where the book does not know the level's size.
    ///
    /// The rule is the same for an RPI order, so it queues behind every public order at its
    /// price: the public size, which grows as public orders join
    /// ([`public_join`](QueueModel::public_join)), and the modelled public orders, which go
    /// before it when a trade fills them ([`trade`](QueueModel::trade)). Other participants'
    /// RPI orders are not in the public book and are not counted ahead of it (0038).
    pub fn accept(
        &mut self,
        key: OrderKey,
        order: NewOrder,
        book: &L2Book,
    ) -> Result<QueuePos, QueueError> {
        let side = order.side.book_side();
        let shown = book
            .level(side, order.px)
            .map_err(QueueError::Book)?
            .ok_or(QueueError::UnknownLevel { side, px: order.px })?;
        self.accept_shown(key, order, shown)
    }

    /// [`accept`](QueueModel::accept) with the level's size given rather than read from a
    /// book: `shown` is the size at the order's side and price as the simulated venue shows it,
    /// with the modelled public orders it holds there. SimVenue gives it when its book, the
    /// real venue's, does not show its own simulated orders (decision 0044).
    pub fn accept_shown(
        &mut self,
        key: OrderKey,
        order: NewOrder,
        shown: Lots,
    ) -> Result<QueuePos, QueueError> {
        if self.orders.contains_key(&key) {
            return Err(QueueError::DuplicateOrder(key));
        }
        if order.qty == Lots::ZERO {
            return Err(QueueError::ZeroQuantity(key));
        }
        let side = order.side.book_side();
        let own = self
            .orders
            .values()
            .map(|held| held.pos)
            .filter(|pos| {
                pos.side == order.side && pos.px == order.px && pos.channel == Channel::Public
            })
            .try_fold(Lots::ZERO, |sum, pos| sum.checked_add(pos.remaining))
            .ok_or(QueueError::Overflow { side, px: order.px })?;
        let pos = QueuePos {
            side: order.side,
            px: order.px,
            channel: order.channel,
            remaining: order.qty,
            ahead: shown.checked_sub(own).unwrap_or(Lots::ZERO),
        };
        self.orders.insert(
            key,
            Held {
                pos,
                seq: self.next_seq,
            },
        );
        self.next_seq += 1;
        Ok(pos)
    }

    /// The level at `side` and `px` shrank by `cancelled` lots from `level_before` with no
    /// trade at that price: each modelled order there advances by none of it (Pessimistic),
    /// `floor(cancelled × ahead / level_before)` (Middle) or `min(cancelled, ahead)`
    /// (Optimistic). Refused when `cancelled` exceeds `level_before`.
    pub fn level_cancel(
        &mut self,
        side: BookSide,
        px: Ticks,
        cancelled: Lots,
        level_before: Lots,
    ) -> Result<(), QueueError> {
        if cancelled > level_before {
            return Err(QueueError::CancelExceedsLevel {
                cancelled,
                level_before,
            });
        }
        self.level_cancel_wide(side, px, cancelled, i128::from(level_before.get()));
        Ok(())
    }

    /// [`QueueModel::level_cancel`] for a level whose size before, its public size and its
    /// modelled orders together, may pass an `i64` of lots (Codex r4185186401). The caller
    /// keeps `cancelled` at most `level_before`.
    pub(crate) fn level_cancel_wide(
        &mut self,
        side: BookSide,
        px: Ticks,
        cancelled: Lots,
        level_before: i128,
    ) {
        if cancelled == Lots::ZERO {
            return;
        }
        let (d, before) = (i128::from(cancelled.get()), level_before);
        for held in self.orders.values_mut() {
            let pos = &mut held.pos;
            if pos.side.book_side() != side || pos.px != px {
                continue;
            }
            let ahead = pos.ahead.get();
            let advance = match self.bracket {
                Bracket::Pessimistic => 0,
                // At most `ahead`, since `d <= before`, so it fits an i64.
                Bracket::Middle => (d * i128::from(ahead) / before) as i64,
                Bracket::Optimistic => cancelled.get().min(ahead),
            };
            pos.ahead = lots(ahead - advance);
        }
    }

    /// `added` lots of public size, not a modelled order, joined the level at `side` and
    /// `px`. It queues behind every public order there and before every RPI order, so each
    /// modelled RPI order there has that much more ahead of it; a public order is unmoved.
    /// Refused, changing nothing, when an RPI order's size ahead would pass an `i64`.
    pub fn public_join(
        &mut self,
        side: BookSide,
        px: Ticks,
        added: Lots,
    ) -> Result<(), QueueError> {
        let behind = |pos: &QueuePos| {
            pos.side.book_side() == side && pos.px == px && pos.channel == Channel::Rpi
        };
        let fits = self
            .orders
            .values()
            .filter(|held| behind(&held.pos))
            .all(|held| held.pos.ahead.checked_add(added).is_some());
        if !fits {
            return Err(QueueError::Overflow { side, px });
        }
        for held in self.orders.values_mut().filter(|held| behind(&held.pos)) {
            held.pos.ahead = lots(held.pos.ahead.get() + added.get());
        }
        Ok(())
    }

    /// A public trade (0038). It reaches the modelled orders on the side its taker hit, at its
    /// price or through it (a better price for the taker's counterparty), in the order the
    /// venue matches them: the best price first, and at a price every public order before any
    /// RPI order, each channel in arrival order.
    ///
    /// - **Through an order's price**, the venue's level there emptied before the trade
    ///   printed beyond it, so nothing is ahead of the order any more, and the trade's size,
    ///   which would have met the order first, fills it.
    /// - **At an order's price**, the trade's size consumes the size ahead of the order first
    ///   (less what it already consumed ahead of an earlier modelled order there) and then
    ///   fills it.
    ///
    /// A trade's size is spent once across the orders it reaches; an order is never filled
    /// past what remains, and an order of a channel the trade's flow cannot fill (an RPI order
    /// under public flow) only loses the size ahead of it. Gives one fill per order filled. A
    /// trade of no size changes nothing: it says nothing of the levels it printed through.
    pub fn trade(&mut self, t: TradeView) -> Vec<SimFill> {
        if t.qty == Lots::ZERO {
            return Vec::new();
        }
        let maker = t.taker.opposite();
        let reached = |pos: &QueuePos| {
            pos.side == maker
                && match maker {
                    Side::Buy => pos.px >= t.px,
                    Side::Sell => pos.px <= t.px,
                }
        };
        let mut queue: Vec<(&OrderKey, &mut Held)> = self
            .orders
            .iter_mut()
            .filter(|(_, held)| reached(&held.pos))
            .collect();
        queue.sort_by(|(_, a), (_, b)| {
            let price = match maker {
                Side::Buy => b.pos.px.cmp(&a.pos.px),
                Side::Sell => a.pos.px.cmp(&b.pos.px),
            };
            price
                .then(a.pos.channel.cmp(&b.pos.channel))
                .then(a.seq.cmp(&b.seq))
        });

        let mut left = t.qty.get();
        // The public size this trade consumed at its own price so far.
        let mut consumed = 0i64;
        let mut fills = Vec::new();
        for (key, held) in queue {
            let pos = &mut held.pos;
            let ahead = pos.ahead.get();
            if pos.px == t.px {
                let eat = (ahead - consumed).max(0).min(left);
                left -= eat;
                consumed += eat;
                pos.ahead = lots(ahead - ahead.min(consumed));
            } else {
                pos.ahead = Lots::ZERO;
            }
            let fillable = t.channel == Channel::Rpi || pos.channel == Channel::Public;
            let qty = if fillable && pos.ahead == Lots::ZERO {
                pos.remaining.get().min(left)
            } else {
                0
            };
            if qty > 0 {
                left -= qty;
                pos.remaining = lots(pos.remaining.get() - qty);
                fills.push(SimFill {
                    key: *key,
                    side: pos.side,
                    px: pos.px,
                    qty: lots(qty),
                    remaining: pos.remaining,
                });
            }
        }
        self.orders
            .retain(|_, held| held.pos.remaining != Lots::ZERO);
        fills
    }
}

/// `n` lots, where the arithmetic above keeps `n` non-negative.
fn lots(n: i64) -> Lots {
    Lots::new(n).expect("queue arithmetic stays non-negative")
}
