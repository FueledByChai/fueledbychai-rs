//! One `orderbook` subscription: the granularity and depth it asked for, the codec's own copy of
//! its levels, the bounds check every frame passes before any of its events is pushed, and where
//! it stands in recovering from a gap (decisions 0124 and 0129).
//!
//! A frame is read whole and applied to a copy of the book; only if the copy passes the check
//! does the book take it and are its events handed back, the windows and every removal of both
//! sides before any level the frame sets. Otherwise the frame is [`Broken`], and the codec pushes
//! a gap and nothing else of it.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{BookSide, InstrumentId, InstrumentSpec, Lots, MdEvent, PxExact, Ticks, TimerTag};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::BOOK_ORDERBOOK;
use crate::discover::plain_decimal;

/// What an orderbook subscription names besides its symbol: the granularity, as written on the
/// wire and as a value, and the depth.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) struct BookParams {
    /// The granularity as the subscription writes it: the quote grid as the contract lists it,
    /// or the tick.
    pub granularity_text: String,
    /// The same, as a value: every level's price is a whole multiple of it.
    pub granularity: Decimal,
    /// The levels per side asked for, which the bounds check reads.
    pub depth: u16,
}

/// Where a book stands, and the timer that acts on it while it waits.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(super) enum Phase {
    /// Subscribed, waiting for a Snapshot until `timer` fires: after the consumer's subscription
    /// (`resubscribed: false`), when the timer resubscribes, or after the codec's own resubscribe
    /// (`resubscribed: true`), when it asks for a reconnect.
    Awaiting { resubscribed: bool, timer: TimerTag },
    /// Built from a Snapshot that passed the check and every Update since.
    Valid,
    /// Found broken: `timer` resubscribes.
    Gapped { timer: TimerTag },
    /// A resubscribe brought no Snapshot in time, and a reconnect was asked for.
    Reconnecting,
}

impl Phase {
    /// The timer whose firing acts on a book in this phase; any other is stale (the book took a
    /// Snapshot, or was unsubscribed).
    pub fn timer(self) -> Option<TimerTag> {
        match self {
            Phase::Awaiting { timer, .. } | Phase::Gapped { timer } => Some(timer),
            Phase::Valid | Phase::Reconnecting => None,
        }
    }
}

/// Why a frame shows that the book may be wrong. Each is counted under its own name.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(super) enum Broken {
    /// Not as documented: an absent side, a side that is not null and lacks either bound, a
    /// level or bound that cannot be read, a price off the tick, a price listed twice on a side.
    Malformed,
    /// Its `granularity` or `depth` differs from the subscription's, or is missing.
    Params,
    /// A level's price is not a whole multiple of the subscription's granularity.
    OffGranularity,
    /// A Snapshot with no level on either side.
    Empty,
    /// The bounds check failed: a level the window removes that the frame does not set to
    /// zero, a best level not at `startPrice`, a deepest one not at `endPrice`, more levels than
    /// the depth, a level outside the bounds, or a crossed book.
    Bounds,
    /// A side holding fewer levels than the depth, its best at `startPrice` and its deepest at
    /// `endPrice`, or none at all within its bounds, in a frame failing the check by nothing
    /// else: an assumption no frame has shown, counted apart.
    Thin,
}

/// A side as a frame states it.
enum Stated {
    /// `null`: no level (Hibachi's WebSocket market docs).
    Null,
    /// Bounded by `startPrice` and `endPrice`, with the levels the frame lists, zeros included.
    Bounded {
        start: Ticks,
        end: Ticks,
        levels: Vec<(Ticks, Lots)>,
    },
}

/// What a frame did to the book, once it passed the check.
pub(super) struct Applied {
    /// The events to push, in order.
    pub events: Vec<MdEvent>,
    /// Its best bid equals its best ask: not crossed, counted apart.
    pub locked: bool,
    /// The sides it sent as null.
    pub nulls: u64,
}

/// One instrument's orderbook subscription.
pub(super) struct OrderBook {
    pub inst: InstrumentId,
    /// The venue symbol, as the subscription spells it.
    pub symbol: String,
    pub params: BookParams,
    /// How long it waits before resubscribing, and then for the Snapshot.
    pub interval: Duration,
    pub phase: Phase,
    /// The codec's copy of the levels.
    pub levels: Levels,
}

/// A book's levels, each side by price.
#[derive(Clone, Default)]
pub(super) struct Levels {
    bids: BTreeMap<Ticks, Lots>,
    asks: BTreeMap<Ticks, Lots>,
}

impl OrderBook {
    /// Forgets every level: the book is no longer trusted.
    pub fn forget(&mut self) {
        self.levels = Levels::default();
    }

    /// Applies one orderbook frame of `kind` to a copy of the book and checks it. `epoch` is the
    /// last snapshot epoch given this instrument, advanced when a Snapshot is taken. An Update is
    /// handed only to a valid book.
    pub fn apply(
        &mut self,
        kind: &str,
        frame: &Map<String, Value>,
        spec: &InstrumentSpec,
        epoch: &mut u32,
    ) -> Result<Applied, Broken> {
        let snapshot = match kind {
            "Snapshot" => true,
            "Update" => false,
            _ => return Err(Broken::Malformed),
        };
        self.check_params(frame)?;
        let data = frame.get("data").and_then(Value::as_object);
        let data = data.ok_or(Broken::Malformed)?;
        let bid = self.side(data, "bid", spec)?;
        let ask = self.side(data, "ask", spec)?;
        let nulls = [&bid, &ask]
            .into_iter()
            .filter(|s| matches!(s, Stated::Null))
            .count() as u64;
        let mut applied = if snapshot {
            self.snapshot(&bid, &ask, epoch)?
        } else {
            self.update(&bid, &ask)?
        };
        applied.nulls = nulls;
        Ok(applied)
    }

    /// The frame's `granularity` and `depth` must be the subscription's: the granularity a
    /// string of the same value, the depth the same whole number.
    fn check_params(&self, frame: &Map<String, Value>) -> Result<(), Broken> {
        let granularity = frame.get("granularity").and_then(Value::as_str);
        let granularity = granularity.and_then(plain_decimal);
        let depth = frame.get("depth").and_then(Value::as_u64);
        let same = granularity == Some(self.params.granularity)
            && depth == Some(u64::from(self.params.depth));
        same.then_some(()).ok_or(Broken::Params)
    }

    /// Side `key` of `data`: null, or bounded with its levels; absent, or not null and lacking
    /// either bound, it is malformed.
    fn side(
        &self,
        data: &Map<String, Value>,
        key: &str,
        spec: &InstrumentSpec,
    ) -> Result<Stated, Broken> {
        let side = match data.get(key) {
            Some(Value::Null) => return Ok(Stated::Null),
            Some(Value::Object(side)) => side,
            _ => return Err(Broken::Malformed),
        };
        let listed = side.get("levels").and_then(Value::as_array);
        let mut levels = Vec::new();
        let mut seen = BTreeSet::new();
        for level in listed.ok_or(Broken::Malformed)? {
            let level = level.as_object().ok_or(Broken::Malformed)?;
            let px = self.price(level.get("price"), spec)?;
            let qty = level.get("quantity").and_then(Value::as_str);
            let qty = qty.and_then(|q| lots(q, spec)).ok_or(Broken::Malformed)?;
            if !seen.insert(px) {
                return Err(Broken::Malformed);
            }
            levels.push((px, qty));
        }
        // Read after the levels, so a level off the granularity is counted as that even where
        // a bound names it.
        let start = bound(side.get("startPrice"), spec)?;
        let end = bound(side.get("endPrice"), spec)?;
        Ok(Stated::Bounded { start, end, levels })
    }

    /// A level's price: a whole multiple of the subscription's granularity, on the tick.
    fn price(&self, value: Option<&Value>, spec: &InstrumentSpec) -> Result<Ticks, Broken> {
        let px = value.and_then(Value::as_str).and_then(plain_decimal);
        let px = px.ok_or(Broken::Malformed)?;
        if px.checked_rem(self.params.granularity) != Some(Decimal::ZERO) {
            return Err(Broken::OffGranularity);
        }
        ticks(px, spec).ok_or(Broken::Malformed)
    }

    /// A Snapshot replaces the book: its levels with a quantity, a null side empty. It needs a
    /// level on some side, and every bounded side must pass the check.
    fn snapshot(&mut self, bid: &Stated, ask: &Stated, epoch: &mut u32) -> Result<Applied, Broken> {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        for (stated, levels) in [(bid, &mut bids), (ask, &mut asks)] {
            if let Stated::Bounded { levels: listed, .. } = stated {
                levels.extend(listed.iter().copied().filter(|(_, qty)| *qty != Lots::ZERO));
            }
        }
        if bids.is_empty() && asks.is_empty() {
            return Err(Broken::Empty);
        }
        let locked = self.check(&bids, &asks, bid, ask)?;
        self.levels = Levels { bids, asks };
        *epoch = epoch.wrapping_add(1);
        let (inst, book) = (self.inst, BOOK_ORDERBOOK);
        let mut events = vec![MdEvent::BookSnapshotBegin {
            inst,
            book,
            epoch: *epoch,
        }];
        let Levels { bids, asks } = &self.levels;
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            events.extend(levels.iter().map(|(&px, &qty)| MdEvent::Level {
                inst,
                book,
                side,
                px,
                qty,
            }));
        }
        events.push(MdEvent::BookSnapshotEnd { inst, book });
        Ok(Applied {
            events,
            locked,
            nulls: 0,
        })
    }

    /// An Update: a null side is cleared; a bounded side loses its levels outside the min..max
    /// of its bounds, each of which the frame must set to zero, then takes the levels it lists.
    /// The removals of both sides come before any level set (decision 0124 item 6).
    fn update(&mut self, bid: &Stated, ask: &Stated) -> Result<Applied, Broken> {
        let Levels { mut bids, mut asks } = self.levels.clone();
        let mut removals = Vec::new();
        let mut sets = Vec::new();
        let (inst, book) = (self.inst, BOOK_ORDERBOOK);
        for (side, stated, levels) in [
            (BookSide::Bid, bid, &mut bids),
            (BookSide::Ask, ask, &mut asks),
        ] {
            let Stated::Bounded {
                start,
                end,
                levels: listed,
            } = stated
            else {
                levels.clear();
                removals.push(MdEvent::SideClear { inst, book, side });
                continue;
            };
            let (lo, hi) = ((*start).min(*end), (*start).max(*end));
            let zeros: BTreeSet<Ticks> = listed
                .iter()
                .filter(|(_, qty)| *qty == Lots::ZERO)
                .map(|(px, _)| *px)
                .collect();
            // A zero inside the range, or better than its best end, empties a level the book
            // has: emitted before the prune. One past the deep end may only have scrolled out:
            // the prune removes it, and it is not emitted (0124 item 6).
            for &px in &zeros {
                let past_deep_end = match side {
                    BookSide::Bid => px < lo,
                    BookSide::Ask => px > hi,
                };
                if levels.contains_key(&px) && !past_deep_end {
                    removals.push(MdEvent::Level {
                        inst,
                        book,
                        side,
                        px,
                        qty: Lots::ZERO,
                    });
                }
            }
            let outside: Vec<Ticks> = levels
                .keys()
                .copied()
                .filter(|px| *px < lo || *px > hi)
                .collect();
            // A level the window removes that the frame does not set to zero is one a lost
            // frame left behind.
            if outside.iter().any(|px| !zeros.contains(px)) {
                return Err(Broken::Bounds);
            }
            levels.retain(|px, _| (lo..=hi).contains(px) && !zeros.contains(px));
            removals.push(MdEvent::SidePrune {
                inst,
                book,
                side,
                lo,
                hi,
            });
            for &(px, qty) in listed.iter().filter(|(_, qty)| *qty != Lots::ZERO) {
                levels.insert(px, qty);
                sets.push(MdEvent::Level {
                    inst,
                    book,
                    side,
                    px,
                    qty,
                });
            }
        }
        let locked = self.check(&bids, &asks, bid, ask)?;
        self.levels = Levels { bids, asks };
        removals.extend(sets);
        Ok(Applied {
            events: removals,
            locked,
            nulls: 0,
        })
    }

    /// The bounds check on the book a frame leaves: each bounded side holds its best level at
    /// `startPrice`, exactly the depth's levels within the min..max of its bounds, the last of
    /// them at `endPrice`, and none outside; and the book is not crossed. Whether it is locked.
    ///
    /// Both sides and the crossed test are checked before a thin side is reported, so a frame
    /// is counted thin only when it fails by nothing else.
    fn check(
        &self,
        bids: &BTreeMap<Ticks, Lots>,
        asks: &BTreeMap<Ticks, Lots>,
        bid: &Stated,
        ask: &Stated,
    ) -> Result<bool, Broken> {
        let depth = usize::from(self.params.depth);
        let thin_bid = check_side(bids.keys().rev().copied().collect(), bid, depth)?;
        let thin_ask = check_side(asks.keys().copied().collect(), ask, depth)?;
        let locked = match (bids.keys().next_back(), asks.keys().next()) {
            (Some(best_bid), Some(best_ask)) if best_bid > best_ask => {
                return Err(Broken::Bounds);
            }
            (Some(best_bid), Some(best_ask)) => best_bid == best_ask,
            _ => false,
        };
        if thin_bid || thin_ask {
            return Err(Broken::Thin);
        }
        Ok(locked)
    }
}

/// One side's check, its prices best first: whether the side is thin (fewer levels than the
/// depth, its best and deepest at its bounds, or none at all), any other failure an error. A
/// null side is not checked.
fn check_side(best_first: Vec<Ticks>, stated: &Stated, depth: usize) -> Result<bool, Broken> {
    let Stated::Bounded { start, end, .. } = stated else {
        return Ok(false);
    };
    let (lo, hi) = ((*start).min(*end), (*start).max(*end));
    if best_first.iter().any(|px| *px < lo || *px > hi) {
        return Err(Broken::Bounds);
    }
    let (Some(best), Some(deepest)) = (best_first.first(), best_first.last()) else {
        return Ok(true);
    };
    if best != start || deepest != end || best_first.len() > depth {
        return Err(Broken::Bounds);
    }
    Ok(best_first.len() < depth)
}

/// A side's bound: a price on the tick.
fn bound(value: Option<&Value>, spec: &InstrumentSpec) -> Result<Ticks, Broken> {
    let px = value.and_then(Value::as_str).and_then(plain_decimal);
    px.and_then(|px| ticks(px, spec)).ok_or(Broken::Malformed)
}

/// `px` on `spec`'s tick, exactly, or `None` off it.
pub(super) fn ticks(px: Decimal, spec: &InstrumentSpec) -> Option<Ticks> {
    let px = PxExact::from_decimal(px)?;
    spec.price_grid.ticks_exact(px)
}

/// A quantity: a plain decimal string that is a whole number of `spec`'s size steps.
pub(super) fn lots(text: &str, spec: &InstrumentSpec) -> Option<Lots> {
    let qty = plain_decimal(text)?;
    let step = spec.size_step.get();
    let lots = qty.checked_div(step)?;
    let whole = lots.fract().is_zero() && lots.checked_mul(step) == Some(qty);
    whole.then(|| lots.to_i64()).flatten().and_then(Lots::new)
}
