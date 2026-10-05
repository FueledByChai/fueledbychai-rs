//! The pre-trade caps (0013 rule 2), as the consumer configures them (0009), and why one
//! refused an order command.
//!
//! The inventory cap is decision 0005's I6, which 0005 calls the pre-trade resting cap: the
//! owner's decision A (2026-10-04) names it the inventory cap, the worst-case position if every
//! resting order on the command's side filled, `|pos + Σ resting same side + new| ≤ cap`, and
//! keeps the name "resting cap" for a gross bound per side (FBC-zf7). The registry checks it
//! in the one path every place, amend, replace and batch item is built through
//! ([`Registry::place`](crate::Registry::place), [`Registry::place_batch`](crate::Registry::place_batch)
//! and [`Live::amend`](crate::Live::amend)), reducing and reduce-only ones included: the formula
//! admits an order that genuinely reduces the position by itself. Cancels are never capped.
//!
//! Quantities are in the market's lots: the consumer converts a notional cap at the price it
//! chooses. A market the configuration does not name admits no order: there is no default.
//! Nor does a market whose position was not seeded from the venue: the worst case starts from
//! the position, which is unknown until then.

use std::collections::BTreeMap;
use std::fmt;

use fbc_core::{InstrumentId, Lots, Side, SignedLots};

/// One market's caps. Every field is required: the configuration has no default value.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct MarketCaps {
    /// The inventory cap (0005's I6): the largest worst-case position, in lots, either way.
    pub inventory: Lots,
}

/// The consumer's pre-trade caps, per market. A market it does not name admits no place or
/// amend ([`CapRefusal::NoCap`]).
#[derive(Clone, Default, Eq, PartialEq, Debug)]
pub struct PreTradeCaps {
    markets: BTreeMap<InstrumentId, MarketCaps>,
}

impl PreTradeCaps {
    /// Caps for no market: every place and amend is refused until a market is configured.
    pub fn new() -> PreTradeCaps {
        PreTradeCaps::default()
    }

    /// These caps with `caps` for `market`, replacing any it had.
    pub fn with_market(mut self, market: InstrumentId, caps: MarketCaps) -> PreTradeCaps {
        self.markets.insert(market, caps);
        self
    }

    /// The caps configured for `market`, if any.
    pub fn market(&self, market: InstrumentId) -> Option<MarketCaps> {
        self.markets.get(&market).copied()
    }
}

/// Why a pre-trade cap refused a place, an amend or a batch item: it was never built.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CapRefusal {
    /// The consumer configured no cap for the market: it admits nothing.
    NoCap(InstrumentId),
    /// The market's position was not seeded from the venue
    /// ([`Registry::seed_position`](crate::Registry::seed_position)): the worst case is not
    /// known, so it admits nothing.
    PositionUnknown(InstrumentId),
    /// The worst-case position on `side` with the order admitted, `worst`, would exceed the
    /// inventory cap `cap` (0005's I6); `worst` is `None` when it does not fit a lot count.
    InventoryCap {
        inst: InstrumentId,
        side: Side,
        worst: Option<Lots>,
        cap: Lots,
    },
}

impl fmt::Display for CapRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapRefusal::NoCap(inst) => {
                write!(f, "no inventory cap is configured for {inst:?}")
            }
            CapRefusal::PositionUnknown(inst) => {
                write!(f, "the position on {inst:?} was not seeded from the venue")
            }
            CapRefusal::InventoryCap {
                inst,
                side,
                worst: Some(worst),
                cap,
            } => write!(
                f,
                "the worst-case position on {inst:?} with the {side:?} order would be {} lots, \
                 over the inventory cap of {} lots",
                worst.get(),
                cap.get()
            ),
            CapRefusal::InventoryCap {
                inst,
                side,
                worst: None,
                cap,
            } => write!(
                f,
                "the worst-case position on {inst:?} with the {side:?} order does not fit a lot \
                 count, so it cannot be held under the inventory cap of {} lots",
                cap.get()
            ),
        }
    }
}

impl std::error::Error for CapRefusal {}

/// What one side of a market holds before an order is judged: its cap, the position (`None`
/// until it is seeded from the venue) and the quantity our other orders on that side may have
/// resting (`None` when the sum does not fit).
#[derive(Copy, Clone, Debug)]
pub(crate) struct Exposure {
    pub(crate) inst: InstrumentId,
    pub(crate) side: Side,
    pub(crate) cap: Option<MarketCaps>,
    pub(crate) pos: Option<SignedLots>,
    pub(crate) others: Option<Lots>,
}

impl Exposure {
    /// Whether `new` lots more resting on the side keep the worst case within the market's
    /// inventory cap (0005's I6): `|pos + Σ resting same side + new| ≤ cap`. Nothing is
    /// exempt: a reducing order passes only by what the formula gives it.
    pub(crate) fn admit(&self, new: Lots) -> Result<(), CapRefusal> {
        let (inst, side) = (self.inst, self.side);
        let cap = self.cap.ok_or(CapRefusal::NoCap(inst))?.inventory;
        let pos = self.pos.ok_or(CapRefusal::PositionUnknown(inst))?;
        let worst = self
            .others
            .and_then(|resting| resting.checked_add(new))
            .and_then(|total| pos.checked_add(SignedLots::of(side, total)))
            .and_then(SignedLots::abs_lots);
        match worst {
            Some(worst) if worst <= cap => Ok(()),
            worst => Err(CapRefusal::InventoryCap {
                inst,
                side,
                worst,
                cap,
            }),
        }
    }
}
