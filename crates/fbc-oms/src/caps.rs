//! The pre-trade caps (0013 rule 2), as the consumer configures them (0009), and why one
//! refused an order command.
//!
//! Two caps per market, both enforced (decision 0052, the owner's decision A of 2026-10-04):
//!
//! - the **inventory cap** is decision 0005's I6, which 0005 calls the pre-trade resting cap:
//!   the worst-case position if every resting order on the command's side filled,
//!   `|pos + Σ resting same side + new| ≤ inventory cap`;
//! - the **resting cap** is a gross bound per side on the order stack, whatever the position:
//!   `Σ resting same side + new ≤ resting cap`. I6 lets the side that reduces the position rest
//!   up to twice the inventory cap; the resting cap bounds it too.
//!
//! Resting counts the same for both ([`OrderRecord::resting`](crate::OrderRecord::resting)):
//! PendingNew and Unknown orders in full, a partly filled order's remainder until it is
//! terminal, an amend at the larger of its old and new quantity from when it is built until it
//! is acknowledged, and the earlier items of the same batch as PendingNew. The inventory cap
//! also counts the fills the venue reported that the inventory does not hold yet
//! ([`OrderRecord::exposure`](crate::OrderRecord::exposure)); those no longer rest, so the
//! resting cap does not. The registry checks both in the one path every place, amend, replace
//! and batch item is built through ([`Registry::place`](crate::Registry::place),
//! [`Registry::place_batch`](crate::Registry::place_batch) and
//! [`Live::amend`](crate::Live::amend)), reducing and reduce-only ones included. Cancels are
//! never capped.
//!
//! One case passes the inventory cap past it (decision 0112, FBC-ugto, the owner's answer of
//! 2026-10-19): an order carrying the venue's reduce-only flag
//! ([`NewOrder::reduce_only`](fbc_core::NewOrder), which a codec sends only where the venue's
//! `OrderCaps` declare one and refuses otherwise) whose admission leaves the side's worst case
//! strictly smaller than it is without the order. The venue keeps such an order from taking the
//! position past zero, so a position already past the cap (orphan or race fills) comes down in
//! steps the resting cap bounds. An order the OMS only classifies reducing (an emulated
//! reduce-only order, FBC-01w) is not exempt, nor is a flagged order that does not shrink the
//! worst case; the resting cap, the market's state and every other check still apply.
//!
//! Quantities are in the market's lots: the consumer converts a notional cap at the price it
//! chooses. Both caps are required configuration with no default in code: a market's
//! configuration missing either is refused ([`CapsConfigError`]), and a market the
//! configuration does not name admits no order. Nor does a market whose position was not
//! seeded from the venue: the worst case starts from the position, which is unknown until
//! then.

use std::collections::BTreeMap;
use std::fmt;

use fbc_core::{InstrumentId, Lots, Side, SignedLots};

use crate::entry::ExitRefusal;

/// One market's caps as the consumer's configuration states them (0009), each one possibly
/// absent. Both are required: [`PreTradeCaps::with_market`] refuses a market missing either,
/// and nothing in code supplies a default.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct MarketCapsConfig {
    /// The inventory cap (0005's I6): the largest worst-case position, in lots, either way.
    pub inventory: Option<Lots>,
    /// The resting cap: the most our orders on one side may have resting, in lots.
    pub resting: Option<Lots>,
}

/// One market's caps, both present: built only from a [`MarketCapsConfig`] that states both
/// ([`PreTradeCaps::with_market`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct MarketCaps {
    inventory: Lots,
    resting: Lots,
}

impl MarketCaps {
    /// The inventory cap (0005's I6): the largest worst-case position, in lots, either way.
    pub fn inventory(&self) -> Lots {
        self.inventory
    }

    /// The resting cap: the most our orders on one side may have resting, in lots.
    pub fn resting(&self) -> Lots {
        self.resting
    }
}

/// Why a configuration of the pre-trade caps was refused: a cap it must state is missing, and
/// no default stands in for it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CapsConfigError {
    /// The market's configuration states no inventory cap.
    MissingInventoryCap(InstrumentId),
    /// The market's configuration states no resting cap.
    MissingRestingCap(InstrumentId),
}

impl fmt::Display for CapsConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapsConfigError::MissingInventoryCap(inst) => write!(
                f,
                "the configuration states no inventory cap for {inst:?}, and there is no default"
            ),
            CapsConfigError::MissingRestingCap(inst) => write!(
                f,
                "the configuration states no resting cap for {inst:?}, and there is no default"
            ),
        }
    }
}

impl std::error::Error for CapsConfigError {}

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

    /// These caps with `config`'s for `market`, replacing any it had. Refused, the whole
    /// configuration with it, when `config` is missing the inventory cap or the resting cap:
    /// neither has a default.
    pub fn with_market(
        mut self,
        market: InstrumentId,
        config: MarketCapsConfig,
    ) -> Result<PreTradeCaps, CapsConfigError> {
        let caps = MarketCaps {
            inventory: config
                .inventory
                .ok_or(CapsConfigError::MissingInventoryCap(market))?,
            resting: config
                .resting
                .ok_or(CapsConfigError::MissingRestingCap(market))?,
        };
        self.markets.insert(market, caps);
        Ok(self)
    }

    /// The markets these caps configure.
    pub(crate) fn markets(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.markets.keys().copied()
    }

    /// The caps configured for `market`, if any.
    pub fn market(&self, market: InstrumentId) -> Option<MarketCaps> {
        self.markets.get(&market).copied()
    }
}

/// Why a pre-trade cap refused a place, an amend or a batch item: it was never built.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CapRefusal {
    /// The consumer configured no caps for the market: it admits nothing.
    NoCap(InstrumentId),
    /// The market's position is not known: no resync seeded it from the venue
    /// ([`Registry::resync`](crate::Registry::resync)), or a fill since could not be placed
    /// against the seed ([`Registry::position`](crate::Registry::position)): the worst case is
    /// not known, so it admits nothing.
    PositionUnknown(InstrumentId),
    /// The worst-case position on `side` with the order admitted, `worst`, would exceed the
    /// inventory cap `cap` (0005's I6); `worst` is `None` when it does not fit a lot count.
    InventoryCap {
        inst: InstrumentId,
        side: Side,
        worst: Option<Lots>,
        cap: Lots,
    },
    /// What our orders on `side` would have resting with the order admitted, `resting`, would
    /// exceed the resting cap `cap`; `resting` is `None` when it does not fit a lot count.
    RestingCap {
        inst: InstrumentId,
        side: Side,
        resting: Option<Lots>,
        cap: Lots,
    },
}

impl fmt::Display for CapRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapRefusal::NoCap(inst) => {
                write!(f, "no pre-trade caps are configured for {inst:?}")
            }
            CapRefusal::PositionUnknown(inst) => {
                write!(
                    f,
                    "the position on {inst:?} was not seeded from the venue, or is no longer known"
                )
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
            CapRefusal::RestingCap {
                inst,
                side,
                resting: Some(resting),
                cap,
            } => write!(
                f,
                "our {side:?} orders on {inst:?} would have {} lots resting with the order, \
                 over the resting cap of {} lots",
                resting.get(),
                cap.get()
            ),
            CapRefusal::RestingCap {
                inst,
                side,
                resting: None,
                cap,
            } => write!(
                f,
                "what our {side:?} orders on {inst:?} would have resting with the order does \
                 not fit a lot count, so it cannot be held under the resting cap of {} lots",
                cap.get()
            ),
        }
    }
}

impl std::error::Error for CapRefusal {}

/// What one side of a market holds before an order is judged: its caps, the position (`None`
/// until it is seeded from the venue), what our other orders on that side may still add to
/// the position ([`OrderRecord::exposure`](crate::OrderRecord::exposure)) and what they have
/// resting ([`OrderRecord::resting`](crate::OrderRecord::resting)), each `None` when the sum
/// does not fit.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Exposure {
    pub(crate) inst: InstrumentId,
    pub(crate) side: Side,
    pub(crate) cap: Option<MarketCaps>,
    pub(crate) pos: Option<SignedLots>,
    pub(crate) others: Option<Lots>,
    pub(crate) others_resting: Option<Lots>,
}

/// What an order brings to its side once admitted: what it may add to the position, for the
/// inventory cap, and what it has resting, for the resting cap.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Adds {
    pub(crate) exposure: Lots,
    pub(crate) resting: Lots,
}

impl Adds {
    /// A new order: all of it may rest and fill.
    pub(crate) fn placing(qty: Lots) -> Adds {
        Adds {
            exposure: qty,
            resting: qty,
        }
    }
}

impl Exposure {
    /// Exit's admission (decision 0012): the order is reduce-only or classified reducing
    /// (`marked`), on the side that reduces the known, non-zero position, and what our orders
    /// on that side may still move the position by ([`OrderRecord::exposure`](crate::OrderRecord::exposure):
    /// PendingNew and Unknown orders in full, remainders, and fills the venue reported that
    /// the inventory does not hold yet), with the `adds` the order brings, stays within the
    /// position's size, so the position never crosses zero if they all fill. The caps are
    /// judged after it, as for any order.
    pub(crate) fn admit_exit(&self, marked: bool, adds: Lots) -> Result<(), ExitRefusal> {
        let (inst, side) = (self.inst, self.side);
        let pos = self.pos.ok_or(ExitRefusal::PositionUnknown(inst))?;
        let reduces = match pos.0.signum() {
            0 => return Err(ExitRefusal::Flat(inst)),
            1 => Side::Sell,
            _ => Side::Buy,
        };
        if side != reduces {
            return Err(ExitRefusal::Increasing { inst, side });
        }
        if !marked {
            return Err(ExitRefusal::Ordinary { inst, side });
        }
        // A position of i64::MIN lots has no size as a lot count; every order on it is
        // judged against the largest one, which is still short of crossing zero.
        let position = pos
            .abs_lots()
            .unwrap_or(Lots::new(i64::MAX).expect("non-negative"));
        let total = self.others.and_then(|others| others.checked_add(adds));
        match total {
            Some(total) if total <= position => Ok(()),
            total => Err(ExitRefusal::CrossesZero {
                inst,
                side,
                total,
                position,
            }),
        }
    }

    /// Whether the order `new` brings keeps the side within both of the market's caps: the
    /// inventory cap (0005's I6), `|pos + Σ resting same side + new| ≤ cap`, then the resting
    /// cap, `Σ resting same side + new ≤ cap`. A reducing order passes the inventory cap by what
    /// the formula gives it, with one exemption (decision 0112): an order carrying the venue's
    /// reduce-only flag (`venue_reduce_only`; for a batch judged again at authorization, every
    /// one of its orders on the side) passes it whenever it leaves the worst case strictly
    /// smaller than the side's worst case without it, even past the cap. The resting cap has
    /// no exemption.
    pub(crate) fn admit(&self, new: Adds, venue_reduce_only: bool) -> Result<(), CapRefusal> {
        let (inst, side) = (self.inst, self.side);
        let caps = self.cap.ok_or(CapRefusal::NoCap(inst))?;
        let pos = self.pos.ok_or(CapRefusal::PositionUnknown(inst))?;
        let worst_with = |adds: Lots| {
            self.others
                .and_then(|others| others.checked_add(adds))
                .and_then(|total| pos.checked_add(SignedLots::of(side, total)))
                .and_then(SignedLots::abs_lots)
        };
        let worst = worst_with(new.exposure);
        // A venue-flagged reduce-only order that strictly shrinks the worst case (FBC-ugto).
        // The worst case without it is compared in i128, so a position of i64::MIN lots, whose
        // size no lot count holds, is compared without wrapping (Codex on PR #183).
        let shrinks = |worst: Lots| {
            venue_reduce_only
                && self.others.is_some_and(|others| {
                    let before =
                        i128::from(pos.0) + i128::from(side.sign()) * i128::from(others.get());
                    i128::from(worst.get()) < before.abs()
                })
        };
        match worst {
            Some(worst) if worst <= caps.inventory || shrinks(worst) => {}
            worst => {
                return Err(CapRefusal::InventoryCap {
                    inst,
                    side,
                    worst,
                    cap: caps.inventory,
                });
            }
        }
        let resting = self
            .others_resting
            .and_then(|others| others.checked_add(new.resting));
        match resting {
            Some(resting) if resting <= caps.resting => Ok(()),
            resting => Err(CapRefusal::RestingCap {
                inst,
                side,
                resting,
                cap: caps.resting,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INST: InstrumentId = InstrumentId::new(1);

    fn lots(n: i64) -> Lots {
        Lots::new(n).unwrap()
    }

    /// The sell side of a long `pos` under an inventory cap of 50 and a resting cap of 11 (test
    /// inputs), our other sells counting `others` (`None`: past a lot count), none resting.
    fn sells(pos: i64, others: Option<i64>) -> Exposure {
        Exposure {
            inst: INST,
            side: Side::Sell,
            cap: Some(MarketCaps {
                inventory: lots(50),
                resting: lots(11),
            }),
            pos: Some(SignedLots(pos)),
            others: others.map(lots),
            others_resting: Some(Lots::ZERO),
        }
    }

    fn over(worst: Option<i64>) -> Result<(), CapRefusal> {
        Err(CapRefusal::InventoryCap {
            inst: INST,
            side: Side::Sell,
            worst: worst.map(lots),
            cap: lots(50),
        })
    }

    #[test]
    fn only_a_venue_reduce_only_order_that_strictly_shrinks_the_worst_case_passes_past_the_cap() {
        let e = sells(62, Some(0));
        assert_eq!(e.admit(Adds::placing(lots(11)), true), Ok(()));
        assert_eq!(e.admit(Adds::placing(lots(11)), false), over(Some(51)));
        // Nothing added leaves the worst case as it is: not strictly smaller.
        assert_eq!(e.admit(Adds::placing(Lots::ZERO), true), over(Some(62)));
        // Others already take the worst case past zero to 58: 11 more grows it to 69.
        let past_zero = sells(62, Some(120));
        assert_eq!(
            past_zero.admit(Adds::placing(lots(11)), true),
            over(Some(69))
        );
        // A worst case that does not fit a lot count shrinks nothing.
        let unknown = sells(62, None);
        assert_eq!(unknown.admit(Adds::placing(lots(11)), true), over(None));
        // A short of i64::MIN lots, whose size no lot count holds, still shrinks by a flagged
        // bid (Codex on PR #183): the worst case before is compared without wrapping.
        let deepest = Exposure {
            side: Side::Buy,
            pos: Some(SignedLots(i64::MIN)),
            ..e
        };
        assert_eq!(deepest.admit(Adds::placing(lots(11)), true), Ok(()));
        assert_eq!(
            deepest.admit(Adds::placing(lots(11)), false),
            Err(CapRefusal::InventoryCap {
                inst: INST,
                side: Side::Buy,
                worst: Some(lots(i64::MAX - 10)),
                cap: lots(50),
            })
        );
        // The resting cap still applies.
        assert_eq!(
            e.admit(Adds::placing(lots(12)), true),
            Err(CapRefusal::RestingCap {
                inst: INST,
                side: Side::Sell,
                resting: Some(lots(12)),
                cap: lots(11),
            })
        );
    }
}
