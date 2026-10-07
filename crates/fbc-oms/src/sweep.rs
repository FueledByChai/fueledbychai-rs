//! The kill switch's "cancel everything" for one market (decision 0012), within 0005's I4 and
//! I7, and the foreign orders in view that decide its shape.
//!
//! [`Registry::cancel_everything`] builds, in every market state, either
//!
//! - an instrument cancel-all ([`CancelEverything::CancelAll`]), only when the venue declares
//!   one (`OrderCaps::cancel_all_instrument`), the registry holds the market's exclusive
//!   account-and-instrument lease (its [`MarketLease`](fbc_core::MarketLease), held while the
//!   market is armed and still covered by the registry's current lease names), a trustworthy
//!   resync has shown the account's open orders, and no order that is not ours is in view on
//!   the market; with it, the explicit cancels of our orders the venue may not hold yet
//!   (PendingNew or Unknown), which the cancel-all can miss; or
//! - the cancels of our orders on the market that are not terminal, each by explicit reference
//!   in a cancel-many ([`Registry::cancel_many`]), saying why no cancel-all was built
//!   ([`CancelAllRefusal`]). The registry holds our orders alone, so no foreign-namespace or
//!   non-canonical order is ever cancelled (I4). An order whose cancel is in flight is
//!   cancelled again.
//!
//! An account cancel-all is never built (0005's I7 admits only the instrument one). Each resync
//! names the markets in Killed ([`ResyncReport::cancel_everything`](crate::ResyncReport)), whose
//! cancel everything the consumer builds again, so the explicit cancels repeat on each resync
//! as 0012 says. Our own-namespace orders the registry does not hold (0005's I7 orphans, on a
//! market a resync already seeded) are FBC-840's; an instrument cancel-all reaches them.
//!
//! **In view.** An order not ours (another namespace's, a non-canonical client id, or one
//! nothing attributes: no client id and a venue id no order of ours had) is in view on its
//! market from the moment an order event, a fill or a resync's snapshot shows it open, under
//! its venue id, and leaves the view only when an order event ends it under that id
//! (cancelled, filled, rejected or expired; an amend under a new id moves it there). A later
//! snapshot not showing it does not clear it: an order seen after the venue read the account
//! is not in that snapshot, and nothing here orders the two (FBC-6m4q). One seen with no venue
//! id cannot be ended by any event, and stays in view. Whatever stays in view keeps the
//! market's cancel everything explicit, which cancels every order of ours all the same.

use std::collections::{HashMap, HashSet};
use std::fmt;

use fbc_core::{
    CancelScope, ClientOrderId, InstrumentId, OrderCaps, OrderUpdate, Support, VenueCommand,
    VenueOrderId, VenueOrderState,
};

use crate::permit::{CancelPlan, PermittedCommand};
use crate::record::{OrdState, OrderRecord};
use crate::registry::Registry;

/// What the kill switch's cancel everything built for one market
/// ([`Registry::cancel_everything`]).
#[derive(Eq, PartialEq, Debug)]
pub enum CancelEverything {
    /// An instrument cancel-all of the market, built under 0005's I7 guard.
    CancelAll {
        /// The instrument cancel-all.
        command: PermittedCommand,
        /// The cancels of our orders on the market the venue may not hold yet (PendingNew or
        /// Unknown), which a cancel-all sent now can miss, by explicit reference as
        /// [`Registry::cancel_many`] builds them.
        unanswered: CancelPlan,
    },
    /// Our orders on the market cancelled by explicit reference, and why no cancel-all was
    /// built.
    Explicit {
        /// The cancels, as [`Registry::cancel_many`] built them.
        plan: CancelPlan,
        /// The first condition of the cancel-all that failed.
        why: CancelAllRefusal,
    },
}

/// Why the cancel everything of a market is no instrument cancel-all, checked in this order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelAllRefusal {
    /// The venue has no instrument cancel-all.
    Unsupported,
    /// The registry does not hold the market's exclusive lease: the market is disarmed, or the
    /// lease names given since no longer cover the lease it holds (0005's I7).
    NoExclusiveLease,
    /// No trustworthy resync has shown the account's open orders yet, so what else rests on
    /// the market is not known.
    NotViewed,
    /// An order not ours is in view on the market (0005's I7).
    ForeignInView,
}

impl fmt::Display for CancelAllRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CancelAllRefusal::Unsupported => "the venue has no instrument cancel-all",
            CancelAllRefusal::NoExclusiveLease => {
                "the market's exclusive lease is not held under the current lease names"
            }
            CancelAllRefusal::NotViewed => {
                "no trustworthy resync has shown the account's open orders yet"
            }
            CancelAllRefusal::ForeignInView => {
                "an order of another namespace or system is in view on the market"
            }
        })
    }
}

impl std::error::Error for CancelAllRefusal {}

/// The orders not ours in view, per market, and whether a trustworthy resync has shown the
/// account's open orders.
#[derive(Debug, Default)]
pub(crate) struct ForeignView {
    viewed: bool,
    by_market: HashMap<InstrumentId, HashSet<VenueOrderId>>,
    /// Markets on which an order not ours was seen with no venue id.
    nameless: HashSet<InstrumentId>,
}

impl ForeignView {
    /// A trustworthy resync showed the account's open orders.
    pub(crate) fn viewed(&mut self) {
        self.viewed = true;
    }

    /// An order not ours shown open on `market` under `vid`.
    pub(crate) fn open(&mut self, market: InstrumentId, vid: Option<&VenueOrderId>) {
        match vid {
            Some(v) => {
                self.by_market.entry(market).or_default().insert(v.clone());
            }
            None => {
                self.nameless.insert(market);
            }
        }
    }

    /// An order event of an order not ours.
    pub(crate) fn update(&mut self, u: &OrderUpdate) {
        match &u.state {
            VenueOrderState::Open | VenueOrderState::Amended { new_vid: None } => {
                self.open(u.inst, u.vid.as_ref());
            }
            VenueOrderState::Amended { new_vid: Some(new) } => {
                self.end(u.inst, u.vid.as_ref());
                self.open(u.inst, Some(new));
            }
            VenueOrderState::Filled
            | VenueOrderState::Canceled(_)
            | VenueOrderState::Rejected(_)
            | VenueOrderState::Expired => self.end(u.inst, u.vid.as_ref()),
        }
    }

    /// The order under `vid` on `market` ended; one with no venue id names nothing to end.
    fn end(&mut self, market: InstrumentId, vid: Option<&VenueOrderId>) {
        if let (Some(v), Some(seen)) = (vid, self.by_market.get_mut(&market)) {
            seen.remove(v);
        }
    }

    pub(crate) fn in_view(&self, market: InstrumentId) -> bool {
        self.nameless.contains(&market)
            || self.by_market.get(&market).is_some_and(|s| !s.is_empty())
    }
}

impl Registry {
    /// Whether an order not ours is in view on `market` (the module documentation says when).
    pub fn foreign_in_view(&self, market: InstrumentId) -> bool {
        self.foreign.in_view(market)
    }

    /// The kill switch's cancel everything for `market` on a venue whose order caps are `caps`
    /// (decision 0012), built in every market state: an instrument cancel-all only when the
    /// venue has one, the registry holds the market's exclusive lease, a trustworthy resync has
    /// shown the account's open orders and no order not ours is in view on the market, with
    /// the explicit cancels of our PendingNew and Unknown orders on it, which it can miss;
    /// otherwise the cancels of our orders on the market that are not terminal, in client id
    /// order, each by explicit reference as [`Registry::cancel_many`] builds them, a cancel in
    /// flight included. Never an account cancel-all. Built again after each resync for every
    /// market it names ([`ResyncReport::cancel_everything`](crate::ResyncReport)).
    pub fn cancel_everything(
        &mut self,
        market: InstrumentId,
        caps: &OrderCaps,
    ) -> CancelEverything {
        let why = if caps.cancel_all_instrument != Support::Native {
            Some(CancelAllRefusal::Unsupported)
        } else if !self.entries.holds_exclusive(market) {
            Some(CancelAllRefusal::NoExclusiveLease)
        } else if !self.foreign.viewed {
            Some(CancelAllRefusal::NotViewed)
        } else if self.foreign.in_view(market) {
            Some(CancelAllRefusal::ForeignInView)
        } else {
            None
        };
        match why {
            None => {
                let unanswered = self.ours_on(market, |state| {
                    matches!(state, OrdState::PendingNew | OrdState::Unknown)
                });
                CancelEverything::CancelAll {
                    command: PermittedCommand::admitted(VenueCommand::CancelAll(
                        CancelScope::Instrument(market),
                    )),
                    unanswered: self.cancel_many(&unanswered, caps),
                }
            }
            Some(why) => {
                let ours = self.ours_on(market, |state| !state.is_terminal());
                CancelEverything::Explicit {
                    plan: self.cancel_many(&ours, caps),
                    why,
                }
            }
        }
    }

    /// Our orders on `market` whose state `keep` takes, in client id order.
    fn ours_on(&self, market: InstrumentId, keep: impl Fn(OrdState) -> bool) -> Vec<ClientOrderId> {
        let mut cids: Vec<ClientOrderId> = self
            .orders
            .values()
            .filter(|rec| rec.placed().inst == market && keep(rec.state()))
            .map(OrderRecord::cid)
            .collect();
        cids.sort();
        cids
    }
}
