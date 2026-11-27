//! The kill switch's "cancel everything" for one market (decision 0012), within 0005's I4 and
//! I7, and the foreign orders in view that decide its shape.
//!
//! [`Registry::cancel_everything`] builds, in every market state, either
//!
//! - an instrument cancel-all ([`CancelEverything::CancelAll`]), only when the venue declares
//!   one (`OrderCaps::cancel_all_instrument`), the registry holds the market's exclusive
//!   account-and-instrument lease (its [`MarketLease`](fbc_core::MarketLease), held while the
//!   market is armed and still covered by the registry's current lease names), the account's
//!   open orders are viewed (below), and no order that is not ours is in view on the market;
//!   with it, the explicit cancels of our orders the venue may not hold yet
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
//! as 0012 says. Our own-namespace orders the registry did not hold (0005's I7 orphans, on a
//! market a resync already seeded) are registered by the resync that acts on them
//! (`src/orphans.rs`, decision 0108), so the explicit cancels reach them too.
//!
//! **In view.** An order not ours (another namespace's, a non-canonical client id, or one
//! nothing attributes: no client id and a venue id no order the registry holds had, a
//! forgotten order of ours included, decision 0157) is in view on its
//! market from the moment an order event, a fill or a resync's snapshot shows it open, under
//! its venue id, and leaves the view only when an order event ends it under that id
//! (cancelled, filled, rejected or expired; an amend under a new id moves it there). A later
//! snapshot not showing it does not clear it: an order seen after the venue read the account
//! is not in that snapshot, and nothing here orders the two (FBC-6m4q). One seen with no venue
//! id cannot be ended by any event, and stays in view. Whatever stays in view keeps the
//! market's cancel everything explicit, which cancels every order of ours all the same.
//!
//! **Shown resting** (decision 0158). The refusal of a place or amend that could add to a
//! market's position reads a set of its own, apart from the view above, which it leaves as it
//! is: an order not ours (another namespace's or a non-canonical client id, or none) that an
//! order event shows open or amended, or a resync's snapshot from any source lists open, under a
//! venue id no order the registry holds had or one of an order it holds as ended Lost or never
//! sent. A fill shows nothing resting. A sighting is stamped with the next order-entry epoch end
//! or retention pass the consumer reports after it, never earlier than the latest end already
//! reported, and unstamped again each time the order is shown again. It lifts by the order's
//! end under its venue id, an amend moving it to the new id, by a trustworthy resync requested
//! at or after its stamp plus the venue's snapshot lag bound whose snapshot does not list its
//! venue id open, or by the registry coming to hold its venue id as an order of ours not ended
//! Lost or never sent. A venue id such an order ended under, or of a forgotten order of ours not
//! ended never sent, is kept as ended: a later event showing it open shows nothing resting, nor
//! does a resync row, unless from a trustworthy snapshot requested at or after the end's stamp
//! plus the lag bound. A retention pass forgets an ended entry once its stamp is at least the
//! pass's floor (the lag bound plus the consumer's resync bound, decision 0157 item 3) before
//! its `now`.
//!
//! **Viewed.** The account's open orders are viewed from the first trustworthy resync on, until
//! an order-entry connection epoch ends: an order not ours placed while the account's private
//! stream was down never reaches the view, so after the end only a trustworthy resync requested
//! after it shows the account's open orders again (FBC-9svb). The consumer reports each end
//! through [`Registry::order_entry_epoch_ended`], from its `ExecHandler::on_epoch_end`, with the
//! last `recv_mono` of the envelopes the session handed it before the end, of any of its
//! connections, which is never earlier than the ended epoch's own last envelope and is that
//! envelope's on a session of one connection: decision 0039 gives an epoch's end no stamp of its
//! own, and that value is the same in replay. A resync requested at or before the latest end
//! reported restores nothing, even one answered after it, and a resync from an untrustworthy
//! source never does. Until a resync restores it, the cancel everything is explicit
//! ([`CancelAllRefusal::NotViewed`]), and so is the orphan escalation (`src/orphans.rs`). An
//! instrument cancel-all authorized before an end is not voided by it (FBC-vkju).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use fbc_core::{
    CancelScope, ClientOrderId, InstrumentId, MonoNs, OrderCaps, OrderUpdate, Support,
    VenueCommand, VenueOrderId, VenueOrderState,
};

use crate::grant::{Counters, Guard, Watch};
use crate::permit::{CancelPlan, PermittedCommand};
use crate::record::{OrdState, OrderRecord};
use crate::registry::Registry;
use crate::retain::reached;

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
    /// No trustworthy resync has shown the account's open orders yet, or none requested after
    /// the latest order-entry epoch end the consumer reported
    /// ([`Registry::order_entry_epoch_ended`]), so what else rests on the market is not known.
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
                "no trustworthy resync has shown the account's open orders yet, or none requested \
                 after the latest order-entry connection end the consumer reported"
            }
            CancelAllRefusal::ForeignInView => {
                "an order of another namespace or system is in view on the market"
            }
        })
    }
}

impl std::error::Error for CancelAllRefusal {}

/// The orders not ours in view, per market, and whether a trustworthy resync has shown the
/// account's open orders since the latest order-entry epoch end.
#[derive(Debug, Default)]
pub(crate) struct ForeignView {
    viewed: bool,
    /// The latest order-entry epoch end the consumer reported: only a trustworthy resync
    /// requested after it views the account's open orders.
    ended: Option<MonoNs>,
    by_market: HashMap<InstrumentId, HashSet<VenueOrderId>>,
    /// Markets on which an order not ours was seen with no venue id.
    nameless: HashSet<InstrumentId>,
    /// Per market, how many events showed an order not ours open there, which an instrument
    /// cancel-all's authorization is checked against at submit (decision 0060).
    seen: Counters,
    /// Per market, the orders not ours shown resting there (decision 0158 item 1), which hold
    /// its places and amends that could add to its position.
    resting: HashMap<InstrumentId, Resting>,
    /// Per market, the venue ids kept as ended (decision 0158 item 6): an order not ours ended
    /// under one, or a forgotten order of ours not ended never sent had one.
    gone: HashMap<InstrumentId, HashMap<VenueOrderId, Stamp>>,
    /// Per market, how many times an order not ours came to be shown resting there with none
    /// before, which a place's, a batch's or an amend's authorization item 2 would have refused
    /// is checked against at submit (decision 0158 item 7).
    resting_seen: Counters,
}

/// When a sighting, or an entry kept as ended, was stamped (decision 0158 item 5).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Stamp {
    /// Not yet: no order-entry epoch end or retention pass was reported since.
    Pending,
    /// At the first instant reported since, never earlier than the latest end reported.
    At(MonoNs),
}

impl Stamp {
    /// Whether `now` is at least `after` past the stamp; never while it is pending.
    fn reached(self, now: MonoNs, after: Duration) -> bool {
        match self {
            Stamp::Pending => false,
            Stamp::At(at) => reached(now, at, after),
        }
    }
}

/// The orders not ours shown resting on one market (decision 0158 item 1).
#[derive(Debug, Default)]
struct Resting {
    /// By venue id.
    open: HashMap<VenueOrderId, Stamp>,
    /// One shown with no venue id.
    nameless: Option<Stamp>,
}

impl Resting {
    fn is_empty(&self) -> bool {
        self.open.is_empty() && self.nameless.is_none()
    }

    fn stamp(&mut self, at: MonoNs) {
        for stamp in self.open.values_mut().chain(self.nameless.as_mut()) {
            if *stamp == Stamp::Pending {
                *stamp = Stamp::At(at);
            }
        }
    }
}

/// The orders not ours shown resting on a market ([`Registry::not_ours_shown`], decision 0158
/// item 8), for the owner to cancel by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotOursShown {
    /// Their venue ids, in order.
    pub ids: Vec<VenueOrderId>,
    /// Whether one is shown with no venue id.
    pub nameless: bool,
}

impl ForeignView {
    /// A trustworthy resync requested at `requested_at` showed the account's open orders: viewed
    /// unless it was requested at or before the latest order-entry epoch end.
    pub(crate) fn viewed(&mut self, requested_at: MonoNs) {
        if self.ended.is_none_or(|end| requested_at > end) {
            self.viewed = true;
        }
    }

    /// An order-entry epoch ended at `at`: the account's open orders are not viewed until a
    /// trustworthy resync requested after the latest end reported.
    fn epoch_ended(&mut self, at: MonoNs) {
        self.viewed = false;
        let latest = self.ended.map_or(at, |end| end.max(at));
        self.ended = Some(latest);
        // Every sighting not yet stamped, at the latest end reported (decision 0158 item 5).
        self.stamp_pending(latest);
    }

    /// An order not ours shown open on `market` under `vid`.
    pub(crate) fn open(&mut self, market: InstrumentId, vid: Option<&VenueOrderId>) {
        self.seen.advance(market);
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

    /// How many events showed an order not ours open on `market` so far, watched for an
    /// instrument cancel-all about to be built.
    pub(crate) fn watch(&mut self, market: InstrumentId) -> Watch {
        self.seen.watch(market)
    }

    pub(crate) fn in_view(&self, market: InstrumentId) -> bool {
        self.nameless.contains(&market)
            || self.by_market.get(&market).is_some_and(|s| !s.is_empty())
    }

    /// An order not ours shown resting on `market` under `vid` (decision 0158 item 1), pending
    /// its stamp, an entry already there unstamped again (item 5); nothing when `vid` is kept as
    /// ended there (item 6).
    pub(crate) fn shown(&mut self, market: InstrumentId, vid: Option<&VenueOrderId>) {
        if vid.is_some_and(|v| self.kept_ended(market, v)) {
            return;
        }
        let resting = self.resting.entry(market).or_default();
        let was_empty = resting.is_empty();
        match vid {
            Some(v) => {
                resting.open.insert(v.clone(), Stamp::Pending);
            }
            None => resting.nameless = Some(Stamp::Pending),
        }
        if was_empty {
            self.resting_seen.advance(market);
        }
    }

    /// A row of a snapshot requested at `requested_at` lists an order not ours open on `market`
    /// under `vid`: shown resting as [`ForeignView::shown`] shows it, unless `vid` is kept as
    /// ended there, when only a row from a `trustworthy` snapshot requested at or after the
    /// end's stamp plus the venue's lag `bound` shows it again (decision 0158 item 6).
    pub(crate) fn shown_row(
        &mut self,
        market: InstrumentId,
        vid: &VenueOrderId,
        requested_at: MonoNs,
        trustworthy: bool,
        bound: Duration,
    ) {
        if let Some(gone) = self.gone.get_mut(&market)
            && let Some(&stamp) = gone.get(vid)
        {
            if !(trustworthy && stamp.reached(requested_at, bound)) {
                return;
            }
            gone.remove(vid);
        }
        self.shown(market, Some(vid));
    }

    /// An order event ended the order under `vid` on `market` (decision 0158 items 4 and 6): no
    /// longer shown resting, and its venue id kept as ended, pending its stamp; an entry already
    /// kept is left as it is. One with no venue id names nothing to end.
    pub(crate) fn ended(&mut self, market: InstrumentId, vid: Option<&VenueOrderId>) {
        let Some(v) = vid else {
            return;
        };
        if let Some(resting) = self.resting.get_mut(&market) {
            resting.open.remove(v);
            if resting.is_empty() {
                self.resting.remove(&market);
            }
        }
        self.gone
            .entry(market)
            .or_default()
            .entry(v.clone())
            .or_insert(Stamp::Pending);
    }

    /// A retention pass at `at` forgot an order of ours on `market` known by `vid`, not ended
    /// never sent: its venue id is kept as ended, stamped `at` (decision 0158 item 6).
    pub(crate) fn forgotten(&mut self, market: InstrumentId, vid: &VenueOrderId, at: MonoNs) {
        self.gone
            .entry(market)
            .or_default()
            .entry(vid.clone())
            .or_insert(Stamp::At(at));
    }

    /// Stamps every sighting and ended entry not yet stamped with `at` (decision 0158 item 5).
    fn stamp_pending(&mut self, at: MonoNs) {
        for resting in self.resting.values_mut() {
            resting.stamp(at);
        }
        for stamp in self.gone.values_mut().flat_map(HashMap::values_mut) {
            if *stamp == Stamp::Pending {
                *stamp = Stamp::At(at);
            }
        }
    }

    /// A retention pass at `now`: stamps every sighting and ended entry not yet stamped with
    /// the larger of `now` and the latest order-entry epoch end reported (decision 0158 item 5).
    pub(crate) fn retention_pass(&mut self, now: MonoNs) {
        self.stamp_pending(self.ended.map_or(now, |end| end.max(now)));
    }

    /// A trustworthy resync requested at `requested_at` whose snapshot lists open the
    /// (market, venue id) pairs in `listed`: lifts every sighting stamped at least the venue's
    /// lag `bound` before it that `listed` lacks, and every one shown with no venue id stamped
    /// so, and names them by market then venue id, one with no venue id first (decision 0158
    /// item 4).
    pub(crate) fn lift(
        &mut self,
        requested_at: MonoNs,
        bound: Duration,
        listed: &HashSet<(InstrumentId, VenueOrderId)>,
    ) -> Vec<(InstrumentId, Option<VenueOrderId>)> {
        let mut lifted = Vec::new();
        for (&market, resting) in &mut self.resting {
            resting.open.retain(|v, stamp| {
                let lifts =
                    stamp.reached(requested_at, bound) && !listed.contains(&(market, v.clone()));
                if lifts {
                    lifted.push((market, Some(v.clone())));
                }
                !lifts
            });
            if resting
                .nameless
                .is_some_and(|stamp| stamp.reached(requested_at, bound))
            {
                resting.nameless = None;
                lifted.push((market, None));
            }
        }
        self.resting.retain(|_, resting| !resting.is_empty());
        lifted.sort_by(|(a, x), (b, y)| {
            (a, x.as_ref().map(VenueOrderId::as_str))
                .cmp(&(b, y.as_ref().map(VenueOrderId::as_str)))
        });
        lifted
    }

    /// The registry came to hold `vid` as an order of ours not ended Lost or never sent: no
    /// order not ours is shown resting under it, nor kept as ended under it, on any market
    /// (decision 0158 item 4).
    pub(crate) fn learnt(&mut self, vid: &VenueOrderId) {
        if self.resting.is_empty() && self.gone.is_empty() {
            return;
        }
        for resting in self.resting.values_mut() {
            resting.open.remove(vid);
        }
        self.resting.retain(|_, resting| !resting.is_empty());
        for gone in self.gone.values_mut() {
            gone.remove(vid);
        }
        self.gone.retain(|_, gone| !gone.is_empty());
    }

    /// Whether `vid` is kept as ended on `market` (decision 0158 item 6).
    pub(crate) fn kept_ended(&self, market: InstrumentId, vid: &VenueOrderId) -> bool {
        self.gone.get(&market).is_some_and(|g| g.contains_key(vid))
    }

    /// Whether an order not ours is shown resting on `market` under `vid`.
    pub(crate) fn has_sighting(&self, market: InstrumentId, vid: &VenueOrderId) -> bool {
        self.resting
            .get(&market)
            .is_some_and(|resting| resting.open.contains_key(vid))
    }

    /// Forgets every entry kept as ended whose stamp is at least `floor` before `now`, none when
    /// there is no floor (decision 0158 item 6).
    pub(crate) fn prune(&mut self, now: MonoNs, floor: Option<Duration>) {
        let Some(floor) = floor else {
            return;
        };
        for gone in self.gone.values_mut() {
            gone.retain(|_, stamp| !stamp.reached(now, floor));
        }
        self.gone.retain(|_, gone| !gone.is_empty());
    }

    /// Whether an order not ours is shown resting on `market` (decision 0158 item 2).
    pub(crate) fn resting_on(&self, market: InstrumentId) -> bool {
        self.resting
            .get(&market)
            .is_some_and(|resting| !resting.is_empty())
    }

    /// How many times an order not ours came to be shown resting on `market` with none before,
    /// watched for a command item 2 would refuse beside one (decision 0158 item 7).
    pub(crate) fn watch_resting(&mut self, market: InstrumentId) -> Watch {
        self.resting_seen.watch(market)
    }

    /// The orders not ours shown resting on `market`.
    fn shown_report(&self, market: InstrumentId) -> NotOursShown {
        let resting = self.resting.get(&market);
        let mut ids: Vec<VenueOrderId> = resting
            .map(|r| r.open.keys().cloned().collect())
            .unwrap_or_default();
        ids.sort_by(|x, y| x.as_str().cmp(y.as_str()));
        NotOursShown {
            ids,
            nameless: resting.is_some_and(|r| r.nameless.is_some()),
        }
    }
}

impl Registry {
    /// Whether an order not ours is in view on `market` (the module documentation says when).
    pub fn foreign_in_view(&self, market: InstrumentId) -> bool {
        self.foreign.in_view(market)
    }

    /// Whether an order not ours is shown resting on `market` (decision 0158 item 1; the module
    /// documentation says when), which holds every place, batch item and amend there that could
    /// add to the position (item 2).
    pub fn not_ours_resting(&self, market: InstrumentId) -> bool {
        self.foreign.resting_on(market)
    }

    /// The orders not ours shown resting on `market`, by venue id, and whether one is shown there
    /// with no venue id, so the owner can cancel them by hand (decision 0158 item 8).
    pub fn not_ours_shown(&self, market: InstrumentId) -> NotOursShown {
        self.foreign.shown_report(market)
    }

    /// An order-entry connection epoch of the registry's account ended (FBC-9svb): the consumer
    /// calls it from its `ExecHandler::on_epoch_end`, once per end, with `at`, the last
    /// `recv_mono` of the envelopes the session handed it before the end, of any of its
    /// connections (never earlier than the ended epoch's own last envelope, and that envelope's
    /// on a session of one connection; decision 0039 gives the end no stamp of its own, and that
    /// value is the same in replay). From it the account's open orders are not viewed, so the
    /// kill switch's cancel everything and the orphan escalation build no instrument
    /// cancel-all ([`CancelAllRefusal::NotViewed`]), until a trustworthy resync requested after
    /// the latest end reported shows them again: an order not ours placed while the stream was
    /// down is in no earlier snapshot. An end earlier than one already reported leaves the
    /// latest end where it is, but still clears the view.
    ///
    /// It also stamps every order not ours shown resting, and every venue id kept as ended, not
    /// yet stamped with the latest end reported (decision 0158 item 5). The consumer reports
    /// `at` on the shard's clock, no earlier than the arrival of every order event and snapshot
    /// it has applied to the registry before the call, from any session on the account.
    pub fn order_entry_epoch_ended(&mut self, at: MonoNs) {
        self.reap_dropped();
        self.foreign.epoch_ended(at);
    }

    /// The latest order-entry epoch end the consumer reported
    /// ([`Registry::order_entry_epoch_ended`]); `None` before the first.
    pub fn order_entry_epoch_end(&self) -> Option<MonoNs> {
        self.foreign.ended
    }

    /// The kill switch's cancel everything for `market` on a venue whose order caps are `caps`
    /// (decision 0012), built in every market state: an instrument cancel-all only when the
    /// venue has one, the registry holds the market's exclusive lease, a trustworthy resync
    /// requested after the latest order-entry epoch end the consumer reported
    /// ([`Registry::order_entry_epoch_ended`], which the consumer must call at each end) has
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
        self.reap_dropped();
        match self.cancel_all_refusal(market, caps) {
            None => {
                let (command, unanswered) = self.instrument_cancel_all(market, caps);
                CancelEverything::CancelAll {
                    command,
                    unanswered,
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

    /// The first of 0005's I7 conditions for an instrument cancel-all of `market` that fails,
    /// in [`CancelAllRefusal`]'s order; `None` when one may be built.
    pub(crate) fn cancel_all_refusal(
        &self,
        market: InstrumentId,
        caps: &OrderCaps,
    ) -> Option<CancelAllRefusal> {
        if caps.cancel_all_instrument != Support::Native {
            Some(CancelAllRefusal::Unsupported)
        } else if !self.entries.holds_exclusive(market) {
            Some(CancelAllRefusal::NoExclusiveLease)
        } else if !self.foreign.viewed {
            Some(CancelAllRefusal::NotViewed)
        } else if self.foreign.in_view(market) {
            Some(CancelAllRefusal::ForeignInView)
        } else {
            None
        }
    }

    /// The instrument cancel-all of `market`, once [`Registry::cancel_all_refusal`] found
    /// nothing against it, with the explicit cancels of our PendingNew and Unknown orders on
    /// it, which it can miss.
    pub(crate) fn instrument_cancel_all(
        &mut self,
        market: InstrumentId,
        caps: &OrderCaps,
    ) -> (PermittedCommand, CancelPlan) {
        let unanswered = self.ours_on(market, |state| {
            matches!(state, OrdState::PendingNew | OrdState::Unknown)
        });
        // Checked at submit against I7's inputs as they are now: the registry's hold of the
        // market's exclusive lease and the foreign orders seen. Either moving may undo the
        // guard it was built under; a change of state that leaves both (the kill switch, its
        // lift) must not hold it back (0060; RB94-1).
        let guard = Guard::exclusive(
            self.entries.watch_exclusive(market),
            self.foreign.watch(market),
        );
        (
            PermittedCommand::guarded(
                VenueCommand::CancelAll(CancelScope::Instrument(market)),
                guard,
                self.instance(),
            ),
            self.cancel_many(&unanswered, caps),
        )
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
