//! The registry of our orders by client id, the routing of venue events and accepted fills to
//! them, the inventory those fills moved, and the pre-trade caps every place, amend and batch
//! item is built under (0013 rule 2: the inventory cap, 0005's I6, and the resting cap,
//! decision 0052).

use std::collections::{HashMap, HashSet};
use std::fmt;

use fbc_core::{
    CidMatch, ClientOrderId, InstrumentId, ItemRef, Lots, MonoNs, Namespace, NewOrder, OrderCaps,
    OrderUpdate, RpcId, Side, SignedLots, SubmitOutcome, Ticks, VenueCommand, VenueOrderId,
};

use crate::caps::{Adds, CapRefusal, Exposure, PreTradeCaps};
use crate::entry::{Entries, StateRefusal};
use crate::grant::Guard;
use crate::ladder;
use crate::ledger::AcceptedFill;
use crate::permit::{
    self, CancelChoice, CancelPlan, Cancellable, Live, PermitRefusal, PermittedCommand, PlacePlan,
};
use crate::record::{Applied, FillApplied, OrderKey, OrderOp, OrderRecord, OutcomeApplied};
use crate::resync::{MarketState, Placed, Placement, Seed};
use crate::sweep::ForeignView;

/// Our orders, by client id, with an index of every venue id they were known by, the
/// inventory per instrument that the fills the ledger accepted moved, and the Unknown ladder's
/// queries out and orders lost ([`Registry::ladder`]), and the consumer's pre-trade caps.
#[derive(Debug, Default)]
pub struct Registry {
    pub(crate) orders: HashMap<ClientOrderId, OrderRecord>,
    caps: PreTradeCaps,
    by_vid: HashMap<VenueOrderId, ClientOrderId>,
    pub(crate) inventory: HashMap<InstrumentId, SignedLots>,
    /// Each market's position as the registry knows it: not seeded (with the fills accepted
    /// so far), seeded by a resync or by hand, or unsettled; only on a seeded market is the
    /// inventory the position.
    pub(crate) markets: HashMap<InstrumentId, MarketState>,
    /// The ledger whose fills the registry applies: the first one it was given.
    ledger: Option<u64>,
    /// The ladder's queries out, by request.
    pub(crate) queries: HashMap<RpcId, ClientOrderId>,
    /// How many orders ended Lost.
    pub(crate) lost: u64,
    /// Each market's armed flag and order-entry state, and the leases arming took (decision
    /// 0012).
    pub(crate) entries: Entries,
    /// The orders not ours in view, which decide whether the kill switch's cancel everything
    /// may be an instrument cancel-all (0005's I7; [`Registry::cancel_everything`]).
    pub(crate) foreign: ForeignView,
}

/// Where [`Registry::apply_fill`] sent a fill the ledger accepted.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillRouted {
    /// To our order `cid`, with what the order did with it; the inventory moved.
    Ours(ClientOrderId, FillApplied),
    /// Flagged, not counted: it names another namespace's client id (decision 0005, I4).
    Foreign(Namespace),
    /// Flagged, not counted: its client id is not canonical (another system's order).
    NotCanonical,
    /// To no order the registry holds, under our namespace's client id `cid` (an order of an
    /// earlier run, say): the inventory moved.
    OursUntracked(ClientOrderId),
    /// Kept by the ledger and, when the registry holds its order `cid`, counted on it, but not
    /// on the inventory: the position the market's resync seeded already holds it (decision
    /// 0055). The order's record has applied the fill: what it did with it (a completion, say)
    /// is read from the record ([`Registry::get`]).
    InSnapshot(ClientOrderId),
    /// Counted, but it falls between the request and the answer of the resync that seeded its
    /// market and nothing shows on which side: the market's position is unknown from now on
    /// ([`Registry::position`]; decision 0055). As for `InSnapshot`, a held order's record has
    /// applied the fill.
    Unsettled(ClientOrderId),
    /// Flagged, not counted: it routes to our order `cid` but names another instrument or the
    /// other side than the order's placement (a misdecoded or misrouted fill).
    Disagrees(ClientOrderId),
    /// Flagged, not counted: its client id `cid` (ours) and its venue id, held by our order
    /// `by_vid`, name different orders.
    Conflicting {
        cid: ClientOrderId,
        by_vid: ClientOrderId,
    },
    /// Flagged, not counted: it names no client id and no venue id of an order the registry
    /// holds, so nothing shows it is ours (another namespace's order on a venue that echoes no
    /// client id looks the same; decision 0005, I4).
    Unattributed,
}

/// Where [`Registry::apply_update`] sent an update.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Routed {
    /// To our order `cid`, with what the order did with it.
    Ours(ClientOrderId, Applied),
    /// Nowhere: it names another namespace's client id (another engine on the account).
    Foreign(Namespace),
    /// Nowhere: its client id is not canonical (another system's order).
    NotCanonical,
    /// Nowhere: neither its client id nor its venue id names an order the registry holds.
    Untracked,
}

/// What the registry refused.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum OmsError {
    /// The registry already holds an order under this client id: a client id is never reused,
    /// so an order (an Unknown one included) is never placed again under it.
    DuplicateCid(ClientOrderId),
    /// The registry holds no order under this client id.
    UnknownCid(ClientOrderId),
    /// The outcome was given for `cid`, but its item names `item`.
    ItemNamesAnother {
        cid: ClientOrderId,
        item: ClientOrderId,
    },
    /// Counting the fill would overflow its order's fill sum or the instrument's inventory:
    /// nothing was counted, and the ledger did not record it.
    FillOverflow(InstrumentId),
    /// The fill was accepted by another ledger than the one whose fills the registry applies,
    /// which cannot vouch that the registry never counted it.
    OtherLedger,
    /// The order's placement nonce was already recorded as another value.
    NonceRecorded(ClientOrderId),
    /// The order's placement was already recorded as sent at other instants.
    SentRecorded(ClientOrderId),
    /// A pre-trade cap refused the place or batch item (0013 rule 2): it was never built.
    Capped(CapRefusal),
    /// The market's state refused the place or batch item before either cap was consulted
    /// (decision 0012): it was never built.
    State(StateRefusal),
    /// The batch names more than one market: a batch is one market's command, so none of it
    /// was built.
    MixedMarkets,
    /// The market's position was already seeded.
    PositionSeeded(InstrumentId),
    /// A fill moved the market's inventory before its position was seeded.
    PositionMoved(InstrumentId),
}

impl fmt::Display for OmsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OmsError::DuplicateCid(cid) => {
                write!(f, "an order is already registered under client id {cid:?}")
            }
            OmsError::UnknownCid(cid) => write!(f, "no order is registered under {cid:?}"),
            OmsError::ItemNamesAnother { cid, item } => {
                write!(f, "an outcome for {cid:?} names another order, {item:?}")
            }
            OmsError::FillOverflow(inst) => {
                write!(
                    f,
                    "a fill on {inst:?} overflows its order's fill sum or the inventory"
                )
            }
            OmsError::OtherLedger => {
                write!(
                    f,
                    "the fill was accepted by another fill ledger than this registry's"
                )
            }
            OmsError::NonceRecorded(cid) => {
                write!(f, "{cid:?} already has another placement nonce recorded")
            }
            OmsError::SentRecorded(cid) => {
                write!(f, "{cid:?} was already recorded as sent at other instants")
            }
            OmsError::Capped(refusal) => write!(f, "refused by a pre-trade cap: {refusal}"),
            OmsError::State(refusal) => write!(f, "refused by the market's state: {refusal}"),
            OmsError::MixedMarkets => write!(f, "a batch of places names more than one market"),
            OmsError::PositionSeeded(inst) => {
                write!(f, "the position on {inst:?} was already seeded")
            }
            OmsError::PositionMoved(inst) => write!(
                f,
                "a fill moved the inventory on {inst:?} before its position was seeded"
            ),
        }
    }
}

impl std::error::Error for OmsError {}

impl Registry {
    /// An empty registry with no pre-trade cap configured: it builds no place or amend until
    /// it is given caps ([`Registry::with_caps`]); cancels need none.
    pub fn new() -> Registry {
        Registry::default()
    }

    /// An empty registry under the consumer's pre-trade caps (0009, 0013 rule 2).
    pub fn with_caps(caps: PreTradeCaps) -> Registry {
        Registry {
            caps,
            ..Registry::default()
        }
    }

    /// The pre-trade caps every place, amend and batch item is checked against.
    pub fn caps(&self) -> &PreTradeCaps {
        &self.caps
    }

    /// The quantity our orders on `inst` and `side` may have resting: each order's
    /// [`OrderRecord::resting`], so PendingNew and Unknown orders count in full, a partly
    /// filled order its remainder until it is terminal, and an amend at the larger of its old
    /// and new quantity from when it is built until it is acknowledged. `None` when the sum
    /// does not fit a lot count. The resting cap bounds this, with the order judged, per side;
    /// the inventory cap counts each order's [`OrderRecord::exposure`]: this, plus the fills
    /// the venue reported that the inventory does not hold yet.
    pub fn resting_on(&self, inst: InstrumentId, side: Side) -> Option<Lots> {
        self.sum_on(inst, side, None, OrderRecord::resting)
    }

    /// The sum of `each` over our orders on `inst` and `side` but `except`; `None` when it
    /// does not fit a lot count.
    fn sum_on(
        &self,
        inst: InstrumentId,
        side: Side,
        except: Option<ClientOrderId>,
        each: fn(&OrderRecord) -> Lots,
    ) -> Option<Lots> {
        self.orders
            .values()
            .filter(|rec| {
                let placed = rec.placed();
                placed.inst == inst && placed.side == side && Some(placed.cid) != except
            })
            .try_fold(Lots::ZERO, |sum, rec| sum.checked_add(each(rec)))
    }

    /// What `inst` and `side` hold before an order is judged against the pre-trade caps: the
    /// market's caps, the position, and what our orders on the side but `except` (the order
    /// an amend changes) may add to the position and have resting.
    fn exposure(&self, inst: InstrumentId, side: Side, except: Option<ClientOrderId>) -> Exposure {
        Exposure {
            inst,
            side,
            cap: self.caps.market(inst),
            pos: self.position(inst),
            others: self.sum_on(inst, side, except, OrderRecord::exposure),
            others_resting: self.sum_on(inst, side, except, OrderRecord::resting),
        }
    }

    /// Builds the place of `order` when its market's state and pre-trade caps admit it, and
    /// registers it PendingNew, so every later check counts it in full. Refused, never built
    /// and never registered, when the market's state builds no place, or, in Exit, when it is
    /// not an exit order on the side that reduces the position within the position's size
    /// ([`OmsError::State`]); refused, never built and never
    /// registered, when it would take the worst case on its side past the inventory cap, or
    /// what its side has resting past the resting cap, or its market has no caps
    /// ([`OmsError::Capped`]), reducing and reduce-only orders included,
    /// or when an order is already registered under its client id. A place built and then not
    /// sent is reported as such ([`Registry::on_outcome`]), which ends the order.
    pub fn place(&mut self, order: NewOrder) -> Result<PermittedCommand, OmsError> {
        self.admit_placement(&order)?;
        self.insert(order.clone())?;
        let guard = self.state_guard(order.inst);
        Ok(PermittedCommand::guarded(VenueCommand::Place(order), guard))
    }

    /// Builds a batch of places of one market from the items the pre-trade caps admit, in the
    /// order given, each judged with the earlier items admitted counted as PendingNew; the
    /// items admitted are registered PendingNew, and each item refused, by a cap or for a
    /// client id already registered (an earlier item's included), is never built and listed
    /// in [`PlacePlan::refused`]. Refused whole, nothing registered, when the items name more
    /// than one market. An empty batch, or one whose every item is refused, builds nothing.
    pub fn place_batch(&mut self, orders: Vec<NewOrder>) -> Result<PlacePlan, OmsError> {
        if let Some(first) = orders.first()
            && orders.iter().any(|o| o.inst != first.inst)
        {
            return Err(OmsError::MixedMarkets);
        }
        let mut plan = PlacePlan::default();
        let mut admitted = Vec::new();
        for order in orders {
            let cid = order.cid;
            match self
                .admit_placement(&order)
                .and_then(|()| self.insert(order.clone()).map(|_| ()))
            {
                Ok(()) => admitted.push(order),
                Err(refusal) => plan.refused.push((cid, refusal)),
            }
        }
        if let Some(first) = admitted.first() {
            let guard = self.state_guard(first.inst);
            plan.command = Some(PermittedCommand::guarded(
                VenueCommand::PlaceBatch(admitted),
                guard,
            ));
        }
        Ok(plan)
    }

    /// The guard of a place, a batch or an amend built now on `market`: its state generation,
    /// checked again at submit (decision 0060).
    fn state_guard(&mut self, market: InstrumentId) -> Guard {
        Guard::state(self.entries.watch(market))
    }

    /// The one pre-trade path of a place and a batch item: the market's state first (decision
    /// 0012), then, in Exit, Exit's admission, then both caps (0013 rule 2).
    fn admit_placement(&self, order: &NewOrder) -> Result<(), OmsError> {
        let admits = self.entries.admits(order.inst).map_err(OmsError::State)?;
        if self.orders.contains_key(&order.cid) {
            return Err(OmsError::DuplicateCid(order.cid));
        }
        let exposure = self.exposure(order.inst, order.side, None);
        let adds = Adds::placing(order.qty);
        admits
            .judge(&exposure, order.reduces(), adds.exposure)
            .map_err(OmsError::State)?;
        exposure.admit(adds).map_err(OmsError::Capped)
    }

    /// Registers a placement about to be sent, PendingNew, without building its command (an
    /// order learnt otherwise, say). Refused when an order is already registered under its
    /// client id. A place is built only through [`Registry::place`] or
    /// [`Registry::place_batch`], under the pre-trade caps.
    pub fn insert(&mut self, placed: NewOrder) -> Result<&OrderRecord, OmsError> {
        let cid = placed.cid;
        match self.orders.entry(cid) {
            std::collections::hash_map::Entry::Occupied(_) => Err(OmsError::DuplicateCid(cid)),
            std::collections::hash_map::Entry::Vacant(slot) => {
                Ok(slot.insert(OrderRecord::new(placed)))
            }
        }
    }

    /// The order registered under `cid`.
    pub fn get(&self, cid: ClientOrderId) -> Option<&OrderRecord> {
        self.orders.get(&cid)
    }

    /// The order the venue knows (or knew, before an amend replaced it) as `vid`.
    pub fn cid_of(&self, vid: &VenueOrderId) -> Option<ClientOrderId> {
        self.by_vid.get(vid).copied()
    }

    /// How many orders are registered.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// Whether no order is registered.
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Routes a venue order update to its order and applies it there
    /// ([`OrderRecord::apply_update`]). One of our client ids the registry holds names the
    /// order; otherwise the venue id does, current or superseded. Another namespace's or a
    /// non-canonical client id names none of ours. An update of an order not ours (another
    /// namespace's, a non-canonical client id, or no client id and a venue id no order of ours
    /// had) moves that order into or out of view ([`Registry::foreign_in_view`]).
    pub fn apply_update(&mut self, u: &OrderUpdate, key: OrderKey) -> Routed {
        let cid = match u.cid {
            Some(CidMatch::Foreign(ns)) => {
                self.foreign.update(u);
                return Routed::Foreign(ns);
            }
            Some(CidMatch::Unparseable) => {
                self.foreign.update(u);
                return Routed::NotCanonical;
            }
            Some(CidMatch::Ours(cid)) if self.orders.contains_key(&cid) => cid,
            Some(CidMatch::Ours(_)) | None => match u.vid.as_ref().and_then(|v| self.cid_of(v)) {
                Some(cid) => cid,
                None => {
                    if u.cid.is_none() {
                        self.foreign.update(u);
                    }
                    return Routed::Untracked;
                }
            },
        };
        let applied = self.with_record(cid, |rec| rec.apply_update(u, key));
        Routed::Ours(cid, applied)
    }

    /// Seeds the position on `inst` by hand, from which the fills the ledger accepts then all
    /// move it: a resync seeds a market from its snapshot instead ([`Registry::resync`]), and
    /// places the fills that straddle it. Until a market is seeded its position is unknown and
    /// the pre-trade caps admit no place or amend on it ([`CapRefusal::PositionUnknown`]).
    /// Refused once the market is seeded, and once a fill moved its inventory before it was
    /// seeded: only a resync's snapshot can place such a fill, so the market stays unknown.
    ///
    /// For tests, fixtures and an owner-assisted testnet run only: a hand seed is never a way
    /// around the rule that nothing is sent after a restart until the first trustworthy resync
    /// (decision 0055's Consequences, Reviewer B's RB80-10 on PR #80).
    pub fn seed_position(&mut self, inst: InstrumentId, pos: SignedLots) -> Result<(), OmsError> {
        match self.markets.get(&inst) {
            Some(MarketState::Seeded(_) | MarketState::Unsettled) => {
                return Err(OmsError::PositionSeeded(inst));
            }
            Some(MarketState::Unseeded(_)) => return Err(OmsError::PositionMoved(inst)),
            None => {}
        }
        self.inventory.insert(inst, pos);
        self.markets
            .insert(inst, MarketState::Seeded(Seed::by_hand(pos)));
        Ok(())
    }

    /// Whether the position on `inst` is known ([`Registry::position`]).
    pub fn position_known(&self, inst: InstrumentId) -> bool {
        self.position(inst).is_some()
    }

    /// The inventory on `inst`: the position seeded from the venue, moved by the accepted
    /// fills the seed does not hold; positive is long. Before the market is seeded, the sum of
    /// the fills accepted so far, which is not the position ([`Registry::position`]).
    pub fn inventory(&self, inst: InstrumentId) -> SignedLots {
        self.inventory.get(&inst).copied().unwrap_or(SignedLots(0))
    }

    /// Applies a fill the [`FillLedger`](crate::FillLedger) accepted, consuming it, so each
    /// accepted fill is applied once: the only path by which the inventory and an order's fill
    /// sum move (decision 0005, I3).
    ///
    /// Routed as [`Registry::apply_update`] routes an order update. A fill of our order counts
    /// on it ([`FillApplied`]), moves the inventory and indexes the venue id it names; one under our namespace's client id
    /// for no order the registry holds moves the inventory only; another namespace's, a
    /// non-canonical or an unattributed one, one whose client id and venue id name different
    /// orders, or one whose instrument or side is not its order's, moves nothing and is
    /// flagged; the first three put their order in view ([`Registry::foreign_in_view`]). None
    /// of this depends on the market's state: a fill in Killed or Cancel-only counts as in
    /// any other (decision 0012). The ledger records
    /// only a fill that counted, once it is applied: a flagged fill is not kept, so it never
    /// moves the retention horizon, and delivered again it is routed again (an unattributed
    /// fill reaches its order once the registry knows the order's venue id). Refused, counting
    /// nothing and leaving the ledger as it was, when the order's fill sum or the inventory
    /// would overflow, or when the fill comes from another ledger than the first one the
    /// registry took a fill from.
    pub fn apply_fill(&mut self, accepted: AcceptedFill<'_, '_>) -> Result<FillRouted, OmsError> {
        let ledger = accepted.ledger_id();
        if self.ledger.is_some_and(|ours| ours != ledger) {
            return Err(OmsError::OtherLedger);
        }
        self.ledger = Some(ledger);
        let fill = accepted.fill();
        let by_vid = fill.vid().and_then(|v| self.cid_of(v));
        let (cid, ours) = match fill.cid {
            Some(CidMatch::Foreign(ns)) => {
                self.foreign.open(fill.inst, fill.vid());
                return Ok(FillRouted::Foreign(ns));
            }
            Some(CidMatch::Unparseable) => {
                self.foreign.open(fill.inst, fill.vid());
                return Ok(FillRouted::NotCanonical);
            }
            Some(CidMatch::Ours(cid)) => match by_vid {
                Some(other) if other != cid => {
                    return Ok(FillRouted::Conflicting { cid, by_vid: other });
                }
                _ if self.orders.contains_key(&cid) => (Some(cid), cid),
                _ => (None, cid),
            },
            None => match by_vid {
                Some(cid) => (Some(cid), cid),
                None => {
                    self.foreign.open(fill.inst, fill.vid());
                    return Ok(FillRouted::Unattributed);
                }
            },
        };
        if let Some(cid) = cid {
            let placed = self.orders[&cid].placed();
            if placed.inst != fill.inst || placed.side != fill.side {
                return Ok(FillRouted::Disagrees(cid));
            }
        }
        let placed = Placed::of(
            ours,
            fill.vid().cloned(),
            fill.qty,
            fill.cum_after(),
            accepted.time(),
            accepted.arrived(),
        );
        let placement = self.placement(fill.inst, cid.is_some(), &placed);
        let overflow = OmsError::FillOverflow(fill.inst);
        let signed = SignedLots::of(fill.side, fill.qty);
        let inventory = match placement {
            Placement::InSnapshot { .. } => self.inventory(fill.inst),
            Placement::After | Placement::Unsettled => self
                .inventory(fill.inst)
                .checked_add(signed)
                .ok_or(overflow.clone())?,
        };
        let cum = match cid {
            Some(cid) if placement == (Placement::InSnapshot { shown: true }) => {
                Some(self.orders[&cid].cum_fills())
            }
            Some(cid) => Some(self.orders[&cid].cum_fills_with(fill.qty).ok_or(overflow)?),
            None => None,
        };
        accepted.commit();
        self.inventory.insert(fill.inst, inventory);
        self.fill_counted(fill.inst, fill.key(), placed, signed, placement);
        let routed = match (cid, cum) {
            (Some(cid), Some(cum)) => {
                let applied =
                    self.with_record(cid, |rec| rec.apply_fill(fill.vid(), fill.cum_after(), cum));
                // The fill's venue id names this order even when the record knows it by
                // another (an amend's new id, before the Amended update), so a later update
                // naming only that id still reaches it.
                if let Some(v) = fill.vid() {
                    self.by_vid.entry(v.clone()).or_insert(cid);
                }
                FillRouted::Ours(cid, applied)
            }
            _ => FillRouted::OursUntracked(ours),
        };
        Ok(match placement {
            Placement::After => routed,
            Placement::InSnapshot { .. } => FillRouted::InSnapshot(ours),
            Placement::Unsettled => FillRouted::Unsettled(ours),
        })
    }

    /// Applies the outcome of one command item to the order `cid` it was sent for
    /// ([`OrderRecord::on_outcome`]): `item` is the item as the reply names it. Refused when
    /// no order is registered under `cid`, or when the item names another client id.
    pub fn on_outcome(
        &mut self,
        cid: ClientOrderId,
        op: OrderOp,
        item: &ItemRef,
        outcome: &SubmitOutcome,
        now: MonoNs,
    ) -> Result<OutcomeApplied, OmsError> {
        if let Some(named) = item.cid
            && named != cid
        {
            return Err(OmsError::ItemNamesAnother { cid, item: named });
        }
        if !self.orders.contains_key(&cid) {
            return Err(OmsError::UnknownCid(cid));
        }
        let applied = self.with_record(cid, |rec| {
            rec.on_outcome(op, item.vid.as_ref(), outcome, now)
        });
        if ladder::is_lost(self.orders[&cid].state())
            && applied == OutcomeApplied::TombstoneResolved
        {
            self.lost += 1;
        }
        Ok(applied)
    }

    /// Records the nonce the placement of `cid` was sent with (from its encode receipt), for
    /// venues that cancel by it. The same nonce again changes nothing; another is refused.
    pub fn placement_nonce_used(&mut self, cid: ClientOrderId, nonce: u64) -> Result<(), OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        match rec.placement_nonce() {
            Some(known) if known != nonce => Err(OmsError::NonceRecorded(cid)),
            _ => {
                rec.set_placement_nonce(nonce);
                Ok(())
            }
        }
    }

    /// Records the amend of `cid` to `px` and `qty` sent at `now` under `rpc`
    /// ([`OrderRecord::amend_sent`]): false once the order is terminal.
    pub fn amend_sent(
        &mut self,
        cid: ClientOrderId,
        px: Ticks,
        qty: Lots,
        rpc: RpcId,
        now: MonoNs,
    ) -> Result<bool, OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        Ok(rec.amend_sent(px, qty, rpc, now))
    }

    /// Releases the reservation of an amend [`Live::amend`] built and never handed to a
    /// gateway, consuming its command, so it can no longer be submitted: it no longer counts
    /// as resting, and the order may be amended again. False, the command dropped all the same,
    /// when it is not an amend, or not the one its order has built and not yet reported sent.
    /// An amend a gateway took is reported with [`Registry::amend_sent`] instead, under the
    /// request the gateway gave it, and its outcome resolves it, a not-sent one included.
    pub fn amend_not_submitted(&mut self, cmd: PermittedCommand) -> bool {
        cmd.built().is_some_and(|(cid, build)| {
            self.orders
                .get_mut(&cid)
                .is_some_and(|rec| rec.withdraw_built(build))
        })
    }

    /// Records the cancel of `cid` sent at `now` under `rpc` ([`OrderRecord::cancel_sent`]):
    /// false once the order is terminal.
    pub fn cancel_sent(
        &mut self,
        cid: ClientOrderId,
        rpc: RpcId,
        now: MonoNs,
    ) -> Result<bool, OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        Ok(rec.cancel_sent(rpc, now))
    }

    /// The permit to amend our order `cid`: it rests (Open or PartiallyFilled) with no command
    /// in flight, no amend built and not yet reported sent, and no cancel waiting for its
    /// acknowledgement. It holds the registry mutably, so nothing changes the orders the
    /// amend is judged against until it is built.
    pub fn live(&mut self, cid: ClientOrderId) -> Result<Live<'_>, PermitRefusal> {
        let placed = self
            .orders
            .get(&cid)
            .ok_or(PermitRefusal::UnknownCid(cid))?
            .placed();
        let exposure = self.exposure(placed.inst, placed.side, Some(cid));
        let state = self.entries.admits(placed.inst);
        let generation = self.entries.watch(placed.inst);
        let rec = self
            .orders
            .get_mut(&cid)
            .expect("the order was found above");
        Live::check(rec, state, generation, exposure)
    }

    /// The permit to cancel our order `cid`: it is not terminal (PendingNew, Unknown and an
    /// order with a command in flight included).
    pub fn cancellable(&mut self, cid: ClientOrderId) -> Result<Cancellable<'_>, PermitRefusal> {
        let rec = self
            .orders
            .get_mut(&cid)
            .ok_or(PermitRefusal::UnknownCid(cid))?;
        Cancellable::check(rec)
    }

    /// The permit to cancel an order the venue shows (an update, a fill or a snapshot), named
    /// by the client id the venue echoed (`None` when it echoes none) and its venue id. Another
    /// namespace's or a non-canonical client id gets none (decision 0005, I4); nor does an
    /// order the registry does not hold (an orphan, which only I7's resync cancels), nor one
    /// whose client id and venue id name different orders of ours, nor one shown under a venue
    /// id its record has not learnt (the event showing it is applied first). Our registered
    /// client id, or with none echoed a venue id our order had, names the order.
    pub fn cancellable_seen(
        &mut self,
        cid: Option<CidMatch>,
        vid: Option<&VenueOrderId>,
    ) -> Result<Cancellable<'_>, PermitRefusal> {
        let cid = match cid {
            Some(CidMatch::Foreign(ns)) => return Err(PermitRefusal::Foreign(ns)),
            Some(CidMatch::Unparseable) => return Err(PermitRefusal::NotCanonical),
            Some(CidMatch::Ours(cid)) if self.orders.contains_key(&cid) => {
                match vid.and_then(|v| self.cid_of(v)) {
                    Some(by_vid) if by_vid != cid => {
                        return Err(PermitRefusal::Conflicting { cid, by_vid });
                    }
                    None if vid.is_some() => return Err(PermitRefusal::Unlearned(cid)),
                    _ => cid,
                }
            }
            Some(CidMatch::Ours(_)) => return Err(PermitRefusal::Untracked),
            None => vid
                .and_then(|v| self.cid_of(v))
                .ok_or(PermitRefusal::Untracked)?,
        };
        self.cancellable(cid)
    }

    /// Builds the cancels of our orders `cids` (each once, in the order given), each through
    /// its [`Cancellable`] permit: an item whose reference the venue's batch cancel declares
    /// (`OrderCaps::batch_cancel`) goes in a cancel-many of its market, at most `max_items`
    /// apiece; an item with only a reference the batch does not declare goes as a single
    /// cancel (on a venue whose batch cancel takes venue ids only, a client-id cancel carrying
    /// its market); one with no usable reference yet waits for its acknowledgement
    /// ([`CancelChoice::AwaitAck`]); one with no permit is refused.
    pub fn cancel_many(&mut self, cids: &[ClientOrderId], caps: &OrderCaps) -> CancelPlan {
        let batch = caps.batch_cancel.filter(|b| b.max_items > 0);
        let mut plan = CancelPlan::default();
        let mut items = Vec::new();
        let mut singles = Vec::new();
        let mut seen = HashSet::new();
        for &cid in cids {
            if !seen.insert(cid) {
                continue;
            }
            let mut permit = match self.cancellable(cid) {
                Ok(permit) => permit,
                Err(refusal) => {
                    plan.refused.push((cid, refusal));
                    continue;
                }
            };
            if let Some(b) = batch
                && let Some(item) = permit.batch_item(caps, b.refs)
            {
                items.push(item);
                continue;
            }
            match permit.cancel(caps) {
                CancelChoice::Send(cmd) => singles.push(cmd),
                CancelChoice::AwaitAck => plan.awaiting_ack.push(cid),
            }
        }
        if let Some(b) = batch {
            plan.commands = permit::batches(items, b.max_items);
        }
        plan.commands.extend(singles);
        plan
    }

    /// Our orders whose cancel waited for their acknowledgement and can now be built: a
    /// reference the venue's single cancel names is usable. Asked after every event, a
    /// deferred cancel is due the moment the acknowledgement lands; in client id order.
    pub fn cancels_due(&self, caps: &OrderCaps) -> Vec<ClientOrderId> {
        let mut due: Vec<ClientOrderId> = self
            .orders
            .values()
            .filter(|rec| {
                rec.cancel_awaits_ack() && permit::cancel_of(rec, caps, caps.cancel_refs).is_some()
            })
            .map(OrderRecord::cid)
            .collect();
        due.sort();
        due
    }

    /// Runs `change` on the registered order `cid`, then indexes every venue id it has.
    pub(crate) fn with_record<R>(
        &mut self,
        cid: ClientOrderId,
        change: impl FnOnce(&mut OrderRecord) -> R,
    ) -> R {
        let rec = self
            .orders
            .get_mut(&cid)
            .expect("the caller checked the order is registered");
        let out = change(rec);
        for vid in rec.known_vids() {
            self.by_vid.entry(vid.clone()).or_insert(cid);
        }
        out
    }
}
