//! The registry of our orders by client id, the routing of venue events and accepted fills to
//! them, and the inventory those fills moved.

use std::collections::{HashMap, HashSet};
use std::fmt;

use fbc_core::{
    CidMatch, ClientOrderId, InstrumentId, ItemRef, Lots, MonoNs, Namespace, NewOrder, OrderCaps,
    OrderUpdate, RpcId, SignedLots, SubmitOutcome, Ticks, VenueOrderId,
};

use crate::ledger::AcceptedFill;
use crate::permit::{self, CancelChoice, CancelPlan, Cancellable, Live, PermitRefusal};
use crate::record::{Applied, FillApplied, OrderKey, OrderOp, OrderRecord, OutcomeApplied};

/// Our orders, by client id, with an index of every venue id they were known by, and the
/// inventory per instrument that the fills the ledger accepted moved.
#[derive(Debug, Default)]
pub struct Registry {
    orders: HashMap<ClientOrderId, OrderRecord>,
    by_vid: HashMap<VenueOrderId, ClientOrderId>,
    inventory: HashMap<InstrumentId, SignedLots>,
    /// The ledger whose fills the registry applies: the first one it was given.
    ledger: Option<u64>,
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
        }
    }
}

impl std::error::Error for OmsError {}

impl Registry {
    /// An empty registry.
    pub fn new() -> Registry {
        Registry::default()
    }

    /// Registers a placement about to be sent, PendingNew. Refused when an order is already
    /// registered under its client id.
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
    /// non-canonical client id names none of ours.
    pub fn apply_update(&mut self, u: &OrderUpdate, key: OrderKey) -> Routed {
        let cid = match u.cid {
            Some(CidMatch::Foreign(ns)) => return Routed::Foreign(ns),
            Some(CidMatch::Unparseable) => return Routed::NotCanonical,
            Some(CidMatch::Ours(cid)) if self.orders.contains_key(&cid) => cid,
            Some(CidMatch::Ours(_)) | None => match u.vid.as_ref().and_then(|v| self.cid_of(v)) {
                Some(cid) => cid,
                None => return Routed::Untracked,
            },
        };
        let applied = self.with_record(cid, |rec| rec.apply_update(u, key));
        Routed::Ours(cid, applied)
    }

    /// The inventory on `inst` the accepted fills moved: positive is long.
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
    /// flagged. The ledger records
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
            Some(CidMatch::Foreign(ns)) => return Ok(FillRouted::Foreign(ns)),
            Some(CidMatch::Unparseable) => return Ok(FillRouted::NotCanonical),
            Some(CidMatch::Ours(cid)) => match by_vid {
                Some(other) if other != cid => {
                    return Ok(FillRouted::Conflicting { cid, by_vid: other });
                }
                _ if self.orders.contains_key(&cid) => (Some(cid), cid),
                _ => (None, cid),
            },
            None => match by_vid {
                Some(cid) => (Some(cid), cid),
                None => return Ok(FillRouted::Unattributed),
            },
        };
        if let Some(cid) = cid {
            let placed = self.orders[&cid].placed();
            if placed.inst != fill.inst || placed.side != fill.side {
                return Ok(FillRouted::Disagrees(cid));
            }
        }
        let overflow = OmsError::FillOverflow(fill.inst);
        let inventory = self
            .inventory(fill.inst)
            .checked_add(SignedLots::of(fill.side, fill.qty))
            .ok_or(overflow.clone())?;
        let cum = match cid {
            Some(cid) => Some(self.orders[&cid].cum_fills_with(fill.qty).ok_or(overflow)?),
            None => None,
        };
        accepted.commit();
        self.inventory.insert(fill.inst, inventory);
        Ok(match (cid, cum) {
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
        Ok(self.with_record(cid, |rec| {
            rec.on_outcome(op, item.vid.as_ref(), outcome, now)
        }))
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
    /// in flight and no cancel waiting for its acknowledgement.
    pub fn live(&self, cid: ClientOrderId) -> Result<Live<'_>, PermitRefusal> {
        let rec = self
            .orders
            .get(&cid)
            .ok_or(PermitRefusal::UnknownCid(cid))?;
        Live::check(rec)
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
    fn with_record<R>(
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
