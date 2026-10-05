//! The registry of our orders by client id, and the routing of venue events to them.

use std::collections::HashMap;
use std::fmt;

use fbc_core::{
    CidMatch, ClientOrderId, ItemRef, MonoNs, Namespace, NewOrder, OrderUpdate, SubmitOutcome,
    VenueOrderId,
};

use crate::record::{Applied, OrderKey, OrderOp, OrderRecord, OutcomeApplied};

/// Our orders, by client id, with an index of every venue id they were known by.
#[derive(Debug, Default)]
pub struct Registry {
    orders: HashMap<ClientOrderId, OrderRecord>,
    by_vid: HashMap<VenueOrderId, ClientOrderId>,
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
        for vid in rec.vid().into_iter().chain(rec.superseded_vids()) {
            self.by_vid.entry(vid.clone()).or_insert(cid);
        }
        out
    }
}
