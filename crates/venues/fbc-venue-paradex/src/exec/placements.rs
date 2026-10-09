//! The final phase of Paradex's two-phase placements (decisions 0054, 0069, 0085, 0091). The
//! reply to `order.create`, and to each created `order.create_batch` item, accepts the order
//! provisionally: Paradex queues it for its risk check. The order's own `OrderEvent`s settle it,
//! and [`Placements`] turns what they show ([`Phase`]) into the placement's final phase:
//!
//! - **Passed** (the order OPEN, or anything of it filled): `ExecEvent::Outcome` of the
//!   placement's request and item, `Accepted` at `AckLevel::Final`, pushed before the order
//!   update that showed it, under that event's `VenueMeta`.
//! - **Refused** (CLOSED with nothing filled, for a reason other than our cancel): an
//!   `ExecEvent::AsyncReject` of the placement (`OpKind::Place`) naming the order by our client
//!   id and its venue id, pushed before the order update. Its kind is
//!   `RejectKind::PostOnlyWouldCross` for `POST_ONLY_WOULD_CROSS` and `RejectKind::Other` for
//!   any other reason; the reason is its `raw`, and it carries no `venue_code`: Paradex states
//!   a cancel reason, not one of the codes `REJECT_CODES` keys on (0069).
//! - **Withdrawn** (CLOSED with nothing filled by USER_CANCELED) and **Pending** (NEW): neither.
//!   A withdrawn placement awaits nothing more; a pending one waits for a later event.
//!
//! An event can come before the reply. What the first such event showed (not Pending) is held
//! for the placement until its reply: a passed placement's reply is then its final acceptance
//! at once, a refused one's its provisional acceptance followed by the asynchronous reject, and
//! a withdrawn one's its provisional acceptance alone.
//!
//! A placement awaits its final phase from the reply that accepts it until an event settles
//! it, and no longer than its connection: a new connection drops every placement held, sent or
//! accepted (the resync reads what rests then, 0027), as do a reply or timeout reporting it
//! anything but accepted. Each placement is held by our client id, so at most one per order
//! sent and unsettled; the venue's own orders and those of other engines are never held.

use std::collections::HashMap;

use fbc_core::{
    AckLevel, CidMatch, ClientOrderId, ExecEvent, ExecSink, ItemRef, OpKind, OrderRef, Reject,
    RejectKind, RpcId, SubmitOutcome, VenueCommand, VenueMeta,
};

use super::order::Phase;

/// What an event showed of a placement before its reply came.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Early {
    Passed,
    Refused(Box<str>),
    Withdrawn,
}

/// The placements awaiting their final phase (module documentation). Its `Debug` shows how
/// many it holds.
#[derive(Default)]
pub struct Placements {
    /// Placements sent and not yet answered, by our client id: their request, and what an
    /// event showed of them first.
    sent: HashMap<ClientOrderId, (RpcId, Option<Early>)>,
    /// Placements accepted provisionally, by our client id: their request and item.
    accepted: HashMap<ClientOrderId, (RpcId, ItemRef)>,
}

impl std::fmt::Debug for Placements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Placements")
            .field("sent", &self.sent.len())
            .field("accepted", &self.accepted.len())
            .finish()
    }
}

/// Events held to be pushed once a call has read them all.
#[derive(Default)]
pub(super) struct Held(pub(super) Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Held {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

impl Placements {
    /// Request `rpc`, `cmd`, was sent: each order it places awaits its reply.
    pub(super) fn sent(&mut self, rpc: RpcId, cmd: &VenueCommand) {
        let orders = match cmd {
            VenueCommand::Place(order) => std::slice::from_ref(order),
            VenueCommand::PlaceBatch(orders) => orders.as_slice(),
            _ => return,
        };
        for order in orders {
            self.sent.insert(order.cid, (rpc, None));
        }
    }

    /// The events a reply decoded into, pushed to `sink` with each placement's final phase
    /// where an earlier event settled it.
    pub(super) fn on_reply(&mut self, held: Held, sink: &mut dyn ExecSink) {
        for (meta, ev) in held.0 {
            let ExecEvent::Outcome { rpc, item, outcome } = &ev else {
                sink.push(meta, ev);
                continue;
            };
            let Some(item) = item else {
                // The whole request: no item of it is accepted.
                self.sent.retain(|_, (sent, _)| sent != rpc);
                sink.push(meta, ev);
                continue;
            };
            let Some((cid, early)) = item
                .cid
                .and_then(|cid| Some((cid, self.take_sent(cid, *rpc)?)))
            else {
                sink.push(meta, ev);
                continue;
            };
            let provisional = SubmitOutcome::Accepted {
                ack: AckLevel::Provisional,
            };
            if *outcome != provisional {
                sink.push(meta, ev);
                continue;
            }
            let (rpc, item) = (*rpc, item.clone());
            match early {
                Some(Early::Passed) => sink.push(meta, fin(rpc, item)),
                Some(Early::Refused(reason)) => {
                    sink.push(meta, ev);
                    sink.push(meta, refused(cid, &item, &reason));
                }
                Some(Early::Withdrawn) => sink.push(meta, ev),
                None => {
                    self.accepted.insert(cid, (rpc, item));
                    sink.push(meta, ev);
                }
            }
        }
    }

    /// The events an order-entry frame decoded into, `phase` what it showed when it is an
    /// `OrderEvent`, pushed to `sink`: each order update preceded by its placement's final
    /// phase when it settles one.
    pub(super) fn on_event(&mut self, phase: Option<Phase>, held: Held, sink: &mut dyn ExecSink) {
        for (meta, ev) in held.0 {
            if let (Some(phase), ExecEvent::Order(update)) = (&phase, &ev)
                && let Some(CidMatch::Ours(cid)) = update.cid
            {
                self.settle(cid, phase, meta, sink);
            }
            sink.push(meta, ev);
        }
    }

    /// Request `rpc` timed out: whatever of it was unanswered is `Unknown`, never accepted.
    pub(super) fn on_rpc_timeout(&mut self, rpc: RpcId) {
        self.sent.retain(|_, (sent, _)| *sent != rpc);
    }

    /// A new connection opened: no placement awaits its final phase on it (module
    /// documentation).
    pub(super) fn on_new_connection(&mut self) {
        self.sent.clear();
        self.accepted.clear();
    }

    /// The placement of `cid` sent as `rpc`, taken from those awaiting a reply: what an event
    /// showed of it first. `None` when no such placement is held.
    fn take_sent(&mut self, cid: ClientOrderId, rpc: RpcId) -> Option<Option<Early>> {
        match self.sent.get(&cid) {
            Some((sent, _)) if *sent == rpc => self.sent.remove(&cid).map(|(_, early)| early),
            _ => None,
        }
    }

    /// What an event of our order `cid` showing `phase` settles, pushed under `meta`.
    fn settle(
        &mut self,
        cid: ClientOrderId,
        phase: &Phase,
        meta: VenueMeta,
        sink: &mut dyn ExecSink,
    ) {
        if *phase == Phase::Pending {
            return;
        }
        if let Some((rpc, item)) = self.accepted.remove(&cid) {
            match phase {
                Phase::Passed => sink.push(meta, fin(rpc, item)),
                Phase::Refused(reason) => sink.push(meta, refused(cid, &item, reason)),
                Phase::Withdrawn | Phase::Pending => {}
            }
            return;
        }
        if let Some((_, early @ None)) = self.sent.get_mut(&cid) {
            *early = match phase {
                Phase::Passed => Some(Early::Passed),
                Phase::Refused(reason) => Some(Early::Refused(reason.clone())),
                Phase::Withdrawn => Some(Early::Withdrawn),
                Phase::Pending => None,
            };
        }
    }
}

/// The final acceptance of request `rpc`'s `item`.
fn fin(rpc: RpcId, item: ItemRef) -> ExecEvent {
    ExecEvent::Outcome {
        rpc,
        item: Some(item),
        outcome: SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
    }
}

/// The asynchronous reject of the placement of our order `cid`, `item`, for `reason` (module
/// documentation).
fn refused(cid: ClientOrderId, item: &ItemRef, reason: &str) -> ExecEvent {
    let target = match &item.vid {
        Some(vid) => OrderRef::Both(cid, vid.clone()),
        None => OrderRef::Client(cid),
    };
    let kind = match reason {
        "POST_ONLY_WOULD_CROSS" => RejectKind::PostOnlyWouldCross,
        _ => RejectKind::Other,
    };
    let reject = Reject {
        kind,
        venue_code: None,
        raw: reason.into(),
    };
    ExecEvent::AsyncReject {
        target,
        op: OpKind::Place,
        reject,
    }
}
