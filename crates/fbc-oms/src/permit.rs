//! Permits (decision 0005): the type-state an amend or a cancel is built through, every field
//! resolved from the order's record, so a codec holds no order registry.
//!
//! A [`Live`] permit is held for an order that rests with nothing in flight (Open or
//! PartiallyFilled, no pending intent, no cancel waiting for its acknowledgement); only it
//! builds an amend. A [`Cancellable`] permit is held for any order that is not terminal
//! (PendingNew, Unknown and orders with a command in flight included); only it builds a cancel.
//! Both come only from the [`Registry`](crate::Registry), which holds our orders alone, so a
//! foreign-namespace or non-canonical order never gets one (0005's I4): it is never cancelled
//! one by one. Neither can be built outside this crate (`tests/ui_permits/`).
//!
//! What a permit builds is a [`PermittedCommand`]: a venue command only this crate makes, which
//! is what an amend, a cancel or a cancel-many is authorized from (decision 0045). It records
//! the registry that built it, and an amend's build token names that registry with its order
//! and build number, so no other registry authorizes it or releases its reservation (decision
//! 0082). A place's or a batch's command holds its orders: dropped unauthorized and not
//! withdrawn, it hands them back to its registry, which ends each not sent ([`DroppedPlaces`];
//! decision 0084), so no held place is stranded.
//!
//! A cancel names its order by the reference design §4.9 orders, as the venue's
//! [`OrderCaps`] declare them (0032): the venue id when it is known and a cancel can name it;
//! else the client id when a cancel can name it and the venue acknowledged the order, or
//! accepts a cancel before the acknowledgement (`cancel_before_ack`); else the placement nonce
//! when a cancel can name it and the order has one; else the cancel waits for the
//! acknowledgement ([`CancelChoice::AwaitAck`]) and is due again the moment it lands
//! ([`Registry::cancels_due`](crate::Registry::cancels_due)). The choice is fbc-core's
//! [`CancelOrder::reference`], the one a codec makes from the same command, so the OMS and the
//! codec agree on what is sent; a command always carries our client id, so on a venue that
//! declares both client-id and nonce cancels, an order not acknowledged and without
//! `cancel_before_ack` waits for its acknowledgement rather than be cancelled by its nonce,
//! which the codec would not pick (FBC-03fi).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use fbc_core::{
    AmendOrder, AmendQty, CancelOrder, ChosenRef, ClientOrderId, InstrumentId, Lots, Namespace,
    OrderCaps, OrderKind, OrderRef, TagSet, Ticks, VenueCommand,
};

use crate::caps::{Adds, CapRefusal, Exposure};
use crate::entry::{Admits, StateRefusal};
use crate::grant::Guard;
use crate::record::{Intent, OrdState, OrderRecord};
use crate::registry::Instance;

/// Why the registry gives no permit for an order.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum PermitRefusal {
    /// The registry holds no order under this client id.
    UnknownCid(ClientOrderId),
    /// The order is terminal: nothing amends or cancels it.
    Terminal(ClientOrderId),
    /// Not resting (PendingNew or Unknown): an amend needs an order the venue holds.
    NotResting(ClientOrderId, OrdState),
    /// A command is in flight on the order, or its cancel waits for the acknowledgement: an
    /// amend waits until it resolves.
    IntentPending(ClientOrderId),
    /// Another namespace's order (another engine on the account): never cancelled one by one
    /// (decision 0005, I4).
    Foreign(Namespace),
    /// An order under a non-canonical client id (another system's): never cancelled one by
    /// one (decision 0005, I4).
    NotCanonical,
    /// An order the venue shows that the registry does not hold, under our namespace's client
    /// id or none: an orphan, which only 0005's I7 cancels, by resync (not modelled here).
    Untracked,
    /// The venue shows our client id `cid` with a venue id our order `by_vid` had: they name
    /// different orders, so neither is cancelled on its word.
    Conflicting {
        cid: ClientOrderId,
        by_vid: ClientOrderId,
    },
    /// The venue shows our order `cid` under a venue id its record has not learnt: the record
    /// would name it by another id, or wait for one. The event showing it is applied first
    /// ([`Registry::apply_update`](crate::Registry::apply_update)), and the record learns it.
    Unlearned(ClientOrderId),
    /// The order is on the Unknown ladder: what the venue holds of it is not known, so it is
    /// never amended until the ladder resolves it (decision 0005, I5; [`Registry::ladder`](crate::Registry::ladder)).
    OnLadder(ClientOrderId),
    /// The order was registered from a resync's snapshot (an earlier run's): its placement's
    /// time in force and channel are not known, so it is never amended, only cancelled
    /// (decision 0055).
    FromSnapshot(ClientOrderId),
}

/// Why a [`Live`] permit builds no amend.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AmendRefusal {
    /// The venue cannot amend orders (`OrderCaps::amend` is `None`).
    NotAmendable,
    /// The order is a market order: an amend targets a resting limit order.
    NotLimit,
    /// The order is partly filled and the venue cannot amend a partly filled order.
    PartiallyFilled,
    /// The price would change and the venue cannot amend a price.
    PriceNotAmendable,
    /// The total would change and the venue cannot amend a quantity.
    QtyNotAmendable,
    /// The new total is at or below the filled quantity: nothing would rest, which is a
    /// cancel, not an amend.
    NothingToRest,
    /// The order carries no reference the venue's amend can name
    /// ([`AmendCaps::refs`](fbc_core::AmendCaps::refs)).
    NoDeclaredReference,
    /// The market's state refused it before either cap was consulted (decision 0012): Killed,
    /// Cancel-only, or Exit, which amends only exit orders ([`ExitRefusal`](crate::ExitRefusal)).
    State(StateRefusal),
    /// A pre-trade cap refused it (0013 rule 2): the amend would take the worst case on the
    /// order's side past the inventory cap, or what the side has resting past the resting
    /// cap, or its market has no caps configured.
    Capped(CapRefusal),
}

/// A venue command this crate built: an amend, a cancel or a cancel-many of one market from a
/// permit, every field from the order's record, a place or a batch of places the
/// pre-trade caps admitted ([`Registry::place`], [`Registry::place_batch`]), or an instrument
/// cancel-all built under 0005's I7 guard
/// ([`Registry::cancel_everything`](crate::Registry::cancel_everything)). Only this crate
/// builds one; it can be read, not edited, and an authorization for an order-affecting
/// command is issued only from one (0045), only by the registry that built it, which judges a
/// place, a batch or an amend against its caps again then (decision 0082). It has no `Clone`:
/// each one is authorized once.
///
/// A place's or a batch's command holds its orders until an authorization is issued from it or
/// it is withdrawn ([`Registry::place_not_submitted`]). Dropped otherwise, wherever it was
/// dropped (an early return, `let _ =`, passed to [`Registry::amend_not_submitted`], refused by
/// another registry, on another thread), it hands its orders back to the registry that built
/// it, which ends each still held not sent
/// ([`NotSentReason::StaleAuthorization`](fbc_core::NotSentReason::StaleAuthorization)) at
/// its next mutating call, freeing what it counted (decision 0084).
#[derive(Eq, PartialEq, Debug)]
pub struct PermittedCommand {
    cmd: VenueCommand,
    /// The registry that built it: only it authorizes the command or releases what the command
    /// reserved (decision 0082).
    origin: Instance,
    /// For an amend, its order and the build's number, by which, with `origin`, alone its
    /// reservation is released ([`Registry::amend_not_submitted`]).
    built: Option<(ClientOrderId, u64)>,
    /// What its authorization is checked against at submit: the market's state generation at
    /// build for a place, a batch or an amend, with its position revision when built in Exit
    /// (decision 0066), 0005's I7 inputs (the exclusive lease's changes
    /// and the foreign orders seen) for an instrument cancel-all (decision 0060); nothing for a
    /// cancel or a cancel-many.
    guard: Guard,
    /// For a place or a batch, its hold on its orders, handed back to its registry when the
    /// command is dropped unauthorized and not withdrawn; nothing for any other command.
    hold: Hold,
}

impl PermittedCommand {
    /// The command, read only.
    pub fn command(&self) -> &VenueCommand {
        &self.cmd
    }

    /// The command and its guard, for the authorization issued from it, which spends its
    /// hold on its orders.
    pub(crate) fn into_parts(self) -> (VenueCommand, Guard) {
        let PermittedCommand {
            cmd,
            guard,
            mut hold,
            ..
        } = self;
        hold.spend();
        (cmd, guard)
    }

    /// Spends its hold on its orders: its registry withdrew them itself.
    pub(crate) fn spend_hold(&mut self) {
        self.hold.spend();
    }

    /// A cancel or a cancel-many a [`Cancellable`] permit of the registry `origin` built,
    /// which nothing holds back at submit.
    pub(crate) fn cancel(cmd: VenueCommand, origin: Instance) -> PermittedCommand {
        debug_assert!(matches!(
            cmd,
            VenueCommand::Cancel(_) | VenueCommand::CancelMany(_)
        ));
        PermittedCommand {
            cmd,
            origin,
            built: None,
            guard: Guard::default(),
            hold: Hold::default(),
        }
    }

    /// A place or a batch of places the market's state and the pre-trade caps of the registry
    /// `origin` admitted, or an instrument cancel-all it built under 0005's I7 guard, checked
    /// at submit against `guard`.
    pub(crate) fn guarded(cmd: VenueCommand, guard: Guard, origin: Instance) -> PermittedCommand {
        PermittedCommand {
            cmd,
            origin,
            built: None,
            guard,
            hold: Hold::default(),
        }
    }

    /// A place or a batch of places the registry `origin` admitted, as [`Self::guarded`],
    /// holding its orders: dropped unauthorized and not withdrawn, it hands them to `dropped`,
    /// the registry's.
    pub(crate) fn holding(
        cmd: VenueCommand,
        guard: Guard,
        origin: Instance,
        dropped: &Arc<DroppedPlaces>,
    ) -> PermittedCommand {
        let cids = places_of(&cmd);
        PermittedCommand {
            hold: Hold(Some(Box::new((Arc::clone(dropped), cids)))),
            ..PermittedCommand::guarded(cmd, guard, origin)
        }
    }

    /// The registry that built it.
    pub(crate) fn origin(&self) -> Instance {
        self.origin
    }

    /// For an amend, its order and the build's number.
    pub(crate) fn built(&self) -> Option<(ClientOrderId, u64)> {
        self.built
    }
}

/// The client ids of the orders a place or a batch of places opens; none for any other command.
pub(crate) fn places_of(cmd: &VenueCommand) -> Vec<ClientOrderId> {
    match cmd {
        VenueCommand::Place(order) => vec![order.cid],
        VenueCommand::PlaceBatch(orders) => orders.iter().map(|o| o.cid).collect(),
        _ => Vec::new(),
    }
}

/// The orders of a registry whose place or batch command was dropped while it still held
/// them, neither authorized nor withdrawn (decision 0084): the registry ends each still held not
/// sent at its next mutating call. Shared by the
/// registry with the commands it builds, which may be dropped on any thread.
#[derive(Debug, Default)]
pub(crate) struct DroppedPlaces {
    /// Whether `cids` may hold any: read without the lock on every mutating call.
    any: AtomicBool,
    cids: Mutex<Vec<ClientOrderId>>,
}

impl DroppedPlaces {
    fn hand_back(&self, cids: Vec<ClientOrderId>) {
        let mut held = self.cids.lock().unwrap_or_else(PoisonError::into_inner);
        held.extend(cids);
        self.any.store(true, Ordering::Release);
    }

    /// The orders handed back since the last call, in the order handed back.
    pub(crate) fn take(&self) -> Vec<ClientOrderId> {
        if !self.any.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut held = self.cids.lock().unwrap_or_else(PoisonError::into_inner);
        self.any.store(false, Ordering::Release);
        std::mem::take(&mut *held)
    }
}

/// A place's or a batch's hold on its orders: dropped unspent, it hands them back to its
/// registry's [`DroppedPlaces`]. It is not part of what the command is: commands compare
/// equal whatever their holds. Boxed, so it adds one pointer to every command.
#[derive(Debug, Default)]
struct Hold(Option<Box<(Arc<DroppedPlaces>, Vec<ClientOrderId>)>>);

impl Hold {
    /// The authorization issued from the command, or its withdrawal, took its orders.
    fn spend(&mut self) {
        self.0 = None;
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(held) = self.0.take() {
            let (dropped, cids) = *held;
            dropped.hand_back(cids);
        }
    }
}

impl PartialEq for Hold {
    fn eq(&self, _: &Hold) -> bool {
        true
    }
}

impl Eq for Hold {}

/// What a [`Cancellable`] permit built.
#[derive(Eq, PartialEq, Debug)]
pub enum CancelChoice {
    /// A cancel naming the order by the reference chosen.
    Send(PermittedCommand),
    /// No reference the venue's cancel can name is usable yet: the cancel waits for the
    /// order's acknowledgement, and is due again the moment it lands
    /// ([`Registry::cancels_due`](crate::Registry::cancels_due)).
    AwaitAck,
}

/// What [`Registry::cancel_many`](crate::Registry::cancel_many) built.
#[derive(Eq, PartialEq, Debug, Default)]
pub struct CancelPlan {
    /// The commands, in order: a cancel-many per market and per `max_items` of the items
    /// whose reference the venue's batch cancel declares, then a single cancel for each item
    /// that has only a reference the batch does not declare.
    pub commands: Vec<PermittedCommand>,
    /// The orders whose cancel waits for their acknowledgement.
    pub awaiting_ack: Vec<ClientOrderId>,
    /// The orders no cancel was built for, and why.
    pub refused: Vec<(ClientOrderId, PermitRefusal)>,
}

/// What [`Registry::place_batch`] built.
#[derive(Eq, PartialEq, Debug, Default)]
pub struct PlacePlan {
    /// The batch of the items admitted, in the order given; `None` when none was.
    pub command: Option<PermittedCommand>,
    /// The items refused, never built, and why.
    pub refused: Vec<(ClientOrderId, crate::OmsError)>,
}

/// The permit to amend an order: it rests (Open or PartiallyFilled) with nothing in flight.
/// Only the [`Registry`](crate::Registry) gives one ([`Registry::live`](crate::Registry::live)),
/// holding it mutably, with what the order's side holds besides the order, so the amend is
/// judged against the pre-trade caps with every other order as the registry has it.
#[derive(Debug)]
pub struct Live<'r> {
    rec: &'r mut OrderRecord,
    /// The registry that gave the permit, which the amend it builds records.
    origin: Instance,
    /// What the market's state admits, read when the permit was given: nothing changes it
    /// while the permit holds the registry.
    state: Result<Admits, StateRefusal>,
    /// The market's state generation then, and in Exit its position revision, which the
    /// amend's authorization is checked against at submit (decisions 0060, 0066).
    guard: Guard,
    exposure: Exposure,
}

impl<'r> Live<'r> {
    pub(crate) fn check(
        rec: &'r mut OrderRecord,
        origin: Instance,
        state: Result<Admits, StateRefusal>,
        guard: Guard,
        exposure: Exposure,
    ) -> Result<Live<'r>, PermitRefusal> {
        let cid = rec.cid();
        match rec.state() {
            OrdState::Terminal(_) => return Err(PermitRefusal::Terminal(cid)),
            state @ (OrdState::PendingNew | OrdState::Unknown) => {
                return Err(PermitRefusal::NotResting(cid, state));
            }
            OrdState::Open | OrdState::PartiallyFilled => {}
        }
        if rec.intent() != Intent::None || rec.cancel_awaits_ack() || rec.amend_built().is_some() {
            return Err(PermitRefusal::IntentPending(cid));
        }
        if rec.unknown_since().is_some() {
            return Err(PermitRefusal::OnLadder(cid));
        }
        if rec.from_snapshot() {
            return Err(PermitRefusal::FromSnapshot(cid));
        }
        Ok(Live {
            rec,
            origin,
            state,
            guard,
            exposure,
        })
    }

    /// The order the permit is for.
    pub fn order(&self) -> &OrderRecord {
        self.rec
    }

    /// Builds the amend of the order to `px` and the total `qty` (filled part included),
    /// every other field from its record: refused where the venue's caps cannot amend it
    /// (no amend at all, a market order, a partly filled order where the venue cannot amend
    /// one, a price or total the venue cannot change, nothing left to rest, or no reference
    /// the amend can name). It names the order without a venue id an earlier amend, not yet
    /// confirmed, may have replaced (on a venue whose amend gives a new id). The amend carries the order's filled quantity
    /// ([`OrderRecord::filled`]) as `cum_filled` (0014 item 5), the venue's reduce-only flag
    /// of the placement, and `reducing`, the caller's classification of the amended order as
    /// one that can only reduce the position, as on [`NewOrder::reducing`](fbc_core::NewOrder):
    /// it chooses the traffic class only and exempts the amend from no check (0013 rule 2).
    ///
    /// Refused, never built, before anything else is judged, when the market's state builds no
    /// amend (Killed or Cancel-only; decision 0012, [`AmendRefusal::State`]), whatever
    /// `reducing` says. In Exit, refused before either cap unless the amended order is an exit
    /// order: reduce-only as placed or `reducing` here, on the side that reduces the position,
    /// the side's orders, this one at what it may fill once amended, within the position's size
    /// ([`ExitRefusal`](crate::ExitRefusal)).
    ///
    /// Refused, never built, when the amend would take the worst case on the order's side past
    /// its market's inventory cap (0005's I6), or what the side has resting past its resting
    /// cap (0052), the order counted at the larger of its resting quantity now and the
    /// amend's, as it is while the amend is in flight, or when its market has no caps
    /// configured ([`AmendRefusal::Capped`]). A replace (an amend on a venue whose amend gives
    /// the order a new id) is checked the same way. On a venue whose amend states the
    /// remaining quantity ([`AmendQty::Remaining`]), the amend's wire carries `qty` less the
    /// filled quantity, which the venue rests whole on top of any fills it takes before the
    /// amend applies: the order is counted, for the inventory cap, at what it may add now
    /// plus that wire quantity, and for the resting cap at the larger of its resting quantity
    /// now and that wire quantity ([`OrderRecord::exposure`], [`OrderRecord::resting`];
    /// FBC-w5n, decision 0064). An amend built counts at once
    /// ([`OrderRecord::amend_built`]), so a check after it, of this order or another, sees
    /// it; it becomes the amend in flight when it is reported sent
    /// ([`Registry::amend_sent`](crate::Registry::amend_sent)), and an order whose amend was
    /// built and never sent gets no further Live permit: it can still be cancelled.
    pub fn amend(
        self,
        caps: &OrderCaps,
        px: Ticks,
        qty: Lots,
        reducing: bool,
    ) -> Result<PermittedCommand, AmendRefusal> {
        let admits = self.state.map_err(AmendRefusal::State)?;
        let rec = &*self.rec;
        let placed = rec.placed();
        let (amend, wire) = amend_shape(rec, caps, px, qty, reducing)?;
        let exposure = rec.exposure_if_amended(qty, wire);
        admits
            .judge(&self.exposure, reducing || placed.reduce_only, exposure)
            .map_err(AmendRefusal::State)?;
        self.exposure
            .admit(Adds {
                exposure,
                resting: rec.resting_if_amended(qty, wire),
            })
            .map_err(AmendRefusal::Capped)?;
        let cid = rec.cid();
        let build = self.rec.set_amend_built(qty, wire);
        Ok(PermittedCommand {
            cmd: VenueCommand::Amend(amend),
            origin: self.origin,
            built: Some((cid, build)),
            guard: self.guard,
            hold: Hold::default(),
        })
    }
}

/// The amend of `rec` to `px` and the total `qty` the venue's caps admit, with the quantity its
/// wire carries on a venue whose amend states the remaining quantity: everything
/// [`Live::amend`] judges before the market's state and the caps, which it changes nothing to
/// judge, so the planner asks it before choosing an amend over a cancel and replace.
pub(crate) fn amend_shape(
    rec: &OrderRecord,
    caps: &OrderCaps,
    px: Ticks,
    qty: Lots,
    reducing: bool,
) -> Result<(AmendOrder, Option<Lots>), AmendRefusal> {
    let amend_caps = caps.amend.as_ref().ok_or(AmendRefusal::NotAmendable)?;
    let placed = rec.placed();
    if placed.kind == OrderKind::Market {
        return Err(AmendRefusal::NotLimit);
    }
    let filled = rec.filled();
    if filled > Lots::ZERO && !amend_caps.when_partially_filled {
        return Err(AmendRefusal::PartiallyFilled);
    }
    if rec.px() != Some(px) && !amend_caps.price {
        return Err(AmendRefusal::PriceNotAmendable);
    }
    if qty != rec.qty() && !amend_caps.qty {
        return Err(AmendRefusal::QtyNotAmendable);
    }
    if qty <= filled {
        return Err(AmendRefusal::NothingToRest);
    }
    let amend = AmendOrder {
        target: rec.order_ref(caps),
        inst: placed.inst,
        side: placed.side,
        tif: placed.tif,
        channel: placed.channel,
        post_only: placed.post_only,
        reduce_only: placed.reduce_only,
        reducing,
        px,
        qty,
        cum_filled: filled,
    };
    if amend.reference(amend_caps).is_none() {
        return Err(AmendRefusal::NoDeclaredReference);
    }
    // On a venue whose amend states the remaining quantity, what its wire carries (FBC-w5n).
    let wire = match amend_caps.qty_semantics {
        AmendQty::Remaining => Some(
            amend
                .wire_qty(AmendQty::Remaining)
                .ok_or(AmendRefusal::NothingToRest)?,
        ),
        AmendQty::TotalIncludingFilled => None,
    };
    Ok((amend, wire))
}

/// The permit to cancel an order: it is not terminal. Only the
/// [`Registry`](crate::Registry) gives one, and never for a foreign-namespace or
/// non-canonical order (I4).
#[derive(Debug)]
pub struct Cancellable<'r> {
    rec: &'r mut OrderRecord,
    /// The registry that gave the permit, which the cancel it builds records.
    origin: Instance,
}

impl<'r> Cancellable<'r> {
    pub(crate) fn check(
        rec: &'r mut OrderRecord,
        origin: Instance,
    ) -> Result<Cancellable<'r>, PermitRefusal> {
        if rec.state().is_terminal() {
            return Err(PermitRefusal::Terminal(rec.cid()));
        }
        Ok(Cancellable { rec, origin })
    }

    /// The order the permit is for.
    pub fn order(&self) -> &OrderRecord {
        self.rec
    }

    /// Builds the cancel of the order, naming it by the reference the module documentation
    /// gives, from the venue's single-cancel references (`cancel_refs`). When none is usable
    /// yet the cancel waits for the order's acknowledgement and the order is marked so
    /// ([`OrderRecord::cancel_awaits_ack`]); a cancel built clears the mark.
    pub fn cancel(self, caps: &OrderCaps) -> CancelChoice {
        match cancel_of(self.rec, caps, caps.cancel_refs) {
            Some(cancel) => {
                self.rec.set_cancel_awaits_ack(false);
                CancelChoice::Send(PermittedCommand::cancel(
                    VenueCommand::Cancel(cancel),
                    self.origin,
                ))
            }
            None => {
                self.rec.set_cancel_awaits_ack(true);
                CancelChoice::AwaitAck
            }
        }
    }

    /// The order's cancel as an item of a batch cancel, when the batch declares a usable
    /// reference for it (clearing any wait for the acknowledgement); `None` otherwise, and the
    /// order is left as it was.
    pub(crate) fn batch_item(
        &mut self,
        caps: &OrderCaps,
        refs: TagSet<fbc_core::RefKind>,
    ) -> Option<CancelOrder> {
        let cancel = cancel_of(self.rec, caps, refs)?;
        self.rec.set_cancel_awaits_ack(false);
        Some(cancel)
    }
}

/// The cancel of `rec`, naming it by the first of `declared` the command carries, as a codec
/// chooses it ([`CancelOrder::reference`]); `None` when it carries none of them, or only the
/// client id while the order is not acknowledged and the venue takes no cancel before the
/// acknowledgement. The command carries the record's venue id unless an amend not yet
/// confirmed may have replaced it, on a venue whose amend does not keep the id.
pub(crate) fn cancel_of(
    rec: &OrderRecord,
    caps: &OrderCaps,
    declared: TagSet<fbc_core::RefKind>,
) -> Option<CancelOrder> {
    let placed = rec.placed();
    let cancel = CancelOrder {
        target: rec.order_ref(caps),
        inst: placed.inst,
        side: placed.side,
        placement_nonce: rec.placement_nonce(),
    };
    let usable = match cancel.reference(declared)? {
        ChosenRef::Venue(_) | ChosenRef::PlacementNonce(_) => true,
        ChosenRef::Client(_) => rec.acknowledged() || caps.cancel_before_ack,
    };
    usable.then_some(cancel)
}

/// The Unknown ladder's tombstone cancel of `rec`, naming it by client id alone, whether or
/// not the venue acknowledged it (design §4.9), built by the registry `origin`; `None` when
/// the venue's single cancel cannot name a client id.
pub(crate) fn tombstone_of(
    rec: &OrderRecord,
    caps: &OrderCaps,
    origin: Instance,
) -> Option<PermittedCommand> {
    if !crate::ladder::cancels_by_client(caps) {
        return None;
    }
    let placed = rec.placed();
    Some(PermittedCommand::cancel(
        VenueCommand::Cancel(CancelOrder {
            target: OrderRef::Client(rec.cid()),
            inst: placed.inst,
            side: placed.side,
            placement_nonce: None,
        }),
        origin,
    ))
}

/// Splits `items` into cancel-many commands of one market each, at most `max_items` (above 0)
/// items apiece, markets in id order and items in the order given, built by the registry
/// `origin`.
pub(crate) fn batches(
    items: Vec<CancelOrder>,
    max_items: u16,
    origin: Instance,
) -> Vec<PermittedCommand> {
    let mut by_market: BTreeMap<InstrumentId, Vec<CancelOrder>> = BTreeMap::new();
    for item in items {
        by_market.entry(item.inst).or_default().push(item);
    }
    let size = usize::from(max_items);
    by_market
        .into_values()
        .flat_map(|items| {
            items
                .chunks(size)
                .map(|chunk| {
                    PermittedCommand::cancel(VenueCommand::CancelMany(chunk.to_vec()), origin)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

impl OrderRecord {
    /// The reference that names the order in a command: our client id, with the venue's id
    /// once known, unless, on a venue whose amend gives the order a new id, an amend not yet
    /// confirmed may have replaced it ([`OrderRecord::amend_unconfirmed`]) or one confirmed
    /// without naming the new id did ([`OrderRecord::vid_retired`]).
    pub(crate) fn order_ref(&self, caps: &OrderCaps) -> OrderRef {
        match self.vid() {
            Some(vid) if !self.id_may_have_moved(caps) => OrderRef::Both(self.cid(), vid.clone()),
            _ => OrderRef::Client(self.cid()),
        }
    }
}

#[cfg(test)]
impl PermittedCommand {
    /// A permitted command from any command, guarded by `guard`, for the authorization's own
    /// tests.
    pub(crate) fn for_test(cmd: VenueCommand, guard: Guard) -> PermittedCommand {
        PermittedCommand::guarded(cmd, guard, Instance::default())
    }
}
