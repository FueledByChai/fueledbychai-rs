//! The checks that need the venue answering over a connection (design §6, BT-502):
//! `amend_ack`, `mixed_batch` and `unknown_on_timeout`. Each runs fbc-runtime's order-entry
//! session for the venue against the stub server, which answers as the setup's
//! [`OrderEntryStub`](super::OrderEntryStub) says, with replies computed from the requests it
//! reads (decision 0047); every order command is built and authorized by fbc-oms as a live
//! consumer's is (`live.rs`). The session's clock is paused and moved by the check, so a
//! request's deadline passes only when the check says (decisions 0005, 0014, 0025).
//!
//! - `amend_ack`: an order the stub accepted is amended, and the stub accepts the amend. The
//!   amend's outcome is `Accepted`, and an `OrderUpdate` in state `Amended` names the order:
//!   from the venue's event reporting the replaced order where `AmendCaps.ack` is
//!   `ReplacedEvent`, synthesized by the codec from the reply where it is `RpcReplyOnly`. Every
//!   such update is judged: every identity it states is the order's, and every field the
//!   amend's (nothing filled, the flags the order's, stated where `events_echo_flags`). A venue
//!   allowing no limit order is skipped. The client id is stated where
//!   `cid_echoed_on_events`, and nothing is written beyond the two requests. Where the amended order gets a new venue id
//!   (`AmendCaps.keeps_venue_id` false), the update names it; where it keeps its id, the update
//!   names no new one. The placement's acceptance is its one item's, or the whole request's,
//!   once as `OrderCaps.ack` has it. Once the amend is sent, nothing refuses the order (an
//!   `AsyncReject` naming it) or ends it (a terminal `OrderUpdate`).
//! - `mixed_batch`: a batch of three placements is answered item by item, the first accepted,
//!   the second rejected and the third never; once its deadline passes, the first is
//!   `Accepted`, the second `Rejected` and the third `Unknown`, each by its index, and nothing
//!   is reported for the whole request. The third has no outcome before the clock moves.
//! - `unknown_on_timeout`: a placement the stub never answers is reported `Unknown` once at its
//!   deadline (nothing before the clock moves), naming no other order, and is never written a
//!   second time, however long the clock then runs.
//!
//! None reads a fixture file.

use std::time::Duration;

use fbc_core::{
    AckLevel, AckModel, AmendAck, AmendCaps, CidMatch, ClientOrderId, ExecEvent, ItemRef, Lots,
    OpKind, OrderKindTag, OrderUpdate, RpcId, Side, SubmitOutcome, Ticks, VenueOrderId,
    VenueOrderState,
};

use super::harness::{Harness, Shape};
use super::live::{Ctx, Live, WAIT};
use super::stub::Answer::{Accept, Reject, Silent};
use super::{Breach, Failure, Subject, Verdict};

const AMEND_ACK: &str = "amend_ack";
const MIXED_BATCH: &str = "mixed_batch";
const UNKNOWN_ON_TIMEOUT: &str = "unknown_on_timeout";
/// The items of the mixed batch: accepted, rejected, unanswered.
const ITEMS: u16 = 3;

/// Runs `amend_ack` against `subject`.
pub fn amend_ack(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(AMEND_ACK, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let Some(amend) = live.order.amend else {
        let why = "OrderCaps.amend is None: the venue takes no amend";
        return Ok(Verdict::Skipped {
            check: AMEND_ACK,
            why,
        });
    };
    // Only a resting limit order is amended (Codex r4223839082).
    if Shape::sendable(&live.order, &[OrderKindTag::Limit]).is_none() {
        let why = "OrderCaps allows no limit order to amend";
        return Ok(Verdict::Skipped {
            check: AMEND_ACK,
            why,
        });
    }
    let (px, qty) = match amended(&live.h, &amend) {
        Ok(to) => to,
        Err(why) => {
            return Ok(Verdict::Skipped {
                check: AMEND_ACK,
                why,
            });
        }
    };
    let ack = format!("AmendCaps.ack is {:?}", amend.ack);
    let model = live.order.ack;
    let echo = live.order.events_echo_flags;
    let echoes_cid = live.order.cid_echoed_on_events;
    let breaches = live.run(vec![vec![Accept], vec![Accept]], async |c| {
        c.ready().await?;
        let (cid, auth) = c.oms.place(c.h)?;
        let placed = c.send(auth, OpKind::Place).await?;
        c.settle(|c| !c.outcomes(placed).is_empty()).await;
        // What the placement's answer brings, every outcome and order update, has come.
        c.churn().await;
        let outcomes = c.outcomes(placed);
        // The single command's one item, or the whole request (Codex r4223068215), accepted
        // once as the venue acknowledges (Codex r4223435588).
        let one =
            |(it, _): &(Option<ItemRef>, SubmitOutcome)| it.as_ref().is_none_or(|it| it.idx == 0);
        let acks: Vec<&SubmitOutcome> = outcomes.iter().map(|(_, o)| o).collect();
        if !outcomes.iter().all(one) || !accepted_once(&acks, model) {
            let what =
                format!("the placement the stub accepted was reported {outcomes:?}, not once");
            return Err(c.h.fail("ExecCodec::on_frame", what));
        }
        // Applied in the order it came, as a live consumer applies it (Codex r4222779025): the
        // venue id an update states names the order where the acceptance names none.
        c.oms.replay(c.h, cid, placed, &c.events())?;
        let placed_vid = c.oms.vid(cid);
        let auth = c.oms.amend(c.h, cid, px, qty)?;
        // Only what the session reports from here on answers the amend (Codex r4222138050).
        let before = c.events().len();
        let rpc = c.send(auth, OpKind::Amend).await?;
        c.answered().await?;
        let names = |u: &OrderUpdate| {
            u.cid == Some(CidMatch::Ours(cid)) || (placed_vid.is_some() && u.vid == placed_vid)
        };
        let updates = |c: &Ctx<'_>| amended_updates(&c.events()[before..], names);
        c.settle(|c| !c.outcomes(rpc).is_empty() && !updates(c).is_empty())
            .await;
        // What the amend's answer brings with them has come.
        c.churn().await;
        let (post_only, reduce_only) = c.oms.flags();
        let asked = Asked {
            cid,
            placed: placed_vid.clone(),
            px,
            qty,
            model,
            echoes_cid,
            flags: Flags {
                post_only,
                reduce_only,
                echo,
            },
        };
        let updates = updates(c);
        let mut breaches = judge_amend(c, rpc, &updates, &asked, &amend, &ack);
        // Nothing written after the amend: the clock has not moved since, so no keepalive is
        // due, and a request written again shows (Codex r4224021315).
        let unasked = c.unasked();
        if unasked > 0 {
            let what = format!(
                "{unasked} frame(s) the check never asked for were written once the amend was \
                 answered; a request is never resent"
            );
            breaches.push(Breach::new("ExecCodec: never resent", what));
        }
        breaches.extend(contradicted(&c.events()[before..], &asked, &updates));
        Ok(breaches)
    })?;
    verdict(AMEND_ACK, breaches, || {
        let mut probed = vec![format!(
            "{ack}: an accepted amend surfaced as OrderUpdate Amended"
        )];
        if !amend.keeps_venue_id {
            probed.push(
                "AmendCaps.keeps_venue_id is false: the update named the new venue id".into(),
            );
        }
        probed
    })
}

/// The price and quantity an order the harness places is amended to: a valid price one step
/// away where the price is amendable, else twice the quantity, or the largest order where that
/// is less (Codex r4222779033), where the quantity is; why there is none otherwise.
fn amended(h: &Harness<'_>, amend: &AmendCaps) -> Result<(Ticks, Lots), &'static str> {
    let spec = h.specs().get(h.inst).expect("the harness's instrument");
    if amend.price {
        let grid = &spec.price_grid;
        let below =
            h.px.0
                .checked_sub(1)
                .and_then(|px| grid.floor_valid(Ticks(px)));
        let above =
            h.px.0
                .checked_add(1)
                .and_then(|px| grid.ceil_valid(Ticks(px)));
        let none = "no valid price lies one step from the harness's";
        return below.or(above).map(|px| (px, h.qty)).ok_or(none);
    }
    if !amend.qty {
        return Err("AmendCaps declares neither the price nor the quantity amendable");
    }
    let doubled = Lots::new(h.qty.get().saturating_mul(2)).expect("a count");
    let qty = spec.max_order_size.map_or(doubled, |max| doubled.min(max));
    let none = "InstrumentSpec.max_order_size admits no size but the harness's own to amend to";
    (qty > h.qty).then_some((h.px, qty)).ok_or(none)
}

/// Every update in state `Amended` that `names` the order, in the order reported: a consumer
/// applies each (Codex r4223629875).
fn amended_updates(events: &[ExecEvent], names: impl Fn(&OrderUpdate) -> bool) -> Vec<OrderUpdate> {
    let update = |e: &ExecEvent| match e {
        ExecEvent::Order(u) => Some(u.clone()),
        _ => None,
    };
    let amended = |u: &OrderUpdate| matches!(u.state, VenueOrderState::Amended { .. });
    events
        .iter()
        .filter_map(update)
        .filter(|u| amended(u) && names(u))
        .collect()
}

/// The amend `amend_ack` sent: of order `cid`, placed under venue id `placed` with `flags`, to
/// `px` and `qty`, on a venue acknowledging as `model`.
struct Asked {
    cid: ClientOrderId,
    placed: Option<VenueOrderId>,
    px: Ticks,
    qty: Lots,
    model: AckModel,
    /// Whether the venue's order events carry the client id (`OrderCaps.cid_echoed_on_events`).
    echoes_cid: bool,
    flags: Flags,
}

/// The flags of the order `amend_ack` placed, and whether its venue's order events echo them
/// (`OrderCaps.events_echo_flags`).
struct Flags {
    post_only: bool,
    reduce_only: bool,
    echo: bool,
}

impl Flags {
    /// Whether the flags an update states, `post_only` and `reduce_only`, are the order's, and
    /// stated where the venue echoes them (Codex r4223839103).
    fn agree(&self, post_only: Option<bool>, reduce_only: Option<bool>) -> bool {
        let agrees = |got: Option<bool>, placed| got.map_or(!self.echo, |got| got == placed);
        agrees(post_only, self.post_only) && agrees(reduce_only, self.reduce_only)
    }
}

/// Whether an item's outcomes are one acceptance as `model` has it (Codex r4222568826): one
/// final acceptance, or, on a two-phase venue, a provisional one, then at most one final.
fn accepted_once(outcomes: &[&SubmitOutcome], model: AckModel) -> bool {
    let level = |o: &&SubmitOutcome| match o {
        SubmitOutcome::Accepted { ack } => Some(*ack),
        _ => None,
    };
    let levels: Option<Vec<AckLevel>> = outcomes.iter().map(level).collect();
    let two_phase = matches!(model, AckModel::TwoPhase { .. });
    match levels.as_deref() {
        Some([AckLevel::Final]) => true,
        Some([AckLevel::Provisional] | [AckLevel::Provisional, AckLevel::Final]) => two_phase,
        _ => false,
    }
}

/// What broke `amend_ack`: the amend's outcome, and each of its `updates` and its new venue id.
fn judge_amend(
    c: &Ctx<'_>,
    rpc: RpcId,
    updates: &[OrderUpdate],
    asked: &Asked,
    amend: &AmendCaps,
    ack: &str,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    let outcomes = c.outcomes(rpc);
    // The amend's one item, once, naming the amended order where it names one (Codex
    // r4222568847); an outcome for the whole request is that item's.
    let own = |it: &Option<ItemRef>| {
        it.as_ref()
            .is_none_or(|it| it.idx == 0 && it.cid.is_none_or(|cid| cid == asked.cid))
    };
    let acks: Vec<&SubmitOutcome> = outcomes.iter().map(|(_, o)| o).collect();
    if !outcomes.iter().all(|(it, _)| own(it)) || !accepted_once(&acks, asked.model) {
        let what = format!("the amend the stub accepted was reported {outcomes:?}, not once");
        breaches.push(Breach::new("ExecCodec::on_frame", what));
    }
    if updates.is_empty() {
        let what = match amend.ack {
            AmendAck::ReplacedEvent => {
                "the venue's event reporting the replaced order surfaced no OrderUpdate \
                 Amended naming the order"
            }
            AmendAck::RpcReplyOnly => {
                "the codec synthesized no OrderUpdate Amended naming the order from the \
                 amend's reply"
            }
        };
        breaches.push(Breach::new(ack, what));
    }
    for update in updates {
        breaches.extend(judge_update(c, update, asked, amend));
    }
    breaches
}

/// What broke `amend_ack` in one `update` reporting the order amended: an identity of another
/// order, a field the amend did not ask for, its new venue id.
fn judge_update(
    c: &Ctx<'_>,
    update: &OrderUpdate,
    asked: &Asked,
    amend: &AmendCaps,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    // Every identity the update states is the amended order's (Codex r4222779011): fbc-oms
    // routes an update by its client id first, and a stranger's or a non-canonical one routes
    // it to no order of ours.
    // A venue whose events carry the client id states it (Codex r4224021308).
    let cid_agrees = update
        .cid
        .map_or(!asked.echoes_cid, |m| m == CidMatch::Ours(asked.cid));
    let vid_agrees = update.vid.is_none() || asked.placed.is_none() || update.vid == asked.placed;
    if !cid_agrees || !vid_agrees {
        let (cid, placed) = (asked.cid, &asked.placed);
        let what = format!(
            "the update names the amended order by client id {:?} and venue id {:?}; the order \
             is {cid:?}, placed under {placed:?}",
            update.cid, update.vid
        );
        breaches.push(Breach::new("OrderUpdate", what));
    }
    // The update states the amended order where it states it (Codex r4222568860), of which the
    // stub filled nothing (Codex r4223629862): fbc-oms raises its filled count from it.
    let agrees = update.inst == c.h.inst
        && update.side == Side::Buy
        && update.cum_filled == Lots::ZERO
        && asked.flags.agree(update.post_only, update.reduce_only)
        && update.px.is_none_or(|px| px == asked.px)
        && update.qty.is_none_or(|qty| qty == asked.qty);
    if !agrees {
        let (px, qty) = (asked.px, asked.qty);
        let what = format!(
            "the update {update:?} contradicts the amend of a buy on {:?} to {px:?} and {qty:?}",
            c.h.inst
        );
        breaches.push(Breach::new("OrderUpdate", what));
    }
    let new_vid = match &update.state {
        VenueOrderState::Amended { new_vid } => new_vid.as_ref(),
        _ => None,
    };
    let placed = asked.placed.as_ref();
    // A new venue id is another than the one the order was placed under (Codex r4222138032).
    if !amend.keeps_venue_id && !renamed(new_vid, placed, update.vid.as_ref()) {
        let what = format!(
            "the amended order gets a new venue id, yet the update names {new_vid:?}, the \
             order having been placed under {placed:?}"
        );
        breaches.push(Breach::new("AmendCaps.keeps_venue_id is false", what));
    }
    // An order that keeps its venue id is given no new one (Codex r4223068206): fbc-oms would
    // retire the id the order still rests under.
    if amend.keeps_venue_id && new_vid.is_some() {
        let what = format!(
            "the amended order keeps its venue id, yet the update names a new venue id \
             {new_vid:?}, the order having been placed under {placed:?}"
        );
        breaches.push(Breach::new("AmendCaps.keeps_venue_id is true", what));
    }
    breaches
}

/// What, of `events` (those reported once the amend was sent), says the amend the stub accepted
/// failed: a refusal naming the order, of any operation (Codex r4223435612), or an update
/// ending it (Reviewer B RB-3il-5), on a cancel-replace venue an end of the old order reported
/// before its replacement, which fbc-oms takes as the order's end, releasing the cap it holds
/// while it still rests under its new venue id. The order is named by its client id, the id it
/// was placed under, or a new one an amended update of `updates` names.
fn contradicted(events: &[ExecEvent], asked: &Asked, updates: &[OrderUpdate]) -> Vec<Breach> {
    let new_vid = |u: &'_ OrderUpdate| match &u.state {
        VenueOrderState::Amended { new_vid } => new_vid.clone(),
        _ => None,
    };
    let new_vids: Vec<VenueOrderId> = updates.iter().filter_map(new_vid).collect();
    let vids: Vec<&VenueOrderId> = asked.placed.iter().chain(&new_vids).collect();
    let by_vid = |vid: Option<&VenueOrderId>| vid.is_some_and(|vid| vids.contains(&vid));
    let mut breaches = Vec::new();
    for event in events {
        match event {
            ExecEvent::AsyncReject { target, op, .. }
                if target.client() == Some(asked.cid) || by_vid(target.venue()) =>
            {
                let what = format!(
                    "the amend the stub accepted, and nothing else, was followed by a refusal \
                     of {op:?} naming the order, {target:?}"
                );
                breaches.push(Breach::new("ExecEvent::AsyncReject", what));
            }
            ExecEvent::Order(u)
                if ended(&u.state)
                    && (u.cid == Some(CidMatch::Ours(asked.cid)) || by_vid(u.vid.as_ref())) =>
            {
                let what = format!(
                    "an update {:?} ended the order the stub amended, which still rests",
                    u.state
                );
                breaches.push(Breach::new("OrderUpdate", what));
            }
            _ => {}
        }
    }
    breaches
}

/// Whether `state` ends an order.
fn ended(state: &VenueOrderState) -> bool {
    !matches!(
        state,
        VenueOrderState::Open | VenueOrderState::Amended { .. }
    )
}

/// Whether an amended update names a new venue id, `new_vid`: one, and another than both the
/// id the order was placed under (`placed`, where fbc-oms knows it) and the id the update names
/// the order by (`current`), so an unchanged id passes for a new one neither way (Codex
/// r4222138032, r4223252204).
fn renamed(
    new_vid: Option<&VenueOrderId>,
    placed: Option<&VenueOrderId>,
    current: Option<&VenueOrderId>,
) -> bool {
    new_vid.is_some() && new_vid != placed && new_vid != current
}

/// Runs `mixed_batch` against `subject`.
pub fn mixed_batch(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(MIXED_BATCH, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let Some(batch) = live.order.batch_place else {
        let why = "OrderCaps.batch_place is None: the venue takes no batch";
        return Ok(Verdict::Skipped {
            check: MIXED_BATCH,
            why,
        });
    };
    if batch.max_items < ITEMS {
        let why = "Batch.max_items is below 3: no batch holds an accepted, a rejected and an \
                   unanswered item";
        return Ok(Verdict::Skipped {
            check: MIXED_BATCH,
            why,
        });
    }
    let model = live.order.ack;
    let breaches = live.run(vec![vec![Accept, Reject, Silent]], async |c| {
        c.ready().await?;
        let (cids, auth) = c.oms.batch(c.h, usize::from(ITEMS))?;
        let rpc = c.send(auth, OpKind::Place).await?;
        c.answered().await?;
        // The clock has not moved since the batch was sent: no deadline has passed, so the item
        // the stub never answered has no outcome yet (Reviewer B RB-3il-1).
        let early = of(&c.outcomes(rpc), 2).len();
        let every = |c: &Ctx<'_>| {
            let outcomes = c.outcomes(rpc);
            let has = |i| {
                outcomes
                    .iter()
                    .any(|(it, _)| it.as_ref().is_some_and(|it| it.idx == i))
            };
            (0..ITEMS).all(has)
        };
        let moved = c.advance_until(every).await;
        // The longest deadline again, so an outcome reported twice, or a resend however late,
        // shows (Codex r4222379980).
        if moved.is_some() {
            c.advance(WAIT).await;
        }
        Ok(judge_batch(c, rpc, &cids, model, moved, early))
    })?;
    verdict(MIXED_BATCH, breaches, || {
        vec![
            "Batch: an item accepted is Accepted".into(),
            "Batch: an item rejected is Rejected".into(),
            "Batch: an item unanswered is Unknown at the deadline, once".into(),
        ]
    })
}

/// The outcomes of item `i` among `outcomes`.
fn of(outcomes: &[(Option<ItemRef>, SubmitOutcome)], i: u16) -> Vec<&SubmitOutcome> {
    let item = |it: &Option<ItemRef>| it.as_ref().is_some_and(|it| it.idx == i);
    outcomes
        .iter()
        .filter(|(it, _)| item(it))
        .map(|(_, o)| o)
        .collect()
}

/// What broke `mixed_batch`: each item's outcomes, an outcome for the whole request, an
/// outcome for the unanswered item before the clock moved (`early` of them), a resend.
fn judge_batch(
    c: &Ctx<'_>,
    rpc: RpcId,
    cids: &[ClientOrderId],
    model: AckModel,
    moved: Option<Duration>,
    early: usize,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    let outcomes = c.outcomes(rpc);
    if early > 0 {
        let what = format!(
            "item 2, which the stub never answered, was reported before its deadline: {early} \
             outcome(s) before the clock moved"
        );
        breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
    }
    if moved.is_none() {
        let what = format!(
            "not every item had an outcome {WAIT:?} after the batch was answered: {outcomes:?}"
        );
        breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
    }
    let whole: Vec<_> = outcomes.iter().filter(|(it, _)| it.is_none()).collect();
    if !whole.is_empty() {
        let what = format!("outcomes for the whole batch, not its items by index: {whole:?}");
        breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
    }
    let of = |i: u16| of(&outcomes, i);
    // Each answered item once (Codex r4222138014): the acceptance final once, after at most one
    // provisional acceptance (a two-phase venue's), and the rejection once.
    let accepted = of(0);
    if !accepted_once(&accepted, model) {
        let what = format!("item 0, which the stub accepted, was reported {accepted:?}, not once");
        breaches.push(Breach::new("OrderCaps.batch_place", what));
    }
    let rejected = of(1);
    if rejected.len() != 1 || !matches!(rejected[0], SubmitOutcome::Rejected(_)) {
        let what = format!("item 1, which the stub rejected, was reported {rejected:?}, not once");
        breaches.push(Breach::new("OrderCaps.batch_place", what));
    }
    let unanswered = of(2);
    if unanswered != [&SubmitOutcome::Unknown] {
        let what = format!(
            "item 2, which the stub never answered, was reported {unanswered:?}, not Unknown \
             once"
        );
        breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
    }
    // Each item names its own order, where it names one (Codex r4222379976): fbc-oms routes an
    // outcome by it.
    let named = |(it, _): &&(Option<ItemRef>, SubmitOutcome)| {
        let it = it.as_ref()?;
        let cid = it.cid?;
        (cids.get(usize::from(it.idx)) != Some(&cid)).then_some(it.idx)
    };
    let misnamed: Vec<u16> = outcomes.iter().filter_map(|o| named(&o)).collect();
    if !misnamed.is_empty() {
        let what = format!("items {misnamed:?} name another order's client id than their own");
        breaches.push(Breach::new("ItemRef.cid", what));
    }
    let beyond: Vec<_> = outcomes
        .iter()
        .filter(|(it, _)| it.as_ref().is_some_and(|it| it.idx >= ITEMS))
        .collect();
    if !beyond.is_empty() {
        let what = format!("outcomes for items the batch does not hold: {beyond:?}");
        breaches.push(Breach::new("ExecCodec::on_frame", what));
    }
    for (i, &cid) in cids.iter().enumerate() {
        breaches.extend(resent(c, cid, &format!("item {i} of the batch")));
    }
    breaches
}

/// Runs `unknown_on_timeout` against `subject`.
pub fn unknown_on_timeout(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(UNKNOWN_ON_TIMEOUT, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let breaches = live.run(vec![vec![Silent]], async |c| {
        c.ready().await?;
        let (cid, auth) = c.oms.place(c.h)?;
        let rpc = c.send(auth, OpKind::Place).await?;
        c.answered().await?;
        // The clock has not moved since the placement was sent: no deadline has passed, so it
        // has no outcome yet (Codex r4222779000).
        let early = c.outcomes(rpc);
        let moved = c.advance_until(|c| !c.outcomes(rpc).is_empty()).await;
        // The longest deadline again, so an outcome reported twice, or a resend however late,
        // shows (Codex r4222379980).
        if moved.is_some() {
            c.advance(WAIT).await;
        }
        let mut breaches = Vec::new();
        if !early.is_empty() {
            let what = format!(
                "the placement the stub never answered was reported {early:?} before its \
                 deadline, before the clock moved"
            );
            breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
        }
        let outcomes = c.outcomes(rpc);
        // Its one item, naming the placement where it names an order (Codex r4222779007).
        let unknown = |(it, o): &(Option<ItemRef>, SubmitOutcome)| {
            let own = |it: &ItemRef| it.idx == 0 && it.cid.is_none_or(|named| named == cid);
            *o == SubmitOutcome::Unknown && it.as_ref().is_none_or(own)
        };
        if outcomes.len() != 1 || !outcomes.iter().all(unknown) {
            let what = format!(
                "the placement the stub never answered was reported {outcomes:?}, not Unknown \
                 once at its deadline"
            );
            breaches.push(Breach::new("ExecCodec::on_rpc_timeout", what));
        }
        breaches.extend(resent(c, cid, "the unanswered placement"));
        Ok(breaches)
    })?;
    verdict(UNKNOWN_ON_TIMEOUT, breaches, || {
        vec![
            "a request unanswered is Unknown at its deadline, once, and never written again".into(),
        ]
    })
}

/// A breach when the order `cid` names was written more than once, as [`Ctx::written`] counts
/// it.
fn resent(c: &Ctx<'_>, cid: ClientOrderId, what: &str) -> Option<Breach> {
    let times = c.written(cid);
    (times > 1).then(|| {
        let what = format!("{what} was written {times} times; a request is never resent");
        Breach::new("ExecCodec: never resent", what)
    })
}

/// `breaches` as the check's verdict: passed, probing what `probed` lists, when there are
/// none.
fn verdict(
    check: &'static str,
    breaches: Vec<Breach>,
    probed: impl FnOnce() -> Vec<String>,
) -> Result<Verdict, Failure> {
    if breaches.is_empty() {
        let probed = probed();
        Ok(Verdict::Passed { check, probed })
    } else {
        Err(Failure { check, breaches })
    }
}

#[cfg(test)]
mod tests {
    use fbc_core::VenueOrderId;

    use super::renamed;

    fn vid(s: &str) -> VenueOrderId {
        crate::toy::with_scope(|scope| scope.venue_order_id(s)).unwrap()
    }

    #[test]
    fn a_new_venue_id_is_another_than_the_placed_one_and_the_one_the_update_names() {
        let (a, b) = (vid("toy-1"), vid("toy-2"));
        assert!(renamed(Some(&b), Some(&a), Some(&a)));
        assert!(renamed(Some(&b), None, None));
        assert!(!renamed(None, Some(&a), Some(&a)));
        assert!(!renamed(Some(&a), Some(&a), None));
        // The placement taught fbc-oms no venue id: the update's own is the old one (Codex
        // r4223252204).
        assert!(!renamed(Some(&a), None, Some(&a)));
    }
}
