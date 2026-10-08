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
//!   `ReplacedEvent`, synthesized by the codec from the reply where it is `RpcReplyOnly`. Where
//!   the amended order gets a new venue id (`AmendCaps.keeps_venue_id` false), the update names
//!   it.
//! - `mixed_batch`: a batch of three placements is answered item by item, the first accepted,
//!   the second rejected and the third never; once its deadline passes, the first is
//!   `Accepted`, the second `Rejected` and the third `Unknown`, each by its index, and nothing
//!   is reported for the whole request.
//! - `unknown_on_timeout`: a placement the stub never answers is reported `Unknown` once at its
//!   deadline, and is never written a second time, however long the clock then runs.
//!
//! None reads a fixture file.

use std::time::Duration;

use fbc_core::{
    AckLevel, AmendAck, AmendCaps, CidMatch, ClientOrderId, ExecEvent, ItemRef, Lots, OpKind,
    OrderUpdate, RpcId, SubmitOutcome, Ticks, VenueOrderId, VenueOrderState,
};

use super::harness::Harness;
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
    let Some((px, qty)) = amended(&live.h, &amend) else {
        let why = "AmendCaps declares neither the price nor the quantity amendable";
        return Ok(Verdict::Skipped {
            check: AMEND_ACK,
            why,
        });
    };
    let ack = format!("AmendCaps.ack is {:?}", amend.ack);
    let breaches = live.run(vec![vec![Accept], vec![Accept]], async |c| {
        c.ready().await?;
        let (cid, auth) = c.oms.place(c.h)?;
        let placed = c.send(auth, OpKind::Place).await?;
        c.settle(|c| !c.outcomes(placed).is_empty()).await;
        let outcomes = c.outcomes(placed);
        let Some((item, outcome @ SubmitOutcome::Accepted { .. })) = outcomes.first() else {
            let what = format!("the placement the stub accepted was reported {outcomes:?}");
            return Err(c.h.fail("ExecCodec::on_frame", what));
        };
        // An acceptance of the whole request is the one item's (Codex r4222379990).
        let whole = ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        };
        let item = item.clone().unwrap_or(whole);
        c.oms.placed(c.h, cid, &item, outcome)?;
        let auth = c.oms.amend(c.h, cid, px, qty)?;
        // Only what the session reports from here on answers the amend (Codex r4222138050).
        let before = c.events().len();
        let rpc = c.send(auth, OpKind::Amend).await?;
        c.answered().await?;
        let names = |u: &OrderUpdate| {
            u.cid == Some(CidMatch::Ours(cid)) || (item.vid.is_some() && u.vid == item.vid)
        };
        let update = |c: &Ctx<'_>| amended_update(&c.events()[before..], names);
        c.settle(|c| !c.outcomes(rpc).is_empty() && update(c).is_some())
            .await;
        Ok(judge_amend(
            c,
            rpc,
            update(c),
            item.vid.as_ref(),
            &amend,
            &ack,
        ))
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
/// away where the price is amendable, else twice the quantity where that is.
fn amended(h: &Harness<'_>, amend: &AmendCaps) -> Option<(Ticks, Lots)> {
    if amend.price {
        let grid = &h.specs().get(h.inst)?.price_grid;
        let below =
            h.px.0
                .checked_sub(1)
                .and_then(|px| grid.floor_valid(Ticks(px)));
        let above =
            h.px.0
                .checked_add(1)
                .and_then(|px| grid.ceil_valid(Ticks(px)));
        return below.or(above).map(|px| (px, h.qty));
    }
    amend
        .qty
        .then(|| h.qty.checked_add(h.qty))
        .flatten()
        .map(|qty| (h.px, qty))
}

/// The update in state `Amended` that `names` the order, if any.
fn amended_update(
    events: &[ExecEvent],
    names: impl Fn(&OrderUpdate) -> bool,
) -> Option<OrderUpdate> {
    let update = |e: &ExecEvent| match e {
        ExecEvent::Order(u) => Some(u.clone()),
        _ => None,
    };
    let amended = |u: &OrderUpdate| matches!(u.state, VenueOrderState::Amended { .. });
    events
        .iter()
        .filter_map(update)
        .find(|u| amended(u) && names(u))
}

/// What broke `amend_ack`: the amend's outcome, the update, its new venue id.
fn judge_amend(
    c: &Ctx<'_>,
    rpc: RpcId,
    update: Option<OrderUpdate>,
    placed: Option<&VenueOrderId>,
    amend: &AmendCaps,
    ack: &str,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    let outcomes = c.outcomes(rpc);
    let accepted =
        |(_, o): &(Option<ItemRef>, SubmitOutcome)| matches!(o, SubmitOutcome::Accepted { .. });
    if outcomes.is_empty() || !outcomes.iter().all(accepted) {
        let what = format!("the amend the stub accepted was reported {outcomes:?}");
        breaches.push(Breach::new("ExecCodec::on_frame", what));
    }
    let Some(update) = update else {
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
        return breaches;
    };
    let new_vid = match &update.state {
        VenueOrderState::Amended { new_vid } => new_vid.as_ref(),
        _ => None,
    };
    // A new venue id is another than the one the order was placed under (Codex r4222138032).
    if !amend.keeps_venue_id && (new_vid.is_none() || new_vid == placed) {
        let what = format!(
            "the amended order gets a new venue id, yet the update names {new_vid:?}, the \
             order having been placed under {placed:?}"
        );
        breaches.push(Breach::new("AmendCaps.keeps_venue_id is false", what));
    }
    breaches
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
    let breaches = live.run(vec![vec![Accept, Reject, Silent]], async |c| {
        c.ready().await?;
        let (cids, auth) = c.oms.batch(c.h, usize::from(ITEMS))?;
        let rpc = c.send(auth, OpKind::Place).await?;
        c.answered().await?;
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
        Ok(judge_batch(c, rpc, &cids, moved))
    })?;
    verdict(MIXED_BATCH, breaches, || {
        vec![
            "Batch: an item accepted is Accepted".into(),
            "Batch: an item rejected is Rejected".into(),
            "Batch: an item unanswered is Unknown at the deadline, once".into(),
        ]
    })
}

/// What broke `mixed_batch`: each item's outcomes, an outcome for the whole request, a
/// resend.
fn judge_batch(
    c: &Ctx<'_>,
    rpc: RpcId,
    cids: &[ClientOrderId],
    moved: Option<Duration>,
) -> Vec<Breach> {
    let mut breaches = Vec::new();
    let outcomes = c.outcomes(rpc);
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
    let of = |i: u16| -> Vec<&SubmitOutcome> {
        let item = |it: &Option<ItemRef>| it.as_ref().is_some_and(|it| it.idx == i);
        outcomes
            .iter()
            .filter(|(it, _)| item(it))
            .map(|(_, o)| o)
            .collect()
    };
    // Each answered item once (Codex r4222138014): the acceptance final once, after at most one
    // provisional acceptance (a two-phase venue's), and the rejection once.
    let accepted = of(0);
    let ack = |level| move |o: &&&SubmitOutcome| **o == &SubmitOutcome::Accepted { ack: level };
    let finals = accepted.iter().filter(ack(AckLevel::Final)).count();
    let provisional = accepted.iter().filter(ack(AckLevel::Provisional)).count();
    if finals != 1 || provisional > 1 || finals + provisional != accepted.len() {
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
        let moved = c.advance_until(|c| !c.outcomes(rpc).is_empty()).await;
        // The longest deadline again, so an outcome reported twice, or a resend however late,
        // shows (Codex r4222379980).
        if moved.is_some() {
            c.advance(WAIT).await;
        }
        let mut breaches = Vec::new();
        let outcomes = c.outcomes(rpc);
        let unknown = |(it, o): &(Option<ItemRef>, SubmitOutcome)| {
            *o == SubmitOutcome::Unknown && it.as_ref().is_none_or(|it| it.idx == 0)
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
