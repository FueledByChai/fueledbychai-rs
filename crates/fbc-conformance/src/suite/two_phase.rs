//! `two_phase_ack` (design §6, BT-502; decisions 0005, 0014, 0085): on a venue whose `OrderCaps.ack`
//! is `TwoPhase`, a provisional acceptance is followed by a final one or an asynchronous reject
//! within the venue's `risk_reject_window`; on a `SinglePhase` venue no provisional acceptance
//! appears. It runs fbc-runtime's order-entry session against the stub server as the other
//! order-entry checks do (`live.rs`), the stub accepting one placement as the venue answers an
//! acceptance.
//!
//! - `SinglePhase`: no outcome the session reported, the cancel-on-disconnect arm's included,
//!   is `Accepted` at `AckLevel::Provisional`, and the placement is accepted once, final.
//! - `TwoPhase`: the placement is accepted once as the model has it (a provisional acceptance,
//!   then at most one final; or final at once), its outcomes naming its one item. Where its
//!   acceptance is provisional, the check moves the clock through the window, and by then the
//!   placement has, after that acceptance, a final
//!   acceptance (`Accepted` at `AckLevel::Final` for its request) or an asynchronous reject
//!   (`ExecEvent::AsyncReject` of a placement naming the order, by our client id or a venue id
//!   its outcomes name), not both. Where the venue accepts it final at once, nothing waits for a second
//!   phase, and the check says so.

use std::time::Duration;

use fbc_core::{
    AckLevel, AckModel, ClientOrderId, ExecEvent, ItemRef, OpKind, RpcId, SubmitOutcome,
    VenueOrderId,
};

use super::live::{Ctx, Live};
use super::orders::{accepted_once, own_item, verdict};
use super::stub::Answer::Accept;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "two_phase_ack";

/// Runs `two_phase_ack` against `subject`.
pub fn two_phase_ack(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(CHECK, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let model = live.order.ack;
    let (breaches, probed) = live.run(vec![vec![Accept]], async |c| {
        c.ready().await?;
        let (cid, auth) = c.oms.place(c.h)?;
        let rpc = c.send(auth, OpKind::Place).await?;
        c.answered().await?;
        let (mut breaches, probed) = match model {
            AckModel::SinglePhase => {
                let probed = "AckModel is SinglePhase: no provisional acceptance appeared";
                (single_phase(c), probed.to_owned())
            }
            AckModel::TwoPhase { risk_reject_window } => {
                two_phase(c, rpc, cid, risk_reject_window).await
            }
        };
        let outcomes = c.outcomes(rpc);
        let acks: Vec<&SubmitOutcome> = outcomes.iter().map(|(_, o)| o).collect();
        if !own_item(&outcomes, cid) || !accepted_once(&acks, model) {
            let what = format!(
                "the placement the stub accepted was reported {outcomes:?}, not accepted once \
                 as {model:?} has it, naming its one item"
            );
            breaches.push(Breach::new("OrderCaps.ack", what));
        }
        Ok((breaches, probed))
    })?;
    verdict(CHECK, breaches, || vec![probed])
}

/// What broke `two_phase_ack` on a single-phase venue: every provisional acceptance the
/// session reported, of any request.
fn single_phase(c: &Ctx<'_>) -> Vec<Breach> {
    let provisional = |e: &&ExecEvent| {
        matches!(
            e,
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Accepted {
                    ack: AckLevel::Provisional
                },
                ..
            }
        )
    };
    let events = c.events();
    let seen: Vec<&ExecEvent> = events.iter().filter(provisional).collect();
    if seen.is_empty() {
        return Vec::new();
    }
    let what = format!("provisional acceptances were reported: {seen:?}");
    vec![Breach::new("AckModel is SinglePhase", what)]
}

/// What broke `two_phase_ack` on a two-phase venue whose window is `window`, for placement
/// `cid`, request `rpc`, and what it probed.
async fn two_phase(
    c: &Ctx<'_>,
    rpc: RpcId,
    cid: ClientOrderId,
    window: Duration,
) -> (Vec<Breach>, String) {
    let model = format!("AckModel is TwoPhase {{ risk_reject_window: {window:?} }}");
    let provisional = c.outcomes(rpc).iter().any(|(_, o)| {
        *o == SubmitOutcome::Accepted {
            ack: AckLevel::Provisional,
        }
    });
    if !provisional {
        return (
            Vec::new(),
            format!("{model}: the placement was accepted final at once"),
        );
    }
    // The whole window, so a second phase after the first shows too (Codex r4226060233).
    c.advance(window).await;
    let phase = match second_phase(c, rpc, cid) {
        (true, false) => "a final acceptance",
        (false, true) => "an asynchronous reject",
        // Both: the venue accepted the order for good and refused it.
        (true, true) => {
            let what = format!(
                "the placement's provisional acceptance was followed by both a final acceptance \
                 and an asynchronous reject: {:?}",
                c.events()
            );
            return (vec![Breach::new(&model, what)], model);
        }
        (false, false) => {
            let what = format!(
                "the placement's provisional acceptance was followed by neither a final \
                 acceptance nor an asynchronous reject within the window: {:?}",
                c.outcomes(rpc)
            );
            return (vec![Breach::new(&model, what)], model);
        }
    };
    let probed =
        format!("{model}: a provisional acceptance was followed by {phase} within the window");
    (Vec::new(), probed)
}

/// What followed placement `cid`'s provisional acceptance, request `rpc`, so far: whether a
/// final acceptance of the request came after it, and whether an asynchronous reject of a
/// placement naming the order did, every identity it states the order's (our client id, a venue
/// id the request's outcomes name)
/// (Codex r4225971611: one reported before the provisional acceptance does not follow it).
fn second_phase(c: &Ctx<'_>, rpc: RpcId, cid: ClientOrderId) -> (bool, bool) {
    let outcomes = c.outcomes(rpc);
    let events = c.events();
    let level = |ack| SubmitOutcome::Accepted { ack };
    let of = |e: &ExecEvent, ack| matches!(e, ExecEvent::Outcome { rpc: r, outcome, .. } if *r == rpc && *outcome == level(ack));
    // Called once a provisional acceptance was reported; none follows where none was.
    let first = events.iter().position(|e| of(e, AckLevel::Provisional));
    let after = &events[first.unwrap_or(events.len())..];
    let accepted = after.iter().any(|e| of(e, AckLevel::Final));
    let vids: Vec<VenueOrderId> = outcomes
        .iter()
        .filter_map(|(it, _)| it.as_ref().and_then(|it: &ItemRef| it.vid.clone()))
        .collect();
    let rejects = |e: &ExecEvent| match e {
        // Every identity it states is the order's, and it states one (Codex r4226156929).
        ExecEvent::AsyncReject { target, op, .. } => {
            let (client, venue) = (target.client(), target.venue());
            *op == OpKind::Place
                && client.is_none_or(|named| named == cid)
                && venue.is_none_or(|vid| vids.contains(vid))
                && (client.is_some() || venue.is_some())
        }
        _ => false,
    };
    (accepted, after.iter().any(rejects))
}
