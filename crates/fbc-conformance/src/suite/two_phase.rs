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
//!   then at most one final; or final at once). Where its acceptance is provisional, the check
//!   moves the clock no further than the window, and by then the placement has a final
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
use super::orders::{accepted_once, verdict};
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
        if !accepted_once(&acks, model) {
            let what = format!(
                "the placement the stub accepted was reported {outcomes:?}, not accepted once \
                 as {model:?} has it"
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
    let resolved = |c: &Ctx<'_>| second_phase(c, rpc, cid) != (false, false);
    let moved = c.advance_within(window, resolved).await;
    let phase = match (moved.is_some(), second_phase(c, rpc, cid)) {
        (true, (true, false)) => "a final acceptance",
        (true, (false, true)) => "an asynchronous reject",
        // Both: the venue accepted the order for good and refused it.
        (true, _) => {
            let what = format!(
                "the placement's provisional acceptance was followed by both a final acceptance \
                 and an asynchronous reject: {:?}",
                c.events()
            );
            return (vec![Breach::new(&model, what)], model);
        }
        (false, _) => {
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
/// final acceptance of the request came, and whether an asynchronous reject of a placement
/// naming the order did, by our client id or a venue id the request's outcomes name.
fn second_phase(c: &Ctx<'_>, rpc: RpcId, cid: ClientOrderId) -> (bool, bool) {
    let outcomes = c.outcomes(rpc);
    let fin = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let accepted = outcomes.iter().any(|(_, o)| *o == fin);
    let vids: Vec<VenueOrderId> = outcomes
        .iter()
        .filter_map(|(it, _)| it.as_ref().and_then(|it: &ItemRef| it.vid.clone()))
        .collect();
    let rejects = |e: &ExecEvent| match e {
        ExecEvent::AsyncReject { target, op, .. } => {
            *op == OpKind::Place
                && (target.client() == Some(cid)
                    || target.venue().is_some_and(|vid| vids.contains(vid)))
        }
        _ => false,
    };
    (accepted, c.events().iter().any(rejects))
}
