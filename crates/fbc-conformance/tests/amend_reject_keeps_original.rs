//! FBC-mwm6's done line: the conformance toy declares `AmendCaps::reject_keeps_original` true,
//! and an amend the venue refuses leaves its order resting as it was (BT-502; decisions 0005,
//! 0014, 0044). fbc-runtime's order-entry session for the toy runs against the stub server on a
//! paused clock, every order command built and authorized by fbc-oms and every event the session
//! reports applied to it as a live consumer applies them (`order_entry`).
//!
//! The toy codec keeps no venue-side order state: the stub, playing the toy venue, refuses each
//! amend and reports nothing else of the order, as a venue that keeps the original does. What
//! this pins is the rest: the toy codec reports the refusal as the amend's outcome alone, and
//! fbc-oms keeps the order resting whatever the amend is refused for.
//!
//! One buy is placed and accepted under venue id [`VID`], then amended to another price and
//! quantity by that venue id, once for each code of the toy's reject table that refuses an
//! amend of a resting order, each amend refused by the stub under its code. That is every code
//! but the two that say the order is not there to amend: `NotFound`, which puts it on the
//! Unknown ladder, and `AlreadyTerminal`, which says it ended (decision 0005). The codes for a
//! refusal that never ends an order (`NotAmendable`, `NoChange`) are among them, and so are the
//! codes that end an order when its placement is refused. For each amend:
//!
//! - its outcome is `Rejected` once, for its one item, carrying the stub's code as
//!   `Reject::venue_code` and the kind the table maps it to;
//! - the session reports nothing else for it: no order update (no `Amended`, no `Canceled`, no
//!   `Rejected` state), no asynchronous reject and no fill;
//! - the registry still holds the order `Open` at its original price, quantity and venue id,
//!   no venue id superseded, nothing filled, resting whole, with no amend in flight or
//!   unconfirmed, so the next amend gets its Live permit.
//!
//! The stub reads each amend naming the order by its venue id alone, as `AmendCaps.refs` has
//! it, asking for the new price and quantity; nothing is written twice. The clock never moves,
//! so no outcome is a timeout's.

mod order_entry;

use fbc_conformance::Frame;
use fbc_conformance::toy::{self, INST_A, REJECT_CODES};
use fbc_core::{
    ExecEvent, ItemRef, Lots, RefKind, RejectKind, Side, SubmitOutcome, TagSet, TerminalReject,
    Ticks,
};
use fbc_oms::{Intent, OrdState};
use order_entry::{Script, answer, field, play};

/// The venue id the stub gives the order placed here.
const VID: &str = "toy-1";
/// The order as `order_entry::buy` places it: 25 lots at 130 865 ticks.
const PX: Ticks = Ticks(130_865);
const QTY: i64 = 25;
/// What every amend asks for: 20 lots at 130 860 ticks.
const AMEND_PX: Ticks = Ticks(130_860);
const AMEND_QTY: i64 = 20;

/// The toy's codes refusing an amend of a resting order, with the kind each maps to: every one
/// but `NotFound` and `AlreadyTerminal`, which say there is no resting order to amend.
fn refusing_amend() -> Vec<(&'static str, RejectKind)> {
    let leaves =
        |kind: &RejectKind| !matches!(kind, RejectKind::NotFound | RejectKind::AlreadyTerminal(_));
    REJECT_CODES
        .iter()
        .copied()
        .filter(|(_, kind)| leaves(kind))
        .collect()
}

/// The script: the placement accepted under [`VID`]; then each amend, which must name the order
/// by [`VID`] alone, refused under the next of `codes`.
fn script(codes: &[(&'static str, RejectKind)]) -> Script {
    let accept = answer("place", |r| {
        Ok(vec![format!(
            "item|rpc={}|i=0|res=ok|vid={VID}",
            field(r, "rpc")?
        )])
    });
    let mut script = Script::default().reply(accept);
    for &(code, _) in codes {
        script = script.reply(answer("amend", move |r| {
            // Either failure names both references the amend carried.
            let (vid, cid) = (field(r, "vid")?, field(r, "cid").ok());
            if vid != VID || cid.is_some() {
                return Err(format!(
                    "expected an amend by venue id {VID} alone, read vid {vid} and cid {cid:?}"
                ));
            }
            Ok(vec![format!(
                "item|rpc={}|i=0|res=rej|code={code}|msg=refused",
                field(r, "rpc")?
            )])
        }));
    }
    script
}

/// The text of every frame the stub received that starts with `kind|`.
fn requests<'a>(received: &'a [Frame], kind: &str) -> Vec<&'a str> {
    let prefix = format!("{kind}|");
    let text = |f: &'a Frame| match f {
        Frame::Text(t) if t.starts_with(&prefix) => Some(t.as_str()),
        _ => None,
    };
    received.iter().filter_map(text).collect()
}

/// `AmendCaps::reject_keeps_original`: an amend of the toy's resting order, refused under each
/// code of its table that refuses an amend, leaves the order Open in the registry at its
/// original price, quantity and venue id, with no order update reported.
#[test]
fn refused_amend_leaves_order_resting() {
    let order = toy::caps().exec.expect("the toy enters orders").order;
    let amend = order.amend.expect("the toy amends");
    assert!(
        amend.reject_keeps_original,
        "the toy declares that a refused amend leaves the original order resting"
    );
    assert_eq!(
        amend.refs,
        TagSet::of(&[RefKind::Venue]),
        "by venue id only"
    );
    let codes = refusing_amend();
    // The refusals that never end an order are probed, and so are those that do on a placement.
    let never_ends = |(_, kind): &&(&str, RejectKind)| TerminalReject::new(*kind).is_none();
    assert_eq!(
        codes.iter().filter(never_ends).count(),
        2,
        "NotAmendable, NoChange"
    );
    assert!(
        codes.len() > 2,
        "a code that ends a refused placement is probed"
    );

    let qty = Lots::new(QTY).unwrap();
    let amend_qty = Lots::new(AMEND_QTY).unwrap();
    let mut played = play(script(&codes), async |run| {
        run.started().await;
        let (cid, _) = run.place();
        run.replied().await;
        {
            let st = run.st.borrow();
            let rec = st.reg.get(cid).unwrap();
            assert_eq!(rec.state(), OrdState::Open, "the placement rests");
            assert_eq!(rec.vid().map(|v| v.as_str()), Some(VID));
            assert_eq!((rec.px(), rec.qty()), (Some(PX), qty));
        }
        for &(code, kind) in &codes {
            let before = run.st.borrow().events.len();
            let rpc = run.amend(cid, AMEND_PX, amend_qty);
            run.replied().await;
            let st = run.st.borrow();

            // Its outcome Rejected once, under the stub's code and the table's kind; nothing
            // else reported, no order update above all.
            let [
                ExecEvent::Outcome {
                    rpc: r,
                    item,
                    outcome,
                },
            ] = &st.events[before..]
            else {
                panic!(
                    "code {code}: the refused amend brought {:?}, not its outcome alone",
                    &st.events[before..]
                );
            };
            assert_eq!(*r, rpc, "code {code}: the outcome answers the amend");
            let one = |it: &ItemRef| it.idx == 0 && it.cid.is_none_or(|named| named == cid);
            assert!(item.as_ref().is_none_or(one), "code {code}: {item:?}");
            let SubmitOutcome::Rejected(reject) = outcome else {
                panic!("code {code}: the amend was reported {outcome:?}, not Rejected");
            };
            assert_eq!(reject.venue_code.as_deref(), Some(code));
            assert_eq!(reject.kind, kind, "code {code}");

            // The order rests as it was placed.
            let rec = st.reg.get(cid).unwrap();
            assert_eq!(rec.state(), OrdState::Open, "code {code}: still Open");
            assert_eq!(rec.px(), Some(PX), "code {code}: its original price");
            assert_eq!(rec.qty(), qty, "code {code}: its original quantity");
            assert_eq!(rec.vid().map(|v| v.as_str()), Some(VID), "code {code}");
            assert_eq!(
                rec.superseded_vids().count(),
                0,
                "code {code}: no id replaced"
            );
            assert_eq!(rec.filled(), Lots::ZERO, "code {code}");
            assert_eq!(rec.resting(), qty, "code {code}: resting whole");
            assert_eq!(rec.intent(), Intent::None, "code {code}: nothing in flight");
            assert!(!rec.amend_unconfirmed(), "code {code}: no amend pending");
            assert_eq!(
                st.reg.resting_on(INST_A, Side::Buy),
                Some(qty),
                "code {code}"
            );
        }
        cid
    });

    // Every frame the stub sent was decoded, on the one connection: no refused frame hid an
    // event.
    assert_eq!(played.counters.decode_errors, 0);
    assert_eq!(played.conns.len(), 1);
    // Each amend was written once, by the order's venue id, asking for the new price and
    // quantity (the toy's wire carries what is left to fill: nothing has).
    assert!(played.st.reg.live(played.out).is_ok(), "amendable again");
    assert_eq!(requests(&played.received, "place").len(), 1);
    let amends = requests(&played.received, "amend");
    assert_eq!(amends.len(), codes.len(), "each amend written once");
    for text in amends {
        let frame = Frame::Text(text.to_owned());
        assert_eq!(field(&frame, "vid").as_deref(), Ok(VID));
        assert_eq!(field(&frame, "px"), Ok(AMEND_PX.0.to_string()));
        assert_eq!(field(&frame, "qty"), Ok(AMEND_QTY.to_string()));
    }
}
