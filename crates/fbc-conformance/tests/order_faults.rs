//! FBC-6to's done line: the fault scripts behind the Java stack's order-map bugs (design §6 and
//! Appendix A; decision 0005's Consequences) played against fbc-runtime's order-entry session
//! with the conformance toy, every order command built and authorized by fbc-oms and every
//! event the session reports applied to it as a live consumer applies them (`order_entry`).
//! Each script is a timeline of typed steps (decision 0025) whose replies are computed from the
//! request the stub reads (decision 0047), so each answer carries that request's id and order.
//!
//! - **A NEW and a PENDING_CANCEL after CANCELED.** Neither the toy nor fbc-core has a NEW or
//!   PENDING_CANCEL state: a venue's NEW is the placement's acceptance and an open update with
//!   nothing filled, and a PENDING_CANCEL (the Java stack's synthetic one included) is the
//!   cancel's acceptance with the order still reported open. Both arrive after the order's
//!   CANCELED, in both orders, the open updates under a venue sequence older and newer than
//!   the CANCELED's: the order stays terminal (I2), every late update is ignored as late, no
//!   permit is given for it again, and nothing of it rests.
//! - **An equal-key partial fill and cancel.** A partial fill (its order update and its fill)
//!   and the CANCELED of the same order under the same venue sequence, in both arrival orders:
//!   both end in the same state with the same filled quantity, inventory and resting (I1), the
//!   fill counted once either way, after the order ended included (I3).
//! - **A fill before its order's events.** The order's fill arrives before the placement's
//!   acknowledgement and its first update: inventory moves once, by that fill (a second delivery
//!   of it is a duplicate), and the filled quantity is max(cum_venue, cum_fills), never their
//!   sum (I3), whether the update's cumulative count equals the fills seen or exceeds them.
//!
//! The toy is single-phase, so 0092's final phase from a placement's order event does not
//! apply; our cancel's CANCELED is `Canceled(Requested)`, as 0093 has Paradex's be. Every
//! script is played on a paused clock nothing moves: no deadline passes, so no outcome is a
//! timeout's, and each order is written once.

mod order_entry;

use std::sync::{Arc, Mutex};

use fbc_conformance::Frame;
use fbc_conformance::toy::{self, SYMBOL_A};
use fbc_core::{CancelReason, ClientOrderId, Lots, SignedLots, encode_cid};
use fbc_oms::{Applied, FillApplied, OrdState, Routed, TerminalKind};
use order_entry::{Outcome, Played, Script, Seen, answer, counted, field, ignored_late, play};

/// The venue id the toy's stub gives every order placed here.
const VID: &str = "toy-1";

/// An order update of our buy of 25 at 130 865 as the toy's wire reports it: `st` its state
/// (with its fields), `cum` its cumulative fill, `seq` the venue's sequence.
fn order(cid: &str, st: &str, cum: i64, seq: u64) -> String {
    format!(
        "order|cid={cid}|vid={VID}|sym={SYMBOL_A}|side=B|st={st}|cum={cum}|px=130865|qty=25\
         |po=1|ro=0|seq={seq}"
    )
}

/// A maker fill `fid` of `qty` lots of our buy, `cum` filled after it, under sequence `seq`.
fn fill(cid: &str, fid: &str, qty: i64, cum: i64, seq: u64) -> String {
    format!(
        "fill|fid={fid}|vid={VID}|cum={cum}|cid={cid}|sym={SYMBOL_A}|side=B|px=130865|qty={qty}\
         |liq=M|fee=100|fa=USDC|pnl=0|fund=0|seq={seq}"
    )
}

/// The acceptance of item 0 of request `rpc`, naming `vid` when given.
fn accepted(rpc: &str, vid: Option<&str>) -> String {
    match vid {
        Some(vid) => format!("item|rpc={rpc}|i=0|res=ok|vid={vid}"),
        None => format!("item|rpc={rpc}|i=0|res=ok"),
    }
}

/// Refuses the frame a responder read unless `holds`, saying what was expected.
fn ensure(holds: bool, expected: &str) -> Result<(), String> {
    holds
        .then_some(())
        .ok_or_else(|| format!("expected {expected}"))
}

/// What the stub learnt of the placement it read: its request id and our client id as the
/// toy's wire spells it.
#[derive(Clone, Default)]
struct Placed(Arc<Mutex<Option<(String, String)>>>);

impl Placed {
    fn read(&self, frame: &Frame) -> Result<(String, String), String> {
        let read = (field(frame, "rpc")?, field(frame, "cid")?);
        *self.0.lock().unwrap() = Some(read.clone());
        Ok(read)
    }

    fn get(&self) -> Result<(String, String), String> {
        let read = self.0.lock().unwrap().clone();
        read.ok_or_else(|| "no placement read".into())
    }
}

/// How many frames the stub received carrying our client id `cid`, as the toy's wire spells it.
fn written(p: &Played<impl Sized>, cid: ClientOrderId) -> usize {
    let caps = toy::caps().exec.unwrap().order;
    let wire = encode_cid(&caps.client_id, cid).unwrap().to_string();
    let carries = |f: &&Frame| matches!(f, Frame::Text(t) if t.contains(&format!("cid={wire}")));
    p.received.iter().filter(carries).count()
}

// ---------------------------------------------------------------------------------------------
// A NEW and a PENDING_CANCEL after CANCELED.
// ---------------------------------------------------------------------------------------------

/// Which of the late events the stub sends first after the CANCELED.
#[derive(Copy, Clone, Debug)]
enum Late {
    NewFirst,
    PendingCancelFirst,
}

/// The script: the placement is read and left unanswered; our cancel (by client id, the toy
/// cancelling before an acknowledgement) is answered with the order's CANCELED under sequence
/// 2, then, in the order `late` says, the NEW (the placement's acceptance, then open updates
/// under sequences 1 and 3) and the PENDING_CANCEL (the cancel's acceptance, then an open
/// update under sequence 4).
fn late_after_canceled(late: Late) -> Script {
    let placed = Placed::default();
    let read = placed.clone();
    let place = answer("place", move |frame| {
        read.read(frame)?;
        Ok(Vec::new())
    });
    let cancel = answer("cancel", move |frame| {
        let (place_rpc, cid) = placed.get()?;
        let rpc = field(frame, "rpc")?;
        ensure(
            field(frame, "cid")? == cid,
            "a cancel of the order placed, by our id",
        )?;
        let new = vec![
            accepted(&place_rpc, Some(VID)),
            order(&cid, "open", 0, 1),
            order(&cid, "open", 0, 3),
        ];
        let pending_cancel = vec![accepted(&rpc, None), order(&cid, "open", 0, 4)];
        let mut out = vec![order(&cid, "canceled|why=requested", 0, 2)];
        match late {
            Late::NewFirst => out.extend(new.into_iter().chain(pending_cancel)),
            Late::PendingCancelFirst => out.extend(pending_cancel.into_iter().chain(new)),
        }
        Ok(out)
    });
    Script::default().reply(place).reply(cancel)
}

/// Plays [`late_after_canceled`]: the order, and whether the registry gave a cancel permit
/// for it once it had ended.
fn played_late(late: Late) -> Played<(ClientOrderId, bool)> {
    play(late_after_canceled(late), async |run| {
        run.started().await;
        let (cid, _) = run.place();
        run.replied().await;
        run.cancel(cid);
        run.replied().await;
        let permitted = run.st.borrow_mut().reg.cancellable(cid).is_ok();
        (cid, permitted)
    })
}

#[test]
fn an_order_stays_terminal_after_a_late_new_and_pending_cancel_in_either_order() {
    let mut outcomes = Vec::new();
    for late in [Late::NewFirst, Late::PendingCancelFirst] {
        let mut p = played_late(late);
        let (cid, permitted) = p.out;
        let o = Outcome::of(&p.st.reg, cid);
        let canceled = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
        assert_eq!(o.state, canceled, "{late:?}");
        assert_eq!(o.resting, Lots::ZERO, "{late:?}");
        assert_eq!(o.resting_buys, Some(Lots::ZERO), "{late:?}");
        assert_eq!(o.filled, Lots::ZERO, "{late:?}");
        assert!(!permitted, "{late:?}: a cancel permit for an ended order");
        assert!(p.st.reg.live(cid).is_err(), "{late:?}: live after it ended");
        // The CANCELED advanced it; the three open updates after it were each ignored as late.
        let advanced = Seen::Order(Routed::Ours(cid, Applied::Advanced));
        assert_eq!(p.st.seen.first(), Some(&advanced), "{late:?}");
        assert_eq!(p.st.seen.len(), 4, "{late:?}: {:?}", p.st.seen);
        assert!(
            ignored_late(&p.st.seen[1..], cid),
            "{late:?}: {:?}",
            p.st.seen
        );
        // The placement and the cancel, each written once.
        assert_eq!(written(&p, cid), 2, "{late:?}");
        outcomes.push(o);
    }
    assert_eq!(outcomes[0], outcomes[1]);
}

// ---------------------------------------------------------------------------------------------
// An equal-key partial fill and cancel.
// ---------------------------------------------------------------------------------------------

/// Which comes first of the partial fill and the CANCELED under the same sequence.
#[derive(Copy, Clone, Debug)]
enum Arrival {
    PartialFirst,
    CancelFirst,
}

/// The script: the placement is accepted and reported open under sequence 1; our cancel is
/// answered with a partial fill of 5 lots (its order update and its fill) and the order's
/// CANCELED with 5 filled, all under sequence 2, in the order `arrival` says, then the
/// cancel's acceptance.
fn equal_key_partial_and_cancel(arrival: Arrival) -> Script {
    let placed = Placed::default();
    let read = placed.clone();
    let place = answer("place", move |frame| {
        let (rpc, cid) = read.read(frame)?;
        Ok(vec![accepted(&rpc, Some(VID)), order(&cid, "open", 0, 1)])
    });
    let cancel = answer("cancel", move |frame| {
        let (_, cid) = placed.get()?;
        ensure(
            field(frame, "vid")? == VID,
            "a cancel of the order placed, by its venue id",
        )?;
        let partial = [order(&cid, "open", 5, 2), fill(&cid, "F-1", 5, 5, 2)];
        let canceled = order(&cid, "canceled|why=requested", 5, 2);
        let mut out = match arrival {
            Arrival::PartialFirst => partial.into_iter().chain([canceled]).collect::<Vec<_>>(),
            Arrival::CancelFirst => [canceled].into_iter().chain(partial).collect(),
        };
        out.push(accepted(&field(frame, "rpc")?, None));
        Ok(out)
    });
    Script::default().reply(place).reply(cancel)
}

fn played_equal_key(arrival: Arrival) -> Played<ClientOrderId> {
    play(equal_key_partial_and_cancel(arrival), async |run| {
        run.started().await;
        let (cid, _) = run.place();
        run.replied().await;
        run.cancel(cid);
        run.replied().await;
        cid
    })
}

#[test]
fn an_equal_key_partial_fill_and_cancel_end_alike_in_both_arrival_orders() {
    let partial_first = played_equal_key(Arrival::PartialFirst);
    let cancel_first = played_equal_key(Arrival::CancelFirst);
    let (a, b) = (partial_first.out, cancel_first.out);
    let first = Outcome::of(&partial_first.st.reg, a);
    let second = Outcome::of(&cancel_first.st.reg, b);
    let five = Lots::new(5).unwrap();
    let want = Outcome {
        state: OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested)),
        filled: five,
        cum_venue: five,
        cum_fills: five,
        resting: Lots::ZERO,
        inventory: SignedLots(5),
        resting_buys: Some(Lots::ZERO),
    };
    assert_eq!(first, want);
    assert_eq!(second, want);
    // The fill was counted once in each: on the open order, or on the order already ended.
    let fills = |p: &Played<ClientOrderId>| {
        let fills = p.st.seen.iter().filter(|s| matches!(s, Seen::Fill(_)));
        fills.cloned().collect::<Vec<_>>()
    };
    let (fa, fb) = (fills(&partial_first), fills(&cancel_first));
    assert!(
        matches!(&fa[..], [f] if counted(f, a, FillApplied::Live)),
        "{fa:?}"
    );
    assert!(
        matches!(&fb[..], [f] if counted(f, b, FillApplied::AfterEnd)),
        "{fb:?}"
    );
    // The partial update came after the CANCELED in the second: ignored as late.
    let late = Seen::Order(Routed::Ours(b, Applied::IgnoredLate));
    assert!(
        cancel_first.st.seen.contains(&late),
        "{:?}",
        cancel_first.st.seen
    );
    assert_eq!(written(&partial_first, a), 1);
    assert_eq!(written(&cancel_first, b), 1);
}

// ---------------------------------------------------------------------------------------------
// A fill before its order's events.
// ---------------------------------------------------------------------------------------------

/// The script: the placement is answered with a fill of 5 lots first, then its acceptance, then
/// the order's first update stating `cum` filled, then the same fill delivered again.
fn fill_before_order_events(cum: i64) -> Script {
    let place = answer("place", move |frame| {
        let (rpc, cid) = (field(frame, "rpc")?, field(frame, "cid")?);
        let first = fill(&cid, "F-1", 5, 5, 1);
        Ok(vec![
            first.clone(),
            accepted(&rpc, Some(VID)),
            order(&cid, "open", cum, 2),
            first,
        ])
    });
    Script::default().reply(place)
}

fn played_fill_first(cum: i64) -> Played<ClientOrderId> {
    play(fill_before_order_events(cum), async |run| {
        run.started().await;
        let (cid, _) = run.place();
        run.replied().await;
        cid
    })
}

#[test]
fn a_fill_before_its_orders_events_moves_inventory_once_and_fills_at_the_larger_count() {
    for cum in [5, 8] {
        let p = played_fill_first(cum);
        let cid = p.out;
        let o = Outcome::of(&p.st.reg, cid);
        let (five, venue) = (Lots::new(5).unwrap(), Lots::new(cum).unwrap());
        assert_eq!(o.state, OrdState::PartiallyFilled, "cum {cum}");
        assert_eq!((o.cum_fills, o.cum_venue), (five, venue), "cum {cum}");
        // max(cum_venue, cum_fills), never their sum.
        assert_eq!(o.filled, venue.max(five), "cum {cum}");
        assert_eq!(o.resting, Lots::new(25 - cum).unwrap(), "cum {cum}");
        // Only the fill moves the inventory, once.
        assert_eq!(o.inventory, SignedLots(5), "cum {cum}");
        let [fill, update, again] = &p.st.seen[..] else {
            panic!("cum {cum}: {:?}", p.st.seen);
        };
        assert!(counted(fill, cid, FillApplied::Live), "cum {cum}: {fill:?}");
        let advanced = Seen::Order(Routed::Ours(cid, Applied::Advanced));
        assert_eq!(update, &advanced, "cum {cum}");
        assert_eq!(again, &Seen::Duplicate, "cum {cum}");
        assert_eq!(written(&p, cid), 1, "cum {cum}");
    }
}
