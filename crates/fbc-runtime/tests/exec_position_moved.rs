//! FBC-zux3's runtime half (decision 0153; FBC-madj, Codex r4216890742 on PR #114): on an epoch
//! armed and resynced, a Quoting place, an amend, a batch of two and a cancel are authorized
//! through fbc-oms, then submitted by the handler as it takes the decoded event of a fill, before
//! it applies that fill and before the session encodes them. The fill takes the buy side's worst
//! case to the inventory cap: a late fill of an order of ours the registry already ended, one the
//! side's counted orders did not hold. (On this market, seeded by a resync read at one instant,
//! an own fill of an order the registry does not hold is held by the seed, decision 0055, and
//! moves nothing.) The handler applies it to the registry through the fill ledger, so
//! `Handed::check_at_submit`, run just before each encode, refuses the place, the amend and the
//! batch, each `NotSent(StaleAuthorization)` with no nonce reserved and nothing written, while
//! the cancel is written with the one nonce reserved after the arm's.
//!
//! This order stands in for FBC-madj's, a submit and then a fill decoded before its encode: the
//! session takes what was submitted only on its next turn, after the input being handled
//! (decision 0057), so the four wait submitted and unencoded while the fill is applied, on every
//! run, with no timing between the peer and the session to rely on. The registry sees what
//! FBC-madj's order gives it: authorize, submit, apply the fill, then the check before the encode.
//!
//! Every order command is one fbc-oms built and authorized (FBC-afd), from a registry armed on
//! the toy's market (`armed_oms`). The venue is the conformance toy.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use armed_oms::{ACCT, Oms, Reserved, churn, cid, order_caps, session_config, settle, watermark};
use common::Peer;
use common::ScriptedWs;
use exec_toy::{INST_A, SYMBOL_A};
use fbc_core::{
    AckLevel, CancelReason, Channel, CidMatch, ClientOrderId, Envelope, ExecEvent, ItemRef, Lots,
    MonoNs, NewOrder, NotSentReason, OrderKind, OrderUpdate, RpcId, Side, SignedLots, SubmitHandle,
    SubmitOutcome, Ticks, Tif, VenueOrderState, WallNs, encode_cid,
};
use fbc_oms::{Admission, Authorization, FillLedger, FillRouted, LedgerConfig, OrderKey, OrderOp};
use fbc_runtime::{ExecHandler, ExecOrders};

/// The registry's inventory cap on the toy's market, in lots (`armed_oms`).
const CAP: i64 = 1_000;
/// What `armed_oms`'s registry rests on the buy side: `V-1` and `V-2`, 10 lots each.
const RESTING: i64 = 20;
/// The ended order's size and its late fill's: with what rests, the buy side's worst case is
/// then the cap.
const LATE: i64 = CAP - RESTING;
/// When the handler's fill arrives on the monotonic clock: after the seeding resync's request
/// (`armed_oms`'s, at 1 000), so what it moves counts on the inventory.
const ARRIVED: MonoNs = MonoNs(2_000);

/// What reached the handler, in order.
#[derive(Clone, Debug, PartialEq)]
enum Step {
    /// A fill, applied to the registry, as it was routed.
    Fill(FillRouted),
    /// What became of a submitted command.
    Submitted(RpcId, Result<(), NotSentReason>),
}

type Steps = Rc<RefCell<Vec<Step>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;
type Shared = Rc<RefCell<Oms>>;
type Stash = Rc<RefCell<Vec<Authorization>>>;
type Orders = Rc<RefCell<Option<ExecOrders>>>;
type Rpcs = Rc<RefCell<Vec<RpcId>>>;

/// The consumer's order-entry handler: it keeps every event and submission's fate, and on a
/// fill submits what its stash holds, then applies the fill to the registry.
struct Keep {
    steps: Steps,
    events: Events,
    oms: Shared,
    stash: Stash,
    orders: Orders,
    submitted: Rpcs,
    ledger: FillLedger,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        if let ExecEvent::Fill(fill) = &env.body {
            let orders = self.orders.borrow();
            let orders = orders.as_ref().unwrap();
            for auth in self.stash.borrow_mut().drain(..) {
                self.submitted
                    .borrow_mut()
                    .push(orders.submit(auth).unwrap());
            }
            let mut oms = self.oms.borrow_mut();
            let routed = match self.ledger.admit(fill, None, ARRIVED) {
                Admission::Apply(accepted) => oms.reg.apply_fill(accepted).unwrap(),
                other => panic!("expected the fill accepted, got {other:?}"),
            };
            self.steps.borrow_mut().push(Step::Fill(routed));
        }
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let receipt = handle.receipt.map(|_| ());
        self.steps
            .borrow_mut()
            .push(Step::Submitted(handle.rpc, receipt));
    }
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places: only the arm's nonce, 0, is reserved.
async fn armed_and_resynced(peer: &mut Peer, orders: &ExecOrders, events: &Events) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
    let wm = watermark(&peer.recv().await);
    peer.send("item|rpc=1|i=0|res=ok");
    peer.send(&format!("rsbegin|wm={wm}"));
    peer.send("rsend");
    settle(|| events.borrow().contains(&ExecEvent::ResyncEnd)).await;
    assert!(orders.may_place());
}

/// A buy of ours of [`LATE`] lots placed through fbc-oms, handed to a gateway, acknowledged as
/// `V-9`, then ended cancelled with nothing filled: it no longer counts on its side.
fn ended(oms: &mut Oms) -> ClientOrderId {
    let c = cid();
    let order = NewOrder {
        cid: c,
        inst: INST_A,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(130_845) },
        qty: Lots::new(LATE).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    };
    let cmd = oms.reg.place(order).unwrap();
    // Taken over by a gateway, so its drop releases nothing (decision 0152).
    drop(oms.reg.authorize(ACCT, cmd).unwrap().hand_over());
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-9")).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid.clone()),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    oms.reg
        .on_outcome(c, OrderOp::Place, &item, &accepted, MonoNs(1_500))
        .unwrap();
    let cancelled = OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        vid: Some(vid),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Canceled(CancelReason::Requested),
        cum_filled: Lots::new(0).unwrap(),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    let key = OrderKey {
        venue: None,
        ingest: 5,
    };
    oms.reg.apply_update(&cancelled, key);
    assert!(oms.reg.get(c).unwrap().state().is_terminal());
    c
}

/// The `nonce=` field of a frame.
fn nonce(line: &str) -> u64 {
    let field = line.split('|').find_map(|f| f.strip_prefix("nonce="));
    field.unwrap().parse().unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_quoting_place_amend_or_batch_submitted_before_a_decoded_fill_took_the_position_to_the_cap_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    // A blocked thread keeps the paused clock from auto-advancing while the sockets are idle.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (steps, events) = (Steps::default(), Events::default());
    let oms: Shared = Rc::new(RefCell::new(Oms::armed()));
    let (stash, slot, submitted) = (Stash::default(), Orders::default(), Rpcs::default());
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3_600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap();
    let keep = Keep {
        steps: Rc::clone(&steps),
        events: Rc::clone(&events),
        oms: Rc::clone(&oms),
        stash: Rc::clone(&stash),
        orders: Rc::clone(&slot),
        submitted: Rc::clone(&submitted),
        ledger,
    };
    let config = session_config(&server.url(), &reserved);
    let (mut session, control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    *slot.borrow_mut() = Some(orders.clone());
    let watch = Rc::clone(&steps);
    let (script_oms, script_submitted) = (Rc::clone(&oms), Rc::clone(&submitted));
    let late: Rc<RefCell<Option<ClientOrderId>>> = Rc::default();
    let script_late = Rc::clone(&late);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        {
            // Authorized while the market is flat and Quoting, each passing the caps.
            let mut oms = script_oms.borrow_mut();
            assert_eq!(oms.reg.position(INST_A), Some(SignedLots(0)));
            *script_late.borrow_mut() = Some(ended(&mut oms));
            let built = [oms.place(), oms.amend(), oms.batch(2), oms.cancel()];
            stash.borrow_mut().extend(built);
        }
        // The ended order's late fill, whole.
        let wire = encode_cid(&order_caps().client_id, script_late.borrow().unwrap()).unwrap();
        peer.send(&format!(
            "fill|seq=1|sym={SYMBOL_A}|fid=F-1|vid=V-9|cid={wire}|side=B|px=130845|qty={LATE}\
             |liq=M|fee=0|fa=USDC|pnl=0|fund=0"
        ));
        let written = peer.recv().await;
        let cancel = script_submitted.borrow()[3];
        assert!(
            written.starts_with(&format!("cancel|rpc={}|", cancel.0)),
            "{written}"
        );
        assert_eq!(nonce(&written), 1);
        let reported = |s: &Step| matches!(s, Step::Submitted(..));
        settle(|| watch.borrow().iter().filter(|s| reported(s)).count() == 4).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    // The fill reached the registry before any of the four was encoded or reported, and took
    // the buy side's worst case to the cap.
    let submitted = submitted.borrow();
    let stale = Err(NotSentReason::StaleAuthorization);
    let steps = steps.borrow();
    let ended_cid = late.borrow().unwrap();
    assert!(
        matches!(&steps[0], Step::Fill(FillRouted::Ours(c, _)) if *c == ended_cid),
        "{steps:?}"
    );
    assert_eq!(
        steps[1..],
        [
            Step::Submitted(submitted[0], stale),
            Step::Submitted(submitted[1], stale),
            Step::Submitted(submitted[2], stale),
            Step::Submitted(submitted[3], Ok(())),
        ]
    );
    assert_eq!(oms.borrow().reg.position(INST_A), Some(SignedLots(LATE)));
    // The arm's nonce and the cancel's: none for the three refused.
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1]]);
    // Refused at submit, not held for the epoch.
    assert_eq!(session.counters().unready_refusals, 0);
}
