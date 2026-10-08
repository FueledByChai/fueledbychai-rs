//! FBC-nvxn's done line (decision 0080): an order of ours placed on an earlier epoch and still
//! resting when the connection drops is unprotected on the next epoch, since that epoch's
//! cancel-on-disconnect arm may cover only what is placed after it. Once the new epoch's arm is
//! accepted and its resync, which shows the order resting, has ended, the handler is told it
//! and cancels it through fbc-oms's authorization, and that cancel is written before any place
//! or amend on its market: a place and an amend submitted meanwhile are held,
//! `NotSent(Disconnected)` and counted in `unprotected_refusals`, until an order event shows the
//! order cancelled, and a place is written then. On a venue whose caps declare that an arm
//! covers the orders already open (`covers_open_orders`), the same order is kept: no cancel is
//! written and a place goes out at once.
//!
//! Every order command is one fbc-oms built and authorized (FBC-afd), from a registry armed on
//! the toy's market (`armed_oms`). The venue is the conformance toy, which declares that an arm
//! does not cover the orders already open, or `armed_oms::COVERING`, the toy declaring that it
//! does.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;

use armed_oms::{
    ACCT, COVERING, HELD, Held, Oms, Reserved, order_caps, reconnected, session_config_for, settle,
    watermark,
};
use common::{Peer, ScriptedWs};
use exec_toy::INST_A;
use fbc_core::{
    AckLevel, CidMatch, ClientOrderId, Envelope, ExecEvent, ItemRef, MonoNs, NotSentReason, RpcId,
    SubmitHandle, SubmitOutcome, VenueCommand, VenueOrderSnapshot, VenueOrderState, encode_cid,
};
use fbc_oms::{CancelChoice, OrderOp};
use fbc_runtime::{ExecControl, ExecHandler, ExecOrders, ExecSession, SafetyReserve};

type Handles = Rc<RefCell<Vec<SubmitHandle>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;
type Orders = Rc<RefCell<Option<ExecOrders>>>;
type Shared = Rc<RefCell<Oms>>;
type Told = Rc<RefCell<Vec<Vec<VenueOrderSnapshot>>>>;

/// Keeps each submission's handle, the events it hears and the unprotected orders it is told;
/// cancels each of those through fbc-oms, one authorized cancel per order.
struct Keep {
    handles: Handles,
    events: Events,
    told: Told,
    orders: Orders,
    oms: Shared,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }

    fn on_unprotected(&mut self, unprotected: &[VenueOrderSnapshot]) {
        self.told.borrow_mut().push(unprotected.to_vec());
        let orders = self.orders.borrow();
        let orders = orders.as_ref().unwrap();
        let mut oms = self.oms.borrow_mut();
        for order in unprotected {
            let Some(CidMatch::Ours(cid)) = order.cid else {
                panic!("only orders of ours are unprotected: {order:?}");
            };
            let permit = oms.reg.cancellable(cid).unwrap();
            let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
                panic!("a cancel of an acknowledged order is sent");
            };
            orders
                .submit(oms.reg.authorize(ACCT, cmd).unwrap())
                .unwrap();
        }
    }
}

/// What a test reads of its session.
struct Seen {
    handles: Handles,
    events: Events,
    told: Told,
    oms: Shared,
}

/// A session of `venue` at `url`, its orders also its handler's.
fn session(venue: &'static Held, url: &str) -> (ExecSession<Keep>, ExecControl, ExecOrders, Seen) {
    let reserve = SafetyReserve::percent(0).unwrap();
    let config = session_config_for(venue, reserve, url, &Reserved::default());
    let seen = Seen {
        handles: Handles::default(),
        events: Events::default(),
        told: Told::default(),
        oms: Rc::new(RefCell::new(Oms::armed())),
    };
    let slot = Orders::default();
    let keep = Keep {
        handles: Rc::clone(&seen.handles),
        events: Rc::clone(&seen.events),
        told: Rc::clone(&seen.told),
        orders: Rc::clone(&slot),
        oms: Rc::clone(&seen.oms),
    };
    let (session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    *slot.borrow_mut() = Some(orders.clone());
    (session, control, orders, seen)
}

/// How many `ResyncEnd`s the handler has heard.
fn resyncs(events: &Events) -> usize {
    let ended = |e: &&ExecEvent| **e == ExecEvent::ResyncEnd;
    events.borrow().iter().filter(ended).count()
}

/// The toy's wire spelling of our client id `cid`.
fn wire(cid: ClientOrderId) -> String {
    encode_cid(&order_caps().client_id, cid)
        .unwrap()
        .to_string()
}

/// Logs in on `peer`, then answers the arm (request `arm`) and the resync, the resync showing
/// `resting`, until the handler has heard the epoch's resync end, `nth` of the session's.
async fn arm_and_resync(
    peer: &mut Peer,
    arm: u64,
    resting: &[String],
    events: &Events,
    nth: usize,
) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, format!("cod|rpc={arm}|on=1"));
    let wm = watermark(&peer.recv().await);
    peer.send(&format!("item|rpc={arm}|i=0|res=ok"));
    peer.send(&format!("rsbegin|wm={wm}"));
    for order in resting {
        peer.send(order);
    }
    peer.send("rsend");
    settle(|| resyncs(events) == nth).await;
}

/// Epoch 0 armed and resynced; a buy of ours placed on it (request 2) and acknowledged as
/// `V-3`, the registry told; then the connection drops with it resting, and epoch 1 logs in,
/// arms (request 3) and resyncs, the resync showing it resting. Its client id and the peer of
/// epoch 1.
async fn placed_then_dropped(
    server: &mut ScriptedWs,
    orders: &ExecOrders,
    oms: &Shared,
    events: &Events,
) -> (ClientOrderId, Peer) {
    let mut peer = server.accept().await;
    arm_and_resync(&mut peer, 1, &[], events, 1).await;
    assert!(orders.may_place_on(INST_A));
    assert!(orders.unprotected().is_empty());
    let auth = oms.borrow_mut().place();
    let VenueCommand::Place(order) = auth.command() else {
        panic!("a place");
    };
    let cid = order.cid;
    assert_eq!(orders.submit(auth), Ok(RpcId(2)));
    let written = peer.recv().await;
    assert!(written.starts_with("place|rpc=2|"), "{written}");
    peer.send("item|rpc=2|i=0|res=ok|vid=V-3");
    let answered = |e: &ExecEvent| matches!(e, ExecEvent::Outcome { rpc: RpcId(2), .. });
    settle(|| events.borrow().iter().any(answered)).await;
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-3")).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let op = OrderOp::Place;
    let told = oms
        .borrow_mut()
        .reg
        .on_outcome(cid, op, &item, &accepted, MonoNs(2_000));
    told.unwrap();

    // The connection drops with the order resting: the venue still holds it.
    peer.drop_conn();
    let mut next = reconnected(server).await;
    let resting = format!(
        "rsorder|cid={}|vid=V-3|sym=TOYA-PERP|side=B|st=open|px=130865|qty=25|cum=0|po=1|ro=0",
        wire(cid)
    );
    arm_and_resync(&mut next, 3, &[resting], events, 2).await;
    (cid, next)
}

#[tokio::test(start_paused = true)]
async fn an_order_resting_from_an_earlier_epoch_is_cancelled_after_the_arm_and_resync_and_before_any_place_or_amend_on_its_market()
 {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let (mut session, control, orders, seen) = session(&HELD, &server.url());
    let (oms, events, handles, told) = (
        Rc::clone(&seen.oms),
        Rc::clone(&seen.events),
        Rc::clone(&seen.handles),
        Rc::clone(&seen.told),
    );
    let script = async move {
        let (cid, mut peer) = placed_then_dropped(&mut server, &orders, &oms, &events).await;
        // The epoch is armed and resynced, and the handler was told the order: its cancel, by
        // the venue id it was acknowledged under, is the first frame after the arm and the
        // resync.
        assert!(orders.may_place());
        assert!(!orders.may_place_on(INST_A));
        let shown = orders.unprotected();
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[0].cid, Some(CidMatch::Ours(cid)));
        assert_eq!(*told.borrow(), [shown]);
        let written = peer.recv().await;
        assert!(written.starts_with("cancel|rpc=4|vid=V-3|"), "{written}");

        // Until the venue shows it cancelled, a place and an amend on its market are held.
        let (place, amend) = {
            let mut oms = oms.borrow_mut();
            (oms.place(), oms.amend())
        };
        assert_eq!(orders.submit(place), Ok(RpcId(5)));
        assert_eq!(orders.submit(amend), Ok(RpcId(6)));
        settle(|| handles.borrow().len() == 4).await;
        armed_oms::churn().await;
        assert!(peer.quiet());

        // The venue shows it cancelled: the market takes places again.
        peer.send("item|rpc=4|i=0|res=ok");
        peer.send(&format!(
            "order|cid={}|vid=V-3|sym=TOYA-PERP|side=B|st=canceled|why=requested|cum=0|qty=25\
             |po=1|ro=0|seq=9",
            wire(cid)
        ));
        settle(|| orders.may_place_on(INST_A)).await;
        assert!(orders.unprotected().is_empty());
        let auth = oms.borrow_mut().place();
        assert_eq!(orders.submit(auth), Ok(RpcId(7)));
        let written = peer.recv().await;
        assert!(written.starts_with("place|rpc=7|"), "{written}");
        settle(|| handles.borrow().len() == 5).await;
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(thaw);

    let handles = seen.handles.borrow();
    let rpcs: Vec<_> = handles.iter().map(|h| h.rpc.0).collect();
    // The epoch-0 place, the epoch-1 cancel, the held place and amend, the later place.
    assert_eq!(rpcs, [2, 4, 5, 6, 7]);
    let held = Err(NotSentReason::Disconnected);
    assert!(handles[0].receipt.is_ok() && handles[1].receipt.is_ok());
    assert_eq!((&handles[2].receipt, &handles[3].receipt), (&held, &held));
    assert!(handles[4].receipt.is_ok());
    assert_eq!(seen.told.borrow().len(), 1);
    let counters = session.counters();
    assert_eq!(counters.unprotected_refusals, 2);
    assert_eq!(counters.unready_refusals, 0);
    // The order the resync showed is the one that was cancelled.
    let shown = &seen.told.borrow()[0][0];
    assert_eq!((shown.inst, &shown.state), (INST_A, &VenueOrderState::Open));
}

#[tokio::test(start_paused = true)]
async fn an_order_resting_from_an_earlier_epoch_is_kept_where_the_caps_declare_an_arm_covers_open_orders()
 {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let (mut session, control, orders, seen) = session(&COVERING, &server.url());
    let (oms, events, handles) = (
        Rc::clone(&seen.oms),
        Rc::clone(&seen.events),
        Rc::clone(&seen.handles),
    );
    let script = async move {
        let (_, mut peer) = placed_then_dropped(&mut server, &orders, &oms, &events).await;
        // The arm covers it: nothing is unprotected, and a place on its market goes out at once,
        // with no cancel before it.
        assert!(orders.may_place_on(INST_A));
        assert!(orders.unprotected().is_empty());
        let auth = oms.borrow_mut().place();
        assert_eq!(orders.submit(auth), Ok(RpcId(4)));
        let written = peer.recv().await;
        assert!(written.starts_with("place|rpc=4|"), "{written}");
        settle(|| handles.borrow().len() == 2).await;
        armed_oms::churn().await;
        assert!(peer.quiet());
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(thaw);

    assert!(seen.told.borrow().is_empty());
    assert!(seen.handles.borrow().iter().all(|h| h.receipt.is_ok()));
    assert_eq!(session.counters().unprotected_refusals, 0);
}
