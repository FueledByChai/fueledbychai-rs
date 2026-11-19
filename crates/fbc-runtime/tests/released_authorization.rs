//! FBC-eeb4's done line in the order-entry session and the shard host (decision 0152): an
//! authorization the session never took over is released at its registry's next mutating call,
//! and one the session took over and reported is not released by its drop.
//!
//! - Refused by `ExecOrders::submit` for another account or an ended session, or still waiting
//!   when the session ends, an authorization is dropped unspent: its place ends not sent
//!   (`NotSentReason::StaleAuthorization`) at the registry's next mutating call.
//! - The session takes an authorization over at its turn, before its gate, its check at submit,
//!   its encode and every report, and as it refuses one for want of room
//!   (`SubmitRefusal::Backpressure`): one it reported, sent or not sent (`Disconnected`,
//!   `StaleAuthorization`, `RateBudget`, `Backpressure`), stays as its report leaves it until
//!   the consumer settles that report.
//! - One it took over and gave back unencoded on its nonce source's refusal, which ends the
//!   session with nothing of it encoded or reported, is dropped unspent unless the shard host
//!   keeps it, and so released.
//! - What the shard host hands back unsubmitted, through the decide pass's default
//!   `Decide::unsubmitted` or `Ladder::push`'s refusal, is released when dropped, or at once by
//!   `Registry::withdraw`.
//!
//! Every order command is one fbc-oms built and authorized through its public path: a registry
//! armed on the conformance toy's market (`armed_oms`).

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use armed_oms::{Oms, Reserved, churn, session_config, settle, watermark};
use common::{Peer, ScriptedWs};
use exec_toy::INST_A;
use fbc_core::{
    AccountKey, ClientOrderId, Envelope, ExecEvent, InstrumentId, ItemRef, MonoNs, NonceBlock,
    NonceRefused, NonceSource, NotSentReason, SubmitHandle, SubmitOutcome, VenueCommand,
};
use fbc_oms::{Authorization, OrdState, OrderOp, Registry, TerminalKind};
use fbc_runtime::{
    Decide, ExecHandler, ExecOrders, ExecSession, ExecSessionError, InFlight, Ladder,
    ReservedBound, SubmitRefusal, Unsubmitted,
};

type Handles = Rc<RefCell<Vec<SubmitHandle>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;

/// Keeps each submission's handle and the events it hears.
struct Keep {
    handles: Handles,
    events: Events,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

/// Nonces counted up from 0 until `refuse` is set, then every reservation refused.
struct Refusing {
    next: u64,
    refuse: Arc<AtomicBool>,
}

impl NonceSource for Refusing {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        block
    }

    fn try_reserve(&mut self, len: u16) -> Result<NonceBlock, NonceRefused> {
        if self.refuse.load(Ordering::Relaxed) {
            return Err(NonceRefused::Exhausted);
        }
        Ok(self.reserve(len))
    }
}

/// The client id of the place `auth` authorizes.
fn placed(auth: &Authorization) -> ClientOrderId {
    match auth.command() {
        VenueCommand::Place(order) => order.cid,
        other => panic!("a place, not {other:?}"),
    }
}

/// Runs a mutating call of the registry that changes nothing else, so it releases what was
/// handed back to it (decision 0084's next mutating call).
fn reap(oms: &mut Oms) {
    let resting = oms.resting;
    assert!(oms.reg.cancellable(resting).is_ok());
}

/// The state of our order `cid`.
fn state(reg: &Registry, cid: ClientOrderId) -> OrdState {
    reg.get(cid).unwrap().state()
}

fn released() -> OrdState {
    OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
}

/// The consumer settles the place `cid` reported not sent for `reason`, as `on_submitted`
/// tells it to.
fn settle_not_sent(reg: &mut Registry, cid: ClientOrderId, reason: NotSentReason) {
    let item = ItemRef {
        idx: 0,
        cid: Some(cid),
        vid: None,
    };
    let outcome = SubmitOutcome::NotSent(reason);
    reg.on_outcome(cid, OrderOp::Place, &item, &outcome, MonoNs(2_000))
        .unwrap();
    assert_eq!(
        state(reg, cid),
        OrdState::Terminal(TerminalKind::NotSent(reason))
    );
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places.
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

#[tokio::test]
async fn an_authorization_refused_for_another_account_or_an_ended_session_is_released() {
    let server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut oms = Oms::armed();

    // A session for another account than the registry's.
    let mut config = session_config(&server.url(), &reserved);
    config.acct = AccountKey::new(9);
    let (other, _other_control) = ExecSession::new(config, |_| {}).unwrap();
    let auth = oms.place();
    let cid = placed(&auth);
    assert_eq!(
        other.orders().submit(auth),
        Err(SubmitRefusal::OtherAccount)
    );
    // Held until the registry's next mutating call, then released.
    assert_eq!(state(&oms.reg, cid), OrdState::PendingNew);
    reap(&mut oms);
    assert_eq!(state(&oms.reg, cid), released());

    // A session that was dropped takes no more commands.
    let (ended, _control) =
        ExecSession::new(session_config(&server.url(), &reserved), |_| {}).unwrap();
    let orders = ended.orders();
    drop(ended);
    let auth = oms.place();
    let cid = placed(&auth);
    assert_eq!(orders.submit(auth), Err(SubmitRefusal::Ended));
    reap(&mut oms);
    assert_eq!(state(&oms.reg, cid), released());
}

#[tokio::test]
async fn one_waiting_when_its_session_ends_is_released_and_one_refused_for_room_is_not() {
    let server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut config = session_config(&server.url(), &reserved);
    // Room for one Normal command waiting, none in reserve.
    config.outbound = ReservedBound::new(1, 0).unwrap();
    let (session, _control) = ExecSession::new(config, |_| {}).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let waiting = oms.place();
    let w = placed(&waiting);
    orders.submit(waiting).unwrap();
    let refused = oms.place();
    let b = placed(&refused);
    assert!(matches!(
        orders.submit(refused),
        Err(SubmitRefusal::Backpressure { .. })
    ));
    // The session ends with the first never encoded and nothing reported: released.
    drop(session);
    reap(&mut oms);
    assert_eq!(state(&oms.reg, w), released());
    // The refusal was reported under its request id: the place stays until it is settled.
    assert_eq!(state(&oms.reg, b), OrdState::PendingNew);
    settle_not_sent(&mut oms.reg, b, NotSentReason::Backpressure);
}

#[tokio::test(start_paused = true)]
async fn an_authorization_the_session_reported_is_not_released_by_its_drop() {
    // A blocked thread keeps the paused clock from auto-advancing while the sockets are idle.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let mut config = session_config(&server.url(), &reserved);
    // One Normal request in flight at a time.
    config.in_flight = InFlight::new(ReservedBound::new(1, 64).unwrap());
    let (mut session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        // Sent, its answer withheld: it holds the one place in flight.
        let sent = oms.place();
        let s = placed(&sent);
        orders.submit(sent).unwrap();
        assert!(peer.recv().await.starts_with("place|"));
        // Refused for that place in flight: not sent, `RateBudget`.
        let budget = oms.place();
        let r = placed(&budget);
        orders.submit(budget).unwrap();
        settle(|| watch.borrow().len() == 2).await;
        // Authorized, then the kill switch: not sent, `StaleAuthorization`.
        let stale = oms.place();
        let k = placed(&stale);
        oms.reg.kill(INST_A);
        orders.submit(stale).unwrap();
        settle(|| watch.borrow().len() == 3).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        (oms, [s, r, k])
    };
    let (run, (mut oms, [s, r, k])) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    let receipts: Vec<_> = handles.borrow().iter().map(|h| h.receipt.clone()).collect();
    assert!(receipts[0].is_ok(), "{receipts:?}");
    assert_eq!(
        receipts[1..],
        [
            Err(NotSentReason::RateBudget),
            Err(NotSentReason::StaleAuthorization)
        ]
    );
    // The session dropped every one it took over: none is released by that.
    reap(&mut oms);
    for cid in [s, r, k] {
        assert_eq!(state(&oms.reg, cid), OrdState::PendingNew, "{cid:?}");
    }
    // What the session reported settles each.
    settle_not_sent(&mut oms.reg, r, NotSentReason::RateBudget);
    settle_not_sent(&mut oms.reg, k, NotSentReason::StaleAuthorization);
}

#[tokio::test(start_paused = true)]
async fn one_refused_before_its_epoch_takes_places_is_reported_disconnected_and_not_released() {
    // The session takes the authorization over before its first refusal, the gate's: a place
    // submitted while the epoch's arm is unanswered is reported `NotSent(Disconnected)`, and its
    // drop releases nothing, so that report alone settles it.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let (mut session, control) =
        ExecSession::new(session_config(&server.url(), &reserved), keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send("auth|ok=1|token=toy-session-token");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        // The resync's request, unanswered like the arm.
        watermark(&peer.recv().await);
        assert!(!orders.may_place());
        let early = oms.place();
        let e = placed(&early);
        orders.submit(early).unwrap();
        settle(|| watch.borrow().len() == 1).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        (oms, e)
    };
    let (run, (mut oms, e)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    assert!(events.borrow().iter().all(|ev| *ev != ExecEvent::ResyncEnd));
    let receipts: Vec<_> = handles.borrow().iter().map(|h| h.receipt.clone()).collect();
    assert_eq!(receipts, [Err(NotSentReason::Disconnected)]);
    reap(&mut oms);
    assert_eq!(state(&oms.reg, e), OrdState::PendingNew);
    settle_not_sent(&mut oms.reg, e, NotSentReason::Disconnected);
}

#[tokio::test(start_paused = true)]
async fn one_its_nonce_source_refused_is_given_back_unencoded_and_released_when_dropped() {
    // The session takes the place over at its turn, its nonce source refuses, and the session
    // ends with nothing of it encoded or reported: given back unencoded and kept by no shard
    // host, the authorization is dropped unspent and its place released (decision 0152).
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let refuse = Arc::new(AtomicBool::new(false));
    let mut config = session_config(&server.url(), &reserved);
    config.nonces = Box::new(Refusing {
        next: 0,
        refuse: Arc::clone(&refuse),
    });
    let (mut session, _control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        refuse.store(true, Ordering::Relaxed);
        let auth = oms.place();
        let cid = placed(&auth);
        orders.submit(auth).unwrap();
        (oms, cid, peer)
    };
    // The script keeps its end of the connection open, so only the refusal ends the session.
    let (run, (mut oms, cid, _peer)) = tokio::join!(session.run(), script);
    assert!(matches!(
        run,
        Err(ExecSessionError::NonceRefused(NonceRefused::Exhausted))
    ));
    drop(thaw);
    assert!(handles.borrow().is_empty());
    assert_eq!(state(&oms.reg, cid), OrdState::PendingNew);
    reap(&mut oms);
    assert_eq!(state(&oms.reg, cid), released());
}

#[tokio::test]
async fn what_the_shard_host_hands_back_unsubmitted_is_released_when_dropped_or_withdrawn() {
    let server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut oms = Oms::armed();

    // The decide pass's default `unsubmitted` drops what it is handed.
    let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
    let auth = oms.place();
    let cid = placed(&auth);
    decide.unsubmitted(Unsubmitted::Authorized(auth));
    reap(&mut oms);
    assert_eq!(state(&oms.reg, cid), released());

    // Another account's session: the ladder hands the authorization back, unplanned.
    let mut config = session_config(&server.url(), &reserved);
    config.acct = AccountKey::new(9);
    let (other, _control) = ExecSession::new(config, |_| {}).unwrap();
    let mut ladder = Ladder::default();
    let auth = oms.place();
    let dropped = placed(&auth);
    drop(ladder.push(&other.orders(), auth).unwrap_err());
    let auth = oms.place();
    let withdrawn = placed(&auth);
    let back = ladder.push(&other.orders(), auth).unwrap_err();
    assert!(ladder.is_empty());
    // Withdrawn, released at once; the one dropped is released by the same call.
    assert!(oms.reg.withdraw(*back));
    assert_eq!(state(&oms.reg, withdrawn), released());
    assert_eq!(state(&oms.reg, dropped), released());
}
