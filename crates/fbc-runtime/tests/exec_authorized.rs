//! FBC-j5bw's done line (decisions 0057, 0060, 0062): on an epoch armed and resynced, a place
//! fbc-oms authorized is encoded with an `EncodeCtx` holding one reserved nonce, and an
//! authorized batch with exactly one reserved nonce per item; an authorization fbc-oms's check
//! at submit refuses, because the kill switch went on, the market was disarmed or its state
//! changed after it was issued, comes back `NotSent(StaleAuthorization)` with no nonce reserved
//! and no byte written, since `ExecSession::send` runs `Authorization::check_at_submit` before
//! encoding every authorized command, so a change while the command waits in the session's
//! queue is seen too; and a cancel issued before the change still goes out, as the check passes
//! every cancel. The kill switch's instrument cancel-all is checked against 0005's I7 guard it
//! was built under: one queued before a disarm, which gives up the market's exclusive lease, is
//! not sent, and one queued before the kill switch went on is still written (the Stop path).
//!
//! Every order command is one fbc-oms built and authorized through its public path,
//! `Registry::authorize` (FBC-afd): a registry armed on the toy's market (`armed_oms`). The
//! venue is the conformance toy.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;

use armed_oms::{Oms, Reserved, churn, session_config, settle, watermark};
use common::{Peer, ScriptedWs};
use exec_toy::INST_A;
use fbc_core::{Envelope, ExecEvent, NotSentReason, RpcId, SubmitHandle, TrafficClass, WallNs};
use fbc_journal::{JournalSink, Record, Recorded};
use fbc_oms::{Leases, Registry};
use fbc_runtime::{ExecHandler, ExecOrders, Journal, SubmitRefusal};

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

/// The `nonce=` field of a frame or a batch item's line.
fn nonce(line: &str) -> u64 {
    let field = line.split('|').find_map(|f| f.strip_prefix("nonce="));
    field.unwrap().parse().unwrap()
}

#[tokio::test(start_paused = true)]
async fn an_authorized_place_and_batch_are_encoded_with_one_reserved_nonce_per_item() {
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
    let config = session_config(&server.url(), &reserved);
    let (mut session, control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;

        // A place: one nonce, 1, which its frame carries.
        let placed = orders.submit(oms.place()).unwrap();
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("place|rpc={}|", placed.0)),
            "{written}"
        );
        assert_eq!(nonce(&written), 1);

        // A batch of three places: one nonce per item, 2, 3 and 4, item by item.
        let batched = orders.submit(oms.batch(3)).unwrap();
        let written = peer.recv().await;
        let mut lines = written.lines();
        assert_eq!(
            lines.next().unwrap(),
            format!("batch|rpc={}|n=3", batched.0)
        );
        let items: Vec<_> = lines.collect();
        assert_eq!(items.len(), 3, "{written}");
        for (i, (line, want)) in items.iter().zip(2..).enumerate() {
            assert!(line.starts_with(&format!("place|i={i}|")), "{line}");
            assert_eq!(nonce(line), want);
        }

        // A batch of two: two nonces, 5 and 6.
        let pair = orders.submit(oms.batch(2)).unwrap();
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("batch|rpc={}|n=2", pair.0)),
            "{written}"
        );
        let nonces: Vec<_> = written.lines().skip(1).map(nonce).collect();
        assert_eq!(nonces, [5, 6]);

        settle(|| watch.borrow().len() == 3).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        [placed, batched, pair]
    };
    let (run, rpcs) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    let handles = handles.borrow();
    let sent: Vec<_> = handles.iter().map(|h| h.rpc).collect();
    assert_eq!(sent, rpcs);
    assert!(handles.iter().all(|h| h.receipt.is_ok()), "{handles:?}");
    // The arm's, the place's, then one block per batch, as long as the batch.
    assert_eq!(
        *reserved.lock().unwrap(),
        [vec![0], vec![1], vec![2, 3, 4], vec![5, 6]]
    );
    assert_eq!(session.counters().unready_refusals, 0);
}

/// Codex P2 r4214607587 on PR #115: a run dropped on an epoch that takes places leaves the
/// session to end that epoch when a journal is set; its orders end with it, so nothing is taken
/// that the run-once session could never send.
#[tokio::test(start_paused = true)]
async fn a_journal_set_after_a_run_dropped_on_a_ready_epoch_ends_its_orders() {
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
    let config = session_config(&server.url(), &reserved);
    let (mut session, _control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    {
        let run = session.run();
        tokio::pin!(run);
        let ready = async {
            let mut peer = server.accept().await;
            armed_and_resynced(&mut peer, &orders, &events).await;
            peer
        };
        let _peer = tokio::select! {
            _ = &mut run => panic!("the run ended"),
            peer = ready => peer,
        };
    }
    assert!(orders.may_place());
    session.set_journal(Journal::new(Rc::new(RefCell::new(Offered))));
    drop(thaw);
    assert!(!orders.may_place());
    let refused = orders.submit(oms.place());
    assert!(matches!(refused, Err(SubmitRefusal::Ended)), "{refused:?}");
    assert!(handles.borrow().is_empty());
}

/// A journal sink that takes every record and keeps none.
struct Offered;

impl JournalSink for Offered {
    fn record(&mut self, _: TrafficClass, _: WallNs, _: &Record) -> Recorded {
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::Ok
    }
}

/// When a test's change of the market's state comes.
#[derive(Clone, Copy)]
enum When {
    /// After the commands are authorized and before they are submitted.
    BeforeSubmit,
    /// After they are submitted and queued, before the session's turn takes them: only a check
    /// at encode, not one where `ExecOrders::submit` queues them, sees it (decisions 0057, 0062;
    /// PR #87 Reviewer B B7, PR #100 Reviewer B B1).
    WhileQueued,
}

/// On an epoch armed and resynced, a place, an amend, a batch of two and a cancel are built and
/// authorized, then `change` moves the market's state `when` the test says: the three whose
/// authorization is stale come back `NotSent(StaleAuthorization)` with no nonce reserved and
/// nothing written, and the cancel is written with the one nonce reserved after the arm's.
async fn stale_after(when: When, change: fn(&mut Registry)) {
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
    let config = session_config(&server.url(), &reserved);
    let (mut session, control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let (place, amend, batch, cancel) = (oms.place(), oms.amend(), oms.batch(2), oms.cancel());
        if let When::BeforeSubmit = when {
            change(&mut oms.reg);
        }
        let submitted = [
            orders.submit(place).unwrap(),
            orders.submit(amend).unwrap(),
            orders.submit(batch).unwrap(),
            orders.submit(cancel).unwrap(),
        ];
        // No await since the submits: the session has not had its turn, so all four still wait.
        if let When::WhileQueued = when {
            change(&mut oms.reg);
        }
        let written = peer.recv().await;
        let cancelled = submitted[3];
        assert!(
            written.starts_with(&format!("cancel|rpc={}|", cancelled.0)),
            "{written}"
        );
        assert_eq!(nonce(&written), 1);
        settle(|| watch.borrow().len() == 4).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        submitted
    };
    let (run, submitted) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    let handles = handles.borrow();
    let [place, amend, batch, cancel] = &handles[..] else {
        panic!("{handles:?}");
    };
    let stale = Err(NotSentReason::StaleAuthorization);
    for (handle, rpc) in [place, amend, batch].into_iter().zip(submitted) {
        assert_eq!((handle.rpc, &handle.receipt), (rpc, &stale));
    }
    assert_eq!(cancel.rpc, submitted[3]);
    assert!(cancel.receipt.is_ok(), "{cancel:?}");
    // The arm's nonce and the cancel's: none for the three refused.
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1]]);
    // Refused at submit, not held for the epoch.
    assert_eq!(session.counters().unready_refusals, 0);
    assert_eq!(cancel.rpc, RpcId(5));
}

#[tokio::test(start_paused = true)]
async fn an_authorization_issued_before_the_kill_switch_went_on_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(When::BeforeSubmit, |reg| {
        reg.kill(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_issued_before_a_disarm_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(When::BeforeSubmit, |reg| {
        reg.disarm(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_issued_before_a_state_change_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    // The owner's Wind-down on the armed market: Quoting becomes Exit.
    stale_after(When::BeforeSubmit, |reg| {
        reg.wind_down(INST_A, Leases::none()).unwrap();
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_queued_before_the_kill_switch_went_on_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(When::WhileQueued, |reg| {
        reg.kill(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_queued_before_a_disarm_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(When::WhileQueued, |reg| {
        reg.disarm(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_queued_before_a_state_change_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(When::WhileQueued, |reg| {
        reg.wind_down(INST_A, Leases::none()).unwrap();
    })
    .await;
}

/// On an epoch armed and resynced, the kill switch's instrument cancel-all of the market and a
/// cancel are built and authorized and submitted in that order, then `change` moves the
/// market's state while both still wait in the session's queue. The cancel-all is checked at
/// encode against 0005's I7 guard it was built under, not the market's state generation
/// (decision 0060): when `sent`, the change left that guard as it was and the cancel-all is
/// written first, its one nonce reserved after the arm's, then the cancel; otherwise the guard
/// moved, the cancel-all comes back `NotSent(StaleAuthorization)` with no nonce reserved and
/// nothing written, and the cancel is the first frame (PR #100 Reviewer B B3).
async fn cancel_all_queued(change: fn(&mut Registry), sent: bool) {
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
    let config = session_config(&server.url(), &reserved);
    let (mut session, control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&handles);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let (cancel_all, cancel) = (oms.cancel_all(), oms.cancel());
        let submitted = [
            orders.submit(cancel_all).unwrap(),
            orders.submit(cancel).unwrap(),
        ];
        // No await since the submits: the session has not had its turn, so both still wait.
        change(&mut oms.reg);
        if sent {
            let written = peer.recv().await;
            assert!(
                written.starts_with(&format!("cancelall|rpc={}|", submitted[0].0)),
                "{written}"
            );
        }
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("cancel|rpc={}|", submitted[1].0)),
            "{written}"
        );
        assert_eq!(nonce(&written), if sent { 2 } else { 1 });
        settle(|| watch.borrow().len() == 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        submitted
    };
    let (run, submitted) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    let handles = handles.borrow();
    let [cancel_all, cancel] = &handles[..] else {
        panic!("{handles:?}");
    };
    assert_eq!(cancel_all.rpc, submitted[0]);
    assert_eq!(cancel.rpc, submitted[1]);
    assert!(cancel.receipt.is_ok(), "{cancel:?}");
    if sent {
        assert!(cancel_all.receipt.is_ok(), "{cancel_all:?}");
        // The arm's nonce, the cancel-all's and the cancel's.
        assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1], vec![2]]);
    } else {
        let stale = Err(NotSentReason::StaleAuthorization);
        assert_eq!(cancel_all.receipt, stale);
        // The arm's nonce and the cancel's: none for the refused cancel-all.
        assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1]]);
    }
    assert_eq!(session.counters().unready_refusals, 0);
}

#[tokio::test(start_paused = true)]
async fn an_instrument_cancel_all_queued_before_a_disarm_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    // The disarm gives up the market's exclusive lease, which the cancel-all was built under.
    cancel_all_queued(
        |reg| {
            reg.disarm(INST_A);
        },
        false,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_instrument_cancel_all_queued_before_the_kill_switch_went_on_is_still_written() {
    // The Stop path: the kill switch leaves the lease and the foreign orders seen as they were,
    // and must not keep our orders resting (decision 0012).
    cancel_all_queued(
        |reg| {
            reg.kill(INST_A);
        },
        true,
    )
    .await;
}
