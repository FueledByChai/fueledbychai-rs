//! FBC-w19's done line for orders fbc-oms authorized (decision 0058; PR #90 Reviewer A A1):
//! a place and an amend submitted through `ExecOrders::submit` on an authenticated epoch that is
//! not yet armed and resynced come back `NotSent(Disconnected)`, counted in `unready_refusals`,
//! with no nonce reserved and no byte written, while a cancel submitted with them goes out; once
//! the epoch is armed and resynced a place is written; on an epoch whose arm the venue rejected
//! a place is held and a cancel still goes out before the epoch ends as a drop; and no place or
//! cancel written before a drop is written again on the epoch after it.
//!
//! Every order command here is one fbc-oms built and authorized (FBC-afd): a registry armed on
//! the toy's market after a trustworthy resync that showed one resting order of ours, `V-1`
//! (`armed_oms`). Nothing here changes the market's state, so fbc-oms's check at submit, which
//! the session runs before encoding each (FBC-j5bw, `exec_authorized.rs`), passes them all. The
//! venue is the conformance toy, declaring cancel-on-disconnect per connection, re-armed on
//! reconnect.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use armed_oms::{Oms, Reserved, churn, reconnected, session_config, settle, watermark};
use common::ScriptedWs;
use fbc_core::{Envelope, ExecEvent, NotSentReason, RpcId, SubmitHandle, SubmitOutcome};
use fbc_runtime::{ExecControl, ExecHandler, ExecOrders, ExecSession};

// ---------------------------------------------------------------------------------------------
// The session.
// ---------------------------------------------------------------------------------------------

type Handles = Rc<RefCell<Vec<SubmitHandle>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;
type Orders = Rc<RefCell<Option<ExecOrders>>>;
type Shared = Rc<RefCell<Oms>>;

/// Keeps each submission's handle and the events it hears; on hearing a request rejected it
/// submits an authorized place and then an authorized cancel through the session's orders.
struct Keep {
    handles: Handles,
    events: Events,
    orders: Orders,
    oms: Shared,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let rejected = matches!(
            env.body,
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Rejected(_),
                ..
            }
        );
        if rejected && let Some(orders) = self.orders.borrow().as_ref() {
            let mut oms = self.oms.borrow_mut();
            orders.submit(oms.place()).unwrap();
            orders.submit(oms.cancel()).unwrap();
        }
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

/// What a test reads of its session: the handles and events its handler kept, its nonce
/// reservations, and the registry that authorizes its orders.
struct Seen {
    handles: Handles,
    events: Events,
    reserved: Reserved,
    oms: Shared,
}

/// A session of the toy at `url`, its orders also its handler's.
fn held_session(url: &str) -> (ExecSession<Keep>, ExecControl, ExecOrders, Seen) {
    let reserved = Reserved::default();
    let config = session_config(url, &reserved);
    let seen = Seen {
        handles: Handles::default(),
        events: Events::default(),
        reserved,
        oms: Rc::new(RefCell::new(Oms::armed())),
    };
    let slot = Orders::default();
    let keep = Keep {
        handles: Rc::clone(&seen.handles),
        events: Rc::clone(&seen.events),
        orders: Rc::clone(&slot),
        oms: Rc::clone(&seen.oms),
    };
    let (session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    *slot.borrow_mut() = Some(orders.clone());
    (session, control, orders, seen)
}

#[tokio::test(start_paused = true)]
async fn an_authorized_place_and_amend_are_held_with_nothing_reserved_or_written_until_ready_and_a_cancel_is_not()
 {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let (mut session, control, orders, seen) = held_session(&server.url());
    let Seen {
        handles,
        events,
        reserved,
        oms,
    } = seen;
    let watch = Rc::clone(&handles);
    let heard = Rc::clone(&events);
    let log = Arc::clone(&reserved);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send("auth|ok=1|token=toy-session-token");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let wm = watermark(&peer.recv().await);
        // Authenticated, neither armed nor resynced: the arm's nonce is the only one reserved.
        assert!(!orders.may_place());
        assert_eq!(*log.lock().unwrap(), [vec![0]]);
        let (place, amend, cancel) = {
            let mut oms = oms.borrow_mut();
            (oms.place(), oms.amend(), oms.cancel())
        };
        let placed = orders.submit(place).unwrap();
        let amended = orders.submit(amend).unwrap();
        let cancelled = orders.submit(cancel).unwrap();
        // The cancel is written; the place and the amend are not.
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("cancel|rpc={}|", cancelled.0)),
            "{written}"
        );
        settle(|| watch.borrow().len() == 3).await;
        churn().await;
        assert!(peer.quiet());
        // Only the cancel's one nonce was reserved for the three.
        assert_eq!(*log.lock().unwrap(), [vec![0], vec![1]]);

        // Armed and resynced: an authorized place is written.
        peer.send("item|rpc=1|i=0|res=ok");
        peer.send(&format!("rsbegin|wm={wm}"));
        peer.send("rsend");
        settle(|| heard.borrow().contains(&ExecEvent::ResyncEnd)).await;
        assert!(orders.may_place());
        let auth = oms.borrow_mut().place();
        let later = orders.submit(auth).unwrap();
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("place|rpc={}|", later.0)),
            "{written}"
        );
        settle(|| watch.borrow().len() == 4).await;
        assert_eq!(*log.lock().unwrap(), [vec![0], vec![1], vec![2]]);

        // The connection drops. The next epoch arms and resyncs again and writes nothing else:
        // neither the cancel nor the place written on the epoch before is written again.
        peer.drop_conn();
        let mut next = reconnected(&mut server).await;
        assert!(next.recv().await.starts_with("auth|ts="));
        next.send("auth|ok=1|token=toy-session-token");
        let rpc = later.0 + 1;
        assert_eq!(next.recv().await, format!("cod|rpc={rpc}|on=1"));
        let wm = watermark(&next.recv().await);
        next.send(&format!("item|rpc={rpc}|i=0|res=ok"));
        next.send(&format!("rsbegin|wm={wm}"));
        next.send("rsend");
        settle(|| {
            heard
                .borrow()
                .iter()
                .filter(|e| **e == ExecEvent::ResyncEnd)
                .count()
                == 2
        })
        .await;
        assert!(orders.may_place());
        churn().await;
        assert!(next.quiet());
        drop(control);
        (placed, amended, cancelled, later)
    };
    let (run, (placed, amended, cancelled, later)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    let disconnected = Err(NotSentReason::Disconnected);
    let handles = handles.borrow();
    let [held_place, held_amend, sent_cancel, sent_place] = &handles[..] else {
        panic!("{handles:?}");
    };
    assert_eq!(
        (held_place.rpc, &held_place.receipt),
        (placed, &disconnected)
    );
    assert_eq!(
        (held_amend.rpc, &held_amend.receipt),
        (amended, &disconnected)
    );
    assert_eq!(sent_cancel.rpc, cancelled);
    assert!(sent_cancel.receipt.is_ok());
    assert_eq!(sent_place.rpc, later);
    assert!(sent_place.receipt.is_ok());
    // The arms of both epochs, the cancel and the later place: nothing for the held two.
    assert_eq!(
        *reserved.lock().unwrap(),
        [vec![0], vec![1], vec![2], vec![3]]
    );
    assert_eq!(session.counters().unready_refusals, 2);
    assert_eq!(session.counters().arm_failures, 0);
}

/// The venue rejects the arm of an epoch already resynced: an authorized place and cancel the
/// handler submits on hearing it are taken before the epoch ends as a drop, the place held,
/// counted, with no nonce reserved and nothing written, and the cancel written; the epoch after
/// the drop writes neither again.
#[tokio::test(start_paused = true)]
async fn on_an_epoch_whose_arm_was_rejected_an_authorized_place_is_held_and_a_cancel_still_goes_out()
 {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let (mut session, control, orders, seen) = held_session(&server.url());
    let heard = Rc::clone(&seen.events);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send("auth|ok=1|token=toy-session-token");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let wm = watermark(&peer.recv().await);
        peer.send(&format!("rsbegin|wm={wm}"));
        peer.send("rsend");
        peer.send("item|rpc=1|i=0|res=rej|code=1005");
        let written = peer.recv().await;
        assert!(written.starts_with("cancel|rpc=3|"), "{written}");
        assert_eq!(peer.next().await, None);
        assert!(!orders.may_place());

        // Reconnected through the pacing: the next epoch arms and resyncs again, and the
        // cancel written on the epoch before is not written again, nor the held place.
        let mut next = reconnected(&mut server).await;
        assert!(next.recv().await.starts_with("auth|ts="));
        next.send("auth|ok=1|token=toy-session-token");
        assert_eq!(next.recv().await, "cod|rpc=4|on=1");
        let wm = watermark(&next.recv().await);
        next.send("item|rpc=4|i=0|res=ok");
        next.send(&format!("rsbegin|wm={wm}"));
        next.send("rsend");
        settle(|| {
            heard
                .borrow()
                .iter()
                .filter(|e| **e == ExecEvent::ResyncEnd)
                .count()
                == 2
        })
        .await;
        assert!(orders.may_place());
        churn().await;
        assert!(next.quiet());
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(thaw);

    let handles = seen.handles.borrow();
    let [held_place, sent_cancel] = &handles[..] else {
        panic!("{handles:?}");
    };
    let disconnected = Err(NotSentReason::Disconnected);
    assert_eq!(
        (held_place.rpc, &held_place.receipt),
        (RpcId(2), &disconnected)
    );
    assert_eq!(sent_cancel.rpc, RpcId(3));
    assert!(sent_cancel.receipt.is_ok());
    // Each epoch's arm and the cancel: none for the place.
    assert_eq!(*seen.reserved.lock().unwrap(), [vec![0], vec![1], vec![2]]);
    assert_eq!(session.counters().unready_refusals, 1);
    assert_eq!(session.counters().arm_failures, 1);
}
