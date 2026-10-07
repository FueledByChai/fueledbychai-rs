//! FBC-j5bw's done line (decisions 0057, 0060, 0062): on an epoch armed and resynced, a place
//! fbc-oms authorized is encoded with an `EncodeCtx` holding one reserved nonce, and an
//! authorized batch with exactly one reserved nonce per item; an authorization fbc-oms's check
//! at submit refuses, because the kill switch went on, the market was disarmed or its state
//! changed after it was issued, comes back `NotSent(StaleAuthorization)` with no nonce reserved
//! and no byte written, since `ExecSession::send` runs `Authorization::check_at_submit` before
//! encoding every authorized command; and a cancel issued before the change still goes out,
//! as the check passes every cancel.
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
use fbc_core::{Envelope, ExecEvent, NotSentReason, RpcId, SubmitHandle};
use fbc_oms::{Leases, Registry};
use fbc_runtime::{ExecHandler, ExecOrders};

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

/// On an epoch armed and resynced, a place, an amend, a batch of two and a cancel are built and
/// authorized, then `change` moves the market's state before they are submitted: the three
/// whose authorization is stale come back `NotSent(StaleAuthorization)` with no nonce reserved
/// and nothing written, and the cancel is written with the one nonce reserved after the arm's.
async fn stale_after(change: fn(&mut Registry)) {
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
        change(&mut oms.reg);
        let submitted = [
            orders.submit(place).unwrap(),
            orders.submit(amend).unwrap(),
            orders.submit(batch).unwrap(),
            orders.submit(cancel).unwrap(),
        ];
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
    stale_after(|reg| {
        reg.kill(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_issued_before_a_disarm_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    stale_after(|reg| {
        reg.disarm(INST_A);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn an_authorization_issued_before_a_state_change_is_not_sent_with_no_nonce_reserved_and_nothing_written()
 {
    // The owner's Wind-down on the armed market: Quoting becomes Exit.
    stale_after(|reg| {
        reg.wind_down(INST_A, Leases::none()).unwrap();
    })
    .await;
}
