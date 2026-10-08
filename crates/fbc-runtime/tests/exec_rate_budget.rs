//! FBC-e8i's done line (decisions 0018, 0030, 0073): an order-entry session charges each
//! request it writes to the buckets that count it, the conformance toy declaring an account
//! limit, a per-pair order limit and a connect limit. A normal place or amend, and any control
//! command (an order query, a consumer's arm), that would take a bucket into the consumer's
//! safety reserve is `NotSent(RateBudget)` with no byte written; cancels and reducing orders are
//! written until the bucket is empty; the session's own cancel-on-disconnect arm and resync may
//! use the reserve too (0073); and every
//! refusal is counted under the scope whose bucket refused it. (A 429 to an order-entry read
//! is counted in `exec_ready.rs`.)
//!
//! Every order command is one fbc-oms built and authorized (`armed_oms`); the clock is paused
//! and held still, so no window slides while a test runs.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use armed_oms::{
    Held, Oms, Reserved, churn, cid, order_caps, session_config_for, settle, watermark,
};
use common::{Peer, ScriptedWs};
use exec_toy::{INST_A, INST_B};
use fbc_core::{
    AckLevel, Channel, ClientOrderId, Envelope, ExecEvent, ItemRef, LimitScope, Lots, MonoNs,
    NewOrder, NotSentReason, OpKind, OrderKind, OrderRef, QueryOrder, RateLimit, RpcId, Side,
    SubmitHandle, SubmitOutcome, TagSet, Ticks, Tif,
};
use fbc_oms::{Authorization, ControlCommand, OrderOp};
use fbc_runtime::{
    BucketKey, ExecHandler, ExecOrders, ExecSession, RateCounts, RateLimiter, SafetyReserve,
    ScopeCounts,
};
use tokio::time::Instant;

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

/// The index of the account limit in [`limits`].
const ACCOUNT: usize = 0;
/// The index of the per-pair limit.
const PAIR: usize = 1;
/// The index of the connect limit.
const IP: usize = 2;

/// An account limit of `account` units a second counting every order-entry request, a per-pair
/// limit of `pair` units a second counting every order request that names an instrument, and an
/// IP limit of five connections a minute.
fn limits(account: u32, pair: u32) -> Vec<RateLimit> {
    use OpKind::{Amend, Cancel, CancelAll, Connect, Control, Place, Query};
    let limit = |scope, ops: &[OpKind], secs, units| RateLimit {
        scope,
        ops: TagSet::of(ops),
        per: Duration::from_secs(secs),
        units,
    };
    vec![
        limit(
            LimitScope::Account,
            &[Place, Amend, Cancel, CancelAll, Query, Control],
            1,
            account,
        ),
        limit(
            LimitScope::Pair,
            &[Place, Amend, Cancel, CancelAll, Query],
            1,
            pair,
        ),
        limit(LimitScope::Ip, &[Connect], 60, 5),
    ]
}

/// A session of the toy declaring `limits`, its limiter keeping `percent` of each bucket for
/// safety traffic; the limiter, the session's orders and what its handler keeps.
fn session(
    server: &ScriptedWs,
    limits: Vec<RateLimit>,
    percent: u8,
) -> (
    ExecSession<Keep>,
    fbc_runtime::ExecControl,
    RateLimiter,
    ExecOrders,
    Handles,
    Events,
) {
    let venue = Held::with_limits(limits);
    let reserve = SafetyReserve::percent(percent).unwrap();
    let config = session_config_for(venue, reserve, &server.url(), &Reserved::default());
    let rates = config.limiter.clone();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let (session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    (session, control, rates, orders, handles, events)
}

/// Keeps the paused clock from auto-advancing while the sockets are idle, until dropped.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
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

/// The units in the bucket `key` of the `limit`th limit now.
fn used(rates: &RateLimiter, limit: usize, key: BucketKey) -> u64 {
    rates.used(Instant::now(), limit, key)
}

fn pair(inst: fbc_core::InstrumentId) -> BucketKey {
    BucketKey::Pair(inst)
}

/// A query of the resting `V-1`, as the Unknown ladder sends one (0005).
fn query() -> ControlCommand {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    ControlCommand::Query(QueryOrder {
        target: OrderRef::Venue(vid),
        inst: INST_A,
        placement_nonce: None,
    })
}

/// A buy of 10 lots at `px` the registry placed and the venue acknowledged as `vid`, told to
/// the registry directly, as no session here wrote it: an order the test may amend.
fn rest(oms: &mut Oms, px: i64, vid: &str) -> ClientOrderId {
    let cid = cid();
    let order = NewOrder {
        cid,
        inst: INST_A,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(px) },
        qty: Lots::new(10).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    };
    let cmd = oms.reg.place(order).unwrap();
    drop(oms.reg.authorize(armed_oms::ACCT, cmd).unwrap());
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(exec_toy::with_scope(|scope| scope.venue_order_id(vid)).unwrap()),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    oms.reg
        .on_outcome(cid, OrderOp::Place, &item, &accepted, MonoNs(1_000))
        .unwrap();
    cid
}

/// An amend of the order `cid` to `px`, classified `reducing` or not, built and authorized.
fn amend(oms: &mut Oms, cid: ClientOrderId, px: i64, reducing: bool) -> Authorization {
    let live = oms.reg.live(cid).unwrap();
    let qty = Lots::new(10).unwrap();
    let cmd = live.amend(&order_caps(), Ticks(px), qty, reducing).unwrap();
    oms.reg.authorize(armed_oms::ACCT, cmd).unwrap()
}

/// A cancel of the order `cid`, built and authorized.
fn cancel(oms: &mut Oms, cid: ClientOrderId) -> Authorization {
    let permit = oms.reg.cancellable(cid).unwrap();
    let fbc_oms::CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
        panic!("a cancel of an acknowledged order is sent");
    };
    oms.reg.authorize(armed_oms::ACCT, cmd).unwrap()
}

/// What became of submission `rpc`, once the session has had its turn.
async fn outcome(handles: &Handles, rpc: RpcId) -> Result<(), NotSentReason> {
    settle(|| handles.borrow().iter().any(|h| h.rpc == rpc)).await;
    let handles = handles.borrow();
    let handle = handles.iter().find(|h| h.rpc == rpc).unwrap();
    handle
        .receipt
        .as_ref()
        .map(|_| ())
        .map_err(|reason| *reason)
}

/// The next frame `peer` reads starts with `kind|rpc=<rpc>|`.
async fn written(peer: &mut Peer, kind: &str, rpc: RpcId) {
    let frame = peer.recv().await;
    let head = format!("{kind}|rpc={}|", rpc.0);
    assert!(frame.starts_with(&head), "{frame} is not {head}");
}

/// Each order request is charged its weight in the account bucket and, when it names an
/// instrument, in that instrument's pair bucket, and nowhere else: never in another
/// instrument's, and the account-wide fee query in no pair bucket. The connection the session
/// opened is charged to the connect limit, and nothing is refused.
#[tokio::test(start_paused = true)]
async fn each_order_request_is_charged_to_its_account_and_pair_buckets_only() {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    let (mut session, control, rates, orders, handles, events) =
        session(&server, limits(50, 50), 0);
    let mut oms = Oms::armed();
    let watch = rates.clone();
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let rates = watch;
        // The login, the arm and the resync, which name no instrument; one connection.
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 3);
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 0);
        assert_eq!(used(&rates, IP, BucketKey::Shared), 1);

        let placed = orders.submit(oms.place()).unwrap();
        written(&mut peer, "place", placed).await;
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 4);
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 1);

        let amended = orders.submit(oms.amend()).unwrap();
        written(&mut peer, "amend", amended).await;
        let cancelled = orders.submit(oms.cancel()).unwrap();
        written(&mut peer, "cancel", cancelled).await;
        let queried = orders.submit_control(query()).unwrap();
        written(&mut peer, "query", queried).await;
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 7);
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 4);

        // The fee query names no instrument: the account bucket only.
        let fees = orders.submit_control(ControlCommand::FeeQuery).unwrap();
        assert_eq!(peer.recv().await, format!("fees|rpc={}", fees.0));
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 8);
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 4);

        // Nothing named the other instrument, and the connect limit holds the one connection.
        assert_eq!(used(&rates, PAIR, pair(INST_B)), 0);
        assert_eq!(used(&rates, IP, BucketKey::Shared), 1);
        settle(|| handles.borrow().len() == 5).await;
        assert!(handles.borrow().iter().all(|h| h.receipt.is_ok()));
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    assert_eq!(rates.counts(), RateCounts::default());
}

/// The pair bucket holds 4 units and the consumer keeps half of every bucket for safety
/// traffic, so normal requests stop at 2. A place and an order query take the bucket to its
/// floor; then a place, an amend and an order query, all normal (0073: the Unknown ladder's
/// queries do not use the reserve), are `NotSent(RateBudget)` with nothing written; a cancel
/// and a reducing amend are written, the bucket's last two units; and a cancel then finds it
/// empty, `NotSent(RateBudget)`. The four refusals are counted under the pair scope, which
/// refused them, and not under the account's.
#[tokio::test(start_paused = true)]
async fn normal_requests_stop_at_the_safety_floor_and_cancels_and_reducing_orders_go_until_empty() {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    let (mut session, control, rates, orders, handles, events) =
        session(&server, limits(50, 4), 50);
    let mut oms = Oms::armed();
    let watch = rates.clone();
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let rates = watch;
        let budget = Err(NotSentReason::RateBudget);
        let v3 = rest(&mut oms, 130_830, "V-3");

        // Below the floor: a place and a query go out, and take the bucket to it.
        let placed = orders.submit(oms.place()).unwrap();
        assert_eq!(outcome(&handles, placed).await, Ok(()));
        written(&mut peer, "place", placed).await;
        let queried = orders.submit_control(query()).unwrap();
        assert_eq!(outcome(&handles, queried).await, Ok(()));
        written(&mut peer, "query", queried).await;
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 2);

        // At the floor: normal traffic is refused, nothing of it written.
        let refused = orders.submit(oms.place()).unwrap();
        assert_eq!(outcome(&handles, refused).await, budget);
        let amended = orders.submit(oms.amend()).unwrap();
        assert_eq!(outcome(&handles, amended).await, budget);
        let queried = orders.submit_control(query()).unwrap();
        assert_eq!(outcome(&handles, queried).await, budget);
        churn().await;
        assert!(peer.quiet());
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 2);

        // A cancel and a reducing amend use the reserve, to the bucket's last unit.
        let cancelled = orders.submit(oms.cancel()).unwrap();
        assert_eq!(outcome(&handles, cancelled).await, Ok(()));
        written(&mut peer, "cancel", cancelled).await;
        let reducing = orders.submit(amend(&mut oms, v3, 130_825, true)).unwrap();
        assert_eq!(outcome(&handles, reducing).await, Ok(()));
        written(&mut peer, "amend", reducing).await;
        assert_eq!(used(&rates, PAIR, pair(INST_A)), 4);

        // Empty: a cancel too is refused, nothing written.
        let amendable = oms.amendable;
        let last = orders.submit(cancel(&mut oms, amendable)).unwrap();
        assert_eq!(outcome(&handles, last).await, budget);
        churn().await;
        assert!(peer.quiet());
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    let pair_only = ScopeCounts {
        pair: 4,
        ..ScopeCounts::default()
    };
    let counts = rates.counts();
    assert_eq!(counts.refused, pair_only);
    assert_eq!(counts.rejected, ScopeCounts::default());
}

/// The account bucket holds 3 units and the consumer keeps two of them for safety traffic, so
/// the login alone takes it to its floor: the session's cancel-on-disconnect arm and its
/// resync still go out, from the reserve (0073), and the epoch takes places. A cancel then
/// finds the bucket empty: `NotSent(RateBudget)`, nothing written, counted under the account
/// scope, as is the close frame the stop then cannot send.
#[tokio::test(start_paused = true)]
async fn the_cancel_on_disconnect_arm_and_the_resync_may_use_the_safety_reserve() {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    // 3 × 67 / 100 = 2 units kept: normal traffic stops at 1.
    let (mut session, control, rates, orders, handles, events) =
        session(&server, limits(3, 50), 67);
    let mut oms = Oms::armed();
    let watch = rates.clone();
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let rates = watch;
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 3);
        assert_eq!(rates.counts(), RateCounts::default());

        let cancelled = orders.submit(oms.cancel()).unwrap();
        assert_eq!(
            outcome(&handles, cancelled).await,
            Err(NotSentReason::RateBudget)
        );
        churn().await;
        assert!(peer.quiet());
        let account = ScopeCounts {
            account: 1,
            ..ScopeCounts::default()
        };
        assert_eq!(rates.counts().refused, account);
        // The close frame the stop sends finds the bucket empty too, and is counted.
        drop(control);
        assert_eq!(peer.next().await, None);
        rates
    };
    let (run, rates) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    let account = ScopeCounts {
        account: 2,
        ..ScopeCounts::default()
    };
    assert_eq!(rates.counts().refused, account);
    assert_eq!(session.counters().arm_failures, 0);
}

/// The account bucket holds 6 units and the consumer keeps half for safety traffic, so the
/// login, the session's own arm and its resync take it to its floor. A cancel-on-disconnect arm
/// the consumer submits is a control command, charged as normal traffic whatever its label
/// (0073, Reviewer B RB-e8i-1 on PR #113): `NotSent(RateBudget)`, nothing written, counted under
/// the account scope, so repeated consumer arms cannot drain the reserve. A cancel then still
/// goes out, from the reserve.
#[tokio::test(start_paused = true)]
async fn a_consumer_arm_is_normal_traffic_and_stops_at_the_safety_floor() {
    let thaw = freeze();
    let mut server = ScriptedWs::start().await;
    let (mut session, control, rates, orders, handles, events) =
        session(&server, limits(6, 50), 50);
    let mut oms = Oms::armed();
    let watch = rates.clone();
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let rates = watch;
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 3);

        let armed = orders
            .submit_control(ControlCommand::ArmCancelOnDisconnect)
            .unwrap();
        assert_eq!(
            outcome(&handles, armed).await,
            Err(NotSentReason::RateBudget)
        );
        churn().await;
        assert!(peer.quiet());
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 3);

        let cancelled = orders.submit(oms.cancel()).unwrap();
        assert_eq!(outcome(&handles, cancelled).await, Ok(()));
        written(&mut peer, "cancel", cancelled).await;
        assert_eq!(used(&rates, ACCOUNT, BucketKey::Shared), 4);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);
    let account = ScopeCounts {
        account: 1,
        ..ScopeCounts::default()
    };
    assert_eq!(rates.counts().refused, account);
}
