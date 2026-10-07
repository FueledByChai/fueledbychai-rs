//! FBC-w19's done line for orders fbc-oms authorized (decision 0058; PR #90 Reviewer A A1):
//! a place and an amend submitted through `ExecOrders::submit` on an authenticated epoch that is
//! not yet armed and resynced come back `NotSent(Disconnected)`, counted in `unready_refusals`,
//! with no nonce reserved and no byte written, while a cancel submitted with them goes out; once
//! the epoch is armed and resynced a place is written; on an epoch whose arm the venue rejected
//! a place is held and a cancel still goes out before the epoch ends as a drop; and no place or
//! cancel written before a drop is written again on the epoch after it.
//!
//! Every order command here is one fbc-oms built and authorized (FBC-afd): a registry armed on
//! the toy's market after a trustworthy resync that showed one resting order of ours, `V-1`.
//! The submit-time check of each authorization is FBC-j5bw's. The venue is the conformance toy,
//! declaring cancel-on-disconnect per connection, re-armed on reconnect.

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::{Peer, ScriptedWs};
use exec_toy::{INST_A, OWN_NS, ToyExec, ToySigner};
use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AckLevel, AmendQty, AssetKey, Channel, CidMatch,
    CidMint, ClientOrderId, ConfigError, EndpointPlan, Envelope, ExecCodec, ExecEndpoint,
    ExecEvent, FieldSpec, HttpPlan, InstrumentSpecDraft, ItemRef, Lots, MarketLease, MdCodec,
    MonoNs, NamespaceLease, NewOrder, NonceBlock, NonceSource, NotSentReason, OrderCaps, OrderKind,
    RpcId, Secrets, Side, SignedLots, SpecTable, SubmitHandle, SubmitOutcome, Subscription,
    SymbolError, Ticks, Tif, VenueCaps, VenueConfig, VenueError, VenueFactory, VenueOrderSnapshot,
    VenueOrderState, WallNs, WireUrl,
};
use fbc_oms::{
    Authorization, CancelChoice, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey,
    OrderOp, PreTradeCaps, Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock,
    ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};

const URL: &str = "held.url";
const ACCT: AccountKey = AccountKey::new(4);
/// The registry's caps on the toy's market, in lots: wide enough for every order here.
const CAP: i64 = 1_000;

/// The conformance toy's order entry at the configured URL.
struct Held;

impl VenueFactory for Held {
    fn id(&self) -> &'static str {
        "TOY-HELD"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(held_caps())
    }

    fn parse_fbc_common_symbol(&self, _: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NoRule)
    }

    fn discover(&self, _: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    fn plan_md(
        &self,
        _: &VenueConfig,
        _: &SpecTable,
        _: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        Ok(Vec::new())
    }

    fn md_codec(&self, _: &VenueConfig, _: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(exec_toy::NoMd)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(vec![ExecEndpoint {
            stream: exec_toy::EXEC_STREAM,
            url: WireUrl::plain(cfg.get(URL).unwrap()),
        }])
    }

    fn exec_codec(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok(Box::new(ToyExec::new(Box::new(ToySigner)))))
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

// ---------------------------------------------------------------------------------------------
// fbc-oms: a registry armed on the toy's market, which builds and authorizes every order command.
// ---------------------------------------------------------------------------------------------

/// The toy's caps, its amend stating the total quantity: fbc-oms builds no amend for a venue
/// whose amend states the quantity still to fill (FBC-b0z9), as the toy's does. Every amend
/// here is of an order nothing filled, which the toy's codec writes the same either way.
fn held_caps() -> VenueCaps {
    let mut caps = exec_toy::caps();
    let exec = caps.exec.as_mut().unwrap();
    exec.order.amend.as_mut().unwrap().qty_semantics = AmendQty::TotalIncludingFilled;
    caps
}

/// The order caps of the test's venue.
fn order_caps() -> OrderCaps {
    held_caps().exec.unwrap().order
}

/// The directory this test binary's leases are taken in.
fn lease_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("fbc-runtime-held-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// One of our client ids, each new, minted under a namespace lease of the toy's namespace.
fn cid() -> ClientOrderId {
    static MINT: OnceLock<Mutex<CidMint>> = OnceLock::new();
    let mint = MINT.get_or_init(|| {
        let lease = NamespaceLease::acquire(&lease_dir(), ACCT, OWN_NS).unwrap();
        Mutex::new(CidMint::new(lease, 0, 0, WallNs(1)))
    });
    mint.lock().unwrap().mint().unwrap()
}

/// The registry, the client id of its resting order `V-1`, which a resync showed, and of its
/// resting order `V-2`, which it placed itself and so may amend.
struct Oms {
    reg: Registry,
    resting: ClientOrderId,
    amendable: ClientOrderId,
}

impl Oms {
    /// A registry under lease names of an account of its own, its caps configuring the toy's
    /// market, after a trustworthy resync that showed the market flat with one resting buy of
    /// ours, `V-1`, and the owner's Start on it: armed, Quoting. It then places a buy of its own
    /// that the venue acknowledged as `V-2` (an order a resync showed is never amended): the
    /// registry is told the acknowledgement directly, as no session here wrote that order.
    fn armed() -> Oms {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let account = format!("acct-{}", NEXT.fetch_add(1, Ordering::Relaxed));
        let caps = order_caps();
        let symbol = exec_toy::specs().get(INST_A).unwrap().venue_symbol.clone();
        let keys = LeaseKeys::new("toy", &account, &caps).with_market(INST_A, symbol.clone());
        let limits = MarketCapsConfig {
            inventory: Some(Lots::new(CAP).unwrap()),
            resting: Some(Lots::new(CAP).unwrap()),
        };
        let pre_trade = PreTradeCaps::new().with_market(INST_A, limits).unwrap();
        let mut reg = Registry::with_caps(pre_trade).with_lease_keys(keys);
        let resting = cid();
        let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
        let snap = ResyncSnapshot {
            watermark: WallNs(1_000),
            requested_at: MonoNs(1_000),
            orders: vec![VenueOrderSnapshot {
                cid: Some(CidMatch::Ours(resting)),
                vid,
                inst: INST_A,
                side: Side::Buy,
                state: VenueOrderState::Open,
                px: Some(Ticks(130_860)),
                qty: Lots::new(10).unwrap(),
                cum_filled: Lots::new(0).unwrap(),
                post_only: None,
                reduce_only: None,
            }],
            positions: vec![(INST_A, SignedLots(0))],
        };
        let ladder = LadderConfig::new(
            Duration::from_secs(1),
            Duration::ZERO,
            Duration::from_secs(10),
            1,
        )
        .unwrap();
        let key = OrderKey {
            venue: None,
            ingest: 1,
        };
        reg.resync(&ladder, &caps, &snap, key).unwrap();
        let dir = lease_dir();
        let market = MarketLease::acquire(&dir, "toy", &account, &symbol).unwrap();
        let acct = AccountLease::acquire(&dir, "toy", &account).unwrap();
        let leases = Leases::market(market).with_account(acct);
        reg.start(INST_A, leases).unwrap();
        let amendable = cid();
        let order = NewOrder {
            cid: amendable,
            inst: INST_A,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(130_850) },
            qty: Lots::new(10).unwrap(),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cmd = reg.place(order).unwrap();
        drop(reg.authorize(ACCT, cmd).unwrap());
        let item = ItemRef {
            idx: 0,
            cid: None,
            vid: Some(exec_toy::with_scope(|scope| scope.venue_order_id("V-2")).unwrap()),
        };
        let accepted = SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        };
        reg.on_outcome(amendable, OrderOp::Place, &item, &accepted, MonoNs(1_000))
            .unwrap();
        Oms {
            reg,
            resting,
            amendable,
        }
    }

    /// A post-only buy of 25 lots, built and authorized.
    fn place(&mut self) -> Authorization {
        let order = NewOrder {
            cid: cid(),
            inst: INST_A,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(130_865) },
            qty: Lots::new(25).unwrap(),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cmd = self.reg.place(order).unwrap();
        self.reg.authorize(ACCT, cmd).unwrap()
    }

    /// An amend of the resting `V-2` to a lower price, built and authorized.
    fn amend(&mut self) -> Authorization {
        let cmd = self
            .reg
            .live(self.amendable)
            .unwrap()
            .amend(&order_caps(), Ticks(130_855), Lots::new(10).unwrap(), false)
            .unwrap();
        self.reg.authorize(ACCT, cmd).unwrap()
    }

    /// A cancel of the resting `V-1`, built and authorized.
    fn cancel(&mut self) -> Authorization {
        let permit = self.reg.cancellable(self.resting).unwrap();
        let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
            panic!("a cancel of an acknowledged order is sent");
        };
        self.reg.authorize(ACCT, cmd).unwrap()
    }
}

// ---------------------------------------------------------------------------------------------
// The session.
// ---------------------------------------------------------------------------------------------

type Reserved = Arc<Mutex<Vec<Vec<u64>>>>;

/// Nonces counted up from 0, each reservation logged.
struct Counting {
    next: u64,
    log: Reserved,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        self.log.lock().unwrap().push(block.as_slice().to_vec());
        block
    }
}

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

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets the session run, without moving the clock, until `done`.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("not settled");
}

/// The next connection the session opens, the clock moved on in steps of the pacing's floor
/// until it has (the tests hold the clock still otherwise).
async fn reconnected(server: &mut ScriptedWs) -> Peer {
    for _ in 0..1_000 {
        tokio::time::advance(Duration::from_millis(10)).await;
        churn().await;
        if let Some(peer) = server.try_accept() {
            return peer;
        }
    }
    panic!("not reconnected");
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
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, url);
    let reserved = Reserved::default();
    let limits = held_caps().limits;
    let config = ExecSessionConfig {
        venue: &Held,
        cfg,
        creds: Secrets::new(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: IngestClock::new(),
        nonces: Box::new(Counting {
            next: 0,
            log: Arc::clone(&reserved),
        }),
        conn: 6,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    };
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

/// The resync request's watermark.
fn watermark(resync: &str) -> String {
    resync.strip_prefix("resync|ts=").unwrap().to_owned()
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
