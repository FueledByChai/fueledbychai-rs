//! FBC-w19 at the session (decision 0058; PR #90 Reviewer A A1, Reviewer B B2): a place and an
//! amend submitted on an authenticated epoch that is not yet armed and resynced are
//! `NotSent(Disconnected)`, counted in `unready_refusals`, with no nonce reserved and no byte
//! written, while a cancel submitted with them goes out; once the epoch is armed and resynced a
//! place is written.
//!
//! Nothing outside fbc-oms issues an `Authorization` yet (FBC-afd), so no integration test can
//! submit a place, an amend or a cancel. This unit test queues them into the session's shared
//! queue as `ExecOrders::submit` queues an authorized command, without an authorization: it is
//! compiled only into fbc-runtime's own tests, and so is no way around 0013 rule 2. The venue is
//! the conformance toy, declaring cancel-on-disconnect per connection, re-armed on reconnect.

#[path = "../tests/common/mod.rs"]
#[allow(unused_imports)]
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use common::ScriptedWs;
use fbc_core::{
    AccountKey, AccountSummary, AmendOrder, AssetKey, CancelOrder, Channel, CidMint, ClientOrderId,
    ConfigError, EndpointPlan, Envelope, ExecCodec, ExecEndpoint, ExecEvent, FieldSpec, HttpPlan,
    InstrumentSpecDraft, Lots, MdCodec, NamespaceLease, NewOrder, NonceBlock, NonceSource,
    NotSentReason, OrderKind, OrderRef, RpcId, Secrets, Side, SpecTable, SubmitHandle,
    Subscription, SymbolError, Ticks, Tif, VenueCaps, VenueCommand, VenueConfig, VenueError,
    VenueFactory, WallNs, WireUrl,
};
use toy::{INST_A, OWN_NS, ToyExec, ToySigner};

use super::Submitted;
use crate::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, IngestClock, ProxyConfig,
    RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};

const URL: &str = "held.url";

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
        Ok(toy::caps())
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
        Box::new(toy::NoMd)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(vec![ExecEndpoint {
            stream: toy::EXEC_STREAM,
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

/// Keeps each submission's handle and the events it hears.
struct Keep {
    handles: Handles,
    events: Rc<RefCell<Vec<ExecEvent>>>,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

/// One of our client ids, minted once under a namespace lease held in a directory of its own.
fn cid() -> ClientOrderId {
    static CID: OnceLock<ClientOrderId> = OnceLock::new();
    *CID.get_or_init(|| {
        let name = format!("fbc-runtime-held-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let cid = CidMint::new(lease, 0, 0, WallNs(1)).mint().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        cid
    })
}

fn place() -> VenueCommand {
    VenueCommand::Place(NewOrder {
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
    })
}

/// An amend of venue order `V-1`, which the toy names amends by.
fn amend() -> VenueCommand {
    let vid = toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    VenueCommand::Amend(AmendOrder {
        target: OrderRef::Venue(vid),
        inst: INST_A,
        side: Side::Buy,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(130_860),
        qty: Lots::new(10).unwrap(),
        cum_filled: Lots::new(4).unwrap(),
    })
}

fn cancel() -> VenueCommand {
    VenueCommand::Cancel(CancelOrder {
        target: OrderRef::Client(cid()),
        inst: INST_A,
        side: Side::Buy,
        placement_nonce: None,
    })
}

/// Queues `cmd` as `ExecOrders::submit` queues an authorized command.
fn queue(orders: &ExecOrders, cmd: VenueCommand) -> RpcId {
    orders.shared.push(Submitted::Control(cmd)).unwrap()
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

#[tokio::test(start_paused = true)]
async fn a_place_and_an_amend_are_held_with_nothing_reserved_or_written_until_ready_and_a_cancel_is_not()
 {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, &server.url());
    let reserved = Reserved::default();
    let limits = Held.caps(&cfg).unwrap().limits;
    let config = ExecSessionConfig {
        venue: &Held,
        cfg,
        creds: Secrets::new(),
        acct: AccountKey::new(4),
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: toy::specs(),
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
    let handles = Handles::default();
    let events = Rc::default();
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };
    let (mut session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&handles);
    let heard = Rc::clone(&events);
    let log = Arc::clone(&reserved);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send("auth|ok=1|token=toy-session-token");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let resync = peer.recv().await;
        let wm = resync.strip_prefix("resync|ts=").unwrap().to_owned();
        // Authenticated, neither armed nor resynced: the arm's nonce is the only one reserved.
        assert!(!orders.may_place());
        let reserved_before = log.lock().unwrap().clone();
        assert_eq!(reserved_before, [vec![0]]);
        let placed = queue(&orders, place());
        let amended = queue(&orders, amend());
        let cancelled = queue(&orders, cancel());
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

        // Armed and resynced: a place is written.
        peer.send("item|rpc=1|i=0|res=ok");
        peer.send(&format!("rsbegin|wm={wm}"));
        peer.send("rsend");
        settle(|| heard.borrow().contains(&ExecEvent::ResyncEnd)).await;
        assert!(orders.may_place());
        let later = queue(&orders, place());
        let written = peer.recv().await;
        assert!(
            written.starts_with(&format!("place|rpc={}|", later.0)),
            "{written}"
        );
        assert_eq!(*log.lock().unwrap(), [vec![0], vec![1], vec![2]]);
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
    assert_eq!(session.counters().unready_refusals, 2);
    assert_eq!(session.counters().arm_failures, 0);
}
