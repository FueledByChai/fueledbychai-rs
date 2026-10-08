//! What the order-entry tests of fbc-oms's authorizations share (FBC-w19's `exec_held.rs`,
//! FBC-j5bw's `exec_authorized.rs`): the conformance toy's order entry as a venue factory, a
//! registry armed on the toy's market that builds and authorizes every order command
//! (FBC-afd's `Registry::authorize`, its only public path), a nonce source that logs each
//! reservation, the session's configuration, and the helpers that let a paused-clock test run.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AckLevel, AmendQty, AssetKey, CancelOnDisconnect,
    Channel, CidMatch, CidMint, ClientOrderId, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint,
    FieldSpec, HttpPlan, InstrumentSpecDraft, ItemRef, Lots, MarketLease, MdCodec, MonoNs,
    NamespaceLease, NewOrder, NonceBlock, NonceSource, OrderCaps, OrderKind, RateLimit, Secrets,
    Side, SignedLots, SpecTable, SubmitOutcome, Subscription, SymbolError, Ticks, Tif, VenueCaps,
    VenueConfig, VenueError, VenueFactory, VenueOrderSnapshot, VenueOrderState, WallNs, WireUrl,
};
use fbc_oms::{
    Authorization, CancelChoice, CancelEverything, LadderConfig, LeaseKeys, Leases,
    MarketCapsConfig, OrderKey, OrderOp, PreTradeCaps, Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Connector, ExecSessionConfig, IngestClock, ProxyConfig, RateLimiter, ReconnectPacing, RpcIds,
    SafetyReserve, WriteStall,
};

use crate::common::{Peer, ScriptedWs};
use crate::exec_toy::{self, INST_A, OWN_NS, ToyExec, ToySigner};

const URL: &str = "held.url";
pub const ACCT: AccountKey = AccountKey::new(4);
/// The registry's caps on the toy's market, in lots: wide enough for every order here.
const CAP: i64 = 1_000;

/// The conformance toy's order entry at the configured URL, declaring the toy's own rate limits
/// or, built [`Held::with_limits`], others, and, built [`Held::covering`], an arm that covers
/// the orders already open (FBC-nvxn).
pub struct Held {
    limits: Option<Vec<RateLimit>>,
    covers_open_orders: bool,
}

/// The toy as it declares itself.
pub static HELD: Held = Held {
    limits: None,
    covers_open_orders: false,
};

/// The toy declaring that an accepted cancel-on-disconnect arm covers the orders already open.
pub static COVERING: Held = Held {
    limits: None,
    covers_open_orders: true,
};

impl Held {
    /// A venue of the test's own: the toy declaring `limits` in place of its own.
    pub fn with_limits(limits: Vec<RateLimit>) -> &'static Held {
        Box::leak(Box::new(Held {
            limits: Some(limits),
            covers_open_orders: false,
        }))
    }
}

impl VenueFactory for Held {
    fn id(&self) -> &'static str {
        "TOY-HELD"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = held_caps();
        if let Some(limits) = &self.limits {
            caps.limits.clone_from(limits);
        }
        if self.covers_open_orders {
            let order = &mut caps.exec.as_mut().unwrap().order;
            order.cancel_on_disconnect = CancelOnDisconnect::PerConnection {
                rearm_on_reconnect: true,
                covers_open_orders: true,
            };
        }
        Ok(caps)
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

    /// Never built: nothing is planned for market data. The conformance toy's codec stands in.
    fn md_codec(&self, _: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(exec_toy::ToyMd::new(ep.stream))
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

/// The toy's caps, its amend stating the total quantity rather than the quantity still to
/// fill, as the toy's does, so an amend counts against the caps by its total (fbc-oms counts
/// a remaining-quantity amend by its wire quantity, decision 0064). Every amend here is of an
/// order nothing filled, which the toy's codec writes the same either way.
pub fn held_caps() -> VenueCaps {
    let mut caps = exec_toy::caps();
    let exec = caps.exec.as_mut().unwrap();
    exec.order.amend.as_mut().unwrap().qty_semantics = AmendQty::TotalIncludingFilled;
    caps
}

/// The order caps of the test's venue.
pub fn order_caps() -> OrderCaps {
    held_caps().exec.unwrap().order
}

/// The directory this test binary's leases are taken in.
pub fn lease_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("fbc-runtime-armed-oms-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// One of our client ids, each new, minted under a namespace lease of the toy's namespace.
pub fn cid() -> ClientOrderId {
    static MINT: OnceLock<Mutex<CidMint>> = OnceLock::new();
    let mint = MINT.get_or_init(|| {
        let lease = NamespaceLease::acquire(&lease_dir(), ACCT, OWN_NS).unwrap();
        Mutex::new(CidMint::new(lease, 0, 0, WallNs(1)))
    });
    mint.lock().unwrap().mint().unwrap()
}

/// The registry, the client id of its resting order `V-1`, which a resync showed, and of its
/// resting order `V-2`, which it placed itself and so may amend.
pub struct Oms {
    pub reg: Registry,
    pub resting: ClientOrderId,
    pub amendable: ClientOrderId,
}

impl Oms {
    /// A registry under lease names of an account of its own, its caps configuring the toy's
    /// market, after a trustworthy resync that showed the market flat with one resting buy of
    /// ours, `V-1`, and the owner's Start on it: armed, Quoting. It then places a buy of its own
    /// that the venue acknowledged as `V-2` (an order a resync showed is never amended): the
    /// registry is told the acknowledgement directly, as no session here wrote that order.
    pub fn armed() -> Oms {
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
    pub fn place(&mut self) -> Authorization {
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
    pub fn amend(&mut self) -> Authorization {
        let cmd = self
            .reg
            .live(self.amendable)
            .unwrap()
            .amend(&order_caps(), Ticks(130_855), Lots::new(10).unwrap(), false)
            .unwrap();
        self.reg.authorize(ACCT, cmd).unwrap()
    }

    /// A cancel of the resting `V-1`, built and authorized.
    pub fn cancel(&mut self) -> Authorization {
        let permit = self.reg.cancellable(self.resting).unwrap();
        let CancelChoice::Send(cmd) = permit.cancel(&order_caps()) else {
            panic!("a cancel of an acknowledged order is sent");
        };
        self.reg.authorize(ACCT, cmd).unwrap()
    }

    /// The kill switch's instrument cancel-all of the toy's market, built and authorized: the
    /// toy has one, the registry holds the market's exclusive lease, a resync showed the
    /// account's open orders and no order not ours is in view (0005's I7), and every order of
    /// ours on it is acknowledged, so no explicit cancel comes with it.
    pub fn cancel_all(&mut self) -> Authorization {
        let CancelEverything::CancelAll {
            command,
            unanswered,
        } = self.reg.cancel_everything(INST_A, &order_caps())
        else {
            panic!("0005's I7 guard holds");
        };
        assert!(unanswered.commands.is_empty(), "{unanswered:?}");
        self.reg.authorize(ACCT, command).unwrap()
    }

    /// A batch of `n` post-only buys of 5 lots each at successive prices, built and authorized.
    pub fn batch(&mut self, n: i64) -> Authorization {
        let orders = (0..n)
            .map(|i| NewOrder {
                cid: cid(),
                inst: INST_A,
                side: Side::Buy,
                kind: OrderKind::Limit {
                    px: Ticks(130_840 - i),
                },
                qty: Lots::new(5).unwrap(),
                tif: Tif::Gtc,
                channel: Channel::Public,
                post_only: true,
                reduce_only: false,
                reducing: false,
            })
            .collect();
        let plan = self.reg.place_batch(orders).unwrap();
        assert!(plan.refused.is_empty(), "{:?}", plan.refused);
        self.reg.authorize(ACCT, plan.command.unwrap()).unwrap()
    }
}

// ---------------------------------------------------------------------------------------------
// The session.
// ---------------------------------------------------------------------------------------------

/// Each nonce reservation the session made, in order.
pub type Reserved = Arc<Mutex<Vec<Vec<u64>>>>;

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

/// The configuration of a session of the toy at `url` for [`ACCT`], its nonces counted up from
/// 0 and each reservation logged in `reserved`.
pub fn session_config(url: &str, reserved: &Reserved) -> ExecSessionConfig {
    session_config_for(&HELD, SafetyReserve::percent(0).unwrap(), url, reserved)
}

/// [`session_config`] for `venue`, its limiter keeping `reserve` of each bucket for safety
/// traffic.
pub fn session_config_for(
    venue: &'static Held,
    reserve: SafetyReserve,
    url: &str,
    reserved: &Reserved,
) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, url);
    let limits = venue.caps(&cfg).unwrap().limits;
    ExecSessionConfig {
        venue,
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
            log: Arc::clone(reserved),
        }),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: 6,
        limiter: RateLimiter::new(&limits, reserve).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    }
}

/// Lets every task run a while without moving the clock.
pub async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets the session run, without moving the clock, until `done`.
pub async fn settle(done: impl Fn() -> bool) {
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
pub async fn reconnected(server: &mut ScriptedWs) -> Peer {
    for _ in 0..1_000 {
        tokio::time::advance(Duration::from_millis(10)).await;
        churn().await;
        if let Some(peer) = server.try_accept() {
            return peer;
        }
    }
    panic!("not reconnected");
}

/// The resync request's watermark.
pub fn watermark(resync: &str) -> String {
    resync.strip_prefix("resync|ts=").unwrap().to_owned()
}
