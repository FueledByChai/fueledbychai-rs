//! FBC-6mf's done line (decisions 0039, 0046, 0057, 0058, 0097, 0100, 0104): fbc-runtime's shard
//! host runs the runtime's toy market-data venue (included by path) over a WebSocket of the
//! test's own on 127.0.0.1, and three SimVenue order-entry sessions, one per bracket
//! (Pessimistic, Middle, Optimistic), each an ordinary `ExecSession` over an in-process stream
//! whose peer is a `SimPeer`, under a simulated account of its own whose fbc-oms registry is
//! armed on the toy's market under that account's leases from the session's own resync. One
//! decide pass submits the same authorized post-only buy to each; the market data reaches every
//! peer through the shard's `PeerFeed` in ingest order, and the brackets fill in their order:
//! Optimistic on the first trade at the order's price, Middle on the second, Pessimistic on
//! the third. Every answer and fill reaches the handlers stamped in the shard's ingest order,
//! and the planned order-entry URL is never connected to.
//!
//! The rest of the file holds the hosted stream's other paths: a market-data connection's end
//! invalidating the engine's books until the next snapshot; the greeting, and a close
//! forgetting it; an answer given on the next connection before its later greeting; what a
//! peer refuses; a run dropped mid-epoch closing the stream, so the venue cancels what its
//! per-connection protection covers, with the stream's frames journaled as a socket's; and an
//! epoch whose arm fails closing the stream, which the next epoch opens again. The venue, its
//! caps, its account names and its rates are synthetic.

#[path = "../../fbc-runtime/tests/common/toy.rs"]
#[allow(dead_code, unused_imports)]
mod toy;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::Encoding;
use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AckModel, AssetKey, BookId, BookSide, Bps,
    CancelOnDisconnect, CancelReason, Channel, CidMint, ClientIdFormat, ClientOrderId, ConfigError,
    ConnKey, ConnState, ConnTopology, EndpointPlan, Envelope, ExecCaps, ExecCodec, ExecEndpoint,
    ExecEvent, FeeBook, FeeEntry, FeeKey, FeeRate, FeeSource, Feed, FeedHealth, FeedSource,
    FieldSpec, FillCaps, FillSource, FundingCaps, HttpPlan, InstrumentId, InstrumentSpecDraft,
    Liquidity, Lots, MarketLease, MatchingCaps, MdCaps, MdCodec, MdEvent, MdTransport, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NonceScope, NonceSource, OrderCaps, OrderKind,
    OrderKindTag, OrderingKey, Readiness, RefKind, Secrets, Side, SnapshotReads, SnapshotSource,
    SpecTable, Stamp, StpScope, StreamId, SubmitOutcome, Subscription, Support, SymbolError,
    TagSet, Ticks, Tif, TifTag, TradeCaps, VenueCaps, VenueConfig, VenueError, VenueFactory,
    VenueFeeSign, VenueMeta, VenueOrderState, WallNs, WireUrl,
};
use fbc_oms::{
    Authorization, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey, PreTradeCaps,
    Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig,
    InProcessStream, IngestClock, Ladder, Liveness, MdControl, MdHandler, MdSession,
    MdSessionConfig, Member, OrderEntryPeer, PeerFeed, ProxyConfig, RateLimiter, ReconnectPacing,
    RpcIds, SafetyReserve, ShardHost, SubmitRefusal, WriteStall,
};
use fbc_sim::{Bracket, PeerCounters, QueueConfig, SimCodec, SimConfig, SimLatency, SimPeer};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// The toy's instrument `A`, its trading book channel, and the order-entry stream the stood-in
/// venue plans.
const INST: InstrumentId = InstrumentId::new(1);
const BOOK: BookId = BookId(0);
const STREAM: StreamId = StreamId(5);
const NS: Namespace = Namespace::new(9);
const MS: u64 = 1_000_000;
const TO_VENUE: Duration = Duration::from_millis(5);
const TO_CLIENT: Duration = Duration::from_millis(3);
/// The configuration key the stood-in venue's planned order-entry URL is under.
const URL: &str = "stood.url";
/// The order every bracket places: a post-only buy of two lots at the bid, behind six.
const BID: i64 = 199;
const QTY: i64 = 2;

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// The directory this test binary's leases are taken in.
fn lease_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("fbc-sim-hosted-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// One client id, minted under a namespace lease of `acct`'s, held for the test binary.
fn cid(acct: AccountKey) -> ClientOrderId {
    static MINTS: OnceLock<Mutex<BTreeMap<u16, CidMint>>> = OnceLock::new();
    let mut mints = MINTS.get_or_init(Mutex::default).lock().unwrap();
    let mint = mints.entry(acct.get()).or_insert_with(|| {
        let lease = NamespaceLease::acquire(&lease_dir(), acct, NS).unwrap();
        CidMint::new(lease, 0, 0, WallNs(1))
    });
    mint.mint().unwrap()
}

// ---------------------------------------------------------------------------------------------
// The stood-in venue: synthetic caps a SimVenue can say everything of, with per-connection
// cancel-on-disconnect, as an order-entry session requires (decision 0058).
// ---------------------------------------------------------------------------------------------

fn stood_caps() -> VenueCaps {
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit]),
                tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
                channels: TagSet::of(&[Channel::Public]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: vec![],
                amend: None,
                cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
                query_refs: TagSet::none(),
                cancel_before_ack: true,
                cancel_is_signed: false,
                batch_place: None,
                batch_cancel: None,
                cancel_all_account: Support::Unsupported,
                cancel_all_instrument: Support::Unsupported,
                cancel_on_disconnect: CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: true,
                    covers_open_orders: false,
                },
                ack: AckModel::SinglePhase,
                client_id: ClientIdFormat::Uuid,
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::None,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                snapshot_reads: SnapshotReads::OneInstant,
                // A synthetic snapshot, answered from the stub's own state: it never lags.
                snapshot_lag_bound: Duration::ZERO,
                events_echo_flags: true,
                sign_cost_hint_us: 0,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: false,
                realized_funding: false,
                fee_sign: VenueFeeSign::PositiveIsCost,
                fee_asset_reported: true,
                fill_id: true,
                replays_fills_on_reconnect: false,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Text,
            touch_sources: vec![],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: true,
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::PerInstrument,
            max_conn_lifetime: None,
        },
        limits: vec![],
        readiness_ceiling: Readiness::Paper,
    }
}

/// The venue SimVenue stands in for: its caps, and one order-entry endpoint at the configured
/// URL, which an in-process session never opens. It builds no codec of its own: the session is
/// given SimVenue's.
struct Stood;

static STOOD: Stood = Stood;

impl VenueFactory for Stood {
    fn id(&self) -> &'static str {
        "SIM-STOOD"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(stood_caps())
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
        unreachable!("nothing is planned for market data")
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(vec![ExecEndpoint {
            stream: STREAM,
            url: WireUrl::plain(cfg.get(URL).unwrap()),
            handshake: Vec::new(),
        }])
    }

    fn exec_codec(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

/// The three brackets, in the order the test names them.
const BRACKETS: [Bracket; 3] = [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic];

/// Bracket `i`'s simulated account.
fn account(i: usize) -> AccountKey {
    AccountKey::new(20 + u16::try_from(i).unwrap())
}

/// A name for bracket `i`'s simulated account, which its leases are taken on: each new, so the
/// tests of this binary, which run at once, never contend for one.
fn account_name(i: usize) -> String {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("sim-{n}-{:?}", BRACKETS[i]).to_lowercase()
}

/// A maker rebate and a taker fee on the toy's instrument for `acct`.
fn fees(acct: AccountKey) -> FeeBook {
    let mut book = FeeBook::new();
    for (liquidity, bps) in [(Liquidity::Maker, -0.5), (Liquidity::Taker, 2.0)] {
        let key = FeeKey {
            account: acct,
            instrument: INST,
            channel: Channel::Public,
            liquidity,
        };
        let entry = FeeEntry {
            rate: FeeRate(Bps(bps)),
            source: FeeSource::ConfigOverride,
            as_of: WallNs(0),
        };
        book.insert(key, entry);
    }
    book
}

/// SimVenue for bracket `i`, standing in for the venue under its own account.
fn sim_config(i: usize) -> SimConfig {
    let caps = stood_caps();
    SimConfig {
        exec: caps.exec.unwrap(),
        matching: caps.matching,
        latency: SimLatency {
            to_venue: TO_VENUE,
            to_client: TO_CLIENT,
        },
        rpc_timeout: Duration::from_secs(2),
        stream: STREAM,
        queue: QueueConfig {
            bracket: BRACKETS[i],
        },
        account: account(i),
        fees: fees(account(i)),
        specs: toy::specs(),
        books: BTreeMap::from([(INST, BOOK)]),
    }
}

/// Nonces counted up from 0.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// An order-entry session's configuration for bracket `i`, on connection `conn` of the shard,
/// whose planned order-entry URL is `url`.
fn exec_config(i: usize, url: &str, clock: &IngestClock, conn: u16) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, url);
    ExecSessionConfig {
        venue: &STOOD,
        cfg,
        creds: Secrets::new(),
        acct: account(i),
        account_wide_cod_authority: None,
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: clock.clone(),
        nonces: Box::new(Counting(0)),
        nonce_source: fbc_journal::NonceSourceId(u32::try_from(i).unwrap()),
        conn,
        limiter: RateLimiter::new(&[], SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 1024,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    }
}

// ---------------------------------------------------------------------------------------------
// fbc-oms: a registry per bracket, armed from the session's own resync.
// ---------------------------------------------------------------------------------------------

/// One bracket's registry, and its resync as the session's events build it.
struct Oms {
    acct: AccountKey,
    name: String,
    reg: Registry,
    /// The shard clock's reading when the stream was reported authenticated: the resync was
    /// asked after it.
    authenticated_at: Option<MonoNs>,
    collecting: Option<ResyncSnapshot>,
    /// Whether the owner's Start armed the market, Quoting.
    armed: bool,
}

impl Oms {
    fn new(i: usize) -> Oms {
        let caps = stood_caps().exec.unwrap().order;
        let name = account_name(i);
        let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
        let keys = LeaseKeys::new("sim", &name, &caps).with_market(INST, symbol);
        let limits = MarketCapsConfig {
            inventory: Some(lots(100)),
            resting: Some(lots(100)),
        };
        let pre_trade = PreTradeCaps::new().with_market(INST, limits).unwrap();
        Oms {
            acct: account(i),
            name,
            reg: Registry::with_caps(pre_trade).with_lease_keys(keys),
            authenticated_at: None,
            collecting: None,
            armed: false,
        }
    }

    /// Takes one of the session's events: a resync is collected, and once it ends the market
    /// is resynced and the owner's Start arms it under the account's market lease.
    fn on_exec(&mut self, env: &Envelope<ExecEvent>) {
        let now = env.stamp.recv_mono;
        match &env.body {
            ExecEvent::Conn {
                state: ConnState::Authenticated,
                ..
            } => self.authenticated_at = Some(now),
            ExecEvent::ResyncBegin { watermark } => {
                self.collecting = Some(ResyncSnapshot {
                    watermark: *watermark,
                    requested_at: self.authenticated_at.unwrap_or(now),
                    orders: Vec::new(),
                    positions: Vec::new(),
                });
            }
            ExecEvent::ResyncOrder(o) => self.collecting.as_mut().unwrap().orders.push(o.clone()),
            ExecEvent::ResyncPosition { inst, qty, .. } => {
                self.collecting
                    .as_mut()
                    .unwrap()
                    .positions
                    .push((*inst, *qty));
            }
            ExecEvent::ResyncEnd => {
                let snap = self.collecting.take().unwrap();
                let caps = stood_caps().exec.unwrap().order;
                let ladder = LadderConfig::new(
                    Duration::from_secs(1),
                    Duration::ZERO,
                    Duration::from_secs(10),
                    1,
                )
                .unwrap();
                let key = OrderKey {
                    venue: env.venue_seq,
                    ingest: env.stamp.ingest_seq,
                };
                let report = self.reg.resync(&ladder, &caps, &snap, key).unwrap();
                assert!(!report.untrustworthy);
                if !self.armed {
                    let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
                    let dir = lease_dir();
                    let market = MarketLease::acquire(&dir, "sim", &self.name, &symbol).unwrap();
                    let acct = AccountLease::acquire(&dir, "sim", &self.name).unwrap();
                    let leases = Leases::market(market).with_account(acct);
                    self.reg.start(INST, leases).unwrap();
                    self.armed = true;
                }
            }
            _ => {}
        }
    }

    /// The order every bracket places, built and authorized for this bracket's account.
    fn place(&mut self) -> Authorization {
        let order = NewOrder {
            cid: cid(self.acct),
            inst: INST,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(BID) },
            qty: lots(QTY),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cmd = self.reg.place(order).unwrap();
        self.reg.authorize(self.acct, cmd).unwrap()
    }
}

// ---------------------------------------------------------------------------------------------
// The consumer: its handlers and decide pass, sharing one world.
// ---------------------------------------------------------------------------------------------

/// What reached a handler, in the order it reached any of them.
#[derive(Clone, Debug)]
enum Seen {
    Md(Stamp, MdEvent),
    Exec(usize, Stamp, ExecEvent),
}

impl Seen {
    fn stamp(&self) -> Stamp {
        match self {
            Seen::Md(stamp, _) | Seen::Exec(_, stamp, _) => *stamp,
        }
    }
}

struct World {
    seen: Vec<Seen>,
    oms: Vec<Oms>,
    /// Whether the trading book's first snapshot reached the market-data handler.
    book: bool,
    /// Whether the decide pass placed the order on every bracket.
    placed: bool,
}

type Shared = Rc<RefCell<World>>;

impl World {
    /// Bracket `i`'s fills, each with its stamp: lots filled.
    fn fills(&self, i: usize) -> Vec<(Stamp, i64)> {
        self.seen
            .iter()
            .filter_map(|seen| match seen {
                Seen::Exec(b, stamp, ExecEvent::Fill(f)) if *b == i => Some((*stamp, f.qty.get())),
                _ => None,
            })
            .collect()
    }

    fn filled(&self, i: usize) -> i64 {
        self.fills(i).iter().map(|(_, qty)| qty).sum()
    }

    /// Whether every bracket's venue accepted the order.
    fn accepted(&self) -> bool {
        (0..self.oms.len()).all(|i| {
            self.seen.iter().any(|seen| {
                matches!(
                    seen,
                    Seen::Exec(b, _, ExecEvent::Outcome {
                        item: Some(_),
                        outcome: SubmitOutcome::Accepted { .. },
                        ..
                    }) if *b == i
                )
            })
        })
    }
}

struct MdLog(Shared);

impl MdHandler for MdLog {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        let mut world = self.0.borrow_mut();
        if matches!(env.body, MdEvent::BookSnapshotEnd { .. }) {
            world.book = true;
        }
        world.seen.push(Seen::Md(env.stamp, env.body));
    }
}

struct ExecLog(Shared, usize);

impl ExecHandler for ExecLog {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut world = self.0.borrow_mut();
        world.oms[self.1].on_exec(&env);
        world.seen.push(Seen::Exec(self.1, env.stamp, env.body));
    }
}

/// A market-data server for one connection on a 127.0.0.1 ephemeral port: it sends each text
/// the test hands it, and reads (and drops) what the session sends. Its URL, and where the
/// test hands it texts.
async fn md_server() -> (String, mpsc::UnboundedSender<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/md", listener.local_addr().unwrap());
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        loop {
            tokio::select! {
                text = rx.recv() => match text {
                    Some(text) => ws.send(Message::text(text)).await.unwrap(),
                    None => return,
                },
                read = ws.next() => if read.is_none() {
                    return;
                },
            }
        }
    });
    (url, tx)
}

/// Lets the shard run, the paused clock moving on in steps of a millisecond, until `done`.
async fn until(world: &Shared, done: impl Fn(&World) -> bool) {
    for _ in 0..10_000 {
        if done(&world.borrow()) {
            return;
        }
        tokio::time::sleep(ms(1)).await;
    }
    panic!("not reached");
}

/// The toy's records for instrument `A`'s book channel 0, numbered from `seq`.
fn book_record(kind: &str, seq: i64, rest: &str) -> String {
    format!("{kind}|sym=A|book=0|seq={seq}{rest}")
}

fn trade(px: i64, qty: i64, seq: i64) -> String {
    format!("trade|sym=A|px={px}|qty={qty}|seq={seq}")
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn three_brackets_on_one_shard_fill_optimistic_then_middle_then_pessimistic() {
    let clock = IngestClock::new();
    // The stood-in venue's planned order-entry URL: a listener nothing may connect to.
    let planned = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    planned.set_nonblocking(true).unwrap();
    let planned_url = format!("ws://{}/exec", planned.local_addr().unwrap());

    let world: Shared = Rc::new(RefCell::new(World {
        seen: Vec::new(),
        oms: (0..BRACKETS.len()).map(Oms::new).collect(),
        book: false,
        placed: false,
    }));

    // One SimPeer per bracket, each the venue end of its session's in-process stream.
    let peers: Vec<Rc<RefCell<SimPeer>>> = (0..BRACKETS.len())
        .map(|i| Rc::new(RefCell::new(SimPeer::new(&sim_config(i)))))
        .collect();
    let streams: Vec<InProcessStream> = peers
        .iter()
        .map(|p| InProcessStream::new(Rc::clone(p)))
        .collect();
    let mut sessions = Vec::new();
    let mut exec_controls: Vec<ExecControl> = Vec::new();
    for (i, stream) in streams.iter().enumerate() {
        let config = exec_config(i, &planned_url, &clock, 2 + i as u16);
        let codec = Box::new(SimCodec::new(&sim_config(i)));
        let handler = ExecLog(Rc::clone(&world), i);
        let (session, control) =
            ExecSession::in_process(config, codec, stream.clone(), handler).unwrap();
        sessions.push(session);
        exec_controls.push(control);
    }
    let orders: Vec<ExecOrders> = sessions.iter().map(ExecSession::orders).collect();

    // The toy's market data: trades and book channel 0 of `A`, fed to every peer first.
    let (md_url, md) = md_server().await;
    let venue = toy::ToyVenue::leak();
    let md_config = MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(&md_url),
            },
            subs: vec![toy::sub(1), toy::book(1, BOOK)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: clock.clone(),
        http_max_body: 1024,
        conn: 1,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let feed = PeerFeed::new(streams.clone(), MdLog(Rc::clone(&world)));
    let (mut md_session, md_control): (_, MdControl) = MdSession::new(md_config, feed).unwrap();

    let mut host = ShardHost::new(clock.clone());
    let decide_world = Rc::clone(&world);
    let mut decide = move |_: &[InstrumentId], ladder: &mut Ladder| {
        let mut world = decide_world.borrow_mut();
        let ready = world.book && world.oms.iter().all(|o| o.armed);
        if ready && !world.placed {
            world.placed = true;
            // A session takes only its own account's authorizations, as the ladder does.
            let stray = world.oms[1].place();
            let refused = orders[0].submit(stray);
            assert!(
                matches!(refused, Err(SubmitRefusal::OtherAccount)),
                "{refused:?}"
            );
            let stray = world.oms[1].place();
            assert!(ladder.push(&orders[0], stray).is_err());
            for (i, orders) in orders.iter().enumerate() {
                let auth = world.oms[i].place();
                ladder.push(orders, auth).unwrap();
            }
        }
    };

    let mut members = vec![Member::md(&mut md_session)];
    members.extend(sessions.iter_mut().map(Member::exec));
    let run = host.run(members, &mut decide);

    // What the test learns between trades: each bracket's lots filled after each trade.
    let mut after = Vec::new();
    let drive = async {
        // The trading book: bids 199 x 6, asks 201 x 5.
        for text in [
            book_record("begin", 1, "|epoch=1"),
            book_record("lvl", 2, "|side=bid|px=199|qty=6"),
            book_record("lvl", 3, "|side=ask|px=201|qty=5"),
            book_record("end", 4, ""),
        ] {
            md.send(text).unwrap();
        }
        // Every bracket armed from its own resync, the book shown, and the order resting.
        until(&world, |w| w.placed && w.accepted()).await;
        // The level shrinks by three with no trade: none, two (floor(3 x 6 / 8)) or three of
        // the six ahead of each bracket's order left, then three trades at its price.
        md.send(book_record("lvl", 5, "|side=bid|px=199|qty=3"))
            .unwrap();
        for (n, qty) in [4, 2, 2].into_iter().enumerate() {
            md.send(trade(BID, qty, 10 + n as i64)).unwrap();
            let first = [2, 1, 0][n];
            until(&world, |w| w.filled(first) > 0).await;
            // Every answer the trade brought is due well within this.
            tokio::time::sleep(ms(50)).await;
            let world = world.borrow();
            after.push([0, 1, 2].map(|i| world.filled(i)));
        }
        drop(exec_controls);
        drop(md_control);
    };
    let (ran, ()) = tokio::join!(run, drive);
    ran.unwrap();

    // Pessimistic, Middle, Optimistic: after the first trade, the second and the third.
    assert_eq!(after, [[0, 0, 1], [0, 2, 2], [2, 2, 2]]);
    let world = world.borrow();
    let first_fill = |i: usize| world.fills(i)[0].0.ingest_seq;
    let (pess, mid, opt) = (first_fill(0), first_fill(1), first_fill(2));
    assert!(opt < mid && mid < pess, "{opt} {mid} {pess}");
    // Each bracket's first fill came after the trade that made it, and before the next.
    let trades_before = |seq: u64| {
        let trades = world.seen.iter().filter(
            |s| matches!(s, Seen::Md(stamp, MdEvent::Trade { .. }) if stamp.ingest_seq < seq),
        );
        trades.count()
    };
    assert_eq!([opt, mid, pess].map(trades_before), [1, 2, 3]);
    // Every fill came on its own session's connection.
    for (i, bracket) in BRACKETS.iter().enumerate() {
        for (stamp, _) in world.fills(i) {
            assert_eq!(stamp.conn.conn, 2 + i as u16, "{bracket:?}");
        }
    }
    // Every input reached the handlers in the shard's ingest order, once.
    let seqs: Vec<u64> = world.seen.iter().map(|s| s.stamp().ingest_seq).collect();
    let mut deduped = seqs.clone();
    deduped.dedup();
    assert!(deduped.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
    // Each bracket's session heard its stream authenticated, armed and resynced, then its
    // order accepted, all from the simulated venue.
    for i in 0..BRACKETS.len() {
        let events: Vec<&ExecEvent> = world
            .seen
            .iter()
            .filter_map(|s| match s {
                Seen::Exec(b, _, ev) if *b == i => Some(ev),
                _ => None,
            })
            .collect();
        assert!(matches!(
            events[0],
            ExecEvent::Conn {
                stream: STREAM,
                state: ConnState::Authenticated
            }
        ));
        assert!(events.iter().any(|e| matches!(e, ExecEvent::ResyncEnd)));
        let order_states: Vec<VenueOrderState> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::Order(u) => Some(u.state.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(order_states.last(), Some(&VenueOrderState::Filled), "{i}");
        assert_eq!(world.filled(i), QTY);
    }
    // No order-entry socket: the planned URL was never connected to, and every peer took every
    // frame and envelope it was handed.
    let accepted = planned.accept();
    assert!(
        matches!(&accepted, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "{accepted:?}"
    );
    for peer in &peers {
        assert_eq!(peer.borrow().counters(), PeerCounters::default());
    }
}

// ---------------------------------------------------------------------------------------------
// A peer fed by hand, as the runtime would feed it.
// ---------------------------------------------------------------------------------------------

/// A SimPeer and SimCodec standing in for the venue, driven by hand.
struct Hand {
    peer: Rc<RefCell<SimPeer>>,
    feed: PeerFeed<Kept>,
    seq: u64,
}

/// Every envelope the consumer's handler was handed, the epoch ends and the tick-to-wire
/// samples it was told.
#[derive(Default)]
struct Kept(
    Vec<Envelope<MdEvent>>,
    Vec<ConnKey>,
    Vec<fbc_runtime::TickToWire>,
);

impl MdHandler for Kept {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.push(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.1.push(key);
    }

    fn on_tick_to_wire(&mut self, sample: fbc_runtime::TickToWire) {
        self.2.push(sample);
    }
}

impl Hand {
    fn new() -> Hand {
        let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(2))));
        let stream = InProcessStream::new(Rc::clone(&peer));
        assert_eq!(format!("{stream:?}"), "InProcessStream");
        Hand {
            peer,
            feed: PeerFeed::new(vec![stream], Kept::default()),
            seq: 0,
        }
    }

    fn md(&mut self, conn: ConnKey, mono: u64, body: MdEvent) {
        let stamp = Stamp {
            ingest_seq: self.seq,
            kernel_rx: None,
            recv_mono: MonoNs(mono),
            recv_wall: WallNs(1_760_000_000_000_000_000 + mono as i64),
            conn,
        };
        self.seq += 1;
        self.feed.on_md(Envelope::new(stamp, VenueMeta::NONE, body));
    }

    fn snapshot(&mut self, conn: ConnKey, mono: u64) {
        let begin = MdEvent::BookSnapshotBegin {
            inst: INST,
            book: BOOK,
            epoch: conn.epoch,
        };
        self.md(conn, mono, begin);
        for (side, px, qty) in [(BookSide::Bid, 199, 6), (BookSide::Ask, 201, 5)] {
            let level = MdEvent::Level {
                inst: INST,
                book: BOOK,
                side,
                px: Ticks(px),
                qty: lots(qty),
            };
            self.md(conn, mono, level);
        }
        self.md(
            conn,
            mono,
            MdEvent::BookSnapshotEnd {
                inst: INST,
                book: BOOK,
            },
        );
    }

    /// Hands the peer the frame `codec` writes for an immediate-or-cancel buy crossing the ask,
    /// as request `rpc` at `mono`.
    fn cross(&mut self, codec: &mut SimCodec, rpc: u64, mono: u64) {
        use fbc_core::{Effect, Effects, EncodeCtx, PathStamps, RpcId, VenueCommand};
        let cmd = VenueCommand::Place(NewOrder {
            cid: cid(account(2)),
            inst: INST,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(201) },
            qty: lots(1),
            tif: Tif::Ioc,
            channel: Channel::Public,
            post_only: false,
            reduce_only: false,
            reducing: false,
        });
        let ctx = EncodeCtx {
            wall: WallNs(1_760_000_000_000_000_000 + mono as i64),
            mono: MonoNs(mono),
            nonces: NonceBlock::EMPTY,
        };
        let mut fx = Effects::new();
        let t = &mut PathStamps::off();
        codec
            .encode(&cmd, RpcId(rpc), &toy::specs(), &ctx, t, &mut fx)
            .unwrap();
        for effect in fx.as_slice() {
            let Effect::Send { frame, .. } = effect else {
                panic!("{effect:?}");
            };
            self.peer.borrow_mut().on_frame(frame.bytes());
        }
    }

    /// Decodes every frame the peer gives by `mono` through `codec`.
    fn answers(&mut self, codec: &mut SimCodec, mono: u64) -> Vec<ExecEvent> {
        let frames = self.peer.borrow_mut().take_due(MonoNs(mono));
        decode(codec, frames)
    }
}

/// `frames` decoded through `codec`, as a session would.
fn decode(codec: &mut SimCodec, frames: Vec<Vec<u8>>) -> Vec<ExecEvent> {
    use fbc_core::{Effects, ExecSink, RawFrame, dispatch};
    #[derive(Default)]
    struct Sink(Vec<ExecEvent>);
    impl ExecSink for Sink {
        fn push(&mut self, _: VenueMeta, ev: ExecEvent) {
            self.0.push(ev);
        }
    }
    let mut sink = Sink::default();
    for frame in frames {
        let mut fx = Effects::new();
        let specs = toy::specs();
        dispatch(&stood_caps(), NS, |scope| {
            let text = RawFrame::Text(std::str::from_utf8(&frame).unwrap());
            codec.on_frame(STREAM, text, scope, &specs, &mut sink, &mut fx)
        })
        .unwrap();
    }
    sink.0
}

/// Whether `events` hold a fill, or an outcome refusing request `rpc`.
fn filled(events: &[ExecEvent]) -> bool {
    events.iter().any(|e| matches!(e, ExecEvent::Fill(_)))
}

fn refused(events: &[ExecEvent]) -> bool {
    events.iter().any(|e| {
        matches!(
            e,
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Rejected { .. },
                ..
            }
        )
    })
}

#[test]
fn a_dropped_market_data_connection_invalidates_the_engines_books_until_the_next_snapshot() {
    let mut hand = Hand::new();
    let mut codec = SimCodec::new(&sim_config(2));
    let first = ConnKey { conn: 1, epoch: 1 };
    let other = ConnKey { conn: 3, epoch: 1 };
    hand.peer.borrow_mut().open(MonoNs(0), WallNs(0));
    assert!(matches!(
        hand.answers(&mut codec, 3 * MS).as_slice(),
        [ExecEvent::Conn {
            state: ConnState::Authenticated,
            ..
        }]
    ));
    hand.snapshot(first, 4 * MS);
    hand.cross(&mut codec, 1, 5 * MS);
    hand.md(
        first,
        12 * MS,
        MdEvent::Health {
            inst: INST,
            feed: Feed::Trades,
            h: FeedHealth::Live,
        },
    );
    let crossed = hand.answers(&mut codec, 13 * MS);
    assert!(filled(&crossed), "{crossed:?}");

    // Another connection's end leaves the book as it is.
    hand.feed.on_epoch_end(other);
    hand.cross(&mut codec, 2, 20 * MS);
    hand.peer.borrow_mut().take_due(MonoNs(25 * MS));
    assert!(filled(&hand.answers(&mut codec, 28 * MS)));

    // The feeding connection's end: nothing crosses the gapped book until its next snapshot.
    hand.feed.on_epoch_end(first);
    hand.cross(&mut codec, 3, 30 * MS);
    let gapped = hand.answers(&mut codec, 38 * MS);
    assert!(refused(&gapped) && !filled(&gapped), "{gapped:?}");
    let next = ConnKey { conn: 1, epoch: 2 };
    hand.snapshot(next, 40 * MS);
    hand.cross(&mut codec, 4, 41 * MS);
    let again = hand.answers(&mut codec, 49 * MS);
    assert!(filled(&again), "{again:?}");
    // The consumer's handler saw every envelope and epoch end the feed was handed, and what
    // else its session tells it.
    assert_eq!(hand.feed.handler().0.len(), 9);
    assert_eq!(hand.feed.handler().1, [other, first]);
    let sample = fbc_runtime::TickToWire {
        stream: toy::STREAM,
        frame: hand.feed.handler().0[0].stamp,
        nanos: 7,
    };
    hand.feed.on_tick_to_wire(sample);
    assert_eq!(hand.feed.handler().2, [sample]);
    assert_eq!(hand.peer.borrow().counters(), PeerCounters::default());
}

#[test]
fn the_greeting_waits_for_the_latency_and_a_close_forgets_it() {
    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(0))));
    let mut p = peer.borrow_mut();
    assert_eq!(p.next_due(), None);
    p.open(MonoNs(10 * MS), WallNs(0));
    assert_eq!(p.next_due(), Some(MonoNs(13 * MS)));
    assert!(p.take_due(MonoNs(12 * MS)).is_empty());
    assert_eq!(p.take_due(MonoNs(13 * MS)).len(), 1);
    assert_eq!(p.next_due(), None);
    p.close(MonoNs(14 * MS), WallNs(0));
    p.open(MonoNs(20 * MS), WallNs(0));
    p.close(MonoNs(21 * MS), WallNs(0));
    assert_eq!(p.next_due(), None);
    assert!(p.take_due(MonoNs(30 * MS)).is_empty());
    assert_eq!(p.counters(), PeerCounters::default());
}

#[test]
fn a_peer_counts_what_its_engine_refuses() {
    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(0))));
    let mut p = peer.borrow_mut();
    // Closed: a frame, and a close.
    p.on_frame(b"garbage");
    p.close(MonoNs(1), WallNs(1));
    p.open(MonoNs(2), WallNs(2));
    p.open(MonoNs(3), WallNs(3));
    // A snapshot end with no snapshot begun.
    let stamp = Stamp {
        ingest_seq: 0,
        kernel_rx: None,
        recv_mono: MonoNs(4),
        recv_wall: WallNs(4),
        conn: ConnKey { conn: 1, epoch: 1 },
    };
    let end = MdEvent::BookSnapshotEnd {
        inst: INST,
        book: BOOK,
    };
    p.on_market(&Envelope::new(stamp, VenueMeta::NONE, end));
    assert_eq!(
        p.counters(),
        PeerCounters {
            refused_frames: 1,
            refused_envelopes: 1,
            refused_lifecycle: 2,
        }
    );
    assert_eq!(p.engine().next_due(), None);
    assert_eq!(p.engine_mut().next_due(), None);
}

#[test]
fn an_answer_due_on_the_next_connection_goes_before_its_later_greeting() {
    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(0))));
    let mut p = peer.borrow_mut();
    let mut codec = SimCodec::new(&sim_config(0));
    p.open(MonoNs(0), WallNs(0));
    assert_eq!(p.take_due(MonoNs(3 * MS)).len(), 1);
    // A resync asked at 4 ms acts at 9 and is answered at 12; the stream closes at 10 and
    // opens again at 11, its greeting due at 14.
    use fbc_core::{Effect, Effects, EncodeCtx, ExecCodec};
    let mut fx = Effects::new();
    let ctx = EncodeCtx {
        wall: WallNs(4 * MS as i64),
        mono: MonoNs(4 * MS),
        nonces: NonceBlock::EMPTY,
    };
    codec.resync(&ctx, &mut fx);
    let Some(Effect::Send { frame, .. }) = fx.as_slice().first() else {
        panic!("{fx:?}");
    };
    p.on_frame(frame.bytes());
    p.close(MonoNs(10 * MS), WallNs(10 * MS as i64));
    p.open(MonoNs(11 * MS), WallNs(11 * MS as i64));
    assert_eq!(p.next_due(), Some(MonoNs(12 * MS)));
    let due = p.take_due(MonoNs(20 * MS));
    assert_eq!(due.len(), 2);
    assert!(
        due[0].starts_with(b"resync"),
        "{:?}",
        String::from_utf8_lossy(&due[0])
    );
    assert!(
        due[1].starts_with(b"ready"),
        "{:?}",
        String::from_utf8_lossy(&due[1])
    );
}

// ---------------------------------------------------------------------------------------------
// One session alone, its market data fed by hand at the shard clock's readings.
// ---------------------------------------------------------------------------------------------

/// The shard clock's monotonic reading, the clock built at `start` on a paused runtime.
fn mono_since(start: tokio::time::Instant) -> MonoNs {
    MonoNs(u64::try_from(start.elapsed().as_nanos()).unwrap())
}

/// The toy's book for `A` at the shard clock's reading, handed through `feed`.
fn feed_snapshot(feed: &mut PeerFeed<MdLog>, start: tokio::time::Instant) {
    let conn = ConnKey { conn: 1, epoch: 1 };
    let stamp = Stamp {
        ingest_seq: 0,
        kernel_rx: None,
        recv_mono: mono_since(start),
        recv_wall: WallNs(1_760_000_000_000_000_000),
        conn,
    };
    let begin = MdEvent::BookSnapshotBegin {
        inst: INST,
        book: BOOK,
        epoch: 1,
    };
    let bid = MdEvent::Level {
        inst: INST,
        book: BOOK,
        side: BookSide::Bid,
        px: Ticks(BID),
        qty: lots(6),
    };
    let end = MdEvent::BookSnapshotEnd {
        inst: INST,
        book: BOOK,
    };
    for body in [begin, bid, end] {
        feed.on_md(Envelope::new(stamp, VenueMeta::NONE, body));
    }
}

/// The epochs a session's handler was told ended, beside the world its log keeps.
struct Ends(Shared, Rc<RefCell<Vec<ConnKey>>>);

impl ExecHandler for Ends {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        ExecLog(Rc::clone(&self.0), 0).on_exec(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.1.borrow_mut().push(key);
    }
}

fn solo_world() -> Shared {
    Rc::new(RefCell::new(World {
        seen: Vec::new(),
        oms: vec![Oms::new(0)],
        book: false,
        placed: false,
    }))
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_run_dropped_mid_epoch_closes_its_stream_and_the_venue_cancels_what_it_protects() {
    let clock = IngestClock::new();
    let start = tokio::time::Instant::now();
    let world = solo_world();
    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(0))));
    let stream = InProcessStream::new(Rc::clone(&peer));
    let config = exec_config(0, "ws://127.0.0.1:9/never", &clock, 2);
    let codec = Box::new(SimCodec::new(&sim_config(0)));
    let ends = Rc::new(RefCell::new(Vec::new()));
    let handler = Ends(Rc::clone(&world), Rc::clone(&ends));
    let (mut session, _control) =
        ExecSession::in_process(config, codec, stream.clone(), handler).unwrap();
    let journal = Rc::new(RefCell::new(Records::default()));
    session.set_journal(fbc_runtime::Journal::new(Rc::clone(&journal)));
    let orders = session.orders();
    let mut feed = PeerFeed::new(vec![stream], MdLog(Rc::clone(&world)));
    {
        let run = session.run();
        let drive = async {
            feed_snapshot(&mut feed, start);
            until(&world, |w| w.oms[0].armed).await;
            let auth = world.borrow_mut().oms[0].place();
            orders.submit(auth).unwrap();
            until(&world, World::accepted).await;
        };
        tokio::select! {
            ran = run => panic!("the run ended: {ran:?}"),
            () = drive => {}
        }
    }
    // The run was dropped with its epoch open: the stream closed then, and the venue cancelled
    // the order its per-connection protection covered, answered for the next connection.
    let frames = peer.borrow_mut().take_due(MonoNs(u64::MAX));
    let mut codec = SimCodec::new(&sim_config(0));
    let events = decode(&mut codec, frames);
    assert!(
        events.iter().any(|e| matches!(
            e,
            ExecEvent::Order(u) if u.state == VenueOrderState::Canceled(CancelReason::Venue)
        )),
        "{events:?}"
    );
    assert_eq!(peer.borrow().counters(), PeerCounters::default());
    // The handler is told the epoch ended once the session that ran it is dropped.
    assert!(ends.borrow().is_empty());
    drop(session);
    assert_eq!(ends.borrow().len(), 1);
    // The stream's frames are journaled as a socket's are (decision 0006): each one the peer
    // answered with at the stamp its events reached the handler with, under Safety, and each
    // the codec wrote, the arm, the resync and the place, on the session's connection.
    let records = &journal.borrow().0;
    let inbound: BTreeSet<u64> = records
        .iter()
        .filter_map(|(class, r)| match r {
            fbc_journal::Record::Inbound { stamp, .. } => {
                assert_eq!(*class, fbc_core::TrafficClass::Safety);
                Some(stamp.ingest_seq)
            }
            _ => None,
        })
        .collect();
    let heard: BTreeSet<u64> = world
        .borrow()
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::Exec(_, stamp, _) => Some(stamp.ingest_seq),
            Seen::Md(..) => None,
        })
        .collect();
    assert_eq!(inbound, heard);
    let written = records
        .iter()
        .filter(|(_, r)| matches!(r, fbc_journal::Record::Outbound { conn, .. } if conn.conn == 2));
    assert_eq!(written.count(), 3);
    let controls = records
        .iter()
        .filter(|(_, r)| matches!(r, fbc_journal::Record::Control { .. }));
    assert_eq!(controls.count(), 2, "the epoch opened and closed");
}

/// Every record a journal was offered, with its class.
#[derive(Default)]
struct Records(Vec<(fbc_core::TrafficClass, fbc_journal::Record)>);

impl fbc_journal::JournalSink for Records {
    fn record(
        &mut self,
        class: fbc_core::TrafficClass,
        _: WallNs,
        record: &fbc_journal::Record,
    ) -> fbc_journal::Recorded {
        self.0.push((class, record.clone()));
        fbc_journal::Recorded::Ok
    }

    fn omit(&mut self, _: fbc_core::TrafficClass, _: WallNs) -> fbc_journal::Recorded {
        fbc_journal::Recorded::Ok
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_epoch_whose_arm_fails_closes_its_stream_and_the_next_opens_it_again() {
    // SimVenue standing in for a venue that cancels on no disconnect, which the session's caps
    // say it does: the codec refuses the arm, so each epoch fails and the next reconnects.
    let mut unprotected = sim_config(0);
    unprotected.exec.order.cancel_on_disconnect = CancelOnDisconnect::None;
    let clock = IngestClock::new();
    let world = solo_world();
    let peer = Rc::new(RefCell::new(SimPeer::new(&unprotected)));
    let stream = InProcessStream::new(Rc::clone(&peer));
    let config = exec_config(0, "ws://127.0.0.1:9/never", &clock, 2);
    let codec = Box::new(SimCodec::new(&unprotected));
    let ends = Rc::new(RefCell::new(Vec::new()));
    let handler = Ends(Rc::clone(&world), Rc::clone(&ends));
    let (mut session, control) = ExecSession::in_process(config, codec, stream, handler).unwrap();
    let run = session.run();
    let drive = async {
        for _ in 0..10_000 {
            if ends.borrow().len() >= 3 {
                break;
            }
            tokio::time::sleep(ms(1)).await;
        }
        drop(control);
    };
    let (ran, ()) = tokio::join!(run, drive);
    ran.unwrap();
    assert!(session.counters().arm_failures >= 3);
    assert_eq!(session.counters().failed_attempts, 0);
    // Every open was matched by a close, and every epoch heard its stream authenticated.
    assert_eq!(peer.borrow().counters(), PeerCounters::default());
    let authenticated = world
        .borrow()
        .seen
        .iter()
        .filter(|s| {
            matches!(
                s,
                Seen::Exec(
                    _,
                    _,
                    ExecEvent::Conn {
                        state: ConnState::Authenticated,
                        ..
                    }
                )
            )
        })
        .count();
    assert!(authenticated >= 3, "{authenticated}");
    let epochs: Vec<u32> = ends.borrow().iter().map(|k| k.epoch).collect();
    assert!(epochs.windows(2).all(|w| w[0] < w[1]), "{epochs:?}");
}

#[test]
fn a_greeting_due_before_an_answer_goes_first() {
    use fbc_core::{Effect, Effects, EncodeCtx, ExecCodec};
    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config(0))));
    let mut p = peer.borrow_mut();
    let mut codec = SimCodec::new(&sim_config(0));
    // Open at 0, its greeting due at 3; a resync asked at 0 acts at 5 and is answered at 8.
    p.open(MonoNs(0), WallNs(0));
    let mut fx = Effects::new();
    let ctx = EncodeCtx {
        wall: WallNs(0),
        mono: MonoNs(0),
        nonces: NonceBlock::EMPTY,
    };
    codec.resync(&ctx, &mut fx);
    let Some(Effect::Send { frame, .. }) = fx.as_slice().first() else {
        panic!("{fx:?}");
    };
    p.on_frame(frame.bytes());
    let due = p.take_due(MonoNs(10 * MS));
    assert_eq!(due.len(), 2);
    assert!(due[0].starts_with(b"ready"));
    assert!(due[1].starts_with(b"resync"));
}

/// A SimPeer that also answers, once, with a frame that is not UTF-8.
struct Noisy(SimPeer, bool);

impl OrderEntryPeer for Noisy {
    fn open(&mut self, mono: MonoNs, wall: WallNs) {
        self.0.open(mono, wall);
    }

    fn close(&mut self, mono: MonoNs, wall: WallNs) {
        self.0.close(mono, wall);
    }

    fn on_frame(&mut self, frame: &[u8]) {
        self.0.on_frame(frame);
    }

    fn on_market(&mut self, env: &Envelope<MdEvent>) {
        self.0.on_market(env);
    }

    fn on_market_epoch_end(&mut self, key: ConnKey) {
        self.0.on_market_epoch_end(key);
    }

    fn next_due(&self) -> Option<MonoNs> {
        self.0.next_due()
    }

    fn take_due(&mut self, now: MonoNs) -> Vec<Vec<u8>> {
        let mut due = self.0.take_due(now);
        if !due.is_empty() && !std::mem::replace(&mut self.1, true) {
            due.push(vec![0xff, 0xfe]);
        }
        due
    }
}

/// The decode errors a session's handler was told of.
struct Undecoded(Shared, Rc<RefCell<Vec<ConnKey>>>);

impl ExecHandler for Undecoded {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        ExecLog(Rc::clone(&self.0), 0).on_exec(env);
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.1.borrow_mut().push(key);
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_frame_that_is_not_utf8_is_read_as_binary_and_journaled_so() {
    let clock = IngestClock::new();
    let world = solo_world();
    let peer = Rc::new(RefCell::new(Noisy(SimPeer::new(&sim_config(0)), false)));
    let stream = InProcessStream::new(Rc::clone(&peer));
    let config = exec_config(0, "ws://127.0.0.1:9/never", &clock, 2);
    let codec = Box::new(SimCodec::new(&sim_config(0)));
    let errors = Rc::new(RefCell::new(Vec::new()));
    let handler = Undecoded(Rc::clone(&world), Rc::clone(&errors));
    let (mut session, control) = ExecSession::in_process(config, codec, stream, handler).unwrap();
    let journal = Rc::new(RefCell::new(Records::default()));
    session.set_journal(fbc_runtime::Journal::new(Rc::clone(&journal)));
    let run = session.run();
    let drive = async {
        for _ in 0..10_000 {
            if !errors.borrow().is_empty() {
                break;
            }
            tokio::time::sleep(ms(1)).await;
        }
        drop(control);
    };
    let (ran, ()) = tokio::join!(run, drive);
    ran.unwrap();
    // The greeting, then the binary frame, which the codec cannot read.
    assert_eq!(errors.borrow().len(), 1);
    assert_eq!(session.counters().decode_errors, 1);
    let binary = journal.borrow().0.iter().any(|(_, r)| {
        matches!(
            r,
            fbc_journal::Record::Inbound {
                opcode: fbc_journal::Opcode::Binary,
                ..
            }
        )
    });
    assert!(binary);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn frames_due_together_and_unread_when_their_epoch_ends_reach_the_next_epoch() {
    // Codex r4313036604 on PR #175: the greeting and a frame after it fall due together; the
    // greeting's epoch ends on a failed arm before the second is read, which the next epoch
    // reads first (here a frame that is not UTF-8, which the codec cannot decode).
    let mut unprotected = sim_config(0);
    unprotected.exec.order.cancel_on_disconnect = CancelOnDisconnect::None;
    let clock = IngestClock::new();
    let world = solo_world();
    let peer = Rc::new(RefCell::new(Noisy(SimPeer::new(&unprotected), false)));
    let stream = InProcessStream::new(Rc::clone(&peer));
    let config = exec_config(0, "ws://127.0.0.1:9/never", &clock, 2);
    let codec = Box::new(SimCodec::new(&unprotected));
    let errors = Rc::new(RefCell::new(Vec::new()));
    let handler = Undecoded(Rc::clone(&world), Rc::clone(&errors));
    let (mut session, control) = ExecSession::in_process(config, codec, stream, handler).unwrap();
    let run = session.run();
    let drive = async {
        for _ in 0..10_000 {
            if !errors.borrow().is_empty() {
                break;
            }
            tokio::time::sleep(ms(1)).await;
        }
        drop(control);
    };
    let (ran, ()) = tokio::join!(run, drive);
    ran.unwrap();
    let greeted = world.borrow().seen.iter().find_map(|s| match s {
        Seen::Exec(_, stamp, ExecEvent::Conn { .. }) => Some(stamp.conn.epoch),
        _ => None,
    });
    let epochs: Vec<u32> = errors.borrow().iter().map(|key| key.epoch).collect();
    assert_eq!(greeted, Some(0));
    assert_eq!(epochs, [1], "read once, on the epoch after the greeting's");
}
