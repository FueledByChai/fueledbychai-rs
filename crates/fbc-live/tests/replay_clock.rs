//! Two closed-loop replays of one journal through a `Link` record the same send times and the
//! same notes (FBC-y7lq, decision 0155 item 4), adapted from fbc-sim's `tests/closed_loop.rs`
//! (FBC-066n, decision 0107). A market-data journal of the runtime's toy venue, included by
//! path, is replayed through fbc-runtime's shard host on `IngestClock::replayed`, the runtime's
//! clock paused, and one SimVenue order-entry session, an ordinary `ExecSession` over an
//! in-process stream, hands every event to a link whose registry it resyncs. The owner's
//! journaled Start arms the market and places a buy through the link; the buy's fill is
//! offered at the ask through the link, inline. The link reads its session's clock, which reads
//! no wall clock in a replay, so each send is timed at the replayed instant it was submitted on,
//! and both runs agree. The venue, its caps, its account and its numbers are synthetic.

#[path = "../../fbc-runtime/tests/common/toy.rs"]
#[allow(dead_code, unused_imports)]
mod toy;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AckModel, AssetKey, BookId, Bps, CancelOnDisconnect,
    Channel, CidMint, ClientIdFormat, ClientOrderId, ConfigError, ConnKey, ConnTopology, Encoding,
    EndpointPlan, Envelope, ExecCaps, ExecCodec, ExecEndpoint, ExecEvent, FeeBook, FeeEntry,
    FeeKey, FeeRate, FeeSource, FeedSource, FieldSpec, FillCaps, FillSource, FundingCaps, HttpPlan,
    InstrumentId, InstrumentSpecDraft, Liquidity, Lots, MarketLease, MatchingCaps, MdCaps, MdCodec,
    MdEvent, MdTransport, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock, NonceScope,
    NonceSource, OrderCaps, OrderKind, OrderKindTag, OrderingKey, RawFrame, Readiness, RefKind,
    Secrets, Side, SnapshotReads, SnapshotSource, SpecTable, Stamp, StpScope, StreamId,
    SubmitHandle, Subscription, Support, SymbolError, TagSet, Ticks, Tif, TifTag, TradeCaps,
    VenueCaps, VenueConfig, VenueError, VenueFactory, VenueFeeSign, VenueOrderSnapshot, WallNs,
    WireUrl,
};
use fbc_journal::{
    ControlEvent, JournalError, JournalReader, JournalWriter, Opaque, Record, RedactionKey,
};
use fbc_live::link::Link;
use fbc_oms::{
    FillLedger, LadderConfig, LeaseKeys, Leases, LedgerConfig, MarketCapsConfig, PreTradeCaps,
    Registry,
};
use fbc_runtime::{
    ClosedLoop, Connector, ConsumerInput, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig,
    InProcessStream, IngestClock, Ladder, MdHandler, MdReplay, MdReplayConfig, Member, PeerFeed,
    ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, ShardHost, WriteStall,
};
use fbc_sim::{Bracket, QueueConfig, SimCodec, SimConfig, SimLatency, SimPeer};

/// The toy's instrument `A`, its trading book channel, and the order-entry stream the stood-in
/// venue plans.
const INST: InstrumentId = InstrumentId::new(1);
const BOOK: BookId = BookId(0);
const STREAM: StreamId = StreamId(5);
const NS: Namespace = Namespace::new(9);
const MS: u64 = 1_000_000;
/// The configuration key the stood-in venue's planned order-entry URL is under.
const URL: &str = "stood.url";
/// The bid and the ask the consumer quotes at, and the lots of its buy.
const BID: i64 = 199;
const ASK: i64 = 201;
const QTY: i64 = 2;
/// The market-data connection the journal records, and the shard whose journal it is.
const MD: ConnKey = ConnKey { conn: 1, epoch: 1 };
const SHARD: u16 = 3;
/// The wall time at the recorded monotonic origin (synthetic).
const WALL0: i64 = 1_700_000_000_000_000_000;
/// The consumer's number for the owner's Start, and when it was taken.
const START: u16 = 1;
const START_AT: u64 = 1_500;
/// The account the session trades.
const ACCT: AccountKey = AccountKey::new(20);

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn at(ms: u64) -> MonoNs {
    MonoNs(ms * MS)
}

fn wall(ms: u64) -> WallNs {
    WallNs(WALL0 + i64::try_from(ms * MS).unwrap())
}

/// A directory of its own under the test target's scratch space, new on every call.
fn fresh_dir(what: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "fbc-live-replay-clock-{}-{what}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Synthetic caps a SimVenue can say everything of, with per-connection cancel-on-disconnect,
/// as an order-entry session requires (decision 0058).
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
                    offers_disarm: true,
                },
                ack: AckModel::SinglePhase,
                client_id: ClientIdFormat::Uuid,
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::None,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                snapshot_reads: SnapshotReads::OneInstant,
                // A synthetic snapshot, answered from SimVenue's own state: it never lags.
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
            status: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::PerInstrument,
            max_conn_lifetime: None,
        },
        limits: vec![],
        exec_limits: None,
        readiness_ceiling: Readiness::Paper,
    }
}

fn order_caps() -> OrderCaps {
    stood_caps().exec.unwrap().order
}

/// The venue SimVenue stands in for: its caps, and one order-entry endpoint at the configured
/// URL, which an in-process session never opens. The session is given SimVenue's codec.
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

/// SimVenue standing in for the venue under `ACCT`, its maker rebate and taker fee on `INST`.
fn sim_config() -> SimConfig {
    let mut fees = FeeBook::new();
    for (liquidity, bps) in [(Liquidity::Maker, -0.5), (Liquidity::Taker, 2.0)] {
        let key = FeeKey {
            account: ACCT,
            instrument: INST,
            channel: Channel::Public,
            liquidity,
        };
        let entry = FeeEntry {
            rate: FeeRate(Bps(bps)),
            source: FeeSource::ConfigOverride,
            as_of: WallNs(0),
        };
        fees.insert(key, entry);
    }
    let caps = stood_caps();
    SimConfig {
        exec: caps.exec.unwrap(),
        matching: caps.matching,
        latency: SimLatency {
            to_venue: ms(5),
            to_client: ms(3),
        },
        rpc_timeout: Duration::from_secs(2),
        stream: STREAM,
        queue: QueueConfig {
            bracket: Bracket::Middle,
        },
        account: ACCT,
        fees,
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

/// The order-entry session's configuration, on connection 2 of the shard.
fn exec_config(clock: &IngestClock) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, "ws://127.0.0.1:9/never-opened");
    ExecSessionConfig {
        venue: &STOOD,
        cfg,
        creds: Secrets::new(),
        acct: ACCT,
        account_wide_cod_authority: None,
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: clock.clone(),
        nonces: Box::new(Counting(0)),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: 2,
        limiter: RateLimiter::new(&[], SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        resync_deadline: fbc_runtime::ResyncDeadline::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 1024,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    }
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[4; 32]).unwrap())
}

fn book_record(kind: &str, seq: i64, rest: &str) -> String {
    format!("{kind}|sym=A|book=0|seq={seq}{rest}")
}

fn level(side: &str, px: i64, qty: i64, seq: i64) -> String {
    book_record("lvl", seq, &format!("|side={side}|px={px}|qty={qty}"))
}

fn trade(px: i64, qty: i64, seq: i64) -> String {
    format!("trade|sym=A|px={px}|qty={qty}|seq={seq}")
}

/// The journal under `root`, as a live shard wrote it: the connection's opening and
/// subscription, the trading book (bids 199 x 6, asks 201 x 5), the owner's Start, trades at
/// the bid that fill the buy behind the book's six lots, trades at the ask that fill its offer,
/// and the connection's closing.
fn journal(root: &Path) {
    let mut writer = JournalWriter::create(root, SHARD, key()).unwrap();
    let mut write = |ms: u64, record: Record| writer.append(wall(ms), &record).unwrap();
    let control = |ev| Record::Control { at: at(1_000), ev };
    write(1_000, control(ControlEvent::Opened(MD)));
    write(
        1_000,
        control(ControlEvent::Subscribe {
            conn: MD,
            add: vec![toy::sub(1), toy::book(1, BOOK)],
            remove: vec![],
        }),
    );
    let day: [(u64, Option<String>); 8] = [
        (1_001, Some(book_record("begin", 1, "|epoch=1"))),
        (1_002, Some(level("bid", BID, 6, 2))),
        (1_003, Some(level("ask", ASK, 5, 3))),
        (1_004, Some(book_record("end", 4, ""))),
        (START_AT, None),
        (3_000, Some(trade(BID, 6, 10))),
        (4_000, Some(trade(BID, 4, 11))),
        (6_000, Some(trade(ASK, 10, 12))),
    ];
    for (seq, (ms, frame)) in (0u64..).zip(day) {
        let record = match &frame {
            Some(text) => {
                let stamp = Stamp {
                    ingest_seq: seq,
                    kernel_rx: None,
                    recv_mono: at(ms),
                    recv_wall: wall(ms),
                    conn: MD,
                };
                Record::inbound(stamp, RawFrame::Text(text))
            }
            None => Record::Consumer {
                at: at(ms),
                kind: START,
                input: true,
                ingest_seq: Some(seq),
                bytes: Opaque(b"start".to_vec()),
            },
        };
        write(ms, record);
    }
    let closed = Record::Control {
        at: at(7_000),
        ev: ControlEvent::Closed(MD),
    };
    write(7_000, closed);
    writer.flush().unwrap();
}

/// The records of the journal at `root`, as its reader reads them.
fn records(root: &Path) -> impl Iterator<Item = Result<Record, JournalError>> {
    JournalReader::open(root, SHARD).unwrap()
}

/// What the consumer holds: the link, the session's orders, its mint and its leases, and the
/// stamp of each input it submitted on, with the order submitted.
struct World {
    link: Link,
    orders: Option<ExecOrders>,
    mint: CidMint,
    leases: PathBuf,
    submitted_on: Vec<(ClientOrderId, MonoNs)>,
}

type Shared = Rc<RefCell<World>>;

impl World {
    /// A post-only limit order on `INST`, built and authorized by the link's registry and
    /// submitted through the link on the input received at `on`.
    fn place(&mut self, side: Side, px: i64, qty: i64, on: MonoNs) {
        let cid = self.mint.mint().unwrap();
        let order = NewOrder {
            cid,
            inst: INST,
            side,
            kind: OrderKind::Limit { px: Ticks(px) },
            qty: lots(qty),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let cmd = self.link.reg().place(order).unwrap();
        let auth = self.link.reg().authorize(ACCT, cmd).unwrap();
        let orders = self.orders.clone().unwrap();
        self.link.submit(&orders, auth).unwrap();
        self.submitted_on.push((cid, on));
    }
}

/// The session's handler: each call handed to the link; a buy's fill offered at the ask at
/// once, inline, through the link.
struct Linked(Shared);

impl ExecHandler for Linked {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut world = self.0.borrow_mut();
        let fill = match &env.body {
            ExecEvent::Fill(f) if f.side == Side::Buy => Some(f.qty.get()),
            _ => None,
        };
        let on = env.stamp.recv_mono;
        world.link.on_exec(env);
        if let Some(qty) = fill {
            world.place(Side::Sell, ASK, qty, on);
        }
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().link.on_epoch_end(key);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.0.borrow_mut().link.on_submitted(handle);
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.0.borrow_mut().link.on_unprotected(orders);
    }
}

/// The consumer's own input, handed back from the journal: the owner's Start arms the market
/// and bids through the link.
struct Inputs(Shared);

impl fbc_runtime::InputHandler for Inputs {
    fn on_input(&mut self, input: ConsumerInput) {
        assert_eq!(input.kind, START);
        let mut world = self.0.borrow_mut();
        let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
        let market = MarketLease::acquire(&world.leases, "sim", "sim-0", &symbol).unwrap();
        let acct = AccountLease::acquire(&world.leases, "sim", "sim-0").unwrap();
        let leases = Leases::market(market).with_account(acct);
        world.link.reg().start(INST, leases).unwrap();
        world.place(Side::Buy, BID, QTY, input.at);
    }
}

/// The market data's handler, behind the session's peer: the consumer reads none of it.
struct Ignored;

impl MdHandler for Ignored {
    fn on_md(&mut self, _: Envelope<MdEvent>) {}
}

/// The decide pass: the consumer submits through the link only, never from the ladder (decision
/// 0155, Consequences).
struct Nothing;

impl fbc_runtime::Decide for Nothing {
    fn decide(&mut self, _: &[InstrumentId], _: &mut Ladder) {}
}

/// An order placed, the stamp of the input it was submitted on, and its send time as the
/// link's registry holds it.
type Send = (ClientOrderId, MonoNs, Option<(MonoNs, WallNs)>);

/// What one run's link recorded: each order's send in the order placed, and every note, as
/// shown.
#[derive(Debug, PartialEq)]
struct Run {
    sends: Vec<Send>,
    notes: Vec<String>,
}

/// Replays the journal at `root` closed-loop through the shard host, with a fresh SimVenue, a
/// fresh link over a fresh registry, and a fresh client-id mint and leases.
async fn closed_loop(root: &Path) -> Run {
    let leases = fresh_dir("leases");
    let clock = IngestClock::replayed(WallNs(WALL0));
    let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
    let keys = LeaseKeys::new("sim", "sim-0", &order_caps()).with_market(INST, symbol);
    let limits = MarketCapsConfig {
        inventory: Some(lots(100)),
        resting: Some(lots(100)),
    };
    let pre_trade = PreTradeCaps::new().with_market(INST, limits).unwrap();
    let reg = Registry::with_caps(pre_trade).with_lease_keys(keys);
    let ledger = LedgerConfig {
        max_age: Duration::from_secs(3_600),
        max_entries: 1_000,
    };
    let ladder = LadderConfig::new(Duration::from_secs(1), Duration::ZERO, ms(10_000), 1).unwrap();
    let link = Link::new(
        reg,
        FillLedger::new(ledger, WallNs(0)).unwrap(),
        order_caps(),
        ladder,
    );
    let ns = NamespaceLease::acquire(&leases, ACCT, NS).unwrap();
    let world: Shared = Rc::new(RefCell::new(World {
        link,
        orders: None,
        mint: CidMint::new(ns, 0, 0, WallNs(1)),
        leases: leases.clone(),
        submitted_on: Vec::new(),
    }));

    let peer = Rc::new(RefCell::new(SimPeer::new(&sim_config())));
    let stream = InProcessStream::new(Rc::clone(&peer));
    let codec = Box::new(SimCodec::new(&sim_config()));
    let handler = Linked(Rc::clone(&world));
    let built = ExecSession::in_process(exec_config(&clock), codec, stream.clone(), handler);
    let (mut session, stop) = built.unwrap();
    {
        let mut w = world.borrow_mut();
        w.orders = Some(session.orders());
        w.link.attach(session.orders(), ACCT);
    }

    let config = MdReplayConfig {
        venue: toy::ToyVenue::leak(),
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain("ws://127.0.0.1:9/replayed"),
            },
            subs: vec![toy::sub(1), toy::book(1, BOOK)],
        },
        specs: toy::specs(),
        conn: MD.conn,
    };
    let mut host = ShardHost::new(clock.clone());
    let feed = PeerFeed::new(vec![stream], Ignored);
    let mut md = MdReplay::new(config, feed).unwrap();
    md.set_inputs(Inputs(Rc::clone(&world)));
    let closed = ClosedLoop {
        clock: clock.clone(),
        records: records(root),
        settle: Duration::from_secs(1),
        stops: vec![stop],
    };
    let members = vec![
        Member::md_replay(&mut md, closed),
        Member::exec(&mut session),
    ];
    host.run(members, &mut Nothing).await.unwrap();
    drop((session, md, host));

    let world = Rc::try_unwrap(world).ok().unwrap().into_inner();
    let reg = world.link.registry();
    let sends = world.submitted_on.iter();
    let sends = sends.map(|(cid, on)| (*cid, *on, reg.get(*cid).unwrap().sent_at()));
    let run = Run {
        sends: sends.collect(),
        notes: world
            .link
            .notes()
            .iter()
            .map(|n| format!("{n:?}"))
            .collect(),
    };
    drop(world);
    let _ = std::fs::remove_dir_all(&leases);
    run
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn two_closed_loop_replays_through_a_link_give_the_same_send_times_and_notes() {
    let root = fresh_dir("journal");
    journal(&root);
    let first = closed_loop(&root).await;
    let second = closed_loop(&root).await;
    assert_eq!(first, second);

    // The buy placed on the Start and its offer placed on the buy's fill, each sent at the
    // replayed instant of the input it was submitted on, on both clocks: the replayed clock's
    // wall time is the journal's, at its recorded origin's offset.
    assert_eq!(first.sends.len(), 2, "{:?}", first.sends);
    assert_eq!(first.sends[0].1, at(START_AT));
    for (cid, on, sent) in &first.sends {
        let (mono, wall) = sent.unwrap_or_else(|| panic!("{cid:?} was not sent"));
        assert_eq!(mono, *on, "{cid:?}");
        assert_eq!(
            wall,
            WallNs(WALL0 + i64::try_from(on.0).unwrap()),
            "{cid:?}"
        );
    }
    assert!(first.sends[1].1 > at(3_000), "{:?}", first.sends);
    assert!(
        !first.notes.iter().any(|n| n.starts_with("Problem")),
        "{:?}",
        first.notes
    );
    let _ = std::fs::remove_dir_all(&root);
}
