//! FBC-066n's done line (BT-501; decisions 0006, 0014, 0046, 0057, 0058, 0100, 0103, 0104): a
//! closed-loop replay. A toy-venue market-data journal is replayed through fbc-runtime's shard
//! host: the decoder replay (`MdReplay`) is a member of the shard (`Member::md_replay`), fed each
//! record at its recorded instant on the shard's replayed clock (`IngestClock::replayed`), and
//! drives the consumer's handlers and decide passes, the host journaling a `Cycle` record per
//! pass as live. Three SimVenue order-entry sessions, one per bracket (Pessimistic, Middle,
//! Optimistic), are ordinary `ExecSession`s over an in-process stream whose peer is a `SimPeer`
//! (FBC-6mf's hosting), each armed through its own fbc-oms registry from its own resync.
//!
//! The consumer's commands depend on the fills it gets, so no script could give them: the
//! owner's journaled Start arms the market and lets the decide pass bid; each buy's fill makes
//! the handler offer what it bought at the ask (submitted inline, the per-event path); once a
//! bracket is flat again its decide pass bids again, twice at most; and the journaled firing of
//! the timer the Start armed winds the brackets down, cancelling what rests. Each command is
//! answered by its bracket's SimVenue at the replayed instant it was encoded plus the configured
//! latency, so the brackets, filled in their order, decide different commands.
//!
//! Time is virtual only: the runtime's clock is paused (`start_paused`), the shard clock reads
//! its wall time from the recorded stamps, and the journal spans an hour that the replay runs
//! through as fast as the CPU. Two runs produce identical commands, outcomes, fills and `Cycle`
//! records. The venue, its caps, its account names and its rates are synthetic.

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

use fbc_core::Encoding;
use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AckModel, AssetKey, BookId, Bps, CancelOnDisconnect,
    Channel, CidMatch, CidMint, ClientIdFormat, ClientOrderId, ConfigError, ConnKey, ConnState,
    ConnTopology, EndpointPlan, Envelope, ExecCaps, ExecCodec, ExecEndpoint, ExecEvent, FeeBook,
    FeeEntry, FeeKey, FeeRate, FeeSource, FeedSource, FieldSpec, FillCaps, FillSource, FundingCaps,
    HttpPlan, InstrumentId, InstrumentSpecDraft, Liquidity, Lots, MarketLease, MatchingCaps,
    MdCaps, MdCodec, MdEvent, MdTransport, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock,
    NonceScope, NonceSource, OrderCaps, OrderKind, OrderKindTag, OrderingKey, RawFrame, Readiness,
    RefKind, RpcId, Secrets, Side, SnapshotReads, SnapshotSource, SpecTable, Stamp, StpScope,
    StreamId, SubmitOutcome, Subscription, Support, SymbolError, TagSet, Ticks, Tif, TifTag,
    TradeCaps, TrafficClass, VenueCaps, VenueConfig, VenueError, VenueFactory, VenueFeeSign,
    VenueOrderState, WallNs, WireUrl,
};
use fbc_journal::{
    ControlEvent, JournalError, JournalReader, JournalWriter, Opaque, Record, RedactionKey,
};
use fbc_oms::{
    Authorization, CancelChoice, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey,
    PreTradeCaps, Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Changed, ClosedLoop, Connector, ConsumerInput, ConsumerTimers, ExecControl, ExecHandler,
    ExecOrders, ExecSession, ExecSessionConfig, InProcessStream, IngestClock, Journal, Ladder,
    MdHandler, MdReplay, MdReplayConfig, Member, PeerFeed, ProxyConfig, RateLimiter,
    ReconnectPacing, ReplayFailure, RpcIds, SafetyReserve, ShardError, ShardHost, WriteStall,
};
use fbc_sim::{Bracket, PeerCounters, QueueConfig, SimCodec, SimConfig, SimLatency, SimPeer};

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
/// The bid and the ask the consumer quotes at, and the lots of each buy.
const BID: i64 = 199;
const ASK: i64 = 201;
const QTY: i64 = 2;
/// The buys each bracket makes at most.
const ROUNDS: usize = 2;
/// The market-data connection the journal records, and the shard whose journal it is.
const MD: ConnKey = ConnKey { conn: 1, epoch: 1 };
const SHARD: u16 = 3;
/// The wall time at the recorded monotonic origin: every recorded stamp is this much apart on
/// the two clocks (synthetic).
const WALL0: i64 = 1_700_000_000_000_000_000;
/// The consumer's own numbers for its inputs.
const START: u16 = 1;
const WIND_DOWN: u16 = 2;
/// When the owner's Start was taken, and when the timer it armed fired: an hour later.
const START_AT: u64 = 1_500;
const WIND_DOWN_AT: u64 = 3_600_000;
/// The three brackets, in the order the test names them.
const BRACKETS: [Bracket; 3] = [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic];

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn at(ms: u64) -> MonoNs {
    MonoNs(ms * MS)
}

/// A directory of its own under the test target's scratch space, new on every call, so the
/// tests of this binary, which run at once, and two runs of one test never share one.
fn fresh_dir(what: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "fbc-sim-closed-loop-{}-{what}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

fn order_caps() -> OrderCaps {
    stood_caps().exec.unwrap().order
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

/// Bracket `i`'s simulated account.
fn account(i: usize) -> AccountKey {
    AccountKey::new(20 + u16::try_from(i).unwrap())
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

/// Bracket `i`'s order-entry session's configuration, on connection `2 + i` of the shard.
fn exec_config(i: usize, clock: &IngestClock) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, "ws://127.0.0.1:9/never-opened");
    ExecSessionConfig {
        venue: &STOOD,
        cfg,
        creds: Secrets::new(),
        acct: account(i),
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: clock.clone(),
        nonces: Box::new(Counting(0)),
        nonce_source: fbc_journal::NonceSourceId(u32::try_from(i).unwrap()),
        conn: 2 + u16::try_from(i).unwrap(),
        limiter: RateLimiter::new(&[], SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 1024,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    }
}

// ---------------------------------------------------------------------------------------------
// The journal: a toy market-data session over an hour, with the owner's Start and the firing of
// the timer it armed, as a live shard journals them.
// ---------------------------------------------------------------------------------------------

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

/// What the live shard took, at the recorded millisecond.
enum Took {
    Frame(String),
    Input(u16, &'static [u8]),
}

/// The day, by recorded millisecond.
fn day() -> Vec<(u64, Took)> {
    let frame = |ms: u64, text: String| (ms, Took::Frame(text));
    vec![
        // The trading book: bids 199 x 6, asks 201 x 5.
        frame(1_001, book_record("begin", 1, "|epoch=1")),
        frame(1_002, level("bid", BID, 6, 2)),
        frame(1_003, level("ask", ASK, 5, 3)),
        frame(1_004, book_record("end", 4, "")),
        // The owner's Start.
        (START_AT, Took::Input(START, b"start")),
        // The bid shrinks by three with no trade, then three trades at it: the brackets' buys
        // fill Optimistic first, then Middle, then Pessimistic.
        frame(2_000, level("bid", BID, 3, 5)),
        frame(3_000, trade(BID, 4, 10)),
        frame(4_000, trade(BID, 2, 11)),
        frame(5_000, trade(BID, 2, 12)),
        // Trades at the ask, which fill what each bracket offered.
        frame(6_000, trade(ASK, 6, 13)),
        frame(7_000, trade(ASK, 6, 14)),
        frame(8_000, trade(ASK, 6, 15)),
        // Trades at the bid, which fill the second buys.
        frame(9_000, trade(BID, 10, 16)),
        frame(10_000, trade(BID, 10, 17)),
        // An hour after the Start, its timer fires: the brackets wind down.
        (WIND_DOWN_AT, Took::Input(WIND_DOWN, b"wind-down")),
        // A trade at the ask that would have filled what rested.
        frame(3_600_500, trade(ASK, 20, 18)),
    ]
}

/// The day's journal under `root`, as the live shard wrote it: the connection's opening and
/// subscription, every frame and input stamped in turn, and its closing. A journal that `whole`
/// is false of ends with the wind-down's firing.
fn journal(root: &Path, whole: bool) {
    let mut writer = JournalWriter::create(root, SHARD, key()).unwrap();
    let wall = |ms: u64| WallNs(WALL0 + i64::try_from(ms * MS).unwrap());
    let mut write = |ms: u64, record: Record| writer.append(wall(ms), &record).unwrap();
    write(
        1_000,
        Record::Control {
            at: at(1_000),
            ev: ControlEvent::Opened(MD),
        },
    );
    write(
        1_000,
        Record::Control {
            at: at(1_000),
            ev: ControlEvent::Subscribe {
                conn: MD,
                add: vec![toy::sub(1), toy::book(1, BOOK)],
                remove: vec![],
            },
        },
    );
    let day = day()
        .into_iter()
        .filter(|(ms, _)| whole || *ms <= WIND_DOWN_AT);
    for (seq, (ms, took)) in day.enumerate() {
        let seq = u64::try_from(seq).unwrap();
        let record = match took {
            Took::Frame(text) => {
                let stamp = Stamp {
                    ingest_seq: seq,
                    kernel_rx: None,
                    recv_mono: at(ms),
                    recv_wall: wall(ms),
                    conn: MD,
                };
                Record::inbound(stamp, RawFrame::Text(&text))
            }
            Took::Input(kind, bytes) => Record::Consumer {
                at: at(ms),
                kind,
                input: true,
                ingest_seq: Some(seq),
                bytes: Opaque(bytes.to_vec()),
            },
        };
        write(ms, record);
        if ms == 10_000 {
            // What the live shard wrote besides that the replay passes over: its order-entry
            // session's frame, on another connection, and the consumer's own annotation.
            write(
                10_001,
                Record::Outbound {
                    at: at(10_001),
                    conn: ConnKey { conn: 9, epoch: 1 },
                    rpc: None,
                    opcode: fbc_journal::Opcode::Text,
                    frame: fbc_core::WireSlice::plain(b"live order entry".to_vec()),
                },
            );
            write(
                10_002,
                Record::Consumer {
                    at: at(10_002),
                    kind: 77,
                    input: false,
                    ingest_seq: None,
                    bytes: Opaque(b"note".to_vec()),
                },
            );
        }
    }
    if whole {
        write(
            3_601_000,
            Record::Control {
                at: at(3_601_000),
                ev: ControlEvent::Closed(MD),
            },
        );
    }
    writer.flush().unwrap();
}

// ---------------------------------------------------------------------------------------------
// The consumer: a registry per bracket, its handlers and its decide pass, sharing one world.
// ---------------------------------------------------------------------------------------------

/// What the consumer submitted on a bracket, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cmd {
    /// A post-only limit order.
    Place {
        cid: ClientOrderId,
        side: Side,
        px: i64,
        qty: i64,
    },
    /// The cancel of an order.
    Cancel { cid: ClientOrderId },
}

/// One command submitted: its bracket, its request, what it was, and the stamp of the input
/// whose handling submitted it (`None` for a decide pass's ladder).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Submitted {
    bracket: usize,
    rpc: RpcId,
    cmd: Cmd,
    handling: Option<Stamp>,
}

/// What reached the consumer, in the order it reached it.
#[derive(Clone, Debug, PartialEq)]
enum Seen {
    Md(Stamp, MdEvent),
    Exec(usize, Stamp, ExecEvent),
    Input(ConsumerInput),
    /// A decide pass, with the instruments it was handed.
    Decide(Vec<InstrumentId>),
    /// A ladder command the host handed back unsubmitted.
    Unsubmitted,
}

/// One bracket's registry and what the consumer tracks of its orders.
struct Oms {
    acct: AccountKey,
    name: String,
    reg: Registry,
    mint: CidMint,
    leases: PathBuf,
    /// The shard clock's reading when the stream was reported authenticated: the resync was
    /// asked after it.
    authenticated_at: Option<MonoNs>,
    collecting: Option<ResyncSnapshot>,
    resynced: bool,
    /// The owner's Start armed the market: the consumer quotes until the wind-down.
    quoting: bool,
    /// Lots bought less lots sold.
    position: i64,
    /// Orders placed and not yet ended, with their side.
    open: BTreeMap<ClientOrderId, Side>,
    /// What each request placed, until its outcome.
    placing: BTreeMap<RpcId, ClientOrderId>,
    /// The buys made.
    rounds: usize,
    orders: Option<ExecOrders>,
    /// The requests submitted inline, whose ids `submit` returned.
    inline: BTreeSet<RpcId>,
    /// The ladder's commands not yet reported submitted, by their place in `World::submitted`:
    /// the session reports each, in the order it took them, to `on_submitted`.
    laddered: std::collections::VecDeque<usize>,
}

impl Oms {
    fn new(i: usize, dir: &Path) -> Oms {
        let name = format!("sim-{i}");
        let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
        let keys = LeaseKeys::new("sim", &name, &order_caps()).with_market(INST, symbol);
        let limits = MarketCapsConfig {
            inventory: Some(lots(100)),
            resting: Some(lots(100)),
        };
        let pre_trade = PreTradeCaps::new().with_market(INST, limits).unwrap();
        let ns = NamespaceLease::acquire(dir, account(i), NS).unwrap();
        Oms {
            acct: account(i),
            name,
            reg: Registry::with_caps(pre_trade).with_lease_keys(keys),
            mint: CidMint::new(ns, 0, 0, WallNs(1)),
            leases: dir.to_path_buf(),
            authenticated_at: None,
            collecting: None,
            resynced: false,
            quoting: false,
            position: 0,
            open: BTreeMap::new(),
            placing: BTreeMap::new(),
            rounds: 0,
            orders: None,
            inline: BTreeSet::new(),
            laddered: std::collections::VecDeque::new(),
        }
    }

    /// Takes one of the session's events: the resync is read into the registry, and the
    /// consumer's view of its orders and position kept.
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
                let snap = self.collecting.as_mut().unwrap();
                snap.positions.push((*inst, *qty));
            }
            ExecEvent::ResyncEnd => {
                let snap = self.collecting.take().unwrap();
                let ladder =
                    LadderConfig::new(Duration::from_secs(1), Duration::ZERO, ms(10_000), 1)
                        .unwrap();
                let key = OrderKey {
                    venue: env.venue_seq,
                    ingest: env.stamp.ingest_seq,
                };
                let report = self.reg.resync(&ladder, &order_caps(), &snap, key).unwrap();
                assert!(!report.untrustworthy);
                self.resynced = true;
            }
            ExecEvent::Outcome { rpc, outcome, .. } => {
                if let Some(cid) = self.placing.remove(rpc)
                    && !matches!(outcome, SubmitOutcome::Accepted { .. })
                {
                    self.open.remove(&cid);
                }
            }
            ExecEvent::Order(u) => {
                let ended = matches!(
                    u.state,
                    VenueOrderState::Filled
                        | VenueOrderState::Canceled(_)
                        | VenueOrderState::Rejected(_)
                );
                if let (true, Some(CidMatch::Ours(cid))) = (ended, &u.cid) {
                    self.open.remove(cid);
                }
            }
            ExecEvent::Fill(f) => match f.side {
                Side::Buy => self.position += f.qty.get(),
                Side::Sell => self.position -= f.qty.get(),
            },
            _ => {}
        }
    }

    /// The owner's Start: the market is armed under the account's leases (decision 0013).
    fn start(&mut self) {
        assert!(self.resynced, "the session resynced before the Start");
        let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
        let market = MarketLease::acquire(&self.leases, "sim", &self.name, &symbol).unwrap();
        let acct = AccountLease::acquire(&self.leases, "sim", &self.name).unwrap();
        self.reg
            .start(INST, Leases::market(market).with_account(acct))
            .unwrap();
        self.quoting = true;
    }

    /// A post-only limit order built and authorized by the registry, and what it is.
    fn place(&mut self, side: Side, px: i64, qty: i64) -> (Authorization, Cmd) {
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
        let cmd = self.reg.place(order).unwrap();
        let auth = self.reg.authorize(self.acct, cmd).unwrap();
        self.open.insert(cid, side);
        (auth, Cmd::Place { cid, side, px, qty })
    }

    /// The cancel of `cid`, built and authorized by the registry.
    fn cancel(&mut self, cid: ClientOrderId) -> (Authorization, Cmd) {
        let choice = self.reg.cancellable(cid).unwrap().cancel(&order_caps());
        let CancelChoice::Send(cmd) = choice else {
            panic!("a cancel by client id waits for nothing");
        };
        (
            self.reg.authorize(self.acct, cmd).unwrap(),
            Cmd::Cancel { cid },
        )
    }

    fn open_on(&self, side: Side) -> bool {
        self.open.values().any(|s| *s == side)
    }
}

struct World {
    seen: Vec<Seen>,
    oms: Vec<Oms>,
    submitted: Vec<Submitted>,
    /// The stamp of the input whose events or input the consumer is being handed.
    handling: Option<Stamp>,
    /// The consumer's timers: inert in a replay, the live firing coming back from the journal.
    timers: ConsumerTimers,
}

type Shared = Rc<RefCell<World>>;

impl World {
    /// Submits `auth` on bracket `i` inline, from a handler: the per-event path.
    fn submit(&mut self, i: usize, (auth, cmd): (Authorization, Cmd)) {
        let orders = self.oms[i].orders.clone().unwrap();
        let rpc = orders.submit(auth).unwrap();
        self.oms[i].inline.insert(rpc);
        if let Cmd::Place { cid, .. } = cmd {
            self.oms[i].placing.insert(rpc, cid);
        }
        let handling = self.handling;
        self.submitted.push(Submitted {
            bracket: i,
            rpc,
            cmd,
            handling,
        });
    }

    /// The session reported request `rpc` on bracket `i` submitted: a ladder command's, when no
    /// inline submission named it, the bracket's next one.
    fn reported(&mut self, i: usize, rpc: RpcId) {
        if self.oms[i].inline.contains(&rpc) {
            return;
        }
        let at = self.oms[i].laddered.pop_front().expect("a ladder command");
        self.submitted[at].rpc = rpc;
        if let Cmd::Place { cid, .. } = self.submitted[at].cmd {
            self.oms[i].placing.insert(rpc, cid);
        }
    }
}

/// The consumer's market-data handler, behind the brackets' peers: it marks the instrument of
/// every event changed, for the next decide pass.
struct MdLog(Shared, Option<Changed>);

impl MdHandler for MdLog {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        if let Some(changed) = &self.1 {
            changed.mark(INST);
        }
        self.0.borrow_mut().seen.push(Seen::Md(env.stamp, env.body));
    }
}

/// Bracket `i`'s handler: a buy's fill is offered at the ask at once, inline.
struct ExecLog(Shared, usize);

impl ExecHandler for ExecLog {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut world = self.0.borrow_mut();
        let i = self.1;
        world.oms[i].on_exec(&env);
        world.seen.push(Seen::Exec(i, env.stamp, env.body.clone()));
        if let ExecEvent::Fill(f) = &env.body
            && f.side == Side::Buy
            && world.oms[i].quoting
        {
            world.handling = Some(env.stamp);
            let offer = world.oms[i].place(Side::Sell, ASK, f.qty.get());
            world.submit(i, offer);
            world.handling = None;
        }
    }

    fn on_submitted(&mut self, handle: fbc_core::SubmitHandle) {
        assert!(handle.receipt.is_ok(), "{handle:?}");
        self.0.borrow_mut().reported(self.1, handle.rpc);
    }
}

/// The consumer's own inputs, handed back from the journal: the Start arms every bracket and
/// arms the wind-down timer; the wind-down's firing stops the quoting and cancels what rests.
struct Inputs(Shared);

impl fbc_runtime::InputHandler for Inputs {
    fn on_input(&mut self, input: ConsumerInput) {
        let mut world = self.0.borrow_mut();
        world.seen.push(Seen::Input(input.clone()));
        let stamp = Stamp {
            ingest_seq: input.ingest_seq,
            kernel_rx: None,
            recv_mono: input.at,
            recv_wall: WallNs(0),
            conn: ConnKey { conn: 0, epoch: 0 },
        };
        match input.kind {
            START => {
                for oms in &mut world.oms {
                    oms.start();
                }
                let armed = world.timers.arm(
                    ms(WIND_DOWN_AT - START_AT),
                    TrafficClass::Safety,
                    WIND_DOWN,
                    b"wind-down".to_vec(),
                );
                assert_eq!(armed, Ok(()));
            }
            WIND_DOWN => {
                world.handling = Some(stamp);
                for i in 0..world.oms.len() {
                    world.oms[i].quoting = false;
                    let open: Vec<ClientOrderId> = world.oms[i].open.keys().copied().collect();
                    for cid in open {
                        let cancel = world.oms[i].cancel(cid);
                        world.submit(i, cancel);
                    }
                }
                world.handling = None;
            }
            other => panic!("no input of kind {other}"),
        }
    }
}

/// The consumer's decide pass: a bracket that quotes, is flat, has made fewer than `ROUNDS`
/// buys and has none resting bids again.
struct Decider(Shared);

impl fbc_runtime::Decide for Decider {
    fn decide(&mut self, instruments: &[InstrumentId], ladder: &mut Ladder) {
        let mut world = self.0.borrow_mut();
        world.seen.push(Seen::Decide(instruments.to_vec()));
        for i in 0..world.oms.len() {
            let oms = &mut world.oms[i];
            if !oms.quoting || oms.position != 0 || oms.rounds >= ROUNDS || oms.open_on(Side::Buy) {
                continue;
            }
            oms.rounds += 1;
            let (auth, cmd) = oms.place(Side::Buy, BID, QTY);
            let orders = oms.orders.clone().unwrap();
            ladder.push(&orders, auth).unwrap();
            // The ladder submits it after this pass: the session reports its request id.
            let at = world.submitted.len();
            world.oms[i].laddered.push_back(at);
            world.submitted.push(Submitted {
                bracket: i,
                rpc: RpcId(u64::MAX),
                cmd,
                handling: None,
            });
        }
    }

    fn unsubmitted(&mut self, _: fbc_runtime::Unsubmitted) {
        self.0.borrow_mut().seen.push(Seen::Unsubmitted);
    }
}

/// Every record a journal was offered, with its class and the wall time it was offered at.
#[derive(Default)]
struct Records(Vec<(TrafficClass, WallNs, Record)>);

impl fbc_journal::JournalSink for Records {
    fn record(
        &mut self,
        class: TrafficClass,
        now: WallNs,
        record: &Record,
    ) -> fbc_journal::Recorded {
        self.0.push((class, now, record.clone()));
        fbc_journal::Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> fbc_journal::Recorded {
        fbc_journal::Recorded::Ok
    }
}

// ---------------------------------------------------------------------------------------------
// One closed-loop run.
// ---------------------------------------------------------------------------------------------

/// What one run produced.
#[derive(Debug, PartialEq)]
struct Run {
    seen: Vec<Seen>,
    submitted: Vec<Submitted>,
    /// What the shard journaled: the host's Cycle records and every session's records.
    journal: Vec<(TrafficClass, WallNs, Record)>,
    counters: Vec<PeerCounters>,
    passes: u64,
    timers_not_armed: u64,
}

impl Run {
    /// Bracket `i`'s events, each with its stamp.
    fn exec(&self, i: usize) -> Vec<(Stamp, &ExecEvent)> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Exec(b, stamp, ev) if *b == i => Some((*stamp, ev)),
                _ => None,
            })
            .collect()
    }

    /// The stamp of the outcome of request `rpc` on bracket `i`, and the outcome.
    fn outcome(&self, i: usize, rpc: RpcId) -> (Stamp, SubmitOutcome) {
        self.exec(i)
            .into_iter()
            .find_map(|(stamp, ev)| match ev {
                ExecEvent::Outcome {
                    rpc: r, outcome, ..
                } if *r == rpc => Some((stamp, outcome.clone())),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no outcome of {rpc:?} on bracket {i}"))
    }

    /// When bracket `i`'s session wrote request `rpc`, on the shard's monotonic clock.
    fn written(&self, i: usize, rpc: RpcId) -> MonoNs {
        let conn = 2 + u16::try_from(i).unwrap();
        self.journal
            .iter()
            .find_map(|(_, _, r)| match r {
                Record::Outbound {
                    at,
                    conn: c,
                    rpc: Some(r),
                    ..
                } if c.conn == conn && *r == rpc => Some(*at),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{rpc:?} was not written on bracket {i}"))
    }

    /// Bracket `i`'s fills: their stamps, sides, prices and lots.
    fn fills(&self, i: usize) -> Vec<(Stamp, Side, i64, i64)> {
        self.exec(i)
            .into_iter()
            .filter_map(|(stamp, ev)| match ev {
                ExecEvent::Fill(f) => Some((stamp, f.side, f.px.0, f.qty.get())),
                _ => None,
            })
            .collect()
    }

    /// Bracket `i`'s commands, in the order submitted.
    fn commands(&self, i: usize) -> Vec<&Submitted> {
        self.submitted.iter().filter(|s| s.bracket == i).collect()
    }

    /// The Cycle records the host journaled.
    fn cycles(&self) -> Vec<(u64, Vec<InstrumentId>)> {
        self.journal
            .iter()
            .filter_map(|(_, _, r)| match r {
                Record::Cycle {
                    last_ingest_seq,
                    instruments,
                } => Some((*last_ingest_seq, instruments.clone())),
                _ => None,
            })
            .collect()
    }
}

/// The records of the journal at `root`, as its reader reads them.
fn records(root: &Path) -> impl Iterator<Item = Result<Record, JournalError>> {
    JournalReader::open(root, SHARD).unwrap()
}

/// Replays the journal at `root` closed-loop through the shard host, with a fresh SimVenue per
/// bracket and a fresh registry, client-id mint and leases per bracket.
async fn closed_loop(root: &Path, settle: Duration) -> Run {
    let leases = fresh_dir("leases");
    let clock = IngestClock::replayed(WallNs(WALL0));
    let world: Shared = Rc::new(RefCell::new(World {
        seen: Vec::new(),
        oms: (0..BRACKETS.len()).map(|i| Oms::new(i, &leases)).collect(),
        submitted: Vec::new(),
        handling: None,
        timers: ConsumerTimers::inert(),
    }));
    let journaled = Rc::new(RefCell::new(Records::default()));

    let peers: Vec<Rc<RefCell<SimPeer>>> = (0..BRACKETS.len())
        .map(|i| Rc::new(RefCell::new(SimPeer::new(&sim_config(i)))))
        .collect();
    let streams: Vec<InProcessStream> = peers
        .iter()
        .map(|p| InProcessStream::new(Rc::clone(p)))
        .collect();
    let mut sessions = Vec::new();
    let mut stops: Vec<ExecControl> = Vec::new();
    for (i, stream) in streams.iter().enumerate() {
        let codec = Box::new(SimCodec::new(&sim_config(i)));
        let handler = ExecLog(Rc::clone(&world), i);
        let (mut session, stop) =
            ExecSession::in_process(exec_config(i, &clock), codec, stream.clone(), handler)
                .unwrap();
        session.set_journal(Journal::new(Rc::clone(&journaled)));
        world.borrow_mut().oms[i].orders = Some(session.orders());
        sessions.push(session);
        stops.push(stop);
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
    let feed = PeerFeed::new(streams, MdLog(Rc::clone(&world), Some(host.changed())));
    let mut md = MdReplay::new(config, feed).unwrap();
    md.set_inputs(Inputs(Rc::clone(&world)));
    host.set_journal(Journal::new(Rc::clone(&journaled)));
    let mut decide = Decider(Rc::clone(&world));
    let closed = ClosedLoop {
        clock: clock.clone(),
        records: records(root),
        settle,
        stops,
    };
    let mut members = vec![Member::md_replay(&mut md, closed)];
    members.extend(sessions.iter_mut().map(Member::exec));
    host.run(members, &mut decide).await.unwrap();
    let passes = host.passes();
    drop((sessions, md, decide, host));

    let world = Rc::try_unwrap(world).ok().unwrap().into_inner();
    assert!(world.oms.iter().all(|o| o.laddered.is_empty()));
    let timers_not_armed = world.timers.not_armed();
    let (seen, submitted) = (world.seen, world.submitted);
    // The sessions' orders, which the registries' brackets held, journal too.
    drop(world.oms);
    let journal = Rc::try_unwrap(journaled).ok().unwrap().into_inner().0;
    let _ = std::fs::remove_dir_all(&leases);
    Run {
        seen,
        submitted,
        journal,
        counters: peers.iter().map(|p| p.borrow().counters()).collect(),
        passes,
        timers_not_armed,
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_closed_loop_replay_through_the_shard_host_gives_the_same_commands_fills_and_cycles_on_every_run()
 {
    let root = fresh_dir("journal");
    journal(&root, true);
    let real = std::time::Instant::now();
    let first = closed_loop(&root, Duration::from_secs(1)).await;
    let second = closed_loop(&root, Duration::from_secs(1)).await;
    // An hour of journal, replayed twice in virtual time: nothing slept on a real clock.
    assert!(
        real.elapsed() < Duration::from_secs(120),
        "{:?}",
        real.elapsed()
    );

    // Identical commands, outcomes, fills, Cycle records and every record the shard journaled,
    // stamps on both clocks included, in the same order.
    assert_eq!(first.submitted, second.submitted);
    assert_eq!(first.seen, second.seen);
    assert_eq!(first.cycles(), second.cycles());
    assert_eq!(first.journal, second.journal);
    assert_eq!(first, second);

    // No clock was read: every stamp's wall time is the recorded origin's plus its monotonic
    // time, as the journal's stamps are.
    for seen in &first.seen {
        if let Seen::Md(stamp, _) | Seen::Exec(_, stamp, _) = seen {
            let wall = WALL0 + i64::try_from(stamp.recv_mono.0).unwrap();
            assert_eq!(stamp.recv_wall, WallNs(wall), "{seen:?}");
        }
    }
    // The consumer's timer armed nothing live: its firing came back from the journal.
    assert_eq!(first.timers_not_armed, 1);

    // The market data reached the consumer at its recorded instants, in the journal's order,
    // each with the shard's next ingest sequence: every input, the venues' answers included, in
    // one ingest order.
    let md: Vec<MonoNs> = first
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::Md(stamp, MdEvent::Trade { .. }) => Some(stamp.recv_mono),
            _ => None,
        })
        .collect();
    let trades: Vec<MonoNs> = [
        3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000, 10_000, 3_600_500,
    ]
    .map(at)
    .to_vec();
    assert_eq!(md, trades);
    let seqs: Vec<u64> = first
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::Md(stamp, _) | Seen::Exec(_, stamp, _) => Some(stamp.ingest_seq),
            Seen::Input(input) => Some(input.ingest_seq),
            _ => None,
        })
        .collect();
    let mut sorted = seqs.clone();
    sorted.dedup();
    assert!(sorted.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");

    // The journaled inputs were handed back at their places: the Start after the book's last
    // record and before the next, and the timer's firing after the last trade at the bid and
    // before the trade at the ask an hour on; each at its recorded instant.
    let place_of = |p: &dyn Fn(&Seen) -> bool| first.seen.iter().position(p).unwrap();
    let start = place_of(&|s| matches!(s, Seen::Input(i) if i.kind == START));
    let wind_down = place_of(&|s| matches!(s, Seen::Input(i) if i.kind == WIND_DOWN));
    let book_end = place_of(&|s| matches!(s, Seen::Md(_, MdEvent::BookSnapshotEnd { .. })));
    let md_at =
        |ms: u64| place_of(&|s| matches!(s, Seen::Md(stamp, _) if stamp.recv_mono == at(ms)));
    assert!(
        book_end < start && start < md_at(2_000),
        "{book_end} {start}"
    );
    assert!(md_at(10_000) < wind_down && wind_down < md_at(3_600_500));
    let Seen::Input(started) = &first.seen[start] else {
        unreachable!()
    };
    assert_eq!(
        (started.at, started.bytes.as_slice()),
        (at(START_AT), &b"start"[..])
    );
    let Seen::Input(wound) = &first.seen[wind_down] else {
        unreachable!()
    };
    assert_eq!(wound.at, at(WIND_DOWN_AT));

    // A Cycle record per decide pass, each naming the last ingest sequence the pass covered.
    let cycles = first.cycles();
    assert_eq!(u64::try_from(cycles.len()).unwrap(), first.passes);
    // A pass after market data decides for its instrument; one after the venues' answers alone
    // for none.
    assert!(cycles.iter().any(|c| c.1 == [INST]), "{cycles:?}");
    assert!(cycles.iter().any(|c| c.1.is_empty()), "{cycles:?}");
    let decided: Vec<Vec<InstrumentId>> = first
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::Decide(instruments) => Some(instruments.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        decided,
        cycles.iter().map(|c| c.1.clone()).collect::<Vec<_>>()
    );
    assert!(cycles.windows(2).all(|w| w[0].0 < w[1].0), "{cycles:?}");
    let passes = first
        .seen
        .iter()
        .filter(|s| matches!(s, Seen::Decide(_)))
        .count();
    assert_eq!(u64::try_from(passes).unwrap(), first.passes);
    assert!(!first.seen.contains(&Seen::Unsubmitted));

    let latency = TO_VENUE + TO_CLIENT;
    for (i, bracket) in BRACKETS.into_iter().enumerate() {
        let commands = first.commands(i);
        // The first command is the buy the decide pass after the Start planned: written at the
        // Start's replayed instant and answered by the bracket's SimVenue the latency later.
        let buy = commands[0];
        assert!(
            matches!(
                buy.cmd,
                Cmd::Place {
                    side: Side::Buy,
                    px: BID,
                    qty: QTY,
                    ..
                }
            ),
            "{bracket:?}: {buy:?}"
        );
        assert_eq!(buy.handling, None, "a ladder command");
        assert_eq!(first.written(i, buy.rpc), at(START_AT), "{bracket:?}");
        // Every command, inline or from the ladder, is answered at the replayed instant it was
        // written plus the configured latency.
        for cmd in &commands {
            let written = first.written(i, cmd.rpc);
            let (answered, outcome) = first.outcome(i, cmd.rpc);
            assert_eq!(
                answered.recv_mono,
                written + latency,
                "{bracket:?}: {cmd:?}"
            );
            assert!(
                matches!(outcome, SubmitOutcome::Accepted { .. }),
                "{bracket:?}: {cmd:?} {outcome:?}"
            );
            // An inline command is written at the instant of the input that made it.
            if let Some(handling) = cmd.handling {
                assert_eq!(written, handling.recv_mono, "{bracket:?}: {cmd:?}");
            }
        }
        // Each offer at the ask was made by a buy's fill, of the lots that fill bought.
        let buys: Vec<(Stamp, i64)> = first
            .fills(i)
            .into_iter()
            .filter(|f| f.1 == Side::Buy)
            .map(|f| (f.0, f.3))
            .collect();
        let offers: Vec<(Stamp, i64)> = commands
            .iter()
            .filter_map(|c| match c.cmd {
                Cmd::Place {
                    side: Side::Sell,
                    qty,
                    ..
                } => Some((c.handling.unwrap(), qty)),
                _ => None,
            })
            .collect();
        assert_eq!(offers, buys, "{bracket:?}");
        // The wind-down's firing cancelled what rested, each answered the latency after it, and
        // nothing filled after it.
        let cancels: Vec<&&Submitted> = commands
            .iter()
            .filter(|c| matches!(c.cmd, Cmd::Cancel { .. }))
            .collect();
        assert!(!cancels.is_empty(), "{bracket:?}");
        for cancel in cancels {
            assert_eq!(cancel.handling.unwrap().recv_mono, at(WIND_DOWN_AT));
        }
        assert!(
            first
                .fills(i)
                .iter()
                .all(|f| f.0.recv_mono < at(WIND_DOWN_AT)),
            "{bracket:?}"
        );
    }
    // Closed loop: the brackets filled in their order, so they decided different commands from
    // the same market data.
    let shape = |i: usize| -> Vec<(Side, i64)> {
        first
            .commands(i)
            .iter()
            .filter_map(|c| match c.cmd {
                Cmd::Place { side, qty, .. } => Some((side, qty)),
                Cmd::Cancel { .. } => None,
            })
            .collect()
    };
    assert_ne!(shape(0), shape(2));
    let (buy, sell) = (Side::Buy, Side::Sell);
    // Pessimistic and Middle each bought both lots in one fill, offered them, bid again once
    // both were sold and offered the second buy; Optimistic bought its first lots one at a
    // time and offered each as it came.
    let whole = vec![(buy, 2), (sell, 2), (buy, 2), (sell, 2)];
    assert_eq!(shape(0), whole);
    assert_eq!(shape(1), whole);
    assert_eq!(
        shape(2),
        [(buy, 2), (sell, 1), (sell, 1), (buy, 2), (sell, 2)]
    );
    for i in 0..BRACKETS.len() {
        // The second buy went out in the decide pass after the fill that left the bracket
        // flat, at that fill's replayed instant.
        let flat = first
            .fills(i)
            .into_iter()
            .rfind(|f| f.1 == Side::Sell)
            .unwrap();
        let rebid = first
            .commands(i)
            .into_iter()
            .filter(|c| {
                matches!(
                    c.cmd,
                    Cmd::Place {
                        side: Side::Buy,
                        ..
                    }
                )
            })
            .nth(1)
            .unwrap();
        assert_eq!(rebid.handling, None);
        assert_eq!(first.written(i, rebid.rpc), flat.0.recv_mono, "{i}");
    }
    let first_fill = |i: usize| first.fills(i)[0].0.ingest_seq;
    let (pess, mid, opt) = (first_fill(0), first_fill(1), first_fill(2));
    assert!(opt < mid && mid < pess, "{opt} {mid} {pess}");
    // Every venue took every frame and envelope it was handed.
    assert_eq!(first.counters, vec![PeerCounters::default(); 3]);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------------------------
// What stops a closed-loop replay.
// ---------------------------------------------------------------------------------------------

/// A replay member over `records` on `clock`, with no order-entry session: how its run ends.
async fn replay_alone(
    clock: IngestClock,
    records: Vec<Result<Record, JournalError>>,
) -> Result<(), ShardError> {
    alone(clock, records, Duration::ZERO).await.0
}

/// [`replay_alone`]: how the run ended, the replay, and what its handler saw.
async fn alone(
    clock: IngestClock,
    records: Vec<Result<Record, JournalError>>,
    settle: Duration,
) -> (Result<(), ShardError>, MdReplay<MdLog>, Shared) {
    let config = MdReplayConfig {
        venue: toy::ToyVenue::leak(),
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain("ws://127.0.0.1:9/replayed"),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        conn: MD.conn,
    };
    let world = solo();
    let mut md = MdReplay::new(config, MdLog(Rc::clone(&world), None)).unwrap();
    let mut host = ShardHost::new(clock.clone());
    let closed = ClosedLoop {
        clock,
        records,
        settle,
        stops: Vec::new(),
    };
    let decided = Rc::clone(&world);
    let mut decide = move |instruments: &[InstrumentId], _: &mut Ladder| {
        let pass = Seen::Decide(instruments.to_vec());
        decided.borrow_mut().seen.push(pass);
    };
    let ran = host
        .run(vec![Member::md_replay(&mut md, closed)], &mut decide)
        .await;
    (ran, md, world)
}

fn solo() -> Shared {
    Rc::new(RefCell::new(World {
        seen: Vec::new(),
        oms: Vec::new(),
        submitted: Vec::new(),
        handling: None,
        timers: ConsumerTimers::inert(),
    }))
}

fn opened() -> Record {
    Record::Control {
        at: at(1),
        ev: ControlEvent::Opened(MD),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_closed_loop_replay_on_a_live_shards_clock_is_refused() {
    let ran = replay_alone(IngestClock::new(), vec![Ok(opened())]).await;
    assert_eq!(ran, Err(ShardError::Replay(ReplayFailure::LiveClock)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_journal_that_cannot_be_read_stops_the_replay_and_the_run() {
    let ran = replay_alone(
        IngestClock::replayed(WallNs(WALL0)),
        vec![Ok(opened()), Err(JournalError::TooLarge)],
    )
    .await;
    let Err(ShardError::Replay(ReplayFailure::Journal(why))) = ran else {
        panic!("{ran:?}");
    };
    assert!(why.contains("too large"), "{why}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_damaged_text_frame_stops_the_replay_and_the_run() {
    let stamp = Stamp {
        ingest_seq: 7,
        kernel_rx: None,
        recv_mono: at(2),
        recv_wall: WallNs(WALL0),
        conn: MD,
    };
    let damaged = Record::Inbound {
        stamp,
        opcode: fbc_journal::Opcode::Text,
        bytes: Opaque(vec![0xff, 0xfe]),
        redact: Vec::new(),
    };
    let ran = replay_alone(
        IngestClock::replayed(WallNs(WALL0)),
        vec![Ok(opened()), Ok(damaged)],
    )
    .await;
    assert_eq!(
        ran,
        Err(ShardError::Replay(ReplayFailure::NotUtf8 { ingest_seq: 7 }))
    );
}

/// A closed-loop replay feeds its connection's timer firings and HTTP results at their
/// recorded instants too, each restamped into the shard's ingest order; once it has played, the
/// same replay fed on its own stamps with the recorded stamps again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_closed_loop_replay_feeds_timers_and_http_results_and_then_lets_go_of_the_shard() {
    let stamp = |seq: u64, ms: u64| Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: at(ms),
        recv_wall: WallNs(WALL0 + i64::try_from(ms * MS).unwrap()),
        conn: MD,
    };
    let records = vec![
        Ok(opened()),
        Ok(Record::Timer {
            stamp: stamp(40, 2),
            tag: fbc_core::TimerTag(1),
        }),
        Ok(Record::HttpResult {
            stamp: stamp(41, 3),
            tag: fbc_core::HttpTag(1),
            result: Err(fbc_core::HttpFailure::Lost),
        }),
        Ok(Record::inbound(
            stamp(42, 4),
            RawFrame::Text(&trade(BID, 1, 1)),
        )),
    ];
    let clock = IngestClock::replayed(WallNs(WALL0));
    let (ran, mut md, world) = alone(clock, records, Duration::ZERO).await;
    assert_eq!(ran, Ok(()));
    let counters = md.counters();
    assert_eq!((counters.timers, counters.http, counters.frames), (1, 1, 1));
    // The trade took the shard's third ingest sequence, at its recorded instant.
    let trades = |world: &Shared| -> Vec<Stamp> {
        world
            .borrow()
            .seen
            .iter()
            .filter_map(|s| match s {
                Seen::Md(stamp, MdEvent::Trade { .. }) => Some(*stamp),
                _ => None,
            })
            .collect()
    };
    assert_eq!(
        trades(&world),
        [Stamp {
            ingest_seq: 2,
            ..stamp(42, 4)
        }]
    );
    md.feed(&Record::inbound(
        stamp(43, 5),
        RawFrame::Text(&trade(BID, 1, 2)),
    ))
    .unwrap();
    assert_eq!(trades(&world)[1], stamp(43, 5));
}

/// What stops a closed-loop replay says why; its configuration shows no records or controls.
#[test]
fn what_stops_a_closed_loop_replay_says_why() {
    let said = [
        ReplayFailure::LiveClock,
        ReplayFailure::Journal("cut short".to_owned()),
        ReplayFailure::NotUtf8 { ingest_seq: 7 },
    ]
    .map(|failure| ShardError::Replay(failure).to_string());
    assert_eq!(
        said,
        [
            "a closed-loop replay stopped: a closed-loop replay needs a replayed clock, not a live one",
            "a closed-loop replay stopped: the journal cannot be read: cut short",
            "a closed-loop replay stopped: the text frame of ingest sequence 7 is not UTF-8",
        ]
    );
    let closed = ClosedLoop {
        clock: IngestClock::new(),
        records: Vec::<Result<Record, JournalError>>::new(),
        settle: ms(5),
        stops: Vec::new(),
    };
    assert_eq!(
        format!("{closed:?}"),
        "ClosedLoop { settle: 5ms, stops: 0, .. }"
    );
}

/// The stamp of a record of the replayed connection, recorded at `ms` as ingest sequence `seq`.
fn md_stamp(seq: u64, ms: u64) -> Stamp {
    Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: at(ms),
        recv_wall: WallNs(WALL0 + i64::try_from(ms * MS).unwrap()),
        conn: MD,
    }
}

/// What reached the solo world: its market data's stamps and its decide passes, in order.
fn passes_and_md(world: &Shared) -> Vec<Option<u64>> {
    world
        .borrow()
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::Md(stamp, _) => Some(Some(stamp.ingest_seq)),
            Seen::Decide(_) => Some(None),
            _ => None,
        })
        .collect()
}

/// Codex P2 r4315555312 on PR #178: the settle time runs from the last recorded instant of the
/// journal, a record the replay passes over included.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_settle_time_runs_from_the_journals_last_recorded_instant() {
    let start = tokio::time::Instant::now();
    let records = vec![
        Ok(opened()),
        Ok(Record::Outbound {
            at: at(50),
            conn: ConnKey { conn: 9, epoch: 1 },
            rpc: None,
            opcode: fbc_journal::Opcode::Text,
            frame: fbc_core::WireSlice::plain(b"live order entry".to_vec()),
        }),
    ];
    let clock = IngestClock::replayed(WallNs(WALL0));
    let (ran, _, _) = alone(clock, records, ms(5)).await;
    assert_eq!(ran, Ok(()));
    assert!(start.elapsed() >= ms(55), "{:?}", start.elapsed());
}

/// Codex P2 r4315555320 on PR #178: a ping the live session stamped takes its place in the
/// shard's ingest order, and calls for a decide pass, though no codec is handed it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_ping_the_live_session_stamped_takes_its_place_in_the_ingest_order() {
    let records = vec![
        Ok(opened()),
        Ok(Record::InboundControl {
            stamp: md_stamp(30, 3),
            frame: fbc_journal::WsControl::Ping(Opaque(b"hb".to_vec())),
        }),
        Ok(Record::inbound(
            md_stamp(31, 4),
            RawFrame::Text(&trade(BID, 1, 1)),
        )),
    ];
    let clock = IngestClock::replayed(WallNs(WALL0));
    let (ran, _, world) = alone(clock, records, Duration::ZERO).await;
    assert_eq!(ran, Ok(()));
    assert_eq!(passes_and_md(&world), [None, Some(1), None]);
}

/// Codex P2 r4315555331 on PR #178: an HTTP result held until its epoch's codec is built takes
/// its ingest sequence as its events are handed on, so the pass after it decides on them.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_held_http_result_takes_its_ingest_sequence_as_its_events_are_handed_on() {
    let answered = Record::HttpResult {
        stamp: md_stamp(20, 2),
        tag: fbc_core::HttpTag(1),
        result: Ok(fbc_journal::HttpResponseRec {
            status: 200,
            headers: vec![],
            body: Opaque(trade(BID, 1, 1).into_bytes()),
            body_redact: Vec::new(),
        }),
    };
    let subscribed = Record::Control {
        at: at(5),
        ev: ControlEvent::Subscribe {
            conn: MD,
            add: vec![toy::sub(1)],
            remove: vec![],
        },
    };
    let records = vec![
        Ok(opened()),
        Ok(answered),
        Ok(subscribed),
        Ok(Record::inbound(
            md_stamp(21, 6),
            RawFrame::Text(&trade(BID, 1, 2)),
        )),
    ];
    let clock = IngestClock::replayed(WallNs(WALL0));
    let (ran, _, world) = alone(clock, records, Duration::ZERO).await;
    assert_eq!(ran, Ok(()));
    assert_eq!(passes_and_md(&world), [Some(0), None, Some(1), None]);
}

/// Codex P2 r4315555339 on PR #178: a replay whose run is dropped while it waits for a record
/// stamps with the recorded stamps again when fed on its own.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_replay_dropped_while_it_waits_stamps_with_the_recorded_stamps_again() {
    let config = MdReplayConfig {
        venue: toy::ToyVenue::leak(),
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain("ws://127.0.0.1:9/replayed"),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        conn: MD.conn,
    };
    let world = solo();
    let mut md = MdReplay::new(config, MdLog(Rc::clone(&world), None)).unwrap();
    let clock = IngestClock::replayed(WallNs(WALL0));
    let mut host = ShardHost::new(clock.clone());
    let records: Vec<Result<Record, JournalError>> = vec![
        Ok(opened()),
        Ok(Record::inbound(
            md_stamp(40, 2),
            RawFrame::Text(&trade(BID, 1, 1)),
        )),
        Ok(Record::inbound(
            md_stamp(41, 100),
            RawFrame::Text(&trade(BID, 1, 2)),
        )),
    ];
    let closed = ClosedLoop {
        clock,
        records,
        settle: Duration::ZERO,
        stops: Vec::new(),
    };
    let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
    tokio::select! {
        ran = host.run(vec![Member::md_replay(&mut md, closed)], &mut decide) => {
            panic!("the run ended first: {ran:?}")
        }
        () = tokio::time::sleep(ms(50)) => {}
    }
    md.feed(&Record::inbound(
        md_stamp(42, 120),
        RawFrame::Text(&trade(BID, 1, 3)),
    ))
    .unwrap();
    let seqs: Vec<u64> = passes_and_md(&world).into_iter().flatten().collect();
    assert_eq!(seqs, [0, 42]);
}

/// Codex P2 r4315555356 on PR #178: answers due exactly as the replay settles are read, and
/// decided on, before the replay ends the sessions.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn answers_due_as_the_replay_settles_are_read_and_decided_on_before_the_sessions_end() {
    let root = fresh_dir("journal");
    journal(&root, false);
    // The wind-down's cancels are answered exactly the latency after the journal's last record.
    let run = closed_loop(&root, TO_VENUE + TO_CLIENT).await;
    for i in 0..BRACKETS.len() {
        for cancel in run
            .commands(i)
            .into_iter()
            .filter(|c| matches!(c.cmd, Cmd::Cancel { .. }))
        {
            let (answered, outcome) = run.outcome(i, cancel.rpc);
            assert_eq!(answered.recv_mono, at(WIND_DOWN_AT) + TO_VENUE + TO_CLIENT);
            assert!(
                matches!(outcome, SubmitOutcome::Accepted { .. }),
                "{outcome:?}"
            );
        }
    }
    // A decide pass came after the last of them.
    let last_exec = run
        .seen
        .iter()
        .rposition(|s| matches!(s, Seen::Exec(..)))
        .unwrap();
    assert!(
        run.seen[last_exec..]
            .iter()
            .any(|s| matches!(s, Seen::Decide(_))),
        "{:?}",
        &run.seen[last_exec..]
    );
    let _ = std::fs::remove_dir_all(&root);
}
