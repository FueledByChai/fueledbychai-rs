//! FBC-w9g's done line (BT-501; decisions 0006, 0014, 0046, 0057, 0058, 0060, 0104): a recorded
//! market-data day replayed through SimVenue gives the same fills on every run.
//!
//! The runtime's toy market-data venue (included by path) runs live over a WebSocket of the
//! test's own on 127.0.0.1, for two connection epochs, and the runtime journals the session:
//! market data only, no order entry. The journal is then replayed twice through fbc-runtime's
//! decoder replay (`MdReplay`, FBC-3kz), each time into a fresh SimVenue per bracket
//! (Pessimistic, Middle, Optimistic) on FBC-6mf's hosting: every envelope reaches each bracket's
//! `SimPeer` through a `PeerFeed`, in the journal's ingest order, before the consumer's handler.
//!
//! A fixed command script names commands at journal positions (the n-th inbound frame of the
//! session): each is built and authorized by the bracket's own fbc-oms registry, re-checked at
//! submit, encoded by the bracket's `SimCodec` and handed to its peer as its frame. As the
//! session does on every epoch (0058), each bracket's stream is opened, its greeting read as the
//! stream authenticated, its cancel-on-disconnect armed and its resync read before the owner's
//! Start arms the market.
//!
//! Time is the journal's: each command is encoded at the recorded stamp of the input it follows,
//! each answer is read at the instant the peer says it is due, and so SimVenue's latency is
//! measured in the recorded stamps. Nothing here reads a clock during a replay.
//!
//! The order-entry session's own loop (`ExecSession`) is not run here: it sleeps on the shard's
//! clock. A closed-loop replay through it in virtual time is FBC-066n's. The venue, its caps,
//! its account names and its rates are synthetic.

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

use fbc_core::Bps;
use fbc_core::{
    AccountKey, AccountLease, AckModel, BookId, CancelOnDisconnect, Channel, CidMint,
    ClientIdFormat, ClientOrderId, ConnKey, ConnState, ConnTopology, Effect, Effects, EncodeCtx,
    Encoding, EndpointPlan, Envelope, ExecCaps, ExecCodec, ExecEvent, ExecSink, FeeBook, FeeEntry,
    FeeKey, FeeRate, FeeSource, FeedSource, FillCaps, FillSource, FundingCaps, InstrumentId,
    Liquidity, Lots, MarketLease, MatchingCaps, MdCaps, MdEvent, MdTransport, MonoNs, Namespace,
    NamespaceLease, NewOrder, NonceBlock, NonceScope, OrderCaps, OrderKind, OrderKindTag,
    OrderingKey, PathStamps, RawFrame, Readiness, RefKind, RpcId, Side, SnapshotReads,
    SnapshotSource, StpScope, StreamId, SubmitOutcome, Support, TagSet, Ticks, Tif, TifTag,
    TradeCaps, VenueCaps, VenueCommand, VenueConfig, VenueFeeSign, VenueMeta, VenueOrderState,
    WallNs, WireUrl, dispatch,
};
use fbc_journal::{JournalReader, JournalWriter, Record, RedactionKey, SinkConfig, journal_queue};
use fbc_oms::{
    CancelChoice, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey, PreTradeCaps,
    Registry, ResyncSnapshot,
};
use fbc_runtime::{
    Connector, InProcessStream, IngestClock, Journal, Liveness, MdHandler, MdReplay,
    MdReplayConfig, MdSession, MdSessionConfig, OrderEntryPeer, PeerFeed, ProxyConfig,
    ReconnectPacing, WriteStall,
};
use fbc_sim::{Bracket, PeerCounters, QueueConfig, SimCodec, SimConfig, SimLatency, SimPeer};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// The toy's instrument `A`, its trading book channel, and SimVenue's order-entry stream.
const INST: InstrumentId = InstrumentId::new(1);
const BOOK: BookId = BookId(0);
const STREAM: StreamId = StreamId(5);
const NS: Namespace = Namespace::new(9);
const TO_VENUE: Duration = Duration::from_millis(5);
const TO_CLIENT: Duration = Duration::from_millis(3);
/// The market-data session's connection number, and the shard whose journal it writes.
const CONN: u16 = 1;
const SHARD: u16 = 3;
/// The three brackets, in the order the test names them.
const BRACKETS: [Bracket; 3] = [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic];

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A directory of its own under the test target's scratch space, new on every call, so the
/// tests of this binary, which run at once, never share one.
fn fresh_dir(what: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "fbc-sim-replayed-{}-{what}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------------------------
// SimVenue, standing in for a synthetic venue with per-connection cancel-on-disconnect, as an
// order-entry session requires (decision 0058).
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

fn order_caps() -> OrderCaps {
    stood_caps().exec.unwrap().order
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

/// SimVenue for bracket `i`, under its own account.
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

// ---------------------------------------------------------------------------------------------
// The live session the journal records: toy market data, two epochs.
// ---------------------------------------------------------------------------------------------

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[4; 32]).unwrap())
}

/// The endpoint the session ran: trades and book channel 0 of `A` at `url`.
fn plan(url: &str) -> EndpointPlan {
    EndpointPlan {
        stream: toy::STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain(url.to_owned()),
        },
        subs: vec![toy::sub(1), toy::book(1, BOOK)],
    }
}

/// What the live session handed its handler: its envelopes and the epochs that ended.
#[derive(Default)]
struct Live {
    envs: Vec<Envelope<MdEvent>>,
    ends: Vec<ConnKey>,
}

#[derive(Clone)]
struct LiveLog(Rc<RefCell<Live>>);

impl MdHandler for LiveLog {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.borrow_mut().envs.push(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().ends.push(key);
    }
}

/// A market-data server on a 127.0.0.1 ephemeral port that takes one connection after another:
/// it sends each text the test hands it on the connection open at the time, and reads (and
/// drops) what the session sends. Its URL, and where the test hands it texts.
async fn md_server() -> (String, mpsc::UnboundedSender<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/md", listener.local_addr().unwrap());
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            loop {
                tokio::select! {
                    text = rx.recv() => match text {
                        Some(text) => ws.send(Message::text(text)).await.unwrap(),
                        None => return,
                    },
                    read = ws.next() => if !matches!(read, Some(Ok(m)) if !m.is_close()) {
                        break;
                    },
                }
            }
        }
    });
    (url, tx)
}

/// Lets the live session run, a millisecond at a time, until `done`. The live run is on the
/// real clock, so a loaded machine only stretches its stamps (a paused clock would race ahead
/// of the loopback socket's reads); a replay reads none.
async fn until(live: &Rc<RefCell<Live>>, done: impl Fn(&Live) -> bool) {
    for _ in 0..30_000 {
        if done(&live.borrow()) {
            return;
        }
        tokio::time::sleep(ms(1)).await;
    }
    panic!("not reached");
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

/// The texts the server sends, in groups the test lets at least 50 ms pass between: so every
/// group's stamps are well apart, and each command's answers are due before the next group. `None` asks the session to reconnect and waits for the epoch's end.
fn day() -> Vec<Option<Vec<String>>> {
    vec![
        // Epoch 1. Frames 0-3: the trading book, bids 199 x 6 and asks 201 x 5.
        Some(vec![
            book_record("begin", 1, "|epoch=1"),
            level("bid", 199, 6, 2),
            level("ask", 201, 5, 3),
            book_record("end", 4, ""),
        ]),
        // Frame 4: a new ask level, which the script places after.
        Some(vec![level("ask", 202, 4, 5)]),
        // Frame 5: the bid shrinks by three with no trade.
        Some(vec![level("bid", 199, 3, 6)]),
        // Frames 6-8: three trades at the bid.
        Some(vec![trade(199, 4, 10)]),
        Some(vec![trade(199, 2, 11)]),
        Some(vec![trade(199, 2, 12)]),
        // Frame 9: a trade at the ask.
        Some(vec![trade(201, 3, 13)]),
        // Frame 10: the ask shrinks, which the script cancels after.
        Some(vec![level("ask", 201, 2, 7)]),
        // Frame 11: the connection drops.
        None,
        // Epoch 2. Frames 12-15: the book again, bids 199 x 4 and asks 201 x 5.
        Some(vec![
            book_record("begin", 1, "|epoch=2"),
            level("bid", 199, 4, 2),
            level("ask", 201, 5, 3),
            book_record("end", 4, ""),
        ]),
        // Frame 16: a trade at the bid big enough for every bracket.
        Some(vec![trade(199, 5, 20)]),
        // Frame 17: a quiet level change, after every answer is due.
        Some(vec![level("ask", 202, 1, 5)]),
    ]
}

/// Runs the toy session live, journaling it at `root`: what its handler was handed.
async fn record(root: &Path) -> Live {
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(root, SHARD, key()).unwrap())
        .unwrap();
    let sink = Rc::new(RefCell::new(sink));
    let (url, md) = md_server().await;
    let venue = toy::ToyVenue::leak();
    let config = MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: plan(&url),
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let live = Rc::new(RefCell::new(Live::default()));
    let (mut session, control) = MdSession::new(config, LiveLog(Rc::clone(&live))).unwrap();
    session.set_journal(Journal::new(sink));
    let drive = async {
        let mut sent = 0;
        for group in day() {
            match group {
                Some(texts) => {
                    sent += texts.len();
                    for text in texts {
                        md.send(text).unwrap();
                    }
                    until(&live, |l| l.envs.len() == sent).await;
                }
                None => {
                    md.send("bye".to_owned()).unwrap();
                    let ends = live.borrow().ends.len();
                    until(&live, |l| l.ends.len() == ends + 1).await;
                }
            }
            tokio::time::sleep(ms(50)).await;
        }
        drop(control);
    };
    let (ran, ()) = tokio::join!(session.run(), drive);
    ran.unwrap();
    writer.close().unwrap();
    live.take()
}

// ---------------------------------------------------------------------------------------------
// The command script.
// ---------------------------------------------------------------------------------------------

/// One command of the script, named `name`: what to do, on every bracket, right after the
/// consumer's handler took the envelopes of the session's `frame`-th inbound frame.
#[derive(Clone, Debug)]
struct Step {
    frame: usize,
    name: &'static str,
    cmd: Cmd,
}

#[derive(Clone, Debug)]
enum Cmd {
    /// A post-only limit order of `qty` lots at `px`, known to the script by its step's name.
    Place { side: Side, px: i64, qty: i64 },
    /// The cancel of the order the step named `of` placed.
    Cancel { of: &'static str },
}

fn place(frame: usize, name: &'static str, side: Side, px: i64, qty: i64) -> Step {
    let cmd = Cmd::Place { side, px, qty };
    Step { frame, name, cmd }
}

/// The day's script: a buy before the market is armed, which fbc-oms refuses; a buy at the bid,
/// a sell at the ask and a post-only buy that would cross; the sell's cancel; and, after the
/// reconnect, a second buy at the bid.
fn script() -> Vec<Step> {
    vec![
        place(0, "early", Side::Buy, 199, 1),
        place(4, "bid", Side::Buy, 199, 2),
        place(4, "ask", Side::Sell, 201, 2),
        place(4, "cross", Side::Buy, 201, 1),
        Step {
            frame: 10,
            name: "cancel",
            cmd: Cmd::Cancel { of: "ask" },
        },
        place(15, "again", Side::Buy, 199, 1),
    ]
}

/// The ingest sequence of each inbound frame of the session's connection in the journal at
/// `root`, in journal order: the journal positions a script's frame numbers name.
fn positions(root: &Path) -> Vec<u64> {
    JournalReader::open(root, SHARD)
        .unwrap()
        .filter_map(|record| match record.unwrap() {
            Record::Inbound { stamp, .. } if stamp.conn.conn == CONN => Some(stamp.ingest_seq),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The replay host: one SimVenue per bracket, fed by the replay, driven by the script.
// ---------------------------------------------------------------------------------------------

/// What a replay produced, in the order it produced it.
#[derive(Clone, PartialEq, Debug)]
enum Seen {
    /// An envelope the consumer's handler took.
    Md(Envelope<MdEvent>),
    /// A market-data connection epoch ended.
    EpochEnd(ConnKey),
    /// The script's step `step` on bracket `bracket`: the request it went as, at `at`.
    Sent {
        bracket: usize,
        step: &'static str,
        rpc: RpcId,
        at: MonoNs,
    },
    /// The script's step `step`, which fbc-oms refused on bracket `bracket`: nothing reached
    /// the venue.
    Refused {
        bracket: usize,
        step: &'static str,
        at: MonoNs,
    },
    /// One event decoded from an answer of bracket `bracket`'s venue, read at `at`, the
    /// instant the peer gave it, `wall` on the recorded wall clock.
    Exec {
        bracket: usize,
        at: MonoNs,
        wall: WallNs,
        meta: VenueMeta,
        ev: ExecEvent,
    },
}

/// One bracket: its venue, its codec and its fbc-oms registry, and what its session would
/// track of its epoch.
struct Arm {
    peer: Rc<RefCell<SimPeer>>,
    codec: SimCodec,
    reg: Registry,
    mint: CidMint,
    acct: AccountKey,
    /// Where the bracket's leases are taken, under the account name `name`.
    dir: PathBuf,
    name: String,
    next_rpc: u64,
    /// The cancel-on-disconnect arm in flight.
    arming: Option<RpcId>,
    /// The resync being read, and when it was asked.
    collecting: Option<ResyncSnapshot>,
    /// The script's orders, by the name of the step that placed them.
    cids: BTreeMap<&'static str, ClientOrderId>,
    /// Answers read, for the ingest key the registry orders a resync by.
    read: u64,
}

impl Arm {
    fn new(i: usize, dir: &Path) -> Arm {
        let name = format!("sim-{i}");
        let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
        let keys = LeaseKeys::new("sim", &name, &order_caps()).with_market(INST, symbol);
        let limits = MarketCapsConfig {
            inventory: Some(lots(100)),
            resting: Some(lots(100)),
        };
        let caps = PreTradeCaps::new().with_market(INST, limits).unwrap();
        let ns = NamespaceLease::acquire(dir, account(i), NS).unwrap();
        Arm {
            peer: Rc::new(RefCell::new(SimPeer::new(&sim_config(i)))),
            codec: SimCodec::new(&sim_config(i)),
            reg: Registry::with_caps(caps).with_lease_keys(keys),
            mint: CidMint::new(ns, 0, 0, WallNs(1)),
            acct: account(i),
            dir: dir.to_path_buf(),
            name,
            next_rpc: 1,
            arming: None,
            collecting: None,
            cids: BTreeMap::new(),
            read: 0,
        }
    }

    fn rpc(&mut self) -> RpcId {
        let rpc = RpcId(self.next_rpc);
        self.next_rpc += 1;
        rpc
    }

    /// Encodes `cmd` as request `rpc` at `at` (`wall`), and hands each frame it writes to the
    /// venue, as the session writes them on its stream.
    fn write(&mut self, cmd: &VenueCommand, rpc: RpcId, at: MonoNs, wall: WallNs) {
        let ctx = EncodeCtx {
            wall,
            mono: at,
            nonces: NonceBlock::EMPTY,
        };
        let mut fx = Effects::new();
        self.codec
            .encode(
                cmd,
                rpc,
                &toy::specs(),
                &ctx,
                &mut PathStamps::off(),
                &mut fx,
            )
            .unwrap();
        self.hand_on(&fx);
    }

    fn hand_on(&mut self, fx: &Effects) {
        for effect in fx.as_slice() {
            let Effect::Send { stream, frame, .. } = effect else {
                panic!("SimCodec only writes frames: {effect:?}");
            };
            assert_eq!(*stream, STREAM);
            self.peer.borrow_mut().on_frame(frame.bytes());
        }
    }

    /// The stream opened at `at` (`wall`): the venue and the codec are told, as the session
    /// tells them on every epoch.
    fn open(&mut self, at: MonoNs, wall: WallNs) {
        self.peer.borrow_mut().open(at, wall);
        let ctx = EncodeCtx {
            wall,
            mono: at,
            nonces: NonceBlock::EMPTY,
        };
        let mut fx = Effects::new();
        self.codec.on_open(STREAM, &ctx, &mut fx);
        self.hand_on(&fx);
    }

    /// Decodes one frame the venue gave, as the session decodes one off its stream.
    fn decode(&mut self, frame: &[u8]) -> Vec<(VenueMeta, ExecEvent)> {
        #[derive(Default)]
        struct Sink(Vec<(VenueMeta, ExecEvent)>);
        impl ExecSink for Sink {
            fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
                self.0.push((meta, ev));
            }
        }
        let mut sink = Sink::default();
        let mut fx = Effects::new();
        let (codec, specs) = (&mut self.codec, toy::specs());
        dispatch(&stood_caps(), NS, |scope| {
            let text = RawFrame::Text(std::str::from_utf8(frame).unwrap());
            codec.on_frame(STREAM, text, scope, &specs, &mut sink, &mut fx)
        })
        .unwrap();
        assert!(fx.is_empty(), "{fx:?}");
        sink.0
    }

    /// What the session does with one event of its stream (decision 0058): once the stream is
    /// authenticated it arms cancel-on-disconnect; once that is accepted it asks a resync; and
    /// once the resync ends the registry is resynced and the owner's Start arms the market.
    fn react(&mut self, meta: &VenueMeta, ev: &ExecEvent, at: MonoNs, wall: WallNs) {
        self.read += 1;
        match ev {
            ExecEvent::Conn {
                state: ConnState::Authenticated,
                ..
            } => {
                let rpc = self.rpc();
                self.arming = Some(rpc);
                self.write(&VenueCommand::ArmCancelOnDisconnect(true), rpc, at, wall);
            }
            ExecEvent::Outcome {
                rpc,
                outcome: SubmitOutcome::Accepted { .. },
                ..
            } if self.arming == Some(*rpc) => {
                self.arming = None;
                let ctx = EncodeCtx {
                    wall,
                    mono: at,
                    nonces: NonceBlock::EMPTY,
                };
                let mut fx = Effects::new();
                self.codec.resync(&ctx, &mut fx);
                self.hand_on(&fx);
                self.collecting = Some(ResyncSnapshot {
                    watermark: wall,
                    requested_at: at,
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
                    venue: meta.venue_seq,
                    ingest: self.read,
                };
                let report = self.reg.resync(&ladder, &order_caps(), &snap, key).unwrap();
                assert!(!report.untrustworthy);
                let symbol = toy::specs().get(INST).unwrap().venue_symbol.clone();
                let market = MarketLease::acquire(&self.dir, "sim", &self.name, &symbol).unwrap();
                let acct = AccountLease::acquire(&self.dir, "sim", &self.name).unwrap();
                self.reg
                    .start(INST, Leases::market(market).with_account(acct))
                    .unwrap();
            }
            _ => {}
        }
    }

    /// The script's `step` at `at` (`wall`): built and authorized by the registry, re-checked at
    /// submit (decision 0060) and written; the request it went as, or `None` when fbc-oms
    /// refused it and nothing was written.
    fn command(&mut self, step: &Step, at: MonoNs, wall: WallNs) -> Option<RpcId> {
        let permitted = match step.cmd {
            Cmd::Place { side, px, qty } => {
                let cid = self.mint.mint().unwrap();
                self.cids.insert(step.name, cid);
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
                self.reg.place(order).ok()?
            }
            Cmd::Cancel { of } => {
                let cid = self.cids[of];
                match self.reg.cancellable(cid).ok()?.cancel(&order_caps()) {
                    CancelChoice::Send(cmd) => cmd,
                    CancelChoice::AwaitAck => return None,
                }
            }
        };
        let auth = self.reg.authorize(self.acct, permitted).ok()?;
        auth.check_at_submit().ok()?;
        let rpc = self.rpc();
        self.write(auth.command(), rpc, at, wall);
        Some(rpc)
    }
}

/// The replay's side of the hosting: the brackets, the script by journal position, and the
/// last recorded stamp, which every instant is read against.
struct Host {
    arms: Vec<Arm>,
    seen: Rc<RefCell<Vec<Seen>>>,
    /// The script's steps, by the ingest sequence of the frame they follow.
    steps: BTreeMap<u64, Vec<Step>>,
    /// The ingest sequences whose steps were given.
    given: BTreeSet<u64>,
    /// The last envelope's stamp on both clocks: an instant after it is that much later on
    /// the wall clock too.
    last: Option<(MonoNs, WallNs)>,
}

impl Host {
    /// The wall clock at `at`, read from the last recorded stamp: no clock is read.
    fn wall(&self, at: MonoNs) -> WallNs {
        let (mono, wall) = self.last.expect("the streams open at the first envelope");
        let since = i128::from(at.0) - i128::from(mono.0);
        WallNs(i64::try_from(i128::from(wall.0) + since).unwrap())
    }

    /// Reads every answer due by `until`, earliest first across the brackets (a tie to the
    /// bracket named first), each at the instant it is due, and acts on it as the session would.
    fn read_until(&mut self, until: MonoNs) {
        // Before the streams open there is nothing to read.
        if self.last.is_none() {
            return;
        }
        loop {
            let due = self
                .arms
                .iter()
                .enumerate()
                .filter_map(|(i, arm)| arm.peer.borrow().next_due().map(|at| (at, i)))
                .filter(|&(at, _)| at <= until)
                .min();
            let Some((at, i)) = due else {
                return;
            };
            let wall = self.wall(at);
            let frames = self.arms[i].peer.borrow_mut().take_due(at);
            for frame in frames {
                for (meta, ev) in self.arms[i].decode(&frame) {
                    self.arms[i].react(&meta, &ev, at, wall);
                    let seen = Seen::Exec {
                        bracket: i,
                        at,
                        wall,
                        meta,
                        ev,
                    };
                    self.seen.borrow_mut().push(seen);
                }
            }
        }
    }

    /// Before `env` reaches the venues: the streams open at the first envelope, and every
    /// answer due by its stamp is read.
    fn before(&mut self, env: &Envelope<MdEvent>) {
        let (mono, wall) = (env.stamp.recv_mono, env.stamp.recv_wall);
        if self.last.is_none() {
            self.last = Some((mono, wall));
            for arm in &mut self.arms {
                arm.open(mono, wall);
            }
        }
        self.read_until(mono);
        self.last = Some((mono, wall));
    }

    /// After the consumer's handler took `env`: the script's steps at its frame, on every
    /// bracket, at its stamp.
    fn after(&mut self, env: &Envelope<MdEvent>) {
        let seq = env.stamp.ingest_seq;
        if !self.given.insert(seq) {
            return;
        }
        let at = env.stamp.recv_mono;
        let wall = env.stamp.recv_wall;
        for step in self.steps.get(&seq).cloned().unwrap_or_default() {
            for (bracket, arm) in self.arms.iter_mut().enumerate() {
                let step_name = step.name;
                let seen = match arm.command(&step, at, wall) {
                    Some(rpc) => Seen::Sent {
                        bracket,
                        step: step_name,
                        rpc,
                        at,
                    },
                    None => Seen::Refused {
                        bracket,
                        step: step_name,
                        at,
                    },
                };
                self.seen.borrow_mut().push(seen);
            }
        }
    }
}

/// The consumer's handler, behind the brackets' peers: it records what it is handed.
struct Consumer(Rc<RefCell<Vec<Seen>>>);

impl MdHandler for Consumer {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.borrow_mut().push(Seen::Md(env));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().push(Seen::EpochEnd(key));
    }
}

/// The replay's market-data handler: the host reads the answers due first, then the peers and
/// the consumer take the envelope through the hosting's `PeerFeed`, then the script's steps at
/// its frame are given.
struct Replayed {
    host: Rc<RefCell<Host>>,
    feed: PeerFeed<Consumer>,
}

impl MdHandler for Replayed {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.host.borrow_mut().before(&env);
        self.feed.on_md(env.clone());
        self.host.borrow_mut().after(&env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.feed.on_epoch_end(key);
    }
}

/// The recorded instant a journal record carries, on the shard's monotonic clock.
fn recorded_at(record: &Record) -> Option<MonoNs> {
    match record {
        Record::Inbound { stamp, .. }
        | Record::InboundControl { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::Timer { stamp, .. }
        | Record::RpcTimeout { stamp, .. } => Some(stamp.recv_mono),
        Record::Control { at, .. }
        | Record::Outbound { at, .. }
        | Record::WriteResult { at, .. }
        | Record::HttpRequest { at, .. } => Some(*at),
        _ => None,
    }
}

/// What one replay gave: everything seen in order, and each venue's counters.
struct Run {
    seen: Vec<Seen>,
    counters: Vec<PeerCounters>,
}

/// Replays the journal at `root` through a fresh SimVenue per bracket, the script's steps at
/// the journal positions `positions` names, then reads every answer still due.
fn replay(root: &Path, positions: &[u64], script: &[Step]) -> Run {
    let dir = fresh_dir("leases");
    let arms: Vec<Arm> = (0..BRACKETS.len()).map(|i| Arm::new(i, &dir)).collect();
    let streams: Vec<InProcessStream> = arms
        .iter()
        .map(|arm| InProcessStream::new(Rc::clone(&arm.peer)))
        .collect();
    let mut steps: BTreeMap<u64, Vec<Step>> = BTreeMap::new();
    for step in script {
        steps
            .entry(positions[step.frame])
            .or_default()
            .push(step.clone());
    }
    let seen = Rc::new(RefCell::new(Vec::new()));
    let host = Rc::new(RefCell::new(Host {
        arms,
        seen: Rc::clone(&seen),
        steps,
        given: BTreeSet::new(),
        last: None,
    }));
    let handler = Replayed {
        host: Rc::clone(&host),
        feed: PeerFeed::new(streams, Consumer(Rc::clone(&seen))),
    };
    let config = MdReplayConfig {
        venue: toy::ToyVenue::leak(),
        cfg: VenueConfig::new(),
        plan: plan("ws://127.0.0.1:1/replayed"),
        specs: toy::specs(),
        conn: CONN,
    };
    let mut md = MdReplay::new(config, handler).unwrap();
    // Every record that carries a recorded instant first lets the answers due by it be read,
    // as the shard would have read them before taking it: a frame that decodes to no event
    // and the epoch's close included.
    for record in JournalReader::open(root, SHARD).unwrap() {
        let record = record.unwrap();
        if let Some(at) = recorded_at(&record) {
            host.borrow_mut().read_until(at);
        }
        md.feed(&record).unwrap();
    }
    md.finish();
    drop(md);
    let mut host = Rc::try_unwrap(host).ok().unwrap().into_inner();
    host.read_until(MonoNs(u64::MAX));
    let counters = host
        .arms
        .iter()
        .map(|a| a.peer.borrow().counters())
        .collect();
    drop(host);
    let _ = std::fs::remove_dir_all(&dir);
    Run {
        seen: Rc::try_unwrap(seen).ok().unwrap().into_inner(),
        counters,
    }
}

// ---------------------------------------------------------------------------------------------
// What a run shows.
// ---------------------------------------------------------------------------------------------

impl Run {
    /// Bracket `i`'s decoded events, each with the instant it was read.
    fn exec(&self, i: usize) -> Vec<(MonoNs, &ExecEvent)> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Exec {
                    bracket, at, ev, ..
                } if *bracket == i => Some((*at, ev)),
                _ => None,
            })
            .collect()
    }

    /// The request bracket `i` sent the script's step `name` as, and when.
    fn sent(&self, i: usize, name: &str) -> Option<(RpcId, MonoNs)> {
        self.seen.iter().find_map(|s| match s {
            Seen::Sent {
                bracket,
                step,
                rpc,
                at,
            } if *bracket == i && *step == name => Some((*rpc, *at)),
            _ => None,
        })
    }

    /// The outcome of request `rpc` on bracket `i`, and when it was read.
    fn outcome(&self, i: usize, rpc: RpcId) -> Option<(MonoNs, SubmitOutcome)> {
        self.exec(i).into_iter().find_map(|(at, ev)| match ev {
            ExecEvent::Outcome {
                rpc: r, outcome, ..
            } if *r == rpc => Some((at, outcome.clone())),
            _ => None,
        })
    }

    /// Bracket `i`'s fills: when each was read, its price and its lots.
    fn fills(&self, i: usize) -> Vec<(MonoNs, i64, i64)> {
        self.exec(i)
            .into_iter()
            .filter_map(|(at, ev)| match ev {
                ExecEvent::Fill(f) => Some((at, f.px.0, f.qty.get())),
                _ => None,
            })
            .collect()
    }

    /// The envelopes the consumer's handler took.
    fn envelopes(&self) -> Vec<Envelope<MdEvent>> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Md(env) => Some(env.clone()),
                _ => None,
            })
            .collect()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_recorded_day_replays_through_simvenue_to_the_same_fills_on_every_run() {
    let root = fresh_dir("journal");
    let live = record(&root).await;
    assert_eq!(live.ends.len(), 2, "two epochs, each ended");
    let positions = positions(&root);
    assert_eq!(positions.len(), 18, "every frame of the day is journaled");

    let first = replay(&root, &positions, &script());
    let second = replay(&root, &positions, &script());

    // The two runs gave the same outcomes, order updates and fills, in the same order, with
    // the same quantities, prices and stamps: everything they produced is equal.
    assert_eq!(first.seen, second.seen);
    assert_eq!(first.counters, second.counters);
    // Every venue took every frame and envelope it was handed.
    assert_eq!(first.counters, vec![PeerCounters::default(); 3]);
    // The market data each run handed the consumer is the live session's, after the venues.
    assert_eq!(first.envelopes(), live.envs);
    let ends: Vec<ConnKey> = first
        .seen
        .iter()
        .filter_map(|s| match s {
            Seen::EpochEnd(key) => Some(*key),
            _ => None,
        })
        .collect();
    assert_eq!(ends, live.ends);

    let at_frame = |n: usize| {
        let env = live
            .envs
            .iter()
            .find(|e| e.stamp.ingest_seq == positions[n]);
        env.unwrap().stamp.recv_mono
    };
    let latency = TO_VENUE + TO_CLIENT;
    for (i, bracket) in BRACKETS.into_iter().enumerate() {
        // The buy given before the market was armed, fbc-oms refused: nothing was written.
        assert!(
            first.seen.contains(&Seen::Refused {
                bracket: i,
                step: "early",
                at: at_frame(0),
            }),
            "{bracket:?}"
        );
        // The bracket's session was authenticated, armed and resynced, each read at the
        // recorded instants plus the configured latency: the greeting due `to_client` after
        // the stream opened at the first envelope, then a full round trip each.
        let opened = at_frame(0);
        let events = first.exec(i);
        assert!(matches!(
            events[0],
            (at, ExecEvent::Conn { state: ConnState::Authenticated, .. }) if at == opened + TO_CLIENT
        ));
        assert!(matches!(
            events[1],
            (at, ExecEvent::Outcome { outcome: SubmitOutcome::Accepted { .. }, .. })
                if at == opened + TO_CLIENT + latency
        ));
        assert!(matches!(
            events[2],
            (at, ExecEvent::ResyncBegin { .. }) if at == opened + TO_CLIENT + latency + latency
        ));

        // Each placement is answered at its frame's recorded stamp plus the latency.
        for (name, accepted) in [
            ("bid", true),
            ("ask", true),
            ("cross", false),
            ("again", true),
        ] {
            let (rpc, sent) = first.sent(i, name).unwrap();
            let (at, outcome) = first.outcome(i, rpc).unwrap();
            assert_eq!(at, sent + latency, "{bracket:?} {name}");
            assert_eq!(
                matches!(outcome, SubmitOutcome::Accepted { .. }),
                accepted,
                "{bracket:?} {name}: {outcome:?}"
            );
        }
        assert_eq!(first.sent(i, "bid").unwrap().1, at_frame(4));
        assert_eq!(first.sent(i, "again").unwrap().1, at_frame(15));
        // The sell's cancel is answered the same way, and the venue ends the sell.
        let (cancel, cancelled_at) = first.sent(i, "cancel").unwrap();
        assert_eq!(cancelled_at, at_frame(10));
        let (at, outcome) = first.outcome(i, cancel).unwrap();
        assert_eq!(at, cancelled_at + latency, "{bracket:?}");
        assert!(
            matches!(outcome, SubmitOutcome::Accepted { .. }),
            "{outcome:?}"
        );
        let sell = first.exec(i).into_iter().find_map(|(at, ev)| match ev {
            ExecEvent::Order(u) if matches!(u.state, VenueOrderState::Canceled(_)) => {
                Some((at, u.side))
            }
            _ => None,
        });
        assert_eq!(
            sell,
            Some((cancelled_at + latency, Side::Sell)),
            "{bracket:?}"
        );

        // Every fill is at the bid, and all three lots placed there filled: two in the first
        // epoch, one in the second, on books the second epoch's snapshot rebuilt.
        let fills = first.fills(i);
        assert!(fills.iter().all(|&(_, px, _)| px == 199), "{fills:?}");
        assert_eq!(
            fills.iter().map(|&(_, _, q)| q).sum::<i64>(),
            3,
            "{bracket:?}"
        );
        assert!(
            fills.iter().any(|&(at, _, _)| at > at_frame(15)),
            "{bracket:?}"
        );
    }
    // The answers due before the first epoch's close were read before it, as the shard reads
    // them: the cancels' among them.
    let closed = first
        .seen
        .iter()
        .position(|s| matches!(s, Seen::EpochEnd(_)))
        .unwrap();
    let cancelled = (0..BRACKETS.len()).map(|i| {
        let (rpc, _) = first.sent(i, "cancel").unwrap();
        first.seen.iter().position(
            |s| matches!(s, Seen::Exec { bracket, ev: ExecEvent::Outcome { rpc: r, .. }, .. } if *bracket == i && *r == rpc),
        )
        .unwrap()
    });
    assert!(cancelled.into_iter().all(|at| at < closed), "{closed}");
    // The brackets fill the bid in their order: Optimistic first, then Middle, then
    // Pessimistic.
    let first_fill = |i: usize| first.fills(i)[0].0;
    let (pess, mid, opt) = (first_fill(0), first_fill(1), first_fill(2));
    assert!(opt < mid && mid < pess, "{opt:?} {mid:?} {pess:?}");
    let _ = std::fs::remove_dir_all(&root);
}

/// The script's positions are the journal's: the same day replayed with the buy at the bid
/// given one frame later is answered at that frame's recorded stamp plus the latency, so its
/// run differs from the first script's, and is again the same on every run.
#[tokio::test(flavor = "current_thread")]
async fn a_step_moved_to_another_journal_position_is_answered_at_that_positions_stamp() {
    let root = fresh_dir("journal");
    let live = record(&root).await;
    let positions = positions(&root);
    let moved: Vec<Step> = script()
        .into_iter()
        .map(|step| match step.name {
            "bid" => Step { frame: 5, ..step },
            _ => step,
        })
        .collect();

    let first = replay(&root, &positions, &moved);
    assert_eq!(first.seen, replay(&root, &positions, &moved).seen);
    assert_ne!(first.seen, replay(&root, &positions, &script()).seen);

    let at_frame = |n: usize| {
        let env = live
            .envs
            .iter()
            .find(|e| e.stamp.ingest_seq == positions[n]);
        env.unwrap().stamp.recv_mono
    };
    for i in 0..BRACKETS.len() {
        let (rpc, sent) = first.sent(i, "bid").unwrap();
        assert_eq!(sent, at_frame(5));
        let (at, outcome) = first.outcome(i, rpc).unwrap();
        assert_eq!(at, at_frame(5) + TO_VENUE + TO_CLIENT);
        assert!(
            matches!(outcome, SubmitOutcome::Accepted { .. }),
            "{outcome:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
