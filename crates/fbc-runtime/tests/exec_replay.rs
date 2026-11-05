//! FBC-tgt's done line (decisions 0006, 0014, 0053, 0056, 0057, 0078, 0081, 0090): decoder
//! replay feeds an order-entry session's journal to a fresh codec of its venue through the same
//! calls the live session made, so a conformance-toy session spanning two epochs, HTTP results
//! and codec timers replays to the same execution events (stamps, venue meta and bodies), on
//! every replay, with the epochs' ends, the unprotected orders told and the decode errors where
//! the live handler heard them. The session runs the toy as its factory builds it, pinging and
//! resyncing over REST; the caller supplies the configuration, credentials, namespace and spec
//! table the session ran with, and the commands the consumer submitted, by request id.
//!
//! A handler that stops its session part-way through an input (FBC-s4bg's case where the stop is
//! the replayed handler's), given the replay's control where it held the session's, is handed
//! on replay exactly what it heard live.
//!
//! The rest are its error paths, on hand-built records: a venue the session refuses, a damaged
//! record, inputs of an epoch that is not open and other sessions' requests, contexts the
//! journal lost or no epoch can take, a command not supplied, an arm the codec refused or the
//! session did not write, a deadline fired while the session waited to reconnect, epochs whose
//! opening or closing the journal lost, gaps, a second planned endpoint, and a control dropped.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use armed_oms::{churn, cid, order_caps, reconnected, settle};
use common::{ScriptedHttp, ScriptedWs};
use exec_toy::{
    EXEC_STREAM, EXEC_URL_KEY, INST_A, OWN_NS, PING_EVERY, REST_URL_KEY, RESYNC_RETRY, RPC_TIMEOUT,
    ToyFactory,
};
use fbc_core::{
    AccountSummary, AckLevel, AssetKey, CancelOnDisconnect, ConfigError, ConnKey, ConnState,
    EncodeCtx, EndpointPlan, Envelope, ExecCodec, ExecEndpoint, ExecEvent, FieldSpec, HttpFailure,
    HttpMethod, HttpPlan, HttpTag, InstrumentSpecDraft, MdCodec, MonoNs, NonceBlock, NonceSource,
    OrderRef, QueryOrder, RawFrame, RpcId, Secrets, SpecTable, Stamp, StreamId, SubmitOutcome,
    Subscription, SymbolError, TimerTag, TrafficClass, VenueCaps, VenueCommand, VenueConfig,
    VenueError, VenueFactory, VenueOrderSnapshot, WallNs, WireSlice, WireUrl, encode_cid,
};
use fbc_journal::{
    ControlEvent, HttpRequestRec, HttpResponseRec, JournalError, JournalReader, JournalSink,
    JournalWriter, Marker, Opaque, Opcode, QueueSink, Record, RecordRef, Recorded, RedactionKey,
    SinkConfig, WriterThread, journal_queue,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecReplay, ExecReplayConfig, ExecReplayCounters,
    ExecSession, ExecSessionConfig, ExecSessionError, IngestClock, Journal, ProxyConfig,
    RateLimiter, ReconnectPacing, ReplayError, RpcIds, SafetyReserve, SessionError, WriteStall,
};
use tokio::time::advance;

/// The connection number the session stamps.
const CONN: u16 = 6;
const SHARD: u16 = 4;
/// The conformance toy as its factory builds it.
static TOY: ToyFactory = ToyFactory;

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

// ---------------------------------------------------------------------------------------------
// What a handler heard, live or replayed.
// ---------------------------------------------------------------------------------------------

/// One thing a handler was told, in order.
#[derive(Clone, Debug, PartialEq)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    Ended(ConnKey),
    Unprotected(Vec<VenueOrderSnapshot>),
    DecodeError(ConnKey),
}

/// A handler keeping what it is told.
#[derive(Clone, Default)]
struct Log(Rc<RefCell<Vec<Heard>>>);

impl Log {
    fn heard(&self) -> Vec<Heard> {
        self.0.borrow().clone()
    }

    fn any(&self, pick: impl Fn(&Heard) -> bool) -> bool {
        self.0.borrow().iter().any(pick)
    }

    /// The bodies of the events heard.
    fn events(&self) -> Vec<ExecEvent> {
        let event = |h: &Heard| match h {
            Heard::Event(env) => Some(env.body.clone()),
            _ => None,
        };
        self.0.borrow().iter().filter_map(event).collect()
    }
}

impl ExecHandler for Log {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().push(Heard::Ended(key));
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.0
            .borrow_mut()
            .push(Heard::Unprotected(orders.to_vec()));
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.0.borrow_mut().push(Heard::DecodeError(key));
    }
}

fn outcome(rpc: RpcId) -> impl Fn(&Heard) -> bool {
    move |h| matches!(h, Heard::Event(env) if matches!(env.body, ExecEvent::Outcome { rpc: r, .. } if r == rpc))
}

// ---------------------------------------------------------------------------------------------
// The live session.
// ---------------------------------------------------------------------------------------------

/// Nonces counted up from 100.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// The toy's configuration: its order-entry URL `ws` and its REST base `rest`.
fn toy_cfg(ws: &str, rest: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, ws);
    cfg.insert(REST_URL_KEY, rest);
    cfg
}

fn session_config(cfg: VenueConfig) -> ExecSessionConfig {
    let limits = TOY.caps(&cfg).unwrap().limits;
    ExecSessionConfig {
        venue: &TOY,
        cfg,
        creds: Secrets::new(),
        acct: armed_oms::ACCT,
        account_wide_cod_authority: None,
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000))
            .unwrap(),
        clock: IngestClock::new(),
        nonces: Box::new(Counting(100)),
        nonce_source: fbc_journal::NonceSourceId(1),
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    }
}

/// The journal queue's sink, counting the HTTP results it is offered, so the test knows when
/// the session has taken one that pushes nothing.
struct Counted {
    queue: QueueSink,
    results: Rc<RefCell<usize>>,
}

impl JournalSink for Counted {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.queue.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        if matches!(record, RecordRef::HttpResult { .. }) {
            *self.results.borrow_mut() += 1;
        }
        self.queue.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.queue.omit(class, now)
    }
}

/// The next request the HTTP server read, the session let run until it has.
async fn requested(http: &mut ScriptedHttp) -> common::Exchange {
    for _ in 0..100_000 {
        if let Some(exchange) = http.try_request() {
            return exchange;
        }
        tokio::task::yield_now().await;
    }
    panic!("nothing requested");
}

/// The resync's watermark in its request line, `GET /resync?ts=<wall>`.
fn watermark(line: &str) -> String {
    line.strip_prefix("GET /resync?ts=").unwrap().to_owned()
}

/// Our order `V-3`, as the toy writes a client id.
fn ours() -> String {
    let wire = encode_cid(&order_caps().client_id, cid()).unwrap();
    format!(
        "rsorder|cid={wire}|vid=V-3|sym=TOYA-PERP|side=B|st=open|px=130865|qty=25|cum=0|po=1|ro=0"
    )
}

/// An order query for venue order `V-1`.
fn query() -> QueryOrder {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    QueryOrder {
        target: OrderRef::Venue(vid),
        inst: INST_A,
        placement_nonce: None,
    }
}

const OK: &str = "HTTP/1.1 200 OK";

/// Runs the toy's session, journaled at `root`, through two epochs: on the first, the venue
/// acknowledges the authentication and accepts the arm, the resync's first request fails (503)
/// and the codec asks again on its retry timer, the second answers it, the consumer's order
/// query is answered and its fee query reaches its deadline unanswered, the ping timer fires,
/// and the connection drops; on the second, the resync over REST shows an order of ours resting
/// (unprotected, 0080), a frame answers no request the codec knows (a decode error), the arm is
/// accepted, and the consumer stops the session. What the handler heard; the request ids of the
/// consumer's query and fee query.
async fn live(root: &Path) -> (Vec<Heard>, RpcId, RpcId) {
    let (queue, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 1 << 20,
            soft_limit_pct: 85,
        },
        redaction_key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(root, SHARD, redaction_key()).unwrap())
        .unwrap();
    let results = Rc::new(RefCell::new(0));
    let sink = Rc::new(RefCell::new(Counted {
        queue,
        results: Rc::clone(&results),
    }));
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let cfg = toy_cfg(&server.url(), &http.url(""));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(session_config(cfg), log.clone()).unwrap();
    session.set_journal(Journal::new(sink));
    let orders = session.orders();
    let watch = log.clone();
    let script = async move {
        // The first epoch.
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send("auth|ok=1|token=toy-session-token|ts=500|seq=1");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let first = requested(&mut http).await;
        let wm = watermark(&first.line);
        peer.send("item|rpc=1|i=0|res=ok|ts=600|seq=2");
        settle(|| watch.any(outcome(RpcId(1)))).await;
        first.answer("HTTP/1.1 503 Service Unavailable", "").await;
        settle(|| *results.borrow() == 1).await;
        // The codec asks again on its retry timer, at a later watermark.
        advance(RESYNC_RETRY).await;
        let second = requested(&mut http).await;
        let wm2 = watermark(&second.line);
        assert_ne!(wm, wm2);
        let body =
            format!("rsbegin|wm={wm2}|ts=700|seq=3\nrspos|sym=TOYA-PERP|qty=0\nrsend|ts=701");
        second.answer(OK, &body).await;
        settle(|| watch.events().contains(&ExecEvent::ResyncEnd)).await;
        // The consumer's order query, answered, and fee query, never answered.
        let queried = orders
            .submit_control(ControlCommand::Query(query()))
            .unwrap();
        let sent = format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", queried.0);
        assert_eq!(peer.recv().await, sent);
        peer.send(&format!("qres|rpc={}|found=0|ts=800|seq=4", queried.0));
        let answered = |h: &Heard| matches!(h, Heard::Event(env) if matches!(env.body, ExecEvent::QueryResult(_)));
        settle(|| watch.any(answered)).await;
        let fees = orders.submit_control(ControlCommand::FeeQuery).unwrap();
        assert_eq!(peer.recv().await, format!("fees|rpc={}", fees.0));
        advance(RPC_TIMEOUT).await;
        settle(|| watch.any(outcome(fees))).await;
        // The ping timer, armed as the epoch opened.
        advance(PING_EVERY).await;
        assert!(peer.recv().await.starts_with("ping|ts="));
        peer.drop_conn();
        settle(|| watch.any(|h| *h == Heard::Ended(key(0)))).await;

        // The second epoch.
        let mut next = reconnected(&mut server).await;
        assert!(next.recv().await.starts_with("auth|ts="));
        next.send("auth|ok=1|token=toy-session-token");
        assert_eq!(next.recv().await, "cod|rpc=4|on=1");
        let third = requested(&mut http).await;
        let wm3 = watermark(&third.line);
        let body = format!(
            "rsbegin|wm={wm3}\n{}\nrspos|sym=TOYA-PERP|qty=25|avg=130865\nrsend",
            ours()
        );
        third.answer(OK, &body).await;
        settle(|| {
            watch
                .events()
                .iter()
                .filter(|e| **e == ExecEvent::ResyncEnd)
                .count()
                == 2
        })
        .await;
        next.send("item|rpc=99|i=0|res=ok");
        next.send("item|rpc=4|i=0|res=ok|ts=900|seq=5");
        settle(|| watch.any(|h| matches!(h, Heard::Unprotected(_)))).await;
        churn().await;
        drop(control);
        while next.next().await.is_some() {}
        (queried, fees)
    };
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let (ran, (queried, fees)) = tokio::join!(session.run(), script);
    drop(thaw);
    ran.unwrap();
    drop(session);
    writer.close().unwrap();
    (log.heard(), queried, fees)
}

/// What a replay of the toy's session needs from the caller, with its configuration `cfg`: what
/// the session ran with.
fn replay_config(cfg: VenueConfig) -> ExecReplayConfig {
    ExecReplayConfig {
        venue: &TOY,
        cfg,
        creds: Secrets::new(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        conn: CONN,
    }
}

/// Replays the journal at `root` into a fresh codec of the toy, the consumer's commands
/// supplied by request id: what the handler heard, and the counts.
fn replay(
    root: &Path,
    cfg: VenueConfig,
    queried: RpcId,
    fees: RpcId,
) -> (Vec<Heard>, ExecReplayCounters) {
    let log = Log::default();
    // The control stands throughout: the live session's was dropped between inputs.
    let (mut replay, _control) = ExecReplay::new(replay_config(cfg), log.clone()).unwrap();
    replay.set_commands(move |rpc| {
        if rpc == queried {
            Some(VenueCommand::Query(query()))
        } else if rpc == fees {
            Some(VenueCommand::FeeQuery)
        } else {
            None
        }
    });
    replay
        .run(JournalReader::open(root, SHARD).unwrap())
        .unwrap();
    let counters = replay.counters();
    drop(replay);
    (log.heard(), counters)
}

/// The done line: the session's events, epoch ends, unprotected orders and decode errors, in
/// order, with their stamps and venue meta, equal on two replays.
#[tokio::test(start_paused = true)]
async fn a_journaled_order_entry_session_replays_to_the_same_execution_events_every_time() {
    let root = fresh_dir("exec_replay_session");
    let (live, queried, fees) = live(&root).await;

    // What the live handler heard, as the script drove it.
    let ended: Vec<&Heard> = live
        .iter()
        .filter(|h| matches!(h, Heard::Ended(_)))
        .collect();
    assert_eq!(ended, [&Heard::Ended(key(0)), &Heard::Ended(key(1))]);
    let events: Vec<&Envelope<ExecEvent>> = live
        .iter()
        .filter_map(|h| match h {
            Heard::Event(env) => Some(&**env),
            _ => None,
        })
        .collect();
    let authenticated = ExecEvent::Conn {
        stream: EXEC_STREAM,
        state: ConnState::Authenticated,
    };
    assert_eq!(events[0].body, authenticated);
    assert_eq!(events[0].venue_seq, Some(1));
    let unknown = |rpc| ExecEvent::Outcome {
        rpc,
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert!(events.iter().any(|e| e.body == unknown(fees)));
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let arms = |e: &&&Envelope<ExecEvent>| matches!(&e.body, ExecEvent::Outcome { outcome, .. } if *outcome == accepted);
    assert_eq!(events.iter().filter(arms).count(), 2);
    assert!(live.contains(&Heard::DecodeError(key(1))));
    let told = live.iter().find_map(|h| match h {
        Heard::Unprotected(orders) => Some(orders.clone()),
        _ => None,
    });
    assert_eq!(told.unwrap().len(), 1);
    // Every event carries its input's stamp, of its epoch, in ingest order.
    for pair in events.windows(2) {
        assert!(pair[0].stamp.ingest_seq <= pair[1].stamp.ingest_seq);
    }
    assert!(events.iter().all(|e| e.stamp.conn.conn == CONN));
    assert!(events.iter().any(|e| e.stamp.conn == key(1)));

    // Two replays hand the handler exactly what the live one heard, stamps and meta included.
    // A replay opens nothing, so any URLs the toy's configuration accepts will do.
    let ws = "ws://127.0.0.1:1/replayed";
    let rest = "http://127.0.0.1:1";
    let (first, counters) = replay(&root, toy_cfg(ws, rest), queried, fees);
    assert_eq!(first, live);
    let (second, again) = replay(&root, toy_cfg(ws, rest), queried, fees);
    assert_eq!(second, live);
    assert_eq!(counters, again);
    assert_eq!(
        counters,
        ExecReplayCounters {
            epochs: 2,
            frames: 6,
            http: 3,
            timers: 2,
            deadlines: 1,
            resyncs: 2,
            encodes: 4,
            decode_errors: 1,
            ..ExecReplayCounters::default()
        }
    );
    fs::remove_dir_all(&root).unwrap();
}

// ---------------------------------------------------------------------------------------------
// A handler that stops its session part-way through an input (FBC-s4bg's replayed case).
// ---------------------------------------------------------------------------------------------

/// A handler that keeps what it is told in `log`, and drops the control it holds as it is
/// handed the first event `at` picks.
struct Stopping {
    log: Log,
    control: Rc<RefCell<Option<ExecControl>>>,
    at: fn(&ExecEvent) -> bool,
}

impl Stopping {
    /// A handler stopping at `at`, logging into `log`, and where its control goes.
    fn new(log: &Log, at: fn(&ExecEvent) -> bool) -> (Stopping, Rc<RefCell<Option<ExecControl>>>) {
        let control = Rc::new(RefCell::new(None));
        let handler = Stopping {
            log: log.clone(),
            control: Rc::clone(&control),
            at,
        };
        (handler, control)
    }
}

impl ExecHandler for Stopping {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let stop = (self.at)(&env.body);
        self.log.on_exec(env);
        if stop {
            self.control.borrow_mut().take();
        }
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.on_epoch_end(key);
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.log.on_unprotected(orders);
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.log.on_decode_error(key);
    }
}

/// A journal writing at `root`, and its writer thread.
fn journal_at(root: &Path) -> (Journal, WriterThread) {
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(config, redaction_key()).unwrap();
    let writer = JournalWriter::create(root, SHARD, redaction_key()).unwrap();
    let writer = drain.spawn(writer).unwrap();
    (Journal::new(Rc::new(RefCell::new(queue))), writer)
}

fn resync_began(ev: &ExecEvent) -> bool {
    matches!(ev, ExecEvent::ResyncBegin { .. })
}

fn resync_ended(ev: &ExecEvent) -> bool {
    *ev == ExecEvent::ResyncEnd
}

fn never(_: &ExecEvent) -> bool {
    false
}

fn told_unprotected(heard: &[Heard]) -> bool {
    heard.iter().any(|h| matches!(h, Heard::Unprotected(_)))
}

/// Runs the toy's session, journaled at `root`, through one epoch whose handler drops the
/// session's control as it is handed the first event `at` picks: the venue acknowledges the
/// authentication and accepts the arm, and the resync over REST shows an order of ours resting
/// (unprotected, 0080) in one result, which pushes four events. What the handler heard.
async fn stopping_live(root: &Path, at: fn(&ExecEvent) -> bool) -> Vec<Heard> {
    let (journal, writer) = journal_at(root);
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let cfg = toy_cfg(&server.url(), &http.url(""));
    let log = Log::default();
    let (handler, control) = Stopping::new(&log, at);
    let (mut session, own) = ExecSession::new(session_config(cfg), handler).unwrap();
    *control.borrow_mut() = Some(own);
    session.set_journal(journal);
    let watch = log.clone();
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let resync = requested(&mut http).await;
        let wm = watermark(&resync.line);
        peer.send("item|rpc=1|i=0|res=ok");
        settle(|| watch.any(outcome(RpcId(1)))).await;
        let body = format!(
            "rsbegin|wm={wm}\n{}\nrspos|sym=TOYA-PERP|qty=25|avg=130865\nrsend",
            ours()
        );
        resync.answer(OK, &body).await;
        // The handler stops the session, which closes the connection.
        while peer.next().await.is_some() {}
    };
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let (ran, ()) = tokio::join!(session.run(), script);
    drop(thaw);
    ran.unwrap();
    drop(session);
    writer.close().unwrap();
    log.heard()
}

/// Replays the journal at `root` into a handler that drops the replay's control, handed it where
/// the live handler held the session's, as it is handed the first event `at` picks: what it
/// heard, and the counts.
fn stopping_replay(root: &Path, at: fn(&ExecEvent) -> bool) -> (Vec<Heard>, ExecReplayCounters) {
    let log = Log::default();
    let (handler, control) = Stopping::new(&log, at);
    let cfg = toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1");
    let (mut replay, own) = ExecReplay::new(replay_config(cfg), handler).unwrap();
    *control.borrow_mut() = Some(own);
    replay
        .run(JournalReader::open(root, SHARD).unwrap())
        .unwrap();
    let counters = replay.counters();
    drop(replay);
    (log.heard(), counters)
}

/// A handler that drops its session's control as it is handed the first of the four events one
/// HTTP result pushes is handed none of the other three live (0053). A replay whose handler
/// drops the replay's control there hands it exactly what the live one heard, the three dropped
/// and counted, on every replay; a replay whose control stands hands on all four and the orders
/// told unprotected, the case FBC-s4bg keeps for a stop the replayed handler does not make.
#[tokio::test(start_paused = true)]
async fn a_handler_that_stops_part_way_through_an_input_is_replayed_to_what_it_heard_live() {
    let root = fresh_dir("exec_replay_stopping");
    let live = stopping_live(&root, resync_began).await;
    let bodies = |heard: &[Heard]| -> Vec<ExecEvent> {
        let event = |h: &Heard| match h {
            Heard::Event(env) => Some(env.body.clone()),
            _ => None,
        };
        heard.iter().filter_map(event).collect()
    };
    let heard = bodies(&live);
    assert_eq!(heard.len(), 3, "{heard:?}");
    assert!(resync_began(&heard[2]));
    assert_eq!(live.last(), Some(&Heard::Ended(key(0))));
    assert!(!told_unprotected(&live));

    let (first, counters) = stopping_replay(&root, resync_began);
    assert_eq!(first, live);
    assert_eq!(counters.stopped, 3);
    let (second, again) = stopping_replay(&root, resync_began);
    assert_eq!(second, live);
    assert_eq!(again, counters);

    let (standing, counters) = stopping_replay(&root, never);
    assert_eq!(counters.stopped, 0);
    assert_eq!(bodies(&standing).len(), 6);
    assert!(told_unprotected(&standing));
    fs::remove_dir_all(&root).unwrap();
}

/// A handler that drops its session's control as it is handed the event that lets its epoch
/// place, the resync's end, is not told the orders of ours resting unprotected, live (0053,
/// 0080) or replayed with the replay's control dropped there.
#[tokio::test(start_paused = true)]
async fn a_handler_that_stops_as_its_epoch_places_is_told_no_unprotected_orders() {
    let root = fresh_dir("exec_replay_stopping_placing");
    let live = stopping_live(&root, resync_ended).await;
    let ended = |h: &Heard| matches!(h, Heard::Event(env) if env.body == ExecEvent::ResyncEnd);
    assert!(live.iter().any(ended));
    assert!(!told_unprotected(&live));
    assert_eq!(live.last(), Some(&Heard::Ended(key(0))));

    let (replayed, counters) = stopping_replay(&root, resync_ended);
    assert_eq!(replayed, live);
    assert_eq!(counters.stopped, 0);
    let (standing, _) = stopping_replay(&root, never);
    assert!(told_unprotected(&standing));
    fs::remove_dir_all(&root).unwrap();
}

// ---------------------------------------------------------------------------------------------
// Error paths, on hand-built records.
// ---------------------------------------------------------------------------------------------

fn stamp(ingest_seq: u64, key: ConnKey) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq * 10),
        recv_wall: WallNs(1_000 + ingest_seq as i64),
        conn: key,
    }
}

fn opened(key: ConnKey) -> Record {
    Record::Control {
        at: MonoNs(1),
        ev: ControlEvent::Opened(key),
    }
}

fn closed(key: ConnKey) -> Record {
    Record::Control {
        at: MonoNs(2),
        ev: ControlEvent::Closed(key),
    }
}

/// A context given at wall time `wall` with `nonces`, an encode's when `rpc` names its request.
fn ctx(rpc: Option<u64>, wall: i64, nonces: &[u64]) -> Record {
    Record::EncodeCtx {
        rpc: rpc.map(RpcId),
        ctx: EncodeCtx {
            wall: WallNs(wall),
            mono: MonoNs(5),
            nonces: NonceBlock::new(nonces.to_vec()),
        },
    }
}

fn frame(seq: u64, key: ConnKey, text: &str) -> Record {
    Record::inbound(stamp(seq, key), RawFrame::Text(text))
}

fn timer(seq: u64, key: ConnKey, tag: u64) -> Record {
    Record::Timer {
        stamp: stamp(seq, key),
        tag: TimerTag(tag),
    }
}

fn result(seq: u64, key: ConnKey, tag: u64, body: &str) -> Record {
    Record::HttpResult {
        stamp: stamp(seq, key),
        tag: HttpTag(tag),
        result: Ok(HttpResponseRec {
            status: 200,
            headers: Vec::new(),
            body: Opaque(body.as_bytes().to_vec()),
            body_redact: Vec::new(),
        }),
    }
}

const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";

/// A replay of the toy, resyncing over REST, into `log`, and its control.
fn toy_replay(log: &Log) -> (ExecReplay<Log>, ExecControl) {
    let cfg = toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1");
    ExecReplay::new(replay_config(cfg), log.clone()).unwrap()
}

/// The toy's arm, request `rpc`, written on epoch `key`: what the session journals as it
/// writes the arm, straight after its context.
fn arm_written(key: ConnKey, rpc: u64) -> Record {
    Record::Outbound {
        at: MonoNs(4),
        conn: key,
        rpc: Some(RpcId(rpc)),
        opcode: Opcode::Text,
        frame: WireSlice::plain(format!("cod|rpc={rpc}|on=1").into_bytes()),
    }
}

/// The first epoch of the toy's order connection opened, authenticated, armed (request 1,
/// written) and resynced over REST (tag 1).
fn armed_and_resyncing() -> Vec<Record> {
    vec![
        opened(key(0)),
        ctx(None, 10, &[]),
        frame(1, key(0), AUTH_ACK),
        ctx(Some(1), 11, &[100]),
        arm_written(key(0), 1),
        ctx(None, 12, &[]),
    ]
}

/// The bodies of the events `log` heard, with each event's stamp's ingest sequence and epoch.
fn heard_at(log: &Log) -> Vec<(u64, u32, ExecEvent)> {
    let at = |h: &Heard| match h {
        Heard::Event(env) => Some((env.stamp.ingest_seq, env.stamp.conn.epoch, env.body.clone())),
        _ => None,
    };
    log.0.borrow().iter().filter_map(at).collect()
}

/// A venue the session refuses, one way or another, else the toy.
#[derive(Copy, Clone)]
enum Odd {
    /// Its caps cannot be read.
    NoCaps,
    /// It declares no order entry.
    NoExec,
    /// Its cancel-on-disconnect is a dead-man timer.
    DeadMan,
    /// It cannot plan its order entry.
    NoPlan,
    /// It plans no order-entry connection.
    NoEndpoint,
    /// It builds no order-entry codec.
    NoCodec,
    /// Its codec cannot be built.
    BadCodec,
    /// It plans a second order-entry connection, on stream 2.
    TwoEndpoints,
    /// Its codec refuses to encode the cancel-on-disconnect arm.
    RefusesArm,
}

struct OddVenue(Odd);

impl OddVenue {
    fn leak(odd: Odd) -> &'static OddVenue {
        Box::leak(Box::new(OddVenue(odd)))
    }
}

impl VenueFactory for OddVenue {
    fn id(&self) -> &'static str {
        "TOY-ODD"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = TOY.caps(cfg)?;
        match self.0 {
            Odd::NoCaps => return Err(ConfigError::Missing("odd.caps")),
            Odd::NoExec => caps.exec = None,
            Odd::DeadMan => {
                let exec = caps.exec.as_mut().unwrap();
                exec.order.cancel_on_disconnect = CancelOnDisconnect::None;
            }
            _ => {}
        }
        Ok(caps)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        TOY.parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        TOY.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        TOY.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        TOY.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let mut plan = TOY.plan_exec(cfg)?;
        match self.0 {
            Odd::NoPlan => return Err(VenueError::NoDiscovery),
            Odd::NoEndpoint => plan.clear(),
            Odd::TwoEndpoints => {
                let second = ExecEndpoint {
                    stream: StreamId(2),
                    ..plan[0].clone()
                };
                plan.push(second);
            }
            _ => {}
        }
        Ok(plan)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        match self.0 {
            Odd::NoCodec => None,
            Odd::BadCodec => Some(Err(VenueError::NoDiscovery)),
            Odd::RefusesArm => {
                let toy = TOY.exec_codec(cfg, creds)?;
                Some(toy.map(|toy| Box::new(NoArm(toy)) as Box<dyn ExecCodec>))
            }
            _ => TOY.exec_codec(cfg, creds),
        }
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        TOY.test_connection(cfg, creds)
    }
}

/// The toy's codec, refusing to encode the cancel-on-disconnect arm.
struct NoArm(Box<dyn ExecCodec>);

impl ExecCodec for NoArm {
    fn nonces_for(&self, call: fbc_core::CtxCall) -> u16 {
        self.0.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut fbc_core::Effects) {
        self.0.on_open(stream, ctx, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut fbc_core::PathStamps<'_>,
        fx: &mut fbc_core::Effects,
    ) -> Result<fbc_core::EncodeReceipt, fbc_core::NotSentReason> {
        if matches!(cmd, VenueCommand::ArmCancelOnDisconnect(_)) {
            return Err(fbc_core::NotSentReason::Unsupported);
        }
        self.0.encode(cmd, rpc, specs, ctx, t, fx)
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &fbc_core::DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn fbc_core::ExecSink,
        fx: &mut fbc_core::Effects,
    ) -> Result<(), fbc_core::DecodeError> {
        self.0.on_frame(stream, f, scope, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<fbc_core::HttpResponse<'_>, HttpFailure>,
        scope: &fbc_core::DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn fbc_core::ExecSink,
        fx: &mut fbc_core::Effects,
    ) -> Result<(), fbc_core::DecodeError> {
        self.0.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut fbc_core::Effects) {
        self.0.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn fbc_core::ExecSink) {
        self.0.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut fbc_core::Effects) {
        self.0.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: fbc_core::Inbound<'_>) -> fbc_core::InboundSpans {
        self.0.redact_inbound(input)
    }
}

fn odd_replay(odd: Odd) -> Result<(ExecReplay<Log>, ExecControl), ExecSessionError> {
    let config = ExecReplayConfig {
        venue: OddVenue::leak(odd),
        ..replay_config(toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1"))
    };
    ExecReplay::new(config, Log::default())
}

/// A venue whose order entry the session would refuse is refused with the session's own error.
#[test]
fn a_venue_the_session_refuses_is_refused_with_the_sessions_error() {
    let refused = |odd| odd_replay(odd).err().unwrap();
    assert_eq!(
        refused(Odd::NoCaps),
        ExecSessionError::Session(SessionError::Config(ConfigError::Missing("odd.caps")))
    );
    assert_eq!(refused(Odd::NoExec), ExecSessionError::NoOrderEntry);
    assert_eq!(
        refused(Odd::DeadMan),
        ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::None)
    );
    assert_eq!(
        refused(Odd::NoPlan),
        ExecSessionError::Venue(VenueError::NoDiscovery)
    );
    assert_eq!(refused(Odd::NoEndpoint), ExecSessionError::Endpoints(0));
    assert_eq!(refused(Odd::NoCodec), ExecSessionError::NoOrderEntry);
    assert_eq!(
        refused(Odd::BadCodec),
        ExecSessionError::Venue(VenueError::NoDiscovery)
    );
}

/// A record the journal cannot read stops the replay with the reader's error; a text frame that
/// is not UTF-8, which no session journals, with its ingest sequence.
#[test]
fn a_damaged_journal_stops_the_replay() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let records = vec![Ok(opened(key(0))), Err(JournalError::TooLarge)];
    let err = replay.run(records).unwrap_err();
    assert!(matches!(err, ReplayError::Journal(JournalError::TooLarge)));

    let damaged = Record::Inbound {
        stamp: stamp(9, key(0)),
        opcode: Opcode::Text,
        bytes: Opaque(vec![0xff, 0xfe]),
        redact: Vec::new(),
    };
    let err = replay.feed(&damaged).unwrap_err();
    assert!(matches!(err, ReplayError::NotUtf8 { ingest_seq: 9 }));
    // A binary frame is the codec's to read: the toy's are text, so it cannot decode one.
    let binary = Record::inbound(stamp(10, key(0)), RawFrame::Binary(&[1, 2]));
    replay.feed(&binary).unwrap();
    assert_eq!(replay.counters().decode_errors, 1);
    assert_eq!(log.heard(), [Heard::DecodeError(key(0))]);
}

/// The toy's session journals each epoch's opening, `on_open`'s context, the authentication's
/// acknowledgement, the arm's context and the resync's: replayed, the handler hears the stream
/// authenticated, the codec encoded the arm (whose answer it then reads) and asked the resync
/// (whose result it then reads), all with the recorded stamps.
#[test]
fn the_sessions_own_arm_and_resync_are_replayed_where_the_journal_places_them() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let mut records = armed_and_resyncing();
    records.extend([
        frame(2, key(0), "item|rpc=1|i=0|res=ok"),
        result(3, key(0), 1, "rsbegin|wm=12\nrsend"),
        closed(key(0)),
    ]);
    replay.run(records.into_iter().map(Ok)).unwrap();
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    assert_eq!(
        heard_at(&log),
        [
            (
                1,
                0,
                ExecEvent::Conn {
                    stream: EXEC_STREAM,
                    state: ConnState::Authenticated,
                }
            ),
            (
                2,
                0,
                ExecEvent::Outcome {
                    rpc: RpcId(1),
                    item: Some(fbc_core::ItemRef {
                        idx: 0,
                        cid: None,
                        vid: None,
                    }),
                    outcome: accepted,
                }
            ),
            (
                3,
                0,
                ExecEvent::ResyncBegin {
                    watermark: WallNs(12)
                }
            ),
            (3, 0, ExecEvent::ResyncEnd),
        ]
    );
    assert_eq!(log.heard().last(), Some(&Heard::Ended(key(0))));
    let counters = replay.counters();
    assert_eq!((counters.encodes, counters.resyncs), (1, 1));
}

/// An input of an epoch that is not open reaches no codec and is counted stale: a frame, an
/// HTTP result and a timer firing of an ended epoch, and an input the live session took as its
/// control dropped, journaled after the epoch's close. The handler is told of a close whose
/// opening the journal lost, as live; records of other connections and outputs are skipped.
#[test]
fn inputs_of_an_epoch_not_open_reach_no_codec() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let other = ConnKey { conn: 9, epoch: 0 };
    let records = vec![
        opened(key(0)),
        ctx(None, 10, &[]),
        closed(key(0)),
        frame(1, key(0), AUTH_ACK),
        result(2, key(0), 1, "rsend"),
        timer(3, key(0), 0),
        closed(key(3)),
        frame(4, other, AUTH_ACK),
        result(4, other, 1, "rsend"),
        timer(4, other, 0),
        closed(other),
        Record::Control {
            at: MonoNs(3),
            ev: ControlEvent::Subscribe {
                conn: key(0),
                add: Vec::new(),
                remove: Vec::new(),
            },
        },
        Record::InboundControl {
            stamp: stamp(5, key(0)),
            frame: fbc_journal::WsControl::Close(None),
        },
        Record::Nonce {
            source: fbc_journal::NonceSourceId(1),
            value: 3,
        },
        // Requests named on another connection are another order-entry session's: counted.
        // A frame naming none, as market data writes, and a request of this session's are not.
        Record::Outbound {
            at: MonoNs(6),
            conn: other,
            rpc: Some(RpcId(5)),
            opcode: Opcode::Text,
            frame: WireSlice::plain(b"cod|rpc=5|on=1".to_vec()),
        },
        Record::HttpRequest {
            at: MonoNs(7),
            conn: other,
            tag: HttpTag(1),
            rpc: Some(RpcId(6)),
            req: HttpRequestRec {
                method: HttpMethod::Get,
                url: WireUrl::plain("http://127.0.0.1:1/fees"),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
        },
        Record::Outbound {
            at: MonoNs(8),
            conn: other,
            rpc: None,
            opcode: Opcode::Text,
            frame: WireSlice::plain(b"sub".to_vec()),
        },
        arm_written(key(0), 7),
    ];
    replay.run(records.into_iter().map(Ok)).unwrap();
    assert_eq!(log.heard(), [Heard::Ended(key(0)), Heard::Ended(key(3))]);
    let counters = replay.counters();
    assert_eq!(
        (counters.stale, counters.frames, counters.epochs),
        (3, 0, 1)
    );
    assert_eq!(counters.foreign_requests, 2);
}

/// A call the live session made whose context the journal lost (an `on_open` or an `on_timer`
/// under Normal in a `Degraded` span) is not made: counted, and the next context goes where the
/// journal places it. A call that waited for its context when its epoch closed was never made
/// live. A context of a resync or an encode while the order connection has no open epoch is
/// counted unplaced.
#[test]
fn a_call_whose_context_the_journal_lost_is_not_made() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let records = vec![
        // The opening's context lost: the acknowledgement finds no authentication asked.
        opened(key(0)),
        Record::Marker(Marker::Degraded {
            from_seq: 1,
            dropped: 1,
        }),
        frame(1, key(0), AUTH_ACK),
        // A timer firing's context lost before the next input.
        timer(2, key(0), 0),
        frame(3, key(0), "item|rpc=8|i=0|res=ok"),
        // One lost before an encode's context, which is encoded all the same: no command is
        // supplied, so it is counted.
        timer(4, key(0), 0),
        ctx(Some(8), 13, &[101]),
        // A timer whose epoch closed before its context: never called live.
        timer(5, key(0), 0),
        closed(key(0)),
        // No epoch open: a resync's and an encode's contexts have nowhere to go.
        ctx(None, 14, &[]),
        ctx(Some(9), 15, &[102]),
    ];
    replay.run(records.into_iter().map(Ok)).unwrap();
    let counters = replay.counters();
    assert_eq!(
        counters,
        ExecReplayCounters {
            epochs: 1,
            frames: 2,
            unsupplied: 1,
            lost_contexts: 3,
            unplaced: 2,
            decode_errors: 2,
            gaps: 1,
            ..ExecReplayCounters::default()
        }
    );
    assert_eq!(
        log.heard(),
        [
            Heard::DecodeError(key(0)),
            Heard::DecodeError(key(0)),
            Heard::Ended(key(0))
        ]
    );
}

/// A consumer's command the caller does not supply is not encoded and counted, so the codec
/// cannot read its answer; one it supplies is encoded with the journaled context, and its answer
/// reads as live. A deadline fired on the open epoch is the connected session's.
#[test]
fn a_consumers_command_is_encoded_only_as_the_caller_supplies_it() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    replay.set_commands(|rpc| (rpc == RpcId(2)).then(|| VenueCommand::Query(query())));
    let mut records = armed_and_resyncing();
    records.extend([
        frame(2, key(0), "item|rpc=1|i=0|res=ok"),
        ctx(Some(2), 20, &[101]),
        ctx(Some(3), 21, &[102]),
        frame(3, key(0), "qres|rpc=2|found=0"),
        frame(4, key(0), "item|rpc=3|i=0|res=ok"),
        Record::RpcTimeout {
            stamp: stamp(5, key(0)),
            rpc: RpcId(3),
        },
    ]);
    replay.run(records.into_iter().map(Ok)).unwrap();
    let counters = replay.counters();
    assert_eq!((counters.encodes, counters.unsupplied), (2, 1));
    assert_eq!(counters.decode_errors, 1);
    assert_eq!(counters.deadlines, 1);
    let events: Vec<ExecEvent> = heard_at(&log).into_iter().map(|(.., ev)| ev).collect();
    assert!(matches!(events[2], ExecEvent::QueryResult(_)), "{events:?}");
    // The request never encoded: the codec reports it Unknown at its deadline, knowing nothing
    // of it.
    assert_eq!(
        events[3],
        ExecEvent::Outcome {
            rpc: RpcId(3),
            item: None,
            outcome: SubmitOutcome::Unknown,
        }
    );
}

/// A deadline fired while the session waited to reconnect, stamped under the epoch it waited
/// to open, reaches the codec and its events the handler with that stamp, as the waiting session
/// hands them on: the arm the dropped epoch sent, unanswered, is `Unknown`.
#[test]
fn a_deadline_fired_while_waiting_to_reconnect_is_handed_on_under_the_epoch_to_open() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let mut records = armed_and_resyncing();
    records.extend([
        closed(key(0)),
        Record::RpcTimeout {
            stamp: stamp(7, key(1)),
            rpc: RpcId(1),
        },
        // Another connection's deadline is not this session's.
        Record::RpcTimeout {
            stamp: stamp(8, ConnKey { conn: 9, epoch: 0 }),
            rpc: RpcId(1),
        },
    ]);
    replay.run(records.into_iter().map(Ok)).unwrap();
    let heard = heard_at(&log);
    let unknown = ExecEvent::Outcome {
        rpc: RpcId(1),
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(heard.last(), Some(&(7, 1, unknown)));
    assert_eq!(replay.counters().deadlines, 1);
}

/// An epoch opened while its endpoint's epoch is still open (its close was dropped) ends that
/// one first, the handler told, and authenticates anew: its arm is due again and replayed.
#[test]
fn an_opening_with_an_epoch_still_open_ends_that_one_first() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let mut records = armed_and_resyncing();
    records.extend([
        opened(key(1)),
        ctx(None, 30, &[]),
        frame(2, key(1), AUTH_ACK),
        ctx(Some(4), 31, &[103]),
        arm_written(key(1), 4),
        frame(3, key(1), "item|rpc=4|i=0|res=ok"),
    ]);
    replay.run(records.into_iter().map(Ok)).unwrap();
    assert!(log.heard().contains(&Heard::Ended(key(0))));
    let counters = replay.counters();
    assert_eq!((counters.epochs, counters.encodes), (2, 2));
    assert_eq!(counters.decode_errors, 0);
    assert!(log.any(outcome(RpcId(4))));
}

/// A second planned endpoint is connection `conn + 1`: its epoch's `on_open` and frames go to
/// the codec with its own stream, and its authentication readies nothing on the order
/// connection, so an encode there is no arm.
#[test]
fn a_second_endpoints_records_reach_the_codec_on_its_stream() {
    let leg = |epoch| ConnKey {
        conn: CONN + 1,
        epoch,
    };
    let log = Log::default();
    let config = ExecReplayConfig {
        venue: OddVenue::leak(Odd::TwoEndpoints),
        ..replay_config(toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1"))
    };
    let (mut replay, _control) = ExecReplay::new(config, log.clone()).unwrap();
    let records = vec![
        opened(key(0)),
        ctx(None, 10, &[]),
        opened(leg(0)),
        ctx(None, 11, &[]),
        frame(1, leg(0), AUTH_ACK),
        ctx(Some(1), 12, &[100]),
        closed(leg(0)),
        // Beyond the plan: not this session's.
        opened(ConnKey {
            conn: CONN + 2,
            epoch: 0,
        }),
    ];
    replay.run(records.into_iter().map(Ok)).unwrap();
    assert_eq!(
        heard_at(&log),
        [(
            1,
            0,
            ExecEvent::Conn {
                stream: StreamId(2),
                state: ConnState::Authenticated,
            }
        )]
    );
    assert_eq!(log.heard().last(), Some(&Heard::Ended(leg(0))));
    let counters = replay.counters();
    assert_eq!((counters.epochs, counters.unsupplied), (2, 1));
}

/// An HTTP result that came back as a failure reaches the codec as live; one the codec cannot
/// decode is counted and the handler told.
#[test]
fn an_http_failure_and_an_undecodable_result_reach_the_codec() {
    let log = Log::default();
    let (mut replay, _control) = toy_replay(&log);
    let mut records = armed_and_resyncing();
    records.extend([
        Record::HttpResult {
            stamp: stamp(2, key(0)),
            tag: HttpTag(1),
            result: Err(HttpFailure::TimedOut),
        },
        // The retry timer the failure armed, carrying its request's tag, asks again (tag 2).
        timer(3, key(0), 1),
        ctx(None, 40, &[]),
        result(4, key(0), 2, "rsbegin|wm=40\nnot a record"),
    ]);
    replay.run(records.into_iter().map(Ok)).unwrap();
    let counters = replay.counters();
    assert_eq!(
        (counters.http, counters.timers, counters.decode_errors),
        (2, 1, 1)
    );
    assert_eq!(log.heard().last(), Some(&Heard::DecodeError(key(0))));
}

/// An arm the codec refused to encode fails its epoch's arm, as the live session's gate has it:
/// the next encode context on that epoch is no arm, and goes to the caller's commands.
#[test]
fn an_arm_the_codec_refused_is_not_taken_for_the_next_encode() {
    let log = Log::default();
    let config = ExecReplayConfig {
        venue: OddVenue::leak(Odd::RefusesArm),
        ..replay_config(toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1"))
    };
    let (mut replay, _control) = ExecReplay::new(config, log.clone()).unwrap();
    let records = vec![
        opened(key(0)),
        ctx(None, 10, &[]),
        frame(1, key(0), AUTH_ACK),
        ctx(Some(1), 11, &[100]),
        ctx(Some(2), 12, &[101]),
    ];
    replay.run(records.into_iter().map(Ok)).unwrap();
    let counters = replay.counters();
    assert_eq!((counters.encodes, counters.unsupplied), (1, 1));
}

/// The session's own arm counts as sent, so that its answer settles the epoch's arm, only where
/// the record after its context is its frame written on that epoch: one the live session did not
/// write (its checks or buckets refused it) failed the epoch's arm, whose answer then lets the
/// epoch place no more. And an input another endpoint brings counts for the order connection's
/// epoch until that epoch's `Closed`: here the arm's answer comes on a second endpoint, and lets
/// the epoch place, so the order of ours its resync showed resting is told, unless the order
/// connection's `Closed` came first.
#[test]
fn an_arm_counts_as_sent_only_where_its_frame_follows_its_context() {
    let leg = ConnKey {
        conn: CONN + 1,
        epoch: 0,
    };
    let run = |written: bool, closed_first: bool| {
        let log = Log::default();
        let config = ExecReplayConfig {
            venue: OddVenue::leak(Odd::TwoEndpoints),
            ..replay_config(toy_cfg("ws://127.0.0.1:1/replayed", "http://127.0.0.1:1"))
        };
        let (mut replay, _control) = ExecReplay::new(config, log.clone()).unwrap();
        let mut records = vec![
            opened(key(0)),
            ctx(None, 10, &[]),
            frame(1, key(0), AUTH_ACK),
            ctx(Some(1), 11, &[100]),
        ];
        if written {
            records.push(arm_written(key(0), 1));
        }
        let body = format!("rsbegin|wm=12\n{}\nrsend", ours());
        records.extend([
            ctx(None, 12, &[]),
            result(2, key(0), 1, &body),
            opened(leg),
            ctx(None, 13, &[]),
        ]);
        if closed_first {
            records.push(closed(key(0)));
        }
        records.extend([frame(3, leg, "item|rpc=1|i=0|res=ok"), closed(leg)]);
        replay.run(records.into_iter().map(Ok)).unwrap();
        assert!(log.any(outcome(RpcId(1))), "{:?}", log.heard());
        log.heard()
    };
    assert!(told_unprotected(&run(true, false)));
    assert!(!told_unprotected(&run(false, false)));
    assert!(!told_unprotected(&run(true, true)));
}

/// Once the replay's control has dropped, every event the codec pushes is dropped and counted,
/// on the open epoch and while the session waits to reconnect, as live; the codec is still fed
/// what the journal holds, and the handler still told each frame it cannot decode and each
/// epoch's end.
#[test]
fn once_the_replays_control_drops_its_handler_is_handed_no_event() {
    let log = Log::default();
    let (mut replay, control) = toy_replay(&log);
    let mut records = armed_and_resyncing();
    records.push(frame(2, key(0), "item|rpc=1|i=0|res=ok"));
    replay.run(records.into_iter().map(Ok)).unwrap();
    let before = log.heard().len();
    assert_eq!(before, 2);
    drop(control);
    let records = vec![
        result(3, key(0), 1, "rsbegin|wm=12\nrsend"),
        frame(4, key(0), "not a record"),
        closed(key(0)),
        Record::RpcTimeout {
            stamp: stamp(5, key(1)),
            rpc: RpcId(9),
        },
    ];
    replay.run(records.into_iter().map(Ok)).unwrap();
    assert_eq!(
        log.heard()[before..],
        [Heard::DecodeError(key(0)), Heard::Ended(key(0))]
    );
    let counters = replay.counters();
    // The resync's begin and end, and the deadline's Unknown.
    assert_eq!(counters.stopped, 3);
    assert_eq!(
        (counters.http, counters.frames, counters.deadlines),
        (1, 3, 1)
    );
}
