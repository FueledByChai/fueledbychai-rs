//! FBC-qwq's done line (decisions 0006, 0014, 0024, 0028, 0138, 0142): exact replay. A
//! conformance-toy order-entry session journaled live, replayed twice through a fresh codec of
//! the toy with the encode contexts and nonces the journal holds and the consumer's commands
//! supplied by request id (`ExecReplay::set_commands`, 0138 item 1), reproduces every outbound
//! frame and HTTP request the journal holds, byte for byte outside their redaction spans,
//! signatures included (`ExecReplay::exact`).
//!
//! The session runs the toy as its factory builds it: it authenticates with the toy's token in a
//! redaction span (0028), arms cancel-on-disconnect and resyncs over REST under a base whose
//! credential is a span of its URL, then sends the consumer's place, amend and cancel, which the
//! toy signs with each context's nonce and time, an order query, a fee query that reaches its
//! deadline, and a ping on its timer; the connection drops, and the second epoch authenticates,
//! arms and resyncs again before the consumer stops. Every order command is one fbc-oms built and
//! authorized (`armed_oms`), from client ids of a mint of the test's own, so the same commands are
//! built again on every run.
//!
//! The golden is that session's journal as recorded once, committed with the configuration and
//! request ids the replay needs and a rendering of every frame and request it holds
//! (`fixtures/conformance-toy/exact_replay/`, whose README says how it was made). The committed
//! journal replays exactly, and the session recorded afresh on every run does too; the fresh one
//! replaces the committed one only when `FBC_REFRESH_GOLDENS=1`, which `scripts/check.sh` sets
//! for this test alone when it is given `--refresh-goldens` (`scripts/golden-refresh.sh`, whose
//! self-test the check runs). A command or a context other than the journal's is a difference,
//! and so is a signature alone: each signature is kept outside every span, and compared.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use armed_oms::{Oms, churn, reconnected, settle};
use common::{ScriptedHttp, ScriptedWs};
use exec_toy::{
    EXEC_URL_KEY, INST_A, OWN_NS, PING_EVERY, REST_URL_KEY, REST_URL_REDACT_KEY, RPC_TIMEOUT,
    ToyFactory,
};
use fbc_core::{
    CidMint, ConnKey, Envelope, ExecEvent, Lots, NamespaceLease, NonceBlock, NonceSource, OrderRef,
    QueryOrder, RpcId, Secrets, VenueCommand, VenueConfig, VenueFactory, VenueOrderSnapshot,
    WallNs, WireSlice,
};
use fbc_journal::{
    BLANK, JournalReader, JournalWriter, Record, RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExactReport, ExecHandler, ExecReplay, ExecReplayConfig, ExecReplayCounters,
    ExecSession, ExecSessionConfig, IngestClock, Journal, ProxyConfig, RateLimiter,
    ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use tokio::time::advance;

/// The connection number the session stamps.
const CONN: u16 = 6;
const SHARD: u16 = 4;
/// The conformance toy as its factory builds it.
static TOY: ToyFactory = ToyFactory;
/// The variable that has the fresh recording replace the committed golden.
const REFRESH: &str = "FBC_REFRESH_GOLDENS";
/// The REST base's path, whose last part is its credential, a span of the URL.
const REST_PATH: &str = "/acct/toy-rest-credential";
const CREDENTIAL: &str = "toy-rest-credential";
const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";
const OK: &str = "HTTP/1.1 200 OK";
/// A golden's journal, configuration and rendering, within its directory.
const JOURNAL: &str = "journal";
const SESSION: &str = "session.txt";
const OUTBOUND: &str = "outbound.txt";
/// The frames and requests the session writes and asks for: nine on its first epoch, three on
/// its second.
const OUTBOUND_RECORDS: u64 = 12;

/// Serializes the tests that read or write the committed golden.
static GOLDEN: Mutex<()> = Mutex::new(());

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
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// The committed golden.
fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/conformance-toy/exact_replay/golden")
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

    /// How many resyncs ended.
    fn resyncs(&self) -> usize {
        let ended = |h: &&Heard| matches!(h, Heard::Event(env) if env.body == ExecEvent::ResyncEnd);
        self.0.borrow().iter().filter(ended).count()
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
// The commands, built the same on every run.
// ---------------------------------------------------------------------------------------------

/// A registry armed on the toy's market whose client ids come from a mint of its own, under a
/// lease in a directory of its own: its first id is 1, so the orders it builds are the same on
/// every run.
fn golden_oms() -> Oms {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = fresh_dir(&format!("exact_replay_cids_{n}"));
    let lease = NamespaceLease::acquire(&dir, armed_oms::ACCT, OWN_NS).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1));
    Oms::minting(0, Box::new(move || mint.mint().unwrap()))
}

/// The consumer's order query for venue order `V-1`.
fn query() -> QueryOrder {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    QueryOrder {
        target: OrderRef::Venue(vid),
        inst: INST_A,
        placement_nonce: None,
    }
}

/// The request ids the session gave the consumer's commands.
#[derive(Copy, Clone, Debug, PartialEq)]
struct Rpcs {
    place: RpcId,
    amend: RpcId,
    cancel: RpcId,
    query: RpcId,
    fees: RpcId,
}

/// The consumer's commands by the request ids `rpcs` the session gave them: the place, amend and
/// cancel a fresh [`golden_oms`] builds, the query and the fee query.
fn commands(rpcs: Rpcs) -> Vec<(RpcId, VenueCommand)> {
    let mut oms = golden_oms();
    let built = [oms.place(), oms.amend(), oms.cancel()].map(|auth| auth.command().clone());
    let [place, amend, cancel] = built;
    vec![
        (rpcs.place, place),
        (rpcs.amend, amend),
        (rpcs.cancel, cancel),
        (rpcs.query, VenueCommand::Query(query())),
        (rpcs.fees, VenueCommand::FeeQuery),
    ]
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

/// The toy's configuration: its order-entry URL `ws`, and its REST base `rest` with the
/// credential span `span`.
fn toy_cfg(ws: &str, rest: &str, span: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, ws);
    cfg.insert(REST_URL_KEY, rest);
    cfg.insert(REST_URL_REDACT_KEY, span);
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

/// The next request the HTTP server read, the session let run until it has: it panics rather
/// than wait forever for one never made, since the paused clock cannot move on by itself.
async fn requested(http: &mut ScriptedHttp) -> common::Exchange {
    for _ in 0..100_000 {
        if let Some(exchange) = http.try_request() {
            return exchange;
        }
        tokio::task::yield_now().await;
    }
    panic!("nothing requested");
}

/// The resync's watermark in its request line, `GET <base path>/resync?ts=<wall>`.
fn watermark(line: &str) -> String {
    line.rsplit_once("/resync?ts=").unwrap().1.to_owned()
}

/// What the live session left: what its handler heard, the REST base it ran with and its span,
/// the request ids of the consumer's commands and the commands themselves.
struct Recorded {
    heard: Vec<Heard>,
    rest: String,
    span: String,
    rpcs: Rpcs,
    commands: Vec<VenueCommand>,
}

/// Runs the toy's session, journaled at `journal`, through two epochs (the module docs say what
/// each holds).
async fn record(journal: &Path) -> Recorded {
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(config, redaction_key()).unwrap();
    let writer = JournalWriter::create(journal, SHARD, redaction_key()).unwrap();
    let writer = drain.spawn(writer).unwrap();
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let rest = http.url(REST_PATH);
    let span = format!("{}..{}", rest.len() - CREDENTIAL.len(), rest.len());
    let cfg = toy_cfg(&server.url(), &rest, &span);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(session_config(cfg), log.clone()).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(queue))));
    let orders = session.orders();
    let mut oms = golden_oms();
    let auths = [oms.place(), oms.amend(), oms.cancel()];
    let commands = auths.iter().map(|auth| auth.command().clone()).collect();
    let watch = log.clone();
    let script = async move {
        // The first epoch: authenticated, armed and resynced.
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let resync = requested(&mut http).await;
        assert!(resync.line.starts_with(&format!("GET {REST_PATH}/resync")));
        let wm = watermark(&resync.line);
        peer.send("item|rpc=1|i=0|res=ok|ts=600|seq=2");
        settle(|| watch.any(outcome(RpcId(1)))).await;
        let body = format!("rsbegin|wm={wm}|ts=700|seq=3\nrspos|sym=TOYA-PERP|qty=0\nrsend|ts=701");
        resync.answer(OK, &body).await;
        settle(|| watch.resyncs() == 1).await;
        assert!(orders.may_place());
        // The consumer's place, amend and cancel, each signed, each accepted.
        let mut sent = Vec::new();
        for (auth, kind) in auths.into_iter().zip(["place", "amend", "cancel"]) {
            let rpc = orders.submit(auth).unwrap();
            let written = peer.recv().await;
            assert!(written.starts_with(&format!("{kind}|rpc={}|", rpc.0)));
            assert!(written.contains("|sig="));
            peer.send(&format!("item|rpc={}|i=0|res=ok", rpc.0));
            settle(|| watch.any(outcome(rpc))).await;
            sent.push(rpc);
        }
        // Its order query, answered, and fee query, never answered.
        let query = orders.submit_control(ControlCommand::Query(query()));
        let query = query.unwrap();
        let line = format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", query.0);
        assert_eq!(peer.recv().await, line);
        peer.send(&format!("qres|rpc={}|found=0|ts=800|seq=4", query.0));
        let answered = |h: &Heard| matches!(h, Heard::Event(env) if matches!(env.body, ExecEvent::QueryResult(_)));
        settle(|| watch.any(answered)).await;
        let fees = orders.submit_control(ControlCommand::FeeQuery).unwrap();
        assert_eq!(peer.recv().await, format!("fees|rpc={}", fees.0));
        advance(RPC_TIMEOUT).await;
        settle(|| watch.any(outcome(fees))).await;
        // The ping timer, armed as the epoch opened; then the connection drops.
        advance(PING_EVERY).await;
        assert!(peer.recv().await.starts_with("ping|ts="));
        peer.drop_conn();
        settle(|| watch.any(|h| *h == Heard::Ended(key(0)))).await;

        // The second epoch: authenticated, armed and resynced again, then stopped.
        let mut next = reconnected(&mut server).await;
        assert!(next.recv().await.starts_with("auth|ts="));
        next.send(AUTH_ACK);
        let arm = RpcId(fees.0 + 1);
        assert_eq!(next.recv().await, format!("cod|rpc={}|on=1", arm.0));
        let resync = requested(&mut http).await;
        let wm = watermark(&resync.line);
        next.send(&format!("item|rpc={}|i=0|res=ok", arm.0));
        settle(|| watch.any(outcome(arm))).await;
        let body = format!("rsbegin|wm={wm}\nrspos|sym=TOYA-PERP|qty=0\nrsend");
        resync.answer(OK, &body).await;
        settle(|| watch.resyncs() == 2).await;
        churn().await;
        drop(control);
        while next.next().await.is_some() {}
        let [place, amend, cancel] = [sent[0], sent[1], sent[2]];
        Rpcs {
            place,
            amend,
            cancel,
            query,
            fees,
        }
    };
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let (ran, rpcs) = tokio::join!(session.run(), script);
    drop(thaw);
    ran.unwrap();
    drop(session);
    writer.close().unwrap();
    Recorded {
        heard: log.heard(),
        rest,
        span,
        rpcs,
        commands,
    }
}

/// Records the toy's session at `journal` on a runtime of its own whose clock moves only as the
/// script moves it.
fn recorded(journal: &Path) -> Recorded {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(record(journal))
}

// ---------------------------------------------------------------------------------------------
// A golden: the journal, what its replay is given, and what it holds.
// ---------------------------------------------------------------------------------------------

/// What a golden's replay is given besides its journal: the REST base, its credential blanked
/// as the journal keeps it, the base's credential span, and the consumer's request ids.
#[derive(Clone, Debug, PartialEq)]
struct Session {
    rest: String,
    span: String,
    rpcs: Rpcs,
}

impl Session {
    /// The session `recorded` ran, its REST credential blanked: a replay needs only the bytes
    /// outside the spans.
    fn of(recorded: &Recorded) -> Session {
        let start = recorded.rest.len() - CREDENTIAL.len();
        let blank = String::from_utf8(vec![BLANK; CREDENTIAL.len()]).unwrap();
        Session {
            rest: format!("{}{blank}", &recorded.rest[..start]),
            span: recorded.span.clone(),
            rpcs: recorded.rpcs,
        }
    }

    /// As a golden's `session.txt` holds it.
    fn text(&self) -> String {
        let r = self.rpcs;
        format!(
            "# FBC-qwq's exact-replay golden, written by crates/fbc-runtime/tests/exact_replay.rs; \
             refresh it with scripts/check.sh --refresh-goldens, never by hand.\n\
             rest_url={}\nrest_url.redact={}\nplace={}\namend={}\ncancel={}\nquery={}\nfees={}\n",
            self.rest, self.span, r.place.0, r.amend.0, r.cancel.0, r.query.0, r.fees.0
        )
    }

    /// Read back from a golden's `session.txt`.
    fn parse(text: &str) -> Session {
        let fields: BTreeMap<&str, &str> = text
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| line.split_once('='))
            .collect();
        let rpc = |name: &str| RpcId(fields[name].parse().unwrap());
        Session {
            rest: fields["rest_url"].to_owned(),
            span: fields["rest_url.redact"].to_owned(),
            rpcs: Rpcs {
                place: rpc("place"),
                amend: rpc("amend"),
                cancel: rpc("cancel"),
                query: rpc("query"),
                fees: rpc("fees"),
            },
        }
    }
}

/// Every outbound frame and HTTP request the journal at `journal` holds, one per line, its spans
/// blanked as the reader returns them: what a person checks a golden by.
fn rendered(journal: &Path) -> String {
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).replace('\n', "\\n");
    let id = |rpc: Option<RpcId>| rpc.map_or_else(|| "-".to_owned(), |rpc| rpc.0.to_string());
    let mut out = String::new();
    for record in JournalReader::open(journal, SHARD).unwrap() {
        let line = match record.unwrap() {
            Record::Outbound {
                conn,
                rpc,
                opcode,
                frame,
                ..
            } => format!(
                "frame {}:{} rpc={} {opcode:?} {}",
                conn.conn,
                conn.epoch,
                id(rpc),
                text(frame.bytes())
            ),
            Record::HttpRequest {
                conn,
                tag,
                rpc,
                req,
                ..
            } => format!(
                "http {}:{} tag={} rpc={} {:?} {} headers={} body={}",
                conn.conn,
                conn.epoch,
                tag.0,
                id(rpc),
                req.method,
                req.url.as_str(),
                req.headers.len(),
                text(req.body.bytes())
            ),
            _ => continue,
        };
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// Where a signature starts in a frame the toy signs: just past its `|sig=`.
fn sig_at(text: &str) -> Option<usize> {
    text.find("|sig=").map(|at| at + "|sig=".len())
}

/// Every signature the journal at `journal` holds, as the reader returns it: each kept outside
/// every redaction span of its frame (0006 keeps signatures; a codec that marked one as a span
/// would have exact replay compare none of its bytes), and each, as the toy signs, 16 hex digits
/// that are not the blank the reader puts in a span.
fn signatures(journal: &Path) -> Vec<String> {
    let mut sigs = Vec::new();
    for record in JournalReader::open(journal, SHARD).unwrap() {
        let Record::Outbound { frame, .. } = record.unwrap() else {
            continue;
        };
        let text = String::from_utf8(frame.bytes().to_vec()).unwrap();
        let Some(start) = sig_at(&text) else {
            continue;
        };
        let covered = |span: &std::ops::Range<u32>| span.end as usize > start;
        assert!(!frame.redactions().iter().any(covered), "{text}");
        let sig = &text[start..];
        assert_eq!(sig.len(), 16, "{text}");
        assert!(sig.bytes().all(|b| b.is_ascii_hexdigit()), "{text}");
        assert!(sig.bytes().any(|b| b != BLANK), "{text}");
        sigs.push(sig.to_owned());
    }
    sigs
}

/// Replays the journal at `journal` into a fresh codec of the toy, configured as `session` says,
/// with `commands` supplied by request id, and with `edit` applied to each record first: what the
/// handler heard, what exact replay found, and the counts.
fn replay(
    journal: &Path,
    session: &Session,
    commands: Vec<(RpcId, VenueCommand)>,
    edit: impl Fn(Record) -> Record,
) -> (Vec<Heard>, ExactReport, ExecReplayCounters) {
    let log = Log::default();
    // A replay opens nothing: the order-entry URL is never journaled, so any will do.
    let cfg = toy_cfg("ws://127.0.0.1:1/replayed", &session.rest, &session.span);
    let config = ExecReplayConfig {
        venue: &TOY,
        cfg,
        creds: Secrets::new(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        conn: CONN,
        compare_outbound: true,
    };
    // The control stands throughout: the live session's was dropped between inputs.
    let (mut replay, _control) = ExecReplay::new(config, log.clone()).unwrap();
    replay.set_commands(move |rpc| {
        let supplied = commands.iter().find(|(id, _)| *id == rpc);
        supplied.map(|(_, cmd)| cmd.clone())
    });
    let records = JournalReader::open(journal, SHARD).unwrap();
    replay.run(records.map(|r| r.map(&edit))).unwrap();
    let (report, counters) = (replay.exact().unwrap(), replay.counters());
    drop(replay);
    (log.heard(), report, counters)
}

/// An exact replay of a journal holding `matched` frames and requests.
fn exact(matched: u64) -> ExactReport {
    ExactReport {
        matched,
        ..ExactReport::default()
    }
}

/// Whether `value`, the refresh variable's, asks for the golden to be rewritten: only `1` does.
fn asked(value: Option<OsString>) -> bool {
    value.is_some_and(|value| value == "1")
}

/// Replaces the golden at `golden` with the one at `fresh`, whole, when `asked`: whether it did.
fn store(fresh: &Path, golden: &Path, asked: bool) -> bool {
    if asked {
        let _ = fs::remove_dir_all(golden);
        copy_dir(fresh, golden);
    }
    asked
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let (from, to) = (entry.path(), to.join(entry.file_name()));
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&from, &to);
        } else {
            fs::copy(&from, &to).unwrap();
        }
    }
}

/// Every file under `dir`, by its path below it, with its bytes.
fn contents(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(at) = dirs.pop() {
        for entry in fs::read_dir(&at).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let bytes = fs::read(&path).unwrap();
                files.insert(path.strip_prefix(dir).unwrap().to_path_buf(), bytes);
            }
        }
    }
    files
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// The session recorded afresh replays exactly on two replays: every frame and request its
/// journal holds reproduced outside its spans, signatures included, nothing unwritten, and the
/// handler hearing what it heard live. The fresh recording replaces the committed golden only
/// when the refresh is asked for.
#[test]
fn a_freshly_journaled_session_replays_to_its_outbound_bytes_on_every_replay() {
    let _golden = GOLDEN.lock().unwrap_or_else(PoisonError::into_inner);
    let root = fresh_dir("exact_replay_fresh");
    let live = recorded(&root.join(JOURNAL));
    let session = Session::of(&live);
    // The commands a fresh registry builds are the ones the live session sent.
    let supplied = commands(session.rpcs);
    let built: Vec<VenueCommand> = supplied[..3].iter().map(|(_, cmd)| cmd.clone()).collect();
    assert_eq!(built, live.commands);

    let shown = rendered(&root.join(JOURNAL));
    assert_eq!(shown.lines().count() as u64, OUTBOUND_RECORDS, "{shown}");
    // The toy's token and the REST credential are blanked; the signatures are kept.
    assert!(shown.contains("|token=22222222222222222"), "{shown}");
    assert!(
        shown.contains("/acct/2222222222222222222/resync?ts="),
        "{shown}"
    );
    assert_eq!(shown.matches("|sig=").count(), 3, "{shown}");
    let journal = root.join(JOURNAL);
    assert_eq!(signatures(&journal).len(), 3);

    let (first, report, counters) = replay(&journal, &session, supplied.clone(), |r| r);
    let (second, again, _) = replay(&journal, &session, supplied, |r| r);
    assert_eq!(report, exact(OUTBOUND_RECORDS));
    assert!(report.is_exact());
    assert_eq!(again, report);
    assert_eq!(first, live.heard);
    assert_eq!(second, live.heard);
    let none_lost = ExecReplayCounters {
        unsupplied: 0,
        lost_contexts: 0,
        unplaced: 0,
        decode_errors: 0,
        gaps: 0,
        ..counters
    };
    assert_eq!(counters, none_lost);
    assert_eq!(counters.encodes, 7);

    fs::write(root.join(SESSION), session.text()).unwrap();
    fs::write(root.join(OUTBOUND), shown).unwrap();
    store(&root, &golden_dir(), asked(std::env::var_os(REFRESH)));
}

/// The committed golden replays exactly on two replays, and its rendering is the one committed
/// beside it, which a person checked: a change to the toy's codec, the replay or the journal that
/// changes one outbound byte outside a span fails here until the golden is refreshed.
#[test]
fn the_committed_golden_replays_to_its_journaled_outbound_bytes() {
    let _golden = GOLDEN.lock().unwrap_or_else(PoisonError::into_inner);
    let dir = golden_dir();
    let journal = dir.join(JOURNAL);
    let committed = fs::read_to_string(dir.join(OUTBOUND)).unwrap();
    assert_eq!(rendered(&journal), committed);
    assert_eq!(committed.lines().count() as u64, OUTBOUND_RECORDS);
    let session = Session::parse(&fs::read_to_string(dir.join(SESSION)).unwrap());
    assert_eq!(Session::parse(&session.text()), session);
    assert_eq!(signatures(&journal).len(), 3);

    let (first, report, counters) = replay(&journal, &session, commands(session.rpcs), |r| r);
    let (second, again, _) = replay(&journal, &session, commands(session.rpcs), |r| r);
    assert_eq!(report, exact(OUTBOUND_RECORDS));
    assert!(report.is_exact());
    assert_eq!(again, report);
    assert_eq!(first, second);
    assert_eq!((counters.unsupplied, counters.gaps), (0, 0));
}

/// A command supplied otherwise than live, or a context other than the journal's, is a
/// difference: the toy signs the order's fields with the context's time and nonce, so the frame
/// replayed differs from the journal's, signature included, and every other frame still matches.
#[test]
fn a_command_or_context_other_than_the_journals_is_a_difference() {
    let _golden = GOLDEN.lock().unwrap_or_else(PoisonError::into_inner);
    let dir = golden_dir();
    let journal = dir.join(JOURNAL);
    let session = Session::parse(&fs::read_to_string(dir.join(SESSION)).unwrap());
    let place = session.rpcs.place;
    let sig = |record: &Record| {
        let mut sig = None;
        if let Record::Outbound { frame, .. } = record {
            let text = String::from_utf8_lossy(frame.bytes()).into_owned();
            sig = text.rsplit_once("|sig=").map(|(_, sig)| sig.to_owned());
        }
        sig
    };

    // The place, a lot larger than live.
    let mut supplied = commands(session.rpcs);
    if let VenueCommand::Place(order) = &mut supplied[0].1 {
        order.qty = Lots::new(order.qty.get() + 1).unwrap();
    }
    let (_, report, _) = replay(&journal, &session, supplied, |r| r);
    // The place's context, a nonce other than the one reserved live.
    let renonced = |record: Record| match record {
        Record::EncodeCtx { rpc, mut ctx } if rpc == Some(place) => {
            ctx.nonces = NonceBlock::new(vec![ctx.nonce(0).unwrap() + 1]);
            Record::EncodeCtx { rpc, ctx }
        }
        other => other,
    };
    let (_, renonce, _) = replay(&journal, &session, commands(session.rpcs), renonced);

    for found in [report, renonce] {
        assert!(!found.is_exact());
        assert_eq!(found.matched, OUTBOUND_RECORDS - 1);
        assert_eq!((found.unwritten, found.excluded), (0, 0));
        assert_eq!(found.differences.len(), 1, "{found:?}");
        let diff = &found.differences[0];
        let replayed = diff.replayed.as_ref().unwrap();
        let rpc = |r: &Record| matches!(r, Record::Outbound { rpc: Some(id), .. } if *id == place);
        assert!(rpc(&diff.journaled) && rpc(replayed), "{diff:?}");
        assert_ne!(sig(&diff.journaled), sig(replayed));
        assert!(sig(replayed).is_some());
    }
}

/// A signature is compared byte for byte like the rest of its frame: the journal's place frame
/// with one digit of its signature changed, every other byte and its spans as they were, is the
/// one difference, the replay's frame equal to it up to the signature.
#[test]
fn a_signature_alone_other_than_the_journals_is_a_difference() {
    let _golden = GOLDEN.lock().unwrap_or_else(PoisonError::into_inner);
    let dir = golden_dir();
    let journal = dir.join(JOURNAL);
    let session = Session::parse(&fs::read_to_string(dir.join(SESSION)).unwrap());
    let place = session.rpcs.place;
    let resigned = |record: Record| match record {
        Record::Outbound {
            at,
            conn,
            rpc,
            opcode,
            frame,
        } if rpc == Some(place) => {
            let mut bytes = frame.bytes().to_vec();
            let last = bytes.last_mut().unwrap();
            *last = if *last == b'0' { b'1' } else { b'0' };
            let frame = WireSlice::redacted(bytes, frame.redactions().to_vec()).unwrap();
            Record::Outbound {
                at,
                conn,
                rpc,
                opcode,
                frame,
            }
        }
        other => other,
    };
    let (_, found, _) = replay(&journal, &session, commands(session.rpcs), resigned);
    assert!(!found.is_exact());
    assert_eq!(found.matched, OUTBOUND_RECORDS - 1);
    assert_eq!((found.unwritten, found.excluded), (0, 0));
    assert_eq!(found.differences.len(), 1, "{found:?}");
    let diff = &found.differences[0];
    let text = |record: &Record| match record {
        Record::Outbound { rpc, frame, .. } if *rpc == Some(place) => {
            String::from_utf8(frame.bytes().to_vec()).unwrap()
        }
        other => panic!("not the place's frame: {other:?}"),
    };
    let (journaled, replayed) = (text(&diff.journaled), text(diff.replayed.as_ref().unwrap()));
    let start = sig_at(&journaled).unwrap();
    assert_eq!(journaled[..start], replayed[..start]);
    assert_ne!(journaled[start..], replayed[start..]);
}

/// What a `Degraded` span may explain is excluded without counting against exactness, and an
/// unwritten frame or request counts against it as a difference does.
#[test]
fn exactness_is_no_difference_and_nothing_unwritten_outside_the_excluded() {
    let excluded = ExactReport {
        matched: 2,
        excluded: 3,
        ..ExactReport::default()
    };
    assert!(excluded.is_exact());
    let unwritten = ExactReport {
        unwritten: 1,
        ..excluded.clone()
    };
    assert!(!unwritten.is_exact());
}

/// The fresh recording replaces a golden only when its refresh is asked for, and then whole, a
/// file the fresh one lacks removed; only the variable set to `1` asks.
#[test]
fn the_golden_is_rewritten_only_when_its_refresh_is_asked() {
    let root = fresh_dir("exact_replay_store");
    let (fresh, golden) = (root.join("fresh"), root.join("golden"));
    fs::create_dir_all(fresh.join(JOURNAL)).unwrap();
    fs::write(fresh.join(JOURNAL).join("4-000000.fbcj"), "new journal").unwrap();
    fs::write(fresh.join(SESSION), "new session").unwrap();
    fs::create_dir_all(&golden).unwrap();
    fs::write(golden.join(SESSION), "old session").unwrap();
    fs::write(golden.join("stale.txt"), "old").unwrap();
    let before = contents(&golden);

    assert!(!store(&fresh, &golden, false));
    assert_eq!(contents(&golden), before);
    assert!(store(&fresh, &golden, true));
    assert_eq!(contents(&golden), contents(&fresh));

    assert!(!asked(None));
    assert!(!asked(Some(OsString::from("0"))));
    assert!(!asked(Some(OsString::from("yes"))));
    assert!(asked(Some(OsString::from("1"))));
}
