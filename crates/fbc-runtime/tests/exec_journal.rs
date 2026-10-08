//! FBC-2pr's done line (decisions 0006, 0009, 0024, 0028, 0078): an order-entry session with a
//! journal records everything that crosses its boundary, in the order it happened. Two sessions
//! are journaled, read back and checked record by record: one of the conformance toy (an
//! authentication frame carrying its token in a redaction span, the venue's acknowledgement
//! echoing it, a ping, the session's cancel-on-disconnect arm and resync, and an order query
//! with its nonce), and one of fbc-core's `auth_toy` (a login over HTTP whose response carries
//! a token, a cookie and a key echoed in a header's name, a refresh timer that logs in again,
//! and a frame echoing a key), its `on_open` and `on_timer` given a nonce each. Each read-back
//! record holds its stamp, its frame's kind and spans, its request id or its context, and the
//! journal files hold none of the synthetic credentials, only their keyed hashes.
//!
//! What an order-entry stream brings is journaled under Safety, since it carries the acks and
//! fills 0006 reserves room for, and so are each epoch's opening and closing and every nonce; an
//! encode's context goes under its command's class (an adding order's Normal); the rest under
//! Normal. Error paths: a nonce source that reserves fewer than asked has what it did
//! reserve journaled and no context, and a run dropped mid-epoch has its epoch journaled closed
//! when the session drops.
//!
//! Every credential of the `auth_toy` session is synthetic and assembled at run time; the
//! conformance toy's token is its own synthetic constant. The expected hashes are computed with
//! `hmac` and `sha2` directly, not through the journal's own code.

mod armed_oms;
#[path = "../../fbc-core/tests/auth_toy/mod.rs"]
mod auth_toy;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use common::{Peer, ScriptedHttp, ScriptedWs};
use fbc_core::{
    AccountKey, AccountSummary, AssetKey, ConfigError, ConnKey, ConnState, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec,
    ExecEndpoint, ExecEvent, ExecSink, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentSpecDraft, MdCodec, NonceBlock, NonceSource, NotSentReason,
    OpKind, PathStamps, RateCharge, RawFrame, RpcCall, RpcId, Secrets, SpecTable, StreamId,
    SubmitOutcome, Subscription, SymbolError, TimerTag, TrafficClass, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, WallNs, WireSlice, WireUrl,
};
use fbc_journal::{
    BLANK, ControlEvent, Entry, HeaderRec, JournalReader, JournalSink, JournalWriter,
    NonceSourceId, Opcode, QueueSink, Record, RecordRef, Recorded, RedactionKey, SinkConfig,
    SpanDigest, WriteRes, WsControl, journal_queue,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecControl, ExecOrders, ExecSession, ExecSessionConfig, ExecSessionError,
    IngestClock, Journal, ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve,
    WriteStall,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const SHARD: u16 = 3;
/// The connection number the sessions stamp.
const CONN: u16 = 6;
/// The account the sessions trade.
const ACCT: AccountKey = AccountKey::new(9);
/// The number the consumer gives the sessions' nonce source in the journal: its own, not the
/// account's.
const SOURCE: NonceSourceId = NonceSourceId(41);
/// The order-entry endpoint's URL; required.
const URL: &str = "exec.url";
/// The `auth_toy` login's URL, the scripted HTTP server's.
const LOGIN: &str = "login.url";
/// How many nonces the `auth_toy` session's `on_open` and `on_timer` ask for.
const CALL_NONCES: &str = "call.nonces";
/// Any value: the `auth_toy` session's codec names a span past the end of every frame (a codec
/// defect).
const BAD_SPANS: &str = "bad.spans";
/// A frame the `auth_toy` session's codec panics redacting (a codec defect).
const PANIC_ON: &str = "panic.on";
/// Any value: the `auth_toy` session's codec drops the control in [`STOP`] as it redacts an
/// HTTP result, as a control dropped on another thread may be then.
const DROP_ON_HTTP: &str = "drop.on.http";
/// How many milliseconds the `auth_toy` session's arm waits for its answer; an hour by default.
const ARM_TIMEOUT_MS: &str = "arm.timeout.ms";
/// The conformance toy's acknowledgement of its authentication, echoing its token.
const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ---------------------------------------------------------------------------------------------
// The journal and what it holds.
// ---------------------------------------------------------------------------------------------

fn key_bytes() -> Vec<u8> {
    format!("SYNTHKEY-{}-order-entry-journal-test", "q4w").into_bytes()
}

fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&key_bytes()).unwrap())
}

/// HMAC-SHA-256 of `bytes` under the test key, computed without the journal.
fn hmac(bytes: &[u8]) -> SpanDigest {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes()).unwrap();
    mac.update(bytes);
    SpanDigest(mac.finalize().into_bytes().into())
}

/// A synthetic credential: lower case, as an HTTP header name arrives, and free of `|`.
fn secret(what: &str) -> String {
    format!("synth{what}{}secret", "-x8n")
}

fn blank(n: usize) -> String {
    String::from_utf8(vec![BLANK; n]).unwrap()
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Every byte under `dir` as written, in path order, a closed segment decompressed.
fn written_bytes(dir: &Path) -> Vec<u8> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            out.extend(written_bytes(&path));
        } else if path.extension().is_some_and(|e| e == "zst") {
            out.extend(zstd::decode_all(fs::File::open(&path).unwrap()).unwrap());
        } else {
            out.extend(fs::read(&path).unwrap());
        }
    }
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// The journal queue's sink, keeping each record it is offered with its class and the wall
/// time it is filed under.
struct Tee {
    queue: QueueSink,
    offered: Vec<(TrafficClass, Record)>,
    filed: Vec<WallNs>,
}

impl JournalSink for Tee {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.offered.push((class, record.clone()));
        self.filed.push(now);
        self.queue.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        self.offered.push((class, record.to_record()));
        self.filed.push(now);
        self.queue.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.queue.omit(class, now)
    }
}

/// What a journaled run left: the records read back, every byte of the files, what the session
/// offered with its class, and how the run ended.
struct Run {
    entries: Vec<Entry>,
    files: Vec<u8>,
    offered: Vec<(TrafficClass, Record)>,
    /// The wall time each offered record was filed under.
    filed: Vec<WallNs>,
    ended: Result<(), ExecSessionError>,
}

impl Run {
    /// The class each offered record matching `pick` was offered under.
    fn classes(&self, pick: impl Fn(&Record) -> bool) -> Vec<TrafficClass> {
        let offered = self.offered.iter().filter(|(_, r)| pick(r));
        offered.map(|(class, _)| *class).collect()
    }
}

/// The events the handler heard.
type Heard = Rc<RefCell<Vec<ExecEvent>>>;

/// What the session has offered its journal so far, as a test waits on it.
#[derive(Clone)]
struct Offered(Rc<RefCell<Tee>>);

impl Offered {
    /// Whether epoch `key` has been journaled closed.
    fn closed(&self, key: ConnKey) -> bool {
        let tee = self.0.borrow();
        tee.offered.iter().any(
            |(_, r)| matches!(r, Record::Control { ev: ControlEvent::Closed(k), .. } if *k == key),
        )
    }

    /// How many HTTP results have been journaled.
    fn answers(&self) -> usize {
        let tee = self.0.borrow();
        let answers = tee.offered.iter();
        answers
            .filter(|(_, r)| matches!(r, Record::HttpResult { .. }))
            .count()
    }
}

/// Runs a session built from `config` with a journal, while `play` drives the venue with the
/// session's orders, its control and what its handler hears, and reads back what it journaled.
async fn journaled<F, Fut>(name: &str, config: ExecSessionConfig, play: F) -> Run
where
    F: FnOnce(ExecOrders, ExecControl, Heard) -> Fut,
    Fut: Future<Output = ()>,
{
    let play = |orders, control, heard, _: Offered| play(orders, control, heard);
    journaled_seeing(name, config, play).await
}

/// [`journaled`], `play` also seeing what the session has offered its journal so far.
async fn journaled_seeing<F, Fut>(name: &str, config: ExecSessionConfig, play: F) -> Run
where
    F: FnOnce(ExecOrders, ExecControl, Heard, Offered) -> Fut,
    Fut: Future<Output = ()>,
{
    let root = fresh_dir(name);
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, redaction_key()).unwrap())
        .unwrap();
    let tee = Rc::new(RefCell::new(Tee {
        queue,
        offered: Vec::new(),
        filed: Vec::new(),
    }));
    let heard = Heard::default();
    let keep = Rc::clone(&heard);
    let handler = move |env: Envelope<ExecEvent>| keep.borrow_mut().push(env.body);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    session.set_journal(Journal::new(tee.clone()));
    let orders = session.orders();
    let offered = Offered(Rc::clone(&tee));
    let (ended, ()) = tokio::join!(session.run(), play(orders, control, heard, offered));
    drop(session);
    writer.close().unwrap();
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    let files = written_bytes(&root);
    fs::remove_dir_all(&root).unwrap();
    let tee = Rc::try_unwrap(tee).ok().unwrap().into_inner();
    Run {
        entries,
        files,
        offered: tee.offered,
        filed: tee.filed,
        ended,
    }
}

/// Waits, in real time, until `done`.
async fn until(done: impl Fn() -> bool) {
    for _ in 0..5_000 {
        if done() {
            return;
        }
        tokio::time::sleep(ms(2)).await;
    }
    panic!("never happened");
}

/// The next text frame from the client, past the pongs it answers the venue's pings with.
async fn recv_text(peer: &mut Peer) -> String {
    loop {
        let heard = peer.recv().await;
        if heard != "<pong>" {
            return heard;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The venues: the conformance toy and the `auth_toy`, each on one order-entry endpoint.
// ---------------------------------------------------------------------------------------------

/// A venue declaring the conformance toy's caps, its one order-entry endpoint at [`URL`], and
/// building its codec with `codec`.
struct Venue {
    codec: fn(&VenueConfig) -> Box<dyn ExecCodec>,
}

impl Venue {
    fn leak(codec: fn(&VenueConfig) -> Box<dyn ExecCodec>) -> &'static Venue {
        Box::leak(Box::new(Venue { codec }))
    }
}

impl VenueFactory for Venue {
    fn id(&self) -> &'static str {
        "TOY-JOURNAL"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(exec_toy::caps())
    }

    fn parse_fbc_common_symbol(&self, _: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NotCommonForm)
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
        cfg: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok((self.codec)(cfg)))
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

fn conformance_toy(_: &VenueConfig) -> Box<dyn ExecCodec> {
    Box::new(exec_toy::ToyExec::new(Box::new(exec_toy::ToySigner)))
}

fn auth_toy(cfg: &VenueConfig) -> Box<dyn ExecCodec> {
    Box::new(AuthWrap {
        login: cfg.get(LOGIN).unwrap().to_owned(),
        call_nonces: cfg.get(CALL_NONCES).unwrap().parse().unwrap(),
        bad_spans: cfg.get(BAD_SPANS).is_some(),
        panic_on: cfg.get(PANIC_ON).map(str::to_owned),
        drop_on_http: cfg.get(DROP_ON_HTTP).is_some(),
        arm_timeout: cfg
            .get(ARM_TIMEOUT_MS)
            .map_or(Duration::from_secs(3_600), |v| ms(v.parse().unwrap())),
    })
}

/// fbc-core's `auth_toy` as an order-entry session takes it: its login sent to the scripted
/// HTTP server, `call_nonces` nonces asked for its `on_open` and `on_timer`, and the session's
/// cancel-on-disconnect arm encoded as a `cod|rpc=<id>` frame (the toy encodes no command),
/// answered by nothing within the test.
struct AuthWrap {
    login: String,
    call_nonces: u16,
    bad_spans: bool,
    panic_on: Option<String>,
    drop_on_http: bool,
    arm_timeout: Duration,
}

thread_local! {
    /// The control the `auth_toy` session's codec drops as it redacts an HTTP result, with
    /// [`DROP_ON_HTTP`].
    static STOP: RefCell<Option<ExecControl>> = const { RefCell::new(None) };
    /// How many HTTP results the `auth_toy` session's codec was handed.
    static DECODED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl AuthWrap {
    /// `fx` with every login sent to the scripted server.
    fn relocate(&self, mut fx: Effects, out: &mut Effects) {
        for effect in fx.take() {
            out.push(match effect {
                Effect::Http {
                    tag,
                    mut req,
                    rpc,
                    timeout,
                    class,
                    charge,
                } => {
                    req.url = WireUrl::plain(self.login.as_str());
                    Effect::Http {
                        tag,
                        req,
                        rpc,
                        timeout,
                        class,
                        charge,
                    }
                }
                other => other,
            });
        }
    }
}

impl ExecCodec for AuthWrap {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        match call {
            CtxCall::Open(_) | CtxCall::Timer(_) => self.call_nonces,
            CtxCall::Resync => auth_toy::AuthToy.nonces_for(call),
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        let mut asked = Effects::new();
        auth_toy::AuthToy.on_open(stream, ctx, &mut asked);
        self.relocate(asked, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        if *cmd != VenueCommand::ArmCancelOnDisconnect(true) {
            return auth_toy::AuthToy.encode(cmd, rpc, specs, ctx, t, fx);
        }
        fx.push(Effect::Send {
            stream: auth_toy::EXEC_STREAM,
            frame: WireSlice::plain(format!("cod|rpc={}", rpc.0).into_bytes()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: self.arm_timeout,
            }),
            class: cmd.traffic_class(),
            charge: RateCharge::one(OpKind::Control, None),
        });
        Ok(EncodeReceipt::new())
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        auth_toy::AuthToy.on_frame(stream, f, scope, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        DECODED.with(|n| n.set(n.get() + 1));
        auth_toy::AuthToy.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        let mut asked = Effects::new();
        auth_toy::AuthToy.on_timer(tag, ctx, &mut asked);
        self.relocate(asked, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        auth_toy::AuthToy.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        auth_toy::AuthToy.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Frame(f) if self.panic_on.as_deref().map(str::as_bytes) == Some(f.bytes()) => {
                panic!("the codec fails redacting")
            }
            Inbound::Frame(f) if self.bad_spans => {
                let past = u32::try_from(f.bytes().len()).unwrap() + 1;
                let span = 0..past;
                InboundSpans::frame(vec![span])
            }
            Inbound::Http(..) if self.drop_on_http => {
                drop(STOP.with(|stop| stop.borrow_mut().take()));
                auth_toy::AuthToy.redact_inbound(input)
            }
            _ => auth_toy::AuthToy.redact_inbound(input),
        }
    }
}

/// Nonces counted up from 100; with `short`, one fewer than asked. Each reservation takes
/// `delay` of wall time, as a source persisting its nonces might, and logs the wall time it
/// returned at.
#[derive(Default)]
struct Counting {
    short: bool,
    delay: Duration,
    returned: Arc<std::sync::Mutex<Vec<WallNs>>>,
    next: u64,
}

impl Counting {
    fn new() -> Counting {
        Counting {
            next: 100,
            ..Counting::default()
        }
    }
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        std::thread::sleep(self.delay);
        let len = len - u16::from(self.short);
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        let now = WallNs(i64::try_from(since.unwrap().as_nanos()).unwrap());
        self.returned.lock().unwrap().push(now);
        block
    }
}

/// A session of `venue` at `url`, its venue configured with `cfg`, trading [`ACCT`], reconnecting
/// only after a minute.
fn config(
    venue: &'static Venue,
    url: &str,
    cfg: &[(&'static str, String)],
    nonces: Counting,
) -> ExecSessionConfig {
    let mut venue_cfg = VenueConfig::new();
    venue_cfg.insert(URL, url);
    for (k, v) in cfg {
        venue_cfg.insert(k, v);
    }
    let limits = venue.caps(&venue_cfg).unwrap().limits;
    let minute = Duration::from_secs(60);
    ExecSessionConfig {
        venue,
        cfg: venue_cfg,
        creds: Secrets::new(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: exec_toy::OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(minute, minute, 100, minute * 10, ms(5_000)).unwrap(),
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        nonce_source: SOURCE,
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    }
}

// ---------------------------------------------------------------------------------------------
// What a record is, for the order checks.
// ---------------------------------------------------------------------------------------------

/// A record's kind, with what the order checks compare: an inbound frame's text as read back
/// (its spans blanked) and its span count, an outbound frame's text, kind and span count, a
/// write's result, a nonce, a context's request id and nonces, a timer's tag, an HTTP request's
/// tag and request id, an HTTP result's tag and status, a control frame's kind, a connection
/// change.
#[derive(Debug, PartialEq)]
enum Seen {
    In(String, usize),
    Out(String, Opcode, usize),
    Wrote(WriteRes),
    Nonce(u64),
    Ctx(Option<RpcId>, Vec<u64>),
    Timer(TimerTag),
    Ask(HttpTag, Option<RpcId>),
    Answer(HttpTag, u16),
    Ping,
    Close,
    Opened(ConnKey),
    Closed(ConnKey),
}

fn seen(record: &Record) -> Seen {
    let text = |b: &[u8]| String::from_utf8(b.to_vec()).unwrap();
    match record {
        Record::Inbound { bytes, redact, .. } => Seen::In(text(&bytes.0), redact.len()),
        Record::Outbound { opcode, frame, .. } => {
            Seen::Out(text(frame.bytes()), *opcode, frame.redactions().len())
        }
        Record::WriteResult { result, .. } => Seen::Wrote(*result),
        Record::Nonce { source, value } => {
            assert_eq!(*source, SOURCE);
            Seen::Nonce(*value)
        }
        Record::EncodeCtx { rpc, ctx } => Seen::Ctx(*rpc, ctx.nonces.as_slice().to_vec()),
        Record::Timer { tag, .. } => Seen::Timer(*tag),
        Record::HttpRequest { tag, rpc, .. } => Seen::Ask(*tag, *rpc),
        Record::HttpResult { tag, result, .. } => {
            Seen::Answer(*tag, result.as_ref().map_or(0, |r| r.status))
        }
        Record::InboundControl {
            frame: WsControl::Ping(_),
            ..
        } => Seen::Ping,
        Record::InboundControl {
            frame: WsControl::Close(_),
            ..
        } => Seen::Close,
        Record::Control {
            ev: ControlEvent::Opened(key),
            ..
        } => Seen::Opened(*key),
        Record::Control {
            ev: ControlEvent::Closed(key),
            ..
        } => Seen::Closed(*key),
        other => panic!("not expected here: {other:?}"),
    }
}

/// The stamp of each stamped record, in journal order.
fn stamps(entries: &[Entry]) -> Vec<fbc_core::Stamp> {
    let stamp = |e: &Entry| match &e.record {
        Record::Inbound { stamp, .. }
        | Record::InboundControl { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::Timer { stamp, .. } => Some(*stamp),
        _ => None,
    };
    entries.iter().filter_map(stamp).collect()
}

/// An order query for venue order `V-1`: one item.
fn query() -> ControlCommand {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    ControlCommand::Query(fbc_core::QueryOrder {
        target: fbc_core::OrderRef::Venue(vid),
        inst: exec_toy::INST_A,
        placement_nonce: None,
    })
}

fn is_resync_end(heard: &Heard) -> bool {
    heard.borrow().contains(&ExecEvent::ResyncEnd)
}

fn query_answered(heard: &Heard) -> bool {
    let answer = |ev: &ExecEvent| matches!(ev, ExecEvent::QueryResult(_));
    heard.borrow().iter().any(answer)
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// The conformance toy's session journals, in order: the epoch opening; `on_open`'s context and
/// the authentication frame it writes (text, its token in a span) with the write's result; the
/// venue's ping; its acknowledgement echoing the token (a span); the arm's nonce, context, frame
/// and write; the resync's context, frame and write; the venue's answers; the query's nonce,
/// context, frame and write; its answer; the epoch closing. Every stamped record carries the
/// next ingest sequence of the epoch. The toy's token is in no file, its keyed hash is. What the
/// stream brought is Safety, as are the epoch's opening and closing and every nonce; the ping
/// and the contexts of `on_open` and the resync are Normal; the arm's and the query's contexts
/// are their commands' class, Safety.
#[tokio::test]
async fn the_conformance_toys_order_entry_session_journals_what_crosses_it_in_order_with_no_credential()
 {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let config = config(venue, &server.url(), &[], Counting::new());
    let (arm, rpc) = (RpcId(1), RpcId(2));
    let run = journaled(
        "exec_journal_conformance",
        config,
        |orders, control, heard| async move {
            let mut peer = server.accept().await;
            assert!(recv_text(&mut peer).await.starts_with("auth|ts="));
            peer.ping_with(b"hb");
            peer.send(AUTH_ACK);
            assert_eq!(
                recv_text(&mut peer).await,
                format!("cod|rpc={}|on=1", arm.0)
            );
            let resync = recv_text(&mut peer).await;
            let wm = resync.strip_prefix("resync|ts=").unwrap().to_owned();
            peer.send(&format!("item|rpc={}|i=0|res=ok", arm.0));
            peer.send_all([format!("rsbegin|wm={wm}"), "rsend".to_owned()]);
            until(|| is_resync_end(&heard)).await;
            assert_eq!(orders.submit_control(query()).unwrap(), rpc);
            let queried = format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", rpc.0);
            assert_eq!(recv_text(&mut peer).await, queried);
            peer.send(&format!("qres|rpc={}|found=0", rpc.0));
            until(|| query_answered(&heard)).await;
            drop(control);
            while peer.next().await.is_some() {}
        },
    )
    .await;
    run.ended.as_ref().unwrap();

    // No credential reaches a file; its keyed hash does.
    let token = exec_toy::TOY_TOKEN;
    assert!(
        !contains(&run.files, token.as_bytes()),
        "the token was written"
    );
    assert!(contains(&run.files, &hmac(token.as_bytes()).0));

    // Everything, in order, as read back.
    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    let Seen::Out(auth, ..) = &read[2] else {
        panic!("{read:#?}")
    };
    let ts: i64 = auth["auth|ts=".len()..]
        .split('|')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let Seen::Out(resync, ..) = &read[11] else {
        panic!("{read:#?}")
    };
    let wm = resync["resync|ts=".len()..].to_owned();
    let expected = [
        Seen::Opened(key(0)),
        Seen::Ctx(None, vec![]),
        Seen::Out(
            format!("auth|ts={ts}|token={}", blank(token.len())),
            Opcode::Text,
            1,
        ),
        Seen::Wrote(WriteRes::Written),
        Seen::Ping,
        Seen::In(format!("auth|ok=1|token={}", blank(token.len())), 1),
        Seen::Nonce(100),
        Seen::Ctx(Some(arm), vec![100]),
        Seen::Out(format!("cod|rpc={}|on=1", arm.0), Opcode::Text, 0),
        Seen::Wrote(WriteRes::Written),
        Seen::Ctx(None, vec![]),
        Seen::Out(format!("resync|ts={wm}"), Opcode::Text, 0),
        Seen::Wrote(WriteRes::Written),
        Seen::In(format!("item|rpc={}|i=0|res=ok", arm.0), 0),
        Seen::In(format!("rsbegin|wm={wm}"), 0),
        Seen::In("rsend".to_owned(), 0),
        Seen::Nonce(101),
        Seen::Ctx(Some(rpc), vec![101]),
        Seen::Out(
            format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", rpc.0),
            Opcode::Text,
            0,
        ),
        Seen::Wrote(WriteRes::Written),
        Seen::In(format!("qres|rpc={}|found=0", rpc.0), 0),
        Seen::Closed(key(0)),
    ];
    assert_eq!(read, expected);

    // Each context holds the time its call was given; the toy put it in its frames.
    let ctxs: Vec<&EncodeCtx> = run
        .entries
        .iter()
        .filter_map(|e| match &e.record {
            Record::EncodeCtx { ctx, .. } => Some(ctx),
            _ => None,
        })
        .collect();
    assert_eq!(ctxs[0].wall, WallNs(ts));
    assert_eq!(ctxs[2].wall.0.to_string(), wm);

    // Every stamped record is of the epoch, at the next ingest sequence.
    let stamps = stamps(&run.entries);
    assert_eq!(stamps.len(), 6);
    for pair in stamps.windows(2) {
        assert_eq!(pair[1].ingest_seq, pair[0].ingest_seq + 1, "{stamps:?}");
    }
    assert!(stamps.iter().all(|s| s.conn == key(0)));

    // Classes: what the stream brought is Safety, as are the epoch's opening and closing, every
    // nonce, and the commands' contexts.
    let inbound = |r: &Record| matches!(r, Record::Inbound { .. });
    assert_eq!(run.classes(inbound), [TrafficClass::Safety; 5]);
    let ping = |r: &Record| matches!(r, Record::InboundControl { .. });
    assert_eq!(run.classes(ping), [TrafficClass::Normal]);
    let nonce = |r: &Record| matches!(r, Record::Nonce { .. });
    assert_eq!(run.classes(nonce), [TrafficClass::Safety; 2]);
    let encode = |r: &Record| matches!(r, Record::EncodeCtx { rpc: Some(_), .. });
    assert_eq!(run.classes(encode), [TrafficClass::Safety; 2]);
    let call = |r: &Record| matches!(r, Record::EncodeCtx { rpc: None, .. });
    assert_eq!(run.classes(call), [TrafficClass::Normal; 2]);
    let control = |r: &Record| matches!(r, Record::Control { .. });
    assert_eq!(run.classes(control), [TrafficClass::Safety; 2]);
}

/// The `auth_toy` session journals, in order: the epoch opening; `on_open`'s nonce and context
/// and the login request it asks for; the login's result, its token, cookie and echoed key
/// blanked; the arm's nonce, context, frame and write; the resync's context; the refresh
/// timer's firing, `on_timer`'s nonce and context and the second login with its result; the
/// venue's frame echoing a key (a span); the epoch closing. None of the synthetic credentials
/// is in a file; each keyed hash is. The login results are Safety, as everything the stream
/// brings, and so are the nonces `on_open` and `on_timer` reserve; the timer and those calls'
/// contexts are Normal.
#[tokio::test]
async fn the_auth_toys_order_entry_session_journals_its_logins_timer_nonces_and_contexts_with_no_credential()
 {
    let token = [secret("tokena"), secret("tokenb")];
    let cookie = secret("cookie");
    let echoed = format!("x-echo-{}", secret("name"));
    let echoed_value = secret("value");
    let in_frame = secret("frame");
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [(LOGIN, http.url("/auth")), (CALL_NONCES, "1".to_owned())];
    let config = config(venue, &server.url(), &cfg, Counting::new());
    let arm = RpcId(1);
    let first = format!("HTTP/1.1 200 OK\r\nSet-Cookie: {cookie}\r\n{echoed}: {echoed_value}");
    let bodies = [
        format!("auth|token={}|refresh=0", token[0]),
        format!("auth|token={}|refresh=3600", token[1]),
    ];
    let hello = format!("hello|key={in_frame}");
    let (live_bodies, live_hello) = (bodies.clone(), hello.clone());
    let run = journaled(
        "exec_journal_auth",
        config,
        |_, control, heard| async move {
            let mut peer = server.accept().await;
            http.request().await.answer(&first, &live_bodies[0]).await;
            assert_eq!(recv_text(&mut peer).await, format!("cod|rpc={}", arm.0));
            http.request()
                .await
                .answer("HTTP/1.1 200 OK", &live_bodies[1])
                .await;
            let authenticated = |heard: &Heard| {
                let state = ConnState::Authenticated;
                let auth = ExecEvent::Conn {
                    stream: auth_toy::EXEC_STREAM,
                    state,
                };
                heard.borrow().iter().filter(|ev| **ev == auth).count()
            };
            until(|| authenticated(&heard) == 2).await;
            peer.send(&live_hello);
            let open = ExecEvent::Conn {
                stream: auth_toy::EXEC_STREAM,
                state: ConnState::Open,
            };
            until(|| heard.borrow().contains(&open)).await;
            drop(control);
            while peer.next().await.is_some() {}
        },
    )
    .await;
    run.ended.as_ref().unwrap();

    // No credential reaches a file; each keyed hash does.
    let secrets = [
        &token[0],
        &token[1],
        &cookie,
        &echoed,
        &echoed_value,
        &in_frame,
    ];
    for leaked in secrets {
        assert!(
            !contains(&run.files, leaked.as_bytes()),
            "{leaked} was written"
        );
    }
    for hash in secrets.map(|s| hmac(s.as_bytes())) {
        assert!(contains(&run.files, &hash.0));
    }

    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    let tag = auth_toy::AUTH_TAG;
    let expected = [
        Seen::Opened(key(0)),
        Seen::Nonce(100),
        Seen::Ctx(None, vec![100]),
        Seen::Ask(tag, None),
        Seen::Answer(tag, 200),
        Seen::Nonce(101),
        Seen::Ctx(Some(arm), vec![101]),
        Seen::Out(format!("cod|rpc={}", arm.0), Opcode::Text, 0),
        Seen::Wrote(WriteRes::Written),
        Seen::Ctx(None, vec![]),
        Seen::Timer(auth_toy::REFRESH_TAG),
        Seen::Nonce(102),
        Seen::Ctx(None, vec![102]),
        Seen::Ask(tag, None),
        Seen::Answer(tag, 200),
        Seen::In(format!("hello|key={}", blank(in_frame.len())), 1),
        Seen::Closed(key(0)),
    ];
    assert_eq!(read, expected);

    // The login results read back with what the codec named blanked, the rest verbatim.
    let results: Vec<&Entry> = run
        .entries
        .iter()
        .filter(|e| matches!(e.record, Record::HttpResult { .. }))
        .collect();
    let Record::HttpResult {
        result: Ok(resp), ..
    } = &results[0].record
    else {
        unreachable!()
    };
    let body = bodies[0].replace(&token[0], &blank(token[0].len()));
    assert_eq!(resp.body.0, body.as_bytes());
    assert_eq!(
        resp.headers[..2],
        [
            HeaderRec {
                name: "set-cookie".into(),
                value: blank(cookie.len()),
                redact: true,
                redact_name: false,
            },
            HeaderRec {
                name: blank(echoed.len()),
                value: blank(echoed_value.len()),
                redact: true,
                redact_name: true,
            },
        ]
    );
    // Every stamped record is of the epoch, at the next ingest sequence.
    let stamps = stamps(&run.entries);
    assert_eq!(stamps.len(), 4);
    for pair in stamps.windows(2) {
        assert_eq!(pair[1].ingest_seq, pair[0].ingest_seq + 1, "{stamps:?}");
    }
    let answers = |r: &Record| matches!(r, Record::HttpResult { .. });
    assert_eq!(run.classes(answers), [TrafficClass::Safety; 2]);
    let timer = |r: &Record| matches!(r, Record::Timer { .. });
    assert_eq!(run.classes(timer), [TrafficClass::Normal]);
    let call = |r: &Record| matches!(r, Record::EncodeCtx { rpc: None, .. });
    assert_eq!(run.classes(call), [TrafficClass::Normal; 3]);
    // Reviewer B RB-2pr-4 on PR #115: a call's nonces are Safety, so a restart reads them all.
    let nonce = |r: &Record| matches!(r, Record::Nonce { .. });
    assert_eq!(run.classes(nonce), [TrafficClass::Safety; 3]);
}

/// Reviewer B RB-2pr-10 on PR #115: an adding order, a post-only buy fbc-oms built and
/// authorized (`armed_oms`), splits its records across the classes. Its nonce is Safety, as every
/// nonce is, so a `Degraded` span keeps it; its context goes under its command's class, Normal,
/// as its frame does; the arm's nonce and context are both Safety.
#[tokio::test]
async fn an_adding_orders_nonce_is_journaled_under_safety_and_its_context_under_normal() {
    let mut server = ScriptedWs::start().await;
    let reserved = armed_oms::Reserved::default();
    let mut config = armed_oms::session_config(&server.url(), &reserved);
    config.nonce_source = SOURCE;
    let place = armed_oms::Oms::armed().place();
    let placed = Rc::new(std::cell::Cell::new(None));
    let keep = Rc::clone(&placed);
    let run = journaled(
        "exec_journal_adding_order",
        config,
        |orders, control, heard| async move {
            let mut peer = server.accept().await;
            assert!(recv_text(&mut peer).await.starts_with("auth|ts="));
            peer.send(AUTH_ACK);
            assert_eq!(recv_text(&mut peer).await, "cod|rpc=1|on=1");
            let wm = armed_oms::watermark(&recv_text(&mut peer).await);
            peer.send("item|rpc=1|i=0|res=ok");
            peer.send_all([format!("rsbegin|wm={wm}"), "rsend".to_owned()]);
            until(|| is_resync_end(&heard)).await;
            assert!(orders.may_place());
            let rpc = orders.submit(place).unwrap();
            keep.set(Some(rpc));
            let written = recv_text(&mut peer).await;
            let prefix = format!("place|rpc={}|", rpc.0);
            assert!(written.starts_with(&prefix), "{written}");
            drop(control);
            while peer.next().await.is_some() {}
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    let rpc = placed.get().unwrap();
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1]]);

    // The arm's and the place's nonces, in order, both Safety.
    let nonce = |r: &Record| matches!(r, Record::Nonce { .. });
    let offered = run.offered.iter().map(|(_, r)| r);
    let nonces: Vec<Seen> = offered.filter(|r| nonce(r)).map(seen).collect();
    assert_eq!(nonces, [Seen::Nonce(0), Seen::Nonce(1)]);
    assert_eq!(run.classes(nonce), [TrafficClass::Safety; 2]);
    // The arm's context is Safety; the place's, carrying its nonce, is Normal.
    let arm = |r: &Record| {
        matches!(
            r,
            Record::EncodeCtx {
                rpc: Some(RpcId(1)),
                ..
            }
        )
    };
    assert_eq!(run.classes(arm), [TrafficClass::Safety]);
    let adding = |r: &Record| matches!(r, Record::EncodeCtx { rpc: Some(id), .. } if *id == rpc);
    assert_eq!(run.classes(adding), [TrafficClass::Normal]);
    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    assert!(read.contains(&Seen::Ctx(Some(rpc), vec![1])), "{read:#?}");
}

// ---------------------------------------------------------------------------------------------
// Error paths.
// ---------------------------------------------------------------------------------------------

/// A nonce source that reserves fewer nonces than `on_open` asks for has the ones it did
/// reserve journaled, each spent, and no context, since no call was made; the session ends with
/// the error, having sent nothing.
#[tokio::test]
async fn a_short_reservation_is_journaled_value_by_value_with_no_context() {
    let mut server = ScriptedWs::start().await;
    let http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [(LOGIN, http.url("/auth")), (CALL_NONCES, "3".to_owned())];
    let short = Counting {
        short: true,
        ..Counting::new()
    };
    let config = config(venue, &server.url(), &cfg, short);
    let run = journaled("exec_journal_short", config, |_, control, _| async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.next().await, None);
        drop(control);
    })
    .await;
    let short = ExecSessionError::Nonces {
        asked: 3,
        reserved: 2,
    };
    assert_eq!(run.ended, Err(short));
    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    let expected = [
        Seen::Opened(key(0)),
        Seen::Nonce(100),
        Seen::Nonce(101),
        Seen::Closed(key(0)),
    ];
    assert_eq!(read, expected);
    assert_eq!(http.connections(), 0);
}

/// A run dropped while its epoch is connected leaves the epoch to be ended when the session
/// drops: it is journaled closed then, as the handler is told.
#[tokio::test]
async fn an_epoch_a_dropped_run_left_connected_is_journaled_closed_when_the_session_drops() {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let config = config(venue, &server.url(), &[], Counting::new());
    let root = fresh_dir("exec_journal_dropped_run");
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, redaction_key()).unwrap())
        .unwrap();
    let queue = Rc::new(RefCell::new(queue));
    let ended = Rc::new(RefCell::new(Vec::new()));
    struct Ends(Rc<RefCell<Vec<ConnKey>>>);
    impl fbc_runtime::ExecHandler for Ends {
        fn on_exec(&mut self, _: Envelope<ExecEvent>) {}
        fn on_epoch_end(&mut self, key: ConnKey) {
            self.0.borrow_mut().push(key);
        }
    }
    let (mut session, _control) = ExecSession::new(config, Ends(Rc::clone(&ended))).unwrap();
    session.set_journal(Journal::new(queue));
    {
        let run = session.run();
        tokio::pin!(run);
        let accepted = async {
            let mut peer = server.accept().await;
            recv_text(&mut peer).await;
            peer
        };
        let _peer = tokio::select! {
            _ = &mut run => panic!("the run ended"),
            peer = accepted => peer,
        };
    }
    assert!(ended.borrow().is_empty());
    drop(session);
    assert_eq!(*ended.borrow(), [key(0)]);
    writer.close().unwrap();
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    fs::remove_dir_all(&root).unwrap();
    let last = entries.last().map(|e| seen(&e.record));
    assert_eq!(last, Some(Seen::Closed(key(0))));
    assert_eq!(seen(&entries[0].record), Seen::Opened(key(0)));
}

/// Codex P1 on PR #115: a frame waiting as the control drops is journaled, reaching no codec,
/// after the epoch's `Closed`, so replay, which feeds a closed epoch nothing, feeds it to no
/// codec either, as the market-data session journals it. The frame is in the session's socket
/// before the session runs again: the session's run is held unpolled while the server writes
/// it and the control drops.
#[tokio::test]
async fn a_frame_waiting_as_the_control_drops_is_journaled_after_the_epoch_closed() {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let config = config(venue, &server.url(), &[], Counting::new());
    let root = fresh_dir("exec_journal_stop_frame");
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, redaction_key()).unwrap())
        .unwrap();
    let heard = Heard::default();
    let keep = Rc::clone(&heard);
    let handler = move |env: Envelope<ExecEvent>| keep.borrow_mut().push(env.body);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(queue))));
    let late = "qres|rpc=99|found=0";
    let held = Rc::new(std::cell::Cell::new(false));
    let ended = {
        let run = session.run();
        tokio::pin!(run);
        let hold = Rc::clone(&held);
        // The run, not polled while `held` is set; `tokio::join!` polls it again on the task's
        // next wake once it is cleared.
        let gated = std::future::poll_fn(move |cx| {
            if hold.get() {
                return std::task::Poll::Pending;
            }
            run.as_mut().poll(cx)
        });
        let script = async move {
            let mut peer = server.accept().await;
            assert!(recv_text(&mut peer).await.starts_with("auth|ts="));
            held.set(true);
            peer.send(late);
            tokio::time::sleep(ms(100)).await;
            drop(control);
            held.set(false);
            while peer.next().await.is_some() {}
        };
        let (ended, ()) = tokio::join!(gated, script);
        ended
    };
    ended.unwrap();
    assert!(heard.borrow().is_empty());
    drop(session);
    writer.close().unwrap();
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    fs::remove_dir_all(&root).unwrap();
    let read: Vec<Seen> = entries.iter().map(|e| seen(&e.record)).collect();
    let tail = [Seen::Closed(key(0)), Seen::In(late.to_owned(), 0)];
    assert!(read.ends_with(&tail), "{read:#?}");
    let closed = |s: &&Seen| matches!(s, Seen::Closed(_));
    assert_eq!(read.iter().filter(closed).count(), 1, "{read:#?}");
}

/// Codex P2 on PR #115: an encode's context, and its nonces' filing time, come from the clock
/// once the source has reserved them, not before: a source that takes its time (persisting its
/// nonces) leaves the encode a time no earlier than its return.
#[tokio::test]
async fn an_encodes_context_is_timed_after_its_nonces_are_reserved() {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let slow = Counting {
        delay: ms(30),
        ..Counting::new()
    };
    let returned = Arc::clone(&slow.returned);
    let config = config(venue, &server.url(), &[], slow);
    let run = journaled(
        "exec_journal_slow_source",
        config,
        |_, control, _| async move {
            let mut peer = server.accept().await;
            assert!(recv_text(&mut peer).await.starts_with("auth|ts="));
            peer.send(AUTH_ACK);
            assert!(recv_text(&mut peer).await.starts_with("cod|rpc="));
            drop(control);
            while peer.next().await.is_some() {}
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    let returned = returned.lock().unwrap().clone();
    let arm = run.entries.iter().find_map(|e| match &e.record {
        Record::EncodeCtx { rpc: Some(_), ctx } => Some(ctx.wall),
        _ => None,
    });
    assert_eq!(returned.len(), 1);
    assert!(arm.unwrap() >= returned[0], "{arm:?} before {returned:?}");
}

/// Codex P2 on PR #115: a codec whose spans do not fit what it named them in is counted, as on a
/// market-data session, and that frame is journaled with all of it hashed.
#[tokio::test]
async fn spans_that_do_not_fit_a_frame_are_counted_and_the_frame_is_hashed_whole() {
    let in_frame = secret("defect");
    let mut server = ScriptedWs::start().await;
    let http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [
        (LOGIN, http.url("/auth")),
        (CALL_NONCES, "0".to_owned()),
        (BAD_SPANS, "1".to_owned()),
    ];
    let config = config(venue, &server.url(), &cfg, Counting::new());
    let root = fresh_dir("exec_journal_bad_spans");
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, redaction_key()).unwrap())
        .unwrap();
    let heard = Heard::default();
    let keep = Rc::clone(&heard);
    let handler = move |env: Envelope<ExecEvent>| keep.borrow_mut().push(env.body);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(queue))));
    let hello = format!("hello|key={in_frame}");
    let script = async move {
        let mut peer = server.accept().await;
        peer.send(&hello);
        let open = ExecEvent::Conn {
            stream: auth_toy::EXEC_STREAM,
            state: ConnState::Open,
        };
        until(|| heard.borrow().contains(&open)).await;
        drop(control);
        while peer.next().await.is_some() {}
    };
    let (ended, ()) = tokio::join!(session.run(), script);
    ended.unwrap();
    assert_eq!(session.counters().refused_redactions, 1);
    drop(session);
    writer.close().unwrap();
    let files = written_bytes(&root);
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    fs::remove_dir_all(&root).unwrap();
    assert!(!contains(&files, in_frame.as_bytes()));
    let frame = entries.iter().find_map(|e| match &e.record {
        Record::Inbound { bytes, .. } => Some(bytes.0.clone()),
        _ => None,
    });
    let whole = format!("hello|key={in_frame}").len();
    assert_eq!(frame.unwrap(), blank(whole).into_bytes());
}

/// What a journal under `root` holds, read back once its writer is closed.
fn read_back(root: &Path) -> Vec<Seen> {
    let entries: Vec<Entry> = JournalReader::open(root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    entries.iter().map(|e| seen(&e.record)).collect()
}

/// A journal queue writing under a fresh `root`, and its writer.
fn queue_at(root: &Path) -> (QueueSink, fbc_journal::WriterThread) {
    let sink = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (queue, drain) = journal_queue(sink, redaction_key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(root, SHARD, redaction_key()).unwrap())
        .unwrap();
    (queue, writer)
}

/// Codex P2 r4213946637 on PR #115: a session dropped as a panic unwinds through it (its
/// handler panicked mid-epoch) still journals the epoch closed, best effort, though its handler
/// is not called.
#[test]
fn an_epoch_a_panic_unwinds_through_is_journaled_closed() {
    let root = fresh_dir("exec_journal_panic");
    let (queue, writer) = queue_at(&root);
    let queue = Rc::new(RefCell::new(queue));
    let journal = Journal::new(Rc::clone(&queue));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            let mut server = ScriptedWs::start().await;
            let venue = Venue::leak(conformance_toy);
            let config = config(venue, &server.url(), &[], Counting::new());
            let handler = |_: Envelope<ExecEvent>| panic!("the handler fails");
            let (mut session, _control) = ExecSession::new(config, handler).unwrap();
            session.set_journal(journal);
            let script = async move {
                let mut peer = server.accept().await;
                recv_text(&mut peer).await;
                peer.send(AUTH_ACK);
                std::future::pending::<()>().await;
            };
            let _ = tokio::join!(session.run(), script);
        })
    }));
    assert!(unwound.is_err());
    drop(rt);
    drop(queue);
    writer.close().unwrap();
    let read = read_back(&root);
    fs::remove_dir_all(&root).unwrap();
    assert_eq!(read.first(), Some(&Seen::Opened(key(0))));
    assert_eq!(read.last(), Some(&Seen::Closed(key(0))), "{read:#?}");
}

/// Codex P2 r4213946641 on PR #115: a journal set while a dropped run's epoch is still open
/// ends that epoch first, so its `Closed` goes to the journal its `Opened` is in, and the new
/// journal gets nothing of it.
#[tokio::test]
async fn a_journal_set_after_a_dropped_run_gets_none_of_its_epoch() {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let config = config(venue, &server.url(), &[], Counting::new());
    let (first, second) = (
        fresh_dir("exec_journal_first"),
        fresh_dir("exec_journal_second"),
    );
    let (queue, writer) = queue_at(&first);
    let (next, next_writer) = queue_at(&second);
    let ended = Rc::new(RefCell::new(Vec::new()));
    struct Ends(Rc<RefCell<Vec<ConnKey>>>);
    impl fbc_runtime::ExecHandler for Ends {
        fn on_exec(&mut self, _: Envelope<ExecEvent>) {}
        fn on_epoch_end(&mut self, key: ConnKey) {
            self.0.borrow_mut().push(key);
        }
    }
    let (mut session, _control) = ExecSession::new(config, Ends(Rc::clone(&ended))).unwrap();
    session.set_journal(Journal::new(Rc::new(RefCell::new(queue))));
    {
        let run = session.run();
        tokio::pin!(run);
        let accepted = async {
            let mut peer = server.accept().await;
            recv_text(&mut peer).await;
            peer
        };
        let _peer = tokio::select! {
            _ = &mut run => panic!("the run ended"),
            peer = accepted => peer,
        };
    }
    session.set_journal(Journal::new(Rc::new(RefCell::new(next))));
    assert_eq!(*ended.borrow(), [key(0)]);
    drop(session);
    assert_eq!(*ended.borrow(), [key(0)]);
    writer.close().unwrap();
    next_writer.close().unwrap();
    let (read, read_next) = (read_back(&first), read_back(&second));
    fs::remove_dir_all(&first).unwrap();
    fs::remove_dir_all(&second).unwrap();
    assert_eq!(read.first(), Some(&Seen::Opened(key(0))));
    assert_eq!(read.last(), Some(&Seen::Closed(key(0))), "{read:#?}");
    assert!(read_next.is_empty(), "{read_next:#?}");
}

/// Codex P2 r4214053432 on PR #115: the nonces `on_open` asks for, and its context, are filed
/// under the time the source returned them at, so a source that takes its time across midnight
/// files them under the new day; and the context's own time is read then too, as an encode's
/// is, so the codec signs no stale time (Reviewer B RB-2pr-3).
#[tokio::test]
async fn a_calls_nonces_are_filed_after_they_are_reserved() {
    let mut server = ScriptedWs::start().await;
    let http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [(LOGIN, http.url("/auth")), (CALL_NONCES, "1".to_owned())];
    let slow = Counting {
        delay: ms(30),
        ..Counting::new()
    };
    let returned = Arc::clone(&slow.returned);
    let config = config(venue, &server.url(), &cfg, slow);
    let run = journaled(
        "exec_journal_slow_call",
        config,
        |_, control, _| async move {
            let _peer = server.accept().await;
            until(|| http.connections() == 1).await;
            drop(control);
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    let returned = returned.lock().unwrap()[0];
    let at = |pick: fn(&Record) -> bool| {
        let i = run.offered.iter().position(|(_, r)| pick(r)).unwrap();
        (run.filed[i], run.offered[i].1.clone())
    };
    let (nonce_filed, _) = at(|r| matches!(r, Record::Nonce { .. }));
    let (ctx_filed, ctx) = at(|r| matches!(r, Record::EncodeCtx { .. }));
    assert!(
        nonce_filed >= returned,
        "{nonce_filed:?} before {returned:?}"
    );
    assert!(ctx_filed >= returned, "{ctx_filed:?} before {returned:?}");
    let Record::EncodeCtx { ctx, .. } = ctx else {
        unreachable!()
    };
    assert!(ctx.wall >= returned, "{:?} before {returned:?}", ctx.wall);
}

/// Codex P1 r4215753453 on PR #115: an HTTP result the session took while the control stood
/// reaches the codec even when the control drops before it is handed on (here as the codec
/// redacts it, as a drop on another thread may come then). The stop is read once, where the
/// input is taken, so the journal, which holds the result in the open epoch before its
/// `Closed`, never shows the codec an input it was not handed live.
#[tokio::test]
async fn a_result_taken_before_the_control_drops_reaches_the_codec_before_the_epoch_closes() {
    let token = secret("takentoken");
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [
        (LOGIN, http.url("/auth")),
        (CALL_NONCES, "0".to_owned()),
        (DROP_ON_HTTP, "1".to_owned()),
    ];
    let config = config(venue, &server.url(), &cfg, Counting::new());
    let body = format!("auth|token={token}|refresh=3600");
    DECODED.with(|n| n.set(0));
    let run = journaled(
        "exec_journal_taken_http",
        config,
        |_, control, _| async move {
            STOP.with(|stop| *stop.borrow_mut() = Some(control));
            let mut peer = server.accept().await;
            http.request().await.answer("HTTP/1.1 200 OK", &body).await;
            while peer.next().await.is_some() {}
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    assert!(STOP.with(|stop| stop.borrow().is_none()));
    assert_eq!(DECODED.with(std::cell::Cell::get), 1);
    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    let answer = read.iter().position(|s| matches!(s, Seen::Answer(..)));
    let closed = read.iter().position(|s| *s == Seen::Closed(key(0)));
    assert!(answer.unwrap() < closed.unwrap(), "{read:#?}");
    assert!(!contains(&run.files, token.as_bytes()));
}

/// Codex P2 r4214053451 on PR #115, answered by P1 r4215070431: an HTTP result that comes back
/// after its epoch ended, while the session waits to reconnect, is hashed whole, all of its body
/// and every header, as a market-data session's is, although one codec serves every epoch: that
/// codec is never handed the result, and any call it took since the request may have changed
/// what its redaction names.
#[tokio::test]
async fn an_ended_epochs_http_result_is_hashed_whole_while_the_session_waits_to_reconnect() {
    let token = secret("staletoken");
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [(LOGIN, http.url("/auth")), (CALL_NONCES, "0".to_owned())];
    let config = config(venue, &server.url(), &cfg, Counting::new());
    let body = format!("auth|token={token}|refresh=3600");
    let live_body = body.clone();
    let run = journaled_seeing(
        "exec_journal_stale_http",
        config,
        |_, control, _, offered| async move {
            let peer = server.accept().await;
            let login = http.request().await;
            peer.drop_conn();
            // The epoch ends as the drop is read; the session then waits a minute to reconnect.
            until(|| offered.closed(key(0))).await;
            let answered = login.answer("HTTP/1.1 200 OK\r\nX-Toy: yes", &live_body);
            let _ = tokio::time::timeout(ms(500), answered).await;
            until(|| offered.answers() == 1).await;
            drop(control);
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    assert!(!contains(&run.files, token.as_bytes()));
    let read: Vec<Seen> = run.entries.iter().map(|e| seen(&e.record)).collect();
    let closed = read.iter().position(|s| *s == Seen::Closed(key(0)));
    let answer = read.iter().position(|s| matches!(s, Seen::Answer(..)));
    assert!(closed < answer, "{read:#?}");
    let resp = run.entries.iter().find_map(|e| match &e.record {
        Record::HttpResult { result: Ok(r), .. } => Some(r.clone()),
        _ => None,
    });
    let resp = resp.unwrap();
    assert_eq!(resp.body.0, blank(body.len()).as_bytes());
    assert!(resp.headers.iter().all(|h| h.redact), "{:#?}", resp.headers);
}

/// Codex P1 r4214607580 on PR #115: once the one codec's `on_open` has run for a later epoch,
/// its state may no longer name what an ended epoch's result carries, so a result of an epoch
/// before the one the codec was last opened for is hashed whole.
#[tokio::test]
async fn an_ended_epochs_http_result_is_hashed_whole_once_the_codec_opened_a_later_epoch() {
    let token = secret("stalereopened");
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [(LOGIN, http.url("/auth")), (CALL_NONCES, "0".to_owned())];
    let quick = ReconnectPacing::new(ms(20), ms(20), 100, ms(60_000), ms(5_000)).unwrap();
    let config = ExecSessionConfig {
        pacing: quick,
        ..config(venue, &server.url(), &cfg, Counting::new())
    };
    let body = format!("auth|token={token}|refresh=3600");
    let live_body = body.clone();
    let run = journaled_seeing(
        "exec_journal_stale_reopened",
        config,
        |_, control, _, offered| async move {
            let first = server.accept().await;
            let login = http.request().await;
            first.drop_conn();
            // The next epoch opens and its `on_open` asks for its own login.
            let _second = server.accept().await;
            let _relogin = http.request().await;
            let answered = login.answer("HTTP/1.1 200 OK\r\nX-Toy: yes", &live_body);
            let _ = tokio::time::timeout(ms(500), answered).await;
            until(|| offered.answers() == 1).await;
            drop(control);
        },
    )
    .await;
    run.ended.as_ref().unwrap();
    assert!(!contains(&run.files, token.as_bytes()));
    let resp = run.entries.iter().find_map(|e| match &e.record {
        Record::HttpResult {
            stamp,
            result: Ok(r),
            ..
        } if stamp.conn == key(0) => Some(r.clone()),
        _ => None,
    });
    let resp = resp.expect("epoch 0's login result is journaled");
    assert_eq!(resp.body.0, blank(body.len()).as_bytes());
    assert!(resp.headers.iter().all(|h| h.redact), "{:#?}", resp.headers);
}

/// Codex P1 r4214784284 on PR #115: a request's deadline that falls due while the session waits
/// to reconnect calls the codec's `on_rpc_timeout`, which may change the state its redaction
/// reads; an ended epoch's HTTP result that comes back after that is hashed whole.
#[tokio::test]
async fn an_ended_epochs_http_result_is_hashed_whole_once_a_deadline_reached_the_codec() {
    let body = stale_after_a_deadline("exec_journal_stale_timed_out", false).await;
    assert_eq!(body.0, blank(body.1).as_bytes());
}

/// Codex P1 r4215070431 on PR #115 (Reviewer B RB-2pr-7): so is one whose epoch was still
/// connected when the deadline reached the codec, here the arm's, which fails the epoch.
#[tokio::test]
async fn an_ended_epochs_http_result_is_hashed_whole_after_a_deadline_reached_the_connected_codec()
{
    let body = stale_after_a_deadline("exec_journal_stale_timed_out_connected", true).await;
    assert_eq!(body.0, blank(body.1).as_bytes());
}

/// Runs an `auth_toy` session whose first login asks to be refreshed at once and whose arm is
/// unanswered at its 300 ms deadline: with `connected`, the deadline falls due in the epoch and
/// fails it; else the venue drops the connection first and it falls due while the session waits
/// to reconnect. The refresh is answered once the epoch is journaled closed and the deadline
/// has reached the codec. Returns the refresh result's journaled body and the length of the one
/// sent, having checked that no credential is in the journal and every header is hashed.
async fn stale_after_a_deadline(name: &str, connected: bool) -> (Vec<u8>, usize) {
    let token = secret("staletimedout");
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = Venue::leak(auth_toy);
    let cfg = [
        (LOGIN, http.url("/auth")),
        (CALL_NONCES, "0".to_owned()),
        (ARM_TIMEOUT_MS, "300".to_owned()),
    ];
    let config = config(venue, &server.url(), &cfg, Counting::new());
    let body = format!("auth|token={token}|refresh=3600");
    let live_body = body.clone();
    let run = journaled_seeing(name, config, |_, control, heard, offered| async move {
        let mut peer = server.accept().await;
        // A login that asks to be refreshed at once: its refresh waits unanswered.
        let first = "auth|token=tokenfirst|refresh=0";
        http.request().await.answer("HTTP/1.1 200 OK", first).await;
        assert_eq!(recv_text(&mut peer).await, "cod|rpc=1");
        let refresh = http.request().await;
        let unknown = ExecEvent::Outcome {
            rpc: RpcId(1),
            item: None,
            outcome: SubmitOutcome::Unknown,
        };
        if connected {
            until(|| heard.borrow().contains(&unknown)).await;
            until(|| offered.closed(key(0))).await;
        } else {
            peer.drop_conn();
            until(|| offered.closed(key(0))).await;
            until(|| heard.borrow().contains(&unknown)).await;
        }
        let answered = refresh.answer("HTTP/1.1 200 OK\r\nX-Toy: yes", &live_body);
        let _ = tokio::time::timeout(ms(500), answered).await;
        until(|| offered.answers() == 2).await;
        drop(control);
    })
    .await;
    run.ended.as_ref().unwrap();
    assert!(!contains(&run.files, token.as_bytes()));
    // The last result journaled is the refresh's, stamped under the ended epoch.
    let resp = run.entries.iter().rev().find_map(|e| match &e.record {
        Record::HttpResult {
            stamp,
            result: Ok(r),
            ..
        } => Some((stamp.conn, r.clone())),
        _ => None,
    });
    let (conn, resp) = resp.expect("the refresh's result is journaled");
    assert_eq!(conn, key(0));
    assert!(resp.headers.iter().all(|h| h.redact), "{:#?}", resp.headers);
    (resp.body.0, body.len())
}

/// Codex P2 r4214243083 on PR #115 (Reviewer A, Reviewer B RB-2pr-1): an epoch journaled closed
/// as the control drops stays closed once, when the codec panics redacting the frame waiting
/// then and the session is dropped as the panic unwinds.
#[test]
fn an_epoch_closed_at_a_stop_is_journaled_closed_once_when_redacting_a_waiting_frame_panics() {
    let root = fresh_dir("exec_journal_stop_panic");
    let (queue, writer) = queue_at(&root);
    let queue = Rc::new(RefCell::new(queue));
    let journal = Journal::new(Rc::clone(&queue));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let boom = "boom";
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            let mut server = ScriptedWs::start().await;
            let http = ScriptedHttp::start().await;
            let venue = Venue::leak(auth_toy);
            let cfg = [
                (LOGIN, http.url("/auth")),
                (CALL_NONCES, "0".to_owned()),
                (PANIC_ON, boom.to_owned()),
            ];
            let config = config(venue, &server.url(), &cfg, Counting::new());
            let heard = Heard::default();
            let keep = Rc::clone(&heard);
            let handler = move |env: Envelope<ExecEvent>| keep.borrow_mut().push(env.body);
            let (mut session, control) = ExecSession::new(config, handler).unwrap();
            session.set_journal(journal);
            let held = Rc::new(std::cell::Cell::new(false));
            let run = session.run();
            tokio::pin!(run);
            let hold = Rc::clone(&held);
            // The run, not polled while `held` is set, so the frame waits as the control drops.
            let gated = std::future::poll_fn(move |cx| {
                if hold.get() {
                    return std::task::Poll::Pending;
                }
                run.as_mut().poll(cx)
            });
            let script = async move {
                let mut peer = server.accept().await;
                peer.send("hello|key=k");
                let open = ExecEvent::Conn {
                    stream: auth_toy::EXEC_STREAM,
                    state: ConnState::Open,
                };
                until(|| heard.borrow().contains(&open)).await;
                held.set(true);
                peer.send(boom);
                tokio::time::sleep(ms(100)).await;
                drop(control);
                held.set(false);
                while peer.next().await.is_some() {}
            };
            let _ = tokio::join!(gated, script);
        })
    }));
    let why = unwound.unwrap_err();
    assert_eq!(
        why.downcast_ref::<&str>(),
        Some(&"the codec fails redacting")
    );
    drop(rt);
    drop(queue);
    writer.close().unwrap();
    let read = read_back(&root);
    fs::remove_dir_all(&root).unwrap();
    assert_eq!(read.first(), Some(&Seen::Opened(key(0))));
    assert_eq!(read.last(), Some(&Seen::Closed(key(0))), "{read:#?}");
    let closed = |s: &&Seen| matches!(s, Seen::Closed(_));
    assert_eq!(read.iter().filter(closed).count(), 1, "{read:#?}");
}
