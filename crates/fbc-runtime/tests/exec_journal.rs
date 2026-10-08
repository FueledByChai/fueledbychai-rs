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
//! fills 0006 reserves room for; an encode's nonces and context under its command's class; the
//! rest under Normal. Error paths: a nonce source that reserves fewer than asked has what it did
//! reserve journaled and no context, and a run dropped mid-epoch has its epoch journaled closed
//! when the session drops.
//!
//! Every credential of the `auth_toy` session is synthetic and assembled at run time; the
//! conformance toy's token is its own synthetic constant. The expected hashes are computed with
//! `hmac` and `sha2` directly, not through the journal's own code.

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
    Subscription, SymbolError, TimerTag, TrafficClass, VenueCaps, VenueCommand, VenueConfig,
    VenueError, VenueFactory, WallNs, WireSlice, WireUrl,
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
/// The account the sessions trade: the journal's nonce source is numbered by it.
const ACCT: AccountKey = AccountKey::new(9);
const SOURCE: NonceSourceId = NonceSourceId(9);
/// The order-entry endpoint's URL; required.
const URL: &str = "exec.url";
/// The `auth_toy` login's URL, the scripted HTTP server's.
const LOGIN: &str = "login.url";
/// How many nonces the `auth_toy` session's `on_open` and `on_timer` ask for.
const CALL_NONCES: &str = "call.nonces";
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

/// The journal queue's sink, keeping each record it is offered with its class.
struct Tee {
    queue: QueueSink,
    offered: Vec<(TrafficClass, Record)>,
}

impl JournalSink for Tee {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.offered.push((class, record.clone()));
        self.queue.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        self.offered.push((class, record.to_record()));
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

/// Runs a session built from `config` with a journal, while `play` drives the venue with the
/// session's orders, its control and what its handler hears, and reads back what it journaled.
async fn journaled<F, Fut>(name: &str, config: ExecSessionConfig, play: F) -> Run
where
    F: FnOnce(ExecOrders, ExecControl, Heard) -> Fut,
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
    }));
    let heard = Heard::default();
    let keep = Rc::clone(&heard);
    let handler = move |env: Envelope<ExecEvent>| keep.borrow_mut().push(env.body);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    session.set_journal(Journal::new(tee.clone()));
    let orders = session.orders();
    let (ended, ()) = tokio::join!(session.run(), play(orders, control, heard));
    drop(session);
    writer.close().unwrap();
    let entries: Vec<Entry> = JournalReader::open(&root, SHARD)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap();
    let files = written_bytes(&root);
    fs::remove_dir_all(&root).unwrap();
    let offered = Rc::try_unwrap(tee).ok().unwrap().into_inner().offered;
    Run {
        entries,
        files,
        offered,
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
    })
}

/// fbc-core's `auth_toy` as an order-entry session takes it: its login sent to the scripted
/// HTTP server, `call_nonces` nonces asked for its `on_open` and `on_timer`, and the session's
/// cancel-on-disconnect arm encoded as a `cod|rpc=<id>` frame (the toy encodes no command),
/// answered by nothing within the test.
struct AuthWrap {
    login: String,
    call_nonces: u16,
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
                timeout: Duration::from_secs(3_600),
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
        auth_toy::AuthToy.redact_inbound(input)
    }
}

/// Nonces counted up from 100; with `short`, one fewer than asked.
struct Counting {
    next: u64,
    short: bool,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let len = len - u16::from(self.short);
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        block
    }
}

/// A session of `venue` at `url`, its venue configured with `cfg`, trading [`ACCT`], reconnecting
/// only after a minute.
fn config(
    venue: &'static Venue,
    url: &str,
    cfg: &[(&'static str, String)],
    short: bool,
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
        nonces: Box::new(Counting { next: 100, short }),
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
/// stream brought is Safety; the ping, the connection changes and the contexts of `on_open` and
/// the resync are Normal; the arm's and the query's nonces and contexts are their commands'
/// class, Safety.
#[tokio::test]
async fn the_conformance_toys_order_entry_session_journals_what_crosses_it_in_order_with_no_credential()
 {
    let mut server = ScriptedWs::start().await;
    let venue = Venue::leak(conformance_toy);
    let config = config(venue, &server.url(), &[], false);
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

    // Classes: what the stream brought is Safety, as are the commands' nonces and contexts.
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
    assert_eq!(run.classes(control), [TrafficClass::Normal; 2]);
}

/// The `auth_toy` session journals, in order: the epoch opening; `on_open`'s nonce and context
/// and the login request it asks for; the login's result, its token, cookie and echoed key
/// blanked; the arm's nonce, context, frame and write; the resync's context; the refresh
/// timer's firing, `on_timer`'s nonce and context and the second login with its result; the
/// venue's frame echoing a key (a span); the epoch closing. None of the synthetic credentials
/// is in a file; each keyed hash is. The login results are Safety, as everything the stream
/// brings; the timer is Normal.
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
    let config = config(venue, &server.url(), &cfg, false);
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
    let config = config(venue, &server.url(), &cfg, true);
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
    let config = config(venue, &server.url(), &[], false);
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
