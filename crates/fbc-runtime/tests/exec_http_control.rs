//! FBC-m8vm's done line (decision 0081, amending 0057's WebSocket-only rule for control
//! commands): an order query (`ControlCommand::Query`) or a fee query (`FeeQuery`) whose encode
//! carries its request as one HTTP request is admitted. It is requested once, its request and
//! the connection it opens charged with the encode as normal traffic (0073), so buckets that
//! refuse them leave it `NotSent(RateBudget)` with nothing requested; its result reaches the
//! codec's `on_http` on the epoch that asked, which clears its deadline whatever the codec
//! pushes; and one whose result its epoch's end drops, or that never comes back while the
//! session waits to reconnect, is handed to `on_rpc_timeout` once at its deadline and never
//! requested again. An order-affecting command over HTTP, and a consumer's cancel-on-disconnect
//! arm, stay `NotSent(Unencodable)`, nothing requested, until FBC-4nfb.
//!
//! The venue is the conformance toy's order entry behind a codec that carries a query, a fee
//! query (and, built so, an order command or a consumer's arm) as one `POST /orders` to a
//! scripted HTTP server, the toy's frame as its body, and reads the answer's body as the toy's
//! frame. The session's own arm and resync stay frames: the tests have the venue accept the arm
//! and answer the resync first ([`ready`]).

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use armed_oms::{ACCT, Oms, churn, held_caps, settle};
use common::{Peer, ScriptedHttp, ScriptedWs};
use exec_toy::{EXEC_STREAM, OWN_NS, RPC_TIMEOUT, ToyExec, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConnKey, CtxCall, DecodeError, DecodeScope, Effect,
    Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec, ExecEndpoint, ExecEvent,
    ExecSink, FieldSpec, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentSpecDraft, LimitScope, MdCodec, NonceBlock, NonceSource,
    NotSentReason, OpKind, PathStamps, RateLimit, RawFrame, RpcId, Secrets, SpecTable, StreamId,
    SubmitHandle, SubmitOutcome, Subscription, SymbolError, TagSet, TimerTag, VenueCaps,
    VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta, WireUrl,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecHandler, ExecSession, ExecSessionConfig, IngestClock, Input, ProxyConfig,
    RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, WriteStall,
};
use tokio::time::{Instant, advance};

// ---------------------------------------------------------------------------------------------
// The venue: the toy's order entry, its queries carried over HTTP.
// ---------------------------------------------------------------------------------------------

/// The order-entry endpoint's URL; required.
const URL: &str = "rest.ws";
/// The HTTP server's base URL; required.
const REST: &str = "rest.base";

/// What the codec was asked, by the session or the venue: encode a request at this instant,
/// hand over an HTTP result (its status, or why none came), or time a request out at this
/// instant.
#[derive(Clone, Debug, PartialEq)]
enum Call {
    Encode {
        rpc: RpcId,
        at: Instant,
    },
    Http {
        rpc: RpcId,
        result: Result<u16, HttpFailure>,
    },
    Timeout {
        rpc: RpcId,
        at: Instant,
    },
}

type Calls = Arc<Mutex<Vec<Call>>>;

/// The toy, its order queries and fee queries carried over HTTP; with `orders`, its order
/// commands and a consumer's arm too; declaring `limits` in place of its own when given.
struct RestToy {
    calls: Calls,
    limits: Option<Vec<RateLimit>>,
    orders: bool,
}

impl RestToy {
    fn leak(limits: Option<Vec<RateLimit>>, orders: bool) -> &'static RestToy {
        Box::leak(Box::new(RestToy {
            calls: Calls::default(),
            limits,
            orders,
        }))
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    /// The requests the codec timed out, with when.
    fn timeouts(&self) -> Vec<(RpcId, Instant)> {
        let timeout = |call: &Call| match call {
            Call::Timeout { rpc, at } => Some((*rpc, *at)),
            _ => None,
        };
        self.calls().iter().filter_map(timeout).collect()
    }

    /// The HTTP results the codec was handed, by request.
    fn results(&self) -> Vec<(RpcId, Result<u16, HttpFailure>)> {
        let result = |call: &Call| match call {
            Call::Http { rpc, result } => Some((*rpc, *result)),
            _ => None,
        };
        self.calls().iter().filter_map(result).collect()
    }

    /// When the codec encoded request `rpc`.
    fn encoded_at(&self, rpc: RpcId) -> Instant {
        let at = |call: &Call| match call {
            Call::Encode { rpc: r, at } if *r == rpc => Some(*at),
            _ => None,
        };
        self.calls().iter().find_map(at).unwrap()
    }
}

impl VenueFactory for RestToy {
    fn id(&self) -> &'static str {
        "TOY-REST"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = held_caps();
        if let Some(limits) = &self.limits {
            caps.limits.clone_from(limits);
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

    /// Never built: nothing is planned for market data.
    fn md_codec(&self, _: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(exec_toy::ToyMd::new(ep.stream))
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url: WireUrl::plain(cfg.get(URL).unwrap()),
        }])
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok(Box::new(Rest {
            inner: ToyExec::new(Box::new(ToySigner)),
            url: format!("{}/orders", cfg.get(REST).unwrap()),
            orders: self.orders,
            calls: Arc::clone(&self.calls),
            asked: HashMap::new(),
            resynced: false,
        })))
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

/// The toy's codec, an encode it carries over HTTP turned from its one frame into one
/// `POST /orders` with that frame as its body, the request's tag its id. Its HTTP results are
/// read as the toy reads frames, and a failure, or an answer it cannot read, reports the
/// request `Unknown` (`ExecCodec::on_http`'s contract). Each encode but the session's own
/// arm, each HTTP result and each timeout is logged.
struct Rest {
    inner: ToyExec,
    url: String,
    orders: bool,
    calls: Calls,
    /// The requests carried over HTTP and not yet answered, by tag.
    asked: HashMap<HttpTag, (RpcId, VenueCommand)>,
    /// Whether the epoch's resync was asked: the session's own arm comes before it, so an arm
    /// encoded after it is a consumer's.
    resynced: bool,
}

impl Rest {
    /// Whether `cmd` goes over HTTP.
    fn over_http(&self, cmd: &VenueCommand) -> bool {
        match cmd {
            VenueCommand::Query(_) | VenueCommand::FeeQuery => true,
            VenueCommand::ArmCancelOnDisconnect(_) => self.resynced,
            _ => self.orders,
        }
    }
}

impl ExecCodec for Rest {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.resynced = false;
        self.inner.on_open(stream, ctx, fx);
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
        let own_arm = matches!(cmd, VenueCommand::ArmCancelOnDisconnect(_)) && !self.resynced;
        if !own_arm {
            let at = Instant::now();
            self.calls.lock().unwrap().push(Call::Encode { rpc, at });
        }
        let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, fx)?;
        if !self.over_http(cmd) {
            return Ok(receipt);
        }
        for effect in fx.take() {
            fx.push(match effect {
                Effect::Send {
                    frame,
                    rpc: Some(call),
                    class,
                    charge,
                    ..
                } => {
                    let tag = HttpTag(call.id.0);
                    self.asked.insert(tag, (call.id, cmd.clone()));
                    Effect::Http {
                        tag,
                        req: HttpRequest {
                            method: HttpMethod::Post,
                            url: WireUrl::plain(&self.url),
                            headers: Vec::new(),
                            body: frame,
                        },
                        rpc: Some(call.id),
                        timeout: call.timeout,
                        class,
                        charge,
                    }
                }
                other => other,
            });
        }
        Ok(receipt)
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
        self.inner.on_frame(stream, f, scope, specs, sink, fx)
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
        let Some((rpc, cmd)) = self.asked.remove(&tag) else {
            return self.inner.on_http(tag, resp, scope, specs, sink, fx);
        };
        let result = resp.as_ref().map(|r| r.status).map_err(|e| *e);
        self.calls.lock().unwrap().push(Call::Http { rpc, result });
        let read = match (resp, &cmd) {
            (Ok(r), VenueCommand::FeeQuery) if r.status == 200 => {
                let rates = Vec::new();
                let rpc = Some(rpc);
                sink.push(VenueMeta::NONE, ExecEvent::FeeRates { rpc, rates });
                true
            }
            (Ok(r), _) if r.status == 200 => std::str::from_utf8(r.body).is_ok_and(|text| {
                let frame = RawFrame::Text(text);
                let inner = &mut self.inner;
                inner
                    .on_frame(EXEC_STREAM, frame, scope, specs, sink, fx)
                    .is_ok()
            }),
            _ => false,
        };
        if !read {
            let unknown = ExecEvent::Outcome {
                rpc,
                item: None,
                outcome: SubmitOutcome::Unknown,
            };
            sink.push(VenueMeta::NONE, unknown);
        }
        Ok(())
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let at = Instant::now();
        self.calls.lock().unwrap().push(Call::Timeout { rpc, at });
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.resynced = true;
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
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

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const CONN: u16 = 6;

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A drop reconnects after 10 ms.
fn quick() -> ReconnectPacing {
    ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000)).unwrap()
}

/// A drop reconnects only after 60 s, well past a request's deadline.
fn slow() -> ReconnectPacing {
    let minute = Duration::from_secs(60);
    ReconnectPacing::new(minute, minute, 100, minute * 10, ms(5_000)).unwrap()
}

/// A session of `venue` at `ws` for [`ACCT`], its HTTP requests to `rest`, its buckets keeping
/// `reserve` for safety traffic.
fn config(
    venue: &'static RestToy,
    ws: &str,
    rest: &str,
    pacing: ReconnectPacing,
    reserve: u8,
) -> ExecSessionConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, ws);
    cfg.insert(REST, rest);
    let limits = venue.caps(&cfg).unwrap().limits;
    let reserve = SafetyReserve::percent(reserve).unwrap();
    ExecSessionConfig {
        venue,
        cfg,
        creds: Secrets::new(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(Counting(0)),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: CONN,
        limiter: RateLimiter::new(&limits, reserve).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    }
}

/// What the handler heard: an event, a submission's handle, or an epoch's end.
#[derive(Debug)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    Submitted(SubmitHandle),
    End,
}

type Log = Rc<RefCell<Vec<Heard>>>;

struct Keep(Log);

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.0.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.0.borrow_mut().push(Heard::Submitted(handle));
    }

    fn on_epoch_end(&mut self, _: ConnKey) {
        self.0.borrow_mut().push(Heard::End);
    }
}

/// How many epochs the handler was told ended.
fn ends(log: &Log) -> usize {
    let log = log.borrow();
    log.iter().filter(|h| matches!(h, Heard::End)).count()
}

/// The handles the handler was given, in order.
fn handles(log: &Log) -> Vec<SubmitHandle> {
    let log = log.borrow();
    let handle = |heard: &Heard| match heard {
        Heard::Submitted(handle) => Some(handle.clone()),
        _ => None,
    };
    log.iter().filter_map(handle).collect()
}

/// The answers the handler heard (query results and fee rates), each with its request and
/// epoch.
fn answers(log: &Log) -> Vec<(RpcId, ConnKey)> {
    let log = log.borrow();
    let answer = |heard: &Heard| match heard {
        Heard::Event(env) => match &env.body {
            ExecEvent::QueryResult(_) | ExecEvent::FeeRates { .. } => {
                env.body.answers().map(|rpc| (rpc, env.stamp.conn))
            }
            _ => None,
        },
        _ => None,
    };
    log.iter().filter_map(answer).collect()
}

/// The outcomes the handler heard, but the venue's acceptances of the session's own arms:
/// request, outcome and epoch.
fn outcomes(log: &Log) -> Vec<(RpcId, SubmitOutcome, ConnKey)> {
    let log = log.borrow();
    let outcome = |heard: &Heard| match heard {
        Heard::Event(env) => match &env.body {
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Accepted { .. },
                ..
            } => None,
            ExecEvent::Outcome { rpc, outcome, .. } => {
                Some((*rpc, outcome.clone(), env.stamp.conn))
            }
            _ => None,
        },
        _ => None,
    };
    log.iter().filter_map(outcome).collect()
}

/// How many times the handler heard a resync end.
fn resync_ends(log: &Log) -> usize {
    let log = log.borrow();
    let end = |h: &&Heard| matches!(h, Heard::Event(env) if env.body == ExecEvent::ResyncEnd);
    log.iter().filter(end).count()
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// The next request the HTTP server read, the session let run until it has; it panics rather
/// than wait forever for one never made.
async fn requested(http: &mut ScriptedHttp) -> common::Exchange {
    for _ in 0..100_000 {
        if let Some(exchange) = http.try_request() {
            return exchange;
        }
        tokio::task::yield_now().await;
    }
    panic!("nothing requested");
}

/// Moves the paused clock on to `at`.
async fn advance_to(at: Instant) {
    advance(at.saturating_duration_since(Instant::now())).await;
}

/// Reads the authentication `peer` was sent and acknowledges it, reads the arm and the resync
/// the session then sends, has the venue accept the arm and answer the resync with nothing
/// open, and waits until the handler heard the resync end: the epoch takes places.
async fn ready(peer: &mut Peer, watch: &Log) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    let resynced = resync_ends(watch);
    peer.send("auth|ok=1|token=toy-session-token");
    let arm = peer.recv().await;
    let rpc = arm
        .strip_prefix("cod|rpc=")
        .and_then(|a| a.strip_suffix("|on=1"));
    let rpc: u64 = rpc.expect(&arm).parse().unwrap();
    let resync = peer.recv().await;
    let wm = resync.strip_prefix("resync|ts=").expect(&resync);
    peer.send(&format!("item|rpc={rpc}|i=0|res=ok"));
    peer.send_all([format!("rsbegin|wm={wm}"), "rsend".to_owned()]);
    settle(|| resync_ends(watch) == resynced + 1).await;
}

/// An order query for venue order `V-1`.
fn query() -> ControlCommand {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    ControlCommand::Query(fbc_core::QueryOrder {
        target: fbc_core::OrderRef::Venue(vid),
        inst: exec_toy::INST_A,
        placement_nonce: None,
    })
}

/// The venue's answer to query `rpc`: no such order.
fn not_found(rpc: RpcId) -> String {
    format!("qres|rpc={}|found=0", rpc.0)
}

const OK: &str = "HTTP/1.1 200 OK";

/// A request's handle reporting it sent.
fn sent(rpc: RpcId) -> SubmitHandle {
    SubmitHandle {
        rpc,
        receipt: Ok(EncodeReceipt::new()),
    }
}

/// A request's handle reporting it not sent, for `reason`.
fn not_sent(rpc: RpcId, reason: NotSentReason) -> SubmitHandle {
    SubmitHandle {
        rpc,
        receipt: Err(reason),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// An order query and a fee query, each carried as one HTTP request, are requested once; each
/// answer reaches the codec's `on_http` on the epoch that asked, and its event the handler, and
/// neither reaches `on_rpc_timeout`, however long the session runs on.
#[tokio::test(start_paused = true)]
async fn an_http_order_query_and_fee_query_are_requested_once_and_answered_on_the_epoch_that_asked()
{
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = RestToy::leak(None, false);
    let config = config(venue, &server.url(), &http.url(""), quick(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        let queried = orders.submit_control(query()).unwrap();
        let exchange = requested(&mut http).await;
        assert_eq!(exchange.line, "POST /orders");
        exchange.answer(OK, &not_found(queried)).await;
        settle(|| answers(&watch).len() == 1).await;
        let fees = orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let exchange = requested(&mut http).await;
        assert_eq!(exchange.line, "POST /orders");
        exchange.answer(OK, "").await;
        settle(|| answers(&watch).len() == 2).await;
        // Their deadlines, and twice them, pass: the answers cleared them.
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert_eq!(http.connections(), 2);
        assert!(peer.quiet());
        drop(control);
        (queried, fees)
    };
    let (run, (queried, fees)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    assert_eq!(handles(&log), [sent(queried), sent(fees)]);
    assert_eq!(answers(&log), [(queried, key(0)), (fees, key(0))]);
    assert_eq!(
        venue.results(),
        [(queried, Ok(200)), (fees, Ok(200))],
        "{:?}",
        venue.calls()
    );
    assert!(venue.timeouts().is_empty());
    assert!(outcomes(&log).is_empty());
    assert_eq!(session.stale(Input::Http), 0);
}

/// A query whose connection drops before its HTTP answer comes back: the answer, coming on the
/// next epoch, is dropped, never reaching the codec, and the query is handed to
/// `on_rpc_timeout` once, exactly at its deadline, its `Unknown` on the epoch then current. It is
/// never requested again.
#[tokio::test(start_paused = true)]
async fn an_http_query_whose_answer_its_epochs_end_drops_is_handed_to_on_rpc_timeout_once_at_its_deadline()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = RestToy::leak(None, false);
    let config = config(venue, &server.url(), &http.url(""), quick(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        let exchange = requested(&mut http).await;
        let due = venue.encoded_at(rpc) + RPC_TIMEOUT;
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        advance(ms(10)).await;
        let mut next = server.accept().await;
        ready(&mut next, &watch).await;
        // The answer comes back on the next epoch: dropped.
        exchange.answer(OK, &not_found(rpc)).await;
        churn().await;
        assert!(answers(&watch).is_empty());
        // Not a moment before its deadline.
        advance_to(due - ms(1)).await;
        churn().await;
        assert!(venue.timeouts().is_empty());
        advance(ms(1)).await;
        settle(|| !venue.timeouts().is_empty()).await;
        // Never again, nor requested again, however long the session runs on.
        advance(RPC_TIMEOUT * 3).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert_eq!(http.connections(), 1);
        assert!(next.quiet());
        drop(control);
        (rpc, due)
    };
    let (run, (rpc, due)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    assert_eq!(handles(&log), [sent(rpc)]);
    assert_eq!(venue.timeouts(), [(rpc, due)]);
    assert_eq!(outcomes(&log), [(rpc, SubmitOutcome::Unknown, key(1))]);
    assert!(answers(&log).is_empty());
    assert!(venue.results().is_empty(), "{:?}", venue.calls());
    assert_eq!(session.stale(Input::Http), 1);
}

/// A query whose connection drops before its HTTP answer comes back, and that has no epoch to
/// come back on before its deadline: it is handed to `on_rpc_timeout` once at its deadline,
/// while the session waits to reconnect, its `Unknown` stamped under the epoch the session waits
/// to open. The next epoch never requests it again.
#[tokio::test(start_paused = true)]
async fn an_http_query_with_no_answer_while_the_session_waits_to_reconnect_is_unknown_once_at_its_deadline()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = RestToy::leak(None, false);
    let config = config(venue, &server.url(), &http.url(""), slow(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        // Held unanswered.
        let _held = requested(&mut http).await;
        let due = venue.encoded_at(rpc) + RPC_TIMEOUT;
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        advance_to(due - ms(1)).await;
        churn().await;
        assert!(venue.timeouts().is_empty());
        advance(ms(1)).await;
        settle(|| !outcomes(&watch).is_empty()).await;
        // The pacing's minute passes and the session reconnects.
        advance(Duration::from_secs(60)).await;
        let mut next = server.accept().await;
        ready(&mut next, &watch).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert_eq!(http.connections(), 1);
        assert!(next.quiet());
        drop(control);
        (rpc, due)
    };
    let (run, (rpc, due)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.timeouts(), [(rpc, due)]);
    assert_eq!(outcomes(&log), [(rpc, SubmitOutcome::Unknown, key(1))]);
    assert!(venue.results().is_empty(), "{:?}", venue.calls());
}

/// A query whose HTTP request fails, and one whose request times out, on the epoch that asked:
/// the codec's `on_http` reports each `Unknown`, which answers no request, and the runtime hands
/// neither to `on_rpc_timeout` (Codex 4212123422 on PR #109): the call to `on_http` for its
/// request clears its deadline whatever the codec pushes. The second's deadline falls at the
/// instant its HTTP timeout does.
#[tokio::test(start_paused = true)]
async fn an_http_query_whose_on_http_reports_unknown_is_never_handed_to_on_rpc_timeout() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = RestToy::leak(None, false);
    let config = config(venue, &server.url(), &http.url(""), quick(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        // The venue closes the connection without an answer.
        let failed = orders.submit_control(query()).unwrap();
        drop(requested(&mut http).await);
        settle(|| outcomes(&watch).len() == 1).await;
        // The venue never answers: the request times out at its deadline.
        let timed_out = orders.submit_control(query()).unwrap();
        let _held = requested(&mut http).await;
        let due = venue.encoded_at(timed_out) + RPC_TIMEOUT;
        advance_to(due).await;
        settle(|| outcomes(&watch).len() == 2).await;
        advance(RPC_TIMEOUT * 3).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert!(peer.quiet());
        drop(control);
        (failed, timed_out)
    };
    let (run, (failed, timed_out)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    assert_eq!(handles(&log), [sent(failed), sent(timed_out)]);
    assert_eq!(
        outcomes(&log),
        [
            (failed, SubmitOutcome::Unknown, key(0)),
            (timed_out, SubmitOutcome::Unknown, key(0)),
        ]
    );
    let results = venue.results();
    assert!(
        matches!(results[..], [(f, Err(_)), (t, Err(HttpFailure::TimedOut))] if f == failed && t == timed_out),
        "{results:?}"
    );
    assert!(venue.timeouts().is_empty(), "{:?}", venue.calls());
}

/// An HTTP query is charged as normal traffic with its encode (decision 0073): with a
/// per-market query limit of two and half of it kept for safety traffic, the first query is
/// requested and the second, which would take the bucket into the reserve, is
/// `NotSent(RateBudget)`, nothing requested and no deadline set, although the toy labels a
/// query Safety.
#[tokio::test(start_paused = true)]
async fn an_http_query_at_the_safety_floor_is_not_sent_rate_budget_with_nothing_requested() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let mut limits = held_caps().limits;
    limits.push(RateLimit {
        scope: LimitScope::Pair,
        ops: TagSet::of(&[OpKind::Query]),
        per: Duration::from_secs(60),
        units: 2,
    });
    let venue = RestToy::leak(Some(limits), false);
    let config = config(venue, &server.url(), &http.url(""), quick(), 50);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        let first = orders.submit_control(query()).unwrap();
        requested(&mut http)
            .await
            .answer(OK, &not_found(first))
            .await;
        settle(|| answers(&watch).len() == 1).await;
        let second = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 2).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert_eq!(http.connections(), 1);
        assert!(peer.quiet());
        drop(control);
        (first, second)
    };
    let (run, (first, second)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = not_sent(second, NotSentReason::RateBudget);
    assert_eq!(handles(&log), [sent(first), refused]);
    assert!(venue.timeouts().is_empty());
    assert_eq!(venue.results(), [(first, Ok(200))]);
}

/// The connection an HTTP query opens is charged with its encode too: with one connection a
/// minute, which the order-entry connection took, the query is `NotSent(RateBudget)`, nothing
/// requested and no connection opened.
#[tokio::test(start_paused = true)]
async fn an_http_query_whose_connection_the_buckets_refuse_is_not_sent_rate_budget_with_nothing_requested()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let mut limits = held_caps().limits;
    limits.push(RateLimit {
        scope: LimitScope::Account,
        ops: TagSet::of(&[OpKind::Connect]),
        per: Duration::from_secs(60),
        units: 1,
    });
    let venue = RestToy::leak(Some(limits), false);
    let config = config(venue, &server.url(), &http.url(""), quick(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(http.try_request().is_none());
        assert_eq!(http.connections(), 0);
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    assert_eq!(handles(&log), [not_sent(rpc, NotSentReason::RateBudget)]);
    assert!(venue.timeouts().is_empty());
    assert!(venue.results().is_empty());
}

/// An order-affecting command fbc-oms authorized whose encode carries its request over HTTP is
/// still `NotSent(Unencodable)` until FBC-4nfb, and so is a consumer's cancel-on-disconnect arm
/// over HTTP, which would protect another connection than the session's: nothing requested or
/// written, no deadline set.
#[tokio::test(start_paused = true)]
async fn an_order_command_or_a_consumers_arm_over_http_is_still_not_sent_unencodable_with_nothing_requested()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let http = ScriptedHttp::start().await;
    let venue = RestToy::leak(None, true);
    let config = config(venue, &server.url(), &http.url(""), quick(), 0);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep(Rc::clone(&log))).unwrap();
    let orders = session.orders();
    let mut oms = Oms::armed();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        ready(&mut peer, &watch).await;
        assert!(orders.may_place());
        let placed = orders.submit(oms.place()).unwrap();
        let cancelled = orders.submit(oms.cancel()).unwrap();
        let armed = orders
            .submit_control(ControlCommand::ArmCancelOnDisconnect)
            .unwrap();
        settle(|| handles(&watch).len() == 3).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert_eq!(http.connections(), 0);
        assert!(peer.quiet());
        drop(control);
        [placed, cancelled, armed]
    };
    let (run, rpcs) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = rpcs.map(|rpc| not_sent(rpc, NotSentReason::Unencodable));
    assert_eq!(handles(&log), refused);
    let encodes = venue.calls();
    assert_eq!(encodes.len(), 3, "{encodes:?}");
    assert!(venue.timeouts().is_empty());
    assert!(outcomes(&log).is_empty());
}
