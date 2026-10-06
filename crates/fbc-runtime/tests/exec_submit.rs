//! FBC-0ga's done line (decision 0057): commands reach the order-entry session's codec only
//! through `ExecOrders`, which takes an fbc-oms `Authorization` for an order-affecting command
//! and a `ControlCommand` for one that affects no order. The session encodes each with an
//! `EncodeCtx` holding one nonce per item, reserved from the consumer's source; clears a
//! request's deadline on the first event that answers it, so an answered request never reaches
//! `on_rpc_timeout`; reports an unanswered one `Unknown` once, at its deadline, also across a
//! reconnect and while disconnected, and never writes it again; and reports a command
//! submitted while its stream has no authenticated epoch `NotSent(Disconnected)` and one whose
//! effects do not carry its request `NotSent(Unencodable)`, each with no byte written.
//!
//! The requests here are control commands (an order query: one item, one nonce, answered by a
//! `qres` record), which reach the same path as an authorized place. Once the toy acknowledges
//! the authentication, the session arms its cancel-on-disconnect and resyncs (FBC-w19): the
//! tests have the venue accept the arm and answer the resync first ([`ready`]), and leave the
//! session's own arm out of what they log.

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::toy::ToyVenue;
use common::{Peer, ScriptedWs};
use exec_toy::{OWN_NS, RPC_TIMEOUT, TOY_TOKEN, ToyExec, ToySigner};
use fbc_core::{
    AccountKey, AccountSummary, AssetKey, ConfigError, ConnKey, ConnState, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec,
    ExecEndpoint, ExecEvent, ExecSink, FieldSpec, HttpFailure, HttpMethod, HttpPlan, HttpRequest,
    HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentSpecDraft, MdCodec, NonceBlock,
    NonceSource, NotSentReason, PathStamps, RawFrame, RpcCall, RpcId, Secrets, SpecTable, StreamId,
    SubmitHandle, SubmitOutcome, Subscription, SymbolError, TimerTag, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, WireUrl,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, ExecSessionError,
    IngestClock, ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, SubmitRefusal,
    WriteStall,
};
use tokio::time::{Instant, advance};

// ---------------------------------------------------------------------------------------------
// The venue: the conformance toy's order entry on one endpoint, its encodes and timeouts logged.
// ---------------------------------------------------------------------------------------------

/// The endpoint's URL; required.
const URL: &str = "exec.url";
/// Any value: the codec's encodes name no request, so their effects do not carry it.
const STRIP: &str = "submit.strip";
/// Any value: the codec's encodes also set a timer, which an encode may.
const TIMER: &str = "submit.timer";
/// A number: the toy's account limit admits this many units a second, not 50.
const UNITS: &str = "submit.units";
/// Any value: the codec's encodes carry their request as an HTTP request, not a frame.
const HTTP: &str = "submit.http";
/// Any value: the codec's encodes give their request a timeout past the end of the clock.
const NEVER: &str = "submit.never";

/// The account the tests' sessions trade.
const ACCT: AccountKey = AccountKey::new(4);

/// What the codec was asked to do: encode a request with this context, or time one out at this
/// instant.
#[derive(Clone, Debug, PartialEq)]
enum Call {
    Encode { rpc: RpcId, ctx: EncodeCtx },
    Timeout { rpc: RpcId, at: Instant },
}

type Calls = Arc<Mutex<Vec<Call>>>;

#[derive(Default)]
struct SubmitToy {
    calls: Calls,
}

impl SubmitToy {
    fn leak() -> &'static SubmitToy {
        Box::leak(Box::default())
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    /// The requests the codec timed out, with when.
    fn timeouts(&self) -> Vec<(RpcId, Instant)> {
        let calls = self.calls.lock().unwrap();
        let timeout = |call: &Call| match call {
            Call::Timeout { rpc, at } => Some((*rpc, *at)),
            Call::Encode { .. } => None,
        };
        calls.iter().filter_map(timeout).collect()
    }
}

impl VenueFactory for SubmitToy {
    fn id(&self) -> &'static str {
        "TOY-SUBMIT"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = exec_toy::caps();
        if let Some(units) = cfg.get(UNITS) {
            caps.limits[0].units = units.parse().unwrap();
        }
        Ok(caps)
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

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        ToyVenue::leak().md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = cfg.get(URL).unwrap();
        Ok(vec![ExecEndpoint {
            stream: exec_toy::EXEC_STREAM,
            url: WireUrl::plain(url),
        }])
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok(Box::new(Logged {
            inner: ToyExec::new(Box::new(ToySigner)),
            calls: Arc::clone(&self.calls),
            strip: cfg.get(STRIP).is_some(),
            timer: cfg.get(TIMER).is_some(),
            http: cfg.get(HTTP).is_some(),
            never: cfg.get(NEVER).is_some(),
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

/// The toy's codec, logging each encode and timeout but the session's own arm, which it encodes
/// as the toy does; with `strip`, an encode's frames name no
/// request, with `timer`, an encode also sets a timer, with `http`, an encode's frames go as
/// HTTP requests naming the request instead, and with `never`, an encode's frames give the
/// request a timeout past the end of the clock.
struct Logged {
    inner: ToyExec,
    calls: Calls,
    strip: bool,
    timer: bool,
    http: bool,
    never: bool,
}

impl ExecCodec for Logged {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
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
        // The session's own arm (FBC-w19, exec_ready.rs) is encoded as the toy does, unlogged.
        if *cmd == VenueCommand::ArmCancelOnDisconnect(true) {
            return self.inner.encode(cmd, rpc, specs, ctx, t, fx);
        }
        let encode = Call::Encode {
            rpc,
            ctx: ctx.clone(),
        };
        self.calls.lock().unwrap().push(encode);
        let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, fx)?;
        if self.timer {
            let (tag, after) = (TimerTag(rpc.0), RPC_TIMEOUT * 10);
            fx.push(Effect::Timer { tag, after });
        }
        if self.strip {
            for effect in fx.take() {
                fx.push(match effect {
                    Effect::Send {
                        stream,
                        frame,
                        class,
                        charge,
                        ..
                    } => Effect::Send {
                        stream,
                        frame,
                        rpc: None,
                        class,
                        charge,
                    },
                    other => other,
                });
            }
        }
        if self.never {
            for effect in fx.take() {
                fx.push(match effect {
                    Effect::Send {
                        stream,
                        frame,
                        rpc: Some(call),
                        class,
                        charge,
                    } => Effect::Send {
                        stream,
                        frame,
                        rpc: Some(RpcCall {
                            timeout: Duration::MAX,
                            ..call
                        }),
                        class,
                        charge,
                    },
                    other => other,
                });
            }
        }
        if self.http {
            for effect in fx.take() {
                fx.push(match effect {
                    Effect::Send {
                        frame,
                        rpc: Some(call),
                        class,
                        charge,
                        ..
                    } => Effect::Http {
                        tag: HttpTag(call.id.0),
                        req: HttpRequest {
                            method: HttpMethod::Post,
                            url: WireUrl::plain("http://127.0.0.1:9/orders"),
                            headers: Vec::new(),
                            body: frame,
                        },
                        rpc: Some(call.id),
                        timeout: call.timeout,
                        class,
                        charge,
                    },
                    other => other,
                });
            }
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
        self.inner.on_http(tag, resp, scope, specs, sink, fx)
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
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

/// Nonces counted up from 0, each reservation logged; from its `short_from`th reservation on,
/// one fewer than asked.
struct Counting {
    next: u64,
    short_from: usize,
    log: Arc<Mutex<Vec<Vec<u64>>>>,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let short = self.log.lock().unwrap().len() >= self.short_from;
        let len = len - u16::from(short);
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        self.log.lock().unwrap().push(block.as_slice().to_vec());
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

/// A session of the logged toy at `url`, trading [`ACCT`]; its nonce reservations logged.
fn setup(
    venue: &'static SubmitToy,
    url: &str,
    pacing: ReconnectPacing,
    cfg: &[(&'static str, &str)],
) -> (ExecSessionConfig, Arc<Mutex<Vec<Vec<u64>>>>) {
    let log = Arc::default();
    let mut venue_cfg = VenueConfig::new();
    venue_cfg.insert(URL, url);
    for (k, v) in cfg {
        venue_cfg.insert(k, v);
    }
    let limits = venue.caps(&venue_cfg).unwrap().limits;
    let nonces = Counting {
        next: 0,
        short_from: usize::MAX,
        log: Arc::clone(&log),
    };
    let config = ExecSessionConfig {
        venue,
        cfg: venue_cfg,
        creds: Secrets::new(),
        acct: ACCT,
        rpc_ids: RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    };
    (config, log)
}

/// What the handler heard: an event, a submission's handle, or an epoch's end.
#[derive(Debug)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    Submitted(SubmitHandle),
    End(ConnKey),
}

type Log = Rc<RefCell<Vec<Heard>>>;

/// What a [`Keep`] submits while handling the first `Authenticated` event, once set.
type OnAuth = Rc<RefCell<Option<(ExecOrders, ControlCommand)>>>;

/// A handler that keeps what it hears; once `on_auth` is set, it submits that control command
/// through those orders while handling the first `Authenticated` event.
struct Keep {
    log: Log,
    on_auth: OnAuth,
}

impl Keep {
    fn new(log: &Log) -> Keep {
        Keep {
            log: Rc::clone(log),
            on_auth: OnAuth::default(),
        }
    }
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let authenticated = ExecEvent::Conn {
            stream: exec_toy::EXEC_STREAM,
            state: ConnState::Authenticated,
        };
        let on_auth = (env.body == authenticated).then(|| self.on_auth.borrow_mut().take());
        if let Some((orders, cmd)) = on_auth.flatten() {
            orders.submit_control(cmd).unwrap();
        }
        self.log.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.log.borrow_mut().push(Heard::Submitted(handle));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::End(key));
    }
}

/// How many epochs the handler was told ended.
fn ends(log: &Log) -> usize {
    let log = log.borrow();
    log.iter().filter(|h| matches!(h, Heard::End(_))).count()
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

/// The outcomes the handler heard: request, item index, outcome and epoch. The venue's
/// acceptances of the session's own arms, which [`ready`] asks for, are left out: no request
/// of the tests' is ever accepted, only answered as a query or timed out.
fn outcomes(log: &Log) -> Vec<(RpcId, Option<u16>, SubmitOutcome, ConnKey)> {
    let log = log.borrow();
    let outcome = |heard: &Heard| match heard {
        Heard::Event(env) => match &env.body {
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Accepted { .. },
                ..
            } => None,
            ExecEvent::Outcome { rpc, item, outcome } => Some((
                *rpc,
                item.as_ref().map(|i| i.idx),
                outcome.clone(),
                env.stamp.conn,
            )),
            _ => None,
        },
        _ => None,
    };
    log.iter().filter_map(outcome).collect()
}

/// How many query answers the handler heard.
fn answers(log: &Log) -> usize {
    let log = log.borrow();
    let answer = |h: &&Heard| matches!(h, Heard::Event(env) if matches!(env.body, ExecEvent::QueryResult(_)));
    log.iter().filter(answer).count()
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets the session run, without moving the clock, until `done`.
async fn settle(done: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("not settled");
}

const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";

/// An order query for venue order `V-1`: one item, as a cancel or a place.
fn query() -> ControlCommand {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    ControlCommand::Query(fbc_core::QueryOrder {
        target: fbc_core::OrderRef::Venue(vid),
        inst: exec_toy::INST_A,
        placement_nonce: None,
    })
}

/// What the toy writes for [`query`], request `rpc`.
fn queried(rpc: RpcId) -> String {
    format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", rpc.0)
}

/// The venue's answer to query `rpc`: no such order.
fn not_found(rpc: RpcId) -> String {
    format!("qres|rpc={}|found=0", rpc.0)
}

/// Acknowledges the authentication `peer` was sent, reads the arm and the resync the session
/// then sends (FBC-w19), has the venue accept the arm and answer the resync with nothing open,
/// and waits until the handler heard the resync end: the epoch takes places. The arm's request.
async fn ready(peer: &mut Peer, watch: &Log) -> RpcId {
    let resynced = resync_ends(watch);
    peer.send(AUTH_ACK);
    let arm = peer.recv().await;
    let rpc = arm
        .strip_prefix("cod|rpc=")
        .and_then(|a| a.strip_suffix("|on=1"));
    let rpc = RpcId(rpc.expect(&arm).parse().unwrap());
    let resync = peer.recv().await;
    let wm = resync.strip_prefix("resync|ts=").expect(&resync);
    peer.send(&format!("item|rpc={}|i=0|res=ok", rpc.0));
    peer.send_all([format!("rsbegin|wm={wm}"), "rsend".to_owned()]);
    settle(|| resync_ends(watch) == resynced + 1).await;
    rpc
}

/// How many times the handler heard a resync end.
fn resync_ends(log: &Log) -> usize {
    let log = log.borrow();
    let end = |h: &&Heard| matches!(h, Heard::Event(env) if env.body == ExecEvent::ResyncEnd);
    log.iter().filter(end).count()
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// A submitted request is encoded once, with one nonce per item reserved from the consumer's
/// source; its handle reaches the handler; the venue's answer clears its deadline, so
/// `on_rpc_timeout` never sees it, however long the session runs on. (Its encode also sets a
/// timer, which an encode's effects may.)
#[tokio::test(start_paused = true)]
async fn a_request_is_encoded_with_one_reserved_nonce_per_item_and_its_answer_clears_its_deadline()
{
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, reserved) = setup(venue, &server.url(), quick(), &[(TIMER, "1")]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        let auth = peer.recv().await;
        assert!(auth.starts_with("auth|ts="), "{auth}");
        assert!(auth.ends_with(TOY_TOKEN));
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        assert_eq!(peer.recv().await, queried(rpc));
        peer.send(&not_found(rpc));
        settle(|| answers(&watch) == 1).await;
        // Its deadline, and twice it, pass: the answer cleared it.
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    // One encode, with exactly one nonce (the request's one item), the source's only
    // reservation after the session's arm's; the toy's on_open and resync ask for none.
    let calls = venue.calls();
    let [Call::Encode { rpc: encoded, ctx }] = &calls[..] else {
        panic!("{calls:?}");
    };
    assert_eq!((*encoded, ctx.nonces.as_slice()), (rpc, &[1][..]));
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1]]);
    // Its handle reached the handler: sent, using no nonce of its own (a query keeps none).
    let sent = SubmitHandle {
        rpc,
        receipt: Ok(EncodeReceipt::new()),
    };
    assert_eq!(handles(&log), [sent]);
    assert_eq!(answers(&log), 1);
    assert!(outcomes(&log).is_empty());
    assert!(venue.timeouts().is_empty());
}

/// An unanswered request is reported `Unknown` once, exactly at its deadline, through the
/// codec's `on_rpc_timeout`; the session never writes it again, not even on the next epoch.
#[tokio::test(start_paused = true)]
async fn an_unanswered_request_is_reported_unknown_once_at_its_deadline_and_never_written_again() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        let (sent_at, sent) = peer.next_at().await.unwrap();
        assert_eq!(sent, queried(rpc));
        // Not a moment before its deadline.
        advance(RPC_TIMEOUT - ms(1)).await;
        churn().await;
        assert!(venue.timeouts().is_empty());
        advance(ms(1)).await;
        settle(|| !venue.timeouts().is_empty()).await;
        // Never again, however long the session runs on.
        advance(RPC_TIMEOUT * 3).await;
        churn().await;
        assert!(peer.quiet());
        // Nor on the next epoch.
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        advance(ms(10)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        ready(&mut next, &watch).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(next.quiet());
        drop(control);
        (rpc, sent_at)
    };
    let (run, (rpc, sent_at)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let timeouts = venue.timeouts();
    assert_eq!(timeouts.len(), 1, "{timeouts:?}");
    assert_eq!(timeouts[0].0, rpc);
    assert_eq!(timeouts[0].1.duration_since(sent_at), RPC_TIMEOUT);
    let unknown = (rpc, None, SubmitOutcome::Unknown, key(0));
    assert_eq!(outcomes(&log), [unknown]);
    // Encoded once.
    let encodes = venue.calls().len() - timeouts.len();
    assert_eq!(encodes, 1);
}

/// A request the handler submits while handling the first epoch's `Authenticated` event goes
/// out at once, behind the session's arm and resync; the connection drops before it is answered, and it is reported `Unknown` at its
/// deadline on the next epoch, which never writes it again.
#[tokio::test(start_paused = true)]
async fn a_request_in_flight_across_a_reconnect_is_reported_unknown_at_its_deadline_on_the_next_epoch()
 {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[]);
    let log = Log::default();
    let keep = Keep::new(&log);
    let on_auth = Rc::clone(&keep.on_auth);
    let (mut session, control) = ExecSession::new(config, keep).unwrap();
    *on_auth.borrow_mut() = Some((session.orders(), query()));
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let (sent_at, sent) = peer.next_at().await.unwrap();
        assert!(sent.starts_with("query|rpc="), "{sent}");
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        advance(ms(10)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        ready(&mut next, &watch).await;
        advance(RPC_TIMEOUT - ms(10) - ms(1)).await;
        churn().await;
        assert!(venue.timeouts().is_empty());
        advance(ms(1)).await;
        settle(|| !outcomes(&watch).is_empty()).await;
        advance(RPC_TIMEOUT).await;
        churn().await;
        assert!(next.quiet());
        drop(control);
        sent_at
    };
    let (run, sent_at) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let rpc = handles(&log)[0].rpc;
    let ends: Vec<_> = log
        .borrow()
        .iter()
        .filter_map(|h| match h {
            Heard::End(key) => Some(*key),
            _ => None,
        })
        .collect();
    assert_eq!(ends, [key(0), key(1)]);
    let timeouts = venue.timeouts();
    assert_eq!(timeouts.len(), 1, "{timeouts:?}");
    assert_eq!(timeouts[0].1.duration_since(sent_at), RPC_TIMEOUT);
    assert_eq!(
        outcomes(&log),
        [(rpc, None, SubmitOutcome::Unknown, key(1))]
    );
}

/// A connection that drops with a request in flight, and is not open again by its deadline:
/// the request is reported `Unknown` then, while the session waits to reconnect, and a command
/// submitted meanwhile is `NotSent(Disconnected)`; the next epoch writes neither.
#[tokio::test(start_paused = true)]
async fn while_disconnected_a_deadline_still_reports_unknown_and_a_new_command_is_not_sent() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, reserved) = setup(venue, &server.url(), slow(), &[]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let first = orders.submit_control(query()).unwrap();
        assert_eq!(peer.recv().await, queried(first));
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        let second = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 2).await;
        advance(RPC_TIMEOUT).await;
        settle(|| !outcomes(&watch).is_empty()).await;
        // The pacing's minute passes and the session reconnects.
        advance(Duration::from_secs(60)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        ready(&mut next, &watch).await;
        churn().await;
        assert!(next.quiet());
        drop(control);
        (first, second)
    };
    let (run, (first, second)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let disconnected = SubmitHandle {
        rpc: second,
        receipt: Err(NotSentReason::Disconnected),
    };
    let sent = SubmitHandle {
        rpc: first,
        receipt: Ok(EncodeReceipt::new()),
    };
    assert_eq!(handles(&log), [sent, disconnected]);
    // Reported under the epoch the session waits to open.
    assert_eq!(
        outcomes(&log),
        [(first, None, SubmitOutcome::Unknown, key(1))]
    );
    // The second was never encoded and no nonce was reserved for it: the first epoch's arm,
    // the first request and the next epoch's arm took one each.
    assert_eq!(venue.timeouts().len(), 1);
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1], vec![2]]);
}

/// A command submitted before the venue acknowledged the authentication is
/// `NotSent(Disconnected)`: nothing is encoded or written, and no deadline is set.
#[tokio::test(start_paused = true)]
async fn a_command_submitted_while_no_epoch_is_authenticated_is_not_sent_with_no_byte_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, reserved) = setup(venue, &server.url(), quick(), &[]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Disconnected),
    };
    assert_eq!(handles(&log), [refused]);
    assert!(venue.calls().is_empty());
    assert!(reserved.lock().unwrap().is_empty());
    assert!(outcomes(&log).is_empty());
}

/// A command whose encode's effects do not carry its request (here, its frame names no rpc,
/// so no deadline would bound it) is `NotSent(Unencodable)`: nothing is written, and no
/// `Unknown` follows.
#[tokio::test(start_paused = true)]
async fn a_command_whose_effects_do_not_carry_its_request_is_not_sent_with_no_byte_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[(STRIP, "1")]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Unencodable),
    };
    assert_eq!(handles(&log), [refused]);
    let calls = venue.calls();
    assert!(matches!(&calls[..], [Call::Encode { .. }]), "{calls:?}");
    assert!(outcomes(&log).is_empty());
}

/// A command whose encode carries its request as an HTTP request is `NotSent(Unencodable)`:
/// order entry is WebSocket-only (decision 0057), since an HTTP request gets no deadline and
/// its result is dropped once its epoch ends, so it could never come back `Unknown` (PR #87
/// Reviewer A, Reviewer B B1). Nothing is written or requested, and no `Unknown` follows.
#[tokio::test(start_paused = true)]
async fn a_command_whose_encode_carries_its_request_over_http_is_not_sent_with_nothing_requested() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[(HTTP, "1")]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Unencodable),
    };
    assert_eq!(handles(&log), [refused]);
    let calls = venue.calls();
    assert!(matches!(&calls[..], [Call::Encode { .. }]), "{calls:?}");
    assert!(outcomes(&log).is_empty());
}

/// A command whose encode gives its request a timeout past the end of the clock is
/// `NotSent(Unencodable)`: its deadline cannot be represented, so it would never come back
/// `Unknown` (0005, `RpcCall`'s own contract; PR #87 Reviewer B B9). Nothing is written, and no
/// `Unknown` follows.
#[tokio::test(start_paused = true)]
async fn a_command_whose_request_has_no_representable_deadline_is_not_sent_with_no_byte_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[(NEVER, "1")]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Unencodable),
    };
    assert_eq!(handles(&log), [refused]);
    let calls = venue.calls();
    assert!(matches!(&calls[..], [Call::Encode { .. }]), "{calls:?}");
    assert!(outcomes(&log).is_empty());
}

/// Request ids are unique for the account across the sessions built in turn for it, so an
/// answer, or an `Unknown`, for one session's request can never be matched to an earlier
/// session's request whose fate fbc-oms still waits on (PR #87 Reviewer B B2).
#[tokio::test(start_paused = true)]
async fn request_ids_are_unique_for_the_account_across_its_sessions() {
    let server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (first, _) = setup(venue, &server.url(), quick(), &[]);
    let (mut second, _) = setup(venue, &server.url(), quick(), &[]);
    // The account's one counter, handed to each session built for it.
    second.rpc_ids = first.rpc_ids.clone();
    let (session, _control) = ExecSession::new(first, |_: Envelope<ExecEvent>| {}).unwrap();
    let earlier = session.orders().submit_control(query()).unwrap();
    drop(session);
    let (session, _control) = ExecSession::new(second, |_: Envelope<ExecEvent>| {}).unwrap();
    let later = session.orders().submit_control(query()).unwrap();
    assert_ne!(earlier, later);
}

// ---------------------------------------------------------------------------------------------
// The error paths.
// ---------------------------------------------------------------------------------------------

/// A request whose frames the buckets refuse is `NotSent(RateBudget)`: nothing is written and
/// no deadline is set.
#[tokio::test(start_paused = true)]
async fn a_request_the_buckets_refuse_is_not_sent_rate_budget_with_no_byte_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    // Three units a second: the authentication, the arm and the resync take them.
    let (config, _) = setup(venue, &server.url(), quick(), &[(UNITS, "3")]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        let rpc = orders.submit_control(query()).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        advance(RPC_TIMEOUT * 2).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::RateBudget),
    };
    assert_eq!(handles(&log), [refused]);
    assert!(venue.timeouts().is_empty());
}

/// A request the codec refuses to encode is reported with the codec's reason, nothing written:
/// the toy queries only by venue id or placement nonce.
#[tokio::test(start_paused = true)]
async fn a_request_the_codec_refuses_is_reported_with_its_reason_and_nothing_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        // The toy queries by venue id or placement nonce, never by client id alone.
        let query = fbc_core::QueryOrder {
            target: fbc_core::OrderRef::Client(common_cid()),
            inst: exec_toy::INST_A,
            placement_nonce: None,
        };
        let rpc = orders.submit_control(ControlCommand::Query(query)).unwrap();
        settle(|| handles(&watch).len() == 1).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        rpc
    };
    let (run, rpc) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let refused = SubmitHandle {
        rpc,
        receipt: Err(NotSentReason::Unsupported),
    };
    assert_eq!(handles(&log), [refused]);
}

/// A client id minted under a namespace lease of the toy's namespace.
fn common_cid() -> fbc_core::ClientOrderId {
    use std::sync::OnceLock;
    static MINT: OnceLock<Mutex<fbc_core::CidMint>> = OnceLock::new();
    let mint = MINT.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-submit-tests-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = fbc_core::NamespaceLease::acquire(&dir, ACCT, OWN_NS).unwrap();
        Mutex::new(fbc_core::CidMint::new(lease, 0, 0, fbc_core::WallNs(0)))
    });
    mint.lock().unwrap().mint().unwrap()
}

/// A nonce source that reserves another count than an encode asks for ends the session before
/// the codec encodes anything.
#[tokio::test(start_paused = true)]
async fn a_nonce_source_that_reserves_another_count_for_an_encode_ends_the_session() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (mut config, _) = setup(venue, &server.url(), quick(), &[]);
    let short = Arc::default();
    // The session's arm takes the first reservation, whole.
    config.nonces = Box::new(Counting {
        next: 0,
        short_from: 1,
        log: Arc::clone(&short),
    });
    let log = Log::default();
    let (mut session, _control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        orders.submit_control(query()).unwrap();
        // The session ends, closing the connection having written nothing more.
        assert_eq!(peer.next().await, None);
        orders
    };
    let (run, orders) = tokio::join!(session.run(), script);
    drop(frozen);
    assert_eq!(
        run,
        Err(ExecSessionError::Nonces {
            asked: 1,
            reserved: 0
        })
    );
    assert!(venue.calls().is_empty());
    assert!(handles(&log).is_empty());
    // A session that has run takes no more submissions.
    assert_eq!(orders.submit_control(query()), Err(SubmitRefusal::Ended));
}

/// Once the session has stopped, or been dropped, `ExecOrders` refuses every submission.
#[tokio::test]
async fn orders_refuse_submissions_once_the_session_has_stopped_or_dropped() {
    let server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), quick(), &[]);
    let (mut session, control) = ExecSession::new(config, |_: Envelope<ExecEvent>| {}).unwrap();
    let orders = session.orders();
    drop(control);
    session.run().await.unwrap();
    assert_eq!(orders.submit_control(query()), Err(SubmitRefusal::Ended));

    let (config, _) = setup(venue, &server.url(), quick(), &[]);
    let (session, _control) = ExecSession::new(config, |_: Envelope<ExecEvent>| {}).unwrap();
    let orders = session.orders();
    drop(session);
    assert_eq!(orders.submit_control(query()), Err(SubmitRefusal::Ended));
}

/// A command submitted while the session waits to reconnect, in the same turn as the control
/// drops, is never reported: the session stops.
#[tokio::test(start_paused = true)]
async fn a_command_waiting_as_the_control_drops_between_epochs_is_never_reported() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), slow(), &[]);
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let peer = server.accept().await;
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        orders.submit_control(query()).unwrap();
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert!(handles(&log).is_empty());
    assert!(venue.calls().is_empty());
}

/// A handler that takes no handles hears nothing of a submission, and nothing is written for a
/// command submitted before the stream is authenticated.
#[tokio::test(start_paused = true)]
async fn a_handler_that_takes_no_handles_still_gets_nothing_written_unauthenticated() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, reserved) = setup(venue, &server.url(), quick(), &[]);
    let (mut session, control) = ExecSession::new(config, |_: Envelope<ExecEvent>| {}).unwrap();
    let orders = session.orders();
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        orders.submit_control(query()).unwrap();
        churn().await;
        assert!(peer.quiet());
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert!(venue.calls().is_empty());
    assert!(reserved.lock().unwrap().is_empty());
}

/// The most times a [`Resubmit`] submits again.
const AGAIN: usize = 1_000;

/// A handler that keeps what it hears and, once `orders` is set, submits a refresh again each
/// time it is told one was not sent, up to [`AGAIN`] times.
struct Resubmit {
    log: Log,
    orders: Rc<RefCell<Option<ExecOrders>>>,
}

impl ExecHandler for Resubmit {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.log.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let again = handle.receipt.is_err() && handles(&self.log).len() < AGAIN;
        self.log.borrow_mut().push(Heard::Submitted(handle));
        if let Some(orders) = self.orders.borrow().as_ref().filter(|_| again) {
            let _ = orders.submit_control(query());
        }
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::End(key));
    }
}

/// A handler that submits again from `on_submitted` each time it hears a command was not sent
/// cannot hold the session in one turn: a turn takes only what waited as it began, and the
/// session yields while more waits, so a control dropped meanwhile stops it (PR #87 Reviewer B
/// B5). In an epoch, every refresh is refused by the buckets.
#[tokio::test(start_paused = true)]
async fn a_handler_resubmitting_what_was_not_sent_cannot_hold_an_epoch_in_one_turn() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    // Three units a second: the authentication, the arm and the resync take them.
    let (config, _) = setup(venue, &server.url(), quick(), &[(UNITS, "3")]);
    let log = Log::default();
    let shared = Rc::default();
    let handler = Resubmit {
        log: Rc::clone(&log),
        orders: Rc::clone(&shared),
    };
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    let orders = session.orders();
    *shared.borrow_mut() = Some(orders.clone());
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        ready(&mut peer, &watch).await;
        orders.submit_control(query()).unwrap();
        settle(|| !handles(&watch).is_empty()).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let heard = handles(&log);
    assert!(heard.len() < AGAIN, "{}", heard.len());
    let rate = |h: &SubmitHandle| h.receipt == Err(NotSentReason::RateBudget);
    assert!(heard.iter().all(rate));
}

/// The same while the session waits to reconnect: every command is `NotSent(Disconnected)`,
/// and a handler submitting again each time still lets the control's drop stop the session.
#[tokio::test(start_paused = true)]
async fn a_handler_resubmitting_what_was_not_sent_cannot_hold_the_wait_to_reconnect() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = SubmitToy::leak();
    let (config, _) = setup(venue, &server.url(), slow(), &[]);
    let log = Log::default();
    let shared = Rc::default();
    let handler = Resubmit {
        log: Rc::clone(&log),
        orders: Rc::clone(&shared),
    };
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    let orders = session.orders();
    *shared.borrow_mut() = Some(orders.clone());
    let watch = Rc::clone(&log);
    let script = async move {
        let peer = server.accept().await;
        peer.drop_conn();
        settle(|| ends(&watch) == 1).await;
        orders.submit_control(query()).unwrap();
        settle(|| !handles(&watch).is_empty()).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let heard = handles(&log);
    assert!(heard.len() < AGAIN, "{}", heard.len());
    let gone = |h: &SubmitHandle| h.receipt == Err(NotSentReason::Disconnected);
    assert!(heard.iter().all(gone));
}
