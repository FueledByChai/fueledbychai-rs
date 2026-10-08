//! FBC-w19's done line (decision 0058): on every epoch the order-entry session sends
//! `ArmCancelOnDisconnect(true)` once the codec reports the stream `Authenticated`, then runs the
//! codec's resync, and its epoch takes no place or amend (`ExecOrders::may_place` is false) until
//! the venue has finally accepted the arm and the epoch's `ResyncEnd` has been handed to the
//! handler (which, hearing the event that opens the epoch, already sees places taken); an arm
//! rejected or unanswered by its deadline leaves the epoch refusing orders, counted, and ends it
//! as a drop reconnected through the pacing, while a command that is not a place or amend still
//! goes out; a venue declaring cancel-on-disconnect `None` or `DeadMan` is refused when the
//! session starts, naming it; and nothing written before a drop is written again after it.
//!
//! The conformance toy declares cancel-on-disconnect per connection, re-armed on reconnect.
//! The commands submitted here are control commands: an order query, a safety command like a
//! cancel, which the gate lets through as it does a cancel. That the session refuses a place and
//! an amend fbc-oms authorized, counted, with no nonce reserved and no byte written, while an
//! authorized cancel goes out, is shown in `exec_held.rs`, and on the gate by `exec_gate.rs`'s
//! unit tests.

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::toy::ToyVenue;
use common::{ScriptedHttp, ScriptedWs};
use exec_toy::{INST_A, OWN_NS, RPC_TIMEOUT, ToyExec, ToySigner};
use fbc_core::{
    AccountKey, AccountSummary, AckLevel, AssetKey, CancelOnDisconnect, ConfigError, ConnKey,
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    EndpointPlan, Envelope, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, FieldSpec, HttpFailure,
    HttpMethod, HttpPlan, HttpRequest, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentSpecDraft, MdCodec, NonceBlock, NonceSource, NotSentReason, OpKind, OrderRef,
    PathStamps, QueryOrder, RateCharge, RawFrame, RejectKind, RpcId, Secrets, SpecTable, StreamId,
    SubmitHandle, SubmitOutcome, Subscription, SymbolError, TimerTag, TrafficClass, VenueCaps,
    VenueCommand, VenueConfig, VenueError, VenueFactory, WireSlice, WireUrl,
};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig, ExecSessionError,
    IngestClock, ProxyConfig, RateLimiter, ReconnectPacing, RpcIds, SafetyReserve, ScopeCounts,
    WriteStall,
};
use tokio::time::advance;

// ---------------------------------------------------------------------------------------------
// The venue: the conformance toy's order entry on one endpoint, its encodes and resyncs logged.
// ---------------------------------------------------------------------------------------------

/// The endpoint's URL; required.
const URL: &str = "exec.url";
/// `none`, `deadman`, or `once` (per connection, not re-armed on reconnect); by default the
/// toy's own declaration, per connection and re-armed on reconnect.
const COD: &str = "ready.cod";
/// A number: the toy's account limit admits this many units a second, not 50.
const UNITS: &str = "ready.units";
/// Any value: the codec refuses to encode an arm.
const REFUSE_ARM: &str = "ready.refuse_arm";
/// Any value: the codec's resync frame weighs more than the toy's account limit ever admits.
const HEAVY_RESYNC: &str = "ready.heavy_resync";
/// Any value: the codec's arm frame weighs more than the toy's account limit ever admits.
const HEAVY_ARM: &str = "ready.heavy_arm";
/// A number: the codec's resync is one HTTP read of this weight, as Paradex's REST resync is
/// (FBC-0sc), in place of the toy's frame.
const HTTP_RESYNC: &str = "ready.http_resync";
/// The URL the HTTP resync reads; by default one never reached.
const HTTP_RESYNC_URL: &str = "ready.http_resync_url";

const ACCT: AccountKey = AccountKey::new(4);

/// What the codec was asked to do: encode a command as a request with a context, or resync with
/// one.
#[derive(Clone, Debug, PartialEq)]
enum Call {
    Encode {
        cmd: VenueCommand,
        rpc: RpcId,
        ctx: EncodeCtx,
    },
    Resync(EncodeCtx),
}

type Calls = Arc<Mutex<Vec<Call>>>;

#[derive(Default)]
struct ReadyToy {
    calls: Calls,
}

impl ReadyToy {
    fn leak() -> &'static ReadyToy {
        Box::leak(Box::default())
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    /// The commands encoded, with their requests.
    fn encoded(&self) -> Vec<(VenueCommand, RpcId)> {
        let encoded = |call: &Call| match call {
            Call::Encode { cmd, rpc, .. } => Some((cmd.clone(), *rpc)),
            Call::Resync(_) => None,
        };
        self.calls().iter().filter_map(encoded).collect()
    }

    fn resyncs(&self) -> usize {
        let calls = self.calls();
        calls
            .iter()
            .filter(|c| matches!(c, Call::Resync(_)))
            .count()
    }
}

impl VenueFactory for ReadyToy {
    fn id(&self) -> &'static str {
        "TOY-READY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = exec_toy::caps();
        let order = &mut caps.exec.as_mut().unwrap().order;
        match cfg.get(COD) {
            Some("none") => order.cancel_on_disconnect = CancelOnDisconnect::None,
            Some("deadman") => {
                let max_ttl = Duration::from_secs(10);
                order.cancel_on_disconnect = CancelOnDisconnect::DeadMan { max_ttl };
            }
            Some("once") => {
                let once = CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: false,
                    covers_open_orders: false,
                };
                order.cancel_on_disconnect = once;
            }
            Some(other) => panic!("{other}"),
            None => {}
        }
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
        Some(Ok(Box::new(Logged {
            inner: ToyExec::new(Box::new(ToySigner)),
            calls: Arc::clone(&self.calls),
            refuse_arm: cfg.get(REFUSE_ARM).is_some(),
            heavy_resync: cfg.get(HEAVY_RESYNC).is_some(),
            heavy_arm: cfg.get(HEAVY_ARM).is_some(),
            http_resync: cfg.get(HTTP_RESYNC).map(|w| {
                let url = cfg
                    .get(HTTP_RESYNC_URL)
                    .unwrap_or("https://venue.invalid/orders");
                (w.parse().unwrap(), url.to_owned())
            }),
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

/// The toy's codec, logging each encode and resync; with `refuse_arm` it refuses to encode an
/// arm, with `heavy_resync` its resync frame weighs 51 units, with `heavy_arm` its arm frame
/// does, and with `http_resync` its resync is one HTTP read of that weight from that URL.
struct Logged {
    inner: ToyExec,
    calls: Calls,
    refuse_arm: bool,
    heavy_resync: bool,
    heavy_arm: bool,
    http_resync: Option<(u32, String)>,
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
        let (cmd, ctx) = (cmd.clone(), ctx.clone());
        let arm = cmd == VenueCommand::ArmCancelOnDisconnect(true);
        self.calls.lock().unwrap().push(Call::Encode {
            cmd: cmd.clone(),
            rpc,
            ctx: ctx.clone(),
        });
        if arm && self.refuse_arm {
            return Err(NotSentReason::Unsupported);
        }
        let receipt = self.inner.encode(&cmd, rpc, specs, &ctx, t, fx)?;
        if arm && self.heavy_arm {
            heavy(fx);
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
        // A two-phase venue's provisional acceptance of request `rpc`'s one item, which the toy
        // never sends.
        if let RawFrame::Text(text) = f
            && let Some(rpc) = text.strip_prefix("prov|rpc=")
        {
            let rpc = RpcId(rpc.parse().unwrap());
            let outcome = SubmitOutcome::Accepted {
                ack: AckLevel::Provisional,
            };
            let item = Some(fbc_core::ItemRef {
                idx: 0,
                cid: None,
                vid: None,
            });
            let ev = ExecEvent::Outcome { rpc, item, outcome };
            sink.push(fbc_core::VenueMeta::NONE, ev);
            return Ok(());
        }
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
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.calls.lock().unwrap().push(Call::Resync(ctx.clone()));
        self.inner.resync(ctx, fx);
        if self.heavy_resync {
            heavy(fx);
        }
        if let Some((weight, url)) = &self.http_resync {
            fx.take();
            fx.push(read(*weight, url));
        }
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

/// Makes every frame in `fx` weigh 51 units, more than the toy's account limit ever admits.
fn heavy(fx: &mut Effects) {
    for effect in fx.take() {
        fx.push(match effect {
            Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge,
            } => Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge: RateCharge {
                    weight: NonZeroU32::new(51).unwrap(),
                    ..charge
                },
            },
            other => other,
        });
    }
}

/// A resync read of `weight` units from `url` over HTTP, which the toy's account limit counts
/// as a query.
fn read(weight: u32, url: &str) -> Effect {
    Effect::Http {
        tag: HttpTag(77),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(url),
            headers: vec![],
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: Duration::from_secs(5),
        class: TrafficClass::Safety,
        charge: RateCharge {
            weight: NonZeroU32::new(weight).unwrap(),
            ..RateCharge::one(OpKind::Query, None)
        },
    }
}

/// Nonces counted up from 0, each reservation logged.
struct Counting {
    next: u64,
    log: Arc<Mutex<Vec<Vec<u64>>>>,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
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

type Reserved = Arc<Mutex<Vec<Vec<u64>>>>;

/// A session of the logged toy at `url`, trading [`ACCT`]; its nonce reservations logged.
fn setup(
    venue: &'static ReadyToy,
    url: &str,
    cfg: &[(&'static str, &str)],
) -> (ExecSessionConfig, Reserved) {
    let log = Reserved::default();
    let mut venue_cfg = VenueConfig::new();
    venue_cfg.insert(URL, url);
    for (k, v) in cfg {
        venue_cfg.insert(k, v);
    }
    let limits = venue.caps(&venue_cfg).unwrap().limits;
    let nonces = Counting {
        next: 0,
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
        pacing: quick(),
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
    };
    (config, log)
}

/// What the handler heard: an event with whether the session took places as it was handed
/// over, a submission's handle, or an epoch's end.
#[derive(Debug)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>, bool),
    Submitted(SubmitHandle),
    End(ConnKey),
}

type Log = Rc<RefCell<Vec<Heard>>>;
type Orders = Rc<RefCell<Option<ExecOrders>>>;

/// A handler that keeps what it hears, with whether the session's orders took places then; on
/// hearing a request rejected it submits `on_reject`, once, through the session's orders.
struct Keep {
    log: Log,
    orders: Orders,
    on_reject: Option<ControlCommand>,
}

impl Keep {
    fn new(log: &Log, orders: &Orders) -> Keep {
        Keep {
            log: Rc::clone(log),
            orders: Rc::clone(orders),
            on_reject: None,
        }
    }
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let orders = self.orders.borrow();
        let may_place = orders.as_ref().is_some_and(ExecOrders::may_place);
        let rejected = matches!(
            env.body,
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Rejected(_),
                ..
            }
        );
        if rejected && let Some(cmd) = self.on_reject.take() {
            orders.as_ref().unwrap().submit_control(cmd).unwrap();
        }
        self.log
            .borrow_mut()
            .push(Heard::Event(Box::new(env), may_place));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.log.borrow_mut().push(Heard::Submitted(handle));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::End(key));
    }
}

/// A session of the logged toy at `url` with `cfg`, its handler keeping what it hears in `log`;
/// the handler's `on_reject`; the session's orders, which the handler reads too.
fn session(
    venue: &'static ReadyToy,
    url: &str,
    cfg: &[(&'static str, &str)],
    log: &Log,
    on_reject: Option<ControlCommand>,
) -> (
    ExecSession<Keep>,
    fbc_runtime::ExecControl,
    ExecOrders,
    Reserved,
) {
    let (config, reserved) = setup(venue, url, cfg);
    let slot = Orders::default();
    let mut keep = Keep::new(log, &slot);
    keep.on_reject = on_reject;
    let (session, control) = ExecSession::new(config, keep).unwrap();
    let orders = session.orders();
    *slot.borrow_mut() = Some(orders.clone());
    (session, control, orders, reserved)
}

/// The bodies of the events the handler heard, with the epoch each was stamped under and
/// whether the session took places as it was handed over.
fn events(log: &Log) -> Vec<(ExecEvent, ConnKey, bool)> {
    let log = log.borrow();
    let event = |heard: &Heard| match heard {
        Heard::Event(env, may_place) => Some((env.body.clone(), env.stamp.conn, *may_place)),
        _ => None,
    };
    log.iter().filter_map(event).collect()
}

fn resync_ends(log: &Log) -> usize {
    let ended = |(ev, _, _): &&(ExecEvent, ConnKey, bool)| *ev == ExecEvent::ResyncEnd;
    events(log).iter().filter(ended).count()
}

fn outcomes(log: &Log) -> Vec<(RpcId, SubmitOutcome)> {
    let outcome = |(ev, _, _): (ExecEvent, ConnKey, bool)| match ev {
        ExecEvent::Outcome { rpc, outcome, .. } => Some((rpc, outcome)),
        _ => None,
    };
    events(log).into_iter().filter_map(outcome).collect()
}

fn ends(log: &Log) -> Vec<ConnKey> {
    let log = log.borrow();
    let end = |heard: &Heard| match heard {
        Heard::End(key) => Some(*key),
        _ => None,
    };
    log.iter().filter_map(end).collect()
}

fn handles(log: &Log) -> Vec<SubmitHandle> {
    let log = log.borrow();
    let handle = |heard: &Heard| match heard {
        Heard::Submitted(handle) => Some(handle.clone()),
        _ => None,
    };
    log.iter().filter_map(handle).collect()
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

const AUTHENTICATED: ExecEvent = ExecEvent::Conn {
    stream: exec_toy::EXEC_STREAM,
    state: ConnState::Authenticated,
};

/// What the toy writes to arm its protection, request `rpc`.
fn arm(rpc: u64) -> String {
    format!("cod|rpc={rpc}|on=1")
}

/// The venue's acceptance of request `rpc`'s one item.
fn accepted(rpc: u64) -> String {
    format!("item|rpc={rpc}|i=0|res=ok")
}

/// A two-phase venue's provisional acceptance of request `rpc`'s one item, which it may still
/// reject.
fn provisional(rpc: u64) -> String {
    format!("prov|rpc={rpc}")
}

/// The venue's refusal of request `rpc`'s one item, for margin.
fn rejected(rpc: u64) -> String {
    format!("item|rpc={rpc}|i=0|res=rej|code=1005")
}

/// The watermark a resync frame asks for.
fn watermark(resync: &str) -> &str {
    resync.strip_prefix("resync|ts=").expect(resync)
}

/// The venue's answer to a resync asked at `wm`: no order and no position.
fn resynced(wm: &str) -> [String; 2] {
    [format!("rsbegin|wm={wm}"), "rsend".to_owned()]
}

/// An order query for venue order `V-1`: a safety command, as a cancel is, which no gate holds.
fn query() -> ControlCommand {
    let vid = exec_toy::with_scope(|scope| scope.venue_order_id("V-1")).unwrap();
    ControlCommand::Query(QueryOrder {
        target: OrderRef::Venue(vid),
        inst: INST_A,
        placement_nonce: None,
    })
}

/// What the toy writes for [`query`], request `rpc`.
fn queried(rpc: RpcId) -> String {
    format!("query|rpc={}|vid=V-1|sym=TOYA-PERP", rpc.0)
}

fn arm_cmd() -> VenueCommand {
    VenueCommand::ArmCancelOnDisconnect(true)
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

/// On each epoch the arm goes out once the codec reports the stream authenticated, and the
/// codec's resync follows it; the epoch takes no place or amend until the venue accepted the
/// arm and the epoch's `ResyncEnd` reached the handler, while a query (no place or amend) goes
/// out at once. The next epoch arms again, with a new request, and resyncs again; nothing the
/// first wrote is written again.
#[tokio::test(start_paused = true)]
async fn each_epoch_arms_after_authentication_and_takes_places_only_once_armed_and_resynced() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, orders, reserved) = session(venue, &server.url(), &[], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        // Nothing before the venue acknowledges the authentication.
        churn().await;
        assert!(peer.quiet());
        assert!(venue.calls().is_empty());
        assert!(!orders.may_place());
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let resync = peer.recv().await;
        let wm = watermark(&resync).to_owned();
        assert!(!orders.may_place());
        // A query is no place or amend: it goes out before the epoch is ready.
        let asked = orders.submit_control(query()).unwrap();
        assert_eq!(peer.recv().await, queried(asked));
        // Accepted, but not yet resynced.
        peer.send(&accepted(1));
        settle(|| outcomes(&watch).len() == 1).await;
        assert!(!orders.may_place());
        peer.send_all(resynced(&wm));
        settle(|| resync_ends(&watch) == 1).await;
        assert!(orders.may_place());

        // The next epoch: not ready until it is armed and resynced again.
        peer.drop_conn();
        settle(|| ends(&watch).len() == 1).await;
        assert!(!orders.may_place());
        advance(ms(10)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        churn().await;
        assert!(next.quiet());
        next.send(AUTH_ACK);
        // A new arm and a new resync, and nothing of the first epoch's.
        assert_eq!(next.recv().await, arm(3));
        let resync = next.recv().await;
        churn().await;
        assert!(next.quiet());
        assert!(!orders.may_place());
        next.send(&accepted(3));
        settle(|| outcomes(&watch).len() == 2).await;
        assert!(!orders.may_place());
        next.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 2).await;
        assert!(orders.may_place());
        // The query is never answered: its deadline passes, and still nothing is written again.
        advance(RPC_TIMEOUT).await;
        churn().await;
        assert!(next.quiet());
        drop(control);
        asked
    };
    let (run, asked) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    // The codec encoded one arm per epoch, each its own request with one reserved nonce, and the
    // query once; and resynced once per epoch, each after the epoch's arm.
    let calls = venue.calls();
    let [
        Call::Encode {
            cmd: first,
            rpc: RpcId(1),
            ctx: first_ctx,
        },
        Call::Resync(_),
        Call::Encode {
            rpc: queried_rpc, ..
        },
        Call::Encode {
            cmd: second,
            rpc: RpcId(3),
            ctx: second_ctx,
        },
        Call::Resync(_),
    ] = &calls[..]
    else {
        panic!("{calls:?}");
    };
    assert_eq!((first, second), (&arm_cmd(), &arm_cmd()));
    assert_eq!(*queried_rpc, asked);
    assert_eq!(first_ctx.nonces.as_slice(), [0]);
    assert_eq!(second_ctx.nonces.as_slice(), [2]);
    assert_eq!(*reserved.lock().unwrap(), [vec![0], vec![1], vec![2]]);
    // The handler heard each epoch authenticated, the arm accepted and the resync, and the
    // session took places from the moment each epoch's ResyncEnd, the last of its two settling
    // events, was handed over: the handler hearing it already sees places taken (PR #90
    // Reviewer B B1), and whatever it submits then goes out only once it has returned.
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let outcome = |rpc| ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item: Some(fbc_core::ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        }),
        outcome: accepted.clone(),
    };
    let (wm0, wm1) = match &calls[..] {
        [_, Call::Resync(a), _, _, Call::Resync(b)] => (a.wall, b.wall),
        _ => unreachable!(),
    };
    let begin = |watermark| ExecEvent::ResyncBegin { watermark };
    assert_eq!(
        events(&log),
        [
            (AUTHENTICATED, key(0), false),
            (outcome(1), key(0), false),
            (begin(wm0), key(0), false),
            (ExecEvent::ResyncEnd, key(0), true),
            (AUTHENTICATED, key(1), false),
            (outcome(3), key(1), false),
            (begin(wm1), key(1), false),
            (ExecEvent::ResyncEnd, key(1), true),
            (
                ExecEvent::Outcome {
                    rpc: asked,
                    item: None,
                    outcome: SubmitOutcome::Unknown,
                },
                key(1),
                true,
            ),
        ]
    );
    assert_eq!(handles(&log).len(), 1);
    assert_eq!(session.counters().arm_failures, 0);
}

/// The venue rejects the arm: the epoch never takes places, ends as a drop counted, and the
/// session reconnects through the pacing; a query the handler submits as it hears the
/// rejection, which a gate would hold were it a place, still goes out on that epoch before it
/// ends. The next epoch arms again.
#[tokio::test(start_paused = true)]
async fn an_arm_rejected_ends_the_epoch_as_a_drop_through_the_pacing_while_a_query_still_goes_out()
{
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, orders, _) = session(venue, &server.url(), &[], &log, Some(query()));
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let resync = peer.recv().await;
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 1).await;
        // Resynced, but not armed.
        assert!(!orders.may_place());
        peer.send(&rejected(1));
        // The query the handler submitted on hearing it goes out, then the epoch ends.
        assert_eq!(peer.recv().await, queried(RpcId(2)));
        settle(|| ends(&watch).len() == 1).await;
        assert!(!orders.may_place());
        assert_eq!(peer.next().await, None);
        // Through the pacing: not before its 10 ms floor.
        advance(ms(9)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        advance(ms(1)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        next.send(AUTH_ACK);
        assert_eq!(next.recv().await, arm(3));
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    let margin = |(rpc, outcome): &(RpcId, SubmitOutcome)| {
        *rpc == RpcId(1)
            && matches!(outcome, SubmitOutcome::Rejected(r) if r.kind == RejectKind::Margin)
    };
    let heard = outcomes(&log);
    assert!(heard.iter().any(margin), "{heard:?}");
    let sent = SubmitHandle {
        rpc: RpcId(2),
        receipt: Ok(EncodeReceipt::new()),
    };
    assert_eq!(handles(&log), [sent]);
    assert!(events(&log).iter().all(|(_, _, may_place)| !may_place));
    assert_eq!(ends(&log), [key(0), key(1)]);
    assert_eq!(session.counters().arm_failures, 1);
    assert_eq!(session.counters().attempts, 2);
}

/// The venue never answers the arm: at its deadline the epoch, resynced or not, still takes no
/// place, and ends as a drop counted; the codec reports the arm `Unknown`, and the next epoch,
/// opened through the pacing, arms again.
#[tokio::test(start_paused = true)]
async fn an_arm_unanswered_by_its_deadline_ends_the_epoch_as_a_drop_and_the_next_arms_again() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, orders, _) = session(venue, &server.url(), &[], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        let (armed_at, sent) = peer.next_at().await.unwrap();
        assert_eq!(sent, arm(1));
        let resync = peer.recv().await;
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 1).await;
        advance(RPC_TIMEOUT - ms(1)).await;
        churn().await;
        assert!(ends(&watch).is_empty());
        assert!(!orders.may_place());
        advance(ms(1)).await;
        settle(|| ends(&watch).len() == 1).await;
        assert!(!orders.may_place());
        advance(ms(10)).await;
        let mut next = server.accept().await;
        next.recv().await;
        next.send(AUTH_ACK);
        assert_eq!(next.recv().await, arm(2));
        drop(control);
        armed_at
    };
    let (run, armed_at) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);

    let _ = armed_at;
    let unknown = (RpcId(1), SubmitOutcome::Unknown);
    assert_eq!(outcomes(&log), [unknown]);
    assert_eq!(ends(&log), [key(0), key(1)]);
    assert_eq!(session.counters().arm_failures, 1);
}

/// A two-phase venue's provisional acceptance of the arm is no acceptance (PR #90 Reviewer A
/// A2): the arm stays pending under its deadline, so a rejection that follows it ends the epoch
/// as a drop, and so does no final answer by the deadline. Only the final acceptance opens an
/// epoch, and when it is the last of the epoch's two settling events the handler hearing it
/// already sees places taken (Reviewer B B1).
#[tokio::test(start_paused = true)]
async fn an_arm_accepted_only_provisionally_stays_pending_under_its_deadline() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, orders, _) = session(venue, &server.url(), &[], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        // Provisional, then rejected: the epoch ends as a drop.
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let resync = peer.recv().await;
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 1).await;
        peer.send(&provisional(1));
        settle(|| outcomes(&watch).len() == 1).await;
        assert!(!orders.may_place());
        peer.send(&rejected(1));
        settle(|| ends(&watch).len() == 1).await;
        assert!(!orders.may_place());

        // Provisional, then nothing by the deadline: the epoch ends as a drop at it.
        advance(ms(100)).await;
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(2));
        let resync = peer.recv().await;
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 2).await;
        peer.send(&provisional(2));
        settle(|| outcomes(&watch).len() == 3).await;
        assert!(!orders.may_place());
        advance(RPC_TIMEOUT - ms(1)).await;
        churn().await;
        assert_eq!(ends(&watch).len(), 1);
        advance(ms(1)).await;
        settle(|| ends(&watch).len() == 2).await;
        assert!(!orders.may_place());

        // Provisional, then final: the epoch, resynced first, takes places as the handler hears
        // the final acceptance, and its arm's deadline ends nothing.
        advance(ms(100)).await;
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(3));
        let resync = peer.recv().await;
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 3).await;
        peer.send(&provisional(3));
        settle(|| outcomes(&watch).len() == 5).await;
        assert!(!orders.may_place());
        peer.send(&accepted(3));
        settle(|| outcomes(&watch).len() == 6).await;
        assert!(orders.may_place());
        advance(RPC_TIMEOUT).await;
        churn().await;
        assert_eq!(ends(&watch).len(), 2);
        assert!(orders.may_place());
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    let provisional = SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    };
    let last = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let heard = outcomes(&log);
    let [
        (RpcId(1), p1),
        (RpcId(1), SubmitOutcome::Rejected(_)),
        (RpcId(2), p2),
        (RpcId(2), SubmitOutcome::Unknown),
        (RpcId(3), p3),
        (RpcId(3), f3),
    ] = &heard[..]
    else {
        panic!("{heard:?}");
    };
    assert_eq!(
        [p1, p2, p3, f3],
        [&provisional, &provisional, &provisional, &last]
    );
    // Only the final acceptance was handed over with places taken.
    let taken: Vec<_> = events(&log).into_iter().filter(|(_, _, m)| *m).collect();
    let [
        (
            ExecEvent::Outcome {
                rpc: RpcId(3),
                outcome,
                ..
            },
            at,
            true,
        ),
    ] = &taken[..]
    else {
        panic!("{taken:?}");
    };
    assert_eq!((outcome, *at), (&last, key(2)));
    assert_eq!(ends(&log), [key(0), key(1), key(2)]);
    assert_eq!(session.counters().arm_failures, 2);
}

/// A codec that refuses to encode the arm leaves it unsent: the epoch ends as a drop, counted,
/// before any resync is asked, and nothing but the authentication was written.
#[tokio::test(start_paused = true)]
async fn an_arm_the_codec_refuses_ends_the_epoch_as_a_drop_with_nothing_written() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, _, reserved) =
        session(venue, &server.url(), &[(REFUSE_ARM, "1")], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        settle(|| ends(&watch).len() == 1).await;
        assert_eq!(peer.next().await, None);
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.encoded(), [(arm_cmd(), RpcId(1))]);
    assert_eq!(venue.resyncs(), 0);
    // Its one nonce was reserved for the encode the codec refused.
    assert_eq!(*reserved.lock().unwrap(), [vec![0]]);
    assert_eq!(session.counters().arm_failures, 1);
}

/// Buckets that cannot take the arm for now leave it unsent, as a codec's refusal does, and the
/// next attempt waits until they would admit it as well as the pacing allows (PR #90 Reviewer B
/// B3).
#[tokio::test(start_paused = true)]
async fn an_arm_the_buckets_refuse_for_now_ends_the_epoch_and_holds_the_next_attempt() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    // One unit a second: the authentication takes it.
    let (mut session, control, _, _) = session(venue, &server.url(), &[(UNITS, "1")], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        settle(|| ends(&watch).len() == 1).await;
        assert_eq!(peer.next().await, None);
        // Well past the pacing's floor, but not yet a second after the authentication.
        advance(ms(900)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        advance(ms(200)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.encoded()[0], (arm_cmd(), RpcId(1)));
    assert_eq!(venue.resyncs(), 0);
    assert!(session.counters().arm_failures >= 1);
}

/// An arm whose frames weigh more than the buckets ever admit ends the session, as a resync's
/// would, rather than reconnect forever (PR #90 Reviewer B B3).
#[tokio::test(start_paused = true)]
async fn an_arm_that_can_never_fit_ends_the_session() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let cfg = [(HEAVY_ARM, "1")];
    let (mut session, _control, _, _) = session(venue, &server.url(), &cfg, &log, None);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.next().await, None);
    };
    let ((), run) = tokio::join!(script, session.run());
    drop(frozen);
    assert_eq!(run, Err(ExecSessionError::ArmNeverFits));
    assert_eq!(venue.encoded(), [(arm_cmd(), RpcId(1))]);
    assert_eq!(venue.resyncs(), 0);
}

/// A resync whose frames the buckets refuse for now ends the epoch as a drop, and the next
/// attempt waits until they would admit it as well as the pacing allows; the arm went out.
#[tokio::test(start_paused = true)]
async fn a_resync_the_buckets_refuse_for_now_ends_the_epoch_and_holds_the_next_attempt() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    // Two units a second: the authentication and the arm take them.
    let (mut session, control, _, _) = session(venue, &server.url(), &[(UNITS, "2")], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        settle(|| ends(&watch).len() == 1).await;
        assert_eq!(peer.next().await, None);
        // Well past the pacing's floor, but not yet a second after the first frame.
        advance(ms(900)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        advance(ms(200)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.resyncs(), 1);
    // The arm is not what failed.
    assert_eq!(session.counters().arm_failures, 0);
}

/// A resync whose frames weigh more together than the buckets ever admit ends the session.
#[tokio::test(start_paused = true)]
async fn a_resync_that_can_never_fit_ends_the_session() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let cfg = [(HEAVY_RESYNC, "1")];
    let (mut session, _control, _, _) = session(venue, &server.url(), &cfg, &log, None);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        assert_eq!(peer.next().await, None);
    };
    let ((), run) = tokio::join!(script, session.run());
    drop(frozen);
    assert_eq!(run, Err(ExecSessionError::ResyncNeverFits));
}

/// A resync made of HTTP reads, as Paradex's is (FBC-0sc), is charged together as a resync's
/// frames are: buckets that refuse its read for now end the epoch as a drop, rather than leave
/// it connected and refusing places with no resync under way, and the next attempt waits until
/// they would admit it as well as the pacing allows (PR #90 Reviewer B B7).
#[tokio::test(start_paused = true)]
async fn a_resync_read_the_buckets_refuse_for_now_ends_the_epoch_and_holds_the_next_attempt() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    // Two units a second: the authentication and the arm take them.
    let cfg = [(UNITS, "2"), (HTTP_RESYNC, "1")];
    let (mut session, control, _, _) = session(venue, &server.url(), &cfg, &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        settle(|| ends(&watch).len() == 1).await;
        assert_eq!(peer.next().await, None);
        // Well past the pacing's floor, but not yet a second after the first frame.
        advance(ms(900)).await;
        churn().await;
        assert!(server.try_accept().is_none());
        advance(ms(200)).await;
        let mut next = server.accept().await;
        assert!(next.recv().await.starts_with("auth|ts="));
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.resyncs(), 1);
    assert_eq!(session.counters().arm_failures, 0);
}

/// A resync read the buckets admit goes out, charged once: with the frames, not again as it
/// starts. The account's three units a second take the authentication, the arm and the read
/// (PR #90 Reviewer B B7). On the real clock, so the read meets a local server.
#[tokio::test]
async fn a_resync_read_the_buckets_admit_goes_out_charged_only_once() {
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let url = http.url("/orders");
    let cfg = [
        (UNITS, "3"),
        (HTTP_RESYNC, "1"),
        (HTTP_RESYNC_URL, url.as_str()),
    ];
    let (mut session, control, _, _) = session(venue, &server.url(), &cfg, &log, None);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let exchange = tokio::time::timeout(Duration::from_secs(10), http.request());
        assert_eq!(
            exchange.await.expect("the read never came").line,
            "GET /orders"
        );
        assert_eq!(http.connections(), 1);
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    assert_eq!(venue.resyncs(), 1);
}

/// A venue's 429 or 418 to an order-entry session's HTTP request is counted under the scopes
/// the request was charged to (FBC-e8i, decision 0030): the toy's account limit charges the
/// resync read, so its 429 counts under the account, and nothing is counted as refused. On the
/// real clock, so the read meets a local server.
#[tokio::test]
async fn a_429_to_an_order_entry_read_is_counted_under_the_scope_it_was_charged_to() {
    let mut server = ScriptedWs::start().await;
    let mut http = ScriptedHttp::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let url = http.url("/orders");
    let cfg = [(HTTP_RESYNC, "1"), (HTTP_RESYNC_URL, url.as_str())];
    let (config, _) = setup(venue, &server.url(), &cfg);
    let rates = config.limiter.clone();
    let keep = Keep::new(&log, &Orders::default());
    let (mut session, control) = ExecSession::new(config, keep).unwrap();
    let watch = rates.clone();
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let exchange = tokio::time::timeout(Duration::from_secs(10), http.request());
        let exchange = exchange.await.expect("the read never came");
        exchange.answer("HTTP/1.1 429 Too Many Requests", "").await;
        for _ in 0..1_000 {
            if watch.counts().rejected.account == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    let account = ScopeCounts {
        account: 1,
        ..ScopeCounts::default()
    };
    let counts = rates.counts();
    assert_eq!(counts.rejected, account);
    assert_eq!(counts.refused, ScopeCounts::default());
}

/// A resync read that weighs more than the buckets ever admit ends the session, as a resync
/// frame's would (PR #90 Reviewer B B7).
#[tokio::test(start_paused = true)]
async fn a_resync_read_that_can_never_fit_ends_the_session() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let cfg = [(HTTP_RESYNC, "51")];
    let (mut session, _control, _, _) = session(venue, &server.url(), &cfg, &log, None);
    let ended = Rc::new(std::cell::Cell::new(false));
    let run = {
        let ended = Rc::clone(&ended);
        async move {
            let run = session.run().await;
            ended.set(true);
            run
        }
    };
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        peer.send(&accepted(1));
        settle(|| ended.get()).await;
        assert_eq!(peer.next().await, None);
    };
    let ((), run) = tokio::join!(script, run);
    drop(frozen);
    assert_eq!(run, Err(ExecSessionError::ResyncNeverFits));
    assert_eq!(venue.resyncs(), 1);
}

/// A venue whose protection outlives a reconnect is armed on the first epoch only: the next
/// epoch resyncs and is ready once its `ResyncEnd` reached the handler, with no arm of its own.
#[tokio::test(start_paused = true)]
async fn protection_not_re_armed_on_reconnect_is_armed_once_and_each_epoch_still_resyncs() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ReadyToy::leak();
    let log = Log::default();
    let (mut session, control, orders, _) =
        session(venue, &server.url(), &[(COD, "once")], &log, None);
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        peer.recv().await;
        peer.send(AUTH_ACK);
        assert_eq!(peer.recv().await, arm(1));
        let resync = peer.recv().await;
        peer.send(&accepted(1));
        peer.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 1).await;
        assert!(orders.may_place());
        peer.drop_conn();
        settle(|| ends(&watch).len() == 1).await;
        advance(ms(10)).await;
        let mut next = server.accept().await;
        next.recv().await;
        next.send(AUTH_ACK);
        let resync = next.recv().await;
        assert!(resync.starts_with("resync|ts="), "{resync}");
        assert!(!orders.may_place());
        next.send_all(resynced(watermark(&resync)));
        settle(|| resync_ends(&watch) == 2).await;
        assert!(orders.may_place());
        drop(control);
    };
    let ((), run) = tokio::join!(script, session.run());
    run.unwrap();
    drop(frozen);

    assert_eq!(venue.encoded(), [(arm_cmd(), RpcId(1))]);
    assert_eq!(venue.resyncs(), 2);
}

/// A venue declaring no cancel-on-disconnect, or a dead-man timer, is refused when the session
/// is built, naming what it declares; nothing is connected.
#[test]
fn a_venue_without_per_connection_protection_is_refused_at_start_naming_it() {
    let venue = ReadyToy::leak();
    let refused = |cod: &str| {
        let (config, _) = setup(venue, "ws://127.0.0.1:1/exec", &[(COD, cod)]);
        ExecSession::new(config, |_: Envelope<ExecEvent>| {}).err()
    };
    let none = refused("none").unwrap();
    assert_eq!(
        none,
        ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::None)
    );
    assert!(none.to_string().contains("None"), "{none}");
    let dead_man = refused("deadman").unwrap();
    let max_ttl = Duration::from_secs(10);
    assert_eq!(
        dead_man,
        ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::DeadMan { max_ttl })
    );
    assert!(dead_man.to_string().contains("DeadMan"), "{dead_man}");
    assert!(venue.calls().is_empty());
}

/// A session that never connected takes no places.
#[test]
fn a_session_not_yet_connected_takes_no_places() {
    let (config, _) = setup(ReadyToy::leak(), "ws://127.0.0.1:1/exec", &[]);
    let (session, _control) = ExecSession::new(config, |_: Envelope<ExecEvent>| {}).unwrap();
    assert!(!session.orders().may_place());
}
