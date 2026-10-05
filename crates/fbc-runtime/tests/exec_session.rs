//! FBC-oaz's done line: an order-entry session runs the conformance toy's order-entry codec
//! against a local WebSocket server; its `on_open` is called once per epoch with exactly the
//! nonces it asked for, its events are stamped in the shard's ingest order and handed to the
//! handler inline, an event of an ended epoch is dropped and counted, and a dropped connection
//! is reconnected through the consumer's pacing (decision 0052).

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};
use std::time::Duration;

use common::toy::ToyVenue;
use common::{Answer, ScriptedWs, Socks5Stub, refusing};
use exec_toy::{EXEC_STREAM, OWN_NS, TOY_TOKEN, ToyExec, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConnKey, ConnState, CtxCall, DecodeError, DecodeScope,
    Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec, ExecEndpoint,
    ExecEvent, ExecSink, FieldSpec, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse,
    HttpTag, Inbound, InboundSpans, InstrumentSpecDraft, LimitScope, MdCodec, MdTransport,
    ModeScope, MonoNs, NonceBlock, NonceSource, NotSentReason, OpKind, PathStamps, RateCharge,
    RateLimit, RawFrame, RpcId, Secrets, SpecTable, StreamId, Subscription, SymbolError, TagSet,
    TimerTag, TrafficClass, VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory,
    VenueMode, Via, WallNs, WireSlice, WireUrl,
};
use fbc_runtime::{
    BucketKey, Connector, ExecControl, ExecCounters, ExecHandler, ExecSession, ExecSessionConfig,
    ExecSessionError, IngestClock, Input, ProxyConfig, RateError, RateLimiter, ReconnectPacing,
    Request, SafetyReserve, SessionError, Step, WriteStall,
};
use futures_util::{FutureExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::Instant;

// ---------------------------------------------------------------------------------------------
// The venue: the conformance toy's order entry, planned on one endpoint.
// ---------------------------------------------------------------------------------------------

/// The endpoint's URL; required.
const URL: &str = "exec.url";
/// How many endpoints to plan; one by default.
const ENDPOINTS: &str = "exec.endpoints";
/// `refuse`: the caps are refused; `md-only`: they declare no `exec` block.
const CAPS: &str = "exec.caps";
/// `none`: no codec; `refuse`: the codec is refused; `plain`: the toy's codec as it is, asking
/// for no nonce. By default the toy's codec, recorded.
const CODEC: &str = "exec.codec";
/// Any value: the recorded codec's `on_open` also sends a frame heavier than the toy's limit.
const HEAVY: &str = "exec.heavy";
/// Any value: the recorded codec's `on_open` asks to reconnect after its frames, then for a
/// frame heavier than the toy's limit, which the reconnect leaves unreached.
const OPEN_BYE: &str = "exec.open_bye";
/// Any value: the venue also declares a per-connection limit, of `Control` and `Query`.
const CONN_LIMIT: &str = "exec.conn_limit";

/// Every `on_open` call the recorded codec saw: its stream and context.
type Opens = Arc<Mutex<Vec<(StreamId, EncodeCtx)>>>;

#[derive(Default)]
struct ExecToy {
    opens: Opens,
}

impl ExecToy {
    fn leak() -> &'static ExecToy {
        Box::leak(Box::default())
    }

    fn opens(&self) -> Vec<(StreamId, EncodeCtx)> {
        self.opens.lock().unwrap().clone()
    }
}

impl VenueFactory for ExecToy {
    fn id(&self) -> &'static str {
        "TOY-EXEC"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        match cfg.get(CAPS) {
            Some("refuse") => Err(ConfigError::Invalid {
                key: CAPS,
                reason: "refused",
            }),
            Some(_) => Ok(VenueCaps {
                exec: None,
                ..exec_toy::caps()
            }),
            None => {
                let mut caps = exec_toy::caps();
                if cfg.get(CONN_LIMIT).is_some() {
                    caps.limits.push(RateLimit {
                        scope: LimitScope::Connection,
                        ops: TagSet::of(&[OpKind::Control, OpKind::Query]),
                        per: Duration::from_secs(60),
                        units: 10,
                    });
                }
                Ok(caps)
            }
        }
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
        let url = cfg
            .get(URL)
            .ok_or(VenueError::Config(ConfigError::Missing(URL)))?;
        let n: u16 = cfg.get(ENDPOINTS).map_or(1, |n| n.parse().unwrap());
        let endpoint = |i: u16| ExecEndpoint {
            stream: StreamId(EXEC_STREAM.0 + i),
            url: WireUrl::plain(url),
        };
        Ok((0..n).map(endpoint).collect())
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let toy = ToyExec::new(Box::new(ToySigner));
        Some(Ok(match cfg.get(CODEC) {
            Some("none") => return None,
            Some("refuse") => return Some(Err(VenueError::Config(ConfigError::Missing("key")))),
            Some(_) => Box::new(toy),
            None => Box::new(Recording {
                inner: toy,
                opens: self.opens.clone(),
                heavy: cfg.get(HEAVY).is_some(),
                bye: cfg.get(OPEN_BYE).is_some(),
            }),
        }))
    }

    fn test_connection(
        &self,
        _: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}

/// The conformance toy's codec, recording each `on_open` and asking a resync after the
/// authentication. Its `on_open` asks for two nonces on the first epoch, three on the second,
/// and so on, so each epoch's reservation differs. Frames of the test's own: `stray`
/// asks for a timer, an HTTP request, and a frame and a reconnect on another stream, then
/// sends `said`; `bye` asks to reconnect its stream, then for a timer and an HTTP request,
/// which the reconnect leaves unreached; `halt` pushes a `ResyncBegin` and asks to send
/// `said`.
struct Recording {
    inner: ToyExec,
    opens: Opens,
    heavy: bool,
    bye: bool,
}

impl ExecCodec for Recording {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        match call {
            CtxCall::Open(_) => 2 + self.opens.lock().unwrap().len() as u16,
            other => self.inner.nonces_for(other),
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens.lock().unwrap().push((stream, ctx.clone()));
        self.inner.on_open(stream, ctx, fx);
        self.inner.resync(ctx, fx);
        if self.bye {
            let reason = "bye";
            fx.push(Effect::Reconnect { stream, reason });
        }
        if self.heavy || self.bye {
            fx.push(Effect::Send {
                stream,
                frame: WireSlice::plain(b"heavy".to_vec()),
                rpc: None,
                class: TrafficClass::Safety,
                charge: RateCharge {
                    weight: NonZeroU32::new(51).unwrap(),
                    ..RateCharge::one(OpKind::Control, None)
                },
            });
        }
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
        self.inner.encode(cmd, rpc, specs, ctx, t, fx)
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
        let other = StreamId(9);
        let send = |stream| Effect::Send {
            stream,
            frame: WireSlice::plain(b"said".to_vec()),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Control, None),
        };
        // A timer and an HTTP request, which the session refuses.
        let unsupported = |fx: &mut Effects| {
            fx.push(Effect::Timer {
                tag: TimerTag(1),
                after: Duration::ZERO,
            });
            fx.push(Effect::Http {
                tag: HttpTag(1),
                req: HttpRequest {
                    method: HttpMethod::Get,
                    url: WireUrl::plain("http://127.0.0.1:1/"),
                    headers: Vec::new(),
                    body: WireSlice::plain(Vec::new()),
                },
                rpc: None,
                timeout: Duration::from_secs(1),
                class: TrafficClass::Normal,
                charge: RateCharge::one(OpKind::Query, None),
            });
        };
        match f {
            RawFrame::Text("stray") => {
                unsupported(fx);
                fx.push(send(other));
                let reason = "stray";
                fx.push(Effect::Reconnect {
                    stream: other,
                    reason,
                });
                fx.push(send(stream));
                Ok(())
            }
            RawFrame::Text("halt") => {
                let begin = ExecEvent::ResyncBegin {
                    watermark: WallNs(0),
                };
                sink.push(fbc_core::VenueMeta::NONE, begin);
                fx.push(send(stream));
                Ok(())
            }
            RawFrame::Text("bye") => {
                let reason = "bye";
                fx.push(Effect::Reconnect { stream, reason });
                unsupported(fx);
                Ok(())
            }
            _ => self.inner.on_frame(stream, f, scope, specs, sink, fx),
        }
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
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

/// Nonces counted up from 0, each reservation logged; a short source reserves one fewer than
/// asked.
struct Counting {
    next: u64,
    short: bool,
    log: Arc<Mutex<Vec<Vec<u64>>>>,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let len = len - u16::from(self.short);
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        self.log.lock().unwrap().push(block.as_slice().to_vec());
        block
    }
}

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

/// The connection number the tests stamp the session with.
const CONN: u16 = 3;

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A fast floor, so a drop reconnects at once in real time.
fn quick() -> ReconnectPacing {
    ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), ms(5_000)).unwrap()
}

fn limiter() -> RateLimiter {
    let limits = exec_toy::caps().limits;
    RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap()
}

/// A session of `venue` at `url`, directly, paced by `pacing`; its nonce reservations logged.
fn setup(
    venue: &'static ExecToy,
    url: &str,
    pacing: ReconnectPacing,
) -> (ExecSessionConfig, Arc<Mutex<Vec<Vec<u64>>>>) {
    let log = Arc::default();
    let mut cfg = VenueConfig::new();
    cfg.insert(URL, url);
    let nonces = Counting {
        next: 0,
        short: false,
        log: Arc::clone(&log),
    };
    let config = ExecSessionConfig {
        venue,
        cfg,
        creds: Secrets::new(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        conn: CONN,
        limiter: limiter(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    (config, log)
}

/// What the handler heard: an event, with the thread it arrived on, or an epoch's end.
#[derive(Debug)]
enum Heard {
    Event(ThreadId, Box<Envelope<ExecEvent>>),
    End(ConnKey),
}

type Log = Rc<RefCell<Vec<Heard>>>;

/// A handler that keeps what it hears; holding the session's control, it drops it on the
/// first `ResyncBegin`.
struct Keep {
    log: Log,
    control: Rc<RefCell<Option<ExecControl>>>,
}

impl Keep {
    fn new(log: &Log) -> Keep {
        Keep {
            log: Rc::clone(log),
            control: Rc::default(),
        }
    }
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        if matches!(env.body, ExecEvent::ResyncBegin { .. }) {
            drop(self.control.borrow_mut().take());
        }
        let heard = Heard::Event(thread::current().id(), Box::new(env));
        self.log.borrow_mut().push(heard);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::End(key));
    }
}

/// How many events the handler has heard.
fn events(log: &Log) -> usize {
    let log = log.borrow();
    log.iter().filter(|h| matches!(h, Heard::Event(..))).count()
}

/// Waits, in real time, until `done` holds.
async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

/// The frames the toy's `on_open` sends with `ctx`: its authentication, then (recorded) the
/// resync.
fn auth(ctx: &EncodeCtx) -> String {
    format!("auth|ts={}|token={TOY_TOKEN}", ctx.wall.0)
}

const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";

fn mode(scope: ModeScope, mode: VenueMode) -> ExecEvent {
    ExecEvent::Mode { scope, mode }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn on_open_gets_exactly_its_nonces_each_epoch_and_events_arrive_stamped_inline_in_order() {
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (config, reserved) = setup(venue, &server.url(), quick());
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut first = server.accept().await;
        let (opened, resync) = (first.recv().await, first.recv().await);
        first.send(AUTH_ACK);
        first.send("mode|m=halted");
        first.ping();
        first.send("mode|m=postonly|sym=TOYA-PERP|seq=4");
        first.send_binary(b"\xff");
        until(|| events(&watch) == 3).await;
        first.drop_conn();

        let mut second = server.accept().await;
        let (reopened, _) = (second.recv().await, second.recv().await);
        second.send("mode|m=reduceonly");
        until(|| events(&watch) == 4).await;
        drop(control);
        // The session closed the connection having sent nothing more.
        assert_eq!(second.next().await, None);
        (opened, resync, reopened)
    };
    let (run, (opened, resync, reopened)) = tokio::join!(session.run(), script);
    run.unwrap();

    // One `on_open` per epoch, each with exactly the nonces it asked for (two, then three),
    // reserved from the consumer's source and nothing else, and the clock's time.
    let opens = venue.opens();
    let blocks: Vec<_> = opens.iter().map(|(_, ctx)| ctx.nonces.as_slice()).collect();
    assert_eq!(blocks, [&[0, 1][..], &[2, 3, 4][..]]);
    assert_eq!(*reserved.lock().unwrap(), [vec![0, 1], vec![2, 3, 4]]);
    assert!(opens.iter().all(|(stream, _)| *stream == EXEC_STREAM));
    assert!(opens[0].1.mono <= opens[1].1.mono);
    // What on_open sent carries its context's time: the authentication and the resync.
    let wall = opens[0].1.wall.0;
    assert_eq!(opened, auth(&opens[0].1));
    assert_eq!(resync, format!("resync|ts={wall}"));
    assert_eq!(reopened, auth(&opens[1].1));

    // Every event on the session's thread, in ingest order, each epoch's end after its last
    // event and before the next epoch's first.
    let here = thread::current().id();
    let log = log.borrow();
    let mut envs = Vec::new();
    let mut ends = Vec::new();
    for heard in log.iter() {
        match heard {
            Heard::Event(thread, env) => {
                assert_eq!(*thread, here);
                envs.push(env);
            }
            Heard::End(key) => ends.push((envs.len(), *key)),
        }
    }
    assert_eq!(ends, [(3, key(0)), (4, key(1))]);
    let conns: Vec<_> = envs.iter().map(|e| e.stamp.conn).collect();
    assert_eq!(conns, [key(0), key(0), key(0), key(1)]);
    // The ping, the undecodable frame and the close frame took their places too.
    let ingest: Vec<_> = envs.iter().map(|e| e.stamp.ingest_seq).collect();
    assert_eq!(ingest, [0, 1, 3, 6]);
    assert!(
        envs.windows(2)
            .all(|w| w[0].stamp.recv_mono <= w[1].stamp.recv_mono)
    );
    let bodies: Vec<_> = envs.iter().map(|e| e.body.clone()).collect();
    let authenticated = ExecEvent::Conn {
        stream: EXEC_STREAM,
        state: ConnState::Authenticated,
    };
    let toya = ModeScope::Instrument(exec_toy::INST_A);
    assert_eq!(
        bodies,
        [
            authenticated,
            mode(ModeScope::Account, VenueMode::Halted),
            mode(toya, VenueMode::PostOnly),
            mode(ModeScope::Account, VenueMode::ReduceOnly),
        ]
    );
    // The venue's sequence, decoded through the scope the venue's caps lend.
    assert_eq!(envs[2].venue_seq, Some(4));
    assert_eq!(session.current(), key(1));
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.failed_attempts), (2, 0));
    assert_eq!(counters.decode_errors, 1);
    assert_eq!(session.stale(Input::Event), 0);
}

#[tokio::test]
async fn an_event_pushed_once_the_handler_stopped_the_session_is_of_an_ended_epoch_and_dropped() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let url = format!("ws://{}/exec", listener.local_addr().unwrap());
    let (config, _) = setup(ExecToy::leak(), &url, quick());
    let log = Log::default();
    let handler = Keep::new(&log);
    let slot = Rc::clone(&handler.control);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *slot.borrow_mut() = Some(control);
    let script = async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = ws.next().await;
        let resync = ws.next().await.unwrap().unwrap();
        let wm = resync
            .to_text()
            .unwrap()
            .strip_prefix("resync|ts=")
            .unwrap();
        // The resync's frames and one more, in one write, so all four wait together. The whole
        // resync is pushed with its end, in one call: the handler stops the session on its
        // first event, so the other two are of an ended epoch, and the frame behind them
        // reaches no codec.
        let frames = [
            format!("rsbegin|wm={wm}"),
            "rspos|sym=TOYA-PERP|qty=-12|avg=65432.5".to_owned(),
            "rsend".to_owned(),
            "mode|m=halted".to_owned(),
        ];
        let mut bytes = Vec::new();
        for text in frames {
            // A final, unmasked text frame under 126 bytes, as a server sends it.
            bytes.extend([0x81, u8::try_from(text.len()).unwrap()]);
            bytes.extend(text.as_bytes());
        }
        ws.get_mut().write_all(&bytes).await.unwrap();
        // The session closes the connection having sent nothing more.
        while let Some(Ok(message)) = ws.next().await {
            assert!(message.is_close(), "{message:?}");
        }
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let log = log.borrow();
    assert_eq!(log.len(), 2, "{log:?}");
    assert!(matches!(
        &log[0],
        Heard::Event(_, env) if matches!(env.body, ExecEvent::ResyncBegin { .. })
    ));
    assert!(matches!(log[1], Heard::End(k) if k == key(0)));
    assert_eq!(session.stale(Input::Event), 2);
    // A stop, not a drop: no epoch opened after it.
    assert_eq!(session.current(), key(0));
    assert_eq!(session.counters().attempts, 1);
}

#[tokio::test]
async fn a_dropped_connection_reconnects_no_sooner_than_the_consumers_floor() {
    let mut server = ScriptedWs::start().await;
    let floor = ReconnectPacing::new(ms(300), ms(300), 100, Duration::from_secs(60), ms(5_000));
    let (config, _) = setup(ExecToy::leak(), &server.url(), floor.unwrap());
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let _ = (first.recv().await, first.recv().await);
        let dropped = Instant::now();
        first.drop_conn();
        let mut second = server.accept().await;
        let waited = dropped.elapsed();
        let _ = (second.recv().await, second.recv().await);
        drop(control);
        waited
    };
    let (run, waited) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(waited >= ms(300), "reconnected after {waited:?}");
    assert_eq!(session.counters().attempts, 2);
    assert_eq!(session.current(), key(1));
}

#[tokio::test(start_paused = true)]
async fn a_refusing_server_sees_attempts_spaced_by_the_backoff_and_within_the_budget() {
    let (addr, mut accepts) = refusing().await;
    // No attempt deadline: on paused time a pending deadline would let the clock jump ahead
    // while the attempt's socket I/O is still under way.
    let pacing = ReconnectPacing::new(ms(1_000), ms(4_000), 3, ms(10_000), Duration::MAX);
    let (config, _) = setup(
        ExecToy::leak(),
        &format!("ws://{addr}/exec"),
        pacing.unwrap(),
    );
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut at = Vec::new();
        while at.len() < 6 {
            at.push(accepts.recv().await.unwrap());
        }
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    let since: Vec<_> = at.iter().map(|t| t.duration_since(at[0])).collect();
    // Backoff 1, 2, then 4 s, held to 10 s by a budget of 3 per 10 s, then 4 s at the ceiling.
    assert_eq!(since, [0, 1_000, 3_000, 10_000, 14_000, 18_000].map(ms));
    let ExecCounters {
        attempts,
        failed_attempts,
        ..
    } = session.counters();
    assert_eq!(attempts, 6);
    assert!(failed_attempts >= 5);
}

// ---------------------------------------------------------------------------------------------
// Effects, the proxy, and what a session refuses.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn stray_effects_are_refused_and_a_reconnect_the_codec_asks_for_opens_the_next_epoch() {
    let mut server = ScriptedWs::start().await;
    let (config, _) = setup(ExecToy::leak(), &server.url(), quick());
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let _ = (first.recv().await, first.recv().await);
        first.send("stray");
        // Only the frame for the session's own stream went out.
        assert_eq!(first.recv().await, "said");
        first.send("bye");
        assert_eq!(first.next().await, None);
        let mut second = server.accept().await;
        let _ = (second.recv().await, second.recv().await);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // A timer, an HTTP request, and a frame and a reconnect for another stream; not the timer
    // and request behind the reconnect, which ended the epoch before their turn (Codex
    // r4188639448).
    assert_eq!(session.counters().refused_effects, 4);
    assert_eq!(session.current(), key(1));
    let ends: Vec<_> = log
        .borrow()
        .iter()
        .filter_map(|h| match h {
            Heard::End(k) => Some(*k),
            Heard::Event(..) => None,
        })
        .collect();
    assert_eq!(ends, [key(0), key(1)]);
}

#[tokio::test]
async fn the_endpoint_opens_through_the_consumers_socks5_proxy_by_name() {
    let mut server = ScriptedWs::start().await;
    let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let stub = Socks5Stub::start(&[("exec.venue.test", local)], Answer::Relay).await;
    let port = server.addr.port();
    let url = format!("ws://exec.venue.test:{port}/exec");
    let (mut config, reserved) = setup(ExecToy::leak(), &url, quick());
    config.cfg.insert(CODEC, "plain");
    let host = stub.addr.ip().to_string();
    config.connector = Connector::new(ProxyConfig::Socks5 {
        host,
        port: stub.addr.port(),
    });
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let opened = peer.recv().await;
        drop(control);
        opened
    };
    let (run, opened) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(opened.starts_with("auth|ts="), "{opened}");
    let targets = stub.targets();
    assert_eq!(targets.len(), 1);
    assert_eq!(
        (targets[0].host.as_str(), targets[0].port),
        ("exec.venue.test", port)
    );
    // The toy's own `on_open` asks for no nonce: none was reserved.
    assert!(reserved.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_nonce_source_that_reserves_another_count_ends_the_session_which_runs_once() {
    let mut server = ScriptedWs::start().await;
    let (mut config, _) = setup(ExecToy::leak(), &server.url(), quick());
    let log = Arc::default();
    config.nonces = Box::new(Counting {
        next: 0,
        short: true,
        log,
    });
    let heard = Log::default();
    let (mut session, _control) = ExecSession::new(config, Keep::new(&heard)).unwrap();
    let script = async {
        let mut peer = server.accept().await;
        // Nothing was sent on the connection.
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    let short = ExecSessionError::Nonces {
        asked: 2,
        reserved: 1,
    };
    assert_eq!(run.err(), Some(short));
    // Its epoch was retired (Codex r4188802893), and the session runs once: a second call
    // connects nothing (Codex r4189174493, r4189174502).
    assert_eq!(session.current(), key(1));
    assert_eq!(session.run().await.err(), Some(ExecSessionError::Ended));
    assert!(server.try_accept().is_none());
    assert_eq!(session.counters().attempts, 1);
    assert!(matches!(heard.borrow()[..], [Heard::End(k)] if k == key(0)));
}

#[tokio::test]
async fn a_write_the_codec_asks_for_with_the_event_that_stopped_the_session_is_not_sent() {
    let mut server = ScriptedWs::start().await;
    let (config, _) = setup(ExecToy::leak(), &server.url(), quick());
    let log = Log::default();
    let handler = Keep::new(&log);
    let slot = Rc::clone(&handler.control);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *slot.borrow_mut() = Some(control);
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        peer.send("halt");
        // The handler stopped the session on the frame's event: its `said` never went out
        // (Codex r4188802881).
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(log.borrow().len(), 2);
    assert_eq!(session.current(), key(0));
}

#[tokio::test]
async fn on_open_frames_the_buckets_refuse_for_now_end_the_epoch_and_go_once_there_is_room() {
    let mut server = ScriptedWs::start().await;
    let pacing = ReconnectPacing::new(ms(300), ms(300), 100, Duration::from_secs(60), ms(5_000));
    let (config, _) = setup(ExecToy::leak(), &server.url(), pacing.unwrap());
    // The account's whole budget of 50 a second, taken by another session sharing the limiter.
    let shared = config.limiter.clone();
    let taken = Request {
        charge: RateCharge {
            weight: NonZeroU32::new(50).unwrap(),
            ..RateCharge::one(OpKind::Control, None)
        },
        via: Via::Frame,
        class: TrafficClass::Safety,
    };
    let other = ConnKey { conn: 99, epoch: 0 };
    shared.charge(Instant::now(), other, &[taken]).unwrap();
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        // Each epoch opened while the budget is spent sends nothing and is dropped, to try
        // again after the floor (Codex r4188802873); once the second is out, one goes.
        let mut empty = 0;
        loop {
            let mut peer = server.accept().await;
            match peer.next().await {
                None => empty += 1,
                Some(opened) => {
                    assert!(opened.starts_with("auth|ts="), "{opened}");
                    assert!(peer.recv().await.starts_with("resync|ts="));
                    break;
                }
            }
        }
        drop(control);
        empty
    };
    let (run, empty) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(empty >= 1);
    assert!(shared.counts().refused.account >= 1);
}

#[tokio::test]
async fn nothing_behind_a_reconnect_on_open_asks_for_is_charged() {
    let mut server = ScriptedWs::start().await;
    let (mut config, _) = setup(ExecToy::leak(), &server.url(), quick());
    // The heavy frame behind the reconnect would never fit; it is never reached, so it is not
    // charged and every epoch opens (Codex r4188995331).
    config.cfg.insert(OPEN_BYE, "yes");
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        for _ in 0..2 {
            let mut peer = server.accept().await;
            assert!(peer.recv().await.starts_with("auth|ts="));
            assert!(peer.recv().await.starts_with("resync|ts="));
            assert_eq!(peer.next().await, None);
        }
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(session.current().epoch >= 2);
}

#[tokio::test]
async fn a_run_dropped_mid_epoch_has_its_epoch_ended_by_the_next_call_or_the_sessions_drop() {
    let mut server = ScriptedWs::start().await;
    for rerun in [true, false] {
        let venue = ExecToy::leak();
        let (mut config, _) = setup(venue, &server.url(), quick());
        // A per-connection limit, so the dropped run's epoch has buckets to forget.
        config.cfg.insert(CONN_LIMIT, "yes");
        let limits = venue.caps(&config.cfg).unwrap().limits;
        config.limiter = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
        let shared = config.limiter.clone();
        let charged = move || shared.used(Instant::now(), 1, BucketKey::Connection(key(0)));
        let log = Log::default();
        let (mut session, _control) = ExecSession::new(config, Keep::new(&log)).unwrap();
        let mut peer = tokio::select! {
            _ = session.run() => panic!("the run ended"),
            peer = async {
                let mut peer = server.accept().await;
                let _ = (peer.recv().await, peer.recv().await);
                peer
            } => peer,
        };
        // The dropped run's socket closed, but its epoch is not yet told ended, and its
        // authentication and resync still count in its bucket.
        assert_eq!(peer.next().await, None);
        assert!(log.borrow().is_empty());
        assert_eq!(charged(), 2);
        if rerun {
            // A session runs once: the next call ends the left epoch and connects nothing
            // (Codex r4188995359).
            assert_eq!(session.run().await.err(), Some(ExecSessionError::Ended));
            assert!(server.try_accept().is_none());
        }
        // Dropping the session ends it otherwise (Codex r4189174470); never twice.
        drop(session);
        assert!(matches!(log.borrow()[..], [Heard::End(k)] if k == key(0)));
        // Its bucket is forgotten with it (Codex r4189428438).
        assert_eq!(charged(), 0);
    }
}

/// A handler whose `on_epoch_end` panics, counting its calls.
struct Panicking(Rc<std::cell::Cell<u32>>);

impl ExecHandler for Panicking {
    fn on_exec(&mut self, _: Envelope<ExecEvent>) {}

    fn on_epoch_end(&mut self, _: ConnKey) {
        self.0.set(self.0.get() + 1);
        panic!("the handler failed");
    }
}

#[tokio::test]
async fn a_handler_that_panics_as_it_is_told_an_epoch_ended_is_never_told_twice() {
    let mut server = ScriptedWs::start().await;
    let (config, _) = setup(ExecToy::leak(), &server.url(), quick());
    let ends = Rc::default();
    let (mut session, _control) = ExecSession::new(config, Panicking(Rc::clone(&ends))).unwrap();
    let script = async move {
        let peer = server.accept().await;
        peer.drop_conn();
    };
    let run = std::panic::AssertUnwindSafe(session.run()).catch_unwind();
    let (run, ()) = tokio::join!(run, script);
    assert!(run.is_err());
    // Dropping the session after the panic tells it nothing again (Codex r4189618551).
    drop(session);
    assert_eq!(ends.get(), 1);
}

#[tokio::test]
async fn on_open_frames_that_can_never_fit_together_end_the_session() {
    let mut server = ScriptedWs::start().await;
    let (mut config, _) = setup(ExecToy::leak(), &server.url(), quick());
    config.cfg.insert(HEAVY, "yes");
    let (mut session, _control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    assert_eq!(run.err(), Some(ExecSessionError::OpenNeverFits));
}

#[test]
fn a_session_needs_a_venue_that_takes_orders_on_one_connection_it_can_open() {
    let refused = |key: &str, value: &str| {
        let (mut config, _) = setup(ExecToy::leak(), "ws://127.0.0.1:1/exec", quick());
        config.cfg.insert(key, value);
        ExecSession::new(config, |_| {}).err().unwrap()
    };
    let caps = ConfigError::Invalid {
        key: CAPS,
        reason: "refused",
    };
    assert_eq!(
        refused(CAPS, "refuse"),
        ExecSessionError::Session(SessionError::Config(caps))
    );
    assert_eq!(refused(CAPS, "md-only"), ExecSessionError::NoOrderEntry);
    assert_eq!(refused(ENDPOINTS, "0"), ExecSessionError::Endpoints(0));
    assert_eq!(refused(ENDPOINTS, "2"), ExecSessionError::Endpoints(2));
    let err = refused(URL, "http://127.0.0.1:1/");
    assert!(
        matches!(&err, ExecSessionError::Session(SessionError::Url(e)) if e.step() == Step::Url)
    );
    assert_eq!(refused(CODEC, "none"), ExecSessionError::NoOrderEntry);
    let missing = VenueError::Config(ConfigError::Missing("key"));
    assert_eq!(refused(CODEC, "refuse"), ExecSessionError::Venue(missing));

    let (mut config, _) = setup(ExecToy::leak(), "ws://127.0.0.1:1/exec", quick());
    config.cfg = VenueConfig::new();
    let err = ExecSession::new(config, |_| {}).err().unwrap();
    let missing = VenueError::Config(ConfigError::Missing(URL));
    assert_eq!(err, ExecSessionError::Venue(missing));

    // A limiter built for other limits than the venue declares.
    let (mut config, _) = setup(ExecToy::leak(), "ws://127.0.0.1:1/exec", quick());
    config.limiter = RateLimiter::new(&[], SafetyReserve::percent(0).unwrap()).unwrap();
    let err = ExecSession::new(config, |_| {}).err().unwrap();
    assert!(matches!(
        err,
        ExecSessionError::Session(SessionError::Rates(RateError::OtherLimits))
    ));
}

/// The test's venue answers as an order-entry venue only, and its recorded codec passes every
/// call it does not record to the toy's.
#[test]
fn the_exec_toy_is_the_conformance_toys_order_entry_and_nothing_else() {
    let venue = ExecToy::leak();
    let cfg = VenueConfig::new();
    assert_eq!(venue.id(), "TOY-EXEC");
    assert!(venue.config_schema().is_empty());
    assert!(venue.parse_fbc_common_symbol("A/USDT").is_err());
    assert!(matches!(venue.discover(&cfg), Err(VenueError::NoDiscovery)));
    let specs = exec_toy::specs();
    assert_eq!(
        venue.plan_md(&cfg, &specs, &BTreeSet::new()),
        Ok(Vec::new())
    );
    let plan = EndpointPlan {
        stream: StreamId(0),
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1:1/"),
        },
        subs: Vec::new(),
    };
    assert!(venue.md_codec(&cfg, &plan).keepalive().is_none());
    assert!(venue.test_connection(&cfg, Secrets::new()).is_none());

    let mut codec = venue.exec_codec(&cfg, Secrets::new()).unwrap().unwrap();
    let ctx = EncodeCtx {
        wall: WallNs(1),
        mono: MonoNs(1),
        nonces: NonceBlock::EMPTY,
    };
    let mut fx = Effects::new();
    assert_eq!(codec.nonces_for(CtxCall::Resync), 0);
    let cmd = VenueCommand::FeeQuery;
    let mut stamps = PathStamps::off();
    let sent = codec.encode(&cmd, RpcId(1), &specs, &ctx, &mut stamps, &mut fx);
    assert!(sent.is_ok());
    let mut pushed = Vec::new();
    let mut sink = |_: fbc_core::VenueMeta, ev: ExecEvent| pushed.push(ev);
    let mut sink = SinkFn(&mut sink);
    let answered = exec_toy::with_scope(|scope| {
        let resp = Err(HttpFailure::NotSent);
        codec.on_http(HttpTag(0), resp, scope, &specs, &mut sink, &mut fx)
    });
    assert!(answered.is_err());
    codec.on_timer(TimerTag(0), &ctx, &mut fx);
    codec.on_rpc_timeout(RpcId(1), &mut sink);
    codec.resync(&ctx, &mut fx);
    let frame = Inbound::Frame(RawFrame::Text("x"));
    assert_eq!(codec.redact_inbound(frame), InboundSpans::NONE);
    // The fee query, then the resync.
    assert_eq!(fx.take().len(), 2);
    assert_eq!(pushed.len(), 1);
}

/// An [`ExecSink`] that is a function.
struct SinkFn<'a>(&'a mut dyn FnMut(fbc_core::VenueMeta, ExecEvent));

impl ExecSink for SinkFn<'_> {
    fn push(&mut self, meta: fbc_core::VenueMeta, ev: ExecEvent) {
        (self.0)(meta, ev)
    }
}
