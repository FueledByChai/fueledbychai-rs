//! FBC-oaz's done line: an order-entry session runs the conformance toy's order-entry codec
//! against a local WebSocket server; its `on_open` is called once per epoch with exactly the
//! nonces it asked for, its events are stamped in the shard's ingest order and handed to the
//! handler inline, an event of an ended epoch is dropped and counted, and a dropped connection
//! is reconnected through the consumer's pacing (decision 0053).
//!
//! FBC-bnl's done line, against local WebSocket and HTTP servers: the codec's HTTP result is
//! answered only to the epoch that asked, a timer of an ended epoch is dropped and counted,
//! `on_timer` is called with exactly the nonces it asked for, and its keepalive (a timer it
//! arms on open and again on each firing) is sent at its declared interval (decision 0056).

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};
use std::time::Duration;

use common::toy::ToyVenue;
use common::{Answer, ScriptedHttp, ScriptedWs, Socks5Stub, refusing};
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
use tokio::time::{Instant, advance};

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
/// The base URL the recorded codec's HTTP requests go to.
const HTTP: &str = "exec.http";
/// Milliseconds: the recorded codec keeps its stream alive with a `ping` frame this often, a
/// timer it arms on open and again on each firing.
const KEEPALIVE: &str = "exec.keepalive";

/// The tag of the recorded codec's keepalive timer.
const PING: TimerTag = TimerTag(99);

/// Every `on_open` call the recorded codec saw: its stream and context.
type Opens = Arc<Mutex<Vec<(StreamId, EncodeCtx)>>>;
/// Every `on_timer` call the recorded codec saw: its tag and context.
type Timers = Arc<Mutex<Vec<(TimerTag, EncodeCtx)>>>;
/// Every HTTP result the recorded codec was handed for a request it asked for:
/// `<tag>:<status>:<body>`, or `<tag>:<failure>`.
type Answers = Arc<Mutex<Vec<String>>>;
/// The recorded codec's inputs of the test's own, in the order it took them: `noise` for each
/// `noise` frame, `timer <n>` for each firing of timer `n` and `http <n>` for the result of
/// each request `n` it asked for.
type Inputs = Arc<Mutex<Vec<String>>>;

#[derive(Default)]
struct ExecToy {
    opens: Opens,
    timers: Timers,
    answers: Answers,
    inputs: Inputs,
}

impl ExecToy {
    fn leak() -> &'static ExecToy {
        Box::leak(Box::default())
    }

    fn opens(&self) -> Vec<(StreamId, EncodeCtx)> {
        self.opens.lock().unwrap().clone()
    }

    fn timers(&self) -> Vec<(TimerTag, EncodeCtx)> {
        self.timers.lock().unwrap().clone()
    }

    fn answers(&self) -> Vec<String> {
        self.answers.lock().unwrap().clone()
    }

    fn inputs(&self) -> Vec<String> {
        self.inputs.lock().unwrap().clone()
    }

    /// How many `noise` frames the codec has decoded.
    fn noise(&self) -> usize {
        let inputs = self.inputs.lock().unwrap();
        inputs.iter().filter(|input| *input == "noise").count()
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
                timers: self.timers.clone(),
                answers: self.answers.clone(),
                inputs: self.inputs.clone(),
                asked: BTreeSet::new(),
                http: cfg.get(HTTP).map(str::to_owned),
                keepalive: cfg.get(KEEPALIVE).map(|n| ms(n.parse().unwrap())),
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

/// The conformance toy's codec, recording each `on_open`, `on_timer` and HTTP result, and asking
/// a resync after the authentication. Its `on_open` asks for two nonces on the first epoch,
/// three on the second, and so on, and its `on_timer` for one on its first call, two on its
/// second, and so on, so each reservation differs. With a keepalive it arms [`PING`] on open,
/// and sends `ping` and arms it again on each firing; any other timer sends `rang|tag=<n>`.
/// Frames of the test's own: `stray` asks for a timer, an HTTP request, and a frame and a
/// reconnect on another stream, then sends `said`; `bye` asks to reconnect its stream, then for
/// a timer and an HTTP request, which the reconnect leaves unreached; `halt` pushes a
/// `ResyncBegin` and asks to send `said`; `arm|tag=<n>|ms=<t>` sets timer `n` to fire in `t`
/// ms; `ask|tag=<n>|path=<p>` asks for a GET of `p` under the configured base URL, whose result
/// it records, pushes as an account `Halted` mode and acknowledges with `got|tag=<n>`, unless
/// its body is `stop|ask|tag=<m>|path=<q>`: then it pushes a `ResyncBegin` instead and asks for
/// request `m` too; `big|kb=<n>` asks to send an `n` KiB frame, then, with `|then=said`, to
/// send `said`, or with `|then=bye`, to reconnect its stream; `noise...` asks for nothing.
/// `stray`'s request goes to the configured base URL when there is one.
struct Recording {
    inner: ToyExec,
    opens: Opens,
    timers: Timers,
    answers: Answers,
    inputs: Inputs,
    /// The tags of the requests `ask` asked for; any other result goes to the toy's codec.
    asked: BTreeSet<HttpTag>,
    http: Option<String>,
    keepalive: Option<Duration>,
    heavy: bool,
    bye: bool,
}

/// The value of `name` in a test frame `kind|name=value|...`.
fn field<'a>(text: &'a str, name: &str) -> &'a str {
    let found = text.split('|').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == name).then_some(value)
    });
    found.unwrap_or_else(|| panic!("{name} in {text}"))
}

fn tag_of(text: &str) -> u64 {
    field(text, "tag").parse().unwrap()
}

impl Recording {
    /// A GET of `path` under the configured base URL, tagged `tag`, whose result is recorded.
    fn get(&mut self, tag: HttpTag, path: &str) -> Effect {
        let base = self.http.as_deref().unwrap();
        let url = WireUrl::plain(format!("{base}{path}"));
        self.asked.insert(tag);
        Effect::Http {
            tag,
            req: HttpRequest {
                method: HttpMethod::Get,
                url,
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: Duration::from_secs(5),
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Query, None),
        }
    }
}

impl ExecCodec for Recording {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        match call {
            CtxCall::Open(_) => 2 + self.opens.lock().unwrap().len() as u16,
            CtxCall::Timer(_) => 1 + self.timers.lock().unwrap().len() as u16,
            other => self.inner.nonces_for(other),
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens.lock().unwrap().push((stream, ctx.clone()));
        self.inner.on_open(stream, ctx, fx);
        self.inner.resync(ctx, fx);
        if let Some(after) = self.keepalive {
            fx.push(Effect::Timer { tag: PING, after });
        }
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
        // A timer and an HTTP request, which the session executes (FBC-bnl). The request goes
        // to the configured base URL when there is one, so its result is the test's own
        // (Reviewer B, B7); `bye`'s is never reached.
        let base = self.http.as_deref().unwrap_or("http://127.0.0.1:1");
        let stray_url = format!("{base}/stray");
        let unsupported = |fx: &mut Effects| {
            fx.push(Effect::Timer {
                tag: TimerTag(1),
                after: Duration::ZERO,
            });
            fx.push(Effect::Http {
                tag: HttpTag(1),
                req: HttpRequest {
                    method: HttpMethod::Get,
                    url: WireUrl::plain(stray_url.clone()),
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
            RawFrame::Text(text) if text.starts_with("arm|") => {
                let tag = TimerTag(tag_of(text));
                let after = ms(field(text, "ms").parse().unwrap());
                fx.push(Effect::Timer { tag, after });
                Ok(())
            }
            RawFrame::Text(text) if text.starts_with("ask|") => {
                let ask = self.get(HttpTag(tag_of(text)), field(text, "path"));
                fx.push(ask);
                Ok(())
            }
            RawFrame::Text(text) if text.starts_with("noise") => {
                self.inputs.lock().unwrap().push("noise".to_owned());
                Ok(())
            }
            RawFrame::Text(text) if text.starts_with("big|") => {
                let kb: usize = field(text, "kb").parse().unwrap();
                fx.push(Effect::Send {
                    stream,
                    frame: WireSlice::plain(vec![b'x'; kb * 1024]),
                    rpc: None,
                    class: TrafficClass::Normal,
                    charge: RateCharge::one(OpKind::Control, None),
                });
                let then = text.split('|').find_map(|part| part.strip_prefix("then="));
                match then {
                    Some("said") => fx.push(send(stream)),
                    Some("bye") => fx.push(Effect::Reconnect {
                        stream,
                        reason: "bye",
                    }),
                    _ => {}
                }
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
        if !self.asked.remove(&tag) {
            return self.inner.on_http(tag, resp, scope, specs, sink, fx);
        }
        self.inputs.lock().unwrap().push(format!("http {}", tag.0));
        let body = resp
            .as_ref()
            .ok()
            .map(|r| String::from_utf8_lossy(r.body).into_owned());
        let answer = match resp {
            Ok(r) => format!("{}:{}:{}", tag.0, r.status, String::from_utf8_lossy(r.body)),
            Err(failure) => format!("{}:{failure:?}", tag.0),
        };
        self.answers.lock().unwrap().push(answer);
        if let Some(then) = body.as_deref().and_then(|b| b.strip_prefix("stop|")) {
            let begin = ExecEvent::ResyncBegin {
                watermark: WallNs(0),
            };
            sink.push(fbc_core::VenueMeta::NONE, begin);
            let ask = self.get(HttpTag(tag_of(then)), field(then, "path"));
            fx.push(ask);
        } else {
            let halted = mode(ModeScope::Account, VenueMode::Halted);
            sink.push(fbc_core::VenueMeta::NONE, halted);
        }
        fx.push(Effect::Send {
            stream: EXEC_STREAM,
            frame: WireSlice::plain(format!("got|tag={}", tag.0).into_bytes()),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Control, None),
        });
        Ok(())
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.timers.lock().unwrap().push((tag, ctx.clone()));
        self.inputs.lock().unwrap().push(format!("timer {}", tag.0));
        self.inner.on_timer(tag, ctx, fx);
        let (text, class) = match tag {
            PING => ("ping".to_owned(), TrafficClass::Safety),
            TimerTag(n) => (format!("rang|tag={n}"), TrafficClass::Normal),
        };
        fx.push(Effect::Send {
            stream: EXEC_STREAM,
            frame: WireSlice::plain(text.into_bytes()),
            rpc: None,
            class,
            charge: RateCharge::one(OpKind::Control, None),
        });
        if let (PING, Some(after)) = (tag, self.keepalive) {
            fx.push(Effect::Timer { tag: PING, after });
        }
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

/// Nonces counted up from 0, each reservation logged; from its `short_from`th reservation on
/// (0 for every one), the source reserves one fewer than asked.
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
        short_from: usize::MAX,
        log: Arc::clone(&log),
    };
    let config = ExecSessionConfig {
        venue,
        cfg,
        creds: Secrets::new(),
        acct: fbc_core::AccountKey::new(1),
        rpc_ids: fbc_runtime::RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        conn: CONN,
        limiter: limiter(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
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
    // reserved from the consumer's source, and the clock's time; between them, the one nonce of
    // the first epoch's cancel-on-disconnect arm, sent once it authenticated (FBC-w19).
    let opens = venue.opens();
    let blocks: Vec<_> = opens.iter().map(|(_, ctx)| ctx.nonces.as_slice()).collect();
    assert_eq!(blocks, [&[0, 1][..], &[3, 4, 5][..]]);
    assert_eq!(
        *reserved.lock().unwrap(),
        [vec![0, 1], vec![2], vec![3, 4, 5]]
    );
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
// FBC-bnl's done line: HTTP requests, timers and the keepalive, each only to the epoch that
// asked (decision 0056).
// ---------------------------------------------------------------------------------------------

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

/// The epochs the handler heard events of, and its ends, in order.
fn heard_conns(log: &Log) -> Vec<(bool, ConnKey)> {
    let log = log.borrow();
    let conn = |heard: &Heard| match heard {
        Heard::Event(_, env) => (true, env.stamp.conn),
        Heard::End(key) => (false, *key),
    };
    log.iter().map(conn).collect()
}

#[tokio::test]
async fn an_http_result_is_answered_only_to_the_epoch_that_asked() {
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ExecToy::leak();
    let (mut config, _) = setup(venue, &server.url(), quick());
    config.cfg.insert(HTTP, &http.url(""));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let _ = (first.recv().await, first.recv().await);
        first.send("ask|tag=1|path=/now");
        let now = http.request().await;
        assert_eq!(now.line, "GET /now");
        now.answer("HTTP/1.1 200 OK", "first").await;
        // What on_http asks for is executed on the socket.
        assert_eq!(first.recv().await, "got|tag=1");
        // A request whose epoch ends before its answer comes back.
        first.send("ask|tag=2|path=/late");
        let late = http.request().await;
        first.drop_conn();
        let mut second = server.accept().await;
        let _ = (second.recv().await, second.recv().await);
        // Answered once the session has read it whole, into the new epoch.
        late.answer("HTTP/1.1 200 OK", "late").await;
        // The new epoch's own request reaches the codec, and what it pushes is stamped under
        // that epoch.
        second.send("ask|tag=3|path=/fresh");
        let fresh = http.request().await;
        assert_eq!(fresh.line, "GET /fresh");
        fresh.answer("HTTP/1.1 201 Created", "fresh").await;
        assert_eq!(second.recv().await, "got|tag=3");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // The old epoch's result (tag 2) reached no codec, though the codec lives across epochs.
    assert_eq!(venue.answers(), ["1:200:first", "3:201:fresh"]);
    assert_eq!(session.stale(Input::Http), 1);
    assert_eq!(
        heard_conns(&log),
        [
            (true, key(0)),
            (false, key(0)),
            (true, key(1)),
            (false, key(1))
        ]
    );
    assert_eq!(session.counters().decode_errors, 0);
}

#[tokio::test]
async fn a_timer_of_an_ended_epoch_is_dropped_and_counted_and_on_timer_gets_exactly_its_nonces() {
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (config, reserved) = setup(venue, &server.url(), quick());
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let _ = (first.recv().await, first.recv().await);
        // Due in 300 ms, but its epoch ends first.
        first.send("arm|tag=5|ms=300");
        first.send("bye");
        assert_eq!(first.next().await, None);
        let mut second = server.accept().await;
        let _ = (second.recv().await, second.recv().await);
        // Past the old timer's time: it was armed before this epoch opened. The heap fires it
        // before any timer armed from now on, so it has come back into nothing once the next
        // one rings.
        tokio::time::sleep(ms(400)).await;
        second.send("arm|tag=7|ms=0");
        assert_eq!(second.recv().await, "rang|tag=7");
        second.send("arm|tag=8|ms=0");
        assert_eq!(second.recv().await, "rang|tag=8");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(session.stale(Input::Timer), 1);
    let timers = venue.timers();
    let tags: Vec<_> = timers.iter().map(|(tag, _)| *tag).collect();
    assert_eq!(tags, [TimerTag(7), TimerTag(8)]);
    // on_timer asked for one nonce, then two: each call got exactly those, reserved from the
    // consumer's source after the two epochs' on_open reservations, and nothing else.
    let blocks: Vec<_> = timers.iter().map(|(_, c)| c.nonces.as_slice()).collect();
    assert_eq!(blocks, [&[5][..], &[6, 7][..]]);
    let all = [vec![0, 1], vec![2, 3, 4], vec![5], vec![6, 7]];
    assert_eq!(*reserved.lock().unwrap(), all);
    // Each context carries the shard clock's time of its firing, after its epoch opened.
    let opened = venue.opens()[1].1.mono;
    assert!(opened <= timers[0].1.mono && timers[0].1.mono <= timers[1].1.mono);
}

#[tokio::test(start_paused = true)]
async fn the_codecs_keepalive_goes_out_at_its_declared_interval_with_the_nonces_it_asks_for() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (mut config, reserved) = setup(venue, &server.url(), quick());
    config.cfg.insert(KEEPALIVE, "1000");
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        let (opened, resync) = peer.next_at().await.unwrap();
        assert!(resync.starts_with("resync|ts="), "{resync}");
        let mut at = Vec::new();
        for _ in 0..3 {
            // Not a moment before the interval.
            advance(ms(999)).await;
            churn().await;
            assert!(peer.quiet());
            advance(ms(1)).await;
            let (when, what) = peer.next_at().await.unwrap();
            assert_eq!(what, "ping");
            at.push(when.duration_since(opened));
        }
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(at, [1_000, 2_000, 3_000].map(ms));
    let timers = venue.timers();
    assert!(timers.iter().all(|(tag, _)| *tag == PING));
    let blocks: Vec<_> = timers
        .iter()
        .map(|(_, c)| c.nonces.as_slice().to_vec())
        .collect();
    assert_eq!(blocks, [vec![2], vec![3, 4], vec![5, 6, 7]]);
    let all = [vec![0, 1], vec![2], vec![3, 4], vec![5, 6, 7]];
    assert_eq!(*reserved.lock().unwrap(), all);
    // Each firing's context carries the time it was due at, on the shard clock.
    let opened = venue.opens()[0].1.mono.0;
    let due: Vec<_> = timers.iter().map(|(_, c)| c.mono.0 - opened).collect();
    assert_eq!(due, [1_000_000_000, 2_000_000_000, 3_000_000_000]);
}

#[tokio::test]
async fn a_nonce_source_that_reserves_another_count_for_a_timer_ends_the_session_before_on_timer() {
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (mut config, _) = setup(venue, &server.url(), quick());
    config.nonces = Box::new(Counting {
        next: 0,
        short_from: 1,
        log: Arc::default(),
    });
    let log = Log::default();
    let (mut session, _control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        peer.send("arm|tag=5|ms=0");
        // The timer reached no codec, so it sent nothing, and the connection closed.
        assert_eq!(peer.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    let short = ExecSessionError::Nonces {
        asked: 1,
        reserved: 0,
    };
    assert_eq!(run.err(), Some(short));
    assert!(venue.timers().is_empty());
    assert_eq!(heard_conns(&log), [(false, key(0))]);
    assert_eq!(session.current(), key(1));
}

#[tokio::test(start_paused = true)]
async fn a_timer_due_during_a_stalled_write_fires_into_on_timer_and_the_window_ends_the_epoch() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    // A 1 s floor and no attempt deadline: neither is the bound under test.
    let pacing = ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX);
    let (mut config, _) = setup(venue, &server.url(), pacing.unwrap());
    config.write_stall = WriteStall::new(ms(3_000)).unwrap();
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        // A timer due in 500 ms, then a 64 MiB frame the peer never reads: far more than the
        // loopback socket buffers hold, so the write waits on the peer.
        peer.send("arm|tag=5|ms=500");
        peer.send("big|kb=65536");
        let release = peer.hold();
        churn().await;
        // The timer fires into the codec while the write still waits, at its own time.
        advance(ms(499)).await;
        churn().await;
        assert!(venue.timers().is_empty());
        advance(ms(1)).await;
        settle(|| venue.timers().len() == 1).await;
        // The write is abandoned at the window, 3 s after it began, and the epoch ends as a
        // drop: the next attempt waits the pacing's 1 s floor, so it starts 4 s in.
        for step in [2_499, 1, 999] {
            advance(ms(step)).await;
            churn().await;
            assert!(server.try_accept().is_none());
        }
        advance(ms(1)).await;
        let mut next = None;
        for _ in 0..100_000 {
            next = server.try_accept();
            if next.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(next.is_some());
        drop((control, release));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(session.counters().write_stalls, 1);
    assert_eq!(venue.timers()[0].0, TimerTag(5));
    assert_eq!(session.current(), key(1));
}

#[tokio::test(start_paused = true)]
async fn a_timers_nonces_mis_reserved_while_a_write_waits_end_the_session_as_the_write_ends() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let pacing = ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX);
    let (mut config, _) = setup(venue, &server.url(), pacing.unwrap());
    config.write_stall = WriteStall::new(ms(3_000)).unwrap();
    config.nonces = Box::new(Counting {
        next: 0,
        short_from: 1,
        log: Arc::default(),
    });
    let (mut session, _control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        // Two timers due while a 64 MiB frame the peer never reads waits to be written.
        peer.send("arm|tag=5|ms=500");
        peer.send("arm|tag=6|ms=600");
        peer.send("big|kb=65536");
        let release = peer.hold();
        churn().await;
        // The first firing's nonces are mis-reserved, so neither reaches the codec; the session
        // ends once the write it waits on is abandoned at the window.
        for step in [500, 100, 2_400] {
            advance(ms(step)).await;
            churn().await;
        }
        release
    };
    let (run, release) = tokio::join!(session.run(), script);
    drop((frozen, release));
    let short = ExecSessionError::Nonces {
        asked: 1,
        reserved: 0,
    };
    assert_eq!(run.err(), Some(short));
    assert!(venue.timers().is_empty());
    assert_eq!(session.counters().write_stalls, 1);
}

/// What woke the session while a write waited.
#[derive(Copy, Clone)]
enum During {
    /// Timer 5, due 500 ms into the write; its `on_timer` asks to send `rang|tag=5`.
    Timer,
    /// The result of request 7, asked for just before; its `on_http` pushes an event and asks
    /// to send `got|tag=7`.
    Http,
}

/// A frame decoding to a 64 MiB frame and then `then`, with `during` taken while the peer does
/// not read the 64 MiB one: what the peer hears on that connection once it reads again, each
/// frame by its length, until it closes (`None`) or the test has heard `frames`; and the events
/// the handler, a closure, was handed.
async fn heard_behind_a_stalled_write(
    during: During,
    then: &str,
    frames: usize,
) -> (Vec<Option<usize>>, usize) {
    let frozen = freeze();
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ExecToy::leak();
    let pacing = ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX);
    let (mut config, _) = setup(venue, &server.url(), pacing.unwrap());
    config.cfg.insert(HTTP, &http.url(""));
    config.write_stall = WriteStall::new(ms(3_000)).unwrap();
    let events = Rc::new(RefCell::new(0));
    let counted = events.clone();
    let handler = move |_| *counted.borrow_mut() += 1;
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        match during {
            During::Timer => peer.send("arm|tag=5|ms=500"),
            During::Http => peer.send("ask|tag=7|path=/x"),
        }
        peer.send(&format!("big|kb=65536|then={then}"));
        let release = peer.hold();
        churn().await;
        match during {
            During::Timer => {
                advance(ms(500)).await;
                settle(|| venue.timers().len() == 1).await;
            }
            During::Http => {
                let asked = http.request().await;
                asked.answer("HTTP/1.1 200 OK", "x").await;
                settle(|| venue.answers().len() == 1).await;
            }
        }
        drop(release);
        let mut heard = Vec::new();
        while heard.len() < frames {
            let next = peer.next().await;
            let end = next.is_none();
            heard.push(next.map(|text| text.len()));
            if end {
                break;
            }
        }
        drop(control);
        heard
    };
    let (run, heard) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let events = *events.borrow();
    (heard, events)
}

const BIG: usize = 65_536 * 1024;

/// What a timer firing asks for while a write waits goes behind the rest of that write's batch
/// (decision 0056), not ahead of it (Reviewer B, B1).
#[tokio::test(start_paused = true)]
async fn a_frame_a_timer_asks_for_while_a_write_waits_goes_behind_the_rest_of_the_batch() {
    let (said, rang) = ("said".len(), "rang|tag=5".len());
    let (heard, _) = heard_behind_a_stalled_write(During::Timer, "said", 3).await;
    assert_eq!(heard, [Some(BIG), Some(said), Some(rang)]);
}

/// So does what an HTTP result coming back while a write waits asks for; the event it pushes
/// reaches the handler at once (Reviewer B, B1).
#[tokio::test(start_paused = true)]
async fn a_frame_an_http_result_asks_for_while_a_write_waits_goes_behind_the_rest_of_the_batch() {
    let (said, got) = ("said".len(), "got|tag=7".len());
    let (heard, events) = heard_behind_a_stalled_write(During::Http, "said", 3).await;
    assert_eq!(heard, [Some(BIG), Some(said), Some(got)]);
    assert_eq!(events, 1);
}

/// A reconnect left in the batch of a waiting write ends the epoch before what a timer firing
/// asked for meanwhile is reached, so it is never sent (Reviewer B, B1).
#[tokio::test(start_paused = true)]
async fn a_reconnect_behind_a_waiting_write_ends_the_epoch_before_a_timers_frame() {
    let (heard, _) = heard_behind_a_stalled_write(During::Timer, "bye", 3).await;
    assert_eq!(heard, [Some(BIG), None]);
}

/// A request an HTTP result asks for while a write waits, after the handler stopped the session
/// on that result's event, is never started, so it is neither charged nor journaled after the
/// stop (Reviewer B, B6).
#[tokio::test(start_paused = true)]
async fn a_request_asked_for_while_a_write_waits_after_the_stop_is_never_started_or_charged() {
    let frozen = freeze();
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ExecToy::leak();
    let pacing = ReconnectPacing::new(ms(1_000), ms(8_000), 100, ms(60_000), Duration::MAX);
    let (mut config, _) = setup(venue, &server.url(), pacing.unwrap());
    config.cfg.insert(HTTP, &http.url(""));
    config.write_stall = WriteStall::new(ms(3_000)).unwrap();
    let limiter = config.limiter.clone();
    // The units charged to the toy's one limit, the account's; the clock never moves.
    let account = || limiter.used(Instant::now(), 0, BucketKey::Shared);
    let log = Log::default();
    let handler = Keep::new(&log);
    let slot = Rc::clone(&handler.control);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *slot.borrow_mut() = Some(control);
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        peer.send("ask|tag=7|path=/x");
        peer.send("big|kb=65536|then=said");
        let release = peer.hold();
        churn().await;
        let asked = http.request().await;
        let before = account();
        // Its event stops the session; then the codec asks for request 8.
        asked
            .answer("HTTP/1.1 200 OK", "stop|ask|tag=8|path=/y")
            .await;
        settle(|| venue.answers().len() == 1).await;
        drop(release);
        before
    };
    let (run, before) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    assert_eq!(venue.answers(), ["7:200:stop|ask|tag=8|path=/y"]);
    // Since the result came back, only the stop's close frame was charged: not request 8.
    assert_eq!(account(), before + 1);
}

// ---------------------------------------------------------------------------------------------
// Frames that keep arriving starve nothing (DeepSeek DS-1): the wake `select!` is unbiased, so
// each turn it polls its branches from a random one, and the control's drop is checked after
// every wake, whichever branch woke it.
// ---------------------------------------------------------------------------------------------

/// How many frames a flood holds.
const FLOOD: usize = 2_000;

/// `FLOOD` frames of noise, each `pad` bytes long past its `noise` prefix.
fn flood(pad: usize) -> impl Iterator<Item = String> {
    let frame = format!("noise{}", "x".repeat(pad));
    std::iter::repeat_n(frame, FLOOD)
}

/// How many `noise` frames the codec decoded before and after it took `what`, which it must
/// have taken.
fn noise_around(inputs: &[String], what: &str) -> (usize, usize) {
    let at = inputs.iter().position(|input| input == what);
    let at = at.unwrap_or_else(|| panic!("the codec never took {what}"));
    let noise = |part: &[String]| part.iter().filter(|input| *input == "noise").count();
    (noise(&inputs[..at]), noise(&inputs[at + 1..]))
}

/// A timer that falls due as a flood of frames lands is fired while frames are still waiting:
/// the session does not decode the whole flood first.
#[tokio::test(start_paused = true)]
async fn a_timer_due_as_frames_keep_arriving_fires_before_they_are_all_decoded() {
    let frozen = freeze();
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (config, _) = setup(venue, &server.url(), quick());
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        peer.send("arm|tag=5|ms=100");
        churn().await;
        // The flood is written in one go before the clock moves, so when the session next
        // runs, the timer has fired and every frame is waiting.
        peer.send_all(flood(0));
        advance(ms(100)).await;
        assert_eq!(peer.recv().await, "rang|tag=5");
        settle(|| venue.noise() == FLOOD).await;
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(frozen);
    let (_, after) = noise_around(&venue.inputs(), "timer 5");
    assert!(after > 0, "the timer waited for the whole flood");
    assert_eq!(venue.noise(), FLOOD);
}

/// An HTTP result that comes back as a flood of frames lands is handed to the codec before the
/// flood, written whole before the answer, is all decoded.
#[tokio::test]
async fn an_http_result_back_as_frames_keep_arriving_is_taken_before_they_are_all_decoded() {
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ExecToy::leak();
    let (mut config, _) = setup(venue, &server.url(), quick());
    config.cfg.insert(HTTP, &http.url(""));
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        peer.send("ask|tag=1|path=/x");
        let asked = http.request().await;
        // 16 MiB, more than the socket buffers hold: frames are still arriving once the
        // session has seen the answer come back.
        peer.send_all(flood(8 * 1024));
        asked.answer("HTTP/1.1 200 OK", "x").await;
        assert_eq!(peer.recv().await, "got|tag=1");
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let (before, _) = noise_around(&venue.inputs(), "http 1");
    assert!(before < FLOOD, "the result waited for the whole flood");
}

/// The control dropped while a flood of frames is still arriving ends the session at its next
/// wake: no frame is decoded after the drop.
#[tokio::test]
async fn a_stop_as_frames_keep_arriving_ends_the_session_before_another_is_decoded() {
    let mut server = ScriptedWs::start().await;
    let venue = ExecToy::leak();
    let (config, _) = setup(venue, &server.url(), quick());
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut peer = server.accept().await;
        let _ = (peer.recv().await, peer.recv().await);
        // 16 MiB: more than the socket buffers hold, so frames are still arriving at the drop.
        peer.send_all(flood(8 * 1024));
        settle(|| venue.noise() > 0).await;
        let at = venue.noise();
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(at < FLOOD, "the whole flood was decoded before the drop");
    assert_eq!(venue.noise(), at);
    // A stop, not a drop: no epoch opened after it.
    assert_eq!(session.current(), key(0));
    assert_eq!(session.counters().attempts, 1);
}

// ---------------------------------------------------------------------------------------------
// Effects, the proxy, and what a session refuses.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn stray_effects_are_refused_and_a_reconnect_the_codec_asks_for_opens_the_next_epoch() {
    let (mut server, mut http) = (ScriptedWs::start().await, ScriptedHttp::start().await);
    let venue = ExecToy::leak();
    let (mut config, _) = setup(venue, &server.url(), quick());
    config.cfg.insert(HTTP, &http.url(""));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let script = async move {
        let mut first = server.accept().await;
        let _ = (first.recv().await, first.recv().await);
        first.send("stray");
        // Of the frames, only the one for the session's own stream went out; then the timer
        // fired (FBC-bnl).
        assert_eq!(first.recv().await, "said");
        assert_eq!(first.recv().await, "rang|tag=1");
        // The request is answered by the script, not by whatever the host has on a port
        // (Reviewer B, B7). The client closes the connection only as its result is taken, so
        // the codec has it before `bye` is sent.
        let stray = http.request().await;
        assert_eq!(stray.line, "GET /stray");
        stray.answer("HTTP/1.1 200 OK", "stray").await;
        first.send("bye");
        assert_eq!(first.next().await, None);
        let mut second = server.accept().await;
        let _ = (second.recv().await, second.recv().await);
        drop(control);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    // A frame and a reconnect for another stream. The timer fired into the codec and the
    // request's result reached it, which the toy cannot decode (FBC-bnl); the timer and the
    // request behind the reconnect were never reached, since it ended the epoch before their
    // turn (Codex r4188639448).
    let counters = session.counters();
    assert_eq!((counters.refused_effects, counters.decode_errors), (2, 1));
    let rang: Vec<_> = venue.timers().iter().map(|(tag, _)| *tag).collect();
    assert_eq!(rang, [TimerTag(1)]);
    assert_eq!(session.stale(Input::Timer) + session.stale(Input::Http), 0);
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
        short_from: 0,
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
    let shared = config.limiter.clone();
    spend_the_accounts_budget(&shared);
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

/// Takes the account's whole budget of 50 a second, as another session sharing `limiter` would;
/// when it was taken.
fn spend_the_accounts_budget(limiter: &RateLimiter) -> Instant {
    let taken = Request {
        charge: RateCharge {
            weight: NonZeroU32::new(50).unwrap(),
            ..RateCharge::one(OpKind::Control, None)
        },
        via: Via::Frame,
        class: TrafficClass::Safety,
    };
    let other = ConnKey { conn: 99, epoch: 0 };
    let at = Instant::now();
    limiter.charge(at, other, &[taken]).unwrap();
    at
}

#[tokio::test(start_paused = true)]
async fn the_next_attempt_after_on_open_frames_the_buckets_refuse_waits_until_they_fit() {
    let mut server = ScriptedWs::start().await;
    // A 10 ms floor, so only the buckets can hold the next attempt back for the second their
    // budget takes to come back. No attempt deadline: on paused time a pending one would let
    // the clock jump ahead while an attempt's socket I/O is still under way.
    let pacing = ReconnectPacing::new(ms(10), ms(100), 100, Duration::from_secs(60), Duration::MAX);
    let (config, _) = setup(ExecToy::leak(), &server.url(), pacing.unwrap());
    let shared = config.limiter.clone();
    let taken_at = spend_the_accounts_budget(&shared);
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        // The first epoch sends nothing and is dropped; the next connects only once the
        // account's budget has room for what on_open sends (Reviewer B, B1), not every floor.
        let mut first = server.accept().await;
        assert_eq!(first.next().await, None);
        let mut second = server.accept().await;
        let (at, opened) = second.next_at().await.unwrap();
        assert!(opened.starts_with("auth|ts="), "{opened}");
        drop(control);
        at
    };
    let (run, at) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(
        at >= taken_at + Duration::from_secs(1),
        "{:?}",
        at - taken_at
    );
    assert_eq!(session.counters().attempts, 2);
    assert_eq!(shared.counts().refused.account, 1);
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

/// A handler whose every call panics, counting its calls; one that holds a poisoned lock, say.
struct AlwaysPanics(Arc<AtomicU32>);

impl ExecHandler for AlwaysPanics {
    fn on_exec(&mut self, _: Envelope<ExecEvent>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("the handler failed");
    }

    fn on_epoch_end(&mut self, _: ConnKey) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("the handler failed again");
    }
}

#[tokio::test]
async fn a_session_dropped_as_its_handlers_panic_unwinds_calls_the_handler_no_more() {
    let mut server = ScriptedWs::start().await;
    let url = server.url();
    let calls = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&calls);
    let venue = ExecToy::leak();
    // The session lives in this thread's frame, so on_exec's panic drops it while unwinding:
    // telling the handler then would panic again and abort the process (Reviewer B, B2).
    let driver = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (config, _) = setup(venue, &url, quick());
        let (mut session, _control) = ExecSession::new(config, AlwaysPanics(seen)).unwrap();
        rt.block_on(session.run())
    });
    let mut peer = server.accept().await;
    let _ = (peer.recv().await, peer.recv().await);
    peer.send(AUTH_ACK);
    let joined = tokio::task::spawn_blocking(move || driver.join());
    assert!(joined.await.unwrap().is_err());
    // on_exec alone: the epoch's end is not told during the unwind.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
    // The fee query, the timer's frame, then the resync.
    assert_eq!(fx.take().len(), 3);
    assert_eq!(pushed.len(), 1);
}

/// An [`ExecSink`] that is a function.
struct SinkFn<'a>(&'a mut dyn FnMut(fbc_core::VenueMeta, ExecEvent));

impl ExecSink for SinkFn<'_> {
    fn push(&mut self, meta: fbc_core::VenueMeta, ev: ExecEvent) {
        (self.0)(meta, ev)
    }
}
