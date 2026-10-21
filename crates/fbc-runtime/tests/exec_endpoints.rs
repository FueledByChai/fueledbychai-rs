//! FBC-tnsf's done line: one order-entry session drives every connection the venue plans, here
//! the conformance toy's order-entry codec planned on two endpoints against local WebSocket
//! servers (decision 0115). Each endpoint's `on_open` is called once per its own epoch with
//! exactly the nonces it asks for; a frame is written to the endpoint its effect names; a drop
//! of one endpoint reconnects it through the consumer's pacing while the other's epoch stays
//! open; and the events of both are stamped in one ingest order.
//!
//! Also: what each endpoint asks for (a timer, an HTTP request, a reconnect, a frame for the
//! other) reaches only the epoch that asked or the connection it names; an event of another
//! endpoint never readies the order connection; and the session ends every endpoint's epoch on
//! a stop, an error, a dropped run and a drop as it does its order connection's.

mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use common::toy::ToyVenue;
use common::{ScriptedHttp, ScriptedWs, hanging, refusing};
use exec_toy::{EXEC_STREAM, OWN_NS, ToyExec, ToySigner};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConnKey, ConnState, CtxCall, DecodeError, DecodeScope,
    Effect, Effects, EncodeCtx, EncodeReceipt, EndpointPlan, Envelope, ExecCodec, ExecEndpoint,
    ExecEvent, ExecSink, FieldSpec, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse,
    HttpTag, Inbound, InboundSpans, InstrumentSpecDraft, LimitScope, MdCodec, ModeScope, MonoNs,
    NonceBlock, NonceRefused, NonceSource, OpKind, PathStamps, RateCharge, RateLimit, RawFrame,
    RpcId, Secrets, SpecTable, StreamId, Subscription, SymbolError, TagSet, TimerTag, TrafficClass,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta, VenueMode, WallNs,
    WireSlice, WireUrl,
};
use fbc_journal::{ControlEvent, JournalSink, Record, RecordRef, Recorded};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Connector, ExecControl, ExecHandler, ExecOrders, ExecSession, ExecSessionConfig,
    ExecSessionError, InProcessStream, IngestClock, Input, Journal, OrderEntryPeer, ProxyConfig,
    RateLimiter, ReconnectPacing, SafetyReserve, SessionError, WriteStall,
};
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::time::Instant;

// ---------------------------------------------------------------------------------------------
// The venue: the conformance toy's order entry, planned on two endpoints.
// ---------------------------------------------------------------------------------------------

/// The order connection's URL, the first endpoint planned, on [`EXEC_STREAM`].
const TRADE: &str = "two.trade";
/// The second endpoint's URL, on [`ACCOUNT`].
const ACCOUNT_URL: &str = "two.account";
/// `same`: the second endpoint is planned on the order connection's stream too.
const STREAMS: &str = "two.streams";
/// Any value: the second endpoint's `on_open` also sends a frame heavier than the toy's limit.
const HEAVY: &str = "two.heavy";
/// The base URL the codec's HTTP requests go to.
const HTTP: &str = "two.http";
/// A count: the venue also declares a limit of that many connections a minute.
const CONNECTS: &str = "two.connects";

/// The second endpoint's stream: an account stream, as Hibachi's.
const ACCOUNT: StreamId = StreamId(2);

/// Every `on_open` call the codec saw: its stream and context.
type Opens = Arc<Mutex<Vec<(StreamId, EncodeCtx)>>>;

#[derive(Default)]
struct Two {
    opens: Opens,
}

impl Two {
    fn leak() -> &'static Two {
        Box::leak(Box::default())
    }

    /// The contexts `on_open` was called with on `stream`, in order.
    fn opens_on(&self, stream: StreamId) -> Vec<EncodeCtx> {
        let opens = self.opens.lock().unwrap();
        let on = opens.iter().filter(|(s, _)| *s == stream);
        on.map(|(_, ctx)| ctx.clone()).collect()
    }
}

/// A limit of `units` connections a minute.
fn connects(units: u32) -> RateLimit {
    RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[OpKind::Connect]),
        per: Duration::from_secs(60),
        units,
    }
}

impl VenueFactory for Two {
    fn id(&self) -> &'static str {
        "TOY-TWO"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = exec_toy::caps();
        if let Some(n) = cfg.get(CONNECTS) {
            caps.limits.push(connects(n.parse().unwrap()));
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

    /// The order connection first, then the account stream's.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = |key| WireUrl::plain(cfg.get(key).unwrap());
        let second = match cfg.get(STREAMS) {
            Some("same") => EXEC_STREAM,
            _ => ACCOUNT,
        };
        Ok(vec![
            ExecEndpoint {
                stream: EXEC_STREAM,
                url: url(TRADE),
                handshake: Vec::new(),
            },
            ExecEndpoint {
                stream: second,
                url: url(ACCOUNT_URL),
                handshake: Vec::new(),
            },
        ])
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok(Box::new(Split {
            inner: ToyExec::new(Box::new(ToySigner)),
            opens: self.opens.clone(),
            http: cfg.get(HTTP).map(str::to_owned),
            heavy: cfg.get(HEAVY).is_some(),
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

/// The conformance toy's codec over both streams, recording each `on_open`. Its `on_open` asks
/// for one nonce on the order connection and for two on the account stream's first epoch,
/// three on its second, and so on, so each reservation differs. Frames of the test's own, on
/// either stream: `relay|to=<n>` asks to send `said|from=<stream>` on stream `n`; `bye|to=<n>`
/// asks to reconnect stream `n`; `arm|ms=<t>` sets a timer, tagged with the stream it came
/// on, which sends `rang|on=<stream>` there; `bin|to=<n>` asks to send the binary frame
/// `ff <stream>` on stream `n`; `both|kb=<n>` asks to send an `n` KiB frame on the account
/// stream, then to reconnect its own; `ask|path=<p>` asks for a GET of `p` under the
/// configured base URL, tagged with its stream, whose result pushes an account `Halted` mode
/// and sends `got|on=<stream>` there; `ready` pushes the order stream authenticated, wherever
/// it is read.
struct Split {
    inner: ToyExec,
    opens: Opens,
    http: Option<String>,
    heavy: bool,
}

/// The value of `name` in a test frame `kind|name=value|...`.
fn field<'a>(text: &'a str, name: &str) -> &'a str {
    let found = text.split('|').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == name).then_some(value)
    });
    found.unwrap_or_else(|| panic!("{name} in {text}"))
}

/// A frame `text` on `stream`, of one `Control` unit.
fn send(stream: StreamId, text: String) -> Effect {
    Effect::Send {
        stream,
        frame: WireSlice::plain(text.into_bytes()),
        rpc: None,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Control, None),
    }
}

impl ExecCodec for Split {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        let opened = |s| {
            self.opens
                .lock()
                .unwrap()
                .iter()
                .filter(|o| o.0 == s)
                .count()
        };
        match call {
            CtxCall::Open(EXEC_STREAM) => 1,
            CtxCall::Open(stream) => 2 + opened(stream) as u16,
            other => self.inner.nonces_for(other),
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens.lock().unwrap().push((stream, ctx.clone()));
        self.inner.on_open(stream, ctx, fx);
        if self.heavy && stream != EXEC_STREAM {
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
    ) -> Result<EncodeReceipt, fbc_core::NotSentReason> {
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
        let to = |text: &str| StreamId(field(text, "to").parse().unwrap());
        match f {
            RawFrame::Text(text) if text.starts_with("relay|") => {
                fx.push(send(to(text), format!("said|from={}", stream.0)));
            }
            RawFrame::Text(text) if text.starts_with("both|") => {
                let kb: usize = field(text, "kb").parse().unwrap();
                fx.push(Effect::Send {
                    stream: ACCOUNT,
                    frame: WireSlice::plain(vec![b'x'; kb * 1024]),
                    rpc: None,
                    class: TrafficClass::Normal,
                    charge: RateCharge::one(OpKind::Control, None),
                });
                let reason = "both";
                fx.push(Effect::Reconnect { stream, reason });
            }
            RawFrame::Text(text) if text.starts_with("bin|") => {
                fx.push(Effect::Send {
                    stream: to(text),
                    frame: WireSlice::plain(vec![0xff, stream.0 as u8]),
                    rpc: None,
                    class: TrafficClass::Normal,
                    charge: RateCharge::one(OpKind::Control, None),
                });
            }
            RawFrame::Text(text) if text.starts_with("bye|") => {
                let reason = "bye";
                fx.push(Effect::Reconnect {
                    stream: to(text),
                    reason,
                });
            }
            RawFrame::Text(text) if text.starts_with("arm|") => {
                let after = Duration::from_millis(field(text, "ms").parse().unwrap());
                let tag = TimerTag(u64::from(stream.0));
                fx.push(Effect::Timer { tag, after });
            }
            RawFrame::Text(text) if text.starts_with("ask|") => {
                let base = self.http.as_deref().unwrap();
                let url = WireUrl::plain(format!("{base}{}", field(text, "path")));
                fx.push(Effect::Http {
                    tag: HttpTag(u64::from(stream.0)),
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
                });
            }
            RawFrame::Text("ready") => {
                let state = ConnState::Authenticated;
                let stream = EXEC_STREAM;
                sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
            }
            _ => return self.inner.on_frame(stream, f, scope, specs, sink, fx),
        }
        Ok(())
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        _: Result<HttpResponse<'_>, HttpFailure>,
        _: &DecodeScope<'_>,
        _: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let halted = ExecEvent::Mode {
            scope: ModeScope::Account,
            mode: VenueMode::Halted,
        };
        sink.push(VenueMeta::NONE, halted);
        let stream = StreamId(u16::try_from(tag.0).unwrap());
        fx.push(send(stream, format!("got|on={}", tag.0)));
        Ok(())
    }

    fn on_timer(&mut self, tag: TimerTag, _: &EncodeCtx, fx: &mut Effects) {
        let stream = StreamId(u16::try_from(tag.0).unwrap());
        fx.push(send(stream, format!("rang|on={}", tag.0)));
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.resync(ctx, fx);
    }

    /// Panics on a frame `boom`, as a codec defect would.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        if let Inbound::Frame(RawFrame::Text("boom")) = input {
            panic!("the codec failed");
        }
        self.inner.redact_inbound(input)
    }
}

/// Nonces counted up from 0, each reservation logged; with `refuse_from`, a reservation of that
/// many or more is refused.
struct Counting {
    next: u64,
    refuse_from: u16,
    log: Arc<Mutex<Vec<Vec<u64>>>>,
}

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        self.log.lock().unwrap().push(block.as_slice().to_vec());
        block
    }

    fn try_reserve(&mut self, len: u16) -> Result<NonceBlock, NonceRefused> {
        if len >= self.refuse_from {
            return Err(NonceRefused::Exhausted);
        }
        Ok(self.reserve(len))
    }
}

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

/// The connection number the tests stamp the order connection with; the account stream's is
/// the next.
const CONN: u16 = 3;

fn key(epoch: u32) -> ConnKey {
    ConnKey { conn: CONN, epoch }
}

fn acct(epoch: u32) -> ConnKey {
    ConnKey {
        conn: CONN + 1,
        epoch,
    }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A reconnect floor of `floor` ms, so a drop reconnects no sooner.
fn floor(floor: u64) -> ReconnectPacing {
    ReconnectPacing::new(
        ms(floor),
        ms(floor),
        100,
        Duration::from_secs(60),
        ms(5_000),
    )
    .unwrap()
}

/// A session of `venue` with its order connection at `trade` and its account stream at
/// `account`, paced by `pacing`; its nonce reservations logged.
fn setup(
    venue: &'static Two,
    trade: &str,
    account: &str,
    pacing: ReconnectPacing,
) -> (ExecSessionConfig, Arc<Mutex<Vec<Vec<u64>>>>) {
    let log = Arc::default();
    let mut cfg = VenueConfig::new();
    cfg.insert(TRADE, trade);
    cfg.insert(ACCOUNT_URL, account);
    let nonces = Counting {
        next: 0,
        refuse_from: u16::MAX,
        log: Arc::clone(&log),
    };
    let limits = exec_toy::caps().limits;
    let config = ExecSessionConfig {
        venue,
        cfg,
        creds: Secrets::new(),
        acct: fbc_core::AccountKey::new(1),
        account_wide_cod_authority: None,
        rpc_ids: fbc_runtime::RpcIds::default(),
        ns: OWN_NS,
        specs: exec_toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(nonces),
        nonce_source: fbc_journal::NonceSourceId(0),
        conn: CONN,
        limiter: RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
        http_max_body: 4096,
        outbound: fbc_runtime::ReservedBound::new(64, 64).unwrap(),
        in_flight: fbc_runtime::InFlight::new(fbc_runtime::ReservedBound::new(64, 64).unwrap()),
    };
    (config, log)
}

/// What the handler heard: an event, an epoch's end, or a frame it could not decode.
#[derive(Debug)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    End(ConnKey),
    Undecoded(ConnKey),
}

type Log = Rc<RefCell<Vec<Heard>>>;

/// A handler that keeps what it hears; holding the session's control, it drops it on the first
/// `ReduceOnly` mode.
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
        if matches!(env.body, ExecEvent::Mode { mode, .. } if mode == VenueMode::ReduceOnly) {
            drop(self.control.borrow_mut().take());
        }
        self.log.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::End(key));
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.log.borrow_mut().push(Heard::Undecoded(key));
    }
}

/// The events the handler heard, in order.
fn events(log: &Log) -> Vec<Envelope<ExecEvent>> {
    let log = log.borrow();
    let events = log.iter().filter_map(|h| match h {
        Heard::Event(env) => Some((**env).clone()),
        _ => None,
    });
    events.collect()
}

/// The epochs the handler was told ended, each with how many events it had heard by then.
fn ends(log: &Log) -> Vec<(usize, ConnKey)> {
    let (mut heard, mut ends) = (0, Vec::new());
    for h in log.borrow().iter() {
        match h {
            Heard::Event(_) => heard += 1,
            Heard::End(key) => ends.push((heard, *key)),
            Heard::Undecoded(_) => {}
        }
    }
    ends
}

/// Waits, in real time, until `done` holds.
async fn until(done: impl Fn() -> bool) {
    for _ in 0..5_000 {
        if done() {
            return;
        }
        tokio::time::sleep(ms(2)).await;
    }
    panic!("never happened");
}

fn auth(ctx: &EncodeCtx) -> String {
    format!("auth|ts={}|token={}", ctx.wall.0, exec_toy::TOY_TOKEN)
}

const AUTH_ACK: &str = "auth|ok=1|token=toy-session-token";

fn mode(mode: VenueMode) -> ExecEvent {
    ExecEvent::Mode {
        scope: ModeScope::Account,
        mode,
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn each_endpoint_opens_its_own_epochs_with_its_nonces_routes_frames_and_reconnects_alone() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Two::leak();
    let (config, reserved) = setup(venue, &trade.url(), &account.url(), floor(200));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(Rc::clone(&kept)));
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let (t_auth, a_auth) = (t.recv().await, a.recv().await);
        // Each connection's frames go on it: the account stream authenticates there.
        a.send(AUTH_ACK);
        a.send("mode|m=halted");
        a.send_binary(b"\xff");
        until(|| events(&watch).len() == 2).await;
        // A frame the account stream's input asks for on the order stream is written there, and
        // the other way round.
        a.send("relay|to=1");
        assert_eq!(t.recv().await, "said|from=2");
        t.send("relay|to=2");
        assert_eq!(a.recv().await, "said|from=1");
        // An event of the account stream claiming the order stream authenticated readies
        // nothing: no arm, no resync, no place (decisions 0058, 0115).
        a.send("ready");
        until(|| events(&watch).len() == 3).await;
        assert!(!orders.may_place());
        // A drop of the account stream reconnects it through its own pacing; the order
        // connection's epoch stays open meanwhile.
        let dropped = Instant::now();
        a.drop_conn();
        let mut a2 = account.accept().await;
        let waited = dropped.elapsed();
        let a2_auth = a2.recv().await;
        t.send("mode|m=postonly");
        until(|| events(&watch).len() == 4).await;
        a2.send("mode|m=cancelonly");
        until(|| events(&watch).len() == 5).await;
        assert!(t.quiet());
        drop(control);
        // Both connections closed, having sent nothing more.
        assert_eq!((t.next().await, a2.next().await), (None, None));
        (t_auth, a_auth, a2_auth, waited)
    };
    let (run, (t_auth, a_auth, a2_auth, waited)) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(waited >= ms(200), "reconnected after {waited:?}");

    // One `on_open` per epoch of each endpoint, each with exactly the nonces it asked for: one
    // on the order connection, two then three on the account stream's two epochs.
    let (trades, accounts) = (venue.opens_on(EXEC_STREAM), venue.opens_on(ACCOUNT));
    let blocks = |opens: &[EncodeCtx]| -> Vec<usize> {
        opens
            .iter()
            .map(|ctx| ctx.nonces.as_slice().len())
            .collect()
    };
    assert_eq!((blocks(&trades), blocks(&accounts)), (vec![1], vec![2, 3]));
    // Reserved from the consumer's one source, every value once.
    let reserved = reserved.lock().unwrap().clone();
    let given: Vec<Vec<u64>> = venue
        .opens
        .lock()
        .unwrap()
        .iter()
        .map(|(_, ctx)| ctx.nonces.as_slice().to_vec())
        .collect();
    assert_eq!(reserved, given);
    let all: BTreeSet<u64> = reserved.iter().flatten().copied().collect();
    assert_eq!(all.len(), 6);
    // What each `on_open` sent went on its own connection.
    assert_eq!(t_auth, auth(&trades[0]));
    assert_eq!(a_auth, auth(&accounts[0]));
    assert_eq!(a2_auth, auth(&accounts[1]));

    // One ingest order over both endpoints, each event stamped with its own connection's epoch.
    let envs = events(&log);
    let stamps: Vec<_> = envs.iter().map(|e| e.stamp.conn).collect();
    assert_eq!(stamps, [acct(0), acct(0), acct(0), key(0), acct(1)]);
    assert!(
        envs.windows(2)
            .all(|w| w[0].stamp.ingest_seq < w[1].stamp.ingest_seq)
    );
    let bodies: Vec<_> = envs.iter().map(|e| e.body.clone()).collect();
    let authenticated = |stream| ExecEvent::Conn {
        stream,
        state: ConnState::Authenticated,
    };
    assert_eq!(
        bodies,
        [
            authenticated(ACCOUNT),
            mode(VenueMode::Halted),
            authenticated(EXEC_STREAM),
            mode(VenueMode::PostOnly),
            mode(VenueMode::CancelOnly),
        ]
    );
    // The account stream's first epoch ended alone, after its events and before its second's;
    // the stop ended both connections' epochs.
    assert_eq!(ends(&log), [(3, acct(0)), (5, key(0)), (5, acct(1))]);
    let undecoded = log.borrow();
    let undecoded = undecoded.iter().filter_map(|h| match h {
        Heard::Undecoded(key) => Some(*key),
        _ => None,
    });
    assert_eq!(undecoded.collect::<Vec<_>>(), [acct(0)]);
    assert_eq!(session.current(), key(0));
    assert_eq!(session.current_on(EXEC_STREAM), Some(key(0)));
    assert_eq!(session.current_on(ACCOUNT), Some(acct(1)));
    assert_eq!(session.current_on(StreamId(9)), None);
    let counters = session.counters();
    assert_eq!((counters.attempts, counters.failed_attempts), (3, 0));
    assert_eq!((counters.decode_errors, counters.refused_effects), (1, 0));
    assert_eq!(session.stale(Input::Event), 0);
    // Each epoch of each endpoint journaled opened and closed under its own connection.
    let controls: Vec<ControlEvent> = kept
        .borrow()
        .0
        .iter()
        .filter_map(|r| match r {
            Record::Control { ev, .. } => Some(ev.clone()),
            _ => None,
        })
        .collect();
    for k in [key(0), acct(0), acct(1)] {
        assert!(controls.contains(&ControlEvent::Opened(k)), "{k:?}");
        assert!(controls.contains(&ControlEvent::Closed(k)), "{k:?}");
    }
}

#[tokio::test]
async fn a_drop_of_the_order_connection_reconnects_it_paced_while_the_account_stream_keeps_going() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Two::leak();
    let (config, _) = setup(venue, &trade.url(), &account.url(), floor(300));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        let dropped = Instant::now();
        t.drop_conn();
        until(|| ends(&watch) == [(0, key(0))]).await;
        // While the order connection waits on its floor, the account stream's frames are read
        // and its timers fire; a frame it asks for on the order stream is refused and counted.
        a.send("mode|m=halted");
        a.send("relay|to=1");
        a.send("arm|ms=0");
        assert_eq!(a.recv().await, "rang|on=2");
        assert_eq!(events(&watch).len(), 1);
        let mut t2 = trade.accept().await;
        let waited = dropped.elapsed();
        let _ = t2.recv().await;
        t2.send("relay|to=2");
        assert_eq!(a.recv().await, "said|from=1");
        drop(control);
        assert_eq!((t2.next().await, a.next().await), (None, None));
        waited
    };
    let (run, waited) = tokio::join!(session.run(), script);
    run.unwrap();
    assert!(waited >= ms(300), "reconnected after {waited:?}");
    assert_eq!(venue.opens_on(EXEC_STREAM).len(), 2);
    assert_eq!(venue.opens_on(ACCOUNT).len(), 1);
    let envs = events(&log);
    assert_eq!(envs[0].stamp.conn, acct(0));
    assert_eq!(ends(&log), [(0, key(0)), (1, key(1)), (1, acct(0))]);
    assert_eq!(session.counters().refused_effects, 1);
    assert_eq!(session.current(), key(1));
}

/// Only the order connection's own report authenticates its epoch; once it has, the arm's
/// answer and the resync's end count for that epoch wherever they are heard, as an account
/// stream carries a venue's answers (decisions 0058, 0115).
#[tokio::test]
async fn the_arm_and_resync_answered_on_the_account_stream_ready_the_order_connections_epoch() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (config, _) = setup(Two::leak(), &trade.url(), &account.url(), floor(10));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let orders = session.orders();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // Before the order connection authenticated, the account stream's answers count for
        // no epoch (here one the codec cannot read: no resync was asked for).
        a.send("rsend");
        until(|| matches!(watch.borrow()[..], [Heard::Undecoded(k)] if k == acct(0))).await;
        t.send(AUTH_ACK);
        let armed = t.recv().await;
        let rpc = field(&armed, "rpc").to_owned();
        assert_eq!(armed, format!("cod|rpc={rpc}|on=1"));
        let resync = t.recv().await;
        let wm = resync.strip_prefix("resync|ts=").unwrap().to_owned();
        assert!(!orders.may_place());
        a.send(&format!("item|rpc={rpc}|i=0|res=ok"));
        a.send(&format!("rsbegin|wm={wm}"));
        a.send("rsend");
        until(|| events(&watch).len() == 4).await;
        assert!(orders.may_place());
        drop(control);
        assert_eq!((t.next().await, a.next().await), (None, None));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let stamps: Vec<_> = events(&log).iter().map(|e| e.stamp.conn).collect();
    assert_eq!(stamps, [key(0), acct(0), acct(0), acct(0)]);
    assert_eq!(session.counters().arm_failures, 0);
}

// ---------------------------------------------------------------------------------------------
// What each endpoint asks for.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_reconnect_one_endpoints_input_asks_for_ends_only_the_epoch_of_the_stream_it_names() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Two::leak();
    let (config, _) = setup(venue, &trade.url(), &account.url(), floor(300));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // The order connection's input reconnects the account stream; meanwhile a frame for it
        // is refused and counted, and the order connection's epoch stays open.
        t.send("bye|to=2");
        assert_eq!(a.next().await, None);
        t.send("relay|to=2");
        // A stream no endpoint carries is a codec defect, refused and counted.
        t.send("relay|to=9");
        t.send("mode|m=halted");
        let mut a2 = account.accept().await;
        let _ = a2.recv().await;
        assert_eq!(events(&watch).len(), 1);
        // A binary frame goes on the endpoint it names as one.
        t.send("bin|to=2");
        assert_eq!(a2.recv().await, common::heard_binary(&[0xff, 1]));
        // The account stream's input reconnects the order connection.
        a2.send("bye|to=1");
        assert_eq!(t.next().await, None);
        let mut t2 = trade.accept().await;
        let _ = t2.recv().await;
        drop(control);
        assert_eq!((t2.next().await, a2.next().await), (None, None));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let envs = events(&log);
    assert_eq!(envs[0].stamp.conn, key(0));
    assert_eq!(
        ends(&log),
        [(0, acct(0)), (1, key(0)), (1, key(1)), (1, acct(1))]
    );
    assert_eq!(session.counters().refused_effects, 2);
    assert_eq!(venue.opens_on(EXEC_STREAM).len(), 2);
    assert_eq!(venue.opens_on(ACCOUNT).len(), 2);
}

#[tokio::test]
async fn each_endpoints_timers_and_http_results_reach_only_the_epoch_that_asked() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let mut http = ScriptedHttp::start().await;
    let venue = Two::leak();
    let (mut config, _) = setup(venue, &trade.url(), &account.url(), floor(300));
    config.cfg.insert(HTTP, &http.url(""));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(Rc::clone(&kept)));
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // The account stream's first epoch asks, arms a timer, and drops: the result and the
        // firing come back while it waits to reconnect, into nothing.
        a.send("ask|path=/one");
        let one = http.request().await;
        a.send("arm|ms=100");
        a.drop_conn();
        until(|| ends(&watch) == [(0, acct(0))]).await;
        one.answer("HTTP/1.1 200 OK", "late").await;
        let mut a2 = account.accept().await;
        let _ = a2.recv().await;
        // Its second epoch's timer and result reach the codec, and what they send goes there.
        a2.send("arm|ms=0");
        assert_eq!(a2.recv().await, "rang|on=2");
        a2.send("ask|path=/two");
        http.request().await.answer("HTTP/1.1 200 OK", "now").await;
        assert_eq!(a2.recv().await, "got|on=2");
        // The order connection's own.
        t.send("arm|ms=0");
        assert_eq!(t.recv().await, "rang|on=1");
        drop(control);
        assert_eq!((t.next().await, a2.next().await), (None, None));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let envs = events(&log);
    assert_eq!(envs.len(), 1);
    assert_eq!(
        (envs[0].stamp.conn, &envs[0].body),
        (acct(1), &mode(VenueMode::Halted))
    );
    assert_eq!(session.stale(Input::Timer), 1);
    assert_eq!(session.stale(Input::Http), 1);
    // Both results journaled, each under the account stream's epoch that asked.
    let answers: Vec<ConnKey> = kept
        .borrow()
        .0
        .iter()
        .filter_map(|r| match r {
            Record::HttpResult { stamp, .. } => Some(stamp.conn),
            _ => None,
        })
        .collect();
    assert_eq!(answers, [acct(0), acct(1)]);
}

// ---------------------------------------------------------------------------------------------
// Stops, errors and drops.
// ---------------------------------------------------------------------------------------------

/// The journal's records, as offered.
#[derive(Default)]
struct Kept(Vec<Record>);

impl JournalSink for Kept {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.0.push(record.clone());
        Recorded::Ok
    }

    fn record_ref(&mut self, _: TrafficClass, _: WallNs, record: RecordRef<'_>) -> Recorded {
        self.0.push(record.to_record());
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::Ok
    }
}

#[tokio::test]
async fn a_stop_on_an_account_event_closes_both_epochs_and_journals_the_frame_waiting_behind_it() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (config, _) = setup(Two::leak(), &trade.url(), &account.url(), floor(10));
    let log = Log::default();
    let handler = Keep::new(&log);
    let slot = Rc::clone(&handler.control);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(Rc::clone(&kept)));
    *slot.borrow_mut() = Some(control);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // The handler stops the session on the first; the second waits behind it.
        a.send_all(["mode|m=reduceonly".to_owned(), "mode|m=halted".to_owned()]);
        assert_eq!((t.next().await, a.next().await), (None, None));
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let envs = events(&log);
    assert_eq!(envs.len(), 1);
    assert_eq!(ends(&log), [(1, key(0)), (1, acct(0))]);
    // Each connection's epoch journaled opened and closed once, the frame that waited stamped
    // after the account stream's close, so replay hands it to no codec.
    let records = kept.borrow().0.clone();
    let control = |ev: ControlEvent| {
        let at = |r: &Record| matches!(r, Record::Control { ev: e, .. } if *e == ev);
        records.iter().filter(|r| at(r)).count()
    };
    for k in [key(0), acct(0)] {
        assert_eq!(control(ControlEvent::Opened(k)), 1, "{k:?}");
        assert_eq!(control(ControlEvent::Closed(k)), 1, "{k:?}");
    }
    let closed = records.iter().position(
        |r| matches!(r, Record::Control { ev: ControlEvent::Closed(k), .. } if *k == acct(0)),
    );
    let waited = records.iter().position(|r| {
        matches!(r, Record::Inbound { stamp, bytes, .. }
            if stamp.conn == acct(0) && bytes.0 == b"mode|m=halted")
    });
    assert!(closed.unwrap() < waited.unwrap(), "{records:?}");
    assert_eq!(session.current_on(ACCOUNT), Some(acct(0)));
}

#[tokio::test]
async fn a_stop_while_the_order_connection_connects_closes_the_account_stream() {
    let (trade, _accepts) = hanging().await;
    let mut account = ScriptedWs::start().await;
    let trade = format!("ws://{trade}/trade");
    let (config, _) = setup(Two::leak(), &trade, &account.url(), floor(10));
    let log = Log::default();
    let handler = Keep::new(&log);
    let slot = Rc::clone(&handler.control);
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *slot.borrow_mut() = Some(control);
    let script = async move {
        let mut a = account.accept().await;
        let _ = a.recv().await;
        a.send("mode|m=halted");
        a.send("mode|m=reduceonly");
        assert_eq!(a.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(events(&log).len(), 2);
    assert_eq!(ends(&log), [(2, acct(0))]);
    // A stop, not a drop: no epoch opened after it.
    assert_eq!(session.current_on(ACCOUNT), Some(acct(0)));
}

#[tokio::test]
async fn an_account_stream_that_never_opens_is_paced_while_the_order_connection_runs() {
    let mut trade = ScriptedWs::start().await;
    let (account, mut accepts) = refusing().await;
    let account = format!("ws://{account}/account");
    let (config, _) = setup(Two::leak(), &trade.url(), &account, floor(20));
    let log = Log::default();
    let (mut session, control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let _ = t.recv().await;
        for _ in 0..3 {
            accepts.recv().await.unwrap();
        }
        t.send("mode|m=halted");
        until(|| events(&watch).len() == 1).await;
        drop(control);
        assert_eq!(t.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let counters = session.counters();
    assert!(counters.failed_attempts >= 2, "{counters:?}");
    assert_eq!(ends(&log), [(1, key(0))]);
}

#[tokio::test]
async fn an_account_attempt_that_runs_out_its_deadline_fails_and_is_paced_again() {
    let mut trade = ScriptedWs::start().await;
    let (account, mut accepts) = hanging().await;
    let account = format!("ws://{account}/account");
    let pacing = ReconnectPacing::new(ms(10), ms(10), 100, Duration::from_secs(60), ms(50));
    let (config, _) = setup(Two::leak(), &trade.url(), &account, pacing.unwrap());
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut t = trade.accept().await;
        let _ = t.recv().await;
        // Each attempt is held unanswered past its deadline, and the next one comes.
        for _ in 0..3 {
            accepts.recv().await.unwrap();
        }
        drop(control);
        assert_eq!(t.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    let counters = session.counters();
    assert!(counters.failed_attempts >= 2, "{counters:?}");
    assert_eq!(session.current_on(ACCOUNT), Some(acct(0)));
}

#[tokio::test]
async fn an_attempt_the_connection_limit_refuses_waits_for_it() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Two::leak();
    let (mut config, _) = setup(venue, &trade.url(), &account.url(), floor(10));
    config.cfg.insert(CONNECTS, "2");
    let limits = venue.caps(&config.cfg).unwrap().limits;
    config.limiter = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
    let (mut session, control) = ExecSession::new(config, |_| {}).unwrap();
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // Both connections spent the minute's two: the account stream's next attempt waits.
        a.drop_conn();
        tokio::time::sleep(ms(300)).await;
        assert!(account.try_accept().is_none());
        drop(control);
        assert_eq!(t.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(session.counters().attempts, 2);
}

#[tokio::test]
async fn account_on_open_frames_that_can_never_fit_together_end_the_session() {
    let (trade, _accepts) = hanging().await;
    let mut account = ScriptedWs::start().await;
    let venue = Two::leak();
    let (mut config, _) = setup(venue, &format!("ws://{trade}/t"), &account.url(), floor(10));
    config.cfg.insert(HEAVY, "yes");
    let log = Log::default();
    let (mut session, _control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let script = async move {
        let mut a = account.accept().await;
        assert_eq!(a.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    assert_eq!(run.err(), Some(ExecSessionError::OpenNeverFits));
    // The epoch it opened is ended, the handler told, and retired as a drop.
    assert_eq!(ends(&log), [(0, acct(0))]);
    assert_eq!(session.current_on(ACCOUNT), Some(acct(1)));
}

#[tokio::test]
async fn a_nonce_source_refusing_the_account_streams_open_ends_the_session() {
    // While the order connection still connects, and once it is open.
    for trade_open in [false, true] {
        let mut trade = ScriptedWs::start().await;
        let (hung, _accepts) = hanging().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let account = format!("ws://{}/account", listener.local_addr().unwrap());
        let trade_url = if trade_open {
            trade.url()
        } else {
            format!("ws://{hung}/trade")
        };
        let (mut config, log) = setup(Two::leak(), &trade_url, &account, floor(10));
        config.nonces = Box::new(Counting {
            next: 0,
            refuse_from: 2,
            log,
        });
        let (mut session, _control) = ExecSession::new(config, |_| {}).unwrap();
        let script = async move {
            if trade_open {
                let mut t = trade.accept().await;
                let _ = t.recv().await;
            }
            // The account stream opens only now.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        };
        let (run, ()) = tokio::join!(session.run(), script);
        let refused = ExecSessionError::NonceRefused(NonceRefused::Exhausted);
        assert_eq!(
            run.err(),
            Some(refused),
            "order connection open: {trade_open}"
        );
    }
}

#[tokio::test]
async fn a_run_dropped_with_both_endpoints_open_has_their_epochs_ended_once() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (config, _) = setup(Two::leak(), &trade.url(), &account.url(), floor(10));
    let log = Log::default();
    let (mut session, _control) = ExecSession::new(config, Keep::new(&log)).unwrap();
    let kept = Rc::new(RefCell::new(Kept::default()));
    session.set_journal(Journal::new(Rc::clone(&kept)));
    let watch = Rc::clone(&log);
    tokio::select! {
        _ = session.run() => panic!("the run ended"),
        () = async {
            let mut t = trade.accept().await;
            let mut a = account.accept().await;
            let _ = (t.recv().await, a.recv().await);
            a.send("mode|m=halted");
            until(|| events(&watch).len() == 1).await;
        } => {}
    }
    assert!(ends(&log).is_empty());
    assert_eq!(session.run().await.err(), Some(ExecSessionError::Ended));
    assert_eq!(ends(&log), [(1, key(0)), (1, acct(0))]);
    drop(session);
    assert_eq!(ends(&log).len(), 2);
    // Each journaled closed once.
    let closed: Vec<ConnKey> = kept
        .borrow()
        .0
        .iter()
        .filter_map(|r| match r {
            Record::Control {
                ev: ControlEvent::Closed(k),
                ..
            } => Some(*k),
            _ => None,
        })
        .collect();
    assert_eq!(closed, [key(0), acct(0)]);
}

/// A handler whose every call panics, counting its calls.
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
async fn a_session_dropped_as_its_handler_panics_tells_no_endpoints_end() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (trade_url, account_url) = (trade.url(), account.url());
    let calls = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&calls);
    let venue = Two::leak();
    let driver = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (config, _) = setup(venue, &trade_url, &account_url, floor(10));
        let (mut session, _control) = ExecSession::new(config, AlwaysPanics(seen)).unwrap();
        rt.block_on(session.run())
    });
    let mut t = trade.accept().await;
    let mut a = account.accept().await;
    let _ = (t.recv().await, a.recv().await);
    a.send("mode|m=halted");
    let joined = tokio::task::spawn_blocking(move || driver.join());
    assert!(joined.await.unwrap().is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------------------------
// What a session refuses to drive.
// ---------------------------------------------------------------------------------------------

/// An in-process venue end that answers nothing.
struct Silent;

impl OrderEntryPeer for Silent {
    fn open(&mut self, _: MonoNs, _: WallNs) {}
    fn close(&mut self, _: MonoNs, _: WallNs) {}
    fn on_frame(&mut self, _: &[u8]) {}
    fn on_market(&mut self, _: &Envelope<fbc_core::MdEvent>) {}
    fn on_market_epoch_end(&mut self, _: ConnKey) {}
    fn next_due(&self) -> Option<MonoNs> {
        None
    }
    fn take_due(&mut self, _: MonoNs) -> Vec<Vec<u8>> {
        Vec::new()
    }
}

#[test]
fn a_session_refuses_endpoints_it_cannot_tell_apart_number_or_open() {
    let config = |account: &str| setup(Two::leak(), "ws://127.0.0.1:1/t", account, floor(10)).0;
    // Two endpoints on one stream.
    let mut same = config("ws://127.0.0.1:1/a");
    same.cfg.insert(STREAMS, "same");
    let err = ExecSession::new(same, |_| {}).err().unwrap();
    assert_eq!(err, ExecSessionError::SharedStream(EXEC_STREAM));
    // No connection number left for the second.
    let mut last = config("ws://127.0.0.1:1/a");
    last.conn = u16::MAX;
    let err = ExecSession::new(last, |_| {}).err().unwrap();
    assert_eq!(
        err,
        ExecSessionError::Session(SessionError::NoConnectionLeft)
    );
    // A second URL that cannot be opened.
    let err = ExecSession::new(config("http://127.0.0.1:1/a"), |_| {});
    assert!(matches!(
        err.err().unwrap(),
        ExecSessionError::Session(SessionError::Url(_))
    ));
    // An in-process stream stands in for exactly one connection (decision 0104).
    let stream = InProcessStream::new(Rc::new(RefCell::new(Silent)));
    let codec = Box::new(ToyExec::new(Box::new(ToySigner)));
    let err = ExecSession::in_process(config("ws://127.0.0.1:1/a"), codec, stream, |_| {});
    assert_eq!(err.err().unwrap(), ExecSessionError::Endpoints(2));
}

// ---------------------------------------------------------------------------------------------
// Codex's review of PR #188.
// ---------------------------------------------------------------------------------------------

/// A journal sink another thread reads: the records offered, as `Kept` keeps them.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<Record>>>);

impl JournalSink for Shared {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.0.lock().unwrap().push(record.clone());
        Recorded::Ok
    }

    fn record_ref(&mut self, _: TrafficClass, _: WallNs, record: RecordRef<'_>) -> Recorded {
        self.0.lock().unwrap().push(record.to_record());
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::Ok
    }
}

/// Codex P2 r4323792811: a codec that panics as the run's end stamps the frame waiting on the
/// account stream leaves that epoch journaled closed once, not again as the session drops while
/// unwinding.
#[tokio::test]
async fn a_codec_panicking_as_a_stop_stamps_an_account_frame_journals_its_epoch_closed_once() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let (trade_url, account_url) = (trade.url(), account.url());
    let records = Shared::default();
    let journal = records.clone();
    let venue = Two::leak();
    let driver = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (config, _) = setup(venue, &trade_url, &account_url, floor(10));
        let log = Log::default();
        let handler = Keep::new(&log);
        let slot = Rc::clone(&handler.control);
        let (mut session, control) = ExecSession::new(config, handler).unwrap();
        session.set_journal(Journal::new(Rc::new(RefCell::new(journal))));
        *slot.borrow_mut() = Some(control);
        rt.block_on(session.run())
    });
    let mut t = trade.accept().await;
    let mut a = account.accept().await;
    let _ = (t.recv().await, a.recv().await);
    // The handler stops the session on the first; the second waits behind it.
    a.send_all(["mode|m=reduceonly".to_owned(), "boom".to_owned()]);
    let joined = tokio::task::spawn_blocking(move || driver.join());
    assert!(joined.await.unwrap().is_err());
    let records = records.0.lock().unwrap();
    let closed = records.iter().filter(
        |r| matches!(r, Record::Control { ev: ControlEvent::Closed(k), .. } if *k == acct(0)),
    );
    assert_eq!(closed.count(), 1, "{records:?}");
}

/// A handler that notes, for each event, whether the session took places then.
struct Placing {
    orders: Rc<RefCell<Option<ExecOrders>>>,
    seen: Rc<RefCell<Vec<(ExecEvent, bool)>>>,
}

impl ExecHandler for Placing {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let placing = self.orders.borrow().as_ref().unwrap().may_place();
        self.seen.borrow_mut().push((env.body, placing));
    }
}

/// Codex P2 r4323792819: once the order connection's epoch ends in a batch that also wrote to
/// the account stream, an event heard while that write waits finds the session taking no
/// places.
#[tokio::test]
async fn an_event_heard_while_a_routed_write_waits_after_the_order_epoch_closed_takes_no_places() {
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let mut http = ScriptedHttp::start().await;
    let venue = Two::leak();
    let (mut config, _) = setup(venue, &trade.url(), &account.url(), floor(10));
    config.cfg.insert(HTTP, &http.url(""));
    let (orders, seen) = (Rc::default(), Rc::default());
    let handler = Placing {
        orders: Rc::clone(&orders),
        seen: Rc::clone(&seen),
    };
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *orders.borrow_mut() = Some(session.orders());
    let watch = Rc::clone(&seen);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        // The order connection armed and resynced: it takes places.
        t.send(AUTH_ACK);
        let armed = t.recv().await;
        let rpc = field(&armed, "rpc").to_owned();
        let resync = t.recv().await;
        let wm = resync.strip_prefix("resync|ts=").unwrap().to_owned();
        t.send(&format!("item|rpc={rpc}|i=0|res=ok"));
        t.send_all([format!("rsbegin|wm={wm}"), "rsend".to_owned()]);
        until(|| watch.borrow().len() == 4).await;
        assert!(watch.borrow().iter().any(|(_, placing)| *placing));
        // An account request out; the account stream stops reading.
        a.send("ask|path=/x");
        let asked = http.request().await;
        let release = a.hold();
        // One batch writes 64 MiB to the account stream, which waits, and ends the order
        // connection's epoch.
        t.send("both|kb=65536");
        assert_eq!(t.next().await, None);
        asked.answer("HTTP/1.1 200 OK", "now").await;
        until(|| watch.borrow().len() == 5).await;
        drop(control);
        release
    };
    let (run, release) = tokio::join!(session.run(), script);
    drop(release);
    run.unwrap();
    let seen = seen.borrow();
    assert_eq!(seen[4], (mode(VenueMode::Halted), false), "{seen:?}");
}

/// A handler that submits a fee query again each time one is not sent, from the order
/// connection's first end until it hears an account event.
struct Resubmitting {
    orders: Rc<RefCell<Option<ExecOrders>>>,
    on: bool,
    log: Log,
}

impl Resubmitting {
    fn submit(&self) {
        let orders = self.orders.borrow();
        let _ = orders
            .as_ref()
            .unwrap()
            .submit_control(ControlCommand::FeeQuery);
    }
}

impl ExecHandler for Resubmitting {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.on = false;
        self.log.borrow_mut().push(Heard::Event(Box::new(env)));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        if key == self::key(0) {
            self.on = true;
            self.submit();
        }
    }

    fn on_submitted(&mut self, handle: fbc_core::SubmitHandle) {
        if self.on && handle.receipt.is_err() {
            self.submit();
        }
    }
}

/// Codex P2 r4323792829: while the order connection waits to reconnect, a handler that submits
/// again from every refusal does not starve the account stream.
#[tokio::test]
async fn commands_submitted_without_end_while_the_order_connection_is_down_starve_no_account_frame()
{
    let (mut trade, mut account) = (ScriptedWs::start().await, ScriptedWs::start().await);
    let venue = Two::leak();
    let (config, _) = setup(venue, &trade.url(), &account.url(), floor(60_000));
    let (orders, log) = (Rc::default(), Log::default());
    let handler = Resubmitting {
        orders: Rc::clone(&orders),
        on: false,
        log: Rc::clone(&log),
    };
    let (mut session, control) = ExecSession::new(config, handler).unwrap();
    *orders.borrow_mut() = Some(session.orders());
    let watch = Rc::clone(&log);
    let script = async move {
        let mut t = trade.accept().await;
        let mut a = account.accept().await;
        let _ = (t.recv().await, a.recv().await);
        t.drop_conn();
        a.send("mode|m=halted");
        until(|| events(&watch).len() == 1).await;
        drop(control);
        assert_eq!(a.next().await, None);
    };
    let (run, ()) = tokio::join!(session.run(), script);
    run.unwrap();
    assert_eq!(events(&log)[0].stamp.conn, acct(0));
}
