//! A market-data session: drives one endpoint of a venue's plan through connection epochs and
//! the subscription reconciler (decisions 0002, 0014, 0023, 0027).
//!
//! Each epoch is one life of the connection, with a fresh [`MdCodec`] from
//! [`VenueFactory::md_codec`]: the session opens the endpoint through the [`Connector`], calls
//! the codec's `on_open`, then `subscribe` with the [`Reconciler`]'s difference. Every frame is
//! decoded inside [`dispatch_market_data`] under the venue's own [`VenueCaps`], and every event
//! the codec pushes is stamped into an [`Envelope`] (0014 item 2) and handed straight to the
//! consumer's [`MdHandler`], in ingest order. The codec's effects are executed in the order it
//! asked for them: frames are written, timers set and a reconnect closes the connection and opens
//! the next epoch. A timer of an older epoch fires into nothing: it is dropped and counted.
//!
//! An HTTP request the codec asks for runs beside the session's reads with its own timeout, and
//! its response, or the [`HttpFailure`] that stands for one, goes to `on_http` of the codec that
//! asked, and only while its epoch is current: one that comes back after a reconnect is dropped
//! and counted, so a snapshot asked for before it never anchors the new epoch's book (0027).
//!
//! A [`MdTransport::Poll`] endpoint opens no connection: its one epoch begins as the session
//! runs, its codec gets data only through the HTTP requests it asks for, and a frame or a
//! reconnect it names its stream in is a codec defect, refused and counted.
//!
//! Every frame, HTTP request and connection attempt is charged to the session's
//! [`RateLimiter`] first (decision 0030). A connection attempt the buckets refuse waits for
//! them; a subscribe call's frames they refuse wait, together and with the call outstanding in
//! the reconciler, until they have room for all of them (one by one only when they never can);
//! an HTTP request they refuse (it and the connection it opens) comes back to `on_http` as
//! [`HttpFailure::NotSent`]; any other frame they refuse is not written. The pong the
//! WebSocket layer sends for each ping is charged too, and a venue's HTTP 429 or 418 is counted
//! under the scopes its request, not its connection, was charged to.
//!
//! One thread drives a session (design §5.1): [`MdSession::run`] spawns no task, so the read,
//! the decode and the handler's call run in one call stack on the caller's current-thread
//! runtime. Not here yet: a bound on a stalled write (FBC-ha3).
//!
//! **Liveness (0033).** On a socket endpoint, each epoch sends its codec's [`Keepalive`] every
//! interval the codec declares (a WebSocket ping, or the codec's own frame as Safety traffic),
//! charged the keepalive's own rate charge; one the buckets refuse is not sent, as any other
//! frame they refuse. A keepalive with a zero interval is a codec defect, refused and counted,
//! and the epoch runs without one. Where the venue declares a `max_conn_lifetime`, the
//! connection is rotated the consumer's [`Liveness`] margin before it ends: the epoch closes
//! and the next opens at once, within the attempt budget but without the floor a drop waits,
//! and subscribes the desired set once. A stream that receives no frame, pings and pongs
//! included, within the consumer's silence window is reported stale (one [`MdEvent::Health`]
//! with [`FeedHealth::Stale`] per desired subscription, all under one stamp of the silent
//! epoch; a write the handler issues as it is told is not sent) and closed, and reconnects as
//! any drop does, through the pacing. A frame that was
//! waiting to be read when the window ran out (a write held the session) counts as heard, not
//! silence. A poll endpoint has none of these.
//!
//! **Kernel receive times and tick-to-wire (FBC-2y3, decision 0031).** On Linux each frame's
//! stamp carries the kernel receive time of the last packet read beneath TLS and WebSocket when
//! it completed ([`crate::Tcp`]); elsewhere, and for timer firings and HTTP results, it is
//! `None`. A write issued while an input is handled, by the codec's effects or through the
//! [`Outbox`] the handler is given, is attributed to that input; when it is Safety traffic and
//! the input's stamp has a kernel receive time, its completion is reported to the handler as a
//! [`TickToWire`]: write done minus that time, for the session's stream.
//!
//! **The journal (0006).** With a [`Journal`] set ([`MdSession::set_journal`]), the session
//! records everything that crosses the shard boundary as it happens, so replay can call each
//! epoch's codec where the live session did: each data frame with its stamp, before it is
//! decoded; each frame it writes, with its connection epoch, rpc and redaction spans, and then
//! `Written` once the write completed; each HTTP request as it starts, by the epoch whose codec
//! asked, and its result with headers or its [`HttpFailure`], one that comes back to an ended
//! epoch included; each timer firing, an ended epoch's included; and a connection's opening and
//! closing by [`ConnKey`] and each `subscribe` call. A record goes under the traffic class of
//! what it records (a frame or request under its effect's, everything else Normal), and nothing
//! waits on the journal: a record the sink has no room for is dropped and counted there, and
//! the frame is written all the same. A write that failed, or that the control's drop
//! interrupted, has no write result: whether any of it reached the venue is unknown, and the
//! connection's `Closed` follows. A frame, timer firing or HTTP result the session takes as the
//! control drops reaches no codec, and is journaled after the epoch's `Closed`, so a replay
//! ([`crate::MdReplay`]) feeds it to none either. Pings, pongs and close frames carry no data and are not
//! journaled. Until FBC-7lm, nothing a codec receives carries redaction spans, so inbound
//! frames and responses would be journaled verbatim: a session that carries a credential (its
//! endpoint URL has a redaction span, or it has sent a frame or HTTP request with one, or with a
//! header the codec marked or one of the journal's secret headers) journals no frame or
//! response it receives from then on, counting each withheld input instead (Codex r4178197275)
//! and omitting it at the sink, which counts it as dropped and marks the gap with a `Degraded`
//! marker (Codex r4178287664). An HTTP failure holds nothing received, so it is still journaled
//! (Codex r4179310275), and so is what the session sends, its spans as keyed hashes. Inbound
//! frames, HTTP requests, HTTP results and subscribe calls are offered to the sink borrowed
//! ([`RecordRef`]), so a sink with no room refuses them before they are copied, at the length
//! the journal's own encoding of them takes.

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{
    ConfigError, ConnKey, DecodeError, Effect, Effects, EndpointPlan, Envelope, FeedHealth,
    HttpFailure, HttpResponse, HttpTag, Keepalive, KeepaliveKind, KernelRxNs, MdCodec, MdEvent,
    MdSink, MdTransport, MonoNs, OpKind, RateCharge, RawFrame, SpecTable, Stamp, StreamId,
    Subscription, TimerTag, TrafficClass, VenueCaps, VenueConfig, VenueFactory, VenueMeta, Via,
    WallNs, WireSlice, dispatch_market_data,
};
use fbc_journal::{ControlEvent, Record, RecordRef, ResponseRef, WriteRes, is_secret_header};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::error::NetError;
use crate::http::{self, Bytes, Response, StatusCode};
use crate::journal::Journal;
use crate::liveness::{Alive, Liveness, LivenessError};
use crate::pacing::{Pacer, ReconnectPacing};
use crate::ratelimit::{RateError, RateLimiter, Refused, Request};
use crate::reconcile::{ReconcileError, Reconciler, SubscribeCall};
use crate::ws::{self, Message, WebSocket};

/// Where the consumer receives a session's events: called once per event, in ingest order, on
/// the thread that drives the session, before the next frame is read (decision 0023).
pub trait MdHandler {
    fn on_md(&mut self, env: Envelope<MdEvent>);

    /// What the session calls for each event: [`MdHandler::on_md`] by default. A handler that
    /// writes while it handles an event implements this and issues its writes through `out`
    /// (decision 0031).
    fn on_md_with(&mut self, env: Envelope<MdEvent>, out: &mut Outbox) {
        let _ = out;
        self.on_md(env);
    }

    /// A Safety-class write attributed to a frame with a kernel receive time completed: its
    /// tick-to-wire (decision 0031). Nothing by default.
    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        let _ = sample;
    }
}

/// The writes a handler issues while it handles an event ([`MdHandler::on_md_with`]): each goes
/// on the session's own stream once the codec's effects for the same input have, in the order
/// issued, charged to the buckets and journaled as a codec's frames are, and attributed to the
/// input being handled (decision 0031). A poll endpoint has no stream to write on: it refuses
/// and counts them, as it does a codec's frames.
#[derive(Debug)]
pub struct Outbox {
    stream: StreamId,
    sends: Vec<Effect>,
}

impl Outbox {
    /// An empty outbox for writes on `stream`.
    pub(crate) fn new(stream: StreamId) -> Outbox {
        Outbox {
            stream,
            sends: Vec::new(),
        }
    }

    /// Issues a write of `frame` as `class` traffic, charged `charge`.
    pub fn send(&mut self, frame: WireSlice, class: TrafficClass, charge: RateCharge) {
        self.sends.push(Effect::Send {
            stream: self.stream,
            frame,
            rpc: None,
            class,
            charge,
        });
    }

    /// Moves the writes issued into `fx`, after what is there.
    pub(crate) fn drain_into(&mut self, fx: &mut Effects) {
        self.sends.drain(..).for_each(|e| fx.push(e));
    }

    /// Drops the writes issued, unsent; how many there were.
    pub(crate) fn discard(&mut self) -> u64 {
        let issued = self.sends.len() as u64;
        self.sends.clear();
        issued
    }
}

/// The tick-to-wire of one Safety-class write (design §5.3, decision 0031): from the kernel
/// receive time of the frame it is attributed to until the write completed, both on the
/// realtime clock.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TickToWire {
    /// The stream written to.
    pub stream: StreamId,
    /// The stamp of the frame being handled when the write was issued; its `kernel_rx` is set.
    pub frame: Stamp,
    /// Write done minus the frame's kernel receive time, in nanoseconds; negative only if the
    /// wall clock stepped back between them.
    pub nanos: i64,
}

/// The tick-to-wire of a write of `class` on `stream` that completed at `written`, attributed
/// to the input stamped `origin`: only Safety traffic attributed to a frame with a kernel
/// receive time has one.
fn tick_to_wire(
    stream: StreamId,
    class: TrafficClass,
    origin: Option<Stamp>,
    written: WallNs,
) -> Option<TickToWire> {
    let frame = origin.filter(|_| class == TrafficClass::Safety)?;
    let KernelRxNs(rx) = frame.kernel_rx?;
    Some(TickToWire {
        stream,
        frame,
        nanos: written.0.saturating_sub(rx),
    })
}

impl<F: FnMut(Envelope<MdEvent>)> MdHandler for F {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self(env)
    }
}

/// The stamp source the sessions of one shard thread share: one ingest sequence, so ingest
/// order is shard-global, and one monotonic origin. Cloning it shares it.
#[derive(Clone, Debug)]
pub struct IngestClock {
    next: Rc<Cell<u64>>,
    origin: Instant,
}

impl Default for IngestClock {
    fn default() -> Self {
        IngestClock::new()
    }
}

impl IngestClock {
    /// A clock whose sequence starts at 0 and whose monotonic origin is now.
    pub fn new() -> IngestClock {
        IngestClock {
            next: Rc::new(Cell::new(0)),
            origin: Instant::now(),
        }
    }

    /// Stamps an input of `conn` arriving now, whose last packet the kernel received at
    /// `kernel_rx` when it knows.
    fn stamp(&self, conn: ConnKey, kernel_rx: Option<KernelRxNs>) -> Stamp {
        let ingest_seq = self.next.get();
        self.next.set(ingest_seq.wrapping_add(1));
        let (recv_mono, recv_wall) = self.now();
        Stamp {
            ingest_seq,
            kernel_rx,
            recv_mono,
            recv_wall,
            conn,
        }
    }

    /// Now, on the monotonic clock from the origin and on the wall clock.
    fn now(&self) -> (MonoNs, WallNs) {
        let mono = Instant::now().duration_since(self.origin).as_nanos();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        (
            MonoNs(u64::try_from(mono).unwrap_or(u64::MAX)),
            WallNs(i64::try_from(wall).unwrap_or(i64::MAX)),
        )
    }
}

/// What a session needs, all from the consumer.
pub struct MdSessionConfig {
    pub venue: &'static dyn VenueFactory,
    pub cfg: VenueConfig,
    /// The endpoint; its `subs` are the first desired set.
    pub plan: EndpointPlan,
    pub specs: SpecTable,
    pub connector: Connector,
    pub pacing: ReconnectPacing,
    pub clock: IngestClock,
    /// The most response-body bytes an HTTP request the codec asks for may read; a longer
    /// body is [`HttpFailure::Lost`].
    pub http_max_body: usize,
    /// The connection number stamped on this session's inputs, unique on its shard.
    pub conn: u16,
    /// The buckets of the venue's declared limits, shared with every session that counts
    /// against the same ones; built for exactly the venue's limits.
    pub limiter: RateLimiter,
    /// The silence window and the rotation margin (0033).
    pub liveness: Liveness,
}

/// Why a session could not start, or stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    /// The venue refused the configuration.
    Config(ConfigError),
    /// A venue's endpoint could not be given a connection number: its range is spent.
    NoConnectionLeft,
    /// No attempt could open the endpoint's URL; the error names the step, never the URL.
    Url(NetError),
    /// The connection has no epoch left to open.
    Epoch(EpochError),
    /// The reconciler refused a call the session made: a session defect.
    Reconcile(ReconcileError),
    /// The venue's rate limits cannot be counted, or the limiter is not theirs.
    Rates(RateError),
    /// The consumer's liveness settings do not fit the venue.
    Liveness(LivenessError),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::Config(e) => write!(f, "venue configuration refused: {e}"),
            SessionError::NoConnectionLeft => {
                f.write_str("the venue has no connection number left for an endpoint")
            }
            SessionError::Url(e) => write!(f, "the endpoint cannot be opened: {e}"),
            SessionError::Epoch(e) => write!(f, "{e}"),
            SessionError::Reconcile(e) => write!(f, "{e}"),
            SessionError::Rates(e) => write!(f, "{e}"),
            SessionError::Liveness(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<EpochError> for SessionError {
    fn from(e: EpochError) -> Self {
        SessionError::Epoch(e)
    }
}

impl From<RateError> for SessionError {
    fn from(e: RateError) -> Self {
        SessionError::Rates(e)
    }
}

impl From<LivenessError> for SessionError {
    fn from(e: LivenessError) -> Self {
        SessionError::Liveness(e)
    }
}

impl From<ReconcileError> for SessionError {
    fn from(e: ReconcileError) -> Self {
        SessionError::Reconcile(e)
    }
}

/// Changes a running session's desired subscriptions; dropping it stops the session, even while
/// a write waits on a peer that stopped reading.
#[derive(Debug)]
pub struct MdControl {
    desired: watch::Sender<BTreeSet<Subscription>>,
    /// Never sent on: its drop is the stop signal a pending write observes.
    _stop: watch::Sender<()>,
}

impl MdControl {
    /// Replaces the desired set; the session sends only the difference. Only the latest set
    /// counts, so nothing queues behind a slow session.
    pub fn set_desired(&self, subs: impl IntoIterator<Item = Subscription>) {
        self.desired.send_replace(subs.into_iter().collect());
    }
}

/// What a session counted, besides the stale inputs [`MdSession::stale`] reports.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct MdCounters {
    /// Connection attempts started.
    pub attempts: u64,
    /// Attempts that did not open, by their deadline or at all.
    pub failed_attempts: u64,
    /// Frames the codec could not decode.
    pub decode_errors: u64,
    /// Subscribe calls the codec refused; their subscriptions stay pending.
    pub refused_subscribes: u64,
    /// Effects refused as codec defects: a frame or reconnect for another stream, or for a
    /// poll endpoint's own, and a keepalive with a zero interval; and a write a handler issued
    /// as it was told its stream fell silent.
    pub refused_effects: u64,
    /// Keepalives due; one the buckets refused was not sent.
    pub keepalives: u64,
    /// Epochs ended because their stream fell silent.
    pub silences: u64,
    /// Epochs ended to rotate the connection before the venue's lifetime.
    pub rotations: u64,
    /// Inbound frames and HTTP responses not journaled because the session carries a
    /// credential (until FBC-7lm).
    pub journal_withheld: u64,
}

/// One market-data endpoint, driven by [`MdSession::run`].
pub struct MdSession<H> {
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    plan: EndpointPlan,
    url: String,
    caps: VenueCaps,
    specs: SpecTable,
    connector: Connector,
    clock: IngestClock,
    pacer: Pacer,
    /// The consumer's silence window.
    silence: Duration,
    /// How long after it opens a connection is rotated; `None` where the venue sets no limit.
    rotate_after: Option<Duration>,
    epochs: Epochs,
    rec: Reconciler,
    desired: watch::Receiver<BTreeSet<Subscription>>,
    stop: watch::Receiver<()>,
    /// Pending timers: deadline, order set, epoch, tag.
    timers: BinaryHeap<Reverse<(Instant, u64, u32, TimerTag)>>,
    timer_seq: u64,
    /// HTTP requests in flight, each with the epoch that asked.
    http: FuturesUnordered<Pending>,
    http_max_body: usize,
    counters: MdCounters,
    rates: RateLimiter,
    /// A subscribe call one of whose frames the buckets refused, with the effects its codec
    /// asked for that have not gone yet, outstanding until they have room; and when to try
    /// again.
    waiting: Option<(SubscribeCall, VecDeque<Effect>)>,
    rate_retry: Option<Instant>,
    journal: Option<Journal>,
    /// The session has carried a credential: nothing it receives is journaled (until FBC-7lm).
    credentialed: Cell<bool>,
    /// Inputs withheld from the journal since ([`MdCounters::journal_withheld`]).
    withheld: Cell<u64>,
    /// The writes the handler issues while it handles an input.
    outbox: Outbox,
    handler: H,
}

/// An HTTP request in flight.
type Pending = Pin<Box<dyn Future<Output = Answered>>>;

/// An HTTP request's result, with the epoch and tag of the codec call that asked for it and
/// the request's traffic class.
struct Answered {
    epoch: u32,
    tag: HttpTag,
    class: TrafficClass,
    result: Result<Response<Bytes>, HttpFailure>,
}

/// How a connected epoch ended.
enum End {
    Stop,
    /// The session stopped and the epoch's `Closed` is already journaled, ahead of the inputs
    /// taken with the stop.
    StopClosed,
    Dropped,
    /// Closed to rotate before the venue's lifetime.
    Rotated,
}

/// What woke a disconnected session: the pacer, a timer, an ended epoch's HTTP result (already
/// dropped and counted), or the control (false: dropped).
enum Idle {
    Attempt,
    Timer,
    Http,
    Desired(bool),
}

/// What woke a connected session.
enum Wake {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Timer,
    Http(Answered),
    Desired(bool),
    Keepalive,
    Silent,
    Rotate,
}

impl<H: MdHandler> MdSession<H> {
    /// A session for `config`'s endpoint, handing events to `handler`, and its control.
    pub fn new(
        config: MdSessionConfig,
        handler: H,
    ) -> Result<(MdSession<H>, MdControl), SessionError> {
        let (url, credentialed) = match &config.plan.transport {
            MdTransport::Socket { url } => {
                ws::check_url(url.as_str()).map_err(SessionError::Url)?;
                (url.as_str().to_owned(), !url.redactions().is_empty())
            }
            MdTransport::Poll { base_url } => (String::new(), !base_url.redactions().is_empty()),
        };
        let caps = config
            .venue
            .caps(&config.cfg)
            .map_err(SessionError::Config)?;
        config.limiter.check(&caps.limits)?;
        let rotate_after = config.liveness.rotate_after(caps.md.max_conn_lifetime)?;
        let epochs = Epochs::new(config.conn);
        let mut rec = Reconciler::new(epochs.current());
        let first: BTreeSet<_> = config.plan.subs.iter().copied().collect();
        let _ = rec.set_desired(first.iter().copied());
        let (tx, desired) = watch::channel(first);
        let (stop_tx, stop) = watch::channel(());
        let outbox = Outbox::new(config.plan.stream);
        let session = MdSession {
            venue: config.venue,
            cfg: config.cfg,
            plan: config.plan,
            url,
            caps,
            specs: config.specs,
            connector: config.connector,
            clock: config.clock,
            pacer: Pacer::new(config.pacing),
            silence: config.liveness.silence(),
            rotate_after,
            epochs,
            rec,
            desired,
            stop,
            timers: BinaryHeap::new(),
            timer_seq: 0,
            http: FuturesUnordered::new(),
            http_max_body: config.http_max_body,
            counters: MdCounters::default(),
            rates: config.limiter,
            waiting: None,
            rate_retry: None,
            journal: None,
            credentialed: Cell::new(credentialed),
            withheld: Cell::new(0),
            outbox,
            handler,
        };
        let control = MdControl {
            desired: tx,
            _stop: stop_tx,
        };
        Ok((session, control))
    }

    /// The current connection epoch.
    pub fn current(&self) -> ConnKey {
        self.epochs.current()
    }

    /// Inputs of an older epoch dropped, by kind.
    pub fn stale(&self, input: Input) -> u64 {
        self.epochs.stale(input)
    }

    pub fn counters(&self) -> MdCounters {
        MdCounters {
            journal_withheld: self.withheld.get(),
            ..self.counters
        }
    }

    /// Records everything the session sends and receives into `journal` from now on (0006);
    /// the module docs say what is recorded.
    pub fn set_journal(&mut self, journal: Journal) {
        self.journal = Some(journal);
    }

    /// Offers the record `make` builds, if the session has a journal, under `class`.
    fn journal(&self, class: TrafficClass, now: WallNs, make: impl FnOnce() -> Record) {
        if let Some(journal) = &self.journal {
            journal.record(class, now, &make());
        }
    }

    /// The journal an input of `class` is offered to: none when the session has no journal,
    /// or when it has carried a credential, in which case the input is withheld, counted, and
    /// omitted at the sink, which marks the gap (Codex r4178287664).
    fn input_journal(&self, class: TrafficClass, now: WallNs) -> Option<&Journal> {
        let journal = self.journal.as_ref()?;
        if self.credentialed.get() {
            self.withheld.set(self.withheld.get() + 1);
            journal.omit(class, now);
            return None;
        }
        Some(journal)
    }

    /// Records a connection change or a subscribe call.
    fn control(&self, ev: impl FnOnce() -> ControlEvent) {
        let (at, now) = self.clock.now();
        self.journal(TrafficClass::Normal, now, || Record::Control {
            at,
            ev: ev(),
        });
    }

    /// Connects, reconnects as paced, and delivers events until the [`MdControl`] is dropped;
    /// a poll endpoint opens nothing. HTTP requests still in flight when it ends are dropped.
    pub async fn run(&mut self) -> Result<(), SessionError> {
        let ran = match self.plan.transport {
            MdTransport::Socket { .. } => self.run_socket().await,
            // Nothing drops a poll endpoint's one epoch: it ends only when the session stops.
            MdTransport::Poll { .. } => self.connected(None).await.map(|_| ()),
        };
        self.http.clear();
        ran
    }

    async fn run_socket(&mut self) -> Result<(), SessionError> {
        loop {
            let mut at = self.pacer.next_attempt(Instant::now());
            let mut waiting = true;
            while waiting {
                // The control first: once it has dropped, no attempt starts, even one that fell
                // due at the same time.
                let timer = self.next_deadline();
                let idle = tokio::select! {
                    biased;
                    r = self.desired.changed() => Idle::Desired(r.is_ok()),
                    _ = sleep_or_never(at) => Idle::Attempt,
                    _ = sleep_or_never(timer) => Idle::Timer,
                    Some(done) = self.http.next() => {
                        let done = self.stamp_http(done);
                        let _ = self.admit_http(done)?;
                        Idle::Http
                    }
                };
                match idle {
                    // A connection the buckets refuse waits for them (decision 0030).
                    Idle::Attempt => {
                        let refused = self.rates.connect(Instant::now()).err();
                        at = refused.map_or(at, |r| r.ready_at);
                        waiting = refused.is_some();
                    }
                    // Only an ended epoch's timers and requests wait while disconnected: each
                    // comes back into nothing.
                    Idle::Timer => {
                        let _ = self.take_timer()?;
                    }
                    Idle::Http => {}
                    Idle::Desired(false) => return Ok(()),
                    Idle::Desired(true) => {
                        let subs = self.desired.borrow_and_update().clone();
                        let _ = self.rec.set_desired(subs);
                    }
                }
            }
            self.pacer.attempted(Instant::now());
            self.counters.attempts += 1;
            // A connect fails at its deadline, is stopped by the control's drop, keeps the
            // desired set current and fires an ended epoch's timers as they fall due. The
            // control first: a drop wins over a handshake that completed at the same time.
            let deadline = Instant::now().checked_add(self.pacer.deadline());
            let opened = {
                let (connector, url) = (self.connector.clone(), self.url.clone());
                let connect = connector.websocket(&url);
                tokio::pin!(connect);
                loop {
                    let timer = self.next_deadline();
                    tokio::select! {
                        biased;
                        r = self.desired.changed() => match r {
                            Ok(()) => {
                                let subs = self.desired.borrow_and_update().clone();
                                let _ = self.rec.set_desired(subs);
                            }
                            Err(_) => return Ok(()),
                        },
                        opened = &mut connect => break opened.ok(),
                        _ = sleep_or_never(deadline) => break None,
                        _ = sleep_or_never(timer) => {
                            let _ = self.take_timer()?;
                        }
                        Some(done) = self.http.next() => {
                            let done = self.stamp_http(done);
                            let _ = self.admit_http(done)?;
                        }
                    }
                }
            };
            let Some(ws) = opened else {
                self.counters.failed_attempts += 1;
                self.pacer.failed(Instant::now());
                continue;
            };
            self.pacer.opened();
            // The ended connection's buckets are forgotten even when the epoch ended in an
            // error (Codex r4179720972).
            let end = self.connected(Some(ws)).await;
            self.rates.closed(self.current());
            match end? {
                End::Stop | End::StopClosed => return Ok(()),
                End::Dropped => self.pacer.dropped(Instant::now()),
                // A planned close, not a drop: the next epoch opens at once, within the
                // budget, since the floor after the last drop or failure passed before this
                // connection opened.
                End::Rotated => {}
            }
            let key = self.epochs.advance()?;
            self.rec.begin_epoch(key)?;
        }
    }

    /// One epoch on the open socket `ws`, or of a poll endpoint (`None`), with a fresh codec,
    /// until it drops or the session stops.
    async fn connected(&mut self, ws: Option<WebSocket>) -> Result<End, SessionError> {
        // A control that has already dropped stops the session before a codec is built, so
        // nothing is sent or asked for after it (Codex r4177698436, r4177790164). A poll
        // endpoint then opens nothing; a socket just opened is journaled opened and closed, and
        // dropped unused, however late the drop comes (Codex r4179310270).
        if ws.is_none() && self.stop.has_changed().is_err() {
            return Ok(End::Stop);
        }
        let key = self.current();
        self.control(|| ControlEvent::Opened(key));
        let end = if self.stop.has_changed().is_err() {
            Ok(End::Stop)
        } else {
            self.epoch(ws, key).await
        };
        if let Ok(End::StopClosed) = end {
            return Ok(End::Stop);
        }
        self.control(|| ControlEvent::Closed(key));
        end
    }

    /// The epoch `key`, opened on `ws`, until it drops or the session stops.
    async fn epoch(
        &mut self,
        mut ws: Option<WebSocket>,
        key: ConnKey,
    ) -> Result<End, SessionError> {
        // The epoch's plan carries the subscriptions wanted now, not the first ones, including a
        // change that arrived as the connection opened.
        let subs = self.desired.borrow_and_update().clone();
        let _ = self.rec.set_desired(subs);
        let plan = EndpointPlan {
            subs: self.rec.desired().iter().copied().collect(),
            ..self.plan.clone()
        };
        let mut codec = self.venue.md_codec(&self.cfg, &plan);
        // Liveness runs on a socket only (0033): the keepalive, the silence window from the
        // last frame heard and the rotation from the open.
        let (silence, rotate_after) = (self.silence, self.rotate_after);
        let (mut alive, refused) =
            Alive::open(ws.is_some(), codec.keepalive(), silence, rotate_after);
        self.counters.refused_effects += u64::from(refused);
        let mut rotated = false;
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        let call = self.rec.opened(key)?;
        (self.waiting, self.rate_retry) = (None, None);
        let mut open = self
            .execute(&mut ws, codec.as_mut(), fx, false, None)
            .await?
            && self.subscribe(&mut ws, codec.as_mut(), call).await?;
        while open {
            let wake = tokio::select! {
                frame = next_frame(&mut ws) => Wake::Frame(frame),
                _ = sleep_or_never(self.next_deadline()) => Wake::Timer,
                // A subscribe call waiting for the buckets tries again as if the set changed.
                _ = sleep_or_never(self.rate_retry) => Wake::Desired(true),
                Some(done) = self.http.next() => Wake::Http(done),
                r = self.desired.changed() => Wake::Desired(r.is_ok()),
                _ = sleep_or_never(alive.keepalive_at) => Wake::Keepalive,
                _ = sleep_or_never(alive.silent_at) => Wake::Silent,
                _ = sleep_or_never(alive.rotate_at) => Wake::Rotate,
            };
            // A frame already waiting when the window ran out, because a write held the
            // session, was heard: it is read, not reported silent.
            let wake = match wake {
                Wake::Silent => match next_frame(&mut ws).now_or_never() {
                    Some(frame) => Wake::Frame(frame),
                    None => Wake::Silent,
                },
                other => other,
            };
            if let Wake::Frame(Some(Ok(_))) = wake {
                alive.heard();
            }
            // The kernel receive time of the last packet read beneath the frame, if any.
            let rx = ws.as_ref().and_then(|ws| ws.get_ref().kernel_rx());
            // The control first: what woke with its drop reaches no codec (Codex r4177887269).
            // The epoch is journaled closed first, and a frame read, a timer taken or an HTTP
            // result that came back with the drop is then stamped and journaled after it (Codex
            // r4178646794, r4179379935), so replay, which feeds a closed epoch nothing, feeds
            // it to no codec either (Codex r4179805832).
            if self.stop.has_changed().is_err() || matches!(wake, Wake::Desired(false)) {
                self.control(|| ControlEvent::Closed(key));
                match wake {
                    Wake::Frame(Some(Ok(message))) => {
                        self.take_in(key, rx, &message);
                    }
                    Wake::Timer => {
                        let _ = self.take_timer()?;
                    }
                    Wake::Http(done) => {
                        self.stamp_http(done);
                    }
                    _ => {}
                }
                if let Some(ws) = ws.as_mut() {
                    close(ws, &self.rates, key);
                }
                return Ok(End::StopClosed);
            }
            open = match wake {
                Wake::Frame(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    let origin = self.decode(codec.as_mut(), key, rx, &message, &mut fx);
                    self.execute(&mut ws, codec.as_mut(), fx, false, origin)
                        .await?
                }
                Wake::Frame(_) => false,
                Wake::Timer => self.fire(&mut ws, codec.as_mut()).await?,
                Wake::Http(done) => match self.admit_http(self.stamp_http(done))? {
                    Some((stamp, done)) => {
                        let mut fx = Effects::new();
                        self.answer(codec.as_mut(), stamp, done, &mut fx);
                        self.execute(&mut ws, codec.as_mut(), fx, false, Some(stamp))
                            .await?
                    }
                    None => true,
                },
                // A dropped control stopped the epoch above.
                Wake::Desired(_) => {
                    let subs = self.desired.borrow_and_update().clone();
                    let call = self.rec.set_desired(subs);
                    self.subscribe(&mut ws, codec.as_mut(), call).await?
                }
                Wake::Keepalive => {
                    alive.beat();
                    let mut open = true;
                    if let Some(k) = &alive.keepalive {
                        open = self.keep_alive(&mut ws, codec.as_mut(), k).await?;
                    }
                    open
                }
                Wake::Silent => {
                    self.counters.silences += 1;
                    self.report_silent(key);
                    if let Some(ws) = ws.as_mut() {
                        close(ws, &self.rates, key);
                    }
                    false
                }
                Wake::Rotate => {
                    self.counters.rotations += 1;
                    rotated = true;
                    if let Some(ws) = ws.as_mut() {
                        close(ws, &self.rates, key);
                    }
                    false
                }
            };
        }
        // A write the control's drop interrupted ends the session, not just the epoch.
        if self.stop.has_changed().is_err() {
            return Ok(End::Stop);
        }
        Ok(if rotated { End::Rotated } else { End::Dropped })
    }

    /// Sends `keepalive` on the epoch's socket, charged its own rate charge as Safety traffic
    /// and attributed to no input; false when the epoch ended.
    async fn keep_alive(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        keepalive: &Keepalive,
    ) -> Result<bool, SessionError> {
        self.counters.keepalives += 1;
        match &keepalive.kind {
            KeepaliveKind::Frame(frame) => {
                let mut fx = Effects::new();
                fx.push(Effect::Send {
                    stream: self.plan.stream,
                    frame: frame.clone(),
                    rpc: None,
                    class: TrafficClass::Safety,
                    charge: keepalive.charge,
                });
                self.execute(ws, codec, fx, false, None).await
            }
            KeepaliveKind::WsPing => {
                // Only a socket has a keepalive; the effects of a result answered while the
                // ping is written follow it.
                let ping = Request {
                    charge: keepalive.charge,
                    via: Via::Frame,
                    class: TrafficClass::Safety,
                };
                let key = self.current();
                let (mut open, mut effects) = (true, VecDeque::new());
                if let Some(socket) = ws.as_mut()
                    && self.rates.charge(Instant::now(), key, &[ping]).is_ok()
                {
                    let ping = Message::Ping(Default::default());
                    open = self.write(socket, codec, ping, &mut effects).await?;
                }
                Ok(open && self.run_effects(ws, codec, effects).await?)
            }
        }
    }

    /// Reports every desired subscription of the silent epoch `key` stale, under one stamp. A
    /// write the handler issues meanwhile is not sent, on the closing connection or the next:
    /// it is counted with the refused effects.
    fn report_silent(&mut self, key: ConnKey) {
        let stamp = self.clock.stamp(key, None);
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.epochs,
            out: &mut self.outbox,
            stamp,
        };
        for sub in self.rec.desired() {
            let (inst, feed, h) = (sub.inst, sub.feed, FeedHealth::Stale);
            sink.push(VenueMeta::NONE, MdEvent::Health { inst, feed, h });
        }
        self.counters.refused_effects += self.outbox.discard();
    }

    /// Stamps one message of epoch `key`, whose last packet the kernel received at `rx` when it
    /// knows, and journals it; its stamp and data frame, when it carries data. Pings, pongs and
    /// close frames take their place in ingest order but carry no data; a ping or a close is
    /// charged for the answer the WebSocket layer sends to it.
    fn take_in<'m>(
        &mut self,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        message: &'m Message,
    ) -> Option<(Stamp, RawFrame<'m>)> {
        let stamp = self.clock.stamp(key, rx);
        let raw = match message {
            Message::Text(text) => RawFrame::Text(text.as_str()),
            Message::Binary(bytes) => RawFrame::Binary(bytes.as_ref()),
            // The WebSocket layer answers a ping with a pong of its own, and a close with a
            // close (decision 0030, Codex r4179720976).
            Message::Ping(_) | Message::Close(_) => {
                self.rates.record(Instant::now(), key, CONTROL);
                return None;
            }
            _ => return None,
        };
        // Offered borrowed, so a full journal refuses it before it is copied (Codex
        // r4178252055).
        let (class, now) = (TrafficClass::Normal, stamp.recv_wall);
        if let Some(journal) = self.input_journal(class, now) {
            journal.record_ref(class, now, RecordRef::Inbound { stamp, frame: raw });
        }
        Some((stamp, raw))
    }

    /// Stamps and journals one message of epoch `key` and decodes it ([`Self::take_in`]); the
    /// handler's writes follow the codec's effects in `fx`. The stamp of a data frame, which
    /// the effects are attributed to.
    fn decode(
        &mut self,
        codec: &mut dyn MdCodec,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        message: &Message,
        fx: &mut Effects,
    ) -> Option<Stamp> {
        let (stamp, raw) = self.take_in(key, rx, message)?;
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.epochs,
            out: &mut self.outbox,
            stamp,
        };
        if feed_frame(codec, &self.caps, &self.specs, raw, &mut sink, fx).is_err() {
            self.counters.decode_errors += 1;
        }
        self.outbox.drain_into(fx);
        Some(stamp)
    }

    /// Hands a current epoch's HTTP result to the codec that asked for it, under `stamp`; the
    /// handler's writes follow the codec's effects in `fx`.
    fn answer(&mut self, codec: &mut dyn MdCodec, stamp: Stamp, done: Answered, fx: &mut Effects) {
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.epochs,
            out: &mut self.outbox,
            stamp,
        };
        let (specs, caps) = (&self.specs, &self.caps);
        let decoded = with_response(&done.result, |resp| {
            feed_http(codec, caps, specs, done.tag, resp, &mut sink, fx)
        });
        if decoded.is_err() {
            self.counters.decode_errors += 1;
        }
        self.outbox.drain_into(fx);
    }

    /// Fires the earliest timer, which is due: an ended epoch's into nothing, the current
    /// epoch's into the codec. The next due timer wakes the session again at once.
    async fn fire(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
    ) -> Result<bool, SessionError> {
        let mut open = true;
        if let Some((stamp, tag)) = self.take_timer()? {
            let mut sink = Sink {
                handler: &mut self.handler,
                epochs: &mut self.epochs,
                out: &mut self.outbox,
                stamp,
            };
            let mut fx = Effects::new();
            feed_timer(codec, stamp, tag, &mut sink, &mut fx);
            self.outbox.drain_into(&mut fx);
            open = self.execute(ws, codec, fx, false, Some(stamp)).await?;
        }
        Ok(open)
    }

    /// Sends `call` and every call the reconciler yields after it, after the call waiting for
    /// the buckets, if any; false when the epoch ended. The codec is asked once per call, and
    /// its frames are charged together: when the buckets refuse them, the call stays
    /// outstanding in the reconciler (its subscriptions pending, a change made meanwhile held
    /// for the call after it) and its effects wait, unexecuted, until the buckets have room
    /// (decision 0030), so the codec's state never runs ahead of what the venue was sent. Only
    /// frames that can never fit together go one by one, so a call too large for the buckets
    /// at once still goes.
    async fn subscribe(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        mut call: Option<SubscribeCall>,
    ) -> Result<bool, SessionError> {
        let mut open = true;
        if let Some((this, fx)) = self.waiting.take() {
            (open, call) = self.send_call(ws, codec, this, fx).await?;
        }
        while open && let Some(this) = call.take() {
            // Offered borrowed, so a full journal refuses it before the sets are copied (Codex
            // r4178567381).
            let (at, now) = self.clock.now();
            if let Some(journal) = &self.journal {
                let (conn, add, remove) = (self.current(), this.add(), this.remove());
                let call = RecordRef::Subscribe {
                    at,
                    conn,
                    add,
                    remove,
                };
                journal.record_ref(TrafficClass::Normal, now, call);
            }
            let mut fx = Effects::new();
            let asked = codec.subscribe(this.add(), this.remove(), &self.specs, &mut fx);
            // A call the codec refused pushed nothing and waits for a retry, a change or an
            // epoch.
            (open, call) = match asked {
                Ok(()) => {
                    self.send_call(ws, &mut *codec, this, fx.take().into())
                        .await?
                }
                Err(_) => {
                    self.counters.refused_subscribes += 1;
                    (true, self.rec.refused(this)?)
                }
            };
        }
        Ok(open)
    }

    /// Executes `effects`, which the codec asked for `call`, in order, and settles the call as
    /// sent, yielding the next call; or, when its frames must wait, keeps the call and what is
    /// left of its effects waiting. The frames left go together once the buckets have room for
    /// all of them (Codex r4179558357); only when they never can, each goes once the buckets
    /// have room for it (Codex r4179474176), and a frame that can never fit ends the session
    /// with [`RateError::NeverFits`]. False when the epoch ended.
    async fn send_call(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        call: SubscribeCall,
        mut effects: VecDeque<Effect>,
    ) -> Result<(bool, Option<SubscribeCall>), SessionError> {
        let (key, own) = (self.current(), self.plan.stream);
        self.rate_retry = None;
        let socket = ws.is_some();
        let frames: Vec<Request> = effects
            .iter()
            .filter_map(|e| frame_of(e, own, socket))
            .collect();
        match self.rates.charge(Instant::now(), key, &frames) {
            Ok(_) => {
                let mut all = Effects::new();
                effects.into_iter().for_each(|e| all.push(e));
                return match self.execute(ws, codec, all, true, None).await? {
                    true => Ok((true, self.rec.sent(call)?)),
                    false => Ok((false, None)),
                };
            }
            Err(Refused { ready_at: Some(at) }) => {
                (self.waiting, self.rate_retry) = (Some((call, effects)), Some(at));
                return Ok((true, None));
            }
            Err(Refused { ready_at: None }) => {}
        }
        loop {
            let socket = ws.is_some();
            let effect = match next_of_call(&mut effects, &self.rates, key, own, socket)? {
                CallStep::Run(effect) => effect,
                CallStep::Wait(at) => {
                    (self.waiting, self.rate_retry) = (Some((call, effects)), Some(at));
                    return Ok((true, None));
                }
                CallStep::Done => return Ok((true, self.rec.sent(call)?)),
            };
            let mut one = Effects::new();
            one.push(effect);
            if !self.execute(ws, codec, one, true, None).await? {
                return Ok((false, None));
            }
        }
    }

    /// Executes `fx` of `codec` in order; false when the socket failed or a reconnect was asked
    /// for, which ends the epoch and leaves the rest unexecuted. A poll endpoint (`ws` is
    /// `None`) refuses every frame and reconnect. Each frame is charged as its turn comes,
    /// unless `charged` says `fx`'s were already, and one the buckets refuse is not written;
    /// effects asked for meanwhile are charged as theirs come. `fx` is attributed to the input
    /// stamped `origin`, and effects asked for meanwhile to the input that asked: a Safety
    /// frame's completion is reported as its tick-to-wire when that input has a kernel receive
    /// time.
    async fn execute(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        mut fx: Effects,
        charged: bool,
        origin: Option<Stamp>,
    ) -> Result<bool, SessionError> {
        let effects = fx.take().into_iter();
        let effects = effects.map(|e| (e, charged, origin)).collect();
        self.run_effects(ws, codec, effects).await
    }

    /// Executes `effects` in order, each with whether it was charged already and the input it
    /// is attributed to ([`Self::execute`]).
    async fn run_effects(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        mut effects: VecDeque<(Effect, bool, Option<Stamp>)>,
    ) -> Result<bool, SessionError> {
        let key = self.current();
        let epoch = key.epoch;
        let own = self.plan.stream;
        let mut open = true;
        while open
            && let Some((effect, origin)) =
                next_admitted(&mut effects, &self.rates, key, own, ws.is_some())
        {
            match (effect, ws.as_mut()) {
                (
                    Effect::Send {
                        stream,
                        frame,
                        rpc,
                        class,
                        ..
                    },
                    Some(ws),
                ) if stream == own => {
                    let bytes = frame.bytes();
                    let text = std::str::from_utf8(bytes).map(Message::text);
                    let message = text.unwrap_or_else(|_| Message::binary(bytes.to_vec()));
                    let (conn, rpc) = (self.current(), rpc.map(|call| call.id));
                    if !frame.redactions().is_empty() {
                        self.credentialed.set(true);
                    }
                    let (at, now) = self.clock.now();
                    self.journal(class, now, || Record::Outbound {
                        at,
                        conn,
                        rpc,
                        frame,
                    });
                    open = self.write(ws, codec, message, &mut effects).await?;
                    if open {
                        let (at, now) = self.clock.now();
                        self.journal(class, now, || Record::WriteResult {
                            at,
                            conn,
                            rpc,
                            result: WriteRes::Written,
                        });
                        let tick = tick_to_wire(own, class, origin, now);
                        tick.into_iter()
                            .for_each(|t| self.handler.on_tick_to_wire(t));
                    }
                }
                (Effect::Timer { tag, after }, _) => {
                    // A timer past the end of the clock never fires.
                    if let Some(at) = Instant::now().checked_add(after) {
                        self.timer_seq += 1;
                        self.timers.push(Reverse((at, self.timer_seq, epoch, tag)));
                    }
                }
                (Effect::Reconnect { stream, .. }, Some(ws)) if stream == own => {
                    close(ws, &self.rates, key);
                    open = false;
                }
                (ask @ Effect::Http { .. }, _) => self.ask(epoch, ask),
                (Effect::Send { .. } | Effect::Reconnect { .. }, _) => {
                    self.counters.refused_effects += 1;
                }
            }
        }
        Ok(open)
    }

    /// Writes `message` on `ws`; false when the write failed or the control's drop interrupted
    /// it. Requests in flight keep going while the write waits. A result that comes back
    /// meanwhile reaches the codec at once, so the handler gets its events in the shard's
    /// ingest order (Codex r4177698441). A request the codec asks for then starts at once, its
    /// timeout running from now (Codex r4177887264); its other effects join `effects`, behind
    /// the rest of the batch, uncharged and attributed to the result. A request behind a
    /// reconnect of this stream, still queued in `effects` or asked for first, waits too: that
    /// reconnect ends the epoch before its turn, so it is never sent (Codex r4177934308).
    async fn write(
        &mut self,
        ws: &mut WebSocket,
        codec: &mut dyn MdCodec,
        message: Message,
        effects: &mut VecDeque<(Effect, bool, Option<Stamp>)>,
    ) -> Result<bool, SessionError> {
        let (epoch, own) = (self.current().epoch, self.plan.stream);
        let send = ws.send(message);
        tokio::pin!(send);
        loop {
            let done = tokio::select! {
                biased;
                sent = &mut send => return Ok(sent.is_ok()),
                _ = self.stop.changed() => return Ok(false),
                Some(done) = self.http.next() => done,
            };
            let done = self.stamp_http(done);
            if let Some((stamp, done)) = self.admit_http(done)? {
                let mut more = Effects::new();
                self.answer(codec, stamp, done, &mut more);
                let mut ends = effects.iter().any(|(e, ..)| ends_epoch(e, own));
                for effect in more.take() {
                    ends |= ends_epoch(&effect, own);
                    match effect {
                        ask @ Effect::Http { .. } if !ends => self.ask(epoch, ask),
                        other => effects.push_back((other, false, Some(stamp))),
                    }
                }
            }
        }
    }

    /// Starts the HTTP request `ask` for the codec of `epoch` ([`start_http`]), journaled as it
    /// starts, under its class and the epoch that asked.
    fn ask(&mut self, epoch: u32, ask: Effect) {
        // The timeout runs from the ask, so journaling the request counts against it (Codex
        // r4178197281).
        let now = Instant::now();
        let key = ConnKey {
            epoch,
            ..self.current()
        };
        if let Effect::Http {
            tag,
            req,
            rpc,
            class,
            ..
        } = &ask
        {
            let secret = |h: &fbc_core::Header| h.redact || is_secret_header(h.name);
            if !req.url.redactions().is_empty()
                || !req.body.redactions().is_empty()
                || req.headers.iter().any(secret)
            {
                self.credentialed.set(true);
            }
            let (at, wall) = self.clock.now();
            // Offered borrowed, so a full journal refuses it before it is cloned (Codex
            // r4178287660).
            if let Some(journal) = &self.journal {
                let record = RecordRef::HttpRequest {
                    at,
                    conn: key,
                    tag: *tag,
                    rpc: *rpc,
                    req,
                };
                journal.record_ref(*class, wall, record);
            }
        }
        let (rates, connector) = (&self.rates, &self.connector);
        self.http.extend(start_http(
            ask,
            key,
            now,
            rates,
            connector,
            self.http_max_body,
        ));
    }

    /// Stamps an HTTP result as it comes back, under the epoch that asked for it, so it takes
    /// its place in the shard's ingest order even when it is dropped, and journals it there.
    fn stamp_http(&self, done: Answered) -> (Stamp, Answered) {
        let key = ConnKey {
            epoch: done.epoch,
            ..self.current()
        };
        let stamp = self.clock.stamp(key, None);
        // Offered borrowed, its header values raw, so a full journal refuses it before its body
        // is copied or a value read (Codex r4178252055, r4179379938); the journal reads them
        // as the codec is handed them. A failure holds no byte of a response, so it is
        // journaled even by a credentialed session (Codex r4179310275).
        let journal = match &done.result {
            Ok(_) => self.input_journal(done.class, stamp.recv_wall),
            Err(_) => self.journal.as_ref(),
        };
        if let Some(journal) = journal {
            let headers: Vec<(&str, &[u8])> = match &done.result {
                Ok(r) => r
                    .headers()
                    .iter()
                    .map(|(n, v)| (n.as_str(), v.as_bytes()))
                    .collect(),
                Err(_) => Vec::new(),
            };
            let result = done.result.as_ref().map_err(|e| *e).map(|r| ResponseRef {
                status: r.status().as_u16(),
                headers: &headers,
                body: r.body(),
            });
            let answer = RecordRef::HttpResult {
                stamp,
                tag: done.tag,
                result,
            };
            journal.record_ref(done.class, stamp.recv_wall, answer);
        }
        (stamp, done)
    }

    /// A stamped HTTP result, when the epoch that asked is current; `None` when it ended
    /// (dropped and counted).
    fn admit_http(
        &mut self,
        (stamp, done): (Stamp, Answered),
    ) -> Result<Option<(Stamp, Answered)>, SessionError> {
        let current = self.epochs.admit(Input::Http, stamp.conn)? == Admit::Current;
        Ok(current.then_some((stamp, done)))
    }

    /// Takes the earliest timer and stamps its firing under the epoch that set it, so it takes
    /// its place in ingest order even when dropped; its stamp and tag when that epoch is
    /// current, `None` when it ended (dropped and counted).
    fn take_timer(&mut self) -> Result<Option<(Stamp, TimerTag)>, SessionError> {
        let mut current = None;
        if let Some(Reverse((_, _, epoch, tag))) = self.timers.pop() {
            let key = ConnKey {
                epoch,
                ..self.current()
            };
            let stamp = self.clock.stamp(key, None);
            self.journal(TrafficClass::Normal, stamp.recv_wall, || Record::Timer {
                stamp,
                tag,
            });
            if self.epochs.admit(Input::Timer, key)? == Admit::Current {
                current = Some((stamp, tag));
            }
        }
        Ok(current)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.timers.peek().map(|Reverse((at, ..))| *at)
    }
}

/// Hands a data frame to `codec` inside the venue's decode scope: the one call a live session
/// and a replay ([`crate::MdReplay`]) make for it (design §10.1).
pub(crate) fn feed_frame(
    codec: &mut dyn MdCodec,
    caps: &VenueCaps,
    specs: &SpecTable,
    raw: RawFrame<'_>,
    sink: &mut dyn MdSink,
    fx: &mut Effects,
) -> Result<(), DecodeError> {
    dispatch_market_data(caps, |scope| codec.on_frame(raw, scope, specs, sink, fx))
}

/// Hands an HTTP result to the codec that asked for it, inside the venue's decode scope: the
/// one call a live session and a replay make for it.
pub(crate) fn feed_http(
    codec: &mut dyn MdCodec,
    caps: &VenueCaps,
    specs: &SpecTable,
    tag: HttpTag,
    resp: Result<HttpResponse<'_>, HttpFailure>,
    sink: &mut dyn MdSink,
    fx: &mut Effects,
) -> Result<(), DecodeError> {
    dispatch_market_data(caps, |scope| {
        codec.on_http(tag, resp, scope, specs, sink, fx)
    })
}

/// Fires a codec's timer at the instant `stamp` gives it: the one call a live session and a
/// replay make for it.
pub(crate) fn feed_timer(
    codec: &mut dyn MdCodec,
    stamp: Stamp,
    tag: TimerTag,
    sink: &mut dyn MdSink,
    fx: &mut Effects,
) {
    codec.on_timer(tag, stamp.recv_mono, stamp.recv_wall, sink, fx);
}

/// Calls `f` with `result` as a codec is handed it: the response's status, its headers in
/// order (a value that is not UTF-8 read lossily, as the journal reads it) and its body, or why
/// none came.
fn with_response<R>(
    result: &Result<Response<Bytes>, HttpFailure>,
    f: impl FnOnce(Result<HttpResponse<'_>, HttpFailure>) -> R,
) -> R {
    match result {
        Ok(response) => {
            let values: Vec<_> = response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), String::from_utf8_lossy(value.as_bytes())))
                .collect();
            let headers: Vec<(&str, &str)> = values.iter().map(|(n, v)| (*n, v.as_ref())).collect();
            f(Ok(HttpResponse {
                status: response.status().as_u16(),
                headers: &headers,
                body: response.body(),
            }))
        }
        Err(failure) => f(Err(*failure)),
    }
}

/// Whether `effect` asks a socket endpoint of stream `own` to reconnect, which ends its epoch.
fn ends_epoch(effect: &Effect, own: StreamId) -> bool {
    matches!(effect, Effect::Reconnect { stream, .. } if *stream == own)
}

/// What a pong or a close frame is charged: a control frame, which safety traffic may send
/// from the reserve (decision 0030).
const CONTROL: Request = Request {
    charge: RateCharge::one(OpKind::Control, None),
    via: Via::Frame,
    class: TrafficClass::Safety,
};

/// The request of `effect` when it is a frame on stream `own` of a socket endpoint, which the
/// buckets charge; `None` for any other effect.
fn frame_of(effect: &Effect, own: StreamId, socket: bool) -> Option<Request> {
    let frame = socket && matches!(effect, Effect::Send { stream, .. } if *stream == own);
    Request::of(effect).filter(|_| frame)
}

/// What a subscribe call does next with its effects.
enum CallStep {
    /// Execute this effect; a frame of it is charged already.
    Run(Effect),
    /// The next frame waits for the buckets until then.
    Wait(Instant),
    /// Every effect has gone.
    Done,
}

/// The next of a subscribe call's `effects`: a frame on stream `own` of a socket endpoint is
/// charged on connection `key` on its own, and waits, back at the front, while the buckets
/// refuse it. One that can never fit is an error: the call could never go, and settling it as
/// sent would leave the reconciler believing what the venue was never told (decision 0030,
/// Codex r4179682238).
fn next_of_call(
    effects: &mut VecDeque<Effect>,
    rates: &RateLimiter,
    key: ConnKey,
    own: StreamId,
    socket: bool,
) -> Result<CallStep, SessionError> {
    let Some(effect) = effects.pop_front() else {
        return Ok(CallStep::Done);
    };
    let Some(request) = frame_of(&effect, own, socket) else {
        return Ok(CallStep::Run(effect));
    };
    match rates.charge(Instant::now(), key, &[request]) {
        Ok(_) => Ok(CallStep::Run(effect)),
        Err(Refused { ready_at: Some(at) }) => {
            effects.push_front(effect);
            Ok(CallStep::Wait(at))
        }
        Err(Refused { ready_at: None }) => Err(RateError::NeverFits(request.charge).into()),
    }
}

/// The next of `effects` to execute, with the input it is attributed to: a frame on stream
/// `own` of a socket endpoint is charged on connection `key` as its turn comes, unless it was
/// already, and one the buckets refuse is dropped unwritten (decision 0030).
fn next_admitted(
    effects: &mut VecDeque<(Effect, bool, Option<Stamp>)>,
    rates: &RateLimiter,
    key: ConnKey,
    own: StreamId,
    socket: bool,
) -> Option<(Effect, Option<Stamp>)> {
    std::iter::from_fn(|| effects.pop_front()).find_map(|(effect, charged, origin)| {
        let request = frame_of(&effect, own, socket).filter(|_| !charged);
        let admitted = request.is_none_or(|r| rates.charge(Instant::now(), key, &[r]).is_ok());
        admitted.then_some((effect, origin))
    })
}

/// The request of the HTTP effect `ask`, for the codec of epoch `key`, as it runs beside the
/// session until it is answered, fails or times out. Its timeout runs from `now`, when the
/// codec asked; one past the end of the clock bounds nothing, so the request is not sent, nor is one
/// the runtime cannot make, both before anything is charged (Codex r4179682244), nor one the
/// buckets refuse (decision 0030): each comes back as [`HttpFailure::NotSent`]. A 429 or 418
/// to it is counted under the scopes it was charged to. `None` for any other effect.
fn start_http(
    ask: Effect,
    key: ConnKey,
    now: Instant,
    rates: &RateLimiter,
    connector: &Connector,
    max_body: usize,
) -> Option<Pending> {
    let request = Request::of(&ask);
    let Effect::Http {
        tag,
        req,
        timeout,
        class,
        ..
    } = ask
    else {
        return None;
    };
    let epoch = key.epoch;
    let deadline = now.checked_add(timeout);
    // The request opens a connection of its own, which a limit on new connections counts too
    // (Codex r4179474175); both are charged or neither. A 429 or 418 answers the request, so it
    // is counted under the request's scopes alone (Codex r4179558360).
    let connect = request.map(|r| Request {
        charge: RateCharge::one(OpKind::Connect, None),
        ..r
    });
    let ready = deadline.zip(http::ready(&req));
    let go = ready.zip(request.zip(connect)).and_then(|(ready, (r, c))| {
        let charged = rates.charge(now, key, &[r, c]).ok();
        charged.map(|_| (ready, rates.scopes(key, &r)))
    });
    let (rates, connector) = (rates.clone(), connector.clone());
    Some(Box::pin(async move {
        let result = match go {
            Some(((deadline, ready), charged)) => {
                // Counted as the status arrives, before a body that may fail is read.
                let mut on_status = |status: StatusCode| {
                    if let 429 | 418 = status.as_u16() {
                        rates.rejected(charged);
                    }
                };
                connector
                    .http_by(ready, deadline, max_body, &mut on_status)
                    .await
            }
            None => Err(HttpFailure::NotSent),
        };
        Answered {
            epoch,
            tag,
            class,
            result,
        }
    }))
}

/// Sends a close frame on connection `key` if the buckets admit it, charged as a control
/// frame (Codex r4179720976), and the socket takes it now, without waiting on a peer that
/// stopped reading; the socket closes when it is dropped either way.
fn close(ws: &mut WebSocket, rates: &RateLimiter, key: ConnKey) {
    if rates.charge(Instant::now(), key, &[CONTROL]).is_ok() {
        let _ = ws.close(None).now_or_never();
    }
}

/// The next message on the socket; a poll endpoint has none, ever.
async fn next_frame(
    ws: &mut Option<WebSocket>,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    match ws {
        Some(ws) => ws.next().await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`, or forever when there is no deadline.
async fn sleep_or_never(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Stamps each pushed event with its input's stamp and hands it to the handler at once (0014
/// item 2), with the outbox its writes go in; an event of an older epoch is dropped and counted.
struct Sink<'a, H> {
    handler: &'a mut H,
    epochs: &'a mut Epochs,
    out: &'a mut Outbox,
    stamp: Stamp,
}

impl<H: MdHandler> MdSink for Sink<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        if let Ok(Admit::Current) = self.epochs.admit(Input::Event, self.stamp.conn) {
            let env = Envelope::new(self.stamp, meta, ev);
            self.handler.on_md_with(env, self.out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ratelimit::SafetyReserve;
    use fbc_core::{LimitScope, RateLimit, TagSet, WireSlice};
    use std::num::NonZeroU32;
    use std::time::Duration;

    #[test]
    fn a_calls_frames_go_each_as_it_fits_wait_at_the_front_or_end_the_session_when_one_never_fits()
    {
        // Two subscribe units per 10 s on a connection, one kept for safety traffic.
        let limits = [RateLimit {
            scope: LimitScope::Connection,
            ops: TagSet::of(&[OpKind::Subscribe]),
            per: Duration::from_secs(10),
            units: 2,
        }];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(50).unwrap()).unwrap();
        let (key, own) = (ConnKey { conn: 1, epoch: 0 }, StreamId(0));
        let frame = |weight| Effect::Send {
            stream: own,
            frame: WireSlice::plain(Vec::new()),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge {
                weight: NonZeroU32::new(weight).unwrap(),
                ..RateCharge::one(OpKind::Subscribe, None)
            },
        };
        let timer = Effect::Timer {
            tag: TimerTag(0),
            after: Duration::ZERO,
        };
        let mut effects = VecDeque::from([timer, frame(1), frame(1)]);
        let mut next = |socket| next_of_call(&mut effects, &rates, key, own, socket);
        assert!(matches!(
            next(true),
            Ok(CallStep::Run(Effect::Timer { .. }))
        ));
        assert!(matches!(next(true), Ok(CallStep::Run(Effect::Send { .. }))));
        // The last waits, back at the front, until the first charge expires.
        assert!(matches!(next(true), Ok(CallStep::Wait(_))));
        // A poll endpoint's frame is charged nothing (a session refuses it).
        assert!(matches!(
            next(false),
            Ok(CallStep::Run(Effect::Send { .. }))
        ));
        assert!(matches!(next(false), Ok(CallStep::Done)));
        // Two units never fit under the normal cap of one: the call can never go, and the
        // session ends saying so rather than settle it as sent (Codex r4179682238).
        let mut heavy = VecDeque::from([frame(2)]);
        let err = next_of_call(&mut heavy, &rates, key, own, true).err();
        let Some(SessionError::Rates(RateError::NeverFits(charge))) = err else {
            panic!("{err:?}");
        };
        assert_eq!((charge.op, charge.weight.get()), (OpKind::Subscribe, 2));
        assert_eq!(
            SessionError::Rates(RateError::NeverFits(charge)).to_string(),
            "a subscribe call's frame of Subscribe weighs 2, more than its buckets ever admit"
        );
        assert_eq!(rates.counts().refused.connection, 2);
    }

    #[test]
    fn only_a_safety_write_attributed_to_a_frame_with_a_kernel_receive_time_has_a_tick_to_wire() {
        let conn = ConnKey { conn: 1, epoch: 0 };
        let clock = IngestClock::new();
        let frame = clock.stamp(conn, Some(KernelRxNs(1_000)));
        let (own, safety) = (StreamId(4), TrafficClass::Safety);
        let tick = tick_to_wire(own, safety, Some(frame), WallNs(1_250));
        assert_eq!(
            tick,
            Some(TickToWire {
                stream: own,
                frame,
                nanos: 250
            })
        );
        // A wall clock stepped back between the two reads as negative.
        let back = tick_to_wire(own, safety, Some(frame), WallNs(900));
        assert_eq!(back.map(|t| t.nanos), Some(-100));
        let normal = tick_to_wire(own, TrafficClass::Normal, Some(frame), WallNs(1_250));
        let timer = tick_to_wire(own, safety, Some(clock.stamp(conn, None)), WallNs(1_250));
        let unattributed = tick_to_wire(own, safety, None, WallNs(1_250));
        assert_eq!((normal, timer, unattributed), (None, None, None));
    }

    #[test]
    fn a_handler_by_default_hands_each_event_to_on_md_and_ignores_tick_to_wire() {
        let mut seen = Vec::new();
        let mut handler = |env: Envelope<MdEvent>| seen.push(env.stamp.ingest_seq);
        let stamp = IngestClock::new().stamp(ConnKey { conn: 1, epoch: 0 }, None);
        let meta = VenueMeta {
            exch_ts: None,
            exch_ts_kind: fbc_core::ExchTsKind::Unknown,
            venue_seq: None,
        };
        let ev = MdEvent::Health {
            inst: fbc_core::InstrumentId::new(1),
            feed: fbc_core::Feed::Trades,
            h: fbc_core::FeedHealth::Stale,
        };
        let mut out = Outbox::new(StreamId(0));
        handler.on_md_with(Envelope::new(stamp, meta, ev), &mut out);
        let nanos = 5;
        let stream = StreamId(0);
        handler.on_tick_to_wire(TickToWire {
            stream,
            frame: stamp,
            nanos,
        });
        let mut fx = Effects::new();
        out.drain_into(&mut fx);
        assert!(fx.is_empty());
        assert_eq!(seen, [0]);
    }

    #[test]
    fn clones_of_a_clock_share_one_ingest_sequence() {
        let clock = IngestClock::default();
        let other = clock.clone();
        let conn = ConnKey { conn: 1, epoch: 2 };
        let seqs = [
            clock.stamp(conn, None),
            other.stamp(conn, None),
            clock.stamp(conn, None),
        ];
        assert_eq!(seqs.map(|s| s.ingest_seq), [0, 1, 2]);
        assert!(seqs.iter().all(|s| s.conn == conn && s.kernel_rx.is_none()));
        assert!(seqs[0].recv_mono <= seqs[2].recv_mono);
    }

    /// Codex r4178725031, r4178988952: a response is offered to the journal as the codec is
    /// handed it, each header value that is not UTF-8 read lossily and a secret one as it came
    /// (the journal writes its digest), so what is journaled is what the codec saw and its size
    /// is the journal's own encoding of it.
    #[test]
    fn a_response_is_offered_as_the_codec_is_handed_it() {
        let response = Response::builder()
            .status(201)
            .header(
                "x-raw",
                hyper::header::HeaderValue::from_bytes(&[b'a', 0xFF, 0xFE, b'b']).unwrap(),
            )
            .header("set-cookie", "sid=a-long-session-credential")
            .body(Bytes::from_static(b"body"))
            .unwrap();
        let handed = with_response(&Ok(response), |r| {
            let r = r.unwrap();
            let headers: Vec<(String, String)> = r
                .headers
                .iter()
                .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
                .collect();
            (r.status, headers, r.body.to_vec())
        });
        assert_eq!(
            handed,
            (
                201,
                vec![
                    ("x-raw".into(), "a\u{FFFD}\u{FFFD}b".into()),
                    ("set-cookie".into(), "sid=a-long-session-credential".into()),
                ],
                b"body".to_vec()
            )
        );
        let failed = with_response(&Err(HttpFailure::TimedOut), |r| r.map(|_| ()));
        assert_eq!(failed, Err(HttpFailure::TimedOut));
    }

    #[test]
    fn a_session_error_reads_as_its_cause() {
        let epoch = SessionError::from(EpochError::Exhausted { conn: 3 });
        assert_eq!(epoch.to_string(), "connection 3 has no epoch left to open");
        let rec = SessionError::from(ReconcileError::ForeignCall);
        assert_eq!(
            rec.to_string(),
            "a subscribe call made by another reconciler was settled here"
        );
    }
}
