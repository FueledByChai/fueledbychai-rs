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
//! runtime.
//!
//! **A stalled write (FBC-ha3, decision 0036).** A write waits on its peer for at most the
//! consumer's [`WriteStall`] window from when it began: one not completed by then is abandoned,
//! counted ([`MdCounters::write_stalls`]), and the epoch ends as a drop, reconnecting through
//! the pacing. While it waits, the session's timers that fall due fire as they do (an ended
//! epoch's into nothing), each stamped then, so it takes its place in ingest order, and the
//! effects a current epoch's codec asks for join the rest of the batch, as an HTTP result's
//! do. A timer due no later than the window fires first, even when the session runs again only
//! after both are past; one due after it does not fire into the codec. A write the session finds
//! completed when it runs again counts as completed, even past the window (a starved or
//! suspended task): the peer took the frame, and the window bounds the wait on the peer, not the
//! session's own scheduling.
//!
//! **Liveness (0033).** On a socket endpoint, each epoch sends its codec's
//! [`Keepalive`](fbc_core::Keepalive) every interval the codec declares (a WebSocket ping, or the
//! codec's own frame as Safety traffic), charged the keepalive's own rate charge; one the buckets
//! refuse is not sent, as any other frame they refuse. A keepalive with a zero interval is a codec
//! defect, refused and counted, and the epoch runs without one. Where the venue declares a
//! `max_conn_lifetime`, the connection is rotated the consumer's [`Liveness`] margin before it
//! ends: the epoch closes and the next opens at once, within the attempt budget but without the
//! floor a drop waits, and subscribes the desired set once. A stream that receives no frame, pings
//! and pongs included, within the consumer's silence window is reported stale (one
//! [`MdEvent::Health`] with [`FeedHealth::Stale`] per subscription wanted then, all under one stamp
//! of the silent epoch; a write the handler issues as it is told is not sent) and closed, and
//! reconnects as any drop does, through the pacing. A frame that was waiting to be read when the
//! window ran out (a write held the session) counts as heard, not silence. A poll endpoint has none
//! of these.
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
//! the frame is written all the same. A write that failed, that outlasted the write-stall
//! window, or that the control's drop interrupted, has no write result: whether any of it
//! reached the venue is unknown, and the connection's `Closed` follows. A frame, timer firing
//! or HTTP result the session takes as the control drops reaches no codec, and is journaled
//! after the epoch's `Closed`, so a replay ([`crate::MdReplay`]) feeds it to none either. Pings,
//! pongs and close frames carry no data, but each is stamped and journaled at its stamp, its
//! payload or close reason only as its keyed hash, since no codec names what is in them
//! (FBC-drf, decision 0041), so the journal's ingest sequences have no gap a `Degraded` marker
//! does not explain. What the session sends is journaled with its redaction spans as keyed
//! hashes. What it receives is journaled with the credentials the epoch's codec names in it
//! ([`MdCodec::redact_inbound`], decision 0028), asked before the input is journaled: a frame's
//! spans, and a response's body spans and the headers it marks, each written as its keyed hash
//! and the rest verbatim, so nothing received is withheld. Spans that do not fit what they
//! were named in ([`InboundSpans::check`]) are a codec defect, counted
//! ([`MdCounters::refused_redactions`]): that input is journaled with its whole body and every
//! header, name and value, hashed, never verbatim. So is a response that comes back to an epoch
//! that has ended, which no codec is left to ask, and which reaches none, live or in replay.
//! Inbound frames, HTTP requests, HTTP results and subscribe calls are offered to the sink
//! borrowed ([`RecordRef`]), so a sink with no room refuses them before they are copied, at the
//! length the journal's own encoding of them takes.
//!
//! **The session core (FBC-e73).** What a session does per epoch whatever its codec (effects,
//! HTTP requests, timers, keepalives, the write-stall bound, the pacing and the stamping and
//! journaling of inputs) lives in [`crate::session_core`], which this session calls; what is
//! here is the market-data codec, the handler and the subscription reconciler.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::time::Duration;

use fbc_core::{
    ConfigError, ConnKey, DecodeError, Effect, Effects, EndpointPlan, Envelope, FeedHealth,
    HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, KernelRxNs, MdCodec, MdEvent,
    MdSink, MdTransport, RateCharge, RawFrame, SpecTable, Stamp, StreamId, Subscription, TimerTag,
    TrafficClass, VenueCaps, VenueConfig, VenueFactory, VenueMeta, WireSlice, dispatch_market_data,
};
use fbc_journal::{ControlEvent, RecordRef};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::error::NetError;
use crate::journal::Journal;
use crate::liveness::{Alive, Liveness, LivenessError};
use crate::pacing::ReconnectPacing;
use crate::ratelimit::{RateError, RateLimiter, Refused, Request};
use crate::reconcile::{ReconcileError, Reconciler, SubscribeCall};
use crate::session_core::{
    Answered, Control, Core, CoreConfig, EpochInputs, IngestClock, TickToWire, close, frame_of,
    next_frame, sleep_or_never, with_response,
};
use crate::stall::WriteStall;
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

    /// Connection epoch `key` ended (a drop, a reconnect the codec asked for, a rotation, a
    /// silence, a stalled write, an error or a stop): called once per epoch, as its `Closed` is
    /// journaled, after every event of it the handler is given and before any of the next
    /// (decision 0039). What the epoch built (a book) may no longer match the venue. Nothing by
    /// default.
    fn on_epoch_end(&mut self, key: ConnKey) {
        let _ = key;
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

impl<F: FnMut(Envelope<MdEvent>)> MdHandler for F {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self(env)
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
    /// The longest one write may wait on a peer that stopped reading (0036).
    pub write_stall: WriteStall,
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
    /// Epochs ended because a write did not complete within the write-stall window.
    pub write_stalls: u64,
    /// Inbound frames and HTTP responses whose codec named credential spans that do not fit
    /// them (a codec defect): journaled with the whole body and every header hashed.
    pub refused_redactions: u64,
}

/// One market-data endpoint, driven by [`MdSession::run`].
pub struct MdSession<H> {
    /// The epochs, effects, HTTP requests, timers, pacing and journal (FBC-e73).
    core: Core,
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    plan: EndpointPlan,
    url: String,
    caps: VenueCaps,
    specs: SpecTable,
    /// The consumer's silence window.
    silence: Duration,
    /// How long after it opens a connection is rotated; `None` where the venue sets no limit.
    rotate_after: Option<Duration>,
    rec: Reconciler,
    desired: watch::Receiver<BTreeSet<Subscription>>,
    /// What the session counted outside its core.
    counters: MdCounters,
    /// A subscribe call one of whose frames the buckets refused, with the effects its codec
    /// asked for that have not gone yet, outstanding until they have room; and when to try
    /// again.
    waiting: Option<(SubscribeCall, VecDeque<Effect>)>,
    rate_retry: Option<Instant>,
    /// The writes the handler issues while it handles an input.
    outbox: Outbox,
    handler: H,
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

/// The epoch's inputs as the core hands them on ([`Feed`]): `$codec` with the session's handler
/// and outbox, split from the core it is handed to.
macro_rules! feed {
    ($session:ident, $codec:expr) => {
        &mut Feed {
            codec: $codec,
            handler: &mut $session.handler,
            outbox: &mut $session.outbox,
            caps: &$session.caps,
            specs: &$session.specs,
            decode_errors: &mut $session.counters.decode_errors,
        }
    };
}

impl<H: MdHandler> MdSession<H> {
    /// A session for `config`'s endpoint, handing events to `handler`, and its control.
    pub fn new(
        config: MdSessionConfig,
        handler: H,
    ) -> Result<(MdSession<H>, MdControl), SessionError> {
        let url = match &config.plan.transport {
            MdTransport::Socket { url } => {
                ws::check_url(url.as_str()).map_err(SessionError::Url)?;
                url.as_str().to_owned()
            }
            MdTransport::Poll { .. } => String::new(),
        };
        let caps = config
            .venue
            .caps(&config.cfg)
            .map_err(SessionError::Config)?;
        config.limiter.check(&caps.limits)?;
        // Only a socket endpoint opens a connection to rotate (Codex r4180333662).
        let rotate_after = match config.plan.transport {
            MdTransport::Socket { .. } => {
                config.liveness.rotate_after(caps.md.max_conn_lifetime)?
            }
            MdTransport::Poll { .. } => None,
        };
        let (stop_tx, stop) = watch::channel(());
        let core = Core::new(CoreConfig {
            own: config.plan.stream,
            connector: config.connector,
            clock: config.clock,
            pacing: config.pacing,
            write_stall: config.write_stall.window(),
            conn: config.conn,
            stop,
            http_max_body: config.http_max_body,
            rates: config.limiter,
        });
        let mut rec = Reconciler::new(core.current());
        let first: BTreeSet<_> = config.plan.subs.iter().copied().collect();
        let _ = rec.set_desired(first.iter().copied());
        let (tx, desired) = watch::channel(first);
        let outbox = Outbox::new(config.plan.stream);
        let session = MdSession {
            core,
            venue: config.venue,
            cfg: config.cfg,
            plan: config.plan,
            url,
            caps,
            specs: config.specs,
            silence: config.liveness.silence(),
            rotate_after,
            rec,
            desired,
            counters: MdCounters::default(),
            waiting: None,
            rate_retry: None,
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
        self.core.current()
    }

    /// Inputs of an older epoch dropped, by kind.
    pub fn stale(&self, input: Input) -> u64 {
        self.core.epochs.stale(input)
    }

    pub fn counters(&self) -> MdCounters {
        let core = &self.core.counters;
        MdCounters {
            attempts: core.attempts,
            failed_attempts: core.failed_attempts,
            refused_effects: core.refused_effects,
            keepalives: core.keepalives,
            write_stalls: core.write_stalls,
            refused_redactions: self.core.refused_redactions(),
            ..self.counters
        }
    }

    /// Records everything the session sends and receives into `journal` from now on (0006);
    /// the module docs say what is recorded.
    pub fn set_journal(&mut self, journal: Journal) {
        self.core.journal = Some(journal);
    }

    /// Connects, reconnects as paced, and delivers events until the [`MdControl`] is dropped;
    /// a poll endpoint opens nothing. HTTP requests still in flight when it ends are dropped.
    pub async fn run(&mut self) -> Result<(), SessionError> {
        let ran = match self.plan.transport {
            MdTransport::Socket { .. } => self.run_socket().await,
            // Nothing drops a poll endpoint's one epoch: it ends only when the session stops.
            MdTransport::Poll { .. } => self.connected(None).await.map(|_| ()),
        };
        self.core.http.clear();
        ran
    }

    async fn run_socket(&mut self) -> Result<(), SessionError> {
        loop {
            let mut ctl = Desired {
                desired: &mut self.desired,
                rec: &mut self.rec,
            };
            let Some(ws) = self.core.connect(&self.url, &mut ctl).await? else {
                return Ok(());
            };
            // The ended connection's buckets are forgotten even when the epoch ended in an
            // error (Codex r4179720972).
            let end = self.connected(Some(ws)).await;
            self.core.rates.closed(self.current());
            match end? {
                End::Stop | End::StopClosed => return Ok(()),
                End::Dropped => self.core.pacer.dropped(Instant::now()),
                // A planned close, not a drop: the next epoch opens at once, within the
                // budget, since the floor after the last drop or failure passed before this
                // connection opened.
                End::Rotated => {}
            }
            let key = self.core.epochs.advance()?;
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
        if ws.is_none() && self.core.stop.has_changed().is_err() {
            return Ok(End::Stop);
        }
        let key = self.current();
        self.core.control(|| ControlEvent::Opened(key));
        let end = if self.core.stop.has_changed().is_err() {
            Ok(End::Stop)
        } else {
            self.epoch(ws, key).await
        };
        if let Ok(End::StopClosed) = end {
            return Ok(End::Stop);
        }
        self.closed(key);
        end
    }

    /// Journals epoch `key` closed and tells the handler it ended, after the last event of it
    /// the handler is given (decision 0039).
    fn closed(&mut self, key: ConnKey) {
        self.core.control(|| ControlEvent::Closed(key));
        self.handler.on_epoch_end(key);
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
        self.core.counters.refused_effects += u64::from(refused);
        let mut rotated = false;
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        let call = self.rec.opened(key)?;
        (self.waiting, self.rate_retry) = (None, None);
        let mut open = self
            .core
            .execute(&mut ws, feed!(self, codec.as_mut()), fx, false, None)
            .await?
            && self.subscribe(&mut ws, codec.as_mut(), call).await?;
        while open {
            let wake = tokio::select! {
                frame = next_frame(&mut ws) => Wake::Frame(frame),
                _ = sleep_or_never(self.core.next_deadline()) => Wake::Timer,
                // A subscribe call waiting for the buckets tries again as if the set changed.
                _ = sleep_or_never(self.rate_retry) => Wake::Desired(true),
                Some(done) = self.core.http.next() => Wake::Http(done),
                r = self.desired.changed() => Wake::Desired(r.is_ok()),
                _ = sleep_or_never(alive.keepalive_at) => Wake::Keepalive,
                _ = sleep_or_never(alive.silent_at) => Wake::Silent,
                _ = sleep_or_never(alive.rotate_at) => Wake::Rotate,
            };
            // A rotation or a keepalive due once the window has run out too is a silence,
            // whichever deadline the select saw first: nothing is written on a silent stream
            // before it is reported (Codex r4180246768, r4180372215). A frame already waiting
            // when the window ran out, because a write held the session, was heard: it is read,
            // not reported silent.
            let wake = match wake {
                Wake::Rotate | Wake::Keepalive if alive.silent_by(Instant::now()) => Wake::Silent,
                other => other,
            };
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
            if self.core.stop.has_changed().is_err() || matches!(wake, Wake::Desired(false)) {
                self.closed(key);
                let redact = |input: Inbound<'_>| codec.redact_inbound(input);
                match wake {
                    Wake::Frame(Some(Ok(message))) => {
                        self.core.take_in(&redact, key, rx, &message);
                    }
                    Wake::Timer => {
                        let _ = self.core.take_timer()?;
                    }
                    Wake::Http(done) => {
                        self.core.stamp_http(Some(&redact), done);
                    }
                    _ => {}
                }
                if let Some(ws) = ws.as_mut() {
                    close(ws, &self.core.rates, key);
                }
                return Ok(End::StopClosed);
            }
            open = match wake {
                Wake::Frame(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    let origin = self.decode(codec.as_mut(), key, rx, &message, &mut fx);
                    self.core
                        .execute(&mut ws, feed!(self, codec.as_mut()), fx, false, origin)
                        .await?
                }
                Wake::Frame(_) => false,
                Wake::Timer => {
                    let feed = feed!(self, codec.as_mut());
                    self.core.fire(&mut ws, feed).await?
                }
                Wake::Http(done) => {
                    let feed = feed!(self, codec.as_mut());
                    self.core.take_http(&mut ws, feed, done).await?
                }
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
                        let feed = feed!(self, codec.as_mut());
                        open = self.core.keep_alive(&mut ws, feed, k).await?;
                    }
                    open
                }
                Wake::Silent => {
                    self.counters.silences += 1;
                    self.report_silent(key);
                    if let Some(ws) = ws.as_mut() {
                        close(ws, &self.core.rates, key);
                    }
                    false
                }
                Wake::Rotate => {
                    self.counters.rotations += 1;
                    rotated = true;
                    if let Some(ws) = ws.as_mut() {
                        close(ws, &self.core.rates, key);
                    }
                    false
                }
            };
        }
        // A write the control's drop interrupted ends the session, not just the epoch.
        if self.core.stop.has_changed().is_err() {
            return Ok(End::Stop);
        }
        Ok(if rotated { End::Rotated } else { End::Dropped })
    }

    /// Reports every subscription wanted now stale, under one stamp of the silent epoch `key`:
    /// the control's latest set, even one the session has not applied yet (Codex
    /// r4179959341); the next epoch applies it. The set is copied first, so a handler that
    /// changes it as it is told finds it unlocked (Codex r4180000633). A write the handler
    /// issues meanwhile is not sent, on the closing connection or the next: it is counted with
    /// the refused effects.
    fn report_silent(&mut self, key: ConnKey) {
        let wanted = self.desired.borrow().clone();
        let stamp = self.core.clock.stamp(key, None);
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.core.epochs,
            out: &mut self.outbox,
            stamp,
        };
        for sub in wanted {
            let (inst, feed, h) = (sub.inst, sub.feed, FeedHealth::Stale);
            sink.push(VenueMeta::NONE, MdEvent::Health { inst, feed, h });
        }
        self.core.counters.refused_effects += self.outbox.discard();
    }

    /// Stamps and journals one message of epoch `key` and decodes it ([`Core::take_in`]); the
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
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        let (stamp, raw) = self.core.take_in(&redact, key, rx, message)?;
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.core.epochs,
            out: &mut self.outbox,
            stamp,
        };
        if feed_frame(codec, &self.caps, &self.specs, raw, &mut sink, fx).is_err() {
            self.counters.decode_errors += 1;
        }
        self.outbox.drain_into(fx);
        Some(stamp)
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
            let (at, now) = self.core.clock.now();
            if let Some(journal) = &self.core.journal {
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
        match self.core.rates.charge(Instant::now(), key, &frames) {
            Ok(_) => {
                let mut all = Effects::new();
                effects.into_iter().for_each(|e| all.push(e));
                let feed = feed!(self, &mut *codec);
                return match self.core.execute(ws, feed, all, true, None).await? {
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
            let effect = match next_of_call(&mut effects, &self.core.rates, key, own, socket)? {
                CallStep::Run(effect) => effect,
                CallStep::Wait(at) => {
                    (self.waiting, self.rate_retry) = (Some((call, effects)), Some(at));
                    return Ok((true, None));
                }
                CallStep::Done => return Ok((true, self.rec.sent(call)?)),
            };
            let mut one = Effects::new();
            one.push(effect);
            let feed = feed!(self, &mut *codec);
            if !self.core.execute(ws, feed, one, true, None).await? {
                return Ok((false, None));
            }
        }
    }
}

/// The market-data session's control as the core waits on it between epochs: the desired
/// set, taken into the reconciler.
struct Desired<'a> {
    desired: &'a mut watch::Receiver<BTreeSet<Subscription>>,
    rec: &'a mut Reconciler,
}

impl Control for Desired<'_> {
    async fn changed(&mut self) -> bool {
        self.desired.changed().await.is_ok()
    }

    fn apply(&mut self) {
        let subs = self.desired.borrow_and_update().clone();
        let _ = self.rec.set_desired(subs);
    }
}

/// What a market-data epoch's timer firings and HTTP results reach as the core hands them on:
/// the epoch's codec, inside the venue's decode scope, and the session's handler, whose writes
/// go in the outbox.
struct Feed<'a, H> {
    codec: &'a mut dyn MdCodec,
    handler: &'a mut H,
    outbox: &'a mut Outbox,
    caps: &'a VenueCaps,
    specs: &'a SpecTable,
    decode_errors: &'a mut u64,
}

impl<H: MdHandler> EpochInputs for Feed<'_, H> {
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.codec.redact_inbound(input)
    }

    fn ring(&mut self, epochs: &mut Epochs, stamp: Stamp, tag: TimerTag, fx: &mut Effects) {
        let mut sink = Sink {
            handler: &mut *self.handler,
            epochs,
            out: &mut *self.outbox,
            stamp,
        };
        feed_timer(&mut *self.codec, stamp, tag, &mut sink, fx);
        self.outbox.drain_into(fx);
    }

    fn answer(&mut self, epochs: &mut Epochs, stamp: Stamp, done: Answered, fx: &mut Effects) {
        let mut sink = Sink {
            handler: &mut *self.handler,
            epochs,
            out: &mut *self.outbox,
            stamp,
        };
        let (codec, specs, caps) = (&mut *self.codec, self.specs, self.caps);
        let decoded = with_response(&done.result, |resp| {
            feed_http(codec, caps, specs, done.tag, resp, &mut sink, fx)
        });
        if decoded.is_err() {
            *self.decode_errors += 1;
        }
        self.outbox.drain_into(fx);
    }

    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.handler.on_tick_to_wire(sample);
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
    use fbc_core::{LimitScope, OpKind, RateLimit, TagSet, WireSlice};
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
