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
//! One thread drives a session (design §5.1): [`MdSession::run`] spawns no task, so the read,
//! the decode and the handler's call run in one call stack on the caller's current-thread
//! runtime. Not here yet: keepalive, rotation and silence (FBC-djl), kernel timestamps
//! (FBC-2y3), rate limits (FBC-bel) and journaling (FBC-f3w).

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{
    ConfigError, ConnKey, Effect, Effects, EndpointPlan, Envelope, HttpFailure, HttpRequest,
    HttpResponse, HttpTag, MdCodec, MdEvent, MdSink, MdTransport, MonoNs, RawFrame, SpecTable,
    Stamp, Subscription, TimerTag, VenueCaps, VenueConfig, VenueFactory, VenueMeta, WallNs,
    dispatch_market_data,
};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::error::NetError;
use crate::http::{Bytes, Response};
use crate::pacing::{Pacer, ReconnectPacing};
use crate::reconcile::{ReconcileError, Reconciler, SubscribeCall};
use crate::ws::{self, Message, WebSocket};

/// Where the consumer receives a session's events: called once per event, in ingest order, on
/// the thread that drives the session, before the next frame is read (decision 0023).
pub trait MdHandler {
    fn on_md(&mut self, env: Envelope<MdEvent>);
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

    /// Stamps an input of `conn` arriving now; `kernel_rx` waits for FBC-2y3.
    fn stamp(&self, conn: ConnKey) -> Stamp {
        let ingest_seq = self.next.get();
        self.next.set(ingest_seq.wrapping_add(1));
        let mono = Instant::now().duration_since(self.origin).as_nanos();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        Stamp {
            ingest_seq,
            kernel_rx: None,
            recv_mono: MonoNs(u64::try_from(mono).unwrap_or(u64::MAX)),
            recv_wall: WallNs(i64::try_from(wall).unwrap_or(i64::MAX)),
            conn,
        }
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
        }
    }
}

impl std::error::Error for SessionError {}

impl From<EpochError> for SessionError {
    fn from(e: EpochError) -> Self {
        SessionError::Epoch(e)
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
    /// poll endpoint's own.
    pub refused_effects: u64,
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
    handler: H,
}

/// An HTTP request in flight.
type Pending = Pin<Box<dyn Future<Output = Answered>>>;

/// An HTTP request's result, with the epoch and tag of the codec call that asked for it.
struct Answered {
    epoch: u32,
    tag: HttpTag,
    result: Result<Response<Bytes>, HttpFailure>,
}

/// How a connected epoch ended.
enum End {
    Stop,
    Dropped,
}

/// What woke a disconnected session: the pacer, a timer, an ended epoch's HTTP result, or the
/// control (false: dropped).
enum Idle {
    Attempt,
    Timer,
    Http(Answered),
    Desired(bool),
}

/// What woke a connected session.
enum Wake {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Timer,
    Http(Answered),
    Desired(bool),
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
        let epochs = Epochs::new(config.conn);
        let mut rec = Reconciler::new(epochs.current());
        let first: BTreeSet<_> = config.plan.subs.iter().copied().collect();
        let _ = rec.set_desired(first.iter().copied());
        let (tx, desired) = watch::channel(first);
        let (stop_tx, stop) = watch::channel(());
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
            epochs,
            rec,
            desired,
            stop,
            timers: BinaryHeap::new(),
            timer_seq: 0,
            http: FuturesUnordered::new(),
            http_max_body: config.http_max_body,
            counters: MdCounters::default(),
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
        self.counters
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
            let at = self.pacer.next_attempt(Instant::now());
            loop {
                let idle = tokio::select! {
                    _ = sleep_or_never(at) => Idle::Attempt,
                    _ = sleep_or_never(self.next_deadline()) => Idle::Timer,
                    Some(done) = self.http.next() => Idle::Http(done),
                    r = self.desired.changed() => Idle::Desired(r.is_ok()),
                };
                match idle {
                    Idle::Attempt => break,
                    // Only an ended epoch's timers and requests wait while disconnected: each
                    // comes back into nothing.
                    Idle::Timer => {
                        let _ = self.take_timer()?;
                    }
                    Idle::Http(done) => {
                        let _ = self.take_http(done)?;
                    }
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
            // desired set current and fires an ended epoch's timers as they fall due.
            let deadline = Instant::now().checked_add(self.pacer.deadline());
            let opened = {
                let (connector, url) = (self.connector.clone(), self.url.clone());
                let connect = connector.websocket(&url);
                tokio::pin!(connect);
                loop {
                    tokio::select! {
                        opened = &mut connect => break opened.ok(),
                        _ = sleep_or_never(deadline) => break None,
                        _ = sleep_or_never(self.next_deadline()) => {
                            let _ = self.take_timer()?;
                        }
                        Some(done) = self.http.next() => {
                            let _ = self.take_http(done)?;
                        }
                        r = self.desired.changed() => match r {
                            Ok(()) => {
                                let subs = self.desired.borrow_and_update().clone();
                                let _ = self.rec.set_desired(subs);
                            }
                            Err(_) => return Ok(()),
                        },
                    }
                }
            };
            let Some(ws) = opened else {
                self.counters.failed_attempts += 1;
                self.pacer.failed(Instant::now());
                continue;
            };
            self.pacer.opened();
            let end = self.connected(Some(ws)).await?;
            self.pacer.dropped(Instant::now());
            if let End::Stop = end {
                return Ok(());
            }
            let key = self.epochs.advance()?;
            self.rec.begin_epoch(key)?;
        }
    }

    /// One epoch on the open socket `ws`, or of a poll endpoint (`None`), with a fresh codec,
    /// until it drops or the session stops.
    async fn connected(&mut self, mut ws: Option<WebSocket>) -> Result<End, SessionError> {
        let key = self.current();
        // The epoch's plan carries the subscriptions wanted now, not the first ones, including a
        // change that arrived as the connection opened.
        let subs = self.desired.borrow_and_update().clone();
        let _ = self.rec.set_desired(subs);
        let plan = EndpointPlan {
            subs: self.rec.desired().iter().copied().collect(),
            ..self.plan.clone()
        };
        let mut codec = self.venue.md_codec(&self.cfg, &plan);
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        let call = self.rec.opened(key)?;
        let mut open = self.execute(&mut ws, fx).await
            && self.subscribe(&mut ws, codec.as_mut(), call).await?;
        while open {
            let wake = tokio::select! {
                frame = next_frame(&mut ws) => Wake::Frame(frame),
                _ = sleep_or_never(self.next_deadline()) => Wake::Timer,
                Some(done) = self.http.next() => Wake::Http(done),
                r = self.desired.changed() => Wake::Desired(r.is_ok()),
            };
            open = match wake {
                Wake::Frame(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    self.decode(codec.as_mut(), key, &message, &mut fx);
                    self.execute(&mut ws, fx).await
                }
                Wake::Frame(_) => false,
                Wake::Timer => self.fire(&mut ws, codec.as_mut()).await?,
                Wake::Http(done) => match self.take_http(done)? {
                    Some((stamp, done)) => {
                        let mut fx = Effects::new();
                        self.answer(codec.as_mut(), stamp, done, &mut fx);
                        self.execute(&mut ws, fx).await
                    }
                    None => true,
                },
                Wake::Desired(true) => {
                    let subs = self.desired.borrow_and_update().clone();
                    let call = self.rec.set_desired(subs);
                    self.subscribe(&mut ws, codec.as_mut(), call).await?
                }
                Wake::Desired(false) => {
                    if let Some(ws) = ws.as_mut() {
                        close(ws);
                    }
                    return Ok(End::Stop);
                }
            };
        }
        // A write the control's drop interrupted ends the session, not just the epoch.
        if self.stop.has_changed().is_err() {
            return Ok(End::Stop);
        }
        Ok(End::Dropped)
    }

    /// Stamps one message of epoch `key` and decodes it; pings, pongs and close frames take
    /// their place in ingest order but carry no data.
    fn decode(
        &mut self,
        codec: &mut dyn MdCodec,
        key: ConnKey,
        message: &Message,
        fx: &mut Effects,
    ) {
        let stamp = self.clock.stamp(key);
        let raw = match message {
            Message::Text(text) => RawFrame::Text(text.as_str()),
            Message::Binary(bytes) => RawFrame::Binary(bytes.as_ref()),
            _ => return,
        };
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.epochs,
            stamp,
        };
        let specs = &self.specs;
        let decoded = dispatch_market_data(&self.caps, |scope| {
            codec.on_frame(raw, scope, specs, &mut sink, fx)
        });
        if decoded.is_err() {
            self.counters.decode_errors += 1;
        }
    }

    /// Hands a current epoch's HTTP result to the codec that asked for it, under `stamp`.
    fn answer(&mut self, codec: &mut dyn MdCodec, stamp: Stamp, done: Answered, fx: &mut Effects) {
        let headers: Vec<(String, String)> = match &done.result {
            Ok(response) => response
                .headers()
                .iter()
                .map(|(name, value)| {
                    let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
                    (name.as_str().to_owned(), value)
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let resp = match &done.result {
            Ok(response) => Ok(HttpResponse {
                status: response.status().as_u16(),
                headers: &headers,
                body: response.body(),
            }),
            Err(failure) => Err(*failure),
        };
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.epochs,
            stamp,
        };
        let specs = &self.specs;
        let decoded = dispatch_market_data(&self.caps, |scope| {
            codec.on_http(done.tag, resp, scope, specs, &mut sink, fx)
        });
        if decoded.is_err() {
            self.counters.decode_errors += 1;
        }
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
                stamp,
            };
            let mut fx = Effects::new();
            codec.on_timer(tag, stamp.recv_mono, stamp.recv_wall, &mut sink, &mut fx);
            open = self.execute(ws, fx).await;
        }
        Ok(open)
    }

    /// Sends `call` and every call the reconciler yields after it; false when the epoch ended.
    async fn subscribe(
        &mut self,
        ws: &mut Option<WebSocket>,
        codec: &mut dyn MdCodec,
        mut call: Option<SubscribeCall>,
    ) -> Result<bool, SessionError> {
        let mut open = true;
        while open && let Some(this) = call.take() {
            let mut fx = Effects::new();
            let asked = codec.subscribe(this.add(), this.remove(), &self.specs, &mut fx);
            open = self.execute(ws, fx).await;
            call = match asked {
                Ok(()) => self.rec.sent(this)?,
                Err(_) => {
                    self.counters.refused_subscribes += 1;
                    self.rec.refused(this)?
                }
            };
        }
        Ok(open)
    }

    /// Executes `fx` in order; false when the socket failed or a reconnect was asked for, which
    /// ends the epoch and leaves the rest unexecuted. A poll endpoint (`ws` is `None`) refuses
    /// every frame and reconnect.
    async fn execute(&mut self, ws: &mut Option<WebSocket>, mut fx: Effects) -> bool {
        let epoch = self.current().epoch;
        let own = self.plan.stream;
        let mut effects = fx.take().into_iter();
        let mut open = true;
        while open && let Some(effect) = effects.next() {
            match (effect, ws.as_mut()) {
                (Effect::Send { stream, frame, .. }, Some(ws)) if stream == own => {
                    let bytes = frame.bytes();
                    let text = std::str::from_utf8(bytes).map(Message::text);
                    let message = text.unwrap_or_else(|_| Message::binary(bytes.to_vec()));
                    open = tokio::select! {
                        biased;
                        sent = ws.send(message) => sent.is_ok(),
                        _ = self.stop.changed() => false,
                    };
                }
                (Effect::Timer { tag, after }, _) => {
                    // A timer past the end of the clock never fires.
                    if let Some(at) = Instant::now().checked_add(after) {
                        self.timer_seq += 1;
                        self.timers.push(Reverse((at, self.timer_seq, epoch, tag)));
                    }
                }
                (Effect::Reconnect { stream, .. }, Some(ws)) if stream == own => {
                    close(ws);
                    open = false;
                }
                (
                    Effect::Http {
                        tag, req, timeout, ..
                    },
                    _,
                ) => self.ask(epoch, tag, req, timeout),
                (Effect::Send { .. } | Effect::Reconnect { .. }, _) => {
                    self.counters.refused_effects += 1;
                }
            }
        }
        open
    }

    /// Starts `req` for the codec of `epoch`; it runs beside the session until it is answered,
    /// fails or times out.
    fn ask(&mut self, epoch: u32, tag: HttpTag, req: HttpRequest, timeout: Duration) {
        let (connector, max_body) = (self.connector.clone(), self.http_max_body);
        self.http.push(Box::pin(async move {
            let result = connector.http_within(&req, timeout, max_body).await;
            Answered { epoch, tag, result }
        }));
    }

    /// Stamps an HTTP result under the epoch that asked for it, so it takes its place in ingest
    /// order even when dropped; it and its stamp when that epoch is current, `None` when it
    /// ended (dropped and counted).
    fn take_http(&mut self, done: Answered) -> Result<Option<(Stamp, Answered)>, SessionError> {
        let key = ConnKey {
            epoch: done.epoch,
            ..self.current()
        };
        let stamp = self.clock.stamp(key);
        let current = self.epochs.admit(Input::Http, key)? == Admit::Current;
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
            let stamp = self.clock.stamp(key);
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

/// Sends a close frame if the socket takes it now, without waiting on a peer that stopped
/// reading; the socket closes when it is dropped either way.
fn close(ws: &mut WebSocket) {
    let _ = ws.close(None).now_or_never();
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
/// item 2); an event of an older epoch is dropped and counted.
struct Sink<'a, H> {
    handler: &'a mut H,
    epochs: &'a mut Epochs,
    stamp: Stamp,
}

impl<H: MdHandler> MdSink for Sink<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        if let Ok(Admit::Current) = self.epochs.admit(Input::Event, self.stamp.conn) {
            self.handler.on_md(Envelope::new(self.stamp, meta, ev));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_of_a_clock_share_one_ingest_sequence() {
        let clock = IngestClock::default();
        let other = clock.clone();
        let conn = ConnKey { conn: 1, epoch: 2 };
        let seqs = [clock.stamp(conn), other.stamp(conn), clock.stamp(conn)];
        assert_eq!(seqs.map(|s| s.ingest_seq), [0, 1, 2]);
        assert!(seqs.iter().all(|s| s.conn == conn && s.kernel_rx.is_none()));
        assert!(seqs[0].recv_mono <= seqs[2].recv_mono);
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
