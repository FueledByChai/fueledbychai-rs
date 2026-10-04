//! A market-data session: drives one [`MdTransport::Socket`] endpoint of a venue's plan through
//! connection epochs and the subscription reconciler (decisions 0002, 0014, 0023).
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
//! One thread drives a session (design §5.1): [`MdSession::run`] spawns no task, so the read,
//! the decode and the handler's call run in one call stack on the caller's current-thread
//! runtime. Not here yet: HTTP effects and planning (FBC-klr), keepalive, rotation and silence
//! (FBC-djl), kernel timestamps (FBC-2y3), rate limits (FBC-bel) and journaling (FBC-f3w).

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::fmt;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use fbc_core::{
    ConfigError, ConnKey, Effect, Effects, EndpointPlan, Envelope, MdCodec, MdEvent, MdSink,
    MdTransport, MonoNs, RawFrame, SpecTable, Stamp, Subscription, TimerTag, VenueCaps,
    VenueConfig, VenueFactory, VenueMeta, WallNs, dispatch_market_data,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::pacing::{Pacer, ReconnectPacing};
use crate::reconcile::{ReconcileError, Reconciler, SubscribeCall};
use crate::ws::{Message, WebSocket};

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
    /// The connection number stamped on this session's inputs, unique on its shard.
    pub conn: u16,
}

/// Why a session could not start, or stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    /// The venue refused the configuration.
    Config(ConfigError),
    /// The endpoint is not a socket (polling is FBC-klr's).
    NotASocket,
    /// The connection has no epoch left to open.
    Epoch(EpochError),
    /// The reconciler refused a call the session made: a session defect.
    Reconcile(ReconcileError),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::Config(e) => write!(f, "venue configuration refused: {e}"),
            SessionError::NotASocket => f.write_str("the endpoint is not a socket"),
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

/// Changes a running session's desired subscriptions; dropping it stops the session.
#[derive(Debug)]
pub struct MdControl {
    desired: watch::Sender<BTreeSet<Subscription>>,
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
    /// Attempts that did not open.
    pub failed_attempts: u64,
    /// Frames the codec could not decode.
    pub decode_errors: u64,
    /// Subscribe calls the codec refused; their subscriptions stay pending.
    pub refused_subscribes: u64,
    /// Effects refused as codec defects: a frame or reconnect for another stream, or an HTTP
    /// request (FBC-klr).
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
    /// Pending timers: deadline, order set, epoch, tag.
    timers: BinaryHeap<Reverse<(Instant, u64, u32, TimerTag)>>,
    timer_seq: u64,
    counters: MdCounters,
    handler: H,
}

/// How a connected epoch ended.
enum End {
    Stop,
    Dropped,
}

/// What woke a disconnected session: the pacer, a timer, or the control (false: dropped).
enum Idle {
    Attempt,
    Timer,
    Desired(bool),
}

/// What woke a connected session.
enum Wake {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Timer,
    Desired(bool),
}

impl<H: MdHandler> MdSession<H> {
    /// A session for `config`'s endpoint, handing events to `handler`, and its control.
    pub fn new(
        config: MdSessionConfig,
        handler: H,
    ) -> Result<(MdSession<H>, MdControl), SessionError> {
        let MdTransport::Socket { url } = &config.plan.transport else {
            return Err(SessionError::NotASocket);
        };
        let url = url.as_str().to_owned();
        let caps = config
            .venue
            .caps(&config.cfg)
            .map_err(SessionError::Config)?;
        let epochs = Epochs::new(config.conn);
        let mut rec = Reconciler::new(epochs.current());
        let first: BTreeSet<_> = config.plan.subs.iter().copied().collect();
        let _ = rec.set_desired(first.iter().copied());
        let (tx, desired) = watch::channel(first);
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
            timers: BinaryHeap::new(),
            timer_seq: 0,
            counters: MdCounters::default(),
            handler,
        };
        Ok((session, MdControl { desired: tx }))
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

    /// Connects, reconnects as paced, and delivers events until the [`MdControl`] is dropped.
    pub async fn run(&mut self) -> Result<(), SessionError> {
        loop {
            let at = self.pacer.next_attempt(Instant::now());
            loop {
                let idle = tokio::select! {
                    _ = sleep_until(at) => Idle::Attempt,
                    _ = sleep_or_never(self.next_deadline()) => Idle::Timer,
                    r = self.desired.changed() => Idle::Desired(r.is_ok()),
                };
                match idle {
                    Idle::Attempt => break,
                    // Only an ended epoch's timers wait while disconnected: each fires into
                    // nothing.
                    Idle::Timer => {
                        if let Some(Reverse((_, _, epoch, _))) = self.timers.pop() {
                            let key = ConnKey {
                                epoch,
                                ..self.current()
                            };
                            self.epochs.admit(Input::Timer, key)?;
                        }
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
            // A connect is stopped by the control's drop, and keeps the desired set current.
            let opened = {
                let connect = self.connector.websocket(&self.url);
                tokio::pin!(connect);
                loop {
                    tokio::select! {
                        opened = &mut connect => break opened,
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
            let Ok(ws) = opened else {
                self.counters.failed_attempts += 1;
                self.pacer.failed(Instant::now());
                continue;
            };
            self.pacer.opened();
            let end = self.connected(ws).await?;
            self.pacer.dropped(Instant::now());
            if let End::Stop = end {
                return Ok(());
            }
            let key = self.epochs.advance()?;
            self.rec.begin_epoch(key)?;
        }
    }

    /// One epoch on the open socket `ws`, with a fresh codec, until it drops or the session
    /// stops.
    async fn connected(&mut self, mut ws: WebSocket) -> Result<End, SessionError> {
        let key = self.current();
        let mut codec = self.venue.md_codec(&self.cfg, &self.plan);
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        let call = self.rec.opened(key)?;
        let mut open = self.execute(&mut ws, fx).await
            && self.subscribe(&mut ws, codec.as_mut(), call).await?;
        while open {
            let wake = tokio::select! {
                frame = ws.next() => Wake::Frame(frame),
                _ = sleep_or_never(self.next_deadline()) => Wake::Timer,
                r = self.desired.changed() => Wake::Desired(r.is_ok()),
            };
            open = match wake {
                Wake::Frame(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    self.decode(codec.as_mut(), key, &message, &mut fx);
                    self.execute(&mut ws, fx).await
                }
                Wake::Frame(_) => false,
                Wake::Timer => self.fire(&mut ws, codec.as_mut(), key).await?,
                Wake::Desired(true) => {
                    let subs = self.desired.borrow_and_update().clone();
                    let call = self.rec.set_desired(subs);
                    self.subscribe(&mut ws, codec.as_mut(), call).await?
                }
                Wake::Desired(false) => {
                    let _ = ws.close(None).await;
                    return Ok(End::Stop);
                }
            };
        }
        Ok(End::Dropped)
    }

    /// Decodes one message of epoch `key`; pings, pongs and close frames carry no data.
    fn decode(
        &mut self,
        codec: &mut dyn MdCodec,
        key: ConnKey,
        message: &Message,
        fx: &mut Effects,
    ) {
        let raw = match message {
            Message::Text(text) => RawFrame::Text(text.as_str()),
            Message::Binary(bytes) => RawFrame::Binary(bytes.as_ref()),
            _ => return,
        };
        let stamp = self.clock.stamp(key);
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

    /// Fires the earliest timer, which is due: an ended epoch's into nothing, the current
    /// epoch's into the codec. The next due timer wakes the session again at once.
    async fn fire(
        &mut self,
        ws: &mut WebSocket,
        codec: &mut dyn MdCodec,
        key: ConnKey,
    ) -> Result<bool, SessionError> {
        let mut open = true;
        if let Some(Reverse((_, _, epoch, tag))) = self.timers.pop()
            && self.epochs.admit(Input::Timer, ConnKey { epoch, ..key })? == Admit::Current
        {
            let stamp = self.clock.stamp(key);
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
        ws: &mut WebSocket,
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
    /// ends the epoch and leaves the rest unexecuted.
    async fn execute(&mut self, ws: &mut WebSocket, mut fx: Effects) -> bool {
        let epoch = self.current().epoch;
        let mut effects = fx.take().into_iter();
        let mut open = true;
        while open && let Some(effect) = effects.next() {
            match effect {
                Effect::Send { stream, frame, .. } if stream == self.plan.stream => {
                    let bytes = frame.bytes();
                    let text = std::str::from_utf8(bytes).map(Message::text);
                    let message = text.unwrap_or_else(|_| Message::binary(bytes.to_vec()));
                    open = ws.send(message).await.is_ok();
                }
                Effect::Timer { tag, after } => {
                    // A timer past the end of the clock never fires.
                    if let Some(at) = Instant::now().checked_add(after) {
                        self.timer_seq += 1;
                        self.timers.push(Reverse((at, self.timer_seq, epoch, tag)));
                    }
                }
                Effect::Reconnect { stream, .. } if stream == self.plan.stream => {
                    let _ = ws.close(None).await;
                    open = false;
                }
                Effect::Send { .. } | Effect::Reconnect { .. } | Effect::Http { .. } => {
                    self.counters.refused_effects += 1;
                }
            }
        }
        open
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.timers.peek().map(|Reverse((at, ..))| *at)
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
