//! An order-entry session (FBC-oaz, decision 0052): drives one account's order-entry
//! connection, as [`VenueFactory::plan_exec`] plans it, through connection epochs on the shared
//! session core (FBC-e73), with one [`ExecCodec`] for the session's life (decisions 0002, 0014,
//! 0015, 0019, 0023).
//!
//! The session opens the endpoint through the consumer's [`Connector`], so its SOCKS5 proxy
//! applies (0019). Each epoch is one life of that connection; the codec is not rebuilt for it,
//! since it holds the account's state (requests waiting for answers, a resync being read) across
//! reconnects. When an epoch opens, the session asks the codec how many nonces its `on_open`
//! needs ([`ExecCodec::nonces_for`] with [`CtxCall::Open`]), reserves exactly that many from the
//! consumer's [`NonceSource`] (none when it asks for none) and calls `on_open` with an
//! [`EncodeCtx`] holding them and the shard clock's wall and monotonic time (0014 item 1).
//!
//! Every frame is stamped from the shard's [`IngestClock`] before it is decoded, and decoded
//! inside the [`DecodeScope`](fbc_core::DecodeScope) [`dispatch`] lends for the venue's own
//! [`VenueCaps`] and the consumer's engine namespace, so client ids and fees decode as
//! `caps.exec` declares them (0015). Every event the codec pushes is stamped with its frame's
//! stamp and handed to the consumer's [`ExecHandler`] at once, on the thread that drives the
//! session and before the next frame is read, as 0023 does for market data. An event of an
//! ended epoch is dropped and counted ([`ExecSession::stale`] of [`Input::Event`]): the epoch
//! ends at once when the consumer stops the session, even inside its handler, so the rest of
//! the frame being handled reaches it no more.
//!
//! The codec's effects are executed in order, each only while the session has not stopped: a
//! frame for the session's own stream is charged to the buckets and written, a reconnect of it
//! ends the epoch and opens the next through the consumer's [`ReconnectPacing`]. What `on_open`
//! asks for is charged together first: buckets that refuse it for now end the epoch as a drop,
//! so `on_open` runs again on the next, opened no sooner than they would admit it, and frames
//! that never fit together end the session. A
//! session runs once ([`ExecSession::run`]): after an error, or once stopped, the consumer
//! builds a new one to connect again. A frame or reconnect for another stream is a codec defect,
//! refused and counted, and so, for now, are timers and HTTP requests, which FBC-bnl brings.
//! A write waits on its peer at most the consumer's [`WriteStall`] window (0036). Submitting
//! commands is FBC-0ga's, and journaling FBC-2pr's.
//!
//! One thread drives a session (design §5.1): [`ExecSession::run`] spawns no task.

use std::fmt;

use fbc_core::{
    ConnKey, CtxCall, Effect, Effects, EncodeCtx, Envelope, ExecCodec, ExecEvent, ExecSink,
    Inbound, InboundSpans, KernelRxNs, Namespace, NonceBlock, NonceSource, Secrets, SpecTable,
    Stamp, StreamId, TimerTag, VenueCaps, VenueConfig, VenueError, VenueFactory, VenueMeta,
    dispatch,
};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::pacing::ReconnectPacing;
use crate::ratelimit::{RateError, RateLimiter, Refused, Request};
use crate::session::SessionError;
use crate::session_core::{
    Answered, Control, Core, CoreConfig, EpochInputs, IngestClock, TickToWire, close, frame_of,
    next_frame,
};
use crate::stall::WriteStall;
use crate::ws::{self, Message, WebSocket};

/// Where the consumer receives an order-entry session's events: called once per event, in
/// ingest order, on the thread that drives the session, before the next frame is read
/// (decision 0052, as 0023 for market data).
pub trait ExecHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>);

    /// Connection epoch `key` ended (a drop, a reconnect the codec asked for, a stalled write,
    /// an error or a stop): called once per epoch, after every event of it the handler is given
    /// and before any of the next (as 0039 for market data). Nothing by default.
    fn on_epoch_end(&mut self, key: ConnKey) {
        let _ = key;
    }
}

impl<F: FnMut(Envelope<ExecEvent>)> ExecHandler for F {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self(env)
    }
}

/// What an order-entry session needs, all from the consumer.
pub struct ExecSessionConfig {
    pub venue: &'static dyn VenueFactory,
    pub cfg: VenueConfig,
    /// The account's credentials, handed to the venue's `exec_codec` once.
    pub creds: Secrets,
    /// The engine namespace the account's client ids are minted under, which the decode scope
    /// reads ours by.
    pub ns: Namespace,
    pub specs: SpecTable,
    pub connector: Connector,
    pub pacing: ReconnectPacing,
    pub clock: IngestClock,
    /// Where the nonces each epoch's `on_open` asks for are reserved.
    pub nonces: Box<dyn NonceSource>,
    /// The connection number stamped on this session's inputs, unique on its shard.
    pub conn: u16,
    /// The buckets of the venue's declared limits, shared with every session that counts
    /// against the same ones; built for exactly the venue's limits.
    pub limiter: RateLimiter,
    /// The longest one write may wait on a peer that stopped reading (0036).
    pub write_stall: WriteStall,
}

/// Why an order-entry session could not start, or stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecSessionError {
    /// What a market-data session could fail with too: the configuration, the URL, the epochs
    /// or the rate limits.
    Session(SessionError),
    /// The venue refused to plan its order entry or build its codec.
    Venue(VenueError),
    /// The venue takes no orders: it declares no `exec` block, or builds no order-entry codec.
    NoOrderEntry,
    /// The venue planned this many order-entry connections; a session drives exactly one
    /// (decision 0052).
    Endpoints(usize),
    /// The consumer's nonce source reserved another number of nonces than were asked for.
    Nonces { asked: u16, reserved: usize },
    /// The frames the codec's `on_open` asks for weigh more together than the venue's buckets
    /// ever admit, so no epoch could open.
    OpenNeverFits,
    /// The session has run: it runs once, and a new one is built to run again.
    Ended,
}

impl fmt::Display for ExecSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecSessionError::Session(e) => write!(f, "{e}"),
            ExecSessionError::Venue(e) => write!(f, "{e}"),
            ExecSessionError::NoOrderEntry => f.write_str("the venue takes no orders"),
            ExecSessionError::Endpoints(n) => write!(
                f,
                "the venue planned {n} order-entry connections; a session drives exactly one"
            ),
            ExecSessionError::Nonces { asked, reserved } => write!(
                f,
                "the nonce source reserved {reserved} nonces when {asked} were asked for"
            ),
            ExecSessionError::Ended => f.write_str("the session has run; it runs once"),
            ExecSessionError::OpenNeverFits => f.write_str(
                "the frames on_open asks for weigh more together than the buckets ever admit",
            ),
        }
    }
}

impl std::error::Error for ExecSessionError {}

impl From<SessionError> for ExecSessionError {
    fn from(e: SessionError) -> Self {
        ExecSessionError::Session(e)
    }
}

impl From<EpochError> for ExecSessionError {
    fn from(e: EpochError) -> Self {
        ExecSessionError::Session(e.into())
    }
}

impl From<RateError> for ExecSessionError {
    fn from(e: RateError) -> Self {
        ExecSessionError::Session(e.into())
    }
}

/// Stops a running session when dropped, even while a write waits on a peer that stopped
/// reading, or inside the session's own handler.
#[derive(Debug)]
pub struct ExecControl {
    /// Never sent on: its drop is the stop signal.
    _stop: watch::Sender<()>,
}

/// What an order-entry session counted, besides the stale inputs [`ExecSession::stale`]
/// reports.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ExecCounters {
    /// Connection attempts started.
    pub attempts: u64,
    /// Attempts that did not open, by their deadline or at all.
    pub failed_attempts: u64,
    /// Frames the codec could not decode.
    pub decode_errors: u64,
    /// Effects refused as codec defects: a frame or reconnect for another stream, and, until
    /// FBC-bnl, every timer and HTTP request.
    pub refused_effects: u64,
    /// Epochs ended because a write did not complete within the write-stall window.
    pub write_stalls: u64,
}

/// One account's order-entry connection, driven by [`ExecSession::run`].
pub struct ExecSession<H: ExecHandler> {
    /// The epochs, effects, writes and pacing (FBC-e73).
    core: Core,
    /// The one codec of the session's life.
    codec: Box<dyn ExecCodec>,
    stream: StreamId,
    url: String,
    caps: VenueCaps,
    ns: Namespace,
    specs: SpecTable,
    nonces: Box<dyn NonceSource>,
    decode_errors: u64,
    /// The epoch a run is connected in, until it ends: still set when a run starts, it was
    /// left by a run cancelled mid-epoch.
    in_epoch: Option<ConnKey>,
    /// Whether [`ExecSession::run`] was called.
    ran: bool,
    handler: H,
}

/// A session dropped while a dropped run left its epoch connected tells the handler it ended
/// (Codex r4189174470), unless it is dropped as a panic unwinds: its buckets are forgotten,
/// but the handler, which may have panicked itself, is not called, since a second panic would
/// abort the process (Reviewer B, B2).
impl<H: ExecHandler> Drop for ExecSession<H> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(key) = self.in_epoch.take() {
                self.core.rates.closed(key);
            }
        } else {
            self.end_left_epoch();
        }
    }
}

/// How a connected epoch ended.
enum End {
    Stop,
    Dropped,
}

impl<H: ExecHandler> ExecSession<H> {
    /// A session for the account `config.creds` reach, handing events to `handler`, and its
    /// control. Refused when the venue takes no orders, plans other than one order-entry
    /// connection, or that connection's URL cannot be opened.
    pub fn new(
        config: ExecSessionConfig,
        handler: H,
    ) -> Result<(ExecSession<H>, ExecControl), ExecSessionError> {
        let (venue, cfg) = (config.venue, &config.cfg);
        let caps = venue.caps(cfg).map_err(SessionError::Config)?;
        if caps.exec.is_none() {
            return Err(ExecSessionError::NoOrderEntry);
        }
        config.limiter.check(&caps.limits)?;
        let mut plan = venue.plan_exec(cfg).map_err(ExecSessionError::Venue)?;
        let endpoint = match (plan.pop(), plan.len()) {
            (Some(endpoint), 0) => endpoint,
            (last, n) => return Err(ExecSessionError::Endpoints(n + usize::from(last.is_some()))),
        };
        ws::check_url(endpoint.url.as_str()).map_err(SessionError::Url)?;
        let codec = venue.exec_codec(cfg, config.creds);
        let codec = codec.ok_or(ExecSessionError::NoOrderEntry)?;
        let codec = codec.map_err(ExecSessionError::Venue)?;
        let (stop_tx, stop) = watch::channel(());
        let core = Core::new(CoreConfig {
            own: endpoint.stream,
            connector: config.connector,
            clock: config.clock,
            pacing: config.pacing,
            write_stall: config.write_stall.window(),
            conn: config.conn,
            stop,
            // The session starts no HTTP request (FBC-bnl).
            http_max_body: 0,
            rates: config.limiter,
        });
        let session = ExecSession {
            core,
            codec,
            stream: endpoint.stream,
            url: endpoint.url.as_str().to_owned(),
            caps,
            ns: config.ns,
            specs: config.specs,
            nonces: config.nonces,
            decode_errors: 0,
            in_epoch: None,
            ran: false,
            handler,
        };
        Ok((session, ExecControl { _stop: stop_tx }))
    }

    /// The current connection epoch.
    pub fn current(&self) -> ConnKey {
        self.core.current()
    }

    /// Inputs of an ended epoch dropped, by kind.
    pub fn stale(&self, input: Input) -> u64 {
        self.core.epochs.stale(input)
    }

    pub fn counters(&self) -> ExecCounters {
        let core = &self.core.counters;
        ExecCounters {
            attempts: core.attempts,
            failed_attempts: core.failed_attempts,
            decode_errors: self.decode_errors,
            refused_effects: core.refused_effects,
            write_stalls: core.write_stalls,
        }
    }

    /// Connects, reconnects as paced, and delivers events until the [`ExecControl`] is
    /// dropped.
    ///
    /// A session runs once (decision 0052): a run that ended, in an error or not, or whose
    /// future was dropped, leaves nothing to run again, and a later call returns
    /// [`ExecSessionError::Ended`] without connecting (Codex r4189174493, r4189174502). A run
    /// dropped while connected leaves its epoch to be ended there or when the session drops:
    /// the handler is told it ended either way (Codex r4188995359, r4189174470). A run that
    /// ends in an error retires its epoch as a drop (Codex r4188802893).
    pub async fn run(&mut self) -> Result<(), ExecSessionError> {
        self.end_left_epoch();
        if std::mem::replace(&mut self.ran, true) {
            return Err(ExecSessionError::Ended);
        }
        loop {
            let mut ctl = Stop(self.core.stop.clone());
            let Some(ws) = self.core.connect(&self.url, &mut ctl).await? else {
                return Ok(());
            };
            let key = self.current();
            self.in_epoch = Some(key);
            let end = self.connected(ws).await;
            match end {
                // `connected` forgot the epoch's buckets.
                Ok(End::Stop) => return Ok(()),
                Ok(End::Dropped) => self.retire(key)?,
                // The run's own error, not one retiring the epoch might add.
                Err(e) => {
                    let _ = self.retire(key);
                    return Err(e);
                }
            }
        }
    }

    /// Ends the epoch a dropped run left connected, if one did: its per-connection buckets
    /// forgotten (Codex r4189428438) and the handler told.
    fn end_left_epoch(&mut self) {
        if let Some(key) = self.in_epoch.take() {
            self.core.rates.closed(key);
            self.handler.on_epoch_end(key);
        }
    }

    /// Ends connected epoch `key` as a drop: its buckets forgotten, the pacing told, the next
    /// epoch opened.
    fn retire(&mut self, key: ConnKey) -> Result<(), ExecSessionError> {
        self.core.rates.closed(key);
        self.core.pacer.dropped(Instant::now());
        self.core.epochs.advance()?;
        Ok(())
    }

    /// Whether the control has dropped.
    fn stopped(&self) -> bool {
        self.core.stop.has_changed().is_err()
    }

    /// One epoch on the open socket `ws`, until it drops or the session stops; the handler is
    /// told it ended either way.
    async fn connected(&mut self, ws: WebSocket) -> Result<End, ExecSessionError> {
        let key = self.current();
        let end = self.epoch(ws, key).await;
        // Cleared before the handler is called, so a handler that panics is never told twice
        // (Codex r4189618551).
        self.in_epoch = None;
        self.core.rates.closed(key);
        self.handler.on_epoch_end(key);
        end
    }

    /// The epoch `key`, opened on `ws`.
    async fn epoch(&mut self, ws: WebSocket, key: ConnKey) -> Result<End, ExecSessionError> {
        let mut ws = Some(ws);
        // A control that dropped as the connection opened stops the session before the codec
        // is told of it, so nothing is reserved or sent.
        let mut open = !self.stopped() && {
            let fx = self.open()?;
            self.open_effects(&mut ws, key, fx).await?
        };
        while open {
            // A frame first: one waiting as the control drops is taken, and stamped below.
            let frame = tokio::select! {
                biased;
                frame = next_frame(&mut ws) => Some(frame),
                _ = self.core.stop.changed() => None,
            };
            // The kernel receive time of the last packet read beneath the frame, if any.
            let rx = ws.as_ref().and_then(|ws| ws.get_ref().kernel_rx());
            open = match frame {
                // A frame read as the control drops is stamped, so it keeps its place in
                // ingest order, but reaches no codec.
                _ if self.stopped() => {
                    if let Some(Some(Ok(message))) = &frame {
                        let codec = &self.codec;
                        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
                        let _ = self.core.take_in(&redact, key, rx, message);
                    }
                    false
                }
                Some(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    let origin = self.decode(key, rx, &message, &mut fx);
                    self.execute(&mut ws, fx, false, origin).await?
                }
                _ => false,
            };
        }
        // A stop closes the connection; a drop, a reconnect the codec asked for (which the
        // core closed) or a failed write leaves it to be dropped.
        if !self.stopped() {
            return Ok(End::Dropped);
        }
        if let Some(socket) = ws.as_mut() {
            close(socket, &self.core.rates, key);
        }
        Ok(End::Stop)
    }

    /// Calls the codec's `on_open` for the session's stream with exactly the nonces it asks
    /// for; its effects.
    fn open(&mut self) -> Result<Effects, ExecSessionError> {
        let asked = self.codec.nonces_for(CtxCall::Open(self.stream));
        let nonces = match asked {
            0 => NonceBlock::EMPTY,
            n => self.nonces.reserve(n),
        };
        if nonces.len() != usize::from(asked) {
            let reserved = nonces.len();
            return Err(ExecSessionError::Nonces { asked, reserved });
        }
        let (mono, wall) = self.core.clock.now();
        let ctx = EncodeCtx { wall, mono, nonces };
        let mut fx = Effects::new();
        self.codec.on_open(self.stream, &ctx, &mut fx);
        Ok(fx)
    }

    /// Stamps one message of epoch `key` and decodes it inside the venue's decode scope; the
    /// codec's effects go in `fx`. The stamp of a data frame, which the effects are attributed
    /// to.
    fn decode(
        &mut self,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        message: &Message,
        fx: &mut Effects,
    ) -> Option<Stamp> {
        let codec = &self.codec;
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        let (stamp, raw) = self.core.take_in(&redact, key, rx, message)?;
        let mut sink = Sink {
            handler: &mut self.handler,
            epochs: &mut self.core.epochs,
            stop: &self.core.stop,
            stamp,
        };
        let (codec, stream, specs) = (&mut self.codec, self.stream, &self.specs);
        let decoded = dispatch(&self.caps, self.ns, |scope| {
            codec.on_frame(stream, raw, scope, specs, &mut sink, fx)
        });
        if decoded.is_err() {
            self.decode_errors += 1;
        }
        Some(stamp)
    }

    /// Executes what `on_open` asked for on epoch `key`. Its frames are charged together
    /// first, so all of them go or none does (Codex r4188802873): buckets that refuse them for
    /// now end the epoch as a drop, which reconnects through the pacing, no sooner than the
    /// buckets would admit them, and calls `on_open` again, rather than leave the codec
    /// believing it sent what it never did; frames that can never fit together end the
    /// session. False when the epoch ended.
    async fn open_effects(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
        mut fx: Effects,
    ) -> Result<bool, ExecSessionError> {
        let mut effects = fx.take();
        // Nothing behind a reconnect of the session's stream is ever reached, so nothing
        // behind it is charged (Codex r4188995331).
        let stream = self.stream;
        let ends = |e: &Effect| matches!(e, Effect::Reconnect { stream: s, .. } if *s == stream);
        if let Some(at) = effects.iter().position(ends) {
            effects.truncate(at + 1);
        }
        let own = |e: &Effect| frame_of(e, stream, true);
        let frames: Vec<Request> = effects.iter().filter_map(own).collect();
        // A stop that came while `on_open` ran charges nothing (Codex r4189174483).
        let now = Instant::now();
        let charged = (!self.stopped()).then(|| self.core.rates.charge(now, key, &frames));
        match charged {
            Some(Ok(_)) => {}
            None => return Ok(false),
            // The next attempt waits for the buckets as well as the pacing, so a refused open
            // does not reconnect at every floor, sending nothing, until they have room
            // (Reviewer B, B1).
            Some(Err(Refused { ready_at: Some(at) })) => {
                self.core.pacer.hold_until(at);
                return Ok(false);
            }
            Some(Err(Refused { ready_at: None })) => return Err(ExecSessionError::OpenNeverFits),
        }
        effects.into_iter().for_each(|e| fx.push(e));
        self.execute(ws, fx, true, None).await
    }

    /// Executes `fx` in order ([`Core::execute`]), each effect as its turn comes, attributed to
    /// the input stamped `origin`, its frames already charged when `charged` says so. A stop,
    /// from the handler or from another thread, ends the epoch before the next effect (Codex
    /// r4188802881). Each timer and HTTP request is refused and counted as its turn comes,
    /// until FBC-bnl, so one behind a reconnect or a failed write, which ends the epoch first,
    /// is never reached (Codex r4188639448). False when the epoch ended.
    async fn execute(
        &mut self,
        ws: &mut Option<WebSocket>,
        mut fx: Effects,
        charged: bool,
        origin: Option<Stamp>,
    ) -> Result<bool, ExecSessionError> {
        for effect in fx.take() {
            if self.stopped() {
                return Ok(false);
            }
            if matches!(effect, Effect::Timer { .. } | Effect::Http { .. }) {
                self.core.counters.refused_effects += 1;
                continue;
            }
            let mut one = Effects::new();
            one.push(effect);
            let inputs = &mut NoInputs;
            if !self.core.execute(ws, inputs, one, charged, origin).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The order-entry session's control as the core waits on it between epochs: only its drop.
struct Stop(watch::Receiver<()>);

impl Control for Stop {
    async fn changed(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }

    fn apply(&mut self) {}
}

/// What the core hands a current epoch's timer firings and HTTP results to: nothing yet, since
/// the session refuses every timer and HTTP request before the core sees it (FBC-bnl), and
/// tick-to-wire on order entry is FBC-qfm's.
struct NoInputs;

impl EpochInputs for NoInputs {
    fn redact_inbound(&self, _: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }

    fn ring(&mut self, _: &mut Epochs, _: Stamp, _: TimerTag, _: &mut Effects) {}

    fn answer(&mut self, _: &mut Epochs, _: Stamp, _: Answered, _: &mut Effects) {}

    fn on_tick_to_wire(&mut self, _: TickToWire) {}
}

/// Stamps each pushed event with its input's stamp and hands it to the handler at once (0014
/// item 2); an event of an ended epoch, one pushed after the control dropped included, is
/// dropped and counted.
struct Sink<'a, H> {
    handler: &'a mut H,
    epochs: &'a mut Epochs,
    stop: &'a watch::Receiver<()>,
    stamp: Stamp,
}

impl<H: ExecHandler> ExecSink for Sink<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        if self.stop.has_changed().is_err() {
            self.epochs.drop_ended(Input::Event);
        } else if let Ok(Admit::Current) = self.epochs.admit(Input::Event, self.stamp.conn) {
            self.handler.on_exec(Envelope::new(self.stamp, meta, ev));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{ConfigError, InstrumentId, ModeScope, VenueMode};

    #[test]
    fn an_exec_session_error_reads_as_its_cause() {
        let config = ConfigError::Missing("toy.url");
        let cases = [
            (
                ExecSessionError::from(SessionError::Config(config)),
                "venue configuration refused: missing configuration key toy.url",
            ),
            (
                ExecSessionError::Venue(VenueError::NoDiscovery),
                "the venue adapter discovers no instruments",
            ),
            (ExecSessionError::NoOrderEntry, "the venue takes no orders"),
            (ExecSessionError::Ended, "the session has run; it runs once"),
            (
                ExecSessionError::OpenNeverFits,
                "the frames on_open asks for weigh more together than the buckets ever admit",
            ),
            (
                ExecSessionError::Endpoints(2),
                "the venue planned 2 order-entry connections; a session drives exactly one",
            ),
            (
                ExecSessionError::Nonces {
                    asked: 2,
                    reserved: 1,
                },
                "the nonce source reserved 1 nonces when 2 were asked for",
            ),
        ];
        for (err, text) in cases {
            assert_eq!(err.to_string(), text);
        }
        let epoch = ExecSessionError::from(EpochError::Exhausted { conn: 3 });
        assert_eq!(epoch.to_string(), "connection 3 has no epoch left to open");
    }

    /// Between epochs the core waits on the control's drop alone: it has no state to apply.
    #[tokio::test]
    async fn the_control_changes_only_by_its_drop() {
        let (tx, rx) = watch::channel(());
        let mut stop = Stop(rx);
        stop.apply();
        drop(tx);
        assert!(!stop.changed().await);
    }

    /// The core never reaches these: the session refuses every timer and HTTP request first.
    #[test]
    fn the_core_hands_an_order_entry_epoch_nothing() {
        let clock = IngestClock::new();
        let key = ConnKey { conn: 1, epoch: 0 };
        let stamp = clock.stamp(key, None);
        let (mut epochs, mut fx) = (Epochs::new(1), Effects::new());
        let mut none = NoInputs;
        let frame = Inbound::Frame(fbc_core::RawFrame::Text("x"));
        assert_eq!(none.redact_inbound(frame), InboundSpans::NONE);
        none.ring(&mut epochs, stamp, TimerTag(0), &mut fx);
        let done = Answered {
            epoch: 0,
            tag: fbc_core::HttpTag(0),
            class: fbc_core::TrafficClass::Normal,
            result: Err(fbc_core::HttpFailure::NotSent),
        };
        none.answer(&mut epochs, stamp, done, &mut fx);
        let (stream, nanos) = (StreamId(1), 0);
        none.on_tick_to_wire(TickToWire {
            stream,
            frame: stamp,
            nanos,
        });
        assert!(fx.is_empty());
        let mut seen = Vec::new();
        let mut handler = |env: Envelope<ExecEvent>| seen.push(env.body);
        handler.on_epoch_end(key);
        let mode = ExecEvent::Mode {
            scope: ModeScope::Instrument(InstrumentId::new(1)),
            mode: VenueMode::Halted,
        };
        handler.on_exec(Envelope::new(stamp, VenueMeta::NONE, mode.clone()));
        assert_eq!(seen, [mode]);
    }
}
