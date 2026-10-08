//! The session core (FBC-e73): what a session does for each connection epoch whatever its
//! codec, shared rather than copied by every session the runtime drives (decisions 0002, 0023,
//! 0027). The market-data session ([`crate::MdSession`]) calls it; its module docs say what
//! each part does as a session sees it.
//!
//! [`Core`] holds a session's epochs, ingest clock, journal, rate limiter, reconnect pacing,
//! pending timers and HTTP requests in flight, and does with them what no codec decides: it
//! waits for each connection attempt the pacing allows and the buckets admit ([`Core::connect`]);
//! executes a codec's effects in order ([`Core::execute`]), writing frames within the write-stall
//! window while timers and HTTP results keep arriving ([`Core::write`]), setting timers and
//! starting HTTP requests that come back only to the epoch that asked; sends keepalives
//! ([`Core::keep_alive`]); and stamps and journals every input, with the spans the epoch's codec
//! names in it.
//!
//! What a session's codec and handler do with an input is the session's: the core hands a
//! current epoch's timer firings and HTTP results to an [`EpochInputs`], and waits on the
//! session's own control through a [`Control`].

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{
    ConnKey, Effect, Effects, HeaderMark, HttpFailure, HttpResponse, HttpTag, Inbound,
    InboundSpans, Keepalive, KeepaliveKind, KernelRxNs, MonoNs, OpKind, RateCharge, RawFrame,
    RpcCall, Stamp, StreamId, TimerTag, TrafficClass, Via, WallNs,
};
use fbc_journal::{CloseRec, ControlEvent, Opaque, Opcode, Record, RecordRef, ResponseRef};
use fbc_journal::{WriteRes, WsControl};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use crate::connector::Connector;
use crate::epoch::{Admit, Epochs, Input};
use crate::http::{self, Bytes, Response, StatusCode};
use crate::journal::Journal;
use crate::pacing::{Pacer, ReconnectPacing};
use crate::ratelimit::{RateLimiter, Request};
use crate::session::SessionError;
use crate::ws::{Message, WebSocket};

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
    pub(crate) fn stamp(&self, conn: ConnKey, kernel_rx: Option<KernelRxNs>) -> Stamp {
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
    pub(crate) fn now(&self) -> (MonoNs, WallNs) {
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

/// What names the credentials in an input of an epoch: its codec's `redact_inbound`
/// (decision 0028).
pub(crate) type Redact<'a> = &'a dyn Fn(Inbound<'_>) -> InboundSpans;

/// Where the core hands what a current epoch's codec and handler take while the core runs:
/// the session's codec, inside the venue's decode scope, and its handler.
pub(crate) trait EpochInputs {
    /// The credentials the epoch's codec names in `input`.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans;
    /// Hands a current epoch's timer firing to its codec, under `stamp`, events admitted by
    /// `epochs`; the handler's writes follow the codec's effects in `fx`.
    fn ring(&mut self, epochs: &mut Epochs, stamp: Stamp, tag: TimerTag, fx: &mut Effects);
    /// Hands a current epoch's HTTP result to the codec that asked for it, under `stamp`,
    /// events admitted by `epochs`; the handler's writes follow the codec's effects in `fx`.
    fn answer(&mut self, epochs: &mut Epochs, stamp: Stamp, done: Answered, fx: &mut Effects);
    /// A Safety-class write attributed to a frame with a kernel receive time completed.
    fn on_tick_to_wire(&mut self, sample: TickToWire);
    /// A frame of request `call` is about to be written, so the request's deadline runs from
    /// now (FBC-0ga). Nothing, by default: no market-data request awaits an answer.
    fn sent_rpc(&mut self, call: RpcCall) {
        let _ = call;
    }
    /// Whether an input handed on while a write waited ended the session, so the core executes
    /// none of the rest of the batch. Never, by default.
    fn halted(&self) -> bool {
        false
    }
}

/// The consumer's control of a session, as the core waits on it between epochs.
pub(crate) trait Control {
    /// Waits until the control changes: true, or false once it dropped.
    async fn changed(&mut self) -> bool;
    /// Takes the control's latest state in.
    fn apply(&mut self);
}

/// What a session counted in its core.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub(crate) struct Counters {
    /// Connection attempts started.
    pub(crate) attempts: u64,
    /// Attempts that did not open, by their deadline or at all.
    pub(crate) failed_attempts: u64,
    /// Effects refused as codec defects.
    pub(crate) refused_effects: u64,
    /// Keepalives due; one the buckets refused was not sent.
    pub(crate) keepalives: u64,
    /// Epochs ended because a write did not complete within the write-stall window.
    pub(crate) write_stalls: u64,
}

/// What a session needs for each epoch whatever its codec; see the module docs.
pub(crate) struct Core {
    /// The session's own stream.
    pub(crate) own: StreamId,
    pub(crate) connector: Connector,
    pub(crate) clock: IngestClock,
    pub(crate) pacer: Pacer,
    /// The consumer's write-stall window.
    write_stall: Duration,
    pub(crate) epochs: Epochs,
    /// Dropped with the consumer's control: the stop signal a pending write observes.
    pub(crate) stop: watch::Receiver<()>,
    /// Pending timers: deadline, order set, epoch, tag.
    timers: BinaryHeap<Reverse<(Instant, u64, u32, TimerTag)>>,
    timer_seq: u64,
    /// HTTP requests in flight, each with the epoch that asked.
    pub(crate) http: FuturesUnordered<Pending>,
    http_max_body: usize,
    pub(crate) counters: Counters,
    /// Inputs whose codec's spans did not fit them.
    refused_redactions: Cell<u64>,
    pub(crate) rates: RateLimiter,
    pub(crate) journal: Option<Journal>,
    /// The class an inbound data frame and an HTTP result are journaled under: Normal on a
    /// market-data session; Safety on an order-entry session, whose inputs carry the acks and
    /// fills 0006 reserves room for and cannot be told apart before they are decoded (decision
    /// 0078).
    pub(crate) inbound: TrafficClass,
}

/// An HTTP request in flight.
pub(crate) type Pending = Pin<Box<dyn Future<Output = Answered>>>;

/// An HTTP request's result, with the epoch and tag of the codec call that asked for it and
/// the request's traffic class.
pub(crate) struct Answered {
    pub(crate) epoch: u32,
    pub(crate) tag: HttpTag,
    pub(crate) class: TrafficClass,
    pub(crate) result: Result<Response<Bytes>, HttpFailure>,
}

/// What woke a disconnected session: the pacer, a timer, an ended epoch's HTTP result (already
/// dropped and counted), or the control (false: dropped).
enum Idle {
    Attempt,
    Timer,
    Http,
    Control(bool),
}

/// The parts of a [`Core`], all from the consumer but the stop signal.
pub(crate) struct CoreConfig {
    pub(crate) own: StreamId,
    pub(crate) connector: Connector,
    pub(crate) clock: IngestClock,
    pub(crate) pacing: ReconnectPacing,
    pub(crate) write_stall: Duration,
    pub(crate) conn: u16,
    pub(crate) stop: watch::Receiver<()>,
    pub(crate) http_max_body: usize,
    pub(crate) rates: RateLimiter,
}

impl Core {
    /// A core at epoch 0 of connection `conn`, with no journal.
    pub(crate) fn new(config: CoreConfig) -> Core {
        Core {
            own: config.own,
            connector: config.connector,
            clock: config.clock,
            pacer: Pacer::new(config.pacing),
            write_stall: config.write_stall,
            epochs: Epochs::new(config.conn),
            stop: config.stop,
            timers: BinaryHeap::new(),
            timer_seq: 0,
            http: FuturesUnordered::new(),
            http_max_body: config.http_max_body,
            counters: Counters::default(),
            refused_redactions: Cell::new(0),
            rates: config.rates,
            journal: None,
            inbound: TrafficClass::Normal,
        }
    }

    /// The current connection epoch.
    pub(crate) fn current(&self) -> ConnKey {
        self.epochs.current()
    }

    /// Inputs whose codec's spans did not fit them.
    pub(crate) fn refused_redactions(&self) -> u64 {
        self.refused_redactions.get()
    }

    /// Offers the record `make` builds, if the session has a journal, under `class`.
    pub(crate) fn journal(&self, class: TrafficClass, now: WallNs, make: impl FnOnce() -> Record) {
        if let Some(journal) = &self.journal {
            journal.record(class, now, &make());
        }
    }

    /// The credentials `redact` names in `input`, checked: [`inbound_spans`], counting a
    /// defect.
    fn spans(&self, redact: Option<Redact<'_>>, input: Inbound<'_>) -> InboundSpans {
        let (spans, refused) = inbound_spans(redact, input);
        let n = self.refused_redactions.get();
        self.refused_redactions.set(n + u64::from(refused));
        spans
    }

    /// Records a connection change or a subscribe call.
    pub(crate) fn control(&self, ev: impl FnOnce() -> ControlEvent) {
        let (at, now) = self.clock.now();
        self.journal(TrafficClass::Normal, now, || Record::Control {
            at,
            ev: ev(),
        });
    }

    /// Waits for each connection attempt the pacing allows and the buckets admit, and opens
    /// `url` within the attempt deadline, until one opens: its socket, or `None` once `ctl`
    /// dropped. Meanwhile `ctl`'s changes are taken in, and an ended epoch's timers fire and its
    /// HTTP results come back, each into nothing.
    pub(crate) async fn connect(
        &mut self,
        url: &str,
        ctl: &mut impl Control,
    ) -> Result<Option<WebSocket>, SessionError> {
        loop {
            let mut at = self.pacer.next_attempt(Instant::now());
            let mut waiting = true;
            while waiting {
                // The control first: once it has dropped, no attempt starts, even one that fell
                // due at the same time.
                let timer = self.next_deadline();
                let idle = tokio::select! {
                    biased;
                    r = ctl.changed() => Idle::Control(r),
                    _ = sleep_or_never(at) => Idle::Attempt,
                    _ = sleep_or_never(timer) => Idle::Timer,
                    Some(done) = self.http.next() => {
                        let done = self.stamp_http(None, done);
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
                    Idle::Control(false) => return Ok(None),
                    Idle::Control(true) => ctl.apply(),
                }
            }
            self.pacer.attempted(Instant::now());
            self.counters.attempts += 1;
            // A connect fails at its deadline, is stopped by the control's drop, keeps the
            // control's state current and fires an ended epoch's timers as they fall due. The
            // control first: a drop wins over a handshake that completed at the same time.
            let deadline = Instant::now().checked_add(self.pacer.deadline());
            let opened = {
                let connector = self.connector.clone();
                let connect = connector.websocket(url);
                tokio::pin!(connect);
                loop {
                    let timer = self.next_deadline();
                    tokio::select! {
                        biased;
                        r = ctl.changed() => {
                            if !r {
                                return Ok(None);
                            }
                            ctl.apply();
                        }
                        opened = &mut connect => break opened.ok(),
                        _ = sleep_or_never(deadline) => break None,
                        _ = sleep_or_never(timer) => {
                            let _ = self.take_timer()?;
                        }
                        Some(done) = self.http.next() => {
                            let done = self.stamp_http(None, done);
                            let _ = self.admit_http(done)?;
                        }
                    }
                }
            };
            match opened {
                Some(ws) => {
                    self.pacer.opened();
                    return Ok(Some(ws));
                }
                None => {
                    self.counters.failed_attempts += 1;
                    self.pacer.failed(Instant::now());
                }
            }
        }
    }

    /// Sends `keepalive` on the epoch's socket, charged its own rate charge as Safety traffic
    /// and attributed to no input; false when the epoch ended.
    pub(crate) async fn keep_alive(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
        keepalive: &Keepalive,
    ) -> Result<bool, SessionError> {
        self.counters.keepalives += 1;
        match &keepalive.kind {
            KeepaliveKind::Frame(frame) => {
                let mut fx = Effects::new();
                fx.push(Effect::Send {
                    stream: self.own,
                    frame: frame.clone(),
                    rpc: None,
                    class: TrafficClass::Safety,
                    charge: keepalive.charge,
                });
                self.execute(ws, inputs, fx, false, None).await
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
                    open = self.write(socket, inputs, ping, &mut effects).await?;
                }
                Ok(open && self.run_effects(ws, inputs, effects).await?)
            }
        }
    }

    /// Stamps one message of epoch `key`, whose last packet the kernel received at `rx` when it
    /// knows, and journals it with the credentials `redact`, the epoch's codec's, names in it;
    /// its stamp and data frame, when it carries data. Pings, pongs and close frames take their
    /// place in ingest order and are journaled there, but carry no data
    /// ([`Self::take_control`]).
    pub(crate) fn take_in<'m>(
        &mut self,
        redact: Redact<'_>,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        message: &'m Message,
    ) -> Option<(Stamp, RawFrame<'m>)> {
        let raw = match message {
            Message::Text(text) => RawFrame::Text(text.as_str()),
            Message::Binary(bytes) => RawFrame::Binary(bytes.as_ref()),
            control => {
                if let Some(frame) = ws_control(control) {
                    self.take_control(key, rx, frame);
                }
                return None;
            }
        };
        let stamp = self.clock.stamp(key, rx);
        // Offered borrowed, so a full journal refuses it before it is copied (Codex
        // r4178252055).
        if let Some(journal) = &self.journal {
            let spans = self.spans(Some(redact), Inbound::Frame(raw));
            let frame = RecordRef::Inbound {
                stamp,
                frame: raw,
                redact: spans.body(),
            };
            journal.record_ref(self.inbound, stamp.recv_wall, frame);
        }
        Some((stamp, raw))
    }

    /// Stamps a ping, pong or close frame of epoch `key` and journals it at that stamp, under
    /// Normal (FBC-drf): every ingest sequence the session takes is journaled, or lies in a gap
    /// the journal marks. The journal writes its payload or reason only as a keyed hash
    /// (decision 0041). A ping or a close is charged for the answer the WebSocket layer sends
    /// to it: a pong of its own, and a close (decision 0030, Codex r4179720976).
    fn take_control(&mut self, key: ConnKey, rx: Option<KernelRxNs>, frame: WsControl) {
        let stamp = self.clock.stamp(key, rx);
        if !matches!(frame, WsControl::Pong(_)) {
            self.rates.record(Instant::now(), key, CONTROL);
        }
        self.journal(TrafficClass::Normal, stamp.recv_wall, || {
            Record::InboundControl { stamp, frame }
        });
    }

    /// Fires the earliest timer, which is due: an ended epoch's into nothing, the current
    /// epoch's into `inputs`. The next due timer wakes the session again at once.
    pub(crate) async fn fire(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
    ) -> Result<bool, SessionError> {
        let mut open = true;
        if let Some((stamp, tag)) = self.take_timer()? {
            let mut fx = Effects::new();
            inputs.ring(&mut self.epochs, stamp, tag, &mut fx);
            open = self.execute(ws, inputs, fx, false, Some(stamp)).await?;
        }
        Ok(open)
    }

    /// Takes an HTTP result as it comes back while an epoch runs: stamped and journaled
    /// ([`Self::stamp_http`]), handed to the codec that asked when its epoch is current, and
    /// its effects executed; false when the epoch ended.
    pub(crate) async fn take_http(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
        done: Answered,
    ) -> Result<bool, SessionError> {
        let redact = |input: Inbound<'_>| inputs.redact_inbound(input);
        Ok(
            match self.admit_http(self.stamp_http(Some(&redact), done))? {
                Some((stamp, done)) => {
                    let mut fx = Effects::new();
                    inputs.answer(&mut self.epochs, stamp, done, &mut fx);
                    self.execute(ws, inputs, fx, false, Some(stamp)).await?
                }
                None => true,
            },
        )
    }

    /// Executes `fx` of the epoch `inputs` serves in order; false when the socket failed or a
    /// reconnect was asked for, which ends the epoch and leaves the rest unexecuted. A poll
    /// endpoint (`ws` is `None`) refuses every frame and reconnect. Each frame is charged as its
    /// turn comes, unless `charged` says `fx`'s were already, and one the buckets refuse is not
    /// written; effects asked for meanwhile are charged as theirs come. `fx` is attributed to
    /// the input stamped `origin`, and effects asked for meanwhile to the input that asked: a
    /// Safety frame's completion is reported as its tick-to-wire when that input has a kernel
    /// receive time.
    pub(crate) async fn execute(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
        mut fx: Effects,
        charged: bool,
        origin: Option<Stamp>,
    ) -> Result<bool, SessionError> {
        // `charged` speaks for the frames alone: each HTTP request is charged as it starts.
        let effects = fx.take().into_iter();
        let effects = effects
            .map(|e| {
                let frame_charged = charged && !is_http(&e);
                (e, frame_charged, origin)
            })
            .collect();
        self.run_effects(ws, inputs, effects).await
    }

    /// Executes `fx` as [`Self::execute`] does, attributed to no input, every frame and HTTP
    /// request in it already charged with the connection each request opens ([`http_of`]), so
    /// none is charged again (PR #90 Reviewer B B7).
    pub(crate) async fn execute_all_charged(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
        mut fx: Effects,
    ) -> Result<bool, SessionError> {
        let effects = fx.take().into_iter().map(|e| (e, true, None)).collect();
        self.run_effects(ws, inputs, effects).await
    }

    /// Executes `effects` in order, each with whether it was charged already and the input it
    /// is attributed to ([`Self::execute`]). Once the control has dropped, or `inputs` halted,
    /// none is executed and the epoch has ended, so what a timer firing or an HTTP result asked
    /// for while a write waited is not executed after a stop that came as the write completed
    /// (FBC-bnl).
    async fn run_effects(
        &mut self,
        ws: &mut Option<WebSocket>,
        inputs: &mut dyn EpochInputs,
        mut effects: VecDeque<(Effect, bool, Option<Stamp>)>,
    ) -> Result<bool, SessionError> {
        let key = self.current();
        let epoch = key.epoch;
        let own = self.own;
        let mut open = self.stop.has_changed().is_ok() && !inputs.halted();
        while open
            && let Some((effect, charged, origin)) =
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
                    // The kind it is sent as is journaled with it (FBC-q7b): its bytes once
                    // blanked may not say.
                    let opcode = match text {
                        Ok(_) => Opcode::Text,
                        Err(_) => Opcode::Binary,
                    };
                    let message = text.unwrap_or_else(|_| Message::binary(bytes.to_vec()));
                    // Its deadline runs from before the write, so a write that fails after bytes
                    // may have left still reaches it (FBC-0ga).
                    rpc.into_iter().for_each(|call| inputs.sent_rpc(call));
                    let (conn, rpc) = (self.current(), rpc.map(|call| call.id));
                    let (at, now) = self.clock.now();
                    self.journal(class, now, || Record::Outbound {
                        at,
                        conn,
                        rpc,
                        opcode,
                        frame,
                    });
                    open = self.write(ws, inputs, message, &mut effects).await?;
                    if open {
                        let (at, now) = self.clock.now();
                        self.journal(class, now, || Record::WriteResult {
                            at,
                            conn,
                            rpc,
                            result: WriteRes::Written,
                        });
                        let tick = tick_to_wire(own, class, origin, now);
                        tick.into_iter().for_each(|t| inputs.on_tick_to_wire(t));
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
                (ask @ Effect::Http { .. }, _) => self.ask(epoch, ask, charged),
                (Effect::Send { .. } | Effect::Reconnect { .. }, _) => {
                    self.counters.refused_effects += 1;
                }
            }
            open &= self.stop.has_changed().is_ok() && !inputs.halted();
        }
        Ok(open)
    }

    /// Writes `message` on `ws`; false when the write failed, did not complete within the
    /// write-stall window (counted), or the control's drop interrupted it. Requests in flight
    /// keep going while the write waits, and timers due no later than the window fire as they
    /// fall due (FBC-ha3), before the window ends it. A result that comes back, or a current
    /// epoch's timer that fires, meanwhile reaches `inputs` at once, so the handler gets its
    /// events in the shard's ingest order (Codex r4177698441). A request the codec asks for then
    /// starts at once, its timeout running from now (Codex r4177887264); its other effects join
    /// `effects`, behind the rest of the batch, uncharged and attributed to the result or
    /// firing. A request behind a reconnect of this stream, still queued in `effects` or asked
    /// for first, waits too: that reconnect ends the epoch before its turn, so it is never sent
    /// (Codex r4177934308); so does one asked for once the control has dropped or the inputs
    /// halted, which no effect follows (Reviewer B, B6).
    async fn write(
        &mut self,
        ws: &mut WebSocket,
        inputs: &mut dyn EpochInputs,
        message: Message,
        effects: &mut VecDeque<(Effect, bool, Option<Stamp>)>,
    ) -> Result<bool, SessionError> {
        let (epoch, own) = (self.current().epoch, self.own);
        // A window past the end of the clock never runs out.
        let stalled = Instant::now().checked_add(self.write_stall);
        let send = ws.send(message);
        tokio::pin!(send);
        loop {
            // Only a timer due no later than the window: one due after it waits for the epoch's
            // end, even when the session first runs again past both (Codex r4180583622).
            let timer = self.next_deadline();
            let timer = timer.filter(|at| stalled.is_none_or(|stalled| *at <= stalled));
            let woke = tokio::select! {
                biased;
                sent = &mut send => return Ok(sent.is_ok()),
                _ = self.stop.changed() => return Ok(false),
                _ = sleep_or_never(timer) => None,
                _ = sleep_or_never(stalled) => {
                    self.counters.write_stalls += 1;
                    return Ok(false);
                }
                Some(done) = self.http.next() => Some(done),
            };
            let mut more = Effects::new();
            let stamp = match woke {
                None => self.take_timer()?.map(|(stamp, tag)| {
                    inputs.ring(&mut self.epochs, stamp, tag, &mut more);
                    stamp
                }),
                Some(done) => {
                    let redact = |input: Inbound<'_>| inputs.redact_inbound(input);
                    let done = self.stamp_http(Some(&redact), done);
                    self.admit_http(done)?.map(|(stamp, done)| {
                        inputs.answer(&mut self.epochs, stamp, done, &mut more);
                        stamp
                    })
                }
            };
            if let Some(stamp) = stamp {
                // A stop the input brought, its handler dropping the control or the inputs
                // halting, starts no request either: it would be charged and journaled after
                // the stop, never sent (Reviewer B, B6).
                let stopped = self.stop.has_changed().is_err() || inputs.halted();
                let mut ends = stopped || effects.iter().any(|(e, ..)| ends_epoch(e, own));
                for effect in more.take() {
                    ends |= ends_epoch(&effect, own);
                    match effect {
                        ask @ Effect::Http { .. } if !ends => self.ask(epoch, ask, false),
                        other => effects.push_back((other, false, Some(stamp))),
                    }
                }
            }
        }
    }

    /// Starts the HTTP request `ask` for the codec of `epoch` ([`start_http`]), journaled as it
    /// starts, under its class and the epoch that asked; `charged` when it and the connection it
    /// opens were charged already.
    fn ask(&mut self, epoch: u32, ask: Effect, charged: bool) {
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
            charged,
        ));
    }

    /// Stamps an HTTP result as it comes back, under the epoch that asked for it, so it takes
    /// its place in the shard's ingest order even when it is dropped, and journals it there
    /// with the credentials the codec of that epoch names in it: `redact`, the current epoch's
    /// codec's when one is running. A result of an ended epoch has no codec left to ask: all
    /// of it is hashed ([`inbound_spans`]).
    pub(crate) fn stamp_http(
        &self,
        redact: Option<Redact<'_>>,
        done: Answered,
    ) -> (Stamp, Answered) {
        let key = ConnKey {
            epoch: done.epoch,
            ..self.current()
        };
        let stamp = self.clock.stamp(key, None);
        // Offered borrowed, its header values raw, so a full journal refuses it before its body
        // is copied (Codex r4178252055, r4179379938); the journal reads them as the codec is
        // handed them, and so does the codec, asked for its spans first. A failure holds no byte
        // of a response and has nothing to name.
        if let Some(journal) = &self.journal {
            let redact = redact.filter(|_| done.epoch == self.current().epoch);
            let spans = with_response(&done.result, |resp| {
                resp.map(|r| self.spans(redact, Inbound::Http(done.tag, r)))
            })
            .unwrap_or_default();
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
                marks: spans.headers(),
                body_redact: spans.body(),
            });
            let answer = RecordRef::HttpResult {
                stamp,
                tag: done.tag,
                result,
            };
            // Under its request's class, or Safety on an order-entry session (decision 0078).
            let class = match self.inbound {
                TrafficClass::Safety => TrafficClass::Safety,
                TrafficClass::Normal => done.class,
            };
            journal.record_ref(class, stamp.recv_wall, answer);
        }
        (stamp, done)
    }

    /// A stamped HTTP result, when the epoch that asked is current; `None` when it ended
    /// (dropped and counted).
    pub(crate) fn admit_http(
        &mut self,
        (stamp, done): (Stamp, Answered),
    ) -> Result<Option<(Stamp, Answered)>, SessionError> {
        let current = self.epochs.admit(Input::Http, stamp.conn)? == Admit::Current;
        Ok(current.then_some((stamp, done)))
    }

    /// Takes the earliest timer and stamps its firing under the epoch that set it, so it takes
    /// its place in ingest order even when dropped; its stamp and tag when that epoch is
    /// current, `None` when it ended (dropped and counted).
    pub(crate) fn take_timer(&mut self) -> Result<Option<(Stamp, TimerTag)>, SessionError> {
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

    /// When the earliest pending timer falls due.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.timers.peek().map(|Reverse((at, ..))| *at)
    }
}

/// Calls `f` with `result` as a codec is handed it: the response's status, its headers in
/// order (a value that is not UTF-8 read lossily, as the journal reads it) and its body, or why
/// none came.
pub(crate) fn with_response<R>(
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

/// The credentials `redact` names in `input` (a codec's `redact_inbound`), and whether they
/// were refused. Spans that do not fit `input` ([`InboundSpans::check`]) are a codec defect and
/// are refused; then, as when there is no codec to ask, all of `input` is marked: the whole
/// body and every header by name and value, so nothing no codec vouched for is journaled
/// verbatim.
fn inbound_spans(redact: Option<Redact<'_>>, input: Inbound<'_>) -> (InboundSpans, bool) {
    let named = redact.map(|redact| redact(input));
    if let Some(spans) = named.as_ref().filter(|s| s.check(input).is_ok()) {
        return (spans.clone(), false);
    }
    let (bytes, headers) = match input {
        Inbound::Frame(frame) => (frame.bytes(), 0),
        Inbound::Http(_, resp) => (resp.body, resp.headers.len()),
    };
    // A body past u32 cannot be journaled at all: the journal refuses it for its size.
    let end = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    let body = (end > 0).then_some(0..end).into_iter().collect();
    let marks = (0..headers as u32).map(|at| (at, HeaderMark::NameAndValue));
    (
        InboundSpans::response(marks.collect(), body),
        named.is_some(),
    )
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
pub(crate) fn frame_of(effect: &Effect, own: StreamId, socket: bool) -> Option<Request> {
    let frame = socket && matches!(effect, Effect::Send { stream, .. } if *stream == own);
    Request::of(effect).filter(|_| frame)
}

/// Whether `effect` is an HTTP request.
fn is_http(effect: &Effect) -> bool {
    matches!(effect, Effect::Http { .. })
}

/// What the HTTP effect `effect` is charged: the request and the connection it opens, which a
/// limit on new connections counts too (Codex r4179474175), both or neither. `None` for any
/// other effect.
pub(crate) fn http_of(effect: &Effect) -> Option<[Request; 2]> {
    let request = Request::of(effect).filter(|_| is_http(effect))?;
    let connect = Request {
        charge: RateCharge::one(OpKind::Connect, None),
        ..request
    };
    Some([request, connect])
}

/// The next of `effects` to execute, with whether it was charged already and the input it is
/// attributed to: a frame on stream `own` of a socket endpoint is charged on connection `key`
/// as its turn comes, unless it was already, and one the buckets refuse is dropped unwritten
/// (decision 0030); an HTTP request is charged as it starts ([`start_http`]).
fn next_admitted(
    effects: &mut VecDeque<(Effect, bool, Option<Stamp>)>,
    rates: &RateLimiter,
    key: ConnKey,
    own: StreamId,
    socket: bool,
) -> Option<(Effect, bool, Option<Stamp>)> {
    std::iter::from_fn(|| effects.pop_front()).find_map(|(effect, charged, origin)| {
        let request = frame_of(&effect, own, socket).filter(|_| !charged);
        let admitted = request.is_none_or(|r| rates.charge(Instant::now(), key, &[r]).is_ok());
        admitted.then_some((effect, charged, origin))
    })
}

/// The request of the HTTP effect `ask`, for the codec of epoch `key`, as it runs beside the
/// session until it is answered, fails or times out. Its timeout runs from `now`, when the
/// codec asked; one past the end of the clock bounds nothing, so the request is not sent, nor is one
/// the runtime cannot make, both before anything is charged (Codex r4179682244), nor one the
/// buckets refuse (decision 0030): each comes back as [`HttpFailure::NotSent`]; one `charged`
/// already, with the connection it opens ([`http_of`]), is not charged again. A 429 or 418 to
/// it is counted under the scopes it was charged to. `None` for any other effect.
fn start_http(
    ask: Effect,
    key: ConnKey,
    now: Instant,
    rates: &RateLimiter,
    connector: &Connector,
    max_body: usize,
    charged: bool,
) -> Option<Pending> {
    let charges = http_of(&ask);
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
    // The request and the connection it opens are charged together ([`http_of`]). A 429 or 418
    // answers the request, so it is counted under the request's scopes alone (Codex
    // r4179558360).
    let ready = deadline.zip(http::ready(&req));
    let go = ready.zip(charges).and_then(|(ready, [r, c])| {
        let admitted = charged || rates.charge(now, key, &[r, c]).is_ok();
        admitted.then(|| (ready, rates.scopes(key, &r)))
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
pub(crate) fn close(ws: &mut WebSocket, rates: &RateLimiter, key: ConnKey) {
    if rates.charge(Instant::now(), key, &[CONTROL]).is_ok() {
        let _ = ws.close(None).now_or_never();
    }
}

/// The record of a WebSocket control message a session received: a ping's or pong's payload,
/// or a close frame's code and reason. `None` for a data frame, and for a raw frame, which
/// only a sender builds: the WebSocket layer reads none, so it takes no place in ingest order.
fn ws_control(message: &Message) -> Option<WsControl> {
    Some(match message {
        Message::Ping(payload) => WsControl::Ping(Opaque(payload.to_vec())),
        Message::Pong(payload) => WsControl::Pong(Opaque(payload.to_vec())),
        Message::Close(close) => WsControl::Close(close.as_ref().map(|c| CloseRec {
            code: c.code.into(),
            reason: c.reason.as_str().to_owned(),
        })),
        Message::Text(_) | Message::Binary(_) | Message::Frame(_) => return None,
    })
}

/// The next message on the socket; a poll endpoint has none, ever.
pub(crate) async fn next_frame(
    ws: &mut Option<WebSocket>,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    match ws {
        Some(ws) => ws.next().await,
        None => std::future::pending().await,
    }
}

/// Sleeps until `at`, or forever when there is no deadline.
pub(crate) async fn sleep_or_never(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// FBC-drf: a control message is journaled with its payload, or its close code and reason;
    /// a data frame is none, nor is a raw frame, which the WebSocket layer never reads.
    #[test]
    fn a_control_message_is_recorded_with_what_it_carries() {
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::Frame;
        let close = CloseFrame {
            code: 4000.into(),
            reason: "bye".into(),
        };
        let cases = [
            (
                Message::Ping(b"p".to_vec().into()),
                Some(WsControl::Ping(Opaque(b"p".to_vec()))),
            ),
            (
                Message::Pong(Vec::new().into()),
                Some(WsControl::Pong(Opaque(Vec::new()))),
            ),
            (Message::Close(None), Some(WsControl::Close(None))),
            (
                Message::Close(Some(close)),
                Some(WsControl::Close(Some(CloseRec {
                    code: 4000,
                    reason: "bye".into(),
                }))),
            ),
            (Message::text("t"), None),
            (Message::binary(vec![1]), None),
            (Message::Frame(Frame::ping(vec![2])), None),
        ];
        for (message, want) in cases {
            assert_eq!(ws_control(&message), want, "{message:?}");
        }
    }

    /// Inputs that take what they are handed and ask for nothing; halted when `.0` says so.
    struct Nothing(bool);

    impl EpochInputs for Nothing {
        fn redact_inbound(&self, _: Inbound<'_>) -> InboundSpans {
            InboundSpans::NONE
        }
        fn ring(&mut self, _: &mut Epochs, _: Stamp, _: TimerTag, _: &mut Effects) {}
        fn answer(&mut self, _: &mut Epochs, _: Stamp, _: Answered, _: &mut Effects) {}
        fn on_tick_to_wire(&mut self, _: TickToWire) {}
        fn halted(&self) -> bool {
            self.0
        }
    }

    /// FBC-bnl: once the control has dropped, or the inputs halted, the core executes no
    /// effect, so what was asked for while a write waited is not executed after a stop that
    /// came as it completed, nor after a timer firing that ended the session (Reviewer B, B1).
    #[tokio::test]
    async fn once_the_control_dropped_or_the_inputs_halted_no_effect_is_executed() {
        let reserve = crate::ratelimit::SafetyReserve::percent(0).unwrap();
        let (tx, stop) = watch::channel(());
        let ms = Duration::from_millis;
        let pacing = ReconnectPacing::new(ms(10), ms(100), 10, ms(1_000), ms(1_000)).unwrap();
        let mut core = Core::new(CoreConfig {
            own: StreamId(1),
            connector: Connector::new(crate::ProxyConfig::Direct),
            clock: IngestClock::new(),
            pacing,
            write_stall: ms(1_000),
            conn: 1,
            stop,
            http_max_body: 0,
            rates: RateLimiter::new(&[], reserve).unwrap(),
        });
        let timer = || Effect::Timer {
            tag: TimerTag(1),
            after: Duration::ZERO,
        };
        let mut fx = Effects::new();
        fx.push(timer());
        let open = core
            .execute(&mut None, &mut Nothing(false), fx, false, None)
            .await;
        assert_eq!((open, core.next_deadline().is_some()), (Ok(true), true));
        // No market-data request awaits an answer: a frame of one is nothing to the inputs.
        Nothing(false).sent_rpc(fbc_core::RpcCall {
            id: fbc_core::RpcId(1),
            timeout: Duration::ZERO,
        });
        let fired = core.fire(&mut None, &mut Nothing(false)).await;
        assert_eq!((fired, core.next_deadline()), (Ok(true), None));
        let mut fx = Effects::new();
        fx.push(timer());
        let open = core
            .execute(&mut None, &mut Nothing(true), fx, false, None)
            .await;
        assert_eq!((open, core.next_deadline()), (Ok(false), None));
        drop(tx);
        let mut fx = Effects::new();
        fx.push(timer());
        let open = core
            .execute(&mut None, &mut Nothing(false), fx, false, None)
            .await;
        assert_eq!((open, core.next_deadline()), (Ok(false), None));
    }
}
