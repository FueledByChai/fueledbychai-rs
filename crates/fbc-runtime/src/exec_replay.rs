//! Decoder replay of an order-entry session (FBC-tgt; decisions 0006, 0014, 0053, 0056, 0057,
//! 0078, 0081, 0090): an [`ExecSession`](crate::ExecSession)'s journal fed back to a fresh codec
//! of its venue through the same calls the live session made, as [`MdReplay`](crate::MdReplay)'s
//! decoder replay does for market data (not closed-loop), so a recorded session rebuilds the
//! same execution events.
//!
//! An [`ExecReplay`] takes the records of one session in journal order, with no socket, no
//! clock and no nonce source: those of its order connection, connection number `conn`, and of
//! endpoint `i` of the venue's plan, `conn + i` (decision 0115). As live, one codec serves every
//! epoch, built once from the caller's credentials ([`VenueFactory::exec_codec`]), since it holds
//! the account's state across reconnects (0053):
//!
//! - an `Opened` control record begins an epoch of its endpoint, and the context the journal
//!   holds next is `on_open`'s: the codec's `on_open` is called with that endpoint's stream and
//!   the context as journaled, the nonces reserved for it and the time it was given (0014 item 1,
//!   0078); `Closed` ends the epoch, and the handler is told it ended
//!   ([`ExecHandler::on_epoch_end`]), as live, an epoch closed whose opening the journal lost
//!   included; an `Opened` while its endpoint's epoch is still open (its `Closed` was dropped)
//!   ends that one first;
//! - each inbound frame and HTTP result of an open epoch goes to the codec with its recorded
//!   stamp, inside the venue's decode scope with the consumer's engine namespace (0015); a codec
//!   timer's firing calls `on_timer` with the context the journal holds next, which carries the
//!   firing's stamp for its time (0056); one of an epoch that is not open (it ended, or the
//!   journal lost its opening) reaches no codec and is counted stale, as live a late arrival is,
//!   and so does an input the live session took as its control dropped, which it journaled after
//!   the epoch's `Closed` (0078 item 4). A ping, pong or close frame reached no codec live
//!   (0041), so its record reaches none here;
//! - a request deadline's firing (0090) calls `on_rpc_timeout` at its recorded stamp: on the
//!   order connection's open epoch as the connected session does, the request failing that
//!   epoch's arm when it is the arm, and otherwise, the session waiting to reconnect then, as the
//!   waiting session does;
//! - a context journaled with no request id that follows no opening and no timer firing is the
//!   resync's, asked on the order connection's open epoch (0058, 0138 item 3), and one with a
//!   request id an encode's (0057): the session's own cancel-on-disconnect arm where the session
//!   sends it, its epoch authenticated with the arm due, as the gate the live session runs says,
//!   built from the same caps; otherwise a command the consumer submitted, which the journal does
//!   not hold (it holds its context, nonces and outbound bytes), so the caller supplies it by
//!   request id ([`ExecReplay::set_commands`], decision 0138), and a context whose command it
//!   does not supply calls nothing
//!   and is counted ([`ExecReplayCounters::unsupplied`]): what answers that request may then
//!   decode otherwise than live. The arm counts as sent, so that its answer settles its epoch's
//!   arm as live, only where the record after its context is its frame written on that epoch:
//!   live, the session journals the frame as it writes it, straight after the context, and
//!   writes none of an arm whose effects do not carry it or the buckets refused, which fails
//!   the epoch's arm (0058, 0073, 0081); an arm the codec refused fails it at once, as live;
//! - every event the codec pushes is stamped with its input's stamp and handed to the consumer's
//!   [`ExecHandler`] through the function the live session hands it through: the order
//!   connection's authentication, the arm's answer, the resync's end and what the event shows of
//!   the orders resting from an earlier epoch count as live, and those orders are told once
//!   ([`ExecHandler::on_unprotected`]) where the live session told them; a frame or result the
//!   codec cannot decode is counted and the handler told ([`ExecHandler::on_decode_error`]);
//! - [`ExecReplay::new`] gives a control, as [`ExecSession::new`](crate::ExecSession::new) does,
//!   which the caller hands the replayed handler where the session's went: a handler that drops
//!   it, as it dropped its session's, part-way through an input included, is handed no event the
//!   codec pushes after (each counted, [`ExecReplayCounters::stopped`]) and not told the
//!   unprotected orders with the event it dropped it on, as live (0053). The replay still feeds
//!   the codec what the journal holds after, as live the inputs taken before the stop reached it.
//!
//! Effects the codec asks for are not executed, and the outbound frames, write results, HTTP
//! requests, nonce records and what became of each submission are outputs, not fed to anything
//! nor handed to the handler ([`ExecHandler::on_submitted`] and
//! [`ExecHandler::on_tick_to_wire`] are not called): reproducing the outbound bytes is exact
//! replay's (FBC-qwq). A consumer's own record (decision 0101) is passed over. Records the sink
//! dropped leave a `Degraded` marker, counted as a gap: a replay across one is not exact, and a
//! call whose context it dropped (an `on_open` or `on_timer`, whose contexts go under Normal) is
//! not made and is counted ([`ExecReplayCounters::lost_contexts`]).
//!
//! What the journal does not say, the replay cannot reproduce:
//!
//! - A context record names no connection, so a journal replayed must hold the records of one
//!   order-entry session only: the replay takes every context in the journal as its session's,
//!   and another session's would call its codec otherwise than live. A request another
//!   connection's records name (an outbound frame or an HTTP request with a request id, which
//!   only order entry sends) is counted ([`ExecReplayCounters::foreign_requests`]) as the sign of
//!   one. Market-data sessions journal no context.
//! - Where a stop the replayed handler does not make came is not journaled (FBC-s4bg): one from
//!   another thread, or from a handler the replay does not call there (another session's, or
//!   this one's `on_submitted`), part-way through an input, whose events pushed after it the
//!   live session handed nobody while the input, taken before, is journaled in the open epoch;
//!   and one that came while the session waiting to reconnect handed on what fell due, as it
//!   reported a command not sent included, before a request deadline it still took and
//!   journaled but whose events it handed nobody (0090 item 2). A replay whose control stands
//!   hands those events on.
//! - The order connection's epoch ends, live, as its connection does, a little before the
//!   session journals its `Closed`: an input another endpoint brings in between, while the
//!   session still executes what that endpoint asked for (0115), is handed on with no
//!   authenticated epoch to count for. The replay ends the epoch at its `Closed`, so it counts
//!   such an input's events for the epoch, as the arm's answer, the resync's end or what they
//!   show of the orders held unprotected.
//!
//! The caller supplies what the session ran with: the [`VenueConfig`], the credentials its codec
//! is built from, its engine namespace, the [`SpecTable`] and its connection number. A venue the
//! live session would refuse (no order entry, no endpoint, a cancel-on-disconnect protection it
//! takes none of) is refused here with the session's own error.

use std::rc::Rc;

use fbc_core::{
    ConnKey, Effects, EncodeCtx, ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpTag, Namespace,
    PathStamps, RawFrame, RpcId, Secrets, SpecTable, Stamp, StreamId, TimerTag, VenueCaps,
    VenueCommand, VenueConfig, VenueFactory, VenueMeta,
};
use fbc_journal::{ControlEvent, HttpResponseRec, JournalError, Marker, Opcode, Record};
use tokio::sync::watch;

use crate::exec_orders::Shared;
use crate::exec_session::{
    Decoding, ExecControl, ExecHandler, ExecSessionError, Late, OrderConn, gate_flags, hand_on,
};
use crate::replay::{ReplayError, with_response};
use crate::session::SessionError;
use crate::session_core::IngestClock;

/// What a replay needs, all from the consumer: what the session ran with.
pub struct ExecReplayConfig {
    pub venue: &'static dyn VenueFactory,
    pub cfg: VenueConfig,
    /// The credentials the codec is built from, as the session's was.
    pub creds: Secrets,
    /// The engine namespace the session decoded under (decision 0015).
    pub ns: Namespace,
    pub specs: SpecTable,
    /// The session's connection number, its order connection's: endpoint `i` of the venue's
    /// plan is `conn + i`, and records of every other connection are skipped.
    pub conn: u16,
}

/// What a replay counted.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ExecReplayCounters {
    /// Epochs opened, of every endpoint.
    pub epochs: u64,
    /// Inbound frames fed to the codec.
    pub frames: u64,
    /// HTTP results fed to the codec.
    pub http: u64,
    /// Codec timers fired into `on_timer`.
    pub timers: u64,
    /// Request deadlines fired into `on_rpc_timeout`.
    pub deadlines: u64,
    /// Resyncs asked of the codec.
    pub resyncs: u64,
    /// Calls of the codec's `encode`, the session's own arms included, whether the codec
    /// encoded the command or refused it.
    pub encodes: u64,
    /// Encode contexts of a request whose command the caller did not supply: no encode was made.
    pub unsupplied: u64,
    /// Calls the live session made whose context the journal does not hold (an `on_open` or an
    /// `on_timer`): not made.
    pub lost_contexts: u64,
    /// Contexts of a resync or an encode while the order connection had no open epoch, which
    /// no live session gives: no call was made.
    pub unplaced: u64,
    /// Inputs of an epoch that was not open, which reached no codec.
    pub stale: u64,
    /// Frames and results the codec could not decode.
    pub decode_errors: u64,
    /// Events the codec pushed once the replay's control had dropped: handed to no one, as live
    /// (decision 0053).
    pub stopped: u64,
    /// Outbound frames and HTTP requests naming a request id on a connection that is not this
    /// session's: another order-entry session's, whose contexts the journal does not tell from
    /// this one's, so the replay is not of one session.
    pub foreign_requests: u64,
    /// `Degraded` markers: places where the journal dropped records.
    pub gaps: u64,
}

/// The call the context the journal holds next was given to.
enum Call {
    /// `on_open` of endpoint `endpoint`'s epoch.
    Open { endpoint: usize },
    /// `on_timer` of the timer tagged so.
    Timer(TimerTag),
}

/// Where a replay finds a consumer's command by its request id.
type Commands = Box<dyn FnMut(RpcId) -> Option<VenueCommand>>;

/// One order-entry session's journal, replayed through one codec of its venue into a handler.
pub struct ExecReplay<H> {
    codec: Box<dyn ExecCodec>,
    caps: VenueCaps,
    ns: Namespace,
    specs: SpecTable,
    /// The order connection's connection number.
    conn: u16,
    /// Each planned endpoint's stream, by its place in the plan.
    streams: Vec<StreamId>,
    /// Each endpoint's open epoch.
    open: Vec<Option<ConnKey>>,
    /// The call the context the journal holds next was given to, if it was one of the calls
    /// matched by their place: an `on_open` or an `on_timer`.
    next: Option<Call>,
    /// The session's own arm the codec just encoded, with the epoch it was encoded on, until the
    /// next record says whether the session wrote it.
    arm: Option<(ConnKey, RpcId)>,
    /// The readiness and gate the events move, as the live session's do.
    orders: Rc<Shared>,
    /// The drop of the control [`ExecReplay::new`] gave.
    stop: watch::Receiver<()>,
    commands: Option<Commands>,
    counters: ExecReplayCounters,
    handler: H,
}

/// The sink the events of an input stamped `$stamp` go through ([`Deliver`]), `$connected`
/// when the order connection's epoch is open, built from the replay's fields other than its
/// codec, so the codec can be called beside it.
macro_rules! deliver {
    ($s:ident, $stamp:expr, $connected:expr) => {
        Deliver {
            orders: &$s.orders,
            handler: &mut $s.handler,
            order: OrderConn {
                stream: $s.streams[0],
                conn: $s.conn,
            },
            stamp: $stamp,
            connected: $connected,
            stop: &$s.stop,
            stopped: &mut $s.counters.stopped,
        }
    };
}

impl<H: ExecHandler> ExecReplay<H> {
    /// A replay of `config`'s session, handing events to `handler`, and its control: the
    /// handler is handed nothing more once it drops, as the session's handler once the session's
    /// dropped. Refused, with the session's own error, for a venue whose order entry the session
    /// would refuse.
    pub fn new(
        config: ExecReplayConfig,
        handler: H,
    ) -> Result<(ExecReplay<H>, ExecControl), ExecSessionError> {
        let ExecReplayConfig {
            venue,
            cfg,
            creds,
            ns,
            specs,
            conn,
        } = config;
        let caps = venue.caps(&cfg).map_err(SessionError::Config)?;
        let exec = caps.exec.as_ref().ok_or(ExecSessionError::NoOrderEntry)?;
        let (rearm, covers_open, one_id) =
            gate_flags(&exec.order).map_err(ExecSessionError::CancelOnDisconnect)?;
        let plan = venue.plan_exec(&cfg).map_err(ExecSessionError::Venue)?;
        if plan.is_empty() {
            return Err(ExecSessionError::Endpoints(0));
        }
        let codec = venue.exec_codec(&cfg, creds);
        let codec = codec.ok_or(ExecSessionError::NoOrderEntry)?;
        let codec = codec.map_err(ExecSessionError::Venue)?;
        let streams: Vec<StreamId> = plan.iter().map(|ep| ep.stream).collect();
        let (control, stop) = ExecControl::pair();
        let orders = Shared::replaying(rearm, covers_open, one_id, IngestClock::new());
        let replay = ExecReplay {
            codec,
            caps,
            ns,
            specs,
            conn,
            open: vec![None; streams.len()],
            streams,
            next: None,
            arm: None,
            orders,
            stop,
            commands: None,
            counters: ExecReplayCounters::default(),
            handler,
        };
        Ok((replay, control))
    }

    /// Supplies each command the consumer submitted by the request id it was given: an encode
    /// context of a request that is not the session's own arm is encoded with the command
    /// `commands` returns for its id, and with none is counted and encodes nothing (decision
    /// 0138).
    pub fn set_commands(&mut self, commands: impl FnMut(RpcId) -> Option<VenueCommand> + 'static) {
        self.commands = Some(Box::new(commands));
    }

    pub fn counters(&self) -> ExecReplayCounters {
        self.counters
    }

    /// Feeds every record of `records` in order ([`ExecReplay::feed`]): a whole journal, as
    /// [`JournalReader`](fbc_journal::JournalReader) reads it.
    pub fn run(
        &mut self,
        records: impl IntoIterator<Item = Result<Record, JournalError>>,
    ) -> Result<(), ReplayError> {
        for record in records {
            self.feed(&record.map_err(ReplayError::Journal)?)?;
        }
        Ok(())
    }

    /// Feeds one record; records of other connections and outputs are skipped. A text frame
    /// whose bytes are not UTF-8, which no session journals, is refused.
    pub fn feed(&mut self, record: &Record) -> Result<(), ReplayError> {
        if let Some(arm) = self.arm.take() {
            self.written(arm, record);
        }
        match record {
            Record::Marker(Marker::Degraded { .. }) => self.counters.gaps += 1,
            Record::Control { ev, .. } => self.control(ev),
            Record::EncodeCtx { rpc, ctx } => self.context(*rpc, ctx),
            Record::Inbound {
                stamp,
                opcode,
                bytes,
                ..
            } => {
                if let Some(endpoint) = self.endpoint(stamp.conn) {
                    let raw = match opcode {
                        Opcode::Text => {
                            RawFrame::Text(std::str::from_utf8(&bytes.0).map_err(|_| {
                                ReplayError::NotUtf8 {
                                    ingest_seq: stamp.ingest_seq,
                                }
                            })?)
                        }
                        Opcode::Binary => RawFrame::Binary(&bytes.0),
                    };
                    self.input();
                    self.frame(endpoint, *stamp, raw);
                }
            }
            Record::HttpResult { stamp, tag, result } => {
                if let Some(endpoint) = self.endpoint(stamp.conn) {
                    self.input();
                    self.http(endpoint, *stamp, *tag, result);
                }
            }
            Record::Timer { stamp, tag } => {
                if let Some(endpoint) = self.endpoint(stamp.conn) {
                    self.input();
                    if self.current(endpoint, stamp.conn) {
                        self.next = Some(Call::Timer(*tag));
                    } else {
                        self.counters.stale += 1;
                    }
                }
            }
            // A deadline is the order connection's: the session stamps it under the epoch it is
            // in or waits to open (0090).
            Record::RpcTimeout { stamp, rpc } if stamp.conn.conn == self.conn => {
                self.input();
                self.deadline(*stamp, *rpc);
            }
            // A ping, pong or close reached no codec live (0041).
            Record::InboundControl { stamp, .. } if self.endpoint(stamp.conn).is_some() => {
                self.input();
            }
            // Only order entry names a request (0057, 0081): another connection's is another
            // order-entry session's.
            Record::Outbound {
                conn, rpc: Some(_), ..
            }
            | Record::HttpRequest {
                conn, rpc: Some(_), ..
            } if self.endpoint(*conn).is_none() => self.counters.foreign_requests += 1,
            _ => {}
        }
        Ok(())
    }

    /// The place in the plan of the endpoint whose connection `key` names, if it is one of
    /// this session's.
    fn endpoint(&self, key: ConnKey) -> Option<usize> {
        let at = usize::from(key.conn.checked_sub(self.conn)?);
        (at < self.streams.len()).then_some(at)
    }

    /// Whether `key` is the open epoch of endpoint `endpoint`.
    fn current(&self, endpoint: usize, key: ConnKey) -> bool {
        self.open[endpoint] == Some(key)
    }

    /// An input of this session's comes: a call that waited for its context never got it, the
    /// journal having dropped it.
    fn input(&mut self) {
        if self.next.take().is_some() {
            self.counters.lost_contexts += 1;
        }
    }

    /// The arm request `rpc`, encoded on epoch `key`, was sent where `record`, the one after
    /// its context, is its frame written on that epoch; otherwise the session wrote nothing of
    /// it, and its epoch's arm failed (decisions 0058, 0073).
    fn written(&mut self, (key, rpc): (ConnKey, RpcId), record: &Record) {
        let written = matches!(
            record,
            Record::Outbound { conn, rpc: Some(id), .. } if *conn == key && *id == rpc
        );
        let mut gate = self.orders.gate.borrow_mut();
        if written {
            gate.arm_sent(key.epoch, rpc);
        } else {
            gate.arm_not_sent(key.epoch);
        }
    }

    fn control(&mut self, ev: &ControlEvent) {
        match ev {
            ControlEvent::Opened(key) => {
                if let Some(endpoint) = self.endpoint(*key) {
                    self.input();
                    // An opening with no close before it (the close was dropped) ends the open
                    // epoch first.
                    if let Some(open) = self.open[endpoint] {
                        self.end(endpoint, open);
                    }
                    self.open[endpoint] = Some(*key);
                    self.next = Some(Call::Open { endpoint });
                    self.counters.epochs += 1;
                }
            }
            ControlEvent::Closed(key) => {
                if let Some(endpoint) = self.endpoint(*key) {
                    // A call that waited for its context then was never made live: the session
                    // stopped or failed first.
                    self.next = None;
                    self.end(endpoint, *key);
                }
            }
            ControlEvent::Subscribe { .. } => {}
        }
    }

    /// Ends epoch `key` of endpoint `endpoint`, the open one or not, and tells the handler, as
    /// live: the order connection's ends its readiness.
    fn end(&mut self, endpoint: usize, key: ConnKey) {
        if self.current(endpoint, key) {
            self.open[endpoint] = None;
            if endpoint == 0 {
                self.orders.set_ready(None);
            }
        }
        self.handler.on_epoch_end(key);
    }

    /// The context `ctx` the session gave a call: an encode's with its request id `rpc`, the
    /// call that waited for it, or the resync's.
    fn context(&mut self, rpc: Option<RpcId>, ctx: &EncodeCtx) {
        let mut fx = Effects::new();
        match (rpc, self.next.take()) {
            (None, Some(Call::Open { endpoint })) => {
                self.codec.on_open(self.streams[endpoint], ctx, &mut fx);
            }
            (None, Some(Call::Timer(tag))) => {
                self.codec.on_timer(tag, ctx, &mut fx);
                self.counters.timers += 1;
            }
            (None, None) => self.resync(ctx),
            (Some(rpc), waited) => {
                self.counters.lost_contexts += u64::from(waited.is_some());
                self.encode(rpc, ctx);
            }
        }
    }

    /// The codec's resync, asked with `ctx` on the order connection's open epoch.
    fn resync(&mut self, ctx: &EncodeCtx) {
        let Some(key) = self.open[0] else {
            self.counters.unplaced += 1;
            return;
        };
        self.codec.resync(ctx, &mut Effects::new());
        self.orders.gate.borrow_mut().resync_asked(key.epoch);
        self.counters.resyncs += 1;
    }

    /// Encodes request `rpc` with `ctx` on the order connection's open epoch: the session's own
    /// arm where it sends one, as its gate says, or the consumer's command the caller supplies.
    fn encode(&mut self, rpc: RpcId, ctx: &EncodeCtx) {
        let Some(key) = self.open[0] else {
            self.counters.unplaced += 1;
            return;
        };
        let epoch = key.epoch;
        let arm = self.orders.ready() == Some(epoch) && self.orders.gate.borrow().arm_due(epoch);
        let cmd = if arm {
            Some(VenueCommand::ArmCancelOnDisconnect(true))
        } else {
            self.commands.as_mut().and_then(|commands| commands(rpc))
        };
        let Some(cmd) = cmd else {
            self.counters.unsupplied += 1;
            return;
        };
        let mut t = PathStamps::off();
        let mut fx = Effects::new();
        let encoded = self
            .codec
            .encode(&cmd, rpc, &self.specs, ctx, &mut t, &mut fx);
        self.counters.encodes += 1;
        if arm {
            // Whether the session wrote an arm the codec encoded, the next record says.
            match encoded {
                Ok(_) => self.arm = Some((key, rpc)),
                Err(_) => self.orders.gate.borrow_mut().arm_not_sent(epoch),
            }
        }
    }

    /// Hands `raw`, a frame of endpoint `endpoint` stamped `stamp`, to the codec when its epoch
    /// is open.
    fn frame(&mut self, endpoint: usize, stamp: Stamp, raw: RawFrame<'_>) {
        if !self.current(endpoint, stamp.conn) {
            self.counters.stale += 1;
            return;
        }
        let decoding = Decoding {
            caps: &self.caps,
            ns: self.ns,
            specs: &self.specs,
        };
        let stream = self.streams[endpoint];
        let mut sink = deliver!(self, stamp, true);
        let fed = decoding.frame(
            &mut *self.codec,
            stream,
            raw,
            &mut sink,
            &mut Effects::new(),
        );
        self.counters.frames += 1;
        self.decoded(stamp, fed.is_ok());
    }

    /// Hands the result of the HTTP request tagged `tag`, of endpoint `endpoint` and stamped
    /// `stamp`, to the codec when its epoch is open.
    fn http(
        &mut self,
        endpoint: usize,
        stamp: Stamp,
        tag: HttpTag,
        result: &Result<HttpResponseRec, HttpFailure>,
    ) {
        if !self.current(endpoint, stamp.conn) {
            self.counters.stale += 1;
            return;
        }
        let decoding = Decoding {
            caps: &self.caps,
            ns: self.ns,
            specs: &self.specs,
        };
        let mut sink = deliver!(self, stamp, true);
        let codec = &mut *self.codec;
        let mut fx = Effects::new();
        let fed = match result {
            Ok(rec) => with_response(rec, |resp| {
                decoding.http(codec, tag, Ok(resp), &mut sink, &mut fx)
            }),
            Err(e) => decoding.http(codec, tag, Err(*e), &mut sink, &mut fx),
        };
        self.counters.http += 1;
        self.decoded(stamp, fed.is_ok());
    }

    /// Counts a frame or result the codec could not decode and tells the handler, as live.
    fn decoded(&mut self, stamp: Stamp, ok: bool) {
        if !ok {
            self.counters.decode_errors += 1;
            self.handler.on_decode_error(stamp.conn);
        }
    }

    /// Hands request `rpc`, whose deadline fired at `stamp`, to the codec's `on_rpc_timeout`:
    /// on the order connection's open epoch as the connected session does, failing that epoch's
    /// arm when it is the arm; otherwise as the session waiting to reconnect does.
    fn deadline(&mut self, stamp: Stamp, rpc: RpcId) {
        self.counters.deadlines += 1;
        let connected = self.current(0, stamp.conn);
        if connected {
            self.orders.gate.borrow_mut().timed_out(rpc);
        }
        let mut sink = deliver!(self, stamp, connected);
        self.codec.on_rpc_timeout(rpc, &mut sink);
    }
}

/// Hands each event the codec pushes for an input stamped `stamp` to the handler as the live
/// session does: on the order connection's open epoch through [`hand_on`], and otherwise, the
/// session waiting to reconnect, as the waiting session does ([`Late`]). Once the replay's
/// control has dropped, each is dropped and counted instead, as live (decision 0053).
struct Deliver<'a, H> {
    orders: &'a Shared,
    handler: &'a mut H,
    order: OrderConn,
    stamp: Stamp,
    connected: bool,
    stop: &'a watch::Receiver<()>,
    stopped: &'a mut u64,
}

impl<H: ExecHandler> ExecSink for Deliver<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        let stop = self.stop;
        let dropped = || stop.has_changed().is_err();
        if dropped() {
            *self.stopped += 1;
        } else if self.connected {
            let (order, stamp) = (self.order, self.stamp);
            let handler = &mut *self.handler;
            hand_on(self.orders, handler, order, stamp, (meta, ev), &dropped);
        } else {
            let mut late = Late {
                handler: &mut *self.handler,
                stop,
                stamp: self.stamp,
                clock: self.orders.clock(),
            };
            late.push(meta, ev);
        }
    }
}
