//! An order-entry session (FBC-oaz, decision 0053): drives one account's order-entry
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
//! refused and counted. A write waits on its peer at most the consumer's [`WriteStall`] window
//! (0036).
//!
//! **The journal (FBC-2pr, decisions 0006, 0078).** With a [`Journal`] set
//! ([`ExecSession::set_journal`]), the session records what crosses its boundary as a
//! market-data session does: each epoch's opening and closing, every frame and HTTP result it
//! receives at its stamp (a stale one too) with the credentials the codec names in it as keyed
//! hashes (0028), every ping, pong and close frame it receives (0041), every frame it writes with
//! its kind and redaction spans and the result of the write, every HTTP request with its
//! request id, and every codec timer that fires. It also records each nonce it reserves from
//! the consumer's [`NonceSource`], one `Nonce` record per value under the account's number as
//! the source, as it is reserved (a short reservation's values included), and the
//! [`EncodeCtx`] it then hands `on_open`, `on_timer`, the resync or an `encode`, the encode's
//! with its request id, just before the call. An inbound frame or HTTP result is journaled
//! under Safety, since an order-entry stream carries acks and fills that cannot be told apart
//! before they are decoded; a write under its frame's class; an encode's nonces and context
//! under its command's class, so a cancel's or a reducing order's are Safety; the rest under
//! Normal. An input that came as the control dropped follows the epoch's `Closed`, reaching no
//! codec; spans a codec names that do not fit their input are counted
//! ([`ExecCounters::refused_redactions`]) and that input is hashed whole. An encode's time is
//! read once its nonces are reserved. Nothing waits on the journal: a record the sink has no
//! room for is dropped and
//! counted there (0006). A request's deadline firing is stamped but not yet journaled
//! (FBC-0hfl).
//!
//! **HTTP requests, timers and keepalives (FBC-bnl, decision 0056).** A request the codec asks
//! for runs beside the session's reads with its own timeout, charged to the buckets, and its
//! result, or the [`HttpFailure`](fbc_core::HttpFailure) that stands for one, is stamped and
//! handed to the codec's `on_http` inside the venue's decode scope only while the epoch that
//! asked is current: one that comes back after a reconnect is dropped and counted
//! ([`ExecSession::stale`] of [`Input::Http`]), although the codec lives on, as 0027 has it for
//! market data. A timer is set for the epoch that asked; one of an ended epoch fires into
//! nothing, dropped and counted ([`Input::Timer`]), and one of the current epoch calls `on_timer`
//! with an [`EncodeCtx`] holding exactly the nonces [`ExecCodec::nonces_for`] asks for with
//! [`CtxCall::Timer`], reserved from the consumer's [`NonceSource`] as for `on_open`, and the
//! firing's stamp for its time. A source that reserves another count ends the session with
//! [`ExecSessionError::Nonces`], and the timer reaches no codec. An order-entry codec keeps its
//! stream alive with its own timer: it arms one and sends its keepalive frame, charged as it
//! declares, as each fires (design §4.8, `on_timer`). Timers keep firing, and results keep
//! coming back, while a write waits on a peer that stopped reading (FBC-ha3), and what they ask
//! for joins the rest of the write's batch. Once the session stops, no timer firing or result
//! reaches the codec, no effect is executed, and requests still in flight are dropped.
//!
//! **Submitting commands (FBC-0ga, decision 0057).** Commands reach the codec only through the
//! session's [`ExecOrders`] ([`ExecSession::orders`]): an order-affecting one only as the
//! fbc-oms [`Authorization`](fbc_oms::Authorization) issued for it, for the session's account
//! (0013 rule 2, 0045), kept until its command is encoded, and one that affects no order as a
//! [`ControlCommand`](fbc_oms::ControlCommand). Each is given its [`RpcId`] at once and is
//! taken on the session's next turn, after the input being handled: one submitted while the
//! stream had no epoch the codec reported authenticated
//! ([`ConnState::Authenticated`](fbc_core::ConnState::Authenticated)), or on an epoch that has
//! since ended, is `NotSent(Disconnected)`. An authorization fbc-oms's check at submit
//! ([`Authorization::check_at_submit`](fbc_oms::Authorization::check_at_submit), decision
//! 0060) refuses, run here just before the encode and not when it was queued, is
//! `NotSent(StaleAuthorization)` with no nonce reserved and nothing written: a place, a batch
//! or an amend whose market's state changed since it was built (the kill switch, a disarm, an
//! arming call), or an instrument cancel-all whose I7 guard moved; a cancel and a cancel-many
//! always pass (FBC-j5bw, decision 0062). Otherwise it is encoded with an [`EncodeCtx`]
//! holding exactly [`VenueCommand::items`](fbc_core::VenueCommand::items) nonces reserved from
//! the consumer's [`NonceSource`] and the shard clock's time. What the codec refuses is
//! `NotSent` for its reason; effects that do not carry the request
//! ([`Effects::carry_request`]), or that name another stream, a reconnect, an HTTP request or
//! a request whose deadline is past the end of the clock, are `NotSent(Unencodable)`, and
//! frames the buckets do not admit together `NotSent(RateBudget)`, each with nothing written.
//! The frames are charged in the class of their command (FBC-e8i, decision 0073): a normal
//! place or amend and every control command the consumer submits (an order query, an arm, a
//! dead-man refresh, the fee query) stop at each bucket's safety reserve, while a cancel, a
//! reducing order and the session's own cancel-on-disconnect arm may use it until the bucket
//! is empty; each refusal is counted under the scope whose bucket refused it.
//! Otherwise the handler is told it was sent, with the nonces it used
//! ([`ExecHandler::on_submitted`]), and its effects are executed. A request's deadline runs
//! from just before its frame is written; the first event that answers it
//! ([`ExecEvent::answers`]) clears it, and one still unanswered then is handed to the codec's
//! `on_rpc_timeout` once, which reports it `Unknown`: whether the connection is the one it was
//! written on, a later one, or none while the session waits to reconnect (stamped under the
//! session's current epoch, which is the one it waits to open). A write that fails or stalls
//! after bytes may have left thus reports `Unknown` too. The session never encodes or writes a
//! command twice, and a reconnect never re-sends one (0005, 0013 rule 1). A deadline that falls
//! due while a write waits on a stalled peer is handled once the write ends, within the
//! write-stall window. Once the session has stopped, nothing more is reported.
//!
//! **Cancel-on-disconnect and the resync, every epoch (FBC-w19, decision 0058).** A session
//! takes only a venue whose cancel-on-disconnect is per connection: one declaring `None` or
//! `DeadMan` is refused when it is built ([`ExecSessionError::CancelOnDisconnect`]). Once the
//! codec reports an epoch's stream authenticated, the session sends `ArmCancelOnDisconnect(true)`
//! as a request of its own (its id from the account's [`RpcIds`], one nonce reserved, its frames
//! charged together), on every epoch where the protection lapses with the connection and
//! otherwise until an arm is accepted, then calls the codec's `resync` with exactly the nonces
//! [`ExecCodec::nonces_for`] asks for with [`CtxCall::Resync`], its frames and HTTP reads (each
//! with the connection it opens) charged together as `on_open`'s are (buckets that refuse them
//! for now end the epoch as a drop, opened again no sooner than they would admit them; what
//! never fits ends the session, [`ExecSessionError::ResyncNeverFits`]). The arm's and the
//! resync's events reach the handler as any other. Until the venue has finally accepted the arm
//! (a provisional acceptance leaves it pending, its deadline standing) and the resync's
//! `ResyncEnd` has been handed to the handler, the epoch takes no place, batch of places or
//! amend: each is `NotSent(Disconnected)`, counted ([`ExecCounters::unready_refusals`]), with
//! no nonce reserved and nothing written, while cancels and control commands go out
//! ([`ExecOrders::may_place`], true already as the handler hears the event that opens the
//! epoch; what it submits then goes out once it returns). An arm the codec refuses, the buckets
//! do not admit, the venue rejects, or that is unanswered at its deadline fails the epoch: once
//! the input being handled and the commands waiting are taken (a cancel among them goes out),
//! the epoch ends as a drop, counted ([`ExecCounters::arm_failures`]), and the next opens
//! through the pacing, no sooner than the buckets would admit the arm when they refused it for
//! now; an arm whose frames never fit ends the session ([`ExecSessionError::ArmNeverFits`]).
//!
//! One thread drives a session (design §5.1): [`ExecSession::run`] spawns no task.

use std::fmt;
use std::rc::Rc;

use fbc_core::{
    AccountKey, CancelOnDisconnect, ConnKey, ConnState, CtxCall, Effect, Effects, EncodeCtx,
    EncodeReceipt, Envelope, ExecCodec, ExecEvent, ExecSink, Inbound, InboundSpans, KernelRxNs,
    MonoNs, Namespace, NonceBlock, NonceSource, NotSentReason, PathStamps, RpcCall, RpcId, Secrets,
    SpecTable, Stamp, StreamId, SubmitHandle, TimerTag, TrafficClass, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, VenueMeta, WallNs, dispatch,
};
use fbc_journal::{ControlEvent, NonceSourceId, Record};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::exec_orders::{ExecOrders, Queued, RpcIds, Rpcs, Shared, Submitted};
use crate::journal::Journal;
use crate::pacing::ReconnectPacing;
use crate::ratelimit::{RateError, RateLimiter, Refused, Request};
use crate::session::SessionError;
use crate::session_core::{
    Answered, Control, Core, CoreConfig, EpochInputs, IngestClock, TickToWire, close, frame_of,
    http_of, next_frame, sleep_or_never, with_response,
};
use crate::stall::WriteStall;
use crate::ws::{self, Message, WebSocket};

/// Where the consumer receives an order-entry session's events: called once per event, in
/// ingest order, on the thread that drives the session, before the next frame is read
/// (decision 0053, as 0023 for market data).
pub trait ExecHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>);

    /// Connection epoch `key` ended (a drop, a reconnect the codec asked for, a stalled write,
    /// an error or a stop): called once per epoch, after every event of it the handler is given
    /// and before any of the next (as 0039 for market data). Nothing by default.
    fn on_epoch_end(&mut self, key: ConnKey) {
        let _ = key;
    }

    /// What became of a command submitted through the session's [`ExecOrders`]: called at most
    /// once per submission, in submission order, before any event answering its request
    /// (decision 0057). `handle.receipt` holds the nonces the encode used when it was sent, or
    /// why it was not. A consumer whose OMS awaits a command's outcome implements it: a command
    /// not sent is reported here only.
    ///
    /// Once the session ends (a stop or an error) nothing more is reported: not the commands
    /// still waiting, nor the one whose nonces the source mis-reserved, and a command reported
    /// sent as the control dropped may never have been written. The consumer treats every
    /// submission not reported, and every one reported sent and not answered, as unresolved
    /// until the venue is read again (0013 rule 1). A command submitted from this call waits for
    /// the session's next turn; one not sent is never resubmitted as it was (0013 rule 1).
    /// Nothing by default.
    fn on_submitted(&mut self, handle: SubmitHandle) {
        let _ = handle;
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
    /// The account they reach: [`ExecOrders::submit`] takes only its authorizations.
    pub acct: AccountKey,
    /// Where the session's request ids are given from: the account's one [`RpcIds`], a clone
    /// handed to every session built for it, so ids never repeat for the account (0057).
    pub rpc_ids: RpcIds,
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
    /// The most response-body bytes an HTTP request the codec asks for may read; a longer
    /// body is [`HttpFailure::Lost`](fbc_core::HttpFailure::Lost) (0027).
    pub http_max_body: usize,
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
    /// (decision 0053).
    Endpoints(usize),
    /// The consumer's nonce source reserved another number of nonces than were asked for, for
    /// `on_open`, `on_timer` or an encode.
    Nonces { asked: u16, reserved: usize },
    /// The frames the codec's `on_open` asks for weigh more together than the venue's buckets
    /// ever admit, so no epoch could open.
    OpenNeverFits,
    /// The venue's cancel-on-disconnect is not per connection: it declares none (`None`), or a
    /// dead-man timer (`DeadMan`), which no planned venue needs. A session places orders only
    /// behind per-connection protection (decision 0058).
    CancelOnDisconnect(CancelOnDisconnect),
    /// The frames the codec's resync asks for weigh more together than the venue's buckets ever
    /// admit, so no epoch could take places.
    ResyncNeverFits,
    /// The frames the codec's cancel-on-disconnect arm asks for weigh more than the venue's
    /// buckets ever admit, so no epoch could take places (PR #90 Reviewer B B3).
    ArmNeverFits,
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
            ExecSessionError::CancelOnDisconnect(cod) => {
                let declared = match cod {
                    CancelOnDisconnect::None => "None",
                    CancelOnDisconnect::PerConnection { .. } => "PerConnection",
                    CancelOnDisconnect::DeadMan { .. } => "DeadMan",
                };
                write!(
                    f,
                    "the venue declares cancel-on-disconnect {declared}; a session places orders \
                     only behind per-connection protection"
                )
            }
            ExecSessionError::ResyncNeverFits => f.write_str(
                "the frames the resync asks for weigh more together than the buckets ever admit",
            ),
            ExecSessionError::ArmNeverFits => f.write_str(
                "the frames the cancel-on-disconnect arm asks for weigh more than the buckets ever \
                 admit",
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
    /// Frames and HTTP results the codec could not decode.
    pub decode_errors: u64,
    /// Effects refused as codec defects: a frame or reconnect for another stream.
    pub refused_effects: u64,
    /// Epochs ended because a write did not complete within the write-stall window.
    pub write_stalls: u64,
    /// Epochs ended because their cancel-on-disconnect arm failed: the codec refused it, the
    /// buckets did not admit it, the venue rejected it, or it was unanswered at its deadline.
    pub arm_failures: u64,
    /// Places, batches of places and amends refused because their epoch was not yet armed and
    /// resynced.
    pub unready_refusals: u64,
    /// Inbound frames and HTTP responses whose codec named credential spans that do not fit
    /// them (a codec defect): journaled with the whole body and every header hashed.
    pub refused_redactions: u64,
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
    /// Where the nonces reserved and the contexts given are journaled (FBC-2pr).
    rec: Recorder,
    decode_errors: u64,
    /// Epochs ended by a failed arm.
    arm_failures: u64,
    /// Places and amends refused on an epoch not yet armed and resynced.
    unready_refusals: u64,
    /// What [`ExecOrders`] and the session share: the commands waiting, the authenticated
    /// epoch.
    orders: Rc<Shared>,
    /// The deadlines of the requests written (FBC-0ga).
    rpcs: Rpcs,
    /// The shard clock, for what is stamped while the core waits to connect.
    clock: IngestClock,
    /// The control's drop, as the codec's inputs see it while the core runs.
    stop: watch::Receiver<()>,
    /// Why a timer firing the core took during a write ended the session: its nonce
    /// reservation, which the core cannot return.
    fault: Option<ExecSessionError>,
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
        self.orders.end();
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
    /// Stopped, the epoch already journaled closed, ahead of the inputs that came with the stop
    /// (Codex P1 on PR #115).
    StopClosed,
    Dropped,
}

/// What woke a connected epoch.
enum Wake {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Timer,
    Http(Answered),
    /// A command was submitted.
    Orders,
    /// A request's deadline fell due.
    Rpc,
    Stop,
}

/// The codec's inputs as the core hands them on ([`Feed`]), built from the session's fields
/// other than its core, so the core can run beside them.
macro_rules! feed {
    ($s:ident) => {
        Feed {
            codec: &mut *$s.codec,
            handler: &mut $s.handler,
            nonces: &mut *$s.nonces,
            rec: &$s.rec,
            caps: &$s.caps,
            ns: $s.ns,
            specs: &$s.specs,
            stop: &$s.stop,
            decode_errors: &mut $s.decode_errors,
            fault: &mut $s.fault,
            rpcs: &mut $s.rpcs,
            orders: &$s.orders,
            own: $s.stream,
        }
    };
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
        let Some(exec) = &caps.exec else {
            return Err(ExecSessionError::NoOrderEntry);
        };
        let rearm = match exec.order.cancel_on_disconnect {
            CancelOnDisconnect::PerConnection { rearm_on_reconnect } => rearm_on_reconnect,
            other => return Err(ExecSessionError::CancelOnDisconnect(other)),
        };
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
        let clock = config.clock.clone();
        let mut core = Core::new(CoreConfig {
            own: endpoint.stream,
            connector: config.connector,
            clock: config.clock,
            pacing: config.pacing,
            write_stall: config.write_stall.window(),
            conn: config.conn,
            stop: stop.clone(),
            http_max_body: config.http_max_body,
            rates: config.limiter,
        });
        // What an order-entry stream brings carries acks and fills (decision 0078).
        core.inbound = TrafficClass::Safety;
        let rec = Recorder {
            journal: None,
            source: NonceSourceId(u32::from(config.acct.get())),
        };
        let session = ExecSession {
            core,
            codec,
            stream: endpoint.stream,
            url: endpoint.url.as_str().to_owned(),
            caps,
            ns: config.ns,
            specs: config.specs,
            nonces: config.nonces,
            rec,
            decode_errors: 0,
            arm_failures: 0,
            unready_refusals: 0,
            orders: Shared::new(config.acct, config.rpc_ids, rearm),
            rpcs: Rpcs::default(),
            clock,
            stop,
            fault: None,
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

    /// Records everything the session sends and receives into `journal` from now on, with the
    /// nonces it reserves and the contexts it gives its codec (0006); the module docs say what
    /// is recorded, and under which class.
    pub fn set_journal(&mut self, journal: Journal) {
        self.rec.journal = Some(journal.clone());
        self.core.journal = Some(journal);
    }

    /// What commands reach the session through: authorizations for its account, and control
    /// commands (decision 0057). It takes none once the session has run or dropped.
    pub fn orders(&self) -> ExecOrders {
        ExecOrders {
            shared: Rc::clone(&self.orders),
        }
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
            arm_failures: self.arm_failures,
            unready_refusals: self.unready_refusals,
            refused_redactions: self.core.refused_redactions(),
        }
    }

    /// Connects, reconnects as paced, and delivers events until the [`ExecControl`] is
    /// dropped.
    ///
    /// A session runs once (decision 0053): a run that ended, in an error or not, or whose
    /// future was dropped, leaves nothing to run again, and a later call returns
    /// [`ExecSessionError::Ended`] without connecting (Codex r4189174493, r4189174502). A run
    /// dropped while connected leaves its epoch to be ended there or when the session drops:
    /// the handler is told it ended either way (Codex r4188995359, r4189174470). A run that
    /// ends in an error retires its epoch as a drop (Codex r4188802893).
    ///
    /// HTTP requests still in flight when it ends are dropped (0027).
    pub async fn run(&mut self) -> Result<(), ExecSessionError> {
        self.end_left_epoch();
        if std::mem::replace(&mut self.ran, true) {
            return Err(ExecSessionError::Ended);
        }
        let ran = self.run_epochs().await;
        self.core.http.clear();
        self.orders.end();
        ran
    }

    /// Opens epoch after epoch, as paced, until the session stops or fails.
    async fn run_epochs(&mut self) -> Result<(), ExecSessionError> {
        loop {
            // While it waits to connect, the session still reports each deadline that falls
            // due, and each command submitted, under the epoch it waits to open.
            let mut ctl = Between {
                stop: self.core.stop.clone(),
                orders: &self.orders,
                rpcs: &mut self.rpcs,
                codec: &mut *self.codec,
                handler: &mut self.handler,
                clock: &self.clock,
                key: self.core.current(),
            };
            let Some(ws) = self.core.connect(&self.url, &mut ctl).await? else {
                return Ok(());
            };
            let key = self.current();
            self.in_epoch = Some(key);
            let end = self.connected(ws).await;
            match end {
                // `connected` forgot the epoch's buckets.
                Ok(End::Stop | End::StopClosed) => return Ok(()),
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
            self.core.control(|| ControlEvent::Closed(key));
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
        self.core.control(|| ControlEvent::Opened(key));
        let end = self.epoch(ws, key).await;
        self.orders.set_ready(None);
        // Cleared before the handler is called, so a handler that panics is never told twice
        // (Codex r4189618551).
        self.in_epoch = None;
        self.core.rates.closed(key);
        let end = match end {
            Ok(End::StopClosed) => Ok(End::Stop),
            end => {
                self.core.control(|| ControlEvent::Closed(key));
                end
            }
        };
        self.handler.on_epoch_end(key);
        end
    }

    /// The epoch `key`, opened on `ws`.
    async fn epoch(&mut self, ws: WebSocket, key: ConnKey) -> Result<End, ExecSessionError> {
        let mut ws = Some(ws);
        // Whether the epoch was journaled closed as the control dropped.
        let mut closed = false;
        // A control that dropped as the connection opened stops the session before the codec
        // is told of it, so nothing is reserved or sent.
        let mut open = !self.stopped() && {
            let fx = self.open()?;
            let never = ExecSessionError::OpenNeverFits;
            self.execute_together(&mut ws, key, fx, never).await?
        };
        while open {
            // Unbiased: each turn polls the branches from a random one, so frames that keep
            // arriving starve no timer, result or stop, and the stop is checked after every wake
            // below whichever branch won (DeepSeek DS-1).
            let rpc_due = self.rpcs.next_deadline();
            let wake = tokio::select! {
                frame = next_frame(&mut ws) => Wake::Frame(frame),
                _ = sleep_or_never(self.core.next_deadline()) => Wake::Timer,
                Some(done) = self.core.http.next() => Wake::Http(done),
                _ = self.orders.wake.notified() => Wake::Orders,
                _ = sleep_or_never(rpc_due) => Wake::Rpc,
                _ = self.core.stop.changed() => Wake::Stop,
            };
            // The kernel receive time of the last packet read beneath the frame, if any.
            let rx = ws.as_ref().and_then(|ws| ws.get_ref().kernel_rx());
            open = match wake {
                // What woke as the control dropped is stamped, so it keeps its place in ingest
                // order, but reaches no codec; so does a frame waiting then. The epoch is
                // journaled closed first, so replay, which feeds a closed epoch nothing, feeds
                // them to no codec either, as on a market-data session (Codex P1 on PR #115).
                _ if self.stopped() => {
                    self.core.control(|| ControlEvent::Closed(key));
                    closed = true;
                    self.take_stopped(&mut ws, key, rx, wake)?;
                    false
                }
                Wake::Frame(Some(Ok(message))) => {
                    let mut fx = Effects::new();
                    let origin = self.decode(key, rx, &message, &mut fx);
                    self.execute(&mut ws, fx, false, origin).await?
                }
                Wake::Frame(_) | Wake::Stop => false,
                Wake::Timer => self.fire(&mut ws).await?,
                Wake::Http(done) => self.answer(&mut ws, done).await?,
                Wake::Orders => true,
                Wake::Rpc => {
                    self.time_out(key);
                    true
                }
            };
            // An epoch just authenticated arms its protection and resyncs, before what waits.
            if open {
                open = self.prepare(&mut ws, key).await?;
            }
            // What was submitted, by the handler as it took the input or meanwhile, goes out
            // before the next input.
            if open {
                open = self.send_queued(&mut ws, key).await?;
            }
            // An epoch whose arm failed ends as a drop, once what waited has gone out.
            if open && self.orders.gate.borrow().failed(key.epoch) {
                self.arm_failures += 1;
                open = false;
            }
        }
        // A stop closes the connection; a drop, a reconnect the codec asked for (which the
        // core closed) or a failed write leaves it to be dropped.
        if !self.stopped() {
            return Ok(End::Dropped);
        }
        if let Some(socket) = ws.as_mut() {
            close(socket, &self.core.rates, key);
        }
        Ok(if closed { End::StopClosed } else { End::Stop })
    }

    /// Calls the codec's `on_open` for the session's stream with exactly the nonces it asks
    /// for; its effects.
    fn open(&mut self) -> Result<Effects, ExecSessionError> {
        let call = CtxCall::Open(self.stream);
        let (mono, wall) = self.core.clock.now();
        let ctx = context(&*self.codec, &mut *self.nonces, &self.rec, call, mono, wall)?;
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
            rpcs: &mut self.rpcs,
            orders: &self.orders,
            own: self.stream,
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

    /// Executes what `on_open` or the codec's resync asked for on epoch `key`. Its frames and
    /// HTTP requests, each request with the connection it opens, are charged together first, so
    /// all of them go or none does (Codex r4188802873; PR #90 Reviewer B B7): buckets that
    /// refuse them for now end the epoch as a drop, which reconnects through the pacing, no
    /// sooner than the buckets would admit them, and calls `on_open` (and the resync) again,
    /// rather than leave the codec believing it sent what it never did, or waiting on a read
    /// that never went; what can never fit together ends the session with `never`. A request
    /// that then does not start (its timeout past the end of the clock, or one the runtime
    /// cannot make) stays charged. False when the epoch ended.
    async fn execute_together(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
        mut fx: Effects,
        never: ExecSessionError,
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
        let requests = |e: &Effect| {
            own(e)
                .map(|f| vec![f])
                .or_else(|| http_of(e).map(Vec::from))
        };
        let all: Vec<Request> = effects.iter().filter_map(requests).flatten().collect();
        // A stop that came while `on_open` ran charges nothing (Codex r4189174483).
        let now = Instant::now();
        let charged = (!self.stopped()).then(|| self.core.rates.charge(now, key, &all));
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
            Some(Err(Refused { ready_at: None })) => return Err(never),
        }
        effects.into_iter().for_each(|e| fx.push(e));
        let open = self
            .core
            .execute_all_charged(ws, &mut feed!(self), fx)
            .await?;
        self.faulted()?;
        Ok(open)
    }

    /// Executes `fx` in order ([`Core::execute`]), as one batch, attributed to the input
    /// stamped `origin`, its frames already charged when `charged` says so. A stop, from the
    /// handler or from another thread, ends the epoch before the next effect (Codex
    /// r4188802881), so a timer or request behind a reconnect or a failed write, which ends the
    /// epoch first, is never reached (Codex r4188639448). A timer firing or an HTTP result the
    /// core takes while a write waits reaches the codec through [`Feed`], and what it asks for
    /// goes behind the rest of `fx`, so a reconnect still in `fx` ends the epoch before it is
    /// reached (Reviewer B, B1); one whose nonces the source mis-reserved halts the batch and
    /// ends the session. False when the epoch ended.
    async fn execute(
        &mut self,
        ws: &mut Option<WebSocket>,
        fx: Effects,
        charged: bool,
        origin: Option<Stamp>,
    ) -> Result<bool, ExecSessionError> {
        let open = self
            .core
            .execute(ws, &mut feed!(self), fx, charged, origin)
            .await?;
        self.faulted()?;
        Ok(open)
    }

    /// Fires the earliest timer, which is due: an ended epoch's into nothing (dropped and
    /// counted), the current epoch's into the codec's `on_timer`, and its effects executed.
    /// False when the epoch ended.
    async fn fire(&mut self, ws: &mut Option<WebSocket>) -> Result<bool, ExecSessionError> {
        let Some((stamp, tag)) = self.core.take_timer()? else {
            return Ok(true);
        };
        let mut fx = Effects::new();
        feed!(self).ring(&mut self.core.epochs, stamp, tag, &mut fx);
        self.faulted()?;
        self.execute(ws, fx, false, Some(stamp)).await
    }

    /// Takes an HTTP result as it comes back: stamped, handed to the codec's `on_http` when the
    /// epoch that asked is current, dropped and counted when it ended, and its effects executed.
    /// False when the epoch ended.
    async fn answer(
        &mut self,
        ws: &mut Option<WebSocket>,
        done: Answered,
    ) -> Result<bool, ExecSessionError> {
        let codec = &self.codec;
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        let stamped = self.core.stamp_http(Some(&redact), done);
        let Some((stamp, done)) = self.core.admit_http(stamped)? else {
            return Ok(true);
        };
        let mut fx = Effects::new();
        feed!(self).answer(&mut self.core.epochs, stamp, done, &mut fx);
        self.execute(ws, fx, false, Some(stamp)).await
    }

    /// Stamps what woke the epoch `key` as the control dropped, and a frame waiting then, so
    /// each keeps its place in ingest order; none reaches the codec.
    fn take_stopped(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
        rx: Option<KernelRxNs>,
        wake: Wake,
    ) -> Result<(), ExecSessionError> {
        let codec = &self.codec;
        let redact = |input: Inbound<'_>| codec.redact_inbound(input);
        let waiting = match wake {
            Wake::Frame(frame) => Some(frame),
            Wake::Timer => {
                let _ = self.core.take_timer()?;
                next_frame(ws).now_or_never()
            }
            Wake::Http(done) => {
                let _ = self.core.stamp_http(Some(&redact), done);
                next_frame(ws).now_or_never()
            }
            Wake::Orders | Wake::Rpc | Wake::Stop => next_frame(ws).now_or_never(),
        };
        if let Some(Some(Ok(message))) = waiting {
            let _ = self.core.take_in(&redact, key, rx, &message);
        }
        Ok(())
    }

    /// The session's error when a timer firing the core took ended it.
    fn faulted(&mut self) -> Result<(), ExecSessionError> {
        self.fault.take().map_or(Ok(()), Err)
    }

    /// Takes the commands waiting as the turn began, in submission order, on epoch `key`
    /// ([`Self::send`]), until the epoch ends or the session stops; what still waits then,
    /// including what the handler submitted meanwhile, is the next turn's, and the session
    /// yields first, so a handler that submits again from `on_submitted` cannot hold it (PR #87
    /// Reviewer B B5). False when the epoch ended.
    async fn send_queued(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let mut open = true;
        let mut turn = self.orders.waiting_now();
        while open
            && !self.stopped()
            && let Some(queued) = turn.next().and_then(|()| self.orders.pop())
        {
            open = self.send(ws, key, queued).await?;
        }
        if open && self.orders.waiting() {
            tokio::task::yield_now().await;
        }
        Ok(open)
    }

    /// Encodes and writes one submitted command on epoch `key`, telling the handler what became
    /// of it (the module docs say how). False when the epoch ended.
    async fn send(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
        queued: Queued,
    ) -> Result<bool, ExecSessionError> {
        let Queued { rpc, item, epoch } = queued;
        let not_sent = |reason| SubmitHandle {
            rpc,
            receipt: Err(reason),
        };
        let cmd = item.command();
        let current = self.orders.ready() == Some(key.epoch) && epoch == Some(key.epoch);
        // No place or amend before the epoch's arm is accepted and its resync has ended
        // (decision 0058): counted, no nonce reserved, nothing written.
        let held = current && !self.orders.gate.borrow().admits(cmd, key.epoch);
        self.unready_refusals += u64::from(held);
        if !current || held {
            self.handler
                .on_submitted(not_sent(NotSentReason::Disconnected));
            return Ok(true);
        }
        // fbc-oms's re-check of an authorization (decision 0060), here at encode, before any
        // nonce is reserved, not where `ExecOrders::submit` queued it: the kill switch, a
        // disarm or another change of the market's state may have come since (decisions 0057,
        // 0062; PR #87 Reviewer B B7). A refusal is `NotSent(StaleAuthorization)`, nothing
        // reserved or written. The authorization is spent when this returns.
        if let Submitted::Authorized(auth) = &item
            && auth.check_at_submit().is_err()
        {
            self.handler
                .on_submitted(not_sent(NotSentReason::StaleAuthorization));
            return Ok(true);
        }
        match self.encode(cmd, budget_class(&item), rpc, key)? {
            Ok((receipt, fx)) => {
                let receipt = Ok(receipt);
                self.handler.on_submitted(SubmitHandle { rpc, receipt });
                self.execute(ws, fx, true, None).await
            }
            Err(unsent) => {
                self.handler.on_submitted(not_sent(unsent.reason()));
                Ok(true)
            }
        }
    }

    /// Encodes `cmd` as request `rpc` for epoch `key`, with an [`EncodeCtx`] holding exactly
    /// its items' nonces, and charges its frames together as `class` (decision 0073): its
    /// receipt and effects, ready to
    /// execute, or why it is not sent, nothing written (the module docs say which). The
    /// session's error when the nonce source reserved another count.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        class: TrafficClass,
        rpc: RpcId,
        key: ConnKey,
    ) -> Result<Result<(EncodeReceipt, Effects), Unsent>, ExecSessionError> {
        // Its nonces and context are journaled under the command's own class, so a cancel's
        // and a reducing order's are Safety (decision 0078).
        let journaled = cmd.traffic_class();
        // A batch longer than u16::MAX items, which no venue takes, has no nonce block. Its time
        // is read once its nonces are reserved, so a source that takes its time leaves the
        // encode no stale time (Codex P2 on PR #115).
        let (rec, clock) = (&self.rec, &self.core.clock);
        let items = cmd
            .items()
            .map(|n| reserve(&mut *self.nonces, n, rec, journaled, || clock.now()));
        let mut fx = Effects::new();
        let encoded = items
            .transpose()?
            .ok_or(NotSentReason::Unencodable)
            .and_then(|(nonces, (mono, wall))| {
                let ctx = EncodeCtx { wall, mono, nonces };
                self.rec.ctx(journaled, Some(rpc), &ctx);
                let mut t = PathStamps::off();
                self.codec
                    .encode(cmd, rpc, &self.specs, &ctx, &mut t, &mut fx)
            });
        let receipt = match encoded {
            Ok(receipt) if carries(&fx, rpc, cmd.traffic_class(), self.stream, Instant::now()) => {
                receipt
            }
            Ok(_) => return Ok(Err(Unsent::Codec(NotSentReason::Unencodable))),
            Err(reason) => return Ok(Err(Unsent::Codec(reason))),
        };
        // Its frames go together or not at all, so the codec's request is either written whole
        // or reported not sent, charged as `class`.
        let own = |e: &Effect| frame_of(e, self.stream, true).map(|r| Request { class, ..r });
        let frames: Vec<_> = fx.as_slice().iter().filter_map(own).collect();
        if let Err(refused) = self.core.rates.charge(Instant::now(), key, &frames) {
            return Ok(Err(Unsent::Budget(refused)));
        }
        Ok(Ok((receipt, fx)))
    }

    /// Arms the venue's cancel-on-disconnect on just authenticated epoch `key`, then asks the
    /// codec's resync, as each is due (decision 0058). False when the epoch ended.
    async fn prepare(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let epoch = key.epoch;
        if self.stopped() || self.orders.ready() != Some(epoch) {
            return Ok(true);
        }
        let mut open = true;
        if self.orders.gate.borrow().arm_due(epoch) {
            open = self.arm(ws, key).await?;
        }
        let resync = {
            let gate = self.orders.gate.borrow();
            gate.resync_due(epoch) && !gate.failed(epoch)
        };
        if open && resync {
            open = self.resync(ws, key).await?;
        }
        Ok(open)
    }

    /// Sends `ArmCancelOnDisconnect(true)` on epoch `key` as a request of the session's own; one
    /// not sent fails the epoch. Buckets that refuse it for now hold the next attempt until they
    /// would admit it, as for `on_open` and the resync; frames that can never fit end the
    /// session with [`ExecSessionError::ArmNeverFits`] (PR #90 Reviewer B B3). False when the
    /// epoch ended.
    async fn arm(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let rpc = self.orders.next_rpc();
        let cmd = VenueCommand::ArmCancelOnDisconnect(true);
        // The session's own arm, one per epoch, may use the safety reserve (decision 0073).
        match self.encode(&cmd, cmd.traffic_class(), rpc, key)? {
            Ok((_, fx)) => {
                self.orders.gate.borrow_mut().arm_sent(key.epoch, rpc);
                self.execute(ws, fx, true, None).await
            }
            Err(Unsent::Budget(Refused { ready_at: None })) => Err(ExecSessionError::ArmNeverFits),
            Err(unsent) => {
                if let Unsent::Budget(Refused { ready_at: Some(at) }) = unsent {
                    self.core.pacer.hold_until(at);
                }
                self.orders.gate.borrow_mut().arm_not_sent(key.epoch);
                Ok(true)
            }
        }
    }

    /// Asks the codec's resync on epoch `key`, with exactly the nonces it asks for, its frames
    /// charged together. False when the epoch ended.
    async fn resync(
        &mut self,
        ws: &mut Option<WebSocket>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let (mono, wall) = self.core.clock.now();
        let call = CtxCall::Resync;
        let ctx = context(&*self.codec, &mut *self.nonces, &self.rec, call, mono, wall)?;
        let mut fx = Effects::new();
        self.codec.resync(&ctx, &mut fx);
        self.orders.gate.borrow_mut().resync_asked(key.epoch);
        let never = ExecSessionError::ResyncNeverFits;
        self.execute_together(ws, key, fx, never).await
    }

    /// Hands each request whose deadline fell due unanswered to the codec's `on_rpc_timeout`,
    /// its events stamped under epoch `key`; once the session has stopped, the sink drops them.
    fn time_out(&mut self, key: ConnKey) {
        for rpc in self.rpcs.take_due(Instant::now()) {
            // An arm unanswered at its deadline fails its epoch, whatever the codec reports.
            self.orders.gate.borrow_mut().timed_out(rpc);
            let stamp = self.core.clock.stamp(key, None);
            let mut sink = Sink {
                handler: &mut self.handler,
                epochs: &mut self.core.epochs,
                stop: &self.core.stop,
                stamp,
                rpcs: &mut self.rpcs,
                orders: &self.orders,
                own: self.stream,
            };
            self.codec.on_rpc_timeout(rpc, &mut sink);
        }
    }
}

/// Where an order-entry session journals the nonces it reserves and the contexts it gives its
/// codec (FBC-2pr, decision 0078): nothing until the consumer sets a journal. The session's
/// nonce source is journaled as the source numbered by its account.
struct Recorder {
    journal: Option<Journal>,
    source: NonceSourceId,
}

impl Recorder {
    /// One `Nonce` record per value of `block`, in the order reserved, under `class`.
    fn nonces(&self, class: TrafficClass, wall: WallNs, block: &NonceBlock) {
        if let Some(journal) = &self.journal {
            for &value in block.as_slice() {
                let source = self.source;
                journal.record(class, wall, &Record::Nonce { source, value });
            }
        }
    }

    /// The context `ctx` given to a call, under `class`; `rpc` names an encode's request.
    fn ctx(&self, class: TrafficClass, rpc: Option<RpcId>, ctx: &EncodeCtx) {
        if let Some(journal) = &self.journal {
            let ctx = ctx.clone();
            journal.record(class, ctx.wall, &Record::EncodeCtx { rpc, ctx });
        }
    }
}

/// Why a request was not sent, nothing of it written: the codec's reason, or the buckets'
/// refusal of its frames.
enum Unsent {
    Codec(NotSentReason),
    Budget(Refused),
}

impl Unsent {
    /// The reason the submitter is told.
    fn reason(&self) -> NotSentReason {
        match self {
            Unsent::Codec(reason) => *reason,
            Unsent::Budget(_) => NotSentReason::RateBudget,
        }
    }
}

/// The traffic class a submitted command's frames are charged as (decision 0073): an
/// authorized order command's own ([`VenueCommand::traffic_class`]), so cancels and reducing
/// orders may use the safety reserve; every control command normal traffic, whatever its
/// label. The consumer submits control commands without authorization or count (the Unknown
/// ladder's order queries, an arm, a dead-man refresh, the fee query), so they stop at each
/// bucket's safety floor and never drain what cancels and reducing orders need (design §4.10
/// step 7; Reviewer B RB-e8i-1 on PR #113). The session's own arm, one per epoch, is charged
/// its label in `arm`.
fn budget_class(item: &Submitted) -> TrafficClass {
    match item {
        Submitted::Authorized(auth) => auth.command().traffic_class(),
        Submitted::Control(_) => TrafficClass::Normal,
    }
}

/// The context for the codec's `call` at `mono` and `wall`: exactly the nonces it asks for,
/// reserved from `nonces` (none when it asks for none), or why they could not be (0014 item 1).
/// The nonces and the context are journaled through `rec` under Normal (decision 0078).
fn context(
    codec: &dyn ExecCodec,
    nonces: &mut dyn NonceSource,
    rec: &Recorder,
    call: CtxCall,
    mono: MonoNs,
    wall: WallNs,
) -> Result<EncodeCtx, ExecSessionError> {
    let class = TrafficClass::Normal;
    let (nonces, _) = reserve(nonces, codec.nonces_for(call), rec, class, || (mono, wall))?;
    let ctx = EncodeCtx { wall, mono, nonces };
    rec.ctx(class, None, &ctx);
    Ok(ctx)
}

/// Exactly `asked` nonces from `nonces` (none reserved when `asked` is 0) with the time `now`
/// gives once they are reserved, or why not. What the source reserved is journaled through
/// `rec` under `class`, filed under that time, even when it is not what was asked for: those
/// values are spent all the same (0006).
fn reserve(
    nonces: &mut dyn NonceSource,
    asked: u16,
    rec: &Recorder,
    class: TrafficClass,
    now: impl FnOnce() -> (MonoNs, WallNs),
) -> Result<(NonceBlock, (MonoNs, WallNs)), ExecSessionError> {
    let block = match asked {
        0 => NonceBlock::EMPTY,
        n => nonces.reserve(n),
    };
    let at = now();
    rec.nonces(class, at.1, &block);
    if block.len() != usize::from(asked) {
        let reserved = block.len();
        return Err(ExecSessionError::Nonces { asked, reserved });
    }
    Ok((block, at))
}

/// Whether `fx`, an encode's effects for request `rpc` of traffic class `class`, may be executed:
/// they carry the request ([`Effects::carry_request`]), every frame goes to the session's own
/// stream `own`, and none asks to reconnect, which would leave a frame of the request unwritten
/// with its outcome unreported (0014 item 3), or is an HTTP request: order entry is
/// WebSocket-only (0057), since an HTTP request gets no deadline here and its result is dropped
/// once its epoch ends, so it could never come back `Unknown`. Nor does a frame name a request
/// whose deadline, its timeout from `now`, is past the end of the clock: that request would
/// never come back `Unknown` either (PR #87 Reviewer B B9).
fn carries(
    fx: &Effects,
    rpc: RpcId,
    class: fbc_core::TrafficClass,
    own: StreamId,
    now: Instant,
) -> bool {
    let elsewhere = |effect: &Effect| match effect {
        Effect::Send { stream, rpc, .. } => {
            *stream != own || rpc.is_some_and(|call| now.checked_add(call.timeout).is_none())
        }
        other => matches!(other, Effect::Reconnect { .. } | Effect::Http { .. }),
    };
    !fx.as_slice().iter().any(elsewhere) && fx.carry_request(rpc, class)
}

/// The order-entry session as the core waits to connect: the control's drop, and what still
/// reaches the handler meanwhile (FBC-0ga). A command submitted is `NotSent(Disconnected)`, and
/// a request whose deadline falls due is handed to the codec's `on_rpc_timeout`, its events
/// stamped under `key`, the epoch the session waits to open; once the control has dropped,
/// neither.
struct Between<'a, H> {
    stop: watch::Receiver<()>,
    orders: &'a Shared,
    rpcs: &'a mut Rpcs,
    codec: &'a mut dyn ExecCodec,
    handler: &'a mut H,
    clock: &'a IngestClock,
    key: ConnKey,
}

impl<H> Between<'_, H> {
    fn stopped(&self) -> bool {
        self.stop.has_changed().is_err()
    }
}

impl<H: ExecHandler> Control for Between<'_, H> {
    /// True when there is something to report (at once when it already waits), false once the
    /// control dropped.
    async fn changed(&mut self) -> bool {
        let (due, waiting) = (self.rpcs.next_deadline(), self.orders.waiting());
        // What waits is taken after a yield, so a handler that submits again from
        // `on_submitted` cannot hold the task (PR #87 Reviewer B B5).
        tokio::select! {
            biased;
            r = self.stop.changed() => r.is_ok(),
            () = tokio::task::yield_now(), if waiting => true,
            _ = self.orders.wake.notified(), if !waiting => true,
            _ = sleep_or_never(due) => true,
        }
    }

    fn apply(&mut self) {
        // Only what waited as the turn began: what the handler submits meanwhile is the next
        // turn's.
        let mut turn = self.orders.waiting_now();
        while !self.stopped()
            && let Some(queued) = turn.next().and_then(|()| self.orders.pop())
        {
            let receipt = Err(NotSentReason::Disconnected);
            let rpc = queued.rpc;
            self.handler.on_submitted(SubmitHandle { rpc, receipt });
        }
        // Once the control has dropped, the sink hands the handler nothing.
        for rpc in self.rpcs.take_due(Instant::now()) {
            let stamp = self.clock.stamp(self.key, None);
            let mut sink = Late {
                handler: &mut *self.handler,
                stop: &self.stop,
                stamp,
            };
            self.codec.on_rpc_timeout(rpc, &mut sink);
        }
    }
}

/// Hands the events of a deadline that fell due while the session waits to connect to the
/// handler, stamped `stamp`, until the control drops.
struct Late<'a, H> {
    handler: &'a mut H,
    stop: &'a watch::Receiver<()>,
    stamp: Stamp,
}

impl<H: ExecHandler> ExecSink for Late<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        if self.stop.has_changed().is_ok() {
            self.handler.on_exec(Envelope::new(self.stamp, meta, ev));
        }
    }
}

/// What the core hands a current epoch's timer firings and HTTP results to: the session's one
/// codec, inside the venue's decode scope for a result, and its handler. Once the control has
/// dropped, or a timer's nonces were mis-reserved, nothing reaches the codec. Tick-to-wire on
/// order entry is FBC-qfm's.
struct Feed<'a, H> {
    codec: &'a mut dyn ExecCodec,
    handler: &'a mut H,
    nonces: &'a mut dyn NonceSource,
    rec: &'a Recorder,
    caps: &'a VenueCaps,
    ns: Namespace,
    specs: &'a SpecTable,
    stop: &'a watch::Receiver<()>,
    decode_errors: &'a mut u64,
    fault: &'a mut Option<ExecSessionError>,
    rpcs: &'a mut Rpcs,
    orders: &'a Shared,
    own: StreamId,
}

impl<H> Feed<'_, H> {
    /// Whether an input may still reach the codec.
    fn live(&self) -> bool {
        self.stop.has_changed().is_ok() && self.fault.is_none()
    }
}

impl<H: ExecHandler> EpochInputs for Feed<'_, H> {
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.codec.redact_inbound(input)
    }

    /// Calls `on_timer` with exactly the nonces it asks for and the firing's time; a source
    /// that reserves another count calls nothing and ends the session.
    fn ring(&mut self, _: &mut Epochs, stamp: Stamp, tag: TimerTag, fx: &mut Effects) {
        if !self.live() {
            return;
        }
        let call = CtxCall::Timer(tag);
        let (mono, wall) = (stamp.recv_mono, stamp.recv_wall);
        match context(&*self.codec, &mut *self.nonces, self.rec, call, mono, wall) {
            Ok(ctx) => self.codec.on_timer(tag, &ctx, fx),
            Err(e) => *self.fault = Some(e),
        }
    }

    fn answer(&mut self, epochs: &mut Epochs, stamp: Stamp, done: Answered, fx: &mut Effects) {
        if !self.live() {
            return;
        }
        let mut sink = Sink {
            handler: &mut *self.handler,
            epochs,
            stop: self.stop,
            stamp,
            rpcs: &mut *self.rpcs,
            orders: self.orders,
            own: self.own,
        };
        let (codec, specs) = (&mut *self.codec, self.specs);
        let decoded = dispatch(self.caps, self.ns, |scope| {
            with_response(&done.result, |resp| {
                codec.on_http(done.tag, resp, scope, specs, &mut sink, fx)
            })
        });
        if decoded.is_err() {
            *self.decode_errors += 1;
        }
    }

    fn on_tick_to_wire(&mut self, _: TickToWire) {}

    fn sent_rpc(&mut self, call: RpcCall) {
        self.rpcs.sent(call, Instant::now());
    }

    /// A timer's mis-reserved nonces end the session before another effect is executed.
    fn halted(&self) -> bool {
        self.fault.is_some()
    }
}

/// Stamps each pushed event with its input's stamp and hands it to the handler at once (0014
/// item 2); an event of an ended epoch, one pushed after the control dropped included, is
/// dropped and counted. An event that answers a request clears its deadline (FBC-0ga), and the
/// codec's report of the session's own stream says whether its epoch is authenticated.
struct Sink<'a, H> {
    handler: &'a mut H,
    epochs: &'a mut Epochs,
    stop: &'a watch::Receiver<()>,
    stamp: Stamp,
    rpcs: &'a mut Rpcs,
    orders: &'a Shared,
    own: StreamId,
}

impl<H: ExecHandler> ExecSink for Sink<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        // A provisional acceptance of the arm answers nothing yet: the venue may still reject
        // it, so its deadline stands (PR #90 Reviewer A A2).
        if let Some(rpc) = ev.answers()
            && !self.orders.gate.borrow().provisional_arm(&ev)
        {
            self.rpcs.answered(rpc);
        }
        if self.stop.has_changed().is_err() {
            self.epochs.drop_ended(Input::Event);
        } else if let Ok(Admit::Current) = self.epochs.admit(Input::Event, self.stamp.conn) {
            let epoch = self.stamp.conn.epoch;
            if let ExecEvent::Conn { stream, state } = &ev
                && *stream == self.own
            {
                let authenticated = *state == ConnState::Authenticated;
                self.orders.set_ready(authenticated.then_some(epoch));
                if authenticated {
                    self.orders.gate.borrow_mut().authenticated(epoch);
                }
            }
            // The arm's answer and the resync's end count as the handler is handed the event,
            // so the handler hearing the one that opens the epoch sees places taken (PR #90
            // Reviewer B B1). What it submits then still goes out only once it has returned,
            // so after fbc-oms has applied the ResyncEnd.
            let settled = self.orders.gate.borrow().settles(epoch, &ev);
            if let Some(settled) = settled {
                self.orders.gate.borrow_mut().settle(epoch, settled);
            }
            self.handler.on_exec(Envelope::new(self.stamp, meta, ev));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::ConfigError;

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
            (
                ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::None),
                "the venue declares cancel-on-disconnect None; a session places orders only \
                 behind per-connection protection",
            ),
            (
                ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::PerConnection {
                    rearm_on_reconnect: false,
                }),
                "the venue declares cancel-on-disconnect PerConnection; a session places orders \
                 only behind per-connection protection",
            ),
            (
                ExecSessionError::CancelOnDisconnect(CancelOnDisconnect::DeadMan {
                    max_ttl: std::time::Duration::from_secs(1),
                }),
                "the venue declares cancel-on-disconnect DeadMan; a session places orders only \
                 behind per-connection protection",
            ),
            (
                ExecSessionError::ResyncNeverFits,
                "the frames the resync asks for weigh more together than the buckets ever admit",
            ),
            (
                ExecSessionError::ArmNeverFits,
                "the frames the cancel-on-disconnect arm asks for weigh more than the buckets ever \
                 admit",
            ),
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
}
