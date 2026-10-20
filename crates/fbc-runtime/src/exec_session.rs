//! An order-entry session (FBC-oaz, decision 0053): drives one account's order-entry
//! connections, as [`VenueFactory::plan_exec`] plans them, through connection epochs on the
//! shared session core (FBC-e73), with one [`ExecCodec`] for the session's life (decisions 0002,
//! 0014, 0015, 0019, 0023). The first planned connection carries the orders; what the module
//! docs say of "the connection" is that one's, and the others' are driven beside it (FBC-tnsf,
//! decision 0115; see the last section).
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
//! ([`ExecSession::set_journal`]), the session records what crosses its boundary as a market-data
//! session does: each epoch's opening and closing, every frame and HTTP result it receives at its
//! stamp (an ended epoch's hashed whole, since the codec is never handed it) with the
//! credentials the codec names in it as keyed hashes (0028), every ping, pong and close frame it
//! receives (0041), every frame it writes with its kind and redaction spans and the result of the
//! write (none for a write that failed, stalled or was interrupted, whose outcome is unknown,
//! 0036), every HTTP request with its request id, and every codec timer that fires. It also records
//! each nonce it reserves from the consumer's [`NonceSource`], one `Nonce` record per value under
//! the number the consumer gives that source ([`ExecSessionConfig::nonce_source`]), as it is
//! reserved (a short reservation's values included), first, when the journal is set, the one
//! the source names as its high-water ([`NonceSource::high_water`]: the nonce a resumed source
//! resumed after, so a restart stays above it though this process reserves none; RB187-1 on
//! PR #187), and the [`EncodeCtx`] it then hands `on_open`,
//! `on_timer`, the resync or an `encode`, the encode's with its request id, just before the call.
//! An inbound frame or HTTP result is journaled under Safety, since an order-entry stream carries
//! acks and fills that cannot be told apart before they are decoded, and so are each epoch's
//! opening and closing, which replay places them in, and every nonce, so a restart reads every
//! value spent; a write under its frame's class; an encode's context under its command's class,
//! so a cancel's or a reducing order's is Safety; the rest under Normal. An input that came as the
//! control dropped follows the epoch's `Closed`, reaching no codec; spans a codec names that do not
//! fit their input are counted ([`ExecCounters::refused_redactions`]) and that input is hashed
//! whole. The time of an encode, `on_open` or the resync is read once its nonces are reserved; a
//! timer's is its firing's. Each request deadline the session hands `on_rpc_timeout` is journaled
//! under Safety as an `RpcTimeout` record at the stamp its events carry, just before the call,
//! connected or waiting to reconnect, so no ingest sequence the session gives goes unjournaled
//! (FBC-0hfl, decision 0090). Nothing waits on the journal: a record the sink has no room for is
//! dropped and counted there (0006).
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
//! An order query or the fee query the consumer submits may instead be carried as exactly one
//! HTTP request and no frame (FBC-m8vm, decision 0081); every other command over HTTP, an
//! order-affecting one until FBC-4nfb, stays `NotSent(Unencodable)`, nothing requested.
//! The frames, or the HTTP request with the connection it opens, are charged together in the
//! class of their command (FBC-e8i, decision 0073), so a refused HTTP query is
//! `NotSent(RateBudget)` with nothing requested: a normal
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
//! write-stall window. Once the session has stopped, nothing more is reported. A command
//! submitted while a handler is handed an input's events, on this session or another sharing
//! its [`IngestClock`], is attributed to that input when it is written (decision 0100), and
//! once a Safety one is written, a cancel or a reducing order, its tick-to-wire from that
//! input's kernel receive time, when it has one, is handed to
//! [`ExecHandler::on_tick_to_wire`] (FBC-qfm, as decision 0031 for market data); the
//! session holds a submission, for [`ShardHost`](crate::ShardHost), from its submission until
//! the turn that takes it has written it.
//!
//! An HTTP query's deadline runs from just before its request starts, and is cleared by the
//! call to the codec's `on_http` for it, whatever the codec pushes, as well as by an event
//! answering it. While the epoch that asked lasts its result always comes back (the request's
//! own timeout bounds it), so its deadline stands only once that epoch ends: then its result is
//! dropped, and the query is handed to `on_rpc_timeout` once at its deadline, on a later epoch
//! or while the session waits to reconnect, and never requested again (decision 0081).
//!
//! **Bounds with a Safety reserve (FBC-hwn, design §5.4, decision 0105).** The commands waiting
//! for the session's turn are bounded by the consumer's [`ExecSessionConfig::outbound`]: Normal
//! traffic (a place, an amend, every control command) is taken while fewer than its Normal
//! bound wait, Safety traffic (a cancel, a cancel-many, an instrument cancel-all, a reducing
//! order) while fewer than the whole bound do. One submitted past that is refused at once,
//! [`SubmitRefusal::Backpressure`](crate::SubmitRefusal::Backpressure) (not sent,
//! `NotSent(Backpressure)`), naming the request id it was given, nothing of it kept (an
//! authorization is dropped, the consumer settling it as a not-sent receipt for that id and
//! planning again; Codex P1 r4312949568 on PR #174), the refusal journaled as a `WriteResult`
//! under its class and that id on the epoch the session is in or waits to open, and counted
//! ([`ExecCounters::backpressure_refusals`]). So a peer that stops reading holds a write, and
//! the handlers of the shard's other members submitting meanwhile, to the bound, and nothing
//! grows past it (Codex P1 r4312640254 on PR #174). The
//! requests in flight (written or asked and neither answered nor timed out) are counted in the
//! consumer's [`InFlight`] table ([`ExecSessionConfig::in_flight`]), shared by the sessions it
//! is handed to: a submitted command its class finds no room for is `NotSent(RateBudget)` at
//! its encode, after the authorization's re-check and before a nonce is reserved, nothing
//! written, counted ([`ExecCounters::in_flight_refusals`]). A place is given back when its
//! request is answered, handed to `on_http` or timed out, when an encoded request is not
//! written, and for every request still in flight once the run ends or its future is dropped.
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
//! **Orders resting from an earlier epoch (FBC-nvxn, decision 0080).** An arm may cover only
//! the orders placed after it. On a venue that does not declare that it covers the orders
//! already open (`CancelOnDisconnect::PerConnection::covers_open_orders`), every order of ours
//! the resync of an epoch that sent an arm shows resting was placed on an earlier epoch and is
//! unprotected: once the epoch takes places, the handler is told them, once
//! ([`ExecHandler::on_unprotected`]), and cancels them through fbc-oms, and until an order event
//! or an order query's answer shows each ended, a place, a batch with an item, or an amend on
//! its market is `NotSent(Disconnected)`, counted ([`ExecCounters::unprotected_refusals`]), with
//! no nonce reserved and nothing written ([`ExecOrders::may_place_on`]). Other markets place.
//!
//! **An in-process stream (FBC-6mf, decision 0104).** [`ExecSession::in_process`] builds the
//! same session over an [`InProcessStream`] with a codec of the consumer's: each epoch opens the
//! stream in place of the planned URL, writes go to its peer, and the peer's answers are read
//! at their due instants as frames off a socket are; everything above is unchanged.
//!
//! **Several connections (FBC-tnsf, decision 0115).** A venue whose order entry and account
//! events arrive on separate connections (Hibachi's trade and account sockets) plans more than
//! one. The first planned endpoint is the order connection: commands, the cancel-on-disconnect
//! arm and the resync go on it, and only its epochs authenticate. Endpoint `i` of the plan is
//! stamped connection `conn + i` ([`ExecSessionConfig::conn`]), each with epochs, reconnect pacing,
//! timers and HTTP requests of its own (two endpoints on one stream are refused,
//! [`ExecSessionError::SharedStream`]), and the one codec serves them all: each epoch of each
//! endpoint calls `on_open` with its stream and exactly the nonces
//! [`ExecCodec::nonces_for`] asks for, charged together as above; each frame is decoded with the
//! stream it came on; a timer or HTTP request belongs to the epoch of the endpoint whose input
//! asked for it. A frame or reconnect the codec asks for on another endpoint's stream is written
//! on, or ends the epoch of, that endpoint, charged there as its turn comes, once the effects
//! for the endpoint that asked have run; while that endpoint has no open epoch it is refused and
//! counted. An endpoint that drops, asks to reconnect, or fails a write reconnects through its
//! own pacing while every other endpoint's epoch stays open, and the order connection's other
//! endpoints keep being read while it waits to reconnect. Every event is stamped from the one
//! [`IngestClock`], so the endpoints' events share one ingest order. An event of another
//! endpoint's input counts for the order connection's authenticated epoch, if one is, as the
//! answer to its arm, the end of its resync or what it shows of the orders held unprotected, but
//! authenticates nothing; an outage of another endpoint holds no place yet (FBC-rta5 decides
//! that per connection). The session's one thread reads no other endpoint while one write waits
//! on a peer, within the write-stall window. A stop closes every endpoint's epoch, a frame
//! already waiting on one stamped behind its close (once stopped, no other endpoint is read; one
//! that woke as the control dropped from another thread is not stamped); an error, a dropped run
//! or the session's drop ends each as it does the order connection's. An in-process stream
//! stands in for exactly one connection.
//!
//! One thread drives a session (design §5.1): [`ExecSession::run`] spawns no task.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use fbc_core::{
    AccountKey, CancelOnDisconnect, ConnKey, ConnState, CtxCall, Effect, Effects, EncodeCtx,
    EncodeReceipt, Envelope, ExecCodec, ExecEvent, ExecSink, Inbound, InboundSpans, KernelRxNs,
    MonoNs, Namespace, NonceBlock, NonceRefused, NonceSource, NotSentReason, PathStamps, RpcCall,
    RpcId, Secrets, SpecTable, Stamp, StreamId, SubmitHandle, TimerTag, TrafficClass, VenueCaps,
    VenueCommand, VenueConfig, VenueError, VenueFactory, VenueMeta, VenueOrderSnapshot, WallNs,
    dispatch,
};
use fbc_journal::{ControlEvent, NonceSourceId, Record};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::watch;
use tokio::time::Instant;

mod legs;

use legs::{Leg, LegWake, Legs, Primary};

use crate::auth::HandshakeHeaders;
use crate::bounds::{InFlight, ReservedBound};
use crate::connector::Connector;
use crate::epoch::{Admit, EpochError, Epochs, Input};
use crate::exec_gate::Hold;
use crate::exec_orders::{ExecOrders, Queued, RpcIds, Rpcs, Shared, Submitted};
use crate::in_process::{InProcessStream, Link};
use crate::journal::Journal;
use crate::pacing::ReconnectPacing;
use crate::ratelimit::{RateError, RateLimiter, Refused, Request};
use crate::session::SessionError;
use crate::session_core::{
    Answered, Control, Core, CoreConfig, EpochInputs, IngestClock, TickToWire, close, frame_of,
    http_of, next_frame, sleep_or_never, with_response,
};
use crate::session_core::{Dial, Wire};
use crate::stall::WriteStall;
use crate::ws::{self, Message};

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

    /// The orders of ours this or an earlier epoch's resync showed resting, placed on an earlier
    /// epoch, which the epoch's cancel-on-disconnect arm may not cover (FBC-nvxn, decision
    /// 0080): called once per epoch, right after the handler is handed the event that lets the
    /// epoch place (the arm's acceptance or the resync's end, whichever comes last), when there
    /// are any, on a venue that does not declare that an arm covers the orders already open
    /// (`covers_open_orders`). Until an order event or an order query's answer shows each
    /// ended, the session takes no place or amend on its market
    /// ([`ExecOrders::may_place_on`]). The consumer cancels them, a Safety cancel per order
    /// built and authorized by fbc-oms ([`ExecOrders::submit`]); one whose registry already
    /// holds the order ended (a snapshot lagging an end heard on an earlier epoch) queries it
    /// instead (`ControlCommand::Query`), and the venue's answer showing it ended releases the
    /// market. One it neither cancels nor queries keeps its market held, and is told again on
    /// each later epoch. Nothing by default.
    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        let _ = orders;
    }

    /// A frame or HTTP answer of epoch `key` that the codec could not decode, as it is read and
    /// before the next input (also counted, `ExecCounters::decode_errors`): it may have held a
    /// fill, a position or an order event the consumer never sees, so a consumer that trades on
    /// what it heard stops here rather than once the session ended (FBC-elg7). Nothing by
    /// default.
    fn on_decode_error(&mut self, key: ConnKey) {
        let _ = key;
    }

    /// The tick-to-wire of a Safety-class write on the session's stream (design §5.3, decisions
    /// 0031, 0100; FBC-qfm): called once its write is done, when the input it is attributed to
    /// is a frame with a kernel receive time. A command is attributed to the input whose events
    /// a handler was being handed when it was submitted, on this session or another sharing its
    /// [`IngestClock`], so a protective cancel the market-data handler submits on a frame is
    /// reported here against that frame's `kernel_rx`; the codec's own Safety frames are
    /// attributed to the input they answer. Nothing is reported where the input has no
    /// `kernel_rx` (off Linux, a timer firing, an HTTP result, a consumer input, a command
    /// submitted outside any handler), for a Normal write or for one that failed or stalled.
    /// The runtime keeps no samples: the consumer aggregates them for its latency gates.
    /// Nothing by default.
    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        let _ = sample;
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
    /// The number the journal names `nonces` by: one per scope the venue's
    /// [`NonceScope`](fbc_core::NonceScope) names (an account, a signing key), the same for every
    /// session that shares that source's sequence, so a restart reads every value it spent
    /// (Codex P2 r4214053442 on PR #115).
    pub nonce_source: NonceSourceId,
    /// The connection number stamped on the inputs of the session's first planned endpoint, the
    /// one that carries its orders; endpoint `i` of the plan is stamped `conn + i` (FBC-tnsf,
    /// decision 0115). Every one is unique on its shard.
    pub conn: u16,
    /// The buckets of the venue's declared limits, shared with every session that counts
    /// against the same ones; built for exactly the venue's limits.
    pub limiter: RateLimiter,
    /// The longest one write may wait on a peer that stopped reading (0036).
    pub write_stall: WriteStall,
    /// The most response-body bytes an HTTP request the codec asks for may read; a longer
    /// body is [`HttpFailure::Lost`](fbc_core::HttpFailure::Lost) (0027).
    pub http_max_body: usize,
    /// The bound on the commands waiting for the connection (design §5.4, decision 0105): a
    /// place, an amend or a control command submitted while the Normal bound's worth wait is
    /// refused at once, `SubmitRefusal::Backpressure` (not sent, `NotSent(Backpressure)`), as is
    /// a cancel or a reducing order submitted while the reserve is full too, nothing of it kept
    /// or written and the refusal journaled as its write result.
    pub outbound: ReservedBound,
    /// The table of requests in flight the session counts against (decision 0105): one shared
    /// by every session of the venue, or the session's own. A submission its class finds no
    /// room for is `NotSent(RateBudget)`, nothing written.
    pub in_flight: InFlight,
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
    /// The venue planned this many order-entry connections: none, which leaves no connection to
    /// carry orders, or more than one for a session over an in-process stream, which stands in
    /// for exactly one (decisions 0053, 0104, 0115).
    Endpoints(usize),
    /// The venue planned two order-entry connections on this stream, so a frame or reconnect
    /// naming it could not be routed to one (decision 0115).
    SharedStream(StreamId),
    /// The consumer's nonce source reserved another number of nonces than were asked for, for
    /// `on_open`, `on_timer` or an encode.
    Nonces { asked: u16, reserved: usize },
    /// The consumer's nonce source refused a reservation for `on_open`, `on_timer`, the
    /// resync or an encode, reserving none (FBC-3bq4): a wall-clock source whose clock fell
    /// too far behind its last nonce (decision 0114). Nothing of the call was made or written.
    NonceRefused(NonceRefused),
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
    /// The venue named a credential header on the planned endpoint of `stream` that no opening
    /// handshake can carry: not a header name, one the handshake sets itself (`Host`,
    /// `Connection`, `Upgrade`, a `Sec-WebSocket-` header, or one that frames a body), or one
    /// it names twice on that endpoint (decision 0116). A venue defect: nothing is opened.
    HandshakeHeader {
        stream: StreamId,
        name: &'static str,
    },
    /// The planned endpoint of `stream` names a credential header but its URL is plain `ws://`
    /// to a host other than this machine's loopback (`127.0.0.0/8`, `::1`, `localhost`), so the
    /// key would cross the network in cleartext (decision 0116). Nothing is opened; the URL is
    /// not shown.
    CleartextHandshake { stream: StreamId },
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
                "the venue planned {n} order-entry connections; a session drives at least one, \
                 and exactly one over an in-process stream"
            ),
            ExecSessionError::SharedStream(stream) => write!(
                f,
                "the venue planned two order-entry connections on stream {}",
                stream.0
            ),
            ExecSessionError::Nonces { asked, reserved } => write!(
                f,
                "the nonce source reserved {reserved} nonces when {asked} were asked for"
            ),
            ExecSessionError::NonceRefused(why) => {
                write!(f, "the nonce source refused a reservation: {why}")
            }
            ExecSessionError::HandshakeHeader { stream, name } => write!(
                f,
                "the venue named header {name} for the opening handshake on stream {}, which \
                 the handshake cannot carry",
                stream.0
            ),
            ExecSessionError::CleartextHandshake { stream } => write!(
                f,
                "the planned endpoint on stream {} carries a credential header over plain ws:// \
                 to a host that is not loopback; give a wss:// URL",
                stream.0
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
    /// Places, batches of places and amends refused on an armed and resynced epoch because an
    /// order of ours on their market rests unprotected from an earlier epoch (FBC-nvxn).
    pub unprotected_refusals: u64,
    /// Inbound frames and HTTP responses whose codec named credential spans that do not fit
    /// them (a codec defect): journaled with the whole body and every header hashed.
    pub refused_redactions: u64,
    /// Submissions refused `NotSent(Backpressure)` because the commands waiting for the
    /// connection filled the bound their class may use ([`ExecSessionConfig::outbound`]).
    pub backpressure_refusals: u64,
    /// Submissions refused `NotSent(RateBudget)` because the requests in flight filled the
    /// bound their class may use ([`ExecSessionConfig::in_flight`]).
    pub in_flight_refusals: u64,
}

/// One account's order-entry connection, driven by [`ExecSession::run`].
pub struct ExecSession<H: ExecHandler> {
    /// The epochs, effects, writes and pacing (FBC-e73).
    core: Core,
    /// The one codec of the session's life.
    codec: Box<dyn ExecCodec>,
    stream: StreamId,
    url: String,
    /// The order connection's credential headers (decision 0116).
    headers: HandshakeHeaders,
    /// The in-process stream the session opens in place of `url` (decision 0104).
    in_process: Option<InProcessStream>,
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
    /// Places and amends refused on a market an order rests unprotected on.
    unprotected_refusals: u64,
    /// Submissions refused for want of room in the table of requests in flight.
    in_flight_refusals: u64,
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
    /// The epoch journaled closed as the control dropped, ahead of the inputs that came with
    /// the stop: neither the run's end nor a drop journals it closed again, even if stamping
    /// those inputs fails or panics (Codex P2 r4214243083 on PR #115).
    closed_early: Option<ConnKey>,
    /// Whether [`ExecSession::run`] was called.
    ran: bool,
    /// Which way an epoch's next turn polls first, of [`WAKES`] (one more with legs).
    turn: u8,
    /// The connection number of the order connection, whose events alone authenticate its
    /// epochs (decisions 0058, 0115).
    conn: u16,
    /// The endpoints planned after the first (FBC-tnsf, decision 0115).
    legs: Legs,
    handler: H,
}

/// A session dropped while a dropped run left its epoch connected tells the handler it ended
/// (Codex r4189174470), unless it is dropped as a panic unwinds: its buckets are forgotten and
/// the epoch is journaled closed if the journal is not borrowed then (Codex P2 r4213946637 on
/// PR #115), but the handler, which may have panicked itself, is not called, since a second
/// panic would abort the process (Reviewer B, B2).
impl<H: ExecHandler> Drop for ExecSession<H> {
    fn drop(&mut self) {
        self.orders.end();
        if std::thread::panicking() {
            if let Some(key) = self.in_epoch.take() {
                self.core.rates.closed(key);
                if !self.was_closed_early(key) {
                    self.core.closed_unwinding(key);
                }
            }
            self.legs.left(None);
        } else {
            self.end_left_epoch();
        }
    }
}

/// Where a session's order entry goes: the venue's planned WebSocket, or an in-process stream
/// with a codec of the consumer's (decision 0104).
enum Entry {
    Socket,
    InProcess(Box<dyn ExecCodec>, InProcessStream),
}

/// How each connection attempt opens the session's connection: its in-process stream when it
/// has one, which opens at once, or the WebSocket at its URL.
struct ExecDial<'a> {
    url: &'a str,
    /// The credential headers the upgrade carries (decision 0116).
    headers: &'a HandshakeHeaders,
    in_process: Option<&'a InProcessStream>,
    clock: &'a IngestClock,
}

impl Dial for ExecDial<'_> {
    type Wire = Link;

    async fn dial(&mut self, connector: &Connector) -> Option<Link> {
        match self.in_process {
            Some(stream) => Some(Link::open(stream, self.clock)),
            None => {
                let ws = connector.websocket_with(self.url, self.headers).await;
                ws.ok().map(Link::Socket)
            }
        }
    }
}

/// How a connected epoch ended.
enum End {
    Stop,
    Dropped,
}

/// What woke a connected epoch.
/// How many ways an epoch's turn can wake: [`Wake`]'s variants.
const WAKES: u8 = 6;

enum Wake {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Timer,
    Http(Answered),
    /// A command was submitted.
    Orders,
    /// A request's deadline fell due.
    Rpc,
    Stop,
    /// One of the other endpoints woke (FBC-tnsf).
    Leg(usize, LegWake),
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
            clock: &$s.clock,
            caps: &$s.caps,
            ns: $s.ns,
            specs: &$s.specs,
            stop: &$s.stop,
            decode_errors: &mut $s.decode_errors,
            fault: &mut $s.fault,
            rpcs: &mut $s.rpcs,
            orders: &$s.orders,
            own: $s.stream,
            conn: $s.conn,
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
        ExecSession::build(config, Entry::Socket, handler)
    }

    /// A session over the in-process stream `stream` in place of the venue's planned
    /// order-entry connection (FBC-6mf, decision 0104), with `codec` as its one codec in place
    /// of the venue's: a simulated venue's, for one that stands in for it. Everything else is
    /// as [`ExecSession::new`] builds it, from the venue's caps and plan: the planned endpoint
    /// names the stream the session's frames go on, which `codec` must write on, but its URL is
    /// never opened, and `config.creds` are dropped unread. An HTTP request the codec asks for
    /// still goes through `config.connector`. Refused when the venue takes no orders or plans
    /// other than one order-entry connection.
    pub fn in_process(
        config: ExecSessionConfig,
        codec: Box<dyn ExecCodec>,
        stream: InProcessStream,
        handler: H,
    ) -> Result<(ExecSession<H>, ExecControl), ExecSessionError> {
        ExecSession::build(config, Entry::InProcess(codec, stream), handler)
    }

    fn build(
        config: ExecSessionConfig,
        entry: Entry,
        handler: H,
    ) -> Result<(ExecSession<H>, ExecControl), ExecSessionError> {
        let Parts {
            core,
            codec,
            stream,
            url,
            headers,
            in_process,
            caps,
            ns,
            specs,
            nonces,
            rec,
            orders,
            rpcs,
            clock,
            stop,
            conn,
            legs,
            control,
        } = parts(config, entry)?;
        let session = ExecSession {
            core,
            codec,
            stream,
            url,
            headers,
            in_process,
            caps,
            ns,
            specs,
            nonces,
            rec,
            decode_errors: 0,
            arm_failures: 0,
            unready_refusals: 0,
            unprotected_refusals: 0,
            in_flight_refusals: 0,
            orders,
            rpcs,
            clock,
            stop,
            fault: None,
            in_epoch: None,
            closed_early: None,
            ran: false,
            turn: 0,
            conn,
            legs,
            handler,
        };
        session.orders.set_conn(session.core.current());
        Ok((session, control))
    }

    /// The current connection epoch of the order connection, the first planned endpoint.
    pub fn current(&self) -> ConnKey {
        self.core.current()
    }

    /// The epoch the planned endpoint of stream `stream` is in, or waits to open; `None` for a
    /// stream the venue planned no order-entry connection on (FBC-tnsf).
    pub fn current_on(&self, stream: StreamId) -> Option<ConnKey> {
        if stream == self.stream {
            Some(self.core.current())
        } else {
            self.legs.current_on(stream)
        }
    }

    /// Records everything the session sends and receives into `journal` from now on, with the
    /// nonces it reserves and the contexts it gives its codec (0006); the module docs say what
    /// is recorded, and under which class. An epoch a dropped run left connected is ended first,
    /// the handler told, so its `Closed` goes to the journal that holds its `Opened` (Codex P2
    /// r4213946641 on PR #115).
    pub fn set_journal(&mut self, journal: Journal) {
        self.end_left_epoch();
        self.rec.journal = Some(journal.clone());
        // The nonce the source resumed after, or last reserved, first, under Safety: a journal
        // then names it for a restart though this process reserves none (RB187-1 on PR #187).
        if let Some(value) = self.nonces.high_water() {
            let block = NonceBlock::new(vec![value]);
            self.rec.nonces(self.clock.now().1, &block);
        }
        // A submission refused for want of room is journaled where it is refused (0105).
        self.orders.set_journal(journal.clone());
        self.legs.set_journal(&journal);
        self.core.journal = Some(journal);
    }

    /// What commands reach the session through: authorizations for its account, and control
    /// commands (decision 0057). It takes none once the session has run or dropped.
    pub fn orders(&self) -> ExecOrders {
        ExecOrders {
            shared: Rc::clone(&self.orders),
        }
    }

    /// Inputs of an ended epoch dropped, by kind, on every planned endpoint.
    pub fn stale(&self, input: Input) -> u64 {
        let legs = self.legs.iter().map(|leg| leg.core.epochs.stale(input));
        self.core.epochs.stale(input) + legs.sum::<u64>()
    }

    /// What the session counted, on every planned endpoint together.
    pub fn counters(&self) -> ExecCounters {
        let mut core = self.core.counters;
        let mut refused_redactions = self.core.refused_redactions();
        for leg in self.legs.iter() {
            let c = &leg.core.counters;
            core.attempts += c.attempts;
            core.failed_attempts += c.failed_attempts;
            core.refused_effects += c.refused_effects;
            core.write_stalls += c.write_stalls;
            refused_redactions += leg.core.refused_redactions();
        }
        ExecCounters {
            attempts: core.attempts,
            failed_attempts: core.failed_attempts,
            decode_errors: self.decode_errors,
            refused_effects: core.refused_effects,
            write_stalls: core.write_stalls,
            arm_failures: self.arm_failures,
            unready_refusals: self.unready_refusals,
            unprotected_refusals: self.unprotected_refusals,
            refused_redactions,
            backpressure_refusals: self.orders.backpressured(),
            in_flight_refusals: self.in_flight_refusals,
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
        let ran = {
            // Dropped with the run's future too: a run dropped mid-epoch gives back the places
            // its requests hold in the consumer's table, which the session, kept, never reports
            // again (decision 0105; Codex P1 r4312949559 on PR #174).
            let _give_back = self.rpcs.give_back_on_drop();
            let ran = self.run_epochs().await;
            // The other endpoints' epochs end with the run: closed on a stop, retired on an
            // error, the handler told (decision 0115).
            ran.and(self.legs.finish(&mut feed!(self)))
        };
        self.core.http.clear();
        self.orders.end();
        // Nothing more is reported: the requests still in flight give back their places in the
        // consumer's table (decision 0105).
        self.rpcs.clear();
        ran
    }

    /// Opens epoch after epoch, as paced, until the session stops or fails, every other
    /// endpoint connected beside the order connection as its own pacing allows.
    async fn run_epochs(&mut self) -> Result<(), ExecSessionError> {
        self.legs.start();
        loop {
            // A submission refused for want of room is journaled under the epoch the session is
            // in, or waits to open (0105).
            self.orders.set_conn(self.core.current());
            // While it waits to connect, the session still reports each deadline that falls
            // due, and each command submitted, under the epoch it waits to open.
            let mut ctl = Between {
                stop: self.core.stop.clone(),
                orders: &self.orders,
                rpcs: &mut self.rpcs,
                codec: &mut *self.codec,
                handler: &mut self.handler,
                clock: &self.clock,
                rec: &self.rec,
                key: self.core.current(),
                legs: &mut self.legs,
                nonces: &mut *self.nonces,
                caps: &self.caps,
                ns: self.ns,
                specs: &self.specs,
                decode_errors: &mut self.decode_errors,
                fault: &mut self.fault,
                own: self.stream,
                pending: None,
                after_leg: false,
            };
            let mut dial = ExecDial {
                url: &self.url,
                headers: &self.headers,
                in_process: self.in_process.as_ref(),
                clock: &self.clock,
            };
            let Some(ws) = self.core.connect_with(&mut dial, &mut ctl).await? else {
                // A leg's input taken meanwhile may have ended the session (decision 0115).
                return self.faulted();
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

    /// Ends the epoch a dropped run left connected, if one did: its orders ended, since the
    /// session runs once and can never send them (Codex P2 r4214607587 on PR #115), its
    /// per-connection buckets forgotten (Codex r4189428438) and the handler told.
    fn end_left_epoch(&mut self) {
        if let Some(key) = self.in_epoch.take() {
            self.orders.end();
            self.core.rates.closed(key);
            if !self.was_closed_early(key) {
                self.core.control(|| ControlEvent::Closed(key));
            }
            self.handler.on_epoch_end(key);
        }
        self.legs.left(Some(&mut self.handler));
    }

    /// Whether epoch `key` was journaled closed as the control dropped; forgets it either way.
    fn was_closed_early(&mut self, key: ConnKey) -> bool {
        self.closed_early.take() == Some(key)
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
    async fn connected(&mut self, ws: Link) -> Result<End, ExecSessionError> {
        let key = self.current();
        self.core.control(|| ControlEvent::Opened(key));
        let end = self.epoch(ws, key).await;
        self.orders.set_ready(None);
        // What the epoch asked over HTTP comes back to no codec now (decision 0081).
        self.rpcs.ended(key);
        // Cleared before the handler is called, so a handler that panics is never told twice
        // (Codex r4189618551).
        self.in_epoch = None;
        self.core.rates.closed(key);
        if !self.was_closed_early(key) {
            self.core.control(|| ControlEvent::Closed(key));
        }
        // An epoch that dropped is followed by the one the session waits to open: what the
        // handler submits as it is told of the end, and is refused, is journaled under that
        // one, not the epoch journaled closed (0105; Codex P2 r4312949575 on PR #174).
        if matches!(end, Ok(End::Dropped))
            && let Some(epoch) = key.epoch.checked_add(1)
        {
            self.orders.set_conn(ConnKey { epoch, ..key });
        }
        self.handler.on_epoch_end(key);
        end
    }

    /// The epoch `key`, opened on `ws`.
    async fn epoch(&mut self, ws: Link, key: ConnKey) -> Result<End, ExecSessionError> {
        let mut ws = Some(ws);
        // A control that dropped as the connection opened stops the session before the codec
        // is told of it, so nothing is reserved or sent.
        let mut open = !self.stopped() && {
            let fx = self.open()?;
            let never = ExecSessionError::OpenNeverFits;
            self.execute_together(&mut ws, key, fx, never).await?
        };
        while open {
            // Fair and deterministic: each turn polls the ways it can wake from the one after
            // the last turn's first, so frames that keep arriving starve no timer, result or
            // stop (DeepSeek DS-1), and the stop is checked after every wake below whichever
            // won. A rotation rather than tokio's random start, so two closed-loop replays of
            // one journal take what is ready on one tick in the same order (decision 0107 item
            // 6; Reviewer B's RB-066n-1 on PR #178).
            let rpc_due = self.rpcs.next_deadline();
            // The other endpoints wake the turn too, when there are any (decision 0115).
            let wakes = WAKES + u8::from(!self.legs.is_empty());
            let first = self.turn % wakes;
            self.turn = (first + 1) % wakes;
            // Once stopped, no other endpoint is read: what waits on it is stamped as the run
            // ends (decision 0115).
            let stopped = self.core.stop.clone();
            let wake = {
                let timer_at = self.core.next_deadline();
                let mut frame = pin!(next_frame(&mut ws));
                let mut timer = pin!(sleep_or_never(timer_at));
                let mut orders = pin!(self.orders.wake.notified());
                let mut rpc = pin!(sleep_or_never(rpc_due));
                let mut stop = pin!(self.core.stop.changed());
                let mut legs = pin!(self.legs.next());
                let http = &mut self.core.http;
                // No result waits: none can be added while the turn waits.
                let mut no_http = false;
                poll_fn(|cx| {
                    for n in 0..wakes {
                        let wake = match (first + n) % wakes {
                            0 => frame.as_mut().poll(cx).map(Wake::Frame),
                            1 => timer.as_mut().poll(cx).map(|()| Wake::Timer),
                            2 if !no_http => match http.poll_next_unpin(cx) {
                                Poll::Ready(Some(done)) => Poll::Ready(Wake::Http(done)),
                                Poll::Ready(None) => {
                                    no_http = true;
                                    Poll::Pending
                                }
                                Poll::Pending => Poll::Pending,
                            },
                            3 => orders.as_mut().poll(cx).map(|()| Wake::Orders),
                            4 => rpc.as_mut().poll(cx).map(|()| Wake::Rpc),
                            5 => stop.as_mut().poll(cx).map(|_| Wake::Stop),
                            6 if stopped.has_changed().is_err() => Poll::Pending,
                            6 => legs.as_mut().poll(cx).map(|(i, w)| Wake::Leg(i, w)),
                            _ => Poll::Pending,
                        };
                        if wake.is_ready() {
                            return wake;
                        }
                    }
                    Poll::Pending
                })
                .await
            };
            // The kernel receive time of the last packet read beneath the frame, if any.
            let rx = ws.as_ref().and_then(Wire::kernel_rx);
            open = match wake {
                // What woke as the control dropped is stamped, so it keeps its place in ingest
                // order, but reaches no codec; so does a frame waiting then. The epoch is
                // journaled closed first, so replay, which feeds a closed epoch nothing, feeds
                // them to no codec either, as on a market-data session (Codex P1 on PR #115).
                // That is noted before they are stamped, so a codec panicking as it redacts
                // them, or an error stamping them, leaves the epoch closed once (Codex P2
                // r4214243083 on PR #115).
                _ if self.stopped() => {
                    self.core.control(|| ControlEvent::Closed(key));
                    self.closed_early = Some(key);
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
                Wake::Leg(i, wake) => self.take_leg(&mut ws, i, wake).await?,
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
        Ok(End::Stop)
    }

    /// Calls the codec's `on_open` for the session's stream with exactly the nonces it asks
    /// for; its effects.
    fn open(&mut self) -> Result<Effects, ExecSessionError> {
        let call = CtxCall::Open(self.stream);
        let (rec, clock) = (&self.rec, &self.clock);
        let ctx = context(&*self.codec, &mut *self.nonces, rec, clock, call, None)?;
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
            conn: self.conn,
        };
        let (codec, stream, specs) = (&mut self.codec, self.stream, &self.specs);
        let decoded = dispatch(&self.caps, self.ns, |scope| {
            codec.on_frame(stream, raw, scope, specs, &mut sink, fx)
        });
        if decoded.is_err() {
            self.decode_errors += 1;
            self.handler.on_decode_error(stamp.conn);
        }
        Some(stamp)
    }

    /// Executes what `on_open` or the codec's resync asked for on the order connection's epoch
    /// `key`, its own frames and HTTP requests charged together ([`together`]), then what it
    /// asked for on another endpoint's stream there ([`Self::route`]). False when the epoch
    /// ended.
    async fn execute_together(
        &mut self,
        ws: &mut Option<Link>,
        key: ConnKey,
        fx: Effects,
        never: ExecSessionError,
    ) -> Result<bool, ExecSessionError> {
        let open = together(&mut self.core, &mut feed!(self), ws, key, fx, never).await?;
        self.route(ws, open).await
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
        ws: &mut Option<Link>,
        fx: Effects,
        charged: bool,
        origin: Option<Stamp>,
    ) -> Result<bool, ExecSessionError> {
        let open = self
            .core
            .execute(ws, &mut feed!(self), fx, charged, origin)
            .await?;
        self.faulted()?;
        self.route(ws, open).await
    }

    /// Executes an encode's effects `fx`, every frame and HTTP request in them, with the
    /// connection each request opens, charged already ([`Self::encode`]), attributed to the
    /// input stamped `origin`: the one being handled on the shard when the command was
    /// submitted, if one was (decision 0100). False when the epoch ended.
    async fn execute_charged(
        &mut self,
        ws: &mut Option<Link>,
        fx: Effects,
        origin: Option<Stamp>,
    ) -> Result<bool, ExecSessionError> {
        let open = self
            .core
            .execute_all_charged(ws, &mut feed!(self), fx, origin)
            .await?;
        self.faulted()?;
        self.route(ws, open).await
    }

    /// Takes what woke the other endpoint `i` while the order connection's epoch is open on `ws`
    /// ([`legs::take`]); whether that epoch is still open.
    async fn take_leg(
        &mut self,
        ws: &mut Option<Link>,
        i: usize,
        wake: LegWake,
    ) -> Result<bool, ExecSessionError> {
        let primary = Primary {
            core: &mut self.core,
            ws,
            open: true,
        };
        legs::take(&mut feed!(self), Some(primary), &mut self.legs, i, wake).await
    }

    /// Executes what the order connection's core handed back for another endpoint's stream on
    /// that endpoint ([`legs::route`]), once its own effects ran (decision 0115); whether the
    /// order connection's epoch is still open, `open` as it was.
    async fn route(&mut self, ws: &mut Option<Link>, open: bool) -> Result<bool, ExecSessionError> {
        let primary = Primary {
            core: &mut self.core,
            ws,
            open,
        };
        legs::route(&mut feed!(self), Some(primary), &mut self.legs).await
    }

    /// Fires the earliest timer, which is due: an ended epoch's into nothing (dropped and
    /// counted), the current epoch's into the codec's `on_timer`, and its effects executed.
    /// False when the epoch ended.
    async fn fire(&mut self, ws: &mut Option<Link>) -> Result<bool, ExecSessionError> {
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
        ws: &mut Option<Link>,
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
        ws: &mut Option<Link>,
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
            // Another endpoint is not read once stopped; one that woke as the control dropped
            // from another thread is not stamped, and the run's end stamps what waits on it
            // (decision 0115).
            Wake::Orders | Wake::Rpc | Wake::Stop | Wake::Leg(..) => next_frame(ws).now_or_never(),
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
        ws: &mut Option<Link>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let mut open = true;
        let mut turn = self.orders.waiting_now();
        // Held until the turn's last command is written, so the shard host submits its next
        // ladder command only once this one is out (decision 0100).
        self.orders.set_sending(true);
        let mut sent = Ok(true);
        while open
            && !self.stopped()
            && let Some(queued) = turn.next().and_then(|()| self.orders.pop())
        {
            sent = self.send(ws, key, queued).await;
            open = *sent.as_ref().unwrap_or(&false);
        }
        self.orders.set_sending(false);
        sent?;
        if open && self.orders.waiting() {
            tokio::task::yield_now().await;
        }
        Ok(open)
    }

    /// Encodes and writes one submitted command on epoch `key`, telling the handler what became
    /// of it (the module docs say how). False when the epoch ended.
    async fn send(
        &mut self,
        ws: &mut Option<Link>,
        key: ConnKey,
        queued: Queued,
    ) -> Result<bool, ExecSessionError> {
        let Queued {
            rpc,
            item,
            epoch,
            origin,
        } = queued;
        let not_sent = |reason| SubmitHandle {
            rpc,
            receipt: Err(reason),
        };
        let cmd = item.command();
        let current = self.orders.ready() == Some(key.epoch) && epoch == Some(key.epoch);
        // No place or amend before the epoch's arm is accepted and its resync has ended
        // (decision 0058), nor on a market an order of ours rests unprotected on from an
        // earlier epoch (FBC-nvxn, decision 0080): counted, no nonce reserved, nothing written.
        let held = if current {
            self.orders.gate.borrow().admits(cmd, key.epoch).err()
        } else {
            None
        };
        match held {
            Some(Hold::Unready) => self.unready_refusals += 1,
            Some(Hold::Unprotected) => self.unprotected_refusals += 1,
            None => {}
        }
        if !current || held.is_some() {
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
        // A place in the consumer's table of requests in flight, before any nonce is reserved:
        // Normal traffic stops at its Normal bound, a cancel or a reducing order at the whole
        // (decision 0105). A refusal is `NotSent(RateBudget)`, nothing reserved or written.
        let class = item.class();
        if !self.rpcs.admit(rpc, class) {
            self.in_flight_refusals += 1;
            self.handler
                .on_submitted(not_sent(NotSentReason::RateBudget));
            return Ok(true);
        }
        let encoded = self.encode(cmd, class, over_http(&item), rpc, key);
        let encoded = match encoded {
            Ok(encoded) => encoded,
            // The session ends with nothing of it encoded or reported (0057); the shard host
            // takes back its own ladder command (decision 0100; Codex P1 r4309350872 on PR #164).
            Err(e) => {
                self.rpcs.unsent(rpc);
                let queued = Queued {
                    rpc,
                    item,
                    epoch,
                    origin,
                };
                self.orders.unsent(queued);
                return Err(e);
            }
        };
        match encoded {
            Ok((receipt, fx)) => {
                let receipt = Ok(receipt);
                self.handler.on_submitted(SubmitHandle { rpc, receipt });
                let open = self.execute_charged(ws, fx, origin).await;
                // A request the epoch ended before writing gives its place back.
                self.rpcs.unsent(rpc);
                open
            }
            Err(unsent) => {
                self.rpcs.unsent(rpc);
                self.handler.on_submitted(not_sent(unsent.reason()));
                Ok(true)
            }
        }
    }

    /// Encodes `cmd` as request `rpc` for epoch `key`, with an [`EncodeCtx`] holding exactly
    /// its items' nonces, carried as one HTTP request only when `http` allows it (decision
    /// 0081), and charges its frames, or its HTTP request with the connection it opens,
    /// together as `class` (decisions 0073, 0081): its receipt and effects, ready to execute,
    /// every frame and request in them charged, or why it is not sent, nothing written or
    /// requested (the module docs say which). The session's error when the nonce source
    /// reserved another count.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        class: TrafficClass,
        http: bool,
        rpc: RpcId,
        key: ConnKey,
    ) -> Result<Result<(EncodeReceipt, Effects), Unsent>, ExecSessionError> {
        // Its context is journaled under the command's own class, so a cancel's and a reducing
        // order's is Safety; its nonces under Safety (decision 0078).
        let journaled = cmd.traffic_class();
        // A batch longer than u16::MAX items, which no venue takes, has no nonce block. Its time
        // is read once its nonces are reserved, so a source that takes its time leaves the
        // encode no stale time (Codex P2 on PR #115).
        let (rec, clock) = (&self.rec, &self.core.clock);
        let items = cmd
            .items()
            .map(|n| reserve(&mut *self.nonces, n, rec, || clock.now()));
        let mut fx = Effects::new();
        let encoded = items
            .transpose()?
            .ok_or(NotSentReason::Unencodable)
            .and_then(|(nonces, (mono, wall))| {
                let ctx = EncodeCtx { wall, mono, nonces };
                self.rec.ctx(journaled, wall, Some(rpc), &ctx);
                let mut t = PathStamps::off();
                self.codec
                    .encode(cmd, rpc, &self.specs, &ctx, &mut t, &mut fx)
            });
        let carried = |fx: &Effects| {
            let now = Instant::now();
            carries(fx, rpc, cmd.traffic_class(), self.stream, http, now)
        };
        let receipt = match encoded {
            Ok(receipt) if carried(&fx) => receipt,
            Ok(_) => return Ok(Err(Unsent::Codec(NotSentReason::Unencodable))),
            Err(reason) => return Ok(Err(Unsent::Codec(reason))),
        };
        // Its frames, or its HTTP request with the connection it opens, go together or not at
        // all, so the codec's request is either sent whole or reported not sent, charged as
        // `class` (decision 0081).
        let own = |e: &Effect| {
            let requests = frame_of(e, self.stream, true)
                .map(|f| vec![f])
                .or_else(|| http_of(e).map(Vec::from));
            requests.map(|rs| rs.into_iter().map(|r| Request { class, ..r }))
        };
        let charged: Vec<_> = fx.as_slice().iter().filter_map(own).flatten().collect();
        if let Err(refused) = self.core.rates.charge(Instant::now(), key, &charged) {
            return Ok(Err(Unsent::Budget(refused)));
        }
        Ok(Ok((receipt, fx)))
    }

    /// Arms the venue's cancel-on-disconnect on just authenticated epoch `key`, then asks the
    /// codec's resync, as each is due (decision 0058). False when the epoch ended.
    async fn prepare(
        &mut self,
        ws: &mut Option<Link>,
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
    async fn arm(&mut self, ws: &mut Option<Link>, key: ConnKey) -> Result<bool, ExecSessionError> {
        let rpc = self.orders.next_rpc();
        let cmd = VenueCommand::ArmCancelOnDisconnect(true);
        // The session's own arm, one per epoch, may use the safety reserve (decision 0073).
        match self.encode(&cmd, cmd.traffic_class(), false, rpc, key)? {
            Ok((_, fx)) => {
                self.orders.gate.borrow_mut().arm_sent(key.epoch, rpc);
                self.execute_charged(ws, fx, None).await
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
        ws: &mut Option<Link>,
        key: ConnKey,
    ) -> Result<bool, ExecSessionError> {
        let (call, rec, clock) = (CtxCall::Resync, &self.rec, &self.clock);
        let ctx = context(&*self.codec, &mut *self.nonces, rec, clock, call, None)?;
        let mut fx = Effects::new();
        self.codec.resync(&ctx, &mut fx);
        self.orders.gate.borrow_mut().resync_asked(key.epoch);
        let never = ExecSessionError::ResyncNeverFits;
        self.execute_together(ws, key, fx, never).await
    }

    /// Hands each request whose deadline fell due unanswered to the codec's `on_rpc_timeout`,
    /// its events stamped under epoch `key` and the firing journaled at that stamp first
    /// (decision 0090); once the session has stopped, the sink drops them.
    fn time_out(&mut self, key: ConnKey) {
        for rpc in self.rpcs.take_due(Instant::now()) {
            // An arm unanswered at its deadline fails its epoch, whatever the codec reports.
            self.orders.gate.borrow_mut().timed_out(rpc);
            let stamp = self.core.clock.stamp(key, None);
            self.rec.timed_out(stamp, rpc);
            let mut sink = Sink {
                handler: &mut self.handler,
                epochs: &mut self.core.epochs,
                stop: &self.core.stop,
                stamp,
                rpcs: &mut self.rpcs,
                orders: &self.orders,
                own: self.stream,
                conn: self.conn,
            };
            self.codec.on_rpc_timeout(rpc, &mut sink);
        }
    }
}

/// What a session is built from, whatever its handler: its order connection's core, its codec
/// and the venue's caps, the other endpoints' legs and its control. Built in one function for
/// every handler type, so its checks are one code path (decision 0115).
struct Parts {
    core: Core,
    codec: Box<dyn ExecCodec>,
    stream: StreamId,
    url: String,
    headers: HandshakeHeaders,
    in_process: Option<InProcessStream>,
    caps: VenueCaps,
    ns: Namespace,
    specs: SpecTable,
    nonces: Box<dyn NonceSource>,
    rec: Recorder,
    orders: Rc<Shared>,
    rpcs: Rpcs,
    clock: IngestClock,
    stop: watch::Receiver<()>,
    conn: u16,
    legs: Legs,
    control: ExecControl,
}

/// The parts of a session for the account `config.creds` reach, over `entry`, or why there is
/// none ([`ExecSession::new`] and [`ExecSession::in_process`] say which).
fn parts(config: ExecSessionConfig, entry: Entry) -> Result<Parts, ExecSessionError> {
    let (venue, cfg) = (config.venue, &config.cfg);
    let caps = venue.caps(cfg).map_err(SessionError::Config)?;
    let Some(exec) = &caps.exec else {
        return Err(ExecSessionError::NoOrderEntry);
    };
    let (rearm, covers_open) = match exec.order.cancel_on_disconnect {
        CancelOnDisconnect::PerConnection {
            rearm_on_reconnect,
            covers_open_orders,
        } => (rearm_on_reconnect, covers_open_orders),
        other => return Err(ExecSessionError::CancelOnDisconnect(other)),
    };
    // Our client id names one venue order where amends keep the venue id (decision 0080).
    let one_id = exec.order.amend.is_none_or(|amend| amend.keeps_venue_id);
    config.limiter.check(&caps.limits)?;
    let plan = venue.plan_exec(cfg).map_err(ExecSessionError::Venue)?;
    // The first endpoint carries the orders; an in-process stream stands in for exactly one
    // (decisions 0104, 0115).
    let one = matches!(entry, Entry::Socket) || plan.len() == 1;
    let Some(endpoint) = plan.first().cloned().filter(|_| one) else {
        return Err(ExecSessionError::Endpoints(plan.len()));
    };
    let streams: Vec<StreamId> = plan.iter().map(|ep| ep.stream).collect();
    if let Some(at) = (1..streams.len()).find(|&i| streams[..i].contains(&streams[i])) {
        return Err(ExecSessionError::SharedStream(streams[at]));
    }
    let siblings = |own: StreamId| streams.iter().copied().filter(|s| *s != own).collect();
    // Each endpoint's credential headers, read before the credentials go to the codec
    // (decision 0116); an in-process stream opens no socket, so it has none.
    let (codec, in_process, mut headers) = match entry {
        Entry::Socket => {
            let mut headers = Vec::with_capacity(plan.len());
            for ep in &plan {
                ws::check_url(ep.url.as_str()).map_err(SessionError::Url)?;
                headers.push(HandshakeHeaders::for_endpoint(ep, &config.creds)?);
            }
            let codec = venue.exec_codec(cfg, config.creds);
            let codec = codec.ok_or(ExecSessionError::NoOrderEntry)?;
            (codec.map_err(ExecSessionError::Venue)?, None, headers)
        }
        Entry::InProcess(codec, stream) => (codec, Some(stream), Vec::new()),
    };
    let mut headers_of = |i: usize| {
        let slot = headers.get_mut(i);
        slot.map_or_else(HandshakeHeaders::none, std::mem::take)
    };
    let (stop_tx, stop) = watch::channel(());
    let clock = config.clock.clone();
    // Endpoint `i` is stamped connection `conn + i`, each with epochs, pacing, timers and
    // requests of its own (decision 0115).
    let core_of = |i: usize, own: StreamId| {
        let conn = u16::try_from(i)
            .ok()
            .and_then(|i| config.conn.checked_add(i));
        let mut core = Core::new(CoreConfig {
            own,
            connector: config.connector.clone(),
            clock: config.clock.clone(),
            pacing: config.pacing,
            write_stall: config.write_stall.window(),
            conn: conn.ok_or(SessionError::NoConnectionLeft)?,
            stop: stop.clone(),
            http_max_body: config.http_max_body,
            rates: config.limiter.clone(),
        });
        // What an order-entry stream brings carries acks and fills (decision 0078).
        core.inbound = TrafficClass::Safety;
        core.siblings = siblings(own);
        Ok::<_, ExecSessionError>(core)
    };
    let core = core_of(0, endpoint.stream)?;
    let mut legs = Vec::new();
    for (i, ep) in plan.iter().enumerate().skip(1) {
        let core = core_of(i, ep.stream)?;
        let url = ep.url.as_str().to_owned();
        legs.push(Leg::new(core, ep.stream, url, headers_of(i)));
    }
    let rec = Recorder {
        journal: None,
        source: config.nonce_source,
    };
    Ok(Parts {
        core,
        codec,
        stream: endpoint.stream,
        url: endpoint.url.as_str().to_owned(),
        headers: headers_of(0),
        in_process,
        caps,
        ns: config.ns,
        specs: config.specs,
        nonces: config.nonces,
        rec,
        orders: Shared::new(
            config.acct,
            config.rpc_ids,
            rearm,
            covers_open,
            one_id,
            clock.clone(),
            config.outbound,
        ),
        rpcs: Rpcs::new(config.in_flight),
        clock,
        stop,
        conn: config.conn,
        legs: Legs::new(legs),
        control: ExecControl { _stop: stop_tx },
    })
}

/// Executes what `on_open` or the codec's resync asked for on epoch `key` of the connection
/// `core` drives, on `ws`. Its frames on that connection and its HTTP requests, each request
/// with the connection it opens, are charged together first, so all of them go or none does
/// (Codex r4188802873; PR #90 Reviewer B B7): buckets that refuse them for now end the epoch as
/// a drop, which reconnects through the pacing, no sooner than the buckets would admit them,
/// and calls `on_open` (and the resync) again, rather than leave the codec believing it sent
/// what it never did, or waiting on a read that never went; what can never fit together ends
/// the session with `never`. A request that then does not start (its timeout past the end of
/// the clock, or one the runtime cannot make) stays charged. A frame or reconnect for another
/// planned endpoint's stream is handed back ([`Core::spilled`]), charged there as its turn
/// comes (decision 0115). False when the epoch ended.
async fn together(
    core: &mut Core,
    feed: &mut Feed<'_>,
    ws: &mut Option<Link>,
    key: ConnKey,
    mut fx: Effects,
    never: ExecSessionError,
) -> Result<bool, ExecSessionError> {
    let mut effects = fx.take();
    // Nothing behind a reconnect of the connection's stream is ever reached, so nothing behind
    // it is charged (Codex r4188995331).
    let stream = core.own;
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
    let stopped = core.stop.has_changed().is_err();
    let charged = (!stopped).then(|| core.rates.charge(now, key, &all));
    match charged {
        Some(Ok(_)) => {}
        None => return Ok(false),
        // The next attempt waits for the buckets as well as the pacing, so a refused open does
        // not reconnect at every floor, sending nothing, until they have room (Reviewer B, B1).
        Some(Err(Refused { ready_at: Some(at) })) => {
            core.pacer.hold_until(at);
            return Ok(false);
        }
        Some(Err(Refused { ready_at: None })) => return Err(never),
    }
    effects.into_iter().for_each(|e| fx.push(e));
    let open = core.execute_all_charged(ws, feed, fx, None).await?;
    feed.fault.take().map_or(Ok(open), Err)
}

/// Where an order-entry session journals the nonces it reserves and the contexts it gives its
/// codec (FBC-2pr, decision 0078): nothing until the consumer sets a journal. The session's
/// nonce source is journaled by the number the consumer gives it.
struct Recorder {
    journal: Option<Journal>,
    source: NonceSourceId,
}

impl Recorder {
    /// One `Nonce` record per value of `block`, in the order reserved, under Safety whatever
    /// the call, so a restart reads every value spent, a Degraded span's included (decision
    /// 0078 item 1; Reviewer B RB-2pr-4 on PR #115).
    fn nonces(&self, wall: WallNs, block: &NonceBlock) {
        if let Some(journal) = &self.journal {
            for &value in block.as_slice() {
                let source = self.source;
                let class = TrafficClass::Safety;
                journal.record(class, wall, &Record::Nonce { source, value });
            }
        }
    }

    /// The firing of request `rpc`'s deadline, at the stamp the events `on_rpc_timeout` pushes
    /// for it carry, under Safety, since it settles an order-affecting request to `Unknown`
    /// (FBC-0hfl, decision 0090): no ingest sequence the session gives goes unjournaled.
    fn timed_out(&self, stamp: Stamp, rpc: RpcId) {
        if let Some(journal) = &self.journal {
            let record = Record::RpcTimeout { stamp, rpc };
            journal.record(TrafficClass::Safety, stamp.recv_wall, &record);
        }
    }

    /// The context `ctx` given to a call, under `class`, filed under `wall`; `rpc` names an
    /// encode's request.
    fn ctx(&self, class: TrafficClass, wall: WallNs, rpc: Option<RpcId>, ctx: &EncodeCtx) {
        if let Some(journal) = &self.journal {
            let ctx = ctx.clone();
            journal.record(class, wall, &Record::EncodeCtx { rpc, ctx });
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

/// Whether a submitted command may be carried as one HTTP request (FBC-m8vm, decision 0081):
/// only a read the consumer submits as a control command, an order query or the fee query.
/// An order-affecting command stays WebSocket-only until FBC-4nfb (0057), and a consumer's
/// cancel-on-disconnect arm over HTTP would arm another connection than the session's.
fn over_http(item: &Submitted) -> bool {
    matches!(
        item,
        Submitted::Control(VenueCommand::Query(_) | VenueCommand::FeeQuery)
    )
}

/// The context for the codec's `call`: exactly the nonces it asks for, reserved from `nonces`
/// (none when it asks for none), or why they could not be (0014 item 1), at `at`, a timer
/// firing's stamp, or else at `clock`'s time once the nonces are reserved, as an encode's is
/// (Reviewer B RB-2pr-3 on PR #115). The context is journaled through `rec` under Normal
/// (decision 0078), filed under `clock`'s time once the nonces are reserved (Codex P2
/// r4214053432 on PR #115).
fn context(
    codec: &dyn ExecCodec,
    nonces: &mut dyn NonceSource,
    rec: &Recorder,
    clock: &IngestClock,
    call: CtxCall,
    at: Option<(MonoNs, WallNs)>,
) -> Result<EncodeCtx, ExecSessionError> {
    let asked = codec.nonces_for(call);
    let (nonces, reserved) = reserve(nonces, asked, rec, || clock.now())?;
    let (mono, wall) = at.unwrap_or(reserved);
    let ctx = EncodeCtx { wall, mono, nonces };
    rec.ctx(TrafficClass::Normal, reserved.1, None, &ctx);
    Ok(ctx)
}

/// Exactly `asked` nonces from `nonces` (none reserved when `asked` is 0) with the time `now`
/// gives once they are reserved, or why not. What the source reserved is journaled through
/// `rec`, filed under that time, even when it is not what was asked for: those values are
/// spent all the same (0006). A source that refuses ([`NonceSource::try_reserve`]) reserved
/// none, so nothing is journaled for it (FBC-3bq4, decision 0114).
fn reserve(
    nonces: &mut dyn NonceSource,
    asked: u16,
    rec: &Recorder,
    now: impl FnOnce() -> (MonoNs, WallNs),
) -> Result<(NonceBlock, (MonoNs, WallNs)), ExecSessionError> {
    let block = match asked {
        0 => NonceBlock::EMPTY,
        n => nonces
            .try_reserve(n)
            .map_err(ExecSessionError::NonceRefused)?,
    };
    let at = now();
    rec.nonces(at.1, &block);
    if block.len() != usize::from(asked) {
        let reserved = block.len();
        return Err(ExecSessionError::Nonces { asked, reserved });
    }
    Ok((block, at))
}

/// Whether `fx`, an encode's effects for request `rpc` of traffic class `class`, may be executed:
/// they carry the request ([`Effects::carry_request`]), every frame goes to the session's own
/// stream `own`, and none asks to reconnect, which would leave a frame of the request unwritten
/// with its outcome unreported (0014 item 3). An HTTP request is admitted only when `http`
/// allows it (an order query or the fee query, decision 0081), and then as the request's only
/// effect that goes out: one HTTP request and no frame, so the call to `on_http` for it, or its
/// deadline once its epoch ended, settles the whole request. Otherwise order entry is
/// WebSocket-only (0057). Nor does a frame or the HTTP request name a request whose deadline,
/// its timeout from `now`, is past the end of the clock: that request would never come back
/// `Unknown` (PR #87 Reviewer B B9).
fn carries(
    fx: &Effects,
    rpc: RpcId,
    class: fbc_core::TrafficClass,
    own: StreamId,
    http: bool,
    now: Instant,
) -> bool {
    let unbounded = |timeout| now.checked_add(timeout).is_none();
    let refused = |effect: &Effect| match effect {
        Effect::Send { stream, rpc, .. } => {
            *stream != own || rpc.is_some_and(|call| unbounded(call.timeout))
        }
        Effect::Http { timeout, .. } => !http || unbounded(*timeout),
        Effect::Reconnect { .. } => true,
        Effect::Timer { .. } => false,
    };
    let effects = fx.as_slice();
    let asks = effects.iter().filter(|e| matches!(e, Effect::Http { .. }));
    let writes = effects.iter().any(|e| matches!(e, Effect::Send { .. }));
    let one_way = match asks.count() {
        0 => true,
        n => n == 1 && !writes,
    };
    one_way && !effects.iter().any(refused) && fx.carry_request(rpc, class)
}

/// The order-entry session as the core waits to connect: the control's drop, and what still
/// reaches the handler meanwhile (FBC-0ga). A command submitted is `NotSent(Disconnected)`, and
/// a request whose deadline falls due is handed to the codec's `on_rpc_timeout`, its events
/// stamped under `key`, the epoch the session waits to open, and the firing journaled at that
/// stamp first (decision 0090); once the control has dropped, the handler is told neither.
/// The other endpoints keep going meanwhile (decision 0115): what wakes one is taken as on an
/// open order connection, what it asks for on the order connection's stream refused and
/// counted, and an error that ends the session left in `fault`, which stops the wait.
struct Between<'a, H> {
    stop: watch::Receiver<()>,
    orders: &'a Shared,
    rpcs: &'a mut Rpcs,
    codec: &'a mut dyn ExecCodec,
    handler: &'a mut H,
    clock: &'a IngestClock,
    rec: &'a Recorder,
    key: ConnKey,
    legs: &'a mut Legs,
    nonces: &'a mut dyn NonceSource,
    caps: &'a VenueCaps,
    ns: Namespace,
    specs: &'a SpecTable,
    decode_errors: &'a mut u64,
    fault: &'a mut Option<ExecSessionError>,
    own: StreamId,
    /// What woke a leg, waiting for `apply`.
    pending: Option<(usize, LegWake)>,
    /// Whether the last change was a leg's: the next wait yields before it polls the legs
    /// again, so the order connection's attempt, dial and deadline, which the core polls only
    /// once this wait is pending, are not starved by a leg's frames that keep arriving.
    after_leg: bool,
}

impl<H: ExecHandler> Between<'_, H> {
    fn stopped(&self) -> bool {
        self.stop.has_changed().is_err()
    }

    /// Leg `woke.0` woke: kept for `apply`, the next wait yielding first.
    fn woke(&mut self, woke: (usize, LegWake)) -> bool {
        (self.pending, self.after_leg) = (Some(woke), true);
        true
    }

    /// Takes what woke a leg while the order connection waits to connect ([`legs::take`]);
    /// false when an error ended the session, left in `fault`.
    async fn take_leg(&mut self, (i, wake): (usize, LegWake)) -> bool {
        let mut feed = Feed {
            codec: &mut *self.codec,
            handler: &mut *self.handler,
            nonces: &mut *self.nonces,
            rec: self.rec,
            clock: self.clock,
            caps: self.caps,
            ns: self.ns,
            specs: self.specs,
            stop: &self.stop,
            decode_errors: &mut *self.decode_errors,
            fault: &mut *self.fault,
            rpcs: &mut *self.rpcs,
            orders: self.orders,
            own: self.own,
            conn: self.key.conn,
        };
        let taken = legs::take(&mut feed, None, self.legs, i, wake).await;
        *self.fault = taken.err();
        self.fault.is_none()
    }
}

impl<H: ExecHandler> Control for Between<'_, H> {
    /// True when there is something to report (at once when it already waits), or a leg woke;
    /// false once the control dropped or a leg's input ended the session.
    async fn changed(&mut self) -> bool {
        if self.fault.is_some() {
            return false;
        }
        let (due, waiting) = (self.rpcs.next_deadline(), self.orders.waiting());
        let legs = !self.legs.is_empty();
        // What waits is taken after a yield, so a handler that submits again from
        // `on_submitted` cannot hold the task (PR #87 Reviewer B B5).
        tokio::select! {
            biased;
            r = self.stop.changed() => r.is_ok(),
            () = tokio::task::yield_now(), if waiting => true,
            _ = self.orders.wake.notified(), if !waiting => true,
            _ = sleep_or_never(due) => true,
            woke = next_leg(self.legs, std::mem::take(&mut self.after_leg)), if legs => {
                self.woke(woke)
            }
        }
    }

    async fn apply(&mut self) {
        // A leg's error ends the session with nothing more reported.
        if let Some(woke) = self.pending.take()
            && !self.take_leg(woke).await
        {
            return;
        }
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
            self.rec.timed_out(stamp, rpc);
            let mut sink = Late {
                handler: &mut *self.handler,
                stop: &self.stop,
                stamp,
                clock: self.clock,
            };
            self.codec.on_rpc_timeout(rpc, &mut sink);
        }
    }
}

/// What wakes a leg next ([`Legs::next`]), after a yield when `yield_first`.
async fn next_leg(legs: &mut Legs, yield_first: bool) -> (usize, LegWake) {
    if yield_first {
        tokio::task::yield_now().await;
    }
    legs.next().await
}

/// Hands the events of a deadline that fell due while the session waits to connect to the
/// handler, stamped `stamp`, until the control drops.
struct Late<'a, H> {
    handler: &'a mut H,
    stop: &'a watch::Receiver<()>,
    stamp: Stamp,
    /// Names the deadline's firing as the input being handled while the handler hears it.
    clock: &'a IngestClock,
}

impl<H: ExecHandler> ExecSink for Late<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        if self.stop.has_changed().is_ok() {
            let (stamp, handler) = (self.stamp, &mut *self.handler);
            let env = Envelope::new(stamp, meta, ev);
            self.clock.attributing(stamp, || handler.on_exec(env));
        }
    }
}

/// What the core hands a current epoch's timer firings and HTTP results to: the session's one
/// codec, inside the venue's decode scope for a result, and its handler. Once a timer's nonces
/// were mis-reserved nothing reaches the codec, and the core takes no more inputs. The stop is
/// read where an input is taken, by the session's loop or as a write waits, not again here: an
/// input taken while the control stood is journaled in its open epoch, so it reaches the codec
/// even when the control drops before it is handed on, from another thread or as the codec
/// redacts it, and replay, which feeds an open epoch's inputs, feeds the codec what it was
/// handed live (Codex P1 r4215753453 on PR #115). Its effects are not executed after the stop,
/// nor its events handed on. A Safety write's tick-to-wire reaches the handler (FBC-qfm).
struct Feed<'a> {
    codec: &'a mut dyn ExecCodec,
    /// The consumer's handler, behind `dyn`, so the code that hands it inputs is one function
    /// whatever the handler's type.
    handler: &'a mut dyn ExecHandler,
    nonces: &'a mut dyn NonceSource,
    rec: &'a Recorder,
    clock: &'a IngestClock,
    caps: &'a VenueCaps,
    ns: Namespace,
    specs: &'a SpecTable,
    stop: &'a watch::Receiver<()>,
    decode_errors: &'a mut u64,
    fault: &'a mut Option<ExecSessionError>,
    rpcs: &'a mut Rpcs,
    orders: &'a Shared,
    /// The order connection's stream and connection number: only its events move the session's
    /// readiness (decision 0115).
    own: StreamId,
    conn: u16,
}

impl Feed<'_> {
    /// Whether an input may still reach the codec: no timer's nonces were mis-reserved.
    fn live(&self) -> bool {
        self.fault.is_none()
    }
}

impl EpochInputs for Feed<'_> {
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
        let at = Some((stamp.recv_mono, stamp.recv_wall));
        let (rec, clock) = (self.rec, self.clock);
        match context(&*self.codec, &mut *self.nonces, rec, clock, call, at) {
            Ok(ctx) => self.codec.on_timer(tag, &ctx, fx),
            Err(e) => *self.fault = Some(e),
        }
    }

    /// Hands a current epoch's HTTP result to the codec's `on_http`, inside the venue's decode
    /// scope. A result of an order-entry request clears its deadline first, whatever the codec
    /// then pushes: `on_http` reports that request's outcome, `Unknown` included, which answers
    /// no request, so `on_rpc_timeout` must not report it again (Codex 4212123422 on PR #109,
    /// decision 0081).
    fn answer(&mut self, epochs: &mut Epochs, stamp: Stamp, done: Answered, fx: &mut Effects) {
        if !self.live() {
            return;
        }
        if let Some(rpc) = done.rpc {
            self.rpcs.answered(rpc);
        }
        let mut sink = Sink {
            handler: &mut *self.handler,
            epochs,
            stop: self.stop,
            stamp,
            rpcs: &mut *self.rpcs,
            orders: self.orders,
            own: self.own,
            conn: self.conn,
        };
        let (codec, specs) = (&mut *self.codec, self.specs);
        let decoded = dispatch(self.caps, self.ns, |scope| {
            with_response(&done.result, |resp| {
                codec.on_http(done.tag, resp, scope, specs, &mut sink, fx)
            })
        });
        if decoded.is_err() {
            *self.decode_errors += 1;
            self.handler.on_decode_error(stamp.conn);
        }
    }

    /// Handed to the handler as the write completes (FBC-qfm).
    fn on_tick_to_wire(&mut self, sample: TickToWire) {
        self.handler.on_tick_to_wire(sample);
    }

    fn sent_rpc(&mut self, call: RpcCall) {
        self.rpcs.sent(call, Instant::now());
    }

    fn asked_rpc(&mut self, call: RpcCall, key: ConnKey) {
        self.rpcs.asked(call, key, Instant::now());
    }

    /// A timer's mis-reserved nonces end the session before another effect is executed.
    fn halted(&self) -> bool {
        self.fault.is_some()
    }
}

/// Stamps each pushed event with its input's stamp and hands it to the handler at once (0014
/// item 2); an event of an ended epoch, one pushed after the control dropped included, is
/// dropped and counted. An event that answers a request clears its deadline (FBC-0ga), and the
/// codec's report of the session's own stream says whether its epoch is authenticated. Only an
/// event of an input of the order connection, connection `conn`, authenticates its epoch; one
/// of another endpoint's input counts for the order connection's authenticated epoch, if one
/// is, as the answer to its arm, the end of its resync or what it shows of the orders held
/// unprotected (decisions 0058, 0080, 0115).
struct Sink<'a> {
    handler: &'a mut dyn ExecHandler,
    epochs: &'a mut Epochs,
    stop: &'a watch::Receiver<()>,
    stamp: Stamp,
    rpcs: &'a mut Rpcs,
    orders: &'a Shared,
    own: StreamId,
    conn: u16,
}

impl ExecSink for Sink<'_> {
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
            // The order connection's epoch the event counts for: its own input's, whose report
            // of the order stream says whether that epoch is authenticated; another endpoint's
            // counts for the order connection's authenticated epoch, if one is, and readies
            // nothing itself (decision 0115).
            let epoch = if self.stamp.conn.conn == self.conn {
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
                Some(epoch)
            } else {
                self.orders.ready()
            };
            if let Some(epoch) = epoch {
                // The arm's answer and the resync's end count as the handler is handed the
                // event, so the handler hearing the one that opens the epoch sees places taken
                // (PR #90 Reviewer B B1). What it submits then still goes out only once it has
                // returned, so after fbc-oms has applied the ResyncEnd.
                let settled = self.orders.gate.borrow().settles(epoch, &ev);
                if let Some(settled) = settled {
                    self.orders.gate.borrow_mut().settle(epoch, settled);
                }
                // What the event shows of the orders resting from an earlier epoch counts as it
                // is handed over too (FBC-nvxn).
                self.orders.gate.borrow_mut().observe(epoch, &ev);
            }
            // While the handler hears the event, what it submits is attributed to its input
            // (decision 0100).
            let (stamp, orders, handler) = (self.stamp, self.orders, &mut *self.handler);
            let clock = orders.clock();
            clock.attributing(stamp, || handler.on_exec(Envelope::new(stamp, meta, ev)));
            // The epoch just became placing with orders unprotected: the handler is told them
            // once, unless it stopped the session as it took the event.
            let notice = epoch.and_then(|epoch| orders.gate.borrow_mut().notice(epoch));
            if let Some(unprotected) = notice
                && self.stop.has_changed().is_ok()
            {
                clock.attributing(stamp, || handler.on_unprotected(&unprotected));
            }
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
                    covers_open_orders: false,
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
                "the venue planned 2 order-entry connections; a session drives at least one, and \
                 exactly one over an in-process stream",
            ),
            (
                ExecSessionError::SharedStream(StreamId(2)),
                "the venue planned two order-entry connections on stream 2",
            ),
            (
                ExecSessionError::Nonces {
                    asked: 2,
                    reserved: 1,
                },
                "the nonce source reserved 1 nonces when 2 were asked for",
            ),
            (
                ExecSessionError::NonceRefused(NonceRefused::Exhausted),
                "the nonce source refused a reservation: no nonce is left below u64::MAX",
            ),
        ];
        for (err, text) in cases {
            assert_eq!(err.to_string(), text);
        }
        let epoch = ExecSessionError::from(EpochError::Exhausted { conn: 3 });
        assert_eq!(epoch.to_string(), "connection 3 has no epoch left to open");
    }

    /// Decision 0081: an encode may carry its request as exactly one HTTP request, with no frame
    /// beside it and a deadline the clock can hold, and only where the command allows it.
    #[test]
    fn an_encode_carries_its_request_as_one_http_request_only_where_allowed() {
        use fbc_core::{HttpMethod, HttpRequest, HttpTag, OpKind, RateCharge, WireSlice, WireUrl};
        use std::time::Duration;
        let (rpc, own, class) = (RpcId(7), StreamId(1), TrafficClass::Safety);
        let charge = RateCharge::one(OpKind::Query, None);
        let http = |timeout| Effect::Http {
            tag: HttpTag(7),
            req: HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain("http://127.0.0.1:9/orders"),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: Some(rpc),
            timeout,
            class,
            charge,
        };
        let frame = Effect::Send {
            stream: own,
            frame: WireSlice::plain(b"query".to_vec()),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: Duration::from_secs(5),
            }),
            class,
            charge,
        };
        let timer = Effect::Timer {
            tag: TimerTag(1),
            after: Duration::from_secs(1),
        };
        let fx = |effects: &[Effect]| {
            let mut fx = Effects::new();
            effects.iter().cloned().for_each(|e| fx.push(e));
            fx
        };
        let now = Instant::now();
        let carried =
            |effects: &[Effect], allowed| carries(&fx(effects), rpc, class, own, allowed, now);
        let five = Duration::from_secs(5);
        assert!(carried(&[http(five), timer.clone()], true));
        assert!(!carried(&[http(five)], false));
        assert!(!carried(&[http(five), http(five)], true));
        assert!(!carried(&[frame.clone(), http(five)], true));
        assert!(!carried(&[http(Duration::MAX)], true));
        assert!(carried(&[frame.clone(), timer], false));
        // A reconnect would leave the request's outcome unreported.
        let reconnect = Effect::Reconnect {
            stream: own,
            reason: "test",
        };
        assert!(!carried(&[frame, reconnect], false));
    }

    #[test]
    fn an_exec_handler_by_default_ignores_a_tick_to_wire() {
        let mut seen = Vec::new();
        let mut handler = |env: Envelope<ExecEvent>| seen.push(env.stamp.ingest_seq);
        let clock = IngestClock::new();
        let frame = clock.stamp(ConnKey { conn: 1, epoch: 0 }, Some(KernelRxNs(1_000)));
        handler.on_tick_to_wire(TickToWire {
            stream: StreamId(1),
            frame,
            nanos: 250,
        });
        assert!(seen.is_empty());
    }
}
