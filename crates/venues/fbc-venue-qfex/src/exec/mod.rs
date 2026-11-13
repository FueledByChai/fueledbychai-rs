//! QFEX's trade WebSocket codec (FBC-zeb0, BT-702): [`QfexExec`] authenticates each connection,
//! subscribes to the account's private channels, feeds every rate refusal back to the shared
//! provider-user budgets, reads the private state QFEX sends, and encodes the v1 order
//! commands (FBC-blcw, `order.rs`). Recording-only it normalizes nothing and settles no
//! command; in a probe mode it decodes the private state into events, and settles the arm by
//! its own echo and a cancel by the `order_response` naming its order (FBC-jxau).
//!
//! It runs in one of two modes:
//!
//! - **Recording-only** ([`QfexExec::new`]), what a test builds: it refuses the
//!   cancel-on-disconnect arm, and ends its epoch on every private state frame and on a resync.
//!   The factory never builds it: with `qfex.exec.mode` absent, `caps.exec` stays `None` and
//!   the factory builds no order-entry codec (decisions 0117, 0122).
//! - **The probe** ([`QfexExec::probe`], decision 0132 item 6, FBC-mfey), which the factory
//!   builds under `preprod_probe` or `production_probe` alone (decision 0143 item 2), its caps
//!   [`probe_exec_caps`] in both. Under `production_probe` alone, loopback origins included, it
//!   refuses every place, batch place and amend `NotSent(Unsupported)`, whatever the epoch and the
//!   order, though its caps declare places, until a record admits places on production (FBC-5d7x,
//!   decision 0145); in every other way it is alike in both. It encodes one cancel-on-disconnect
//!   arm per epoch, and refuses unsent every later arm on that epoch and every disarm (item 1), so
//!   at most one arm is in flight on a connection. Only the arm's own echo answers it
//!   (`echo::arm_echo`), on its epoch, while it is in flight on the authenticated stream: an ack
//!   accepts it at `AckLevel::Final`, as FBC-291a's capture of QFEX production shows
//!   (`fixtures/qfex/exec-recorded/arm-production/`, decision 0143 item 4), and an err rejects it
//!   unless its code is `RateLimited`, which holds every frame budget for its kept hint and answers
//!   nothing, the arm then failing at its deadline whatever echoes it later (decision 0132 item 1,
//!   as 0147 item 1 reads 0134 item 5). An echo with a member added or a value of another type, a
//!   cancelled order and a position answer nothing; an arm nothing answers ends `Unknown` at its
//!   deadline, and the session ends a rejected or unknown arm's epoch (decision 0058). The journal
//!   keeps the arm's echo and, while the arm is the only request in flight, the shape of every
//!   other echo (item 3, as 0134 item 7 narrows it; `echo.rs`); an answered arm is in flight no
//!   more, so its answer closes that window, as a page of its resync in flight does. The codec
//!   decides the answer on the bytes the journal keeps, so replay answers the arm where live did.
//!   Its epoch stays open on private state, which it decodes (below), and its resync is decision
//!   0132 item 5's (`resync.rs`): a signed positions read, the open orders' pages and the trades of
//!   the orders it placed, pushed in one bracket once every read has answered, or a reconnect at
//!   its own deadline; its caps declare the snapshot untrustworthy, so no resync seeds a position.
//!
//! - **Authentication.** On opening, the authentication frame from `src/auth` (its two nonces
//!   reserved for the open, `nonces_for`) and a timer for QFEX's deadline: the server closes a
//!   connection that has not authenticated within one minute
//!   ([authenticate](https://docs.qfex.com/websocket/channels/trade/authenticate)). The reply is
//!   the page's `{"type":"auth","result":"success"}` or the AsyncAPI's `{"authenticated":true}`
//!   ([trade.yaml](https://docs.qfex.com/websocket/trade.yaml)); anything else, or no reply when
//!   the deadline fires, ends the epoch. The deadline's tag names the epoch's opening, so a
//!   timer of an earlier epoch does nothing.
//! - **Subscriptions, and when the stream is authenticated (FBC-bsou, decision 0132 item 1).**
//!   A successful auth reply sends `order_responses`, `fills` and `positions`, one frame each,
//!   as the AsyncAPI's `subscribe_orders`, `subscribe_fills` and `subscribe_positions` show,
//!   and pushes no connection event. The stream is reported authenticated once per epoch, on
//!   the last of the three `{"subscribed":…}` confirmations, in any order: the session sends its
//!   cancel-on-disconnect arm and resync on that report, so none of the epoch's subscriptions
//!   is outstanding when they go out, and what the arm cancels reaches a confirmed channel.
//!   Only a channel's first confirmation in the epoch confirms it; a repeat keeps the epoch
//!   open, and an ack (whose echoed channel the journal spans, 0134 item 4) confirms nothing.
//!   Every other frame meanwhile takes the path it takes once the stream is authenticated, and
//!   a confirmation of a channel the codec never subscribes is not recognised. Before the last
//!   confirmation, an err other than RateLimited ends the epoch (0134 item 5), and a
//!   RateLimited one holds the budget with the confirmation still pending. An
//!   `{"unsubscribed":…}` naming one of the three ends the epoch, before or after the report:
//!   the codec never unsubscribes, and without them the session would not hear its cancels and
//!   fills. The authentication deadline runs until the last confirmation, so a confirmation
//!   that never comes ends that epoch only.
//! - **Errs and acks (decision 0134).** The journal keeps an err or an ack that echoes a request
//!   as its shape and the few values item 3 lists (`echo.rs`), and the codec decides from what it
//!   keeps, live as in replay: it blanks the spans `redact_inbound` names before it reads any
//!   frame. A RateLimited err, in every state, is one budget-only `Effect::RateLimited` with the
//!   bucket of a mapped echo `type` and the hint of a kept `message` (0126). Before the stream
//!   is authenticated every other err ends the epoch. Once it is, an err with a request's code
//!   is counted and settles nothing, its request left to end `Unknown` at its deadline, and one
//!   with a code about the session ([`ErrCode::ends_session`]) or an undocumented one ends the
//!   epoch. An ack is counted and settles nothing. The one exception to both is the arm's own
//!   echo, which answers the arm as the probe's paragraph says, and is then read as every err
//!   or ack is.
//! - **A frame that is not a JSON object**, a frame kept whole among them (which replay reads
//!   as a run of `2`s), is refused (`DecodeError::Malformed`) and ends the epoch, in every state
//!   (0134 item 6). So does a frame that repeats a member or matches two envelopes.
//! - **Private state.** An `order_response` whose status is `RATE_LIMITED` holds every frame
//!   budget, as an echo naming no bucket does. Recording-only, every other private state frame
//!   is inspected, each number exactly ([`PrivateState`]), with no event, and ends the epoch
//!   (FBC-zeb0), and a resync asks to reconnect. In a probe mode, where FBC-zeb0's rule no
//!   longer holds (decision 0143 item 4), each is decoded (`private.rs`, decision 0132 items 5
//!   and 8) with the epoch kept open: an `order_response` into an order update, a live `FILLED`
//!   ending the order only when its `quantity_remaining` is zero; a `fill_response` into a
//!   fill keyed by its `trade_id`; a `position_response` into the account's position; a page no
//!   read asked for into nothing; and one on a symbol the spec table does not hold into
//!   nothing, counted (decision 0147 item 2). Each event carries no venue time. A frame not as
//!   documented, an unmapped status among them, is a decode error and pushes nothing; only a
//!   frame the codec does not recognise ends the epoch. A page the resync asked for is read by
//!   the resync (`resync.rs`).
//! - **Commands.** A limit GTC, ALO (post-only) or limit IOC place, and a cancel or an order
//!   query by the venue's order UUID, each one frame naming its request with the consumer's
//!   deadline and labelled with its command's traffic class, sent only on an authenticated
//!   epoch (`NotSent(Disconnected)` otherwise); everything else is refused unsent (`order.rs`),
//!   and under `production_probe` every place too, before the epoch is read (decision 0145),
//!   but the probe's one arm per epoch, [`ARM`], charged as the rate page's 0.1 for
//!   `cancel_on_disconnect` under `Control`, which charges both buckets since the page names
//!   none for it. While the resync's open-order pages run, a query, answered in the same kind
//!   of frame, is refused `NotSent(Backpressure)` with nothing written (item 5).
//!   No reply settles a request but the probe's arm, as above, and its cancels (decision 0132
//!   item 8): a `CANCELLED` `order_response` naming a cancel's order accepts it at
//!   `AckLevel::Final`, pushed before the order's update, and a `NO_SUCH_ORDER` one rejects it
//!   `RejectKind::NotFound` and ends nothing, each only while that cancel is the one request in
//!   flight naming the order, a query of it counted, and the order id compared exactly. An err
//!   or an ack settles no cancel, whatever it echoes, and nothing settles a place, whose
//!   client-id echo no capture shows yet: each other request ends `Unknown` at its deadline.
//!   Recording-only, no reply settles anything but its deadline.
//!
//! Each frame and its rate charge: the authentication as one tenth of a weight unit under
//! `Control` (the rate page lists no weight for it, and `Control` charges both buckets), each
//! subscription as the page's 0.1 under `Subscribe`, both Safety traffic; each command as
//! `order.rs` says.

mod caps;
pub(crate) mod echo;
mod ids;
pub(crate) mod json;
mod order;
mod private;
mod resync;

use core::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use fbc_core::{
    AckLevel, ConfigError, ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects,
    EncodeCtx, EncodeReceipt, ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpResponse, HttpTag,
    Inbound, InboundSpans, ItemRef, NotSentReason, OpKind, PathStamps, RateCharge, RawFrame,
    Reject, RejectKind, RpcCall, RpcId, SpecTable, StreamId, SubmitOutcome, TimerTag, TrafficClass,
    VenueCommand, VenueConfig, VenueMeta, VenueOrderId, WireSlice,
};

use self::echo::Envelope;
use self::json::{Kind, Member, Node};
use self::private::{Decoded, Misread, Status};
use crate::account_reads;
use crate::auth::TradeAuth;
use crate::probe::{Environment, KEY_EXEC_MODE, Probe};
use crate::rate;

pub use self::caps::{RISK_REJECT_WINDOW, probe_exec_caps};
pub use self::echo::{ARM, ErrCode};
pub use self::ids::{CLIENT_ID_FORMAT, is_venue_order_id};
pub(crate) use self::private::exact;
pub use self::private::{OrderStatus, PrivateState};
pub use self::resync::{ResyncEvidence, TRADES_WEIGHT};

/// The trade WebSocket, the codec's one stream.
pub const TRADE: StreamId = StreamId(0);

/// The private channels subscribed once authenticated, in this order.
pub const CHANNELS: [&str; 3] = ["order_responses", "fills", "positions"];

/// QFEX's authentication deadline, from the authenticate page, which the codec also holds the
/// confirmations of its subscriptions to (FBC-bsou).
pub const AUTH_DEADLINE: Duration = Duration::from_secs(60);

/// The authentication deadline's tag is this, plus the number of the opening it belongs to.
const AUTH_TIMER: u64 = 1 << 32;

/// What [`QfexExec::on_frame`] answers a frame it cannot read with: one answer, whatever the
/// bytes, so a frame kept whole meets live what its blanked bytes meet in replay.
const NOT_AN_OBJECT: &str = "QFEX trade frame is not a JSON object";

/// What the codec counted, across its epochs.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct QfexExecCounts {
    errs: [u64; 10],
    /// Errs whose `error_code` is absent, spanned, or not one of the documented ten.
    pub undocumented_errs: u64,
    /// Acks, each counted; only an ack echoing the arm in flight answers anything, the arm
    /// (decision 0132 item 1).
    pub acks: u64,
    /// Arms accepted, each by an ack echoing it (decision 0132 item 1).
    pub arm_accepted: u64,
    /// Arms rejected, each by an err echoing it whose code is not `RateLimited` (decision 0147
    /// item 1).
    pub arm_rejected: u64,
    /// `Effect::RateLimited` holds asked for, by RateLimited errs and `RATE_LIMITED` orders.
    pub rate_holds: u64,
    /// `subscribed` replies naming one of [`CHANNELS`], repeats included.
    pub subscribed: u64,
    /// `unsubscribed` replies naming one of [`CHANNELS`]; each ended its epoch.
    pub unsubscribed: u64,
    /// Private state frames inspected or decoded, valid or not; recording-only, each ended its
    /// epoch.
    pub private_state: u64,
    /// `order_response` frames, in a probe mode, whose status it does not map (decision 0132
    /// item 8); each was a decode error with the epoch open.
    pub unmapped_statuses: u64,
    /// Order, fill and position frames, in a probe mode, on a symbol the spec table does not
    /// hold: each pushed nothing (decision 0147 item 2).
    pub unheld: u64,
    /// Pages of open orders and of trades, in a probe mode, that no read of the codec asked
    /// for: each was inspected and pushed nothing.
    pub pages_unasked: u64,
    /// Cancels accepted, each by a `CANCELLED` `order_response` naming its order (decision
    /// 0132 item 8).
    pub cancels_accepted: u64,
    /// Cancels rejected, each by a `NO_SUCH_ORDER` `order_response` naming its order (decision
    /// 0132 item 8).
    pub cancels_not_found: u64,
    /// `CANCELLED` and `NO_SUCH_ORDER` frames naming an order more than one request in flight
    /// names, a cancel or a query: each settled nothing (decision 0132 item 8).
    pub replies_naming_several: u64,
    /// Frames refused as not a JSON object, a repeated member, or two envelopes at once.
    pub undecodable: u64,
    /// Resyncs that pushed their `ResyncEnd` (decision 0132 item 5).
    pub resyncs: u64,
    /// Resyncs whose epoch ended before their `ResyncEnd`, by the codec or the connection.
    pub resyncs_unfinished: u64,
    /// Full `get_user_orders` pages a resync read.
    pub full_pages: u64,
    /// Order rows and trade rows a resync's pages listed again, by `order_id` or `trade_id`.
    pub duplicate_rows: u64,
    /// Trade rows a resync read for an order this codec did not place: none was pushed.
    pub unplaced_trades: u64,
    /// Order rows and positions a resync read on a symbol the spec table does not hold: none
    /// was pushed (decision 0147 item 2).
    pub unheld_rows: u64,
}

impl QfexExecCounts {
    /// The errs counted with the documented code `code`.
    pub fn errs(&self, code: ErrCode) -> u64 {
        self.errs[code.index()]
    }
}

/// Where the codec is in its current epoch.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Phase {
    /// No epoch is open, or the codec asked to end it.
    Closed,
    /// Authentication was sent; its reply has not come.
    Awaiting,
    /// Authentication succeeded and the subscriptions were sent: bit `i` is set once QFEX has
    /// confirmed `CHANNELS[i]` in this epoch.
    Subscribing(u8),
    /// Every subscription was confirmed, and the stream reported authenticated.
    Authenticated,
}

/// Every channel confirmed.
const ALL_CONFIRMED: u8 = (1 << CHANNELS.len()) - 1;

/// What a request in flight on the current epoch was sent as: the arm, a place, or a cancel or
/// a query naming the venue's order id.
#[derive(Clone, Debug)]
enum Sent {
    Arm,
    Place,
    Cancel(VenueOrderId),
    Query(VenueOrderId),
}

/// The order-entry codec for one QFEX account's trade WebSocket, recording-only or in a probe
/// mode.
pub struct QfexExec {
    auth: TradeAuth,
    /// Each command's deadline, after which it ends `Unknown`.
    rpc_timeout: Duration,
    /// The probe mode's settings; `None` for the recording-only codec.
    probe: Option<Probe>,
    phase: Phase,
    /// How many times a stream opened: the current epoch's number.
    opens: u32,
    /// The request the current epoch's arm was encoded as, once it was. It stays set once the
    /// arm is answered, so a later arm on the epoch is still refused and the arm's echo is still
    /// kept as its own.
    ///
    /// Both this and `in_flight` are set when a request is encoded, which is the last the codec
    /// hears of it before its deadline or its answer: a session that then finds no room in its
    /// buckets (`ExecSession`'s charge after `encode`) writes nothing and tells the codec
    /// nothing. A request refused there stays in `in_flight` until the epoch ends, which only
    /// closes the arm's window early (decision 0132 item 3), the cautious side. An arm refused
    /// there leaves `armed` set, so the frames that arrive before the epoch ends are journaled
    /// under the arm's rules though no arm was written, and an ack echoing it would answer an
    /// arm the session never sent; the session fails that epoch at once (its gate's
    /// `arm_not_sent`, which then reads no answer), so this lasts only until it closes. The
    /// session journals each encode's context before that charge, so a replay that re-runs the
    /// journaled encodes redacts and answers alike. A runtime callback for an encoded request
    /// left unwritten would make both exact.
    armed: Option<RpcId>,
    /// Whether a `RateLimited` err echoed the current epoch's arm: QFEX refused it for rate, so
    /// nothing answers it after, and it ends `Unknown` at its deadline (decision 0132 item 1).
    /// It stays in `in_flight` until then, so the arm's window is what it was.
    arm_rate_refused: bool,
    /// The requests encoded on the current epoch that neither their deadline nor an answer has
    /// ended, in order, each with what it was sent as: the arm is answered by its own echo
    /// (decision 0132 item 1), and a cancel by an `order_response` naming its order while no
    /// other request in flight names it (item 8). Nothing else settles here.
    ///
    /// As `armed` says, a request the session's buckets refused after it was encoded stays here
    /// until the epoch ends: a `CANCELLED` or `NO_SUCH_ORDER` naming its order would then answer
    /// a cancel the consumer was told was not sent (fbc-oms reads that answer as changing
    /// nothing), and a second cancel of that order would leave each unsettled until its
    /// deadline. A runtime callback for an encoded request left unwritten would make it exact.
    in_flight: Vec<(RpcId, Sent)>,
    counts: QfexExecCounts,
    last: Option<PrivateState>,
    /// The REST origin the probe's guard admitted, without a trailing `/`; empty for the
    /// recording-only codec, which reads nothing over REST.
    rest: String,
    /// The orders this codec placed and has not seen end, across its epochs (decision 0132
    /// item 5): each leaves on an order update showing it ended, for `ended` or for good
    /// (`resync.rs`). As `armed` says, a place the session's buckets refused after it was
    /// encoded is recorded all the same; it only moves the trades read's start time earlier,
    /// the cautious side.
    placed: Vec<resync::Placed>,
    /// The orders an order update ended on an epoch whose resync had not ended, placed on an
    /// earlier epoch, until a resync ends: the fills they missed are not read yet (`resync.rs`).
    ended: Vec<resync::Placed>,
    /// Whether the current epoch's resync has pushed its `ResyncEnd`.
    resynced: bool,
    /// The latest `fill_response` time this codec decoded, in whole Unix seconds.
    latest_fill_secs: Option<u64>,
    /// The resync in flight on the current epoch.
    resync: Option<resync::Resync>,
    /// What the last resync showed, finished or not.
    last_resync: Option<ResyncEvidence>,
}

impl QfexExec {
    /// The recording-only codec for the account `auth` authenticates as, each command it sends
    /// awaiting its reply for `rpc_timeout`, the consumer's request deadline. It refuses the
    /// cancel-on-disconnect arm, and the factory never builds it.
    pub fn new(auth: TradeAuth, rpc_timeout: Duration) -> QfexExec {
        QfexExec::with(auth, rpc_timeout, None, String::new())
    }

    /// The codec in the probe mode `cfg` sets (decision 0132 item 6, decision 0143 item 2),
    /// alike in both but for `production_probe`'s refusal of every place, batch place and amend
    /// (decision 0145), for the account `auth` authenticates as: each command it sends, its one
    /// arm per epoch included, awaits its reply for `qfex.exec.rpc_timeout`. Its settings are
    /// those the mode's guard admitted ([`crate::probe::mode`]), and its resync reads positions
    /// from the REST origin that same guard admitted ([`crate::probe::connections`]), so the
    /// signed read never leaves the environment the mode names. Refused, naming the key and
    /// never a value, when `qfex.exec.mode` is absent, for everything `probe::connections`
    /// refuses, and when `auth` was built for another account key than `cfg` binds, or for the
    /// primary account where `cfg` selects a subaccount or the other way round.
    pub fn probe(auth: TradeAuth, cfg: &VenueConfig) -> Result<QfexExec, ConfigError> {
        let probe = crate::probe::mode(cfg)?.ok_or(ConfigError::Missing(KEY_EXEC_MODE))?;
        let connections = crate::probe::connections(cfg)?;
        let subaccount = matches!(
            crate::auth::account_selection(cfg)?,
            crate::auth::AccountSelection::Subaccount(_)
        );
        if auth.account() != connections.account || auth.is_subaccount() != subaccount {
            return Err(ConfigError::Invalid {
                key: crate::auth::ACCOUNT_KEY,
                reason: "the trade handle was built for another account",
            });
        }
        let rest = connections.rest.to_owned();
        Ok(QfexExec::with(auth, probe.rpc_timeout(), Some(probe), rest))
    }

    fn with(
        auth: TradeAuth,
        rpc_timeout: Duration,
        probe: Option<Probe>,
        rest: String,
    ) -> QfexExec {
        QfexExec {
            auth,
            rpc_timeout,
            probe,
            phase: Phase::Closed,
            opens: 0,
            armed: None,
            arm_rate_refused: false,
            in_flight: Vec::new(),
            counts: QfexExecCounts::default(),
            last: None,
            rest,
            placed: Vec::new(),
            ended: Vec::new(),
            resynced: false,
            latest_fill_secs: None,
            resync: None,
            last_resync: None,
        }
    }

    /// The probe mode's settings, `None` for the recording-only codec.
    pub fn probe_settings(&self) -> Option<Probe> {
        self.probe
    }

    /// What it counted so far.
    pub fn counts(&self) -> QfexExecCounts {
        self.counts
    }

    /// The last private state frame the recording-only codec read whole, before that frame
    /// ended its epoch; a probe codec decodes its frames instead, and leaves this `None`.
    pub fn last_private(&self) -> Option<&PrivateState> {
        self.last.as_ref()
    }

    /// What the last resync showed, whether it pushed its `ResyncEnd` or its epoch ended first:
    /// decision 0132 item 5's evidence for the probe.
    pub fn last_resync(&self) -> Option<&ResyncEvidence> {
        self.last_resync.as_ref()
    }

    /// The current epoch's authentication deadline.
    fn deadline(&self) -> TimerTag {
        TimerTag(AUTH_TIMER | u64::from(self.opens))
    }

    /// The journal's spans of a frame: decision 0134's for one that can echo a request (one
    /// span, whole, for a binary one), as 0132 item 3 and 0134 item 7 narrow them once the
    /// epoch's arm was sent, and the account's (`TradeAuth::inbound_spans`) for the rest.
    fn spans(&self, frame: RawFrame<'_>) -> Vec<Range<u32>> {
        let account = self.auth.inbound_spans(Inbound::Frame(frame));
        match (self.armed, frame) {
            (Some(arm), RawFrame::Text(text)) if rate::can_echo(frame.bytes()) => {
                let only = matches!(self.in_flight.as_slice(), [(rpc, Sent::Arm)] if *rpc == arm)
                    && !self.resync_reading();
                echo::armed_spans(text, account.body(), only)
            }
            _ => frame_spans(account.body(), frame),
        }
    }

    /// Asks to end the epoch, and reads nothing more of it: a resync in flight ends unfinished.
    fn end(&mut self, fx: &mut Effects, reason: &'static str) {
        self.phase = Phase::Closed;
        self.drop_resync();
        fx.push(Effect::Reconnect {
            stream: TRADE,
            reason,
        });
    }

    fn undecodable(&mut self, fx: &mut Effects, what: &'static str) -> Result<(), DecodeError> {
        self.counts.undecodable += 1;
        self.end(fx, "an undecodable QFEX trade frame");
        Err(DecodeError::Malformed(what))
    }

    /// A budget-only hold (0126): it answers and resends nothing.
    fn hold(&mut self, op: Option<OpKind>, retry_after: Option<Duration>, fx: &mut Effects) {
        self.counts.rate_holds += 1;
        fx.push(Effect::RateLimited {
            stream: TRADE,
            charge: op.map(|op| RateCharge::one(op, None)),
            retry_after,
        });
    }

    /// Whether this codec refuses every place, batch place and amend unsent, whatever the epoch
    /// and the order: the probe under `production_probe`, loopback origins included, until a
    /// record admits places on production (FBC-5d7x, decision 0145).
    fn refuses_places(&self) -> bool {
        self.probe
            .is_some_and(|p| p.environment() == Environment::Production)
    }

    /// The request the probe's arm is sent as: `on`, in the probe, on an epoch that has not
    /// armed yet (decision 0132 item 1). Every disarm, a later arm on the epoch, and any arm of
    /// the recording-only codec are refused `Unsupported`, unsent.
    fn arm(&self, on: bool) -> Result<order::Request, NotSentReason> {
        if self.probe.is_none() || !on || self.armed.is_some() {
            return Err(NotSentReason::Unsupported);
        }
        Ok(order::Request {
            frame: ARM.as_bytes().to_vec(),
            charge: RateCharge::one(OpKind::Control, None),
            target: None,
        })
    }

    /// The epoch's arm, when `top`, the frame as the journal keeps it, is the arm's own echo
    /// (`echo::arm_echo`) and the arm is still in flight on this authenticated epoch: neither
    /// answered, refused for rate nor past its deadline (decision 0132 item 1). Decided on the
    /// kept bytes, so replay answers the arm where live did.
    fn arm_echoed(&self, top: &[Member<'_>]) -> Option<RpcId> {
        let rpc = self.armed.filter(|_| !self.arm_rate_refused)?;
        let in_flight = self
            .in_flight
            .iter()
            .any(|(sent, what)| *sent == rpc && matches!(what, Sent::Arm));
        (self.phase == Phase::Authenticated && in_flight && echo::arm_echo(top).is_some())
            .then_some(rpc)
    }

    /// Answers the request `rpc`, once: the runtime clears its deadline on the answer
    /// (`ExecEvent::answers`), so it leaves `in_flight` here.
    fn answer(
        &mut self,
        rpc: RpcId,
        item: Option<ItemRef>,
        outcome: SubmitOutcome,
        sink: &mut dyn ExecSink,
    ) {
        self.in_flight.retain(|(sent, _)| *sent != rpc);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    /// Settles the cancel in flight naming `order` with `outcome`, when it is the one request
    /// in flight that names the order (decision 0132 item 8): the venue's order id compared
    /// exactly, nothing matched by arrival order. A reply naming an order that more than one
    /// request in flight names, a cancel and a query of it included, settles nothing and is
    /// counted; one naming only a query, or nothing in flight, settles nothing. Whether it
    /// settled.
    fn settle_cancel(
        &mut self,
        order: &VenueOrderId,
        outcome: SubmitOutcome,
        sink: &mut dyn ExecSink,
    ) -> bool {
        let naming: Vec<(RpcId, bool)> = self
            .in_flight
            .iter()
            .filter_map(|(rpc, sent)| match sent {
                Sent::Cancel(vid) if vid == order => Some((*rpc, true)),
                Sent::Query(vid) if vid == order => Some((*rpc, false)),
                _ => None,
            })
            .collect();
        match naming.as_slice() {
            [(rpc, true)] => {
                let item = ItemRef {
                    idx: 0,
                    cid: None,
                    vid: Some(order.clone()),
                };
                self.answer(*rpc, Some(item), outcome, sink);
                true
            }
            [_, _, ..] => {
                self.counts.replies_naming_several += 1;
                false
            }
            _ => false,
        }
    }

    /// An err: the arm's rejection when it echoes the arm in flight (`arm`) with a code other
    /// than `RateLimited` (decision 0132 item 1, as 0147 item 1 reads 0134 item 5), decided
    /// before the err is read as every err is, which may end the epoch. The rejection carries
    /// the documented code alone: the message is a span (0132 item 3), which the codec reads
    /// as absent (0134 item 5). A `RateLimited` one settles nothing, and leaves the arm to fail
    /// at its deadline (0132 item 1): no later echo answers it.
    fn on_err_frame(
        &mut self,
        err: echo::Err<'_>,
        arm: Option<RpcId>,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) {
        if arm.is_some() && err.code == Some(ErrCode::RateLimited) {
            self.arm_rate_refused = true;
        } else if let Some(rpc) = arm {
            self.counts.arm_rejected += 1;
            let reject = Reject {
                kind: RejectKind::Other,
                venue_code: err.code.map(|code| code.wire().into()),
                raw: Arc::from(""),
            };
            self.answer(rpc, None, SubmitOutcome::Rejected(reject), sink);
        }
        self.on_err(err, fx);
    }

    fn on_err(&mut self, err: echo::Err<'_>, fx: &mut Effects) {
        match err.code {
            Some(code) => self.counts.errs[code.index()] += 1,
            None => self.counts.undocumented_errs += 1,
        }
        match err.code {
            Some(ErrCode::RateLimited) => self.hold(
                err.echo_type.and_then(rate::operation),
                err.message.and_then(rate::hint),
                fx,
            ),
            _ if self.phase != Phase::Authenticated => {
                self.end(fx, "a QFEX err before the stream was authenticated");
            }
            Some(code) if !code.ends_session() => {}
            Some(_) => self.end(fx, "a QFEX err about the session"),
            None => self.end(fx, "an undocumented QFEX err"),
        }
    }

    fn on_auth_reply(&mut self, top: &[Member<'_>], fx: &mut Effects) {
        if !auth_success(top) {
            return self.end(fx, "QFEX did not authenticate the stream");
        }
        // Not yet reported: that waits for the last confirmation.
        self.phase = Phase::Subscribing(0);
        for channel in CHANNELS {
            let (frame, charge, class) = subscription(channel);
            fx.push(Effect::Send {
                stream: TRADE,
                frame,
                rpc: None,
                class,
                charge,
            });
        }
    }

    /// QFEX confirmed the subscription of `CHANNELS[at]`. The confirmation that completes the
    /// epoch's three reports the stream authenticated; a repeat, or one once reported, changes
    /// nothing.
    fn confirmed(&mut self, at: usize, sink: &mut dyn ExecSink) {
        self.counts.subscribed += 1;
        let Phase::Subscribing(mask) = self.phase else {
            return;
        };
        let mask = mask | 1 << at;
        if mask != ALL_CONFIRMED {
            self.phase = Phase::Subscribing(mask);
            return;
        }
        self.phase = Phase::Authenticated;
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Conn {
                stream: TRADE,
                state: ConnState::Authenticated,
            },
        );
    }

    /// A frame past the auth reply, whether or not every subscription is confirmed yet.
    fn on_private(
        &mut self,
        top: &[Member<'_>],
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if let Some((name, at)) = channel_reply(top) {
            match name {
                "subscribed" => {
                    self.confirmed(at, sink);
                    return Ok(());
                }
                "unsubscribed" => {
                    self.counts.unsubscribed += 1;
                    self.end(fx, "QFEX unsubscribed a private channel");
                    return Ok(());
                }
                _ => {}
            }
        }
        if let [(name, value)] = top
            && name == "order_response"
            && value
                .members()
                .and_then(|order| json::get(order, "status"))
                .and_then(Node::str)
                == Some("RATE_LIMITED")
        {
            // The order's state echoes no request, so it names no bucket: hold them all.
            self.hold(None, None, fx);
            return Ok(());
        }
        if self.probe.is_some() {
            if let Some(read) = self.on_resync_page(top, scope, specs, sink, fx) {
                return read;
            }
            return self.on_decoded(top, scope, specs, sink, fx);
        }
        let Some(read) = private::inspect(top, scope) else {
            self.end(fx, "an unrecognised QFEX trade frame");
            return Err(DecodeError::Malformed("unrecognised QFEX trade frame"));
        };
        self.counts.private_state += 1;
        // Recording-only, private state is not normalized, and ends the epoch (FBC-zeb0).
        self.end(fx, "QFEX private state is not normalized yet");
        let state = read.map_err(|_| DecodeError::Malformed("invalid QFEX private state"))?;
        self.last = Some(state);
        Ok(())
    }

    /// A private state frame in a probe mode (FBC-jxau, decision 0132 items 5 and 8, decision
    /// 0143 item 4), decoded into its events with the epoch kept open: a frame not as
    /// documented is a decode error, and only one the codec does not recognise ends the epoch.
    /// A `CANCELLED` order settles the cancel naming it before its update is pushed, and a
    /// `NO_SUCH_ORDER` settles it and is no update; nothing settles a place, which ends
    /// `Unknown` at its deadline (item 8).
    fn on_decoded(
        &mut self,
        top: &[Member<'_>],
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let Some(read) = private::decode(top, scope, specs) else {
            self.end(fx, "an unrecognised QFEX trade frame");
            return Err(DecodeError::Malformed("unrecognised QFEX trade frame"));
        };
        self.counts.private_state += 1;
        let decoded = match read {
            Ok(decoded) => decoded,
            Err(Misread::Status) => {
                self.counts.unmapped_statuses += 1;
                return Err(DecodeError::Malformed("unmapped QFEX order status"));
            }
            Err(Misread::Shape) => {
                return Err(DecodeError::Malformed("invalid QFEX private state"));
            }
        };
        match decoded {
            Decoded::Order { update, status } => {
                self.track_update(&update);
                if status == Status::Cancelled
                    && let Some(vid) = update.vid.clone()
                    && self.settle_cancel(&vid, accepted(), sink)
                {
                    self.counts.cancels_accepted += 1;
                }
                sink.push(VenueMeta::NONE, ExecEvent::Order(update));
            }
            Decoded::NoSuchOrder(vid) => {
                let reject = Reject {
                    kind: RejectKind::NotFound,
                    venue_code: Some("NO_SUCH_ORDER".into()),
                    raw: Arc::from("NO_SUCH_ORDER"),
                };
                if self.settle_cancel(&vid, SubmitOutcome::Rejected(reject), sink) {
                    self.counts.cancels_not_found += 1;
                }
            }
            Decoded::Fill { fill, secs } => {
                self.latest_fill_secs = self.latest_fill_secs.max(Some(secs));
                sink.push(VenueMeta::NONE, ExecEvent::Fill(fill));
            }
            Decoded::Position {
                inst,
                qty,
                avg_entry,
            } => sink.push(
                VenueMeta::NONE,
                ExecEvent::Position {
                    inst,
                    qty,
                    avg_entry,
                },
            ),
            Decoded::Page => self.counts.pages_unasked += 1,
            Decoded::Unheld => self.counts.unheld += 1,
        }
        Ok(())
    }
}

/// A request accepted at `AckLevel::Final`: the arm by its ack, a cancel by a `CANCELLED`
/// naming its order. Final, since fbc-oms ends a tombstone order only on a cancel accepted at
/// Final (decision 0132 items 1 and 8).
fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// The journal's spans of a frame off a trade socket with no arm sent on it: decision 0134's
/// for one that can echo a request (one span, whole, for a binary one), and `account`, the
/// spans the socket's authentication names (`TradeAuth::inbound_spans`, or the probe's account
/// reads', FBC-h3z1), for the rest.
pub(crate) fn frame_spans(account: &[Range<u32>], frame: RawFrame<'_>) -> Vec<Range<u32>> {
    if !rate::can_echo(frame.bytes()) {
        return account.to_vec();
    }
    match frame {
        RawFrame::Text(text) => echo::spans(text, account),
        RawFrame::Binary(bytes) => {
            let end = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            std::iter::once(0..end)
                .filter(|span| !span.is_empty())
                .collect()
        }
    }
}

/// The frame subscribing `channel`, one of [`CHANNELS`], as the AsyncAPI's `subscribe_orders`,
/// `subscribe_fills` and `subscribe_positions` show it, and what it is charged and sent as: the
/// rate page's 0.1 under `Subscribe`, Safety traffic. The codec sends one per channel on a
/// successful auth reply, and the probe's liveness hold the same three
/// (`account_reads::subscriptions`, decision 0146).
pub(crate) fn subscription(channel: &str) -> (WireSlice, RateCharge, TrafficClass) {
    let body = format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#);
    (
        WireSlice::plain(body.into_bytes()),
        RateCharge::one(OpKind::Subscribe, None),
        TrafficClass::Safety,
    )
}

/// A reply about one of [`CHANNELS`]: `top`, the members of a frame in none of the err and ack
/// envelopes, is one member whose value is a string naming a channel the codec subscribes (the
/// AsyncAPI's `{"subscribed":…}` and `{"unsubscribed":…}` replies to a subscribe and an
/// unsubscribe). The member's name, and the channel's place in [`CHANNELS`]. The codec and the
/// probe's liveness hold (`account_reads::confirmation`, decision 0146) read nothing else as one.
pub(crate) fn channel_reply<'t>(top: &'t [Member<'_>]) -> Option<(&'t str, usize)> {
    let [(name, value)] = top else {
        return None;
    };
    let channel = value.str()?;
    let at = CHANNELS.iter().position(|ours| *ours == channel)?;
    Some((name.as_ref(), at))
}

/// Whether `top`, the members of a frame in none of the err and ack envelopes, is QFEX's reply
/// that the stream is authenticated: the authenticate page's `{"type":"auth","result":"success"}`
/// (with no `err` beside it) or the AsyncAPI's `{"authenticated":true}`, its one member. The
/// codec and the probe's account reads (FBC-h3z1) take nothing else as success.
pub(crate) fn auth_success(top: &[Member<'_>]) -> bool {
    let text = |name: &str| json::get(top, name).and_then(Node::str);
    let documented = text("type") == Some("auth")
        && text("result") == Some("success")
        && json::get(top, "err").is_none();
    let observed = matches!(top, [(name, value)]
        if name == "authenticated" && matches!(value.kind, Kind::Literal("true")));
    documented || observed
}

impl ExecCodec for QfexExec {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        match call {
            CtxCall::Open(TRADE) => TradeAuth::NONCES,
            CtxCall::Resync if self.probe.is_some() => TradeAuth::REST_NONCES,
            _ => 0,
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens = self.opens.wrapping_add(1);
        self.phase = Phase::Closed;
        // A new connection: nothing sent on it, no arm (QFEX's flag is per connection), and no
        // resync; the orders placed, those kept ended and the latest fill time span epochs.
        self.drop_resync();
        self.resynced = false;
        self.armed = None;
        self.arm_rate_refused = false;
        self.in_flight.clear();
        if stream != TRADE {
            return;
        }
        let Ok(frame) = self.auth.frame(ctx) else {
            return self.end(fx, "QFEX authentication could not be signed");
        };
        self.phase = Phase::Awaiting;
        fx.push(Effect::Send {
            stream: TRADE,
            frame,
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Control, None),
        });
        fx.push(Effect::Timer {
            tag: self.deadline(),
            after: AUTH_DEADLINE,
        });
    }

    /// One frame for a command `order.rs` sends, or the probe's one arm per epoch, once the
    /// stream is authenticated; a command it refuses is refused for what it is, whatever the
    /// epoch, and under `production_probe` that is every place, batch place and amend, refused
    /// first, with nothing changed (decision 0145). QFEX signs no order and takes no nonce, so
    /// the receipt is empty.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let request = match cmd {
            VenueCommand::Place(_) | VenueCommand::PlaceBatch(_) | VenueCommand::Amend(_)
                if self.refuses_places() =>
            {
                return Err(NotSentReason::Unsupported);
            }
            VenueCommand::ArmCancelOnDisconnect(on) => self.arm(*on)?,
            _ => order::request(cmd, specs)?,
        };
        if self.phase != Phase::Authenticated {
            return Err(NotSentReason::Disconnected);
        }
        // One reply of a kind in flight: a query answers in the resync's pages' frame.
        if matches!(cmd, VenueCommand::Query(_)) && self.orders_paging() {
            return Err(NotSentReason::Backpressure);
        }
        self.record_place(cmd, ctx);
        let sent = match (cmd, request.target) {
            (VenueCommand::ArmCancelOnDisconnect(_), _) => {
                self.armed = Some(rpc);
                Sent::Arm
            }
            (VenueCommand::Cancel(_), Some(vid)) => Sent::Cancel(vid),
            (VenueCommand::Query(_), Some(vid)) => Sent::Query(vid),
            _ => Sent::Place,
        };
        self.in_flight.push((rpc, sent));
        fx.push(Effect::Send {
            stream: TRADE,
            frame: WireSlice::plain(request.frame),
            rpc: Some(RpcCall {
                id: rpc,
                timeout: self.rpc_timeout,
            }),
            class: cmd.traffic_class(),
            charge: request.charge,
        });
        Ok(EncodeReceipt::new())
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        frame: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        // What the journal keeps of the frame, and so all replay can read (0134 item 5).
        let clear = json::blank(frame.bytes(), &self.spans(frame));
        let text = match (stream, frame) {
            (TRADE, RawFrame::Text(_)) => String::from_utf8(clear).ok(),
            _ => None,
        };
        let Some(root) = text.as_deref().and_then(json::parse) else {
            return self.undecodable(fx, NOT_AN_OBJECT);
        };
        let Some(top) = root.members() else {
            return self.undecodable(fx, NOT_AN_OBJECT);
        };
        if root.repeats() {
            return self.undecodable(fx, "QFEX trade frame repeats a member");
        }
        // Whether the frame is the arm's own echo, the arm in flight (decision 0132 item 1).
        let arm = self.arm_echoed(top);
        match echo::envelope(top) {
            Envelope::Ambiguous => self.undecodable(fx, "QFEX trade frame is two envelopes"),
            Envelope::FlatErr(body) | Envelope::NestedErr(body) => {
                self.on_err_frame(echo::err(body), arm, sink, fx);
                Ok(())
            }
            Envelope::Ack(_) => {
                self.counts.acks += 1;
                if let Some(rpc) = arm {
                    self.counts.arm_accepted += 1;
                    self.answer(rpc, None, accepted(), sink);
                }
                Ok(())
            }
            Envelope::Other => match self.phase {
                Phase::Awaiting => {
                    self.on_auth_reply(top, fx);
                    Ok(())
                }
                Phase::Subscribing(_) | Phase::Authenticated => {
                    self.on_private(top, scope, specs, sink, fx)
                }
                Phase::Closed => {
                    self.end(fx, "a QFEX trade frame after the epoch ended");
                    Ok(())
                }
            },
        }
    }

    /// The resync's positions read (`resync.rs`); every other answer is refused.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.on_positions(tag, resp, scope, specs, sink, fx)
    }

    /// QFEX's authentication deadline, which runs from the opening until the last subscription
    /// is confirmed, and the resync's own: past either, the epoch ends.
    fn on_timer(&mut self, tag: TimerTag, _ctx: &EncodeCtx, fx: &mut Effects) {
        if self.on_resync_timer(tag, fx) || tag != self.deadline() {
            return;
        }
        match self.phase {
            Phase::Awaiting => self.end(fx, "QFEX authentication unanswered within 60 s"),
            Phase::Subscribing(_) => {
                self.end(fx, "QFEX private subscriptions unconfirmed within 60 s");
            }
            Phase::Closed | Phase::Authenticated => {}
        }
    }

    /// A sent command no reply answered ends `Unknown` at its deadline: every place, and the
    /// probe's arm and cancels when nothing answered them.
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.in_flight.retain(|(sent, _)| *sent != rpc);
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Outcome {
                rpc,
                item: None,
                outcome: SubmitOutcome::Unknown,
            },
        );
    }

    /// In a probe mode, decision 0132 item 5's resync (`resync.rs`): its positions read, signed
    /// with the two nonces `nonces_for` asks, its first page and its own deadline. Recording-only,
    /// the epoch ends, since a session must never believe it resynced.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.start_resync(ctx, fx);
    }

    /// A frame's spans as [`QfexExec`]'s `spans` gives them; an HTTP answer's, the account's
    /// (`TradeAuth::inbound_spans`, the positions' `balance` values among them), or its whole
    /// body for a status outside 2xx, which nothing decodes.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Frame(frame) => InboundSpans::frame(self.spans(frame)),
            Inbound::Http(_, resp) if !account_reads::success(resp.status) => {
                InboundSpans::response(Vec::new(), account_reads::whole(resp.body))
            }
            Inbound::Http(..) => self.auth.inbound_spans(input),
        }
    }
}
