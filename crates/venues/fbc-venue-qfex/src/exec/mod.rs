//! QFEX's trade WebSocket codec (FBC-zeb0, BT-702): [`QfexExec`] authenticates each connection,
//! subscribes to the account's private channels, feeds every rate refusal back to the shared
//! provider-user budgets, inspects the private state QFEX sends, and encodes the v1 order
//! commands (FBC-blcw, `order.rs`). It normalizes nothing, and no reply settles a command.
//!
//! It runs in one of two modes:
//!
//! - **Recording-only** ([`QfexExec::new`]), what a test builds: it refuses the
//!   cancel-on-disconnect arm, and ends its epoch on every private state frame and on a resync.
//!   The factory never builds it: with `qfex.exec.mode` absent, `caps.exec` stays `None` and
//!   the factory builds no order-entry codec (decisions 0117, 0122).
//! - **The probe** ([`QfexExec::probe`], decision 0132 item 6, FBC-mfey), which the factory
//!   builds under `preprod_probe` or `production_probe` alone (decision 0143 item 2), its caps
//!   [`probe_exec_caps`] in both. Under `production_probe` alone, loopback origins included,
//!   it refuses every place, batch place and amend `NotSent(Unsupported)`, whatever the epoch
//!   and the order, though its caps declare places, until a record admits places on
//!   production (FBC-5d7x, decision 0145); in every other way it is alike in both. It encodes
//!   one cancel-on-disconnect arm per epoch, and refuses unsent every later arm on that epoch
//!   and every disarm (item 1), so at most one arm is in flight on a connection. No frame
//!   answers the arm yet: every arm ends `Unknown` at its deadline, and the session ends the
//!   epoch (decision 0058). The journal keeps the arm's echo and, while the arm is the only
//!   request in flight, the shape of every other echo (item 3, as 0134 item 7 narrows it;
//!   `echo.rs`), so a capture shows whatever answers the arm. Its epoch stays open on the
//!   private state frames stage one brings, which it still inspects and normalizes into
//!   nothing, and its resync asks for nothing, so the session places nothing (0058) until
//!   FBC-jxau lands item 5's resync.
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
//!   epoch. An ack is counted and settles nothing.
//! - **A frame that is not a JSON object**, a frame kept whole among them (which replay reads
//!   as a run of `2`s), is refused (`DecodeError::Malformed`) and ends the epoch, in every state
//!   (0134 item 6). So does a frame that repeats a member or matches two envelopes.
//! - **Private state.** An `order_response` whose status is `RATE_LIMITED` holds every frame
//!   budget, as an echo naming no bucket does. Every other private state frame is inspected,
//!   each number exactly ([`PrivateState`]), with no order, fill, position or resync event:
//!   nothing is normalized until FBC-jxau. Recording-only, it ends the epoch, and a resync asks
//!   to reconnect; in the probe, the epoch stays open and a resync asks for nothing.
//! - **Commands.** A limit GTC, ALO (post-only) or limit IOC place, and a cancel or an order
//!   query by the venue's order UUID, each one frame naming its request with the consumer's
//!   deadline and labelled with its command's traffic class, sent only on an authenticated
//!   epoch (`NotSent(Disconnected)` otherwise); everything else is refused unsent (`order.rs`),
//!   and under `production_probe` every place too, before the epoch is read (decision 0145),
//!   but the probe's one arm per epoch, [`ARM`], charged as the rate page's 0.1 for
//!   `cancel_on_disconnect` under `Control`, which charges both buckets since the page names
//!   none for it.
//!   No reply settles a request, whatever it names: an `order_response`, a fill, a page, an err
//!   or an ack is read as above, and each sent request ends `Unknown` at its deadline. Reply
//!   correlation stays candidate-only (`tests/correlation.rs`) until a decision, on evidence,
//!   says otherwise.
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

use core::ops::Range;
use std::time::Duration;

use fbc_core::{
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, RpcCall, RpcId, SpecTable, StreamId,
    SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueMeta, WireSlice,
};

use self::echo::Envelope;
use self::json::{Kind, Member, Node};
use crate::auth::TradeAuth;
use crate::probe::{Environment, Probe};
use crate::rate;

pub use self::caps::{RISK_REJECT_WINDOW, probe_exec_caps};
pub use self::echo::{ARM, ErrCode};
pub use self::ids::{CLIENT_ID_FORMAT, is_venue_order_id};
pub(crate) use self::private::exact;
pub use self::private::{OrderStatus, PrivateState};

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
    /// Acks, each settling nothing.
    pub acks: u64,
    /// `Effect::RateLimited` holds asked for, by RateLimited errs and `RATE_LIMITED` orders.
    pub rate_holds: u64,
    /// `subscribed` replies naming one of [`CHANNELS`], repeats included.
    pub subscribed: u64,
    /// `unsubscribed` replies naming one of [`CHANNELS`]; each ended its epoch.
    pub unsubscribed: u64,
    /// Private state frames inspected, valid or not; each ended its epoch, but in the probe.
    pub private_state: u64,
    /// Frames refused as not a JSON object, a repeated member, or two envelopes at once.
    pub undecodable: u64,
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
    /// The request the current epoch's arm was encoded as, once it was.
    ///
    /// Both this and `in_flight` are set when a request is encoded, which is the last the codec
    /// hears of it before its deadline: a session that then finds no room in its buckets
    /// (`ExecSession`'s charge after `encode`) writes nothing and tells the codec nothing. A
    /// request refused there stays in `in_flight` until the epoch ends, which only closes the
    /// arm's window early (decision 0132 item 3), the cautious side. An arm refused there
    /// leaves `armed` set, so the frames that arrive before the epoch ends are journaled under
    /// the arm's rules though no arm was written; the session fails that epoch at once (its
    /// gate's `arm_not_sent`), so this lasts only until it closes. The session journals each
    /// encode's context before that charge, so a replay that re-runs the journaled encodes
    /// redacts alike. A runtime callback for an encoded request left unwritten would make both
    /// exact.
    armed: Option<RpcId>,
    /// The requests encoded on the current epoch whose deadline has not fired, in order: no
    /// reply settles one yet.
    in_flight: Vec<RpcId>,
    counts: QfexExecCounts,
    last: Option<PrivateState>,
}

impl QfexExec {
    /// The recording-only codec for the account `auth` authenticates as, each command it sends
    /// awaiting its reply for `rpc_timeout`, the consumer's request deadline. It refuses the
    /// cancel-on-disconnect arm, and the factory never builds it.
    pub fn new(auth: TradeAuth, rpc_timeout: Duration) -> QfexExec {
        QfexExec::with(auth, rpc_timeout, None)
    }

    /// The codec in a probe mode (decision 0132 item 6, decision 0143 item 2), alike in both but
    /// for `production_probe`'s refusal of every place, batch place and amend (decision 0145),
    /// for the account `auth` authenticates as, under the settings `probe` the mode's guard
    /// admitted ([`crate::probe::mode`]): each command it sends, its one arm per epoch included,
    /// awaits its reply for `qfex.exec.rpc_timeout`.
    pub fn probe(auth: TradeAuth, probe: Probe) -> QfexExec {
        QfexExec::with(auth, probe.rpc_timeout(), Some(probe))
    }

    fn with(auth: TradeAuth, rpc_timeout: Duration, probe: Option<Probe>) -> QfexExec {
        QfexExec {
            auth,
            rpc_timeout,
            probe,
            phase: Phase::Closed,
            opens: 0,
            armed: None,
            in_flight: Vec::new(),
            counts: QfexExecCounts::default(),
            last: None,
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

    /// The last private state frame it read whole, before that frame ended its epoch: what a
    /// later ticket normalizes, for the tests and the probe meanwhile.
    pub fn last_private(&self) -> Option<&PrivateState> {
        self.last.as_ref()
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
                echo::armed_spans(text, account.body(), self.in_flight == [arm])
            }
            _ => frame_spans(account.body(), frame),
        }
    }

    /// Asks to end the epoch, and reads nothing more of it.
    fn end(&mut self, fx: &mut Effects, reason: &'static str) {
        self.phase = Phase::Closed;
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
        })
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
            let body = format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#);
            fx.push(Effect::Send {
                stream: TRADE,
                frame: WireSlice::plain(body.into_bytes()),
                rpc: None,
                class: TrafficClass::Safety,
                charge: RateCharge::one(OpKind::Subscribe, None),
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
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        // `{"subscribed":…}` and `{"unsubscribed":…}`, each the frame's one member, naming one
        // of the channels the epoch subscribed (the AsyncAPI's replies to a subscribe and an
        // unsubscribe).
        if let [(name, value)] = top
            && let Some(at) = value
                .str()
                .and_then(|channel| CHANNELS.iter().position(|ours| *ours == channel))
        {
            match name.as_ref() {
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
        let Some(read) = private::inspect(top, scope) else {
            self.end(fx, "an unrecognised QFEX trade frame");
            return Err(DecodeError::Malformed("unrecognised QFEX trade frame"));
        };
        self.counts.private_state += 1;
        // The probe keeps its epoch open on what stage one brings: a position snapshot on
        // subscribing, an order the arm cancels (decision 0132 items 1 and 7).
        if self.probe.is_none() {
            self.end(fx, "QFEX private state is not normalized yet");
        }
        let state = read.map_err(|_| DecodeError::Malformed("invalid QFEX private state"))?;
        self.last = Some(state);
        Ok(())
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
            _ => 0,
        }
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.opens = self.opens.wrapping_add(1);
        self.phase = Phase::Closed;
        // A new connection: nothing sent on it, and no arm (QFEX's flag is per connection).
        self.armed = None;
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
        _ctx: &EncodeCtx,
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
        if matches!(cmd, VenueCommand::ArmCancelOnDisconnect(_)) {
            self.armed = Some(rpc);
        }
        self.in_flight.push(rpc);
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
        _specs: &SpecTable,
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
        match echo::envelope(top) {
            Envelope::Ambiguous => self.undecodable(fx, "QFEX trade frame is two envelopes"),
            Envelope::FlatErr(body) | Envelope::NestedErr(body) => {
                self.on_err(echo::err(body), fx);
                Ok(())
            }
            Envelope::Ack(_) => {
                self.counts.acks += 1;
                Ok(())
            }
            Envelope::Other => match self.phase {
                Phase::Awaiting => {
                    self.on_auth_reply(top, fx);
                    Ok(())
                }
                Phase::Subscribing(_) | Phase::Authenticated => {
                    self.on_private(top, scope, sink, fx)
                }
                Phase::Closed => {
                    self.end(fx, "a QFEX trade frame after the epoch ended");
                    Ok(())
                }
            },
        }
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "unexpected QFEX private HTTP response",
        ))
    }

    /// QFEX's authentication deadline, which runs from the opening until the last subscription
    /// is confirmed: past it, the epoch ends.
    fn on_timer(&mut self, tag: TimerTag, _ctx: &EncodeCtx, fx: &mut Effects) {
        if tag != self.deadline() {
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

    /// Every sent command ends `Unknown` at its deadline, the probe's arm included: no reply
    /// settles one.
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.in_flight.retain(|sent| *sent != rpc);
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Outcome {
                rpc,
                item: None,
                outcome: SubmitOutcome::Unknown,
            },
        );
    }

    /// No resync can be read yet, and a session must never believe one was. Recording-only,
    /// the epoch ends. In the probe it asks for nothing and pushes no `ResyncBegin` or
    /// `ResyncEnd`, so the session places and amends nothing on the epoch (decision 0058)
    /// while it stays open for whatever answers the arm, until FBC-jxau lands decision 0132
    /// item 5's resync and its own deadline.
    fn resync(&mut self, _ctx: &EncodeCtx, fx: &mut Effects) {
        if self.probe.is_none() {
            self.end(fx, "QFEX resync is not implemented: recording only");
        }
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Frame(frame) => InboundSpans::frame(self.spans(frame)),
            Inbound::Http(..) => self.auth.inbound_spans(input),
        }
    }
}
