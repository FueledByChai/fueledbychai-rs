//! QFEX's trade WebSocket codec, recording-only (FBC-zeb0, BT-702): [`QfexExec`] authenticates
//! each connection, subscribes to the account's private channels, feeds every rate refusal back
//! to the shared provider-user budgets, and inspects the private state QFEX sends. It sends no
//! command and normalizes nothing: `caps.exec` stays `None`, readiness `Record`, and the factory
//! builds no order-entry codec (decisions 0117, 0122), so only a test or the probe builds this
//! one.
//!
//! - **Authentication.** On opening, the authentication frame from `src/auth` (its two nonces
//!   reserved for the open, `nonces_for`) and a timer for QFEX's deadline: the server closes a
//!   connection that has not authenticated within one minute
//!   ([authenticate](https://docs.qfex.com/websocket/channels/trade/authenticate)). The reply is
//!   the page's `{"type":"auth","result":"success"}` or the AsyncAPI's `{"authenticated":true}`
//!   ([trade.yaml](https://docs.qfex.com/websocket/trade.yaml)); anything else, or no reply when
//!   the deadline fires, ends the epoch. The deadline's tag names the epoch's opening, so a
//!   timer of an earlier epoch does nothing.
//! - **Subscriptions.** Once authenticated, `order_responses`, `fills` and `positions`, one
//!   frame each, as the AsyncAPI's `subscribe_orders`, `subscribe_fills` and
//!   `subscribe_positions` show; a `{"subscribed":…}` naming one of them keeps the epoch open.
//!   When the stream is reported authenticated is FBC-bsou's to change (0132 item 1).
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
//!   each number exactly ([`PrivateState`]), and ends the epoch with no order, fill, position or
//!   resync event: nothing is normalized until FBC-jxau. A resync asks to reconnect.
//!
//! Each frame and its rate charge: the authentication as one tenth of a weight unit under
//! `Control` (the rate page lists no weight for it, and `Control` charges both buckets), each
//! subscription as the page's 0.1 under `Subscribe`, both Safety traffic.

mod echo;
mod json;
mod private;

use core::ops::Range;
use std::time::Duration;

use fbc_core::{
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, RpcId, SpecTable, StreamId,
    SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueMeta, WireSlice,
};

use self::echo::Envelope;
use self::json::{Kind, Member, Node};
use crate::auth::TradeAuth;
use crate::rate;

pub use self::echo::ErrCode;
pub use self::private::{OrderStatus, PrivateState};

/// The trade WebSocket, the codec's one stream.
pub const TRADE: StreamId = StreamId(0);

/// The private channels subscribed once authenticated, in this order.
pub const CHANNELS: [&str; 3] = ["order_responses", "fills", "positions"];

/// QFEX's authentication deadline, from the authenticate page.
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
    /// `subscribed` replies naming one of [`CHANNELS`].
    pub subscribed: u64,
    /// Private state frames inspected, valid or not; each ended its epoch.
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
    /// The stream was reported authenticated.
    Authenticated,
}

/// The recording-only order-entry codec for one QFEX account's trade WebSocket.
pub struct QfexExec {
    auth: TradeAuth,
    phase: Phase,
    /// How many times a stream opened: the current epoch's number.
    opens: u32,
    counts: QfexExecCounts,
    last: Option<PrivateState>,
}

impl QfexExec {
    /// The codec for the account `auth` authenticates as.
    pub fn new(auth: TradeAuth) -> QfexExec {
        QfexExec {
            auth,
            phase: Phase::Closed,
            opens: 0,
            counts: QfexExecCounts::default(),
            last: None,
        }
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
    /// span, whole, for a binary one), the account's (`TradeAuth::inbound_spans`) for the rest.
    fn spans(&self, frame: RawFrame<'_>) -> Vec<Range<u32>> {
        let account = self.auth.inbound_spans(Inbound::Frame(frame));
        if !rate::can_echo(frame.bytes()) {
            return account.body().to_vec();
        }
        match frame {
            RawFrame::Text(text) => echo::spans(text, account.body()),
            RawFrame::Binary(bytes) => {
                let end = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
                std::iter::once(0..end)
                    .filter(|span| !span.is_empty())
                    .collect()
            }
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

    fn on_auth_reply(&mut self, top: &[Member<'_>], sink: &mut dyn ExecSink, fx: &mut Effects) {
        let text = |name: &str| json::get(top, name).and_then(Node::str);
        let documented = text("type") == Some("auth")
            && text("result") == Some("success")
            && json::get(top, "err").is_none();
        let observed = matches!(top, [(name, value)]
            if name == "authenticated" && matches!(value.kind, Kind::Literal("true")));
        if !(documented || observed) {
            return self.end(fx, "QFEX did not authenticate the stream");
        }
        self.phase = Phase::Authenticated;
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Conn {
                stream: TRADE,
                state: ConnState::Authenticated,
            },
        );
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

    fn on_private(
        &mut self,
        top: &[Member<'_>],
        scope: &DecodeScope<'_>,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if let [(name, value)] = top
            && name == "subscribed"
            && value
                .str()
                .is_some_and(|channel| CHANNELS.contains(&channel))
        {
            self.counts.subscribed += 1;
            return Ok(());
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
        self.end(fx, "QFEX private state is not normalized yet");
        let state = read.map_err(|_| DecodeError::Malformed("invalid QFEX private state"))?;
        self.last = Some(state);
        Ok(())
    }
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

    /// Every command is refused unsent: the encoder is FBC-blcw's.
    fn encode(
        &mut self,
        _cmd: &VenueCommand,
        _rpc: RpcId,
        _specs: &SpecTable,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
        _fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        Err(NotSentReason::Unsupported)
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
                    self.on_auth_reply(top, sink, fx);
                    Ok(())
                }
                Phase::Authenticated => self.on_private(top, scope, fx),
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

    fn on_timer(&mut self, tag: TimerTag, _ctx: &EncodeCtx, fx: &mut Effects) {
        if self.phase == Phase::Awaiting && tag == self.deadline() {
            self.end(fx, "QFEX authentication unanswered within 60 s");
        }
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        sink.push(
            VenueMeta::NONE,
            ExecEvent::Outcome {
                rpc,
                item: None,
                outcome: SubmitOutcome::Unknown,
            },
        );
    }

    /// No resync can be read yet, and a session must never believe one was: end the epoch.
    fn resync(&mut self, _ctx: &EncodeCtx, fx: &mut Effects) {
        self.end(fx, "QFEX resync is not implemented: recording only");
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Frame(frame) => InboundSpans::frame(self.spans(frame)),
            Inbound::Http(..) => self.auth.inbound_spans(input),
        }
    }
}
