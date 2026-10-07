//! Paradex's read-only private-stream codec (FBC-dly, decision 0061): an [`ExecCodec`] that
//! logs in, authenticates one WebSocket, subscribes the account's private channels and decodes
//! what they carry, and refuses every command. It is design §10.2's calibration session, which
//! reads an account's orders and fills and sends no order method, and the first half of the
//! order-entry codec (FBC-xvf).
//!
//! - **The login.** The first `on_open` asks for a login ([`LoginCycle`], src/auth): `POST
//!   /auth` signed at the context's time. Its token goes into one JSON-RPC `auth` frame
//!   ([`SessionToken::ws_frame`]), the token inside the frame's redaction span (docs.paradex.trade
//!   WebSocket "Authentication": `{"jsonrpc":"2.0","method":"auth","params":{...},"id":0}`).
//! - **Authenticated.** The venue's acknowledgement of that frame (a reply with a `result`) is
//!   reported as [`ConnState::Authenticated`], and the four private channels are then
//!   subscribed, once each on the connection ([`PRIVATE_CHANNELS`]). The subscriptions also keep
//!   Paradex from closing the socket for having none (close 4031, met by the Java library on
//!   2026-09-23).
//! - **The refresh.** Every login answer, refused or not, sets the refresh timer for the
//!   interval the consumer configures; its firing logs in again. Paradex keeps a connection
//!   authenticated for its lifetime ("After the initial authentication, you do not need to
//!   re-authenticate"), so a new token is not sent on the open socket: it authenticates the
//!   next connection, whose `on_open` sends the auth frame with it at once, as the Java
//!   library's order socket takes the latest refreshed token when it connects. A refresh that
//!   gave no token, or an auth frame the venue refused, leaves no token to reuse, so the next
//!   connection logs in first. The timer follows the configured interval alone, never the
//!   token's bytes, which replay blanks (0028).
//! - **Refusals.** A login that gives no token for the connection waiting on it, a refused
//!   auth frame and a refused private channel each report the stream [`ConnState::Closed`] and
//!   ask for a reconnect (WebSocket "Error Handling": on an authentication error, log in
//!   again and reconnect); a refusal the venue sent is reported first as
//!   an [`ExecEvent::UncorrelatedError`] with its code. A login that cannot be signed asks for
//!   the reconnect alone, since `on_open` has no sink.
//! - **Decoding.** Binary frames are the SBE templates of schema 1:2 (0054): `OrderEvent`,
//!   `FillEvent`, `PositionEvent` and `AccountEvent` through their decoders; a heartbeat and any
//!   template not decoded are skipped, as the schema's versioning policy requires. Text frames
//!   are the JSON-RPC replies to the codec's own requests.
//! - **Nothing order-affecting.** Every frame the codec writes is one of the two methods of
//!   [`ReadMethod`], which has no variant for an order; `encode` refuses every command
//!   `NotSent(Unsupported)` with no effect, and no call asks for a nonce.
//! - **Not built here.** `resync` asks for nothing: the REST resync with the token in a
//!   redacted header is the full codec's (FBC-xvf). No client ping is sent: Paradex pings every
//!   55 seconds and the WebSocket layer answers, and an order-entry codec cannot ask the runtime
//!   for a WebSocket ping (0056); a configured client ping is FBC-jkly's.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use fbc_core::{
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, Reject, RejectKind, RpcId, Secrets,
    SpecTable, StreamId, SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueConfig,
    VenueError, VenueMeta, WireSlice,
};
use serde_json::{Value, json};

use crate::auth::{Login, LoginCycle, token_spans};
use crate::md::sbe::Message;

use super::{
    TEMPLATE_ACCOUNT, TEMPLATE_FILL, TEMPLATE_ORDER, TEMPLATE_POSITION, decode_account_event,
    decode_fill_event, decode_order_event, decode_position_event,
};

/// The tag of every login this codec asks for.
pub const LOGIN_REQUEST: HttpTag = HttpTag(1);
/// The tag of the timer whose firing logs in again.
pub const REFRESH_TIMER: TimerTag = TimerTag(1);

/// The private channels subscribed on every authenticated connection, once each, as Paradex
/// names them (docs.paradex.trade WebSocket channels `orders.{market_symbol}`,
/// `fills.{market_symbol}`, `positions` and `account`; `ALL` for every market, as the Java
/// library subscribes them).
pub const PRIVATE_CHANNELS: [&str; 4] = ["orders.ALL", "fills.ALL", "positions", "account"];

/// The only JSON-RPC methods this codec writes. Neither names an order: the type cannot
/// express an order-affecting command, so nothing this codec sends can place, amend or cancel.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum ReadMethod {
    /// `auth`, carrying the session token.
    Auth,
    /// `subscribe` to one of [`PRIVATE_CHANNELS`].
    Subscribe(&'static str),
}

/// What one connection has done so far.
#[derive(Debug)]
struct Conn {
    stream: StreamId,
    /// The JSON-RPC id of the auth frame sent on it, until its reply; `None` before it is sent
    /// and after its reply.
    auth: Option<u64>,
    /// Whether an auth frame was sent on it: a later login answer is a refresh.
    auth_sent: bool,
    /// The subscribe requests sent on it and not yet answered, by JSON-RPC id.
    subscribing: BTreeMap<u64, &'static str>,
}

/// Paradex's read-only private-stream codec (module documentation). Its `Debug` shows no
/// account, key or token.
pub struct ReadOnlyExec {
    cycle: LoginCycle,
    refresh: Duration,
    /// The connection now open, from `on_open`.
    conn: Option<Conn>,
    /// Whether the token the cycle holds may authenticate the next connection: set by a login
    /// that gave one, cleared by a refresh that gave none and by a refused auth frame.
    reuse: bool,
    next_id: u64,
}

impl fmt::Debug for ReadOnlyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOnlyExec")
            .field("refresh", &self.refresh)
            .field("conn", &self.conn)
            .field("reuse", &self.reuse)
            .finish_non_exhaustive()
    }
}

impl ReadOnlyExec {
    /// The codec for the account `creds` hold, under `cfg` (src/auth's keys: the REST base,
    /// chain id, signature lifetime, refresh interval and request timeout). Refused naming a
    /// missing or invalid key, never a value.
    pub fn new(cfg: &VenueConfig, creds: Secrets) -> Result<ReadOnlyExec, VenueError> {
        let login = Login::new(cfg, creds)?;
        let refresh = login.refresh_interval();
        Ok(ReadOnlyExec {
            cycle: LoginCycle::new(login, LOGIN_REQUEST, REFRESH_TIMER),
            refresh,
            conn: None,
            reuse: false,
            next_id: 1,
        })
    }

    /// The next JSON-RPC id: never repeated over the codec's life, so a reply names one request.
    fn id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Writes `method` on the open connection. An auth frame needs the token the cycle holds;
    /// without one, or without a connection, nothing is written.
    fn send(&mut self, method: ReadMethod, fx: &mut Effects) {
        let Some(stream) = self.conn.as_ref().map(|c| c.stream) else {
            return;
        };
        let id = self.id();
        let (frame, op) = match method {
            ReadMethod::Auth => {
                let Some(token) = self.cycle.token() else {
                    return;
                };
                (token.ws_frame(id), OpKind::Control)
            }
            ReadMethod::Subscribe(channel) => {
                let text = json!({
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": { "channel": channel },
                    "id": id,
                });
                (
                    WireSlice::plain(text.to_string().into_bytes()),
                    OpKind::Subscribe,
                )
            }
        };
        let conn = self.conn.as_mut().expect("checked above");
        match method {
            ReadMethod::Auth => {
                conn.auth = Some(id);
                conn.auth_sent = true;
            }
            ReadMethod::Subscribe(channel) => {
                conn.subscribing.insert(id, channel);
            }
        }
        // Authentication and the private channels are what the session needs before anything
        // else, so they ride the safety floor.
        fx.push(Effect::Send {
            stream,
            frame,
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(op, None),
        });
    }

    /// Reports the open connection closed and asks for it again. `refusal`, the venue's, is
    /// reported first. No token is reused after a refusal.
    fn close(
        &mut self,
        refusal: Option<Reject>,
        reason: &'static str,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) {
        self.reuse = false;
        let Some(conn) = self.conn.take() else {
            return;
        };
        if let Some(reject) = refusal {
            sink.push(VenueMeta::NONE, ExecEvent::UncorrelatedError(reject));
        }
        let state = ConnState::Closed;
        let stream = conn.stream;
        sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
        fx.push(Effect::Reconnect { stream, reason });
    }

    /// A JSON-RPC reply: the auth frame's, or a subscribe's.
    fn on_text(
        &mut self,
        text: &str,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let reply: Value = serde_json::from_str(text)
            .map_err(|_| DecodeError::Malformed("text frame is not JSON"))?;
        let id = reply.get("id").and_then(Value::as_u64);
        let refusal = match (reply.get("result"), reply.get("error")) {
            (Some(result), None) if !result.is_null() => None,
            (None, Some(error)) => Some(reject(error)?),
            _ => {
                return Err(DecodeError::Malformed(
                    "text frame is neither a reply nor an error",
                ));
            }
        };
        let conn = self
            .conn
            .as_mut()
            .ok_or(DecodeError::Malformed("a reply with no connection open"))?;
        let id = id.ok_or(DecodeError::Malformed("a reply without an id"))?;
        if conn.auth == Some(id) {
            conn.auth = None;
            match refusal {
                None => {
                    let (stream, state) = (conn.stream, ConnState::Authenticated);
                    sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
                    for channel in PRIVATE_CHANNELS {
                        self.send(ReadMethod::Subscribe(channel), fx);
                    }
                }
                Some(reject) => self.close(Some(reject), "Paradex refused the auth", sink, fx),
            }
            return Ok(());
        }
        if conn.subscribing.remove(&id).is_none() {
            return Err(DecodeError::Malformed("a reply to no request sent"));
        }
        if let Some(reject) = refusal {
            let reason = "Paradex refused a private channel";
            self.close(Some(reject), reason, sink, fx);
        }
        Ok(())
    }
}

/// A JSON-RPC `error` object as a refusal: its `code` (a number) and `message`.
fn reject(error: &Value) -> Result<Reject, DecodeError> {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .ok_or(DecodeError::Malformed("error code"))?;
    let message = error.get("message").and_then(Value::as_str).unwrap_or("");
    Ok(Reject {
        kind: RejectKind::Other,
        venue_code: Some(code.to_string().into()),
        raw: message.into(),
    })
}

impl ExecCodec for ReadOnlyExec {
    /// None: the login signs a timestamp, not a nonce, and nothing else is signed.
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// With a token a login gave and nothing since refused, the auth frame at once and the
    /// refresh timer, since a timer of an earlier connection fires into nothing (0056);
    /// otherwise the login. A login that cannot be signed asks for the connection again.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.conn = Some(Conn {
            stream,
            auth: None,
            auth_sent: false,
            subscribing: BTreeMap::new(),
        });
        if self.reuse && self.cycle.token().is_some() {
            self.send(ReadMethod::Auth, fx);
            fx.push(Effect::Timer {
                tag: REFRESH_TIMER,
                after: self.refresh,
            });
        } else if self.cycle.start(ctx, fx).is_err() {
            let reason = "the Paradex login could not be signed";
            fx.push(Effect::Reconnect { stream, reason });
        }
    }

    /// Refused: this codec sends no command.
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
        _stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let frame = match f {
            RawFrame::Text(text) => return self.on_text(text, sink, fx),
            RawFrame::Binary(frame) => frame,
        };
        match Message::parse(frame)?.header().template_id {
            TEMPLATE_ORDER => decode_order_event(frame, scope, specs, sink),
            TEMPLATE_FILL => decode_fill_event(frame, scope, specs, sink),
            TEMPLATE_POSITION => decode_position_event(frame, scope, specs, sink),
            TEMPLATE_ACCOUNT => decode_account_event(frame, scope, sink),
            // Heartbeats, and every template not decoded here, are skipped.
            _ => Ok(()),
        }
    }

    /// A login's answer. The connection waiting on it gets the auth frame, or is closed and
    /// asked for again when no token came; for a connection already authenticating, it is a
    /// refresh: a token is kept for the next connection, and none means the next one logs in.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if tag != LOGIN_REQUEST {
            return Err(DecodeError::Malformed("an answer to no request asked"));
        }
        let answered = self.cycle.on_answer(resp, fx).is_ok();
        self.reuse = answered;
        let waiting = self.conn.as_ref().is_some_and(|c| !c.auth_sent);
        match (waiting, answered) {
            (true, true) => self.send(ReadMethod::Auth, fx),
            (true, false) => self.close(None, "the Paradex login gave no token", sink, fx),
            (false, _) => {}
        }
        Ok(())
    }

    /// The refresh timer: the next login. One that cannot be signed sets the timer again
    /// ([`LoginCycle::on_timer`]), leaving the connection as it is.
    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        if tag == REFRESH_TIMER {
            // The cycle has set the timer again; the token held stays until a login replaces it.
            let _ = self.cycle.on_timer(ctx, fx);
        }
    }

    /// No request this codec sends has an rpc; one the runtime names anyway is `Unknown`.
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let outcome = SubmitOutcome::Unknown;
        let event = ExecEvent::Outcome {
            rpc,
            item: None,
            outcome,
        };
        sink.push(VenueMeta::NONE, event);
    }

    /// Nothing: the REST resync is the full codec's (FBC-xvf).
    fn resync(&mut self, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    /// A login answer's token ([`token_spans`]); the frames carry none (the auth frame's reply
    /// does not echo the token).
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Http(_, resp) => token_spans(&resp),
            Inbound::Frame(_) => InboundSpans::NONE,
        }
    }
}
