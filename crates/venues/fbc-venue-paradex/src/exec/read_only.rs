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
//!   an [`ExecEvent::UncorrelatedError`] with its code. A reply to the auth frame or a
//!   subscribe is a refusal whenever it carries an `error`, beside a `result` too and whatever
//!   the error holds (its code when that is an integer), as the Java client fails the auth on
//!   any non-null error (`ParadexOrderWebSocketClient.onAuthResponse`); one with neither
//!   member closes the stream the same way with nothing reported first, so no connection
//!   waits silently on a reply it could not read (FBC-z5om). A login that cannot be signed
//!   asks for the reconnect alone, since `on_open` has no sink.
//! - **Decoding.** Binary frames are the SBE templates of schema 1:2 (0054): `OrderEvent`,
//!   `FillEvent`, `PositionEvent` and `AccountEvent` through their decoders; a heartbeat and any
//!   template not decoded are skipped, as the schema's versioning policy requires. Text frames
//!   are the JSON-RPC replies to the codec's own requests. A reply member that is JSON `null`
//!   is read as absent, as the order replies read it (FBC-cexu) and as the Java client reads
//!   the auth reply (`ParadexOrderWebSocketClient.onAuthResponse`): a `result` beside
//!   `"error": null` is that result, and an `error` beside `"result": null` is that error
//!   (FBC-4lp7).
//! - **Nothing order-affecting.** Every frame the codec writes is one of the two methods of
//!   [`ReadMethod`], which has no variant for an order; `encode` refuses every command
//!   `NotSent(Unsupported)` with no effect, and no call asks for a nonce.
//! - **The resync.** `resync` reads the open orders and positions over REST (FBC-0sc's
//!   [`resync_requests`]), each read carrying the current token in a redacted header
//!   ([`SessionToken::header`]) and none elsewhere, under tags of their own for that resync, so
//!   an answer to an earlier resync is never paired with a later one. The two answers decode
//!   whole into the resync events at the watermark `ctx.wall` ([`decode_resync`]) once both
//!   are in; a read that fails, is answered with an error status or does not decode pushes
//!   nothing and asks for the connection again, so the epoch ends rather than waiting on a
//!   resync that will never end (FBC-xvf, decision 0071). The read-only codec can thus seed
//!   positions as the order-entry codec does, once a session asks it to resync.
//! - **Not built here.** No client ping is sent: Paradex pings every 55 seconds and the
//!   WebSocket layer answers, and an order-entry codec cannot ask the runtime for a WebSocket
//!   ping (0056); a configured client ping is FBC-jkly's.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use fbc_core::{
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans,
    NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, Reject, RejectKind, RpcId, Secrets,
    SpecTable, StreamId, SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueConfig,
    VenueError, VenueMeta, WallNs, WireSlice,
};
use serde_json::{Value, json};

use crate::auth::{Login, LoginCycle, REST_URL, SessionToken, TIMEOUT, token_spans};
use crate::md::sbe::Message;

use super::reply::member;
use super::{
    ResyncTags, TEMPLATE_ACCOUNT, TEMPLATE_FILL, TEMPLATE_ORDER, TEMPLATE_POSITION,
    decode_account_event, decode_fill_event, decode_order_event, decode_position_event,
    decode_resync, resync_requests,
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
    /// Whether the venue acknowledged its auth frame.
    authenticated: bool,
}

/// A resync asked for and not yet decoded: its two reads' tags, its watermark, and the answers
/// in so far.
#[derive(Debug)]
struct Resync {
    tags: ResyncTags,
    watermark: WallNs,
    orders: Option<Vec<u8>>,
    positions: Option<Vec<u8>>,
}

/// The first tag a resync's or a query's read is given: [`LOGIN_REQUEST`] is the only one
/// below it.
const FIRST_READ_TAG: u64 = 2;

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
    /// The REST base (no trailing slash) and how long a read waits, as the login's.
    rest: String,
    timeout: Duration,
    /// The resync in flight, if any.
    resync: Option<Resync>,
    next_tag: u64,
}

impl fmt::Debug for ReadOnlyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOnlyExec")
            .field("refresh", &self.refresh)
            .field("conn", &self.conn)
            .field("reuse", &self.reuse)
            .field("resync", &self.resync.as_ref().map(|r| r.tags))
            .finish_non_exhaustive()
    }
}

impl ReadOnlyExec {
    /// The codec for the account `creds` hold, under `cfg` (src/auth's keys: the REST base,
    /// chain id, signature lifetime, refresh interval and request timeout). Refused naming a
    /// missing or invalid key, never a value.
    pub fn new(cfg: &VenueConfig, creds: Secrets) -> Result<ReadOnlyExec, VenueError> {
        ReadOnlyExec::with_first_id(cfg, creds, 1)
    }

    /// The codec, its JSON-RPC ids counted up from `first_id`: the order-entry codec keeps
    /// them apart from its requests' ids ([`CONTROL_IDS`](super::CONTROL_IDS)).
    pub(super) fn with_first_id(
        cfg: &VenueConfig,
        creds: Secrets,
        first_id: u64,
    ) -> Result<ReadOnlyExec, VenueError> {
        let login = Login::new(cfg, creds)?;
        let refresh = login.refresh_interval();
        let (rest, timeout) = rest_reads(cfg);
        Ok(ReadOnlyExec {
            cycle: LoginCycle::new(login, LOGIN_REQUEST, REFRESH_TIMER),
            refresh,
            conn: None,
            reuse: false,
            next_id: first_id,
            rest,
            timeout,
            resync: None,
            next_tag: FIRST_READ_TAG,
        })
    }

    /// The stream of the open connection once the venue acknowledged its auth frame.
    pub(super) fn authenticated(&self) -> Option<StreamId> {
        self.conn
            .as_ref()
            .filter(|c| c.authenticated)
            .map(|c| c.stream)
    }

    /// The token the latest login gave, which every REST read carries.
    pub(super) fn token(&self) -> Option<&SessionToken> {
        self.cycle.token()
    }

    /// The REST base, no trailing slash, and how long a read waits for its answer.
    pub(super) fn rest(&self) -> (&str, Duration) {
        (&self.rest, self.timeout)
    }

    /// A tag no read of this codec had before: never [`LOGIN_REQUEST`], never repeated.
    pub(super) fn read_tag(&mut self) -> HttpTag {
        let tag = HttpTag(self.next_tag);
        self.next_tag += 1;
        tag
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

    /// A JSON-RPC reply: the auth frame's, or a subscribe's. One that is neither a result nor a
    /// refusal closes the stream as a refusal does (module documentation), so a connection
    /// never waits on a reply it could not read; a reply naming no request this connection
    /// awaits is refused with nothing pushed.
    pub(super) fn on_text(
        &mut self,
        text: &str,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let reply: Value = serde_json::from_str(text)
            .map_err(|_| DecodeError::Malformed("text frame is not JSON"))?;
        let conn = self
            .conn
            .as_mut()
            .ok_or(DecodeError::Malformed("a reply with no connection open"))?;
        let id = reply
            .get("id")
            .and_then(Value::as_u64)
            .ok_or(DecodeError::Malformed("a reply without an id"))?;
        if conn.auth == Some(id) {
            conn.auth = None;
            match Reply::read(&reply) {
                Reply::Result => {
                    conn.authenticated = true;
                    let (stream, state) = (conn.stream, ConnState::Authenticated);
                    sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
                    for channel in PRIVATE_CHANNELS {
                        self.send(ReadMethod::Subscribe(channel), fx);
                    }
                }
                Reply::Refused(reject) => {
                    self.close(Some(reject), "Paradex refused the auth", sink, fx);
                }
                Reply::Unreadable => {
                    let reason = "a Paradex auth reply could not be read";
                    self.close(None, reason, sink, fx);
                }
            }
            return Ok(());
        }
        if conn.subscribing.remove(&id).is_none() {
            return Err(DecodeError::Malformed("a reply to no request sent"));
        }
        match Reply::read(&reply) {
            Reply::Result => {}
            Reply::Refused(reject) => {
                let reason = "Paradex refused a private channel";
                self.close(Some(reject), reason, sink, fx);
            }
            Reply::Unreadable => {
                let reason = "a Paradex subscribe reply could not be read";
                self.close(None, reason, sink, fx);
            }
        }
        Ok(())
    }
}

/// What a reply to the auth frame or a subscribe says.
enum Reply {
    /// A `result` and no `error`: the request was accepted.
    Result,
    /// An `error`, whatever else the reply holds: the venue refused the request.
    Refused(Reject),
    /// Neither member: nothing the codec can read.
    Unreadable,
}

impl Reply {
    /// Reads `reply`, a member that is JSON `null` as absent (FBC-4lp7). Any `error` is a
    /// refusal, beside a `result` too, as the Java client reads the auth reply
    /// (`ParadexOrderWebSocketClient.onAuthResponse`: any non-null `error` fails it).
    fn read(reply: &Value) -> Reply {
        match (member(reply, "result"), member(reply, "error")) {
            (_, Some(error)) => Reply::Refused(reject(error)),
            (Some(_), None) => Reply::Result,
            (None, None) => Reply::Unreadable,
        }
    }
}

/// A JSON-RPC `error` as a refusal: its `code` when it is an integer, none otherwise, and its
/// `message` (the error itself when it is a string).
fn reject(error: &Value) -> Reject {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .unwrap_or("");
    Reject {
        kind: RejectKind::Other,
        venue_code: code.map(|code| code.to_string().into()),
        raw: message.into(),
    }
}

/// The REST base, without a trailing slash, and the read timeout, from the keys
/// [`Login::new`] has already accepted (an `https://` base and a positive whole `<n>s` or
/// `<n>ms`), so neither is refused here.
fn rest_reads(cfg: &VenueConfig) -> (String, Duration) {
    let rest = cfg.get(REST_URL).unwrap_or_default();
    let text = cfg.get(TIMEOUT).unwrap_or_default();
    let timeout = match text.strip_suffix("ms") {
        Some(ms) => Duration::from_millis(ms.parse().unwrap_or_default()),
        None => Duration::from_secs(text.trim_end_matches('s').parse().unwrap_or_default()),
    };
    (rest.trim_end_matches('/').to_owned(), timeout)
}

impl ReadOnlyExec {
    /// Asks for the resync's two reads, each with the current token in a redacted header,
    /// under tags of their own; `ctx.wall` is its watermark. With no token, or a connection
    /// gone, nothing is read and the connection is asked for again.
    fn start_resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.resync = None;
        let Some(header) = self.token().map(SessionToken::header) else {
            self.drop_resync("no Paradex token for the resync", fx);
            return;
        };
        let tags = ResyncTags {
            orders: self.read_tag(),
            positions: self.read_tag(),
        };
        for effect in resync_requests(&self.rest, &[header], self.timeout, tags).take() {
            fx.push(effect);
        }
        self.resync = Some(Resync {
            tags,
            watermark: ctx.wall,
            orders: None,
            positions: None,
        });
    }

    /// Ends the resync in flight with nothing pushed, and asks for the open connection again
    /// with a fresh login, so the epoch ends instead of waiting on a resync that never ends.
    fn drop_resync(&mut self, reason: &'static str, fx: &mut Effects) {
        self.resync = None;
        self.reuse = false;
        if let Some(stream) = self.conn.as_ref().map(|c| c.stream) {
            fx.push(Effect::Reconnect { stream, reason });
        }
    }

    /// Whether `tag` is a read of the resync in flight.
    fn resync_read(&self, tag: HttpTag) -> bool {
        self.resync
            .as_ref()
            .is_some_and(|r| r.tags.orders == tag || r.tags.positions == tag)
    }

    /// One of the resync's two answers: held until the other is in, then both decoded whole
    /// and pushed. A failed, refused or undecodable read drops the resync ([`drop_resync`]).
    ///
    /// [`drop_resync`]: ReadOnlyExec::drop_resync
    fn on_resync_answer(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let body = match resp {
            Ok(resp) if (200..300).contains(&resp.status) => resp.body.to_vec(),
            _ => {
                self.drop_resync("a Paradex resync read failed", fx);
                return Ok(());
            }
        };
        let resync = self.resync.as_mut().expect("checked by resync_read");
        if tag == resync.tags.orders {
            resync.orders = Some(body);
        } else {
            resync.positions = Some(body);
        }
        let (Some(orders), Some(positions)) = (&resync.orders, &resync.positions) else {
            return Ok(());
        };
        match decode_resync(resync.watermark, orders, positions, scope, specs) {
            Ok(answer) => {
                self.resync = None;
                answer.push_into(sink);
                Ok(())
            }
            Err(err) => {
                self.drop_resync("a Paradex resync answer did not decode", fx);
                Err(err)
            }
        }
    }
}

impl ExecCodec for ReadOnlyExec {
    /// None: the login signs a timestamp, not a nonce, and nothing else is signed.
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// With a token a login gave and nothing since refused, the auth frame at once and the
    /// refresh timer, since a timer of an earlier connection fires into nothing (0056);
    /// otherwise the login. A login that cannot be signed asks for the connection again. A
    /// resync of an earlier connection is dropped: its answers come back only to the epoch that
    /// asked (0027).
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.resync = None;
        self.conn = Some(Conn {
            stream,
            auth: None,
            auth_sent: false,
            subscribing: BTreeMap::new(),
            authenticated: false,
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

    /// A resync read's answer (the module documentation's resync), or a login's. The connection
    /// waiting on a login gets the auth frame, or is closed and asked for again when no token
    /// came; for a connection already authenticating, it is a refresh: a token is kept for the
    /// next connection, and none means the next one logs in.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if self.resync_read(tag) {
            return self.on_resync_answer(tag, resp, scope, specs, sink, fx);
        }
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

    /// The open orders and positions, read over REST with the current token (the module
    /// documentation's resync), as of `ctx.wall`.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.start_resync(ctx, fx);
    }

    /// A login answer's token ([`token_spans`]); nothing else carries one: the frames do not
    /// (the auth frame's reply does not echo the token), nor do the resync's answers, which are
    /// never blanked whatever they hold.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Http(LOGIN_REQUEST, resp) => token_spans(&resp),
            Inbound::Http(..) | Inbound::Frame(_) => InboundSpans::NONE,
        }
    }
}
