//! A toy order-entry codec that authenticates over HTTP, for FBC-7lm (decision 0028): the
//! venue answers its login with a token in the response body and a cookie in a `Set-Cookie`
//! header, and echoes a key back in a `hello` frame. Its `redact_inbound` names those spans so
//! the journal keeps them only as keyed hashes. It decodes nothing from those spans' bytes, so a
//! journaled record, read back with them blanked, replays through it to the same events and
//! effects.
//!
//! fbc-core's `inbound_redaction` test and fbc-journal's `inbound_redaction` test share it
//! (the latter by path). Its protocol describes no real venue: one record per body or frame,
//! `kind|key=value|...`, as the toy venue's. It takes no order (encode refuses every command)
//! and holds no state: every call answers from its input alone.

#![allow(dead_code)]

#[path = "../common/mod.rs"]
mod common;

use core::ops::Range;
use std::time::Duration;

use fbc_core::{
    ConnState, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    ExecCodec, ExecEvent, ExecSink, HeaderMark, HttpFailure, HttpMethod, HttpRequest, HttpResponse,
    HttpTag, Inbound, InboundSpans, Namespace, NotSentReason, OpKind, RateCharge, RawFrame, RpcId,
    SpecTable, StreamId, SubmitOutcome, TimerTag, TrafficClass, VenueCommand, VenueMeta, WireSlice,
    WireUrl, dispatch,
};

/// The order-entry stream.
pub const EXEC_STREAM: StreamId = StreamId(1);
/// The login request's tag.
pub const AUTH_TAG: HttpTag = HttpTag(1);
/// The timer that logs in again before the token runs out.
pub const REFRESH_TAG: TimerTag = TimerTag(1);
/// The login: a control request, counted against the account.
const CONTROL: RateCharge = RateCharge::one(OpKind::Control, None);
const AUTH_URL: &str = "https://toy.invalid/auth";
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
/// A header the toy's venue echoes a request key in, by name: `X-Echo-<key>`.
const ECHO_PREFIX: &str = "X-Echo-";

/// The toy's order-entry codec. It holds nothing between calls.
pub struct AuthToy;

/// The span of field `key`'s value in a `kind|key=value|...` record: from after `key=` to the
/// next `|` or line end. `None` when the field is absent or empty. A blanked value is still
/// found, since the blank byte is neither a separator nor a line end.
pub fn field_span(bytes: &[u8], key: &str) -> Option<Range<u32>> {
    let needle = format!("|{key}=");
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())?
        + needle.len();
    let len = bytes[at..]
        .iter()
        .position(|&b| b == b'|' || b == b'\n')
        .unwrap_or(bytes.len() - at);
    (len > 0).then(|| at as u32..(at + len) as u32)
}

/// A record's kind and the value of `key`, read from its text.
fn field<'a>(text: &'a str, key: &str) -> (&'a str, Option<&'a str>) {
    let mut parts = text.split('|');
    let kind = parts.next().unwrap_or("");
    let value = parts
        .filter_map(|part| part.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v);
    (kind, value)
}

/// The login request: nothing in it is a credential.
fn login(fx: &mut Effects) {
    fx.push(Effect::Http {
        tag: AUTH_TAG,
        req: HttpRequest {
            method: HttpMethod::Post,
            url: WireUrl::plain(AUTH_URL),
            headers: Vec::new(),
            body: WireSlice::plain(b"login|user=toy".to_vec()),
        },
        rpc: None,
        timeout: AUTH_TIMEOUT,
        class: TrafficClass::Safety,
        charge: CONTROL,
    });
}

impl ExecCodec for AuthToy {
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// Logs in over HTTP.
    fn on_open(&mut self, _stream: StreamId, _ctx: &EncodeCtx, fx: &mut Effects) {
        login(fx);
    }

    fn encode(
        &mut self,
        _cmd: &VenueCommand,
        _rpc: RpcId,
        _specs: &SpecTable,
        _ctx: &EncodeCtx,
        _fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        Err(NotSentReason::Unsupported)
    }

    /// `hello|key=<echoed key>`: the stream is open.
    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let text = core::str::from_utf8(f.bytes()).map_err(|_| DecodeError::Malformed("text"))?;
        match field(text, "key") {
            ("hello", Some(_)) => {
                let state = ConnState::Open;
                sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
                Ok(())
            }
            _ => Err(DecodeError::Malformed("kind")),
        }
    }

    /// `auth|token=<token>|refresh=<seconds>` with status 200: authenticated, and a login
    /// again after `refresh` seconds. A login that got no response reconnects.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        if tag != AUTH_TAG {
            return Err(DecodeError::Malformed("tag"));
        }
        let resp = match resp {
            Ok(resp) => resp,
            Err(_) => {
                let reason = "login failed";
                fx.push(Effect::Reconnect {
                    stream: EXEC_STREAM,
                    reason,
                });
                return Ok(());
            }
        };
        if resp.status != 200 {
            return Err(DecodeError::Malformed("status"));
        }
        let body = core::str::from_utf8(resp.body).map_err(|_| DecodeError::Malformed("body"))?;
        let (kind, refresh) = field(body, "refresh");
        let refresh: u64 = match (kind, refresh) {
            ("auth", Some(secs)) => secs
                .parse()
                .map_err(|_| DecodeError::Malformed("refresh"))?,
            _ => return Err(DecodeError::Malformed("kind")),
        };
        field_span(resp.body, "token").ok_or(DecodeError::Malformed("token"))?;
        let state = ConnState::Authenticated;
        let stream = EXEC_STREAM;
        sink.push(VenueMeta::NONE, ExecEvent::Conn { stream, state });
        let after = Duration::from_secs(refresh);
        fx.push(Effect::Timer {
            tag: REFRESH_TAG,
            after,
        });
        Ok(())
    }

    /// Logs in again.
    fn on_timer(&mut self, _tag: TimerTag, _ctx: &EncodeCtx, fx: &mut Effects) {
        login(fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let (item, outcome) = (None, SubmitOutcome::Unknown);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    fn resync(&mut self, _ctx: &EncodeCtx, _fx: &mut Effects) {}

    /// The key a `hello` frame echoes; the login response's token, its `Set-Cookie` value, and
    /// any `X-Echo-<key>` header whole.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Frame(f) => {
                InboundSpans::frame(field_span(f.bytes(), "key").into_iter().collect())
            }
            Inbound::Http(AUTH_TAG, resp) => {
                let headers = resp
                    .headers
                    .iter()
                    .enumerate()
                    .filter_map(|(at, (name, _))| {
                        let mark = if name.eq_ignore_ascii_case("Set-Cookie") {
                            HeaderMark::Value
                        } else if name.len() > ECHO_PREFIX.len()
                            && name[..ECHO_PREFIX.len()].eq_ignore_ascii_case(ECHO_PREFIX)
                        {
                            HeaderMark::NameAndValue
                        } else {
                            return None;
                        };
                        Some((at as u32, mark))
                    });
                let body = field_span(resp.body, "token").into_iter().collect();
                InboundSpans::response(headers.collect(), body)
            }
            Inbound::Http(..) => InboundSpans::NONE,
        }
    }
}

/// What one call of the toy gave: its result, the events it pushed and the effects it asked
/// for. Two decodes that agree on these agree on everything the runtime sees.
#[derive(Debug, PartialEq)]
pub struct Decoded {
    pub result: Result<(), DecodeError>,
    pub events: Vec<ExecEvent>,
    pub effects: Vec<Effect>,
}

struct Events(Vec<ExecEvent>);

impl ExecSink for Events {
    fn push(&mut self, _meta: VenueMeta, ev: ExecEvent) {
        self.0.push(ev);
    }
}

/// Runs `call` with the decode scope a synthetic venue's caps lend, collecting what it gives.
fn decode(
    call: impl for<'s> FnOnce(
        &'s DecodeScope<'s>,
        &mut dyn ExecSink,
        &mut Effects,
    ) -> Result<(), DecodeError>,
) -> Decoded {
    let (mut sink, mut fx) = (Events(Vec::new()), Effects::new());
    let result = dispatch(&common::uuid_caps(), Namespace::new(1), |scope| {
        call(scope, &mut sink, &mut fx)
    });
    Decoded {
        result,
        events: sink.0,
        effects: fx.take(),
    }
}

/// Hands the toy the login response (or failure) `resp` to tag `tag`.
pub fn decode_http(tag: HttpTag, resp: Result<HttpResponse<'_>, HttpFailure>) -> Decoded {
    let specs = SpecTable::new();
    decode(|scope, sink, fx| AuthToy.on_http(tag, resp, scope, &specs, sink, fx))
}

/// Hands the toy frame `f` on its order-entry stream.
pub fn decode_frame(f: RawFrame<'_>) -> Decoded {
    let specs = SpecTable::new();
    decode(|scope, sink, fx| AuthToy.on_frame(EXEC_STREAM, f, scope, &specs, sink, fx))
}
