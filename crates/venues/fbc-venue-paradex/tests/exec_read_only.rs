//! FBC-dly (decision 0061): Paradex's read-only private-stream codec, driven through
//! `ExecCodec` only with hand-built responses and frames. `on_open` asks for the login; the
//! token goes into one JSON-RPC `auth` frame inside its redaction span; the venue's
//! acknowledgement is reported Authenticated and each private channel is then subscribed once;
//! the refresh timer logs in again at the configured interval, and the next connection's auth
//! frame carries the new token; every command is refused with no effect; and the login
//! response's token span is named by `redact_inbound`, so the response replayed with the span
//! blanked yields the same events and effects (0028).
//!
//! The key and account are the synthetic ones in `fixtures/paradex/signing` (read from the
//! vectors file's header), the tokens made-up text that never looks like one Paradex issues,
//! and the SBE frames the hand-built ones in `fixtures/paradex/exec/` (SYNTHETIC). The JSON-RPC
//! replies follow docs.paradex.trade WebSocket "Authentication", "Subscription Channels" and
//! "Error Handling".

mod common;
mod md;

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use common::{Vectors, secrets};
use fbc_core::{
    AmendOrder, CancelOrder, CancelScope, Channel, CidMint, ClientOrderId, ConfigError, ConnState,
    CtxCall, DecodeError, Effect, Effects, EncodeCtx, ExecCodec, ExecEvent, ExecSink, HttpFailure,
    HttpMethod, HttpResponse, HttpTag, Inbound, InboundSpans, Lots, MonoNs, NamespaceLease,
    NewOrder, NonceBlock, NotSentReason, OpKind, OrderKind, OrderRef, PathStamps, QueryOrder,
    RateCharge, RawFrame, Reject, RejectKind, RpcId, Secret, Secrets, Side, SpecTable, StreamId,
    SubmitOutcome, Ticks, Tif, TimerTag, TrafficClass, VenueCommand, VenueConfig, VenueError,
    VenueMeta, WallNs, WireSlice, dispatch,
};
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::{
    LOGIN_REQUEST, PRIVATE_CHANNELS, REFRESH_TIMER, ReadOnlyExec, decode_account_event,
    decode_fill_event, decode_order_event, decode_position_event,
};
use fbc_venue_paradex::factory::caps;
use md::BTC;
use serde_json::Value;

const REST: &str = "https://api.testnet.paradex.trade/v1";
/// Made-up session tokens: letters, digits, `-`, `_` and `.`, never three base64 parts.
const TOKEN: &str = "SYNTHETIC.session-token.one";
const OTHER_TOKEN: &str = "SYNTHETIC.session-token.two_2";
/// A refresh interval no default could be mistaken for.
const REFRESH_EVERY: Duration = Duration::from_secs(45);
const STREAM: StreamId = StreamId(3);
/// The engine namespace the decode scope is lent under.
const OWN: fbc_core::Namespace = fbc_core::Namespace::new(7);

fn cfg() -> VenueConfig {
    let vectors = Vectors::read();
    let mut cfg = VenueConfig::new();
    cfg.insert(REST_URL, REST);
    cfg.insert(CHAIN_ID, &vectors.header["chain_id"]);
    cfg.insert(SIGNATURE_LIFETIME, "3600s");
    cfg.insert(REFRESH, "45s");
    cfg.insert(TIMEOUT, "5000ms");
    cfg
}

/// The synthetic account and key, as the consumer hands them over.
fn creds() -> Secrets {
    let vectors = Vectors::read();
    let mut creds = Secrets::new();
    creds.insert(
        ACCOUNT_ADDRESS,
        Secret::new(vectors.header["account"].clone()),
    );
    creds.insert(SIGNING_KEY, Secret::new(vectors.header["key"].clone()));
    creds
}

fn codec() -> ReadOnlyExec {
    ReadOnlyExec::new(&cfg(), creds()).unwrap()
}

fn ctx_at(secs: i64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(secs * 1_000_000_000),
        mono: MonoNs(1),
        nonces: NonceBlock::EMPTY,
    }
}

fn ctx() -> EncodeCtx {
    ctx_at(1_780_000_000)
}

fn login_body(token: &str) -> String {
    format!(r#"{{"jwt_token": "{token}"}}"#)
}

fn ok(body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status: 200,
        headers: &[],
        body,
    })
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// What one codec call returned, pushed and asked for.
struct Call {
    result: Result<(), DecodeError>,
    events: Vec<ExecEvent>,
    fx: Vec<Effect>,
}

fn open(codec: &mut ReadOnlyExec) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_open(STREAM, &ctx(), &mut fx);
    fx.take()
}

fn answer(codec: &mut ReadOnlyExec, resp: Result<HttpResponse<'_>, HttpFailure>) -> Call {
    let specs = md::specs();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch(&caps(), OWN, |scope| {
        codec.on_http(LOGIN_REQUEST, resp, scope, &specs, &mut sink, &mut fx)
    });
    Call {
        result,
        events: sink.0.into_iter().map(|(_, ev)| ev).collect(),
        fx: fx.take(),
    }
}

fn frame(codec: &mut ReadOnlyExec, f: RawFrame<'_>) -> Call {
    let specs = md::specs();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch(&caps(), OWN, |scope| {
        codec.on_frame(STREAM, f, scope, &specs, &mut sink, &mut fx)
    });
    Call {
        result,
        events: sink.0.into_iter().map(|(_, ev)| ev).collect(),
        fx: fx.take(),
    }
}

fn text(codec: &mut ReadOnlyExec, text: &str) -> Call {
    frame(codec, RawFrame::Text(text))
}

fn fire(codec: &mut ReadOnlyExec, tag: TimerTag) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_timer(tag, &ctx_at(1_780_000_045), &mut fx);
    fx.take()
}

/// The frames `fx` writes, each parsed as JSON, with its redaction spans.
fn sends(fx: &[Effect]) -> Vec<(Value, WireSlice)> {
    fx.iter()
        .filter_map(|effect| match effect {
            Effect::Send {
                stream,
                frame,
                rpc,
                class,
                ..
            } => {
                assert_eq!(*stream, STREAM);
                assert_eq!(*rpc, None, "the codec sends no order-entry request");
                assert_eq!(*class, TrafficClass::Safety);
                let json = serde_json::from_slice(frame.bytes()).expect("a JSON-RPC frame");
                Some((json, frame.clone()))
            }
            _ => None,
        })
        .collect()
}

/// The login requests `fx` asks for.
fn logins(fx: &[Effect]) -> usize {
    fx.iter()
        .filter(|effect| match effect {
            Effect::Http { tag, req, .. } => {
                assert_eq!(*tag, LOGIN_REQUEST);
                assert_eq!(req.method, HttpMethod::Post);
                assert_eq!(req.url.as_str(), format!("{REST}/auth"));
                true
            }
            _ => false,
        })
        .count()
}

/// The refresh timers `fx` sets, each asserted to be the configured interval.
fn timers(fx: &[Effect]) -> usize {
    fx.iter()
        .filter(|effect| match effect {
            Effect::Timer { tag, after } => {
                assert_eq!((*tag, *after), (REFRESH_TIMER, REFRESH_EVERY));
                true
            }
            _ => false,
        })
        .count()
}

/// The one auth frame `fx` writes: its JSON-RPC id, after checking that the token stands in
/// `params.bearer` and that the frame's one redaction span covers it exactly.
fn auth_frame(fx: &[Effect], token: &str) -> u64 {
    let [(json, frame)]: [_; 1] = sends(fx).try_into().expect("one frame");
    assert_eq!(json["jsonrpc"], "2.0");
    assert_eq!(json["method"], "auth");
    assert_eq!(json["params"]["bearer"], token);
    let [span] = <[_; 1]>::try_from(frame.redactions().to_vec()).expect("one span");
    let (start, end) = (span.start as usize, span.end as usize);
    assert_eq!(&frame.bytes()[start..end], token.as_bytes());
    json["id"].as_u64().expect("a numeric id")
}

fn reply(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","result":{{"node_id":"a1b2c3d4e5f6g7h8"}},"usIn":1682556415569005368,"usDiff":1291796,"id":{id}}}"#
    )
}

fn subscribed(id: u64, channel: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","result":{{"channel":"{channel}"}},"usIn":1682556415569005368,"usDiff":1291796,"id":{id}}}"#
    )
}

fn error(id: u64, code: i64, message: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","error":{{"code":{code},"message":"{message}"}},"usIn":1710522972729581,"usOut":1710522972729618,"usDiff":37,"id":{id}}}"#
    )
}

/// The subscribe frames `fx` writes: each channel with its JSON-RPC id, charged as a
/// subscription of no one instrument.
fn subscribes(fx: &[Effect]) -> Vec<(String, u64)> {
    for effect in fx {
        if let Effect::Send { charge, .. } = effect {
            assert_eq!(*charge, RateCharge::one(OpKind::Subscribe, None));
        }
    }
    sends(fx)
        .into_iter()
        .map(|(json, _)| {
            assert_eq!(json["method"], "subscribe");
            let channel = json["params"]["channel"].as_str().unwrap().to_owned();
            (channel, json["id"].as_u64().unwrap())
        })
        .collect()
}

/// A codec on an authenticated connection, its subscriptions answered.
fn authenticated() -> ReadOnlyExec {
    let mut codec = codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let acked = text(&mut codec, &reply(id));
    for (channel, id) in subscribes(&acked.fx) {
        let call = text(&mut codec, &subscribed(id, &channel));
        assert!(call.result.is_ok() && call.events.is_empty() && call.fx.is_empty());
    }
    codec
}

fn closed_and_reconnecting(call: &Call) {
    call.result.as_ref().unwrap();
    let last = call.events.last().expect("an event");
    assert_eq!(
        last,
        &ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Closed
        }
    );
    let reconnects: Vec<_> = call
        .fx
        .iter()
        .filter(|e| matches!(e, Effect::Reconnect { stream, .. } if *stream == STREAM))
        .collect();
    assert_eq!(reconnects.len(), 1, "{:?}", call.fx);
    assert!(
        sends(&call.fx).is_empty(),
        "nothing written on a closing connection"
    );
}

#[test]
fn on_open_asks_for_the_login_and_writes_nothing() {
    let mut codec = codec();
    assert_eq!(codec.nonces_for(CtxCall::Open(STREAM)), 0);
    let fx = open(&mut codec);
    assert_eq!(logins(&fx), 1);
    assert_eq!(fx.len(), 1, "the login alone: {fx:?}");
}

#[test]
fn the_token_is_answered_into_one_auth_frame_inside_its_redaction_span() {
    let mut codec = codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let call = answer(&mut codec, ok(body.as_bytes()));
    call.result.unwrap();
    assert!(call.events.is_empty(), "nothing is authenticated yet");
    auth_frame(&call.fx, TOKEN);
    assert_eq!(timers(&call.fx), 1);
    assert_eq!(logins(&call.fx), 0);
    assert_eq!(call.fx.len(), 2, "{:?}", call.fx);
    let Some(Effect::Send { charge, .. }) =
        call.fx.iter().find(|e| matches!(e, Effect::Send { .. }))
    else {
        unreachable!()
    };
    assert_eq!(*charge, RateCharge::one(OpKind::Control, None));
}

#[test]
fn the_acknowledgement_reports_authenticated_and_subscribes_each_private_channel_once() {
    let mut codec = codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let call = text(&mut codec, &reply(id));
    call.result.unwrap();
    assert_eq!(
        call.events,
        vec![ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Authenticated
        }]
    );
    let subs = subscribes(&call.fx);
    let mut channels: Vec<_> = subs.iter().map(|(c, _)| c.as_str()).collect();
    channels.sort_unstable();
    let mut expected = PRIVATE_CHANNELS.to_vec();
    expected.sort_unstable();
    assert_eq!(channels, expected, "each private channel once");
    assert_eq!(
        PRIVATE_CHANNELS,
        ["orders.ALL", "fills.ALL", "positions", "account"]
    );
    assert_eq!(call.fx.len(), 4);
    // Each acknowledgement is consumed; a second acknowledgement of the auth frame answers
    // nothing sent.
    for (channel, id) in subs {
        let call = text(&mut codec, &subscribed(id, &channel));
        call.result.unwrap();
        assert!(call.events.is_empty() && call.fx.is_empty());
    }
    let again = text(&mut codec, &reply(id));
    assert_eq!(
        again.result,
        Err(DecodeError::Malformed("a reply to no request sent"))
    );
    assert!(again.events.is_empty() && again.fx.is_empty());
}

#[test]
fn the_refresh_timer_logs_in_again_at_the_configured_interval_and_the_next_connection_carries_the_new_token()
 {
    let mut codec = authenticated();
    // The timer set by the first answer fires: a login, nothing written.
    let fx = fire(&mut codec, REFRESH_TIMER);
    assert_eq!(logins(&fx), 1);
    assert_eq!(fx.len(), 1);
    // Its answer holds the new token: the connection stays authenticated (Paradex keeps it so
    // for its lifetime), nothing is written or reported, and the timer is set again for the
    // configured interval.
    let body = login_body(OTHER_TOKEN);
    let call = answer(&mut codec, ok(body.as_bytes()));
    call.result.unwrap();
    assert!(call.events.is_empty());
    assert!(
        sends(&call.fx).is_empty(),
        "no auth frame on an open connection"
    );
    assert_eq!(timers(&call.fx), 1);
    assert_eq!(call.fx.len(), 1);
    // The next connection authenticates with the new token at once, and sets the refresh
    // timer for itself: an earlier connection's timer fires into nothing (0056).
    let fx = open(&mut codec);
    assert_eq!(logins(&fx), 0);
    assert_eq!(timers(&fx), 1);
    let id = auth_frame(&fx, OTHER_TOKEN);
    let call = text(&mut codec, &reply(id));
    assert_eq!(subscribes(&call.fx).len(), PRIVATE_CHANNELS.len());
    // Another timer's firing asks for nothing.
    assert!(fire(&mut codec, TimerTag(99)).is_empty());
}

#[test]
fn the_refresh_interval_depends_on_no_token_byte() {
    // Tokens of any length and content set the same timer.
    for token in [
        TOKEN,
        OTHER_TOKEN,
        "2",
        "2222222222222222222222222222222222222222",
    ] {
        let mut codec = codec();
        open(&mut codec);
        let body = login_body(token);
        let call = answer(&mut codec, ok(body.as_bytes()));
        assert_eq!(timers(&call.fx), 1, "{token}");
    }
}

#[test]
fn a_login_that_gives_no_token_reports_closed_and_asks_for_a_reconnect() {
    let refused = [
        Err(HttpFailure::TimedOut),
        Err(HttpFailure::NotSent),
        Ok(HttpResponse {
            status: 401,
            headers: &[],
            body: br#"{"error":"NOT_AUTHENTICATED"}"#,
        }),
        ok(br#"{"no_token": "here"}"#),
    ];
    for resp in refused {
        let mut codec = codec();
        open(&mut codec);
        let call = answer(&mut codec, resp);
        closed_and_reconnecting(&call);
        // Why no token came is reported first (FBC-3f8z), then the stream closed.
        login_refusal(&call);
        assert_eq!(call.events.len(), 2, "{:?}", call.events);
        // The next connection logs in first.
        let fx = open(&mut codec);
        assert_eq!(logins(&fx), 1);
        assert!(sends(&fx).is_empty());
    }
}

/// The refusal a login that gave no token reports before the stream's `Closed` (FBC-3f8z): an
/// uncorrelated error of kind `Other`, its text the codec's own sentence.
fn login_refusal(call: &Call) -> Reject {
    let [ExecEvent::UncorrelatedError(reject), ExecEvent::Conn { .. }] = call.events.as_slice()
    else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.kind, RejectKind::Other);
    assert!(
        reject.raw.starts_with("the Paradex login "),
        "{}",
        reject.raw
    );
    reject.clone()
}

/// A login refused with an HTTP status and Paradex's error body.
fn refused(status: u16, body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status,
        headers: &[],
        body,
    })
}

/// Paradex's REST error body (docs.paradex.trade "Errors": `error`, `message`, `data`).
fn paradex_error(code: &str, message: &str) -> String {
    serde_json::json!({ "error": code, "message": message, "data": null }).to_string()
}

/// The owner's first testnet run (FBC-3f8z): the login was refused and nothing said why. A
/// refusal with Paradex's error body is reported with its HTTP status, Paradex's code (also as
/// the reject's `venue_code`) and its message, before the stream closes and asks for a
/// reconnect; the next connection logs in again.
#[test]
fn a_refused_login_reports_its_http_status_and_paradexs_error_code_and_message() {
    let cases = [
        (
            401,
            "INVALID_STARKNET_SIGNATURE",
            "Invalid Starknet signature",
        ),
        (
            400,
            "NOT_ONBOARDED",
            "User has never called /onboarding endpoint",
        ),
        (403, "ACCOUNT_NOT_FOUND", "Account not found"),
        (429, "RATE_LIMIT_EXCEEDED", "Too many requests"),
    ];
    for (status, code, message) in cases {
        let mut codec = codec();
        open(&mut codec);
        let body = paradex_error(code, message);
        let call = answer(&mut codec, refused(status, body.as_bytes()));
        closed_and_reconnecting(&call);
        let reject = login_refusal(&call);
        assert_eq!(reject.venue_code.as_deref(), Some(code));
        assert_eq!(
            &*reject.raw,
            format!("the Paradex login was refused: HTTP {status}, {code}: {message}")
        );
        // Connection close and the paced reconnect are as before: one reconnect, and the next
        // connection logs in first.
        let fx = open(&mut codec);
        assert_eq!(logins(&fx), 1);
        assert!(sends(&fx).is_empty());
    }
}

/// A login that got no response names how it failed; no status or venue code exists to name.
#[test]
fn a_login_with_no_response_reports_how_the_transport_failed() {
    let cases = [
        (HttpFailure::NotSent, "the request was never sent"),
        (HttpFailure::TimedOut, "no answer came within the timeout"),
        (
            HttpFailure::Lost,
            "the connection failed after the request was sent",
        ),
    ];
    for (failure, why) in cases {
        let mut codec = codec();
        open(&mut codec);
        let call = answer(&mut codec, Err(failure));
        closed_and_reconnecting(&call);
        let reject = login_refusal(&call);
        assert_eq!(reject.venue_code, None);
        assert_eq!(
            &*reject.raw,
            format!("the Paradex login got no response: {why}")
        );
        assert_eq!(logins(&open(&mut codec)), 1);
    }
}

/// A refusal whose body is not Paradex's error (a proxy's page, an empty body, an error with no
/// readable member) is reported with its status alone, never the body; an answer of 2xx with no
/// token names its status and the part that did not read. A code that is not a plain
/// identifier is not shown, its message still is.
#[test]
fn a_refusal_without_paradexs_error_reports_its_status_and_never_the_body() {
    let page = b"<html><body>403 Forbidden by a made-up proxy</body></html>";
    let cases: [(u16, &[u8], Option<&str>, &str); 6] = [
        (
            403,
            page,
            None,
            "the Paradex login was refused: HTTP 403, with no Paradex error in the answer",
        ),
        (
            502,
            b"",
            None,
            "the Paradex login was refused: HTTP 502, with no Paradex error in the answer",
        ),
        (
            401,
            br#"{"error":{"nested":true},"message":42}"#,
            None,
            "the Paradex login was refused: HTTP 401, with no Paradex error in the answer",
        ),
        (
            401,
            br#"{"error":"NOT AN IDENTIFIER","message":"Unauthorized"}"#,
            None,
            "the Paradex login was refused: HTTP 401, no error code: Unauthorized",
        ),
        (
            400,
            br#"{"error":"VALIDATION_ERROR"}"#,
            Some("VALIDATION_ERROR"),
            "the Paradex login was refused: HTTP 400, VALIDATION_ERROR: no message",
        ),
        (
            200,
            br#"{"no_token": "here"}"#,
            None,
            "the Paradex login answer held no token: HTTP 200, malformed frame: jwt_token",
        ),
    ];
    for (status, body, code, text) in cases {
        let mut codec = codec();
        open(&mut codec);
        let call = answer(&mut codec, refused(status, body));
        closed_and_reconnecting(&call);
        let reject = login_refusal(&call);
        assert_eq!(reject.venue_code.as_deref(), code);
        assert_eq!(&*reject.raw, text);
        assert!(!reject.raw.contains("proxy"), "the body is never shown");
    }
}

/// Codex r4225701545 (P1) and r4225701546 (P2) on PR #135: a credential the venue echoes is
/// withheld whatever its length, as every field element the login carries (the account, `0x` and
/// hex, and the signature's numbers) is written with a digit, and a run of hex digits is withheld
/// even without one; and the text shown is plain ASCII on one line: every whitespace character,
/// Unicode line and paragraph separators included, becomes a space, and a word holding any
/// other character outside printable ASCII (a format control such as a bidi override or a
/// zero-width space) is withheld.
#[test]
fn a_short_echo_is_withheld_and_the_text_shown_is_one_line_of_plain_ascii() {
    let message = "Account 0x1 and 0x0abc, signature [\"12345\",\"0xf\"], key 7, felt deadbeefcafe \
                   expired\u{2028}forged line\u{2029}para \u{202e}evil\u{202c} zero\u{200b}width \
                   caf\u{e9} done";
    let mut one = codec();
    open(&mut one);
    let body = paradex_error("INVALID_SIGNATURE", message);
    let call = answer(&mut one, refused(401, body.as_bytes()));
    closed_and_reconnecting(&call);
    let reject = login_refusal(&call);
    assert_eq!(
        &*reject.raw,
        "the Paradex login was refused: HTTP 401, INVALID_SIGNATURE: Account <withheld> and \
         <withheld>, signature [\"<withheld>\",\"<withheld>\"], key <withheld>, felt <withheld> \
         expired forged line para <withheld> <withheld> <withheld> done"
    );
    assert!(
        reject
            .raw
            .bytes()
            .all(|b| b == b' ' || b.is_ascii_graphic()),
        "{}",
        reject.raw
    );
    // A code holding a digit-only or hex run is not shown.
    for code in ["0x1", "ABC_0x1f", "DEADBEEF", "E_1234567890123456"] {
        let mut codec = codec();
        open(&mut codec);
        let body = paradex_error(code, "Unauthorized");
        let reject = login_refusal(&answer(&mut codec, refused(401, body.as_bytes())));
        assert_eq!(reject.venue_code, None, "{code}");
    }
}

/// A refused login never reports the token, the login's signature, the key or the account,
/// even when the venue's answer echoes them in its code and message, checked by the rehearsal's
/// secret search: every word of the venue's text that could hold one is withheld, the rest
/// shown, control characters become spaces, and the message is cut at a bound.
#[test]
fn a_refused_login_reports_no_token_signature_key_or_account_even_when_the_venue_echoes_them() {
    let (account, key) = secrets::synthetic();
    let fx = open(&mut codec());
    let signature = fx
        .iter()
        .find_map(|e| match e {
            Effect::Http { req, .. } => req
                .headers
                .iter()
                .find(|h| h.name == "PARADEX-STARKNET-SIGNATURE")
                .map(|h| h.value.clone()),
            _ => None,
        })
        .expect("the login carries its signature");
    let numbers = secrets::signature_numbers(&signature);
    assert!(!numbers.is_empty());
    let felt = |hex: &str| fbc_venue_paradex::sign::Felt::from_hex(hex).unwrap();
    let message = format!(
        "Invalid signature {signature} for account {account} (0x{:x}, {}) key={key}\n\
         token:{TOKEN} [{}] {OTHER_TOKEN}",
        felt(&account),
        felt(&key),
        numbers.join(",")
    );
    let codes = [
        account.clone(),
        key.clone(),
        TOKEN.to_owned(),
        numbers[0].clone(),
    ];
    for code in codes {
        let mut codec = codec();
        open(&mut codec);
        let body = paradex_error(&code, &message);
        let call = answer(&mut codec, refused(401, body.as_bytes()));
        closed_and_reconnecting(&call);
        let reject = login_refusal(&call);
        assert_eq!(
            reject.venue_code, None,
            "a code holding a secret is not shown"
        );
        let shown = format!(
            "{} {} {:?} {:?}",
            reject.venue_code.as_deref().unwrap_or(""),
            reject.raw,
            call.events,
            call.fx
        );
        secrets::secrets_absent(
            &shown,
            TOKEN,
            std::slice::from_ref(&signature),
            "the login refusal",
        );
        secrets::secrets_absent(&shown, OTHER_TOKEN, &[], "the login refusal");
        assert!(
            reject.raw.starts_with(
                "the Paradex login was refused: HTTP 401, no error code: Invalid signature "
            ),
            "{}",
            reject.raw
        );
        assert!(reject.raw.contains("<withheld>"), "{}", reject.raw);
        assert!(reject.raw.contains("for account"), "{}", reject.raw);
        assert!(!reject.raw.contains('\n'), "{}", reject.raw);
    }
    // A long message is cut at its bound.
    let mut codec = codec();
    open(&mut codec);
    let long = "a word ".repeat(200);
    let body = paradex_error("VALIDATION_ERROR", &long);
    let call = answer(&mut codec, refused(400, body.as_bytes()));
    let reject = login_refusal(&call);
    let prefix = "the Paradex login was refused: HTTP 400, VALIDATION_ERROR: ";
    let shown = reject.raw.strip_prefix(prefix).expect("the prefix");
    assert!(shown.ends_with("..."), "{shown}");
    assert!(shown.chars().count() <= 203, "{}", shown.chars().count());
}

#[test]
fn a_refused_auth_frame_reports_the_venues_code_then_closed_and_the_next_connection_logs_in() {
    let mut codec = codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let call = text(&mut codec, &error(id, 40111, "Invalid Bearer Token"));
    closed_and_reconnecting(&call);
    let [ExecEvent::UncorrelatedError(reject), _] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.kind, RejectKind::Other);
    assert_eq!(reject.venue_code.as_deref(), Some("40111"));
    // A token the venue refused is not reused.
    let fx = open(&mut codec);
    assert_eq!(logins(&fx), 1);
    assert!(sends(&fx).is_empty());
}

#[test]
fn a_reused_token_the_venue_refuses_is_dropped_and_the_next_connection_logs_in() {
    let mut codec = authenticated();
    let fx = open(&mut codec);
    let id = auth_frame(&fx, TOKEN);
    let call = text(&mut codec, &error(id, 40110, "Malformed Bearer Token"));
    closed_and_reconnecting(&call);
    assert_eq!(logins(&open(&mut codec)), 1);
}

#[test]
fn a_refresh_that_gives_no_token_leaves_the_connection_and_the_next_one_logs_in() {
    let mut codec = authenticated();
    fire(&mut codec, REFRESH_TIMER);
    let call = answer(&mut codec, Err(HttpFailure::Lost));
    call.result.unwrap();
    assert!(call.events.is_empty(), "the connection stays authenticated");
    assert_eq!(call.fx.len(), 1);
    assert_eq!(timers(&call.fx), 1);
    assert_eq!(logins(&open(&mut codec)), 1);
}

#[test]
fn a_refused_private_channel_reports_the_code_then_closed() {
    let mut codec = codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let subs = subscribes(&text(&mut codec, &reply(id)).fx);
    let (_, sub) = &subs[1];
    let call = text(&mut codec, &error(*sub, -32602, "Invalid parameters"));
    closed_and_reconnecting(&call);
    let [ExecEvent::UncorrelatedError(reject), _] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.venue_code.as_deref(), Some("-32602"));
    // The closed connection answers nothing more.
    let (_, other) = &subs[0];
    let late = text(&mut codec, &subscribed(*other, "orders.ALL"));
    assert!(late.result.is_err() && late.events.is_empty() && late.fx.is_empty());
}

#[test]
fn a_login_that_cannot_be_signed_asks_for_the_connection_again() {
    let mut codec = codec();
    let mut fx = Effects::new();
    codec.on_open(STREAM, &ctx_at(-5), &mut fx);
    let fx = fx.take();
    assert_eq!(logins(&fx), 0);
    assert!(
        fx.iter()
            .any(|e| matches!(e, Effect::Reconnect { stream, .. } if *stream == STREAM))
    );
    // A refresh that cannot be signed only sets the timer again.
    let mut codec = authenticated();
    let mut fx = Effects::new();
    codec.on_timer(REFRESH_TIMER, &ctx_at(-5), &mut fx);
    assert_eq!(timers(fx.as_slice()), 1);
    assert_eq!(fx.len(), 1);
}

/// One command of every kind on BTC.
fn every_command() -> Vec<VenueCommand> {
    let dir = std::env::temp_dir().join(format!("fbc-paradex-read-only-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let account = fbc_core::AccountKey::new(5);
    let lease = NamespaceLease::acquire(&dir, account, OWN).unwrap();
    let cid: ClientOrderId = CidMint::new(lease, 0, 0, WallNs(0)).mint().unwrap();
    let _ = fs::remove_dir_all(&dir);
    let qty = Lots::new(10).unwrap();
    let px = Ticks(1_000_000);
    let order = NewOrder {
        cid,
        inst: BTC,
        side: Side::Buy,
        qty,
        kind: OrderKind::Limit { px },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: true,
    };
    let amend = AmendOrder {
        target: OrderRef::Client(cid),
        inst: BTC,
        side: Side::Buy,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: true,
        px,
        qty,
        cum_filled: Lots::new(0).unwrap(),
    };
    let cancel = CancelOrder {
        target: OrderRef::Client(cid),
        inst: BTC,
        side: Side::Buy,
        placement_nonce: None,
    };
    let query = QueryOrder {
        target: OrderRef::Client(cid),
        inst: BTC,
        placement_nonce: None,
    };
    vec![
        VenueCommand::Place(order.clone()),
        VenueCommand::PlaceBatch(vec![order]),
        VenueCommand::Amend(amend),
        VenueCommand::Cancel(cancel.clone()),
        VenueCommand::CancelMany(vec![cancel]),
        VenueCommand::CancelAll(CancelScope::Instrument(BTC)),
        VenueCommand::CancelAll(CancelScope::Account),
        VenueCommand::ArmCancelOnDisconnect(true),
        VenueCommand::ArmCancelOnDisconnect(false),
        VenueCommand::RefreshDeadMan,
        VenueCommand::Query(query),
        VenueCommand::FeeQuery,
    ]
}

#[test]
fn every_command_is_refused_unsupported_with_no_effect_and_no_call_asks_for_a_nonce() {
    let mut codec = authenticated();
    let specs: SpecTable = md::specs();
    for cmd in every_command() {
        let mut fx = Effects::new();
        let result = codec.encode(
            &cmd,
            RpcId(7),
            &specs,
            &ctx(),
            &mut PathStamps::off(),
            &mut fx,
        );
        assert_eq!(result, Err(NotSentReason::Unsupported), "{cmd:?}");
        assert!(fx.is_empty(), "{cmd:?} asked for {fx:?}");
    }
    for call in [
        CtxCall::Open(STREAM),
        CtxCall::Timer(REFRESH_TIMER),
        CtxCall::Resync,
    ] {
        assert_eq!(codec.nonces_for(call), 0);
    }
    // The resync only reads (FBC-xvf, tests/exec_codec.rs): two GETs, nothing written.
    let mut fx = Effects::new();
    codec.resync(&ctx(), &mut fx);
    assert!(sends(fx.as_slice()).is_empty());
    let reads = fx.as_slice().iter().filter(
        |e| matches!(e, Effect::Http { req, rpc: None, .. } if req.method == HttpMethod::Get),
    );
    assert_eq!((reads.count(), fx.len()), (2, 2));
    // A deadline the runtime names anyway is Unknown.
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(7), &mut sink);
    let [(_, ExecEvent::Outcome { rpc, item, outcome })] = sink.0.as_slice() else {
        panic!("{:?}", sink.0)
    };
    assert_eq!(
        (*rpc, item, outcome),
        (RpcId(7), &None, &SubmitOutcome::Unknown)
    );
}

/// `slice`'s bytes with every redaction span blanked, as the journal's reader gives them back
/// (0028's blank byte, `2`).
fn blanked(bytes: &[u8], spans: &[core::ops::Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        out[span.start as usize..span.end as usize].fill(b'2');
    }
    out
}

/// `fx` with every frame's spans blanked, for comparing modulo them.
fn modulo_spans(fx: &[Effect]) -> Vec<Effect> {
    fx.iter()
        .map(|effect| match effect {
            Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge,
            } => Effect::Send {
                stream: *stream,
                frame: WireSlice::redacted(
                    blanked(frame.bytes(), frame.redactions()),
                    frame.redactions().to_vec(),
                )
                .unwrap(),
                rpc: *rpc,
                class: *class,
                charge: *charge,
            },
            other => other.clone(),
        })
        .collect()
}

#[test]
fn redact_inbound_names_the_login_tokens_span_and_the_blanked_replay_yields_the_same_events_and_effects()
 {
    let body = login_body(TOKEN).into_bytes();
    let live_codec = codec();
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: &body,
    };
    let spans = live_codec.redact_inbound(Inbound::Http(LOGIN_REQUEST, resp));
    let at = body
        .windows(TOKEN.len())
        .position(|w| w == TOKEN.as_bytes())
        .unwrap() as u32;
    let token_span = at..at + TOKEN.len() as u32;
    assert_eq!(spans, InboundSpans::response(vec![], vec![token_span]));
    spans.check(Inbound::Http(LOGIN_REQUEST, resp)).unwrap();
    // The frames carry no credential: the auth reply does not echo the token.
    let ack = reply(1);
    assert_eq!(
        live_codec.redact_inbound(Inbound::Frame(RawFrame::Text(&ack))),
        InboundSpans::NONE
    );

    // The session live, then replayed with the login response's token blanked.
    let replay_body = blanked(&body, spans.body());
    let run = |body: &[u8]| {
        let mut codec = codec();
        let opened = open(&mut codec);
        let answered = answer(&mut codec, ok(body));
        let acked = text(&mut codec, &reply(1));
        let refresh = fire(&mut codec, REFRESH_TIMER);
        let reopened = open(&mut codec);
        let effects = [opened, answered.fx, acked.fx, refresh, reopened].concat();
        let events = [answered.events, acked.events].concat();
        (modulo_spans(&effects), events)
    };
    let live = run(&body);
    let replay = run(&replay_body);
    assert_eq!(live, replay);
    assert!(!live.1.is_empty() && !live.0.is_empty());
}

/// The bytes of exec fixture `name`, as `exec_order_events.rs` reads them.
fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/exec")
        .join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .map(|line| line.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

type Decoder =
    fn(&[u8], &fbc_core::DecodeScope<'_>, &SpecTable, &mut dyn ExecSink) -> Result<(), DecodeError>;

#[test]
fn private_frames_decode_as_their_decoders_do_and_other_templates_are_skipped() {
    let account: Decoder = |f, scope, _specs, sink| decode_account_event(f, scope, sink);
    let cases: [(&str, Decoder); 4] = [
        ("order-new-v1.sbe.txt", decode_order_event),
        ("fill-rpi-v2.sbe.txt", decode_fill_event),
        ("position-long-v2.sbe.txt", decode_position_event),
        ("account-v2.sbe.txt", account),
    ];
    let mut codec = authenticated();
    let specs = md::specs();
    for (name, decoder) in cases {
        let bytes = fixture(name);
        let mut direct = Sink::default();
        dispatch(&caps(), OWN, |scope| {
            decoder(&bytes, scope, &specs, &mut direct)
        })
        .unwrap();
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        call.result.unwrap();
        let direct: Vec<_> = direct.0.into_iter().map(|(_, ev)| ev).collect();
        assert!(!direct.is_empty(), "{name}");
        assert_eq!(call.events, direct, "{name}");
        assert!(call.fx.is_empty(), "{name}");
    }
    // A heartbeat (template 40) and a template not decoded: skipped.
    for template in [40u16, 99] {
        let mut bytes = vec![0, 0];
        bytes.extend(template.to_le_bytes());
        bytes.extend([1, 0, 2, 0]);
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        assert!(call.result.is_ok() && call.events.is_empty() && call.fx.is_empty());
    }
    // A refused frame pushes nothing.
    let short = fixture("order-short-block.sbe.txt");
    let call = frame(&mut codec, RawFrame::Binary(&short));
    assert!(call.result.is_err() && call.events.is_empty());
    let call = frame(&mut codec, RawFrame::Binary(&[1, 2, 3]));
    assert!(call.result.is_err() && call.events.is_empty());
}

#[test]
fn text_frames_that_answer_nothing_are_refused_with_nothing_pushed() {
    let mut codec = authenticated();
    for bad in [
        "not json",
        r#"{"jsonrpc":"2.0","method":"subscription","params":{}}"#,
        r#"{"jsonrpc":"2.0","result":null,"id":1}"#,
        r#"{"jsonrpc":"2.0","result":{},"id":12345}"#,
        r#"{"jsonrpc":"2.0","result":{}}"#,
        r#"{"jsonrpc":"2.0","error":{"message":"no code"},"id":1}"#,
    ] {
        let call = text(&mut codec, bad);
        assert!(call.result.is_err(), "{bad}");
        assert!(call.events.is_empty() && call.fx.is_empty(), "{bad}");
    }
    // A reply before any connection opened answers nothing either.
    let mut fresh = ReadOnlyExec::new(&cfg(), creds()).unwrap();
    assert!(text(&mut fresh, &reply(1)).result.is_err());
}

#[test]
fn an_answer_to_no_login_is_refused() {
    let mut codec = codec();
    open(&mut codec);
    let specs = md::specs();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let body = login_body(TOKEN);
    let result = dispatch(&caps(), OWN, |scope| {
        codec.on_http(
            HttpTag(42),
            ok(body.as_bytes()),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    });
    assert!(result.is_err());
    assert!(sink.0.is_empty() && fx.is_empty());
}

#[test]
fn the_codec_is_refused_naming_a_missing_key_and_shows_no_credential() {
    let vectors = Vectors::read();
    let mut partial = VenueConfig::new();
    partial.insert(REST_URL, REST);
    partial.insert(CHAIN_ID, &vectors.header["chain_id"]);
    partial.insert(SIGNATURE_LIFETIME, "3600s");
    partial.insert(TIMEOUT, "5000ms");
    let err = ReadOnlyExec::new(&partial, creds()).unwrap_err();
    assert_eq!(err, VenueError::Config(ConfigError::Missing(REFRESH)));
    let mut codec = authenticated();
    open(&mut codec);
    let shown = format!("{codec:?}");
    let vectors = Vectors::read();
    for secret in [TOKEN, &vectors.header["account"], &vectors.header["key"]] {
        assert!(!shown.contains(secret), "{shown}");
    }
}

/// `reply` with `"<name>": null` beside its members, as Paradex may write the member it does
/// not fill.
fn with_null(reply: &str, name: &str) -> String {
    let at = reply.rfind("\"id\":").expect("a reply with an id");
    format!("{}\"{name}\":null,{}", &reply[..at], &reply[at..])
}

#[test]
fn an_auth_and_a_subscribe_reply_read_a_json_null_error_or_result_as_absent() {
    // A result beside "error": null is the result: the auth reply authenticates and each
    // subscribe reply is consumed.
    let mut codec = crate::codec();
    open(&mut codec);
    let body = login_body(TOKEN);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let call = text(&mut codec, &with_null(&reply(id), "error"));
    call.result.unwrap();
    assert_eq!(
        call.events,
        vec![ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Authenticated
        }]
    );
    let subs = subscribes(&call.fx);
    assert_eq!(subs.len(), PRIVATE_CHANNELS.len());
    for (channel, id) in &subs {
        let call = text(&mut codec, &with_null(&subscribed(*id, channel), "error"));
        call.result.unwrap();
        assert!(call.events.is_empty() && call.fx.is_empty());
    }
    // An error beside "result": null is that error: the auth reply is refused with its code.
    let mut codec = crate::codec();
    open(&mut codec);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let refused = with_null(&error(id, 40111, "Invalid Bearer Token"), "result");
    let call = text(&mut codec, &refused);
    closed_and_reconnecting(&call);
    let [ExecEvent::UncorrelatedError(reject), _] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.venue_code.as_deref(), Some("40111"));
    // And a subscribe reply's.
    let mut codec = crate::codec();
    open(&mut codec);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let subs = subscribes(&text(&mut codec, &reply(id)).fx);
    let refused = with_null(&error(subs[0].1, -32602, "Invalid parameters"), "result");
    let call = text(&mut codec, &refused);
    closed_and_reconnecting(&call);
    let [ExecEvent::UncorrelatedError(reject), _] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.venue_code.as_deref(), Some("-32602"));
}

/// Replies to request `id` that read as neither a result nor a readable refusal, each with the
/// venue code of the refusal it reports first (`Some(None)`: a refusal with no code), if any.
/// An error stated at all is the venue's refusal, as the Java client reads the auth reply
/// (`ParadexOrderWebSocketClient.onAuthResponse`: any non-null `error`).
fn unreadable(id: u64) -> Vec<(String, Option<Option<&'static str>>)> {
    vec![
        // Neither member, or both null.
        (format!(r#"{{"jsonrpc":"2.0","id":{id}}}"#), None),
        (
            format!(r#"{{"jsonrpc":"2.0","result":null,"error":null,"id":{id}}}"#),
            None,
        ),
        // A result beside an error: the error.
        (
            format!(
                r#"{{"jsonrpc":"2.0","result":{{}},"error":{{"code":40111,"message":"Invalid Bearer Token"}},"id":{id}}}"#
            ),
            Some(Some("40111")),
        ),
        // An error whose code is not an integer, or that has none, or is no object.
        (
            format!(
                r#"{{"jsonrpc":"2.0","error":{{"code":"40111","message":"Invalid Bearer Token"}},"id":{id}}}"#
            ),
            Some(None),
        ),
        (
            format!(r#"{{"jsonrpc":"2.0","error":{{"message":"no code"}},"id":{id}}}"#),
            Some(None),
        ),
        (
            format!(r#"{{"jsonrpc":"2.0","error":"denied","id":{id}}}"#),
            Some(None),
        ),
    ]
}

/// Asserts `call` closed the stream and asked for a reconnect, reporting first the refusal
/// `refusal` names (`unreadable`), or nothing else.
fn closed_as(call: &Call, refusal: Option<Option<&str>>, bad: &str) {
    closed_and_reconnecting(call);
    match (refusal, call.events.as_slice()) {
        (None, [_]) => {}
        (Some(code), [ExecEvent::UncorrelatedError(reject), _]) => {
            assert_eq!(reject.kind, RejectKind::Other, "{bad}");
            assert_eq!(reject.venue_code.as_deref(), code, "{bad}");
        }
        _ => panic!("{bad}: {:?}", call.events),
    }
}

#[test]
fn an_unreadable_auth_or_subscribe_reply_reports_closed_and_asks_for_a_reconnect() {
    let body = login_body(TOKEN);
    // The auth reply: the connection is closed and asked for again, and its token is not
    // reused, so the next connection logs in.
    for case in 0..unreadable(0).len() {
        let mut codec = crate::codec();
        open(&mut codec);
        let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
        let (bad, refusal) = unreadable(id).swap_remove(case);
        let call = text(&mut codec, &bad);
        closed_as(&call, refusal, &bad);
        // The closed connection answers nothing more.
        assert!(text(&mut codec, &reply(id)).result.is_err(), "{bad}");
        let fx = open(&mut codec);
        assert_eq!(logins(&fx), 1, "{bad}");
        assert!(sends(&fx).is_empty(), "{bad}");
    }
    // A subscribe reply: the same, on an authenticated connection.
    for case in 0..unreadable(0).len() {
        let mut codec = crate::codec();
        open(&mut codec);
        let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
        let subs = subscribes(&text(&mut codec, &reply(id)).fx);
        let (bad, refusal) = unreadable(subs[2].1).swap_remove(case);
        let call = text(&mut codec, &bad);
        closed_as(&call, refusal, &bad);
        let late = text(&mut codec, &subscribed(subs[0].1, &subs[0].0));
        assert!(late.result.is_err() && late.events.is_empty() && late.fx.is_empty());
        assert_eq!(logins(&open(&mut codec)), 1, "{bad}");
    }
    // A readable auth reply still authenticates.
    let mut codec = crate::codec();
    open(&mut codec);
    let id = auth_frame(&answer(&mut codec, ok(body.as_bytes())).fx, TOKEN);
    let call = text(&mut codec, &reply(id));
    call.result.unwrap();
    assert_eq!(
        call.events,
        vec![ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Authenticated
        }]
    );
    assert_eq!(subscribes(&call.fx).len(), PRIVATE_CHANNELS.len());
}
