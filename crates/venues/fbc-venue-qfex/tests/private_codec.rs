//! FBC-zeb0: QFEX's trade WebSocket codec, recording-only (`caps.exec` stays `None`).
//!
//! - It authenticates and then subscribes to `order_responses`, `fills` and `positions`, for the
//!   primary account and for a subaccount; the stream is reported authenticated only once QFEX
//!   confirms all three (FBC-bsou, `tests/private_subscribe.rs`).
//! - A RateLimited err in either documented envelope, with either documented hint form, and an
//!   order whose status is `RATE_LIMITED`, each hold the shared provider-user budget and settle
//!   or reconnect nothing (decisions 0126, 0134 item 5).
//! - Once authenticated, a request's err is counted by its code and keeps the epoch open; an err
//!   about the session, an undocumented one, and any err before authentication end the epoch,
//!   that epoch only. A generic ack is counted and keeps it open.
//! - The journal keeps each err and ack as decision 0134 items 2 to 4 list, and replaying the
//!   journaled session gives the same events, effects and counts; a frame kept whole answers
//!   live as its blanked bytes answer in replay (item 6).
//! - An auth failure, a missing auth reply at the 60 s deadline and a malformed frame each end
//!   their own epoch only; private numbers parse exactly; every recognised private state frame
//!   ends the epoch with no order, fill, position or resync event.
//!
//! Every key, account and frame here is synthetic: the frames are built from the shapes QFEX's
//! pages show (each test names the page), or are `fixtures/qfex/private` (its `SYNTHETIC` file).

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fbc_core::{
    ConnKey, ConnState, CtxCall, DecodeError, Effect, Effects, EncodeCtx, ExecCodec, ExecEvent,
    ExecSink, HttpFailure, HttpResponse, HttpTag, Inbound, LimitScope, MonoNs, Namespace,
    NonceBlock, NotSentReason, OpKind, PathStamps, RateCharge, RawFrame, Readiness, RpcId, Secret,
    Secrets, SpecTable, Stamp, StreamId, SubmitOutcome, TimerTag, TrafficClass, VenueCaps,
    VenueCommand, VenueConfig, VenueFactory, VenueMeta, Via, WallNs, check_redactions, dispatch,
};
use fbc_journal::{JournalReader, JournalWriter, Record, RedactionKey};
use fbc_runtime::{RateLimiter, RateUserKey, Request, SafetyReserve};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::{
    AUTH_DEADLINE, CHANNELS, ErrCode, OrderStatus, PrivateState, QfexExec, TRADE,
};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use rust_decimal::Decimal;
use serde_json::Value;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// The authenticate page's success reply, and the AsyncAPI's.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;
const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
/// What the codec answers a frame it cannot read with.
const NOT_AN_OBJECT: DecodeError = DecodeError::Malformed("QFEX trade frame is not a JSON object");
const INVALID_STATE: DecodeError = DecodeError::Malformed("invalid QFEX private state");

// ----- Building and driving a codec -----

fn cfg(account: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert(auth::ACCOUNT_ID, account);
    cfg.insert(
        auth::ACCOUNT_KEY,
        if account == "primary" { "4" } else { "9" },
    );
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

fn fresh(account: &str) -> QfexExec {
    QfexExec::new(
        TradeAuth::new(&cfg(account), secrets()).unwrap(),
        Duration::from_secs(2),
    )
}

fn caps() -> VenueCaps {
    Qfex.caps(&cfg("primary")).unwrap()
}

/// The context of the `n`th opening: its wall time and its two nonces.
fn ctx(n: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_000_000_000_000_000 + n as i64 * 1_000_000_000),
        mono: MonoNs(n),
        nonces: NonceBlock::new(vec![n, u64::MAX - n]),
    }
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

/// What one call gave.
#[derive(Debug, PartialEq)]
struct Out {
    result: Result<(), DecodeError>,
    events: Vec<ExecEvent>,
    effects: Vec<Effect>,
}

impl Out {
    fn quiet() -> Out {
        Out {
            result: Ok(()),
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    fn effects(effects: Vec<Effect>) -> Out {
        Out {
            effects,
            ..Out::quiet()
        }
    }

    /// Whether it ended its epoch, and only that.
    fn reconnects(&self) -> bool {
        self.events.is_empty()
            && matches!(
                self.effects.as_slice(),
                [Effect::Reconnect { stream: TRADE, .. }]
            )
    }
}

fn on(codec: &mut QfexExec, stream: StreamId, frame: RawFrame<'_>) -> Out {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, Namespace::new(1), |scope| {
        codec.on_frame(stream, frame, scope, &SpecTable::new(), &mut sink, &mut fx)
    });
    Out {
        result,
        events: sink.0,
        effects: fx.take(),
    }
}

fn text(codec: &mut QfexExec, frame: &str) -> Out {
    on(codec, TRADE, RawFrame::Text(frame))
}

fn open(codec: &mut QfexExec, n: u64) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(n), &mut fx);
    fx.take()
}

fn timer(codec: &mut QfexExec, tag: TimerTag) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_timer(tag, &ctx(0), &mut fx);
    fx.take()
}

/// The tag of the authentication deadline among `effects`.
fn deadline(effects: &[Effect]) -> TimerTag {
    match effects {
        [_, Effect::Timer { tag, after }] if *after == AUTH_DEADLINE => *tag,
        _ => panic!("no deadline in {effects:?}"),
    }
}

/// QFEX's confirmation of the subscription to `channel`.
fn confirm(channel: &str) -> String {
    format!(r#"{{"subscribed":"{channel}"}}"#)
}

/// Confirms the three subscriptions of an epoch whose auth reply came, checking the last
/// confirmation, and only it, reported the stream authenticated (FBC-bsou).
fn confirm_all(codec: &mut QfexExec) {
    for channel in &CHANNELS[..2] {
        let out = text(codec, &confirm(channel));
        assert_eq!(out, Out::quiet(), "{channel}");
    }
    let out = text(codec, &confirm(CHANNELS[2]));
    assert_eq!(out.events, [authenticated()], "{out:?}");
    assert!(out.effects.is_empty() && out.result.is_ok(), "{out:?}");
}

/// Opens epoch `n` and authenticates it, checking it did: the auth reply sends the three
/// subscriptions and reports nothing, and their last confirmation reports the stream.
fn authenticate(codec: &mut QfexExec, n: u64) {
    open(codec, n);
    let out = text(codec, AUTHENTICATED);
    assert!(out.events.is_empty(), "{out:?}");
    assert_eq!(out.effects.len(), 3);
    confirm_all(codec);
}

fn authed(account: &str) -> QfexExec {
    let mut codec = fresh(account);
    authenticate(&mut codec, 1);
    codec
}

fn authenticated() -> ExecEvent {
    ExecEvent::Conn {
        stream: TRADE,
        state: ConnState::Authenticated,
    }
}

fn held(op: Option<OpKind>, retry_after: Option<Duration>) -> Effect {
    Effect::RateLimited {
        stream: TRADE,
        charge: op.map(|op| RateCharge::one(op, None)),
        retry_after,
    }
}

fn blanked(bytes: &[u8], spans: &[core::ops::Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        out[span.start as usize..span.end as usize].fill(b'2');
    }
    out
}

fn redacted(codec: &QfexExec, frame: &str) -> String {
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(frame)));
    check_redactions(frame.as_bytes(), spans.body()).unwrap();
    String::from_utf8(blanked(frame.as_bytes(), spans.body())).unwrap()
}

fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

/// A nested err, as QFEX's errors page and AsyncAPI write one, echoing `echo`.
fn nested(code: &str, message: &str, echo: &str) -> String {
    format!(r#"{{"err":{{"error_code":"{code}","message":{message},"incoming_message":{echo}}}}}"#)
}

/// A flat err, as QFEX's rate page writes one.
fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":{message},"incoming_message":{echo}}}"#
    )
}

/// An `add_order` echo in the request's `{type, params}` form, as the errors page shows one.
const ADD_ORDER: &str = r#"{"type":"add_order","params":{"symbol":"AAPL-USD","side":"BUY","order_type":"LIMIT","order_time_in_force":"GTC","quantity":1,"price":200.5,"client_order_id":"echo-cid-0001","reduce_only":false}}"#;

// ----- Authentication and subscriptions -----

#[test]
fn authentication_then_the_three_subscriptions_for_a_primary_and_a_subaccount() {
    for (account, reply, spans) in [("primary", AUTH_OK, 2), (SUB, AUTHENTICATED, 3)] {
        let mut codec = fresh(account);
        assert_eq!(codec.nonces_for(CtxCall::Open(TRADE)), TradeAuth::NONCES);
        assert_eq!(codec.nonces_for(CtxCall::Open(StreamId(1))), 0);
        assert_eq!(codec.nonces_for(CtxCall::Resync), 0);
        assert_eq!(codec.nonces_for(CtxCall::Timer(TimerTag(1))), 0);

        let effects = open(&mut codec, 1);
        let [
            Effect::Send {
                stream: TRADE,
                frame,
                rpc: None,
                class: TrafficClass::Safety,
                charge,
            },
            Effect::Timer { after, .. },
        ] = effects.as_slice()
        else {
            panic!("{effects:?}")
        };
        assert_eq!(*after, AUTH_DEADLINE);
        assert_eq!(*after, Duration::from_secs(60));
        assert_eq!(*charge, RateCharge::one(OpKind::Control, None));
        let body: Value = serde_json::from_slice(frame.bytes()).unwrap();
        assert_eq!(body["type"], "auth");
        assert_eq!(body["params"]["hmac"]["public_key"], PUBLIC);
        assert_eq!(
            body["params"].get("account_id").and_then(Value::as_str),
            (account != "primary").then_some(SUB)
        );
        assert_eq!(frame.redactions().len(), spans);
        let shown = format!("{frame:?}");
        assert!(!shown.contains(PUBLIC) && !shown.contains(SUB));

        let out = text(&mut codec, reply);
        assert_eq!(out.result, Ok(()));
        // Reported only once QFEX confirms the three subscriptions (FBC-bsou).
        assert!(out.events.is_empty(), "{out:?}");
        assert_eq!(out.effects.len(), CHANNELS.len());
        for (effect, channel) in out
            .effects
            .iter()
            .zip(["order_responses", "fills", "positions"])
        {
            let Effect::Send {
                stream: TRADE,
                frame,
                rpc: None,
                class: TrafficClass::Safety,
                charge,
            } = effect
            else {
                panic!("{effect:?}")
            };
            assert_eq!(
                frame.bytes(),
                format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#)
                    .as_bytes()
            );
            assert!(frame.redactions().is_empty());
            // One tenth of a weight unit: the rate page's 0.1 for a subscribe.
            assert_eq!(*charge, RateCharge::one(OpKind::Subscribe, None));
        }
        confirm_all(&mut codec);
        assert_eq!(codec.counts().subscribed, 3);
    }
}

// ----- Rate refusals -----

/// Each documented rate refusal and the hold it gives: QFEX's rate page (the flat envelope and
/// the seconds form) and errors page (the nested envelope, its `cancel_all_orders` example and
/// the milliseconds form), and the AsyncAPI's `RATE_LIMITED` order status.
fn refusals() -> Vec<(String, Effect)> {
    let secs = |n| Some(Duration::from_secs(n));
    let ms = |n| Some(Duration::from_millis(n));
    let seconds = |n: u64| format!(r#""Rate limit exceeded, retry after {n} seconds""#);
    let millis = |n: u64| format!(r#""Too many requests. Please retry after {n}ms.""#);
    vec![
        (
            flat("RateLimited", &seconds(7), ADD_ORDER),
            held(Some(OpKind::Place), secs(7)),
        ),
        (
            flat("RateLimited", &millis(250), r#"{"type":"cancel_order","params":{"order_id":"11111111-1111-4111-8111-111111111111"}}"#),
            held(Some(OpKind::Cancel), ms(250)),
        ),
        (
            nested("RateLimited", &seconds(3), r#"{"type":"modify_order","params":{"order_id":"11111111-1111-4111-8111-111111111111","price":201}}"#),
            held(Some(OpKind::Amend), secs(3)),
        ),
        (
            nested("RateLimited", &millis(200), r#"{"type":"cancel_all_orders","params":{}}"#),
            held(Some(OpKind::CancelAll), ms(200)),
        ),
        // A message in neither form: no hint, so at least the declared window.
        (
            nested("RateLimited", r#""Too many requests.""#, r#"{"type":"get_user_orders","params":{}}"#),
            held(Some(OpKind::Query), None),
        ),
        // A request type the codec maps to no bucket: every frame budget.
        (
            nested("RateLimited", &seconds(5), r#"{"type":"cancel_on_disconnect","params":{"enabled":true}}"#),
            held(None, secs(5)),
        ),
        // The documented envelope example: no hint, and its message may be null.
        (
            nested("RateLimited", "null", ADD_ORDER),
            held(Some(OpKind::Place), None),
        ),
        // A string echo names no bucket; a zero hint is no hint.
        (
            flat("RateLimited", &seconds(0), r#""subscribe""#),
            held(None, None),
        ),
        (
            r#"{"err":{"error_code":"RateLimited","message":"Too many requests. Please retry after 40ms."}}"#
                .to_owned(),
            held(None, ms(40)),
        ),
        (
            r#"{"order_response":{"order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","status":"RATE_LIMITED","client_order_id":"synthetic-cid-1"}}"#
                .to_owned(),
            held(None, None),
        ),
    ]
}

#[test]
fn every_documented_rate_refusal_holds_the_budget_and_settles_or_reconnects_nothing() {
    for account in ["primary", SUB] {
        let mut codec = authed(account);
        for (frame, hold) in refusals() {
            assert_eq!(
                text(&mut codec, &frame),
                Out::effects(vec![hold]),
                "{frame}"
            );
        }
        let counts = codec.counts();
        assert_eq!(counts.errs(ErrCode::RateLimited), 9);
        assert_eq!(counts.rate_holds, 10);
        // The epoch is still open and authenticated: a request's err is still only counted.
        let err = nested("InvalidOrder", "null", ADD_ORDER);
        assert_eq!(text(&mut codec, &err), Out::quiet());
        // And nothing answered a request: one sent would still end Unknown at its deadline.
        let mut sink = Sink::default();
        codec.on_rpc_timeout(RpcId(17), &mut sink);
        assert_eq!(
            sink.0,
            [ExecEvent::Outcome {
                rpc: RpcId(17),
                item: None,
                outcome: SubmitOutcome::Unknown
            }]
        );
    }
    // A RateLimited err holds before authentication too, and the epoch stays open for its reply
    // and the confirmations.
    let mut codec = fresh(SUB);
    open(&mut codec, 1);
    let (frame, hold) = refusals().remove(3);
    assert_eq!(text(&mut codec, &frame), Out::effects(vec![hold.clone()]));
    assert!(text(&mut codec, AUTHENTICATED).events.is_empty());
    assert_eq!(text(&mut codec, &frame), Out::effects(vec![hold]));
    confirm_all(&mut codec);
}

#[test]
fn a_hold_reaches_the_budget_the_primary_and_its_subaccounts_share() {
    let limits = caps().exec_limits.unwrap();
    assert!(limits.iter().all(|limit| limit.scope == LimitScope::User));
    let reserve = SafetyReserve::percent(50).unwrap();
    let user = RateUserKey::new(0x2eb0_0001);
    let primary_budget = RateLimiter::for_user(user, &limits, reserve).unwrap();
    let sub_budget = RateLimiter::for_user(user, &limits, reserve).unwrap();
    let conn = ConnKey { conn: 1, epoch: 1 };
    let now = tokio::time::Instant::now();
    let at = |secs| now + Duration::from_secs(secs);
    let request = |op, class| Request {
        charge: RateCharge::one(op, None),
        via: Via::Frame,
        class,
    };
    let place = request(OpKind::Place, TrafficClass::Normal);
    let cancel = request(OpKind::Cancel, TrafficClass::Safety);
    let mut primary = authed("primary");
    let mut sub = authed(SUB);
    let refuse = |codec: &mut QfexExec, frame: &str, budget: &RateLimiter, when| {
        let out = text(codec, frame);
        let [
            Effect::RateLimited {
                charge,
                retry_after,
                ..
            },
        ] = out.effects.as_slice()
        else {
            panic!("{out:?}")
        };
        budget.provider_rejected(when, conn, *charge, *retry_after);
    };

    // The subaccount's general refusal holds the primary's places, not its cancels.
    let general = flat(
        "RateLimited",
        r#""Rate limit exceeded, retry after 4 seconds""#,
        ADD_ORDER,
    );
    refuse(&mut sub, &general, &sub_budget, now);
    assert!(primary_budget.charge(now, conn, &[place]).is_err());
    assert!(primary_budget.charge(now, conn, &[cancel]).is_ok());
    assert!(primary_budget.charge(at(5), conn, &[place]).is_ok());
    // The primary's cancel refusal holds the subaccount's cancels, not its places.
    let cancels = nested(
        "RateLimited",
        r#""Too many requests. Please retry after 6000ms.""#,
        r#"{"type":"cancel_order","params":{}}"#,
    );
    refuse(&mut primary, &cancels, &primary_budget, at(5));
    assert!(sub_budget.charge(at(5), conn, &[cancel]).is_err());
    assert!(sub_budget.charge(at(5), conn, &[place]).is_ok());
    // A RATE_LIMITED order names no bucket and no hint: both, for the declared window.
    let order = r#"{"order_response":{"status":"RATE_LIMITED"}}"#;
    refuse(&mut sub, order, &sub_budget, at(12));
    for req in [place, cancel] {
        assert!(primary_budget.charge(at(13), conn, &[req]).is_err());
    }
}

// ----- Errs and acks -----

/// A request's documented codes: counted, settling nothing, the epoch kept open.
const REQUEST_CODES: [ErrCode; 5] = [
    ErrCode::InvalidJsonFormat,
    ErrCode::InvalidParameter,
    ErrCode::ServerError,
    ErrCode::InvalidOrder,
    ErrCode::InvalidOrderId,
];

/// The session's documented codes: each ends the epoch.
const SESSION_CODES: [ErrCode; 4] = [
    ErrCode::AlreadyAuthenticated,
    ErrCode::PermissionDenied,
    ErrCode::KycRequired,
    ErrCode::TncRequired,
];

#[test]
fn once_authenticated_a_request_err_is_counted_and_a_session_err_ends_its_own_epoch_only() {
    let mut codec = authed(SUB);
    let echoes = [
        r#"{"type":"add_order","params":{"side":"BUY","quantity":1}}"#,
        r#"{"type":"modify_order","params":{"order_id":"11111111-1111-4111-8111-111111111111","price":-1}}"#,
        r#"{"type":"get_user_trades","params":{}}"#,
        ADD_ORDER,
        r#"{"type":"cancel_order","params":{"order_id":"not-a-uuid"}}"#,
    ];
    for (code, echo) in REQUEST_CODES.into_iter().zip(echoes) {
        let frame = nested(code.wire(), r#""as the errors page says""#, echo);
        assert_eq!(text(&mut codec, &frame), Out::quiet(), "{frame}");
        assert_eq!(codec.counts().errs(code), 1);
    }
    // The flat envelope, and an err that echoes nothing, alike.
    assert_eq!(
        text(&mut codec, &flat("InvalidOrder", "null", "{}")),
        Out::quiet()
    );
    let bare = r#"{"err":{"error_code":"ServerError","message":"try again shortly"}}"#;
    assert_eq!(text(&mut codec, bare), Out::quiet());
    assert_eq!(codec.counts().errs(ErrCode::InvalidOrder), 2);
    assert_eq!(codec.counts().errs(ErrCode::ServerError), 2);

    // Each session code, and an undocumented one, ends the epoch it came in; the next epoch
    // authenticates and keeps a request's err open again.
    let get_order =
        r#"{"type":"get_order","params":{"order_id":"11111111-1111-4111-8111-111111111111"}}"#;
    let ends: Vec<String> = SESSION_CODES
        .iter()
        .map(|code| nested(code.wire(), "null", get_order))
        .chain([
            nested("SomethingQfexAddedLater", r#""new""#, get_order),
            flat("PermissionDenied", r#""not permitted""#, "{}"),
            r#"{"err":{"message":"no code at all"}}"#.to_owned(),
        ])
        .collect();
    for (n, frame) in ends.iter().enumerate() {
        let out = text(&mut codec, frame);
        assert!(out.reconnects() && out.result.is_ok(), "{frame}: {out:?}");
        // Nothing more of that epoch is read.
        assert!(text(&mut codec, AUTHENTICATED).reconnects());
        authenticate(&mut codec, 2 + n as u64);
        let err = nested("InvalidParameter", "null", ADD_ORDER);
        assert_eq!(text(&mut codec, &err), Out::quiet());
    }
    let counts = codec.counts();
    for code in SESSION_CODES {
        let want = if code == ErrCode::PermissionDenied {
            2
        } else {
            1
        };
        assert_eq!(counts.errs(code), want, "{code:?}");
    }
    assert_eq!(counts.undocumented_errs, 2);
    assert_eq!(
        counts.errs(ErrCode::InvalidParameter),
        1 + ends.len() as u64
    );
    assert_eq!(counts.rate_holds, 0);
}

#[test]
fn before_authentication_every_err_but_a_rate_refusal_ends_the_epoch() {
    for code in ErrCode::ALL
        .into_iter()
        .filter(|c| *c != ErrCode::RateLimited)
    {
        let mut codec = fresh("primary");
        open(&mut codec, 1);
        let out = text(&mut codec, &nested(code.wire(), "null", ADD_ORDER));
        assert!(out.reconnects() && out.result.is_ok(), "{code:?}: {out:?}");
        // The next epoch is not affected.
        authenticate(&mut codec, 2);
    }
}

#[test]
fn a_generic_ack_is_recognised_and_keeps_the_epoch_open_settling_nothing() {
    // The AsyncAPI's `ack`: `{"ack":{"incoming_message":{...}}}`, tied to no command.
    let acks = [
        format!(r#"{{"ack":{{"incoming_message":{ADD_ORDER}}}}}"#),
        r#"{"ack":{"incoming_message":{}}}"#.to_owned(),
    ];
    let mut codec = fresh(SUB);
    open(&mut codec, 1);
    // Before authentication too: the deadline still bounds the wait for the reply and the
    // confirmations, which an ack is not (FBC-bsou).
    assert_eq!(text(&mut codec, &acks[0]), Out::quiet());
    assert!(text(&mut codec, AUTHENTICATED).events.is_empty());
    assert_eq!(text(&mut codec, &acks[1]), Out::quiet());
    confirm_all(&mut codec);
    for ack in &acks {
        assert_eq!(text(&mut codec, ack), Out::quiet());
    }
    assert_eq!(codec.counts().acks, 4);
    let err = nested("InvalidOrder", "null", ADD_ORDER);
    assert_eq!(text(&mut codec, &err), Out::quiet());
}

// ----- The journal and replay -----

/// One input of a session.
enum Step {
    Open(u64),
    Frame(String),
    /// The current epoch's authentication deadline fires.
    Deadline,
    RpcTimeout(u64),
}

/// Runs `steps` through `codec`, handing each frame to `journal` first, as the runtime does,
/// and reading the frame `journal` gives back: the live frame to record it, the journaled one
/// in replay.
fn run(
    codec: &mut QfexExec,
    steps: &[Step],
    mut journal: impl FnMut(&QfexExec, &str) -> String,
) -> Vec<Out> {
    let mut tag = TimerTag(0);
    let mut outs = Vec::new();
    for step in steps {
        let out = match step {
            Step::Open(n) => {
                let effects = open(codec, *n);
                tag = deadline(&effects);
                Out::effects(effects)
            }
            Step::Frame(frame) => {
                let frame = journal(codec, frame);
                text(codec, &frame)
            }
            Step::Deadline => Out::effects(timer(codec, tag)),
            Step::RpcTimeout(rpc) => {
                let mut sink = Sink::default();
                codec.on_rpc_timeout(RpcId(*rpc), &mut sink);
                Out {
                    events: sink.0,
                    ..Out::quiet()
                }
            }
        };
        outs.push(out);
    }
    outs
}

fn stamp(seq: u64) -> Stamp {
    Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: MonoNs(seq),
        recv_wall: WallNs(1_760_000_000_000_000_000),
        conn: ConnKey { conn: 0, epoch: 1 },
    }
}

/// Every file under `dir`, read whole.
fn files(dir: &Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            out.push(fs::read(&path).unwrap());
        }
    }
    out
}

fn twos(n: usize) -> String {
    "2".repeat(n)
}

#[test]
fn the_journal_keeps_errs_and_acks_as_0134_lists_and_their_replay_gives_the_same_events_effects_and_counts()
 {
    let frame = |s: &str| Step::Frame(s.to_owned());
    let get_order =
        r#"{"type":"get_order","params":{"order_id":"11111111-1111-4111-8111-111111111111"}}"#;
    let mut steps = vec![Step::Open(1), frame(AUTHENTICATED)];
    steps.extend(CHANNELS.map(|c| frame(&confirm(c))));
    steps.extend(refusals().into_iter().map(|(f, _)| Step::Frame(f)));
    for code in REQUEST_CODES {
        steps.push(Step::Frame(nested(code.wire(), r#""rejected""#, ADD_ORDER)));
    }
    steps.extend([
        Step::Frame(format!(r#"{{"ack":{{"incoming_message":{ADD_ORDER}}}}}"#)),
        frame(r#"{"err":{"error_code":"InvalidParameter","message":"price out of bounds"}}"#),
        frame(
            r#"{"err":{"error_code":"InvalidOrder","message":"bad","incoming_message":{"type":"add_order"},"request_id":17},"seq":3}"#,
        ),
        Step::RpcTimeout(5),
        Step::Frame(nested("PermissionDenied", "null", get_order)),
    ]);
    // Each epoch below ends on its last frame.
    let ending = [
        nested("TncRequired", "null", get_order),
        nested("KycRequired", "null", get_order),
        nested("SomethingQfexAddedLater", "null", get_order),
        // Kept whole: an echo of authentication (the errors page's AlreadyAuthenticated example),
        // an escape, a repeated member, a malformed frame, and a member name holding the key.
        nested("AlreadyAuthenticated", "null", r#"{"type":"auth","params":{}}"#),
        nested("AlreadyAuthenticated", "null", get_order),
        nested("InvalidOrder", r#""bad \"price\"""#, ADD_ORDER),
        r#"{"err":{"error_code":"InvalidOrder","error_code":"InvalidOrder","incoming_message":{}}}"#
            .to_owned(),
        r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"type":"add_order""#.to_owned(),
        nested(
            "InvalidOrder",
            "null",
            &format!(r#"{{"type":"add_order","params":{{"{PUBLIC}":1}}}}"#),
        ),
        fixture("order_response.json"),
    ];
    for (n, last) in ending.iter().enumerate() {
        steps.extend([Step::Open(2 + n as u64), frame(AUTH_OK)]);
        steps.extend(CHANNELS.map(|c| frame(&confirm(c))));
        steps.push(Step::Frame(last.clone()));
    }
    // And the epochs that end before authentication.
    steps.extend([
        Step::Open(20),
        Step::Frame(nested("InvalidParameter", "null", "{}")),
        Step::Open(21),
        Step::Frame(refusals().remove(0).0),
        Step::Frame(r#"{"ack":{"incoming_message":{}}}"#.to_owned()),
        Step::Deadline,
        Step::Open(22),
        frame(r#"{"authenticated":false}"#),
    ]);

    // Live, journaling every frame as the runtime would, with the codec's own spans.
    let root = std::env::temp_dir().join(format!("fbc-qfex-zeb0-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let key = Arc::new(RedactionKey::new(&[9; 32]).unwrap());
    let mut writer = JournalWriter::create(&root, 0, key).unwrap();
    let mut journaled = Vec::new();
    let mut live = fresh(SUB);
    let live_outs = run(&mut live, &steps, |codec, frame| {
        let raw = RawFrame::Text(frame);
        let spans = codec.redact_inbound(Inbound::Frame(raw));
        let record = Record::inbound_redacted(stamp(journaled.len() as u64), raw, &spans).unwrap();
        writer
            .append(WallNs(1_760_000_000_000_000_000), &record)
            .unwrap();
        journaled.push(frame.to_owned());
        frame.to_owned()
    });
    writer.flush().unwrap();
    drop(writer);

    // On disk: no account material, signature or echoed value in clear.
    let disk: Vec<u8> = files(&root).concat();
    let has = |needle: &str| disk.windows(needle.len()).any(|w| w == needle.as_bytes());
    assert!(has("RateLimited") && has("\"incoming_message\""));
    for hidden in [
        PUBLIC,
        SUB,
        "echo-cid-0001",
        "AAPL-USD\",\"side\":\"BUY\",\"order_type",
    ] {
        assert!(!has(hidden), "{hidden} reached the journal");
    }

    // Read back: each frame blanked as 0134 items 2 to 4 say.
    let back: Vec<String> = JournalReader::open(&root, 0)
        .unwrap()
        .map(|record| match record.unwrap() {
            Record::Inbound { bytes, .. } => String::from_utf8(bytes.0).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect();
    let _ = fs::remove_dir_all(&root);
    assert_eq!(back.len(), journaled.len());
    let read = |live: &str| back[journaled.iter().position(|f| f == live).unwrap()].clone();
    // The add_order echo's params, each value a span of its own, its quotes kept.
    let params = format!(
        r#"{{"type":"add_order","params":{{"symbol":"{}","side":"222","order_type":"22222","order_time_in_force":"222","quantity":2,"price":22222,"client_order_id":"{}","reduce_only":22222}}}}"#,
        twos(8),
        twos(13)
    );
    let (flat_7s, _) = &refusals()[0];
    assert_eq!(
        read(flat_7s),
        format!(
            r#"{{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 7 seconds","incoming_message":{params}}}"#
        )
    );
    let (cancel_all, _) = &refusals()[3];
    assert_eq!(&read(cancel_all), cancel_all, "nothing in it to span");
    let (unhinted, _) = &refusals()[4];
    assert_eq!(
        read(unhinted),
        format!(
            r#"{{"err":{{"error_code":"RateLimited","message":"{}","incoming_message":{{"type":"get_user_orders","params":{{}}}}}}}}"#,
            twos(18)
        )
    );
    let (unmapped, _) = &refusals()[5];
    assert_eq!(
        read(unmapped),
        format!(
            r#"{{"err":{{"error_code":"RateLimited","message":"Rate limit exceeded, retry after 5 seconds","incoming_message":{{"type":"{}","params":{{"enabled":2222}}}}}}}}"#,
            twos(20)
        )
    );
    let (null_message, _) = &refusals()[6];
    assert_eq!(
        read(null_message),
        format!(
            r#"{{"err":{{"error_code":"RateLimited","message":2222,"incoming_message":{params}}}}}"#
        )
    );
    let (string_echo, _) = &refusals()[7];
    assert_eq!(
        read(string_echo),
        r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 0 seconds","incoming_message":"222222222"}"#
    );
    // Each request code, its message a span and its echo's params spans.
    for code in REQUEST_CODES {
        assert_eq!(
            read(&nested(code.wire(), r#""rejected""#, ADD_ORDER)),
            format!(
                r#"{{"err":{{"error_code":"{}","message":"22222222","incoming_message":{params}}}}}"#,
                code.wire()
            )
        );
    }
    // Each session code alike, the get_order echo's order id a span.
    let order_echo = format!(
        r#"{{"type":"get_order","params":{{"order_id":"{}"}}}}"#,
        twos(36)
    );
    for code in [
        "PermissionDenied",
        "TncRequired",
        "KycRequired",
        "AlreadyAuthenticated",
    ] {
        assert_eq!(
            read(&nested(code, "null", get_order)),
            format!(
                r#"{{"err":{{"error_code":"{code}","message":2222,"incoming_message":{order_echo}}}}}"#
            )
        );
    }
    // The other rate refusals: each kept but for its params; the order status untouched.
    let (flat_ms, _) = &refusals()[1];
    assert_eq!(
        read(flat_ms),
        format!(
            r#"{{"type":"Err","error_code":"RateLimited","message":"Too many requests. Please retry after 250ms.","incoming_message":{{"type":"cancel_order","params":{{"order_id":"{}"}}}}}}"#,
            twos(36)
        )
    );
    let (nested_3s, _) = &refusals()[2];
    assert_eq!(
        read(nested_3s),
        format!(
            r#"{{"err":{{"error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds","incoming_message":{{"type":"modify_order","params":{{"order_id":"{}","price":222}}}}}}}}"#,
            twos(36)
        )
    );
    for (frame, _) in &refusals()[8..] {
        assert_eq!(
            &read(frame),
            frame,
            "it echoes nothing and names no account"
        );
    }
    assert_eq!(
        read(&format!(r#"{{"ack":{{"incoming_message":{ADD_ORDER}}}}}"#)),
        format!(r#"{{"ack":{{"incoming_message":{params}}}}}"#)
    );
    assert_eq!(
        read(
            r#"{"err":{"error_code":"InvalidOrder","message":"bad","incoming_message":{"type":"add_order"},"request_id":17},"seq":3}"#
        ),
        r#"{"err":{"error_code":"InvalidOrder","message":"222","incoming_message":{"type":"add_order"},"request_id":22},"seq":2}"#
    );
    assert_eq!(
        read(&nested("SomethingQfexAddedLater", "null", get_order)),
        format!(
            r#"{{"err":{{"error_code":"{}","message":2222,"incoming_message":{{"type":"get_order","params":{{"order_id":"{}"}}}}}}}}"#,
            twos(23),
            twos(36)
        )
    );
    // An err echoing nothing keeps what the account's redaction keeps: here, all of it.
    let bare = r#"{"err":{"error_code":"InvalidParameter","message":"price out of bounds"}}"#;
    assert_eq!(read(bare), bare);
    // Each frame item 2 keeps whole: one span, a run of 2s.
    for whole in &ending[3..9] {
        if whole.contains("get_order") {
            continue;
        }
        assert_eq!(read(whole), twos(whole.len()), "{whole}");
    }
    // A private state frame: its user id only (the account's redaction).
    assert!(!read(&ending[9]).contains("5a5a5a5a-0000-4000-8000-0000000000a1"));

    // Replay: a fresh codec, handed the journaled frames, gives what live gave.
    let mut replay = fresh(SUB);
    let mut next = back.into_iter();
    let replay_outs = run(&mut replay, &steps, |_, _| next.next().unwrap());
    assert_eq!(live_outs.len(), replay_outs.len());
    for (at, (a, b)) in live_outs.iter().zip(&replay_outs).enumerate() {
        assert_eq!(a, b, "step {at}");
    }
    assert_eq!(live.counts(), replay.counts());
    assert_eq!(live.last_private(), replay.last_private());
    let counts = live.counts();
    assert_eq!(
        (
            counts.undocumented_errs,
            counts.acks,
            counts.rate_holds,
            counts.subscribed,
            counts.private_state,
            counts.undecodable,
        ),
        (1, 2, 11, 3 * (1 + ending.len() as u64), 1, 5)
    );
    assert_eq!(counts.errs(ErrCode::RateLimited), 10);
    // The frames kept whole are read as no err at all, so counted as undecodable only.
    assert_eq!(counts.errs(ErrCode::InvalidOrder), 2);
    // No step pushed an order, fill, position or resync event.
    for out in &live_outs {
        for event in &out.events {
            assert!(
                matches!(event, ExecEvent::Conn { .. } | ExecEvent::Outcome { .. }),
                "{event:?}"
            );
        }
    }
}

#[test]
fn a_frame_kept_whole_answers_live_as_its_blanked_bytes_answer_in_replay() {
    let whole = [
        nested(
            "AlreadyAuthenticated",
            "null",
            r#"{"type":"auth","params":{}}"#,
        ),
        flat(
            "RateLimited",
            r#""Rate limit exceeded, retry after 7 seconds""#,
            r#"{"type":"auth"}"#,
        ),
        nested("RateLimited", r#""Too many \"requests\"""#, ADD_ORDER),
        r#"{"err":{"error_code":"RateLimited","error_code":"RateLimited","incoming_message":{}}}"#
            .to_owned(),
        r#"{"err":{"error_code":"RateLimited","incoming_message":{"type":"add_order"}"#.to_owned(),
        r#"{"err":{"incoming_message":{}},"ack":{"incoming_message":{}}}"#.to_owned(),
        r#"{"order_response":{"incoming_message":{}}}"#.to_owned(),
        format!(
            r#"{{"err":{{"error_code":"InvalidOrder","incoming_message":{{"params":{{"n":0.{}1}}}}}}}}"#,
            "0".repeat(320)
        ),
    ];
    for frame in &whole {
        let mut live = authed(SUB);
        let mut replay = authed(SUB);
        let spans = live.redact_inbound(Inbound::Frame(RawFrame::Text(frame)));
        assert_eq!(spans.body(), vec![0..frame.len() as u32], "{frame}");
        let journaled = twos(frame.len());
        let got = text(&mut live, frame);
        assert_eq!(got, text(&mut replay, &journaled), "{frame}");
        assert_eq!(got.result, Err(NOT_AN_OBJECT), "{frame}");
        assert!(got.effects.len() == 1 && got.events.is_empty() && got.reconnects());
        assert_eq!(live.counts(), replay.counts());
        // Nothing the codec may read shows a refusal: no hold (0134 item 6).
        assert_eq!(live.counts().rate_holds, 0);
        // As any input that is not a JSON object is answered, before authentication too.
        for other in ["[1]", "\"s\"", "nope", "12"] {
            let mut codec = authed(SUB);
            assert_eq!(text(&mut codec, other), got, "{other}");
        }
        let mut waiting = fresh(SUB);
        open(&mut waiting, 1);
        assert_eq!(text(&mut waiting, frame), got);
    }
    // A binary frame is no JSON object either, and one that names an echo is kept whole.
    let mut codec = authed(SUB);
    let binary = br#"{"ack":{"incoming_message":{}}}"#;
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Binary(binary)));
    assert_eq!(spans.body(), vec![0..binary.len() as u32]);
    assert!(
        codec
            .redact_inbound(Inbound::Frame(RawFrame::Binary(b"")))
            .is_empty()
    );
    let out = on(&mut codec, TRADE, RawFrame::Binary(binary));
    assert_eq!(out.result, Err(NOT_AN_OBJECT));
    assert!(out.reconnects());
}

/// Varies err, ack and private frames byte by byte, and holds the codec to decision 0134 item
/// 5: whatever the frame, the codec answers it live as it answers what the journal keeps of it,
/// and the journal keeps that again unchanged.
#[test]
fn mutated_frames_decode_alike_live_and_from_what_the_journal_keeps() {
    let seeds = [
        flat("RateLimited", r#""Rate limit exceeded, retry after 7 seconds""#, ADD_ORDER),
        nested("RateLimited", r#""Too many requests. Please retry after 200ms.""#, r#"{"type":"cancel_all_orders","params":{}}"#),
        nested("InvalidOrder", "null", r#"{"type":"cancel_order","params":{"order_id":"o-1","n":[1,true]}}"#),
        format!(r#"{{"ack":{{"incoming_message":{ADD_ORDER}}}}}"#),
        nested("PermissionDenied", r#""no""#, r#"{"type":"auth","params":{"account_id":"5a5a5a5a-0000-4000-8000-00000000c0de"}}"#),
        r#"{"order_response":{"order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","status":"ACK","side":"BUY","price":200,"quantity":1,"quantity_remaining":1,"user_id":"u-1","trade_id":null}}"#.to_owned(),
        r#"{"subscribed":"fills"}"#.to_owned(),
    ];
    let alphabet = b"{}[]\":,\\0123456789aeE.-+ trnulfsTdy_";
    let mut state: u64 = 0x2eb0_5eed;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as usize
    };
    let mut live = fresh(SUB);
    let mut replay = fresh(SUB);
    let mut objects = 0;
    for round in 0..6_000u64 {
        let mut bytes = seeds[round as usize % seeds.len()].clone().into_bytes();
        for _ in 0..1 + next() % 3 {
            let at = next() % bytes.len();
            match next() % if bytes.len() > 1 { 3 } else { 2 } {
                0 => bytes[at] = alphabet[next() % alphabet.len()],
                1 => bytes.insert(at, alphabet[next() % alphabet.len()]),
                _ => {
                    bytes.remove(at);
                }
            }
        }
        let frame = String::from_utf8(bytes).unwrap();
        for codec in [&mut live, &mut replay] {
            authenticate(codec, round + 1);
        }
        let kept = redacted(&live, &frame);
        assert_eq!(redacted(&replay, &kept), kept, "{frame}");
        let got = text(&mut live, &frame);
        assert_eq!(got, text(&mut replay, &kept), "{frame}");
        objects += usize::from(got.result != Err(NOT_AN_OBJECT));
    }
    assert_eq!(live.counts(), replay.counts());
    assert_eq!(live.last_private(), replay.last_private());
    assert!(objects > 2_000, "only {objects} frames decoded as objects");
}

// ----- Ending an epoch -----

#[test]
fn an_auth_failure_a_missing_auth_reply_and_a_malformed_frame_each_end_their_own_epoch_only() {
    let mut codec = fresh("primary");
    let mut n = 0;
    let mut reopen = |codec: &mut QfexExec| {
        n += 1;
        open(codec, n)
    };
    // Every reply but the two successes is a failure.
    for reply in [
        r#"{"type":"auth","result":"failure"}"#,
        r#"{"type":"auth","result":"success","err":"x"}"#,
        r#"{"authenticated":false}"#,
        r#"{"authenticated":"true"}"#,
        r#"{"authenticated":true,"user_id":"u"}"#,
        r#"{"subscribed":"fills"}"#,
        &fixture("order_response.json"),
    ] {
        reopen(&mut codec);
        let out = text(&mut codec, reply);
        assert!(out.reconnects() && out.result.is_ok(), "{reply}: {out:?}");
    }
    // A malformed frame, and any input that is no JSON object, in every state.
    for frame in [
        "not-json",
        "{\"a\":",
        "[]",
        "17",
        "{\"a\":1,\"a\":1}",
        "{\"err\":{},\"type\":\"Err\"}",
    ] {
        reopen(&mut codec);
        let out = text(&mut codec, frame);
        assert!(out.result.is_err() && out.reconnects(), "{frame}: {out:?}");
        let mut authed = authed("primary");
        let out = text(&mut authed, frame);
        assert!(out.result.is_err() && out.reconnects(), "{frame}: {out:?}");
        // The next epoch is untouched.
        authenticate(&mut authed, 2);
    }
    let repeated = text(&mut authed("primary"), "{\"a\":1,\"a\":1}");
    assert_eq!(
        repeated.result,
        Err(DecodeError::Malformed("QFEX trade frame repeats a member"))
    );
    let two = text(&mut authed("primary"), "{\"err\":{},\"type\":\"Err\"}");
    assert_eq!(
        two.result,
        Err(DecodeError::Malformed("QFEX trade frame is two envelopes"))
    );
    let elsewhere = on(&mut authed("primary"), StreamId(1), RawFrame::Text(AUTH_OK));
    assert_eq!(elsewhere.result, Err(NOT_AN_OBJECT));
    assert!(elsewhere.reconnects());

    // The deadline: only the current epoch's, and only while the reply is awaited.
    let mut codec = fresh(SUB);
    let first = deadline(&open(&mut codec, 1));
    assert!(timer(&mut codec, TimerTag(first.0 + 1)).is_empty());
    let second = deadline(&open(&mut codec, 2));
    assert_ne!(first, second);
    assert!(
        timer(&mut codec, first).is_empty(),
        "an earlier epoch's deadline"
    );
    let fired = timer(&mut codec, second);
    assert!(Out::effects(fired).reconnects());
    assert!(
        timer(&mut codec, second).is_empty(),
        "the epoch already ended"
    );
    // After the reply it still ends the epoch while a confirmation is missing (FBC-bsou), and
    // once all three came it does nothing.
    let third = deadline(&open(&mut codec, 3));
    assert!(text(&mut codec, AUTH_OK).events.is_empty());
    assert!(Out::effects(timer(&mut codec, third)).reconnects());
    let fourth = deadline(&open(&mut codec, 4));
    assert!(text(&mut codec, AUTH_OK).events.is_empty());
    confirm_all(&mut codec);
    assert!(timer(&mut codec, fourth).is_empty());

    // An authentication that cannot be signed ends the epoch before anything is sent.
    let mut codec = fresh(SUB);
    let mut fx = Effects::new();
    let unsigned = EncodeCtx {
        nonces: NonceBlock::EMPTY,
        ..ctx(1)
    };
    codec.on_open(TRADE, &unsigned, &mut fx);
    assert!(Out::effects(fx.take()).reconnects());
    // A stream the codec does not serve opens nothing, and reads nothing.
    codec.on_open(StreamId(1), &ctx(2), &mut fx);
    assert!(fx.is_empty());
    assert!(text(&mut codec, r#"{"subscribed":"fills"}"#).reconnects());
}

// ----- Private state -----

#[test]
fn a_private_quantity_with_twenty_significant_digits_parses_exactly() {
    let quantity = "12345678901.234567891";
    let more = "12345678901.234567892";
    // Through f64 the two are one number, and neither is itself.
    let f = |s: &str| s.parse::<f64>().unwrap();
    assert_eq!(f(quantity), f(more));
    assert_ne!(f(quantity).to_string(), quantity);
    let order = |remaining: &str| {
        format!(
            r#"{{"order_response":{{"order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","status":"ACK","side":"BUY","price":200.5,"quantity":{quantity},"quantity_remaining":{remaining},"user_id":"5a5a5a5a-0000-4000-8000-0000000000a1","trade_id":null}}}}"#
        )
    };
    let mut codec = authed("primary");
    let out = text(&mut codec, &order(quantity));
    assert!(out.reconnects() && out.result.is_ok(), "{out:?}");
    let exact = |s: &str| Decimal::from_str_exact(s).unwrap();
    let Some(PrivateState::Order {
        status,
        price,
        quantity: read,
        remaining,
        ..
    }) = codec.last_private()
    else {
        panic!("{:?}", codec.last_private())
    };
    assert_eq!(*status, OrderStatus::Ack);
    assert_eq!(
        (*price, *read, *remaining),
        (exact("200.5"), exact(quantity), exact(quantity))
    );
    assert_eq!(read.to_string(), quantity);
    // One unit in the twentieth digit more than the quantity remains: refused, exactly.
    let mut codec = authed("primary");
    assert_eq!(text(&mut codec, &order(more)).result, Err(INVALID_STATE));
    // Exponents move the point exactly; a value a Decimal cannot hold is refused, not rounded.
    for (written, value) in [
        ("1.5e-3", "0.0015"),
        ("15E+2", "1500"),
        ("-2.5e1", "-25"),
        ("12.5e-1", "1.25"),
        ("1e0", "1"),
    ] {
        let fill = format!(
            r#"{{"fill_response":{{"trade_id":"55555555-5555-4555-8555-555555555555","order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","side":"SELL","price":200,"quantity":1,"fee":{written},"timestamp":1760000003.25}}}}"#
        );
        let mut codec = authed("primary");
        assert_eq!(text(&mut codec, &fill).result, Ok(()), "{written}");
        let Some(PrivateState::Fill { raw_fee, .. }) = codec.last_private() else {
            panic!()
        };
        assert_eq!(*raw_fee, exact(value), "{written}");
    }
    for written in [
        "1e65",
        "1.2345678901234567890123456789012",
        "1e-60",
        "79228162514264337593543950336",
    ] {
        let fill = format!(
            r#"{{"fill_response":{{"trade_id":"55555555-5555-4555-8555-555555555555","order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","side":"SELL","price":200,"quantity":1,"fee":{written},"timestamp":1}}}}"#
        );
        let mut codec = authed("primary");
        assert_eq!(
            text(&mut codec, &fill).result,
            Err(INVALID_STATE),
            "{written}"
        );
    }
}

/// Codex P2 on 02c2d231 in PR #226: an exponent at or past the ends of `i32`, or a mantissa
/// whose integer digits would overflow the decimal point's position, is refused as malformed
/// private state, never a panic or a padding of gigabytes of zeros.
#[test]
fn an_exponent_at_the_ends_of_i32_or_past_them_is_refused_not_a_panic() {
    let position = |written: &str| {
        format!(r#"{{"position_response":{{"symbol":"AAPL-USD","position":{written}}}}}"#)
    };
    for written in [
        // Codex's frame: serde_json accepts it, as a number that underflows to zero.
        "1e-2147483648",
        "1E-2147483648",
        "-1.5e-2147483648",
        "1e-2147483647",
        "0e-2147483648",
        "1e-99999999999",
        "1e-65",
        "1e65",
        // Integer digits far past what a Decimal holds, brought back by an exponent.
        &format!("1{}e-400", "0".repeat(500)),
    ] {
        let frame = position(written);
        assert!(serde_json::from_str::<Value>(&frame).is_ok(), "{written}");
        let mut codec = authed(SUB);
        let out = text(&mut codec, &frame);
        assert_eq!(out.result, Err(INVALID_STATE), "{written}");
        assert!(out.reconnects(), "{written}");
        assert_eq!(codec.last_private(), None, "{written}");
    }
    // Past f64's range serde_json refuses the frame itself: not a JSON object to the codec.
    for written in ["1e2147483647", "1e+2147483647", "-1e400"] {
        let frame = position(written);
        assert!(serde_json::from_str::<Value>(&frame).is_err(), "{written}");
        let out = text(&mut authed(SUB), &frame);
        assert_eq!(out.result, Err(NOT_AN_OBJECT), "{written}");
        assert!(out.reconnects(), "{written}");
    }
    // The ends of the range still read exactly.
    for (written, value) in [
        ("1e-28", "0.0000000000000000000000000001"),
        ("0e64", "0"),
        ("1e28", "10000000000000000000000000000"),
    ] {
        let mut codec = authed(SUB);
        assert_eq!(
            text(&mut codec, &position(written)).result,
            Ok(()),
            "{written}"
        );
        assert_eq!(
            codec.last_private(),
            Some(&PrivateState::Position {
                position: Decimal::from_str_exact(value).unwrap()
            }),
            "{written}"
        );
    }
}

#[test]
fn every_recognised_private_state_frame_ends_the_epoch_with_no_order_fill_position_or_resync_event()
{
    let id = |n: char| {
        n.to_string().repeat(8)
            + "-"
            + &n.to_string().repeat(4)
            + "-4"
            + &n.to_string().repeat(3)
            + "-8"
            + &n.to_string().repeat(3)
            + "-"
            + &n.to_string().repeat(12)
    };
    let caps = caps();
    let order = |n| {
        dispatch(&caps, Namespace::new(1), |scope| {
            scope.venue_order_id(&id(n)).unwrap()
        })
    };
    let trade = |n| {
        dispatch(&caps, Namespace::new(1), |scope| {
            scope.fill_id(&id(n)).unwrap()
        })
    };
    let dec = |s: &str| Decimal::from_str_exact(s).unwrap();
    let cancelled = fixture("order_response.json").replace("\"ACK\"", "\"CANCELLED\"");
    let user_trades = r#"{"user_trades":[{"trade_id":"55555555-5555-4555-8555-555555555555","symbol":"AAPL-USD","price":200.0,"quantity":1.0,"aggressor_side":"BUY","order_id":"11111111-1111-4111-8111-111111111111","client_order_id":"","fee":0.002,"timestamp":1760000003}],"count":1}"#;
    let observed_trades = r#"{"user_trades_response":[]}"#;
    let cases = [
        (
            fixture("order_response.json"),
            PrivateState::Order {
                order: order('1'),
                status: OrderStatus::Ack,
                price: dec("200.0"),
                quantity: dec("1.0"),
                remaining: dec("1.0"),
            },
        ),
        (
            cancelled.clone(),
            PrivateState::Order {
                order: order('1'),
                status: OrderStatus::Cancelled,
                price: dec("200.0"),
                quantity: dec("1.0"),
                remaining: dec("1.0"),
            },
        ),
        (
            fixture("fill_response.json"),
            PrivateState::Fill {
                trade: trade('5'),
                order: order('1'),
                price: dec("200.0"),
                quantity: dec("1.0"),
                raw_fee: dec("0.002"),
            },
        ),
        (
            fixture("all_orders_response.json"),
            PrivateState::OpenOrders {
                orders: vec![order('2'), order('3')],
            },
        ),
        (
            user_trades.to_owned(),
            PrivateState::Trades {
                trades: vec![trade('5')],
            },
        ),
        (
            observed_trades.to_owned(),
            PrivateState::Trades { trades: Vec::new() },
        ),
        (
            fixture("position_response.json"),
            PrivateState::Position {
                position: dec("1.0"),
            },
        ),
    ];
    for (frame, want) in cases {
        let mut codec = authed(SUB);
        let out = text(&mut codec, &frame);
        assert!(out.reconnects() && out.result.is_ok(), "{frame}: {out:?}");
        assert_eq!(codec.last_private(), Some(&want));
        assert_eq!(codec.counts().private_state, 1);
    }

    // Each documented shape refused when it is not as documented, the epoch ended all the same.
    let edit = |frame: &str, path: &[&str], value: Value| {
        let mut json: Value = serde_json::from_str(frame).unwrap();
        let mut at = &mut json;
        for step in path {
            at = match step.parse::<usize>() {
                Ok(n) => &mut at[n],
                Err(_) => &mut at[*step],
            };
        }
        *at = value;
        json.to_string()
    };
    let order_frame = fixture("order_response.json");
    let fill_frame = fixture("fill_response.json");
    let page = fixture("all_orders_response.json");
    let o = |key, value| edit(&order_frame, &["order_response", key], value);
    let f = |key, value| edit(&fill_frame, &["fill_response", key], value);
    let mut invalid = vec![
        o("status", "FILLED".into()),
        o("quantity_remaining", 2.into()),
        o("trade_id", "55555555-5555-4555-8555-555555555555".into()),
        o("trade_id", true.into()),
        o("order_id", "not-a-uuid".into()),
        o("order_id", Value::Null),
        o("side", "UNKNOWN".into()),
        o("symbol", "".into()),
        o("price", 0.into()),
        o("quantity", "1".into()),
        f("side", "UNKNOWN".into()),
        f("price", 0.into()),
        f("quantity", (-1).into()),
        f("timestamp", (-1).into()),
        f("fee", Value::Null),
        f("trade_id", "REDACTED".into()),
        f("order_id", "bad".into()),
        edit(&page, &["all_orders_response", "twaps"], Value::Null),
        edit(&page, &["all_orders_response", "orders"], serde_json::json!({})),
        edit(&page, &["all_orders_response", "orders", "1", "order_id"], "22222222-2222-4222-8222-222222222222".into()),
        edit(&page, &["all_orders_response", "orders", "0", "quantity_remaining"], (-1).into()),
        edit(&page, &["all_orders_response", "orders", "0"], 1.into()),
        edit(user_trades, &["count"], Value::Null),
        edit(user_trades, &["count"], (-1).into()),
        edit(user_trades, &["user_trades", "0", "fee"], Value::Null),
        edit(user_trades, &["user_trades"], serde_json::json!({})),
        edit(observed_trades, &["count"], "1".into()),
        r#"{"user_trades":[{"trade_id":"55555555-5555-4555-8555-555555555555","symbol":"AAPL-USD","price":1,"quantity":1,"order_id":"11111111-1111-4111-8111-111111111111","fee":0,"timestamp":1},{"trade_id":"55555555-5555-4555-8555-555555555555","symbol":"AAPL-USD","price":1,"quantity":1,"order_id":"11111111-1111-4111-8111-111111111111","fee":0,"timestamp":1}],"count":2}"#.to_owned(),
        r#"{"position_response":{"symbol":"AAPL-USD"}}"#.to_owned(),
        r#"{"position_response":[]}"#.to_owned(),
        // A RATE_LIMITED order holds only as the frame's one member; otherwise it is an order.
        r#"{"order_response":{"status":"RATE_LIMITED"},"x":1}"#.to_owned(),
        edit(&fill_frame, &["position_response"], serde_json::json!({"symbol":"AAPL-USD","position":0})),
    ];
    // The page's companion may be its stop orders instead of its TWAPs.
    let stop_orders = edit(
        &page,
        &["all_orders_response", "stop_orders"],
        serde_json::json!([]),
    );
    let stop_orders = edit(&stop_orders, &["all_orders_response", "twaps"], Value::Null);
    let mut codec = authed(SUB);
    assert_eq!(text(&mut codec, &stop_orders).result, Ok(()));
    invalid.push(
        edit(&page, &["all_orders_response", "stop_orders"], 1.into())
            .replace("\"twaps\"", "\"other\""),
    );
    for frame in &invalid {
        let mut codec = authed(SUB);
        let out = text(&mut codec, frame);
        assert_eq!(out.result, Err(INVALID_STATE), "{frame}");
        assert!(out.reconnects(), "{frame}");
        assert_eq!(codec.last_private(), None);
    }
    // A frame the codec does not recognise ends the epoch too: channels it never subscribes to.
    for frame in [
        fixture("balance_response.json"),
        fixture("stop_order_response.json"),
        r#"{"subscribed":"balances"}"#.to_owned(),
        r#"{"unsubscribed":"balances"}"#.to_owned(),
        r#"{"subscribed":"fills","x":1}"#.to_owned(),
        AUTHENTICATED.to_owned(),
    ] {
        let mut codec = authed(SUB);
        let out = text(&mut codec, &frame);
        assert_eq!(
            out.result,
            Err(DecodeError::Malformed("unrecognised QFEX trade frame")),
            "{frame}"
        );
        assert!(out.reconnects(), "{frame}");
    }
}

// ----- What the codec does not do -----

/// The commands FBC-blcw's encoder sends are `tests/private_orders.rs`'s; these stay refused.
#[test]
fn no_session_command_is_encoded_and_the_factory_still_offers_no_order_entry() {
    let mut codec = authed(SUB);
    for cmd in [
        VenueCommand::ArmCancelOnDisconnect(true),
        VenueCommand::ArmCancelOnDisconnect(false),
        VenueCommand::RefreshDeadMan,
        VenueCommand::FeeQuery,
    ] {
        let mut fx = Effects::new();
        let sent = codec.encode(
            &cmd,
            RpcId(7),
            &SpecTable::new(),
            &ctx(9),
            &mut PathStamps::off(),
            &mut fx,
        );
        assert_eq!(sent, Err(NotSentReason::Unsupported));
        assert!(fx.is_empty());
    }
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let http = dispatch(&caps, Namespace::new(1), |scope| {
        codec.on_http(
            HttpTag(1),
            Err(HttpFailure::NotSent),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut fx,
        )
    });
    assert_eq!(
        http,
        Err(DecodeError::Malformed(
            "unexpected QFEX private HTTP response"
        ))
    );
    assert!(sink.0.is_empty() && fx.is_empty());
    // A resync cannot be read yet: the epoch ends rather than look resynced.
    codec.resync(&ctx(9), &mut fx);
    assert!(Out::effects(fx.take()).reconnects());
    // An HTTP response is redacted as the account's material says.
    let body = br#"{"user_id":"u-1","x":1}"#;
    let spans = codec.redact_inbound(Inbound::Http(
        HttpTag(1),
        HttpResponse {
            status: 200,
            headers: &[],
            body,
        },
    ));
    assert_eq!(spans.body(), vec![12u32..15]);

    // The factory: market data only, as on main.
    let mut full = cfg(SUB);
    full.insert(auth::TRADE_URL, "wss://trade.qfex.com");
    let caps = Qfex.caps(&full).unwrap();
    assert!(caps.exec.is_none());
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    assert!(Qfex.plan_exec(&full).unwrap().is_empty());
    assert!(Qfex.exec_codec(&full, secrets()).is_none());
}

/// FBC-zeb0 (f): every source file is part of the crate, declared by a `mod` in its parent, so
/// none is compiled only where a test includes it by path.
#[test]
fn every_source_file_is_in_the_module_tree() {
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut all = Vec::new();
    walk(&src, &mut all);
    assert!(all.len() >= 13, "{all:?}");
    for path in all {
        let rel = path.strip_prefix(&src).unwrap();
        let parts: Vec<&str> = rel.iter().map(|part| part.to_str().unwrap()).collect();
        // `a/b.rs` and `a/b/mod.rs` are module `b`, declared in `a/mod.rs`, `a.rs` or lib.rs.
        let (module, dir) = match parts.as_slice() {
            ["lib.rs"] => continue,
            [dir @ .., module, "mod.rs"] => (*module, dir),
            [dir @ .., file] => (file.strip_suffix(".rs").unwrap(), dir),
            [] => unreachable!(),
        };
        let parent = match dir {
            [] => src.join("lib.rs"),
            _ => {
                let dir = src.join(dir.join("/"));
                let nested = dir.join("mod.rs");
                if nested.exists() {
                    nested
                } else {
                    dir.with_extension("rs")
                }
            }
        };
        let text = fs::read_to_string(&parent).unwrap();
        let declared = text.lines().any(|line| {
            let line = line.trim();
            let line = line
                .strip_prefix("pub ")
                .or_else(|| line.strip_prefix("pub(crate) "))
                .unwrap_or(line);
            line == format!("mod {module};")
        });
        assert!(declared, "{rel:?} is not declared in {parent:?}");
    }
}
