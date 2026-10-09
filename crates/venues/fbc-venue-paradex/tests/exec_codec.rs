//! FBC-xvf (decisions 0002, 0009, 0014, 0016, 0028, 0061, 0071): Paradex's order-entry codec,
//! driven through `ExecCodec` only with hand-built responses and frames. One connection carries
//! the order methods and the private channels: a command is encoded on the authenticated
//! connection and its JSON-RPC reply decoded into its outcome; the resync's and the order
//! query's REST reads carry the token only in a redacted header; a read built after the
//! configured refresh interval carries the new token, the refresh timer depending on no token
//! byte; and a command while no connection is authenticated is not sent, with no REST
//! fallback.
//!
//! The key and account are the synthetic ones in `fixtures/paradex/signing` (read from the
//! vectors file's header), the tokens made-up text that never looks like one Paradex issues,
//! and the replies, REST answers and SBE frames the hand-built ones in `fixtures/paradex/exec/`
//! (SYNTHETIC).

mod common;
mod md;

use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use common::Vectors;
use fbc_core::{
    AccountKey, AckLevel, Channel, CidMint, ClientIdFormat, ClientOrderId, ConfigError, ConnState,
    CtxCall, DecodeError, Effect, Effects, EncodeCtx, ExecCodec, ExecEvent, ExecSink, Header,
    HttpFailure, HttpMethod, HttpResponse, HttpTag, Inbound, InboundSpans, ItemRef, Lots, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OrderKind, OrderRef,
    PathStamps, QueryOrder, RawFrame, RejectKind, RpcCall, RpcId, Secret, Secrets, Side, SpecTable,
    StreamId, SubmitOutcome, Ticks, Tif, TimerTag, TrafficClass, VenueCommand, VenueConfig,
    VenueError, VenueMeta, VenueOrderId, WallNs, dispatch, encode_cid,
};
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::{
    CONTROL_IDS, LOGIN_REQUEST, ModifyRequests, PRIVATE_CHANNELS, ParadexEncoder, ParadexExec,
    REFRESH_TIMER, decode_order_event, decode_order_query, decode_resync,
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
/// The REST reads' timeout, as configured (`5000ms`).
const READ_TIMEOUT: Duration = Duration::from_millis(5_000);
/// How long an order request waits for its reply, as the consumer gives it.
const RPC_TIMEOUT: Duration = Duration::from_millis(2_500);
const STREAM: StreamId = StreamId(3);
/// The engine namespace the decode scope is lent under, and the client ids minted in.
const OWN: Namespace = Namespace::new(7);
/// The order id of our order in every fixture.
const OID: &str = "1759500000000000001";

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

fn codec_with(cfg: &VenueConfig) -> ParadexExec {
    let signer = Box::new(Vectors::read().signer());
    ParadexExec::new(cfg, creds(), signer, STREAM, RPC_TIMEOUT).unwrap()
}

fn fresh() -> ParadexExec {
    codec_with(&cfg())
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
    status(200, body)
}

fn status(status: u16, body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status,
        headers: &[],
        body,
    })
}

/// The text of exec fixture `name`.
fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/exec")
        .join(name);
    fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn fixture_text(name: &str) -> String {
    String::from_utf8(fixture(name)).unwrap()
}

/// The bytes of SBE exec fixture `name`: hex bytes, `#` to the end of a line a comment.
fn sbe(name: &str) -> Vec<u8> {
    fixture_text(name)
        .lines()
        .map(|line| line.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

/// Client id `n` of the first two minted in [`OWN`]: the first is the one the fixtures carry
/// (`01000700-199b-81ab-8200-00054d0aa3f5`).
fn cid(n: usize) -> ClientOrderId {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-paradex-codec-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(3), OWN).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_622_400_000_000_000));
        let ids = (0..2).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        ids
    })[n]
}

fn uuid(n: usize) -> String {
    encode_cid(&ClientIdFormat::Uuid, cid(n))
        .unwrap()
        .as_str()
        .to_owned()
}

fn vid(wire: &str) -> VenueOrderId {
    dispatch(&caps(), OWN, |scope| scope.venue_order_id(wire)).unwrap()
}

fn place() -> VenueCommand {
    VenueCommand::Place(NewOrder {
        cid: cid(0),
        inst: BTC,
        side: Side::Buy,
        qty: Lots::new(150).unwrap(),
        kind: OrderKind::Limit { px: Ticks(620_000) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    })
}

fn query_of(target: OrderRef) -> QueryOrder {
    QueryOrder {
        target,
        inst: BTC,
        placement_nonce: None,
    }
}

/// The query for our first order by its client id.
fn query() -> VenueCommand {
    VenueCommand::Query(query_of(OrderRef::Client(cid(0))))
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

impl Call {
    /// Asserts the call refused its input with nothing pushed and nothing asked for.
    fn refused(&self) {
        assert!(self.result.is_err(), "accepted: {:?}", self.events);
        assert!(self.events.is_empty() && self.fx.is_empty());
    }
}

fn open(codec: &mut ParadexExec) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_open(STREAM, &ctx(), &mut fx);
    fx.take()
}

fn answer(
    codec: &mut ParadexExec,
    tag: HttpTag,
    resp: Result<HttpResponse<'_>, HttpFailure>,
) -> Call {
    let specs = md::specs();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch(&caps(), OWN, |scope| {
        codec.on_http(tag, resp, scope, &specs, &mut sink, &mut fx)
    });
    Call {
        result,
        events: sink.0.into_iter().map(|(_, ev)| ev).collect(),
        fx: fx.take(),
    }
}

fn frame(codec: &mut ParadexExec, f: RawFrame<'_>) -> Call {
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

fn text(codec: &mut ParadexExec, text: &str) -> Call {
    frame(codec, RawFrame::Text(text))
}

fn fire(codec: &mut ParadexExec, tag: TimerTag) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_timer(tag, &ctx_at(1_780_000_045), &mut fx);
    fx.take()
}

fn resync(codec: &mut ParadexExec) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.resync(&ctx(), &mut fx);
    fx.take()
}

/// `codec` encoding `cmd` as request `rpc` against the test specs: what it returned and asked
/// for.
fn encode(
    codec: &mut ParadexExec,
    cmd: &VenueCommand,
    rpc: RpcId,
) -> (Result<(), NotSentReason>, Effects) {
    let mut fx = Effects::new();
    let result = codec.encode(
        cmd,
        rpc,
        &md::specs(),
        &ctx(),
        &mut PathStamps::off(),
        &mut fx,
    );
    (
        result.map(|receipt| assert!(receipt.nonces().is_empty())),
        fx,
    )
}

/// Asserts `codec` refuses `cmd` as `reason`, asking for nothing.
fn not_sent(codec: &mut ParadexExec, cmd: &VenueCommand, rpc: RpcId, reason: NotSentReason) {
    let (result, fx) = encode(codec, cmd, rpc);
    assert_eq!(result, Err(reason), "{cmd:?}");
    assert!(fx.is_empty(), "{cmd:?} asked for {fx:?}");
}

/// The JSON frames `fx` writes, with their redaction spans' count.
fn sends(fx: &[Effect]) -> Vec<(Value, Option<RpcCall>)> {
    fx.iter()
        .filter_map(|effect| match effect {
            Effect::Send {
                stream, frame, rpc, ..
            } => {
                assert_eq!(*stream, STREAM);
                let json = serde_json::from_slice(frame.bytes()).expect("a JSON-RPC frame");
                Some((json, *rpc))
            }
            _ => None,
        })
        .collect()
}

/// The login requests `fx` asks for.
fn logins(fx: &[Effect]) -> usize {
    fx.iter()
        .filter(|e| matches!(e, Effect::Http { tag, .. } if *tag == LOGIN_REQUEST))
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

fn reconnects(fx: &[Effect]) -> usize {
    fx.iter()
        .filter(|e| matches!(e, Effect::Reconnect { stream, .. } if *stream == STREAM))
        .count()
}

/// The one auth frame `fx` writes: its JSON-RPC id, after checking it carries `token`.
fn auth_frame(fx: &[Effect], token: &str) -> u64 {
    let [(json, rpc)]: [_; 1] = sends(fx).try_into().expect("one frame");
    assert_eq!((json["method"].as_str(), rpc), (Some("auth"), None));
    assert_eq!(json["params"]["bearer"], token);
    json["id"].as_u64().expect("a numeric id")
}

fn reply(id: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","result":{{"node_id":"a1b2c3d4e5f6g7h8"}},"id":{id}}}"#)
}

fn subscribed(id: u64, channel: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","result":{{"channel":"{channel}"}},"id":{id}}}"#)
}

fn error(id: u64, code: i64, message: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","error":{{"code":{code},"message":"{message}"}},"id":{id}}}"#)
}

/// `codec`, opened, logged in with `token` and authenticated: the auth frame's id and the
/// subscriptions' ids, each answered.
fn authenticate(codec: &mut ParadexExec, token: &str) -> Vec<u64> {
    open(codec);
    let body = login_body(token);
    let auth = auth_frame(&answer(codec, LOGIN_REQUEST, ok(body.as_bytes())).fx, token);
    let acked = text(codec, &reply(auth));
    acked.result.unwrap();
    assert_eq!(
        acked.events,
        [ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Authenticated
        }]
    );
    let mut ids = vec![auth];
    let subs = sends(&acked.fx);
    assert_eq!(subs.len(), PRIVATE_CHANNELS.len());
    for (json, _) in subs {
        let id = json["id"].as_u64().unwrap();
        let channel = json["params"]["channel"].as_str().unwrap();
        let call = text(codec, &subscribed(id, channel));
        assert!(call.result.is_ok() && call.events.is_empty() && call.fx.is_empty());
        ids.push(id);
    }
    ids
}

fn authenticated() -> ParadexExec {
    let mut codec = fresh();
    authenticate(&mut codec, TOKEN);
    codec
}

/// The REST reads `fx` asks for, each asserted to carry `token` in one redacted header and
/// nowhere else: their tags, URLs and rpcs.
fn reads(fx: &[Effect], token: &str) -> Vec<(HttpTag, String, Option<RpcId>)> {
    fx.iter()
        .map(|effect| {
            let Effect::Http {
                tag,
                req,
                rpc,
                timeout,
                ..
            } = effect
            else {
                panic!("not a read: {effect:?}");
            };
            assert_eq!(req.method, HttpMethod::Get);
            assert_eq!(*timeout, READ_TIMEOUT);
            let expected = Header {
                name: "Authorization",
                value: format!("Bearer {token}"),
                redact: true,
            };
            assert_eq!(req.headers, [expected], "the token in a redacted header");
            assert!(!req.url.as_str().contains(token), "no token in the URL");
            assert!(req.body.bytes().is_empty(), "no body");
            (*tag, req.url.as_str().to_owned(), *rpc)
        })
        .collect()
}

#[test]
fn a_command_is_encoded_on_the_authenticated_connection_and_its_reply_decoded_into_its_outcome() {
    let mut codec = authenticated();
    let cmd = place();
    let (result, fx) = encode(&mut codec, &cmd, RpcId(11));
    result.unwrap();
    assert!(fx.carry_request(RpcId(11), cmd.traffic_class()));
    // The encoder's frame, on the connection the codec authenticated, awaiting its reply.
    let mut alone = ParadexEncoder::new(Box::new(Vectors::read().signer()), STREAM, RPC_TIMEOUT);
    let mut expected = Effects::new();
    alone
        .encode(
            &cmd,
            RpcId(11),
            &md::specs(),
            &ctx(),
            &mut PathStamps::off(),
            &mut expected,
        )
        .unwrap();
    assert_eq!(fx.as_slice(), expected.as_slice());
    let [(json, rpc)]: [_; 1] = sends(fx.as_slice()).try_into().unwrap();
    assert_eq!(json["method"], "order.create");
    assert_eq!(
        (json["id"].as_u64(), json["params"]["client_id"].as_str()),
        (Some(11), Some(uuid(0).as_str()))
    );
    assert_eq!(
        rpc,
        Some(RpcCall {
            id: RpcId(11),
            timeout: RPC_TIMEOUT
        })
    );
    // Its reply (id 11): accepted, provisionally, under the venue's order id.
    let call = text(&mut codec, &fixture_text("reply-create.json"));
    call.result.unwrap();
    let item = ItemRef {
        idx: 0,
        cid: Some(cid(0)),
        vid: Some(vid(OID)),
    };
    let provisional = SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    };
    assert_eq!(
        call.events,
        [ExecEvent::Outcome {
            rpc: RpcId(11),
            item: Some(item),
            outcome: provisional
        }]
    );
    assert!(call.fx.is_empty());
    // The same reply again answers nothing: the request was settled.
    text(&mut codec, &fixture_text("reply-create.json")).refused();

    // Cancel-on-disconnect's arm is one of the encoder's frames too (the runtime sends it),
    // and its reply is final.
    let arm = VenueCommand::ArmCancelOnDisconnect(true);
    let (result, fx) = encode(&mut codec, &arm, RpcId(19));
    result.unwrap();
    let [(json, _)]: [_; 1] = sends(fx.as_slice()).try_into().unwrap();
    assert_eq!(json["method"], "order.cancel_on_disconnect");
    let call = text(&mut codec, &fixture_text("reply-cancel-on-disconnect.json"));
    let fin = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    assert_eq!(
        call.events,
        [ExecEvent::Outcome {
            rpc: RpcId(19),
            item: Some(ItemRef {
                idx: 0,
                cid: None,
                vid: None
            }),
            outcome: fin
        }]
    );
}

#[test]
fn a_command_while_no_connection_is_authenticated_is_not_sent_and_falls_back_to_nothing() {
    let disconnected = NotSentReason::Disconnected;
    let mut codec = fresh();
    // No connection.
    not_sent(&mut codec, &place(), RpcId(1), disconnected);
    // Open, the login asked for.
    open(&mut codec);
    not_sent(&mut codec, &place(), RpcId(1), disconnected);
    // The auth frame written, not yet acknowledged.
    let body = login_body(TOKEN);
    let auth = auth_frame(
        &answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes())).fx,
        TOKEN,
    );
    not_sent(&mut codec, &place(), RpcId(1), disconnected);
    not_sent(
        &mut codec,
        &VenueCommand::ArmCancelOnDisconnect(true),
        RpcId(1),
        disconnected,
    );
    // Refused: the connection is closed.
    let call = text(&mut codec, &error(auth, 40111, "Invalid Bearer Token"));
    assert_eq!(reconnects(&call.fx), 1);
    not_sent(&mut codec, &place(), RpcId(1), disconnected);
    // Authenticated, then reconnected: the new connection is not authenticated until its own
    // auth frame is acknowledged.
    let mut codec = authenticated();
    let auth = auth_frame(&open(&mut codec), TOKEN);
    not_sent(&mut codec, &place(), RpcId(2), disconnected);
    text(&mut codec, &reply(auth)).result.unwrap();
    let (result, _) = encode(&mut codec, &place(), RpcId(2));
    result.unwrap();
    // A connection on another stream than the encoder's is not the order connection.
    let mut codec = fresh();
    let mut fx = Effects::new();
    codec.on_open(StreamId(9), &ctx(), &mut fx);
    let body = login_body(TOKEN);
    let call = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
    let Some(Effect::Send { frame, .. }) =
        call.fx.iter().find(|e| matches!(e, Effect::Send { .. }))
    else {
        panic!("{:?}", call.fx)
    };
    let auth: Value = serde_json::from_slice(frame.bytes()).unwrap();
    let specs = md::specs();
    let mut sink = Sink::default();
    dispatch(&caps(), OWN, |scope| {
        codec.on_frame(
            StreamId(9),
            RawFrame::Text(&reply(auth["id"].as_u64().unwrap())),
            scope,
            &specs,
            &mut sink,
            &mut Effects::new(),
        )
    })
    .unwrap();
    not_sent(&mut codec, &place(), RpcId(1), disconnected);
}

#[test]
fn the_codecs_own_frames_take_ids_from_control_ids_so_no_reply_is_mistaken_for_an_orders() {
    let mut codec = fresh();
    let ids = authenticate(&mut codec, TOKEN);
    assert_eq!(ids.len(), 1 + PRIVATE_CHANNELS.len());
    assert!(ids.iter().all(|id| *id >= CONTROL_IDS), "{ids:?}");
    assert_eq!(CONTROL_IDS, 1 << 52);
    // An rpc from there up could not be told from the codec's own: not sent.
    not_sent(
        &mut codec,
        &place(),
        RpcId(CONTROL_IDS),
        NotSentReason::Unencodable,
    );
    // A reply to a request never sent, below or above the line, answers nothing.
    text(&mut codec, &fixture_text("reply-create.json")).refused();
    text(&mut codec, &reply(ids[0])).refused();
    text(&mut codec, &reply(CONTROL_IDS + 100)).refused();
    text(&mut codec, "not json").refused();
    text(
        &mut codec,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{}}"#,
    )
    .refused();
    // An error with no id answers no request: it is reported with its code.
    let call = text(&mut codec, &fixture_text("error-no-id.json"));
    call.result.unwrap();
    let [ExecEvent::UncorrelatedError(reject)] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.venue_code.as_deref(), Some("-32700"));
    // A refused private channel still closes the connection, as in the read-only codec.
    let mut codec = fresh();
    open(&mut codec);
    let body = login_body(TOKEN);
    let auth = auth_frame(
        &answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes())).fx,
        TOKEN,
    );
    let subs = sends(&text(&mut codec, &reply(auth)).fx);
    let sub = subs[0].0["id"].as_u64().unwrap();
    let call = text(&mut codec, &error(sub, -32602, "Invalid parameters"));
    assert_eq!(reconnects(&call.fx), 1);
    assert_eq!(
        call.events.last(),
        Some(&ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Closed
        })
    );
    let [ExecEvent::UncorrelatedError(reject), _] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(reject.kind, RejectKind::Other);
}

#[test]
fn the_resync_reads_orders_and_positions_with_the_token_only_in_a_redacted_header_and_decodes_them_whole()
 {
    let mut codec = authenticated();
    assert_eq!(codec.nonces_for(CtxCall::Resync), 0);
    let fx = resync(&mut codec);
    let read = reads(&fx, TOKEN);
    let urls: Vec<_> = read.iter().map(|(_, url, _)| url.as_str()).collect();
    assert_eq!(
        urls,
        [format!("{REST}/orders"), format!("{REST}/positions")]
    );
    assert!(read.iter().all(|(_, _, rpc)| rpc.is_none()));
    for effect in &fx {
        let Effect::Http { class, .. } = effect else {
            unreachable!()
        };
        assert_eq!(*class, TrafficClass::Safety);
    }
    let (orders_tag, positions_tag) = (read[0].0, read[1].0);
    assert!(orders_tag != LOGIN_REQUEST && positions_tag != LOGIN_REQUEST);
    // The positions answer first: held, nothing pushed until both are in.
    let (orders, positions) = (
        fixture("rest-orders-open.json"),
        fixture("rest-positions.json"),
    );
    let first = answer(&mut codec, positions_tag, ok(&positions));
    assert!(first.result.is_ok() && first.events.is_empty() && first.fx.is_empty());
    let second = answer(&mut codec, orders_tag, ok(&orders));
    second.result.unwrap();
    assert!(second.fx.is_empty());
    // The two decoded whole, at the watermark of the context the reads were built with.
    let specs = md::specs();
    let direct = dispatch(&caps(), OWN, |scope| {
        decode_resync(ctx().wall, &orders, &positions, scope, &specs)
    })
    .unwrap();
    let direct: Vec<_> = direct.events().iter().map(|(_, ev)| ev.clone()).collect();
    assert_eq!(second.events, direct);
    assert_eq!(
        second.events.first(),
        Some(&ExecEvent::ResyncBegin {
            watermark: ctx().wall
        })
    );
    assert_eq!(second.events.last(), Some(&ExecEvent::ResyncEnd));
    assert!(second.events.len() > 2);
    // Done: a repeated answer answers nothing.
    answer(&mut codec, orders_tag, ok(&orders)).refused();
}

#[test]
fn the_order_query_is_the_orders_history_read_with_the_token_only_in_a_redacted_header_under_its_rpc()
 {
    let mut codec = authenticated();
    let cmd = query();
    let (result, fx) = encode(&mut codec, &cmd, RpcId(12));
    result.unwrap();
    assert!(fx.carry_request(RpcId(12), cmd.traffic_class()));
    let [(tag, url, rpc)]: [_; 1] = reads(fx.as_slice(), TOKEN).try_into().unwrap();
    assert_eq!(url, format!("{REST}/orders-history?client_id={}", uuid(0)));
    assert_eq!(rpc, Some(RpcId(12)));
    let body = fixture("rest-orders-history-filled.json");
    let call = answer(&mut codec, tag, ok(&body));
    call.result.unwrap();
    let specs = md::specs();
    let VenueCommand::Query(q) = &cmd else {
        unreachable!()
    };
    let direct = dispatch(&caps(), OWN, |scope| {
        decode_order_query(RpcId(12), q, &body, scope, &specs)
    })
    .unwrap();
    let direct: Vec<_> = direct.events().iter().map(|(_, ev)| ev.clone()).collect();
    assert_eq!(call.events, direct);
    let [ExecEvent::QueryResult(found)] = call.events.as_slice() else {
        panic!("{:?}", call.events)
    };
    assert_eq!(found.rpc(), RpcId(12));
    // Answered: the same answer again is no query's.
    answer(&mut codec, tag, ok(&body)).refused();
}

#[test]
fn a_rest_read_built_after_the_configured_refresh_interval_carries_the_new_token() {
    let mut codec = authenticated();
    let (_, fx) = encode(&mut codec, &query(), RpcId(12));
    assert_eq!(reads(fx.as_slice(), TOKEN).len(), 1);
    // The refresh timer, set for the configured interval, fires: a login.
    let fx = fire(&mut codec, REFRESH_TIMER);
    assert_eq!((logins(&fx), fx.len()), (1, 1));
    let body = login_body(OTHER_TOKEN);
    let call = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
    call.result.unwrap();
    assert!(call.events.is_empty() && sends(&call.fx).is_empty());
    assert_eq!((timers(&call.fx), call.fx.len()), (1, 1));
    // Every read built from now on carries the new token, and only in its redacted header.
    let fx = resync(&mut codec);
    assert_eq!(reads(&fx, OTHER_TOKEN).len(), 2);
    let (_, fx) = encode(&mut codec, &query(), RpcId(13));
    assert_eq!(reads(fx.as_slice(), OTHER_TOKEN).len(), 1);
    // The open connection keeps taking commands: no new auth frame was needed.
    let (result, _) = encode(&mut codec, &place(), RpcId(14));
    result.unwrap();
}

#[test]
fn the_refresh_timer_depends_on_no_token_byte() {
    for token in [
        TOKEN,
        OTHER_TOKEN,
        "2",
        "2222222222222222222222222222222222222222",
    ] {
        let mut codec = fresh();
        open(&mut codec);
        let body = login_body(token);
        let call = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
        assert_eq!(timers(&call.fx), 1, "{token}");
        // A refresh's answer sets the same timer, whatever token it gives.
        fire(&mut codec, REFRESH_TIMER);
        let call = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
        assert_eq!((timers(&call.fx), call.fx.len()), (1, 1), "{token}");
    }
}

#[test]
fn the_reads_take_the_configured_base_and_timeout_in_seconds_too() {
    let mut cfg = cfg();
    cfg.insert(REST_URL, &format!("{REST}/"));
    cfg.insert(TIMEOUT, "7s");
    let mut codec = codec_with(&cfg);
    authenticate(&mut codec, TOKEN);
    let fx = resync(&mut codec);
    let Some(Effect::Http { req, timeout, .. }) = fx.first() else {
        panic!("{fx:?}")
    };
    assert_eq!(req.url.as_str(), format!("{REST}/orders"));
    assert_eq!(*timeout, Duration::from_secs(7));
}

#[test]
fn a_resync_read_that_fails_is_refused_or_does_not_decode_pushes_nothing_and_asks_for_the_connection_again()
 {
    let failures: [Result<HttpResponse<'_>, HttpFailure>; 4] = [
        Err(HttpFailure::NotSent),
        Err(HttpFailure::TimedOut),
        Err(HttpFailure::Lost),
        status(401, br#"{"error":"NOT_AUTHENTICATED"}"#),
    ];
    for failure in failures {
        let mut codec = authenticated();
        let fx = resync(&mut codec);
        let read = reads(&fx, TOKEN);
        let call = answer(&mut codec, read[0].0, failure);
        call.result.unwrap();
        assert!(call.events.is_empty(), "{:?}", call.events);
        assert_eq!((reconnects(&call.fx), call.fx.len()), (1, 1));
        // The other read's answer belongs to no resync now.
        answer(&mut codec, read[1].0, ok(&fixture("rest-positions.json"))).refused();
        // The next connection logs in afresh.
        assert_eq!(logins(&open(&mut codec)), 1);
    }
    // Both in, one undecodable: refused, nothing pushed, the connection asked for again.
    let mut codec = authenticated();
    let read = reads(&resync(&mut codec), TOKEN);
    answer(&mut codec, read[0].0, ok(&fixture("rest-orders-open.json")))
        .result
        .unwrap();
    let call = answer(&mut codec, read[1].0, ok(b"{}"));
    assert_eq!(
        call.result,
        Err(DecodeError::Malformed("positions: results"))
    );
    assert!(call.events.is_empty());
    assert_eq!((reconnects(&call.fx), call.fx.len()), (1, 1));
    // A second resync replaces the first: an answer to the first's reads is no resync's.
    let mut codec = authenticated();
    let first = reads(&resync(&mut codec), TOKEN);
    let second = reads(&resync(&mut codec), TOKEN);
    assert!(
        first
            .iter()
            .all(|(tag, ..)| second.iter().all(|(t, ..)| t != tag))
    );
    answer(
        &mut codec,
        first[0].0,
        ok(&fixture("rest-orders-open.json")),
    )
    .refused();
}

#[test]
fn a_new_connection_drops_the_requests_and_reads_of_the_one_before() {
    // Replies and answers come back only to the epoch that asked (0027): a new connection holds
    // nothing an earlier one sent, so what the session refused after its encode (a frame or a
    // read for its rate budget) is held no longer than its connection (Codex
    // 4211490398, 4211642782 and 4211822659 on PR #109; the runtime telling the codec is
    // FBC-9r5o).
    let mut codec = authenticated();
    let (result, _) = encode(&mut codec, &place(), RpcId(11));
    result.unwrap();
    let resync_reads = reads(&resync(&mut codec), TOKEN);
    let (_, fx) = encode(&mut codec, &query(), RpcId(12));
    let [(query_tag, ..)]: [_; 1] = reads(fx.as_slice(), TOKEN).try_into().unwrap();
    open(&mut codec);
    answer(
        &mut codec,
        query_tag,
        ok(&fixture("rest-orders-history-filled.json")),
    )
    .refused();
    for (tag, ..) in resync_reads {
        answer(&mut codec, tag, ok(&fixture("rest-positions.json"))).refused();
    }
    // The request's reply is no longer awaited; a deadline the runtime still names is Unknown.
    text(&mut codec, &fixture_text("reply-create.json")).refused();
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(11), &mut sink);
    let unknown = ExecEvent::Outcome {
        rpc: RpcId(11),
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(sink.0, [(VenueMeta::NONE, unknown)]);
}

#[test]
fn a_request_whose_reply_was_unknown_adds_nothing_at_its_timeout_after_a_new_connection() {
    // A reply with every item Unknown answers nothing, so the runtime still times the request
    // out, on whichever connection is open then; that timeout adds nothing, and a reconnect in
    // between does not make it report a second Unknown (Reviewer B RB-xvf-1 on PR #109).
    for reconnect in [false, true] {
        let mut codec = authenticated();
        encode(&mut codec, &place(), RpcId(11)).0.unwrap();
        let replied = text(&mut codec, &error(11, -32603, "internal error"));
        replied.result.unwrap();
        let unknown = ExecEvent::Outcome {
            rpc: RpcId(11),
            item: None,
            outcome: SubmitOutcome::Unknown,
        };
        assert_eq!(replied.events, [unknown]);
        if reconnect {
            open(&mut codec);
        }
        let mut sink = Sink::default();
        codec.on_rpc_timeout(RpcId(11), &mut sink);
        assert!(sink.0.is_empty(), "reconnect {reconnect}: {:?}", sink.0);
    }
}

#[test]
fn a_resync_with_no_token_reads_nothing_and_asks_for_the_connection_again() {
    let mut codec = fresh();
    // No connection at all: nothing.
    assert!(resync(&mut codec).is_empty());
    // A connection waiting on its login.
    open(&mut codec);
    let fx = resync(&mut codec);
    assert_eq!((reconnects(&fx), fx.len()), (1, 1));
}

#[test]
fn a_query_answered_with_no_result_is_not_sent_or_unknown_for_its_rpc() {
    let cases: [(Result<HttpResponse<'_>, HttpFailure>, SubmitOutcome); 6] = [
        (
            Err(HttpFailure::NotSent),
            SubmitOutcome::NotSent(NotSentReason::Disconnected),
        ),
        (Err(HttpFailure::TimedOut), SubmitOutcome::Unknown),
        (Err(HttpFailure::Lost), SubmitOutcome::Unknown),
        (status(503, b"unavailable"), SubmitOutcome::Unknown),
        (ok(b"not json"), SubmitOutcome::Unknown),
        (ok(br#"{"results":[{}]}"#), SubmitOutcome::Unknown),
    ];
    for (resp, outcome) in cases {
        let mut codec = authenticated();
        let (_, fx) = encode(&mut codec, &query(), RpcId(12));
        let [(tag, ..)]: [_; 1] = reads(fx.as_slice(), TOKEN).try_into().unwrap();
        let call = answer(&mut codec, tag, resp);
        call.result.unwrap();
        assert!(call.fx.is_empty());
        let expected = ExecEvent::Outcome {
            rpc: RpcId(12),
            item: None,
            outcome,
        };
        assert_eq!(call.events, [expected]);
    }
}

#[test]
fn a_query_the_codec_cannot_send_is_refused_with_no_effect() {
    // No token yet: no read can be made.
    let mut codec = fresh();
    open(&mut codec);
    not_sent(&mut codec, &query(), RpcId(12), NotSentReason::Disconnected);
    let mut codec = authenticated();
    // An instrument the spec table does not hold.
    let unknown = QueryOrder {
        inst: fbc_core::InstrumentId::new(99),
        ..query_of(OrderRef::Client(cid(0)))
    };
    let cmd = VenueCommand::Query(unknown);
    not_sent(&mut codec, &cmd, RpcId(12), NotSentReason::Unencodable);
    // Paradex is queried by client id only.
    let by_venue = VenueCommand::Query(query_of(OrderRef::Venue(vid(OID))));
    not_sent(&mut codec, &by_venue, RpcId(12), NotSentReason::Unsupported);
    // Commands the encoder refuses go nowhere either, and are not waited on.
    for cmd in [VenueCommand::RefreshDeadMan, VenueCommand::FeeQuery] {
        not_sent(&mut codec, &cmd, RpcId(12), NotSentReason::Unsupported);
    }
}

#[test]
fn a_timeout_is_unknown_for_an_unanswered_request_and_a_late_answer_settles_nothing() {
    let mut codec = authenticated();
    let (result, _) = encode(&mut codec, &place(), RpcId(11));
    result.unwrap();
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(11), &mut sink);
    let unknown = ExecEvent::Outcome {
        rpc: RpcId(11),
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(sink.0, [(VenueMeta::NONE, unknown)]);
    text(&mut codec, &fixture_text("reply-create.json")).refused();
    // A query's deadline, if the runtime names one, drops it too.
    let (_, fx) = encode(&mut codec, &query(), RpcId(12));
    let [(tag, ..)]: [_; 1] = reads(fx.as_slice(), TOKEN).try_into().unwrap();
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(12), &mut sink);
    assert_eq!(sink.0.len(), 1);
    answer(
        &mut codec,
        tag,
        ok(&fixture("rest-orders-history-filled.json")),
    )
    .refused();
}

#[test]
fn private_events_decode_on_the_same_connection() {
    let mut codec = authenticated();
    let bytes = sbe("order-new-v1.sbe.txt");
    let specs: SpecTable = md::specs();
    let mut direct = Sink::default();
    dispatch(&caps(), OWN, |scope| {
        decode_order_event(
            &bytes,
            scope,
            &specs,
            &mut ModifyRequests::new(),
            &mut direct,
        )
    })
    .unwrap();
    let call = frame(&mut codec, RawFrame::Binary(&bytes));
    call.result.unwrap();
    let direct: Vec<_> = direct.0.into_iter().map(|(_, ev)| ev).collect();
    assert!(!direct.is_empty());
    assert_eq!(call.events, direct);
}

#[test]
fn redact_inbound_names_the_login_tokens_span_and_nothing_in_a_resync_answer_or_a_frame() {
    let mut codec = authenticated();
    let body = login_body(TOKEN).into_bytes();
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: &body,
    };
    let at = body
        .windows(TOKEN.len())
        .position(|w| w == TOKEN.as_bytes())
        .unwrap() as u32;
    let token_span = at..at + TOKEN.len() as u32;
    assert_eq!(
        codec.redact_inbound(Inbound::Http(LOGIN_REQUEST, resp)),
        InboundSpans::response(vec![], vec![token_span])
    );
    // A resync answer holding an escape (which a login answer is blanked whole for) is kept
    // as it is: it carries no credential, and replay must decode it the same.
    let read = reads(&resync(&mut codec), TOKEN);
    let escaped = br#"{"results":[],"note":"a \"quoted\" word"}"#;
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: escaped,
    };
    assert_eq!(
        codec.redact_inbound(Inbound::Http(read[0].0, resp)),
        InboundSpans::NONE
    );
    let reply = fixture_text("reply-create.json");
    assert_eq!(
        codec.redact_inbound(Inbound::Frame(RawFrame::Text(&reply))),
        InboundSpans::NONE
    );
}

#[test]
fn no_call_asks_for_a_nonce_and_the_codec_shows_no_credential() {
    let mut codec = authenticated();
    for call in [
        CtxCall::Open(STREAM),
        CtxCall::Timer(REFRESH_TIMER),
        CtxCall::Resync,
    ] {
        assert_eq!(codec.nonces_for(call), 0);
    }
    resync(&mut codec);
    encode(&mut codec, &query(), RpcId(12)).0.unwrap();
    let shown = format!("{codec:?}");
    let vectors = Vectors::read();
    for secret in [TOKEN, &vectors.header["account"], &vectors.header["key"]] {
        assert!(!shown.contains(secret), "{shown}");
    }
    // Refused naming a missing key, never a value.
    let mut partial = cfg();
    let mut values = VenueConfig::new();
    for key in [REST_URL, CHAIN_ID, SIGNATURE_LIFETIME, TIMEOUT] {
        values.insert(key, partial.get(key).unwrap());
    }
    partial = values;
    let signer = Box::new(Vectors::read().signer());
    let err = ParadexExec::new(&partial, creds(), signer, STREAM, RPC_TIMEOUT).unwrap_err();
    assert_eq!(err, VenueError::Config(ConfigError::Missing(REFRESH)));
}

/// `reply` with `"<name>": null` beside its members, as Paradex may write the member it does
/// not fill.
fn with_null(reply: &str, name: &str) -> String {
    let at = reply.rfind("\"id\":").expect("a reply with an id");
    format!("{}\"{name}\":null,{}", &reply[..at], &reply[at..])
}

#[test]
fn an_auth_and_a_subscribe_reply_read_a_json_null_error_or_result_as_absent() {
    // A result beside "error": null is the result: the auth reply authenticates, each
    // subscribe reply is consumed, and a command is then encoded.
    let mut codec = fresh();
    open(&mut codec);
    let body = login_body(TOKEN);
    let auth = auth_frame(
        &answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes())).fx,
        TOKEN,
    );
    let call = text(&mut codec, &with_null(&reply(auth), "error"));
    call.result.unwrap();
    assert_eq!(
        call.events,
        [ExecEvent::Conn {
            stream: STREAM,
            state: ConnState::Authenticated
        }]
    );
    let subs = sends(&call.fx);
    assert_eq!(subs.len(), PRIVATE_CHANNELS.len());
    for (json, _) in subs {
        let id = json["id"].as_u64().unwrap();
        let channel = json["params"]["channel"].as_str().unwrap();
        let call = text(&mut codec, &with_null(&subscribed(id, channel), "error"));
        call.result.unwrap();
        assert!(call.events.is_empty() && call.fx.is_empty());
    }
    let (result, _) = encode(&mut codec, &place(), RpcId(1));
    result.unwrap();
    // An error beside "result": null is that error: the auth reply is refused with its code.
    let mut codec = fresh();
    open(&mut codec);
    let auth = auth_frame(
        &answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes())).fx,
        TOKEN,
    );
    let refused = with_null(&error(auth, 40111, "Invalid Bearer Token"), "result");
    let call = text(&mut codec, &refused);
    call.result.unwrap();
    assert_eq!(reconnects(&call.fx), 1);
    let [
        ExecEvent::UncorrelatedError(reject),
        ExecEvent::Conn { state, .. },
    ] = call.events.as_slice()
    else {
        panic!("{:?}", call.events)
    };
    assert_eq!(
        (reject.venue_code.as_deref(), *state),
        (Some("40111"), ConnState::Closed)
    );
    not_sent(&mut codec, &place(), RpcId(1), NotSentReason::Disconnected);
    // And a subscribe reply's.
    let mut codec = fresh();
    open(&mut codec);
    let auth = auth_frame(
        &answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes())).fx,
        TOKEN,
    );
    let subs = sends(&text(&mut codec, &reply(auth)).fx);
    let sub = subs[0].0["id"].as_u64().unwrap();
    let refused = with_null(&error(sub, -32602, "Invalid parameters"), "result");
    let call = text(&mut codec, &refused);
    call.result.unwrap();
    assert_eq!(reconnects(&call.fx), 1);
    let [
        ExecEvent::UncorrelatedError(reject),
        ExecEvent::Conn { state, .. },
    ] = call.events.as_slice()
    else {
        panic!("{:?}", call.events)
    };
    assert_eq!(
        (reject.venue_code.as_deref(), *state),
        (Some("-32602"), ConnState::Closed)
    );
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

/// Asserts `call` closed the stream and asked for a reconnect, writing nothing, after the
/// refusal `refusal` names (`unreadable`), or nothing else.
fn closed_as(call: &Call, refusal: Option<Option<&str>>, bad: &str) {
    call.result.as_ref().unwrap();
    assert_eq!(reconnects(&call.fx), 1, "{bad}: {:?}", call.fx);
    assert!(sends(&call.fx).is_empty(), "{bad}");
    let closed = ExecEvent::Conn {
        stream: STREAM,
        state: ConnState::Closed,
    };
    match (refusal, call.events.as_slice()) {
        (None, [last]) => assert_eq!(last, &closed, "{bad}"),
        (Some(code), [ExecEvent::UncorrelatedError(reject), last]) => {
            assert_eq!(last, &closed, "{bad}");
            assert_eq!(reject.kind, RejectKind::Other, "{bad}");
            assert_eq!(reject.venue_code.as_deref(), code, "{bad}");
        }
        _ => panic!("{bad}: {:?}", call.events),
    }
}

#[test]
fn an_unreadable_auth_or_subscribe_reply_reports_closed_and_asks_for_a_reconnect() {
    let body = login_body(TOKEN);
    // The auth reply: the connection is closed and asked for again, no command is sent on it,
    // and its token is not reused, so the next connection logs in.
    for case in 0..unreadable(0).len() {
        let mut codec = fresh();
        open(&mut codec);
        let login = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
        let auth = auth_frame(&login.fx, TOKEN);
        let (bad, refusal) = unreadable(auth).swap_remove(case);
        let call = text(&mut codec, &bad);
        closed_as(&call, refusal, &bad);
        not_sent(&mut codec, &place(), RpcId(1), NotSentReason::Disconnected);
        text(&mut codec, &reply(auth)).refused();
        let fx = open(&mut codec);
        assert_eq!(logins(&fx), 1, "{bad}");
        assert!(sends(&fx).is_empty(), "{bad}");
    }
    // A subscribe reply: the same, on an authenticated connection.
    for case in 0..unreadable(0).len() {
        let mut codec = fresh();
        open(&mut codec);
        let login = answer(&mut codec, LOGIN_REQUEST, ok(body.as_bytes()));
        let auth = auth_frame(&login.fx, TOKEN);
        let subs = sends(&text(&mut codec, &reply(auth)).fx);
        let sub = subs[2].0["id"].as_u64().unwrap();
        let (bad, refusal) = unreadable(sub).swap_remove(case);
        let call = text(&mut codec, &bad);
        closed_as(&call, refusal, &bad);
        not_sent(&mut codec, &place(), RpcId(1), NotSentReason::Disconnected);
        let first = subs[0].0["id"].as_u64().unwrap();
        text(&mut codec, &subscribed(first, PRIVATE_CHANNELS[0])).refused();
        assert_eq!(logins(&open(&mut codec)), 1, "{bad}");
    }
    // A readable auth reply still authenticates, and a command is then encoded.
    let mut codec = fresh();
    authenticate(&mut codec, TOKEN);
    let (result, _) = encode(&mut codec, &place(), RpcId(1));
    result.unwrap();
}

// FBC-6oj (decision 0085's Consequences): a placement Paradex accepts provisionally gets its
// final phase from the order's event: `Accepted` at `AckLevel::Final` for its request once an
// event shows the order past the risk check (OPEN, or anything filled), or an asynchronous
// reject of the placement once one shows it closed with nothing filled for a reason other than
// our cancel.

/// Our first order (cid 0, [`OID`]) as its `OrderEvent` shows it: `order-new-v1.sbe.txt`, NEW,
/// with its status byte set to `status` (1 NEW, 3 OPEN, 4 CLOSED).
fn new_event_as(status: u8) -> Vec<u8> {
    let mut bytes = sbe("order-new-v1.sbe.txt");
    // The 8-byte message header, then `status` at block offset 16.
    assert_eq!(bytes[8 + 16], 1, "order-new-v1 is NEW");
    bytes[8 + 16] = status;
    bytes
}

/// What the order event `bytes` decodes into on its own, with nothing awaiting its final phase.
fn decoded(bytes: &[u8]) -> Vec<ExecEvent> {
    let call = frame(&mut authenticated(), RawFrame::Binary(bytes));
    call.result.unwrap();
    call.events
}

fn item0() -> ItemRef {
    ItemRef {
        idx: 0,
        cid: Some(cid(0)),
        vid: Some(vid(OID)),
    }
}

fn accepted(rpc: u64, ack: AckLevel) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item: Some(item0()),
        outcome: SubmitOutcome::Accepted { ack },
    }
}

/// The asynchronous reject of our first order's placement for `reason`, as `kind`.
fn place_refused(kind: RejectKind, reason: &str) -> ExecEvent {
    ExecEvent::AsyncReject {
        target: OrderRef::Both(cid(0), vid(OID)),
        op: fbc_core::OpKind::Place,
        reject: fbc_core::Reject {
            kind,
            venue_code: None,
            raw: reason.into(),
        },
    }
}

/// A codec that placed our first order as request 11 and read its reply, provisional.
fn placed() -> ParadexExec {
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    let call = text(&mut codec, &fixture_text("reply-create.json"));
    call.result.unwrap();
    assert_eq!(call.events, [accepted(11, AckLevel::Provisional)]);
    codec
}

#[test]
fn a_placement_is_final_once_its_order_event_shows_it_open_and_never_again() {
    let mut codec = placed();
    // NEW: received, its risk check not yet passed. The order update alone.
    let new = sbe("order-new-v1.sbe.txt");
    let call = frame(&mut codec, RawFrame::Binary(&new));
    call.result.unwrap();
    assert_eq!(call.events, decoded(&new));
    // OPEN: past the risk check. The final acceptance of request 11's item, then the update.
    let open = new_event_as(3);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    call.result.unwrap();
    let mut expected = vec![accepted(11, AckLevel::Final)];
    expected.extend(decoded(&open));
    assert_eq!(call.events, expected);
    // A later event of the order is the update alone: the placement is settled.
    for later in [open, sbe("order-closed-post-only-v2.sbe.txt")] {
        let call = frame(&mut codec, RawFrame::Binary(&later));
        call.result.unwrap();
        assert_eq!(call.events, decoded(&later));
    }
}

#[test]
fn a_placement_whose_order_event_shows_anything_filled_is_final() {
    for fixture in [
        "order-closed-filled-v2.sbe.txt",
        "order-market-ioc-v1.sbe.txt",
        "order-closed-canceled-v2.sbe.txt",
        "order-modify-success-v2.sbe.txt",
    ] {
        let mut codec = placed();
        let bytes = sbe(fixture);
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        call.result.unwrap();
        let mut expected = vec![accepted(11, AckLevel::Final)];
        expected.extend(decoded(&bytes));
        assert_eq!(call.events, expected, "{fixture}");
    }
}

#[test]
fn a_placement_whose_order_event_shows_it_closed_with_nothing_filled_is_rejected_asynchronously() {
    let cases = [
        (
            "order-closed-post-only-v2.sbe.txt",
            RejectKind::PostOnlyWouldCross,
            "POST_ONLY_WOULD_CROSS",
        ),
        (
            "order-closed-margin-v2.sbe.txt",
            RejectKind::Other,
            "NOT_ENOUGH_MARGIN",
        ),
    ];
    for (fixture, kind, reason) in cases {
        let mut codec = placed();
        let bytes = sbe(fixture);
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        call.result.unwrap();
        let mut expected = vec![place_refused(kind, reason)];
        expected.extend(decoded(&bytes));
        assert_eq!(call.events, expected, "{fixture}");
    }
}

/// Order event `bytes` with its cancel reason `from` (non-empty) stated as `to` instead.
fn with_reason(mut bytes: Vec<u8>, from: &str, to: &str) -> Vec<u8> {
    let from = from.as_bytes();
    let at = bytes.windows(from.len()).position(|w| w == from).unwrap();
    let len = u8::try_from(to.len()).unwrap();
    bytes.splice(
        at - 1..at + from.len(),
        [&[len][..], to.as_bytes()].concat(),
    );
    bytes
}

#[test]
fn an_ioc_or_market_order_closed_unfilled_for_no_known_refusal_is_final_not_rejected() {
    // A MARKET IOC buy that found nothing to fill: `sizeOpen` (block offset 44) equal to its
    // `size` (36), CLOSED with no cancel reason. It passed the risk check and expired.
    let mut market = sbe("order-market-ioc-v1.sbe.txt");
    let size = market[8 + 36..8 + 44].to_vec();
    market[8 + 44..8 + 52].copy_from_slice(&size);
    // A LIMIT IOC that crossed nothing: the margin fixture's POST_ONLY (19) made IOC, and its
    // reason one no source names as a refusal.
    let mut limit = sbe("order-closed-margin-v2.sbe.txt");
    assert_eq!(limit[8 + 19], 3, "order-closed-margin-v2 is POST_ONLY");
    limit[8 + 19] = 2;
    let limit = with_reason(limit, "NOT_ENOUGH_MARGIN", "SOME_UNLISTED_REASON");
    for (name, bytes) in [("market IOC", market), ("limit IOC", limit)] {
        let mut codec = placed();
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        call.result.unwrap();
        let mut expected = vec![accepted(11, AckLevel::Final)];
        expected.extend(decoded(&bytes));
        assert_eq!(call.events, expected, "{name}");
    }
}

#[test]
fn an_ioc_order_closed_unfilled_for_want_of_margin_is_still_rejected_asynchronously() {
    let mut ioc = sbe("order-closed-margin-v2.sbe.txt");
    ioc[8 + 19] = 2;
    let mut codec = placed();
    let call = frame(&mut codec, RawFrame::Binary(&ioc));
    call.result.unwrap();
    let mut expected = vec![place_refused(RejectKind::Other, "NOT_ENOUGH_MARGIN")];
    expected.extend(decoded(&ioc));
    assert_eq!(call.events, expected);
}

#[test]
fn a_placement_we_cancel_before_its_risk_check_is_neither_final_nor_rejected() {
    // CLOSED by USER_CANCELED with nothing filled: our own cancel, not the venue's refusal.
    let mut canceled = sbe("order-closed-post-only-v2.sbe.txt");
    let (from, to) = (
        b"POST_ONLY_WOULD_CROSS".as_slice(),
        b"USER_CANCELED".as_slice(),
    );
    let at = canceled
        .windows(from.len())
        .position(|w| w == from)
        .unwrap();
    canceled.splice(at - 1..at + from.len(), [&[13u8][..], to].concat());
    let mut codec = placed();
    let call = frame(&mut codec, RawFrame::Binary(&canceled));
    call.result.unwrap();
    assert_eq!(call.events, decoded(&canceled));
    // And it is settled: a later OPEN event of the order (none is sent, but were it) adds nothing.
    let open = new_event_as(3);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    assert_eq!(call.events, decoded(&open));
}

#[test]
fn an_order_event_before_the_placements_reply_settles_it_when_the_reply_comes() {
    // OPEN first: the reply is the final acceptance at once.
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    let open = new_event_as(3);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    assert_eq!(call.events, decoded(&open));
    let call = text(&mut codec, &fixture_text("reply-create.json"));
    call.result.unwrap();
    assert_eq!(call.events, [accepted(11, AckLevel::Final)]);
    // NEW then a refusal first: the reply's provisional acceptance, then the reject.
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    for fixture in ["order-new-v1.sbe.txt", "order-closed-post-only-v2.sbe.txt"] {
        let bytes = sbe(fixture);
        let call = frame(&mut codec, RawFrame::Binary(&bytes));
        assert_eq!(call.events, decoded(&bytes), "{fixture}");
    }
    let call = text(&mut codec, &fixture_text("reply-create.json"));
    call.result.unwrap();
    let refused = place_refused(RejectKind::PostOnlyWouldCross, "POST_ONLY_WOULD_CROSS");
    assert_eq!(call.events, [accepted(11, AckLevel::Provisional), refused]);
}

#[test]
fn a_batch_items_placement_is_final_on_its_order_event_and_a_refused_item_awaits_nothing() {
    let mut codec = authenticated();
    let first = match place() {
        VenueCommand::Place(o) => o,
        _ => unreachable!(),
    };
    let second = NewOrder {
        cid: cid(1),
        ..first.clone()
    };
    let batch = VenueCommand::PlaceBatch(vec![first, second]);
    encode(&mut codec, &batch, RpcId(13)).0.unwrap();
    let call = text(&mut codec, &fixture_text("reply-create-batch-mixed.json"));
    call.result.unwrap();
    assert_eq!(call.events.len(), 2, "{:?}", call.events);
    let open = new_event_as(3);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    let mut expected = vec![accepted(13, AckLevel::Final)];
    expected.extend(decoded(&open));
    assert_eq!(call.events, expected);
}

#[test]
fn a_new_connection_a_timeout_or_an_unaccepted_reply_leaves_no_placement_awaiting_its_final_phase()
{
    let opened = new_event_as(3);
    // Accepted provisionally, then a new connection: the resync settles what rests now.
    let mut codec = placed();
    open(&mut codec);
    let call = frame(&mut codec, RawFrame::Binary(&opened));
    assert_eq!(call.events, decoded(&opened));
    // Sent, then timed out (Unknown): no reply, so no final phase either.
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    codec.on_rpc_timeout(RpcId(11), &mut Sink::default());
    let call = frame(&mut codec, RawFrame::Binary(&opened));
    assert_eq!(call.events, decoded(&opened));
    // Refused by its reply: ended, never accepted.
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    text(&mut codec, &error(11, -32602, "invalid params"))
        .result
        .unwrap();
    let call = frame(&mut codec, RawFrame::Binary(&opened));
    assert_eq!(call.events, decoded(&opened));
}

/// `bytes` with our first order's venue id ([`OID`]) replaced by `other`, as long.
fn with_vid(bytes: &[u8], other: &str) -> Vec<u8> {
    let (from, to) = (OID.as_bytes(), other.as_bytes());
    assert_eq!(from.len(), to.len());
    let at = bytes.windows(from.len()).position(|w| w == from).unwrap();
    let mut out = bytes.to_vec();
    out[at..at + from.len()].copy_from_slice(to);
    out
}

/// What `codec` pushed for `f`, with each event's `VenueMeta`.
fn frame_metas(codec: &mut ParadexExec, f: RawFrame<'_>) -> Vec<(VenueMeta, ExecEvent)> {
    let specs = md::specs();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    dispatch(&caps(), OWN, |scope| {
        codec.on_frame(STREAM, f, scope, &specs, &mut sink, &mut fx)
    })
    .unwrap();
    sink.0
}

#[test]
fn a_phase_an_event_before_the_reply_showed_carries_that_events_venue_meta() {
    // Codex r4228157167 on PR #145: the reply states no venue time or sequence, the event does.
    let cases = [
        (new_event_as(3), accepted(11, AckLevel::Final), 0),
        (
            sbe("order-closed-post-only-v2.sbe.txt"),
            place_refused(RejectKind::PostOnlyWouldCross, "POST_ONLY_WOULD_CROSS"),
            1,
        ),
    ];
    for (event, phase, at) in cases {
        let mut codec = authenticated();
        encode(&mut codec, &place(), RpcId(11)).0.unwrap();
        let shown = frame_metas(&mut codec, RawFrame::Binary(&event));
        let meta = shown.last().unwrap().0;
        assert!(meta.venue_seq.is_some() && meta.exch_ts.is_some());
        let reply = fixture_text("reply-create.json");
        let pushed = frame_metas(&mut codec, RawFrame::Text(&reply));
        assert_eq!(pushed[at], (meta, phase));
    }
}

#[test]
fn an_event_naming_another_venue_id_settles_no_placement() {
    // Codex r4228157178 on PR #145: our client id under a venue id the reply did not name is
    // not the placement's event, before the reply or after it.
    let other = "1759500000000000009";
    let mut codec = placed();
    let stray = with_vid(&new_event_as(3), other);
    let call = frame(&mut codec, RawFrame::Binary(&stray));
    assert_eq!(call.events, decoded(&stray));
    let open = new_event_as(3);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    let mut expected = vec![accepted(11, AckLevel::Final)];
    expected.extend(decoded(&open));
    assert_eq!(call.events, expected);
    // Before the reply: the stray event is not taken for the placement's.
    let mut codec = authenticated();
    encode(&mut codec, &place(), RpcId(11)).0.unwrap();
    let stray = with_vid(&sbe("order-closed-post-only-v2.sbe.txt"), other);
    frame(&mut codec, RawFrame::Binary(&stray)).result.unwrap();
    let call = text(&mut codec, &fixture_text("reply-create.json"));
    assert_eq!(call.events, [accepted(11, AckLevel::Provisional)]);
    let call = frame(&mut codec, RawFrame::Binary(&open));
    let mut expected = vec![accepted(11, AckLevel::Final)];
    expected.extend(decoded(&open));
    assert_eq!(call.events, expected);
}
