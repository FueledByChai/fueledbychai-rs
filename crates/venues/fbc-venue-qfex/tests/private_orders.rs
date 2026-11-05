//! FBC-blcw: QFEX's order commands encoded offline, on the recording-only trade-socket codec
//! (`caps.exec` stays `None`, readiness `Record`, and the factory builds no order-entry codec, so
//! these tests build the codec directly).
//!
//! - A limit GTC, an ALO (post-only, GTC) and a limit IOC place each encode the `add_order`
//!   frame QFEX's AsyncAPI documents, alike for the primary account and a subaccount (the
//!   account is the authentication's, never the frame's), charged in tenths of QFEX's weight
//!   unit on the provider user's shared general bucket.
//! - `cancel_order` and `get_order` by the venue's order UUID encode exactly, the cancel charged
//!   on the cancel bucket.
//! - An off-grid or non-positive price, an out-of-range quantity, reduce-only, a modify, a
//!   client-id cancel or query, every batch, cancel-many and cancel-all, and the
//!   cancel-on-disconnect arm (on and off) are each refused with nothing written.
//! - No reply frame settles a request: each sent command ends `Unknown` at its deadline.
//! - FBC-99w5 (decision 0136): a place goes out only on a spec whose order shapes list its QFEX
//!   `order_type` (`LIMIT`, or `ALO` for post-only) and its `order_time_in_force` (`GTC` or
//!   `IOC`). One that omits either, lists nothing (empty or null in the refdata) or states none
//!   is refused `Unsupported` with nothing written, on specs built here and on specs discovered
//!   from `fixtures/qfex/rest/` through `InstrumentResolver::spec`.
//!
//! Every key, account, id and frame here is synthetic: the frames are built from the shapes
//! QFEX's pages show (each test names the page), or from `fixtures/qfex/private` (its
//! `SYNTHETIC` file).

use std::fs;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use fbc_core::{
    AccountKey, AliasTable, AmendOrder, AssetSym, CancelOrder, CancelScope, Channel, CidMint,
    ClientOrderId, ConnKey, ConnState, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec,
    ExecEvent, ExecSink, FundingSpec, HttpResponse, InstrumentId, InstrumentKind,
    InstrumentResolver, InstrumentSpec, LimitScope, Listing, Lots, MonoNs, Namespace,
    NamespaceLease, NewOrder, NonceBlock, NotSentReason, OpKind, OrderKind, OrderKindTag, OrderRef,
    OrderShapes, PathStamps, PriceGrid, QueryOrder, RateCharge, RawFrame, Readiness, RpcCall,
    RpcId, Secret, Secrets, Side, SizeStep, SpecTable, SubmitOutcome, TagSet, Ticks, Tif,
    TradingStatus, TrafficClass, UnderlyingId, VenueCaps, VenueCommand, VenueConfig, VenueFactory,
    VenueId, VenueMeta, VenueOrderId, Via, WallNs, dispatch, dispatch_market_data, encode_cid,
};
use fbc_runtime::{BucketKey, RateLimiter, RateUserKey, Request, SafetyReserve};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::discover::REFDATA_TAG;
use fbc_venue_qfex::exec::{CLIENT_ID_FORMAT, QfexExec, TRADE};
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// The AsyncAPI's authentication reply.
const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
/// The consumer's request deadline.
const RPC_TIMEOUT: Duration = Duration::from_millis(2_500);
/// Our namespace.
const NS: Namespace = Namespace::new(7);

/// A fixed 0.01 grid, lots of 0.01, five to 100,000 lots: `AAPL-USD`.
const AAPL: InstrumentId = InstrumentId::new(7);
/// A fixed 0.000001 grid, lots of 1,000: exact decimals far from one, `PEPE-USD`.
const PEPE: InstrumentId = InstrumentId::new(8);
/// A banded grid, 0.01 below 1,000 and 0.05 from it: a price can be off the grid, `GOOG-USD`.
const GOOG: InstrumentId = InstrumentId::new(9);
/// An instrument the spec table does not hold.
const MISSING: InstrumentId = InstrumentId::new(99);

/// Made-up venue order UUIDs, version 4.
const ORDER: &str = "2f05173c-992a-426b-bcaf-6b341644bb62";
const OTHER: &str = "5B309929-206F-40EC-804D-CBE46E81AFC1";

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

fn caps() -> VenueCaps {
    Qfex.caps(&cfg("primary")).unwrap()
}

fn fresh(account: &str) -> QfexExec {
    QfexExec::new(
        TradeAuth::new(&cfg(account), secrets()).unwrap(),
        RPC_TIMEOUT,
    )
}

/// The context of the `n`th call: the runtime reserves one nonce for an encode's one item,
/// which QFEX's frames never use.
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

/// What one frame gave: the events pushed and the effects asked for.
fn frame(codec: &mut QfexExec, text: &str) -> (Vec<ExecEvent>, Vec<Effect>) {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    // A frame QFEX refuses to describe may be a decode error; what it pushed is what counts.
    let _ = dispatch(&caps, NS, |scope| {
        codec.on_frame(
            TRADE,
            RawFrame::Text(text),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    });
    (sink.0, fx.take())
}

/// QFEX's confirmations of the three private subscriptions, the last of which reports the
/// stream authenticated (FBC-bsou).
const CONFIRMATIONS: [&str; 3] = [
    r#"{"subscribed":"order_responses"}"#,
    r#"{"subscribed":"fills"}"#,
    r#"{"subscribed":"positions"}"#,
];

/// Confirms the three subscriptions of an epoch whose auth reply came, checking that the last
/// confirmation, and only it, reported the stream authenticated.
fn confirm_all(codec: &mut QfexExec) {
    for confirmation in &CONFIRMATIONS[..2] {
        assert_eq!(frame(codec, confirmation), (vec![], vec![]));
    }
    let (events, effects) = frame(codec, CONFIRMATIONS[2]);
    assert_eq!(
        events,
        [ExecEvent::Conn {
            stream: TRADE,
            state: ConnState::Authenticated,
        }]
    );
    assert!(effects.is_empty(), "{effects:?}");
}

/// Opens epoch `n` and authenticates it, checking it did: the auth reply sends the three private
/// subscriptions and reports nothing, and their last confirmation reports the stream.
fn authenticate(codec: &mut QfexExec, n: u64) {
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(n), &mut fx);
    let (events, effects) = frame(codec, AUTHENTICATED);
    assert!(events.is_empty(), "{events:?}");
    assert_eq!(effects.len(), 3, "{effects:?}");
    confirm_all(codec);
}

fn authed(account: &str) -> QfexExec {
    let mut codec = fresh(account);
    authenticate(&mut codec, 1);
    codec
}

/// What one encode against `specs` gave, and what it pushed.
fn encode_on(
    codec: &mut QfexExec,
    cmd: &VenueCommand,
    rpc: RpcId,
    specs: &SpecTable,
) -> (Result<EncodeReceipt, NotSentReason>, Vec<Effect>) {
    let mut fx = Effects::new();
    let sent = codec.encode(
        cmd,
        rpc,
        specs,
        &ctx(rpc.0),
        &mut PathStamps::off(),
        &mut fx,
    );
    if sent.is_ok() {
        assert!(fx.carry_request(rpc, cmd.traffic_class()), "{fx:?}");
    }
    (sent, fx.take())
}

/// One sent command: its frame as text, its rate charge and its traffic class, after checking
/// it is one frame on the trade socket naming `rpc` with the consumer's deadline, unredacted
/// (it holds no credential), and that no nonce was used.
fn sent(
    codec: &mut QfexExec,
    cmd: &VenueCommand,
    rpc: RpcId,
) -> (String, RateCharge, TrafficClass) {
    sent_on(codec, cmd, rpc, &specs())
}

/// One command sent against `specs`, checked as [`sent`] checks it.
fn sent_on(
    codec: &mut QfexExec,
    cmd: &VenueCommand,
    rpc: RpcId,
    specs: &SpecTable,
) -> (String, RateCharge, TrafficClass) {
    let (result, effects) = encode_on(codec, cmd, rpc, specs);
    assert_eq!(result, Ok(EncodeReceipt::new()), "{cmd:?}");
    let [
        Effect::Send {
            stream,
            frame,
            rpc: Some(call),
            class,
            charge,
        },
    ] = effects.as_slice()
    else {
        panic!("one timed frame expected, got {effects:?}");
    };
    assert_eq!(*stream, TRADE);
    assert_eq!(
        *call,
        RpcCall {
            id: rpc,
            timeout: RPC_TIMEOUT
        }
    );
    assert!(frame.redactions().is_empty());
    let text = String::from_utf8(frame.bytes().to_vec()).unwrap();
    // Every frame is one JSON object.
    let value: Value = serde_json::from_str(&text).unwrap();
    assert!(value.is_object());
    (text, *charge, *class)
}

fn refused(codec: &mut QfexExec, cmd: &VenueCommand, why: NotSentReason) {
    refused_on(codec, cmd, why, &specs());
}

fn refused_on(codec: &mut QfexExec, cmd: &VenueCommand, why: NotSentReason, specs: &SpecTable) {
    let (result, effects) = encode_on(codec, cmd, RpcId(900), specs);
    assert_eq!(result, Err(why), "{cmd:?}");
    assert!(effects.is_empty(), "{cmd:?} wrote {effects:?}");
}

fn charge(op: OpKind, inst: InstrumentId, tenths: u32) -> RateCharge {
    RateCharge {
        op,
        inst: Some(inst),
        weight: tenths.try_into().unwrap(),
    }
}

// ----- Specs, ids and commands -----

fn spec(
    id: InstrumentId,
    symbol: &str,
    price_grid: PriceGrid,
    step: &str,
    min: i64,
    max: Option<i64>,
) -> InstrumentSpec {
    let venue_symbol = dispatch_market_data(&caps(), |scope| scope.venue_symbol(symbol)).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(id.get()),
        kind: InstrumentKind::Perpetual,
        price_grid,
        quote_grid: None,
        size_step: SizeStep::new(step.parse::<Decimal>().unwrap()).unwrap(),
        min_size: Lots::new(min).unwrap(),
        min_notional: None,
        max_order_size: max.map(|max| Lots::new(max).unwrap()),
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        // Every v1 shape listed: LIMIT and ALO, GTC and IOC (decision 0136).
        order_shapes: Some(v1_lists()),
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// Lists naming exactly the v1 places' order types and times in force: `LIMIT` and `ALO`,
/// `GTC` and `IOC`.
fn v1_lists() -> OrderShapes {
    OrderShapes {
        kinds: TagSet::of(&[OrderKindTag::Limit]),
        post_only: true,
        tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
    }
}

fn dec(text: &str) -> Decimal {
    text.parse().unwrap()
}

fn specs() -> SpecTable {
    let mut specs = SpecTable::new();
    specs.insert(spec(
        AAPL,
        "AAPL-USD",
        PriceGrid::fixed(dec("0.01")).unwrap(),
        "0.01",
        5,
        Some(100_000),
    ));
    specs.insert(spec(
        PEPE,
        "PEPE-USD",
        PriceGrid::fixed(dec("0.000001")).unwrap(),
        "1000",
        1,
        None,
    ));
    specs.insert(spec(
        GOOG,
        "GOOG-USD",
        PriceGrid::banded(&[(dec("0"), dec("0.01")), (dec("1000"), dec("0.05"))]).unwrap(),
        "0.001",
        1,
        Some(1_000_000),
    ));
    specs
}

/// Client ids minted under a held namespace lease, once per test binary.
fn cids() -> &'static [ClientOrderId] {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-qfex-orders-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_760_000_000_000_000_000));
        let ids = (0..8).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        ids
    })
}

fn cid(n: usize) -> ClientOrderId {
    cids()[n]
}

/// A client id as QFEX's frame carries it.
fn wire(cid: ClientOrderId) -> String {
    encode_cid(&CLIENT_ID_FORMAT, cid)
        .unwrap()
        .as_str()
        .to_owned()
}

fn venue_id(id: &str) -> VenueOrderId {
    dispatch_market_data(&caps(), |scope| scope.venue_order_id(id)).unwrap()
}

fn order(
    n: usize,
    inst: InstrumentId,
    side: Side,
    lots: i64,
    px: i64,
    tif: Tif,
    post_only: bool,
) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst,
        side,
        qty: Lots::new(lots).unwrap(),
        kind: OrderKind::Limit { px: Ticks(px) },
        tif,
        channel: Channel::Public,
        post_only,
        reduce_only: false,
        reducing: false,
    }
}

/// The three supported places: a limit GTC buy, an ALO sell and a limit IOC buy.
fn gtc() -> NewOrder {
    order(0, AAPL, Side::Buy, 150, 20_050, Tif::Gtc, false)
}

fn alo() -> NewOrder {
    order(1, AAPL, Side::Sell, 5, 20_100, Tif::Gtc, true)
}

fn ioc() -> NewOrder {
    order(2, AAPL, Side::Buy, 100_000, 19_999, Tif::Ioc, false)
}

fn place(o: NewOrder) -> VenueCommand {
    VenueCommand::Place(o)
}

fn cancel(target: OrderRef, inst: InstrumentId) -> VenueCommand {
    VenueCommand::Cancel(CancelOrder {
        target,
        inst,
        side: Side::Buy,
        placement_nonce: None,
    })
}

fn query(target: OrderRef, inst: InstrumentId) -> VenueCommand {
    VenueCommand::Query(QueryOrder {
        target,
        inst,
        placement_nonce: None,
    })
}

/// The `add_order` frame the AsyncAPI's `AddOrderCommand` documents
/// (https://docs.qfex.com/websocket/trade.yaml): its example's members in its example's order,
/// prices and quantities as JSON numbers, no member it leaves optional and we do not use.
fn add_order(
    symbol: &str,
    side: &str,
    order_type: &str,
    tif: &str,
    quantity: &str,
    price: &str,
    cid: ClientOrderId,
) -> String {
    format!(
        r#"{{"type":"add_order","params":{{"symbol":"{symbol}","side":"{side}","order_type":"{order_type}","order_time_in_force":"{tif}","quantity":{quantity},"price":{price},"client_order_id":"{}"}}}}"#,
        wire(cid)
    )
}

// ----- Places -----

/// The add_order page (https://docs.qfex.com/websocket/channels/trade/add_order) and the AsyncAPI
/// name LIMIT and ALO as order types and GTC and IOC as times in force; the order-types page
/// (https://docs.qfex.com/qfex/order-types) admits ALO with GTC only. The rate page
/// (https://docs.qfex.com/websocket/rate) weighs add_order 1.0: ten tenths.
#[test]
fn each_supported_place_encodes_the_documented_add_order_frame_for_the_selected_account_with_its_charge()
 {
    let want = [
        (
            gtc(),
            add_order("AAPL-USD", "BUY", "LIMIT", "GTC", "1.5", "200.5", cid(0)),
        ),
        (
            alo(),
            add_order("AAPL-USD", "SELL", "ALO", "GTC", "0.05", "201", cid(1)),
        ),
        (
            ioc(),
            add_order("AAPL-USD", "BUY", "LIMIT", "IOC", "1000", "199.99", cid(2)),
        ),
    ];
    let mut by_account = Vec::new();
    for account in ["primary", SUB] {
        let mut codec = authed(account);
        let mut frames = Vec::new();
        for (n, (o, frame)) in want.iter().enumerate() {
            let (text, charge, class) = sent(&mut codec, &place(o.clone()), RpcId(n as u64 + 1));
            assert_eq!(&text, frame, "{account}");
            assert_eq!(charge, self::charge(OpKind::Place, AAPL, 10));
            assert_eq!(class, TrafficClass::Normal);
            // The account is the authentication's: no frame names it, nor the key.
            for word in [SUB, PUBLIC, "account", "user_id", "reduce_only"] {
                assert!(!text.contains(word), "{text} names {word}");
            }
            frames.push(text);
        }
        by_account.push(frames);
    }
    assert_eq!(by_account[0], by_account[1]);

    let mut codec = authed(SUB);
    // The OMS's own reducing classification is no venue flag: the same frame, sent as Safety.
    let reducing = NewOrder {
        reducing: true,
        side: Side::Sell,
        ..ioc()
    };
    let (text, charge, class) = sent(&mut codec, &place(reducing), RpcId(10));
    assert_eq!(
        text,
        add_order("AAPL-USD", "SELL", "LIMIT", "IOC", "1000", "199.99", cid(2))
    );
    assert_eq!(charge, self::charge(OpKind::Place, AAPL, 10));
    assert_eq!(class, TrafficClass::Safety);

    // Exact decimals, never an exponent: a millionth price, lots of a thousand.
    let tiny = order(3, PEPE, Side::Buy, 3, 5, Tif::Gtc, true);
    let (text, charge, _) = sent(&mut codec, &place(tiny), RpcId(11));
    assert_eq!(
        text,
        add_order("PEPE-USD", "BUY", "ALO", "GTC", "3000", "0.000005", cid(3))
    );
    assert_eq!(charge, self::charge(OpKind::Place, PEPE, 10));
    let big = order(
        4,
        PEPE,
        Side::Sell,
        123_456_789,
        1_234_567_891,
        Tif::Ioc,
        false,
    );
    let (text, _, _) = sent(&mut codec, &place(big), RpcId(12));
    assert_eq!(
        text,
        add_order(
            "PEPE-USD",
            "SELL",
            "LIMIT",
            "IOC",
            "123456789000",
            "1234.567891",
            cid(4)
        )
    );
    // On a banded grid, a price on its band's tick.
    let banded = order(5, GOOG, Side::Buy, 2_708, 100_005, Tif::Gtc, false);
    let (text, charge, _) = sent(&mut codec, &place(banded), RpcId(13));
    assert_eq!(
        text,
        add_order(
            "GOOG-USD",
            "BUY",
            "LIMIT",
            "GTC",
            "2.708",
            "1000.05",
            cid(5)
        )
    );
    assert_eq!(charge, self::charge(OpKind::Place, GOOG, 10));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["params"]["quantity"], json!(2.708));
}

// ----- Cancels and queries -----

/// The cancel_order page (https://docs.qfex.com/websocket/channels/trade/cancel_order) and the
/// get_order page (https://docs.qfex.com/websocket/channels/trade/get_order), each frame in its
/// example's member order; the rate page weighs cancel_order 1.0 and get_order 2.0.
#[test]
fn cancel_and_get_order_by_venue_uuid_encode_exactly() {
    let mut codec = authed("primary");
    let targets = [
        (OrderRef::Venue(venue_id(ORDER)), ORDER),
        (OrderRef::Both(cid(0), venue_id(OTHER)), OTHER),
    ];
    for (n, (target, id)) in targets.into_iter().enumerate() {
        let rpc = RpcId(20 + 2 * n as u64);
        let (text, charge, class) = sent(&mut codec, &cancel(target.clone(), AAPL), rpc);
        assert_eq!(
            text,
            format!(
                r#"{{"type":"cancel_order","params":{{"order_id":"{id}","symbol":"AAPL-USD","cancel_order_id_type":"order_id"}}}}"#
            )
        );
        assert_eq!(charge, self::charge(OpKind::Cancel, AAPL, 10));
        assert_eq!(class, TrafficClass::Safety);

        let (text, charge, class) = sent(&mut codec, &query(target, GOOG), RpcId(rpc.0 + 1));
        assert_eq!(
            text,
            format!(r#"{{"type":"get_order","params":{{"symbol":"GOOG-USD","order_id":"{id}"}}}}"#)
        );
        assert_eq!(charge, self::charge(OpKind::Query, GOOG, 20));
        assert_eq!(class, TrafficClass::Safety);
    }
}

/// Decisions 0123 and 0128: every charge lands on the buckets the primary account and its
/// subaccounts share, places and queries on the general one and cancels on the cancel one, in
/// tenths of a weight unit.
#[test]
fn the_charges_reach_the_provider_users_shared_buckets() {
    let limits = caps().exec_limits.unwrap();
    assert!(limits.iter().all(|limit| limit.scope == LimitScope::User));
    let reserve = SafetyReserve::percent(20).unwrap();
    let user = RateUserKey::new(0xb1c0_0001);
    let budgets = [
        RateLimiter::for_user(user, &limits, reserve).unwrap(),
        RateLimiter::for_user(user, &limits, reserve).unwrap(),
    ];
    let conn = ConnKey { conn: 1, epoch: 1 };
    let now = tokio::time::Instant::now();
    for (budget, account) in budgets.iter().zip(["primary", SUB]) {
        let mut codec = authed(account);
        for (n, cmd) in [
            place(gtc()),
            place(alo()),
            place(ioc()),
            cancel(OrderRef::Venue(venue_id(ORDER)), AAPL),
            query(OrderRef::Venue(venue_id(ORDER)), AAPL),
        ]
        .iter()
        .enumerate()
        {
            let (_, charge, class) = sent(&mut codec, cmd, RpcId(n as u64 + 1));
            let request = Request {
                charge,
                via: Via::Frame,
                class,
            };
            budget.charge(now, conn, &[request]).unwrap();
        }
    }
    // Each account sent three places (10 each) and a query (20) on the general bucket, and a
    // cancel (10) on the cancel bucket; both see the whole user's use.
    for budget in &budgets {
        assert_eq!(budget.used(now, 0, BucketKey::Shared), 2 * (3 * 10 + 20));
        assert_eq!(budget.used(now, 1, BucketKey::Shared), 2 * 10);
    }
}

// ----- Refusals -----

/// Every command the v1 encoder does not send, and why: what QFEX leaves undocumented or
/// contradictory is refused, not guessed.
fn refusals() -> Vec<(VenueCommand, NotSentReason)> {
    use NotSentReason::{FlagConflict, Unencodable, Unsupported};
    let with = |f: fn(&mut NewOrder)| {
        let mut o = gtc();
        f(&mut o);
        place(o)
    };
    let amend = AmendOrder {
        target: OrderRef::Venue(venue_id(ORDER)),
        inst: AAPL,
        side: Side::Buy,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
        px: Ticks(20_100),
        qty: Lots::new(250).unwrap(),
        cum_filled: Lots::ZERO,
    };
    let by_client = OrderRef::Client(cid(0));
    let mut out = vec![
        // A price off the grid, at zero and below; on the banded grid 1,000.01 is off its 0.05.
        (
            place(order(6, GOOG, Side::Buy, 1, 100_001, Tif::Gtc, false)),
            Unencodable,
        ),
        (
            place(order(6, GOOG, Side::Sell, 1, 100_003, Tif::Gtc, true)),
            Unencodable,
        ),
        (
            with(|o| o.kind = OrderKind::Limit { px: Ticks(0) }),
            Unencodable,
        ),
        (
            with(|o| o.kind = OrderKind::Limit { px: Ticks(-1) }),
            Unencodable,
        ),
        (
            with(|o| {
                o.kind = OrderKind::Limit {
                    px: Ticks(i64::MIN),
                }
            }),
            Unencodable,
        ),
        // A quantity of nothing, below the minimum and above the maximum.
        (with(|o| o.qty = Lots::ZERO), Unencodable),
        (with(|o| o.qty = Lots::new(4).unwrap()), Unencodable),
        (with(|o| o.qty = Lots::new(100_001).unwrap()), Unencodable),
        (with(|o| o.qty = Lots::new(i64::MAX).unwrap()), Unencodable),
        // An instrument the spec table does not hold.
        (with(|o| o.inst = MISSING), Unencodable),
        (
            cancel(OrderRef::Venue(venue_id(ORDER)), MISSING),
            Unencodable,
        ),
        (
            query(OrderRef::Venue(venue_id(ORDER)), MISSING),
            Unencodable,
        ),
        // A shape v1 does not send: a market order, fill-or-kill, the RPI channel, and an ALO
        // with any time in force but GTC (the order-types page's table).
        (with(|o| o.kind = OrderKind::Market), Unsupported),
        (with(|o| o.tif = Tif::Fok), Unsupported),
        (with(|o| o.channel = Channel::Rpi), Unsupported),
        (
            with(|o| {
                o.post_only = true;
                o.tif = Tif::Ioc;
            }),
            FlagConflict,
        ),
        (
            with(|o| {
                o.post_only = true;
                o.tif = Tif::Fok;
            }),
            Unsupported,
        ),
        // Reduce-only, until a capture settles its semantics, whatever else the order is.
        (with(|o| o.reduce_only = true), Unsupported),
        (
            with(|o| {
                o.reduce_only = true;
                o.reducing = true;
                o.tif = Tif::Ioc;
            }),
            Unsupported,
        ),
        (
            with(|o| {
                o.reduce_only = true;
                o.post_only = true;
            }),
            Unsupported,
        ),
        // modify_order, until a capture settles its quantity and a partly filled order's.
        (VenueCommand::Amend(amend.clone()), Unsupported),
        (
            VenueCommand::Amend(AmendOrder {
                cum_filled: Lots::new(50).unwrap(),
                post_only: true,
                ..amend.clone()
            }),
            Unsupported,
        ),
        (
            VenueCommand::Amend(AmendOrder {
                target: OrderRef::Client(cid(0)),
                ..amend
            }),
            Unsupported,
        ),
        // A cancel or query by client id, which may cancel several orders of the symbol.
        (cancel(by_client.clone(), AAPL), Unsupported),
        (
            VenueCommand::Cancel(CancelOrder {
                target: by_client.clone(),
                inst: AAPL,
                side: Side::Sell,
                placement_nonce: Some(41),
            }),
            Unsupported,
        ),
        (query(by_client.clone(), AAPL), Unsupported),
        (
            VenueCommand::Query(QueryOrder {
                target: by_client,
                inst: AAPL,
                placement_nonce: Some(41),
            }),
            Unsupported,
        ),
        // A venue id that is no UUID at all.
        (
            cancel(OrderRef::Venue(venue_id("not-a-uuid")), AAPL),
            Unencodable,
        ),
        (
            query(OrderRef::Venue(venue_id("not-a-uuid")), AAPL),
            Unencodable,
        ),
        (
            cancel(OrderRef::Venue(venue_id(&ORDER[1..])), AAPL),
            Unencodable,
        ),
        (
            query(OrderRef::Venue(venue_id(&ORDER[1..])), AAPL),
            Unencodable,
        ),
        // Every batch, cancel-many and cancel-all.
        (VenueCommand::PlaceBatch(Vec::new()), Unsupported),
        (VenueCommand::PlaceBatch(vec![gtc()]), Unsupported),
        (VenueCommand::PlaceBatch(vec![gtc(), alo()]), Unsupported),
        (VenueCommand::CancelMany(Vec::new()), Unsupported),
        (
            VenueCommand::CancelMany(vec![CancelOrder {
                target: OrderRef::Venue(venue_id(ORDER)),
                inst: AAPL,
                side: Side::Buy,
                placement_nonce: None,
            }]),
            Unsupported,
        ),
        (VenueCommand::CancelAll(CancelScope::Account), Unsupported),
        (
            VenueCommand::CancelAll(CancelScope::Instrument(AAPL)),
            Unsupported,
        ),
        // The cancel-on-disconnect arm, on and off, and what QFEX has no request for.
        (VenueCommand::ArmCancelOnDisconnect(true), Unsupported),
        (VenueCommand::ArmCancelOnDisconnect(false), Unsupported),
        (VenueCommand::RefreshDeadMan, Unsupported),
        (VenueCommand::FeeQuery, Unsupported),
    ];
    out.push((
        VenueCommand::CancelMany(vec![
            CancelOrder {
                target: OrderRef::Venue(venue_id(ORDER)),
                inst: AAPL,
                side: Side::Buy,
                placement_nonce: None,
            },
            CancelOrder {
                target: OrderRef::Venue(venue_id(OTHER)),
                inst: GOOG,
                side: Side::Sell,
                placement_nonce: None,
            },
        ]),
        Unsupported,
    ));
    out
}

#[test]
fn each_refused_command_writes_nothing() {
    for account in ["primary", SUB] {
        let mut codec = authed(account);
        for (cmd, why) in refusals() {
            refused(&mut codec, &cmd, why);
        }
        // Nothing refused touched the epoch: a supported place still goes out.
        sent(&mut codec, &place(gtc()), RpcId(1));
    }
    // A command is refused for what it is before the connection is asked about.
    let mut codec = fresh("primary");
    for (cmd, why) in refusals() {
        refused(&mut codec, &cmd, why);
    }
}

/// A supported command goes out only on an authenticated epoch: not before a stream opens, not
/// while authentication is unanswered, not while a subscription is unconfirmed (FBC-bsou), and
/// not once the epoch has ended.
#[test]
fn a_supported_command_waits_for_an_authenticated_epoch() {
    let supported = [
        place(gtc()),
        place(alo()),
        place(ioc()),
        cancel(OrderRef::Venue(venue_id(ORDER)), AAPL),
        query(OrderRef::Venue(venue_id(ORDER)), AAPL),
    ];
    let disconnected = |codec: &mut QfexExec| {
        for cmd in &supported {
            refused(codec, cmd, NotSentReason::Disconnected);
        }
    };
    let mut codec = fresh(SUB);
    disconnected(&mut codec);
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(1), &mut fx);
    disconnected(&mut codec);
    let (events, _) = frame(&mut codec, AUTHENTICATED);
    assert!(events.is_empty(), "{events:?}");
    disconnected(&mut codec);
    for confirmation in &CONFIRMATIONS[..2] {
        frame(&mut codec, confirmation);
        disconnected(&mut codec);
    }
    let (events, _) = frame(&mut codec, CONFIRMATIONS[2]);
    assert_eq!(events.len(), 1);
    for (n, cmd) in supported.iter().enumerate() {
        sent(&mut codec, cmd, RpcId(n as u64 + 1));
    }
    // Private state ends the epoch (FBC-zeb0): nothing more goes out on it.
    let (_, effects) = frame(&mut codec, &fixture("order_response.json"));
    assert!(matches!(effects.as_slice(), [Effect::Reconnect { .. }]));
    disconnected(&mut codec);
    // A resync ends it too.
    authenticate(&mut codec, 2);
    let mut fx = Effects::new();
    codec.resync(&ctx(2), &mut fx);
    disconnected(&mut codec);
    // The next epoch authenticates and sends again.
    authenticate(&mut codec, 3);
    sent(&mut codec, &place(gtc()), RpcId(9));
}

// ----- No reply settles a request -----

fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

/// `fixtures/qfex/private/order_response.json` naming `client_order_id`, `order_id` and
/// `status`.
fn order_response(cid: &str, order_id: &str, status: &str, remaining: f64) -> String {
    let mut frame: Value = serde_json::from_str(&fixture("order_response.json")).unwrap();
    let order = &mut frame["order_response"];
    order["client_order_id"] = json!(cid);
    order["order_id"] = json!(order_id);
    order["status"] = json!(status);
    order["quantity_remaining"] = json!(remaining);
    frame.to_string()
}

/// QFEX's errors page and AsyncAPI write a nested err; its rate page a flat one; the AsyncAPI a
/// generic ack. Each echoes the request as it was sent.
fn nested(code: &str, echo: &str) -> String {
    format!(
        r#"{{"err":{{"error_code":"{code}","message":"as the errors page says","incoming_message":{echo}}}}}"#
    )
}

fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":"{message}","incoming_message":{echo}}}"#
    )
}

fn ack(echo: &str) -> String {
    format!(r#"{{"ack":{{"incoming_message":{echo}}}}}"#)
}

#[test]
fn an_rpc_timeout_gives_unknown_and_no_reply_frame_settles_a_request() {
    let mut codec = authed("primary");
    let commands = [
        place(gtc()),
        place(alo()),
        place(ioc()),
        cancel(OrderRef::Venue(venue_id(ORDER)), AAPL),
        query(OrderRef::Venue(venue_id(OTHER)), AAPL),
    ];
    let frames: Vec<String> = commands
        .iter()
        .enumerate()
        .map(|(n, cmd)| sent(&mut codec, cmd, RpcId(n as u64 + 1)).0)
        .collect();
    let [gtc_frame, alo_frame, ioc_frame, cancel_frame, query_frame] = frames.as_slice() else {
        unreachable!()
    };

    // Replies that keep the epoch open: an err with a request's code, an ack and a rate refusal,
    // each echoing a request exactly as it was sent, and a RATE_LIMITED order naming our place.
    let open = [
        nested("InvalidOrder", gtc_frame),
        flat("InvalidOrderId", "no such order", cancel_frame),
        nested("InvalidParameter", ioc_frame),
        ack(alo_frame),
        ack(query_frame),
        ack(cancel_frame),
        flat(
            "RateLimited",
            "Rate limit exceeded, retry after 2 seconds",
            gtc_frame,
        ),
        order_response(&wire(cid(0)), ORDER, "RATE_LIMITED", 1.5),
    ];
    for reply in &open {
        let (events, effects) = frame(&mut codec, reply);
        assert!(events.is_empty(), "{reply} settled {events:?}");
        assert!(
            effects
                .iter()
                .all(|effect| matches!(effect, Effect::RateLimited { .. })),
            "{reply}: {effects:?}"
        );
    }

    // Replies that name our requests by client id or order id: the GTC's ACK, the ALO's crossing
    // outcome as the enums page has it (REJECTED) and as the order-types page has it
    // (CANCELLED), the IOC's IOC_CANCELLED, the cancelled order, the queried order's page, and a
    // fill of our place. Each ends its epoch (private state is not normalized yet) and settles
    // nothing.
    let mut page: Value = serde_json::from_str(&fixture("all_orders_response.json")).unwrap();
    page["all_orders_response"]["orders"][0]["order_id"] = json!(OTHER);
    page["all_orders_response"]["orders"][0]["client_order_id"] = json!(wire(cid(1)));
    let mut fill: Value = serde_json::from_str(&fixture("fill_response.json")).unwrap();
    fill["fill_response"]["order_id"] = json!(ORDER);
    fill["fill_response"]["client_order_id"] = json!(wire(cid(0)));
    let ending = [
        order_response(&wire(cid(0)), ORDER, "ACK", 1.5),
        order_response(&wire(cid(1)), OTHER, "REJECTED", 0.0),
        order_response(&wire(cid(1)), OTHER, "CANCELLED", 0.0),
        order_response(&wire(cid(2)), ORDER, "IOC_CANCELLED", 0.0),
        order_response("", ORDER, "CANCELLED", 0.0),
        page.to_string(),
        fill.to_string(),
    ];
    for (n, reply) in ending.iter().enumerate() {
        let (events, effects) = frame(&mut codec, reply);
        assert!(events.is_empty(), "{reply} settled {events:?}");
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Reconnect { stream: TRADE, .. }]
            ),
            "{reply}: {effects:?}"
        );
        authenticate(&mut codec, 2 + n as u64);
    }
    // And the open-epoch replies again, now in a later epoch.
    for reply in &open {
        let (events, _) = frame(&mut codec, reply);
        assert!(events.is_empty(), "{reply} settled {events:?}");
    }

    // Each request ends Unknown at its deadline, once, whatever came back.
    for rpc in 1..=commands.len() as u64 {
        let mut sink = Sink::default();
        codec.on_rpc_timeout(RpcId(rpc), &mut sink);
        assert_eq!(
            sink.0,
            [ExecEvent::Outcome {
                rpc: RpcId(rpc),
                item: None,
                outcome: SubmitOutcome::Unknown,
            }]
        );
    }
}

// ----- What stays off -----

#[test]
fn the_factory_still_offers_no_order_entry() {
    let mut full = cfg(SUB);
    full.insert(auth::TRADE_URL, "wss://trade.qfex.com");
    let caps = Qfex.caps(&full).unwrap();
    assert!(caps.exec.is_none());
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    assert!(Qfex.plan_exec(&full).unwrap().is_empty());
    assert!(Qfex.exec_codec(&full, secrets()).is_none());
}

// ----- The order types and times in force the symbol lists (FBC-99w5, decision 0136) -----

fn lists(kinds: &[OrderKindTag], post_only: bool, tifs: &[Tif]) -> OrderShapes {
    OrderShapes {
        kinds: TagSet::of(kinds),
        post_only,
        tifs: TagSet::of(tifs),
    }
}

/// The spec table with `AAPL-USD`'s order shapes replaced by `shapes`.
fn aapl_listing(shapes: Option<OrderShapes>) -> SpecTable {
    let mut table = specs();
    let mut aapl = table.get(AAPL).unwrap().clone();
    aapl.order_shapes = shapes;
    table.insert(aapl);
    table
}

/// QFEX's refdata lists its order types and times in force apart (the refdata channel page,
/// https://docs.qfex.com/websocket/channels/mds/refdata), and post-only is the ALO type (the
/// enums page). Each v1 place needs its type and its time in force listed, and nothing more.
#[test]
fn each_v1_place_encodes_on_a_spec_listing_just_its_type_and_time_in_force() {
    use OrderKindTag::Limit;
    let cases = [
        (
            gtc(),
            lists(&[Limit], false, &[Tif::Gtc]),
            add_order("AAPL-USD", "BUY", "LIMIT", "GTC", "1.5", "200.5", cid(0)),
        ),
        (
            alo(),
            lists(&[], true, &[Tif::Gtc]),
            add_order("AAPL-USD", "SELL", "ALO", "GTC", "0.05", "201", cid(1)),
        ),
        (
            ioc(),
            lists(&[Limit], false, &[Tif::Ioc]),
            add_order("AAPL-USD", "BUY", "LIMIT", "IOC", "1000", "199.99", cid(2)),
        ),
    ];
    for account in ["primary", SUB] {
        let mut codec = authed(account);
        for (n, (o, shapes, frame)) in cases.iter().enumerate() {
            let table = aapl_listing(Some(*shapes));
            let (text, charge, _) =
                sent_on(&mut codec, &place(o.clone()), RpcId(n as u64 + 1), &table);
            assert_eq!(&text, frame, "{account}");
            assert_eq!(charge, self::charge(OpKind::Place, AAPL, 10));
        }
    }
}

/// A place whose type or time in force its spec does not list, or whose spec lists nothing or
/// states no lists, is refused `Unsupported` before any frame is written: QFEX signs no order
/// (the add_order page's frame has no signature, and its HMAC signs only the authentication),
/// so nothing is spent on it, and nothing reaches the shared buckets.
#[test]
fn a_place_its_spec_does_not_list_is_refused_unsupported_with_nothing_written() {
    use OrderKindTag::{Limit, Market};
    let v1 = v1_lists();
    let without_type = |o: &NewOrder| {
        if o.post_only {
            OrderShapes {
                post_only: false,
                ..v1
            }
        } else {
            OrderShapes {
                kinds: TagSet::none(),
                ..v1
            }
        }
    };
    let without_tif = |o: &NewOrder| {
        let others: Vec<Tif> = v1.tifs.iter().filter(|tif| *tif != o.tif).collect();
        OrderShapes {
            tifs: TagSet::of(&others),
            ..v1
        }
    };
    for o in [gtc(), alo(), ioc()] {
        let unlisted = [
            Some(without_type(&o)),
            Some(without_tif(&o)),
            // Empty or null in the refdata: nothing of either kind.
            Some(lists(&[], false, &[])),
            // Only what v1 never sends: a market order, fill-or-kill.
            Some(lists(&[Market], false, &[Tif::Fok])),
            // The type listed and only the other time in force, or the reverse.
            Some(lists(
                &[Limit],
                true,
                if o.tif == Tif::Gtc {
                    &[Tif::Ioc, Tif::Fok]
                } else {
                    &[Tif::Gtc, Tif::Fok]
                },
            )),
            // No lists stated at all: QFEX treats that as nothing admitted.
            None,
        ];
        for shapes in unlisted {
            let table = aapl_listing(shapes);
            // Before the connection is asked about, and on an authenticated epoch alike.
            refused_on(
                &mut fresh("primary"),
                &place(o.clone()),
                NotSentReason::Unsupported,
                &table,
            );
            let mut codec = authed(SUB);
            refused_on(
                &mut codec,
                &place(o.clone()),
                NotSentReason::Unsupported,
                &table,
            );
            // Nothing refused touched the epoch: the same place goes out on lists naming it.
            sent(&mut codec, &place(o.clone()), RpcId(1));
            // A cancel and a query are never held to the lists.
            let cancel = cancel(OrderRef::Venue(venue_id(ORDER)), AAPL);
            sent_on(&mut codec, &cancel, RpcId(2), &table);
            let query = query(OrderRef::Venue(venue_id(ORDER)), AAPL);
            sent_on(&mut codec, &query, RpcId(3), &table);
        }
    }
    // A shape v1 never sends is refused for what it is, whatever the spec lists.
    let mut codec = authed("primary");
    let table = aapl_listing(Some(lists(
        &[Limit, Market],
        true,
        &[Tif::Gtc, Tif::Ioc, Tif::Fok],
    )));
    let fok = NewOrder {
        tif: Tif::Fok,
        ..gtc()
    };
    let alo_ioc = NewOrder {
        tif: Tif::Ioc,
        ..alo()
    };
    let market = NewOrder {
        kind: OrderKind::Market,
        ..ioc()
    };
    refused_on(&mut codec, &place(fok), NotSentReason::Unsupported, &table);
    refused_on(
        &mut codec,
        &place(alo_ioc),
        NotSentReason::FlagConflict,
        &table,
    );
    refused_on(
        &mut codec,
        &place(market),
        NotSentReason::Unsupported,
        &table,
    );
}

/// The specs discovery gives for one hand-built `GET /refdata` answer, through
/// `InstrumentResolver::spec`, each symbol listed under `first` and the ids after it, with its
/// wire symbol.
fn discovered(body: &str, first: u32) -> (SpecTable, Vec<(InstrumentId, String)>) {
    let mut cfg = cfg("primary");
    cfg.insert(KEY_REST_URL, "https://api.qfex.com");
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    let plan = Qfex.discover(&cfg).unwrap();
    let answer = HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    };
    let drafts = dispatch_market_data(&caps(), |scope| {
        plan.parse(&[(REFDATA_TAG, Ok(answer))], scope)
    })
    .unwrap()
    .done()
    .expect("one round");
    let mut resolver = InstrumentResolver::new(AliasTable::empty());
    let mut table = SpecTable::new();
    let mut ids = Vec::new();
    for (n, draft) in drafts.into_iter().enumerate() {
        let id = InstrumentId::new(first + n as u32);
        let listing = Listing {
            id,
            underlying: UnderlyingId::new(id.get()),
        };
        resolver
            .list(VenueId::new(4), draft.asset, listing)
            .unwrap();
        ids.push((id, draft.venue_symbol.as_wire().to_owned()));
        table.insert(resolver.spec(VenueId::new(4), draft, 1, WallNs(0)).unwrap());
    }
    (table, ids)
}

fn rest_fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/rest")
            .join(name),
    )
    .unwrap()
}

/// The specs discovered from `fixtures/qfex/rest/refdata.json` and its variant: each v1 place
/// goes out on a symbol listing its type and time in force, as the add_order frame naming that
/// symbol, and is refused with nothing written on every other. The encoder reads no status:
/// whether a halted or delisted symbol is traded is fbc-oms's to judge.
#[test]
fn the_lists_discovered_from_refdata_decide_which_v1_places_each_symbol_takes() {
    let (main, main_ids) = discovered(&rest_fixture("refdata.json"), 21);
    let (variant, variant_ids) = discovered(&rest_fixture("refdata_order_lists.json"), 31);
    // Which of the GTC, ALO and IOC places each symbol takes.
    let want: [(&str, [bool; 3]); 8] = [
        // LIMIT, MARKET and ALO with GTC, IOC and FOK.
        ("AAPL-USD", [true, true, true]),
        // LIMIT without ALO.
        ("ANTHROPIC-USD", [true, false, true]),
        // ALO without LIMIT, GTC and IOC.
        ("NVDA-USD", [false, true, false]),
        // Empty arrays.
        ("GOLD-USD", [false, false, false]),
        // No IOC.
        ("MSFT-USD", [true, true, false]),
        // Null for both.
        ("TSLA-USD", [false, false, false]),
        // LIMIT and GTC among names outside the mapping.
        ("META-USD", [true, false, false]),
        // Only names outside the mapping, and IOC.
        ("AMZN-USD", [false, false, false]),
    ];
    let symbols = main_ids
        .iter()
        .map(|id| (id, &main))
        .chain(variant_ids.iter().map(|id| (id, &variant)));
    let mut codec = authed("primary");
    let mut rpc = 100;
    let mut seen = Vec::new();
    for (((inst, symbol), table), (want_symbol, takes)) in symbols.zip(want) {
        assert_eq!(symbol, want_symbol);
        seen.push(symbol.as_str());
        // A hundred lots at two thousand ticks: within every symbol's size limits and on its
        // grid.
        let at = |o: NewOrder| NewOrder {
            inst: *inst,
            qty: Lots::new(100).unwrap(),
            kind: OrderKind::Limit { px: Ticks(2_000) },
            ..o
        };
        let names = [("LIMIT", "GTC"), ("ALO", "GTC"), ("LIMIT", "IOC")];
        for ((o, (order_type, tif)), takes) in [at(gtc()), at(alo()), at(ioc())]
            .into_iter()
            .zip(names)
            .zip(takes)
        {
            rpc += 1;
            let cmd = place(o);
            if takes {
                let (text, charge, _) = sent_on(&mut codec, &cmd, RpcId(rpc), table);
                let frame: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(frame["params"]["symbol"], json!(symbol), "{text}");
                assert_eq!(frame["params"]["order_type"], json!(order_type), "{text}");
                assert_eq!(frame["params"]["order_time_in_force"], json!(tif), "{text}");
                assert_eq!(charge, self::charge(OpKind::Place, *inst, 10));
            } else {
                refused_on(&mut codec, &cmd, NotSentReason::Unsupported, table);
            }
        }
    }
    assert_eq!(seen.len(), 8);
    // The example's lists, discovered: the GTC place on AAPL-USD as the add_order page shows it.
    let aapl = NewOrder {
        inst: main_ids[0].0,
        qty: Lots::new(1_500).unwrap(),
        kind: OrderKind::Limit { px: Ticks(20_050) },
        ..gtc()
    };
    let (text, _, _) = sent_on(&mut codec, &place(aapl), RpcId(999), &main);
    assert_eq!(
        text,
        add_order("AAPL-USD", "BUY", "LIMIT", "GTC", "1.5", "200.5", cid(0))
    );
}
