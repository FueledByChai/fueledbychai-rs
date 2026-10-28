//! Offline QFEX command wire fixtures. The venue still declares no execution capability.

use std::fs;
use std::sync::OnceLock;
use std::time::Duration;

use fbc_core::{
    AccountKey, AmendOrder, AssetSym, CancelOrder, Channel, CidMint, ClientIdFormat, ClientOrderId,
    Effect, Effects, EncodeCtx, ExecEvent, ExecSink, FundingSpec, InstrumentId, InstrumentKind,
    InstrumentSpec, Lots, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason,
    OpKind, OrderKind, OrderRef, PathStamps, PriceGrid, QueryOrder, RawFrame, RpcId, Secret,
    Secrets, Side, SizeStep, SpecTable, StreamId, Ticks, Tif, TradingStatus, UnderlyingId,
    VenueCommand, VenueConfig, VenueFactory, VenueId, VenueMeta, VenueOrderId, WallNs,
    dispatch_market_data, encode_cid,
};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const INST: InstrumentId = InstrumentId::new(7);
const OLD: &str = "11111111-1111-4111-8111-111111111111";

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert("qfex.trade_url", "wss://trade.qfex.com");
    cfg.insert("qfex.public_key", "synthetic_public");
    cfg.insert("qfex.account_id", "primary");
    cfg.insert("qfex.account_key", "4");
    cfg.insert("qfex.rpc_timeout", "2500ms");
    cfg
}

fn creds() -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(
        "qfex.public_key",
        Secret::new("synthetic_public".to_owned()),
    );
    creds.insert(
        "qfex.secret_key",
        Secret::new("synthetic_secret".to_owned()),
    );
    creds
}

fn ctx() -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_545_414_000_000_000),
        mono: MonoNs(0),
        nonces: NonceBlock::new(vec![1, 2]),
    }
}

fn specs() -> SpecTable {
    let caps = Qfex.caps(&cfg()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol("AAPL-USD")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    let mut specs = SpecTable::new();
    specs.insert(InstrumentSpec {
        id: INST,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.01".parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.01".parse::<Decimal>().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: Some(Lots::new(1000).unwrap()),
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
        version: 1,
        fetched_at: WallNs(0),
    });
    specs
}

fn cid() -> ClientOrderId {
    static MINTED: OnceLock<ClientOrderId> = OnceLock::new();
    *MINTED.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("fbc-qfex-offline-orders-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), Namespace::new(7)).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_760_545_414_000_000_000));
        let id = mint.mint().unwrap();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        id
    })
}

fn venue_id(id: &str) -> VenueOrderId {
    dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
        scope.venue_order_id(id)
    })
    .unwrap()
}

fn place() -> VenueCommand {
    VenueCommand::Place(NewOrder {
        cid: cid(),
        inst: INST,
        side: Side::Buy,
        qty: Lots::new(150).unwrap(),
        kind: OrderKind::Limit { px: Ticks(20_000) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
    })
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);
impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

fn auth(codec: &mut Box<dyn fbc_core::ExecCodec>, fx: &mut Effects) {
    codec.on_open(StreamId(0), &ctx(), fx);
    fx.take();
    dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(r#"{"type":"auth","result":"success"}"#),
            scope,
            &specs(),
            &mut Sink::default(),
            fx,
        )
    })
    .unwrap();
    fx.take();
}

fn sent(
    codec: &mut Box<dyn fbc_core::ExecCodec>,
    cmd: &VenueCommand,
    rpc: RpcId,
) -> (Value, fbc_core::RateCharge, fbc_core::TrafficClass) {
    let mut fx = Effects::new();
    assert_eq!(
        codec.encode(cmd, rpc, &specs(), &ctx(), &mut PathStamps::off(), &mut fx),
        Ok(fbc_core::EncodeReceipt::new())
    );
    let [
        Effect::Send {
            frame,
            rpc: Some(call),
            charge,
            class,
            stream,
        },
    ] = fx.as_slice()
    else {
        panic!("one timed QFEX command frame expected");
    };
    assert_eq!(*stream, StreamId(0));
    assert_eq!(call.id, rpc);
    assert_eq!(call.timeout, Duration::from_millis(2500));
    (
        serde_json::from_slice(frame.bytes()).unwrap(),
        *charge,
        *class,
    )
}

#[test]
fn selected_commands_encode_exact_wire_and_documented_weights() {
    let mut codec = Qfex.exec_codec(&cfg(), creds()).unwrap().unwrap();
    let mut fx = Effects::new();
    assert_eq!(
        codec.encode(
            &place(),
            RpcId(1),
            &specs(),
            &ctx(),
            &mut PathStamps::off(),
            &mut fx
        ),
        Err(NotSentReason::Disconnected),
        "offline calls also require confirmed authentication"
    );
    assert!(fx.is_empty());
    auth(&mut codec, &mut fx);

    let (body, charge, class) = sent(&mut codec, &place(), RpcId(1));
    let wire_cid = encode_cid(&ClientIdFormat::Uuid, cid()).unwrap();
    assert_eq!(
        body,
        json!({"type":"add_order","params":{
            "symbol":"AAPL-USD","side":"BUY","order_type":"LIMIT",
            "order_time_in_force":"GTC","quantity":1.5,"price":200,
            "client_order_id":wire_cid.as_str(),"reduce_only":false
        }})
    );
    assert_eq!(charge.op, OpKind::Place);
    assert_eq!(charge.inst, Some(INST));
    assert_eq!(charge.weight.get(), 10);
    assert_eq!(class, fbc_core::TrafficClass::Normal);

    let amend = VenueCommand::Amend(AmendOrder {
        target: OrderRef::Venue(venue_id(OLD)),
        inst: INST,
        side: Side::Sell,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: true,
        reducing: true,
        px: Ticks(20_100),
        qty: Lots::new(250).unwrap(),
        cum_filled: Lots::new(50).unwrap(),
    });
    let (body, charge, class) = sent(&mut codec, &amend, RpcId(2));
    assert_eq!(
        body,
        json!({"type":"modify_order","params":{
            "order_id":OLD,"symbol":"AAPL-USD","side":"SELL","order_type":"LIMIT",
            "quantity":2.5,"price":201,"reduce_only":true
        }})
    );
    assert_eq!(charge.op, OpKind::Amend);
    assert_eq!(charge.inst, Some(INST));
    assert_eq!(charge.weight.get(), 10);
    assert_eq!(class, fbc_core::TrafficClass::Safety);

    let cancel = VenueCommand::Cancel(CancelOrder {
        target: OrderRef::Venue(venue_id(OLD)),
        inst: INST,
        side: Side::Buy,
        placement_nonce: None,
    });
    let (body, charge, class) = sent(&mut codec, &cancel, RpcId(3));
    assert_eq!(
        body,
        json!({"type":"cancel_order","params":{
            "order_id":OLD,"symbol":"AAPL-USD","cancel_order_id_type":"order_id"
        }})
    );
    assert_eq!(charge.op, OpKind::Cancel);
    assert_eq!(charge.inst, Some(INST));
    assert_eq!(charge.weight.get(), 10);
    assert_eq!(class, fbc_core::TrafficClass::Safety);

    let query = VenueCommand::Query(QueryOrder {
        target: OrderRef::Venue(venue_id(OLD)),
        inst: INST,
        placement_nonce: None,
    });
    let (body, charge, class) = sent(&mut codec, &query, RpcId(4));
    assert_eq!(
        body,
        json!({"type":"get_order","params":{"symbol":"AAPL-USD","order_id":OLD}})
    );
    assert_eq!(charge.op, OpKind::Query);
    assert_eq!(charge.inst, Some(INST));
    assert_eq!(charge.weight.get(), 20);
    assert_eq!(class, fbc_core::TrafficClass::Safety);

    let mut sink = Sink::default();
    let mut replacement: Value = serde_json::from_str(include_str!(
        "../../../../fixtures/qfex/private/order_ack_full.json"
    ))
    .unwrap();
    replacement["order_response"]["order_id"] = json!("22222222-2222-4222-8222-222222222222");
    let replacement = replacement.to_string();
    dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(&replacement),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    })
    .unwrap();
    assert!(
        sink.0.is_empty(),
        "an unsolicited replacement UUID settles no RPC"
    );
    assert!(matches!(fx.as_slice(), [Effect::Reconnect { .. }]));
    fx.take();
    assert_eq!(
        codec.encode(
            &place(),
            RpcId(4),
            &specs(),
            &ctx(),
            &mut PathStamps::off(),
            &mut fx
        ),
        Err(NotSentReason::Disconnected)
    );
    codec.on_rpc_timeout(RpcId(2), &mut sink);
    assert_eq!(
        sink.0,
        [ExecEvent::Outcome {
            rpc: RpcId(2),
            item: None,
            outcome: fbc_core::SubmitOutcome::Unknown
        }]
    );
}

#[test]
fn get_order_page_cannot_settle_a_query_without_request_identity() {
    let mut codec = Qfex.exec_codec(&cfg(), creds()).unwrap().unwrap();
    let mut fx = Effects::new();
    auth(&mut codec, &mut fx);
    let query = VenueCommand::Query(QueryOrder {
        target: OrderRef::Venue(venue_id("2f05173c-992a-426b-bcaf-6b341644bb62")),
        inst: INST,
        placement_nonce: None,
    });
    sent(&mut codec, &query, RpcId(11));
    let mut sink = Sink::default();
    dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
        codec.on_frame(
            StreamId(0),
            RawFrame::Text(include_str!(
                "../../../../fixtures/qfex/private/open_orders_page.json"
            )),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    })
    .unwrap();
    assert!(sink.0.is_empty());
    assert!(matches!(fx.as_slice(), [Effect::Reconnect { .. }]));
    codec.on_rpc_timeout(RpcId(11), &mut sink);
    assert_eq!(
        sink.0,
        [ExecEvent::Outcome {
            rpc: RpcId(11),
            item: None,
            outcome: fbc_core::SubmitOutcome::Unknown,
        }]
    );
}

#[test]
fn unsafe_command_shapes_and_missing_timeout_refuse_without_effects() {
    let mut missing = cfg();
    missing.insert("qfex.rpc_timeout", "");
    assert!(Qfex.exec_codec(&missing, creds()).unwrap().is_err());
    let mut seconds = cfg();
    seconds.insert("qfex.rpc_timeout", "2s");
    assert!(Qfex.exec_codec(&seconds, creds()).unwrap().is_ok());
    let mut codec = Qfex.exec_codec(&cfg(), creds()).unwrap().unwrap();
    let mut fx = Effects::new();
    auth(&mut codec, &mut fx);
    let mut invalid = vec![
        (
            VenueCommand::ArmCancelOnDisconnect(true),
            NotSentReason::Unsupported,
        ),
        (
            VenueCommand::Cancel(CancelOrder {
                target: OrderRef::Client(cid()),
                inst: INST,
                side: Side::Buy,
                placement_nonce: None,
            }),
            NotSentReason::Unsupported,
        ),
        (
            VenueCommand::Cancel(CancelOrder {
                target: OrderRef::Venue(venue_id("not-a-uuid")),
                inst: INST,
                side: Side::Buy,
                placement_nonce: None,
            }),
            NotSentReason::Unencodable,
        ),
        (
            VenueCommand::Query(QueryOrder {
                target: OrderRef::Client(cid()),
                inst: INST,
                placement_nonce: None,
            }),
            NotSentReason::Unsupported,
        ),
        (
            VenueCommand::Query(QueryOrder {
                target: OrderRef::Venue(venue_id("not-a-uuid")),
                inst: INST,
                placement_nonce: None,
            }),
            NotSentReason::Unencodable,
        ),
    ];
    let mut market = place();
    if let VenueCommand::Place(ref mut order) = market {
        order.kind = OrderKind::Market;
    }
    invalid.push((market, NotSentReason::Unsupported));
    let mut ioc = place();
    if let VenueCommand::Place(ref mut order) = ioc {
        order.tif = Tif::Ioc;
    }
    invalid.push((ioc, NotSentReason::Unsupported));
    let mut zero = place();
    if let VenueCommand::Place(ref mut order) = zero {
        order.qty = Lots::ZERO;
    }
    invalid.push((zero, NotSentReason::Unencodable));
    let mut zero_price = place();
    if let VenueCommand::Place(ref mut order) = zero_price {
        order.kind = OrderKind::Limit { px: Ticks(0) };
    }
    invalid.push((zero_price, NotSentReason::Unencodable));
    let amend = AmendOrder {
        target: OrderRef::Venue(venue_id(OLD)),
        inst: INST,
        side: Side::Buy,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
        px: Ticks(20_000),
        qty: Lots::new(100).unwrap(),
        cum_filled: Lots::new(100).unwrap(),
    };
    invalid.push((
        VenueCommand::Amend(amend.clone()),
        NotSentReason::Unencodable,
    ));
    let mut unsupported_amend = amend;
    unsupported_amend.tif = Tif::Fok;
    invalid.push((
        VenueCommand::Amend(unsupported_amend),
        NotSentReason::Unsupported,
    ));
    for (cmd, reason) in invalid {
        assert_eq!(
            codec.encode(
                &cmd,
                RpcId(8),
                &specs(),
                &ctx(),
                &mut PathStamps::off(),
                &mut fx
            ),
            Err(reason)
        );
        assert!(fx.is_empty(), "refused command must not write");
    }
}
