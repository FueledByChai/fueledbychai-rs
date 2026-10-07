//! FBC-xe1 (decisions 0002, 0004, 0014, 0018, 0034, 0054): Paradex order entry encoded as the
//! socket's JSON-RPC 2.0 frames from the full command and a fixed `EncodeCtx` only. Each
//! command kind the socket takes is held to a frame built by hand here in the shape of
//! docs.paradex.trade's WebSocket method pages (and the Java library's
//! `buildSignedPlaceOrderJson` / `buildSignedModifyOrderJson`), with `signature_timestamp` the
//! context's wall time in milliseconds and the signature the one a second signer over the same
//! synthetic key gives for the same hand-written message. The same command and context encode
//! byte-identically twice, each signer call is marked as the sign stage, and an undeclared time
//! in force, reference or command (a client-id item in a batch cancel among them) is refused
//! `NotSent(Unsupported)` with no effect and no signer call.
//!
//! The signing key and account are the synthetic ones in `fixtures/paradex/signing/`'s header.

mod common;
mod md;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use common::Vectors;
use fbc_core::{
    AccountKey, AmendOrder, AmendWire, CancelOrder, CancelScope, CancelWire, Channel, CidMint,
    ClientIdFormat, ClientOrderId, Effect, Effects, EncodeCtx, EncodeReceipt, InstrumentId, Lots,
    MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OpKind, OrderKind,
    OrderRef, OrderSigner, PathEdge, PathMark, PathRecorder, PathStage, PathStamps, PlaceWire,
    QueryOrder, RateCharge, RpcCall, RpcId, Side, Sig, SignError, SpecTable, StreamId, Ticks, Tif,
    TrafficClass, VenueCommand, VenueOrderId, WallNs, dispatch, encode_cid,
};
use fbc_venue_paradex::exec::ParadexEncoder;
use fbc_venue_paradex::factory::caps_with_order_entry;
use fbc_venue_paradex::sign::{OrderMessage, ParadexOrderType, ParadexSigner};
use md::{BTC, ETH};
use rust_decimal::Decimal;

const OWN: Namespace = Namespace::new(7);
const STREAM: StreamId = StreamId(3);
const TIMEOUT: Duration = Duration::from_millis(2_500);
const RPC: RpcId = RpcId(42);
/// 2026-10-05T00:00:00.123456789Z: `signature_timestamp` is 1759622400123.
const WALL: WallNs = WallNs(1_759_622_400_123_456_789);
const TS_MS: u64 = 1_759_622_400_123;
const OID: &str = "1759500000000000001";
const OID2: &str = "1759500000000000002";
/// 62000.5 on the specs' 0.1 tick.
const PX: Ticks = Ticks(620_005);

fn ctx() -> EncodeCtx {
    // The runtime reserves a nonce per item whatever the venue's scope; Paradex uses none.
    EncodeCtx {
        wall: WALL,
        mono: MonoNs(77),
        nonces: NonceBlock::consecutive(900, 10).unwrap(),
    }
}

fn dec(text: &str) -> Decimal {
    text.parse().unwrap()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// Client id `n` of the first forty minted in [`OWN`], under a lease in a directory of the
/// test process's own.
fn cid(n: u64) -> ClientOrderId {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let minted = MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-paradex-encode-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(3), OWN).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_622_400_000_000_000));
        let ids = (0..40).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        ids
    });
    minted[usize::try_from(n).unwrap()]
}

/// Our client id in Paradex's UUID format, through fbc-core's codec.
fn uuid(seq: u64) -> String {
    encode_cid(&ClientIdFormat::Uuid, cid(seq))
        .unwrap()
        .as_str()
        .to_owned()
}

fn vid(text: &str) -> VenueOrderId {
    dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.venue_order_id(text)
    })
    .unwrap()
}

/// The synthetic signer, a second instance for the expected signatures.
fn signer() -> ParadexSigner {
    Vectors::read().signer()
}

/// A signature as a JSON string, written out by hand: `"[\"r\",\"s\"]"`.
fn sig_json(sig: fbc_venue_paradex::sign::StarkSig) -> String {
    format!(r#""[\"{}\",\"{}\"]""#, sig.r, sig.s)
}

/// The expected signature of a new order over the hand-written message.
fn order_sig(side: Side, order_type: ParadexOrderType, size: &str, price: &str) -> String {
    let msg = OrderMessage {
        timestamp_ms: TS_MS,
        market: "BTC-USD-PERP",
        side,
        order_type,
        size: dec(size),
        price: dec(price),
    };
    sig_json(signer().sign_order(&msg).unwrap())
}

/// The JSON-RPC envelope around `params`, as `ParadexOrderWebSocketClient.call` writes it.
fn rpc_frame(method: &str, params: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":42}}"#)
}

/// Counts its signer calls, passing them to the synthetic signer.
struct Counting {
    inner: ParadexSigner,
    calls: Arc<AtomicUsize>,
}

impl OrderSigner for Counting {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.sign_place(w)
    }
    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.sign_amend(w)
    }
    fn sign_cancel(&mut self, w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.sign_cancel(w)
    }
}

/// Refuses everything.
struct Failing;

impl OrderSigner for Failing {
    fn sign_place(&mut self, _: &PlaceWire<'_>) -> Result<Sig, SignError> {
        Err(SignError::Backend("synthetic"))
    }
    fn sign_amend(&mut self, _: &AmendWire<'_>) -> Result<Sig, SignError> {
        Err(SignError::Backend("synthetic"))
    }
    fn sign_cancel(&mut self, _: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        Err(SignError::Backend("synthetic"))
    }
}

#[derive(Default)]
struct Marks(Vec<PathMark>);

impl PathRecorder for Marks {
    fn mark(&mut self, mark: PathMark) {
        self.0.push(mark);
    }
}

/// What one encode gave: its result, its effects, the signer calls and the path marks.
struct Encoded {
    result: Result<EncodeReceipt, NotSentReason>,
    fx: Effects,
    sign_calls: usize,
    marks: Vec<PathMark>,
}

fn encode_with(cmd: &VenueCommand, specs: &SpecTable) -> Encoded {
    let calls = Arc::new(AtomicUsize::new(0));
    let signer = Counting {
        inner: signer(),
        calls: Arc::clone(&calls),
    };
    let mut enc = ParadexEncoder::new(Box::new(signer), STREAM, TIMEOUT);
    let (mut marks, mut fx) = (Marks::default(), Effects::new());
    let result = enc.encode(
        cmd,
        RPC,
        specs,
        &ctx(),
        &mut PathStamps::new(&mut marks),
        &mut fx,
    );
    Encoded {
        result,
        fx,
        sign_calls: calls.load(Ordering::Relaxed),
        marks: marks.0,
    }
}

fn encode(cmd: &VenueCommand) -> Encoded {
    encode_with(cmd, &md::specs())
}

/// The one frame `cmd` encodes into, asserting the rest of its effect: the stream, the rpc
/// with the configured timeout, the command's traffic class and `charge`, and an empty receipt.
fn frame(cmd: &VenueCommand, charge: RateCharge) -> String {
    let out = encode(cmd);
    assert_eq!(
        out.result,
        Ok(EncodeReceipt::new()),
        "Paradex uses no nonce"
    );
    assert!(out.fx.carry_request(RPC, cmd.traffic_class()));
    match out.fx.as_slice() {
        [
            Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge: charged,
            },
        ] => {
            assert_eq!(*stream, STREAM);
            assert_eq!(
                *rpc,
                Some(RpcCall {
                    id: RPC,
                    timeout: TIMEOUT
                })
            );
            assert_eq!(*class, cmd.traffic_class());
            assert_eq!(*charged, charge);
            assert!(
                frame.redactions().is_empty(),
                "nothing in it is a credential"
            );
            String::from_utf8(frame.bytes().to_vec()).unwrap()
        }
        other => panic!("not one frame: {other:?}"),
    }
}

/// Asserts `cmd` is refused with `why`, with no effect pushed and no signer call made.
fn refused(cmd: &VenueCommand, why: NotSentReason) {
    refused_with(cmd, &md::specs(), why);
}

fn refused_with(cmd: &VenueCommand, specs: &SpecTable, why: NotSentReason) {
    let out = encode_with(cmd, specs);
    assert_eq!(out.result, Err(why), "{cmd:?}");
    assert!(out.fx.is_empty(), "{cmd:?} pushed {:?}", out.fx);
    assert_eq!(out.sign_calls, 0, "{cmd:?} reached the signer");
    assert!(out.marks.is_empty());
}

fn order(seq: u64) -> NewOrder {
    NewOrder {
        cid: cid(seq),
        inst: BTC,
        side: Side::Buy,
        qty: lots(5),
        kind: OrderKind::Limit { px: PX },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
    }
}

fn place_charge(inst: InstrumentId) -> RateCharge {
    RateCharge::one(OpKind::Place, Some(inst))
}

fn amend(target: OrderRef) -> AmendOrder {
    AmendOrder {
        target,
        inst: BTC,
        side: Side::Sell,
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(620_010),
        qty: lots(8),
        cum_filled: lots(3),
    }
}

fn cancel(target: OrderRef) -> CancelOrder {
    CancelOrder {
        target,
        inst: BTC,
        side: Side::Buy,
        placement_nonce: None,
    }
}

#[test]
fn a_limit_order_is_order_create_with_its_fields_signed_and_its_timestamp_in_milliseconds() {
    let sig = order_sig(Side::Buy, ParadexOrderType::Limit, "0.005", "62000.5");
    let want = rpc_frame(
        "order.create",
        &format!(
            r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"BUY","signature_timestamp":1759622400123,"size":"0.005","type":"LIMIT","price":"62000.5","instruction":"GTC","signature":{sig}}}"#,
            uuid(1)
        ),
    );
    let place = VenueCommand::Place(order(1));
    assert_eq!(frame(&place, place_charge(BTC)), want);
}

#[test]
fn each_instruction_and_the_reduce_only_flag_are_written_as_paradex_names_them() {
    let sig = order_sig(Side::Sell, ParadexOrderType::Limit, "0.005", "62000.5");
    let params = |instruction: &str, flags: &str| {
        rpc_frame(
            "order.create",
            &format!(
                r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"SELL","signature_timestamp":1759622400123,"size":"0.005","type":"LIMIT","price":"62000.5","instruction":"{instruction}"{flags},"signature":{sig}}}"#,
                uuid(2)
            ),
        )
    };
    let sell = NewOrder {
        side: Side::Sell,
        ..order(2)
    };
    let cases = [
        (
            NewOrder {
                post_only: true,
                ..sell.clone()
            },
            params("POST_ONLY", ""),
        ),
        (
            NewOrder {
                tif: Tif::Ioc,
                reduce_only: true,
                ..sell.clone()
            },
            params("IOC", r#","flags":["REDUCE_ONLY"]"#),
        ),
        (
            NewOrder {
                reduce_only: true,
                ..sell.clone()
            },
            params("GTC", r#","flags":["REDUCE_ONLY"]"#),
        ),
        // An RPI order is RPI whether or not it also asks for post-only (0054).
        (
            NewOrder {
                channel: Channel::Rpi,
                ..sell.clone()
            },
            params("RPI", ""),
        ),
        (
            NewOrder {
                channel: Channel::Rpi,
                post_only: true,
                ..sell.clone()
            },
            params("RPI", ""),
        ),
    ];
    for (o, want) in cases {
        let place = VenueCommand::Place(o.clone());
        assert_eq!(frame(&place, place_charge(BTC)), want, "{o:?}");
    }
}

#[test]
fn a_market_order_is_written_and_signed_at_price_zero() {
    let sig = order_sig(Side::Buy, ParadexOrderType::Market, "0.005", "0");
    let want = rpc_frame(
        "order.create",
        &format!(
            r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"BUY","signature_timestamp":1759622400123,"size":"0.005","type":"MARKET","price":"0","instruction":"IOC","signature":{sig}}}"#,
            uuid(3)
        ),
    );
    let market = NewOrder {
        kind: OrderKind::Market,
        tif: Tif::Ioc,
        ..order(3)
    };
    assert_eq!(frame(&VenueCommand::Place(market), place_charge(BTC)), want);
}

#[test]
fn a_post_only_or_rpi_market_order_is_unsupported_before_signing() {
    // Post-only and RPI rest on the book; a market order never rests, so the pair is refused
    // alone and as a batch item, before any item is signed.
    let market = |seq: u64| NewOrder {
        kind: OrderKind::Market,
        ..order(seq)
    };
    let post_only = NewOrder {
        post_only: true,
        ..market(32)
    };
    let rpi = NewOrder {
        channel: Channel::Rpi,
        ..market(33)
    };
    for bad in [post_only, rpi] {
        refused(
            &VenueCommand::Place(bad.clone()),
            NotSentReason::Unsupported,
        );
        refused(
            &VenueCommand::PlaceBatch(vec![order(34), bad]),
            NotSentReason::Unsupported,
        );
    }
    // A market order at the default time in force is still written, as GTC.
    let sig = order_sig(Side::Buy, ParadexOrderType::Market, "0.005", "0");
    let want = rpc_frame(
        "order.create",
        &format!(
            r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"BUY","signature_timestamp":1759622400123,"size":"0.005","type":"MARKET","price":"0","instruction":"GTC","signature":{sig}}}"#,
            uuid(35)
        ),
    );
    assert_eq!(
        frame(&VenueCommand::Place(market(35)), place_charge(BTC)),
        want
    );
}

#[test]
fn a_batch_is_order_create_batch_with_each_item_signed_and_charged_its_item_count() {
    let item = |seq: u64, side: Side, size: &str| {
        let sig = order_sig(side, ParadexOrderType::Limit, size, "62000.5");
        let side = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };
        format!(
            r#"{{"client_id":"{}","market":"BTC-USD-PERP","side":"{side}","signature_timestamp":1759622400123,"size":"{size}","type":"LIMIT","price":"62000.5","instruction":"POST_ONLY","signature":{sig}}}"#,
            uuid(seq)
        )
    };
    let want = rpc_frame(
        "order.create_batch",
        &format!(
            r#"{{"orders":[{},{}]}}"#,
            item(4, Side::Buy, "0.005"),
            item(5, Side::Sell, "0.012")
        ),
    );
    let batch = VenueCommand::PlaceBatch(vec![
        NewOrder {
            post_only: true,
            ..order(4)
        },
        NewOrder {
            side: Side::Sell,
            qty: lots(12),
            post_only: true,
            ..order(5)
        },
    ]);
    let two = RateCharge {
        op: OpKind::Place,
        inst: Some(BTC),
        weight: 2.try_into().unwrap(),
    };
    assert_eq!(frame(&batch, two), want);
    // One sign stage per signed item.
    let out = encode(&batch);
    assert_eq!(out.sign_calls, 2);
    let sign = |edge| PathMark {
        stage: PathStage::Sign,
        edge,
    };
    let (start, end) = (sign(PathEdge::Start), sign(PathEdge::End));
    assert_eq!(out.marks, vec![start, end, start, end]);
    // A batch across markets names no single instrument.
    let VenueCommand::PlaceBatch(mut orders) = batch else {
        unreachable!()
    };
    orders[1].inst = ETH;
    let out = encode(&VenueCommand::PlaceBatch(orders));
    let Effect::Send { charge, .. } = &out.fx.as_slice()[0] else {
        panic!("not a frame")
    };
    assert_eq!(charge.inst, None);
}

#[test]
fn a_modify_is_order_modify_signed_over_the_venue_order_id_with_the_total_size() {
    let msg = OrderMessage {
        timestamp_ms: TS_MS,
        market: "BTC-USD-PERP",
        side: Side::Sell,
        order_type: ParadexOrderType::Limit,
        size: dec("0.008"),
        price: dec("62001"),
    };
    let sig = sig_json(signer().sign_modify(&msg, OID).unwrap());
    let want = rpc_frame(
        "order.modify",
        &format!(
            r#"{{"id":"{OID}","market":"BTC-USD-PERP","price":"62001","side":"SELL","signature":{sig},"signature_timestamp":1759622400123,"size":"0.008","type":"LIMIT"}}"#
        ),
    );
    let charge = RateCharge::one(OpKind::Amend, Some(BTC));
    // 3 of 8 filled: the size is the total, 8 (0054's AmendQty::TotalIncludingFilled).
    let by_both = VenueCommand::Amend(amend(OrderRef::Both(cid(6), vid(OID))));
    assert_eq!(frame(&by_both, charge), want);
    let by_vid = VenueCommand::Amend(amend(OrderRef::Venue(vid(OID))));
    assert_eq!(frame(&by_vid, charge), want);
    let out = encode(&by_vid);
    assert_eq!((out.sign_calls, out.marks.len()), (1, 2));
}

#[test]
fn cancels_are_unsigned_by_venue_id_or_by_client_id_with_the_market() {
    let charge = RateCharge::one(OpKind::Cancel, Some(BTC));
    let by_vid = VenueCommand::Cancel(cancel(OrderRef::Both(cid(7), vid(OID))));
    assert_eq!(
        frame(&by_vid, charge),
        rpc_frame("order.cancel", &format!(r#"{{"id":"{OID}"}}"#))
    );
    let by_cid = VenueCommand::Cancel(cancel(OrderRef::Client(cid(7))));
    assert_eq!(
        frame(&by_cid, charge),
        rpc_frame(
            "order.cancel",
            &format!(r#"{{"client_id":"{}","market":"BTC-USD-PERP"}}"#, uuid(7))
        )
    );
    for cmd in [by_vid, by_cid] {
        let out = encode(&cmd);
        assert_eq!((out.sign_calls, out.marks.len()), (0, 0), "unsigned");
    }
}

#[test]
fn a_batch_cancel_is_order_cancel_batch_by_venue_ids() {
    let many = VenueCommand::CancelMany(vec![
        cancel(OrderRef::Venue(vid(OID))),
        cancel(OrderRef::Both(cid(8), vid(OID2))),
    ]);
    let two = RateCharge {
        op: OpKind::Cancel,
        inst: Some(BTC),
        weight: 2.try_into().unwrap(),
    };
    assert_eq!(
        frame(&many, two),
        rpc_frame(
            "order.cancel_batch",
            &format!(r#"{{"order_ids":["{OID}","{OID2}"]}}"#)
        )
    );
    assert_eq!(encode(&many).sign_calls, 0);
}

#[test]
fn a_cancel_all_names_its_market_and_an_account_one_names_none() {
    let market = VenueCommand::CancelAll(CancelScope::Instrument(ETH));
    assert_eq!(
        frame(&market, RateCharge::one(OpKind::CancelAll, Some(ETH))),
        rpc_frame("order.cancel_all", r#"{"market":"ETH-USD-PERP"}"#)
    );
    let account = VenueCommand::CancelAll(CancelScope::Account);
    assert_eq!(
        frame(&account, RateCharge::one(OpKind::CancelAll, None)),
        rpc_frame("order.cancel_all", "{}")
    );
}

#[test]
fn cancel_on_disconnect_is_enabled_as_safety_and_disabled_as_normal_traffic() {
    let control = RateCharge::one(OpKind::Control, None);
    let on = VenueCommand::ArmCancelOnDisconnect(true);
    assert_eq!(on.traffic_class(), TrafficClass::Safety);
    assert_eq!(
        frame(&on, control),
        rpc_frame("order.cancel_on_disconnect", r#"{"enabled":true}"#)
    );
    let off = VenueCommand::ArmCancelOnDisconnect(false);
    assert_eq!(off.traffic_class(), TrafficClass::Normal);
    assert_eq!(
        frame(&off, control),
        rpc_frame("order.cancel_on_disconnect", r#"{"enabled":false}"#)
    );
}

#[test]
fn the_same_command_and_context_encode_byte_identically_twice() {
    let commands = [
        VenueCommand::Place(order(9)),
        VenueCommand::PlaceBatch(vec![order(10), order(11)]),
        VenueCommand::Amend(amend(OrderRef::Venue(vid(OID)))),
        VenueCommand::Cancel(cancel(OrderRef::Client(cid(12)))),
        VenueCommand::CancelMany(vec![cancel(OrderRef::Venue(vid(OID2)))]),
        VenueCommand::CancelAll(CancelScope::Instrument(BTC)),
        VenueCommand::ArmCancelOnDisconnect(true),
    ];
    for cmd in &commands {
        let (first, second) = (encode(cmd), encode(cmd));
        assert!(first.result.is_ok(), "{cmd:?}");
        assert_eq!(first.fx, second.fx, "{cmd:?}");
        // One encoder, twice: it keeps no state between encodes that changes the bytes.
        let mut enc = ParadexEncoder::new(Box::new(signer()), STREAM, TIMEOUT);
        let mut fx = Effects::new();
        for _ in 0..2 {
            enc.encode(
                cmd,
                RPC,
                &md::specs(),
                &ctx(),
                &mut PathStamps::off(),
                &mut fx,
            )
            .unwrap();
        }
        assert_eq!(
            fx.as_slice(),
            [first.fx.as_slice(), first.fx.as_slice()].concat()
        );
    }
}

#[test]
fn an_undeclared_time_in_force_reference_or_command_is_unsupported_with_no_effect() {
    let fok = NewOrder {
        tif: Tif::Fok,
        ..order(13)
    };
    refused(
        &VenueCommand::Place(fok.clone()),
        NotSentReason::Unsupported,
    );
    // One undeclared item refuses the whole batch before any item is signed.
    refused(
        &VenueCommand::PlaceBatch(vec![order(14), fok]),
        NotSentReason::Unsupported,
    );
    // order.modify names the venue id only.
    let by_cid = AmendOrder {
        target: OrderRef::Client(cid(15)),
        ..amend(OrderRef::Client(cid(15)))
    };
    refused(&VenueCommand::Amend(by_cid), NotSentReason::Unsupported);
    let fok_amend = AmendOrder {
        tif: Tif::Fok,
        ..amend(OrderRef::Venue(vid(OID)))
    };
    refused(&VenueCommand::Amend(fok_amend), NotSentReason::Unsupported);
    // order.cancel_batch takes venue ids only: a client-id item refuses the batch.
    refused(
        &VenueCommand::CancelMany(vec![
            cancel(OrderRef::Venue(vid(OID))),
            cancel(OrderRef::Client(cid(16))),
        ]),
        NotSentReason::Unsupported,
    );
    // Longer than the declared ten.
    refused(
        &VenueCommand::PlaceBatch((20..31).map(order).collect()),
        NotSentReason::Unsupported,
    );
    let eleven = (0..11).map(|_| cancel(OrderRef::Venue(vid(OID)))).collect();
    refused(
        &VenueCommand::CancelMany(eleven),
        NotSentReason::Unsupported,
    );
    // No dead-man timer and no fee query on the socket; the query is the codec's REST request.
    refused(&VenueCommand::RefreshDeadMan, NotSentReason::Unsupported);
    refused(&VenueCommand::FeeQuery, NotSentReason::Unsupported);
    let query = QueryOrder {
        target: OrderRef::Client(cid(17)),
        inst: BTC,
        placement_nonce: None,
    };
    refused(&VenueCommand::Query(query), NotSentReason::Unsupported);
}

#[test]
fn an_order_combining_features_the_caps_declare_in_conflict_is_a_flag_conflict() {
    let conflicts = [
        NewOrder {
            post_only: true,
            tif: Tif::Ioc,
            ..order(18)
        },
        NewOrder {
            channel: Channel::Rpi,
            tif: Tif::Ioc,
            ..order(18)
        },
        NewOrder {
            channel: Channel::Rpi,
            reduce_only: true,
            ..order(18)
        },
    ];
    for o in conflicts {
        refused(&VenueCommand::Place(o.clone()), NotSentReason::FlagConflict);
        refused(
            &VenueCommand::PlaceBatch(vec![order(19), o]),
            NotSentReason::FlagConflict,
        );
    }
    let amend = AmendOrder {
        tif: Tif::Ioc,
        ..amend(OrderRef::Venue(vid(OID)))
    };
    refused(&VenueCommand::Amend(amend), NotSentReason::FlagConflict);
}

#[test]
fn what_cannot_be_written_is_unencodable_with_no_effect() {
    let unknown = InstrumentId::new(99);
    refused(
        &VenueCommand::Place(NewOrder {
            inst: unknown,
            ..order(20)
        }),
        NotSentReason::Unencodable,
    );
    refused(
        &VenueCommand::Place(NewOrder {
            qty: lots(0),
            ..order(20)
        }),
        NotSentReason::Unencodable,
    );
    refused(
        &VenueCommand::Place(NewOrder {
            kind: OrderKind::Limit { px: Ticks(0) },
            ..order(20)
        }),
        NotSentReason::Unencodable,
    );
    refused(
        &VenueCommand::Place(NewOrder {
            kind: OrderKind::Limit { px: Ticks(-5) },
            ..order(20)
        }),
        NotSentReason::Unencodable,
    );
    refused(
        &VenueCommand::PlaceBatch(vec![]),
        NotSentReason::Unencodable,
    );
    refused(
        &VenueCommand::CancelMany(vec![]),
        NotSentReason::Unencodable,
    );
    // An amend to its filled quantity leaves nothing to rest: a cancel, not an amend.
    let to_filled = AmendOrder {
        qty: lots(3),
        ..amend(OrderRef::Venue(vid(OID)))
    };
    refused(&VenueCommand::Amend(to_filled), NotSentReason::Unencodable);
    let amend_unknown = AmendOrder {
        inst: unknown,
        ..amend(OrderRef::Venue(vid(OID)))
    };
    refused(
        &VenueCommand::Amend(amend_unknown),
        NotSentReason::Unencodable,
    );
    // A cancel by client id needs its market; one by venue id does not.
    let cancel_unknown = |target| CancelOrder {
        inst: unknown,
        ..cancel(target)
    };
    refused(
        &VenueCommand::Cancel(cancel_unknown(OrderRef::Client(cid(21)))),
        NotSentReason::Unencodable,
    );
    assert!(
        encode(&VenueCommand::Cancel(cancel_unknown(OrderRef::Venue(vid(
            OID
        )))))
        .result
        .is_ok()
    );
    refused(
        &VenueCommand::CancelAll(CancelScope::Instrument(unknown)),
        NotSentReason::Unencodable,
    );
    // Missing from an empty table.
    refused_with(
        &VenueCommand::Place(order(22)),
        &SpecTable::new(),
        NotSentReason::Unencodable,
    );
}

#[test]
fn a_wall_time_before_1970_or_a_failing_signer_sends_nothing() {
    let mut enc = ParadexEncoder::new(Box::new(signer()), STREAM, TIMEOUT);
    let early = EncodeCtx {
        wall: WallNs(-1),
        ..ctx()
    };
    let mut fx = Effects::new();
    for cmd in [
        VenueCommand::Place(order(23)),
        VenueCommand::Amend(amend(OrderRef::Venue(vid(OID)))),
    ] {
        let out = enc.encode(
            &cmd,
            RPC,
            &md::specs(),
            &early,
            &mut PathStamps::off(),
            &mut fx,
        );
        assert_eq!(out, Err(NotSentReason::Unencodable));
    }
    let mut failing = ParadexEncoder::new(Box::new(Failing), STREAM, TIMEOUT);
    let mut marks = Marks::default();
    for cmd in [
        VenueCommand::Place(order(24)),
        VenueCommand::PlaceBatch(vec![order(25), order(26)]),
        VenueCommand::Amend(amend(OrderRef::Venue(vid(OID)))),
    ] {
        let out = failing.encode(
            &cmd,
            RPC,
            &md::specs(),
            &ctx(),
            &mut PathStamps::new(&mut marks),
            &mut fx,
        );
        assert_eq!(out, Err(NotSentReason::SignFailed));
    }
    assert!(fx.is_empty());
    // The failed signer calls are still marked, start and end.
    assert_eq!(marks.0.len(), 6);
    // Unsigned requests need no signer.
    let cancel = VenueCommand::Cancel(cancel(OrderRef::Venue(vid(OID))));
    let out = failing.encode(
        &cancel,
        RPC,
        &md::specs(),
        &ctx(),
        &mut PathStamps::off(),
        &mut fx,
    );
    assert!(out.is_ok());
    assert!(format!("{failing:?}").starts_with("ParadexEncoder"));
}
