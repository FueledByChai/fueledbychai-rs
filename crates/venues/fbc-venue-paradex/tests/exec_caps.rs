//! FBC-olg (decisions 0003, 0015, 0054): Paradex's order entry declares `ExecCaps` with every
//! field, each value cited in `src/exec/caps.rs`, and the VenueCaps built with them decode a
//! fill's fee under Paradex's sign and our client id in Paradex's UUID format through the
//! core's `DecodeScope`. Batch cancels name venue ids only, amends name the venue id and send
//! the order's total size, and the order socket negotiates SBE schema 1:2 while market data
//! stays on 1:1. The decision record cites a Paradex page for every order method and lists
//! every value no Paradex document states, with the conservative value chosen.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckModel, AmendAck, AmendQty, AssetSym, CancelOnDisconnect, CancelOrder, Channel,
    ChosenRef, CidMatch, CidMint, ClientIdFormat, ClientOrderId, Feature, FillSource, InstrumentId,
    LimitScope, Money, Namespace, NamespaceLease, NonceScope, OpKind, OrderKindTag, OrderRef,
    OrderingKey, RateCharge, RefKind, Side, SnapshotSource, Support, TagSet, TifTag, VenueCaps,
    VenueFeeSign, Via, WallNs, dispatch,
};
use fbc_venue_paradex::exec::{self, ORDER_SBE_SCHEMA_VERSION};
use fbc_venue_paradex::factory::{caps, caps_with_order_entry};
use fbc_venue_paradex::md::sbe;

const OWN: Namespace = Namespace::new(7);
const ACCOUNT: AccountKey = AccountKey::new(3);
/// 2026-10-05T00:00:00Z in nanoseconds.
const START: WallNs = WallNs(1_759_622_400_000_000_000);
const BTC: InstrumentId = InstrumentId::new(1);

/// A directory of its own for the namespace lease the client ids are minted under.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-paradex-exec-caps-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        LockDir(dir)
    }
}

impl Drop for LockDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn mint(n: usize) -> Vec<ClientOrderId> {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, START);
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

/// A FillEvent `fee` as the schema carries it (`Value8`: an int64 mantissa at exponent -8),
/// in the nanos `DecodeScope::fee` takes.
fn value8_nanos(mantissa: i64) -> i128 {
    i128::from(mantissa) * 10
}

#[test]
fn paradex_exec_caps_build_venue_caps_that_decode_fees_and_client_ids_as_paradex_sends_them() {
    let caps: VenueCaps = caps_with_order_entry();
    let exec = caps.exec.as_ref().expect("Paradex declares order entry");
    assert_eq!(exec, &exec::exec_caps());
    // FillEvent.fee: "Fee charged (positive = paid, negative = rebate)".
    assert_eq!(exec.fills.fee_sign, VenueFeeSign::PositiveIsCost);
    assert_eq!(exec.order.client_id, ClientIdFormat::Uuid);
    let cids = mint(2);
    dispatch(&caps, OWN, |scope| {
        // A taker fee of 0.0003 USDC (mantissa 30_000) is a cost to us.
        let paid = scope.fee(value8_nanos(30_000), usdc()).unwrap();
        assert_eq!(paid.cost(), Money::new(300_000, usdc()));
        // A maker rebate, sent negative, costs us less than nothing.
        let rebate = scope.fee(value8_nanos(-12_000), usdc()).unwrap();
        assert_eq!(rebate.cost(), Money::new(-120_000, usdc()));
        for cid in &cids {
            // Our client id, as the codec writes it on order.create and Paradex echoes it in
            // OrderEvent.clientOrderId and FillEvent.clientOrderId.
            let wire = fbc_core::encode_cid(&exec.order.client_id, *cid).unwrap();
            // order.create's client_id is "max 64 chars"; a UUID is 36.
            assert_eq!(wire.len(), 36, "{wire}");
            assert_eq!(wire.matches('-').count(), 4, "{wire}");
            assert_eq!(scope.client_order_id(&wire), CidMatch::Ours(*cid));
        }
        // A random (version 4) UUID, as another client of the account would send, is not ours.
        let foreign = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        assert_eq!(scope.client_order_id(foreign), CidMatch::Unparseable);
    });
}

#[test]
fn batch_cancels_name_venue_ids_only_and_amends_send_the_total_size_by_venue_id() {
    let exec = exec::exec_caps();
    let order = &exec.order;
    // order.cancel takes the order id, or client_id with the market.
    assert_eq!(
        order.cancel_refs,
        TagSet::of(&[RefKind::Venue, RefKind::Client])
    );
    // order.cancel_batch takes order_ids only.
    let batch = order.batch_cancel.expect("Paradex cancels in batches");
    assert_eq!(batch.refs, TagSet::of(&[RefKind::Venue]));
    // An order not yet acknowledged has no venue id: a batch cancel cannot name it (0032), a
    // single cancel names it by client id.
    let cid = mint(1)[0];
    let unacked = CancelOrder {
        target: OrderRef::Client(cid),
        inst: BTC,
        side: Side::Buy,
        placement_nonce: None,
    };
    assert_eq!(unacked.reference(batch.refs), None);
    assert_eq!(
        unacked.reference(order.cancel_refs),
        Some(ChosenRef::Client(cid))
    );
    // order.modify names the order by id, and its size is the "new (or unchanged) size".
    let amend = order.amend.expect("Paradex amends");
    assert_eq!(amend.refs, TagSet::of(&[RefKind::Venue]));
    assert_eq!(amend.qty_semantics, AmendQty::TotalIncludingFilled);
    assert!(amend.price && amend.qty && !amend.flags);
    // The amend is final on the order event reporting it, and its queue priority is measured
    // in calibration, never assumed.
    assert_eq!(amend.ack, AmendAck::ReplacedEvent);
    assert_eq!(amend.keeps_priority, None);
    // "The modified order maintains its original order ID."
    assert!(amend.keeps_venue_id);
    // Undocumented, so declared conservatively (0054): no amend of a partly filled order, a
    // rejected amend leaves the original as its order event shows.
    assert!(!amend.when_partially_filled);
    assert!(amend.reject_keeps_original);
}

#[test]
fn every_order_and_fill_capability_is_the_value_the_record_states() {
    let exec = exec::exec_caps();
    let o = &exec.order;
    assert_eq!(
        o.kinds,
        TagSet::of(&[OrderKindTag::Limit, OrderKindTag::Market])
    );
    assert_eq!(o.tifs, TagSet::of(&[TifTag::Gtc, TifTag::Ioc]));
    assert_eq!(o.channels, TagSet::of(&[Channel::Public, Channel::Rpi]));
    assert!(o.post_only && o.reduce_only);
    assert_eq!(
        o.flag_conflicts,
        [
            (Feature::PostOnly, Feature::Ioc),
            (Feature::Ioc, Feature::Rpi),
            (Feature::Rpi, Feature::ReduceOnly),
        ]
    );
    assert_eq!(o.query_refs, TagSet::of(&[RefKind::Client]));
    assert!(!o.cancel_before_ack);
    assert!(!o.cancel_is_signed);
    assert_eq!(o.batch_place.map(|b| b.max_items), Some(10));
    assert_eq!(o.batch_cancel.map(|b| b.max_items), Some(10));
    assert_eq!(o.cancel_all_account, Support::Native);
    assert_eq!(o.cancel_all_instrument, Support::Native);
    assert_eq!(
        o.cancel_on_disconnect,
        CancelOnDisconnect::PerConnection {
            rearm_on_reconnect: true
        }
    );
    assert_eq!(
        o.ack,
        AckModel::TwoPhase {
            risk_reject_window: Duration::from_secs(5)
        }
    );
    assert!(o.cid_echoed_on_events && o.events_echo_flags);
    assert_eq!(o.nonce_scope, NonceScope::None);
    assert_eq!(o.ordering_key, OrderingKey::VenueSeq);
    assert_eq!(o.snapshot_source, SnapshotSource::Untrustworthy);
    assert_eq!(o.sign_cost_hint_us, 200);
    let f = &exec.fills;
    assert_eq!(f.source, FillSource::Native);
    assert!(f.liquidity_flag && f.realized_pnl && f.realized_funding);
    // FillEvent.feeCurrency exists only at schema version 2, which the order socket negotiates.
    assert!(f.fee_asset_reported && f.fill_id);
    assert!(!f.replays_fills_on_reconnect);
}

#[test]
fn order_entry_adds_its_rate_limits_and_counts_order_methods_against_the_ip_limit() {
    let md = caps();
    let full = caps_with_order_entry();
    // Order entry changes nothing about market data or matching.
    assert_eq!(md.exec, None);
    assert_eq!((&full.md, full.matching), (&md.md, md.matching));
    assert_eq!(full.readiness_ceiling, md.readiness_ceiling);
    let place = RateCharge::one(OpKind::Place, Some(BTC));
    let order_ops = [
        OpKind::Place,
        OpKind::Amend,
        OpKind::Cancel,
        OpKind::CancelAll,
    ];
    // "POST, DELETE, PUT /orders | 800 req/s OR 17250 req/m | Account", shared by the WebSocket
    // methods ("same Redis counters").
    let account: Vec<_> = full
        .limits
        .iter()
        .filter(|l| l.scope == LimitScope::Account && l.counts(&place, Via::Frame))
        .map(|l| (l.per, l.units))
        .collect();
    assert_eq!(
        account,
        [
            (Duration::from_secs(1), 800),
            (Duration::from_secs(60), 17_250)
        ]
    );
    for l in full.limits.iter().filter(|l| l.counts(&place, Via::Frame)) {
        assert!(order_ops.iter().all(|&op| l.ops.contains(op)), "{l:?}");
    }
    // One per-IP bucket of 1500 req/m counts REST, queries and, conservatively, order methods.
    let ip: Vec<_> = full
        .limits
        .iter()
        .filter(|l| l.scope == LimitScope::Ip && l.per == Duration::from_secs(60))
        .filter(|l| l.ops.contains(OpKind::Query))
        .collect();
    assert_eq!(ip.len(), 1);
    assert_eq!(ip[0].units, 1500);
    assert!(order_ops.iter().all(|&op| ip[0].ops.contains(op)));
    assert!(ip[0].ops.contains(OpKind::Rest));
    // Market data alone counts no order method against any limit.
    assert!(!md.limits.iter().any(|l| l.counts(&place, Via::Frame)));
    // Every market-data limit is still declared, the IP one widened.
    assert_eq!(full.limits.len(), md.limits.len() + 2);
}

#[test]
fn the_order_socket_negotiates_schema_1_2_and_market_data_stays_on_1_1() {
    assert_eq!(sbe::SCHEMA_ID, 1);
    assert_eq!(sbe::SCHEMA_VERSION, 1);
    assert_eq!(ORDER_SBE_SCHEMA_VERSION, 2);
}

/// The decision record FBC-olg added: the one whose title names Paradex's order entry.
fn record() -> String {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../docs/decisions");
    let mut found = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("paradex-s-order-entry-declares"))
        })
        .collect::<Vec<_>>();
    assert_eq!(found.len(), 1, "{found:?}");
    fs::read_to_string(found.pop().unwrap()).unwrap()
}

#[test]
fn the_record_cites_a_paradex_page_for_every_order_method_and_lists_each_undocumented_value() {
    let text = record();
    for (method, page) in [
        ("order.create", "order-create/order-create"),
        (
            "order.create_batch",
            "order-create-batch/order-create-batch",
        ),
        ("order.modify", "order-modify/order-modify"),
        ("order.cancel", "order-cancel/order-cancel"),
        (
            "order.cancel_batch",
            "order-cancel-batch/order-cancel-batch",
        ),
        ("order.cancel_all", "order-cancel-all/order-cancel-all"),
        (
            "order.cancel_on_disconnect",
            "order-cancel-on-disconnect/order-cancel-on-disconnect",
        ),
    ] {
        let url = format!("https://docs.paradex.trade/ws/web-socket-channels/{page}");
        let line = text
            .lines()
            .find(|l| l.contains(&format!("`{method}`")) && l.contains(&url));
        assert!(line.is_some(), "{method} is not cited with {url}");
    }
    // Each undocumented value is listed with the field it sets and the run that confirms it.
    let undocumented = text
        .split("### Values no Paradex document states")
        .nth(1)
        .expect("the record lists the undocumented values");
    for field in [
        "when_partially_filled",
        "reject_keeps_original",
        "cancel_before_ack",
        "replays_fills_on_reconnect",
        "batch_place",
        "batch_cancel",
        "cancel_on_disconnect",
        "risk_reject_window",
        "snapshot_source",
        "keeps_priority",
        "LimitScope::Ip",
        "RateCharge::weight",
        "qty_semantics",
    ] {
        assert!(
            undocumented.contains(&format!("`{field}`")),
            "{field} is not listed"
        );
    }
    assert!(undocumented.contains("FBC-8xr"));
}
