//! FBC-aml (decisions 0004, 0014, 0022, 0054): Paradex's private `OrderEvent` (SBE template
//! 20) decoded into order updates through the core's `DecodeScope` only, at schema versions 1
//! and 2 and with a longer root block. Each update carries the cumulative filled quantity
//! (`size` less `sizeOpen`), the venue sequence, the echoed instruction and flags and our client
//! id as the frame states them; a modify's request_info reaches the lattice as 0054 decides (the
//! amend final on SUCCESS for MODIFY_ORDER, a REJECTED one an asynchronous reject of the amend
//! beside the order as the event shows it, PENDING confirming nothing); and a frame shorter
//! than its declared block, or refused for any other reason, pushes nothing.
//!
//! The frames are the hand-built ones in `fixtures/paradex/exec/` (SYNTHETIC: their account is
//! 32 made-up bytes).

mod md;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use fbc_core::{
    AccountKey, CancelReason, CidMatch, CidMint, ClientOrderId, DecodeError, ExchNs, ExchTsKind,
    ExecEvent, ExecSink, Lots, Namespace, NamespaceLease, OpKind, OrderRef, OrderUpdate, Reject,
    RejectKind, Side, Ticks, VenueMeta, VenueOrderId, VenueOrderState, WallNs, dispatch,
};
use fbc_venue_paradex::exec::{TEMPLATE_ORDER, decode_order_event};
use fbc_venue_paradex::factory::caps_with_order_entry;
use md::BTC;

const OWN: Namespace = Namespace::new(7);
const ACCOUNT: AccountKey = AccountKey::new(3);
/// 2026-10-05T00:00:00Z in nanoseconds.
const START: WallNs = WallNs(1_759_622_400_000_000_000);
/// The 8-byte message header every frame starts with.
const HEADER: usize = 8;
/// The order id of every frame but the foreign and no-cid ones.
const OID: &str = "1759500000000000001";
/// 62000.0 on the specs' 0.1 tick.
const PX: Ticks = Ticks(620_000);
/// 61999.5, the price the modify SUCCESS frames amend to.
const PX_AMENDED: Ticks = Ticks(619_995);

/// The bytes of exec fixture `name`: whitespace-separated hex bytes, `#` to the end of a line
/// a comment.
fn frame(name: &str) -> Vec<u8> {
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

/// A directory of its own for the namespace lease the client ids are minted under.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "fbc-paradex-order-events-{}-{n}",
            std::process::id()
        ));
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

/// The first two client ids minted in [`OWN`]: the ones the fixtures carry.
fn ours() -> [ClientOrderId; 2] {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, START);
    [mint.mint().unwrap(), mint.mint().unwrap()]
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// Decodes `bytes` through the core's dispatch under Paradex's order-entry caps: what the
/// decoder returned and every event it pushed.
fn decode(bytes: &[u8]) -> (Result<(), DecodeError>, Vec<(VenueMeta, ExecEvent)>) {
    let caps = caps_with_order_entry();
    let specs = md::specs();
    let mut sink = Sink::default();
    let result = dispatch(&caps, OWN, |scope| {
        decode_order_event(bytes, scope, &specs, &mut sink)
    });
    (result, sink.0)
}

/// The one order update `bytes` decodes into, and what the venue said about it.
fn one_update(bytes: &[u8]) -> (VenueMeta, OrderUpdate) {
    let (result, events) = decode(bytes);
    result.unwrap();
    match <[_; 1]>::try_from(events) {
        Ok([(meta, ExecEvent::Order(update))]) => (meta, update),
        Ok([other]) => panic!("not an order update: {other:?}"),
        Err(events) => panic!("{} events: {events:?}", events.len()),
    }
}

/// The refusal `bytes` decode into, asserting nothing was pushed.
fn refused(bytes: &[u8]) -> DecodeError {
    let (result, events) = decode(bytes);
    assert!(events.is_empty(), "a refused frame pushed {events:?}");
    result.expect_err("the frame is refused")
}

fn vid(wire: &str) -> VenueOrderId {
    dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.venue_order_id(wire)
    })
    .unwrap()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// The meta of a frame whose `seq` is `seq`: its `ts` (the generator's 1759500000200000 us plus
/// the seq), a publish time, and the seq as the venue sequence.
fn meta(seq: u64) -> VenueMeta {
    let us = 1_759_500_000_200_000 + i64::try_from(seq).unwrap();
    VenueMeta {
        exch_ts: Some(ExchNs(us * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq),
    }
}

/// The order update the open and amended fixtures describe, with the given state, price and
/// total, nothing filled.
fn resting(cid: ClientOrderId, state: VenueOrderState, px: Ticks, qty: i64) -> OrderUpdate {
    OrderUpdate {
        cid: Some(CidMatch::Ours(cid)),
        vid: Some(vid(OID)),
        inst: BTC,
        side: Side::Buy,
        state,
        cum_filled: Lots::ZERO,
        px: Some(px),
        qty: Some(lots(qty)),
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

/// `bytes` with the root block's byte at `offset` set to `value`.
fn with_byte(mut bytes: Vec<u8>, offset: usize, value: u8) -> Vec<u8> {
    bytes[HEADER + offset] = value;
    bytes
}

/// `bytes` with the root block's int64 at `offset` set to `value`.
fn with_i64(mut bytes: Vec<u8>, offset: usize, value: i64) -> Vec<u8> {
    bytes[HEADER + offset..HEADER + offset + 8].copy_from_slice(&value.to_le_bytes());
    bytes
}

/// `bytes` with a root block of `len` bytes, its header saying so: the block's first `len`
/// bytes, then the var data as it was.
fn with_block(bytes: &[u8], len: u16) -> Vec<u8> {
    let block = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    let mut out = len.to_le_bytes().to_vec();
    out.extend_from_slice(&bytes[2..HEADER + usize::from(len)]);
    out.extend_from_slice(&bytes[HEADER + block..]);
    out
}

/// `bytes` (a frame whose header states its block length) with its var data replaced by
/// `vars`, each written as a `varString8`.
fn with_vars(bytes: &[u8], vars: &[&str]) -> Vec<u8> {
    let block = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    let mut out = bytes[..HEADER + block].to_vec();
    for var in vars {
        out.push(u8::try_from(var.len()).unwrap());
        out.extend_from_slice(var.as_bytes());
    }
    out
}

#[test]
fn a_version_1_new_order_decodes_as_open_with_our_client_id_its_flags_and_venue_sequence() {
    let [first, _] = ours();
    let bytes = frame("order-new-v1.sbe.txt");
    assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]), TEMPLATE_ORDER);
    let (meta_got, update) = one_update(&bytes);
    assert_eq!(meta_got, meta(5001));
    // NEW: accepted (provisionally, AckModel::TwoPhase), so it is reported resting.
    assert_eq!(update, resting(first, VenueOrderState::Open, PX, 150));
}

#[test]
fn cumulative_filled_is_size_less_size_open_and_reduce_only_is_read_from_the_flags() {
    let [_, second] = ours();
    let (meta_got, update) = one_update(&frame("order-open-partial-v1.sbe.txt"));
    assert_eq!(meta_got.venue_seq, Some(5002));
    assert_eq!(
        update,
        OrderUpdate {
            cid: Some(CidMatch::Ours(second)),
            vid: Some(vid("1759500000000000002")),
            inst: BTC,
            side: Side::Sell,
            state: VenueOrderState::Open,
            // size 0.15, sizeOpen 0.10: 0.05 filled, 50 lots of 0.001.
            cum_filled: lots(50),
            px: Some(PX),
            qty: Some(lots(150)),
            // GTC, flags REDUCE_ONLY.
            post_only: Some(false),
            reduce_only: Some(true),
        }
    );
}

#[test]
fn an_rpi_order_is_echoed_post_only_as_the_venue_rule_makes_it() {
    let (_, update) = one_update(&frame("order-rpi-v1.sbe.txt"));
    assert_eq!(update.post_only, Some(true));
    assert_eq!(update.reduce_only, Some(false));
}

#[test]
fn a_client_id_not_ours_is_unparseable_and_an_empty_one_is_none() {
    let (_, foreign) = one_update(&frame("order-foreign-cid-v1.sbe.txt"));
    assert_eq!(foreign.cid, Some(CidMatch::Unparseable));
    assert_eq!(foreign.vid, Some(vid("1759500000000000004")));
    let (_, none) = one_update(&frame("order-no-cid-v1.sbe.txt"));
    assert_eq!(none.cid, None);
    assert_eq!(none.vid, Some(vid("1759500000000000005")));
}

#[test]
fn a_market_order_has_no_price_and_closes_filled() {
    let (meta_got, update) = one_update(&frame("order-market-ioc-v1.sbe.txt"));
    assert_eq!(meta_got.venue_seq, Some(5006));
    assert_eq!(update.state, VenueOrderState::Filled);
    assert_eq!(update.px, None);
    assert_eq!(update.cum_filled, lots(150));
    assert_eq!(update.qty, Some(lots(150)));
    assert_eq!(update.post_only, Some(false));
    // A market order whose price is sent as 0 (the JSON feed's "0 for MARKET orders") has
    // none either.
    let zero = with_i64(frame("order-market-ioc-v1.sbe.txt"), 20, 0);
    assert_eq!(one_update(&zero).1.px, None);
    // A market order stating any other price contradicts that rule: refused, not passed on as
    // an order update a limit order's would look the same as (DeepSeek DS-1 on PR #85).
    let priced = with_i64(frame("order-market-ioc-v1.sbe.txt"), 20, 6_200_000_000_000);
    assert_eq!(
        refused(&priced),
        DecodeError::Malformed("market order with a price")
    );
}

#[test]
fn closed_orders_end_filled_or_cancelled_by_their_cancel_reason() {
    let [first, _] = ours();
    let closed = |name: &str| one_update(&frame(name)).1;
    let filled = closed("order-closed-filled-v2.sbe.txt");
    assert_eq!(filled.state, VenueOrderState::Filled);
    assert_eq!(filled.cum_filled, lots(150));
    assert_eq!(filled.cid, Some(CidMatch::Ours(first)));
    let user = closed("order-closed-canceled-v2.sbe.txt");
    assert_eq!(
        user.state,
        VenueOrderState::Canceled(CancelReason::Requested)
    );
    assert_eq!(user.cum_filled, lots(50));
    let post_only = closed("order-closed-post-only-v2.sbe.txt");
    assert_eq!(
        post_only.state,
        VenueOrderState::Canceled(CancelReason::PostOnly)
    );
    assert_eq!(post_only.cum_filled, Lots::ZERO);
    // A reason with no CancelReason of its own is the venue's.
    let margin = closed("order-closed-margin-v2.sbe.txt");
    assert_eq!(margin.state, VenueOrderState::Canceled(CancelReason::Venue));
    // Closed with part of it open and no reason stated: the venue ended it.
    let unstated = with_i64(frame("order-closed-filled-v2.sbe.txt"), 44, 5_000_000);
    let unstated = one_update(&unstated).1;
    assert_eq!(
        unstated.state,
        VenueOrderState::Canceled(CancelReason::Venue)
    );
    assert_eq!(unstated.cum_filled, lots(100));
}

#[test]
fn a_version_2_frame_without_request_info_and_a_version_1_longer_block_decode_without_it() {
    let [first, _] = ours();
    // The 1:2 layout without request_info: a 126-byte block and no requestId or
    // requestMessage after cancelReason (absent appended var data is missing).
    let (meta_got, update) = one_update(&frame("order-v2-without-request-info.sbe.txt"));
    assert_eq!(meta_got, meta(5014));
    assert_eq!(update, resting(first, VenueOrderState::Open, PX, 150));
    // A version-1 frame has no request_info, whatever bytes a longer block carries where
    // version 2 puts it: the fixture's are a modify SUCCESS for MODIFY_ORDER (4, 1), which at
    // version 2 would be an amend, so only the version gate keeps this an order update.
    let longer = frame("order-v1-longer-block.sbe.txt");
    assert_eq!((longer[HEADER + 126], longer[HEADER + 127]), (4, 1));
    let (meta_got, update) = one_update(&longer);
    assert_eq!(meta_got, meta(5007));
    assert_eq!(update, resting(first, VenueOrderState::Open, PX, 150));
    // Nor is a REJECTED for MODIFY_ORDER (3, 1) there an asynchronous reject.
    let rejected = with_byte(longer, 126, 3);
    let (meta_got, update) = one_update(&rejected);
    assert_eq!(meta_got, meta(5007));
    assert_eq!(update, resting(first, VenueOrderState::Open, PX, 150));
}

#[test]
fn a_pending_or_processed_modify_confirms_nothing() {
    let [first, _] = ours();
    let pending = frame("order-modify-pending-v2.sbe.txt");
    let (meta_got, update) = one_update(&pending);
    assert_eq!(meta_got, meta(5020));
    assert_eq!(update, resting(first, VenueOrderState::Open, PX, 150));
    // PROCESSED (2) confirms nothing either, nor does an unknown request status.
    for status in [2, 0, 254, 9] {
        let other = with_byte(pending.clone(), 126, status);
        assert_eq!(
            one_update(&other).1.state,
            VenueOrderState::Open,
            "{status}"
        );
    }
}

#[test]
fn a_modify_success_is_the_amend_final_at_the_events_price_and_size_under_the_same_venue_id() {
    let [first, _] = ours();
    for (name, seq) in [
        ("order-modify-success-v2.sbe.txt", 5021),
        // The longer root block: 8 bytes past requestType, skipped.
        ("order-modify-success-longer-block.sbe.txt", 5023),
    ] {
        let (meta_got, update) = one_update(&frame(name));
        assert_eq!(meta_got, meta(seq), "{name}");
        // keeps_venue_id: the amended order keeps its id, so no new one.
        let amended = VenueOrderState::Amended { new_vid: None };
        assert_eq!(update, resting(first, amended, PX_AMENDED, 200), "{name}");
    }
    // SUCCESS for a request that is not MODIFY_ORDER confirms no amend.
    let unspecified = with_byte(frame("order-modify-success-v2.sbe.txt"), 127, 0);
    assert_eq!(one_update(&unspecified).1.state, VenueOrderState::Open);
    // An order the same event shows closed is ended, whatever its request says.
    let closed = with_byte(frame("order-modify-success-v2.sbe.txt"), 16, 4);
    let closed = with_i64(closed, 44, 0);
    assert_eq!(one_update(&closed).1.state, VenueOrderState::Filled);
}

#[test]
fn a_modify_rejected_is_an_async_reject_of_the_amend_and_the_order_as_the_event_shows_it() {
    let [first, _] = ours();
    let (result, events) = decode(&frame("order-modify-rejected-v2.sbe.txt"));
    result.unwrap();
    let reject = ExecEvent::AsyncReject {
        target: OrderRef::Both(first, vid(OID)),
        op: OpKind::Amend,
        reject: Reject {
            kind: RejectKind::Other,
            venue_code: None,
            raw: "synthetic rejection message".into(),
        },
    };
    // The original order, at its original price and size, still rests
    // (reject_keeps_original).
    let order = ExecEvent::Order(resting(first, VenueOrderState::Open, PX, 150));
    assert_eq!(events, vec![(meta(5022), reject), (meta(5022), order)]);
    // REJECTED for a request that is not MODIFY_ORDER rejects no amend.
    let unspecified = with_byte(frame("order-modify-rejected-v2.sbe.txt"), 127, 0);
    assert_eq!(one_update(&unspecified).1.state, VenueOrderState::Open);
    // A rejected modify of an order not ours names it by its venue id alone.
    let foreign = with_vars(
        &frame("order-modify-rejected-v2.sbe.txt"),
        &[OID, "", "BTC-USD-PERP", "", "req-7002", ""],
    );
    let (result, events) = decode(&foreign);
    result.unwrap();
    match &events[..] {
        [
            (_, ExecEvent::AsyncReject { target, reject, .. }),
            (_, ExecEvent::Order(u)),
        ] => {
            assert_eq!(target, &OrderRef::Venue(vid(OID)));
            assert_eq!(&*reject.raw, "");
            assert_eq!(u.cid, None);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_frame_shorter_than_its_declared_block_is_refused_with_nothing_pushed() {
    assert_eq!(
        refused(&frame("order-short-block.sbe.txt")),
        DecodeError::Malformed("SBE frame shorter than its declared block")
    );
    // Shorter than the header.
    assert!(matches!(
        refused(&frame("order-new-v1.sbe.txt")[..5]),
        DecodeError::Malformed(_)
    ));
}

#[test]
fn a_frame_the_decoder_cannot_read_whole_is_refused_with_nothing_pushed() {
    let v1 = frame("order-new-v1.sbe.txt");
    let rejected = frame("order-modify-rejected-v2.sbe.txt");
    let malformed = |bytes: Vec<u8>| matches!(refused(&bytes), DecodeError::Malformed(_));
    // Another template.
    let mut fill = v1.clone();
    fill[2] = 21;
    assert!(malformed(fill));
    // Root fields outside the schema's values, or not modelled: a side, a status
    // (UNTRIGGERED, and an unknown one), an order type (STOP_LIMIT), a time in force.
    assert!(malformed(with_byte(v1.clone(), 17, 3)));
    assert!(malformed(with_byte(v1.clone(), 16, 2)));
    assert!(malformed(with_byte(v1.clone(), 16, 9)));
    assert!(malformed(with_byte(v1.clone(), 18, 3)));
    assert!(malformed(with_byte(v1.clone(), 19, 9)));
    // A negative seq, a null or off-step size, more open than the size, a limit order with no
    // price, a price off the grid.
    assert!(malformed(with_i64(v1.clone(), 8, -1)));
    assert!(malformed(with_i64(v1.clone(), 36, i64::MIN)));
    assert!(malformed(with_i64(v1.clone(), 36, 15_000_001)));
    assert!(malformed(with_i64(v1.clone(), 44, 16_000_000)));
    assert!(malformed(with_i64(v1.clone(), 20, i64::MIN)));
    assert!(malformed(with_i64(v1.clone(), 20, 6_200_000_000_001)));
    // A block that ends before a field the decoder needs: the flags, the sizes, the price, the
    // instruction, the side.
    for len in [125, 40, 24, 19, 17] {
        assert!(malformed(with_block(&v1, len)), "{len}");
    }
    // The var data cut short: no orderId, an orderId running past the frame, no market, no
    // cancelReason.
    assert!(malformed(with_vars(&v1, &[])));
    let block_end = HEADER + 126;
    assert!(malformed(v1[..block_end + 5].to_vec()));
    assert!(malformed(with_vars(&v1, &[OID, ""])));
    assert!(malformed(with_vars(&v1, &[OID, "", "BTC-USD-PERP"])));
    // An empty order id is refused by the scope.
    assert!(matches!(
        refused(&with_vars(&v1, &["", "", "BTC-USD-PERP", ""])),
        DecodeError::IdRefused(_)
    ));
    // A market missing from the spec table.
    assert_eq!(
        refused(&with_vars(&v1, &[OID, "", "SOL-USD-PERP", ""])),
        DecodeError::UnknownInstrument
    );
    // A version-2 REJECTED frame whose requestMessage runs past the frame pushes neither the
    // reject nor the order.
    assert!(malformed(rejected[..rejected.len() - 3].to_vec()));
    // Request data that is not UTF-8.
    let mut bad = rejected.clone();
    let last = bad.len() - 1;
    bad[last] = 0xff;
    assert!(malformed(bad));
}
