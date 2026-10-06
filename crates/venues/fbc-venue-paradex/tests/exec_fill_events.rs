//! FBC-uiy (decisions 0004, 0009, 0014, 0022): Paradex's private `FillEvent` (SBE template 21)
//! decoded into fills through the core's `DecodeScope` only, at schema versions 1 and 2 and with
//! a longer root block. Each fill carries the venue's fill id (keyed with its fill type, so
//! fills of different types sharing an id stay apart) and order id, its side, price, size and
//! liquidity, the fee under the declared sign (`PositiveIsCost`) in the asset the frame
//! names, the venue's realized P&L (a null one `None`) and realized funding, and our client id
//! as the frame states them; a position transfer's null `seq` is no venue sequence; a
//! venue-initiated fill (LIQUIDATION, UNWIND_TRANSFER, SETTLE_MARKET, BLOCK_TRADE) is pushed
//! with no client-id match; and a frame shorter than its declared block, or refused for any
//! other reason, pushes nothing.
//!
//! The frames are the hand-built ones in `fixtures/paradex/exec/` (SYNTHETIC: their account is
//! 32 made-up bytes).

mod md;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use fbc_core::{
    AccountKey, AssetSym, CidMatch, CidMint, ClientOrderId, DecodeError, ExchNs, ExchTsKind,
    ExecEvent, ExecSink, Fee, FillEvent, FillId, FillIdent, Liquidity3, Lots, Money, Namespace,
    NamespaceLease, Side, Ticks, VenueMeta, VenueOrderId, WallNs, dispatch,
};
use fbc_venue_paradex::exec::{TEMPLATE_FILL, decode_fill_event};
use fbc_venue_paradex::factory::caps_with_order_entry;
use md::BTC;

const OWN: Namespace = Namespace::new(7);
const ACCOUNT: AccountKey = AccountKey::new(3);
/// 2026-10-05T00:00:00Z in nanoseconds.
const START: WallNs = WallNs(1_759_622_400_000_000_000);
/// The 8-byte message header every frame starts with.
const HEADER: usize = 8;
/// 62000.0 on the specs' 0.1 tick.
const PX: Ticks = Ticks(620_000);
/// The test specs' settlement currency: what realized P&L and funding are in, and the fee of a
/// frame that names no fee currency.
fn usd() -> AssetSym {
    AssetSym::new("USD").unwrap()
}

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
            "fbc-paradex-fill-events-{}-{n}",
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
        decode_fill_event(bytes, scope, &specs, &mut sink)
    });
    (result, sink.0)
}

/// The one fill `bytes` decodes into, and what the venue said about it.
fn one_fill(bytes: &[u8]) -> (VenueMeta, FillEvent) {
    let (result, events) = decode(bytes);
    result.unwrap();
    match <[_; 1]>::try_from(events) {
        Ok([(meta, ExecEvent::Fill(fill))]) => (meta, fill),
        Ok([other]) => panic!("not a fill: {other:?}"),
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

fn fid(wire: &str) -> FillId {
    dispatch(&caps_with_order_entry(), OWN, |scope| scope.fill_id(wire)).unwrap()
}

/// The fee the venue's raw `nanos` of `asset` mean under Paradex's declared sign.
fn fee(nanos: i128, asset: &str) -> Fee {
    let asset = AssetSym::new(asset).unwrap();
    dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.fee(nanos, asset)
    })
    .unwrap()
}

fn usd_nanos(nanos: i128) -> Money {
    Money::new(nanos, usd())
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

/// The meta of a frame whose `seq` is `seq`: its `ts` (the generator's 1759500000300000 us
/// plus the seq), a publish time, and the seq as the venue sequence.
fn meta(seq: u64) -> VenueMeta {
    let us = 1_759_500_000_300_000 + i64::try_from(seq).unwrap();
    VenueMeta {
        exch_ts: Some(ExchNs(us * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq),
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

/// `bytes` with its header's version set to `version`.
fn with_version(mut bytes: Vec<u8>, version: u16) -> Vec<u8> {
    bytes[6..8].copy_from_slice(&version.to_le_bytes());
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

/// The var data of the version-2 taker fill, with `fee_ccy` appended when given.
fn taker_vars(fee_ccy: Option<&str>) -> Vec<&str> {
    let mut vars = vec![
        "8615262148007718002",
        "1759500000000000002",
        "01000700-199b-81ab-8200-00099b9bf518",
        "9615262148007718002",
        "BTC-USD-PERP",
    ];
    vars.extend(fee_ccy);
    vars
}

#[test]
fn a_version_1_maker_fill_decodes_with_our_client_id_a_rebate_and_no_realized_pnl() {
    let [first, _] = ours();
    let bytes = frame("fill-maker-v1.sbe.txt");
    assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]), TEMPLATE_FILL);
    let (meta_got, fill) = one_fill(&bytes);
    assert_eq!(meta_got, meta(6001));
    assert_eq!(
        fill,
        FillEvent {
            ident: FillIdent::Venue {
                fill: fid("FILL:8615262148007718001"),
                vid: Some(vid("1759500000000000001")),
                // FillEvent states no remaining size, so no cumulative quantity.
                cum_after: None,
            },
            cid: Some(CidMatch::Ours(first)),
            inst: BTC,
            side: Side::Buy,
            px: PX,
            // 0.05 in lots of 0.001.
            qty: lots(50),
            liquidity: Liquidity3::Maker,
            // fee -0.0031, "negative = rebate": a rebate, so a negative cost. Version 1 names
            // no fee currency: the instrument's settlement currency.
            fee: fee(-3_100_000, "USD"),
            // realizedPnl null: no position closed.
            realized_pnl: None,
            realized_funding: Some(usd_nanos(0)),
            replay: false,
        }
    );
    assert_eq!(fill.fee.cost(), Money::new(-3_100_000, usd()));
}

#[test]
fn a_version_2_taker_fill_carries_its_fee_in_the_named_asset_realized_pnl_and_funding() {
    let [_, second] = ours();
    let (meta_got, fill) = one_fill(&frame("fill-taker-close-v2.sbe.txt"));
    assert_eq!(meta_got, meta(6002));
    let expected = FillEvent {
        ident: FillIdent::Venue {
            fill: fid("FILL:8615262148007718002"),
            vid: Some(vid("1759500000000000002")),
            cum_after: None,
        },
        cid: Some(CidMatch::Ours(second)),
        inst: BTC,
        side: Side::Sell,
        px: PX,
        qty: lots(50),
        liquidity: Liquidity3::Taker,
        // fee 0.62 USDC, "positive = paid": a cost.
        fee: fee(620_000_000, "USDC"),
        // realizedPnl 3.5 and realizedFunding -0.12, both P&L as the venue states them.
        realized_pnl: Some(usd_nanos(3_500_000_000)),
        realized_funding: Some(usd_nanos(-120_000_000)),
        replay: false,
    };
    assert_eq!(fill, expected);
    assert_eq!(
        fill.fee.cost(),
        Money::new(620_000_000, AssetSym::new("USDC").unwrap())
    );
    // A null realizedFunding is none reported, as a null realizedPnl is.
    let unreported = with_i64(frame("fill-taker-close-v2.sbe.txt"), 99, i64::MIN);
    let (_, fill) = one_fill(&unreported);
    assert_eq!(fill.realized_funding, None);
    assert_eq!(fill.realized_pnl, Some(usd_nanos(3_500_000_000)));
    // The longer root block: 8 bytes past orderbookSeqNo, skipped.
    let (meta_got, longer) = one_fill(&frame("fill-longer-block-v2.sbe.txt"));
    assert_eq!(meta_got, meta(6004));
    assert_eq!(
        longer,
        FillEvent {
            ident: FillIdent::Venue {
                fill: fid("FILL:8615262148007718004"),
                vid: Some(vid("1759500000000000002")),
                cum_after: None,
            },
            ..expected
        }
    );
}

#[test]
fn an_rpi_fill_is_the_accounts_own_execution_and_its_fee_asset_is_the_one_named() {
    let [first, _] = ours();
    let (meta_got, fill) = one_fill(&frame("fill-rpi-v2.sbe.txt"));
    assert_eq!(meta_got, meta(6003));
    assert_eq!(fill.cid, Some(CidMatch::Ours(first)));
    assert_eq!(fill.liquidity, Liquidity3::Maker);
    // fee 0.015 DIME.
    assert_eq!(fill.fee, fee(15_000_000, "DIME"));
    assert_eq!(fill.realized_pnl, None);
}

#[test]
fn a_frame_naming_no_fee_currency_has_its_fee_in_the_settlement_currency() {
    let v2 = frame("fill-taker-close-v2.sbe.txt");
    // A version-2 frame with no feeCurrency after market, and one with an empty one.
    for vars in [taker_vars(None), taker_vars(Some(""))] {
        let (_, fill) = one_fill(&with_vars(&v2, &vars));
        assert_eq!(fill.fee, fee(620_000_000, "USD"), "{vars:?}");
    }
    // A version-1 frame stops after market (the schema: "a v0/v1 decoder stops after
    // market"), so a fee currency appended past it is not read.
    let v1 = frame("fill-maker-v1.sbe.txt");
    let mut appended = v1.clone();
    appended.extend_from_slice(&[4, b'D', b'I', b'M', b'E']);
    assert_eq!(one_fill(&appended).1.fee, fee(-3_100_000, "USD"));
    // A version-1 frame with a version-2 block (flags and orderbookSeqNo bytes) decodes the
    // same as version 1, its extra bytes skipped.
    let v2_block_at_v1 = with_version(v2.clone(), 1);
    assert_eq!(one_fill(&v2_block_at_v1).1.fee, fee(620_000_000, "USD"));
}

#[test]
fn a_liquidation_is_pushed_with_no_client_id_match() {
    // The frame carries one of our client ids; a venue-initiated fill does not match it.
    let liquidation = frame("fill-liquidation-v2.sbe.txt");
    let (meta_got, fill) = one_fill(&liquidation);
    assert_eq!(meta_got, meta(6005));
    assert_eq!(
        fill,
        FillEvent {
            ident: FillIdent::Venue {
                fill: fid("LIQUIDATION:8615262148007718005"),
                vid: Some(vid("1759500000000000009")),
                cum_after: None,
            },
            cid: None,
            inst: BTC,
            side: Side::Sell,
            // 61000.0, 0.15.
            px: Ticks(610_000),
            qty: lots(150),
            liquidity: Liquidity3::Taker,
            fee: fee(45_750_000, "USDC"),
            realized_pnl: Some(usd_nanos(-150_000_000_000)),
            realized_funding: Some(usd_nanos(-300_000_000)),
            replay: false,
        }
    );
    // SETTLE_MARKET and BLOCK_TRADE are venue-initiated too.
    for fill_type in [4, 6] {
        let other = with_byte(liquidation.clone(), 16, fill_type);
        assert_eq!(one_fill(&other).1.cid, None, "{fill_type}");
    }
    // The same frame as a FILL is the account's own execution: its client id is matched.
    let [first, _] = ours();
    let own = with_byte(liquidation, 16, 1);
    assert_eq!(one_fill(&own).1.cid, Some(CidMatch::Ours(first)));
}

#[test]
fn a_position_transfer_with_a_null_seq_decodes_with_no_venue_sequence() {
    let (meta_got, fill) = one_fill(&frame("fill-transfer-null-seq-v2.sbe.txt"));
    assert_eq!(
        meta_got,
        VenueMeta {
            exch_ts: Some(ExchNs(1_759_500_000_309_000 * 1_000)),
            exch_ts_kind: ExchTsKind::Publish,
            venue_seq: None,
        }
    );
    assert_eq!(
        fill,
        FillEvent {
            // No order id: the fill names no order.
            ident: FillIdent::Venue {
                fill: fid("UNWIND_TRANSFER:8615262148007718006"),
                vid: None,
                cum_after: None,
            },
            // UNWIND_TRANSFER: venue-initiated, no client-id match (and none sent).
            cid: None,
            inst: BTC,
            side: Side::Buy,
            px: PX,
            qty: lots(50),
            // NON_REPRESENTABLE: the venue does not say.
            liquidity: Liquidity3::Unknown,
            fee: fee(0, "USDC"),
            realized_pnl: Some(usd_nanos(-200_000_000)),
            realized_funding: Some(usd_nanos(0)),
            replay: false,
        }
    );
    // A null seq is no venue sequence at version 1 too.
    let v1 = with_i64(frame("fill-maker-v1.sbe.txt"), 8, i64::MIN);
    assert_eq!(one_fill(&v1).0.venue_seq, None);
}

#[test]
fn a_frame_shorter_than_its_declared_block_is_refused_with_nothing_pushed() {
    assert_eq!(
        refused(&frame("fill-short-block.sbe.txt")),
        DecodeError::Malformed("SBE frame shorter than its declared block")
    );
    // Shorter than the header.
    assert!(matches!(
        refused(&frame("fill-maker-v1.sbe.txt")[..5]),
        DecodeError::Malformed(_)
    ));
}

#[test]
fn a_frame_the_decoder_cannot_read_whole_is_refused_with_nothing_pushed() {
    let v1 = frame("fill-maker-v1.sbe.txt");
    let v2 = frame("fill-taker-close-v2.sbe.txt");
    let malformed = |bytes: Vec<u8>| matches!(refused(&bytes), DecodeError::Malformed(_));
    // Another template.
    let mut order = v1.clone();
    order[2] = 20;
    assert!(malformed(order));
    // Root fields outside the schema's values: a fill type (an unknown one, NON_REPRESENTABLE),
    // a side, a liquidity.
    for fill_type in [0, 7, 254] {
        assert!(
            malformed(with_byte(v1.clone(), 16, fill_type)),
            "{fill_type}"
        );
    }
    assert!(malformed(with_byte(v1.clone(), 17, 3)));
    assert!(malformed(with_byte(v1.clone(), 18, 0)));
    assert!(malformed(with_byte(v1.clone(), 18, 3)));
    // A negative seq, a null or off-grid price, a null or off-step size, a null fee.
    assert!(malformed(with_i64(v1.clone(), 8, -1)));
    assert!(malformed(with_i64(v1.clone(), 19, i64::MIN)));
    assert!(malformed(with_i64(v1.clone(), 19, 6_200_000_000_001)));
    assert!(malformed(with_i64(v1.clone(), 27, i64::MIN)));
    assert!(malformed(with_i64(v1.clone(), 27, 5_000_001)));
    assert!(malformed(with_i64(v1.clone(), 35, i64::MIN)));
    // A block that ends before a field the decoder reads: realizedFunding, realizedPnl, the
    // fee, the size, the price, the liquidity, the side, the fill type, the seq, the ts.
    for len in [106, 50, 42, 34, 26, 18, 17, 16, 15, 7] {
        assert!(malformed(with_block(&v1, len)), "{len}");
    }
    // The var data cut short: no fillId, a fillId running past the frame, no orderId, no
    // clientOrderId, no tradeId, no market.
    assert!(malformed(with_vars(&v1, &[])));
    assert!(malformed(v1[..HEADER + 107 + 5].to_vec()));
    let fill_id = "8615262148007718001";
    let oid = "1759500000000000001";
    assert!(malformed(with_vars(&v1, &[fill_id])));
    assert!(malformed(with_vars(&v1, &[fill_id, oid])));
    assert!(malformed(with_vars(&v1, &[fill_id, oid, ""])));
    assert!(malformed(with_vars(&v1, &[fill_id, oid, "", "t"])));
    // An empty fill id is refused by the scope.
    assert!(matches!(
        refused(&with_vars(&v1, &["", oid, "", "t", "BTC-USD-PERP"])),
        DecodeError::IdRefused(_)
    ));
    // A market missing from the spec table.
    assert_eq!(
        refused(&with_vars(&v1, &[fill_id, oid, "", "t", "SOL-USD-PERP"])),
        DecodeError::UnknownInstrument
    );
    // A fee currency running past the frame, not UTF-8, or no asset symbol (longer than eight
    // bytes).
    assert!(malformed(v2[..v2.len() - 2].to_vec()));
    let mut bad = v2.clone();
    let last = bad.len() - 1;
    bad[last] = 0xff;
    assert!(malformed(bad));
    assert!(malformed(with_vars(&v2, &taker_vars(Some("NOTANASSET")))));
}

#[test]
fn fills_of_different_types_sharing_a_fill_id_have_different_keys() {
    // Paradex: a fill id is "Unique string ID of fill per FillType" (AsyncAPI
    // ResponsesFillResult.id), so a FILL and a LIQUIDATION may share one. Keyed by the id alone,
    // the OMS ledger would take the second for a duplicate of the first and drop it.
    let liquidation = frame("fill-liquidation-v2.sbe.txt");
    let names = [
        (1, "FILL"),
        (2, "LIQUIDATION"),
        (3, "UNWIND_TRANSFER"),
        (4, "SETTLE_MARKET"),
        (5, "RPI"),
        (6, "BLOCK_TRADE"),
    ];
    let mut keys = Vec::new();
    for (fill_type, name) in names {
        let (_, fill) = one_fill(&with_byte(liquidation.clone(), 16, fill_type));
        // The key spells the fill type before the venue's id.
        assert_eq!(
            fill.ident,
            FillIdent::Venue {
                fill: fid(&format!("{name}:8615262148007718005")),
                vid: Some(vid("1759500000000000009")),
                cum_after: None,
            },
            "{name}"
        );
        keys.push(fill.key());
    }
    for (i, a) in keys.iter().enumerate() {
        for b in &keys[i + 1..] {
            assert_ne!(a, b);
        }
    }
    // The same frame decoded twice: the same key, so a re-sent fill is still a duplicate.
    assert_eq!(
        one_fill(&liquidation).1.key(),
        one_fill(&liquidation).1.key()
    );
    // The longest fill id the core takes, with the longest prefix, still fits; one byte more
    // is refused rather than cut.
    let v1 = frame("fill-maker-v1.sbe.txt");
    let transfer = with_byte(v1, 16, 3);
    let oid = "1759500000000000001";
    let room = fbc_core::MAX_VENUE_ID_LEN - "UNWIND_TRANSFER:".len();
    let longest = "9".repeat(room);
    let (_, fill) = one_fill(&with_vars(
        &transfer,
        &[&longest, oid, "", "t", "BTC-USD-PERP"],
    ));
    assert_eq!(
        fill.ident,
        FillIdent::Venue {
            fill: fid(&format!("UNWIND_TRANSFER:{longest}")),
            vid: Some(vid(oid)),
            cum_after: None,
        }
    );
    let too_long = "9".repeat(room + 1);
    assert!(matches!(
        refused(&with_vars(
            &transfer,
            &[&too_long, oid, "", "t", "BTC-USD-PERP"]
        )),
        DecodeError::IdRefused(_)
    ));
}
