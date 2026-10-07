//! FBC-4zz (decisions 0004, 0014, 0022): Paradex's private `PositionEvent` (SBE template 22,
//! the `positions` channel) decoded into the account's position and `AccountEvent` (template
//! 23, the `account` channel) into its balance, through the core's `DecodeScope` only. A
//! position is signed lots (`side` BUY long, SELL short, `size` absolute) with the average entry
//! exactly as the frame states it, never rounded to the tick; a flat one has no average entry;
//! a balance is the account value and free collateral in the settlement asset the frame names.
//! A frame for an instrument missing from the spec table, shorter than its declared block, or
//! refused for any other reason pushes nothing.
//!
//! The frames are the hand-built ones in `fixtures/paradex/exec/` (SYNTHETIC: their account is
//! 32 made-up bytes).

mod md;

use std::fs;
use std::path::PathBuf;

use fbc_core::{
    AssetSym, DecodeError, ExchNs, ExchTsKind, ExecEvent, ExecSink, Money, PxExact, SignedLots,
    VenueMeta, dispatch,
};
use fbc_venue_paradex::exec::{
    TEMPLATE_ACCOUNT, TEMPLATE_POSITION, decode_account_event, decode_position_event,
};
use fbc_venue_paradex::factory::caps;
use md::BTC;

/// The engine namespace the order-entry dispatch is lent under.
const OWN: fbc_core::Namespace = fbc_core::Namespace::new(7);
/// The 8-byte message header every frame starts with.
const HEADER: usize = 8;

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

#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

type Decoded = (Result<(), DecodeError>, Vec<(VenueMeta, ExecEvent)>);

/// Decodes `bytes` as a `PositionEvent` through the core's dispatch under Paradex's
/// order-entry caps: what the decoder returned and every event it pushed.
fn decode_position(bytes: &[u8]) -> Decoded {
    let specs = md::specs();
    let mut sink = Sink::default();
    let result = dispatch(&caps(), OWN, |scope| {
        decode_position_event(bytes, scope, &specs, &mut sink)
    });
    (result, sink.0)
}

/// Decodes `bytes` as an `AccountEvent`, as [`decode_position`] does a position.
fn decode_account(bytes: &[u8]) -> Decoded {
    let mut sink = Sink::default();
    let result = dispatch(&caps(), OWN, |scope| {
        decode_account_event(bytes, scope, &mut sink)
    });
    (result, sink.0)
}

/// The one event `decoded` holds, asserting the decoder returned `Ok`.
fn one((result, events): Decoded) -> (VenueMeta, ExecEvent) {
    result.unwrap();
    match <[_; 1]>::try_from(events) {
        Ok([one]) => one,
        Err(events) => panic!("{} events: {events:?}", events.len()),
    }
}

/// The refusal `decoded` holds, asserting nothing was pushed.
fn refused((result, events): Decoded) -> DecodeError {
    assert!(events.is_empty(), "a refused frame pushed {events:?}");
    result.expect_err("the frame is refused")
}

fn malformed(decoded: Decoded) -> bool {
    matches!(refused(decoded), DecodeError::Malformed(_))
}

/// The meta of a frame published at `us` microseconds with sequence `seq`.
fn meta(us: i64, seq: u64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(us * 1_000)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq),
    }
}

/// The position event of `qty` lots of 0.001 at the average entry `mantissa × 10⁻⁸`.
fn position(qty: i64, avg_entry: Option<i64>) -> ExecEvent {
    ExecEvent::Position {
        inst: BTC,
        qty: SignedLots(qty),
        avg_entry: avg_entry.map(|m| PxExact::new(m, -8)),
    }
}

fn usdc(nanos: i128) -> Money {
    Money::new(nanos, AssetSym::new("USDC").unwrap())
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

/// `bytes` with its var data replaced by `vars`, each written as a `varString8`.
fn with_vars(bytes: &[u8], vars: &[&[u8]]) -> Vec<u8> {
    let block = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    let mut out = bytes[..HEADER + block].to_vec();
    for var in vars {
        out.push(u8::try_from(var.len()).unwrap());
        out.extend_from_slice(var);
    }
    out
}

#[test]
fn a_long_position_decodes_into_signed_lots_with_its_exact_average_entry() {
    let bytes = frame("position-long-v2.sbe.txt");
    assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]), TEMPLATE_POSITION);
    let (meta_got, event) = one(decode_position(&bytes));
    assert_eq!(meta_got, meta(1_759_500_000_407_001, 7001));
    // BUY: long. 0.15 in lots of 0.001; the average entry 61234.56789012 as sent, off the
    // instrument's 0.1 tick and not rounded to it.
    assert_eq!(event, position(150, Some(6_123_456_789_012)));
    let ExecEvent::Position {
        avg_entry: Some(avg),
        ..
    } = event
    else {
        panic!("not a position: {event:?}");
    };
    assert_eq!(avg.to_decimal().unwrap().to_string(), "61234.56789012");
    let specs = md::specs();
    let grid = &specs.by_symbol("BTC-USD-PERP").unwrap().price_grid;
    assert_eq!(grid.ticks_exact(avg), None, "the entry is off the grid");
    // The longer root block: 8 bytes past status, skipped.
    let (meta_got, longer) = one(decode_position(&frame("position-longer-block-v2.sbe.txt")));
    assert_eq!(meta_got, meta(1_759_500_000_407_004, 7004));
    assert_eq!(longer, position(150, Some(6_123_456_789_012)));
}

#[test]
fn a_short_position_at_version_1_decodes_into_negative_lots() {
    let (meta_got, event) = one(decode_position(&frame("position-short-v1.sbe.txt")));
    assert_eq!(meta_got, meta(1_759_500_000_407_002, 7002));
    // SELL: short. 0.05 at 62000.05.
    assert_eq!(event, position(-50, Some(6_200_005_000_000)));
    // The layout is the same at version 2, where markPrice may be null; it is not read.
    let v2 = with_i64(
        with_version(frame("position-short-v1.sbe.txt"), 2),
        49,
        i64::MIN,
    );
    assert_eq!(
        one(decode_position(&v2)).1,
        position(-50, Some(6_200_005_000_000))
    );
}

#[test]
fn a_flat_position_has_no_average_entry_whatever_its_side() {
    // CLOSED, size 0: flat, and the average entry the frame still states is not one.
    let closed = frame("position-closed-v2.sbe.txt");
    let (meta_got, event) = one(decode_position(&closed));
    assert_eq!(meta_got, meta(1_759_500_000_407_003, 7003));
    assert_eq!(event, position(0, None));
    // Flat on either side, on no side (NON_REPRESENTABLE), with a null average entry, and
    // while OPEN, UNSPECIFIED or NON_REPRESENTABLE.
    for side in [1, 2, 254] {
        let bytes = with_byte(closed.clone(), 16, side);
        assert_eq!(one(decode_position(&bytes)).1, position(0, None), "{side}");
    }
    let null_entry = with_i64(closed.clone(), 25, i64::MIN);
    assert_eq!(one(decode_position(&null_entry)).1, position(0, None));
    for status in [1, 3, 254] {
        let bytes = with_byte(closed.clone(), 153, status);
        assert_eq!(
            one(decode_position(&bytes)).1,
            position(0, None),
            "{status}"
        );
    }
    // An open position whose status is UNSPECIFIED or NON_REPRESENTABLE is still its size.
    let long = frame("position-long-v2.sbe.txt");
    for status in [3, 254] {
        let bytes = with_byte(long.clone(), 153, status);
        assert_eq!(
            one(decode_position(&bytes)).1,
            position(150, Some(6_123_456_789_012)),
            "{status}"
        );
    }
}

#[test]
fn a_position_for_an_instrument_missing_from_the_spec_table_is_refused_with_nothing_pushed() {
    let long = frame("position-long-v2.sbe.txt");
    let unknown = with_vars(&long, &[b"SOL-USD-PERP", b"8615262148007718001"]);
    assert_eq!(
        refused(decode_position(&unknown)),
        DecodeError::UnknownInstrument
    );
    // The same frame naming BTC-USD-PERP decodes: the market is what was refused.
    let known = with_vars(&long, &[b"BTC-USD-PERP", b"8615262148007718001"]);
    assert_eq!(
        one(decode_position(&known)).1,
        position(150, Some(6_123_456_789_012))
    );
}

#[test]
fn a_position_frame_the_decoder_cannot_read_whole_is_refused_with_nothing_pushed() {
    assert_eq!(
        refused(decode_position(&frame("position-short-block.sbe.txt"))),
        DecodeError::Malformed("SBE frame shorter than its declared block")
    );
    let long = frame("position-long-v2.sbe.txt");
    let bad = |bytes: Vec<u8>| malformed(decode_position(&bytes));
    // Shorter than the header; another template (an AccountEvent's id on this frame, and an
    // AccountEvent frame).
    assert!(bad(long[..5].to_vec()));
    assert!(bad(with_template(long.clone(), TEMPLATE_ACCOUNT)));
    assert!(bad(frame("account-v1.sbe.txt")));
    // A side outside the schema's values, and NON_REPRESENTABLE on a position that has a size.
    for side in [0, 3, 254] {
        assert!(bad(with_byte(long.clone(), 16, side)), "{side}");
    }
    // A status outside the schema's values, and CLOSED on a position that has a size.
    for status in [0, 4, 2] {
        assert!(bad(with_byte(long.clone(), 153, status)), "{status}");
    }
    // A null or negative seq; a null, negative or off-step size.
    assert!(bad(with_i64(long.clone(), 8, i64::MIN)));
    assert!(bad(with_i64(long.clone(), 8, -1)));
    assert!(bad(with_i64(long.clone(), 17, i64::MIN)));
    assert!(bad(with_i64(long.clone(), 17, -15_000_000)));
    assert!(bad(with_i64(long.clone(), 17, 15_000_001)));
    // An open position whose average entry is null, zero or negative.
    for entry in [i64::MIN, 0, -6_123_456_789_012] {
        assert!(bad(with_i64(long.clone(), 25, entry)), "{entry}");
    }
    // A block that ends before a field the decoder reads: status, the average entry, the
    // size, the side, the seq, the ts.
    for len in [153, 32, 24, 16, 15, 7] {
        assert!(bad(with_block(&long, len)), "{len}");
    }
    // The var data cut short: no market, a market running past the frame or not UTF-8.
    assert!(bad(with_vars(&long, &[])));
    assert!(bad(long[..HEADER + 154 + 5].to_vec()));
    assert!(bad(with_vars(&long, &[b"BTC-USD-PERP\xff"])));
}

#[test]
fn an_account_event_decodes_into_the_balance_in_its_settlement_asset() {
    let v1 = frame("account-v1.sbe.txt");
    assert_eq!(u16::from_le_bytes([v1[2], v1[3]]), TEMPLATE_ACCOUNT);
    let (meta_got, event) = one(decode_account(&v1));
    assert_eq!(meta_got, meta(1_759_500_000_508_001, 8001));
    // accountValue 612.34 ("Portfolio value including unrealized PnL") is the equity;
    // freeCollateral 550.25 ("Collateral available for new orders") what is available; both in
    // the settlement asset the frame names, USDC.
    assert_eq!(
        event,
        ExecEvent::Balance {
            equity: usdc(612_340_000_000),
            available: usdc(550_250_000_000),
        }
    );
    // Version 2 with lastSeenNotification (a 121-byte block), and with 8 more bytes: the
    // appended fields are skipped.
    let v2_balance = ExecEvent::Balance {
        equity: usdc(598_765_432_110),
        available: usdc(0),
    };
    let (meta_got, event) = one(decode_account(&frame("account-v2.sbe.txt")));
    assert_eq!(meta_got, meta(1_759_500_000_508_002, 8002));
    assert_eq!(event, v2_balance);
    let (meta_got, event) = one(decode_account(&frame("account-longer-block-v2.sbe.txt")));
    assert_eq!(meta_got, meta(1_759_500_000_508_003, 8003));
    assert_eq!(event, v2_balance);
    // Version 2 also describes the 113-byte layout without lastSeenNotification (the schema:
    // "the header version byte cannot tell them apart").
    let v2_short_layout = with_version(v1.clone(), 2);
    assert_eq!(
        one(decode_account(&v2_short_layout)).1,
        one(decode_account(&v1)).1
    );
    // A negative account value and the LIQUIDATION status are the venue's to state: the
    // balance is pushed as sent.
    let underwater = with_byte(with_i64(v1.clone(), 48, -325_000_000), 112, 2);
    assert_eq!(
        one(decode_account(&underwater)).1,
        ExecEvent::Balance {
            equity: usdc(-3_250_000_000),
            available: usdc(550_250_000_000),
        }
    );
    // Another settlement asset is the one named.
    let dime = with_vars(&v1, &[b"DIME"]);
    assert_eq!(
        one(decode_account(&dime)).1,
        ExecEvent::Balance {
            equity: Money::new(612_340_000_000, AssetSym::new("DIME").unwrap()),
            available: Money::new(550_250_000_000, AssetSym::new("DIME").unwrap()),
        }
    );
}

#[test]
fn an_account_frame_the_decoder_cannot_read_whole_is_refused_with_nothing_pushed() {
    assert_eq!(
        refused(decode_account(&frame("account-short-block.sbe.txt"))),
        DecodeError::Malformed("SBE frame shorter than its declared block")
    );
    let v1 = frame("account-v1.sbe.txt");
    let bad = |bytes: Vec<u8>| malformed(decode_account(&bytes));
    // Shorter than the header; another template.
    assert!(bad(v1[..5].to_vec()));
    assert!(bad(with_template(v1.clone(), TEMPLATE_POSITION)));
    assert!(bad(frame("position-long-v2.sbe.txt")));
    // A null or negative seq; a null free collateral or account value.
    assert!(bad(with_i64(v1.clone(), 8, i64::MIN)));
    assert!(bad(with_i64(v1.clone(), 8, -1)));
    assert!(bad(with_i64(v1.clone(), 24, i64::MIN)));
    assert!(bad(with_i64(v1.clone(), 48, i64::MIN)));
    // A block that ends before a field the decoder reads: the account value, the free
    // collateral, the seq, the ts.
    for len in [55, 31, 15, 7] {
        assert!(bad(with_block(&v1, len)), "{len}");
    }
    // The settlement asset missing, empty, running past the frame, not UTF-8, or no asset
    // symbol (longer than eight bytes).
    assert!(bad(with_vars(&v1, &[])));
    assert!(bad(with_vars(&v1, &[b""])));
    assert!(bad(v1[..v1.len() - 1].to_vec()));
    assert!(bad(with_vars(&v1, &[b"USD\xff"])));
    assert!(bad(with_vars(&v1, &[b"NOTANASSET"])));
}

/// `bytes` with its header's template id set to `template`.
fn with_template(mut bytes: Vec<u8>, template: u16) -> Vec<u8> {
    bytes[2..4].copy_from_slice(&template.to_le_bytes());
    bytes
}
