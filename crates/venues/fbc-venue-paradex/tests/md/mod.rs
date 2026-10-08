//! Shared by the market-data tests: the hand-built SBE fixtures in `fixtures/paradex/md/`, a
//! spec table, and a decode through the core's market-data dispatch.

#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

use fbc_core::{
    AssetSym, DecodeError, Effects, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec,
    Lots, MdCodec, MdEvent, MdSink, PriceGrid, RawFrame, SizeStep, SpecTable, StreamId,
    TradingStatus, UnderlyingId, VenueId, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_paradex::factory::caps;
use fbc_venue_paradex::md::ParadexMd;
use rust_decimal::Decimal;
use serde_json::Value;

pub const BTC: InstrumentId = InstrumentId::new(1);
pub const ETH: InstrumentId = InstrumentId::new(2);

/// The bytes of fixture `name`: whitespace-separated hex bytes, `#` to the end of a line a
/// comment.
pub fn frame(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/md")
        .join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .map(|line| line.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect()
}

/// The bytes of binary fixture `name`, as captured.
pub fn raw(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/md")
        .join(name);
    fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A perpetual spelled `symbol` on a 0.1 price tick and a 0.001 size step.
pub fn spec(id: InstrumentId, symbol: &str) -> InstrumentSpec {
    let venue_symbol = dispatch_market_data(&caps(), |scope| scope.venue_symbol(symbol)).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id,
        venue: VenueId::new(1),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(Decimal::new(1, 1)).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(Decimal::new(1, 3)).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// BTC-USD-PERP and ETH-USD-PERP.
pub fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(spec(BTC, "BTC-USD-PERP"));
    table.insert(spec(ETH, "ETH-USD-PERP"));
    table
}

/// Every event a codec pushed, with what the venue said about it.
#[derive(Default)]
pub struct Collect(pub Vec<(VenueMeta, MdEvent)>);

impl MdSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

/// What one frame decoded to: the result, the events pushed and the effects asked for.
pub struct Decoded {
    pub result: Result<(), DecodeError>,
    pub events: Vec<(VenueMeta, MdEvent)>,
    pub fx: Effects,
}

/// Decodes `frame` through `codec` inside the core's market-data dispatch.
pub fn decode_with(codec: &mut ParadexMd, frame: RawFrame<'_>) -> Decoded {
    let (specs, mut sink, mut fx) = (specs(), Collect::default(), Effects::new());
    let result = dispatch_market_data(&caps(), |scope| {
        codec.on_frame(frame, scope, &specs, &mut sink, &mut fx)
    });
    Decoded {
        result,
        events: sink.0,
        fx,
    }
}

/// Decodes binary `bytes` through a fresh codec.
pub fn decode(bytes: &[u8]) -> Decoded {
    decode_with(&mut ParadexMd::new(StreamId(0)), RawFrame::Binary(bytes))
}

/// Asserts `bytes` are refused with `err`, with nothing pushed and nothing asked for.
pub fn refused(bytes: &[u8], err: DecodeError) {
    let out = decode(bytes);
    assert_eq!(out.result, Err(err));
    assert!(out.events.is_empty() && out.fx.is_empty());
}

/// A JSON-RPC error answering request `id`, in the shape docs.paradex.trade shows for the
/// WebSocket API ("Invalid parameters").
pub fn rpc_error(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","error":{{"code":-32602,"message":"Invalid parameters"}},"usIn":1,"usOut":2,"usDiff":1,"id":{id}}}"#
    )
}

/// The health event reporting that the venue refused `feed` of `inst`.
pub fn refused_sub(inst: InstrumentId, feed: fbc_core::Feed) -> (VenueMeta, MdEvent) {
    let h = fbc_core::FeedHealth::Refused;
    (VenueMeta::NONE, MdEvent::Health { inst, feed, h })
}

/// One frame of a capture, as the WebSocket delivered it.
pub enum Captured {
    Text(String),
    Binary(Vec<u8>),
}

/// The frames of capture `name`: one JSON object per line, `opcode` `text` with the frame in
/// `text`, or `binary` with its bytes in standard base64 in `b64`.
pub fn capture(name: &str) -> Vec<Captured> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/md")
        .join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .map(|line| {
            let v: Value = serde_json::from_str(line).unwrap();
            match v["opcode"].as_str() {
                Some("text") => Captured::Text(v["text"].as_str().unwrap().to_owned()),
                Some("binary") => Captured::Binary(base64(v["b64"].as_str().unwrap())),
                other => panic!("opcode {other:?}"),
            }
        })
        .collect()
}

/// Standard base64 (RFC 4648 §4, `=` padding) decoded; panics on any other character.
pub fn base64(text: &str) -> Vec<u8> {
    let value = |c: u8| match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => panic!("not base64: {c}"),
    };
    let digits = text.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(digits.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &c in digits {
        acc = (acc << 6) | u32::from(value(c));
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

/// BTC-USD-PERP on Paradex's grid: a 0.1 price tick and a 0.00001 size step, the grid every
/// level in both captures lies on (the greatest common divisor of their prices and sizes).
pub fn live_specs() -> SpecTable {
    let mut btc = spec(BTC, "BTC-USD-PERP");
    btc.size_step = SizeStep::new(Decimal::new(1, 5)).unwrap();
    let mut table = SpecTable::new();
    table.insert(btc);
    table
}
