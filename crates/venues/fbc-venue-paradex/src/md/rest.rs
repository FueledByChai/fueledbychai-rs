//! Paradex's REST order book, `GET /v1/orderbook/{market}` at depth 15 (docs.paradex.trade,
//! "Get market orderbook"): the snapshot BT-401's book oracle compares with the book built from
//! the deltas at the same seq_no. Decoding is pure; taking the snapshot is the runtime's.
//!
//! The response is JSON: `market`, `seq_no` ("Sequence number of the orderbook"),
//! `last_updated_at` (milliseconds), and `bids` and `asks`, each a list of `[price, size]`
//! decimal strings; the `best_*` fields are not read.

use fbc_core::{
    Channel, DecodeError, ExchNs, InstrumentId, InstrumentSpec, Lvl, PxExact, SpecTable,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use super::{lots_of, market};

/// The depth asked for: 15 levels per side, compared with the top 15 of the stream-built book,
/// which carries the whole book (decision 0074).
pub const ORDERBOOK_DEPTH: usize = 15;

/// The order channels the response's `bids` and `asks` show: the public book. The response
/// names the interactive book's best levels apart (`best_bid_interactive`,
/// `best_ask_interactive`), so the RPI liquidity the `interactive_deltas` channel includes is
/// not in them.
pub const ORDERBOOK_CHANNELS: &[Channel] = &[Channel::Public];

/// The request path and query of the order book of the market spelled `symbol`.
pub fn orderbook_path(symbol: &str) -> String {
    format!("/v1/orderbook/{symbol}?depth={ORDERBOOK_DEPTH}")
}

/// One REST order book snapshot, levels on the instrument's grid and size step.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OrderbookSnapshot {
    pub inst: InstrumentId,
    /// The book's sequence number, the one the book channel's frames carry (decision 0022).
    pub seq_no: u64,
    /// `last_updated_at`: the book's last update, on the venue's clock.
    pub updated_at: ExchNs,
    /// From the highest price down.
    pub bids: Vec<Lvl>,
    /// From the lowest price up.
    pub asks: Vec<Lvl>,
}

/// Decodes a `/orderbook` response body. Refused, naming the part: a body that is not a JSON
/// object, a market missing from `specs`, a missing or negative `seq_no`, a missing
/// `last_updated_at` or one out of range in nanoseconds, and a side that is not a list of `[price, size]` strings on the grid and size step, has a
/// level of zero size, is out of order (each price once, bids down, asks up) or holds more than
/// [`ORDERBOOK_DEPTH`] levels.
pub fn decode_orderbook(body: &[u8], specs: &SpecTable) -> Result<OrderbookSnapshot, DecodeError> {
    let doc: Value = serde_json::from_slice(body)
        .map_err(|_| DecodeError::Malformed("orderbook response is not JSON"))?;
    let doc = doc.as_object().ok_or(DecodeError::Malformed(
        "orderbook response is not an object",
    ))?;
    let symbol = doc.get("market").and_then(Value::as_str);
    let spec = market(symbol.ok_or(DecodeError::Malformed("market"))?, specs)?;
    let seq_no = doc.get("seq_no").and_then(Value::as_u64);
    let seq_no = seq_no.ok_or(DecodeError::Malformed("seq_no"))?;
    let ms = doc.get("last_updated_at").and_then(Value::as_i64);
    let ns = ms.and_then(|ms| ms.checked_mul(1_000_000));
    let updated_at = ExchNs(ns.ok_or(DecodeError::Malformed("last_updated_at"))?);
    Ok(OrderbookSnapshot {
        inst: spec.id,
        seq_no,
        updated_at,
        bids: side(spec, doc, Side::Bids)?,
        asks: side(spec, doc, Side::Asks)?,
    })
}

/// A side of the response, and the faults it names.
#[derive(Copy, Clone)]
enum Side {
    Bids,
    Asks,
}

/// What can be wrong with a side, indexing [`FAULTS`].
#[derive(Copy, Clone)]
enum Fault {
    Missing,
    Deep,
    Pair,
    Price,
    Size,
    Zero,
    Order,
}

/// Each side's faults, in [`Fault`]'s order.
const FAULTS: [[&str; 7]; 2] = [
    [
        "bids",
        "bids: more levels than depth 15",
        "bids: a level is not [price, size]",
        "bids: price",
        "bids: size",
        "bids: a level of zero size",
        "bids: levels out of order",
    ],
    [
        "asks",
        "asks: more levels than depth 15",
        "asks: a level is not [price, size]",
        "asks: price",
        "asks: size",
        "asks: a level of zero size",
        "asks: levels out of order",
    ],
];

impl Side {
    /// The fault `what` of this side.
    fn fault(self, what: Fault) -> DecodeError {
        DecodeError::Malformed(FAULTS[self as usize][what as usize])
    }

    /// Whether `next` may follow `prev` on this side: bids down, asks up, each price once.
    fn follows(self, prev: Lvl, next: Lvl) -> bool {
        match self {
            Side::Bids => next.px < prev.px,
            Side::Asks => next.px > prev.px,
        }
    }
}

fn side(
    spec: &InstrumentSpec,
    doc: &Map<String, Value>,
    side: Side,
) -> Result<Vec<Lvl>, DecodeError> {
    let key = FAULTS[side as usize][Fault::Missing as usize];
    let entries = doc.get(key).and_then(Value::as_array);
    let entries = entries.ok_or(side.fault(Fault::Missing))?;
    if entries.len() > ORDERBOOK_DEPTH {
        return Err(side.fault(Fault::Deep));
    }
    let mut levels: Vec<Lvl> = Vec::with_capacity(entries.len());
    for entry in entries {
        let level = level(spec, entry, side)?;
        if levels
            .last()
            .is_some_and(|&prev| !side.follows(prev, level))
        {
            return Err(side.fault(Fault::Order));
        }
        levels.push(level);
    }
    Ok(levels)
}

/// One `[price, size]` entry, exactly on the grid and the size step, of nonzero size.
fn level(spec: &InstrumentSpec, entry: &Value, side: Side) -> Result<Lvl, DecodeError> {
    let Some([px, qty]) = entry.as_array().map(Vec::as_slice) else {
        return Err(side.fault(Fault::Pair));
    };
    let px: Option<PxExact> = px.as_str().and_then(|s| s.parse().ok());
    let px = px.and_then(|px| spec.price_grid.ticks_exact(px));
    let px = px.ok_or(side.fault(Fault::Price))?;
    let qty = qty.as_str().and_then(|s| Decimal::from_str_exact(s).ok());
    let qty = qty.ok_or(side.fault(Fault::Size))?;
    let qty = lots_of(spec, qty).map_err(|_| side.fault(Fault::Size))?;
    if qty.get() == 0 {
        return Err(side.fault(Fault::Zero));
    }
    Ok(Lvl { px, qty })
}
