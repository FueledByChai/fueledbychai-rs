//! The `get_user_orders` pages each session's resync read (decision 0132 item 5, FBC-4h1p): the
//! codec counts every full page, and the probe records each one with its page size and offset.
//!
//! The codec keeps them in its own resync evidence (`ResyncEvidence::full_pages`), which no
//! caller of an unchanged `ExecSession` can reach: the session owns its codec, built by the
//! venue's factory, and hands its handler only the events it decodes. So the probe reads them
//! from the session's journal (decision 0006), as it reads the session's frames for the slice
//! and for its provider rate refusals (`capture.rs`, `refusals.rs`), and leaves the runtime's
//! contract and the codec as they are:
//!
//! - **an ask** is an outbound frame on the session's connection that is a `get_user_orders`
//!   request, `{"type":"get_user_orders","params":{"limit":N,"offset":M}}`, as the codec writes
//!   each page of its resync ([`ask`]). The journal keeps an outbound frame as written: the
//!   codec names no span in a page's request;
//! - **its answer** is the first text frame after it on that connection, before its epoch's
//!   close, that is a page as the codec reads one on the probe's sessions ([`rows`]), read from
//!   the bytes the journal kept. A frame journaled after the close reached no codec: a session
//!   the probe stops journals its epoch closed, then the frame waiting, which no codec reads. The
//!   codec reads a page only from a text frame: a binary one it refuses as no JSON object, ending
//!   its epoch, whatever its bytes hold, so `capture.rs` offers [`Pages::heard`] only the frames
//!   the journal kept as text. A page is JSON in which no object names a member twice (the codec
//!   refuses any frame that does), holding one `all_orders_response` with exactly an `orders`
//!   and a `twaps` array (`account_reads::page_rows`), no more of them together than the page
//!   size, each TWAP an object and each order row an object naming a string `order_id` and,
//!   unless its `type` is one of the two stop kinds the resync leaves out, a non-empty string
//!   `symbol` (`Resync::orders_page` and `private::snapshot`). On a symbol the session's spec
//!   table does not hold, the codec reads nothing more of a row: stage one's sessions run on an
//!   empty table. A place stage's sessions hold the traded spec (FBC-23ch), so a row on its
//!   symbol, X's for one, is read as the codec reads an open order of its snapshot ([`held`]):
//!   its `order_id` a UUID, its `side` `BUY` or `SELL`, its `price` and `quantity` positive
//!   numbers on the spec's grid and in whole size steps, its `quantity_remaining` a number from
//!   zero to its quantity leaving a filled part in whole size steps, and its `client_order_id`
//!   absent, null or a string; a row that is not is refused, and so is its page. The codec sends
//!   a page only while no other is in flight and reads the first such frame as its answer, so the
//!   pairing is the codec's own;
//! - **a full page** is one whose orders and TWAPs together are as many as its page size, as the
//!   codec counts it; a shorter one, an empty one included, ends the paging and is recorded as
//!   no full page.
//!
//! A frame the journal keeps whole (decision 0134 item 2), blanked to `2`s, is a JSON number,
//! not an object, and is not read as a page; nor is a frame the codec refuses as a page, which
//! ends its epoch and so the session (its decode errors count it). An ask no page answered (the
//! session stopped first, or the codec refused the answer) is counted asked and not read. A
//! partial capture (decision 0006) may miss an ask or its answer, so its records may be short.
//! Nothing of a row is kept: the probe records only page sizes, offsets and counts, never an id
//! or an amount (decision 0009).
use std::collections::HashSet;

use fbc_core::{InstrumentSpec, PxExact, SpecTable};
use fbc_venue_qfex::exec::is_venue_order_id;
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

/// The two stop kinds of QFEX's [enums page](https://docs.qfex.com/api-reference/enums), whose
/// order rows the codec's resync leaves out of its snapshot unread (its `STOP_KINDS`).
const STOP_KINDS: [&str; 2] = ["TAKE_PROFIT", "STOP_LOSS"];

/// One `get_user_orders` page a session's resync asked for: its page size and offset.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Page {
    pub size: u16,
    pub offset: u32,
}

/// What a session's journal shows of its resync's `get_user_orders` pages.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pages {
    /// The pages asked for.
    pub asked: u64,
    /// The asks a page answered.
    pub read: u64,
    /// The orders those pages listed, TWAPs aside: what decision 0132 item 5's cross-check
    /// compares with REST's `open_orders` ([`crate::capture::SessionRun::resync_open_orders`]).
    pub orders: u64,
    /// Each full page, in the order read: its page size and offset.
    pub full: Vec<Page>,
    /// The page asked for that no page has answered yet.
    waiting: Option<Page>,
}

impl Pages {
    /// Reads `frame`, one the session wrote: an ask ([`ask`]) waits for its answer.
    pub fn sent(&mut self, frame: &[u8]) {
        if let Some(page) = ask(frame) {
            self.asked += 1;
            self.waiting = Some(page);
        }
    }

    /// Ends the session's epoch: no frame journaled after its close reached the codec (the
    /// runtime journals a frame waiting as a session stops after its close, and hands it to no
    /// codec), so none answers the page waiting.
    pub fn closed(&mut self) {
        self.waiting = None;
    }

    /// Reads `text`, a text frame QFEX sent a stage-one session, which holds no spec, as its
    /// journal kept it ([`Pages::heard_on`] on an empty spec table).
    #[allow(dead_code)] // stage one's tests read pages through it
    pub fn heard(&mut self, text: &str) {
        self.heard_on(text, &SpecTable::new());
    }

    /// Reads `text`, a text frame QFEX sent the session as its journal kept it: the answer to the
    /// page waiting, when it is a page as the codec reads one on the session's spec table `specs`
    /// ([`rows_on`]), recorded when it is full. A binary frame is never offered, since the codec
    /// reads no page from one.
    pub fn heard_on(&mut self, text: &str, specs: &SpecTable) {
        let Some(page) = self.waiting else {
            return;
        };
        let Some((orders, listed)) = orders_on(text, page.size, specs) else {
            return;
        };
        self.waiting = None;
        self.read += 1;
        self.orders += orders as u64;
        if listed == usize::from(page.size) {
            self.full.push(page);
        }
    }

    /// The full pages as the report lists them: each by its page size and offset, or `none`.
    pub fn listed(&self) -> String {
        let full: Vec<String> = (self.full.iter())
            .map(|p| format!("page size {} at offset {}", p.size, p.offset))
            .collect();
        match full.is_empty() {
            true => "none".to_owned(),
            false => full.join("; "),
        }
    }

    /// The report's line on them, under a session.
    pub fn markdown(&self) -> String {
        format!(
            "- Its resync's `get_user_orders` pages (decision 0132 item 5), as its journal kept \
             them (a partial capture may miss one): asked {}, answered by a page {}; full pages, \
             each by its page size and offset: {}",
            self.asked,
            self.read,
            self.listed()
        )
    }

    /// The evidence's entry for them.
    pub fn to_json(&self) -> Value {
        let full: Vec<Value> = (self.full.iter())
            .map(|p| json!({"page_size": p.size, "offset": p.offset}))
            .collect();
        json!({"asked": self.asked, "read": self.read, "orders": self.orders, "full_pages": full})
    }
}

/// The page `frame` asks for, when it is a `get_user_orders` request naming its page size, 1 to
/// `u16::MAX`, and its offset, as the codec writes one; `None` for any other frame.
pub fn ask(frame: &[u8]) -> Option<Page> {
    let top: Value = serde_json::from_slice(frame).ok()?;
    let asks = top.get("type").and_then(Value::as_str) == Some("get_user_orders");
    let params = top.get("params").filter(|_| asks)?;
    let number = |key: &str| params.get(key).and_then(Value::as_u64);
    let size = u16::try_from(number("limit")?).ok().filter(|n| *n > 0)?;
    let offset = u32::try_from(number("offset")?).ok()?;
    Some(Page { size, offset })
}

/// [`rows_on`] on an empty spec table, as stage one's sessions run: no row is read past its
/// symbol.
#[allow(dead_code)] // stage one's tests read pages through it
pub fn rows(text: &str, size: u16) -> Option<usize> {
    rows_on(text, size, &SpecTable::new())
}

/// How many orders and TWAPs together `text` lists, when it is a page answering an ask of page
/// size `size` as the codec reads one on a session whose spec table is `specs`: JSON repeating
/// no member ([`repeats`]), one `all_orders_response` holding exactly an `orders` and a `twaps`
/// array, at most `size` together, each TWAP an object, each order row an object naming a string
/// `order_id` and, unless its `type` is a stop kind, a non-empty string `symbol`, and a row on a
/// symbol `specs` holds read as [`held`] reads it. `None` for any other frame.
pub fn rows_on(text: &str, size: u16, specs: &SpecTable) -> Option<usize> {
    orders_on(text, size, specs).map(|(_, rows)| rows)
}

/// [`rows_on`]'s page, and how many of its rows are orders, TWAPs aside: `(orders, rows)`.
fn orders_on(text: &str, size: u16, specs: &SpecTable) -> Option<(usize, usize)> {
    let Ok(Value::Object(top)) = serde_json::from_str::<Value>(text) else {
        return None;
    };
    let page = match top.get("all_orders_response") {
        Some(Value::Object(page)) if top.len() == 1 && !repeats(text) => page,
        _ => return None,
    };
    let list = |key: &str| page.get(key).and_then(Value::as_array);
    let (orders, twaps) = (list("orders")?, list("twaps")?);
    let read = |row: &Value| {
        let text = |key: &str| row.get(key).and_then(Value::as_str);
        let symbol = text("symbol").filter(|symbol| !symbol.is_empty());
        let on = |symbol: &str| match (specs.by_symbol(symbol), row.as_object()) {
            (None, _) => true,
            (Some(spec), Some(row)) => held(row, spec),
            (Some(_), None) => false,
        };
        text("order_id").is_some()
            && (text("type").is_some_and(|kind| STOP_KINDS.contains(&kind))
                || symbol.is_some_and(on))
    };
    let shaped = page.len() == 2
        && orders.iter().all(read)
        && twaps.iter().all(Value::is_object)
        && orders.len() + twaps.len() <= usize::from(size);
    shaped.then_some((orders.len(), orders.len() + twaps.len()))
}

/// Whether `row`, an order row on `spec`'s symbol, reads as the codec reads an open order of its
/// resync's snapshot (`private::snapshot`): its `order_id` a UUID, its `side` `BUY` or `SELL`,
/// its `price` a positive number exactly on `spec`'s grid, its `quantity` a positive number in
/// whole size steps, its `quantity_remaining` a number from zero to the quantity whose filled
/// part is in whole size steps, and its `client_order_id` absent, null or a string.
pub fn held(row: &Map<String, Value>, spec: &InstrumentSpec) -> bool {
    let number = |key: &str| match row.get(key) {
        Some(Value::Number(n)) => exact(&n.to_string()),
        _ => None,
    };
    let steps = |size: Decimal| {
        let step = spec.size_step.get();
        let n = size.checked_div(step);
        n.is_some_and(|n| n.fract().is_zero() && n.checked_mul(step) == Some(size))
    };
    let order = row.get("order_id").and_then(Value::as_str);
    let side = row.get("side").and_then(Value::as_str);
    let price = number("price").filter(|p| *p > Decimal::ZERO);
    let on_grid = price
        .and_then(PxExact::from_decimal)
        .is_some_and(|px| spec.price_grid.ticks_exact(px).is_some());
    let quantity = number("quantity").filter(|q| *q > Decimal::ZERO && steps(*q));
    let remaining = number("quantity_remaining").filter(|r| *r >= Decimal::ZERO);
    let left = match (quantity, remaining) {
        (Some(q), Some(r)) => r <= q && steps(q - r),
        _ => false,
    };
    let cid = matches!(
        row.get("client_order_id"),
        None | Some(Value::Null | Value::String(_))
    );
    order.is_some_and(is_venue_order_id)
        && matches!(side, Some("BUY" | "SELL"))
        && on_grid
        && left
        && cid
}

/// A JSON number's text as a `Decimal`, exactly, as the codec reads one: a plain decimal or one
/// with an exponent, `None` for one a `Decimal` cannot hold without rounding.
fn exact(raw: &str) -> Option<Decimal> {
    match raw.contains(['e', 'E']) {
        true => Decimal::from_scientific(raw).ok(),
        false => Decimal::from_str_exact(raw).ok(),
    }
}

/// Whether an object anywhere in `text`, JSON serde_json has parsed, names a member twice, as
/// the codec judges it (`json::Node::repeats`, each name with its escapes decoded). serde_json's
/// `Value` keeps only the last of a repeated member, so the text is scanned: each string where
/// an object expects a member name is that name, decoded by serde_json.
fn repeats(text: &str) -> bool {
    // Each container open, innermost last: an object's names so far, or `None` for an array.
    let mut open: Vec<Option<HashSet<Option<String>>>> = Vec::new();
    let mut name_next = false;
    let bytes = text.as_bytes();
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        at += 1;
        match byte {
            b'{' => {
                open.push(Some(HashSet::new()));
                name_next = true;
            }
            b'[' => open.push(None),
            b'}' | b']' => {
                open.pop();
                name_next = false;
            }
            b',' => name_next = matches!(open.last(), Some(Some(_))),
            b'"' => {
                let start = at - 1;
                while let Some(&byte) = bytes.get(at) {
                    at += if byte == b'\\' { 2 } else { 1 };
                    if byte == b'"' {
                        break;
                    }
                }
                let name = text
                    .get(start..at)
                    .and_then(|s| serde_json::from_str(s).ok());
                if std::mem::take(&mut name_next)
                    && let Some(Some(names)) = open.last_mut()
                    && !names.insert(name)
                {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}
