//! QFEX's private state frames, inspected and never normalized: no value here is an order
//! update, a fill, a fee or a resync until a later ticket establishes what each means (FBC-jxau,
//! decision 0132). The shapes are QFEX's trade AsyncAPI
//! (<https://docs.qfex.com/websocket/trade.yaml>): `order_response` (`OrderResponse`),
//! `fill_response` (`FillResponse`), `all_orders_response`, the trade history pages and
//! `position_response` (`PositionPayload`). Every number is read from its own text, exactly,
//! never through `f64` (FBC-zeb0 (c)).
//!
//! The members the trade socket's authentication redacts (`user_id`, `id` and the rest,
//! `src/auth`) are never read, so a frame inspects the same in replay as live.

use std::collections::HashSet;

use fbc_core::{DecodeScope, FillId, VenueOrderId};
use rust_decimal::Decimal;

use super::json::{self, Kind, Member, Node};

/// The private-state envelopes the codec recognises, each the frame's only top-level member.
pub(crate) const ENVELOPES: [&str; 6] = [
    "order_response",
    "fill_response",
    "all_orders_response",
    "user_trades",
    "user_trades_response",
    "position_response",
];

/// An order status the codec has seen QFEX send and reads; no other is read until private
/// conformance establishes what it means.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderStatus {
    Ack,
    Cancelled,
}

/// What the codec read of one private state frame, which still ends its epoch: nothing here is
/// evidence that one of our commands caused it, that a fee has a sign, or that a page or a
/// pulse is the whole account.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PrivateState {
    /// An `order_response` with a status read and an order identity.
    Order {
        order: VenueOrderId,
        status: OrderStatus,
        price: Decimal,
        quantity: Decimal,
        remaining: Decimal,
    },
    /// A `fill_response`: its fee as QFEX writes it, a number of unstated currency and sign.
    Fill {
        trade: FillId,
        order: VenueOrderId,
        price: Decimal,
        quantity: Decimal,
        raw_fee: Decimal,
    },
    /// One `all_orders_response` page: open orders only, paginated, with no watermark.
    OpenOrders { orders: Vec<VenueOrderId> },
    /// One trade history page, documented as `user_trades` with a `count` and observed as
    /// `user_trades_response`; it omits our side and states no page boundary.
    Trades { trades: Vec<FillId> },
    /// One symbol's `position_response`, with no account-wide completion marker.
    Position { position: Decimal },
}

/// Inspects `top`, a frame's top-level members: `None` when it names none of [`ENVELOPES`], an
/// error when it names more than one, or when the one it names is not as documented. The error
/// names what was wrong, never a value.
pub(crate) fn inspect(
    top: &[Member<'_>],
    scope: &DecodeScope<'_>,
) -> Option<Result<PrivateState, &'static str>> {
    let mut found = top
        .iter()
        .filter(|(name, _)| ENVELOPES.contains(&name.as_ref()));
    let (name, body) = found.next()?;
    if found.next().is_some() {
        return Some(Err("ambiguous private envelope"));
    }
    Some(one(name, body, top, scope))
}

fn one(
    name: &str,
    body: &Node<'_>,
    top: &[Member<'_>],
    scope: &DecodeScope<'_>,
) -> Result<PrivateState, &'static str> {
    match name {
        "order_response" => {
            let row = object(body)?;
            symbol(row)?;
            side(row, "side")?;
            let price = positive(row, "price")?;
            let quantity = positive(row, "quantity")?;
            let remaining = nonnegative(row, "quantity_remaining")?;
            if remaining > quantity {
                return Err("remaining exceeds quantity");
            }
            if json::get(row, "trade_id")
                .is_some_and(|id| !matches!(id.kind, Kind::Literal("null")))
            {
                return Err("order pulse carries a trade id");
            }
            let status = match json::get(row, "status").and_then(Node::str) {
                Some("ACK") => OrderStatus::Ack,
                Some("CANCELLED") => OrderStatus::Cancelled,
                _ => return Err("unverified order status"),
            };
            Ok(PrivateState::Order {
                order: order_id(row, scope)?,
                status,
                price,
                quantity,
                remaining,
            })
        }
        "fill_response" => {
            let row = object(body)?;
            side(row, "side")?;
            nonnegative(row, "timestamp")?;
            Ok(PrivateState::Fill {
                trade: fill_id(row, scope)?,
                order: order_id(row, scope)?,
                price: positive(row, "price")?,
                quantity: positive(row, "quantity")?,
                raw_fee: number(row, "fee")?,
            })
        }
        "all_orders_response" => {
            let page = object(body)?;
            let rows = array(page, "orders")?;
            // TWAPs and stop orders are other open-order classes: a page with them is partial.
            array(page, "twaps")
                .or_else(|_| array(page, "stop_orders"))
                .map_err(|_| "orders page companion")?;
            let mut orders = Vec::with_capacity(rows.len());
            for row in rows {
                let row = object(row)?;
                symbol(row)?;
                side(row, "side")?;
                positive(row, "quantity")?;
                nonnegative(row, "quantity_remaining")?;
                orders.push(order_id(row, scope)?);
            }
            unique(&orders, "a repeated order id in the page")?;
            Ok(PrivateState::OpenOrders { orders })
        }
        "user_trades" | "user_trades_response" => {
            let rows = body.items().ok_or("trades page")?;
            // The documented page states its count; the observed one may, and is held to it.
            if name == "user_trades" || json::get(top, "count").is_some() {
                let count = json::get(top, "count").map(|node| &node.kind);
                let Some(Kind::Num(raw)) = count else {
                    return Err("trades count");
                };
                raw.parse::<u64>().map_err(|_| "trades count")?;
            }
            let mut trades = Vec::with_capacity(rows.len());
            for row in rows {
                let row = object(row)?;
                symbol(row)?;
                order_id(row, scope)?;
                positive(row, "price")?;
                positive(row, "quantity")?;
                nonnegative(row, "timestamp")?;
                number(row, "fee")?;
                trades.push(fill_id(row, scope)?);
            }
            unique(&trades, "a repeated trade id in the page")?;
            Ok(PrivateState::Trades { trades })
        }
        _ => {
            let row = object(body)?;
            symbol(row)?;
            Ok(PrivateState::Position {
                position: number(row, "position")?,
            })
        }
    }
}

fn object<'n, 'a>(node: &'n Node<'a>) -> Result<&'n [Member<'a>], &'static str> {
    node.members().ok_or("private envelope")
}

fn array<'n, 'a>(row: &'n [Member<'a>], key: &'static str) -> Result<&'n [Node<'a>], &'static str> {
    json::get(row, key).and_then(Node::items).ok_or(key)
}

fn unique<T: Eq + std::hash::Hash>(ids: &[T], what: &'static str) -> Result<(), &'static str> {
    let mut seen = HashSet::new();
    ids.iter()
        .all(|id| seen.insert(id))
        .then_some(())
        .ok_or(what)
}

/// A UUID's wire form, 8-4-4-4-12 hex digits; no page restricts its version or case.
fn uuid<'n>(row: &'n [Member<'_>], key: &'static str) -> Result<&'n str, &'static str> {
    let id = json::get(row, key).and_then(Node::str).ok_or(key)?;
    let shaped = id.len() == 36
        && id.bytes().enumerate().all(|(at, byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        });
    shaped.then_some(id).ok_or("a UUID")
}

fn order_id(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<VenueOrderId, &'static str> {
    scope
        .venue_order_id(uuid(row, "order_id")?)
        .map_err(|_| "order_id")
}

fn fill_id(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<FillId, &'static str> {
    scope
        .fill_id(uuid(row, "trade_id")?)
        .map_err(|_| "trade_id")
}

fn symbol(row: &[Member<'_>]) -> Result<(), &'static str> {
    json::get(row, "symbol")
        .and_then(Node::str)
        .is_some_and(|symbol| !symbol.is_empty())
        .then_some(())
        .ok_or("symbol")
}

fn side(row: &[Member<'_>], key: &'static str) -> Result<(), &'static str> {
    matches!(
        json::get(row, key).and_then(Node::str),
        Some("BUY" | "SELL")
    )
    .then_some(())
    .ok_or(key)
}

/// The number `key` names, exactly as written: a plain decimal, or one with an exponent, each
/// refused rather than rounded when a `Decimal` cannot hold it.
fn number(row: &[Member<'_>], key: &'static str) -> Result<Decimal, &'static str> {
    let Some(Kind::Num(raw)) = json::get(row, key).map(|node| &node.kind) else {
        return Err(key);
    };
    exact(raw).ok_or(key)
}

/// A JSON number's text as a `Decimal`, exactly: an exponent moves the decimal point in the
/// text itself, and `Decimal::from_str_exact` refuses what it cannot hold without rounding.
pub(crate) fn exact(raw: &str) -> Option<Decimal> {
    let Some((mantissa, exponent)) = raw.split_once(['e', 'E']) else {
        return Decimal::from_str_exact(raw).ok();
    };
    // A Decimal holds at most 28 places, so a larger shift could only overflow or round. The
    // range is checked as a range: `i32::MIN` has no absolute value (Codex P2, PR #226).
    let exponent: i32 = exponent.parse().ok().filter(|e| (-64..=64).contains(e))?;
    let (sign, unsigned) = match mantissa.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mantissa),
    };
    let (int, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{int}{frac}");
    let point = i32::try_from(int.len()).ok()?.checked_add(exponent)?;
    let text = if point <= 0 {
        format!(
            "{sign}0.{}{digits}",
            "0".repeat(point.unsigned_abs() as usize)
        )
    } else if point as usize >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(point as usize - digits.len())
        )
    } else {
        let (whole, part) = digits.split_at(point as usize);
        format!("{sign}{whole}.{part}")
    };
    Decimal::from_str_exact(&text).ok()
}

fn positive(row: &[Member<'_>], key: &'static str) -> Result<Decimal, &'static str> {
    number(row, key).and_then(|n| (n > Decimal::ZERO).then_some(n).ok_or(key))
}

fn nonnegative(row: &[Member<'_>], key: &'static str) -> Result<Decimal, &'static str> {
    number(row, key).and_then(|n| (n >= Decimal::ZERO).then_some(n).ok_or(key))
}
