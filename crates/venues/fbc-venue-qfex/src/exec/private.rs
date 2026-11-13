//! QFEX's private state frames. The shapes are QFEX's trade AsyncAPI
//! (<https://docs.qfex.com/websocket/trade.yaml>): `order_response` (`OrderResponse`),
//! `fill_response` (`FillResponse`), `all_orders_response`, the trade history pages and
//! `position_response` (`PositionPayload`). Every number is read from its own text, exactly,
//! never through `f64` (FBC-zeb0 (c)).
//!
//! The recording-only codec only inspects them ([`inspect`], [`PrivateState`]), and each still
//! ends its epoch there (FBC-zeb0). Under either probe mode (decision 0143 item 4) the codec
//! decodes them ([`decode`], FBC-jxau, decision 0132 items 5 and 8): an `order_response` into
//! an order update, a `fill_response` into a fill, a `position_response` into the account's
//! position, each with no venue time (`VenueMeta::NONE`: what kind of time QFEX's timestamps
//! are is unknown, 0132 item 5, and the probe caps declare no ordering key). A frame on a
//! symbol the consumer's spec table does not hold pushes nothing and is counted, and one on a
//! held symbol whose price is off the grid or whose size is off the step is refused, never
//! rounded (decision 0147 item 2, decision 0120 item 2). An order frame never makes a fill
//! (decision 0005): only a `fill_response` does. The resync (`resync.rs`) reads the rows of the
//! pages it asked for here too: an open order's as an `order_response`'s ([`snapshot`]), and a
//! trade's ([`trade`]).
//!
//! The members the trade socket's authentication redacts (`user_id`, `id` and the rest,
//! `src/auth`) are never read, so a frame reads the same in replay as live.

use std::collections::HashSet;

use fbc_core::{
    CancelReason, CidMatch, DecodeScope, Fee, FillEvent, FillId, FillIdent, FillOrigin,
    InstrumentId, InstrumentSpec, Liquidity3, Lots, OrderUpdate, PxExact, Side, SignedLots,
    SpecTable, Ticks, VenueOrderId, VenueOrderSnapshot, VenueOrderState,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::ids;
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
            let rows = trade_rows(name, body, top)?;
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

/// The rows of a trade history page, `body` the member `name` names among `top`, a frame's
/// top-level members: the documented `user_trades` beside a whole `count`, or the AsyncAPI's
/// `user_trades_response`, held to a `count` when it states one.
pub(super) fn trade_rows<'n, 'a>(
    name: &str,
    body: &'n Node<'a>,
    top: &[Member<'_>],
) -> Result<&'n [Node<'a>], &'static str> {
    let rows = body.items().ok_or("trades page")?;
    // The documented page states its count; the observed one may, and is held to it.
    if name == "user_trades" || json::get(top, "count").is_some() {
        let count = json::get(top, "count").map(|node| &node.kind);
        let Some(Kind::Num(raw)) = count else {
            return Err("trades count");
        };
        raw.parse::<u64>().map_err(|_| "trades count")?;
    }
    Ok(rows)
}

// ----- Decoding, under either probe mode (FBC-jxau) -----

/// An `order_response` status the probe maps, from the
/// [enums page](https://docs.qfex.com/api-reference/enums)'s OrderStatus: `ACK` "Order
/// accepted.", `FILLED` "Order filled, this could be partially filled.", `IOC_PARTIALLY_FILLED`
/// "partially filled and the remainder cancelled", `IOC_CANCELLED` "The unfilled portion of an
/// immediate-or-cancel order was cancelled." and `CANCELLED` "Order successfully cancelled."
/// `NO_SUCH_ORDER` ("Cancel rejected, no such order exists.") is [`Decoded::NoSuchOrder`], and
/// `RATE_LIMITED` the codec reads before decoding. Every other status is unmapped until
/// captures show what it means (FBC-jdj7; decision 0132 item 8).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Status {
    Ack,
    Filled,
    IocPartiallyFilled,
    IocCancelled,
    Cancelled,
}

/// What one private state frame decodes to under a probe mode.
#[derive(Clone, Debug)]
pub(crate) enum Decoded {
    /// An `order_response`, as an order update, with the status it was read from. Its state:
    /// `ACK` `Open`; `FILLED` and `IOC_PARTIALLY_FILLED` `Filled` when `quantity_remaining` is
    /// zero and otherwise `Open`, a partial fill (decision 0132 item 8; FBC-jdj7 refines
    /// `IOC_PARTIALLY_FILLED` with something left from captures); `IOC_CANCELLED`
    /// `Canceled(Unfilled)`; `CANCELLED` `Canceled(Venue)`, since a cancel accepted does not
    /// show it was ours that ended the order.
    Order { update: OrderUpdate, status: Status },
    /// A `NO_SUCH_ORDER` `order_response`, read for its `order_id` alone: no page shows its
    /// shape, so the other members are not read, and it is no order update. It ends nothing
    /// (decision 0132 item 8).
    NoSuchOrder(VenueOrderId),
    /// A `fill_response`, as a fill of an order, and its `timestamp` rounded down to whole
    /// seconds, from which the resync reads missed fills (decision 0132 item 5).
    Fill { fill: FillEvent, secs: u64 },
    /// A `position_response`: the account's position in a held instrument.
    Position {
        inst: InstrumentId,
        qty: SignedLots,
        avg_entry: Option<PxExact>,
    },
    /// An `all_orders_response` or a trade history page no read of the resync in flight asked
    /// for, inspected as the recording-only codec inspects it.
    Page,
    /// An order, fill or position frame on a symbol the spec table does not hold (decision
    /// 0147 item 2).
    Unheld,
}

/// Why a private state frame was not decoded: a status the probe does not map, or any other
/// shape QFEX's pages do not give. Neither names a value.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Misread {
    Status,
    Shape,
}

impl From<&'static str> for Misread {
    fn from(_: &'static str) -> Misread {
        Misread::Shape
    }
}

/// Decodes `top`, a frame's top-level members, under a probe mode: `None` when it names none
/// of [`ENVELOPES`], an error when it names more than one, or when the one it names is not as
/// documented.
pub(crate) fn decode(
    top: &[Member<'_>],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Option<Result<Decoded, Misread>> {
    let mut found = top
        .iter()
        .filter(|(name, _)| ENVELOPES.contains(&name.as_ref()));
    let (name, body) = found.next()?;
    if found.next().is_some() {
        return Some(Err(Misread::Shape));
    }
    Some(match name.as_ref() {
        "order_response" => order(body, scope, specs),
        "fill_response" => fill(body, scope, specs).map_err(Misread::from),
        "position_response" => position(body, specs).map_err(Misread::from),
        _ => one(name, body, top, scope)
            .map(|_| Decoded::Page)
            .map_err(Misread::from),
    })
}

fn order(body: &Node<'_>, scope: &DecodeScope<'_>, specs: &SpecTable) -> Result<Decoded, Misread> {
    let row = object(body)?;
    let status = json::get(row, "status")
        .and_then(Node::str)
        .ok_or("status")?;
    let status = match status {
        "NO_SUCH_ORDER" => return Ok(Decoded::NoSuchOrder(order_id(row, scope)?)),
        "ACK" => Status::Ack,
        "FILLED" => Status::Filled,
        "IOC_PARTIALLY_FILLED" => Status::IocPartiallyFilled,
        "IOC_CANCELLED" => Status::IocCancelled,
        "CANCELLED" => Status::Cancelled,
        _ => return Err(Misread::Status),
    };
    let read = read_row(row, scope)?;
    // FILLED names its trade (the changelog of 12 Nov 2025); it is never read further.
    if json::get(row, "trade_id").is_some_and(|id| {
        !matches!(id.kind, Kind::Literal("null")) && !id.str().is_some_and(ids::uuid_form)
    }) {
        return Err(Misread::Shape);
    }
    let Some(spec) = held(row, specs)? else {
        return Ok(Decoded::Unheld);
    };
    let (px, qty, cum_filled) = read.on(spec)?;
    let state = match status {
        Status::Ack => VenueOrderState::Open,
        Status::Filled | Status::IocPartiallyFilled if read.remaining.is_zero() => {
            VenueOrderState::Filled
        }
        Status::Filled | Status::IocPartiallyFilled => VenueOrderState::Open,
        Status::IocCancelled => VenueOrderState::Canceled(CancelReason::Unfilled),
        Status::Cancelled => VenueOrderState::Canceled(CancelReason::Venue),
    };
    let update = OrderUpdate {
        cid: read.cid,
        vid: Some(read.vid),
        inst: spec.id,
        side: read.side,
        state,
        cum_filled,
        px: Some(px),
        qty: Some(qty),
        post_only: None,
        reduce_only: None,
    };
    Ok(Decoded::Order { update, status })
}

/// What an `order_response` and a page's order row both name: the order, its side, its price
/// and quantity, each positive, what is left of it, from zero to its quantity, and its client
/// id.
struct Row {
    vid: VenueOrderId,
    side: Side,
    price: Decimal,
    quantity: Decimal,
    remaining: Decimal,
    cid: Option<CidMatch>,
}

fn read_row(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<Row, &'static str> {
    let vid = order_id(row, scope)?;
    let side = read_side(row, "side")?;
    let price = positive(row, "price")?;
    let quantity = positive(row, "quantity")?;
    let remaining = nonnegative(row, "quantity_remaining")?;
    if remaining > quantity {
        return Err("quantity_remaining");
    }
    Ok(Row {
        vid,
        side,
        price,
        quantity,
        remaining,
        cid: cid(row, scope)?,
    })
}

impl Row {
    /// Its price on `spec`'s grid, its quantity and its filled part in `spec`'s lots, each
    /// exactly or refused.
    fn on(&self, spec: &InstrumentSpec) -> Result<(Ticks, Lots, Lots), &'static str> {
        Ok((
            ticks(spec, self.price)?,
            lots(spec, self.quantity)?,
            lots(spec, self.quantity - self.remaining)?,
        ))
    }
}

/// One `orders` row of a page the resync asked for (decision 0132 item 5), as an open order of
/// its snapshot, read as an `order_response` is but for its status: `None` on a symbol the spec
/// table does not hold, whatever else the row holds, since it is left out of the bracket
/// (decision 0147 item 2).
pub(super) fn snapshot(
    row: &[Member<'_>],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<Option<VenueOrderSnapshot>, &'static str> {
    let Some(spec) = held(row, specs)? else {
        return Ok(None);
    };
    let read = read_row(row, scope)?;
    let (px, qty, cum_filled) = read.on(spec)?;
    Ok(Some(VenueOrderSnapshot {
        cid: read.cid,
        vid: read.vid,
        inst: spec.id,
        side: read.side,
        state: VenueOrderState::Open,
        px: Some(px),
        qty,
        cum_filled,
        post_only: None,
        reduce_only: None,
    }))
}

/// One row of a trade history page the resync asked for (decision 0132 item 5), as the
/// [get_user_trades page](https://docs.qfex.com/websocket/channels/trade/get_user_trades) and
/// the AsyncAPI's `TradePayload` show it: its `trade_id`, `order_id`, positive `price` and
/// `quantity` and `fee`, and, where the row names them, its `client_order_id` and its
/// `timestamp` (the page's, in seconds; the AsyncAPI's row has neither). Its side is not read:
/// a replayed fill takes our side from the order the codec placed. On a symbol the spec table
/// does not hold, only its two ids are read, `cid` and `at` are `None`, and the row is evidence
/// alone (decision 0147 item 2).
pub(super) struct Trade {
    pub(super) fill: FillId,
    pub(super) vid: VenueOrderId,
    pub(super) cid: Option<CidMatch>,
    pub(super) at: Option<TradeAt>,
}

/// A trade row on a held symbol: its instrument, its price on the grid, its quantity in lots
/// and its fee through the scope, each exactly or refused.
pub(super) struct TradeAt {
    pub(super) inst: InstrumentId,
    pub(super) px: Ticks,
    pub(super) qty: Lots,
    pub(super) fee: Fee,
}

pub(super) fn trade(
    row: &Node<'_>,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<Trade, &'static str> {
    let row = object(row)?;
    let fill = fill_id(row, scope)?;
    let vid = order_id(row, scope)?;
    let Some(spec) = held(row, specs)? else {
        return Ok(Trade {
            fill,
            vid,
            cid: None,
            at: None,
        });
    };
    let price = positive(row, "price")?;
    let quantity = positive(row, "quantity")?;
    let fee = number(row, "fee")?;
    // Read as a `fill_response`'s is where the row names it; not kept.
    json::get(row, "timestamp")
        .map(|_| seconds(row))
        .transpose()?;
    let cid = cid(row, scope)?;
    let at = Some(TradeAt {
        inst: spec.id,
        px: ticks(spec, price)?,
        qty: lots(spec, quantity)?,
        fee: scope.fee(nanos(fee)?, spec.settle_ccy).map_err(|_| "fee")?,
    });
    Ok(Trade { fill, vid, cid, at })
}

/// A `fill_response`, keyed by its `trade_id` (decision 0132 item 5), as an execution of the
/// order it names whatever its `execution_type`, which is not read (FBC-jdj7 reads
/// `LIQUIDATION` from captures; decision 0120 item 1): its side as the account's, its fee
/// through the scope as QFEX writes it, a cost when positive (decision 0004), exact to the
/// nano or refused.
fn fill(
    body: &Node<'_>,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<Decoded, &'static str> {
    let row = object(body)?;
    let trade = fill_id(row, scope)?;
    let vid = order_id(row, scope)?;
    let side = read_side(row, "side")?;
    let price = positive(row, "price")?;
    let quantity = positive(row, "quantity")?;
    let fee = number(row, "fee")?;
    let secs = seconds(row)?;
    let cid = cid(row, scope)?;
    let Some(spec) = held(row, specs)? else {
        return Ok(Decoded::Unheld);
    };
    let fill = FillEvent {
        ident: FillIdent::Venue {
            fill: trade,
            vid: Some(vid),
            cum_after: None,
        },
        origin: FillOrigin::Order {
            cid,
            px: ticks(spec, price)?,
        },
        inst: spec.id,
        side,
        qty: lots(spec, quantity)?,
        liquidity: Liquidity3::Unknown,
        fee: scope.fee(nanos(fee)?, spec.settle_ccy).map_err(|_| "fee")?,
        realized_pnl: None,
        realized_funding: None,
        replay: false,
    };
    Ok(Decoded::Fill { fill, secs })
}

/// A fee in nanos of its currency, exactly, or refused.
fn nanos(fee: Decimal) -> Result<i128, &'static str> {
    fee.checked_mul(Decimal::from(1_000_000_000_u32))
        .filter(|n| n.fract().is_zero())
        .and_then(|n| n.to_i128())
        .ok_or("fee")
}

/// The row's `timestamp`, at least zero, rounded down to whole seconds (the AsyncAPI's
/// "Timestamp in seconds with fractional part").
fn seconds(row: &[Member<'_>]) -> Result<u64, &'static str> {
    nonnegative(row, "timestamp")?
        .floor()
        .to_u64()
        .ok_or("timestamp")
}

/// A `position_response`: `position` in whole size steps, positive long, and its
/// `average_price` as the entry price only while the position is open and the price positive.
/// The members it does not read (`margin`, `margin_mode`, the spanned `id`) may be anything.
fn position(body: &Node<'_>, specs: &SpecTable) -> Result<Decoded, &'static str> {
    let row = object(body)?;
    let position = number(row, "position")?;
    let average = json::get(row, "average_price")
        .map(|_| nonnegative(row, "average_price"))
        .transpose()?;
    let Some(spec) = held(row, specs)? else {
        return Ok(Decoded::Unheld);
    };
    let qty = SignedLots(steps(spec, position).ok_or("position")?);
    let avg_entry = match average {
        Some(price) if qty.0 != 0 && price > Decimal::ZERO => {
            Some(PxExact::from_decimal(price).ok_or("average_price")?)
        }
        _ => None,
    };
    Ok(Decoded::Position {
        inst: spec.id,
        qty,
        avg_entry,
    })
}

/// The spec of the row's `symbol`, `None` when the table holds none.
fn held<'s>(
    row: &[Member<'_>],
    specs: &'s SpecTable,
) -> Result<Option<&'s InstrumentSpec>, &'static str> {
    let symbol = json::get(row, "symbol")
        .and_then(Node::str)
        .filter(|symbol| !symbol.is_empty())
        .ok_or("symbol")?;
    Ok(specs.by_symbol(symbol))
}

/// The row's `client_order_id`: `None` when absent, null or empty, as QFEX answers an order
/// that names none (decision 0132 item 5), so the order is routed by its venue id; otherwise
/// ours, another namespace's or not canonical, through the scope.
fn cid(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<Option<CidMatch>, &'static str> {
    match json::get(row, "client_order_id") {
        None => Ok(None),
        Some(node) if matches!(node.kind, Kind::Literal("null")) => Ok(None),
        Some(node) => {
            let wire = node.str().ok_or("client_order_id")?;
            Ok((!wire.is_empty()).then(|| scope.client_order_id(wire)))
        }
    }
}

/// `price` on the spec's grid, exactly, or refused (decision 0120 item 2).
fn ticks(spec: &InstrumentSpec, price: Decimal) -> Result<Ticks, &'static str> {
    PxExact::from_decimal(price)
        .and_then(|px| spec.price_grid.ticks_exact(px))
        .ok_or("price")
}

/// `size` in whole size steps, or refused.
fn lots(spec: &InstrumentSpec, size: Decimal) -> Result<Lots, &'static str> {
    steps(spec, size).and_then(Lots::new).ok_or("quantity")
}

/// `size` divided by the spec's size step, when that is a whole number an `i64` holds.
pub(super) fn steps(spec: &InstrumentSpec, size: Decimal) -> Option<i64> {
    let step = spec.size_step.get();
    let n = size.checked_div(step)?;
    (n.fract().is_zero() && n.checked_mul(step) == Some(size))
        .then(|| n.to_i64())
        .flatten()
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

/// The id `key` names, when `rule` accepts it.
fn id<'n>(
    row: &'n [Member<'_>],
    key: &'static str,
    rule: fn(&str) -> bool,
) -> Result<&'n str, &'static str> {
    let id = json::get(row, key).and_then(Node::str).ok_or(key)?;
    rule(id).then_some(id).ok_or("a UUID")
}

/// An order id, by the one order-id rule the encoder's cancels and queries share.
fn order_id(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<VenueOrderId, &'static str> {
    scope
        .venue_order_id(id(row, "order_id", ids::is_venue_order_id)?)
        .map_err(|_| "order_id")
}

/// A trade id, in a UUID's wire form.
fn fill_id(row: &[Member<'_>], scope: &DecodeScope<'_>) -> Result<FillId, &'static str> {
    scope
        .fill_id(id(row, "trade_id", ids::uuid_form)?)
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
    read_side(row, key).map(|_| ())
}

/// The side `key` names: `BUY` or `SELL`, the enums page's OrderDirection as the frames write
/// it.
fn read_side(row: &[Member<'_>], key: &'static str) -> Result<Side, &'static str> {
    match json::get(row, key).and_then(Node::str) {
        Some("BUY") => Ok(Side::Buy),
        Some("SELL") => Ok(Side::Sell),
        _ => Err(key),
    }
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
