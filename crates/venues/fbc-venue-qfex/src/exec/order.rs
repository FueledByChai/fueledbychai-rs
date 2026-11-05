//! QFEX's order commands as trade-socket frames (FBC-blcw, BT-702), built from the command and
//! the spec table alone, on the owner's v1 scope of 2026-10-29. The codec ([`super::QfexExec`])
//! sends each as one frame naming its request; no reply settles it, so each ends `Unknown` at
//! its deadline. QFEX signs no order frame: the HMAC signs only the authentication (`src/sign`),
//! so nothing here touches a credential.
//!
//! What is sent, each frame in its page's example member order:
//!
//! - **A limit place**, `add_order` with the members QFEX's AsyncAPI `AddOrderCommand` documents
//!   ([trade.yaml](https://docs.qfex.com/websocket/trade.yaml), and the
//!   [add_order page](https://docs.qfex.com/websocket/channels/trade/add_order)): `symbol`,
//!   `side` (`BUY` or `SELL`), `order_type`, `order_time_in_force`, `quantity` and `price` as
//!   exact JSON numbers (lots times the size step, ticks times the grid's finest step, never an
//!   exponent), and `client_order_id`. Three shapes, from the
//!   [enums page](https://docs.qfex.com/api-reference/enums)'s OrderType and OrderTimeInForce and
//!   the [order-types page](https://docs.qfex.com/qfex/order-types)'s table, which admits ALO
//!   with GTC only: `LIMIT`/`GTC`, `ALO`/`GTC` for a post-only order, and `LIMIT`/`IOC`.
//!   `take_profit` and `stop_loss`, which the AsyncAPI leaves optional, are not written;
//!   neither is `reduce_only`, which the AsyncAPI's `AddOrderCommand` does not define and the
//!   add_order page shows as `0` in its example and as a boolean in its samples.
//! - **A cancel**, `cancel_order` by the venue's order UUID with `cancel_order_id_type`
//!   `order_id`, which "cancels exactly one order"
//!   ([cancel_order](https://docs.qfex.com/websocket/channels/trade/cancel_order)).
//! - **A query**, `get_order` by the venue's order UUID, the only reference its page documents
//!   ([get_order](https://docs.qfex.com/websocket/channels/trade/get_order)).
//!
//! What is refused, with nothing written: what QFEX leaves undocumented or contradictory, rather
//! than guessed.
//!
//! - `Unsupported`: a market order, a fill-or-kill, the RPI channel, a post-only order with any
//!   time in force but GTC (`FlagConflict` for IOC, the one other v1 time in force: the pair
//!   the order-types page refuses together), and reduce-only, until a capture settles its
//!   semantics; a place whose `order_type` or `order_time_in_force` its symbol's refdata does not
//!   list (decision 0136: the spec's `order_shapes`, from discovery, must list `LIMIT`, or `ALO`
//!   for a post-only order, and the time in force; a spec stating none lists nothing), which
//!   QFEX could refuse (the enums page names `INVALID_ORDER_TYPE` and `INVALID_TIME_IN_FORCE`)
//!   only once it was sent and charged; `modify_order`, until a capture settles whether its
//!   quantity is the total or the remaining quantity and how a partly filled order is modified
//!   (the enums page names `CANNOT_MODIFY_PARTIAL_FILL`); a cancel or query by client id, since
//!   a cancel by client id "may cancel multiple orders" of the symbol; every batch, cancel-many
//!   and cancel-all; the cancel-on-disconnect arm, on and off (FBC-mfey's, under decision 0132's
//!   probe mode only); and a dead-man refresh and a fee query, which QFEX has no request for.
//! - `Unencodable`: an instrument the spec table does not hold, a price off the grid or not
//!   positive, a quantity of no lots or outside the spec's minimum and maximum, a venue order id
//!   the one order-id rule refuses ([`super::is_venue_order_id`], which the private decoder reads
//!   `order_id` by too), and a client id that does not fit the client-id format
//!   ([`super::CLIENT_ID_FORMAT`], in which every `client_order_id` is written).
//!
//! Each frame's charge is in tenths of QFEX's weight unit ([rate
//! page](https://docs.qfex.com/websocket/rate)), on the provider user's shared buckets
//! ([`crate::trade_rate`], decisions 0123 and 0128): `add_order` 1.0 and `get_order` 2.0 on
//! the general bucket, `cancel_order` 1.0 on the cancel bucket, each naming its instrument.

use std::num::NonZeroU32;

use fbc_core::{
    CancelOrder, Channel, ClientOrderId, InstrumentId, InstrumentSpec, Lots, NewOrder,
    NotSentReason, OpKind, OrderKind, OrderKindTag, OrderRef, QueryOrder, RateCharge, Side,
    SpecTable, Ticks, Tif, VenueCommand, encode_cid,
};
use rust_decimal::Decimal;

use super::ids::{CLIENT_ID_FORMAT, is_venue_order_id};
use NotSentReason::{FlagConflict, Unencodable, Unsupported};

/// `add_order` and `cancel_order` weigh 1.0, ten tenths, on the rate page.
const ONE_UNIT: NonZeroU32 = NonZeroU32::new(10).expect("ten is not zero");
/// `get_order` weighs 2.0.
const TWO_UNITS: NonZeroU32 = NonZeroU32::new(20).expect("twenty is not zero");

/// One request: its frame and what QFEX charges for it.
pub(crate) struct Request {
    pub(crate) frame: Vec<u8>,
    pub(crate) charge: RateCharge,
}

/// The frame `cmd` is sent as, or why it is not sent.
pub(crate) fn request(cmd: &VenueCommand, specs: &SpecTable) -> Result<Request, NotSentReason> {
    match cmd {
        VenueCommand::Place(order) => place(order, specs),
        VenueCommand::Cancel(cancel) => cancel_order(cancel, specs),
        VenueCommand::Query(query) => get_order(query, specs),
        VenueCommand::Amend(_)
        | VenueCommand::PlaceBatch(_)
        | VenueCommand::CancelMany(_)
        | VenueCommand::CancelAll(_)
        | VenueCommand::ArmCancelOnDisconnect(_)
        | VenueCommand::RefreshDeadMan
        | VenueCommand::FeeQuery => Err(Unsupported),
    }
}

/// A place's QFEX `order_type` and `order_time_in_force`, its limit price, or why v1 does not
/// send its shape. Every admitted shape is named here and nowhere else.
fn shape(o: &NewOrder) -> Result<(&'static str, &'static str, Ticks), NotSentReason> {
    let OrderKind::Limit { px } = o.kind else {
        return Err(Unsupported);
    };
    if o.channel != Channel::Public || o.reduce_only {
        return Err(Unsupported);
    }
    match (o.post_only, o.tif) {
        (false, Tif::Gtc) => Ok(("LIMIT", "GTC", px)),
        (true, Tif::Gtc) => Ok(("ALO", "GTC", px)),
        (false, Tif::Ioc) => Ok(("LIMIT", "IOC", px)),
        (true, Tif::Ioc) => Err(FlagConflict),
        (_, Tif::Fok) => Err(Unsupported),
    }
}

fn place(o: &NewOrder, specs: &SpecTable) -> Result<Request, NotSentReason> {
    let (order_type, tif, px) = shape(o)?;
    let spec = spec(specs, o.inst)?;
    listed(spec, o.post_only, o.tif)?;
    let price = price(spec, px)?;
    let quantity = quantity(spec, o.qty)?;
    let cid = client_order_id(o.cid)?;
    let body = format!(
        r#"{{"type":"add_order","params":{{"symbol":{},"side":"{}","order_type":"{order_type}","order_time_in_force":"{tif}","quantity":{quantity},"price":{price},"client_order_id":{}}}}}"#,
        symbol(spec),
        side(o.side),
        json_string(&cid),
    );
    Ok(charged(body, OpKind::Place, o.inst, ONE_UNIT))
}

fn cancel_order(c: &CancelOrder, specs: &SpecTable) -> Result<Request, NotSentReason> {
    let order_id = venue_order_id(&c.target)?;
    let spec = spec(specs, c.inst)?;
    let body = format!(
        r#"{{"type":"cancel_order","params":{{"order_id":{},"symbol":{},"cancel_order_id_type":"order_id"}}}}"#,
        json_string(order_id),
        symbol(spec),
    );
    Ok(charged(body, OpKind::Cancel, c.inst, ONE_UNIT))
}

fn get_order(q: &QueryOrder, specs: &SpecTable) -> Result<Request, NotSentReason> {
    let order_id = venue_order_id(&q.target)?;
    let spec = spec(specs, q.inst)?;
    let body = format!(
        r#"{{"type":"get_order","params":{{"symbol":{},"order_id":{}}}}}"#,
        symbol(spec),
        json_string(order_id),
    );
    Ok(charged(body, OpKind::Query, q.inst, TWO_UNITS))
}

/// `Unsupported` unless the symbol's lists name a limit of this form and time in force: its
/// `order_types` `LIMIT`, or `ALO` for a post-only order, and its `order_time_in_force`
/// (decision 0136). QFEX's refdata states lists for every symbol, so a spec stating none is read
/// as listing nothing. An amend, once one is sent, takes the same check.
fn listed(spec: &InstrumentSpec, post_only: bool, tif: Tif) -> Result<(), NotSentReason> {
    let shapes = spec.order_shapes.ok_or(Unsupported)?;
    if !shapes.lists(OrderKindTag::Limit, post_only, tif) {
        return Err(Unsupported);
    }
    Ok(())
}

fn charged(body: String, op: OpKind, inst: InstrumentId, weight: NonZeroU32) -> Request {
    Request {
        frame: body.into_bytes(),
        charge: RateCharge {
            op,
            inst: Some(inst),
            weight,
        },
    }
}

fn spec(specs: &SpecTable, inst: InstrumentId) -> Result<&InstrumentSpec, NotSentReason> {
    specs.get(inst).ok_or(Unencodable)
}

fn symbol(spec: &InstrumentSpec) -> String {
    json_string(spec.venue_symbol.as_wire())
}

fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

/// The price at `px` as an exact decimal, when it is on the grid and positive.
fn price(spec: &InstrumentSpec, px: Ticks) -> Result<Decimal, NotSentReason> {
    if !spec.price_grid.valid_at(px) {
        return Err(Unencodable);
    }
    let price = spec
        .price_grid
        .px_of(px)
        .and_then(|price| price.to_decimal())
        .ok_or(Unencodable)?;
    if price <= Decimal::ZERO {
        return Err(Unencodable);
    }
    Ok(price.normalize())
}

/// `lots` as an exact decimal quantity, when it is at least one lot and within the spec's
/// minimum and maximum.
fn quantity(spec: &InstrumentSpec, lots: Lots) -> Result<Decimal, NotSentReason> {
    let n = lots.get();
    let below = n < spec.min_size.get().max(1);
    let above = spec.max_order_size.is_some_and(|max| n > max.get());
    if below || above {
        return Err(Unencodable);
    }
    Decimal::from(n)
        .checked_mul(spec.size_step.get())
        .map(|qty| qty.normalize())
        .ok_or(Unencodable)
}

/// Our client id as `client_order_id` carries it, in the crate's one client-id format.
fn client_order_id(cid: ClientOrderId) -> Result<String, NotSentReason> {
    encode_cid(&CLIENT_ID_FORMAT, cid)
        .map(|wire| wire.as_str().to_owned())
        .map_err(|_| Unencodable)
}

/// The venue's order id a cancel or query names: `Unsupported` for a reference that carries
/// none (QFEX cancels and queries by our client id are not sent), `Unencodable` for one the
/// order-id rule the private decoder shares refuses.
fn venue_order_id(target: &OrderRef) -> Result<&str, NotSentReason> {
    let id = target.venue().ok_or(Unsupported)?.as_str();
    if !is_venue_order_id(id) {
        return Err(Unencodable);
    }
    Ok(id)
}

/// `value` as a JSON string, escaped: a venue symbol comes from the venue.
fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("a string serializes")
}
