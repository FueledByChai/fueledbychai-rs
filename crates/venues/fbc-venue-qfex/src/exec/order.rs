//! QFEX's order commands as trade-socket frames (FBC-blcw, BT-702), built from the command and
//! the spec table alone, on the owner's v1 scope of 2026-10-29. The codec ([`super::QfexExec`])
//! sends each as one frame naming its request. Only a probe's cancel is settled by a reply, the
//! `order_response` naming its order (decision 0132 item 8); every other request ends `Unknown`
//! at its deadline. QFEX signs no order frame: the HMAC signs only the authentication (`src/sign`),
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
//! Under `production_probe` with `qfex.exec.production_places` (decision 0148 item 4), the codec
//! first asks [`staged`], which refuses `Unsupported` every place but a post-only GTC or a limit
//! IOC on the public book, not reduce-only, of no more than the instrument's `min_size`; what it
//! admits is then built as above, so one below the minimum stays `Unencodable`.
//!
//! Each frame's charge is in tenths of QFEX's weight unit ([rate
//! page](https://docs.qfex.com/websocket/rate)), on the provider user's shared buckets
//! ([`crate::trade_rate`], decisions 0123 and 0128): `add_order` 1.0 and `get_order` 2.0 on
//! the general bucket, `cancel_order` 1.0 on the cancel bucket, each naming its instrument.

use std::num::NonZeroU32;

use fbc_core::{
    CancelOrder, Channel, ClientOrderId, InstrumentId, InstrumentSpec, Lots, NewOrder,
    NotSentReason, OpKind, OrderKind, OrderKindTag, OrderRef, QueryOrder, RateCharge, Side,
    SpecTable, Ticks, Tif, VenueCommand, VenueOrderId, encode_cid,
};
use rust_decimal::Decimal;

use super::ids::{CLIENT_ID_FORMAT, is_venue_order_id};
use NotSentReason::{FlagConflict, Unencodable, Unsupported};

/// `add_order` and `cancel_order` weigh 1.0, ten tenths, on the rate page.
const ONE_UNIT: NonZeroU32 = NonZeroU32::new(10).expect("ten is not zero");
/// `get_order` weighs 2.0.
const TWO_UNITS: NonZeroU32 = NonZeroU32::new(20).expect("twenty is not zero");

/// One request: its frame, what QFEX charges for it, and the venue order it names, which a
/// cancel and a query name and a place and the arm do not.
pub(crate) struct Request {
    pub(crate) frame: Vec<u8>,
    pub(crate) charge: RateCharge,
    pub(crate) target: Option<VenueOrderId>,
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

/// Decision 0148 item 4's fence under `production_probe` with `qfex.exec.production_places`:
/// `Ok` only for a post-only GTC or a limit IOC on the public book, not reduce-only, of no more
/// than the instrument's `min_size`; every other place is `Unsupported`, its flags read no
/// further, so a post-only IOC is not `FlagConflict` here. One below `min_size` goes on to
/// [`place`], which refuses it `Unencodable` as it does in every mode, and so does an
/// instrument the spec table does not hold. Whether the order reduces is not read: the flatten
/// IOC 0148 item 5 builds is reducing with no reduce-only flag. The minimum is the spec table's,
/// so the fence is only as good as that table, which the probe takes from this adapter's
/// discovery in the same run (0148 item 4). Its kind, channel and reduce-only checks repeat
/// refusals [`shape`] makes later, so the fence does not depend on `shape` staying as it is; the
/// unit tests below pin each check on its own, before the table is read.
pub(crate) fn staged(o: &NewOrder, specs: &SpecTable) -> Result<(), NotSentReason> {
    let shape = matches!(o.kind, OrderKind::Limit { .. })
        && o.channel == Channel::Public
        && !o.reduce_only
        && matches!((o.post_only, o.tif), (true, Tif::Gtc) | (false, Tif::Ioc));
    if !shape || o.qty.get() > spec(specs, o.inst)?.min_size.get() {
        return Err(Unsupported);
    }
    Ok(())
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
    Ok(charged(body, OpKind::Place, o.inst, ONE_UNIT, None))
}

fn cancel_order(c: &CancelOrder, specs: &SpecTable) -> Result<Request, NotSentReason> {
    let order_id = venue_order_id(&c.target)?;
    let spec = spec(specs, c.inst)?;
    let body = format!(
        r#"{{"type":"cancel_order","params":{{"order_id":{},"symbol":{},"cancel_order_id_type":"order_id"}}}}"#,
        json_string(order_id.as_str()),
        symbol(spec),
    );
    Ok(charged(
        body,
        OpKind::Cancel,
        c.inst,
        ONE_UNIT,
        Some(order_id.clone()),
    ))
}

fn get_order(q: &QueryOrder, specs: &SpecTable) -> Result<Request, NotSentReason> {
    let order_id = venue_order_id(&q.target)?;
    let spec = spec(specs, q.inst)?;
    let body = format!(
        r#"{{"type":"get_order","params":{{"symbol":{},"order_id":{}}}}}"#,
        symbol(spec),
        json_string(order_id.as_str()),
    );
    Ok(charged(
        body,
        OpKind::Query,
        q.inst,
        TWO_UNITS,
        Some(order_id.clone()),
    ))
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

fn charged(
    body: String,
    op: OpKind,
    inst: InstrumentId,
    weight: NonZeroU32,
    target: Option<VenueOrderId>,
) -> Request {
    Request {
        frame: body.into_bytes(),
        charge: RateCharge {
            op,
            inst: Some(inst),
            weight,
        },
        target,
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
fn venue_order_id(target: &OrderRef) -> Result<&VenueOrderId, NotSentReason> {
    let id = target.venue().ok_or(Unsupported)?;
    if !is_venue_order_id(id.as_str()) {
        return Err(Unencodable);
    }
    Ok(id)
}

/// `value` as a JSON string, escaped: a venue symbol comes from the venue.
fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("a string serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{AccountKey, CidMint, Namespace, NamespaceLease, WallNs};

    /// One client id minted under a held namespace lease in a directory of its own.
    fn cid() -> ClientOrderId {
        let dir = std::env::temp_dir().join(format!("fbc-qfex-staged-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), Namespace::new(7)).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_760_000_000_000_000_000));
        let cid = mint.mint().unwrap();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cid
    }

    /// A limit IOC on the public book, not reduce-only: a shape the fence admits.
    fn ioc(cid: ClientOrderId) -> NewOrder {
        NewOrder {
            cid,
            inst: InstrumentId::new(7),
            side: Side::Buy,
            qty: Lots::new(1).unwrap(),
            kind: OrderKind::Limit { px: Ticks(1) },
            tif: Tif::Ioc,
            channel: Channel::Public,
            post_only: false,
            reduce_only: false,
            reducing: false,
        }
    }

    /// On an empty spec table, an admitted shape reaches the table and is `Unencodable`, so each
    /// shape the fence refuses `Unsupported` there is refused by the fence's own check, whatever
    /// [`shape`] would answer it with later (decision 0148 item 4).
    #[test]
    fn the_fence_refuses_each_shape_by_its_own_check_before_the_table_is_read() {
        let cid = cid();
        let empty = SpecTable::new();
        let alo = NewOrder {
            tif: Tif::Gtc,
            post_only: true,
            ..ioc(cid)
        };
        assert_eq!(staged(&ioc(cid), &empty), Err(Unencodable));
        assert_eq!(staged(&alo, &empty), Err(Unencodable));
        for (what, o) in [
            (
                "market IOC",
                NewOrder {
                    kind: OrderKind::Market,
                    ..ioc(cid)
                },
            ),
            (
                "RPI limit IOC",
                NewOrder {
                    channel: Channel::Rpi,
                    ..ioc(cid)
                },
            ),
            (
                "RPI ALO GTC",
                NewOrder {
                    channel: Channel::Rpi,
                    ..alo
                },
            ),
            (
                "reduce-only limit IOC",
                NewOrder {
                    reduce_only: true,
                    ..ioc(cid)
                },
            ),
            (
                "reduce-only ALO GTC",
                NewOrder {
                    reduce_only: true,
                    ..alo
                },
            ),
            (
                "limit GTC",
                NewOrder {
                    tif: Tif::Gtc,
                    ..ioc(cid)
                },
            ),
            (
                "post-only IOC",
                NewOrder {
                    post_only: true,
                    ..ioc(cid)
                },
            ),
        ] {
            assert_eq!(staged(&o, &empty), Err(Unsupported), "{what}");
        }
    }
}
