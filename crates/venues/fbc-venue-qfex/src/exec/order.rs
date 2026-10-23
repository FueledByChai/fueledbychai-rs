//! Offline-only order wire encoder. QFEX has no universal request identifier in these
//! frames or their published replies, so a send only starts an RPC deadline; it never
//! establishes an accepted outcome. The containing factory still declares no exec caps.

use fbc_core::{
    Channel, ClientIdFormat, InstrumentSpec, Lots, NotSentReason, OpKind, OrderKind, OrderRef,
    RateCharge, Side, SpecTable, Tif, VenueCommand, encode_cid,
};
use rust_decimal::Decimal;
use std::num::NonZeroU32;

use NotSentReason::{Unencodable, Unsupported};

pub fn frame(
    cmd: &VenueCommand,
    specs: &SpecTable,
) -> Result<(Vec<u8>, RateCharge), NotSentReason> {
    let (body, op, inst) = match cmd {
        VenueCommand::Place(order) => {
            let spec = specs.get(order.inst).ok_or(Unencodable)?;
            let OrderKind::Limit { px: ticks } = order.kind else {
                return Err(Unsupported);
            };
            if order.tif != Tif::Gtc || order.channel != Channel::Public || order.post_only {
                return Err(Unsupported);
            }
            let px = price(spec, ticks)?;
            let qty = quantity(spec, order.qty)?;
            let cid = encode_cid(&ClientIdFormat::Uuid, order.cid).map_err(|_| Unencodable)?;
            let symbol = json_string(spec.venue_symbol.as_wire());
            let side = side(order.side);
            let cid = json_string(cid.as_str());
            (
                format!(
                    "{{\"type\":\"add_order\",\"params\":{{\"symbol\":{symbol},\"side\":\"{side}\",\"order_type\":\"LIMIT\",\"order_time_in_force\":\"GTC\",\"quantity\":{qty},\"price\":{px},\"client_order_id\":{cid},\"reduce_only\":{}}}}}",
                    order.reduce_only
                ),
                OpKind::Place,
                order.inst,
            )
        }
        VenueCommand::Amend(amend) => {
            if amend.tif != Tif::Gtc || amend.channel != Channel::Public || amend.post_only {
                return Err(Unsupported);
            }
            if amend.qty.get() <= amend.cum_filled.get() {
                return Err(Unencodable);
            }
            let spec = specs.get(amend.inst).ok_or(Unencodable)?;
            let venue_id = venue_uuid(&amend.target)?;
            let px = price(spec, amend.px)?;
            let qty = quantity(spec, amend.qty)?;
            let symbol = json_string(spec.venue_symbol.as_wire());
            let venue_id = json_string(venue_id);
            let side = side(amend.side);
            (
                format!(
                    "{{\"type\":\"modify_order\",\"params\":{{\"order_id\":{venue_id},\"symbol\":{symbol},\"side\":\"{side}\",\"order_type\":\"LIMIT\",\"quantity\":{qty},\"price\":{px},\"reduce_only\":{}}}}}",
                    amend.reduce_only
                ),
                OpKind::Amend,
                amend.inst,
            )
        }
        VenueCommand::Cancel(cancel) => {
            let spec = specs.get(cancel.inst).ok_or(Unencodable)?;
            let venue_id = json_string(venue_uuid(&cancel.target)?);
            let symbol = json_string(spec.venue_symbol.as_wire());
            (
                format!(
                    "{{\"type\":\"cancel_order\",\"params\":{{\"order_id\":{venue_id},\"symbol\":{symbol},\"cancel_order_id_type\":\"order_id\"}}}}"
                ),
                OpKind::Cancel,
                cancel.inst,
            )
        }
        VenueCommand::Query(query) => {
            // The published endpoint only accepts a venue order UUID. An Unknown place
            // without one cannot be resolved by guessing from its client id.
            let spec = specs.get(query.inst).ok_or(Unencodable)?;
            let venue_id = json_string(venue_uuid(&query.target)?);
            let symbol = json_string(spec.venue_symbol.as_wire());
            (
                format!(
                    "{{\"type\":\"get_order\",\"params\":{{\"symbol\":{symbol},\"order_id\":{venue_id}}}}}"
                ),
                OpKind::Query,
                query.inst,
            )
        }
        _ => return Err(Unsupported),
    };
    // QFEX documents get_order as two general-bucket units. Other supported
    // commands cost one unit in this offline codec; the final private limits
    // will scale all weights together before order entry is enabled.
    let charge = if op == OpKind::Query {
        RateCharge {
            op,
            inst: Some(inst),
            weight: NonZeroU32::new(2).expect("two is nonzero"),
        }
    } else {
        RateCharge::one(op, Some(inst))
    };
    Ok((body.into_bytes(), charge))
}

fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn quantity(spec: &InstrumentSpec, lots: Lots) -> Result<String, NotSentReason> {
    if lots.get() == 0
        || lots.get() < spec.min_size.get()
        || spec
            .max_order_size
            .is_some_and(|max| lots.get() > max.get())
    {
        return Err(Unencodable);
    }
    Decimal::from(lots.get())
        .checked_mul(spec.size_step.get())
        .map(|qty| qty.normalize().to_string())
        .ok_or(Unencodable)
}

fn price(spec: &InstrumentSpec, ticks: fbc_core::Ticks) -> Result<String, NotSentReason> {
    if !spec.price_grid.valid_at(ticks) {
        return Err(Unencodable);
    }
    let decimal = spec
        .price_grid
        .px_of(ticks)
        .and_then(|price| price.to_decimal())
        .ok_or(Unencodable)?;
    if decimal <= Decimal::ZERO {
        return Err(Unencodable);
    }
    Ok(decimal.normalize().to_string())
}

fn venue_uuid(target: &OrderRef) -> Result<&str, NotSentReason> {
    let id = target.venue().ok_or(Unsupported)?.as_str();
    if !valid_uuid_v4(id) {
        return Err(Unencodable);
    }
    Ok(id)
}

fn valid_uuid_v4(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, byte)| match i {
            8 | 13 | 18 | 23 => byte == b'-',
            14 => byte == b'4',
            19 => matches!(byte.to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b'),
            _ => byte.is_ascii_hexdigit(),
        })
}

fn json_string(value: &str) -> String {
    // Serializing a Rust string cannot fail and safely escapes provider-supplied symbols.
    serde_json::to_string(value).expect("a string is serializable")
}
