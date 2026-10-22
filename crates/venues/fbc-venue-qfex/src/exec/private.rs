//! Documented private wire shapes, inspected offline only. No value here is an order,
//! fill, fee, or trustworthy resync until private conformance establishes its semantics.

use std::collections::HashSet;
use std::str::FromStr;

use fbc_core::{DecodeScope, FillId, VenueOrderId};
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub(super) enum PrivateTruth {
    Fill {
        trade: FillId,
        order: VenueOrderId,
        /// QFEX documents a number but not its currency or positive/negative meaning.
        raw_fee: Decimal,
    },
    /// `all_orders_response` is paginated, open-only and has no snapshot watermark.
    OpenOrdersPage {
        orders: Vec<VenueOrderId>,
    },
    /// `user_trades` omits our side and does not identify a stable page boundary.
    TradesPage {
        trades: Vec<FillId>,
    },
    /// A single symbol pulse, without an account-wide completion marker.
    PositionPulse,
    Other,
}

/// Validate identities and numeric wire fields while deliberately retaining only evidence
/// that can be interpreted without fee-sign, page-completeness, or position-scope guesses.
pub(super) fn inspect(
    value: &Value,
    scope: &DecodeScope<'_>,
) -> Result<PrivateTruth, &'static str> {
    let object = value.as_object().ok_or("private envelope")?;
    let recognized = [
        "fill_response",
        "all_orders_response",
        "user_trades",
        "position_response",
    ]
    .iter()
    .filter(|name| object.contains_key(**name))
    .count();
    if recognized > 1 {
        return Err("ambiguous private envelope");
    }
    if let Some(fill) = object.get("fill_response") {
        positive_decimal(fill, "price")?;
        positive_decimal(fill, "quantity")?;
        nonnegative_decimal(fill, "timestamp")?;
        side(fill)?;
        let trade = fill_id(fill, "trade_id", scope)?;
        let order = order_id(fill, "order_id", scope)?;
        return Ok(PrivateTruth::Fill {
            trade,
            order,
            raw_fee: decimal(fill, "fee")?,
        });
    }
    if let Some(page) = object.get("all_orders_response") {
        let rows = page
            .get("orders")
            .and_then(Value::as_array)
            .ok_or("orders page")?;
        // TWAPs are a separate open-order class. A page containing them is still partial.
        page.get("twaps")
            .or_else(|| page.get("stop_orders"))
            .and_then(Value::as_array)
            .ok_or("orders page companion")?;
        let mut ids = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();
        for row in rows {
            let id = order_id(row, "order_id", scope)?;
            if !seen.insert(id.clone()) {
                return Err("duplicate order id in page");
            }
            symbol(row)?;
            side(row)?;
            positive_decimal(row, "quantity")?;
            nonnegative_decimal(row, "quantity_remaining")?;
            ids.push(id);
        }
        return Ok(PrivateTruth::OpenOrdersPage { orders: ids });
    }
    if let Some(rows) = object.get("user_trades") {
        let rows = rows.as_array().ok_or("trades page")?;
        object
            .get("count")
            .and_then(Value::as_u64)
            .ok_or("trades count")?;
        let mut ids = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();
        for row in rows {
            let id = fill_id(row, "trade_id", scope)?;
            if !seen.insert(id.clone()) {
                return Err("duplicate trade id in page");
            }
            order_id(row, "order_id", scope)?;
            symbol(row)?;
            positive_decimal(row, "price")?;
            positive_decimal(row, "quantity")?;
            nonnegative_decimal(row, "timestamp")?;
            decimal(row, "fee")?;
            ids.push(id);
        }
        return Ok(PrivateTruth::TradesPage { trades: ids });
    }
    if let Some(position) = object.get("position_response") {
        symbol(position)?;
        decimal(position, "position")?;
        return Ok(PrivateTruth::PositionPulse);
    }
    Ok(PrivateTruth::Other)
}

fn uuid<'a>(row: &'a Value, field: &str) -> Result<&'a str, &'static str> {
    let id = row
        .get(field)
        .and_then(Value::as_str)
        .ok_or("missing UUID")?;
    // This checks UUID wire form only; no protocol guarantee restricts trade ids to v4.
    if id.len() != 36
        || !id.bytes().enumerate().all(|(i, byte)| match i {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
    {
        return Err("invalid UUID");
    }
    Ok(id)
}

fn fill_id(row: &Value, field: &str, scope: &DecodeScope<'_>) -> Result<FillId, &'static str> {
    scope.fill_id(uuid(row, field)?).map_err(|_| "fill id")
}

fn order_id(
    row: &Value,
    field: &str,
    scope: &DecodeScope<'_>,
) -> Result<VenueOrderId, &'static str> {
    scope
        .venue_order_id(uuid(row, field)?)
        .map_err(|_| "order id")
}

fn symbol(row: &Value) -> Result<&str, &'static str> {
    row.get("symbol")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("symbol")
}

fn side(row: &Value) -> Result<(), &'static str> {
    match row.get("side").and_then(Value::as_str) {
        Some("BUY" | "SELL") => Ok(()),
        _ => Err("side"),
    }
}

fn decimal(row: &Value, field: &str) -> Result<Decimal, &'static str> {
    let number = row.get(field).and_then(Value::as_number).ok_or("number")?;
    Decimal::from_str(&number.to_string()).map_err(|_| "decimal")
}

fn positive_decimal(row: &Value, field: &str) -> Result<Decimal, &'static str> {
    decimal(row, field).and_then(|value| (value > Decimal::ZERO).then_some(value).ok_or("positive"))
}

fn nonnegative_decimal(row: &Value, field: &str) -> Result<Decimal, &'static str> {
    decimal(row, field).and_then(|value| {
        (value >= Decimal::ZERO)
            .then_some(value)
            .ok_or("nonnegative")
    })
}

#[cfg(test)]
mod tests {
    use fbc_core::{VenueConfig, VenueFactory, dispatch_market_data};
    use rust_decimal::Decimal;
    use serde_json::{Value, json};

    use super::{PrivateTruth, inspect};
    use crate::{KEY_MDS_URL, Qfex};

    fn with_scope<T>(f: impl for<'a> FnOnce(&fbc_core::DecodeScope<'a>) -> T) -> T {
        let mut cfg = VenueConfig::new();
        cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
        let caps = Qfex.caps(&cfg).unwrap();
        dispatch_market_data(&caps, f)
    }

    fn fixture(raw: &str) -> Value {
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn fill_keeps_uuid_identity_and_raw_fee_without_normalizing_cost() {
        let mut fill = fixture(include_str!(
            "../../../../../fixtures/qfex/private/fill_response.json"
        ));
        with_scope(|scope| {
            let trade = scope
                .fill_id("92adf4db-1e48-40cf-9e2a-b0ecf079a16a")
                .unwrap();
            let order = scope
                .venue_order_id("5b309929-206f-40ec-804d-cbe46e81afc1")
                .unwrap();
            assert_eq!(
                inspect(&fill, scope),
                Ok(PrivateTruth::Fill {
                    trade: trade.clone(),
                    order: order.clone(),
                    raw_fee: Decimal::new(2, 3),
                })
            );
            assert!(
                scope
                    .fee(2_000_000, fbc_core::AssetSym::new("USD").unwrap())
                    .is_err()
            );
            fill["fill_response"]["fee"] = json!(-0.002);
            assert_eq!(
                inspect(&fill, scope),
                Ok(PrivateTruth::Fill {
                    trade,
                    order,
                    raw_fee: Decimal::new(-2, 3),
                })
            );
            fill["fill_response"]["trade_id"] = json!("REDACTED");
            assert_eq!(inspect(&fill, scope), Err("invalid UUID"));
        });
    }

    #[test]
    fn pages_and_pulse_never_claim_a_complete_resync() {
        let orders = fixture(include_str!(
            "../../../../../fixtures/qfex/private/open_orders_page.json"
        ));
        let trades = fixture(include_str!(
            "../../../../../fixtures/qfex/private/user_trades_page.json"
        ));
        let position = fixture(include_str!(
            "../../../../../fixtures/qfex/private/position_pulse.json"
        ));
        with_scope(|scope| {
            assert_eq!(
                inspect(&orders, scope),
                Ok(PrivateTruth::OpenOrdersPage {
                    orders: vec![
                        scope
                            .venue_order_id("2f05173c-992a-426b-bcaf-6b341644bb62")
                            .unwrap()
                    ],
                })
            );
            assert_eq!(
                inspect(&trades, scope),
                Ok(PrivateTruth::TradesPage {
                    trades: vec![
                        scope
                            .fill_id("92adf4db-1e48-40cf-9e2a-b0ecf079a16a")
                            .unwrap()
                    ],
                })
            );
            assert_eq!(inspect(&position, scope), Ok(PrivateTruth::PositionPulse));
            assert_eq!(
                inspect(&json!({"all_orders_response":{"orders":[]}}), scope),
                Err("orders page companion")
            );
            assert_eq!(
                inspect(&json!({"user_trades":[],"count":-1}), scope),
                Err("trades count")
            );
        });
    }

    #[test]
    fn conflicting_envelopes_and_duplicate_ids_fail_closed() {
        let mut fill = fixture(include_str!(
            "../../../../../fixtures/qfex/private/fill_response.json"
        ));
        let mut orders = fixture(include_str!(
            "../../../../../fixtures/qfex/private/open_orders_page.json"
        ));
        with_scope(|scope| {
            fill["position_response"] = json!({"symbol":"AAPL-USD","position":0});
            assert_eq!(inspect(&fill, scope), Err("ambiguous private envelope"));
            let duplicate = orders["all_orders_response"]["orders"][0].clone();
            orders["all_orders_response"]["orders"]
                .as_array_mut()
                .unwrap()
                .push(duplicate);
            assert_eq!(inspect(&orders, scope), Err("duplicate order id in page"));
        });
    }

    #[test]
    fn malformed_fill_and_trade_history_refuse_without_inferring_side_or_fee() {
        let original = fixture(include_str!(
            "../../../../../fixtures/qfex/private/fill_response.json"
        ));
        let mut trades = fixture(include_str!(
            "../../../../../fixtures/qfex/private/user_trades_page.json"
        ));
        with_scope(|scope| {
            for (field, replacement, expected) in [
                ("side", json!("UNKNOWN"), "side"),
                ("price", json!(0), "positive"),
                ("quantity", json!(-1), "positive"),
                ("timestamp", json!(-1), "nonnegative"),
                ("fee", Value::Null, "number"),
                ("order_id", json!("bad"), "invalid UUID"),
            ] {
                let mut fill = original.clone();
                fill["fill_response"][field] = replacement;
                assert_eq!(inspect(&fill, scope), Err(expected), "{field}");
            }
            let duplicate = trades["user_trades"][0].clone();
            trades["user_trades"]
                .as_array_mut()
                .unwrap()
                .push(duplicate);
            assert_eq!(inspect(&trades, scope), Err("duplicate trade id in page"));
        });
    }
}
