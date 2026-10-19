//! Binance USD-M's instrument discovery and its Java-era ticker rule (decisions 0003 and 0004,
//! design §4.4; the plan's shape is decision 0035).
//!
//! [`plan`] is one `GET /fapi/v1/exchangeInfo` under the REST base, a public request of weight
//! 1 against the IP's `REQUEST_WEIGHT` budget ([INFO] in `caps.rs`: "Request Weight: 1"); its
//! parser reads the answer inside a [`DecodeScope`] into one [`InstrumentSpecDraft`] per
//! `PERPETUAL` symbol. The answer is `{"symbols": [symbol, ...], ...}`; each symbol states its
//! rules as a list of `filters`, each named by its `filterType` ([COMMON]: "Filters"). A
//! perpetual that leaves out a field or a filter a draft is built from, or states it as `null`,
//! refuses the whole answer by name ([`PlanError::Missing`]): a filter as its type
//! (`PRICE_FILTER`), a filter's field as `<type>.<field>` (`LOT_SIZE.minQty`). A value stated
//! but not as documented is malformed and named the same way.
//!
//! What is read, and how:
//! - `contractType`: `PERPETUAL` is read. Every other value is passed over before anything else
//!   of its symbol is read: the delivery contracts (`CURRENT_MONTH`, `NEXT_MONTH`,
//!   `CURRENT_QUARTER`, `NEXT_QUARTER`, `PERPETUAL_DELIVERING`), `TRADIFI_PERPETUAL`, and the
//!   empty value a settled symbol carries. A value that is not a string is malformed.
//! - `status` ([COMMON]: "Contract status"): `TRADING` is [`TradingStatus::Trading`],
//!   `TRADING_CANCEL_ONLY` [`TradingStatus::CancelOnly`]; `PENDING_TRADING`, `TRADING_HALT`,
//!   `PRE_DELIVERING`, `DELIVERING`, `PRE_SETTLE` and `SETTLING` take no new orders here, so
//!   [`TradingStatus::Halted`]; `DELIVERED` and `CLOSE` [`TradingStatus::Delisted`]. Another value
//!   is malformed: nothing is read as trading that Binance does not say is.
//! - `symbol` through the decode scope; `baseAsset` and `quoteAsset` (the key, as written:
//!   `BTC`, `USDT`) and `marginAsset` (the settlement asset). A perpetual with an asset name an
//!   [`AssetSym`] cannot hold (more than eight bytes, or not printable ASCII; Binance lists
//!   `1000000MOGUSDT`, base `1000000MOG`, and `币安人生USDT`, base `币安人生`) cannot be keyed,
//!   so it alone is passed over before its symbol and filters are read: the consumer finds it
//!   not listed, and every other symbol is still discovered. An empty asset name is malformed.
//! - `PRICE_FILTER` `tickSize` is the fixed price grid ("disabled on value 0" is no grid, so 0 is
//!   malformed). `LOT_SIZE` `stepSize` is the size step; its `minQty`, a quantity is valid only
//!   at `minQty` plus whole steps, is the smallest order in whole steps (one off the step grid is
//!   malformed), and its `maxQty` the largest limit order, floored to whole steps.
//!   `MIN_NOTIONAL` `notional` is the smallest notional, in the quote asset. `MAX_NUM_ORDERS`
//!   `limit`, optional, is the most open orders; a symbol without it states none. Two filters of
//!   one type are malformed, since they could disagree.
//! - USD-M perpetuals are linear: one unit of size is one unit of the base, so the multiplier
//!   is 1. The symbol is all an order names, so there is no native id. `exchangeInfo` states no
//!   funding schedule (that is `GET /fapi/v1/fundingInfo`), no position limit (leverage brackets
//!   are per account), no single price band (`PERCENT_PRICE` is asymmetric) and no fee schedule:
//!   those are not stated.
//!
//! [`parse_fbc_common_symbol`] is FBC's Java-era rule: `X/USDT` is `XUSDT`, read as the key that
//! perpetual's draft states (`X`, `USDT`, perpetual).
//!
//! Pages cited, under `https://developers.binance.com/docs/derivatives/usds-margined-futures/`:
//! [INFO] `market-data/rest-api/Exchange-Information`; [COMMON] `common-definition`.

use core::num::NonZeroU32;
use core::time::Duration;

use fbc_core::{
    AssetKey, AssetSym, DecodeError, DecodeScope, Effect, Effects, FundingSpec, HttpMethod,
    HttpPlan, HttpRequest, HttpTag, InstrumentKind, InstrumentSpecDraft, Lots, Money, OpKind,
    PlanError, PriceGrid, RateCharge, SizeStep, SymbolError, TradingStatus, TrafficClass,
    VenueConfig, VenueError, WireSlice, WireUrl, common_symbol_parts,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::config::{KEY_SNAPSHOT_TIMEOUT, duration, rest_base};

/// The tag of discovery's one request.
pub const EXCHANGE_INFO_TAG: HttpTag = HttpTag(1);

/// The path of the exchange information under the REST base [INFO].
pub const EXCHANGE_INFO_PATH: &str = "/fapi/v1/exchangeInfo";

/// The request's documented weight [INFO].
const EXCHANGE_INFO_WEIGHT: NonZeroU32 = NonZeroU32::MIN;

/// Nanos per unit of [`Money`].
const NANOS: i64 = 1_000_000_000;

/// Discovery's plan under `cfg`: one `GET` of [`EXCHANGE_INFO_PATH`] under the REST base
/// ([`KEY_REST_BASE_URL`](crate::KEY_REST_BASE_URL)), waiting the REST timeout
/// ([`KEY_SNAPSHOT_TIMEOUT`]), charged its documented weight; read by [`decode_exchange_info`].
/// Only those two keys are read.
pub(crate) fn plan(cfg: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
    let url = format!("{}{EXCHANGE_INFO_PATH}", rest_base(cfg)?);
    let timeout = duration(cfg, KEY_SNAPSHOT_TIMEOUT)?;
    let mut fx = Effects::new();
    fx.push(request(WireUrl::plain(url), timeout));
    let plan = HttpPlan::new(fx, |responses, scope| {
        // The plan holds one request, so its parser is handed one response.
        let answer = responses.first().ok_or(PlanError::Answers)?;
        decode_exchange_info(answer.body, scope)
    });
    Ok(plan.expect("one HTTP request makes a valid plan"))
}

/// A public REST request: no header, no credential.
fn request(url: WireUrl, timeout: Duration) -> Effect {
    Effect::Http {
        tag: EXCHANGE_INFO_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout,
        class: TrafficClass::Normal,
        charge: RateCharge {
            op: OpKind::Rest,
            inst: None,
            weight: EXCHANGE_INFO_WEIGHT,
        },
    }
}

/// The drafts of an `exchangeInfo` answer's perpetuals, in the answer's order. Refused, naming
/// the part: a body that is not a JSON object, `symbols` missing or not a list, a symbol that is
/// not an object, a perpetual missing a required field or filter, and a value not as documented.
pub fn decode_exchange_info(
    body: &[u8],
    scope: &DecodeScope<'_>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let doc: Value =
        serde_json::from_slice(body).map_err(|_| malformed("exchangeInfo response is not JSON"))?;
    let doc = doc
        .as_object()
        .ok_or(malformed("exchangeInfo response is not an object"))?;
    let symbols = required(doc, "symbols")?
        .as_array()
        .ok_or(malformed("symbols"))?;
    let mut drafts = Vec::new();
    for symbol in symbols {
        let symbol = symbol
            .as_object()
            .ok_or(malformed("symbols: a symbol is not an object"))?;
        if let Some(draft) = draft(symbol, scope)? {
            drafts.push(draft);
        }
    }
    Ok(drafts)
}

/// One symbol as a draft, or `None` for a symbol that is not a `PERPETUAL`.
fn draft(
    m: &Map<String, Value>,
    scope: &DecodeScope<'_>,
) -> Result<Option<InstrumentSpecDraft>, PlanError> {
    if text(m, "contractType")? != "PERPETUAL" {
        return Ok(None);
    }
    let status = match text(m, "status")? {
        "TRADING" => TradingStatus::Trading,
        "TRADING_CANCEL_ONLY" => TradingStatus::CancelOnly,
        "PENDING_TRADING" | "TRADING_HALT" | "PRE_DELIVERING" | "DELIVERING" | "PRE_SETTLE"
        | "SETTLING" => TradingStatus::Halted,
        "DELIVERED" | "CLOSE" => TradingStatus::Delisted,
        _ => return Err(malformed("status").into()),
    };
    let base = asset_sym(m, "baseAsset")?;
    let quote = asset_sym(m, "quoteAsset")?;
    let settle_ccy = asset_sym(m, "marginAsset")?;
    // An asset name the core cannot hold: this perpetual alone is passed over (Codex
    // r4311120955, Reviewer B RB-1).
    let (Some(base), Some(quote), Some(settle_ccy)) = (base, quote, settle_ccy) else {
        return Ok(None);
    };
    let venue_symbol = scope.venue_symbol(text(m, "symbol")?)?;
    let asset = AssetKey {
        base,
        quote,
        kind: InstrumentKind::Perpetual,
    };
    let filters = Filters::read(required(m, "filters")?)?;

    let price = filters.required("PRICE_FILTER")?;
    let tick = decimal(price, "PRICE_FILTER.tickSize", "tickSize")?;
    let price_grid = PriceGrid::fixed(tick).map_err(|_| malformed("PRICE_FILTER.tickSize"))?;

    let lot = filters.required("LOT_SIZE")?;
    let step = decimal(lot, "LOT_SIZE.stepSize", "stepSize")?;
    let size_step = SizeStep::new(step).ok_or(malformed("LOT_SIZE.stepSize"))?;
    let min_qty = decimal(lot, "LOT_SIZE.minQty", "minQty")?;
    let min_size = (min_qty > Decimal::ZERO)
        .then(|| whole_steps(min_qty, step))
        .flatten()
        .ok_or(malformed("LOT_SIZE.minQty"))?;
    let max_qty = decimal(lot, "LOT_SIZE.maxQty", "maxQty")?;
    let max_order_size = floor_steps(max_qty, step).ok_or(malformed("LOT_SIZE.maxQty"))?;

    let notional = filters.required("MIN_NOTIONAL")?;
    let notional = decimal(notional, "MIN_NOTIONAL.notional", "notional")?;
    let min_notional = money(notional, asset.quote).ok_or(malformed("MIN_NOTIONAL.notional"))?;

    let max_open_orders = match filters.optional("MAX_NUM_ORDERS") {
        None => None,
        Some(f) => {
            let limit = field(f, "MAX_NUM_ORDERS.limit", "limit")?.as_u64();
            let limit = limit.and_then(|n| u32::try_from(n).ok());
            Some(limit.ok_or(malformed("MAX_NUM_ORDERS.limit"))?)
        }
    };

    Ok(Some(InstrumentSpecDraft {
        asset,
        venue_symbol,
        native_id: None,
        price_grid,
        quote_grid: None,
        size_step,
        min_size,
        min_notional: Some(min_notional),
        max_order_size: Some(max_order_size),
        position_limit: None,
        price_band: None,
        max_open_orders,
        multiplier: Decimal::ONE,
        settle_ccy,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status,
    }))
}

/// A symbol's filters by `filterType`, each type at most once.
struct Filters<'a>(Vec<(&'a str, &'a Map<String, Value>)>);

impl<'a> Filters<'a> {
    fn read(value: &'a Value) -> Result<Filters<'a>, PlanError> {
        let list = value.as_array().ok_or(malformed("filters"))?;
        let mut filters: Vec<(&str, &Map<String, Value>)> = Vec::with_capacity(list.len());
        for f in list {
            let f = f
                .as_object()
                .ok_or(malformed("filters: a filter is not an object"))?;
            let kind = f.get("filterType").and_then(Value::as_str);
            let kind = kind.ok_or(malformed("filters: filterType"))?;
            if filters.iter().any(|(k, _)| *k == kind) {
                return Err(malformed("filters: a filterType twice").into());
            }
            filters.push((kind, f));
        }
        Ok(Filters(filters))
    }

    fn optional(&self, kind: &str) -> Option<&'a Map<String, Value>> {
        self.0.iter().find(|(k, _)| *k == kind).map(|(_, f)| *f)
    }

    fn required(&self, kind: &'static str) -> Result<&'a Map<String, Value>, PlanError> {
        self.optional(kind).ok_or(PlanError::Missing(kind))
    }
}

fn malformed(part: &'static str) -> DecodeError {
    DecodeError::Malformed(part)
}

/// A field that must be stated, named `name` when it is not: missing, or `null`, refuses the
/// answer.
fn field<'a>(
    m: &'a Map<String, Value>,
    name: &'static str,
    key: &str,
) -> Result<&'a Value, PlanError> {
    m.get(key)
        .filter(|v| !v.is_null())
        .ok_or(PlanError::Missing(name))
}

/// A symbol's required field, named by its key.
fn required<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a Value, PlanError> {
    field(m, key, key)
}

/// A symbol's required string field.
fn text<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a str, PlanError> {
    Ok(required(m, key)?.as_str().ok_or(malformed(key))?)
}

/// A required asset as written, or `None` for a stated name an [`AssetSym`] cannot hold (more
/// than eight bytes, or not printable ASCII): a name Binance lists (`1000000MOG`, `币安人生`)
/// that the core cannot key. An empty name, or one that is not a string, is malformed.
fn asset_sym(m: &Map<String, Value>, key: &'static str) -> Result<Option<AssetSym>, PlanError> {
    let name = text(m, key)?;
    if name.is_empty() {
        return Err(malformed(key).into());
    }
    Ok(AssetSym::new(name))
}

/// A filter's required decimal string, exactly as written and not negative, named `name`.
fn decimal(m: &Map<String, Value>, name: &'static str, key: &str) -> Result<Decimal, PlanError> {
    let value = field(m, name, key)?.as_str().ok_or(malformed(name))?;
    let value = Decimal::from_str_exact(value).map_err(|_| malformed(name))?;
    if value.is_sign_negative() && !value.is_zero() {
        return Err(malformed(name).into());
    }
    Ok(value)
}

/// `qty` as a whole number of steps of `step`, or `None` when it is off the step grid.
fn whole_steps(qty: Decimal, step: Decimal) -> Option<Lots> {
    let count = qty.checked_div(step)?;
    count
        .fract()
        .is_zero()
        .then(|| count.to_i64())
        .flatten()
        .and_then(Lots::new)
}

/// `qty` in whole steps of `step`, floored: the largest whole-step size within it.
fn floor_steps(qty: Decimal, step: Decimal) -> Option<Lots> {
    qty.checked_div(step)?.floor().to_i64().and_then(Lots::new)
}

/// An amount of `asset`, exact in nanos.
fn money(amount: Decimal, asset: AssetSym) -> Option<Money> {
    let nanos = amount.checked_mul(Decimal::from(NANOS))?;
    let nanos = nanos.fract().is_zero().then(|| nanos.to_i128()).flatten()?;
    Some(Money::new(nanos, asset))
}

/// FBC's Java-era rule for Binance USD-M: `X/USDT` is `XUSDT`, read as the key that perpetual's
/// draft states, `X/USDT` perpetual. Every other ticker is refused: another quote as
/// [`SymbolError::Unmapped`] (under the seeded aliases, `USDC=USD` and `USDT=USD`, `X/USDC`
/// would resolve to the `XUSDT` listing, not `XUSDC`), another form as
/// [`SymbolError::NotCommonForm`].
pub(crate) fn parse_fbc_common_symbol(s: &str) -> Result<AssetKey, SymbolError> {
    let (base, quote) = common_symbol_parts(s)?;
    if quote.as_str() != "USDT" {
        return Err(SymbolError::Unmapped);
    }
    Ok(AssetKey {
        base,
        quote,
        kind: InstrumentKind::Perpetual,
    })
}
