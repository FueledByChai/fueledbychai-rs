//! Paradex's instrument discovery and its Java-era ticker rule (decisions 0003 and 0004,
//! design §4.4).
//!
//! [`plan`] is one `GET /markets` under the REST base (docs.paradex.trade, "List available
//! markets"), a public request; [`decode_markets`] reads its answer inside a [`DecodeScope`]
//! into one [`InstrumentSpecDraft`] per order-book perpetual. The answer is
//! `{"results": [MarketResp, ...]}`; docs.paradex.trade marks every field of a `MarketResp`
//! optional, and the parser requires the ones a draft is built from ([`REQUIRED`]): a market
//! that leaves one out, or states it as `null`, refuses the whole answer by name
//! ([`PlanError::Missing`]). A value stated but not as documented is malformed and named.
//!
//! What is read, and how:
//! - `asset_kind`: `PERP` is read. `PERP_OPTION`, `OPTION`, `FUTURE` and `SPOT`, the other
//!   documented kinds, are not modelled here and are passed over before anything else of
//!   theirs is read; an undocumented kind is malformed.
//! - `trading_mode`: `STANDARD` is [`TradingStatus::Trading`]. `RFQ_ONLY` takes no order-book
//!   orders, the only orders this adapter places, so the market is passed over; another mode
//!   is malformed. `/markets` states no halt.
//! - `symbol` through the decode scope; `base_currency`, `quote_currency` (the key's quote:
//!   `USD`) and `settlement_currency` (`USDC`) as written.
//! - `price_tick_size` is the fixed price grid, `order_size_increment` the size step; the
//!   smallest order is one step, since `/markets` states no other minimum size.
//! - `min_notional` ("in USD") in the quote currency; `max_order_size` and `position_limit` ("in
//!   base currency") in whole steps, a limit off the step floored to the largest whole-step
//!   size within it.
//! - `price_bands_width` ("0.05 means 5%" from the mark) and `max_funding_rate` (per funding
//!   period) in basis points; `funding_period_hours` (a number, `8`) the funding interval, in
//!   whole seconds; `max_open_orders` a count.
//! - Paradex's perpetuals are linear: one unit of size is one unit of the base, so the
//!   multiplier is 1. The symbol is all an order names, so there is no native id. `fee_config`
//!   (an override of the account's rates) is not read: no public fee schedule is stated.
//!
//! [`parse_fbc_common_symbol`] is FBC's Java-era rule: `X/USDT` is `X-USD-PERP`, read as the key
//! that market's draft states (`X`, `USD`, perpetual).

use core::time::Duration;

use fbc_core::{
    AssetKey, AssetSym, Bps, ConfigError, DecodeError, DecodeScope, Effect, Effects, FundingSpec,
    HttpMethod, HttpPlan, HttpRequest, HttpTag, InstrumentKind, InstrumentSpecDraft, Lots, Money,
    OpKind, PlanError, PriceGrid, RateCharge, SizeStep, SymbolError, TradingStatus, TrafficClass,
    VenueConfig, VenueError, WireSlice, WireUrl, common_symbol_parts,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::auth::{self, REST_URL, TIMEOUT};
use crate::factory::duration;

/// The tag of discovery's one request.
pub const MARKETS_TAG: HttpTag = HttpTag(1);

/// The path of the market list under the REST base.
pub const MARKETS_PATH: &str = "/markets";

/// The fields of a perpetual's `MarketResp` a draft is built from: a market that leaves one
/// out is refused by name.
pub const REQUIRED: &[&str] = &[
    "symbol",
    "asset_kind",
    "trading_mode",
    "base_currency",
    "quote_currency",
    "settlement_currency",
    "price_tick_size",
    "order_size_increment",
    "min_notional",
    "max_order_size",
    "position_limit",
    "price_bands_width",
    "max_open_orders",
    "funding_period_hours",
    "max_funding_rate",
];

/// The `asset_kind`s docs.paradex.trade documents that are not perpetuals: passed over.
const OTHER_KINDS: &[&str] = &["PERP_OPTION", "OPTION", "FUTURE", "SPOT"];

/// Nanos per unit of [`Money`].
const NANOS: i64 = 1_000_000_000;

/// The market list's URL: the REST base ([`REST_URL`]) and [`MARKETS_PATH`]. The base is read
/// as src/auth reads it for the login: `https://`, or `http://` to a loopback test stub; a host,
/// an optional port and the path `/v1` alone; no query, fragment or user. Refused naming
/// [`REST_URL`], never echoing the value.
pub fn markets_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
    let text = cfg.get(REST_URL).ok_or(ConfigError::Missing(REST_URL))?;
    let invalid = |reason| ConfigError::Invalid {
        key: REST_URL,
        reason,
    };
    let base = text.trim_end_matches('/');
    let (after, plain) = match (base.strip_prefix("https://"), base.strip_prefix("http://")) {
        (Some(after), _) => (after, false),
        (None, Some(after)) => (after, true),
        (None, None) => {
            return Err(invalid(
                "not an https:// URL, or an http:// one to a loopback host",
            ));
        }
    };
    if after.contains(['?', '#', '@']) {
        return Err(invalid(
            "the adapter writes the path; give the API base with no query, fragment or user",
        ));
    }
    let (authority, path) = after.split_at(after.find('/').unwrap_or(after.len()));
    if authority.is_empty() || path != "/v1" {
        return Err(invalid("give a host and the path /v1 alone"));
    }
    if plain && !auth::is_loopback_authority(authority) {
        return Err(invalid(
            "http:// is for a loopback test stub only (127.0.0.0/8, ::1 or localhost): give an \
             https:// base",
        ));
    }
    Ok(WireUrl::plain(format!("{base}{MARKETS_PATH}")))
}

/// Discovery's one request: `GET /markets` at `url`, waiting `timeout` for its answer. A public
/// REST request: no header, no credential.
pub fn markets_request(url: WireUrl, timeout: Duration) -> Effect {
    Effect::Http {
        tag: MARKETS_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url,
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Rest, None),
    }
}

/// Discovery's plan under `cfg`: [`markets_request`] at [`markets_url`], waiting the REST
/// timeout ([`TIMEOUT`]), read by [`decode_markets`].
pub fn plan(cfg: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
    let url = markets_url(cfg)?;
    let text = cfg.get(TIMEOUT).ok_or(ConfigError::Missing(TIMEOUT))?;
    let timeout = duration(text).ok_or(ConfigError::Invalid {
        key: TIMEOUT,
        reason: "not a positive whole <n>s or <n>ms",
    })?;
    let mut fx = Effects::new();
    fx.push(markets_request(url, timeout));
    let plan = HttpPlan::new(fx, |responses, scope| {
        // The plan holds one request, so its parser is handed one response.
        let answer = responses.first().ok_or(PlanError::Answers)?;
        decode_markets(answer.body, scope)
    });
    Ok(plan.expect("one HTTP request makes a valid plan"))
}

/// The drafts of a `/markets` answer's order-book perpetuals, in the answer's order. Refused,
/// naming the part: a body that is not a JSON object, `results` missing or not a list, a market
/// that is not an object, a perpetual missing a [`REQUIRED`] field, and a value not as
/// documented.
pub fn decode_markets(
    body: &[u8],
    scope: &DecodeScope<'_>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let doc: Value = serde_json::from_slice(body)
        .map_err(|_| DecodeError::Malformed("markets response is not JSON"))?;
    let doc = doc
        .as_object()
        .ok_or(DecodeError::Malformed("markets response is not an object"))?;
    let results = required(doc, "results")?;
    let results = results
        .as_array()
        .ok_or(DecodeError::Malformed("results"))?;
    let mut drafts = Vec::with_capacity(results.len());
    for market in results {
        let market = market
            .as_object()
            .ok_or(DecodeError::Malformed("results: a market is not an object"))?;
        if let Some(draft) = draft(market, scope)? {
            drafts.push(draft);
        }
    }
    Ok(drafts)
}

/// One market as a draft, or `None` for a market this adapter does not trade: a kind other than
/// a perpetual, or an RFQ-only perpetual.
fn draft(
    m: &Map<String, Value>,
    scope: &DecodeScope<'_>,
) -> Result<Option<InstrumentSpecDraft>, PlanError> {
    match text(m, "asset_kind")? {
        "PERP" => {}
        kind if OTHER_KINDS.contains(&kind) => return Ok(None),
        _ => return Err(DecodeError::Malformed("asset_kind").into()),
    }
    let status = match text(m, "trading_mode")? {
        "STANDARD" => TradingStatus::Trading,
        "RFQ_ONLY" => return Ok(None),
        _ => return Err(DecodeError::Malformed("trading_mode").into()),
    };
    let venue_symbol = scope.venue_symbol(text(m, "symbol")?)?;
    let asset = AssetKey {
        base: asset_sym(m, "base_currency")?,
        quote: asset_sym(m, "quote_currency")?,
        kind: InstrumentKind::Perpetual,
    };
    let settle_ccy = asset_sym(m, "settlement_currency")?;
    let tick = decimal(m, "price_tick_size")?;
    let price_grid = PriceGrid::fixed(tick).map_err(|_| malformed("price_tick_size"))?;
    let step = decimal(m, "order_size_increment")?;
    let size_step = SizeStep::new(step).ok_or(malformed("order_size_increment"))?;
    let min_notional = money(m, "min_notional", asset.quote)?;
    let max_order_size = steps(m, "max_order_size", step)?;
    let position_limit = steps(m, "position_limit", step)?;
    let price_band = bps(m, "price_bands_width")?;
    let max_open_orders = required(m, "max_open_orders")?.as_u64();
    let max_open_orders = max_open_orders.and_then(|n| u32::try_from(n).ok());
    let max_open_orders = max_open_orders.ok_or(malformed("max_open_orders"))?;
    let funding = FundingSpec::Known {
        interval: hours(m, "funding_period_hours")?,
        cap: Some(bps(m, "max_funding_rate")?),
    };
    Ok(Some(InstrumentSpecDraft {
        asset,
        venue_symbol,
        native_id: None,
        price_grid,
        quote_grid: None,
        size_step,
        min_size: Lots::new(1).expect("one is not negative"),
        min_notional: Some(min_notional),
        max_order_size: Some(max_order_size),
        position_limit: Some(position_limit),
        price_band: Some(price_band),
        max_open_orders: Some(max_open_orders),
        multiplier: Decimal::ONE,
        settle_ccy,
        funding,
        public_fees: None,
        status,
    }))
}

fn malformed(part: &'static str) -> DecodeError {
    DecodeError::Malformed(part)
}

/// A field the market must state: missing, or `null`, refuses the answer by name.
fn required<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a Value, PlanError> {
    m.get(key)
        .filter(|v| !v.is_null())
        .ok_or(PlanError::Missing(key))
}

/// A required string field.
fn text<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a str, PlanError> {
    Ok(required(m, key)?.as_str().ok_or(malformed(key))?)
}

/// A required asset, a valid asset symbol as written.
fn asset_sym(m: &Map<String, Value>, key: &'static str) -> Result<AssetSym, PlanError> {
    Ok(AssetSym::new(text(m, key)?).ok_or(malformed(key))?)
}

/// A required decimal string, exactly as written.
fn decimal(m: &Map<String, Value>, key: &'static str) -> Result<Decimal, PlanError> {
    let value = Decimal::from_str_exact(text(m, key)?).map_err(|_| malformed(key))?;
    Ok(value)
}

/// A required decimal string that is not negative.
fn non_negative(m: &Map<String, Value>, key: &'static str) -> Result<Decimal, PlanError> {
    let value = decimal(m, key)?;
    if value.is_sign_negative() && !value.is_zero() {
        return Err(malformed(key).into());
    }
    Ok(value)
}

/// A required amount of `asset`, exact in nanos.
fn money(m: &Map<String, Value>, key: &'static str, asset: AssetSym) -> Result<Money, PlanError> {
    let nanos = non_negative(m, key)?.checked_mul(Decimal::from(NANOS));
    let nanos = nanos
        .filter(|n| n.fract().is_zero())
        .and_then(|n| n.to_i128());
    Ok(Money::new(nanos.ok_or(malformed(key))?, asset))
}

/// A required size in base units as whole steps of `step`, floored: the largest whole-step
/// size within it.
fn steps(m: &Map<String, Value>, key: &'static str, step: Decimal) -> Result<Lots, PlanError> {
    let count = non_negative(m, key)?.checked_div(step);
    let count = count.and_then(|c| c.floor().to_i64()).and_then(Lots::new);
    Ok(count.ok_or(malformed(key))?)
}

/// A required fraction (`0.05`) in basis points (500).
fn bps(m: &Map<String, Value>, key: &'static str) -> Result<Bps, PlanError> {
    let value = non_negative(m, key)?.checked_mul(Decimal::from(10_000));
    Ok(Bps(value.and_then(|v| v.to_f64()).ok_or(malformed(key))?))
}

/// A required positive number of hours, a whole number of seconds.
fn hours(m: &Map<String, Value>, key: &'static str) -> Result<Duration, PlanError> {
    let value = required(m, key)?;
    let hours = match value {
        Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    };
    let secs = hours.and_then(|h| h.checked_mul(Decimal::from(3600)));
    let secs = secs.filter(|s| s.fract().is_zero() && *s > Decimal::ZERO);
    let secs = secs.and_then(|s| s.to_u64()).ok_or(malformed(key))?;
    Ok(Duration::from_secs(secs))
}

/// FBC's Java-era rule for Paradex: `X/USDT` is `X-USD-PERP`, read as the key that market's
/// draft states, `X/USD` perpetual. Every other ticker is refused: another quote as
/// [`SymbolError::Unmapped`], another form as [`SymbolError::NotCommonForm`].
pub fn parse_fbc_common_symbol(s: &str) -> Result<AssetKey, SymbolError> {
    let (base, quote) = common_symbol_parts(s)?;
    if quote.as_str() != "USDT" {
        return Err(SymbolError::Unmapped);
    }
    let usd = AssetSym::new("USD").expect("USD is a valid asset symbol");
    Ok(AssetKey {
        base,
        quote: usd,
        kind: InstrumentKind::Perpetual,
    })
}
