//! Public QFEX instrument discovery (0117, 0118).
//!
//! [QFEX REST refdata](https://docs.qfex.com/api-reference/rest/market-data/refdata) returns
//! `{ "type": "refdata", "data": [...] }` with one entry per perpetual. Its schema lists the
//! required fields; this parser refuses a missing or
//! malformed field it uses rather than inventing a grid or quantity. `ACTIVE`, `INACTIVE` and
//! `DELISTED` map to Trading, Halted and Delisted respectively. The public reference data does
//! not specify funding, fees or position limits. Its changing absolute `min_price` and
//! `max_price` are not a static basis-point band, so `price_band` remains unset.
//!
//! QFEX's Java registry uppercased `BASE/QUOTE` and converted it to `BASE-QUOTE`; the matching
//! draft key keeps the venue's stated quote asset. The resolver's explicit alias table may
//! subsequently equate USD, USDT and USDC.

use std::time::Duration;

use fbc_core::{
    AssetKey, AssetSym, ConfigError, DecodeError, DecodeScope, Effect, Effects, FundingSpec,
    HttpMethod, HttpPlan, HttpRequest, HttpTag, InstrumentKind, InstrumentSpecDraft, Lots, OpKind,
    PlanError, PriceGrid, RateCharge, SizeStep, SymbolError, TradingStatus, TrafficClass,
    VenueConfig, VenueError, WireSlice, WireUrl, common_symbol_parts,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::{KEY_REST_TIMEOUT, KEY_REST_URL, valid_host_port};

/// The tag of the public reference-data request.
pub const REFDATA_TAG: HttpTag = HttpTag(1);

/// Its path under the REST origin.
pub const REFDATA_PATH: &str = "/refdata";

pub(crate) fn plan(cfg: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
    let url = refdata_url(cfg)?;
    let timeout = rest_timeout(cfg)?;
    let mut effects = Effects::new();
    effects.push(Effect::Http {
        tag: REFDATA_TAG,
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
    });
    Ok(HttpPlan::new(effects, |responses, scope| {
        let answer = responses.first().ok_or(PlanError::Answers)?;
        decode_refdata(answer.body, scope)
    })
    .expect("one request makes a valid plan"))
}

/// A configured HTTP origin, with no path, user, query or fragment. Plain HTTP is confined to
/// loopback test stubs; production refdata is HTTPS.
pub fn refdata_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
    let raw = cfg
        .get(KEY_REST_URL)
        .ok_or(ConfigError::Missing(KEY_REST_URL))?;
    let bad = || ConfigError::Invalid {
        key: KEY_REST_URL,
        reason: "expected an HTTPS origin, or HTTP loopback origin",
    };
    let origin = raw.strip_suffix('/').unwrap_or(raw);
    let (authority, plain) = if let Some(host) = origin.strip_prefix("https://") {
        (host, false)
    } else if let Some(host) = origin.strip_prefix("http://") {
        (host, true)
    } else {
        return Err(bad());
    };
    if !valid_host_port(authority) {
        return Err(bad());
    }
    let host = authority
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']').map(|(host, _)| host))
        .unwrap_or_else(|| authority.split(':').next().unwrap_or(""));
    if plain && host != "localhost" && host != "::1" && !host.starts_with("127.") {
        return Err(bad());
    }
    Ok(WireUrl::plain(format!("{origin}{REFDATA_PATH}")))
}

fn rest_timeout(cfg: &VenueConfig) -> Result<Duration, ConfigError> {
    let raw = cfg
        .get(KEY_REST_TIMEOUT)
        .ok_or(ConfigError::Missing(KEY_REST_TIMEOUT))?;
    let (number, unit) = if let Some(n) = raw.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1000u64)
    } else {
        ("", 0)
    };
    let ms = number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(unit))
        .ok_or(ConfigError::Invalid {
            key: KEY_REST_TIMEOUT,
            reason: "expected positive whole <n>ms or <n>s",
        })?;
    Ok(Duration::from_millis(ms))
}

/// The Java-era QFEX ticker rule: trimmed, uppercased `BASE/QUOTE` names the venue's
/// `BASE-QUOTE` perpetual. The core's common-symbol API accepts only slash form.
pub fn parse_fbc_common_symbol(s: &str) -> Result<AssetKey, SymbolError> {
    let normalized = s.trim().to_ascii_uppercase();
    let (base, quote) = common_symbol_parts(&normalized)?;
    Ok(AssetKey {
        base,
        quote,
        kind: InstrumentKind::Perpetual,
    })
}

/// Decode all listed perpetuals in response order. A malformed item refuses the entire
/// response; omission of a required field is named precisely.
pub fn decode_refdata(
    body: &[u8],
    scope: &DecodeScope<'_>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let doc: Value = serde_json::from_slice(body).map_err(|_| malformed("refdata JSON"))?;
    let doc = doc.as_object().ok_or(malformed("refdata object"))?;
    if text(doc, "type")? != "refdata" {
        return Err(malformed("type").into());
    }
    let data = required(doc, "data")?.as_array().ok_or(malformed("data"))?;
    let mut drafts = Vec::with_capacity(data.len());
    for entry in data {
        let entry = entry.as_object().ok_or(malformed("data item"))?;
        drafts.push(draft(entry, scope)?);
    }
    Ok(drafts)
}

fn draft(
    m: &Map<String, Value>,
    scope: &DecodeScope<'_>,
) -> Result<InstrumentSpecDraft, PlanError> {
    let status = match text(m, "status")? {
        "ACTIVE" => TradingStatus::Trading,
        "INACTIVE" => TradingStatus::Halted,
        "DELISTED" => TradingStatus::Delisted,
        _ => return Err(malformed("status").into()),
    };
    let base = asset(m, "base_asset")?;
    let quote = asset(m, "quote_asset")?;
    let settle_ccy = asset(m, "margin_asset")?;
    let venue_symbol = scope.venue_symbol(text(m, "symbol")?)?;
    // The REST response states the separate assets. A conflicting ticker must not be
    // silently used as the market-data subscription name for those assets.
    let expected = format!("{}-{}", base.as_str(), quote.as_str());
    if venue_symbol.as_wire() != expected {
        return Err(malformed("symbol").into());
    }
    let price_grid =
        PriceGrid::fixed(decimal(m, "tick_size")?).map_err(|_| malformed("tick_size"))?;
    let step = decimal(m, "lot_size")?;
    let size_step = SizeStep::new(step).ok_or(malformed("lot_size"))?;
    let minimum = decimal(m, "min_quantity")?;
    let minimum_steps = whole_steps(minimum, step).ok_or(malformed("min_quantity"))?;
    if minimum_steps.get() == 0 {
        return Err(malformed("min_quantity").into());
    }
    let maximum = decimal(m, "max_quantity")?;
    let maximum_steps = floor_steps(maximum, step).ok_or(malformed("max_quantity"))?;
    if maximum_steps.get() < minimum_steps.get() {
        return Err(malformed("max_quantity").into());
    }
    Ok(InstrumentSpecDraft {
        asset: AssetKey {
            base,
            quote,
            kind: InstrumentKind::Perpetual,
        },
        venue_symbol,
        // The live public response reuses clobPairId across distinct symbols, so it cannot
        // serve as an unambiguous native instrument key. Subscriptions name `symbol`.
        native_id: None,
        price_grid,
        quote_grid: None,
        size_step,
        min_size: minimum_steps,
        min_notional: None,
        max_order_size: Some(maximum_steps),
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        settle_ccy,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status,
    })
}

fn malformed(name: &'static str) -> DecodeError {
    DecodeError::Malformed(name)
}

fn required<'a>(m: &'a Map<String, Value>, name: &'static str) -> Result<&'a Value, PlanError> {
    m.get(name)
        .filter(|value| !value.is_null())
        .ok_or(PlanError::Missing(name))
}

fn text<'a>(m: &'a Map<String, Value>, name: &'static str) -> Result<&'a str, PlanError> {
    Ok(required(m, name)?.as_str().ok_or(malformed(name))?)
}

fn asset(m: &Map<String, Value>, name: &'static str) -> Result<AssetSym, PlanError> {
    AssetSym::new(text(m, name)?).ok_or(malformed(name).into())
}

fn decimal(m: &Map<String, Value>, name: &'static str) -> Result<Decimal, PlanError> {
    let value = Decimal::from_str_exact(text(m, name)?).map_err(|_| malformed(name))?;
    if value.is_sign_negative() && !value.is_zero() {
        return Err(malformed(name).into());
    }
    Ok(value)
}

fn whole_steps(qty: Decimal, step: Decimal) -> Option<Lots> {
    let count = qty.checked_div(step)?;
    if !count.fract().is_zero() {
        return None;
    }
    Lots::new(count.to_i64()?)
}

fn floor_steps(qty: Decimal, step: Decimal) -> Option<Lots> {
    Lots::new(qty.checked_div(step)?.floor().to_i64()?)
}
