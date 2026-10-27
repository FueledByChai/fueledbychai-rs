//! Hibachi's instrument discovery and its Java-era ticker rule (decisions 0003, 0004, 0035 and
//! 0125).
//!
//! [`plan`] is one public `GET /market/exchange-info` on the market-data REST origin
//! ([`KEY_DATA_REST_URL`](crate::KEY_DATA_REST_URL)), with no header and no credential, charged
//! one request against the IP's REST limit (`caps.rs`, [LIMITS]). Its parser,
//! [`decode_exchange_info`], reads the answer inside a [`DecodeScope`] into one
//! [`InstrumentSpecDraft`] per entry of `futureContracts`, in the answer's order. The answer's
//! shape is the Market API spec's `ExchangeInfoResponse`, `ContractData` and `ExchangeStatus`
//! ([SPEC]); the SDK's `FutureContract` and `ExchangeInfo` ([SDK]) agree with it on the fields
//! read here. So does the example on [AD]'s `/market/exchange-info`, but for its older contract
//! shape, `"live": true` and no `status` (below); [AD]'s prose names the exchange-level field
//! `maintenanceStatus`, while its own example, [SPEC] and [SDK] all state it as `status`.
//!
//! A contract that leaves out a required field, or states it `null`, refuses the whole answer by
//! name ([`PlanError::Missing`]); a value stated but not as documented refuses it as malformed,
//! named the same way. One answer is one result (0035), so a contract is never dropped quietly.
//!
//! What is read, and how:
//!
//! - The exchange's `status` ([SPEC] `ExchangeStatus`: `NORMAL`, `SCHEDULED_MAINTENANCE`,
//!   `UNSCHEDULED_MAINTENANCE`): required, refused as missing or malformed under the name
//!   `exchange status`, since a contract has a `status` of its own. Any value but `NORMAL`, the
//!   SDK's `NORMAL, MAINTENANCE` comment included, makes every contract
//!   [`TradingStatus::Halted`]. `currentMaintenanceWindow` and `maintenanceWindow` are not read
//!   here: the re-read of `/market/exchange-info` (FBC-1632) reads them and halts each market at
//!   a window's begin. So no contract's session calendar holds them as `ClosedWindow`s yet, which
//!   decision 0125's Consequences expect; FBC-ord5 maps them.
//! - A contract's `status` ([SPEC] `ContractStatus`: `LISTED`, `PRE_OPEN`, `LIVE`, `PAUSED`,
//!   `CLOSED`, none described), matched exactly: `LIVE` is [`TradingStatus::Trading`] while the
//!   exchange is `NORMAL`; `LISTED`, `PRE_OPEN`, `PAUSED` and `CLOSED` are each
//!   [`TradingStatus::Halted`], never a narrower state guessed at (Hibachi's FX docs, [FX]: the
//!   weekend halt "transitions to PAUSED"). Any other value, a lowercase `live` included, is
//!   malformed, and it is checked while the exchange is in maintenance too. An older answer
//!   shape that states `"live": true` and no `status` ([AD]'s example) is refused as missing:
//!   `live` is not read. Nothing else makes a contract trading: not `marketOpenTimestamp`, not
//!   its session calendar.
//! - `id`, a whole number within `u32` ([SPEC] `u32`), is the contract's native id
//!   ([`VenueNativeId::Number`]), the contract id Hibachi's signer packs; `symbol` is the venue
//!   symbol, `<underlyingSymbol>/<quote>-P` as Hibachi spells every perpetual (`ETH/USDT-P`),
//!   otherwise malformed; the asset key is (`underlyingSymbol`, that quote, perpetual), and
//!   `settlementSymbol` the settlement asset. Every contract is a linear perpetual: the
//!   multiplier is 1, as the Java library states it (`HibachiRestApi.java` L389, [JAVA]).
//! - `tickSize` is the fixed price grid, the prices orders are accepted at. The quote grid is the
//!   finest `orderbookGranularities` entry at or above `tickSize`, the grid the public book is
//!   aggregated at ([JAVA] `resolveEffectiveTickSize`, L362-L371 and L394-L435; its tests,
//!   `HibachiRestApiTickTest.java`), where it is coarser than the tick (ETH's 0.01 over a tick of
//!   0.000001), and `None` where it is the tick itself, as `fbc-core` defines `quote_grid` (the
//!   book on the acceptance grid). It is kept exactly as listed, so it names the granularity an
//!   orderbook subscription asks for. An entry that is not a plain decimal string greater than
//!   zero is passed over, as [JAVA] passes over a blank, unparsable or non-positive one. A plain
//!   decimal string is ASCII digits with at most one decimal point and a digit on each side of
//!   it, so a JSON number, an exponent, a sign, a digit separator, a space or a bare leading or
//!   trailing point is passed over too: [JAVA]'s `BigDecimal` reads some of those, and this is
//!   the stricter side. A contract with no entry at or above its tick (none listed, none well
//!   formed, or all finer) is quoted on its tick, as [JAVA] falls back to it: its `quote_grid` is
//!   `None` too (the crate docs say what FBC-2kmp must check before trusting such a contract's
//!   book). It is not refused: a discovery answer is one result (0035), and refusing it would
//!   refuse every other contract's too. `orderbookGranularities` itself is required, and must be
//!   a list.
//! - `stepSize` is the size step; `minOrderSize`, the smallest order, in whole steps (one off the
//!   step grid, or zero, is malformed); `minNotional`, the smallest notional, in the quote asset.
//!   These are strings in [SPEC], read exactly; one that is not a plain decimal string (as
//!   above, so a negative one too) or not a string at all is malformed.
//! - `underlyingDecimals` and `settlementDecimals` (whole numbers, [SPEC] `minimum: 0`; above 255
//!   malformed) are the signing parameters ([`SigningParams::AssetDecimals`], 0125).
//! - `marketOpenTimestamp` and `marketCloseTimestamp` ([SPEC]: a string or null, no unit, no
//!   description; [SDK] `FutureContract` optional strings) are each read as Unix seconds in a
//!   decimal string, the form [SDK]'s test data and [AD]'s example give the sibling
//!   `marketCreationTimestamp` (`"1727701319.73488"`). Absent or null advertises nothing. One
//!   that is not a plain decimal string (as above), or that read as seconds lies after
//!   2100-01-01T00:00:00Z (as a count of milliseconds would), refuses the answer as malformed
//!   naming the field. A close is the calendar's final close, an open its first open, each
//!   rounded toward closed to whole nanoseconds; the parser reads no clock (0035), so the
//!   calendar closes the contract from its close on and before its open, and the re-read
//!   (FBC-1632) halts the market at that close.
//! - An FX contract, one whose `underlyingSymbol` is in [`FX_UNDERLYINGS`], closes every week
//!   from Friday 17:00 to Sunday 17:00 New York time ([`FX_WEEKEND`], [FX]). No field of the
//!   answer marks a contract FX, so the underlyings [FX] names as FX pairs are listed here; a
//!   pair it does not name has no weekend calendar until it is added, and the re-read still
//!   halts it when Hibachi shows it `PAUSED`. The weekend holds through the call auction before
//!   each reopening ([AUCTION]: its "resumption after a trading halt (24/5 FX markets)"; about 15
//!   minutes in its EUR/USD example, no fixed duration stated), which trades nothing.
//! - Not stated, so `None`: the largest order, the position limit (free margin times leverage,
//!   per account, [LIMITS]), a price band (the 50 bps limit on [LIMITS] is market orders'), the
//!   open-order limit and the fee schedule (`feeConfig` is not read). Funding is
//!   [`FundingSpec::Unknown`] until FBC-204c shows that the rates Hibachi publishes are the
//!   hourly rate its Funding page defines (the crate docs' Funding section; the settlement
//!   interval is [`FUNDING_SETTLEMENT_INTERVAL`](crate::FUNDING_SETTLEMENT_INTERVAL)).
//!   `displayName`, the margin and risk factors, `marketCreationTimestamp`, `feeConfig` and
//!   `instantWithdrawalLimit` are not read.
//!
//! [`parse_fbc_common_symbol`] is FBC's Java-era rule: `X/USDT` is `X/USDT-P`, as is the Java
//! library's own spelling `X/USDT-P`, each trimmed and uppercased as the Java registry reads it
//! (`HibachiTickerRegistry.java` L59-L72, [JAVA]).
//!
//! Sources:
//! [SPEC] <https://hibachi-docs.redocly.app/marketapi/market> (its OpenAPI document,
//! `https://hibachi-docs.redocly.app/_spec/marketapi/index.yaml`);
//! [SDK] <https://github.com/hibachi-xyz/hibachi_sdk/blob/14a76b95230869dd622c31a1fc4132a6eac1a9fa/python/hibachi_xyz/types.py>
//! and its `tests/unit/data/response.exchange_info.0.json`;
//! [AD] <https://api-doc.hibachi.xyz/>, Market API, `/market/exchange-info`;
//! [JAVA] <https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api/HibachiRestApi.java>
//! and `commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/HibachiTickerRegistry.java`
//! at that commit;
//! [FX] <https://docs.hibachi.xyz/hibachi-docs/trading/fx-trading>;
//! [AUCTION] <https://docs.hibachi.xyz/hibachi-docs/trading/call-auctions>;
//! [LIMITS] <https://docs.hibachi.xyz/hibachi-docs/trading/limits>;
//! US daylight time since 2007, `EST5EDT,M3.2.0/2,M11.1.0/2`:
//! <https://github.com/eggert/tz/blob/2026e/northamerica#L193-L194>.

use core::time::Duration;

use fbc_core::{
    AssetKey, AssetSym, CalendarParts, DecodeError, DecodeScope, DstRule, Effect, Effects,
    FundingSpec, HttpMethod, HttpPlan, HttpRequest, HttpTag, InstrumentKind, InstrumentSpecDraft,
    LocalTime, Lots, Money, Month, OpKind, PlanError, PriceGrid, RateCharge, SessionCalendar,
    SigningParams, SizeStep, SymbolError, TradingStatus, TrafficClass, Transition, UtcOffsetRule,
    VenueConfig, VenueError, VenueNativeId, WallNs, WeekOfMonth, WeekTime, Weekday, WeeklyClosure,
    WireSlice, WireUrl, common_symbol_parts,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value};

use crate::config::{rest_origin, rest_timeout};

/// The tag of discovery's one request.
pub const EXCHANGE_INFO_TAG: HttpTag = HttpTag(1);

/// The exchange information's path on the market-data REST origin [SPEC].
pub const EXCHANGE_INFO_PATH: &str = "/market/exchange-info";

/// The underlyings of the contracts that close for the FX weekend: those of the pairs Hibachi's
/// FX docs name, "EUR/USD and GBP/USD" ([FX], Accounts segregation).
pub const FX_UNDERLYINGS: &[&str] = &["EUR", "GBP"];

/// New York's offset rule: UTC-5, and UTC-4 from the second Sunday of March at 02:00 to the
/// first Sunday of November at 02:00, US law since 2007 (`EST5EDT,M3.2.0/2,M11.1.0/2`; tz
/// `northamerica` L193-L194).
pub const NEW_YORK: UtcOffsetRule = UtcOffsetRule::with_dst(
    -18_000,
    DstRule::new(
        3_600,
        Transition {
            month: Month::March,
            week: WeekOfMonth::Second,
            weekday: Weekday::Sunday,
            at: LocalTime::hms(2, 0, 0).unwrap(),
        },
        Transition {
            month: Month::November,
            week: WeekOfMonth::First,
            weekday: Weekday::Sunday,
            at: LocalTime::hms(2, 0, 0).unwrap(),
        },
    )
    .unwrap(),
)
.unwrap();

/// FX pairs "are tradable 24/5, with a weekend trading halt from Friday 17:00 ET to Sunday 17:00
/// ET" ([FX], 24/5 trading).
pub const FX_WEEKEND: WeeklyClosure = WeeklyClosure::new(
    NEW_YORK,
    WeekTime {
        weekday: Weekday::Friday,
        at: LocalTime::hms(17, 0, 0).unwrap(),
    },
    WeekTime {
        weekday: Weekday::Sunday,
        at: LocalTime::hms(17, 0, 0).unwrap(),
    },
)
.unwrap();

/// The latest instant a contract may advertise, 2100-01-01T00:00:00Z, in Unix seconds.
const LATEST_SECS: i64 = 4_102_444_800;

/// Nanos per second, and per unit of [`Money`].
const NANOS: i64 = 1_000_000_000;

/// The exchange-level `status`'s name in a refusal, apart from a contract's `status`.
const EXCHANGE_STATUS: &str = "exchange status";

/// Discovery's plan under `cfg`: one `GET` of [`EXCHANGE_INFO_PATH`] on the market-data REST
/// origin, waiting the REST timeout, read by [`decode_exchange_info`]. Only those two keys are
/// read.
pub(crate) fn plan(cfg: &VenueConfig) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
    let url = format!("{}{EXCHANGE_INFO_PATH}", rest_origin(cfg)?);
    let timeout = rest_timeout(cfg)?;
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
        charge: RateCharge::one(OpKind::Rest, None),
    }
}

/// The drafts of an exchange-info answer's contracts, in the answer's order. Refused, naming the
/// part: a body that is not a JSON object, the exchange status missing or not a string,
/// `futureContracts` missing or not a list, a contract that is not an object, missing a required
/// field or stating one not as documented.
pub fn decode_exchange_info(
    body: &[u8],
    scope: &DecodeScope<'_>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let doc: Value =
        serde_json::from_slice(body).map_err(|_| malformed("exchange-info answer is not JSON"))?;
    let doc = doc
        .as_object()
        .ok_or(malformed("exchange-info answer is not an object"))?;
    let normal = field(doc, EXCHANGE_STATUS, "status")?
        .as_str()
        .ok_or(malformed(EXCHANGE_STATUS))?
        == "NORMAL";
    let contracts = required(doc, "futureContracts")?
        .as_array()
        .ok_or(malformed("futureContracts"))?;
    let mut drafts = Vec::with_capacity(contracts.len());
    for contract in contracts {
        let contract = contract
            .as_object()
            .ok_or(malformed("futureContracts: a contract is not an object"))?;
        drafts.push(draft(contract, normal, scope)?);
    }
    Ok(drafts)
}

/// One contract as a draft; `normal` is whether the exchange's status is `NORMAL`.
fn draft(
    m: &Map<String, Value>,
    normal: bool,
    scope: &DecodeScope<'_>,
) -> Result<InstrumentSpecDraft, PlanError> {
    let status = match text(m, "status")? {
        "LIVE" if normal => TradingStatus::Trading,
        "LIVE" | "LISTED" | "PRE_OPEN" | "PAUSED" | "CLOSED" => TradingStatus::Halted,
        _ => return Err(malformed("status").into()),
    };
    let id = required(m, "id")?.as_u64();
    let id = id.filter(|n| *n <= u64::from(u32::MAX));
    let native_id = VenueNativeId::Number(id.ok_or(malformed("id"))?);
    let base = asset(m, "underlyingSymbol")?;
    let settle_ccy = asset(m, "settlementSymbol")?;
    let symbol = text(m, "symbol")?;
    let quote = quote_of(symbol, base).ok_or(malformed("symbol"))?;
    let venue_symbol = scope.venue_symbol(symbol)?;

    let tick = decimal(m, "tickSize")?;
    let price_grid = PriceGrid::fixed(tick).map_err(|_| malformed("tickSize"))?;
    let quote_grid = quote_grid(required(m, "orderbookGranularities")?, tick)?;

    let step = decimal(m, "stepSize")?;
    let size_step = SizeStep::new(step).ok_or(malformed("stepSize"))?;
    let min_size = whole_steps(decimal(m, "minOrderSize")?, step)
        .filter(|lots| lots.get() > 0)
        .ok_or(malformed("minOrderSize"))?;
    let min_notional = money(decimal(m, "minNotional")?, quote).ok_or(malformed("minNotional"))?;

    let signing = SigningParams::AssetDecimals {
        underlying: decimals(m, "underlyingDecimals")?,
        settlement: decimals(m, "settlementDecimals")?,
    };

    let first_open = advertised(m, "marketOpenTimestamp", Round::Up)?;
    let final_close = advertised(m, "marketCloseTimestamp", Round::Down)?;
    let fx = FX_UNDERLYINGS.contains(&base.as_str());
    let sessions = SessionCalendar::new(CalendarParts {
        first_open,
        final_close,
        windows: Vec::new(),
        weekly: if fx { vec![FX_WEEKEND] } else { Vec::new() },
    });

    Ok(InstrumentSpecDraft {
        asset: AssetKey {
            base,
            quote,
            kind: InstrumentKind::Perpetual,
        },
        venue_symbol,
        native_id: Some(native_id),
        price_grid,
        quote_grid,
        size_step,
        min_size,
        min_notional: Some(min_notional),
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        settle_ccy,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status,
        sessions,
        signing: Some(signing),
    })
}

/// The quote asset of `symbol` when it is `<base>/<quote>-P`, the quote of ASCII letters and
/// digits.
fn quote_of(symbol: &str, base: AssetSym) -> Option<AssetSym> {
    let (pair_base, quote) = symbol.strip_suffix("-P")?.split_once('/')?;
    let letters = !quote.is_empty() && quote.bytes().all(|b| b.is_ascii_alphanumeric());
    (pair_base == base.as_str() && letters)
        .then(|| AssetSym::new(quote))
        .flatten()
}

/// The finest granularity at or above `tick` among `listed`, exactly as listed, where it is
/// coarser than `tick`; `None` when it is the tick itself or none is, as `fbc-core` defines
/// `quote_grid` (`None` where the book uses the acceptance grid). An entry that is not a plain
/// decimal string ([`plain_decimal`]) greater than zero is passed over.
fn quote_grid(listed: &Value, tick: Decimal) -> Result<Option<Decimal>, PlanError> {
    let listed = listed
        .as_array()
        .ok_or(malformed("orderbookGranularities"))?;
    Ok(listed
        .iter()
        .filter_map(Value::as_str)
        .filter_map(plain_decimal)
        .filter(|g| *g > Decimal::ZERO && *g >= tick)
        .min()
        .filter(|g| *g > tick))
}

/// Which way an advertised instant rounds to whole nanoseconds: toward closed.
#[derive(Copy, Clone)]
enum Round {
    /// An open: later.
    Up,
    /// A close: earlier.
    Down,
}

/// An optional advertised instant, Unix seconds in a plain decimal string ([`plain_decimal`]),
/// no later than 2100.
fn advertised(
    m: &Map<String, Value>,
    key: &'static str,
    round: Round,
) -> Result<Option<WallNs>, PlanError> {
    let Some(value) = m.get(key).filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let secs = value
        .as_str()
        .and_then(plain_decimal)
        .filter(|secs| *secs <= Decimal::from(LATEST_SECS))
        .ok_or(malformed(key))?;
    let nanos = secs * Decimal::from(NANOS);
    let nanos = match round {
        Round::Up => nanos.ceil(),
        Round::Down => nanos.floor(),
    };
    Ok(Some(WallNs(nanos.to_i64().ok_or(malformed(key))?)))
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

/// A required field, named by its key.
fn required<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a Value, PlanError> {
    field(m, key, key)
}

/// A required string field.
fn text<'a>(m: &'a Map<String, Value>, key: &'static str) -> Result<&'a str, PlanError> {
    Ok(required(m, key)?.as_str().ok_or(malformed(key))?)
}

/// A required asset name, as written: one an [`AssetSym`] cannot hold (empty, more than sixteen
/// bytes, or not printable ASCII) is malformed.
fn asset(m: &Map<String, Value>, key: &'static str) -> Result<AssetSym, PlanError> {
    Ok(AssetSym::new(text(m, key)?).ok_or(malformed(key))?)
}

/// A required plain decimal string ([`plain_decimal`]), exactly as written, so never negative.
fn decimal(m: &Map<String, Value>, key: &'static str) -> Result<Decimal, PlanError> {
    Ok(plain_decimal(text(m, key)?).ok_or(malformed(key))?)
}

/// `s` read exactly when it is plain decimal digits with at most one decimal point and a digit on
/// each side of it; `None` for anything else, a sign, an exponent, a digit separator, a space or
/// a bare leading or trailing point included, though `Decimal`'s own parser reads some of them.
pub(crate) fn plain_decimal(s: &str) -> Option<Decimal> {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    let plain = match s.split_once('.') {
        Some((whole, fraction)) => digits(whole) && digits(fraction),
        None => digits(s),
    };
    plain.then(|| Decimal::from_str_exact(s).ok()).flatten()
}

/// A required count of decimal places, a whole number from 0 to 255.
fn decimals(m: &Map<String, Value>, key: &'static str) -> Result<u8, PlanError> {
    let n = required(m, key)?.as_u64();
    Ok(n.and_then(|n| u8::try_from(n).ok()).ok_or(malformed(key))?)
}

/// `qty` as a whole number of steps of `step`, or `None` when it is off the step grid. That many
/// steps must be `qty` exactly: a quotient past `Decimal`'s 28 digits rounds, so one off the grid
/// by less than that would otherwise read as whole.
fn whole_steps(qty: Decimal, step: Decimal) -> Option<Lots> {
    let count = qty.checked_div(step)?;
    let whole = count.fract().is_zero() && count.checked_mul(step) == Some(qty);
    whole.then(|| count.to_i64()).flatten().and_then(Lots::new)
}

/// An amount of `asset`, exact in nanos.
fn money(amount: Decimal, asset: AssetSym) -> Option<Money> {
    let nanos = amount.checked_mul(Decimal::from(NANOS))?;
    let nanos = nanos.fract().is_zero().then(|| nanos.to_i128()).flatten()?;
    Some(Money::new(nanos, asset))
}

/// FBC's Java-era rule for Hibachi: `X/USDT`, and the Java library's own `X/USDT-P`, trimmed and
/// uppercased, are the key the `X/USDT-P` contract's draft states (`X`, `USDT`, perpetual). A
/// ticker with another quote is [`SymbolError::Unmapped`] (under the seeded aliases `X/USDC`
/// would otherwise resolve to `X/USDT-P`), another form [`SymbolError::NotCommonForm`].
pub(crate) fn parse_fbc_common_symbol(s: &str) -> Result<AssetKey, SymbolError> {
    let normalized = s.trim().to_ascii_uppercase();
    let (base, quote) = common_symbol_parts(&normalized)?;
    let quote = match quote.as_str() {
        "USDT" => quote,
        "USDT-P" => AssetSym::new("USDT").ok_or(SymbolError::Unmapped)?,
        _ => return Err(SymbolError::Unmapped),
    };
    Ok(AssetKey {
        base,
        quote,
        kind: InstrumentKind::Perpetual,
    })
}
