//! FBC-jyyl's done line, the discovery half: the hand-built `fixtures/hibachi/rest` exchange-info
//! answers decode into instrument drafts with the contract id, the tick, the quote grid, the
//! sizes, the signing parameters and the session calendar each from the contract's own values;
//! each contract's status is mapped conservatively; the advertised open and close are read as
//! decimal Unix seconds; a contract missing a required field, or stating one not as documented,
//! refuses the answer; and `X/USDT` is `X/USDT-P`.

use std::time::Duration;

use fbc_core::{
    AliasTable, AssetKey, AssetSym, DecodeError, Effect, FundingSpec, HttpFailure, HttpMethod,
    HttpRequest, HttpResponse, InstrumentId, InstrumentKind, InstrumentResolver,
    InstrumentSpecDraft, Listing, Lots, Money, OpKind, PlanError, PriceGrid, RateCharge,
    SigningParams, SizeStep, SymbolError, TradingStatus, TrafficClass, UnderlyingId, VenueConfig,
    VenueFactory, VenueId, VenueNativeId, WallNs, WireSlice, WireUrl, dispatch_market_data,
};
use fbc_venue_hibachi::discover::{EXCHANGE_INFO_TAG, FX_UNDERLYINGS, FX_WEEKEND};
use fbc_venue_hibachi::{
    Hibachi, KEY_DATA_REST_URL, KEY_DATA_WS_URL, KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE,
    KEY_REST_TIMEOUT,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const MAIN: &str = include_str!("../../../../fixtures/hibachi/rest/exchange_info.json");
const SCHEDULED: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_scheduled_maintenance.json");
const UNSCHEDULED: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_unscheduled_maintenance.json");
const STATUS_ABSENT: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_status_absent.json");
const STATUS_NULL: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_status_null.json");
const STATUS_UNDOCUMENTED: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_status_undocumented.json");
const CLOSE_MS: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_close_ms.json");
const CLOSE_NOT_NUMBER: &str =
    include_str!("../../../../fixtures/hibachi/rest/exchange_info_close_not_number.json");

const NS: i64 = 1_000_000_000;
const VENUE: VenueId = VenueId::new(6);

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_DATA_REST_URL, "https://data-api.hibachi.xyz");
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    cfg.insert(KEY_ORDERBOOK_DEPTH, "25");
    cfg.insert(KEY_DATA_WS_URL, "wss://data-api.hibachi.xyz");
    cfg.insert(KEY_ORDERBOOK_RESUBSCRIBE, "2000ms");
    cfg
}

fn discover_with(
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let plan = Hibachi.discover(&cfg()).unwrap();
    let caps = Hibachi.caps(&cfg()).unwrap();
    dispatch_market_data(&caps, |scope| {
        plan.parse(&[(EXCHANGE_INFO_TAG, answer)], scope)
    })
    .map(|step| step.done().expect("one round"))
}

fn decode_body(body: &[u8]) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body,
    }))
}

fn decode(doc: &Value) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    decode_body(doc.to_string().as_bytes())
}

fn json_of(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

fn main_drafts() -> Vec<InstrumentSpecDraft> {
    decode_body(MAIN.as_bytes()).expect("the main fixture decodes")
}

fn find<'a>(drafts: &'a [InstrumentSpecDraft], symbol: &str) -> &'a InstrumentSpecDraft {
    drafts
        .iter()
        .find(|d| d.venue_symbol.as_wire() == symbol)
        .unwrap_or_else(|| panic!("{symbol} discovered"))
}

/// The main fixture with `edit` applied to the contract `symbol`.
fn with_contract(symbol: &str, edit: impl FnOnce(&mut serde_json::Map<String, Value>)) -> Value {
    let mut doc = json_of(MAIN);
    let contract = doc["futureContracts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|c| c["symbol"] == symbol)
        .unwrap();
    edit(contract.as_object_mut().unwrap());
    doc
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

fn sym(s: &str) -> AssetSym {
    AssetSym::new(s).unwrap()
}

fn perp(base: &str, quote: &str) -> AssetKey {
    AssetKey {
        base: sym(base),
        quote: sym(quote),
        kind: InstrumentKind::Perpetual,
    }
}

fn at(secs: i64) -> WallNs {
    WallNs(secs * NS)
}

fn malformed(part: &'static str) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    Err(PlanError::Decode(DecodeError::Malformed(part)))
}

#[test]
fn the_plan_is_one_public_get_of_exchange_info_on_the_configured_origin() {
    let plan = Hibachi.discover(&cfg()).unwrap();
    assert_eq!(
        plan.requests(),
        &[Effect::Http {
            tag: EXCHANGE_INFO_TAG,
            req: HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain("https://data-api.hibachi.xyz/market/exchange-info"),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: Duration::from_millis(5000),
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Rest, None),
        }]
    );
    // A trailing slash is not doubled, a port is kept, and a test stub on a loopback host is
    // reached over plain HTTP.
    for (origin, url) in [
        (
            "https://data-api.hibachi.xyz/",
            "https://data-api.hibachi.xyz/market/exchange-info",
        ),
        (
            "https://data-api.hibachi.xyz:8443",
            "https://data-api.hibachi.xyz:8443/market/exchange-info",
        ),
        (
            "http://127.0.0.1:3456",
            "http://127.0.0.1:3456/market/exchange-info",
        ),
        (
            "http://localhost:3456/",
            "http://localhost:3456/market/exchange-info",
        ),
        (
            "http://[::1]:3456",
            "http://[::1]:3456/market/exchange-info",
        ),
        (
            "https://[2001:db8::1]",
            "https://[2001:db8::1]/market/exchange-info",
        ),
    ] {
        let mut cfg = cfg();
        cfg.insert(KEY_DATA_REST_URL, origin);
        let plan = Hibachi.discover(&cfg).unwrap();
        let [Effect::Http { req, .. }] = plan.requests() else {
            panic!("one GET")
        };
        assert_eq!(req.url, WireUrl::plain(url), "{origin}");
    }
    let mut cfg = cfg();
    cfg.insert(KEY_REST_TIMEOUT, "1500ms");
    let plan = Hibachi.discover(&cfg).unwrap();
    let [Effect::Http { timeout, .. }] = plan.requests() else {
        panic!("one GET")
    };
    assert_eq!(*timeout, Duration::from_millis(1500));
}

#[test]
fn eth_is_discovered_with_its_contract_id_tick_and_quote_grid() {
    let drafts = main_drafts();
    assert_eq!(drafts.len(), 11);
    let eth = find(&drafts, "ETH/USDT-P");
    assert_eq!(eth.asset, perp("ETH", "USDT"));
    // The contract id Hibachi's signer packs.
    assert_eq!(eth.native_id, Some(VenueNativeId::Number(1)));
    assert_eq!(eth.price_grid, PriceGrid::fixed(dec("0.000001")).unwrap());
    // The finest granularity at or above the tick, coarser than the tick.
    assert_eq!(eth.quote_grid, Some(dec("0.01")));
    assert!(eth.quote_grid.unwrap() > eth.price_grid.finest());
    assert_eq!(eth.settle_ccy, sym("USDT"));
    assert_eq!(eth.multiplier, Decimal::ONE);
    assert_eq!(eth.status, TradingStatus::Trading);
    assert_eq!(eth.funding, FundingSpec::Unknown);
    assert_eq!(eth.public_fees, None);
    assert_eq!(eth.max_order_size, None);
    assert_eq!(eth.position_limit, None);
    assert_eq!(eth.price_band, None);
    assert_eq!(eth.max_open_orders, None);
    // Always open: no advertised open or close, and not FX.
    assert_eq!(eth.sessions, None);
    // Each contract keeps its own id, in the answer's order.
    let ids: Vec<_> = drafts.iter().map(|d| d.native_id.clone()).collect();
    let expected: Vec<_> = [1, 3, 2, 9, 11, 12, 13, 21, 31, 32, 33]
        .into_iter()
        .map(|n| Some(VenueNativeId::Number(n)))
        .collect();
    assert_eq!(ids, expected);
}

#[test]
fn the_quote_grid_is_the_finest_granularity_at_or_above_the_tick() {
    let drafts = main_drafts();
    // Coarser than the tick: ETH lists 0.01 over a tick of 0.000001, EUR 0.0001 over 0.00001.
    assert_eq!(find(&drafts, "ETH/USDT-P").quote_grid, Some(dec("0.01")));
    assert_eq!(find(&drafts, "EUR/USDT-P").quote_grid, Some(dec("0.0001")));
    // SOL lists its tick among its granularities: its quote grid equals its price grid, which
    // fbc-core spells None (the book on the acceptance grid), never Some of the tick.
    let sol = find(&drafts, "SOL/USDT-P");
    assert_eq!(sol.price_grid, PriceGrid::fixed(dec("0.001")).unwrap());
    assert_eq!(sol.quote_grid, None);
    // The tick written another way ("0.0010") is still the tick, so still None.
    let doc = with_contract("SOL/USDT-P", |c| {
        c.insert("orderbookGranularities".into(), json!(["0.0010", "0.01"]));
    });
    assert_eq!(find(&decode(&doc).unwrap(), "SOL/USDT-P").quote_grid, None);
    // In any order, and exactly as listed, so it names the granularity a subscription asks for.
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert(
            "orderbookGranularities".into(),
            json!(["10", "0.0000001", "0.010", "1"]),
        );
    });
    let grid = find(&decode(&doc).unwrap(), "ETH/USDT-P")
        .quote_grid
        .unwrap();
    assert_eq!(grid, dec("0.01"));
    assert_eq!(grid.to_string(), "0.010");
}

#[test]
fn a_contract_with_no_granularity_at_or_above_its_tick_is_quoted_on_its_tick() {
    let drafts = main_drafts();
    // None listed (SUI), none well formed (APT: blank, unparsable, negative, zero, null), all
    // finer than the tick (SEI): quoted on the tick, as Java's resolveEffectiveTickSize falls
    // back, so no quote grid of its own; still discovered, still trading.
    for (symbol, tick) in [
        ("SUI/USDT-P", "0.0001"),
        ("APT/USDT-P", "0.001"),
        ("SEI/USDT-P", "0.01"),
    ] {
        let draft = find(&drafts, symbol);
        assert_eq!(draft.quote_grid, None, "{symbol}");
        assert_eq!(draft.price_grid, PriceGrid::fixed(dec(tick)).unwrap());
        assert_eq!(draft.status, TradingStatus::Trading, "{symbol}");
    }
    // What is not a plain decimal string is passed over too: a JSON number, an exponent, a
    // digit separator, a sign, a bare point, a space.
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert(
            "orderbookGranularities".into(),
            json!([
                0.01,
                "1e-2",
                "0.0_1",
                "+0.01",
                ".01",
                "1.",
                " 0.1",
                "0.0000005"
            ]),
        );
    });
    assert_eq!(find(&decode(&doc).unwrap(), "ETH/USDT-P").quote_grid, None);
    // A well-formed entry beside malformed ones is still found.
    let doc = with_contract("APT/USDT-P", |c| {
        c.insert(
            "orderbookGranularities".into(),
            json!(["", "abc", "0.01", "-0.1"]),
        );
    });
    assert_eq!(
        find(&decode(&doc).unwrap(), "APT/USDT-P").quote_grid,
        Some(dec("0.01"))
    );
}

#[test]
fn sizes_come_from_step_size_min_order_size_and_min_notional() {
    let drafts = main_drafts();
    let usdt = |units: i128| Some(Money::new(units * 1_000_000_000, sym("USDT")));
    for (symbol, step, min_lots, notional) in [
        ("ETH/USDT-P", "0.000000001", 1_000, usdt(1)),
        ("SOL/USDT-P", "0.00000001", 10_000, usdt(5)),
        ("EUR/USDT-P", "0.01", 100, usdt(10)),
        ("DOGE/USDT-P", "1", 10, usdt(1)),
    ] {
        let draft = find(&drafts, symbol);
        assert_eq!(
            draft.size_step,
            SizeStep::new(dec(step)).unwrap(),
            "{symbol}"
        );
        assert_eq!(draft.min_size, Lots::new(min_lots).unwrap(), "{symbol}");
        assert_eq!(draft.min_notional, notional, "{symbol}");
    }
}

#[test]
fn signing_params_are_the_contracts_asset_decimals() {
    let drafts = main_drafts();
    for (symbol, underlying, settlement) in [
        ("ETH/USDT-P", 9, 6),
        ("SOL/USDT-P", 8, 6),
        ("EUR/USDT-P", 2, 6),
        ("DOGE/USDT-P", 0, 6),
    ] {
        assert_eq!(
            find(&drafts, symbol).signing,
            Some(SigningParams::AssetDecimals {
                underlying,
                settlement
            }),
            "{symbol}"
        );
    }
    // Each half from its own field: every fixture contract settles in USDT at 6 decimals, so
    // other settlement decimals are set here, each paired with an underlying that differs.
    for (underlying, settlement) in [(255, 6), (9, 8), (9, 0), (0, 255)] {
        let doc = with_contract("ETH/USDT-P", |c| {
            c.insert("underlyingDecimals".into(), json!(underlying));
            c.insert("settlementDecimals".into(), json!(settlement));
        });
        assert_eq!(
            find(&decode(&doc).unwrap(), "ETH/USDT-P").signing,
            Some(SigningParams::AssetDecimals {
                underlying,
                settlement
            }),
            "{underlying}, {settlement}"
        );
    }
}

#[test]
fn an_fx_contract_carries_the_new_york_weekend_calendar() {
    assert_eq!(FX_UNDERLYINGS, ["EUR", "GBP"]);
    let drafts = main_drafts();
    let eur = find(&drafts, "EUR/USDT-P");
    assert_eq!(eur.status, TradingStatus::Trading);
    let calendar = eur.sessions.as_ref().expect("an FX weekend calendar");
    assert_eq!(calendar.parts().weekly, vec![FX_WEEKEND]);
    assert_eq!(calendar.parts().first_open, None);
    assert_eq!(calendar.parts().final_close, None);
    assert!(calendar.parts().windows.is_empty());
    // Friday 17:00 to Sunday 17:00 New York time, in UTC seconds checked against zoneinfo
    // (America/New_York): a winter weekend (22:00 UTC) and a summer one (21:00 UTC); then, in
    // 2027, the weekend daylight time begins (closing at 22:00 UTC, reopening at 21:00 UTC)
    // and the next one, the last weekend of October, still daylight time, and the weekend it
    // ends (closing at 21:00 UTC, reopening at 22:00 UTC). The rule since 2007 (the second
    // Sunday of March to the first Sunday of November) gives each of these; the rule before it
    // (the first Sunday of April to the last Sunday of October) puts at least one edge of each
    // of these four an hour off.
    for (close, reopen) in [
        // 2027-01-08 and 01-10.
        (1_799_445_600, 1_799_618_400),
        // 2027-07-09 and 07-11.
        (1_815_166_800, 1_815_339_600),
        // 2027-03-12, EST, and 03-14, EDT.
        (1_804_888_800, 1_805_058_000),
        // 2027-03-19 and 03-21, EDT.
        (1_805_490_000, 1_805_662_800),
        // 2027-10-29 and 10-31, EDT.
        (1_824_843_600, 1_825_016_400),
        // 2027-11-05, EDT, and 11-07, EST.
        (1_825_448_400, 1_825_624_800),
    ] {
        assert!(calendar.is_open(WallNs(at(close).0 - 1)), "{close}");
        assert!(!calendar.is_open(at(close)), "{close}");
        assert!(!calendar.is_open(at(close + 86_400)), "{close}");
        assert!(!calendar.is_open(WallNs(at(reopen).0 - 1)), "{reopen}");
        assert!(calendar.is_open(at(reopen)), "{reopen}");
    }
    // GBP, PAUSED, has the same calendar.
    let gbp = find(&drafts, "GBP/USDT-P");
    assert_eq!(
        gbp.sessions.as_ref().unwrap().parts().weekly,
        vec![FX_WEEKEND]
    );
    // A crypto contract has none.
    for symbol in ["ETH/USDT-P", "SOL/USDT-P", "DOGE/USDT-P"] {
        assert_eq!(find(&drafts, symbol).sessions, None, "{symbol}");
    }
}

#[test]
fn only_live_is_trading_and_every_other_listed_status_is_halted() {
    let drafts = main_drafts();
    for (symbol, status) in [
        ("ETH/USDT-P", TradingStatus::Trading),
        ("SOL/USDT-P", TradingStatus::Trading),
        ("EUR/USDT-P", TradingStatus::Trading),
        ("WIF/USDT-P", TradingStatus::Trading),
        ("GBP/USDT-P", TradingStatus::Halted),
        ("DOGE/USDT-P", TradingStatus::Halted),
        ("AVAX/USDT-P", TradingStatus::Halted),
        ("XRP/USDT-P", TradingStatus::Halted),
    ] {
        assert_eq!(find(&drafts, symbol).status, status, "{symbol}");
    }
    // PAUSED, LISTED, PRE_OPEN and CLOSED, each on its own fixture contract.
    let statuses: Vec<_> = json_of(MAIN)["futureContracts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["status"].as_str().unwrap().to_owned())
        .collect();
    for listed in ["LISTED", "PRE_OPEN", "PAUSED", "CLOSED"] {
        assert_eq!(
            statuses.iter().filter(|s| *s == listed).count(),
            1,
            "{listed}"
        );
    }
}

#[test]
fn every_contract_is_halted_while_the_exchange_is_not_normal() {
    // The two maintenance states the spec lists, each its own fixture.
    for (name, text) in [
        ("SCHEDULED_MAINTENANCE", SCHEDULED),
        ("UNSCHEDULED_MAINTENANCE", UNSCHEDULED),
    ] {
        assert_eq!(json_of(text)["status"], name);
        let drafts = decode_body(text.as_bytes()).unwrap();
        assert_eq!(drafts.len(), 3, "{name}");
        assert!(
            drafts.iter().all(|d| d.status == TradingStatus::Halted),
            "{name}"
        );
        // The contracts that are LIVE are the ones maintenance halts.
        assert_eq!(json_of(text)["futureContracts"][0]["status"], "LIVE");
    }
    // Anything but NORMAL halts every contract of the main fixture, the SDK's MAINTENANCE and
    // a lowercase normal included.
    for status in [
        "SCHEDULED_MAINTENANCE",
        "UNSCHEDULED_MAINTENANCE",
        "MAINTENANCE",
        "normal",
    ] {
        let mut doc = json_of(MAIN);
        doc["status"] = json!(status);
        let drafts = decode(&doc).unwrap();
        assert_eq!(drafts.len(), 11);
        assert!(
            drafts.iter().all(|d| d.status == TradingStatus::Halted),
            "{status}"
        );
    }
}

#[test]
fn a_contract_status_absent_or_null_refuses_the_answer_as_missing() {
    // Absent: the older answer shape with "live": true and no status, which is not read.
    assert_eq!(json_of(STATUS_ABSENT)["futureContracts"][1]["live"], true);
    for text in [STATUS_ABSENT, STATUS_NULL] {
        assert_eq!(
            decode_body(text.as_bytes()),
            Err(PlanError::Missing("status"))
        );
    }
}

#[test]
fn an_undocumented_contract_status_refuses_the_answer_as_malformed() {
    // The fixture's lowercase live, matched exactly.
    assert_eq!(
        decode_body(STATUS_UNDOCUMENTED.as_bytes()),
        malformed("status")
    );
    for status in [
        json!("Live"),
        json!("TRADING"),
        json!(""),
        json!(1),
        json!(true),
    ] {
        let doc = with_contract("SOL/USDT-P", |c| {
            c.insert("status".into(), status.clone());
        });
        assert_eq!(decode(&doc), malformed("status"), "{status}");
    }
    // Refused while the exchange is in maintenance too.
    let mut doc = json_of(STATUS_UNDOCUMENTED);
    doc["status"] = json!("UNSCHEDULED_MAINTENANCE");
    assert_eq!(decode(&doc), malformed("status"));
}

#[test]
fn neither_an_advertised_open_nor_the_calendar_makes_a_contract_trading() {
    let drafts = main_drafts();
    // PRE_OPEN with an advertised open: the calendar opens then, the contract stays Halted.
    let avax = find(&drafts, "AVAX/USDT-P");
    assert_eq!(avax.status, TradingStatus::Halted);
    let calendar = avax.sessions.as_ref().unwrap();
    assert_eq!(calendar.parts().first_open, Some(at(1_830_000_000)));
    assert!(!calendar.is_open(WallNs(at(1_830_000_000).0 - 1)));
    assert!(calendar.is_open(at(1_830_000_000)));
    // PAUSED FX on a Wednesday, its calendar open: still Halted.
    let gbp = find(&drafts, "GBP/USDT-P");
    assert!(gbp.sessions.as_ref().unwrap().is_open(at(1_799_272_800)));
    assert_eq!(gbp.status, TradingStatus::Halted);
    // An open advertised on a LISTED or CLOSED contract changes nothing.
    for symbol in ["DOGE/USDT-P", "XRP/USDT-P"] {
        let doc = with_contract(symbol, |c| {
            c.insert("marketOpenTimestamp".into(), json!("1700000000"));
        });
        let drafts = decode(&doc).unwrap();
        assert_eq!(
            find(&drafts, symbol).status,
            TradingStatus::Halted,
            "{symbol}"
        );
    }
}

#[test]
fn an_advertised_close_closes_the_calendar_from_that_instant_on() {
    let drafts = main_drafts();
    // WIF is LIVE with an advertised open and close, each decimal seconds.
    let wif = find(&drafts, "WIF/USDT-P");
    assert_eq!(wif.status, TradingStatus::Trading);
    let calendar = wif.sessions.as_ref().unwrap();
    let close = WallNs(1_830_000_000 * NS + 500_000_000);
    let open = WallNs(1_800_000_000 * NS + 250_000_000);
    assert_eq!(calendar.parts().final_close, Some(close));
    assert_eq!(calendar.parts().first_open, Some(open));
    assert!(calendar.parts().weekly.is_empty());
    assert!(!calendar.is_open(WallNs(open.0 - 1)));
    assert!(calendar.is_open(open));
    assert!(calendar.is_open(WallNs(close.0 - 1)));
    for probe in [
        close,
        WallNs(close.0 + 1),
        at(2_000_000_000),
        WallNs(i64::MAX),
    ] {
        assert!(!calendar.is_open(probe), "{probe:?}");
    }
    // A close alone, on an FX contract: both close it.
    let doc = with_contract("EUR/USDT-P", |c| {
        c.insert("marketCloseTimestamp".into(), json!("1830000000"));
    });
    let drafts = decode(&doc).unwrap();
    let parts = find(&drafts, "EUR/USDT-P")
        .sessions
        .as_ref()
        .unwrap()
        .parts()
        .clone();
    assert_eq!(parts.final_close, Some(at(1_830_000_000)));
    assert_eq!(parts.weekly, vec![FX_WEEKEND]);
}

#[test]
fn market_timestamps_both_null_or_both_absent_advertise_nothing() {
    let main = json_of(MAIN);
    let contracts = main["futureContracts"].as_array().unwrap();
    // ETH states both null; SOL leaves both out.
    assert!(contracts[0]["marketOpenTimestamp"].is_null());
    assert!(contracts[0]["marketCloseTimestamp"].is_null());
    assert!(contracts[1].get("marketOpenTimestamp").is_none());
    assert!(contracts[1].get("marketCloseTimestamp").is_none());
    let drafts = main_drafts();
    assert_eq!(find(&drafts, "ETH/USDT-P").sessions, None);
    assert_eq!(find(&drafts, "SOL/USDT-P").sessions, None);
}

#[test]
fn a_close_in_milliseconds_or_not_a_number_refuses_the_answer() {
    for text in [CLOSE_MS, CLOSE_NOT_NUMBER] {
        assert_eq!(
            decode_body(text.as_bytes()),
            malformed("marketCloseTimestamp")
        );
    }
    for (key, value) in [
        ("marketCloseTimestamp", json!("4102444800.000000001")),
        ("marketCloseTimestamp", json!(1_830_000_000)),
        ("marketCloseTimestamp", json!("-1")),
        ("marketCloseTimestamp", json!("+1830000000")),
        ("marketCloseTimestamp", json!("1.2.3")),
        ("marketCloseTimestamp", json!("1830000000.")),
        ("marketCloseTimestamp", json!(".5")),
        ("marketCloseTimestamp", json!(".")),
        ("marketCloseTimestamp", json!("1_830_000_000")),
        ("marketCloseTimestamp", json!("")),
        ("marketCloseTimestamp", json!(" 1830000000")),
        ("marketCloseTimestamp", json!("1e9")),
        ("marketOpenTimestamp", json!("1800000000000")),
        ("marketOpenTimestamp", json!("tomorrow")),
        ("marketOpenTimestamp", json!(false)),
    ] {
        let doc = with_contract("ETH/USDT-P", |c| {
            c.insert(key.into(), value.clone());
        });
        assert_eq!(decode(&doc), malformed(key), "{key}: {value}");
    }
    // 2100-01-01T00:00:00Z itself is not after it.
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert("marketCloseTimestamp".into(), json!("4102444800"));
    });
    let drafts = decode(&doc).unwrap();
    let parts = find(&drafts, "ETH/USDT-P")
        .sessions
        .as_ref()
        .unwrap()
        .parts()
        .clone();
    assert_eq!(parts.final_close, Some(at(4_102_444_800)));
    // Finer than a nanosecond, each rounds toward closed: the open later, the close earlier.
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert("marketOpenTimestamp".into(), json!("1800000000.0000000001"));
        c.insert(
            "marketCloseTimestamp".into(),
            json!("1830000000.0000000009"),
        );
    });
    let drafts = decode(&doc).unwrap();
    let parts = find(&drafts, "ETH/USDT-P")
        .sessions
        .as_ref()
        .unwrap()
        .parts()
        .clone();
    assert_eq!(parts.first_open, Some(WallNs(1_800_000_000 * NS + 1)));
    assert_eq!(parts.final_close, Some(at(1_830_000_000)));
}

/// The contract fields discovery requires: the Market API spec's `Contract` and `ContractData`
/// fields it reads.
const REQUIRED: [&str; 12] = [
    "id",
    "symbol",
    "underlyingSymbol",
    "settlementSymbol",
    "underlyingDecimals",
    "settlementDecimals",
    "tickSize",
    "stepSize",
    "minOrderSize",
    "minNotional",
    "orderbookGranularities",
    "status",
];

#[test]
fn a_contract_missing_a_required_field_refuses_the_answer() {
    for key in REQUIRED {
        let absent = with_contract("ETH/USDT-P", |c| {
            c.remove(key);
        });
        assert_eq!(
            decode(&absent),
            Err(PlanError::Missing(key)),
            "{key} absent"
        );
        let null = with_contract("ETH/USDT-P", |c| {
            c.insert(key.into(), Value::Null);
        });
        assert_eq!(decode(&null), Err(PlanError::Missing(key)), "{key} null");
    }
    // The exchange's status and the contract list are required too.
    for key in ["status", "futureContracts"] {
        let named = if key == "status" {
            "exchange status"
        } else {
            key
        };
        let mut doc = json_of(MAIN);
        doc.as_object_mut().unwrap().remove(key);
        assert_eq!(decode(&doc), Err(PlanError::Missing(named)), "{key}");
        doc[key] = Value::Null;
        assert_eq!(decode(&doc), Err(PlanError::Missing(named)), "{key}");
    }
    // What is not read is not required.
    let doc = with_contract("ETH/USDT-P", |c| {
        for key in [
            "displayName",
            "initialMarginRate",
            "maintenanceMarginRate",
            "marketCreationTimestamp",
            "marketOpenTimestamp",
            "marketCloseTimestamp",
        ] {
            c.remove(key);
        }
    });
    let mut doc = doc;
    for key in ["feeConfig", "instantWithdrawalLimit", "maintenanceWindow"] {
        doc.as_object_mut().unwrap().remove(key);
    }
    assert_eq!(decode(&doc).unwrap().len(), 11);
}

#[test]
fn a_value_not_as_documented_refuses_the_answer_as_malformed() {
    for (key, value) in [
        ("id", json!("1")),
        ("id", json!(-1)),
        ("id", json!(1.5)),
        ("id", json!(4_294_967_296_u64)),
        ("symbol", json!("ETH-USDT-P")),
        ("symbol", json!("ETH/USDT")),
        ("symbol", json!("BTC/USDT-P")),
        ("symbol", json!("ETH/US-DT-P")),
        ("symbol", json!("ETH/-P")),
        ("symbol", json!("ETH/ABCDEFGHIJKLMNOPQ-P")),
        ("symbol", json!(1)),
        ("underlyingSymbol", json!("")),
        ("underlyingSymbol", json!("ABCDEFGHIJKLMNOPQ")),
        ("settlementSymbol", json!("US DT")),
        ("tickSize", json!("0")),
        ("tickSize", json!("-0.1")),
        ("tickSize", json!(0.000001)),
        ("tickSize", json!("abc")),
        // Plain decimal digits only: no digit separator, sign, exponent, bare point or space.
        ("tickSize", json!("0.000_001")),
        ("tickSize", json!("+0.000001")),
        ("tickSize", json!(".000001")),
        ("tickSize", json!("1.")),
        ("tickSize", json!("1e-6")),
        ("tickSize", json!(" 0.000001")),
        ("tickSize", json!("-0")),
        ("stepSize", json!("0")),
        ("stepSize", json!("0.000_000_001")),
        ("minOrderSize", json!("0.0000000015")),
        ("minOrderSize", json!("0")),
        ("minOrderSize", json!("+0.000001")),
        ("minNotional", json!("0.0000000001")),
        ("minNotional", json!("-1")),
        ("minNotional", json!("-0")),
        ("minNotional", json!("1_000")),
        ("underlyingDecimals", json!(256)),
        ("underlyingDecimals", json!(-1)),
        ("settlementDecimals", json!("6")),
        ("orderbookGranularities", json!("0.01")),
    ] {
        let doc = with_contract("ETH/USDT-P", |c| {
            c.insert(key.into(), value.clone());
        });
        assert_eq!(decode(&doc), malformed(key), "{key}: {value}");
    }
    // Off the step grid by less than Decimal's precision: the quotient rounds to a whole
    // number, but that many steps is not the size stated.
    for min in [
        "3000000000.0000000000000000001",
        "3000000000000000000.0000000001",
    ] {
        let doc = with_contract("ETH/USDT-P", |c| {
            c.insert("stepSize".into(), json!("3"));
            c.insert("minOrderSize".into(), json!(min));
        });
        assert_eq!(decode(&doc), malformed("minOrderSize"), "{min}");
    }
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert("stepSize".into(), json!("3"));
        c.insert("minOrderSize".into(), json!("3000000000"));
    });
    assert_eq!(
        find(&decode(&doc).unwrap(), "ETH/USDT-P").min_size,
        Lots::new(1_000_000_000).unwrap()
    );
    let mut doc = json_of(MAIN);
    doc["status"] = json!(1);
    assert_eq!(decode(&doc), malformed("exchange status"));
    doc["status"] = json!("NORMAL");
    doc["futureContracts"] = json!({});
    assert_eq!(decode(&doc), malformed("futureContracts"));
    doc["futureContracts"] = json!(["ETH/USDT-P"]);
    assert_eq!(
        decode(&doc),
        malformed("futureContracts: a contract is not an object")
    );
    assert_eq!(
        decode_body(b"not json"),
        malformed("exchange-info answer is not JSON")
    );
    assert_eq!(
        decode_body(b"[]"),
        malformed("exchange-info answer is not an object")
    );
}

#[test]
fn the_largest_u32_contract_id_is_its_native_id() {
    let doc = with_contract("ETH/USDT-P", |c| {
        c.insert("id".into(), json!(u32::MAX));
    });
    assert_eq!(
        find(&decode(&doc).unwrap(), "ETH/USDT-P").native_id,
        Some(VenueNativeId::Number(u64::from(u32::MAX)))
    );
}

#[test]
fn an_empty_contract_list_discovers_nothing() {
    let mut doc = json_of(MAIN);
    doc["futureContracts"] = json!([]);
    assert_eq!(decode(&doc), Ok(Vec::new()));
}

#[test]
fn every_perpetual_states_its_funding_unknown() {
    for text in [MAIN, SCHEDULED, UNSCHEDULED] {
        let drafts = decode_body(text.as_bytes()).unwrap();
        assert!(drafts.iter().all(|d| d.funding == FundingSpec::Unknown));
        assert!(
            drafts
                .iter()
                .all(|d| d.asset.kind == InstrumentKind::Perpetual)
        );
    }
}

#[test]
fn the_ticker_rule_maps_x_usdt_to_x_usdt_p() {
    let eth = perp("ETH", "USDT");
    for ticker in ["ETH/USDT", " eth/usdt ", "ETH/USDT-P", "eth/usdt-p"] {
        assert_eq!(Hibachi.parse_fbc_common_symbol(ticker), Ok(eth), "{ticker}");
    }
    for ticker in ["ETH/USDC", "ETH/USD", "EUR/USD-P", "ETH/USDT-Q"] {
        assert_eq!(
            Hibachi.parse_fbc_common_symbol(ticker),
            Err(SymbolError::Unmapped),
            "{ticker}"
        );
    }
    for ticker in ["ETH", "ETH-USDT-P", "ETH/", "/USDT", "ETH/USDT/P"] {
        assert_eq!(
            Hibachi.parse_fbc_common_symbol(ticker),
            Err(SymbolError::NotCommonForm),
            "{ticker}"
        );
    }
    // The key it reads is the one ETH/USDT-P's draft states: the resolver finds that draft's
    // listing from the Java-era ticker.
    let drafts = main_drafts();
    let draft = find(&drafts, "ETH/USDT-P").clone();
    assert_eq!(draft.asset, eth);
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let listing = Listing {
        id: InstrumentId::new(7),
        underlying: UnderlyingId::new(7),
    };
    resolver.list(VENUE, draft.asset, listing).unwrap();
    let key = Hibachi.parse_fbc_common_symbol("ETH/USDT").unwrap();
    assert_eq!(resolver.resolve(VENUE, key), Ok(InstrumentId::new(7)));
    let spec = resolver.spec(VENUE, draft, 1, WallNs(0)).unwrap();
    assert_eq!(spec.venue_symbol.as_wire(), "ETH/USDT-P");
    assert_eq!(spec.quote_ccy, sym("USDT"));
    assert_eq!(spec.quote_grid, Some(dec("0.01")));
    assert_eq!(spec.native_id, Some(VenueNativeId::Number(1)));
}

#[test]
fn a_failed_or_refused_request_ends_discovery_before_the_parser() {
    assert_eq!(
        discover_with(Err(HttpFailure::TimedOut)),
        Err(PlanError::Http {
            tag: EXCHANGE_INFO_TAG,
            failure: HttpFailure::TimedOut
        })
    );
    assert_eq!(
        discover_with(Ok(HttpResponse {
            status: 503,
            headers: &[],
            body: MAIN.as_bytes(),
        })),
        Err(PlanError::Status {
            tag: EXCHANGE_INFO_TAG,
            status: 503
        })
    );
}
