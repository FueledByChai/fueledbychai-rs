//! FBC-fwf: Binance USD-M discovers its instruments with one `GET /fapi/v1/exchangeInfo` under
//! the REST base, weighted as Binance documents, a plan of effects whose parser builds an
//! `InstrumentSpecDraft` per `PERPETUAL` symbol inside the decode scope from its filters and
//! refuses an answer whose perpetual leaves out a required filter, by name; and its Java-era
//! ticker rule reads `X/USDT` as the key of the `XUSDT` perpetual, refusing every other ticker.
//!
//! The answer is `fixtures/binance-usdm/rest/exchange_info.json`, hand-built in the shape
//! Binance documents for "Exchange Information".

mod common;

use std::num::NonZeroU32;
use std::time::Duration;

use common::{config, rest_fixture};
use fbc_core::{
    AliasTable, AssetKey, AssetSym, ConfigError, DecodeError, Effect, FundingSpec, HttpFailure,
    HttpMethod, HttpRequest, HttpResponse, HttpTag, IdError, InstrumentId, InstrumentKind,
    InstrumentResolver, InstrumentSpecDraft, Listing, Lots, Money, OpKind, PlanError, PriceGrid,
    RateCharge, ResolveError, SizeStep, SymbolError, TradingStatus, TrafficClass, UnderlyingId,
    VenueConfig, VenueError, VenueFactory, VenueId, WireSlice, WireUrl, dispatch_market_data,
};
use fbc_venue_binance_usdm::{BinanceUsdm, KEY_REST_BASE_URL, KEY_SNAPSHOT_TIMEOUT};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

/// The only keys discovery reads: the REST base and the REST request timeout.
fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_REST_BASE_URL, "https://fapi.binance.com");
    cfg.insert(KEY_SNAPSHOT_TIMEOUT, "5000ms");
    cfg
}

fn exchange_info() -> String {
    rest_fixture("exchange_info.json")
}

/// The tag of the plan's one request.
fn tag() -> HttpTag {
    let plan = BinanceUsdm.discover(&cfg()).unwrap();
    let [Effect::Http { tag, .. }] = plan.requests() else {
        panic!("one HTTP request")
    };
    *tag
}

/// Discovery's plan answered with `answer`, parsed in the decode scope market data is
/// dispatched with (discovery holds no engine namespace).
fn discover_with(
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let plan = BinanceUsdm.discover(&cfg()).unwrap();
    let caps = BinanceUsdm.caps(&config()).unwrap();
    dispatch_market_data(&caps, |scope| plan.parse(&[(tag(), answer)], scope))
        .map(|step| step.done().expect("a one-round plan ends in its result"))
}

fn discover(body: &str) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    }))
}

/// The fixture with `edit` applied to its symbol number `n`.
fn with_symbol(n: usize, edit: impl FnOnce(&mut Map<String, Value>)) -> String {
    let mut doc: Value = serde_json::from_str(&exchange_info()).unwrap();
    edit(doc["symbols"][n].as_object_mut().unwrap());
    doc.to_string()
}

/// The fixture with `edit` applied to its first symbol, BTCUSDT.
fn with_btc(edit: impl FnOnce(&mut Map<String, Value>)) -> String {
    with_symbol(0, edit)
}

/// BTCUSDT's filters, to edit.
fn filters(m: &mut Map<String, Value>) -> &mut Vec<Value> {
    m.get_mut("filters").unwrap().as_array_mut().unwrap()
}

/// BTCUSDT's filter of `kind`, to edit.
fn filter<'a>(m: &'a mut Map<String, Value>, kind: &str) -> &'a mut Map<String, Value> {
    let found = filters(m).iter_mut().find(|f| f["filterType"] == kind);
    found.unwrap().as_object_mut().unwrap()
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

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

fn malformed(part: &'static str) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    Err(PlanError::Decode(DecodeError::Malformed(part)))
}

fn symbols(drafts: &[InstrumentSpecDraft]) -> Vec<&str> {
    drafts.iter().map(|d| d.venue_symbol.as_wire()).collect()
}

#[test]
fn discovery_is_one_weighted_get_of_exchange_info_under_the_rest_base_and_nothing_else() {
    let plan = BinanceUsdm.discover(&cfg()).unwrap();
    let expected = Effect::Http {
        tag: tag(),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain("https://fapi.binance.com/fapi/v1/exchangeInfo"),
            headers: vec![],
            body: WireSlice::plain(vec![]),
        },
        rpc: None,
        timeout: Duration::from_millis(5000),
        class: TrafficClass::Normal,
        // "Request Weight: 1" against the IP's REQUEST_WEIGHT budget; it names no instrument.
        charge: RateCharge {
            op: OpKind::Rest,
            inst: None,
            weight: NonZeroU32::new(1).unwrap(),
        },
    };
    assert_eq!(plan.requests(), [expected]);

    // A trailing slash on the base is not doubled; a test stub may be plain HTTP with a port.
    let mut stub = cfg();
    stub.insert(KEY_REST_BASE_URL, "http://127.0.0.1:8080/");
    let plan = BinanceUsdm.discover(&stub).unwrap();
    let Effect::Http { req, .. } = &plan.requests()[0] else {
        panic!("an HTTP request")
    };
    assert_eq!(
        req.url,
        WireUrl::plain("http://127.0.0.1:8080/fapi/v1/exchangeInfo")
    );
    // The full market-data configuration discovers the same way.
    assert_eq!(BinanceUsdm.discover(&config()).unwrap().requests().len(), 1);
}

#[test]
fn discovery_refuses_a_configuration_it_cannot_send_by_the_key_at_fault() {
    let refused = |key: &str, value: Option<&str>| {
        let (base, mut changed) = (cfg(), VenueConfig::new());
        for k in [KEY_REST_BASE_URL, KEY_SNAPSHOT_TIMEOUT] {
            let v = if k == key { value } else { base.get(k) };
            v.map(|v| changed.insert(k, v));
        }
        BinanceUsdm.discover(&changed).err()
    };
    let missing = |key| Some(VenueError::Config(ConfigError::Missing(key)));
    assert_eq!(refused(KEY_REST_BASE_URL, None), missing(KEY_REST_BASE_URL));
    assert_eq!(
        refused(KEY_SNAPSHOT_TIMEOUT, None),
        missing(KEY_SNAPSHOT_TIMEOUT)
    );
    let invalid = |key, value| {
        let Some(VenueError::Config(ConfigError::Invalid { key: at, .. })) = refused(key, value)
        else {
            panic!("{value:?} is refused")
        };
        assert_eq!(at, key, "{value:?}");
    };
    for bad in [
        "wss://fapi.binance.com",
        "https://",
        "https://fapi.binance.com/fapi",
        "https://fapi.binance.com?x=1",
        "https://user@fapi.binance.com",
        "https://bad host",
        "https://fapi.binance.com:0",
    ] {
        invalid(KEY_REST_BASE_URL, Some(bad));
    }
    for bad in ["5000", "0ms", "5s", "fast"] {
        invalid(KEY_SNAPSHOT_TIMEOUT, Some(bad));
    }
}

#[test]
fn the_documented_answer_decodes_into_one_draft_per_perpetual_in_the_decode_scope() {
    let drafts = discover(&exchange_info()).unwrap();
    // The delivery contract, the TradFi perpetual and the settling symbol whose contractType is
    // empty are not PERPETUALs: none is a draft. 1000000MOG now fits the sixteen-byte asset
    // symbol (decision 0118); 币安人生 remains unkeyable because it is not ASCII.
    assert_eq!(
        symbols(&drafts),
        ["BTCUSDT", "ETHUSDT", "BTCUSDC", "OLDUSDT", "1000000MOGUSDT"]
    );
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol("BTCUSDT"));
    let expected = InstrumentSpecDraft {
        asset: perp("BTC", "USDT"),
        venue_symbol: venue_symbol.unwrap(),
        native_id: None,
        // PRICE_FILTER tickSize.
        price_grid: PriceGrid::fixed(dec("0.1")).unwrap(),
        quote_grid: None,
        // LOT_SIZE stepSize; minQty and maxQty in whole steps.
        size_step: SizeStep::new(dec("0.001")).unwrap(),
        min_size: Lots::new(1).unwrap(),
        max_order_size: Some(Lots::new(1_000_000).unwrap()),
        // MIN_NOTIONAL notional, in the quote asset.
        min_notional: Some(Money::new(100_000_000_000, sym("USDT"))),
        // exchangeInfo states no position limit (leverage brackets are per account), no single
        // price band (PERCENT_PRICE is asymmetric) and no funding schedule.
        position_limit: None,
        price_band: None,
        // MAX_NUM_ORDERS limit.
        max_open_orders: Some(200),
        multiplier: Decimal::ONE,
        settle_ccy: sym("USDT"),
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
    };
    assert_eq!(drafts[0], expected);
    let eth = &drafts[1];
    assert_eq!(eth.asset, perp("ETH", "USDT"));
    assert_eq!(eth.price_grid, PriceGrid::fixed(dec("0.01")).unwrap());
    assert_eq!(eth.max_open_orders, Some(100));
    assert_eq!(eth.max_order_size, Some(Lots::new(10_000_000).unwrap()));
    assert_eq!(
        eth.min_notional,
        Some(Money::new(20_000_000_000, sym("USDT")))
    );
    // A USDC-margined perpetual states its own quote and margin asset; its minQty is two steps.
    let usdc = &drafts[2];
    assert_eq!(usdc.asset, perp("BTC", "USDC"));
    assert_eq!(usdc.settle_ccy, sym("USDC"));
    assert_eq!(usdc.min_size, Lots::new(2).unwrap());
    // A closed perpetual is still listed, as delisted.
    assert_eq!(drafts[3].status, TradingStatus::Delisted);
    assert_eq!(drafts[3].size_step, SizeStep::new(Decimal::ONE).unwrap());

    // An optional filter left out is not stated; the draft is still built.
    let answer = with_btc(|m| filters(m).retain(|f| f["filterType"] != "MAX_NUM_ORDERS"));
    assert_eq!(discover(&answer).unwrap()[0].max_open_orders, None);
    // An empty list is an empty discovery, not a refusal.
    assert_eq!(discover(r#"{"symbols":[]}"#), Ok(vec![]));
}

#[test]
fn a_perpetual_missing_a_required_filter_or_field_refuses_the_whole_answer_by_name() {
    for kind in ["PRICE_FILTER", "LOT_SIZE", "MIN_NOTIONAL"] {
        let absent = with_btc(|m| filters(m).retain(|f| f["filterType"] != kind));
        assert_eq!(discover(&absent), Err(PlanError::Missing(kind)), "{kind}");
    }
    for (kind, field, named) in [
        ("PRICE_FILTER", "tickSize", "PRICE_FILTER.tickSize"),
        ("LOT_SIZE", "stepSize", "LOT_SIZE.stepSize"),
        ("LOT_SIZE", "minQty", "LOT_SIZE.minQty"),
        ("LOT_SIZE", "maxQty", "LOT_SIZE.maxQty"),
        ("MIN_NOTIONAL", "notional", "MIN_NOTIONAL.notional"),
        ("MAX_NUM_ORDERS", "limit", "MAX_NUM_ORDERS.limit"),
    ] {
        let absent = with_btc(|m| drop(filter(m, kind).remove(field)));
        assert_eq!(discover(&absent), Err(PlanError::Missing(named)), "{named}");
        let null = with_btc(|m| drop(filter(m, kind).insert(field.into(), Value::Null)));
        assert_eq!(discover(&null), Err(PlanError::Missing(named)), "{named}");
    }
    for field in [
        "symbol",
        "contractType",
        "status",
        "baseAsset",
        "quoteAsset",
        "marginAsset",
        "filters",
    ] {
        let absent = with_btc(|m| drop(m.remove(field)));
        assert_eq!(discover(&absent), Err(PlanError::Missing(field)), "{field}");
        let null = with_btc(|m| drop(m.insert(field.into(), Value::Null)));
        let refused = Err(PlanError::Missing(field));
        assert_eq!(discover(&null), refused, "{field} null");
    }
    assert_eq!(discover("{}"), Err(PlanError::Missing("symbols")));
    // A symbol that is not a PERPETUAL is passed over before anything else of it is read: the
    // delivery contract without its PRICE_FILTER refuses nothing.
    let quarter = with_symbol(3, |m| {
        filters(m).retain(|f| f["filterType"] != "PRICE_FILTER")
    });
    assert_eq!(symbols(&discover(&quarter).unwrap()).len(), 5);
}

#[test]
fn a_value_stated_but_invalid_is_malformed_and_named() {
    let filter_cases: &[(&str, &str, Value, &str)] = &[
        (
            "PRICE_FILTER",
            "tickSize",
            "0".into(),
            "PRICE_FILTER.tickSize",
        ),
        (
            "PRICE_FILTER",
            "tickSize",
            "0.1x".into(),
            "PRICE_FILTER.tickSize",
        ),
        (
            "PRICE_FILTER",
            "tickSize",
            0.1.into(),
            "PRICE_FILTER.tickSize",
        ),
        ("LOT_SIZE", "stepSize", "-0.001".into(), "LOT_SIZE.stepSize"),
        ("LOT_SIZE", "stepSize", "0".into(), "LOT_SIZE.stepSize"),
        ("LOT_SIZE", "minQty", "0".into(), "LOT_SIZE.minQty"),
        // Off the step grid: (quantity - minQty) must be a whole number of steps, so the
        // smallest order is not a whole-step size.
        ("LOT_SIZE", "minQty", "0.0015".into(), "LOT_SIZE.minQty"),
        ("LOT_SIZE", "maxQty", "-1".into(), "LOT_SIZE.maxQty"),
        ("LOT_SIZE", "maxQty", "1e30".into(), "LOT_SIZE.maxQty"),
        (
            "MIN_NOTIONAL",
            "notional",
            "-5".into(),
            "MIN_NOTIONAL.notional",
        ),
        (
            "MIN_NOTIONAL",
            "notional",
            "0.0000000001".into(),
            "MIN_NOTIONAL.notional",
        ),
        (
            "MAX_NUM_ORDERS",
            "limit",
            "200".into(),
            "MAX_NUM_ORDERS.limit",
        ),
        (
            "MAX_NUM_ORDERS",
            "limit",
            (-1).into(),
            "MAX_NUM_ORDERS.limit",
        ),
        (
            "MAX_NUM_ORDERS",
            "limit",
            5_000_000_000u64.into(),
            "MAX_NUM_ORDERS.limit",
        ),
    ];
    for (kind, field, value, part) in filter_cases {
        let answer = with_btc(|m| drop(filter(m, kind).insert((*field).into(), value.clone())));
        assert_eq!(discover(&answer), malformed(part), "{kind}.{field}={value}");
    }
    let cases: &[(&str, Value, &str)] = &[
        ("contractType", 7.into(), "contractType"),
        ("status", "PAUSED".into(), "status"),
        ("status", 1.into(), "status"),
        ("symbol", 7.into(), "symbol"),
        ("quoteAsset", "".into(), "quoteAsset"),
        ("marginAsset", 1.into(), "marginAsset"),
        ("filters", json!({}), "filters"),
        ("filters", json!([1]), "filters: a filter is not an object"),
        (
            "filters",
            json!([{"tickSize": "0.1"}]),
            "filters: filterType",
        ),
    ];
    for (field, value, part) in cases {
        let answer = with_btc(|m| drop(m.insert((*field).into(), value.clone())));
        assert_eq!(discover(&answer), malformed(part), "{field}={value}");
    }
    // Two filters of one type could disagree: the answer is refused, not read either way.
    let twice = with_btc(|m| {
        let tick = json!({"filterType": "PRICE_FILTER", "tickSize": "0.5"});
        filters(m).push(tick);
    });
    assert_eq!(discover(&twice), malformed("filters: a filterType twice"));
    // A symbol the decode scope refuses is refused as the scope says.
    let empty = with_btc(|m| drop(m.insert("symbol".into(), "".into())));
    let refused = Err(PlanError::Decode(DecodeError::IdRefused(IdError::Empty)));
    assert_eq!(discover(&empty), refused);
}

#[test]
fn each_documented_status_of_a_perpetual_reads_as_what_it_allows() {
    for (status, reads) in [
        ("TRADING", TradingStatus::Trading),
        ("TRADING_CANCEL_ONLY", TradingStatus::CancelOnly),
        // Not open to new orders, or not yet: halted, never trading.
        ("PENDING_TRADING", TradingStatus::Halted),
        ("TRADING_HALT", TradingStatus::Halted),
        ("PRE_DELIVERING", TradingStatus::Halted),
        ("DELIVERING", TradingStatus::Halted),
        ("PRE_SETTLE", TradingStatus::Halted),
        ("SETTLING", TradingStatus::Halted),
        ("DELIVERED", TradingStatus::Delisted),
        ("CLOSE", TradingStatus::Delisted),
    ] {
        let answer = with_btc(|m| drop(m.insert("status".into(), status.into())));
        assert_eq!(discover(&answer).unwrap()[0].status, reads, "{status}");
    }
}

#[test]
fn an_answer_that_is_not_the_documented_document_never_becomes_drafts() {
    assert_eq!(
        discover("not json"),
        malformed("exchangeInfo response is not JSON")
    );
    assert_eq!(
        discover("[]"),
        malformed("exchangeInfo response is not an object")
    );
    assert_eq!(discover(r#"{"symbols":{}}"#), malformed("symbols"));
    assert_eq!(
        discover(r#"{"symbols":[1]}"#),
        malformed("symbols: a symbol is not an object")
    );
    // An answer that is not a 2xx response never reaches the parser.
    let failed = discover_with(Err(HttpFailure::TimedOut));
    let (tag, failure) = (tag(), HttpFailure::TimedOut);
    assert_eq!(failed, Err(PlanError::Http { tag, failure }));
    let body = exchange_info();
    let limited = discover_with(Ok(HttpResponse {
        status: 429,
        headers: &[],
        body: body.as_bytes(),
    }));
    assert_eq!(limited, Err(PlanError::Status { tag, status: 429 }));
}

#[test]
fn a_java_era_x_usdt_ticker_names_the_xusdt_perpetual_and_no_other_form_is_read() {
    let drafts = discover(&exchange_info()).unwrap();
    let rule = |s| BinanceUsdm.parse_fbc_common_symbol(s);
    let btc = rule("BTC/USDT").unwrap();
    // The key the BTCUSDT draft states, exactly: BTCUSDC states BTC/USDC, another key.
    assert_eq!(btc, perp("BTC", "USDT"));
    let named = drafts.iter().filter(|d| d.asset == btc);
    assert_eq!(
        Vec::from_iter(named.map(|d| d.venue_symbol.as_wire())),
        ["BTCUSDT"]
    );
    assert_eq!(rule("ETH/USDT"), Ok(perp("ETH", "USDT")));

    // The legacy ticker and the BTCUSDT draft resolve to one listing under the seeded aliases
    // (USDT=USD), and to the same one with no aliases at all.
    let venue = VenueId::new(3);
    let listing = Listing {
        id: InstrumentId::new(7),
        underlying: UnderlyingId::new(1),
    };
    for aliases in [AliasTable::seeded(), AliasTable::empty()] {
        let mut resolver = InstrumentResolver::new(aliases);
        resolver.list(venue, drafts[0].asset, listing).unwrap();
        assert_eq!(resolver.resolve(venue, btc), Ok(listing.id));
    }
    let mut resolver = InstrumentResolver::new(AliasTable::empty());
    resolver.list(venue, drafts[2].asset, listing).unwrap();
    let unlisted = Err(ResolveError::NotListed { venue, key: btc });
    assert_eq!(resolver.resolve(venue, btc), unlisted);

    // Binance's FBC rule reads X/USDT alone: another quote is unmapped (under the seeded
    // aliases BTC/USDC would name the BTC/USDT listing), another form is not FBC's common form.
    for other in ["BTC/USDC", "BTC/USD", "BTC/BUSD"] {
        assert_eq!(rule(other), Err(SymbolError::Unmapped), "{other}");
    }
    for other in [
        "BTCUSDT",
        "BTC",
        "BTC-USDT",
        "BTC/USDT/X",
        "/USDT",
        "BTC/",
        "",
    ] {
        assert_eq!(rule(other), Err(SymbolError::NotCommonForm), "{other}");
    }
}

#[test]
fn a_perpetual_whose_asset_names_do_not_fit_an_asset_symbol_is_passed_over_alone() {
    // The core's asset symbol holds at most sixteen bytes of printable ASCII. A longer or
    // non-ASCII name cannot be keyed. It is passed over
    // before its symbol and filters are read; every other symbol is still discovered (Codex
    // r4311120955, Reviewer B RB-1). An empty name, or one that is not a string, is still
    // malformed.
    let expected = ["BTCUSDT", "ETHUSDT", "BTCUSDC", "OLDUSDT", "1000000MOGUSDT"];
    for field in ["baseAsset", "quoteAsset", "marginAsset"] {
        for name in [
            "ABCDEFGHIJKLMNOPQ",
            "币安人生",
            "BTC\u{e9}",
            "BT C",
            "\u{7f}",
        ] {
            let unkeyable = with_btc(|m| {
                m.insert(field.into(), name.into());
                m.insert("symbol".into(), "".into());
                filters(m).clear();
            });
            let drafts = discover(&unkeyable).unwrap();
            assert_eq!(symbols(&drafts), expected[1..], "{field}={name:?}");
        }
    }
    let listed = with_symbol(8, |m| drop(m.insert("filters".into(), json!(7))));
    assert_eq!(symbols(&discover(&listed).unwrap()), expected);
}
