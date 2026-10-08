//! FBC-l5o: Paradex discovers its instruments with one `GET /markets` under the REST base, a
//! plan of effects whose parser builds an `InstrumentSpecDraft` per perpetual inside the decode
//! scope and refuses an answer that leaves out a required field, by name; and its Java-era
//! ticker rule reads `X/USDT` as the key of `X-USD-PERP`, refusing every other ticker.
//!
//! The answer is `fixtures/paradex/rest/markets.json`, hand-built in the shape docs.paradex.trade
//! documents for "List available markets".

use std::time::Duration;

use fbc_core::{
    AliasTable, AssetKey, AssetSym, ConfigError, DecodeError, Effect, FundingSpec, HttpFailure,
    HttpMethod, HttpRequest, HttpResponse, IdError, InstrumentId, InstrumentKind,
    InstrumentResolver, InstrumentSpecDraft, Listing, Lots, Money, OpKind, PlanError, PriceGrid,
    RateCharge, SizeStep, SymbolError, TradingStatus, TrafficClass, UnderlyingId, VenueConfig,
    VenueError, VenueFactory, VenueId, WireSlice, WireUrl, dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{REST_URL, TIMEOUT};
use fbc_venue_paradex::discover::MARKETS_TAG;
use fbc_venue_paradex::factory::caps;
use rust_decimal::Decimal;
use serde_json::Value;

const BASE: &str = "https://api.testnet.paradex.trade/v1";

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(REST_URL, BASE);
    cfg.insert(TIMEOUT, "5s");
    cfg
}

fn markets() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/rest/markets.json");
    std::fs::read_to_string(path).expect("the hand-built /markets answer")
}

/// Discovery's plan answered with `answer`, parsed in the decode scope market data is
/// dispatched with (discovery holds no engine namespace).
fn discover_with(
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let plan = ParadexFactory.discover(&cfg()).unwrap();
    dispatch_market_data(&caps(), |scope| plan.parse(&[(MARKETS_TAG, answer)], scope))
        .map(|step| step.done().expect("a one-round plan ends in its result"))
}

fn discover(body: &str) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    }))
}

/// The fixture with `edit` applied to its first market, BTC-USD-PERP.
fn with_btc(edit: impl FnOnce(&mut serde_json::Map<String, Value>)) -> String {
    let mut doc: Value = serde_json::from_str(&markets()).unwrap();
    edit(doc["results"][0].as_object_mut().unwrap());
    doc.to_string()
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

#[test]
fn discovery_is_one_get_of_markets_under_the_rest_base_and_nothing_else() {
    let plan = ParadexFactory.discover(&cfg()).unwrap();
    let expected = Effect::Http {
        tag: MARKETS_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(format!("{BASE}/markets")),
            headers: vec![],
            body: WireSlice::plain(vec![]),
        },
        rpc: None,
        timeout: Duration::from_secs(5),
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Rest, None),
    };
    assert_eq!(plan.requests(), [expected]);

    // A trailing slash on the base is the same base; a loopback stub may be plain HTTP.
    let mut stub = cfg();
    stub.insert(REST_URL, "http://127.0.0.1:8080/v1/");
    let plan = ParadexFactory.discover(&stub).unwrap();
    let Effect::Http { req, .. } = &plan.requests()[0] else {
        panic!("an HTTP request")
    };
    assert_eq!(req.url, WireUrl::plain("http://127.0.0.1:8080/v1/markets"));
}

#[test]
fn discovery_refuses_a_configuration_it_cannot_send_by_the_key_at_fault() {
    // The test configuration with `key` set to `value`, or left out.
    let refused = |key: &str, value: Option<&str>| {
        let (base, mut changed) = (cfg(), VenueConfig::new());
        for k in [REST_URL, TIMEOUT] {
            let v = if k == key { value } else { base.get(k) };
            v.map(|v| changed.insert(k, v));
        }
        ParadexFactory.discover(&changed).err()
    };
    let missing = |key| Some(VenueError::Config(ConfigError::Missing(key)));
    assert_eq!(refused(REST_URL, None), missing(REST_URL));
    assert_eq!(refused(TIMEOUT, None), missing(TIMEOUT));
    for bad in [
        "ftp://api.testnet.paradex.trade/v1",
        "http://api.testnet.paradex.trade/v1",
        "https://api.testnet.paradex.trade/v1?x=1",
        "https://api.testnet.paradex.trade/v1#top",
        "https://user@api.testnet.paradex.trade/v1",
        "https://api.testnet.paradex.trade",
        "https://api.testnet.paradex.trade/v2",
        "https:///v1",
    ] {
        let Some(VenueError::Config(ConfigError::Invalid { key, .. })) =
            refused(REST_URL, Some(bad))
        else {
            panic!("{bad} is refused")
        };
        assert_eq!(key, REST_URL, "{bad}");
    }
    for bad in ["5", "0s", "fast"] {
        let Some(VenueError::Config(ConfigError::Invalid { key, .. })) =
            refused(TIMEOUT, Some(bad))
        else {
            panic!("{bad} is refused")
        };
        assert_eq!(key, TIMEOUT, "{bad}");
    }
}

#[test]
fn the_documented_answer_decodes_into_one_draft_per_perpetual_in_the_decode_scope() {
    let drafts = discover(&markets()).unwrap();
    // The option and the RFQ-only perpetual are not order-book perpetuals: neither is a draft.
    let symbols = drafts.iter().map(|d| d.venue_symbol.as_wire());
    assert_eq!(Vec::from_iter(symbols), ["BTC-USD-PERP", "ETH-USD-PERP"]);
    let venue_symbol = dispatch_market_data(&caps(), |scope| scope.venue_symbol("BTC-USD-PERP"));
    let expected = InstrumentSpecDraft {
        asset: perp("BTC", "USD"),
        venue_symbol: venue_symbol.unwrap(),
        native_id: None,
        price_grid: PriceGrid::fixed(dec("0.1")).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(dec("0.00001")).unwrap(),
        // One order_size_increment, the smallest size the venue states.
        min_size: Lots::new(1).unwrap(),
        min_notional: Some(Money::new(10_000_000_000, sym("USD"))),
        // 50 BTC in steps of 0.00001.
        max_order_size: Some(Lots::new(5_000_000).unwrap()),
        // 100.000015 BTC floors to whole steps.
        position_limit: Some(Lots::new(10_000_001).unwrap()),
        price_band: Some(fbc_core::Bps(500.0)),
        max_open_orders: Some(100),
        multiplier: Decimal::ONE,
        settle_ccy: sym("USDC"),
        funding: FundingSpec::Known {
            interval: Duration::from_secs(8 * 3600),
            cap: Some(fbc_core::Bps(500.0)),
        },
        public_fees: None,
        status: TradingStatus::Trading,
    };
    assert_eq!(drafts[0], expected);
    let eth = &drafts[1];
    assert_eq!(eth.asset, perp("ETH", "USD"));
    assert_eq!(eth.max_open_orders, Some(50));
    assert_eq!(eth.price_band, Some(fbc_core::Bps(250.0)));
    let hourly = FundingSpec::Known {
        interval: Duration::from_secs(3600),
        cap: Some(fbc_core::Bps(500.0)),
    };
    assert_eq!(eth.funding, hourly);

    // An empty list is an empty discovery, not a refusal.
    assert_eq!(discover(r#"{"results":[]}"#), Ok(vec![]));
}

#[test]
fn a_market_missing_a_required_field_refuses_the_whole_answer_by_name() {
    for field in [
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
    ] {
        // Left out, or stated as null: the complete ETH market is refused with it.
        let absent = with_btc(|m| drop(m.remove(field)));
        assert_eq!(discover(&absent), Err(PlanError::Missing(field)), "{field}");
        let null = with_btc(|m| drop(m.insert(field.into(), Value::Null)));
        assert_eq!(
            discover(&null),
            Err(PlanError::Missing(field)),
            "{field} null"
        );
    }
    assert_eq!(discover("{}"), Err(PlanError::Missing("results")));
}

#[test]
fn a_value_stated_but_invalid_is_malformed_and_named() {
    let cases: &[(&str, Value, &str)] = &[
        ("asset_kind", "SWAP".into(), "asset_kind"),
        ("trading_mode", "AUCTION".into(), "trading_mode"),
        ("symbol", 7.into(), "symbol"),
        ("base_currency", "TOOLONGBASE".into(), "base_currency"),
        ("quote_currency", "".into(), "quote_currency"),
        ("settlement_currency", 1.into(), "settlement_currency"),
        ("price_tick_size", "0".into(), "price_tick_size"),
        ("price_tick_size", "0.1x".into(), "price_tick_size"),
        ("price_tick_size", 0.1.into(), "price_tick_size"),
        (
            "order_size_increment",
            "-0.001".into(),
            "order_size_increment",
        ),
        ("min_notional", "-1".into(), "min_notional"),
        ("min_notional", "0.0000000001".into(), "min_notional"),
        ("max_order_size", "-5".into(), "max_order_size"),
        ("max_order_size", "1e30".into(), "max_order_size"),
        ("position_limit", "much".into(), "position_limit"),
        ("price_bands_width", "-0.05".into(), "price_bands_width"),
        ("max_open_orders", "100".into(), "max_open_orders"),
        ("max_open_orders", (-1).into(), "max_open_orders"),
        (
            "max_open_orders",
            5_000_000_000u64.into(),
            "max_open_orders",
        ),
        ("funding_period_hours", 0.into(), "funding_period_hours"),
        ("funding_period_hours", (-8).into(), "funding_period_hours"),
        (
            "funding_period_hours",
            0.0001.into(),
            "funding_period_hours",
        ),
        ("funding_period_hours", "8".into(), "funding_period_hours"),
        ("max_funding_rate", "-0.05".into(), "max_funding_rate"),
    ];
    for (field, value, part) in cases {
        let answer = with_btc(|m| drop(m.insert((*field).into(), value.clone())));
        assert_eq!(discover(&answer), malformed(part), "{field}={value}");
    }
    // A fractional funding period of whole seconds is read: 1.5 hours.
    let answer = with_btc(|m| drop(m.insert("funding_period_hours".into(), 1.5.into())));
    let Ok(drafts) = discover(&answer) else {
        panic!("1.5 hours is 5400 s")
    };
    let FundingSpec::Known { interval, .. } = drafts[0].funding else {
        panic!("stated")
    };
    assert_eq!(interval, Duration::from_secs(5400));
    // A symbol the decode scope refuses is refused as the scope says.
    let empty = with_btc(|m| drop(m.insert("symbol".into(), "".into())));
    let refused = Err(PlanError::Decode(DecodeError::IdRefused(IdError::Empty)));
    assert_eq!(discover(&empty), refused);
}

#[test]
fn an_answer_that_is_not_the_documented_document_never_becomes_drafts() {
    assert_eq!(
        discover("not json"),
        malformed("markets response is not JSON")
    );
    assert_eq!(
        discover("[]"),
        malformed("markets response is not an object")
    );
    assert_eq!(discover(r#"{"results":{}}"#), malformed("results"));
    assert_eq!(
        discover(r#"{"results":[1]}"#),
        malformed("results: a market is not an object")
    );
    // An answer that is not a 2xx response never reaches the parser.
    let failed = discover_with(Err(HttpFailure::TimedOut));
    let (tag, failure) = (MARKETS_TAG, HttpFailure::TimedOut);
    assert_eq!(failed, Err(PlanError::Http { tag, failure }));
    let body = markets();
    let unavailable = discover_with(Ok(HttpResponse {
        status: 503,
        headers: &[],
        body: body.as_bytes(),
    }));
    assert_eq!(unavailable, Err(PlanError::Status { tag, status: 503 }));
}

#[test]
fn a_java_era_x_usdt_ticker_names_the_x_usd_perp_draft_and_no_other_form_is_read() {
    let drafts = discover(&markets()).unwrap();
    let rule = |s| ParadexFactory.parse_fbc_common_symbol(s);
    let btc = rule("BTC/USDT").unwrap();
    // The key the BTC-USD-PERP draft states, exactly: no alias is needed to find it.
    assert_eq!(btc, perp("BTC", "USD"));
    let named = drafts.iter().filter(|d| d.asset == btc);
    let named = Vec::from_iter(named.map(|d| d.venue_symbol.as_wire()));
    assert_eq!(named, ["BTC-USD-PERP"]);
    assert_eq!(rule("ETH/USDT"), Ok(perp("ETH", "USD")));

    // The legacy ticker and the draft resolve to one listing, under no aliases at all.
    let venue = VenueId::new(2);
    let listing = Listing {
        id: InstrumentId::new(7),
        underlying: UnderlyingId::new(1),
    };
    let mut resolver = InstrumentResolver::new(AliasTable::empty());
    resolver.list(venue, drafts[0].asset, listing).unwrap();
    assert_eq!(resolver.resolve(venue, btc), Ok(listing.id));

    // Paradex's FBC rule reads X/USDT alone: another quote is unmapped, another form is not
    // FBC's common form.
    for other in ["BTC/USD", "BTC/USDC", "BTC/EUR"] {
        assert_eq!(rule(other), Err(SymbolError::Unmapped), "{other}");
    }
    for other in ["BTC-USD-PERP", "BTCUSDT", "BTC/USDT/X", "/USDT", "BTC/", ""] {
        assert_eq!(rule(other), Err(SymbolError::NotCommonForm), "{other}");
    }
}
