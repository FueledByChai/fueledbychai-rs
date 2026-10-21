//! FBC-e3g7: public `GET /refdata` supplies exact QFEX perpetual specs and a Java-era ticker
//! rule, with missing or malformed values refused inside the decode scope.

use std::time::Duration;

use fbc_core::{
    AliasTable, AssetKey, AssetSym, ConfigError, DecodeError, Effect, FundingSpec, HttpFailure,
    HttpMethod, HttpResponse, InstrumentId, InstrumentKind, InstrumentResolver, Listing, Lots,
    OpKind, PlanError, PriceGrid, RateCharge, ResolveError, SizeStep, SymbolError, TradingStatus,
    TrafficClass, UnderlyingId, VenueConfig, VenueFactory, VenueId, WallNs, WireSlice, WireUrl,
    dispatch_market_data,
};
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex, discover::REFDATA_TAG};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const VENUE: VenueId = VenueId::new(4);
const AAPL: InstrumentId = InstrumentId::new(1);

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com/");
    cfg.insert(KEY_REST_URL, "https://api.qfex.com/");
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    cfg
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../../fixtures/qfex/rest/refdata.json")).unwrap()
}

fn discover_with(
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<Vec<fbc_core::InstrumentSpecDraft>, PlanError> {
    let plan = Qfex.discover(&cfg()).unwrap();
    let caps = Qfex.caps(&cfg()).unwrap();
    dispatch_market_data(&caps, |scope| plan.parse(&[(REFDATA_TAG, answer)], scope))
        .map(|step| step.done().expect("one round"))
}

fn decode(doc: &Value) -> Result<Vec<fbc_core::InstrumentSpecDraft>, PlanError> {
    let body = doc.to_string();
    discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    }))
}

fn decode_body(body: &[u8]) -> Result<Vec<fbc_core::InstrumentSpecDraft>, PlanError> {
    discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body,
    }))
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

fn sym(s: &str) -> AssetSym {
    AssetSym::new(s).unwrap()
}

fn key(base: &str, quote: &str) -> AssetKey {
    AssetKey {
        base: sym(base),
        quote: sym(quote),
        kind: InstrumentKind::Perpetual,
    }
}

#[test]
fn plan_is_one_public_get_with_configured_origin_timeout_and_no_credentials() {
    let plan = Qfex.discover(&cfg()).unwrap();
    assert_eq!(
        plan.requests(),
        &[Effect::Http {
            tag: REFDATA_TAG,
            req: fbc_core::HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain("https://api.qfex.com/refdata"),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: Duration::from_secs(5),
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Rest, None),
        }]
    );
    let mut stub = cfg();
    stub.insert(KEY_REST_URL, "http://127.0.0.1:3456/");
    let plan = Qfex.discover(&stub).unwrap();
    let [Effect::Http { req, .. }] = plan.requests() else {
        panic!("one GET")
    };
    assert_eq!(req.url, WireUrl::plain("http://127.0.0.1:3456/refdata"));
    for origin in [
        "http://localhost:3456",
        "http://[::1]:3456",
        "http://127.23.45.67:3456",
    ] {
        let mut cfg = cfg();
        cfg.insert(KEY_REST_URL, origin);
        let plan = Qfex.discover(&cfg).unwrap();
        let [Effect::Http { req, .. }] = plan.requests() else {
            panic!("one GET")
        };
        assert_eq!(req.url, WireUrl::plain(format!("{origin}/refdata")));
    }
    let mut seconds = cfg();
    seconds.insert(KEY_REST_TIMEOUT, "2s");
    let plan = Qfex.discover(&seconds).unwrap();
    let [Effect::Http { timeout, .. }] = plan.requests() else {
        panic!("one GET")
    };
    assert_eq!(*timeout, Duration::from_secs(2));
}

#[test]
fn discovery_reads_exact_grids_sizes_assets_and_all_known_statuses() {
    let drafts = decode(&fixture()).unwrap();
    assert_eq!(drafts.len(), 4);
    let aapl = &drafts[0];
    assert_eq!(aapl.venue_symbol.as_wire(), "AAPL-USD");
    assert_eq!(aapl.asset, key("AAPL", "USD"));
    // The live public list reuses clobPairId, so symbol rather than that field is the key.
    assert_eq!(aapl.native_id, None);
    assert_eq!(aapl.price_grid, PriceGrid::fixed(dec("0.01")).unwrap());
    assert_eq!(aapl.size_step, SizeStep::new(dec("0.001")).unwrap());
    assert_eq!(aapl.min_size, Lots::new(100).unwrap());
    assert_eq!(
        aapl.max_order_size,
        Some(Lots::new(10_000_000_000).unwrap())
    );
    assert_eq!(aapl.settle_ccy, sym("USDC"));
    assert_eq!(aapl.status, TradingStatus::Trading);
    assert_eq!(aapl.multiplier, Decimal::ONE);
    assert_eq!(aapl.quote_grid, None);
    assert_eq!(aapl.min_notional, None);
    assert_eq!(aapl.position_limit, None);
    assert_eq!(aapl.price_band, None);
    assert_eq!(aapl.max_open_orders, None);
    assert_eq!(aapl.funding, FundingSpec::Unknown);
    assert_eq!(aapl.public_fees, None);
    assert_eq!(drafts[1].asset, key("ANTHROPIC", "USD"));
    assert_eq!(drafts[1].min_size, Lots::new(2).unwrap());
    assert_eq!(drafts[1].status, TradingStatus::Trading);
    assert_eq!(drafts[2].status, TradingStatus::Halted);
    assert_eq!(drafts[3].status, TradingStatus::Delisted);
    assert_eq!(
        drafts[3].max_order_size,
        Some(Lots::new(10_000_000).unwrap())
    );
}

#[test]
fn ticker_rule_and_alias_table_resolve_verified_qfex_names() {
    assert_eq!(
        Qfex.parse_fbc_common_symbol("AAPL/USD"),
        Ok(key("AAPL", "USD"))
    );
    assert_eq!(
        Qfex.parse_fbc_common_symbol(" aapl/usd "),
        Ok(key("AAPL", "USD"))
    );
    assert_eq!(
        Qfex.parse_fbc_common_symbol("GOLD/USDT"),
        Ok(key("GOLD", "USDT"))
    );
    assert_eq!(
        Qfex.parse_fbc_common_symbol("ANTHROPIC/USD"),
        Ok(key("ANTHROPIC", "USD"))
    );
    for bad in ["AAPL-USD", "AAPL", "AAPL/", "/USD", "AAPL/USD/X"] {
        assert_eq!(
            Qfex.parse_fbc_common_symbol(bad),
            Err(SymbolError::NotCommonForm)
        );
    }
    let drafts = decode(&fixture()).unwrap();
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    resolver
        .list(
            VENUE,
            key("AAPL", "USD"),
            Listing {
                id: AAPL,
                underlying: UnderlyingId::new(1),
            },
        )
        .unwrap();
    resolver
        .list(
            VENUE,
            key("ANTHROPIC", "USD"),
            Listing {
                id: InstrumentId::new(2),
                underlying: UnderlyingId::new(2),
            },
        )
        .unwrap();
    resolver
        .list(
            VENUE,
            key("GOLD", "USD"),
            Listing {
                id: InstrumentId::new(3),
                underlying: UnderlyingId::new(3),
            },
        )
        .unwrap();
    assert_eq!(
        resolver.resolve(VENUE, Qfex.parse_fbc_common_symbol("AAPL/USDT").unwrap()),
        Ok(AAPL)
    );
    let spec = resolver
        .spec(VENUE, drafts[0].clone(), 1, WallNs(0))
        .unwrap();
    assert_eq!(spec.id, AAPL);
    assert_eq!(spec.quote_ccy, sym("USD"));
    assert_eq!(spec.settle_ccy, sym("USDC"));
    assert_eq!(
        resolver.resolve(
            VENUE,
            Qfex.parse_fbc_common_symbol("ANTHROPIC/USDC").unwrap()
        ),
        Ok(InstrumentId::new(2))
    );
    assert_eq!(
        resolver.resolve(VENUE, Qfex.parse_fbc_common_symbol("GOLD/USDT").unwrap()),
        Ok(InstrumentId::new(3))
    );
    assert_eq!(
        resolver.resolve(VENUE, key("NVDA", "USD")),
        Err(ResolveError::NotListed {
            venue: VENUE,
            key: key("NVDA", "USD")
        })
    );
}

#[test]
fn missing_and_malformed_required_values_refuse_the_whole_answer() {
    for field in [
        "symbol",
        "base_asset",
        "quote_asset",
        "margin_asset",
        "tick_size",
        "lot_size",
        "min_quantity",
        "max_quantity",
        "status",
    ] {
        let mut doc = fixture();
        doc["data"][0].as_object_mut().unwrap().remove(field);
        assert_eq!(
            decode(&doc),
            Err(PlanError::Missing(field)),
            "missing {field}"
        );
        let mut doc = fixture();
        doc["data"][0][field] = Value::Null;
        assert_eq!(decode(&doc), Err(PlanError::Missing(field)), "null {field}");
    }
    let mut doc = fixture();
    doc["data"][0]["symbol"] = json!("AAPL-KRW");
    assert_eq!(
        decode(&doc),
        Err(PlanError::Decode(DecodeError::Malformed("symbol")))
    );
    for (field, value) in [
        ("tick_size", json!("0")),
        ("lot_size", json!("0")),
        ("min_quantity", json!("0.0005")),
        ("max_quantity", json!("0.01")),
        ("max_quantity", json!("bad")),
        ("status", json!("TRADING")),
    ] {
        let mut doc = fixture();
        doc["data"][0][field] = value;
        assert_eq!(
            decode(&doc),
            Err(PlanError::Decode(DecodeError::Malformed(field))),
            "{field}"
        );
    }
    assert_eq!(
        decode(&json!({"type":"refdata"})),
        Err(PlanError::Missing("data"))
    );
    assert_eq!(
        decode(&json!({"type":"other","data":[]})),
        Err(PlanError::Decode(DecodeError::Malformed("type")))
    );
    assert_eq!(decode(&json!({"type":"refdata","data":[]})), Ok(Vec::new()));
    assert_eq!(decode(&json!({"data":[]})), Err(PlanError::Missing("type")));
    for (body, part) in [
        (b"not json".as_slice(), "refdata JSON"),
        (b"[]".as_slice(), "refdata object"),
    ] {
        assert_eq!(
            decode_body(body),
            Err(PlanError::Decode(DecodeError::Malformed(part)))
        );
    }
    assert_eq!(
        decode(&json!({"type":"refdata","data":{}})),
        Err(PlanError::Decode(DecodeError::Malformed("data")))
    );
    assert_eq!(
        decode(&json!({"type":"refdata","data":[false]})),
        Err(PlanError::Decode(DecodeError::Malformed("data item")))
    );
    for (field, value) in [
        ("base_asset", json!("ABCDEFGHIJKLMNOPQ")),
        ("margin_asset", json!("")),
        ("quote_asset", json!(42)),
        ("tick_size", json!("-0.01")),
        ("lot_size", json!("-0.001")),
        ("min_quantity", json!("0")),
        ("min_quantity", json!("99999999999999999999999999")),
        ("max_quantity", json!("-1")),
    ] {
        let mut doc = fixture();
        doc["data"][0][field] = value;
        assert_eq!(
            decode(&doc),
            Err(PlanError::Decode(DecodeError::Malformed(field))),
            "{field}"
        );
    }
}

#[test]
fn discovery_refuses_invalid_configuration_and_http_failures() {
    let mut missing = VenueConfig::new();
    missing.insert(KEY_MDS_URL, "wss://mds.qfex.com/");
    missing.insert(KEY_REST_TIMEOUT, "5000ms");
    assert!(matches!(
        Qfex.discover(&missing),
        Err(fbc_core::VenueError::Config(ConfigError::Missing(
            KEY_REST_URL
        )))
    ));
    for url in [
        "http://api.qfex.com",
        "https://api.qfex.com/path",
        "https://user@api.qfex.com",
        "https://api.qfex.com?x=1",
        "https://bad..host",
        "http://192.168.1.2",
        "http://127.example.com",
        "http://127.0.0.1.example.com",
    ] {
        let mut bad = cfg();
        bad.insert(KEY_REST_URL, url);
        assert!(Qfex.discover(&bad).is_err(), "{url}");
    }
    for timeout in ["0ms", "-1s", "5", "1.5s", "18446744073709551615s"] {
        let mut bad = cfg();
        bad.insert(KEY_REST_TIMEOUT, timeout);
        assert!(Qfex.discover(&bad).is_err(), "{timeout}");
    }
    assert_eq!(
        discover_with(Err(HttpFailure::TimedOut)),
        Err(PlanError::Http {
            tag: REFDATA_TAG,
            failure: HttpFailure::TimedOut
        })
    );
}
