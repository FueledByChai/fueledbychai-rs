//! FBC-e3g7: public `GET /refdata` supplies exact QFEX perpetual specs and a Java-era ticker
//! rule, with missing or malformed values refused inside the decode scope.
//!
//! FBC-99w5 (decision 0136): each symbol's `order_types` and `order_time_in_force` become its
//! draft's `order_shapes`, exactly the set it lists: `LIMIT` a plain limit, `ALO` the post-only
//! form, `MARKET` a market order, and `GTC`, `IOC` and `FOK`; any other name is ignored, an
//! empty or null list admits nothing of its kind, a missing list refuses the answer as
//! `Missing` and anything but null or an array of strings as malformed.

use std::time::Duration;

use fbc_core::{
    AliasTable, AssetKey, AssetSym, ConfigError, DecodeError, Effect, FundingSpec, HttpFailure,
    HttpMethod, HttpResponse, InstrumentId, InstrumentKind, InstrumentResolver, Listing, Lots,
    OpKind, OrderKindTag, OrderShapes, PlanError, PriceGrid, RateCharge, ResolveError, SizeStep,
    SymbolError, TagSet, TifTag, TradingStatus, TrafficClass, UnderlyingId, VenueConfig,
    VenueFactory, VenueId, WallNs, WireSlice, WireUrl, dispatch_market_data,
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

/// The variant beside it: a symbol with no IOC, one listing null for both, one listing names
/// outside the mapping (and duplicates), and one listing only names outside it.
fn order_lists() -> Value {
    serde_json::from_str(include_str!(
        "../../../../fixtures/qfex/rest/refdata_order_lists.json"
    ))
    .unwrap()
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
    // The symbol's lists reach the spec the encoder reads (0136).
    assert_eq!(spec.order_shapes, drafts[0].order_shapes);
    assert!(spec.order_shapes.is_some());
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
    assert_eq!(
        decode(&json!({"type":"refdata","data":null})),
        Ok(Vec::new())
    );
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

// ----- The order types and times in force each symbol lists (FBC-99w5, decision 0136) -----

fn shapes(kinds: &[OrderKindTag], post_only: bool, tifs: &[TifTag]) -> Option<OrderShapes> {
    Some(OrderShapes {
        kinds: TagSet::of(kinds),
        post_only,
        tifs: TagSet::of(tifs),
    })
}

/// The refdata channel page (https://docs.qfex.com/websocket/channels/mds/refdata), whose
/// payloads mirror the REST fields, lists `"order_types": ["LIMIT", "MARKET", "ALO"]` and
/// `"order_time_in_force": ["GTC", "IOC", "FOK"]`, the OrderType and OrderTimeInForce names of
/// the enums page (https://docs.qfex.com/api-reference/enums); post-only is the ALO order type.
#[test]
fn every_hand_built_refdata_entry_carries_both_lists_in_the_example_shape() {
    let names = |list: &Value| -> Vec<String> {
        list.as_array()
            .expect("an array")
            .iter()
            .map(|name| name.as_str().expect("a string").to_owned())
            .collect()
    };
    let main = fixture();
    let entries = main["data"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    for entry in entries {
        for list in ["order_types", "order_time_in_force"] {
            names(&entry[list]);
        }
    }
    // The first entry lists exactly the page's example.
    assert_eq!(
        names(&entries[0]["order_types"]),
        ["LIMIT", "MARKET", "ALO"]
    );
    assert_eq!(
        names(&entries[0]["order_time_in_force"]),
        ["GTC", "IOC", "FOK"]
    );
    // The variant carries both members on every entry too, arrays of strings but for the one
    // symbol listing null for both.
    let variant = order_lists();
    let entries = variant["data"].as_array().unwrap();
    assert_eq!(entries.len(), 4);
    let mut null = Vec::new();
    for entry in entries {
        let object = entry.as_object().unwrap();
        for list in ["order_types", "order_time_in_force"] {
            match &object[list] {
                Value::Null => null.push((entry["symbol"].as_str().unwrap(), list)),
                other => {
                    names(other);
                }
            }
        }
    }
    assert_eq!(
        null,
        [
            ("TSLA-USD", "order_types"),
            ("TSLA-USD", "order_time_in_force")
        ]
    );
}

#[test]
fn each_symbol_decodes_to_exactly_the_order_shapes_it_lists() {
    use OrderKindTag::{Limit, Market};
    use TifTag::{Fok, Gtc, Ioc};
    let drafts = decode(&fixture()).unwrap();
    let listed: Vec<_> = drafts
        .iter()
        .map(|d| (d.venue_symbol.as_wire(), d.order_shapes))
        .collect();
    assert_eq!(
        listed,
        [
            // LIMIT, MARKET and ALO with GTC, IOC and FOK: the page's example.
            ("AAPL-USD", shapes(&[Limit, Market], true, &[Gtc, Ioc, Fok])),
            // LIMIT without ALO: a plain limit, never post-only.
            (
                "ANTHROPIC-USD",
                shapes(&[Limit, Market], false, &[Gtc, Ioc, Fok])
            ),
            // ALO without LIMIT: the post-only form, no plain limit.
            ("NVDA-USD", shapes(&[], true, &[Gtc, Ioc])),
            // Empty arrays: nothing of either kind.
            ("GOLD-USD", shapes(&[], false, &[])),
        ]
    );
    let drafts = decode(&order_lists()).unwrap();
    let listed: Vec<_> = drafts
        .iter()
        .map(|d| (d.venue_symbol.as_wire(), d.order_shapes))
        .collect();
    assert_eq!(
        listed,
        [
            // No IOC.
            ("MSFT-USD", shapes(&[Limit, Market], true, &[Gtc, Fok])),
            // Null for both: nothing of either kind, as for empty arrays.
            ("TSLA-USD", shapes(&[], false, &[])),
            // TAKE_PROFIT, STOP_LOSS, STOP_MARKET, TWAP_LIMIT (the enums page's stop and TWAP
            // types), another spelling and an unknown string ignored; a repeat is one member.
            ("META-USD", shapes(&[Limit], false, &[Gtc])),
            // Only names outside the mapping: no kind and no post-only form.
            ("AMZN-USD", shapes(&[], false, &[Ioc])),
        ]
    );
    // Every other value each symbol states is read as before.
    assert!(drafts.iter().all(|d| d.status == TradingStatus::Trading));
    assert_eq!(drafts[1].asset, key("TSLA", "USD"));
    assert_eq!(drafts[1].min_size, Lots::new(1).unwrap());
}

#[test]
fn each_list_reads_on_its_own_and_one_null_admits_nothing_of_its_kind_alone() {
    use OrderKindTag::{Limit, Market};
    use TifTag::{Fok, Gtc, Ioc};
    let mut doc = fixture();
    doc["data"][0]["order_types"] = Value::Null;
    let drafts = decode(&doc).unwrap();
    assert_eq!(drafts[0].order_shapes, shapes(&[], false, &[Gtc, Ioc, Fok]));
    let mut doc = fixture();
    doc["data"][0]["order_time_in_force"] = json!([]);
    let drafts = decode(&doc).unwrap();
    assert_eq!(drafts[0].order_shapes, shapes(&[Limit, Market], true, &[]));
    // One name of each.
    let mut doc = fixture();
    doc["data"][0]["order_types"] = json!(["MARKET"]);
    doc["data"][0]["order_time_in_force"] = json!(["FOK"]);
    let drafts = decode(&doc).unwrap();
    assert_eq!(drafts[0].order_shapes, shapes(&[Market], false, &[Fok]));
}

#[test]
fn a_missing_list_refuses_the_answer_and_a_malformed_one_too() {
    for list in ["order_types", "order_time_in_force"] {
        // Missing from any entry, the first or a later one: the whole answer, market data
        // included, as for every field discovery reads.
        for entry in [0, 3] {
            let mut doc = fixture();
            doc["data"][entry].as_object_mut().unwrap().remove(list);
            assert_eq!(decode(&doc), Err(PlanError::Missing(list)), "{list}");
        }
        let mut doc = order_lists();
        doc["data"][1].as_object_mut().unwrap().remove(list);
        assert_eq!(decode(&doc), Err(PlanError::Missing(list)), "{list}");
        // Neither null nor an array of strings, or an item that is not a string.
        for value in [
            json!("LIMIT"),
            json!("GTC"),
            json!(42),
            json!(true),
            json!({}),
            json!({"LIMIT": true}),
            json!([1]),
            json!(["LIMIT", null]),
            json!(["GTC", 7]),
            json!([["LIMIT"]]),
            json!([{"name": "GTC"}]),
            json!([false]),
        ] {
            let mut doc = fixture();
            doc["data"][2][list] = value.clone();
            assert_eq!(
                decode(&doc),
                Err(PlanError::Decode(DecodeError::Malformed(list))),
                "{list}: {value}"
            );
        }
    }
}
