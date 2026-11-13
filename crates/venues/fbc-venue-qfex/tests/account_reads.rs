//! FBC-h3z1: the probe's account reads (decision 0132 item 7), built and decoded in the venue
//! crate so the probe (FBC-l931) writes no QFEX wire or credential code.
//!
//! - Nothing is built without a probe mode, or under a configuration its guard or decision 0122
//!   item 1's check refuses. These tests read under `preprod_probe`; `production_probe`'s guard
//!   (decision 0143 item 2) is held in `tests/probe_mode.rs`, through the same
//!   `probe::connections`.
//! - `GET /user/subaccounts` carries the four signed headers from `src/auth`, each `redact: true`,
//!   and no account header; `GET /user/positions` selects a subaccount by its redacted header and
//!   the primary account by none; both are charged `OpKind::Rest`.
//! - The trade socket's authentication names a subaccount's `account_id` and the primary's none,
//!   as the order-entry codec's does; `get_user_orders` encodes exactly at page sizes 1 and 1000,
//!   is refused at 0 and 1001, and is charged `Query` at weight 50.
//! - The listing, the positions and the page decode exactly and refuse what QFEX does not
//!   document; an err is no page, and a RateLimited one is the codec's rate hold; the
//!   authentication reply is success exactly where the codec takes it as success.
//! - The liveness hold's subscriptions (decision 0146) are the codec's own three, frame, charge
//!   and class, and none is built without a probe mode; a confirmation is read exactly where the
//!   codec reads one, and an err on a trade socket as a page reads one.
//! - The redaction spans cover every listed account id, every `user_id` and every balance value,
//!   and no `Debug`, `Display` or error shows a header value, an account id, a `user_id` or a
//!   balance value. A listing read back from the journal is refused, its ids blanked to no
//!   UUID; positions read back from it decode as live.
//!
//! Every key, account, id and answer here is synthetic: the answers are
//! `fixtures/qfex/private` (its `SYNTHETIC` file) or built from the shapes QFEX's pages show,
//! and each test names the page.

use std::fs;
use std::path::Path;
use std::time::Duration;

use fbc_core::{
    AccountKey, CidMatch, CidMint, ClientOrderId, ConfigError, DecodeScope, Effect, Effects,
    EncodeCtx, ExecCodec, ExecEvent, ExecSink, Header, HttpResponse, HttpTag, Inbound, MonoNs,
    Namespace, NamespaceLease, NonceBlock, OpKind, RateCharge, RawFrame, Secret, Secrets, Side,
    SignError, SpecTable, TrafficClass, VenueCaps, VenueConfig, VenueFactory, VenueMeta, WallNs,
    check_redactions, dispatch, encode_cid,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::account_reads::{
    self, AnswerError, BuildError, MAX_PAGE, NotAPage, OrdersPage, PAGE_WEIGHT, PageAsk, Positions,
    RateHold, SocketErr, Subaccounts,
};
use fbc_venue_qfex::auth::{self, AccountSelection, ReadAuth, TradeAuth};
use fbc_venue_qfex::exec::{CLIENT_ID_FORMAT, ErrCode, QfexExec, TRADE};
use fbc_venue_qfex::probe::{
    self, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, PREPROD_MDS,
    PREPROD_PROBE, PREPROD_REST, PREPROD_TRADE,
};
use fbc_venue_qfex::sign::HmacKey;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL};
use rust_decimal::Decimal;
use serde_json::Value;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// Another made-up public key: a credential that reaches another account.
const OTHER_PUBLIC: &str = "qfex_pub_SYNTHETIC_0002";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// The made-up subaccounts `subaccounts.json` lists; the first is the traded one.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
const SUB2: &str = "5a5a5a5a-0000-4000-8000-00000000beef";
/// A made-up subaccount no listing names.
const UNLISTED: &str = "5a5a5a5a-0000-4000-8000-0000000f00d5";
/// The made-up `user_id` of `all_orders_response.json`.
const USER: &str = "5a5a5a5a-0000-4000-8000-0000000000a1";
/// The made-up balance values of `positions.json`, as written.
const BALANCE: [&str; 9] = [
    "1000.25", "-3.5", "27.08", "10.75", "0.5", "-0.125", "963.42", "0.0", "12.5",
];
/// Our namespace.
const NS: Namespace = Namespace::new(7);

// ----- Configurations, readers and contexts -----

/// A probe configuration trading `account` on these three origins.
fn on(account: &str, trade: &str, rest: &str, mds: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, PREPROD_PROBE);
    cfg.insert(auth::TRADE_URL, trade);
    cfg.insert(KEY_REST_URL, rest);
    cfg.insert(KEY_MDS_URL, mds);
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    cfg.insert(KEY_PAGE_SIZE, "1000");
    cfg.insert(KEY_RESYNC_TIMEOUT, "30s");
    cfg.insert(KEY_RPC_TIMEOUT, "2500ms");
    cfg.insert(auth::ACCOUNT_ID, account);
    cfg.insert(
        auth::ACCOUNT_KEY,
        if account == "primary" { "4" } else { "9" },
    );
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn preprod(account: &str) -> VenueConfig {
    on(account, PREPROD_TRADE, PREPROD_REST, PREPROD_MDS)
}

fn loopback(account: &str) -> VenueConfig {
    on(
        account,
        "ws://127.0.0.1:9001",
        "http://127.0.0.1:9002",
        "ws://127.0.0.1:9003",
    )
}

fn with(mut cfg: VenueConfig, key: &str, value: &str) -> VenueConfig {
    cfg.insert(key, value);
    cfg
}

fn creds_for(public: &str, secret: &str) -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(auth::PUBLIC_KEY, Secret::new(public.to_owned()));
    creds.insert(auth::SECRET_KEY, Secret::new(secret.to_owned()));
    creds
}

fn creds() -> Secrets {
    creds_for(PUBLIC, SECRET)
}

fn selection(account: &str) -> AccountSelection<'_> {
    match account {
        "primary" => AccountSelection::Primary,
        id => AccountSelection::Subaccount(id),
    }
}

/// The reader for `account` with the configured account's credentials.
fn reader(account: &str) -> ReadAuth {
    ReadAuth::new(selection(account), creds()).unwrap()
}

/// The context of the `n`th call: its wall time and two nonces.
fn ctx(n: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_000_000_000_000_000 + n as i64 * 1_000_000_000),
        mono: MonoNs(n),
        nonces: NonceBlock::new(vec![n, u64::MAX - n]),
    }
}

fn caps() -> VenueCaps {
    Qfex.caps(&preprod("primary")).unwrap()
}

/// `f`, given a scope that decodes as the probe's caps declare, in our namespace.
fn scoped<R>(f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    dispatch(&caps(), NS, f)
}

fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

fn answer(status: u16, body: &str) -> HttpResponse<'_> {
    HttpResponse {
        status,
        headers: &[],
        body: body.as_bytes(),
    }
}

fn ok(body: &str) -> HttpResponse<'_> {
    answer(200, body)
}

/// The headers as (name, value, redact).
fn shown(headers: &[Header]) -> Vec<(&str, &str, bool)> {
    headers
        .iter()
        .map(|h| (h.name, h.value.as_str(), h.redact))
        .collect()
}

/// What the journal reads back: every span as `2`s.
fn blanked(bytes: &[u8], spans: &[core::ops::Range<u32>]) -> String {
    let mut bytes = bytes.to_vec();
    for span in spans {
        bytes[span.start as usize..span.end as usize].fill(b'2');
    }
    String::from_utf8(bytes).unwrap()
}

// ----- Nothing is built outside the probe's guard -----

#[test]
fn a_config_without_the_probe_mode_or_refused_by_its_guard_builds_no_request() {
    let mut md_only = VenueConfig::new();
    md_only.insert(KEY_MDS_URL, PREPROD_MDS);
    md_only.insert(KEY_REST_URL, PREPROD_REST);
    md_only.insert(KEY_REST_TIMEOUT, "5000ms");
    let cases = [
        (md_only, "market data only"),
        (
            with(preprod(SUB), KEY_EXEC_MODE, "live"),
            "another exec mode",
        ),
        (
            with(preprod(SUB), KEY_REST_URL, "https://api.qfex.com"),
            "a qfex.com host",
        ),
        (
            with(preprod(SUB), auth::TRADE_URL, "wss://trade.qfex.com"),
            "a qfex.com trade host",
        ),
        (
            with(preprod(SUB), KEY_REST_URL, "http://127.0.0.1:9002"),
            "a mixed set",
        ),
        (
            with(preprod(SUB), auth::ACCOUNT_KEY, "09"),
            "an account key decision 0122 refuses",
        ),
    ];
    let reader = reader(SUB);
    for (cfg, what) in cases {
        let want = probe::connections(&cfg).expect_err(what);
        let build = BuildError::Config(want);
        assert_eq!(
            account_reads::subaccounts(&cfg, &reader, &ctx(1)),
            Err(build),
            "{what}"
        );
        assert_eq!(
            account_reads::positions(&cfg, &reader, &ctx(1)),
            Err(build),
            "{what}"
        );
        assert_eq!(
            account_reads::authentication(&cfg, &reader, &ctx(1)),
            Err(build),
            "{what}"
        );
        let ask = PageAsk::new(MAX_PAGE, 0).unwrap();
        assert_eq!(account_reads::user_orders(&cfg, ask), Err(build), "{what}");
        assert_eq!(account_reads::subscriptions(&cfg), Err(build), "{what}");
        assert_eq!(account_reads::trade_url(&cfg, &reader), Err(want), "{what}");
        // Named, never shown: the refusal names the setting, not its value.
        let text = format!("{build} {build:?}");
        assert!(!text.contains("://") && !text.contains("09"), "{text}");
    }
    // The market-data-only configuration refuses for want of the mode itself.
    let mut md_only = VenueConfig::new();
    md_only.insert(KEY_MDS_URL, PREPROD_MDS);
    assert_eq!(
        account_reads::user_orders(&md_only, PageAsk::new(1, 0).unwrap()),
        Err(BuildError::Config(ConfigError::Missing(KEY_EXEC_MODE)))
    );
}

// ----- The REST requests -----

/// The four signed headers `ctx` gives the configured credentials, each `redact: true`.
fn signed(ctx: &EncodeCtx) -> (String, String, String) {
    let nonce = [ctx.nonces.as_slice()[0], ctx.nonces.as_slice()[1]];
    let signed = HmacKey::new(Secret::new(SECRET.to_owned()))
        .unwrap()
        .sign(nonce, ctx.wall)
        .unwrap();
    (
        signed.signature().to_owned(),
        signed.nonce().to_owned(),
        signed.unix_ts().to_string(),
    )
}

#[test]
fn the_listing_carries_the_four_signed_headers_each_redacted_and_no_account_header() {
    let (signature, nonce, unix_ts) = signed(&ctx(3));
    let want = vec![
        ("x-qfex-public-key", PUBLIC, true),
        ("x-qfex-hmac-signature", signature.as_str(), true),
        ("x-qfex-nonce", nonce.as_str(), true),
        ("x-qfex-timestamp", unix_ts.as_str(), true),
    ];
    for (cfg, origin) in [
        (preprod(SUB), PREPROD_REST),
        (loopback("primary"), "http://127.0.0.1:9002"),
        (
            with(preprod(SUB), KEY_REST_URL, "https://api.qfex.io/"),
            PREPROD_REST,
        ),
    ] {
        // Whichever account the reader names, the listing selects none.
        for account in ["primary", SUB, SUB2] {
            let read = account_reads::subaccounts(&cfg, &reader(account), &ctx(3)).unwrap();
            assert_eq!(read.req.method, fbc_core::HttpMethod::Get);
            assert_eq!(
                read.req.url.as_str(),
                format!("{origin}/user/subaccounts"),
                "{account}"
            );
            assert!(read.req.url.redactions().is_empty());
            assert!(read.req.body.bytes().is_empty());
            assert_eq!(shown(&read.req.headers), want, "{account}");
            assert_eq!(read.charge, RateCharge::one(OpKind::Rest, None));
            assert_eq!(read.class, TrafficClass::Normal);
        }
    }
    assert_eq!(account_reads::SUBACCOUNTS_PATH, "/user/subaccounts");
}

#[test]
fn the_positions_read_selects_a_subaccount_by_its_redacted_header_and_the_primary_by_none() {
    let (signature, nonce, unix_ts) = signed(&ctx(4));
    for (account, selected) in [("primary", None), (SUB, Some(SUB)), (SUB2, Some(SUB2))] {
        let read = account_reads::positions(&preprod(SUB), &reader(account), &ctx(4)).unwrap();
        assert_eq!(read.req.method, fbc_core::HttpMethod::Get);
        assert_eq!(
            read.req.url.as_str(),
            format!("{PREPROD_REST}/user/positions")
        );
        let mut want = vec![
            ("x-qfex-public-key", PUBLIC, true),
            ("x-qfex-hmac-signature", signature.as_str(), true),
            ("x-qfex-nonce", nonce.as_str(), true),
            ("x-qfex-timestamp", unix_ts.as_str(), true),
        ];
        if let Some(id) = selected {
            want.push(("x-qfex-requested-account-id", id, true));
        }
        assert_eq!(shown(&read.req.headers), want, "{account}");
        assert_eq!(read.charge, RateCharge::one(OpKind::Rest, None));
        assert_eq!(read.class, TrafficClass::Normal);
        // Signed as the configured account's own read is (FBC-mfey), which redacts the key,
        // the signature and the selection alike.
        if account == SUB {
            let own = TradeAuth::new(&preprod(SUB), creds())
                .unwrap()
                .rest_headers(&ctx(4))
                .unwrap();
            let values = |h: &[Header]| h.iter().map(|h| h.value.clone()).collect::<Vec<_>>();
            assert_eq!(values(&own), values(&read.req.headers));
        }
    }
    assert_eq!(account_reads::POSITIONS_PATH, "/user/positions");
}

#[test]
fn a_read_with_the_wrong_nonces_or_before_the_epoch_is_refused_with_nothing_built() {
    let cfg = preprod(SUB);
    let reader = reader(SUB);
    for nonces in [vec![], vec![1], vec![1, 2, 3]] {
        let ctx = EncodeCtx {
            nonces: NonceBlock::new(nonces),
            ..ctx(1)
        };
        let rest = BuildError::Sign(SignError::Unsignable("a QFEX REST read takes two nonces"));
        assert_eq!(account_reads::subaccounts(&cfg, &reader, &ctx), Err(rest));
        assert_eq!(account_reads::positions(&cfg, &reader, &ctx), Err(rest));
        assert_eq!(
            account_reads::authentication(&cfg, &reader, &ctx),
            Err(BuildError::Sign(SignError::Unsignable(
                "QFEX authentication takes two nonces"
            )))
        );
    }
    let before = EncodeCtx {
        wall: WallNs(-1),
        ..ctx(1)
    };
    let refused = account_reads::subaccounts(&cfg, &reader, &before).unwrap_err();
    assert!(matches!(refused, BuildError::Sign(_)));
    assert!(account_reads::positions(&cfg, &reader, &before).is_err());
    assert!(account_reads::authentication(&cfg, &reader, &before).is_err());
    assert!(format!("{refused}").starts_with("QFEX account read not signed: "));
}

// ----- The trade socket -----

#[test]
fn the_authentication_names_a_subaccounts_account_id_and_the_primarys_none() {
    let cfg = preprod(SUB);
    for account in ["primary", SUB, SUB2] {
        let write = account_reads::authentication(&cfg, &reader(account), &ctx(5)).unwrap();
        assert_eq!(write.charge, RateCharge::one(OpKind::Control, None));
        assert_eq!(write.class, TrafficClass::Safety);
        let frame: Value = serde_json::from_slice(write.frame.bytes()).unwrap();
        assert_eq!(frame["type"], "auth");
        let params = frame["params"].as_object().unwrap();
        match account {
            "primary" => assert!(!params.contains_key("account_id")),
            id => assert_eq!(params["account_id"], id),
        }
        // Each credential and the account id is a redaction span of its own.
        let spans = write.frame.redactions();
        let text = std::str::from_utf8(write.frame.bytes()).unwrap();
        let spanned: Vec<&str> = spans
            .iter()
            .map(|s| &text[s.start as usize..s.end as usize])
            .collect();
        assert_eq!(spanned.len(), if account == "primary" { 2 } else { 3 });
        assert_eq!(spanned[0], PUBLIC);
        if account != "primary" {
            assert_eq!(spanned[2], account);
        }
    }
    // Built exactly as the order-entry codec builds the configured account's own (FBC-77aa).
    for account in ["primary", SUB] {
        let own = TradeAuth::new(&preprod(account), creds())
            .unwrap()
            .frame(&ctx(5))
            .unwrap();
        let read = account_reads::authentication(&preprod(account), &reader(account), &ctx(5))
            .unwrap()
            .frame;
        assert_eq!(read, own, "{account}");
    }
}

#[test]
fn the_trade_socket_opens_on_the_guarded_origin_with_the_readers_own_key() {
    // The configured account's credentials: the URL an order-entry session opens.
    let cfg = preprod(SUB);
    let url = account_reads::trade_url(&cfg, &reader(SUB)).unwrap();
    assert_eq!(url, probe::connections(&cfg).unwrap().trade);
    // Another account's credentials: its own key, the URL's one redaction span.
    let other = ReadAuth::new(
        AccountSelection::Subaccount(SUB2),
        creds_for(OTHER_PUBLIC, "s2"),
    )
    .unwrap();
    let url = account_reads::trade_url(&loopback("primary"), &other).unwrap();
    let text = format!("ws://127.0.0.1:9001/?api_key={OTHER_PUBLIC}");
    assert_eq!(url.as_str(), text);
    let at = text.find(OTHER_PUBLIC).unwrap() as u32;
    assert_eq!(url.redactions(), vec![at..at + OTHER_PUBLIC.len() as u32]);
    assert!(!format!("{url:?}").contains(OTHER_PUBLIC));
}

#[test]
fn get_user_orders_encodes_exactly_within_its_page_size_and_is_refused_outside_it() {
    let cfg = preprod(SUB);
    for (limit, offset) in [(1, 0), (1, 1000), (1000, 0), (1000, 1000)] {
        let ask = PageAsk::new(limit, offset).unwrap();
        assert_eq!((ask.limit(), ask.offset()), (limit, offset));
        let write = account_reads::user_orders(&cfg, ask).unwrap();
        let want = format!(
            r#"{{"type":"get_user_orders","params":{{"limit":{limit},"offset":{offset}}}}}"#
        );
        assert_eq!(write.frame.bytes(), want.as_bytes());
        assert!(write.frame.redactions().is_empty());
        assert_eq!(
            write.charge,
            RateCharge {
                op: OpKind::Query,
                inst: None,
                weight: PAGE_WEIGHT,
            }
        );
        assert_eq!(PAGE_WEIGHT.get(), 50);
        assert_eq!(write.class, TrafficClass::Normal);
    }
    for limit in [0, 1001, u16::MAX] {
        assert_eq!(PageAsk::new(limit, 0), Err(BuildError::PageSize), "{limit}");
    }
    assert_eq!(MAX_PAGE, 1000);
    assert_eq!(
        BuildError::PageSize.to_string(),
        "a get_user_orders page size outside 1 to 1000"
    );
    let missing = ConfigError::Missing(KEY_EXEC_MODE);
    assert_eq!(
        BuildError::from(missing).to_string(),
        format!("QFEX probe configuration refused: {missing}")
    );
    let sign = BuildError::from(SignError::Backend("x"));
    assert_eq!(sign, BuildError::Sign(SignError::Backend("x")));
}

// ----- The reader -----

#[test]
fn the_reader_refuses_a_noncanonical_account_and_missing_or_unsafe_credentials() {
    let invalid = |key, reason| ConfigError::Invalid { key, reason };
    for id in [
        "5A5A5A5A-0000-4000-8000-00000000C0DE",
        "00000000-0000-0000-0000-000000000000",
        "primary",
        "",
        " 5a5a5a5a-0000-4000-8000-00000000c0de",
    ] {
        assert_eq!(
            ReadAuth::new(AccountSelection::Subaccount(id), creds()).err(),
            Some(invalid(
                auth::ACCOUNT_ID,
                "expected primary or a lowercase subaccount UUID"
            )),
            "{id:?}"
        );
    }
    let mut no_public = Secrets::new();
    no_public.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    assert_eq!(
        ReadAuth::new(AccountSelection::Primary, no_public).err(),
        Some(ConfigError::Missing(auth::PUBLIC_KEY))
    );
    for unsafe_key in ["", "has space", "qfex_pub_&x", &"k".repeat(257)] {
        assert_eq!(
            ReadAuth::new(AccountSelection::Primary, creds_for(unsafe_key, SECRET)).err(),
            Some(invalid(
                auth::PUBLIC_KEY,
                "expected 1 to 256 ASCII letters, digits, _ or -"
            ))
        );
    }
    let mut no_secret = Secrets::new();
    no_secret.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    assert_eq!(
        ReadAuth::new(AccountSelection::Primary, no_secret).err(),
        Some(ConfigError::Missing(auth::SECRET_KEY))
    );
    assert_eq!(
        ReadAuth::new(AccountSelection::Primary, creds_for(PUBLIC, "")).err(),
        Some(invalid(auth::SECRET_KEY, "an empty secret"))
    );
    // A handle shows whether it reads a subaccount, and nothing else.
    for (account, sub) in [("primary", false), (SUB, true)] {
        let reader = reader(account);
        assert_eq!(reader.is_subaccount(), sub);
        let debug = format!("{reader:?}");
        assert_eq!(debug, format!("ReadAuth {{ subaccount: {sub}, .. }}"));
    }
    assert_eq!(ReadAuth::NONCES, 2);
}

// ----- The listing -----

#[test]
fn the_listing_decodes_two_ids_and_null_or_empty_to_none() {
    let listed = decode_listing(&fixture("subaccounts.json"));
    assert_eq!(listed.len(), 2);
    assert!(!listed.is_empty());
    assert_eq!(
        listed.accounts().collect::<Vec<_>>(),
        [
            AccountSelection::Primary,
            AccountSelection::Subaccount(SUB),
            AccountSelection::Subaccount(SUB2)
        ]
    );
    for body in [
        r#"{"account_ids":null}"#,
        r#"{"account_ids":[]}"#,
        r#"{"$schema":"https://schemas.example/x.json","account_ids":null}"#,
    ] {
        let none = decode_listing(body);
        assert!(none.is_empty(), "{body}");
        assert_eq!(none.len(), 0, "{body}");
        assert_eq!(
            none.accounts().collect::<Vec<_>>(),
            [AccountSelection::Primary]
        );
    }
}

fn decode_listing(body: &str) -> Subaccounts {
    account_reads::decode_subaccounts(&ok(body)).unwrap()
}

#[test]
fn the_listing_refuses_a_status_malformed_json_a_missing_bad_or_repeated_id_and_more() {
    let fixture = fixture("subaccounts.json");
    // A 401, named by its status only: its body is never read or shown.
    let refused = account_reads::decode_subaccounts(&answer(401, &fixture)).unwrap_err();
    assert_eq!(refused, AnswerError::Status(401));
    assert_eq!(refused.to_string(), "QFEX answered HTTP 401");
    for status in [199, 204, 299] {
        let body = if status == 204 {
            r#"{"account_ids":[]}"#
        } else {
            ""
        };
        let got = account_reads::decode_subaccounts(&answer(status, body));
        assert_eq!(got.is_ok(), status == 204, "{status}");
    }
    let cases: [(&str, AnswerError); 14] = [
        ("{", AnswerError::Malformed),
        ("", AnswerError::Malformed),
        ("[]", AnswerError::Malformed),
        ("null", AnswerError::Malformed),
        (
            r#"{"account_ids":[],"account_ids":[]}"#,
            AnswerError::Malformed,
        ),
        ("{}", AnswerError::Missing("account_ids")),
        (
            r#"{"$schema":"https://schemas.example/x.json"}"#,
            AnswerError::Missing("account_ids"),
        ),
        (
            &format!(r#"{{"account_ids":["{}"]}}"#, SUB.to_uppercase()),
            AnswerError::Invalid("account_ids"),
        ),
        (
            &format!(r#"{{"account_ids":["{SUB}","{SUB2}","{SUB}"]}}"#),
            AnswerError::Repeated("account id"),
        ),
        (
            &format!(r#"{{"account_ids":["{SUB}"],"next_cursor":"x"}}"#),
            AnswerError::Undocumented,
        ),
        (
            r#"{"account_ids":"x"}"#,
            AnswerError::Invalid("account_ids"),
        ),
        (
            r#"{"account_ids":[1]}"#,
            AnswerError::Invalid("account_ids"),
        ),
        (
            r#"{"account_ids":["00000000-0000-0000-0000-000000000000"]}"#,
            AnswerError::Invalid("account_ids"),
        ),
        (
            r#"{"$schema":1,"account_ids":[]}"#,
            AnswerError::Invalid("$schema"),
        ),
    ];
    for (body, want) in cases.iter() {
        assert_eq!(
            account_reads::decode_subaccounts(&ok(body)),
            Err(*want),
            "{body}"
        );
    }
    // Not UTF-8.
    let bytes = [0xffu8, 0xfe];
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: &bytes,
    };
    assert_eq!(
        account_reads::decode_subaccounts(&resp),
        Err(AnswerError::Malformed)
    );
    // Each refusal names a member or a status, never a value.
    for err in [
        AnswerError::Malformed,
        AnswerError::Missing("account_ids"),
        AnswerError::Undocumented,
        AnswerError::Invalid("account_ids"),
        AnswerError::Repeated("account id"),
    ] {
        let text = format!("{err} {err:?}");
        assert!(!text.contains(SUB) && !text.contains("5a5a"), "{text}");
        assert!(text.starts_with("QFEX's answer"), "{text}");
    }
}

#[test]
fn the_membership_check_passes_the_primary_and_a_listed_id_and_fails_an_unlisted_one() {
    let listed = decode_listing(&fixture("subaccounts.json"));
    // The traded account as decision 0122 selects it.
    for (cfg, admitted) in [
        (preprod("primary"), true),
        (preprod(SUB), true),
        (preprod(SUB2), true),
        (preprod(UNLISTED), false),
    ] {
        let traded = auth::account_selection(&cfg).unwrap();
        assert_eq!(listed.admits(traded), admitted, "{traded:?}");
    }
    let none = decode_listing(r#"{"account_ids":null}"#);
    assert!(none.admits(AccountSelection::Primary));
    assert!(!none.admits(AccountSelection::Subaccount(SUB)));
}

// ----- The positions -----

fn decode_positions(body: &str) -> Result<Positions, AnswerError> {
    scoped(|scope| account_reads::decode_positions(&ok(body), scope))
}

#[test]
fn positions_of_zero_in_every_spelling_and_null_decode_flat() {
    let flat = decode_positions(&fixture("positions_flat.json")).unwrap();
    assert!(flat.is_flat());
    let rows: Vec<(&str, Decimal, u64)> = flat
        .rows()
        .iter()
        .map(|r| (r.symbol.as_wire(), r.position, r.open_orders))
        .collect();
    assert_eq!(
        rows,
        [
            ("AAPL-USD", Decimal::ZERO, 0),
            ("NVDA-USD", Decimal::ZERO, 1),
            ("MSFT-USD", Decimal::ZERO, 0),
        ]
    );
    for body in [
        r#"{"balance":{},"positions":null}"#,
        r#"{"balance":{},"positions":[]}"#,
        r#"{"$schema":"https://schemas.example/p.json","positions":null,"balance":{"deposit":1}}"#,
    ] {
        let none = decode_positions(body).unwrap();
        assert!(none.is_flat() && none.rows().is_empty(), "{body}");
    }
}

#[test]
fn a_tiny_or_twenty_digit_position_decodes_exactly_and_not_flat() {
    let open = decode_positions(&fixture("positions.json")).unwrap();
    assert!(!open.is_flat());
    let rows = open.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].symbol.as_wire(), "AAPL-USD");
    assert_eq!(
        rows[0].position,
        Decimal::from_str_exact("0.000000000001").unwrap()
    );
    assert_eq!(rows[0].open_orders, 2);
    assert_eq!(rows[1].symbol.as_wire(), "NVDA-USD");
    assert_eq!(
        rows[1].position,
        Decimal::from_str_exact("-1234567890.1234567890").unwrap()
    );
    assert_eq!(rows[1].position.to_string(), "-1234567890.1234567890");
    assert_eq!(rows[1].open_orders, 0);
    // One of them alone is enough.
    for position in [
        "1e-12",
        "-1E-12",
        "12345678901234567890",
        "0.00000000000000000001",
    ] {
        let body = format!(
            r#"{{"balance":{{}},"positions":[{{"symbol":"X-USD","position":{position},"open_orders":0}}]}}"#
        );
        let one = decode_positions(&body).unwrap();
        assert!(!one.is_flat(), "{position}");
        assert!(!one.rows()[0].position.is_zero(), "{position}");
    }
}

#[test]
fn positions_refuse_a_missing_or_string_position_a_repeated_symbol_and_more() {
    let row = |members: &str| format!(r#"{{"balance":{{}},"positions":[{{{members}}}]}}"#);
    let cases: Vec<(String, AnswerError)> = vec![
        (
            row(r#""symbol":"A-USD","open_orders":0"#),
            AnswerError::Missing("position"),
        ),
        (
            row(r#""symbol":"A-USD","position":"0","open_orders":0"#),
            AnswerError::Invalid("position"),
        ),
        (
            row(r#""symbol":"A-USD","position":null,"open_orders":0"#),
            AnswerError::Invalid("position"),
        ),
        (
            row(r#""symbol":"A-USD","position":1e400,"open_orders":0"#),
            AnswerError::Malformed,
        ),
        (
            row(r#""symbol":"A-USD","position":1e-65,"open_orders":0"#),
            AnswerError::Invalid("position"),
        ),
        (
            r#"{"balance":{},"positions":[{"symbol":"A-USD","position":0,"open_orders":0},{"symbol":"A-USD","position":0,"open_orders":0}]}"#.to_owned(),
            AnswerError::Repeated("symbol"),
        ),
        (r#"{"positions":null}"#.to_owned(), AnswerError::Missing("balance")),
        (
            r#"{"balance":null,"positions":null}"#.to_owned(),
            AnswerError::Invalid("balance"),
        ),
        (
            r#"{"balance":[],"positions":null}"#.to_owned(),
            AnswerError::Invalid("balance"),
        ),
        (
            r#"{"balance":{},"positions":null,"count":0}"#.to_owned(),
            AnswerError::Undocumented,
        ),
        (r#"{"balance":{}}"#.to_owned(), AnswerError::Missing("positions")),
        (
            r#"{"balance":{},"positions":{}}"#.to_owned(),
            AnswerError::Invalid("positions"),
        ),
        (
            r#"{"balance":{},"positions":[1]}"#.to_owned(),
            AnswerError::Invalid("positions"),
        ),
        (
            row(r#""position":0,"open_orders":0"#),
            AnswerError::Missing("symbol"),
        ),
        (
            row(r#""symbol":1,"position":0,"open_orders":0"#),
            AnswerError::Invalid("symbol"),
        ),
        (
            row(r#""symbol":"","position":0,"open_orders":0"#),
            AnswerError::Invalid("symbol"),
        ),
        (
            row(r#""symbol":"A-USD","position":0"#),
            AnswerError::Missing("open_orders"),
        ),
        (
            row(r#""symbol":"A-USD","position":0,"open_orders":-1"#),
            AnswerError::Invalid("open_orders"),
        ),
        (
            row(r#""symbol":"A-USD","position":0,"open_orders":1.5"#),
            AnswerError::Invalid("open_orders"),
        ),
        (
            row(r#""symbol":"A-USD","position":0,"open_orders":"1""#),
            AnswerError::Invalid("open_orders"),
        ),
        (
            r#"{"balance":{},"balance":{},"positions":null}"#.to_owned(),
            AnswerError::Malformed,
        ),
    ];
    for (body, want) in &cases {
        assert_eq!(decode_positions(body), Err(*want), "{body}");
    }
    // A status outside 2xx is refused by its status, whatever the body holds.
    let flat = fixture("positions_flat.json");
    let refused = scoped(|scope| account_reads::decode_positions(&answer(500, &flat), scope));
    assert_eq!(refused, Err(AnswerError::Status(500)));
}

// ----- The get_user_orders page -----

fn page_of(body: &str, limit: u16) -> Result<OrdersPage, NotAPage> {
    let reader = reader(SUB);
    let ask = PageAsk::new(limit, 0).unwrap();
    scoped(|scope| account_reads::orders_page(&reader, RawFrame::Text(body), ask, scope))
}

fn page(orders: &[String], twaps: usize) -> String {
    let twaps =
        vec![r#"{"twap_id":"44444444-4444-4444-8444-444444444444","status":"ACTIVE"}"#; twaps];
    format!(
        r#"{{"all_orders_response":{{"orders":[{}],"twaps":[{}]}}}}"#,
        orders.join(","),
        twaps.join(",")
    )
}

/// One order row as `get_user_orders`' example shows it, with these members replaced.
fn row(order_id: &str, quantity: &str, remaining: &str, cid: &str) -> String {
    format!(
        r#"{{"order_id":"{order_id}","symbol":"AAPL-USD","status":"ACK","quantity":{quantity},"price":199.5,"take_profit":0.0,"stop_loss":0.0,"side":"BUY","type":"LIMIT","time_in_force":"GTC","user_id":"{USER}","client_order_id":"{cid}","quantity_remaining":{remaining},"update_time":1760000001.5,"trade_id":null}}"#
    )
}

const ORDER: &str = "22222222-2222-4222-8222-222222222222";

#[test]
fn an_empty_page_decodes_empty_and_a_page_of_the_limit_is_full() {
    let empty = page_of(&page(&[], 0), 1000).unwrap();
    assert!(empty.is_empty() && !empty.is_full());
    assert!(empty.orders().is_empty() && empty.twaps() == 0);
    assert_eq!(empty.ask(), PageAsk::new(1000, 0).unwrap());
    // One TWAP and no order is not empty, and at a page size of one it is full.
    let twap = page_of(&page(&[], 1), 1000).unwrap();
    assert!(!twap.is_empty() && !twap.is_full());
    assert_eq!(twap.twaps(), 1);
    assert!(page_of(&page(&[], 1), 1).unwrap().is_full());
    // The fixture's two orders and one TWAP fill a page of three, not one of four.
    let all = fixture("all_orders_response.json");
    assert!(page_of(&all, 3).unwrap().is_full());
    let four = page_of(&all, 4).unwrap();
    assert!(!four.is_full() && !four.is_empty());
    // More than the page size answers no page of it.
    assert_eq!(
        page_of(&all, 2),
        Err(NotAPage::Other("more orders and TWAPs than the page size"))
    );
}

#[test]
fn rows_decode_exactly_and_a_repeated_order_id_is_kept() {
    let fixed = page_of(&fixture("all_orders_response.json"), 1000).unwrap();
    let orders = fixed.orders();
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].order.as_str(), ORDER);
    assert_eq!(orders[0].symbol.as_wire(), "AAPL-USD");
    assert_eq!(orders[0].side, Side::Sell);
    assert_eq!(
        orders[0].quantity,
        Decimal::from_str_exact("2.708").unwrap()
    );
    assert_eq!(
        orders[0].remaining,
        Decimal::from_str_exact("2.708").unwrap()
    );
    assert_eq!(orders[0].client, CidMatch::Unparseable);
    assert_eq!(
        orders[1].order.as_str(),
        "33333333-3333-4333-8333-333333333333"
    );
    assert_eq!(orders[1].side, Side::Buy);
    assert_eq!(orders[1].quantity, Decimal::from(3));
    assert_eq!(orders[1].client, CidMatch::Unparseable);
    assert_eq!(fixed.twaps(), 1);

    // A repeated order id is kept, for the caller to deduplicate; a client id of ours is ours.
    let ours = minted();
    let wire = encode_cid(&CLIENT_ID_FORMAT, ours).unwrap();
    let rows = [
        row(ORDER, "0.1", "0.05", wire.as_str()),
        row(ORDER, "12345678901234567.891", "0", ""),
        row(&ORDER.to_uppercase(), "1e-3", "1E-3", "not-ours"),
    ];
    let repeated = page_of(&page(&rows, 0), 3).unwrap();
    let orders = repeated.orders();
    assert_eq!(orders.len(), 3);
    assert_eq!(orders[0].order, orders[1].order);
    assert_eq!(orders[0].client, CidMatch::Ours(ours));
    assert_eq!(
        orders[0].remaining,
        Decimal::from_str_exact("0.05").unwrap()
    );
    assert_eq!(
        orders[1].quantity,
        Decimal::from_str_exact("12345678901234567.891").unwrap()
    );
    assert_eq!(orders[1].remaining, Decimal::ZERO);
    assert_eq!(
        orders[2].quantity,
        Decimal::from_str_exact("0.001").unwrap()
    );
    assert_eq!(orders[2].remaining, orders[2].quantity);
    assert!(repeated.is_full());
}

/// One client id minted under a held namespace lease.
fn minted() -> ClientOrderId {
    let dir = std::env::temp_dir().join(format!("fbc-qfex-reads-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), NS).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_760_000_000_000_000_000));
    let id = mint.mint().unwrap();
    drop(mint);
    let _ = fs::remove_dir_all(&dir);
    id
}

#[test]
fn a_row_over_its_quantity_or_with_an_order_id_the_rule_refuses_is_no_page() {
    let refused = |rows: &[String], what: &'static str| {
        assert_eq!(
            page_of(&page(rows, 0), 1000),
            Err(NotAPage::Other(what)),
            "{rows:?}"
        );
    };
    refused(&[row(ORDER, "1", "1.000000001", "")], "quantity_remaining");
    refused(&[row(ORDER, "1", "-0.5", "")], "quantity_remaining");
    for id in [
        "not-a-uuid",
        "",
        "22222222-2222-4222-8222-22222222222",
        "{22222222-2222-4222-8222-2222222222}",
    ] {
        refused(&[row(id, "1", "1", "")], "order_id");
    }
    refused(&[row(ORDER, "0", "0", "")], "quantity");
    refused(&[row(ORDER, "-1", "0", "")], "quantity");
    refused(&[row(ORDER, "\"1\"", "0", "")], "quantity");
    refused(&[row(ORDER, "1", "\"1\"", "")], "quantity_remaining");
    let base = row(ORDER, "1", "1", "");
    let without = |member: &str| {
        let mut row: Value = serde_json::from_str(&base).unwrap();
        row.as_object_mut().unwrap().remove(member);
        row.to_string()
    };
    for member in [
        "order_id",
        "symbol",
        "side",
        "quantity",
        "quantity_remaining",
        "client_order_id",
    ] {
        refused(&[without(member)], member);
    }
    refused(&[base.replace(r#""BUY""#, r#""HOLD""#)], "side");
    refused(&[base.replace(r#""AAPL-USD""#, r#""""#)], "symbol");
    refused(
        &[base.replace(r#""client_order_id":"""#, r#""client_order_id":null"#)],
        "client_order_id",
    );
    refused(&["1".to_owned()], "an order row");
}

#[test]
fn any_frame_but_one_page_of_orders_and_twaps_is_no_page() {
    let other = |frame: &str, what: &'static str| {
        assert_eq!(page_of(frame, 1000), Err(NotAPage::Other(what)), "{frame}");
    };
    other(r#"{"all_orders_response":{"orders":[]}}"#, "twaps");
    other(r#"{"all_orders_response":{"twaps":[]}}"#, "orders");
    other(
        r#"{"all_orders_response":{"orders":null,"twaps":[]}}"#,
        "orders",
    );
    other(
        r#"{"all_orders_response":{"orders":[],"twaps":[1]}}"#,
        "twaps",
    );
    other(
        r#"{"all_orders_response":{"orders":[],"twaps":[],"stop_orders":[]}}"#,
        "an undocumented page member",
    );
    other(
        r#"{"all_orders_response":[]}"#,
        "not one all_orders_response",
    );
    other(
        r#"{"all_orders_response":{"orders":[],"twaps":[]},"count":0}"#,
        "not one all_orders_response",
    );
    other(r#"{"user_trades":[]}"#, "not one all_orders_response");
    other(r#"{"authenticated":true}"#, "not one all_orders_response");
    other("[]", "not a JSON object");
    other("nope", "not a JSON object");
    other(r#"{"a":1,"a":1}"#, "a repeated member");
    other(
        r#"{"ack":{"incoming_message":{"type":"get_user_orders","params":{}}}}"#,
        "an ack",
    );
    other(
        r#"{"type":"Err","err":{"error_code":"ServerError"},"error_code":"ServerError"}"#,
        "two envelopes",
    );
    // A binary frame.
    let reader = reader(SUB);
    let ask = PageAsk::new(1, 0).unwrap();
    let binary =
        scoped(|scope| account_reads::orders_page(&reader, RawFrame::Binary(b"{}"), ask, scope));
    assert_eq!(binary, Err(NotAPage::Other("not a text frame")));
    // The errors name what was wrong, never a value.
    assert_eq!(
        NotAPage::Other("an ack").to_string(),
        "not a get_user_orders page: an ack"
    );
}

/// A flat err, as QFEX's rate page writes one.
fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":{message},"incoming_message":{echo}}}"#
    )
}

/// A nested err, as QFEX's errors page and AsyncAPI write one.
fn nested(code: &str, message: &str, echo: &str) -> String {
    format!(r#"{{"err":{{"error_code":"{code}","message":{message},"incoming_message":{echo}}}}}"#)
}

const PAGE_ECHO: &str = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;

#[test]
fn an_err_is_no_page_and_a_rate_limited_one_is_the_codecs_rate_hold() {
    let hint_s = r#""Rate limit exceeded, retry after 7 seconds""#;
    let hint_ms = r#""Too many requests. Please retry after 200ms.""#;
    let query = Some(RateCharge::one(OpKind::Query, None));
    let cases = [
        (
            flat("RateLimited", hint_s, PAGE_ECHO),
            NotAPage::RateLimited(RateHold {
                charge: query,
                retry_after: Some(Duration::from_secs(7)),
            }),
        ),
        (
            nested("RateLimited", hint_ms, PAGE_ECHO),
            NotAPage::RateLimited(RateHold {
                charge: query,
                retry_after: Some(Duration::from_millis(200)),
            }),
        ),
        (
            nested("RateLimited", "null", "{}"),
            NotAPage::RateLimited(RateHold {
                charge: None,
                retry_after: None,
            }),
        ),
        (
            flat("RateLimited", r#""slow down""#, r#"{"type":"mystery"}"#),
            NotAPage::RateLimited(RateHold {
                charge: None,
                retry_after: None,
            }),
        ),
        (
            nested("InvalidParameter", r#""bad limit""#, PAGE_ECHO),
            NotAPage::Err(Some(ErrCode::InvalidParameter)),
        ),
        (
            flat("PermissionDenied", "null", PAGE_ECHO),
            NotAPage::Err(Some(ErrCode::PermissionDenied)),
        ),
        (
            nested("SomethingNew", "null", PAGE_ECHO),
            NotAPage::Err(None),
        ),
        (
            r#"{"err":{"error_code":"ServerError"}}"#.to_owned(),
            NotAPage::Err(Some(ErrCode::ServerError)),
        ),
    ];
    for (frame, want) in &cases {
        assert_eq!(page_of(frame, 1000), Err(*want), "{frame}");
        // A RateLimited err is the hold the order-entry codec asks for on the same bytes.
        if let NotAPage::RateLimited(hold) = want {
            let effects = codec_effects(frame);
            assert_eq!(
                effects,
                [Effect::RateLimited {
                    stream: TRADE,
                    charge: hold.charge,
                    retry_after: hold.retry_after,
                }],
                "{frame}"
            );
        }
    }
    assert_eq!(
        NotAPage::Err(Some(ErrCode::InvalidOrder)).to_string(),
        "QFEX answered an err: InvalidOrder"
    );
    assert_eq!(
        NotAPage::Err(None).to_string(),
        "QFEX answered an undocumented err"
    );
    assert_eq!(
        cases[0].1.to_string(),
        "QFEX refused the page read: RateLimited"
    );
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

/// A probe codec for the traded subaccount, opened.
fn opened_codec() -> QfexExec {
    let cfg = preprod(SUB);
    let mut codec = QfexExec::probe(
        TradeAuth::new(&cfg, creds()).unwrap(),
        probe::mode(&cfg).unwrap().unwrap(),
    );
    codec.on_open(TRADE, &ctx(1), &mut Effects::new());
    codec
}

/// The effects the codec asks for when `frame` is the first frame after its opening.
fn codec_frame(frame: RawFrame<'_>) -> Vec<Effect> {
    let mut codec = opened_codec();
    let mut fx = Effects::new();
    let mut sink = Sink::default();
    let _ =
        scoped(|scope| codec.on_frame(TRADE, frame, scope, &SpecTable::new(), &mut sink, &mut fx));
    fx.take()
}

fn codec_effects(frame: &str) -> Vec<Effect> {
    codec_frame(RawFrame::Text(frame))
}

// ----- The liveness hold (decision 0146) -----

/// The authenticate page's success reply.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;

#[test]
fn the_holds_subscriptions_are_the_codecs_own_three_built_only_under_the_guard() {
    let sent: Vec<(String, RateCharge, TrafficClass)> = codec_effects(AUTH_OK)
        .into_iter()
        .map(|effect| match effect {
            Effect::Send {
                frame,
                rpc: None,
                class,
                charge,
                ..
            } => (
                String::from_utf8(frame.bytes().to_vec()).unwrap(),
                charge,
                class,
            ),
            other => panic!("{other:?}"),
        })
        .collect();
    for cfg in [preprod("primary"), preprod(SUB), loopback(SUB)] {
        let subs = account_reads::subscriptions(&cfg).unwrap();
        let built: Vec<(String, RateCharge, TrafficClass)> = (subs.iter())
            .map(|w| {
                assert!(
                    w.frame.redactions().is_empty(),
                    "a subscription names nothing"
                );
                let text = String::from_utf8(w.frame.bytes().to_vec()).unwrap();
                (text, w.charge, w.class)
            })
            .collect();
        assert_eq!(built, sent);
    }
    // The AsyncAPI's three, in the codec's order, each the rate page's 0.1 under Subscribe.
    let channels: Vec<&str> = sent.iter().map(|(text, ..)| text.as_str()).collect();
    assert_eq!(
        channels,
        [
            r#"{"type":"subscribe","params":{"channels":["order_responses"]}}"#,
            r#"{"type":"subscribe","params":{"channels":["fills"]}}"#,
            r#"{"type":"subscribe","params":{"channels":["positions"]}}"#,
        ]
    );
    assert!(sent.iter().all(|(_, charge, class)| {
        *charge == RateCharge::one(OpKind::Subscribe, None) && *class == TrafficClass::Safety
    }));
}

/// The probe codec for the traded subaccount, its authentication answered, waiting for its
/// three confirmations.
fn subscribing_codec() -> QfexExec {
    let mut codec = opened_codec();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    scoped(|scope| {
        let frame = RawFrame::Text(AUTH_OK);
        codec.on_frame(TRADE, frame, scope, &SpecTable::new(), &mut sink, &mut fx)
    })
    .unwrap();
    assert_eq!(fx.take().len(), 3, "the three subscriptions");
    codec
}

#[test]
fn a_confirmation_is_read_exactly_where_the_codec_reads_one() {
    let err = flat("InvalidParameter", "null", r#"{"type":"subscribe"}"#);
    let cases: [(RawFrame<'_>, Option<usize>); 16] = [
        (
            RawFrame::Text(r#"{"subscribed":"order_responses"}"#),
            Some(0),
        ),
        (RawFrame::Text(r#"{"subscribed":"fills"}"#), Some(1)),
        (RawFrame::Text(r#"{"subscribed":"positions"}"#), Some(2)),
        (RawFrame::Text(r#" { "subscribed" : "fills" } "#), Some(1)),
        (RawFrame::Text(r#"{"subscribed":"trades"}"#), None),
        (RawFrame::Text(r#"{"unsubscribed":"fills"}"#), None),
        (RawFrame::Text(r#"{"subscribed":"fills","x":1}"#), None),
        (
            RawFrame::Text(r#"{"subscribed":"fills","subscribed":"fills"}"#),
            None,
        ),
        (RawFrame::Text(r#"{"subscribed":["fills"]}"#), None),
        (RawFrame::Text(r#"{"subscribed":true}"#), None),
        (
            RawFrame::Text(
                r#"{"ack":{"incoming_message":{"type":"subscribe","params":{"channels":["fills"]}}}}"#,
            ),
            None,
        ),
        (RawFrame::Text(&err), None),
        (RawFrame::Text(AUTH_OK), None),
        (RawFrame::Text("[\"fills\"]"), None),
        (RawFrame::Text("nope"), None),
        (RawFrame::Binary(br#"{"subscribed":"fills"}"#), None),
    ];
    let reader = reader(SUB);
    for (frame, want) in cases {
        assert_eq!(
            account_reads::confirmation(&reader, frame),
            want,
            "{frame:?}"
        );
        // The codec counts a confirmation exactly there, and only one completing the three
        // reports the stream authenticated.
        let mut codec = subscribing_codec();
        let mut sink = Sink::default();
        let mut fx = Effects::new();
        let _ = scoped(|scope| {
            codec.on_frame(TRADE, frame, scope, &SpecTable::new(), &mut sink, &mut fx)
        });
        assert_eq!(
            codec.counts().subscribed,
            u64::from(want.is_some()),
            "{frame:?}"
        );
    }
    // The three, in any order, are the three the codec reports the stream authenticated on.
    let mut codec = subscribing_codec();
    let mut sink = Sink::default();
    let mut seen = Vec::new();
    for channel in ["positions", "order_responses", "fills"] {
        let text = format!(r#"{{"subscribed":"{channel}"}}"#);
        seen.extend(account_reads::confirmation(&reader, RawFrame::Text(&text)));
        let mut fx = Effects::new();
        scoped(|scope| {
            let frame = RawFrame::Text(&text);
            codec.on_frame(TRADE, frame, scope, &SpecTable::new(), &mut sink, &mut fx)
        })
        .unwrap();
    }
    assert_eq!(seen, [2, 0, 1]);
    assert!(matches!(sink.0.as_slice(), [ExecEvent::Conn { .. }]));
}

#[test]
fn an_err_on_a_trade_socket_is_read_as_a_page_reads_one() {
    let hint_s = r#""Rate limit exceeded, retry after 7 seconds""#;
    let sub_echo = r#"{"type":"subscribe","params":{"channels":["fills"]}}"#;
    let reader = reader(SUB);
    let errs = [
        (
            flat("RateLimited", hint_s, PAGE_ECHO),
            SocketErr::RateLimited(RateHold {
                charge: Some(RateCharge::one(OpKind::Query, None)),
                retry_after: Some(Duration::from_secs(7)),
            }),
        ),
        (
            nested("RateLimited", "null", sub_echo),
            SocketErr::RateLimited(RateHold {
                charge: Some(RateCharge::one(OpKind::Subscribe, None)),
                retry_after: None,
            }),
        ),
        (
            nested("InvalidParameter", r#""bad channel""#, sub_echo),
            SocketErr::Other(Some(ErrCode::InvalidParameter)),
        ),
        (
            flat("SomethingNew", "null", sub_echo),
            SocketErr::Other(None),
        ),
    ];
    for (frame, want) in &errs {
        assert_eq!(
            account_reads::socket_err(&reader, RawFrame::Text(frame)),
            Some(*want),
            "{frame}"
        );
        // The same err a page read is refused with.
        assert_eq!(page_of(frame, 1000), Err(NotAPage::from(*want)), "{frame}");
    }
    // An err echoing the authentication is kept whole (decision 0134 item 2), so it reads as no
    // JSON object, as a session's codec holds no budget for it (0134 item 6); every frame in no
    // err envelope is none either.
    let echoed = format!(
        r#"{{"type":"Err","error_code":"RateLimited","message":null,"incoming_message":{{"type":"auth","params":{{"hmac":{{"public_key":"{PUBLIC}"}}}}}}}}"#
    );
    let repeated = r#"{"type":"Err","error_code":"RateLimited","error_code":"RateLimited"}"#;
    for frame in [
        echoed.as_str(),
        repeated,
        r#"{"subscribed":"fills"}"#,
        r#"{"ack":{"incoming_message":{}}}"#,
        AUTH_OK,
        "nope",
    ] {
        assert_eq!(
            account_reads::socket_err(&reader, RawFrame::Text(frame)),
            None,
            "{frame}"
        );
    }
    let binary = flat("RateLimited", hint_s, PAGE_ECHO);
    assert_eq!(
        account_reads::socket_err(&reader, RawFrame::Binary(binary.as_bytes())),
        None
    );
    // What each says: a code, never a value.
    assert_eq!(errs[0].1.to_string(), "QFEX answered an err: RateLimited");
    assert_eq!(
        errs[2].1.to_string(),
        "QFEX answered an err: InvalidParameter"
    );
    assert_eq!(errs[3].1.to_string(), "QFEX answered an undocumented err");
}

// ----- The authentication reply -----

#[test]
fn the_authentication_reply_is_success_exactly_where_the_codec_takes_it_as_success() {
    let replies = [
        r#"{"type":"auth","result":"success"}"#,
        r#"{"authenticated":true}"#,
        r#" { "authenticated" : true } "#,
        r#"{"result":"success","type":"auth","note":1}"#,
        r#"{"authenticated":false}"#,
        r#"{"authenticated":"true"}"#,
        r#"{"authenticated":true,"x":1}"#,
        r#"{"type":"auth","result":"failure"}"#,
        r#"{"type":"auth"}"#,
        r#"{"type":"auth","result":"success","err":1}"#,
        r#"{"type":"auth","result":"success","type":"auth"}"#,
        r#"{"type":"auth","result":"success"}"#,
        r#"{"type":"auth","result":"success","incoming_message":{}}"#,
        r#"{"err":{"error_code":"PermissionDenied","message":null,"incoming_message":{"type":"auth","params":{}}}}"#,
        r#"{"type":"Err","error_code":"AlreadyAuthenticated","message":null,"incoming_message":{}}"#,
        r#"{"ack":{"incoming_message":{}}}"#,
        r#"{"subscribed":"fills"}"#,
        "[true]",
        "nope",
        "",
    ];
    let reader = reader(SUB);
    let mut accepted = 0;
    for reply in replies {
        let effects = codec_effects(reply);
        // The codec's success sends the three private subscriptions.
        let codec = effects.len() == 3
            && effects
                .iter()
                .all(|effect| matches!(effect, Effect::Send { .. }));
        assert_eq!(
            account_reads::authenticated(&reader, RawFrame::Text(reply)),
            codec,
            "{reply}"
        );
        accepted += usize::from(codec);
    }
    // The documented and the observed replies, spaced, with a member beside, and escaped.
    assert_eq!(
        accepted, 5,
        "the documented, the observed and their spellings"
    );
    // A binary frame is no reply, to either.
    assert!(!account_reads::authenticated(
        &reader,
        RawFrame::Binary(br#"{"authenticated":true}"#)
    ));
    assert!(matches!(
        codec_frame(RawFrame::Binary(br#"{"authenticated":true}"#)).as_slice(),
        [Effect::Reconnect { .. }]
    ));
}

// ----- Redaction -----

#[test]
fn the_spans_cover_every_listed_account_id_every_user_id_and_every_balance_value() {
    let reader = reader(SUB);
    let http = |body: &str, status: u16| -> (String, Vec<core::ops::Range<u32>>) {
        let resp = answer(status, body);
        let spans = account_reads::redact_inbound(&reader, Inbound::Http(HttpTag(1), resp));
        spans.check(Inbound::Http(HttpTag(1), resp)).unwrap();
        assert!(spans.headers().is_empty());
        (
            blanked(body.as_bytes(), spans.body()),
            spans.body().to_vec(),
        )
    };
    // The listing: each listed id, as many `2`s between its quotes.
    let listing = fixture("subaccounts.json");
    let (kept, spans) = http(&listing, 200);
    assert_eq!(spans.len(), 2);
    for id in [SUB, SUB2] {
        let at = listing.find(id).unwrap() as u32;
        assert!(spans.contains(&(at..at + 36)), "{id}");
        assert!(!kept.contains(id));
    }
    assert!(kept.contains(r#""$schema""#) && kept.contains("schemas.example"));
    // The positions: every balance value and nothing else.
    let positions = fixture("positions.json");
    let (kept, spans) = http(&positions, 200);
    assert_eq!(spans.len(), BALANCE.len());
    let balance: Value = serde_json::from_str(&positions).unwrap();
    let blank: Value = serde_json::from_str(&kept).unwrap();
    for (name, value) in balance["balance"].as_object().unwrap() {
        let written = value.to_string();
        assert_eq!(
            blank["balance"][name].to_string(),
            "2".repeat(written.len()),
            "{name}"
        );
    }
    let written: Vec<String> = BALANCE.iter().map(|v| v.to_string()).collect();
    let at = |v: &str| positions.find(&format!(": {v}")).unwrap() as u32 + 2;
    for (value, span) in written.iter().zip(&spans) {
        assert_eq!(*span, at(value)..at(value) + value.len() as u32, "{value}");
    }
    assert_eq!(blank["positions"], balance["positions"]);
    // A refused answer is kept whole, an empty one has nothing to keep.
    let refused = format!(r#"{{"title":"Unauthorized","detail":"{PUBLIC} {SUB}"}}"#);
    let (kept, spans) = http(&refused, 401);
    assert_eq!(spans, vec![0..refused.len() as u32]);
    assert!(!kept.contains(PUBLIC) && !kept.contains(SUB));
    assert!(http("", 500).1.is_empty());
    // A page: every user_id, and the page decodes the same from what is kept.
    let all = fixture("all_orders_response.json");
    let frame = RawFrame::Text(&all);
    let spans = account_reads::redact_inbound(&reader, Inbound::Frame(frame));
    spans.check(Inbound::Frame(frame)).unwrap();
    assert_eq!(spans.body().len(), all.matches(USER).count());
    let kept = blanked(all.as_bytes(), spans.body());
    assert!(!kept.contains(USER));
    assert_eq!(page_of(&kept, 1000), page_of(&all, 1000));
    // An echoing frame is kept as the order-entry codec keeps one on an epoch with no arm.
    let codec = opened_codec();
    for frame in [
        flat(
            "RateLimited",
            r#""Rate limit exceeded, retry after 7 seconds""#,
            PAGE_ECHO,
        ),
        nested("InvalidParameter", r#""bad""#, PAGE_ECHO),
        r#"{"ack":{"incoming_message":{"type":"get_user_orders","params":{"limit":1}}}}"#
            .to_owned(),
        nested("PermissionDenied", "null", r#"{"type":"auth","params":{}}"#),
        all.clone(),
        r#"{"authenticated":true}"#.to_owned(),
    ] {
        let input = Inbound::Frame(RawFrame::Text(&frame));
        assert_eq!(
            account_reads::redact_inbound(&reader, input),
            codec.redact_inbound(input),
            "{frame}"
        );
        // And the page reads the RateLimited err from what is kept, as live.
        let kept = blanked(
            frame.as_bytes(),
            account_reads::redact_inbound(&reader, input).body(),
        );
        if frame.contains("RateLimited") {
            assert_eq!(page_of(&kept, 1000), page_of(&frame, 1000));
        }
    }
    let binary = RawFrame::Binary(br#"{"incoming_message":{}}"#);
    assert_eq!(
        account_reads::redact_inbound(&reader, Inbound::Frame(binary)).body(),
        vec![0..23]
    );
    // The account's own key and id are spans wherever they appear.
    let named = format!(r#"{{"note":"{PUBLIC}","other":"{SUB}"}}"#);
    let spans = account_reads::redact_inbound(&reader, Inbound::Frame(RawFrame::Text(&named)));
    check_redactions(named.as_bytes(), spans.body()).unwrap();
    assert_eq!(spans.body().len(), 2);
}

/// The listing decodes the live answer only: the journal keeps each listed id as a keyed hash,
/// read back as 36 `2`s, dashes included, which is no UUID. The positions read no member the
/// journal blanks, so they decode the same from the journal as live.
#[test]
fn a_journaled_listing_is_refused_in_replay_and_journaled_positions_decode_as_live() {
    let reader = reader(SUB);
    let kept = |body: &str| {
        let resp = ok(body);
        let spans = account_reads::redact_inbound(&reader, Inbound::Http(HttpTag(1), resp));
        blanked(body.as_bytes(), spans.body())
    };
    let listing = fixture("subaccounts.json");
    assert_eq!(decode_listing(&listing).len(), 2);
    let replayed = kept(&listing);
    assert!(replayed.contains(&format!(r#""{}""#, "2".repeat(36))));
    assert_eq!(
        account_reads::decode_subaccounts(&ok(&replayed)),
        Err(AnswerError::Invalid("account_ids"))
    );
    for name in ["positions.json", "positions_flat.json"] {
        let positions = fixture(name);
        let replayed = kept(&positions);
        assert_ne!(replayed, positions, "{name}");
        let live = decode_positions(&positions).unwrap();
        assert!(!live.rows().is_empty(), "{name}");
        assert_eq!(decode_positions(&replayed), Ok(live), "{name}");
    }
}

// ----- What nothing shows -----

#[test]
fn no_debug_display_or_error_shows_a_header_value_an_account_id_a_user_id_or_a_balance() {
    let cfg = preprod(SUB);
    let (signature, nonce, unix_ts) = signed(&ctx(9));
    let mut shown = Vec::new();
    for account in ["primary", SUB, SUB2] {
        let reader = reader(account);
        shown.push(format!("{reader:?}"));
        shown.push(format!(
            "{:?}",
            account_reads::subaccounts(&cfg, &reader, &ctx(9)).unwrap()
        ));
        shown.push(format!(
            "{:?}",
            account_reads::positions(&cfg, &reader, &ctx(9)).unwrap()
        ));
        // Its own nonce and time are written in clear, as the codec's are (FBC-77aa), so it
        // is made with another context than the REST reads' headers.
        shown.push(format!(
            "{:?}",
            account_reads::authentication(&cfg, &reader, &ctx(10)).unwrap()
        ));
        shown.push(format!(
            "{:?}",
            account_reads::trade_url(&cfg, &reader).unwrap()
        ));
    }
    let listed = decode_listing(&fixture("subaccounts.json"));
    shown.push(format!("{listed:?}"));
    assert_eq!(format!("{listed:?}"), "Subaccounts { listed: 2 }");
    shown.push(format!("{:?}", listed.accounts().collect::<Vec<_>>()));
    shown.push(format!(
        "{:?}",
        decode_positions(&fixture("positions.json")).unwrap()
    ));
    shown.push(format!(
        "{:?}",
        page_of(&fixture("all_orders_response.json"), 1000).unwrap()
    ));
    // Every refusal of an answer that holds them.
    let listing = fixture("subaccounts.json");
    for status in [401, 500] {
        let err = account_reads::decode_subaccounts(&answer(status, &listing)).unwrap_err();
        shown.push(format!("{err} {err:?}"));
    }
    let bad = listing.replace(SUB2, &SUB2.to_uppercase());
    let err = account_reads::decode_subaccounts(&ok(&bad)).unwrap_err();
    shown.push(format!("{err} {err:?}"));
    let positions = fixture("positions.json").replace(r#""position": 1e-12"#, r#""position": "1""#);
    let err = decode_positions(&positions).unwrap_err();
    assert_eq!(err, AnswerError::Invalid("position"));
    shown.push(format!("{err} {err:?}"));
    for frame in [
        flat("InvalidOrder", &format!(r#""{SUB} {USER}""#), PAGE_ECHO),
        fixture("all_orders_response.json").replace("2.708", "\"x\""),
    ] {
        let err = page_of(&frame, 1000).unwrap_err();
        shown.push(format!("{err} {err:?}"));
    }
    let all = shown.join("\n");
    let mut forbidden = vec![
        PUBLIC,
        SECRET,
        signature.as_str(),
        nonce.as_str(),
        unix_ts.as_str(),
        SUB,
        SUB2,
        USER,
    ];
    let upper = SUB2.to_uppercase();
    forbidden.push(&upper);
    forbidden.extend(BALANCE.iter().filter(|v| v.len() > 3));
    for word in forbidden {
        assert!(!all.contains(word), "{word} shown in:\n{all}");
    }
}

/// No credential is read or held in the account reads' own module: every signed part and
/// account id comes from `src/auth`'s handle (`tests/signing.rs` scans every file too).
#[test]
fn the_account_reads_read_no_credential_outside_src_auth_and_src_sign() {
    let text =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/account_reads.rs"))
            .unwrap();
    for word in [
        "expose(",
        "Secret",
        "Secrets",
        "HmacKey",
        "creds",
        "sign::",
        "SECRET_KEY",
        "PUBLIC_KEY",
        "TradeAuth",
    ] {
        assert!(!text.contains(word), "src/account_reads.rs names {word}");
    }
    assert!(text.contains("reader.frame(ctx)?"));
    assert!(text.contains("reader.user_headers(ctx)?"));
    assert!(text.contains("reader.rest_headers(ctx)?"));
}
