//! FBC-xzp (decisions 0003, 0009, 0015, 0016, 0054, 0072): the Paradex factory's order entry.
//! `caps()` declares Paradex's `ExecCaps`; `plan_exec` gives one order-entry endpoint from the
//! configuration, its socket on SBE 1:2; `exec_codec` builds the order-entry codec or the
//! read-only one from synthetic `Secrets`, driven here through `ExecCodec` only, and refuses a
//! missing or malformed credential naming its key and never its value; and Test Connection's
//! plan decodes a hand-built account answer into an `AccountSummary` whose `Debug` shows neither
//! the address nor the balance.
//!
//! The key and account are the synthetic ones in `fixtures/paradex/signing` (read from the
//! vectors file's header), the token made-up text that never looks like one Paradex issues.

mod common;
mod md;

use std::fs;
use std::sync::OnceLock;
use std::time::Duration;

use common::Vectors;
use fbc_core::{
    AccountKey, CancelOnDisconnect, Channel, CidMint, ClientOrderId, ConfigError, ConnState,
    Effect, Effects, EncodeCtx, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, FieldUnit,
    HttpFailure, HttpPlan, HttpResponse, HttpTag, Lots, MonoNs, Namespace, NamespaceLease,
    NewOrder, NonceBlock, NotSentReason, OrderKind, PathStamps, PlanError, PlanStep, RawFrame,
    RpcCall, RpcId, Secret, Secrets, Side, SnapshotSource, Ticks, Tif, VenueCommand, VenueConfig,
    VenueError, VenueFactory, VenueMeta, WallNs, WireUrl, dispatch, dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, ACCOUNT_TAG, CHAIN_ID, LOGIN_TAG, REFRESH, REST_URL, SIGNATURE_LIFETIME,
    SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::{LOGIN_REQUEST, PRIVATE_CHANNELS, ParadexEncoder, exec_caps};
use fbc_venue_paradex::factory::{
    EXEC_MODE, EXEC_STREAM, EXEC_URL, ExecMode, MD_URL, RPC_TIMEOUT, caps, market_data_caps,
};
use md::BTC;
use serde_json::Value;

const REST: &str = "https://api.testnet.paradex.trade/v1";
const WS: &str = "wss://ws.api.testnet.paradex.trade/v1";
/// A made-up session token: letters, digits, `-`, `_` and `.`, never three base64 parts.
const TOKEN: &str = "SYNTHETIC.session-token.factory";
/// How long an order request awaits its reply, as configured (`2500ms`).
const RPC_WAIT: Duration = Duration::from_millis(2_500);
/// The engine namespace the client ids are minted in.
const OWN: Namespace = Namespace::new(7);
/// A malformed credential: no Paradex value looks like it, and no message may echo it.
const MALFORMED: &str = "0xSYNTHETIC-not-hex-0fbc";

fn cfg(mode: &str) -> VenueConfig {
    let vectors = Vectors::read();
    let chain = vectors.header["chain_id"].clone();
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (MD_URL, WS),
        (EXEC_URL, WS),
        (EXEC_MODE, mode),
        (RPC_TIMEOUT, "2500ms"),
        (REST_URL, REST),
        (CHAIN_ID, &chain),
        (SIGNATURE_LIFETIME, "3600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// `cfg(mode)` without `key`, or with `key` set to `value`.
fn cfg_with(mode: &str, key: &'static str, value: Option<&str>) -> VenueConfig {
    let base = cfg(mode);
    let mut cfg = VenueConfig::new();
    for k in [
        MD_URL,
        EXEC_URL,
        EXEC_MODE,
        RPC_TIMEOUT,
        REST_URL,
        CHAIN_ID,
        SIGNATURE_LIFETIME,
        REFRESH,
        TIMEOUT,
    ] {
        if k != key {
            cfg.insert(k, base.get(k).unwrap());
        }
    }
    if let Some(value) = value {
        cfg.insert(key, value);
    }
    cfg
}

/// The synthetic account and key, as the consumer hands them over.
fn creds() -> Secrets {
    let vectors = Vectors::read();
    let mut creds = Secrets::new();
    creds.insert(
        ACCOUNT_ADDRESS,
        Secret::new(vectors.header["account"].clone()),
    );
    creds.insert(SIGNING_KEY, Secret::new(vectors.header["key"].clone()));
    creds
}

/// [`creds`] without `key`, or with `key` set to `value`.
fn creds_with(key: &'static str, value: Option<&str>) -> Secrets {
    let mut creds = creds();
    creds.take(key);
    if let Some(value) = value {
        creds.insert(key, Secret::new(value.to_owned()));
    }
    creds
}

/// The synthetic account and key text, which nothing shown may contain.
fn secret_text() -> [String; 2] {
    let vectors = Vectors::read();
    [
        vectors.header["account"].clone(),
        vectors.header["key"].clone(),
    ]
}

fn assert_hidden(shown: &str, secrets: &[&str]) {
    for secret in secrets {
        assert!(!shown.contains(secret), "{shown}");
    }
}

fn ctx() -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_780_000_000_000_000_000),
        mono: MonoNs(1),
        nonces: NonceBlock::EMPTY,
    }
}

fn ok(body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status: 200,
        headers: &[],
        body,
    })
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, ev: ExecEvent) {
        self.0.push(ev);
    }
}

/// The JSON frames `fx` writes on the order-entry stream.
fn sends(fx: &[Effect]) -> Vec<Value> {
    fx.iter()
        .filter_map(|effect| match effect {
            Effect::Send { stream, frame, .. } => {
                assert_eq!(*stream, EXEC_STREAM);
                Some(serde_json::from_slice(frame.bytes()).expect("a JSON-RPC frame"))
            }
            _ => None,
        })
        .collect()
}

/// `codec`, opened on the planned stream, logged in and its auth frame acknowledged: the
/// session reports the stream authenticated and subscribes the private channels.
fn authenticate(codec: &mut dyn ExecCodec) {
    let specs = md::specs();
    let mut fx = Effects::new();
    codec.on_open(EXEC_STREAM, &ctx(), &mut fx);
    let login = fx.take();
    assert!(
        matches!(login.as_slice(), [Effect::Http { tag, .. }] if *tag == LOGIN_REQUEST),
        "one login: {login:?}"
    );
    let body = format!(r#"{{"jwt_token": "{TOKEN}"}}"#);
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    dispatch(&caps(), OWN, |scope| {
        codec.on_http(
            LOGIN_REQUEST,
            ok(body.as_bytes()),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    })
    .unwrap();
    let [auth]: [Value; 1] = sends(&fx.take()).try_into().expect("one auth frame");
    assert_eq!(auth["method"], "auth");
    assert_eq!(auth["params"]["bearer"], TOKEN);
    let ack = format!(
        r#"{{"jsonrpc":"2.0","result":{{"node_id":"a1b2"}},"id":{}}}"#,
        auth["id"]
    );
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    dispatch(&caps(), OWN, |scope| {
        let frame = RawFrame::Text(&ack);
        codec.on_frame(EXEC_STREAM, frame, scope, &specs, &mut sink, &mut fx)
    })
    .unwrap();
    assert_eq!(
        sink.0,
        [ExecEvent::Conn {
            stream: EXEC_STREAM,
            state: ConnState::Authenticated
        }]
    );
    assert_eq!(sends(&fx.take()).len(), PRIVATE_CHANNELS.len());
}

/// A post-only buy of 150 lots at tick 620000, under a client id minted in [`OWN`] once per
/// test process: the tests run in parallel, and two leases on one directory would collide
/// (Reviewer B RB-xzp-2 on PR #111).
fn place() -> VenueCommand {
    static CID: OnceLock<ClientOrderId> = OnceLock::new();
    let cid = *CID.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-paradex-factory-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(3), OWN).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_622_400_000_000_000));
        let cid = mint.mint().unwrap();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        cid
    });
    VenueCommand::Place(NewOrder {
        cid,
        inst: BTC,
        side: Side::Buy,
        qty: Lots::new(150).unwrap(),
        kind: OrderKind::Limit { px: Ticks(620_000) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    })
}

/// What `codec` returned and asked for encoding `cmd` as request `rpc`.
fn encode(
    codec: &mut dyn ExecCodec,
    cmd: &VenueCommand,
    rpc: RpcId,
) -> (Result<(), NotSentReason>, Vec<Effect>) {
    let mut fx = Effects::new();
    let result = codec.encode(
        cmd,
        rpc,
        &md::specs(),
        &ctx(),
        &mut PathStamps::off(),
        &mut fx,
    );
    (result.map(|_| ()), fx.take())
}

/// The codec the factory builds under `cfg` from `creds`.
fn built(cfg: &VenueConfig, creds: Secrets) -> Result<Box<dyn ExecCodec>, VenueError> {
    ParadexFactory
        .exec_codec(cfg, creds)
        .expect("Paradex takes orders")
}

/// Asserts `refused` is the configuration refused for `key`, `Missing` when `missing`, and that
/// neither its `Debug` nor its `Display` shows a credential or [`MALFORMED`].
fn refused_for(refused: Result<Box<dyn ExecCodec>, VenueError>, key: &'static str, missing: bool) {
    let Err(err) = refused else {
        panic!("{key}: a codec was built")
    };
    match &err {
        VenueError::Config(ConfigError::Missing(k)) if missing => assert_eq!(*k, key),
        VenueError::Config(ConfigError::Invalid { key: k, .. }) if !missing => {
            assert_eq!(*k, key)
        }
        other => panic!("{key}: {other:?}"),
    }
    let [account, signing_key] = secret_text();
    for shown in [format!("{err:?}"), format!("{err}")] {
        assert_hidden(&shown, &[&account, &signing_key, MALFORMED]);
    }
}

#[test]
fn the_factory_declares_paradex_exec_caps_with_its_market_data() {
    let declared = ParadexFactory.caps(&cfg("orders")).unwrap();
    assert_eq!(declared, caps());
    assert_eq!(declared.exec, Some(exec_caps()));
    // The same in either mode: the read-only codec refuses what the caps allow.
    assert_eq!(ParadexFactory.caps(&cfg("read-only")).unwrap(), declared);
    // Market data and matching are unchanged; the order limits are added (decision 0054).
    let md = market_data_caps();
    assert_eq!(md.exec, None);
    assert_eq!((&declared.md, declared.matching), (&md.md, md.matching));
    assert_eq!(declared.limits.len(), md.limits.len() + 2);
    // A runtime session takes it: per-connection cancel-on-disconnect, re-armed on reconnect.
    let exec = declared.exec.unwrap();
    assert!(matches!(
        exec.order.cancel_on_disconnect,
        CancelOnDisconnect::PerConnection { .. }
    ));
    // The owner's answer C to RB-olg-3: no resync seeds a Paradex position until a new record.
    assert_eq!(exec.order.snapshot_source, SnapshotSource::Untrustworthy);
}

#[test]
fn plan_exec_gives_one_order_entry_endpoint_from_the_configuration_on_sbe_1_2() {
    let expected = vec![ExecEndpoint {
        stream: EXEC_STREAM,
        url: WireUrl::plain(format!("{WS}?sbeSchemaId=1&sbeSchemaVersion=2")),
    }];
    for mode in ["orders", "read-only"] {
        assert_eq!(ParadexFactory.plan_exec(&cfg(mode)), Ok(expected.clone()));
    }
    // The URL is the configured one, not market data's.
    let other = "wss://ws.api.prod.paradex.trade/v1";
    let planned = ParadexFactory.plan_exec(&cfg_with("orders", EXEC_URL, Some(other)));
    assert_eq!(
        planned.unwrap()[0].url,
        WireUrl::plain(format!("{other}?sbeSchemaId=1&sbeSchemaVersion=2"))
    );
    // Market data stays on 1:1.
    let md = ParadexFactory::md_url(&cfg("orders")).unwrap();
    assert_eq!(
        md,
        WireUrl::plain(format!("{WS}?sbeSchemaId=1&sbeSchemaVersion=1"))
    );
    // A missing or unusable URL is refused, naming the key.
    let missing = ParadexFactory.plan_exec(&cfg_with("orders", EXEC_URL, None));
    assert_eq!(
        missing,
        Err(VenueError::Config(ConfigError::Missing(EXEC_URL)))
    );
    for url in [
        "https://x.invalid/v1",
        "wss://x.invalid/v1?a=1",
        "wss://x.invalid/v1#f",
    ] {
        let refused = ParadexFactory.plan_exec(&cfg_with("orders", EXEC_URL, Some(url)));
        assert!(
            matches!(
                refused,
                Err(VenueError::Config(ConfigError::Invalid {
                    key: EXEC_URL,
                    ..
                }))
            ),
            "{url}: {refused:?}"
        );
    }
}

/// The order socket carries the session token and every signed order, so plain `ws://` is for a
/// loopback test stub only, as plain `http://` is for the REST base (Reviewer B RB-xzp-1 on
/// PR #111).
#[test]
fn plan_exec_refuses_plain_ws_to_a_remote_host_and_takes_it_to_a_loopback_stub() {
    for url in [
        "ws://ws.api.prod.paradex.trade/v1",
        "ws://10.0.0.1/v1",
        "ws://[2001:db8::1]:443/v1",
        "ws://localhost.example.com/v1",
        "ws://user@127.0.0.1/v1",
        "ws://127.0.0.1:0/v1",
        "ws:///v1",
    ] {
        let refused = ParadexFactory.plan_exec(&cfg_with("orders", EXEC_URL, Some(url)));
        assert!(
            matches!(
                refused,
                Err(VenueError::Config(ConfigError::Invalid {
                    key: EXEC_URL,
                    ..
                }))
            ),
            "{url}: {refused:?}"
        );
    }
    for url in [
        "ws://127.0.0.1:8080/v1",
        "ws://127.5.6.7/v1",
        "ws://localhost:9001",
        "ws://LOCALHOST/v1",
        "ws://[::1]:9001/v1",
    ] {
        let planned = ParadexFactory.plan_exec(&cfg_with("orders", EXEC_URL, Some(url)));
        assert_eq!(
            planned.map(|endpoints| endpoints[0].url.clone()),
            Ok(WireUrl::plain(format!(
                "{url}?sbeSchemaId=1&sbeSchemaVersion=2"
            ))),
            "{url}"
        );
    }
}

#[test]
fn the_schema_names_the_order_entry_keys_at_account_scope() {
    let schema = ParadexFactory.config_schema();
    let field = |key| schema.iter().find(|f| f.key == key).expect(key);
    for key in [
        EXEC_URL,
        EXEC_MODE,
        RPC_TIMEOUT,
        ACCOUNT_ADDRESS,
        SIGNING_KEY,
    ] {
        assert_eq!(field(key).scope, fbc_core::ConfigScope::Account, "{key}");
    }
    assert_eq!(field(RPC_TIMEOUT).unit, FieldUnit::Duration);
    assert!(field(EXEC_URL).doc.contains("sbeSchemaVersion=2"));
    assert!(field(EXEC_MODE).doc.contains("no default"));
}

#[test]
fn exec_mode_and_the_rpc_timeout_are_read_from_the_configuration_with_no_default() {
    let mode = ParadexFactory::exec_mode;
    assert_eq!(mode(&cfg("orders")), Ok(ExecMode::Orders));
    assert_eq!(mode(&cfg("read-only")), Ok(ExecMode::ReadOnly));
    assert_eq!(
        mode(&cfg_with("orders", EXEC_MODE, None)),
        Err(ConfigError::Missing(EXEC_MODE))
    );
    for text in ["", "Orders", "readonly", "trade"] {
        let refused = mode(&cfg_with("orders", EXEC_MODE, Some(text)));
        assert!(
            matches!(refused, Err(ConfigError::Invalid { key: EXEC_MODE, .. })),
            "{text}: {refused:?}"
        );
    }
    let timeout = ParadexFactory::rpc_timeout;
    assert_eq!(timeout(&cfg("orders")), Ok(RPC_WAIT));
    let three = cfg_with("orders", RPC_TIMEOUT, Some("3s"));
    assert_eq!(timeout(&three), Ok(Duration::from_secs(3)));
    for text in ["", "0ms", "0s", "ms", "s", "2500", "2.5s", "-1s", "1m"] {
        let refused = timeout(&cfg_with("orders", RPC_TIMEOUT, Some(text)));
        assert!(
            matches!(
                refused,
                Err(ConfigError::Invalid {
                    key: RPC_TIMEOUT,
                    ..
                })
            ),
            "{text}: {refused:?}"
        );
    }
}

#[test]
fn exec_codec_builds_the_order_entry_codec_signing_with_the_key_in_secrets() {
    let mut codec = built(&cfg("orders"), creds()).unwrap();
    // Order entry is WebSocket only: nothing goes out before the connection is authenticated.
    let cmd = place();
    let (before, fx) = encode(codec.as_mut(), &cmd, RpcId(1));
    assert_eq!(before, Err(NotSentReason::Disconnected));
    assert!(fx.is_empty());
    authenticate(codec.as_mut());
    // On the authenticated connection the place is one frame awaiting its reply for the
    // configured timeout, signed by the account's key: the very frame an encoder holding the
    // vectors' signer writes.
    let (result, fx) = encode(codec.as_mut(), &cmd, RpcId(1));
    assert_eq!(result, Ok(()));
    let [Effect::Send { rpc, .. }] = fx.as_slice() else {
        panic!("one frame: {fx:?}")
    };
    assert_eq!(
        *rpc,
        Some(RpcCall {
            id: RpcId(1),
            timeout: RPC_WAIT
        })
    );
    let signer = Box::new(Vectors::read().signer());
    let mut encoder = ParadexEncoder::new(signer, EXEC_STREAM, RPC_WAIT);
    let mut expected = Effects::new();
    let specs = md::specs();
    let mut t = PathStamps::off();
    encoder
        .encode(&cmd, RpcId(1), &specs, &ctx(), &mut t, &mut expected)
        .unwrap();
    assert_eq!(fx, expected.take());
    let [json]: [Value; 1] = sends(&fx).try_into().unwrap();
    assert_eq!(json["method"], "order.create");
    assert!(json["params"]["signature"].is_string());
}

#[test]
fn exec_codec_builds_the_read_only_codec_which_refuses_every_command() {
    let mut codec = built(&cfg("read-only"), creds()).unwrap();
    authenticate(codec.as_mut());
    let (result, fx) = encode(codec.as_mut(), &place(), RpcId(1));
    assert_eq!(result, Err(NotSentReason::Unsupported));
    assert!(fx.is_empty());
    // The read-only mode needs no request timeout: it sends no request.
    let without = cfg_with("read-only", RPC_TIMEOUT, None);
    assert!(built(&without, creds()).is_ok());
}

#[test]
fn exec_codec_refuses_a_missing_or_malformed_credential_naming_it_and_never_its_value() {
    let [account, key] = secret_text();
    for mode in ["orders", "read-only"] {
        for field in [ACCOUNT_ADDRESS, SIGNING_KEY] {
            let missing = built(&cfg(mode), creds_with(field, None));
            refused_for(missing, field, true);
            let malformed = built(&cfg(mode), creds_with(field, Some(MALFORMED)));
            refused_for(malformed, field, false);
        }
        // A key out of the curve's range, in the shape of a valid one.
        let out_of_range = format!("0x{}", "f".repeat(64));
        let refused = built(&cfg(mode), creds_with(SIGNING_KEY, Some(&out_of_range)));
        let Err(err) = refused else {
            panic!("{mode}: out-of-range key accepted")
        };
        assert!(matches!(
            err,
            VenueError::Config(ConfigError::Invalid {
                key: SIGNING_KEY,
                ..
            })
        ));
        assert_hidden(&format!("{err:?} {err}"), &[&out_of_range, &account, &key]);
        // No credentials at all: the first one the codec needs is named.
        let none = built(&cfg(mode), Secrets::new());
        refused_for(none, ACCOUNT_ADDRESS, true);
    }
}

#[test]
fn exec_codec_refuses_a_missing_or_invalid_setting_naming_it() {
    for (mode, key) in [
        ("orders", EXEC_MODE),
        ("orders", RPC_TIMEOUT),
        ("orders", REST_URL),
        ("orders", CHAIN_ID),
        ("read-only", REST_URL),
        ("read-only", CHAIN_ID),
    ] {
        refused_for(built(&cfg_with(mode, key, None), creds()), key, true);
        let bad = cfg_with(mode, key, Some("not-a-value"));
        refused_for(built(&bad, creds()), key, false);
    }
}

#[test]
fn test_connection_decodes_the_account_into_a_summary_whose_debug_shows_no_address_or_balance() {
    let plan = ParadexFactory
        .test_connection(&cfg("orders"), creds())
        .expect("Paradex proves credentials")
        .unwrap();
    let run = |plan: HttpPlan<_>, answers: &[(HttpTag, _)]| {
        dispatch_market_data(&caps(), |scope| plan.parse(answers, scope))
    };
    let next = |step: Result<PlanStep<_>, PlanError>| {
        let Ok(PlanStep::Next(round)) = step else {
            panic!("another round follows")
        };
        round.build(&ctx()).unwrap()
    };
    let login = next(run(plan, &[]));
    assert!(matches!(
        login.requests(),
        [Effect::Http { tag, .. }] if *tag == LOGIN_TAG
    ));
    let body = format!(r#"{{"jwt_token": "{TOKEN}"}}"#);
    let read = next(run(login, &[(LOGIN_TAG, ok(body.as_bytes()))]));
    // A hand-built answer in docs.paradex.trade's "Get account information" shape.
    const ADDRESS: &str = "0x5f1ac0ffee5ca1ab1ec0de";
    const BALANCE: &str = "98765.4321";
    let answer = format!(
        r#"{{"account":"{ADDRESS}","account_value":"{BALANCE}","free_collateral":"1","settlement_asset":"USDC","status":"ACTIVE"}}"#
    );
    let summary = run(read, &[(ACCOUNT_TAG, ok(answer.as_bytes()))])
        .unwrap()
        .done()
        .unwrap();
    assert_eq!(summary.account, ADDRESS);
    assert!(summary.equity.is_some());
    let shown = format!("{summary:?}");
    assert_hidden(&shown, &[ADDRESS, BALANCE, "98765", "4321", "c0ffee"]);
}
