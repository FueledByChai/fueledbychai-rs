//! FBC-mfey: QFEX's pre-production probe mode (decision 0132 items 1, 3 and 6, as 0134 item 7
//! narrows item 3).
//!
//! - The guard admits exactly the three pre-production origins, with or without a trailing `/`,
//!   and an all-loopback set; it refuses a mixed set, a `qfex.com` host, any other origin, an
//!   unknown mode, a missing or out-of-range setting and an `rpc_timeout` not longer than the
//!   500 ms speed bump, each in every factory method before anything is planned, and in the
//!   probe's own connections, which take decision 0122 item 1's account check too.
//! - An absent mode gives `exec: None` and no codec; under the mode the factory declares item
//!   6's `ExecCaps`, each value cited, matching what the encoder does, and one order-entry
//!   endpoint.
//! - The codec writes one arm per epoch, refuses a second arm and a disarm with nothing written,
//!   and leaves the arm unanswered at its deadline; its epoch stays open on private state, and
//!   its resync asks for nothing.
//! - The journal keeps an equal arm echo but its message, and a RateLimited err echoing the arm
//!   with a documented hint in clear; a non-equal echo in the arm's window as its shape, but for
//!   what 0134 item 3 keeps; every other echo as 0126 and 0134 say; and decoder replay of the
//!   journal gives the same events and effects.
//! - The private REST reads' headers come from `src/auth`, signed as QFEX's REST reference says.
//!
//! Every key, account, id and frame here is synthetic: the frames are built from the shapes
//! QFEX's pages show (each test names the page), or are `fixtures/qfex/private` (its
//! `SYNTHETIC` file); the signature vector is `fixtures/qfex/signing` (its `SYNTHETIC` file).

use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckModel, AssetSym, CancelOnDisconnect, CancelOrder, CancelScope, Channel, CidMint,
    ClientOrderId, ConfigError, ConnKey, ConnState, DecodeError, Effect, Effects, EncodeCtx,
    EncodeReceipt, ExecCodec, ExecEvent, ExecSink, Feature, FundingSpec, Inbound, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock,
    NotSentReason, OpKind, OrderKind, OrderKindTag, OrderRef, OrderShapes, OrderingKey, PathStamps,
    PriceGrid, QueryOrder, RateCharge, RawFrame, Readiness, RefKind, RpcCall, RpcId, Secret,
    Secrets, Side, SignError, SizeStep, SnapshotReads, SnapshotSource, SpecTable, Stamp,
    SubmitOutcome, Support, TagSet, Ticks, Tif, TifTag, TradingStatus, TrafficClass, UnderlyingId,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueId, VenueMeta,
    VenueOrderId, Via, WallNs, check_redactions, dispatch, dispatch_market_data,
};
use fbc_journal::{JournalReader, JournalWriter, Record, RedactionKey};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::{ARM, CHANNELS, ErrCode, QfexExec, TRADE, probe_exec_caps};
use fbc_venue_qfex::probe::{
    self, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, Origins, PREPROD_MDS,
    PREPROD_PROBE, PREPROD_REST, PREPROD_TRADE, SPEED_BUMP,
};
use fbc_venue_qfex::sign::HmacKey;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex};
use serde_json::Value;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// The AsyncAPI's authentication reply.
const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
/// The consumer's request deadline in these configurations.
const RPC_TIMEOUT: Duration = Duration::from_millis(2_500);
/// A fixed 0.01 grid, lots of 0.01: `AAPL-USD`.
const AAPL: InstrumentId = InstrumentId::new(7);
/// A made-up venue order UUID, version 4.
const ORDER: &str = "2f05173c-992a-426b-bcaf-6b341644bb62";
/// Our namespace.
const NS: Namespace = Namespace::new(7);
/// What the codec answers a frame it cannot read with.
const NOT_AN_OBJECT: DecodeError = DecodeError::Malformed("QFEX trade frame is not a JSON object");
const INVALID_STATE: DecodeError = DecodeError::Malformed("invalid QFEX private state");

// ----- Configurations -----

/// A probe configuration for `account` on these three origins.
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

/// The probe on QFEX's pre-production origins.
fn preprod(account: &str) -> VenueConfig {
    on(account, PREPROD_TRADE, PREPROD_REST, PREPROD_MDS)
}

/// The probe on a loopback stub.
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

/// `cfg` without `key`: rebuilt from what it holds, since a configuration removes nothing.
fn without(account: &str, key: &str) -> VenueConfig {
    let full = preprod(account);
    let mut cfg = VenueConfig::new();
    for name in [
        KEY_EXEC_MODE,
        auth::TRADE_URL,
        KEY_REST_URL,
        KEY_MDS_URL,
        KEY_REST_TIMEOUT,
        KEY_PAGE_SIZE,
        KEY_RESYNC_TIMEOUT,
        KEY_RPC_TIMEOUT,
        auth::ACCOUNT_ID,
        auth::ACCOUNT_KEY,
        auth::PUBLIC_KEY,
    ] {
        if name != key {
            cfg.insert(name, full.get(name).unwrap());
        }
    }
    cfg
}

fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

/// The probe's settings `cfg` sets, which the guard admitted.
fn settings(cfg: &VenueConfig) -> probe::Probe {
    probe::mode(cfg).unwrap().unwrap()
}

// ----- Building and driving a codec -----

fn probe_codec(account: &str) -> QfexExec {
    let cfg = preprod(account);
    QfexExec::probe(TradeAuth::new(&cfg, secrets()).unwrap(), settings(&cfg))
}

fn recording(account: &str) -> QfexExec {
    let cfg = preprod(account);
    QfexExec::new(TradeAuth::new(&cfg, secrets()).unwrap(), RPC_TIMEOUT)
}

fn caps() -> VenueCaps {
    Qfex.caps(&preprod("primary")).unwrap()
}

/// The context of the `n`th call: its wall time and two nonces.
fn ctx(n: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_000_000_000_000_000 + n as i64 * 1_000_000_000),
        mono: MonoNs(n),
        nonces: NonceBlock::new(vec![n, u64::MAX - n]),
    }
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

/// What one call gave: its result (a decode error or a refusal, as text), the events pushed and
/// the effects asked for.
#[derive(Debug, PartialEq)]
struct Out {
    result: Result<(), String>,
    events: Vec<ExecEvent>,
    effects: Vec<Effect>,
}

impl Out {
    fn quiet() -> Out {
        Out {
            result: Ok(()),
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Whether it ended its epoch, and only that.
    fn reconnects(&self) -> bool {
        self.events.is_empty()
            && matches!(
                self.effects.as_slice(),
                [Effect::Reconnect { stream: TRADE, .. }]
            )
    }
}

fn text(codec: &mut dyn ExecCodec, frame: &str) -> Out {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, NS, |scope| {
        codec.on_frame(
            TRADE,
            RawFrame::Text(frame),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    });
    Out {
        result: result.map_err(|e| format!("{e:?}")),
        events: sink.0,
        effects: fx.take(),
    }
}

fn open(codec: &mut dyn ExecCodec, n: u64) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(n), &mut fx);
    fx.take()
}

fn encode(codec: &mut dyn ExecCodec, cmd: &VenueCommand, rpc: u64) -> Out {
    let mut fx = Effects::new();
    let sent = codec.encode(
        cmd,
        RpcId(rpc),
        &specs(),
        &ctx(rpc),
        &mut PathStamps::off(),
        &mut fx,
    );
    Out {
        result: sent
            .map(|receipt| assert_eq!(receipt, EncodeReceipt::new()))
            .map_err(|why| format!("{why:?}")),
        events: Vec::new(),
        effects: fx.take(),
    }
}

fn deadline(codec: &mut dyn ExecCodec, rpc: u64) -> Out {
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(rpc), &mut sink);
    Out {
        events: sink.0,
        ..Out::quiet()
    }
}

fn authenticated() -> ExecEvent {
    ExecEvent::Conn {
        stream: TRADE,
        state: ConnState::Authenticated,
    }
}

/// Opens epoch `n` and authenticates it: the auth reply sends the three subscriptions, and
/// their last confirmation reports the stream (FBC-bsou).
fn authenticate(codec: &mut dyn ExecCodec, n: u64) {
    open(codec, n);
    let out = text(codec, AUTHENTICATED);
    assert_eq!(out.effects.len(), 3, "{out:?}");
    for (at, channel) in CHANNELS.iter().enumerate() {
        let out = text(codec, &format!(r#"{{"subscribed":"{channel}"}}"#));
        let want = if at == 2 {
            vec![authenticated()]
        } else {
            vec![]
        };
        assert_eq!(out.events, want, "{channel}");
    }
}

/// The arm, as the codec sends it on `rpc`: one frame naming the request with the consumer's
/// deadline, Safety traffic, charged the rate page's 0.1 under `Control`.
fn arm_sent(rpc: u64) -> Vec<Effect> {
    vec![Effect::Send {
        stream: TRADE,
        frame: fbc_core::WireSlice::plain(ARM.as_bytes().to_vec()),
        rpc: Some(RpcCall {
            id: RpcId(rpc),
            timeout: RPC_TIMEOUT,
        }),
        class: TrafficClass::Safety,
        charge: RateCharge::one(OpKind::Control, None),
    }]
}

const ARM_ON: VenueCommand = VenueCommand::ArmCancelOnDisconnect(true);
const DISARM: VenueCommand = VenueCommand::ArmCancelOnDisconnect(false);

fn refused(why: NotSentReason) -> Out {
    Out {
        result: Err(format!("{why:?}")),
        ..Out::quiet()
    }
}

fn sent(effects: Vec<Effect>) -> Out {
    Out {
        effects,
        ..Out::quiet()
    }
}

fn unknown(rpc: u64) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item: None,
        outcome: SubmitOutcome::Unknown,
    }
}

// ----- Specs, ids and commands -----

fn spec() -> InstrumentSpec {
    let venue_symbol =
        dispatch_market_data(&caps(), |scope| scope.venue_symbol("AAPL-USD")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id: AAPL,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(AAPL.get()),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.01".parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.01".parse().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: rust_decimal::Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        // Every v1 shape listed: LIMIT and ALO, GTC and IOC (decision 0136).
        order_shapes: Some(OrderShapes {
            kinds: TagSet::of(&[OrderKindTag::Limit]),
            post_only: true,
            tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
        }),
        version: 1,
        fetched_at: WallNs(0),
    }
}

fn specs() -> SpecTable {
    let mut specs = SpecTable::new();
    specs.insert(spec());
    specs
}

/// Client ids minted under a held namespace lease, once per test binary.
fn cids() -> &'static [ClientOrderId] {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-qfex-probe-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_760_000_000_000_000_000));
        let ids = (0..4).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = fs::remove_dir_all(&dir);
        ids
    })
}

fn venue_id() -> VenueOrderId {
    dispatch_market_data(&caps(), |scope| scope.venue_order_id(ORDER)).unwrap()
}

/// A limit order of the given shape on `AAPL-USD`.
fn order(kind: OrderKind, tif: Tif, channel: Channel, post_only: bool, reduce: bool) -> NewOrder {
    NewOrder {
        cid: cids()[0],
        inst: AAPL,
        side: Side::Buy,
        qty: Lots::new(100).unwrap(),
        kind,
        tif,
        channel,
        post_only,
        reduce_only: reduce,
        reducing: false,
    }
}

fn limit() -> OrderKind {
    OrderKind::Limit { px: Ticks(20_050) }
}

fn cancel(target: OrderRef) -> CancelOrder {
    CancelOrder {
        target,
        inst: AAPL,
        side: Side::Buy,
        placement_nonce: None,
    }
}

fn query(target: OrderRef) -> VenueCommand {
    VenueCommand::Query(QueryOrder {
        target,
        inst: AAPL,
        placement_nonce: None,
    })
}

// ----- The mode and its guard -----

/// Every factory method that plans or builds something, and the probe's own connections,
/// refuse `cfg` with `want`: nothing is planned.
fn refused_everywhere(cfg: &VenueConfig, want: ConfigError, what: &str) {
    assert_eq!(probe::mode(cfg), Err(want), "{what}");
    assert_eq!(Qfex.caps(cfg).err(), Some(want), "{what}");
    assert_eq!(Qfex.exec_account_key(cfg), Err(want), "{what}");
    assert_eq!(Qfex.plan_exec(cfg), Err(VenueError::Config(want)), "{what}");
    assert!(
        matches!(Qfex.exec_codec(cfg, secrets()), Some(Err(VenueError::Config(e))) if e == want),
        "{what}"
    );
    assert!(
        matches!(Qfex.discover(cfg), Err(VenueError::Config(e)) if e == want),
        "{what}"
    );
    let subs = Default::default();
    assert_eq!(
        Qfex.plan_md(cfg, &specs(), &subs),
        Err(VenueError::Config(want)),
        "{what}"
    );
    assert_eq!(probe::connections(cfg).err(), Some(want), "{what}");
}

/// Every factory method admits `cfg`, on the side of the guard `side`.
fn admitted(cfg: &VenueConfig, side: Origins, what: &str) {
    let probe = probe::mode(cfg).unwrap_or_else(|e| panic!("{what}: {e}"));
    let probe = probe.unwrap_or_else(|| panic!("{what}: no probe"));
    assert_eq!(probe.origins(), side, "{what}");
    assert!(Qfex.caps(cfg).unwrap().exec.is_some(), "{what}");
    assert!(Qfex.exec_account_key(cfg).unwrap().is_some(), "{what}");
    assert_eq!(Qfex.plan_exec(cfg).unwrap().len(), 1, "{what}");
    assert!(
        matches!(Qfex.exec_codec(cfg, secrets()), Some(Ok(_))),
        "{what}"
    );
    assert!(Qfex.discover(cfg).is_ok(), "{what}");
    assert!(
        Qfex.plan_md(cfg, &specs(), &Default::default()).is_ok(),
        "{what}"
    );
    assert_eq!(probe::connections(cfg).unwrap().origins, side, "{what}");
}

#[test]
fn the_guard_admits_exactly_the_three_preproduction_origins_and_an_all_loopback_set() {
    for account in ["primary", SUB] {
        admitted(&preprod(account), Origins::PreProduction, "pre-production");
        admitted(&loopback(account), Origins::Loopback, "loopback");
    }
    // A trailing `/` is ignored, on each origin or all of them.
    let slashed = |key: &str, cfg: VenueConfig| {
        let value = format!("{}/", cfg.get(key).unwrap());
        with(cfg, key, &value)
    };
    for key in [auth::TRADE_URL, KEY_REST_URL, KEY_MDS_URL] {
        admitted(&slashed(key, preprod(SUB)), Origins::PreProduction, key);
        admitted(&slashed(key, loopback(SUB)), Origins::Loopback, key);
    }
    let all = slashed(
        auth::TRADE_URL,
        slashed(KEY_REST_URL, slashed(KEY_MDS_URL, preprod(SUB))),
    );
    admitted(&all, Origins::PreProduction, "every origin slashed");
    // Loopback is any 127.0.0.0/8 address or [::1], with or without a port, under either
    // scheme each origin takes.
    for (trade, rest, mds) in [
        ("wss://127.0.0.1", "https://127.0.0.1", "wss://127.0.0.1"),
        (
            "ws://127.9.8.7:1",
            "http://127.255.255.254:65535",
            "ws://127.0.0.2:80",
        ),
        ("ws://[::1]:9001", "http://[::1]:9002", "wss://[::1]"),
    ] {
        admitted(&on(SUB, trade, rest, mds), Origins::Loopback, trade);
    }

    // The probe's own connections: the guarded origins without the `/`, the trade socket as an
    // order-entry session opens it, and the account key 0122 binds.
    let connections = |cfg: &VenueConfig| {
        let c = probe::connections(cfg).unwrap();
        (
            c.trade.as_str().to_owned(),
            c.trade.redactions().to_vec(),
            c.rest.to_owned(),
            c.mds.to_owned(),
            c.account,
        )
    };
    let url = format!("{PREPROD_TRADE}/?api_key={PUBLIC}");
    let key_at = url.find(PUBLIC).unwrap() as u32;
    let key_span = key_at..key_at + PUBLIC.len() as u32;
    assert_eq!(
        connections(&all),
        (
            url,
            std::slice::from_ref(&key_span).to_vec(),
            PREPROD_REST.to_owned(),
            PREPROD_MDS.to_owned(),
            AccountKey::new(9)
        )
    );
    let local = connections(&loopback("primary"));
    assert_eq!(local.0, format!("ws://127.0.0.1:9001/?api_key={PUBLIC}"));
    assert_eq!(
        (local.2.as_str(), local.3.as_str(), local.4),
        (
            "http://127.0.0.1:9002",
            "ws://127.0.0.1:9003",
            AccountKey::new(4)
        )
    );
}

#[test]
fn the_guard_refuses_each_other_configuration_before_anything_is_planned() {
    let invalid = |key, reason| ConfigError::Invalid { key, reason };
    let mixed = "mixes a loopback origin with a pre-production one";
    let production = "a production qfex.com host";
    let other = "neither its pre-production origin nor a loopback origin";
    let mut cases: Vec<(VenueConfig, ConfigError, String)> = Vec::new();
    let mut case = |cfg, want, what: &str| cases.push((cfg, want, what.to_owned()));

    // A mixed set, whichever origin differs.
    let local = loopback(SUB);
    for key in [KEY_REST_URL, KEY_MDS_URL] {
        let cfg = with(preprod(SUB), key, local.get(key).unwrap());
        case(
            cfg,
            invalid(key, mixed),
            "a loopback origin among pre-production ones",
        );
    }
    let cfg = with(loopback(SUB), KEY_MDS_URL, PREPROD_MDS);
    case(
        cfg,
        invalid(KEY_MDS_URL, mixed),
        "a pre-production origin among loopback ones",
    );
    let cfg = with(loopback(SUB), auth::TRADE_URL, PREPROD_TRADE);
    case(
        cfg,
        invalid(KEY_REST_URL, mixed),
        "the trade origin alone pre-production",
    );

    // Any qfex.com host: production's own origins, alone or all three.
    for (key, value) in [
        (auth::TRADE_URL, "wss://trade.qfex.com"),
        (auth::TRADE_URL, "wss://TRADE.QFEX.COM"),
        (auth::TRADE_URL, "wss://qfex.com"),
        (KEY_REST_URL, "https://api.qfex.com"),
        (KEY_REST_URL, "https://api.qfex.com:443/"),
        (KEY_MDS_URL, "wss://mds.qfex.com"),
        (KEY_MDS_URL, "ws://mds.qfex.com:8080"),
    ] {
        case(
            with(preprod(SUB), key, value),
            invalid(key, production),
            value,
        );
    }
    let prod = on(
        SUB,
        "wss://trade.qfex.com",
        "https://api.qfex.com",
        "wss://mds.qfex.com",
    );
    case(
        prod,
        invalid(auth::TRADE_URL, production),
        "every production origin",
    );

    // Any other origin: a pre-production host written otherwise, another key's origin, another
    // host, a host name for loopback, and an address that is not loopback.
    for (key, value) in [
        (auth::TRADE_URL, "wss://trade.qfex.io:443"),
        (auth::TRADE_URL, "wss://trade.qfex.io/v1"),
        (auth::TRADE_URL, "wss://trade.qfex.io//"),
        (auth::TRADE_URL, "ws://trade.qfex.io"),
        (auth::TRADE_URL, "WSS://trade.qfex.io"),
        (auth::TRADE_URL, "wss://TRADE.qfex.io"),
        (auth::TRADE_URL, "wss://user@trade.qfex.io"),
        (auth::TRADE_URL, "wss://trade.qfex.io.example.org"),
        (auth::TRADE_URL, PREPROD_MDS),
        (auth::TRADE_URL, "https://api.qfex.io"),
        (auth::TRADE_URL, "wss://example.org"),
        (auth::TRADE_URL, " wss://trade.qfex.io"),
        (KEY_REST_URL, "http://api.qfex.io"),
        (KEY_REST_URL, "https://api.qfex.io/refdata"),
        (KEY_REST_URL, "wss://api.qfex.io"),
        (KEY_MDS_URL, "wss://mds.qfex.io?x=1"),
        (KEY_MDS_URL, PREPROD_TRADE),
    ] {
        case(with(preprod(SUB), key, value), invalid(key, other), value);
    }
    for (key, value) in [
        (auth::TRADE_URL, "ws://localhost:9001"),
        (auth::TRADE_URL, "ws://10.0.0.1:9001"),
        (auth::TRADE_URL, "ws://[::2]:9001"),
        (auth::TRADE_URL, "ws://[::ffff:127.0.0.1]:9001"),
        (auth::TRADE_URL, "ws://127.0.0.1:0"),
        (auth::TRADE_URL, "ws://127.1:9001"),
        (KEY_REST_URL, "http://0.0.0.0:9002"),
        (KEY_REST_URL, "ftp://127.0.0.1:9002"),
        (KEY_MDS_URL, "http://127.0.0.1:9003"),
        (KEY_MDS_URL, "ws://127.0.0.1:9003/path"),
    ] {
        case(with(loopback(SUB), key, value), invalid(key, other), value);
    }

    // An unknown mode.
    for mode in ["probe", "PREPROD_PROBE", "preprod_probe ", "", "production"] {
        let want = invalid(KEY_EXEC_MODE, "expected preprod_probe, or no value");
        case(with(preprod(SUB), KEY_EXEC_MODE, mode), want, mode);
    }

    // A missing origin or setting, none with a default.
    for key in [
        auth::TRADE_URL,
        KEY_REST_URL,
        KEY_MDS_URL,
        KEY_PAGE_SIZE,
        KEY_RESYNC_TIMEOUT,
        KEY_RPC_TIMEOUT,
    ] {
        case(without(SUB, key), ConfigError::Missing(key), key);
    }

    // A page size outside 1 to 1000, or not written as it prints.
    for size in ["0", "1001", "65536", "-1", "+5", "05", "1e3", "", "10 "] {
        let want = invalid(KEY_PAGE_SIZE, "expected a whole number from 1 to 1000");
        case(with(preprod(SUB), KEY_PAGE_SIZE, size), want, size);
    }
    // A deadline that is not a positive whole <n>ms or <n>s.
    for key in [KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT] {
        for value in ["0s", "0ms", "1.5s", "-1s", "+1s", "1m", "30", "s", "ms", ""] {
            let want = invalid(key, "expected positive whole <n>ms or <n>s");
            case(with(preprod(SUB), key, value), want, value);
        }
    }
    // A request deadline not longer than the speed bump.
    for value in ["500ms", "499ms", "1ms"] {
        let want = invalid(
            KEY_RPC_TIMEOUT,
            "must be longer than QFEX's 500 ms speed bump",
        );
        case(with(preprod(SUB), KEY_RPC_TIMEOUT, value), want, value);
    }

    for (cfg, want, what) in &cases {
        refused_everywhere(cfg, *want, what);
    }
    assert!(cases.len() > 80, "{} cases", cases.len());

    // The bounds themselves pass, and the deadlines read as written.
    for (size, resync, rpc) in [("1", "1ms", "501ms"), ("1000", "120s", "3s")] {
        let cfg = with(
            with(
                with(preprod(SUB), KEY_PAGE_SIZE, size),
                KEY_RESYNC_TIMEOUT,
                resync,
            ),
            KEY_RPC_TIMEOUT,
            rpc,
        );
        admitted(&cfg, Origins::PreProduction, size);
    }
    let probe = settings(&preprod(SUB));
    assert_eq!(
        (
            probe.page_size(),
            probe.resync_timeout(),
            probe.rpc_timeout()
        ),
        (1000, Duration::from_secs(30), RPC_TIMEOUT)
    );
    assert!(probe.rpc_timeout() > SPEED_BUMP);

    // The probe's own connections apply decision 0122 item 1's account check too, and need the
    // mode: refusals that name the setting, never its value.
    for (key, value) in [
        (auth::ACCOUNT_ID, "Primary"),
        (auth::ACCOUNT_ID, "5A5A5A5A-0000-4000-8000-00000000C0DE"),
        (auth::ACCOUNT_KEY, "09"),
    ] {
        let err = probe::connections(&with(preprod(SUB), key, value)).unwrap_err();
        assert!(
            matches!(err, ConfigError::Invalid { key: k, .. } if k == key),
            "{err}"
        );
        assert!(!err.to_string().contains(value), "{err}");
    }
    for key in [auth::ACCOUNT_ID, auth::ACCOUNT_KEY] {
        assert_eq!(
            probe::connections(&without(SUB, key)),
            Err(ConfigError::Missing(key))
        );
    }
    let mut md = without(SUB, KEY_EXEC_MODE);
    assert_eq!(
        probe::connections(&md),
        Err(ConfigError::Missing(KEY_EXEC_MODE))
    );
    // And no refusal names a value it refused.
    md.insert(auth::TRADE_URL, "wss://secret-host.example.org");
    md.insert(KEY_EXEC_MODE, PREPROD_PROBE);
    let err = Qfex.caps(&md).unwrap_err().to_string();
    assert!(!err.contains("secret-host"), "{err}");
}

#[test]
fn an_absent_mode_gives_market_data_only_and_no_codec() {
    // Production's origins, probe settings the mode would refuse, and no mode: as before the
    // mode existed, nothing of the probe is read.
    let mut cfg = without(SUB, KEY_EXEC_MODE);
    cfg.insert(auth::TRADE_URL, "wss://trade.qfex.com");
    cfg.insert(KEY_REST_URL, "https://api.qfex.com");
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert(KEY_PAGE_SIZE, "0");
    cfg.insert(KEY_RPC_TIMEOUT, "1ms");
    assert_eq!(probe::mode(&cfg), Ok(None));
    let caps = Qfex.caps(&cfg).unwrap();
    assert_eq!(caps.exec, None);
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    assert_eq!(Qfex.plan_exec(&cfg), Ok(Vec::new()));
    assert!(Qfex.exec_codec(&cfg, secrets()).is_none());
    assert_eq!(Qfex.exec_account_key(&cfg), Ok(Some(AccountKey::new(9))));
    // A market-data configuration alone, with no account, is unchanged too.
    let mut md = VenueConfig::new();
    md.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    assert_eq!(Qfex.caps(&md).unwrap().exec, None);
    // The schema lists the mode and what it reads.
    for key in [
        KEY_EXEC_MODE,
        KEY_PAGE_SIZE,
        KEY_RESYNC_TIMEOUT,
        KEY_RPC_TIMEOUT,
        auth::TRADE_URL,
        auth::PUBLIC_KEY,
        auth::SECRET_KEY,
    ] {
        assert!(
            Qfex.config_schema().iter().any(|field| field.key == key),
            "{key}"
        );
    }
}

// ----- What the mode declares and plans -----

#[test]
fn under_the_mode_the_factory_declares_item_6s_exec_caps_and_one_order_entry_endpoint() {
    for cfg in [preprod("primary"), loopback(SUB)] {
        let caps = Qfex.caps(&cfg).unwrap();
        let exec = caps.exec.clone().expect("the probe declares order entry");
        assert_eq!(exec, probe_exec_caps());
        // Everything else is market data's, readiness `Record` included.
        let mut md = VenueConfig::new();
        md.insert(KEY_MDS_URL, "wss://mds.qfex.com");
        let market_data = Qfex.caps(&md).unwrap();
        assert_eq!(
            VenueCaps {
                exec: None,
                ..caps.clone()
            },
            market_data
        );
        assert_eq!(caps.readiness_ceiling, Readiness::Record);

        // Item 6's list.
        let o = &exec.order;
        assert_eq!(o.cancel_on_disconnect, CancelOnDisconnect::AccountWide);
        assert_eq!(o.snapshot_source, SnapshotSource::Untrustworthy);
        assert_eq!(o.snapshot_reads, SnapshotReads::Separate);
        assert_eq!(o.amend, None);
        assert_eq!(o.cancel_refs, TagSet::of(&[RefKind::Venue]));
        assert_eq!(o.query_refs, TagSet::of(&[RefKind::Venue]));
        assert_eq!((o.batch_place, o.batch_cancel), (None, None));
        assert_eq!(o.cancel_all_account, Support::Unsupported);
        assert_eq!(o.cancel_all_instrument, Support::Unsupported);
        assert_eq!(o.kinds, TagSet::of(&[OrderKindTag::Limit]));
        assert_eq!(o.tifs, TagSet::of(&[TifTag::Gtc, TifTag::Ioc]));
        assert_eq!(o.channels, TagSet::of(&[Channel::Public]));
        assert!(o.post_only && !o.reduce_only);
        assert_eq!(o.flag_conflicts, vec![(Feature::PostOnly, Feature::Ioc)]);
        assert!(!o.cid_echoed_on_events);
        assert_eq!(o.client_id, fbc_venue_qfex::exec::CLIENT_ID_FORMAT);
        let AckModel::TwoPhase { risk_reject_window } = o.ack else {
            panic!("{:?}", o.ack);
        };
        assert!(risk_reject_window > SPEED_BUMP, "{risk_reject_window:?}");
        // And what the pages leave silent, declared conservatively (0054).
        assert_eq!(o.ordering_key, OrderingKey::None);
        assert!(!o.cancel_before_ack && !o.events_echo_flags && !o.cancel_is_signed);
        let f = &exec.fills;
        assert!(f.fill_id && !f.replays_fills_on_reconnect);
        assert!(!f.liquidity_flag && !f.realized_pnl && !f.realized_funding);
        assert!(!f.fee_asset_reported);

        // One order-entry endpoint: the trade socket, its key a redaction span, no handshake
        // header (it authenticates in its frames).
        let plan = Qfex.plan_exec(&cfg).unwrap();
        let [endpoint] = plan.as_slice() else {
            panic!("{plan:?}");
        };
        assert_eq!(endpoint.stream, TRADE);
        assert_eq!(endpoint.url, probe::connections(&cfg).unwrap().trade);
        assert!(
            endpoint
                .url
                .as_str()
                .ends_with(&format!("/?api_key={PUBLIC}"))
        );
        assert_eq!(endpoint.url.redactions().len(), 1);
        assert!(endpoint.handshake.is_empty());
    }

    // Each value cites its page: every field of the probe's OrderCaps and FillCaps is
    // declared under a comment of its own that names its source, a page, the AsyncAPI, a
    // decision or a ticket (0054).
    let source =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec/caps.rs")).unwrap();
    let body = &source[source.find("pub fn probe_exec_caps").unwrap()..];
    let lines: Vec<&str> = body.lines().collect();
    let mut fields = 0;
    for (at, line) in lines.iter().enumerate() {
        let field = line.len() - line.trim_start().len() == 12
            && line.trim_start().split(':').next().is_some_and(|name| {
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            });
        if field {
            fields += 1;
            let comment: String = lines[..at]
                .iter()
                .rev()
                .take_while(|above| above.trim_start().starts_with("//"))
                .map(|above| above.to_ascii_lowercase())
                .collect();
            let sources = ["page", "asyncapi", "decision", "fbc-", "https://"];
            assert!(
                sources.iter().any(|source| comment.contains(source)),
                "no citation above {line}"
            );
        }
    }
    // OrderCaps' 26 fields and FillCaps' 8.
    assert_eq!(fields, 34);
}

/// Decision 0003's `caps_truthful` rule on the probe: every command the caps leave out is
/// refused unsent for the reason they give, and each one they declare is sent.
#[test]
fn the_probe_caps_declare_exactly_what_the_codec_sends() {
    let caps = probe_exec_caps();
    let o = &caps.order;
    let mut codec = probe_codec(SUB);
    authenticate(&mut codec, 1);
    let mut rpc = 10;
    let mut send = |codec: &mut QfexExec, cmd: &VenueCommand| {
        rpc += 1;
        encode(codec, cmd, rpc)
    };
    let both = OrderRef::Both(cids()[1], venue_id());
    let place = |kind, tif, channel, post_only, reduce| {
        VenueCommand::Place(order(kind, tif, channel, post_only, reduce))
    };
    // The controls: each shape the caps allow, a cancel and a query naming every reference.
    for (tif, post_only) in [(Tif::Gtc, false), (Tif::Gtc, true), (Tif::Ioc, false)] {
        let cmd = place(limit(), tif, Channel::Public, post_only, false);
        let out = send(&mut codec, &cmd);
        assert!(
            out.result.is_ok() && out.effects.len() == 1,
            "{cmd:?}: {out:?}"
        );
    }
    for cmd in [
        VenueCommand::Cancel(cancel(both.clone())),
        query(both.clone()),
    ] {
        let out = send(&mut codec, &cmd);
        assert!(
            out.result.is_ok() && out.effects.len() == 1,
            "{cmd:?}: {out:?}"
        );
    }
    // The absences, each refused for what the caps make it, with nothing written.
    assert!(!o.kinds.contains(OrderKindTag::Market) && !o.tifs.contains(TifTag::Fok));
    assert!(!o.channels.contains(Channel::Rpi) && !o.reduce_only);
    let unsupported = NotSentReason::Unsupported;
    let cases = [
        (
            place(OrderKind::Market, Tif::Ioc, Channel::Public, false, false),
            unsupported,
        ),
        (
            place(limit(), Tif::Fok, Channel::Public, false, false),
            unsupported,
        ),
        (
            place(limit(), Tif::Gtc, Channel::Rpi, false, false),
            unsupported,
        ),
        (
            place(limit(), Tif::Gtc, Channel::Public, false, true),
            unsupported,
        ),
        (
            place(limit(), Tif::Ioc, Channel::Public, true, false),
            NotSentReason::FlagConflict,
        ),
        (
            VenueCommand::PlaceBatch(vec![order(
                limit(),
                Tif::Gtc,
                Channel::Public,
                false,
                false,
            )]),
            unsupported,
        ),
        (
            VenueCommand::Cancel(cancel(OrderRef::Client(cids()[1]))),
            unsupported,
        ),
        (
            VenueCommand::CancelMany(vec![cancel(both.clone())]),
            unsupported,
        ),
        (query(OrderRef::Client(cids()[1])), unsupported),
        (VenueCommand::CancelAll(CancelScope::Account), unsupported),
        (
            VenueCommand::CancelAll(CancelScope::Instrument(AAPL)),
            unsupported,
        ),
        (VenueCommand::RefreshDeadMan, unsupported),
    ];
    assert_eq!(o.amend, None);
    for (cmd, why) in cases {
        assert_eq!(send(&mut codec, &cmd), refused(why), "{cmd:?}");
    }
    // The flag conflict the caps declare is the one the encoder refuses as such.
    assert_eq!(o.flag_conflicts, [(Feature::PostOnly, Feature::Ioc)]);
    // And the protection they declare: the arm sent once, every disarm refused (item 1).
    assert_eq!(send(&mut codec, &ARM_ON).effects.len(), 1);
    assert_eq!(send(&mut codec, &DISARM), refused(unsupported));
}

// ----- The arm -----

#[test]
fn the_codec_writes_one_arm_per_epoch_refuses_a_second_and_a_disarm_and_leaves_it_unanswered() {
    let mut codec = probe_codec(SUB);
    // Before the stream is authenticated: not sent, as every command.
    open(&mut codec, 1);
    assert_eq!(
        encode(&mut codec, &ARM_ON, 1),
        refused(NotSentReason::Disconnected)
    );
    assert_eq!(
        encode(&mut codec, &DISARM, 2),
        refused(NotSentReason::Unsupported)
    );
    authenticate(&mut codec, 1);

    // The one arm: the cancel-on-disconnect page's request, exactly.
    assert_eq!(
        ARM,
        r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#
    );
    assert_eq!(encode(&mut codec, &ARM_ON, 3), sent(arm_sent(3)));
    // Charged to both of the provider user's buckets, since the rate page names none for it.
    let Effect::Send { charge, .. } = &arm_sent(3)[0] else {
        unreachable!()
    };
    let limits = caps().exec_limits.unwrap();
    assert!(limits.iter().all(|limit| limit.counts(charge, Via::Frame)));
    // A second arm and a disarm, each refused with nothing written, before and after the first
    // arm's deadline.
    assert_eq!(
        encode(&mut codec, &ARM_ON, 4),
        refused(NotSentReason::Unsupported)
    );
    assert_eq!(
        encode(&mut codec, &DISARM, 5),
        refused(NotSentReason::Unsupported)
    );

    // Nothing answers it: an echo of it, an err echoing it, a cancelled order and a position
    // settle nothing.
    let arm_echoes = [
        format!(r#"{{"ack":{{"incoming_message":{ARM}}}}}"#),
        format!(
            r#"{{"err":{{"error_code":"InvalidParameter","message":"no","incoming_message":{ARM}}}}}"#
        ),
        format!(
            r#"{{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 2 seconds","incoming_message":{ARM}}}"#
        ),
        fixture("order_response.json").replace("\"ACK\"", "\"CANCELLED\""),
        fixture("position_response.json"),
    ];
    for frame in &arm_echoes {
        let out = text(&mut codec, frame);
        assert!(out.events.is_empty(), "{frame}: {out:?}");
        assert!(
            !out.effects
                .iter()
                .any(|e| matches!(e, Effect::Reconnect { .. })),
            "{frame}: {out:?}"
        );
    }
    // Unanswered at its deadline: Unknown, as every request.
    assert_eq!(
        deadline(&mut codec, 3),
        Out {
            events: vec![unknown(3)],
            ..Out::quiet()
        }
    );
    assert_eq!(
        encode(&mut codec, &ARM_ON, 6),
        refused(NotSentReason::Unsupported)
    );

    // The next epoch arms again, once.
    authenticate(&mut codec, 2);
    assert_eq!(encode(&mut codec, &ARM_ON, 7), sent(arm_sent(7)));
    assert_eq!(
        encode(&mut codec, &ARM_ON, 8),
        refused(NotSentReason::Unsupported)
    );

    // The codec the factory builds under the mode is this one.
    let mut built = Qfex.exec_codec(&preprod(SUB), secrets()).unwrap().unwrap();
    authenticate(built.as_mut(), 1);
    assert_eq!(encode(built.as_mut(), &ARM_ON, 9), sent(arm_sent(9)));
    assert_eq!(
        encode(built.as_mut(), &DISARM, 10),
        refused(NotSentReason::Unsupported)
    );

    // Outside the mode, the arm stays refused (FBC-blcw).
    let mut off = recording(SUB);
    authenticate(&mut off, 1);
    assert_eq!(
        encode(&mut off, &ARM_ON, 11),
        refused(NotSentReason::Unsupported)
    );
    assert!(off.probe_settings().is_none());
    assert!(codec.probe_settings().is_some());
}

// ----- What stage one brings -----

fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

#[test]
fn the_probe_keeps_its_epoch_open_on_private_state_and_its_resync_asks_for_nothing() {
    let frames = [
        fixture("order_response.json"),
        fixture("order_response.json").replace("\"ACK\"", "\"CANCELLED\""),
        fixture("fill_response.json"),
        fixture("all_orders_response.json"),
        fixture("position_response.json"),
        r#"{"user_trades_response":[]}"#.to_owned(),
    ];
    for frame in &frames {
        let mut probe = probe_codec(SUB);
        authenticate(&mut probe, 1);
        assert_eq!(text(&mut probe, frame), Out::quiet(), "{frame}");
        assert!(probe.last_private().is_some());
        assert_eq!(probe.counts().private_state, 1);
        // Still open: the arm goes out after it.
        assert_eq!(encode(&mut probe, &ARM_ON, 1), sent(arm_sent(1)));
        // Recording-only, the same frame ends the epoch.
        let mut off = recording(SUB);
        authenticate(&mut off, 1);
        assert!(text(&mut off, frame).reconnects(), "{frame}");
    }
    // Before the last confirmation too: a position snapshot on subscribing.
    let mut probe = probe_codec(SUB);
    open(&mut probe, 1);
    text(&mut probe, AUTHENTICATED);
    assert_eq!(
        text(&mut probe, r#"{"subscribed":"positions"}"#),
        Out::quiet()
    );
    assert_eq!(
        text(&mut probe, &fixture("position_response.json")),
        Out::quiet()
    );
    // A private frame not as documented is a decode error, its epoch kept open.
    let mut probe = probe_codec(SUB);
    authenticate(&mut probe, 1);
    let bad = r#"{"position_response":{"symbol":"AAPL-USD"}}"#;
    assert_eq!(
        text(&mut probe, bad),
        Out {
            result: Err(format!("{INVALID_STATE:?}")),
            ..Out::quiet()
        }
    );
    // One the codec does not recognise still ends it, as does a frame that is not an object.
    assert!(text(&mut probe, &fixture("balance_response.json")).reconnects());
    let mut probe = probe_codec(SUB);
    authenticate(&mut probe, 1);
    let out = text(&mut probe, "[1]");
    assert!(out.reconnects() && out.result == Err(format!("{NOT_AN_OBJECT:?}")));

    // The resync asks for nothing and pushes nothing, so the session places nothing (0058),
    // and the epoch stays open for whatever answers the arm.
    let mut probe = probe_codec(SUB);
    authenticate(&mut probe, 1);
    assert_eq!(encode(&mut probe, &ARM_ON, 1), sent(arm_sent(1)));
    let mut fx = Effects::new();
    probe.resync(&ctx(2), &mut fx);
    assert!(fx.is_empty(), "{fx:?}");
    assert_eq!(probe.nonces_for(fbc_core::CtxCall::Resync), 0);
    let ack = format!(r#"{{"ack":{{"incoming_message":{ARM}}}}}"#);
    assert_eq!(text(&mut probe, &ack), Out::quiet());
    // Recording-only, it ends the epoch.
    let mut off = recording(SUB);
    authenticate(&mut off, 1);
    off.resync(&ctx(2), &mut fx);
    assert!(matches!(fx.take().as_slice(), [Effect::Reconnect { .. }]));
}

// ----- The journal and replay -----

/// One input of a session.
#[derive(Clone)]
enum Step {
    Open(u64),
    Frame(String),
    Encode(VenueCommand, u64),
    Deadline(u64),
}

/// Runs `steps` through `codec`, handing each frame to `journal` first, as the runtime does,
/// and reading the frame `journal` gives back: the live frame to record it, the journaled one
/// in replay.
fn run(
    codec: &mut QfexExec,
    steps: &[Step],
    mut journal: impl FnMut(&QfexExec, &str) -> String,
) -> Vec<Out> {
    steps
        .iter()
        .map(|step| match step {
            Step::Open(n) => Out {
                effects: open(codec, *n),
                ..Out::quiet()
            },
            Step::Frame(frame) => {
                let frame = journal(codec, frame);
                text(codec, &frame)
            }
            Step::Encode(cmd, rpc) => encode(codec, cmd, *rpc),
            Step::Deadline(rpc) => deadline(codec, *rpc),
        })
        .collect()
}

fn stamp(seq: u64) -> Stamp {
    Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: MonoNs(seq),
        recv_wall: WallNs(1_760_000_000_000_000_000),
        conn: ConnKey { conn: 0, epoch: 1 },
    }
}

/// Every file under `dir`, read whole.
fn files(dir: &Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            out.push(fs::read(&path).unwrap());
        }
    }
    out
}

fn twos(n: usize) -> String {
    "2".repeat(n)
}

fn nested(code: &str, message: &str, echo: &str) -> String {
    format!(r#"{{"err":{{"error_code":"{code}","message":{message},"incoming_message":{echo}}}}}"#)
}

fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":{message},"incoming_message":{echo}}}"#
    )
}

fn ack(echo: &str) -> String {
    format!(r#"{{"ack":{{"incoming_message":{echo}}}}}"#)
}

/// An `add_order` echo in the request's `{type, params}` form, as the errors page shows one.
const ADD_ORDER: &str = r#"{"type":"add_order","params":{"symbol":"AAPL-USD","side":"BUY","order_type":"LIMIT","order_time_in_force":"GTC","quantity":1,"price":200.5,"client_order_id":"echo-cid-0001"}}"#;

/// The add_order echo as the journal keeps it: each params value a span of its own.
fn add_order_kept() -> String {
    format!(
        r#"{{"type":"add_order","params":{{"symbol":"{}","side":"222","order_type":"22222","order_time_in_force":"222","quantity":2,"price":22222,"client_order_id":"{}"}}}}"#,
        twos(8),
        twos(13)
    )
}

#[test]
fn the_journal_keeps_the_arms_echo_and_the_window_and_replay_gives_the_same_events_and_effects() {
    let frame = |s: &str| Step::Frame(s.to_owned());
    let reordered =
        r#"{ "params" : { "cancel_on_disconnect" : true } , "type" : "cancel_on_disconnect" }"#;
    let hint_s = r#""Rate limit exceeded, retry after 3 seconds""#;
    let hint_ms = r#""Too many requests. Please retry after 200ms.""#;
    // The arm's own echo (0132 item 3, 0134 item 7).
    let arm_ack = ack(ARM);
    let arm_ack_reordered = ack(reordered);
    let arm_rejected = nested("InvalidParameter", r#""cannot arm""#, ARM);
    let arm_limited_s = flat("RateLimited", hint_s, ARM);
    let arm_limited_ms = nested("RateLimited", hint_ms, reordered);
    let arm_limited_text = nested("RateLimited", r#""slow down""#, ARM);
    let arm_null = flat("RateLimited", "null", ARM);
    // Other echoes in the arm's window, the arm the only request in flight.
    let request_err = nested("InvalidOrder", r#""bad price""#, ADD_ORDER);
    let query_limited = nested(
        "RateLimited",
        hint_s,
        r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#,
    );
    let arm_plus =
        ack(r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true,"x":"y"}}"#);
    let string_echo = flat("InvalidParameter", r#""no""#, r#""cancel_on_disconnect""#);
    let escaped = nested("InvalidOrder", r#""a \"quoted\" reason""#, "{}");
    let undocumented_member = r#"{"err":{"error_code":"ServerError","message":"m","incoming_message":null,"request_id":17},"seq":3}"#;
    // The arm's echo with a member added in its body or beside its envelope: not the frame's
    // only content (0132 item 3), so any other echo, its undocumented values spans.
    let arm_note = format!(r#"{{"ack":{{"incoming_message":{ARM},"note":"arm-note-value"}}}}"#);
    let arm_seq = format!(
        r#"{{"err":{{"error_code":"InvalidParameter","message":"arm-seq-message","incoming_message":{ARM}}},"seq":3}}"#
    );
    let arm_ack_seq = format!(r#"{{"ack":{{"incoming_message":{ARM}}},"seq":3}}"#);
    let arm_flat_extra = format!(
        r#"{{"type":"Err","error_code":"RateLimited","message":{hint_s},"incoming_message":{ARM},"extra":"arm-extra-value"}}"#
    );
    // Escaped values in the window: judged on their bytes, so each is a span (0134 item 3).
    let escaped_hint = r#""Rate limit exceeded, retry after 3 second\u0073""#;
    let arm_escaped_hint = nested("RateLimited", escaped_hint, ARM);
    let query_escaped_hint = nested(
        "RateLimited",
        escaped_hint,
        r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#,
    );
    let escaped_type = nested(
        "RateLimited",
        hint_s,
        r#"{"type":"get\u005fuser_orders","params":{}}"#,
    );
    // An escaped frame once another request is in flight: kept whole (0134 item 2).
    let cancel_cmd = VenueCommand::Cancel(cancel(OrderRef::Venue(venue_id())));

    let mut steps = vec![Step::Open(1), frame(AUTHENTICATED)];
    steps.extend(CHANNELS.map(|c| frame(&format!(r#"{{"subscribed":"{c}"}}"#))));
    steps.push(Step::Encode(ARM_ON.clone(), 1));
    for f in [
        &arm_ack,
        &arm_ack_reordered,
        &arm_rejected,
        &arm_limited_s,
        &arm_limited_ms,
        &arm_limited_text,
        &arm_null,
        &request_err,
        &query_limited,
        &arm_plus,
        &string_echo,
        &escaped,
        &undocumented_member.to_owned(),
        &arm_note,
        &arm_seq,
        &arm_ack_seq,
        &arm_flat_extra,
        &arm_escaped_hint,
        &query_escaped_hint,
        &escaped_type,
    ] {
        steps.push(frame(f));
    }
    // A second request: the window closes, the arm's own echo is still kept.
    steps.push(Step::Encode(cancel_cmd.clone(), 2));
    steps.push(frame(&arm_ack));
    steps.push(frame(&request_err));
    for f in [
        &arm_note,
        &arm_seq,
        &arm_ack_seq,
        &arm_flat_extra,
        &arm_escaped_hint,
    ] {
        steps.push(frame(f));
    }
    steps.push(frame(&escaped));
    // Epoch 2: the arm past its deadline is in flight no more; its echo is still its own.
    steps.extend([Step::Open(2), frame(AUTHENTICATED)]);
    steps.extend(CHANNELS.map(|c| frame(&format!(r#"{{"subscribed":"{c}"}}"#))));
    steps.extend([
        Step::Encode(ARM_ON.clone(), 3),
        Step::Deadline(3),
        Step::Deadline(1),
        frame(&arm_ack),
        frame(&arm_plus),
        frame(&escaped),
    ]);
    // Epoch 3, not armed yet: the arm's echo is any echo (0126, 0134).
    steps.extend([Step::Open(3), frame(AUTHENTICATED)]);
    steps.extend(CHANNELS.map(|c| frame(&format!(r#"{{"subscribed":"{c}"}}"#))));
    steps.push(frame(&arm_ack));
    steps.push(frame(&arm_limited_s));
    // Epochs each ended by an echo in the window: an undocumented code on the arm's echo, an
    // echo of authentication, a frame in no envelope, a repeated member, a malformed frame.
    let ending = [
        nested("ArmNotAllowedHere", "null", ARM),
        nested("InvalidParameter", "null", r#"{"type":"auth","params":{}}"#),
        format!(
            r#"{{"notice":{{"incoming_message":{{"type":"get_order","params":{{"order_id":"{ORDER}"}}}}}},"seq":4}}"#
        ),
        r#"{"err":{"error_code":"InvalidOrder","error_code":"InvalidOrder","incoming_message":{"type":"add_order","x":1}}}"#.to_owned(),
        r#"{"err":{"error_code":"InvalidOrder","incoming_message":{"#.to_owned(),
        nested(r"Rate\u004cimited", hint_s, ADD_ORDER),
    ];
    for (n, last) in ending.iter().enumerate() {
        steps.extend([Step::Open(4 + n as u64), frame(AUTHENTICATED)]);
        steps.extend(CHANNELS.map(|c| frame(&format!(r#"{{"subscribed":"{c}"}}"#))));
        steps.push(Step::Encode(ARM_ON.clone(), 10 + n as u64));
        steps.push(frame(last));
    }

    // Live, journaling every frame as the runtime would, with the codec's own spans.
    let root = std::env::temp_dir().join(format!("fbc-qfex-mfey-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let key = Arc::new(RedactionKey::new(&[9; 32]).unwrap());
    let mut writer = JournalWriter::create(&root, 0, key).unwrap();
    let mut journaled: Vec<String> = Vec::new();
    let mut live = probe_codec(SUB);
    let live_outs = run(&mut live, &steps, |codec, frame| {
        let raw = RawFrame::Text(frame);
        let spans = codec.redact_inbound(Inbound::Frame(raw));
        check_redactions(frame.as_bytes(), spans.body()).unwrap();
        let record = Record::inbound_redacted(stamp(journaled.len() as u64), raw, &spans).unwrap();
        writer
            .append(WallNs(1_760_000_000_000_000_000), &record)
            .unwrap();
        journaled.push(frame.to_owned());
        frame.to_owned()
    });
    writer.flush().unwrap();
    drop(writer);

    // On disk: the arm in clear, and no account material or other echoed value.
    let disk: Vec<u8> = files(&root).concat();
    let has = |needle: &str| disk.windows(needle.len()).any(|w| w == needle.as_bytes());
    assert!(has(ARM) && has("Rate limit exceeded, retry after 3 seconds"));
    for hidden in [
        PUBLIC,
        SUB,
        "echo-cid-0001",
        "AAPL-USD",
        "cannot arm",
        "slow down",
        ORDER,
        "arm-note-value",
        "arm-seq-message",
        "arm-extra-value",
        r"second\u0073",
        r"get\u005f",
        r"Rate\u004c",
    ] {
        assert!(!has(hidden), "{hidden} reached the journal");
    }

    let back: Vec<String> = JournalReader::open(&root, 0)
        .unwrap()
        .map(|record| match record.unwrap() {
            Record::Inbound { bytes, .. } => String::from_utf8(bytes.0).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect();
    let _ = fs::remove_dir_all(&root);
    assert_eq!(back.len(), journaled.len());
    // The journaled form of the `nth` time `live` was framed.
    let read = |live: &str, nth: usize| {
        let at = journaled
            .iter()
            .enumerate()
            .filter(|(_, f)| *f == live)
            .nth(nth)
            .unwrap()
            .0;
        back[at].clone()
    };

    // The arm's own echo: kept, but its message, a RateLimited err's hint form kept too.
    assert_eq!(read(&arm_ack, 0), arm_ack);
    assert_eq!(read(&arm_ack_reordered, 0), arm_ack_reordered);
    assert_eq!(
        read(&arm_rejected, 0),
        nested("InvalidParameter", &format!("\"{}\"", twos(10)), ARM)
    );
    assert_eq!(read(&arm_limited_s, 0), arm_limited_s);
    assert_eq!(read(&arm_limited_ms, 0), arm_limited_ms);
    assert_eq!(
        read(&arm_limited_text, 0),
        nested("RateLimited", &format!("\"{}\"", twos(9)), ARM)
    );
    assert_eq!(read(&arm_null, 0), flat("RateLimited", "2222", ARM));
    // Another echo in the window: its shape, with a documented code, a mapped echo type and a
    // RateLimited err's hint form in clear, every other value a span of its own.
    assert_eq!(
        read(&request_err, 0),
        nested(
            "InvalidOrder",
            &format!("\"{}\"", twos(9)),
            &add_order_kept()
        )
    );
    assert_eq!(
        read(&query_limited, 0),
        nested(
            "RateLimited",
            hint_s,
            r#"{"type":"get_user_orders","params":{"limit":2222,"offset":2}}"#
        )
    );
    assert_eq!(
        read(&arm_plus, 0),
        ack(&format!(
            r#"{{"type":"{}","params":{{"cancel_on_disconnect":2222,"x":"2"}}}}"#,
            twos(20)
        ))
    );
    assert_eq!(
        read(&string_echo, 0),
        flat("InvalidParameter", r#""22""#, &format!("\"{}\"", twos(20)))
    );
    // An escape, which 0134 item 2 keeps whole, keeps its shape in the window.
    assert_eq!(
        read(&escaped, 0),
        nested("InvalidOrder", &format!("\"{}\"", twos(19)), "{}")
    );
    assert_eq!(
        read(undocumented_member, 0),
        r#"{"err":{"error_code":"ServerError","message":"2","incoming_message":2222,"request_id":22},"seq":2}"#
    );
    // The arm's echo as redacted when it is any other echo: its type mapped to no bucket.
    let as_any = format!(
        r#"{{"type":"{}","params":{{"cancel_on_disconnect":2222}}}}"#,
        twos(20)
    );
    // The arm's echo with a member added, in its body, beside its envelope or in the flat err:
    // not the frame's only content, so its shape, the arm's values and the member's spans.
    let arm_note_kept = format!(
        r#"{{"ack":{{"incoming_message":{as_any},"note":"{}"}}}}"#,
        twos(14)
    );
    let arm_seq_kept = format!(
        r#"{{"err":{{"error_code":"InvalidParameter","message":"{}","incoming_message":{as_any}}},"seq":2}}"#,
        twos(15)
    );
    let arm_ack_seq_kept = format!(r#"{{"ack":{{"incoming_message":{as_any}}},"seq":2}}"#);
    let arm_flat_extra_kept = format!(
        r#"{{"type":"Err","error_code":"RateLimited","message":{hint_s},"incoming_message":{as_any},"extra":"{}"}}"#,
        twos(15)
    );
    assert_eq!(read(&arm_note, 0), arm_note_kept);
    assert_eq!(read(&arm_seq, 0), arm_seq_kept);
    assert_eq!(read(&arm_ack_seq, 0), arm_ack_seq_kept);
    assert_eq!(read(&arm_flat_extra, 0), arm_flat_extra_kept);
    // An escaped hint, echo type or code is a span: each is judged on its bytes.
    let escaped_hint_kept = format!("\"{}\"", twos(escaped_hint.len() - 2));
    assert_eq!(
        read(&arm_escaped_hint, 0),
        nested("RateLimited", &escaped_hint_kept, ARM)
    );
    assert_eq!(
        read(&query_escaped_hint, 0),
        nested(
            "RateLimited",
            &escaped_hint_kept,
            r#"{"type":"get_user_orders","params":{"limit":2222,"offset":2}}"#
        )
    );
    assert_eq!(
        read(&escaped_type, 0),
        nested(
            "RateLimited",
            hint_s,
            &format!(r#"{{"type":"{}","params":{{}}}}"#, twos(20))
        )
    );
    // A second request in flight: the arm's echo still its own, the rest as 0134 says, an arm
    // echo with a member added included.
    assert_eq!(read(&arm_ack, 1), arm_ack);
    assert_eq!(read(&request_err, 1), read(&request_err, 0));
    assert_eq!(read(&arm_note, 1), arm_note_kept);
    assert_eq!(read(&arm_seq, 1), arm_seq_kept);
    assert_eq!(read(&arm_ack_seq, 1), arm_ack_seq_kept);
    assert_eq!(read(&arm_flat_extra, 1), arm_flat_extra_kept);
    assert_eq!(read(&arm_escaped_hint, 1), read(&arm_escaped_hint, 0));
    assert_eq!(read(&escaped, 1), twos(escaped.len()));
    // Past the arm's deadline: its echo its own, the rest as 0134 says.
    assert_eq!(read(&arm_ack, 2), arm_ack);
    assert_eq!(
        read(&arm_plus, 1),
        ack(&format!(
            r#"{{"type":"{}","params":{{"cancel_on_disconnect":2222,"x":"2"}}}}"#,
            twos(20)
        ))
    );
    assert_eq!(read(&escaped, 2), twos(escaped.len()));
    // No arm on the epoch: its echo is redacted as any echo, its type mapped to no bucket.
    assert_eq!(read(&arm_ack, 3), ack(&as_any));
    assert_eq!(
        read(&arm_limited_s, 1),
        flat("RateLimited", hint_s, &as_any)
    );
    // The epochs ended in the window: the undocumented code on the arm's echo kept in clear,
    // an echo of authentication and a malformed frame whole, a frame in no envelope and a
    // repeated member kept as their shape.
    assert_eq!(
        read(&ending[0], 0),
        nested("ArmNotAllowedHere", "2222", ARM)
    );
    assert_eq!(read(&ending[1], 0), twos(ending[1].len()));
    assert_eq!(
        read(&ending[2], 0),
        format!(
            r#"{{"notice":{{"incoming_message":{{"type":"{}","params":{{"order_id":"{}"}}}}}},"seq":2}}"#,
            twos(9),
            twos(36)
        )
    );
    assert_eq!(
        read(&ending[3], 0),
        r#"{"err":{"error_code":"InvalidOrder","error_code":"InvalidOrder","incoming_message":{"type":"add_order","x":2}}}"#
    );
    assert_eq!(read(&ending[4], 0), twos(ending[4].len()));
    assert_eq!(
        read(&ending[5], 0),
        nested(
            &twos(r"Rate\u004cimited".len()),
            &format!("\"{}\"", twos(hint_s.len() - 2)),
            &add_order_kept()
        )
    );

    // What the codec did live: it read each from what the journal keeps.
    let at = |live_frame: &str, nth: usize| {
        let mut seen = 0;
        steps
            .iter()
            .position(|step| match step {
                Step::Frame(f) if f == live_frame => {
                    seen += 1;
                    seen == nth + 1
                }
                _ => false,
            })
            .unwrap()
    };
    let held = |retry_after| Out {
        effects: vec![Effect::RateLimited {
            stream: TRADE,
            charge: None,
            retry_after,
        }],
        ..Out::quiet()
    };
    assert_eq!(live_outs[at(&arm_ack, 0)], Out::quiet());
    assert_eq!(live_outs[at(&arm_rejected, 0)], Out::quiet());
    assert_eq!(
        live_outs[at(&arm_limited_s, 0)],
        held(Some(Duration::from_secs(3)))
    );
    assert_eq!(
        live_outs[at(&arm_limited_ms, 0)],
        held(Some(Duration::from_millis(200)))
    );
    assert_eq!(live_outs[at(&arm_limited_text, 0)], held(None));
    assert_eq!(
        live_outs[at(&query_limited, 0)],
        Out {
            effects: vec![Effect::RateLimited {
                stream: TRADE,
                charge: Some(RateCharge::one(OpKind::Query, None)),
                retry_after: Some(Duration::from_secs(3)),
            }],
            ..Out::quiet()
        }
    );
    // The arm's echo with a member added answers nothing, a RateLimited one holding every
    // frame budget; an escaped hint is no hint, an escaped echo type no bucket.
    for nth in 0..2 {
        assert_eq!(live_outs[at(&arm_note, nth)], Out::quiet());
        assert_eq!(live_outs[at(&arm_seq, nth)], Out::quiet());
        assert_eq!(live_outs[at(&arm_ack_seq, nth)], Out::quiet());
        assert_eq!(
            live_outs[at(&arm_flat_extra, nth)],
            held(Some(Duration::from_secs(3)))
        );
        assert_eq!(live_outs[at(&arm_escaped_hint, nth)], held(None));
    }
    assert_eq!(
        live_outs[at(&query_escaped_hint, 0)],
        Out {
            effects: vec![Effect::RateLimited {
                stream: TRADE,
                charge: Some(RateCharge::one(OpKind::Query, None)),
                retry_after: None,
            }],
            ..Out::quiet()
        }
    );
    assert_eq!(
        live_outs[at(&escaped_type, 0)],
        held(Some(Duration::from_secs(3)))
    );
    assert_eq!(live_outs[at(&escaped, 0)], Out::quiet());
    assert!(live_outs[at(&escaped, 1)].reconnects());
    for last in &ending {
        assert!(live_outs[at(last, 0)].reconnects(), "{last}");
    }
    // Nothing answered a request: no outcome but at a deadline.
    for (out, step) in live_outs.iter().zip(&steps) {
        for event in &out.events {
            match (event, step) {
                (ExecEvent::Conn { .. }, _) => {}
                (ExecEvent::Outcome { rpc, .. }, Step::Deadline(n)) => assert_eq!(rpc.0, *n),
                (other, _) => panic!("{other:?}"),
            }
        }
    }

    // Replay: a fresh codec, handed the journaled frames, gives what live gave.
    let mut replay = probe_codec(SUB);
    let mut next = back.into_iter();
    let replay_outs = run(&mut replay, &steps, |_, _| next.next().unwrap());
    assert_eq!(live_outs.len(), replay_outs.len());
    for (at, (a, b)) in live_outs.iter().zip(&replay_outs).enumerate() {
        assert_eq!(a, b, "step {at}");
    }
    assert_eq!(live.counts(), replay.counts());
    let counts = live.counts();
    assert_eq!(counts.errs(ErrCode::RateLimited), 12);
    assert!(counts.acks >= 5 && counts.undocumented_errs >= 1);
}

// ----- The private REST reads' headers (src/auth) -----

/// The `docs_shaped` vector of `fixtures/qfex/signing/hmac-vectors.json`: its secret, nonce,
/// wall time and signature.
fn docs_vector() -> (String, [u64; 2], WallNs, String, String) {
    let file: Value = serde_json::from_str(include_str!(
        "../../../../fixtures/qfex/signing/hmac-vectors.json"
    ))
    .unwrap();
    let v = file["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "docs_shaped")
        .unwrap();
    (
        v["secret"].as_str().unwrap().to_owned(),
        [
            v["nonce"][0].as_u64().unwrap(),
            v["nonce"][1].as_u64().unwrap(),
        ],
        WallNs(v["wall_ns"].as_i64().unwrap()),
        v["nonce_hex"].as_str().unwrap().to_owned(),
        v["signature"].as_str().unwrap().to_owned(),
    )
}

#[test]
fn the_rest_reads_headers_come_from_src_auth_signed_as_qfex_documents() {
    let (secret, nonce, wall, nonce_hex, signature) = docs_vector();
    let creds = || {
        let mut creds = Secrets::new();
        creds.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
        creds.insert(auth::SECRET_KEY, Secret::new(secret.clone()));
        creds
    };
    let ctx = EncodeCtx {
        wall,
        mono: MonoNs(1),
        nonces: NonceBlock::new(nonce.to_vec()),
    };
    assert_eq!(TradeAuth::REST_NONCES, 2);
    let unix_ts = (wall.0 / 1_000_000_000).to_string();
    for (account, selected) in [("primary", None), (SUB, Some(SUB))] {
        let handle = TradeAuth::new(&preprod(account), creds()).unwrap();
        let headers = handle.rest_headers(&ctx).unwrap();
        let shown: Vec<(&str, &str, bool)> = headers
            .iter()
            .map(|h| (h.name, h.value.as_str(), h.redact))
            .collect();
        let mut want = vec![
            ("x-qfex-public-key", PUBLIC, true),
            ("x-qfex-hmac-signature", signature.as_str(), true),
            ("x-qfex-nonce", nonce_hex.as_str(), false),
            ("x-qfex-timestamp", unix_ts.as_str(), false),
        ];
        // The subaccount selected, always redacted; the primary account by its absence.
        if let Some(id) = selected {
            want.push(("x-qfex-requested-account-id", id, true));
        }
        assert_eq!(shown, want, "{account}");
        // Signed as the authentication is, the same HMAC (src/sign).
        let signed = HmacKey::new(Secret::new(secret.clone()))
            .unwrap()
            .sign(nonce, wall)
            .unwrap();
        assert_eq!(signed.signature(), signature);
        // A header's Debug shows no redacted value.
        let debug = format!("{headers:?}");
        assert!(!debug.contains(PUBLIC) && !debug.contains(&signature) && !debug.contains(SUB));
    }
    // Refused, building nothing, for any other number of nonces or a time before the epoch.
    let handle = TradeAuth::new(&preprod(SUB), creds()).unwrap();
    for nonces in [vec![], vec![1], vec![1, 2, 3]] {
        let ctx = EncodeCtx {
            wall,
            mono: MonoNs(1),
            nonces: NonceBlock::new(nonces),
        };
        assert!(matches!(
            handle.rest_headers(&ctx),
            Err(SignError::Unsignable(_))
        ));
    }
    let before = EncodeCtx {
        wall: WallNs(-1),
        mono: MonoNs(1),
        nonces: NonceBlock::new(nonce.to_vec()),
    };
    assert!(handle.rest_headers(&before).is_err());
}

/// No credential is read or held outside `src/auth` and `src/sign`: the files this ticket adds
/// name none, and the factory hands the consumer's credentials to `src/auth` unread
/// (`tests/signing.rs` scans every file).
#[test]
fn the_probe_mode_reads_no_credential_outside_src_auth_and_src_sign() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for file in [
        "probe.rs",
        "exec/caps.rs",
        "exec/echo.rs",
        "exec/mod.rs",
        "caps.rs",
    ] {
        let text = fs::read_to_string(src.join(file)).unwrap();
        for word in [
            "expose(",
            "Secret",
            "HmacKey",
            "creds",
            "sign::",
            "SECRET_KEY",
            "PUBLIC_KEY",
        ] {
            assert!(!text.contains(word), "src/{file} names {word}");
        }
    }
    let lib = fs::read_to_string(src.join("lib.rs")).unwrap();
    assert_eq!(
        lib.matches("creds").count(),
        3,
        "the factory's three hand-offs"
    );
    assert!(lib.contains("auth::TradeAuth::new(cfg, creds)"));
    assert!(!lib.contains("expose(") && !lib.contains("HmacKey"));
}

/// A probe codec's journaled frames hold no echoed request value even in the arm's window when
/// they echo authentication (0132 item 3).
#[test]
fn an_echo_of_authentication_stays_whole_in_the_arms_window() {
    let mut codec = probe_codec(SUB);
    authenticate(&mut codec, 1);
    assert_eq!(encode(&mut codec, &ARM_ON, 1), sent(arm_sent(1)));
    for frame in [
        nested(
            "AlreadyAuthenticated",
            "null",
            r#"{"type":"auth","params":{}}"#,
        ),
        ack(&format!(
            r#"{{"type":"auth","params":{{"hmac":{{"public_key":"{PUBLIC}"}}}}}}"#
        )),
        format!(r#"{{"wrapper":[{}]}}"#, ack(r#"{"type":"auth"}"#)),
    ] {
        let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(&frame)));
        assert_eq!(spans.body(), vec![0..frame.len() as u32], "{frame}");
    }
    // And a frame holding the account's own key in a member name stays whole too.
    let named = nested("InvalidOrder", "null", &format!(r#"{{"{PUBLIC}":1}}"#));
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(&named)));
    assert_eq!(spans.body(), vec![0..named.len() as u32]);
    // Every span lies inside the frame and splits no character.
    let frame = nested("InvalidOrder", r#""é é""#, r#"{"type":"add_order"}"#);
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(&frame)));
    check_redactions(frame.as_bytes(), spans.body()).unwrap();
}
