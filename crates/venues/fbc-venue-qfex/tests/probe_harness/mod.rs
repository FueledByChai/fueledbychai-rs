//! What `tests/probe_settle.rs` and `tests/probe_resync.rs` share (FBC-jxau): the probe's
//! configurations, its codec built directly and by the factory, the calls that drive it, the
//! frames these tests build, FBC-291a's capture, and [`run_twice`], which runs a script of
//! inputs live, journaling every input through fbc-journal with the codec's own spans as the
//! runtime does, then replays the journal into a fresh codec and holds every step's events and
//! effects equal.
//!
//! Every key, account, id and frame here is synthetic, built from the shapes QFEX's pages show
//! or read from `fixtures/qfex/private` (its `SYNTHETIC` file), but for the capture, which is
//! `fixtures/qfex/exec-recorded/arm-production/` as FBC-291a recorded and scrubbed it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckLevel, AssetSym, CancelOrder, Channel, CidMint, ClientOrderId, ConnKey,
    ConnState, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec, ExecEvent, ExecSink,
    FundingSpec, HttpFailure, HttpRequest, HttpResponse, HttpTag, Inbound, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock,
    NotSentReason, OpKind, OrderKind, OrderKindTag, OrderRef, OrderShapes, PathStamps, PriceGrid,
    QueryOrder, RateCharge, RawFrame, Reject, RejectKind, RpcCall, RpcId, Secret, Secrets, Side,
    SizeStep, SpecTable, Stamp, SubmitOutcome, TagSet, Ticks, Tif, TimerTag, TradingStatus,
    TrafficClass, UnderlyingId, VenueCaps, VenueCommand, VenueConfig, VenueFactory, VenueId,
    VenueMeta, VenueOrderId, WallNs, WireSlice, check_redactions, dispatch, dispatch_market_data,
    encode_cid,
};
use fbc_journal::{
    HttpRequestRec, HttpResponseRec, JournalReader, JournalWriter, Record, RedactionKey,
};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::{ARM, CHANNELS, CLIENT_ID_FORMAT, QfexExec, TRADE};
use fbc_venue_qfex::probe::{
    KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, PREPROD_MDS, PREPROD_PROBE,
    PREPROD_REST, PREPROD_TRADE, PRODUCTION_MDS, PRODUCTION_PROBE, PRODUCTION_REST,
    PRODUCTION_TRADE,
};
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex};

/// A made-up public key, in the alphabet QFEX's examples show.
pub const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
pub const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// A made-up secret.
pub const SECRET: &str = "synthetic-qfex-secret-0001";
/// The AsyncAPI's authentication reply.
pub const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
/// The consumer's request deadline in these configurations.
pub const RPC_TIMEOUT: Duration = Duration::from_millis(2_500);
/// The resync's deadline in these configurations.
pub const RESYNC_TIMEOUT: Duration = Duration::from_secs(30);
/// A fixed 0.01 grid, lots of 0.01: `AAPL-USD`.
pub const AAPL: InstrumentId = InstrumentId::new(7);
/// A made-up venue order UUID, version 4.
pub const ORDER: &str = "2f05173c-992a-426b-bcaf-6b341644bb62";
/// Our namespace.
pub const NS: Namespace = Namespace::new(7);

pub const ARM_ON: VenueCommand = VenueCommand::ArmCancelOnDisconnect(true);
pub const DISARM: VenueCommand = VenueCommand::ArmCancelOnDisconnect(false);

// ----- Configurations and codecs -----

/// A pre-production probe configuration for `account` on these three origins, its page size
/// 1000.
pub fn on(account: &str, trade: &str, rest: &str, mds: &str) -> VenueConfig {
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
pub fn preprod(account: &str) -> VenueConfig {
    on(account, PREPROD_TRADE, PREPROD_REST, PREPROD_MDS)
}

/// The probe on a loopback stub.
pub fn loopback(account: &str) -> VenueConfig {
    on(
        account,
        "ws://127.0.0.1:9001",
        "http://127.0.0.1:9002",
        "ws://127.0.0.1:9003",
    )
}

/// The probe in its production mode on QFEX's production origins (decision 0143 item 2).
pub fn production(account: &str) -> VenueConfig {
    let cfg = on(account, PRODUCTION_TRADE, PRODUCTION_REST, PRODUCTION_MDS);
    with(cfg, KEY_EXEC_MODE, PRODUCTION_PROBE)
}

/// The probe in its production mode on a loopback stub.
pub fn production_loopback(account: &str) -> VenueConfig {
    with(loopback(account), KEY_EXEC_MODE, PRODUCTION_PROBE)
}

/// Each probe configuration for `account`: both modes, on QFEX's origins and on loopback.
pub fn every_mode(account: &str) -> [VenueConfig; 4] {
    [
        preprod(account),
        production(account),
        loopback(account),
        production_loopback(account),
    ]
}

/// `cfg` with `key` set to `value`: a page size, say.
pub fn with(mut cfg: VenueConfig, key: &str, value: &str) -> VenueConfig {
    cfg.insert(key, value);
    cfg
}

pub fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

/// The probe codec `cfg` configures, built directly.
pub fn probe_on(cfg: &VenueConfig) -> QfexExec {
    QfexExec::probe(TradeAuth::new(cfg, secrets()).unwrap(), cfg).unwrap()
}

/// The probe codec `cfg` configures, built by the factory.
pub fn factory(cfg: &VenueConfig) -> Box<dyn ExecCodec> {
    Qfex.exec_codec(cfg, secrets()).unwrap().unwrap()
}

/// The recording-only codec for `cfg`'s account, which the factory never builds.
pub fn recording(cfg: &VenueConfig) -> QfexExec {
    QfexExec::new(TradeAuth::new(cfg, secrets()).unwrap(), RPC_TIMEOUT)
}

pub fn caps() -> VenueCaps {
    Qfex.caps(&preprod("primary")).unwrap()
}

/// The context of the `n`th call: its wall time, `n` seconds past 1 760 000 000, and two
/// nonces.
pub fn ctx(n: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_000_000_000_000_000 + n as i64 * 1_000_000_000),
        mono: MonoNs(n),
        nonces: NonceBlock::new(vec![n, u64::MAX - n]),
    }
}

// ----- Driving a codec -----

/// Keeps every event with what the codec said of its time.
#[derive(Default)]
pub struct Sink(pub Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, event: ExecEvent) {
        self.0.push((meta, event));
    }
}

/// What one call gave: its result (a decode error or a refusal, as text), the events pushed
/// with their meta, and the effects asked for.
#[derive(Clone, Debug, PartialEq)]
pub struct Out {
    pub result: Result<(), String>,
    pub events: Vec<(VenueMeta, ExecEvent)>,
    pub effects: Vec<Effect>,
}

impl Out {
    pub fn quiet() -> Out {
        Out {
            result: Ok(()),
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Only `events`, each with no venue meta.
    pub fn events(events: Vec<ExecEvent>) -> Out {
        Out {
            events: events.into_iter().map(|e| (VenueMeta::NONE, e)).collect(),
            ..Out::quiet()
        }
    }

    /// Only `effects`.
    pub fn effects(effects: Vec<Effect>) -> Out {
        Out {
            effects,
            ..Out::quiet()
        }
    }

    /// Whether it ended its epoch, and only that.
    pub fn reconnects(&self) -> bool {
        self.events.is_empty() && self.asks_to_reconnect()
    }

    /// Whether one of its effects ends the epoch.
    pub fn asks_to_reconnect(&self) -> bool {
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::Reconnect { stream: TRADE, .. }))
    }

    /// Every outcome it pushed.
    pub fn outcomes(&self) -> Vec<&ExecEvent> {
        self.events
            .iter()
            .map(|(_, e)| e)
            .filter(|e| matches!(e, ExecEvent::Outcome { .. }))
            .collect()
    }
}

pub fn text(codec: &mut dyn ExecCodec, frame: &str) -> Out {
    text_with(codec, frame, &specs())
}

/// A text frame, read against the spec table `specs`.
pub fn text_with(codec: &mut dyn ExecCodec, frame: &str, specs: &SpecTable) -> Out {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, NS, |scope| {
        codec.on_frame(
            TRADE,
            RawFrame::Text(frame),
            scope,
            specs,
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

pub fn http(
    codec: &mut dyn ExecCodec,
    tag: HttpTag,
    resp: Result<HttpResponse<'_>, HttpFailure>,
) -> Out {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, NS, |scope| {
        codec.on_http(tag, resp, scope, &specs(), &mut sink, &mut fx)
    });
    Out {
        result: result.map_err(|e| format!("{e:?}")),
        events: sink.0,
        effects: fx.take(),
    }
}

pub fn open(codec: &mut dyn ExecCodec, n: u64) -> Vec<Effect> {
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(n), &mut fx);
    fx.take()
}

pub fn encode(codec: &mut dyn ExecCodec, cmd: &VenueCommand, rpc: u64) -> Out {
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

pub fn deadline(codec: &mut dyn ExecCodec, rpc: u64) -> Out {
    let mut sink = Sink::default();
    codec.on_rpc_timeout(RpcId(rpc), &mut sink);
    Out {
        events: sink.0,
        ..Out::quiet()
    }
}

pub fn resync(codec: &mut dyn ExecCodec, n: u64) -> Out {
    let mut fx = Effects::new();
    codec.resync(&ctx(n), &mut fx);
    Out::effects(fx.take())
}

pub fn timer(codec: &mut dyn ExecCodec, tag: TimerTag, at: u64) -> Out {
    let mut fx = Effects::new();
    codec.on_timer(tag, &ctx(at), &mut fx);
    Out::effects(fx.take())
}

pub fn authenticated() -> ExecEvent {
    ExecEvent::Conn {
        stream: TRADE,
        state: ConnState::Authenticated,
    }
}

/// QFEX's confirmation of `channel`'s subscription.
pub fn confirm(channel: &str) -> String {
    format!(r#"{{"subscribed":"{channel}"}}"#)
}

/// Opens epoch `n` and authenticates it: the auth reply sends the three subscriptions, and
/// their last confirmation reports the stream (FBC-bsou).
pub fn authenticate(codec: &mut dyn ExecCodec, n: u64) {
    open(codec, n);
    let out = text(codec, AUTHENTICATED);
    assert_eq!(out.effects.len(), 3, "{out:?}");
    for (at, channel) in CHANNELS.iter().enumerate() {
        let out = text(codec, &confirm(channel));
        let want = if at == 2 {
            vec![(VenueMeta::NONE, authenticated())]
        } else {
            vec![]
        };
        assert_eq!(out.events, want, "{channel}");
    }
}

/// The arm, as the codec sends it on `rpc`: one frame naming the request with the consumer's
/// deadline, Safety traffic, charged the rate page's 0.1 under `Control`.
pub fn arm_sent(rpc: u64) -> Vec<Effect> {
    vec![Effect::Send {
        stream: TRADE,
        frame: WireSlice::plain(ARM.as_bytes().to_vec()),
        rpc: Some(RpcCall {
            id: RpcId(rpc),
            timeout: RPC_TIMEOUT,
        }),
        class: TrafficClass::Safety,
        charge: RateCharge::one(OpKind::Control, None),
    }]
}

pub fn refused(why: NotSentReason) -> Out {
    Out {
        result: Err(format!("{why:?}")),
        ..Out::quiet()
    }
}

pub fn sent(effects: Vec<Effect>) -> Out {
    Out::effects(effects)
}

pub fn outcome(rpc: u64, outcome: SubmitOutcome) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item: None,
        outcome,
    }
}

pub fn unknown(rpc: u64) -> ExecEvent {
    outcome(rpc, SubmitOutcome::Unknown)
}

/// The arm on `rpc` accepted, as an ack echoing it answers it.
pub fn accepted(rpc: u64) -> ExecEvent {
    outcome(
        rpc,
        SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
    )
}

/// The arm on `rpc` rejected by an err with the documented code `code`, or with none: the
/// documented code alone, since the message is a span read as absent.
pub fn rejected(rpc: u64, code: Option<&str>) -> ExecEvent {
    outcome(
        rpc,
        SubmitOutcome::Rejected(Reject {
            kind: RejectKind::Other,
            venue_code: code.map(Into::into),
            raw: Arc::from(""),
        }),
    )
}

/// A budget-only hold of every frame budget.
pub fn held(retry_after: Option<Duration>) -> Out {
    Out::effects(vec![Effect::RateLimited {
        stream: TRADE,
        charge: None,
        retry_after,
    }])
}

// ----- Specs, ids and commands -----

pub fn spec() -> InstrumentSpec {
    spec_for("AAPL-USD", AAPL)
}

/// The spec of `AAPL-USD`, but for its symbol and id: on a fixed 0.01 grid, in lots of 0.01.
pub fn spec_for(symbol: &str, id: InstrumentId) -> InstrumentSpec {
    let venue_symbol = dispatch_market_data(&caps(), |scope| scope.venue_symbol(symbol)).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(id.get()),
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

pub fn specs() -> SpecTable {
    let mut specs = SpecTable::new();
    specs.insert(spec());
    specs
}

/// Client ids minted under a held namespace lease, once per test binary.
pub fn cids() -> &'static [ClientOrderId] {
    static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    MINTED.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-qfex-jxau-cids-{}", std::process::id()));
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

/// Our `n`th client id as `client_order_id` carries it.
pub fn wire_cid(n: usize) -> String {
    encode_cid(&CLIENT_ID_FORMAT, cids()[n])
        .unwrap()
        .as_str()
        .to_owned()
}

/// The venue order id `id`, as a decoder reads it.
pub fn venue_id(id: &str) -> VenueOrderId {
    dispatch_market_data(&caps(), |scope| scope.venue_order_id(id)).unwrap()
}

/// A limit GTC buy of 1.00 at 200.50 on `AAPL-USD`, our first client id.
pub fn gtc() -> NewOrder {
    NewOrder {
        cid: cids()[0],
        inst: AAPL,
        side: Side::Buy,
        qty: Lots::new(100).unwrap(),
        kind: OrderKind::Limit { px: Ticks(20_050) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
    }
}

/// A cancel of the order `id` names, by its venue id.
pub fn cancel(id: &str) -> VenueCommand {
    VenueCommand::Cancel(CancelOrder {
        target: OrderRef::Venue(venue_id(id)),
        inst: AAPL,
        side: Side::Buy,
        placement_nonce: None,
    })
}

/// A query of the order `id` names, by its venue id.
pub fn query(id: &str) -> VenueCommand {
    VenueCommand::Query(QueryOrder {
        target: OrderRef::Venue(venue_id(id)),
        inst: AAPL,
        placement_nonce: None,
    })
}

// ----- Frames -----

pub fn twos(n: usize) -> String {
    "2".repeat(n)
}

/// The nested err, `message` written as JSON.
pub fn nested(code: &str, message: &str, echo: &str) -> String {
    format!(r#"{{"err":{{"error_code":"{code}","message":{message},"incoming_message":{echo}}}}}"#)
}

/// The flat err, `message` written as JSON.
pub fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":{message},"incoming_message":{echo}}}"#
    )
}

pub fn ack(echo: &str) -> String {
    format!(r#"{{"ack":{{"incoming_message":{echo}}}}}"#)
}

/// `base`'s members, each written as JSON, in order, with each of `changes` replacing the
/// member it names, or removing it (`None`), or, for a member `base` lacks, added last: one
/// object.
pub fn row(base: &[(&str, &str)], changes: &[(&str, Option<&str>)]) -> String {
    let mut members: Vec<(String, Option<String>)> = base
        .iter()
        .map(|(name, value)| ((*name).to_owned(), Some((*value).to_owned())))
        .collect();
    for (name, value) in changes {
        let value = value.map(str::to_owned);
        match members.iter_mut().find(|(have, _)| have == name) {
            Some(member) => member.1 = value,
            None => members.push(((*name).to_owned(), value)),
        }
    }
    let body: Vec<String> = members
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| format!(r#""{name}":{value}"#)))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// A made-up `user_id`, which the account's spans redact.
pub const USER: &str = "5a5a5a5a-0000-4000-8000-0000000000a1";
/// A made-up trade UUID.
pub const TRADE_ID: &str = "55555555-5555-4555-8555-555555555555";

/// `fixtures/qfex/private/order_response.json`'s members, but naming [`ORDER`]: an `ACK` of a
/// buy of 1.0 at 200.0 on `AAPL-USD`, nothing filled, under a client id not ours.
pub const ORDER_ROW: &[(&str, &str)] = &[
    ("order_id", "\"2f05173c-992a-426b-bcaf-6b341644bb62\""),
    ("symbol", "\"AAPL-USD\""),
    ("status", "\"ACK\""),
    ("quantity", "1.0"),
    ("price", "200.0"),
    ("take_profit", "0.0"),
    ("stop_loss", "0.0"),
    ("side", "\"BUY\""),
    ("type", "\"LIMIT\""),
    ("time_in_force", "\"GTC\""),
    ("user_id", "\"5a5a5a5a-0000-4000-8000-0000000000a1\""),
    ("client_order_id", "\"synthetic-cid-1\""),
    ("quantity_remaining", "1.0"),
    ("update_time", "1760000000.123"),
    ("trade_id", "null"),
];

/// `fixtures/qfex/private/fill_response.json`'s members, but naming [`ORDER`]: a sell of 1.0
/// at 200.0 on `AAPL-USD`, its fee 0.002, under a client id not ours.
pub const FILL_ROW: &[(&str, &str)] = &[
    ("trade_id", "\"55555555-5555-4555-8555-555555555555\""),
    ("user_id", "\"5a5a5a5a-0000-4000-8000-0000000000a1\""),
    ("symbol", "\"AAPL-USD\""),
    ("price", "200.0"),
    ("quantity", "1.0"),
    ("side", "\"SELL\""),
    ("aggressor_side", "\"BUY\""),
    ("order_id", "\"2f05173c-992a-426b-bcaf-6b341644bb62\""),
    ("fee", "0.002"),
    ("order_type", "\"LIMIT\""),
    ("tif", "\"GTC\""),
    ("order_price", "200.0"),
    ("client_order_id", "\"synthetic-cid-1\""),
    ("remaining_quantity", "0.0"),
    ("take_profit", "0.0"),
    ("stop_loss", "0.0"),
    ("execution_type", "\"NEW\""),
    ("timestamp", "1760000003.25"),
    ("realised_pnl", "0.0"),
];

/// `fixtures/qfex/private/position_response.json`'s members: long 1.0 `AAPL-USD` at 200.0.
pub const POSITION_ROW: &[(&str, &str)] = &[
    ("id", "\"66666666-6666-4666-8666-666666666666\""),
    ("symbol", "\"AAPL-USD\""),
    ("position", "1.0"),
    ("margin_alloc", "10.0"),
    ("realised_pnl", "0.0"),
    ("unrealised_pnl", "0.5"),
    ("net_funding", "0.0"),
    ("open_orders", "1.0"),
    ("open_quantity", "2.708"),
    ("leverage", "20.0"),
    ("initial_margin", "0.05"),
    ("maintenance_margin", "0.03333333333333333"),
    ("average_price", "200.0"),
];

/// An `order_response`: [`ORDER_ROW`] with `changes`.
pub fn order_frame(changes: &[(&str, Option<&str>)]) -> String {
    format!(r#"{{"order_response":{}}}"#, row(ORDER_ROW, changes))
}

/// A `fill_response`: [`FILL_ROW`] with `changes`.
pub fn fill_frame(changes: &[(&str, Option<&str>)]) -> String {
    format!(r#"{{"fill_response":{}}}"#, row(FILL_ROW, changes))
}

/// A `position_response`: [`POSITION_ROW`] with `changes`.
pub fn position_frame(changes: &[(&str, Option<&str>)]) -> String {
    format!(r#"{{"position_response":{}}}"#, row(POSITION_ROW, changes))
}

/// `value` as a JSON string.
pub fn quoted(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

/// A file of `fixtures/qfex/private`.
pub fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

/// FBC-291a's capture of QFEX production (`fixtures/qfex/exec-recorded/arm-production/`).
pub fn capture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/qfex/exec-recorded/arm-production")
}

/// One captured frame: its file name and its text.
pub type Captured = (String, String);

/// The frames the capture holds of the session `prefix` names (`primary` or `subaccount`), in
/// order: those before its arm and those in its arm's window, each with its file name.
pub fn captured(prefix: &str) -> (Vec<Captured>, Vec<Captured>) {
    let mut names: Vec<String> = fs::read_dir(capture_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with(&format!("{prefix}-")) && name.ends_with(".json"))
        .collect();
    names.sort();
    let read = |name: &String| {
        (
            name.clone(),
            fs::read_to_string(capture_dir().join(name)).unwrap(),
        )
    };
    let before = names
        .iter()
        .filter(|n| n.contains("-before-arm"))
        .map(read)
        .collect();
    let window = names
        .iter()
        .filter(|n| n.contains("-in-window"))
        .map(read)
        .collect();
    (before, window)
}

// ----- A script, live and replayed -----

/// One input of a session.
#[derive(Clone, Debug)]
pub enum Step {
    /// The trade stream opens, epoch `n`, with `ctx(n)`.
    Open(u64),
    /// A text frame arrives.
    Frame(String),
    /// A command is encoded on `rpc`, with `ctx(rpc)`.
    Encode(VenueCommand, u64),
    /// Request `rpc`'s deadline falls due.
    Deadline(u64),
    /// The session asks for a resync, with `ctx(n)`.
    Resync(u64),
    /// The answer to the `nth` HTTP request the codec asked for so far (from 0).
    Http {
        nth: usize,
        status: u16,
        body: String,
    },
    /// No answer came to the `nth` HTTP request.
    HttpFailed { nth: usize, failure: HttpFailure },
    /// The `nth` timer the codec asked for so far (from 0, each opening's authentication
    /// deadline counted) fires, with `ctx(at)`.
    Timer { nth: usize, at: u64 },
}

impl Step {
    pub fn frame(text: &str) -> Step {
        Step::Frame(text.to_owned())
    }
}

/// Epoch `n` opened and authenticated: the opening, the auth reply and the three confirmations.
pub fn epoch(n: u64) -> Vec<Step> {
    let mut steps = vec![Step::Open(n), Step::frame(AUTHENTICATED)];
    steps.extend(CHANNELS.map(|c| Step::Frame(confirm(c))));
    steps
}

/// Epoch `n` authenticated and armed on `rpc`.
pub fn armed(n: u64, rpc: u64) -> Vec<Step> {
    let mut steps = epoch(n);
    steps.push(Step::Encode(ARM_ON, rpc));
    steps
}

/// What a script's journal holds on disk and as read back.
pub struct Disk {
    /// Every byte of every file the writer wrote.
    pub bytes: Vec<u8>,
    /// Each frame as the reader gives it back, blanked.
    pub frames: Vec<String>,
    /// Each HTTP request as the reader gives it back, its secret headers blanked.
    pub requests: Vec<HttpRequestRec>,
    /// Each HTTP answer's status and body as the reader gives them back, blanked.
    pub bodies: Vec<Option<(u16, Vec<u8>)>>,
}

impl Disk {
    /// Whether `needle` lies anywhere on disk.
    pub fn has(&self, needle: &str) -> bool {
        self.bytes
            .windows(needle.len())
            .any(|w| w == needle.as_bytes())
    }
}

/// Where a script's inputs come from: written to the journal live, read from it in replay.
trait Inputs {
    /// The frame `live` as the codec reads it.
    fn frame(&mut self, codec: &QfexExec, live: &str) -> String;
    /// The answer `live` to `tag` as the codec reads it.
    fn http(
        &mut self,
        codec: &QfexExec,
        tag: HttpTag,
        live: Result<(u16, Vec<u8>), HttpFailure>,
    ) -> Result<(u16, Vec<u8>), HttpFailure>;
    /// A request the codec asked for.
    fn request(&mut self, tag: HttpTag, rpc: Option<RpcId>, req: &HttpRequest);
}

struct Live {
    writer: JournalWriter,
    seq: u64,
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

impl Live {
    fn append(&mut self, record: &Record) {
        self.seq += 1;
        self.writer
            .append(WallNs(1_760_000_000_000_000_000), record)
            .unwrap();
    }
}

impl Inputs for Live {
    fn frame(&mut self, codec: &QfexExec, live: &str) -> String {
        let raw = RawFrame::Text(live);
        let spans = codec.redact_inbound(Inbound::Frame(raw));
        check_redactions(live.as_bytes(), spans.body()).unwrap();
        let record = Record::inbound_redacted(stamp(self.seq), raw, &spans).unwrap();
        self.append(&record);
        live.to_owned()
    }

    fn http(
        &mut self,
        codec: &QfexExec,
        tag: HttpTag,
        live: Result<(u16, Vec<u8>), HttpFailure>,
    ) -> Result<(u16, Vec<u8>), HttpFailure> {
        let result = match &live {
            Ok((status, body)) => {
                let resp = HttpResponse {
                    status: *status,
                    headers: &[],
                    body,
                };
                let spans = codec.redact_inbound(Inbound::Http(tag, resp));
                Ok(HttpResponseRec::redacted(&resp, &spans).unwrap())
            }
            Err(failure) => Err(*failure),
        };
        let record = Record::HttpResult {
            stamp: stamp(self.seq),
            tag,
            result,
        };
        self.append(&record);
        live
    }

    fn request(&mut self, tag: HttpTag, rpc: Option<RpcId>, req: &HttpRequest) {
        let record = Record::HttpRequest {
            at: MonoNs(self.seq),
            conn: ConnKey { conn: 0, epoch: 1 },
            tag,
            rpc,
            req: HttpRequestRec::from(req),
        };
        self.append(&record);
    }
}

/// The journal's inputs read back, requests passed over.
struct Replay(std::vec::IntoIter<Record>);

impl Inputs for Replay {
    fn frame(&mut self, _: &QfexExec, _: &str) -> String {
        match self.0.next() {
            Some(Record::Inbound { bytes, .. }) => String::from_utf8(bytes.0).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    fn http(
        &mut self,
        _: &QfexExec,
        tag: HttpTag,
        _: Result<(u16, Vec<u8>), HttpFailure>,
    ) -> Result<(u16, Vec<u8>), HttpFailure> {
        match self.0.next() {
            Some(Record::HttpResult {
                tag: kept, result, ..
            }) => {
                assert_eq!(kept, tag);
                result.map(|rec| (rec.status, rec.body.0))
            }
            other => panic!("{other:?}"),
        }
    }

    fn request(&mut self, tag: HttpTag, _: Option<RpcId>, _: &HttpRequest) {
        match self.0.next() {
            Some(Record::HttpRequest { tag: kept, .. }) => assert_eq!(kept, tag),
            other => panic!("{other:?}"),
        }
    }
}

/// Runs `steps` through `codec`, its inputs from `inputs`.
fn drive(codec: &mut QfexExec, steps: &[Step], inputs: &mut dyn Inputs) -> Vec<Out> {
    let mut http_tags: Vec<HttpTag> = Vec::new();
    let mut timer_tags: Vec<TimerTag> = Vec::new();
    let mut outs = Vec::new();
    for step in steps {
        let out = match step {
            Step::Open(n) => Out::effects(open(codec, *n)),
            Step::Frame(live) => {
                let frame = inputs.frame(codec, live);
                text(codec, &frame)
            }
            Step::Encode(cmd, rpc) => encode(codec, cmd, *rpc),
            Step::Deadline(rpc) => deadline(codec, *rpc),
            Step::Resync(n) => resync(codec, *n),
            Step::Http { nth, status, body } => {
                let tag = http_tags[*nth];
                let got = inputs.http(codec, tag, Ok((*status, body.clone().into_bytes())));
                let (status, body) = got.unwrap();
                let resp = HttpResponse {
                    status,
                    headers: &[],
                    body: &body,
                };
                http(codec, tag, Ok(resp))
            }
            Step::HttpFailed { nth, failure } => {
                let tag = http_tags[*nth];
                let got = inputs.http(codec, tag, Err(*failure));
                http(codec, tag, Err(got.unwrap_err()))
            }
            Step::Timer { nth, at } => timer(codec, timer_tags[*nth], *at),
        };
        for effect in &out.effects {
            match effect {
                Effect::Http { tag, req, rpc, .. } => {
                    http_tags.push(*tag);
                    inputs.request(*tag, *rpc, req);
                }
                Effect::Timer { tag, .. } => timer_tags.push(*tag),
                _ => {}
            }
        }
        outs.push(out);
    }
    outs
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

/// Runs `steps` through a codec `make` builds, live, journaling every frame, HTTP request and
/// HTTP answer through fbc-journal with the codec's own spans as the runtime does; then hands a
/// fresh codec the same steps with the inputs the journal reads back, as decoder replay does.
/// Every step must give the same result, events and effects both ways, and the two codecs must
/// count alike. Returns the live codec, what each step gave it, and the journal.
pub fn run_twice(make: impl Fn() -> QfexExec, steps: &[Step]) -> (QfexExec, Vec<Out>, Disk) {
    static CASE: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "fbc-qfex-jxau-{}-{}",
        std::process::id(),
        CASE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&root);
    let key = Arc::new(RedactionKey::new(&[11; 32]).unwrap());
    let mut live_inputs = Live {
        writer: JournalWriter::create(&root, 0, key).unwrap(),
        seq: 0,
    };
    let mut live = make();
    let live_outs = drive(&mut live, steps, &mut live_inputs);
    let written = live_inputs.seq;
    live_inputs.writer.flush().unwrap();
    drop(live_inputs);

    let bytes = files(&root).concat();
    let back: Vec<Record> = JournalReader::open(&root, 0)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let _ = fs::remove_dir_all(&root);
    assert_eq!(back.len() as u64, written);
    let mut disk = Disk {
        bytes,
        frames: Vec::new(),
        requests: Vec::new(),
        bodies: Vec::new(),
    };
    for record in &back {
        match record {
            Record::Inbound { bytes, .. } => {
                disk.frames
                    .push(String::from_utf8(bytes.0.clone()).unwrap());
            }
            Record::HttpRequest { req, .. } => disk.requests.push(req.clone()),
            Record::HttpResult { result, .. } => disk.bodies.push(
                result
                    .as_ref()
                    .ok()
                    .map(|rec| (rec.status, rec.body.0.clone())),
            ),
            other => panic!("{other:?}"),
        }
    }

    let mut replay = make();
    let replay_outs = drive(&mut replay, steps, &mut Replay(back.into_iter()));
    assert_eq!(live_outs.len(), replay_outs.len());
    for (at, (a, b)) in live_outs.iter().zip(&replay_outs).enumerate() {
        assert_eq!(a, b, "step {at}: {:?}", steps[at]);
    }
    assert_eq!(live.counts(), replay.counts());
    (live, live_outs, disk)
}
