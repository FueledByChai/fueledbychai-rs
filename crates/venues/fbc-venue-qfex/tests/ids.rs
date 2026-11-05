//! FBC-s8wq: QFEX's client-id format and its venue order-id rule, each one cited thing the whole
//! codec uses.
//!
//! - The crate's client-id format, [`CLIENT_ID_FORMAT`], is `ClientIdFormat::Uuid`, the one
//!   format named in `src/`. Every `client_order_id` the encoder writes is exactly `encode_cid`
//!   under it, for a mint's first ids and for the last ids a namespace can issue, and reads back
//!   as ours in our namespace and as foreign in another; the ids other systems send QFEX
//!   (`fixtures/qfex/conformance/ids_roundtrip/java_era.txt`) read as Unparseable.
//! - The venue order-id rule, [`is_venue_order_id`], accepts every example `order_id` in QFEX's
//!   responses, modify_order's off-variant example, a version 7 UUID and an upper-case one, and
//!   refuses what is not a UUID's 8-4-4-4-12 hex form. `cancel_order` and `get_order` encode
//!   exactly the ids it accepts and refuse, with nothing written, the ids it refuses; the private
//!   codec's inspection reads an `order_response`'s and a `fill_response`'s order id by it too.
//!
//! `caps.exec` stays `None` (FBC-mfey declares the probe's `ExecCaps`), so these tests build the
//! codec directly. Every key, account and client id here is made up; the order ids are QFEX's
//! documented examples, or made up where a test names none.

use std::fs;
use std::path::{Path, PathBuf};

use fbc_core::{
    AccountKey, AssetSym, CancelOrder, Channel, CidMatch, CidMint, ClientIdFormat, ClientOrderId,
    ConnState, DecodeError, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec, ExecEvent,
    ExecSink, FundingSpec, IdError, InstrumentId, InstrumentKind, InstrumentSpec, Lots, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OrderKind, OrderKindTag,
    OrderRef, OrderShapes, PathStamps, PriceGrid, QueryOrder, RawFrame, RpcId, Secret, Secrets,
    Side, SizeStep, SpecTable, TagSet, Ticks, Tif, TradingStatus, UnderlyingId, VenueCaps,
    VenueCommand, VenueConfig, VenueFactory, VenueId, VenueMeta, VenueOrderId, WallNs, decode_cid,
    dispatch, dispatch_market_data, encode_cid,
};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::{
    CLIENT_ID_FORMAT, OrderStatus, PrivateState, QfexExec, TRADE, is_venue_order_id,
};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0002";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0002";
/// The AsyncAPI's authentication reply.
const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
/// Our namespace, and another engine's.
const NS: Namespace = Namespace::new(7);
const OTHER_NS: Namespace = Namespace::new(8);
/// A fixed 0.01 grid, lots of 0.01: `AAPL-USD`.
const AAPL: InstrumentId = InstrumentId::new(7);
const INVALID_STATE: DecodeError = DecodeError::Malformed("invalid QFEX private state");

/// The venue order ids the rule must accept, each with where it comes from.
const ACCEPTED: [(&str, &str); 8] = [
    // The `order_response` example on the add_order, cancel_order and order pages
    // (https://docs.qfex.com/websocket/channels/trade/add_order,
    // https://docs.qfex.com/websocket/channels/trade/cancel_order,
    // https://docs.qfex.com/websocket/channels/trade/order), read 2026-11-04.
    (
        "5b309929-206f-40ec-804d-cbe46e81afc1",
        "add_order, cancel_order and order responses",
    ),
    // The `all_orders_response` example on the get_order page
    // (https://docs.qfex.com/websocket/channels/trade/get_order).
    ("2f05173c-992a-426b-bcaf-6b341644bb62", "get_order response"),
    // The order ids the get_order, cancel_order and modify_order pages' request examples name.
    ("57cc1941-8ad1-4df3-b34c-bb556518befc", "get_order request"),
    (
        "0c7c8e4d-f67e-4aa5-9c64-36a1a622ac35",
        "cancel_order request",
    ),
    (
        "510333ac-3f7b-4d88-934a-4396d48824cc",
        "modify_order request",
    ),
    // The modify_order page's example response
    // (https://docs.qfex.com/websocket/channels/trade/modify_order): its variant nibble is 4,
    // which a version 4 UUID check refuses.
    (MODIFY_EXAMPLE, "modify_order response"),
    // A made-up version 7 UUID, and the add_order example in upper case.
    ("01934f2a-6b1c-7d3e-8f40-5a6b7c8d9e0f", "a version 7 UUID"),
    ("5B309929-206F-40EC-804D-CBE46E81AFC1", "an upper-case UUID"),
];

/// modify_order's example response's `order_id`.
const MODIFY_EXAMPLE: &str = "88888888-4444-4444-4444-1234567890ab";

/// The ids the rule must refuse, each with what is wrong with it.
const REFUSED: [(&str, &str); 9] = [
    ("", "empty"),
    ("5b309929-206f-40ec-804d-cbe46e81afc", "35 characters"),
    ("5b309929-206f-40ec-804d-cbe46e81afc10", "37 characters"),
    (
        "5b309929-206f-40ec-804d-cbe46e81afcg",
        "a non-hex character",
    ),
    ("5b30992-9206f-40ec-804d-cbe46e81afc1", "a misplaced hyphen"),
    ("5b309929-206f40ec--804d-cbe46e81afc1", "a misplaced hyphen"),
    ("5b309929a206fa40eca804dacbe46e81afc1", "no hyphens"),
    (
        "5b309929-206f-40ec-804d-cbe46e81af\u{e9}",
        "a non-ASCII character",
    ),
    (
        "my-client-oid-123",
        "a client id, as cancel_order's example by client id",
    ),
];

// ----- Building and driving a codec -----

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert(auth::ACCOUNT_ID, "primary");
    cfg.insert(auth::ACCOUNT_KEY, "4");
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

fn caps() -> VenueCaps {
    Qfex.caps(&cfg()).unwrap()
}

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

/// What one frame gave: the decode's result, the events pushed and the effects asked for.
fn frame(codec: &mut QfexExec, text: &str) -> (Result<(), DecodeError>, Vec<ExecEvent>) {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, NS, |scope| {
        codec.on_frame(
            TRADE,
            RawFrame::Text(text),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    });
    (result, sink.0)
}

/// QFEX's confirmations of the three private subscriptions, the last of which reports the
/// stream authenticated (FBC-bsou).
const CONFIRMATIONS: [&str; 3] = [
    r#"{"subscribed":"order_responses"}"#,
    r#"{"subscribed":"fills"}"#,
    r#"{"subscribed":"positions"}"#,
];

/// A codec with an authenticated epoch: the auth reply reports nothing, and the last of the
/// three subscription confirmations reports the stream authenticated.
fn authed() -> QfexExec {
    let mut codec = QfexExec::new(
        TradeAuth::new(&cfg(), secrets()).unwrap(),
        std::time::Duration::from_secs(2),
    );
    let mut fx = Effects::new();
    codec.on_open(TRADE, &ctx(1), &mut fx);
    assert_eq!(frame(&mut codec, AUTHENTICATED), (Ok(()), vec![]));
    for confirmation in &CONFIRMATIONS[..2] {
        assert_eq!(frame(&mut codec, confirmation), (Ok(()), vec![]));
    }
    let (result, events) = frame(&mut codec, CONFIRMATIONS[2]);
    assert_eq!(result, Ok(()));
    assert_eq!(
        events,
        [ExecEvent::Conn {
            stream: TRADE,
            state: ConnState::Authenticated,
        }]
    );
    codec
}

/// What one encode gave: its result, and the frames it wrote, as text.
fn encode(
    codec: &mut QfexExec,
    cmd: &VenueCommand,
    rpc: u64,
) -> (Result<EncodeReceipt, NotSentReason>, Vec<String>) {
    let mut fx = Effects::new();
    let result = codec.encode(
        cmd,
        RpcId(rpc),
        &specs(),
        &ctx(rpc),
        &mut PathStamps::off(),
        &mut fx,
    );
    let effects = fx.take();
    let frames = effects
        .iter()
        .map(|effect| match effect {
            Effect::Send { frame, .. } => String::from_utf8(frame.bytes().to_vec()).unwrap(),
            other => panic!("{cmd:?} asked for {other:?}"),
        })
        .collect();
    (result, frames)
}

/// The one frame `cmd` is sent as.
fn sent(codec: &mut QfexExec, cmd: &VenueCommand, rpc: u64) -> String {
    let (result, frames) = encode(codec, cmd, rpc);
    assert_eq!(result, Ok(EncodeReceipt::new()), "{cmd:?}");
    let [frame] = frames.as_slice() else {
        panic!("{cmd:?} wrote {frames:?}");
    };
    frame.clone()
}

fn specs() -> SpecTable {
    let venue_symbol =
        dispatch_market_data(&caps(), |scope| scope.venue_symbol("AAPL-USD")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    let mut specs = SpecTable::new();
    specs.insert(InstrumentSpec {
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
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        // Lists admitting the places these tests send: LIMIT and ALO, GTC and IOC (0136).
        order_shapes: Some(OrderShapes {
            kinds: TagSet::of(&[OrderKindTag::Limit]),
            post_only: true,
            tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
        }),
        version: 1,
        fetched_at: WallNs(0),
    });
    specs
}

fn venue_id(id: &str) -> Result<VenueOrderId, IdError> {
    dispatch_market_data(&caps(), |scope| scope.venue_order_id(id))
}

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/qfex")
        .join(path)
}

fn read(path: &str) -> String {
    fs::read_to_string(fixture(path)).unwrap()
}

// ----- The client-id format -----

/// `n` client ids from a mint seeded with the high-water mark `hwm` and the start `start`,
/// under a namespace lease held in a directory of their own.
fn minted(name: &str, hwm: u64, start: WallNs, n: usize) -> Vec<ClientOrderId> {
    let dir = std::env::temp_dir().join(format!("fbc-qfex-ids-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let lease = NamespaceLease::acquire(&dir, AccountKey::new(4), NS).unwrap();
    let mut mint = CidMint::new(lease, hwm, 0, start);
    let ids = (0..n).map(|_| mint.mint().unwrap()).collect();
    if hwm == u64::MAX - n as u64 {
        // These were the last ids the namespace can issue.
        assert_eq!(mint.mint(), Err(IdError::Exhausted));
    }
    drop(mint);
    let _ = fs::remove_dir_all(&dir);
    ids
}

#[test]
fn the_crates_client_id_format_is_the_uuid_format() {
    assert_eq!(CLIENT_ID_FORMAT, ClientIdFormat::Uuid);
}

/// No other client-id format is named in the crate's source: what the encoder writes and what
/// the probe caps will declare (FBC-mfey) are the one constant.
#[test]
fn the_constant_is_the_only_client_id_format_the_source_names() {
    fn files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = Vec::new();
    files(&src, &mut sources);
    let mut named = Vec::new();
    for path in &sources {
        for (n, line) in fs::read_to_string(path).unwrap().lines().enumerate() {
            if !line.trim_start().starts_with("//") && line.contains("ClientIdFormat::") {
                named.push(format!("{}:{}: {}", path.display(), n + 1, line.trim()));
            }
        }
    }
    assert_eq!(named.len(), 1, "{named:#?}");
    assert!(
        named[0].contains("pub const CLIENT_ID_FORMAT: ClientIdFormat = ClientIdFormat::Uuid;"),
        "{named:#?}"
    );
}

/// For a mint's first ids, ids at a realistic start time and the last three a namespace can
/// issue, the `client_order_id` of the `add_order` frame is exactly `encode_cid` under the
/// constant: 36 characters of `[0-9a-f-]`, ours in our namespace and foreign in another.
#[test]
fn every_client_order_id_the_encoder_writes_is_encode_cid_under_the_constant_and_round_trips() {
    let mut cids = minted("first", 0, WallNs(0), 3);
    cids.extend(minted("now", 0, WallNs(1_762_300_000_000_000_000), 3));
    cids.extend(minted("last", u64::MAX - 3, WallNs(0), 3));
    assert_eq!(cids[0].seq(), 1);
    assert_eq!(cids[8].seq(), u64::MAX);

    let mut codec = authed();
    for (n, cid) in cids.iter().enumerate() {
        let order = NewOrder {
            cid: *cid,
            inst: AAPL,
            side: Side::Buy,
            qty: Lots::new(150).unwrap(),
            kind: OrderKind::Limit { px: Ticks(20_050) },
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let text = sent(&mut codec, &VenueCommand::Place(order), n as u64 + 1);
        let frame: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(frame["type"], json!("add_order"));
        let wire = frame["params"]["client_order_id"].as_str().unwrap();
        assert_eq!(wire, encode_cid(&CLIENT_ID_FORMAT, *cid).unwrap().as_str());
        assert_eq!(wire.len(), 36, "{wire}");
        assert!(
            wire.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-'),
            "{wire}"
        );
        assert_eq!(
            decode_cid(&CLIENT_ID_FORMAT, NS, wire),
            CidMatch::Ours(*cid)
        );
        assert_eq!(
            decode_cid(&CLIENT_ID_FORMAT, OTHER_NS, wire),
            CidMatch::Foreign(NS)
        );
    }
}

/// The made-up ids in the shapes other systems send QFEX: QFEX's documented example client ids
/// (a version 4 UUID and hyphenated labels) and the Java library's decimal millisecond counter.
/// Each decodes Unparseable under the constant, never as ours or another engine's.
#[test]
fn every_java_era_id_decodes_unparseable_under_the_constant() {
    let ids: Vec<String> = read("conformance/ids_roundtrip/java_era.txt")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect();
    let uuid_v4 = |id: &str| is_venue_order_id(id) && id.as_bytes()[14] == b'4';
    let label = |id: &str| id.contains('-') && !is_venue_order_id(id);
    let counter = |id: &str| id.len() == 13 && id.bytes().all(|b| b.is_ascii_digit());
    for (shape, holds) in [
        ("a version 4 UUID", &uuid_v4 as &dyn Fn(&str) -> bool),
        ("a hyphenated label", &label),
        ("a decimal millisecond counter", &counter),
    ] {
        assert!(ids.iter().any(|id| holds(id)), "no id is {shape}: {ids:?}");
    }
    for id in &ids {
        for own in [NS, OTHER_NS] {
            assert_eq!(
                decode_cid(&CLIENT_ID_FORMAT, own, id),
                CidMatch::Unparseable,
                "{id}"
            );
        }
    }
    // The fixture directory's README says the values are made up.
    let readme = read("conformance/README.md");
    assert!(
        readme
            .split("\n- ")
            .any(|item| item.contains("ids_roundtrip/java_era.txt") && item.contains("made up")),
        "{readme}"
    );
}

// ----- The venue order-id rule -----

#[test]
fn the_venue_order_id_rule_accepts_any_uuid_in_its_wire_form_and_refuses_the_rest() {
    for (id, what) in ACCEPTED {
        assert!(is_venue_order_id(id), "{what}: {id}");
    }
    for (id, what) in REFUSED {
        assert!(!is_venue_order_id(id), "{what}: {id:?}");
    }
    assert_eq!(REFUSED[1].0.len(), 35);
    assert_eq!(REFUSED[2].0.len(), 37);
    assert!(REFUSED[3..8].iter().all(|(id, _)| id.len() == 36));
}

/// cancel_order and get_order encode, exactly, every id the rule accepts, and refuse every id it
/// refuses with nothing written (an empty id is no `VenueOrderId` at all).
#[test]
fn cancel_order_and_get_order_encode_exactly_the_ids_the_rule_accepts() {
    let cancel = |target| {
        VenueCommand::Cancel(CancelOrder {
            target,
            inst: AAPL,
            side: Side::Sell,
            placement_nonce: None,
        })
    };
    let query = |target| {
        VenueCommand::Query(QueryOrder {
            target,
            inst: AAPL,
            placement_nonce: None,
        })
    };
    let mut codec = authed();
    for (n, (id, what)) in ACCEPTED.into_iter().enumerate() {
        let target = OrderRef::Venue(venue_id(id).unwrap());
        let rpc = 10 + 2 * n as u64;
        assert_eq!(
            sent(&mut codec, &cancel(target.clone()), rpc),
            format!(
                r#"{{"type":"cancel_order","params":{{"order_id":"{id}","symbol":"AAPL-USD","cancel_order_id_type":"order_id"}}}}"#
            ),
            "{what}"
        );
        assert_eq!(
            sent(&mut codec, &query(target), rpc + 1),
            format!(r#"{{"type":"get_order","params":{{"symbol":"AAPL-USD","order_id":"{id}"}}}}"#),
            "{what}"
        );
    }
    assert_eq!(venue_id(""), Err(IdError::Empty));
    for (id, what) in REFUSED.into_iter().skip(1) {
        let target = OrderRef::Venue(venue_id(id).unwrap());
        for cmd in [cancel(target.clone()), query(target)] {
            assert_eq!(
                encode(&mut codec, &cmd, 99),
                (Err(NotSentReason::Unencodable), Vec::new()),
                "{what}: {id}"
            );
        }
    }
    // Nothing refused touched the epoch.
    sent(
        &mut codec,
        &cancel(OrderRef::Venue(venue_id(MODIFY_EXAMPLE).unwrap())),
        100,
    );
}

/// The private codec's inspection reads an `order_response`'s and a `fill_response`'s order id
/// by the same rule: modify_order's example passes, and so does every id the rule accepts, while
/// every id it refuses makes the frame invalid.
#[test]
fn the_private_inspection_reads_order_ids_by_the_same_rule() {
    let with = |file: &str, envelope: &str, id: &str| {
        let mut frame: Value = serde_json::from_str(&read(file)).unwrap();
        frame[envelope]["order_id"] = json!(id);
        frame.to_string()
    };
    let order = |id| with("private/order_response.json", "order_response", id);
    let fill = |id| with("private/fill_response.json", "fill_response", id);
    let dec = |s: &str| Decimal::from_str_exact(s).unwrap();

    let named = venue_id(MODIFY_EXAMPLE).unwrap();
    let mut codec = authed();
    assert_eq!(frame(&mut codec, &order(MODIFY_EXAMPLE)).0, Ok(()));
    assert_eq!(
        codec.last_private(),
        Some(&PrivateState::Order {
            order: named.clone(),
            status: OrderStatus::Ack,
            price: dec("200.0"),
            quantity: dec("1.0"),
            remaining: dec("1.0"),
        })
    );
    let mut codec = authed();
    assert_eq!(frame(&mut codec, &fill(MODIFY_EXAMPLE)).0, Ok(()));
    let Some(PrivateState::Fill { order: filled, .. }) = codec.last_private() else {
        panic!("{:?}", codec.last_private());
    };
    assert_eq!(filled, &named);

    for (id, what) in ACCEPTED {
        let want = venue_id(id).unwrap();
        for text in [order(id), fill(id)] {
            let mut codec = authed();
            assert_eq!(frame(&mut codec, &text).0, Ok(()), "{what}: {text}");
            let named = match codec.last_private() {
                Some(
                    PrivateState::Order { order: id, .. } | PrivateState::Fill { order: id, .. },
                ) => id,
                other => panic!("{what}: {other:?}"),
            };
            assert_eq!(named, &want, "{what}");
        }
    }
    for (id, what) in REFUSED {
        for text in [order(id), fill(id)] {
            let mut codec = authed();
            assert_eq!(
                frame(&mut codec, &text).0,
                Err(INVALID_STATE),
                "{what}: {text}"
            );
            assert_eq!(codec.last_private(), None, "{what}");
        }
    }
}
