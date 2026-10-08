//! FBC-x69b's done line: testnet_trade's wiring, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for Paradex, with synthetic credentials, goes through login, arm,
//! resync, start, place, ack, cancel, cancel ack and stop in that order, the session sending
//! exactly one place (`order.create`) and one cancel (`order.cancel`); a mainnet URL or chain id
//! is refused before any connection; no credential appears in what it prints; and an order that
//! does not fit the market's minimum notional is refused before the session connects.
//!
//! The stub answers, over HTTP, the login (`POST /v1/auth`, a made-up token), the order book the
//! order is priced from (the hand-built `fixtures/paradex/rest/orderbook-btc-2002.json`) and the
//! resync's open orders and positions (none: flat); over the order socket, the auth frame, the
//! four private subscriptions, the cancel-on-disconnect arm, the place (its order echoed under a
//! made-up venue id) and the cancel (queued, then the order event, an SBE `OrderEvent` at 1:2
//! built here from Paradex's schema, reporting it CLOSED by USER_CANCELED). The credentials are
//! the synthetic account and key of `fixtures/paradex/signing` (read from the vectors file's
//! header, so no key-shaped value sits in this crate). The run is on real time, bounded at 60 s.

#[path = "../examples/testnet_trade/args.rs"]
mod args;
#[path = "../src/auth.rs"]
mod auth;
#[path = "../examples/testnet_trade/link.rs"]
mod link;
#[path = "../examples/testnet_trade/trade.rs"]
mod trade;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use args::{Options, OrderSide, Parsed, TESTNET_CHAIN, USAGE};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_core::{Secret, Secrets};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// The session token the stub's login gives: made-up text that never looks like a JWT.
const TOKEN: &str = "SYNTHETIC.session-token.testnet-trade";
/// The venue id the stub gives the order.
const VID: &str = "1759500000000000777";
const MARKET: &str = "BTC-USD-PERP";

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

/// The synthetic account and key from the signing vectors' header.
fn synthetic() -> (String, String) {
    let text = fs::read_to_string(fixture("paradex/signing/paradex-vectors.tsv")).unwrap();
    let mut header = HashMap::new();
    for line in text.lines().filter_map(|l| l.strip_prefix("# ")) {
        for pair in line.split(' ') {
            if let Some((k, v)) = pair.split_once('=') {
                header.insert(k.to_owned(), v.to_owned());
            }
        }
    }
    (header["account"].clone(), header["key"].clone())
}

fn secrets() -> Secrets {
    let (account, key) = synthetic();
    let mut creds = Secrets::new();
    creds.insert(ACCOUNT_ADDRESS, Secret::new(account));
    creds.insert(SIGNING_KEY, Secret::new(key));
    creds
}

/// A lease directory of the test's own.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-testnet-trade-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn options(stub: &StubServer, min_notional: &str) -> Options {
    Options {
        market: MARKET.to_owned(),
        tick: Decimal::from_str("0.1").unwrap(),
        step: Decimal::from_str("0.00001").unwrap(),
        min_notional: Decimal::from_str(min_notional).unwrap(),
        // A resting cap above the order: the order's size is --order-usd's, never the cap's.
        resting_cap_usd: Decimal::from(20),
        inventory_cap_usd: Decimal::from(50),
        order_usd: Decimal::from(11),
        namespace: 1,
        side: OrderSide::Buy,
        away_bps: 300,
        hold_secs: 0,
        step_timeout_secs: 10,
        rest_url: stub.http_url("/v1"),
        ws_url: stub.ws_url("/v1"),
        proxy: ProxyConfig::Direct,
        lease_dir: lease_dir(),
        sole_trader: true,
    }
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// The stub's HTTP answers: the login, the order book and the resync's two reads (no open
/// order, no position).
fn routes() -> HttpRouter {
    routes_with(r#"{"results":[]}"#.to_owned())
}

/// As [`routes`], `GET /orders` answering `orders`.
fn routes_with(orders: String) -> HttpRouter {
    let book = fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let empty = r#"{"results":[]}"#;
    HttpRouter::new()
        .route(
            Method::POST,
            PathPattern::exact("/v1/auth"),
            reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#)),
        )
        .route(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            reply(200, book),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/orders"),
            reply(200, orders),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            reply(200, empty),
        )
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = Decimal::from_str(text).unwrap() * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2 in its 128-byte block layout, as
/// `fixtures/paradex/exec/README.md` describes it: our post-only limit order `cid` on `side`
/// (1 BUY, 2 SELL) of `size` at `price`, venue id `vid`, CLOSED by USER_CANCELED with nothing
/// filled. The account is 32 made-up bytes.
fn order_closed_on(side: u8, seq: i64, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    order_event(side, seq, vid, cid, price, size, size, "USER_CANCELED")
}

/// An `OrderEvent` CLOSED with `open` of `size` still open and the cancel reason `reason`
/// (empty and nothing open: filled).
#[allow(clippy::too_many_arguments)]
fn order_event(
    side: u8,
    seq: i64,
    vid: &str,
    cid: &str,
    price: &str,
    size: &str,
    open: &str,
    reason: &str,
) -> Vec<u8> {
    let null = i64::MIN;
    let ts = 1_759_500_000_205_011i64;
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([4u8, side, 1, 3]); // CLOSED, the side, LIMIT, POST_ONLY
    f.extend(e8(price).to_le_bytes());
    f.extend(null.to_le_bytes()); // triggerPrice
    f.extend(e8(size).to_le_bytes()); // size
    f.extend(e8(open).to_le_bytes()); // sizeOpen
    f.extend(null.to_le_bytes()); // avgFillPrice
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    f.extend([0u8, 0, 0, 0]); // stp, flags, requestStatus, requestType
    assert_eq!(f.len(), 8 + 128);
    for s in [vid, cid, MARKET, reason, "", ""] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// A `FillEvent` (template 21) at version 1 in its 107-byte block layout, as
/// `fixtures/paradex/exec/README.md` describes it: our buy `cid` (venue id `vid`) made one step
/// (0.00001) at `price` for no fee. The account is 32 made-up bytes.
fn fill_event(seq: i64, vid: &str, cid: &str, price: &str) -> Vec<u8> {
    let ts = 1_759_500_000_204_011i64;
    let mut f = Vec::new();
    for v in [107u16, 21, 1, 1] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([1u8, 1, 1]); // FILL, BUY, MAKER
    f.extend(e8(price).to_le_bytes());
    f.extend(e8("0.00001").to_le_bytes()); // size
    f.extend(0i64.to_le_bytes()); // fee
    f.extend(i64::MIN.to_le_bytes()); // realizedPnl: null
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(e8(price).to_le_bytes()); // underlyingPrice
    f.extend(0i64.to_le_bytes()); // realizedFunding
    assert_eq!(f.len(), 8 + 107);
    for s in [
        "8615262148007719001",
        vid,
        cid,
        "9615262148007719001",
        MARKET,
    ] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// What the stub saw of the place, for the cancel's order event.
#[derive(Default)]
struct Placed {
    cid: String,
    price: String,
    size: String,
}

/// Answers each JSON-RPC frame the session sends by its method: the auth frame and the
/// subscriptions with an empty result, the arm with `enabled: true`, the place with its order
/// under [`VID`], the cancel queued and then the order event closing the order.
fn responder(placed: Arc<Mutex<Placed>>) -> Responder {
    responder_with(placed, Arc::new(Vec::new()), End::Canceled, End::Canceled)
}

/// As [`responder`], the placed order's cancel answered with `extra` frames before the order
/// event closing it.
fn responder_extra(placed: Arc<Mutex<Placed>>, extra: Vec<Vec<u8>>) -> Responder {
    responder_full(
        placed,
        Arc::new(Vec::new()),
        End::Canceled,
        End::Canceled,
        Arc::new(extra),
    )
}

/// How the stub ends an order it is asked to cancel, in the order event after the cancel's
/// reply.
#[derive(Clone, Copy)]
enum End {
    /// CLOSED by USER_CANCELED with nothing filled.
    Canceled,
    /// CLOSED by USER_CANCELED with one step (0.00001) of it filled first; for the placed
    /// order the fill's own event comes before the order event.
    PartlyFilled,
    /// CLOSED with nothing open and no cancel reason: a fill raced the cancel.
    Filled,
    /// A batch's restored orders only: the second item `ALREADY_CLOSED` and no order event.
    Refused,
    /// Restored orders only: each FILLED by an order event that comes with the placed order's
    /// cancel, so Stop finds nothing of theirs to cancel.
    FilledBeforeStop,
}

/// The order event ending an order of `size` at `price` as `end` says.
#[allow(clippy::too_many_arguments)]
fn order_ended(
    end: End,
    side: u8,
    seq: i64,
    vid: &str,
    cid: &str,
    price: &str,
    size: &str,
) -> Vec<u8> {
    match end {
        End::Canceled | End::Refused => order_closed_on(side, seq, vid, cid, price, size),
        End::FilledBeforeStop => order_event(side, seq, vid, cid, price, size, "0", ""),
        End::PartlyFilled => {
            let open = Decimal::from_str(size).unwrap() - Decimal::from_str("0.00001").unwrap();
            let open = open.to_string();
            order_event(side, seq, vid, cid, price, size, &open, "USER_CANCELED")
        }
        End::Filled => order_event(side, seq, vid, cid, price, size, "0", ""),
    }
}

/// An open order of ours an earlier run left: its venue id and wire client id, a post-only sell
/// of 0.00001 at 70000.
struct Restored {
    vid: String,
    cid: String,
}

/// As [`responder`], answering a batch cancel of `restored` too: every item queued and then
/// each order's event ending it as `restored_end` says, or, for [`End::Refused`], the second
/// item `ALREADY_CLOSED` and no order event. The placed order's cancel is answered by an order
/// event ending it as `placed_end` says.
fn responder_with(
    placed: Arc<Mutex<Placed>>,
    restored: Arc<Vec<Restored>>,
    placed_end: End,
    restored_end: End,
) -> Responder {
    responder_full(
        placed,
        restored,
        placed_end,
        restored_end,
        Arc::new(Vec::new()),
    )
}

/// As [`responder_with`], with `extra` frames sent with the placed order's cancel, before the
/// order event closing it.
fn responder_full(
    placed: Arc<Mutex<Placed>>,
    restored: Arc<Vec<Restored>>,
    placed_end: End,
    restored_end: End,
    extra: Arc<Vec<Vec<u8>>>,
) -> Responder {
    let close = !matches!(restored_end, End::Refused);
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary frame from the client".to_owned());
        };
        let req: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let id = req["id"].clone();
        let params = &req["params"];
        let ok = |result: Value| {
            Frame::text(json!({"jsonrpc": "2.0", "result": result, "id": id}).to_string())
        };
        match req["method"].as_str().unwrap_or("") {
            "auth" | "subscribe" => Ok(vec![ok(json!({}))]),
            "order.cancel_on_disconnect" => Ok(vec![ok(json!({"enabled": true}))]),
            "order.create" => {
                let mut p = placed.lock().unwrap();
                p.cid = params["client_id"].as_str().unwrap_or("").to_owned();
                p.price = params["price"].as_str().unwrap_or("").to_owned();
                p.size = params["size"].as_str().unwrap_or("").to_owned();
                let order = json!({
                    "id": VID, "client_id": p.cid, "market": MARKET, "side": "BUY",
                    "type": "LIMIT", "instruction": "POST_ONLY", "price": p.price,
                    "size": p.size, "remaining_size": p.size, "status": "NEW",
                });
                Ok(vec![ok(json!({"order": order}))])
            }
            "order.cancel" => {
                let p = placed.lock().unwrap();
                let queued = ok(json!({"order_id": VID, "status": "QUEUED_FOR_CANCELLATION"}));
                let closed = order_ended(placed_end, 1, 5_001, VID, &p.cid, &p.price, &p.size);
                let mut frames = vec![queued];
                if matches!(placed_end, End::PartlyFilled) {
                    frames.push(Frame::Binary(fill_event(5_000, VID, &p.cid, &p.price)));
                }
                // Before the placed order's event, on the same socket: the run sees them
                // before its closed step, so before Stop builds its plan.
                if matches!(restored_end, End::FilledBeforeStop) {
                    for (i, r) in restored.iter().enumerate() {
                        let seq = 5_100 + i64::try_from(i).unwrap();
                        let ev =
                            order_ended(restored_end, 2, seq, &r.vid, &r.cid, "70000", "0.00001");
                        frames.push(Frame::Binary(ev));
                    }
                }
                frames.extend(extra.iter().cloned().map(Frame::Binary));
                frames.push(Frame::Binary(closed));
                Ok(frames)
            }
            "order.cancel_batch" => {
                let ids = params["order_ids"].as_array().cloned().unwrap_or_default();
                let mut results = Vec::new();
                let mut events = Vec::new();
                for (i, id) in ids.iter().enumerate() {
                    let vid = id.as_str().unwrap_or("");
                    let ours = restored
                        .iter()
                        .find(|r| r.vid == vid)
                        .ok_or("not restored")?;
                    let status = if close || i == 0 {
                        "QUEUED_FOR_CANCELLATION"
                    } else {
                        "ALREADY_CLOSED"
                    };
                    results.push(json!({"id": vid, "market": MARKET, "status": status}));
                    if close {
                        let seq = 6_000 + i64::try_from(i).unwrap();
                        let ev =
                            order_ended(restored_end, 2, seq, vid, &ours.cid, "70000", "0.00001");
                        events.push(Frame::Binary(ev));
                    }
                }
                let mut frames = vec![ok(json!({"results": results}))];
                frames.extend(events);
                Ok(frames)
            }
            other => Err(format!("an unexpected method {other}")),
        }
    })
}

/// Two open orders of ours an earlier run left on the market, their client ids minted in our
/// namespace (account 1, namespace 1) under a lease of their own, and the `GET /orders`
/// answer that shows them.
fn restored() -> (Vec<Restored>, String) {
    use fbc_core::{AccountKey, CidMint, Namespace, NamespaceLease, WallNs, encode_cid};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let fmt = fbc_venue_paradex::exec::exec_caps().order.client_id;
    let orders: Vec<Restored> = ["1759500000000000901", "1759500000000000902"]
        .into_iter()
        .map(|vid| Restored {
            vid: vid.to_owned(),
            cid: encode_cid(&fmt, mint.mint().unwrap()).unwrap().to_string(),
        })
        .collect();
    let results: Vec<Value> = orders
        .iter()
        .map(|o| {
            json!({
                "id": o.vid, "client_id": o.cid, "market": MARKET, "side": "SELL",
                "type": "LIMIT", "instruction": "POST_ONLY", "price": "70000",
                "size": "0.00001", "remaining_size": "0.00001", "status": "OPEN", "flags": [],
            })
        })
        .collect();
    (orders, json!({ "results": results }).to_string())
}

/// The happy script with the batch cancel of the restored orders after it.
fn restored_script(placed: Arc<Mutex<Placed>>, restored: Vec<Restored>, end: End) -> WsScript {
    let with = responder_with(placed, Arc::new(restored), End::Canceled, end);
    // The batch cancel is the ninth frame, unless the orders ended before Stop.
    let frames = if matches!(end, End::FilledBeforeStop) {
        8
    } else {
        9
    };
    let mut steps = vec![Step::Accept];
    steps.extend((0..frames).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    WsScript::new(steps)
}

/// The order socket's script: one connection, eight frames answered (auth, four
/// subscriptions, the arm, the place, the cancel).
fn script(placed: Arc<Mutex<Placed>>) -> WsScript {
    let with = responder(placed);
    let mut steps = vec![Step::Accept];
    steps.extend((0..8).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    WsScript::new(steps)
}

/// Runs testnet_trade against `stub` with `opts`, bounded at 60 s: its report and what it
/// printed.
async fn run_against(opts: &Options) -> (trade::Report, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: trade::Out = buf.clone();
    let run = trade::run(opts, None, secrets(), out);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let report = ran
        .unwrap_or_else(|_| panic!("testnet_trade did not finish in 60 s; it printed:\n{printed}"))
        .unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));
    (report, printed)
}

/// The JSON-RPC methods the session sent on the socket, in order.
fn methods(stub: &StubServer) -> Vec<String> {
    stub.connections()
        .iter()
        .flat_map(|c| c.received.iter())
        .filter_map(|f| match f {
            Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
            Frame::Binary(_) => None,
        })
        .map(|v| v["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

/// The first word after `STEP ` of each step line, in order.
fn steps(printed: &str) -> Vec<String> {
    printed
        .lines()
        .filter_map(|l| l.strip_prefix("STEP "))
        .map(|l| l.split_whitespace().next().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn testnet_trade_places_one_post_only_order_and_cancels_it_against_the_stub() {
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(script(Arc::clone(&placed)), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        [
            "login",
            "arm",
            "resync",
            "start",
            "place",
            "ack",
            "cancel",
            "cancel-ack",
            "closed",
            "stop"
        ],
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 1),
        "{printed}"
    );
    // Exactly one place and one cancel reached the venue, and nothing else order-affecting.
    let sent = methods(&stub);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.create"), 1, "{sent:?}");
    assert_eq!(count("order.cancel"), 1, "{sent:?}");
    assert_eq!(
        count("order.cancel_all") + count("order.cancel_batch") + count("order.modify"),
        0,
        "{sent:?}"
    );
    assert_eq!(count("order.cancel_on_disconnect"), 1, "{sent:?}");
    assert_eq!(stub.connections().len(), 1);
    // The order: post-only, priced 3% under the fixture's best bid of 62000.2 on the 0.1 tick
    // (60140.1 after the floor), sized to the test's $11 --order-usd on the 0.00001 step: 18
    // lots (the $20 resting cap would admit 33).
    let create = stub.connections()[0]
        .received
        .iter()
        .find_map(|f| match f {
            Frame::Text(t) if t.contains("order.create") => serde_json::from_str::<Value>(t).ok(),
            _ => None,
        })
        .unwrap();
    assert_eq!(create["params"]["instruction"], "POST_ONLY");
    assert_eq!(create["params"]["side"], "BUY");
    assert_eq!(create["params"]["price"], "60140.1");
    assert_eq!(create["params"]["size"], "0.00018");
    // The steps print what the owner needs.
    assert!(
        printed.contains("TESTNET a loopback test stub"),
        "{printed}"
    );
    assert!(printed.contains("OWNER-ASSISTED testnet run"), "{printed}");
    assert!(
        printed.contains("SEED position 0 lots seeded by hand"),
        "{printed}"
    );
    assert!(
        printed.contains("DONE ok places sent 1 cancels sent 1"),
        "{printed}"
    );
    // The touch was read twice: to price the order, and just before the place.
    assert!(printed.contains("BBO again bid 62000.2"), "{printed}");
    let book = format!("GET /v1/orderbook/{MARKET}?depth=15");
    assert_eq!(
        stub.http_requests().iter().filter(|r| **r == book).count(),
        2
    );
    // No credential, and not the session token, appears in what it printed.
    let (account, key) = synthetic();
    let lower = printed.to_ascii_lowercase();
    for secret in [&account, &key, &TOKEN.to_owned()] {
        let bare = secret.trim_start_matches("0x").to_ascii_lowercase();
        assert!(
            !lower.contains(&bare),
            "a credential was printed:\n{printed}"
        );
    }
}

#[tokio::test]
async fn an_unanswered_auth_frame_times_out_the_login_and_stops_with_nothing_placed() {
    // The login answers over HTTP, but the auth frame is never acknowledged.
    let script = WsScript::new(vec![Step::Accept, Step::Read { conn: 0 }]);
    let stub = StubServer::start(script, routes()).await.unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    assert!(!report.ok, "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(printed.contains("TIMEOUT login"), "{printed}");
    assert_eq!(steps(&printed), ["stop"], "{printed}");
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    assert_eq!(methods(&stub), ["auth"]);
}

#[tokio::test]
async fn an_unanswered_place_times_out_and_stop_leaves_it_to_cancel_on_disconnect() {
    // Everything is answered but the place: the ack step times out and the order is not
    // acknowledged. Paradex takes no cancel by client id before the acknowledgement, so Stop's
    // cancel waits for one; none comes within the step timeout, so nothing more is sent and the
    // socket's close leaves the order to cancel-on-disconnect.
    let with = responder(Arc::new(Mutex::new(Placed::default())));
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    script.push(Step::Read { conn: 0 });
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "place", "stop"],
        "{printed}"
    );
    assert!(printed.contains("TIMEOUT ack"), "{printed}");
    assert!(printed.contains("FBC-m8vm"), "{printed}");
    assert!(
        printed.contains("1 orders not yet acknowledged"),
        "{printed}"
    );
    assert!(
        printed.contains("cancel-on-disconnect to cancel them"),
        "{printed}"
    );
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 0),
        "{printed}"
    );
    let sent = methods(&stub);
    assert_eq!(sent.iter().filter(|m| *m == "order.create").count(), 1);
    assert!(
        !sent
            .iter()
            .any(|m| m.starts_with("order.cancel_") && m != "order.cancel_on_disconnect")
    );
    assert!(!sent.iter().any(|m| m == "order.cancel"), "{sent:?}");
}

#[tokio::test]
async fn a_mainnet_chain_id_or_url_is_refused_before_any_connection() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    // The stub's own URLs, but the mainnet chain id.
    let opts = options(&stub, "10");
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, Some("PRIVATE_SN_PARACLEAR_MAINNET"), secrets(), out)
        .await
        .unwrap_err();
    assert!(refused.contains("not the testnet chain id"), "{refused}");
    // Mainnet URLs, testnet chain: refused naming the URL.
    for (rest, ws, named) in [
        (
            "https://api.prod.paradex.trade/v1",
            "wss://ws.api.prod.paradex.trade/v1",
            "--rest-url",
        ),
        (
            "https://api.testnet.paradex.trade/v1",
            "wss://ws.api.prod.paradex.trade/v1",
            "--ws-url",
        ),
        (
            "http://api.testnet.paradex.trade/v1",
            "wss://ws.api.testnet.paradex.trade/v1",
            "--rest-url",
        ),
    ] {
        let mut opts = options(&stub, "10");
        opts.rest_url = rest.to_owned();
        opts.ws_url = ws.to_owned();
        let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
        let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
        assert!(refused.starts_with(named), "{refused}");
        assert!(refused.contains("testnet only"), "{refused}");
    }
    // Nothing connected: no socket, no HTTP request.
    assert!(stub.connections().is_empty());
    assert!(stub.http_requests().is_empty());
}

#[tokio::test]
async fn an_order_below_the_minimum_notional_is_refused_before_the_session_connects() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let opts = options(&stub, "100");
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.contains("below the market's minimum $100"),
        "{refused}"
    );
    // Only the order book was read; no socket was opened and no login made.
    assert_eq!(
        stub.http_requests(),
        [format!("GET /v1/orderbook/{MARKET}?depth=15")]
    );
    assert!(stub.connections().is_empty());
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| (*a).to_owned()).collect()
}

const MARKET_ARGS: [&str; 21] = [
    "--sole-trader",
    "--namespace",
    "1",
    "--order-usd",
    "11",
    "--side",
    "buy",
    "--away-bps",
    "300",
    "--resting-cap-usd",
    "11",
    "--inventory-cap-usd",
    "50",
    "--market",
    "BTC-USD-PERP",
    "--tick",
    "0.1",
    "--step",
    "0.00001",
    "--min-notional",
    "10",
];

#[test]
fn the_command_line_defaults_to_testnet_and_refuses_mainnet_urls() {
    let Ok(Parsed::Trade(opts)) = args::parse(strings(&MARKET_ARGS)) else {
        panic!("the market flags parse");
    };
    assert_eq!(opts.rest_url, "https://api.testnet.paradex.trade/v1");
    assert_eq!(opts.ws_url, "wss://ws.api.testnet.paradex.trade/v1");
    assert_eq!((opts.side, opts.away_bps), (OrderSide::Buy, 300));
    // The leases and the client-id high-water mark default to a directory that outlives a
    // reboot, never the temporary directory.
    assert!(
        opts.lease_dir.ends_with(".fueledbychai/testnet_trade"),
        "{opts:?}"
    );
    for (flag, url) in [
        ("--rest-url", "https://api.prod.paradex.trade/v1"),
        ("--ws-url", "wss://ws.api.prod.paradex.trade/v1"),
        ("--rest-url", "https://user@api.testnet.paradex.trade/v1"),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade.evil.example/v1",
        ),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, url]));
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(flag), "{flag} {url}: {err}");
    }
    // A loopback stub on one side only is refused too.
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--rest-url", "http://127.0.0.1:9/v1"]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("one is a loopback stub")
    );
    // Every market flag is required, and the bounds hold.
    let mut argv = strings(&MARKET_ARGS);
    argv.remove(0);
    assert!(
        args::parse(argv)
            .unwrap_err()
            .starts_with("--sole-trader is required")
    );
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&[
        "--rest-url",
        "http://127.0.0.1:9/v1",
        "--ws-url",
        "ws://127.0.0.1:9/v1",
        "--socks5",
        "proxy.example:1080",
    ]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .starts_with("--socks5 with loopback stub URLs")
    );
    // The caps and the order's side and distance, too: the consumer's own, with no default.
    for missing in [
        "--market",
        "--tick",
        "--step",
        "--min-notional",
        "--resting-cap-usd",
        "--inventory-cap-usd",
        "--side",
        "--away-bps",
        "--order-usd",
        "--namespace",
    ] {
        let i = MARKET_ARGS.iter().position(|a| *a == missing).unwrap();
        let mut argv = strings(&MARKET_ARGS);
        argv.drain(i..i + 2);
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(missing), "{err}");
    }
    for (flag, bad) in [
        ("--away-bps", "99"),
        ("--hold", "61"),
        ("--side", "both"),
        ("--resting-cap-usd", "0"),
        ("--inventory-cap-usd", "-50"),
        ("--namespace", "0"),
        ("--namespace", "65536"),
        // Above the $11 resting cap: the cap bounds the order.
        ("--order-usd", "12"),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, bad]));
        assert!(args::parse(argv).unwrap_err().starts_with(flag));
    }
}

#[test]
fn the_guard_admits_only_the_testnet_chain_id() {
    let (rest, ws) = (args::TESTNET_REST, args::TESTNET_WS);
    assert_eq!(
        args::testnet_guard(rest, ws, None),
        Ok(args::Target::Testnet)
    );
    assert_eq!(
        args::testnet_guard(rest, ws, Some(TESTNET_CHAIN)),
        Ok(args::Target::Testnet)
    );
    for chain in ["PRIVATE_SN_PARACLEAR_MAINNET", "0x1", ""] {
        let err = args::testnet_guard(rest, ws, Some(chain)).unwrap_err();
        assert!(err.contains("not the testnet chain id"), "{chain}: {err}");
    }
}

#[test]
fn help_names_the_environment_the_flags_and_every_line() {
    let parsed = args::parse(strings(&["--help"])).unwrap();
    assert_eq!(parsed, Parsed::Help);
    for word in [
        auth::ACCOUNT_VAR,
        auth::KEY_VAR,
        args::CHAIN_VAR,
        "PRIVATE_SN_POTC_SEPOLIA",
        "--market",
        "--tick",
        "--step",
        "--min-notional",
        "--resting-cap-usd",
        "--inventory-cap-usd",
        "--side",
        "--away-bps",
        "--hold",
        "--step-timeout",
        "--rest-url",
        "--ws-url",
        "--socks5",
        "--lease-dir",
        "--sole-trader",
        "--namespace",
        "--order-usd",
        "BBO again",
        "STEP login",
        "STEP arm",
        "STEP resync",
        "STEP start",
        "STEP place",
        "STEP ack",
        "STEP cancel ",
        "STEP cancel-ack",
        "STEP closed",
        "STEP stop",
        "TIMEOUT",
        "DONE",
    ] {
        assert!(USAGE.contains(word), "--help does not mention {word}");
    }
}

#[test]
fn credentials_come_from_the_two_variables_and_a_missing_one_is_named_not_shown() {
    let (account, key) = synthetic();
    let env = |a: Option<&str>, k: Option<&str>| {
        let (a, k) = (a.map(str::to_owned), k.map(str::to_owned));
        move |var: &str| match var {
            "PARADEX_ACCOUNT_ADDRESS" => a.clone(),
            "PARADEX_PRIVATE_KEY" => k.clone(),
            _ => None,
        }
    };
    let creds = auth::paradex_secrets(env(Some(&account), Some(&key))).unwrap();
    assert_eq!(creds.get(ACCOUNT_ADDRESS).unwrap().expose(), account);
    assert_eq!(creds.get(SIGNING_KEY).unwrap().expose(), key);
    assert!(!format!("{creds:?}").contains(&account[2..]));
    let err = auth::paradex_secrets(env(Some(&account), None)).unwrap_err();
    assert!(err.starts_with("PARADEX_PRIVATE_KEY is not set"), "{err}");
    assert!(!err.contains(&account[2..]));
    let err = auth::paradex_secrets(env(Some(""), Some(&key))).unwrap_err();
    assert!(
        err.starts_with("PARADEX_ACCOUNT_ADDRESS is not set"),
        "{err}"
    );
    assert!(!err.contains(&key[2..]));
}

#[tokio::test]
async fn a_run_without_the_sole_trader_confirmation_or_through_a_proxy_to_a_stub_is_refused() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.sole_trader = false;
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.starts_with("--sole-trader is required"),
        "{refused}"
    );
    // A SOCKS5 proxy would reach its own host's loopback, not the stub on this machine.
    let mut opts = options(&stub, "10");
    opts.proxy = ProxyConfig::Socks5 {
        host: "127.0.0.1".to_owned(),
        port: 9,
    };
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.starts_with("--socks5 with loopback stub URLs"),
        "{refused}"
    );
    assert!(stub.connections().is_empty());
    assert!(stub.http_requests().is_empty());
}

#[tokio::test]
async fn the_client_id_high_water_mark_is_read_before_minting_and_kept_after() {
    // A mark far above the wall-clock floor, as an earlier run would leave after its clock ran
    // ahead: the run mints above it and keeps the new mark before the place goes out.
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(script(Arc::clone(&placed)), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let hwm = trade::HighWater::at(&opts.lease_dir, fbc_core::Namespace::new(1));
    let mark = 1u64 << 60;
    hwm.write(mark).unwrap();
    let (report, printed) = run_against(&opts).await;
    assert!(report.ok, "{printed}");
    assert_eq!(hwm.read().unwrap(), mark + 1);
    // A mark that is not a number is refused before anything is sent.
    fs::write(
        opts.lease_dir.join("cid-high-water-account-1-ns-1"),
        "not a number",
    )
    .unwrap();
    let dir = opts.lease_dir.clone();
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.lease_dir = dir;
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(refused.contains("not a high-water mark"), "{refused}");
    assert!(stub.connections().is_empty());
}

#[test]
fn the_resync_the_run_seeds_from_is_the_latest() {
    use fbc_core::{InstrumentId, MonoNs, SignedLots, WallNs};
    use fbc_oms::{ResyncReport, ResyncSnapshot};
    let resynced = |pos: i64| link::Note::Resynced {
        report: ResyncReport::default(),
        snapshot: ResyncSnapshot {
            watermark: WallNs(pos),
            requested_at: MonoNs(0),
            orders: Vec::new(),
            positions: vec![(InstrumentId::new(1), SignedLots(pos))],
        },
    };
    assert!(trade::latest_resync(&[]).is_none());
    // An earlier epoch's resync, a reconnect, then the current epoch's.
    let notes = [
        resynced(3),
        link::Note::EpochEnd(fbc_core::ConnKey { conn: 0, epoch: 1 }),
        resynced(-2),
    ];
    let (_, snap) = trade::latest_resync(&notes).unwrap();
    assert_eq!(snap.positions, [(InstrumentId::new(1), SignedLots(-2))]);
}

#[tokio::test]
async fn stop_cancels_an_earlier_runs_orders_and_waits_for_them_to_end() {
    let (orders, body) = restored();
    let cids: Vec<String> = orders.iter().map(|o| o.cid.clone()).collect();
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(
        restored_script(Arc::clone(&placed), orders, End::Canceled),
        routes_with(body),
    )
    .await
    .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(
        printed.contains("2 open orders (2 on the market)"),
        "{printed}"
    );
    assert!(printed.contains("cancel all: 1 cancels sent"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 2),
        "{printed}"
    );
    assert_eq!(
        methods(&stub)
            .iter()
            .filter(|m| *m == "order.cancel_batch")
            .count(),
        1
    );
    // The mint is floored by the restored ids the resync showed: the place reuses neither.
    let p = placed.lock().unwrap();
    assert!(!cids.contains(&p.cid), "{}", p.cid);
}

#[tokio::test]
async fn a_batch_cancel_with_an_item_refused_and_no_order_event_fails_the_stop() {
    let (orders, body) = restored();
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(
        restored_script(Arc::clone(&placed), orders, End::Refused),
        routes_with(body),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    // Every step of the round trip happened, but Stop did not cancel what it found: the second
    // item is refused, so the batch is not accepted on its first item alone, and no order event
    // reports either ended.
    assert!(!report.ok, "{printed}");
    assert!(printed.contains("came back Rejected"), "{printed}");
    assert!(printed.contains("TIMEOUT stop"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn the_snapshot_floor_is_the_highest_of_our_ids_it_shows() {
    use fbc_core::{
        AccountKey, CidMatch, CidMint, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease, Side,
        VenueOrderSnapshot, VenueOrderState, WallNs,
    };
    use fbc_oms::ResyncSnapshot;
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 41, 0, WallNs(0));
    let (a, b) = (mint.mint().unwrap(), mint.mint().unwrap());
    let vid = |v: &str| {
        let caps = fbc_venue_paradex::factory::caps();
        fbc_core::dispatch(&caps, Namespace::new(1), |scope| scope.venue_order_id(v)).unwrap()
    };
    let open = |cid, v: &str| VenueOrderSnapshot {
        cid,
        vid: vid(v),
        inst: InstrumentId::new(1),
        side: Side::Sell,
        state: VenueOrderState::Open,
        px: None,
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::new(0).unwrap(),
        post_only: None,
        reduce_only: None,
    };
    let mut snap = ResyncSnapshot {
        watermark: WallNs(0),
        requested_at: MonoNs(0),
        orders: vec![],
        positions: vec![],
    };
    assert_eq!(trade::snapshot_max(&snap), 0);
    snap.orders = vec![
        open(Some(CidMatch::Ours(b)), "V-2"),
        open(Some(CidMatch::Foreign(Namespace::new(9))), "V-3"),
        open(Some(CidMatch::Ours(a)), "V-1"),
        open(None, "V-4"),
    ];
    assert_eq!(trade::snapshot_max(&snap), 43);
}

/// Runs testnet_trade against a stub whose cancel of the placed order ends it as `end`, with
/// no open order or position on the market: its report and what it printed.
async fn run_with_placed_end(end: End) -> (trade::Report, String) {
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder_with(
        Arc::clone(&placed),
        Arc::new(Vec::new()),
        end,
        End::Canceled,
    );
    let mut script = vec![Step::Accept];
    script.extend((0..8).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let ran = run_against(&opts).await;
    stub.finished().await.unwrap();
    ran
}

#[tokio::test]
async fn an_order_that_fills_instead_of_cancelling_fails_the_run() {
    let (report, printed) = run_with_placed_end(End::Filled).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the order ended without being cancelled: Filled"),
        "{printed}"
    );
    assert!(!printed.contains("STEP closed"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn an_order_cancelled_after_a_partial_fill_fails_the_run() {
    // Paradex ends a post-only order whose remainder was cancelled after a maker fill CLOSED
    // by USER_CANCELED with less than its size open: a trade happened, so the round trip did
    // not.
    let (report, printed) = run_with_placed_end(End::PartlyFilled).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the order was cancelled after 1 lots of it filled; inventory now 1 lots"),
        "{printed}"
    );
    // The backstop sees the fill too.
    assert!(
        printed.contains("the inventory moved from 0 to 1 lots during the run"),
        "{printed}"
    );
    assert!(!printed.contains("STEP closed"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn stop_fails_when_an_earlier_runs_orders_fill_instead_of_cancelling() {
    for (end, said) in [
        (
            End::Filled,
            "a Stop cancel's order ended without being cancelled: Filled",
        ),
        (
            End::PartlyFilled,
            "a Stop cancel's order was cancelled after 1 lots of it filled",
        ),
    ] {
        let (orders, body) = restored();
        let placed = Arc::new(Mutex::new(Placed::default()));
        let stub = StubServer::start(
            restored_script(Arc::clone(&placed), orders, end),
            routes_with(body),
        )
        .await
        .unwrap();
        let opts = options(&stub, "10");
        let (report, printed) = run_against(&opts).await;
        stub.finished().await.unwrap();
        // The new order's round trip happened, and both restored orders ended, but not by the
        // cancel alone: Stop did not cancel them untouched.
        assert!(printed.contains("STEP closed"), "{printed}");
        assert!(!report.ok, "{printed}");
        assert_eq!(printed.matches(said).count(), 2, "{printed}");
        assert!(printed.contains("DONE failed"), "{printed}");
    }
}

#[tokio::test]
async fn an_order_on_the_market_that_is_not_ours_refuses_the_run_before_start() {
    // A random UUID client id: another system's order (the Java brokers, the venue's UI). The
    // registry would neither cancel it nor count its fills.
    let foreign = json!({ "results": [{
        "id": "1759500000000000903", "client_id": "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
        "market": MARKET, "side": "SELL", "type": "LIMIT", "instruction": "POST_ONLY",
        "price": "70000", "size": "0.00001", "remaining_size": "0.00001", "status": "OPEN",
        "flags": [],
    }]});
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder(Arc::clone(&placed));
    // Auth, four subscriptions and the arm: nothing after them.
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes_with(foreign.to_string()))
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("1 open orders on the market are not ours"),
        "{printed}"
    );
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "stop"],
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(
        !methods(&stub)
            .iter()
            .any(|m| m.starts_with("order.c") && m != "order.cancel_on_disconnect")
    );
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn an_earlier_runs_order_that_fills_before_stop_fails_the_run() {
    // Both restored orders fill (as the placed order's cancel goes out), so Stop has nothing
    // of theirs to cancel and no fill event moves the inventory: the run still traded.
    let (orders, body) = restored();
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(
        restored_script(Arc::clone(&placed), orders, End::FilledBeforeStop),
        routes_with(body),
    )
    .await
    .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(printed.contains("STEP closed"), "{printed}");
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert!(!report.ok, "{printed}");
    assert_eq!(
        printed
            .matches("an order of ours traded during the run")
            .count(),
        2,
        "{printed}"
    );
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn an_order_of_ours_not_cancelled_when_the_run_ends_fails_it() {
    // Stop's cancel of an order can go unsent (the session ended before it took it, say), with
    // no acknowledgement or order event left to wait for: the run's end judges every order of
    // ours on the market by its state, so one still open fails the run.
    use fbc_core::{
        AccountKey, CancelReason, CidMatch, CidMint, InstrumentId, Lots, MonoNs, Namespace,
        NamespaceLease, Side, VenueOrderSnapshot, VenueOrderState, WallNs,
    };
    use fbc_oms::{LadderConfig, OrdState, OrderKey, Registry, ResyncSnapshot};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let (a, b) = (mint.mint().unwrap(), mint.mint().unwrap());
    let caps = fbc_venue_paradex::factory::caps();
    let vid = |v: &str| {
        fbc_core::dispatch(&caps, Namespace::new(1), |scope| scope.venue_order_id(v)).unwrap()
    };
    let shown = |cid, v: &str, state| VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: vid(v),
        inst: InstrumentId::new(1),
        side: Side::Sell,
        state,
        px: None,
        qty: Lots::new(1).unwrap(),
        cum_filled: Lots::new(0).unwrap(),
        post_only: None,
        reduce_only: None,
    };
    let order_caps = caps.exec.clone().unwrap().order;
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    let mut reg = Registry::new();
    let snapshot = |orders| ResyncSnapshot {
        watermark: WallNs(0),
        requested_at: MonoNs(0),
        orders,
        positions: vec![],
    };
    let key = |n| OrderKey {
        venue: Some(n),
        ingest: n,
    };
    reg.resync(
        &ladder,
        &order_caps,
        &snapshot(vec![
            shown(a, "V-1", VenueOrderState::Open),
            shown(b, "V-2", VenueOrderState::Open),
        ]),
        key(1),
    )
    .unwrap();
    let owned = [(a, Lots::ZERO), (b, Lots::ZERO)];
    let left = trade::not_cancelled(&reg, &owned);
    assert_eq!(left.iter().map(|(cid, _)| *cid).collect::<Vec<_>>(), [a, b]);
    assert!(
        left.iter().all(|(_, s)| *s == Some(OrdState::Open)),
        "{left:?}"
    );
    // `a` cancelled, `b` still open: only `b` is left.
    reg.resync(
        &ladder,
        &order_caps,
        &snapshot(vec![
            shown(a, "V-1", VenueOrderState::Canceled(CancelReason::Requested)),
            shown(b, "V-2", VenueOrderState::Open),
        ]),
        key(2),
    )
    .unwrap();
    let left = trade::not_cancelled(&reg, &owned);
    assert_eq!(left.iter().map(|(cid, _)| *cid).collect::<Vec<_>>(), [b]);
    // An order the registry holds no record of is not known to be cancelled either.
    let c = mint.mint().unwrap();
    assert_eq!(trade::not_cancelled(&reg, &[(c, Lots::ZERO)]), [(c, None)]);
}

#[tokio::test]
async fn orders_under_another_namespace_than_the_one_allocated_refuse_the_run_before_start() {
    // The earlier run's orders were minted under namespace 1; this run is allocated 2, so they
    // are another consumer's: never registered, never cancelled as ours.
    let (_, body) = restored();
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder(Arc::clone(&placed));
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes_with(body))
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.namespace = 2;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("2 open orders on the market are not ours"),
        "{printed}"
    );
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "stop"],
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
}

#[tokio::test]
async fn a_touch_that_moved_toward_the_order_before_the_place_stops_the_run_with_nothing_placed() {
    // The first read prices the order at 60140.1, 3% under 62000.2; by the second the bid is
    // 61000.2, so the order would rest only about 1.4% behind it.
    let book = fs::read_to_string(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let mut moved: Value = serde_json::from_str(&book).unwrap();
    moved["bids"] = json!([["61000.2", "0.333"]]);
    moved["asks"] = json!([["61000.5", "0.1"]]);
    moved["best_bid_api"] = json!(["61000.2", "0.333"]);
    moved["best_bid_interactive"] = json!(["61000.2", "0.333"]);
    moved["best_ask_api"] = json!(["61000.5", "0.1"]);
    moved["best_ask_interactive"] = json!(["61000.5", "0.1"]);
    moved["seq_no"] = json!(2003);
    let moved = moved.to_string();
    let reads = Arc::new(AtomicU64::new(0));
    let empty = r#"{"results":[]}"#;
    let routes = HttpRouter::new()
        .route(
            Method::POST,
            PathPattern::exact("/v1/auth"),
            reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#)),
        )
        .route_fn(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            move |_| {
                if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    reply(200, book.clone())
                } else {
                    reply(200, moved.clone())
                }
            },
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/orders"),
            reply(200, empty),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            reply(200, empty),
        );
    let with = responder(Arc::new(Mutex::new(Placed::default())));
    // Auth, four subscriptions and the arm: nothing after them.
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes)
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the touch moved toward the order (bid 61000.2, ask 61000.5)"),
        "{printed}"
    );
    assert!(printed.contains("nothing placed"), "{printed}");
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "stop"],
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(!methods(&stub).iter().any(|m| m == "order.create"));
}

/// A frame of `fixtures/paradex/exec`: whitespace-separated hex bytes, `#` to the end of a
/// line a comment.
fn sbe_fixture(name: &str) -> Vec<u8> {
    let text = fs::read_to_string(fixture(&format!("paradex/exec/{name}"))).unwrap();
    text.lines()
        .map(|l| l.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

#[tokio::test]
async fn a_fill_not_of_our_orders_or_a_moved_position_fails_the_run() {
    // Each comes in while the placed order's cancel is answered, the round trip otherwise
    // whole: another system's fill on the account (a random UUID client id, an order the
    // registry does not hold), then the venue reporting the account long 0.15 when the run
    // seeded it flat.
    let foreign = fill_event(
        4_000,
        "1759500000000000999",
        "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
        "62000",
    );
    let position = sbe_fixture("position-long-v2.sbe.txt");
    for (extra, said) in [
        (foreign, "a fill not of our orders came in"),
        (
            position,
            "the venue reported the account's position in instrument 1 as 15000 lots, not the 0",
        ),
    ] {
        let placed = Arc::new(Mutex::new(Placed::default()));
        let with = responder_extra(Arc::clone(&placed), vec![extra]);
        let mut script = vec![Step::Accept];
        script.extend((0..8).map(|_| Step::Respond {
            conn: 0,
            with: with.clone(),
        }));
        let stub = StubServer::start(WsScript::new(script), routes())
            .await
            .unwrap();
        let opts = options(&stub, "10");
        let (report, printed) = run_against(&opts).await;
        stub.finished().await.unwrap();
        assert!(printed.contains("STEP closed"), "{printed}");
        assert!(!report.ok, "{printed}");
        assert!(printed.contains(said), "{printed}");
        assert!(printed.contains("DONE failed"), "{printed}");
    }
}

#[test]
fn the_output_writer_never_waits_for_a_stalled_sink() {
    use std::io::Write;
    use std::sync::mpsc;
    /// A sink whose first write waits until the test releases it.
    struct Stalled {
        release: mpsc::Receiver<()>,
        held: bool,
        got: Arc<Mutex<Vec<u8>>>,
    }
    impl Write for Stalled {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if !self.held {
                self.held = true;
                self.release.recv().unwrap();
            }
            self.got.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let (release, rx) = mpsc::channel();
    let got = Arc::new(Mutex::new(Vec::new()));
    let sink = Stalled {
        release: rx,
        held: false,
        got: Arc::clone(&got),
    };
    let (mut writer, printing) = trade::detached(sink);
    // Every line is handed over while the sink is stalled on the first: none waits for it.
    let (done, wrote) = mpsc::channel();
    let lines = std::thread::spawn(move || {
        for i in 0..1_000 {
            writeln!(writer, "line {i}").unwrap();
        }
        done.send(()).unwrap();
        writer
    });
    wrote
        .recv_timeout(Duration::from_secs(30))
        .expect("a write waited for the stalled sink");
    let writer = lines.join().unwrap();
    assert!(got.lock().unwrap().is_empty());
    // Released, the sink gets every line in order; the thread ends once the writer is dropped.
    release.send(()).unwrap();
    drop(writer);
    printing.join().unwrap();
    let expected: String = (0..1_000).map(|i| format!("line {i}\n")).collect();
    assert_eq!(
        String::from_utf8(got.lock().unwrap().clone()).unwrap(),
        expected
    );
}
