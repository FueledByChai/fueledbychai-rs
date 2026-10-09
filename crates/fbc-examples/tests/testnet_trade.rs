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

/// A directory removed when it is dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

thread_local! {
    /// The lease directories the test on this thread made: removed when the test's thread
    /// ends, so no run leaves its lease files and high-water mark behind.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of the test's own, removed when the test's thread ends.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-testnet-trade-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
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
        Arc::new(Vec::new()),
    )
}

/// As [`responder`], the cancel-on-disconnect arm answered with `extra` frames after its
/// reply: they reach the run before its resync ends, so before the place.
fn responder_arm_extra(placed: Arc<Mutex<Placed>>, extra: Vec<Vec<u8>>) -> Responder {
    responder_full(
        placed,
        Arc::new(Vec::new()),
        End::Canceled,
        End::Canceled,
        Arc::new(Vec::new()),
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
        End::PartlyFilled => {
            let open = Decimal::from_str(size).unwrap() - Decimal::from_str("0.00001").unwrap();
            let open = open.to_string();
            order_event(side, seq, vid, cid, price, size, &open, "USER_CANCELED")
        }
        End::Filled => order_event(side, seq, vid, cid, price, size, "0", ""),
    }
}

/// An open order of ours an earlier run left: its venue id and wire client id, a post-only
/// limit order (a sell at 70000 unless said otherwise).
struct Restored {
    vid: String,
    cid: String,
    /// Its side (1 BUY, 2 SELL) and limit price.
    side: u8,
    price: &'static str,
    /// Its size and what of it is open (less when it filled in part before the run).
    size: &'static str,
    open: &'static str,
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
        Arc::new(Vec::new()),
    )
}

/// As [`responder_with`], with `extra` frames sent with the placed order's cancel, before the
/// order event closing it, and `arm_extra` frames after the arm's reply.
fn responder_full(
    placed: Arc<Mutex<Placed>>,
    restored: Arc<Vec<Restored>>,
    placed_end: End,
    restored_end: End,
    extra: Arc<Vec<Vec<u8>>>,
    arm_extra: Arc<Vec<Vec<u8>>>,
) -> Responder {
    Responder::new(respond_full(
        placed,
        restored,
        placed_end,
        restored_end,
        extra,
        arm_extra,
    ))
}

/// [`responder_full`]'s answers, for a test that wraps them.
fn respond_full(
    placed: Arc<Mutex<Placed>>,
    restored: Arc<Vec<Restored>>,
    placed_end: End,
    restored_end: End,
    extra: Arc<Vec<Vec<u8>>>,
    arm_extra: Arc<Vec<Vec<u8>>>,
) -> impl Fn(&Frame) -> Result<Vec<Frame>, String> + Send + Sync + 'static {
    let close = !matches!(restored_end, End::Refused);
    move |frame| {
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
            "order.cancel_on_disconnect" => {
                let mut frames = vec![ok(json!({"enabled": true}))];
                frames.extend(arm_extra.iter().cloned().map(Frame::Binary));
                Ok(frames)
            }
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
                    if vid == VID {
                        // The placed order, Safety-cancelled after a reconnect (decision 0080).
                        let p = placed.lock().unwrap();
                        let status = "QUEUED_FOR_CANCELLATION";
                        results.push(json!({"id": vid, "market": MARKET, "status": status}));
                        let closed =
                            order_ended(placed_end, 1, 5_001, VID, &p.cid, &p.price, &p.size);
                        events.push(Frame::Binary(closed));
                        continue;
                    }
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
                        // Cancelled with what was open: an earlier fill stays filled.
                        let ev = match restored_end {
                            End::Canceled => order_event(
                                ours.side,
                                seq,
                                vid,
                                &ours.cid,
                                ours.price,
                                ours.size,
                                ours.open,
                                "USER_CANCELED",
                            ),
                            _ => order_ended(
                                restored_end,
                                ours.side,
                                seq,
                                vid,
                                &ours.cid,
                                ours.price,
                                "0.00001",
                            ),
                        };
                        events.push(Frame::Binary(ev));
                    }
                }
                let mut frames = vec![ok(json!({"results": results}))];
                frames.extend(events);
                Ok(frames)
            }
            other => Err(format!("an unexpected method {other}")),
        }
    }
}

/// Two open orders of ours an earlier run left on the market, their client ids minted in our
/// namespace (account 1, namespace 1) under a lease of their own, and the `GET /orders`
/// answer that shows them.
fn restored() -> (Vec<Restored>, String) {
    restored_sized("0.00001", "0.00001")
}

/// As [`restored`], each order of `size` with `open` of it open.
fn restored_sized(size: &'static str, open: &'static str) -> (Vec<Restored>, String) {
    restored_as(2, "70000", size, open)
}

/// As [`restored`], each order on `side` (1 BUY, 2 SELL) at `price`, of `size` with `open` of
/// it open.
fn restored_as(
    side: u8,
    price: &'static str,
    size: &'static str,
    open: &'static str,
) -> (Vec<Restored>, String) {
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
            side,
            price,
            size,
            open,
        })
        .collect();
    let results: Vec<Value> = orders
        .iter()
        .map(|o| {
            json!({
                "id": o.vid, "client_id": o.cid, "market": MARKET,
                "side": if o.side == 1 { "BUY" } else { "SELL" },
                "type": "LIMIT", "instruction": "POST_ONLY", "price": o.price,
                "size": o.size, "remaining_size": o.open, "status": "OPEN", "flags": [],
            })
        })
        .collect();
    (orders, json!({ "results": results }).to_string())
}

/// The script of a run that finds orders of ours an earlier run left on the market: the
/// opening (auth, four subscriptions, the arm), then the Safety batch cancel of the restored
/// orders sent as the connection opens (decision 0080), ended as `end` says. Nothing is placed
/// beside them. With [`End::Refused`] they are not shown ended, so Stop's batch cancel of them
/// follows, answered the same way.
fn restored_script(placed: Arc<Mutex<Placed>>, restored: Vec<Restored>, end: End) -> WsScript {
    let with = responder_with(placed, Arc::new(restored), End::Canceled, end);
    let frames = if matches!(end, End::Refused) { 8 } else { 7 };
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
    // The resting cap is in lots at the order's price ($20 at 60140.1: 33 lots), where it
    // rests; the inventory cap at the highest of that price, the bid and the ask ($50 at 62000.5: 80
    // lots, not the 83 at the order's price), so a position is never worth more than the cap
    // at the market.
    assert!(
        printed.contains("caps: resting 33 lots per side, inventory 80 lots"),
        "{printed}"
    );
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
    // Reviewer B's RB114-16: whether the auth frame went out before the one-second step
    // timeout depends on the machine's load (the HTTP login and the connect come first), so
    // the test holds either way: nothing but the auth frame was sent, never more than once.
    let sent = methods(&stub);
    assert!(
        sent.len() <= 1 && sent.iter().all(|m| m == "auth"),
        "{sent:?}"
    );
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
    // The testnet's chain is the one its GET /v1/system/config reports (starknet_chain_id),
    // PRIVATE_SN_PARACLEAR_TESTNET: signing for any other fails every login with HTTP 401
    // STARKNET_SIGNATURE_VERIFICATION_FAILED (the owner's first runs, 2026-10-09).
    assert_eq!(TESTNET_CHAIN, "PRIVATE_SN_PARACLEAR_TESTNET");
    // The same chain id as the Java library writes it (the decimal felt) and in hex.
    for chain in [
        "8458834024819506728615521019831122032732688838300959446835911345492",
        "0x505249564154455f534e5f50415241434c4541525f544553544e4554",
        "0X505249564154455F534E5F50415241434C4541525F544553544E4554",
    ] {
        assert_eq!(
            args::testnet_guard(rest, ws, Some(chain)),
            Ok(args::Target::Testnet),
            "{chain}"
        );
    }
    // Mainnet's, by name, in decimal and in hex; and anything else.
    for chain in [
        "PRIVATE_SN_PARACLEAR_MAINNET",
        "8458834024819506728615521019831122032732688838300957472069977523540",
        "0x505249564154455f534e5f50415241434c4541525f4d41494e4e4554",
        "0x1",
        "8458834024819506728615521019831122032732688838300959446835911345493",
        // The Java library's stale testnet default, which Paradex's testnet no longer is.
        "PRIVATE_SN_POTC_SEPOLIA",
        "7693264728749915528729180568779831130134670232771119425",
        "0x505249564154455f534e5f504f54435f5345504f4c4941",
        "",
    ] {
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
        "PRIVATE_SN_PARACLEAR_TESTNET",
        // A refused login: the line, and the usual causes (FBC-x69b, the owner's first runs).
        "NOTE login refused",
        "a mainnet key on testnet",
        "an Ethereum address",
        "not onboarded",
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
    let mut notes = vec![
        resynced(3),
        link::Note::EpochEnd {
            key: fbc_core::ConnKey { conn: 0, epoch: 1 },
            why: link::ENDED_UNSAID.to_owned(),
        },
        resynced(-2),
    ];
    let (_, snap) = trade::latest_resync(&notes).unwrap().unwrap();
    assert_eq!(snap.positions, [(InstrumentId::new(1), SignedLots(-2))]);
    // A later resync the registry refused: the latest is that refusal, never the one before.
    notes.push(link::Note::EpochEnd {
        key: fbc_core::ConnKey { conn: 0, epoch: 2 },
        why: link::ENDED_UNSAID.to_owned(),
    });
    notes.push(link::Note::ResyncRefused("duplicate".to_owned()));
    assert_eq!(
        trade::latest_resync(&notes).unwrap().unwrap_err(),
        "duplicate"
    );
    notes.push(resynced(4));
    let (_, snap) = trade::latest_resync(&notes).unwrap().unwrap();
    assert_eq!(snap.positions, [(InstrumentId::new(1), SignedLots(4))]);
}

#[tokio::test]
async fn an_earlier_runs_orders_are_safety_cancelled_as_the_connection_opens_and_nothing_is_placed()
{
    // Two sells an earlier run left resting: this connection's cancel-on-disconnect may not
    // cover them, so the session tells them unprotected and takes no place on their market
    // until they end (decision 0080). Codex r4226246463: the run cancels both at once, a
    // Safety cancel built and authorized by fbc-oms, not at Stop; it places nothing beside
    // them, waits for them to end, and Stop finds nothing left to cancel. A run after that
    // places.
    let (orders, body) = restored();
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
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("2 open orders (2 on the market)"),
        "{printed}"
    );
    assert!(
        printed.contains(
            "NOTE 2 orders of ours rest from an earlier connection or run, which this \
             connection's cancel-on-disconnect may not cover (decision 0080): Safety cancel \
             requests [2] sent for them at once\n"
        ),
        "{printed}"
    );
    assert!(
        printed.contains(
            "2 orders of ours an earlier run left rest on the market, where this connection's \
             cancel-on-disconnect may not cover them: Safety-cancelled as it opened (decision \
             0080), and shown ended; nothing placed, run again"
        ),
        "{printed}"
    );
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "stop"],
        "{printed}"
    );
    // Stop found them ended: its cancel-all sent nothing.
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert!(!printed.contains("did not end cancelled"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 1),
        "{printed}"
    );
    let sent = methods(&stub);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel_batch").count(),
        1
    );
    assert!(!sent.iter().any(|m| m == "order.create"), "{sent:?}");
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
    // The Safety batch cancel's second item is refused and no order event reports either
    // ended, so the market stays held (decision 0080): nothing was placed, and Stop cancels
    // them again. Its batch is not accepted on its first item alone.
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("not shown ended within the step timeout, so Stop cancels them"),
        "{printed}"
    );
    assert!(printed.contains("came back Rejected"), "{printed}");
    assert!(printed.contains("TIMEOUT stop"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn the_runs_order_resting_after_a_reconnect_is_safety_cancelled_at_once_as_its_round_trips_cancel()
 {
    // Codex r4226246463: the connection drops after the place's ack, and the next one's resync
    // shows the order resting, where that connection's cancel-on-disconnect may not cover it
    // (decision 0080). The link Safety-cancels it as the connection opens, not when the
    // 30 s hold ends, and the run takes that cancel as the round trip's: one place, one
    // cancel.
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder(Arc::clone(&placed));
    // Auth, four subscriptions, the arm and the place; then, on the next connection, auth,
    // four subscriptions, the arm and the Safety batch cancel.
    let mut script = vec![Step::Accept];
    script.extend((0..7).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    script.extend([Step::Close { conn: 0 }, Step::Accept]);
    script.extend((0..7).map(|_| Step::Respond {
        conn: 1,
        with: with.clone(),
    }));
    // `GET /orders`: none before the place; the placed order resting after it.
    let shown = Arc::clone(&placed);
    let book = fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let empty = r#"{"results":[]}"#;
    let routes = HttpRouter::new()
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
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), move |_| {
            let p = shown.lock().unwrap();
            if p.cid.is_empty() {
                return reply(200, empty);
            }
            let row = json!({
                "id": VID, "client_id": p.cid, "market": MARKET, "side": "BUY",
                "type": "LIMIT", "instruction": "POST_ONLY", "price": p.price,
                "size": p.size, "remaining_size": p.size, "status": "OPEN", "flags": [],
            });
            reply(200, json!({ "results": [row] }).to_string())
        })
        .route(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            reply(200, empty),
        );
    let stub = StubServer::start(WsScript::new(script), routes)
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.hold_secs = 30;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the hold was not ended by the Safety cancel: {printed}"
    );
    assert!(
        printed.contains(
            "NOTE 1 orders of ours rest from an earlier connection or run, which this \
             connection's cancel-on-disconnect may not cover (decision 0080): Safety cancel \
             requests ["
        ),
        "{printed}"
    );
    assert!(
        printed.contains(
            "the Safety cancel sent when a reconnect's resync showed the order resting \
             (decision 0080)"
        ),
        "{printed}"
    );
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
    let sent = methods(&stub);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.create"), 1, "{sent:?}");
    assert_eq!(count("order.cancel_batch"), 1, "{sent:?}");
    assert_eq!(count("order.cancel"), 0, "{sent:?}");
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
async fn an_earlier_runs_orders_that_fill_instead_of_cancelling_fail_the_run() {
    for (end, said) in [
        (End::Filled, "Terminal(Filled)"),
        (End::PartlyFilled, "an order of ours traded during the run"),
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
        // Nothing was placed (they held the market), and both restored orders ended, but not
        // by their Safety cancel alone: a fill came first.
        assert!(printed.contains("nothing placed"), "{printed}");
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

/// Runs testnet_trade against a stub whose second `GET /orderbook` answers `second` (the first
/// answers the fixture) and whose socket answers nothing after the arm: its report, what it
/// printed and the methods it sent.
async fn run_with_second_book(second: String) -> (trade::Report, String, Vec<String>) {
    let book = fs::read_to_string(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    run_with_books(book, second).await
}

/// As [`run_with_second_book`], the first read answering `book`.
async fn run_with_books(book: String, second: String) -> (trade::Report, String, Vec<String>) {
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
                    reply(200, second.clone())
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
    let sent = methods(&stub);
    (report, printed, sent)
}

#[tokio::test]
async fn a_bid_that_rose_past_the_inventory_cap_on_a_book_with_no_asks_stops_the_run() {
    // Codex r4226246459: with no ask resting, the inventory cap is at the bid, not at the buy's
    // own price below it. The first read gives $50 as 80 lots at the bid of 62000.2; by the
    // second the bid rose to 63000.2 (the buy at 60140.1 is still more than 300 bps behind
    // it), and $50 is now 79 lots there, fewer than the caps hold.
    let first = String::from_utf8(one_sided("asks")).unwrap();
    let mut rose: Value = serde_json::from_str(&first).unwrap();
    rose["bids"] = json!([["63000.2", "0.333"]]);
    rose["best_bid_api"] = json!(["63000.2", "0.333"]);
    rose["best_bid_interactive"] = json!(["63000.2", "0.333"]);
    rose["seq_no"] = json!(2003);
    let (report, printed, sent) = run_with_books(first, rose.to_string()).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("caps: resting 33 lots per side, inventory 80 lots"),
        "{printed}"
    );
    assert!(
        printed.contains("the inventory cap of $50 is now 79 lots at 63000.2"),
        "{printed}"
    );
    assert!(printed.contains("nothing placed"), "{printed}");
    assert!(!sent.iter().any(|m| m == "order.create"));
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
    let (report, printed, sent) = run_with_second_book(moved.to_string()).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the touch moved toward the order (bid 61000.2, ask 61000.5)"),
        "{printed}"
    );
    // The client-id mark was kept before the touch's second read, not between it and the place.
    assert!(
        printed_before(&printed, "MARK ", "the touch moved"),
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
    assert!(!sent.iter().any(|m| m == "order.create"));
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

#[tokio::test]
async fn an_earlier_runs_order_filled_in_part_before_the_run_and_safety_cancelled_is_not_this_runs_fill()
 {
    // Each restored order is 2 lots with 1 filled before this run: its Safety cancel (decision
    // 0080) cancels the open lot, and the fill from before is not this run's. The run itself
    // places nothing beside them, so it fails for that alone.
    let (orders, body) = restored_sized("0.00002", "0.00001");
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
    assert!(!report.ok, "{printed}");
    assert!(printed.contains("(decision 0080)"), "{printed}");
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert!(!printed.contains("filled during the run"), "{printed}");
    assert!(!printed.contains("traded during the run"), "{printed}");
    assert!(!printed.contains("did not end cancelled"), "{printed}");
    assert!(!printed.contains("inventory moved"), "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
}

#[test]
fn a_resync_after_the_seed_that_disagrees_with_the_inventory_is_reported() {
    use fbc_core::{InstrumentId, MonoNs, SignedLots, WallNs};
    use fbc_oms::{PositionCheck, ResyncReport, ResyncSnapshot};
    let inst = InstrumentId::new(1);
    let resynced = |checks| link::Note::Resynced {
        report: ResyncReport {
            checks,
            ..ResyncReport::default()
        },
        snapshot: ResyncSnapshot {
            watermark: WallNs(0),
            requested_at: MonoNs(0),
            orders: Vec::new(),
            positions: Vec::new(),
        },
    };
    // The first resync (nothing seeded, nothing compared), then a reconnect's that agrees.
    let agrees = PositionCheck::Agrees {
        inst,
        position: SignedLots(0),
    };
    let mut notes = vec![resynced(vec![]), resynced(vec![agrees])];
    assert!(trade::resync_disagreements(&notes).is_empty());
    // A reconnect's resync showing the account long 5 lots where the registry holds 0.
    notes.push(resynced(vec![PositionCheck::Desync {
        inst,
        venue: SignedLots(5),
        ledger: Some(SignedLots(0)),
    }]));
    notes.push(resynced(vec![PositionCheck::Unsettled(inst)]));
    let found = trade::resync_disagreements(&notes);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].starts_with("Desync"), "{found:?}");
    assert!(found[1].starts_with("Unsettled"), "{found:?}");
}

#[tokio::test]
async fn a_second_order_book_older_than_the_first_stops_the_run_with_nothing_placed() {
    // The same touch, but seq 2001 after the first read's 2002: a stale answer.
    let book = fs::read_to_string(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let mut stale: Value = serde_json::from_str(&book).unwrap();
    stale["seq_no"] = json!(2001);
    let (report, printed, sent) = run_with_second_book(stale.to_string()).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the second order book (seq 2001) is older than the first (seq 2002)"),
        "{printed}"
    );
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "stop"],
        "{printed}"
    );
    assert!(!sent.iter().any(|m| m == "order.create"));
}

#[tokio::test]
async fn a_moved_position_or_an_order_not_ours_seen_before_the_place_stops_the_run_with_nothing_placed()
 {
    // Each arrives with the arm's reply, before the place: the venue reporting the account
    // long 0.15 when the REST position seeded it flat, and an order event showing another
    // system's order (a random UUID client id) open on the market.
    for (frame, said) in [
        (
            sbe_fixture("position-long-v2.sbe.txt"),
            "the venue reported the account's position in instrument 1 as 15000 lots",
        ),
        (
            sbe_fixture("order-foreign-cid-v1.sbe.txt"),
            "an order not ours is in view on the market",
        ),
    ] {
        let with = responder_arm_extra(Arc::new(Mutex::new(Placed::default())), vec![frame]);
        // Auth, four subscriptions and the arm: nothing after them.
        let mut script = vec![Step::Accept];
        script.extend((0..6).map(|_| Step::Respond {
            conn: 0,
            with: with.clone(),
        }));
        let stub = StubServer::start(WsScript::new(script), routes())
            .await
            .unwrap();
        let opts = options(&stub, "10");
        let (report, printed) = run_against(&opts).await;
        stub.finished().await.unwrap();
        assert!(!report.ok, "{printed}");
        assert!(
            printed.contains("the account changed since the seed, an order of ours traded, or an order not ours is in view; nothing placed"),
            "{printed}"
        );
        assert!(printed.contains(said), "{printed}");
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
}

/// An `OrderEvent` reporting order `vid` (client id `cid`), a post-only sell of 0.00001 at
/// 70000, OPEN with all of it open.
fn order_open(seq: i64, vid: &str, cid: &str) -> Vec<u8> {
    let mut f = order_event(2, seq, vid, cid, "70000", "0.00001", "0.00001", "");
    f[8 + 16] = 3; // OPEN
    f
}

/// Whether line `first` is printed before line `then`.
fn printed_before(printed: &str, first: &str, then: &str) -> bool {
    match (printed.find(first), printed.find(then)) {
        (Some(a), Some(b)) => a < b,
        _ => false,
    }
}

#[tokio::test]
async fn an_order_of_ours_the_registry_does_not_hold_refuses_the_place_or_fails_the_run() {
    // An earlier run's order in our namespace that the (untrustworthy) resync did not show,
    // reported open by an order event: the registry holds no record of it, so Stop would not
    // cancel it nor the run count its fills.
    let (orphans, _) = restored();
    let orphan = || order_open(5_200, &orphans[0].vid, &orphans[0].cid);
    let said = "an order in our namespace that the registry does not hold";

    // Before the place: it comes with the arm's reply.
    let with = responder_arm_extra(Arc::new(Mutex::new(Placed::default())), vec![orphan()]);
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(printed.contains(said), "{printed}");
    assert!(
        printed.contains(
            "the account changed since the seed, an order of ours traded, or an order not ours is in view; nothing placed"
        ),
        "{printed}"
    );
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "stop"],
        "{printed}"
    );
    assert!(!methods(&stub).iter().any(|m| m == "order.create"));
    // The client-id mark was kept before the last checks, so nothing waits between them and
    // the place.
    assert!(printed_before(&printed, "MARK ", "BBO again"), "{printed}");

    // During the round trip: it comes with the placed order's cancel.
    let with = responder_extra(Arc::new(Mutex::new(Placed::default())), vec![orphan()]);
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

#[tokio::test]
async fn a_resync_the_registry_refuses_stops_the_run_with_nothing_placed() {
    // `GET /orders` lists the same order of ours twice: the registry refuses the snapshot, so
    // the gate the session opened at its end has no resync applied behind it.
    let (orders, _) = restored();
    let row = json!({
        "id": orders[0].vid, "client_id": orders[0].cid, "market": MARKET, "side": "SELL",
        "type": "LIMIT", "instruction": "POST_ONLY", "price": "70000", "size": "0.00001",
        "remaining_size": "0.00001", "status": "OPEN", "flags": [],
    });
    let twice = json!({ "results": [row.clone(), row] }).to_string();
    let with = responder(Arc::new(Mutex::new(Placed::default())));
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes_with(twice))
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(printed.contains("the resync was refused"), "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert_eq!(steps(&printed), ["login", "arm", "stop"], "{printed}");
    assert!(!methods(&stub).iter().any(|m| m == "order.create"));
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn the_account_audit_counts_a_refused_resync_and_orders_of_ours_the_registry_does_not_hold() {
    use fbc_core::{AccountKey, CidMint, MonoNs, Namespace, NamespaceLease, SignedLots, WallNs};
    use fbc_oms::{Registry, ResyncReport, ResyncSnapshot};
    let resynced = |report| link::Note::Resynced {
        report,
        snapshot: ResyncSnapshot {
            watermark: WallNs(0),
            requested_at: MonoNs(0),
            orders: Vec::new(),
            positions: Vec::new(),
        },
    };
    let reg = Registry::new();
    let start = Some(SignedLots(0));
    assert!(
        trade::account_changes(&[resynced(ResyncReport::default())], &reg, start, &[]).is_empty()
    );

    // A reconnect's resync the registry refused: what it holds may no longer be the account's.
    let refused = [
        resynced(ResyncReport::default()),
        link::Note::ResyncRefused("the resync lists the order twice".to_owned()),
    ];
    let found = trade::account_changes(&refused, &reg, start, &[]);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("a resync was refused"), "{found:?}");

    // A reconnect's resync showing an open order of ours the registry does not hold, and an
    // order event of another.
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let (a, b) = (mint.mint().unwrap(), mint.mint().unwrap());
    let caps = fbc_venue_paradex::factory::caps();
    let vid = fbc_core::dispatch(&caps, Namespace::new(1), |s| s.venue_order_id("V-9")).unwrap();
    let untracked = [
        resynced(ResyncReport {
            untracked: vec![(a, vid.clone())],
            ..ResyncReport::default()
        }),
        link::Note::Orphan {
            cid: b,
            vid: Some(vid),
        },
    ];
    let found = trade::account_changes(&untracked, &reg, start, &[]);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(
        found
            .iter()
            .all(|f| f.contains("an order in our namespace that the registry does not hold")),
        "{found:?}"
    );
}

#[test]
fn a_blocking_write_that_outlives_its_timeout_does_not_hold_up_the_runtimes_end() {
    use std::sync::mpsc;
    // Work blocked until the test releases it, as a `sync_all` on a stalled disk would be.
    let (release, held) = mpsc::channel::<()>();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let got = runtime.block_on(trade::off_thread(Duration::from_millis(50), move || {
        held.recv().is_ok()
    }));
    assert_eq!(got, None);
    // The runtime ends although the work is still blocked: the sample's exit never waits on it.
    let (ended, ending) = mpsc::channel();
    std::thread::spawn(move || {
        drop(runtime);
        let _ = ended.send(());
    });
    let dropped = ending.recv_timeout(Duration::from_secs(10));
    let _ = release.send(());
    assert!(
        dropped.is_ok(),
        "the runtime's end waited for the blocked work"
    );
    // Work that ends in time hands back its result.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        runtime.block_on(trade::off_thread(Duration::from_secs(10), || 7)),
        Some(7)
    );
}

#[test]
fn a_tests_lease_directories_are_removed_when_its_thread_ends() {
    let dir = std::thread::spawn(lease_dir).join().unwrap();
    assert!(!dir.exists(), "{}", dir.display());
}

#[test]
fn a_timed_out_write_keeps_what_it_holds_until_it_ends() {
    // The mint's namespace lease stays held while a mark's write runs past its timeout, so no
    // other run can take the namespace and keep a newer mark that the late write would replace.
    use fbc_core::{AccountKey, Namespace, NamespaceLease};
    use std::sync::mpsc;
    let dir = lease_dir();
    let (acct, ns) = (AccountKey::new(1), Namespace::new(1));
    let lease = NamespaceLease::acquire(&dir, acct, ns).unwrap();
    let (release, held) = mpsc::channel::<()>();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let got = runtime.block_on(trade::off_thread_holding(
        Duration::from_millis(50),
        lease,
        move || held.recv().is_ok(),
    ));
    assert!(got.is_none());
    // Timed out, but the write still runs: the namespace is still leased.
    assert!(NamespaceLease::acquire(&dir, acct, ns).is_err());
    release.send(()).unwrap();
    // Once it ends, the lease is released.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if NamespaceLease::acquire(&dir, acct, ns).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the lease was never released"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Work that ends in time hands back its result and what it held (RB114-7: the caller
    // keeps the lease for the rest of its run); dropping that releases it.
    let lease = NamespaceLease::acquire(&dir, acct, ns).unwrap();
    let (got, kept) = runtime
        .block_on(trade::off_thread_holding(
            Duration::from_secs(10),
            lease,
            || 7,
        ))
        .expect("in time");
    assert_eq!(got, 7);
    assert!(NamespaceLease::acquire(&dir, acct, ns).is_err());
    drop(kept);
    assert!(NamespaceLease::acquire(&dir, acct, ns).is_ok());
}

#[tokio::test]
async fn an_ask_that_rose_past_the_inventory_cap_before_the_place_stops_the_run_with_nothing_placed()
 {
    // The first read gives the inventory cap of $50 as 80 lots at the ask of 62000.5. By the
    // second the touch rose (bid 63000.2, ask 63000.5): the buy at 60140.1 is still more than
    // 300 bps behind it, but $50 is now 79 lots at the ask, fewer than the caps hold.
    let book = fs::read_to_string(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let mut rose: Value = serde_json::from_str(&book).unwrap();
    rose["bids"] = json!([["63000.2", "0.333"]]);
    rose["asks"] = json!([["63000.5", "0.1"]]);
    rose["best_bid_api"] = json!(["63000.2", "0.333"]);
    rose["best_bid_interactive"] = json!(["63000.2", "0.333"]);
    rose["best_ask_api"] = json!(["63000.5", "0.1"]);
    rose["best_ask_interactive"] = json!(["63000.5", "0.1"]);
    rose["seq_no"] = json!(2003);
    let (report, printed, sent) = run_with_second_book(rose.to_string()).await;
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("the inventory cap of $50 is now 79 lots at 63000.5"),
        "{printed}"
    );
    assert!(printed.contains("nothing placed"), "{printed}");
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "stop"],
        "{printed}"
    );
    assert!(!sent.iter().any(|m| m == "order.create"));
}

#[test]
fn an_order_of_ours_that_filled_since_the_run_took_it_on_is_reported_traded() {
    // An earlier run's order the resync restored, one lot of two filled before the run: only a
    // fill beyond that, reported after the run took it on, counts as traded. The run checks it
    // before the place as well as after Stop.
    use fbc_core::{
        AccountKey, CidMatch, CidMint, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease, Side,
        VenueOrderSnapshot, VenueOrderState, WallNs,
    };
    use fbc_oms::{LadderConfig, OrderKey, Registry, ResyncSnapshot};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let a = mint.mint().unwrap();
    let caps = fbc_venue_paradex::factory::caps();
    let vid = fbc_core::dispatch(&caps, Namespace::new(1), |s| s.venue_order_id("V-1")).unwrap();
    let shown = |filled| VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(a)),
        vid: vid.clone(),
        inst: InstrumentId::new(1),
        side: Side::Sell,
        state: VenueOrderState::Open,
        px: None,
        qty: Lots::new(2).unwrap(),
        cum_filled: Lots::new(filled).unwrap(),
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
    let resync = |reg: &mut Registry, filled, n| {
        let snap = ResyncSnapshot {
            watermark: WallNs(0),
            requested_at: MonoNs(0),
            orders: vec![shown(filled)],
            positions: vec![],
        };
        let key = OrderKey {
            venue: Some(n),
            ingest: n,
        };
        reg.resync(&ladder, &order_caps, &snap, key).unwrap();
    };
    resync(&mut reg, 1, 1);
    let owned = [(a, Lots::new(1).unwrap())];
    assert!(trade::traded(&reg, &owned).is_empty());
    assert!(trade::account_changes(&[], &reg, None, &owned).is_empty());
    resync(&mut reg, 2, 2);
    assert_eq!(trade::traded(&reg, &owned), [(a, Lots::new(2).unwrap())]);
    assert_eq!(
        trade::account_changes(&[], &reg, None, &owned),
        ["an order of ours traded during the run: venue order V-1 has 2 lots filled"]
    );
}

#[tokio::test]
async fn an_order_of_ours_resting_on_the_orders_side_refuses_the_place() {
    // Codex r4215880971: the registry sums resting lots, and the run converts --resting-cap-usd
    // to lots at the new order's price only. Two restored buys of 9 lots each at 62000, near
    // the touch, and the new buy of 22 lots at 49600.1 (2000 bps behind the bid of 62000.2)
    // make 40 lots, the $20 cap's 40 at 49600.1, so the registry admits them, yet $22.07 would
    // rest. The restored ones are Safety-cancelled as the connection opens (decision 0080) and
    // the run places nothing beside them; were one still resting at the place, the same-side
    // refusal would hold it back (`place_refusal`, tested on its own).
    let (orders, body) = restored_as(1, "62000", "0.00009", "0.00009");
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder_with(
        Arc::clone(&placed),
        Arc::new(orders),
        End::Canceled,
        End::Canceled,
    );
    // Auth, four subscriptions, the arm and the Safety batch cancel of the restored orders.
    let mut script = vec![Step::Accept];
    script.extend((0..7).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes_with(body))
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.away_bps = 2000;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("Safety-cancelled as it opened (decision 0080), and shown ended"),
        "{printed}"
    );
    assert!(printed.contains("nothing placed"), "{printed}");
    // The one Safety batch cancel ended both restored orders; Stop sent none.
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 1),
        "{printed}"
    );
    assert!(!printed.contains("did not end cancelled"), "{printed}");
    assert!(!methods(&stub).iter().any(|m| m == "order.create"));
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn the_orders_of_ours_resting_on_a_side_are_those_open_on_it() {
    // A restored buy of 3 lots with 1 filled, a restored sell of 4, and an id the registry does
    // not hold: on each side, the restored order there with what of it rests.
    use fbc_core::{
        AccountKey, CidMatch, CidMint, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease, Side,
        VenueOrderSnapshot, VenueOrderState, WallNs,
    };
    use fbc_oms::{LadderConfig, OrderKey, Registry, ResyncSnapshot};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let (buy, sell, unheld) = (
        mint.mint().unwrap(),
        mint.mint().unwrap(),
        mint.mint().unwrap(),
    );
    let caps = fbc_venue_paradex::factory::caps();
    let shown = |cid, vid: &str, side, qty, filled| VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: fbc_core::dispatch(&caps, Namespace::new(1), |s| s.venue_order_id(vid)).unwrap(),
        inst: InstrumentId::new(1),
        side,
        state: VenueOrderState::Open,
        px: None,
        qty: Lots::new(qty).unwrap(),
        cum_filled: Lots::new(filled).unwrap(),
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
    let snap = ResyncSnapshot {
        watermark: WallNs(0),
        requested_at: MonoNs(0),
        orders: vec![
            shown(buy, "V-1", Side::Buy, 3, 1),
            shown(sell, "V-2", Side::Sell, 4, 0),
        ],
        positions: vec![],
    };
    let key = OrderKey {
        venue: Some(1),
        ingest: 1,
    };
    reg.resync(&ladder, &order_caps, &snap, key).unwrap();
    let owned = [(buy, Lots::ZERO), (sell, Lots::ZERO), (unheld, Lots::ZERO)];
    assert_eq!(
        trade::resting_on_side(&reg, &owned, Side::Buy),
        [(buy, Lots::new(2).unwrap())]
    );
    assert_eq!(
        trade::resting_on_side(&reg, &owned, Side::Sell),
        [(sell, Lots::new(4).unwrap())]
    );
}

#[tokio::test]
async fn an_order_not_ours_that_opened_and_ended_during_the_run_fails_it() {
    // Codex r4216297514: another system's order (a random UUID client id) opens and is cancelled
    // while the placed order's cancel is answered, after the audit before the place: by the
    // run's end it is no longer in view, but it was there, so the run fails.
    let uuid = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
    let vid = "1759500000000000998";
    let opened = order_open(4_000, vid, uuid);
    let closed = order_closed_on(2, 4_001, vid, uuid, "70000", "0.00001");
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder_extra(Arc::clone(&placed), vec![opened, closed]);
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
    assert!(
        printed.contains("an order not ours was reported during the run"),
        "{printed}"
    );
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn the_account_audit_counts_every_order_not_ours_reported_not_only_those_in_view() {
    use fbc_core::Namespace;
    use fbc_oms::{Registry, Routed};
    let reg = Registry::new();
    for routed in [
        Routed::Foreign(Namespace::new(2)),
        Routed::NotCanonical,
        Routed::Untracked,
    ] {
        let changes = trade::account_changes(&[link::Note::Order(routed)], &reg, None, &[]);
        assert_eq!(changes.len(), 1, "{changes:?}");
        assert!(
            changes[0].starts_with("an order not ours was reported during the run"),
            "{changes:?}"
        );
    }
}

#[test]
fn the_account_audit_counts_an_amend_of_ours_and_every_problem_the_link_noted() {
    // Codex r4226570452: the sample never amends, so an amend of one of our orders was another
    // process's (the sole-trader assumption broke). Codex r4226570456: a venue error after
    // login, an asynchronous reject, or an event the registry could not apply leaves the run
    // uncertain, as an undecodable frame does. Each fails the run.
    use fbc_core::{AccountKey, CidMint, Namespace, NamespaceLease, WallNs};
    use fbc_oms::{Applied, Registry, Routed};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let cid = CidMint::new(lease, 0, 0, WallNs(0)).mint().unwrap();
    let reg = Registry::new();
    let amended = link::Note::Order(Routed::Ours(cid, Applied::Amended));
    let changes = trade::account_changes(&[amended], &reg, None, &[]);
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert!(
        changes[0].starts_with("an order of ours was amended during the run"),
        "{changes:?}"
    );
    let problem = link::Note::Problem("venue error naming no request".to_owned());
    let changes = trade::account_changes(&[problem], &reg, None, &[]);
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert!(
        changes[0].starts_with("the run noted a problem: venue error naming no request"),
        "{changes:?}"
    );
    // An ordinary update of ours is not a change.
    let advanced = link::Note::Order(Routed::Ours(cid, Applied::Advanced));
    assert!(trade::account_changes(&[advanced], &reg, None, &[]).is_empty());
}

#[test]
fn a_refused_url_or_proxy_is_never_echoed() {
    // Codex r4216452827: a URL or proxy that carries a secret (a user and password, a token in
    // the query or the fragment) is refused naming its flag, never with what was typed. A
    // testnet URL with a query or a fragment is refused too, so none is printed later.
    let secret = "hunter2";
    for (flag, value) in [
        (
            "--rest-url",
            "https://user:hunter2@api.testnet.paradex.trade/v1",
        ),
        (
            "--ws-url",
            "wss://user:hunter2@ws.api.testnet.paradex.trade/v1",
        ),
        (
            "--rest-url",
            "https://api.prod.paradex.trade/v1?token=hunter2",
        ),
        ("--ws-url", "wss://ws.api.prod.paradex.trade/v1#hunter2"),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade/v1?token=hunter2",
        ),
        ("--ws-url", "wss://ws.api.testnet.paradex.trade/v1#hunter2"),
        (
            "--rest-url",
            "ftp://user:hunter2@api.testnet.paradex.trade/v1",
        ),
        ("--rest-url", "https://[hunter2/v1"),
        ("--socks5", "user:hunter2@proxy.example:x"),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, value]));
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(flag), "{flag} {value}: {err}");
        assert!(!err.contains(secret), "{flag} {value}: {err}");
    }
}

#[test]
fn the_fill_baseline_of_an_order_the_resync_restored_is_the_snapshots() {
    // Codex r4216452822: an order event can raise a restored order's fill in the registry
    // before the run takes the order on; the baseline is what the snapshot showed, so that
    // fill still counts as traded.
    use fbc_core::{
        AccountKey, CidMatch, CidMint, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease, Side,
        VenueOrderSnapshot, VenueOrderState, WallNs,
    };
    use fbc_oms::{LadderConfig, OrderKey, Registry, ResyncSnapshot};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let (a, other) = (mint.mint().unwrap(), mint.mint().unwrap());
    let caps = fbc_venue_paradex::factory::caps();
    let shown = |cid, vid: &str, inst, filled| VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: fbc_core::dispatch(&caps, Namespace::new(1), |s| s.venue_order_id(vid)).unwrap(),
        inst: InstrumentId::new(inst),
        side: Side::Sell,
        state: VenueOrderState::Open,
        px: None,
        qty: Lots::new(3).unwrap(),
        cum_filled: Lots::new(filled).unwrap(),
        post_only: None,
        reduce_only: None,
    };
    let snapshot = |filled| ResyncSnapshot {
        watermark: WallNs(0),
        requested_at: MonoNs(0),
        orders: vec![shown(a, "V-1", 1, filled), shown(other, "V-2", 2, 0)],
        positions: vec![],
    };
    // Only the market's orders: the other instrument's is not taken on.
    let first = snapshot(1);
    assert_eq!(
        trade::restored_baseline(&first),
        [(a, Lots::new(1).unwrap())]
    );
    // The registry has since seen 2 filled: the snapshot's 1 stays the baseline.
    let order_caps = caps.exec.clone().unwrap().order;
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    let mut reg = Registry::new();
    let key = OrderKey {
        venue: Some(1),
        ingest: 1,
    };
    reg.resync(&ladder, &order_caps, &snapshot(2), key).unwrap();
    let owned = trade::restored_baseline(&first);
    assert_eq!(trade::traded(&reg, &owned), [(a, Lots::new(2).unwrap())]);
}

#[test]
fn nothing_is_placed_while_the_current_connections_gate_is_closed() {
    // Codex r4216636810: a reconnect after the resync step leaves the new connection's gate
    // closed until its resync ends, and that resync has left no note for the account audit to
    // judge yet; the place is refused, not queued to go out when the gate opens.
    use fbc_core::{AccountKey, CidMint, Lots, Namespace, NamespaceLease, WallNs};
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let resting = [(mint.mint().unwrap(), Lots::new(9).unwrap())];
    let changed = ["a fill not of our orders came in".to_owned()];
    // The gate closed: refused whatever the audit and the side show.
    let closed = trade::place_refusal(false, 0, &[], &[]).expect("a closed gate refuses");
    assert!(
        closed.starts_with("the order socket's current connection takes no place yet"),
        "{closed}"
    );
    assert_eq!(
        trade::place_refusal(false, 2, &changed, &resting).as_deref(),
        Some(closed.as_str())
    );
    // Open: the audit, then an order of ours resting on the side, refuse it; nothing else.
    assert!(
        trade::place_refusal(true, 2, &changed, &resting)
            .unwrap()
            .starts_with("the account changed since the seed")
    );
    assert!(
        trade::place_refusal(true, 2, &[], &resting)
            .unwrap()
            .starts_with("1 orders of ours (9 lots) rest on the order's side")
    );
    // Orders of ours an earlier run left on the market hold it (decision 0080).
    assert!(
        trade::place_refusal(true, 2, &[], &[])
            .unwrap()
            .starts_with("2 orders of ours rest on the market from an earlier connection")
    );
    assert_eq!(trade::place_refusal(true, 0, &[], &[]), None);
}

#[tokio::test]
async fn the_testnet_line_names_the_hosts_never_the_urls() {
    // Codex r4216636821: an accepted URL may still carry a token in its path; the TESTNET line
    // names each host only.
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    // A path other than /v1 is refused before anything is printed or connects (Codex
    // r4217231772): no token in a path is ever sent or shown.
    let mut opts = options(&stub, "100");
    opts.ws_url = stub.ws_url("/v1/SYNTHETIC-path-token-hunter2");
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: trade::Out = buf.clone();
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(refused.starts_with("--ws-url"), "{refused}");
    assert!(!refused.contains("hunter2"), "{refused}");
    assert!(buf.borrow().is_empty());
    assert!(stub.http_requests().is_empty());
    let opts = options(&stub, "100");
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: trade::Out = buf.clone();
    // Refused before the session connects (below the minimum notional), after the line.
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    assert!(
        printed.contains(&format!(
            "TESTNET a loopback test stub, not Paradex: REST host 127.0.0.1 WS host 127.0.0.1 \
             chain {TESTNET_CHAIN}"
        )),
        "{printed}"
    );
    assert!(!printed.contains("hunter2"), "{printed}");
    assert!(!refused.contains("hunter2"), "{refused}");
}

#[test]
fn a_refused_chain_id_or_argument_is_never_echoed() {
    // Codex r4216636829 and Reviewer B's RB114-10: a private key pasted into PARADEX_CHAIN_ID,
    // as a stray argument or as a flag's value is refused naming the variable, the position
    // or the flag, never with what was typed.
    let key = "0xSYNTHETIC-pasted-secret-hunter2";
    let (rest, ws) = (args::TESTNET_REST, args::TESTNET_WS);
    let err = args::testnet_guard(rest, ws, Some(key)).unwrap_err();
    assert!(err.starts_with("PARADEX_CHAIN_ID"), "{err}");
    assert!(err.contains("not the testnet chain id"), "{err}");
    assert!(!err.contains(key), "{err}");
    let mut argv = strings(&MARKET_ARGS);
    argv.push(key.to_owned());
    let err = args::parse(argv).unwrap_err();
    assert!(err.starts_with("argument 22"), "{err}");
    assert!(!err.contains(key), "{err}");
    for flag in [
        "--tick",
        "--min-notional",
        "--resting-cap-usd",
        "--away-bps",
        "--namespace",
        "--side",
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, key]));
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(flag), "{flag}: {err}");
        assert!(!err.contains(key), "{flag}: {err}");
    }
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--market", "0x0123/not-a-market"]));
    let err = args::parse(argv).unwrap_err();
    assert!(err.starts_with("--market"), "{err}");
    assert!(!err.contains("0x0123"), "{err}");
    // An unknown flag is still named: it starts with '-', so it is a flag, not a value.
    let mut argv = strings(&MARKET_ARGS);
    argv.push("--no-such-flag".to_owned());
    assert!(
        args::parse(argv)
            .unwrap_err()
            .starts_with("unknown argument --no-such-flag")
    );
    // Reviewer B's RB114-15: a flag typed with its value after '=' is named by the flag part
    // only; an argument that starts with '-' but is not shaped like a flag by its position.
    for (typed, flag) in [
        (
            "--socks5=SYNTHETIC-user:hunter2@proxy.example:1080",
            "--socks5",
        ),
        (
            "--private-key=0xSYNTHETIC-pasted-secret-hunter2",
            "--private-key",
        ),
        (
            "--rest-url=https://api.testnet.paradex.trade/hunter2",
            "--rest-url",
        ),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.push(typed.to_owned());
        let err = args::parse(argv).unwrap_err();
        assert!(
            err.starts_with(&format!("unknown argument {flag} ")),
            "{typed}: {err}"
        );
        assert!(!err.contains("hunter2"), "{typed}: {err}");
    }
    for typed in ["-0xSYNTHETIC-hunter2", "--SYNTHETIC0hunter2", "--x/hunter2"] {
        let mut argv = strings(&MARKET_ARGS);
        argv.push(typed.to_owned());
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with("argument 22"), "{typed}: {err}");
        assert!(!err.contains("hunter2"), "{typed}: {err}");
    }
}

#[test]
fn a_market_not_shaped_like_a_paradex_market_is_refused_before_any_request() {
    // Codex r4216890750: a private key pasted as --market would be sent in GET /orderbook's
    // path; only a Paradex perpetual's shape (BASE-QUOTE-PERP, letters and digits, at most 32
    // long) is taken, and a refusal never echoes the value.
    // Reviewer B's RB114-14: some Paradex markets carry a lower-case prefix (kBONK-USD-PERP);
    // the letters are md_watch's Paradex spelling, either case.
    for good in [
        "BTC-USD-PERP",
        "ETH-USD-PERP",
        "kBONK-USD-PERP",
        "kPEPE-USD-PERP",
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&["--market", good]));
        let Ok(Parsed::Trade(opts)) = args::parse(argv) else {
            panic!("{good} is a market");
        };
        assert_eq!(opts.market, good);
    }
    for bad in [
        "0xSYNTHETIC0hunter2",
        "0X0123456789ABCDEF0123456789ABCDEF",
        "0123456789ABCDEF0123456789ABCDEF",
        "ETH-USD",
        "BTC-USD-PERP/",
        "BTC_USD-PERP",
        "BTC--PERP",
        "-BTC-USD-PERP",
        "BTC-USD-0123456789ABCDEF0123456789ABCDEF",
        // Codex r4226495325: a credential split into parts of at most 12 is still refused, by
        // the count of parts (at most 5) and the whole length (at most 32).
        "0123456789AB-CDEF01234567-89ABCDEF0123-456789ABCDEF",
        "0123-4567-89AB-CDEF-0123-4567-89AB-CDEF-0123-4567-89AB-CDEF-0123-4567-89AB-CDEF",
        "BTC-USD-PERP-A-B-C",
        // Codex r4226570445: the spec is built as a perpetual's, so an option, a future or a
        // spot market is refused.
        "BTC-USD-27JUN25-100000-C",
        "BTC-USD-27JUN25",
        "ETH-USD-SPOT",
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&["--market", bad]));
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with("--market"), "{bad}: {err}");
        assert!(!err.contains(bad), "{bad}: {err}");
    }
}

#[tokio::test]
async fn a_frame_the_codec_could_not_decode_fails_the_run() {
    // Codex r4217056359: a frame the codec could not decode may have held a fill, a position
    // or an order event the run never saw; the round trip otherwise whole, the run fails.
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder_extra(Arc::clone(&placed), vec![vec![0xff, 0x00, 0x13]]);
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
    assert!(
        printed.contains("NOTE 1 frames or HTTP answers could not be decoded"),
        "{printed}"
    );
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[test]
fn a_secret_shaped_proxy_host_or_a_relative_lease_directory_is_refused_unshown() {
    // Codex r4217056333 and r4217056342: a key pasted as the --socks5 host would be resolved
    // through DNS, and one pasted as --lease-dir would become a directory in the checkout and
    // appear in errors; neither is taken, and the refusal never shows it.
    // Built at run time: no key-shaped literal sits in this crate.
    let hex64 = "0123456789abcdef".repeat(4);
    for value in [
        format!("0x{hex64}:1080"),
        format!("{hex64}:1080"),
        "proxy_host!:1080".to_owned(),
        "-proxy.example:1080".to_owned(),
        "proxy..example:1080".to_owned(),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(["--socks5".to_owned(), value.clone()]);
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with("--socks5"), "{value}: {err}");
        assert!(!err.contains(&value[..8]), "{value}: {err}");
    }
    for good in ["proxy.example:1080", "10.0.0.1:1080", "[::1]:1080"] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&["--socks5", good]));
        assert!(args::parse(argv).is_ok(), "{good}");
    }
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&[
        "--lease-dir",
        "0xSYNTHETIC-pasted-secret-hunter2",
    ]));
    let err = args::parse(argv).unwrap_err();
    assert!(err.starts_with("--lease-dir"), "{err}");
    assert!(!err.contains("hunter2"), "{err}");
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--lease-dir", "/var/tmp/testnet_trade"]));
    let Ok(Parsed::Trade(opts)) = args::parse(argv) else {
        panic!("an absolute --lease-dir is taken");
    };
    assert_eq!(opts.lease_dir, PathBuf::from("/var/tmp/testnet_trade"));
}

#[test]
fn a_testnet_url_is_exactly_the_testnet_base_and_a_stub_path_exactly_v1() {
    // Codex r4217231772 and Reviewer B's RB114-13: whatever sits in a URL's path is sent to the
    // venue (in the REST requests' paths and the WebSocket upgrade), so a testnet URL must be
    // exactly Paradex's and a loopback stub's path exactly /v1, on both flags; the refusal
    // names the flag and never shows the URL.
    let secret = "0xSYNTHETIC-pasted-secret-hunter2";
    let (rest, ws) = (args::TESTNET_REST, args::TESTNET_WS);
    let (stub_rest, stub_ws) = ("http://127.0.0.1:9/v1", "ws://127.0.0.1:9/v1");
    assert_eq!(
        args::testnet_guard(rest, ws, None),
        Ok(args::Target::Testnet)
    );
    for (r, w) in [
        (stub_rest, stub_ws),
        ("https://[::1]:9/v1", "wss://[::1]:9/v1"),
        ("http://127.0.0.2/v1", "ws://127.255.255.254/v1"),
    ] {
        assert_eq!(
            args::testnet_guard(r, w, None),
            Ok(args::Target::LoopbackStub),
            "{r} {w}"
        );
    }
    for (flag, r, w) in [
        (
            "--rest-url",
            format!("https://api.testnet.paradex.trade/{secret}/v1"),
            ws.to_owned(),
        ),
        (
            "--rest-url",
            format!("https://api.testnet.paradex.trade/v1/{secret}"),
            ws.to_owned(),
        ),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade/v1/".to_owned(),
            ws.to_owned(),
        ),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade:443/v1".to_owned(),
            ws.to_owned(),
        ),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade".to_owned(),
            ws.to_owned(),
        ),
        (
            "--ws-url",
            rest.to_owned(),
            format!("wss://ws.api.testnet.paradex.trade/{secret}"),
        ),
        (
            "--ws-url",
            rest.to_owned(),
            format!("wss://ws.api.testnet.paradex.trade/v1/{secret}"),
        ),
        (
            "--ws-url",
            rest.to_owned(),
            "wss://ws.api.testnet.paradex.trade".to_owned(),
        ),
        (
            "--rest-url",
            format!("http://127.0.0.1:9/{secret}/v1"),
            stub_ws.to_owned(),
        ),
        (
            "--rest-url",
            "http://127.0.0.1:9/".to_owned(),
            stub_ws.to_owned(),
        ),
        (
            "--rest-url",
            "http://127.0.0.1:9x/v1".to_owned(),
            stub_ws.to_owned(),
        ),
        (
            "--ws-url",
            stub_rest.to_owned(),
            format!("ws://127.0.0.1:9/v1/{secret}"),
        ),
        (
            "--ws-url",
            stub_rest.to_owned(),
            "ws://127.0.0.1:9".to_owned(),
        ),
    ] {
        let err = args::testnet_guard(&r, &w, None).unwrap_err();
        assert!(err.starts_with(flag), "{r} {w}: {err}");
        assert!(!err.contains("hunter2"), "{r} {w}: {err}");
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(["--rest-url".to_owned(), r.clone(), "--ws-url".to_owned(), w]);
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(flag), "{r}: {err}");
        assert!(!err.contains("hunter2"), "{r}: {err}");
    }
}

#[test]
fn a_refused_url_never_names_its_host() {
    // Codex r4217231780 and Reviewer B's RB114-12: a key pasted as a URL's host (with or
    // without 0x; a Stark key without 0x fits in one DNS label) is refused naming the flag and
    // the expected URL only, never the host typed.
    // Built at run time: no key-shaped literal sits in this crate.
    let hex64 = "0123456789abcdef".repeat(4);
    let shown = &hex64[1..9];
    for host in [
        format!("0x{hex64}"),
        hex64[1..].to_owned(),
        format!("{}.example", &hex64[1..]),
        format!("{}:9", &hex64[1..]),
    ] {
        for (flag, r, w) in [
            (
                "--rest-url",
                format!("https://{host}/v1"),
                args::TESTNET_WS.to_owned(),
            ),
            (
                "--ws-url",
                args::TESTNET_REST.to_owned(),
                format!("wss://{host}/v1"),
            ),
        ] {
            let err = args::testnet_guard(&r, &w, None).unwrap_err();
            assert!(err.starts_with(flag), "{r} {w}: {err}");
            assert!(!err.contains(shown), "{r} {w}: {err}");
        }
    }
}

#[tokio::test]
async fn the_namespace_lease_is_held_until_the_run_ends() {
    // Reviewer B's RB114-7: the namespace lease stays held for the whole run, not only until
    // the client-id mark is kept, so another testnet_trade with the same --namespace and
    // --lease-dir on another market is refused while this one trades.
    use fbc_core::{AccountKey, Namespace, NamespaceLease};
    let placed = Arc::new(Mutex::new(Placed::default()));
    let dir = lease_dir();
    let probed = Arc::new(Mutex::new(None::<bool>));
    let inner = respond_full(
        Arc::clone(&placed),
        Arc::new(Vec::new()),
        End::Canceled,
        End::Canceled,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
    );
    let (at, seen) = (dir.clone(), Arc::clone(&probed));
    let with = Responder::new(move |frame| {
        if let Frame::Text(text) = frame
            && text.contains("\"order.cancel\"")
        {
            // Long after the mark was kept: another run's take of the namespace.
            let free = NamespaceLease::acquire(&at, AccountKey::new(1), Namespace::new(1)).is_ok();
            *seen.lock().unwrap() = Some(free);
        }
        inner(frame)
    });
    let mut script = vec![Step::Accept];
    script.extend((0..8).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.lease_dir = dir.clone();
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(*probed.lock().unwrap(), Some(false), "{printed}");
    // Released once the run ended.
    assert!(NamespaceLease::acquire(&dir, AccountKey::new(1), Namespace::new(1)).is_ok());
}

/// As [`routes`], the login answered with `login` and the order book with `book`.
fn routes_answering(login: HttpReply, book: Vec<u8>) -> HttpRouter {
    let empty = r#"{"results":[]}"#;
    HttpRouter::new()
        .route(Method::POST, PathPattern::exact("/v1/auth"), login)
        .route(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            reply(200, book),
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
        )
}

/// The fixture's order book.
fn book() -> Vec<u8> {
    fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap()
}

/// No credential, and not the session token, in `printed`.
fn no_credential_in(printed: &str) {
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
async fn three_refused_logins_stop_the_run_at_once_naming_the_status_code_and_message() {
    // The owner's first testnet runs (2026-10-09): every login was refused (HTTP 401,
    // STARKNET_SIGNATURE_VERIFICATION_FAILED: the sample signed for a stale chain id), the
    // socket reconnected five times, and the run stopped on 'TIMEOUT login' saying nothing of
    // why. Now each connection's end names the refusal, and the third refusal in a row stops
    // the run at once, long before the step timeout. The refusal echoes the account, as some
    // of Paradex's do: it is withheld.
    let (account, _) = synthetic();
    let body = json!({
        "error": "STARKNET_SIGNATURE_VERIFICATION_FAILED",
        "message": format!("verification failed on Curve for account {account}"),
    });
    let routes = routes_answering(reply(401, body.to_string()), book());
    let script = WsScript::new(vec![Step::Accept, Step::Accept, Step::Accept]);
    let stub = StubServer::start(script, routes).await.unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 40;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the run waited for its step timeout:\n{printed}"
    );
    assert!(!report.ok, "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert_eq!(steps(&printed), ["stop"], "{printed}");
    let refused = "the Paradex login was refused: HTTP 401, \
                   STARKNET_SIGNATURE_VERIFICATION_FAILED: verification failed on Curve for \
                   account <withheld>";
    assert!(
        printed.contains(&format!("NOTE login refused 3 times: {refused}")),
        "{printed}"
    );
    // The usual causes are named with it.
    for cause in [
        "a mainnet key on testnet",
        "an Ethereum address",
        "not onboarded",
    ] {
        assert!(printed.contains(cause), "{cause}:\n{printed}");
    }
    // Each connection that ended before the stop says why.
    for epoch in 0..2 {
        let ended = format!("connection ConnKey {{ conn: 0, epoch: {epoch} }} ended: {refused}");
        assert!(printed.contains(&ended), "{ended}:\n{printed}");
    }
    // Nothing was written on any socket: no login gave a token for the auth frame.
    assert!(methods(&stub).is_empty(), "{:?}", methods(&stub));
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_refused_auth_frame_is_shown_with_its_code_and_never_the_token_it_echoes() {
    // Reviewer B's RB-3 on PR #135: the auth frame's refusal is the venue's JSON-RPC message
    // as received, and that frame carries the bearer token, which a refusal may echo. The run
    // shows the refusal's code and words, never a word that could hold the token.
    let refuse = Responder::new(|frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary frame".to_owned());
        };
        let req: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let error = json!({"code": 40110, "message": format!("invalid bearer {TOKEN}")});
        let refused = json!({"jsonrpc": "2.0", "error": error, "id": req["id"]});
        Ok(vec![Frame::text(refused.to_string())])
    });
    let mut script = Vec::new();
    for conn in 0..3 {
        script.push(Step::Accept);
        script.push(Step::Respond {
            conn,
            with: refuse.clone(),
        });
    }
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 40;
    let (report, printed) = run_against(&opts).await;
    assert!(!report.ok, "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert!(
        printed.contains("NOTE login refused 3 times: 40110: invalid bearer <withheld>"),
        "{printed}"
    );
    assert_eq!(methods(&stub), ["auth", "auth", "auth"]);
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_connection_the_venue_closed_is_named_and_the_run_goes_on_on_the_next() {
    // The venue closes the first connection as it opens: the session reconnects, and the
    // round trip completes on the second. The run says the first ended, and that the runtime
    // does not say why (FBC-dmlw).
    let placed = Arc::new(Mutex::new(Placed::default()));
    let with = responder(Arc::clone(&placed));
    let mut script = vec![Step::Accept, Step::Close { conn: 0 }, Step::Accept];
    script.extend((0..8).map(|_| Step::Respond {
        conn: 1,
        with: with.clone(),
    }));
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(
        printed.contains(
            "NOTE the order socket's connection ConnKey { conn: 0, epoch: 0 } ended: the socket \
             closed or failed, a write stalled, or the login could not be signed (fbc-runtime \
             does not say which yet, FBC-dmlw)"
        ),
        "{printed}"
    );
    assert!(report.ok, "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 1),
        "{printed}"
    );
}

#[test]
fn a_venue_error_is_shown_with_every_word_that_could_hold_a_secret_withheld() {
    // Kept: the codec's sentence, Paradex's code, an HTTP status and a short JSON-RPC code.
    let sentence = "the Paradex login was refused: HTTP 401, \
                    STARKNET_SIGNATURE_VERIFICATION_FAILED: verification failed on Curve";
    assert_eq!(link::shown(sentence), sentence);
    assert_eq!(link::shown("code -32600"), "code -32600");
    // Withheld: a token, a long word, hex, digits mixed with letters; control characters
    // become spaces.
    // A token's shape (dotted parts), built at run time: no token-shaped literal sits here.
    let token = format!("{0}.{0}.{0}", "SyntheticPart");
    assert_eq!(
        link::shown(&format!("invalid bearer {token}")),
        "invalid bearer <withheld>"
    );
    assert_eq!(
        link::shown("key 0x1a2b3c4d and abcdef and a1b2c3 and 12345678"),
        "key <withheld> and <withheld> and <withheld> and <withheld>"
    );
    assert_eq!(link::shown("bad\nline\tbreak"), "bad line break");
    let long = "word ".repeat(100);
    assert!(link::shown(&long).ends_with("..."));
    assert!(link::shown(&long).chars().count() <= 203);
    // A venue error: its kind, its code (when one is plain) and its words.
    let reject = |code: Option<&str>, raw: &str| fbc_core::Reject {
        kind: fbc_core::RejectKind::Other,
        venue_code: code.map(Into::into),
        raw: raw.into(),
    };
    assert_eq!(link::described(&reject(None, sentence)), sentence);
    assert_eq!(
        link::described(&reject(
            Some("STARKNET_SIGNATURE_VERIFICATION_FAILED"),
            sentence
        )),
        sentence
    );
    assert_eq!(
        link::described(&reject(Some("40110"), &format!("invalid bearer {TOKEN}"))),
        "40110: invalid bearer <withheld>"
    );
    assert_eq!(
        link::described(&reject(Some(&"A".repeat(80)), "refused")),
        "<withheld>: refused"
    );
}

/// The fixture's order book with `side` (`bids` or `asks`) emptied, as Paradex's testnet
/// answers a market where nobody rests on that side (`"asks":[]`).
fn one_sided(side: &str) -> Vec<u8> {
    let mut book: Value = serde_json::from_slice(&book()).unwrap();
    book[side] = json!([]);
    for key in [
        "best_ask_api",
        "best_ask_interactive",
        "best_bid_api",
        "best_bid_interactive",
    ] {
        if key.contains(&side[..3]) {
            book.as_object_mut().unwrap().remove(key);
        }
    }
    book.to_string().into_bytes()
}

#[test]
fn a_buy_needs_only_the_bid_side_and_a_sell_only_the_ask_side() {
    // The owner's testnet BTC and ETH books have bids but no asks (2026-10-09). A buy rests
    // behind the best bid and a sell behind the best ask: the other side is not needed, and
    // the inventory cap is then at the higher of the order's price and the bid (Codex
    // r4226246459: a buy's own price is below the bid, so a position would be worth more than
    // the cap at the market).
    let opts_on = |side: OrderSide| {
        let Ok(Parsed::Trade(mut opts)) = args::parse(strings(&MARKET_ARGS)) else {
            panic!("the market flags parse");
        };
        opts.side = side;
        *opts
    };
    let book_of = |bytes: Vec<u8>, opts: &Options| {
        let specs = trade::specs(
            &fbc_venue_paradex::ParadexFactory,
            &trade::config(opts),
            opts,
        )
        .unwrap();
        fbc_venue_paradex::md::rest::decode_orderbook(&bytes, &specs).unwrap()
    };
    let buy = opts_on(OrderSide::Buy);
    let priced = trade::price(&buy, &book_of(one_sided("asks"), &buy)).unwrap();
    // 3% under the best bid of 62000.2, floored onto the tick; $50 at the bid is 80 lots, not
    // the 83 it is at the order's price.
    assert_eq!(priced.px, Decimal::from_str("60140.1").unwrap());
    assert_eq!(
        (priced.bid, priced.ask),
        (Some(Decimal::from_str("62000.2").unwrap()), None)
    );
    assert_eq!(priced.inventory.get(), 80);
    let sell = opts_on(OrderSide::Sell);
    let priced = trade::price(&sell, &book_of(one_sided("bids"), &sell)).unwrap();
    // 3% over the best ask of 62000.5, ceiled onto the tick.
    assert_eq!(priced.px, Decimal::from_str("63860.6").unwrap());
    assert_eq!(
        (priced.bid, priced.ask),
        (None, Some(Decimal::from_str("62000.5").unwrap()))
    );
    // The side the order rests behind is needed.
    let err = trade::price(&buy, &book_of(one_sided("bids"), &buy))
        .err()
        .unwrap();
    assert!(err.contains("no bid"), "{err}");
    let err = trade::price(&sell, &book_of(one_sided("asks"), &sell))
        .err()
        .unwrap();
    assert!(err.contains("no ask"), "{err}");
}

#[tokio::test]
async fn a_buy_on_a_book_with_no_asks_places_and_cancels_as_on_a_full_book() {
    let placed = Arc::new(Mutex::new(Placed::default()));
    let routes = routes_answering(
        reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#)),
        one_sided("asks"),
    );
    let stub = StubServer::start(script(Arc::clone(&placed)), routes)
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(printed.contains("BBO bid 62000.2 ask none"), "{printed}");
    assert!(
        printed.contains("BBO again bid 62000.2 ask none"),
        "{printed}"
    );
    // The inventory cap at the bid, above the order's price: $50 at 62000.2 is 80 lots.
    assert!(
        printed.contains("caps: resting 33 lots per side, inventory 80 lots"),
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 1),
        "{printed}"
    );
}

#[test]
fn the_inventory_cap_is_at_the_highest_of_the_orders_price_and_the_touch() {
    let d = |v: &str| Decimal::from_str(v).unwrap();
    // A buy below a bid with no ask: the bid (Codex r4226246459).
    assert_eq!(
        trade::inventory_price(d("104.5"), Some(d("110")), None),
        d("110")
    );
    // A buy on a full book: the ask.
    assert_eq!(
        trade::inventory_price(d("104.5"), Some(d("110")), Some(d("110.1"))),
        d("110.1")
    );
    // A sell above the ask, with or without a bid: its own price.
    assert_eq!(
        trade::inventory_price(d("115.6"), None, Some(d("110.1"))),
        d("115.6")
    );
    assert_eq!(
        trade::inventory_price(d("115.6"), Some(d("110")), Some(d("110.1"))),
        d("115.6")
    );
}

#[test]
fn the_default_lease_directory_is_under_an_absolute_home_only() {
    // Codex r4226246468: a relative HOME would put the leases and the client-id mark under
    // the directory the run starts in, so two runs started in different directories would
    // not exclude each other. It is refused, as a relative --lease-dir is.
    assert_eq!(
        args::lease_dir_under(Some("/home/trader".into())),
        Ok(PathBuf::from("/home/trader/.fueledbychai/testnet_trade"))
    );
    for home in [Some("home/trader".into()), Some("".into()), None] {
        let err = args::lease_dir_under(home).unwrap_err();
        assert!(err.contains("give --lease-dir"), "{err}");
    }
    let err = args::lease_dir_under(Some("trader".into())).unwrap_err();
    assert!(err.contains("not an absolute path"), "{err}");
    // The value itself is never shown.
    assert!(!err.contains("trader"), "{err}");
}

#[test]
fn a_stub_host_is_a_numeric_loopback_address_never_a_name() {
    // Codex r4217903067: 'localhost' is a name a resolver may map anywhere, so it is not
    // taken as a stub on this machine: only 127.0.0.0/8 and [::1] are.
    for (r, w) in [
        ("http://localhost:9/v1", "ws://localhost:9/v1"),
        ("http://LOCALHOST:9/v1", "ws://127.0.0.1:9/v1"),
        ("http://127.0.0.1:9/v1", "ws://localhost:9/v1"),
    ] {
        assert!(args::testnet_guard(r, w, None).is_err(), "{r} {w}");
    }
    assert_eq!(
        args::testnet_guard("http://127.1.2.3:9/v1", "ws://[::1]:9/v1", None),
        Ok(args::Target::LoopbackStub)
    );
}

#[test]
fn a_bracketed_ipv6_proxy_is_kept_without_its_brackets() {
    // Codex r4217903060: the connector resolves (host, port), which takes ::1 but not [::1].
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--socks5", "[::1]:1080"]));
    let Ok(Parsed::Trade(opts)) = args::parse(argv) else {
        panic!("a bracketed IPv6 proxy is taken");
    };
    assert_eq!(
        opts.proxy,
        ProxyConfig::Socks5 {
            host: "::1".to_owned(),
            port: 1080
        }
    );
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--socks5", "proxy.example:1080"]));
    let Ok(Parsed::Trade(opts)) = args::parse(argv) else {
        panic!("a named proxy is taken");
    };
    assert_eq!(
        opts.proxy,
        ProxyConfig::Socks5 {
            host: "proxy.example".to_owned(),
            port: 1080
        }
    );
}
