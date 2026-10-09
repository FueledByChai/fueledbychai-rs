//! What Paradex's conformance fixtures (`fixtures/paradex/conformance`) assume (FBC-6oj): two
//! perpetuals, BTC-USD-PERP and ETH-USD-PERP, on a 0.1 price tick and a 0.001 size step; the
//! configuration and the synthetic account and key of `fixtures/paradex/signing`; the golden
//! commands, each signed at its Java vector's timestamp ([`golden_table`]); the bootstrap that
//! takes a fresh order-entry codec to its authenticated state (the login's answer, then the
//! auth frame's reply); and how Paradex's order entry answers over the stub server
//! ([`order_entry`]): the login and a flat REST resync over HTTP, the opening's frames by
//! method, and each order request as Paradex's WebSocket API answers it, an accepted
//! placement or amend followed by its `OrderEvent` (SBE 1:2).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use fbc_conformance::suite::{
    Answer, BatchFailures, BootReply, Bootstrap, Golden, OrderEntryStub, RefusedItem, Replier,
    Setup, UnansweredItem,
};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responded, Responder, StubServer,
};
use fbc_core::{
    AccountKey, AmendOrder, CancelOrder, Channel, CidMint, ClientIdFormat, ClientOrderId,
    EncodeCtx, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock,
    OrderKind, OrderRef, RpcId, Secret, Secrets, Side, SpecTable, Ticks, Tif, VenueCommand,
    VenueConfig, VenueOrderId, WallNs, dispatch, encode_cid,
};
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{
    ACCOUNT_ADDRESS, CHAIN_ID, REFRESH, REST_URL, SIGNATURE_LIFETIME, SIGNING_KEY, TIMEOUT,
};
use fbc_venue_paradex::exec::CONTROL_IDS;
use fbc_venue_paradex::factory::{EXEC_MODE, EXEC_STREAM, EXEC_URL, MD_URL, RPC_TIMEOUT, caps};
use serde_json::{Value, json};

use crate::common::Vectors;
use crate::md::{self, BTC, ETH};

/// Paradex's conformance fixture directory.
pub const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../fixtures/paradex/conformance"
);

/// The session token the login gives: made-up text that never looks like a JWT.
pub const TOKEN: &str = "SYNTHETIC.session-token.conformance";

/// BTC-USD-PERP (the instrument the order-entry checks trade) and ETH-USD-PERP.
pub fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(md::spec(BTC, "BTC-USD-PERP"));
    table.insert(md::spec(ETH, "ETH-USD-PERP"));
    table
}

/// The configuration: URLs no check connects to (the order-entry checks point them at the
/// stub), the chain id of the Java vectors, and the timings.
pub fn cfg() -> VenueConfig {
    let chain = Vectors::read().header["chain_id"].clone();
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (MD_URL, "wss://conformance.invalid/v1"),
        (EXEC_URL, "wss://conformance.invalid/v1"),
        (EXEC_MODE, "orders"),
        (RPC_TIMEOUT, "5000ms"),
        (REST_URL, "https://conformance.invalid/v1"),
        (CHAIN_ID, chain.as_str()),
        (SIGNATURE_LIFETIME, "3600s"),
        (REFRESH, "60s"),
        (TIMEOUT, "5000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// The synthetic account and key of `fixtures/paradex/signing`, read from the vectors' header.
pub fn creds() -> Secrets {
    let vectors = Vectors::read();
    let mut creds = Secrets::new();
    creds.insert(
        ACCOUNT_ADDRESS,
        Secret::new(vectors.header["account"].clone()),
    );
    creds.insert(SIGNING_KEY, Secret::new(vectors.header["key"].clone()));
    creds
}

/// What the fixtures assume.
pub fn assumed() -> Setup {
    Setup {
        specs: specs(),
        cfg: cfg(),
        creds: creds(),
        goldens: goldens(),
        exec_stream: EXEC_STREAM,
        order_entry: Some(order_entry()),
        bootstrap: Some(bootstrap()),
    }
}

/// The login's answer, then the auth frame's reply (the codec's first control id).
pub fn bootstrap() -> Bootstrap {
    Bootstrap {
        replies: vec![
            BootReply::Http {
                tag: None,
                status: 200,
                headers: Vec::new(),
                body: format!(r#"{{"jwt_token":"{TOKEN}"}}"#).into_bytes(),
            },
            BootReply::Text(ok(json!(CONTROL_IDS), json!({}))),
        ],
    }
}

fn ok(id: Value, result: Value) -> String {
    json!({"jsonrpc": "2.0", "result": result, "id": id}).to_string()
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// How Paradex's order entry answers over the stub server (module documentation).
pub fn order_entry() -> OrderEntryStub {
    let http = HttpRouter::new()
        .route_fn(Method::POST, PathPattern::exact("/v1/auth"), |_| {
            reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#))
        })
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), |_| {
            reply(200, r#"{"results":[]}"#)
        })
        .route_fn(Method::GET, PathPattern::exact("/v1/positions"), |_| {
            reply(200, r#"{"results":[]}"#)
        });
    // auth, four subscriptions and the cancel-on-disconnect arm, answered by method.
    let opening = (0..6).map(|_| opener()).collect();
    OrderEntryStub {
        point: point_at,
        http,
        opening,
        reply: replier(),
        // Decision 0069: a refused item's error carries no code, and `order.create_batch`
        // answers in one `results` list, so both are `Unknown` from the reply (decision 0089).
        batch: BatchFailures {
            refused: RefusedItem::Unknown,
            unanswered: UnansweredItem::InReply,
        },
    }
}

/// The order-entry socket and the REST base at the stub.
pub fn point_at(cfg: &mut VenueConfig, stub: &StubServer) {
    cfg.insert(EXEC_URL, &stub.ws_url("/v1"));
    cfg.insert(REST_URL, &stub.http_url("/v1"));
}

fn request(frame: &Frame) -> Result<Value, String> {
    let Frame::Text(text) = frame else {
        return Err("a binary frame from the client".into());
    };
    serde_json::from_str(text).map_err(|e| e.to_string())
}

fn opener() -> Responder {
    Responder::new(|frame| {
        let req = request(frame)?;
        let id = req["id"].clone();
        match req["method"].as_str().unwrap_or("") {
            "auth" | "subscribe" => Ok(vec![Frame::text(ok(id, json!({})))]),
            "order.cancel_on_disconnect" => Ok(vec![Frame::text(ok(id, json!({"enabled": true})))]),
            other => Err(format!("an opening frame of method {other}")),
        }
    })
}

/// One order the stub's venue accepted: what its order events restate.
#[derive(Clone, Debug)]
struct Order {
    cid: String,
    market: String,
    side: String,
    instruction: String,
    reduce_only: bool,
}

impl Order {
    fn of(params: &Value) -> Order {
        let text = |k: &str| params[k].as_str().unwrap_or_default().to_owned();
        let reduce_only = params["flags"]
            .as_array()
            .is_some_and(|f| f.iter().any(|v| v == "REDUCE_ONLY"));
        Order {
            cid: text("client_id"),
            market: text("market"),
            side: text("side"),
            instruction: text("instruction"),
            reduce_only,
        }
    }
}

/// What the stub's venue remembers between requests: each order it accepted by its venue id,
/// its last venue id and its last order-event sequence.
#[derive(Default)]
struct Book {
    orders: HashMap<String, Order>,
    last_vid: u64,
    seq: i64,
}

impl Book {
    fn accept(&mut self, params: &Value) -> (String, Order) {
        self.last_vid += 1;
        let vid = format!("17595000000000{:05}", self.last_vid);
        let order = Order::of(params);
        self.orders.insert(vid.clone(), order.clone());
        (vid, order)
    }

    fn seq(&mut self) -> i64 {
        self.seq += 1;
        5_000 + self.seq
    }
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d: rust_decimal::Decimal = text.parse().unwrap_or_default();
    let d = d * rust_decimal::Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap_or(i64::MIN)
}

/// What an order event reports beside the order: its status (3 OPEN) and its request_info
/// (status 4 SUCCESS, type 1 MODIFY_ORDER; 0 0 for none).
struct Report {
    status: u8,
    request: (u8, u8),
}

/// An `OrderEvent` (template 20) at schema 1:2 in its 128-byte block layout, as
/// `fixtures/paradex/exec/README.md` lays it out: `order` at `price` for `size`, nothing filled.
fn order_event(vid: &str, order: &Order, price: &str, size: &str, seq: i64, r: Report) -> Vec<u8> {
    let ts = 1_759_500_000_205_011i64;
    let side = if order.side == "SELL" { 2u8 } else { 1 };
    let tif = match order.instruction.as_str() {
        "IOC" => 2u8,
        "POST_ONLY" => 3,
        "RPI" => 4,
        _ => 1,
    };
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([r.status, side, 1, tif]); // the status, the side, LIMIT, the instruction
    f.extend(e8(price).to_le_bytes());
    f.extend(i64::MIN.to_le_bytes()); // triggerPrice: null
    f.extend(e8(size).to_le_bytes()); // size
    f.extend(e8(size).to_le_bytes()); // sizeOpen: nothing filled
    f.extend(i64::MIN.to_le_bytes()); // avgFillPrice: null
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend(0xa0u8..=0xbf); // account: 32 made-up bytes
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    f.push(0); // stp
    f.push(u8::from(order.reduce_only)); // flags: REDUCE_ONLY is bit 0
    f.extend([r.request.0, r.request.1]);
    let request_id = if r.request == (0, 0) { "" } else { "req-1" };
    for s in [vid, &order.cid, &order.market, "", request_id, ""] {
        f.push(u8::try_from(s.len()).expect("a short string"));
        f.extend(s.as_bytes());
    }
    f
}

pub fn replier() -> Replier {
    let book = Arc::new(Mutex::new(Book::default()));
    Replier::new(move |frame, answers| {
        let mut book = book.lock().unwrap_or_else(|e| e.into_inner());
        answer(&mut book, &request(frame)?, answers)
    })
}

/// The order `order.create` echoes, as `fixtures/paradex/exec/reply-create.json` shows it.
fn echo(vid: &str, p: &Value, status: &str) -> Value {
    json!({
        "id": vid, "client_id": p["client_id"], "market": p["market"], "side": p["side"],
        "type": p["type"], "instruction": p["instruction"], "price": p["price"],
        "size": p["size"], "remaining_size": p["size"], "status": status,
    })
}

fn text_of(p: &Value, key: &str) -> String {
    p[key].as_str().unwrap_or("0").to_owned()
}

fn answer(book: &mut Book, req: &Value, answers: &[Answer]) -> Responded {
    let id = req["id"].clone();
    let p = &req["params"];
    let refuse = |code: &str| -> Responded {
        let code: i64 = code
            .parse()
            .map_err(|_| format!("{code} is no JSON-RPC error code"))?;
        let error = json!({"code": code, "message": "synthetic: refused"});
        let frame = json!({"jsonrpc": "2.0", "error": error, "id": id});
        Ok(vec![Frame::text(frame.to_string())])
    };
    let one = |answers: &[Answer]| match answers {
        [only] => Ok(only.clone()),
        _ => Err(format!("{} answers for a single request", answers.len())),
    };
    match req["method"].as_str().unwrap_or("") {
        "order.create" => match one(answers)? {
            Answer::Accept => {
                let (vid, order) = book.accept(p);
                let seq = book.seq();
                let report = Report {
                    status: 3,
                    request: (0, 0),
                };
                let event = order_event(
                    &vid,
                    &order,
                    &text_of(p, "price"),
                    &text_of(p, "size"),
                    seq,
                    report,
                );
                let reply = ok(id.clone(), json!({ "order": echo(&vid, p, "NEW") }));
                Ok(vec![Frame::text(reply), Frame::Binary(event)])
            }
            Answer::Reject => refuse("-32602"),
            Answer::RejectCode(code) => refuse(&code),
            Answer::Silent => Ok(Vec::new()),
        },
        "order.create_batch" => {
            let items = p["orders"].as_array().cloned().unwrap_or_default();
            if items.len() != answers.len() {
                return Err(format!("{} items, {} answers", items.len(), answers.len()));
            }
            let (mut results, mut events) = (Vec::new(), Vec::new());
            for (item, answer) in items.iter().zip(answers) {
                match answer {
                    Answer::Accept => {
                        let (vid, order) = book.accept(item);
                        let seq = book.seq();
                        let report = Report {
                            status: 3,
                            request: (0, 0),
                        };
                        events.push(Frame::Binary(order_event(
                            &vid,
                            &order,
                            &text_of(item, "price"),
                            &text_of(item, "size"),
                            seq,
                            report,
                        )));
                        results.push(json!({ "order": echo(&vid, item, "NEW") }));
                    }
                    Answer::Reject | Answer::RejectCode(_) => {
                        results.push(json!({ "error": "synthetic: refused" }));
                    }
                    Answer::Silent => {}
                }
            }
            let mut frames = vec![Frame::text(ok(id, json!({ "results": results })))];
            frames.extend(events);
            Ok(frames)
        }
        "order.modify" => match one(answers)? {
            Answer::Accept => {
                let vid = text_of(p, "id");
                let order = book
                    .orders
                    .get(&vid)
                    .cloned()
                    .ok_or(format!("no order {vid}"))?;
                let mut echoed = echo(&vid, p, "OPEN");
                echoed["client_id"] = json!(order.cid);
                echoed["instruction"] = json!(order.instruction);
                let seq = book.seq();
                let report = Report {
                    status: 3,
                    request: (4, 1),
                };
                let event = order_event(
                    &vid,
                    &order,
                    &text_of(p, "price"),
                    &text_of(p, "size"),
                    seq,
                    report,
                );
                Ok(vec![
                    Frame::text(ok(id, json!({ "order": echoed }))),
                    Frame::Binary(event),
                ])
            }
            Answer::Reject => refuse("-32602"),
            Answer::RejectCode(code) => refuse(&code),
            Answer::Silent => Ok(Vec::new()),
        },
        other => Err(format!("not a request the stub answers: {other}")),
    }
}

/// The namespace the golden commands' client ids are minted in.
pub const GOLDEN_NS: Namespace = Namespace::new(1);

/// Each golden command: its name, the Java vector (`fixtures/paradex/signing/paradex-vectors.tsv`)
/// whose signature its frame carries (`None` for an unsigned cancel), its rpc and its command.
pub fn golden_table() -> Vec<(&'static str, Option<&'static str>, u64, VenueCommand)> {
    vec![
        // sell_limit: ETH-USD-PERP, SELL LIMIT 1.25 at 2450.7 (0.1 tick, 0.001 step).
        (
            "place",
            Some("sell_limit"),
            21,
            VenueCommand::Place(limit(0, ETH, Side::Sell, 1_250, 24_507)),
        ),
        // market_sell: BTC-USD-PERP, SELL MARKET 0.5, written at price 0 with IOC.
        (
            "place-market",
            Some("market_sell"),
            22,
            VenueCommand::Place(NewOrder {
                kind: OrderKind::Market,
                tif: Tif::Ioc,
                ..limit(1, BTC, Side::Sell, 500, 0)
            }),
        ),
        // edge_trailing_zeros: ETH-USD-PERP, BUY LIMIT 2.5 at 2450.7, as a batch of one.
        (
            "place-batch",
            Some("edge_trailing_zeros"),
            23,
            VenueCommand::PlaceBatch(vec![limit(2, ETH, Side::Buy, 2_500, 24_507)]),
        ),
        // modify: BTC-USD-PERP order 1759400000123201703010000 to SELL 0.02 at 65200.1.
        (
            "amend",
            Some("modify"),
            24,
            VenueCommand::Amend(AmendOrder {
                target: OrderRef::Venue(vid("1759400000123201703010000")),
                inst: BTC,
                side: Side::Sell,
                tif: Tif::Gtc,
                channel: Channel::Public,
                post_only: false,
                reduce_only: false,
                reducing: false,
                px: Ticks(652_001),
                qty: Lots::new(20).unwrap(),
                cum_filled: Lots::ZERO,
            }),
        ),
        (
            "cancel",
            None,
            25,
            VenueCommand::Cancel(cancel(OrderRef::Client(cid(0)))),
        ),
        (
            "cancel-batch",
            None,
            26,
            VenueCommand::CancelMany(vec![
                cancel(OrderRef::Venue(vid("1759500000000000001"))),
                cancel(OrderRef::Venue(vid("1759500000000000002"))),
            ]),
        ),
    ]
}

/// The golden commands, each encoded at its Java vector's timestamp.
pub fn goldens() -> Vec<Golden> {
    let vectors = Vectors::read();
    golden_table()
        .into_iter()
        .map(|(name, row, rpc, cmd)| {
            let ms = row.map_or(1_759_400_000_200, |row| vectors.row(row).u64("timestamp"));
            Golden {
                name,
                rpc: RpcId(rpc),
                ctx: EncodeCtx {
                    wall: WallNs(i64::try_from(ms).unwrap() * 1_000_000),
                    mono: MonoNs(1),
                    nonces: NonceBlock::EMPTY,
                },
                cmd,
            }
        })
        .collect()
}

fn limit(n: usize, inst: InstrumentId, side: Side, lots: i64, px: i64) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst,
        side,
        qty: Lots::new(lots).unwrap(),
        kind: OrderKind::Limit { px: Ticks(px) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
    }
}

fn cancel(target: OrderRef) -> CancelOrder {
    CancelOrder {
        target,
        inst: BTC,
        side: Side::Buy,
        placement_nonce: None,
    }
}

pub fn vid(wire: &str) -> VenueOrderId {
    dispatch(&caps(), GOLDEN_NS, |scope| scope.venue_order_id(wire)).unwrap()
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory at a
/// fixed time, so every run mints the same ids.
pub fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!(
            "fbc-paradex-conformance-goldens-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), GOLDEN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_400_000_000_000_000));
        let cids = (0..3).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[n]
}

/// Client id `n` as Paradex's wire spells it.
pub fn uuid(n: usize) -> String {
    encode_cid(&ClientIdFormat::Uuid, cid(n))
        .unwrap()
        .as_str()
        .to_owned()
}
