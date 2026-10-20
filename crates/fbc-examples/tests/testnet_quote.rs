//! FBC-elg7's done line: testnet_quote, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for Paradex, with synthetic credentials, over several requote cycles:
//! no amend frame is ever sent; every replace is a cancel acknowledged (and the order ended)
//! before the next place on that level; every place is within the $50 inventory cap and the $11
//! resting cap per side; after a scripted reconnect every order of ours still open on the market
//! is cancelled before any place; a resync position mismatch Stops it (kill switch + cancel all)
//! with no place after; a mainnet URL or chain id is refused before any connection; and no
//! credential appears in what it prints or in what the stack logs.
//!
//! **The reconnect's cancel.** The ticket asks for an instrument cancel-all there. fbc-oms
//! builds one only under 0005's I7 guard, which needs a trustworthy resync to have shown the
//! account's open orders; Paradex's snapshot source is Trustworthy (decision 0094), so the
//! loop's sweeps (at the start and after each reconnect) and Stop send the instrument
//! cancel-all (`order.cancel_all` for the market), the orders the resync shows still
//! Safety-cancelled by the session as the connection opens (decision 0080). The stub answers it
//! `{"status":"ok"}` and closes every order of ours still open on the market.
//!
//! The stub is a small stateful venue ([`Venue`]): it answers the login, the order book the
//! test scripts cycle by cycle, the resync's open orders (from what it holds) and positions over
//! HTTP, and on the order socket the auth frame, the subscriptions, the cancel-on-disconnect
//! arm, each place (its order echoed under a new venue id) and each cancel (queued, then an SBE
//! `OrderEvent` at 1:2 reporting the order CLOSED by USER_CANCELED). The run is on real time,
//! each bounded at 60 s.

#[path = "../examples/testnet_quote/args.rs"]
mod args;
// The credentials (review paths, used unchanged): the testnet's and, with --mainnet, the
// mainnet-only variables.
#[path = "../src/auth.rs"]
mod auth;
#[path = "../src/auth_mainnet.rs"]
mod auth_mainnet;
// The position check's REST reads (a review path, used unchanged): it uses a part of it.
#[allow(dead_code)]
#[path = "../src/auth_rest.rs"]
mod auth_rest;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;
// Shared by the mainnet samples: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/mainnet.rs"]
mod mainnet;
// Shared by the samples that can fill: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/positions.rs"]
mod positions;
#[path = "../examples/testnet_quote/quote.rs"]
mod quote;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/testnet.rs"]
mod testnet;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once};

use std::time::Duration;

use args::{Options, Parsed, USAGE};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responded, Responder, Step, StubServer, WsScript,
};
use fbc_core::{
    AccountKey, AccountSummary, AssetKey, CidMint, ConfigError, EndpointPlan, ExchNs, ExecCodec,
    ExecEndpoint, FieldSpec, HttpPlan, InstrumentSpecDraft, LimitScope, Lots, MdCodec, Namespace,
    NamespaceLease, OpKind, RpcId, Secret, Secrets, Side, SignedLots, SnapshotSource, SpecTable,
    Subscription, SymbolError, TagSet, Ticks, VenueCaps, VenueConfig, VenueError, VenueFactory,
    WallNs, encode_cid,
};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::Notify;

/// The session token the stub's login gives: made-up text that never looks like a JWT.
const TOKEN: &str = "SYNTHETIC.session-token.testnet-quote";
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

/// The synthetic credentials twice: the session's and the position check's own login.
fn creds() -> (Secrets, Secrets) {
    (secrets(), secrets())
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
    /// The lease directories the test on this thread made, removed when its thread ends.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of the test's own, removed when the test's thread ends.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-testnet-quote-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
    dir
}

fn dec(text: &str) -> Decimal {
    Decimal::from_str(text).unwrap()
}

/// The owner's caps ($11 resting per side, $50 inventory), quotes of $10.50 30 bps behind the
/// touch on the BTC grid (tick 0.1, step 0.00001), at most `cycles` cycles 100 ms apart.
fn options(stub: &StubServer, cycles: u64) -> Options {
    Options {
        market: MARKET.to_owned(),
        tick: dec("0.1"),
        step: dec("0.00001"),
        min_notional: dec("5"),
        resting_cap_usd: dec("11"),
        inventory_cap_usd: dec("50"),
        order_usd: dec("10.5"),
        namespace: 1,
        bid_bps: 30,
        ask_bps: 30,
        duration_secs: 60,
        cycles: Some(cycles),
        interval_ms: 100,
        step_timeout_secs: 10,
        rest_url: stub.http_url("/v1"),
        ws_url: stub.ws_url("/v1"),
        proxy: ProxyConfig::Direct,
        lease_dir: lease_dir(),
        sole_trader: true,
        mainnet: false,
        cancel_window: args::CANCEL_NOT_OURS_WINDOW,
    }
}

/// A `GET /orderbook` answer with one level a side (`None`: the side is empty).
fn book(seq: u64, bid: Option<&str>, ask: Option<&str>) -> String {
    let level = |px: Option<&str>| match px {
        Some(px) => json!([[px, "0.5"]]),
        None => json!([]),
    };
    json!({
        "market": MARKET, "seq_no": seq, "last_updated_at": 1_759_500_000_000_i64,
        "bids": level(bid), "asks": level(ask),
    })
    .to_string()
}

/// The stub's mark price unless a test scripts others: between the first touch's best bid
/// (62000.2) and ask (62000.5).
const MARK: &str = "62000.3";

fn mark() -> Decimal {
    dec(MARK)
}

/// The first touch every test starts from: bid 62000.2, ask 62000.5.
fn b0() -> String {
    book(1, Some("62000.2"), Some("62000.5"))
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = dec(text) * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2, as `fixtures/paradex/exec/README.md` describes
/// it: our post-only limit order `cid` on `side` (1 BUY, 2 SELL) of `size` at `price`, venue id
/// `vid`, CLOSED by USER_CANCELED with nothing filled. The account is 32 made-up bytes.
fn order_closed(side: u8, seq: i64, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    order_event(side, seq, vid, cid, price, size, size, "USER_CANCELED")
}

/// As [`order_closed`], with `open` of `size` still open and the cancel reason `reason` (empty
/// and nothing open: filled).
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
    order_event_at(ORDER_TS, side, seq, vid, cid, price, size, open, reason)
}

/// The venue time (µs) the stub's order events carry, but for another program's cancels.
const ORDER_TS: i64 = 1_759_500_000_205_011;

/// As [`order_event`], stamped `ts` (µs) by the venue.
#[allow(clippy::too_many_arguments)]
fn order_event_at(
    ts: i64,
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

/// A `FillEvent` (template 21) at version 1, as `fixtures/paradex/exec/README.md` describes it:
/// our buy `cid` (venue id `vid`) made `size` at `price` for no fee, fill id `fill`.
fn fill_event(seq: i64, fill: &str, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    let ts = 1_759_500_000_204_011i64;
    let mut f = Vec::new();
    for v in [107u16, 21, 1, 1] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([1u8, 1, 1]); // FILL, BUY, MAKER
    f.extend(e8(price).to_le_bytes());
    f.extend(e8(size).to_le_bytes());
    f.extend(0i64.to_le_bytes()); // fee
    f.extend(i64::MIN.to_le_bytes()); // realizedPnl: null
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(e8(price).to_le_bytes()); // underlyingPrice
    f.extend(0i64.to_le_bytes()); // realizedFunding
    assert_eq!(f.len(), 8 + 107);
    for s in [fill, vid, cid, "9615262148007719001", MARKET] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// A `PositionEvent` (template 22) at version 2: `fixtures/paradex/exec/position-long-v2`
/// with its size set to `size` (long, absolute).
fn position_event(size: &str) -> Vec<u8> {
    let text = fs::read_to_string(fixture("paradex/exec/position-long-v2.sbe.txt")).unwrap();
    let mut f: Vec<u8> = text
        .lines()
        .map(|line| line.split('#').next().unwrap())
        .flat_map(str::split_whitespace)
        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
        .collect();
    f[8 + 17..8 + 25].copy_from_slice(&e8(size).to_le_bytes());
    f
}

/// The account the REST answers name: the 32 made-up bytes the SBE frames carry, as hex.
fn account() -> String {
    let hex: String = (0xa0u8..=0xbf).map(|b| format!("{b:02x}")).collect();
    format!("0x{hex}")
}

/// An order the stub venue holds.
#[derive(Clone, Debug)]
struct Held {
    vid: String,
    cid: String,
    /// BUY or SELL.
    side: String,
    price: String,
    size: String,
    open: bool,
}

/// The stub venue: what it holds and what it is told to do.
#[derive(Default)]
struct Venue {
    orders: Vec<Held>,
    next_vid: u64,
    seq: i64,
    /// The order books `GET /orderbook` answers in turn, the last one again after, its `seq_no`
    /// one higher each time (the venue's book moves on: Paradex's `seq_no` rises with every
    /// change, and our fill changes it).
    books: Vec<String>,
    book_reads: usize,
    /// The mark prices `GET /markets/summary` answers in turn, the last one again after
    /// ([`MARK`] when none is scripted), how many it answered, and the mark it last answered
    /// when each place came.
    marks: Vec<&'static str>,
    mark_reads: usize,
    marks_at_create: Vec<Decimal>,
    /// The chain `GET /system/config` names (mainnet's when `None`).
    chain: Option<&'static str>,
    /// `POST /auth` answered so far, and from which one (from 1) it is refused with a 401.
    logins: usize,
    login_refused_from: Option<usize>,
    /// A position on another market (its symbol and size, long) that `GET /positions` lists
    /// beside the market's.
    elsewhere: Option<(&'static str, &'static str)>,
    /// How many `GET /positions` reads answer the account's position before
    /// [`Venue::positions_script`] and [`Venue::position_after_first`] apply (1 when 0: the
    /// resync's; a mainnet run reads once more first, for the other markets).
    unscripted: usize,
    /// Answer the `n`th place (from 1) with this JSON-RPC error code, creating nothing.
    reject_create: Option<(usize, i64)>,
    /// Answer the `n`th single cancel (from 1) with this JSON-RPC error code, closing nothing.
    reject_cancel: Option<(usize, i64)>,
    /// Right after refusing a single cancel ([`Venue::reject_cancel`]), close its order, still
    /// open, by a user cancel the loop never sent: another program's cancel while ours waits to
    /// be sent again (Codex r4309154204 on PR #159).
    cotenant_after_refused_cancel: bool,
    /// As [`Venue::cotenant_after_refused_cancel`], with the user cancel's order event sent
    /// before the refusal: another program's cancel racing ours, read first (Codex r4310042612
    /// on PR #159).
    cotenant_before_refused_cancel: bool,
    cancels: usize,
    /// When each place, each single cancel and each instrument cancel-all came.
    create_times: Vec<std::time::Instant>,
    cancel_times: Vec<std::time::Instant>,
    cancel_all_times: Vec<std::time::Instant>,
    /// Answer the `n`th instrument cancel-all (from 1) with this JSON-RPC error code, closing
    /// nothing.
    reject_cancel_all: Option<(usize, i64)>,
    /// Answer the next single cancel with the order's CLOSED event only, never the reply: the
    /// venue's order stream ahead of the cancel's reply, which then never comes.
    closed_without_reply_once: bool,
    orders_reads: usize,
    positions_reads: usize,
    /// From the second `GET /orders` on: hide the open sell orders (a lagging snapshot).
    hide_sells_after_first: bool,
    /// From the second `GET /positions` on: this position (a size, long) instead of flat.
    position_after_first: Option<&'static str>,
    /// The account's position on the market (long), moved by every fill the stub sends:
    /// `GET /positions` answers it, so positions follow fills unless a test says otherwise.
    position: Decimal,
    /// The answers of the `GET /positions` reads after the first (the resync's), in turn,
    /// before [`Venue::position`] is answered again: `None` flat, `Some` a long of that size
    /// (FBC-hpg6: the position check's reads after the first fill and at Stop).
    positions_script: VecDeque<Option<&'static str>>,
    /// Notified at this `GET /positions` read (from 1; the first is the resync's).
    positions_signal: Option<(usize, Arc<Notify>)>,
    /// How many `GET /orderbook` reads were answered when each `GET /positions` read came, and
    /// when each place came: which touch a place was priced from, against the position check's
    /// reads (RA157-2 / RB157-1 on PR #157).
    books_at_positions: Vec<usize>,
    books_at_create: Vec<usize>,
    /// The lease directory whose market lease is tried at each `GET /positions` read, and
    /// whether it was held then (Codex r4305979062 on PR #157).
    lease_probe: Option<PathBuf>,
    lease_held: Vec<bool>,
    /// Answer the next single cancel ALREADY_CLOSED, with no order event.
    already_closed_once: bool,
    /// Answer the next single cancel of a buy queued, then fill the buy whole (its fill event
    /// sent twice, the second a duplicate) and close it filled: a fill racing the cancel.
    fill_next_buy_cancel: bool,
    /// Answer every instrument cancel-all with Paradex's internal error
    /// (`fixtures/paradex/exec/error-internal.json`), closing nothing.
    refuse_cancel_all: bool,
    /// Send a frame no codec decodes after the reply to this place (from 1).
    garbage_after_create: Option<usize>,
    /// Notified at this `GET /orderbook` read (from 1): the loop's cycle before it settled.
    signal: Option<(usize, Arc<Notify>)>,
    /// Close the order this place (from 1) created right after its reply, nothing filled, for
    /// this reason: Paradex's risk check refusing it (`POST_ONLY_WOULD_CROSS`: the touch moved
    /// through the quote).
    close_create: Option<(usize, &'static str)>,
    /// After the reply to this place (from 1), report the account's position long this size,
    /// with no fill: the venue's position stream ahead of the fill, or a change not ours.
    position_after_create: Option<(usize, &'static str)>,
    /// Answer the next single cancel of a buy queued, then close the buy filled whole on an
    /// order update, its fill event not sent (the fill stream behind the order stream).
    filled_unsent_next_buy_cancel: bool,
    creates: usize,
    /// Another program on the account (FBC-aoh5: the owner's 2026-10-18 mainnet session, where
    /// a Java instance's orphan reconcile cancelled every order on its ticker it did not place):
    /// right after the reply to each of these places (from 1), every order of ours the stub
    /// holds open, of the side named (BUY or SELL; both when `None`), is closed by a user cancel
    /// the loop never sent (`user_cancel_ws`).
    cotenant_after_create: Vec<(usize, Option<&'static str>)>,
    /// How long each `GET /orderbook` read takes to answer: a cycle whose reads take longer than
    /// the cancel-not-ours window (Codex r4309154208 on PR #159). The stub's handler blocks for
    /// it, so the whole test runtime waits, as a slow venue would make the loop.
    book_delay: Option<Duration>,
    /// Answer every instrument cancel-all with the order events closing the orders first, and
    /// its `{"status":"ok"}` after them: the order stream ahead of the cancel-all's reply.
    cancel_all_closes_first: bool,
    /// Every frame the stub sent, with the connection's number, for the wire-order checks.
    log: Vec<(usize, String)>,
}

type Shared = Arc<Mutex<Venue>>;

fn lock(venue: &Shared) -> MutexGuard<'_, Venue> {
    venue.lock().unwrap_or_else(|e| e.into_inner())
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// Whether a market lease in `dir` is held, by this process or another: each `market+` lock
/// file tried without waiting (an advisory lock, as fbc-core's leases take it).
fn market_lease_held(dir: &std::path::Path) -> bool {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("market+"))
        .any(|e| {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(e.path())
                .unwrap();
            matches!(file.try_lock(), Err(fs::TryLockError::WouldBlock))
        })
}

/// The stub's HTTP answers, from `venue`.
fn routes(venue: &Shared, login: HttpReply) -> HttpRouter {
    let (books, orders, positions) = (venue.clone(), venue.clone(), venue.clone());
    let (logins, marks, chain) = (venue.clone(), venue.clone(), venue.clone());
    HttpRouter::new()
        .route_fn(Method::POST, PathPattern::exact("/v1/auth"), move |_| {
            let mut v = lock(&logins);
            v.logins += 1;
            match v.login_refused_from {
                Some(n) if v.logins >= n => {
                    let body = json!({
                        "error": "STARKNET_SIGNATURE_VERIFICATION_FAILED",
                        "message": "verification failed on Curve",
                    });
                    reply(401, body.to_string())
                }
                _ => login.clone(),
            }
        })
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/system/config"),
            move |_| {
                let v = lock(&chain);
                let named = v.chain.unwrap_or("PRIVATE_SN_PARACLEAR_MAINNET");
                let body = json!({
                    "starknet_chain_id": named, "l1_chain_id": "1",
                    "block_explorer_url": "https://example.invalid/", "environment": "stub",
                });
                reply(200, body.to_string())
            },
        )
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/markets/summary"),
            move |_| {
                let mut v = lock(&marks);
                let at = v.mark_reads.min(v.marks.len().saturating_sub(1));
                v.mark_reads += 1;
                let mark = v.marks.get(at).copied().unwrap_or(MARK);
                let body = json!({"results": [{
                    "symbol": MARKET, "mark_price": mark, "oracle_price": mark,
                    "last_traded_price": mark, "bid": "62000.2", "ask": "62000.5",
                    "funding_rate": "0.0001", "open_interest": "1", "volume_24h": "1",
                    "created_at": 1_759_500_000_000u64,
                }]});
                reply(200, body.to_string())
            },
        )
        .route_fn(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            move |_| {
                let delay = lock(&books).book_delay;
                if let Some(delay) = delay {
                    std::thread::sleep(delay);
                }
                let mut v = lock(&books);
                let last = v.books.len() - 1;
                let at = v.book_reads.min(last);
                let past = v.book_reads.saturating_sub(last);
                v.book_reads += 1;
                if let Some((n, signal)) = &v.signal
                    && v.book_reads == *n
                {
                    signal.notify_one();
                }
                let mut book: Value = serde_json::from_str(&v.books[at]).unwrap();
                if past > 0 {
                    let seq = book["seq_no"].as_u64().unwrap();
                    book["seq_no"] = json!(seq + u64::try_from(past).unwrap());
                }
                reply(200, book.to_string())
            },
        )
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), move |_| {
            let mut v = lock(&orders);
            v.orders_reads += 1;
            let hide = v.hide_sells_after_first && v.orders_reads > 1;
            let results: Vec<Value> = v
                .orders
                .iter()
                .filter(|o| o.open && !(hide && o.side == "SELL"))
                .map(|o| {
                    json!({
                        "id": o.vid, "client_id": o.cid, "market": MARKET, "side": o.side,
                        "type": "LIMIT", "instruction": "POST_ONLY", "price": o.price,
                        "size": o.size, "remaining_size": o.size, "status": "OPEN",
                        "flags": [], "account": account(),
                    })
                })
                .collect();
            reply(200, json!({ "results": results }).to_string())
        })
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            move |_| {
                let mut v = lock(&positions);
                v.positions_reads += 1;
                let books = v.book_reads;
                v.books_at_positions.push(books);
                if let Some(dir) = v.lease_probe.clone() {
                    v.lease_held.push(market_lease_held(&dir));
                }
                if let Some((n, signal)) = &v.positions_signal
                    && v.positions_reads == *n
                {
                    signal.notify_one();
                }
                let later = v.positions_reads > v.unscripted.max(1);
                let scripted = if later {
                    v.positions_script.pop_front()
                } else {
                    None
                };
                let size: Option<String> = match scripted {
                    Some(answer) => answer.map(str::to_owned),
                    None => v
                        .position_after_first
                        .filter(|_| later)
                        .map(str::to_owned)
                        .or_else(|| (!v.position.is_zero()).then(|| v.position.to_string())),
                };
                let other = v.elsewhere.map(|(market, size)| {
                    json!({
                        "account": account(), "average_entry_price": "3000",
                        "average_entry_price_usd": "3000", "average_exit_price": "0",
                        "cached_funding_index": "0", "closed_at": 0, "cost": "0",
                        "cost_usd": "0", "created_at": 1_759_622_300_000_i64,
                        "id": "other-position", "last_fill_id": "fill-other",
                        "last_updated_at": 1_759_622_390_000_i64, "leverage": "",
                        "liquidation_price": "", "market": market,
                        "realized_positional_funding_pnl": "0", "realized_positional_pnl": "0",
                        "seq_no": 3, "side": "LONG", "size": size, "status": "OPEN",
                        "unrealized_funding_pnl": "0", "unrealized_pnl": "0",
                    })
                });
                let ours = size.map(|size| {
                    json!({
                        "account": account(),
                        "average_entry_price": "62000", "average_entry_price_usd": "62000",
                        "average_exit_price": "0", "cached_funding_index": "0", "closed_at": 0,
                        "cost": "0", "cost_usd": "0", "created_at": 1_759_622_300_000_i64,
                        "id": "BTC-USD-PERP-position", "last_fill_id": "fill-BTC-USD-PERP",
                        "last_updated_at": 1_759_622_390_000_i64, "leverage": "",
                        "liquidation_price": "", "market": MARKET,
                        "realized_positional_funding_pnl": "0", "realized_positional_pnl": "0",
                        "seq_no": 7, "side": "LONG", "size": size, "status": "OPEN",
                        "unrealized_funding_pnl": "0", "unrealized_pnl": "0",
                    })
                });
                let results: Vec<Value> = ours.into_iter().chain(other).collect();
                let body = json!({ "results": results }).to_string();
                reply(200, body)
            },
        )
}

/// The stub's answer to each JSON-RPC frame on connection `conn`, from `venue`.
fn responder(venue: &Shared, conn: usize) -> Responder {
    let venue = venue.clone();
    Responder::new(move |frame| respond(&venue, conn, frame))
}

/// The stub's answer to the JSON-RPC frame `frame` on connection `conn`, from `venue`.
fn respond(venue: &Shared, conn: usize, frame: &Frame) -> Responded {
    let Frame::Text(text) = frame else {
        return Err("a binary frame from the client".to_owned());
    };
    let req: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let id = req["id"].clone();
    let params = &req["params"];
    let ok = |result: Value| {
        Frame::text(json!({"jsonrpc": "2.0", "result": result, "id": id}).to_string())
    };
    let mut v = lock(venue);
    let method = req["method"].as_str().unwrap_or("").to_owned();
    v.log.push((conn, text.clone()));
    let closed = |v: &mut Venue, at: usize| {
        v.orders[at].open = false;
        v.seq += 1;
        let o = v.orders[at].clone();
        let side = if o.side == "BUY" { 1 } else { 2 };
        Frame::Binary(order_closed(
            side,
            5_000 + v.seq,
            &o.vid,
            &o.cid,
            &o.price,
            &o.size,
        ))
    };
    let frames = match method.as_str() {
        "auth" | "subscribe" => vec![ok(json!({}))],
        "order.cancel_on_disconnect" => vec![ok(json!({"enabled": true}))],
        "order.create" => {
            v.create_times.push(std::time::Instant::now());
            let at = v
                .mark_reads
                .saturating_sub(1)
                .min(v.marks.len().saturating_sub(1));
            let served = dec(v.marks.get(at).copied().unwrap_or(MARK));
            v.marks_at_create.push(served);
            if let Some((n, code)) = v.reject_create
                && n == v.create_times.len()
            {
                let error = json!({"code": code, "message": "order.create refused"});
                let reply = json!({"jsonrpc": "2.0", "error": error, "id": id});
                return Ok(vec![Frame::text(reply.to_string())]);
            }
            v.next_vid += 1;
            let held = Held {
                vid: format!("17595000000000{:05}", v.next_vid),
                cid: params["client_id"].as_str().unwrap_or("").to_owned(),
                side: params["side"].as_str().unwrap_or("").to_owned(),
                price: params["price"].as_str().unwrap_or("").to_owned(),
                size: params["size"].as_str().unwrap_or("").to_owned(),
                open: true,
            };
            let order = json!({
                "id": held.vid, "client_id": held.cid, "market": MARKET, "side": held.side,
                "type": "LIMIT", "instruction": "POST_ONLY", "price": held.price,
                "size": held.size, "remaining_size": held.size, "status": "NEW",
            });
            v.orders.push(held);
            v.creates += 1;
            let books = v.book_reads;
            v.books_at_create.push(books);
            let mut frames = vec![ok(json!({ "order": order }))];
            if let Some((n, size)) = v.position_after_create
                && n == v.creates
            {
                frames.push(Frame::Binary(position_event(size)));
            }
            if let Some((n, reason)) = v.close_create
                && n == v.creates
            {
                let at = v.orders.len() - 1;
                v.orders[at].open = false;
                v.seq += 1;
                let o = v.orders[at].clone();
                let side = if o.side == "BUY" { 1 } else { 2 };
                frames.push(Frame::Binary(order_event(
                    side,
                    5_000 + v.seq,
                    &o.vid,
                    &o.cid,
                    &o.price,
                    &o.size,
                    &o.size,
                    reason,
                )));
            }
            if v.garbage_after_create == Some(v.creates) {
                frames.push(Frame::Binary(b"\xff".to_vec()));
            }
            let creates = v.creates;
            if let Some((_, side)) = v
                .cotenant_after_create
                .iter()
                .copied()
                .find(|(n, _)| *n == creates)
            {
                for at in 0..v.orders.len() {
                    if v.orders[at].open && side.is_none_or(|s| v.orders[at].side == s) {
                        frames.push(user_cancelled(&mut v, at));
                    }
                }
            }
            frames
        }
        "order.cancel" => {
            v.cancels += 1;
            v.cancel_times.push(std::time::Instant::now());
            if let Some((n, code)) = v.reject_cancel
                && n == v.cancels
            {
                let error = json!({"code": code, "message": "order.cancel refused"});
                let reply = json!({"jsonrpc": "2.0", "error": error, "id": id});
                let mut frames = vec![Frame::text(reply.to_string())];
                if (v.cotenant_after_refused_cancel || v.cotenant_before_refused_cancel)
                    && let Some(at) = v.orders.iter().position(|o| {
                        o.open
                            && (params["id"].as_str() == Some(o.vid.as_str())
                                || params["client_id"].as_str() == Some(o.cid.as_str()))
                    })
                {
                    let cancelled = user_cancelled(&mut v, at);
                    if v.cotenant_before_refused_cancel {
                        frames.insert(0, cancelled);
                    } else {
                        frames.push(cancelled);
                    }
                }
                return Ok(frames);
            }
            let at = v
                .orders
                .iter()
                .position(|o| {
                    params["id"].as_str() == Some(o.vid.as_str())
                        || params["client_id"].as_str() == Some(o.cid.as_str())
                })
                .ok_or("a cancel of no order the stub holds")?;
            let vid = v.orders[at].vid.clone();
            if v.fill_next_buy_cancel && v.orders[at].side == "BUY" {
                v.fill_next_buy_cancel = false;
                v.orders[at].open = false;
                let o = v.orders[at].clone();
                // The fill moves the account's position (its duplicate does not).
                v.position += dec(&o.size);
                v.seq += 2;
                let fill = fill_event(
                    5_000 + v.seq,
                    "8615262148007719001",
                    &o.vid,
                    &o.cid,
                    &o.price,
                    &o.size,
                );
                let filled =
                    order_event(1, 5_001 + v.seq, &o.vid, &o.cid, &o.price, &o.size, "0", "");
                let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                vec![
                    queued,
                    Frame::Binary(fill.clone()),
                    Frame::Binary(fill),
                    Frame::Binary(filled),
                ]
            } else if v.filled_unsent_next_buy_cancel && v.orders[at].side == "BUY" {
                v.filled_unsent_next_buy_cancel = false;
                v.orders[at].open = false;
                let o = v.orders[at].clone();
                v.seq += 1;
                let filled =
                    order_event(1, 5_000 + v.seq, &o.vid, &o.cid, &o.price, &o.size, "0", "");
                let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                vec![queued, Frame::Binary(filled)]
            } else if v.closed_without_reply_once {
                v.closed_without_reply_once = false;
                vec![closed(&mut v, at)]
            } else if v.already_closed_once {
                v.already_closed_once = false;
                vec![ok(json!({"order_id": vid, "status": "ALREADY_CLOSED"}))]
            } else {
                let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                vec![queued, closed(&mut v, at)]
            }
        }
        "order.cancel_batch" => {
            let ids = params["order_ids"].as_array().cloned().unwrap_or_default();
            let mut results = Vec::new();
            let mut events = Vec::new();
            for id in &ids {
                let vid = id.as_str().unwrap_or("");
                let at = v
                    .orders
                    .iter()
                    .position(|o| o.vid == vid)
                    .ok_or("a batch cancel of no order the stub holds")?;
                events.push(closed(&mut v, at));
                let status = "QUEUED_FOR_CANCELLATION";
                results.push(json!({"id": vid, "market": MARKET, "status": status}));
            }
            let mut frames = vec![ok(json!({ "results": results }))];
            frames.extend(events);
            frames
        }
        "order.cancel_all" => {
            // An instrument cancel-all, which fbc-oms builds under 0005's I7 guard now that
            // Paradex's resync is trustworthy (decision 0094): `{"status":"ok"}`, then every
            // order still open on the market CLOSED, each by its order event.
            if params["market"] != MARKET {
                return Err(format!("a cancel-all not of the market: {params}"));
            }
            v.cancel_all_times.push(std::time::Instant::now());
            if let Some((n, code)) = v.reject_cancel_all
                && n == v.cancel_all_times.len()
            {
                let error = json!({"code": code, "message": "order.cancel_all refused"});
                let reply = json!({"jsonrpc": "2.0", "error": error, "id": id});
                return Ok(vec![Frame::text(reply.to_string())]);
            }
            if v.refuse_cancel_all {
                let error = json!({"code": -32603, "message": "Internal error"});
                let reply = json!({"jsonrpc": "2.0", "error": error, "id": id});
                return Ok(vec![Frame::text(reply.to_string())]);
            }
            let mut frames = vec![ok(json!({"status": "ok"}))];
            for at in 0..v.orders.len() {
                if v.orders[at].open {
                    frames.push(closed(&mut v, at));
                }
            }
            if v.cancel_all_closes_first {
                frames.rotate_left(1);
            }
            frames
        }
        other => return Err(format!("an unexpected method {other}")),
    };
    Ok(frames)
}

/// The order event closing the order `at` the stub holds by a user cancel the loop never sent
/// (`user_cancel_ws`, which Paradex's codec reads as `Canceled(Requested)`, as it reads our own
/// cancels' USER_CANCELED): another program on the account (FBC-aoh5). The venue stamps it 2 s
/// per place after the stub's other order events: the cancels of one action (one reconcile, one
/// cancel-all in the UI) share a venue time, and another action's is seconds later.
fn user_cancelled(v: &mut Venue, at: usize) -> Frame {
    v.orders[at].open = false;
    v.seq += 1;
    let o = v.orders[at].clone();
    let side = if o.side == "BUY" { 1 } else { 2 };
    let ts = ORDER_TS + i64::try_from(v.creates).unwrap() * 2_000_000;
    Frame::Binary(order_event_at(
        ts,
        side,
        5_000 + v.seq,
        &o.vid,
        &o.cid,
        &o.price,
        &o.size,
        &o.size,
        "user_cancel_ws",
    ))
}

/// `n` answered frames on connection `conn`.
fn answered(venue: &Shared, conn: usize, n: usize) -> Vec<Step> {
    let with = responder(venue, conn);
    (0..n)
        .map(|_| Step::Respond {
            conn,
            with: with.clone(),
        })
        .collect()
}

/// One connection, `n` frames answered.
fn one_connection(venue: &Shared, n: usize) -> WsScript {
    let mut steps = vec![Step::Accept];
    steps.extend(answered(venue, 0, n));
    WsScript::new(steps)
}

/// Two connections: `first` frames answered on the first, which the stub then closes, and
/// `second` on the next.
fn reconnecting(venue: &Shared, first: usize, second: usize) -> WsScript {
    let mut steps = vec![Step::Accept];
    steps.extend(answered(venue, 0, first));
    steps.push(Step::Close { conn: 0 });
    steps.push(Step::Accept);
    steps.extend(answered(venue, 1, second));
    WsScript::new(steps)
}

/// A venue answering `books` in turn.
fn venue(books: Vec<String>) -> Shared {
    Arc::new(Mutex::new(Venue {
        books,
        ..Venue::default()
    }))
}

fn login_ok() -> HttpReply {
    reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#))
}

/// Runs testnet_quote with `opts` until it ends or `interrupt` completes, bounded at 60 s: its
/// report and what it printed.
async fn run_until(
    opts: &Options,
    interrupt: impl std::future::Future<Output = ()>,
) -> (quote::Report, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let run = quote::run(opts, None, creds(), out, interrupt);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let report = ran
        .unwrap_or_else(|_| panic!("testnet_quote did not finish in 60 s; it printed:\n{printed}"))
        .unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));
    (report, printed)
}

/// Runs testnet_quote with `opts` until it ends by itself.
async fn run_against(opts: &Options) -> (quote::Report, String) {
    run_until(opts, std::future::pending()).await
}

/// The JSON-RPC requests the session sent on connection `conn`, in order.
fn requests(venue: &Shared, conn: usize) -> Vec<Value> {
    lock(venue)
        .log
        .iter()
        .filter(|(c, _)| *c == conn)
        .map(|(_, t)| serde_json::from_str(t).unwrap())
        .collect()
}

fn methods(venue: &Shared, conn: usize) -> Vec<String> {
    requests(venue, conn)
        .iter()
        .map(|r| r["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

/// The first four words after the tag of each line tagged `tag`.
fn tagged<'a>(printed: &'a str, tag: &str) -> Vec<&'a str> {
    printed
        .lines()
        .filter(|l| l.split_whitespace().next() == Some(tag))
        .collect()
}

/// No credential, and not the session token, in `text`.
fn no_credential_in(text: &str) {
    let (account, key) = synthetic();
    let lower = text.to_ascii_lowercase();
    for secret in [&account, &key, &TOKEN.to_owned()] {
        let bare = secret.trim_start_matches("0x").to_ascii_lowercase();
        assert!(!lower.contains(&bare), "a credential appeared:\n{text}");
    }
}

/// Everything the stack logs through the `log` facade, captured at every level it compiles in.
mod capture {
    use super::*;
    use log::{LevelFilter, Log, Metadata, Record};

    static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

    struct Capture;

    impl Log for Capture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &Record<'_>) {
            let line = format!("{} {} {}", record.level(), record.target(), record.args());
            LINES.lock().unwrap_or_else(|e| e.into_inner()).push(line);
        }

        fn flush(&self) {}
    }

    static CAPTURE: Capture = Capture;

    /// Installs the capture once for the test binary.
    pub fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            log::set_logger(&CAPTURE).expect("no other logger in this test binary");
            log::set_max_level(LevelFilter::Trace);
        });
    }

    /// Every record logged so far, by any test of the binary.
    pub fn all() -> String {
        LINES.lock().unwrap_or_else(|e| e.into_inner()).join("\n")
    }
}

/// The side (BUY or SELL) of the order venue id `vid` names.
fn side_of(venue: &Shared, vid: &str) -> String {
    let v = lock(venue);
    v.orders.iter().find(|o| o.vid == vid).unwrap().side.clone()
}

/// The venue ids a cancel request names.
fn cancelled_ids(req: &Value) -> Vec<String> {
    match req["method"].as_str() {
        Some("order.cancel") => vec![req["params"]["id"].as_str().unwrap().to_owned()],
        Some("order.cancel_batch") => req["params"]["order_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
async fn testnet_quote_requotes_by_cancel_and_replace_within_the_caps_against_the_stub() {
    capture::install();
    // Cycle 1 quotes the first touch; the touch moves up for cycle 2 and down for cycle 3, so
    // both sides are replaced twice; cycle 4 sees no change and sends nothing.
    let books = vec![
        b0(),
        b0(),
        book(2, Some("62100.0"), Some("62100.3")),
        book(3, Some("61900.0"), Some("61900.4")),
    ];
    let venue = venue(books);
    // The opening six (auth, four subscriptions, the arm), the start's sweep (an instrument
    // cancel-all: Paradex's resync is trustworthy, decision 0094), two places, two replaces of
    // two cancels and two places each, and Stop's instrument cancel-all.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 4 + 4 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 4 reached", "{printed}");
    assert_eq!(
        (report.cycles, report.places_sent, report.cancels_sent),
        (4, 6, 6),
        "{printed}"
    );
    assert_eq!(
        (report.cancelled, report.fills, report.inventory),
        (6, 0, 0),
        "{printed}"
    );
    // Nothing of the answered requests is kept, in the loop or the link.
    assert_eq!(report.requests_held, 0, "{printed}");
    // Every order ended by a cancel of ours (the replaces' and Stop's instrument cancel-all,
    // whose order events say USER_CANCELED as another program's would): none is counted as a
    // cancel not ours (FBC-aoh5).
    assert_eq!(report.cancels_not_ours, 0, "{printed}");
    assert!(!printed.contains("\nWARN"), "{printed}");

    // No amend frame, ever: every change is a cancel and a place.
    let sent = methods(&venue, 0);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.modify"), 0, "{sent:?}");
    assert_eq!(count("order.create"), 6, "{sent:?}");
    assert_eq!(count("order.cancel"), 4, "{sent:?}");
    assert_eq!(count("order.cancel_batch"), 0, "{sent:?}");
    // The start's sweep and Stop: the instrument cancel-all, first and last.
    assert_eq!(count("order.cancel_all"), 2, "{sent:?}");
    assert_eq!(sent[6], "order.cancel_all", "{sent:?}");
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));

    // Per side, on the wire: a place, then the cancel of that very order before the next place.
    let reqs = requests(&venue, 0);
    for side in ["BUY", "SELL"] {
        let mut resting: Option<String> = None;
        let mut places = 0;
        for req in &reqs {
            match req["method"].as_str().unwrap() {
                "order.create" if req["params"]["side"] == side => {
                    assert!(
                        resting.is_none(),
                        "a place while {resting:?} rests: {reqs:#?}"
                    );
                    let cid = req["params"]["client_id"].as_str().unwrap();
                    let v = lock(&venue);
                    resting = Some(v.orders.iter().find(|o| o.cid == cid).unwrap().vid.clone());
                    places += 1;
                }
                "order.cancel" | "order.cancel_batch" => {
                    for id in cancelled_ids(req) {
                        if side_of(&venue, &id) == side {
                            assert_eq!(resting.as_deref(), Some(id.as_str()), "{reqs:#?}");
                            resting = None;
                        }
                    }
                }
                // Every order of ours on the market (Stop's).
                "order.cancel_all" => resting = None,
                _ => {}
            }
        }
        assert_eq!(places, 3, "{side}: {reqs:#?}");
        assert!(resting.is_none(), "{side}: Stop left one resting");
    }

    // Per side, in what it printed before Stop: each place acknowledged, and each replace's
    // cancel acknowledged and its order closed before the next place.
    let before_stop = printed.split("\nSTOP ").next().unwrap();
    for side in ["bid", "ask"] {
        let tags: Vec<&str> = before_stop
            .lines()
            .filter(|l| l.split_whitespace().nth(2) == Some(side))
            .map(|l| l.split_whitespace().next().unwrap())
            .collect();
        assert_eq!(
            tags,
            [
                "PLACE",
                "ACK",
                "CANCEL",
                "CANCEL-ACK",
                "CLOSED",
                "PLACE",
                "ACK",
                "CANCEL",
                "CANCEL-ACK",
                "CLOSED",
                "PLACE",
                "ACK"
            ],
            "{side}:\n{printed}"
        );
    }

    // Every place within the caps: its notional at its price at most the $11 resting cap, and
    // a fill of it (from flat) worth at most the $50 inventory cap at the touch.
    for req in reqs.iter().filter(|r| r["method"] == "order.create") {
        assert_eq!(req["params"]["instruction"], "POST_ONLY");
        let px = dec(req["params"]["price"].as_str().unwrap());
        let size = dec(req["params"]["size"].as_str().unwrap());
        assert!(px * size <= dec("11"), "{req}");
        assert!(dec("62100.3") * size <= dec("50"), "{req}");
        // 16 lots: $10.50 at the first touch's ask quote, 62186.6.
        assert_eq!(size, dec("0.00016"), "{req}");
    }
    // The quotes 30 bps behind each touch: the first at 61814.1 / 62186.6.
    let first = |side: &str| {
        reqs.iter()
            .find(|r| r["method"] == "order.create" && r["params"]["side"] == side)
            .map(|r| r["params"]["price"].as_str().unwrap().to_owned())
            .unwrap()
    };
    assert_eq!(
        (first("BUY"), first("SELL")),
        ("61814.1".into(), "62186.6".into())
    );

    assert!(
        printed.contains("PLUMBING TEST, not a strategy"),
        "{printed}"
    );
    assert!(
        printed.contains(
            "QUOTE 16 lots (0.00016 at the size step) per side; caps at 62186.6: \
                          resting 17 lots per side, inventory 80 lots"
        ),
        "{printed}"
    );
    assert_eq!(tagged(&printed, "CYCLE").len(), 3, "{printed}");
    assert!(
        printed.contains(
            "SUMMARY cycles 4 orders placed 6 cancels sent 6 orders cancelled 6 fills 0 \
             inventory 0 lots cancels not ours 0"
        ),
        "{printed}"
    );
    assert!(
        printed.contains("DONE ok (--cycles 4 reached)"),
        "{printed}"
    );
    assert!(printed.contains("STEP stop"), "{printed}");
    // At Stop, once its cancels were answered, GET /positions is compared with the registry's
    // position on every run, fills or not (FBC-hpg6), before the stop line (the market still leased).
    let at_stop = "GET /positions 0 lots, the registry 0 lots (0 fills applied): they agree";
    let stop_line = printed.find("STEP stop").unwrap();
    let check = printed
        .find("POSITION at Stop: +")
        .unwrap_or_else(|| panic!("no Stop comparison:\n{printed}"));
    assert!(check < stop_line, "{printed}");
    assert!(printed[check..].contains(at_stop), "{printed}");
    assert_eq!(printed.matches("POSITION ").count(), 1, "{printed}");
    // No credential, and not the session token, in what it printed or what the stack logged.
    no_credential_in(&printed);
    no_credential_in(&capture::all());
}

#[tokio::test]
async fn after_a_reconnect_every_order_of_ours_is_cancelled_before_any_place() {
    capture::install();
    let venue = venue(vec![b0()]);
    // The resync after the reconnect shows the bid resting but not the ask (a lagging
    // snapshot): the session tells the bid unprotected and the link Safety-cancels it (decision
    // 0080), and the loop's own sweep cancels the ask, which the registry still holds open, by
    // an instrument cancel-all (Paradex's resync is trustworthy, decision 0094).
    lock(&venue).hide_sells_after_first = true;
    // First connection: the opening six, the start's sweep (a cancel-all) and two places, then
    // the stub closes it. Second: the opening six, the bid's Safety cancel, the sweep's
    // cancel-all, two places, Stop's cancel-all.
    let script = reconnecting(&venue, 6 + 1 + 2, 6 + 1 + 1 + 2 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    let conn1 = methods(&venue, 1);
    let first_place = conn1
        .iter()
        .position(|m| m == "order.create")
        .unwrap_or_else(|| panic!("nothing placed after the reconnect: {conn1:?}\n{printed}"));
    // Before any place on the new connection, a cancel of each order of ours conn 0 left: the
    // bid's Safety cancel names it, and the sweep's instrument cancel-all takes the ask.
    let before = &requests(&venue, 1)[..first_place];
    let cancelled: Vec<String> = before.iter().flat_map(cancelled_ids).collect();
    let (bid, ask) = {
        let v = lock(&venue);
        let of = |side: &str| {
            v.orders[..2]
                .iter()
                .find(|o| o.side == side)
                .unwrap()
                .vid
                .clone()
        };
        (of("BUY"), of("SELL"))
    };
    assert!(
        cancelled.contains(&bid),
        "the bid not cancelled first: {conn1:?}\n{printed}"
    );
    assert!(
        !cancelled.contains(&ask),
        "the ask is the sweep's: {conn1:?}\n{printed}"
    );
    assert_eq!(
        before
            .iter()
            .filter(|r| r["method"] == "order.cancel_all")
            .count(),
        1,
        "{conn1:?}\n{printed}"
    );
    assert_eq!(
        conn1[..6].iter().filter(|m| m.contains("order.")).count(),
        1,
        "{conn1:?}"
    );
    assert_eq!(
        conn1.iter().filter(|m| *m == "order.create").count(),
        2,
        "{conn1:?}"
    );
    assert_eq!(
        conn1.iter().filter(|m| *m == "order.modify").count(),
        0,
        "{conn1:?}"
    );
    // The reconnect says how: the instrument cancel-all fbc-oms builds once a trustworthy
    // resync has shown the account's open orders (0005's I7; decision 0094).
    assert!(
        printed.contains(
            "RECONNECT every order of ours still open on the market is cancelled before any \
             place: 1 cancels sent (an instrument cancel-all)"
        ),
        "{printed}"
    );
    assert!(printed.contains("Safety cancel requests"), "{printed}");
    // The Safety cancel, the reconnect's sweep and Stop are ours: no cancel not ours counted.
    assert_eq!(report.cancels_not_ours, 0, "{printed}");
    assert!(!printed.contains("\nWARN"), "{printed}");
    no_credential_in(&printed);
}

#[tokio::test]
async fn after_a_cancel_not_ours_a_reconnects_sweep_cancels_ours_explicitly_never_a_cancel_all() {
    capture::install();
    // RB-aoh5-5 on PR #159: another program cancels cycle 1's bid (a WARN), then the stub closes
    // the order socket. The resync after the reconnect does not show the ask (a lagging
    // snapshot), which the registry still holds open: the reconnect's sweep cancels it by name,
    // never by an instrument cancel-all, which would cancel the other program's orders too.
    let venue = venue(vec![b0()]);
    {
        let mut v = lock(&venue);
        v.cotenant_after_create = vec![(2, Some("BUY"))];
        v.hide_sells_after_first = true;
    }
    // First connection: the opening six, the start's sweep (a cancel-all), two places, then the
    // stub closes it. Second: the opening six, the sweep's cancel of the ask, two places,
    // Stop's batch cancel.
    let script = reconnecting(&venue, 6 + 1 + 2, 6 + 1 + 2 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.cancels_not_ours, 1, "{printed}");
    assert_eq!(warned(&printed, false), 1, "{printed}");
    let conn1 = methods(&venue, 1);
    assert!(
        !conn1.iter().any(|m| m == "order.cancel_all"),
        "an instrument cancel-all after the cancel not ours: {conn1:?}\n{printed}"
    );
    let first_place = conn1
        .iter()
        .position(|m| m == "order.create")
        .unwrap_or_else(|| panic!("nothing placed after the reconnect: {conn1:?}\n{printed}"));
    let before: Vec<String> = requests(&venue, 1)[..first_place]
        .iter()
        .flat_map(cancelled_ids)
        .collect();
    let ask = vids_of(&venue, "SELL")[0].clone();
    assert_eq!(before, [ask], "{conn1:?}\n{printed}");
    let reconnect = tagged(&printed, "RECONNECT");
    assert_eq!(reconnect.len(), 1, "{printed}");
    assert!(
        reconnect[0].contains("no instrument cancel-all: another program cancelled our orders"),
        "{printed}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_resync_position_mismatch_stops_it_with_no_place_after() {
    let venue = venue(vec![b0()]);
    // After the reconnect the venue reports a long position the inventory (flat) does not hold.
    lock(&venue).position_after_first = Some("0.001");
    // First connection: the opening six, the start's sweep (an instrument cancel-all, decision
    // 0094) and two places. Second: the opening six and the Safety cancel of both resting
    // orders; then Stop's instrument cancel-all, and nothing more.
    let script = reconnecting(&venue, 6 + 1 + 2, 6 + 1 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 50);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("a resync found the position other than the registry's"),
        "{printed}"
    );
    let conn1 = methods(&venue, 1);
    assert!(!conn1.iter().any(|m| m == "order.create"), "{conn1:?}");
    assert!(printed.contains("STEP stop"), "{printed}");
    assert!(printed.contains("kill switch on"), "{printed}");
    // Every order of ours ended cancelled all the same.
    assert_eq!(report.cancelled, 2, "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    let stop = printed.find("STOP ").unwrap();
    assert!(!printed[stop..].contains("PLACE "), "{printed}");
}

#[tokio::test]
async fn the_interrupt_stops_it_cancelling_every_order() {
    let venue = venue(vec![b0()]);
    let signal = Arc::new(Notify::new());
    // The interrupt comes as cycle 2 reads the touch: cycle 1's two orders rest, acknowledged.
    lock(&venue).signal = Some((3, Arc::clone(&signal)));
    // The opening six, the start's sweep, two places, Stop's: the instrument cancel-alls.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1_000);
    let interrupt = async move { signal.notified().await };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (2, 2),
        "{printed}"
    );
    assert_eq!(report.cancelled, 2, "{printed}");
}

#[tokio::test]
async fn a_cancel_that_comes_back_unknown_stops_it() {
    // The touch moves for cycle 2; the bid's cancel is answered ALREADY_CLOSED, which Paradex's
    // single cancel decodes as Unknown (FBC-rht4), and the stub leaves the bid open: the loop
    // Stops, places nothing more, and Stop cancels the bid again.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).already_closed_once = true;
    // The opening six, the start's sweep (an instrument cancel-all, decision 0094), two places,
    // the two replace cancels, and Stop's instrument cancel-all, which ends the bid.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 10);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("came back Unknown"), "{printed}");
    let reqs = requests(&venue, 0);
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}"
    );
    let bid = lock(&venue).orders[0].vid.clone();
    assert_eq!(cancelled_ids(&reqs[9]), [bid], "{sent:?}");
    assert_eq!(reqs[11]["method"], "order.cancel_all", "{sent:?}");
    assert_eq!(reqs.len(), 12, "{sent:?}");
    assert!(printed.contains("came back Unknown"), "{printed}");
    assert!(printed.contains("went Unknown"), "{printed}");
    assert!(printed.contains("STEP stop"), "{printed}");
    // Every order of ours ended all the same.
    assert_eq!(report.cancelled, 2, "{printed}");
}

#[tokio::test]
async fn a_refused_login_stops_it_at_once() {
    let venue = venue(vec![b0()]);
    let body = json!({
        "error": "STARKNET_SIGNATURE_VERIFICATION_FAILED",
        "message": "verification failed on Curve",
    });
    let script = WsScript::new(vec![Step::Accept, Step::Accept, Step::Accept]);
    let stub = StubServer::start(script, routes(&venue, reply(401, body.to_string())))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.step_timeout_secs = 40;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    assert!(started.elapsed() < Duration::from_secs(30), "{printed}");
    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("a login was refused: the Paradex login was refused: HTTP 401"),
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(methods(&venue, 0).is_empty());
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_mainnet_url_or_chain_id_is_refused_before_any_connection() {
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 1);
    let out: testnet::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    for chain in ["PRIVATE_SN_PARACLEAR_MAINNET", "PRIVATE_SN_POTC_SEPOLIA"] {
        let refused = quote::run(
            &opts,
            Some(chain),
            creds(),
            out.clone(),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(refused.contains("not the testnet chain id"), "{refused}");
        assert!(!refused.contains("POTC"), "{refused}");
    }
    let mut mainnet = opts.clone();
    mainnet.rest_url = "https://api.prod.paradex.trade/v1".to_owned();
    mainnet.ws_url = "wss://ws.api.prod.paradex.trade/v1".to_owned();
    let refused = quote::run(&mainnet, None, creds(), out.clone(), std::future::pending())
        .await
        .unwrap_err();
    assert!(refused.contains("testnet only"), "{refused}");
    assert!(stub.connections().is_empty());
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    // The command line refuses mainnet URLs too.
    let mut argv = required();
    argv.extend(strings(&[
        "--rest-url",
        "https://api.prod.paradex.trade/v1",
    ]));
    assert!(args::parse(argv).unwrap_err().contains("testnet only"));
}

#[tokio::test]
async fn an_order_on_the_market_that_is_not_ours_refuses_the_run_before_start() {
    let venue = venue(vec![b0()]);
    lock(&venue).orders.push(Held {
        vid: "1759500000000099999".to_owned(),
        cid: "someone-else".to_owned(),
        side: "BUY".to_owned(),
        price: "60000".to_owned(),
        size: "0.0001".to_owned(),
        open: true,
    });
    let stub = StubServer::start(one_connection(&venue, 6), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("not ours"), "{printed}");
    assert!(!printed.contains("STEP start"), "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
}

#[tokio::test]
async fn the_duration_ends_the_loop() {
    // Cycle 1's touch (GET /orderbook read 2) is never answered, so the frames sent do not
    // depend on how much of the second the start took: whenever --duration passes, during the
    // start or the touch, nothing was placed (RB-elg7-4 on PR #149). The start's sweep and Stop
    // each send the instrument cancel-all (decision 0094): the start runs whatever the
    // duration, which only the cycles watch. The run ends at --duration, not at
    // --step-timeout's 10 s.
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1_000_000);
    opts.duration_secs = 1;
    opts.rest_url = holding_relay(&opts.rest_url, 2, Arc::new(Notify::new()), None).await;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--duration 1 s reached", "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 2),
        "{printed}"
    );
}

#[tokio::test]
async fn an_earlier_runs_order_is_cancelled_before_the_first_place() {
    // An order of ours an earlier run left resting: the session tells it unprotected as the
    // connection opens, the link Safety-cancels it, and the loop places only once it ended.
    let venue = venue(vec![b0()]);
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let fmt = fbc_venue_paradex::exec::exec_caps().order.client_id;
    let cid = encode_cid(&fmt, mint.mint().unwrap()).unwrap().to_string();
    lock(&venue).orders.push(Held {
        vid: "1759500000000088888".to_owned(),
        cid,
        side: "SELL".to_owned(),
        price: "70000".to_owned(),
        size: "0.00001".to_owned(),
        open: true,
    });
    // The opening six, the Safety cancel, the start's sweep and Stop's instrument cancel-alls
    // (decision 0094), and two places.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(sent[6], "order.cancel_batch", "{sent:?}");
    assert_eq!(
        cancelled_ids(&requests(&venue, 0)[6]),
        ["1759500000000088888"]
    );
    assert_eq!(sent[7], "order.cancel_all", "{sent:?}");
    assert_eq!(report.cancelled, 3, "{printed}");
    // The earlier run's order ended by our Safety cancel at the start, before the start's
    // sweep: ours, never a cancel not ours (FBC-aoh5).
    assert_eq!(report.cancels_not_ours, 0, "{printed}");
    assert!(!printed.contains("\nWARN"), "{printed}");
}

#[tokio::test]
async fn a_fill_during_a_replace_requotes_from_the_position_it_left_never_past_the_usd_cap() {
    // The market rose 29% since the first touch, so fbc-oms's caps in lots (from the first
    // touch) admit more than the USD caps at the market. Cycle 2 replaces both quotes; the bid's
    // cancel races a fill of all of it (12 lots; the venue sends the fill twice). Placing the
    // bid again from the position the cycle read (flat) would admit 24 lots long, $19.2 at
    // 80000, past the $16 inventory cap; the loop requotes from the position the fill left.
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    lock(&venue).fill_next_buy_cancel = true;
    // The opening six, the start's sweep (an instrument cancel-all, decision 0094), two places,
    // two replace cancels, the ask's new place, Stop's cancel-all.
    let script = one_connection(&venue, 6 + 1 + 2 + 2 + 1 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    // 12 lots a quote; the caps admit 25 lots of inventory at the first touch.
    assert!(printed.contains("QUOTE 12 lots"), "{printed}");
    assert!(printed.contains("inventory 25 lots"), "{printed}");
    let reqs = requests(&venue, 0);
    let buys = reqs
        .iter()
        .filter(|r| r["method"] == "order.create" && r["params"]["side"] == "BUY")
        .count();
    assert_eq!(
        buys, 1,
        "the bid was placed again after the fill: {reqs:#?}\n{printed}"
    );
    assert!(
        printed.contains("SKIP bid: a fill would take the position to 24 lots"),
        "{printed}"
    );
    // The fill is counted once, its duplicate not at all; the inventory moved by it.
    assert_eq!((report.fills, report.inventory), (1, 12), "{printed}");
    assert_eq!(tagged(&printed, "FILL").len(), 1, "{printed}");
}

/// [`a_fill_during_a_replace_requotes_from_the_position_it_left_never_past_the_usd_cap`]'s run
/// (cycle 2's bid cancel races a fill of all of it, 12 lots) with the reads of `GET /positions`
/// after the resync answered `script` in turn, then the position the fills left, and a step
/// timeout of `timeout` seconds; the stub answers `frames` frames. Its report, what it printed,
/// and the methods the session sent.
async fn fill_run(
    script: Vec<Option<&'static str>>,
    timeout: u64,
    frames: usize,
) -> (quote::Report, String, Vec<String>) {
    let (report, printed, venue) = fill_run_until(
        script,
        timeout,
        frames,
        None,
        |_| {},
        std::future::pending(),
    )
    .await;
    (report, printed, methods(&venue, 0))
}

/// [`fill_run`]'s run, with `signal` notified at that `GET /positions` read, the options
/// changed by `tweak`, and the run interrupted when `interrupt` completes: its report, what it
/// printed, and the stub venue.
async fn fill_run_until(
    script: Vec<Option<&'static str>>,
    timeout: u64,
    frames: usize,
    signal: Option<(usize, Arc<Notify>)>,
    tweak: impl FnOnce(&mut Options),
    interrupt: impl std::future::Future<Output = ()>,
) -> (quote::Report, String, Shared) {
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    {
        let mut v = lock(&venue);
        v.fill_next_buy_cancel = true;
        v.positions_script = script.into();
        v.positions_signal = signal;
    }
    let stub = StubServer::start(one_connection(&venue, frames), routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    opts.step_timeout_secs = timeout;
    tweak(&mut opts);
    let (report, printed) = run_until(&opts, interrupt).await;
    tokio::time::timeout(Duration::from_secs(30), stub.finished())
        .await
        .unwrap_or_else(|_| panic!("the stub's script did not play to its end:\n{printed}"))
        .unwrap();
    (report, printed, venue)
}

/// The opening six, the start's sweep, two places, the two replace cancels (the bid's raced by
/// its fill), the ask's new place and Stop's cancel-all: the loop that goes on after the fill.
const GOES_ON: usize = 6 + 1 + 2 + 2 + 1 + 1;

#[tokio::test]
async fn after_the_first_fill_get_positions_is_compared_with_the_registry_and_agreeing_goes_on() {
    // GET /positions follows the fill (12 lots long): the check after it agrees at the first
    // read, and the loop goes on to place the ask again; at Stop it agrees again.
    let (report, printed, sent) = fill_run(Vec::new(), 10, GOES_ON).await;
    assert!(report.ok, "{printed}");
    assert_eq!((report.fills, report.inventory), (1, 12), "{printed}");
    let after = printed
        .find("POSITION after the first fill: +")
        .unwrap_or_else(|| panic!("no check after the fill:\n{printed}"));
    let agree = "GET /positions 12 lots, the registry 12 lots (1 fills applied): they agree";
    assert!(printed[after..].starts_with("POSITION after the first fill: +"));
    assert!(
        printed[after..].lines().next().unwrap().contains(agree),
        "{printed}"
    );
    // After the FILL line, before the ask's new place.
    assert!(printed.find("\nFILL ").unwrap() < after, "{printed}");
    let last_place = printed.rfind("\nPLACE ").unwrap();
    assert!(after < last_place, "{printed}");
    assert!(!printed.contains("DIFFER"), "{printed}");
    assert!(
        printed[printed.find("POSITION at Stop: +").unwrap()..].contains(agree),
        "{printed}"
    );
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        3,
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_mismatch_after_the_first_fill_that_clears_on_the_re_read_is_printed_and_goes_on() {
    // The first read after the fill still shows the account flat (GET /positions lagging the
    // fill stream); the re-read, one step timeout later, shows the 12 lots: both readings are
    // printed, with their times, and the loop goes on.
    let (report, printed, sent) = fill_run(vec![None], 2, GOES_ON).await;
    assert!(report.ok, "{printed}");
    let first = printed
        .find("POSITION after the first fill: +")
        .unwrap_or_else(|| panic!("no check after the fill:\n{printed}"));
    let line = printed[first..].lines().next().unwrap();
    assert!(
        line.contains("GET /positions 0 lots, the registry 12 lots (1 fills applied): they DIFFER"),
        "{printed}"
    );
    assert!(line.contains("read once more in 2 s"), "{printed}");
    let again = printed
        .find("POSITION re-read after the first fill: +")
        .unwrap_or_else(|| panic!("no re-read:\n{printed}"));
    let line = printed[again..].lines().next().unwrap();
    assert!(
        line.contains(
            "GET /positions 12 lots, the registry 12 lots (1 fills applied): they agree now"
        ),
        "{printed}"
    );
    // Both readings' times, the re-read at least the step timeout after the first.
    let ms = |line: &str| -> u128 {
        let at = line.find(": +").unwrap() + 3;
        line[at..].split("ms").next().unwrap().parse().unwrap()
    };
    let (t1, t2) = (
        ms(printed[first..].lines().next().unwrap()),
        ms(printed[again..].lines().next().unwrap()),
    );
    assert!(t2 >= t1 + 2_000, "{t1} {t2}\n{printed}");
    assert!(
        line.contains(&format!("first +{t1}ms GET /positions 0 lots")),
        "{printed}"
    );
    assert!(!printed.contains(positions::HAND_CHECK), "{printed}");
    // The loop went on: the ask placed again after the re-read.
    assert!(again < printed.rfind("\nPLACE ").unwrap(), "{printed}");
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        3,
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_mismatch_after_the_first_fill_that_persists_stops_it_with_no_place_after() {
    // GET /positions keeps showing the account flat after the 12-lot fill: the re-read still
    // disagrees, so the loop Stops (kill switch on, every order of ours cancelled by the
    // instrument cancel-all) and places nothing after the fill; the run fails, saying to check
    // the position by hand. The Stop comparison disagrees as well.
    let script = vec![None, None, None, None];
    // The opening six, the start's sweep, two places, the two replace cancels and Stop's
    // cancel-all: the ask is not placed again.
    let (report, printed, sent) = fill_run(script, 2, 6 + 1 + 2 + 2 + 1).await;
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains(positions::HAND_CHECK), "{printed}");
    assert!(
        report.ended.contains("after the first fill"),
        "{}\n{printed}",
        report.ended
    );
    // Both readings, in the reason and on their lines.
    assert_eq!(
        report
            .ended
            .matches("GET /positions 0 lots, the registry 12 lots (1 fills applied)")
            .count(),
        2,
        "{}",
        report.ended
    );
    assert!(
        printed.contains("POSITION re-read after the first fill: +"),
        "{printed}"
    );
    assert!(printed.contains("they STILL DIFFER"), "{printed}");
    // No place after the fill: only cycle 1's two.
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}\n{printed}"
    );
    let fill = printed.find("\nFILL ").unwrap();
    assert!(!printed[fill..].contains("\nPLACE "), "{printed}");
    // Stop: the kill switch and the instrument cancel-all, last on the wire.
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));
    assert!(printed.contains("kill switch on"), "{printed}");
    assert!(printed.contains("POSITION re-read at Stop: +"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    assert_eq!(report.inventory, 12, "{printed}");
}

#[tokio::test]
async fn after_a_position_check_the_pass_reads_a_fresh_touch_before_it_places() {
    // RA157-2 / RB157-1 on PR #157: the check after the first fill takes at least one REST
    // read (and, on a mismatch, a re-read after the step timeout) while the touch the pass holds
    // ages. The ask placed after the check must be priced from a GET /orderbook read after the
    // check's last read, never from one read before it: both for a check that agrees at once
    // and for a mismatch the re-read clears.
    for (script, last_check_read) in [(Vec::new(), 1), (vec![None], 2)] {
        let (report, printed, venue) =
            fill_run_until(script, 2, GOES_ON, None, |_| {}, std::future::pending()).await;
        assert!(report.ok, "{printed}");
        let v = lock(&venue);
        // The resync's read, the check's read(s), the Stop comparison's.
        assert_eq!(
            v.books_at_positions.len(),
            last_check_read + 2,
            "{:?}\n{printed}",
            v.books_at_positions
        );
        assert_eq!(v.books_at_create.len(), 3, "{:?}", v.books_at_create);
        let checked_at = v.books_at_positions[last_check_read];
        let priced_at = v.books_at_create[2];
        assert!(
            priced_at > checked_at,
            "the place after the check was priced from a touch read before it (books at the \
             positions reads {:?}, at the places {:?}):\n{printed}",
            v.books_at_positions,
            v.books_at_create
        );
    }
}

#[tokio::test]
async fn the_interrupt_during_the_position_check_stops_it_at_once() {
    // RA157-1 / RB157-2 on PR #157: the check after the first fill disagrees (GET /positions
    // still flat), so its re-read is a step timeout (10 s) away; the interrupt comes as that
    // first read is answered. The loop Stops at once, without waiting out the re-read, places
    // nothing after the fill, and the Stop comparison (GET /positions now following the fill)
    // agrees.
    let signal = Arc::new(Notify::new());
    let came: Rc<Cell<Option<std::time::Instant>>> = Rc::new(Cell::new(None));
    let interrupt = {
        let (signal, came) = (Arc::clone(&signal), Rc::clone(&came));
        async move {
            signal.notified().await;
            came.set(Some(std::time::Instant::now()));
        }
    };
    // The opening six, the start's sweep, two places, the two replace cancels and Stop's
    // cancel-all: the ask is not placed again.
    let (report, printed, venue) = fill_run_until(
        vec![None],
        10,
        6 + 1 + 2 + 2 + 1,
        Some((2, signal)),
        |_| {},
        interrupt,
    )
    .await;
    let ended = std::time::Instant::now();
    let came = came
        .get()
        .unwrap_or_else(|| panic!("no interrupt:\n{printed}"));
    assert!(
        ended.duration_since(came) < Duration::from_secs(4),
        "the interrupt waited {:?} for the position check:\n{printed}",
        ended.duration_since(came)
    );
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert!(report.ok, "{printed}");
    assert!(
        !printed.contains("POSITION re-read after the first fill"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "placed after the interrupt: {sent:?}\n{printed}"
    );
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));
    let at_stop = printed
        .find("POSITION at Stop: +")
        .unwrap_or_else(|| panic!("no Stop comparison:\n{printed}"));
    assert!(
        printed[at_stop..]
            .lines()
            .next()
            .unwrap()
            .contains("GET /positions 12 lots, the registry 12 lots (1 fills applied): they agree"),
        "{printed}"
    );
}

#[tokio::test]
async fn the_duration_passing_during_the_position_check_ends_the_loop_at_once() {
    // RA157-1 / RB157-2 on PR #157: as the interrupt, --duration (1 s) passing while the check
    // after the first fill waits for its re-read (a step timeout, 10 s, away) ends the loop at
    // --duration, not after the re-read; nothing is placed after the fill, and the Stop
    // comparison agrees.
    let began = std::time::Instant::now();
    let (report, printed, venue) = fill_run_until(
        vec![None],
        10,
        6 + 1 + 2 + 2 + 1,
        None,
        |opts| opts.duration_secs = 1,
        std::future::pending(),
    )
    .await;
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "the run took {:?}:\n{printed}",
        began.elapsed()
    );
    assert_eq!(report.ended, "--duration 1 s reached", "{printed}");
    assert!(report.ok, "{printed}");
    assert!(
        printed.contains("POSITION after the first fill: +"),
        "{printed}"
    );
    assert!(
        !printed.contains("POSITION re-read after the first fill"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}\n{printed}"
    );
    assert!(printed.contains("POSITION at Stop: +",), "{printed}");
}

#[tokio::test]
async fn the_stop_comparison_runs_before_the_market_lease_is_released() {
    // Codex r4305979062 on PR #157: Stop's comparison (here a mismatch the re-read clears, so
    // two reads a step timeout apart) runs while the run still holds the market's lease, after
    // Stop's cancels are answered and before the market is disarmed and its leases released;
    // no other process can take the market while the run compares against its registry.
    let venue = venue(vec![b0()]);
    lock(&venue).positions_script = vec![Some("0.00012"), None].into();
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1);
    opts.step_timeout_secs = 2;
    lock(&venue).lease_probe = Some(opts.lease_dir.clone());
    let (report, printed) = run_against(&opts).await;
    tokio::time::timeout(Duration::from_secs(30), stub.finished())
        .await
        .unwrap_or_else(|_| panic!("the stub's script did not play to its end:\n{printed}"))
        .unwrap();
    assert!(report.ok, "{printed}");
    // The resync's read, Stop's read and its re-read: the lease held at each.
    assert_eq!(lock(&venue).lease_held, vec![true; 3], "{printed}");
    let cancel_ack = printed.rfind("\nCANCEL-ACK ").unwrap();
    let first = printed.find("POSITION at Stop: +").unwrap();
    let again = printed.find("POSITION re-read at Stop: +").unwrap();
    let stop = printed.find("STEP stop").unwrap();
    assert!(
        cancel_ack < first && first < again && again < stop,
        "{printed}"
    );
    assert!(printed[stop..].contains("leases released"), "{printed}");
}

#[tokio::test]
async fn the_stop_comparison_fails_a_run_whose_mismatch_persists_and_passes_one_that_clears() {
    // No fill: one cycle, then Stop. GET /positions at Stop shows a long of 12 lots the
    // registry (flat) does not hold: a mismatch the re-read confirms fails the run with both
    // readings; one the re-read clears passes it.
    for (script, ok) in [
        (vec![Some("0.00012"), Some("0.00012")], false),
        (vec![Some("0.00012"), None], true),
    ] {
        let venue = venue(vec![b0()]);
        lock(&venue).positions_script = script.into();
        // The opening six, the start's sweep, two places, Stop's cancel-all.
        let stub = StubServer::start(
            one_connection(&venue, 6 + 1 + 2 + 1),
            routes(&venue, login_ok()),
        )
        .await
        .unwrap();
        let mut opts = options(&stub, 1);
        opts.step_timeout_secs = 2;
        let (report, printed) = run_against(&opts).await;
        tokio::time::timeout(Duration::from_secs(30), stub.finished())
            .await
            .unwrap_or_else(|_| panic!("the stub's script did not play to its end:\n{printed}"))
            .unwrap();
        assert_eq!(report.ok, ok, "{printed}");
        let stop = printed.find("STEP stop").unwrap();
        let first = printed
            .find("POSITION at Stop: +")
            .unwrap_or_else(|| panic!("no Stop comparison:\n{printed}"));
        assert!(first < stop, "{printed}");
        assert!(
            printed[first..].lines().next().unwrap().contains(
                "GET /positions 12 lots, the registry 0 lots (0 fills applied): they DIFFER"
            ),
            "{printed}"
        );
        assert!(printed.contains("POSITION re-read at Stop: +"), "{printed}");
        if ok {
            assert!(printed.contains("they agree now"), "{printed}");
            assert!(printed.contains("DONE ok"), "{printed}");
        } else {
            assert!(printed.contains("they STILL DIFFER"), "{printed}");
            assert!(report.ended.contains(positions::HAND_CHECK), "{printed}");
            assert!(report.ended.contains("at Stop"), "{printed}");
            assert!(printed.contains("DONE failed"), "{printed}");
        }
    }
}

#[tokio::test]
async fn a_cancel_all_whose_fate_is_unknown_stops_it_and_fails_the_run() {
    // Every instrument cancel-all is answered with an internal error (its fate unknown): the
    // start's sweep (decision 0094) comes back Unknown, so the loop Stops before any place, and
    // Stop's cancel-all comes back Unknown too: not every Stop cancel was accepted, and the run
    // fails.
    let venue = venue(vec![b0()]);
    lock(&venue).refuse_cancel_all = true;
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
    // Paradex's internal error leaves a request's fate unknown.
    assert!(report.ended.contains("came back Unknown"), "{printed}");
    assert!(printed.contains("Stop's cancel"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        &sent[6..],
        ["order.cancel_all", "order.cancel_all"],
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_frame_the_codec_cannot_decode_stops_it_at_once() {
    // After the second place's reply the venue sends a frame no codec decodes: it may have held
    // a fill or an order event, so the loop Stops at once instead of quoting on (Codex
    // r4243908424), and every order of ours is cancelled.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).garbage_after_create = Some(2);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 10);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("could not decode"), "{printed}");
    // Cycle 1 may or may not have settled when the frame was read; nothing was placed after.
    assert!(report.cycles <= 1, "{printed}");
    assert_eq!((report.places_sent, report.cancelled), (2, 2), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_quote_the_venue_closes_post_only_would_cross_is_placed_again_next_cycle() {
    // Paradex's risk check closes cycle 1's first quote POST_ONLY_WOULD_CROSS (the touch moved
    // through it between GET /orderbook and the place): an asynchronous reject of our own
    // post-only place, which leaves that side out of the cycle; cycle 2 places it again, and the
    // run goes on (RB-elg7-1).
    let venue = venue(vec![b0()]);
    lock(&venue).close_create = Some((1, "POST_ONLY_WOULD_CROSS"));
    // The opening six, two places, cycle 2's place again, Stop's cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 2 reached", "{printed}");
    assert_eq!(report.places_sent, 3, "{printed}");
    assert!(printed.contains("POST_ONLY_WOULD_CROSS"), "{printed}");
    assert!(printed.contains("DONE ok"), "{printed}");
    let creates = requests(&venue, 0)
        .into_iter()
        .filter(|r| r["method"] == "order.create")
        .map(|r| r["params"]["side"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    // The side the venue closed is the one placed again.
    assert_eq!(creates[2], creates[0], "{creates:?}\n{printed}");
}

#[tokio::test]
async fn an_asynchronous_reject_other_than_post_only_would_cross_stops_it() {
    // The risk check closes cycle 1's first quote for another reason: the loop Stops.
    let venue = venue(vec![b0()]);
    lock(&venue).close_create = Some((1, "NOT_ENOUGH_MARGIN"));
    // The opening six, two places, Stop's cancel of the other quote.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("asynchronous reject"), "{printed}");
    assert!(report.ended.contains("NOT_ENOUGH_MARGIN"), "{printed}");
    assert_eq!(report.places_sent, 2, "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn an_order_book_older_than_one_already_read_stops_it_with_no_place_after() {
    // Cycle 2's GET /orderbook answers seq 4 after seq 5 (a stale cache or replica) at a lower
    // price: the loop Stops rather than quote from it (Codex r4244080123).
    let venue = venue(vec![
        book(5, Some("62000.2"), Some("62000.5")),
        book(5, Some("62000.2"), Some("62000.5")),
        book(4, Some("50000.0"), Some("50000.3")),
    ]);
    // The opening six, two places, Stop's cancel of both.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report.ended.contains("answered seq 4 after seq 5"),
        "{printed}"
    );
    assert_eq!((report.places_sent, report.cancelled), (2, 2), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_venue_position_the_inventory_does_not_hold_plans_nothing() {
    // After cycle 1's second place the venue reports a long of 12 lots and no fill comes: the
    // inventory stays flat. Cycle 2's touch moved, yet no pass plans (no replace) while they
    // disagree; the book does not settle in time and the loop Stops, cancelling both quotes
    // (Codex r4244080127).
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).position_after_create = Some((2, "0.00012"));
    // The opening six, two places, Stop's cancel of both.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 5);
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .contains("the venue's position (12 lots) and the inventory (0 lots) agreeing"),
        "{printed}"
    );
    assert!(
        printed.contains("nothing is planned until they agree"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}\n{printed}"
    );
    assert_eq!((report.places_sent, report.cancelled), (2, 2), "{printed}");
}

#[tokio::test]
async fn an_order_filled_on_its_update_before_its_fill_is_read_plans_nothing() {
    // Cycle 2 replaces both quotes; the bid's cancel races a fill of all of it that the order
    // stream reports (CLOSED, nothing open) but the fill stream never brings: the inventory
    // stays flat. No pass plans (the bid is not placed again from the flat inventory) while the
    // venue's fills and those read disagree; the book does not settle in time and the loop
    // Stops (Codex r4244145969).
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    lock(&venue).filled_unsent_next_buy_cancel = true;
    // The opening six, two places, two replace cancels; nothing rests for Stop to cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 5);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .contains("(12 lots) and the fills read (0 lots) agreeing"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}\n{printed}"
    );
    assert_eq!(report.places_sent, 2, "{printed}");
}

#[tokio::test]
async fn a_client_id_high_water_mark_near_the_end_of_the_sequence_space_refuses_to_mint() {
    // The kept mark is within RESERVE of u64::MAX: the loop refuses to mint, places nothing,
    // and keeps the mark as it was, never a wrapped one (Codex r4244080134).
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(one_connection(&venue, 6), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let hwm = testnet::HighWater::at(&opts.lease_dir, Namespace::new(1));
    let near = u64::MAX - 5;
    hwm.write(near).unwrap();
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .contains("client-id sequence space is exhausted"),
        "{printed}"
    );
    assert_eq!(report.places_sent, 0, "{printed}");
    assert_eq!(hwm.read().unwrap(), near, "{printed}");
}

/// A TCP relay in front of the stub's HTTP port (`base`, its `http_url("/v1")`) that holds the
/// `n`th `GET /v1/orderbook` request (from 1), telling `signal` as it holds it, until `release`
/// is notified (never, with none): a touch the venue answers late or not at all. Its own base
/// URL.
async fn holding_relay(
    base: &str,
    n: usize,
    signal: Arc<Notify>,
    release: Option<Arc<Notify>>,
) -> String {
    holding_relay_on(base, b"GET /v1/orderbook", n, signal, release).await
}

/// As [`holding_relay`], holding the `n`th request whose bytes hold `needle` (a request line's
/// start, `GET /v1/positions`).
async fn holding_relay_on(
    base: &str,
    needle: &'static [u8],
    n: usize,
    signal: Arc<Notify>,
    release: Option<Arc<Notify>>,
) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = base
        .strip_prefix("http://")
        .and_then(|s| s.strip_suffix("/v1"))
        .unwrap()
        .to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicU64::new(0));
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                return;
            };
            let (seen, signal, release) = (Arc::clone(&seen), Arc::clone(&signal), release.clone());
            tokio::spawn(async move {
                let (mut from_client, mut to_client) = client.into_split();
                let (mut from_server, mut to_server) = server.into_split();
                let up = async move {
                    let mut buf = vec![0u8; 16_384];
                    loop {
                        let k = match from_client.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(k) => k,
                        };
                        let read = &buf[..k];
                        if read.windows(needle.len()).any(|w| w == needle) {
                            let at = seen.fetch_add(1, Ordering::SeqCst) + 1;
                            if at == n as u64 {
                                signal.notify_one();
                                match &release {
                                    Some(release) => release.notified().await,
                                    None => std::future::pending::<()>().await,
                                }
                            }
                        }
                        if to_server.write_all(read).await.is_err() {
                            break;
                        }
                    }
                };
                let down = async move {
                    let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
                };
                tokio::join!(up, down);
            });
        }
    });
    format!("http://{addr}/v1")
}

/// A WebSocket relay to the stub at `ws_url`: its first connection is cut (both sockets
/// dropped, no close frame) once `cut` is notified; each later one is piped through, `again`
/// notified as it is accepted.
async fn cutting_relay(ws_url: &str, cut: Arc<Notify>, again: Arc<Notify>) -> String {
    let target = ws_url
        .strip_prefix("ws://")
        .and_then(|s| s.strip_suffix("/v1"))
        .unwrap()
        .to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut first = true;
        while let Ok((mut client, _)) = listener.accept().await {
            let Ok(mut server) = tokio::net::TcpStream::connect(&target).await else {
                return;
            };
            let was_first = std::mem::replace(&mut first, false);
            if !was_first {
                again.notify_one();
            }
            let cut = Arc::clone(&cut);
            tokio::spawn(async move {
                let pipe = tokio::io::copy_bidirectional(&mut client, &mut server);
                if was_first {
                    tokio::select! {
                        _ = pipe => {}
                        () = cut.notified() => {}
                    }
                } else {
                    let _ = pipe.await;
                }
            });
        }
    });
    format!("ws://{addr}/v1")
}

#[tokio::test]
async fn a_connection_end_during_the_high_water_write_keeps_the_client_id_mint() {
    // Cycle 1's first pass writes the client-id high-water mark. The write is held, and while
    // it is in flight the relay cuts the order socket; once the session has reconnected the
    // write is let go. Nothing was placed, so nothing of ours rests. The loop must keep the
    // mint through the reconnect and quote, not Stop with "no client-id mint" (Codex
    // r4244206466, RB-elg7-6 on PR #149).
    let venue = venue(vec![b0()]);
    // First connection: the opening six, then the cut. Second: the opening six, two places,
    // Stop's cancel.
    let mut steps = vec![Step::Accept];
    steps.extend(answered(&venue, 0, 6));
    steps.push(Step::Accept);
    steps.extend(answered(&venue, 1, 6 + 1 + 2 + 1));
    let stub = StubServer::start(WsScript::new(steps), routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 1);
    let (cut, again) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    opts.ws_url = cutting_relay(&opts.ws_url, Arc::clone(&cut), Arc::clone(&again)).await;
    let hold = testnet::held_writes::hold(&opts.lease_dir);
    tokio::spawn(async move {
        hold.entered().await;
        cut.notify_one();
        again.notified().await;
        hold.release();
    });
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 1 reached", "{printed}");
    assert!(printed.contains("RECONNECT"), "{printed}");
    assert_eq!(
        methods(&venue, 1)
            .iter()
            .filter(|m| *m == "order.create")
            .count(),
        2,
        "{printed}"
    );
    // One write of the mark, the held one, kept: none started beside it.
    let hwm = testnet::HighWater::at(&opts.lease_dir, Namespace::new(1));
    let kept = format!(
        "MARK client-id high-water mark {} kept",
        hwm.read().unwrap()
    );
    assert_eq!(printed.matches("MARK client-id").count(), 1, "{printed}");
    assert!(printed.contains(&kept), "{printed}");
}

#[tokio::test]
async fn the_interrupt_while_a_pass_reads_a_fresh_touch_places_nothing_after() {
    // As in the fill-during-a-replace test, the bid's cancel races a fill of all of it, so the
    // pass reads a fresh touch (GET /orderbook read 4). The relay holds that read; the interrupt
    // comes, and only then is the read answered. The pass must take the interrupt before it
    // plans: nothing is placed after it (Codex r4243989985).
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    lock(&venue).fill_next_buy_cancel = true;
    // The opening six, the start's sweep, two places, two replace cancels, and Stop's
    // instrument cancel-all (decision 0094), though nothing rests for it to cancel.
    let script = one_connection(&venue, 6 + 1 + 2 + 2 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let (signal, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    opts.rest_url = holding_relay(
        &opts.rest_url,
        4,
        Arc::clone(&signal),
        Some(Arc::clone(&release)),
    )
    .await;
    // The read is answered as the interrupt is delivered, before the loop sees either.
    let interrupt = async move {
        signal.notified().await;
        release.notify_one();
    };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert!(report.ok, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "placed after the interrupt: {sent:?}\n{printed}"
    );
    assert_eq!((report.places_sent, report.fills), (2, 1), "{printed}");
}

#[tokio::test]
async fn the_interrupt_while_the_touch_is_unanswered_stops_it_at_once() {
    // Cycle 2's GET /orderbook is never answered; the interrupt comes as it is sent. The loop
    // Stops at once, cancelling the quotes cycle 1 left, instead of waiting out --step-timeout
    // for the answer (Codex r4243989987).
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let signal = Arc::new(Notify::new());
    let mut opts = options(&stub, 1_000);
    opts.rest_url = holding_relay(&opts.rest_url, 3, Arc::clone(&signal), None).await;
    opts.step_timeout_secs = 20;
    let interrupt = async move { signal.notified().await };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent, report.cancelled),
        (2, 2, 2),
        "{printed}"
    );
}

#[test]
fn a_late_answer_to_a_released_request_is_told_from_the_sessions_own_arm() {
    // compact() releases a place once its first answer (Paradex's Provisional) is in; its
    // Accepted{Final} can come after, with the link no longer holding the request. The released
    // requests are remembered, so that answer is not counted as a cancel-on-disconnect arm
    // (RB-elg7-5 on PR #149); the oldest are forgotten past quote::Released::KEPT.
    let mut released = quote::Released::default();
    assert_eq!(released.answer(RpcId(1)), quote::Unheld::Arm);
    for n in 1..=3 {
        released.release(RpcId(n));
    }
    assert_eq!(released.answer(RpcId(2)), quote::Unheld::Late);
    assert_eq!(released.answer(RpcId(4)), quote::Unheld::Arm);
    let kept = u64::try_from(quote::Released::KEPT).unwrap();
    for n in 4..=3 + kept {
        released.release(RpcId(n));
    }
    assert_eq!(
        released.answer(RpcId(3)),
        quote::Unheld::Arm,
        "the oldest are forgotten"
    );
    assert_eq!(released.answer(RpcId(4)), quote::Unheld::Late);
    assert_eq!(released.answer(RpcId(3 + kept)), quote::Unheld::Late);
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).to_owned()).collect()
}

/// Every required flag, valid.
fn required() -> Vec<String> {
    strings(&[
        "--sole-trader",
        "--namespace",
        "7",
        "--market",
        "BTC-USD-PERP",
        "--tick",
        "0.1",
        "--step",
        "0.00001",
        "--min-notional",
        "10",
        "--resting-cap-usd",
        "11",
        "--inventory-cap-usd",
        "50",
        "--order-usd",
        "10.5",
        "--bid-bps",
        "25",
        "--ask-bps",
        "35",
        "--duration",
        "600",
    ])
}

fn parsed(argv: Vec<String>) -> Options {
    match args::parse(argv) {
        Ok(Parsed::Quote(opts)) => *opts,
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_command_line_takes_every_required_flag_and_defaults_to_testnet() {
    let opts = parsed(required());
    assert_eq!((opts.bid_bps, opts.ask_bps), (25, 35));
    assert_eq!((opts.duration_secs, opts.cycles), (600, None));
    assert_eq!((opts.interval_ms, opts.step_timeout_secs), (1_000, 15));
    assert_eq!(opts.rest_url, args::TESTNET_REST);
    assert_eq!(opts.ws_url, args::TESTNET_WS);
    assert_eq!(opts.namespace, 7);
    // No flag sets the owner's 30 s window for a cancel not ours (FBC-aoh5).
    assert_eq!(opts.cancel_window, Duration::from_secs(30));
    assert!(
        opts.lease_dir.ends_with(".fueledbychai/testnet_trade"),
        "{:?}",
        opts.lease_dir
    );
    let mut argv = required();
    argv.extend(strings(&[
        "--cycles",
        "5",
        "--interval-ms",
        "250",
        "--step-timeout",
        "30",
        "--lease-dir",
        "/var/tmp/q",
        "--socks5",
        "127.0.0.1:1080",
    ]));
    let opts = parsed(argv);
    assert_eq!(
        (opts.cycles, opts.interval_ms, opts.step_timeout_secs),
        (Some(5), 250, 30)
    );
    assert_eq!(opts.lease_dir, PathBuf::from("/var/tmp/q"));
    assert!(matches!(opts.proxy, ProxyConfig::Socks5 { .. }));
    assert_eq!(args::parse(strings(&["--help"])), Ok(Parsed::Help));
}

#[test]
fn the_command_line_refuses_what_is_missing_out_of_range_or_unknown_never_echoing_it() {
    // Each required flag left out is named.
    for flag in [
        "--market",
        "--tick",
        "--step",
        "--min-notional",
        "--resting-cap-usd",
        "--inventory-cap-usd",
        "--order-usd",
        "--namespace",
        "--bid-bps",
        "--ask-bps",
        "--duration",
    ] {
        let argv = required();
        let at = argv.iter().position(|a| a == flag).unwrap();
        let mut without = argv.clone();
        without.drain(at..at + 2);
        let refused = args::parse(without).unwrap_err();
        assert!(
            refused.contains(&format!("{flag} is required")),
            "{refused}"
        );
    }
    let mut without = required();
    without.remove(0);
    assert!(
        args::parse(without)
            .unwrap_err()
            .contains("--sole-trader is required")
    );
    // Out of range, or not a number: refused, the value never shown.
    for (flag, value) in [
        ("--bid-bps", "0"),
        ("--ask-bps", "2001"),
        ("--duration", "86401"),
        ("--cycles", "0"),
        ("--interval-ms", "99"),
        ("--step-timeout", "121"),
        ("--tick", "-1"),
        ("--min-notional", "SECRETVALUE"),
        ("--market", "SECRETVALUE"),
    ] {
        let mut argv = required();
        argv.extend(strings(&[flag, value]));
        let refused = args::parse(argv).unwrap_err();
        assert!(refused.contains(flag), "{flag}: {refused}");
        assert!(!refused.contains("SECRETVALUE"), "{refused}");
    }
    let mut argv = required();
    argv.extend(strings(&["--order-usd", "12"]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("above --resting-cap-usd")
    );
    let mut argv = required();
    argv.push("--bid-bps".to_owned());
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("--bid-bps needs a value")
    );
    let mut argv = required();
    argv.push("--secret=SECRETVALUE".to_owned());
    let refused = args::parse(argv).unwrap_err();
    assert!(refused.contains("--secret typed with '='"), "{refused}");
    assert!(!refused.contains("SECRETVALUE"), "{refused}");
    let mut argv = required();
    argv.push("--nope".to_owned());
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("unknown argument --nope")
    );
    let mut argv = required();
    argv.push("SECRETVALUE".to_owned());
    let refused = args::parse(argv).unwrap_err();
    assert!(refused.contains("not a flag"), "{refused}");
    assert!(!refused.contains("SECRETVALUE"), "{refused}");
}

#[test]
fn help_says_it_is_a_plumbing_test_not_a_strategy_and_names_every_line() {
    assert!(USAGE.contains("PLUMBING TEST, NOT A STRATEGY"));
    assert!(USAGE.contains("cancel-and-replace, never an amend"));
    for name in [
        "PARADEX_ACCOUNT_ADDRESS",
        "PARADEX_PRIVATE_KEY",
        args::CHAIN_VAR,
        "--bid-bps",
        "--ask-bps",
        "--duration",
        "--cycles",
        "Ctrl-C",
        "TESTNET ONLY",
        "PLACE",
        "CANCEL-ACK",
        "RECONNECT",
        "SUMMARY",
        "DONE ok|failed",
        // The position check (FBC-hpg6).
        "POSITION after the first fill",
        "POSITION re-read",
        "POSITION at Stop",
        "check the position by hand",
        // A cancel not ours (FBC-aoh5).
        "WARN",
        "a cancel of an order of ours that this program did not send",
        "one quoter per account",
        "within 30 s",
        "cancels not ours",
    ] {
        assert!(USAGE.contains(name), "{name}");
    }
}

/// A book snapshot for the pure functions, through the decoder the loop reads with.
fn snapshot(
    bid: Option<&str>,
    ask: Option<&str>,
) -> fbc_venue_paradex::md::rest::OrderbookSnapshot {
    let cfg = testnet::config("http://127.0.0.1:1/v1", "ws://127.0.0.1:1/v1");
    let specs = testnet::specs(
        &fbc_venue_paradex::ParadexFactory,
        &cfg,
        MARKET,
        dec("0.1"),
        dec("0.00001"),
    )
    .unwrap();
    fbc_venue_paradex::md::rest::decode_orderbook(book(1, bid, ask).as_bytes(), &specs).unwrap()
}

fn plain_options() -> Options {
    let mut opts = parsed(required());
    opts.min_notional = dec("5");
    opts.bid_bps = 30;
    opts.ask_bps = 30;
    opts
}

#[test]
fn each_side_is_quoted_only_within_the_caps_and_behind_its_own_touch() {
    let opts = plain_options();
    let qty = Lots::new(16).unwrap();
    let full = snapshot(Some("62000.2"), Some("62000.5"));
    let wanted = quote::desired(&opts, qty, SignedLots(0), &full, mark());
    assert_eq!(
        (wanted.bid, wanted.ask),
        (Some(Ticks(618_141)), Some(Ticks(621_866)))
    );
    assert!(wanted.skipped.is_empty(), "{:?}", wanted.skipped);
    assert_eq!(wanted.book.quote(Side::Buy, 0).unwrap().qty, qty);
    assert!(wanted.book.quote(Side::Sell, 0).unwrap().post_only);

    // A one-sided book quotes the side that rests.
    let bids_only = quote::desired(
        &opts,
        qty,
        SignedLots(0),
        &snapshot(Some("62000.2"), None),
        mark(),
    );
    assert_eq!((bids_only.bid.is_some(), bids_only.ask), (true, None));
    assert!(bids_only.skipped[0].contains("ask: its book side is empty"));

    // A quote above the resting cap at its price, or below the minimum notional, is left out.
    let big = quote::desired(&opts, Lots::new(18).unwrap(), SignedLots(0), &full, mark());
    assert!(
        big.skipped
            .iter()
            .all(|s| s.contains("above --resting-cap-usd")),
        "{:?}",
        big.skipped
    );
    let small = quote::desired(&opts, Lots::new(5).unwrap(), SignedLots(0), &full, mark());
    assert!(
        small
            .skipped
            .iter()
            .all(|s| s.contains("below the market's minimum"))
    );
    assert_eq!(small.book.quote(Side::Buy, 0), None);

    // Long 70 lots: a bid fill would take the position to 86 lots, $53.5 at the ask quote,
    // past the $50 inventory cap; the ask, reducing it, is quoted.
    let long = quote::desired(&opts, qty, SignedLots(70), &full, mark());
    assert_eq!((long.bid, long.ask.is_some()), (None, true));
    assert!(long.skipped[0].starts_with("bid: a fill would take the position to 86 lots"));
    let short = quote::desired(&opts, qty, SignedLots(-70), &full, mark());
    assert_eq!((short.bid.is_some(), short.ask), (true, None));

    // A bid so far down it would be priced at zero is refused, not quoted at zero.
    let mut far = opts.clone();
    far.bid_bps = 2_000;
    let tiny = quote::quote_prices(&far, &snapshot(Some("0.1"), Some("0.2")));
    assert!(tiny.0.unwrap_err().contains("strictly behind the touch"));
}

#[test]
fn the_quote_size_and_caps_come_from_the_first_touch() {
    let opts = plain_options();
    let sizing = quote::size(&opts, &snapshot(Some("62000.2"), Some("62000.5")), mark()).unwrap();
    assert_eq!(
        (
            sizing.qty.get(),
            sizing.resting.get(),
            sizing.inventory.get(),
            sizing.at,
            sizing.mark
        ),
        (16, 17, 80, dec("62186.6"), mark())
    );
    // No side to quote, no lot in the size, or a notional below the market's minimum: refused.
    assert!(
        quote::size(&opts, &snapshot(None, None), mark())
            .unwrap_err()
            .contains("no side")
    );
    let mut tiny = opts.clone();
    tiny.order_usd = dec("0.1");
    assert!(
        quote::size(&tiny, &snapshot(Some("62000.2"), None), mark())
            .unwrap_err()
            .contains("less than one size step")
    );
    let mut strict = opts.clone();
    strict.min_notional = dec("10");
    assert!(
        quote::size(&strict, &snapshot(Some("62000.2"), Some("62000.5")), mark())
            .unwrap_err()
            .contains("below the market's minimum")
    );
}

#[test]
fn the_planner_never_gets_an_amend_and_the_shape_is_checked_against_the_caps() {
    let caps = fbc_venue_paradex::exec::exec_caps().order;
    quote::shape_admitted(&caps).unwrap();
    assert!(quote::cancel_and_replace(&caps).amend.is_none());
    let mut no_post_only = caps.clone();
    no_post_only.post_only = false;
    assert!(
        quote::shape_admitted(&no_post_only)
            .unwrap_err()
            .contains("post-only GTC limit")
    );
}

// ---------------------------------------------------------------------------------------------
// FBC-tyft: the supervised MAINNET mode (--mainnet), and the nine Codex findings carried from
// PR #149 (owner decision 2026-10-15), each a test that fails without its fix.
// ---------------------------------------------------------------------------------------------

/// Paradex's factory in everything but its caps: the snapshot source it declares, and, when
/// given, the units of the per-IP `Rest` bucket of a minute (Paradex's 600, `POST /auth`'s) and
/// of the per-IP connection bucket of a minute (Paradex's 600).
struct Declared {
    source: SnapshotSource,
    rest_per_minute: Option<u32>,
    connects_per_minute: Option<u32>,
}

/// Declares the snapshot source untrustworthy, whatever Paradex's own adapter declares.
static UNTRUSTED: Declared = Declared {
    source: SnapshotSource::Untrustworthy,
    rest_per_minute: None,
    connects_per_minute: None,
};

/// Paradex's caps with the per-IP `Rest` bucket at 10 a minute (9 for normal traffic, its
/// safety reserve kept).
static TEN_RESTS_A_MINUTE: Declared = Declared {
    source: SnapshotSource::Trustworthy,
    rest_per_minute: Some(10),
    connects_per_minute: None,
};

/// Paradex's caps with the per-IP connection bucket at 8 a minute (all 8 for normal traffic:
/// 10% of 8 rounds down to none kept).
static EIGHT_CONNECTS_A_MINUTE: Declared = Declared {
    source: SnapshotSource::Trustworthy,
    rest_per_minute: None,
    connects_per_minute: Some(8),
};

impl VenueFactory for Declared {
    fn id(&self) -> &'static str {
        fbc_venue_paradex::ParadexFactory.id()
    }
    fn config_schema(&self) -> &'static [FieldSpec] {
        fbc_venue_paradex::ParadexFactory.config_schema()
    }
    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = fbc_venue_paradex::ParadexFactory.caps(cfg)?;
        if let Some(exec) = caps.exec.as_mut() {
            exec.order.snapshot_source = self.source;
        }
        for (units, op) in [
            (self.rest_per_minute, OpKind::Rest),
            (self.connects_per_minute, OpKind::Connect),
        ] {
            let Some(units) = units else { continue };
            let only = TagSet::of(&[op]);
            for limit in &mut caps.limits {
                if limit.scope == LimitScope::Ip
                    && limit.ops == only
                    && limit.per == Duration::from_secs(60)
                {
                    limit.units = units;
                }
            }
        }
        Ok(caps)
    }
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        fbc_venue_paradex::ParadexFactory.parse_fbc_common_symbol(s)
    }
    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        fbc_venue_paradex::ParadexFactory.discover(cfg)
    }
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        fbc_venue_paradex::ParadexFactory.plan_md(cfg, specs, subs)
    }
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        fbc_venue_paradex::ParadexFactory.md_codec(cfg, ep)
    }
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        fbc_venue_paradex::ParadexFactory.plan_exec(cfg)
    }
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        fbc_venue_paradex::ParadexFactory.exec_codec(cfg, creds)
    }
    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        fbc_venue_paradex::ParadexFactory.test_connection(cfg, creds)
    }
}

/// A mainnet run on the stub: `--mainnet`, caps of $11 resting per side and $50 inventory, quotes of $11 (17 lots at the first touch's ask quote, 62186.6) 30 bps behind the
/// touch, the BTC market's $10 minimum notional, `--duration 60`, at most `cycles` cycles 100 ms
/// apart.
fn mainnet_options(stub: &StubServer, cycles: u64) -> Options {
    let mut opts = options(stub, cycles);
    opts.mainnet = true;
    opts.min_notional = dec("10");
    opts.order_usd = dec("11");
    opts
}

/// A venue answering `books` in turn to a mainnet run, whose first two `GET /positions` reads
/// (the other markets' check and the resync's) answer the account's position.
fn mainnet_venue(books: Vec<String>) -> Shared {
    let venue = venue(books);
    lock(&venue).unscripted = 2;
    venue
}

/// Runs testnet_quote on `factory` with `opts` until it ends or `interrupt` completes, bounded at
/// 60 s: its report and what it printed.
async fn run_venue_until(
    factory: &'static dyn VenueFactory,
    opts: &Options,
    interrupt: impl std::future::Future<Output = ()>,
) -> (Result<quote::Report, String>, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let run = quote::run_venue(factory, opts, None, creds(), out, interrupt);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let ran = ran.unwrap_or_else(|_| panic!("testnet_quote did not finish in 60 s:\n{printed}"));
    (ran, printed)
}

/// The places the session sent on connection `conn`.
fn creates(venue: &Shared, conn: usize) -> usize {
    methods(venue, conn)
        .iter()
        .filter(|m| *m == "order.create")
        .count()
}

/// Stop as the run printed it: the kill switch and the cancel of everything of ours, nothing
/// placed after the `STOP` line.
fn stopped(printed: &str) {
    let stop = printed
        .find("\nSTOP ")
        .unwrap_or_else(|| panic!("no STOP line:\n{printed}"));
    assert!(!printed[stop..].contains("\nPLACE "), "{printed}");
    let step = printed[stop..]
        .find("STEP stop")
        .unwrap_or_else(|| panic!("no STEP stop:\n{printed}"));
    assert!(
        printed[stop + step..].contains("kill switch on; cancel all:"),
        "{printed}"
    );
}

/// [`position_event`] naming the market `market` (12 characters, as BTC-USD-PERP).
fn position_event_on(market: &str, size: &str) -> Vec<u8> {
    let mut f = position_event(size);
    assert_eq!(market.len(), 12);
    f[8 + 154 + 1..8 + 154 + 13].copy_from_slice(market.as_bytes());
    f
}

/// Every required flag of a mainnet run, valid.
fn mainnet_argv() -> Vec<String> {
    let mut argv = required();
    argv.push("--mainnet".to_owned());
    argv
}

#[tokio::test]
async fn mainnet_mode_requotes_by_cancel_and_replace_within_the_caps_for_several_cycles() {
    capture::install();
    // As the testnet loop: cycle 1 quotes the first touch, the touch moves up for cycle 2 and
    // down for cycle 3 (both sides replaced twice), cycle 4 changes nothing. On mainnet's guard,
    // its chain check, the other markets' check and the trustworthy resync's seed.
    let books = vec![
        b0(),
        b0(),
        book(2, Some("62100.0"), Some("62100.3")),
        book(3, Some("61900.0"), Some("61900.4")),
    ];
    let venue = mainnet_venue(books);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 4 + 4 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 4 reached", "{printed}");
    assert_eq!(
        (report.cycles, report.places_sent, report.cancels_sent),
        (4, 6, 6),
        "{printed}"
    );
    assert_eq!(report.cancelled, 6, "{printed}");
    // No amend frame, ever: every change a cancel and then a place; the start's sweep and Stop
    // the instrument cancel-all, first and last.
    let sent = methods(&venue, 0);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.modify"), 0, "{sent:?}");
    assert_eq!(
        (count("order.create"), count("order.cancel")),
        (6, 4),
        "{sent:?}"
    );
    assert_eq!(count("order.cancel_all"), 2, "{sent:?}");
    assert_eq!(sent[6], "order.cancel_all", "{sent:?}");
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));
    // Per side on the wire: a place, then the cancel of that very order before the next place.
    let reqs = requests(&venue, 0);
    for side in ["BUY", "SELL"] {
        let mut resting: Option<String> = None;
        for req in &reqs {
            match req["method"].as_str().unwrap() {
                "order.create" if req["params"]["side"] == side => {
                    assert!(resting.is_none(), "a place while {resting:?} rests");
                    let cid = req["params"]["client_id"].as_str().unwrap();
                    let v = lock(&venue);
                    resting = Some(v.orders.iter().find(|o| o.cid == cid).unwrap().vid.clone());
                }
                "order.cancel" => {
                    for id in cancelled_ids(req) {
                        if side_of(&venue, &id) == side {
                            assert_eq!(resting.as_deref(), Some(id.as_str()), "{reqs:#?}");
                            resting = None;
                        }
                    }
                }
                "order.cancel_all" => resting = None,
                _ => {}
            }
        }
        assert!(resting.is_none(), "{side}: Stop left one resting");
    }
    // Per side, printed before Stop: each replace's cancel acknowledged and its order closed
    // before the next place.
    let before_stop = printed.split("\nSTOP ").next().unwrap();
    for side in ["bid", "ask"] {
        let tags: Vec<&str> = before_stop
            .lines()
            .filter(|l| l.split_whitespace().nth(2) == Some(side))
            .map(|l| l.split_whitespace().next().unwrap())
            .collect();
        let replace = ["PLACE", "ACK", "CANCEL", "CANCEL-ACK", "CLOSED"];
        let want: Vec<&str> = replace
            .iter()
            .chain(&replace)
            .chain(&["PLACE", "ACK"])
            .copied()
            .collect();
        assert_eq!(tags, want, "{side}:\n{printed}");
    }
    // Every place within the caps and at least the minimum at the mark it was judged by.
    let v = lock(&venue);
    for (req, mark) in reqs
        .iter()
        .filter(|r| r["method"] == "order.create")
        .zip(&v.marks_at_create)
    {
        assert_eq!(req["params"]["instruction"], "POST_ONLY");
        let px = dec(req["params"]["price"].as_str().unwrap());
        let size = dec(req["params"]["size"].as_str().unwrap());
        assert_eq!(size, dec("0.00017"), "{req}");
        assert!(px * size <= dec("11"), "{req}");
        assert!(size * *mark >= dec("10"), "{req} at the mark {mark}");
        assert!(dec("62100.3") * size <= dec("50"), "{req}");
    }
    drop(v);
    // Mainnet's chain is read first, before anything else.
    let http = stub.http_requests();
    assert_eq!(http[0], "GET /v1/system/config", "{http:?}");
    for line in [
        "MAINNET a loopback test stub, not Paradex: REST host 127.0.0.1 WS host 127.0.0.1 chain \
         PRIVATE_SN_PARACLEAR_MAINNET",
        "SUPERVISED mainnet run",
        "STEP chain",
        "STEP positions",
        "SEED position 0 lots seeded by the trustworthy resync (decision 0094)",
        "PLUMBING TEST, not a strategy",
        "DONE ok (--cycles 4 reached)",
    ] {
        assert!(printed.contains(line), "{line}:\n{printed}");
    }
    assert!(!printed.contains("OWNER-ASSISTED"), "{printed}");
    assert!(!printed.contains("seeded by hand from"), "{printed}");
    stopped(&printed);
    no_credential_in(&printed);
    no_credential_in(&capture::all());
}

#[test]
fn mainnet_mode_needs_both_mainnet_and_sole_trader_and_takes_only_mainnets_urls() {
    let opts = parsed(mainnet_argv());
    assert!(opts.mainnet);
    assert_eq!(
        (opts.rest_url.as_str(), opts.ws_url.as_str()),
        (mainnet::MAINNET_REST, mainnet::MAINNET_WS)
    );
    assert_eq!(
        (mainnet::MAINNET_REST, mainnet::MAINNET_WS),
        (
            "https://api.prod.paradex.trade/v1",
            "wss://ws.api.prod.paradex.trade/v1"
        )
    );
    assert!(
        opts.lease_dir.ends_with(".fueledbychai/mainnet_smoke"),
        "{:?}",
        opts.lease_dir
    );
    // Without --sole-trader: refused.
    let mut argv = mainnet_argv();
    argv.retain(|a| a != "--sole-trader");
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("--sole-trader is required")
    );
    // Without --mainnet, mainnet's URLs are the testnet guard's to refuse.
    for (flag, url) in [
        ("--rest-url", "https://api.prod.paradex.trade/v1"),
        ("--ws-url", "wss://ws.api.prod.paradex.trade/v1"),
    ] {
        let mut argv = required();
        argv.extend(strings(&[flag, url]));
        let refused = args::parse(argv).unwrap_err();
        assert!(refused.contains("testnet only"), "{refused}");
    }
    // With it, anything but mainnet's URLs exactly (or both a loopback stub's) is refused, the
    // URL never shown.
    for (flag, url) in [
        ("--rest-url", "https://api.testnet.paradex.trade/v1"),
        ("--ws-url", "wss://ws.api.testnet.paradex.trade/v1"),
        ("--rest-url", "https://api.prod.paradex.trade/v2"),
        ("--rest-url", "https://api.prod.paradex.trade/v1/"),
        ("--ws-url", "ws://ws.api.prod.paradex.trade/v1"),
        ("--rest-url", "https://evil.example/v1"),
    ] {
        let mut argv = mainnet_argv();
        argv.extend(strings(&[flag, url]));
        let refused = args::parse(argv).unwrap_err();
        assert!(refused.contains("mainnet only"), "{url}: {refused}");
        assert!(!refused.contains(url), "{refused}");
    }
    let mut argv = mainnet_argv();
    argv.extend(strings(&["--rest-url", "http://127.0.0.1:9/v1"]));
    assert!(args::parse(argv).unwrap_err().contains("both"));
    // Both a loopback stub's: taken (the tests' stubs).
    let mut argv = mainnet_argv();
    argv.extend(strings(&[
        "--rest-url",
        "http://127.0.0.1:9/v1",
        "--ws-url",
        "ws://127.0.0.1:9/v1",
    ]));
    assert!(parsed(argv).mainnet);
    // --help names the mode, its limits and its lines.
    for name in [
        "--mainnet",
        "MAINNET MODE",
        "PARADEX_MAINNET_ACCOUNT_ADDRESS",
        "PARADEX_MAINNET_PRIVATE_KEY",
        "PRIVATE_SN_PARACLEAR_MAINNET",
        "https://api.prod.paradex.trade/v1",
        "wss://ws.api.prod.paradex.trade/v1",
        "no ceiling",
        "at least 20",
        "at most 3600",
        "never seeded by hand",
        "ORDER_SIZE_BELOW_MIN",
        "STEP chain",
        "STEP positions",
        "SUPERVISED",
        "PACE",
    ] {
        assert!(USAGE.contains(name), "--help lacks {name}");
    }
}

#[tokio::test]
async fn mainnet_mode_refuses_non_positive_caps_offsets_and_a_duration_past_the_owners_limits_before_anything_connects()
 {
    let set = |flag: &str, value: &str| -> Result<Parsed, String> {
        let mut argv = mainnet_argv();
        match argv.iter().position(|a| a == flag) {
            Some(at) => argv[at + 1] = value.to_owned(),
            // An optional flag (--interval-ms) is added.
            None => argv.extend([flag.to_owned(), value.to_owned()]),
        }
        args::parse(argv)
    };
    for (flag, value, why) in [
        ("--resting-cap-usd", "0", "positive"),
        ("--inventory-cap-usd", "-50", "positive"),
        ("--bid-bps", "19", "at least 20 basis points"),
        ("--ask-bps", "19", "at least 20 basis points"),
        ("--duration", "3601", "at most 3600 s"),
        ("--interval-ms", "20001", "at most 20000 ms"),
    ] {
        let refused = set(flag, value).unwrap_err();
        assert!(refused.contains(flag), "{flag}: {refused}");
        assert!(refused.contains(why), "{flag}: {refused}");
    }
    // Any positive caps (no ceiling, owner 2026-10-19: FBC-thks), offsets of 20 and an hour are
    // taken.
    for (flag, value) in [
        ("--resting-cap-usd", "10.5"),
        ("--inventory-cap-usd", "20"),
        ("--resting-cap-usd", "5000"),
        ("--inventory-cap-usd", "20000"),
        ("--bid-bps", "20"),
        ("--ask-bps", "20"),
        ("--duration", "3600"),
        ("--interval-ms", "20000"),
    ] {
        assert!(matches!(set(flag, value), Ok(Parsed::Quote(_))), "{flag}");
    }
    assert_eq!(
        (args::MAINNET_OFFSET_MIN, args::MAINNET_DURATION_MAX),
        (20, 3_600)
    );
    // --duration and both caps stay required, with no default.
    for flag in ["--duration", "--resting-cap-usd", "--inventory-cap-usd"] {
        let mut argv = mainnet_argv();
        let at = argv.iter().position(|a| a == flag).unwrap();
        argv.drain(at..at + 2);
        let refused = args::parse(argv).unwrap_err();
        assert!(
            refused.contains(&format!("{flag} is required")),
            "{refused}"
        );
    }
    // A testnet run has none of these limits.
    let mut argv = required();
    argv.extend(strings(&[
        "--bid-bps",
        "1",
        "--duration",
        "86400",
        "--interval-ms",
        "60000",
    ]));
    assert!(matches!(args::parse(argv), Ok(Parsed::Quote(_))));
    // Options built some other way are checked again by the run, before anything connects:
    // against a loopback stub, which sees nothing.
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    type Break = (fn(&mut Options), &'static str);
    let breaks: [Break; 9] = [
        (
            |o| o.resting_cap_usd = dec("0"),
            "--resting-cap-usd 0 is not positive",
        ),
        (|o| o.interval_ms = 20_001, "at most 20000 ms"),
        (
            |o| o.inventory_cap_usd = dec("0"),
            "--inventory-cap-usd 0 is not positive",
        ),
        (
            |o| o.inventory_cap_usd = dec("-1"),
            "--inventory-cap-usd -1 is not positive",
        ),
        (|o| o.bid_bps = 10, "at least 20"),
        (|o| o.ask_bps = 5, "at least 20"),
        (|o| o.duration_secs = 7_200, "at most 3600"),
        (|o| o.sole_trader = false, "--sole-trader is required"),
        (
            |o| o.rest_url = "https://api.testnet.paradex.trade/v1".to_owned(),
            "--rest-url and --ws-url",
        ),
    ];
    for (break_it, why) in breaks {
        let mut opts = mainnet_options(&stub, 1);
        break_it(&mut opts);
        let (ran, printed) = run_venue_until(
            &fbc_venue_paradex::ParadexFactory,
            &opts,
            std::future::pending(),
        )
        .await;
        let refused = ran
            .err()
            .unwrap_or_else(|| panic!("not refused:\n{printed}"));
        assert!(refused.contains(why), "{why}: {refused}");
        assert!(printed.is_empty(), "{printed}");
    }
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(stub.connections().is_empty());
}

#[tokio::test]
async fn mainnet_mode_takes_large_caps_and_prints_them_before_anything_connects() {
    // No ceiling (owner 2026-10-19, FBC-thks): $5000 resting per side and $20000 inventory
    // run one requote cycle, and the SUPERVISED line names them before the first request.
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 1);
    opts.resting_cap_usd = dec("5000");
    opts.inventory_cap_usd = dec("20000");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 1 reached", "{printed}");
    assert_eq!(report.places_sent, 2, "{printed}");
    let caps = "caps $5000 resting per side, $20000 inventory";
    let supervised = printed
        .lines()
        .position(|l| l.starts_with("SUPERVISED mainnet run") && l.contains(caps))
        .unwrap_or_else(|| panic!("no SUPERVISED line with {caps}:\n{printed}"));
    let chain = printed
        .lines()
        .position(|l| l.starts_with("STEP chain"))
        .unwrap_or_else(|| panic!("no STEP chain:\n{printed}"));
    assert!(supervised < chain, "{printed}");
    assert_eq!(stub.http_requests()[0], "GET /v1/system/config");
    stopped(&printed);
    no_credential_in(&printed);
}

#[tokio::test]
async fn mainnet_mode_checks_the_chain_id_before_anything_connects_and_at_start() {
    // PARADEX_CHAIN_ID, when set, must be mainnet's: by name or as its felt.
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 1);
    for chain in [
        mainnet::MAINNET_CHAIN,
        mainnet::MAINNET_CHAIN_DECIMAL,
        &format!("0x{}", mainnet::MAINNET_CHAIN_HEX),
    ] {
        assert_eq!(quote::guard(&opts, Some(chain)), Ok(true), "{chain}");
    }
    for chain in [
        "PRIVATE_SN_PARACLEAR_TESTNET",
        testnet::TESTNET_CHAIN_DECIMAL,
        "PRIVATE_SN_POTC_SEPOLIA",
    ] {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let out: testnet::Out = buf.clone();
        let refused = quote::run(&opts, Some(chain), creds(), out, std::future::pending())
            .await
            .unwrap_err();
        assert!(refused.contains("PARADEX_CHAIN_ID"), "{refused}");
        assert!(
            refused.contains("PRIVATE_SN_PARACLEAR_MAINNET"),
            "{refused}"
        );
        assert!(!refused.contains(chain), "{refused}");
        assert!(buf.borrow().is_empty());
    }
    assert!(stub.http_requests().is_empty());
    assert!(stub.connections().is_empty());
    // At start, GET /system/config must name mainnet's chain: another refuses the run before the
    // order socket connects, and before anything but that read.
    for (chain, shown) in [
        (
            "PRIVATE_SN_PARACLEAR_TESTNET",
            "PRIVATE_SN_PARACLEAR_TESTNET",
        ),
        ("0xdeadbeef secret", "not shown"),
    ] {
        let venue = mainnet_venue(vec![b0()]);
        lock(&venue).chain = Some(chain);
        let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
            .await
            .unwrap();
        let opts = mainnet_options(&stub, 1);
        let (ran, printed) = run_venue_until(
            &fbc_venue_paradex::ParadexFactory,
            &opts,
            std::future::pending(),
        )
        .await;
        let refused = ran
            .err()
            .unwrap_or_else(|| panic!("not refused:\n{printed}"));
        assert!(
            refused.contains("PRIVATE_SN_PARACLEAR_MAINNET"),
            "{refused}"
        );
        assert!(refused.contains(shown), "{refused}");
        assert!(!refused.contains("deadbeef"), "{refused}");
        assert_eq!(stub.http_requests(), ["GET /v1/system/config"]);
        assert!(stub.connections().is_empty());
    }
}

#[test]
fn mainnet_mode_reads_only_the_mainnet_credentials_and_refuses_a_testnet_one() {
    let (account, key) = synthetic();
    let env = |pairs: &[(&str, &str)]| {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |var: &str| map.get(var).cloned()
    };
    let mainnet_vars = [
        (auth_mainnet::ACCOUNT_VAR, account.as_str()),
        (auth_mainnet::KEY_VAR, key.as_str()),
    ];
    let testnet_vars = [
        (auth::ACCOUNT_VAR, account.as_str()),
        (auth::KEY_VAR, key.as_str()),
    ];
    let mut mainnet = parsed(mainnet_argv());
    let (session, check) = quote::credentials(&mainnet, env(&mainnet_vars)).unwrap();
    for creds in [&session, &check] {
        assert!(creds.get(ACCOUNT_ADDRESS).is_some() && creds.get(SIGNING_KEY).is_some());
    }
    // A testnet variable set beside them refuses the run; the testnet ones alone are never
    // taken in their place; no value is ever shown.
    for var in auth_mainnet::TESTNET_VARS {
        let mut pairs = mainnet_vars.to_vec();
        pairs.push((var, "0x1"));
        let refused = quote::credentials(&mainnet, env(&pairs)).unwrap_err();
        assert!(
            refused.contains(var) && refused.contains("unset"),
            "{refused}"
        );
        assert!(!refused.contains(&account) && !refused.contains(&key));
    }
    let refused = quote::credentials(&mainnet, env(&testnet_vars)).unwrap_err();
    assert!(
        refused.contains("PARADEX_MAINNET_ACCOUNT_ADDRESS"),
        "{refused}"
    );
    assert!(!refused.contains(&account) && !refused.contains(&key));
    // A testnet run reads the testnet variables, never the mainnet ones.
    mainnet.mainnet = false;
    let testnet = mainnet;
    assert!(quote::credentials(&testnet, env(&testnet_vars)).is_ok());
    let refused = quote::credentials(&testnet, env(&mainnet_vars)).unwrap_err();
    assert!(refused.contains("PARADEX_ACCOUNT_ADDRESS"), "{refused}");
}

#[tokio::test]
async fn an_untrustworthy_snapshot_source_refuses_mainnet_mode_before_anything_connects() {
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 1);
    let (ran, printed) = run_venue_until(&UNTRUSTED, &opts, std::future::pending()).await;
    let refused = ran
        .err()
        .unwrap_or_else(|| panic!("not refused:\n{printed}"));
    assert!(refused.contains("untrustworthy"), "{refused}");
    assert!(refused.contains("never seeded by hand"), "{refused}");
    assert!(refused.contains("decision 0094"), "{refused}");
    assert!(printed.is_empty(), "{printed}");
    assert!(stub.http_requests().is_empty());
    assert!(stub.connections().is_empty());
}

#[tokio::test]
async fn a_resync_that_seeds_nothing_refuses_start_in_mainnet_mode_never_seeding_by_hand() {
    // Past the run's up-front check, on a venue declaring its snapshot untrustworthy: the resync
    // seeds nothing, and a mainnet market is never seeded by hand, so Start is refused and the
    // run Stops with nothing placed. (A testnet run seeds such a market by hand, decision 0067.)
    let venue = mainnet_venue(vec![b0()]);
    // The opening six; Stop has nothing to cancel.
    let stub = StubServer::start(one_connection(&venue, 6), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 1);
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let lines = testnet::Lines {
        out,
        t0: std::time::Instant::now(),
    };
    let report = quote::run_on(&UNTRUSTED, &opts, creds(), lines, std::future::pending())
        .await
        .unwrap();
    stub.finished().await.unwrap();
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.starts_with("Start refused"), "{printed}");
    assert!(report.ended.contains("never seeded by hand"), "{printed}");
    assert!(printed.contains("declared untrustworthy"), "{printed}");
    assert!(!printed.contains("STEP start"), "{printed}");
    assert!(!printed.contains("SEED "), "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
    assert_eq!(creates(&venue, 0), 0);
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_the_interrupt() {
    let venue = mainnet_venue(vec![b0()]);
    let signal = Arc::new(Notify::new());
    // The interrupt comes as cycle 2 reads the touch: cycle 1's two orders rest.
    lock(&venue).signal = Some((3, Arc::clone(&signal)));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 1_000);
    let (report, printed) = run_until(&opts, async move { signal.notified().await }).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert_eq!((report.places_sent, report.cancelled), (2, 2), "{printed}");
    assert_eq!(
        methods(&venue, 0).last().map(String::as_str),
        Some("order.cancel_all")
    );
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_at_the_duration() {
    // Cycle 1's touch is never answered: --duration (1 s) passes with nothing placed.
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 1_000_000);
    opts.duration_secs = 1;
    opts.rest_url = holding_relay(&opts.rest_url, 2, Arc::new(Notify::new()), None).await;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--duration 1 s reached", "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
    assert_eq!(
        methods(&venue, 0).last().map(String::as_str),
        Some("order.cancel_all")
    );
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_a_resync_mismatch_with_no_place_after() {
    // After the reconnect the venue reports a long the inventory (flat) does not hold.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).position_after_first = Some("0.001");
    let script = reconnecting(&venue, 6 + 1 + 2, 6 + 1 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 50);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("a resync found the position other than the registry's"),
        "{printed}"
    );
    assert_eq!(creates(&venue, 1), 0, "{printed}");
    assert_eq!(report.cancelled, 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_an_order_of_ours_gone_unknown() {
    // The bid's replace cancel is answered ALREADY_CLOSED (Unknown, FBC-rht4).
    let venue = mainnet_venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).already_closed_once = true;
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 10);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("came back Unknown"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    assert_eq!(
        methods(&venue, 0).last().map(String::as_str),
        Some("order.cancel_all")
    );
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_a_refused_login() {
    // The other markets' check logs in (POST /auth 1); the session's login (2 and on) is
    // refused: the loop Stops at once, nothing sent on the order socket.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).login_refused_from = Some(2);
    let script = WsScript::new(vec![Step::Accept, Step::Accept, Step::Accept]);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = mainnet_options(&stub, 3);
    opts.step_timeout_secs = 40;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    assert!(started.elapsed() < Duration::from_secs(30), "{printed}");
    assert!(!report.ok, "{printed}");
    assert!(report.ended.starts_with("a login was refused"), "{printed}");
    assert_eq!((report.places_sent, report.cancels_sent), (0, 0));
    assert!(methods(&venue, 0).is_empty());
    stopped(&printed);
    no_credential_in(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_a_permanent_refusal() {
    // The first place is refused with Paradex's method error (code 100, RejectKind::Other).
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).reject_create = Some((1, 100));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("refused permanently"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn mainnet_mode_stops_on_a_position_mismatch_that_persists() {
    // Cycle 2's bid cancel races a fill of all of it (17 lots); GET /positions keeps showing the
    // account flat: the check after the fill and its re-read disagree, so the loop Stops with
    // nothing placed after the fill (FBC-hpg6).
    let venue = mainnet_venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    {
        let mut v = lock(&venue);
        v.fill_next_buy_cancel = true;
        v.positions_script = vec![None, None, None, None].into();
    }
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 3);
    opts.step_timeout_secs = 2;
    let (report, printed) = run_against(&opts).await;
    tokio::time::timeout(Duration::from_secs(30), stub.finished())
        .await
        .unwrap_or_else(|_| panic!("the stub's script did not play to its end:\n{printed}"))
        .unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains(positions::HAND_CHECK), "{printed}");
    assert!(report.ended.contains("after the first fill"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    let fill = printed.find("\nFILL ").unwrap();
    assert!(!printed[fill..].contains("\nPLACE "), "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn every_place_reaches_the_minimum_at_the_mark_and_a_side_below_it_is_left_out_never_sent() {
    // The 2026-10-16 live finding: Paradex judges the minimum notional at the MARK. The mark
    // falls to 58000 for cycle 2 (17 lots there are $9.86, under the $10 minimum): both sides
    // are left out, printed, and their quotes cancelled; nothing below the minimum is sent.
    let venue = mainnet_venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).marks = vec![MARK, MARK, "58000"];
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    for side in ["bid", "ask"] {
        let skipped = format!(
            "SKIP {side}: its notional $9.86 at the mark 58000 is below the market's minimum $10"
        );
        assert!(printed.contains(&skipped), "{skipped}:\n{printed}");
    }
    assert!(printed.contains("mark 58000"), "{printed}");
    let reqs = requests(&venue, 0);
    let v = lock(&venue);
    for (req, mark) in reqs
        .iter()
        .filter(|r| r["method"] == "order.create")
        .zip(&v.marks_at_create)
    {
        let size = dec(req["params"]["size"].as_str().unwrap());
        assert!(size * *mark >= dec("10"), "{req} at {mark}");
    }
    // The mark is read with every touch: once for the sizing and once a cycle.
    assert_eq!(v.mark_reads, v.book_reads, "{printed}");
}

#[tokio::test]
async fn a_quote_below_the_minimum_at_the_first_mark_refuses_the_run_before_the_order_socket_connects()
 {
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).marks = vec!["58000"];
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 1);
    let (ran, printed) = run_venue_until(
        &fbc_venue_paradex::ParadexFactory,
        &opts,
        std::future::pending(),
    )
    .await;
    let refused = ran
        .err()
        .unwrap_or_else(|| panic!("not refused:\n{printed}"));
    assert!(
        refused
            .contains("at the mark price 58000 is $9.86 (17 lots), below the market's minimum $10"),
        "{refused}"
    );
    assert!(stub.connections().is_empty());
}

#[tokio::test]
async fn an_asynchronous_order_size_below_min_stops_it_as_a_sizing_error() {
    // Paradex's risk check closes cycle 1's first quote ORDER_SIZE_BELOW_MIN after accepting it:
    // a sizing error, permanent, never a venue fault: the run Stops and fails.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).close_create = Some((1, "ORDER_SIZE_BELOW_MIN"));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.starts_with("a sizing error"), "{printed}");
    assert!(report.ended.contains("ORDER_SIZE_BELOW_MIN"), "{printed}");
    // The adapter reads the reason into the common kind (decision 0096): the loop branches on
    // that, never on Paradex's text (Codex r4306449948 on PR #158).
    assert!(report.ended.contains("MinNotional"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn a_position_on_another_market_refuses_mainnet_mode_before_the_order_socket_connects() {
    // Codex r4277631493 on PR #149: --sole-trader, yet the account holds a position elsewhere.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).elsewhere = Some(("ETH-USD-PERP", "0.5"));
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = mainnet_options(&stub, 1);
    let (ran, printed) = run_venue_until(
        &fbc_venue_paradex::ParadexFactory,
        &opts,
        std::future::pending(),
    )
    .await;
    let refused = ran
        .err()
        .unwrap_or_else(|| panic!("not refused:\n{printed}"));
    assert!(refused.contains("ETH-USD-PERP 0.5"), "{refused}");
    assert!(refused.contains("--sole-trader"), "{refused}");
    assert!(stub.connections().is_empty());
    assert!(!printed.contains("STEP positions"), "{printed}");
    // The answer is read for other markets only: the run's own, a flat one, and a name not
    // shaped like a market are not listed or not shown.
    let body = json!({"results": [
        {"market": MARKET, "size": "0.001", "status": "OPEN"},
        {"market": "SOL-USD-PERP", "size": "0", "status": "CLOSED"},
        {"market": "0xSECRETVALUE-not-a-market", "size": "-2", "status": "OPEN"},
    ]})
    .to_string();
    let listed = positions::other_markets(body.as_bytes(), MARKET).unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert!(!listed[0].contains("SECRETVALUE"), "{listed:?}");
    assert!(positions::other_markets(b"not json", MARKET).is_err());
}

#[tokio::test]
async fn a_position_event_on_another_market_stops_the_loop() {
    // The same premise once the loop runs: the venue reports a position on another market (the
    // adapter, knowing only the run's market, refuses the frame: a decode error), and the loop
    // Stops with nothing placed after.
    let venue = venue(vec![b0()]);
    let mut steps = vec![Step::Accept];
    steps.extend(answered(&venue, 0, 6 + 1 + 2));
    steps.push(Step::Push {
        conn: 0,
        frame: Frame::Binary(position_event_on("ETH-USD-PERP", "0.5")),
    });
    steps.extend(answered(&venue, 0, 1));
    let stub = StubServer::start(WsScript::new(steps), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 1_000);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("could not decode"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn a_permanent_place_refusal_stops_it_and_fails_the_run() {
    // Codex r4277631500 on PR #149: a place refused for good (Paradex's method error, code 100:
    // RejectKind::Other) was only printed, and placed again every cycle, the run ending DONE
    // ok. It Stops the loop now, fails the run, and is never sent again.
    let venue = venue(vec![b0()]);
    lock(&venue).reject_create = Some((1, 100));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        report.ended.contains("refused permanently (Other"),
        "{printed}"
    );
    assert!(printed.contains("DONE failed"), "{printed}");
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn a_place_refused_for_its_rate_waits_its_retry_time_before_it_is_placed_again() {
    // Codex r4277631500 on PR #149: RateLimited (Paradex's 42901, no retry time stated) is
    // retried only after RATE_LIMITED_WAIT, never on the ordinary cycle interval.
    let venue = venue(vec![b0()]);
    lock(&venue).reject_create = Some((1, 42_901));
    // The opening six, the start's sweep, the first pass's two places (one refused), the
    // refused side's place after the wait, Stop's.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(printed.contains("PACE "), "{printed}");
    let times = lock(&venue).create_times.clone();
    assert_eq!(times.len(), 3, "{printed}");
    let waited = times[2].duration_since(times[0]);
    assert!(waited >= quote::RATE_LIMITED_WAIT, "{waited:?}\n{printed}");
    let first = requests(&venue, 0)
        .into_iter()
        .filter(|r| r["method"] == "order.create")
        .map(|r| r["params"]["side"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        first[2], first[0],
        "the refused side is the one placed again"
    );
}

#[tokio::test]
async fn a_position_reported_during_a_reconnects_resync_is_kept_until_the_inventory_agrees() {
    // Codex r4277631505 on PR #149: on the second connection the venue's position stream reports
    // a long of 16 lots before the resync's GET /positions answers (held by the relay until the
    // client has read the event); the resync's snapshot shows the account flat, as the
    // inventory is. The resync used to drop that report, and the loop quoted again from the
    // inventory; the report is kept now, so nothing is planned while it differs and the book
    // does not settle: Stop, with nothing placed on the second connection.
    let venue = venue(vec![b0()]);
    let release = Arc::new(Notify::new());
    let mut steps = vec![Step::Accept];
    steps.extend(answered(&venue, 0, 6 + 1 + 2));
    steps.push(Step::Close { conn: 0 });
    steps.push(Step::Accept);
    steps.extend(answered(&venue, 1, 5));
    steps.push(Step::Push {
        conn: 1,
        frame: Frame::Binary(position_event("0.00016")),
    });
    steps.push(Step::Barrier { conn: 1 });
    let releasing = {
        let (venue, release) = (venue.clone(), Arc::clone(&release));
        Responder::new(move |frame| {
            release.notify_one();
            respond(&venue, 1, frame)
        })
    };
    steps.push(Step::Respond {
        conn: 1,
        with: releasing,
    });
    // The Safety cancel of conn 0's two orders, the sweep's cancel-all, Stop's.
    steps.extend(answered(&venue, 1, 1 + 1 + 1));
    let stub = StubServer::start(WsScript::new(steps), routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 5);
    opts.step_timeout_secs = 2;
    opts.rest_url = holding_relay_on(
        &opts.rest_url,
        b"GET /v1/positions",
        2,
        Arc::new(Notify::new()),
        Some(release),
    )
    .await;
    let (report, printed) = run_against(&opts).await;
    tokio::time::timeout(Duration::from_secs(30), stub.finished())
        .await
        .unwrap_or_else(|_| panic!("the stub's script did not play to its end:\n{printed}"))
        .unwrap();
    assert_eq!(
        creates(&venue, 1),
        0,
        "quoted from a stale inventory:\n{printed}"
    );
    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .contains("the venue's position (16 lots) and the inventory (0 lots) agreeing"),
        "{printed}"
    );
    stopped(&printed);
}

#[tokio::test]
async fn a_replace_is_placed_only_after_its_cancel_is_answered_even_once_its_order_ended() {
    // Codex r4277631497 on PR #149: cycle 2's bid cancel is answered with the order's CLOSED
    // event only (the order stream ahead of the reply), and the reply never comes. The bid's
    // replacement used to be placed at once, before any CANCEL-ACK; no pass plans now until
    // the cancel is answered: it comes back Unknown at the RPC timeout, and the loop Stops.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).closed_without_reply_once = true;
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert_eq!(
        creates(&venue, 0),
        2,
        "a replacement went out before its cancel was answered:\n{printed}"
    );
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("came back Unknown"), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        &sent[9..],
        ["order.cancel", "order.cancel", "order.cancel_all"]
    );
    // The bid ended (USER_CANCELED) while its cancel's reply was still in flight: ours, never
    // a cancel not ours (FBC-aoh5).
    assert_eq!(report.cancels_not_ours, 0, "{printed}");
    assert!(!printed.contains("\nWARN"), "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn after_a_fill_a_pass_requotes_only_from_an_order_book_newer_than_the_one_it_held() {
    // Codex r4289363354 on PR #149: cycle 2's bid cancel races a fill; the pass after it reads a
    // fresh touch, and GET /orderbook answers seq 2 again (a stale cache), the book held. It is
    // read again until seq 3; the ask's new place is priced from that, never the stale one.
    let books = vec![
        b0(),
        b0(),
        book(2, Some("80000.0"), Some("80000.2")),
        book(2, Some("80000.0"), Some("80000.2")),
        book(3, Some("80000.0"), Some("80000.2")),
    ];
    let venue = venue(books);
    lock(&venue).fill_next_buy_cancel = true;
    let stub = StubServer::start(one_connection(&venue, GOES_ON), routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(
        printed.contains("answered seq 2 after the position moved, no newer than the book held"),
        "{printed}"
    );
    // GET /orderbook read 4 is the stale answer: the ask's new place comes after a later read
    // (5, seq 3, or one after the position check that follows the fill).
    let v = lock(&venue);
    assert_eq!(v.books_at_create.len(), 3, "{printed}");
    assert!(
        v.books_at_create[2] > 4,
        "the ask was priced from the stale book ({:?}):\n{printed}",
        v.books_at_create
    );
    let last_cycle = printed.rfind("\nCYCLE 2 ").unwrap();
    assert!(
        !printed[last_cycle..]
            .starts_with("\nCYCLE 2 touch bid 80000.0 ask 80000.2 (GET /orderbook seq 2)"),
        "{printed}"
    );
}

#[tokio::test]
async fn a_permanent_cancel_refusal_stops_it_never_sending_that_cancel_again() {
    // Codex r4289363361 on PR #149: cycle 2's bid cancel is refused for good (code 100). The OMS
    // clears the cancel's intent, and the next pass used to send it again and again; it Stops
    // the loop now, the bid cancelled by Stop's instrument cancel-all.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).reject_cancel = Some((1, 100));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        report.ended.contains("refused permanently (Other"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel").count(),
        2,
        "{sent:?}"
    );
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));
    assert_eq!(creates(&venue, 0), 2, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn a_cancel_refused_for_its_rate_is_sent_again_only_after_its_retry_time() {
    // Codex r4289363361 on PR #149: a replace cancel refused RateLimited (42901) is sent again
    // only after RATE_LIMITED_WAIT, never at the next pass.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).reject_cancel = Some((1, 42_901));
    // The opening six, the start's sweep, two places, cycle 2's two cancels (the first refused),
    // after the wait the refused one again and the two places, Stop's.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 3 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert!(printed.contains("PACE "), "{printed}");
    let v = lock(&venue);
    assert_eq!(v.cancel_times.len(), 3, "{printed}");
    let waited = v.cancel_times[2].duration_since(v.cancel_times[0]);
    assert!(waited >= quote::RATE_LIMITED_WAIT, "{waited:?}\n{printed}");
}

#[tokio::test]
async fn a_stalled_high_water_write_stops_it_at_the_settle_deadline() {
    // Codex r4289363366 on PR #149: the write of the client-id high-water mark is bounded by the
    // book's settling time, as every other wait of a pass is, not by a fresh step timeout of
    // its own: the stalled write Stops the loop at the settle deadline, nothing placed.
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1);
    opts.step_timeout_secs = 2;
    let hold = testnet::held_writes::hold(&opts.lease_dir);
    let (report, printed) = run_against(&opts).await;
    hold.release();
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert_eq!(
        report.ended, "the client-id high-water mark's write did not happen in time",
        "{printed}"
    );
    assert!(printed.contains("TIMEOUT settle"), "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn every_rest_read_of_the_loop_is_charged_to_the_venues_rate_limit() {
    // Codex r4289363370 on PR #149: the touch (and mark) reads went out uncounted, so a fast
    // loop could pass Paradex's per-IP limits. On a venue declaring 10 Rest requests a minute
    // (9 for normal traffic), the run sends at most 9 of them (its logins, every GET /orderbook
    // and GET /markets/summary), and once the limit has no room within the step timeout it
    // Stops, rather than read past it.
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1_000);
    opts.step_timeout_secs = 1;
    let (ran, printed) = run_venue_until(&TEN_RESTS_A_MINUTE, &opts, std::future::pending()).await;
    stub.finished().await.unwrap();
    let report = ran.unwrap_or_else(|e| panic!("refused: {e}\n{printed}"));
    let rest = stub
        .http_requests()
        .iter()
        .filter(|r| {
            [
                "POST /v1/auth",
                "GET /v1/orderbook",
                "GET /v1/markets/summary",
            ]
            .iter()
            .any(|p| r.starts_with(p))
        })
        .count();
    assert!(
        rest <= 9,
        "{rest} Rest requests: {:?}",
        stub.http_requests()
    );
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("rate limit"), "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn the_last_cycle_stops_the_loop_without_waiting_the_interval() {
    // Codex r4289363377 on PR #149: --cycles 1 with the longest interval (60 s): the loop Stops
    // as the cycle ends, its quotes never resting for the pause.
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1);
    opts.interval_ms = 60_000;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}\n{printed}",
        started.elapsed()
    );
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 1 reached", "{printed}");
    assert_eq!(report.places_sent, 2, "{printed}");
}

// ---------------------------------------------------------------------------------------------
// Codex's review of PR #158 (head 9fc846a).
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_stop_cancel_refused_for_its_rate_is_sent_again_after_its_retry_time() {
    // Codex r4306449947 on PR #158: Stop's instrument cancel-all is refused RateLimited. Stop
    // used to wait out its timeout for the refused request, leaving the quotes resting until the
    // session's teardown; it sends the cancel again once the retry time passed, and the run ends
    // ok with every order of ours cancelled.
    let venue = venue(vec![b0()]);
    lock(&venue).reject_cancel_all = Some((2, 42_901));
    // The opening six, the start's sweep, two places, Stop's refused cancel-all and its retry.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.cancelled, 2, "{printed}");
    let times = lock(&venue).cancel_all_times.clone();
    assert_eq!(times.len(), 3, "{printed}");
    let waited = times[2].duration_since(times[1]);
    assert!(waited >= quote::RATE_LIMITED_WAIT, "{waited:?}\n{printed}");
    assert!(printed.contains("PACE "), "{printed}");
    stopped(&printed);
}

#[tokio::test]
async fn a_sweep_cancel_refused_for_its_rate_is_sent_again_after_its_retry_time() {
    // Codex r4306449947 on PR #158: after a reconnect the sweep's instrument cancel-all is
    // refused RateLimited while the ask (which the lagging resync did not show) still rests. The
    // sweep used to wait out its timeout and Stop; it sends the cancel again once the retry time
    // passed, the ask ends, and the loop quotes on the new connection.
    let venue = venue(vec![b0()]);
    {
        let mut v = lock(&venue);
        v.hide_sells_after_first = true;
        v.reject_cancel_all = Some((2, 42_901));
    }
    // First connection: the opening six, the start's sweep and two places. Second: the opening
    // six, the bid's Safety cancel, the sweep's refused cancel-all and its retry, two places,
    // Stop's cancel-all.
    let script = reconnecting(&venue, 6 + 1 + 2, 6 + 1 + 1 + 1 + 2 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(creates(&venue, 1), 2, "{printed}");
    let times = lock(&venue).cancel_all_times.clone();
    assert!(times.len() >= 3, "{printed}");
    let waited = times[2].duration_since(times[1]);
    assert!(waited >= quote::RATE_LIMITED_WAIT, "{waited:?}\n{printed}");
}

#[tokio::test]
async fn the_position_checks_charge_the_connections_they_open() {
    // Codex r4306449950 on PR #158: the position check's REST reads (its login and GET
    // /positions, each on a connection of its own) were charged as requests but not as the
    // connections they open. On a venue declaring 8 connections a minute, every connection the
    // run opens (each HTTP request's and the order socket's) is charged first, so the stub
    // never sees more than 8, whatever the run then does.
    let venue = mainnet_venue(vec![b0()]);
    let stub = StubServer::start(one_connection(&venue, 6), routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = mainnet_options(&stub, 3);
    opts.step_timeout_secs = 1;
    let (ran, printed) =
        run_venue_until(&EIGHT_CONNECTS_A_MINUTE, &opts, std::future::pending()).await;
    let opened = stub.http_requests().len() + stub.connections().len();
    assert!(
        opened <= 8,
        "{opened} connections: {:?} and {} order sockets\n{ran:?}\n{printed}",
        stub.http_requests(),
        stub.connections().len()
    );
    assert!(printed.contains("STEP positions"), "{printed}");
}

// FBC-aoh5: a cancel of an order of ours that the run never sent (the owner's 2026-10-18 mainnet
// session: a Java instance on the same account cancelled every order on its ticker it did not
// place, every 3 s). The owner's decision: WARN once and requote; a second on a later cycle
// within 30 s Stops a mainnet run (explicit cancels of ours, never an instrument cancel-all,
// which would cancel the other program's orders too) and fails it; a testnet run only warns.

/// The touch one cycle moves to and back from, so every cycle replaces both quotes: its `seq_no`
/// `seq`.
fn up(seq: u64) -> String {
    book(seq, Some("62100.0"), Some("62100.3"))
}

/// The first touch again, under `seq_no` `seq`.
fn base(seq: u64) -> String {
    book(seq, Some("62000.2"), Some("62000.5"))
}

/// The first read and cycle 1 at the first touch, then the touch moving every cycle to cycle
/// `cycles`, and the same touch after.
fn alternating(cycles: u64) -> Vec<String> {
    let mut books = vec![b0(), b0()];
    for c in 2..=cycles {
        books.push(if c % 2 == 0 { up(c) } else { base(c) });
    }
    books
}

/// The index of the first request the session sent on connection 0 after the co-tenant's first
/// cancel: the request right after the reply to place `n` (from 1) that it followed.
fn after_create(venue: &Shared, n: usize) -> usize {
    let sent = methods(venue, 0);
    let mut seen = 0;
    for (at, m) in sent.iter().enumerate() {
        if m == "order.create" {
            seen += 1;
            if seen == n {
                return at + 1;
            }
        }
    }
    panic!("no place {n}: {sent:?}")
}

/// The venue ids of the orders the stub holds of `side` (BUY or SELL), in the order placed.
fn vids_of(venue: &Shared, side: &str) -> Vec<String> {
    let v = lock(venue);
    v.orders
        .iter()
        .filter(|o| o.side == side)
        .map(|o| o.vid.clone())
        .collect()
}

/// The `WARN` lines, the first one's text checked: it names what it may be, the owner's rule and,
/// on mainnet, the Stop on a repeat.
fn warned(printed: &str, mainnet: bool) -> usize {
    let warns = tagged(printed, "WARN");
    let first = warns
        .first()
        .unwrap_or_else(|| panic!("no WARN line:\n{printed}"));
    for part in [
        "ended by a user cancel this program did not send",
        "another program on the account, or the Paradex UI",
        "one quoter per account",
    ] {
        assert!(first.contains(part), "{part}:\n{printed}");
    }
    let rule = if mainnet {
        "a second on a later cycle within 30 s Stops the run, as does one of the loop's requote \
         within 30 s of its place"
    } else {
        "testnet mode only warns"
    };
    assert!(first.contains(rule), "{rule}:\n{printed}");
    warns.len()
}

#[tokio::test]
async fn a_one_off_cancel_not_ours_warns_once_and_the_mainnet_loop_requotes_and_ends_done_ok() {
    capture::install();
    // Cycle 1's two quotes are cancelled by another program right after the ask's place (one
    // event: one reconcile cancels both sides); cycle 2 places both again; cycles 3 and 4 send
    // nothing. Stop then cancels the two resting quotes explicitly, by a batch cancel naming
    // them: another program is on the account, so never the instrument cancel-all.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).cotenant_after_create = vec![(2, None)];
    // The opening six, the start's sweep, two places, two places again, Stop's batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 4 reached", "{printed}");
    assert_eq!(report.cancels_not_ours, 2, "{printed}");
    assert_eq!(warned(&printed, true), 1, "one event, one WARN:\n{printed}");
    assert_eq!(creates(&venue, 0), 4, "{printed}");
    let sent = methods(&venue, 0);
    let after = after_create(&venue, 2);
    // The start's sweep is the only instrument cancel-all, before the co-tenant's cancel.
    assert_eq!(sent[6], "order.cancel_all", "{sent:?}");
    assert!(
        !sent[after..].iter().any(|m| m == "order.cancel_all"),
        "an instrument cancel-all after the cancel not ours: {sent:?}"
    );
    // Stop's cancel names both resting quotes, explicitly.
    let last = requests(&venue, 0).last().cloned().unwrap();
    assert_eq!(last["method"], "order.cancel_batch", "{sent:?}");
    let mut named = cancelled_ids(&last);
    named.sort();
    let mut resting = vec![
        vids_of(&venue, "BUY")[1].clone(),
        vids_of(&venue, "SELL")[1].clone(),
    ];
    resting.sort();
    assert_eq!(named, resting, "{sent:?}");
    assert!(
        printed.contains("no instrument cancel-all: another program cancelled our orders"),
        "{printed}"
    );
    assert!(printed.contains(" cancels not ours 2\n"), "{printed}");
    assert!(
        printed.contains("DONE ok (--cycles 4 reached)"),
        "{printed}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_second_cancel_not_ours_on_a_later_cycle_within_30_s_stops_a_mainnet_run_explicitly() {
    capture::install();
    // Cycle 1's quotes are cancelled by another program (the first event: WARN); cycle 2 places
    // both again and the other program cancels the bid (the second, on a later cycle within
    // 30 s): Stop, the ask cancelled explicitly, nothing placed after, the run failed.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).cotenant_after_create = vec![(2, None), (4, Some("BUY"))];
    // The opening six, the start's sweep, two places, two places again, Stop's batch cancel of
    // the ask.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 50);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    assert_eq!(report.cancels_not_ours, 3, "{printed}");
    assert_eq!(warned(&printed, true), 2, "{printed}");
    stopped(&printed);
    assert_eq!(creates(&venue, 0), 4, "nothing placed after:\n{printed}");
    let sent = methods(&venue, 0);
    let after = after_create(&venue, 2);
    assert_eq!(sent[6], "order.cancel_all", "{sent:?}");
    assert!(
        !sent[after..].iter().any(|m| m == "order.cancel_all"),
        "an instrument cancel-all after the cancel not ours: {sent:?}"
    );
    // Stop's one cancel names the ask still resting, explicitly.
    let last = requests(&venue, 0).last().cloned().unwrap();
    assert_eq!(last["method"], "order.cancel_batch", "{sent:?}");
    assert_eq!(cancelled_ids(&last), [vids_of(&venue, "SELL")[1].clone()]);
    assert!(
        printed.contains("no instrument cancel-all: another program cancelled our orders"),
        "{printed}"
    );
    assert!(printed.contains(" cancels not ours 3\n"), "{printed}");
    assert!(printed.contains("DONE failed (another program cancelled our orders"));
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_later_cancel_not_ours_of_an_order_older_than_the_first_still_stops_a_mainnet_run() {
    capture::install();
    // RB-aoh5-1 / Codex r4308625185 on PR #159: another program cancels cycle 1's bid (the
    // first: WARN); cycle 2 places the bid again and the other program then cancels cycle 1's
    // ask, which rested unchanged since before the first: a second action on a later cycle
    // within 30 s, whatever its order's age: Stop, the bid cancelled explicitly, nothing placed
    // after, the run failed.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).cotenant_after_create = vec![(2, Some("BUY")), (3, Some("SELL"))];
    // The opening six, the start's sweep, two places, the bid's place again, Stop's batch
    // cancel of it.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    assert_eq!(report.cancels_not_ours, 2, "{printed}");
    assert_eq!(warned(&printed, true), 2, "{printed}");
    stopped(&printed);
    assert_eq!(creates(&venue, 0), 3, "nothing placed after:\n{printed}");
    let last = requests(&venue, 0).last().cloned().unwrap();
    assert_eq!(last["method"], "order.cancel_batch", "{printed}");
    assert_eq!(cancelled_ids(&last), [vids_of(&venue, "BUY")[1].clone()]);
    assert!(printed.contains(" cancels not ours 2\n"), "{printed}");
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_co_tenant_cancelling_our_orders_every_three_cycles_is_stopped_within_two_events() {
    capture::install();
    // Java's orphan reconcile on a 3-cycle period: every order of ours resting is cancelled
    // after cycle 1's places and again after cycle 4's (the touch moves every cycle, so each
    // cycle replaces both quotes). The second event Stops the run, with nothing of ours left to
    // cancel and no instrument cancel-all.
    let venue = mainnet_venue(alternating(10));
    lock(&venue).cotenant_after_create = vec![(2, None), (8, None), (14, None)];
    // The opening six, the start's sweep; cycle 1 two places, cycle 2 two places (both ended),
    // cycles 3 and 4 two cancels and two places each; Stop has nothing open to cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 4 + 4),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 50);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    // Cycle 4 counts when its last place's reply is read before the other program's cancels
    // that follow it (separate frames): 3 or 4, the Stop the same.
    assert!(matches!(report.cycles, 3 | 4), "{printed}");
    assert_eq!(report.cancels_not_ours, 4, "{printed}");
    assert_eq!(warned(&printed, true), 2, "{printed}");
    stopped(&printed);
    assert_eq!(creates(&venue, 0), 8, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel_all").count(),
        1,
        "only the start's sweep: {sent:?}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_cancel_not_ours_after_the_window_only_warns_again() {
    capture::install();
    // The window shortened to 300 ms for the test (the owner's is 30 s): cycle 1's quotes and
    // cycle 7's are cancelled by another program, at least six 100 ms pauses apart, so the
    // second starts a new window: a WARN, never a Stop. Cycle 8 places both again.
    let venue = mainnet_venue(alternating(8));
    lock(&venue).cotenant_after_create = vec![(2, None), (14, None)];
    // The opening six, the start's sweep; cycle 1 two places, cycle 2 two places, cycles 3 to 7
    // two cancels and two places each, cycle 8 two places; Stop's batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 4 * 5 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 8);
    opts.cancel_window = Duration::from_millis(300);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 8 reached", "{printed}");
    assert_eq!(report.cancels_not_ours, 4, "{printed}");
    assert_eq!(tagged(&printed, "WARN").len(), 2, "{printed}");
    assert!(
        !printed.contains("another program cancelled our orders:"),
        "{printed}"
    );
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel_all").count(),
        1,
        "only the start's sweep: {sent:?}"
    );
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_batch"));
    assert!(
        printed.contains("DONE ok (--cycles 8 reached)"),
        "{printed}"
    );
}

#[tokio::test]
async fn a_mainnet_interval_at_which_a_co_tenants_cancels_fall_outside_the_window_is_refused() {
    // RB-aoh5-7 on PR #159, scaled: the window shortened to 300 ms, the interval 400 ms. Another
    // program cancels both quotes after every cycle's places; the loop requotes a side only on
    // the next cycle, so each event comes more than the window after the one before and every
    // event would be a first one (a WARN each cycle, DONE ok, never a Stop). A mainnet run at
    // such an interval is refused before anything connects: the interval may be at most two
    // thirds of the window (20000 ms of the owner's 30 s), which leaves a third for the cycle's
    // reads and a 3 s reconcile.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).cotenant_after_create = vec![(2, None), (4, None), (6, None)];
    // Were it run: the opening six, the start's sweep, cycles 1 to 4 two places each, Stop's
    // batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 * 4 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 4);
    opts.cancel_window = Duration::from_millis(300);
    opts.interval_ms = 400;
    let (ran, printed) = run_venue_until(
        &fbc_venue_paradex::ParadexFactory,
        &opts,
        std::future::pending(),
    )
    .await;
    let refused = ran
        .err()
        .unwrap_or_else(|| panic!("not refused:\n{printed}"));
    assert!(refused.contains("--interval-ms 400"), "{refused}");
    assert!(refused.contains("at most 200 ms"), "{refused}");
    assert!(printed.is_empty(), "{printed}");
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(stub.connections().is_empty());
    // Two thirds of the window is taken; at it the co-tenant is Stopped on its second event,
    // however long the cycle (RB-aoh5-8:
    // `at_the_largest_mainnet_interval_a_co_tenant_cancelling_every_requote_is_stopped_...`).
    assert_eq!(
        args::mainnet_interval_max(Duration::from_millis(300)),
        Duration::from_millis(200)
    );
    assert_eq!(
        args::mainnet_interval_max(args::CANCEL_NOT_OURS_WINDOW),
        Duration::from_secs(20)
    );
}

#[tokio::test]
async fn at_the_largest_mainnet_interval_a_co_tenant_cancelling_every_requote_is_stopped_however_long_its_cycle()
 {
    capture::install();
    // Codex r4309154208 / RB-aoh5-8 / RB-aoh5-9 on PR #159, scaled: the window shortened to
    // 1 s, the loop run at the largest interval a mainnet run takes with it (two thirds: 666 ms),
    // and every touch read taking 1.1 s, so each cycle is longer than the window. Another
    // program cancels both quotes right after every cycle's places. Its cancel of the loop's
    // requote is the repeat however long the cycle that placed it: the second event Stops the
    // run, with nothing of ours left to cancel and nothing placed after.
    let venue = mainnet_venue(vec![b0()]);
    {
        let mut v = lock(&venue);
        v.cotenant_after_create = vec![(2, None), (4, None), (6, None)];
        v.book_delay = Some(Duration::from_millis(1_100));
    }
    // The opening six, the start's sweep, cycle 1 two places, cycle 2 two places; Stop has
    // nothing open to cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 4);
    opts.cancel_window = Duration::from_secs(1);
    opts.interval_ms =
        u64::try_from(args::mainnet_interval_max(opts.cancel_window).as_millis()).unwrap();
    assert_eq!(opts.interval_ms, 666);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    let warns = tagged(&printed, "WARN");
    assert_eq!(warns.len(), 2, "{printed}");
    assert!(
        warns[1].contains("placing the requote it cancelled): Stop"),
        "{printed}"
    );
    stopped(&printed);
    assert_eq!(creates(&venue, 0), 4, "nothing placed after:\n{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel_all").count(),
        1,
        "only the start's sweep: {sent:?}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn at_the_largest_mainnet_interval_a_requote_refused_for_its_rate_leaves_the_next_place_the_requote()
 {
    // RB-aoh5-10 on PR #159, scaled as the test above: another program cancels both quotes
    // after cycle 1's places (the first event, a WARN). Cycle 2's first requote place, the bid,
    // is refused for its rate (42901), so it never rests; the bid is placed again after the
    // retry time, and that place, the one that rests, is the side's requote: another program's
    // cancel of it right after its place Stops the run, though the cycle that placed it took
    // longer than the window since the first event. A refused place used to keep the requote
    // slot, so that cancel was judged a new first event and only warned.
    let printed = an_unrested_requote_place(|v| {
        v.reject_create = Some((3, 42_901));
        // Creates count only the places the stub took: cycle 1's two, cycle 2's ask, the bid.
        v.cotenant_after_create = vec![(2, None), (4, Some("BUY"))];
    })
    .await;
    assert!(printed.contains("PACE "), "{printed}");
}

#[tokio::test]
async fn at_the_largest_mainnet_interval_a_requote_that_would_cross_leaves_the_next_place_the_requote()
 {
    // RB-aoh5-10 on PR #159, as the test above with cycle 2's bid closed POST_ONLY_WOULD_CROSS
    // by the venue's risk check after its reply (an asynchronous reject): it never rested, its
    // side is left out of the cycle, and the next cycle's bid is the requote whose cancel Stops.
    let printed = an_unrested_requote_place(|v| {
        v.close_create = Some((3, "POST_ONLY_WOULD_CROSS"));
        v.cotenant_after_create = vec![(2, None), (5, Some("BUY"))];
    })
    .await;
    assert!(printed.contains("POST_ONLY_WOULD_CROSS"), "{printed}");
}

/// [`at_the_largest_mainnet_interval_a_requote_refused_for_its_rate_leaves_the_next_place_the_requote`]
/// and its post-only twin: the window 1 s, the loop at the largest mainnet interval with it,
/// every touch read 1.1 s, `script` making cycle 2's bid place end without resting. The run
/// Stops on the second event (the cancel of the bid placed after it), with five places sent
/// and only the ask left for Stop to cancel. What was printed.
async fn an_unrested_requote_place(script: impl FnOnce(&mut Venue)) -> String {
    capture::install();
    let venue = mainnet_venue(vec![b0()]);
    {
        let mut v = lock(&venue);
        script(&mut v);
        v.book_delay = Some(Duration::from_millis(1_100));
    }
    // The opening six, the start's sweep, cycle 1 two places, cycle 2 two places (the bid
    // never rested), the bid placed again; Stop's cancel of the ask.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 4);
    opts.cancel_window = Duration::from_secs(1);
    opts.interval_ms =
        u64::try_from(args::mainnet_interval_max(opts.cancel_window).as_millis()).unwrap();
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    let warns = tagged(&printed, "WARN");
    assert_eq!(warns.len(), 2, "{printed}");
    assert!(warns[1].contains("the bid #"), "{printed}");
    assert!(
        warns[1].contains("placing the requote it cancelled): Stop"),
        "{printed}"
    );
    stopped(&printed);
    let sides = requests(&venue, 0)
        .into_iter()
        .filter(|r| r["method"] == "order.create")
        .map(|r| r["params"]["side"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(sides.len(), 5, "nothing placed after:\n{printed}");
    assert_eq!(
        sides[2], "BUY",
        "cycle 2's unrested place is the bid: {sides:?}"
    );
    assert_eq!(sides[4], "BUY", "its side placed again: {sides:?}");
    no_credential_in(&printed);
    printed
}

#[tokio::test]
async fn a_cancel_of_ours_refused_for_its_rate_stops_covering_its_order_so_anothers_cancel_is_seen()
{
    capture::install();
    // Codex r4309154204 / RB-aoh5-4 on PR #159: cycle 2's first replace cancel is refused
    // RateLimited (42901), and another program then cancels that order, still open, while ours
    // waits its retry time. The refused cancel no longer covers it: a cancel not ours, a WARN;
    // the loop never sends the refused cancel again (the order ended), places both quotes, and
    // Stop cancels them explicitly.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    {
        let mut v = lock(&venue);
        v.reject_cancel = Some((1, 42_901));
        v.cotenant_after_refused_cancel = true;
    }
    // The opening six, the start's sweep, two places, cycle 2's two cancels (the first refused),
    // the two places, Stop's batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.cancels_not_ours, 1, "{printed}");
    assert_eq!(warned(&printed, false), 1, "{printed}");
    assert!(printed.contains(" cancels not ours 1\n"), "{printed}");
    assert_eq!(lock(&venue).cancel_times.len(), 2, "{printed}");
    assert_eq!(creates(&venue, 0), 4, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_batch"));
}

#[tokio::test]
async fn anothers_cancel_read_before_the_refusal_of_ours_is_counted_at_the_refusal() {
    capture::install();
    // Codex r4310042612 on PR #159: as the test above, with another program's cancel of the
    // order read before the venue's refusal of ours (42901). The order ended while our cancel,
    // sent and unanswered, covered it: held, then counted at the refusal, a WARN.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    {
        let mut v = lock(&venue);
        v.reject_cancel = Some((1, 42_901));
        v.cotenant_before_refused_cancel = true;
    }
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.cancels_not_ours, 1, "{printed}");
    assert_eq!(warned(&printed, false), 1, "{printed}");
    let refused = printed.find("PACE ").expect("the refusal paced the loop");
    let warn = printed.find("\nWARN ").expect("a WARN line");
    assert!(warn > refused, "counted at the refusal:\n{printed}");
    assert_eq!(lock(&venue).cancel_times.len(), 2, "{printed}");
    assert_eq!(creates(&venue, 0), 4, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_batch"));
}

#[tokio::test]
async fn a_co_tenant_cancelling_the_loops_replace_of_its_requote_is_stopped() {
    capture::install();
    // Codex r4310042604 on PR #159, scaled: the window 3 s, the loop at the largest mainnet
    // interval with it (2 s). Another program cancels both quotes after cycle 1's places (the
    // first event); cycle 2 places the requotes; the touch moves at cycle 3, more than 3 s after
    // the first event, and the loop replaces both itself; the other program then cancels both
    // replaces. A replace of the requote is judged from the requote's place, 2 s before: the
    // repeat, Stop. It used to be judged by the window's first event alone: a new first, a WARN.
    // The start's read of the touch, cycle 1's, cycle 2's, then the touch moved.
    let moved = book(2, Some("62100.0"), Some("62100.3"));
    let venue = mainnet_venue(vec![b0(), b0(), b0(), moved]);
    lock(&venue).cotenant_after_create = vec![(2, None), (6, None)];
    // The opening six, the start's sweep, cycle 1 two places, cycle 2 two places, cycle 3 two
    // cancels and two places; Stop has nothing open to cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 4),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = mainnet_options(&stub, 4);
    opts.cancel_window = Duration::from_secs(3);
    opts.interval_ms =
        u64::try_from(args::mainnet_interval_max(opts.cancel_window).as_millis()).unwrap();
    assert_eq!(opts.interval_ms, 2_000);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("another program cancelled our orders: one quoter per account"),
        "{printed}"
    );
    let warns = tagged(&printed, "WARN");
    assert_eq!(warns.len(), 2, "{printed}");
    assert!(
        warns[1].contains("placing the requote it cancelled): Stop"),
        "{printed}"
    );
    stopped(&printed);
    assert_eq!(creates(&venue, 0), 6, "nothing placed after:\n{printed}");
    let times = lock(&venue).create_times.clone();
    assert!(
        times[5].duration_since(times[0]) > Duration::from_secs(3),
        "the replaces came more than the window after the first event:\n{printed}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn in_testnet_mode_a_repeated_cancel_not_ours_only_warns() {
    capture::install();
    // As the mainnet repeat: cycle 1's quotes, then cycle 2's bid, cancelled by another
    // program. On testnet both only warn; cycle 3 places the bid again and the run ends at
    // --cycles, Stop's cancels explicit all the same.
    let venue = venue(vec![b0()]);
    lock(&venue).cotenant_after_create = vec![(2, None), (4, Some("BUY"))];
    // The opening six, the start's sweep, two places, two places again, the bid's place, Stop's
    // batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 4 reached", "{printed}");
    assert_eq!(report.cancels_not_ours, 3, "{printed}");
    assert_eq!(warned(&printed, false), 2, "{printed}");
    assert_eq!(creates(&venue, 0), 5, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.cancel_all").count(),
        1,
        "only the start's sweep: {sent:?}"
    );
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_batch"));
    assert!(printed.contains(" cancels not ours 3\n"), "{printed}");
}

#[tokio::test]
async fn our_own_cancel_all_whose_order_events_come_before_its_reply_is_never_counted() {
    // Stop's instrument cancel-all is answered with the order events closing both quotes first
    // (USER_CANCELED, as another program's cancel would read) and its reply after: ours, in
    // flight, never a cancel not ours.
    let venue = mainnet_venue(vec![b0()]);
    lock(&venue).cancel_all_closes_first = true;
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = mainnet_options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("the stub's script: {e:?}\n{printed}"));
    assert!(report.ok, "{printed}");
    assert_eq!(report.cancelled, 2, "{printed}");
    assert_eq!(report.cancels_not_ours, 0, "{printed}");
    assert!(!printed.contains("\nWARN"), "{printed}");
    assert!(printed.contains(" cancels not ours 0\n"), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(sent.last().map(String::as_str), Some("order.cancel_all"));
}

#[test]
fn cancels_not_ours_of_one_action_are_one_event_and_a_later_action_repeats_it() {
    use quote::{CancelsNotOurs, EVENT_SPAN, NotOurs};
    let t0 = tokio::time::Instant::now();
    let s = Duration::from_secs;
    // The venue's time on a cancel, `secs` after the first action's.
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let (buy, sell) = (Side::Buy, Side::Sell);
    assert_eq!(EVENT_SPAN, s(1));
    let mut seen = CancelsNotOurs::new(args::CANCEL_NOT_OURS_WINDOW);
    assert_eq!(args::CANCEL_NOT_OURS_WINDOW, s(30));
    // Both sides, placed before it was seen, of one reconcile: one event, even read 1.5 s apart
    // (split across a cycle); an earlier run's order (placed by no one this run) as well.
    let (b1, a1, a0, old) = (cid(), cid(), cid(), cid());
    for (order, side) in [(b1, buy), (a1, sell), (a0, sell)] {
        seen.placed(order, side, t0);
    }
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0 + s(1)), NotOurs::First);
    let read_later = t0 + Duration::from_millis(2_500);
    assert_eq!(
        seen.on_cancel(a1, sell, at(0), read_later),
        NotOurs::SameEvent
    );
    assert_eq!(
        seen.on_cancel(old, buy, at(0), t0 + s(2)),
        NotOurs::SameEvent
    );
    // RB-aoh5-1 / Codex r4308625185: an order resting since before the first, cancelled by a
    // later action (the venue's time 10 s on) within 30 s: the repeat.
    assert_eq!(
        seen.on_cancel(a0, sell, at(10), t0 + s(11)),
        NotOurs::Repeat
    );
    // A requote placed after it, cancelled within 30 s: the repeat again; its other side,
    // placed before and cancelled with it, the same event as it.
    let (b2, a2) = (cid(), cid());
    seen.placed(b2, buy, t0 + s(12));
    seen.placed(a2, sell, t0 + s(13));
    assert_eq!(seen.on_cancel(b2, buy, at(20), t0 + s(21)), NotOurs::Repeat);
    assert_eq!(
        seen.on_cancel(a2, sell, at(20), t0 + s(21)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.orders(), 6);

    // Codex r4309154208 / RB-aoh5-9: a cycle longer than the window. The loop's requotes of
    // both sides are placed 40 s after the first event and cancelled 3 s later by another
    // program's next reconcile: the repeat however long the cycle, the other side the same event.
    let mut seen = CancelsNotOurs::new(s(30));
    let (b1, a1) = (cid(), cid());
    seen.placed(b1, buy, t0);
    seen.placed(a1, sell, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    assert_eq!(seen.on_cancel(a1, sell, at(0), t0), NotOurs::SameEvent);
    let (b2, a2) = (cid(), cid());
    seen.placed(b2, buy, t0 + s(40));
    seen.placed(a2, sell, t0 + s(40));
    assert_eq!(seen.on_cancel(b2, buy, at(43), t0 + s(43)), NotOurs::Repeat);
    assert_eq!(
        seen.on_cancel(a2, sell, at(43), t0 + s(43)),
        NotOurs::SameEvent
    );

    // RB-aoh5-10: a requote place that never rested (refused for its rate, a post-only place
    // that would cross, or not sent) frees its side's slot, so the place after it, the one that
    // rests, is the requote: its cancel 3 s after its place is the repeat, 43 s from the first.
    let mut seen = CancelsNotOurs::new(s(30));
    let (b1, refused, b2) = (cid(), cid(), cid());
    seen.placed(b1, buy, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    seen.placed(refused, buy, t0 + s(25));
    seen.unrested(refused);
    seen.retain(|c| *c != refused);
    seen.placed(b2, buy, t0 + s(40));
    assert_eq!(seen.on_cancel(b2, buy, at(43), t0 + s(43)), NotOurs::Repeat);
    // A replace that ended unrested frees nothing, nor does the other side's: the requote
    // that rested stays timed from its place (40 s), so a replace cancelled 32 s after it is
    // a new first (from the unrested replace's place it would be the repeat).
    let mut seen = CancelsNotOurs::new(s(30));
    let (b1, requote, refused, other, replace) = (cid(), cid(), cid(), cid(), cid());
    seen.placed(b1, buy, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    seen.placed(requote, buy, t0 + s(40));
    seen.placed(other, sell, t0 + s(40));
    seen.unrested(other);
    seen.retain(|c| *c != requote);
    seen.placed(refused, buy, t0 + s(55));
    seen.unrested(refused);
    seen.placed(replace, buy, t0 + s(60));
    assert_eq!(
        seen.on_cancel(replace, buy, at(72), t0 + s(72)),
        NotOurs::First
    );

    // Past 30 s from the window's first event, a new window: a requote cancelled more than
    // 30 s after its place; a later requote within 30 s of its place; a replace of a requote
    // (not the first placed on its side since the event) judged by the window's first only.
    let mut seen = CancelsNotOurs::new(s(30));
    assert_eq!(seen.on_cancel(old, buy, at(0), t0), NotOurs::First);
    let late = cid();
    seen.placed(late, buy, t0 + s(1));
    assert_eq!(
        seen.on_cancel(late, buy, at(32), t0 + s(32)),
        NotOurs::First
    );
    let requote = cid();
    seen.placed(requote, buy, t0 + s(33));
    assert_eq!(
        seen.on_cancel(requote, buy, at(40), t0 + s(40)),
        NotOurs::Repeat
    );
    // Codex r4310042604: a replace of the requote is timed from the requote's place: within
    // 30 s of it the repeat, past it a new first.
    let (requote, replace) = (cid(), cid());
    seen.placed(requote, buy, t0 + s(41));
    seen.placed(replace, buy, t0 + s(50));
    seen.retain(|c| *c != requote);
    assert_eq!(
        seen.on_cancel(replace, buy, at(63), t0 + s(63)),
        NotOurs::Repeat
    );
    let (requote, replace) = (cid(), cid());
    seen.placed(requote, buy, t0 + s(64));
    seen.placed(replace, buy, t0 + s(80));
    seen.retain(|c| *c != requote);
    assert_eq!(
        seen.on_cancel(replace, buy, at(95), t0 + s(95)),
        NotOurs::First
    );
    // A requote that filled: the next order on its side is its replace, timed from its place
    // (past 30 s from the window's first event, 95 s).
    let (requote, next) = (cid(), cid());
    seen.placed(requote, sell, t0 + s(126));
    seen.retain(|c| *c != requote);
    seen.placed(next, sell, t0 + s(140));
    assert_eq!(
        seen.on_cancel(next, sell, at(150), t0 + s(150)),
        NotOurs::Repeat
    );
    // RB-aoh5-2: an order resting unchanged since before the first, cancelled 10 minutes on.
    let mut seen = CancelsNotOurs::new(s(30));
    let (c1, c2) = (cid(), cid());
    seen.placed(c1, buy, t0);
    seen.placed(c2, sell, t0);
    assert_eq!(seen.on_cancel(c1, buy, at(1), t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(c2, sell, at(600), t0 + s(600)),
        NotOurs::First
    );

    // With no venue time, when each was seen: within the span one event, later another.
    let mut seen = CancelsNotOurs::new(s(30));
    let (d1, d2, d3) = (cid(), cid(), cid());
    for order in [d1, d2, d3] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(d1, buy, None, t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(d2, buy, None, t0 + Duration::from_millis(1_900)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.on_cancel(d3, buy, None, t0 + s(5)), NotOurs::Repeat);
}

#[test]
fn a_cancel_of_ours_covers_its_orders_only_once_the_session_reports_it_sent() {
    // Codex r4308625182 / RB-aoh5-3 on PR #159: the session decodes a frame waiting in its turn
    // before it writes what was queued, so a cancel of ours submitted but not yet reported sent
    // covers nothing (another program's cancel read then is not ours); reported sent, it covers
    // its orders; reported not sent (Codex r4308561813), it never does. One another request of
    // ours still covers stays covered.
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let (a, b, c) = (cid(), cid(), cid());
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(1), [a]);
    covered.submitted(RpcId(2), [a, b]);
    assert!(!covered.covers(a) && !covered.covers(b), "pending: not yet");
    covered.reported(RpcId(1), true);
    assert!(covered.covers(a));
    assert!(!covered.covers(b));
    // Request 2 never written: b never covered, a still is (request 1).
    covered.reported(RpcId(2), false);
    assert!(covered.covers(a));
    assert!(!covered.covers(b));
    // A report of no request pending changes nothing.
    covered.reported(RpcId(2), true);
    assert!(!covered.covers(b));
    // Forgotten once ended, pending or covered.
    covered.submitted(RpcId(3), [b, c]);
    covered.reported(RpcId(3), true);
    covered.submitted(RpcId(4), [a, c]);
    covered.retain(|x| *x == b);
    assert!(!covered.covers(a) && covered.covers(b) && !covered.covers(c));
    covered.reported(RpcId(4), true);
    assert!(
        !covered.covers(a) && !covered.covers(c),
        "ended while pending"
    );

    // Codex r4309154204 / RB-aoh5-4: a cancel the venue refused (for its rate, say) no longer
    // covers the order it named, while another request of ours still does; a whole request
    // refused covers nothing it covered; a refusal while pending leaves nothing to cover.
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(5), [a, b]);
    covered.submitted(RpcId(6), [b]);
    covered.reported(RpcId(5), true);
    covered.reported(RpcId(6), true);
    covered.refused(RpcId(5), Some(a));
    assert!(!covered.covers(a), "refused for a");
    covered.refused(RpcId(5), Some(b));
    assert!(covered.covers(b), "request 6 still covers b");
    covered.refused(RpcId(6), None);
    assert!(!covered.covers(b));
    covered.submitted(RpcId(7), [a, c]);
    covered.submitted(RpcId(8), [c]);
    covered.reported(RpcId(8), true);
    covered.refused(RpcId(7), Some(a));
    covered.reported(RpcId(7), true);
    assert!(!covered.covers(a) && covered.covers(c));
    covered.refused(RpcId(7), None);
    assert!(covered.covers(c), "request 8 still covers c");

    // Codex r4310042612: an order a user cancel ended while only unanswered requests of ours
    // covered it is held. Every one refused: released, a cancel not ours; one accepted: ours.
    let (buy, sell) = (Side::Buy, Side::Sell);
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(9), [a]);
    covered.submitted(RpcId(10), [a, b]);
    covered.reported(RpcId(9), true);
    covered.reported(RpcId(10), true);
    assert!(covered.hold(a, buy, Some(ExchNs(7))));
    covered.retain(|_| false);
    assert!(
        covered.refused(RpcId(9), Some(a)).is_empty(),
        "10 still covers a"
    );
    assert_eq!(
        covered.refused(RpcId(10), None),
        vec![link::Released {
            cid: a,
            side: buy,
            at: Some(ExchNs(7))
        }]
    );
    assert!(covered.refused(RpcId(10), None).is_empty(), "released once");
    covered.submitted(RpcId(11), [b]);
    covered.reported(RpcId(11), true);
    assert!(covered.hold(b, sell, None));
    covered.accepted(RpcId(11), Some(b));
    assert!(
        covered.refused(RpcId(11), Some(b)).is_empty(),
        "accepted: ours"
    );
    // Accepted before the order's end: ours, nothing held.
    covered.submitted(RpcId(12), [c]);
    covered.reported(RpcId(12), true);
    covered.accepted(RpcId(12), None);
    assert!(!covered.hold(c, buy, None));
    assert!(!covered.hold(a, buy, None), "covered by nothing");
}
