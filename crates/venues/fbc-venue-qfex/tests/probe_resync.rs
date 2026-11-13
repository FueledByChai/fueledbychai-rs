//! FBC-jxau: the probe codec's resync (decision 0132 item 5), under either probe mode (decisions
//! 0143 and 0145), against a stub that answers each read from a held account.
//!
//! - The resync call signs one `GET /user/positions` with its two nonces, its public key,
//!   signature and subaccount headers `redact: true`, charged `OpKind::Rest`, and asks the first
//!   `get_user_orders` page and its own deadline; the recording-only codec still ends its epoch.
//! - `get_user_orders` pages run at page sizes 1 and 1000 across every boundary, one in flight at
//!   a time, until a short page: orders deduplicated by `order_id`, every full page counted, a
//!   full page adding no new id asking to reconnect, TWAPs and stop orders left out.
//! - Its own deadline and a failed, refused or malformed read ask to reconnect.
//! - One read of a kind: a query is refused while the orders' pages run.
//! - Missed fills are read from item 5's start time, and only rows of orders this codec placed
//!   become replayed fills, with our side, keyed by `trade_id`: an order a live update ends
//!   while the reads run included, and none the codec held only for a resync that has ended.
//! - Nothing is pushed until the last read answers, then one bracket, the open orders before
//!   the fills; and fbc-oms seeds no position from it.
//! - Replaying the redacted journal gives the same events and effects, and the journal holds
//!   no credential, subaccount id or balance value.
//!
//! Every key, account, id and frame here is synthetic, built from the shapes QFEX's pages show
//! or read from `fixtures/qfex/private` (its `SYNTHETIC` file).

#[allow(dead_code)]
mod probe_harness;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step as WsStep, StubServer, WsScript,
};
use fbc_core::{
    CidMatch, CtxCall, Effect, Effects, EncodeCtx, ExecCodec, ExecEvent, FillEvent, FillIdent,
    FillOrigin, HttpFailure, HttpMethod, HttpRequest, HttpTag, Inbound, Lots, MonoNs, NonceBlock,
    NotSentReason, OpKind, RateCharge, RawFrame, Side, SignedLots, SnapshotReads, SnapshotSource,
    Ticks, TimerTag, TrafficClass, VenueCommand, VenueConfig, VenueMeta, VenueOrderSnapshot,
    VenueOrderState, WireSlice, WireUrl,
};
use fbc_core::{ConnKey, Envelope, NonceSource, VenueFactory};
use fbc_journal::{
    JournalReader, JournalWriter, NonceSourceId, Record, RedactionKey, SinkConfig, journal_queue,
};
use fbc_runtime::http::Method;
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecControl, ExecHandler, ExecReplay, ExecReplayConfig,
    ExecSession, ExecSessionConfig, InFlight, IngestClock, Journal, ProxyConfig, RateLimiter,
    RateUserKey, ReconnectPacing, ReservedBound, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::account_reads::PAGE_WEIGHT;
use fbc_venue_qfex::auth::TradeAuth;
use fbc_venue_qfex::exec::{ARM, CHANNELS, QfexExec, TRADE, TRADES_WEIGHT, probe_exec_caps};
use fbc_venue_qfex::probe::{self, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT};

use probe_harness::*;

/// A balance value the positions answer carries, which the journal must not keep.
const BALANCE: &str = "98765.4321";

/// The `n`th made-up venue order UUID.
fn oid(n: u32) -> String {
    format!("{n:08x}-0000-4000-8000-{n:012x}")
}

/// The `n`th made-up trade UUID.
fn tid(n: u32) -> String {
    format!("{n:08x}-1111-4111-8111-{n:012x}")
}

/// An `orders` row: [`ORDER_ROW`] as order `n`, naming no client id, with `changes`.
fn order(n: u32, changes: &[(&str, Option<&str>)]) -> String {
    let id = quoted(&oid(n));
    let mut all: Vec<(&str, Option<&str>)> = vec![
        ("order_id", Some(id.as_str())),
        ("client_order_id", Some("\"\"")),
    ];
    all.extend_from_slice(changes);
    row(ORDER_ROW, &all)
}

/// A `twaps` row, TWAP `n`.
fn twap(n: u32) -> String {
    format!(
        r#"{{"twap_id":"{}","client_twap_id":"","symbol":"AAPL-USD","status":"ACTIVE","side":"BUY"}}"#,
        oid(9_000 + n)
    )
}

/// One `all_orders_response` page.
fn page(orders: &[String], twaps: &[String]) -> String {
    format!(
        r#"{{"all_orders_response":{{"orders":[{}],"twaps":[{}]}}}}"#,
        orders.join(","),
        twaps.join(",")
    )
}

/// What the account shows: its open orders, then its TWAPs.
#[derive(Clone)]
enum Item {
    Order(String),
    Twap(String),
}

/// The stub's page at `offset` of `limit` items of `account`, orders and TWAPs together.
fn slice(account: &[Item], limit: usize, offset: usize) -> String {
    let items: Vec<&Item> = account.iter().skip(offset).take(limit).collect();
    let orders: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            Item::Order(row) => Some(row.clone()),
            Item::Twap(_) => None,
        })
        .collect();
    let twaps: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            Item::Twap(row) => Some(row.clone()),
            Item::Order(_) => None,
        })
        .collect();
    page(&orders, &twaps)
}

/// Every page the stub answers `account` with at `limit`, from offset 0 to its first short page.
fn pages(account: &[Item], limit: usize) -> Vec<String> {
    (0..=account.len() / limit)
        .map(|k| slice(account, limit, k * limit))
        .collect()
}

/// The open order `n` of [`order`] as the resync shows it: our snapshot of a buy of 1.00 at
/// 200.00 on `AAPL-USD`, nothing filled, naming no client id.
fn shown(n: u32) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: None,
        vid: venue_id(&oid(n)),
        inst: AAPL,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(20_000)),
        qty: Lots::new(100).unwrap(),
        cum_filled: Lots::new(0).unwrap(),
        post_only: None,
        reduce_only: None,
    }
}

/// A `GET /user/positions` answer: each row's symbol and position, and a balance.
fn positions(rows: &[(&str, &str)]) -> String {
    let rows: Vec<String> = rows
        .iter()
        .map(|(symbol, position)| {
            format!(r#"{{"symbol":"{symbol}","position":{position},"open_orders":0}}"#)
        })
        .collect();
    format!(
        r#"{{"balance":{{"deposit":{BALANCE},"available_balance":{BALANCE}}},"positions":[{}]}}"#,
        rows.join(",")
    )
}

/// The account flat: `AAPL-USD` zero, and a symbol the spec table does not hold.
fn flat() -> String {
    positions(&[("AAPL-USD", "0"), ("NVDA-USD", "-3.5")])
}

/// A trade row: trade `n` of order `order` on `AAPL-USD`, 1.0 at 200.0, its fee 0.002, under
/// `cid` when it names one, its time `1760000004`.
fn trade(n: u32, order: &str, cid: Option<&str>) -> String {
    let cid = cid.map_or(String::new(), |cid| {
        format!(r#","client_order_id":{}"#, quoted(cid))
    });
    format!(
        r#"{{"trade_id":"{}","symbol":"AAPL-USD","price":200.0,"quantity":1.0,"aggressor_side":"BUY","order_id":"{order}","fee":0.002,"timestamp":1760000004{cid}}}"#,
        tid(n)
    )
}

/// The documented trades page, with its count.
fn trades(rows: &[String]) -> String {
    format!(
        r#"{{"user_trades":[{}],"count":{}}}"#,
        rows.join(","),
        rows.len()
    )
}

/// The AsyncAPI's trades page.
fn trades_response(rows: &[String]) -> String {
    format!(r#"{{"user_trades_response":[{}]}}"#, rows.join(","))
}

/// The `get_user_orders` page the resync asks for.
fn orders_ask(limit: u16, offset: u32) -> Effect {
    read(
        format!(r#"{{"type":"get_user_orders","params":{{"limit":{limit},"offset":{offset}}}}}"#),
        PAGE_WEIGHT,
    )
}

/// The `get_user_trades` page the resync asks for.
fn trades_ask(start: u64, limit: u16, offset: u32) -> Effect {
    read(
        format!(
            r#"{{"type":"get_user_trades","params":{{"start_ts":{start},"limit":{limit},"offset":{offset}}}}}"#
        ),
        TRADES_WEIGHT,
    )
}

fn read(body: String, weight: std::num::NonZeroU32) -> Effect {
    Effect::Send {
        stream: TRADE,
        frame: WireSlice::plain(body.into_bytes()),
        rpc: None,
        class: TrafficClass::Safety,
        charge: RateCharge {
            op: OpKind::Query,
            inst: None,
            weight,
        },
    }
}

/// The positions read's tag, the resync's deadline's tag, on epoch `n`.
fn positions_tag(n: u64) -> HttpTag {
    HttpTag((3 << 32) | n)
}

fn resync_timer(n: u64) -> TimerTag {
    TimerTag((2 << 32) | n)
}

/// The resync's three effects on epoch `n` of `cfg`'s codec, asked with `ctx(at)`: the signed
/// positions read, the first page at `limit` and the deadline.
fn resync_effects(cfg: &VenueConfig, n: u64, at: u64, limit: u16) -> Vec<Effect> {
    let rest = probe::connections(cfg).unwrap().rest.to_owned();
    let headers = TradeAuth::new(cfg, secrets())
        .unwrap()
        .rest_headers(&ctx(at))
        .unwrap();
    vec![
        Effect::Http {
            tag: positions_tag(n),
            req: HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain(format!("{rest}/user/positions")),
                headers,
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: RESYNC_TIMEOUT,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Rest, None),
        },
        orders_ask(limit, 0),
        Effect::Timer {
            tag: resync_timer(n),
            after: RESYNC_TIMEOUT,
        },
    ]
}

/// The probe on `cfg` with page size `size`.
fn sized(cfg: VenueConfig, size: u16) -> VenueConfig {
    with(cfg, KEY_PAGE_SIZE, &size.to_string())
}

/// A bracket, as pushed: `ResyncBegin` at `ctx(at)`'s wall time, then `events`, then
/// `ResyncEnd`.
fn bracket(at: u64, events: Vec<ExecEvent>) -> Vec<ExecEvent> {
    let mut all = vec![ExecEvent::ResyncBegin {
        watermark: ctx(at).wall,
    }];
    all.extend(events);
    all.push(ExecEvent::ResyncEnd);
    all
}

/// Each step's events, with no venue time.
fn untimed(out: &Out) -> Vec<ExecEvent> {
    out.events
        .iter()
        .map(|(meta, event)| {
            assert_eq!(*meta, VenueMeta::NONE);
            event.clone()
        })
        .collect()
}

/// Whether `out` asked to reconnect and nothing else.
fn reconnects(out: &Out) -> bool {
    out.events.is_empty() && matches!(out.effects.as_slice(), [Effect::Reconnect { .. }])
}

/// The steps of a resync at `ctx(5)` on epoch 1, the positions answered `body` first.
fn resynced(body: &str) -> Vec<Step> {
    let mut steps = epoch(1);
    steps.push(Step::Resync(5));
    steps.push(Step::Http {
        nth: 0,
        status: 200,
        body: body.to_owned(),
    });
    steps
}

// ----- The signed read -----

#[test]
fn the_resync_signs_one_positions_read_with_redacted_headers_and_asks_the_first_page() {
    for cfg in [
        preprod("primary"),
        preprod(SUB),
        production(SUB),
        production_loopback("primary"),
        sized(loopback(SUB), 1),
    ] {
        let size = probe::mode(&cfg).unwrap().unwrap().page_size();
        let want = resync_effects(&cfg, 1, 5, size);
        let subaccount = cfg.get(fbc_venue_qfex::auth::ACCOUNT_ID) == Some(SUB);
        let Effect::Http { req, .. } = &want[0] else {
            unreachable!()
        };
        // The public key and the signature, and the subaccount for a subaccount, are spans;
        // the nonce and the time are in clear.
        let flags: Vec<bool> = req.headers.iter().map(|h| h.redact).collect();
        let mut expected = vec![true, true, false, false];
        if subaccount {
            expected.push(true);
        }
        assert_eq!(flags, expected, "{req:?}");
        let mut direct = probe_on(&cfg);
        let mut built = factory(&cfg);
        for codec in [&mut direct as &mut dyn ExecCodec, built.as_mut()] {
            assert_eq!(codec.nonces_for(CtxCall::Resync), TradeAuth::REST_NONCES);
            authenticate(codec, 1);
            assert_eq!(resync(codec, 5), Out::effects(want.clone()));
        }

        // The journal keeps the read with its secret headers blanked, and the answer's balance
        // values as spans; replay reads both alike.
        let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &resynced(&flat()));
        assert_eq!(outs[5], Out::effects(want.clone()));
        let kept: Vec<(&str, bool)> = disk.requests[0]
            .headers
            .iter()
            .map(|h| (h.name.as_str(), h.value.bytes().all(|b| b == b'2')))
            .collect();
        assert_eq!(kept.len(), expected.len());
        for ((_, blank), redact) in kept.iter().zip(&expected) {
            assert_eq!(blank, redact, "{kept:?}");
        }
        for secret in [PUBLIC, SUB, SECRET, BALANCE] {
            assert!(!disk.has(secret), "{secret} on disk");
        }
        assert!(disk.has("NVDA-USD") && disk.has("-3.5"));
        assert_eq!(codec.counts().unheld_rows, 1);
    }
    // Recording-only, it ends the epoch and asks for no nonce.
    let mut off = recording(&preprod(SUB));
    assert_eq!(off.nonces_for(CtxCall::Resync), 0);
    authenticate(&mut off, 1);
    assert!(reconnects(&resync(&mut off, 5)));
}

#[test]
fn a_resync_out_of_turn_or_unsigned_asks_to_reconnect() {
    let cfg = preprod(SUB);
    // Before the stream is authenticated.
    let mut codec = probe_on(&cfg);
    open(&mut codec, 1);
    assert!(reconnects(&resync(&mut codec, 5)));
    // A second resync while the first is in flight: the first ends unfinished.
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    assert_eq!(resync(&mut codec, 5).effects.len(), 3);
    assert!(reconnects(&resync(&mut codec, 6)));
    assert_eq!(codec.counts().resyncs_unfinished, 1);
    assert!(!codec.last_resync().unwrap().finished);
    // With a query in flight, whose answer is a page's frame.
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    assert!(encode(&mut codec, &query(ORDER), 2).result.is_ok());
    assert!(reconnects(&resync(&mut codec, 5)));
    // With no nonce for the read's signature.
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    let unsigned = EncodeCtx {
        wall: ctx(5).wall,
        mono: MonoNs(5),
        nonces: NonceBlock::new(Vec::new()),
    };
    let mut fx = Effects::new();
    codec.resync(&unsigned, &mut fx);
    assert!(matches!(fx.take().as_slice(), [Effect::Reconnect { .. }]));
    assert_eq!(codec.counts().resyncs_unfinished, 0);
}

// ----- get_user_orders pages -----

/// Orders 1, 2 and 3, a take-profit order, a TWAP and an order on a symbol the table does not
/// hold, in that order.
fn account() -> Vec<Item> {
    vec![
        Item::Order(order(1, &[])),
        Item::Order(order(2, &[])),
        Item::Order(order(4, &[("type", Some("\"TAKE_PROFIT\""))])),
        Item::Order(order(3, &[])),
        Item::Order(order(5, &[("symbol", Some("\"NVDA-USD\""))])),
        Item::Twap(twap(1)),
        Item::Order(order(6, &[("type", Some("\"STOP_LOSS\""))])),
    ]
}

#[test]
fn pages_get_user_orders_across_boundaries_at_page_sizes_1_and_1000_and_counts_full_pages() {
    let open = vec![
        ExecEvent::ResyncOrder(shown(1)),
        ExecEvent::ResyncOrder(shown(2)),
        ExecEvent::ResyncOrder(shown(3)),
    ];
    for (size, full) in [(1_u16, 7_u64), (2, 3), (7, 1), (1000, 0)] {
        for base in [preprod("primary"), production(SUB)] {
            let cfg = sized(base, size);
            let answers = pages(&account(), usize::from(size));
            let mut steps = resynced(&flat());
            steps.extend(answers.iter().map(|p| Step::frame(p)));
            let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
            // Each page asks the next, one in flight at a time, until the short one.
            for (k, out) in outs[7..outs.len() - 1].iter().enumerate() {
                let offset = (k as u32 + 1) * u32::from(size);
                assert_eq!(*out, Out::effects(vec![orders_ask(size, offset)]), "{k}");
            }
            // Only the last answer pushes, the stop orders, the TWAP and the order the table
            // does not hold left out.
            let last = outs.last().unwrap();
            assert!(last.effects.is_empty() && last.result.is_ok(), "{last:?}");
            assert_eq!(untimed(last), bracket(5, open.clone()), "size {size}");
            assert!(outs[5..outs.len() - 1].iter().all(|o| o.events.is_empty()));
            let counts = codec.counts();
            assert_eq!(
                (counts.full_pages, counts.resyncs),
                (full, 1),
                "size {size}"
            );
            assert_eq!(counts.unheld_rows, 2, "a page row and a position row");
            let evidence = codec.last_resync().unwrap();
            assert!(evidence.finished && evidence.start_ts.is_none());
            let offsets: Vec<(u16, u32)> = evidence
                .full_pages
                .iter()
                .map(|ask| (ask.limit(), ask.offset()))
                .collect();
            let want: Vec<(u16, u32)> = (0..full as u32)
                .map(|k| (size, k * u32::from(size)))
                .collect();
            assert_eq!(offsets, want);
            assert_eq!(evidence.positions.len(), 2);
        }
    }
    // 1001 orders at page size 1000: one full page, then one of one order.
    let cfg = sized(preprod("primary"), 1000);
    let account: Vec<Item> = (1..=1001).map(|n| Item::Order(order(n, &[]))).collect();
    let mut steps = resynced(&flat());
    steps.extend(pages(&account, 1000).iter().map(|p| Step::frame(p)));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(outs[7], Out::effects(vec![orders_ask(1000, 1000)]));
    let events = untimed(&outs[8]);
    assert_eq!(events.len(), 1003);
    assert_eq!(events[1], ExecEvent::ResyncOrder(shown(1)));
    assert_eq!(events[1001], ExecEvent::ResyncOrder(shown(1001)));
    assert_eq!(codec.counts().full_pages, 1);
}

#[test]
fn deduplicates_order_ids_a_shifting_set_repeats_across_pages() {
    // Page size 2: order 9 arrives at the front between the first page and the second, so the
    // second repeats order 2, which has filled 0.4 meanwhile: the later row replaces it.
    let cfg = sized(preprod(SUB), 2);
    let mut steps = resynced(&flat());
    steps.push(Step::Frame(page(&[order(1, &[]), order(2, &[])], &[])));
    steps.push(Step::Frame(page(
        &[
            order(2, &[("quantity_remaining", Some("0.6"))]),
            order(3, &[]),
        ],
        &[],
    )));
    steps.push(Step::Frame(page(&[order(4, &[])], &[])));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(outs[7], Out::effects(vec![orders_ask(2, 2)]));
    assert_eq!(outs[8], Out::effects(vec![orders_ask(2, 4)]));
    let mut two = shown(2);
    two.cum_filled = Lots::new(40).unwrap();
    assert_eq!(
        untimed(&outs[9]),
        bracket(
            5,
            vec![
                ExecEvent::ResyncOrder(shown(1)),
                ExecEvent::ResyncOrder(two),
                ExecEvent::ResyncOrder(shown(3)),
                ExecEvent::ResyncOrder(shown(4)),
            ]
        )
    );
    assert_eq!(codec.counts().duplicate_rows, 1);
    assert_eq!(codec.counts().full_pages, 2);
}

#[test]
fn a_full_page_adding_no_new_id_asks_to_reconnect() {
    for cfg in [sized(preprod(SUB), 1), sized(production("primary"), 1)] {
        let mut steps = resynced(&flat());
        steps.push(Step::Frame(page(&[order(1, &[])], &[])));
        steps.push(Step::Frame(page(&[order(1, &[])], &[])));
        steps.push(Step::Frame(page(&[], &[])));
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        assert_eq!(outs[7], Out::effects(vec![orders_ask(1, 1)]));
        assert!(
            reconnects(&outs[8]) && outs[8].result.is_ok(),
            "{:?}",
            outs[8]
        );
        // Later frames are not taken: the epoch has ended.
        assert!(reconnects(&outs[9]), "{:?}", outs[9]);
        let counts = codec.counts();
        assert_eq!(
            (counts.full_pages, counts.resyncs, counts.resyncs_unfinished),
            (2, 0, 1)
        );
        let evidence = codec.last_resync().unwrap();
        assert!(!evidence.finished);
        assert_eq!(evidence.full_pages.len(), 2);
        assert_eq!(evidence.positions.len(), 2);
    }
    // A full page of TWAPs alone names new ids, so the paging goes on.
    let cfg = sized(preprod(SUB), 1);
    let mut steps = resynced(&flat());
    steps.push(Step::Frame(page(&[], &[twap(1)])));
    steps.push(Step::Frame(page(&[], &[twap(2)])));
    steps.push(Step::Frame(page(&[], &[])));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(outs[8], Out::effects(vec![orders_ask(1, 2)]));
    assert_eq!(untimed(&outs[9]), bracket(5, vec![]));
    assert_eq!(codec.counts().full_pages, 2);
}

// ----- Its own deadline, and failed reads -----

#[test]
fn its_own_timer_asks_to_reconnect_until_resync_end_and_a_failed_read_does_too() {
    let cfg = preprod(SUB);
    // The deadline before the last read answers: the second timer asked, the first being the
    // authentication's.
    let mut steps = resynced(&flat());
    steps.push(Step::Timer { nth: 1, at: 40 });
    steps.push(Step::Frame(page(&[], &[])));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert!(reconnects(&outs[7]), "{:?}", outs[7]);
    assert!(
        reconnects(&outs[8]),
        "the page after the deadline is not taken"
    );
    assert_eq!(codec.counts().resyncs_unfinished, 1);
    // After the bracket, or from an earlier epoch, it does nothing.
    let mut steps = resynced(&flat());
    steps.push(Step::Frame(page(&[], &[])));
    steps.push(Step::Timer { nth: 1, at: 40 });
    steps.extend(epoch(2));
    steps.push(Step::Resync(9));
    steps.push(Step::Timer { nth: 1, at: 41 });
    steps.push(Step::Timer { nth: 3, at: 42 });
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(untimed(&outs[7]), bracket(5, vec![]));
    assert_eq!(outs[8], Out::quiet());
    assert_eq!(outs[14], Out::effects(resync_effects(&cfg, 2, 9, 1000)));
    assert_eq!(outs[15], Out::quiet(), "epoch 1's deadline");
    assert!(reconnects(&outs[16]), "epoch 2's deadline");
    assert_eq!(codec.counts().resyncs, 1);

    // A read that failed, a status outside 2xx and an answer not as documented.
    for (answer, malformed) in [
        (Err(HttpFailure::TimedOut), false),
        (Err(HttpFailure::NotSent), false),
        (Err(HttpFailure::Lost), false),
        (
            Ok((
                401,
                r#"{"error":"unauthorized-synthetic-detail"}"#.to_owned(),
            )),
            false,
        ),
        (Ok((500, String::new())), false),
        (Ok((200, r#"{"positions":[]}"#.to_owned())), true),
        (Ok((200, "not json".to_owned())), true),
        // Off the size step on a held symbol.
        (Ok((200, positions(&[("AAPL-USD", "0.005")]))), true),
        (Ok((200, fixture("positions.json"))), true),
    ] {
        let mut steps = epoch(1);
        steps.push(Step::Resync(5));
        steps.push(match answer.clone() {
            Ok((status, body)) => Step::Http {
                nth: 0,
                status,
                body,
            },
            Err(failure) => Step::HttpFailed { nth: 0, failure },
        });
        steps.push(Step::Frame(page(&[], &[])));
        let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
        assert!(
            outs[6].asks_to_reconnect() && outs[6].events.is_empty(),
            "{answer:?}"
        );
        let want = match malformed {
            true => Err(format!(
                "{:?}",
                fbc_core::DecodeError::Malformed("invalid QFEX positions answer")
            )),
            false => Ok(()),
        };
        assert_eq!(outs[6].result, want, "{answer:?}");
        assert!(reconnects(&outs[7]));
        assert_eq!(codec.counts().resyncs_unfinished, 1);
        // A body outside 2xx is kept as a span, whole.
        assert!(!disk.has("unauthorized-synthetic-detail"));
    }

    // An answer the codec did not ask for: of an earlier epoch, or a second one.
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    resync(&mut codec, 5);
    let body = flat();
    let ok = || {
        Ok(fbc_core::HttpResponse {
            status: 200,
            headers: &[],
            body: body.as_bytes(),
        })
    };
    let unexpected = Err(format!(
        "{:?}",
        fbc_core::DecodeError::Malformed("unexpected QFEX private HTTP response")
    ));
    assert_eq!(http(&mut codec, positions_tag(7), ok()).result, unexpected);
    assert_eq!(http(&mut codec, positions_tag(1), ok()), Out::quiet());
    assert_eq!(http(&mut codec, positions_tag(1), ok()).result, unexpected);

    // A RateLimited err answering a page holds the query bucket and settles nothing: the
    // resync waits for its deadline.
    let limited = nested(
        "RateLimited",
        r#""Rate limit exceeded, retry after 3 seconds""#,
        r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#,
    );
    let mut steps = resynced(&flat());
    steps.push(Step::Frame(limited));
    steps.push(Step::Timer { nth: 1, at: 40 });
    let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(
        outs[7],
        Out::effects(vec![Effect::RateLimited {
            stream: TRADE,
            charge: Some(RateCharge::one(OpKind::Query, None)),
            retry_after: Some(Duration::from_secs(3)),
        }])
    );
    assert!(reconnects(&outs[8]));

    // A page not as documented ends the epoch as a decode error: an undocumented member, an
    // order row on a held symbol off the grid or with no side. One on a symbol the table does
    // not hold is not read for the grid.
    let invalid = Err(format!(
        "{:?}",
        fbc_core::DecodeError::Malformed("invalid QFEX resync page")
    ));
    for bad in [
        r#"{"all_orders_response":{"orders":[],"twaps":[],"stop_orders":[]}}"#.to_owned(),
        page(&[order(1, &[("price", Some("200.005"))])], &[]),
        page(&[order(1, &[("side", None)])], &[]),
        page(&[order(1, &[("order_id", None)])], &[]),
        page(
            &[order(
                1,
                &[("symbol", Some("\"NVDA-USD\"")), ("order_id", None)],
            )],
            &[],
        ),
        r#"{"all_orders_response":{"orders":[1],"twaps":[]}}"#.to_owned(),
    ] {
        let mut steps = resynced(&flat());
        steps.push(Step::Frame(bad.clone()));
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        assert!(
            outs[7].asks_to_reconnect() && outs[7].events.is_empty(),
            "{bad}"
        );
        assert_eq!(outs[7].result, invalid, "{bad}");
        assert_eq!(codec.counts().resyncs_unfinished, 1);
    }
    // Rows on a symbol the table does not hold are skipped and counted whatever they hold but
    // their order id (decision 0147 item 2): off the grid, a price of zero, no side, more left
    // than the quantity.
    let nvda = Some("\"NVDA-USD\"");
    let unheld = page(
        &[
            order(1, &[("symbol", nvda), ("price", Some("200.005"))]),
            order(2, &[("symbol", nvda), ("price", Some("0"))]),
            order(3, &[("symbol", nvda), ("side", None)]),
            order(4, &[("symbol", nvda), ("quantity_remaining", Some("5"))]),
        ],
        &[],
    );
    let mut steps = resynced(&flat());
    steps.push(Step::Frame(unheld));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(untimed(&outs[7]), bracket(5, vec![]));
    assert_eq!(
        codec.counts().unheld_rows,
        5,
        "four page rows and a position row"
    );
}

// ----- One read of a kind -----

#[test]
fn one_reply_of_a_kind_is_in_flight() {
    let cfg = preprod(SUB);
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    assert!(encode(&mut codec, &gtc_place(), 3).result.is_ok());
    resync(&mut codec, 5);
    // While the orders' pages run, a query, answered by a page's frame, is refused unwritten.
    assert_eq!(
        encode(&mut codec, &query(ORDER), 6),
        refused(NotSentReason::Backpressure)
    );
    // A frame naming a page and another envelope is not read as a page, but refused.
    let two = r#"{"all_orders_response":{"orders":[],"twaps":[]},"user_trades":[],"count":0}"#;
    assert_eq!(
        text(&mut codec, two),
        Out {
            result: Err(format!(
                "{:?}",
                fbc_core::DecodeError::Malformed("invalid QFEX private state")
            )),
            ..Out::quiet()
        }
    );
    // A trades page meanwhile is not read as the orders' page.
    let early = trades(&[]);
    assert_eq!(text(&mut codec, &early), Out::quiet());
    assert_eq!(codec.counts().pages_unasked, 1);
    // Once they end, the trades read runs, and a query goes out.
    assert_eq!(
        text(&mut codec, &page(&[], &[])),
        Out::effects(vec![trades_ask(1_760_000_003, 1000, 0)])
    );
    assert!(encode(&mut codec, &query(ORDER), 7).result.is_ok());
    // Its answer is not read as a page of the resync.
    assert_eq!(
        text(&mut codec, &fixture("all_orders_response.json")),
        Out::quiet()
    );
    assert_eq!(codec.counts().pages_unasked, 2);

    // While a page is in flight the arm is not the only request in flight, so the arm's window
    // is closed: an escaped err is kept whole (0134 item 2), as outside it; the arm's own echo
    // is kept in clear all the same, and answers it.
    let escaped = nested("InvalidOrder", r#""a \"quoted\" reason""#, "{}");
    let spans = |codec: &QfexExec, frame: &str| {
        codec
            .redact_inbound(Inbound::Frame(RawFrame::Text(frame)))
            .body()
            .to_vec()
    };
    let whole: Vec<std::ops::Range<u32>> = std::iter::once(0..escaped.len() as u32).collect();
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    encode(&mut codec, &ARM_ON, 1);
    assert_ne!(spans(&codec, &escaped), whole, "in the window");
    resync(&mut codec, 5);
    assert_eq!(spans(&codec, &escaped), whole, "a page in flight");
    assert!(spans(&codec, &ack(ARM)).is_empty());
    assert_eq!(text(&mut codec, &ack(ARM)), Out::events(vec![accepted(1)]));
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    encode(&mut codec, &ARM_ON, 1);
    resync(&mut codec, 5);
    text(&mut codec, &page(&[], &[]));
    assert_ne!(spans(&codec, &escaped), whole, "no page in flight");
}

/// Our limit GTC buy of 1.00 at 200.50 on `AAPL-USD`.
fn gtc_place() -> VenueCommand {
    VenueCommand::Place(gtc())
}

// ----- Missed fills -----

/// Our order's `ACK`, under our first client id, as [`ORDER`].
fn our_ack() -> String {
    order_frame(&[("client_order_id", Some(&quoted(&wire_cid(0))))])
}

#[test]
fn reads_trades_from_item_5s_start_time_and_only_our_orders_become_replayed_fills() {
    for size in [1000_u16, 1] {
        let cfg = sized(preprod(SUB), size);
        // Epoch 1: our place at 1760000003 s, its ack, and a fill of it at 1760000002.75.
        let mut steps = epoch(1);
        steps.push(Step::Encode(gtc_place(), 3));
        steps.push(Step::Frame(our_ack()));
        steps.push(Step::Frame(fill_frame(&[
            ("client_order_id", Some(&quoted(&wire_cid(0)))),
            ("timestamp", Some("1760000002.75")),
        ])));
        // Epoch 2: the resync reads trades from 1760000002, the earlier, rounded down.
        steps.extend(epoch(2));
        steps.push(Step::Resync(9));
        steps.push(Step::Http {
            nth: 0,
            status: 200,
            body: flat(),
        });
        steps.push(Step::Frame(page(&[], &[])));
        let ours_by_cid = trade(1, ORDER, Some(&wire_cid(0)));
        let ours_by_vid = trade(2, ORDER, None);
        let foreign = trade(3, &oid(77), Some(""));
        let unheld = trade(4, ORDER, None).replace("AAPL-USD", "NVDA-USD");
        let rows = [
            ours_by_cid.clone(),
            foreign,
            ours_by_vid,
            ours_by_cid,
            unheld,
        ];
        let answers: Vec<String> = if size == 1 {
            let mut answers: Vec<String> = rows
                .iter()
                .map(|row| trades_response(std::slice::from_ref(row)))
                .collect();
            answers.push(trades_response(&[]));
            answers
        } else {
            vec![trades(&rows)]
        };
        let first = steps.len();
        steps.extend(answers.iter().map(|a| Step::frame(a)));
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        let ExecEvent::Fill(live) = untimed(&outs[7])[0].clone() else {
            panic!("{:?}", outs[7]);
        };
        assert_eq!(
            outs[first - 1],
            Out::effects(vec![trades_ask(1_760_000_002, size, 0)])
        );
        for (k, out) in outs[first..outs.len() - 1].iter().enumerate() {
            let offset = (k as u32 + 1) * u32::from(size);
            assert_eq!(
                *out,
                Out::effects(vec![trades_ask(1_760_000_002, size, offset)])
            );
        }
        let replayed = |n: u32, cid: Option<CidMatch>| {
            ExecEvent::Fill(FillEvent {
                ident: FillIdent::Venue {
                    fill: fill_id(&tid(n)),
                    vid: Some(venue_id(ORDER)),
                    cum_after: None,
                },
                origin: FillOrigin::Order {
                    cid,
                    px: Ticks(20_000),
                },
                // Our side, from the order placed: the row names the aggressor's alone.
                side: Side::Buy,
                replay: true,
                ..live.clone()
            })
        };
        assert_eq!(
            untimed(outs.last().unwrap()),
            bracket(
                9,
                vec![
                    replayed(1, Some(CidMatch::Ours(cids()[0]))),
                    replayed(2, None),
                ]
            ),
            "size {size}"
        );
        let counts = codec.counts();
        assert_eq!((counts.unplaced_trades, counts.duplicate_rows), (2, 1));
        let evidence = codec.last_resync().unwrap();
        assert_eq!(evidence.start_ts, Some(1_760_000_002));
        assert_eq!(
            evidence.unplaced_trades,
            vec![fill_id(&tid(3)), fill_id(&tid(4))]
        );
        assert!(evidence.full_pages.is_empty());
    }
}

#[test]
fn the_start_time_is_the_oldest_unended_place_or_the_latest_fill_and_neither_reads_nothing() {
    let cfg = preprod(SUB);
    let start = |steps: Vec<Step>| {
        let mut steps = steps;
        steps.push(Step::Resync(9));
        steps.push(Step::Http {
            nth: 0,
            status: 200,
            body: flat(),
        });
        steps.push(Step::Frame(page(&[], &[])));
        let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        outs.last().unwrap().clone()
    };
    let from = |at: u64| Out::effects(vec![trades_ask(at, 1000, 0)]);
    // A place alone: its send time.
    let mut steps = epoch(1);
    steps.push(Step::Encode(gtc_place(), 3));
    assert_eq!(start(steps), from(1_760_000_003));
    // A fill alone, of another order: its time, rounded down.
    let mut steps = epoch(1);
    steps.push(Step::Frame(fill_frame(&[(
        "timestamp",
        Some("1760000001.99"),
    )])));
    assert_eq!(start(steps), from(1_760_000_001));
    // A place that ended, cancelled or filled: none.
    for end in [
        order_frame(&[
            ("client_order_id", Some(&quoted(&wire_cid(0)))),
            ("status", Some("\"CANCELLED\"")),
        ]),
        order_frame(&[
            ("client_order_id", Some(&quoted(&wire_cid(0)))),
            ("status", Some("\"FILLED\"")),
            ("quantity_remaining", Some("0")),
        ]),
    ] {
        let mut steps = epoch(1);
        steps.push(Step::Encode(gtc_place(), 3));
        steps.push(Step::Frame(our_ack()));
        steps.push(Step::Frame(end));
        assert_eq!(untimed(&start(steps)), bracket(9, vec![]));
    }
    // Another order's end leaves ours placed.
    let mut steps = epoch(1);
    steps.push(Step::Encode(gtc_place(), 3));
    steps.push(Step::Frame(order_frame(&[(
        "status",
        Some("\"CANCELLED\""),
    )])));
    assert_eq!(start(steps), from(1_760_000_003));
    // Under production_probe no place is sent, so nothing places a start time.
    let cfg = production(SUB);
    let mut codec = probe_on(&cfg);
    authenticate(&mut codec, 1);
    assert_eq!(
        encode(&mut codec, &gtc_place(), 3),
        refused(NotSentReason::Unsupported)
    );
    resync(&mut codec, 9);
    assert_eq!(codec.last_resync(), None);
    assert_eq!(text(&mut codec, &page(&[], &[])), Out::quiet());
}

// ----- What is pushed, and when -----

#[test]
fn pushes_orders_before_fills_inside_one_bracket() {
    let cfg = sized(preprod(SUB), 1);
    let mut prefix = epoch(1);
    prefix.push(Step::Encode(gtc_place(), 3));
    prefix.push(Step::Frame(our_ack()));
    prefix.extend(epoch(2));
    prefix.push(Step::Resync(9));
    let long = positions(&[("AAPL-USD", "1.5"), ("NVDA-USD", "2")]);
    let reads = vec![
        Step::Frame(page(&[order(1, &[])], &[])),
        Step::Frame(page(&[], &[])),
        Step::Frame(trades_response(&[trade(1, ORDER, None)])),
        Step::Frame(trades_response(&[])),
    ];
    let want = bracket(
        9,
        vec![
            ExecEvent::ResyncOrder(shown(1)),
            ExecEvent::ResyncPosition {
                inst: AAPL,
                qty: SignedLots(150),
                avg_entry: None,
            },
            replayed_fill(),
        ],
    );
    // The positions answer first, then last.
    for positions_at in [0, reads.len()] {
        let mut steps = prefix.clone();
        let mut reads = reads.clone();
        reads.insert(
            positions_at,
            Step::Http {
                nth: 0,
                status: 200,
                body: long.clone(),
            },
        );
        steps.extend(reads);
        let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        let tail = &outs[prefix.len()..];
        for out in &tail[..tail.len() - 1] {
            assert!(out.events.is_empty(), "{out:?}");
        }
        assert_eq!(untimed(tail.last().unwrap()), want, "at {positions_at}");
    }
}

/// Trade 1 of our order, found by its venue id, as the resync pushes it.
fn replayed_fill() -> ExecEvent {
    let fee = dispatch_fee(2_000_000);
    ExecEvent::Fill(FillEvent {
        ident: FillIdent::Venue {
            fill: fill_id(&tid(1)),
            vid: Some(venue_id(ORDER)),
            cum_after: None,
        },
        origin: FillOrigin::Order {
            cid: None,
            px: Ticks(20_000),
        },
        inst: AAPL,
        side: Side::Buy,
        qty: Lots::new(100).unwrap(),
        liquidity: fbc_core::Liquidity3::Unknown,
        fee,
        realized_pnl: None,
        realized_funding: None,
        replay: true,
    })
}

fn fill_id(id: &str) -> fbc_core::FillId {
    fbc_core::dispatch_market_data(&caps(), |scope| scope.fill_id(id)).unwrap()
}

/// A fee of `nanos` dollars' billionths, as the scope builds one.
fn dispatch_fee(nanos: i128) -> fbc_core::Fee {
    let usd = fbc_core::AssetSym::new("USD").unwrap();
    fbc_core::dispatch(&caps(), NS, |scope| scope.fee(nanos, usd)).unwrap()
}

/// Our place on epoch 1, no answer of it seen, then epoch 2's resync at `ctx(9)`: the account
/// flat and no open order, so the trades read from the place's send time is in flight.
fn reading_trades() -> Vec<Step> {
    let mut steps = epoch(1);
    steps.push(Step::Encode(gtc_place(), 3));
    steps.extend(epoch(2));
    steps.push(Step::Resync(9));
    steps.push(Step::Http {
        nth: 0,
        status: 200,
        body: flat(),
    });
    steps.push(Step::Frame(page(&[], &[])));
    steps
}

#[test]
fn a_trades_page_or_held_trade_row_not_as_documented_asks_to_reconnect() {
    // Decision 0147 item 2: a trade row on a held symbol off the grid or off the step, and a
    // page not as documented, ask to reconnect, pushing nothing; one on a symbol the table does
    // not hold is read for its two ids alone, and kept as evidence.
    let cfg = preprod(SUB);
    let invalid = Err(format!(
        "{:?}",
        fbc_core::DecodeError::Malformed("invalid QFEX resync page")
    ));
    let held = trade(1, ORDER, None);
    let unheld = |n: u32, from: &str, to: &str| {
        let row = trade(n, ORDER, None).replace("AAPL-USD", "NVDA-USD");
        assert!(row.contains(from), "{from}");
        row.replace(from, to)
    };
    let changed = |from: &str, to: &str| {
        assert!(held.contains(from), "{from}");
        trades(&[held.replace(from, to)])
    };
    let bad = [
        // The documented page with no count, a count not a whole number, rows not an array, and
        // a row not an object.
        format!(r#"{{"user_trades":[{held}]}}"#),
        r#"{"user_trades":[],"count":-1}"#.to_owned(),
        r#"{"user_trades_response":{}}"#.to_owned(),
        r#"{"user_trades_response":[1]}"#.to_owned(),
        // A held row: off the grid, off the step, a fee finer than a nano, a price or a
        // quantity not positive, a fee not a number, a trade id or an order id not a UUID, a
        // negative time, a client id not a string.
        changed(r#""price":200.0"#, r#""price":200.005"#),
        changed(r#""quantity":1.0"#, r#""quantity":1.005"#),
        changed(r#""fee":0.002"#, r#""fee":0.0000000001"#),
        changed(r#""price":200.0"#, r#""price":0"#),
        changed(r#""quantity":1.0"#, r#""quantity":-1.0"#),
        changed(r#""fee":0.002"#, r#""fee":"0.002""#),
        changed(&tid(1), "not-a-uuid"),
        changed(ORDER, "not-an-order"),
        changed("1760000004", "-1"),
        changed("1760000004", r#"1760000004,"client_order_id":7"#),
        // A row the table does not hold whose ids are not as documented.
        trades(&[unheld(2, &tid(2), "not-a-uuid")]),
        trades(&[unheld(2, ORDER, "not-an-order")]),
    ];
    for frame in &bad {
        let mut steps = reading_trades();
        let asked = steps.len() - 1;
        steps.push(Step::frame(frame));
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        assert_eq!(
            outs[asked],
            Out::effects(vec![trades_ask(1_760_000_003, 1000, 0)])
        );
        let out = outs.last().unwrap();
        assert!(
            out.asks_to_reconnect() && out.events.is_empty(),
            "{frame}: {out:?}"
        );
        assert_eq!(out.result, invalid, "{frame}");
        let counts = codec.counts();
        assert_eq!(
            (counts.resyncs, counts.resyncs_unfinished),
            (0, 1),
            "{frame}"
        );
        assert!(!codec.last_resync().unwrap().finished, "{frame}");
    }

    // Rows the table does not hold, each not as documented past its ids: skipped, counted and
    // kept as evidence, and the resync ends.
    let rows = [
        unheld(11, r#""price":200.0"#, r#""price":200.005"#),
        unheld(12, r#""price":200.0"#, r#""price":0"#),
        unheld(13, r#""quantity":1.0"#, r#""quantity":1.005"#),
        unheld(14, r#""fee":0.002"#, r#""fee":0.0000000001"#),
        unheld(15, "1760000004", "-1"),
        unheld(16, "1760000004", r#"1760000004,"client_order_id":7"#),
    ];
    let mut steps = reading_trades();
    steps.push(Step::Frame(trades(&rows)));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    let out = outs.last().unwrap();
    assert!(out.result.is_ok() && out.effects.is_empty(), "{out:?}");
    assert_eq!(untimed(out), bracket(9, vec![]));
    assert_eq!(codec.counts().unplaced_trades, 6);
    let evidence = codec.last_resync().unwrap();
    assert!(evidence.finished);
    let ids: Vec<fbc_core::FillId> = (11..=16).map(|n| fill_id(&tid(n))).collect();
    assert_eq!(evidence.unplaced_trades, ids);
}

#[test]
fn a_page_row_under_our_client_id_binds_its_venue_id_for_the_trades_read() {
    // Our place on epoch 1 whose answer never arrived; epoch 2's page lists it under our
    // client id, so a trade row naming only its venue id is a replayed fill of it, with our
    // side (decision 0132 item 5). Without the page row the same trade row is not ours.
    let cfg = preprod(SUB);
    let ours = quoted(&wire_cid(0));
    let listed = row(ORDER_ROW, &[("client_order_id", Some(ours.as_str()))]);
    for (listed, want_order) in [(Some(listed), true), (None, false)] {
        let mut steps = epoch(1);
        steps.push(Step::Encode(gtc_place(), 3));
        steps.extend(epoch(2));
        steps.push(Step::Resync(9));
        steps.push(Step::Http {
            nth: 0,
            status: 200,
            body: flat(),
        });
        let orders: Vec<String> = listed.into_iter().collect();
        steps.push(Step::Frame(page(&orders, &[])));
        steps.push(Step::Frame(trades(&[trade(1, ORDER, None)])));
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        let events = untimed(outs.last().unwrap());
        if want_order {
            let [
                ExecEvent::ResyncBegin { .. },
                ExecEvent::ResyncOrder(order),
                fill,
                ExecEvent::ResyncEnd,
            ] = events.as_slice()
            else {
                panic!("{events:?}");
            };
            assert_eq!(order.cid, Some(CidMatch::Ours(cids()[0])));
            assert_eq!(order.vid, venue_id(ORDER));
            assert_eq!(*fill, replayed_fill());
            assert_eq!(codec.counts().unplaced_trades, 0);
        } else {
            assert_eq!(events, bracket(9, vec![]));
            assert_eq!(codec.counts().unplaced_trades, 1);
        }
    }
}

/// Trade 1 of our order, by our first client id, as the resync pushes it.
fn replayed_fill_by_cid() -> ExecEvent {
    let ExecEvent::Fill(fill) = replayed_fill() else {
        unreachable!()
    };
    ExecEvent::Fill(FillEvent {
        origin: FillOrigin::Order {
            cid: Some(CidMatch::Ours(cids()[0])),
            px: Ticks(20_000),
        },
        ..fill
    })
}

/// A case of an order ended live during the resync: its name, what epoch 1 saw after the place,
/// the reads and the live end in their order, the trade row's client id, and whether the page
/// lists the order.
type Ended<'a> = (&'a str, Vec<Step>, Vec<Step>, Option<&'a str>, bool);

#[test]
fn an_order_ended_live_during_the_resync_still_has_its_missed_fill_replayed() {
    // Our order, placed on epoch 1, fills while the connection is down. On epoch 2 a live
    // order_response ends it while the resync's reads run: the trades read was asked from its
    // send time, so the fill its page lists is our replayed fill, with our side, keyed by its
    // trade_id, and not an unplaced trade (decision 0132 item 5).
    let cfg = preprod(SUB);
    let wire = wire_cid(0);
    let ours = quoted(&wire);
    let ends = [
        vec![("status", Some("\"CANCELLED\""))],
        vec![
            ("status", Some("\"FILLED\"")),
            ("quantity_remaining", Some("0")),
        ],
    ];
    for end in &ends {
        let ended = |cid: &str| {
            let mut changes = end.clone();
            changes.push(("client_order_id", Some(cid)));
            Step::Frame(order_frame(&changes))
        };
        let positions = Step::Http {
            nth: 0,
            status: 200,
            body: flat(),
        };
        let listed = row(ORDER_ROW, &[("client_order_id", Some(ours.as_str()))]);
        let cases: [Ended<'_>; 5] = [
            // Ended under our client id while the orders' pages run; the row names it.
            (
                "by client id",
                vec![],
                vec![ended(&ours), positions.clone(), Step::Frame(page(&[], &[]))],
                Some(wire.as_str()),
                false,
            ),
            // Its venue id bound by epoch 1's ack, ended by that id alone; the row names it.
            (
                "by the venue id its ack bound",
                vec![Step::Frame(our_ack())],
                vec![
                    ended("\"\""),
                    positions.clone(),
                    Step::Frame(page(&[], &[])),
                ],
                None,
                false,
            ),
            // Its venue id bound by the live end itself.
            (
                "by the venue id its end bound",
                vec![],
                vec![positions.clone(), ended(&ours), Step::Frame(page(&[], &[]))],
                None,
                false,
            ),
            // Its venue id bound only by the resync's page, ended by that id alone while the
            // trades read is in flight.
            (
                "by the venue id the page bound",
                vec![],
                vec![
                    positions.clone(),
                    Step::Frame(page(std::slice::from_ref(&listed), &[])),
                    ended("\"\""),
                ],
                None,
                true,
            ),
            // Ended under our client id while the trades read is in flight.
            (
                "by client id while trades are read",
                vec![],
                vec![positions.clone(), Step::Frame(page(&[], &[])), ended(&ours)],
                Some(wire.as_str()),
                false,
            ),
        ];
        for (case, first, reads, cid, listed) in cases {
            let mut steps = epoch(1);
            steps.push(Step::Encode(gtc_place(), 3));
            steps.extend(first);
            steps.extend(epoch(2));
            steps.push(Step::Resync(9));
            steps.extend(reads);
            steps.push(Step::Frame(trades(&[trade(1, ORDER, cid)])));
            let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
            let asked = outs
                .iter()
                .any(|out| out.effects == vec![trades_ask(1_760_000_003, 1000, 0)]);
            assert!(asked, "{case}: {end:?}");
            let fill = if cid.is_some() {
                replayed_fill_by_cid()
            } else {
                replayed_fill()
            };
            let events = untimed(outs.last().unwrap());
            if listed {
                let [
                    ExecEvent::ResyncBegin { .. },
                    ExecEvent::ResyncOrder(order),
                    pushed,
                    ExecEvent::ResyncEnd,
                ] = events.as_slice()
                else {
                    panic!("{case}: {end:?}: {events:?}");
                };
                assert_eq!(order.vid, venue_id(ORDER), "{case}");
                assert_eq!(*pushed, fill, "{case}: {end:?}");
            } else {
                assert_eq!(events, bracket(9, vec![fill]), "{case}: {end:?}");
            }
            assert_eq!(codec.counts().unplaced_trades, 0, "{case}: {end:?}");
            let evidence = codec.last_resync().unwrap();
            assert!(evidence.finished, "{case}");
            assert!(evidence.unplaced_trades.is_empty(), "{case}: {end:?}");
        }
    }
}

#[test]
fn an_order_ended_during_a_resync_is_forgotten_once_that_resync_ends() {
    // Our order ends live during epoch 2's resync, which replays its missed fill. A fill of
    // another order then gives epoch 3's resync a start time, and its trades page lists a
    // second trade of our order under our client id: no resync can need the order any more,
    // so the codec no longer holds it, and that row is an unplaced trade (decision 0132 item 5).
    let cfg = preprod(SUB);
    let ours = quoted(&wire_cid(0));
    let other = quoted(&oid(77));
    let mut steps = epoch(1);
    steps.push(Step::Encode(gtc_place(), 3));
    steps.extend(epoch(2));
    steps.push(Step::Resync(9));
    steps.push(Step::Frame(order_frame(&[
        ("client_order_id", Some(ours.as_str())),
        ("status", Some("\"CANCELLED\"")),
    ])));
    steps.push(Step::Http {
        nth: 0,
        status: 200,
        body: flat(),
    });
    steps.push(Step::Frame(page(&[], &[])));
    steps.push(Step::Frame(trades(&[trade(1, ORDER, Some(&wire_cid(0)))])));
    let first = steps.len() - 1;
    steps.push(Step::Frame(fill_frame(&[
        ("order_id", Some(other.as_str())),
        ("timestamp", Some("1760000005.5")),
    ])));
    steps.extend(epoch(3));
    steps.push(Step::Resync(20));
    steps.push(Step::Http {
        nth: 1,
        status: 200,
        body: flat(),
    });
    steps.push(Step::Frame(page(&[], &[])));
    let asked = steps.len() - 1;
    steps.push(Step::Frame(trades(&[trade(2, ORDER, Some(&wire_cid(0)))])));
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(
        untimed(&outs[first]),
        bracket(9, vec![replayed_fill_by_cid()])
    );
    // The latest fill alone gives the start time: our order is no longer one not seen to end.
    assert_eq!(
        outs[asked],
        Out::effects(vec![trades_ask(1_760_000_005, 1000, 0)])
    );
    assert_eq!(untimed(outs.last().unwrap()), bracket(20, vec![]));
    let counts = codec.counts();
    assert_eq!((counts.resyncs, counts.unplaced_trades), (2, 1));
    let evidence = codec.last_resync().unwrap();
    assert!(evidence.finished);
    assert_eq!(evidence.unplaced_trades, vec![fill_id(&tid(2))]);
}

#[test]
fn a_page_after_the_last_or_a_twap_naming_no_id_is_read_as_the_rules_say() {
    // The pages ended before the positions answered: a page frame then is not one the resync
    // asked for, so it is counted and changes nothing, and the answer ends the resync.
    let cfg = preprod(SUB);
    let mut steps = epoch(1);
    steps.push(Step::Resync(5));
    steps.push(Step::Frame(page(&[], &[])));
    steps.push(Step::Frame(page(&[order(1, &[])], &[])));
    steps.push(Step::Http {
        nth: 0,
        status: 200,
        body: flat(),
    });
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    let n = outs.len();
    assert_eq!(outs[n - 3], Out::quiet());
    assert_eq!(outs[n - 2], Out::quiet());
    assert_eq!(codec.counts().pages_unasked, 1);
    assert_eq!(untimed(&outs[n - 1]), bracket(5, vec![]));

    // At page size 1, a full page of one TWAP whose `twap_id` is absent or not a string names
    // no new id, so it asks to reconnect.
    for twap in [
        r#"{"client_twap_id":"","symbol":"AAPL-USD","status":"ACTIVE","side":"BUY"}"#,
        r#"{"twap_id":7,"client_twap_id":"","symbol":"AAPL-USD","status":"ACTIVE","side":"BUY"}"#,
    ] {
        let mut steps = resynced(&flat());
        steps.push(Step::Frame(page(&[], &[twap.to_owned()])));
        let (codec, outs, _) = run_twice(|| probe_on(&sized(preprod(SUB), 1)), &steps);
        assert!(reconnects(outs.last().unwrap()), "{twap}");
        assert!(outs.last().unwrap().result.is_ok(), "{twap}");
        let counts = codec.counts();
        assert_eq!(
            (counts.full_pages, counts.resyncs_unfinished),
            (1, 1),
            "{twap}"
        );
    }
}

#[test]
fn seeds_no_position() {
    let order_caps = probe_exec_caps().order;
    assert_eq!(order_caps.snapshot_source, SnapshotSource::Untrustworthy);
    assert_eq!(order_caps.snapshot_reads, SnapshotReads::Separate);
    let cfg = preprod(SUB);
    let mut steps = resynced(&positions(&[("AAPL-USD", "1.5")]));
    steps.push(Step::Frame(page(&[order(1, &[])], &[])));
    let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    let events = untimed(outs.last().unwrap());
    let ExecEvent::ResyncBegin { watermark } = events[0] else {
        panic!("{events:?}");
    };
    let mut snapshot = fbc_oms::ResyncSnapshot {
        watermark,
        requested_at: MonoNs(5),
        orders: Vec::new(),
        positions: Vec::new(),
    };
    for event in &events {
        match event {
            ExecEvent::ResyncOrder(o) => snapshot.orders.push(o.clone()),
            ExecEvent::ResyncPosition { inst, qty, .. } => snapshot.positions.push((*inst, *qty)),
            _ => {}
        }
    }
    assert_eq!(snapshot.positions, vec![(AAPL, SignedLots(150))]);
    let mut registry = fbc_oms::Registry::new();
    let ladder = fbc_oms::LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap();
    let key = fbc_oms::OrderKey {
        venue: None,
        ingest: 1,
    };
    let report = registry
        .resync(&ladder, &order_caps, &snapshot, key)
        .unwrap();
    assert!(report.untrustworthy);
    assert_eq!(registry.position(AAPL), None);
    assert!(!registry.position_known(AAPL));
}

// ----- The journal -----

#[test]
fn replaying_the_redacted_journal_gives_the_same_resync_events_and_effects() {
    // One long script under each mode: a resync to its end, one ended by its deadline, one by a
    // refused read, each epoch armed; `run_twice` holds every step equal live and replayed.
    for cfg in [
        sized(preprod(SUB), 1),
        sized(production_loopback("primary"), 1),
    ] {
        let mut steps = armed(1, 1);
        steps.push(Step::Resync(5));
        steps.push(Step::frame(&ack(ARM)));
        steps.push(Step::Frame(page(&[order(1, &[])], &[])));
        steps.push(Step::Frame(page(&[], &[twap(1)])));
        steps.push(Step::Http {
            nth: 0,
            status: 200,
            body: flat(),
        });
        steps.push(Step::Frame(page(&[], &[])));
        steps.extend(armed(2, 2));
        steps.push(Step::Resync(7));
        steps.push(Step::Timer { nth: 3, at: 50 });
        steps.extend(armed(3, 3));
        steps.push(Step::Resync(8));
        steps.push(Step::Http {
            nth: 2,
            status: 403,
            body: r#"{"message":"forbidden-synthetic-detail"}"#.to_owned(),
        });
        let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
        assert_eq!(untimed(&outs[7]), vec![accepted(1)]);
        assert_eq!(
            untimed(&outs[11]),
            bracket(5, vec![ExecEvent::ResyncOrder(shown(1))])
        );
        let counts = codec.counts();
        assert_eq!(
            (counts.resyncs, counts.resyncs_unfinished, counts.full_pages),
            (1, 2, 2)
        );
        for secret in [
            PUBLIC,
            SUB,
            SECRET,
            BALANCE,
            "forbidden-synthetic-detail",
            USER,
        ] {
            assert!(!disk.has(secret), "{secret} on disk");
        }
        assert_eq!(disk.requests.len(), 3);
        assert!(disk.bodies.iter().all(Option::is_some));
    }
}

// ----- A session against the stub, and its journal replayed -----

/// One connection attempt a day: a session that reconnects attempts no second.
const DAY: Duration = Duration::from_secs(86_400);

/// The venue, for a session's `&'static dyn VenueFactory`.
static QFEX: fbc_venue_qfex::Qfex = fbc_venue_qfex::Qfex;

/// The journal's shard.
const SHARD: u16 = 0;

/// A key for the journal's keyed hashes.
fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[17; 32]).unwrap())
}

/// Nonces counted up from `0`: every nonce a session reserves is new, as QFEX asks.
struct Counting(u64);

impl NonceSource for Counting {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let block = NonceBlock::consecutive(self.0, len).unwrap();
        self.0 += u64::from(len);
        block
    }
}

/// One thing a handler was told, in order.
#[derive(Clone, Debug, PartialEq)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    Ended(ConnKey),
}

/// When a handler drops its control: once the arm has its outcome and a resync has ended, or at
/// the `n`th epoch's end.
#[derive(Clone, Copy)]
enum StopAt {
    ArmAndResync,
    Epochs(usize),
}

/// A handler keeping what it is told, which drops its control, live or in replay, where
/// [`StopAt`] says.
#[derive(Clone)]
struct Log {
    heard: Rc<RefCell<Vec<Heard>>>,
    control: Rc<RefCell<Option<ExecControl>>>,
    stop: StopAt,
}

impl Log {
    fn new(stop: StopAt) -> Log {
        Log {
            heard: Rc::default(),
            control: Rc::default(),
            stop,
        }
    }

    fn heard(&self) -> Vec<Heard> {
        self.heard.borrow().clone()
    }

    /// Every event it was handed.
    fn events(&self) -> Vec<ExecEvent> {
        let heard = self.heard.borrow();
        let events = heard.iter().filter_map(|h| match h {
            Heard::Event(env) => Some(env.body.clone()),
            Heard::Ended(_) => None,
        });
        events.collect()
    }

    fn check(&self) {
        let heard = self.heard.borrow();
        let done = match self.stop {
            StopAt::ArmAndResync => {
                let event = |pick: &dyn Fn(&ExecEvent) -> bool| {
                    heard
                        .iter()
                        .any(|h| matches!(h, Heard::Event(env) if pick(&env.body)))
                };
                event(&|e| matches!(e, ExecEvent::Outcome { .. }))
                    && event(&|e| *e == ExecEvent::ResyncEnd)
            }
            StopAt::Epochs(n) => {
                heard
                    .iter()
                    .filter(|h| matches!(h, Heard::Ended(_)))
                    .count()
                    == n
            }
        };
        if done {
            drop(self.control.borrow_mut().take());
        }
    }
}

impl ExecHandler for Log {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.heard.borrow_mut().push(Heard::Event(Box::new(env)));
        self.check();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.heard.borrow_mut().push(Heard::Ended(key));
        self.check();
    }
}

/// A responder sending `frames` when it reads `want`, and refusing anything else.
fn answer(want: &'static str, frames: &[&str]) -> Responder {
    let frames: Vec<Frame> = frames.iter().map(|f| Frame::text(*f)).collect();
    Responder::new(move |frame| match frame {
        Frame::Text(text) if text == want => Ok(frames.clone()),
        other => Err(format!("not {want}: {other:?}")),
    })
}

/// A responder confirming the subscription it read, as the AsyncAPI's `subscribed` reply does.
fn confirming() -> Responder {
    Responder::new(|frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary subscribe".to_owned());
        };
        let channel = CHANNELS
            .iter()
            .find(|c| text.contains(&format!(r#"["{c}"]"#)))
            .ok_or("no channel")?;
        Ok(vec![Frame::text(confirm(channel))])
    })
}

/// A responder answering the `get_user_orders` page it read from `account`, at the frame's own
/// `limit` and `offset`, as QFEX's page says it pages.
fn paging(account: Vec<Item>) -> Responder {
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary page".to_owned());
        };
        let read = |member: &str| -> Option<usize> {
            let at = text.find(&format!(r#""{member}":"#))? + member.len() + 3;
            let digits: String = text[at..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        };
        if !text.starts_with(r#"{"type":"get_user_orders""#) {
            return Err(format!("not a page: {text}"));
        }
        let (limit, offset) = read("limit")
            .zip(read("offset"))
            .ok_or("no limit or offset")?;
        Ok(vec![Frame::text(slice(&account, limit, offset))])
    })
}

/// Connection `conn`'s opening: accepted, authenticated, its three subscriptions confirmed, and
/// its arm answered by its ack.
fn opened(conn: usize) -> Vec<WsStep> {
    let mut steps = vec![
        WsStep::Accept,
        WsStep::Respond {
            conn,
            with: Responder::new(|_| Ok(vec![Frame::text(AUTHENTICATED)])),
        },
    ];
    for _ in CHANNELS {
        steps.push(WsStep::Respond {
            conn,
            with: confirming(),
        });
    }
    steps.push(WsStep::Respond {
        conn,
        with: answer(ARM, &[&ack(ARM)]),
    });
    steps
}

/// Each positions read's public key and subaccount header, as the stub received them.
type Reads = Arc<Mutex<Vec<(String, Option<String>)>>>;

/// The stub's positions route: `body` for every read, each read's public key and subaccount
/// header kept in `seen`.
fn positions_route(body: String, seen: Reads) -> HttpRouter {
    HttpRouter::new().route_fn(
        Method::GET,
        PathPattern::exact("/user/positions"),
        move |req| {
            let public = req.header("x-qfex-public-key").unwrap_or("").to_owned();
            let sub = req.header("x-qfex-requested-account-id").map(str::to_owned);
            seen.lock().unwrap().push((public, sub));
            HttpReply {
                status: 200,
                body: body.clone().into_bytes(),
            }
        },
    )
}

/// What a session against the stub left: what its handler heard, its journal's directory, and
/// what each positions read sent.
struct Ran {
    heard: Vec<Heard>,
    events: Vec<ExecEvent>,
    journal: PathBuf,
    reads: Vec<(String, Option<String>)>,
    connections: usize,
}

/// A fresh directory of its own for a journal.
fn journal_dir(what: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("fbc-qfex-jxau-stub-{what}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The configuration of a probe session for `account` against `server`, its page size `size`
/// and its resync deadline `resync`.
fn against(server: &StubServer, account: &str, size: u16, resync: &str) -> VenueConfig {
    let ws = server.ws_url("");
    let cfg = on(account, &ws, &server.http_url(""), &ws);
    with(sized(cfg, size), KEY_RESYNC_TIMEOUT, resync)
}

/// Runs one probe session under `cfg` against `server`, journaled at `journal`, its provider
/// user `user`, stopped where `stop` says, at most `pacing`'s attempts.
async fn session(
    cfg: &VenueConfig,
    server: &StubServer,
    journal: &Path,
    user: u64,
    stop: StopAt,
    pacing: ReconnectPacing,
    seen: Reads,
) -> Ran {
    let (queue, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 16 << 20,
            soft_limit_pct: 90,
        },
        redaction_key(),
    )
    .unwrap();
    let writer = JournalWriter::create(journal, SHARD, redaction_key()).unwrap();
    let writer = drain.spawn(writer).unwrap();
    let limits = QFEX.caps(cfg).unwrap().exec_limits.unwrap();
    let reserve = SafetyReserve::percent(10).unwrap();
    let limiter = RateLimiter::for_user(RateUserKey::new(user), &limits, reserve).unwrap();
    let acct = QFEX.exec_account_key(cfg).unwrap().unwrap();
    let bound = ReservedBound::new(4, 4).unwrap();
    let config = ExecSessionConfig {
        venue: &QFEX,
        cfg: cfg.clone(),
        creds: secrets(),
        acct,
        account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
        rpc_ids: RpcIds::default(),
        ns: NS,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing,
        clock: IngestClock::new(),
        nonces: Box::new(Counting(1_000)),
        nonce_source: NonceSourceId(0),
        conn: 0,
        limiter,
        write_stall: WriteStall::new(Duration::from_secs(10)).unwrap(),
        http_max_body: 1 << 20,
        outbound: bound,
        in_flight: InFlight::new(bound),
    };
    let log = Log::new(stop);
    let (mut live, control) = ExecSession::new(config, log.clone()).unwrap();
    *log.control.borrow_mut() = Some(control);
    live.set_journal(Journal::new(Rc::new(RefCell::new(queue))));
    let ran = tokio::time::timeout(Duration::from_secs(30), live.run()).await;
    ran.expect("the session stopped").unwrap();
    drop(live);
    writer.close().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let reads = seen.lock().unwrap().clone();
    Ran {
        heard: log.heard(),
        events: log.events(),
        journal: journal.to_owned(),
        reads,
        connections: server.connections().len(),
    }
}

/// Replays `ran`'s journal under `cfg`, the handler dropping its control where the live one
/// did: what it heard, and whether every frame and request the codec asked for is the
/// journal's, outside its spans, signatures included.
fn replayed(cfg: &VenueConfig, ran: &Ran, stop: StopAt) -> (Vec<Heard>, bool) {
    let log = Log::new(stop);
    let config = ExecReplayConfig {
        venue: &QFEX,
        cfg: cfg.clone(),
        creds: secrets(),
        ns: NS,
        specs: specs(),
        conn: 0,
        compare_outbound: true,
    };
    let (mut replay, control) = ExecReplay::new(config, log.clone()).unwrap();
    *log.control.borrow_mut() = Some(control);
    replay
        .run(JournalReader::open(&ran.journal, SHARD).unwrap())
        .unwrap();
    let report = replay.exact().unwrap();
    assert_eq!(replay.counters().decode_errors, 0);
    drop(replay);
    (log.heard(), report.is_exact())
}

/// Every byte of every file under `dir`.
fn on_disk(dir: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            bytes.extend(on_disk(&path));
        } else {
            bytes.extend(std::fs::read(&path).unwrap());
        }
    }
    bytes
}

fn holds(bytes: &[u8], needle: &str) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[tokio::test]
async fn a_session_against_the_stub_pages_at_sizes_1_and_1000_and_its_journal_replays_to_the_same_events()
 {
    // At size 1, three orders and a TWAP, a page each, then a short one; at size 1000, 1002
    // rows across the boundary, order 1000 shown at the first page's end and again at the
    // second's start, as a set that shifts between reads shows it.
    let small = vec![
        Item::Order(order(1, &[])),
        Item::Twap(twap(1)),
        Item::Order(order(2, &[])),
        Item::Order(order(3, &[])),
    ];
    let mut large: Vec<Item> = (1..=1000).map(|n| Item::Order(order(n, &[]))).collect();
    large.insert(1000, Item::Order(order(1000, &[])));
    large.push(Item::Order(order(1001, &[])));
    let cases = [
        (1, small, SUB, 0x7a1_0001, vec![1, 2, 3], 5),
        (1000, large, "primary", 0x7a1_0002, (1..=1001).collect(), 2),
    ];
    for (size, account, who, user, want, asked) in cases {
        let mut steps = opened(0);
        for _ in 0..asked {
            steps.push(WsStep::Respond {
                conn: 0,
                with: paging(account.clone()),
            });
        }
        let seen = Arc::default();
        let router = positions_route(flat(), Arc::clone(&seen));
        let server = StubServer::start(WsScript::new(steps), router)
            .await
            .unwrap();
        let cfg = against(&server, who, size, "30s");
        let one = ReconnectPacing::new(DAY, DAY, 1, DAY, Duration::from_secs(5)).unwrap();
        let journal = journal_dir(&format!("pages-{size}"));
        let ran = session(
            &cfg,
            &server,
            &journal,
            user,
            StopAt::ArmAndResync,
            one,
            seen,
        )
        .await;
        server.finished().await.unwrap();

        // The arm accepted by its ack; one bracket, each order once, in the venue's order,
        // TWAPs left out, the flat positions pushing none.
        assert_eq!(ran.connections, 1);
        let outcomes: Vec<&ExecEvent> = (ran.events.iter())
            .filter(|e| matches!(e, ExecEvent::Outcome { .. }))
            .collect();
        assert!(
            matches!(
                outcomes.as_slice(),
                [ExecEvent::Outcome {
                    outcome: fbc_core::SubmitOutcome::Accepted {
                        ack: fbc_core::AckLevel::Final
                    },
                    item: None,
                    ..
                }]
            ),
            "{outcomes:?}"
        );
        let begin = ran
            .events
            .iter()
            .position(|e| matches!(e, ExecEvent::ResyncBegin { .. }));
        let begin = begin.unwrap();
        let end = ran
            .events
            .iter()
            .position(|e| *e == ExecEvent::ResyncEnd)
            .unwrap();
        let orders: Vec<ExecEvent> = want
            .iter()
            .map(|n| ExecEvent::ResyncOrder(shown(*n)))
            .collect();
        assert_eq!(ran.events[begin + 1..end], orders[..], "size {size}");
        assert_eq!(
            ran.events
                .iter()
                .filter(|e| matches!(e, ExecEvent::ResyncBegin { .. }))
                .count(),
            1
        );
        // Every page the codec asked: `asked` of them, at offsets one page apart.
        let conns = server.connections();
        let pages: Vec<String> = (conns[0].received.iter())
            .filter_map(|f| match f {
                Frame::Text(t) if t.contains("get_user_orders") => Some(t.clone()),
                _ => None,
            })
            .collect();
        let offsets: Vec<String> = (0..asked)
            .map(|k| {
                format!(
                    r#"{{"type":"get_user_orders","params":{{"limit":{size},"offset":{}}}}}"#,
                    k * size
                )
            })
            .collect();
        assert_eq!(pages, offsets);
        // One positions read, its public key on the wire, and the subaccount's header only for
        // a subaccount; the journal holds neither, nor the secret nor a balance value.
        let sub = (who == SUB).then(|| SUB.to_owned());
        assert_eq!(ran.reads, vec![(PUBLIC.to_owned(), sub)]);
        let bytes = on_disk(&journal);
        for absent in [PUBLIC, SUB, SECRET, BALANCE] {
            assert!(!holds(&bytes, absent), "{absent} in the journal");
        }
        let requests = (JournalReader::open(&journal, SHARD).unwrap())
            .filter(|r| matches!(r, Ok(Record::HttpRequest { .. })))
            .count();
        assert_eq!(requests, 1);

        // Replayed, the handler hears what it heard live, stamps included, and every frame and
        // request the codec asks for is the journal's.
        let (heard, exact) = replayed(&cfg, &ran, StopAt::ArmAndResync);
        assert_eq!(heard, ran.heard, "size {size}");
        assert!(exact, "size {size}");
        let _ = std::fs::remove_dir_all(&journal);
    }
}

#[tokio::test]
async fn a_session_against_the_stub_reconnects_on_a_full_page_with_no_new_id_and_on_its_resync_deadline()
 {
    // Epoch 1 at size 1: the same order on two full pages, so its codec asks to reconnect.
    // Epoch 2: its page never answered, so its resync deadline ends it.
    let repeated = vec![Item::Order(order(1, &[]))];
    let mut steps = opened(0);
    for _ in 0..2 {
        steps.push(WsStep::Respond {
            conn: 0,
            with: Responder::new({
                let account = repeated.clone();
                move |_| Ok(vec![Frame::text(slice(&account, 1, 0))])
            }),
        });
    }
    steps.extend(opened(1));
    steps.push(WsStep::Read { conn: 1 });
    let seen = Arc::default();
    let router = positions_route(flat(), Arc::clone(&seen));
    let server = StubServer::start(WsScript::new(steps), router)
        .await
        .unwrap();
    let cfg = against(&server, "primary", 1, "1s");
    let paced = ReconnectPacing::new(
        Duration::from_millis(10),
        Duration::from_millis(100),
        100,
        Duration::from_secs(60),
        Duration::from_secs(5),
    )
    .unwrap();
    let journal = journal_dir("reconnects");
    let ran = session(
        &cfg,
        &server,
        &journal,
        0x7a1_0003,
        StopAt::Epochs(2),
        paced,
        seen,
    )
    .await;
    server.finished().await.unwrap();

    // Two connections, each armed and accepted, each resync begun and none ended, each read
    // its positions; the session stopped at the second epoch's end.
    assert_eq!(ran.connections, 2);
    let count = |pick: &dyn Fn(&ExecEvent) -> bool| ran.events.iter().filter(|e| pick(e)).count();
    assert_eq!(count(&|e| matches!(e, ExecEvent::Outcome { .. })), 2);
    assert_eq!(count(&|e| *e == ExecEvent::ResyncEnd), 0);
    assert_eq!(count(&|e| matches!(e, ExecEvent::ResyncOrder(_))), 0);
    let ended = (ran.heard.iter())
        .filter(|h| matches!(h, Heard::Ended(_)))
        .count();
    assert_eq!(ended, 2);
    assert_eq!(ran.reads.len(), 2);
    // Epoch 1 asked exactly two pages, offsets 0 and 1, and asked no third: the full page that
    // added no new id ended it, not its resync deadline. Epoch 2 asked one.
    let conns = server.connections();
    let asked = |conn: usize| -> Vec<String> {
        (conns[conn].received.iter())
            .filter_map(|f| match f {
                Frame::Text(t) if t.contains("get_user_orders") => Some(t.clone()),
                _ => None,
            })
            .collect()
    };
    let at = |offset: u32| {
        format!(r#"{{"type":"get_user_orders","params":{{"limit":1,"offset":{offset}}}}}"#)
    };
    assert_eq!(asked(0), vec![at(0), at(1)]);
    assert_eq!(asked(1), vec![at(0)]);

    let (heard, exact) = replayed(&cfg, &ran, StopAt::Epochs(2));
    assert_eq!(heard, ran.heard);
    assert!(exact);
    let _ = std::fs::remove_dir_all(&journal);
}
