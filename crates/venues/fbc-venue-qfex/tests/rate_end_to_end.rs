//! FBC-p7rn, decision 0135 item 2's carrier: QFEX's rate charges and `RateLimited` replies end to
//! end, offline, with no provider connection and no limit provoked (0135 item 1).
//!
//! - **Charges.** Every frame the codec writes under `preprod_probe`, a follow-on page of each
//!   resync read included, and the signed positions read carry their weight in tenths of a
//!   weight unit (decision 0128): the weight
//!   [QFEX's rate page](https://docs.qfex.com/websocket/rate) gives the message type or, where
//!   the page gives none (authentication) or does not cover it (the REST positions read), the
//!   codec's own, each as `WEIGHTS` cites it. Each is charged under the operation that puts it in
//!   the bucket `src/trade_rate.rs` maps it to: `cancel_order` the cancel bucket alone,
//!   `cancel_on_disconnect` (`Control`, since the page names no bucket for it) both, the
//!   positions read (`Rest`) neither (decision 0132 item 5). Each is charged through
//!   fbc-runtime's own limiter, so the bucket it lands in is the runtime's matching, not the
//!   test's reading of the declaration.
//! - **Faults end to end.** fbc-runtime's `ExecSession`, unchanged, runs the codec under
//!   `preprod_probe` for a primary and a subaccount selection against the conformance kit's stub
//!   on 127.0.0.1, on a paused clock only the test moves. Each session has its own
//!   `RateLimiter::for_user` built from `caps.exec_limits` under one `RateUserKey` (decisions 0123
//!   and 0128), beside a limiter of another user's key, which admits a place and a cancel while
//!   every frame budget of the first user is held. The stub refuses with each shape 0135 item 2
//!   names; after each, the other account's next frame of each bucket the refusal holds
//!   (0126, 0134 item 5) is `NotSent(RateBudget)` with nothing written until the hint, or the
//!   declared window where none is kept, has passed, while a frame of a bucket it does not hold
//!   goes out. An err echoing authentication is kept whole, holds nothing and ends its epoch
//!   (0134 items 2 and 6). No other refusal settles, resends or reconnects, and each refused place
//!   ends `Unknown` at its deadline, to the instant (0057, 0132 item 8), and is never resent
//!   (0005).
//! - **Decoder replay.** Each session's journal, replayed through `ExecReplay`, hands its handler
//!   the same events at the same stamps, gives the codec the same calls with the same events,
//!   effects and counts after each (a recording wrapper around the codec, live and in replay),
//!   and asks for every frame and request the journal holds, byte for byte outside its spans;
//!   what it asks for that the journal holds none of is exactly the commands the buckets refused.
//!
//! The order commands are built and authorized by fbc-oms (decision 0045). Its registry is
//! seeded flat by a snapshot of the test's own making, as the conformance suite seeds its own
//! (decision 0087): QFEX's resync is untrustworthy and seeds nothing (0132 item 5), and what is
//! under test here is the rate budget, not the seed.
//!
//! Every key, account, id and frame here is synthetic, built from the shapes QFEX's pages show.

#[allow(dead_code)]
mod probe_harness;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step as WsStep, StubServer, WsScript,
};
use fbc_core::{
    AccountKey, AccountLease, AccountSummary, AssetKey, CancelScope, Channel, CidMint, ConfigError,
    ConnKey, CtxCall, DecodeError, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    EndpointPlan, Envelope, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, FieldSpec, HttpFailure,
    HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentSpecDraft, LimitScope, Lots,
    MarketLease, MdCodec, MonoNs, NamespaceLease, NewOrder, NonceBlock, NonceSource, NotSentReason,
    OpKind, OrderCaps, OrderKind, OrderRef, PathStamps, QueryOrder, RateCharge, RawFrame, RpcId,
    Secrets, Side, SignedLots, SnapshotSource, SpecTable, StreamId, SubmitHandle, SubmitOutcome,
    Subscription, SymbolError, Ticks, Tif, TimerTag, TrafficClass, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, VenueMeta, VenueOrderId, VenueSymbol, Via, WallNs,
};
use fbc_journal::{
    JournalReader, JournalWriter, NonceSourceId, Record, RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::{
    CancelChoice, ControlCommand, LadderConfig, LeaseKeys, Leases, MarketCapsConfig, OrderKey,
    PreTradeCaps, Registry, ResyncSnapshot,
};
use fbc_runtime::http::Method;
use fbc_runtime::{
    AccountWideCodAuthority, BucketKey, Connector, ExecControl, ExecCounters, ExecHandler,
    ExecOrders, ExecReplay, ExecReplayConfig, ExecSession, ExecSessionConfig, InFlight,
    IngestClock, Journal, ProxyConfig, RateLimiter, RateUserKey, ReconnectPacing, Request,
    ReservedBound, RpcIds, SafetyReserve, ScopeCounts, WriteStall,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::auth::TradeAuth;
use fbc_venue_qfex::exec::{ARM, CHANNELS, QfexExec, QfexExecCounts, TRADE};
use fbc_venue_qfex::probe::{KEY_PAGE_SIZE, KEY_RPC_TIMEOUT, PREPROD_PROBE};
use serde_json::Value;
use tokio::time::{Instant, advance};

use probe_harness::*;

// ----- The weights -----

/// The rate page's 12,000 weight units per 60 seconds in each bucket, in tenths (decision 0128).
const BUCKET: u32 = 120_000;
/// The general bucket's place in `caps.exec_limits`, and the cancel bucket's.
const GENERAL: usize = 0;
const CANCEL: usize = 1;

/// One kind of frame or request the codec writes: what it is, the operation and weight in
/// tenths it is charged, whether it names its instrument, the buckets it lands in, and where the
/// weight comes from.
struct Weighed {
    kind: &'static str,
    op: OpKind,
    tenths: u32,
    names_instrument: bool,
    general: bool,
    cancel: bool,
    cite: &'static str,
}

/// Every kind the codec writes under `preprod_probe`, as the rate page weighs it.
const WEIGHTS: [Weighed; 9] = [
    Weighed {
        kind: "auth",
        op: OpKind::Control,
        tenths: 1,
        names_instrument: false,
        general: true,
        cancel: true,
        // Not the page's: the page lists no weight for authentication and says its limits
        // apply to messages after it. The codec's conservative weight is the least it can
        // charge, 0.1, under Control, which charges both buckets (src/exec/mod.rs).
        cite: "the codec's own 0.1 under Control: the rate page lists no weight for auth",
    },
    Weighed {
        kind: "subscribe",
        op: OpKind::Subscribe,
        tenths: 1,
        names_instrument: false,
        general: true,
        cancel: false,
        cite: "rate page: subscribe 0.1",
    },
    Weighed {
        kind: "add_order",
        op: OpKind::Place,
        tenths: 10,
        names_instrument: true,
        general: true,
        cancel: false,
        cite: "rate page: add_order 1.0",
    },
    Weighed {
        kind: "cancel_order",
        op: OpKind::Cancel,
        tenths: 10,
        names_instrument: true,
        general: false,
        cancel: true,
        cite: "rate page: cancel_order 1.0, the cancel bucket's",
    },
    Weighed {
        kind: "get_order",
        op: OpKind::Query,
        tenths: 20,
        names_instrument: true,
        general: true,
        cancel: false,
        cite: "rate page: get_order 2.0",
    },
    Weighed {
        kind: "cancel_on_disconnect",
        op: OpKind::Control,
        tenths: 1,
        names_instrument: false,
        general: true,
        cancel: true,
        cite: "rate page: cancel_on_disconnect 0.1; the page names no bucket for it, so Control \
               charges both (src/trade_rate.rs)",
    },
    Weighed {
        kind: "get_user_orders",
        op: OpKind::Query,
        tenths: 50,
        names_instrument: false,
        general: true,
        cancel: false,
        cite: "rate page: get_user_orders 5.0 (decision 0132, Context)",
    },
    Weighed {
        kind: "get_user_trades",
        op: OpKind::Query,
        tenths: 5,
        names_instrument: false,
        general: true,
        cancel: false,
        cite: "rate page: get_user_trades 0.5",
    },
    Weighed {
        kind: "GET /user/positions",
        op: OpKind::Rest,
        tenths: 1,
        names_instrument: false,
        general: false,
        cancel: false,
        // QFEX documents no REST limit, and its two buckets count only authenticated
        // WebSocket messages (decisions 0128, 0132 item 5).
        cite: "decision 0132 item 5: OpKind::Rest, which no declared bucket counts",
    },
];

fn weighed(kind: &str) -> &'static Weighed {
    WEIGHTS
        .iter()
        .find(|w| w.kind == kind)
        .unwrap_or_else(|| panic!("no weight for {kind}"))
}

/// The kind a frame or request is: the positions read by its URL, a frame by its `type`.
fn kind_of(effect: &Effect) -> &'static str {
    match effect {
        Effect::Http { req, .. } => {
            assert!(req.url.as_str().ends_with("/user/positions"), "{req:?}");
            "GET /user/positions"
        }
        Effect::Send { frame, .. } => {
            let v: Value = serde_json::from_slice(frame.bytes()).unwrap();
            let kind = v["type"].as_str().unwrap().to_owned();
            WEIGHTS
                .iter()
                .find(|w| w.kind == kind)
                .unwrap_or_else(|| panic!("an unweighed frame {kind}"))
                .kind
        }
        other => panic!("not a request: {other:?}"),
    }
}

fn alo() -> NewOrder {
    NewOrder {
        cid: cids()[1],
        post_only: true,
        ..gtc()
    }
}

fn ioc() -> NewOrder {
    NewOrder {
        cid: cids()[2],
        tif: Tif::Ioc,
        ..gtc()
    }
}

/// A `get_user_orders` page listing nothing: short at any page size.
const EMPTY_PAGE: &str = r#"{"all_orders_response":{"orders":[],"twaps":[]}}"#;
/// The AsyncAPI's trades page, listing nothing.
const NO_TRADES: &str = r#"{"user_trades_response":[]}"#;

/// A made-up order no placement of the codec's names, which a page lists.
const LISTED: &str = "00000009-0000-4000-8000-000000000009";

/// A `get_user_orders` page listing one open order, [`LISTED`], under no client id: full at
/// page size 1.
fn one_order_page() -> String {
    let id = quoted(LISTED);
    let order = row(
        ORDER_ROW,
        &[("order_id", Some(&id)), ("client_order_id", Some("\"\""))],
    );
    format!(r#"{{"all_orders_response":{{"orders":[{order}],"twaps":[]}}}}"#)
}

/// The AsyncAPI's trades page listing one trade of [`LISTED`]: full at page size 1.
fn one_trade_page() -> String {
    let trade = [
        format!(r#""trade_id":"{TRADE_ID}","symbol":"AAPL-USD","price":200.0,"quantity":1.0"#),
        format!(r#""aggressor_side":"BUY","order_id":"{LISTED}","fee":0.002"#),
        r#""timestamp":1760000004"#.to_owned(),
    ];
    format!(r#"{{"user_trades_response":[{{{}}}]}}"#, trade.join(","))
}
/// The account's positions: flat, holding none.
const FLAT: &str = r#"{"balance":{},"positions":[]}"#;

#[tokio::test(start_paused = true)]
async fn every_frame_the_codec_writes_carries_its_documented_weight_in_the_bucket_trade_rate_maps()
{
    let caps = caps();
    let limits = caps
        .exec_limits
        .clone()
        .expect("the probe's private buckets");
    // The two user-scoped buckets, general and cancel, each 12,000 units per 60 s in tenths.
    assert_eq!(limits.len(), 2);
    for limit in &limits {
        assert_eq!(limit.scope, LimitScope::User);
        assert_eq!((limit.units, limit.per), (BUCKET, Duration::from_secs(60)));
    }
    assert!(limits[GENERAL].ops.contains(OpKind::Place));
    assert!(limits[CANCEL].ops.contains(OpKind::Cancel));

    // Every frame the codec writes, and its positions read: the authentication on opening, the
    // three subscriptions on the auth reply, the arm, a GTC, an ALO and an IOC place, a cancel and
    // a query by venue id, then a resync's positions read, its orders pages and, since the codec
    // placed orders it has not seen end, its trades pages. At page size 1 the first page of each
    // read is full, so each asks a second, at offset 1. An instrument cancel-all, which the rate
    // page weighs 2.0, is refused unsent: the codec writes no `cancel_all_orders`.
    let mut steps = epoch(1);
    steps.push(Step::Encode(ARM_ON, 2));
    steps.push(Step::Encode(VenueCommand::Place(gtc()), 3));
    steps.push(Step::Encode(VenueCommand::Place(alo()), 4));
    steps.push(Step::Encode(VenueCommand::Place(ioc()), 5));
    steps.push(Step::Encode(cancel(ORDER), 6));
    steps.push(Step::Encode(query(ORDER), 7));
    let cancel_all = VenueCommand::CancelAll(CancelScope::Instrument(AAPL));
    steps.push(Step::Encode(cancel_all, 8));
    // The query's deadline, so no query is in flight when the resync is asked (0132 item 5).
    steps.push(Step::Deadline(7));
    steps.push(Step::Resync(9));
    steps.push(Step::Http {
        nth: 0,
        status: 200,
        body: FLAT.to_owned(),
    });
    steps.push(Step::frame(&one_order_page()));
    steps.push(Step::frame(EMPTY_PAGE));
    steps.push(Step::frame(&one_trade_page()));
    steps.push(Step::frame(NO_TRADES));
    let cfg = with(loopback("primary"), KEY_PAGE_SIZE, "1");
    let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    assert_eq!(outs[11], refused(NotSentReason::Unsupported));
    let resynced = outs.last().unwrap().events.last().unwrap();
    assert_eq!(*resynced, (VenueMeta::NONE, ExecEvent::ResyncEnd));

    let written: Vec<&Effect> = (outs.iter())
        .flat_map(|out| &out.effects)
        .filter(|e| e.charge().is_some())
        .collect();
    let kinds: Vec<&str> = written.iter().map(|e| kind_of(e)).collect();
    assert_eq!(
        kinds,
        [
            "auth",
            "subscribe",
            "subscribe",
            "subscribe",
            "cancel_on_disconnect",
            "add_order",
            "add_order",
            "add_order",
            "cancel_order",
            "get_order",
            "GET /user/positions",
            "get_user_orders",
            "get_user_orders",
            "get_user_trades",
            "get_user_trades",
        ]
    );
    // Each read's pages, at offsets 0 and 1 of page size 1.
    let pages: Vec<(&str, String, String)> = (written.iter())
        .filter(|e| matches!(kind_of(e), "get_user_orders" | "get_user_trades"))
        .map(|e| {
            let Effect::Send { frame, .. } = e else {
                unreachable!()
            };
            let v: Value = serde_json::from_slice(frame.bytes()).unwrap();
            let p = &v["params"];
            (kind_of(e), p["limit"].to_string(), p["offset"].to_string())
        })
        .collect();
    let page = |kind, offset: &str| (kind, "1".to_owned(), offset.to_owned());
    assert_eq!(
        pages,
        [
            page("get_user_orders", "0"),
            page("get_user_orders", "1"),
            page("get_user_trades", "0"),
            page("get_user_trades", "1"),
        ]
    );
    // The three places are the three shapes v1 sends.
    let shapes: Vec<(String, String)> = (written.iter())
        .filter(|e| kind_of(e) == "add_order")
        .map(|e| {
            let Effect::Send { frame, .. } = e else {
                unreachable!()
            };
            let v: Value = serde_json::from_slice(frame.bytes()).unwrap();
            let p = &v["params"];
            (
                p["order_type"].to_string(),
                p["order_time_in_force"].to_string(),
            )
        })
        .collect();
    let shape = |t: &str, tif: &str| (format!("\"{t}\""), format!("\"{tif}\""));
    assert_eq!(
        shapes,
        [
            shape("LIMIT", "GTC"),
            shape("ALO", "GTC"),
            shape("LIMIT", "IOC")
        ]
    );

    // Each is charged its weight, and lands in exactly the buckets its kind names, charged
    // through the runtime's own limiter under a provider-user key of its own.
    let limiter = RateLimiter::for_user(
        RateUserKey::new(0x97a0_0001),
        &limits,
        SafetyReserve::percent(10).unwrap(),
    )
    .unwrap();
    let conn = ConnKey { conn: 0, epoch: 1 };
    let used = |limit| limiter.used(Instant::now(), limit, BucketKey::Shared);
    for effect in written {
        let kind = kind_of(effect);
        let w = weighed(kind);
        let (charge, _) = effect.charge().unwrap();
        let inst = w.names_instrument.then_some(AAPL);
        assert_eq!(
            charge,
            RateCharge {
                op: w.op,
                inst,
                weight: std::num::NonZeroU32::new(w.tenths).unwrap(),
            },
            "{kind}: {}",
            w.cite
        );
        for (limit, lands) in [(GENERAL, w.general), (CANCEL, w.cancel)] {
            let via = match effect {
                Effect::Http { .. } => Via::Http,
                _ => Via::Frame,
            };
            assert_eq!(limits[limit].counts(&charge, via), lands, "{kind}");
        }
        let before = (used(GENERAL), used(CANCEL));
        let request = Request::of(effect).unwrap();
        limiter.charge(Instant::now(), conn, &[request]).unwrap();
        let tenths = u64::from(w.tenths);
        let want = (
            before.0 + if w.general { tenths } else { 0 },
            before.1 + if w.cancel { tenths } else { 0 },
        );
        assert_eq!((used(GENERAL), used(CANCEL)), want, "{kind}: {}", w.cite);
    }
}

// ----- The sessions -----

/// The provider user both sessions belong to, and another user's.
const USER: RateUserKey = RateUserKey::new(0x97a0_0010);
const OTHER_USER: RateUserKey = RateUserKey::new(0x97a0_0020);
/// The requests' deadline in these sessions.
const DEADLINE: Duration = Duration::from_secs(10);
/// One connection attempt a day: a session whose epoch ends attempts no second.
const DAY: Duration = Duration::from_secs(86_400);
/// The declared window, which a hold without a hint lasts at least.
const WINDOW: Duration = Duration::from_secs(60);
const MS: Duration = Duration::from_millis(1);
const SHARD: u16 = 0;

/// The venue order ids the stub acknowledges the two resting orders with.
const PRIMARY_ORDER: &str = "00000001-0000-4000-8000-000000000001";
const SUB_ORDER: &str = "00000002-0000-4000-8000-000000000002";

/// The hints, as the two documented forms write them (0134 item 3).
const SECONDS_HINT: Duration = Duration::from_secs(3);
const MILLIS_HINT: Duration = Duration::from_millis(1_500);
const CANCEL_HINT: Duration = Duration::from_secs(2);

/// An echo of `kind` with a parameter, which the journal spans.
fn echo(kind: &str) -> String {
    format!(r#"{{"type":"{kind}","params":{{"symbol":"AAPL-USD"}}}}"#)
}

/// A flat RateLimited err echoing an add_order, with a seconds hint (the rate page's form).
fn flat_place_refusal() -> String {
    flat(
        "RateLimited",
        "\"Rate limit exceeded, retry after 3 seconds\"",
        &echo("add_order"),
    )
}

/// A nested RateLimited err echoing an add_order, with a milliseconds hint (the errors page's).
fn nested_place_refusal() -> String {
    nested(
        "RateLimited",
        "\"Too many requests. Please retry after 1500ms.\"",
        &echo("add_order"),
    )
}

/// An `order_response` whose status is `RATE_LIMITED`, under the place's own client id.
fn rate_limited_order(place: &Value) -> String {
    let cid = place["params"]["client_order_id"].to_string();
    order_frame(&[
        ("status", Some("\"RATE_LIMITED\"")),
        ("client_order_id", Some(&cid)),
    ])
}

/// A flat RateLimited err echoing a cancel_order, with a seconds hint.
fn cancel_refusal() -> String {
    flat(
        "RateLimited",
        "\"Rate limit exceeded, retry after 2 seconds\"",
        &echo("cancel_order"),
    )
}

/// A flat RateLimited err whose echo is `{}`, naming no bucket, its message no hint.
fn bucketless_refusal() -> String {
    flat("RateLimited", "\"Rate limit exceeded\"", "{}")
}

/// A flat RateLimited err echoing authentication, which the journal keeps whole (0134 item 2).
fn auth_refusal() -> String {
    flat(
        "RateLimited",
        "\"Rate limit exceeded, retry after 1 seconds\"",
        r#"{"type":"auth","params":{"hmac":{"public_key":"qfex_pub_SYNTHETIC_0001"}}}"#,
    )
}

/// A responder answering a frame whose `type` is `want` with what `answer` builds from it.
fn answering(
    want: &'static str,
    answer: impl Fn(&Value) -> String + Send + Sync + 'static,
) -> Responder {
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary frame".to_owned());
        };
        let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if v["type"] != want {
            return Err(format!("not {want}: {text}"));
        }
        Ok(vec![Frame::text(answer(&v))])
    })
}

/// A responder reading one frame of `type` `want` and answering nothing.
fn reading(want: &'static str) -> Responder {
    Responder::new(move |frame| match frame {
        Frame::Text(text)
            if serde_json::from_str::<Value>(text).is_ok_and(|v| v["type"] == want) =>
        {
            Ok(Vec::new())
        }
        other => Err(format!("not {want}: {other:?}")),
    })
}

/// The place's `ACK`, as the venue order `order_id`, under its own client id, price and size.
fn acked(order_id: &'static str) -> impl Fn(&Value) -> String {
    move |place| {
        let p = &place["params"];
        let (cid, px, qty) = (
            p["client_order_id"].to_string(),
            p["price"].to_string(),
            p["quantity"].to_string(),
        );
        order_frame(&[
            ("order_id", Some(&quoted(order_id))),
            ("client_order_id", Some(&cid)),
            ("price", Some(&px)),
            ("quantity", Some(&qty)),
            ("quantity_remaining", Some(&qty)),
        ])
    }
}

/// A responder confirming the subscription it read, as the AsyncAPI's `subscribed` reply does.
fn confirming() -> Responder {
    Responder::new(|frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary subscribe".to_owned());
        };
        let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let channel = v["params"]["channels"][0].as_str().ok_or("no channel")?;
        Ok(vec![Frame::text(confirm(channel))])
    })
}

/// Connection `conn`'s opening: accepted, authenticated, its three subscriptions confirmed, its
/// arm acknowledged and its resync's one page answered empty.
fn opening(conn: usize) -> Vec<WsStep> {
    let mut steps = vec![
        WsStep::Accept,
        WsStep::Respond {
            conn,
            with: answering("auth", |_| AUTHENTICATED.to_owned()),
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
        with: answering("cancel_on_disconnect", |_| ack(ARM)),
    });
    steps.push(WsStep::Respond {
        conn,
        with: answering("get_user_orders", |_| EMPTY_PAGE.to_owned()),
    });
    steps
}

/// The primary's connection and the subaccount's.
const P: usize = 0;
const S: usize = 1;

/// The stub's script, in the order the test drives the sessions: the primary's opening, the
/// subaccount's, then one step per frame the test expects each to write, answered or read.
fn script() -> Vec<WsStep> {
    let respond = |conn, with| WsStep::Respond { conn, with };
    let mut steps = opening(P);
    steps.extend(opening(S));
    steps.extend([
        // The two resting orders, each acknowledged.
        respond(P, answering("add_order", acked(PRIMARY_ORDER))),
        respond(S, answering("add_order", acked(SUB_ORDER))),
        // (1) A flat refusal of the primary's IOC place; the subaccount's cancel goes out, then
        // its place once the hint has passed.
        respond(P, answering("add_order", |_| flat_place_refusal())),
        respond(S, reading("cancel_order")),
        respond(S, reading("add_order")),
        // (2) A nested refusal of the subaccount's ALO place; the primary's cancel goes out,
        // then its place.
        respond(S, answering("add_order", |_| nested_place_refusal())),
        respond(P, reading("cancel_order")),
        respond(P, reading("add_order")),
        // (3) A RATE_LIMITED status for the primary's place; the subaccount writes nothing
        // until the declared window has passed, then its place.
        respond(P, answering("add_order", rate_limited_order)),
        respond(S, reading("add_order")),
        // (4) A refusal of the subaccount's cancel; the primary's place and query go out, then
        // its cancel.
        respond(S, answering("cancel_order", |_| cancel_refusal())),
        respond(P, reading("add_order")),
        respond(P, reading("get_order")),
        respond(P, reading("cancel_order")),
        // (5) A refusal echoing `{}` of the primary's query; the subaccount writes nothing until
        // the declared window has passed, then its cancel.
        respond(P, answering("get_order", |_| bucketless_refusal())),
        respond(S, reading("cancel_order")),
        // (6) A refusal echoing authentication of the primary's query, which ends its epoch
        // and holds nothing: the subaccount's place and query go out at once.
        respond(P, answering("get_order", |_| auth_refusal())),
        respond(S, reading("add_order")),
        respond(S, reading("get_order")),
    ]);
    steps
}

/// The stub's REST route: [`FLAT`] for every positions read.
fn positions_route() -> HttpRouter {
    HttpRouter::new().route_fn(Method::GET, PathPattern::exact("/user/positions"), |_| {
        HttpReply {
            status: 200,
            body: FLAT.as_bytes().to_vec(),
        }
    })
}

/// Stops tokio's paused clock from jumping while socket I/O is under way: it moves only by
/// `advance`, until the returned sender drops.
fn freeze() -> std::sync::mpsc::Sender<()> {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    thaw
}

/// Lets every task run a while without moving the clock.
async fn churn() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

/// Lets every task run, without moving the clock, until `done`; panics after a minute of
/// wall-clock time, so a regression fails instead of hanging.
async fn until(what: &str, done: impl Fn() -> bool) {
    let expired = Arc::new(AtomicBool::new(false));
    let flag = expired.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(60));
        flag.store(true, Ordering::SeqCst);
    });
    while !done() {
        assert!(!expired.load(Ordering::SeqCst), "{what}: not within 60 s");
        tokio::task::yield_now().await;
    }
}

/// Moves virtual time by `by`, then lets every task run. The clock stops on the way at each
/// refused place's deadline that falls inside the step, so its `Unknown` is stamped at that
/// deadline and not at the step's end.
async fn wait(driven: &Driven, by: Duration) {
    let end = Instant::now() + by;
    let mut stops: Vec<Instant> = (driven.deadlines.iter())
        .copied()
        .filter(|at| *at > Instant::now() && *at < end)
        .collect();
    stops.sort();
    for stop in stops.into_iter().chain([end]) {
        advance(stop - Instant::now()).await;
        churn().await;
    }
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

// ----- A recording codec, live and in replay -----

/// One call of the codec: what it was, what it pushed and asked for, and its counts after it.
#[derive(Clone, Debug, PartialEq)]
struct Call {
    what: String,
    result: Result<(), String>,
    events: Vec<(VenueMeta, ExecEvent)>,
    effects: Vec<Effect>,
    counts: QfexExecCounts,
}

/// Every call of one codec, in order.
type Calls = Arc<Mutex<Vec<Call>>>;

/// The probe's codec, built as the factory builds it, its every call kept.
struct Recording {
    codec: QfexExec,
    calls: Calls,
}

struct Pushed<'s>(&'s mut dyn ExecSink, Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Pushed<'_> {
    fn push(&mut self, meta: VenueMeta, event: ExecEvent) {
        self.1.push((meta, event.clone()));
        self.0.push(meta, event);
    }
}

impl Recording {
    fn keep(
        &self,
        what: String,
        result: Result<(), String>,
        events: Vec<(VenueMeta, ExecEvent)>,
        fx: &Effects,
    ) {
        self.calls.lock().unwrap().push(Call {
            what,
            result,
            events,
            effects: fx.as_slice().to_vec(),
            counts: self.codec.counts(),
        });
    }
}

impl ExecCodec for Recording {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.codec.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        let mut mine = Effects::new();
        self.codec.on_open(stream, ctx, &mut mine);
        self.keep(format!("open {stream:?}"), Ok(()), Vec::new(), &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let mut mine = Effects::new();
        let sent = self.codec.encode(cmd, rpc, specs, ctx, t, &mut mine);
        let result = sent.as_ref().map(|_| ()).map_err(|why| format!("{why:?}"));
        self.keep(format!("encode {rpc:?} {cmd:?}"), result, Vec::new(), &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
        sent
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        frame: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let mut mine = Effects::new();
        let mut pushed = Pushed(sink, Vec::new());
        let read = self
            .codec
            .on_frame(stream, frame, scope, specs, &mut pushed, &mut mine);
        let events = pushed.1;
        let result = read.as_ref().map(|_| ()).map_err(|e| format!("{e:?}"));
        self.keep("frame".to_owned(), result, events, &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
        read
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let mut mine = Effects::new();
        let mut pushed = Pushed(sink, Vec::new());
        let read = self
            .codec
            .on_http(tag, resp, scope, specs, &mut pushed, &mut mine);
        let events = pushed.1;
        let result = read.as_ref().map(|_| ()).map_err(|e| format!("{e:?}"));
        self.keep(format!("http {tag:?}"), result, events, &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
        read
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        let mut mine = Effects::new();
        self.codec.on_timer(tag, ctx, &mut mine);
        self.keep(format!("timer {tag:?}"), Ok(()), Vec::new(), &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let mut pushed = Pushed(sink, Vec::new());
        self.codec.on_rpc_timeout(rpc, &mut pushed);
        let events = pushed.1;
        self.keep(format!("deadline {rpc:?}"), Ok(()), events, &Effects::new());
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        let mut mine = Effects::new();
        self.codec.resync(ctx, &mut mine);
        self.keep("resync".to_owned(), Ok(()), Vec::new(), &mine);
        mine.take().into_iter().for_each(|e| fx.push(e));
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.codec.redact_inbound(input)
    }
}

/// QFEX's factory, but for the order-entry codec, which it builds as QFEX's factory does and
/// wraps in a [`Recording`] keeping its calls in `calls`.
struct Recorded {
    calls: Calls,
}

impl Recorded {
    fn venue() -> (&'static Recorded, Calls) {
        let calls = Calls::default();
        let venue = Box::leak(Box::new(Recorded {
            calls: Arc::clone(&calls),
        }));
        (venue, calls)
    }
}

impl VenueFactory for Recorded {
    fn id(&self) -> &'static str {
        Qfex.id()
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        Qfex.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Qfex.caps(cfg)
    }

    fn exec_account_key(&self, cfg: &VenueConfig) -> Result<Option<AccountKey>, ConfigError> {
        Qfex.exec_account_key(cfg)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        Qfex.parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Qfex.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        Qfex.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Qfex.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Qfex.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let codec = TradeAuth::new(cfg, creds).and_then(|auth| Ok(QfexExec::probe(auth, cfg)?));
        Some(codec.map(|codec| {
            Box::new(Recording {
                codec,
                calls: Arc::clone(&self.calls),
            }) as Box<dyn ExecCodec>
        }))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        Qfex.test_connection(cfg, creds)
    }
}

// ----- What a session's handler hears, and the registry behind it -----

/// One thing a handler was told, in order.
#[derive(Clone, Debug, PartialEq)]
enum Heard {
    Event(Box<Envelope<ExecEvent>>),
    Ended(ConnKey),
    DecodeError(ConnKey),
}

#[derive(Default)]
struct Book {
    heard: Vec<Heard>,
    /// What became of each submission, by request id.
    receipts: HashMap<RpcId, Result<(), NotSentReason>>,
    /// The account's registry, live; none in replay.
    reg: Option<Registry>,
}

#[derive(Clone, Default)]
struct Watch(Rc<RefCell<Book>>);

impl Watch {
    fn heard(&self) -> Vec<Heard> {
        self.0.borrow().heard.clone()
    }

    fn events(&self) -> Vec<ExecEvent> {
        let book = self.0.borrow();
        let events = book.heard.iter().filter_map(|h| match h {
            Heard::Event(env) => Some(env.body.clone()),
            _ => None,
        });
        events.collect()
    }

    fn ended(&self) -> Vec<ConnKey> {
        let book = self.0.borrow();
        let ended = book.heard.iter().filter_map(|h| match h {
            Heard::Ended(key) => Some(*key),
            _ => None,
        });
        ended.collect()
    }

    /// Whether the arm was accepted and a resync ended: the epoch may place.
    fn ready(&self) -> bool {
        let events = self.events();
        let accepted = events.iter().any(|e| {
            matches!(
                e,
                ExecEvent::Outcome {
                    outcome: SubmitOutcome::Accepted { .. },
                    ..
                }
            )
        });
        accepted && events.contains(&ExecEvent::ResyncEnd)
    }

    fn receipt(&self, rpc: RpcId) -> Option<Result<(), NotSentReason>> {
        self.0.borrow().receipts.get(&rpc).copied()
    }
}

impl ExecHandler for Watch {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut book = self.0.borrow_mut();
        if let (ExecEvent::Order(update), Some(reg)) = (&env.body, book.reg.as_mut()) {
            let key = OrderKey {
                venue: None,
                ingest: env.stamp.ingest_seq,
            };
            reg.apply_update(update, key);
        }
        book.heard.push(Heard::Event(Box::new(env)));
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().heard.push(Heard::Ended(key));
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let receipt = handle.receipt.map(|_| ());
        self.0.borrow_mut().receipts.insert(handle.rpc, receipt);
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.0.borrow_mut().heard.push(Heard::DecodeError(key));
    }
}

/// A directory of the test's own, removed when dropped.
struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn dir(what: &str) -> Dir {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("fbc-qfex-p7rn-{what}-{}-{n}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    Dir(path)
}

/// One submission: its request id and its command, which a replay is handed by that id.
struct Sent {
    rpc: RpcId,
    cmd: VenueCommand,
}

/// One account's side of the test: its configuration, its session's orders, its registry (in
/// its handler's book), its client-id mint, and every command it submitted.
struct Account {
    cfg: VenueConfig,
    acct: AccountKey,
    conn: u16,
    order: OrderCaps,
    watch: Watch,
    mint: CidMint,
    orders: Option<ExecOrders>,
    sent: Vec<Sent>,
    /// The resting order the stub acknowledged, by client id.
    resting: Option<fbc_core::ClientOrderId>,
    calls: Calls,
    venue: &'static Recorded,
    journal: Dir,
    _leases: Dir,
}

/// The configuration of a probe session for `account` against `server`.
fn against(server: &StubServer, account: &str) -> VenueConfig {
    let ws = server.ws_url("");
    let cfg = on(account, &ws, &server.http_url(""), &ws);
    assert_eq!(
        cfg.get(fbc_venue_qfex::probe::KEY_EXEC_MODE),
        Some(PREPROD_PROBE)
    );
    with(cfg, KEY_RPC_TIMEOUT, "10s")
}

impl Account {
    fn new(server: &StubServer, account: &str, conn: u16) -> Account {
        let cfg = against(server, account);
        let acct = Qfex.exec_account_key(&cfg).unwrap().unwrap();
        let order = Qfex.caps(&cfg).unwrap().exec.unwrap().order;
        let leases = dir(&format!("leases-{account}"));
        let name = format!("p7rn-{account}");
        let symbol: VenueSymbol = spec().venue_symbol;
        let keys = LeaseKeys::new(Qfex.id(), &name, &order).with_market(AAPL, symbol.clone());
        let cap = Some(Lots::new(1_000_000).unwrap());
        let limits = MarketCapsConfig {
            inventory: cap,
            resting: cap,
        };
        let pre = PreTradeCaps::new().with_market(AAPL, limits).unwrap();
        let mut reg = Registry::with_caps(pre).with_lease_keys(keys);
        reg.bind_account(acct).unwrap();
        // Seeded flat by a snapshot of the test's own (module documentation, decision 0087).
        let snap = ResyncSnapshot {
            watermark: WallNs(1_000),
            requested_at: MonoNs(1_000),
            orders: Vec::new(),
            positions: vec![(AAPL, SignedLots(0))],
        };
        let second = Duration::from_secs(1);
        let ladder = LadderConfig::new(second, Duration::ZERO, 10 * second, 1).unwrap();
        let ours = OrderCaps {
            snapshot_source: SnapshotSource::Trustworthy,
            ..order.clone()
        };
        let key = OrderKey {
            venue: None,
            ingest: 1,
        };
        reg.resync(&ladder, &ours, &snap, key).unwrap();
        let market = MarketLease::acquire(&leases.0, Qfex.id(), &name, &symbol).unwrap();
        let account_lease = AccountLease::acquire(&leases.0, Qfex.id(), &name).unwrap();
        reg.start(AAPL, Leases::market(market).with_account(account_lease))
            .unwrap();
        let ns = NamespaceLease::acquire(&leases.0, acct, NS).unwrap();
        let mint = CidMint::new(ns, 0, 0, WallNs(1_760_000_000_000_000_000));
        let watch = Watch::default();
        watch.0.borrow_mut().reg = Some(reg);
        let (venue, calls) = Recorded::venue();
        Account {
            cfg,
            acct,
            conn,
            order,
            watch,
            mint,
            orders: None,
            sent: Vec::new(),
            resting: None,
            calls,
            venue,
            journal: dir(&format!("journal-{account}")),
            _leases: leases,
        }
    }

    /// The session's configuration, its limiter built from `caps.exec_limits` under [`USER`].
    fn session(&self, limiter: RateLimiter) -> ExecSessionConfig {
        let bound = ReservedBound::new(32, 32).unwrap();
        ExecSessionConfig {
            venue: self.venue,
            cfg: self.cfg.clone(),
            creds: secrets(),
            acct: self.acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(self.acct)),
            rpc_ids: RpcIds::default(),
            ns: NS,
            specs: specs(),
            connector: Connector::new(ProxyConfig::Direct),
            pacing: ReconnectPacing::new(DAY, DAY, 1, DAY, Duration::MAX).unwrap(),
            clock: IngestClock::new(),
            nonces: Box::new(Counting(1_000)),
            nonce_source: NonceSourceId(0),
            conn: self.conn,
            limiter,
            write_stall: WriteStall::new(Duration::from_secs(3600)).unwrap(),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        }
    }

    fn submitted(&mut self, rpc: RpcId, cmd: VenueCommand) -> RpcId {
        self.sent.push(Sent { rpc, cmd });
        rpc
    }

    /// A limit buy of 1.00 at 200.50 on `AAPL-USD`, of the shape `post_only` and `tif` give,
    /// built and authorized by the account's registry, and submitted.
    fn place(&mut self, post_only: bool, tif: Tif) -> RpcId {
        let order = NewOrder {
            cid: self.mint.mint().unwrap(),
            inst: AAPL,
            side: Side::Buy,
            qty: Lots::new(100).unwrap(),
            kind: OrderKind::Limit { px: Ticks(20_050) },
            tif,
            channel: Channel::Public,
            post_only,
            reduce_only: false,
            reducing: false,
        };
        let auth = {
            let mut book = self.watch.0.borrow_mut();
            let reg = book.reg.as_mut().unwrap();
            let cmd = reg.place(order).unwrap();
            reg.authorize(self.acct, cmd).unwrap()
        };
        let cmd = auth.command().clone();
        let rpc = self.orders.as_ref().unwrap().submit(auth).unwrap();
        self.submitted(rpc, cmd)
    }

    fn gtc(&mut self) -> RpcId {
        self.place(false, Tif::Gtc)
    }

    /// A cancel of the resting order, by its venue id, built and authorized by the registry.
    fn cancel(&mut self) -> RpcId {
        let cid = self.resting.unwrap();
        let auth = {
            let mut book = self.watch.0.borrow_mut();
            let reg = book.reg.as_mut().unwrap();
            let CancelChoice::Send(cmd) = reg.cancellable(cid).unwrap().cancel(&self.order) else {
                panic!("the resting order's cancel waits for its ack");
            };
            reg.authorize(self.acct, cmd).unwrap()
        };
        let cmd = auth.command().clone();
        let rpc = self.orders.as_ref().unwrap().submit(auth).unwrap();
        self.submitted(rpc, cmd)
    }

    /// A query of the resting order, by its venue id: a control command.
    fn query(&mut self) -> RpcId {
        let vid = self.resting_vid().unwrap();
        let query = QueryOrder {
            target: OrderRef::Venue(vid),
            inst: AAPL,
            placement_nonce: None,
        };
        let cmd = VenueCommand::Query(query.clone());
        let control = ControlCommand::Query(query);
        let rpc = self
            .orders
            .as_ref()
            .unwrap()
            .submit_control(control)
            .unwrap();
        self.submitted(rpc, cmd)
    }

    fn resting_vid(&self) -> Option<VenueOrderId> {
        let cid = self.resting?;
        let book = self.watch.0.borrow();
        book.reg.as_ref()?.get(cid)?.vid().cloned()
    }

    /// The command submitted on `rpc`.
    fn command(&self, rpc: RpcId) -> VenueCommand {
        let sent = self.sent.iter().find(|s| s.rpc == rpc).unwrap();
        sent.cmd.clone()
    }

    /// What became of `rpc`, once the session said.
    async fn receipt(&self, rpc: RpcId) -> Result<(), NotSentReason> {
        until(&format!("{rpc:?}'s receipt"), || {
            self.watch.receipt(rpc).is_some()
        })
        .await;
        self.watch.receipt(rpc).unwrap()
    }
}

/// Every frame the stub received on connection `conn`, as text.
fn received(server: &StubServer, conn: usize) -> Vec<String> {
    let conns = server.connections();
    let Some(record) = conns.get(conn) else {
        return Vec::new();
    };
    (record.received.iter())
        .map(|f| match f {
            Frame::Text(t) => t.clone(),
            Frame::Binary(_) => "<binary>".to_owned(),
        })
        .collect()
}

/// What the test drove and saw, for the checks after the sessions stopped.
#[derive(Default)]
struct Driven {
    /// Each command the buckets refused, by account: `NotSent(RateBudget)`, nothing written.
    held: [Vec<RpcId>; 2],
    /// Each command written, by account.
    written: [Vec<RpcId>; 2],
    /// The places the venue refused for their rate, by account.
    refused_places: [Vec<RpcId>; 2],
    /// `counts().rejected` after each refusal.
    rejected: Vec<ScopeCounts>,
    /// Each refused place's deadline: its write's instant plus [`DEADLINE`].
    deadlines: Vec<Instant>,
}

impl Driven {
    /// Keeps `rpc`, written by `who` just now, as a place the venue refused for its rate.
    fn refused_place(&mut self, who: usize, rpc: RpcId) {
        self.refused_places[who].push(rpc);
        self.deadlines.push(Instant::now() + DEADLINE);
    }
}

/// Submits on `who`, through `submit`, a command the buckets must refuse: `NotSent(RateBudget)`,
/// and nothing more on its connection.
async fn held(
    server: &StubServer,
    who: &mut Account,
    driven: &mut Driven,
    submit: impl FnOnce(&mut Account) -> RpcId,
) {
    let conn = who.conn as usize;
    let before = received(server, conn).len();
    let rpc = submit(who);
    assert_eq!(
        who.receipt(rpc).await,
        Err(NotSentReason::RateBudget),
        "{rpc:?}"
    );
    churn().await;
    assert_eq!(received(server, conn).len(), before, "{rpc:?} was written");
    driven.held[conn].push(rpc);
}

/// Submits on `who`, through `submit`, a command that goes out: sent, and the stub's script read
/// it as its step `mark`.
async fn goes_out(
    server: &StubServer,
    who: &mut Account,
    driven: &mut Driven,
    mark: &mut usize,
    submit: impl FnOnce(&mut Account) -> RpcId,
) -> RpcId {
    let rpc = submit(who);
    assert_eq!(who.receipt(rpc).await, Ok(()), "{rpc:?}");
    *mark += 1;
    let at = *mark;
    until(&format!("{rpc:?} read"), || server.played() >= at).await;
    driven.written[who.conn as usize].push(rpc);
    rpc
}

/// Waits for the shared limiter's `n`th refusal, and keeps its counts.
async fn refusal(limiter: &RateLimiter, driven: &mut Driven, n: u64) {
    until(&format!("refusal {n}"), || {
        limiter.counts().rejected.user == n
    })
    .await;
    let rejected = limiter.counts().rejected;
    assert_eq!(
        rejected,
        ScopeCounts {
            user: n,
            ..ScopeCounts::default()
        }
    );
    driven.rejected.push(rejected);
}

#[tokio::test(start_paused = true)]
async fn two_accounts_of_one_provider_user_hold_what_each_refusal_names() {
    let frozen = freeze();
    let steps = script();
    let total = steps.len();
    let server = StubServer::start(WsScript::new(steps), positions_route())
        .await
        .unwrap();
    let mut primary = Account::new(&server, "primary", P as u16);
    let mut sub = Account::new(&server, SUB, S as u16);
    let limits = Qfex.caps(&primary.cfg).unwrap().exec_limits.unwrap();
    assert_eq!(limits, Qfex.caps(&sub.cfg).unwrap().exec_limits.unwrap());
    let reserve = SafetyReserve::percent(10).unwrap();
    // Each session its own limiter of the one provider user's key, and another user's beside.
    let p_limiter = RateLimiter::for_user(USER, &limits, reserve).unwrap();
    let s_limiter = RateLimiter::for_user(USER, &limits, reserve).unwrap();
    let other = RateLimiter::for_user(OTHER_USER, &limits, reserve).unwrap();
    let limiter = p_limiter.clone();

    let journaled = |account: &Account| {
        let (queue, drain) = journal_queue(
            SinkConfig {
                budget_bytes: 16 << 20,
                soft_limit_pct: 90,
            },
            redaction_key(),
        )
        .unwrap();
        let writer = JournalWriter::create(&account.journal.0, SHARD, redaction_key()).unwrap();
        (queue, drain.spawn(writer).unwrap())
    };
    let (p_queue, p_writer) = journaled(&primary);
    let (s_queue, s_writer) = journaled(&sub);
    let (mut p_session, p_control) =
        ExecSession::new(primary.session(p_limiter), primary.watch.clone()).unwrap();
    let (mut s_session, s_control) =
        ExecSession::new(sub.session(s_limiter), sub.watch.clone()).unwrap();
    p_session.set_journal(Journal::new(Rc::new(RefCell::new(p_queue))));
    s_session.set_journal(Journal::new(Rc::new(RefCell::new(s_queue))));
    primary.orders = Some(p_session.orders());
    sub.orders = Some(s_session.orders());

    let p_ready = primary.watch.clone();
    let s_ready = sub.watch.clone();
    let mut driven = Driven::default();
    let server = &server;
    let drive = async {
        let (p, s) = (&mut primary, &mut sub);
        let d = &mut driven;
        // Both opened, armed and resynced. Each opening charged its authentication (0.1 in both
        // buckets), three subscriptions (0.1 each, general), the arm (0.1 in both) and one
        // orders page (5.0, general); the positions read is in neither.
        until("the subaccount's opening", || s_ready.ready()).await;
        churn().await;
        let mut mark = 2 * opening(P).len();
        assert_eq!(server.played(), mark);
        let used = |limit| limiter.used(Instant::now(), limit, BucketKey::Shared);
        assert_eq!((used(GENERAL), used(CANCEL)), (2 * 55, 2 * 2));

        // The two resting orders, acknowledged under our client ids.
        let p1 = goes_out(server, p, d, &mut mark, Account::gtc).await;
        p.resting = Some(cid_of(p, p1));
        until("the primary's ack", || p.resting_vid().is_some()).await;
        let s1 = goes_out(server, s, d, &mut mark, Account::gtc).await;
        s.resting = Some(cid_of(s, s1));
        until("the subaccount's ack", || s.resting_vid().is_some()).await;

        // (1) A flat RateLimited err echoing add_order, 3 seconds: the general bucket is held
        // for both accounts; the subaccount's place and query are not written, its cancel is.
        let refused = goes_out(server, p, d, &mut mark, |a| a.place(false, Tif::Ioc)).await;
        d.refused_place(P, refused);
        refusal(&limiter, d, 1).await;
        held(server, s, d, Account::gtc).await;
        held(server, s, d, Account::query).await;
        goes_out(server, s, d, &mut mark, Account::cancel).await;
        wait(d, SECONDS_HINT - MS).await;
        held(server, s, d, Account::gtc).await;
        wait(d, MS).await;
        goes_out(server, s, d, &mut mark, Account::gtc).await;

        // (2) A nested RateLimited err echoing add_order, 1500 ms: the same for the primary.
        let refused = goes_out(server, s, d, &mut mark, |a| a.place(true, Tif::Gtc)).await;
        d.refused_place(S, refused);
        refusal(&limiter, d, 2).await;
        held(server, p, d, Account::gtc).await;
        held(server, p, d, Account::query).await;
        goes_out(server, p, d, &mut mark, Account::cancel).await;
        wait(d, MILLIS_HINT - MS).await;
        held(server, p, d, Account::gtc).await;
        wait(d, MS).await;
        goes_out(server, p, d, &mut mark, Account::gtc).await;

        // (3) An order_response whose status is RATE_LIMITED: every frame budget held for the
        // declared window, so no bucket is left free.
        let refused = goes_out(server, p, d, &mut mark, Account::gtc).await;
        d.refused_place(P, refused);
        refusal(&limiter, d, 3).await;
        held(server, s, d, Account::gtc).await;
        held(server, s, d, Account::cancel).await;
        held(server, s, d, Account::query).await;
        // Another provider user's limiter, meanwhile, admits a place and a cancel: the hold is
        // the one user's alone.
        other_admits(&other);
        wait(d, WINDOW - MS).await;
        held(server, s, d, Account::cancel).await;
        wait(d, MS).await;
        goes_out(server, s, d, &mut mark, Account::gtc).await;

        // (4) A RateLimited err echoing cancel_order, 2 seconds: the cancel bucket is held, and
        // places and queries stay free.
        goes_out(server, s, d, &mut mark, Account::cancel).await;
        refusal(&limiter, d, 4).await;
        held(server, p, d, Account::cancel).await;
        goes_out(server, p, d, &mut mark, Account::gtc).await;
        goes_out(server, p, d, &mut mark, Account::query).await;
        wait(d, CANCEL_HINT - MS).await;
        held(server, p, d, Account::cancel).await;
        wait(d, MS).await;
        goes_out(server, p, d, &mut mark, Account::cancel).await;

        // (5) A RateLimited err whose echo is `{}`, its message no hint: every frame budget
        // held for at least the declared window.
        goes_out(server, p, d, &mut mark, Account::query).await;
        refusal(&limiter, d, 5).await;
        held(server, s, d, Account::gtc).await;
        held(server, s, d, Account::cancel).await;
        held(server, s, d, Account::query).await;
        wait(d, WINDOW - MS).await;
        held(server, s, d, Account::query).await;
        wait(d, MS).await;
        goes_out(server, s, d, &mut mark, Account::cancel).await;

        // (6) A RateLimited err echoing authentication: kept whole, so it holds nothing, and it
        // ends the primary's epoch; the subaccount's place and query go out at once.
        assert!(p.watch.ended().is_empty());
        goes_out(server, p, d, &mut mark, Account::query).await;
        until("the primary's epoch end", || p_ready.ended().len() == 1).await;
        churn().await;
        assert_eq!(limiter.counts().rejected.user, 5);
        goes_out(server, s, d, &mut mark, Account::gtc).await;
        goes_out(server, s, d, &mut mark, Account::query).await;
        assert_eq!(mark, total);

        // Every request's deadline passes: each unanswered one ends Unknown.
        wait(d, DEADLINE).await;
        server.finished().await.unwrap();
        churn().await;
        drop(p_control);
        drop(s_control);
    };
    let s_run = async {
        // The subaccount connects once the primary is ready, so the stub's connections are
        // numbered as the script expects.
        until("the primary's opening", || p_ready.ready()).await;
        s_session.run().await
    };
    let (p_ran, s_ran, ()) = tokio::join!(p_session.run(), s_run, drive);
    p_ran.unwrap();
    s_ran.unwrap();
    let counters = [p_session.counters(), s_session.counters()];
    drop(p_session);
    drop(s_session);
    drop(frozen);
    p_writer.close().unwrap();
    s_writer.close().unwrap();

    check_live(
        &limiter,
        &other,
        &driven,
        counters,
        [&primary, &sub],
        server,
    );
    for account in [&primary, &sub] {
        check_replay(account, &driven);
    }
}

/// The client id of the place submitted on `rpc`.
fn cid_of(account: &Account, rpc: RpcId) -> fbc_core::ClientOrderId {
    match account.command(rpc) {
        VenueCommand::Place(order) => order.cid,
        other => panic!("{other:?}"),
    }
}

fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[23; 32]).unwrap())
}

/// The journal of `account`'s session, read back.
fn journal(account: &Account) -> Vec<Record> {
    JournalReader::open(&account.journal.0, SHARD)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Charges another provider user's limiter a Normal place and a cancel on `AAPL`, each admitted
/// and in its bucket alone, while the first user's every frame budget is held.
fn other_admits(other: &RateLimiter) {
    let conn = ConnKey { conn: 0, epoch: 1 };
    let used = |limit| other.used(Instant::now(), limit, BucketKey::Shared);
    let request = |op| Request {
        charge: RateCharge {
            op,
            inst: Some(AAPL),
            weight: std::num::NonZeroU32::new(10).unwrap(),
        },
        via: Via::Frame,
        class: TrafficClass::Normal,
    };
    assert_eq!((used(GENERAL), used(CANCEL)), (0, 0));
    other
        .charge(Instant::now(), conn, &[request(OpKind::Place)])
        .unwrap();
    assert_eq!((used(GENERAL), used(CANCEL)), (10, 0));
    other
        .charge(Instant::now(), conn, &[request(OpKind::Cancel)])
        .unwrap();
    assert_eq!((used(GENERAL), used(CANCEL)), (10, 10));
    assert_eq!(other.counts(), fbc_runtime::RateCounts::default());
}

/// What the live run must show, once both sessions stopped.
fn check_live(
    limiter: &RateLimiter,
    other: &RateLimiter,
    driven: &Driven,
    counters: [ExecCounters; 2],
    accounts: [&Account; 2],
    server: &StubServer,
) {
    // Each refusal counted once under the user scope, and nothing else; every frame the buckets
    // held counted as refused there; the other user's limiter, which admitted a place and a
    // cancel during (3)'s hold, counting no refusal and, its charges past their window, empty.
    let rejected: Vec<u64> = driven.rejected.iter().map(|c| c.user).collect();
    assert_eq!(rejected, [1, 2, 3, 4, 5]);
    let counts = limiter.counts();
    assert_eq!(counts.rejected.user, 5);
    let held = (driven.held[P].len() + driven.held[S].len()) as u64;
    assert_eq!(
        counts.refused,
        ScopeCounts {
            user: held,
            ..ScopeCounts::default()
        }
    );
    assert_eq!(other.counts(), fbc_runtime::RateCounts::default());
    for limit in [GENERAL, CANCEL] {
        assert_eq!(other.used(Instant::now(), limit, BucketKey::Shared), 0);
    }

    // One connection attempt each, no attempt failed, nothing refused for the in-flight bound.
    // The primary's one decode error is the err echoing authentication, kept whole.
    for (who, c) in counters.iter().enumerate() {
        assert_eq!((c.attempts, c.failed_attempts), (1, 0), "{who}");
        assert_eq!(c.arm_failures, 0);
        assert_eq!(c.in_flight_refusals, 0);
        assert_eq!(c.backpressure_refusals, 0);
        assert_eq!(c.refused_effects, 0);
        assert_eq!(c.decode_errors, u64::from(who == P), "{who}");
    }
    assert_eq!(server.connections().len(), 2);

    for (who, account) in accounts.iter().enumerate() {
        let records = journal(account);
        let events = account.watch.events();
        let ended = account.watch.ended();
        // One epoch each: the primary's ended by the err echoing authentication, the
        // subaccount's at the stop.
        assert_eq!(ended.len(), 1, "{who}: {ended:?}");
        let epoch = ended[0];

        // What each request was written as: each written command once, on the one epoch, and
        // none the buckets held; the stub received exactly what the journal holds.
        let outbound: Vec<(Option<RpcId>, String)> = (records.iter())
            .filter_map(|r| match r {
                Record::Outbound {
                    conn, rpc, frame, ..
                } => {
                    assert_eq!(*conn, epoch);
                    Some((*rpc, String::from_utf8(frame.bytes().to_vec()).unwrap()))
                }
                _ => None,
            })
            .collect();
        let once = |rpc: RpcId| outbound.iter().filter(|(r, _)| *r == Some(rpc)).count();
        for rpc in &driven.written[who] {
            assert_eq!(once(*rpc), 1, "{who}: {rpc:?}");
        }
        for rpc in &driven.held[who] {
            assert_eq!(once(*rpc), 0, "{who}: {rpc:?}");
        }
        let stub = received(server, who);
        assert_eq!(stub.len(), outbound.len(), "{who}");
        // The six frames of the opening, then the written commands, in order.
        let written: Vec<Option<RpcId>> = outbound[6..].iter().map(|(rpc, _)| *rpc).collect();
        let want: Vec<Option<RpcId>> = driven.written[who].iter().map(|r| Some(*r)).collect();
        assert_eq!(written, want, "{who}");
        // No place is written twice: each client id appears in one frame.
        for sent in &account.sent {
            if let VenueCommand::Place(order) = &sent.cmd {
                let wire = fbc_core::encode_cid(&fbc_venue_qfex::exec::CLIENT_ID_FORMAT, order.cid)
                    .unwrap();
                let carrying = stub.iter().filter(|f| f.contains(wire.as_str())).count();
                let want = usize::from(driven.written[who].contains(&sent.rpc));
                assert_eq!(carrying, want, "{who}: {:?}", sent.rpc);
            }
        }

        // No refusal settles: the only outcomes are the arm's acceptance and an Unknown for
        // each written request at its deadline, the refused places among them.
        let outcomes: Vec<(RpcId, SubmitOutcome)> = (events.iter())
            .filter_map(|e| match e {
                ExecEvent::Outcome { rpc, outcome, .. } => Some((*rpc, outcome.clone())),
                _ => None,
            })
            .collect();
        let accepted: Vec<&(RpcId, SubmitOutcome)> = (outcomes.iter())
            .filter(|(_, o)| matches!(o, SubmitOutcome::Accepted { .. }))
            .collect();
        assert_eq!(accepted.len(), 1, "{who}: the arm alone");
        let mut unknown: Vec<RpcId> = (outcomes.iter())
            .filter(|(_, o)| *o == SubmitOutcome::Unknown)
            .map(|(rpc, _)| *rpc)
            .collect();
        assert_eq!(outcomes.len(), 1 + unknown.len(), "{who}: {outcomes:?}");
        unknown.sort();
        let mut want = driven.written[who].clone();
        want.sort();
        assert_eq!(unknown, want, "{who}");
        // Each refused place ended Unknown at its deadline: the clock stopped there, and its
        // timeout is stamped exactly its deadline after its write, neither before nor after.
        for rpc in &driven.refused_places[who] {
            let written = (records.iter()).find_map(|r| match r {
                Record::Outbound {
                    at, rpc: Some(r), ..
                } if r == rpc => Some(*at),
                _ => None,
            });
            let timed_out = (records.iter()).find_map(|r| match r {
                Record::RpcTimeout { stamp, rpc: r } if r == rpc => Some(stamp.recv_mono),
                _ => None,
            });
            let (written, timed_out) = (written.unwrap(), timed_out.unwrap());
            assert_eq!(
                timed_out.0 - written.0,
                DEADLINE.as_nanos() as u64,
                "{who}: {rpc:?}"
            );
        }

        // The codec: each RateLimited reply a budget-only hold, the err echoing authentication
        // none, and no reconnect but the one that err asked for.
        let calls = account.calls.lock().unwrap().clone();
        let last = calls.last().unwrap().counts;
        let holds: Vec<&Effect> = (calls.iter())
            .flat_map(|c| &c.effects)
            .filter(|e| matches!(e, Effect::RateLimited { .. }))
            .collect();
        let reconnects: Vec<&Call> = (calls.iter())
            .filter(|c| {
                c.effects
                    .iter()
                    .any(|e| matches!(e, Effect::Reconnect { .. }))
            })
            .collect();
        let hold = |op: Option<OpKind>, after: Option<Duration>| Effect::RateLimited {
            stream: TRADE,
            charge: op.map(|op| RateCharge::one(op, None)),
            retry_after: after,
        };
        if who == P {
            assert_eq!(
                holds,
                [
                    &hold(Some(OpKind::Place), Some(SECONDS_HINT)),
                    &hold(None, None),
                    &hold(None, None),
                ]
            );
            assert_eq!(last.rate_holds, 3);
            assert_eq!(last.undecodable, 1);
            let [ended] = reconnects.as_slice() else {
                panic!("{reconnects:?}");
            };
            assert_eq!(ended.what, "frame");
            assert!(ended.result.is_err());
            // The err echoing authentication is journaled whole, as a run of `2`s.
            let inbound: Vec<String> = (records.iter())
                .filter_map(|r| match r {
                    Record::Inbound { bytes, .. } => {
                        Some(String::from_utf8(bytes.0.clone()).unwrap())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(*inbound.last().unwrap(), "2".repeat(auth_refusal().len()));
        } else {
            assert_eq!(
                holds,
                [
                    &hold(Some(OpKind::Place), Some(MILLIS_HINT)),
                    &hold(Some(OpKind::Cancel), Some(CANCEL_HINT)),
                ]
            );
            assert_eq!(last.rate_holds, 2);
            assert_eq!(last.undecodable, 0);
            assert!(reconnects.is_empty(), "{reconnects:?}");
        }
    }
}

/// Replays `account`'s journal: the handler hears what it heard live, the codec is called alike
/// with the same events, effects and counts after each call, and it asks for every frame and
/// request the journal holds, what it asks for beyond them being the commands the buckets held.
fn check_replay(account: &Account, driven: &Driven) {
    let (venue, calls) = Recorded::venue();
    let config = ExecReplayConfig {
        venue,
        cfg: account.cfg.clone(),
        creds: secrets(),
        ns: NS,
        specs: specs(),
        conn: account.conn,
        compare_outbound: true,
    };
    let watch = Watch::default();
    let (mut replay, control): (ExecReplay<Watch>, ExecControl) =
        ExecReplay::new(config, watch.clone()).unwrap();
    let commands: HashMap<RpcId, VenueCommand> = (account.sent.iter())
        .map(|s| (s.rpc, s.cmd.clone()))
        .collect();
    replay.set_commands(move |rpc| commands.get(&rpc).cloned());
    replay
        .run(JournalReader::open(&account.journal.0, SHARD).unwrap())
        .unwrap();
    let who = account.conn as usize;
    assert_eq!(watch.heard(), account.watch.heard(), "{who}");
    let live = account.calls.lock().unwrap().clone();
    let replayed = calls.lock().unwrap().clone();
    assert_eq!(live.len(), replayed.len(), "{who}");
    for (at, (a, b)) in live.iter().zip(&replayed).enumerate() {
        assert_eq!(a, b, "{who}: call {at}");
    }
    let report = replay.exact().unwrap();
    assert!(report.differences.is_empty(), "{who}: {report:?}");
    assert_eq!(report.unwritten, driven.held[who].len() as u64, "{who}");
    assert_eq!(report.excluded, 0);
    assert_eq!(replay.counters().unsupplied, 0);
    assert_eq!(
        replay.counters().decode_errors,
        u64::from(who == P),
        "{who}"
    );
    drop(control);
}
