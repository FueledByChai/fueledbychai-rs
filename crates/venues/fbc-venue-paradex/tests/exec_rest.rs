//! FBC-0sc (decisions 0005, 0013, 0014): Paradex's REST resync and order query, as plans.
//!
//! A resync reads `GET /orders` (the open orders) and `GET /positions` in one round, and the
//! two answers decode into one complete resync: `ResyncBegin` with the watermark of the context
//! the requests were built with, a `ResyncOrder` per open order, a `ResyncPosition` per
//! position held and `ResyncEnd`. The Unknown ladder's query reads `GET /orders-history` by our
//! client id, which finds a closed order too, and decodes into a `QueryResult` carrying the
//! query's rpc: the order found, open or terminal, or absent. The requests carry no session
//! token: they carry only the headers their builder is given (FBC-xvf passes src/auth's). An
//! answer that fails to decode anywhere, the last entry included, pushes nothing.
//!
//! The responses are the hand-built ones in `fixtures/paradex/exec/rest-*.json`, in the shapes
//! docs.paradex.trade documents (SYNTHETIC: their account is the made-up bytes a0..bf).

mod md;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountKey, CancelReason, CidMatch, CidMint, ClientOrderId, DecodeError, Effect, EncodeCtx,
    ExecEvent, ExecSink, Header, HttpAnswer, HttpFailure, HttpMethod, HttpPlan, HttpResponse,
    HttpTag, Lots, MonoNs, Namespace, NamespaceLease, NonceBlock, NotSentReason, OpKind, OrderRef,
    PlanError, PxExact, QueryOrder, RateCharge, RpcId, Side, SignedLots, Ticks, TrafficClass,
    VenueMeta, VenueOrderId, VenueOrderSnapshot, VenueOrderState, WallNs, dispatch,
};
use fbc_venue_paradex::exec::{RestAnswer, ResyncTags, query_plan, resync_plan};
use fbc_venue_paradex::factory::caps_with_order_entry;
use md::{BTC, ETH};
use serde_json::Value;

const OWN: Namespace = Namespace::new(7);
const ACCOUNT: AccountKey = AccountKey::new(3);
/// 2026-10-05T00:00:00Z in nanoseconds.
const START: WallNs = WallNs(1_759_622_400_000_000_000);
/// The REST base the requests are built under.
const BASE: &str = "https://api.testnet.paradex.trade/v1";
const TIMEOUT: Duration = Duration::from_secs(5);
const TAGS: ResyncTags = ResyncTags {
    orders: HttpTag(21),
    positions: HttpTag(22),
};
const QUERY_TAG: HttpTag = HttpTag(23);
const RPC: RpcId = RpcId(77);
/// The order id of our order in every fixture.
const OID: &str = "1759500000000000001";

/// The text of exec fixture `name`.
fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/exec")
        .join(name);
    fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Fixture `name` as JSON, for a test to change one field of.
fn json(name: &str) -> Value {
    serde_json::from_slice(&fixture(name)).unwrap()
}

/// A directory of its own for the namespace lease the client ids are minted under.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("fbc-paradex-exec-rest-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        LockDir(dir)
    }
}

impl Drop for LockDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The first two client ids minted in [`OWN`]: the first is the one the fixtures carry.
fn ours() -> [ClientOrderId; 2] {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, START);
    [mint.mint().unwrap(), mint.mint().unwrap()]
}

fn vid(wire: &str) -> VenueOrderId {
    dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.venue_order_id(wire)
    })
    .unwrap()
}

fn ctx() -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(START.0 + 9_000_000_000),
        mono: MonoNs(123),
        nonces: NonceBlock::EMPTY,
    }
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

type Run = (Result<(), PlanError>, Vec<(VenueMeta, ExecEvent)>);

/// Answers `plan`'s requests, in its order, with `answers`, parses them inside the core's
/// order-entry dispatch and pushes what the plan gives: what parsing returned and every event
/// pushed.
fn answer(plan: HttpPlan<RestAnswer>, answers: &[Result<(u16, Vec<u8>), HttpFailure>]) -> Run {
    let tags: Vec<HttpTag> = plan
        .requests()
        .iter()
        .map(|effect| match effect {
            Effect::Http { tag, .. } => *tag,
            other => panic!("not a request: {other:?}"),
        })
        .collect();
    let answers: Vec<HttpAnswer<'_>> = tags
        .iter()
        .zip(answers)
        .map(|(tag, answer)| {
            let resp = answer.as_ref().map(|(status, body)| HttpResponse {
                status: *status,
                headers: &[],
                body,
            });
            (*tag, resp.map_err(|failure| *failure))
        })
        .collect();
    let mut sink = Sink::default();
    let parsed = dispatch(&caps_with_order_entry(), OWN, |scope| {
        plan.parse(&answers, scope)
    });
    let result = parsed.map(|step| {
        let answer = step.done().expect("a plan of one round");
        answer.push_into(&mut sink);
    });
    (result, sink.0)
}

fn ok(body: Vec<u8>) -> Result<(u16, Vec<u8>), HttpFailure> {
    Ok((200, body))
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

/// The resync plan built at [`ctx`] with no headers, answered with `orders` and `positions`.
fn resync(orders: Vec<u8>, positions: Vec<u8>) -> Run {
    let plan = resync_plan(BASE, &[], TIMEOUT, TAGS, &ctx(), &md::specs()).unwrap();
    answer(plan, &[ok(orders), ok(positions)])
}

/// The query for our first order by `target`.
fn query(target: OrderRef) -> QueryOrder {
    QueryOrder {
        target,
        inst: BTC,
        placement_nonce: None,
    }
}

/// The query plan for our first order by its client id, answered with `history`.
fn ask(history: Vec<u8>) -> Run {
    let [cid, _] = ours();
    let plan = query_plan(
        &query(OrderRef::Client(cid)),
        RPC,
        BASE,
        &[],
        TIMEOUT,
        QUERY_TAG,
        &md::specs(),
    )
    .unwrap();
    answer(plan, &[ok(history)])
}

/// The refusal `run` holds, asserting nothing was pushed.
fn refused((result, events): Run) -> PlanError {
    assert!(events.is_empty(), "a refused answer pushed {events:?}");
    result.expect_err("the answer is refused")
}

fn malformed(what: &'static str) -> PlanError {
    PlanError::Decode(DecodeError::Malformed(what))
}

/// The requests of `plan`, in its order, as the runtime would make them.
fn requests<T>(plan: &HttpPlan<T>) -> Vec<&Effect> {
    plan.requests().iter().collect()
}

#[test]
fn the_resync_reads_open_orders_and_positions_with_get_carrying_only_the_headers_given() {
    let given = Header {
        name: "X-Session",
        value: "given".into(),
        redact: true,
    };
    let plan = resync_plan(
        BASE,
        std::slice::from_ref(&given),
        TIMEOUT,
        TAGS,
        &ctx(),
        &md::specs(),
    )
    .unwrap();
    let requests = requests(&plan);
    assert_eq!(requests.len(), 2, "one round of two reads");
    for (effect, (tag, path)) in requests
        .iter()
        .zip([(TAGS.orders, "/orders"), (TAGS.positions, "/positions")])
    {
        let Effect::Http {
            tag: asked,
            req,
            rpc,
            timeout,
            class,
            charge,
        } = effect
        else {
            panic!("not a request: {effect:?}");
        };
        assert_eq!(*asked, tag);
        assert_eq!(req.method, HttpMethod::Get);
        assert_eq!(req.url.as_str(), format!("{BASE}{path}"));
        assert!(req.url.redactions().is_empty());
        assert_eq!(req.headers, vec![given.clone()], "only the headers given");
        assert!(req.body.bytes().is_empty());
        assert_eq!(*rpc, None, "a resync is no order-entry request");
        assert_eq!(*timeout, TIMEOUT);
        assert_eq!(*class, TrafficClass::Safety);
        assert_eq!(*charge, RateCharge::one(OpKind::Query, None));
    }

    // Built with none, a request carries no header at all: the token is the caller's to add.
    let bare = resync_plan(BASE, &[], TIMEOUT, TAGS, &ctx(), &md::specs()).unwrap();
    for effect in bare.requests() {
        let Effect::Http { req, .. } = effect else {
            panic!("not a request: {effect:?}");
        };
        assert!(req.headers.is_empty());
    }

    // The two reads must be told apart by their tags.
    let same = ResyncTags {
        orders: HttpTag(5),
        positions: HttpTag(5),
    };
    let err = resync_plan(BASE, &[], TIMEOUT, same, &ctx(), &md::specs()).unwrap_err();
    assert_eq!(err, PlanError::DuplicateTag(HttpTag(5)));
}

#[test]
fn the_query_reads_the_orders_history_by_our_client_id_under_the_querys_rpc() {
    let [cid, _] = ours();
    let given = Header {
        name: "X-Session",
        value: "given".into(),
        redact: true,
    };
    for target in [OrderRef::Client(cid), OrderRef::Both(cid, vid(OID))] {
        let plan = query_plan(
            &query(target),
            RPC,
            BASE,
            std::slice::from_ref(&given),
            TIMEOUT,
            QUERY_TAG,
            &md::specs(),
        )
        .unwrap();
        let [
            Effect::Http {
                tag,
                req,
                rpc,
                timeout,
                class,
                charge,
            },
        ] = plan.requests()
        else {
            panic!("one request: {:?}", plan.requests());
        };
        assert_eq!(*tag, QUERY_TAG);
        assert_eq!(req.method, HttpMethod::Get);
        assert_eq!(
            req.url.as_str(),
            format!("{BASE}/orders-history?client_id=01000700-199b-81ab-8200-00054d0aa3f5")
        );
        assert_eq!(req.headers, vec![given.clone()]);
        assert!(req.body.bytes().is_empty());
        assert_eq!(*rpc, Some(RPC), "the query names its request");
        assert_eq!(*timeout, TIMEOUT);
        assert_eq!(
            *class,
            TrafficClass::Safety,
            "a query rides the safety floor"
        );
        assert_eq!(*charge, RateCharge::one(OpKind::Query, Some(BTC)));
    }
}

#[test]
fn a_query_that_names_no_client_id_is_not_sent() {
    // Paradex's caps declare queries by client id only (0054): a venue id alone is refused.
    let err = query_plan(
        &query(OrderRef::Venue(vid(OID))),
        RPC,
        BASE,
        &[],
        TIMEOUT,
        QUERY_TAG,
        &md::specs(),
    )
    .unwrap_err();
    assert_eq!(err, NotSentReason::Unsupported);
}

#[test]
fn a_query_for_an_instrument_the_table_does_not_hold_is_not_sent() {
    let [cid, _] = ours();
    let mut other = query(OrderRef::Client(cid));
    other.inst = fbc_core::InstrumentId::new(99);
    let err = query_plan(&other, RPC, BASE, &[], TIMEOUT, QUERY_TAG, &md::specs()).unwrap_err();
    assert_eq!(err, NotSentReason::Unencodable);
}

#[test]
fn an_answer_shows_the_events_it_pushes_in_order() {
    let plan = resync_plan(BASE, &[], TIMEOUT, TAGS, &ctx(), &md::specs()).unwrap();
    let tags = [TAGS.orders, TAGS.positions];
    let (orders, positions) = (
        fixture("rest-orders-open.json"),
        fixture("rest-positions.json"),
    );
    let answers: Vec<HttpAnswer<'_>> = tags
        .iter()
        .zip([&orders, &positions])
        .map(|(tag, body)| {
            let resp = HttpResponse {
                status: 200,
                headers: &[],
                body,
            };
            (*tag, Ok(resp))
        })
        .collect();
    let answer = dispatch(&caps_with_order_entry(), OWN, |scope| {
        plan.parse(&answers, scope)
    })
    .unwrap()
    .done()
    .unwrap();
    let shown = answer.events().to_vec();
    let mut sink = Sink::default();
    answer.push_into(&mut sink);
    assert_eq!(shown, sink.0);
    assert_eq!(shown.len(), 7);
}

#[test]
fn open_orders_and_positions_decode_into_one_complete_resync_with_the_requests_watermark() {
    let [cid, _] = ours();
    let (result, events) = resync(
        fixture("rest-orders-open.json"),
        fixture("rest-positions.json"),
    );
    result.unwrap();
    assert!(
        events.iter().all(|(meta, _)| *meta == VenueMeta::NONE),
        "a REST snapshot carries no venue sequence"
    );
    let events: Vec<ExecEvent> = events.into_iter().map(|(_, ev)| ev).collect();
    let foreign = dispatch(&caps_with_order_entry(), OWN, |scope| {
        scope.client_order_id("3f2504e0-4f89-41d3-9a0c-0305e82c3301")
    });
    // A random UUID is no canonical client id: not ours.
    assert_eq!(foreign, CidMatch::Unparseable);
    assert_eq!(
        events,
        vec![
            ExecEvent::ResyncBegin {
                watermark: ctx().wall
            },
            // Ours: a reduce-only GTC sell, 0.05 of 0.15 filled.
            ExecEvent::ResyncOrder(VenueOrderSnapshot {
                cid: Some(CidMatch::Ours(cid)),
                vid: vid(OID),
                inst: BTC,
                side: Side::Sell,
                state: VenueOrderState::Open,
                px: Some(Ticks(620_000)),
                qty: Lots::new(150).unwrap(),
                cum_filled: Lots::new(50).unwrap(),
                post_only: Some(false),
                reduce_only: Some(true),
            }),
            // Not ours (a random UUID); NEW is resting too.
            ExecEvent::ResyncOrder(VenueOrderSnapshot {
                cid: Some(foreign),
                vid: vid("1759500000000000002"),
                inst: ETH,
                side: Side::Buy,
                state: VenueOrderState::Open,
                px: Some(Ticks(30_005)),
                qty: Lots::new(1250).unwrap(),
                cum_filled: Lots::new(0).unwrap(),
                post_only: Some(true),
                reduce_only: Some(false),
            }),
            // No client id; RPI is post-only (0054).
            ExecEvent::ResyncOrder(VenueOrderSnapshot {
                cid: None,
                vid: vid("1759500000000000003"),
                inst: BTC,
                side: Side::Buy,
                state: VenueOrderState::Open,
                px: Some(Ticks(619_905)),
                qty: Lots::new(50).unwrap(),
                cum_filled: Lots::new(0).unwrap(),
                post_only: Some(true),
                reduce_only: Some(false),
            }),
            // Signed lots, the average entry exactly as sent; the closed SOL position, in a
            // market the table does not hold, is flat and not listed.
            ExecEvent::ResyncPosition {
                inst: BTC,
                qty: SignedLots(150),
                avg_entry: Some(PxExact::new(6_123_456_789_012, -8)),
            },
            ExecEvent::ResyncPosition {
                inst: ETH,
                qty: SignedLots(-50),
                avg_entry: Some(PxExact::new(300_005, -2)),
            },
            ExecEvent::ResyncEnd,
        ]
    );
}

#[test]
fn an_account_with_nothing_open_resyncs_to_an_empty_snapshot() {
    let empty = br#"{"results": []}"#.to_vec();
    let (result, events) = resync(empty.clone(), empty);
    result.unwrap();
    let events: Vec<ExecEvent> = events.into_iter().map(|(_, ev)| ev).collect();
    assert_eq!(
        events,
        vec![
            ExecEvent::ResyncBegin {
                watermark: ctx().wall
            },
            ExecEvent::ResyncEnd
        ]
    );
}

/// The `QueryResult` `run` pushed: its rpc, target and what it found.
fn query_result(run: Run) -> (RpcId, OrderRef, Option<VenueOrderSnapshot>) {
    let (result, events) = run;
    result.unwrap();
    match <[_; 1]>::try_from(events) {
        Ok([(meta, ExecEvent::QueryResult(answer))]) => {
            assert_eq!(meta, VenueMeta::NONE);
            assert_eq!(
                ExecEvent::QueryResult(answer.clone()).answers(),
                Some(RPC),
                "the answer clears the query's deadline"
            );
            (
                answer.rpc(),
                answer.target().clone(),
                answer.found().cloned(),
            )
        }
        Ok(other) => panic!("not a query result: {other:?}"),
        Err(events) => panic!("{} events: {events:?}", events.len()),
    }
}

/// Our first order as the history fixtures show it, in `state` with `cum_filled` lots filled.
fn history_order(state: VenueOrderState, cum_filled: i64) -> VenueOrderSnapshot {
    let [cid, _] = ours();
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid)),
        vid: vid(OID),
        inst: BTC,
        side: Side::Buy,
        state,
        px: Some(Ticks(620_000)),
        qty: Lots::new(150).unwrap(),
        cum_filled: Lots::new(cum_filled).unwrap(),
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

#[test]
fn a_filled_order_answers_the_query_with_its_terminal_snapshot() {
    let [cid, _] = ours();
    let (rpc, target, found) = query_result(ask(fixture("rest-orders-history-filled.json")));
    assert_eq!(rpc, RPC);
    assert_eq!(target, OrderRef::Client(cid));
    assert_eq!(found, Some(history_order(VenueOrderState::Filled, 150)));
}

#[test]
fn a_cancelled_order_answers_the_query_with_its_terminal_snapshot() {
    let (_, _, found) = query_result(ask(fixture("rest-orders-history-canceled.json")));
    let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
    assert_eq!(found, Some(history_order(cancelled, 50)));
}

#[test]
fn an_open_order_answers_the_query_found_resting() {
    let (_, _, found) = query_result(ask(fixture("rest-orders-history-open.json")));
    assert_eq!(found, Some(history_order(VenueOrderState::Open, 50)));
}

#[test]
fn an_order_the_history_does_not_hold_answers_the_query_as_absent() {
    let [cid, _] = ours();
    let (rpc, target, found) = query_result(ask(fixture("rest-orders-history-empty.json")));
    assert_eq!((rpc, target, found), (RPC, OrderRef::Client(cid), None));
}

#[test]
fn a_query_by_both_ids_is_answered_for_that_order_only() {
    let [cid, _] = ours();
    let run = |oid: &str| {
        let plan = query_plan(
            &query(OrderRef::Both(cid, vid(oid))),
            RPC,
            BASE,
            &[],
            TIMEOUT,
            QUERY_TAG,
            &md::specs(),
        )
        .unwrap();
        answer(plan, &[ok(fixture("rest-orders-history-filled.json"))])
    };
    let (_, target, found) = query_result(run(OID));
    assert_eq!(target, OrderRef::Both(cid, vid(OID)));
    assert_eq!(found, Some(history_order(VenueOrderState::Filled, 150)));
    // The history shows our client id under another venue id: not this order.
    let err = refused(run("1759500000000000009"));
    assert_eq!(
        err,
        malformed("orders-history: the order is not the one queried")
    );
}

#[test]
fn a_positions_answer_that_fails_on_its_last_entry_pushes_nothing() {
    // The open orders and the first position decode; the second position's size is off the
    // step. Nothing of the resync is pushed, not even its begin.
    let err = refused(resync(
        fixture("rest-orders-open.json"),
        fixture("rest-positions-bad-second.json"),
    ));
    assert_eq!(
        err,
        PlanError::Decode(DecodeError::Malformed(
            "size off the instrument's size step"
        ))
    );
}

#[test]
fn an_orders_answer_that_fails_on_its_last_entry_pushes_nothing() {
    let mut orders = json("rest-orders-open.json");
    orders["results"][2]["type"] = "STOP_LIMIT".into();
    let err = refused(resync(bytes(&orders), fixture("rest-positions.json")));
    assert_eq!(err, malformed("order type not modelled"));
}

#[test]
fn a_resync_with_a_failed_or_refused_read_pushes_nothing() {
    let orders = fixture("rest-orders-open.json");
    let positions = fixture("rest-positions.json");
    let plan = || resync_plan(BASE, &[], TIMEOUT, TAGS, &ctx(), &md::specs()).unwrap();

    let err = refused(answer(
        plan(),
        &[ok(orders.clone()), Err(HttpFailure::TimedOut)],
    ));
    assert_eq!(
        err,
        PlanError::Http {
            tag: TAGS.positions,
            failure: HttpFailure::TimedOut
        }
    );
    let err = refused(answer(plan(), &[Ok((401, orders)), ok(positions)]));
    assert_eq!(
        err,
        PlanError::Status {
            tag: TAGS.orders,
            status: 401
        }
    );
}

/// The open orders fixture with `change` made to its first order.
fn orders_with(change: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut orders = json("rest-orders-open.json");
    change(&mut orders["results"][0]);
    bytes(&orders)
}

#[test]
fn an_open_order_the_snapshot_cannot_state_refuses_the_resync() {
    let positions = || fixture("rest-positions.json");
    let cases: Vec<(Vec<u8>, PlanError)> = vec![
        (b"not json".to_vec(), malformed("orders: not a JSON object")),
        (b"[]".to_vec(), malformed("orders: not a JSON object")),
        (br#"{"next": null}"#.to_vec(), malformed("orders: results")),
        (
            br#"{"results": [7]}"#.to_vec(),
            malformed("order: not a JSON object"),
        ),
        (
            orders_with(|o| o["market"] = "DOGE-USD-PERP".into()),
            PlanError::Decode(DecodeError::UnknownInstrument),
        ),
        (
            orders_with(|o| {
                o.as_object_mut().unwrap().remove("market");
            }),
            malformed("order market"),
        ),
        (
            orders_with(|o| o["id"] = "".into()),
            PlanError::Decode(DecodeError::IdRefused(fbc_core::IdError::Empty)),
        ),
        (orders_with(|o| o["id"] = 7.into()), malformed("order id")),
        (
            orders_with(|o| o["side"] = "LONG".into()),
            malformed("order side"),
        ),
        (
            orders_with(|o| o["type"] = "TAKE_PROFIT_LIMIT".into()),
            malformed("order type not modelled"),
        ),
        (
            orders_with(|o| o["instruction"] = "FOK".into()),
            malformed("order instruction"),
        ),
        (
            orders_with(|o| o["status"] = "UNTRIGGERED".into()),
            malformed("order status not modelled"),
        ),
        (
            orders_with(|o| o["status"] = "CLOSED".into()),
            malformed("orders: an order that is not open"),
        ),
        (
            orders_with(|o| o["price"] = "62000.05".into()),
            malformed("order price"),
        ),
        (
            orders_with(|o| o["price"] = "0".into()),
            malformed("order price"),
        ),
        (
            orders_with(|o| o["price"] = "abc".into()),
            malformed("order price"),
        ),
        (
            orders_with(|o| {
                o["type"] = "MARKET".into();
                o["price"] = "62000".into();
            }),
            malformed("market order with a price"),
        ),
        (
            orders_with(|o| o["size"] = "0.1505".into()),
            malformed("size off the instrument's size step"),
        ),
        (
            orders_with(|o| o["size"] = "x".into()),
            malformed("order size"),
        ),
        (
            orders_with(|o| o["remaining_size"] = "0.2".into()),
            malformed("order remaining size above its size"),
        ),
        (
            orders_with(|o| o["remaining_size"] = Value::Null),
            malformed("order remaining size"),
        ),
        (
            orders_with(|o| o["flags"] = "REDUCE_ONLY".into()),
            malformed("order flags"),
        ),
        (
            orders_with(|o| o["flags"] = serde_json::json!([1])),
            malformed("order flags"),
        ),
        (
            orders_with(|o| o["client_id"] = 5.into()),
            malformed("order client id"),
        ),
        (
            orders_with(|o| o["cancel_reason"] = 5.into()),
            malformed("order cancel reason"),
        ),
    ];
    for (orders, expected) in cases {
        let err = refused(resync(orders, positions()));
        assert_eq!(err, expected);
    }
}

#[test]
fn an_open_order_states_what_its_optional_fields_leave_out() {
    // No flags at all: reduce-only is not echoed. A market order's price is 0, so none; a
    // missing client id and cancel reason are none.
    let orders = orders_with(|o| {
        let o = o.as_object_mut().unwrap();
        o.remove("flags");
        o.remove("client_id");
        o.remove("cancel_reason");
        o.insert("type".into(), "MARKET".into());
        o.insert("instruction".into(), "IOC".into());
        o.insert("price".into(), "0".into());
    });
    let (result, events) = resync(orders, fixture("rest-positions.json"));
    result.unwrap();
    let Some((_, ExecEvent::ResyncOrder(snap))) = events.get(1) else {
        panic!("no order: {events:?}");
    };
    assert_eq!(snap.cid, None);
    assert_eq!(snap.px, None);
    assert_eq!(snap.reduce_only, None);
    assert_eq!(snap.post_only, Some(false));
    // And null flags are no flags echoed either.
    let orders = orders_with(|o| o["flags"] = Value::Null);
    let (result, events) = resync(orders, fixture("rest-positions.json"));
    result.unwrap();
    let Some((_, ExecEvent::ResyncOrder(snap))) = events.get(1) else {
        panic!("no order: {events:?}");
    };
    assert_eq!(snap.reduce_only, None);
}

/// The positions fixture with `change` made to its first position.
fn positions_with(change: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut positions = json("rest-positions.json");
    change(&mut positions["results"][0]);
    bytes(&positions)
}

#[test]
fn a_position_the_snapshot_cannot_state_refuses_the_resync() {
    let orders = || fixture("rest-orders-open.json");
    let cases: Vec<(Vec<u8>, PlanError)> = vec![
        (b"{}".to_vec(), malformed("positions: results")),
        (b"7".to_vec(), malformed("positions: not a JSON object")),
        (
            br#"{"results": ["x"]}"#.to_vec(),
            malformed("position: not a JSON object"),
        ),
        (
            positions_with(|p| p["market"] = "DOGE-USD-PERP".into()),
            PlanError::Decode(DecodeError::UnknownInstrument),
        ),
        (
            positions_with(|p| p["market"] = Value::Null),
            malformed("position market"),
        ),
        (
            positions_with(|p| p["side"] = "SHORT".into()),
            malformed("position side"),
        ),
        (
            positions_with(|p| p["side"] = "BUY".into()),
            malformed("position side"),
        ),
        (
            positions_with(|p| p["status"] = "CLOSED".into()),
            malformed("closed position with a size"),
        ),
        (
            positions_with(|p| p["status"] = "LIQUIDATED".into()),
            malformed("position status"),
        ),
        (
            positions_with(|p| p["size"] = "0.15x".into()),
            malformed("position size"),
        ),
        (
            positions_with(|p| p["average_entry_price"] = "0".into()),
            malformed("position average entry"),
        ),
        (
            positions_with(|p| p["average_entry_price"] = "-1".into()),
            malformed("position average entry"),
        ),
        (
            positions_with(|p| {
                p.as_object_mut().unwrap().remove("average_entry_price");
            }),
            malformed("position average entry"),
        ),
        (
            positions_with(|p| p["market"] = "ETH-USD-PERP".into()),
            malformed("positions: a market listed twice"),
        ),
    ];
    for (positions, expected) in cases {
        let err = refused(resync(orders(), positions));
        assert_eq!(err, expected);
    }
}

#[test]
fn a_flat_position_is_not_listed_whatever_its_side_or_status() {
    // Zero, signed or not, open or closed, on either side: flat, so not listed (an instrument
    // not listed is flat, 0049); its average entry is not read.
    for (size, side, status) in [
        ("0", "LONG", "OPEN"),
        ("-0", "SHORT", "CLOSED"),
        ("0.000", "SHORT", "OPEN"),
    ] {
        let positions = positions_with(|p| {
            p["size"] = size.into();
            p["side"] = side.into();
            p["status"] = status.into();
            p["average_entry_price"] = Value::Null;
        });
        let (result, events) = resync(fixture("rest-orders-open.json"), positions);
        result.unwrap();
        let listed: Vec<_> = events
            .iter()
            .filter_map(|(_, ev)| match ev {
                ExecEvent::ResyncPosition { inst, .. } => Some(*inst),
                _ => None,
            })
            .collect();
        assert_eq!(listed, vec![ETH], "{size} {side} {status}");
    }
}

/// The filled history fixture with `change` made to its order list.
fn history_with(change: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut history = json("rest-orders-history-filled.json");
    change(&mut history["results"]);
    bytes(&history)
}

#[test]
fn a_history_answer_that_names_another_or_no_single_order_is_refused() {
    let [_, second] = ours();
    let wire_second = {
        let caps = caps_with_order_entry();
        let format = caps.exec.unwrap().order.client_id;
        fbc_core::encode_cid(&format, second).unwrap()
    };
    let cases: Vec<(Vec<u8>, PlanError)> = vec![
        (
            b"null".to_vec(),
            malformed("orders-history: not a JSON object"),
        ),
        (
            br#"{"results": {}}"#.to_vec(),
            malformed("orders-history: results"),
        ),
        (
            history_with(|r| {
                let first = r[0].clone();
                r.as_array_mut().unwrap().push(first);
            }),
            malformed("orders-history: more than one order"),
        ),
        (
            history_with(|r| r[0]["client_id"] = wire_second.as_str().into()),
            malformed("orders-history: the order is not the one queried"),
        ),
        (
            history_with(|r| r[0]["client_id"] = "".into()),
            malformed("orders-history: the order is not the one queried"),
        ),
        (
            history_with(|r| r[0]["market"] = "ETH-USD-PERP".into()),
            malformed("orders-history: the order is in another market"),
        ),
        (
            history_with(|r| r[0]["status"] = "UNTRIGGERED".into()),
            malformed("order status not modelled"),
        ),
    ];
    for (history, expected) in cases {
        let err = refused(ask(history));
        assert_eq!(err, expected);
    }
}

#[test]
fn a_closed_order_reports_how_it_closed() {
    let found = |status: &str, remaining: &str, reason: &str| {
        let history = history_with(|r| {
            r[0]["status"] = status.into();
            r[0]["remaining_size"] = remaining.into();
            r[0]["cancel_reason"] = reason.into();
        });
        query_result(ask(history)).2.unwrap().state
    };
    assert_eq!(
        found("CLOSED", "0", "POST_ONLY_WOULD_CROSS"),
        VenueOrderState::Canceled(CancelReason::PostOnly)
    );
    assert_eq!(
        found("CLOSED", "0.1", "NOT_ENOUGH_MARGIN"),
        VenueOrderState::Canceled(CancelReason::Venue)
    );
    assert_eq!(
        found("CLOSED", "0.1", ""),
        VenueOrderState::Canceled(CancelReason::Venue)
    );
    assert_eq!(found("NEW", "0.15", ""), VenueOrderState::Open);
}
