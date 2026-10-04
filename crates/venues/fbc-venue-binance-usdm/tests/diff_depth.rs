//! FBC-tfb's done line: the diff-depth book (`<symbol>@depth@100ms`) is anchored on a REST
//! snapshot as Binance documents ("How to manage a local order book correctly"). The clean
//! fixture yields one snapshot request carrying its documented weight, one complete snapshot and
//! then exactly the expected deltas; the duplicate is ignored without a gap; the gap and the
//! swapped pair each push a Gap followed by a new snapshot request and a fresh snapshot under a
//! new epoch; and a TimedOut snapshot request is retried on its timer. Error paths follow.
//!
//! In every fixture the REST snapshot answers after the first two frames, which the codec
//! buffers: the first is older than the snapshot and dropped, the second straddles it.

mod common;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use common::{BTC, ETH, Sink, config, fixture, rest_fixture, specs};
use fbc_core::{
    BookSide, DecodeError, Effect, Effects, EndpointPlan, ExchNs, ExchTsKind, Feed, FeedHealth,
    HttpFailure, HttpMethod, HttpResponse, HttpTag, InstrumentId, Lots, MdCodec, MdEvent, MonoNs,
    OpKind, RateCharge, RawFrame, SpecTable, Subscription, Ticks, TimerTag, TrafficClass,
    VenueFactory, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_binance_usdm::{BOOK_DIFF, BinanceUsdm, rest_depth_weight};

const SNAPSHOT_URL: &str = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000";

fn diff(inst: InstrumentId) -> Subscription {
    Subscription {
        inst,
        feed: Feed::Book(BOOK_DIFF),
    }
}

fn plan() -> EndpointPlan {
    let subs = BTreeSet::from([diff(BTC), diff(ETH)]);
    BinanceUsdm
        .plan_md(&config(), &specs(), &subs)
        .unwrap()
        .remove(0)
}

/// A codec that has subscribed BTC's diff-depth book.
fn subscribed() -> Box<dyn MdCodec> {
    let plan = plan();
    let mut codec = BinanceUsdm.md_codec(&config(), &plan);
    let mut fx = Effects::new();
    codec
        .subscribe(&[diff(BTC)], &[], &specs(), &mut fx)
        .unwrap();
    codec
}

/// Decodes `frame` through the core's market-data dispatch.
fn frame(codec: &mut dyn MdCodec, frame: &str) -> (Result<(), DecodeError>, Sink, Effects) {
    frame_with(codec, frame, &specs())
}

fn frame_with(
    codec: &mut dyn MdCodec,
    frame: &str,
    specs: &SpecTable,
) -> (Result<(), DecodeError>, Sink, Effects) {
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(RawFrame::Text(frame), scope, specs, &mut sink, &mut fx)
    });
    (result, sink, fx)
}

/// Hands the codec `resp` for the request tagged `tag`.
fn http(
    codec: &mut dyn MdCodec,
    tag: HttpTag,
    resp: Result<HttpResponse<'_>, HttpFailure>,
) -> (Result<(), DecodeError>, Sink, Effects) {
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_http(tag, resp, scope, &specs(), &mut sink, &mut fx)
    });
    (result, sink, fx)
}

fn ok(body: &str) -> Result<HttpResponse<'_>, HttpFailure> {
    Ok(HttpResponse {
        status: 200,
        headers: &[],
        body: body.as_bytes(),
    })
}

/// The tag of the one effect in `fx`, a snapshot request for BTC as documented: GET
/// /fapi/v1/depth at the configured limit, its timeout, normal traffic, no rpc, and the
/// documented weight of that limit against the REST budget.
fn snapshot_request(fx: &Effects) -> HttpTag {
    let [
        Effect::Http {
            tag,
            req,
            rpc,
            timeout,
            class,
            charge,
        },
    ] = fx.as_slice()
    else {
        panic!("one snapshot request: {fx:?}")
    };
    assert_eq!(req.method, HttpMethod::Get);
    assert_eq!(req.url.as_str(), SNAPSHOT_URL);
    assert!(req.headers.is_empty() && req.body.bytes().is_empty());
    assert_eq!(*rpc, None);
    assert_eq!(*timeout, Duration::from_secs(5));
    assert_eq!(*class, TrafficClass::Normal);
    let weight = rest_depth_weight(1000).unwrap();
    assert_eq!(weight, NonZeroU32::new(20).unwrap());
    assert_eq!(
        *charge,
        RateCharge {
            op: OpKind::Rest,
            inst: Some(BTC),
            weight,
        }
    );
    *tag
}

fn meta(u: u64, t_ms: i64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(t_ms * 1_000_000)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(u),
    }
}

fn level(side: BookSide, px: i64, qty: i64) -> MdEvent {
    MdEvent::Level {
        inst: BTC,
        book: BOOK_DIFF,
        side,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

const BID: BookSide = BookSide::Bid;
const ASK: BookSide = BookSide::Ask;

fn begin(epoch: u32) -> MdEvent {
    MdEvent::BookSnapshotBegin {
        inst: BTC,
        book: BOOK_DIFF,
        epoch,
    }
}

const END: MdEvent = MdEvent::BookSnapshotEnd {
    inst: BTC,
    book: BOOK_DIFF,
};

const GAP: MdEvent = MdEvent::Health {
    inst: BTC,
    feed: Feed::Book(BOOK_DIFF),
    h: FeedHealth::Gap,
};

/// `rest/depth_snapshot.json` under epoch `epoch`, then e1's deltas (e0 is dropped).
fn first_snapshot(epoch: u32) -> Vec<(VenueMeta, MdEvent)> {
    let snap = meta(1_027_024, 1_589_436_922_959);
    let e1 = meta(1_027_030, 1_589_436_922_960);
    vec![
        (snap, begin(epoch)),
        (snap, level(BID, 740_540, 4_310)),
        (snap, level(BID, 740_530, 1_000)),
        (snap, level(ASK, 740_550, 1_200)),
        (snap, level(ASK, 740_560, 3_000)),
        (snap, END),
        (e1, level(BID, 740_540, 4_000)),
        (e1, level(ASK, 740_550, 0)),
    ]
}

fn e2() -> Vec<(VenueMeta, MdEvent)> {
    vec![(meta(1_027_040, 1_589_436_923_060), level(ASK, 740_570, 500))]
}

fn e3() -> Vec<(VenueMeta, MdEvent)> {
    vec![(meta(1_027_055, 1_589_436_923_160), level(BID, 740_530, 0))]
}

fn e4() -> Vec<(VenueMeta, MdEvent)> {
    vec![(meta(1_027_060, 1_589_436_923_260), level(BID, 740_545, 800))]
}

/// `rest/depth_snapshot_resync.json` under `epoch`.
fn resync_snapshot(epoch: u32) -> Vec<(VenueMeta, MdEvent)> {
    let snap = meta(1_027_058, 1_589_436_923_259);
    vec![
        (snap, begin(epoch)),
        (snap, level(BID, 740_540, 4_000)),
        (snap, level(ASK, 740_560, 3_000)),
        (snap, level(ASK, 740_570, 500)),
        (snap, END),
    ]
}

/// Runs a fixture's first two frames and the first snapshot: one request, then the snapshot and
/// e1's deltas. Returns the codec and the frames.
fn anchored(name: &str) -> (Box<dyn MdCodec>, Vec<String>) {
    let mut codec = subscribed();
    let frames = fixture(name);
    // e0 starts the buffer and asks for the one snapshot.
    let (result, sink, fx) = frame(codec.as_mut(), &frames[0]);
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty());
    let tag = snapshot_request(&fx);
    // e1 is buffered: nothing pushed, nothing asked.
    let (result, sink, fx) = frame(codec.as_mut(), &frames[1]);
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty() && fx.is_empty());
    let body = rest_fixture("depth_snapshot.json");
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(fx.is_empty());
    assert_eq!(sink.0, first_snapshot(1));
    (codec, frames)
}

/// Feeds `frames` and returns everything pushed and asked for.
fn run(codec: &mut dyn MdCodec, frames: &[String]) -> (Vec<(VenueMeta, MdEvent)>, Vec<Effect>) {
    let (mut events, mut effects) = (Vec::new(), Vec::new());
    for f in frames {
        let (result, sink, mut fx) = frame(codec, f);
        assert_eq!(result, Ok(()), "{f}");
        events.extend(sink.0);
        effects.extend(fx.take());
    }
    (events, effects)
}

#[test]
fn the_diff_depth_stream_is_subscribed_by_name_and_its_snapshot_counts_against_the_ip_weight() {
    let plan = plan();
    let mut codec = BinanceUsdm.md_codec(&config(), &plan);
    let mut fx = Effects::new();
    codec.subscribe(&plan.subs, &[], &specs(), &mut fx).unwrap();
    let [Effect::Send { frame: sent, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let sent: serde_json::Value = serde_json::from_slice(sent.bytes()).unwrap();
    assert_eq!(
        sent["params"],
        serde_json::json!(["btcusdt@depth@100ms", "ethusdt@depth@100ms"])
    );
    // The snapshot request is charged to the REST weight budget per IP, and nothing else.
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    snapshot_request(&fx);
    let Some((charge, via)) = fx.as_slice()[0].charge() else {
        panic!("charged")
    };
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let counted: Vec<_> = caps
        .limits
        .iter()
        .filter(|l| l.counts(&charge, via))
        .map(|l| (l.scope, l.units))
        .collect();
    assert_eq!(counted, [(fbc_core::LimitScope::Ip, 2400)]);
}

#[test]
fn the_clean_fixture_yields_one_weighted_snapshot_request_one_snapshot_and_the_deltas() {
    let (mut codec, frames) = anchored("diff_depth.jsonl");
    let (events, effects) = run(codec.as_mut(), &frames[2..]);
    assert!(effects.is_empty(), "{effects:?}");
    assert_eq!(events, [e2(), e3(), e4()].concat());
}

#[test]
fn a_duplicate_event_is_ignored_without_a_gap() {
    let (mut codec, frames) = anchored("diff_depth_duplicate.jsonl");
    let (events, effects) = run(codec.as_mut(), &frames[2..]);
    assert!(effects.is_empty(), "{effects:?}");
    // e2, e2 again (ignored), e3.
    assert_eq!(events, [e2(), e3()].concat());
}

/// After a break: a Gap with the offending frame's metadata, one new snapshot request, and on
/// its response a fresh snapshot under epoch 2 followed by the buffered event that straddles it.
fn resyncs_after_a_break(name: &str, breaking: usize, before: Vec<(VenueMeta, MdEvent)>) {
    let (mut codec, frames) = anchored(name);
    let (events, effects) = run(codec.as_mut(), &frames[2..breaking]);
    assert!(effects.is_empty());
    assert_eq!(events, before);
    let (result, sink, fx) = frame(codec.as_mut(), &frames[breaking]);
    assert_eq!(result, Ok(()));
    let offending: VenueMeta = serde_json::from_str::<serde_json::Value>(&frames[breaking])
        .map(|v| {
            meta(
                v["data"]["u"].as_u64().unwrap(),
                v["data"]["T"].as_i64().unwrap(),
            )
        })
        .unwrap();
    assert_eq!(sink.0, [(offending, GAP)]);
    let tag = snapshot_request(&fx);
    // The frames after the break are buffered for the new anchor.
    let (events, effects) = run(codec.as_mut(), &frames[breaking + 1..]);
    assert!(events.is_empty() && effects.is_empty());
    let body = rest_fixture("depth_snapshot_resync.json");
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(fx.is_empty());
    // Buffered events below lastUpdateId 1027058 are dropped; e4 (1027056..=1027060) straddles.
    assert_eq!(sink.0, [resync_snapshot(2), e4()].concat());
}

#[test]
fn a_gap_pushes_a_gap_then_a_new_snapshot_request_and_a_fresh_snapshot_under_a_new_epoch() {
    // e0 e1 [snapshot] e2 e4: e4's pu (1027055) is not e2's u (1027040).
    resyncs_after_a_break("diff_depth_gap.jsonl", 3, e2());
}

#[test]
fn a_swapped_pair_pushes_a_gap_then_a_new_snapshot_request_and_a_fresh_snapshot() {
    // e0 e1 [snapshot] e3 e2 e4: e3's pu (1027040) is not e1's u (1027030).
    resyncs_after_a_break("diff_depth_swapped.jsonl", 2, Vec::new());
}

#[test]
fn a_timed_out_snapshot_request_is_retried_on_its_timer() {
    let mut codec = subscribed();
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let first = snapshot_request(&fx);
    let (result, sink, fx) = http(codec.as_mut(), first, Err(HttpFailure::TimedOut));
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty());
    let [Effect::Timer { tag: timer, after }] = fx.as_slice() else {
        panic!("one retry timer: {fx:?}")
    };
    assert_eq!(*after, Duration::from_secs(2));
    // Frames while waiting are buffered, and ask for nothing.
    let (events, effects) = run(codec.as_mut(), &frames[1..2]);
    assert!(events.is_empty() && effects.is_empty());
    // Another timer's firing does nothing.
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    codec.on_timer(
        TimerTag(timer.0 + 100),
        MonoNs(1),
        WallNs(1),
        &mut sink,
        &mut fx,
    );
    assert!(sink.0.is_empty() && fx.is_empty());
    // The retry timer asks again.
    codec.on_timer(*timer, MonoNs(2), WallNs(2), &mut sink, &mut fx);
    assert!(sink.0.is_empty());
    let second = snapshot_request(&fx);
    assert_ne!(second, first);
    // A late answer to the first request is refused, and changes nothing.
    let body = rest_fixture("depth_snapshot.json");
    let (result, sink, fx) = http(codec.as_mut(), first, ok(&body));
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "a response to no pending snapshot request"
        ))
    );
    assert!(sink.0.is_empty() && fx.is_empty());
    // The retry's answer anchors the book on the frame buffered while waiting.
    let (result, sink, _) = http(codec.as_mut(), second, ok(&body));
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0, first_snapshot(1));
    // The timer, already fired, does nothing again.
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    codec.on_timer(*timer, MonoNs(3), WallNs(3), &mut sink, &mut fx);
    assert!(sink.0.is_empty() && fx.is_empty());
}

#[test]
fn every_other_failure_and_a_refused_or_unreadable_response_are_retried_on_the_timer() {
    let body = rest_fixture("depth_snapshot.json");
    let edit = |from: &str, to: &str| Ok((200, body.replace(from, to)));
    let malformed = |what| Err(DecodeError::Malformed(what));
    type Case = (Result<(u16, String), HttpFailure>, Result<(), DecodeError>);
    let cases: Vec<Case> = vec![
        (Err(HttpFailure::NotSent), Ok(())),
        (Err(HttpFailure::Lost), Ok(())),
        (
            Ok((429, "{}".into())),
            malformed("the venue refused the depth snapshot request"),
        ),
        (Ok((200, "not json".into())), malformed("not JSON")),
        (Ok((200, "[]".into())), malformed("not a JSON object")),
        (
            edit(r#""lastUpdateId":1027024"#, r#""lastUpdateId":"x""#),
            malformed("lastUpdateId"),
        ),
        (edit(r#""T":1589436922959"#, r#""T":null"#), malformed("T")),
        (edit(r#""bids":"#, r#""x":"#), malformed("bids")),
        (
            edit(r#"["7405.60","3.000"]"#, r#"["7405.605","3.000"]"#),
            malformed("asks"),
        ),
        (
            edit(r#"["7405.30","1.000"]"#, r#"["7405.30"]"#),
            malformed("bids"),
        ),
    ];
    for (resp, want) in cases {
        let mut codec = subscribed();
        let frames = fixture("diff_depth.jsonl");
        let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
        let tag = snapshot_request(&fx);
        let resp = match &resp {
            Ok((status, body)) => Ok(HttpResponse {
                status: *status,
                headers: &[],
                body: body.as_bytes(),
            }),
            Err(failure) => Err(*failure),
        };
        let (result, sink, fx) = http(codec.as_mut(), tag, resp);
        assert_eq!(result, want);
        assert!(sink.0.is_empty());
        let [Effect::Timer { tag: timer, after }] = fx.as_slice() else {
            panic!("one retry timer: {fx:?}")
        };
        assert_eq!(*after, Duration::from_secs(2));
        let (mut sink, mut fx) = (Sink::default(), Effects::new());
        codec.on_timer(*timer, MonoNs(1), WallNs(1), &mut sink, &mut fx);
        snapshot_request(&fx);
    }
}

#[test]
fn a_snapshot_deeper_than_the_configured_limit_is_refused_and_retried() {
    let mut cfg = config();
    cfg.insert(fbc_venue_binance_usdm::KEY_SNAPSHOT_LIMIT, "5");
    let mut codec = BinanceUsdm.md_codec(&cfg, &plan());
    let mut fx = Effects::new();
    codec
        .subscribe(&[diff(BTC)], &[], &specs(), &mut fx)
        .unwrap();
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let [Effect::Http { tag, req, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    assert!(req.url.as_str().ends_with("&limit=5"));
    let deep: Vec<String> = (0..6)
        .map(|i| format!(r#"["7404.{i:02}","1.000"]"#))
        .collect();
    let body = rest_fixture("depth_snapshot.json").replace(
        r#"[["7405.40","4.310"],["7405.30","1.000"]]"#,
        &format!("[{}]", deep.join(",")),
    );
    let (result, sink, fx) = http(codec.as_mut(), *tag, ok(&body));
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "bids: more levels than the limit asked for"
        ))
    );
    assert!(sink.0.is_empty());
    assert!(matches!(fx.as_slice(), [Effect::Timer { .. }]));
    let body = rest_fixture("depth_snapshot.json").replace(
        r#"[["7405.50","1.200"],["7405.60","3.000"]]"#,
        &format!("[{}]", deep.join(",")),
    );
    let mut codec = BinanceUsdm.md_codec(&cfg, &plan());
    codec
        .subscribe(&[diff(BTC)], &[], &specs(), &mut Effects::new())
        .unwrap();
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let [Effect::Http { tag, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let (result, _, _) = http(codec.as_mut(), *tag, ok(&body));
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "asks: more levels than the limit asked for"
        ))
    );
}

/// The one retry timer in `fx`, its interval the configured one.
fn retry_timer(fx: &Effects) -> TimerTag {
    let [Effect::Timer { tag, after }] = fx.as_slice() else {
        panic!("one retry timer: {fx:?}")
    };
    assert_eq!(*after, Duration::from_secs(2));
    *tag
}

/// Fires `timer` and returns the snapshot request it asks for.
fn fire(codec: &mut dyn MdCodec, timer: TimerTag) -> HttpTag {
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    codec.on_timer(timer, MonoNs(1), WallNs(1), &mut sink, &mut fx);
    assert!(sink.0.is_empty());
    snapshot_request(&fx)
}

/// e3 alone after a snapshot at 1027045, which it straddles: the snapshot under `epoch`, then
/// e3's delta.
fn bridged_by_e3(epoch: u32) -> Vec<(VenueMeta, MdEvent)> {
    let snap = meta(1_027_045, 1_589_436_922_959);
    let mut want: Vec<_> = first_snapshot(epoch)[..6]
        .iter()
        .map(|&(_, ev)| (snap, ev))
        .collect();
    want.extend(e3());
    want
}

#[test]
fn a_snapshot_the_buffered_events_do_not_bridge_is_never_published_and_is_retried() {
    // Codex r4177522988: a snapshot older than the first event after it is never published,
    // so no consumer sees it valid.
    let mut codec = subscribed();
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let tag = snapshot_request(&fx);
    // e2 (1027031..=1027040) is buffered; the snapshot is at 1027024, before e2 begins.
    let (events, effects) = run(codec.as_mut(), &frames[2..3]);
    assert!(events.is_empty() && effects.is_empty());
    let body = rest_fixture("depth_snapshot.json");
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty(), "{:?}", sink.0);
    let tag = fire(codec.as_mut(), retry_timer(&fx));
    // The retry's snapshot is bridged by e3, buffered meanwhile: the first epoch published.
    let (events, effects) = run(codec.as_mut(), &frames[3..4]);
    assert!(events.is_empty() && effects.is_empty());
    let body = body.replace(r#""lastUpdateId":1027024"#, r#""lastUpdateId":1027045"#);
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(fx.is_empty());
    assert_eq!(sink.0, bridged_by_e3(1));
}

#[test]
fn a_snapshot_with_nothing_buffered_to_bridge_it_is_held_until_an_event_does() {
    let mut codec = subscribed();
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let tag = snapshot_request(&fx);
    let body = rest_fixture("depth_snapshot.json");
    // e0 alone is buffered, and dropped: it ends before the snapshot, which is held.
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty() && fx.is_empty());
    // e0 again is dropped too; e1 straddles: the snapshot, then its delta; e2 follows.
    let (events, effects) = run(codec.as_mut(), &frames[..3]);
    assert!(effects.is_empty());
    assert_eq!(events, [first_snapshot(1), e2()].concat());

    // A held snapshot the next event does not bridge is never published, and is retried.
    let mut codec = subscribed();
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let tag = snapshot_request(&fx);
    let (result, sink, _) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty());
    let (result, sink, fx) = frame(codec.as_mut(), &frames[2]);
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty(), "{:?}", sink.0);
    let tag = fire(codec.as_mut(), retry_timer(&fx));
    let (events, effects) = run(codec.as_mut(), &frames[3..4]);
    assert!(events.is_empty() && effects.is_empty());
    let body = body.replace(r#""lastUpdateId":1027024"#, r#""lastUpdateId":1027045"#);
    let (result, sink, _) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0, bridged_by_e3(1));
}

#[test]
fn an_unsubscribed_book_drops_its_state_and_its_frames_and_answers_are_refused_or_ignored() {
    let mut codec = subscribed();
    let frames = fixture("diff_depth.jsonl");
    let (_, _, fx) = frame(codec.as_mut(), &frames[0]);
    let tag = snapshot_request(&fx);
    let mut fx = Effects::new();
    codec
        .subscribe(&[], &[diff(BTC)], &specs(), &mut fx)
        .unwrap();
    // The UNSUBSCRIBE goes out.
    assert!(matches!(fx.as_slice(), [Effect::Send { .. }]));
    // The snapshot answers nothing now.
    let body = rest_fixture("depth_snapshot.json");
    let (result, sink, fx) = http(codec.as_mut(), tag, ok(&body));
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "a response to no pending snapshot request"
        ))
    );
    assert!(sink.0.is_empty() && fx.is_empty());
    // A frame still in flight is read and pushes nothing.
    let (events, effects) = run(codec.as_mut(), &frames[1..2]);
    assert!(events.is_empty() && effects.is_empty());
    // Subscribing again buffers afresh and asks again.
    codec
        .subscribe(&[diff(BTC)], &[], &specs(), &mut Effects::new())
        .unwrap();
    let (_, _, fx) = frame(codec.as_mut(), &frames[1]);
    snapshot_request(&fx);
    // Subscribing an instrument already subscribed keeps its state: no second request.
    let mut fx = Effects::new();
    codec
        .subscribe(&[diff(BTC)], &[], &specs(), &mut fx)
        .unwrap();
    let (_, _, more) = frame(codec.as_mut(), &frames[2]);
    assert!(more.is_empty());
}

#[test]
fn diff_frames_the_codec_cannot_read_are_refused_with_nothing_pushed() {
    let e1 = &fixture("diff_depth.jsonl")[1];
    let cases: Vec<(String, DecodeError)> = vec![
        (
            e1.replace(r#""e":"depthUpdate""#, r#""e":"bookTicker""#),
            DecodeError::Malformed("e"),
        ),
        (
            e1.replace(r#""U":1027011"#, r#""U":"x""#),
            DecodeError::Malformed("U"),
        ),
        (
            e1.replace(r#""U":1027011"#, r#""U":1027031"#),
            DecodeError::Malformed("U"),
        ),
        (
            e1.replace(r#""pu":1027010"#, r#""pu":-1"#),
            DecodeError::Malformed("pu"),
        ),
        (
            e1.replace(r#""u":1027030"#, r#""u":null"#),
            DecodeError::Malformed("u"),
        ),
        (
            e1.replace(r#""b":[["7405.40","4.000"]]"#, r#""b":{}"#),
            DecodeError::Malformed("b"),
        ),
        (
            e1.replace(r#"["7405.50","0"]"#, r#"["7405.505","0"]"#),
            DecodeError::Malformed("a"),
        ),
        (
            e1.replace(r#""s":"BTCUSDT""#, r#""s":"SOLUSDT""#)
                .replace("btcusdt@", "solusdt@"),
            DecodeError::UnknownInstrument,
        ),
    ];
    let mut codec = subscribed();
    for (f, want) in cases {
        let (result, sink, fx) = frame(codec.as_mut(), &f);
        assert_eq!(result, Err(want), "{f}");
        assert!(sink.0.is_empty() && fx.is_empty(), "{f}");
    }
    // None of them started the buffer: the first good frame asks for the snapshot.
    let (_, _, fx) = frame(codec.as_mut(), e1);
    snapshot_request(&fx);
    // A frame for an instrument missing from the spec table is refused.
    let (result, sink, _) = frame_with(codec.as_mut(), e1, &SpecTable::new());
    assert_eq!(result, Err(DecodeError::UnknownInstrument));
    assert!(sink.0.is_empty());
}

#[test]
fn a_refused_configuration_subscribes_no_diff_book() {
    let mut cfg = config();
    cfg.insert(fbc_venue_binance_usdm::KEY_SNAPSHOT_RETRY, "0ms");
    let mut codec = BinanceUsdm.md_codec(&cfg, &plan());
    let mut fx = Effects::new();
    assert!(
        codec
            .subscribe(&[diff(BTC)], &[], &specs(), &mut fx)
            .is_err()
    );
    assert!(fx.is_empty());
    let (result, sink, fx) = frame(codec.as_mut(), &fixture("diff_depth.jsonl")[0]);
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty() && fx.is_empty());
}
