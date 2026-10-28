//! FBC-1632's done line, the codec half (decision 0130): the status poll's codec, run as the
//! runtime runs it on a wall clock the test moves (`common::status::Sim`), on the hand-built
//! answers of `fixtures/hibachi/rest/status/`. Each test is named for the clause it proves:
//! reads sent from timers at the interval and judged at the timer each answer asks for; each
//! market's status mapped as discovery maps it and reported once each time it differs; every
//! market halted while a read fails or is refused; halts at a maintenance window's begin, a
//! session's end and an advertised close on a timer, between reads; a read sent before an
//! advertised open or judged at or after a close halted; a market a read leaves out halted; no
//! read in flight across a halt lifting it; the window after each reopening; and never trading
//! on a schedule alone. Reads in flight together too: one held past an advertised open, the
//! window after a reopening or a close it is the first to advertise, judged from its send to its
//! answer; one answered after a later-sent one, which leaves that read's close and windows
//! standing and adds its own; and a market trading again while every answer comes after the next
//! read is sent. `tests/status_stub.rs` runs the same codec in the runtime against the
//! conformance stub.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::status::{
    AFTER, ETH, EUR, FRIDAY, FX_CLOSE, FX_REOPEN, INTERVAL, MARKETS, NS, Sim, WEDNESDAY, WIF,
    answer, answer_json, each, specs, subs,
};
use fbc_core::{
    DecodeError, Effect, Effects, Feed, HttpFailure, HttpResponse, HttpTag, MdCodec, MdEvent,
    MdSink, MonoNs, TradingStatus, VenueMeta, WallNs,
};
use fbc_venue_hibachi::{Hibachi, HibachiStatus};
use serde_json::{Value, json};

const TRADING: TradingStatus = TradingStatus::Trading;
const HALTED: TradingStatus = TradingStatus::Halted;

type Seen = Vec<(f64, fbc_core::InstrumentId, TradingStatus)>;

/// `doc` with `symbol`'s contract changed by `change`.
fn with_contract(mut doc: Value, symbol: &str, change: impl FnOnce(&mut Value)) -> Vec<u8> {
    let contracts = doc["futureContracts"].as_array_mut().unwrap();
    let contract = contracts
        .iter_mut()
        .find(|c| c["symbol"] == symbol)
        .unwrap();
    change(contract);
    doc.to_string().into_bytes()
}

/// `live.json` with ETH's `status` as given.
fn eth_status(status: Value) -> Vec<u8> {
    with_contract(answer_json("live"), "ETH/USDT-P", |c| c["status"] = status)
}

#[test]
fn reads_go_out_from_timers_at_the_interval_and_each_answer_is_judged_at_the_timer_it_asks_for() {
    let plan = common::status::poll_plan(&common::config(), &subs());
    let specs = specs();
    let mut codec = HibachiStatus::new(plan.stream, &common::config());
    let mut fx = Effects::new();
    // on_open asks for the first read's timer, at once; subscribing sends and asks nothing.
    codec.on_open(&mut fx);
    codec.subscribe(&plan.subs, &[], &specs, &mut fx).unwrap();
    let [Effect::Timer { tag: first, after }] = fx.take()[..] else {
        panic!("one timer")
    };
    assert_eq!(after, Duration::ZERO);
    struct Kept(Vec<MdEvent>);
    impl MdSink for Kept {
        fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
            assert_eq!(meta, VenueMeta::NONE);
            self.0.push(ev);
        }
    }
    let mut sink = Kept(Vec::new());
    let wall = WallNs(WEDNESDAY * NS);
    codec.on_timer(first, MonoNs(0), wall, &mut sink, &mut fx);
    // The read, then the next read's timer at the interval; nothing reported yet. (The schedule
    // check of EUR's FX weekend, a timer of its own, comes first.)
    let effects = fx.take();
    let http: Vec<HttpTag> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Http { tag, req, .. } => {
                assert_eq!(
                    req.url.as_str(),
                    "https://data-api.hibachi.xyz/market/exchange-info"
                );
                Some(*tag)
            }
            _ => None,
        })
        .collect();
    let timers: Vec<Duration> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Timer { after, .. } => Some(*after),
            _ => None,
        })
        .collect();
    assert_eq!(http.len(), 1, "{effects:?}");
    let to_close = Duration::from_secs(u64::try_from(FX_CLOSE - WEDNESDAY).unwrap());
    assert_eq!(timers, [to_close, Duration::from_secs(60)]);
    assert!(sink.0.is_empty());
    // The answer is read, nothing reported, and one timer asked for at once ...
    let body = answer("live");
    let response = HttpResponse {
        status: 200,
        headers: &[],
        body: &body,
    };
    let caps = fbc_core::VenueFactory::caps(&Hibachi, &common::config()).unwrap();
    fbc_core::dispatch_market_data(&caps, |scope| {
        codec.on_http(http[0], Ok(response), scope, &specs, &mut sink, &mut fx)
    })
    .unwrap();
    assert!(sink.0.is_empty());
    let [Effect::Timer { tag: judge, after }] = fx.take()[..] else {
        panic!("one timer")
    };
    assert_eq!(after, Duration::ZERO);
    // ... at whose wall time it is judged: every market trading.
    let later = WallNs(wall.0 + NS / 10);
    codec.on_timer(judge, MonoNs(1), later, &mut sink, &mut fx);
    let expected: Vec<MdEvent> = MARKETS
        .iter()
        .map(|&inst| MdEvent::Status {
            inst,
            status: TRADING,
        })
        .collect();
    assert_eq!(sink.0, expected);
    assert_eq!(codec.counts().reads, 1);
    assert_eq!(codec.counts().judged, 1);
    assert_eq!(codec.reported(ETH), Some(TRADING));
    // A frame is refused: a poll endpoint receives none.
    let refused = fbc_core::dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            fbc_core::RawFrame::Text("{}"),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    });
    assert!(refused.is_err());
}

#[test]
fn the_sim_sends_each_read_an_interval_after_the_last() {
    let mut sim = Sim::new(WEDNESDAY);
    sim.answer_next("live");
    for k in 1..=3 {
        sim.advance_to((WEDNESDAY + k * INTERVAL) as f64);
        assert_eq!(sim.requests.len(), 1, "{k}");
        assert_eq!(sim.requests[0].1, (WEDNESDAY + k * INTERVAL) * NS);
        sim.answer_next("live");
    }
    assert_eq!(sim.urls.len(), 4);
}

#[test]
fn only_a_live_contract_on_a_normal_exchange_is_trading_each_reported_once_when_it_differs() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    let script: [(i64, Vec<u8>); 15] = [
        (60, answer("live")),
        (120, answer("eth_paused")),
        (180, answer("eth_paused")),
        (240, answer("live")),
        (300, eth_status(json!("LISTED"))),
        (360, answer("live")),
        (420, eth_status(json!("PRE_OPEN"))),
        (480, answer("live")),
        (540, eth_status(json!("CLOSED"))),
        (600, answer("live")),
        (660, answer("unscheduled_maintenance")),
        (720, answer("unscheduled_maintenance")),
        (780, answer("live")),
        (840, answer("scheduled_maintenance")),
        (900, answer("live")),
    ];
    for (at, body) in script {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &body).unwrap();
    }
    let mut expected: Seen = each(0.0, &MARKETS, TRADING);
    for (halted, lifted) in [
        (120.0, 240.0),
        (300.0, 360.0),
        (420.0, 480.0),
        (540.0, 600.0),
    ] {
        expected.push((halted, ETH, HALTED));
        expected.push((lifted, ETH, TRADING));
    }
    // The exchange in either maintenance halts every market, LIVE as each is listed.
    expected.extend(each(660.0, &MARKETS, HALTED));
    expected.extend(each(780.0, &MARKETS, TRADING));
    expected.extend(each(840.0, &MARKETS, HALTED));
    // The scheduled maintenance's window (1792584290 to 1792584330, t+290 to t+330) had ended,
    // with the window after it, before t+840: the exchange's status alone halted, and the next
    // read lifts.
    expected.extend(each(900.0, &MARKETS, TRADING));
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(sim.codec.counts().judged, 16);
}

#[test]
fn a_failed_read_halts_every_market_until_a_read_sent_after_it_succeeds() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    // A read with no response, then one answered 503: every market halted at the first, once.
    sim.advance_to((t + 60) as f64);
    let tag = sim.next_read();
    sim.respond(tag, Err(HttpFailure::TimedOut)).unwrap();
    sim.advance_to((t + 120) as f64);
    let tag = sim.next_read();
    let unavailable = HttpResponse {
        status: 503,
        headers: &[],
        body: b"",
    };
    sim.respond(tag, Ok(unavailable)).unwrap();
    sim.read_at(t + 180, "live");
    // Two reads in flight: the later fails, then the earlier, sent before that failure, is
    // answered LIVE: it lifts nothing. The next read does.
    sim.advance_to((t + 240) as f64);
    sim.advance_to((t + 300) as f64);
    let (earlier, later) = (sim.next_read(), sim.next_read());
    sim.respond(later, Err(HttpFailure::Lost)).unwrap();
    sim.advance_to((t + 310) as f64);
    sim.answer(earlier, &answer("live")).unwrap();
    sim.read_at(t + 360, "live");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(60.0, &MARKETS, HALTED));
    expected.extend(each(180.0, &MARKETS, TRADING));
    expected.extend(each(300.0, &MARKETS, HALTED));
    expected.extend(each(360.0, &MARKETS, TRADING));
    assert_eq!(sim.since(0, t), expected);
    let counts = sim.codec.counts();
    assert_eq!(
        (counts.failed_reads, counts.reads, counts.judged),
        (3, 7, 4)
    );
    // A failure of a read already settled, or of no read, is refused.
    assert!(sim.respond(HttpTag(99), Err(HttpFailure::NotSent)).is_err());
}

#[test]
fn a_refused_answer_halts_every_market_and_names_the_part() {
    let mut doc = answer_json("live");
    doc["status"] = Value::Null;
    let exchange_null = doc.to_string().into_bytes();
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"begin": 1_792_584_400, "end": 1_792_584_400}]);
    let empty_window = doc.to_string().into_bytes();
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!({"begin": 1_792_584_400, "end": 1_792_584_500});
    let window_not_list = doc.to_string().into_bytes();
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"begin": "soon", "end": 1_792_584_500}]);
    let window_not_number = doc.to_string().into_bytes();
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"end": 1_792_584_500}]);
    let window_no_begin = doc.to_string().into_bytes();
    let mut doc = answer_json("live");
    let eth = doc["futureContracts"][0].clone();
    doc["futureContracts"].as_array_mut().unwrap().push(eth);
    let twice = doc.to_string().into_bytes();
    let refused: [(&str, Vec<u8>, &str); 16] = [
        ("status null", answer("status_null"), "status"),
        ("status absent", with_status_removed(), "status"),
        ("status lowercase", eth_status(json!("live")), "status"),
        ("status undocumented", eth_status(json!("HALTED")), "status"),
        ("close in ms", answer("close_ms"), "marketCloseTimestamp"),
        (
            "close not a number",
            answer("close_not_number"),
            "marketCloseTimestamp",
        ),
        (
            "open in ms",
            with_contract(answer_json("live"), "WIF/USDT-P", |c| {
                c["marketOpenTimestamp"] = json!("1792584000000")
            }),
            "marketOpenTimestamp",
        ),
        ("window in ms", answer("window_ms"), "maintenanceWindow"),
        ("window empty", empty_window, "maintenanceWindow"),
        ("window not a list", window_not_list, "maintenanceWindow"),
        (
            "window bound not a number",
            window_not_number,
            "maintenanceWindow",
        ),
        (
            "window without a begin",
            window_no_begin,
            "maintenanceWindow",
        ),
        ("exchange status null", exchange_null, "exchange status"),
        ("symbol listed twice", twice, "symbol listed twice"),
        (
            "no symbol",
            with_contract(answer_json("live"), "EUR/USDT-P", |c| {
                c.as_object_mut().unwrap().remove("symbol");
            }),
            "symbol",
        ),
        (
            "not JSON",
            b"<html>".to_vec(),
            "exchange-info answer is not JSON",
        ),
    ];
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    let mut at = t;
    for (name, body, part) in &refused {
        at += INTERVAL;
        sim.advance_to(at as f64);
        let from = sim.reported.len();
        let tag = sim.next_read();
        assert_eq!(
            sim.answer(tag, body),
            Err(DecodeError::Malformed(part)),
            "{name}"
        );
        // Every market halted at once, at the poll that refused it.
        let seconds = (at - t) as f64;
        assert_eq!(
            sim.since(from, t),
            each(seconds, &MARKETS, HALTED),
            "{name}"
        );
        // A read sent after it lifts them.
        at += INTERVAL;
        sim.read_at(at, "live");
        let seconds = (at - t) as f64;
        let lifted = sim.since(from + 3, t);
        assert_eq!(lifted, each(seconds, &MARKETS, TRADING), "{name}");
    }
    assert_eq!(sim.codec.counts().refused_answers, 16);
}

/// `live.json` with ETH's `status` left out.
fn with_status_removed() -> Vec<u8> {
    with_contract(answer_json("live"), "ETH/USDT-P", |c| {
        c.as_object_mut().unwrap().remove("status");
    })
}

/// The Friday timeline with the window `window` publishes, from t+90 to t+150: what was
/// reported.
fn friday_window(window: &str) -> Vec<(f64, fbc_core::InstrumentId, TradingStatus)> {
    let t = FRIDAY;
    let mut sim = Sim::new(t);
    // The first read publishes the window: nothing halts before its begin.
    sim.answer_next(window);
    let (begin, end) = (t + 90, t + 150);
    assert!(sim.armed.iter().any(|(due, _)| *due == begin * NS));
    // A read sent before the begin, answered after the halt the begin's timer reported, LIVE
    // and NORMAL: still halted.
    sim.advance_to((t + 60) as f64);
    sim.advance_to(begin as f64);
    sim.answer_next(window);
    sim.read_at(t + 120, window);
    // The window ended at t+150: the window after it runs to t+270, the answers no longer
    // listing it.
    assert!(end + AFTER == t + 270);
    sim.read_at(t + 180, "live");
    sim.read_at(t + 240, "live");
    sim.read_at(t + 300, "live");
    sim.since(0, t)
}

#[test]
fn a_maintenance_window_halts_every_market_at_its_begin_and_a_read_in_flight_never_lifts_it() {
    let seen = friday_window("window_number");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(90.0, &MARKETS, HALTED));
    expected.extend(each(300.0, &MARKETS, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_window_bound_reads_alike_as_a_number_and_as_a_string() {
    assert_eq!(
        friday_window("window_number"),
        friday_window("window_string")
    );
}

#[test]
fn an_fx_session_end_halts_the_pair_at_its_time_and_a_read_in_flight_never_lifts_it() {
    let t = FRIDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    for k in 1..=8 {
        sim.read_at(t + k * INTERVAL, "live");
    }
    // A read sent before the close (t+600), answered after the pair's halt, LIVE and NORMAL.
    sim.advance_to((t + 540) as f64);
    assert_eq!(FX_CLOSE, t + 600);
    sim.advance_to(FX_CLOSE as f64);
    sim.answer_next("live");
    // The weekend: a read listing it LIVE does not lift it either.
    sim.read_at(t + 660, "live");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((600.0, EUR, HALTED));
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(sim.codec.counts().scheduled_halts, 1);
}

#[test]
fn the_fx_reopening_holds_the_pair_halted_for_the_configured_window_and_never_on_a_schedule() {
    // Sunday, two minutes before the reopening at 17:00 New York.
    let t = FX_REOPEN - 120;
    let mut sim = Sim::new(t);
    // The first timer finds the pair closed: halted at once, before any read.
    assert_eq!(sim.since(0, t), [(0.0, EUR, HALTED)]);
    sim.answer_next("live");
    // Every read until one sent the configured window after the reopening: still halted, and
    // nothing between reads lifts it.
    for k in 1..=3 {
        sim.read_at(t + k * INTERVAL, "live");
        sim.advance_to((t + k * INTERVAL + 30) as f64);
    }
    assert_eq!(FX_REOPEN + AFTER, t + 240);
    sim.read_at(t + 240, "live");
    let mut expected = vec![(0.0, EUR, HALTED), (0.0, ETH, TRADING), (0.0, WIF, TRADING)];
    expected.push((240.0, EUR, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_close_between_reads_halts_at_that_instant_and_a_read_showing_it_live_stays_halted() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    // The close at t+150 advertised from t+60.
    sim.read_at(t + 60, "wif_close");
    assert!(sim.armed.iter().any(|(due, _)| *due == (t + 150) * NS));
    // A read sent before the close, answered after the halt at the close, LIVE.
    sim.advance_to((t + 120) as f64);
    sim.advance_to((t + 150) as f64);
    sim.answer_next("wif_close");
    // The next read still lists it LIVE with the same close: halted.
    sim.read_at(t + 180, "wif_close");
    // A read sent before a close it is the first to advertise (ETH's, t+250), answered after it:
    // halted.
    sim.advance_to((t + 240) as f64);
    sim.advance_to((t + 260) as f64);
    sim.answer_next("eth_close_first");
    // A read leaving EUR out halts EUR; ETH, listed LIVE with no close by a read sent after its
    // halt, trades again.
    sim.read_at(t + 300, "eur_left_out");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, WIF, HALTED));
    expected.push((260.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    expected.push((300.0, EUR, HALTED));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn an_answer_judged_at_its_close_is_halted() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    // Advertised for the very instant the answer is judged: at or after it, halted.
    let at_now = with_contract(answer_json("live"), "WIF/USDT-P", |c| {
        c["marketCloseTimestamp"] = json!(WEDNESDAY.to_string())
    });
    let tag = sim.next_read();
    sim.answer(tag, &at_now).unwrap();
    let expected = vec![(0.0, ETH, TRADING), (0.0, EUR, TRADING), (0.0, WIF, HALTED)];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_sent_before_an_advertised_open_is_halted_whatever_its_status() {
    let t = WEDNESDAY;
    let opens = with_contract(answer_json("live"), "WIF/USDT-P", |c| {
        c["marketOpenTimestamp"] = json!((WEDNESDAY + 100).to_string())
    });
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(tag, &opens).unwrap();
    for k in 1..=4 {
        sim.advance_to((t + k * INTERVAL) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &opens).unwrap();
    }
    // Sent before the open (t+100), or before the window after it (t+220): halted; sent at
    // t+240: trading.
    let expected = vec![
        (0.0, ETH, TRADING),
        (0.0, EUR, TRADING),
        (0.0, WIF, HALTED),
        (240.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_halt_another_reads_answer_reports_is_not_lifted_by_a_read_sent_before_it() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    // Two reads in flight: the later answers ETH PAUSED, then the earlier answers LIVE.
    sim.advance_to((t + 60) as f64);
    sim.advance_to((t + 120) as f64);
    let (earlier, later) = (sim.next_read(), sim.next_read());
    sim.advance_to((t + 125) as f64);
    sim.answer(later, &answer("eth_paused")).unwrap();
    sim.advance_to((t + 130) as f64);
    sim.answer(earlier, &answer("live")).unwrap();
    sim.read_at(t + 180, "live");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((125.0, ETH, HALTED));
    expected.push((180.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_market_subscribed_later_is_checked_at_once_and_one_removed_reports_nothing() {
    // Saturday: the FX weekend holds EUR.
    let t = FX_CLOSE + 86_400;
    let cfg = common::config();
    let eth_only = BTreeSet::from([fbc_core::Subscription {
        inst: ETH,
        feed: Feed::Trades,
    }]);
    let mut sim = Sim::with(&cfg, &eth_only, t);
    sim.answer_next("live");
    assert_eq!(sim.since(0, t), [(0.0, ETH, TRADING)]);
    // EUR subscribed: halted by the schedule check asked for at once, before any read.
    let eur = fbc_core::Subscription {
        inst: EUR,
        feed: Feed::Trades,
    };
    let mut fx = Effects::new();
    sim.codec
        .subscribe(&[eur], &[], &sim.specs, &mut fx)
        .unwrap();
    let [Effect::Timer { tag, after }] = fx.take()[..] else {
        panic!("one timer")
    };
    assert_eq!(after, Duration::ZERO);
    struct Kept(Vec<MdEvent>);
    impl MdSink for Kept {
        fn push(&mut self, _: VenueMeta, ev: MdEvent) {
            self.0.push(ev);
        }
    }
    let mut kept = Kept(Vec::new());
    let wall = WallNs(t * NS);
    sim.codec.on_timer(tag, MonoNs(0), wall, &mut kept, &mut fx);
    let halted = MdEvent::Status {
        inst: EUR,
        status: HALTED,
    };
    assert_eq!(kept.0, [halted]);
    // ETH removed: a read halting every market reports EUR alone.
    let eth = eth_only.first().copied().unwrap();
    sim.codec
        .subscribe(&[], &[eth], &sim.specs, &mut fx)
        .unwrap();
    assert!(fx.is_empty());
    assert_eq!(sim.codec.counts().reads, 1);
    let from = sim.reported.len();
    sim.advance_to((t + INTERVAL) as f64);
    let tag = sim.next_read();
    sim.respond(tag, Err(HttpFailure::TimedOut)).unwrap();
    assert!(sim.since(from, t).is_empty(), "EUR was already halted");
    // Every subscription refused as plan_md refuses it.
    let stats = fbc_core::Subscription {
        inst: ETH,
        feed: Feed::Stats,
    };
    assert!(
        sim.codec
            .subscribe(&[stats], &[], &sim.specs, &mut fx)
            .is_err()
    );
}

#[test]
fn nothing_lifts_a_halt_between_reads_and_no_schedule_reports_trading() {
    // The Friday window and the FX close: every report between two reads is a halt.
    let t = FRIDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("window_number");
    let mut k = 1;
    while t + k * INTERVAL <= FX_CLOSE + 2 * INTERVAL {
        let at = t + k * INTERVAL;
        sim.read_at(at, "window_number");
        sim.advance_to((at + INTERVAL / 2) as f64);
        k += 1;
    }
    for r in &sim.reported {
        let between = (r.at / NS - t) % INTERVAL != 0;
        assert!(!between || r.status == HALTED, "{r:?}");
    }
    assert!(sim.reported.iter().any(|r| r.status == HALTED));
}

#[test]
fn the_crate_docs_say_what_status_does_not_check_and_how_late_an_unannounced_change_arrives() {
    // The crate docs as one line of text, each `//!` line's text joined by single spaces.
    let docs = include_str!("../src/lib.rs")
        .lines()
        .filter_map(|line| line.strip_prefix("//!"))
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ");
    for stated in [
        "A command the consumer authorizes through `fbc-oms`'s registry directly is not checked \
         against status",
        "A change no schedule announces (an FX pair paused early, an unscheduled maintenance) \
         reaches the consumer up to one interval late",
    ] {
        assert!(docs.contains(stated), "the crate docs do not say: {stated}");
    }
}

#[test]
fn an_answer_with_no_maintenance_window_list_publishes_none() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    let mut absent = answer_json("live");
    absent.as_object_mut().unwrap().remove("maintenanceWindow");
    let tag = sim.next_read();
    sim.answer(tag, absent.to_string().as_bytes()).unwrap();
    let mut null = answer_json("live");
    null["maintenanceWindow"] = Value::Null;
    sim.advance_to((t + INTERVAL) as f64);
    let tag = sim.next_read();
    sim.answer(tag, null.to_string().as_bytes()).unwrap();
    assert_eq!(sim.since(0, t), each(0.0, &MARKETS, TRADING));
    assert_eq!(sim.codec.counts().judged, 2);
}

/// `live.json` with `symbol`'s contract advertising `field` at `at` (Unix seconds).
fn advertising(symbol: &str, field: &'static str, at: i64) -> Vec<u8> {
    with_contract(answer_json("live"), symbol, |c| {
        c[field] = json!(at.to_string())
    })
}

/// Two reads in flight, sent at t+60 and t+120 and answered out of order: the later with
/// `later` at t+125, the earlier with `earlier` at t+130. The poll at t+179.
fn answered_out_of_order(earlier: &[u8], later: &[u8]) -> Sim {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    sim.advance_to((t + 120) as f64);
    let (first, second) = (sim.next_read(), sim.next_read());
    sim.advance_to((t + 125) as f64);
    sim.answer(second, later).unwrap();
    sim.advance_to((t + 130) as f64);
    sim.answer(first, earlier).unwrap();
    sim.advance_to((t + 179) as f64);
    sim
}

#[test]
fn a_read_answered_after_a_later_sent_one_leaves_the_later_reads_close_standing() {
    // The later read advertises WIF's close at t+150; the earlier, sent before it and answered
    // after it, advertises none: the close still halts WIF at its instant, between reads.
    let sim = answered_out_of_order(&answer("live"), &answer("wif_close"));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, WIF, HALTED));
    assert_eq!(sim.since(0, WEDNESDAY), expected);
    assert_eq!(sim.codec.counts().scheduled_halts, 1);
}

#[test]
fn a_read_answered_after_a_later_sent_one_adds_its_close_and_windows_and_removes_none() {
    // The earlier read, answered last, advertises WIF's close at t+150, or publishes a window
    // from t+150 to t+200; the later advertises neither. Which of the two shows the venue later
    // is not known, so the close or the window is kept: halted at t+150.
    let t = WEDNESDAY;
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"begin": t + 150, "end": t + 200, "note": "Synthetic"}]);
    let window = doc.to_string().into_bytes();
    let sim = answered_out_of_order(&answer("wif_close"), &answer("live"));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, WIF, HALTED));
    assert_eq!(sim.since(0, t), expected);
    let sim = answered_out_of_order(&window, &answer("live"));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(150.0, &MARKETS, HALTED));
    assert_eq!(sim.since(0, t), expected);
    // Answered in send order, the later read's schedule replaces the earlier's: no halt.
    for earlier in [answer("wif_close"), window] {
        let mut sim = Sim::new(t);
        sim.answer_next("live");
        sim.advance_to((t + 60) as f64);
        sim.advance_to((t + 120) as f64);
        let (first, second) = (sim.next_read(), sim.next_read());
        sim.advance_to((t + 125) as f64);
        sim.answer(first, &earlier).unwrap();
        sim.advance_to((t + 130) as f64);
        sim.answer(second, &answer("live")).unwrap();
        sim.advance_to((t + 179) as f64);
        assert_eq!(sim.since(0, t), each(0.0, &MARKETS, TRADING));
    }
}

#[test]
fn a_read_answered_after_a_later_sent_one_leaves_the_later_reads_windows_standing() {
    // The later read publishes a window from t+150 to t+200; the earlier publishes none: the
    // window still halts every market at its begin, between reads.
    let t = WEDNESDAY;
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"begin": t + 150, "end": t + 200, "note": "Synthetic"}]);
    let sim = answered_out_of_order(&answer("live"), doc.to_string().as_bytes());
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(150.0, &MARKETS, HALTED));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_held_until_after_an_advertised_open_and_its_window_is_halted_if_sent_before_them() {
    // WIF opens at t+100, and the window after it ends at t+220.
    let t = WEDNESDAY;
    assert_eq!(t + 100 + AFTER, t + 220);
    let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", t + 100);
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(tag, &opens).unwrap();
    for k in 1..=2 {
        sim.advance_to((t + k * INTERVAL) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &opens).unwrap();
    }
    // The read sent at t+180, held until t+225, after the window: sent before it, so halted.
    sim.advance_to((t + 180) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 225) as f64);
    sim.answer(held, &opens).unwrap();
    // The read sent at t+240, answered at t+250: trading.
    sim.advance_to((t + 250) as f64);
    let tag = sim.next_read();
    sim.answer(tag, &opens).unwrap();
    let expected = vec![
        (0.0, ETH, TRADING),
        (0.0, EUR, TRADING),
        (0.0, WIF, HALTED),
        (250.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_sent_inside_the_window_after_the_fx_reopening_and_answered_after_it_is_halted() {
    // The reopening at t+150, and the window after it to t+270.
    let t = FX_REOPEN - 150;
    assert_eq!(FX_REOPEN + AFTER, t + 270);
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    for k in 1..=3 {
        sim.read_at(t + k * INTERVAL, "live");
    }
    // The read sent at t+240, held until t+280, after the window: sent inside it, so halted.
    sim.advance_to((t + 240) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 280) as f64);
    sim.answer(held, &answer("live")).unwrap();
    // The read sent at t+300: trading.
    sim.read_at(t + 300, "live");
    let expected = vec![
        (0.0, EUR, HALTED),
        (0.0, ETH, TRADING),
        (0.0, WIF, TRADING),
        (300.0, EUR, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_halted_market_answered_after_a_close_it_is_the_first_to_advertise_reports_no_trading() {
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.read_at(t + 60, "eth_paused");
    // A read sent at t+120, held until t+130, LIVE and the first to advertise ETH's close at
    // t+125: judged after the close, so halted, and nothing reported.
    let closes = advertising("ETH/USDT-P", "marketCloseTimestamp", t + 125);
    sim.advance_to((t + 120) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 130) as f64);
    sim.answer(held, &closes).unwrap();
    sim.advance_to((t + 180) as f64);
    let tag = sim.next_read();
    sim.answer(tag, &closes).unwrap();
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((60.0, ETH, HALTED));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_market_trades_again_while_every_answer_comes_after_the_next_read_is_sent() {
    // Every read answered 90 s after it is sent, the interval 60 s: two reads always in flight.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    let bodies = ["eth_paused", "live", "live", "live", "live"];
    for (k, name) in (1..).zip(bodies) {
        let sent = t + k * INTERVAL;
        sim.advance_to((sent + 90) as f64);
        let (tag, asked) = sim.requests.remove(0);
        assert_eq!(asked, sent * NS);
        sim.answer(tag, &answer(name)).unwrap();
    }
    // PAUSED, judged at t+150; the read sent at t+120, before that halt, lifts nothing; the one
    // sent at t+180, after it, lifts it when judged at t+270.
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, ETH, HALTED));
    expected.push((270.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}
