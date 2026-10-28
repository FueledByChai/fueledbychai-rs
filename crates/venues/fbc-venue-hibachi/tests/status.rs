//! FBC-1632's done line, the codec half (decision 0130): the status poll's codec, run as the
//! runtime runs it on a wall clock the test moves (`common::status::Sim`), on the hand-built
//! answers of `fixtures/hibachi/rest/status/`. Each test is named for the clause it proves:
//! reads sent from timers at the interval and judged at the timer each answer asks for; each
//! market's status mapped as discovery maps it and reported once each time it differs; every
//! market halted while a read fails or is refused; halts at a maintenance window's begin, a
//! session's end and an advertised close on a timer, between reads; a read sent before an
//! advertised open or judged at or after a close halted; a market a read leaves out halted; no
//! read in flight across a halt, or across the market's subscription, lifting it; the window
//! after each reopening, one a read shows and one a failed or refused read (whatever already
//! halts the market), a read leaving the market out (likewise), a read no longer advertising a
//! close or an open that held the market, a read advertising instead an open earlier than an
//! instant a close or an open showed the market halted, or a market subscribed once the reads
//! had begun, for the first time or again, however long no market was subscribed, on a poll
//! planned and kept with no subscription (not one subscribed before the poll sent any read,
//! which is subscribed afresh however many timers had fired: the reads begin with the first read
//! sent) may hide included, while an open a later read advertises no earlier than every such
//! instant, moved or after a close, keeps the window after it, a read still advertising a close
//! that holds it keeps the market halted by that close, and a past open dropped halts nothing;
//! and never trading on a schedule alone. The schedule is the endpoint's: a market subscribed
//! later is checked at once against the closes answers advertised before. Reads in flight
//! together too: one held past an advertised open, the window after a reopening or a close it is
//! the first to advertise, judged from its send to its answer; one answered after a later-sent
//! one, or answered first but sent before the earlier was answered, either of which may show the
//! venue later, so each leaves the other's open, close and windows standing and adds its own, the
//! earlier close and the later open kept, so a halt by a close or an open only one advertises,
//! past or future, holds the market until the window after the first read no longer advertising
//! it, while one answered after a later-sent one replaces nothing, so it ends no halt by the open
//! standing, nor by an earlier open it advertises; a read sent once another was answered replacing
//! it; and a market trading again while every answer comes after the next read is sent. The
//! module's and the crate's docs say each clause on subscriptions and the schedule as the code
//! does it. `tests/status_oracle.rs` checks the guarantee at random against a venue drawn from a
//! seed.
//! `tests/status_stub.rs` runs the same codec in the runtime against the conformance stub.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::status::{
    AFTER, ETH, EUR, FRIDAY, FX_CLOSE, FX_REOPEN, INTERVAL, MARKETS, NS, Sim, WEDNESDAY, WIF,
    WIF_MARK, answer, answer_json, each, specs, subs,
};
use fbc_core::{
    DecodeError, Effect, Effects, Feed, HttpFailure, HttpResponse, HttpTag, MdCodec, MdEvent,
    MdSink, MonoNs, Subscription, TradingStatus, VenueFactory, VenueMeta, WallNs,
};
use fbc_venue_hibachi::{Hibachi, HibachiStatus, KEY_AUCTION_HALT, STATUS_STREAM};
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
    // Each halt by status: its answers, then LIVE and NORMAL again. The first of those shows the
    // venue reopened the market by then, so it and every read sent within the window after it
    // (AFTER, two intervals) leave the market halted, and the third lifts it (RA214-2).
    let halts: [(Vec<Vec<u8>>, &[fbc_core::InstrumentId]); 6] = [
        (vec![answer("eth_paused"), answer("eth_paused")], &[ETH]),
        (vec![eth_status(json!("LISTED"))], &[ETH]),
        (vec![eth_status(json!("PRE_OPEN"))], &[ETH]),
        (vec![eth_status(json!("CLOSED"))], &[ETH]),
        (
            vec![
                answer("unscheduled_maintenance"),
                answer("unscheduled_maintenance"),
            ],
            &MARKETS,
        ),
        // The scheduled maintenance's window (1792584290 to 1792584330, t+290 to t+330) has
        // ended, with the window after it, long before: the exchange's status alone halts.
        (vec![answer("scheduled_maintenance")], &MARKETS),
    ];
    assert_eq!(AFTER, 2 * INTERVAL);
    let mut script: Vec<(i64, Vec<u8>)> = vec![(60, answer("live"))];
    let mut expected: Seen = each(0.0, &MARKETS, TRADING);
    for (bodies, markets) in halts {
        let halted = (script.len() as i64 + 1) * INTERVAL;
        for body in bodies {
            script.push(((script.len() as i64 + 1) * INTERVAL, body));
        }
        for _ in 0..3 {
            script.push(((script.len() as i64 + 1) * INTERVAL, answer("live")));
        }
        let lifted = script.len() as i64 * INTERVAL;
        // The exchange in either maintenance halts every market, LIVE as each is listed.
        expected.extend(each(halted as f64, markets, HALTED));
        expected.extend(each(lifted as f64, markets, TRADING));
    }
    let reads = script.len();
    for (at, body) in script {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &body).unwrap();
    }
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(sim.codec.counts().judged, 1 + reads as u64);
}

#[test]
fn a_failed_read_halts_every_market_until_the_window_after_the_first_read_sent_after_it_succeeds() {
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
    // The reads failed, so the venue may have halted and reopened every market meanwhile: the
    // read sent at t+180, the first to succeed, and the one sent inside the window after it
    // leave them halted; the read sent at t+300 lifts them (Verifier V-1).
    assert_eq!(180 + AFTER, 300);
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    // Two reads in flight: the later fails, then the earlier, sent before that failure, is
    // answered LIVE at the same instant: it lifts nothing and starts no window (from t+420 it
    // would end at t+540). The next read starts it, and the read sent at t+600 lifts them.
    sim.advance_to((t + 360) as f64);
    sim.advance_to((t + 420) as f64);
    let (earlier, later) = (sim.next_read(), sim.next_read());
    sim.respond(later, Err(HttpFailure::Lost)).unwrap();
    sim.answer(earlier, &answer("live")).unwrap();
    for at in [480, 540, 600] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(60.0, &MARKETS, HALTED));
    expected.extend(each(300.0, &MARKETS, TRADING));
    expected.extend(each(420.0, &MARKETS, HALTED));
    expected.extend(each(600.0, &MARKETS, TRADING));
    assert_eq!(sim.since(0, t), expected);
    let counts = sim.codec.counts();
    assert_eq!(
        (counts.failed_reads, counts.reads, counts.judged),
        (3, 11, 8)
    );
    // A failure of a read already settled, or of no read, is refused.
    assert!(sim.respond(HttpTag(99), Err(HttpFailure::NotSent)).is_err());
}

#[test]
fn a_refused_answer_holds_every_market_for_the_window_after_the_first_read_sent_after_it_shows_it()
{
    // Verifier V-1 (a): a refused answer shows nothing of the venue, which may have halted and
    // reopened every market meanwhile, so the first read sent after it that shows them trading
    // starts the window after a reopening, as a read showing one does.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    let tag = sim.next_read();
    let refused = sim.answer(tag, &answer("status_null"));
    assert_eq!(refused, Err(DecodeError::Malformed("status")));
    // The read sent at t+120, held until t+170: the venue reopened them, if it halted them, by
    // then, so the window runs from that judged time to t+290, not from the send (to t+240).
    sim.advance_to((t + 120) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 170) as f64);
    sim.answer(held, &answer("live")).unwrap();
    // The reads sent at t+180 and t+240, inside it, leave them halted; the one sent at t+300
    // lifts them.
    assert_eq!(170 + AFTER, 290);
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(60.0, &MARKETS, HALTED));
    expected.extend(each(300.0, &MARKETS, TRADING));
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(sim.codec.counts().refused_answers, 1);
}

/// `live.json` without `symbol`'s contract.
fn leaving_out(symbol: &str) -> Vec<u8> {
    let mut doc = answer_json("live");
    let contracts = doc["futureContracts"].as_array_mut().unwrap();
    contracts.retain(|c| c["symbol"] != symbol);
    doc.to_string().into_bytes()
}

#[test]
fn a_market_a_read_leaves_out_trades_again_only_the_window_after_the_first_read_listing_it_live() {
    // Verifier V-1 (b): a market a read leaves out may have been halted, or delisted and listed
    // again, so the first read that can lift it and lists it trading again starts the window
    // after a reopening, as a read showing one does.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    // Two reads in flight: the later leaves EUR out, then the earlier, sent before that halt,
    // lists it LIVE at the same instant: it lifts nothing and starts no window (from t+120 it
    // would end at t+240).
    sim.advance_to((t + 60) as f64);
    sim.advance_to((t + 120) as f64);
    let (earlier, later) = (sim.next_read(), sim.next_read());
    sim.answer(later, &leaving_out("EUR/USDT-P")).unwrap();
    sim.answer(earlier, &answer("live")).unwrap();
    // The read sent at t+180 lists EUR LIVE again: it and the one sent inside the window after
    // it leave EUR halted; the one sent at t+300 lifts it.
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((120.0, EUR, HALTED));
    expected.push((300.0, EUR, TRADING));
    assert_eq!(sim.since(0, t), expected);
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
    assert_eq!(AFTER, 2 * INTERVAL);
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
        // The first read sent after it and the one sent inside the window after that leave them
        // halted; the read sent at the window's end lifts them (Verifier V-1).
        for _ in 0..3 {
            at += INTERVAL;
            sim.read_at(at, "live");
        }
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
    // The close falls strictly between two reads, at t+630, the reads sent at t+600 and t+660
    // (RA214-1): only the calendar's timer halts the pair at its instant.
    let t = FX_CLOSE - 630;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    for k in 1..=9 {
        sim.read_at(t + k * INTERVAL, "live");
    }
    // The read sent at t+600, before the close, held across it and answered at t+645, LIVE and
    // NORMAL: the pair stays halted from the close's own instant.
    sim.advance_to((t + 600) as f64);
    let held = sim.next_read();
    sim.advance_to(FX_CLOSE as f64);
    assert_eq!(
        sim.last(EUR),
        Some(HALTED),
        "halted at the close, no read due"
    );
    sim.advance_to((t + 645) as f64);
    sim.answer(held, &answer("live")).unwrap();
    // The weekend: a read listing it LIVE does not lift it either.
    sim.read_at(t + 660, "live");
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((630.0, EUR, HALTED));
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(sim.codec.counts().scheduled_halts, 1);
    assert!(sim.armed.iter().any(|(due, _)| *due == FX_CLOSE * NS));
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
    // A read leaving EUR out halts EUR. ETH is listed LIVE with no close by the read sent at
    // t+300, after its halt: it no longer advertises the close that halted ETH, so ETH stays
    // halted until a read sent the window after it, at t+420 (V1C-2).
    for at in [300, 360, 420] {
        sim.read_at(t + at, "eur_left_out");
    }
    assert_eq!(300 + AFTER, 420);
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, WIF, HALTED));
    expected.push((260.0, ETH, HALTED));
    expected.push((300.0, EUR, HALTED));
    expected.push((420.0, ETH, TRADING));
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
    // The read sent at t+180 shows ETH reopened, so it and the one sent inside the window after
    // that (to t+300) leave it halted; the read sent at t+300 lifts it.
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((125.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
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
    let docs = module_docs(include_str!("../src/lib.rs"));
    for stated in [
        "A command the consumer authorizes through `fbc-oms`'s registry directly is not checked \
         against status",
        "A change no schedule announces (an FX pair paused early, an unscheduled maintenance) \
         reaches the consumer up to one interval late",
    ] {
        assert!(docs.contains(stated), "the crate docs do not say: {stated}");
    }
}

/// The `//!` docs of the source file `text` as one line, each line's text joined by single
/// spaces.
fn module_docs(text: &str) -> String {
    text.lines()
        .filter_map(|line| line.strip_prefix("//!"))
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn the_docs_hold_every_market_subscribed_once_a_read_was_sent_and_keep_every_answer_no_later_read_replaced()
 {
    // Verifier VE3-1, VE3-2, VE4-1, VE5-1 and VE5-2: the status module's docs, the crate's and the
    // auction window's say what the code does: every market subscribed once the reads have begun,
    // which is once a read was sent, not a timer fired, is held, for the first time or again; one
    // subscribed before is afresh; the poll is planned with no subscription so its codec keeps
    // what it knows; and the schedule is the endpoint's, an answer replaced only by one whose read
    // was sent once it was judged (the tests above and below prove each).
    let status = module_docs(include_str!("../src/status.rs"));
    for stated in [
        "It is planned for every set of subscriptions, alone and carrying none for the empty set",
        "so the endpoint stays open while the venue's market data runs",
        "A read timer sends a read only while some market is subscribed",
        "The reads begin with the first read the endpoint sends, for whichever markets are \
         subscribed then, not with its first timer: a read timer that fires while no market is \
         subscribed sends no read (above) and begins nothing",
        "and the read was sent after the market was subscribed",
        "So a read in flight across a halt, or across the market's subscription, never lifts it",
        "A market subscribed once the reads have begun, for the first time on the endpoint or \
         again, however long no market was subscribed meanwhile",
        "A market subscribed before the reads began, however many timers had fired, is subscribed \
         afresh",
        "It is the endpoint's, kept whichever markets are subscribed, so a market subscribed later \
         is checked against it at once",
        "so a read sent no earlier than an answer was judged shows the venue no earlier than that \
         answer does, and its answer replaces that one's",
        "in whichever order they come, which shows the venue later is not known, so the schedule \
         keeps both",
        "and the market is not subscribed once the reads have begun (above)",
        "or ends before what the first read judged shows of a market subscribed before the reads \
         began",
    ] {
        assert!(
            status.contains(stated),
            "the status docs do not say: {stated}"
        );
    }
    let lib = module_docs(include_str!("../src/lib.rs"));
    for stated in [
        "carrying every subscription (no connection for an empty set)",
        "The status poll is planned for every set, alone and carrying none for the empty one, so \
         it stays open",
        "a market is reported trading only by a read sent after every such halt and after it was \
         subscribed",
        "or by the market subscribed once the reads had begun, that is once the poll had sent a \
         read, for whichever markets, for the first time or again",
        "and the market is not subscribed once the reads have begun, from its begin",
    ] {
        assert!(lib.contains(stated), "the crate docs do not say: {stated}");
    }
    // KEY_AUCTION_HALT's own doc block (Verifier LD-2: not every doc of the file).
    let config = item_doc(
        include_str!("../src/config.rs"),
        "pub const KEY_AUCTION_HALT",
    );
    for stated in [
        "or after the market was subscribed once the reads had begun, that is once the poll had \
         sent a read, for whichever markets, for the first time or again",
        "and the market is not subscribed once the reads have begun, from its begin",
    ] {
        assert!(
            config.contains(stated),
            "KEY_AUCTION_HALT's doc does not say: {stated}"
        );
    }
    let schema = Hibachi.config_schema();
    let field = schema.iter().find(|f| f.key == KEY_AUCTION_HALT).unwrap();
    let stated = "the market subscribed once the reads had begun, that is once the poll had sent a \
                  read, for the first time or again";
    assert!(
        field.doc.contains(stated),
        "its schema doc does not say: {stated}"
    );
}

/// The `///` doc block directly above the line of the source file `text` that starts with
/// `item`, as one line, as [`module_docs`] joins them.
fn item_doc(text: &str, item: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let at = (lines.iter())
        .position(|line| line.trim_start().starts_with(item))
        .unwrap_or_else(|| panic!("no line starts with {item}"));
    let mut docs: Vec<&str> = (lines[..at].iter().rev())
        .map_while(|line| line.trim_start().strip_prefix("///"))
        .map(str::trim)
        .collect();
    docs.reverse();
    assert!(!docs.is_empty(), "{item} has no doc");
    docs.join(" ")
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
}

#[test]
fn a_read_sent_before_another_is_answered_replaces_nothing_that_one_advertises_and_one_sent_after_does()
 {
    // Verifier VE5-2, the schedule's half: an answer shows the venue at some instant between its
    // read's send and its judged time, so of two reads answered in send order the later shows the
    // venue later only when it was sent once the earlier was answered. The earlier advertises
    // WIF's close at t+150, or publishes a window from t+150 to t+200; the later advertises
    // neither.
    let t = WEDNESDAY;
    let mut doc = answer_json("live");
    doc["maintenanceWindow"] = json!([{"begin": t + 150, "end": t + 200, "note": "Synthetic"}]);
    let window = doc.to_string().into_bytes();
    // Answered in send order, but the later read was sent at t+120, before the earlier was
    // answered at t+125: either may show the venue later, so the close or the window is kept and
    // halts at t+150. (Replaced, as by the latest-sent read before, neither halts.)
    for (earlier, halted) in [
        (answer("wif_close"), vec![WIF]),
        (window.clone(), MARKETS.to_vec()),
    ] {
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
        let mut expected = each(0.0, &MARKETS, TRADING);
        expected.extend(each(150.0, &halted, HALTED));
        assert_eq!(sim.since(0, t), expected);
    }
    // A read sent once the earlier was answered shows the venue later, and replaces it: the
    // read sent at t+60 answered at t+70, or at t+120, the very instant the later is sent, and
    // the one sent at t+120 answered at t+125: no halt.
    for answered in [70, 120] {
        for earlier in [answer("wif_close"), window.clone()] {
            let mut sim = Sim::new(t);
            sim.answer_next("live");
            sim.advance_to((t + 60) as f64);
            let first = sim.next_read();
            sim.advance_to((t + answered) as f64);
            if answered < 120 {
                sim.answer(first, &earlier).unwrap();
                sim.advance_to((t + 120) as f64);
            } else {
                // The read timer at t+120 fired first: the later read is sent, then the
                // earlier's answer comes.
                assert_eq!(sim.codec.counts().reads, 3);
                sim.answer(first, &earlier).unwrap();
            }
            let second = sim.next_read();
            sim.advance_to((t + 125) as f64);
            sim.answer(second, &answer("live")).unwrap();
            sim.advance_to((t + 179) as f64);
            let seen = sim.since(0, t);
            assert_eq!(
                seen,
                each(0.0, &MARKETS, TRADING),
                "answered at t+{answered}"
            );
        }
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

/// The reads sent at t+60 and t+120 answered out of order ([`answered_out_of_order`]: the later
/// with `later`, the earlier with `earlier`), then the reads sent at t+180, t+240 and t+300, each
/// answered at once with `then`: what was reported.
fn out_of_order_then(earlier: &[u8], later: &[u8], then: &[u8]) -> Seen {
    let t = WEDNESDAY;
    let mut sim = answered_out_of_order(earlier, later);
    for at in [180, 240, 300] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, then).unwrap();
    }
    sim.since(0, t)
}

#[test]
fn a_future_open_a_read_answered_after_a_later_sent_one_advertises_holds_until_the_window_after_the_read_no_longer_advertising_it()
 {
    // Verifier VE-1, probe (a): the read sent at t+60 and answered at t+130, after the one sent
    // at t+120, is the only one to advertise ETH's open at t+200, and halts ETH. The schedule
    // keeps that open, so the read sent at t+180, which no longer advertises it, ends that halt
    // as one no schedule announces (V1C-2): the window runs from its judged time to t+300.
    let t = WEDNESDAY;
    let opens = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 200);
    let seen = out_of_order_then(&opens, &answer("live"), &answer("live"));
    assert_eq!(180 + AFTER, 300);
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((130.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_past_open_a_read_answered_after_a_later_sent_one_advertises_holds_until_the_window_after_the_read_no_longer_advertising_it()
 {
    // Verifier VE-1, probe (b): the same with ETH's open at t+100, already past when that read is
    // judged and the window after it running to t+220. The read sent at t+180, inside that
    // window, no longer advertises the open: it starts the window, to t+300, and ETH does not
    // trade at t+180.
    let t = WEDNESDAY;
    assert_eq!(100 + AFTER, 220);
    let opened = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 100);
    let seen = out_of_order_then(&opened, &answer("live"), &answer("live"));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((130.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_read_answered_after_a_later_sent_one_keeps_the_later_of_its_open_and_the_one_standing() {
    // Verifier VE-1: the read sent at t+120 and answered at t+125 advertises one open of ETH's,
    // which halts ETH, and the read sent at t+60 and answered at t+130 another, which holds it
    // too. Which shows the venue later is not known, so the schedule keeps the later open, which
    // holds every read the earlier holds: the read sent at t+180, advertising neither, is inside
    // its window and starts the window after a reopening, to t+300. Kept as t+50, whose window
    // ended at t+170, ETH would trade at t+180, whichever read advertised the later open: the
    // earlier-sent (t+100, its window to t+220) or the later-sent (t+250, to t+370).
    let t = WEDNESDAY;
    assert_eq!((50 + AFTER, 100 + AFTER), (170, 220));
    let open = |at: i64| advertising("ETH/USDT-P", "marketOpenTimestamp", t + at);
    for (earlier, later) in [(100, 50), (50, 250)] {
        let seen = out_of_order_then(&open(earlier), &open(later), &answer("live"));
        let mut expected = each(0.0, &MARKETS, TRADING);
        expected.push((125.0, ETH, HALTED));
        expected.push((300.0, ETH, TRADING));
        assert_eq!(
            seen, expected,
            "earlier-sent t+{earlier}, later-sent t+{later}"
        );
    }
}

#[test]
fn a_close_a_read_answered_after_a_later_sent_one_advertises_holds_until_the_window_after_the_read_no_longer_advertising_it()
 {
    // The close analogue of VE-1: the later-sent read advertises WIF's close at t+900, the
    // earlier, answered after it at t+130, at t+100, which halts WIF. The schedule keeps the
    // earlier close, so the read sent at t+180, advertising t+900 again, no longer advertises the
    // close that halted WIF: the window runs from its judged time to t+300.
    let t = WEDNESDAY;
    let early = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 100);
    let late = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 900);
    let seen = out_of_order_then(&early, &late, &late);
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((130.0, WIF, HALTED));
    expected.push((300.0, WIF, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_read_answered_after_a_later_sent_one_replaces_no_schedule_so_it_ends_no_halt_by_the_open_standing()
 {
    // Verifier VE-2: the read sent at t+120 and answered at t+125 advertises ETH's open at t+200,
    // which halts ETH; the read sent at t+60 and answered at t+130 advertises none, but it is
    // answered after a later-sent one, so it replaces no schedule and ends no halt by that open.
    // The open's own window (to t+320) ends the halt: every later read advertises the open, and
    // the read sent at t+360 lifts ETH. (Were that halt ended as one no schedule announces, the
    // first read that can lift ETH, the one sent at t+300, would start the window, to t+420.)
    let t = WEDNESDAY;
    let opens = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 200);
    let mut sim = answered_out_of_order(&answer("live"), &opens);
    // The read sent at t+180 is held until t+250 and answered just before the one sent at t+240:
    // neither can lift ETH, each sent before a halt by the open.
    sim.advance_to((t + 180) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 250) as f64);
    sim.answer(held, &opens).unwrap();
    let tag = sim.next_read();
    sim.answer(tag, &opens).unwrap();
    for at in [300, 360] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &opens).unwrap();
    }
    assert_eq!((200 + AFTER, 300 + AFTER), (320, 420));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((125.0, ETH, HALTED));
    expected.push((360.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_answered_after_a_later_sent_one_replaces_no_schedule_so_an_earlier_open_it_advertises_ends_no_halt()
 {
    // The read sent at t+120 and answered at t+125 advertises ETH's open at t+300, which halts
    // ETH and shows it not yet open as late as t+125; the read sent at t+60 and answered at t+130
    // advertises an open at t+50, earlier than that and holding its own span, but it replaces no
    // schedule, so it ends no halt (VE2-2's mark is a replacing read's). The read sent at t+180
    // and answered at t+185 moves the open to t+150, after t+125: the venue's reopening, whose
    // window ends at t+270, so the read sent at t+300 lifts ETH. (Were the halt ended at t+130 as
    // one no schedule announces, the read judged at t+185 would start the window, to t+305.)
    let t = WEDNESDAY;
    let open = |at: i64| advertising("ETH/USDT-P", "marketOpenTimestamp", t + at);
    let mut sim = answered_out_of_order(&open(50), &open(300));
    let moved = open(150);
    sim.advance_to((t + 180) as f64);
    let tag = sim.next_read();
    sim.advance_to((t + 185) as f64);
    sim.answer(tag, &moved).unwrap();
    for at in [240, 300] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &moved).unwrap();
    }
    assert_eq!((150 + AFTER, 185 + AFTER), (270, 305));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((125.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
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
    let bodies = [
        "eth_paused",
        "live",
        "live",
        "live",
        "live",
        "live",
        "live",
        "live",
        "live",
    ];
    for (k, name) in (1..).zip(bodies) {
        let sent = t + k * INTERVAL;
        sim.advance_to((sent + 90) as f64);
        let (tag, asked) = sim.requests.remove(0);
        assert_eq!(asked, sent * NS);
        sim.answer(tag, &answer(name)).unwrap();
    }
    // PAUSED, judged at t+150; the read sent at t+120, before that halt, lifts nothing; the one
    // sent at t+180, after it, shows ETH reopened when judged at t+270, so every read sent
    // before t+390 leaves it halted, the last of them (sent at t+360) when judged at t+450; the
    // read sent at t+480, after that, lifts it when judged at t+570.
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, ETH, HALTED));
    expected.push((570.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_maintenance_the_exchange_shows_past_its_window_holds_every_market_after_its_observed_end() {
    // RA214-2, probe 1: the window t+290 to t+330, first published at t+300, and the window
    // after it to t+450; the exchange still in SCHEDULED_MAINTENANCE at the read sent at t+480,
    // past both, and NORMAL at t+540. The venue reopened at some instant after the first and no
    // later than the second's judged time, so its call auction may run to t+660.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    for at in [60, 120, 180, 240] {
        sim.read_at(t + at, "live");
    }
    for at in [300, 360, 420, 480] {
        sim.read_at(t + at, "scheduled_maintenance");
    }
    for at in [540, 600, 660] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(300.0, &MARKETS, HALTED));
    assert_eq!(540 + AFTER, 660);
    expected.extend(each(660.0, &MARKETS, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_late_fx_reopening_holds_the_pair_for_the_window_after_the_read_that_shows_it() {
    // RA214-2, probe 2: the calendar reopens EUR at t+120 and its window ends at t+240, but the
    // venue lists EUR PAUSED through the read sent at t+300 and LIVE from t+360: halted until a
    // read sent the window after that one, at t+480.
    let t = FX_REOPEN - 120;
    let paused = with_contract(answer_json("live"), "EUR/USDT-P", |c| {
        c["status"] = json!("PAUSED")
    });
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(tag, &paused).unwrap();
    for k in 1..=5 {
        sim.advance_to((t + k * INTERVAL) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &paused).unwrap();
    }
    for at in [360, 420, 480] {
        sim.read_at(t + at, "live");
    }
    let expected = vec![
        (0.0, EUR, HALTED),
        (0.0, ETH, TRADING),
        (0.0, WIF, TRADING),
        (480.0, EUR, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_sent_before_a_status_halt_and_showing_trading_after_it_shows_no_reopening() {
    // The read sent at t+60 is answered LIVE at t+130, after the one sent at t+120 showed ETH
    // PAUSED at t+125: which of the two shows the venue later is not known, so it shows no
    // reopening. The read sent at t+180, judged at t+230, does: the window after it runs to
    // t+350, so the read sent at t+300 still leaves ETH halted and the one sent at t+360 lifts
    // it. (Counted from t+130, the window would end at t+250, and t+300 would lift it.)
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    sim.advance_to((t + 120) as f64);
    let (earlier, later) = (sim.next_read(), sim.next_read());
    sim.advance_to((t + 125) as f64);
    sim.answer(later, &answer("eth_paused")).unwrap();
    sim.advance_to((t + 130) as f64);
    sim.answer(earlier, &answer("live")).unwrap();
    for (sent, answered) in [(180, 230), (240, 245), (300, 300), (360, 360)] {
        sim.advance_to((t + sent) as f64);
        let tag = sim.next_read();
        sim.advance_to((t + answered) as f64);
        sim.answer(tag, &answer("live")).unwrap();
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((125.0, ETH, HALTED));
    expected.push((360.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_close_or_an_open_a_later_read_no_longer_advertises_holds_the_market_for_the_window_after_that_read()
 {
    // V1C-2: the codec cannot tell a close or an open the venue withdrew from one it passed and
    // then reopened the market after, so a halt by that close, or before that open, that ends at
    // a read no longer advertising it ends as a halt no schedule announces: the first read that
    // can lift the market and shows it trading starts the window after a reopening.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 30);
    sim.answer(tag, &closes).unwrap();
    // WIF's close halts it at t+30; the read sent at t+60 advertises none, so the window runs
    // from its judged time to t+180.
    sim.read_at(t + 60, "live");
    // ETH's open at t+300, advertised by the read sent at t+120, halts it; the read sent at
    // t+180 advertises none, so the window runs from its judged time to t+300.
    sim.advance_to((t + 120) as f64);
    let tag = sim.next_read();
    let opens = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 300);
    sim.answer(tag, &opens).unwrap();
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    assert_eq!((60 + AFTER, 180 + AFTER), (180, 300));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((30.0, WIF, HALTED));
    expected.push((120.0, ETH, HALTED));
    expected.push((180.0, WIF, TRADING));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_later_close_or_an_earlier_open_past_its_window_holds_the_market_as_none_advertised_does() {
    // V1C-2: a read advertising a close later than its judged time, or an open whose window
    // ended by its send, no longer advertises the close or open that halted the market either:
    // the window runs from its judged time.
    let t = WEDNESDAY;
    // `live.json` with ETH's open, if any, and WIF's close (Unix seconds).
    let body = |open: Option<i64>, close: i64| {
        let doc = advertising("WIF/USDT-P", "marketCloseTimestamp", close);
        let doc: Value = serde_json::from_slice(&doc).unwrap();
        with_contract(doc, "ETH/USDT-P", |c| {
            c["marketOpenTimestamp"] = json!(open.map(|o| o.to_string()))
        })
    };
    // WIF's close at t+30 halts it; from the read sent at t+60, every read advertises it closing
    // at t+900 instead. ETH's open at t+300, advertised by the read sent at t+120, halts it; from
    // the read sent at t+180, every read advertises it opening at t-600, its window long past.
    let script = [
        (0, body(None, t + 30)),
        (60, body(None, t + 900)),
        (120, body(Some(t + 300), t + 900)),
        (180, body(Some(t - 600), t + 900)),
        (240, body(Some(t - 600), t + 900)),
        (300, body(Some(t - 600), t + 900)),
    ];
    let mut sim = Sim::new(t);
    for (at, body) in script {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &body).unwrap();
    }
    assert_eq!((60 + AFTER, 180 + AFTER), (180, 300));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((30.0, WIF, HALTED));
    expected.push((120.0, ETH, HALTED));
    expected.push((180.0, WIF, TRADING));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_held_past_a_close_it_no_longer_advertises_lifts_nothing_and_the_next_read_starts_the_window()
 {
    // V1C-2: the read sent at t+60, before WIF's close at t+90, and answered at t+100 without
    // it may show the venue after the close: it cannot lift the halt at the close, and the first
    // read that can (sent at t+120) starts the window, to t+240.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(
        tag,
        &advertising("WIF/USDT-P", "marketCloseTimestamp", t + 90),
    )
    .unwrap();
    sim.advance_to((t + 60) as f64);
    let held = sim.next_read();
    sim.advance_to((t + 100) as f64);
    sim.answer(held, &answer("live")).unwrap();
    for at in [120, 180, 240] {
        sim.read_at(t + at, "live");
    }
    assert_eq!(120 + AFTER, 240);
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((90.0, WIF, HALTED));
    expected.push((240.0, WIF, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_halt_before_an_open_a_later_read_moves_ends_the_window_after_the_open_it_advertises() {
    // The schedule still ends this halt: the read sent at t+180 and answered at t+190 moves
    // ETH's open from t+300 to t+170, which still holds it (to t+290), and the reads after it
    // advertise t+170 too, so the window counts from that open and the read sent at t+300
    // lifts ETH. (Counted from t+190, the read moving it, it would run to t+310, past that read.)
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.read_at(t + 60, "live");
    sim.advance_to((t + 120) as f64);
    let tag = sim.next_read();
    sim.answer(
        tag,
        &advertising("ETH/USDT-P", "marketOpenTimestamp", t + 300),
    )
    .unwrap();
    let moved = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 170);
    sim.advance_to((t + 180) as f64);
    let tag = sim.next_read();
    sim.advance_to((t + 190) as f64);
    sim.answer(tag, &moved).unwrap();
    for at in [240, 300] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &moved).unwrap();
    }
    assert_eq!((170 + AFTER, 190 + AFTER), (290, 310));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((120.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_past_open_a_later_read_no_longer_advertises_halts_no_trading_market() {
    // WIF opened at t+30 and trades from the read sent at t+180, after the window; the read sent
    // at t+240 no longer advertises the open, which held no read sent from t+150: nothing halts.
    let t = WEDNESDAY;
    let opened = advertising("WIF/USDT-P", "marketOpenTimestamp", t + 30);
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(tag, &opened).unwrap();
    for at in [60, 120, 180] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &opened).unwrap();
    }
    for at in [240, 300, 360] {
        sim.read_at(t + at, "live");
    }
    let expected = vec![
        (0.0, ETH, TRADING),
        (0.0, EUR, TRADING),
        (0.0, WIF, HALTED),
        (180.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

/// Each read sent at `t` plus each `(sent, answered, body)`, in order, answered with `body` at
/// `t` plus `answered`, from the first (sent at `t`): what was reported.
fn script(t: i64, reads: &[(i64, i64, &[u8])]) -> Seen {
    let mut sim = Sim::new(t);
    for &(sent, answered, body) in reads {
        sim.advance_to((t + sent) as f64);
        let tag = sim.next_read();
        sim.advance_to((t + answered) as f64);
        sim.answer(tag, body).unwrap();
    }
    sim.since(0, t)
}

#[test]
fn an_open_earlier_than_the_close_that_halted_the_market_holds_it_for_the_window_after_the_read_advertising_it()
 {
    // Verifier VE2-1, probes A and A2: WIF's close at t+30 halts it; the read sent at t+60
    // advertises no close and an open before t+30 (t-30, its window to t+90, or t, to t+120),
    // which holds that read. Any reopening after the close is no earlier than t+30, so that open
    // is not one: the codec cannot tell a close the venue withdrew from one it passed, and the
    // halt ends as one no schedule announces, the window counted from that read's judged time to
    // t+180, whether the later reads advertise that open or nothing.
    let t = WEDNESDAY;
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 30);
    let live = answer("live");
    assert_eq!(60 + AFTER, 180);
    for open in [t - 30, t] {
        let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", open);
        for later in [&live, &opens] {
            let mut reads = vec![(0, 0, &closes[..]), (60, 60, &opens[..])];
            reads.extend([120, 180, 240].map(|at| (at, at, &later[..])));
            let mut expected = each(0.0, &MARKETS, TRADING);
            expected.push((30.0, WIF, HALTED));
            expected.push((180.0, WIF, TRADING));
            assert_eq!(script(t, &reads), expected, "open t{:+}", open - t);
        }
    }
}

#[test]
fn a_close_a_read_drops_while_still_advertising_an_earlier_open_holds_the_market_for_the_window_after_that_read()
 {
    // Verifier VE2-1, probe A3: the first read advertises WIF's open at t-30, its window to t+90,
    // and its close at t+30; the read sent at t+60 drops only the close. That open, before the
    // close, is not the reopening after it, so the window runs from that read's judged time.
    let t = WEDNESDAY;
    let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", t - 30);
    let both = with_contract(serde_json::from_slice(&opens).unwrap(), "WIF/USDT-P", |c| {
        c["marketCloseTimestamp"] = json!((t + 30).to_string())
    });
    let mut reads = vec![(0, 0, &both[..])];
    reads.extend([60, 120, 180].map(|at| (at, at, &opens[..])));
    let expected = vec![
        (0.0, ETH, TRADING),
        (0.0, EUR, TRADING),
        (0.0, WIF, HALTED),
        (180.0, WIF, TRADING),
    ];
    assert_eq!(script(t, &reads), expected);
}

#[test]
fn an_open_earlier_than_a_read_still_showing_the_market_closed_holds_it_for_the_window_after_the_read_advertising_it()
 {
    // VE2-1, the rest of the class: WIF's close at t+90 halts it; the read sent at t+120 and held
    // until t+150 still advertises that close, so it may show WIF closed as late as t+150; the
    // read sent at t+180 and answered at t+185 advertises instead an open at t+130, after the
    // close but before t+150, which holds that read. Any reopening is no earlier than the latest
    // instant a close showed WIF halted, so that open is not one: the window runs from t+185 to
    // t+305, and the read sent at t+360 lifts WIF. (Taken as the reopening, as an open no earlier
    // than the close alone would be, its window would end at t+250 and the read sent at t+300
    // would lift it.) The read sent at t+240 and answered at t+245 advertises that open again,
    // still holding it: once a read showed WIF reopened, what showed it halted before is behind
    // that read's window, so the same open ends no halt again (counted from t+245, the window
    // would run to t+365, past the read sent at t+360).
    let t = WEDNESDAY;
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 90);
    let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", t + 130);
    let mut reads = vec![
        (0, 0, &closes[..]),
        (60, 60, &closes[..]),
        (120, 150, &closes[..]),
        (180, 185, &opens[..]),
        (240, 245, &opens[..]),
    ];
    reads.extend([300, 360].map(|at| (at, at, &opens[..])));
    assert_eq!((130 + AFTER, 185 + AFTER, 245 + AFTER), (250, 305, 365));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((90.0, WIF, HALTED));
    expected.push((360.0, WIF, TRADING));
    assert_eq!(script(t, &reads), expected);
}

#[test]
fn an_open_no_earlier_than_every_instant_a_close_or_an_open_showed_the_market_halted_is_its_reopening()
 {
    // The schedule still ends a halt by a close through an open the venue advertises after it:
    // WIF's close at t+30 halts it, and the read sent at t+60 and answered at t+70 advertises
    // instead an open at t+30 or t+40, no earlier than the close, the latest instant anything
    // showed WIF halted (no read showed the close passed; the timers at t+30 and t+60 halted WIF
    // by it), and every later read the same open. The window after that open ends at t+150 or
    // t+160, so the read sent at t+180 lifts WIF. (Counted from t+70, the window would end at
    // t+190, past that read.)
    let t = WEDNESDAY;
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 30);
    assert_eq!((30 + AFTER, 40 + AFTER, 70 + AFTER), (150, 160, 190));
    for open in [t + 30, t + 40] {
        let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", open);
        let mut reads = vec![(0, 0, &closes[..]), (60, 70, &opens[..])];
        reads.extend([120, 180].map(|at| (at, at, &opens[..])));
        let mut expected = each(0.0, &MARKETS, TRADING);
        expected.push((30.0, WIF, HALTED));
        expected.push((180.0, WIF, TRADING));
        assert_eq!(script(t, &reads), expected, "open t+{}", open - t);
    }
}

#[test]
fn a_read_still_advertising_the_close_that_halted_the_market_keeps_it_halted_by_that_close_though_it_cannot_lift_it()
 {
    // WIF's close at t+90 halts it while the read sent at t+60 is in flight; that read, answered
    // at t+100, still advertises the close, so it keeps WIF halted by it (and shows it closed as
    // late as t+100) and ends no halt, though it cannot lift WIF. The read sent at t+120 and
    // answered at t+125 advertises instead an open at t+100, no earlier than that: the venue's
    // reopening, whose window ends at t+220, so the read sent at t+240 lifts WIF. (Had the read
    // judged at t+100 ended the halt as one no schedule announces, the read judged at t+125 would
    // start the window, to t+245.)
    let t = WEDNESDAY;
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 90);
    let opens = advertising("WIF/USDT-P", "marketOpenTimestamp", t + 100);
    let mut reads = vec![
        (0, 0, &closes[..]),
        (60, 100, &closes[..]),
        (120, 125, &opens[..]),
    ];
    reads.extend([180, 240].map(|at| (at, at, &opens[..])));
    assert_eq!((100 + AFTER, 125 + AFTER), (220, 245));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((90.0, WIF, HALTED));
    expected.push((240.0, WIF, TRADING));
    assert_eq!(script(t, &reads), expected);
}

#[test]
fn an_open_a_read_moves_before_an_earlier_read_showed_the_market_not_yet_open_holds_it_for_the_window_after_that_read()
 {
    // Verifier VE2-2, probe B: the read sent at t+120 advertises ETH's open at t+300, so the
    // venue had not opened ETH by then; the read sent at t+180 moves it to t+70, its window to
    // t+190, which holds that read. Any reopening came after t+120, so that open is not one: the
    // window runs from t+180 to t+300.
    let t = WEDNESDAY;
    let live = answer("live");
    let later = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 300);
    let moved = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 70);
    let mut reads = vec![
        (0, 0, &live[..]),
        (60, 60, &live[..]),
        (120, 120, &later[..]),
    ];
    reads.extend([180, 240, 300].map(|at| (at, at, &moved[..])));
    assert_eq!((70 + AFTER, 180 + AFTER), (190, 300));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((120.0, ETH, HALTED));
    expected.push((300.0, ETH, TRADING));
    assert_eq!(script(t, &reads), expected);
    // The read advertising t+300 sent at t+120 and held until t+150: it may show ETH not yet open
    // as late as t+150, so the read sent at t+180 and answered at t+185 moving the open to t+130
    // does not move the reopening there: the window runs from t+185 to t+305, and the read sent
    // at t+360 lifts ETH. (Counted from t+130, the window would end at t+250.)
    let moved = advertising("ETH/USDT-P", "marketOpenTimestamp", t + 130);
    let mut reads = vec![
        (0, 0, &live[..]),
        (60, 60, &live[..]),
        (120, 150, &later[..]),
        (180, 185, &moved[..]),
    ];
    reads.extend([240, 300, 360].map(|at| (at, at, &moved[..])));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((150.0, ETH, HALTED));
    expected.push((360.0, ETH, TRADING));
    assert_eq!(script(t, &reads), expected);
}

/// `body` publishing the maintenance window from t+30 to t+100 ([`WEDNESDAY`] as t).
fn in_window(body: &[u8]) -> Vec<u8> {
    let t = WEDNESDAY;
    let mut doc: Value = serde_json::from_slice(body).unwrap();
    doc["maintenanceWindow"] = json!([{"begin": t + 30, "end": t + 100, "note": "Synthetic"}]);
    doc.to_string().into_bytes()
}

/// The window from t+30 to t+100 published at t; the read sent at t+60 settled at t+70 by
/// `settle`; the read sent at t+120 answered at t+130 LIVE with the window; the reads sent at
/// t+180, t+240 and t+300 answered at once LIVE: what was reported.
fn in_window_then(settle: impl FnOnce(&mut Sim, HttpTag)) -> Seen {
    let t = WEDNESDAY;
    let window = in_window(&answer("live"));
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    sim.answer(tag, &window).unwrap();
    sim.advance_to((t + 60) as f64);
    let tag = sim.next_read();
    sim.advance_to((t + 70) as f64);
    settle(&mut sim, tag);
    sim.advance_to((t + 120) as f64);
    let tag = sim.next_read();
    sim.advance_to((t + 130) as f64);
    sim.answer(tag, &window).unwrap();
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    sim.since(0, t)
}

#[test]
fn a_failed_read_holds_markets_already_halted_for_the_window_after_the_first_read_sent_after_it() {
    // Verifier VE2-3: a failed read shows nothing of the venue, whatever already halts the
    // markets. The maintenance window halts every market at t+30 and, with the window after it,
    // holds them until t+220; the read sent at t+60 fails at t+70; the read sent at t+120,
    // answered at t+130, starts the window after a reopening, to t+250, so the read sent at
    // t+240 leaves every market halted and the one sent at t+300 lifts them.
    assert_eq!((100 + AFTER, 130 + AFTER), (220, 250));
    let seen = in_window_then(|sim, tag| sim.respond(tag, Err(HttpFailure::TimedOut)).unwrap());
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(30.0, &MARKETS, HALTED));
    expected.extend(each(300.0, &MARKETS, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_read_leaving_out_a_market_already_halted_holds_it_for_the_window_after_the_first_read_listing_it_again()
 {
    // Verifier VE2-3: a read leaving a market out shows nothing of it, whatever already halts it.
    // The read sent at t+60 and answered at t+70, inside the maintenance window, leaves WIF out:
    // ETH and EUR trade again from the read sent at t+240, after the window and the window after
    // it (t+220), and WIF only from the one sent at t+300, after the window following the read
    // judged at t+130 that lists it again.
    let left_out = in_window(&leaving_out("WIF/USDT-P"));
    let seen = in_window_then(|sim, tag| sim.answer(tag, &left_out).unwrap());
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.extend(each(30.0, &MARKETS, HALTED));
    expected.extend(each(240.0, &[ETH, EUR], TRADING));
    expected.push((300.0, WIF, TRADING));
    assert_eq!(seen, expected);
}

#[test]
fn a_market_subscribed_again_is_halted_at_once_and_held_for_the_window_after_the_first_read_sent_after_it()
 {
    // Verifier VE2-4, probe F: WIF's close at t+30 halts it; at t+40 its last subscription is
    // removed and taken again. No read was judged for it meanwhile, so the venue may have halted
    // and reopened it, as across a failed read: it is reported halted at once, and the read sent
    // at t+60, the first after that, starts the window after a reopening, to t+180, as it would
    // have had WIF stayed subscribed (a read no longer advertising the close).
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    let tag = sim.next_read();
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 30);
    sim.answer(tag, &closes).unwrap();
    sim.advance_to((t + 40) as f64);
    sim.subscribe(&[], &[WIF_MARK]);
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to((t + 40) as f64);
    for at in [60, 120, 180] {
        sim.read_at(t + at, "live");
    }
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((30.0, WIF, HALTED));
    expected.push((40.0, WIF, HALTED));
    expected.push((180.0, WIF, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_read_sent_before_a_market_is_subscribed_again_starts_no_window_for_it() {
    // VE2-4: WIF's last subscription removed and taken again at t+61, while the read sent at
    // t+60 is in flight. That read, answered LIVE at t+62, may show the venue before a halt and
    // reopening no read was judged for, so it lifts nothing and starts no window (from t+62 the
    // window would end at t+182); the read sent at t+120, answered at t+125, starts it, to t+245,
    // and the read sent at t+300 lifts WIF.
    let t = WEDNESDAY;
    let mut sim = Sim::new(t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    let in_flight = sim.next_read();
    sim.advance_to((t + 61) as f64);
    sim.subscribe(&[], &[WIF_MARK]);
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to((t + 62) as f64);
    sim.answer(in_flight, &answer("live")).unwrap();
    sim.advance_to((t + 120) as f64);
    let tag = sim.next_read();
    sim.advance_to((t + 125) as f64);
    sim.answer(tag, &answer("live")).unwrap();
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    assert_eq!((62 + AFTER, 125 + AFTER), (182, 245));
    let mut expected = each(0.0, &MARKETS, TRADING);
    expected.push((61.0, WIF, HALTED));
    expected.push((300.0, WIF, TRADING));
    assert_eq!(sim.since(0, t), expected);
}

/// ETH's trades alone.
fn eth_trades() -> BTreeSet<Subscription> {
    BTreeSet::from([Subscription {
        inst: ETH,
        feed: Feed::Trades,
    }])
}

#[test]
fn a_market_subscribed_once_the_reads_have_begun_is_held_and_no_read_sent_before_it_lifts_it() {
    // Verifier VE5-1's probe: ETH's trades alone subscribed; the read sent at t answered LIVE; the
    // read sent at t+60 held; the read sent at t+120 shows WIF paused, leaves it out, or fails, at
    // t+121; WIF is subscribed for the first time at t+122, and the read held since t+60 is
    // answered LIVE at t+123. That read may show the venue before the pause, so it cannot report
    // WIF: WIF is held, halted at t+122, and trades only from the read sent the window after the
    // first read sent after its subscription (t+180), at t+300. (Subscribed afresh, as before, it
    // traded at t+123.)
    let t = WEDNESDAY;
    let paused = with_contract(answer_json("live"), "WIF/USDT-P", |c| {
        c["status"] = json!("PAUSED")
    });
    let left_out = leaving_out("WIF/USDT-P");
    assert_eq!(180 + AFTER, 300);
    for what in ["paused", "left out", "failed"] {
        let mut sim = Sim::with(&common::config(), &eth_trades(), t);
        sim.answer_next("live");
        sim.advance_to((t + 60) as f64);
        let held = sim.next_read();
        sim.advance_to((t + 120) as f64);
        let shows = sim.next_read();
        sim.advance_to((t + 121) as f64);
        match what {
            "paused" => sim.answer(shows, &paused).unwrap(),
            "left out" => sim.answer(shows, &left_out).unwrap(),
            _ => sim.respond(shows, Err(HttpFailure::TimedOut)).unwrap(),
        }
        sim.advance_to((t + 122) as f64);
        sim.subscribe(&[WIF_MARK], &[]);
        sim.advance_to((t + 122) as f64);
        sim.advance_to((t + 123) as f64);
        sim.answer(held, &answer("live")).unwrap();
        for at in [180, 240, 300] {
            sim.read_at(t + at, "live");
        }
        let mut expected = vec![(0.0, ETH, TRADING)];
        if what == "failed" {
            expected.push((121.0, ETH, HALTED));
        }
        expected.push((122.0, WIF, HALTED));
        if what == "failed" {
            expected.push((300.0, ETH, TRADING));
        }
        expected.push((300.0, WIF, TRADING));
        assert_eq!(sim.since(0, t), expected, "{what}");
    }
}

#[test]
fn a_market_subscribed_once_the_reads_have_begun_trades_only_the_window_after_a_read_sent_after_it()
{
    // VE5-1's whole class: a read sent after the subscription is not enough. The read sent at
    // t+60 shows WIF paused while only ETH is subscribed, and the one sent at t+120 shows it LIVE:
    // WIF reopened between, its call auction running up to t+240. WIF is subscribed at t+130; the
    // read sent at t+180 is the first that can report it, and shows it LIVE, so the window runs
    // from its judged time to t+300. (Only a halt counted at the subscription, the suggested fix,
    // let the read sent at t+180 report WIF trading inside that auction.)
    let t = WEDNESDAY;
    let paused = with_contract(answer_json("live"), "WIF/USDT-P", |c| {
        c["status"] = json!("PAUSED")
    });
    let mut sim = Sim::with(&common::config(), &eth_trades(), t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    let tag = sim.next_read();
    sim.answer(tag, &paused).unwrap();
    sim.read_at(t + 120, "live");
    sim.advance_to((t + 130) as f64);
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to((t + 130) as f64);
    for at in [180, 240, 300] {
        sim.read_at(t + at, "live");
    }
    let expected = vec![
        (0.0, ETH, TRADING),
        (130.0, WIF, HALTED),
        (300.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

#[test]
fn a_market_subscribed_later_is_checked_at_once_against_the_closes_answers_advertised_before() {
    // Verifier VE5-2: the read sent at t+60 advertises WIF's close at t+100 while only ETH is
    // subscribed; WIF is subscribed at t+70. The schedule is the endpoint's, so the check asked for
    // then arms the halt at that close (before, a fresh market's schedule was empty, so no timer
    // came at t+100, and the first report waited for the read sent at t+120). WIF is held, halted
    // at t+70; the reads sent at t+120 and t+180 still advertise the passed close; the one sent at
    // t+240 lists WIF LIVE without it, so it ends that halt as one no schedule announces and the
    // window runs from it to t+360.
    let t = WEDNESDAY;
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 100);
    let mut sim = Sim::with(&common::config(), &eth_trades(), t);
    sim.answer_next("live");
    sim.advance_to((t + 60) as f64);
    let tag = sim.next_read();
    sim.answer(tag, &closes).unwrap();
    sim.advance_to((t + 70) as f64);
    let armed = sim.armed.len();
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to((t + 70) as f64);
    let close_check = sim.armed[armed..]
        .iter()
        .any(|&(due, _)| due == (t + 100) * NS);
    assert!(
        close_check,
        "no check armed at the close: {:?}",
        &sim.armed[armed..]
    );
    for at in [120, 180] {
        sim.advance_to((t + at) as f64);
        let tag = sim.next_read();
        sim.answer(tag, &closes).unwrap();
    }
    for at in [240, 300, 360] {
        sim.read_at(t + at, "live");
    }
    assert_eq!(240 + AFTER, 360);
    let expected = vec![
        (0.0, ETH, TRADING),
        (70.0, WIF, HALTED),
        (360.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
    assert_eq!(
        sim.codec.counts().scheduled_halts,
        0,
        "WIF was halted already"
    );
}

#[test]
fn a_market_removed_and_subscribed_again_before_the_reads_begin_is_subscribed_afresh() {
    // No read has been sent, so nothing went unwatched: the first read reports WIF trading.
    let t = WEDNESDAY;
    let mut sim = Sim::unstarted(&common::config(), &subs(), t);
    sim.subscribe(&[], &[WIF_MARK]);
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to(t as f64);
    sim.answer_next("live");
    sim.read_at(t + INTERVAL, "live");
    assert_eq!(sim.since(0, t), each(0.0, &MARKETS, TRADING));
}

#[test]
fn a_market_subscribed_again_after_every_subscription_was_removed_is_held_by_the_poll_that_stays_open()
 {
    // Verifier VE3-1: the consumer holds only WIF's mark. The first read advertises WIF's close
    // at t+30, which halts it; at t+40 the consumer removes every subscription, and takes WIF's
    // back at once or at t+100, past the read due at t+60, which is not sent while no market is
    // subscribed. The status poll is planned for the empty set too, alone, on the same stream and
    // transport, so the runtime's `MdVenue` keeps its endpoint and this codec: WIF is held as
    // across a failed read, halted when it is taken back and trading only from a read sent the
    // window after the first read sent after that. (Closed with the empty set, the endpoint would
    // be opened again with a fresh codec, whose first read, listing WIF LIVE, would report it
    // trading at once, inside the call auction a reopening after the close may run.)
    let t = WEDNESDAY;
    let cfg = common::config();
    let specs = specs();
    let wif = BTreeSet::from([WIF_MARK]);
    let status_poll = |subs: &BTreeSet<Subscription>| {
        let plans = Hibachi.plan_md(&cfg, &specs, subs).unwrap();
        plans.into_iter().find(|p| p.stream == STATUS_STREAM)
    };
    let open = status_poll(&wif).unwrap();
    let closes = advertising("WIF/USDT-P", "marketCloseTimestamp", t + 30);
    assert_eq!((60 + AFTER, 120 + AFTER), (180, 240));
    for (taken_back, first_after, lifted) in [(40, 60, 180), (100, 120, 240)] {
        let mut sim = Sim::with(&cfg, &wif, t);
        let tag = sim.next_read();
        sim.answer(tag, &closes).unwrap();
        sim.advance_to((t + 40) as f64);
        // Every subscription removed: the status poll is still planned, with none.
        let kept =
            status_poll(&BTreeSet::new()).expect("the status poll planned for no subscription");
        assert_eq!((&kept.transport, kept.subs.len()), (&open.transport, 0));
        sim.subscribe(&[], &[WIF_MARK]);
        sim.advance_to((t + taken_back) as f64);
        sim.subscribe(&[WIF_MARK], &[]);
        sim.advance_to((t + taken_back) as f64);
        // The read due while no market was subscribed was not sent.
        assert_eq!(sim.codec.counts().reads, 1, "taken back at t+{taken_back}");
        for at in (first_after..=lifted).step_by(60) {
            sim.read_at(t + at, "live");
        }
        let expected = vec![
            (0.0, WIF, TRADING),
            (30.0, WIF, HALTED),
            (taken_back as f64, WIF, HALTED),
            (lifted as f64, WIF, TRADING),
        ];
        assert_eq!(sim.since(0, t), expected, "taken back at t+{taken_back}");
    }
}

#[test]
fn a_market_taken_again_once_the_reads_have_begun_is_held_though_its_last_subscription_was_removed_before_they_began()
 {
    // Verifier VE3-2 had WIF subscribed afresh here: its last subscription is removed before the
    // first read and taken again at t+10, after it, so no read was judged while WIF was
    // subscribed. But that read was judged for the markets subscribed then and showed WIF's
    // contract too, and what it showed of WIF, as anything a read judged while WIF had no
    // subscription shows, was judged for no market of it (Verifier VE5-1). WIF is held as every
    // market subscribed once the reads have begun is: halted at t+10, and trading only from a
    // read sent the window after the first read sent after that (t+60), at t+180.
    let t = WEDNESDAY;
    let mut sim = Sim::unstarted(&common::config(), &subs(), t);
    sim.subscribe(&[], &[WIF_MARK]);
    sim.advance_to(t as f64);
    sim.answer_next("live");
    sim.advance_to((t + 10) as f64);
    sim.subscribe(&[WIF_MARK], &[]);
    sim.advance_to((t + 10) as f64);
    for at in [60, 120, 180] {
        sim.read_at(t + at, "live");
    }
    assert_eq!(60 + AFTER, 180);
    let expected = vec![
        (0.0, ETH, TRADING),
        (0.0, EUR, TRADING),
        (10.0, WIF, HALTED),
        (180.0, WIF, TRADING),
    ];
    assert_eq!(sim.since(0, t), expected);
}

/// Each change of subscriptions a test takes: at seconds after the origin, what is added and what
/// is removed.
type Change = (i64, &'static [Subscription], &'static [Subscription]);

/// WIF's mark taken at t+5, the control of [`WIF_CHURNED`].
const WIF_TAKEN: &[Change] = &[(5, &[WIF_MARK], &[])];

/// WIF's mark taken at t+5, removed at t+6 and taken again at t+7.
const WIF_CHURNED: &[Change] = &[
    (5, &[WIF_MARK], &[]),
    (6, &[], &[WIF_MARK]),
    (7, &[WIF_MARK], &[]),
];

/// The poll's codec opened at `t` on `first`, `changes` taken each at its instant (the schedule
/// check each asks for fired then), and the reads sent at t+60, t+120 and t+180 answered LIVE at
/// once: what it reported, and how many reads it sent before t+60.
fn churned(t: i64, first: &BTreeSet<Subscription>, changes: &[Change]) -> (Seen, u64) {
    let mut sim = Sim::with(&common::config(), first, t);
    if !first.is_empty() {
        sim.answer_next("live");
    }
    for &(at, add, remove) in changes {
        sim.advance_to((t + at) as f64);
        sim.subscribe(add, remove);
        sim.advance_to((t + at) as f64);
    }
    sim.advance_to((t + INTERVAL - 1) as f64);
    let before = sim.codec.counts().reads;
    for at in [60, 120, 180] {
        sim.read_at(t + at, "live");
    }
    (sim.since(0, t), before)
}

#[test]
fn a_market_subscribed_removed_and_taken_again_before_the_poll_sends_a_read_is_subscribed_afresh() {
    // Verifier VE4-1, probe 2: the poll opens with no market subscribed, so its first timer, at t,
    // sends no read, and the reads have not begun: they begin with the first read the poll sends,
    // not its first timer. WIF is taken at t+5, removed at t+6 and taken again at t+7, all before
    // the first read is sent at t+60. No read was ever sent, so nothing of WIF went unwatched: it
    // is subscribed afresh, as when it is simply taken at t+5 (the control), with no halt at t+7
    // and no window, and the read sent at t+60 reports it trading. (Taken to begin at the first
    // timer, the reads would hold WIF from t+7 to the read sent at t+180.)
    let t = WEDNESDAY;
    for (what, changes) in [("taken", WIF_TAKEN), ("churned", WIF_CHURNED)] {
        let (seen, before) = churned(t, &BTreeSet::new(), changes);
        assert_eq!(before, 0, "{what}: a read sent before t+60");
        assert_eq!(seen, [(60.0, WIF, TRADING)], "{what}");
    }
}

#[test]
fn the_reads_begin_with_the_first_read_sent_whichever_markets_it_was_sent_for() {
    // VE4-1's counterpart: the poll opens on ETH's trades alone, and its first timer, at t, sends
    // a read for ETH. The reads have begun, so WIF, taken at t+5 for the first time (WIF_TAKEN)
    // or taken at t+5, removed at t+6 and taken again at t+7 (WIF_CHURNED), is subscribed once
    // they had begun, not afresh: halted each time it is taken, and trading only from the read
    // sent the window after the first read sent after that (t+60), at t+180 (Verifier VE5-1: a
    // first subscription is held as one taken again).
    let t = WEDNESDAY;
    let eth = BTreeSet::from([Subscription {
        inst: ETH,
        feed: Feed::Trades,
    }]);
    assert_eq!(60 + AFTER, 180);
    for (what, changes, halts) in [
        ("taken", WIF_TAKEN, &[5.0][..]),
        ("churned", WIF_CHURNED, &[5.0, 7.0][..]),
    ] {
        let (seen, before) = churned(t, &eth, changes);
        assert_eq!(before, 1, "{what}");
        let mut expected = vec![(0.0, ETH, TRADING)];
        expected.extend(halts.iter().map(|&at| (at, WIF, HALTED)));
        expected.push((180.0, WIF, TRADING));
        assert_eq!(seen, expected, "{what}");
    }
}
