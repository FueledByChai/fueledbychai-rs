//! FBC-ou9g's function-level proof of the flatten (decisions 0012, 0063, 0148 items 5, 6, 8 and
//! 9, 0151 items 5, 8, 9 and 10): its next step, its price within the cap on the grid, how an IOC
//! came back, the stop it gives its session, what its record says and never says, the owner's
//! Flatten on an armed market, the handler's expected fills, and the QFEX model's answers to a
//! limit IOC and its position frames. Declared by path in `tests/qfex_preprod_probe.rs`. The runs
//! that reach each of these are in `place_stages.rs`; every value here is made up.

use super::*;

use std::str::FromStr as _;

use fbc_core::{Lots, NewOrder, OrderKind, PriceGrid, Side, SignedLots, Ticks, Tif};
use fbc_oms::{OrdState, TerminalKind};
use flatten::{FlattenRun, Ioc, IocRun, Next};
use market::Quote;
use model::{IocAnswer, Listed, PositionPush, QfexModel};
use places::{Flattening, Stop};
use rust_decimal::Decimal;

/// The model's fresh touch in ticks of a cent: 100.00 bid, 100.10 asked.
const QUOTE: Quote = Quote {
    bid: Ticks(10_000),
    ask: Ticks(10_010),
};

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

#[test]
fn the_flatten_places_one_reducing_ioc_while_its_budget_lasts_and_stops_below_the_minimum() {
    let min = lots(2);
    // Flat: nothing to place, whatever the budget.
    assert_eq!(flatten::next(SignedLots(0), min, 0), Next::Flat);
    assert_eq!(flatten::next(SignedLots(0), min, 3), Next::Flat);
    // Long: a sell; short: a buy, each on the side that reduces the position.
    assert_eq!(flatten::next(SignedLots(2), min, 1), Next::Ioc(Side::Sell));
    assert_eq!(flatten::next(SignedLots(-5), min, 1), Next::Ioc(Side::Buy));
    // Below the minimum size, either way: no IOC is placed at it.
    let below = Next::Stop(Flattening::BelowMinimum);
    assert_eq!(flatten::next(SignedLots(1), min, 3), below);
    assert_eq!(flatten::next(SignedLots(-1), min, 3), below);
    // The budget spent: used up.
    let used_up = Next::Stop(Flattening::UsedUp);
    assert_eq!(flatten::next(SignedLots(2), min, 0), used_up);
    assert_eq!(flatten::next(SignedLots(i64::MIN), min, 0), used_up);
    // A position whose size no lot count holds is reduced by a buy while the budget lasts.
    assert_eq!(
        flatten::next(SignedLots(i64::MIN), min, 1),
        Next::Ioc(Side::Buy)
    );
}

#[test]
fn a_flatten_ioc_is_priced_within_its_cap_on_the_grid_or_capped() {
    let found = place_stages::spec_of(Listed::trading());
    let spec = &found.spec;
    // On the cent grid: a sell 10 bps below the 100.00 bid, 99.90, still crossing it; a buy 10
    // bps above the 100.10 ask, 100.20 (100.2001 floored), still crossing it.
    let sell = flatten::ioc_price(spec, Side::Sell, QUOTE, 10).unwrap();
    assert_eq!(sell, Ticks(9_990));
    let buy = flatten::ioc_price(spec, Side::Buy, QUOTE, 10).unwrap();
    assert_eq!(buy, Ticks(10_020));
    // A bid of zero: no positive price within the cap below it, so the sell is capped; the buy
    // crosses the ask as before.
    let zero = Quote {
        bid: Ticks(0),
        ask: Ticks(10_010),
    };
    let capped = Err(Flattening::Capped);
    assert_eq!(flatten::ioc_price(spec, Side::Sell, zero, 10), capped);
    assert_eq!(
        flatten::ioc_price(spec, Side::Buy, zero, 10),
        Ok(Ticks(10_020))
    );
    // A grid coarser than the touch near it: two significant figures, so prices step by 10.00
    // above 100. The cap cannot hold there: a sell rounded up onto the grid (as quantize rounds a
    // sell) lands above the 100.50 bid it must cross, and a buy rounded down lands below the
    // 100.50 ask, so neither is sent.
    let coarse = fbc_core::InstrumentSpec {
        price_grid: PriceGrid::sig_figs(2, 2, false).unwrap(),
        ..spec.clone()
    };
    let off = Quote {
        bid: Ticks(10_050),
        ask: Ticks(10_050),
    };
    assert_eq!(flatten::ioc_price(&coarse, Side::Sell, off, 10), capped);
    assert_eq!(flatten::ioc_price(&coarse, Side::Buy, off, 10), capped);
    // There, the touch on the grid: the IOC at the touch, 0 bps through it.
    let on = Quote {
        bid: Ticks(10_000),
        ask: Ticks(11_000),
    };
    assert_eq!(
        flatten::ioc_price(&coarse, Side::Sell, on, 10),
        Ok(Ticks(10_000))
    );
    assert_eq!(
        flatten::ioc_price(&coarse, Side::Buy, on, 10),
        Ok(Ticks(11_000))
    );
}

#[test]
fn a_flatten_ioc_comes_back_as_its_state_fills_and_echoes_show() {
    let qty = lots(2);
    let ended = Some(OrdState::Terminal(TerminalKind::Filled));
    let open = Some(OrdState::Open);
    let of = |state, fills: i64, venue: i64| {
        Ioc::of(state, (lots(fills), lots(venue), qty), false, false)
    };
    assert_eq!(of(ended, 2, 2), Ioc::Filled);
    assert_eq!(of(ended, 1, 1), Ioc::PartEnded);
    assert_eq!(of(ended, 0, 0), Ioc::Expired);
    assert_eq!(of(ended, 1, 2), Ioc::Unsettled);
    assert_eq!(of(open, 1, 1), Ioc::PartOpen);
    // Acknowledged and left open with no fill: QFEX answered, so it is not Unknown.
    assert_eq!(of(open, 0, 0), Ioc::Open);
    assert_eq!(of(Some(OrdState::Unknown), 0, 0), Ioc::Unknown);
    assert_eq!(of(Some(OrdState::PendingNew), 0, 0), Ioc::Unknown);
    assert_eq!(of(None, 0, 0), Ioc::Unknown);
    // An err echoing its add_order, and one the session did not send, whatever its state.
    let counts = (lots(0), lots(0), qty);
    assert_eq!(
        Ioc::of(Some(OrdState::PendingNew), counts, true, false),
        Ioc::Refused
    );
    assert_eq!(Ioc::of(ended, counts, false, true), Ioc::NotSent);
    // Its wait ends once it ended with every fill its events state.
    assert!(Ioc::settled(ended, lots(2), lots(2)));
    assert!(!Ioc::settled(ended, lots(1), lots(2)));
    assert!(!Ioc::settled(open, lots(1), lots(1)));
    assert!(!Ioc::settled(None, lots(0), lots(0)));
    // Each says what it came back as, each its own words.
    let said: Vec<&str> = [
        Ioc::Filled,
        Ioc::PartEnded,
        Ioc::PartOpen,
        Ioc::Open,
        Ioc::Expired,
        Ioc::Unsettled,
        Ioc::Unknown,
        Ioc::Refused,
        Ioc::NotSent,
    ]
    .map(Ioc::says)
    .to_vec();
    let mut unique = said.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), said.len(), "{said:?}");
}

#[test]
fn the_flattens_own_stop_replaces_the_fill_it_answered_and_no_other_first_stop() {
    let capped = Stop::Flatten(Flattening::Capped);
    let fill = Some(Stop::UnexpectedFill);
    assert_eq!(flatten::session_stop(fill, Some(capped)), Some(capped));
    assert_eq!(flatten::session_stop(None, Some(capped)), Some(capped));
    assert_eq!(flatten::session_stop(fill, None), fill);
    assert_eq!(flatten::session_stop(None, None), None);
    // A stop raised before the fill stays the session's, the first raised.
    let first = Some(Stop::NotOurCid);
    assert_eq!(flatten::session_stop(first, Some(capped)), first);
}

#[test]
fn each_flatten_stop_is_named_said_and_marked() {
    let stops = [
        (Flattening::BelowMinimum, "Flatten(BelowMinimum)"),
        (Flattening::Refused, "Flatten(Refused)"),
        (Flattening::Capped, "Flatten(Capped)"),
        (Flattening::BeyondCaps, "Flatten(BeyondCaps)"),
        (Flattening::UsedUp, "Flatten(UsedUp)"),
        (Flattening::Unsettled, "Flatten(Unsettled)"),
        (Flattening::NotEnded, "Flatten(NotEnded)"),
        (Flattening::NoTouch, "Flatten(NoTouch)"),
    ];
    for (why, name) in stops {
        let stop = Stop::Flatten(why);
        assert_eq!(stop.name(), name);
        assert_eq!(Stop::named(name), Some(stop));
        assert!(Stop::ALL.contains(&stop));
        assert!(
            stop.says().contains("the position the owner's"),
            "{}",
            stop.says()
        );
        // Each is marked, and cancels in a drop session only once a fill of ours came, which
        // every flatten answers (0151 items 2 and 6).
        assert!(stop.marks() && !stop.cancels_in_drop(false) && stop.cancels_in_drop(true));
    }
}

/// A flatten that stopped capped, with every part of its record set.
fn stopped_capped() -> FlattenRun {
    FlattenRun {
        after: flatten::AFTER_FILL,
        cancels: Some(places::Cancels {
            sent: 1,
            ended: true,
            ..places::Cancels::default()
        }),
        exit: Some(true),
        iocs: vec![
            IocRun {
                side: "sell",
                sent: true,
                came_back: Some(Ioc::Expired),
                refused: None,
            },
            IocRun {
                side: "sell",
                ..IocRun::default()
            },
        ],
        blocked: None,
        compared: Vec::new(),
        flat: false,
        rest_flat: false,
        stop: Some("Flatten(Capped)"),
        error: None,
        presses: 2,
    }
}

#[test]
fn a_flattens_report_and_evidence_say_what_it_did_and_no_value() {
    let run = stopped_capped();
    assert_eq!(run.sent(), 1);
    let mut s = String::new();
    flatten::markdown(&mut s, &run);
    for said in [
        "#### The flatten (FBC-ou9g; decision 0148 item 5, 0151 item 9)",
        "After an unexpected fill of ours (0151 item 9), while the session was open",
        flatten::FENCES,
        "- The cancels: 1 cancel by venue id; every order they named was seen to end",
        "- The owner's Flatten: pressed: the market in Exit",
        "- The flatten IOCs, 1 sent:\n  - IOC 1 (sell): expired unfilled\n  - IOC 2 (sell): not \
         sent",
        "- Kept back by another order of ours: no",
        "- The comparisons with never-arming reads: none made",
        "- How it ended: stopped not flat, Flatten(Capped): the position is the owner's",
        "- Presses of Ctrl-C while it ran, each recorded, none ending it: 2",
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    let json = flatten::to_json(&run);
    assert_eq!(json["stop"], "Flatten(Capped)");
    assert_eq!(json["iocs"][0]["came_back"], Ioc::Expired.says());
    assert_eq!(json["iocs"][1]["sent"], false);
    assert_eq!(
        (json["exit"].as_bool(), json["presses"].as_u64()),
        (Some(true), Some(2))
    );
    // The cap, the attempts, prices and sizes never appear: only fences met (0151 item 10).
    assert!(
        !s.contains("10 bps") && !s.contains("99.9") && !s.contains("0.01"),
        "{s}"
    );
    // The other ways it says how it ended.
    let ways = [
        (
            true,
            true,
            None,
            None,
            "flat: the registry's position flat, and REST positions flat",
        ),
        (
            true,
            false,
            None,
            None,
            "but no never-arming read agreed with it",
        ),
        (
            false,
            false,
            None,
            Some("synthetic reason"),
            "not flat: synthetic reason",
        ),
        (false, false, None, None, "not flat"),
    ];
    for (flat, rest_flat, stop, error, said) in ways {
        let run = FlattenRun {
            flat,
            rest_flat,
            stop,
            error: error.map(str::to_owned),
            ..FlattenRun::default()
        };
        assert!(run.ended().contains(said), "{}", run.ended());
    }
    // Its cancels held, refused or never made, the owner's Flatten refused or not pressed, an IOC
    // refused before it was sent, kept back, and a comparison of each end.
    let mut s = String::new();
    let compared = |ended| compare::Compared {
        step: "a flatten IOC",
        reads: Vec::new(),
        first_agreed: None,
        ended,
        stop: None,
    };
    let held = FlattenRun {
        cancels: Some(places::Cancels {
            held: true,
            ..places::Cancels::default()
        }),
        exit: Some(false),
        iocs: vec![
            IocRun {
                side: "buy",
                refused: Some("State".to_owned()),
                ..IocRun::default()
            },
            IocRun {
                side: "buy",
                sent: true,
                ..IocRun::default()
            },
        ],
        blocked: Some("synthetic reason".to_owned()),
        compared: [
            compare::Ended::Agreed,
            compare::Ended::Lagged,
            compare::Ended::Disagreed,
            compare::Ended::Unread,
            compare::Ended::Refused,
        ]
        .map(compared)
        .to_vec(),
        ..FlattenRun::default()
    };
    flatten::markdown(&mut s, &held);
    let in_flight = format!("  - IOC 2 (buy): {}", flatten::IN_FLIGHT);
    for said in [
        "- The cancels: 0 cancels by venue id; held or refused",
        "- The owner's Flatten: refused",
        "  - IOC 1 (buy): not sent: refused, State",
        in_flight.as_str(),
        "- Kept back by another order of ours: synthetic reason",
        "a flatten IOC: agreed; a flatten IOC: lagged; a flatten IOC: disagreed: a stop \
         (decision 0148 item 8); a flatten IOC: unread; a flatten IOC: refused: a provider rate \
         refusal, a stop",
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    let mut s = String::new();
    let none = FlattenRun {
        cancels: Some(places::Cancels::default()),
        ..FlattenRun::default()
    };
    flatten::markdown(&mut s, &none);
    assert!(
        s.contains("not every order they named was seen to end in time"),
        "{s}"
    );
    assert!(s.contains("- The owner's Flatten: not pressed"), "{s}");
    assert!(s.contains("  - none placed"), "{s}");
    let mut s = String::new();
    flatten::markdown(&mut s, &FlattenRun::default());
    assert!(s.contains("- The cancels: none made"), "{s}");
}

/// `fill`, but of the order whose venue id is `vid`, as QFEX names each order its own.
fn with_order(fill: fbc_core::FillEvent, vid: &str) -> fbc_core::FillEvent {
    let caps = probe_caps();
    let ident = fbc_core::dispatch(&caps, fbc_core::Namespace::new(1), |scope| {
        fbc_core::FillIdent::Venue {
            fill: scope.fill_id(T2).unwrap(),
            vid: Some(scope.venue_order_id(vid).unwrap()),
            cum_after: None,
        }
    });
    fbc_core::FillEvent { ident, ..fill }
}

#[test]
fn a_fill_the_session_expects_stops_nothing_and_any_other_fill_of_ours_stops_it() {
    use fbc_runtime::ExecHandler;
    let (mut handler, link) = place_stages::handled(100);
    let cids = crossing::minted(2);
    let (ioc, x) = (cids[0], cids[1]);
    for cid in [ioc, x] {
        let order = NewOrder {
            cid,
            inst: fbc_core::InstrumentId::new(1),
            side: Side::Buy,
            qty: lots(1),
            kind: OrderKind::Limit { px: Ticks(1) },
            tif: Tif::Ioc,
            channel: fbc_core::Channel::Public,
            post_only: false,
            reduce_only: false,
            reducing: true,
        };
        link.borrow_mut().reg().insert(order).unwrap();
    }
    handler.watched.borrow_mut().expected.push(ioc);
    // The flatten IOC's fill: counted, no stop, no fill of ours that cancels or flattens.
    handler.on_exec(place_stages::fill_env(
        1,
        qfex_fill(ioc, T1, false),
        Some(2_000),
    ));
    {
        let w = handler.watched.borrow();
        assert_eq!(
            (w.expected_fills, w.fill_of_ours, w.stop()),
            (1, false, None)
        );
    }
    // Any other order of ours filling: UnexpectedFill, as before (0151 item 9).
    let other = with_order(
        qfex_fill(x, T2, false),
        "5a5a5a5a-0000-4000-8000-0000000000a2",
    );
    handler.on_exec(place_stages::fill_env(2, other, Some(3_000)));
    let w = handler.watched.borrow();
    assert_eq!(w.stop(), Some(Stop::UnexpectedFill));
    assert!(w.fill_of_ours && w.expected_fills == 1);
}

#[test]
fn an_err_echoing_our_add_order_is_a_refusal_and_a_rate_refusal_is_not() {
    let cids: Vec<String> = crossing::minted(1)
        .into_iter()
        .map(crossing::text_of)
        .collect();
    let (mut tee, tap) = crossing::teed();
    let add = crossing::add_order(&cids[0], "SELL", "99.9");
    crossing::sent(&mut tee, &add);
    let from = tap.kept().heard.len();
    assert!(!tap.kept().err_echo_from(from));
    // A RateLimited err echoing it refuses the request, not the order.
    crossing::heard_err(&mut tee, "err", "RateLimited", &add, &cids[0]);
    assert!(!tap.kept().err_echo_from(from));
    crossing::heard_err(&mut tee, "err", "InvalidOrder", &add, &cids[0]);
    assert!(tap.kept().err_echo_from(from));
    // Only those heard from `from` on count.
    let later = tap.kept().heard.len();
    assert!(!tap.kept().err_echo_from(later));
}

/// A limit IOC `add_order` of `cid` on `side` at `price`, as the codec writes one.
fn ioc_order(cid: &str, side: &str, price: &str, quantity: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"{side}","#,
            r#""order_type":"LIMIT","order_time_in_force":"IOC","quantity":{quantity},"#,
            r#""price":{price},"client_order_id":"{cid}"}}}}"#,
        ),
        side = side,
        price = price,
        quantity = quantity,
        cid = cid
    )
}

/// The `n`th frame of `frames` as JSON, its one top-level member's name and body.
fn member(frames: &[Frame], n: usize) -> (String, Value) {
    let Frame::Text(text) = &frames[n] else {
        panic!("a binary frame")
    };
    let v: Value = serde_json::from_str(text).unwrap();
    let (name, body) = v.as_object().unwrap().iter().next().unwrap();
    (name.clone(), body.clone())
}

#[test]
fn the_models_limit_ioc_is_answered_as_its_knob_says() {
    let cid = "5a5a5a5a-0000-4000-8000-0000000000c1";
    // Full, the default: filled whole against the bid it crosses, at the bid, as a taker; its
    // position follows from the fill.
    let mut model = QfexModel::default();
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(ioc_order(cid, "SELL", "99.9", "0.01")));
    let frames = frames.unwrap();
    let (fill, body) = member(&frames, 0);
    assert_eq!(fill, "fill_response");
    assert_eq!(
        (
            body["execution_type"].as_str(),
            body["price"].to_string().as_str()
        ),
        (Some("TAKER"), "100")
    );
    let (order, body) = member(&frames, 1);
    assert_eq!(
        (order.as_str(), body["status"].as_str()),
        ("order_response", Some("FILLED"))
    );
    assert_eq!(body["client_order_id"], cid);
    assert_eq!(
        model.current(&None),
        r#"{"balance":{},"positions":[{"open_orders":0,"position":-0.01,"symbol":"SYN-USD"}]}"#
    );
    // One that crosses no touch expires, as Expire answers every one.
    let frames = model.trade(0, &Frame::text(ioc_order(cid, "SELL", "100.05", "0.01")));
    let (_, body) = member(&frames.unwrap(), 0);
    assert_eq!(body["status"], "IOC_CANCELLED");
    let mut model = QfexModel {
        ioc: IocAnswer::Expire,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(ioc_order(cid, "BUY", "100.2", "0.01")));
    let frames = frames.unwrap();
    assert_eq!(frames.len(), 1);
    let (_, body) = member(&frames, 0);
    assert_eq!(
        (
            body["status"].as_str(),
            body["quantity_remaining"].to_string().as_str()
        ),
        (Some("IOC_CANCELLED"), "0.01")
    );
    assert!(model.fills.is_empty());
    // Partial: one lot filled, the rest left as quantity_remaining on IOC_PARTIALLY_FILLED.
    let mut model = QfexModel {
        ioc: IocAnswer::Partial,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(ioc_order(cid, "BUY", "100.2", "0.02")));
    let frames = frames.unwrap();
    let (_, fill) = member(&frames, 0);
    assert_eq!(fill["quantity"].to_string(), "0.01");
    let (_, order) = member(&frames, 1);
    assert_eq!(order["status"], "IOC_PARTIALLY_FILLED");
    assert_eq!(order["quantity_remaining"].to_string(), "0.01");
    // Refuse: an err echoing it; Silent: nothing; neither makes an order.
    for (ioc, answered) in [(IocAnswer::Refuse, 1), (IocAnswer::Silent, 0)] {
        let mut model = QfexModel {
            ioc,
            ..QfexModel::default()
        };
        crossing::session(&mut model, 0);
        let text = ioc_order(cid, "BUY", "100.2", "0.01");
        let frames = model.trade(0, &Frame::text(&text)).unwrap();
        assert_eq!(frames.len(), answered);
        assert!(model.orders.is_empty() && model.fills.is_empty());
        if answered == 1 {
            let Frame::Text(err) = &frames[0] else {
                panic!("a text frame")
            };
            let v: Value = serde_json::from_str(err).unwrap();
            assert_eq!(v["error_code"], "InvalidOrder");
            assert_eq!(v["incoming_message"]["params"]["client_order_id"], cid);
        }
        assert_eq!(model.log.last().unwrap().what, format!("ioc {ioc:?}"));
    }
}

#[test]
fn the_models_position_frame_comes_before_or_after_a_fills_frames() {
    let cid = "5a5a5a5a-0000-4000-8000-0000000000c2";
    for (push, at) in [(PositionPush::Before, 0), (PositionPush::After, 2)] {
        let mut model = QfexModel {
            position_push: Some(push),
            ..QfexModel::default()
        };
        crossing::session(&mut model, 0);
        let frames = model.trade(0, &Frame::text(ioc_order(cid, "SELL", "99.9", "0.01")));
        let frames = frames.unwrap();
        assert_eq!(frames.len(), 3);
        let (name, body) = member(&frames, at);
        assert_eq!(name, "position_response");
        assert_eq!(body["position"].to_string(), "-0.01");
        assert_eq!(
            Decimal::from_str(&body["position"].to_string()).unwrap(),
            Decimal::new(-1, 2)
        );
    }
    // None by default: the fill's frames alone.
    let mut model = QfexModel::default();
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(ioc_order(cid, "SELL", "99.9", "0.01")));
    assert_eq!(frames.unwrap().len(), 2);
}

/// A post-only GTC `add_order` of `cid`, as the codec writes X.
fn resting_order(cid: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"BUY","#,
            r#""order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"#,
            r#""client_order_id":"{cid}"}}}}"#,
        ),
        cid = cid
    )
}

#[test]
fn the_models_maker_fill_the_reads_lag_holds_the_read_pages_until_its_connection_writes() {
    // FBC-23ch's third review fix carried to the flatten: X's maker fill pushed after its ACK in
    // the same answer, while REST lags it (rest_lag), leaves a comparison free to agree before the
    // session read the fill, so a read socket's page waits from that fill until the placing
    // connection's client writes again (the flatten's first frame) or its end is seen; the
    // session's own pages are answered at once. With no lag the reads show the fill at once, and
    // nothing is held.
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let mut model = QfexModel {
        maker_fill: true,
        rest_lag: 3,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    let answer = model
        .trade(0, &Frame::text(resting_order("cid-x")))
        .unwrap();
    assert_eq!(answer.len(), 3, "the ACK, the fill and FILLED");
    assert!(model.knob_frames_unread());
    assert!(model.trade(1, &Frame::text(page)).unwrap().is_empty());
    assert_eq!(model.held_pages.len(), 1);
    assert_eq!(model.trade(0, &Frame::text(page)).unwrap().len(), 1);
    // The placing connection's end seen, a page is answered at once again.
    model.conns.get_mut(&0).unwrap().settled = true;
    assert!(!model.knob_frames_unread());
    assert_eq!(model.trade(1, &Frame::text(page)).unwrap().len(), 1);
    let mut model = QfexModel {
        maker_fill: true,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    model
        .trade(0, &Frame::text(resting_order("cid-x")))
        .unwrap();
    assert!(!model.knob_frames_unread());
    assert_eq!(model.trade(1, &Frame::text(page)).unwrap().len(), 1);
}

#[test]
fn the_models_refusals_after_the_first_limit_ioc_wait_for_one() {
    // ReadSocketAfterIoc and ReadAuthEchoAfterIoc refuse nothing before the run's first limit IOC,
    // however many pages or authentications come after X, then refuse the first one after it,
    // once.
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let auth = r#"{"type":"auth","params":{}}"#;
    let refused = |frames: &[Frame]| {
        let Frame::Text(text) = &frames[0] else {
            panic!("a text frame")
        };
        text.contains("RateLimited")
    };
    for limited in [
        model::Limited::ReadSocketAfterIoc,
        model::Limited::ReadAuthEchoAfterIoc,
    ] {
        let mut model = QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        };
        crossing::session(&mut model, 0);
        model
            .trade(0, &Frame::text(resting_order("cid-x")))
            .unwrap();
        let ask = |model: &mut QfexModel, conn: usize| {
            let text = match limited {
                model::Limited::ReadSocketAfterIoc => page,
                _ => auth,
            };
            model.trade(conn, &Frame::text(text)).unwrap()
        };
        for conn in 1..4 {
            assert!(!refused(&ask(&mut model, conn)), "{limited:?}");
        }
        let ioc = ioc_order("cid-f", "SELL", "99.9", "0.01");
        model.trade(0, &Frame::text(ioc)).unwrap();
        assert!(refused(&ask(&mut model, 4)), "{limited:?}");
        assert!(!refused(&ask(&mut model, 5)), "{limited:?}");
    }
}

#[tokio::test]
async fn the_models_late_order_response_and_resync_fills_come_as_their_knobs_say() {
    // ioc_order_after: a limit IOC filled whole is answered its fill_response alone, its FILLED
    // order_response pushed later.
    let cid = "5a5a5a5a-0000-4000-8000-0000000000c3";
    let venue = model::Venue::start(QfexModel {
        ioc_order_after: Some(std::time::Duration::from_secs(1)),
        ..QfexModel::default()
    })
    .await;
    let frames = {
        let mut model = venue.model.lock().unwrap();
        crossing::session(&mut model, 0);
        model.trade(0, &Frame::text(ioc_order(cid, "SELL", "99.9", "0.01")))
    };
    let frames = frames.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(member(&frames, 0).0, "fill_response");
    // fill_in_resync: the arm's fills go before that connection's next get_user_orders page,
    // and not with the arm's ack.
    let arm = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let mut model = QfexModel {
        fill_in_resync: Some(1),
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    model.trade(0, &Frame::text(arm)).unwrap();
    model
        .trade(0, &Frame::text(resting_order("cid-x")))
        .unwrap();
    crossing::session(&mut model, 1);
    let armed = model.trade(1, &Frame::text(arm)).unwrap();
    assert_eq!(armed.len(), 1, "the ack alone");
    // Another connection's page carries none of them.
    assert_eq!(model.trade(2, &Frame::text(page)).unwrap().len(), 1);
    let paged = model.trade(1, &Frame::text(page)).unwrap();
    let names: Vec<String> = (0..paged.len()).map(|n| member(&paged, n).0).collect();
    assert_eq!(
        names,
        ["fill_response", "order_response", "all_orders_response"]
    );
    assert_eq!(model.trade(1, &Frame::text(page)).unwrap().len(), 1);
}

#[test]
fn the_models_held_resync_page_waits_for_the_tests_release() {
    // resync_page_held: the arm's fills are sent with that connection's next get_user_orders
    // page asked for, and the page itself is held for the test's release, not sent.
    let arm = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let mut model = QfexModel {
        fill_in_resync: Some(1),
        resync_page_held: true,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    model.trade(0, &Frame::text(arm)).unwrap();
    model
        .trade(0, &Frame::text(resting_order("cid-x")))
        .unwrap();
    crossing::session(&mut model, 1);
    assert_eq!(model.trade(1, &Frame::text(arm)).unwrap().len(), 1);
    let paged = model.trade(1, &Frame::text(page)).unwrap();
    let names: Vec<String> = (0..paged.len()).map(|n| member(&paged, n).0).collect();
    assert_eq!(names, ["fill_response", "order_response"]);
    let held: Vec<(usize, String)> = (model.held.iter())
        .map(|(conn, frame)| (*conn, member(std::slice::from_ref(frame), 0).0))
        .collect();
    assert_eq!(held, [(1, "all_orders_response".to_owned())]);
    // Only that page is held: the next is answered at once.
    assert_eq!(model.trade(1, &Frame::text(page)).unwrap().len(), 1);
}
