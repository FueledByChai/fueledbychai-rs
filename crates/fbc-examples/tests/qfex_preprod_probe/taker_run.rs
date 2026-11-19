//! FBC-ou9g's function-level proof of stage two's taker IOC (decision 0132 item 7, 0148 items 5
//! and 10, 0151 items 8, 9 and 10): what follows the IOC, the taker fee's sign, the order of its
//! frames, what its record and the run's declarations say and never say, the hand actions the
//! run leaves, and the QFEX model's fee_sign and ioc_frame_order knobs. Declared by path in
//! `tests/qfex_preprod_probe.rs`. The runs that reach each of these are in `place_stages.rs`;
//! every value here is made up.

use super::*;

use capture::{AccountRead, Evidence};
use fbc_core::{AssetSym, MonoNs, SignedLots};
use flatten::Ioc;
use model::{IocFrameOrder, QfexModel};
use places::MarkerRun;
use stage_two::{CrossRun, StageTwo};
use taker::{After, FeeSign, FrameOrder, Taken};
use tee::{Cid, Heard};

/// A fee of `nanos` as the QFEX adapter's scope decodes it, a cost positive when we paid it.
fn fee(nanos: i128) -> fbc_core::Fee {
    let caps = probe_caps();
    fbc_core::dispatch(&caps, fbc_core::Namespace::new(1), |scope| {
        scope.fee(nanos, AssetSym::new("USD").unwrap()).unwrap()
    })
}

#[test]
fn the_fee_sign_is_read_from_the_decoded_fee_and_never_its_amount() {
    assert_eq!(FeeSign::of(&fee(1)), FeeSign::Paid);
    assert_eq!(FeeSign::of(&fee(1_000_000_000_000)), FeeSign::Paid);
    assert_eq!(FeeSign::of(&fee(0)), FeeSign::Zero);
    assert_eq!(FeeSign::of(&fee(-1)), FeeSign::Received);
    assert_eq!(FeeSign::of(&fee(-7_000_000)), FeeSign::Received);
    let words: Vec<&str> = [FeeSign::Paid, FeeSign::Zero, FeeSign::Received]
        .map(FeeSign::word)
        .to_vec();
    assert_eq!(words, ["paid", "zero", "received"]);
    // Said once each, in the order they came, or not measured with none.
    assert_eq!(
        taker::fee_said(&[]),
        "not measured: the taker IOC brought no fill"
    );
    assert_eq!(taker::fee_said(&[FeeSign::Paid, FeeSign::Paid]), "paid");
    assert_eq!(
        taker::fee_said(&[FeeSign::Received, FeeSign::Paid, FeeSign::Received]),
        "received and paid"
    );
}

/// An `order_response` heard naming the `n`th `add_order` the run sent, as `cid` says.
fn order(cid: Cid) -> Heard {
    Heard::Order {
        at: MonoNs(1),
        status: "FILLED".to_owned(),
        cid,
        ours: true,
    }
}

/// An `order_response` `ACK` heard naming the `n`th `add_order` the run sent, as `cid` says.
fn acked(cid: Cid) -> Heard {
    Heard::Order {
        at: MonoNs(1),
        status: "ACK".to_owned(),
        cid,
        ours: true,
    }
}

/// A `fill_response` heard naming the `n`th `add_order` the run sent, as `cid` says.
fn fill(cid: Cid) -> Heard {
    Heard::Fill {
        at: MonoNs(1),
        cid,
        ours: true,
        trade_id: true,
        fee: None,
    }
}

#[test]
fn the_frame_order_is_the_last_order_response_against_the_last_fill_response() {
    let (us, other) = (Cid::Exact(3), Cid::Exact(2));
    // Every fill_response before the final order_response.
    let heard = [order(us), fill(us), fill(us), order(us)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::FillFirst);
    // A fill_response after the final order_response, the order FBC-tfxk asks about; written
    // in another case, it still names the order.
    let heard = [order(us), fill(Cid::Differs(3))];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::OrderFirst);
    // Frames naming another order do not count.
    let heard = [fill(us), order(us), fill(other), order(other)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::FillFirst);
    // No fill_response naming it, or no order_response.
    assert_eq!(FrameOrder::of(&[order(us)], 3), FrameOrder::NoFill);
    assert_eq!(FrameOrder::of(&[], 3), FrameOrder::NoFill);
    assert_eq!(FrameOrder::of(&[fill(us)], 3), FrameOrder::NoOrder);
    // A frame with no client id of ours names no order.
    let heard = [fill(Cid::Absent), order(Cid::Foreign), order(us)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::NoFill);
    for order in [
        FrameOrder::FillFirst,
        FrameOrder::OrderFirst,
        FrameOrder::NoFill,
        FrameOrder::NoOrder,
    ] {
        assert!(!order.says().chars().any(|c| c.is_ascii_digit()));
    }
    assert!(FrameOrder::OrderFirst.says().contains("FBC-tfxk"));
}

#[test]
fn an_ack_is_no_final_order_response_so_an_ack_then_a_fill_is_not_measured() {
    // PR #277's review (Codex, P2): the taker IOC's ACK, then its fill_response, and no final
    // order_response before its deadline. The ACK is no final order_response, so the order
    // FBC-tfxk asks about was never observed: not measured, never OrderFirst.
    let us = Cid::Exact(3);
    assert_eq!(
        FrameOrder::of(&[acked(us), fill(us)], 3),
        FrameOrder::NoOrder
    );
    assert_eq!(
        FrameOrder::of(&[fill(us), acked(us)], 3),
        FrameOrder::NoOrder
    );
    // With its final order_response heard, an ACK anywhere changes nothing.
    let heard = [acked(us), fill(us), order(us)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::FillFirst);
    let heard = [acked(us), order(us), fill(us)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::OrderFirst);
    let heard = [order(us), fill(us), acked(us)];
    assert_eq!(FrameOrder::of(&heard, 3), FrameOrder::OrderFirst);
    // An ACK with no fill is still no fill.
    assert_eq!(FrameOrder::of(&[acked(us)], 3), FrameOrder::NoFill);
    // The report says it as not measured, naming the final order_response it lacks.
    let said = FrameOrder::NoOrder.says();
    assert!(said.starts_with("not measured: "), "{said}");
    assert!(said.contains("no final order_response"), "{said}");
    assert!(said.contains("ACK"), "{said}");
}

#[test]
fn what_follows_the_taker_ioc_is_the_flatten_a_stop_or_nothing() {
    let (flat, long, short) = (SignedLots(0), SignedLots(1), SignedLots(-1));
    // Left open with a fill: the run stops not flat, with no flatten (0151 item 8).
    assert_eq!(
        taker::after_ioc(Ioc::PartOpen, long, false),
        After::Remainder
    );
    assert_eq!(
        taker::after_ioc(Ioc::PartOpen, flat, false),
        After::Remainder
    );
    assert_eq!(
        taker::after_ioc(Ioc::PartOpen, flat, true),
        After::Remainder
    );
    // Ended with fills the venue's count states that had not all reached the registry: the run
    // stops not flat, with no flatten, whatever the registry's position, a flat one included;
    // and so does any end with the venue's count above the registry's.
    for position in [flat, long, short] {
        let unsettled = taker::after_ioc(Ioc::Unsettled, position, true);
        assert_eq!(unsettled, After::Unsettled, "{position:?}");
        for ioc in [Ioc::Unknown, Ioc::Refused] {
            assert_eq!(taker::after_ioc(ioc, position, true), After::Unsettled);
        }
    }
    assert_eq!(After::Unsettled.stop(), Some(places::Stop::IocUnsettled));
    assert_eq!(After::Remainder.stop(), Some(places::Stop::IocRemainder));
    assert_eq!((After::Flatten.stop(), After::Nothing.stop()), (None, None));
    // Any other end that leaves the position off flat is flattened, either side.
    for ioc in [Ioc::Filled, Ioc::PartEnded, Ioc::Unknown] {
        assert_eq!(
            taker::after_ioc(ioc, long, false),
            After::Flatten,
            "{ioc:?}"
        );
        assert_eq!(
            taker::after_ioc(ioc, short, false),
            After::Flatten,
            "{ioc:?}"
        );
    }
    // Flat, nothing: an expired, refused or unsent IOC among them.
    for ioc in [Ioc::Expired, Ioc::Refused, Ioc::NotSent, Ioc::Filled] {
        assert_eq!(
            taker::after_ioc(ioc, flat, false),
            After::Nothing,
            "{ioc:?}"
        );
    }
}

#[test]
fn the_taker_ioc_is_said_with_no_id_or_value() {
    assert_eq!(taker::came_back(None), "not measured: it was not placed");
    let unplaced = Taken::default();
    assert_eq!(
        taker::came_back(Some(&unplaced)),
        "not measured: it was not placed"
    );
    let taken = |ioc| Taken {
        ioc: Some(ioc),
        ..Taken::default()
    };
    assert_eq!(
        taker::came_back(Some(&taken(Ioc::PartOpen))),
        taker::REMAINDER
    );
    assert_eq!(
        taker::came_back(Some(&taken(Ioc::Expired))),
        "expired unfilled: recorded, and nothing flattened"
    );
    assert_eq!(
        taker::came_back(Some(&taken(Ioc::Unsettled))),
        taker::UNSETTLED
    );
    assert_eq!(
        taker::came_back(Some(&taken(Ioc::Filled))),
        Ioc::Filled.says()
    );
    let filled = Taken {
        ioc: Some(Ioc::Filled),
        fees: vec![FeeSign::Received],
        frame_order: Some(FrameOrder::OrderFirst),
    };
    let lines = taker::session_lines(Some(&filled));
    for said in [
        "- How the taker IOC came back: filled whole, its fills settled\n",
        "- The taker fee's sign (decision 0148 item 10; no amount): received\n",
        &format!(
            "- The order of its final order_response and its fill_response (FBC-tfxk): {}\n",
            FrameOrder::OrderFirst.says()
        ),
    ] {
        assert!(lines.contains(said), "{said}: {lines}");
    }
    let none = taker::session_lines(None);
    assert!(
        none.contains("; no amount): not measured: it was not placed\n"),
        "{none}"
    );
    assert!(none.contains("(FBC-tfxk): not measured\n"), "{none}");
    let json = taker::taken_json(Some(&filled));
    assert_eq!(json["fee_sign"], "received");
    assert_eq!(json["fee_signs"], serde_json::json!(["received"]));
    assert_eq!(json["frame_order"], FrameOrder::OrderFirst.says());
    let json = taker::taken_json(None);
    assert_eq!(json["fee_sign"], Value::Null);
    assert_eq!(json["came_back"], "not measured: it was not placed");
}

/// A read of `label` after a session, flat or not, listing `orders` orders of ours.
fn read(label: &str, flat: bool, orders: usize) -> AccountRead {
    AccountRead {
        before: refusals::AFTER_TAKE,
        label: label.to_owned(),
        flat,
        page_orders: orders,
        ..AccountRead::default()
    }
}

#[test]
fn the_hand_actions_follow_each_named_accounts_last_read() {
    let named = ["primary", "subaccount"];
    assert_eq!(taker::hand_actions(&[], &named), (false, false));
    // The last read of each judges: an earlier one off flat or listing an order does not.
    let reads = [
        read("primary (take)", false, 1),
        read("subaccount", true, 0),
        read("primary", true, 0),
    ];
    assert_eq!(taker::hand_actions(&reads, &named), (false, false));
    let reads = [
        read("primary (take)", true, 1),
        read("subaccount", false, 0),
    ];
    assert_eq!(taker::hand_actions(&reads, &named), (true, true));
    // REST's open_orders alone asks for a hand cancel too; an account not named is not read.
    let rest = AccountRead {
        rest_open_orders: 1,
        ..read("subaccount", true, 0)
    };
    assert_eq!(taker::hand_actions(&[rest], &named), (true, false));
    let other = read("primaryx", false, 3);
    assert_eq!(taker::hand_actions(&[other], &named), (false, false));
}

/// A taker IOC session on `account` whose IOC came back filled with `fees`.
fn take(account: &'static str, fees: Vec<FeeSign>) -> CrossRun {
    CrossRun {
        account,
        taken: Some(Taken {
            ioc: Some(Ioc::Filled),
            fees,
            frame_order: Some(FrameOrder::FillFirst),
        }),
        ..CrossRun::default()
    }
}

#[test]
fn the_declarations_section_says_each_item_and_no_value() {
    // A run that built no registry, read nothing and took no IOC: each item still said.
    let ev = Evidence {
        places_asked: true,
        accounts: vec!["primary", "subaccount"],
        ..Evidence::default()
    };
    let mut s = String::new();
    taker::markdown(&mut s, &ev);
    for said in [
        "## What the run declares, and the owner's part (decision 0148 item 10, 0151 item 10)",
        &format!("- The key: {}", taker::key()),
        "(decision 0148 item 3): none: no place-stage registry was built",
        "- The taker fee's sign, by account (no amount): not measured: no taker IOC session ran",
        "- The hand actions declared: no `--reconciled` declaration was given",
        // Nothing was read, so what the run leaves the owner by hand is not known (FBC-77r8's
        // review, carried).
        &format!(
            "- The hand actions this run leaves the owner: {}",
            taker::HAND_NOT_KNOWN
        ),
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    assert!(taker::key().contains("`qfex.exec.production_places = probe_place_stages`"));
    // Taker sessions, one whose IOC was not placed; --reconciled; reads that leave both hands.
    let ev = Evidence {
        stage_two: Some(StageTwo {
            takes: vec![
                take("primary", vec![FeeSign::Zero]),
                CrossRun {
                    account: "subaccount",
                    ..CrossRun::default()
                },
            ],
            ..StageTwo::default()
        }),
        marker: Some(MarkerRun {
            reconciled: Some("both"),
            ..MarkerRun::default()
        }),
        reads: vec![read("primary", false, 0), read("subaccount", true, 2)],
        ..ev
    };
    let mut s = String::new();
    taker::markdown(&mut s, &ev);
    for said in [
        "(no amount): primary: zero; subaccount: not measured: the taker IOC was not placed",
        "- The hand actions declared: `--reconciled both`, the owner's declaration",
        "a hand cancel on QFEX production's site, the last read of a named account listing an \
         order; and a hand close of the position, the last read of a named account showing it \
         off flat (decision 0148 item 9); the next place-stage run then needs --reconciled",
        &format!("- Not checked by the probe: {}", taker::UNCHECKED),
        &format!("- The fences met, never their values: {}", taker::FENCES),
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    let json = taker::to_json(&ev);
    assert_eq!(
        json["taker_fee_signs"],
        serde_json::json!([
            "primary: zero",
            "subaccount: not measured: the taker IOC was not placed"
        ])
    );
    assert_eq!(
        (
            &json["hand_actions_left"]["hand_cancel"],
            &json["hand_actions_left"]["hand_close"]
        ),
        (&Value::Bool(true), &Value::Bool(true))
    );
    assert_eq!(json["sole_trader_checked"], false);
    assert_eq!(json["declarations"], serde_json::json!([]));
    // A hand cancel alone, then a hand close alone.
    for (reads, cancel, close) in [
        (vec![read("primary", true, 1)], true, false),
        (vec![read("subaccount", false, 0)], false, true),
    ] {
        let ev = Evidence {
            reads,
            ..ev.clone()
        };
        let mut s = String::new();
        taker::markdown(&mut s, &ev);
        assert_eq!(s.contains("a hand cancel on QFEX"), cancel, "{s}");
        assert_eq!(s.contains("a hand close of the position"), close, "{s}");
    }
}

/// A limit IOC `add_order` of `cid`, a buy at `price` of `quantity`, as the codec writes one.
fn buy_ioc(cid: &str, price: &str, quantity: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"BUY","#,
            r#""order_type":"LIMIT","order_time_in_force":"IOC","quantity":{quantity},"#,
            r#""price":{price},"client_order_id":"{cid}"}}}}"#,
        ),
        price = price,
        quantity = quantity,
        cid = cid
    )
}

/// The top-level member names of `frames`, in order.
fn members(frames: &[Frame]) -> Vec<String> {
    (frames.iter())
        .map(|f| {
            let Frame::Text(text) = f else {
                panic!("a binary frame")
            };
            let v: Value = serde_json::from_str(text).unwrap();
            v.as_object().unwrap().keys().next().unwrap().clone()
        })
        .collect()
}

#[test]
fn the_models_fee_sign_and_frame_order_knobs_shape_a_filled_iocs_frames() {
    let cid = "5a5a5a5a-0000-4000-8000-0000000000d1";
    for (sign, fee) in [
        (model::FeeSign::Paid, "0.001"),
        (model::FeeSign::Zero, "0.0"),
        (model::FeeSign::Received, "-0.001"),
    ] {
        for (order, first) in [
            (
                IocFrameOrder::FillFirst,
                ["fill_response", "order_response"],
            ),
            (
                IocFrameOrder::OrderFirst,
                ["order_response", "fill_response"],
            ),
        ] {
            let mut model = QfexModel {
                fee_sign: sign,
                ioc_frame_order: order,
                ..QfexModel::default()
            };
            crossing::session(&mut model, 0);
            let frames = model.trade(0, &Frame::text(buy_ioc(cid, "100.2", "0.01")));
            let frames = frames.unwrap();
            assert_eq!(members(&frames), first, "{sign:?} {order:?}");
            let at = first.iter().position(|m| *m == "fill_response").unwrap();
            let Frame::Text(text) = &frames[at] else {
                panic!("a text frame")
            };
            let v: Value = serde_json::from_str(text).unwrap();
            assert_eq!(v["fill_response"]["fee"].to_string(), fee);
            assert_eq!(v["fill_response"]["execution_type"], "TAKER");
        }
    }
    // Withheld: the final order_response alone, though the model made the fill, which its
    // trades and positions show.
    let mut model = QfexModel {
        ioc_frame_order: IocFrameOrder::Withheld,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(buy_ioc(cid, "100.2", "0.01")));
    assert_eq!(members(&frames.unwrap()), ["order_response"]);
    assert_eq!(model.fills.len(), 1);
    // ioc_acked: its ACK first, then its fill's frames as the frame order says.
    let mut model = QfexModel {
        ioc_acked: true,
        ..QfexModel::default()
    };
    crossing::session(&mut model, 0);
    let frames = model.trade(0, &Frame::text(buy_ioc(cid, "100.2", "0.01")));
    let frames = frames.unwrap();
    let statuses: Vec<Value> = (frames.iter())
        .map(|f| {
            let Frame::Text(text) = f else {
                panic!("a text frame")
            };
            let v: Value = serde_json::from_str(text).unwrap();
            v["order_response"]["status"].clone()
        })
        .collect();
    assert_eq!(
        members(&frames),
        ["order_response", "fill_response", "order_response"]
    );
    assert_eq!(statuses[0], "ACK");
    assert_eq!(statuses[2], "FILLED");
    // The defaults: a fee paid, the fill_response first.
    let model = QfexModel::default();
    assert_eq!(
        (model.fee_sign, model.ioc_frame_order, model.ioc_acked),
        (model::FeeSign::Paid, IocFrameOrder::FillFirst, false)
    );
}

#[test]
fn stage_twos_report_gives_each_taker_ioc_session_after_its_crossing_session() {
    let cross = |account| CrossRun {
        account,
        ..CrossRun::default()
    };
    let two = StageTwo {
        gate: Some(true),
        crosses: vec![cross("primary"), cross("subaccount")],
        takes: vec![
            // Its span bound met, not read after it, followed by the next account's crossing.
            CrossRun {
                cancels_lead: Some(std::time::Duration::from_millis(300)),
                ..take("primary", vec![FeeSign::Paid])
            },
            // Read after it, with nothing resting, and the run's last.
            CrossRun {
                read: true,
                ..take("subaccount", vec![FeeSign::Received])
            },
        ],
        finished: true,
        ..StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &two, &["primary", "subaccount"]);
    // Each account's crossing session, then its taker IOC session, before the next account's.
    let at = |said: &str| {
        report
            .find(said)
            .unwrap_or_else(|| panic!("{said}: {report}"))
    };
    assert!(at("### The crossing post-only on primary") < at("### The taker IOC on primary"));
    assert!(at("### The taker IOC on primary") < at("### The crossing post-only on subaccount"));
    assert!(at("### The crossing post-only on subaccount") < at("### The taker IOC on subaccount"));
    for said in [
        &format!("- What followed: {}", taker::THEN_TAKE),
        "; the span bound stopped it, its cancels begun 300 ms before the bound",
        "- An order resting on the account when the session ended: not known: the account was \
         not read after the session",
        "- An order resting on the account when the session ended: no\n",
        &format!("- What followed: {}", taker::THEN_PAGES),
        "; no amount): received\n",
    ] {
        assert!(report.contains(said), "{said}: {report}");
    }
    assert!(!report.contains(taker::RUN_ENDED), "{report}");
    // The run ended before the subaccount's crossing session: the primary's taker IOC session
    // is followed by the run's end, not by a session that never ran, and so is a crossing
    // session no taker IOC session followed (FBC-ou9g).
    let ended = StageTwo {
        crosses: vec![cross("primary")],
        takes: vec![take("primary", Vec::new())],
        ..StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &ended, &[]);
    let section = report.split("### The taker IOC on primary").nth(1).unwrap();
    assert!(
        section.contains(&format!("- What followed: {}", taker::RUN_ENDED)),
        "{section}"
    );
    assert!(!report.contains(taker::THEN_PAGES), "{report}");
    let alone = StageTwo {
        crosses: vec![cross("primary")],
        ..StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &alone, &[]);
    assert!(
        report.contains(&format!("- What followed: {}", taker::RUN_ENDED)),
        "{report}"
    );
    assert!(!report.contains(taker::THEN_TAKE), "{report}");
    let json = stage_two::to_json(&two);
    assert_eq!(json["takes"][0]["cancels_lead_ms"], 300);
    assert_eq!(json["takes"][1]["taken"]["fee_sign"], "received");
    assert_eq!(json["crossings"][1]["account"], "subaccount");
}

#[test]
fn the_reads_after_a_taker_ioc_session_say_their_stop_as_met_after_it() {
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::Rest,
    };
    let after = "after the taker IOC session";
    use stage_two::Session;
    for (session, said) in [(Session::Take, "taker IOC"), (Session::Cross, "crossing")] {
        let cases = [
            ((Some(rate), true), (true, true, true)),
            ((None, true), (true, true, true)),
            ((None, false), (true, true, true)),
            ((None, false), (false, true, true)),
            ((None, false), (false, false, true)),
        ];
        let stops = [
            rate,
            places::Stop::Interrupted,
            places::Stop::NotFlat,
            places::Stop::LeftResting,
            places::Stop::LeftResting,
        ];
        for ((first, rest), want) in cases.into_iter().zip(stops) {
            let (stop, when) = stage::stage_read_stop(session, first, rest).unwrap();
            assert_eq!(stop, want);
            assert!(when.contains(&format!("the {said} session")), "{when}");
            assert_eq!(session == Session::Take, when.contains(after), "{when}");
        }
        let none = stage::stage_read_stop(session, (None, false), (false, false, false));
        assert_eq!(none, None);
    }
}

#[test]
fn a_sessions_refusal_is_named_by_its_variant_alone() {
    // As `Link::submit` gives a session's refusal, its `Debug`: the request id it carries is left
    // out (0151 item 10).
    for (debug, name) in [
        ("Backpressure { rpc: RpcId(42) }", "Backpressure"),
        ("Ended", "Ended"),
        ("OtherAccount", "OtherAccount"),
    ] {
        assert_eq!(places::refusal_named(debug), name);
        assert!(!places::refusal_named(debug).contains("42"));
    }
}

// ----- FBC-77r8's review of PR #273, carried into the taker IOC (FBC-ou9g) ----------------------

#[test]
fn the_hand_actions_are_not_known_for_an_account_not_read_or_whose_last_read_failed() {
    let named = ["primary", "subaccount"];
    let both = [read("primary (take)", true, 0), read("subaccount", true, 0)];
    assert!(!taker::hand_unknown(&both, &named, &[]));
    // A named account with no read, or whose last read could not be made.
    assert!(taker::hand_unknown(&both[..1], &named, &[]));
    assert!(taker::hand_unknown(&both, &named, &["subaccount"]));
    // An account not named does not count.
    assert!(!taker::hand_unknown(&both, &named, &["primaryx"]));
    let ev = |reads: Vec<AccountRead>, last_unread: Vec<&'static str>| Evidence {
        places_asked: true,
        accounts: named.to_vec(),
        reads,
        last_unread,
        ..Evidence::default()
    };
    let left = |ev: &Evidence| {
        let mut s = String::new();
        taker::markdown(&mut s, ev);
        let line = "- The hand actions this run leaves the owner: ";
        let at = s.find(line).unwrap_or_else(|| panic!("{s}"));
        s[at + line.len()..].lines().next().unwrap().to_owned()
    };
    let clean = ev(both.to_vec(), Vec::new());
    assert!(left(&clean).starts_with("none: the last read of every named account"));
    assert_eq!(taker::to_json(&clean)["hand_actions_left"]["known"], true);
    // Every read made was clean, but the subaccount's last read failed: not known, never none.
    let failed = ev(both.to_vec(), vec!["subaccount"]);
    assert_eq!(left(&failed), taker::HAND_NOT_KNOWN);
    assert_eq!(taker::to_json(&failed)["hand_actions_left"]["known"], false);
    // A hand cancel shown, and the rest not known.
    let cancel = ev(
        vec![read("primary (take)", true, 1), read("subaccount", true, 0)],
        vec!["subaccount"],
    );
    let said = left(&cancel);
    assert!(
        said.starts_with("a hand cancel on QFEX production's site"),
        "{said}"
    );
    assert!(
        said.contains("; and the rest not known: a named account was not read"),
        "{said}"
    );
    assert!(
        said.ends_with("the next place-stage run then needs --reconciled"),
        "{said}"
    );
}

#[test]
fn the_reads_after_a_taker_ioc_session_order_an_order_not_ours_after_a_position_and_before_ours() {
    // stage::stage_after_stop under the taker IOC session: a provider rate refusal, the owner's
    // Ctrl-C, a position not flat, then an order or a TWAP not ours (ReadDisagrees), then an
    // order of ours resting or an account not read (LeftResting), each said as met after the
    // taker IOC session (FBC-77r8's review, carried).
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    let others = (places::Stop::ReadDisagrees, stage::TAKE_OTHERS);
    let after = "after the taker IOC session";
    let unread = "after the taker IOC session, a named account not read after it";
    for ((refused, pressed, not_flat, unexpected, resting, not_read), want) in [
        (
            (Some(rate), true, true, true, true, true),
            Some((rate, "in the reads after the taker IOC session")),
        ),
        (
            (None, true, true, true, true, true),
            Some((
                places::Stop::Interrupted,
                "during the reads after the taker IOC session",
            )),
        ),
        (
            (None, false, true, true, true, true),
            Some((places::Stop::NotFlat, after)),
        ),
        ((None, false, false, true, true, true), Some(others)),
        ((None, false, false, true, false, false), Some(others)),
        (
            (None, false, false, false, true, false),
            Some((places::Stop::LeftResting, after)),
        ),
        (
            (None, false, false, false, false, true),
            Some((places::Stop::LeftResting, unread)),
        ),
        ((None, false, false, false, false, false), None),
    ] {
        let got = stage::stage_after_stop(
            stage_two::Session::Take,
            (refused, pressed),
            (not_flat, unexpected),
            (resting, not_read),
        );
        assert_eq!(
            got, want,
            "{refused:?} {pressed} {not_flat} {unexpected} {resting} {not_read}"
        );
    }
    // The crossing session's wording under the crossing session.
    let got = stage::stage_after_stop(
        stage_two::Session::Cross,
        (None, false),
        (false, true),
        (false, false),
    );
    assert_eq!(
        got,
        Some((places::Stop::ReadDisagrees, stage::CROSS_OTHERS))
    );
}

#[test]
fn a_taker_sessions_report_says_what_its_check_and_its_reads_showed() {
    // From the check and the reads alone, never the session's shape (FBC-77r8's review,
    // carried): the check not made, an order or a TWAP not ours, a position not flat; then the
    // check made and the reads clean; then a named account not read after it.
    let section = |run: CrossRun| {
        let two = StageTwo {
            crosses: vec![CrossRun {
                account: "primary",
                ..CrossRun::default()
            }],
            takes: vec![run],
            ..StageTwo::default()
        };
        let mut report = String::new();
        stage_two::markdown(&mut report, &two, &[]);
        let section = report.split("### The taker IOC on primary").nth(1).unwrap();
        section.to_owned()
    };
    let shown = section(CrossRun {
        others: Some(true),
        not_flat_after: Some(true),
        stop: Some("NotFlat"),
        ..take("primary", Vec::new())
    });
    for said in [
        "- No other order of ours (decision 0148 item 6): not checked: the session ended before \
         it",
        &format!(
            "- An order or a TWAP not ours in the reads after the session: {}",
            taker::OTHERS_TAKE
        ),
        &format!(
            "- A position not flat in the reads after the session: {}",
            stage_two::CROSS_POSITION
        ),
    ] {
        assert!(shown.contains(said), "{said}: {shown}");
    }
    let clean = section(CrossRun {
        checked: true,
        others: Some(false),
        not_flat_after: Some(false),
        ..take("primary", Vec::new())
    });
    for said in [
        "- No other order of ours (decision 0148 item 6): none: it may be placed",
        "- An order or a TWAP not ours in the reads after the session: none\n",
        "- A position not flat in the reads after the session: no\n",
    ] {
        assert!(clean.contains(said), "{said}: {clean}");
    }
    let unread = section(CrossRun {
        checked: true,
        ..take("primary", Vec::new())
    });
    assert!(
        unread.contains(
            "- An order or a TWAP not ours in the reads after the session: not known: a named \
             account was not read after the session"
        ),
        "{unread}"
    );
    let json = stage_two::to_json(&StageTwo {
        takes: vec![CrossRun {
            others: Some(true),
            not_flat_after: Some(true),
            stop: Some("ReadDisagrees"),
            ..take("primary", Vec::new())
        }],
        ..StageTwo::default()
    });
    let take = &json["takes"][0];
    assert_eq!(take["other_order_checked"], false);
    assert_eq!(take["others_after"], true);
    assert_eq!(take["owners_position"], stage_two::CROSS_POSITION_BESIDE);
}
