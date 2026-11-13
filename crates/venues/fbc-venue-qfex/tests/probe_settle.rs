//! FBC-jxau: what answers the probe's requests (decision 0132 items 1, 3 and 8, as 0134 items 5
//! to 7 and 0147 read them), under either probe mode (decision 0143 item 4).
//!
//! - The arm's answer: FBC-291a's capture of QFEX production, an ack echoing the arm on the
//!   primary account and on the subaccount, decodes to `Accepted` at `AckLevel::Final` under
//!   either probe mode, on QFEX's origins and on loopback, built directly and by the factory;
//!   an echo with a member added or of another type answers nothing, nor does anything but the
//!   arm's own echo while the arm is in flight on its epoch; an err echoing the arm rejects it
//!   with its documented code, unless its code is `RateLimited`, which holds every frame budget
//!   for its kept hint, or for the declared window when it has none, and settles nothing.
//! - Cancel settlement by the venue's order id (item 8): a `CANCELLED` `order_response` naming
//!   the one cancel in flight that names the order accepts it at Final before the order's
//!   update, a `NO_SUCH_ORDER` one rejects it `NotFound` and is no update; nothing else settles
//!   it, an err least of all, and an `InvalidOrderId` err keeps the epoch open.
//! - The private frames decoded under either probe mode (items 5 and 8, decision 0147 item 2):
//!   an `order_response` into an order update by the status table, a live `FILLED` ending the
//!   order only with nothing remaining; a `fill_response` into a fill keyed by its trade id,
//!   its fee a cost through the scope; a `position_response` into the account's position; a
//!   page no read asked for, and a frame on a symbol the spec table does not hold, into
//!   nothing; while every place still ends `Unknown` at its deadline. The recording-only codec
//!   still ends its epoch on each (FBC-zeb0's rule, which now holds only outside the modes).
//! - Replaying the journal, redacted as 0132 item 3 and 0134 say, gives the same events and
//!   effects (`probe_harness::run_twice`, which the scripts here run through).
//!
//! Every key, account, id and frame here is synthetic (`probe_harness`), but for the capture.

#[allow(dead_code)]
mod probe_harness;

use std::time::Duration;

use fbc_core::{
    AssetSym, CancelReason, CidMatch, ConnKey, DecodeError, Effect, ExecEvent, FillEvent,
    FillIdent, FillKey, FillOrigin, InstrumentId, ItemRef, Liquidity3, Lots, Money, OpKind,
    OrderUpdate, PxExact, RateCharge, Reject, RejectKind, RpcId, Side, SignedLots, SpecTable,
    SubmitOutcome, TerminalReject, Ticks, TrafficClass, VenueCommand, VenueConfig, VenueOrderState,
    Via, dispatch_market_data,
};
use fbc_runtime::{RateLimiter, RateUserKey, Request, SafetyReserve};
use fbc_venue_qfex::exec::{ARM, ErrCode, QfexExec};
use probe_harness::*;

/// The arm's echo with its members reordered and spaced.
const REORDERED: &str =
    r#"{ "params" : { "cancel_on_disconnect" : true } , "type" : "cancel_on_disconnect" }"#;
/// The arm's echo with its type escaped, equal to the arm once read.
const ESCAPED_TYPE: &str =
    r#"{"type":"cancel\u005fon_disconnect","params":{"cancel_on_disconnect":true}}"#;
const HINT_S: &str = r#""Rate limit exceeded, retry after 3 seconds""#;
const HINT_MS: &str = r#""Too many requests. Please retry after 200ms.""#;

// ----- The arm's answer (decision 0132 item 1) -----

#[test]
fn the_captured_arm_answer_decodes_to_accepted_at_final_on_either_account_in_either_mode() {
    for (prefix, account) in [("primary", "primary"), ("subaccount", SUB)] {
        let (before, window) = captured(prefix);
        assert_eq!(before.len(), 4, "{prefix}");
        // The answer the capture holds: an ack echoing the arm, byte for byte, last in the
        // window.
        let (last, answer) = window.last().unwrap();
        assert!(last.ends_with("-in-window.json"), "{last}");
        assert_eq!(*answer, ack(ARM), "{last}");
        for cfg in every_mode(account) {
            let mut steps = vec![Step::Open(1)];
            steps.extend(before.iter().map(|(_, f)| Step::frame(f)));
            steps.push(Step::Encode(ARM_ON, 1));
            steps.extend(window.iter().map(|(_, f)| Step::frame(f)));
            // A repeat of the ack, then a second arm and a disarm on the epoch.
            steps.push(Step::frame(answer));
            steps.push(Step::Encode(ARM_ON, 2));
            steps.push(Step::Encode(DISARM, 3));
            let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
            // The last confirmation alone reports the stream.
            assert!(
                outs[1].events.is_empty() && outs[1].effects.len() == 3,
                "{prefix}"
            );
            assert_eq!(outs[2..4], [Out::quiet(), Out::quiet()], "{prefix}");
            assert_eq!(outs[4], Out::events(vec![authenticated()]), "{prefix}");
            assert_eq!(outs[5], sent(arm_sent(1)));
            let ack_at = 5 + window.len();
            // Nothing before the ack answers the arm or ends the epoch: the primary account's
            // eleven positions on a symbol no spec holds, each pushing nothing and counted
            // (decision 0147 item 2).
            for (at, out) in outs[6..ack_at].iter().enumerate() {
                assert_eq!(*out, Out::quiet(), "{prefix} {at}");
            }
            // The ack: the arm accepted at Final, nothing else.
            assert_eq!(outs[ack_at], Out::events(vec![accepted(1)]), "{prefix}");
            // Once answered, nothing answers it again, and the epoch arms no more.
            assert_eq!(outs[ack_at + 1], Out::quiet(), "{prefix}");
            assert_eq!(
                outs[ack_at + 2],
                refused(fbc_core::NotSentReason::Unsupported)
            );
            assert_eq!(
                outs[ack_at + 3],
                refused(fbc_core::NotSentReason::Unsupported)
            );
            let counts = codec.counts();
            assert_eq!((counts.arm_accepted, counts.arm_rejected), (1, 0));
            assert_eq!(counts.acks, 2);
            assert_eq!(counts.unheld, (ack_at - 6) as u64, "{prefix}");
            assert_eq!(counts.unheld, if prefix == "primary" { 11 } else { 0 });
            // The journal keeps every captured frame byte for byte: the capture is what the
            // probe journaled, and its ack is the arm's own echo, kept in clear.
            let mut want: Vec<String> = before.iter().map(|(_, f)| f.clone()).collect();
            want.extend(window.iter().map(|(_, f)| f.clone()));
            want.push(answer.clone());
            assert_eq!(disk.frames, want, "{prefix}");
        }
        // The factory's codec answers it alike.
        let cfg = production(account);
        let mut built = factory(&cfg);
        open(built.as_mut(), 1);
        for (_, f) in &before {
            text(built.as_mut(), f);
        }
        assert_eq!(encode(built.as_mut(), &ARM_ON, 1), sent(arm_sent(1)));
        let outs: Vec<Out> = window
            .iter()
            .map(|(_, f)| text(built.as_mut(), f))
            .collect();
        assert_eq!(outs.last().unwrap(), &Out::events(vec![accepted(1)]));
        // The recording-only codec arms nothing, so the same ack answers nothing.
        let mut off = recording(&cfg);
        authenticate(&mut off, 1);
        assert_eq!(
            encode(&mut off, &ARM_ON, 1),
            refused(fbc_core::NotSentReason::Unsupported)
        );
        assert_eq!(text(&mut off, answer), Out::quiet());
    }
}

/// Each frame of `variants` on an epoch of its own, armed on rpc 1, then the arm's own ack:
/// what each variant gave and what the ack gave after it.
fn then_the_ack(cfg: &fbc_core::VenueConfig, variants: &[String]) -> Vec<(Out, Out)> {
    variants
        .iter()
        .map(|variant| {
            let mut steps = armed(1, 1);
            steps.push(Step::frame(variant));
            steps.push(Step::Frame(ack(ARM)));
            let (_, outs, _) = run_twice(|| probe_on(cfg), &steps);
            let n = outs.len();
            (outs[n - 2].clone(), outs[n - 1].clone())
        })
        .collect()
}

#[test]
fn an_arm_echo_with_a_member_added_or_of_another_type_answers_nothing() {
    let value = |v: &str| {
        format!(r#"{{"type":"cancel_on_disconnect","params":{{"cancel_on_disconnect":{v}}}}}"#)
    };
    let plus_params =
        r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true,"x":"y"}}"#;
    let plus_echo =
        r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true},"id":1}"#;
    let mut variants = vec![
        // A member added in the params, in the echo, in the body, or beside the envelope.
        ack(plus_params),
        ack(plus_echo),
        format!(r#"{{"ack":{{"incoming_message":{ARM},"note":"n"}}}}"#),
        format!(r#"{{"ack":{{"incoming_message":{ARM}}},"seq":3}}"#),
        format!(
            r#"{{"err":{{"error_code":"InvalidParameter","message":"m","incoming_message":{ARM}}},"seq":3}}"#
        ),
        nested("InvalidParameter", r#""m""#, plus_params),
        format!(
            r#"{{"type":"Err","error_code":"InvalidOrder","message":"m","incoming_message":{ARM},"extra":1}}"#
        ),
        // Another type: for the flag, for the params, for the echo, for the request.
        ack(r#"{"type":"cancel_on_disconnect","params":[]}"#),
        ack(r#""cancel_on_disconnect""#),
        ack("{}"),
        ack("null"),
        ack(r#"{"type":"subscribe","params":{"cancel_on_disconnect":true}}"#),
        flat("ServerError", "null", r#""cancel_on_disconnect""#),
        // An order the arm cancels and a position: never the arm's answer.
        fixture("order_response.json").replace("\"ACK\"", "\"CANCELLED\""),
        fixture("position_response.json"),
    ];
    variants.extend(["\"true\"", "1", "null", "false"].map(|v| ack(&value(v))));
    for cfg in [preprod(SUB), production_loopback("primary")] {
        for (variant, (out, after)) in variants.iter().zip(then_the_ack(&cfg, &variants)) {
            assert!(out.outcomes().is_empty(), "{variant}: {out:?}");
            assert!(!out.asks_to_reconnect(), "{variant}: {out:?}");
            // The arm is still in flight: its own ack accepts it.
            assert_eq!(after, Out::events(vec![accepted(1)]), "{variant}");
        }
        // The arm's echo reordered and spaced, or with an escape that reads as the arm, is
        // the arm's own echo.
        for echo in [REORDERED, ESCAPED_TYPE] {
            let mut steps = armed(1, 1);
            steps.push(Step::Frame(ack(echo)));
            let (_, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
            assert_eq!(
                *outs.last().unwrap(),
                Out::events(vec![accepted(1)]),
                "{echo}"
            );
            // Kept in clear: replay reads the arm's own echo.
            assert_eq!(*disk.frames.last().unwrap(), ack(echo));
        }
    }
}

#[test]
fn only_the_arm_in_flight_on_its_own_authenticated_epoch_is_answered() {
    let arm_ack = || Step::Frame(ack(ARM));
    let cfg = preprod("primary");
    let mut steps = epoch(1);
    // Before the epoch's arm: an ack echoing it answers nothing; then the arm, which it does.
    steps.extend([arm_ack(), Step::Encode(ARM_ON, 1), arm_ack(), arm_ack()]);
    // The next epoch: the last epoch's arm is no request of it, until its own arm.
    steps.extend(epoch(2));
    steps.extend([arm_ack(), Step::Encode(ARM_ON, 2)]);
    // Past its deadline, the arm is answered by nothing.
    steps.extend([Step::Deadline(2), arm_ack()]);
    // Before the stream is authenticated, the arm is not sent, and nothing is answered.
    steps.extend([
        Step::Open(3),
        Step::frame(AUTHENTICATED),
        Step::Encode(ARM_ON, 3),
        arm_ack(),
    ]);
    // An epoch the codec ended: the echo answers nothing.
    steps.extend(armed(4, 4));
    steps.extend([Step::frame("[1]"), arm_ack()]);
    let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
    let answered: Vec<(usize, ExecEvent)> = outs
        .iter()
        .enumerate()
        .flat_map(|(at, out)| out.outcomes().into_iter().map(move |e| (at, e.clone())))
        .collect();
    // The ack after the arm, and the deadline; no other step answers a request.
    assert_eq!(answered, vec![(7, accepted(1)), (16, unknown(2))]);
    assert_eq!(outs[6], sent(arm_sent(1)));
    assert_eq!(outs[20], refused(fbc_core::NotSentReason::Disconnected));
    assert!(outs[28].reconnects());
    assert_eq!(codec.counts().arm_accepted, 1);
    assert_eq!(codec.counts().acks, 7);
}

// ----- An err echoing the arm (decision 0132 item 1, 0134 item 7, 0147 item 1) -----

/// Each frame of `frames` on an armed epoch of its own: what it gave, the counts after it, and
/// what the arm's ack gave next.
fn each_on_its_epoch(frames: &[String]) -> Vec<(Out, QfexExec, Out)> {
    frames
        .iter()
        .map(|frame| {
            let mut steps = armed(1, 1);
            steps.push(Step::frame(frame));
            steps.push(Step::Frame(ack(ARM)));
            let (codec, outs, _) = run_twice(|| probe_on(&production(SUB)), &steps);
            let n = outs.len();
            (outs[n - 2].clone(), codec, outs[n - 1].clone())
        })
        .collect()
}

#[test]
fn an_err_echoing_the_arm_rejects_it_and_a_rate_limited_one_holds_for_its_hint_and_settles_nothing()
{
    // Every documented code but RateLimited, an undocumented one, none, and one of another
    // type, in either err envelope: the arm rejected with the documented code alone.
    let mut cases: Vec<(String, Option<ErrCode>, bool)> = Vec::new();
    for code in ErrCode::ALL {
        if code == ErrCode::RateLimited {
            continue;
        }
        for frame in [
            nested(code.wire(), r#""cannot arm""#, ARM),
            flat(code.wire(), "null", ARM),
        ] {
            cases.push((frame, Some(code), code.ends_session()));
        }
    }
    for frame in [
        nested("ArmNotAllowedHere", r#""no""#, ARM),
        flat("ArmNotAllowedHere", "null", ARM),
        format!(r#"{{"err":{{"message":"no","incoming_message":{ARM}}}}}"#),
        format!(r#"{{"type":"Err","incoming_message":{ARM}}}"#),
        format!(r#"{{"err":{{"error_code":7,"incoming_message":{ARM}}}}}"#),
        format!(r#"{{"type":"Err","error_code":null,"incoming_message":{ARM}}}"#),
    ] {
        cases.push((frame, None, true));
    }
    let frames: Vec<String> = cases.iter().map(|(f, _, _)| f.clone()).collect();
    for ((frame, code, ends), (out, codec, after)) in cases.iter().zip(each_on_its_epoch(&frames)) {
        let want = rejected(1, code.map(ErrCode::wire));
        assert_eq!(
            out.events,
            vec![(fbc_core::VenueMeta::NONE, want)],
            "{frame}"
        );
        assert!(out.result.is_ok(), "{frame}");
        // A request's code leaves the epoch open, for the session to end (decision 0058); a
        // code about the session, or an undocumented one, ends it here too (0134 item 5).
        assert_eq!(
            out.reconnects() || out.asks_to_reconnect(),
            *ends,
            "{frame}"
        );
        assert_eq!(out.effects.len(), usize::from(*ends), "{frame}");
        let counts = codec.counts();
        assert_eq!(
            (counts.arm_rejected, counts.arm_accepted),
            (1, 0),
            "{frame}"
        );
        match code {
            Some(code) => assert_eq!(counts.errs(*code), 1, "{frame}"),
            None => assert_eq!(counts.undocumented_errs, 1, "{frame}"),
        }
        // Answered once: the ack that follows answers nothing.
        assert!(after.outcomes().is_empty(), "{frame}: {after:?}");
    }

    // RateLimited, in either envelope: a hold of every frame budget, for a kept hint or for
    // none, and no answer, the arm left to its deadline: a later ack answers nothing (decision
    // 0132 item 1).
    let overflow = r#""Rate limit exceeded, retry after 18446744073709552 seconds""#;
    let zero = r#""Rate limit exceeded, retry after 0 seconds""#;
    let escaped = r#""Rate limit exceeded, retry after 3 second\u0073""#;
    // Each message, the hold it asks for, and whether the journal keeps it in clear: exactly
    // when it is one of the two hint forms, whatever its N, judged on its bytes (0134 item 3).
    let holds: Vec<(&str, Option<Duration>, bool)> = vec![
        (HINT_S, Some(Duration::from_secs(3)), true),
        (HINT_MS, Some(Duration::from_millis(200)), true),
        (r#""slow down""#, None, false),
        ("null", None, false),
        (zero, None, true),
        (overflow, None, true),
        (escaped, None, false),
    ];
    for cfg in [preprod("primary"), production(SUB)] {
        for (message, retry, clear) in &holds {
            for (n, frame) in [
                nested("RateLimited", message, ARM),
                flat("RateLimited", message, REORDERED),
            ]
            .into_iter()
            .enumerate()
            {
                let mut steps = armed(1, 1);
                steps.push(Step::frame(&frame));
                // Each ends at its deadline, the second after an ack that answers nothing.
                if n == 1 {
                    steps.push(Step::Frame(ack(ARM)));
                }
                steps.push(Step::Deadline(1));
                let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
                let k = outs.len();
                assert_eq!(outs[k - 2 - n], held(*retry), "{frame}");
                if n == 1 {
                    assert_eq!(outs[k - 2], Out::quiet(), "{frame}");
                }
                assert_eq!(outs[k - 1], Out::events(vec![unknown(1)]), "{frame}");
                let counts = codec.counts();
                assert_eq!((counts.arm_rejected, counts.arm_accepted), (0, 0));
                let kept = &disk.frames[disk.frames.len() - 1 - n];
                assert_eq!(kept == &frame, *clear, "{kept}");
                let quoted = message.strip_prefix('"').and_then(|m| m.strip_suffix('"'));
                assert!(*clear || quoted.is_none_or(|m| !disk.has(m)), "{kept}");
            }
        }
        // An absent message holds for the declared window too, and leaves the arm to its
        // deadline; a RateLimited err beside a member added is not the arm's echo, so it holds
        // for its hint and the arm's ack still accepts it.
        for (frame, retry, last) in [
            (
                format!(r#"{{"err":{{"error_code":"RateLimited","incoming_message":{ARM}}}}}"#),
                None,
                Out::quiet(),
            ),
            (
                format!(
                    r#"{{"type":"Err","error_code":"RateLimited","message":{HINT_S},"incoming_message":{ARM},"extra":"x"}}"#
                ),
                Some(Duration::from_secs(3)),
                Out::events(vec![accepted(1)]),
            ),
        ] {
            let mut steps = armed(1, 1);
            steps.push(Step::frame(&frame));
            steps.push(Step::Frame(ack(ARM)));
            let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
            let k = outs.len();
            assert_eq!(outs[k - 2], held(retry), "{frame}");
            assert_eq!(outs[k - 1], last, "{frame}");
        }
    }
}

#[test]
fn a_rate_limited_arm_echo_holds_every_frame_budget_for_its_hint_or_the_declared_window() {
    let limits = caps().exec_limits.unwrap();
    let reserve = SafetyReserve::percent(50).unwrap();
    let conn = ConnKey { conn: 1, epoch: 1 };
    let request = |op, class| Request {
        charge: RateCharge::one(op, None),
        via: Via::Frame,
        class,
    };
    let every = [
        request(OpKind::Place, TrafficClass::Normal),
        request(OpKind::Cancel, TrafficClass::Safety),
        request(OpKind::Control, TrafficClass::Safety),
        request(OpKind::Query, TrafficClass::Normal),
    ];
    for (user, message, held_for, admitted_at) in [
        (0x7a0_0001_u64, HINT_S, 2, 4),
        (0x7a0_0002_u64, r#""slow down""#, 59, 61),
    ] {
        let budget = RateLimiter::for_user(RateUserKey::new(user), &limits, reserve).unwrap();
        let mut codec = probe_on(&preprod("primary"));
        authenticate(&mut codec, 1);
        assert_eq!(encode(&mut codec, &ARM_ON, 1), sent(arm_sent(1)));
        let out = text(&mut codec, &nested("RateLimited", message, ARM));
        let [
            Effect::RateLimited {
                charge,
                retry_after,
                ..
            },
        ] = out.effects.as_slice()
        else {
            panic!("{out:?}")
        };
        assert_eq!(*charge, None, "the arm's echo names no bucket");
        let now = tokio::time::Instant::now();
        let at = |secs| now + Duration::from_secs(secs);
        budget.provider_rejected(now, conn, *charge, *retry_after);
        for req in every {
            assert!(budget.charge(now, conn, &[req]).is_err(), "{message}");
            assert!(
                budget.charge(at(held_for), conn, &[req]).is_err(),
                "{message}"
            );
        }
        for req in every {
            assert!(
                budget.charge(at(admitted_at), conn, &[req]).is_ok(),
                "{message}"
            );
        }
    }
}

// ----- Cancel settlement by venue id (decision 0132 item 8) -----

/// The cancel of [`ORDER`] as the codec sends it, which an err or an ack may echo.
const CANCEL_SENT: &str = r#"{"type":"cancel_order","params":{"order_id":"2f05173c-992a-426b-bcaf-6b341644bb62","symbol":"AAPL-USD","cancel_order_id_type":"order_id"}}"#;
/// Another made-up venue order UUID.
const OTHER: &str = "8b1c0f9e-1d2e-4f3a-9b8c-7d6e5f4a3b2c";

fn invalid() -> String {
    format!("{:?}", DecodeError::Malformed("invalid QFEX private state"))
}

fn unmapped() -> String {
    format!("{:?}", DecodeError::Malformed("unmapped QFEX order status"))
}

/// `value` as a JSON string, for a frame's member.
fn q(value: &str) -> String {
    quoted(value)
}

/// [`ORDER`]'s order update as [`ORDER_ROW`] reads with `state` and `cum` lots filled, under
/// the client id `cid`.
fn update(state: VenueOrderState, cum: i64, cid: Option<CidMatch>) -> ExecEvent {
    ExecEvent::Order(OrderUpdate {
        cid,
        vid: Some(venue_id(ORDER)),
        inst: AAPL,
        side: Side::Buy,
        state,
        cum_filled: Lots::new(cum).unwrap(),
        px: Some(Ticks(20_000)),
        qty: Some(Lots::new(100).unwrap()),
        post_only: None,
        reduce_only: None,
    })
}

/// [`ORDER_ROW`]'s client id: not canonical, so not ours.
fn theirs() -> Option<CidMatch> {
    Some(CidMatch::Unparseable)
}

/// A `CANCELLED` [`ORDER`] with 0.4 left: 0.6 filled first.
fn cancelled() -> String {
    order_frame(&[
        ("status", Some(r#""CANCELLED""#)),
        ("quantity_remaining", Some("0.4")),
    ])
}

/// What [`cancelled`] pushes as an order update.
fn cancelled_update() -> ExecEvent {
    update(VenueOrderState::Canceled(CancelReason::Venue), 60, theirs())
}

/// `NO_SUCH_ORDER` naming `id`, its order id and status alone.
fn no_such(id: &str) -> String {
    format!(
        r#"{{"order_response":{{"order_id":{},"status":"NO_SUCH_ORDER"}}}}"#,
        q(id)
    )
}

/// The request `rpc` settled for the order [`ORDER`], as a reply naming it answers it.
fn settled(rpc: u64, outcome: SubmitOutcome) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item: Some(ItemRef {
            idx: 0,
            cid: None,
            vid: Some(venue_id(ORDER)),
        }),
        outcome,
    }
}

fn final_ack() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: fbc_core::AckLevel::Final,
    }
}

fn not_found() -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind: RejectKind::NotFound,
        venue_code: Some("NO_SUCH_ORDER".into()),
        raw: "NO_SUCH_ORDER".into(),
    })
}

/// Epoch 1 armed on rpc 1 and a cancel of [`ORDER`] sent on rpc 2, then `rest`, live and
/// replayed: the codec, and what each of `rest` gave.
fn after_cancel(cfg: &VenueConfig, rest: Vec<Step>) -> (QfexExec, Vec<Out>) {
    let mut steps = armed(1, 1);
    steps.push(Step::Encode(cancel(ORDER), 2));
    let k = steps.len();
    steps.extend(rest);
    let (codec, outs, _) = run_twice(|| probe_on(cfg), &steps);
    let sent = &outs[k - 1];
    assert!(sent.result.is_ok() && sent.effects.len() == 1, "{sent:?}");
    (codec, outs[k..].to_vec())
}

/// The settle configurations: both modes, both accounts, on QFEX's origins and on loopback.
fn settle_modes() -> [VenueConfig; 4] {
    [
        preprod("primary"),
        production(SUB),
        loopback(SUB),
        production_loopback("primary"),
    ]
}

#[test]
fn cancelled_and_no_such_order_settle_a_cancel_by_venue_id_and_ends_nothing_else() {
    // NO_SUCH_ORDER ends nothing: fbc-oms reads NotFound as a query to make, never a terminal
    // state (decision 0005).
    assert!(TerminalReject::new(RejectKind::NotFound).is_none());
    for cfg in settle_modes() {
        // CANCELLED naming the cancel's order: accepted at Final, then the order's update;
        // once settled, a repeat is the update alone, and the arm is left to its deadline.
        let (codec, outs) = after_cancel(
            &cfg,
            vec![
                Step::Frame(cancelled()),
                Step::Frame(cancelled()),
                Step::Deadline(1),
            ],
        );
        assert_eq!(
            outs,
            [
                Out::events(vec![settled(2, final_ack()), cancelled_update()]),
                Out::events(vec![cancelled_update()]),
                Out::events(vec![unknown(1)]),
            ]
        );
        let counts = codec.counts();
        assert_eq!((counts.cancels_accepted, counts.cancels_not_found), (1, 0));

        // NO_SUCH_ORDER naming it, read for its order id alone, whatever else it carries or
        // lacks: rejected NotFound and no update; a CANCELLED after it settles nothing more.
        for frame in [
            no_such(ORDER),
            order_frame(&[
                ("status", Some(r#""NO_SUCH_ORDER""#)),
                ("price", Some("0")),
                ("quantity", Some("0")),
                ("quantity_remaining", Some("0")),
                ("side", Some(r#""""#)),
            ]),
            order_frame(&[
                ("status", Some(r#""NO_SUCH_ORDER""#)),
                ("symbol", Some(r#""NVDA-USD""#)),
            ]),
        ] {
            let (codec, outs) =
                after_cancel(&cfg, vec![Step::frame(&frame), Step::Frame(cancelled())]);
            assert_eq!(
                outs,
                [
                    Out::events(vec![settled(2, not_found())]),
                    Out::events(vec![cancelled_update()]),
                ],
                "{frame}"
            );
            let counts = codec.counts();
            assert_eq!((counts.cancels_accepted, counts.cancels_not_found), (0, 1));
        }

        // NO_SUCH_ORDER with no cancel in flight pushes nothing and keeps the epoch open; one
        // whose order id is not a UUID is a decode error, the cancel left to settle.
        let mut steps = armed(1, 1);
        steps.extend([Step::Frame(no_such(ORDER)), Step::Encode(cancel(ORDER), 2)]);
        steps.extend([Step::Frame(no_such("bad")), Step::Frame(no_such(ORDER))]);
        let (_, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        let n = outs.len();
        assert_eq!(outs[n - 4], Out::quiet());
        assert!(outs[n - 3].result.is_ok() && outs[n - 3].effects.len() == 1);
        assert_eq!(
            outs[n - 2],
            Out {
                result: Err(invalid()),
                ..Out::quiet()
            }
        );
        assert_eq!(outs[n - 1], Out::events(vec![settled(2, not_found())]));
    }
}

/// Whether no step of `outs` but those `at` pushed an outcome, and each of those exactly
/// `want`.
fn only_outcomes(outs: &[Out], want: &[(usize, ExecEvent)]) {
    let got: Vec<(usize, ExecEvent)> = outs
        .iter()
        .enumerate()
        .flat_map(|(at, out)| out.outcomes().into_iter().map(move |e| (at, e.clone())))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn a_reply_settles_only_the_one_cancel_it_names_and_nothing_by_its_arrival() {
    let upper = ORDER.to_uppercase();
    for cfg in [preprod("primary"), production(SUB)] {
        // On one epoch, none of these settles the cancel: another order's CANCELLED, the same
        // order's id in another case, every other status naming it (updates, an unmapped
        // status, a rate refusal), and NO_SUCH_ORDER for another order. Its deadline ends it.
        let other =
            |status: &str| order_frame(&[("order_id", Some(&q(OTHER))), ("status", Some(status))]);
        let named = |status: &str, remaining: &str| {
            order_frame(&[
                ("status", Some(status)),
                ("quantity_remaining", Some(remaining)),
            ])
        };
        let rest = vec![
            Step::Frame(other(r#""CANCELLED""#)),
            Step::Frame(order_frame(&[
                ("order_id", Some(&q(&upper))),
                ("status", Some(r#""CANCELLED""#)),
            ])),
            Step::Frame(no_such(&upper)),
            Step::Frame(no_such(OTHER)),
            Step::Frame(named(r#""ACK""#, "1.0")),
            Step::Frame(named(r#""FILLED""#, "1.0")),
            Step::Frame(named(r#""IOC_PARTIALLY_FILLED""#, "0.4")),
            Step::Frame(named(r#""IOC_CANCELLED""#, "0.4")),
            Step::Frame(named(r#""REJECTED""#, "1.0")),
            Step::Frame(named(r#""CANCELLED_STP""#, "1.0")),
            Step::Frame(named(r#""RATE_LIMITED""#, "1.0")),
            Step::Deadline(2),
            Step::Frame(cancelled()),
            Step::Frame(no_such(ORDER)),
        ];
        let (codec, outs) = after_cancel(&cfg, rest);
        only_outcomes(&outs, &[(11, unknown(2))]);
        let other_vid = |state| {
            ExecEvent::Order(OrderUpdate {
                vid: Some(venue_id(OTHER)),
                ..match update(state, 0, theirs()) {
                    ExecEvent::Order(u) => u,
                    _ => unreachable!(),
                }
            })
        };
        assert_eq!(
            outs[0],
            Out::events(vec![other_vid(VenueOrderState::Canceled(
                CancelReason::Venue
            ))])
        );
        let ExecEvent::Order(upper_update) = &outs[1].events[0].1 else {
            panic!("{:?}", outs[1])
        };
        assert_eq!(upper_update.vid, Some(venue_id(&upper)));
        assert_eq!(outs[2..4], [Out::quiet(), Out::quiet()]);
        assert_eq!(
            outs[4],
            Out::events(vec![update(VenueOrderState::Open, 0, theirs())])
        );
        assert_eq!(
            outs[5],
            Out::events(vec![update(VenueOrderState::Open, 0, theirs())])
        );
        assert_eq!(
            outs[6],
            Out::events(vec![update(VenueOrderState::Open, 60, theirs())])
        );
        assert_eq!(
            outs[7],
            Out::events(vec![update(
                VenueOrderState::Canceled(CancelReason::Unfilled),
                60,
                theirs()
            )])
        );
        for out in &outs[8..10] {
            assert_eq!(
                *out,
                Out {
                    result: Err(unmapped()),
                    ..Out::quiet()
                }
            );
        }
        assert_eq!(outs[10], held(None));
        // Past its deadline the cancel is in flight no more.
        assert_eq!(outs[12], Out::events(vec![cancelled_update()]));
        assert_eq!(outs[13], Out::quiet());
        let counts = codec.counts();
        assert_eq!(counts.unmapped_statuses, 2);
        assert_eq!(
            (
                counts.cancels_accepted,
                counts.cancels_not_found,
                counts.replies_naming_several
            ),
            (0, 0, 0)
        );

        // Two cancels of the order, or a cancel and a query of it: a reply naming it names
        // more than one request, and settles neither; each ends Unknown at its deadline.
        for second in [cancel(ORDER), query(ORDER)] {
            let (codec, outs) = after_cancel(
                &cfg,
                vec![
                    Step::Encode(second.clone(), 3),
                    Step::Frame(cancelled()),
                    Step::Frame(no_such(ORDER)),
                    Step::Deadline(2),
                    Step::Deadline(3),
                ],
            );
            assert!(outs[0].result.is_ok() && outs[0].effects.len() == 1);
            assert_eq!(outs[1], Out::events(vec![cancelled_update()]));
            assert_eq!(outs[2], Out::quiet());
            only_outcomes(&outs, &[(3, unknown(2)), (4, unknown(3))]);
            assert_eq!(codec.counts().replies_naming_several, 2, "{second:?}");
        }
        // A query alone is never settled by an order_response, and a reply naming it is no
        // second request.
        let mut steps = armed(1, 1);
        steps.extend([
            Step::Encode(query(ORDER), 2),
            Step::Frame(cancelled()),
            Step::Frame(no_such(ORDER)),
            Step::Deadline(2),
        ]);
        let (codec, outs, _) = run_twice(|| probe_on(&cfg), &steps);
        let n = outs.len();
        only_outcomes(&outs[n - 3..], &[(2, unknown(2))]);
        assert_eq!(codec.counts().replies_naming_several, 0);

        // A cancel of the last epoch is no request of this one: the reply settles nothing,
        // and the runtime ends it Unknown at its deadline.
        let mut rest = epoch(2);
        rest.extend([
            Step::Frame(cancelled()),
            Step::Frame(no_such(ORDER)),
            Step::Deadline(2),
        ]);
        let (_, outs) = after_cancel(&cfg, rest);
        let n = outs.len();
        only_outcomes(&outs[n - 3..], &[(2, unknown(2))]);
        assert_eq!(outs[n - 3], Out::events(vec![cancelled_update()]));

        // A CANCELLED not as documented settles nothing and keeps the epoch open: a later one
        // settles the cancel.
        let (_, outs) = after_cancel(
            &cfg,
            vec![
                Step::Frame(order_frame(&[
                    ("status", Some(r#""CANCELLED""#)),
                    ("price", Some("200.005")),
                ])),
                Step::Frame(order_frame(&[
                    ("status", Some(r#""CANCELLED""#)),
                    ("quantity", None),
                ])),
                Step::Frame(cancelled()),
            ],
        );
        assert_eq!(
            outs,
            [
                Out {
                    result: Err(invalid()),
                    ..Out::quiet()
                },
                Out {
                    result: Err(invalid()),
                    ..Out::quiet()
                },
                Out::events(vec![settled(2, final_ack()), cancelled_update()]),
            ]
        );

        // Recording-only, a reply naming its cancel ends the epoch and settles nothing.
        for frame in [cancelled(), no_such(ORDER)] {
            let mut off = recording(&cfg);
            authenticate(&mut off, 1);
            let sent = encode(&mut off, &cancel(ORDER), 1);
            assert!(sent.result.is_ok() && sent.effects.len() == 1);
            assert!(text(&mut off, &frame).reconnects(), "{frame}");
        }
    }
}

#[test]
fn an_err_settles_no_cancel_and_an_invalid_order_id_err_keeps_the_epoch_open() {
    let cfg = production(SUB);
    // Every err with a request's code, whatever it echoes: quiet and counted, the cancel then
    // settled by the CANCELLED naming its order.
    let mut frames = Vec::new();
    for code in ErrCode::ALL {
        if code == ErrCode::RateLimited || code.ends_session() {
            continue;
        }
        frames.push((code, nested(code.wire(), r#""bad order""#, CANCEL_SENT)));
        frames.push((code, flat(code.wire(), "null", CANCEL_SENT)));
    }
    let id = ErrCode::InvalidOrderId;
    frames.extend([
        (id, nested(id.wire(), r#""bad order""#, "{}")),
        (id, flat(id.wire(), "null", r#""cancel_order""#)),
        (
            id,
            r#"{"err":{"error_code":"InvalidOrderId","message":"bad order"}}"#.to_owned(),
        ),
    ]);
    for (code, frame) in &frames {
        let (codec, outs) = after_cancel(&cfg, vec![Step::frame(frame), Step::Frame(cancelled())]);
        assert_eq!(
            outs,
            [
                Out::quiet(),
                Out::events(vec![settled(2, final_ack()), cancelled_update()]),
            ],
            "{frame}"
        );
        assert_eq!(codec.counts().errs(*code), 1, "{frame}");
    }
    // An ack echoing the cancel: counted, and settles nothing.
    let (codec, outs) = after_cancel(&cfg, vec![Step::Frame(ack(CANCEL_SENT)), Step::Deadline(2)]);
    assert_eq!(outs, [Out::quiet(), Out::events(vec![unknown(2)])]);
    assert_eq!(codec.counts().acks, 1);
    // A RateLimited err echoing the cancel holds the cancel bucket for its hint, and settles
    // nothing.
    let (_, outs) = after_cancel(
        &cfg,
        vec![
            Step::Frame(nested(
                "RateLimited",
                r#""Rate limit exceeded, retry after 2 seconds""#,
                CANCEL_SENT,
            )),
            Step::Frame(cancelled()),
        ],
    );
    assert_eq!(
        outs[0],
        Out::effects(vec![Effect::RateLimited {
            stream: fbc_venue_qfex::exec::TRADE,
            charge: Some(RateCharge::one(OpKind::Cancel, None)),
            retry_after: Some(Duration::from_secs(2)),
        }])
    );
    assert_eq!(
        outs[1],
        Out::events(vec![settled(2, final_ack()), cancelled_update()])
    );
    // An err about the session, or an undocumented one, echoing the cancel ends the epoch and
    // settles nothing: the cancel ends Unknown at its deadline, and a reply on the next epoch
    // settles nothing.
    let mut ends: Vec<String> = ErrCode::ALL
        .into_iter()
        .filter(|code| code.ends_session())
        .map(|code| flat(code.wire(), "null", CANCEL_SENT))
        .collect();
    ends.push(nested("OrderGone", r#""gone""#, CANCEL_SENT));
    for frame in &ends {
        let mut rest = vec![Step::frame(frame), Step::Deadline(2)];
        rest.extend(epoch(2));
        rest.push(Step::Frame(cancelled()));
        let (_, outs) = after_cancel(&cfg, rest);
        assert!(outs[0].reconnects(), "{frame}: {:?}", outs[0]);
        assert_eq!(outs[1], Out::events(vec![unknown(2)]));
        only_outcomes(&outs, &[(1, unknown(2))]);
        assert_eq!(*outs.last().unwrap(), Out::events(vec![cancelled_update()]));
    }
    // Before the stream is authenticated, InvalidOrderId ends the epoch, as every err but
    // RateLimited does (decision 0134 item 5).
    let mut early = probe_on(&cfg);
    open(&mut early, 1);
    text(&mut early, AUTHENTICATED);
    text(&mut early, &confirm("order_responses"));
    assert!(text(&mut early, &nested("InvalidOrderId", "null", CANCEL_SENT)).reconnects());
}

// ----- Private frames decoded under either probe mode (decision 0132 items 5 and 8) -----

/// `frame` on a fresh authenticated codec `cfg` configures: what it gave, and the codec.
fn decoded(cfg: &VenueConfig, frame: &str) -> (Out, QfexExec) {
    let mut codec = probe_on(cfg);
    authenticate(&mut codec, 1);
    let out = text(&mut codec, frame);
    (out, codec)
}

fn usd() -> AssetSym {
    AssetSym::new("USD").unwrap()
}

/// [`FILL_ROW`]'s fill, under the client id `cid`, its fee `nanos` billionths of a USD.
fn fill(cid: Option<CidMatch>, nanos: i128) -> FillEvent {
    let fee = fbc_core::dispatch(&caps(), NS, |scope| scope.fee(nanos, usd())).unwrap();
    FillEvent {
        ident: FillIdent::Venue {
            fill: dispatch_market_data(&caps(), |scope| scope.fill_id(TRADE_ID)).unwrap(),
            vid: Some(venue_id(ORDER)),
            cum_after: None,
        },
        origin: FillOrigin::Order {
            cid,
            px: Ticks(20_000),
        },
        inst: AAPL,
        side: Side::Sell,
        qty: Lots::new(100).unwrap(),
        liquidity: Liquidity3::Unknown,
        fee,
        realized_pnl: None,
        realized_funding: None,
        replay: false,
    }
}

fn position(inst: InstrumentId, lots: i64, avg: Option<PxExact>) -> ExecEvent {
    ExecEvent::Position {
        inst,
        qty: SignedLots(lots),
        avg_entry: avg,
    }
}

#[test]
fn an_order_response_decodes_to_an_order_update_by_its_status_and_quantities() {
    let ours = q(&wire_cid(0));
    let filled = VenueOrderState::Filled;
    let open = VenueOrderState::Open;
    // Each status and quantity left, the state and the lots filled it reads as: a FILLED or
    // IOC_PARTIALLY_FILLED ends the order only with nothing left (decision 0132 item 8).
    let table: Vec<(&str, &str, VenueOrderState, i64)> = vec![
        ("ACK", "1.0", open.clone(), 0),
        ("FILLED", "0", filled.clone(), 100),
        ("FILLED", "0.0", filled.clone(), 100),
        ("FILLED", "-0.0", filled.clone(), 100),
        ("FILLED", "0.4", open.clone(), 60),
        ("FILLED", "1.0", open.clone(), 0),
        ("IOC_PARTIALLY_FILLED", "0.4", open.clone(), 60),
        ("IOC_PARTIALLY_FILLED", "0", filled.clone(), 100),
        (
            "IOC_CANCELLED",
            "0.4",
            VenueOrderState::Canceled(CancelReason::Unfilled),
            60,
        ),
        (
            "CANCELLED",
            "1.0",
            VenueOrderState::Canceled(CancelReason::Venue),
            0,
        ),
    ];
    for cfg in [preprod("primary"), production(SUB)] {
        for (status, remaining, state, cum) in &table {
            let frame = order_frame(&[
                ("status", Some(&q(status))),
                ("quantity_remaining", Some(remaining)),
            ]);
            let (out, codec) = decoded(&cfg, &frame);
            assert_eq!(
                out,
                Out::events(vec![update(state.clone(), *cum, theirs())]),
                "{frame}"
            );
            assert_eq!(codec.counts().private_state, 1);
        }
        // A FILLED naming its trade is one order update, and no fill (decision 0005).
        let frame = order_frame(&[
            ("status", Some(r#""FILLED""#)),
            ("quantity_remaining", Some("0")),
            ("trade_id", Some(&q(TRADE_ID))),
        ]);
        assert_eq!(
            decoded(&cfg, &frame).0,
            Out::events(vec![update(filled.clone(), 100, theirs())])
        );
        // The client id: none when empty, absent or null; ours when it is ours.
        for cid in [Some(r#""""#), None, Some("null")] {
            let frame = order_frame(&[("client_order_id", cid)]);
            assert_eq!(
                decoded(&cfg, &frame).0,
                Out::events(vec![update(open.clone(), 0, None)]),
                "{frame}"
            );
        }
        let frame = order_frame(&[("client_order_id", Some(&ours))]);
        assert_eq!(
            decoded(&cfg, &frame).0,
            Out::events(vec![update(
                open.clone(),
                0,
                Some(CidMatch::Ours(cids()[0]))
            )])
        );

        // Unmapped statuses, counted; and every shape QFEX's pages do not give: a decode error
        // each, nothing pushed, the epoch kept open for a cancel.
        let unmapped_frames: Vec<String> = ["MODIFIED", "REJECTED", "EXPIRED", "NOT_FOUND"]
            .map(|status| order_frame(&[("status", Some(&q(status)))]))
            .into();
        let invalid_frames: Vec<String> = [
            ("status", None),
            ("status", Some("5")),
            ("price", Some("200.005")),
            ("price", Some("0")),
            ("price", Some(r#""200.0""#)),
            ("quantity", Some("1.005")),
            ("quantity_remaining", Some("1.1")),
            ("quantity_remaining", Some("0.005")),
            ("quantity_remaining", Some("-0.1")),
            ("trade_id", Some("true")),
            ("trade_id", Some(r#""t-1""#)),
            ("order_id", Some(r#""bad""#)),
            ("order_id", None),
            ("client_order_id", Some("5")),
            ("side", Some(r#""HOLD""#)),
            ("symbol", Some(r#""""#)),
            ("symbol", None),
        ]
        .map(|change| order_frame(&[change]))
        .into_iter()
        // A size off the step whose filled part is on it.
        .chain([order_frame(&[
            ("quantity", Some("1.005")),
            ("quantity_remaining", Some("0.005")),
        ])])
        .collect();
        let wrong = [
            (unmapped(), unmapped_frames),
            (invalid(), invalid_frames),
            (
                invalid(),
                vec![
                    r#"{"order_response":[]}"#.to_owned(),
                    format!(
                        r#"{{"order_response":{},"fill_response":{}}}"#,
                        row(ORDER_ROW, &[]),
                        row(FILL_ROW, &[])
                    ),
                ],
            ),
        ];
        for (want, frames) in wrong {
            for frame in frames {
                let (out, mut codec) = decoded(&cfg, &frame);
                assert_eq!(
                    out,
                    Out {
                        result: Err(want.clone()),
                        ..Out::quiet()
                    },
                    "{frame}"
                );
                let sent = encode(&mut codec, &cancel(ORDER), 1);
                assert!(sent.result.is_ok() && sent.effects.len() == 1, "{frame}");
                let unmapped = u64::from(want == unmapped());
                assert_eq!(codec.counts().unmapped_statuses, unmapped, "{frame}");
            }
        }
        // On a symbol the spec table does not hold: nothing, counted (decision 0147 item 2).
        let (out, codec) = decoded(&cfg, &order_frame(&[("symbol", Some(r#""NVDA-USD""#))]));
        assert_eq!(out, Out::quiet());
        assert_eq!(codec.counts().unheld, 1);
    }
}

#[test]
fn a_fill_response_decodes_to_a_fill_keyed_by_its_trade_id_its_fee_a_cost_through_the_scope() {
    for cfg in [preprod("primary"), production(SUB)] {
        let (out, _) = decoded(&cfg, &fill_frame(&[]));
        let want = fill(theirs(), 2_000_000);
        assert_eq!(out, Out::events(vec![ExecEvent::Fill(want.clone())]));
        let trade = dispatch_market_data(&caps(), |scope| scope.fill_id(TRADE_ID)).unwrap();
        assert_eq!(want.key(), FillKey::Venue(trade));
        assert_eq!(want.fee.cost(), Money::new(2_000_000, usd()));
        // A negative fee is a rebate, never negated (decision 0004); a fee exact to the nano
        // in an exponent is read; our client id is ours, and none is none.
        for (changes, want) in [
            (vec![("fee", Some("-0.001"))], fill(theirs(), -1_000_000)),
            (vec![("fee", Some("2e-3"))], fill(theirs(), 2_000_000)),
            (vec![("fee", Some("0"))], fill(theirs(), 0)),
            (
                vec![("client_order_id", Some(q(&wire_cid(1)).as_str()))],
                fill(Some(CidMatch::Ours(cids()[1])), 2_000_000),
            ),
            (
                vec![("client_order_id", Some(r#""""#))],
                fill(None, 2_000_000),
            ),
        ] {
            let frame = fill_frame(&changes);
            assert_eq!(
                decoded(&cfg, &frame).0,
                Out::events(vec![ExecEvent::Fill(want)]),
                "{frame}"
            );
        }
        let rebate = fill(theirs(), -1_000_000);
        assert_eq!(rebate.fee.cost().nanos, -1_000_000);
        // Refused, never rounded: a fee finer than a nano, a price off the grid, a size off the
        // step; and every other shape the pages do not give.
        for change in [
            ("fee", Some("1e-10")),
            ("fee", Some("0.0000000001")),
            ("fee", Some(r#""0.002""#)),
            ("fee", None),
            ("price", Some("200.005")),
            ("price", Some("-200.0")),
            ("quantity", Some("1.005")),
            ("quantity", Some("0")),
            ("timestamp", None),
            ("timestamp", Some("-1")),
            ("trade_id", Some(r#""t-1""#)),
            ("order_id", Some(r#""bad""#)),
            ("side", Some(r#""buy""#)),
            ("client_order_id", Some("[]")),
        ] {
            let frame = fill_frame(&[change]);
            let (out, mut codec) = decoded(&cfg, &frame);
            assert_eq!(
                out,
                Out {
                    result: Err(invalid()),
                    ..Out::quiet()
                },
                "{frame}"
            );
            let sent = encode(&mut codec, &cancel(ORDER), 1);
            assert!(sent.result.is_ok(), "{frame}");
        }
        // On a symbol the spec table does not hold: nothing, counted.
        let (out, codec) = decoded(&cfg, &fill_frame(&[("symbol", Some(r#""NVDA-USD""#))]));
        assert_eq!(out, Out::quiet());
        assert_eq!(codec.counts().unheld, 1);
    }
}

#[test]
fn a_position_response_is_the_accounts_position_and_an_unheld_symbol_pushes_nothing() {
    let at = |price: i64| Some(PxExact::new(price, 0));
    for cfg in [preprod("primary"), production(SUB)] {
        for (changes, want) in [
            (vec![], position(AAPL, 100, at(200))),
            (
                vec![("position", Some("-0.5"))],
                position(AAPL, -50, at(200)),
            ),
            (vec![("position", Some("0"))], position(AAPL, 0, None)),
            (vec![("position", Some("-0.0"))], position(AAPL, 0, None)),
            (
                vec![("average_price", Some("0"))],
                position(AAPL, 100, None),
            ),
            (vec![("average_price", None)], position(AAPL, 100, None)),
            (
                vec![("average_price", Some("200.125"))],
                position(AAPL, 100, Some(PxExact::new(200_125, -3))),
            ),
        ] {
            let frame = position_frame(&changes);
            assert_eq!(decoded(&cfg, &frame).0, Out::events(vec![want]), "{frame}");
        }
        for change in [
            ("position", Some("0.005")),
            ("position", None),
            ("position", Some(r#""1""#)),
            ("average_price", Some("-1")),
            ("average_price", Some("null")),
            ("average_price", Some("100000000000000000000")),
            ("symbol", None),
        ] {
            let frame = position_frame(&[change]);
            let (out, mut codec) = decoded(&cfg, &frame);
            assert_eq!(
                out,
                Out {
                    result: Err(invalid()),
                    ..Out::quiet()
                },
                "{frame}"
            );
            assert!(encode(&mut codec, &cancel(ORDER), 1).result.is_ok());
        }
    }
    // FBC-291a's eleven captured positions: flat on the placeholder symbol when a spec holds
    // it, and nothing, counted, when none does (stage one's empty table).
    let (_, window) = captured("primary");
    let positions = &window[..window.len() - 1];
    assert_eq!(positions.len(), 11);
    let placeholder = InstrumentId::new(8);
    let mut held = specs();
    held.insert(spec_for("22222222", placeholder));
    for cfg in [production("primary"), preprod("primary")] {
        let mut codec = probe_on(&cfg);
        authenticate(&mut codec, 1);
        for (name, frame) in positions {
            assert_eq!(
                text_with(&mut codec, frame, &held),
                Out::events(vec![position(placeholder, 0, None)]),
                "{name}"
            );
        }
        let mut codec = probe_on(&cfg);
        authenticate(&mut codec, 1);
        for (name, frame) in positions {
            assert_eq!(
                text_with(&mut codec, frame, &SpecTable::new()),
                Out::quiet(),
                "{name}"
            );
        }
        assert_eq!(codec.counts().unheld, 11);
    }
}

#[test]
fn a_page_no_read_asked_for_pushes_nothing_and_every_place_still_ends_unknown() {
    for cfg in [preprod("primary"), production(SUB)] {
        let mut codec = probe_on(&cfg);
        authenticate(&mut codec, 1);
        for frame in [
            fixture("all_orders_response.json"),
            r#"{"user_trades_response":[]}"#.to_owned(),
            r#"{"user_trades":[],"count":0}"#.to_owned(),
        ] {
            assert_eq!(text(&mut codec, &frame), Out::quiet(), "{frame}");
        }
        assert_eq!(codec.counts().pages_unasked, 3);
        // A page not as documented is a decode error, the epoch kept open.
        let out = text(&mut codec, r#"{"all_orders_response":{"orders":[]}}"#);
        assert_eq!(
            out,
            Out {
                result: Err(invalid()),
                ..Out::quiet()
            }
        );
        assert_eq!(codec.counts().pages_unasked, 3);
    }

    // Under preprod_probe a place is sent, and nothing it answers settles it: its ACK, a
    // FILLED and a fill naming our client id are an order's updates and a fill, and the place
    // ends Unknown at its deadline (decision 0132 item 8).
    let ours = q(&wire_cid(0));
    let ours_order = |more: &[(&str, Option<&str>)]| {
        let mut changes = vec![
            ("client_order_id", Some(ours.as_str())),
            ("price", Some("200.5")),
        ];
        changes.extend_from_slice(more);
        order_frame(&changes)
    };
    let ours_fill = fill_frame(&[
        ("client_order_id", Some(ours.as_str())),
        ("price", Some("200.5")),
        ("side", Some(r#""BUY""#)),
    ]);
    let mut steps = epoch(1);
    steps.push(Step::Encode(VenueCommand::Place(gtc()), 1));
    steps.push(Step::Frame(ours_order(&[])));
    steps.push(Step::Frame(ours_order(&[
        ("status", Some(r#""FILLED""#)),
        ("quantity_remaining", Some("0")),
    ])));
    steps.push(Step::Frame(ours_fill));
    steps.push(Step::Deadline(1));
    let (_, outs, disk) = run_twice(|| probe_on(&preprod("primary")), &steps);
    let ours_update = |state, cum| {
        ExecEvent::Order(OrderUpdate {
            cid: Some(CidMatch::Ours(cids()[0])),
            vid: Some(venue_id(ORDER)),
            inst: AAPL,
            side: Side::Buy,
            state,
            cum_filled: Lots::new(cum).unwrap(),
            px: Some(Ticks(20_050)),
            qty: Some(Lots::new(100).unwrap()),
            post_only: None,
            reduce_only: None,
        })
    };
    let n = outs.len();
    assert!(outs[n - 5].result.is_ok() && outs[n - 5].effects.len() == 1);
    assert_eq!(
        outs[n - 4],
        Out::events(vec![ours_update(VenueOrderState::Open, 0)])
    );
    assert_eq!(
        outs[n - 3],
        Out::events(vec![ours_update(VenueOrderState::Filled, 100)])
    );
    let ExecEvent::Fill(ours_fill) = &outs[n - 2].events[0].1 else {
        panic!("{:?}", outs[n - 2])
    };
    assert_eq!(
        (ours_fill.cid(), ours_fill.side, outs[n - 2].events.len()),
        (Some(CidMatch::Ours(cids()[0])), Side::Buy, 1)
    );
    only_outcomes(&outs, &[(n - 1, unknown(1))]);
    // The journal keeps no user_id, and replay gave the same events (run_twice).
    assert!(!disk.has(USER));
    // Under production_probe the place is not sent (decision 0145).
    let mut codec = probe_on(&production("primary"));
    authenticate(&mut codec, 1);
    assert_eq!(
        encode(&mut codec, &VenueCommand::Place(gtc()), 1),
        refused(fbc_core::NotSentReason::Unsupported)
    );
}

#[test]
fn outside_either_probe_mode_every_private_state_frame_still_ends_the_epoch() {
    let frames = [
        order_frame(&[]),
        order_frame(&[("status", Some(r#""CANCELLED""#))]),
        fill_frame(&[]),
        position_frame(&[]),
        fixture("all_orders_response.json"),
    ];
    for frame in &frames {
        let mut off = recording(&preprod("primary"));
        authenticate(&mut off, 1);
        assert!(text(&mut off, frame).reconnects(), "{frame}");
        assert!(off.last_private().is_some(), "{frame}");
        assert_eq!(off.counts().unheld, 0);
        for cfg in [preprod("primary"), production(SUB)] {
            let (out, codec) = decoded(&cfg, frame);
            assert!(
                out.result.is_ok() && !out.asks_to_reconnect(),
                "{frame}: {out:?}"
            );
            assert!(codec.last_private().is_none());
        }
    }
}

// ----- One session's journal, replayed (decision 0132 item 3, 0134) -----

#[test]
fn the_journal_keeps_what_0132_item_3_and_0134_keep_and_replays_to_the_same_events_and_effects() {
    // One session's script, the arm's answers, a cancel's settlement and the private frames
    // together, live and replayed (run_twice): each step's events and effects alike both ways.
    let ours = q(&wire_cid(0));
    let other_echo = r#"{"type":"get_order","params":{"order_id":"2f05173c-992a-426b-bcaf-6b341644bb62","symbol":"AAPL-USD"}}"#;
    let hinted = flat("RateLimited", HINT_S, ARM);
    let other = nested("InvalidParameter", r#""cannot arm""#, other_echo);
    let bad_id = nested("InvalidOrderId", r#""bad order id""#, CANCEL_SENT);
    let escaped = flat("InvalidParameter", r#""cannot\u0020arm""#, other_echo);
    let ours_order = order_frame(&[
        ("client_order_id", Some(ours.as_str())),
        ("price", Some("200.5")),
    ]);
    let ours_fill = fill_frame(&[
        ("client_order_id", Some(ours.as_str())),
        ("price", Some("200.5")),
        ("side", Some(r#""BUY""#)),
    ]);
    let rejection = nested("InvalidParameter", r#""cannot arm""#, ARM);

    let mut steps = armed(1, 1);
    let at = |steps: &mut Vec<Step>, step: Step| {
        steps.push(step);
        steps.len() - 1
    };
    // The arm's window: a RateLimited err echoing it holds for its hint and leaves it to its
    // deadline, so its ack then answers nothing, and an err echoing another request answers
    // nothing.
    let hold = at(&mut steps, Step::Frame(hinted.clone()));
    let in_window = at(&mut steps, Step::Frame(other));
    let late_ack = at(&mut steps, Step::Frame(ack(ARM)));
    let arm_deadline = at(&mut steps, Step::Deadline(1));
    // A cancel: an InvalidOrderId err echoing it keeps the epoch open, and CANCELLED settles it.
    let cancel_1 = at(&mut steps, Step::Encode(cancel(ORDER), 2));
    let invalid_id = at(&mut steps, Step::Frame(bad_id));
    let settle = at(&mut steps, Step::Frame(cancelled()));
    // A place, its order update, its fill, the account's position, and its deadline.
    let place = at(&mut steps, Step::Encode(VenueCommand::Place(gtc()), 3));
    let acked = at(&mut steps, Step::Frame(ours_order));
    let filled = at(&mut steps, Step::Frame(ours_fill));
    let held_position = at(&mut steps, Step::Frame(position_frame(&[])));
    let place_deadline = at(&mut steps, Step::Deadline(3));
    // An err with an escape once the arm is answered: kept whole, so the epoch ends.
    let whole = at(&mut steps, Step::Frame(escaped.clone()));
    // Epoch 2: an err echoing its arm rejects it, and NO_SUCH_ORDER rejects its cancel.
    for step in armed(2, 4) {
        at(&mut steps, step);
    }
    let reject = at(&mut steps, Step::Frame(rejection));
    let cancel_2 = at(&mut steps, Step::Encode(cancel(ORDER), 5));
    let not_there = at(&mut steps, Step::Frame(no_such(ORDER)));
    // Epoch 3: its arm's ack accepts it.
    for step in armed(3, 6) {
        at(&mut steps, step);
    }
    let accept = at(&mut steps, Step::Frame(ack(ARM)));

    for cfg in [preprod("primary"), preprod(SUB), loopback("primary")] {
        let (codec, outs, disk) = run_twice(|| probe_on(&cfg), &steps);
        // The same events and effects at each step, live and replayed (run_twice), these among
        // them: the hint held at its step and the arm Unknown at its deadline, the cancel
        // accepted and rejected, the place Unknown, epoch 2's arm rejected with its documented
        // code alone, and epoch 3's accepted.
        assert_eq!(outs[hold], held(Some(Duration::from_secs(3))));
        assert_eq!(outs[in_window], Out::quiet());
        assert_eq!(outs[late_ack], Out::quiet());
        assert_eq!(outs[arm_deadline], Out::events(vec![unknown(1)]));
        assert_eq!(outs[accept], Out::events(vec![accepted(6)]));
        assert!(outs[cancel_1].result.is_ok() && outs[cancel_1].effects.len() == 1);
        assert_eq!(outs[invalid_id], Out::quiet());
        assert_eq!(
            outs[settle],
            Out::events(vec![settled(2, final_ack()), cancelled_update()])
        );
        assert!(outs[place].result.is_ok() && outs[place].effects.len() == 1);
        assert!(
            matches!(
                outs[acked].events.as_slice(),
                [(_, ExecEvent::Order(update))] if update.cid == Some(CidMatch::Ours(cids()[0]))
            ),
            "{:?}",
            outs[acked]
        );
        assert!(
            matches!(
                outs[filled].events.as_slice(),
                [(_, ExecEvent::Fill(fill))] if fill.side == Side::Buy
            ),
            "{:?}",
            outs[filled]
        );
        assert_eq!(
            outs[held_position],
            Out::events(vec![position(
                AAPL,
                100,
                Some(PxExact::from_decimal("200.0".parse().unwrap()).unwrap())
            )])
        );
        assert_eq!(outs[place_deadline], Out::events(vec![unknown(3)]));
        assert!(outs[whole].asks_to_reconnect(), "{:?}", outs[whole]);
        assert_eq!(
            outs[reject],
            Out::events(vec![rejected(4, Some("InvalidParameter"))])
        );
        assert!(outs[cancel_2].result.is_ok());
        assert_eq!(outs[not_there], Out::events(vec![settled(5, not_found())]));
        only_outcomes(
            &outs,
            &[
                (arm_deadline, unknown(1)),
                (settle, settled(2, final_ack())),
                (place_deadline, unknown(3)),
                (reject, rejected(4, Some("InvalidParameter"))),
                (not_there, settled(5, not_found())),
                (accept, accepted(6)),
            ],
        );
        let counts = codec.counts();
        assert_eq!((counts.arm_accepted, counts.arm_rejected), (1, 1));
        assert_eq!((counts.cancels_accepted, counts.cancels_not_found), (1, 1));

        // On disk: the arm's ack and the hint form in clear; the err in the window as its
        // shape, its message and its echo's values spanned; the cancel's err as 0134 keeps
        // it, the order id it echoes spanned; the escaped err after the answer whole; epoch
        // 2's rejection with its code and the arm in clear, its message spanned.
        let frame = |step: usize| {
            let n = steps[..=step]
                .iter()
                .filter(|s| matches!(s, Step::Frame(_)))
                .count();
            disk.frames[n - 1].clone()
        };
        assert_eq!(frame(hold), hinted);
        assert_eq!(frame(late_ack), ack(ARM));
        assert_eq!(frame(accept), ack(ARM));
        let kept = frame(in_window);
        assert!(
            kept.contains(r#""error_code":"InvalidParameter""#),
            "{kept}"
        );
        assert!(
            !kept.contains("cannot arm") && !kept.contains(ORDER),
            "{kept}"
        );
        let kept = frame(invalid_id);
        assert!(kept.contains(r#""error_code":"InvalidOrderId""#), "{kept}");
        assert!(
            !kept.contains("bad order id") && !kept.contains(ORDER),
            "{kept}"
        );
        assert_eq!(frame(whole), twos(escaped.len()));
        let kept = frame(reject);
        assert!(
            kept.contains(r#""error_code":"InvalidParameter""#),
            "{kept}"
        );
        assert!(kept.contains(ARM), "{kept}");
        assert!(!kept.contains("cannot arm"), "{kept}");
        for absent in ["cannot arm", "bad order id", PUBLIC, SUB, SECRET, USER] {
            assert!(!disk.has(absent), "{absent} on disk");
        }
    }
}
