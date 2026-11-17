//! Stage two's taker IOC (FBC-ou9g, a slice of FBC-9sk2; decision 0132 item 7, 0148 items 5, 6
//! and 10, 0151 items 5, 8, 9 and 10): its pure parts, its record and the report's items the
//! taker brings. The driver is `stage.rs`'s taker session ([`crate::places::TAKE`]), on each
//! named account whose drop test passed in the run, right after that account's crossing session,
//! with the run's exact-echo gate open.
//!
//! - **What follows the IOC** ([`after_ioc`]): one left open with a fill, as the adapter decodes
//!   QFEX's `IOC_PARTIALLY_FILLED` with a remainder (0151 item 8, until FBC-jdj7), stops the run
//!   not flat with no flatten; so does one whose fills the venue's count states but which had
//!   not all reached the registry by its deadline, since the flatten judges the registry's
//!   position and that is not the venue's; one that leaves the registry's position off flat is
//!   flattened through Exit while its session is open ([`crate::flatten`]); one that expired
//!   unfilled is recorded and flattens nothing. The taker's fill is expected (0151 item 9) and
//!   stops nothing once flat.
//! - **The taker fee's sign** ([`FeeSign`]; decision 0148 item 10): paid, zero or received, read
//!   from fbc-core's `Fee` as the adapter decodes it, a cost positive when we paid it (decision
//!   0004), never its amount.
//! - **The order of the IOC's final `order_response` and its `fill_response`** ([`FrameOrder`]),
//!   for FBC-tfxk: from the frames the tee heard, never a frame.
//! - **The run's declarations and the owner's part** ([`markdown`], [`to_json`]; decision 0148
//!   item 10, 0151 item 10): the key the place stages' sessions carry, the `CodProbeRun`
//!   declarations, the fences met (never a value), the taker fee's sign per account,
//!   `--sole-trader` and the owner's confirmations as unchecked, and every hand action: the
//!   `--reconciled` declaration, and the hand cancel or hand close the run leaves the owner.

use std::fmt::Write as _;

use fbc_core::{Fee, SignedLots};
use fbc_venue_qfex::probe::{KEY_PRODUCTION_PLACES, PROBE_PLACE_STAGES};
use serde_json::{Value, json};

use crate::capture::{AccountRead, Evidence};
use crate::flatten::Ioc;
use crate::places::Stop;
use crate::tee::{Cid, Heard};

/// The taker fee's sign (decision 0148 item 10): never its amount.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FeeSign {
    /// A cost: positive as fbc-core holds a fee (decision 0004).
    Paid,
    Zero,
    /// A rebate: negative as fbc-core holds a fee.
    Received,
}

impl FeeSign {
    /// The sign of `fee` as the adapter decoded it, a cost positive when we paid it (decision
    /// 0004): its sign alone is read, never its amount, and it is neither negated nor made
    /// absolute.
    pub fn of(fee: &Fee) -> FeeSign {
        match fee.cost().nanos.signum() {
            1 => FeeSign::Paid,
            0 => FeeSign::Zero,
            _ => FeeSign::Received,
        }
    }

    /// As the report says it.
    pub fn word(self) -> &'static str {
        match self {
            FeeSign::Paid => "paid",
            FeeSign::Zero => "zero",
            FeeSign::Received => "received",
        }
    }
}

/// What the report says of the taker fee's sign, from `fees`, the sign of each fill of the taker
/// IOC in the order they came: each word once, in that order, or not measured when none came.
pub fn fee_said(fees: &[FeeSign]) -> String {
    let mut words: Vec<&'static str> = Vec::new();
    for word in fees.iter().map(|f| f.word()) {
        if !words.contains(&word) {
            words.push(word);
        }
    }
    match words.is_empty() {
        true => "not measured: the taker IOC brought no fill".to_owned(),
        false => words.join(" and "),
    }
}

/// The order of the taker IOC's final `order_response` and its `fill_response`s, as the tee
/// heard them on its session (FBC-tfxk).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrameOrder {
    /// Every `fill_response` came before the final `order_response`.
    FillFirst,
    /// A `fill_response` came after the final `order_response`.
    OrderFirst,
    /// No `fill_response` named it: not measured.
    NoFill,
    /// A `fill_response` named it, but no `order_response` did: not measured.
    NoOrder,
}

impl FrameOrder {
    /// The order on `heard`, the frames the tee heard from the taker session's first on, of the
    /// frames naming `sent`, the taker IOC's `add_order` among those the run sent, by their client
    /// id ([`Cid::sent`]): the last `order_response` naming it against the last `fill_response`
    /// naming it.
    pub fn of(heard: &[Heard], sent: usize) -> FrameOrder {
        let names = |cid: &Cid| cid.sent() == Some(sent);
        let order = heard
            .iter()
            .rposition(|h| matches!(h, Heard::Order { cid, .. } if names(cid)));
        let fill = heard
            .iter()
            .rposition(|h| matches!(h, Heard::Fill { cid, .. } if names(cid)));
        match (order, fill) {
            (_, None) => FrameOrder::NoFill,
            (None, Some(_)) => FrameOrder::NoOrder,
            (Some(order), Some(fill)) if fill < order => FrameOrder::FillFirst,
            (Some(_), Some(_)) => FrameOrder::OrderFirst,
        }
    }

    /// As the report says it.
    pub fn says(self) -> &'static str {
        match self {
            FrameOrder::FillFirst => {
                "fill_response first: every fill_response came before the final order_response"
            }
            FrameOrder::OrderFirst => {
                "order_response first: a fill_response came after the final order_response, the \
                 order FBC-tfxk asks about"
            }
            FrameOrder::NoFill => "not measured: no fill_response named the taker IOC",
            FrameOrder::NoOrder => {
                "not measured: a fill_response named the taker IOC, but no order_response did"
            }
        }
    }
}

/// What follows the taker IOC once its wait ended ([`after_ioc`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum After {
    /// Nothing: it left the registry's position flat, expired unfilled among those ways.
    Nothing,
    /// The flatten, while its session is open: the owner's Flatten into Exit, then the flatten
    /// IOCs (decision 0148 item 5).
    Flatten,
    /// The run stops not flat, with no flatten: left open with a fill (0151 item 8, until
    /// FBC-jdj7).
    Remainder,
    /// The run stops not flat, with no flatten: the venue's count states fills that had not all
    /// reached the registry by the IOC's deadline, so the registry's position, which a flatten
    /// judges, is not the venue's.
    Unsettled,
}

impl After {
    /// The stop it raises: [`Stop::IocRemainder`] or [`Stop::IocUnsettled`], none otherwise.
    pub fn stop(self) -> Option<Stop> {
        match self {
            After::Remainder => Some(Stop::IocRemainder),
            After::Unsettled => Some(Stop::IocUnsettled),
            After::Nothing | After::Flatten => None,
        }
    }
}

/// What follows the taker IOC, which came back `ioc`, the registry's position on the market then
/// `position`, `unsettled` whether the venue's count of its fills is above the registry's: one
/// left open with a fill stops the run not flat ([`After::Remainder`]); one whose fills had not
/// all reached the registry stops it not flat too ([`After::Unsettled`]), whatever the
/// registry's position, never [`After::Nothing`]; one that left the position off flat is
/// flattened; any other flattens nothing.
pub fn after_ioc(ioc: Ioc, position: SignedLots, unsettled: bool) -> After {
    match (ioc, position.0, unsettled) {
        (Ioc::PartOpen, ..) => After::Remainder,
        (Ioc::Unsettled, ..) | (_, _, true) => After::Unsettled,
        (_, 0, false) => After::Nothing,
        _ => After::Flatten,
    }
}

/// The taker IOC on one account, as the report and the evidence show it: status words alone,
/// never an id, a price, a size or an amount.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Taken {
    /// How it came back by the end of its wait.
    pub ioc: Option<Ioc>,
    /// The sign of each of its fills' fees, in the order they came.
    pub fees: Vec<FeeSign>,
    /// The order of its final `order_response` and its `fill_response`s.
    pub frame_order: Option<FrameOrder>,
}

/// What a flatten answers once the taker IOC left the registry's position off flat, as its
/// record says it.
pub const AFTER_TAKER: &str = "the taker IOC's fill (decision 0132 item 7), expected, so it stops \
     nothing once flat (0151 item 9)";

/// What a flatten answers once a fill of the taker IOC reached its session only after its steps
/// ended, during its Stop's cancels or after them, and left the registry's position off flat.
pub const AFTER_LATE_TAKER: &str = "the taker IOC's fill (decision 0132 item 7), expected, that \
     reached the session after its steps ended, during its Stop's cancels or after them (0151 \
     item 9): it stops nothing once flat; the Stop's Kill lifted first, so the owner's Flatten \
     could put the market in Exit, and the market Killed again after, then disarmed as the Stop \
     disarms it";

/// What the report says follows a session the run ended after, its own stop aside: the run's
/// stop or an error that is no stop, met after it, before any later session.
pub const RUN_ENDED: &str = "no later session: the run ended first, as the report's opening says \
     (a stop, or an error that is no stop); nothing more is placed on this account or any other";

/// What the report says follows a crossing session that did not stop the run: its account's
/// taker IOC session.
pub const THEN_TAKE: &str = "this account's taker IOC session, which starts only after every \
     named account reads clean again and no registry of the run holds an order of ours (decision \
     0148 item 6)";

/// What follows a taker IOC session that did not stop the run, on an account before the last.
pub const NEXT_ACCOUNT: &str = "the next named account's crossing session, which starts only \
     after every named account reads clean again and no registry of the run holds an order of \
     ours (decision 0148 item 6)";

/// What follows the last named account's taker IOC session that did not stop the run: the run's
/// end, until FBC-4plj builds stage three.
pub const LAST_TAKE: &str = "the run's end: stage three is not built yet (FBC-4plj builds it)";

/// What the report says of an order resting after a taker IOC session, for the owner (decision
/// 0132 item 7, on production's site as decision 0143 item 4 reads it).
pub const HAND_CANCEL_TAKE: &str = "an order rests on the account after the taker IOC session: \
     cancel it by hand on QFEX production's site, then give the next place-stage run \
     --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What the report says when the reads after a taker IOC session list an order or a TWAP not
/// ours (decision 0148 item 8; FBC-77r8's review, carried).
pub const OTHERS_TAKE: &str = "an order or a TWAP that is not ours is listed on a named account \
     after the taker IOC session, which the streams did not show: a stop, ReadDisagrees \
     (decision 0148 item 8); read the accounts on QFEX's site before the next run";

/// What the report says of a taker IOC left open with a fill (0151 item 8).
pub const REMAINDER: &str = "left open with a fill, as the adapter decodes QFEX's \
     IOC_PARTIALLY_FILLED with a remainder: the run stops not flat with no flatten, the position \
     the owner's (0151 item 8, until FBC-jdj7)";

/// What the report says of a taker IOC whose fills had not all reached the registry by its
/// deadline.
pub const UNSETTLED: &str = "ended, but not every fill its order events state reached the \
     registry by its deadline: the run stops not flat with no flatten, the position the owner's, \
     since the registry's position is not the venue's (IocUnsettled)";

/// How the taker IOC came back, as the report says it.
pub fn came_back(taken: Option<&Taken>) -> String {
    match taken.and_then(|t| t.ioc) {
        None => "not measured: it was not placed".to_owned(),
        Some(Ioc::PartOpen) => REMAINDER.to_owned(),
        Some(Ioc::Unsettled) => UNSETTLED.to_owned(),
        Some(Ioc::Expired) => "expired unfilled: recorded, and nothing flattened".to_owned(),
        Some(ioc) => ioc.says().to_owned(),
    }
}

/// The report's lines on the taker IOC itself, under its session's section.
pub fn session_lines(taken: Option<&Taken>) -> String {
    let fees = taken.map_or("not measured: it was not placed".to_owned(), |t| {
        fee_said(&t.fees)
    });
    let order = (taken.and_then(|t| t.frame_order)).map_or("not measured", FrameOrder::says);
    format!(
        "- How the taker IOC came back: {}\n\
         - The taker fee's sign (decision 0148 item 10; no amount): {fees}\n\
         - The order of its final order_response and its fill_response (FBC-tfxk): {order}\n",
        came_back(taken)
    )
}

/// The evidence's entry for the taker IOC.
pub fn taken_json(taken: Option<&Taken>) -> Value {
    json!({"came_back": came_back(taken),
        "fee_signs": taken.map(|t| t.fees.iter().map(|f| f.word()).collect::<Vec<_>>()),
        "fee_sign": taken.map(|t| fee_said(&t.fees)),
        "frame_order": taken.and_then(|t| t.frame_order).map(FrameOrder::says)})
}

/// The hand actions the run leaves the owner (decision 0148 item 10), from `reads`, every read
/// the run made, the last read of each of `named` judging it: a hand cancel when one listed an
/// order of ours, a hand close when one showed a position off flat. `(cancel, close)`.
pub fn hand_actions(reads: &[AccountRead], named: &[&str]) -> (bool, bool) {
    let last = |name: &str| {
        let of = |r: &&AccountRead| r.label == name || r.label.starts_with(&format!("{name} ("));
        reads.iter().rev().find(of)
    };
    let lasts: Vec<&AccountRead> = named.iter().filter_map(|n| last(n)).collect();
    let cancel = (lasts.iter()).any(|r| r.page_orders > 0 || r.rest_open_orders > 0);
    let close = lasts.iter().any(|r| !r.flat);
    (cancel, close)
}

/// Whether what the run leaves the owner by hand is not known for a named account of `named`:
/// one `reads` holds no read of, or one of `last_unread`, whose last read could not be made
/// (FBC-77r8's review, carried: a report line comes from what the reads showed, never from the
/// run's shape).
pub fn hand_unknown(reads: &[AccountRead], named: &[&str], last_unread: &[&str]) -> bool {
    let read = |name: &str| {
        let of = |r: &AccountRead| r.label == name || r.label.starts_with(&format!("{name} ("));
        reads.iter().any(of)
    };
    (named.iter()).any(|n| !read(n) || last_unread.contains(n))
}

/// What the report says of the hand actions when a named account was not read, or its last read
/// could not be made ([`hand_unknown`]).
pub const HAND_NOT_KNOWN: &str = "not known: a named account was not read, or its last read \
     could not be made (the errors below); read every named account on QFEX's site before the \
     next run, and give it --reconciled for any hand cancel or hand close made";

/// The fences the run met, as the report says them (0151 item 10): never a value.
pub const FENCES: &str = "the caps set, positive and each admitting three of the instrument's \
     minimum size at the touch (--resting-cap-usd and --inventory-cap-usd); the distance from the \
     touch at least 20 bps (--away-bps); the flatten cap below the distance (--flatten-cap-bps); \
     the flatten attempts 1 to 3 (--flatten-attempts)";

/// What the report says is not checked by the probe (decisions 0144 and 0148 items 7 and 10).
pub const UNCHECKED: &str = "--sole-trader, the owner's declaration that no other process, Java \
     or Rust, trades any named account (decision 0148 item 7), and the owner's confirmations \
     before the run that the user has no subaccount the configuration does not name (decision \
     0144) and that nothing was traded by hand from the first read to the last: each recorded as \
     given and unchecked; the probe cannot check them";

/// The key the place stages' sessions carry, as the report says it.
pub fn key() -> String {
    format!(
        "`{KEY_PRODUCTION_PLACES} = {PROBE_PLACE_STAGES}`, on every place-stage session's \
         configuration and on no read's (decision 0148 item 4)"
    )
}

/// The `CodProbeRun` declarations the run's registries carry, each `<step> on <account>:
/// <declaration>`, none a testnet run.
fn declarations(ev: &Evidence) -> Vec<String> {
    (ev.place_registries.iter())
        .map(|run| {
            format!(
                "{} on {}: {}",
                run.step.name(),
                run.account(),
                run.declaration
            )
        })
        .collect()
}

/// The taker fee's sign on each account whose taker IOC session ran, `<account>: <sign>`.
fn fee_signs(ev: &Evidence) -> Vec<String> {
    let takes = ev.stage_two.iter().flat_map(|t| t.takes.iter());
    let signs = takes.map(|run| format!("{}: {}", run.account, came_fee(run)));
    signs.collect()
}

/// The fee's sign of one taker session's IOC.
fn came_fee(run: &crate::stage_two::CrossRun) -> String {
    match &run.taken {
        Some(taken) if taken.ioc.is_some() => fee_said(&taken.fees),
        _ => "not measured: the taker IOC was not placed".to_owned(),
    }
}

/// The hand actions, as the report says them: the `--reconciled` declaration, then what the
/// run's last reads leave the owner.
fn hands(ev: &Evidence) -> (String, String) {
    let reconciled = ev.marker.as_ref().and_then(|m| m.reconciled);
    let declared = match reconciled {
        Some(said) => format!(
            "`--reconciled {said}`, the owner's declaration of what was done by hand after an \
             earlier run's stop (0151 item 6): recorded as given and unchecked"
        ),
        None => "no `--reconciled` declaration was given".to_owned(),
    };
    let (cancel, close) = hand_actions(&ev.reads, &ev.accounts);
    let unknown = hand_unknown(&ev.reads, &ev.accounts, &ev.last_unread);
    let left = match (cancel, close) {
        (false, false) if unknown => HAND_NOT_KNOWN.to_owned(),
        (false, false) => "none: the last read of every named account showed no order of ours \
                           and a flat position"
            .to_owned(),
        (cancel, close) => {
            let mut asked = Vec::new();
            if cancel {
                asked.push(
                    "a hand cancel on QFEX production's site, the last read of a named account \
                     listing an order",
                );
            }
            if close {
                asked.push(
                    "a hand close of the position, the last read of a named account showing it \
                     off flat (decision 0148 item 9)",
                );
            }
            if unknown {
                asked.push(
                    "the rest not known: a named account was not read, or its last read could \
                     not be made (the errors below)",
                );
            }
            format!(
                "{}; the next place-stage run then needs --reconciled",
                asked.join("; and ")
            )
        }
    };
    (declared, left)
}

/// The report's section on what the run declares and the owner's part (decision 0148 item 10,
/// 0151 item 10): never an id, a value or an amount.
pub fn markdown(s: &mut String, ev: &Evidence) {
    let list = |items: Vec<String>, none: &str| match items.is_empty() {
        true => none.to_owned(),
        false => items.join("; "),
    };
    let declared = list(declarations(ev), "none: no place-stage registry was built");
    let fees = list(fee_signs(ev), "not measured: no taker IOC session ran");
    let (reconciled, left) = hands(ev);
    let _ = writeln!(
        s,
        "## What the run declares, and the owner's part (decision 0148 item 10, 0151 item \
         10)\n\n\
         - The key: {}\n\
         - The CodProbeRun declarations, each on its registry's one market, none a testnet run \
           (decision 0148 item 3): {declared}\n\
         - The fences met, never their values: {FENCES}\n\
         - The taker fee's sign, by account (no amount): {fees}\n\
         - Not checked by the probe: {UNCHECKED}\n\
         - The hand actions declared: {reconciled}\n\
         - The hand actions this run leaves the owner: {left}\n",
        key()
    );
}

/// The evidence's entry for what the run declares and the owner's part.
pub fn to_json(ev: &Evidence) -> Value {
    let (reconciled, left) = hands(ev);
    let (cancel, close) = hand_actions(&ev.reads, &ev.accounts);
    let known = !hand_unknown(&ev.reads, &ev.accounts, &ev.last_unread);
    json!({"key": key(), "declarations": declarations(ev), "fences_met": FENCES,
        "taker_fee_signs": fee_signs(ev), "unchecked": UNCHECKED,
        "sole_trader_checked": false, "confirmations_checked": false,
        "hand_actions_declared": reconciled,
        "hand_actions_left": {"said": left, "hand_cancel": cancel, "hand_close": close,
            "known": known}})
}
