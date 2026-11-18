//! Stage two's parts (FBC-su9i, a slice of FBC-9sk2; decisions 0005, 0132 item 7, 0148 items 2
//! and 6, and 0151 items 5 and 13): what stage two's crossing session reads before it places,
//! and what the report says of the run's client ids and statuses, each a pure function of what
//! the run kept in memory.
//!
//! - **The exact-echo gate** ([`EchoGate::of`]; decision 0148 item 2): open once the run's tee
//!   heard an `order_response` whose status is `ACK` and whose `client_order_id`, its raw bytes
//!   between the quotes as the frame arrived, is exactly a client id this run sent
//!   (`tee::Kept::exact_ack`). fbc-core's `CidMatch` cannot judge it, since `decode_cid` reads a
//!   UUID in either case (`crates/fbc-core/src/cid.rs`). An upper-cased, escaped, absent, null or
//!   empty client id keeps it shut, and an `ack` or `err` echo of an `add_order` is recorded with
//!   how exact it was ([`Echoes`]) but never opens it. It is per run: the tee is the run's own,
//!   kept in memory, and no later run reads it.
//! - **No other order of ours** ([`no_other_order`]; decision 0148 item 6, 0151 item 5): before
//!   a crossing order, every registry of every named account the run built holds no order on the
//!   market open or `Unknown` (`Registry::resting_on`, both sides zero), but for X and Y on a
//!   drop registry once the reads after its drop showed each ended ([`ended_after_drop`]), and
//!   each named account's latest never-arming read lists no open order. An order left `Unknown`
//!   is never resent (decision 0005) and stays counted resting, so it blocks every crossing order
//!   after it.
//! - **The report's items** (decision 0132's Consequences, 0148 item 10): the characters the
//!   run's client ids exercised, by character class, the rest named as not measured
//!   ([`Characters`]); whether QFEX accepted `add_order` as encoded, without `take_profit`,
//!   `stop_loss` or `reduce_only` ([`AsEncoded`], FBC-blcw); whether `ACK`, rejects, ends and
//!   fills echo our client id exactly ([`Echoes`]); which statuses are rejects and which ends
//!   ([`Statuses`]); and how the crossing post-only was refused ([`Crossed`]). Each says counts,
//!   status names and character classes alone, never an id, a price or a size.
//! - **The record** ([`StageTwo`], [`CrossRun`]; the driver is `stage.rs`'s `Run::cross_tests`):
//!   the gate as judged, the run's frames read once its sessions ended ([`StageTwo::read`]),
//!   and each named account's crossing session, which only an account whose drop test passed in
//!   this run runs ([`crossers`]); after one, its cross registry still counting an order of ours
//!   resting, an `Unknown` one included ([`counted_resting`]), is an order left resting. The
//!   report's section and the evidence's entry ([`markdown`], [`to_json`]) say it with no id, no
//!   price and no size.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::time::Duration;

use fbc_core::{Charset, ClientIdFormat, ClientOrderId, InstrumentId, Lots, Side};
use fbc_oms::{OrdState, Registry};

use crate::capture::AccountRead;
use crate::ledger::Step;
use crate::places::{Stop, TwoRun};
use crate::tee::{Cid, EchoCid, Heard, Kept};

// ----- The exact-echo gate (decision 0148 item 2) -----

/// How the frames of one kind gave back our client id: how many exactly, written otherwise,
/// absent or null, empty, or naming a client id not ours.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Exactness {
    pub exact: usize,
    /// Ours, but not byte for byte: in another case, or with a JSON escape.
    pub differs: usize,
    pub absent: usize,
    pub empty: usize,
    pub foreign: usize,
}

impl Exactness {
    /// Counts one frame whose client id read `cid`.
    fn count(&mut self, cid: Cid) {
        match cid {
            Cid::Exact(_) => self.exact += 1,
            Cid::Differs(_) => self.differs += 1,
            Cid::Absent => self.absent += 1,
            Cid::Empty => self.empty += 1,
            Cid::Foreign => self.foreign += 1,
        }
    }

    /// Counts one `ack` or `err` echo of an `add_order` whose client id read `cid`; an `err`
    /// that echoes no message ([`EchoCid::Unechoed`]) is no echo of one, and is not counted.
    fn count_echo(&mut self, cid: EchoCid) {
        match cid {
            EchoCid::Exact => self.exact += 1,
            EchoCid::Differs => self.differs += 1,
            EchoCid::Foreign => self.foreign += 1,
            EchoCid::Absent => self.absent += 1,
            EchoCid::Unechoed => {}
        }
    }

    /// The frames counted.
    pub fn total(&self) -> usize {
        self.exact + self.differs + self.absent + self.empty + self.foreign
    }

    /// What the report says of it: counts alone.
    pub fn says(&self) -> String {
        if self.total() == 0 {
            return "none heard".to_owned();
        }
        if self.exact == self.total() {
            return format!("exact, byte for byte ({})", self.exact);
        }
        let parts = [
            (self.exact, "exact, byte for byte"),
            (
                self.differs,
                "not byte for byte: ours, in another case or with a JSON escape",
            ),
            (self.absent, "absent or null"),
            (self.empty, "empty"),
            (self.foreign, "naming a client id not ours"),
        ];
        let said: Vec<String> = (parts.iter().filter(|(n, _)| *n > 0))
            .map(|(n, what)| format!("{n} {what}"))
            .collect();
        said.join("; ")
    }
}

/// How the run's frames naming an order of ours gave back its client id, kind by kind (decision
/// 0132's Consequences: "whether ACK, rejects and fills echo our client id exactly"). An
/// `order_response` is an ACK, an end (a status after its order's ACK, or one of [`ENDS`]
/// wherever it came), a reject (any other status before its order's ACK) or, naming no client id
/// of ours, unnamed, since it cannot be tied to its order's ACK. An `ACK` naming no client id at
/// all, absent, null or empty, is counted among the ACKs whether or not its order was known ours,
/// since nothing in it can tie it to an order, ours or a stray's (0151 item 13's stop then ends
/// the run): so the report says how such an ACK came, not that none was heard.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Echoes {
    pub acks: Exactness,
    pub rejects: Exactness,
    pub ends: Exactness,
    pub unnamed: Exactness,
    pub fills: Exactness,
    /// The `ack` and `err` echoes of an `add_order`: recorded, and never opening the gate.
    pub ack_echoes: Exactness,
    pub err_echoes: Exactness,
}

impl Echoes {
    /// Every frame of `kept` naming an order of ours, each counted under its kind. A
    /// `RATE_LIMITED` `order_response` and a `RateLimited` `err` echo refuse a request and say
    /// nothing of the order: each is a provider rate refusal (decision 0148 item 8), counted
    /// elsewhere.
    pub fn of(kept: &Kept) -> Echoes {
        let mut echoes = Echoes::default();
        let mut acked: Vec<usize> = Vec::new();
        for heard in &kept.heard {
            match heard {
                Heard::Order {
                    status, cid, ours, ..
                } if *ours && status != "RATE_LIMITED" => {
                    let kind = match (status == "ACK", cid.sent()) {
                        (true, sent) => {
                            acked.extend(sent);
                            &mut echoes.acks
                        }
                        (false, Some(n)) if acked.contains(&n) || is_end(status) => {
                            &mut echoes.ends
                        }
                        (false, Some(_)) => &mut echoes.rejects,
                        (false, None) => &mut echoes.unnamed,
                    };
                    kind.count(*cid);
                }
                Heard::Order {
                    status,
                    cid: cid @ (Cid::Absent | Cid::Empty),
                    ..
                } if status == "ACK" => echoes.acks.count(*cid),
                Heard::Fill { cid, ours, .. } if *ours => echoes.fills.count(*cid),
                Heard::Echo {
                    envelope,
                    cid,
                    rate_limited: false,
                    ..
                } => match *envelope {
                    "err" => echoes.err_echoes.count_echo(*cid),
                    _ => echoes.ack_echoes.count_echo(*cid),
                },
                _ => {}
            }
        }
        echoes
    }

    /// What the report says of each kind, in order.
    pub fn says(&self) -> Vec<(&'static str, String)> {
        vec![
            ("ACK", self.acks.says()),
            (
                "rejects (a status before its order's ACK, not a fill's or an IOC's end)",
                self.rejects.says(),
            ),
            (
                "ends (a status after its order's ACK, or a fill's or an IOC's end wherever it \
                 came)",
                self.ends.says(),
            ),
            (
                "order_responses naming no client id of ours",
                self.unnamed.says(),
            ),
            ("fills", self.fills.says()),
            (
                "ack echoes of an add_order (never opening the gate)",
                self.ack_echoes.says(),
            ),
            (
                "err echoes of an add_order (never opening the gate)",
                self.err_echoes.says(),
            ),
        ]
    }
}

/// The run's exact-echo gate (decision 0148 item 2), and how every frame naming an order of ours
/// gave back its client id.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct EchoGate {
    /// Whether an `ACK` echoed a client id this run sent, byte for byte: only then may the run
    /// place a crossing order.
    pub open: bool,
    pub echoes: Echoes,
}

impl EchoGate {
    /// The gate `kept`, the run's own tee, gives.
    pub fn of(kept: &Kept) -> EchoGate {
        EchoGate {
            open: kept.exact_ack(),
            echoes: Echoes::of(kept),
        }
    }

    /// What the report says of it.
    pub fn says(&self) -> &'static str {
        match self.open {
            true => EchoGate::open_said(),
            false => EchoGate::shut_said(),
        }
    }

    /// What the report says of an open gate.
    pub fn open_said() -> &'static str {
        "open: an ACK in this run echoed one of our client ids byte for byte (decision 0148 item \
         2)"
    }

    /// What the report says of a shut gate.
    pub fn shut_said() -> &'static str {
        "shut: no ACK in this run echoed one of our client ids byte for byte, so no crossing \
         order is placed (decision 0148 item 2)"
    }
}

// ----- No other order of ours (decision 0148 item 6, 0151 item 5) -----

/// Whether the reads after a drop showed X and Y each ended, as 0151 item 5 counts it: absent
/// from the last read's `get_user_orders`, named by no `get_user_trades` row since X's send, and
/// listed by `GET /user/historic-orders` (read whole), whatever its `terminal_status`. An
/// absence counts as an end, not a fill, only as decision 0132 item 4 reads one and
/// [`crate::places::close_verdict`] reads X's: the trades since X's send read whole with no row
/// at all, since a row naming the other order, another or none is something trading on the
/// account and leaves a fill not ruled out, and the last read's position flat. None counts ended
/// when a provider rate refusal cut those reads short or a read was not made. X then Y.
pub fn ended_after_drop(two: &TwoRun) -> [bool; 2] {
    let (Some(after_drop), Some(after)) = (&two.after_drop, &two.after) else {
        return [false; 2];
    };
    let Some(last) = after_drop.last.filter(|_| after_drop.refused.is_none()) else {
        return [false; 2];
    };
    let absent = [!last.x, !last.y];
    let quiet = after.trades_whole && after.trades == 0 && last.flat;
    let quiet = quiet && after.naming.iter().all(|rows| *rows == 0);
    let untraded = |n: usize| quiet && after.naming.get(n).is_some();
    let terminal =
        |n: usize| after.historic_whole && after.terminal.get(n).is_some_and(Option::is_some);
    [0, 1].map(|n| absent[n] && untraded(n) && terminal(n))
}

/// One registry the run built, as [`no_other_order`] reads it.
#[derive(Clone, Copy)]
pub struct Built<'a> {
    /// Its named account, as the configuration names it.
    pub account: &'static str,
    pub step: Step,
    pub registry: &'a Registry,
    /// The orders 0151 item 5 counts ended though the registry still counts them resting: X and
    /// Y on a drop registry, each once [`ended_after_drop`] showed it ended. None on any other.
    pub ended: &'a [ClientOrderId],
}

/// Why [`no_other_order`] refused a crossing order: named with no id and no amount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Blocked {
    /// A registry holds an order of ours on the market open, in flight or `Unknown` (an `Unknown`
    /// order is never resent and stays counted resting, decision 0005), or X or Y not yet shown
    /// ended (0151 item 5).
    Registry { account: &'static str, step: Step },
    /// A registry's resting count overflowed, so it was not read as none.
    Uncounted { account: &'static str, step: Step },
    /// A named account has no never-arming read to judge it by.
    Unread { account: &'static str },
    /// A named account's never-arming read lists an open order or a TWAP, or counts one on REST.
    Listed { account: &'static str },
}

impl Blocked {
    /// The stop it is in a crossing session, where no crossing order is then placed: an order
    /// of ours a registry counts resting is [`Stop::LeftResting`] (an `Unknown` one counts as
    /// resting, decision 0005), and a read that lists an order, or no read, is
    /// [`Stop::ReadDisagrees`], since the reads before the session showed none.
    pub fn stop(&self) -> Stop {
        match self {
            Blocked::Registry { .. } | Blocked::Uncounted { .. } => Stop::LeftResting,
            Blocked::Unread { .. } | Blocked::Listed { .. } => Stop::ReadDisagrees,
        }
    }

    /// What it is, for the report and standard output.
    pub fn says(&self) -> String {
        match self {
            Blocked::Registry { account, step } => format!(
                "the {} registry on {account} holds an order of ours on the market, open, in \
                 flight or Unknown (an Unknown order is never resent and stays counted resting, \
                 decision 0005; X and Y count ended only as 0151 item 5 rules)",
                step.name()
            ),
            Blocked::Uncounted { account, step } => format!(
                "the {} registry on {account}'s resting orders could not be counted",
                step.name()
            ),
            Blocked::Unread { account } => {
                format!("{account} has no never-arming read to show it holds no order of ours")
            }
            Blocked::Listed { account } => format!(
                "{account}'s never-arming read lists an open order or a TWAP, or REST counts one"
            ),
        }
    }
}

/// Whether a crossing order (the post-only through the touch, the taker IOC) or a flatten IOC may
/// be placed on `inst` (decision 0148 item 6, 0151 item 5): every registry of `built`, the run's
/// every one on every named account, holds no order on `inst` open or `Unknown` (its resting
/// lots on both sides, less those of its orders 0151 item 5 counts ended, zero), and each named
/// account of `reads` has a never-arming read listing no open order and no TWAP, and counting none
/// on REST. A read listing any order refuses, since on accounts `--sole-trader` declares ours
/// alone an order is ours or a stray's, and either keeps a crossing order back (0151 item 13).
/// Refused at the first that does not hold, registries first.
pub fn no_other_order(
    inst: InstrumentId,
    built: &[Built<'_>],
    reads: &[(&'static str, Option<&AccountRead>)],
) -> Result<(), Blocked> {
    for b in built {
        let (account, step) = (b.account, b.step);
        let resting = |side| b.registry.resting_on(inst, side);
        let total = resting(Side::Buy).zip(resting(Side::Sell));
        let total = total.and_then(|(buy, sell)| buy.checked_add(sell));
        let ended = (b.ended.iter().filter_map(|cid| b.registry.get(*cid)))
            .filter(|r| r.placed().inst == inst)
            .try_fold(Lots::ZERO, |sum, r| sum.checked_add(r.resting()));
        let left = total.zip(ended).and_then(|(t, e)| t.checked_sub(e));
        match left {
            None => return Err(Blocked::Uncounted { account, step }),
            Some(left) if left != Lots::ZERO => return Err(Blocked::Registry { account, step }),
            Some(_) => {}
        }
    }
    for (account, read) in reads {
        let account = *account;
        let Some(read) = read else {
            return Err(Blocked::Unread { account });
        };
        if read.page_orders > 0 || read.page_twaps > 0 || read.rest_open_orders > 0 {
            return Err(Blocked::Listed { account });
        }
    }
    Ok(())
}

// ----- The report's items (decision 0132's Consequences, 0148 item 10) -----

/// One class of the characters a client-id format may use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Class {
    pub name: &'static str,
    pub chars: RangeInclusive<char>,
}

const fn class(name: &'static str, chars: RangeInclusive<char>) -> Class {
    Class { name, chars }
}

/// The classes of the characters `fmt` writes a client id in (`crates/fbc-core/src/cid.rs`).
pub fn alphabet(fmt: &ClientIdFormat) -> Vec<Class> {
    let digits = class("decimal digits", '0'..='9');
    match fmt {
        ClientIdFormat::Uuid => vec![
            digits,
            class("lowercase letters a to f", 'a'..='f'),
            class("the hyphen", '-'..='-'),
        ],
        ClientIdFormat::Hex { .. } => vec![digits, class("lowercase letters a to f", 'a'..='f')],
        ClientIdFormat::Numeric { .. } => vec![digits],
        ClientIdFormat::Alnum { charset, .. } => {
            let mut classes = vec![digits, class("lowercase letters a to z", 'a'..='z')];
            if *charset == Charset::Alphanumeric {
                classes.push(class("uppercase letters A to Z", 'A'..='Z'));
            }
            classes
        }
    }
}

/// The characters the run's client ids exercised, by class of the format's alphabet: those an
/// `ACK` echoed byte for byte, those the run sent that no such `ACK` showed, and those it never
/// sent, which are not measured (decision 0132's Consequences: whether QFEX takes the format in
/// every character).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Characters {
    pub classes: Vec<ClassUse>,
    /// Characters the run sent that the format's alphabet does not hold: counted, never shown.
    pub outside: usize,
}

/// One class's characters as the run used them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassUse {
    pub name: &'static str,
    /// In an id an `ACK` echoed byte for byte.
    pub echoed: BTreeSet<char>,
    /// In an id the run sent, but none of them in an id such an `ACK` echoed.
    pub sent_only: BTreeSet<char>,
    /// Never in an id the run sent: not measured.
    pub not_measured: BTreeSet<char>,
}

impl Characters {
    /// What `kept`, the run's tee, shows of the characters of its client ids in `fmt`.
    pub fn of(kept: &Kept, fmt: &ClientIdFormat) -> Characters {
        let (sent, echoed) = kept.characters();
        let classes = alphabet(fmt);
        let classes: Vec<ClassUse> = (classes.iter())
            .map(|c| {
                let all = c.chars.clone().collect::<BTreeSet<char>>();
                let in_class = |set: &BTreeSet<char>| -> BTreeSet<char> {
                    set.intersection(&all).copied().collect()
                };
                let (echoed, sent) = (in_class(&echoed), in_class(&sent));
                ClassUse {
                    name: c.name,
                    sent_only: sent.difference(&echoed).copied().collect(),
                    not_measured: all.difference(&sent).copied().collect(),
                    echoed,
                }
            })
            .collect();
        let held = |ch: &char| alphabet(fmt).iter().any(|c| c.chars.contains(ch));
        let outside = sent.iter().filter(|ch| !held(ch)).count();
        Characters { classes, outside }
    }

    /// What the report says of them: per class, the characters echoed byte for byte, those sent
    /// but not shown so, and those not measured, each as ranges of the class.
    pub fn says(&self) -> String {
        let mut said: Vec<String> = (self.classes.iter())
            .map(|c| {
                let part = |set: &BTreeSet<char>| match set.is_empty() {
                    true => "none".to_owned(),
                    false => ranges(set),
                };
                format!(
                    "{}: echoed byte for byte by an ACK {}; sent, not shown so {}; not measured \
                     {}",
                    c.name,
                    part(&c.echoed),
                    part(&c.sent_only),
                    part(&c.not_measured)
                )
            })
            .collect();
        if self.outside > 0 {
            said.push(format!(
                "{} characters sent outside the format's alphabet",
                self.outside
            ));
        }
        said.join("; ")
    }
}

/// `set` as runs of consecutive characters, `0-3, 5, 8-9`.
fn ranges(set: &BTreeSet<char>) -> String {
    let mut runs: Vec<(char, char)> = Vec::new();
    for ch in set {
        match runs.last_mut() {
            Some((_, end)) if u32::from(*end) + 1 == u32::from(*ch) => *end = *ch,
            _ => runs.push((*ch, *ch)),
        }
    }
    let said: Vec<String> = (runs.iter())
        .map(|(a, b)| match a == b {
            true => a.to_string(),
            false => format!("{a}-{b}"),
        })
        .collect();
    said.join(", ")
}

/// Whether QFEX accepted `add_order` as the codec encodes it, with none of `take_profit`,
/// `stop_loss` and `reduce_only` (FBC-blcw).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AsEncoded {
    /// No `add_order` was sent: not measured.
    NotSent,
    /// An `add_order` carried one of the three: not as encoded.
    Carried,
    /// Every `add_order` went out as encoded, and an `ACK` of an order of ours came.
    Accepted,
    /// Every `add_order` went out as encoded, and no `ACK` of an order of ours came.
    NotAcked,
}

impl AsEncoded {
    /// What `kept`, the run's tee, shows.
    pub fn of(kept: &Kept) -> AsEncoded {
        let sent = kept.sent();
        let carried = |s: &crate::tee::Sent| s.take_profit || s.stop_loss || s.reduce_only;
        match (sent.is_empty(), sent.iter().any(carried)) {
            (true, _) => AsEncoded::NotSent,
            (false, true) => AsEncoded::Carried,
            (false, false) if kept.acked_ours_from(0) => AsEncoded::Accepted,
            (false, false) => AsEncoded::NotAcked,
        }
    }

    /// What the report says of it.
    pub fn says(self) -> &'static str {
        match self {
            AsEncoded::NotSent => "not measured: no add_order was sent",
            AsEncoded::Carried => {
                "not as encoded: an add_order carried take_profit, stop_loss or reduce_only"
            }
            AsEncoded::Accepted => {
                "yes: QFEX acknowledged (ACK) an add_order sent as encoded, with none of \
                 take_profit, stop_loss and reduce_only (FBC-blcw)"
            }
            AsEncoded::NotAcked => {
                "not shown: every add_order went out as encoded, with none of take_profit, \
                 stop_loss and reduce_only, but no ACK of ours came (FBC-blcw)"
            }
        }
    }
}

/// The `order_response` statuses the adapter maps (`fbc-venue-qfex`'s `exec/private.rs`, and the
/// codec's `RATE_LIMITED`): every other is unmapped until captures show what it means (decision
/// 0132 item 8, FBC-jdj7).
pub const MAPPED: [&str; 7] = [
    "ACK",
    "FILLED",
    "IOC_PARTIALLY_FILLED",
    "IOC_CANCELLED",
    "CANCELLED",
    "NO_SUCH_ORDER",
    "RATE_LIMITED",
];

/// The statuses that end an order by what it traded, or end an IOC: an end wherever it comes,
/// before its order's `ACK` too, since an order that trades at once or an IOC that never rests
/// may have no `ACK` (decision 0132 item 8, "an `IOC_*` status by the quantities it states").
pub const ENDS: [&str; 3] = ["FILLED", "IOC_PARTIALLY_FILLED", "IOC_CANCELLED"];

/// Whether `status` is one of [`ENDS`].
fn is_end(status: &str) -> bool {
    ENDS.contains(&status)
}

/// Which statuses the run's `order_response`s naming an order of ours were rejects and which
/// ends (decision 0132's Consequences), as status names, each once in the order first heard.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Statuses {
    /// A status that came before any `ACK` of its order, but one of [`ENDS`].
    pub rejects: Vec<String>,
    /// A status, other than `ACK`, that came after its order's `ACK`, and one of [`ENDS`]
    /// wherever it came.
    pub ends: Vec<String>,
    /// A status, other than `ACK`, of one naming no client id of ours (`NO_SUCH_ORDER`
    /// answering a cancel, say), which cannot be tied to its order's `ACK`.
    pub unnamed: Vec<String>,
    /// The `err` echoes of an `add_order`: a refusal with no status. A `RateLimited` one is a
    /// provider rate refusal (decision 0148 item 8), not counted here.
    pub err_echoes: usize,
}

impl Statuses {
    /// What `kept`, the run's tee, shows.
    pub fn of(kept: &Kept) -> Statuses {
        let mut statuses = Statuses::default();
        let mut acked: Vec<usize> = Vec::new();
        let once = |list: &mut Vec<String>, status: &str| {
            if !list.iter().any(|s| s == status) {
                list.push(status.to_owned());
            }
        };
        for heard in &kept.heard {
            match heard {
                Heard::Order {
                    status, cid, ours, ..
                } if *ours && status != "RATE_LIMITED" => match (status == "ACK", cid.sent()) {
                    (true, sent) => acked.extend(sent),
                    (false, Some(n)) if acked.contains(&n) || is_end(status) => {
                        once(&mut statuses.ends, status)
                    }
                    (false, Some(_)) => once(&mut statuses.rejects, status),
                    (false, None) => once(&mut statuses.unnamed, status),
                },
                Heard::Echo {
                    envelope: "err",
                    rate_limited: false,
                    cid,
                    ..
                } if *cid != EchoCid::Unechoed => statuses.err_echoes += 1,
                _ => {}
            }
        }
        statuses
    }

    /// What the report says of them, each status marked when the adapter does not map it.
    pub fn says(&self) -> String {
        let named = |list: &[String]| match list.is_empty() {
            true => "none".to_owned(),
            false => {
                let each = list.iter().map(|s| match MAPPED.contains(&s.as_str()) {
                    true => s.clone(),
                    false => format!("{s} (unmapped by the adapter)"),
                });
                each.collect::<Vec<String>>().join(", ")
            }
        };
        format!(
            "rejects (before the order's ACK, not a fill's or an IOC's end): {}; ends (after \
             it, or a fill's or an IOC's end wherever it came): {}; naming no client id of ours: \
             {}; err echoes of an add_order: {}",
            named(&self.rejects),
            named(&self.ends),
            named(&self.unnamed),
            self.err_echoes
        )
    }
}

/// How the crossing post-only came back (decision 0132 item 7: "how QFEX refuses a crossing
/// post-only order ... and whether that refusal echoes our client id").
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Crossed {
    /// An `order_response` `CANCELLED` before any `ACK`, naming our client id byte for byte
    /// (`exact`) or not.
    Cancelled { exact: bool },
    /// An `order_response` whose status the adapter does not map ([`MAPPED`]), before any `ACK`.
    Unmapped { status: String, exact: bool },
    /// An `order_response` of another mapped status before any `ACK`.
    Ended { status: String, exact: bool },
    /// An `err` echo of its `add_order`, other than a `RateLimited` one, and how it gave back
    /// the client id it echoes.
    ErrEcho(EchoCid),
    /// No answer naming it, but an `err` echoing no message, other than a `RateLimited` one,
    /// came after its `add_order`: perhaps its refusal, which cannot be told from another's.
    ErrUnechoed,
    /// A provider rate refusal answered it, a `RateLimited` `err` echo or a `RATE_LIMITED`
    /// `order_response`, and nothing else did: a refusal of the request, which says nothing of
    /// how QFEX refuses a crossing post-only, so that is not measured (decision 0148 item 8's
    /// stop).
    RateLimited,
    /// A fill of it: a stop, `UnexpectedFill` (0151 item 9).
    Filled,
    /// An `ACK`, then an `order_response` of it with this status, and no fill: QFEX acknowledged
    /// it, then ended it.
    AckedThenEnded(String),
    /// An `ACK`, and no status after it and no fill: QFEX rested it.
    Rested,
    /// No frame of it, and the registry holds it `Unknown`: never resent, still counted resting
    /// (decision 0005).
    Unknown,
    /// No frame of it, and the registry does not hold it `Unknown`.
    Unanswered,
}

impl Crossed {
    /// How `kept`, the run's tee, from its `from`th frame heard on (the session's first), shows
    /// the order whose `add_order` carried `cid`, and `state`, where its registry holds it. A
    /// fill comes first, then its first status other than `ACK`, after an `ACK` or before one,
    /// then an `ACK`, then an `err` echo of an `add_order` heard after it was sent, whatever
    /// client id it echoes, then a provider rate refusal, then an `err` echoing no message heard
    /// after it was sent, then the registry's `Unknown`. The session sends one `add_order`, so
    /// such an echo or refusal after `from` is its. It is read once the order ended or met its
    /// deadline, before the session's Stop sends its cancels, so a status after its `ACK` is
    /// QFEX's own end of it, not an answer to a cancel of ours.
    pub fn of(kept: &Kept, from: usize, cid: &str, state: Option<OrdState>) -> Crossed {
        let n = kept.index_of(cid);
        let mine = |c: &Cid| n.is_some() && c.sent() == n;
        let heard = || kept.heard.iter().skip(from);
        // When its add_order was sent: an err heard before it answered something else.
        let sent_at = n.and_then(|n| kept.sent().get(n)).map(|s| s.at);
        let after = |at: &fbc_core::MonoNs| sent_at.is_some_and(|sent| *at >= sent);
        if heard().any(|h| matches!(h, Heard::Fill { cid, .. } if mine(cid))) {
            return Crossed::Filled;
        }
        let mut acked = false;
        for h in heard() {
            match h {
                Heard::Order { status, cid, .. } if mine(cid) && status == "ACK" => acked = true,
                Heard::Order { status, cid, .. } if mine(cid) => {
                    let exact = matches!(cid, Cid::Exact(_));
                    let (said, status) = (status.as_str(), status.clone());
                    return match (acked, said, MAPPED.contains(&said)) {
                        (true, ..) => Crossed::AckedThenEnded(status),
                        (false, "CANCELLED", _) => Crossed::Cancelled { exact },
                        (false, _, true) => Crossed::Ended { status, exact },
                        (false, _, false) => Crossed::Unmapped { status, exact },
                    };
                }
                _ => {}
            }
        }
        let err = |h: &Heard| match h {
            Heard::Echo {
                envelope: "err",
                cid,
                rate_limited: false,
                at,
            } if after(at) => Some(*cid),
            _ => None,
        };
        let echo = heard().filter_map(err).find(|c| *c != EchoCid::Unechoed);
        let unechoed = heard().filter_map(err).any(|c| c == EchoCid::Unechoed);
        let rate = |h: &Heard| match h {
            Heard::Echo { rate_limited, .. } => *rate_limited,
            Heard::Order { status, .. } => status == "RATE_LIMITED",
            Heard::Fill { .. } => false,
        };
        match (acked, echo, heard().any(rate), unechoed, state) {
            (true, ..) => Crossed::Rested,
            (false, Some(echo), ..) => Crossed::ErrEcho(echo),
            (false, None, true, ..) => Crossed::RateLimited,
            (false, None, false, true, _) => Crossed::ErrUnechoed,
            (false, None, false, false, Some(OrdState::Unknown)) => Crossed::Unknown,
            (false, None, false, false, _) => Crossed::Unanswered,
        }
    }

    /// How an answer named our client id, as [`Crossed::says`] puts it.
    fn naming(exact: bool) -> &'static str {
        match exact {
            true => "echoing our client id byte for byte",
            false => "naming our client id, not byte for byte (another case or a JSON escape)",
        }
    }

    /// What the report says of it: a status name at most.
    pub fn says(&self) -> String {
        match self {
            Crossed::Cancelled { exact } => format!(
                "refused: an order_response CANCELLED before any ACK, {}",
                Crossed::naming(*exact)
            ),
            Crossed::Unmapped { status, exact } => format!(
                "refused: an order_response {status} before any ACK, a status the adapter does \
                 not map (FBC-jdj7), {}",
                Crossed::naming(*exact)
            ),
            Crossed::Ended { status, exact } => format!(
                "ended: an order_response {status} before any ACK, {}",
                Crossed::naming(*exact)
            ),
            Crossed::ErrEcho(cid) => {
                let named = match cid {
                    EchoCid::Exact => Crossed::naming(true),
                    EchoCid::Differs => Crossed::naming(false),
                    EchoCid::Foreign => "naming a client id not ours",
                    EchoCid::Absent | EchoCid::Unechoed => "naming no client id",
                };
                format!(
                    "refused: an err echo of its add_order {named}, its place left Unknown at its \
                     deadline, never resent and counted resting (decision 0005)"
                )
            }
            Crossed::ErrUnechoed => "not known: an err echoing no message came after its \
                 add_order, perhaps its refusal; nothing named it, so its place ended Unknown at \
                 its deadline, never resent and counted resting (decision 0005)"
                .to_owned(),
            Crossed::RateLimited => {
                "not measured: a provider rate refusal (RateLimited) answered its add_order, which \
                 says nothing of how QFEX refuses a crossing post-only (decision 0148 item 8)"
                    .to_owned()
            }
            Crossed::Filled => {
                "filled: it crossed and traded, a stop (UnexpectedFill, 0151 item 9)".to_owned()
            }
            Crossed::AckedThenEnded(status) => {
                let unmapped = match MAPPED.contains(&status.as_str()) {
                    true => "",
                    false => ", a status the adapter does not map (FBC-jdj7)",
                };
                format!(
                    "refused after an ACK: QFEX acknowledged it (ACK), then an order_response \
                     {status} ended it, and it did not fill{unmapped}"
                )
            }
            Crossed::Rested => {
                "rested: QFEX acknowledged it (ACK), nothing ended it after, and it did not fill"
                    .to_owned()
            }
            Crossed::Unknown => {
                "Unknown at its deadline: nothing answered it, so it is never resent and stays \
                 counted resting (decision 0005), and it keeps every crossing order after it back"
                    .to_owned()
            }
            Crossed::Unanswered => "no answer heard".to_owned(),
        }
    }
}

// ----- Stage two's record (FBC-su9i; decision 0132 item 7, 0148 items 2, 6 and 10) -----

/// The named accounts stage two's crossing post-only may run on, in turn: each of `named` whose
/// drop test passed in this run, as `drops`, this run's drop tests, judged it (decision 0148 item
/// 2). The verdict is the run's own, in memory: none is read from another run.
pub fn crossers(drops: &[crate::places::DropRun], named: &[&'static str]) -> Vec<&'static str> {
    let passed = |account: &&'static str| {
        (drops.iter())
            .any(|d| d.account == *account && d.verdict.as_ref().is_some_and(|v| v.passed))
    };
    named.iter().copied().filter(passed).collect()
}

/// `cid` as the codec writes it on QFEX, byte for byte: what the tee keeps of the `add_order`
/// that carried it ([`Kept::index_of`]), in memory, never shown.
pub fn cid_text(cid: ClientOrderId) -> Result<String, String> {
    let text = fbc_core::encode_cid(&fbc_venue_qfex::exec::CLIENT_ID_FORMAT, cid);
    Ok(text
        .map_err(|e| crate::places::variant_name(&e))?
        .to_string())
}

/// Whether `registry` still counts an order of ours on `inst` resting, on either side, open, in
/// flight or `Unknown`: an `Unknown` order is never resent and stays counted resting until a
/// ladder the probe does not run resolves it (decision 0005), so after a session it is an order
/// of ours still resting (decision 0132 item 7). A count that overflows is counted resting.
pub fn counted_resting(registry: &Registry, inst: InstrumentId) -> bool {
    let resting = |side| registry.resting_on(inst, side);
    let total = resting(Side::Buy).zip(resting(Side::Sell));
    let total = total.and_then(|(buy, sell)| buy.checked_add(sell));
    total != Some(Lots::ZERO)
}

/// One named account's crossing session (decision 0132 item 7), as the report and the evidence
/// show it: names, flags, counts, status names and times alone, never an id, a price or a size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrossRun {
    /// The account it ran on, as the configuration names it.
    pub account: &'static str,
    pub authenticated: bool,
    /// The arm's outcome, as the session reported it.
    pub arm: Option<String>,
    pub resynced: bool,
    /// Whether the cross registry's market was Started, under its market lease.
    pub started: bool,
    /// Why [`no_other_order`] kept the crossing post-only back, when it did ([`Blocked::says`]).
    pub blocked: Option<String>,
    /// Whether the session reached [`no_other_order`]: what the report says of another order
    /// of ours comes from that check alone, never from the session's shape.
    pub checked: bool,
    /// The crossing post-only's request: whether it was handed to the session, shown resting by
    /// an order event, echoed byte for byte by an `ACK`, its outcome and its time to its `ACK`.
    pub placed: crate::places::XRun,
    /// How it came back, once it ended or met its deadline ([`Crossed::of`]).
    pub crossed: Option<Crossed>,
    /// The session's Stop cancels by venue id (decision 0132 item 7, 0148 item 8).
    pub cancels: Option<crate::places::Cancels>,
    /// Each comparison of the streams with the reads (0151 item 4).
    pub compared: Vec<crate::compare::Compared>,
    /// How the session ended: a graceful close, the abrupt drop after held cancels (0151 item
    /// 11), or its connection's own end.
    pub ended_by: Option<&'static str>,
    /// How long before the cross registry's span bound its Stop's cancels began, when the span
    /// bound stopped it (decision 0148 item 8: one `--rpc-timeout-ms` early).
    pub cancels_lead: Option<Duration>,
    /// Whether the read of every named account after the session showed an order of ours
    /// resting: the crossing order on its own account's page, or an order REST counts and no
    /// page lists, which is not told from ours.
    pub resting: bool,
    /// Whether those reads listed an order or a TWAP not ours, what the streams did not show
    /// (decision 0148 item 8): `None` when one of them could not be made and none listed one.
    pub others: Option<bool>,
    /// Whether those reads showed a position not flat on a named account, the owner's whatever
    /// stop came first (0151 item 9): `None` when one of them could not be made and none showed
    /// one.
    pub not_flat_after: Option<bool>,
    /// Whether the cross registry still counted an order of ours resting after the session
    /// ([`counted_resting`]): an `Unknown` one, never resent (decision 0005).
    pub counted_resting: bool,
    /// Whether the account was read after the session.
    pub read: bool,
    /// Why the session could not run its course, when that was no stop: never an id or value.
    pub error: Option<String>,
    /// How QFEX gave back our client id on the session, frame by frame (`places::echoes`).
    pub echoes: Vec<String>,
    /// Whether its `add_order` went out as encoded (FBC-blcw); `None` when none was sent.
    pub as_encoded: Option<bool>,
    /// The run's stop, by name, when one came during it.
    pub stop: Option<&'static str>,
    /// The flatten of a fill of ours the session saw, while it was open (FBC-ou9g; 0151 item 9):
    /// the crossing post-only's, or, in a taker IOC session, the taker IOC's.
    pub flatten: Option<crate::flatten::FlattenRun>,
    /// In a taker IOC session (FBC-ou9g), the taker IOC: how it came back, its fees' signs and
    /// the order of its frames; `None` in a crossing session.
    pub taken: Option<crate::taker::Taken>,
}

/// Stage two (FBC-su9i; decision 0132 item 7, 0148 items 2 and 10), as the report and the
/// evidence show it: the run's exact-echo gate as judged once every named account's drop test
/// ran, what the run's frames show of our client ids and QFEX's statuses, and each crossing
/// session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StageTwo {
    /// Whether the gate was open, once judged: `None` when the run stopped before every named
    /// account's drop test ended.
    pub gate: Option<bool>,
    /// What the run's frames show, read once the run's sessions ended ([`StageTwo::read`]).
    pub echoes: Echoes,
    pub characters: Option<Characters>,
    pub as_encoded: Option<AsEncoded>,
    pub statuses: Statuses,
    /// Each named account's crossing session, in run order.
    pub crosses: Vec<CrossRun>,
    /// Each named account's taker IOC session (FBC-ou9g), in run order: one on each account whose
    /// crossing session did not stop the run.
    pub takes: Vec<CrossRun>,
    /// Whether every account due to cross ran its crossing and its taker IOC sessions with no
    /// stop and no error, so the run reached its end, `stage::NOT_BUILT` (FBC-ou9g).
    pub finished: bool,
}

impl StageTwo {
    /// Reads what `kept`, the run's tee, shows of the whole run's frames into it: how each kind
    /// of frame gave back our client id, the characters the client ids exercised in QFEX's
    /// format, whether `add_order` was accepted as encoded and which statuses were rejects and
    /// ends.
    pub fn read(&mut self, kept: &Kept) {
        self.echoes = Echoes::of(kept);
        self.characters = Some(Characters::of(
            kept,
            &fbc_venue_qfex::exec::CLIENT_ID_FORMAT,
        ));
        self.as_encoded = Some(AsEncoded::of(kept));
        self.statuses = Statuses::of(kept);
    }
}

/// What the report says of an order resting after a crossing session, for the owner (decision
/// 0132 item 7, on production's site as decision 0143 item 4 reads it).
pub const HAND_CANCEL_CROSS: &str = "an order rests on the account after the crossing session: \
     cancel it by hand on QFEX production's site, then give the next place-stage run \
     --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What the report says of an order of ours the cross registry still counts resting after a
/// crossing session when no read after it listed one, for the owner: counted resting, never seen
/// resting (decision 0005, the owner's ruling under 0151 item 6 on FBC-gisx).
pub const HAND_CANCEL_CROSS_COUNTED: &str = "the cross registry still counts an order of ours \
     resting, Unknown and never resent (decision 0005), which no read after the session listed: \
     counted resting, not seen resting (the owner's ruling under 0151 item 6); if it still rests, \
     cancel it by hand on QFEX production's site, then give the next place-stage run \
     --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What the report says of a stage-two session's account not read after the session, once the
/// session sent an order of ours, for the owner: an order of ours may rest there, counted
/// resting, never seen resting (decision 0005, the owner's ruling under 0151 item 6 on FBC-gisx).
pub const HAND_CANCEL_STAGE_UNREAD: &str = "not known: the account was not read after the \
     session, so nothing showed the end of an order of ours that may rest there: counted \
     resting, not seen resting (decision 0005, the owner's ruling under 0151 item 6); read it on \
     QFEX's site, cancel any order of ours still resting by hand, then give the next place-stage \
     run --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What the report says of a stage-two session's account not read after the session when the
/// session sent no order of ours.
const NOT_READ: &str = "not known: the account was not read after the session; read it on \
     QFEX's site before the next run";

/// The owner's hand cancel after stage-two session `run`, the taker IOC session's when `take`:
/// the session's own words when a read after it listed an order of ours resting; counted
/// resting, never seen resting, when only its registry still counts one; and the account not
/// read, an order of ours sent there counted resting, when it was not read after the session
/// (decision 0005, the owner's ruling under 0151 item 6 on FBC-gisx). After the abrupt drop the
/// registry hears nothing more, and its reads, read again until one lists no order of ours, are
/// what show the drop's cancel (0151 item 11): once they read the account and listed none, no
/// order of ours is counted resting by the registry alone. `None` when a read showed none and
/// the registry counts none, or nothing was sent on an account not read. The report, the
/// evidence and the `RESTING` line give the same.
pub fn hand_cancel(run: &CrossRun, take: bool) -> Option<&'static str> {
    use crate::taker::{HAND_CANCEL_TAKE, HAND_CANCEL_TAKE_COUNTED};
    let (seen, counted) = match take {
        true => (HAND_CANCEL_TAKE, HAND_CANCEL_TAKE_COUNTED),
        false => (HAND_CANCEL_CROSS, HAND_CANCEL_CROSS_COUNTED),
    };
    let dropped = run.ended_by == Some(crate::stage::DROPPED);
    match (run.resting, run.counted_resting, run.read) {
        (true, _, _) => Some(seen),
        (false, true, true) if dropped => None,
        (false, true, _) => Some(counted),
        (false, false, false) if run.placed.submitted => Some(HAND_CANCEL_STAGE_UNREAD),
        (false, false, _) => None,
    }
}

/// What the report says of an order resting on `run`'s account after a stage-two session, the
/// taker IOC session's when `take`: [`hand_cancel`]'s, or else whether the account was read.
fn resting_said(run: &CrossRun, take: bool) -> &'static str {
    match (hand_cancel(run, take), run.read) {
        (Some(hand), _) => hand,
        (None, true) => "no",
        (None, false) => NOT_READ,
    }
}

/// What the report says when the reads after a crossing session list an order or a TWAP not
/// ours (decision 0148 item 8).
pub const OTHERS_CROSS: &str = "an order or a TWAP that is not ours is listed on a named account \
     after the crossing session, which the streams did not show: a stop, ReadDisagrees (decision \
     0148 item 8); read the accounts on QFEX's site before the next run";

/// What the report says of a fill of the crossing post-only, beside its stop (0151 item 9).
pub const CROSS_FILLED: &str = "a fill of ours is flattened while its session is still open, \
     before its Stop, as the flatten below says when one ran, and the run then stops (0151 item \
     9)";

/// What the report says of a position not flat in the reads after a crossing session when the
/// run stops on it (0151 item 9).
pub const CROSS_POSITION: &str = "a read after the session shows a position not flat: the run \
     stops not flat, and the position is the owner's (0151 item 9)";

/// What the report says of a position not flat in the reads after a crossing session when the
/// run stopped on a stop before it, the session's own or a provider rate refusal or Ctrl-C in
/// those reads (0151 item 9, decision 0148 item 8).
pub const CROSS_POSITION_BESIDE: &str = "a read after the session shows a position not flat, so \
     the position is the owner's (0151 item 9), though the run stopped first on the stop below";

/// What the report says of a position after a crossing session, given whether the reads after
/// it showed one not flat, `not_flat` ([`CrossRun::not_flat_after`]), and the run's stop.
pub fn position_said(not_flat: Option<bool>, stop: Option<&str>) -> &'static str {
    match (not_flat, stop) {
        (Some(true), Some("NotFlat")) => CROSS_POSITION,
        (Some(true), _) => CROSS_POSITION_BESIDE,
        (Some(false), _) => "no",
        (None, _) => {
            "not known: a named account was not read after the session; read it on QFEX's site \
             before the next run"
        }
    }
}

/// What a crossing session's Stop cancels did, as the report says it.
fn cross_cancels_said(cancels: Option<&crate::places::Cancels>) -> String {
    match cancels {
        None => "none: the session's connection ended first".to_owned(),
        Some(c) => format!(
            "Kill, then {} cancel{} by venue id; {}; {} press{} of Ctrl-C while they ran, each \
             recorded; then the market disarmed (decision 0132 item 7, 0148 item 8)",
            c.sent,
            if c.sent == 1 { "" } else { "s" },
            match (c.held, c.blocked, c.ended) {
                _ if c.cut_short => crate::places::CUT_SHORT,
                (_, true, _) =>
                    "refused locally or met by a provider rate refusal, so not every \
                                 order was seen to end (0151 item 11)",
                (true, false, _) =>
                    "answered other than accepted, so not every order was seen \
                                     to end",
                (false, false, true) => "every order they named was seen to end",
                (false, false, false) => "not every order they named was seen to end in time",
            },
            c.presses,
            if c.presses == 1 { "" } else { "es" }
        ),
    }
}

/// The report's section on stage two: the gate, the run's client ids and statuses, then each
/// named account's crossing session and, after it, its taker IOC session (FBC-ou9g).
pub fn markdown(s: &mut String, two: &StageTwo) {
    use crate::places::{echoes_said, ms, placed_said, yn};
    use std::fmt::Write as _;
    let gate = match two.gate {
        Some(true) => EchoGate::open_said(),
        Some(false) => EchoGate::shut_said(),
        None => {
            "not judged: the run stopped before every named account's drop test ended, so no \
                 crossing order was placed"
        }
    };
    let echoes: Vec<String> = (two.echoes.says().into_iter())
        .map(|(kind, said)| format!("  - {kind}: {said}"))
        .collect();
    let characters = (two.characters.as_ref()).map_or("not read".to_owned(), Characters::says);
    let encoded = two.as_encoded.map_or("not read", AsEncoded::says);
    let _ = writeln!(
        s,
        "## Stage two (FBC-su9i, decision 0132 item 7)\n\n\
         Once every named account's drop test passed, and only while an ACK in this run echoed \
         one of our client ids byte for byte (decision 0148 item 2), each named account in turn \
         places one post-only GTC at the instrument's minimum size at the far touch, through it, \
         to show how QFEX refuses a crossing post-only and whether that refusal echoes our client \
         id, then, in a session of its own, one limit IOC at the instrument's minimum size at the \
         far touch, through it, whose fill is flattened through Exit (FBC-ou9g); each only while \
         no registry of the run and no read of a named account shows another order of ours \
         (decision 0148 item 6, 0151 item 5). What this run's frames show, read on every session \
         (no id, no price and no size is shown):\n\n\
         - The exact-echo gate: {gate}\n\
         - The characters our client ids exercised, by class of QFEX's client-id format \
           (decision 0132's Consequences; the rest not measured): {characters}\n\
         - Whether QFEX accepted add_order as encoded, without take_profit, stop_loss or \
           reduce_only (FBC-blcw): {encoded}\n\
         - Whether ACK, rejects and fills echo our client id exactly, frame kind by kind:\n{}\n\
         - Which statuses are rejects and which ends: {}\n",
        echoes.join("\n"),
        two.statuses.says(),
    );
    for (n, run) in two.crosses.iter().enumerate() {
        let crossed = match &run.crossed {
            Some(crossed) => crossed.says(),
            None => "not measured: it was not placed".to_owned(),
        };
        let filled = match run.crossed {
            Some(Crossed::Filled) => format!("; {CROSS_FILLED}"),
            _ => String::new(),
        };
        let resting = resting_said(run, false);
        let others = match run.others {
            Some(true) => OTHERS_CROSS,
            Some(false) => "none",
            None => {
                "not known: a named account was not read after the session; read it on QFEX's \
                 site before the next run"
            }
        };
        let lead = run.cancels_lead.map_or(String::new(), |lead| {
            format!(
                "; the span bound stopped it, its cancels begun {} before the bound",
                ms(lead)
            )
        });
        let take = two.takes.iter().find(|t| t.account == run.account);
        let followed = match (run.stop, take) {
            (Some(stop), _) => stopped(stop),
            (None, Some(_)) => crate::taker::THEN_TAKE.to_owned(),
            (None, None) => crate::taker::RUN_ENDED.to_owned(),
        };
        let _ = writeln!(
            s,
            "### The crossing post-only on {} (FBC-su9i)\n\n\
             On {} (named by the run's configuration), whose drop test passed in this run: its \
             cross registry built after clean reads of every named account, its CancelOnlyTrip \
             given to the journal, seeded flat; one session armed, resynced and Started, then \
             the crossing post-only; before the session stops it cancels every order of ours by \
             venue id (decision 0132 item 7, 0148 item 8).\n\n\
             - Stream authenticated: {}; what answered the arm: {}; resync applied: {}; market \
               Started under its market lease: {}\n\
             - No other order of ours (decision 0148 item 6): {}\n\
             - The crossing post-only: {}\n\
             - How QFEX refused it: {crossed}{filled}\n\
             - The Stop's cancels: {}{lead}\n\
             - Ended by: {}\n\
             - An order resting on the account when the session ended: {resting}\n\
             - An order or a TWAP not ours in the reads after the session: {others}\n\
             - A position not flat in the reads after the session: {}\n\
             - Our client id as QFEX gave it back, frame by frame: {}\n\
             - add_order sent as encoded (FBC-blcw): {}\n\
             - Error: {}\n\
             - What followed: {followed}\n",
            run.account,
            run.account,
            yn(run.authenticated),
            run.arm.as_deref().unwrap_or("none reported"),
            yn(run.resynced),
            yn(run.started),
            match (run.blocked.as_deref(), run.checked) {
                (Some(b), _) => format!("kept back: {b}"),
                (None, true) => "none: it may be placed".to_owned(),
                (None, false) => "not checked: the session ended before it".to_owned(),
            },
            placed_said(&run.placed),
            cross_cancels_said(run.cancels.as_ref()),
            run.ended_by.unwrap_or("it did not open"),
            position_said(run.not_flat_after, run.stop),
            echoes_said(&run.echoes),
            run.as_encoded.map_or("none sent", yn),
            run.error.as_deref().unwrap_or("none"),
        );
        if !run.compared.is_empty() {
            crate::places::compared_markdown(s, &run.compared);
        }
        if let Some(flatten) = &run.flatten {
            crate::flatten::markdown(s, flatten);
        }
        if let Some(take) = take {
            let next = match (n + 1 < two.crosses.len(), two.finished) {
                (true, _) => Next::Account,
                (false, true) => Next::End,
                (false, false) => Next::Ended,
            };
            take_markdown(s, take, next);
        }
    }
}

/// What the report says follows a session that stopped the run with `stop`.
fn stopped(stop: &str) -> String {
    format!(
        "the run's stop, {stop}: nothing more is placed on this account or any other (decision \
         0132 item 7)"
    )
}

/// What came after a taker IOC session that did not stop the run itself (FBC-ou9g).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Next {
    /// The next named account's crossing session ran.
    Account,
    /// The run reached its end, `stage::NOT_BUILT`, on the last account to cross.
    End,
    /// The run ended first, on a stop or an error that is no stop, before any later session.
    Ended,
}

/// The report's section on one named account's taker IOC session (FBC-ou9g), `next` what came
/// after it.
fn take_markdown(s: &mut String, run: &CrossRun, next: Next) {
    use crate::places::{echoes_said, ms, placed_said, yn};
    use crate::taker::{LAST_TAKE, NEXT_ACCOUNT, OTHERS_TAKE, RUN_ENDED};
    use std::fmt::Write as _;
    let resting = resting_said(run, true);
    let others = match run.others {
        Some(true) => OTHERS_TAKE,
        Some(false) => "none",
        None => {
            "not known: a named account was not read after the session; read it on QFEX's \
             site before the next run"
        }
    };
    let lead = run.cancels_lead.map_or(String::new(), |lead| {
        format!(
            "; the span bound stopped it, its cancels begun {} before the bound",
            ms(lead)
        )
    });
    let followed = match (run.stop, next) {
        (Some(stop), _) => stopped(stop),
        (None, Next::Account) => NEXT_ACCOUNT.to_owned(),
        (None, Next::End) => LAST_TAKE.to_owned(),
        (None, Next::Ended) => RUN_ENDED.to_owned(),
    };
    let _ = writeln!(
        s,
        "### The taker IOC on {} (FBC-ou9g)\n\n\
         On {} (named by the run's configuration), after its crossing session, whose drop test \
         passed in this run, with the exact-echo gate open: its take registry built after clean \
         reads of every named account, its CancelOnlyTrip given to the journal, seeded flat; one \
         session armed, resynced and Started, then one limit IOC at the instrument's minimum size \
         at the far touch, through it, its fill expected and flattened through Exit while the \
         session is open; before the session stops it cancels every order of ours by venue id \
         (decision 0132 item 7, 0148 items 5 and 8).\n\n\
         - Stream authenticated: {}; what answered the arm: {}; resync applied: {}; market \
           Started under its market lease: {}\n\
         - No other order of ours (decision 0148 item 6): {}\n\
         - The taker IOC: {}\n\
         {}\
         - The Stop's cancels: {}{lead}\n\
         - Ended by: {}\n\
         - An order resting on the account when the session ended: {resting}\n\
         - An order or a TWAP not ours in the reads after the session: {others}\n\
         - A position not flat in the reads after the session: {}\n\
         - Our client id as QFEX gave it back, frame by frame: {}\n\
         - add_order sent as encoded (FBC-blcw): {}\n\
         - Error: {}\n\
         - What followed: {followed}\n",
        run.account,
        run.account,
        yn(run.authenticated),
        run.arm.as_deref().unwrap_or("none reported"),
        yn(run.resynced),
        yn(run.started),
        match (run.blocked.as_deref(), run.checked) {
            (Some(b), _) => format!("kept back: {b}"),
            (None, true) => "none: it may be placed".to_owned(),
            (None, false) => "not checked: the session ended before it".to_owned(),
        },
        placed_said(&run.placed),
        crate::taker::session_lines(run.taken.as_ref()),
        cross_cancels_said(run.cancels.as_ref()),
        run.ended_by.unwrap_or("it did not open"),
        position_said(run.not_flat_after, run.stop),
        echoes_said(&run.echoes),
        run.as_encoded.map_or("none sent", yn),
        run.error.as_deref().unwrap_or("none"),
    );
    if !run.compared.is_empty() {
        crate::places::compared_markdown(s, &run.compared);
    }
    if let Some(flatten) = &run.flatten {
        crate::flatten::markdown(s, flatten);
    }
}

/// The evidence's entry for stage two.
pub fn to_json(two: &StageTwo) -> serde_json::Value {
    use crate::places::{compared_json, placed_json};
    use serde_json::json;
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let echoes: serde_json::Map<String, serde_json::Value> = (two.echoes.says().into_iter())
        .map(|(kind, said)| (kind.to_owned(), json!(said)))
        .collect();
    let session = |run: &CrossRun, take: bool| {
        let compared: Vec<serde_json::Value> = run.compared.iter().map(compared_json).collect();
        let cancels = run.cancels.as_ref().map(|c| {
            json!({"sent": c.sent, "held": c.held, "blocked": c.blocked, "ended": c.ended,
                "presses": c.presses, "cut_short": c.cut_short})
        });
        json!({"account": run.account, "authenticated": run.authenticated, "arm": run.arm,
            "resynced": run.resynced, "started": run.started, "blocked": run.blocked,
            "other_order_checked": run.checked,
            "cancels": cancels, "cancels_lead_ms": run.cancels_lead.map(ms),
            "compared": compared, "ended_by": run.ended_by, "resting": run.resting,
            "counted_resting": run.counted_resting, "read_after": run.read,
            "others_after": run.others, "not_flat_after": run.not_flat_after,
            "owners_position": (run.not_flat_after == Some(true))
                .then(|| position_said(run.not_flat_after, run.stop)),
            "hand_cancel": hand_cancel(run, take),
            "echoes": run.echoes, "add_order_as_encoded": run.as_encoded,
            "error": run.error, "stop": run.stop,
            "flatten": run.flatten.as_ref().map(crate::flatten::to_json)})
    };
    let crosses: Vec<serde_json::Value> = (two.crosses.iter())
        .map(|run| {
            let mut entry = session(run, false);
            entry["crossing_post_only"] = placed_json(&run.placed);
            entry["refused"] = json!(run.crossed.as_ref().map(Crossed::says));
            entry
        })
        .collect();
    let takes: Vec<serde_json::Value> = (two.takes.iter())
        .map(|run| {
            let mut entry = session(run, true);
            entry["taker_ioc"] = placed_json(&run.placed);
            entry["taken"] = crate::taker::taken_json(run.taken.as_ref());
            entry
        })
        .collect();
    json!({"exact_echo_gate": two.gate,
        "characters": two.characters.as_ref().map(Characters::says),
        "add_order_as_encoded": two.as_encoded.map(AsEncoded::says),
        "echoes": echoes, "statuses": two.statuses.says(), "crossings": crosses,
        "takes": takes})
}
