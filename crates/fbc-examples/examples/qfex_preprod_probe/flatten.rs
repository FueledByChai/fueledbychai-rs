//! The flatten (FBC-ou9g, a slice of FBC-9sk2; decisions 0012, 0063, 0148 items 5, 6, 8 and 9,
//! 0151 items 5, 8, 9 and 10): what closes a position a fill of ours leaves, reached from every
//! place-stage registry while its session is open. The driver is `stage.rs`'s `Run::flatten`;
//! here are its pure parts and its record.
//!
//! - **The order of its steps** (0151 item 9): every other order of ours on the market cancelled
//!   by venue id, each an fbc-oms authorization through the `Link`, and each seen to end, or no
//!   flatten IOC (decision 0148 item 6) and the run stops not flat; then the owner's Flatten into
//!   Exit, which `CodProbeRun::owner_supervised_places` admits after a fill (decision 0148 item
//!   3; Start is never pressed again); then, while the account's one flatten budget lasts
//!   (`--flatten-attempts` across its registries, `places::Allowance`), one reducing limit IOC at
//!   a time ([`next`]), each placed only while no registry of the run holds another order of
//!   ours and the named accounts' reads before the session list none (`stage_two::no_other_order`,
//!   decision 0148 item 6), priced from a touch read just before it ([`ioc_price`]), its client id
//!   minted before those checks so nothing is awaited between them and the place, waited on until
//!   it ends with its fills settled or its deadline, and the position read again from the
//!   registry; then the streams compared with a never-arming read, which shows REST positions
//!   flat when it agrees with the registry's flat position. A fill that reaches the session only
//!   during its Stop's cancels or after them is flattened then, before the close
//!   ([`AFTER_LATE_FILL`]).
//! - **Each flatten IOC** (decision 0148 item 5): a limit IOC at the instrument's minimum size on
//!   the side that reduces the position, classified reducing (`NewOrder::reducing`) with no
//!   reduce-only flag, built through the registry's one pre-trade path outside the planner, as
//!   `paradex_testnet_acceptance`'s flatten is, and judged there by Exit and both caps (decisions
//!   0012 and 0063). Its fills are the session's expected fills (`places::Watched::expected`), so
//!   they stop nothing. One left `Unknown` is never resent (decision 0005) and keeps every later
//!   flatten IOC back; one whose fills had not all reached the registry by its deadline is the
//!   last placed, since the registry's position is then not QFEX's.
//! - **Its stops** ([`Flattening`]), each leaving the position not flat and the owner's (decision
//!   0148 items 5, 8 and 9): a residual below the minimum size, a refusal (the owner's Flatten,
//!   or the IOC refused by fbc-oms, by the session, or by an err QFEX echoed its `add_order`
//!   with, or its client id not minted), a price no grid holds within the cap, an IOC a USD cap
//!   would not admit at the touch it is priced from (decision 0113), the budget used
//!   up, an IOC whose fills had not all reached the registry, other orders of ours not seen to
//!   end, and no touch fresh and uncrossed to price from: a flatten that does not run its course
//!   always stops the run, a taker IOC session's included.
//! - **What it says** ([`FlattenRun`], [`markdown`], [`to_json`]): names, flags, counts and
//!   status words alone. The cap, the attempts and every price and size are fences met, never
//!   values (0151 item 10).

use std::fmt::Write as _;

use fbc_core::{InstrumentSpec, Lots, Side, SignedLots, Ticks};
use fbc_oms::OrdState;
use serde_json::{Value, json};

use crate::compare::{Compared, Ended};
use crate::market::{self, Quote};
use crate::places::{Cancels, Flattening, Stop};

/// What the flatten does next ([`next`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Next {
    /// The registry's position is flat: no IOC is placed.
    Flat,
    /// One flatten IOC, on this side, the one that reduces the position.
    Ioc(Side),
    /// The flatten stops, not flat.
    Stop(Flattening),
}

/// What the flatten does at `position`, the registry's position on the market, with `min_size`
/// the instrument's minimum size and `left` the flatten IOCs the account's budget still allows:
/// nothing once flat; [`Flattening::BelowMinimum`] when what is left is below the minimum size,
/// which no IOC is placed at; [`Flattening::UsedUp`] once the budget is spent; otherwise one IOC
/// on the side that reduces the position (a sell while long, a buy while short).
pub fn next(position: SignedLots, min_size: Lots, left: u64) -> Next {
    let size = position.abs_lots();
    match (position.0.signum(), size) {
        (0, _) => Next::Flat,
        (_, Some(size)) if size < min_size => Next::Stop(Flattening::BelowMinimum),
        _ if left == 0 => Next::Stop(Flattening::UsedUp),
        (1, _) => Next::Ioc(Side::Sell),
        _ => Next::Ioc(Side::Buy),
    }
}

/// Basis points in one.
const BPS: i128 = 10_000;

/// A flatten IOC's price on `side` from `quote`, the touch read just before it: at most
/// `cap_bps` through the touch it crosses (the bid for a sell, the ask for a buy), snapped onto
/// `spec`'s grid toward that touch as `market::flatten_price` snaps it (a sell ceiled, as
/// `InstrumentSpec::quantize` rounds a sell up, and a buy floored). [`Flattening::Capped`] when
/// no such price is on the grid: none positive, or the grid's price nearest the cap no longer
/// crossing that touch, or lying further through it than the cap, so the IOC is not sent.
pub fn ioc_price(
    spec: &InstrumentSpec,
    side: Side,
    quote: Quote,
    cap_bps: u32,
) -> Result<Ticks, Flattening> {
    let px = market::flatten_price(spec, side, quote, cap_bps).map_err(|_| Flattening::Capped)?;
    let cap = i128::from(cap_bps);
    let (p, bid, ask) = (
        i128::from(px.0),
        i128::from(quote.bid.0),
        i128::from(quote.ask.0),
    );
    let held = match side {
        Side::Sell => p <= bid && p * BPS >= bid * (BPS - cap),
        Side::Buy => p >= ask && p * BPS <= ask * (BPS + cap),
    };
    match held {
        true => Ok(px),
        false => Err(Flattening::Capped),
    }
}

/// How one flatten IOC came back by the end of its wait ([`Ioc::of`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Ioc {
    /// Ended filled whole, its fills settled.
    Filled,
    /// Ended with part of it filled, its fills settled.
    PartEnded,
    /// Left open with a fill, as the adapter decodes QFEX's `IOC_PARTIALLY_FILLED` with a
    /// remainder (0151 item 8): it never ends in the registry, so it stays counted resting.
    PartOpen,
    /// Acknowledged and left open with no fill by its deadline: it never ended in the registry,
    /// so it stays counted resting, and it is never resent (decision 0005).
    Open,
    /// Ended unfilled: expired, nothing on the book within its price.
    Expired,
    /// Ended, but the fills its order events state did not all arrive by its deadline.
    Unsettled,
    /// No answer that left it open or ended it by its deadline: in flight or `Unknown`, never
    /// resent (decision 0005).
    Unknown,
    /// QFEX echoed its `add_order` with an err: refused, left in flight or `Unknown`, never
    /// resent.
    Refused,
    /// The session did not send it.
    NotSent,
}

impl Ioc {
    /// What a flatten IOC came back as: `state`, its state in the registry at the end of its
    /// wait (none once not registered), its fills counted `cum_fills` against the venue's count
    /// `cum_venue` and its size `qty`; `err_echoed` whether QFEX echoed its `add_order` with an
    /// err; `not_sent` whether the session did not send it.
    pub fn of(
        state: Option<OrdState>,
        (cum_fills, cum_venue, qty): (Lots, Lots, Lots),
        err_echoed: bool,
        not_sent: bool,
    ) -> Ioc {
        let settled = cum_fills >= cum_venue;
        match state {
            _ if not_sent => Ioc::NotSent,
            _ if err_echoed => Ioc::Refused,
            Some(OrdState::Terminal(_)) if !settled => Ioc::Unsettled,
            Some(OrdState::Terminal(_)) if cum_fills >= qty => Ioc::Filled,
            Some(OrdState::Terminal(_)) if cum_fills > Lots::ZERO => Ioc::PartEnded,
            Some(OrdState::Terminal(_)) => Ioc::Expired,
            Some(OrdState::Open | OrdState::PartiallyFilled) if cum_venue > Lots::ZERO => {
                Ioc::PartOpen
            }
            // QFEX answered: acknowledged, open in the registry, with no fill it states.
            Some(OrdState::Open) => Ioc::Open,
            _ => Ioc::Unknown,
        }
    }

    /// Whether the wait for it is over: it ended with its fills settled.
    pub fn settled(state: Option<OrdState>, cum_fills: Lots, cum_venue: Lots) -> bool {
        state.is_some_and(OrdState::is_terminal) && cum_fills >= cum_venue
    }

    /// As the report says it.
    pub fn says(self) -> &'static str {
        match self {
            Ioc::Filled => "filled whole, its fills settled",
            Ioc::PartEnded => "filled in part and ended, its fills settled",
            Ioc::PartOpen => {
                "filled in part and left open, as the adapter decodes IOC_PARTIALLY_FILLED with a \
                 remainder (0151 item 8): counted resting"
            }
            Ioc::Open => {
                "acknowledged and left open with no fill by its deadline: counted resting, never \
                 resent (decision 0005)"
            }
            Ioc::Expired => "expired unfilled",
            Ioc::Unsettled => {
                "ended, but not every fill its order events state arrived by its \
                               deadline"
            }
            Ioc::Unknown => {
                "no answer by its deadline: in flight or Unknown, never resent (decision 0005), \
                 and counted resting"
            }
            Ioc::Refused => {
                "refused by an err QFEX echoed its add_order with: left in flight or Unknown, \
                 never resent"
            }
            Ioc::NotSent => "not sent by the session",
        }
    }
}

/// One flatten IOC ([`FlattenRun::iocs`]): never a price, a size or an id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IocRun {
    /// The side it reduced on, `buy` or `sell`.
    pub side: &'static str,
    /// Whether it was handed to the session.
    pub sent: bool,
    /// How it came back, once handed to the session: none while it waits, or once its session's
    /// connection ended first ([`IN_FLIGHT`]).
    pub came_back: Option<Ioc>,
    /// Why it was not built or handed over, by the refusal's variant name alone.
    pub refused: Option<String>,
}

/// One flatten, as the report and the evidence show it (decision 0148 item 10): names, flags,
/// counts and status words alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlattenRun {
    /// What it answered: an unexpected fill of ours (0151 item 9).
    pub after: &'static str,
    /// The cancels of every other order of ours on the market, by venue id.
    pub cancels: Option<Cancels>,
    /// Whether the owner's Flatten put the market in Exit; `None` when it was not pressed, the
    /// registry flat or an order of ours not seen to end.
    pub exit: Option<bool>,
    /// Each flatten IOC, in order.
    pub iocs: Vec<IocRun>,
    /// Why `stage_two::no_other_order` kept a flatten IOC back, when it did.
    pub blocked: Option<String>,
    /// Each comparison of the streams with the reads: after the cancels, after each IOC, and the
    /// one that shows REST positions flat.
    pub compared: Vec<Compared>,
    /// Whether the registry's position was flat when it ended.
    pub flat: bool,
    /// Whether a never-arming read after its last step agreed with the registry's flat
    /// position: REST positions flat.
    pub rest_flat: bool,
    /// Its own stop, by name, when it stopped not flat.
    pub stop: Option<&'static str>,
    /// Why it could not run its course, when that was no stop of its own: never an id or value.
    pub error: Option<String>,
    /// The owner's presses taken while it ran, each recorded, none ending it.
    pub presses: u32,
}

impl FlattenRun {
    /// The flatten IOCs sent.
    pub fn sent(&self) -> usize {
        self.iocs.iter().filter(|i| i.sent).count()
    }

    /// How it ended, as the report says it.
    pub fn ended(&self) -> String {
        match (self.flat, self.rest_flat, self.stop, &self.error) {
            (_, _, Some(stop), _) => format!(
                "stopped not flat, {stop}: the position is the owner's (decision 0148 items 5 \
                 and 9)"
            ),
            (true, true, None, _) => {
                "flat: the registry's position flat, and REST positions flat on a never-arming \
                 read that agreed with it"
                    .to_owned()
            }
            (true, false, None, _) => {
                "the registry's position flat, but no never-arming read agreed with it: REST \
                 positions not shown flat"
                    .to_owned()
            }
            (false, _, None, Some(why)) => format!("not flat: {why}"),
            (false, _, None, None) => "not flat".to_owned(),
        }
    }
}

/// What a flatten answers once its session saw a fill of ours other than an expected one.
pub const AFTER_FILL: &str = "an unexpected fill of ours (0151 item 9)";

/// What a flatten answers once a fill of ours other than an expected one reached its session
/// only after its steps ended, during its Stop's cancels or after them, before its close.
pub const AFTER_LATE_FILL: &str = "an unexpected fill of ours that reached the session after its \
     steps ended, during its Stop's cancels or after them (0151 item 9): the Stop's Kill lifted \
     first, so the owner's Flatten could put the market in Exit under the market lease still \
     held, and the market Killed again after, then disarmed as the Stop disarms it";

/// How a flatten IOC handed to the session came back when the session's connection ended
/// before its answer was heard.
pub const IN_FLIGHT: &str = "sent, its answer not heard before the session's connection ended: in \
     flight or Unknown, never resent (decision 0005)";

/// What the flatten does when not every other order of ours was seen to end (decision 0148 item
/// 6, 0151 item 9).
pub const NOT_ENDED: &str = "not every other order of ours on the market was seen to end, so no \
     flatten IOC is placed (decision 0148 item 6) and the run stops not flat";

/// Why a flatten did not run its course when its session's connection ended during it.
pub const CONNECTION_ENDED: &str = "the session's connection ended during the flatten";

/// The fences a flatten IOC meets, as the report says them (0151 item 10).
pub const FENCES: &str = "each a limit IOC at the instrument's minimum size on the side that \
     reduces the position, classified reducing with no reduce-only flag, built through the \
     registry's one pre-trade path and judged by Exit and both caps (decisions 0012 and 0063), \
     priced at most --flatten-cap-bps through the touch it crosses, read just before it, and no \
     more than --flatten-attempts for the account across its registries (fences met; no price, \
     size, cap or count of attempts is shown)";

/// What a comparison came to, as the report names it.
fn ended(e: Ended) -> &'static str {
    match e {
        Ended::Agreed => "agreed",
        Ended::Lagged => "lagged",
        Ended::Disagreed => "disagreed: a stop (decision 0148 item 8)",
        Ended::Unread => "unread",
        Ended::Refused => "refused: a provider rate refusal, a stop",
    }
}

/// The report's section on a flatten, under its session's.
pub fn markdown(s: &mut String, run: &FlattenRun) {
    let cancels = match &run.cancels {
        None => "none made".to_owned(),
        Some(c) => format!(
            "{} cancel{} by venue id; {}",
            c.sent,
            if c.sent == 1 { "" } else { "s" },
            match (c.held, c.ended) {
                (true, _) => "held or refused, so not every order was seen to end",
                (false, true) => "every order they named was seen to end",
                (false, false) => "not every order they named was seen to end in time",
            }
        ),
    };
    let exit = match run.exit {
        Some(true) => "pressed: the market in Exit",
        Some(false) => "refused",
        None => "not pressed",
    };
    let iocs: Vec<String> = (run.iocs.iter().enumerate())
        .map(|(n, ioc)| {
            let came = match (&ioc.refused, ioc.came_back, ioc.sent) {
                (Some(why), _, _) => format!("not sent: refused, {why}"),
                (None, Some(back), _) => back.says().to_owned(),
                (None, None, true) => IN_FLIGHT.to_owned(),
                (None, None, false) => "not sent".to_owned(),
            };
            format!("  - IOC {} ({}): {came}", n + 1, ioc.side)
        })
        .collect();
    let iocs = match iocs.is_empty() {
        true => "  - none placed".to_owned(),
        false => iocs.join("\n"),
    };
    let compared: Vec<String> = (run.compared.iter())
        .map(|c| format!("{}: {}", c.step, ended(c.ended)))
        .collect();
    let compared = match compared.is_empty() {
        true => "none made".to_owned(),
        false => compared.join("; "),
    };
    let _ = writeln!(
        s,
        "#### The flatten (FBC-ou9g; decision 0148 item 5, 0151 item 9)\n\n\
         After {}, while the session was open: every other order of ours cancelled by venue id \
         and seen to end, then the owner's Flatten into Exit, then flatten IOCs, {FENCES}.\n\n\
         - The cancels: {cancels}\n\
         - The owner's Flatten: {exit}\n\
         - The flatten IOCs, {} sent:\n{iocs}\n\
         - Kept back by another order of ours: {}\n\
         - The comparisons with never-arming reads: {compared}\n\
         - How it ended: {}\n\
         - Presses of Ctrl-C while it ran, each recorded, none ending it: {}\n",
        run.after,
        run.sent(),
        run.blocked.as_deref().unwrap_or("no"),
        run.ended(),
        run.presses,
    );
}

/// The evidence's entry for a flatten.
pub fn to_json(run: &FlattenRun) -> Value {
    let iocs: Vec<Value> = (run.iocs.iter())
        .map(|ioc| {
            json!({"side": ioc.side, "sent": ioc.sent,
                "came_back": ioc.came_back.map(Ioc::says), "refused": ioc.refused})
        })
        .collect();
    let compared: Vec<Value> = run
        .compared
        .iter()
        .map(crate::places::compared_json)
        .collect();
    json!({"after": run.after, "fences": FENCES,
        "cancels": crate::places::cancels_json(run.cancels.as_ref()), "exit": run.exit,
        "iocs": iocs, "blocked": run.blocked, "compared": compared, "flat": run.flat,
        "rest_flat": run.rest_flat, "stop": run.stop, "error": run.error,
        "presses": run.presses, "ended": run.ended()})
}

/// The stop a flatten's own stop gives its session, whose first stop was `first`: the flatten's
/// own stop replaces [`Stop::UnexpectedFill`], the fill it answered, since it says why the
/// position is left the owner's; any other first stop stays the session's, the first raised.
pub fn session_stop(first: Option<Stop>, own: Option<Stop>) -> Option<Stop> {
    match (first, own) {
        (Some(Stop::UnexpectedFill) | None, Some(own)) => Some(own),
        (first, _) => first,
    }
}
