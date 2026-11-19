//! Stage three, the pages (FBC-4plj; decision 0132 items 5 and 7, 0148 items 7, 8 and 10): on
//! each named account whose drop test passed in this run, with the run's exact-echo gate open,
//! after its taker IOC session, one session on its pages registry rests up to three post-only
//! GTC orders at the instrument's minimum size `--away-bps` behind the touch, P1, P2 and P3, each
//! an fbc-oms authorization spent from the pages registry's own list, and cancels them by venue
//! id. Beside it, the pager, a second socket that never arms and never places, pages
//! `get_user_orders` at a page size of 1 ([`PAGE_SIZE`]) between those places and cancels, as
//! [`PLAN`] orders them, every frame charged to the shared provider-user limiter first
//! (`compare::Socket::page`). Each page is recorded with its size and offset ([`PageRead`]), what
//! it listed by name alone ([`named`]) and which of ours the streams showed open as it was asked,
//! so the pages show where QFEX's page boundaries fall as orders come and go (decision 0132 item
//! 5's evidence for a later flip). Each place and each cancel is compared with never-arming reads,
//! as stage two's are (0151 item 4), and the session ends through the place stages' session
//! machinery (`stage.rs`, `Policy::Stage`): every order of ours still open cancelled by venue id
//! before it stops, then the read of every named account.
//!
//! A provider rate refusal on the pager's socket stops the run, [`Stop::RateRefused`] on
//! [`RefusedOn::Pager`], and so does a page listing an order that is none of P1 to P3, or a
//! TWAP, [`Stop::ReadDisagrees`] (decision 0148 item 8). So does a page listing one of P1 to P3
//! that the streams showed ended past its lag ([`Ends`]): judged as `compare::judge` judges such
//! a row (an order of ours listed that the streams showed ended), and allowed as lag only as
//! `compare::compare` allows it, until a comparison begun after the streams showed it ended has
//! ended, or `--agree-wait-ms` from then has passed (decisions 0109 and 0151 item 4; PR #284's
//! review). A page listing one within its lag is recorded and stops nothing; the next comparison
//! judges it. A page that could not be made (no room
//! in the shared limiter, or its socket failed) is recorded and stops nothing: the pages are
//! evidence, and the next page opens the socket again. A maker fill of a stage-three order is
//! flattened while the session is open, then stops the run, as every unexpected fill does (0151
//! item 9).
//!
//! Nothing of a row is kept in the report or the evidence: a page is recorded by its size, its
//! offset, the names P1 to P3 of ours it listed, and counts. The frames the pager read, their
//! credential spans blanked as a journal would keep them, go to the `pages-production/` slice
//! alone, scrubbed there as every place-stage frame is (decision 0148 item 10).

use std::fmt::Write as _;
use std::time::Duration;

use fbc_core::{CidMatch, ClientOrderId, VenueOrderId};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::compare::{Listed, PageAttempt};
use crate::places::{RefusedOn, Stop, XRun};
use crate::refusals::Refusals;
use crate::stage_two::{CrossRun, Session};

/// The page size the pager asks for: one row a page, so every boundary shows (decision 0132
/// item 7).
pub const PAGE_SIZE: u16 = 1;

/// The names of stage three's three orders, in the order they are placed.
pub const NAMES: [&str; 3] = ["P1", "P2", "P3"];

/// What the comparison after each order's place is reported under.
pub const PLACED: [&str; 3] = ["P1's place", "P2's place", "P3's place"];

/// What the comparison after each order's cancel is reported under.
pub const CANCELLED: [&str; 3] = ["P1's cancel", "P2's cancel", "P3's cancel"];

/// One step of stage three's session.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Place the order of [`NAMES`] at this index, then compare.
    Place(usize),
    /// Ask the pager for the page at this offset.
    Page(u32),
    /// Cancel the order of [`NAMES`] at this index by venue id, then compare.
    Cancel(usize),
    /// Ask the pager for every page from offset 0 until a short one ([`walk_end`]).
    Walk,
}

/// Stage three's interleaving (FBC-4plj): place P1 and P2; page 0; place P3; page 1; cancel P1;
/// page 2; page 3; cancel P2 and P3; then the pages from 0 until a short page.
pub const PLAN: [Action; 11] = [
    Action::Place(0),
    Action::Place(1),
    Action::Page(0),
    Action::Place(2),
    Action::Page(1),
    Action::Cancel(0),
    Action::Page(2),
    Action::Page(3),
    Action::Cancel(1),
    Action::Cancel(2),
    Action::Walk,
];

/// How long the walk of every page from offset 0 may take, from its first page: the reads after a
/// session's patience, so a venue answering new full pages for ever cannot hold the run open
/// (FBC-8o7a's bound).
pub const WALK_PATIENCE: Duration = crate::stage::AFTER_PATIENCE;

/// What one page came to, by names and counts alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Came {
    /// Read: the names of ours it listed, in its order; how many orders it listed that are none
    /// of ours; its TWAPs; and whether it was full.
    Read {
        ours: Vec<&'static str>,
        others: usize,
        twaps: usize,
        full: bool,
    },
    /// The shared limiter had no room for one of its frames, so it was not sent.
    Skipped,
    /// It could not be made: its socket failed, or its answer was not a page.
    Failed,
    /// A provider rate refusal on the pager: the run's stop.
    Refused,
}

impl Came {
    /// What the report says of it.
    pub fn says(&self) -> String {
        match self {
            Came::Read {
                ours,
                others,
                twaps,
                full,
            } => format!(
                "listed {}; {others} order{} not ours; {twaps} TWAP{}; {}",
                match ours.is_empty() {
                    true => "none of ours".to_owned(),
                    false => ours.join(", "),
                },
                if *others == 1 { "" } else { "s" },
                if *twaps == 1 { "" } else { "s" },
                if *full { "full" } else { "short" }
            ),
            Came::Skipped => "not sent: the shared limiter had no room".to_owned(),
            Came::Failed => "not read: its socket failed or its answer was no page".to_owned(),
            Came::Refused => "a provider rate refusal on the pager: the run's stop".to_owned(),
        }
    }
}

/// One page the pager asked for (FBC-4h1p's record: its page size and offset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRead {
    /// Whether it was one of the walk's pages, from offset 0 until a short one.
    pub walk: bool,
    pub size: u16,
    pub offset: u32,
    /// The names of ours the streams showed open as it was asked.
    pub open: Vec<&'static str>,
    pub came: Came,
}

/// How the walk of every page from offset 0 ended.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WalkEnd {
    /// A page that was not full: the walk's end.
    Short,
    /// A full page listing an order an earlier page of the walk listed, as a venue that ignores
    /// the offset answers: the walk ends there.
    Repeated,
    /// [`WALK_PATIENCE`] passed with its pages not all read.
    Cut,
    /// A page could not be made ([`Came::Skipped`] or [`Came::Failed`]).
    NotRead,
    /// A page listed an order that is none of ours, or a TWAP, or one of ours the streams showed
    /// ended past its lag ([`Ends`]): the run's stop, [`Stop::ReadDisagrees`] (decision 0148 item
    /// 8).
    Disagreed,
}

impl WalkEnd {
    pub fn says(self) -> &'static str {
        match self {
            WalkEnd::Short => "a short page, its end",
            WalkEnd::Repeated => {
                "a full page listing an order an earlier page of it listed, as a venue that \
                 ignores the offset answers"
            }
            WalkEnd::Cut => "cut 60 s after its first page (the reads after a session's patience)",
            WalkEnd::NotRead => "a page that could not be made",
            WalkEnd::Disagreed => {
                "a page listing an order or a TWAP not ours, or one of ours the streams showed \
                 ended past its lag, which the streams did not show: the run's stop, \
                 ReadDisagrees (decision 0148 item 8)"
            }
        }
    }
}

/// Whether the walk ends at `came`, its page, given the orders the walk's earlier pages listed,
/// `seen`, by venue id: `None` when the next offset follows. A full page at the page size of 1
/// lists one row; a row the walk saw already is [`WalkEnd::Repeated`].
pub fn walk_end(came: &PageAttempt, seen: &[VenueOrderId]) -> Option<WalkEnd> {
    match came {
        PageAttempt::Read { full: false, .. } => Some(WalkEnd::Short),
        PageAttempt::Read { listed, .. } => {
            let new = listed.iter().any(|l| !seen.contains(&l.order));
            (!new && !listed.is_empty()).then_some(WalkEnd::Repeated)
        }
        PageAttempt::Skipped | PageAttempt::Failed(_) => Some(WalkEnd::NotRead),
        // The run's stop; the walk ends with it.
        PageAttempt::Refused => Some(WalkEnd::NotRead),
    }
}

/// One of ours, as stage three's pages are matched to it: its name, its client id and its venue
/// id once known.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ours {
    pub name: &'static str,
    pub cid: ClientOrderId,
    pub venue: Option<VenueOrderId>,
}

/// The names of `ours` that `listed`, one page's rows, names, in the page's order, and how many
/// rows are none of them: a row is ours by its venue id once known, or by its client id decoded
/// under our namespace while its venue id is not, as `compare::judge` matches one.
pub fn named(listed: &[Listed], ours: &[Ours]) -> (Vec<&'static str>, usize) {
    let mut names = Vec::new();
    let mut others = 0;
    for row in listed {
        let by_venue = ours.iter().find(|o| o.venue.as_ref() == Some(&row.order));
        let by_cid =
            || (ours.iter()).find(|o| o.venue.is_none() && row.client == CidMatch::Ours(o.cid));
        match by_venue.or_else(by_cid) {
            Some(o) => names.push(o.name),
            None => others += 1,
        }
    }
    (names, others)
}

/// What `attempt`, a page of the pager, came to, by names alone ([`named`]), and the stop it is:
/// [`Stop::RateRefused`] on [`RefusedOn::Pager`] for a provider rate refusal, and
/// [`Stop::ReadDisagrees`] for a page listing an order that is none of `ours`, or a TWAP: a read
/// showing an order the streams did not show (decision 0148 item 8), which stops the run before
/// the plan's next action (PR #284's review). None otherwise.
pub fn came(attempt: &PageAttempt, ours: &[Ours]) -> (Came, Option<Stop>) {
    match attempt {
        PageAttempt::Read {
            listed,
            twaps,
            full,
            ..
        } => {
            let (ours, others) = named(listed, ours);
            let stop = (others > 0 || *twaps > 0).then_some(Stop::ReadDisagrees);
            let came = Came::Read {
                ours,
                others,
                twaps: *twaps,
                full: *full,
            };
            (came, stop)
        }
        PageAttempt::Skipped => (Came::Skipped, None),
        PageAttempt::Failed(_) => (Came::Failed, None),
        PageAttempt::Refused => (
            Came::Refused,
            Some(Stop::RateRefused {
                on: RefusedOn::Pager,
            }),
        ),
    }
}

/// When the run first saw each of stage three's orders ended in the streams, by name, and whether
/// a comparison begun after then has ended since: what a page listing one is judged by
/// ([`Ends::past`]; PR #284's review).
///
/// `compare::judge` counts an order of ours a read lists that the streams showed ended as a
/// disagreement, and `compare::compare` allows it as lag only within its wait: a read after the
/// step may still list the order until a read agrees or `--agree-wait-ms` from the comparison's
/// start passes, and the comparison then stops the run, [`Stop::ReadDisagrees`], if its last
/// read still lists it (decisions 0109 and 0151 item 4). A page is held to the same rule: one of
/// ours it lists that the streams showed ended is within its lag until a comparison begun after
/// the streams showed it ended has ended, or until `--agree-wait-ms` from then has passed, the
/// lag measured from the stream's event as 0109 measures it; past either, it is the run's stop.
#[derive(Clone, Debug)]
pub struct Ends {
    wait: Duration,
    /// Each order's name, when the run first saw it ended, and whether a comparison begun after
    /// then has ended since.
    seen: Vec<(&'static str, Instant, bool)>,
}

impl Ends {
    /// `wait`, `--agree-wait-ms`: how long a read may lag the streams (0151 item 4).
    pub fn new(wait: Duration) -> Ends {
        Ends {
            wait,
            seen: Vec::new(),
        }
    }

    /// Notes `ended`, the names of ours the streams show ended `now`, each when first seen.
    pub fn note(&mut self, ended: &[&'static str], now: Instant) {
        for name in ended {
            if !self.seen.iter().any(|(n, ..)| n == name) {
                self.seen.push((name, now, false));
            }
        }
    }

    /// The names the run has seen ended so far, in the order it saw them: what a comparison
    /// beginning now covers once it has ended ([`Ends::compared`]).
    pub fn ended(&self) -> Vec<&'static str> {
        self.seen.iter().map(|(n, ..)| *n).collect()
    }

    /// Marks `names`, seen ended before a comparison began, past their lag: that comparison has
    /// ended, by a read that agreed or at the end of its wait.
    pub fn compared(&mut self, names: &[&'static str]) {
        for seen in &mut self.seen {
            if names.contains(&seen.0) {
                seen.2 = true;
            }
        }
    }

    /// The names past their lag `at`: a comparison begun after the run saw them ended has ended,
    /// or `--agree-wait-ms` from then has passed.
    pub fn past(&self, at: Instant) -> Vec<&'static str> {
        (self.seen.iter())
            .filter(|(_, seen, compared)| *compared || at >= *seen + self.wait)
            .map(|(n, ..)| *n)
            .collect()
    }
}

/// One of ours a page listed that the streams showed ended as it was asked: the page, by its
/// place among the pages the pager asked for, the order's name, and whether it was past its lag
/// ([`Ends::past`]), the run's stop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndedListed {
    pub page: usize,
    pub name: &'static str,
    pub past: bool,
}

/// What `came`, the `page`th page, lists of ours that the streams showed ended as it was asked,
/// `ended`, each past its lag when `past` names it ([`Ends`]), and the stop it is:
/// [`Stop::ReadDisagrees`] when any is past its lag, as `compare::compare` stops on such a read at
/// the end of its wait (decision 0148 item 8). None otherwise.
pub fn ended_listed(
    came: &Came,
    page: usize,
    (ended, past): (&[&'static str], &[&'static str]),
) -> (Vec<EndedListed>, Option<Stop>) {
    let Came::Read { ours, .. } = came else {
        return (Vec::new(), None);
    };
    let listed: Vec<EndedListed> = (ours.iter())
        .filter(|name| ended.contains(name))
        .map(|name| EndedListed {
            page,
            name,
            past: past.contains(name),
        })
        .collect();
    let stop = listed.iter().any(|l| l.past).then_some(Stop::ReadDisagrees);
    (listed, stop)
}

/// Whether the last page `run` recorded was its stop for what it listed: an order or a TWAP not
/// ours, or one of ours the streams showed ended past its lag ([`WalkEnd::Disagreed`]).
pub fn last_disagreed(run: &PagesRun) -> bool {
    let at = run.pages.len().checked_sub(1);
    let foreign = (run.pages.last())
        .is_some_and(|p| matches!(p.came, Came::Read { others, twaps, .. } if others + twaps > 0));
    let stale = (run.ended_listed.iter()).any(|l| Some(l.page) == at && l.past);
    foreign || stale
}

/// What the report says of a page's listing of ours the streams showed ended, `listed`: nothing
/// when it listed none.
pub fn ended_said(listed: &[&EndedListed]) -> String {
    let each: Vec<String> = (listed.iter())
        .map(|l| match l.past {
            true => format!(
                "{}, which the streams showed ended past its lag: the run's stop, ReadDisagrees \
                 (decision 0148 item 8)",
                l.name
            ),
            false => format!(
                "{}, which the streams showed ended within its lag (decisions 0109 and 0151 item \
                 4): no stop",
                l.name
            ),
        })
        .collect();
    match each.is_empty() {
        true => String::new(),
        false => format!("; it listed {}", each.join("; and ")),
    }
}

/// The order QFEX's pages list our orders in, as page 0 showed it with P1 and P2 open and P1
/// placed first: oldest first when it listed P1, newest first when it listed P2, and `None` when
/// no such page was read.
pub fn order_shown(pages: &[PageRead]) -> Option<&'static str> {
    let first = pages.iter().find(|p| !p.walk && p.offset == 0)?;
    if first.open != ["P1", "P2"] {
        return None;
    }
    match &first.came {
        Came::Read { ours, .. } if ours == &["P1"] => Some("oldest first"),
        Came::Read { ours, .. } if ours == &["P2"] => Some("newest first"),
        _ => None,
    }
}

/// One order's cancel in stage three, by name alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CancelRun {
    pub name: &'static str,
    /// Whether the cancel was handed to the session.
    pub sent: bool,
    /// Why it was not, when it was not: the order had ended, or its acknowledgement had not come.
    pub not_sent: Option<&'static str>,
    /// Its request's outcome, as the session reported it.
    pub outcome: Option<String>,
    /// Whether its order was seen to end by `--rpc-timeout-ms` from the cancel's send.
    pub ended: bool,
}

/// Why a cancel was not sent: its order had ended first.
pub const ENDED_FIRST: &str = "its order had ended";
/// Why a cancel was not sent: its order's acknowledgement, which a cancel by venue id needs, had
/// not come; the session's Stop cancels it once it does.
pub const AWAITING_ACK: &str = "its order's acknowledgement had not come";

/// One named account's stage three, as the report and the evidence show it: names, flags,
/// counts, page sizes and offsets alone, never an id, a price or a size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PagesRun {
    /// The session, as stage two's sessions are recorded.
    pub session: CrossRun,
    /// Each order placed, P1 to P3, and its request.
    pub orders: Vec<(&'static str, XRun)>,
    pub cancels: Vec<CancelRun>,
    /// Each page the pager asked for, in order.
    pub pages: Vec<PageRead>,
    /// Each order of ours a page listed that the streams showed ended as it was asked
    /// ([`ended_listed`]).
    pub ended_listed: Vec<EndedListed>,
    /// How the walk ended, once it began.
    pub walk: Option<WalkEnd>,
    /// The provider rate refusals the pager's socket met, as a read socket's are counted.
    pub refusals: Refusals,
    /// Whether the pager asked for any page.
    pub paged: bool,
    /// The frames the pager read, as a journal would keep them: for the `pages-production/`
    /// slice alone, never the report or the evidence.
    pub frames: Vec<String>,
}

/// What follows a stage-three session that did not stop the run, on an account before the last.
pub const NEXT_ACCOUNT: &str = "the next named account's crossing session, which starts only \
     after every named account reads clean again and no registry of the run holds an order of \
     ours (decision 0148 item 6)";

/// What follows the last named account's stage-three session that did not stop the run: the
/// run's end ([`crate::stage::FINISHED`]).
pub const LAST_PAGES: &str = "the run's end: every place stage ran, and the report answers \
     decision 0132's list for each named account";

/// What the report says of an order resting after a stage-three session, for the owner
/// (decision 0132 item 7, on production's site as decision 0143 item 4 reads it).
pub const HAND_CANCEL_PAGES: &str = "an order rests on the account after the stage-three session: \
     cancel it by hand on QFEX production's site, then give the next place-stage run \
     --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What the report says of an order the pages registry still counts resting after its session,
/// which no read listed (the owner's ruling under 0151 item 6 on FBC-gisx).
pub const HAND_CANCEL_PAGES_COUNTED: &str = "the pages registry still counts an order of ours \
     resting after the stage-three session, which no read listed (counted resting, not seen \
     resting): read the account on QFEX production's site, cancel any order of ours there by \
     hand, then give the next place-stage run --reconciled hand-cancel (0151 item 6)";

/// What the report says when the reads after a stage-three session list an order or a TWAP not
/// ours (decision 0148 item 8).
pub const OTHERS_PAGES: &str = "an order or a TWAP that is not ours is listed on a named account \
     after the stage-three session, which the streams did not show: a stop, ReadDisagrees \
     (decision 0148 item 8); read the accounts on QFEX's site before the next run";

/// When the reads after a stage-three session listed an order or a TWAP not ours.
pub const PAGES_OTHERS: &str =
    "after the stage-three session, a read listing an order or a TWAP not ours";

/// The report's section on stage three: each named account's session, its pages and what
/// followed it, given the accounts whose crossing sessions began, in run order, `crossed`, and
/// whether the run reached its end, `finished`.
pub fn markdown(s: &mut String, runs: &[PagesRun], (crossed, finished): (&[&str], bool)) {
    use crate::places::{echoes_said, placed_said, yn};
    let _ = writeln!(
        s,
        "## Stage three (FBC-4plj, decision 0132 item 7)\n\n\
         On each named account whose drop test passed in this run, with the exact-echo gate \
         open, after its taker IOC session: its pages registry built after clean reads of every \
         named account, its CancelOnlyTrip given to the journal, seeded flat; one session armed, \
         resynced and Started, which places P1 and P2, each one post-only GTC at the \
         instrument's minimum size --away-bps behind the touch, then P3, then cancels P1, then \
         P2 and P3, by venue id, each place and cancel compared with never-arming reads; beside \
         it, the pager, a socket that never arms, asks for get_user_orders at a page size of {} \
         between them (page 0 after P1 and P2, page 1 after P3, pages 2 and 3 after P1's \
         cancel), then every page from offset 0 until a short one (decision 0132 item 5's \
         evidence). No id, price or size is shown.\n",
        PAGE_SIZE
    );
    for run in runs {
        let session = &run.session;
        // A later account's crossing session began after this one.
        let at = crossed.iter().position(|a| *a == session.account);
        let next = at.is_some_and(|at| at + 1 < crossed.len());
        let followed = match (session.stop, next, finished) {
            (Some(stop), ..) => crate::stage_two::stopped(stop),
            (None, true, _) => NEXT_ACCOUNT.to_owned(),
            (None, false, true) => LAST_PAGES.to_owned(),
            (None, false, false) => crate::taker::RUN_ENDED.to_owned(),
        };
        let _ = writeln!(
            s,
            "### Stage three on {}\n\n\
             - Stream authenticated: {}; what answered the arm: {}; resync applied: {}; market \
               Started under its market lease: {}\n\
             - The order QFEX's pages list ours in, as page 0 showed it: {}\n\
             - The walk from offset 0: {}\n\
             - Ended by: {}\n\
             - An order resting on the account when the session ended: {}\n\
             - Our client id as QFEX gave it back, frame by frame: {}\n\
             - Error: {}\n\
             - What followed: {followed}\n",
            session.account,
            yn(session.authenticated),
            session.arm.as_deref().unwrap_or("none reported"),
            yn(session.resynced),
            yn(session.started),
            order_shown(&run.pages).unwrap_or("not shown"),
            run.walk.map_or("not begun", WalkEnd::says),
            session.ended_by.unwrap_or("it did not open"),
            crate::stage_two::resting_said(session, crate::stage_two::Session::Pages),
            echoes_said(&session.echoes),
            session.error.as_deref().unwrap_or("none"),
        );
        for (name, placed) in &run.orders {
            let _ = writeln!(s, "- {name}: {}", placed_said(placed));
        }
        for c in &run.cancels {
            let _ = writeln!(
                s,
                "- {}'s cancel: handed to the session {}{}; its outcome {}; its order seen to \
                 end {}",
                c.name,
                yn(c.sent),
                c.not_sent.map_or(String::new(), |why| format!(" ({why})")),
                c.outcome.as_deref().unwrap_or("none reported"),
                yn(c.ended)
            );
        }
        for (at, page) in run.pages.iter().enumerate() {
            let ended: Vec<&EndedListed> =
                run.ended_listed.iter().filter(|l| l.page == at).collect();
            let _ = writeln!(
                s,
                "- {}page at size {}, offset {} (open in the streams: {}): {}{}",
                if page.walk { "The walk's " } else { "A " },
                page.size,
                page.offset,
                match page.open.is_empty() {
                    true => "none".to_owned(),
                    false => page.open.join(", "),
                },
                page.came.says(),
                ended_said(&ended)
            );
        }
        let _ = writeln!(s);
        if !session.compared.is_empty() {
            crate::places::compared_markdown(s, &session.compared);
        }
        if let Some(flatten) = &session.flatten {
            crate::flatten::markdown(s, flatten);
        }
    }
}

/// The evidence's entry for stage three: no frame, no id, no amount.
pub fn to_json(runs: &[PagesRun]) -> Value {
    use crate::places::{compared_json, placed_json};
    let each: Vec<Value> = (runs.iter())
        .map(|run| {
            let session = &run.session;
            let orders: Vec<Value> = (run.orders.iter())
                .map(|(name, placed)| json!({"name": name, "placed": placed_json(placed)}))
                .collect();
            let cancels: Vec<Value> = (run.cancels.iter())
                .map(|c| {
                    json!({"name": c.name, "sent": c.sent, "not_sent": c.not_sent,
                        "outcome": c.outcome, "ended": c.ended})
                })
                .collect();
            let pages: Vec<Value> = (run.pages.iter())
                .map(|p| {
                    json!({"walk": p.walk, "size": p.size, "offset": p.offset,
                        "open": p.open, "came": p.came.says()})
                })
                .collect();
            let ended: Vec<Value> = (run.ended_listed.iter())
                .map(|l| json!({"page": l.page, "name": l.name, "past_its_lag": l.past}))
                .collect();
            let compared: Vec<Value> = session.compared.iter().map(compared_json).collect();
            json!({"account": session.account, "authenticated": session.authenticated,
                "arm": session.arm, "resynced": session.resynced, "started": session.started,
                "orders": orders, "cancels": cancels, "pages": pages, "ended_listed": ended,
                "page_order": order_shown(&run.pages),
                "walk": run.walk.map(WalkEnd::says), "compared": compared,
                "ended_by": session.ended_by, "resting": session.resting,
                "counted_resting": session.counted_resting, "read_after": session.read,
                "hand_cancel": crate::stage_two::hand_cancel(session, Session::Pages),
                "others_after": session.others, "not_flat_after": session.not_flat_after,
                "error": session.error, "stop": session.stop,
                "flatten": session.flatten.as_ref().map(crate::flatten::to_json)})
        })
        .collect();
    json!(each)
}
