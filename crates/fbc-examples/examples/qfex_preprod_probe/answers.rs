//! Decision 0132's list, answered for each named account (FBC-4plj; decision 0132's
//! Consequences, 0148 item 10): what the place stages' report answers, for the primary account
//! and the subaccount alike, each item in words or as fences met, from what the run showed, and
//! "not measured" where it did not, its reason given. It reads the run's [`Evidence`] alone: the
//! drop test ([`crate::places`]), stage two ([`crate::stage_two`], [`crate::taker`]), stage three
//! ([`crate::pager`]), the account reads, the sessions' journals ([`crate::capture`]) and the
//! registries ([`crate::ledger`]).
//!
//! Some items are the run's, not an account's: the client ids, how QFEX echoes them, how long
//! `ACK` takes, the statuses and whether `add_order` went out as encoded are read from the whole
//! run's frames, every named account's sessions together, so each account's list gives the
//! run's answer and says so ([`RUN_WIDE`]).
//!
//! No id, no price, no size, no amount and no value of a row reaches it: counts, status names,
//! member names, kinds, flags and times in whole milliseconds alone (decisions 0009 and 0148 item
//! 10).

use std::fmt::Write as _;

use serde_json::{Map, Value, json};

use crate::capture::{AccountRead, Evidence, SessionRun};
use crate::compare::{Compared, Ended};
use crate::pager::{Came, PagesRun};
use crate::places::{DropRun, Sides, TradeIds, Traded, XAfter};
use crate::stage_two::{CrossRun, StageTwo};

/// What an item read from the whole run's frames says first.
pub const RUN_WIDE: &str = "for the run, every named account's sessions together:";

/// The items, in the order decision 0132's Consequences lists them, then those decision 0148
/// item 10 and the ticket add.
pub const ITEMS: [&str; 23] = [
    "What answers an arm",
    "Whether enabling cancels open orders, and whether those cancels are reported",
    "The drop test (decision 0132 item 4)",
    "Whether a graceful close cancels",
    "Whether every other named account stayed clean (decision 0144 item 3)",
    "Whether QFEX accepts the probe mode's ClientIdFormat at its full length and in every \
     character it uses",
    "Whether ACK, rejects and fills echo a sent client id exactly, byte for byte",
    "How long ACK takes, for post-only and other places, and against the speed bump's hold",
    "Which statuses are rejects and ends, and how QFEX refuses a crossing post-only",
    "What GET /user/historic-orders shows for each order of ours that ended",
    "What kind of time a trade's timestamp is, and whether a fill's trade_id is the same in the \
     fills channel and in get_user_trades",
    "Which side field a get_user_trades row carries, and whether it is ours or the aggressor's",
    "How the pages behave at their boundaries",
    "What open_orders counts",
    "The lag between the stream and the reads",
    "Whether QFEX accepted add_order as encoded (FBC-blcw)",
    "The order of the taker IOC's frames (FBC-tfxk)",
    "The taker fee's sign (decision 0148 item 10)",
    "Each resync against REST's open_orders (decision 0132 item 5's cross-check)",
    "Each resync's full pages, by page size and offset (FBC-4h1p)",
    "get_user_trades rows naming an order the codec did not place (decision 0132 item 5)",
    "The provider rate refusals, by session and read socket (decision 0135 item 2)",
    "The fill ledgers' bounds, horizons and replays (decision 0132 item 5)",
];

/// One named account's runs, as the evidence holds them.
struct Of<'a> {
    account: &'a str,
    drop: Option<&'a DropRun>,
    cross: Option<&'a CrossRun>,
    take: Option<&'a CrossRun>,
    pages: Option<&'a PagesRun>,
    two: Option<&'a StageTwo>,
    sessions: Vec<&'a SessionRun>,
    reads: Vec<&'a AccountRead>,
}

impl<'a> Of<'a> {
    fn new(ev: &'a Evidence, sessions: &'a [SessionRun], account: &'a str) -> Of<'a> {
        let two = ev.stage_two.as_ref();
        let find = |runs: Option<&'a Vec<CrossRun>>| {
            runs.and_then(|runs| runs.iter().find(|r| r.account == account))
        };
        Of {
            account,
            drop: ev.drops.iter().find(|d| d.account == account),
            cross: find(two.map(|t| &t.crosses)),
            take: find(two.map(|t| &t.takes)),
            pages: ev.stage_three.iter().find(|p| p.session.account == account),
            two,
            sessions: sessions.iter().filter(|s| s.account == account).collect(),
            reads: ev
                .reads
                .iter()
                .filter(|r| of_account(&r.label, account))
                .collect(),
        }
    }

    /// The stage-two and stage-three sessions, each with its name.
    fn staged(&self) -> Vec<(&'static str, &'a CrossRun)> {
        let named = [
            (crate::places::CROSS, self.cross),
            (crate::places::TAKE, self.take),
            (crate::places::PAGES, self.pages.map(|p| &p.session)),
        ];
        named
            .into_iter()
            .filter_map(|(n, r)| r.map(|r| (n, r)))
            .collect()
    }
}

/// Whether `label`, an account read's or a read socket's, is `account`'s: its first word.
fn of_account(label: &str, account: &str) -> bool {
    label.split(' ').next() == Some(account)
}

/// Each item's answer for `account`, in [`ITEMS`]' order.
pub fn answers(ev: &Evidence, sessions: &[SessionRun], account: &str) -> Vec<String> {
    let of = Of::new(ev, sessions, account);
    vec![
        arm(&of),
        drop_item(&of, crate::places::enabling_said),
        drop_item(&of, |d| {
            format!(
                "{}; the drop's reach over X, an order of an earlier connection: {}",
                crate::places::verdict_said(d),
                crate::places::reach_said(d)
            )
        }),
        drop_item(&of, crate::places::close_said),
        drop_item(&of, |d| {
            crate::places::others_said(d.others_clean).to_owned()
        }),
        run_wide(of.two, |t| {
            let lengths = t.lengths.as_ref().map_or_else(
                || "not measured: no client id was sent".to_owned(),
                crate::stage_two::FullLength::says,
            );
            let chars = (t.characters.as_ref()).map_or_else(
                || "not measured".to_owned(),
                crate::stage_two::Characters::says,
            );
            format!("{lengths}; the characters: {chars}")
        }),
        run_wide(of.two, |t| {
            let said: Vec<String> = (t.echoes.says().into_iter())
                .map(|(kind, said)| format!("{kind}: {said}"))
                .collect();
            said.join("; ")
        }),
        run_wide(of.two, |t| t.acks.says()),
        statuses(&of),
        historic(&of),
        trade_time(&of),
        sides(&of),
        pages(of.pages),
        open_orders(&of),
        lag(&of),
        run_wide(of.two, |t| {
            (t.as_encoded).map_or("not measured: no add_order was sent".to_owned(), |a| {
                a.says().to_owned()
            })
        }),
        frame_order(of.take),
        match of.take.and_then(|t| t.taken.as_ref()) {
            Some(taken) => crate::taker::fee_said(&taken.fees),
            None => "not measured: no taker IOC was placed on this account".to_owned(),
        },
        resyncs(&of),
        full_pages(&of),
        foreign(&of),
        refusals(ev, &of),
        ledgers(ev, account),
    ]
}

/// What answered each session's arm on the account.
fn arm(of: &Of<'_>) -> String {
    let said = |arm: Option<&String>| arm.map_or("none reported".to_owned(), String::clone);
    let mut each: Vec<String> = Vec::new();
    if let Some(d) = of.drop {
        each.push(format!(
            "{}: {}",
            crate::places::DROP_ONE,
            said(d.arm.as_ref())
        ));
        if let Some(two) = &d.two {
            each.push(format!(
                "{}: {}",
                crate::places::DROP_TWO,
                said(two.arm.as_ref())
            ));
        }
    }
    for (name, run) in of.staged() {
        each.push(format!("{name}: {}", said(run.arm.as_ref())));
    }
    match each.is_empty() {
        true => "not measured: no session armed on this account".to_owned(),
        false => each.join("; "),
    }
}

/// An item the drop test answers, `said` of the account's drop test, or not measured.
fn drop_item(of: &Of<'_>, said: impl Fn(&DropRun) -> String) -> String {
    match of.drop {
        Some(d) => said(d),
        None => "not measured: no drop test ran on this account".to_owned(),
    }
}

/// An item read from the whole run's frames: `said` of stage two's record, after
/// [`RUN_WIDE`], or not measured when no drop session ran.
fn run_wide(two: Option<&StageTwo>, said: impl Fn(&StageTwo) -> String) -> String {
    match two {
        Some(two) => format!("{RUN_WIDE} {}", said(two)),
        None => "not measured: no drop session ran".to_owned(),
    }
}

/// The run's rejects and ends, and how the account's crossing post-only was refused.
fn statuses(of: &Of<'_>) -> String {
    let crossed = match of.cross {
        Some(c) => match &c.crossed {
            Some(crossed) => crossed.says(),
            None => "not measured: no crossing post-only was placed".to_owned(),
        },
        None => "not measured: no crossing session ran on this account".to_owned(),
    };
    let run = run_wide(of.two, |t| t.statuses.says());
    format!("{run}; this account's crossing post-only: {crossed}")
}

/// The `terminal_status` of each order read on historic-orders.
fn historic(of: &Of<'_>) -> String {
    let mut each: Vec<String> = Vec::new();
    if let Some(d) = of.drop {
        each.push(format!("X {}", crate::places::terminal_said(d, 0)));
        if d.two.as_ref().is_some_and(|t| t.y.submitted) {
            each.push(format!("Y {}", crate::places::terminal_said(d, 1)));
        }
    }
    if let Some(traded) = of.take.and_then(|t| t.traded.as_ref()) {
        let status = traded.terminal.first().cloned().flatten();
        let status = status.unwrap_or_else(|| match traded.historic_whole {
            true => "not listed".to_owned(),
            false => "not read".to_owned(),
        });
        each.push(format!("the taker IOC {status}"));
        let flattens = traded.terminal.iter().skip(1);
        let listed = flattens.clone().filter(|t| t.is_some()).count();
        // A flatten IOC not listed on pages not all read is not shown unlisted (PR #284's
        // review).
        if traded.terminal.len() > 1 {
            each.push(format!(
                "its flatten IOCs {listed} of {} listed{}",
                traded.terminal.len() - 1,
                match traded.historic_whole {
                    true => "",
                    false => ", on GET /user/historic-orders pages not all read",
                }
            ));
        }
    }
    if of.pages.is_some() {
        each.push("stage three's orders not read (no historic-orders read follows it)".to_owned());
    }
    match each.is_empty() {
        true => "not measured: no order of ours was read on GET /user/historic-orders".to_owned(),
        false => each.join("; "),
    }
}

/// What the account's trades reads kept: drop session one's, after the drop, and after the
/// taker IOC session, as `(time kinds, trade ids, sides as a maker, sides as a taker, rows not
/// ours, fields, whether any page was read, and each read not read whole)`.
struct Rows {
    kinds: Vec<&'static str>,
    ids: TradeIds,
    maker: Sides,
    taker: Sides,
    foreign: usize,
    fields: Vec<String>,
    /// Whether any read of the account read a `get_user_trades` page.
    read: bool,
    /// Each read whose pages were not all read, to a short page, by when it was made and why:
    /// what it shows is of the pages read alone, never of the whole history (PR #284's review).
    unwhole: Vec<String>,
}

/// One trades read of the account, as [`rows`] adds it up: when it was made, whether every page
/// was read to a short one (`whole`), the rows it read, and why it could not be made whole.
struct History<'a> {
    when: &'static str,
    whole: bool,
    trades: usize,
    error: Option<&'a str>,
}

impl History<'_> {
    /// Whether it read any page: a read not whole that read one read a full page, so a row.
    fn paged(&self) -> bool {
        self.whole || self.trades > 0
    }

    /// Why it is not whole, by when it was made: its error, or none given.
    fn unwhole(&self) -> Option<String> {
        let why = self.error.unwrap_or("its pages were not all read");
        (!self.whole).then(|| format!("{}: {why}", self.when))
    }
}

/// When each trades read of the account was made, as [`Rows::unwhole`] names it.
const AFTER_ONE: &str = "after drop session one";
const AFTER_DROP: &str = "after the drop";
const AFTER_TAKE: &str = "after the taker IOC session";

fn rows(of: &Of<'_>) -> Rows {
    let mut rows = Rows {
        kinds: Vec::new(),
        ids: TradeIds::default(),
        maker: Sides::default(),
        taker: Sides::default(),
        foreign: 0,
        fields: Vec::new(),
        read: false,
        unwhole: Vec::new(),
    };
    let mut add = |kinds: &[&'static str],
                   ids: TradeIds,
                   foreign: usize,
                   fields: &[String],
                   history: History<'_>| {
        rows.unwhole.extend(history.unwhole());
        if !history.paged() {
            return;
        }
        for kind in kinds {
            if !rows.kinds.contains(kind) {
                rows.kinds.push(kind);
            }
        }
        for field in fields {
            if !rows.fields.contains(field) {
                rows.fields.push(field.clone());
            }
        }
        rows.ids = rows.ids.plus(ids);
        rows.foreign += foreign;
        rows.read = true;
    };
    let mut maker = Sides::default();
    if let Some(d) = of.drop {
        if let Some(XAfter {
            time_kinds,
            trade_ids,
            sides,
            foreign,
            fields,
            trades,
            trades_read,
            error,
            ..
        }) = &d.after
        {
            let history = History {
                when: AFTER_ONE,
                whole: *trades_read,
                trades: *trades,
                error: error.as_deref(),
            };
            add(time_kinds, *trade_ids, *foreign, fields, history);
            maker = plus(maker, *sides);
        }
        if let Some(t) = d.two.as_ref().and_then(|t| t.after.as_ref()) {
            add(
                &t.time_kinds,
                t.trade_ids,
                t.foreign,
                &t.fields,
                traded(AFTER_DROP, t),
            );
            maker = plus(maker, t.sides);
        }
    }
    let mut taker = Sides::default();
    if let Some(t) = of.take.and_then(|t| t.traded.as_ref()) {
        add(
            &t.time_kinds,
            t.trade_ids,
            t.foreign,
            &t.fields,
            traded(AFTER_TAKE, t),
        );
        taker = t.sides;
    }
    rows.maker = maker;
    rows.taker = taker;
    rows
}

/// A [`Traded`] read, made `when`, as a [`History`].
fn traded<'a>(when: &'static str, t: &'a Traded) -> History<'a> {
    History {
        when,
        whole: t.trades_whole,
        trades: t.trades,
        error: t.error.as_deref(),
    }
}

impl Rows {
    /// What an item read from the rows says when no read of the account read a page: no read
    /// made, or those made, each with why, none reading a page.
    fn not_read(&self) -> Option<String> {
        if self.read {
            return None;
        }
        Some(match self.unwhole.is_empty() {
            true => "not measured: no get_user_trades read was made on this account".to_owned(),
            false => format!(
                "not measured: no get_user_trades page was read on this account ({})",
                self.unwhole.join("; ")
            ),
        })
    }

    /// What an item read from the rows adds when a read of the account was not read whole: what
    /// it says is of the pages read alone. Empty when every read was whole.
    fn partly(&self) -> String {
        match self.unwhole.is_empty() {
            true => String::new(),
            false => format!(
                "; of the pages read alone, since a get_user_trades read was not read whole ({})",
                self.unwhole.join("; ")
            ),
        }
    }
}

/// Two counts of sides added up.
fn plus(a: Sides, b: Sides) -> Sides {
    let add = |x: [usize; 3], y: [usize; 3]| [x[0] + y[0], x[1] + y[1], x[2] + y[2]];
    Sides {
        side: add(a.side, b.side),
        aggressor: add(a.aggressor, b.aggressor),
    }
}

/// The kind of a trade's `timestamp`, and whether its `trade_id` is the fills channel's.
fn trade_time(of: &Of<'_>) -> String {
    let rows = rows(of);
    if let Some(not_read) = rows.not_read() {
        return not_read;
    }
    let kinds = match rows.kinds.is_empty() {
        true => "not measured: no get_user_trades row naming an order of ours was read".to_owned(),
        false => format!(
            "{}; whether it is the matching engine's time its magnitude does not show",
            rows.kinds.join(", and ")
        ),
    };
    format!(
        "the timestamp: {kinds}; the trade_id: {}{}",
        rows.ids.says(),
        rows.partly()
    )
}

/// How one role's rows stated our side, as [`sides`] says it.
fn stated(sides: &Sides) -> String {
    let [ours, other, absent] = sides.side;
    let [a_ours, a_other, a_absent] = sides.aggressor;
    format!(
        "side ours on {ours}, the other side on {other}, absent on {absent}; aggressor_side ours \
         on {a_ours}, the other side on {a_other}, absent on {a_absent}"
    )
}

/// Which side fields a row carries, and whether `side` is ours or the aggressor's: told apart
/// only on a row of an order of ours that was the maker, since on a taker's row the two are one.
fn sides(of: &Of<'_>) -> String {
    let rows = rows(of);
    if let Some(not_read) = rows.not_read() {
        return not_read;
    }
    let carried: Vec<&str> = (rows.fields.iter())
        .filter(|f| f.contains("side"))
        .map(String::as_str)
        .collect();
    let carried = match (carried.is_empty(), rows.fields.is_empty()) {
        (_, true) => "not shown: no row was read".to_owned(),
        (true, false) => "none: no member whose name holds side".to_owned(),
        (false, false) => carried.join(" and "),
    };
    let maker = rows.maker;
    let whose = match (maker.rows(), maker.side) {
        (0, _) if rows.taker.rows() == 0 => {
            "not measured: no row naming an order of ours was read".to_owned()
        }
        (0, _) => format!(
            "not told apart: on the taker IOC's {} row{} ({}) ours is the aggressor's, and no \
             row of a maker fill of ours was read",
            rows.taker.rows(),
            if rows.taker.rows() == 1 { "" } else { "s" },
            stated(&rows.taker)
        ),
        (n, [ours, 0, 0]) if ours == n => {
            format!(
                "ours: on {n} row{} of a maker fill of ours, side was our side",
                s(n)
            )
        }
        (n, [0, other, 0]) if other == n => format!(
            "the aggressor's: on {n} row{} of a maker fill of ours, side was the other side",
            s(n)
        ),
        (n, _) => format!(
            "not told apart: on {n} row{} of a maker fill of ours, {}",
            s(n),
            stated(&maker)
        ),
    };
    format!("carried: {carried}; side is {whose}{}", rows.partly())
}

fn s(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// How the pager's pages behaved at their boundaries.
fn pages(run: Option<&PagesRun>) -> String {
    let Some(run) = run else {
        return "not measured: stage three did not run on this account".to_owned();
    };
    if run.pages.is_empty() {
        return "not measured: the pager asked for no page".to_owned();
    }
    let read: Vec<&crate::pager::PageRead> = (run.pages.iter())
        .filter(|p| matches!(p.came, Came::Read { .. }))
        .collect();
    let full = (read.iter())
        .filter(|p| matches!(p.came, Came::Read { full: true, .. }))
        .count();
    // A page listing one of ours the streams did not show open as it was asked: a no only of
    // the pages read, and not measured when none was (PR #284's review).
    let ended = read.iter().any(|p| match &p.came {
        Came::Read { ours, .. } => ours.iter().any(|o| !p.open.contains(o)),
        _ => false,
    });
    let ended = match (ended, read.len()) {
        (true, _) => "yes".to_owned(),
        (false, 0) => "not measured: no page was read".to_owned(),
        (false, n) if n == run.pages.len() => "no".to_owned(),
        (false, n) => format!("none on the {n} page{} read", s(n)),
    };
    format!(
        "{} page{} asked at a page size of {}, {} read, {full} of them full; the order QFEX's \
         pages list ours in: {}; a page listing an order of ours the streams did not show open: \
         {}; the walk from offset 0: {}",
        run.pages.len(),
        s(run.pages.len()),
        crate::pager::PAGE_SIZE,
        read.len(),
        crate::pager::order_shown(&run.pages).unwrap_or("not shown"),
        ended,
        run.walk.map_or("not begun", crate::pager::WalkEnd::says)
    )
}

/// What REST's `open_orders` counted against the orders the same read's page listed, on each
/// read of the account whose page was not full, so listed every order there.
fn open_orders(of: &Of<'_>) -> String {
    let whole: Vec<&AccountRead> = (of.reads.iter().copied())
        .filter(|r| !r.page_full)
        .collect();
    if whole.is_empty() {
        return "not measured: no read of this account listed a whole page".to_owned();
    }
    let equal = |r: &&&AccountRead| r.rest_open_orders == r.page_orders as u64;
    let agreed = whole.iter().filter(equal).count();
    let with_order = whole
        .iter()
        .filter(|r| equal(r) && r.page_orders > 0)
        .count();
    let kind = match (agreed == whole.len(), with_order > 0) {
        (true, true) => "the open orders the pages list, on every whole read, with an order open",
        (true, false) => {
            "nothing open on every whole read, as the pages list nothing: not shown \
                          with an order open"
        }
        (false, _) => "not what the pages list on every whole read",
    };
    format!(
        "{kind}: it equalled the orders the read's page listed on {agreed} of {} whole reads of \
         this account, {with_order} of them with an order open",
        whole.len()
    )
}

/// The comparisons of the streams with the reads on the account's sessions.
fn lag(of: &Of<'_>) -> String {
    let mut compared: Vec<&Compared> = Vec::new();
    if let Some(d) = of.drop {
        compared.extend(&d.compared);
        compared.extend(d.two.iter().flat_map(|t| &t.compared));
    }
    for (_, run) in of.staged() {
        compared.extend(&run.compared);
    }
    if compared.is_empty() {
        return "not measured: no comparison ran on this account".to_owned();
    }
    let agreed: Vec<u128> = (compared.iter())
        .filter(|c| c.ended == Ended::Agreed)
        .filter_map(|c| c.first_agreed.map(|d| d.as_millis()))
        .collect();
    let count = |e: Ended| compared.iter().filter(|c| c.ended == e).count();
    let first = match (agreed.iter().min(), agreed.iter().max()) {
        (Some(least), Some(most)) => {
            format!(", the first agreeing read ending from {least} ms to {most} ms after its step")
        }
        _ => String::new(),
    };
    format!(
        "{} comparison{}: {} agreed{first}; {} ended lagged; {} disagreed; {} met a provider \
         rate refusal; {} with no read made",
        compared.len(),
        s(compared.len()),
        agreed.len(),
        count(Ended::Lagged),
        count(Ended::Disagreed),
        count(Ended::Refused),
        count(Ended::Unread)
    )
}

/// The order of the taker IOC's final `order_response` and its `fill_response`s.
fn frame_order(take: Option<&CrossRun>) -> String {
    match take.and_then(|t| t.taken.as_ref()) {
        Some(taken) => (taken.frame_order).map_or(
            "not measured: the taker IOC's frames were not read".to_owned(),
            |o| o.says().to_owned(),
        ),
        None => "not measured: no taker IOC was placed on this account".to_owned(),
    }
}

/// Each session's resync: the orders its pages listed against REST's `open_orders`, compared
/// only when every page it asked for was answered, so pages not all read never read as
/// agreeing or differing (PR #284's review).
fn resyncs(of: &Of<'_>) -> String {
    let each: Vec<String> = (of.sessions.iter())
        .filter(|run| run.pages.read > 0 || run.resync_open_orders.is_some())
        .map(|run| {
            let listed = run.pages.orders;
            let (asked, read) = (run.pages.asked, run.pages.read);
            let said = match run.resync_open_orders {
                Some(open) if asked == 0 => {
                    format!("{open}: not compared, since no page it asked for was journaled")
                }
                Some(open) if read < asked => format!(
                    "{open}: not compared, since {read} of the {asked} pages it asked for were \
                     answered"
                ),
                Some(open) if open == listed => format!("{open}: they agree"),
                Some(open) => format!("{open}: they differ"),
                None => "not journaled".to_owned(),
            };
            format!(
                "{}: its pages listed {listed} order{}, its GET /user/positions counted \
                 open_orders {said}",
                run.label(),
                if listed == 1 { "" } else { "s" }
            )
        })
        .collect();
    match each.is_empty() {
        true => "not measured: no resync on this account was journaled".to_owned(),
        false => each.join("; "),
    }
}

/// Each session's resync's full pages, of the pages answered alone when not every page asked
/// for was (PR #284's review).
fn full_pages(of: &Of<'_>) -> String {
    let each: Vec<String> = (of.sessions.iter())
        .filter(|run| run.pages.asked > 0)
        .map(|run| {
            let (asked, read) = (run.pages.asked, run.pages.read);
            match (read < asked, run.pages.full.is_empty()) {
                (false, _) => format!("{}: {}", run.label(), run.pages.listed()),
                (true, true) => format!(
                    "{}: not measured: none among the {read} of the {asked} pages it asked for \
                     that were answered",
                    run.label()
                ),
                (true, false) => format!(
                    "{}: {}, of the {read} of the {asked} pages it asked for that were answered",
                    run.label(),
                    run.pages.listed()
                ),
            }
        })
        .collect();
    match each.is_empty() {
        true => "not measured: no resync on this account asked for a page".to_owned(),
        false => each.join("; "),
    }
}

/// The trades rows naming none of the orders read for: none only when every read of the
/// account read its pages whole, so a history not read whole never reads as none (PR #284's
/// review).
fn foreign(of: &Of<'_>) -> String {
    let rows = rows(of);
    if let Some(not_read) = rows.not_read() {
        return not_read;
    }
    match (rows.foreign, rows.unwhole.is_empty()) {
        (0, true) => "none".to_owned(),
        (0, false) => format!(
            "not measured: none on the pages read, but a get_user_trades read was not read whole \
             ({})",
            rows.unwhole.join("; ")
        ),
        (n, _) => format!(
            "{n} row{}: an order of another's, or one this run did not name, traded on the \
             account (none is pushed as a fill){}",
            s(n),
            rows.partly()
        ),
    }
}

/// The provider rate refusals the account's sessions and read sockets met.
fn refusals(ev: &Evidence, of: &Of<'_>) -> String {
    let sessions: u64 = of.sessions.iter().map(|r| r.refusals.met()).sum();
    let sockets = (ev.read_sockets.iter()).filter(|r| of_account(&r.label, of.account));
    let (count, met) = sockets.fold((0, 0), |(n, m), r| (n + 1, m + r.refusals.met()));
    format!(
        "at least {sessions} on this account's {} session{} and {met} on its {count} read \
         socket{}, each by name in the provider rate refusals section",
        of.sessions.len(),
        s(of.sessions.len()),
        s(count)
    )
}

/// The account's place-stage registries' ledgers.
fn ledgers(ev: &Evidence, account: &str) -> String {
    let each: Vec<String> = (ev.place_registries.iter())
        .filter(|r| r.account() == account)
        .map(crate::ledger::PlaceRun::said)
        .collect();
    match each.is_empty() {
        true => "not measured: no registry was built on this account".to_owned(),
        false => each.join("; "),
    }
}

/// The report's section: the run's own lines, then each named account's list.
pub fn markdown(s: &mut String, ev: &Evidence, sessions: &[SessionRun]) {
    let _ = writeln!(
        s,
        "## Decision 0132's list, for each named account (FBC-4plj, decision 0148 item 10)\n\n\
         Each item in words or as fences met, never an id, a price, a size or an amount, from \
         what the run showed, and not measured where it did not. An item read from the whole \
         run's frames says so: the client ids, their echoes, ACK's latency, the statuses and \
         add_order's encoding are the run's, every named account's sessions together.\n\n\
         - The environment: QFEX {}, under the probe mode `{}`, against {}\n\
         - The accounts, as the run's configuration names them, never listed by QFEX (decision \
           0144): {}\n\
         - The key, the declarations, the fences met, the owner's unchecked confirmations and \
           every hand action: the section on what the run declares, below\n",
        ev.environment,
        ev.mode,
        ev.target,
        ev.accounts.join(", ")
    );
    for account in &ev.accounts {
        let _ = writeln!(s, "### Decision 0132's list on {account}\n");
        for (item, answer) in ITEMS.iter().zip(answers(ev, sessions, account)) {
            let _ = writeln!(s, "- {item}: {answer}");
        }
        let _ = writeln!(s);
    }
}

/// The evidence's entry: each named account's items, by name.
pub fn to_json(ev: &Evidence, sessions: &[SessionRun]) -> Value {
    let mut by = Map::new();
    for account in &ev.accounts {
        let items: Map<String, Value> = (ITEMS.iter())
            .zip(answers(ev, sessions, account))
            .map(|(item, answer)| ((*item).to_owned(), json!(answer)))
            .collect();
        by.insert((*account).to_owned(), Value::Object(items));
    }
    json!({"environment": ev.environment, "exec_mode": ev.mode, "target": ev.target,
        "accounts": ev.accounts, "run_wide_items_say": RUN_WIDE, "by_account": by})
}
