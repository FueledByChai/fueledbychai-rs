//! What stage one found, and the scrubbed slice of the sessions' journal it writes from it
//! (`fixtures/`, to commit as `fixtures/qfex/exec-recorded/arm/`, or a production run's as
//! `fixtures/qfex/exec-recorded/arm-production/`, decision 0143 item 4). The report
//! (`report.md`) and the same machine-readable (`evidence.json`) are written by
//! [`crate::report`]. A run that holds a connection first
//! (decision 0146) reports the hold too ([`crate::hold`]); the hold's frames are not journaled,
//! so the slice holds none of them.
//!
//! **The slice** ([`slice`]) is every frame QFEX sent on a session's trade socket, read back from
//! the journal (decision 0006) with every span the codec named blanked to `2`s: the account's
//! identifiers and credentials (decisions 0028, 0122 item 3) and, in an echo, the spans that
//! decisions 0132 item 3 and 0134 keep in place of the whole frame, so the arm's own echo is
//! there as received but its err message. Each frame is placed against the arm: before it was
//! written, in its window (from its write to its own deadline, or the connection's end: the
//! deadline of another request, a place's, ends no window), or after. A session's end is taken
//! as its connection's end when the journal holds no `Closed` control for it, as after an abrupt
//! drop (FBC-77r8): the first record of a later session ends it, its window and the page it waits
//! for alike.
//! A frame that is not a top-level `ack` or `err` envelope echoing a request has every JSON number
//! replaced by `0` ([`scrub`], [`zero_numbers`]), so no amount of the account (a position, a
//! margin, a balance) is published (decision 0009); its member names, strings and nesting are as
//! received. No frame we wrote is kept.
//!
//! **The place stages' scrub** ([`Scrub::Places`], [`scrub_places`]; decision 0148 item 10)
//! scrubs their frames as FBC-291a's capture was, whose `symbol`s the owner blanked by hand
//! after the run, except that each fee keeps its sign. Stage one's frames are only the arm's and
//! the resync's, and stage one's slice keeps its own scrub, so its captures are as before. In
//! every frame, a top-level echo included, every `symbol`'s string value becomes [`SYMBOL`]. A
//! top-level echo is otherwise kept as the codec's spans left it. In any other frame, every
//! number is zeroed and every string that reads, its escapes decoded, as a decimal (an amount
//! QFEX writes as a string) becomes `"0"`; a `fee` becomes `-1`, `0` or `1` by its sign; and
//! every `id` or `*_id` member, at any depth, is blanked to `2`s at its length as written,
//! whether or not the codec's spans (decision 0028, `src/auth`) already did. Member names, other
//! strings, literals, whitespace and nesting are as received.
//!
//! **The resync's pages.** The same read of the journal pairs each `get_user_orders` request a
//! session wrote with the page that answered it, and records each full page with its page size
//! and offset ([`crate::pages`], decision 0132 item 5): counts, never a row.
//!
//! **Gaps.** The journal's sink never blocks: a record it has no room for is dropped and counted,
//! and the gap is marked `Degraded` once space returns or at its close (decision 0006). This
//! program is the evidence, so a gap is never assumed away. A session's capture is whole only
//! when no loss the run found could hold its records: no gap a `Degraded` marker shows, no drop
//! the markers leave unaccounted ([`Evidence::dropped`]), no record a stopped writer left
//! unwritten ([`Evidence::writer_failed`]), and no end of the journal the slice could not read.
//! The slice marks partial every session whose records a loss could hold ([`Gaps`]): from the
//! point a writer error or an unreadable end could start, and every session for drops the
//! markers leave unaccounted.
//! Any of them makes the run incomplete, and a partial session's capture concludes nothing of
//! what did or did not answer its arm, nor states where any frame of it fell against the arm.
//!
//! **Privacy.** Every fixture file, the report and the evidence pass the privacy check's patterns
//! (`evidence::privacy_hits`, paradex_testnet_acceptance's, reused by path) before they are
//! written: a fixture they find something in is withheld and named, and a report or evidence
//! file is replaced by a note ([`outputs`]). The run then runs scripts/privacy-check.sh over the
//! fixtures. The patterns know credential and address shapes only, so the report also names every
//! member whose string value a fixture keeps ([`kept_strings`]), for the owner to read each file
//! for an identifying string (an email, a user name, an address in another form) before
//! committing it.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use fbc_core::{CidMatch, ClientOrderId, RpcId, SpecTable, VenueOrderId};
use fbc_journal::{ControlEvent, JournalReader, Marker, Opcode, Record};
use fbc_venue_qfex::account_reads::{OrdersPage, PageOrder, Positions};
use fbc_venue_qfex::exec::ARM;
use serde_json::{Map, Value};

use crate::auth_qfex::Armed;
use crate::evidence::privacy_hits;
use crate::hold::HoldRun;
use crate::ledger::RegistryRun;
use crate::market::Preflight;
use crate::pages::Pages;
use crate::refusals::{ReadSocket, Refusals};

/// What one account's reads showed before a session or the hold, or after the hold.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountRead {
    /// The account the session after them arms on, `the hold` for the reads before the hold, or
    /// `after the hold` for those after it (`refusals::HOLD`, `refusals::AFTER_HOLD`).
    pub before: &'static str,
    /// `primary` or `subaccount`, as the run's configuration names the account (decision 0144),
    /// `(armed)` when the session arms on it: never an id.
    pub label: String,
    /// The rows `GET /user/positions` listed, and whether every one was flat.
    pub rows: usize,
    pub flat: bool,
    /// The sum of those rows' `open_orders`, which no page describes (decision 0132 item 5).
    pub rest_open_orders: u64,
    /// What `get_user_orders` listed on a socket that never armed.
    pub page_orders: usize,
    pub page_twaps: usize,
    /// Whether the page was full, so another may have followed.
    pub page_full: bool,
    /// How many of the page's orders were the one order the read admits ([`AllowedOrder`]):
    /// none unless one was admitted on this account.
    pub page_allowed: usize,
    /// Whether the page listed that order at all, found by its venue id once the streams named
    /// it, whatever client id its row carries ([`AllowedOrder::names`]): what the reads after a
    /// session say of whether X rests, apart from what a read before a session admits.
    pub page_lists_it: bool,
    /// The `open_orders` GET /user/positions counts on symbols other than the admitted order's,
    /// when the page listed it ([`AccountRead::of`]): none otherwise.
    pub rest_open_elsewhere: u64,
    /// How many of the page's orders are none of the orders the read knows
    /// ([`AllowedOrder::unknown`]): the reads after a drop session know the drop test's orders on
    /// their account, X and then Y, so one of these is an order the streams did not show
    /// (decision 0148 item 8, FBC-77r8's review). Every other read knows none, so it counts every
    /// order its page lists, and judges them by [`AccountRead::refusal`] alone. It counts the
    /// one page the read makes: an order on a later page of a full one, or one no page lists and
    /// REST's `open_orders` alone counts (a stop order), is not counted, since that count does
    /// not tell it from ours. A read after a drop session then shows it as an order resting
    /// ([`Stop::LeftResting`](crate::places::Stop::LeftResting), marked), not as one unknown.
    pub page_unknown: usize,
}

impl AccountRead {
    /// What one account's reads showed: `positions`, its GET /user/positions, and `page`, one
    /// `get_user_orders` page on a socket of its own that never armed, counting the orders that
    /// are `allowed` when it is on `account` ([`AllowedOrder::count`]), whether it lists that
    /// order at all ([`AllowedOrder::listed`]) and, when the page admits that order, the REST
    /// `open_orders` on every other symbol than its own.
    pub fn of(
        positions: &Positions,
        page: &OrdersPage,
        allowed: Option<&AllowedOrder>,
        account: Armed,
    ) -> AccountRead {
        let rows = positions.rows();
        let on = allowed.filter(|a| a.account == account).and_then(|a| {
            let mut listed = page.orders().iter();
            listed.find(|o| a.matches(&o.order, &o.client))
        });
        let elsewhere = on.map_or(0, |order| {
            let others = rows.iter().filter(|row| row.symbol != order.symbol);
            others.map(|row| row.open_orders).sum()
        });
        AccountRead {
            rows: rows.len(),
            flat: positions.is_flat(),
            rest_open_orders: rows.iter().map(|row| row.open_orders).sum(),
            page_orders: page.orders().len(),
            page_twaps: page.twaps(),
            page_full: page.is_full(),
            page_allowed: AllowedOrder::count(allowed, account, page.orders()),
            page_lists_it: AllowedOrder::listed(allowed, account, page.orders()),
            rest_open_elsewhere: elsewhere,
            ..AccountRead::default()
        }
    }

    /// Why no session may start on it, or `None` when one may: no open order or TWAP on the
    /// page but the one order it admits, if any ([`AccountRead::page_allowed`]), every position
    /// flat, and no open order counted by REST beyond that one. Decision 0132 item 7 asks an
    /// empty `get_user_orders` page and flat REST positions, but before the drop test's second
    /// session, when X alone may be open. The probe reads it strictly: a REST `open_orders` count
    /// above the orders admitted refuses too, since an order the page does not list (a stop
    /// order, which no page says it covers) could be cancelled by the arm (item 2) and its
    /// `CANCELLED` report would then confound the arm's capture. With X admitted and listed, REST
    /// must count exactly it, one open order on its symbol and none on another (0132 item 7, 0144
    /// item 4).
    pub fn refusal(&self) -> Option<String> {
        let unadmitted = self.page_orders.saturating_sub(self.page_allowed);
        let admitted = u64::try_from(self.page_allowed).unwrap_or(u64::MAX);
        let page = match self.page_allowed {
            0 => "shows no open order on get_user_orders",
            _ => "shows no open order on get_user_orders but the one the read admits",
        };
        if !self.flat || unadmitted > 0 || self.page_twaps > 0 {
            Some("holds an open order, a TWAP or a position that is not flat".to_owned())
        } else if self.rest_open_orders > admitted {
            Some(format!(
                "{page}, but GET /user/positions counts {}; the probe reads that disagreement \
                 strictly, since an order the page does not list could be cancelled by the arm \
                 and confound its capture",
                self.rest_open_orders
            ))
        } else if self.rest_open_orders < admitted || self.rest_open_elsewhere > 0 {
            Some(format!(
                "{page}, but GET /user/positions counts {} open orders, {} of them on another \
                 symbol than its own: the read admits it alone, as one open order on its symbol \
                 (decision 0132 item 7)",
                self.rest_open_orders, self.rest_open_elsewhere
            ))
        } else {
            None
        }
    }
}

/// The one order of ours a read of every named account before a session may show open, on its
/// account alone: decision 0132 item 7's one exception, X before the drop test's second session
/// (FBC-77r8). Every other read admits none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedOrder {
    pub account: Armed,
    pub cid: ClientOrderId,
    /// Its venue id, once the streams named it.
    pub venue: Option<VenueOrderId>,
}

impl AllowedOrder {
    /// Whether a page's order, `order` with the client id `client` decoded under our namespace,
    /// is it: its client id ours and its own, and its venue id its own when that is known.
    pub fn matches(&self, order: &VenueOrderId, client: &CidMatch) -> bool {
        let venue = self.venue.as_ref().is_none_or(|v| v == order);
        venue && *client == CidMatch::Ours(self.cid)
    }

    /// Whether a page's order, `order` with the client id `client`, names it: by its venue id
    /// once that is known, whatever the row's client id, and otherwise by its client id. Looser
    /// than [`AllowedOrder::matches`], which admits it before a session: a row naming its venue
    /// id with an empty or unreadable client id still shows it resting.
    pub fn names(&self, order: &VenueOrderId, client: &CidMatch) -> bool {
        match &self.venue {
            Some(venue) => venue == order,
            None => *client == CidMatch::Ours(self.cid),
        }
    }

    /// Whether `orders`, a page of `account`'s, list it ([`AllowedOrder::names`]): never when
    /// `allowed` is none or is on another account.
    pub fn listed(allowed: Option<&AllowedOrder>, account: Armed, orders: &[PageOrder]) -> bool {
        let listing = |a: &AllowedOrder| orders.iter().any(|o| a.names(&o.order, &o.client));
        allowed
            .filter(|a| a.account == account)
            .is_some_and(listing)
    }

    /// How many of `orders`, a page of `account`'s, are named by none of `known`
    /// ([`AllowedOrder::names`]) that is on `account` ([`AccountRead::page_unknown`]).
    pub fn unknown(known: &[&AllowedOrder], account: Armed, orders: &[PageOrder]) -> usize {
        let mine = || known.iter().filter(|a| a.account == account);
        let named = |o: &&PageOrder| mine().any(|a| a.names(&o.order, &o.client));
        orders.iter().filter(|o| !named(o)).count()
    }

    /// How many of `orders`, a page of `account`'s, are it ([`AccountRead::page_allowed`]): none
    /// when `allowed` admits none or is on another account, and never more than one, since
    /// decision 0132 item 7 admits the one order X; a page listing it twice then holds one
    /// order the read does not admit.
    pub fn count(allowed: Option<&AllowedOrder>, account: Armed, orders: &[PageOrder]) -> usize {
        let matching = |a: &AllowedOrder| {
            let of = orders.iter();
            of.filter(|o| a.matches(&o.order, &o.client)).count().min(1)
        };
        allowed.filter(|a| a.account == account).map_or(0, matching)
    }
}

/// Where a frame arrived, against the session's arm.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum When {
    BeforeArm,
    InWindow,
    AfterWindow,
}

impl When {
    pub fn slug(self) -> &'static str {
        match self {
            When::BeforeArm => "before-arm",
            When::InWindow => "in-window",
            When::AfterWindow => "after-window",
        }
    }
}

/// One frame QFEX sent on a session's socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Arrived {
    /// What it is ([`kind`]).
    pub kind: String,
    /// Where the records read place it against the arm; stated only for a whole capture
    /// ([`SessionRun::position`]).
    pub when: When,
    /// Its fixture file's name.
    pub file: String,
}

/// A partial capture's frame's position against the arm: a gap could hold the arm's write, its
/// deadline or its connection's close, so where the records read place a frame is not stated.
pub const UNVERIFIED: &str = "position unverified";

/// One session: what the session reported, and what its journal shows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionRun {
    /// `primary` or `subaccount`.
    pub account: &'static str,
    /// The session's name in the report, when it is not its account's: a place stage's
    /// (`places::DROP_ONE`), since a place-stage account runs several sessions; empty for stage
    /// one's, which run one per account ([`SessionRun::label`]).
    pub name: &'static str,
    /// Whether the codec reported the stream authenticated: authentication succeeded and QFEX
    /// confirmed the three subscriptions (decision 0132 item 1).
    pub authenticated: bool,
    /// Whether the journal shows the arm written.
    pub arm_sent: bool,
    /// The arm's outcome, as the session reported it.
    pub arm: Option<String>,
    /// What ended the session.
    pub ended_by: &'static str,
    pub attempts: u64,
    pub failed_attempts: u64,
    pub arm_failures: u64,
    pub decode_errors: u64,
    /// An error the session ended with.
    pub error: Option<String>,
    /// Every frame QFEX sent on it, in order.
    pub frames: Vec<Arrived>,
    /// The provider rate refusals it met (decision 0135 item 2): the limiter's count while it
    /// ran (`stage.rs`), and its frames the journal kept that were refusals or unread
    /// ([`slice`]).
    pub refusals: Refusals,
    /// Which of [`SessionRun::frames`] were provider rate refusals, by index.
    pub refusal_frames: Vec<usize>,
    /// Its resync's `get_user_orders` pages, each full one with its page size and offset
    /// (decision 0132 item 5), as its journal kept them ([`crate::pages`]).
    pub pages: Pages,
    /// Why its capture is partial: each way the journal could be missing its records ([`Gaps`]).
    /// Empty when the journal was written and read whole and kept every record it could hold.
    pub partial: Vec<String>,
}

/// What the capture shows of the arm (decision 0132 item 1).
const NOT_SENT: &str = "No arm was sent: the stream was never reported authenticated. Nothing is \
     known of what answers the arm; the frames before it show why (the authentication reply and \
     the subscription confirmations, whose exact shape FBC-bsou's note asks for first).";
const ACKED: &str = "An ack echoing the arm arrived in its window: the shape decision 0132 item 1 \
     reads as the arm's Final acceptance, which the probe codec reports (FBC-jxau decodes it from \
     FBC-291a's capture); the drop test (0132 item 4) then shows whether the reading holds.";
const ERRED: &str = "An err echoing the arm arrived in its window: under decision 0132 item 1 \
     the arm's rejection, unless its code is RateLimited.";
const OTHER: &str = "This capture shows none of them answering the arm. If one does, a new \
     record decides whether its shape can name the arm (decision 0132 item 1).";
const NOTHING: &str = "Nothing arrived in the arm's window: no reply came before the arm's \
     deadline. If later captures show the same, QFEX cannot meet decision 0058 as 0132 reads it, \
     and nothing places on it until a new record decides what answers the arm (decision 0132 \
     item 1).";
const LATE: &str = "The arm was echoed only after its deadline: a capture with a longer \
     --rpc-timeout-ms, or a new record, decides whether that reply answers the arm (decision \
     0132 item 1).";
const PARTIAL: &str = "The capture is partial: the journal may be missing records of this \
     session's (decision 0006), so it cannot show what did or did not answer the arm, whether \
     the arm was written included, and nothing is concluded from it. A run whose journal drops \
     nothing decides (decision 0132 item 1).";

impl SessionRun {
    /// How the report, the evidence and the refusals' table name it: its [`SessionRun::name`],
    /// or its account's when it has none.
    pub fn label(&self) -> &'static str {
        match self.name.is_empty() {
            true => self.account,
            false => self.name,
        }
    }

    /// The prefix of its fixture files: its account's name, then its own as words joined by
    /// `-` when it has one, so two sessions of one account never share a file.
    pub fn slug(&self) -> String {
        match self.name.is_empty() {
            true => self.account.to_owned(),
            false => format!("{}-{}", self.account, self.name.replace(' ', "-")),
        }
    }

    /// Whether the session did what stage one asks: the stream authenticated, the arm was
    /// written and its outcome reported, once, with no second connection attempt, and the
    /// journal kept every record of it.
    pub fn ok(&self) -> bool {
        self.authenticated
            && self.arm_sent
            && self.arm.is_some()
            && self.attempts == 1
            && self.error.is_none()
            && self.partial.is_empty()
    }

    /// Where `frame`, one of its frames, arrived against the arm, as the capture can state it:
    /// its window position ([`When::slug`]) when the capture is whole, [`UNVERIFIED`] when it is
    /// partial.
    pub fn position(&self, frame: &Arrived) -> &'static str {
        match self.partial.is_empty() {
            true => frame.when.slug(),
            false => UNVERIFIED,
        }
    }

    /// What the capture shows of the arm: for a partial capture, only that it is partial and
    /// the echoes of the arm it kept, with no position; else an echo of it in its window; else
    /// the kinds of what arrived there, which echo nothing of the arm; else that nothing
    /// arrived; and, in either of the last two, any echo of the arm that arrived after the
    /// window, late.
    pub fn answer(&self) -> String {
        let at = |when: When| self.frames.iter().filter(move |f| f.when == when);
        let echo = |f: &&Arrived| {
            f.kind.starts_with("ack echoing the arm") || f.kind.starts_with("err echoing the arm")
        };
        if !self.partial.is_empty() {
            let kept: Vec<String> = (self.frames.iter().filter(echo))
                .map(|f| format!("{} ({}, {})", f.kind, self.position(f), f.file))
                .collect();
            return format!(
                "{PARTIAL} Why: {}. Echoes of the arm it kept: {}.",
                self.partial.join("; "),
                list(&kept)
            );
        }
        if !self.arm_sent {
            return NOT_SENT.to_owned();
        }
        let window: Vec<&Arrived> = at(When::InWindow).collect();
        if window
            .iter()
            .any(|f| f.kind.starts_with("ack echoing the arm"))
        {
            return ACKED.to_owned();
        }
        if window
            .iter()
            .any(|f| f.kind.starts_with("err echoing the arm"))
        {
            return ERRED.to_owned();
        }
        let late: Vec<String> = at(When::AfterWindow)
            .filter(echo)
            .map(|f| format!("{} ({})", f.kind, f.file))
            .collect();
        let late = match late.is_empty() {
            true => String::new(),
            false => format!(" After the window, late: {}. {LATE}", late.join("; ")),
        };
        if window.is_empty() {
            return format!("{NOTHING}{late}");
        }
        let mut kinds: Vec<&str> = Vec::new();
        for f in &window {
            if !kinds.contains(&f.kind.as_str()) {
                kinds.push(&f.kind);
            }
        }
        format!(
            "Frames arrived in the arm's window, but none echoed the arm: {}. {OTHER}{late}",
            kinds.join("; ")
        )
    }
}

/// What a run found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Evidence {
    pub started: String,
    /// The environment the run's probe mode names, `pre-production` or `production` (decision
    /// 0143 item 3), whichever side of the guard its origins are on.
    pub environment: &'static str,
    /// That mode: `preprod_probe` or `production_probe`.
    pub mode: &'static str,
    /// Where the origins pointed: QFEX pre-production, QFEX production, or a loopback test stub.
    pub target: &'static str,
    /// The accounts the run reads before every session, `primary` then `subaccount`, as its
    /// configuration names them (decision 0144): never a list QFEX gave, since the probe sends
    /// no `GET /user/subaccounts`, and never an id.
    pub accounts: Vec<&'static str>,
    /// The accounts the run asked to arm on (`--arm`), in run order: none for a run that only
    /// holds.
    pub arms: Vec<&'static str>,
    /// Whether the run asked for the liveness hold (`--hold-secs`, decision 0146).
    pub hold_asked: bool,
    /// What the hold's connection heard, once the hold started.
    pub hold: Option<HoldRun>,
    /// Whether the run asked for the place stages (`--place-stages`, FBC-c6kw).
    pub places_asked: bool,
    /// What the place stages' preflight passed, once it started ([`crate::market`]): never a
    /// value.
    pub preflight: Option<Preflight>,
    pub reads: Vec<AccountRead>,
    /// The named accounts whose last account read in a place-stage run could not be made, in
    /// the order they failed: what the run leaves the owner by hand is not known for them
    /// ([`crate::taker::hand_unknown`]; FBC-ou9g, carrying FBC-77r8's review).
    pub last_unread: Vec<&'static str>,
    pub sessions: Vec<SessionRun>,
    /// Each registry's fill ledger: its bounds, and its horizon and replay counts at each of its
    /// sessions' ends (decision 0132 item 5).
    pub registries: Vec<RegistryRun>,
    /// Every read socket the reads opened, and the provider rate refusals each met (decision 0135
    /// item 2); a session's are its own ([`SessionRun::refusals`]).
    pub read_sockets: Vec<ReadSocket>,
    /// Why the probe stopped before its sessions ended.
    pub stopped: Option<String>,
    /// The place stages' stop, by name, when one stopped the run (`places::Stop`, FBC-23ch):
    /// never an id or an amount.
    pub stop: Option<&'static str>,
    /// What a place-stage run did with stop markers (0151 item 6), once it opened its lease
    /// directory.
    pub marker: Option<crate::places::MarkerRun>,
    /// Each named account's drop test, in run order, once its registry was built (FBC-23ch,
    /// FBC-77r8): what its two sessions did, what its reads showed and its verdict, names, flags,
    /// counts and times alone.
    pub drops: Vec<crate::places::DropRun>,
    /// Stage two (FBC-su9i), once a drop session ran: the run's exact-echo gate, what its frames
    /// show of our client ids and QFEX's statuses, and each named account's crossing session.
    pub stage_two: Option<crate::stage_two::StageTwo>,
    /// Each place-stage registry built: its step, its declaration, its bounds as fences, and its
    /// ledger at each of its sessions' ends (decision 0132 item 5).
    pub place_registries: Vec<crate::ledger::PlaceRun>,
    pub fixtures: Vec<String>,
    /// The members whose string value a written fixture keeps as received ([`Scrub::kept`]: none
    /// a place stage's scrub wrote): names only, for the owner's reading before committing.
    pub strings: Vec<String>,
    /// Fixture files withheld, and what the patterns found in each (never the value).
    pub withheld: Vec<String>,
    /// What was not written, or not checked.
    pub errors: Vec<String>,
    /// The records the journal's sink dropped, Normal then Safety, read once its writer closed
    /// (`stage::journal_closed`): a drop no `Degraded` marker reached the journal for (one
    /// offered once the writer had stopped, or once the sink closed) is counted here too.
    pub dropped: [u64; 2],
    /// The write error that stopped the journal's writer thread, when one did
    /// (`stage::journal_closed`): every record the sink accepted after the last one written is
    /// then in no file, and neither counted nor marked as dropped.
    pub writer_failed: Option<String>,
}

impl Evidence {
    /// Whether the run did all it asked: the hold, when it asked for one, did what decision 0146
    /// asks; every session armed with its capture whole; the journal's sink dropped nothing and
    /// its writer wrote everything; every registry's ledger bounds held; and every file written.
    /// A run did something: a session ran, or the hold did.
    pub fn ok(&self) -> bool {
        self.stopped.is_none()
            && self.registries.iter().all(RegistryRun::held)
            && self
                .place_registries
                .iter()
                .all(crate::ledger::PlaceRun::held)
            && self.withheld.is_empty()
            && self.errors.is_empty()
            && self.dropped == [0, 0]
            && self.writer_failed.is_none()
            && (!self.sessions.is_empty() || self.hold.is_some())
            && self.sessions.iter().all(SessionRun::ok)
            && self.hold.as_ref().is_none_or(HoldRun::ok)
    }
}

/// What a frame is, from the bytes the journal kept: an err or an ack named by what it echoes
/// (the arm, as decision 0132 item 1 compares it, or another request), with an err's code as
/// kept; any other object by its member names. Never a value but a kept code.
pub fn kind(text: &str) -> String {
    let Ok(Value::Object(top)) = serde_json::from_str::<Value>(text) else {
        return "not a JSON object".to_owned();
    };
    let arm: Value = serde_json::from_str(ARM).expect("the arm is JSON");
    let Some((envelope, body)) = envelope(&top) else {
        return names(&top);
    };
    let code = body.get("error_code").and_then(Value::as_str);
    let code = code.map(|code| format!(" ({code})")).unwrap_or_default();
    match body.get("incoming_message") {
        Some(echo) if *echo == arm => format!("{envelope} echoing the arm{code}"),
        Some(_) => format!("{envelope} echoing another request{code}"),
        None => format!("{envelope} echoing nothing{code}"),
    }
}

/// A frame's top-level `ack` or `err` envelope, nested (`{"ack": {...}}`, `{"err": {...}}`) or
/// flat (`{"type": "Err", ...}`), and the object that holds its members.
pub fn envelope(top: &Map<String, Value>) -> Option<(&'static str, &Map<String, Value>)> {
    let flat = top.get("type").and_then(Value::as_str) == Some("Err");
    match (top.get("ack"), top.get("err"), flat) {
        (Some(Value::Object(ack)), None, false) => Some(("ack", ack)),
        (None, Some(Value::Object(err)), false) => Some(("err", err)),
        (None, None, true) => Some(("err", top)),
        _ => None,
    }
}

/// An object's member names, joined.
fn names(top: &Map<String, Value>) -> String {
    let names: Vec<&str> = top.keys().map(String::as_str).collect();
    format!("{{{}}}", names.join(", "))
}

/// `text` with every JSON number outside a string replaced by `0`; strings, member names,
/// literals, whitespace and nesting as they are.
pub fn zero_numbers(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let (mut string, mut escaped, mut number) = (false, false, false);
    for c in text.chars() {
        if string {
            out.push(c);
            (string, escaped) = match (escaped, c) {
                (true, _) => (true, false),
                (false, '\\') => (true, true),
                (false, '"') => (false, false),
                _ => (true, false),
            };
            continue;
        }
        let part = c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E');
        if number && part {
            continue;
        }
        number = c == '-' || c.is_ascii_digit();
        string = c == '"';
        out.push(if number { '0' } else { c });
    }
    out
}

/// Whether `text` is a top-level `ack` or `err` envelope echoing a request (an
/// `incoming_message` in the envelope, as [`kind`] reads it), whose values decisions 0132 item 3
/// and 0134 already span.
fn echoes(text: &str) -> bool {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(top)) => {
            envelope(&top).is_some_and(|(_, body)| body.contains_key("incoming_message"))
        }
        _ => false,
    }
}

/// A fixture's text: as kept when it is a top-level echo ([`echoes`]); otherwise with every
/// number zeroed, an `incoming_message` nested anywhere else included. Stage one's scrub.
pub fn scrub(text: &str) -> String {
    match echoes(text) {
        true => text.to_owned(),
        false => zero_numbers(text),
    }
}

/// How a slice scrubs a frame ([`slice_scrubbed`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Scrub {
    /// Stage one's, [`scrub`].
    Arm,
    /// The place stages' (decision 0148 item 10), [`scrub_places`], which their slice takes
    /// ([`slice_places`]); stage one never does.
    Places,
}

impl Scrub {
    fn apply(self, text: &str) -> String {
        match self {
            Scrub::Arm => scrub(text),
            Scrub::Places => scrub_places(text),
        }
    }

    /// The members of a fixture this scrub wrote whose string value it keeps as received
    /// ([`kept_strings`]). After [`Scrub::Places`], no decimal-shaped string is one: that scrub
    /// wrote it, `0` or a fee's sign, since it rewrites every one outside an echo, and an echo's
    /// are the codec's `2`s.
    pub fn kept(self, text: &str) -> BTreeSet<String> {
        match self {
            Scrub::Arm => kept_strings(text),
            Scrub::Places => kept_but(text, decimal),
        }
    }
}

/// The one placeholder every `symbol` of a place stage's capture becomes, in an echo too: the
/// one the owner put in place of each of FBC-291a's by hand
/// (`fixtures/qfex/exec-recorded/arm-production/`), so neither a value nor a file's size tells
/// two markets apart.
pub const SYMBOL: &str = "22222222";

/// A place stage's fixture text (decision 0148 item 10), scrubbed member by member
/// ([`scrub_members`]): a top-level echo ([`echoes`]) as stage one's [`scrub`] keeps one but
/// each `symbol`, which the codec's spans leave at its length; any other frame whole.
pub fn scrub_places(text: &str) -> String {
    scrub_members(text, echoes(text))
}

/// One level of nesting [`scrub_members`] reads in: an object, with the name of the member it
/// read last and whether a member name comes next; or an array, with the name of the member that
/// holds it, whose rule its values take.
struct Level {
    object: bool,
    name: Option<String>,
    name_next: bool,
}

/// The name of the member whose value is read at `levels`' innermost level.
fn under(levels: &[Level]) -> Option<&str> {
    levels.last().and_then(|level| level.name.as_deref())
}

/// `text` with each value replaced by the rule of the member it is under: under `symbol`, a
/// string as [`SYMBOL`]; in an `echo`, every other value as written; under `id` or a name
/// ending `_id`, a string's characters or a number's blanked to `2`s at its length as written;
/// under `fee`, a number or a decimal-shaped string as its [`sign`], the string still a string;
/// under any other name, or none, a number as `0` and a decimal-shaped string ([`decimal`]) as
/// `"0"`. A string is judged with its escapes decoded ([`decoded`]). An array's values are under
/// the member that holds it. Member names, other strings, literals, whitespace and nesting are
/// as they are. It walks the text as written, so a member a parser would overwrite is scrubbed
/// too; text that is not JSON is walked as far as it reads, its numbers zeroed as
/// [`zero_numbers`] zeroes them, unless it is an echo.
fn scrub_members(text: &str, echo: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut levels: Vec<Level> = Vec::new();
    let mut at = 0;
    while let Some(c) = text[at..].chars().next() {
        let end = match c {
            '"' => {
                let (end, closed) = string_end(text, at);
                let quoted = &text[at..end];
                let raw = &quoted[1..quoted.len() - usize::from(closed)];
                match levels.last_mut() {
                    Some(level) if level.object && level.name_next => {
                        level.name = Some(decoded(quoted, raw));
                        level.name_next = false;
                        out.push_str(quoted);
                    }
                    _ => {
                        out.push('"');
                        out.push_str(&string_value(under(&levels), quoted, raw, echo));
                        out.push_str(if closed { "\"" } else { "" });
                    }
                }
                end
            }
            '-' | '0'..='9' => {
                let len = text[at..]
                    .bytes()
                    .take_while(|b| b.is_ascii_digit() || b"-+.eE".contains(b))
                    .count();
                out.push_str(&number_value(under(&levels), &text[at..at + len], echo));
                at + len
            }
            _ => {
                match c {
                    '{' => levels.push(Level {
                        object: true,
                        name: None,
                        name_next: true,
                    }),
                    '[' => levels.push(Level {
                        object: false,
                        name: under(&levels).map(str::to_owned),
                        name_next: false,
                    }),
                    '}' | ']' => {
                        levels.pop();
                    }
                    ',' => {
                        if let Some(level) = levels.last_mut().filter(|level| level.object) {
                            level.name_next = true;
                        }
                    }
                    _ => {}
                }
                out.push(c);
                at + c.len_utf8()
            }
        };
        at = end;
    }
    out
}

/// Where the string opening at `at` in `text` ends, just past its closing quote, and whether it
/// has one: a string the text ends inside ends with it.
fn string_end(text: &str, at: usize) -> (usize, bool) {
    let mut escaped = false;
    for (i, b) in text.bytes().enumerate().skip(at + 1) {
        match (escaped, b) {
            (true, _) => escaped = false,
            (false, b'\\') => escaped = true,
            (false, b'"') => return (i + 1, true),
            _ => {}
        }
    }
    (text.len(), false)
}

/// A member's name or a string value, `quoted` as written: its escapes decoded, or, when it does
/// not decode (an unknown escape, or the text ending inside it), `raw`, its characters as
/// written.
fn decoded(quoted: &str, raw: &str) -> String {
    serde_json::from_str(quoted).unwrap_or_else(|_| raw.to_owned())
}

/// Whether a member named `name` names an identifier: `id`, or a name ending `_id`
/// (`order_id`, `trade_id`, `client_order_id`, `user_id`).
fn is_id(name: &str) -> bool {
    name == "id" || name.ends_with("_id")
}

/// The characters of a string value, `quoted` as written and `raw` its characters between the
/// quotes, under the member `name`, in an `echo` or not, as [`scrub_members`] writes them.
fn string_value(name: Option<&str>, quoted: &str, raw: &str, echo: bool) -> String {
    let text = decoded(quoted, raw);
    match name {
        Some("symbol") => SYMBOL.to_owned(),
        _ if echo => raw.to_owned(),
        Some(name) if is_id(name) => "2".repeat(raw.len()),
        Some("fee") if decimal(&text) => sign(&text).to_owned(),
        _ if decimal(&text) => "0".to_owned(),
        _ => raw.to_owned(),
    }
}

/// A number `run`, under the member `name`, in an `echo` or not, as [`scrub_members`] writes
/// it.
fn number_value(name: Option<&str>, run: &str, echo: bool) -> String {
    match name {
        _ if echo => run.to_owned(),
        Some(name) if is_id(name) => "2".repeat(run.len()),
        Some("fee") => sign(run).to_owned(),
        _ => "0".to_owned(),
    }
}

/// Whether `s` is decimal-shaped: an optional sign, digits with an optional fraction (either
/// side of the point may be empty, not both), and an optional exponent, whose digits may be
/// signed but not empty.
fn decimal(s: &str) -> bool {
    let digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    let s = s.strip_prefix(['-', '+']).unwrap_or(s);
    let (mantissa, exponent) = match s.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (s, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let exponent = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['-', '+']).unwrap_or(e);
        !e.is_empty() && digits(e)
    });
    !(whole.is_empty() && fraction.is_empty()) && digits(whole) && digits(fraction) && exponent
}

/// The sign of the number or decimal string `s`, as `-1`, `0` or `1`: `0` when its mantissa has
/// no digit but zeros, so `-0.0` and `0e5` are `0`. A fee keeps the sign QFEX wrote it with, and
/// nothing more (decision 0148 item 10).
fn sign(s: &str) -> &'static str {
    let mantissa = s.split(['e', 'E']).next().unwrap_or(s);
    match (
        mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b)),
        mantissa.starts_with('-'),
    ) {
        (false, _) => "0",
        (true, true) => "-1",
        (true, false) => "1",
    }
}

/// The members of the JSON `text` whose string value is kept: every member, at any depth (an
/// array's strings under the member that holds it), with a string value that is not empty and
/// not wholly `2`s, the codec's blanking. Names only, never a value. Stage one's
/// ([`Scrub::kept`]).
pub fn kept_strings(text: &str) -> BTreeSet<String> {
    kept_but(text, |_| false)
}

/// [`kept_strings`], but for the string values `scrubbed` says a scrub wrote.
fn kept_but(text: &str, scrubbed: fn(&str) -> bool) -> BTreeSet<String> {
    fn walk(value: &Value, under: &str, scrubbed: fn(&str) -> bool, found: &mut BTreeSet<String>) {
        match value {
            Value::Object(members) => {
                for (name, value) in members {
                    walk(value, name, scrubbed, found);
                }
            }
            Value::Array(items) => {
                (items.iter()).for_each(|item| walk(item, under, scrubbed, found))
            }
            Value::String(s) if !s.chars().all(|c| c == '2') && !scrubbed(s) => {
                found.insert(under.to_owned());
            }
            _ => {}
        }
    }
    let mut found = BTreeSet::new();
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        walk(&value, "(top level)", scrubbed, &mut found);
    }
    found
}

/// The connection a record names, when it names one.
fn conn_of(record: &Record) -> Option<u16> {
    match record {
        Record::Inbound { stamp, .. }
        | Record::InboundControl { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::Timer { stamp, .. }
        | Record::RpcTimeout { stamp, .. } => Some(stamp.conn.conn),
        Record::Outbound { conn, .. }
        | Record::WriteResult { conn, .. }
        | Record::HttpRequest { conn, .. }
        | Record::Control {
            ev: ControlEvent::Opened(conn) | ControlEvent::Closed(conn),
            ..
        }
        | Record::Control {
            ev: ControlEvent::Subscribe { conn, .. },
            ..
        } => Some(conn.conn),
        _ => None,
    }
}

/// The journal's gaps, each placed on every session whose records it could hold (decision
/// 0006).
///
/// The sink numbers every record offered to it from 0, except the `Degraded` markers it writes
/// itself (this program journals none of its own), and keeps one gap open at a time: from the
/// first record it drops to the record that closes it, its marker written just before that
/// record, or as the journal's last record when the sink closes with the gap open. So `seq`,
/// the records read plus the drops the markers read so far count, is a record's number for every
/// record offered before the first drop of a gap not yet marked, and no less than that gap's
/// `from_seq` for any record after (one the Safety reserve kept while the gap was open). A gap's
/// records were offered after the last record numbered below its `from_seq` and before the
/// record after its marker. The sessions run one after the other, session `n` on connection
/// `n`, so the gap could hold the records of every session from the one the last record before
/// it named (0 when none did) to the one the first record after it names (the last when none
/// does). Every session it could hold is marked partial; none is passed over.
///
/// What no marker shows is placed at the journal's end ([`Gaps::end`]), through the last
/// session: the records after a cut the slice could not read past; those the writer accepted
/// and never wrote, when a write error stopped it, which all follow the last record written;
/// and drops the sink counted that the markers read do not account for, which all follow the
/// record after the last marker read (a drop before it is that marker's or an earlier one's),
/// but may precede records the Safety reserve kept after them, so they are placed from the
/// session the last record before that marker named (from the first session when no marker
/// was read).
#[derive(Default)]
struct Gaps {
    seq: u64,
    /// The number and session of each record read that names a session's connection.
    named: Vec<(u64, usize)>,
    /// Gaps read whose last session no record has named yet: why, and their first session.
    open: Vec<(String, usize)>,
    /// The drops the `Degraded` markers read count, all told.
    marked: u64,
    /// The session the last record named before the last `Degraded` marker read (0 when none
    /// did, or no marker was read): the first a drop no marker read shows could be in.
    unmarked_from: usize,
}

impl Gaps {
    /// The session the last record read that names one named, or the first.
    fn last_named(&self) -> usize {
        self.named.last().map_or(0, |&(_, n)| n)
    }

    fn read(&mut self, record: &Record, sessions: &mut [SessionRun]) {
        if let Record::Marker(Marker::Degraded { from_seq, dropped }) = *record {
            let before = self.named.iter().rev().find(|(seq, _)| *seq < from_seq);
            let why = format!(
                "a journal gap, {dropped} dropped from its record {from_seq} on (a Degraded \
                 marker, decision 0006), could hold this session's records"
            );
            self.open.push((why, before.map_or(0, |&(_, n)| n)));
            self.seq += dropped;
            self.marked = self.marked.saturating_add(dropped);
            self.unmarked_from = self.last_named();
            return;
        }
        // Every other record took a number, other markers included, and none is a gap:
        // `SessionStart` and `Recovered` mark none, and `CancelOnly` (decision 0139), which the
        // runtime journals only for a journal given a registry's trip (this program gives it
        // none), follows a drop without marking it. That drop is marked by the `Degraded`
        // marker closing its gap or ending the journal, or else counted in the sink's drop
        // counts and no marker's ([`Gaps::end`]), either of which fails the run.
        let named = conn_of(record).map(usize::from);
        if let Some(n) = named.filter(|n| *n < sessions.len()) {
            self.named.push((self.seq, n));
            for (why, first) in std::mem::take(&mut self.open) {
                mark(sessions, first, n, &why);
            }
        }
        self.seq += 1;
    }

    /// Marks partial, through the last session, every session whose records the journal's end
    /// leaves in doubt: each gap still open, from its first session; with `cut`, why the
    /// journal could not be read on, from the session the last record read named; with
    /// `writer_failed`, from that session too; and when the sink counted `dropped` records and
    /// the markers read account for another number, from [`Gaps::unmarked_from`].
    fn end(
        self,
        sessions: &mut [SessionRun],
        cut: Option<&str>,
        writer_failed: bool,
        dropped: u64,
    ) {
        let last = sessions.len().saturating_sub(1);
        let read_to = self.last_named();
        for (why, first) in self.open {
            mark(sessions, first, last, &why);
        }
        if let Some(cut) = cut {
            let why = format!(
                "the journal could not be read to its end ({cut}), so the records after the \
                 last one read could be this session's"
            );
            mark(sessions, read_to, last, &why);
        }
        if writer_failed {
            let why = "the journal's writer stopped at an error, so the records the sink \
                       accepted after the last one it wrote are in no file, neither counted nor \
                       marked as dropped (decision 0006), and could be this session's";
            mark(sessions, read_to, last, why);
        }
        if self.marked != dropped {
            let why = format!(
                "records dropped: {dropped} by the journal's sink's count, {} by its Degraded \
                 markers (decision 0006), so a drop no marker shows could hold this session's \
                 records",
                self.marked
            );
            mark(sessions, self.unmarked_from, last, &why);
        }
    }
}

/// Marks sessions `a` to `b` partial, `why`, whichever way round the two are.
fn mark(sessions: &mut [SessionRun], a: usize, b: usize, why: &str) {
    let (from, to) = (a.min(b), a.max(b));
    for run in sessions.iter_mut().take(to + 1).skip(from) {
        run.partial.push(why.to_owned());
    }
}

/// A session's arm, as its journal shows it: where its frames fall against the arm, and the
/// request id the arm was written under, whose deadline alone ends its window.
#[derive(Copy, Clone)]
struct Window {
    when: When,
    arm: Option<RpcId>,
}

/// Ends the arm's window of connection `conn`, if it is open.
fn close(window: &mut [Window], conn: u16) {
    if let Some(window) = window.get_mut(usize::from(conn))
        && window.when == When::InWindow
    {
        window.when = When::AfterWindow;
    }
}

/// Reads shard `shard` of the journal under `journal`, where session `n` of `ev` ran on
/// connection `n`: marks each session whose arm was written, adds every frame QFEX sent it to
/// its frames, marks partial each session whose records the journal could be missing ([`Gaps`],
/// held to `ev`'s writer and drop counts), and gives each frame's fixture file and text. A
/// partial session's files are named `<account>-<n>-partial.json`, with no position.
///
/// A journal that cannot be opened, or read to its end, is an error, returned only once every
/// session from the one the last record read named through the last is marked partial: a
/// session the read never reached is partial, never one whose arm was not sent.
///
/// Stage one's slice: each frame's text scrubbed by [`scrub`]. [`slice_scrubbed`] takes the
/// scrub.
pub fn slice(
    journal: &Path,
    shard: u16,
    ev: &mut Evidence,
) -> Result<Vec<(String, String)>, String> {
    slice_scrubbed(journal, shard, ev, Scrub::Arm)
}

/// [`slice`], each frame's text scrubbed as `scrub` says, its sessions' pages read on an empty
/// spec table, as stage one's sessions run.
pub fn slice_scrubbed(
    journal: &Path,
    shard: u16,
    ev: &mut Evidence,
    scrub: Scrub,
) -> Result<Vec<(String, String)>, String> {
    slice_with(journal, shard, ev, scrub, &SpecTable::new())
}

/// A place-stage run's slice (decision 0148 item 10): [`slice`], each frame's text scrubbed by
/// [`Scrub::Places`], and each page its sessions' resyncs read judged on `specs`, the spec table
/// those sessions held, as their codec read it ([`crate::pages::rows_on`]).
pub fn slice_places(
    journal: &Path,
    shard: u16,
    ev: &mut Evidence,
    specs: &SpecTable,
) -> Result<Vec<(String, String)>, String> {
    slice_with(journal, shard, ev, Scrub::Places, specs)
}

/// [`slice_scrubbed`], each page judged on `specs`.
fn slice_with(
    journal: &Path,
    shard: u16,
    ev: &mut Evidence,
    scrub: Scrub,
    specs: &SpecTable,
) -> Result<Vec<(String, String)>, String> {
    let dropped = ev.dropped[0].saturating_add(ev.dropped[1]);
    let writer_failed = ev.writer_failed.is_some();
    let sessions = &mut ev.sessions[..];
    let unarmed = Window {
        when: When::BeforeArm,
        arm: None,
    };
    let mut window = vec![unarmed; sessions.len()];
    // Each frame's session, its place among that session's frames, and its scrubbed text,
    // named once the journal's end has said which captures are partial.
    let mut texts: Vec<(usize, usize, String)> = Vec::new();
    let mut gaps = Gaps::default();
    let mut cut = None;
    match JournalReader::open(journal, shard) {
        Err(e) => cut = Some(format!("the journal: {e}")),
        Ok(reader) => {
            for entry in reader.entries() {
                let record = match entry {
                    Ok(entry) => entry.record,
                    Err(e) => {
                        cut = Some(format!("the journal: {e}"));
                        break;
                    }
                };
                gaps.read(&record, sessions);
                read(record, sessions, &mut window, &mut texts, (scrub, specs));
            }
        }
    }
    gaps.end(sessions, cut.as_deref(), writer_failed, dropped);
    let files = texts.into_iter().map(|(at, i, text)| {
        let run = &mut sessions[at];
        let when = match run.partial.is_empty() {
            true => run.frames[i].when.slug(),
            false => "partial",
        };
        let file = format!("{}-{:03}-{when}.json", run.slug(), i + 1);
        run.frames[i].file.clone_from(&file);
        (file, text)
    });
    let files = files.collect();
    cut.map_or(Ok(files), Err)
}

/// Adds what `record` shows of the sessions to them: an arm written, its window's end, a frame
/// QFEX sent (to `texts` too, unnamed, scrubbed by `scrub`), and among those the provider rate
/// refusals and the frames no refusal can be read from, read from the bytes the journal kept
/// ([`Refusals::count`]), and each page of a resync judged on `specs`, the sessions' spec table.
fn read(
    record: Record,
    sessions: &mut [SessionRun],
    window: &mut [Window],
    texts: &mut Vec<(usize, usize, String)>,
    (scrub, specs): (Scrub, &SpecTable),
) {
    // The sessions run one after the other, session `n` on connection `n`, so a record of a later
    // session ends every earlier one: its end is taken as its close, its epoch journaled closed
    // or not, since an abrupt drop (decision 0132 item 4's) may journal no `Closed` control.
    if let Some(later) = conn_of(&record) {
        let earlier = sessions.iter_mut().zip(window.iter_mut());
        for (run, window) in earlier.take(usize::from(later)) {
            run.pages.closed();
            if window.when == When::InWindow {
                window.when = When::AfterWindow;
            }
        }
    }
    match record {
        Record::Outbound {
            conn, rpc, frame, ..
        } if frame.bytes() == ARM.as_bytes() => {
            let at = usize::from(conn.conn);
            if let Some(run) = sessions.get_mut(at) {
                run.arm_sent = true;
                window[at] = Window {
                    when: When::InWindow,
                    arm: rpc,
                };
            }
        }
        // Any other frame a session wrote: one of its resync's pages, or no page.
        Record::Outbound { conn, frame, .. } => {
            if let Some(run) = sessions.get_mut(usize::from(conn.conn)) {
                run.pages.sent(frame.bytes());
            }
        }
        // The arm's own deadline ends its window. A place stage's session sends places with
        // deadlines of their own, and a place's ends nothing.
        Record::RpcTimeout { stamp, rpc } => {
            let at = usize::from(stamp.conn.conn);
            if window.get(at).is_some_and(|w| w.arm == Some(rpc)) {
                close(window, stamp.conn.conn);
            }
        }
        // An epoch's close also ends the page its session waits for: a frame journaled after it
        // reached no codec.
        Record::Control {
            ev: ControlEvent::Closed(key),
            ..
        } => {
            close(window, key.conn);
            if let Some(run) = sessions.get_mut(usize::from(key.conn)) {
                run.pages.closed();
            }
        }
        Record::Inbound {
            stamp,
            opcode,
            bytes,
            ..
        } => {
            let at = usize::from(stamp.conn.conn);
            if let Some(run) = sessions.get_mut(at) {
                let text = String::from_utf8_lossy(&bytes.0).into_owned();
                if run.refusals.count(&text) {
                    run.refusal_frames.push(run.frames.len());
                }
                // The codec reads a page only from a text frame: a binary one it refuses as no
                // JSON object, ending its epoch, whatever its bytes hold.
                if opcode == Opcode::Text {
                    run.pages.heard_on(&text, specs);
                }
                texts.push((at, run.frames.len(), scrub.apply(&text)));
                run.frames.push(Arrived {
                    kind: kind(&text),
                    when: window[at].when,
                    file: String::new(),
                });
            }
        }
        // A gap, and every record no session's frames need, [`Gaps`] has read.
        _ => {}
    }
}

/// The fixture directory's README of a run that armed on no account, so wrote no frame: the hold
/// keeps none (decision 0146 item 6).
fn readme_unarmed(ev: &Evidence) -> String {
    format!(
        "# QFEX trade-socket frames: none\n\n\
         Recorded {} on QFEX {} (the probe mode `{}`), from {}, by the owner-run program \
         `qfex_preprod_probe` (crates/fbc-examples): a run that armed on no account, so this \
         directory holds no frame. Its liveness hold (FBC-39ks, decision 0146) keeps no frame \
         either: what the hold's connection heard is in the run's report.md and evidence.json, \
         which the owner reads before committing beside a README of their own (decision 0146 \
         item 6).\n",
        ev.started.get(..10).unwrap_or(""),
        ev.environment,
        ev.mode,
        ev.target,
    )
}

/// What a fixture directory's README says of a gap in the journal: nothing when it has none.
fn gap_note(ev: &Evidence) -> String {
    let [normal, safety] = ev.dropped;
    let sessions = shown(ev);
    match (
        has_gap(ev),
        sessions.iter().any(|run| !run.partial.is_empty()),
    ) {
        (false, _) => String::new(),
        (true, false) => format!(
            "\n**This capture is partial** (decision 0006): the journal's sink dropped {normal} \
             Normal and {safety} Safety records, and {UNPLACED}. What answered an arm may be \
             missing here, and nothing is concluded from its absence.\n"
        ),
        (true, true) => format!(
            "\n**This capture is partial** (decision 0006): the capture of {} is partial (the \
             run's report says why; the journal's sink dropped {normal} Normal and {safety} \
             Safety records). Each of their files is named `<account>-<n>-partial.json` and \
             states no position against the arm, since a gap could hold the arm's write, its \
             deadline or its connection's close. What answered an arm may be missing here, and \
             nothing is concluded from its absence.\n",
            partial_accounts(&sessions)
        ),
    }
}

/// The README of a place-stage run's fixture directory (`drop-production/`, 0151 item 7): what
/// its frames are and how they were scrubbed (decision 0148 item 10), never a value. Only the
/// drop test's sessions are named: stage two's crossing sessions' frames go in
/// `places-production/` ([`readme_crossing`]).
fn readme_places(ev: &Evidence) -> String {
    let sessions: Vec<String> = (ev.sessions.iter())
        .filter(|run| !is_crossing(run))
        .map(|run| format!("{} on {}", run.label(), run.account))
        .collect();
    let sessions = match sessions.is_empty() {
        true => "no session ran, so this directory holds no frame".to_owned(),
        false => sessions.join(" then "),
    };
    format!(
        "# QFEX trade-socket frames: the drop test's sessions\n\n\
         Recorded {} on QFEX {} (the probe mode `{}`), from {}, by the owner-run program \
         `qfex_preprod_probe` (crates/fbc-examples, FBC-23ch and FBC-77r8, slices of FBC-9sk2; \
         decisions 0148 and 0151): both sessions of each named account's drop test of decision \
         0132 item 4 ({sessions}), on accounts named by the run's configuration, not listed by \
         QFEX (decision 0144). Drop session one ends by a graceful close; drop session two by \
         an abrupt drop, with no WebSocket close, so its frames end at the drop and its \
         connection's end is taken as the next session's first record, or the journal's end. \
         These are recorded frames, not synthetic ones: this directory carries no SYNTHETIC \
         marker.\n\n\
         Each file is one frame QFEX sent on a session's trade socket, in the order received, \
         named `<account>-<session>-<n>-<when>.json` against the session's arm: `before-arm`, \
         `in-window` (from the arm's write to its own deadline or the connection's end) or \
         `after-window`, or `partial` for a partial capture.\n\n\
         Method: the sessions' journal (decision 0006), read back with every span the codec \
         named blanked to `2`s (decisions 0028 and 0122 item 3), then scrubbed member by member \
         as decision 0148 item 10 asks and 0151 item 7 reads it: in every frame each `symbol` \
         becomes `{SYMBOL}`; a top-level ack or err echo is otherwise as the codec's spans left \
         it; in any other frame every number and every decimal string is zeroed, each `fee` is \
         kept as its sign alone (-1, 0 or 1), and every `id` and `*_id` member is blanked to \
         `2`s at its length. No frame we wrote is kept. Every file passed the privacy check's \
         patterns before it was written, and scripts/privacy-check.sh then scanned the \
         directory. Those patterns know credential and address shapes only, so the owner reads \
         every file before committing this directory as \
         `fixtures/qfex/exec-recorded/drop-production/`, starting from the members the run \
         found keeping a string value: {}.\n{}",
        ev.started.get(..10).unwrap_or(""),
        ev.environment,
        ev.mode,
        ev.target,
        list(&ev.strings),
        gap_note(ev),
    )
}

/// Whether `run` is one of stage two's sessions, a crossing session (FBC-su9i) or a taker IOC
/// session (FBC-ou9g), whose frames go in the `places-production/` slice (0151 item 7).
pub fn is_crossing(run: &SessionRun) -> bool {
    run.name == crate::places::CROSS || run.name == crate::places::TAKE
}

/// Fixture files, each named, with its text.
pub type Files = Vec<(String, String)>;

/// The files of `files`, as [`slice_places`] named them, of stage two's crossing sessions of
/// `ev`, then the others: each file's name begins with its session's [`SessionRun::slug`].
pub fn split_crossing(ev: &Evidence, files: Files) -> (Files, Files) {
    let crossing: Vec<String> = (ev.sessions.iter().filter(|run| is_crossing(run)))
        .map(|run| format!("{}-", run.slug()))
        .collect();
    files
        .into_iter()
        .partition(|(file, _)| crossing.iter().any(|slug| file.starts_with(slug.as_str())))
}

/// The README of a place-stage run's `places-production/` fixture directory (0151 item 7):
/// stage two's crossing and taker IOC sessions' frames, scrubbed as the drop test's are.
fn readme_crossing(ev: &Evidence) -> String {
    let sessions: Vec<String> = (ev.sessions.iter().filter(|run| is_crossing(run)))
        .map(|run| format!("{} on {}", run.label(), run.account))
        .collect();
    format!(
        "# QFEX trade-socket frames: stage two's crossing post-only and taker IOC\n\n\
         Recorded {} on QFEX {} (the probe mode `{}`), from {}, by the owner-run program \
         `qfex_preprod_probe` (crates/fbc-examples, FBC-su9i and FBC-ou9g, slices of FBC-9sk2; \
         decisions 0132 item 7, 0148 and 0151): each named account's crossing session, then its \
         taker IOC session ({}). The crossing session places one post-only GTC at the \
         instrument's minimum size at the far touch, through it, placed only once that \
         account's drop test passed and an ACK in the run echoed one of our client ids byte for \
         byte (decision 0148 item 2), to show how QFEX refuses a crossing post-only and whether \
         that refusal echoes our client id. The taker IOC session places one limit IOC at the \
         minimum size at the far touch, through it, for the taker fee's sign, the IOC's \
         statuses and the order of its final order_response and its fill_response (FBC-tfxk), \
         and flattens its fill through Exit with reducing limit IOCs (decision 0148 item 5). \
         Each session cancels every order of ours by venue id before it stops. These are \
         recorded frames, not synthetic ones: this directory carries no SYNTHETIC marker.\n\n\
         Each file is one frame QFEX sent on a session's trade socket, in the order received, \
         named `<account>-the-crossing-session-<n>-<when>.json` or \
         `<account>-the-taker-IOC-session-<n>-<when>.json` against the session's arm: \
         `before-arm`, `in-window` or `after-window`, or `partial` for a partial capture.\n\n\
         Method: as the drop test's slice (`drop-production/`): the journal read back with \
         every span the codec named blanked to `2`s, then scrubbed member by member as decision \
         0148 item 10 asks and 0151 item 7 reads it: each `symbol` becomes `{SYMBOL}`; a \
         top-level ack or err echo is otherwise as the codec's spans left it; in any other \
         frame every number and every decimal string is zeroed, each `fee` kept as its sign \
         alone, and every `id` and `*_id` member blanked to `2`s at its length. No frame we \
         wrote is kept. Every file passed the privacy check's patterns before it was written, \
         and scripts/privacy-check.sh then scanned the directory; the owner reads every file \
         before committing this directory as `fixtures/qfex/exec-recorded/places-production/`, \
         starting from the members the run found keeping a string value: {}.\n{}",
        ev.started.get(..10).unwrap_or(""),
        ev.environment,
        ev.mode,
        ev.target,
        sessions.join(" then "),
        list(&ev.strings),
        gap_note(ev),
    )
}

/// The fixture directory's README.
pub fn readme(ev: &Evidence) -> String {
    if ev.places_asked {
        return readme_places(ev);
    }
    if ev.arms.is_empty() && ev.hold_asked {
        return readme_unarmed(ev);
    }
    let accounts: Vec<&str> = ev.sessions.iter().map(|s| s.account).collect();
    let gap = gap_note(ev);
    format!(
        "# QFEX trade-socket frames: what answered the cancel-on-disconnect arm\n\n\
         Recorded {} on QFEX {} (the probe mode `{}`), from {}, by the owner-run program \
         `qfex_preprod_probe` (crates/fbc-examples, FBC-l931; decision 0143 for production), \
         stage one of decision 0132 item 7: one session per account ({}, each named by the \
         run's configuration, not listed by QFEX: decision 0144), each arming \
         cancel-on-disconnect once and placing nothing. These are recorded frames, not \
         synthetic ones: this directory carries no SYNTHETIC marker.\n\n\
         Each file is one frame QFEX sent on a session's trade socket, in the order received, \
         named `<account>-<n>-<when>.json`: `before-arm` (the authentication reply, the \
         subscription confirmations and whatever came before the arm was written), `in-window` \
         (from the arm's write to its deadline or the connection's end, what decision 0132 item \
         3 keeps: when the capture is whole, whatever answered the arm is here) and \
         `after-window`.\n\n\
         Method: the sessions' journal (decision 0006), read back with every span the codec \
         named blanked to `2`s: the account's identifiers and credentials (decisions 0028 and \
         0122 item 3) and, in an echo, the spans decisions 0132 item 3 and 0134 keep in place of \
         the whole frame, so the arm's own echo is as received but its err message. A frame that \
         echoes no request has every JSON number replaced by `0`, so no amount of the account is \
         published (decision 0009); its member names, strings and nesting are as received. No \
         frame we wrote is kept: the arm we sent was `{ARM}`. Every file passed the privacy \
         check's patterns before it was written, and scripts/privacy-check.sh then scanned the \
         directory. Those patterns know credential and address shapes only, so the owner reads \
         every file for an identifying string (an email, a user name, an address in another \
         form) before committing this directory, starting from the members the run found \
         keeping a string value: {}.\n{gap}",
        ev.started.get(..10).unwrap_or(""),
        ev.environment,
        ev.mode,
        ev.target,
        accounts.join(" then "),
        list(&ev.strings),
    )
}

/// Whether the journal has a gap: a record its sink dropped, a writer stopped at an error, or
/// a session's capture the slice found partial.
pub fn has_gap(ev: &Evidence) -> bool {
    ev.dropped != [0, 0]
        || ev.writer_failed.is_some()
        || ev.sessions.iter().any(|run| !run.partial.is_empty())
}

/// What is said of a gap the slice placed on no session: only of a journal never sliced, or
/// of a run with no session, since the slice places what it finds of every gap.
const UNPLACED: &str =
    "the drops could not be placed on a session; no session's capture is shown whole";

/// The sessions as the report, the evidence and the README show them: as the slice left them,
/// unless the journal has a gap the slice placed on none of them, when each is shown partial
/// for it ([`UNPLACED`]).
pub fn shown(ev: &Evidence) -> Vec<SessionRun> {
    let unplaced = has_gap(ev) && ev.sessions.iter().all(|run| run.partial.is_empty());
    let shown = |run: &SessionRun| match unplaced {
        true => SessionRun {
            partial: vec![UNPLACED.to_owned()],
            ..run.clone()
        },
        false => run.clone(),
    };
    ev.sessions.iter().map(shown).collect()
}

/// The accounts of `sessions` whose capture is partial, or, when there is none, [`UNPLACED`]:
/// said only of a journal with a gap.
pub fn partial_accounts(sessions: &[SessionRun]) -> String {
    let partial: Vec<&str> = (sessions.iter())
        .filter(|run| !run.partial.is_empty())
        .map(|run| run.account)
        .collect();
    match partial.is_empty() {
        true => UNPLACED.to_owned(),
        false => partial.join("; "),
    }
}

/// Writes `files` and the README into `dir`, each only when the privacy check's patterns find
/// nothing in it; one they find something in is withheld and named in `ev`. Stage one's: the
/// files [`slice`] scrubbed.
pub fn write_fixtures(
    dir: &Path,
    files: Vec<(String, String)>,
    ev: &mut Evidence,
) -> Result<(), String> {
    write_fixtures_scrubbed(dir, files, ev, Scrub::Arm)
}

/// [`write_fixtures`] of files `scrub` scrubbed ([`slice_scrubbed`]), whose kept strings are
/// read as [`Scrub::kept`] reads them.
pub fn write_fixtures_scrubbed(
    dir: &Path,
    files: Vec<(String, String)>,
    ev: &mut Evidence,
    scrub: Scrub,
) -> Result<(), String> {
    ev.strings.clear();
    write_fixtures_with(dir, files, ev, scrub, readme)
}

/// [`write_fixtures_scrubbed`] of stage two's crossing sessions' files into
/// `places-production/` (0151 item 7), with that directory's README, the strings it keeps added
/// to those the run's other slice kept.
pub fn write_crossing_fixtures(
    dir: &Path,
    files: Vec<(String, String)>,
    ev: &mut Evidence,
) -> Result<(), String> {
    write_fixtures_with(dir, files, ev, Scrub::Places, readme_crossing)
}

/// Writes `files` and the README `readme` gives into `dir`, as [`write_fixtures_scrubbed`]
/// says, the strings they keep added to `ev`'s.
fn write_fixtures_with(
    dir: &Path,
    files: Vec<(String, String)>,
    ev: &mut Evidence,
    scrub: Scrub,
    readme: fn(&Evidence) -> String,
) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("the fixtures directory: {e}"))?;
    let mut strings: BTreeSet<String> = ev.strings.drain(..).collect();
    for (file, text) in files {
        if write_checked(dir, file, &text, ev)? {
            strings.extend(scrub.kept(&text));
        }
    }
    ev.strings = strings.into_iter().collect();
    write_checked(dir, "README.md".to_owned(), &readme(ev), ev)?;
    Ok(())
}

/// Writes `text` into `dir/file` when the privacy check's patterns find nothing in it, and says
/// whether it did; withholds it, named in `ev`, otherwise.
fn write_checked(dir: &Path, file: String, text: &str, ev: &mut Evidence) -> Result<bool, String> {
    let hits = privacy_hits(text);
    if !hits.is_empty() {
        ev.withheld.push(format!("{file}: {}", hits.join(", ")));
        return Ok(false);
    }
    fs::write(dir.join(&file), text).map_err(|e| format!("{file}: {e}"))?;
    ev.fixtures.push(file);
    Ok(true)
}

/// `items` joined by `; `, or `none` when there are none.
pub fn list(items: &[String]) -> String {
    match items.is_empty() {
        true => "none".to_owned(),
        false => items.join("; "),
    }
}
