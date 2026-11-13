//! What stage one found, and the files it writes from it: the report (`report.md`), the same
//! machine-readable (`evidence.json`), and the scrubbed slice of the sessions' journal
//! (`fixtures/`, to commit as `fixtures/qfex/exec-recorded/arm/`, or a production run's as
//! `fixtures/qfex/exec-recorded/arm-production/`, decision 0143 item 4). A run that holds a
//! connection first (decision 0146) reports the hold too ([`crate::hold`]); the hold's frames are
//! not journaled, so the slice holds none of them.
//!
//! **The slice** ([`slice`]) is every frame QFEX sent on a session's trade socket, read back from
//! the journal (decision 0006) with every span the codec named blanked to `2`s: the account's
//! identifiers and credentials (decisions 0028, 0122 item 3) and, in an echo, the spans that
//! decisions 0132 item 3 and 0134 keep in place of the whole frame, so the arm's own echo is
//! there as received but its err message. Each frame is placed against the arm: before it was
//! written, in its window (from its write to its deadline, or the connection's end), or after.
//! A frame that is not a top-level `ack` or `err` envelope echoing a request has every JSON number
//! replaced by `0` ([`scrub`], [`zero_numbers`]), so no amount of the account (a position, a
//! margin, a balance) is published (decision 0009); its member names, strings and nesting are as
//! received. No frame we wrote is kept.
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
//! file is replaced by a note. The run then runs scripts/privacy-check.sh over the fixtures.
//! The patterns know credential and address shapes only, so the report also names every member
//! whose string value a fixture keeps ([`kept_strings`]), for the owner to read each file for an
//! identifying string (an email, a user name, an address in another form) before committing it.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use fbc_journal::{ControlEvent, JournalReader, Marker, Record};
use fbc_venue_qfex::exec::ARM;
use serde_json::{Map, Value, json};

use crate::evidence::privacy_hits;
use crate::hold::{self, HoldRun};
use crate::ledger::{self, RegistryRun};
use crate::refusals::{self, ReadSocket, Refusals};

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
}

impl AccountRead {
    /// Why no session may start on it, or `None` when one may: no open order or TWAP on the
    /// page, every position flat, and no open order counted by REST. Decision 0132 item 7 asks an
    /// empty `get_user_orders` page and flat REST positions. The probe reads it strictly: a REST
    /// `open_orders` count above zero refuses too, since an order the page does not list (a stop
    /// order, which no page says it covers) could be cancelled by the arm (item 2) and its
    /// `CANCELLED` report would then confound the arm's capture.
    pub fn refusal(&self) -> Option<String> {
        if !self.flat || self.page_orders > 0 || self.page_twaps > 0 {
            Some("holds an open order, a TWAP or a position that is not flat".to_owned())
        } else if self.rest_open_orders > 0 {
            Some(format!(
                "shows no open order on get_user_orders, but GET /user/positions counts {}; \
                 the probe reads that disagreement strictly, since an order the page does not \
                 list could be cancelled by the arm and confound its capture",
                self.rest_open_orders
            ))
        } else {
            None
        }
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
    /// Why its capture is partial: each way the journal could be missing its records ([`Gaps`]).
    /// Empty when the journal was written and read whole and kept every record it could hold.
    pub partial: Vec<String>,
}

/// What the capture shows of the arm (decision 0132 item 1).
const NOT_SENT: &str = "No arm was sent: the stream was never reported authenticated. Nothing is \
     known of what answers the arm; the frames before it show why (the authentication reply and \
     the subscription confirmations, whose exact shape FBC-bsou's note asks for first).";
const ACKED: &str = "An ack echoing the arm arrived in its window: the shape decision 0132 item 1 \
     reads as the arm's Final acceptance. FBC-jxau decodes it from this capture; the drop test \
     (0132 item 4) then shows whether the reading holds.";
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
    pub reads: Vec<AccountRead>,
    pub sessions: Vec<SessionRun>,
    /// Each registry's fill ledger: its bounds, and its horizon and replay counts at each of its
    /// sessions' ends (decision 0132 item 5).
    pub registries: Vec<RegistryRun>,
    /// Every read socket the reads opened, and the provider rate refusals each met (decision 0135
    /// item 2); a session's are its own ([`SessionRun::refusals`]).
    pub read_sockets: Vec<ReadSocket>,
    /// Why the probe stopped before its sessions ended.
    pub stopped: Option<String>,
    pub fixtures: Vec<String>,
    /// The members whose string value a written fixture keeps as received ([`kept_strings`]):
    /// names only, for the owner's reading before committing.
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
            && self.withheld.is_empty()
            && self.errors.is_empty()
            && self.dropped == [0, 0]
            && self.writer_failed.is_none()
            && (!self.sessions.is_empty() || self.hold.is_some())
            && self.sessions.iter().all(SessionRun::ok)
            && self.hold.as_ref().is_none_or(HoldRun::ok)
    }

    /// What the report's title says the run is for: the arm's capture, the hold, or both.
    fn about(&self) -> &'static str {
        match (self.hold_asked, self.arms.is_empty()) {
            (false, _) => "what answers the cancel-on-disconnect arm",
            (true, true) => "how a trade connection stays alive (the liveness hold)",
            (true, false) => {
                "how a trade connection stays alive (the liveness hold), and what answers the \
                 cancel-on-disconnect arm"
            }
        }
    }
}

/// Where the accounts read came from, as `evidence.json` says it (decision 0144 item 3).
const ACCOUNTS_NOTE: &str = "named by the run's configuration, not listed by QFEX: the probe \
     sends no GET /user/subaccounts. The owner must confirm in QFEX's app before the run that the \
     user has no other subaccount; the probe records no such confirmation and cannot check it \
     (decision 0144)";

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

/// A fixture's text: as kept when it is a top-level `ack` or `err` envelope echoing a request
/// (an `incoming_message` in the envelope, as [`kind`] reads it), whose values decisions 0132
/// item 3 and 0134 already span; otherwise with every number zeroed, an `incoming_message`
/// nested anywhere else included.
pub fn scrub(text: &str) -> String {
    let echoes = match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(top)) => {
            envelope(&top).is_some_and(|(_, body)| body.contains_key("incoming_message"))
        }
        _ => false,
    };
    match echoes {
        true => text.to_owned(),
        false => zero_numbers(text),
    }
}

/// The members of the JSON `text` whose string value is kept: every member, at any depth (an
/// array's strings under the member that holds it), with a string value that is not empty and
/// not wholly `2`s, the codec's blanking. Names only, never a value.
pub fn kept_strings(text: &str) -> BTreeSet<String> {
    fn walk(value: &Value, under: &str, found: &mut BTreeSet<String>) {
        match value {
            Value::Object(members) => {
                for (name, value) in members {
                    walk(value, name, found);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, under, found)),
            Value::String(s) if !s.chars().all(|c| c == '2') => {
                found.insert(under.to_owned());
            }
            _ => {}
        }
    }
    let mut found = BTreeSet::new();
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        walk(&value, "(top level)", &mut found);
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

/// Ends the arm's window of connection `conn`, if it is open.
fn close(window: &mut [When], conn: u16) {
    if let Some(when) = window.get_mut(usize::from(conn))
        && *when == When::InWindow
    {
        *when = When::AfterWindow;
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
pub fn slice(
    journal: &Path,
    shard: u16,
    ev: &mut Evidence,
) -> Result<Vec<(String, String)>, String> {
    let dropped = ev.dropped[0].saturating_add(ev.dropped[1]);
    let writer_failed = ev.writer_failed.is_some();
    let sessions = &mut ev.sessions[..];
    let mut window = vec![When::BeforeArm; sessions.len()];
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
                read(record, sessions, &mut window, &mut texts);
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
        let file = format!("{}-{:03}-{when}.json", run.account, i + 1);
        run.frames[i].file.clone_from(&file);
        (file, text)
    });
    let files = files.collect();
    cut.map_or(Ok(files), Err)
}

/// Adds what `record` shows of the sessions to them: an arm written, its window's end, a frame
/// QFEX sent (to `texts` too, unnamed), and among those the provider rate refusals and the frames
/// no refusal can be read from, read from the bytes the journal kept ([`Refusals::count`]).
fn read(
    record: Record,
    sessions: &mut [SessionRun],
    window: &mut [When],
    texts: &mut Vec<(usize, usize, String)>,
) {
    match record {
        Record::Outbound { conn, frame, .. } if frame.bytes() == ARM.as_bytes() => {
            let at = usize::from(conn.conn);
            if let Some(run) = sessions.get_mut(at) {
                run.arm_sent = true;
                window[at] = When::InWindow;
            }
        }
        // The arm is the only request a session sends: its deadline ends the window.
        Record::RpcTimeout { stamp, .. } => close(window, stamp.conn.conn),
        Record::Control {
            ev: ControlEvent::Closed(key),
            ..
        } => close(window, key.conn),
        Record::Inbound { stamp, bytes, .. } => {
            let at = usize::from(stamp.conn.conn);
            if let Some(run) = sessions.get_mut(at) {
                let text = String::from_utf8_lossy(&bytes.0).into_owned();
                if run.refusals.count(&text) {
                    run.refusal_frames.push(run.frames.len());
                }
                texts.push((at, run.frames.len(), scrub(&text)));
                run.frames.push(Arrived {
                    kind: kind(&text),
                    when: window[at],
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

/// The fixture directory's README.
pub fn readme(ev: &Evidence) -> String {
    if ev.arms.is_empty() && ev.hold_asked {
        return readme_unarmed(ev);
    }
    let accounts: Vec<&str> = ev.sessions.iter().map(|s| s.account).collect();
    let [normal, safety] = ev.dropped;
    let sessions = shown(ev);
    let gap = match (
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
    };
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
fn has_gap(ev: &Evidence) -> bool {
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
fn shown(ev: &Evidence) -> Vec<SessionRun> {
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
fn partial_accounts(sessions: &[SessionRun]) -> String {
    let partial: Vec<&str> = (sessions.iter())
        .filter(|run| !run.partial.is_empty())
        .map(|run| run.account)
        .collect();
    match partial.is_empty() {
        true => UNPLACED.to_owned(),
        false => partial.join("; "),
    }
}

/// Whether the arm was written, as the capture can say: yes when the journal shows it, no only
/// when it shows none and the capture is whole, and `None`, not shown, when it is partial.
fn arm_written(run: &SessionRun) -> Option<bool> {
    (run.arm_sent || run.partial.is_empty()).then_some(run.arm_sent)
}

/// Writes `files` and the README into `dir`, each only when the privacy check's patterns find
/// nothing in it; one they find something in is withheld and named in `ev`.
pub fn write_fixtures(
    dir: &Path,
    files: Vec<(String, String)>,
    ev: &mut Evidence,
) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("the fixtures directory: {e}"))?;
    let mut strings = BTreeSet::new();
    for (file, text) in files {
        if write_checked(dir, file, &text, ev)? {
            strings.extend(kept_strings(&text));
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

/// The report.
pub fn markdown(ev: &Evidence) -> String {
    let mut s = String::new();
    let result = if ev.ok() { "complete" } else { "INCOMPLETE" };
    let hold = match ev.hold_asked {
        true => " The liveness hold: FBC-39ks and decision 0146.",
        false => "",
    };
    let _ = writeln!(
        s,
        "# QFEX {} stage one: {}\n\n\
         FBC-l931, decision 0132 items 1, 3 and 7, and decision 0143.{hold} Environment: QFEX \
         {}, under the probe mode `{}`. Started {} against {}. Result: {result}.\n",
        ev.environment,
        ev.about(),
        ev.environment,
        ev.mode,
        ev.started,
        ev.target
    );
    let sessions = shown(ev);
    if let Some(line) = refusals::headline(ev, &sessions) {
        let _ = writeln!(s, "{line}");
    }
    if let Some(why) = &ev.stopped {
        let _ = writeln!(
            s,
            "**The probe stopped before its sessions ended:** {why}\n"
        );
    }
    let (gap, partial) = match has_gap(ev) {
        false => ("", "none".to_owned()),
        true => (
            "**The journal is incomplete** (decision 0006): the run is incomplete, and a partial \
             capture concludes nothing of what answered its arm. ",
            partial_accounts(&sessions),
        ),
    };
    let [normal, safety] = ev.dropped;
    let writer = (ev.writer_failed.as_ref())
        .map(|e| format!(" The journal's writer stopped at an error: {e}."))
        .unwrap_or_default();
    let _ = writeln!(
        s,
        "{gap}Records the journal's sink dropped: {normal} Normal, {safety} Safety. Sessions \
         whose capture is partial: {partial}.{writer}\n"
    );
    let (n, names) = (ev.accounts.len(), ev.accounts.join(", "));
    let (held, around) = match ev.hold_asked {
        true => (
            " The liveness hold reads them as a session does before it opens, and again after \
             it ends, whatever ended it (decision 0146 item 3).",
            " and around the hold",
        ),
        false => ("", ""),
    };
    let _ = writeln!(
        s,
        "## The accounts read before each session{around}\n\n\
         Accounts named by the run's configuration, to be read before every session: {n} \
         ({names}; ids not shown), not a list QFEX gave: the probe sends no \
         `GET /user/subaccounts` (decision 0144). The table below lists the reads made. Before \
         the run the owner must confirm in QFEX's app that the user has no subaccount the \
         configuration does not name; the probe records no such confirmation and cannot check \
         it, so the checks below, and what they show of the user's other accounts, cover the \
         named accounts only. A session starts only when every account it reads shows no open \
         order on `get_user_orders` (a socket that never arms) and flat REST positions \
         (decision 0132 item 7), and, reading that strictly, no REST `open_orders` either.{held}\n\n\
         | read | account | position rows | flat | REST open_orders | page orders | page TWAPs | page full |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for r in &ev.reads {
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            refusals::read_when(r.before),
            r.label,
            r.rows,
            yn(r.flat),
            r.rest_open_orders,
            r.page_orders,
            r.page_twaps,
            yn(r.page_full)
        );
    }
    let _ = writeln!(s);
    match &ev.hold {
        Some(held) => hold::markdown(&mut s, held, ev.environment),
        None if ev.hold_asked => {
            let _ = writeln!(
                s,
                "## The liveness hold (FBC-39ks, decision 0146)\n\n\
                 The run asked for a hold, but it did not start: the probe stopped before it.\n"
            );
        }
        None => {}
    }
    let _ = writeln!(s, "## The sessions\n");
    if ev.arms.is_empty() && ev.hold_asked {
        let _ = writeln!(s, "None: the run asked for no session (no --arm).\n");
    }
    for run in &sessions {
        let arm = run.arm.as_deref().unwrap_or("none reported");
        let capture = match run.partial.is_empty() {
            true => "whole: no loss the run found could hold this session's records (no gap \
                     the journal's markers show, no drop they leave unaccounted, no record its \
                     writer left unwritten, no end of the journal left unread)"
                .to_owned(),
            false => format!("PARTIAL: {}", run.partial.join("; ")),
        };
        let _ = writeln!(
            s,
            "### {}\n\n\
             - Stream authenticated (authentication and the three subscriptions confirmed): {}\n\
             - Arm written: {}; its outcome as the session reported it: {arm}\n\
             - Ended by: {}; connection attempts {} (failed {}), arm failures {}, frames the \
               codec could not decode {}\n\
             - Error: {}\n\
             - Capture: {capture}\n\n\
             **What it shows:** {}\n\n\
             | n | when | what | file |\n| --- | --- | --- | --- |",
            run.account,
            yn(run.authenticated),
            arm_written(run).map_or("not shown (the capture is partial)", yn),
            run.ended_by,
            run.attempts,
            run.failed_attempts,
            run.arm_failures,
            run.decode_errors,
            run.error.as_deref().unwrap_or("none"),
            run.answer(),
        );
        for (n, f) in run.frames.iter().enumerate() {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} |",
                n + 1,
                run.position(f),
                f.kind,
                f.file
            );
        }
        let _ = writeln!(s);
    }
    refusals::markdown(&mut s, ev, &sessions);
    if !(ev.arms.is_empty() && ev.hold_asked) {
        ledger::markdown(&mut s, &ev.registries);
    }
    let _ = writeln!(
        s,
        "## Files\n\n\
         Fixtures written: {}. Withheld: {}. Not written or not checked: {}.\n\n\
         Members whose string value a fixture keeps as received: {}. The privacy check knows \
         credential and address shapes only: read every fixture for an identifying string (an \
         email, a user name, an address in another form) before committing it.\n\n\
         Not measured by stage one (FBC-9sk2's stages): whether arming cancels open orders (a \
         session starts only on accounts whose reads above show none), whether a graceful close \
         or a drop cancels, and everything decision 0132 asks of a place. Not checked by the \
         probe: whether the user has a subaccount the configuration does not name (the owner \
         must confirm none before the run, which the probe does not record: decision 0144).",
        list(&ev.fixtures),
        list(&ev.withheld),
        list(&ev.errors),
        list(&ev.strings),
    );
    s
}

/// The evidence, machine-readable.
pub fn to_json(ev: &Evidence) -> Value {
    let reads: Vec<Value> = ev
        .reads
        .iter()
        .map(|r| {
            json!({"read": refusals::read_when(r.before), "before": r.before,
                "account": r.label, "position_rows": r.rows,
                "flat": r.flat, "rest_open_orders": r.rest_open_orders,
                "page_orders": r.page_orders, "page_twaps": r.page_twaps, "page_full": r.page_full})
        })
        .collect();
    let shown = shown(ev);
    let sessions: Vec<Value> = shown
        .iter()
        .map(|run| {
            let frames: Vec<Value> = run
                .frames
                .iter()
                .map(|f| json!({"when": run.position(f), "what": f.kind, "file": f.file}))
                .collect();
            json!({"account": run.account, "authenticated": run.authenticated,
                "arm_sent": arm_written(run), "arm_outcome": run.arm, "ended_by": run.ended_by,
                "attempts": run.attempts, "failed_attempts": run.failed_attempts,
                "arm_failures": run.arm_failures, "decode_errors": run.decode_errors,
                "error": run.error, "answer": run.answer(), "ok": run.ok(),
                "capture": if run.partial.is_empty() { "whole" } else { "partial" },
                "partial_because": run.partial, "frames": frames})
        })
        .collect();
    let [normal, safety] = ev.dropped;
    json!({"ticket": "FBC-l931", "started": ev.started, "environment": ev.environment,
        "exec_mode": ev.mode, "target": ev.target, "ok": ev.ok(),
        "journal_dropped": {"normal": normal, "safety": safety},
        "journal_writer_failed": ev.writer_failed,
        "stopped": ev.stopped,
        "accounts": {"source": "configuration", "named": ev.accounts,
            "count": ev.accounts.len(), "others_checked": false, "note": ACCOUNTS_NOTE},
        "asked": {"arms": ev.arms, "hold": ev.hold_asked},
        "hold": ev.hold.as_ref().map(|held| hold::to_json(held, ev.environment)),
        "reads": reads,
        "sessions": sessions,
        "provider_rate_refusals": refusals::to_json(ev, &shown),
        "registries": ledger::to_json(&ev.registries),
        "fixtures": ev.fixtures, "kept_string_members": ev.strings,
        "fixtures_withheld": ev.withheld, "errors": ev.errors})
}

/// The texts of `report.md` and `evidence.json`, each through the privacy check's patterns: a
/// text they find something in is replaced by a note naming what was found, never the value,
/// and the run fails.
pub fn outputs(ev: &mut Evidence) -> (String, String) {
    let texts = |ev: &Evidence| (markdown(ev), to_json(ev).to_string());
    let (report, evidence) = texts(ev);
    for (name, text) in [("report.md", &report), ("evidence.json", &evidence)] {
        let hits = privacy_hits(text);
        if !hits.is_empty() {
            ev.errors
                .push(format!("{name} withheld: it held {}", hits.join(", ")));
        }
    }
    let (report, evidence) = texts(ev);
    let keep = |text: String| match privacy_hits(&text).is_empty() {
        true => text,
        false => "withheld: the privacy check's patterns found what may be a credential; the \
                  run failed\n"
            .to_owned(),
    };
    (keep(report), keep(evidence))
}

fn yn(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

fn list(items: &[String]) -> String {
    match items.is_empty() {
        true => "none".to_owned(),
        false => items.join("; "),
    }
}
