//! The liveness hold (decision 0146, FBC-39ks): what one trade connection that never arms heard
//! while it was held, and the report's and the evidence's account of it.
//!
//! The hold opens one trade connection on one account the run's configuration names, sends its
//! authentication and then the three subscriptions a session sends (`order_responses`, `fills`,
//! `positions`), and then sends nothing until its end, but the pongs and the close reply the
//! WebSocket layer writes to each server ping and server close and, at its end, its close. It
//! never arms, places, cancels or reads, and sends no ping of its own (decision 0146 item 2).
//! Each frame it writes is charged to the run's shared limiter first, its close as a control
//! frame, as a session charges its own (refused, and the connection dropped, when the limiter
//! has no room), and each reply the WebSocket layer writes for it is recorded there, never
//! refused, as a session records one (decision 0030). It runs before the run's sessions, while
//! no other connection of the probe is open, after every named account was read as before a
//! session, and every named account is read again once its connection is over (item 3). It ends
//! at its duration (`--hold-secs`, from the connection's opening), at the owner's interrupt
//! (Ctrl-C), at a server close, at the connection's end or at a refusal, whichever comes first;
//! nothing reconnects (item 4). `stage.rs` drives the connection; this module holds what it
//! heard ([`HoldRun`]).
//!
//! What it keeps (item 5): each ping, pong and close it received, with its time since the
//! connection opened, a payload or a close reason by its length only and a close code in full,
//! as decision 0041 has the journal keep them; how many data frames it received, by kind
//! ([`kind_of`]: a kind name read in memory from what a journal would keep of the frame, never
//! a value), never what they held, an order or fill frame among them reported as such; each
//! frame it wrote, by what it was, so the report says what it sent, not what it meant to; and
//! the provider rate refusals it met (decision 0135 item 2). A close that arrives
//! after the hold began its own is read as the server's echo of it, never as a server close,
//! whether or not its own was written by then; one whose code is not an echo's (none, or 1000)
//! is flagged as one that may have crossed it. The report says whether any ping came before
//! authentication, the intervals between the pings after it, each server close or that none
//! came, how the hold ended, and that a lifetime longer than the hold was not measured.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use fbc_venue_qfex::exec::CHANNELS;
use serde_json::{Map, Value, json};

use crate::capture;
use crate::refusals::Refusals;

/// The kinds of a frame that reports an order or a fill of the account: what the
/// `order_responses` and `fills` channels the hold subscribes to deliver (the codec's
/// `order_response` and `fill_response`, and the `stop_order_response` of
/// `fixtures/qfex/private/`). One received on the hold means something traded, or an order
/// changed, on the account while it held (decision 0146, "What would show this was wrong").
pub const ORDERS_AND_FILLS: [&str; 3] = ["order_response", "stop_order_response", "fill_response"];

/// The kind a data frame whose kept bytes are no JSON object is counted under: the journal
/// would keep it whole (decision 0134 item 2), or it is not JSON.
pub const UNREAD: &str = "unread (kept whole, or not JSON)";

/// What follows the kind of a frame the journal would keep whole, read in memory as it arrived.
pub const KEPT_WHOLE: &str = "(kept whole, read in memory)";

/// The kind a binary data frame is counted under.
pub const BINARY: &str = "binary";

/// Whether `name`, a member name or a `type` the server wrote, may be shown as a kind: a word of
/// ASCII letters and underscores, at most 64 long, so no id, address or amount can be one.
fn word(name: &str) -> bool {
    let letters = name.bytes().all(|b| b.is_ascii_alphabetic() || b == b'_');
    letters && (1..=64).contains(&name.len())
}

/// The kind of a JSON object, by its message kind: `err` or `ack` for an err or an ack
/// envelope ([`capture::envelope`]); the `type` it names, when that is a word (the
/// authentication reply's `auth`); its one member's name (`subscribed`, `order_response`,
/// `position_response`); otherwise its member names, joined, when every one is a word.
fn kind_of_object(top: &Map<String, Value>) -> String {
    if let Some((envelope, _)) = capture::envelope(top) {
        return envelope.to_owned();
    }
    if let Some(Value::String(kind)) = top.get("type")
        && word(kind)
    {
        return kind.clone();
    }
    let names: Vec<&str> = top.keys().map(String::as_str).collect();
    match names.as_slice() {
        [one] if word(one) => (*one).to_owned(),
        all if !all.is_empty() && all.iter().all(|n| word(n)) => format!("{{{}}}", all.join(", ")),
        _ => "an object whose member names are not shown".to_owned(),
    }
}

/// The kind a text data frame off the hold's connection is counted under, read in memory and
/// never kept: from `kept`, the frame as a journal would keep it, its spans blanked
/// ([`crate::refusals::kept`]); when that is no JSON object ([`UNREAD`]), `arrived`, the frame as
/// it arrived, is read in memory for an order or fill frame or an err alone, counted as one kept
/// whole, so a frame the journal would keep whole still shows as one.
pub fn kind_of(kept: &str, arrived: &str) -> String {
    let object = |text: &str| match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(top)) => Some(kind_of_object(&top)),
        _ => None,
    };
    if let Some(kind) = object(kept) {
        return kind;
    }
    match object(arrived) {
        Some(kind) if ORDERS_AND_FILLS.contains(&kind.as_str()) || kind == "err" => {
            format!("{kind} {KEPT_WHOLE}")
        }
        _ => UNREAD.to_owned(),
    }
}

/// The refusal a text frame the journal would keep whole is when, as it `arrived`, read in
/// memory, it is an err (decision 0146 item 4, an err frame ending the hold): its `error_code`
/// when that is a word ([`word`]: no id, address or amount can be one), otherwise that its code
/// is not shown. `None` for a frame in no err envelope. Nothing of it is kept but that code.
pub fn whole_err(arrived: &str) -> Option<String> {
    let Ok(Value::Object(top)) = serde_json::from_str::<Value>(arrived) else {
        return None;
    };
    let ("err", body) = capture::envelope(&top)? else {
        return None;
    };
    let says = "QFEX answered an err the journal would keep whole, read in memory";
    Some(match body.get("error_code") {
        Some(Value::String(code)) if word(code) => format!("{says}: {code}"),
        _ => format!("{says}, its code not shown"),
    })
}

/// Whether a kind [`kind_of`] gave is an order or fill frame's.
pub fn is_order_or_fill(kind: &str) -> bool {
    let name = kind.split(' ').next().unwrap_or(kind);
    ORDERS_AND_FILLS.contains(&name)
}

/// A control frame the hold's connection received.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Control {
    /// A ping, its payload by length only.
    Ping { len: usize },
    /// A pong, its payload by length only: the hold sends no ping, so one is unsolicited.
    Pong { len: usize },
    /// A close frame: its code in full (`None` for one with no body), its reason by length
    /// only, and whether it came after the hold began its own close, so is read as the
    /// server's echo of it rather than a close of the server's.
    Close {
        code: Option<u16>,
        reason_len: usize,
        echo: bool,
    },
}

/// One control frame, and when it arrived since the connection opened.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Heard {
    pub at: Duration,
    pub control: Control,
}

/// What ended the hold.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum End {
    /// The connection never opened.
    #[default]
    Unopened,
    /// Its duration, `--hold-secs`, from the connection's opening.
    Duration,
    /// The owner's interrupt (Ctrl-C), which ends it as its duration does (decision 0146 item 4).
    Interrupted,
    /// A close frame from the server.
    ServerClose,
    /// The connection's end with no close frame: the stream ended, or failed.
    NoCloseFrame,
    /// A refusal the hold met: the error says which.
    Refused,
}

impl End {
    /// What the report says ended it.
    pub fn says(&self) -> &'static str {
        match self {
            End::Unopened => "the connection never opened",
            End::Duration => "its duration, --hold-secs",
            End::Interrupted => "the owner's interrupt (Ctrl-C)",
            End::ServerClose => "the server's close",
            End::NoCloseFrame => "the connection's end with no close frame",
            End::Refused => "a refusal",
        }
    }
}

/// What the reads of every named account after the hold showed (decision 0146 item 3).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ReadAfter {
    /// Not made: the hold's connection never opened, so nothing was held.
    #[default]
    NotRead,
    /// Every named account showed no open order and flat positions, as before the hold.
    Clean,
    /// A read failed, or an account showed an open order or a position: the error says which.
    Stopped(String),
}

/// What the hold's connection heard.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HoldRun {
    /// The account it authenticated as, `primary` or `subaccount`: never an id.
    pub account: &'static str,
    /// How long it was to be held, from the connection's opening.
    pub duration: Duration,
    /// Whether the connection opened.
    pub opened: bool,
    /// When QFEX's reply that the connection is authenticated arrived, since it opened.
    pub authenticated: Option<Duration>,
    /// How many of the three subscriptions QFEX confirmed.
    pub confirmed: usize,
    /// When the last of the three confirmations arrived, since the connection opened.
    pub subscribed: Option<Duration>,
    /// Every ping, pong and close it received, in order.
    pub heard: Vec<Heard>,
    /// How many data frames it received, the authentication reply and the confirmations among
    /// them: counted, never kept.
    pub data_frames: u64,
    /// The same frames by kind ([`kind_of`]): kind names and counts only.
    pub kinds: BTreeMap<String, u64>,
    /// The replies the WebSocket layer wrote for it, a pong to each ping and a close reply to a
    /// server close, each recorded on the run's shared limiter as a control frame, never
    /// refused, as a session records one (decision 0030).
    pub replies: u64,
    /// Each frame it wrote, in order, by what it was (`authentication`, then
    /// `subscribe <channel>`): recorded once the write returned, so a frame refused or never
    /// built is not among them.
    pub sent: Vec<String>,
    /// When it began its own close, since the connection opened: from then a close it reads is
    /// the echo of its own.
    pub close_begun: Option<Duration>,
    /// When its own close was written, since the connection opened: `None` for one begun and
    /// not written within `--read-wait-ms`.
    pub close_sent: Option<Duration>,
    /// Whether the run's shared limiter had no room for its own close, charged as a control
    /// frame as a session's close is: then it sent none and dropped the connection, as a
    /// session does (decision 0030).
    pub close_refused: bool,
    /// What ended it, and when, since the connection opened.
    pub ended_by: End,
    /// Whether the owner pressed Ctrl-C after something else ended it, while it closed, or as
    /// its duration ended it: the owner's interrupt as much as one that ended it, so a run that
    /// also arms starts no session.
    pub interrupted_closing: bool,
    pub ended_at: Option<Duration>,
    /// For an end with no close frame, the stream's error, when it gave one.
    pub dropped: Option<String>,
    /// Why it was refused, or what failed.
    pub error: Option<String>,
    /// The provider rate refusals it met (decision 0135 item 2): the limiter's count while it
    /// ran, and its frames, each read as a journal would keep it, and in memory as it arrived
    /// when that is unread.
    pub refusals: Refusals,
    /// What the reads of every named account after it showed.
    pub read_after: ReadAfter,
}

/// A duration in whole milliseconds, as the report and the evidence give one.
pub fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl HoldRun {
    /// Whether the hold did what decision 0146 asks: its connection authenticated and QFEX
    /// confirmed its three subscriptions, nothing refused it, no order or fill frame reached it,
    /// and it ended at its duration, at the owner's interrupt, at a server close or at the
    /// connection's end. A server close and an end with no close frame are findings, not
    /// failures.
    pub fn ok(&self) -> bool {
        self.opened
            && self.authenticated.is_some()
            && self.confirmed == CHANNELS.len()
            && self.error.is_none()
            && self.orders_or_fills() == 0
            && matches!(
                self.ended_by,
                End::Duration | End::Interrupted | End::ServerClose | End::NoCloseFrame
            )
    }

    /// How many order or fill frames it received ([`ORDERS_AND_FILLS`]).
    pub fn orders_or_fills(&self) -> u64 {
        let counted = self.kinds.iter().filter(|(kind, _)| is_order_or_fill(kind));
        counted.map(|(_, n)| *n).fold(0, u64::saturating_add)
    }

    /// Why the hold stops the probe, or `None` when it did what decision 0146 asks.
    pub fn stops(&self) -> Option<String> {
        if self.ok() {
            return None;
        }
        Some(match (&self.error, self.orders_or_fills()) {
            (Some(error), _) => format!("the hold: {error}"),
            (None, n) if n > 0 => format!(
                "the hold received {n} order or fill frames: an order or a fill of the account \
                 while it held, which decision 0146 says would show it wrong"
            ),
            (None, _) => format!(
                "the hold: {} before its three subscriptions were confirmed",
                self.ended_by.says()
            ),
        })
    }

    /// When each ping arrived, since the connection opened.
    pub fn pings(&self) -> Vec<Duration> {
        let ping = |h: &Heard| matches!(h.control, Control::Ping { .. }).then_some(h.at);
        self.heard.iter().filter_map(ping).collect()
    }

    /// How many pings came before authentication: every one, when it never authenticated.
    pub fn pings_before_auth(&self) -> usize {
        let before = |at: &Duration| self.authenticated.is_none_or(|auth| *at < auth);
        self.pings().iter().filter(|at| before(at)).count()
    }

    /// When each ping after authentication arrived, since the connection opened.
    pub fn pings_after_auth(&self) -> Vec<Duration> {
        let Some(auth) = self.authenticated else {
            return Vec::new();
        };
        self.pings().into_iter().filter(|at| *at >= auth).collect()
    }

    /// The intervals between the pings after authentication, in order.
    pub fn intervals(&self) -> Vec<Duration> {
        let after = self.pings_after_auth();
        after.windows(2).map(|pair| pair[1] - pair[0]).collect()
    }

    /// The time from authentication to the first ping after it.
    pub fn first_after_auth(&self) -> Option<Duration> {
        let first = *self.pings_after_auth().first()?;
        Some(first - self.authenticated?)
    }

    /// How many pongs it received.
    pub fn pongs(&self) -> usize {
        let pong = |h: &&Heard| matches!(h.control, Control::Pong { .. });
        self.heard.iter().filter(pong).count()
    }

    /// Each close the server sent of its own: every close but the echo of the hold's own.
    pub fn server_closes(&self) -> Vec<Heard> {
        let ours = |h: &&Heard| matches!(h.control, Control::Close { echo: false, .. });
        self.heard.iter().filter(ours).copied().collect()
    }

    /// Whether the server echoed the hold's own close.
    pub fn close_echoed(&self) -> bool {
        self.echo_code().is_some()
    }

    /// The code of the first close read after the hold began its own (`Some(None)` for one with
    /// no body), or `None` when none was read.
    pub fn echo_code(&self) -> Option<Option<u16>> {
        self.heard.iter().find_map(|h| match h.control {
            Control::Close {
                code, echo: true, ..
            } => Some(code),
            _ => None,
        })
    }
}

/// `items` in milliseconds, joined.
fn listed(items: &[Duration]) -> String {
    let items: Vec<String> = items.iter().map(|d| format!("{} ms", ms(*d))).collect();
    items.join(", ")
}

/// What the report says of the hold's own close: whether it began one, when it was written,
/// and what answered it.
fn own_close(hold: &HoldRun) -> String {
    if hold.close_refused {
        return "the run's shared limiter had no room for it, charged as a control frame: it \
                sent none and dropped the connection, as a session does (decision 0030)"
            .to_owned();
    }
    let Some(begun) = hold.close_begun else {
        return "it sent none".to_owned();
    };
    let sent = match hold.close_sent {
        Some(sent) => format!("sent at {} ms", ms(sent)),
        None => format!(
            "begun at {} ms and not written within --read-wait-ms",
            ms(begun)
        ),
    };
    match hold.echo_code() {
        None => format!("{sent}; no echo of it was read within --read-wait-ms"),
        Some(None | Some(1000)) => format!("{sent}; the server echoed it"),
        Some(Some(c)) => format!(
            "{sent}; the close read after it carries code {c}, not an echo's (none, or 1000): \
             it may be a close of the server's that crossed its own"
        ),
    }
}

/// What the report says the hold wrote, from what it recorded writing.
fn wrote(hold: &HoldRun) -> String {
    match hold.sent.as_slice() {
        [] => "It wrote no frame of its own (the error below says why)".to_owned(),
        sent => format!(
            "It wrote, in order: {}; then nothing but the pongs and the close reply the WebSocket \
             layer writes to each server ping and server close and, at its end, its close (a \
             session sends the same authentication and the same three subscriptions: {})",
            sent.join(", "),
            CHANNELS.join(", ")
        ),
    }
}

/// When, as the report says it.
fn at(d: Option<Duration>) -> String {
    d.map_or_else(|| "never".to_owned(), |d| format!("{} ms", ms(d)))
}

/// A close's code as the report and the evidence give it: in full, or that it had no body.
fn code(code: Option<u16>) -> String {
    code.map_or_else(
        || "no code (a close with no body)".to_owned(),
        |c| c.to_string(),
    )
}

/// The data frames by kind, as the report lists them: `none` when it received none.
fn by_kind(hold: &HoldRun) -> String {
    let kinds: Vec<String> = (hold.kinds.iter())
        .map(|(kind, n)| format!("{kind} {n}"))
        .collect();
    match kinds.is_empty() {
        true => "none".to_owned(),
        false => kinds.join("; "),
    }
}

/// What the report says the reads after the hold showed.
fn read_after(hold: &HoldRun) -> String {
    match &hold.read_after {
        ReadAfter::NotRead => "not made: the connection never opened, so nothing was held".into(),
        ReadAfter::Clean => "every named account showed no open order and flat positions, as \
                             before it"
            .into(),
        ReadAfter::Stopped(why) => format!("STOPPED: {why}"),
    }
}

/// The two conditions of a run that holds, which the probe cannot check (decision 0146 item 3).
pub const CONDITIONS: [&str; 2] = [
    "the owner confirmed before the run that the user has no subaccount the configuration \
     leaves out (decision 0144 item 2)",
    "the owner asserts that nothing traded on any account of the user from the first read \
     before the hold to the last read after it",
];

/// The report's section on the hold, run on QFEX `environment`.
pub fn markdown(s: &mut String, hold: &HoldRun, environment: &str) {
    let result = if hold.ok() { "complete" } else { "INCOMPLETE" };
    let _ = writeln!(
        s,
        "## The liveness hold (FBC-39ks, decision 0146)\n\n\
         One trade connection on the {} account, on QFEX {environment}, to be held for {} s from \
         its opening. It never armed, placed, cancelled or read. {}. It sent no ping of its own. \
         Each frame it wrote was charged to the run's shared limiter first, and each pong and \
         close reply the WebSocket layer wrote for it recorded there, as a session's are \
         (decision 0030). No session and no other connection of the probe was open while it \
         was. Its frames are not journaled: each ping, pong and close is kept below by its time \
         since the connection opened, a payload or a close reason by its length only and a close \
         code in full (decision 0041), and its data frames are counted by kind, never kept. \
         Result: {result}.\n\n\
         Every account the run's configuration names was read before the hold opened and again \
         after it ended, whatever ended it (the accounts table's rows `before the hold` and \
         `after the hold`). **Conditions of the run, which the probe cannot check** (decision \
         0146 item 3): {}; and {}.\n",
        hold.account,
        hold.duration.as_secs(),
        wrote(hold),
        CONDITIONS[0],
        CONDITIONS[1],
    );
    let pings = hold.pings();
    let before = hold.pings_before_auth();
    let before = match (before, hold.authenticated) {
        (0, Some(_)) => "no".to_owned(),
        (n, Some(_)) => format!("yes, {n}"),
        (n, None) => format!("{n}, all before any authentication: it never authenticated"),
    };
    let after = hold.pings_after_auth();
    let first = hold.first_after_auth().map_or_else(
        || "no ping came after it".to_owned(),
        |d| format!("the first {} ms after it", ms(d)),
    );
    let intervals = match hold.intervals().as_slice() {
        [] => "none (fewer than two pings after authentication)".to_owned(),
        all => {
            let (lo, hi) = (all.iter().min(), all.iter().max());
            let (lo, hi) = (
                lo.copied().unwrap_or_default(),
                hi.copied().unwrap_or_default(),
            );
            format!(
                "{} (shortest {} ms, longest {} ms)",
                listed(all),
                ms(lo),
                ms(hi)
            )
        }
    };
    let closes: Vec<String> = (hold.server_closes().iter())
        .filter_map(|h| match h.control {
            Control::Close {
                code: c,
                reason_len,
                ..
            } => Some(format!(
                "code {}, a reason of {reason_len} bytes, at {} ms since the connection opened",
                code(c),
                ms(h.at)
            )),
            _ => None,
        })
        .collect();
    let closes = match closes.is_empty() {
        true => "none within the hold".to_owned(),
        false => closes.join("; "),
    };
    let mut ended = match &hold.dropped {
        Some(why) => format!("{} ({why})", hold.ended_by.says()),
        None => hold.ended_by.says().to_owned(),
    };
    if hold.interrupted_closing {
        ended.push_str("; the owner's interrupt (Ctrl-C) came as it closed");
    }
    let echo = own_close(hold);
    let _ = writeln!(
        s,
        "- Connection opened: {}\n\
         - Authenticated: {} since the connection opened\n\
         - Subscriptions confirmed: {} of {}, the last at {}\n\
         - Any ping before authentication: {before}\n\
         - Pings after authentication: {}, {first}\n\
         - Intervals between the pings after authentication: {intervals}\n\
         - Pings in all: {}; pongs received: {}\n\
         - Server closes: {closes}\n\
         - Ended by: {ended}, at {} since the connection opened\n\
         - Its own close: {echo}\n\
         - Replies the WebSocket layer wrote for it, each recorded on the shared limiter: {}\n\
         - Data frames received, counted and never kept: {}; by kind, read from what a journal \
           would keep of each: {}\n\
         - Order or fill frames received: {}\n\
         - Accounts read after it: {}\n\
         - Error: {}\n\n\
         A lifetime longer than the hold was not measured: a hold that ended at its duration \
         shows only that QFEX did not close the connection within it. A code 1002 with an \
         18-byte reason may stand for a close code the WebSocket layer does not allow (1004, \
         1005, 1006, 1015, one from 1016 to 2999, or one outside 1000 to 4999), which it \
         reports in its place. The result speaks for QFEX {environment} only (decision 0146 \
         item 8).\n\n\
         | n | ms since the connection opened | frame | payload or reason bytes | close code |\n\
         | --- | --- | --- | --- | --- |",
        if hold.opened { "yes" } else { "no" },
        at(hold.authenticated),
        hold.confirmed,
        CHANNELS.len(),
        at(hold.subscribed),
        after.len(),
        pings.len(),
        hold.pongs(),
        at(hold.ended_at),
        hold.replies,
        hold.data_frames,
        by_kind(hold),
        match hold.orders_or_fills() {
            0 => "none".to_owned(),
            n => format!(
                "**{n}**: an order or a fill of the account while it held, which decision 0146 \
                 says would show it wrong"
            ),
        },
        read_after(hold),
        hold.error.as_deref().unwrap_or("none"),
    );
    for (n, h) in hold.heard.iter().enumerate() {
        let (frame, len, close) = match h.control {
            Control::Ping { len } => ("ping", len, String::new()),
            Control::Pong { len } => ("pong", len, String::new()),
            Control::Close {
                code: c,
                reason_len,
                echo,
            } => match echo {
                true => ("close, the echo of its own", reason_len, code(c)),
                false => ("close, the server's", reason_len, code(c)),
            },
        };
        let _ = writeln!(
            s,
            "| {} | {} | {frame} | {len} | {close} |",
            n + 1,
            ms(h.at)
        );
    }
    let _ = writeln!(s);
}

/// The evidence's entry for the hold, in milliseconds.
pub fn to_json(hold: &HoldRun, environment: &str) -> Value {
    let heard: Vec<Value> = (hold.heard.iter())
        .map(|h| match h.control {
            Control::Ping { len } => json!({"at_ms": ms(h.at), "frame": "ping", "len": len}),
            Control::Pong { len } => json!({"at_ms": ms(h.at), "frame": "pong", "len": len}),
            Control::Close {
                code,
                reason_len,
                echo,
            } => json!({"at_ms": ms(h.at), "frame": "close", "code": code,
                "reason_len": reason_len, "echo_of_ours": echo}),
        })
        .collect();
    let closes: Vec<Value> = (hold.server_closes().iter())
        .filter_map(|h| match h.control {
            Control::Close {
                code, reason_len, ..
            } => Some(json!({"at_ms": ms(h.at), "code": code, "reason_len": reason_len})),
            _ => None,
        })
        .collect();
    let all_ms = |items: Vec<Duration>| -> Vec<u64> { items.into_iter().map(ms).collect() };
    json!({"ticket": "FBC-39ks", "decision": "0146", "environment": environment,
        "account": hold.account, "duration_s": hold.duration.as_secs(), "ok": hold.ok(),
        "armed": false, "placed": false,
        "sent": hold.sent,
        "opened": hold.opened,
        "authenticated_ms": hold.authenticated.map(ms),
        "subscriptions_confirmed": hold.confirmed, "subscribed_ms": hold.subscribed.map(ms),
        "pings_before_authentication": hold.pings_before_auth(),
        "pings_after_authentication_ms": all_ms(hold.pings_after_auth()),
        "first_ping_after_authentication_ms": hold.first_after_auth().map(ms),
        "intervals_after_authentication_ms": all_ms(hold.intervals()),
        "pongs_received": hold.pongs(),
        "server_closes": closes,
        "ended_by": hold.ended_by.says(), "ended_ms": hold.ended_at.map(ms),
        "interrupted_as_it_closed": hold.interrupted_closing,
        "ended_without_close_frame": hold.dropped,
        "close_begun_ms": hold.close_begun.map(ms), "close_sent_ms": hold.close_sent.map(ms),
        "close_refused_by_limiter": hold.close_refused,
        "close_echoed": hold.close_echoed(),
        "replies_recorded_on_limiter": hold.replies,
        "data_frames": hold.data_frames, "data_frames_by_kind": hold.kinds,
        "order_or_fill_frames": hold.orders_or_fills(),
        "accounts_read_after": match &hold.read_after {
            ReadAfter::NotRead => json!(null),
            ReadAfter::Clean => json!("clean"),
            ReadAfter::Stopped(why) => json!({"stopped": why}),
        },
        "conditions_not_checked": CONDITIONS,
        "error": hold.error,
        "heard": heard,
        "lifetime_longer_than_hold_measured": false})
}
