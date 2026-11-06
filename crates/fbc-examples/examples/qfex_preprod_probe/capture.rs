//! What stage one found, and the files it writes from it: the report (`report.md`), the same
//! machine-readable (`evidence.json`), and the scrubbed slice of the sessions' journal
//! (`fixtures/`, to commit as `fixtures/qfex/exec-recorded/arm/`).
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

use fbc_journal::{ControlEvent, JournalReader, Record};
use fbc_venue_qfex::exec::ARM;
use serde_json::{Map, Value, json};

use crate::evidence::privacy_hits;

/// What one account's reads showed before a session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountRead {
    /// The account the session after them arms on.
    pub before: &'static str,
    /// `primary`, or `subaccount N` in the listing's order, `(armed)` when the session arms on
    /// it: never an id.
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
    pub when: When,
    /// Its fixture file's name.
    pub file: String,
}

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

impl SessionRun {
    /// Whether the session did what stage one asks: the stream authenticated, the arm was
    /// written and its outcome reported, once, with no second connection attempt.
    pub fn ok(&self) -> bool {
        self.authenticated
            && self.arm_sent
            && self.arm.is_some()
            && self.attempts == 1
            && self.error.is_none()
    }

    /// What the capture shows of the arm: an echo of it in its window; else the kinds of what
    /// arrived there, which echo nothing of the arm; else that nothing arrived; and, in either
    /// of the last two, any echo of the arm that arrived after the window, late.
    pub fn answer(&self) -> String {
        let at = |when: When| self.frames.iter().filter(move |f| f.when == when);
        let echo = |f: &&Arrived| {
            f.kind.starts_with("ack echoing the arm") || f.kind.starts_with("err echoing the arm")
        };
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
    /// QFEX pre-production, or a loopback test stub.
    pub target: &'static str,
    /// How many subaccounts `GET /user/subaccounts` listed.
    pub listed: Option<usize>,
    pub reads: Vec<AccountRead>,
    pub sessions: Vec<SessionRun>,
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
}

impl Evidence {
    /// Whether the run did all stage one asks: every session armed, and every file written.
    pub fn ok(&self) -> bool {
        self.stopped.is_none()
            && self.withheld.is_empty()
            && self.errors.is_empty()
            && !self.sessions.is_empty()
            && self.sessions.iter().all(SessionRun::ok)
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
fn envelope(top: &Map<String, Value>) -> Option<(&'static str, &Map<String, Value>)> {
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

/// Ends the arm's window of connection `conn`, if it is open.
fn close(window: &mut [When], conn: u16) {
    if let Some(when) = window.get_mut(usize::from(conn))
        && *when == When::InWindow
    {
        *when = When::AfterWindow;
    }
}

/// Reads shard `shard` of the journal under `journal`, where session `n` ran on connection `n`:
/// marks each session whose arm was written, adds every frame QFEX sent it to its frames, and
/// gives each frame's fixture file and text.
pub fn slice(
    journal: &Path,
    shard: u16,
    sessions: &mut [SessionRun],
) -> Result<Vec<(String, String)>, String> {
    let reader = JournalReader::open(journal, shard).map_err(|e| format!("the journal: {e}"))?;
    let mut window = vec![When::BeforeArm; sessions.len()];
    let mut files = Vec::new();
    for entry in reader.entries() {
        match entry.map_err(|e| format!("the journal: {e}"))?.record {
            Record::Outbound { conn, frame, .. } if frame.bytes() == ARM.as_bytes() => {
                let at = usize::from(conn.conn);
                if let Some(run) = sessions.get_mut(at) {
                    run.arm_sent = true;
                    window[at] = When::InWindow;
                }
            }
            // The arm is the only request a session sends: its deadline ends the window.
            Record::RpcTimeout { stamp, .. } => close(&mut window, stamp.conn.conn),
            Record::Control {
                ev: ControlEvent::Closed(key),
                ..
            } => close(&mut window, key.conn),
            Record::Inbound { stamp, bytes, .. } => {
                let at = usize::from(stamp.conn.conn);
                if let Some(run) = sessions.get_mut(at) {
                    let text = String::from_utf8_lossy(&bytes.0).into_owned();
                    let n = run.frames.len() + 1;
                    let file = format!("{}-{n:03}-{}.json", run.account, window[at].slug());
                    let kind = kind(&text);
                    files.push((file.clone(), scrub(&text)));
                    run.frames.push(Arrived {
                        kind,
                        when: window[at],
                        file,
                    });
                }
            }
            _ => {}
        }
    }
    Ok(files)
}

/// The fixture directory's README.
pub fn readme(ev: &Evidence) -> String {
    let accounts: Vec<&str> = ev.sessions.iter().map(|s| s.account).collect();
    format!(
        "# QFEX trade-socket frames: what answered the cancel-on-disconnect arm\n\n\
         Recorded {} from {} by the owner-run program `qfex_preprod_probe` (crates/fbc-examples, \
         FBC-l931), stage one of decision 0132 item 7: one session per account ({}), each \
         arming cancel-on-disconnect once and placing nothing. These are recorded frames, not \
         synthetic ones: this directory carries no SYNTHETIC marker.\n\n\
         Each file is one frame QFEX sent on a session's trade socket, in the order received, \
         named `<account>-<n>-<when>.json`: `before-arm` (the authentication reply, the \
         subscription confirmations and whatever came before the arm was written), `in-window` \
         (from the arm's write to its deadline or the connection's end, what decision 0132 item \
         3 keeps, so whatever answered the arm is here) and `after-window`.\n\n\
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
         keeping a string value: {}.\n",
        ev.started.get(..10).unwrap_or(""),
        ev.target,
        accounts.join(" then "),
        list(&ev.strings),
    )
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
    let _ = writeln!(
        s,
        "# QFEX pre-production stage one: what answers the cancel-on-disconnect arm\n\n\
         FBC-l931, decision 0132 items 1, 3 and 7. Started {} against {}. Result: {result}.\n",
        ev.started, ev.target
    );
    if let Some(why) = &ev.stopped {
        let _ = writeln!(
            s,
            "**The probe stopped before its sessions ended:** {why}\n"
        );
    }
    let listed = ev
        .listed
        .map_or_else(|| "not read".to_owned(), |n| n.to_string());
    let _ = writeln!(
        s,
        "## The accounts before each session\n\n\
         `GET /user/subaccounts` listed: {listed} (ids not shown). A session starts only when \
         every account shows no open order on `get_user_orders` (a socket that never arms) and \
         flat REST positions (decision 0132 item 7), and, reading that strictly, no REST \
         `open_orders` either.\n\n\
         | before | account | position rows | flat | REST open_orders | page orders | page TWAPs | page full |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for r in &ev.reads {
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            r.before,
            r.label,
            r.rows,
            yn(r.flat),
            r.rest_open_orders,
            r.page_orders,
            r.page_twaps,
            yn(r.page_full)
        );
    }
    let _ = writeln!(s, "\n## The sessions\n");
    for run in &ev.sessions {
        let arm = run.arm.as_deref().unwrap_or("none reported");
        let _ = writeln!(
            s,
            "### {}\n\n\
             - Stream authenticated (authentication and the three subscriptions confirmed): {}\n\
             - Arm written: {}; its outcome as the session reported it: {arm}\n\
             - Ended by: {}; connection attempts {} (failed {}), arm failures {}, frames the \
               codec could not decode {}\n\
             - Error: {}\n\n\
             **What it shows:** {}\n\n\
             | n | when | what | file |\n| --- | --- | --- | --- |",
            run.account,
            yn(run.authenticated),
            yn(run.arm_sent),
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
                f.when.slug(),
                f.kind,
                f.file
            );
        }
        let _ = writeln!(s);
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
         or a drop cancels, and everything decision 0132 asks of a place.",
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
            json!({"before": r.before, "account": r.label, "position_rows": r.rows,
                "flat": r.flat, "rest_open_orders": r.rest_open_orders,
                "page_orders": r.page_orders, "page_twaps": r.page_twaps, "page_full": r.page_full})
        })
        .collect();
    let sessions: Vec<Value> = ev
        .sessions
        .iter()
        .map(|run| {
            let frames: Vec<Value> = run
                .frames
                .iter()
                .map(|f| json!({"when": f.when.slug(), "what": f.kind, "file": f.file}))
                .collect();
            json!({"account": run.account, "authenticated": run.authenticated,
                "arm_sent": run.arm_sent, "arm_outcome": run.arm, "ended_by": run.ended_by,
                "attempts": run.attempts, "failed_attempts": run.failed_attempts,
                "arm_failures": run.arm_failures, "decode_errors": run.decode_errors,
                "error": run.error, "answer": run.answer(), "ok": run.ok(), "frames": frames})
        })
        .collect();
    json!({"ticket": "FBC-l931", "started": ev.started, "target": ev.target, "ok": ev.ok(),
        "stopped": ev.stopped, "subaccounts_listed": ev.listed, "reads": reads,
        "sessions": sessions, "fixtures": ev.fixtures, "kept_string_members": ev.strings,
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
