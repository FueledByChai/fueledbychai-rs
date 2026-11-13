//! The provider rate refusals each session, each read socket and the liveness hold's connection
//! met (decision 0135 item 2, FBC-m9rj; decision 0146 item 2). A count above zero is provider
//! rate-limit evidence: it un-defers FBC-s5ox, which records the first `RateLimited` a session
//! meets. The probe provokes none (0135 item 1): it sends only its reads, each session's arm and
//! the hold's authentication and subscriptions, each charged to the shared budget first.
//!
//! Each session and read socket states these counts ([`Refusals`]):
//!
//! - **the limiter's**: how much the run's provider-user limiter's `counts().rejected` rose while
//!   it ran. In a session, the runtime holds the shared budget for each `Effect::RateLimited` its
//!   codec gives (decision 0126). On a read socket, the probe holds it for each `RateLimited` err
//!   the adapter reads in either of the socket's two frames, the authentication reply or the
//!   page, as `account_reads` asks of its caller and as a session's runtime holds it for its
//!   codec's errs in every state. The limiter is user-scoped only (decision 0123,
//!   `RateLimiter::for_user`), so its user count is every refusal it counted. Nothing else counts
//!   on it meanwhile: the sessions and reads run one after the other, and the run's provider user
//!   is its own;
//! - **the RateLimited errs**: frames in an `err` envelope whose `error_code` is `RateLimited`;
//! - **the RATE_LIMITED order responses**: frames whose one member is an `order_response` with
//!   `status` `RATE_LIMITED`, the state QFEX reports a rate-refused order in. A read socket
//!   places nothing and subscribes to no channel, so none is expected on one;
//! - **the frames unread**: frames whose kept bytes are no JSON object, so no refusal can be read
//!   from what the journal keeps of them: one decision 0134 item 2 keeps whole (an echo of
//!   authentication, a JSON escape, a repeated member, a malformed frame), blanked to `2`s, or
//!   one that is not JSON at all;
//! - **on a read socket, the refusals in frames kept whole**: each unread frame read once more,
//!   in memory only, as it arrived (nothing of it is written or shown, only whether it was a
//!   refusal). A refusal QFEX sends in a frame 0134 keeps whole holds no budget (0134 item 6),
//!   so neither the limiter nor the frames as kept can count it, and 0135's "What would show this
//!   was wrong" names exactly that case. A session's frames reach only its codec and the journal,
//!   so a refusal in one it keeps whole is not seen: its unread count says how many such frames
//!   there were.
//!
//! The frames are read as the journal keeps them ([`refusal`]): in a session, every frame its
//! journal kept on its connection, as the slice reads it back (`capture.rs`); on a read socket,
//! whose frames are not journaled, the two it read, each blanked as a journal blanks a frame
//! ([`kept`]) with the spans the adapter names. An err keeps a documented code in clear (decision
//! 0134 item 3). Frames after the page are not read: the probe closes the socket. The hold's
//! connection is counted as a read socket is, every data frame it received read the same way:
//! the probe holds the shared budget for each `RateLimited` err among them, and its frames are not
//! journaled either. It is reported beside the read sockets before it, under [`HOLD`].
//!
//! What a session or read socket **met** ([`Refusals::met`]) is at least the larger of the
//! limiter's count and the refusals its frames show. The two count the same refusals, but each
//! can miss one the other sees (a hold whose frame a partial capture lost, a frame the codec held
//! no budget for, such as a `RATE_LIMITED` order response before or after authentication), and
//! they cannot be matched frame by frame, so the larger is a lower bound, never a total.
//!
//! The REST reads' answers are not read for refusals: QFEX declares no REST bucket (decision
//! 0123), and a 429 or 418 on one stops the probe, its status named where it stopped.

use std::fmt::Write as _;

use fbc_core::{Inbound, InboundSpans, RawFrame};
use fbc_journal::BLANK;
use serde_json::{Map, Value, json};

use crate::capture::{self, Evidence, SessionRun};
use crate::hold::HoldRun;

/// The ticket a refusal met is recorded under (decision 0135 item 2).
pub const RECORD_UNDER: &str = "FBC-s5ox";

/// What the read sockets before the liveness hold read before, and what the hold's own counts
/// are reported under, in place of a session's name.
pub const HOLD: &str = "the hold";

/// What the read sockets after the liveness hold (decision 0146 item 3) are reported under, in
/// place of the session they would read before.
pub const AFTER_HOLD: &str = "after the hold";

/// When a read was made, as the report and the evidence say it: `after the hold`, or before the
/// session or the hold `before` names.
pub fn read_when(before: &str) -> String {
    match before {
        AFTER_HOLD => AFTER_HOLD.to_owned(),
        _ => format!("before {before}"),
    }
}

/// The provider rate refusals one session or read socket met.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Refusals {
    /// How much the run's limiter's `counts().rejected` rose while it ran.
    pub limiter: u64,
    /// Frames in an `err` envelope whose `error_code` is `RateLimited`.
    pub errs: u64,
    /// Frames whose `order_response` has `status` `RATE_LIMITED`.
    pub orders: u64,
    /// Frames whose kept bytes are no JSON object, so none of the counts above reads them.
    pub unread: u64,
    /// On a read socket, the unread frames that were a provider rate refusal as they arrived,
    /// read in memory only ([`Refusals::hear`]). Always 0 for a session, whose frames this
    /// program does not see.
    pub whole: u64,
}

impl Refusals {
    /// How many it met, at least: the larger of the limiter's count and the refusals its frames
    /// show together.
    pub fn met(&self) -> u64 {
        let frames = self.errs.saturating_add(self.orders);
        self.limiter.max(frames.saturating_add(self.whole))
    }

    /// Counts `text`, a frame as the journal keeps it: a provider rate refusal ([`refusal`]), or
    /// a frame no refusal can be read from ([`Refusals::unread`]). Says whether it was a
    /// refusal.
    pub fn count(&mut self, text: &str) -> bool {
        let Some(top) = object(text) else {
            self.unread += 1;
            return false;
        };
        match classify(&top) {
            Some(Refusal::Err) => self.errs += 1,
            Some(Refusal::Order) => self.orders += 1,
            None => return false,
        }
        true
    }

    /// Counts a read socket's frame: `kept`, as a journal would keep it ([`Refusals::count`]),
    /// and, only when that is unread, `arrived`, the frame as it arrived, read in memory for a
    /// refusal kept whole ([`Refusals::whole`]). Nothing of `arrived` is kept.
    pub fn hear(&mut self, kept: &str, arrived: &str) {
        let unread = self.unread;
        self.count(kept);
        if self.unread > unread && refusal(arrived).is_some() {
            self.whole += 1;
        }
    }
}

/// What a journal keeps of `frame` when its codec names `spans` in it, as the slice reads a
/// session's frames back: each span's bytes blanked to [`BLANK`], as the journal blanks them
/// (decision 0028), and the whole frame blanked when the spans do not fit it, as the runtime
/// then marks it (a codec defect); then read as text, UTF-8 or not.
pub fn kept(frame: RawFrame<'_>, spans: &InboundSpans) -> String {
    let mut bytes = frame.bytes().to_vec();
    let end = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    let whole = std::iter::once(0..end).collect();
    let fits = Some(spans).filter(|spans| spans.check(Inbound::Frame(frame)).is_ok());
    let spans = fits.map_or(whole, |spans| spans.body().to_vec());
    for span in spans {
        let end = (span.end as usize).min(bytes.len());
        if let Some(part) = bytes.get_mut(span.start as usize..end) {
            part.fill(BLANK);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// What a provider rate refusal arrived as.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A top-level `err` envelope, nested or flat, whose `error_code` is `RateLimited`.
    Err,
    /// A frame whose one member is an `order_response` whose `status` is `RATE_LIMITED`.
    Order,
}

/// `text` as one JSON object, or `None`.
fn object(text: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(top)) => Some(top),
        _ => None,
    }
}

/// Whether `text`, a frame as the journal keeps it, is a provider rate refusal: an `err`
/// envelope as [`capture::kind`] reads one, with the code `RateLimited` in clear, or an
/// `order_response` as the codec reads a `RATE_LIMITED` one, the frame's one member.
pub fn refusal(text: &str) -> Option<Refusal> {
    classify(&object(text)?)
}

fn classify(top: &Map<String, Value>) -> Option<Refusal> {
    let is = |found: Option<&Value>, want: &str| found.and_then(Value::as_str) == Some(want);
    if let Some(("err", body)) = capture::envelope(top) {
        return is(body.get("error_code"), "RateLimited").then_some(Refusal::Err);
    }
    match (top.len(), top.get("order_response")) {
        (1, Some(Value::Object(order))) => {
            is(order.get("status"), "RATE_LIMITED").then_some(Refusal::Order)
        }
        _ => None,
    }
}

/// One read socket the probe opened before a session or the hold, or after the hold, and the
/// refusals it met.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadSocket {
    /// The session it read before (`primary` or `subaccount`), [`HOLD`], or [`AFTER_HOLD`].
    pub before: &'static str,
    /// The account it read, as the reads name it (`AccountRead::label`): never an id.
    pub label: String,
    pub refusals: Refusals,
}

/// What every read socket and session met, all told, at least.
pub fn total(reads: &[ReadSocket], sessions: &[SessionRun]) -> u64 {
    let reads = reads.iter().map(|r| r.refusals.met());
    let sessions = sessions.iter().map(|run| run.refusals.met());
    reads.chain(sessions).fold(0, u64::saturating_add)
}

/// What the run met, at least: every read socket, every one of `sessions` and the hold's
/// connection.
pub fn met(ev: &Evidence, sessions: &[SessionRun]) -> u64 {
    let hold = ev.hold.as_ref().map_or(0, |hold| hold.refusals.met());
    total(&ev.read_sockets, sessions).saturating_add(hold)
}

/// What each session met, with the read sockets before it, in run order: its name, the count,
/// and whether it ran (a probe stopped by a read before it starts none). The hold is one of
/// them, under [`HOLD`], with the read sockets before it and its own connection, and the read
/// sockets after it are another, under [`AFTER_HOLD`].
pub fn by_session(
    reads: &[ReadSocket],
    sessions: &[SessionRun],
    hold: Option<&HoldRun>,
) -> Vec<(&'static str, u64, bool)> {
    let mut by: Vec<(&'static str, u64, bool)> = Vec::new();
    let names = reads.iter().map(|r| r.before);
    let held = hold.map(|_| HOLD);
    for name in (held.into_iter().chain(names)).chain(sessions.iter().map(|run| run.account)) {
        if by.iter().all(|(seen, ..)| *seen != name) {
            by.push((name, 0, false));
        }
    }
    for (name, met, ran) in &mut by {
        let read = reads.iter().filter(|r| r.before == *name);
        let read = read.map(|r| r.refusals.met()).fold(0, u64::saturating_add);
        let (own, went) = match *name {
            HOLD => (hold.map(|hold| hold.refusals.met()), hold.is_some()),
            AFTER_HOLD => (None, hold.is_some()),
            _ => {
                let run = sessions.iter().find(|run| run.account == *name);
                (run.map(|run| run.refusals.met()), run.is_some())
            }
        };
        *met = read.saturating_add(own.unwrap_or(0));
        *ran = went;
    }
    by
}

/// The fixture files of the frames of `run` that were provider rate refusals, each as `ev`'s
/// files show it: its name when it was written, `(withheld)` when the privacy check's patterns
/// held it back, and `(not written)` otherwise (the fixtures directory removed, or never
/// written).
fn files(run: &SessionRun, ev: &Evidence) -> Vec<String> {
    let frames = run.refusal_frames.iter().filter_map(|&i| run.frames.get(i));
    let named = frames.map(|f| {
        let file = &f.file;
        let withheld = ev
            .withheld
            .iter()
            .any(|w| w.starts_with(&format!("{file}:")));
        match (ev.fixtures.contains(file), withheld) {
            (true, _) => file.clone(),
            (false, true) => format!("{file} (withheld)"),
            (false, false) => format!("{file} (not written)"),
        }
    });
    named.collect()
}

/// Whether `ev`'s fixtures hold a frame of `sessions` that was a provider rate refusal.
fn written(ev: &Evidence, sessions: &[SessionRun]) -> bool {
    let mut frames = sessions.iter().flat_map(|run| {
        let frames = run.refusal_frames.iter().filter_map(|&i| run.frames.get(i));
        frames.map(|f| &f.file)
    });
    frames.any(|file| ev.fixtures.contains(file))
}

/// The report's line naming the evidence, when the run met any.
pub fn headline(ev: &Evidence, sessions: &[SessionRun]) -> Option<String> {
    let met = met(ev, sessions);
    (met > 0).then(|| {
        format!(
            "**Provider rate-limit evidence to record under {RECORD_UNDER}** (decision 0135 item \
             2): provider rate refusals met, at least: {met}. The probe provokes none (0135 item \
             1); the section on them below says where each was met.\n"
        )
    })
}

/// `counts` added up.
fn sum(counts: impl Iterator<Item = u64>) -> u64 {
    counts.fold(0, u64::saturating_add)
}

/// The frames of the read sockets, `sessions` and the hold's connection that were unread.
fn unread(ev: &Evidence, sessions: &[SessionRun]) -> u64 {
    let reads = ev.read_sockets.iter().map(|r| r.refusals.unread);
    let hold = ev.hold.iter().map(|hold| hold.refusals.unread);
    sum(reads
        .chain(hold)
        .chain(sessions.iter().map(|run| run.refusals.unread)))
}

/// The report's section on the provider rate refusals, `sessions` as the report shows them.
pub fn markdown(s: &mut String, ev: &Evidence, sessions: &[SessionRun]) {
    let reads = &ev.read_sockets[..];
    let _ = writeln!(
        s,
        "## Provider rate refusals\n\n\
         Decision 0135 item 2: how many provider rate refusals each session and each read socket \
         met. The probe provokes none (0135 item 1). Each is counted three ways: how much the \
         run's provider-user limiter's `counts().rejected` rose while it ran (a session's holds, \
         which the runtime makes for its codec's `RateLimited`, decision 0126, and a read \
         socket's, which the probe makes for each `RateLimited` err the adapter reads in one of \
         its frames); its frames in an `err` whose `error_code` is `RateLimited`; and its \
         `order_response`s whose `status` is `RATE_LIMITED`. A session's frames are those its \
         journal kept; a read socket's, which are not journaled, the two it read (the \
         authentication reply and the page), each read as a journal would keep it.\n\n\
         A frame the journal keeps whole (decision 0134 item 2: an echo of authentication, a \
         JSON escape, a repeated member) shows no code and holds no budget (0134 item 6), so \
         none of those counts can see a refusal in it: each such frame, and each that is not \
         JSON, is counted unread. A read socket's unread frames are read once more in memory, as \
         they arrived (nothing of them is written), for a refusal kept whole; a session's reach \
         only its codec and the journal, so a refusal in one it keeps whole is not seen here. \
         The REST reads' answers are not read for refusals: a 429 or 418 on one stops the probe, \
         its status named where it stopped.\n\n\
         What one met is at least the larger of the limiter's count and the refusals its frames \
         show: the two count the same refusals, but each can miss one the other sees (a hold \
         whose frame a partial capture lost, a frame the codec held no budget for), so the \
         larger is a lower bound, not a total.\n"
    );
    if ev.hold_asked {
        let _ = writeln!(
            s,
            "The liveness hold's connection (decision 0146) is counted as a read socket is, \
             every data frame it received read the same way, and is listed under \"{HOLD}\" \
             with the read sockets before it.\n"
        );
    }
    let hold = ev.hold.as_ref();
    let hold_met = hold.map_or(0, |hold| hold.refusals.met());
    let read_met = sum(reads.iter().map(|r| r.refusals.met()));
    let hold_whole = hold.map(|hold| hold.refusals.whole);
    let whole = sum(reads.iter().map(|r| r.refusals.whole).chain(hold_whole));
    match met(ev, sessions) > 0 {
        true => {
            let mut said = format!(
                "**This is provider rate-limit evidence, to record under {RECORD_UNDER}.** \
                 {RECORD_UNDER} records the first refusal a session met from that session's \
                 journal (`journal/`), replays it to the effect the codec gave, and judges whether \
                 the user's charged traffic in the declared window before it was within the \
                 declared budget (decision 0135 item 2)."
            );
            if written(ev, sessions) {
                said.push_str(
                    " The fixtures that hold a session's refusal frames are named in the table \
                     below.",
                );
            }
            if read_met > 0 {
                said.push_str(
                    " A refusal met on a read socket has no frame in the journal, since the \
                     probe's reads are not journaled: only its count is here.",
                );
            }
            if hold_met > 0 {
                said.push_str(
                    " A refusal met on the hold's connection has no frame in the journal either: \
                     the hold is not journaled, and only its count is here.",
                );
            }
            if whole > 0 {
                let _ = write!(
                    said,
                    " Of them, {whole} arrived in a frame decision 0134 item 2 keeps whole, which \
                     holds no budget (0134 item 6), read only in memory as it arrived: the case \
                     decision 0135's \"What would show this was wrong\" names, for \
                     {RECORD_UNDER} to judge against that record."
                );
            }
            let _ = writeln!(s, "{said}\n");
        }
        false => {
            let _ = writeln!(s, "No provider rate refusal was counted.\n");
        }
    }
    let _ = writeln!(
        s,
        "Frames unread, kept whole or not JSON: {}. A refusal in a session's unread frame would \
         not show in its counts.\n",
        unread(ev, sessions)
    );
    let by: Vec<String> = (by_session(reads, sessions, hold).into_iter())
        .map(|(name, met, ran)| match (ran, name) {
            (true, _) => format!("{name} {met}"),
            (false, HOLD) => format!("{name} {met} (it did not run)"),
            (false, _) => format!("{name} {met} (its session did not run)"),
        })
        .collect();
    let by = match by.is_empty() {
        true => "none (no read socket opened)".to_owned(),
        false => by.join("; "),
    };
    let _ = writeln!(
        s,
        "Met by each session, at least, with the read sockets before it: {by}.\n\n\
         | session | on | limiter rejected | RateLimited errs | RATE_LIMITED order responses | \
         frames unread | refusals in frames kept whole | met, at least | refusal frames |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    let row = |s: &mut String, session: &str, on: &str, r: &Refusals, whole: &str, frames: &str| {
        let _ = writeln!(
            s,
            "| {session} | {on} | {} | {} | {} | {} | {whole} | {} | {frames} |",
            r.limiter,
            r.errs,
            r.orders,
            r.unread,
            r.met()
        );
    };
    for name in by_session(reads, sessions, hold)
        .into_iter()
        .map(|(name, ..)| name)
    {
        for r in reads.iter().filter(|r| r.before == name) {
            let on = format!("read socket: {}", r.label);
            let whole = r.refusals.whole.to_string();
            row(s, name, &on, &r.refusals, &whole, "not journaled");
        }
        if let Some(hold) = hold.filter(|_| name == HOLD) {
            let on = format!("the hold's connection: {}", hold.account);
            let whole = hold.refusals.whole.to_string();
            row(s, name, &on, &hold.refusals, &whole, "not journaled");
        }
        for run in sessions.iter().filter(|run| run.account == name) {
            let on = match run.partial.is_empty() {
                true => "the session",
                false => "the session (capture partial: its frames' counts may be short)",
            };
            let frames = files(run, ev);
            let frames = match frames.is_empty() {
                true => "none".to_owned(),
                false => frames.join("; "),
            };
            row(s, name, on, &run.refusals, "not seen", &frames);
        }
    }
    let _ = writeln!(s);
}

/// One count's entry in the evidence; `whole` is `None` for a session, which sees none.
fn counts(r: &Refusals, whole: Option<u64>) -> Value {
    json!({"limiter_rejected": r.limiter, "rate_limited_errs": r.errs,
        "rate_limited_order_responses": r.orders, "frames_unread": r.unread,
        "rate_limited_in_frames_kept_whole": whole, "met_at_least": r.met()})
}

/// The evidence's entry for the provider rate refusals, `sessions` as the evidence shows them.
pub fn to_json(ev: &Evidence, sessions: &[SessionRun]) -> Value {
    let reads = &ev.read_sockets[..];
    let hold = ev.hold.as_ref();
    let met = met(ev, sessions);
    let by: Vec<Value> = (by_session(reads, sessions, hold).into_iter())
        .map(|(name, met, ran)| json!({"session": name, "met_at_least": met, "session_ran": ran}))
        .collect();
    let read_sockets: Vec<Value> = (reads.iter())
        .map(|r| {
            let counts = counts(&r.refusals, Some(r.refusals.whole));
            json!({"before": r.before, "account": r.label, "counts": counts})
        })
        .collect();
    let shown: Vec<Value> = (sessions.iter())
        .map(|run| {
            json!({"session": run.account, "capture_whole": run.partial.is_empty(),
                "counts": counts(&run.refusals, None), "frames": files(run, ev)})
        })
        .collect();
    let hold = hold.map(|hold| {
        let counts = counts(&hold.refusals, Some(hold.refusals.whole));
        json!({"account": hold.account, "counts": counts})
    });
    json!({"met_at_least": met, "record_under": (met > 0).then_some(RECORD_UNDER),
        "frames_unread": unread(ev, sessions), "by_session": by,
        "read_sockets": read_sockets, "sessions": shown, "hold": hold})
}
