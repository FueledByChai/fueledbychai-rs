//! The report (`report.md`) and the same machine-readable (`evidence.json`), written from what
//! stage one found ([`Evidence`], [`crate::capture`]), each through the privacy check's patterns
//! before it is written ([`outputs`]). The report answers what decision 0132 items 1, 3 and 7 ask
//! of stage one, for each session and account read; a run that holds a connection first (decision
//! 0146) reports the hold too ([`crate::hold`]).

use std::fmt::Write as _;

use serde_json::{Value, json};

use crate::capture::{Evidence, SessionRun, has_gap, list, partial_accounts, shown};
use crate::evidence::privacy_hits;
use crate::{hold, ledger, refusals};

/// Where the accounts read came from, as `evidence.json` says it (decision 0144 item 3).
const ACCOUNTS_NOTE: &str = "named by the run's configuration, not listed by QFEX: the probe \
     sends no GET /user/subaccounts. The owner must confirm in QFEX's app before the run that the \
     user has no other subaccount; the probe records no such confirmation and cannot check it \
     (decision 0144)";

impl Evidence {
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

/// Whether the arm was written, as the capture can say: yes when the journal shows it, no only
/// when it shows none and the capture is whole, and `None`, not shown, when it is partial.
fn arm_written(run: &SessionRun) -> Option<bool> {
    (run.arm_sent || run.partial.is_empty()).then_some(run.arm_sent)
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
             - Capture: {capture}\n\
             {}\n\n\
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
            run.pages.markdown(),
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
                "partial_because": run.partial, "orders_pages": run.pages.to_json(),
                "frames": frames})
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
