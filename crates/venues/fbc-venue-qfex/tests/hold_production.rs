//! FBC-sbbl: the owner's liveness hold on QFEX production (FBC-39ks, decision 0146), committed in
//! `fixtures/qfex/exec-recorded/hold-production/`, settles FBC-39ks's step 2 there. The trade
//! codec sends no client keepalive, because QFEX's server pings kept an authenticated connection
//! open for the hour with no server close.
//!
//! The probe wrote each run's `report.md` and `evidence.json`; the README beside them is an
//! agent's. This test holds:
//!
//! - each run's evidence to what the README says of it, every number derived from the evidence;
//! - each report to its evidence;
//! - the codec's doc comment, and `fixtures/qfex/README.md`, to that directory.
//!
//! The probe truncates each time and each interval to a whole millisecond on its own, so an
//! interval may differ by 1 ms from the difference of the two ping times around it.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../fixtures/qfex")
}

fn hold_dir() -> PathBuf {
    fixtures().join("exec-recorded/hold-production")
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn evidence(run: &str) -> Value {
    serde_json::from_str(&read(&hold_dir().join(run).join("evidence.json"))).unwrap()
}

/// Every run of whitespace as one space, so a phrase matches however the text wraps it.
fn flat(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn n(v: &Value) -> u64 {
    v.as_u64().unwrap_or_else(|| panic!("not a count: {v}"))
}

fn ns(v: &Value) -> Vec<u64> {
    v.as_array().unwrap().iter().map(n).collect()
}

fn s(v: &Value) -> &str {
    v.as_str().unwrap_or_else(|| panic!("not a string: {v}"))
}

/// What a run's evidence says of its pings, checked for consistency on the way.
struct Pings {
    authenticated: u64,
    subscribed: u64,
    at: Vec<u64>,
    intervals: Vec<u64>,
    first_after_auth: u64,
}

impl Pings {
    fn of(hold: &Value) -> Pings {
        let at = ns(&hold["pings_after_authentication_ms"]);
        let intervals = ns(&hold["intervals_after_authentication_ms"]);
        let authenticated = n(&hold["authenticated_ms"]);
        let first_after_auth = n(&hold["first_ping_after_authentication_ms"]);
        assert!(at.len() >= 3, "at least three server pings");
        assert_eq!(intervals.len(), at.len() - 1);
        for (i, gap) in intervals.iter().enumerate() {
            let diff = at[i + 1] - at[i];
            assert!(
                gap.abs_diff(diff) <= 1,
                "interval {i}: {gap} against {diff}"
            );
        }
        assert!(first_after_auth.abs_diff(at[0] - authenticated) <= 1);
        // Every ping the hold heard came after authentication, each with an 8-byte payload.
        let pings: Vec<u64> = hold["heard"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|f| f["frame"] == "ping")
            .map(|f| {
                assert_eq!(n(&f["len"]), 8);
                n(&f["at_ms"])
            })
            .collect();
        assert_eq!(pings, at);
        assert_eq!(n(&hold["pings_before_authentication"]), 0);
        assert!(at[0] > authenticated);
        Pings {
            authenticated,
            subscribed: n(&hold["subscribed_ms"]),
            at,
            intervals,
            first_after_auth,
        }
    }

    fn shortest(&self) -> u64 {
        *self.intervals.iter().min().unwrap()
    }

    fn longest(&self) -> u64 {
        *self.intervals.iter().max().unwrap()
    }

    fn last(&self) -> u64 {
        *self.at.last().unwrap()
    }

    /// The median interval, which the README states only when it is a whole one.
    fn median(&self) -> u64 {
        let mut sorted = self.intervals.clone();
        sorted.sort_unstable();
        let mid = sorted.len() / 2;
        if sorted.len().is_multiple_of(2) {
            assert_eq!(sorted[mid - 1], sorted[mid], "a whole median");
        }
        sorted[mid]
    }
}

/// What both runs share: production, the probe mode, the hold's configuration and what it wrote,
/// nothing armed or placed, no order or fill frame, and no rate refusal.
fn common(e: &Value) -> &Value {
    assert_eq!(e["environment"], "production");
    assert_eq!(e["exec_mode"], "production_probe");
    assert_eq!(e["target"], "QFEX production");
    let hold = &e["hold"];
    assert_eq!(hold["environment"], "production");
    assert_eq!(hold["decision"], "0146");
    assert_eq!(hold["account"], "primary");
    assert_eq!(n(&hold["duration_s"]), 3600);
    assert_eq!(hold["opened"], true);
    assert_eq!(hold["armed"], false);
    assert_eq!(hold["placed"], false);
    assert_eq!(hold["error"], Value::Null);
    let sent: Vec<&str> = hold["sent"].as_array().unwrap().iter().map(s).collect();
    assert_eq!(
        sent,
        [
            "authentication",
            "subscribe order_responses",
            "subscribe fills",
            "subscribe positions",
        ]
    );
    assert_eq!(n(&hold["subscriptions_confirmed"]), 3);
    assert_eq!(n(&hold["pongs_received"]), 0, "the hold sent no ping");
    assert_eq!(n(&hold["order_or_fill_frames"]), 0);
    let by_kind = hold["data_frames_by_kind"].as_object().unwrap();
    let kinds: Vec<&str> = by_kind.keys().map(String::as_str).collect();
    assert_eq!(kinds, ["authenticated", "position_response", "subscribed"]);
    assert_eq!(n(&by_kind["authenticated"]), 1);
    assert_eq!(n(&by_kind["subscribed"]), 3);
    assert_eq!(
        by_kind.values().map(n).sum::<u64>(),
        n(&hold["data_frames"])
    );
    assert_eq!(hold["lifetime_longer_than_hold_measured"], false);
    assert_eq!(n(&e["provider_rate_refusals"]["met_at_least"]), 0);
    assert_eq!(n(&e["journal_dropped"]["normal"]), 0);
    assert_eq!(n(&e["journal_dropped"]["safety"]), 0);
    // The accounts read before it: both named accounts, flat, with no open order.
    for read in e["reads"].as_array().unwrap() {
        assert_eq!(read["flat"], true);
        assert_eq!(n(&read["rest_open_orders"]), 0);
        assert_eq!(n(&read["page_orders"]), 0);
        assert_eq!(n(&read["page_twaps"]), 0);
    }
    hold
}

fn reads(e: &Value, when: &str) -> Vec<String> {
    e["reads"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["read"] == when)
        .map(|r| s(&r["account"]).to_owned())
        .collect()
}

#[test]
fn the_hold_ran_its_hour_on_server_pings_with_no_server_close() {
    let e = evidence("");
    let hold = common(&e);
    assert_eq!(e["ok"], true);
    assert_eq!(hold["ok"], true);
    assert_eq!(hold["ended_by"], "its duration, --hold-secs");
    assert!(n(&hold["ended_ms"]) >= 3_600_000);
    assert_eq!(hold["server_closes"], Value::Array(vec![]));
    assert!(
        hold["heard"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["frame"] == "ping"),
        "it heard nothing but pings"
    );
    let pings = Pings::of(hold);
    assert_eq!(
        n(&hold["replies_recorded_on_limiter"]),
        pings.at.len() as u64
    );
    assert_eq!(hold["close_sent_ms"], hold["close_begun_ms"]);
    assert_eq!(hold["close_echoed"], false);
    assert_eq!(hold["accounts_read_after"], "clean");
    assert_eq!(
        reads(&e, "before the hold"),
        ["primary (held)", "subaccount"]
    );
    assert_eq!(
        reads(&e, "after the hold"),
        ["primary (held)", "subaccount"]
    );

    // The README's numbers.
    let readme = flat(&read(&hold_dir().join("README.md")));
    let near = pings
        .intervals
        .iter()
        .filter(|g| (9900..=10100).contains(*g))
        .count();
    let short = pings.intervals.iter().position(|g| *g == pings.shortest());
    let long = pings.intervals.iter().position(|g| *g == pings.longest());
    assert_eq!(short, long.map(|i| i + 1), "the longest, then the shortest");
    for phrase in [
        "No ping came.".to_owned(),
        format!(
            "The connection authenticated at {} ms, and its last subscription was confirmed at {} ms.",
            pings.authenticated, pings.subscribed
        ),
        format!(
            "{} server pings came, each with an 8-byte payload. The first was {} ms after \
             authentication (at {} ms) and the last at {} ms.",
            pings.at.len(),
            pings.first_after_auth,
            pings.at[0],
            pings.last()
        ),
        format!(
            "The {} intervals between them have a median of {} ms. {near} lie between 9900 ms \
             and 10100 ms. The shortest is {} ms and the longest {} ms; those two are consecutive",
            pings.intervals.len(),
            pings.median(),
            pings.shortest(),
            pings.longest()
        ),
        format!(
            "The WebSocket layer wrote a pong for each of the {} pings.",
            pings.at.len()
        ),
        "**Server closes.** None came within the hold.".to_owned(),
        format!(
            "At its duration (`--hold-secs`), at {} ms. It sent its own close at {} ms, and no \
             echo of that close was read within `--read-wait-ms`.",
            n(&hold["ended_ms"]),
            n(&hold["close_sent_ms"])
        ),
        format!(
            "There were {} data frames: `authenticated` 1, `subscribed` 3 and \
             `position_response` {}, and no order or fill frame.",
            n(&hold["data_frames"]),
            n(&hold["data_frames_by_kind"]["position_response"])
        ),
        "Both named accounts read flat, with no open order, before the hold and again after it."
            .to_owned(),
        "**Rate refusals.** None were counted.".to_owned(),
        format!(
            "| The hold | `report.md`, `evidence.json` | 03:01:53Z | its duration, {} ms after the \
             connection opened | complete |",
            n(&hold["ended_ms"])
        ),
    ] {
        assert!(
            readme.contains(&flat(&phrase)),
            "the README lacks: {phrase}"
        );
    }
    assert_eq!(e["started"], "2026-11-15 03:01:53Z");
}

#[test]
fn the_first_run_ended_at_a_server_close_with_no_code_and_could_not_read_after_it() {
    let e = evidence("first-run");
    let hold = common(&e);
    assert_eq!(e["ok"], false);
    assert_eq!(hold["ok"], true, "the hold itself refused nothing");
    assert_eq!(hold["ended_by"], "the server's close");
    let close_at = n(&hold["ended_ms"]);
    let closes = hold["server_closes"].as_array().unwrap();
    assert_eq!(closes.len(), 1);
    assert_eq!(n(&closes[0]["at_ms"]), close_at);
    assert_eq!(closes[0]["code"], Value::Null, "a close with no body");
    assert_eq!(n(&closes[0]["reason_len"]), 0);
    let heard = hold["heard"].as_array().unwrap();
    let last = heard.last().unwrap();
    assert_eq!(last["frame"], "close");
    assert_eq!(last["echo_of_ours"], false);
    assert_eq!(n(&last["at_ms"]), close_at);
    let pings = Pings::of(hold);
    assert_eq!(heard.len(), pings.at.len() + 1, "the pings, then the close");
    assert_eq!(
        hold["close_sent_ms"],
        Value::Null,
        "it sent no close of its own"
    );
    assert_eq!(
        n(&hold["replies_recorded_on_limiter"]),
        pings.at.len() as u64 + 1,
        "a pong for each ping and the close reply"
    );
    let stopped = s(&hold["accounts_read_after"]["stopped"]);
    assert_eq!(stopped.matches("no read socket within 10000 ms").count(), 2);
    assert_eq!(
        reads(&e, "before the hold"),
        ["primary (held)", "subaccount"]
    );
    assert!(reads(&e, "after the hold").is_empty());

    let readme = flat(&read(&hold_dir().join("README.md")));
    for phrase in [
        format!(
            "It authenticated at {} ms, and its last subscription was confirmed at {} ms. No \
             ping came before authentication.",
            pings.authenticated, pings.subscribed
        ),
        format!(
            "{} pings came after authentication, the first {} ms after it and the last at {} ms. \
             The {} intervals between them lie between {} ms and {} ms.",
            pings.at.len(),
            pings.first_after_auth,
            pings.last(),
            pings.intervals.len(),
            pings.shortest(),
            pings.longest()
        ),
        format!(
            "One server close came, at {close_at} ms, {} ms after the last ping. It had no close \
             code (a close with no body) and a 0-byte reason. The hold had sent no close of its \
             own. The WebSocket layer wrote {} replies: {} pongs and the close reply.",
            close_at - pings.last(),
            pings.at.len() + 1,
            pings.at.len()
        ),
        "could not open a socket within 10000 ms on either account".to_owned(),
        format!(
            "| The first run | `first-run/report.md`, `first-run/evidence.json` | 02:05:33Z | a \
             server close, {close_at} ms after the connection opened, during a QFEX outage | \
             incomplete: neither account could be read after it |"
        ),
    ] {
        assert!(
            readme.contains(&flat(&phrase)),
            "the README lacks: {phrase}"
        );
    }
    assert_eq!(e["started"], "2026-11-15 02:05:33Z");
}

/// Each report, as the probe wrote it, states its evidence.
#[test]
fn each_report_states_its_evidence() {
    for (run, result) in [
        ("", "Result: complete."),
        ("first-run", "Result: INCOMPLETE."),
    ] {
        let e = evidence(run);
        let hold = &e["hold"];
        let pings = Pings::of(hold);
        let report = read(&hold_dir().join(run).join("report.md"));
        let head = report.lines().nth(2).unwrap();
        assert!(
            head.contains("Environment: QFEX production"),
            "{run}: {head}"
        );
        assert!(head.contains(&format!("Started {}", s(&e["started"]))));
        assert!(head.ends_with(result), "{run}: {head}");
        for line in [
            "- Any ping before authentication: no".to_owned(),
            format!(
                "- Authenticated: {} ms since the connection opened",
                pings.authenticated
            ),
            format!(
                "- Pings after authentication: {}, the first {} ms after it",
                pings.at.len(),
                pings.first_after_auth
            ),
            format!("- Pings in all: {}; pongs received: 0", pings.at.len()),
            format!(
                "- Replies the WebSocket layer wrote for it, each recorded on the shared limiter: {}",
                n(&hold["replies_recorded_on_limiter"])
            ),
            "- Order or fill frames received: none".to_owned(),
        ] {
            assert!(
                report.lines().any(|l| l == line),
                "{run}: the report lacks {line}"
            );
        }
        let intervals = pings
            .intervals
            .iter()
            .map(|g| format!("{g} ms"))
            .collect::<Vec<_>>()
            .join(", ");
        assert!(report.contains(&format!(
            "- Intervals between the pings after authentication: {intervals} (shortest {} ms, \
             longest {} ms)",
            pings.shortest(),
            pings.longest()
        )));
        // The report's table lists every frame the hold heard, numbered in order.
        let rows: Vec<&str> = report
            .lines()
            .filter(|l| {
                l.starts_with("| ")
                    && l.split(" | ")
                        .next()
                        .is_some_and(|c| c[2..].parse::<u32>().is_ok())
            })
            .collect();
        let heard = hold["heard"].as_array().unwrap();
        assert_eq!(rows.len(), heard.len(), "{run}");
        for (row, frame) in rows.iter().zip(heard) {
            assert!(
                row.contains(&format!(" | {} | ", n(&frame["at_ms"]))),
                "{run}: {row}"
            );
        }
    }
    let complete = read(&hold_dir().join("report.md"));
    assert!(
        complete
            .lines()
            .any(|l| l == "- Server closes: none within the hold")
    );
    assert!(complete.contains("- Ended by: its duration, --hold-secs, at 3600002 ms"));
    let first = read(&hold_dir().join("first-run/report.md"));
    assert!(first.lines().any(|l| {
        l == "- Server closes: code no code (a close with no body), a reason of 0 bytes, at \
              1535872 ms since the connection opened"
    }));
}

/// A run's `started`, `YYYY-MM-DD HH:MM:SSZ`: its day, and its seconds since midnight.
fn started(e: &Value) -> (String, u64) {
    let (day, time) = s(&e["started"]).split_once(' ').unwrap();
    let hms: Vec<u64> = time
        .strip_suffix('Z')
        .unwrap()
        .split(':')
        .map(|f| f.parse().unwrap())
        .collect();
    assert_eq!(hms.len(), 3);
    (day.to_owned(), hms[0] * 3600 + hms[1] * 60 + hms[2])
}

/// The README records what the ticket asks of it, and what neither run measured (decision 0146
/// items 5 and 8), each number it takes from the runs derived from their evidence.
#[test]
fn the_readme_says_what_was_not_measured_and_what_this_settles() {
    let hold = evidence("");
    let first = evidence("first-run");
    let duration = n(&hold["hold"]["duration_s"]);
    assert_eq!(duration, n(&first["hold"]["duration_s"]), "the same flags");
    let close_at = n(&first["hold"]["ended_ms"]);
    let pings = Pings::of(&first["hold"]);
    // The two runs' day, and the hold's start after the first run's close: the close came no
    // earlier than its run's start plus the time its connection had been open.
    let (day, hold_start) = started(&hold);
    let (first_day, first_start) = started(&first);
    assert_eq!(day, first_day);
    let after_close = hold_start - (first_start + close_at.div_ceil(1000));
    assert!(
        (25 * 60..=35 * 60).contains(&after_close),
        "about half an hour: {after_close} s"
    );
    // The first run's last ping before its close, in tenths of a second.
    let tenths = (close_at - pings.last() + 50) / 100;
    let readme = flat(&read(&hold_dir().join("README.md")));
    for phrase in [
        format!("Recorded on {day} on QFEX production, under the probe mode `production_probe`."),
        format!("ran with `--production --hold-secs {duration}` and no `--arm`."),
        "The hold above started about half an hour after that close.".to_owned(),
        format!(
            "The server was still pinging every {} s until {}.{} s before it closed",
            pings.median() / 1000,
            tenths / 10,
            tenths % 10
        ),
        format!(
            "**A lifetime longer than the hold.** A hold that ends at its duration shows only that \
             QFEX did not close the connection within {duration} s, and the first run lasted \
             {close_at} ms."
        ),
        "**An armed connection.** Neither run armed cancel-on-disconnect.".to_owned(),
        "**Pre-production.** The result speaks for QFEX production only (decision 0146 item 8)."
            .to_owned(),
        "Whether QFEX then cancels the account's orders was not measured, since neither run armed."
            .to_owned(),
        "the trade codec sends no client keepalive".to_owned(),
        "(https://docs.qfex.com/websocket/channels/trade/authenticate)".to_owned(),
        "No frame is committed (decision 0146 item 6)".to_owned(),
        "the owner answered \"Yes, commit both\" (FBC-sbbl)".to_owned(),
        "`crates/venues/fbc-venue-qfex/tests/hold_production.rs`".to_owned(),
    ] {
        assert!(
            readme.contains(&flat(&phrase)),
            "the README lacks: {phrase}"
        );
    }
    assert_eq!(hold["hold"]["lifetime_longer_than_hold_measured"], false);
    assert_eq!(first["hold"]["lifetime_longer_than_hold_measured"], false);
    assert_eq!(hold["hold"]["armed"], false);
    assert_eq!(first["hold"]["armed"], false);
    let index = flat(&read(&fixtures().join("README.md")));
    assert!(index.contains("`exec-recorded/hold-production/`"));
}

/// The trade codec's doc comment says it sends no client keepalive, and cites QFEX's
/// authenticate page and the README that records the hold (FBC-sbbl's step 2), with the hold's
/// numbers as its evidence gives them.
#[test]
fn the_codec_s_doc_comment_cites_the_authenticate_page_and_the_hold_s_readme() {
    let hold = evidence("");
    let pings = Pings::of(&hold["hold"]);
    let src = read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec/mod.rs"));
    let module_doc: String = src
        .lines()
        .filter_map(|l| l.strip_prefix("//!"))
        .collect::<Vec<_>>()
        .join("\n");
    let module_doc = flat(&module_doc);
    let start = module_doc
        .find("**Liveness: no client keepalive (FBC-sbbl, decision 0146 item 8).**")
        .expect("the liveness paragraph");
    let end = module_doc[start..]
        .find("- **Subscriptions")
        .map(|i| start + i)
        .unwrap();
    let liveness = &module_doc[start..end];
    for phrase in [
        "The codec sends no keepalive frame and arms no keepalive timer".to_owned(),
        "(https://docs.qfex.com/websocket/channels/trade/authenticate)".to_owned(),
        "`fixtures/qfex/exec-recorded/hold-production/README.md`".to_owned(),
        "After its subscriptions the connection sent nothing but those pongs until its own close \
         at the hour's end."
            .to_owned(),
        format!(
            "No ping came before authentication, a ping came about every {} s after it, and no \
             server close came in {} s.",
            pings.median() / 1000,
            n(&hold["hold"]["duration_s"])
        ),
        "That speaks for production only.".to_owned(),
        "- pre-production;".to_owned(),
        "Whether QFEX cancels the account's orders at such a close".to_owned(),
    ] {
        assert!(
            liveness.contains(&flat(&phrase)),
            "the paragraph lacks: {phrase}"
        );
    }
    assert_eq!(hold["hold"]["server_closes"], Value::Array(vec![]));
    assert!(hold_dir().join("README.md").is_file());
    let src = flat(&src);
    assert!(src.contains("It sends no client keepalive"));
    assert!(src.contains("an hour-long hold measured on production only"));
}
