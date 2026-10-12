//! What the acceptance run found, and the files it writes from it: the evidence report
//! (`report.md`, for the owner and the snapshot-trust decision FBC-2f7w cites), the same
//! evidence machine-readable (`evidence.json`), and the scrubbed slice of the run's journal
//! (`fixtures/`, for `fixtures/paradex/exec-recorded/`).
//!
//! **Scrubbing.** The journal already holds every credential span only as a keyed hash, its
//! bytes blanked when read back (decision 0006, 0028): the login answer's token, the auth
//! frame's. The slice keeps only what the venue sent (inbound frames and HTTP answers, never a
//! frame we wrote, which carries order signatures) and, of that:
//! - each SBE `OrderEvent`, `FillEvent` and `PositionEvent` with its 32 `account` bytes
//!   replaced by the synthetic bytes `a0`..`bf` the hand-built fixtures use; an `AccountEvent`
//!   (a balance) or a template not listed is left out;
//! - each JSON frame or answer with every `account` and `signature` value and every `0x` string
//!   of 40 or more hex digits (an address, a signature) replaced by `SCRUBBED`; a login answer
//!   is left out;
//! - and, before anything is written, every file is checked as scripts/privacy-check.sh checks
//!   (a long `0x` hex value, a bare run of 64 hex digits, a JWT, a bearer token): one that
//!   still matches is left out and the report says so.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use fbc_journal::{JournalReader, Opcode, Record};
use serde_json::{Value, json};

/// Whether a step ran, and if not why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ran {
    Yes,
    /// Not run because the venue's state made it impossible (an empty side of the book).
    Skipped(String),
    /// Not run because an earlier step failed.
    NotRun,
}

/// One command the step sent: its kind, request id, item count and every outcome the session
/// reported for it, in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sent {
    pub kind: String,
    pub rpc: u64,
    pub items: usize,
    pub outcomes: Vec<String>,
}

/// One order of ours: its label in the report, client id as sent, venue id, and each state the
/// registry held it in after each event (the order lattice), with its price and total.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    pub label: String,
    pub client_id: String,
    pub venue_id: Option<String>,
    pub states: Vec<String>,
}

/// A position as GET /positions shows it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestPosition {
    pub side: String,
    pub size: String,
    pub status: String,
}

/// One comparison of GET /orders and GET /positions with the stream, after a step.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Comparison {
    /// What it follows.
    pub after: String,
    /// What the stream showed of each order compared: `present <price> x <size>` or `absent`.
    pub expected: Vec<(String, String)>,
    /// Whether GET /orders agreed with the stream (every order the stream shows open listed
    /// at its price and size, every one it shows ended absent) at some read before the wait
    /// passed.
    pub orders_agreed: bool,
    /// Whether GET /orders, and GET /positions, each agreed at the first read (question (a)
    /// is of GET /orders alone).
    pub first_read_orders_agreed: bool,
    pub first_read_positions_agreed: bool,
    /// From the stream's last change of the orders compared to the first read at which both
    /// agreed. None for a disconnect's position-only comparison: it reads only after the
    /// disconnect's own wait, so its time to agree is no lag behind the stream.
    pub lag_ms: Option<u128>,
    /// How many reads were made.
    pub reads: u32,
    /// What disagreed at the first read, and at the last when it never agreed.
    pub first_mismatches: Vec<String>,
    pub last_mismatches: Vec<String>,
    /// The position the fills imply (the seed plus every fill), in the market's units, and
    /// what GET /positions showed for the market (none: not listed).
    pub stream_position: String,
    pub rest_position: Option<RestPosition>,
    pub positions_agreed: bool,
    /// GET /orders' top-level keys besides `results`, at the first read (a cursor such as
    /// `next` says it paginates).
    pub page_keys: Vec<String>,
    /// The rate-limit headers the first GET /orders answer carried.
    pub limits: Vec<(String, String)>,
    /// A read that failed.
    pub errors: Vec<String>,
    /// What was sent since the comparison before it (the per-IP limit check, design §13.3):
    /// this program's own REST requests (its reads, its logins and the order-book reads), the
    /// WebSocket order methods, and the connections opened (each also makes a login and two
    /// resync reads, and a login every refresh interval while it stays open).
    pub rest_since: u64,
    pub ws_since: u64,
    pub connections_since: u64,
    /// Whether any order was compared: a disconnect's comparison is of the position only (the
    /// stream saw nothing after the socket ended), and is left out of question (a).
    pub orders_compared: bool,
}

/// GET /orders-history?client_id= for one order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History {
    pub label: String,
    /// The status looked for.
    pub wanted: String,
    pub listed: bool,
    /// From the stream showing the order so to the first read listing it so.
    pub lag_ms: Option<u128>,
    pub reads: u32,
    pub status: Option<String>,
    pub cancel_reason: Option<String>,
    /// Whether the history shows part of the order filled (`remaining_size` below `size`).
    pub filled: Option<bool>,
    pub error: Option<String>,
}

/// What became of an order a disconnect should cancel (`cod-dropped`, `cod-graceful`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Disconnect {
    /// How the socket ended: `dropped (no close frame)` or `graceful close`.
    pub how: String,
    /// Whether GET /orders stopped listing the order within the wait (and its history shows
    /// it cancelled, nothing filled), and after how long.
    pub cancelled: bool,
    /// Whether the last GET /orders answer read after the disconnect listed it: false when it
    /// was gone, or no answer was read (then nothing was measured).
    pub still_listed: bool,
    pub after_ms: Option<u128>,
    pub waited_s: u64,
    /// What the order's history said then.
    pub history: Option<History>,
    /// Whether the next connection's resync still showed it resting (it was then
    /// Safety-cancelled, decision 0080).
    pub resting_at_reconnect: Option<bool>,
}

/// One step's evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub name: String,
    pub ran: Ran,
    pub pass: bool,
    pub sent: Vec<Sent>,
    pub orders: Vec<Trace>,
    pub comparisons: Vec<Comparison>,
    pub histories: Vec<History>,
    pub disconnect: Option<Disconnect>,
    pub notes: Vec<String>,
}

impl Step {
    pub fn new(name: &str) -> Step {
        Step {
            name: name.to_owned(),
            ran: Ran::NotRun,
            pass: false,
            sent: Vec::new(),
            orders: Vec::new(),
            comparisons: Vec::new(),
            histories: Vec::new(),
            disconnect: None,
            notes: Vec::new(),
        }
    }
}

/// The whole run's evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Evidence {
    /// When the run started, as a UTC date and time.
    pub started: String,
    /// `Paradex testnet` or `a loopback test stub`.
    pub target: String,
    pub market: String,
    pub caps: String,
    pub steps: Vec<Step>,
    /// Lines worth reading that belong to no step (a phase's resync, a refusal).
    pub notes: Vec<String>,
    /// The order events as received (from the journal), per venue order id.
    pub frames: Vec<FrameRow>,
    /// Fixture files left out by the final privacy check.
    pub withheld: Vec<String>,
    /// A required output that could not be written (the journal, the fixtures): the run fails.
    pub artifact_errors: Vec<String>,
}

impl Evidence {
    /// The step named `name`, added when missing.
    pub fn step(&mut self, name: &str) -> &mut Step {
        if let Some(at) = self.steps.iter().position(|s| s.name == name) {
            return &mut self.steps[at];
        }
        self.steps.push(Step::new(name));
        self.steps.last_mut().expect("just pushed")
    }

    /// True when every step ran (or was skipped by the venue's state) and passed, and every
    /// required output was written.
    pub fn ok(&self) -> bool {
        self.artifact_errors.is_empty()
            && self
                .steps
                .iter()
                .all(|s| s.pass || matches!(s.ran, Ran::Skipped(_)))
    }
}

/// One `OrderEvent` frame as received, read from the journal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrameRow {
    pub venue_id: String,
    pub client_id: String,
    pub status: String,
    pub price: String,
    pub size: String,
    pub size_open: String,
    pub cancel_reason: String,
    /// `requestStatus`/`requestType` and `requestId`, `requestMessage` (version 2).
    pub request_status: Option<u8>,
    pub request_type: Option<u8>,
    pub request_id: Option<String>,
    pub request_message: Option<String>,
}

/// The block offset of each template's 32 `account` bytes (Paradex's schema, as the adapter's
/// decoders read it): `OrderEvent` 76, `FillEvent` 59, `PositionEvent` 81.
fn account_offset(template: u16) -> Option<usize> {
    match template {
        20 => Some(76),
        21 => Some(59),
        22 => Some(81),
        _ => None,
    }
}

/// The SBE frame's template id and version, when it has a header.
fn header(bytes: &[u8]) -> Option<(u16, u16, u16)> {
    let at = |i: usize| {
        bytes
            .get(i..i + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
    };
    Some((at(0)?, at(2)?, at(6)?))
}

/// An `OrderEvent` frame's row, or `None` for another frame or one it cannot read.
pub fn order_row(bytes: &[u8]) -> Option<FrameRow> {
    let (block_len, template, version) = header(bytes)?;
    if template != 20 {
        return None;
    }
    let block = bytes.get(8..8 + usize::from(block_len))?;
    let i64_at = |o: usize| {
        block
            .get(o..o + 8)
            .map(|b| i64::from_le_bytes(b.try_into().expect("eight bytes")))
    };
    let e8 = |o: usize| match i64_at(o) {
        Some(i64::MIN) | None => "null".to_owned(),
        Some(v) => rust_decimal::Decimal::new(v, 8).normalize().to_string(),
    };
    let status = match block.get(16) {
        Some(1) => "NEW",
        Some(2) => "UNTRIGGERED",
        Some(3) => "OPEN",
        Some(4) => "CLOSED",
        _ => "?",
    };
    let mut tail = &bytes[8 + usize::from(block_len)..];
    let mut var = || -> Option<String> {
        let (&len, rest) = tail.split_first()?;
        let s = rest.get(..usize::from(len))?;
        tail = &rest[usize::from(len)..];
        Some(String::from_utf8_lossy(s).into_owned())
    };
    let venue_id = var()?;
    let client_id = var()?;
    let _market = var()?;
    let cancel_reason = var().unwrap_or_default();
    let (request_id, request_message) = if version >= 2 {
        (var(), var())
    } else {
        (None, None)
    };
    let request = |o: usize| (version >= 2).then(|| block.get(o).copied()).flatten();
    Some(FrameRow {
        venue_id,
        client_id,
        status: status.to_owned(),
        price: e8(20),
        size: e8(36),
        size_open: e8(44),
        cancel_reason,
        request_status: request(126),
        request_type: request(127),
        request_id,
        // The venue's own words: shown as testnet_trade shows them.
        request_message: request_message.map(|m| crate::link::shown(&m)),
    })
}

/// `bytes` with its `account` bytes replaced by `a0`..`bf`, or `None` when it is no order, fill
/// or position frame, or too short to hold them.
pub fn scrub_sbe(bytes: &[u8]) -> Option<Vec<u8>> {
    let (block_len, template, _) = header(bytes)?;
    let at = 8 + account_offset(template)?;
    if at + 32 > 8 + usize::from(block_len) || at + 32 > bytes.len() {
        return None;
    }
    let mut out = bytes.to_vec();
    for (i, b) in out[at..at + 32].iter_mut().enumerate() {
        *b = 0xa0 + u8::try_from(i).expect("below 32");
    }
    Some(out)
}

/// Whether `s` is `0x` and 40 or more hex digits.
fn long_hex(s: &str) -> bool {
    s.strip_prefix("0x")
        .is_some_and(|h| h.len() >= 40 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// `v` with every `account` and `signature` value and every long `0x` hex string replaced by
/// `SCRUBBED`.
pub fn scrub_json(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                if matches!(key.as_str(), "account" | "signature") && !value.is_null() {
                    *value = Value::String("SCRUBBED".to_owned());
                } else {
                    scrub_json(value);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(scrub_json),
        Value::String(s) if long_hex(s) => *s = "SCRUBBED".to_owned(),
        _ => {}
    }
}

/// What scripts/privacy-check.sh would refuse in `text`, by name (never the value), or more: a
/// `0x` value of 40 or more hex digits, a bare run of exactly 64 hex digits, anything
/// JWT-shaped, a bearer token (every occurrence), a PEM private-key header. The program also
/// runs the check itself over the written fixtures ([`privacy_check`]).
pub fn privacy_hits(text: &str) -> Vec<&'static str> {
    let bytes = text.as_bytes();
    let mut hits = Vec::new();
    let hex_run = |from: usize| {
        bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_hexdigit())
            .count()
    };
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"0x") && hex_run(i + 2) >= 40 {
            hits.push("a 0x value of 40 or more hex digits");
            break;
        }
        i += 1;
    }
    let mut i = 0;
    while i < bytes.len() {
        let run = hex_run(i);
        if run == 64
            && (i == 0 || !word(bytes[i - 1]))
            && bytes.get(i + 64).is_none_or(|b| !word(*b))
        {
            hits.push("a bare run of 64 hex digits");
            break;
        }
        i += run.max(1);
    }
    if text.contains("eyJ") {
        hits.push("a JWT-shaped value");
    }
    // Every `bearer ` (either case of its first letter, as the check matches it), not the
    // first alone.
    let token_char = |b: &u8| b.is_ascii_alphanumeric() || b"._~+/-".contains(b);
    let bearer = (0..bytes.len()).any(|at| {
        (bytes[at..].starts_with(b"Bearer ") || bytes[at..].starts_with(b"bearer "))
            && bytes[at + 7..].iter().take_while(|b| token_char(b)).count() >= 20
    });
    if bearer {
        hits.push("a bearer token");
    }
    if text.contains("-----BEGIN ") && text.contains("PRIVATE KEY-----") {
        hits.push("a PEM private-key header");
    }
    hits
}

/// A frame written as the hand-built fixtures are: whitespace-separated hex bytes, sixteen to
/// a line, after `#` comment lines.
fn hex_lines(comment: &str, bytes: &[u8]) -> String {
    let mut out = String::new();
    for line in comment.lines() {
        let _ = writeln!(out, "# {line}");
    }
    for chunk in bytes.chunks(16) {
        let line: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let _ = writeln!(out, "{}", line.join(" "));
    }
    out
}

/// What [`write_fixtures`] wrote and left out.
#[derive(Debug, Default)]
pub struct Written {
    pub files: Vec<String>,
    pub withheld: Vec<String>,
    pub order_rows: Vec<FrameRow>,
}

/// Reads shard `shard` of the journal under `journal` and writes its scrubbed inbound slice into
/// `dir` (module documentation), with a README naming `date`, `target` (Paradex testnet or a
/// test stub), `market` and the method, and `rest`, the
/// REST answers the comparisons read (scrubbed alike): what it wrote, what it left out, and
/// the order events read.
pub fn write_fixtures(
    journal: &Path,
    shard: u16,
    dir: &Path,
    date: &str,
    target: &str,
    market: &str,
    rest: &[(String, Vec<u8>)],
) -> Result<Written, String> {
    fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let reader = JournalReader::open(journal, shard).map_err(|e| format!("the journal: {e}"))?;
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut n = 0usize;
    let mut written = Written::default();
    let mut name = |kind: &str, ext: &str| {
        n += 1;
        format!("{n:04}-{kind}.{ext}")
    };
    for entry in reader.entries() {
        let entry = entry.map_err(|e| format!("the journal: {e}"))?;
        match entry.record {
            Record::Inbound { opcode, bytes, .. } => match opcode {
                Opcode::Binary => {
                    let Some((_, template, version)) = header(&bytes.0) else {
                        continue;
                    };
                    if let Some(row) = order_row(&bytes.0) {
                        written.order_rows.push(row);
                    }
                    let Some(scrubbed) = scrub_sbe(&bytes.0) else {
                        continue;
                    };
                    let kind = match template {
                        20 => "order",
                        21 => "fill",
                        _ => "position",
                    };
                    let comment = format!(
                        "Recorded from {target} {date} by paradex_testnet_acceptance: an SBE \
                         frame (template {template}, version {version}), its account bytes \
                         replaced by a0..bf."
                    );
                    files.insert(
                        name(&format!("{kind}-v{version}"), "sbe.txt"),
                        hex_lines(&comment, &scrubbed),
                    );
                }
                _ => {
                    let Ok(mut v) = serde_json::from_slice::<Value>(&bytes.0) else {
                        continue;
                    };
                    scrub_json(&mut v);
                    files.insert(name("frame", "json"), v.to_string());
                }
            },
            Record::HttpResult {
                result: Ok(resp), ..
            } => {
                let Ok(mut v) = serde_json::from_slice::<Value>(&resp.body.0) else {
                    continue;
                };
                // A login answer: its token is blanked in the journal, and it is no fixture.
                if v.get("jwt_token").is_some() {
                    continue;
                }
                scrub_json(&mut v);
                files.insert(name("rest-resync", "json"), v.to_string());
            }
            _ => {}
        }
    }
    for (what, body) in rest {
        let Ok(mut v) = serde_json::from_slice::<Value>(body) else {
            continue;
        };
        scrub_json(&mut v);
        files.insert(name(&format!("rest-{what}"), "json"), v.to_string());
    }
    let readme = format!(
        "# Paradex order-entry frames recorded on testnet\n\n\
         Recorded {date} from {target} ({market}) by the owner-run acceptance program \
         `paradex_testnet_acceptance` (crates/fbc-examples, FBC-xji8) for FBC-8xr: the frames \
         and REST answers the venue sent during one run of place, amend, cancel, batch create, \
         cancel-many, an IOC expiry and two cancel-on-disconnect cases, in the order received.\n\n\
         Method: the order-entry session's journal (decision 0006), which holds every credential \
         only as a keyed hash, read back and scrubbed: only what the venue sent is kept (never a \
         frame we wrote, which carries order signatures); each SBE order, fill and position \
         frame's 32 account bytes are replaced by the synthetic bytes a0..bf; an account (balance) \
         frame is left out; every `account` value and every 0x string of 40 or more hex digits in \
         JSON is replaced by `SCRUBBED`; a login answer is left out. `.sbe.txt` files are written \
         as the hand-built fixtures in fixtures/paradex/exec/ are (hex bytes, `#` comments); \
         `rest-*.json` are the acceptance program's own GET /orders, /positions and \
         /orders-history reads, scrubbed alike. Every file passed the privacy check's patterns \
         before it was written.\n"
    );
    files.insert("README.md".to_owned(), readme);
    for (file, text) in files {
        let hits = privacy_hits(&text);
        if !hits.is_empty() {
            written
                .withheld
                .push(format!("{file}: {}", hits.join(", ")));
            continue;
        }
        fs::write(dir.join(&file), text).map_err(|e| format!("{file}: {e}"))?;
        written.files.push(file);
    }
    Ok(written)
}

/// A yes or no.
fn yn(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

/// An optional lag in milliseconds.
fn ms(lag: Option<u128>) -> String {
    lag.map_or_else(|| "-".to_owned(), |ms| format!("{ms} ms"))
}

/// The report's answers to FBC-2f7w's five questions, FBC-8xr's items and decision 0092's, each
/// `(question, answer)`.
pub fn answers(ev: &Evidence) -> Vec<(String, String)> {
    let comparisons: Vec<&Comparison> = ev.steps.iter().flat_map(|s| &s.comparisons).collect();
    let histories: Vec<&History> = ev
        .steps
        .iter()
        .flat_map(|s| {
            s.histories
                .iter()
                .chain(s.disconnect.iter().flat_map(|d| &d.history))
        })
        .collect();
    let of_orders: Vec<&&Comparison> = comparisons.iter().filter(|c| c.orders_compared).collect();
    let all_orders = !of_orders.is_empty() && of_orders.iter().all(|c| c.orders_agreed);
    let first = of_orders
        .iter()
        .filter(|c| c.first_read_orders_agreed)
        .count();
    // REST's lag behind the stream: of the comparisons of orders (a disconnect's position-only
    // comparison has none, [`Comparison::lag_ms`]).
    let max_lag = of_orders.iter().filter_map(|c| c.lag_ms).max();
    let positions = !comparisons.is_empty() && comparisons.iter().all(|c| c.positions_agreed);
    let sides: Vec<String> = comparisons
        .iter()
        .filter_map(|c| c.rest_position.as_ref())
        .map(|p| format!("{} {} {}", p.side, p.size, p.status))
        .collect();
    let mut keys: Vec<String> = comparisons
        .iter()
        .flat_map(|c| c.page_keys.iter().cloned())
        .collect();
    keys.sort();
    keys.dedup();
    let history_lags: Vec<String> = histories
        .iter()
        .map(|h| {
            format!(
                "{} {}: {}",
                h.label,
                h.wanted,
                if h.listed {
                    format!("listed after {}", ms(h.lag_ms))
                } else {
                    "never listed so".to_owned()
                }
            )
        })
        .collect();
    let ioc = ev
        .steps
        .iter()
        .find(|s| s.name == "ioc")
        .and_then(|s| s.histories.first())
        .map(|h| {
            format!(
                "GET /orders-history: status {}, cancel_reason {}",
                h.status.as_deref().unwrap_or("-"),
                h.cancel_reason.as_deref().unwrap_or("-")
            )
        });
    let ioc_frame =
        ev.frames
            .iter()
            .rev()
            .find(|f| {
                f.status == "CLOSED"
                    && ev.steps.iter().any(|s| {
                        s.name == "ioc" && s.orders.iter().any(|o| o.client_id == f.client_id)
                    })
            })
            .map(|f| {
                format!(
                    "; the closing OrderEvent's cancelReason {:?}",
                    f.cancel_reason
                )
            });
    let cod = |name: &str| {
        ev.steps
            .iter()
            .find(|s| s.name == name)
            .and_then(|s| s.disconnect.as_ref())
            .map_or_else(
                || "not run".to_owned(),
                |d| {
                    let (verdict, listing, after) = if d.cancelled {
                        (
                            "Paradex cancelled the order",
                            "stopped listing it",
                            ms(d.after_ms),
                        )
                    } else if d.after_ms.is_some() {
                        (
                            "not judged: gone, but its history did not show it cancelled with \
                             nothing filled",
                            "stopped listing it",
                            ms(d.after_ms),
                        )
                    } else if d.still_listed {
                        (
                            "the order was NOT cancelled",
                            "still listed it",
                            format!("{} s", d.waited_s),
                        )
                    } else {
                        (
                            "not measured: no GET /orders answer was read after the disconnect",
                            "answered nothing",
                            format!("{} s", d.waited_s),
                        )
                    };
                    format!(
                        "{}: {} (GET /orders {} after {}; history status {}, cancel_reason {}); \
                         still resting at the next connection's resync: {}",
                        d.how,
                        verdict,
                        listing,
                        after,
                        d.history
                            .as_ref()
                            .and_then(|h| h.status.clone())
                            .unwrap_or_else(|| "-".to_owned()),
                        d.history
                            .as_ref()
                            .and_then(|h| h.cancel_reason.clone())
                            .unwrap_or_else(|| "-".to_owned()),
                        d.resting_at_reconnect.map_or("-", yn)
                    )
                },
            )
    };
    // The amend's order events: request_info as received.
    let amended: Vec<&FrameRow> = ev
        .frames
        .iter()
        .filter(|f| {
            f.request_type.is_some_and(|t| t != 0)
                || f.request_id.as_deref().is_some_and(|r| !r.is_empty())
        })
        .collect();
    let amend_rpc = ev
        .steps
        .iter()
        .find(|s| s.name == "amend")
        .and_then(|s| s.sent.first())
        .map(|s| s.rpc);
    let request_info = if amended.is_empty() {
        "no OrderEvent carried request_info".to_owned()
    } else {
        let rows: Vec<String> = amended
            .iter()
            .map(|f| {
                format!(
                    "{} sizeOpen {} requestStatus {:?} requestType {:?} requestId {:?}",
                    f.status, f.size_open, f.request_status, f.request_type, f.request_id
                )
            })
            .collect();
        format!(
            "{} events carried it: [{}]; our order.modify JSON-RPC id was {}",
            amended.len(),
            rows.join("; "),
            amend_rpc.map_or("-".to_owned(), |r| r.to_string())
        )
    };
    let limits: Vec<String> = comparisons
        .iter()
        .map(|c| {
            let headers = if c.limits.is_empty() {
                "no rate-limit header".to_owned()
            } else {
                let l: Vec<String> = c.limits.iter().map(|(k, v)| format!("{k}={v}")).collect();
                l.join(" ")
            };
            format!(
                "{}: {headers} (sent since the comparison before: {} REST requests of this \
                 program's, {} WebSocket order methods, {} connections opened)",
                c.after, c.rest_since, c.ws_since, c.connections_since
            )
        })
        .collect();
    vec![
        (
            "(a) GET /orders lists every order the stream acknowledged and omits every one it ended".to_owned(),
            format!(
                "{} at every comparison ({} of {} agreed at the first read)",
                if all_orders { "yes, eventually" } else { "NO" },
                first,
                of_orders.len()
            ),
        ),
        (
            "(b) REST lag behind the stream".to_owned(),
            format!("at most {}", ms(max_lag)),
        ),
        (
            "(c) GET /positions matches the fills (signed size, LONG/SHORT, CLOSED at 0)".to_owned(),
            format!(
                "{}; positions seen: [{}]",
                if positions { "yes at every comparison" } else { "NO" },
                sides.join("; ")
            ),
        ),
        (
            "(d) GET /orders paginates".to_owned(),
            if keys.is_empty() {
                "no cursor key seen beside results".to_owned()
            } else {
                format!("keys beside results: {}", keys.join(", "))
            },
        ),
        (
            "(e) GET /orders-history?client_id= lag".to_owned(),
            history_lags.join("; "),
        ),
        ("cancel-on-disconnect, dropped socket".to_owned(), cod("cod-dropped")),
        ("cancel-on-disconnect, graceful close (FBC-gkdz)".to_owned(), cod("cod-graceful")),
        (
            "0092 (1): the cancel reason of an IOC that expires unfilled".to_owned(),
            format!(
                "{}{}",
                ioc.unwrap_or_else(|| "not run".to_owned()),
                ioc_frame.unwrap_or_default()
            ),
        ),
        (
            "0092 (2): an IOC/MARKET risk refusal other than NOT_ENOUGH_MARGIN".to_owned(),
            "not exercised: the program never sends an order that could take".to_owned(),
        ),
        (
            "0092 (3): a GTC order closed unfilled by self-trade prevention".to_owned(),
            "not exercised: the program never crosses its own orders".to_owned(),
        ),
        (
            "FBC-g3bw: request_info persistence and requestId echo".to_owned(),
            request_info,
        ),
        (
            "FBC-b0z9 / FBC-olg: order.modify size is the total; modify of a partly filled order".to_owned(),
            "the amend sent the order's total; no partial fill arose (orders rest far from the \
             touch), so total-versus-remaining is not distinguished by this run"
                .to_owned(),
        ),
        (
            "FBC-olg: cancel by client id before the ack".to_owned(),
            "not tested: Paradex declares cancel_before_ack false, so fbc-oms answers AwaitAck and \
             nothing bypasses it"
                .to_owned(),
        ),
        (
            "FBC-nvxn: does a new connection's arm cover orders already open".to_owned(),
            "not exercised: each connection Safety-cancels what its resync shows resting before \
             it places (decision 0080), so no order rests unprotected long enough to tell"
                .to_owned(),
        ),
        (
            "FBC-uiy / FBC-4zz: fill side, realizedFunding, feeCurrency, fill ids, funding without \
             fill ids, PositionEvent size and CLOSED"
                .to_owned(),
            "not exercised unless a fill arose (see the order traces): every order rests far \
             from the touch"
                .to_owned(),
        ),
        (
            "design §13.3: does the per-IP limit count WebSocket order methods".to_owned(),
            format!(
                "judged only from the deployment box: a remaining count that fell by more than \
                 every REST request sent between two samples (this program's, counted below, \
                 plus each connection's login and two resync reads and a login every refresh \
                 interval) counts the WebSocket order methods; the first GET /orders of each \
                 comparison: {}",
                limits.join("; ")
            ),
        ),
    ]
}

/// The report in Markdown.
pub fn markdown(ev: &Evidence) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Paradex testnet acceptance (FBC-8xr)\n");
    let _ = writeln!(
        out,
        "Run {} against {}, market {}, {}. Written by `paradex_testnet_acceptance` \
         (FBC-xji8). Outcome: **{}**.\n",
        ev.started,
        ev.target,
        ev.market,
        ev.caps,
        if ev.ok() {
            "every step passed"
        } else {
            "a step failed"
        }
    );
    let _ = writeln!(out, "## Answers\n\n| Question | Answer |\n| --- | --- |");
    for (q, a) in answers(ev) {
        let _ = writeln!(out, "| {} | {} |", q.replace('|', "/"), a.replace('|', "/"));
    }
    let _ = writeln!(out, "\n## Steps\n");
    let _ = writeln!(
        out,
        "| Step | Ran | Pass | Sent |\n| --- | --- | --- | --- |"
    );
    for s in &ev.steps {
        let sent: Vec<String> = s
            .sent
            .iter()
            .map(|c| format!("{} (request {}, {} items)", c.kind, c.rpc, c.items))
            .collect();
        let ran = match &s.ran {
            Ran::Yes => "yes".to_owned(),
            Ran::Skipped(why) => format!("skipped: {why}"),
            Ran::NotRun => "not run".to_owned(),
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            s.name,
            ran,
            yn(s.pass),
            sent.join("; ")
        );
    }
    for s in &ev.steps {
        let _ = writeln!(out, "\n### {}\n", s.name);
        for c in &s.sent {
            let _ = writeln!(
                out,
                "- sent {} (request {}, {} items): outcomes {}",
                c.kind,
                c.rpc,
                c.items,
                c.outcomes.join(", ")
            );
        }
        for o in &s.orders {
            let _ = writeln!(
                out,
                "- order {} (client id {}, venue id {}): lattice {}",
                o.label,
                o.client_id,
                o.venue_id.as_deref().unwrap_or("-"),
                o.states.join(" -> ")
            );
        }
        if !s.comparisons.is_empty() {
            let _ = writeln!(
                out,
                "\n| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |\n| --- | --- | --- | --- | --- | --- | --- | --- |"
            );
            for c in &s.comparisons {
                let expected: Vec<String> = c
                    .expected
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect();
                let rest = c.rest_position.as_ref().map_or_else(
                    || "not listed".to_owned(),
                    |p| format!("{} {} {}", p.side, p.size, p.status),
                );
                let _ = writeln!(
                    out,
                    "| {} | {} | {} | {} / {} | {} | {} | {} / {} | {} |",
                    c.after,
                    expected.join("; "),
                    yn(c.orders_agreed),
                    yn(c.first_read_orders_agreed),
                    yn(c.first_read_positions_agreed),
                    ms(c.lag_ms),
                    c.reads,
                    c.stream_position,
                    rest,
                    yn(c.positions_agreed)
                );
            }
            for c in &s.comparisons {
                for m in c
                    .first_mismatches
                    .iter()
                    .map(|m| ("first read", m))
                    .chain(c.last_mismatches.iter().map(|m| ("last read", m)))
                {
                    let _ = writeln!(out, "- after {}, {}: {}", c.after, m.0, m.1);
                }
                for e in &c.errors {
                    let _ = writeln!(out, "- after {}: a read failed: {e}", c.after);
                }
            }
        }
        for h in &s.histories {
            let _ = writeln!(
                out,
                "- GET /orders-history?client_id= for {} ({} wanted): listed {}, after {}, {} reads, status {}, cancel_reason {}{}",
                h.label,
                h.wanted,
                yn(h.listed),
                ms(h.lag_ms),
                h.reads,
                h.status.as_deref().unwrap_or("-"),
                h.cancel_reason.as_deref().unwrap_or("-"),
                h.error
                    .as_deref()
                    .map(|e| format!(", error {e}"))
                    .unwrap_or_default()
            );
        }
        if let Some(d) = &s.disconnect {
            let _ = writeln!(
                out,
                "- disconnect: {}; cancelled by Paradex: {} (after {}, waited up to {} s); still listed by the last GET /orders read: {}; still resting at the next resync: {}",
                d.how,
                yn(d.cancelled),
                ms(d.after_ms),
                d.waited_s,
                yn(d.still_listed),
                d.resting_at_reconnect.map_or("-", yn)
            );
        }
        for n in &s.notes {
            let _ = writeln!(out, "- note: {n}");
        }
    }
    if !ev.frames.is_empty() {
        let _ = writeln!(
            out,
            "\n## Order events as received\n\n| Venue id | Client id | Status | Price | Size | Open | Cancel reason | requestStatus | requestType | requestId | requestMessage |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
        );
        for f in &ev.frames {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                f.venue_id,
                f.client_id,
                f.status,
                f.price,
                f.size,
                f.size_open,
                f.cancel_reason,
                f.request_status.map_or("-".to_owned(), |v| v.to_string()),
                f.request_type.map_or("-".to_owned(), |v| v.to_string()),
                f.request_id.as_deref().unwrap_or("-"),
                f.request_message
                    .as_deref()
                    .unwrap_or("-")
                    .replace('|', "/")
            );
        }
    }
    if !ev.notes.is_empty() {
        let _ = writeln!(out, "\n## Notes\n");
        for n in &ev.notes {
            let _ = writeln!(out, "- {n}");
        }
    }
    if !ev.artifact_errors.is_empty() {
        let _ = writeln!(out, "\n## Outputs not written (the run fails)\n");
        for e in &ev.artifact_errors {
            let _ = writeln!(out, "- {e}");
        }
    }
    if !ev.withheld.is_empty() {
        let _ = writeln!(out, "\n## Fixture files left out by the privacy check\n");
        for w in &ev.withheld {
            let _ = writeln!(out, "- {w}");
        }
    }
    out
}

/// The evidence as JSON.
pub fn to_json(ev: &Evidence) -> Value {
    let comparison = |c: &Comparison| {
        json!({
            "after": c.after,
            "expected": c.expected.iter().map(|(k, v)| json!({"order": k, "stream": v})).collect::<Vec<_>>(),
            "orders_agreed": c.orders_agreed,
            "first_read_orders_agreed": c.first_read_orders_agreed,
            "first_read_positions_agreed": c.first_read_positions_agreed,
            "lag_ms": c.lag_ms.map(|v| v as u64),
            "reads": c.reads,
            "first_mismatches": c.first_mismatches,
            "last_mismatches": c.last_mismatches,
            "stream_position": c.stream_position,
            "rest_position": c.rest_position.as_ref().map(|p| json!({"side": p.side, "size": p.size, "status": p.status})),
            "positions_agreed": c.positions_agreed,
            "page_keys": c.page_keys,
            "rate_limit_headers": c.limits.iter().map(|(k, v)| json!({"name": k, "value": v})).collect::<Vec<_>>(),
            "errors": c.errors,
            "rest_requests_since": c.rest_since,
            "ws_order_methods_since": c.ws_since,
            "connections_since": c.connections_since,
            "orders_compared": c.orders_compared,
        })
    };
    let history = |h: &History| {
        json!({
            "order": h.label, "wanted": h.wanted, "listed": h.listed,
            "lag_ms": h.lag_ms.map(|v| v as u64), "reads": h.reads, "status": h.status,
            "cancel_reason": h.cancel_reason, "filled": h.filled, "error": h.error,
        })
    };
    let steps: Vec<Value> = ev
        .steps
        .iter()
        .map(|s| {
            json!({
                "step": s.name,
                "ran": match &s.ran { Ran::Yes => "yes".to_owned(), Ran::Skipped(w) => format!("skipped: {w}"), Ran::NotRun => "not run".to_owned() },
                "pass": s.pass,
                "sent": s.sent.iter().map(|c| json!({"kind": c.kind, "rpc": c.rpc, "items": c.items, "outcomes": c.outcomes})).collect::<Vec<_>>(),
                "orders": s.orders.iter().map(|o| json!({"order": o.label, "client_id": o.client_id, "venue_id": o.venue_id, "lattice": o.states})).collect::<Vec<_>>(),
                "comparisons": s.comparisons.iter().map(comparison).collect::<Vec<_>>(),
                "histories": s.histories.iter().map(history).collect::<Vec<_>>(),
                "disconnect": s.disconnect.as_ref().map(|d| json!({
                    "how": d.how, "cancelled": d.cancelled, "still_listed": d.still_listed, "after_ms": d.after_ms.map(|v| v as u64),
                    "waited_s": d.waited_s, "history": d.history.as_ref().map(history),
                    "resting_at_reconnect": d.resting_at_reconnect,
                })),
                "notes": s.notes,
            })
        })
        .collect();
    json!({
        "program": "paradex_testnet_acceptance",
        "ticket": "FBC-8xr",
        "started": ev.started,
        "target": ev.target,
        "market": ev.market,
        "caps": ev.caps,
        "ok": ev.ok(),
        "answers": answers(ev).into_iter().map(|(q, a)| json!({"question": q, "answer": a})).collect::<Vec<_>>(),
        "steps": steps,
        "order_events": ev.frames.iter().map(|f| json!({
            "venue_id": f.venue_id, "client_id": f.client_id, "status": f.status, "price": f.price,
            "size": f.size, "size_open": f.size_open, "cancel_reason": f.cancel_reason,
            "request_status": f.request_status, "request_type": f.request_type,
            "request_id": f.request_id, "request_message": f.request_message,
        })).collect::<Vec<_>>(),
        "notes": ev.notes,
        "fixtures_withheld": ev.withheld,
        "outputs_not_written": ev.artifact_errors,
    })
}

/// The UTC date and time of `secs` since the epoch, `YYYY-MM-DD HH:MM:SSZ`.
pub fn utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Howard Hinnant's civil-from-days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Runs this repository's scripts/privacy-check.sh over `dir` alone (its own patterns, and the
/// local master key files it checks by default): `Ok` when it passed having scanned every file
/// under `dir`, or what it said (file and line only, never a value). `script` is the check's
/// path.
///
/// The script lists a git work tree's files with git, which leaves out ignored ones: under a
/// checkout's ignored target/ it would scan none and pass. So git is kept from looking above
/// `dir` (it is then no work tree and every file is listed), and the count the script says it
/// scanned must be the number of files under `dir`.
pub fn privacy_check(script: &Path, dir: &Path) -> Result<(), String> {
    if !script.is_file() {
        return Err(format!(
            "the privacy check {} is not there: the fixtures were not checked",
            script.display()
        ));
    }
    let dir = dir
        .canonicalize()
        .map_err(|e| format!("the fixtures directory: {e}"))?;
    let files = count_files(&dir).map_err(|e| format!("the fixtures directory: {e}"))?;
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(script)
        .env("LOOP_ROOT", &dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE");
    if let Some(parent) = dir.parent() {
        cmd.env("GIT_CEILING_DIRECTORIES", parent);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("the privacy check could not run: {e}"))?;
    if out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let scanned = stdout.lines().find_map(|l| {
            l.strip_prefix("privacy: clean (")
                .and_then(|rest| rest.split_once(' '))
                .and_then(|(n, _)| n.parse::<usize>().ok())
        });
        match scanned {
            Some(n) if n == files => Ok(()),
            Some(n) => Err(format!(
                "the privacy check scanned {n} of the {files} fixture files"
            )),
            None => Err("the privacy check did not say how many files it scanned".to_owned()),
        }
    } else {
        let said: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| l.starts_with("privacy: "))
            .map(str::to_owned)
            .collect();
        Err(format!(
            "the privacy check refused the fixtures: {}",
            said.join("; ")
        ))
    }
}

/// How many files are under `dir`, recursively.
fn count_files(dir: &Path) -> std::io::Result<usize> {
    let mut n = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            n += count_files(&entry.path())?;
        } else {
            n += 1;
        }
    }
    Ok(n)
}
