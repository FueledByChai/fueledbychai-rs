//! qfex_preprod_probe's command line, parsed by hand (no argument-parsing dependency). No value
//! typed is ever echoed: a key pasted into the wrong flag would be printed.

use std::path::PathBuf;
use std::time::Duration;

use fbc_runtime::{ProxyConfig, RateUserKey};
use fbc_venue_qfex::exec::AUTH_DEADLINE;
use fbc_venue_qfex::probe::{
    Environment, PREPROD_MDS, PREPROD_PROBE, PREPROD_REST, PREPROD_TRADE, PRODUCTION_MDS,
    PRODUCTION_PROBE, PRODUCTION_REST, PRODUCTION_TRADE,
};

use crate::auth_qfex::{Armed, PUBLIC_VAR, SECRET_VAR, SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR};
use crate::ledger::{self, Bounds, MAX_AGE_S, MAX_ENTRIES, MAX_SPAN_S};

/// The provider user a run's rate limiter counts for: one user, its accounts sharing QFEX's
/// buckets (decision 0123). The program's own numbering: a process probes one user.
pub const USER: RateUserKey = RateUserKey::new(1);

/// The probe codec's resync deadline, which each session's configuration gives it as
/// `qfex.exec.resync_timeout`: a resync not ended by then ends the epoch (decision 0132 item 5).
pub const RESYNC_TIMEOUT: Duration = Duration::from_secs(30);

/// The help text `--help` prints.
pub fn usage() -> String {
    let resync = RESYNC_TIMEOUT.as_secs();
    format!(
        "\
qfex_preprod_probe: stage one of the QFEX probe (decision 0132 item 7, FBC-l931): on QFEX's
PRE-PRODUCTION environment, or with --production on QFEX PRODUCTION (decision 0143), an
owner-assisted run that arms cancel-on-disconnect once per account and records what answers
the arm: on pre-production a declared testnet run (decision 0067), on production with no such
declaration (decision 0143). It places nothing and sends no command but each session's arm,
its resync's reads (one GET /user/positions and get_user_orders pages, decision 0132 item 5)
and the probe's own reads. With --hold-secs it first holds one trade connection that never
arms (the liveness hold, decision 0146, FBC-39ks), and a run may hold without arming at all.

Before anything connects it refuses any origin but the environment's own (or, for a test stub,
all of them loopback addresses): without --production, a production origin is refused; with it,
a pre-production one. It also refuses an account decision 0122 refuses, missing or empty
credentials, a SOCKS5 proxy with loopback origins, and a missing or unaccepted --registry-span,
--ledger-max-age or --ledger-max-entries: fill-ledger bounds that cannot outlast a registry's
sessions and fills (decision 0132 item 5). Then, before each session, it reads the accounts its
configuration names: the primary account, and the subaccount {SUB_ID_VAR} names when it is set
(GET /user/positions, and get_user_orders on a trade socket that never arms). A session starts
only when every one of them holds no open order (none on the page, none counted by REST) and a
flat position. It sends no GET /user/subaccounts (decision 0144): before each run, confirm in
QFEX's app that the user has no subaccount the configuration does not name, which the probe
cannot check. {SUB_ID_VAR} must name a subaccount, never the primary account. Each session
arms once and resyncs. Once the arm has its outcome (its answer, or Unknown at its deadline)
and the resync has ended, or at the connection's end, --session-wait or its registry's span
bound, the program stops it, through its stop control, with no second connection attempt. The
sessions run one after the other, so no two arms are ever in flight across the user.

The liveness hold (decision 0146), before any session and while no other connection of the
probe is open, reads the named accounts as before a session, then opens one trade connection on
the --hold-account: it sends its authentication and the three subscriptions a session sends,
then nothing but the pongs and the close reply the WebSocket layer writes to each server ping
and server close, and never arms, places, cancels or reads. Each frame it writes, and each such
reply, is charged to the run's shared rate limiter as a session's is. It records each ping, pong
and close it receives with its time since the connection opened, and counts its data frames by
kind, keeping none; an order or fill frame on it is reported and stops the probe. It ends at
--hold-secs, at Ctrl-C, at a server close, at the connection's end or at a refusal (its
authentication refused, an err frame, or a subscription unconfirmed at QFEX's 60 s deadline),
with no reconnection. Once its connection is over, whatever ended it, the named accounts are
read again, and the report gives both reads. Ctrl-C is the hold's from its connection's opening
until its close and the read of the echo are over: before the hold ended it ends the hold, and
while the hold closes it is recorded; either stops a run that also arms, so no session starts.
Ctrl-C at any other time ends the program at once, writing no report, as it always has. A run
that holds needs no --arm, and one with no --arm takes no --registry-span, --ledger-max-age or
--ledger-max-entries.

To hold on QFEX production (decision 0146 item 1), with no session:

  cargo run -p fbc-examples --example qfex_preprod_probe -- --production \\
      --evidence-dir /abs/new/dir --hold-secs <S> --socks5 <HOST:PORT>

Two conditions of such a run, which the probe cannot check and its report states (decision
0146 item 3): confirm in QFEX's app before the run that the user has no subaccount the
configuration does not name, and let nothing trade on any account of the user from the first
read to the last. Read report.md before committing it, beside a README of your own, under
fixtures/qfex/exec-recorded/hold-production/ (hold/ for pre-production); no frame is committed.

Usage: cargo run -p fbc-examples --example qfex_preprod_probe -- [OPTIONS]

Options:
  --production             run on QFEX PRODUCTION: the adapter's production_probe mode, the
                           production origins as the defaults, and registries built with no
                           testnet declaration (decision 0143); without it, {PREPROD_PROBE} on
                           pre-production
  --evidence-dir <DIR>     required: an absolute path, empty or not there, where the run writes
                           report.md, evidence.json, its journal (journal/) and the scrubbed
                           fixtures (fixtures/, to commit as fixtures/qfex/exec-recorded/arm/,
                           or as fixtures/qfex/exec-recorded/arm-production/ with --production);
                           a run that only holds writes no frame, and its report.md is committed
                           under fixtures/qfex/exec-recorded/hold/, or hold-production/ with
                           --production, beside the owner's README (decision 0146 item 6)
  --arm <ACCOUNT>          once or twice, in run order: primary, or subaccount (the one
                           {SUB_ID_VAR} names); required unless --hold-secs is given
  --hold-secs <S>          hold one trade connection that never arms for S whole seconds from
                           its opening, 1 to {MAX_HOLD_S} (a day), before any session (decision
                           0146); no default: without it there is no hold, and a run with
                           neither it nor --arm is refused
  --hold-account <ACCOUNT> the account the hold authenticates as: primary (the default with
                           --hold-secs), or subaccount (the one {SUB_ID_VAR} names)
  --rpc-timeout-ms <MS>    the arm's deadline, its window of capture: 501 to 60000 (default
                           10000); it must exceed QFEX's 500 ms speed bump
  --page-size <N>          the get_user_orders page each read asks for, 1 to 1000 (default 1000)
  --read-wait-ms <MS>      how long each read waits for its answer, 1 to 120000 (default 10000)
  --connect-wait-ms <MS>   how long a connection may take to open, 1 to 120000 (default 10000)
  --session-wait <S>       the longest one session runs before the program stops it, up to 3600
                           (default 120); longer than --connect-wait-ms, QFEX's 60 s
                           authentication deadline and the longer of --rpc-timeout-ms and the
                           resync's {resync} s deadline together, so the program never stops a
                           session while its arm or its resync is in flight
  --registry-span <S>      required with --arm, no default, refused without it: the longest
                           from a registry's first session's start to its last session's end,
                           1 to {MAX_SPAN_S}; a session that
                           reaches it is stopped. Longer than --connect-wait-ms, QFEX's 60 s
                           authentication deadline and the longer of --rpc-timeout-ms and the
                           resync's {resync} s deadline together, as --session-wait is. Stage
                           one gives each session its own registry
  --ledger-max-age <S>     required with --arm, no default, refused without it: how long each
                           registry's fill ledger keeps a fill, 1 to {MAX_AGE_S}; longer than
                           --registry-span (decision 0132 item 5)
  --ledger-max-entries <N> required with --arm, no default, refused without it: the most
                           fills it keeps, 1 to {MAX_ENTRIES}; above the fill ceiling, every
                           order a registry's sessions may place at its largest size in size
                           steps (stage one places nothing: 0)
  --trade-url <URL>        default {PREPROD_TRADE}, or {PRODUCTION_TRADE} with --production
  --rest-url <URL>         default {PREPROD_REST}, or {PRODUCTION_REST} with --production
  --mds-url <URL>          default {PREPROD_MDS}, or {PRODUCTION_MDS} with --production
  --socks5 <HOST:PORT>     connect through this SOCKS5 proxy (default: directly)

Credentials, from the environment only:
  {PUBLIC_VAR}, {SECRET_VAR}   the primary account's credential (required)
  {SUB_ID_VAR}                   the subaccount the run reads before every session and arms
                                       on with --arm subaccount, or holds on with --hold-account
                                       subaccount (required with either)
  {SUB_PUBLIC_VAR}, {SUB_SECRET_VAR}
                                       a credential scoped to that subaccount (optional, both
                                       or neither); otherwise the primary credential reads it
  The same variables on either environment, set to that environment's keys (QFEX refused a
  production key on pre-production). With --production the mode is {PRODUCTION_PROBE}. No
  All-accounts key is needed: give each account a key scoped to it alone.
"
    )
}

/// The longest hold `--hold-secs` takes: a day (decision 0146 item 4).
pub const MAX_HOLD_S: u64 = 86_400;

/// The liveness hold a run asks for (decision 0146).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    /// `--hold-secs`: how long the connection is held from its opening.
    pub duration: Duration,
    /// `--hold-account`: the account it authenticates as, the primary unless it names the
    /// subaccount.
    pub account: Armed,
}

/// What to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// Where the evidence is written.
    pub evidence_dir: PathBuf,
    /// The accounts a session arms on, in run order, each at most once: none for a run that
    /// only holds.
    pub arms: Vec<Armed>,
    /// The liveness hold, before any session, when `--hold-secs` asks for one.
    pub hold: Option<Hold>,
    /// `--production`: the run is on QFEX production, under the adapter's `production_probe`
    /// mode (decision 0143 item 3); otherwise on pre-production, under `preprod_probe`.
    pub production: bool,
    pub trade_url: String,
    pub rest_url: String,
    pub mds_url: String,
    pub proxy: ProxyConfig,
    /// Each request's deadline, the arm's included, in milliseconds.
    pub rpc_timeout_ms: u64,
    /// The get_user_orders page size.
    pub page_size: u16,
    pub read_wait: Duration,
    pub connect_wait: Duration,
    pub session_wait: Duration,
    /// Each registry's fill-ledger bounds and the span bound they outlast (decision 0132 item
    /// 5), accepted against stage one's fill ceiling: given exactly when a session arms.
    pub ledger: Option<Bounds>,
    /// The provider user the run's rate limiter counts for, and whose shared budget a refusal
    /// holds (decision 0123): always [`USER`] from the command line. A test that runs several
    /// probes at once in one process gives each its own, since a user's budget is process-wide.
    pub rate_user: RateUserKey,
}

impl Options {
    /// The environment the run's probe mode names.
    pub fn environment(&self) -> Environment {
        environment(self.production)
    }
}

/// The environment `--production` names, or its absence.
fn environment(production: bool) -> Environment {
    match production {
        true => Environment::Production,
        false => Environment::PreProduction,
    }
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Run(Box<Options>),
}

/// Parses the arguments after the program name.
pub fn parse(argv: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    let mut evidence_dir = None;
    let mut arms = Vec::new();
    let mut production = false;
    // Each defaults to the environment's own origin once the flags are read.
    let (mut trade_url, mut rest_url, mut mds_url) = (None, None, None);
    let mut proxy = ProxyConfig::Direct;
    let (mut rpc_timeout_ms, mut page_size) = (10_000, 1000);
    let mut read_wait = Duration::from_millis(10_000);
    let mut connect_wait = Duration::from_millis(10_000);
    let mut session_wait = Duration::from_secs(120);
    // The fill ledger's bounds and the span bound: required with --arm, with no defaults.
    let (mut span, mut max_age, mut max_entries) = (None, None, None);
    // The hold: none unless --hold-secs asks for one.
    let (mut hold_secs, mut hold_account) = (None, None);
    let mut argv = argv.into_iter();
    while let Some(arg) = argv.next() {
        let mut value = || {
            let value = argv
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"));
            value.ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--production" => production = true,
            "--evidence-dir" => evidence_dir = Some(absolute(&value()?)?),
            "--arm" => arms.push(armed(&value()?, &arms)?),
            "--hold-secs" => hold_secs = Some(secs(&arg, &value()?, MAX_HOLD_S)?),
            "--hold-account" => hold_account = Some(held(&value()?)?),
            "--rpc-timeout-ms" => rpc_timeout_ms = number(&arg, &value()?, 501, 60_000)?,
            "--page-size" => page_size = number(&arg, &value()?, 1, 1000)? as u16,
            "--read-wait-ms" => read_wait = millis(&arg, &value()?)?,
            "--connect-wait-ms" => connect_wait = millis(&arg, &value()?)?,
            "--session-wait" => session_wait = secs(&arg, &value()?, 3600)?,
            "--registry-span" => span = Some(secs(&arg, &value()?, MAX_SPAN_S)?),
            "--ledger-max-age" => max_age = Some(secs(&arg, &value()?, MAX_AGE_S)?),
            "--ledger-max-entries" => {
                max_entries = Some(number(&arg, &value()?, 1, MAX_ENTRIES)? as usize);
            }
            "--trade-url" => trade_url = Some(value()?),
            "--rest-url" => rest_url = Some(value()?),
            "--mds-url" => mds_url = Some(value()?),
            "--socks5" => proxy = socks5(&value()?)?,
            _ => return Err("an unknown argument (not shown); --help lists the flags".to_owned()),
        }
    }
    let evidence_dir = evidence_dir.ok_or("--evidence-dir is required")?;
    let hold = match (hold_secs, hold_account) {
        (Some(duration), account) => Some(Hold {
            duration,
            account: account.unwrap_or(Armed::Primary),
        }),
        (None, Some(_)) => {
            return Err("--hold-account names the hold's account: give --hold-secs too".to_owned());
        }
        (None, None) => None,
    };
    if arms.is_empty() && hold.is_none() {
        return Err(
            "--arm or --hold-secs is required: arm on primary, subaccount or both, hold a \
             connection, or both"
                .to_owned(),
        );
    }
    let ledger = match arms.is_empty() {
        true if span.is_some() || max_age.is_some() || max_entries.is_some() => {
            return Err(
                "--registry-span, --ledger-max-age and --ledger-max-entries bound the \
                        registries --arm builds: a run with no --arm takes none of them"
                    .to_owned(),
            );
        }
        true => None,
        false => Some(ledger_bounds(
            (connect_wait, rpc_timeout_ms, session_wait),
            span,
            max_age,
            max_entries,
        )?),
    };
    let [trade, rest, mds] = environment(production).origins();
    Ok(Parsed::Run(Box::new(Options {
        evidence_dir,
        arms,
        hold,
        production,
        trade_url: trade_url.unwrap_or_else(|| trade.to_owned()),
        rest_url: rest_url.unwrap_or_else(|| rest.to_owned()),
        mds_url: mds_url.unwrap_or_else(|| mds.to_owned()),
        proxy,
        rpc_timeout_ms,
        page_size,
        read_wait,
        connect_wait,
        session_wait,
        ledger,
        rate_user: USER,
    })))
}

/// The ledger's bounds of a run that arms, from its three flags, each required, and the waits
/// `(connect_wait, rpc_timeout_ms, session_wait)` a session's arm and resync take.
fn ledger_bounds(
    (connect_wait, rpc_timeout_ms, session_wait): (Duration, u64, Duration),
    span: Option<Duration>,
    max_age: Option<Duration>,
    max_entries: Option<usize>,
) -> Result<Bounds, String> {
    // A session opens and authenticates within QFEX's deadline, then arms and resyncs at once,
    // waiting out the arm's deadline and the resync's: a --session-wait no longer than the
    // three could stop it with its arm in flight, spending that account's capture with no
    // outcome, or with a resync read unanswered in its journal. The span bound stops a session
    // too, so it must outlast the same three.
    let settled = Duration::from_millis(rpc_timeout_ms).max(RESYNC_TIMEOUT);
    let to_settle = connect_wait + AUTH_DEADLINE + settled;
    let (auth_s, resync_s) = (AUTH_DEADLINE.as_secs(), RESYNC_TIMEOUT.as_secs());
    if session_wait <= to_settle {
        return Err(format!(
            "--session-wait must be longer than --connect-wait-ms, QFEX's {auth_s} s \
             authentication deadline and the longer of --rpc-timeout-ms and the resync's \
             {resync_s} s deadline together, or a session could be stopped with its arm or its \
             resync in flight (the values are not shown)"
        ));
    }
    let span = span.ok_or("--registry-span is required: it has no default")?;
    let max_age = max_age.ok_or("--ledger-max-age is required: it has no default")?;
    let max_entries = max_entries.ok_or("--ledger-max-entries is required: it has no default")?;
    if span <= to_settle {
        return Err(format!(
            "--registry-span must be longer than --connect-wait-ms, QFEX's {auth_s} s \
             authentication deadline and the longer of --rpc-timeout-ms and the resync's \
             {resync_s} s deadline together, or the span bound could stop a session with its \
             arm or its resync in flight (the values are not shown)"
        ));
    }
    Bounds::new(max_age, max_entries, span, &ledger::stage_one())
}

/// An absolute path; refused, never shown, otherwise.
fn absolute(value: &str) -> Result<PathBuf, String> {
    let dir = PathBuf::from(value);
    match dir.is_absolute() {
        true => Ok(dir),
        false => Err("--evidence-dir: not an absolute path (the value is not shown)".to_owned()),
    }
}

/// `primary` or `subaccount`, not already in `arms`.
fn armed(value: &str, arms: &[Armed]) -> Result<Armed, String> {
    let armed = match value {
        "primary" => Armed::Primary,
        "subaccount" => Armed::Subaccount,
        _ => return Err("--arm: primary or subaccount (the value is not shown)".to_owned()),
    };
    match arms.contains(&armed) {
        true => Err(format!("--arm {value} given twice: each account arms once")),
        false => Ok(armed),
    }
}

/// `primary` or `subaccount`, the hold's account.
fn held(value: &str) -> Result<Armed, String> {
    match value {
        "primary" => Ok(Armed::Primary),
        "subaccount" => Ok(Armed::Subaccount),
        _ => Err("--hold-account: primary or subaccount (the value is not shown)".to_owned()),
    }
}

/// A whole number from `lo` to `hi`.
fn number(flag: &str, value: &str, lo: u64, hi: u64) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(n) if (lo..=hi).contains(&n) => Ok(n),
        _ => Err(format!(
            "{flag}: not a whole number from {lo} to {hi} (not shown)"
        )),
    }
}

/// Whole seconds from 1 to `hi`.
fn secs(flag: &str, value: &str, hi: u64) -> Result<Duration, String> {
    number(flag, value, 1, hi).map(Duration::from_secs)
}

/// Whole milliseconds from 1 to 120000.
fn millis(flag: &str, value: &str) -> Result<Duration, String> {
    number(flag, value, 1, 120_000).map(Duration::from_millis)
}

/// `host:port`, the port from 1 to 65535, the host one a resolver takes (a name, IPv4, or a
/// bracketed IPv6 address, whose brackets the connector does not take).
fn socks5(value: &str) -> Result<ProxyConfig, String> {
    let bad = || "--socks5: not host:port (the value is not shown)".to_owned();
    let (host, port) = value.rsplit_once(':').ok_or_else(bad)?;
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(bad)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let name = |b: u8| b.is_ascii_alphanumeric() || b".-:".contains(&b);
    match !host.is_empty() && host.len() <= 253 && host.bytes().all(name) {
        true => Ok(ProxyConfig::Socks5 {
            host: host.to_owned(),
            port,
        }),
        false => Err(bad()),
    }
}
