//! qfex_preprod_probe's command line, parsed by hand (no argument-parsing dependency). No value
//! typed is ever echoed: a key pasted into the wrong flag would be printed.

use std::path::PathBuf;
use std::time::Duration;

use fbc_runtime::{ProxyConfig, RateUserKey};
use fbc_venue_qfex::exec::AUTH_DEADLINE;
use fbc_venue_qfex::probe::{PREPROD_MDS, PREPROD_REST, PREPROD_TRADE};

use crate::auth_qfex::{Armed, PUBLIC_VAR, SECRET_VAR, SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR};
use crate::ledger::{self, Bounds, MAX_AGE_S, MAX_ENTRIES, MAX_SPAN_S};

/// The provider user a run's rate limiter counts for: one user, its accounts sharing QFEX's
/// buckets (decision 0123). The program's own numbering: a process probes one user.
pub const USER: RateUserKey = RateUserKey::new(1);

/// The help text `--help` prints.
pub fn usage() -> String {
    format!(
        "\
qfex_preprod_probe: stage one of the QFEX pre-production probe (decision 0132 item 7, FBC-l931):
on QFEX's PRE-PRODUCTION environment only, an owner-assisted run (decision 0067) that arms
cancel-on-disconnect once per account and records what answers the arm. It places nothing and
sends no command but each session's arm and the probe's own reads.

Before anything connects it refuses any origin but QFEX's pre-production ones (or, for a test
stub, all of them loopback addresses), an account decision 0122 refuses, missing or empty
credentials, a SOCKS5 proxy with loopback origins, and a missing or unaccepted --registry-span,
--ledger-max-age or --ledger-max-entries: fill-ledger bounds that cannot outlast a registry's
sessions and fills (decision 0132 item 5). Then it reads GET /user/subaccounts, and before each
session the primary account and every listed subaccount (GET /user/positions, and
get_user_orders on a trade socket that never arms): a session starts only when every account
holds no open order (none on the page, none counted by REST) and a flat position. Each session
arms once; at the arm's answer, its deadline, the connection's end, --session-wait or its
registry's span bound the program stops it, through its stop control, with no second
connection attempt. The sessions run one after the other, so no two arms are ever in flight
across the user.

Usage: cargo run -p fbc-examples --example qfex_preprod_probe -- [OPTIONS]

Options:
  --evidence-dir <DIR>     required: an absolute path, empty or not there, where the run writes
                           report.md, evidence.json, its journal (journal/) and the scrubbed
                           fixtures (fixtures/, to commit as fixtures/qfex/exec-recorded/arm/)
  --arm <ACCOUNT>          required, once or twice, in run order: primary, or subaccount (the
                           one {SUB_ID_VAR} names)
  --rpc-timeout-ms <MS>    the arm's deadline, its window of capture: 501 to 60000 (default
                           10000); it must exceed QFEX's 500 ms speed bump
  --page-size <N>          the get_user_orders page each read asks for, 1 to 1000 (default 1000)
  --read-wait-ms <MS>      how long each read waits for its answer, 1 to 120000 (default 10000)
  --connect-wait-ms <MS>   how long a connection may take to open, 1 to 120000 (default 10000)
  --session-wait <S>       the longest one session runs before the program stops it, up to 3600
                           (default 120); longer than --connect-wait-ms, QFEX's 60 s
                           authentication deadline and --rpc-timeout-ms together, so the
                           program never stops a session while its arm is in flight
  --registry-span <S>      required, no default: the longest from a registry's first session's
                           start to its last session's end, 1 to {MAX_SPAN_S}; a session that
                           reaches it is stopped. Longer than --connect-wait-ms, QFEX's 60 s
                           authentication deadline and --rpc-timeout-ms together, as
                           --session-wait is. Stage one gives each session its own registry
  --ledger-max-age <S>     required, no default: how long each registry's fill ledger keeps a
                           fill, 1 to {MAX_AGE_S}; longer than --registry-span (decision 0132
                           item 5)
  --ledger-max-entries <N> required, no default: the most fills it keeps, 1 to {MAX_ENTRIES};
                           above the fill ceiling, every order a registry's sessions may place
                           at its largest size in size steps (stage one places nothing: 0)
  --trade-url <URL>        default {PREPROD_TRADE}
  --rest-url <URL>         default {PREPROD_REST}
  --mds-url <URL>          default {PREPROD_MDS}
  --socks5 <HOST:PORT>     connect through this SOCKS5 proxy (default: directly)

Credentials, from the environment only:
  {PUBLIC_VAR}, {SECRET_VAR}   the provider user's primary credential (required)
  {SUB_ID_VAR}                   the subaccount to arm on (required with --arm subaccount)
  {SUB_PUBLIC_VAR}, {SUB_SECRET_VAR}
                                       a credential scoped to that subaccount (optional, both
                                       or neither); otherwise the primary credential reads it
"
    )
}

/// What to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// Where the evidence is written.
    pub evidence_dir: PathBuf,
    /// The accounts a session arms on, in run order, each at most once.
    pub arms: Vec<Armed>,
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
    /// 5), accepted against stage one's fill ceiling.
    pub ledger: Bounds,
    /// The provider user the run's rate limiter counts for, and whose shared budget a refusal
    /// holds (decision 0123): always [`USER`] from the command line. A test that runs several
    /// probes at once in one process gives each its own, since a user's budget is process-wide.
    pub rate_user: RateUserKey,
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
    let (mut trade_url, mut rest_url, mut mds_url) = (
        PREPROD_TRADE.to_owned(),
        PREPROD_REST.to_owned(),
        PREPROD_MDS.to_owned(),
    );
    let mut proxy = ProxyConfig::Direct;
    let (mut rpc_timeout_ms, mut page_size) = (10_000, 1000);
    let mut read_wait = Duration::from_millis(10_000);
    let mut connect_wait = Duration::from_millis(10_000);
    let mut session_wait = Duration::from_secs(120);
    // The fill ledger's bounds and the span bound: required, with no defaults.
    let (mut span, mut max_age, mut max_entries) = (None, None, None);
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
            "--evidence-dir" => evidence_dir = Some(absolute(&value()?)?),
            "--arm" => arms.push(armed(&value()?, &arms)?),
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
            "--trade-url" => trade_url = value()?,
            "--rest-url" => rest_url = value()?,
            "--mds-url" => mds_url = value()?,
            "--socks5" => proxy = socks5(&value()?)?,
            _ => return Err("an unknown argument (not shown); --help lists the flags".to_owned()),
        }
    }
    let evidence_dir = evidence_dir.ok_or("--evidence-dir is required")?;
    if arms.is_empty() {
        return Err("--arm is required: primary, subaccount, or both".to_owned());
    }
    // A session opens, authenticates within QFEX's deadline, then arms and waits out the arm's:
    // a --session-wait no longer than all three could stop it with its arm in flight, spending
    // that account's capture with no outcome. The span bound stops a session too, so it must
    // outlast the same three.
    let to_arm = connect_wait + AUTH_DEADLINE + Duration::from_millis(rpc_timeout_ms);
    if session_wait <= to_arm {
        return Err(format!(
            "--session-wait must be longer than --connect-wait-ms, QFEX's {} s authentication \
             deadline and --rpc-timeout-ms together, or a session could be stopped with its arm \
             in flight (the values are not shown)",
            AUTH_DEADLINE.as_secs()
        ));
    }
    let span = span.ok_or("--registry-span is required: it has no default")?;
    let max_age = max_age.ok_or("--ledger-max-age is required: it has no default")?;
    let max_entries = max_entries.ok_or("--ledger-max-entries is required: it has no default")?;
    if span <= to_arm {
        return Err(format!(
            "--registry-span must be longer than --connect-wait-ms, QFEX's {} s authentication \
             deadline and --rpc-timeout-ms together, or the span bound could stop a session \
             with its arm in flight (the values are not shown)",
            AUTH_DEADLINE.as_secs()
        ));
    }
    let ledger = Bounds::new(max_age, max_entries, span, &ledger::stage_one())?;
    Ok(Parsed::Run(Box::new(Options {
        evidence_dir,
        arms,
        trade_url,
        rest_url,
        mds_url,
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
