//! qfex_preprod_probe's command line, parsed by hand (no argument-parsing dependency). No value
//! typed is ever echoed: a key pasted into the wrong flag would be printed.
//!
//! `--place-stages` (FBC-c6kw, the first slice of FBC-9sk2) parses into [`Places`] beside the
//! [`Options`] every run takes ([`Parsed::Places`]): the place stages' flags, each refused before
//! anything connects, naming the flag and never its value (decisions 0113 and 0148 item 7, and
//! 0151 items 1 and 4).

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use fbc_core::Side;
use fbc_runtime::{ProxyConfig, RateUserKey};
use fbc_venue_qfex::exec::AUTH_DEADLINE;
use fbc_venue_qfex::probe::{
    Environment, PREPROD_MDS, PREPROD_PROBE, PREPROD_REST, PREPROD_TRADE, PRODUCTION_MDS,
    PRODUCTION_PROBE, PRODUCTION_REST, PRODUCTION_TRADE,
};
use rust_decimal::Decimal;

use crate::auth_qfex::{Armed, PUBLIC_VAR, SECRET_VAR, SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR};
use crate::ledger::{self, Bounds, MAX_AGE_S, MAX_ENTRIES, MAX_FLATTEN_ATTEMPTS, MAX_SPAN_S};
use crate::testnet;

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
declaration (decision 0143). A stage-one run places nothing and sends no command but each
session's arm, its resync's reads (one GET /user/positions and get_user_orders pages, decision
0132 item 5) and the probe's own reads. With --hold-secs it first holds one trade connection
that never arms (the liveness hold, decision 0146, FBC-39ks), and a run may hold without arming
at all.

With --place-stages (FBC-c6kw and FBC-23ch, slices of FBC-9sk2; decisions 0148 and 0151) it
runs the place stages' preflight, then drop session one of decision 0132 item 4's drop test: on
production alone (--production; 0151 item 1), and, until FBC-4plj lands every stop decisions
0132 item 7 and 0148 item 8 require, only against a loopback test stub, every origin (trade,
REST and MDS) a loopback address. It prints a CAPS line with the environment and both caps
first, before any request; runs the hold first, alone, when --hold-secs is given; reads the
instrument --symbol names from this run's GET /refdata on the guarded REST origin, refusing one
that is not listed, not trading, or that does not list both a post-only GTC and a limit IOC
(decision 0148 item 4); holds --ledger-max-entries above each place registry's fill ceiling
(0151 item 3); opens the MDS touch (bbo) on the guarded MDS origin and refuses a touch older
than --touch-max-age-ms, a crossed or locked one, or one from before the MDS connection's
current epoch; and refuses either cap below three of the instrument's minimum size at the
touch.

Drop session one runs on the first named account: every named account read clean, the drop
registry built (CodProbeRun::owner_supervised_places on its one market, its CancelOnlyTrip given
to the journal before any market arms, decision 0139) and seeded flat (decision 0148 item 3);
one session that arms cancel-on-disconnect, its arm accepted and its resync applied; Start under
the market lease; X, one post-only GTC at the instrument's minimum size --away-bps behind the
touch, its client id minted under the run's namespace lease, as an fbc-oms authorization (until
an ACK echoes our client id, a session places at most one post-only GTC far from the touch,
decision 0132 item 7); X's ACK or its deadline; the streams compared with never-arming reads at
--read-interval-ms, at most 10 within --agree-wait-ms, every frame charged to the shared limiter
first; a graceful close; then the read of every named account and of X and its trades. The run
then ends, its outcome not ok, until FBC-77r8 builds drop session two: X still resting is
reported for your hand cancel on QFEX's site and the stop marker written (LeftResting).

Each stop decisions 0132 item 7 and 0148 item 8 name stops the run by name, with the read of
every named account after it, the report and the stop marker: an order event or fill of ours
without our client id, an order_response naming no order of ours (0151 item 13), a replayed
fill refused for its time, a resync that disagrees or leaves a fill unsettled, a provider rate
refusal on the session, a read socket or REST, a read showing an order or a position the streams
did not, an unplanned end of the connection, the span bound, a fill of X, a position not flat,
an order left resting, and Ctrl-C. In a drop session only Ctrl-C and a fill of ours cancel (0151
item 2): Kill, every order of ours cancelled by venue id, a wait of up to --rpc-timeout-ms for
each to end (a second Ctrl-C meanwhile recorded), the market disarmed, then a graceful close;
cancels held by a provider rate refusal or refused locally end it by a graceful close too (0151
item 11). Every other stop sends no cancel and ends the session by a graceful close.

Before anything connects, a place-stage run opens its lease directory (0151 items 1 and 6): it
tags the directory production on its first use and refuses one tagged for another environment;
it refuses to start while a stop marker an earlier place-stage run wrote is in it or in
production's default lease directory, unless --reconciled is given, and refuses --reconciled
when there is none; and it takes the client-id namespace's lease there for the run, refused
while another run holds it, beside the high-water mark its ids are minted above. With
--reconciled it reads every named account after the preflight and clears the marker only if
every read is clean; the declaration is the owner's, which the probe does not check. A run that
stops (any stop but NoEcho, each named in its report) writes a marker with the stop's name and
its start time. In a place-stage run Ctrl-C outside the hold's connection never ends the
program: a press from the hold's first read to its last, its connecting included, is the hold's
(below), and a press during the preflight, the reads, drop session one or its stop's cancels
stops the run, Interrupted, and its report and its marker are written.

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
Ctrl-C at any other time ends the program at once, writing no report, as it always has, except
in a place-stage run. There Ctrl-C from the hold's first read to its last stops the run before its
preflight, with its report and no stop marker: one before the connection opens keeps it from
opening, and one during the reads after it lets them end first. Ctrl-C at any other time stops
it with its report (above). A run that holds needs no
--arm, and one with neither --arm nor --place-stages takes no --registry-span, --ledger-max-age
or --ledger-max-entries.

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
                           --production, beside the owner's README (decision 0146 item 6); a
                           place-stage run writes its scrubbed slice into drop-production/, to
                           commit as fixtures/qfex/exec-recorded/drop-production/ once every
                           file is read (0151 item 7)
  --arm <ACCOUNT>          once or twice, in run order: primary, or subaccount (the one
                           {SUB_ID_VAR} names); required unless --hold-secs is given, or
                           --place-stages, which refuses it
  --hold-secs <S>          hold one trade connection that never arms for S whole seconds from
                           its opening, 1 to {MAX_HOLD_S} (a day), before any session (decision
                           0146); no default: without it there is no hold, and a run with
                           none of it, --arm and --place-stages is refused
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
  --registry-span <S>      required with --arm, no default, refused without it or
                           --place-stages: the longest from a registry's first session's start
                           to its last session's end, 1 to {MAX_SPAN_S}; a session that
                           reaches it is stopped. Longer than --connect-wait-ms, QFEX's 60 s
                           authentication deadline and the longer of --rpc-timeout-ms and the
                           resync's {resync} s deadline together, as --session-wait is. Stage
                           one gives each session its own registry
  --ledger-max-age <S>     required with --arm, no default, refused without it or
                           --place-stages: how long each registry's fill ledger keeps a fill,
                           1 to {MAX_AGE_S}; longer than --registry-span (decision 0132 item 5)
  --ledger-max-entries <N> required with --arm, no default, refused without it or
                           --place-stages: the most fills it keeps, 1 to {MAX_ENTRIES}; above
                           the fill ceiling, every order a registry's sessions may place at its
                           largest size in size steps (stage one places nothing: 0)
                           With --place-stages the three are required too, --registry-span is
                           at most 3600 (decision 0148 item 7) and longer than the drop test's
                           two sessions, each --connect-wait-ms, QFEX's 60 s authentication
                           deadline, the longer of --rpc-timeout-ms and the resync's {resync} s
                           deadline and --agree-wait-ms, and the reads of every named account
                           between them, each --read-wait-ms (0151 item 3)
  --place-stages           the place stages' preflight and drop session one (above): refused
                           with --arm, and without --production; needs every flag below, each
                           of which it alone takes
  --symbol <SYMBOL>        required: the venue symbol traded, at most 32 ASCII letters, digits
                           and punctuation; never written to the report, the evidence or a
                           fixture
  --side <SIDE>            required, buy or sell, no default: the side of every place but a
                           flatten's, which takes the other
  --sole-trader            required: no other process, Java or Rust, trades any account of the
                           user (decision 0148 item 7)
  --resting-cap-usd <USD>  required, positive, no default: the resting cap (decision 0113)
  --inventory-cap-usd <USD>
                           required, positive, no default: the inventory cap; both caps are
                           printed on the CAPS line before any request, and neither is written
                           to the report or the evidence
  --away-bps <BPS>         required: how far behind the touch a resting order rests, 20 to 10000
  --flatten-cap-bps <BPS>  required: how far through the touch it crosses a flatten IOC may be
                           priced, at least 1 and below --away-bps
  --flatten-attempts <N>   required: the flatten IOCs an account may place in a run, 1 to 3
  --namespace <N>          required: the client-id namespace, 1 to 65535
  --lease-dir <DIR>        the place stages' lease directory, an absolute path (default
                           $HOME/.fueledbychai/{LEASE_DIR_NAME}); never the Paradex testnet
                           samples' default; tagged production on its first use, and refused
                           tagged otherwise (0151 item 1)
  --reconciled <WHAT>      hand-cancel, hand-close, both or none: what the owner did by hand
                           after an earlier place-stage run's stop, which the probe does not
                           check; required while that run's stop marker exists, and refused
                           otherwise (0151 item 6)
  --agree-wait-ms <MS>     required, 1 to 60000: how long a read may lag the streams, after
                           each place, cancel and fill of drop session one
  --read-interval-ms <MS>  required, 1000 to 60000: the time between two reads of a comparison;
                           one read, then one each --read-interval-ms within --agree-wait-ms,
                           at most {MAX_READS} in all (0151 item 4)
  --touch-max-age-ms <MS>  required, 1 to 60000: a touch older than this is not priced from,
                           in the preflight and when X is priced
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

/// The place stages' lease directory's name under `$HOME/.fueledbychai`: production's own, apart
/// from the Paradex testnet samples' (decision 0148 item 7). No pre-production place path is
/// built (0151 item 1), so there is no pre-production one.
pub const LEASE_DIR_NAME: &str = "qfex_probe_production";

/// The most reads one comparison of the streams against the reads makes: one, then one each
/// `--read-interval-ms` within `--agree-wait-ms` (0151 item 4).
pub const MAX_READS: u64 = 10;

/// The longest `--registry-span` a place-stage run takes: an hour (decision 0148 item 7).
pub const MAX_PLACE_SPAN_S: u64 = 3600;

/// The longest venue symbol `--symbol` takes.
pub const MAX_SYMBOL: usize = 32;

/// What a place-stage run asks for (`--place-stages`, FBC-c6kw), beside the [`Options`] every
/// run takes: each value the owner's, with no default but the lease directory's, and refused
/// before anything connects (decisions 0113 and 0148 item 7). `--sole-trader` is required, so a
/// parsed run always gave it; it is not kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Places {
    /// `--symbol`: the venue symbol traded, shape-checked only, and never written to a fixture,
    /// the report or the evidence.
    pub symbol: String,
    /// `--side`: the side of every place but a flatten's, which takes the other.
    pub side: Side,
    /// `--resting-cap-usd` and `--inventory-cap-usd`: positive, printed on the CAPS line alone.
    pub resting_cap_usd: Decimal,
    pub inventory_cap_usd: Decimal,
    /// `--away-bps`: how far behind the touch a resting order rests, 20 to 10000.
    pub away_bps: u32,
    /// `--flatten-cap-bps`: how far through the touch it crosses a flatten IOC may be priced, at
    /// least 1 and below `away_bps`.
    pub flatten_cap_bps: u32,
    /// `--flatten-attempts`: the flatten IOCs an account may place in a run, 1 to 3.
    pub flatten_attempts: u64,
    /// `--namespace`: the client-id namespace, which the run mints under its lease in
    /// `lease_dir` (`places::Mint`).
    pub namespace: u16,
    /// `--lease-dir`, or its default under `$HOME` ([`LEASE_DIR_NAME`]): the run's lease
    /// directory, where it holds its leases and writes its stop marker (`places`, 0151 items 1
    /// and 6).
    pub lease_dir: PathBuf,
    /// Production's default lease directory under `$HOME`, when `$HOME` gives one: a stop marker
    /// there holds a run as one in `lease_dir` does (0151 item 6).
    pub default_lease_dir: Option<PathBuf>,
    /// `--reconciled`: what the owner declares was done by hand after an earlier run's stop
    /// (0151 item 6), which the probe does not check.
    pub reconciled: Option<Reconciled>,
    /// `--agree-wait-ms`: how long a read may lag the streams (FBC-23ch's comparisons), which
    /// the span bound's floor counts.
    pub agree_wait: Duration,
    /// `--read-interval-ms`: the time between two reads of one comparison (FBC-23ch).
    pub read_interval: Duration,
    /// `--touch-max-age-ms`: a touch older than this is not priced from.
    pub touch_max_age: Duration,
    /// What a test asks of the run beyond its flags ([`TestHooks`]): never in the program.
    #[cfg(test)]
    pub test: TestHooks,
}

/// What a place-stage test asks of the run beyond its command line, to reach a path no flag and
/// no answer of the QFEX model reaches. Compiled into the test binary alone, so the program has
/// none, and every parsed run has each off.
#[cfg(test)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestHooks {
    /// X spent from the drop registry's list before drop session one runs, so its place is
    /// beyond the list (`places::Stop::BeyondList`).
    pub x_spent: bool,
    /// The run's journal answering every Normal record dropped (`tee::Tee::refusing_normal`), so
    /// its soft limit is reached on the session's first Normal record (decision 0139).
    pub refuse_normal: bool,
    /// X not priced once the market armed, so drop session one fails with no stop
    /// (`stage::Halt::Error`), its reads after it made as after any session.
    pub x_unpriced: bool,
}

/// What `--reconciled` declares the owner did by hand after an earlier place-stage run's stop
/// (0151 item 6): 0132 item 7's hand cancel, 0148 item 9's hand close, both, or neither.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Reconciled {
    HandCancel,
    HandClose,
    Both,
    None,
}

impl Reconciled {
    /// As `--reconciled` takes it and the report gives it.
    pub fn name(self) -> &'static str {
        match self {
            Reconciled::HandCancel => "hand-cancel",
            Reconciled::HandClose => "hand-close",
            Reconciled::Both => "both",
            Reconciled::None => "none",
        }
    }
}

/// `hand-cancel`, `hand-close`, `both` or `none`.
fn reconciled(value: &str) -> Result<Reconciled, String> {
    let all = [
        Reconciled::HandCancel,
        Reconciled::HandClose,
        Reconciled::Both,
        Reconciled::None,
    ];
    let found = all.into_iter().find(|r| r.name() == value);
    found.ok_or_else(|| {
        "--reconciled: hand-cancel, hand-close, both or none (the value is not shown)".to_owned()
    })
}

/// What the command line asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    Run(Box<Options>),
    /// A place-stage run (`--place-stages`): its options, and what the place stages ask for.
    Places(Box<Options>, Box<Places>),
}

/// The place stages' flags as typed, each checked as it is read, before they are put together
/// ([`places`]).
#[derive(Default)]
struct PlaceArgs {
    /// `--place-stages`.
    asked: bool,
    /// The place-stage flags typed so far, in order: the first for the refusal of one given
    /// without `--place-stages`, each for the refusal of one given twice.
    seen: Vec<&'static str>,
    symbol: Option<String>,
    side: Option<Side>,
    sole_trader: bool,
    resting_cap_usd: Option<Decimal>,
    inventory_cap_usd: Option<Decimal>,
    away_bps: Option<u32>,
    flatten_cap_bps: Option<u32>,
    flatten_attempts: Option<u64>,
    namespace: Option<u16>,
    lease_dir: Option<PathBuf>,
    reconciled: Option<Reconciled>,
    agree_wait: Option<Duration>,
    read_interval: Option<Duration>,
    touch_max_age: Option<Duration>,
}

impl PlaceArgs {
    /// Reads `flag`, a place stage's, taking its value from `value` when it has one: `Ok(false)`
    /// when `flag` is none of them. A flag given twice is refused, naming it and neither value,
    /// rather than the last one kept: each sets the direction or the size of real orders.
    fn read(
        &mut self,
        flag: &str,
        value: &mut dyn FnMut() -> Result<String, String>,
    ) -> Result<bool, String> {
        // Only a place-stage flag is ever seen, so a flag seen before is one given twice.
        if self.seen.contains(&flag) {
            return Err(format!(
                "{flag} is given twice: each place-stage flag is given once (the values are not \
                 shown)"
            ));
        }
        let name: &'static str = match flag {
            "--symbol" => {
                self.symbol = Some(symbol(&value()?)?);
                "--symbol"
            }
            "--side" => {
                self.side = Some(side(&value()?)?);
                "--side"
            }
            "--sole-trader" => {
                self.sole_trader = true;
                "--sole-trader"
            }
            "--resting-cap-usd" => {
                self.resting_cap_usd = Some(testnet::positive(flag, &value()?)?);
                "--resting-cap-usd"
            }
            "--inventory-cap-usd" => {
                self.inventory_cap_usd = Some(testnet::positive(flag, &value()?)?);
                "--inventory-cap-usd"
            }
            "--away-bps" => {
                self.away_bps = Some(number(flag, &value()?, 20, 10_000)? as u32);
                "--away-bps"
            }
            "--flatten-cap-bps" => {
                self.flatten_cap_bps = Some(number(flag, &value()?, 1, 9_999)? as u32);
                "--flatten-cap-bps"
            }
            "--flatten-attempts" => {
                let attempts = number(flag, &value()?, 1, MAX_FLATTEN_ATTEMPTS)?;
                self.flatten_attempts = Some(attempts);
                "--flatten-attempts"
            }
            "--namespace" => {
                self.namespace = Some(number(flag, &value()?, 1, 65_535)? as u16);
                "--namespace"
            }
            "--lease-dir" => {
                self.lease_dir = Some(testnet::absolute(&value()?)?);
                "--lease-dir"
            }
            "--reconciled" => {
                self.reconciled = Some(reconciled(&value()?)?);
                "--reconciled"
            }
            "--agree-wait-ms" => {
                let ms = number(flag, &value()?, 1, 60_000)?;
                self.agree_wait = Some(Duration::from_millis(ms));
                "--agree-wait-ms"
            }
            "--read-interval-ms" => {
                let ms = number(flag, &value()?, 1_000, 60_000)?;
                self.read_interval = Some(Duration::from_millis(ms));
                "--read-interval-ms"
            }
            "--touch-max-age-ms" => {
                let ms = number(flag, &value()?, 1, 60_000)?;
                self.touch_max_age = Some(Duration::from_millis(ms));
                "--touch-max-age-ms"
            }
            _ => return Ok(false),
        };
        self.seen.push(name);
        Ok(true)
    }
}

/// The place stages' flags `args` put together, every one but `--lease-dir` and `--reconciled`
/// required, for a run
/// on production (`production`) that arms nothing (`arms`); `home` is `$HOME`, for the lease
/// directory's default and the testnet samples' default it must not be. Refused naming the flag,
/// never its value (decisions 0113 and 0148 item 7, 0151 items 1 and 4).
fn places(
    args: PlaceArgs,
    production: bool,
    arms: &[Armed],
    home: Option<OsString>,
) -> Result<Places, String> {
    if !arms.is_empty() {
        return Err(
            "--place-stages and --arm: the place stages arm their own sessions on each \
                    named account in turn; give one or the other"
                .to_owned(),
        );
    }
    if !production {
        return Err(
            "--place-stages runs on QFEX production alone: give --production. No \
                    pre-production place path is built (0151 item 1), since decision 0144 item \
                    6 leaves which accounts the place stages read on pre-production undecided"
                .to_owned(),
        );
    }
    let need = |flag: &str| format!("{flag} is required with --place-stages: it has no default");
    if !args.sole_trader {
        return Err(format!(
            "{}: no other process, Java or Rust, may trade any account of the user while the place \
             stages run (decision 0148 item 7)",
            need("--sole-trader")
        ));
    }
    let away_bps = args.away_bps.ok_or_else(|| need("--away-bps"))?;
    let flatten_cap_bps = args
        .flatten_cap_bps
        .ok_or_else(|| need("--flatten-cap-bps"))?;
    if flatten_cap_bps >= away_bps {
        return Err(
            "--flatten-cap-bps must be below --away-bps (decision 0148 item 7; the values \
                    are not shown)"
                .to_owned(),
        );
    }
    let agree_wait = args.agree_wait.ok_or_else(|| need("--agree-wait-ms"))?;
    let read_interval = args
        .read_interval
        .ok_or_else(|| need("--read-interval-ms"))?;
    if reads(agree_wait, read_interval) > MAX_READS {
        return Err(format!(
            "--agree-wait-ms with --read-interval-ms allows more than {MAX_READS} reads per \
             comparison: one read, then one each --read-interval-ms within --agree-wait-ms \
             (0151 item 4; the values are not shown)"
        ));
    }
    let default_lease_dir = testnet::lease_dir_named(home.clone(), LEASE_DIR_NAME).ok();
    let lease_dir = match args.lease_dir {
        Some(dir) if dir.components().any(|c| c == Component::ParentDir) => {
            return Err(
                "--lease-dir: a path with a `..` component, which could name the Paradex \
                 testnet samples' default lease directory: give it without one (the path is not \
                 shown)"
                    .to_owned(),
            );
        }
        Some(dir) => dir,
        None => testnet::lease_dir_named(home.clone(), LEASE_DIR_NAME)?,
    };
    if testnet::lease_dir_under(home).is_ok_and(|testnet| same_dir(&testnet, &lease_dir)) {
        return Err(
            "--lease-dir: the Paradex testnet samples' default lease directory: the place \
                    stages keep their leases apart from it (decision 0148 item 7; the path is not \
                    shown)"
                .to_owned(),
        );
    }
    Ok(Places {
        symbol: args.symbol.ok_or_else(|| need("--symbol"))?,
        side: args.side.ok_or_else(|| need("--side"))?,
        resting_cap_usd: args
            .resting_cap_usd
            .ok_or_else(|| need("--resting-cap-usd"))?,
        inventory_cap_usd: args
            .inventory_cap_usd
            .ok_or_else(|| need("--inventory-cap-usd"))?,
        away_bps,
        flatten_cap_bps,
        flatten_attempts: args
            .flatten_attempts
            .ok_or_else(|| need("--flatten-attempts"))?,
        namespace: args.namespace.ok_or_else(|| need("--namespace"))?,
        lease_dir,
        default_lease_dir,
        reconciled: args.reconciled,
        agree_wait,
        read_interval,
        touch_max_age: args
            .touch_max_age
            .ok_or_else(|| need("--touch-max-age-ms"))?,
        #[cfg(test)]
        test: TestHooks::default(),
    })
}

/// Whether `a` and `b` are one directory: equal as written (a trailing slash or a `.` aside), or,
/// where both exist, once each symbolic link is resolved.
fn same_dir(a: &Path, b: &Path) -> bool {
    let resolved = |p: &Path| std::fs::canonicalize(p).ok();
    a == b || resolved(a).is_some_and(|a| resolved(b) == Some(a))
}

/// The reads one comparison makes within `agree_wait` at `read_interval`: one, then one each
/// interval, none beginning at or past the wait's end (0151 item 4).
pub fn reads(agree_wait: Duration, read_interval: Duration) -> u64 {
    let each = read_interval.as_millis().max(1);
    let within = agree_wait.as_millis().div_ceil(each).max(1);
    u64::try_from(within).unwrap_or(u64::MAX)
}

/// The floor a place-stage run's `--registry-span` must be above (0151 item 3): the drop
/// test's two sessions, each opening within `--connect-wait-ms`, authenticating within QFEX's
/// deadline, waiting out the longer of `--rpc-timeout-ms` and the resync's deadline, and its
/// reads lagging the streams by up to `--agree-wait-ms`, and between them the reads of each of
/// the `accounts` named accounts, each within `--read-wait-ms`.
pub fn place_span_floor(opts: &Options, places: &Places, accounts: usize) -> Duration {
    let settled = Duration::from_millis(opts.rpc_timeout_ms).max(RESYNC_TIMEOUT);
    let session = opts.connect_wait + AUTH_DEADLINE + settled + places.agree_wait;
    let reads = opts.read_wait * u32::try_from(accounts).unwrap_or(u32::MAX);
    session * 2 + reads
}

/// Refuses a place-stage run whose span bound is not above [`place_span_floor`] for its
/// `accounts` named accounts, naming `--registry-span` and never a value: the span bound could
/// otherwise stop the drop test's second session before its reads end (0151 item 3).
pub fn check_place_span(opts: &Options, places: &Places, accounts: usize) -> Result<(), String> {
    let span = opts.ledger.as_ref().map(Bounds::span);
    match span.is_some_and(|span| span > place_span_floor(opts, places, accounts)) {
        true => Ok(()),
        false => Err(format!(
            "--registry-span must be longer than the drop test's two sessions, each \
             --connect-wait-ms, QFEX's {} s authentication deadline, the longer of \
             --rpc-timeout-ms and the resync's {} s deadline and --agree-wait-ms, and the reads of \
             every named account between them, each --read-wait-ms, or the span bound could stop \
             its second session before its reads end (0151 item 3; the values are not shown)",
            AUTH_DEADLINE.as_secs(),
            RESYNC_TIMEOUT.as_secs()
        )),
    }
}

/// A venue symbol as `--symbol` takes it: non-empty, ASCII graphic, at most [`MAX_SYMBOL`]
/// bytes. Its shape only: discovery says whether QFEX lists it. Refused, never shown.
fn symbol(value: &str) -> Result<String, String> {
    let shaped = !value.is_empty()
        && value.len() <= MAX_SYMBOL
        && value.bytes().all(|b| b.is_ascii_graphic());
    match shaped {
        true => Ok(value.to_owned()),
        false => Err(format!(
            "--symbol: not a venue symbol of at most {MAX_SYMBOL} ASCII letters, digits and \
             punctuation (the value is not shown)"
        )),
    }
}

/// `buy` or `sell`.
fn side(value: &str) -> Result<Side, String> {
    match value {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err("--side: buy or sell (the value is not shown)".to_owned()),
    }
}

/// Parses the arguments after the program name, `$HOME` read for a place-stage run's lease
/// directory ([`places`]).
pub fn parse(argv: impl IntoIterator<Item = String>) -> Result<Parsed, String> {
    parse_with_home(argv, std::env::var_os("HOME"))
}

/// Parses the arguments after the program name, `home` standing for `$HOME`.
pub fn parse_with_home(
    argv: impl IntoIterator<Item = String>,
    home: Option<OsString>,
) -> Result<Parsed, String> {
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
    // The place stages' flags (FBC-c6kw): none unless --place-stages asks for them.
    let mut place = PlaceArgs::default();
    let mut argv = argv.into_iter();
    while let Some(arg) = argv.next() {
        let mut value = || {
            let value = argv
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"));
            value.ok_or_else(|| format!("{arg} needs a value"))
        };
        if place.read(&arg, &mut value)? {
            continue;
        }
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--production" => production = true,
            "--place-stages" => place.asked = true,
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
    if let (false, Some(flag)) = (place.asked, place.seen.first()) {
        return Err(format!(
            "{flag} is a place stage's flag: give --place-stages too"
        ));
    }
    if arms.is_empty() && hold.is_none() && !place.asked {
        return Err(
            "--arm or --hold-secs is required, or --place-stages: arm on primary, subaccount or \
             both, hold a connection, run the place stages' preflight, or hold first"
                .to_owned(),
        );
    }
    let places = match place.asked {
        true => Some(places(place, production, &arms, home)?),
        false => None,
    };
    if places.is_some() && span.is_some_and(|span| span > Duration::from_secs(MAX_PLACE_SPAN_S)) {
        return Err(format!(
            "--registry-span: at most {MAX_PLACE_SPAN_S} s with --place-stages (decision 0148 \
             item 7; the value is not shown)"
        ));
    }
    let ledger = match arms.is_empty() && places.is_none() {
        true if span.is_some() || max_age.is_some() || max_entries.is_some() => {
            return Err(
                "--registry-span, --ledger-max-age and --ledger-max-entries bound the \
                        registries --arm and --place-stages build: a run with no --arm takes none \
                        of them, unless it runs --place-stages"
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
    let opts = Box::new(Options {
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
    });
    Ok(match places {
        Some(places) => Parsed::Places(opts, Box::new(places)),
        None => Parsed::Run(opts),
    })
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
