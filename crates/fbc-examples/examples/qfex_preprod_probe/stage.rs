//! Stage one of the QFEX probe (decision 0132 items 1, 3, 6 and 7; FBC-l931), on pre-production,
//! or with `--production` on production (decision 0143): one session per account, each arming
//! cancel-on-disconnect once, so the journal captures whatever answers the arm. It places
//! nothing and sends no command but each session's arm, its resync's reads and the probe's own
//! reads. With `--hold-secs` it first holds one trade connection that never arms (decision 0146),
//! and a run may hold without arming at all.
//!
//! - **Before anything connects** ([`run`]): every credential the run reads is set
//!   (`src/auth_qfex.rs`), and the configuration of each account it reads, under the adapter's
//!   probe mode for the run's environment (`preprod_probe`, or `production_probe` with
//!   `--production`), passes that mode's guard and decision 0122 item 1's account check (the
//!   adapter's `probe::connections`): any QFEX origin but the environment's own, a host of the
//!   other environment's domain (`qfex.com` on pre-production, `qfex.io` on production), a mix
//!   of loopback and other origins, or an account id that is not `primary` or a lowercase
//!   subaccount UUID is refused, naming the setting and never its value, and so is a
//!   `QFEX_SUBACCOUNT_ID` that selects the primary account (decision 0144 item 1). Loopback
//!   origins with a SOCKS5 proxy are refused too: the proxy would reach its own host's loopback.
//! - **The accounts** ([`named`], [`Run::check_accounts`]): the ones the run's configuration
//!   names, the primary account and the subaccount `QFEX_SUBACCOUNT_ID` names when it is set
//!   (decision 0144). The probe sends no `GET /user/subaccounts`: the owner must confirm in
//!   QFEX's app before the run that the user has no other subaccount, which the probe neither
//!   checks nor records, and the report says so. Before each session, for each of them,
//!   `GET /user/positions` and one `get_user_orders` page on a trade socket of its own that
//!   sends only its authentication and that read, and never arms. Every request takes its
//!   origin from the guarded configuration, through the adapter's `account_reads`, and is
//!   charged to the run's one provider-user rate limiter first (refused, not waited for, when it
//!   has no room).
//!   A `RateLimited` err answering its authentication or its page holds that limiter's shared
//!   budget, as the adapter asks of its caller (decision 0126), and stops the probe as any
//!   refused read does.
//!   An account that cannot be read, an open order, a TWAP or a position that is not flat stops
//!   the probe before the session, and so does a REST `open_orders` count above zero
//!   ([`AccountRead::refusal`]). Every read, write and close on a read socket waits at most
//!   `--read-wait-ms`. The account a session arms on is one of them by construction: the
//!   primary account, or the subaccount `QFEX_SUBACCOUNT_ID` names.
//! - **The liveness hold** ([`Run::hold`], decision 0146, FBC-39ks): before the sessions, while no
//!   other connection of the probe is open, every named account is read as before a session,
//!   then one trade connection opens on the account `--hold-account` names (the primary unless
//!   it names the subaccount), through the same adapter functions and shared limiter as the read
//!   sockets. It sends its authentication and, once that succeeded, the three subscriptions a
//!   session sends (`account_reads::subscriptions`), whose confirmations it reads, all within
//!   QFEX's 60 s authentication deadline; then nothing but the pongs and the close reply the
//!   WebSocket layer writes to each server ping and server close, each recorded on the shared
//!   limiter as a session records one (`RateLimiter::record`, decision 0030). It never arms,
//!   places, cancels or reads, and sends no ping of its own. It ends at `--hold-secs` from its
//!   opening or at the owner's interrupt (Ctrl-C, [`Interrupt`]), each a close charged as a
//!   control frame (none, and the connection dropped, when the limiter has no room, as a
//!   session's) and then the server's echo read for at most `--read-wait-ms`; at a server close
//!   (read on until the close reply has gone out); or at the connection's end; nothing
//!   reconnects. Every ping, pong and close it receives is recorded with its time since the
//!   connection opened ([`crate::hold`]); every data frame is counted by kind and read for a
//!   provider rate refusal, never kept, and a `RateLimited` err among them holds the shared
//!   budget as on a read socket (an err a journal would keep whole is read in memory as it
//!   arrived, for its code alone, and holds none). A refusal (authentication refused, a
//!   confirmation missing at the deadline, an err) ends it with a close as its duration does,
//!   and an err read while it closes, whatever ended it, is its refusal too, unless one already
//!   refused it. Once its connection is over, whatever ended it, every named account is read
//!   again, each whatever another showed; an account that then shows an open order or a
//!   position, a hold that did not do what decision 0146 asks, an order or fill frame on it, or
//!   the owner's interrupt in a run that also arms stops the probe, so no session starts. The
//!   interrupt is the hold's from its connection's opening, when the hold polls it before
//!   writing anything, until its close and the read of the echo are over: one that comes as it
//!   closes is recorded as such and stops a run that arms too. An interrupt at any other time
//!   is not the hold's: the program (`main.rs`, [`crate::interrupt`]) then ends at once,
//!   writing no report, as the probe always has.
//! - **The session** ([`Run::session`]): fbc-runtime's `ExecSession`, unchanged, with the
//!   consumer's `AccountWideCodAuthority` for the account it arms on. The program submits
//!   nothing: the session sends its own arm once the codec reports the stream authenticated
//!   (decision 0058), and asks for its resync, whose reads the probe codec sends (decision 0132
//!   item 5: one signed `GET /user/positions` on the REST origin and `get_user_orders` pages on
//!   the session's socket; no `get_user_trades`, since the session places nothing and decodes no
//!   fill before it), so nothing places. The codec answers the arm from its echo (an ack accepts
//!   it, an err rejects it, decision 0132 item 1; FBC-jxau). The handler stops the session
//!   through its control once the arm has its outcome (its answer, or `Unknown` at its deadline)
//!   and the resync has ended, so the journal holds each read with its answer, or at the
//!   connection's end (the codec ends the epoch on a read that fails or outlasts
//!   `qfex.exec.resync_timeout`), whichever comes first, and the program at `--session-wait` or at
//!   its registry's span bound (`--registry-span`), through the same control; the reconnect pacing
//!   allows one attempt a day, so no second attempt starts meanwhile. The journal keeps every frame
//!   as the codec names its spans, so the arm's window is kept as decision 0132 item 3 says. The
//!   sessions run one after the other: no two arms are in flight across the user.
//! - **The registries** ([`crate::ledger`]): each session's own, since each arms its own account
//!   (decision 0082), built as the session starts: as decision 0132 item 7 says on
//!   pre-production, and with no testnet declaration on production (decisions 0067 and 0143
//!   item 3), each with one fill ledger under the owner's bounds (decision 0132 item 5). Its span
//!   bound runs from then, and its ledger's horizon and replay counts are recorded as the session
//!   ends, for the report.
//! - **The provider rate refusals** ([`crate::refusals`]): each read socket's and each session's,
//!   counted as decision 0135 item 2 says, for the report: the limiter's count while it ran
//!   ([`Run::rejected`]), and its frames that were refusals or unread, a read socket's two as it
//!   reads them and a session's from its journal.
//! - **The resync's full pages** ([`crate::pages`]): each full `get_user_orders` page a session's
//!   resync read, with its page size and offset (decision 0132 item 5, FBC-4h1p), from the
//!   session's journal, since its codec's own count cannot be reached from outside the session.
//! - **The files** ([`crate::capture`]): the report, the evidence, the journal and the scrubbed
//!   fixture slice, which scripts/privacy-check.sh must pass. A record the journal dropped,
//!   marked in it or only counted by its sink, one its writer never wrote, and a journal that
//!   cannot be read to its end each make the run incomplete and the capture of every session
//!   that could have lost a record partial (decision 0006).

use std::cell::RefCell;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{
    AccountKey, ConnKey, ConnState, EncodeCtx, Envelope, ExecEvent, HttpResponse, Inbound, MonoNs,
    Namespace, NonceBlock, NonceSource, OpKind, RateCharge, RawFrame, SpecTable, TrafficClass,
    VenueCaps, VenueConfig, VenueFactory, Via, WallNs, dispatch,
};
use fbc_journal::{
    JournalError, JournalWriter, NonceSourceId, QueueSink, RedactionKey, SinkConfig, journal_queue,
};
use fbc_runtime::ws::{Message, WebSocket};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecControl, ExecHandler, ExecSession, ExecSessionConfig,
    InFlight, IngestClock, Journal, ProxyConfig, RateLimiter, ReconnectPacing, ReservedBound,
    RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::account_reads::{
    self, NotAPage, OrdersPage, PageAsk, RestRead, SocketErr, SocketWrite,
};
use fbc_venue_qfex::auth::{ACCOUNT_KEY, AccountSelection, ReadAuth, TRADE_URL, account_selection};
use fbc_venue_qfex::exec::{AUTH_DEADLINE, CHANNELS};
use fbc_venue_qfex::probe::{
    self, Environment, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, Origins,
};
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_URL};
use futures_util::{FutureExt, StreamExt};

use crate::auth_qfex::{self, Armed, Lookup, SUB_ID_VAR};
use crate::capture::{self, AccountRead, Evidence, SessionRun};
use crate::evidence;
use crate::hold::{self as held_frames, Control, End, Heard, HoldRun, ReadAfter};
use crate::ledger::{self, Opened, RegistryLedger};
use crate::probe_args::{Hold, Options, RESYNC_TIMEOUT};
use crate::refusals::{self, AFTER_HOLD, HOLD, ReadSocket, Refusals};

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The owner's interrupt, as the liveness hold waits on it: it resolves when the owner presses
/// Ctrl-C (`main.rs` wires tokio's `ctrl_c` through [`crate::interrupt`]; a test resolves its
/// own). Only the hold's connection polls it: once as the connection opens, before anything is
/// written, then while it holds, and once more when its close is over, before it drops it
/// (decision 0146 item 4).
pub type Interrupt = Pin<Box<dyn Future<Output = ()>>>;

/// What a pong, a close reply or the hold's own close is charged: a control frame, which safety
/// traffic may send from the reserve, as a session charges one (fbc-runtime's session core,
/// decision 0030).
const CONTROL: fbc_runtime::Request = fbc_runtime::Request {
    charge: RateCharge::one(OpKind::Control, None),
    via: Via::Frame,
    class: TrafficClass::Safety,
};

/// The venue, for the sessions' `&'static dyn VenueFactory`.
static QFEX: Qfex = Qfex;

/// The client-id namespace the sessions' decode scope reads ours by: stage one mints none.
const NS: Namespace = Namespace::new(1);

/// The journal's shard.
const SHARD: u16 = 0;

/// The reconnect pacing's floor, ceiling and window: one connection attempt a day.
const DAY: Duration = Duration::from_secs(86_400);

/// The connection an HTTP request or a read socket's frame is charged under: QFEX's buckets are
/// per user, and these are on no session's connection.
const NO_CONN: ConnKey = ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// The consumer's account key for each account a session arms on (decision 0122): the program's
/// own numbering.
fn account_key(armed: Armed) -> AccountKey {
    match armed {
        Armed::Primary => AccountKey::new(1),
        Armed::Subaccount => AccountKey::new(2),
    }
}

/// How an account is named in the output: never by its id.
fn name(armed: Armed) -> &'static str {
    match armed {
        Armed::Primary => "primary",
        Armed::Subaccount => "subaccount",
    }
}

/// The accounts the run's configuration names, which it reads before every session (decision
/// 0144 item 1): the primary account, then the subaccount [`SUB_ID_VAR`] names, when it is set
/// and not empty. Only whether it is set is read here; `src/auth_qfex.rs` reads its value. Never
/// a list QFEX gave: the probe sends no `GET /user/subaccounts`.
fn named(lookup: Lookup<'_>) -> Vec<Armed> {
    let sub = lookup(SUB_ID_VAR).is_some_and(|id| !id.is_empty());
    let mut named = vec![Armed::Primary];
    named.extend(sub.then_some(Armed::Subaccount));
    named
}

/// Where a run's origins pointed, as the report names it.
pub fn target(origins: Origins) -> &'static str {
    match origins {
        Origins::PreProduction => "QFEX pre-production",
        Origins::Production => "QFEX production",
        Origins::Loopback => "a loopback test stub",
    }
}

/// The environment a run's probe mode names, as the report names it (decision 0143 item 3).
pub fn environment(environment: Environment) -> &'static str {
    match environment {
        Environment::PreProduction => "pre-production",
        Environment::Production => "production",
    }
}

/// What a run ended with (the evidence is read by the integration test; the program prints the
/// result on its DONE line).
#[allow(dead_code)]
#[derive(Debug)]
pub struct Outcome {
    pub ok: bool,
    pub evidence: Evidence,
}

/// The run's nonces: QFEX's random source, one for every read and session, so no nonce repeats
/// among those it remembers (the adapter's `QfexNonces`).
#[derive(Clone)]
struct Nonces(Arc<Mutex<QfexNonces>>);

impl NonceSource for Nonces {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        self.0.lock().expect("the nonce source").reserve(len)
    }
}

/// Runs stage one as [`run_until`] does, with no interrupt: the integration test's entry, the
/// program passing the owner's.
#[allow(dead_code)]
pub async fn run(opts: &Options, lookup: Lookup<'_>, out: Out) -> Result<Outcome, String> {
    run_until(opts, lookup, out, Box::pin(std::future::pending())).await
}

/// Runs stage one: the guard, the reads, the hold when `opts.hold` asks for one (ended early by
/// `interrupt`), one session per account in `opts.arms`, then the files. An error is a refusal
/// before anything connected; once the evidence directory exists, the outcome is in the report.
pub async fn run_until(
    opts: &Options,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
) -> Result<Outcome, String> {
    // The accounts a session arms on, then the one the hold authenticates as.
    let mut used = opts.arms.clone();
    used.extend(opts.hold.map(|hold| hold.account));
    let first = *(used.first()).ok_or("--arm or --hold-secs is required: nothing to run")?;
    auth_qfex::check(lookup, &used)?;
    // Every account a session arms on or the hold authenticates as is among them: `check`
    // refuses the subaccount when no variable names it.
    let named = named(lookup);
    assert!(used.iter().all(|a| named.contains(a)), "a named account");
    let base = base_config(opts);
    // What the guard admitted of the configuration of each account the run reads, every one a
    // session may arm on included: the environment its mode names and the side its origins are
    // on. Each is the base plus its account and keys, so they are all one pair, which the run's
    // environment and the report's target are read from; the proxy's refusal still looks at
    // every one.
    let mut admitted = Vec::new();
    for account in &named {
        let (cfg, _) = configured(&base, lookup, *account)?;
        let to = probe::connections(&cfg).map_err(|e| format!("refused, nothing sent: {e}"))?;
        // 0122 item 1 admits `primary` as an account id, but the subaccount slot must name a
        // subaccount: otherwise the primary account is read twice, counted as two accounts, and
        // armed under the subaccount's key (decision 0144 item 1).
        let selected = account_selection(&cfg);
        if *account == Armed::Subaccount && matches!(selected, Ok(AccountSelection::Primary)) {
            return Err(format!(
                "refused, nothing sent: {SUB_ID_VAR} selects the primary account, which the \
                 probe reads already: give a subaccount's UUID, or unset it"
            ));
        }
        admitted.push((to.environment, to.origins));
    }
    let (env, origins) = admitted[0];
    assert!(admitted.iter().all(|a| *a == (env, origins)), "one base");
    let loopback = admitted.iter().any(|(_, o)| *o == Origins::Loopback);
    if loopback && opts.proxy != ProxyConfig::Direct {
        return Err(
            "--socks5 with loopback origins: the proxy would reach its own host's \
                    loopback, not this machine's; connect to a stub directly"
                .to_owned(),
        );
    }
    let dir = &opts.evidence_dir;
    fs::create_dir_all(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    let mut listing = fs::read_dir(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    if listing.next().is_some() {
        return Err("--evidence-dir is not empty: give a new directory".to_owned());
    }
    let (cfg, _) = configured(&base, lookup, first)?;
    let caps = QFEX.caps(&cfg).map_err(|e| e.to_string())?;
    let limits = caps.exec_limits.clone().unwrap_or_default();
    let reserve = SafetyReserve::percent(10).expect("10% is a reserve");
    let limiter = RateLimiter::for_user(opts.rate_user, &limits, reserve);
    let limiter = limiter.map_err(|e| format!("the rate limiter: {e:?}"))?;
    let mut nonces = Nonces(Arc::new(Mutex::new(QfexNonces::os())));
    let key: Vec<u8> = nonces
        .reserve(4)
        .as_slice()
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let key = Arc::new(RedactionKey::new(&key).map_err(|e| format!("the journal: {e}"))?);
    let sink = SinkConfig {
        budget_bytes: 16 << 20,
        soft_limit_pct: 90,
    };
    let (sink, drain) =
        journal_queue(sink, Arc::clone(&key)).map_err(|e| format!("the journal: {e}"))?;
    // Kept to read its drop counts once the writer has closed.
    let sink = Rc::new(RefCell::new(sink));
    let writer = JournalWriter::create(dir.join("journal"), SHARD, key);
    let writer = writer.map_err(|e| format!("the journal: {e}"))?;
    let writing = drain
        .spawn(writer)
        .map_err(|e| format!("the journal: {e}"))?;
    let target = target(origins);
    // The sessions' clock, then the run's monotonic origin just after it, on the same runtime
    // clock ([`ledger::mono_since`]).
    let clock = IngestClock::new();
    let origin = tokio::time::Instant::now();
    let mut run = Run {
        opts,
        lookup,
        out,
        base,
        caps,
        connector: Connector::new(opts.proxy.clone()),
        limiter,
        nonces,
        clock,
        journal: Journal::new(Rc::clone(&sink)),
        origin,
        environment: env,
        interrupt: Some(interrupt),
        ev: Evidence {
            started: evidence::utc(wall_secs()),
            environment: environment(env),
            mode: env.mode(),
            target,
            accounts: named.iter().map(|a| name(*a)).collect(),
            arms: opts.arms.iter().map(|a| name(*a)).collect(),
            hold_asked: opts.hold.is_some(),
            ..Evidence::default()
        },
        named,
    };
    let held = opts.hold.map_or(String::new(), |hold| {
        format!(
            "; first holds one trade connection on {} for {} s",
            name(hold.account),
            hold.duration.as_secs()
        )
    });
    run.line(format_args!(
        "PROBE stage one on QFEX {} ({}), against {target}: arms on {:?}{held}",
        environment(env),
        env.mode(),
        opts.arms
    ));
    if let Err(why) = run.stage().await {
        run.line(format_args!("STOPPED {why}"));
        run.ev.stopped = Some(why);
    }
    let Run {
        journal,
        mut ev,
        out,
        ..
    } = run;
    // Every session has dropped its handle on the journal; this one goes too, and the writer
    // thread writes what is queued, the marker of a gap still open, and closes its segment.
    // Then the sink's drop counts, every class's, which nothing offers to now: a drop the
    // journal holds no marker for counts as much as one it does (decision 0006).
    drop(journal);
    let closed = writing.close();
    journal_closed(&mut ev, closed, &sink.borrow());
    drop(sink);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let failed = write_slice(dir, &mut ev, &script).err();
    ev.errors.extend(failed);
    let (report, json) = capture::outputs(&mut ev);
    fs::write(dir.join("report.md"), report).map_err(|e| format!("report.md: {e}"))?;
    fs::write(dir.join("evidence.json"), json).map_err(|e| format!("evidence.json: {e}"))?;
    let ok = ev.ok();
    let done = if ok { "ok" } else { "INCOMPLETE" };
    let _ = writeln!(
        out.borrow_mut(),
        "DONE {done}: {} ({} fixtures)",
        dir.display(),
        ev.fixtures.len()
    );
    Ok(Outcome { ok, evidence: ev })
}

/// The configuration every session and read shares: the probe mode of the run's environment
/// (`preprod_probe`, or `production_probe` with `--production`), its settings and the origins.
/// Each session's account and public key are added from the environment, and its account key
/// from the program's numbering ([`configured`]).
fn base_config(opts: &Options) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, opts.environment().mode());
    cfg.insert(TRADE_URL, &opts.trade_url);
    cfg.insert(KEY_REST_URL, &opts.rest_url);
    cfg.insert(KEY_MDS_URL, &opts.mds_url);
    cfg.insert(KEY_PAGE_SIZE, &opts.page_size.to_string());
    let resync = format!("{}s", RESYNC_TIMEOUT.as_secs());
    cfg.insert(KEY_RESYNC_TIMEOUT, &resync);
    cfg.insert(KEY_RPC_TIMEOUT, &format!("{}ms", opts.rpc_timeout_ms));
    cfg
}

/// The configuration of the session arming on `armed`, and its credentials: `base` with the
/// account and public key the environment gives it (`auth_qfex::session`) and the account key
/// the program numbers it by (decision 0122 item 1).
fn configured(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, fbc_core::Secrets), String> {
    let (mut cfg, creds) = auth_qfex::session(base, lookup, armed)?;
    cfg.insert(ACCOUNT_KEY, &account_key(armed).get().to_string());
    Ok((cfg, creds))
}

/// Now on the local wall clock.
fn wall_ns() -> WallNs {
    let wall = SystemTime::now().duration_since(UNIX_EPOCH);
    WallNs(wall.map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)))
}

/// The seconds since the epoch now.
fn wall_secs() -> i64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH);
    now.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// The records `sink` dropped since it was made, Normal then Safety ([`Evidence::dropped`]).
pub fn journal_dropped(sink: &QueueSink) -> [u64; 2] {
    [TrafficClass::Normal, TrafficClass::Safety].map(|class| sink.dropped(class))
}

/// What the run knows of its journal once the writer thread has closed (`closed`, what
/// `WriterThread::close` returned), which the slice holds the journal it reads to (decision
/// 0006): the error that stopped the writer, if one did ([`Evidence::writer_failed`], and among
/// the errors), and `sink`'s drop counts ([`Evidence::dropped`]), which nothing offers to now.
pub fn journal_closed(ev: &mut Evidence, closed: Result<(), JournalError>, sink: &QueueSink) {
    if let Err(e) = closed {
        ev.errors.push(format!("the journal's writer: {e}"));
        ev.writer_failed = Some(e.to_string());
    }
    ev.dropped = journal_dropped(sink);
}

/// Writes the fixture slice of the journal under `dir` into `dir/fixtures`, then runs the
/// privacy check `script` over it: fixtures it refuses are removed, never left to copy.
pub fn write_slice(dir: &Path, ev: &mut Evidence, script: &Path) -> Result<(), String> {
    let files = capture::slice(&dir.join("journal"), SHARD, ev)?;
    let fixtures = dir.join("fixtures");
    capture::write_fixtures(&fixtures, files, ev)?;
    let checked = evidence::privacy_check(script, &fixtures);
    if checked.is_err() {
        let _ = fs::remove_dir_all(&fixtures);
        ev.fixtures.clear();
    }
    checked.map_err(|e| format!("{e}; the fixtures directory was removed"))
}

/// One run's shared state.
struct Run<'a> {
    opts: &'a Options,
    lookup: Lookup<'a>,
    out: Out,
    base: VenueConfig,
    /// The probe mode's caps, whose scope decodes the reads.
    caps: VenueCaps,
    connector: Connector,
    limiter: RateLimiter,
    nonces: Nonces,
    clock: IngestClock,
    journal: Journal,
    /// The run's monotonic origin, on the runtime's clock, taken just after `clock`'s.
    origin: tokio::time::Instant,
    /// The environment the run's probe mode names, which its registries are built for.
    environment: Environment,
    /// The owner's interrupt, until the hold takes it.
    interrupt: Option<Interrupt>,
    ev: Evidence,
    /// The accounts the run's configuration names, read before every session ([`named`]).
    named: Vec<Armed>,
}

impl Run<'_> {
    fn line(&self, args: fmt::Arguments<'_>) {
        let _ = writeln!(self.out.borrow_mut(), "{args}");
    }

    /// The hold, when the run asks for one, then each session, each after the checks of every
    /// account the configuration names.
    async fn stage(&mut self) -> Result<(), String> {
        self.line(format_args!(
            "ACCOUNTS {} named by the run's configuration, not listed by QFEX (decision 0144): {}",
            self.ev.accounts.len(),
            self.ev.accounts.join(", ")
        ));
        if let Some(hold) = self.opts.hold {
            self.hold(hold).await?;
        }
        let arms = self.opts.arms.clone();
        for (n, armed) in arms.into_iter().enumerate() {
            let (cfg, creds) = configured(&self.base, self.lookup, armed)?;
            self.check_accounts(name(armed), armed, " (armed)").await?;
            let bounds = (self.opts.ledger.as_ref()).ok_or("--arm without the ledger's bounds")?;
            // Stage one's registries serve one session each: each arms its own account.
            let opened = Opened::now(self.origin, wall_ns());
            let label = name(armed);
            let mut registry =
                RegistryLedger::new(label, account_key(armed), bounds, opened, self.environment);
            let ran = self
                .session(n, armed, cfg, creds, registry.deadline())
                .await;
            registry.session_ended(label, self.mono());
            self.ev.registries.push(registry.run().clone());
            self.ev.sessions.push(ran?);
        }
        Ok(())
    }

    /// Now on the run's monotonic clock: the runtime's, from just after the sessions'
    /// `IngestClock` started on it ([`ledger::mono_since`]).
    fn mono(&self) -> MonoNs {
        ledger::mono_since(self.origin)
    }

    /// The context of one signed read: now, and two nonces from the run's source.
    fn ctx(&mut self) -> EncodeCtx {
        EncodeCtx {
            wall: wall_ns(),
            mono: self.mono(),
            nonces: self.nonces.reserve(ReadAuth::NONCES),
        }
    }

    /// Charges one request to the run's limiter, or refuses it, nothing sent, when the buckets
    /// have no room for it now.
    fn admit(&self, charge: RateCharge, via: Via, class: TrafficClass) -> Result<(), String> {
        let request = fbc_runtime::Request { charge, via, class };
        let charged = self
            .limiter
            .charge(tokio::time::Instant::now(), NO_CONN, &[request]);
        charged
            .map(|_| ())
            .map_err(|_| "not sent: the rate limit has no room".to_owned())
    }

    async fn rest(&self, read: &RestRead) -> Result<(u16, Vec<u8>), String> {
        self.admit(read.charge, Via::Http, read.class)?;
        auth_qfex::send(&self.connector, &read.req, self.opts.read_wait).await
    }

    /// Writes one of the adapter's frames on a read socket or the hold's connection, waiting at
    /// most `--read-wait-ms` for a peer that stopped reading.
    async fn write(&self, ws: &mut WebSocket, write: &SocketWrite) -> Result<(), String> {
        self.admit(write.charge, Via::Frame, write.class)?;
        let wait = self.opts.read_wait;
        let written = tokio::time::timeout(wait, auth_qfex::write(ws, &write.frame)).await;
        written.map_err(|_| format!("the socket took no frame within {} ms", wait.as_millis()))?
    }

    /// Reads every account the configuration names before `before`, the session on `on` or the
    /// hold ([`HOLD`]) on it, `on`'s label marked `mark`, each under its own configuration
    /// ([`configured`]), which the guard admitted before anything connected; stops the probe
    /// at the first that cannot be read, or holds an open order, a TWAP or a position that is
    /// not flat.
    async fn check_accounts(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
    ) -> Result<(), String> {
        for account in self.named.clone() {
            self.check_account(before, on, mark, account).await?;
        }
        Ok(())
    }

    /// Reads every account the configuration names after the hold on `on` ([`AFTER_HOLD`]), as
    /// [`Run::check_accounts`] does, but each whatever another showed, so the evidence holds
    /// every account's read after the hold (decision 0146 item 3); stops the probe once all were
    /// read when any could not be read or is not clean, naming each.
    async fn check_accounts_after_hold(&mut self, on: Armed) -> Result<(), String> {
        let mut failed = Vec::new();
        for account in self.named.clone() {
            let checked = self.check_account(AFTER_HOLD, on, " (held)", account).await;
            failed.extend(checked.err());
        }
        match failed.is_empty() {
            true => Ok(()),
            false => Err(failed.join("; ")),
        }
    }

    /// Reads `account` before `before`, as [`Run::check_accounts`] reads each.
    async fn check_account(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
    ) -> Result<(), String> {
        let mut label = name(account).to_owned();
        label.push_str(if account == on { mark } else { "" });
        let (cfg, _) = configured(&self.base, self.lookup, account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let mut met = None;
        let read = self.read_account(&cfg, selection, &mut met).await;
        // A read socket's refusals are reported whether or not its read succeeded.
        if let Some(refusals) = met {
            self.ev.read_sockets.push(ReadSocket {
                before,
                label: label.clone(),
                refusals,
            });
        }
        let read = read.map_err(|e| format!("{label} could not be read: {e}"))?;
        let refusal = read.refusal();
        let shown = format!(
            "{} open orders, {} TWAPs, flat {}, REST open_orders {}",
            read.page_orders, read.page_twaps, read.flat, read.rest_open_orders
        );
        let when = refusals::read_when(before);
        self.line(format_args!("READ {when}: {label}: {shown}"));
        self.ev.reads.push(AccountRead {
            before,
            label: label.clone(),
            ..read
        });
        if let Some(why) = refusal {
            let starts = match before {
                HOLD => "no hold starts (decision 0146 item 3)",
                AFTER_HOLD => {
                    "read after the hold, though every account showed none before it: what \
                     decision 0146 says would show it wrong; no session starts"
                }
                _ => "no session starts (decision 0132 item 7)",
            };
            return Err(format!("{label} {why}: {starts}"));
        }
        Ok(())
    }

    /// One account's positions, and its open orders on a trade socket of its own; `met` is the
    /// provider rate refusals that socket met, once it opened.
    async fn read_account(
        &mut self,
        cfg: &VenueConfig,
        account: AccountSelection<'_>,
        met: &mut Option<Refusals>,
    ) -> Result<AccountRead, String> {
        let reader = auth_qfex::reader(self.lookup, account)?;
        let ctx = self.ctx();
        let read = account_reads::positions(cfg, &reader, &ctx).map_err(|e| e.to_string())?;
        let (status, body) = self.rest(&read).await?;
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        let positions = dispatch(&self.caps, NS, |scope| {
            account_reads::decode_positions(&resp, scope)
        });
        let positions = positions.map_err(|e| format!("GET /user/positions: {e}"))?;
        let url = account_reads::trade_url(cfg, &reader).map_err(|e| e.to_string())?;
        let mut ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await?;
        let before = self.rejected();
        let mut heard = Refusals::default();
        let page = self.page(cfg, &reader, &mut ws, &mut heard).await;
        heard.limiter = self.rejected().saturating_sub(before);
        *met = Some(heard);
        let wait = self.opts.read_wait;
        let closed = tokio::time::timeout(wait, ws.close(None)).await;
        let page = page?;
        // A close the peer refuses is passed over (it may have closed first); one it never
        // takes fails the read, so the run cannot hang on it.
        let _ = closed.map_err(|_| {
            format!(
                "the read socket did not close within {} ms",
                wait.as_millis()
            )
        })?;
        Ok(AccountRead {
            rows: positions.rows().len(),
            flat: positions.is_flat(),
            rest_open_orders: positions.rows().iter().map(|row| row.open_orders).sum(),
            page_orders: page.orders().len(),
            page_twaps: page.twaps(),
            page_full: page.is_full(),
            ..AccountRead::default()
        })
    }

    /// The socket's authentication, its success, then one `get_user_orders` page from offset 0;
    /// each of the two frames it reads counted in `heard` ([`Run::heard`]), which holds the
    /// shared budget for a `RateLimited` err in either.
    async fn page(
        &mut self,
        cfg: &VenueConfig,
        reader: &ReadAuth,
        ws: &mut WebSocket,
        heard: &mut Refusals,
    ) -> Result<OrdersPage, String> {
        let ctx = self.ctx();
        let auth = account_reads::authentication(cfg, reader, &ctx).map_err(|e| e.to_string())?;
        let ask = PageAsk::new(self.opts.page_size, 0).map_err(|e| e.to_string())?;
        self.write(ws, &auth).await?;
        let reply = next(ws, self.opts.read_wait).await?;
        let _ = self.heard(heard, reader, &reply, ask);
        if !account_reads::authenticated(reader, reply.raw()) {
            return Err("QFEX did not authenticate the read socket".to_owned());
        }
        let ask_frame = account_reads::user_orders(cfg, ask).map_err(|e| e.to_string())?;
        self.write(ws, &ask_frame).await?;
        let reply = next(ws, self.opts.read_wait).await?;
        let page = self.heard(heard, reader, &reply, ask);
        page.map_err(|e| format!("get_user_orders: {e}"))
    }

    /// Counts `frame`, off `reader`'s read socket, in `heard` ([`Refusals::hear`]): as a journal
    /// would keep it, its spans the adapter's (`account_reads::redact_inbound`, blanked by
    /// [`refusals::kept`]), and, when that is unread, as it arrived, in memory only. Then reads it
    /// as the adapter reads a page, which reads an err before anything else: a `RateLimited` err,
    /// whether it answers the authentication or the page, holds the shared budget, as a session's
    /// runtime holds it for its codec's errs in every state (decision 0126); one the journal
    /// would keep whole holds none, as the codec holds none for it (0134 item 6).
    fn heard(
        &self,
        heard: &mut Refusals,
        reader: &ReadAuth,
        frame: &Data,
        ask: PageAsk,
    ) -> Result<OrdersPage, NotAPage> {
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        heard.hear(
            &refusals::kept(raw, &spans),
            &String::from_utf8_lossy(raw.bytes()),
        );
        let page = dispatch(&self.caps, NS, |scope| {
            account_reads::orders_page(reader, raw, ask, scope)
        });
        if let Err(NotAPage::RateLimited(hold)) = &page {
            let now = tokio::time::Instant::now();
            (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
        }
        page
    }

    /// The run's limiter's count of provider refusals so far: its user count, every one it
    /// counted, since its buckets are all user-scoped (decision 0123).
    fn rejected(&self) -> u64 {
        self.limiter.counts().rejected.user
    }

    /// The liveness hold (decision 0146) on `hold.account`: every named account read as before
    /// a session, then its one connection ([`Run::held`]), then, once that connection opened,
    /// every named account read again, whatever ended it and whatever another account's read
    /// after it showed ([`Run::check_accounts_after_hold`], item 3). What it heard is the
    /// evidence's whether or not it did what the record asks. One that did not, an account read
    /// after it that shows an open order or a position, and the owner's interrupt in a run that
    /// also arms each stop the probe, as a refused read does, so no session starts.
    async fn hold(&mut self, hold: Hold) -> Result<(), String> {
        self.check_accounts(HOLD, hold.account, " (held)").await?;
        let label = name(hold.account);
        self.line(format_args!(
            "HOLD {label}: one trade connection for {} s: its authentication and three \
             subscriptions, then nothing; it never arms or places",
            hold.duration.as_secs()
        ));
        let mut run = HoldRun {
            account: label,
            duration: hold.duration,
            ..HoldRun::default()
        };
        let before = self.rejected();
        let held = self.held(hold, &mut run).await;
        run.refusals.limiter = self.rejected().saturating_sub(before);
        if let Err(why) = held {
            run.error.get_or_insert(why);
        }
        let pings = run.pings();
        let closes = run.server_closes().len();
        self.line(format_args!(
            "HOLD {label}: ended by {}; {} pings ({} before authentication), {closes} server \
             closes",
            run.ended_by.says(),
            pings.len(),
            run.pings_before_auth()
        ));
        let mut why = run.stops();
        if why.is_none() && !self.opts.arms.is_empty() {
            why = match (&run.ended_by, run.interrupted_closing) {
                (End::Interrupted, _) => {
                    Some("the owner's interrupt (Ctrl-C) ended the hold: no session starts".into())
                }
                (_, true) => Some(
                    "the owner's interrupt (Ctrl-C) came as the hold closed: no session starts"
                        .into(),
                ),
                _ => None,
            };
        }
        // The reads after it, once its connection opened: a hold that never opened held nothing.
        if run.opened {
            let after = self.check_accounts_after_hold(hold.account).await;
            run.read_after = match after {
                Ok(()) => ReadAfter::Clean,
                Err(e) => {
                    let after = format!("after the hold: {e}");
                    why = Some(match why {
                        Some(why) => format!("{why}; {after}"),
                        None => after,
                    });
                    ReadAfter::Stopped(e)
                }
            };
        }
        self.ev.hold = Some(run);
        why.map_or(Ok(()), Err)
    }

    /// Charges the hold's own close to the run's limiter as a control frame, as a session
    /// charges its own: refused, nothing sent, when the buckets have no room for it.
    fn admit_close(&self) -> Result<(), String> {
        self.admit(CONTROL.charge, CONTROL.via, CONTROL.class)
    }

    /// The hold's connection: opened on `hold.account`'s guarded trade origin, held until it
    /// ends ([`Run::holding`]), then closed: at its duration, the owner's interrupt or a
    /// refusal, a close and the server's echo read for at most `--read-wait-ms`; at a server
    /// close, read on until the close reply has gone out; an err read meanwhile is kept as its
    /// refusal, unless one already refused it. The owner's interrupt is the hold's
    /// from the connection's opening until then: a press that did not end it, made while it
    /// closed or ready as something else ended it, is recorded as one made as it closed
    /// (`run.interrupted_closing`). An error before it opened is returned; everything after is
    /// in `run`.
    async fn held(&mut self, hold: Hold, run: &mut HoldRun) -> Result<(), String> {
        let (cfg, _) = configured(&self.base, self.lookup, hold.account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let reader = auth_qfex::reader(self.lookup, selection)?;
        let url = account_reads::trade_url(&cfg, &reader).map_err(|e| e.to_string())?;
        let subscriptions = account_reads::subscriptions(&cfg).map_err(|e| e.to_string())?;
        // The owner's interrupt, which only this connection waits on, and which goes with it.
        let mut interrupt =
            (self.interrupt.take()).unwrap_or_else(|| Box::pin(std::future::pending()));
        let ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await;
        let mut ws = ws.map_err(of_the_hold)?;
        let opened = tokio::time::Instant::now();
        run.opened = true;
        // Polled now, before anything is written, so the interrupt listens from the
        // connection's opening (`main.rs` hands it each Ctrl-C from its first poll on).
        let ended = match interrupt.as_mut().now_or_never() {
            Some(()) => End::Interrupted,
            None => {
                self.holding(
                    &cfg,
                    &reader,
                    &subscriptions,
                    &mut ws,
                    opened,
                    &mut interrupt,
                    run,
                )
                .await
            }
        };
        run.ended_at = Some(opened.elapsed());
        let wait = self.opts.read_wait;
        let mut drain = ended != End::NoCloseFrame;
        if matches!(ended, End::Duration | End::Interrupted | End::Refused) {
            match self.admit_close() {
                Ok(()) => {
                    // From here a close read is the echo of its own, though the write may not
                    // finish: the next read flushes a close queued and not yet written.
                    run.close_begun = Some(opened.elapsed());
                    let closed = tokio::time::timeout(wait, ws.close(None)).await;
                    if matches!(closed, Ok(Ok(()))) {
                        run.close_sent = Some(opened.elapsed());
                    }
                }
                // No room for it: the connection is dropped with no close, as a session's is.
                Err(_) => {
                    run.close_refused = true;
                    drain = false;
                }
            }
        }
        if drain {
            // The server's echo of the close, or, after the server's close, the reply the
            // WebSocket layer writes on the next read; then the stream's end. A data frame
            // still in flight is read meanwhile: an err among them is a refusal as one that
            // ended the hold is (decision 0146 item 4), the first kept, so the hold is not what
            // the record asks and a run that also arms starts no session.
            let drained = async {
                while let Some(Ok(msg)) = ws.next().await {
                    if let Some(data) = hear(run, opened.elapsed(), msg, &self.limiter)
                        && let Some(err) = self.hold_data(run, &reader, &data)
                    {
                        run.error
                            .get_or_insert_with(|| format!("{err}, read as the hold closed"));
                    }
                }
            };
            let _ = tokio::time::timeout(wait, drained).await;
        }
        // A press while it closed, or one ready as its duration ended it and the wait took the
        // duration, is the owner's interrupt as much as one that ended it. An interrupt that
        // ended the hold is not polled again.
        run.interrupted_closing =
            ended != End::Interrupted && interrupt.as_mut().now_or_never().is_some();
        drop(ws);
        drop(interrupt);
        run.ended_by = ended;
        Ok(())
    }

    /// The hold's connection from its opening to its end: its authentication, then, once QFEX
    /// answers it with success, the three subscriptions, then nothing; every frame it receives
    /// recorded ([`hear`], [`Run::hold_data`]). It ends at its duration from `opened`, at
    /// `interrupt`, at a server close, at the connection's end, or at a refusal (`run.error`
    /// says which): the authentication refused, a frame that is an err, or the three
    /// confirmations not all in within QFEX's authentication deadline.
    #[allow(clippy::too_many_arguments)]
    async fn holding(
        &mut self,
        cfg: &VenueConfig,
        reader: &ReadAuth,
        subscriptions: &[SocketWrite],
        ws: &mut WebSocket,
        opened: tokio::time::Instant,
        interrupt: &mut Interrupt,
        run: &mut HoldRun,
    ) -> End {
        fn refused(run: &mut HoldRun, why: String) -> End {
            run.error = Some(why);
            End::Refused
        }
        let ctx = self.ctx();
        let sent = match account_reads::authentication(cfg, reader, &ctx) {
            Ok(auth) => self.write(ws, &auth).await.map_err(of_the_hold),
            Err(e) => Err(e.to_string()),
        };
        if let Err(why) = sent {
            return refused(run, why);
        }
        run.sent.push("authentication".to_owned());
        let end = opened + run.duration;
        let deadline = opened + AUTH_DEADLINE;
        let mut confirmed = [false; CHANNELS.len()];
        loop {
            let settled = run.subscribed.is_some();
            let msg = tokio::select! {
                msg = ws.next() => msg,
                () = tokio::time::sleep_until(end) => return End::Duration,
                () = &mut *interrupt => return End::Interrupted,
                () = tokio::time::sleep_until(deadline), if !settled => {
                    let why = format!(
                        "QFEX did not authenticate the hold's connection and confirm its three \
                         subscriptions within {} s",
                        AUTH_DEADLINE.as_secs()
                    );
                    return refused(run, why);
                }
            };
            let at = opened.elapsed();
            let msg = match msg {
                Some(Ok(msg)) => msg,
                // The stream failed, with its error, or ended.
                ended => {
                    run.dropped = ended.and_then(Result::err).map(|e| e.to_string());
                    return End::NoCloseFrame;
                }
            };
            let closing = matches!(msg, Message::Close(_));
            let Some(data) = hear(run, at, msg, &self.limiter) else {
                match closing {
                    true => return End::ServerClose,
                    false => continue,
                }
            };
            if let Some(err) = self.hold_data(run, reader, &data) {
                return refused(run, err);
            }
            let raw = data.raw();
            if run.authenticated.is_none() {
                if !account_reads::authenticated(reader, raw) {
                    let why = "QFEX did not authenticate the hold's connection".to_owned();
                    return refused(run, why);
                }
                run.authenticated = Some(at);
                for (subscription, channel) in subscriptions.iter().zip(CHANNELS) {
                    if let Err(why) = self.write(ws, subscription).await {
                        return refused(run, of_the_hold(why));
                    }
                    run.sent.push(format!("subscribe {channel}"));
                }
            } else if let Some(at_channel) = account_reads::confirmation(reader, raw)
                && !std::mem::replace(&mut confirmed[at_channel], true)
            {
                run.confirmed += 1;
                if run.confirmed == CHANNELS.len() {
                    run.subscribed = Some(at);
                }
            }
        }
    }

    /// Counts `frame`, a data frame off the hold's connection, by kind ([`held_frames::kind_of`])
    /// and reads it for a provider rate refusal as a read socket's ([`Refusals::hear`]): as a
    /// journal would keep it, and, when that is unread, as it arrived, in memory only; nothing of
    /// it is kept but its kind's name. Returns the refusal it is when it is an err, which ends
    /// the hold (decision 0146 item 4): read as the adapter reads one from what the journal
    /// keeps, having held the shared budget for a `RateLimited` one, as for a read socket's
    /// (decision 0126); or, for an err the journal would keep whole, read in memory as it
    /// arrived ([`held_frames::whole_err`]), holding no budget, as a session's codec holds none
    /// for one (decision 0134 item 6).
    fn hold_data(&self, run: &mut HoldRun, reader: &ReadAuth, frame: &Data) -> Option<String> {
        run.data_frames += 1;
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        let (kept, arrived) = (
            refusals::kept(raw, &spans),
            String::from_utf8_lossy(raw.bytes()),
        );
        let kind = match frame {
            Data::Binary(_) => held_frames::BINARY.to_owned(),
            Data::Text(_) => held_frames::kind_of(&kept, &arrived),
        };
        let whole = kind == format!("err {}", held_frames::KEPT_WHOLE);
        *run.kinds.entry(kind).or_default() += 1;
        (run.refusals).hear(&kept, &arrived);
        let err = account_reads::socket_err(reader, raw);
        if let Some(SocketErr::RateLimited(hold)) = &err {
            let now = tokio::time::Instant::now();
            (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
        }
        match err {
            Some(err) => Some(err.to_string()),
            None if whole => held_frames::whole_err(&arrived),
            None => None,
        }
    }

    /// One session on `armed`, connection `n` of the run: it arms once and places nothing, and
    /// is stopped at `span_end` at the latest, its registry's span bound.
    async fn session(
        &mut self,
        n: usize,
        armed: Armed,
        cfg: VenueConfig,
        creds: fbc_core::Secrets,
        span_end: tokio::time::Instant,
    ) -> Result<SessionRun, String> {
        let acct = account_key(armed);
        let before = self.rejected();
        let seen = Rc::new(RefCell::new(Seen::default()));
        let control = Rc::new(RefCell::new(None));
        let watch = Watch {
            seen: Rc::clone(&seen),
            control: Rc::clone(&control),
        };
        let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, self.opts.connect_wait);
        let bound = ReservedBound::new(4, 4).expect("a bound with Normal room");
        let config = ExecSessionConfig {
            venue: &QFEX,
            cfg,
            creds,
            acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
            rpc_ids: RpcIds::default(),
            ns: NS,
            specs: SpecTable::new(),
            connector: self.connector.clone(),
            pacing: pacing.expect("one attempt a day, each within --connect-wait-ms"),
            clock: self.clock.clone(),
            nonces: Box::new(self.nonces.clone()),
            nonce_source: NonceSourceId(0),
            conn: u16::try_from(n).expect("one or two sessions"),
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        };
        let label = name(armed);
        let built = ExecSession::new(config, watch);
        let (mut session, ctl) = built.map_err(|e| format!("the {label} session: {e}"))?;
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        self.line(format_args!(
            "SESSION {label}: it arms once and places nothing"
        ));
        let ran = {
            let run = session.run();
            tokio::pin!(run);
            tokio::select! {
                ran = &mut run => ran,
                () = tokio::time::sleep(self.opts.session_wait) => {
                    Watch::stop(&seen, &control, "--session-wait");
                    run.await
                }
                // Stage one places nothing, so a session at its span bound has nothing to
                // cancel: the stop control alone ends it, a moment later (the report's past the
                // span bound).
                () = tokio::time::sleep_until(span_end) => {
                    Watch::stop(&seen, &control, "--registry-span");
                    run.await
                }
            }
        };
        let counters = session.counters();
        drop(session);
        let seen = seen.borrow();
        let arm = seen.arm.as_deref().unwrap_or("none");
        let ended_by = seen.stopped.unwrap_or("the session's own end");
        self.line(format_args!(
            "SESSION {label}: arm outcome {arm}, ended by {ended_by}"
        ));
        Ok(SessionRun {
            account: label,
            authenticated: seen.authenticated,
            arm: seen.arm.clone(),
            ended_by,
            attempts: counters.attempts,
            failed_attempts: counters.failed_attempts,
            arm_failures: counters.arm_failures,
            decode_errors: counters.decode_errors,
            error: ran.err().map(|e| e.to_string()),
            // The holds its runtime made while it ran; its frames' counts are the slice's.
            refusals: Refusals {
                limiter: self.rejected().saturating_sub(before),
                ..Refusals::default()
            },
            ..SessionRun::default()
        })
    }
}

/// `error`, from `auth_qfex`'s socket helpers, which the read sockets share and which name the
/// read socket, said of the hold's connection.
fn of_the_hold(error: String) -> String {
    error
        .replacen(
            "no read socket within",
            "the hold's connection did not open within",
            1,
        )
        .replacen("the read socket", "the hold's connection", 1)
}

/// Records `msg`, which the hold's connection received `at` after it opened: a ping, pong or
/// close in `run.heard` (a payload and a reason by length only, a code in full, a close after
/// the hold began its own as its echo); a data frame is handed back, for [`Run::hold_data`]. The
/// reply the WebSocket layer writes to a ping (its pong, none once either side began to close)
/// or to a server close (its close reply; none to the echo of the hold's own) is recorded on
/// `limiter` as a control frame, past its cap if need be, as a session records it (decision
/// 0030).
pub fn hear(run: &mut HoldRun, at: Duration, msg: Message, limiter: &RateLimiter) -> Option<Data> {
    let control = match msg {
        Message::Text(text) => return Some(Data::Text(text.as_str().to_owned())),
        Message::Binary(bytes) => return Some(Data::Binary(bytes.to_vec())),
        Message::Ping(payload) => Control::Ping { len: payload.len() },
        Message::Pong(payload) => Control::Pong { len: payload.len() },
        Message::Close(frame) => Control::Close {
            code: frame.as_ref().map(|f| u16::from(f.code)),
            reason_len: frame.as_ref().map_or(0, |f| f.reason.len()),
            echo: run.close_begun.is_some(),
        },
        // A raw frame is something to send: reading never yields one.
        Message::Frame(_) => return None,
    };
    // The WebSocket layer answers a ping only while the connection is open both ways: before
    // the hold began its own close and before any close of the server's (tungstenite answers a
    // ping only in its active state). A close of the server's is yielded only in that state,
    // and answered.
    let open = run.close_begun.is_none() && run.server_closes().is_empty();
    let answered = match control {
        Control::Ping { .. } => open,
        Control::Close { echo, .. } => !echo,
        Control::Pong { .. } => false,
    };
    if answered {
        limiter.record(tokio::time::Instant::now(), NO_CONN, CONTROL);
        run.replies += 1;
    }
    run.heard.push(Heard { at, control });
    None
}

/// A data frame off a read socket or the hold's connection.
pub enum Data {
    Text(String),
    Binary(Vec<u8>),
}

impl Data {
    /// The frame as the adapter reads one.
    fn raw(&self) -> RawFrame<'_> {
        match self {
            Data::Text(text) => RawFrame::Text(text),
            Data::Binary(bytes) => RawFrame::Binary(bytes),
        }
    }
}

/// The next data frame on a read socket within `wait`; pings and pongs are passed over.
async fn next(ws: &mut WebSocket, wait: Duration) -> Result<Data, String> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Data::Text(text.as_str().to_owned())),
                Some(Ok(Message::Binary(bytes))) => return Ok(Data::Binary(bytes.to_vec())),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                _ => return Err("the read socket closed before it answered".to_owned()),
            }
        }
    };
    let waited = tokio::time::timeout(wait, read).await;
    waited.map_err(|_| format!("no answer within {} ms", wait.as_millis()))?
}

/// What stops a session that ran its course: the arm's outcome and the resync's end.
pub const ARM_AND_RESYNC: &str = "the arm's outcome and the resync's end";

/// What a session's handler saw.
#[derive(Default)]
struct Seen {
    authenticated: bool,
    /// The arm's outcome: the first outcome the session reports, since the program submits
    /// nothing and the arm is the session's only request.
    arm: Option<String>,
    /// Whether the session's resync has ended (`ResyncEnd`).
    resynced: bool,
    /// What stopped the session, when the program did.
    stopped: Option<&'static str>,
}

/// The session's handler: it stops the session once the arm has its outcome and the resync has
/// ended, in either order, or at the connection's end.
struct Watch {
    seen: Rc<RefCell<Seen>>,
    control: Rc<RefCell<Option<ExecControl>>>,
}

impl Watch {
    /// Records `why` when nothing stopped the session yet, and drops its control: the stop.
    fn stop(seen: &RefCell<Seen>, control: &RefCell<Option<ExecControl>>, why: &'static str) {
        seen.borrow_mut().stopped.get_or_insert(why);
        drop(control.borrow_mut().take());
    }
}

impl ExecHandler for Watch {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let done = {
            let mut seen = self.seen.borrow_mut();
            let state = ConnState::Authenticated;
            seen.authenticated |=
                matches!(env.body, ExecEvent::Conn { state: s, .. } if s == state);
            match env.body {
                ExecEvent::Outcome { outcome, .. } if seen.arm.is_none() => {
                    seen.arm = Some(format!("{outcome:?}"));
                }
                ExecEvent::ResyncEnd => seen.resynced = true,
                _ => {}
            }
            seen.arm.is_some() && seen.resynced
        };
        if done {
            Watch::stop(&self.seen, &self.control, ARM_AND_RESYNC);
        }
    }

    fn on_epoch_end(&mut self, _key: ConnKey) {
        Watch::stop(&self.seen, &self.control, "the connection's end");
    }
}
