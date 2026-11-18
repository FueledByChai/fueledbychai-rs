//! Stage one of the QFEX probe (decision 0132 items 1, 3, 6 and 7; FBC-l931), on pre-production,
//! or with `--production` on production (decision 0143): one session per account, each arming
//! cancel-on-disconnect once, so the journal captures whatever answers the arm. Stage one places
//! nothing and sends no command but each session's arm, its resync's reads and the probe's own
//! reads. With `--hold-secs` it first holds one trade connection that never arms (decision 0146),
//! and a run may hold without arming at all. With `--place-stages` it runs the place stages'
//! preflight and each named account's drop test, which places X and Y (below).
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
//!   writing no report, as the probe always has, but in a place-stage run, whose press from the
//!   hold's first read to its last is the hold's too ([`Run::hold`]).
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
//! - **The place stages' preflight** ([`run_places_until`], FBC-c6kw, the first slice of
//!   FBC-9sk2; decision 0148): a run with `--place-stages` prints a `CAPS` line with the
//!   environment and both caps first, before any request, the hold's included (decisions 0113
//!   items 1 and 2 and 0148 item 7); it is refused, nothing sent, unless every origin is a
//!   loopback test stub's (until FBC-4plj lands every stop decisions 0132 item 7 and 0148 item 8
//!   require), and unless `--registry-span` is above the drop test's floor for the accounts the
//!   configuration names (`probe_args::check_place_span`, 0151 item 3). It holds first when
//!   `--hold-secs` asks, alone (the owner's Ctrl-C from the hold's first read to its last stops
//!   it before its preflight, with no stop marker, [`Run::hold`]),
//!   then runs [`crate::market`]'s preflight (the spec from this run's
//!   `GET /refdata`, the count bound, the MDS touch and the caps), then each named account's
//!   drop test ([`Run::drop_tests`], below). [`place_config`] is the configuration a place
//!   stage's sessions take, the only one with `qfex.exec.production_places` (decision 0148 item
//!   4).
//!   Before anything connects it opens its lease directory ([`places::RunLeases::open`], FBC-23ch:
//!   the production tag, the stop markers held to `--reconciled`, the namespace lease and the
//!   run's mint; 0151 items 1 and 6). With `--reconciled` it reads every named account after the
//!   preflight and clears the marker only if every read is clean ([`Run::reconcile`]). The
//!   owner's Ctrl-C outside the hold ([`Presses`]) during the preflight or those reads stops the
//!   run, [`Stop::Interrupted`], and any stop of the place stages but `NoEcho` writes the stop
//!   marker with the report ([`Run::mark`]), keeping the stops of the marker it replaces unless
//!   the run cleared it.
//! - **The journal's tee** ([`crate::tee`], FBC-23ch): the run's journal sink is wrapped by a
//!   [`Tee`], which hands every record on unchanged and reads the sessions' frames as the journal
//!   keeps them, for the stops the drop sessions wait on.
//! - **The drop test** ([`Run::drop_tests`], FBC-23ch and FBC-77r8; decisions 0132 items 4 and 7,
//!   0148 and 0151) on each named account in turn, each starting only once the one before ended
//!   with no stop. **Drop session one**: every named account read clean (unless the reads that
//!   cleared a stop marker just did); the account's drop registry built, its `CancelOnlyTrip` given
//!   to the journal before any market arms (decision 0139), seeded flat (0148 item 3); one
//!   unchanged `ExecSession` on [`place_config`] with the discovered spec table, the run's
//!   `--namespace`, its one `RpcIds`, the run's next session number, an `AccountWideCodAuthority`
//!   for its own account (decisions 0121 and 0122) and one connection attempt, its handler a
//!   [`places::PlaceHandler`] over the registry's `Link`; once it is authenticated, its arm
//!   accepted and its resync applied, Start under the market lease, then X, a post-only GTC at the
//!   instrument's minimum size `--away-bps` behind the MDS touch, kept current meanwhile, minted
//!   under the run's namespace lease and authorized by fbc-oms, the one place before an ACK echoes
//!   our client id (decision 0132 item 7), spent from the registry's list first
//!   ([`places::Allowance`]); X's ACK or its deadline; the streams compared with never-arming reads
//!   ([`crate::compare`]); a graceful close; the read of every named account after it, and X and
//!   its trades read (FBC-be57). The first stop the tee, the handler, a comparison, the span bound
//!   or the owner's Ctrl-C raises ends the steps: only Ctrl-C and a fill of ours then cancel, by
//!   venue id (0151 item 2, [`Run::stop_cancels`]), and every session ends by a graceful close,
//!   cancels held included (0151 item 11), unless its connection ended first. The reads after it
//!   wait out a provider rate refusal's hold on the shared budget ([`Run::admitted`]), as does the
//!   close. A stop ends the run, its marker written, X still resting reported for the owner's hand
//!   cancel. **Drop session two** (FBC-77r8): the read before it of every named account
//!   ([`Run::read_before_two`]), which admits X alone, one open order on its symbol (decision 0132
//!   item 7, 0144 item 4); then a session on the same `Link` and registry, seeded nothing (0148
//!   item 3) and not Started again, its market armed from drop session one: its arm accepted; Y, as
//!   X was placed; Y's ACK or its deadline; the read before the drop, one never-arming read beside
//!   the session, a `CANCELLED` of X heard by its end being the arm's, reported, and a stop or a
//!   press met as its socket closes taken before the drop; then the abrupt drop, its run dropped
//!   unpolled, its connection with it, with no WebSocket close. From the first arm to the last read
//!   nothing cancels X or Y but Ctrl-C or a fill of ours, nothing reconnects, and no later arm is
//!   sent (decision 0132 item 4). **After the drop** ([`Run::judge_drop`]): reads of the account at
//!   the comparisons' cadence until X and Y are absent or `--agree-wait-ms` ends, then the trades
//!   since X's send and `GET /user/historic-orders`, and the verdict ([`crate::verdict`]), judged
//!   per account in memory and never carried to another account or run (decision 0148 item 2); then
//!   the read of every named account. A stop in drop session two ends it as one in drop session one
//!   does, before any drop, and a failure there that is no stop gains the stop the reads after it
//!   show ([`after_stop`]), or a stop for an order that may rest when X's account was not read; a
//!   read before the drop that was not made, or that showed what the streams did not, is a stop
//!   before the drop; a failed verdict, an arm-time cancel QFEX did not report, a fill of X or
//!   Y the reads show (the run stopping not flat, the position the owner's, 0151 item 9) or an
//!   order still resting after it is a stop, marked, so nothing more is placed on that account or
//!   any other. With no stop the run ends after the last account's drop test, naming FBC-su9i,
//!   which builds stage two ([`NOT_BUILT`]).
//! - **The files** ([`crate::report`] for the report and the evidence, [`crate::capture`] for the
//!   journal and the scrubbed fixture slice), which scripts/privacy-check.sh must pass. A record
//!   the journal dropped, marked in it or only counted by its sink, one its writer never wrote, and
//!   a journal that cannot be read to its end each make the run incomplete and the capture of every
//!   session that could have lost a record partial (decision 0006).

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
    Namespace, NonceBlock, NonceSource, OpKind, RateCharge, RawFrame, SpecTable, SubmitOutcome,
    TrafficClass, VenueCaps, VenueConfig, VenueFactory, Via, WallNs, dispatch,
};
use fbc_journal::{
    JournalError, JournalWriter, NonceSourceId, QueueSink, RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::OrdState;
use fbc_runtime::ws::{Message, WebSocket};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecControl, ExecHandler, ExecSession, ExecSessionConfig,
    ExecSessionError, InFlight, IngestClock, Journal, ProxyConfig, RateLimiter, ReconnectPacing,
    ReservedBound, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::account_reads::{
    self, NotAPage, OrdersPage, PageAsk, RestRead, SocketErr, SocketWrite,
};
use fbc_venue_qfex::auth::{ACCOUNT_KEY, AccountSelection, ReadAuth, TRADE_URL, account_selection};
use fbc_venue_qfex::exec::{AUTH_DEADLINE, CHANNELS};
use fbc_venue_qfex::probe::{
    self, Environment, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_PRODUCTION_PLACES, KEY_RESYNC_TIMEOUT,
    KEY_RPC_TIMEOUT, Origins, PROBE_PLACE_STAGES,
};
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_URL};
use futures_util::{FutureExt, StreamExt};

use crate::auth_qfex::{self, Armed, Lookup, SUB_ID_VAR};
use crate::capture::{self, AccountRead, AllowedOrder, Evidence, SessionRun};
use crate::compare::Reader as _;
use crate::evidence;
use crate::hold::{self as held_frames, Control, End, Heard, HoldRun, ReadAfter};
use crate::interrupt::Presses;
use crate::ledger::{self, Opened, PlaceBounds, PlaceLists, PlaceRegistry, RegistryLedger, Step};
use crate::link::{Link, LinkHandler};
use crate::market::{self, Discovered, PlaceCaps, Preflight};
use crate::places::{
    self, Allowance, Cancels, DropRun, PlaceHandler, RunLeases, Stop, Traded, TwoRun, Watched,
};
use crate::probe_args::{self as args, Hold, Options, Places, RESYNC_TIMEOUT};
use crate::refusals::{
    self, AFTER_DROP_ONE, AFTER_DROP_TWO, AFTER_HOLD, HOLD, ReadSocket, Refusals,
};
use crate::tee::{Tap, Tee};

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The owner's interrupt, as the liveness hold waits on it: it resolves when the owner presses
/// Ctrl-C (`main.rs` wires tokio's `ctrl_c` through [`crate::interrupt`]; a test resolves its
/// own). Only the hold's connection polls it: once as the connection opens, before anything is
/// written, then while it holds, and once more when its close is over, before it drops it
/// (decision 0146 item 4).
pub type Interrupt = Pin<Box<dyn Future<Output = ()>>>;

/// What a pong, a close reply, the hold's own close or a read socket's close is charged: a
/// control frame, which safety traffic may send from the reserve, as a session charges one
/// (fbc-runtime's session core, decision 0030).
pub const CONTROL: fbc_runtime::Request = fbc_runtime::Request {
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
pub const DAY: Duration = Duration::from_secs(86_400);

/// The connection an HTTP request or a read socket's frame is charged under: QFEX's buckets are
/// per user, and these are on no session's connection.
pub const NO_CONN: ConnKey = ConnKey {
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
    run_with(opts, None, lookup, out, interrupt).await
}

/// Runs a place-stage run as [`run_places_until`] does, with no interrupt: the integration
/// test's entry.
#[allow(dead_code)]
pub async fn run_places(
    opts: &Options,
    places: &Places,
    lookup: Lookup<'_>,
    out: Out,
) -> Result<Outcome, String> {
    let pending = Box::pin(std::future::pending());
    run_places_until(opts, places, lookup, out, pending, Presses::none()).await
}

/// Runs a place-stage run (`--place-stages`, FBC-c6kw): the `CAPS` line, the guard, the
/// loopback-only gate and the span bound's floor, the lease directory ([`RunLeases::open`]:
/// its tag, the stop markers and `--reconciled`, the namespace lease and the mint), the hold
/// when `opts.hold` asks for one (ended early by `interrupt`), then the place stages' preflight
/// ([`crate::market`]) and, with `--reconciled`, the reads of every named account that clear
/// the stop marker, then the files. It places nothing: it stops after them, its outcome not ok,
/// since the place stages after them are not built yet. A press of `presses` during the
/// preflight or the reads stops it, [`Stop::Interrupted`], its report and its stop marker
/// written (decision 0148 item 8, 0151 item 6).
pub async fn run_places_until(
    opts: &Options,
    places: &Places,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
    presses: Presses,
) -> Result<Outcome, String> {
    run_with(opts, Some((places, presses)), lookup, out, interrupt).await
}

/// The refusal of a place-stage run on an origin that is not loopback (FBC-c6kw), until FBC-4plj
/// lifts it.
pub const LOOPBACK_ONLY: &str = "--place-stages: refused, nothing sent: until FBC-4plj lands \
     every stop decisions 0132 item 7 and 0148 item 8 require, the place stages run only against \
     a loopback test stub, every origin (trade, REST and MDS) a loopback address";

/// Why a place-stage run ends after every named account's drop test, when no stop came first
/// (FBC-77r8): stage two is not built yet.
pub const NOT_BUILT: &str = "stage two of the place stages (decision 0132 item 7: the crossing \
     post-only and the taker IOC) and the stages after it are not built yet (FBC-su9i builds \
     stage two): the run ends after every named account's drop test, its outcome not ok";

/// Why a place-stage run that holds stops when the owner's Ctrl-C comes before the hold's
/// connection opens, during the reads before it or its connect: it is the hold's interrupt, so
/// no preflight starts and no stop marker is written (decision 0146 item 4).
pub const INTERRUPTED_BEFORE_HOLD: &str = "the owner's interrupt (Ctrl-C) came before the \
     hold's connection opened: it never opened, and no preflight starts";

/// Why a place-stage run that holds stops when the owner's Ctrl-C comes during the reads after
/// the hold, which run to their end first (decision 0146 items 3 and 4): no preflight starts,
/// and no stop marker is written.
pub const INTERRUPTED_AFTER_HOLD: &str = "the owner's interrupt (Ctrl-C) came during the reads \
     after the hold: no preflight starts";

/// [`run_until`] or, with `places` and the presses it takes, [`run_places_until`].
async fn run_with(
    opts: &Options,
    places: Option<(&Places, Presses)>,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
) -> Result<Outcome, String> {
    let (places, presses) = match places {
        Some((places, presses)) => (Some(places), presses),
        None => (None, Presses::none()),
    };
    // The caps, first, before anything else, and so before any request (decisions 0113 items 1
    // and 2, 0148 item 7): on standard output alone, never in the report or the evidence
    // (0151 item 10).
    if let Some(places) = places {
        let _ = writeln!(
            out.borrow_mut(),
            "CAPS on QFEX {}: --resting-cap-usd {} USD, --inventory-cap-usd {} USD: the \
             operator's own (decision 0113), printed before any request and written to neither \
             the report nor the evidence",
            environment(opts.environment()),
            places.resting_cap_usd,
            places.inventory_cap_usd
        );
    }
    let named = named(lookup);
    // The accounts a session arms on, then the one the hold authenticates as, then, for the
    // place stages, every account the configuration names, each of which they run on in turn.
    let mut used = opts.arms.clone();
    used.extend(opts.hold.map(|hold| hold.account));
    used.extend(places.iter().flat_map(|_| named.iter().copied()));
    let first = *(used.first()).ok_or("--arm or --hold-secs is required: nothing to run")?;
    auth_qfex::check(lookup, &used)?;
    // Every account a session arms on or the hold authenticates as is among them: `check`
    // refuses the subaccount when no variable names it.
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
    if let Some(places) = places {
        // The guard admits one side for all three origins, so loopback here is every one.
        if origins != Origins::Loopback {
            return Err(LOOPBACK_ONLY.to_owned());
        }
        args::check_place_span(opts, places, named.len())?;
    }
    // The lease directory, before anything connects or the evidence directory is made: its tag,
    // the stop markers and --reconciled, then the namespace lease for the run (0151 items 1 and
    // 6).
    let leases = match places {
        Some(places) => Some(RunLeases::open(places, wall_ns(), opts.read_wait)?),
        None => None,
    };
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
    // Read on its way through by the tee, whose stops the place stages wait on (FBC-23ch), and
    // kept to read the queue's drop counts once the writer has closed.
    let tee = Tee::new(sink);
    // A test's own journal past its soft limit (decision 0139), which the program never asks for.
    #[cfg(test)]
    let tee = match places.is_some_and(|p| p.test.refuse_normal) {
        true => tee.refusing_normal(),
        false => tee,
    };
    let sink = Rc::new(RefCell::new(tee));
    let tee = sink.borrow().tap();
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
            places_asked: places.is_some(),
            marker: leases.as_ref().map(|l| l.marker.clone()),
            ..Evidence::default()
        },
        named,
        places,
        presses,
        leases,
        stop: None,
        tee,
        rpc_ids: RpcIds::default(),
        specs: None,
        patient: None,
        rest_refused: 0,
        unread: Vec::new(),
    };
    let held = opts.hold.map_or(String::new(), |hold| {
        format!(
            "; first holds one trade connection on {} for {} s",
            name(hold.account),
            hold.duration.as_secs()
        )
    });
    match places {
        None => run.line(format_args!(
            "PROBE stage one on QFEX {} ({}), against {target}: arms on {:?}{held}",
            environment(env),
            env.mode(),
            opts.arms
        )),
        Some(_) => run.line(format_args!(
            "PROBE place stages on QFEX {} ({}), against {target}: the preflight, then each \
             named account's drop test in turn (FBC-23ch and FBC-77r8): drop session one places \
             X, one post-only GTC at the minimum size behind the touch, and closes gracefully; \
             drop session two places Y and drops abruptly; the reads after the drop judge the \
             account's verdict; the run ends after the last account's, not ok, until FBC-su9i \
             builds stage two{held}",
            environment(env),
            env.mode(),
        )),
    }
    // A marker found is held to --reconciled as the lease directory opened, so both are here.
    if let Some(marker) = &run.ev.marker
        && let (false, Some(declared)) = (marker.found.is_empty(), marker.reconciled)
    {
        let found = places::stops_said(&marker.found);
        run.line(format_args!(
            "MARKER a stop marker from an earlier place-stage run ({found}): \
             --reconciled {declared}, the owner's declaration, which the probe does not check; \
             every named account is read after the preflight, and the marker is cleared only if \
             every read is clean (0151 item 6)"
        ));
    }
    if let Err(why) = run.stage().await {
        run.line(format_args!("STOPPED {why}"));
        run.ev.stopped = Some(why);
    }
    run.mark();
    let Run {
        journal,
        mut ev,
        out,
        specs,
        ..
    } = run;
    // Every session has dropped its handle on the journal; this one goes too, and the writer
    // thread writes what is queued, the marker of a gap still open, and closes its segment.
    // Then the sink's drop counts, every class's, which nothing offers to now: a drop the
    // journal holds no marker for counts as much as one it does (decision 0006).
    drop(journal);
    let closed = writing.close();
    journal_closed(&mut ev, closed, sink.borrow().inner());
    drop(sink);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let failed = match (places, &specs) {
        (Some(_), specs) => {
            let specs = specs.clone().unwrap_or_else(SpecTable::new);
            write_place_slice(dir, &mut ev, &specs, &script).err()
        }
        (None, _) => write_slice(dir, &mut ev, &script).err(),
    };
    ev.errors.extend(failed);
    let (report, json) = crate::report::outputs(&mut ev);
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
pub fn base_config(opts: &Options) -> VenueConfig {
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

/// The configuration of a place stage's session on `armed` (decision 0148 item 4): `armed`'s
/// session configuration ([`configured`]) with `qfex.exec.production_places` set to
/// `probe_place_stages`, under which the production probe codec encodes a post-only GTC or a
/// limit IOC at the instrument's minimum size and refuses every other place. Only a place
/// stage's sessions take it: [`base_config`] never holds the key, so no stage-one session and no
/// read has it.
#[allow(dead_code)] // the place stages' driver opens its sessions on it (FBC-23ch)
pub fn place_config(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, fbc_core::Secrets), String> {
    let (mut cfg, creds) = configured(base, lookup, armed)?;
    cfg.insert(KEY_PRODUCTION_PLACES, PROBE_PLACE_STAGES);
    Ok((cfg, creds))
}

/// The configuration of the session arming on `armed`, and its credentials: `base` with the
/// account and public key the environment gives it (`auth_qfex::session`) and the account key
/// the program numbers it by (decision 0122 item 1).
pub fn configured(
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
    checked_fixtures(&dir.join("fixtures"), ev, script, |fixtures, ev| {
        capture::write_fixtures(fixtures, files, ev)
    })
}

/// The directory a place-stage run writes its fixture slice into (0151 item 7), to commit as
/// `fixtures/qfex/exec-recorded/drop-production/` once the owner has read every file.
pub const DROP_SLICE: &str = "drop-production";

/// A place-stage run's [`write_slice`]: the slice scrubbed by `capture::Scrub::Places`
/// (decision 0148 item 10), each resync page read on `specs`, the spec table its sessions held,
/// written into `dir/drop-production/` with its README, then checked by `script`.
pub fn write_place_slice(
    dir: &Path,
    ev: &mut Evidence,
    specs: &SpecTable,
    script: &Path,
) -> Result<(), String> {
    let files = capture::slice_places(&dir.join("journal"), SHARD, ev, specs)?;
    checked_fixtures(&dir.join(DROP_SLICE), ev, script, |fixtures, ev| {
        capture::write_fixtures_scrubbed(fixtures, files, ev, capture::Scrub::Places)
    })
}

/// Writes `fixtures` with `write`, then runs the privacy check `script` over it: fixtures it
/// refuses are removed, never left to copy.
fn checked_fixtures(
    fixtures: &Path,
    ev: &mut Evidence,
    script: &Path,
    write: impl FnOnce(&Path, &mut Evidence) -> Result<(), String>,
) -> Result<(), String> {
    write(fixtures, ev)?;
    let checked = evidence::privacy_check(script, fixtures);
    if checked.is_err() {
        let _ = fs::remove_dir_all(fixtures);
        ev.fixtures.clear();
    }
    checked.map_err(|e| format!("{e}; the fixtures directory was removed"))
}

/// What a never-arming read socket of an account is made of ([`Run::socket_parts`]): its
/// configuration, which the guard admitted, its reader, and the context of each signed frame or
/// request.
struct SocketParts {
    cfg: VenueConfig,
    reader: ReadAuth,
    ctx: Box<dyn FnMut() -> EncodeCtx>,
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
    /// What a place-stage run asks for (`--place-stages`): `None` for stage one.
    places: Option<&'a Places>,
    /// The owner's presses a place-stage run takes outside the hold ([`Presses`]): none for
    /// stage one.
    presses: Presses,
    /// A place-stage run's lease directory, namespace lease and mint ([`RunLeases`]).
    leases: Option<RunLeases>,
    /// The place stages' stop, once one stopped the run ([`Run::halt`]).
    stop: Option<Stop>,
    /// What the journal's tee keeps of the sessions' frames, and the stops it raises
    /// ([`crate::tee`]).
    tee: Tap,
    /// The request ids every session of the run shares, so no two requests of the run share an
    /// id in its one journal.
    rpc_ids: RpcIds,
    /// The spec table a place-stage run's sessions hold, once its preflight discovered it: the
    /// slice reads their pages on it.
    specs: Option<SpecTable>,
    /// Until when a read waits for the shared limiter's room ([`Run::admitted`]): set for the
    /// reads after a place stage's session alone.
    patient: Option<tokio::time::Instant>,
    /// The account reads' `GET /user/positions` answered HTTP 429 or 418 so far: provider rate
    /// refusals of REST, which the shared limiter does not count ([`Run::read_account`]).
    rest_refused: u64,
    /// The accounts whose reads could not be made so far, one entry per read ([`Run::read_one`]),
    /// which the evidence's reads do not hold.
    unread: Vec<Armed>,
}

impl Run<'_> {
    fn line(&self, args: fmt::Arguments<'_>) {
        let _ = writeln!(self.out.borrow_mut(), "{args}");
    }

    /// Records `stop`, met `when`, as the run's: the first stop is the run's, by name in the
    /// evidence. Returns what the run stopped with, its name and what it is, never an id or an
    /// amount.
    fn halt(&mut self, stop: Stop, when: &str) -> String {
        let stop = *self.stop.get_or_insert(stop);
        self.ev.stop = Some(stop.name());
        format!(
            "{} {when}: {}; no further session runs",
            stop.name(),
            stop.says()
        )
    }

    /// Writes the stop marker for the run's stop, when it marks one (0151 item 6), in its lease
    /// directory, keeping the stops of the marker it found there unless it cleared it, and
    /// records what it wrote, or why it could not, in the evidence.
    fn mark(&mut self) {
        let (Some(stop), Some(leases), Some(marker)) =
            (self.stop, &self.leases, self.ev.marker.as_mut())
        else {
            return;
        };
        let earlier = match marker.cleared {
            true => &[][..],
            false => &leases.carried[..],
        };
        match places::write_marker(&leases.dir, stop, earlier, &self.ev.started) {
            // A stop that is not a fault writes none.
            Ok(written) => {
                marker.written = written.then_some(stop.name());
                if written {
                    let _ = writeln!(
                        self.out.borrow_mut(),
                        "MARKER written ({}): the next place-stage run needs --reconciled (0151 \
                         item 6)",
                        stop.name()
                    );
                }
            }
            Err(e) => {
                marker.error = Some(e.clone());
                self.ev.errors.push(e);
            }
        }
    }

    /// The hold, when the run asks for one, then each session, each after the checks of every
    /// account the configuration names; a place-stage run's preflight and drop tests.
    async fn stage(&mut self) -> Result<(), String> {
        self.line(format_args!(
            "ACCOUNTS {} named by the run's configuration, not listed by QFEX (decision 0144): {}",
            self.ev.accounts.len(),
            self.ev.accounts.join(", ")
        ));
        if let Some(hold) = self.opts.hold {
            self.hold(hold).await?;
        }
        if let Some(places) = self.places {
            // After the hold, so the MDS touch is not open while it holds (decision 0146). A
            // press made before it, outside the hold, is taken as it starts.
            let mut pre = Preflight::default();
            let mut presses = std::mem::take(&mut self.presses);
            let passed = unless_pressed(&mut presses, self.preflight(places, &mut pre)).await;
            self.presses = presses;
            self.ev.preflight = Some(pre);
            let market = match passed {
                Some(passed) => passed?,
                None => return Err(self.halt(Stop::Interrupted, "during the preflight")),
            };
            let read = self.reconcile().await?;
            return self.drop_tests(places, market, read).await;
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

    /// With `--reconciled` and a stop marker found (0151 item 6), reads every named account, as
    /// before drop session one ([`places::DROP_ONE`]), and clears the marker only if every read
    /// is clean; a read that is not clean stops the run with the marker kept, and a provider rate
    /// refusal they met is the run's stop, marked before the stops kept ([`Run::unless_refused`]).
    /// Without them it
    /// reads nothing. Whether it read every named account, clean, so drop session one needs no
    /// reads of its own before it.
    async fn reconcile(&mut self) -> Result<bool, String> {
        let Some(leases) = self.leases.as_ref().filter(|l| l.reconciling()) else {
            return Ok(false);
        };
        let marked = leases.marked.clone();
        let mark = self.read_mark();
        let mut presses = std::mem::take(&mut self.presses);
        let on = self.named[0];
        let reads = self.check_accounts(places::DROP_ONE, on, " (drop)");
        let read = unless_pressed(&mut presses, reads).await;
        self.presses = presses;
        // A provider rate refusal they met is the run's stop, the marker kept with it.
        let when = "in the reads that clear the marker";
        let read = read.map(|reads| self.unless_refused(mark, reads, when));
        match read {
            None => Err(self.halt(Stop::Interrupted, "during the reads that clear the marker")),
            Some(Err(why)) => Err(format!("{why}; the stop marker is kept (0151 item 6)")),
            Some(Ok(())) => {
                let cleared = places::clear_markers(&marked);
                let marker = self.ev.marker.get_or_insert_with(Default::default);
                match cleared {
                    Ok(()) => {
                        marker.cleared = true;
                        self.line(format_args!(
                            "MARKER cleared: every named account read clean (0151 item 6)"
                        ));
                        Ok(true)
                    }
                    Err(e) => {
                        marker.error = Some(e.clone());
                        Err(e)
                    }
                }
            }
        }
    }

    /// The place stages' preflight ([`crate::market`]), each step it passes recorded in `pre`:
    /// the spec from this run's `GET /refdata` on the configuration the guard admitted, the count
    /// bound against each place registry's ceiling, the MDS touch, and the caps at it. No account
    /// connects, and nothing authenticated opens. What it gives the drop session: the spec, the
    /// registries' bounds, the caps in lots and the touch, still open.
    async fn preflight(&mut self, places: &Places, pre: &mut Preflight) -> Result<Market, String> {
        let (cfg, _) = configured(&self.base, self.lookup, self.named[0])?;
        let cfg = market::market_config(&cfg, self.opts.read_wait);
        self.line(format_args!(
            "PREFLIGHT the instrument's spec from GET /refdata on the guarded REST origin"
        ));
        let found = market::discover_spec(&cfg, &places.symbol, &self.connector, &self.limiter);
        let found = found.await?;
        pre.passed.push(market::DISCOVERED);
        let parsed =
            (self.opts.ledger.as_ref()).ok_or("--place-stages without the ledger's bounds")?;
        let bounds = market::count_bound(parsed, &found, places)?;
        pre.passed.push(market::COUNTED);
        self.line(format_args!(
            "PREFLIGHT the touch from the guarded MDS origin"
        ));
        let opened = market::Touch::open(
            &cfg,
            &found,
            &self.connector,
            &self.clock,
            self.opts.connect_wait,
        );
        let within = self.opts.connect_wait + self.opts.read_wait;
        let mut touch = opened?;
        let quote = touch.first(within, places.touch_max_age).await?;
        pre.passed.push(market::TOUCHED);
        let caps = market::place_caps(places, &found, quote)?;
        pre.passed.push(market::CAPPED);
        self.line(format_args!(
            "PREFLIGHT passed: the spec, the count bound, the touch and the caps"
        ));
        self.specs = Some(found.specs.clone());
        Ok(Market {
            found,
            bounds,
            caps,
            touch,
        })
    }

    /// The client-id namespace reads decode ours by: a place-stage run's `--namespace`, stage
    /// one's own otherwise.
    fn ns(&self) -> Namespace {
        self.places.map_or(NS, |p| Namespace::new(p.namespace))
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
    /// have no room for it now ([`admit`]).
    fn admit(&self, charge: RateCharge, via: Via, class: TrafficClass) -> Result<(), String> {
        admit(&self.limiter, charge, via, class)
    }

    /// [`Run::admit`], but while [`Run::patient`] is set, a request the buckets have no room
    /// for yet waits for the room, when it comes before then, and is charged then: the reads
    /// after a place stage's session, which must be made (decision 0132 item 7), wait out the
    /// hold a provider rate refusal put on the shared budget (decision 0126) rather than skip.
    async fn admitted(
        &self,
        charge: RateCharge,
        via: Via,
        class: TrafficClass,
    ) -> Result<(), String> {
        loop {
            let request = fbc_runtime::Request { charge, via, class };
            let now = tokio::time::Instant::now();
            let refused = match self.limiter.charge(now, NO_CONN, &[request]) {
                Ok(_) => return Ok(()),
                Err(refused) => refused,
            };
            match (refused.ready_at, self.patient) {
                (Some(ready), Some(until)) if ready <= until => {
                    tokio::time::sleep_until(ready).await;
                }
                _ => return Err("not sent: the rate limit has no room".to_owned()),
            }
        }
    }

    async fn rest(&self, read: &RestRead) -> Result<(u16, Vec<u8>), String> {
        self.admitted(read.charge, Via::Http, read.class).await?;
        auth_qfex::send(&self.connector, &read.req, self.opts.read_wait).await
    }

    /// Closes a read socket, its close frame charged to the shared limiter first as a control
    /// frame, as a session charges its own (decision 0030), and as every frame of the read is
    /// ([`Run::admitted`]); with no room, under a provider hold say (decision 0126), it is
    /// dropped with no close frame, as the hold's and a session's are. A close the peer refuses
    /// is passed over (it may have closed first); one it never takes within `--read-wait-ms` is
    /// an error, so the run cannot hang on it.
    async fn close_read(&self, mut ws: WebSocket) -> Result<(), String> {
        let admitted = self.admitted(CONTROL.charge, CONTROL.via, CONTROL.class);
        if admitted.await.is_err() {
            return Ok(());
        }
        let wait = self.opts.read_wait;
        let closed = tokio::time::timeout(wait, ws.close(None)).await;
        closed.map(|_| ()).map_err(|_| {
            format!(
                "the read socket did not close within {} ms",
                wait.as_millis()
            )
        })
    }

    /// Writes one of the adapter's frames on a read socket or the hold's connection, waiting at
    /// most `--read-wait-ms` for a peer that stopped reading.
    async fn write(&self, ws: &mut WebSocket, write: &SocketWrite) -> Result<(), String> {
        self.admitted(write.charge, Via::Frame, write.class).await?;
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
        self.check_accounts_allowing(before, on, mark, None).await
    }

    /// Reads every account the configuration names as [`Run::check_accounts`] does, but admits
    /// `allowed`, when given, on its account alone: a page listing it and a REST count of it are
    /// clean there ([`AccountRead::refusal`]). Decision 0132 item 7's one exception, X before the
    /// drop test's second session (FBC-77r8, [`Run::read_before_two`]); every other read admits
    /// none.
    async fn check_accounts_allowing(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        allowed: Option<&AllowedOrder>,
    ) -> Result<(), String> {
        for account in self.named.clone() {
            self.check_account(before, on, mark, account, allowed)
                .await?;
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
            let checked = (self.check_account(AFTER_HOLD, on, " (held)", account, None)).await;
            failed.extend(checked.err());
        }
        match failed.is_empty() {
            true => Ok(()),
            false => Err(failed.join("; ")),
        }
    }

    /// Reads `account` before `before`, as [`Run::check_accounts_allowing`] reads each, admitting
    /// `allowed` when it is on `account`.
    async fn check_account(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        allowed: Option<&AllowedOrder>,
    ) -> Result<(), String> {
        let read = self
            .read_one(before, on, mark, account, (allowed, &[]))
            .await?;
        let label = &read.label;
        if let Some(why) = read.refusal() {
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

    /// Reads `account` before `before` (on `on`, its label marked `mark`), admitting `allowed`
    /// when it is on it, and records the read in the evidence and on a `READ` line: the read, or
    /// why it could not be made, `account` then kept among the reads not made
    /// ([`Run::unread`]). Its read socket's refusals are recorded either way.
    async fn read_one(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        admits: (Option<&AllowedOrder>, &[&AllowedOrder]),
    ) -> Result<AccountRead, String> {
        let read = self.reading_one(before, on, mark, account, admits).await;
        if read.is_err() {
            self.unread.push(account);
        }
        read
    }

    /// [`Run::read_one`]'s read.
    async fn reading_one(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        (allowed, known): (Option<&AllowedOrder>, &[&AllowedOrder]),
    ) -> Result<AccountRead, String> {
        let mut label = name(account).to_owned();
        label.push_str(if account == on { mark } else { "" });
        let (cfg, _) = configured(&self.base, self.lookup, account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let mut met = None;
        let admits = (allowed, account);
        let read = self
            .read_account(&cfg, selection, admits, known, &mut met)
            .await;
        // A read socket's refusals are reported whether or not its read succeeded.
        if let Some(refusals) = met {
            self.ev.read_sockets.push(ReadSocket {
                before,
                label: label.clone(),
                refusals,
            });
        }
        let read = read.map_err(|e| format!("{label} could not be read: {e}"))?;
        let shown = format!(
            "{} open orders, {} TWAPs, flat {}, REST open_orders {}",
            read.page_orders, read.page_twaps, read.flat, read.rest_open_orders
        );
        let when = refusals::read_when(before);
        self.line(format_args!("READ {when}: {label}: {shown}"));
        let read = AccountRead {
            before,
            label,
            ..read
        };
        self.ev.reads.push(read.clone());
        Ok(read)
    }

    /// One account's positions, and its open orders on a trade socket of its own, counting
    /// those that are `admits.0` when it is on `admits.1`, the account read
    /// ([`AccountRead::of`]), and those that are none of `known` on it
    /// ([`AccountRead::page_unknown`]); `met` is the provider rate refusals that socket met, once
    /// it opened.
    async fn read_account(
        &mut self,
        cfg: &VenueConfig,
        account: AccountSelection<'_>,
        admits: (Option<&AllowedOrder>, Armed),
        known: &[&AllowedOrder],
        met: &mut Option<Refusals>,
    ) -> Result<AccountRead, String> {
        let reader = auth_qfex::reader(self.lookup, account)?;
        let ctx = self.ctx();
        let read = account_reads::positions(cfg, &reader, &ctx).map_err(|e| e.to_string())?;
        let (status, body) = self.rest(&read).await?;
        if matches!(status, 429 | 418) {
            self.rest_refused += 1;
            return Err(format!(
                "GET /user/positions: HTTP {status}, a provider rate refusal"
            ));
        }
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        let positions = dispatch(&self.caps, self.ns(), |scope| {
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
        let closed = self.close_read(ws).await;
        let page = page?;
        closed?;
        let mut read = AccountRead::of(&positions, &page, admits.0, admits.1);
        read.page_unknown = AllowedOrder::unknown(known, admits.1, page.orders());
        Ok(read)
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
        let reply = next(ws, self.opts.read_wait, &self.limiter).await?;
        let _ = self.heard(heard, reader, &reply, ask);
        if !account_reads::authenticated(reader, reply.raw()) {
            return Err("QFEX did not authenticate the read socket".to_owned());
        }
        let ask_frame = account_reads::user_orders(cfg, ask).map_err(|e| e.to_string())?;
        self.write(ws, &ask_frame).await?;
        let reply = next(ws, self.opts.read_wait, &self.limiter).await?;
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
        let page = dispatch(&self.caps, self.ns(), |scope| {
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
    ///
    /// In a place-stage run the owner's Ctrl-C outside the hold's connection ([`Presses`]) is
    /// the hold's too, never a stop of the place stages, so it writes no stop marker: one during
    /// the reads before the hold or its connect keeps the connection from opening
    /// ([`INTERRUPTED_BEFORE_HOLD`]), and one during the reads after it, which run to their end
    /// first, starts no preflight ([`INTERRUPTED_AFTER_HOLD`]). `main.rs`'s runtime has one
    /// thread, so from the connection's opening, before anything awaits, each press is the
    /// connection's.
    async fn hold(&mut self, hold: Hold) -> Result<(), String> {
        let mut presses = std::mem::take(&mut self.presses);
        let opening = unless_pressed(&mut presses, self.open_hold(hold)).await;
        self.presses = presses;
        let (mut run, before, connected) = match opening {
            None => return Err(INTERRUPTED_BEFORE_HOLD.to_owned()),
            Some(opening) => opening?,
        };
        let label = run.account;
        let held = self.held(&mut run, connected).await;
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
        if why.is_none() && (!self.opts.arms.is_empty() || self.places.is_some()) {
            // What the interrupt stops: a place-stage run's preflight (FBC-c6kw), or the sessions.
            let next = match self.places {
                Some(_) => "no preflight starts",
                None => "no session starts",
            };
            why = match (&run.ended_by, run.interrupted_closing) {
                (End::Interrupted, _) => Some(format!(
                    "the owner's interrupt (Ctrl-C) ended the hold: {next}"
                )),
                (_, true) => Some(format!(
                    "the owner's interrupt (Ctrl-C) came as the hold closed: {next}"
                )),
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
            if self.presses.pressed().now_or_never().is_some() {
                why = Some(match why {
                    Some(why) => format!("{why}; {INTERRUPTED_AFTER_HOLD}"),
                    None => INTERRUPTED_AFTER_HOLD.to_owned(),
                });
            }
        }
        self.ev.hold = Some(run);
        why.map_or(Ok(()), Err)
    }

    /// The hold's beginning: every named account read as before a session, its `HOLD` line,
    /// its record, the limiter's count of refusals before its connection, and that connection
    /// opened ([`Run::connect_hold`]). A read that is not clean is returned; an error opening the
    /// connection is the hold's, in the connection's place.
    async fn open_hold(
        &mut self,
        hold: Hold,
    ) -> Result<(HoldRun, u64, Result<Connected, String>), String> {
        self.check_accounts(HOLD, hold.account, " (held)").await?;
        let label = name(hold.account);
        self.line(format_args!(
            "HOLD {label}: one trade connection for {} s: its authentication and three \
             subscriptions, then nothing; it never arms or places",
            hold.duration.as_secs()
        ));
        let run = HoldRun {
            account: label,
            duration: hold.duration,
            ..HoldRun::default()
        };
        let before = self.rejected();
        let connected = self.connect_hold(hold).await;
        Ok((run, before, connected))
    }

    /// The hold's connection opened on `hold.account`'s guarded trade origin, with what it
    /// writes on it.
    async fn connect_hold(&self, hold: Hold) -> Result<Connected, String> {
        let (cfg, _) = configured(&self.base, self.lookup, hold.account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let reader = auth_qfex::reader(self.lookup, selection)?;
        let url = account_reads::trade_url(&cfg, &reader).map_err(|e| e.to_string())?;
        let subscriptions = account_reads::subscriptions(&cfg).map_err(|e| e.to_string())?;
        let ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await;
        let ws = ws.map_err(of_the_hold)?;
        Ok(Connected {
            cfg,
            reader,
            subscriptions,
            ws,
        })
    }

    /// Charges the hold's own close to the run's limiter as a control frame, as a session
    /// charges its own: refused, nothing sent, when the buckets have no room for it.
    fn admit_close(&self) -> Result<(), String> {
        self.admit(CONTROL.charge, CONTROL.via, CONTROL.class)
    }

    /// The hold's connection, `connected` ([`Run::connect_hold`]), held until it ends
    /// ([`Run::holding`]), then closed: at its duration, the owner's interrupt or a
    /// refusal, a close and the server's echo read for at most `--read-wait-ms`; at a server
    /// close, read on until the close reply has gone out; an err read meanwhile is kept as its
    /// refusal, unless one already refused it. The owner's interrupt is the hold's
    /// from the connection's opening until then: a press that did not end it, made while it
    /// closed or ready as something else ended it, is recorded as one made as it closed
    /// (`run.interrupted_closing`). An error before it opened is returned; everything after is
    /// in `run`.
    async fn held(
        &mut self,
        run: &mut HoldRun,
        connected: Result<Connected, String>,
    ) -> Result<(), String> {
        let Connected {
            cfg,
            reader,
            subscriptions,
            mut ws,
        } = connected?;
        // The owner's interrupt, which only this connection waits on, and which goes with it.
        let mut interrupt =
            (self.interrupt.take()).unwrap_or_else(|| Box::pin(std::future::pending()));
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

/// The hold's connection, opened, and what it writes on it ([`Run::connect_hold`]).
struct Connected {
    cfg: VenueConfig,
    reader: ReadAuth,
    subscriptions: Vec<SocketWrite>,
    ws: WebSocket,
}

/// Charges one request, `charge` sent `via` as `class` traffic, to `limiter` under [`NO_CONN`], or
/// refuses it, nothing sent, when the buckets have no room for it now: every read socket's frame
/// and every REST read of the run, the comparisons' included (decision 0135 item 1).
pub fn admit(
    limiter: &RateLimiter,
    charge: RateCharge,
    via: Via,
    class: TrafficClass,
) -> Result<(), String> {
    let request = fbc_runtime::Request { charge, via, class };
    let charged = limiter.charge(tokio::time::Instant::now(), NO_CONN, &[request]);
    charged
        .map(|_| ())
        .map_err(|_| "not sent: the rate limit has no room".to_owned())
}

/// `work`'s output, or `None` when a press of `presses` came first: a press already made is
/// taken before `work` is polled, and `work` is dropped at the press.
async fn unless_pressed<T>(presses: &mut Presses, work: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        () = presses.pressed() => None,
        done = work => Some(done),
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
    pub fn raw(&self) -> RawFrame<'_> {
        match self {
            Data::Text(text) => RawFrame::Text(text),
            Data::Binary(bytes) => RawFrame::Binary(bytes),
        }
    }
}

/// The next data frame on a read socket within `wait`; pings and pongs are passed over. The pong
/// the WebSocket layer writes to each ping, which it answers while the socket is open both ways,
/// as it is while this reads, is recorded on `limiter` as a control frame, past its cap if need
/// be, as the hold's and a session's are (decision 0030).
pub async fn next(
    ws: &mut WebSocket,
    wait: Duration,
    limiter: &RateLimiter,
) -> Result<Data, String> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Data::Text(text.as_str().to_owned())),
                Some(Ok(Message::Binary(bytes))) => return Ok(Data::Binary(bytes.to_vec())),
                Some(Ok(Message::Ping(_))) => {
                    limiter.record(tokio::time::Instant::now(), NO_CONN, CONTROL);
                }
                Some(Ok(Message::Pong(_))) => {}
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

// ----- Drop session one (FBC-23ch; decisions 0132 items 4 and 7, 0148, 0151) -----

/// What the place stages' preflight gives the drop session ([`Run::preflight`]): the traded
/// instrument, the registries' bounds, the caps in lots and the MDS touch, kept open.
pub struct Market {
    found: Discovered,
    bounds: PlaceBounds,
    caps: PlaceCaps,
    touch: market::Touch,
}

/// What ends a drop session's steps before their course is run: a stop of the place stages, or
/// a failure that is no stop (the arm not accepted, a refused Start, a touch too old to price X
/// from), each said with no id and no value.
enum Halt {
    Stop(Stop),
    Error(String),
}

impl From<Stop> for Halt {
    fn from(stop: Stop) -> Halt {
        Halt::Stop(stop)
    }
}

/// Which session of a drop test (decision 0132 item 4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Which {
    One,
    Two,
}

impl Which {
    /// Its name: what its session and the reads before it are reported under.
    fn name(self) -> &'static str {
        match self {
            Which::One => places::DROP_ONE,
            Which::Two => places::DROP_TWO,
        }
    }

    /// What the reads after it are reported under.
    fn after(self) -> &'static str {
        match self {
            Which::One => AFTER_DROP_ONE,
            Which::Two => AFTER_DROP_TWO,
        }
    }

    /// The order it places: X, or Y.
    fn order(self) -> &'static str {
        match self {
            Which::One => "X",
            Which::Two => "Y",
        }
    }
}

/// One drop session's handles: its `Link`, what its handler saw, its orders, its account key,
/// its market and its registry's span bound, the account it runs on, which session it is, and
/// where its frames begin among those the tee heard and the `add_order`s it saw sent.
struct DropCtx {
    link: Rc<RefCell<Link>>,
    wake: Rc<tokio::sync::Notify>,
    watched: Rc<RefCell<Watched>>,
    orders: fbc_runtime::ExecOrders,
    acct: AccountKey,
    inst: fbc_core::InstrumentId,
    /// The registry's lot caps, fixed at the preflight's touch ([`market::caps_admit`]).
    caps: PlaceCaps,
    span_end: tokio::time::Instant,
    on: Armed,
    which: Which,
    heard_from: usize,
    sent_from: usize,
}

/// X or Y as its drop session placed it: its client id and when it was sent.
#[derive(Copy, Clone)]
struct Placed {
    cid: fbc_core::ClientOrderId,
    at: tokio::time::Instant,
    /// Its send on the wall clock, in whole seconds: the trades read after X start there.
    wall_secs: u64,
}

/// How long the reads after a place stage's session may wait for the shared limiter's room
/// ([`Run::admitted`]): longer than any hold QFEX's rate page shows a refusal asking for.
pub const AFTER_PATIENCE: Duration = Duration::from_secs(60);

/// How long a graceful close may take before the session's socket is dropped, as
/// `paradex_testnet_acceptance` waits for one.
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// What ends drop session one when it ran its course: a graceful close (decision 0132 item 4).
pub const GRACEFUL: &str = "a graceful close";
/// What ends drop session two when it ran its course: its connection dropped abruptly, with no
/// WebSocket close (decision 0132 item 4), by dropping the session's run.
pub const DROPPED: &str = "the abrupt drop";
/// What ends one whose connection ended first: the server's close, the codec's end or a
/// transport error ([`Stop::ConnectionEnded`]).
pub const CONNECTION_ENDED: &str = "the connection's end";
/// What ends one whose close the shared budget had no room for within [`AFTER_PATIENCE`]: the
/// runtime drops such a close frame unsent, and the connection with it, so the close may not
/// have been graceful, and whether it cancelled X is not measured (0151 item 11).
pub const NO_ROOM_TO_CLOSE: &str =
    "a close the shared budget had no room for, which may not have been graceful";

/// A place stage's session pacing (decision 0148 item 7): one connection attempt a day, each
/// within `connect_wait` (`--connect-wait-ms`), so a connection's end is never followed by
/// another.
pub fn place_pacing(connect_wait: Duration) -> ReconnectPacing {
    let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, connect_wait);
    pacing.expect("one attempt a day, each within --connect-wait-ms")
}

/// The stop and the error of a drop session whose session's run ended first, `ended`, given
/// `raised`, the stop its tee or handler raised: that stop, or, when none was raised (the run
/// ended before any epoch opened, so its handler heard no end), [`Stop::ConnectionEnded`], with
/// the run's error by its variant's name alone.
pub fn ended_first(
    raised: Option<Stop>,
    ended: Result<(), ExecSessionError>,
) -> (Option<Stop>, Option<String>) {
    let error = ended.err().map(|e| {
        format!(
            "the session ended before it ran its course: {}",
            places::variant_name(&e)
        )
    });
    match raised {
        Some(stop) => (Some(stop), error),
        None => (Some(Stop::ConnectionEnded), error),
    }
}

/// Whether the reads of `reads` were clean on every named account but `on`: `None` when `named`
/// names no other account (FBC-77r8: whether the other named accounts stayed clean through a
/// drop test).
pub fn others_clean(reads: &[AccountRead], named: &[Armed], on: Armed) -> Option<bool> {
    let others: Vec<&str> = (named.iter().filter(|a| **a != on))
        .map(|a| name(*a))
        .collect();
    if others.is_empty() {
        return None;
    }
    let theirs = reads.iter().filter(|r| others.contains(&r.label.as_str()));
    Some(theirs.into_iter().all(|r| r.refusal().is_none()))
}

impl Run<'_> {
    /// Each named account's drop test in turn ([`Run::drop_test`]; decision 0132 item 4, 0148
    /// items 1 and 2), each starting only once the one before ended with no stop, so no order
    /// of ours rests on another account while a session runs (decision 0132 items 4 and 7). The
    /// first account's reads before drop session one are skipped when `read` (the reads that
    /// cleared a stop marker just made them). The first stop ends the run, so a drop test that
    /// failed places nothing more on its account or any other; with none, the run ends after the
    /// last account's, [`NOT_BUILT`], naming FBC-su9i, which builds stage two.
    async fn drop_tests(
        &mut self,
        places: &Places,
        mut market: Market,
        read: bool,
    ) -> Result<(), String> {
        for (n, on) in self.named.clone().into_iter().enumerate() {
            self.drop_test(places, &mut market, on, read && n == 0)
                .await?;
        }
        Err(NOT_BUILT.to_owned())
    }

    /// The drop test on `on` (FBC-23ch, FBC-77r8; decision 0132 item 4), as [`Run::drop_sessions`]
    /// runs it, then, once it was built, what it says of the other named accounts' reads during
    /// it and the run's stop, if one came.
    async fn drop_test(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        read: bool,
    ) -> Result<(), String> {
        let (reads, made) = (self.ev.reads.len(), self.ev.drops.len());
        let unread = self.unread.len();
        let tested = self.drop_sessions(places, market, on, read).await;
        if self.ev.drops.len() > made {
            let clean = others_clean(&self.ev.reads[reads..], &self.named, on);
            // A read of another account that could not be made leaves its cleanness not known,
            // since the evidence's reads hold only those made (FBC-77r8's review).
            let missed = self.unread[unread..].iter().any(|a| *a != on);
            let others = places::OthersClean::of(clean, missed);
            let stop = self.stop.map(Stop::name);
            let run = self.drop_run();
            run.others_clean = others;
            run.stop = stop;
        }
        tested
    }

    /// The drop test's sessions on `on`, after the preflight gave `market` and, unless `read`
    /// (the reads that cleared a stop marker), every named account read clean: the drop registry
    /// built and its trip given to the journal before any market arms (decision 0139), seeded
    /// flat (0148 item 3), then drop session one ([`Run::drop_session`]), the after-session read
    /// of every named account, and X and its trades read (FBC-be57). Then, with no stop, the read
    /// before drop session two ([`Run::read_before_two`]), which admits X alone, and drop session
    /// two on the same `Link` and registry with no second seed, ended by the abrupt drop; the
    /// reads after it ([`Run::after_drop`]), X's and Y's trades and historic-orders rows, the
    /// verdict ([`crate::verdict::DropVerdict`]), and the read of every named account after it.
    /// From the first arm to the last read nothing cancels X or Y but Ctrl-C or a fill of ours,
    /// nothing reconnects, and no later arm is sent (decision 0132 item 4, 0151 item 2).
    ///
    /// Each way it ends the run is a stop, marked, the first that applies. After drop session
    /// one: the session's stop; a provider rate refusal in the reads after it, then one reading
    /// X; the owner's Ctrl-C during them; a position not flat or a trade naming X (0151 item 9);
    /// a trade naming another order or none ([`Stop::DropFailed`], decision 0132 item 4); X's
    /// trades or historic orders not read whole ([`Stop::LeftResting`]); then a failure of
    /// the session that raised no stop, with [`Stop::ReadDisagrees`] marked when an order none
    /// of ours or a TWAP shows, else [`Stop::LeftResting`] when an order of ours rests or X was
    /// placed on an account that could not be read; then a stop of the read before drop session
    /// two. After drop session two: the session's stop; a failure that raised no stop, with the
    /// stop the reads after it show ([`after_stop`]: a provider rate refusal, Ctrl-C, a position
    /// not flat or a trade naming X or Y, an order none of ours or a TWAP, an order of ours
    /// still resting), or else [`Stop::LeftResting`] when X's account was not read, so an order
    /// that may rest is marked; the verdict's stop (0151 items 9 and 13), unless the reads' stop
    /// outranks it ([`outranks`]: a position not flat, or an order none of ours or a TWAP, over
    /// a stop after it in the verdict's own order; FBC-77r8's review); then the reads' stop.
    /// With none it ends `Ok`, the next account's drop test next.
    async fn drop_sessions(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        read: bool,
    ) -> Result<(), String> {
        let label = name(on);
        if !read {
            let mark = self.read_mark();
            let mut presses = std::mem::take(&mut self.presses);
            let reads = self.check_accounts(places::DROP_ONE, on, " (drop)");
            let reads = unless_pressed(&mut presses, reads).await;
            self.presses = presses;
            match reads {
                None => {
                    let when = "during the reads before drop session one";
                    return Err(self.halt(Stop::Interrupted, when));
                }
                Some(reads) => {
                    let when = "in the reads before drop session one";
                    self.unless_refused(mark, reads, when)?;
                }
            }
        }
        let (cfg, creds) = place_config(&self.base, self.lookup, on)?;
        // Drop session two's configuration, read before anything is placed, so no failure to
        // read it can come between the sessions while X rests.
        let two_config = place_config(&self.base, self.lookup, on)?;
        let acct = account_key(on);
        let order = (self.caps.exec.as_ref()).map(|e| e.order.clone());
        let order = order.ok_or("the probe's caps declare no order entry")?;
        let inst = market.found.inst;
        let symbol = market.found.spec.venue_symbol.clone();
        let keys = places::lease_keys(label, &order, inst, symbol);
        let opened = Opened::now(self.origin, wall_ns());
        let caps = market::pre_trade_caps(inst, market.caps);
        let built = PlaceRegistry::new(
            Step::Drop,
            label,
            acct,
            &market.bounds,
            opened,
            inst,
            caps,
            keys,
        );
        // Its trip, before any market of it arms (decision 0139).
        built.trip_on(&self.journal);
        let rpc = Duration::from_millis(self.opts.rpc_timeout_ms);
        let ladder = fbc_oms::LadderConfig::new(rpc, Duration::ZERO, rpc * 6, 2);
        let ladder = ladder.map_err(|e| format!("the ladder: {}", places::variant_name(&e)))?;
        let (link, mut placed) = built.into_link(order, ladder);
        let link = Rc::new(RefCell::new(link));
        // The flat seed, after clean reads of every named account (decision 0148 item 3): once,
        // for both of the drop test's sessions.
        let seeded = link
            .borrow_mut()
            .reg()
            .seed_position(inst, fbc_core::SignedLots(0));
        seeded.map_err(|e| format!("the flat seed was refused: {}", places::variant_name(&e)))?;
        self.line(format_args!(
            "REGISTRY drop on {label}: {} on its one market, its CancelOnlyTrip given to the \
             journal before any market arms (decision 0139), then seeded flat after clean reads \
             of every named account (decision 0148 item 3)",
            ledger::PLACES_DECLARATION
        ));
        let lists = PlaceLists::new(market.found.min_size, places.flatten_attempts)?;
        let mut allowance = Allowance::new(&lists);
        // A test's X spent before the session, so its place is beyond the list.
        #[cfg(test)]
        if places.test.x_spent {
            let _ = allowance.take(Step::Drop, ledger::X);
        }
        self.ev.drops.push(DropRun {
            account: label,
            ..DropRun::default()
        });
        let span_end = placed.deadline();
        let ctx = (cfg, creds, Rc::clone(&link), span_end);
        let ran = self
            .drop_session(places, market, on, ctx, &mut allowance, None)
            .await;
        placed.session_ended(&link.borrow(), places::DROP_ONE, self.mono());
        self.ev.place_registries.push(placed);
        let (stop, x) = match ran {
            Ok(ran) => ran,
            Err(why) => {
                self.drop_run().error = Some(why.clone());
                (None, None)
            }
        };
        let said = stop.map(|stop| self.halt(stop, "in drop session one"));
        let vid_of = |cid| (link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        let x_allowed = x.map(|x| AllowedOrder {
            account: on,
            cid: x.cid,
            venue: vid_of(x.cid),
        });
        // The reads after the session wait out a hold the shared budget is under (decision
        // 0126), for at most AFTER_PATIENCE.
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let ours: Vec<&AllowedOrder> = x_allowed.iter().collect();
        let after = self.read_after(Which::One, on, &ours).await;
        let (mut x_refused, mut x_pressed, mut x_unread) = (None, false, false);
        if let (Some(x), Some(allowed)) = (x, &x_allowed) {
            let read = self
                .read_orders(Which::One, on, x.wall_secs, &[allowed])
                .await;
            self.drop_run().after = Some(read.0.x_after());
            x_unread = !(read.0.trades_whole && read.0.historic_whole);
            (x_refused, x_pressed) = (read.1, read.2);
        }
        self.patient = None;
        // Whether the close cancelled X, measured on a read of X's own account alone (another
        // account's says nothing of X) and, for X absent, on that account flat and X's trades
        // read whole with no row since its send (decision 0132 item 4).
        let run = self.drop_run();
        let read = places::XAccountRead {
            read: after.x_read,
            listed: after.x_listed,
            flat: after.x_flat,
        };
        let verdict = places::close_verdict(run, read);
        (run.close_cancelled_x, run.close_not_measured) = (verdict.ok(), verdict.err());
        run.x_resting = after.x_listed;
        run.x_read = after.x_read;
        // The run's end, the first that applies: the session's stop; a provider rate refusal in
        // the reads after it, then one reading X; the owner's Ctrl-C during those reads; a
        // position not flat, or a trade naming X; a trade naming another order or none, which
        // fails the drop test already; X's trades or historic orders not read whole, X left
        // unjudged; then a failure of the session that was no stop, kept in the evidence as
        // the session's error whichever stop came after it, with any order of ours still resting
        // marked for the owner's hand cancel; then a stop of the read before drop session two. A
        // stop after the session is raised, and marked, whether or not the session failed (0151
        // item 6). X still resting then is the owner's to cancel by hand (decision 0132 item 7).
        let ended = 'ended: {
            if let Some(said) = said {
                break 'ended Err(said);
            }
            if let Some(stop) = after.refused {
                break 'ended Err(self.halt(stop, "in the reads after drop session one"));
            }
            if let Some(stop) = x_refused {
                break 'ended Err(self.halt(stop, "reading X after drop session one"));
            }
            if after.pressed || x_pressed {
                let when = "during the reads after drop session one";
                break 'ended Err(self.halt(Stop::Interrupted, when));
            }
            // A trade naming X, seen with no session open, is a fill of ours: not flat, the
            // position the owner's, whatever the position read before it showed (0151 item 9),
            // so drop session two is not run.
            let x_traded = (self.drop_run().after.as_ref()).is_some_and(|a| a.trades_naming_x > 0);
            if after.not_flat || x_traded {
                break 'ended Err(self.halt(Stop::NotFlat, "after drop session one"));
            }
            // A row since X's send naming another order or none, on an account still flat:
            // something else traded on the account, which 0132 item 4 rules out from the first
            // arm to the read after the drop, so the drop test has failed already and Y is not
            // placed. The session is over, so nothing is cancelled (0151 item 2).
            let other =
                (self.drop_run().after.as_ref()).is_some_and(|a| a.trades > a.trades_naming_x);
            if other {
                let when = "after drop session one, a get_user_trades row since X's send naming \
                            another order or none (decision 0132 item 4)";
                break 'ended Err(self.halt(Stop::DropFailed, when));
            }
            // A read of X's trades or historic orders that failed with no refusal leaves X
            // unjudged, as an account that could not be read does before drop session two: Y is
            // not placed beside an X whose fills were never read.
            if x_unread {
                let when = "reading X after drop session one";
                break 'ended Err(self.halt(Stop::LeftResting, when));
            }
            // A failure of the session that raised no stop, after every stop the reads showed:
            // an order none of ours or a TWAP is what the streams did not show (decision 0148
            // item 8), and otherwise an order of ours still resting, or X placed on an account
            // that could not be read, so X may rest unjudged (as the read before drop session two
            // marks one), is marked for the owner's hand cancel (0151 item 6).
            if let Some(why) = self.drop_run().error.clone() {
                let unread = x.is_some() && !after.x_read;
                if after.unexpected {
                    self.halt(Stop::ReadDisagrees, "after drop session one");
                } else if after.resting || unread {
                    self.halt(Stop::LeftResting, "after drop session one");
                }
                break 'ended Err(why);
            }
            // X, placed and ended with no stop: the read before drop session two admits it
            // alone, whether it still rests or a graceful close cancelled it (decision 0132
            // items 4 and 7).
            let x = x_allowed
                .as_ref()
                .expect("X, placed when drop session one ran");
            self.read_before_two(x).await
        };
        if let Err(why) = ended {
            if after.x_listed {
                self.line(format_args!("RESTING {}", places::HAND_CANCEL));
            }
            return Err(why);
        }
        let (x, x_allowed) = x
            .zip(x_allowed)
            .expect("X, placed when drop session one ran");
        let mine = format!("{label} (drop)");
        let mut reads = self.ev.reads.iter().rev();
        let before_two = reads.find(|r| r.before == places::DROP_TWO && r.label == mine);
        let x_before_two = before_two.is_some_and(|r| r.page_allowed > 0);
        let run = self.drop_run();
        run.x_before_two = Some(x_before_two);
        run.two = Some(TwoRun::default());
        // Drop session two, on the same registry and Link, seeded nothing.
        let (cfg, creds) = two_config;
        let ctx = (cfg, creds, Rc::clone(&link), span_end);
        let ran = self
            .drop_session(places, market, on, ctx, &mut allowance, Some(x))
            .await;
        let now = self.mono();
        let ended = self.ev.place_registries.last_mut();
        ended
            .expect("the drop registry")
            .session_ended(&link.borrow(), places::DROP_TWO, now);
        let (stop, y) = match ran {
            Ok(ran) => ran,
            Err(why) => {
                self.two_run().error = Some(why.clone());
                (None, None)
            }
        };
        let said = stop.map(|stop| self.halt(stop, "in drop session two"));
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let (mut verdict, mut pressed) = (None, false);
        let allowed = |p: Placed| AllowedOrder {
            account: on,
            cid: p.cid,
            venue: vid_of(p.cid),
        };
        let x_allowed = AllowedOrder {
            venue: vid_of(x.cid),
            ..x_allowed
        };
        let y_allowed = y.map(allowed);
        if self.two_run().ended_by == Some(DROPPED) {
            let y_allowed =
                (y_allowed.as_ref()).expect("Y, placed when drop session two reached its drop");
            let symbol = market.found.spec.venue_symbol.as_wire().to_owned();
            let (judged, held) = self
                .judge_drop(on, &symbol, x, (&x_allowed, y_allowed))
                .await;
            verdict = Some(judged);
            pressed = held;
        }
        // X first, so the read says whether X still rests (FBC-77r8's review).
        let ours: Vec<&AllowedOrder> = std::iter::once(&x_allowed)
            .chain(y_allowed.as_ref())
            .collect();
        let mut after = self.read_after(Which::Two, on, &ours).await;
        // A failure that raised no stop: X's trades, and Y's once it was submitted, read after
        // the account reads, as after drop session one, so a fill of either seen with no session
        // open stops the run not flat whatever the position read before it showed (0151 item 9).
        // A provider rate refusal or a press of Ctrl-C meanwhile is the reads' too. A read not
        // whole adds no stop: the failure is still the run's end, with the reads' stop after it.
        if said.is_none() && self.two_run().error.is_some() {
            let (traded, refused, held) =
                self.read_orders(Which::Two, on, x.wall_secs, &ours).await;
            after.not_flat |= traded.naming.iter().any(|&n| n > 0);
            after.refused = after.refused.or(refused);
            pressed |= held;
            self.two_run().after = Some(traded);
        }
        self.patient = None;
        let two = self.two_run();
        two.resting = after.resting;
        two.read = after.read;
        two.x_resting = after.x_listed;
        two.x_read = after.x_read;
        if after.resting {
            self.line(format_args!("RESTING {}", places::HAND_CANCEL_ORDERS));
        }
        // The run's end, the first that applies: the session's stop; a failure that was no stop,
        // with the reads' stop after it when they show one (a provider rate refusal, Ctrl-C, a
        // position not flat or a trade naming X or Y, an order still resting), so an order still
        // resting is marked for the hand cancel the report asks for (0151 items 6 and 9); the
        // verdict's stop (decision 0132 item 7, 0151 items 9 and 13); then the reads' stop.
        let read_stop = after_stop(&after, pressed);
        if let Some(said) = said {
            return Err(said);
        }
        if let Some(why) = self.two_run().error.clone() {
            // X's account not read, or read on a full page not listing X: X, and Y once placed,
            // may rest unjudged, marked as the read before drop session two marks an account it
            // could not read (0151 item 6; FBC-77r8's review).
            let unread = (!after.x_read).then_some((Stop::LeftResting, UNREAD_AFTER_TWO));
            return Err(match read_stop.or(unread) {
                Some((stop, when)) => format!("{why}; {}", self.halt(stop, when)),
                None => why,
            });
        }
        let judged = verdict.as_ref().and_then(crate::verdict::DropVerdict::stop);
        self.drop_run().verdict = verdict;
        // What the reads after the session show of the account, a position not flat or an order
        // or a TWAP the streams did not show, outranks a verdict's stop that comes after it in
        // the verdict's own order ([`outranks`]): the position is then the owner's (0151 item
        // 9), or the account holds what the streams did not show (decision 0148 item 8).
        if let Some(stop) = judged.filter(|j| !outranks(read_stop.map(|(s, _)| s), *j)) {
            return Err(self.halt(stop, "after the drop"));
        }
        match read_stop {
            Some((stop, when)) => Err(self.halt(stop, when)),
            None => Ok(()),
        }
    }

    /// The account's drop test, as the evidence holds it: the last begun.
    fn drop_run(&mut self) -> &mut DropRun {
        self.ev.drops.last_mut().expect("a drop test begun")
    }

    /// Its drop session two, as the evidence holds it.
    fn two_run(&mut self) -> &mut TwoRun {
        self.drop_run().two.get_or_insert_with(TwoRun::default)
    }

    /// What `d`'s order (X in drop session one, Y in two) became, as the evidence holds it.
    fn placed_run(&mut self, which: Which) -> &mut places::XRun {
        match which {
            Which::One => &mut self.drop_run().x,
            Which::Two => &mut self.two_run().y,
        }
    }

    /// The drop test's verdict on `on` once drop session two dropped (decision 0132 item 4, 0151
    /// items 5 and 13): the reads after the drop until X and Y are absent ([`Run::after_drop`]),
    /// then every `get_user_trades` page since X's send and every `GET /user/historic-orders`
    /// page ([`Run::read_orders`]), judged with what drop session two and the read before it
    /// showed. Whether the owner pressed Ctrl-C meanwhile is the second given: the presses are
    /// taken, none ending the reads.
    async fn judge_drop(
        &mut self,
        on: Armed,
        symbol: &str,
        x: Placed,
        (x_allowed, y_allowed): (&AllowedOrder, &AllowedOrder),
    ) -> (crate::verdict::DropVerdict, bool) {
        let order = |o: &AllowedOrder| crate::verdict::DropOrder::new(o.cid, o.venue.clone());
        let orders = order(x_allowed).and_then(|x| order(y_allowed).map(|y| (x, y)));
        let (after_drop, pressed) = match &orders {
            Ok((x, y)) => self.after_drop(on, symbol, (x, y)).await,
            Err(_) => (crate::verdict::AfterDrop::default(), false),
        };
        let both = [x_allowed, y_allowed];
        let read = self.read_orders(Which::Two, on, x.wall_secs, &both);
        let (traded, refused, read_pressed) = read.await;
        let refused = after_drop
            .refused
            .map(|on| Stop::RateRefused { on })
            .or(refused);
        let refused = refused.and_then(|stop| match stop {
            Stop::RateRefused { on } => Some(on),
            _ => None,
        });
        let run = self.drop_run();
        let two = run.two.clone().unwrap_or_default();
        let naming = [0, 1].map(|n| traded.naming.get(n).copied().unwrap_or(0));
        let terminal = [0, 1].map(|n| traded.terminal.get(n).cloned().flatten());
        let facts = crate::verdict::Facts {
            x_before_two: run.x_before_two.unwrap_or(false),
            arm_reported_x: two.x_cancel_reported,
            x_cancelled_before_drop: two.x_cancelled_before_drop,
            x_before_drop: two.before_drop.map(|[x, _]| x),
            y_before_drop: two.before_drop.map(|[_, y]| y),
            y_rested: two.y.acked,
            y_open_at_drop: two.y_open_at_drop,
            after: after_drop.last,
            // Whole when every page was read, whatever a later read met: a refusal is its own
            // failure (FBC-77r8's review), and one met reading the trades left them not whole.
            traded: traded.trades_whole.then_some(naming),
            // What the pages read showed, whole or not: a fill or a status a page showed is not
            // erased by a later page that failed (0151 items 5 and 9; FBC-77r8's review).
            traded_seen: naming,
            // The rows naming neither X nor Y, on the pages read: another order's, or none's.
            other_trades: traded.trades.saturating_sub(naming.iter().sum()),
            historic: traded.historic_whole.then(|| terminal.clone()),
            terminal_seen: terminal,
            others_after: after_drop.others,
            refused,
        };
        let verdict = crate::verdict::DropVerdict::judge(&facts);
        let two = self.two_run();
        two.after_drop = Some(after_drop);
        two.after = Some(traded);
        self.line(format_args!(
            "VERDICT drop test on {}: {}",
            name(on),
            match verdict.passed {
                true => "passed",
                false => "failed",
            }
        ));
        (verdict, pressed || read_pressed)
    }

    /// The reads after the drop on `on` ([`crate::verdict::until_absent`]): never-arming reads
    /// of `get_user_orders` and REST positions on `symbol`, every frame and request charged to
    /// the shared limiter first, at the comparisons' cadence, until one lists neither `orders`
    /// nor the wait ends (0151 item 4), its socket's refusals reported under
    /// [`AFTER_DROP_TWO`]. The owner's presses meanwhile are taken, none ending them: whether
    /// one was is the second given.
    async fn after_drop(
        &mut self,
        on: Armed,
        symbol: &str,
        orders: (&crate::verdict::DropOrder, &crate::verdict::DropOrder),
    ) -> (crate::verdict::AfterDrop, bool) {
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let made = match self.socket_parts(on) {
            Err(_) => None,
            Ok(mut parts) => {
                let places = self.places.expect("a place-stage run");
                let cadence = crate::compare::Cadence::of(places);
                let ctx = &mut *parts.ctx;
                let mut socket =
                    crate::compare::Socket::new(self.io(symbol), &parts.cfg, &parts.reader, ctx);
                let after = {
                    let reads = crate::verdict::until_absent(&mut socket, orders, cadence);
                    tokio::pin!(reads);
                    loop {
                        tokio::select! {
                            biased;
                            after = &mut reads => break after,
                            () = presses.pressed() => pressed = true,
                        }
                    }
                };
                Some((after, socket.close().await))
            }
        };
        pressed |= presses.try_pressed();
        self.presses = presses;
        let Some((after, heard)) = made else {
            return (crate::verdict::AfterDrop::default(), pressed);
        };
        self.ev.read_sockets.push(ReadSocket {
            before: AFTER_DROP_TWO,
            label: format!("{} (the reads after the drop)", name(on)),
            refusals: heard,
        });
        self.line(format_args!(
            "READ after the drop: {}: {} reads, {}",
            name(on),
            after.reads.len(),
            match after.absent_after {
                Some(_) => "the last listing neither X nor Y",
                None => "none listing neither X nor Y",
            }
        ));
        (after, pressed)
    }

    /// What a never-arming read socket of `on`'s is made of: `on`'s configuration, which the
    /// guard admitted, its reader and the context of each signed frame or request.
    fn socket_parts(&self, on: Armed) -> Result<SocketParts, String> {
        let (cfg, _) = configured(&self.base, self.lookup, on)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let reader = auth_qfex::reader(self.lookup, selection)?;
        let mut nonces = self.nonces.clone();
        let origin = self.origin;
        let ctx = Box::new(move || EncodeCtx {
            wall: wall_ns(),
            mono: ledger::mono_since(origin),
            nonces: nonces.reserve(ReadAuth::NONCES),
        });
        Ok(SocketParts { cfg, reader, ctx })
    }

    /// What a never-arming read socket reads with: the run's connector, shared limiter, caps,
    /// namespace, page size and waits, and the traded `symbol` as QFEX writes it.
    fn io<'s>(&'s self, symbol: &'s str) -> crate::compare::Io<'s> {
        crate::compare::Io {
            connector: &self.connector,
            limiter: &self.limiter,
            caps: &self.caps,
            ns: self.ns(),
            page_size: self.opts.page_size,
            connect_wait: self.opts.connect_wait,
            read_wait: self.opts.read_wait,
            symbol,
        }
    }

    /// A place stage's session configuration on `cfg` (decision 0148 item 4; [`place_config`]):
    /// the discovered spec table, the run's `--namespace`, its one `RpcIds`, the run's next
    /// session number, an `AccountWideCodAuthority` for its own account `acct` (decisions 0121
    /// and 0122), and one connection attempt.
    fn place_session_config(
        &self,
        acct: AccountKey,
        cfg: VenueConfig,
        creds: fbc_core::Secrets,
        specs: SpecTable,
    ) -> ExecSessionConfig {
        let pacing = place_pacing(self.opts.connect_wait);
        let bound = ReservedBound::new(4, 4).expect("a bound with Normal room");
        let conn = u16::try_from(self.ev.sessions.len()).expect("a few sessions");
        ExecSessionConfig {
            venue: &QFEX,
            cfg,
            creds,
            acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
            rpc_ids: self.rpc_ids.clone(),
            ns: self.ns(),
            specs,
            connector: self.connector.clone(),
            pacing,
            clock: self.clock.clone(),
            nonces: Box::new(self.nonces.clone()),
            nonce_source: NonceSourceId(0),
            conn,
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        }
    }

    /// A drop session on `on` (decision 0132 item 4): drop session one, or, given X (`x`, which
    /// session one placed), drop session two. An unchanged `ExecSession` whose handler is a
    /// [`PlaceHandler`] over the drop registry's `link`, its steps ([`Run::drop_steps`] or
    /// [`Run::two_steps`]) run beside it and the MDS touch driven on. Whatever ends the steps, a
    /// stop that cancels in a drop session (the owner's Ctrl-C, or any stop once a fill of ours
    /// came: 0151 item 2) is followed by its cancels ([`Run::stop_cancels`]) and a comparison
    /// after them; every other stop sends no cancel. Then the session ends by a graceful close
    /// (0151 item 11: no drop test passed yet), unless its connection ended first, after waiting
    /// for the shared budget's room when a provider refusal holds it; a Ctrl-C while it waits,
    /// with no cancels run yet, is taken as one made during the steps, and so is a stop the tee
    /// or the handler raised meanwhile, a fill of ours among them: its cancels run once there is
    /// room, or, with none within the patience, are held as 0151 item 11 says. But drop session
    /// two that ran its course, Y placed and the read before the drop made, ends by the abrupt
    /// drop: its run is dropped, unpolled, and its connection with it, with no WebSocket close,
    /// no wait for room and no cancel (decision 0132 item 4). Gives the run's stop, if any came,
    /// and the order it placed (X, or Y), once it was submitted; an error is a failure before the
    /// session opened.
    async fn drop_session(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        (cfg, creds, link, span_end): (
            VenueConfig,
            fbc_core::Secrets,
            Rc<RefCell<Link>>,
            tokio::time::Instant,
        ),
        allowance: &mut Allowance,
        x: Option<Placed>,
    ) -> Result<(Option<Stop>, Option<Placed>), String> {
        let which = match x {
            None => Which::One,
            Some(_) => Which::Two,
        };
        let acct = account_key(on);
        let label = name(on);
        let before = self.rejected();
        let sockets = self.ev.read_sockets.len();
        let wake = Rc::new(tokio::sync::Notify::new());
        // A later session of the registry's watches only what follows it.
        let watched = Rc::new(RefCell::new(Watched::after(&link.borrow())));
        let control = Rc::new(RefCell::new(None));
        let handler = PlaceHandler {
            inner: LinkHandler {
                link: Rc::clone(&link),
                wake: Rc::clone(&wake),
            },
            watched: Rc::clone(&watched),
            control: Rc::clone(&control),
        };
        let config = self.place_session_config(acct, cfg, creds, market.found.specs.clone());
        let built = ExecSession::new(config, handler);
        let (mut session, ctl) = built.map_err(|e| format!("the {label} session: {e}"))?;
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        let orders = session.orders();
        link.borrow_mut().attach(orders.clone(), acct);
        match which {
            Which::One => self.line(format_args!(
                "SESSION {label} (drop session one): it arms, places X, compares, and closes \
                 gracefully"
            )),
            Which::Two => self.line(format_args!(
                "SESSION {label} (drop session two): it arms, places Y, reads before the drop, \
                 and drops abruptly"
            )),
        }
        let (heard_from, sent_from) = {
            let kept = self.tee.kept();
            (kept.heard.len(), kept.sent().len())
        };
        let d = DropCtx {
            link,
            wake,
            watched: Rc::clone(&watched),
            orders,
            acct,
            inst: market.found.inst,
            caps: market.caps,
            span_end,
            on,
            which,
            heard_from,
            sent_from,
        };
        let mut presses = std::mem::take(&mut self.presses);
        let mut placed = None;
        let (mut stop, mut error) = (None, None);
        let view = market.touch.view();
        let found = market.found.clone();
        // Whether `running` completed: an `async fn` polled again once it completed panics, so
        // every select on it after the first one that took it is skipped.
        let mut over = false;
        let mut ended_by = GRACEFUL;
        let mut rested_at_close = false;
        {
            let running = session.run();
            tokio::pin!(running);
            let drive = market.touch.drive();
            tokio::pin!(drive);
            let steps: Pin<Box<dyn Future<Output = Result<(), Halt>> + '_>> = match x {
                None => Box::pin(self.drop_steps(
                    places,
                    &found,
                    &view,
                    &d,
                    allowance,
                    &mut presses,
                    &mut placed,
                )),
                Some(x) => Box::pin(self.two_steps(
                    places,
                    (&found, &view),
                    &d,
                    allowance,
                    &mut presses,
                    (x, &mut placed),
                )),
            };
            let stepped = tokio::select! {
                biased;
                ended = &mut running => Err(ended),
                () = &mut drive => unreachable!("the touch is driven until dropped"),
                stepped = steps => Ok(stepped),
            };
            match stepped {
                // A stop the tee or the handler raised as the steps ended is the session's too.
                Ok(Ok(())) => stop = self.stop_now(&d),
                Ok(Err(Halt::Stop(s))) => stop = Some(s),
                Ok(Err(Halt::Error(e))) => error = Some(e),
                // The session ended first: its handler or the tee says why, or it ended before
                // any epoch opened.
                Err(ended) => {
                    over = true;
                    (stop, error) = ended_first(self.stop_now(&d), ended);
                }
            }
            // The abrupt drop's case, decided once the steps ended: drop session two that ran
            // its course waits for no room and sends no cancel (decision 0132 item 4).
            let drop_it = which == Which::Two && stop.is_none() && error.is_none() && !over;
            // The stop's cancels, then the wait for room to close; a press of Ctrl-C taken while
            // the close waits, before any cancels ran, is a stop that cancels too (0151 item 2),
            // and so is a stop the tee or the handler raised meanwhile once a fill of ours came,
            // so they run once there is room, or, with none within the patience, are held (0151
            // item 11), and the close waits again after them.
            let (mut cancelled, mut pressed, mut roomless) = (false, false, false);
            loop {
                let fill = d.watched.borrow().fill_of_ours || self.tee.kept().fill_of_ours;
                let cancels = pressed || stop.is_some_and(|s| s.cancels_in_drop(fill));
                if cancels && !cancelled && !over {
                    cancelled = true;
                    let cancels = tokio::select! {
                        biased;
                        _ = &mut running => None,
                        c = self.stop_cancels(&d, &mut presses) => Some(c),
                    };
                    over |= cancels.is_none();
                    let cancels = cancels.unwrap_or_else(|| Cancels {
                        held: true,
                        ..Cancels::default()
                    });
                    let compare = !cancels.held && cancels.ended && !over;
                    // X, and Y in drop session two: every order of the drop test placed so far.
                    let ours = (x.iter().chain(placed.iter())).map(|p| p.cid);
                    let ours: Vec<fbc_core::ClientOrderId> = ours.collect();
                    self.stop_cancelled(which, cancels);
                    if compare {
                        let step = "the stop's cancels";
                        let compare = self.compare_step(&d, (&found, &ours), on, step, None);
                        let compared = tokio::select! {
                            biased;
                            _ = &mut running => None,
                            c = compare => Some(c),
                        };
                        over |= compared.is_none();
                        if let Some(compared) = compared {
                            self.compared(which, compared);
                        }
                    }
                }
                // A provider rate refusal holds the shared budget (decision 0126), the session's
                // close frame included, which the runtime would then drop unsent, the connection
                // with it: the close waits out the hold, the session read on meanwhile, so it is
                // graceful (0151 item 11). A hold that outlasts the patience leaves no room.
                if !over && !drop_it && !roomless && self.rejected() > before {
                    let room = self.room_for_close();
                    tokio::pin!(room);
                    let room = loop {
                        tokio::select! {
                            biased;
                            _ = &mut running => break None,
                            () = presses.pressed(), if !cancelled && !pressed => pressed = true,
                            room = &mut room => break Some(room),
                        }
                    };
                    over |= room.is_none();
                    roomless = room == Some(false);
                    if roomless {
                        ended_by = NO_ROOM_TO_CLOSE;
                    }
                }
                // The press is the run's stop when none came before it, even when the connection
                // ended as it waited, so that it is not lost: no later read takes it again.
                if pressed && !cancelled {
                    stop = stop.or(Some(Stop::Interrupted));
                }
                // A stop the tee or the handler raised as the close waited, a fill of ours among
                // them, is the session's too when none came before it, and cancels as one
                // raised during the steps would (0151 item 2): back to the cancels, with room.
                stop = stop.or_else(|| self.stop_now(&d));
                let fill = d.watched.borrow().fill_of_ours || self.tee.kept().fill_of_ours;
                let cancels = pressed || stop.is_some_and(|s| s.cancels_in_drop(fill));
                if cancels && !cancelled && !over {
                    continue;
                }
                break;
            }
            if over {
                ended_by = CONNECTION_ENDED;
            } else if drop_it {
                // The abrupt drop (decision 0132 item 4): its end planned, the session's run is
                // dropped here unpolled, its connection with it, with no WebSocket close.
                d.watched.borrow_mut().closing = true;
                ended_by = DROPPED;
            } else {
                // What a graceful close could cancel: X as the registry holds it now.
                rested_at_close = placed.as_ref().is_some_and(|p| {
                    let state = d.link.borrow().registry().get(p.cid).map(|r| r.state());
                    matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled))
                });
                // The close: the control's drop sends the close frame (decision 0132 item 4),
                // the end of its connection planned.
                d.watched.borrow_mut().closing = true;
                drop(control.borrow_mut().take());
                let _ = tokio::time::timeout(CLOSE_WAIT, &mut running).await;
            }
        }
        // A stop raised after the steps, as the session closed or while it waited for room to
        // close, is the session's when none came before it.
        stop = stop.or_else(|| self.stop_now(&d));
        self.presses = presses;
        let counters = session.counters();
        drop(session);
        // What the comparisons' sockets put on the shared limiter themselves, each counted under
        // its socket ([`Run::compare_step`]), so not counted again as the session's.
        let compared: u64 = (self.ev.read_sockets[sockets..].iter())
            .map(|r| r.refusals.limiter)
            .sum();
        let w = d.watched.borrow();
        let arm = w.arm.as_ref().map(places::outcome_name);
        let (echoes, as_encoded) = {
            let kept = self.tee.kept();
            let echoes = places::echoes(&kept, d.heard_from);
            (echoes, places::as_encoded(&kept, d.sent_from))
        };
        let (authenticated, resynced) = (w.authenticated, w.resynced);
        drop(w);
        match which {
            Which::One => {
                let run = self.drop_run();
                run.echoes = echoes;
                run.as_encoded = as_encoded;
                run.x.rested_at_close = rested_at_close;
                run.authenticated = authenticated;
                run.arm.clone_from(&arm);
                run.resynced = resynced;
                run.ended_by = Some(ended_by);
            }
            Which::Two => {
                let two = self.two_run();
                two.echoes = echoes;
                two.as_encoded = as_encoded;
                two.authenticated = authenticated;
                two.arm.clone_from(&arm);
                two.resynced = resynced;
                two.ended_by = Some(ended_by);
            }
        }
        self.line(format_args!(
            "SESSION {label} ({}): arm outcome {}, ended by {ended_by}",
            which.name(),
            arm.as_deref().unwrap_or("none")
        ));
        self.ev.sessions.push(SessionRun {
            account: label,
            name: which.name(),
            authenticated,
            arm,
            ended_by,
            attempts: counters.attempts,
            failed_attempts: counters.failed_attempts,
            arm_failures: counters.arm_failures,
            decode_errors: counters.decode_errors,
            error: error.clone(),
            refusals: Refusals {
                limiter: self.rejected().saturating_sub(before + compared),
                ..Refusals::default()
            },
            ..SessionRun::default()
        });
        match (stop, error) {
            (None, Some(why)) => {
                match which {
                    Which::One => self.drop_run().error = Some(why),
                    Which::Two => self.two_run().error = Some(why),
                }
                Ok((None, placed))
            }
            (stop, _) => Ok((stop, placed)),
        }
    }

    /// Records `cancels`, a stop's cancels in drop session `which`.
    fn stop_cancelled(&mut self, which: Which, cancels: Cancels) {
        match which {
            Which::One => self.drop_run().cancels = Some(cancels),
            Which::Two => self.two_run().cancels = Some(cancels),
        }
    }

    /// Waits, for [`AFTER_PATIENCE`] at most, until the shared limiter has room for a control
    /// frame, a session's close: each time it has none, until the time it gives. The room is
    /// found by charging one control frame, so the close is charged twice, which over-counts
    /// what is sent and never under-counts it (decision 0030). Whether room came: none when the
    /// hold ends past the patience or the limiter gives no time ([`NO_ROOM_TO_CLOSE`]).
    async fn room_for_close(&self) -> bool {
        let until = tokio::time::Instant::now() + AFTER_PATIENCE;
        loop {
            let now = tokio::time::Instant::now();
            match self.limiter.charge(now, NO_CONN, &[CONTROL]) {
                Ok(_) => return true,
                Err(fbc_runtime::Refused {
                    ready_at: Some(ready),
                }) if ready <= until => tokio::time::sleep_until(ready).await,
                Err(_) => return false,
            }
        }
    }

    /// The first stop the tee or the session's handler raised, if any: the tee's first.
    fn stop_now(&self, d: &DropCtx) -> Option<Stop> {
        self.tee.stop().or_else(|| d.watched.borrow().stop())
    }

    /// Waits until `found` finds what it looks for in the session's `Link` and handler, or a
    /// stop comes first: one the tee or the handler raised ([`Run::stop_now`]), the owner's
    /// Ctrl-C ([`Stop::Interrupted`]) or the registry's span bound ([`Stop::SpanBound`]).
    async fn waiting<T>(
        &self,
        d: &DropCtx,
        presses: &mut Presses,
        mut found: impl FnMut(&Link, &Watched) -> Option<T>,
    ) -> Result<T, Stop> {
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(stop) = self.stop_now(d) {
                return Err(stop);
            }
            if let Some(t) = found(&d.link.borrow(), &d.watched.borrow()) {
                return Ok(t);
            }
            tokio::select! {
                biased;
                () = presses.pressed() => return Err(Stop::Interrupted),
                () = tokio::time::sleep_until(d.span_end) => return Err(Stop::SpanBound),
                _ = self.tee.stopped() => {}
                () = woken => {}
            }
        }
    }

    /// The stop already raised, or a press already made, if either: as [`Run::waiting`] would
    /// take it at once, without waiting.
    fn not_stopped(&self, d: &DropCtx, presses: &mut Presses) -> Result<(), Stop> {
        match self.stop_now(d) {
            Some(stop) => Err(stop),
            None if presses.try_pressed() => Err(Stop::Interrupted),
            None => Ok(()),
        }
    }

    /// `work`, or the stop that comes first, as [`Run::waiting`] watches for one.
    async fn unless_stopped<T>(
        &self,
        d: &DropCtx,
        presses: &mut Presses,
        work: impl Future<Output = T>,
    ) -> Result<T, Stop> {
        tokio::pin!(work);
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(stop) = self.stop_now(d) {
                return Err(stop);
            }
            tokio::select! {
                biased;
                () = presses.pressed() => return Err(Stop::Interrupted),
                () = tokio::time::sleep_until(d.span_end) => return Err(Stop::SpanBound),
                done = &mut work => return Ok(done),
                _ = self.tee.stopped() => {}
                () = woken => {}
            }
        }
    }

    /// Waits for the session's stream authenticated, its arm accepted and its resync applied,
    /// the place gate open: an arm that came back otherwise is a failure that is no stop, its
    /// order not placed (decision 0132 item 4).
    async fn armed(&self, d: &DropCtx, presses: &mut Presses) -> Result<(), Halt> {
        let orders = d.orders.clone();
        let arm = self
            .waiting(d, presses, |_, w| {
                let ready = w.authenticated && w.resynced && orders.may_place();
                let refused =
                    (w.arm.as_ref()).filter(|a| !matches!(a, SubmitOutcome::Accepted { .. }));
                match (ready, refused) {
                    (_, Some(arm)) => Some(Err(places::outcome_name(arm))),
                    (true, None) => Some(Ok(())),
                    (false, None) => None,
                }
            })
            .await?;
        arm.map_err(|name| {
            Halt::Error(format!(
                "{}'s arm came back {name}, not Accepted: {} is not placed (decision 0132 item 4)",
                d.which.name(),
                d.which.order()
            ))
        })
    }

    /// Drop session one's steps (decision 0132 item 4): authentication, an `Accepted` arm and
    /// the resync, the place gate open ([`Run::armed`]); Start under the market lease; X
    /// ([`Run::place_resting`]); then the streams compared with the reads. Each step ends at the
    /// first stop ([`Run::waiting`]).
    #[allow(clippy::too_many_arguments)]
    async fn drop_steps(
        &mut self,
        places: &Places,
        found: &Discovered,
        touch: &market::TouchView,
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        x: &mut Option<Placed>,
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let dir = self.leases.as_ref().map(|l| l.dir.clone());
        let dir = dir.ok_or_else(|| Halt::Error("no lease directory".to_owned()))?;
        places::start(d.link.borrow_mut().reg(), &dir, d.inst).map_err(Halt::Error)?;
        self.drop_run().started = true;
        self.line(format_args!(
            "START {}: the market armed under its market lease, with no account lease (QFEX's \
             nonce scope is none)",
            name(d.on)
        ));
        let placed = self
            .place_resting(places, (found, touch), d, allowance, presses, x)
            .await?;
        let ours = [placed.cid];
        let compared = self.compare_step(d, (found, &ours), d.on, "X's place", Some(placed.at));
        let compared = self.unless_stopped(d, presses, compared).await?;
        let stop = compared.0.stop;
        self.compared(Which::One, compared);
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(()),
        }
    }

    /// Drop session two's steps (decision 0132 item 4): authentication, an `Accepted` arm and
    /// the resync ([`Run::armed`]); no Start, since the market stays armed from drop session one
    /// on the same registry; Y ([`Run::place_resting`]); then the read before the drop, one
    /// never-arming read of `get_user_orders` and REST positions beside the session, which lists
    /// X, `x`, and Y or not. Each step ends at the first stop ([`Run::waiting`]), and a provider
    /// rate refusal in the read is one (decision 0148 item 8); a stop raised, or a press made,
    /// as the read's socket closed is taken before the drop, so Ctrl-C then cancels. Then a read
    /// showing what the streams did not, an order or a TWAP neither X nor Y or a position the
    /// registry does not hold, is a stop before the drop ([`Stop::ReadDisagrees`], decision 0148
    /// item 8; FBC-77r8's review), and so is a read not made ([`Stop::LeftResting`]), neither
    /// cancelling anything (0151 item 2). Then, as
    /// the session drops (nothing is awaited between the steps' end and the drop), what its
    /// frames and its registry showed of X and Y ([`Run::at_drop`]).
    async fn two_steps(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        (x, y): (Placed, &mut Option<Placed>),
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let placed = self
            .place_resting(places, (found, touch), d, allowance, presses, y)
            .await?;
        let vid = |cid| (d.link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        let order = |cid| crate::verdict::DropOrder::new(cid, vid(cid)).map_err(Halt::Error);
        let (x, y) = (order(x.cid)?, order(placed.cid)?);
        let parts = self.socket_parts(d.on).map_err(Halt::Error)?;
        let symbol = found.spec.venue_symbol.as_wire();
        let SocketParts {
            cfg,
            reader,
            mut ctx,
        } = parts;
        let mut socket = crate::compare::Socket::new(self.io(symbol), &cfg, &reader, &mut *ctx);
        let read = self.unless_stopped(d, presses, socket.read()).await?;
        // The arm's window ends with the read before the drop: a CANCELLED of X heard by then
        // is the arm's, reported (decision 0132 item 4: no report settles nothing until that
        // read).
        let reported = self
            .tee
            .kept()
            .cancelled_of_from(d.heard_from, x.cid_text());
        let heard = socket.close().await;
        self.two_run().x_cancel_reported = reported;
        self.ev.read_sockets.push(ReadSocket {
            before: places::DROP_TWO,
            label: format!("{} (the read before the drop)", name(d.on)),
            refusals: heard,
        });
        // What the streams did not show (decision 0148 item 8): an order or a TWAP that is
        // neither X nor Y, which the account-wide drop could cancel out of sight, or a position
        // the registry does not hold. X absent is allowed, since the arm may have cancelled it.
        let mut disagrees = false;
        let before_drop = match read {
            crate::compare::Attempt::Read(read) => {
                let listing = crate::verdict::Listing::of(&read, &x, &y);
                let streams = streams_of(&d.link.borrow(), d.inst, found, &[x.cid, y.cid]);
                let others = crate::verdict::others(&read, &x, &y);
                disagrees = others > 0 || read.position != streams.position;
                Some([listing.x, listing.y])
            }
            crate::compare::Attempt::Refused(on) => {
                self.line(format_args!(
                    "READ before the drop: {}: refused by a provider rate refusal",
                    name(d.on)
                ));
                return Err(Halt::Stop(Stop::RateRefused { on }));
            }
            _ => None,
        };
        self.two_run().before_drop = before_drop;
        self.two_run().before_drop_disagrees = disagrees;
        self.line(format_args!(
            "READ before the drop: {}: {}",
            name(d.on),
            match before_drop {
                Some([x, y]) => format!("X listed {x}, Y listed {y}"),
                None => "not made".to_owned(),
            }
        ));
        // A stop raised, or a press made, meanwhile comes first: Ctrl-C or a fill of ours then
        // cancels (0151 item 2).
        self.not_stopped(d, presses)?;
        // Then the read's own stop, before the drop: it cancels nothing, and the session ends by
        // a graceful close (0151 item 2), its orders left for the owner's hand cancel. A read not
        // made (it failed, or found no room) looked at nothing the drop could cancel out of
        // sight: X and Y may rest unjudged, as an account the read before drop session two could
        // not read leaves X (FBC-77r8's review).
        if before_drop.is_none() {
            self.line(format_args!(
                "READ before the drop: {}: not made, so no drop",
                name(d.on)
            ));
            return Err(Halt::Stop(Stop::LeftResting));
        }
        if disagrees {
            self.line(format_args!(
                "READ before the drop: {}: an order, a TWAP or a position the streams did not \
                 show (decision 0148 item 8): no drop",
                name(d.on)
            ));
            return Err(Halt::Stop(Stop::ReadDisagrees));
        }
        self.at_drop(d, (&x, &y));
        Ok(())
    }

    /// What drop session two showed of X and Y, `x` and `y`, as it drops: whether a `CANCELLED`
    /// of X was heard in the session, so the drop did not end it (decision 0132 item 4), and
    /// whether Y was still open in the registry, with no `CANCELLED` of it heard, so the drop had
    /// Y of its own to cancel (0151 item 13).
    fn at_drop(
        &mut self,
        d: &DropCtx,
        (x, y): (&crate::verdict::DropOrder, &crate::verdict::DropOrder),
    ) {
        let (x_cancelled, y_cancelled) = {
            let kept = self.tee.kept();
            let of =
                |o: &crate::verdict::DropOrder| kept.cancelled_of_from(d.heard_from, o.cid_text());
            (of(x), of(y))
        };
        let state = d.link.borrow().registry().get(y.cid).map(|r| r.state());
        let open = matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled));
        let two = self.two_run();
        two.x_cancelled_before_drop = x_cancelled;
        two.y_open_at_drop = open && !y_cancelled;
    }

    /// Places `d`'s order, X in drop session one or Y in two: a post-only GTC at the
    /// instrument's minimum size `--away-bps` behind the touch, spent from the drop registry's
    /// list first ([`Allowance`]), its client id minted under the run's namespace lease, as an
    /// fbc-oms authorization; the one place before an ACK echoes our client id in its session
    /// (decision 0132 item 7). Before it is built into a command, both USD caps are asked again
    /// at the touch it is priced at ([`market::caps_admit`]); an order either would not admit
    /// is not sent, and never resized, the run's stop [`Stop::BeyondCaps`] (decision 0113).
    /// Then its ACK, or its deadline. Gives it, as placed, once handed to the session (`placed`
    /// holds it from then); a stop or a press first places nothing.
    async fn place_resting(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        placed: &mut Option<Placed>,
    ) -> Result<Placed, Halt> {
        let what = match d.which {
            Which::One => ledger::X,
            Which::Two => ledger::Y,
        };
        let order = d.which.order();
        allowance.take(Step::Drop, what)?;
        let leases = self.leases.as_mut().expect("a place-stage run's leases");
        let cid = leases.mint.mint(1).await.map_err(Halt::Error)?[0];
        let quote = touch.now(places.touch_max_age).map_err(Halt::Error)?;
        let px = market::rest_price(&found.spec, places.side, quote, places.away_bps);
        let px = px.map_err(Halt::Error)?;
        // A test's X not priced: a failure of drop session one that is no stop.
        #[cfg(test)]
        if places.test.x_unpriced && d.which == Which::One {
            return Err(Halt::Error("a test's X could not be priced".to_owned()));
        }
        let new = fbc_core::NewOrder {
            cid,
            inst: d.inst,
            side: places.side,
            qty: found.min_size,
            kind: fbc_core::OrderKind::Limit { px },
            tif: fbc_core::Tif::Gtc,
            channel: fbc_core::Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        // A stop raised, or a press made, while the id was minted and the touch read, before the
        // order is built: nothing more is placed (decision 0132 item 7, 0151 item 2).
        self.not_stopped(d, presses)?;
        // The caps asked again at the touch the order (X, or Y) is priced at: fbc-oms holds them
        // in lots fixed at the preflight's touch, which a touch risen since leaves above what the
        // USD caps admit (decision 0113). It is then not sent, and never resized.
        if !market::caps_admit(places, found, quote, d.caps, found.min_size) {
            return Err(Halt::Stop(Stop::BeyondCaps));
        }
        let submitted = {
            let mut link = d.link.borrow_mut();
            let cmd = link
                .reg()
                .place(new)
                .map_err(|e| format!("{order} was refused: {}", places::variant_name(&e)));
            let auth = cmd.and_then(|cmd| {
                let auth = link.reg().authorize(d.acct, cmd);
                auth.map_err(|e| {
                    format!("{order} was not authorized: {}", places::variant_name(&e))
                })
            });
            auth.and_then(|auth| link.submit(&d.orders, auth))
        };
        let rpc = submitted.map_err(Halt::Error)?;
        let now = Placed {
            cid,
            at: tokio::time::Instant::now(),
            wall_secs: u64::try_from(wall_secs()).unwrap_or(0),
        };
        *placed = Some(now);
        self.placed_run(d.which).submitted = true;
        self.line(format_args!(
            "PLACE {order} on {}: one post-only GTC at the instrument's minimum size, --away-bps \
             behind the touch (no price, size or id is shown)",
            name(d.on)
        ));
        // Its ACK, or its deadline.
        let acked = self
            .waiting(d, presses, |link, _| {
                let state = link.registry().get(cid).map(|r| r.state());
                let rested =
                    state.is_some_and(|s| !matches!(s, OrdState::PendingNew | OrdState::Unknown));
                match (rested, link.outcome_of(rpc)) {
                    (true, outcome) => Some((true, outcome)),
                    (false, Some(outcome)) => Some((false, Some(outcome))),
                    (false, None) => None,
                }
            })
            .await?;
        let exact = self.tee.kept().exact_ack_from(d.heard_from);
        // It is ours whatever client id its later frames carry (decision 0132 item 7).
        let vid = d
            .link
            .borrow()
            .registry()
            .get(cid)
            .and_then(|r| r.vid().cloned());
        if let Some(vid) = vid {
            self.tee.ours(vid.as_str());
        }
        // Acked: resting now, or ended after an `ACK` of ours (a fill at once, say); an order
        // that ended with no `ACK`, refused or cancelled as it came, never rested.
        let open = {
            let link = d.link.borrow();
            let state = link.registry().get(cid).map(|r| r.state());
            matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled))
        };
        let acked_ours = self.tee.kept().acked_ours_from(d.heard_from);
        let run = self.placed_run(d.which);
        run.acked = acked.0 && (open || acked_ours);
        run.outcome = acked.1.as_ref().map(places::outcome_name);
        run.exact_echo = exact;
        run.ack_after = run.acked.then(|| now.at.elapsed());
        // A test's session failing once its order was answered, that order resting.
        #[cfg(test)]
        if places.test.fail_after_place == Some(usize::from(d.which == Which::Two)) {
            return Err(Halt::Error(format!(
                "a test's session failed after {order}'s answer"
            )));
        }
        Ok(now)
    }
    /// The streams of `d`'s session, its orders `ours` on `found`'s market, compared with
    /// never-arming reads of `on` after the step named `step`, made at `since` (now when `None`),
    /// at the run's cadence (0151 item 4, [`crate::compare`]): its socket's refusals reported
    /// under `d`'s session.
    async fn compare_step(
        &self,
        d: &DropCtx,
        (found, ours): (&Discovered, &[fbc_core::ClientOrderId]),
        on: Armed,
        step: &'static str,
        since: Option<tokio::time::Instant>,
    ) -> (crate::compare::Compared, Option<ReadSocket>) {
        let since = since.unwrap_or_else(tokio::time::Instant::now);
        let places = self.places.expect("a place-stage run");
        let label = name(on);
        let (cfg, _) = match configured(&self.base, self.lookup, on) {
            Ok(configured) => configured,
            Err(why) => return (unread(step, why), None),
        };
        let reader = account_selection(&cfg)
            .map_err(|e| e.to_string())
            .and_then(|selection| auth_qfex::reader(self.lookup, selection));
        let reader = match reader {
            Ok(reader) => reader,
            Err(why) => return (unread(step, why), None),
        };
        let io = crate::compare::Io {
            connector: &self.connector,
            limiter: &self.limiter,
            caps: &self.caps,
            ns: self.ns(),
            page_size: self.opts.page_size,
            connect_wait: self.opts.connect_wait,
            read_wait: self.opts.read_wait,
            symbol: found.spec.venue_symbol.as_wire(),
        };
        let mut nonces = self.nonces.clone();
        let origin = self.origin;
        let mut ctx = move || EncodeCtx {
            wall: wall_ns(),
            mono: ledger::mono_since(origin),
            nonces: nonces.reserve(ReadAuth::NONCES),
        };
        let mut socket = crate::compare::Socket::new(io, &cfg, &reader, &mut ctx);
        let streams = || streams_of(&d.link.borrow(), d.inst, found, ours);
        let cadence = crate::compare::Cadence::of(places);
        let compared = crate::compare::compare(step, &mut socket, streams, cadence, since).await;
        // Its limiter count is the refusals it held the shared budget for itself, so a session's
        // refusal met meanwhile is counted once, as the session's.
        let heard = socket.close().await;
        let socket = ReadSocket {
            before: d.which.name(),
            label: format!("{label} (the comparison after {step})"),
            refusals: heard,
        };
        self.line(format_args!(
            "COMPARE after {step}: {} reads, {}",
            compared.reads.len(),
            match compared.first_agreed {
                Some(after) =>
                    format!("the first agreeing {} ms after the step", after.as_millis()),
                None => "none agreed".to_owned(),
            }
        ));
        (compared, Some(socket))
    }

    /// Records `compared`, a comparison of drop session `which`, and its read socket's refusals.
    fn compared(
        &mut self,
        which: Which,
        (compared, socket): (crate::compare::Compared, Option<ReadSocket>),
    ) {
        self.ev.read_sockets.extend(socket);
        match which {
            Which::One => self.drop_run().compared.push(compared),
            Which::Two => self.two_run().compared.push(compared),
        }
    }

    /// A stop's cancels in a drop session (0151 items 2 and 11; as `paradex_testnet_acceptance`
    /// stops): Kill, cancel everything of ours on the market by venue id, each an fbc-oms
    /// authorization through the `Link`, those awaiting their order's acknowledgement sent as
    /// each lands; then a wait, up to `--rpc-timeout-ms`, for every order they name to end,
    /// each of the owner's presses meanwhile counted, a second Ctrl-C included, none ending it
    /// early; then the market disarmed. A cancel refused locally, answered other than accepted,
    /// or met by a provider rate refusal holds them: the wait ends there.
    async fn stop_cancels(&self, d: &DropCtx, presses: &mut Presses) -> Cancels {
        let mut c = Cancels::default();
        let refusals_before = self.tee.kept().stops.len();
        let (cmds, mut waiting, refused) = {
            let mut link = d.link.borrow_mut();
            let caps = link.caps().clone();
            let reg = link.reg();
            reg.kill(d.inst);
            match reg.cancel_everything(d.inst, &caps) {
                fbc_oms::CancelEverything::CancelAll {
                    command,
                    unanswered,
                } => {
                    let mut cmds = vec![command];
                    cmds.extend(unanswered.commands);
                    (cmds, unanswered.awaiting_ack, 0)
                }
                fbc_oms::CancelEverything::Explicit { plan, .. } => {
                    (plan.commands, plan.awaiting_ack, plan.refused.len())
                }
            }
        };
        c.held |= refused > 0;
        let mut rpcs = Vec::new();
        let submit = |cmd, rpcs: &mut Vec<fbc_core::RpcId>| {
            let mut link = d.link.borrow_mut();
            let auth = link
                .reg()
                .authorize(d.acct, cmd)
                .map_err(|e| format!("{e:?}"));
            auth.and_then(|auth| link.submit(&d.orders, auth))
                .map(|rpc| rpcs.push(rpc))
        };
        for cmd in cmds {
            c.held |= submit(cmd, &mut rpcs).is_err();
        }
        let until = tokio::time::Instant::now() + Duration::from_millis(self.opts.rpc_timeout_ms);
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let due: Vec<fbc_core::ClientOrderId> = {
                let mut link = d.link.borrow_mut();
                let caps = link.caps().clone();
                let due = link.reg().cancels_due(&caps);
                due.into_iter()
                    .filter(|cid| waiting.contains(cid))
                    .collect()
            };
            for cid in due {
                waiting.retain(|w| *w != cid);
                let built = {
                    let mut link = d.link.borrow_mut();
                    let caps = link.caps().clone();
                    link.reg()
                        .cancellable(cid)
                        .map(|permit| permit.cancel(&caps))
                };
                match built {
                    Ok(fbc_oms::CancelChoice::Send(cmd)) => {
                        c.held |= submit(cmd, &mut rpcs).is_err()
                    }
                    Ok(fbc_oms::CancelChoice::AwaitAck) => waiting.push(cid),
                    Err(_) => c.held = true,
                }
            }
            let (named, answered) = {
                let link = d.link.borrow();
                let named: Vec<_> = rpcs.iter().flat_map(|r| link.cids_of(*r)).collect();
                let answered: Vec<_> = rpcs.iter().filter_map(|r| link.outcome_of(*r)).collect();
                let ended = named
                    .iter()
                    .all(|cid| (link.registry().get(*cid)).is_none_or(|r| r.state().is_terminal()));
                (ended && waiting.is_empty(), answered)
            };
            let rate = (self
                .tee
                .kept()
                .stops
                .get(refusals_before..)
                .unwrap_or_default())
            .iter()
            .any(|s| matches!(s, Stop::RateRefused { .. }));
            c.held |= rate
                || answered
                    .iter()
                    .any(|o| !matches!(o, SubmitOutcome::Accepted { .. }));
            if named || c.held {
                c.ended = named;
                break;
            }
            tokio::select! {
                biased;
                () = presses.pressed() => c.presses += 1,
                () = woken => {}
                () = tokio::time::sleep_until(until) => break,
            }
        }
        c.sent = rpcs.len();
        d.link.borrow_mut().reg().disarm(d.inst);
        self.line(format_args!(
            "STOP cancels: Kill, then {} cancels by venue id (0151 item 2), {}; the market \
             disarmed",
            c.sent,
            match (c.held, c.ended) {
                (true, _) => "held",
                (false, true) => "every order they named seen to end",
                (false, false) => "not every order they named seen to end in time",
            }
        ));
        if c.presses > 0 {
            self.line(format_args!(
                "STOP cancels: {} more presses of Ctrl-C while they ran, recorded",
                c.presses
            ));
        }
        c
    }

    /// The read of every named account after drop session `which` on `on` (decision 0132 item
    /// 7), each whatever another showed. `ours` are the drop test's orders placed so far, X
    /// first: X is admitted on its account, so the read says whether it rests, and an order on
    /// any account's page that is none of `ours`, or a TWAP, is what the streams did not show
    /// ([`AfterReads::unexpected`], decision 0148 item 8). The owner's presses meanwhile are
    /// taken, none ending the reads: the run stops after them. A provider rate refusal they
    /// meet, a `GET /user/positions` answered 429 or 418, any rise in the shared limiter's
    /// `rejected.user` count, or one their read sockets' frames show, in clear or kept whole
    /// ([`Refusals::shown`]), is a stop too, once they all ran.
    async fn read_after(&mut self, which: Which, on: Armed, ours: &[&AllowedOrder]) -> AfterReads {
        let mut after = AfterReads::default();
        let mark = self.read_mark();
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let x = ours.first().copied();
        for account in self.named.clone() {
            let read = {
                let read = self.read_one(which.after(), on, " (drop)", account, (x, ours));
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        biased;
                        read = &mut read => break read,
                        () = presses.pressed() => pressed = true,
                    }
                }
            };
            match read {
                Ok(read) => {
                    // X's account is read when its page listed X or was the last: a full page
                    // not listing X leaves X unread, not absent. With no X to find, after drop
                    // session two, the account is read when its read ended.
                    let found = x.is_none() || read.page_lists_it || !read.page_full;
                    after.read |= account == on;
                    after.x_read |= account == on && found;
                    after.x_flat |= account == on && read.flat;
                    after.not_flat |= !read.flat;
                    after.resting |= read.page_orders > 0 || read.rest_open_orders > 0;
                    // An order none of ours, or a TWAP: what the streams did not show (decision
                    // 0148 item 8), as compare::judge counts one. An open order REST alone counts
                    // (a stop order) or one on a later page of a full page is not told from ours
                    // ([`AccountRead::page_unknown`]): it shows an order resting, marked.
                    after.unexpected |= read.page_unknown > 0 || read.page_twaps > 0;
                    // X by its venue id once known, whatever client id its row carries.
                    after.x_listed |= read.page_lists_it;
                }
                Err(why) => self.ev.errors.push(why),
            }
        }
        self.presses = presses;
        after.pressed = pressed;
        after.refused = self.refused_since(mark);
        after
    }

    /// Where the account reads stand, for [`Run::refused_since`].
    fn read_mark(&self) -> ReadMark {
        ReadMark {
            rejected: self.rejected(),
            rest: self.rest_refused,
            sockets: self.ev.read_sockets.len(),
        }
    }

    /// The provider rate refusal the account reads met since `mark`, if any, a stop (decision
    /// 0148 item 8): a `GET /user/positions` answered 429 or 418, then any rise in the shared
    /// limiter's `rejected.user` count, or one their read sockets' frames show, in clear or kept
    /// whole ([`Refusals::shown`]).
    fn refused_since(&self, mark: ReadMark) -> Option<Stop> {
        let sockets = &self.ev.read_sockets[mark.sockets..];
        let shown = sockets.iter().any(|r| r.refusals.shown() > 0);
        let socket = self.rejected() > mark.rejected || shown;
        match (self.rest_refused > mark.rest, socket) {
            (true, _) => Some(Stop::RateRefused {
                on: places::RefusedOn::Rest,
            }),
            (false, true) => Some(Stop::RateRefused {
                on: places::RefusedOn::ReadSocket,
            }),
            (false, false) => None,
        }
    }

    /// `reads`, the account reads made since `mark`, before `when`: a provider rate refusal they
    /// met is the run's stop, marked (decision 0148 item 8, 0151 item 6), whether or not they
    /// failed on it, the failure kept among the errors; otherwise their own result.
    fn unless_refused(
        &mut self,
        mark: ReadMark,
        reads: Result<(), String>,
        when: &str,
    ) -> Result<(), String> {
        let Some(stop) = self.refused_since(mark) else {
            return reads;
        };
        if let Err(why) = reads {
            self.ev.errors.push(why);
        }
        Err(self.halt(stop, when))
    }

    /// The read before drop session two (FBC-77r8; decision 0132 item 7, 0144 item 4): every
    /// named account read as before a session ([`Run::check_accounts_allowing`]) under
    /// [`places::DROP_TWO`], admitting `x`, X resting from drop session one, alone on its account
    /// ([`AccountRead::refusal`]: X listed, and REST counting one open order on its symbol and
    /// none on another). The reads wait out a hold the shared budget is under, as the reads after
    /// a session do, and the owner's presses meanwhile are taken, none ending them. X rests
    /// whatever they show, so each way they end the run is a stop, marked: a provider rate
    /// refusal they met ([`Stop::RateRefused`]; one their sockets' frames show, kept whole
    /// included, [`Refusals::shown`]), then the owner's Ctrl-C ([`Stop::Interrupted`]),
    /// then a read that is not clean, an open order but X, a TWAP or a position the streams never
    /// showed ([`Stop::ReadDisagrees`]), then an account that could not be read, X left resting
    /// unjudged ([`Stop::LeftResting`]).
    async fn read_before_two(&mut self, x: &AllowedOrder) -> Result<(), String> {
        let (mark, made) = (self.read_mark(), self.ev.reads.len());
        let mut presses = std::mem::take(&mut self.presses);
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let mut pressed = false;
        let read = {
            let some = Some(x);
            let read = self.check_accounts_allowing(places::DROP_TWO, x.account, " (drop)", some);
            tokio::pin!(read);
            loop {
                tokio::select! {
                    biased;
                    read = &mut read => break read,
                    () = presses.pressed() => pressed = true,
                }
            }
        };
        pressed |= presses.try_pressed();
        self.patient = None;
        self.presses = presses;
        // A refusal their read sockets' frames show, in clear or kept whole, is one too, though
        // the adapter reads no hold from a frame kept whole ([`Refusals::shown`]).
        let refused = self.refused_since(mark);
        let unclean = (self.ev.reads[made..].last()).is_some_and(|r| r.refusal().is_some());
        let stop = match (refused, pressed, &read) {
            (Some(stop), ..) => stop,
            (None, true, _) => Stop::Interrupted,
            (None, false, Ok(())) => return Ok(()),
            (None, false, Err(_)) if unclean => Stop::ReadDisagrees,
            (None, false, Err(_)) => Stop::LeftResting,
        };
        let why = read.err().map_or(String::new(), |why| format!("{why}; "));
        let said = self.halt(stop, "in the read before drop session two");
        Err(format!("{why}{said}"))
    }

    /// The drop test's orders and their trades read after drop session `which` (FBC-be57,
    /// FBC-77r8) on a socket of `on`'s that never arms: every `get_user_trades` page from X's
    /// send, `since` in whole seconds, and every `GET /user/historic-orders` page, each charged to
    /// the shared limiter first; the trades naming each of `orders` (X, then Y after the drop)
    /// counted, each one's `terminal_status` kept as a status name, and the member names the
    /// rows carry. A full page the same as the one before it, row for row, as a venue ignoring
    /// the offset answers, fails the read rather than paging on. A provider rate refusal is a
    /// stop. The owner's presses meanwhile, and one made before they end, are taken, none ending
    /// them: whether one was is the last given.
    async fn read_orders(
        &mut self,
        which: Which,
        on: Armed,
        since: u64,
        orders: &[&AllowedOrder],
    ) -> (Traded, Option<Stop>, bool) {
        let mut after = Traded {
            naming: vec![0; orders.len()],
            terminal: vec![None; orders.len()],
            ..Traded::default()
        };
        let before = self.rejected();
        let mut heard = Refusals::default();
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let read = {
            let read = self.reading_orders(on, since, orders, &mut after, &mut heard);
            tokio::pin!(read);
            loop {
                tokio::select! {
                    biased;
                    read = &mut read => break read,
                    () = presses.pressed() => pressed = true,
                }
            }
        };
        pressed |= presses.try_pressed();
        self.presses = presses;
        heard.limiter = self.rejected().saturating_sub(before);
        let what = match which {
            Which::One => "X and its trades",
            Which::Two => "X, Y and their trades",
        };
        self.ev.read_sockets.push(ReadSocket {
            before: which.after(),
            label: format!("{} ({what})", name(on)),
            refusals: heard,
        });
        let refused = match read {
            Ok(()) => None,
            Err((why, refused)) => {
                after.error = Some(why);
                refused
            }
        };
        let rose = self.rejected() > before;
        let refused = refused.or(rose.then_some(Stop::RateRefused {
            on: places::RefusedOn::ReadSocket,
        }));
        (after, refused, pressed)
    }

    /// [`Run::read_orders`]'s reads, into `after`, each frame counted in `heard`.
    async fn reading_orders(
        &mut self,
        on: Armed,
        since: u64,
        orders: &[&AllowedOrder],
        after: &mut Traded,
        heard: &mut Refusals,
    ) -> Result<(), (String, Option<Stop>)> {
        let plain = |e: String| (e, None);
        let (cfg, _) = configured(&self.base, self.lookup, on).map_err(plain)?;
        let selection = account_selection(&cfg).map_err(|e| plain(e.to_string()))?;
        let reader = auth_qfex::reader(self.lookup, selection).map_err(plain)?;
        let mut drop_orders = Vec::new();
        for order in orders {
            let made = crate::verdict::DropOrder::new(order.cid, order.venue.clone());
            drop_orders.push(made.map_err(|e| plain(format!("a client id: {e}")))?);
        }
        let url = account_reads::trade_url(&cfg, &reader).map_err(|e| plain(e.to_string()))?;
        let opened = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await;
        let mut opened = opened.map_err(plain)?;
        let ws = &mut opened;
        let ctx = self.ctx();
        let auth = account_reads::authentication(&cfg, &reader, &ctx);
        self.write(ws, &auth.map_err(|e| plain(e.to_string()))?)
            .await
            .map_err(plain)?;
        let reply = next(ws, self.opts.read_wait, &self.limiter)
            .await
            .map_err(plain)?;
        self.heard_x(heard, &reader, &reply)?;
        if !account_reads::authenticated(&reader, reply.raw()) {
            return Err(plain(
                "QFEX did not authenticate the read socket".to_owned(),
            ));
        }
        let mut offset = 0;
        let mut before: Option<Vec<account_reads::TradeRow>> = None;
        loop {
            let ask = account_reads::TradesAsk::new(since, self.opts.page_size, offset);
            let ask = ask.map_err(|e| plain(e.to_string()))?;
            let write = account_reads::user_trades(&cfg, ask).map_err(|e| plain(e.to_string()))?;
            self.write(ws, &write).await.map_err(plain)?;
            let reply = next(ws, self.opts.read_wait, &self.limiter)
                .await
                .map_err(plain)?;
            self.heard_x(heard, &reader, &reply)?;
            let page = account_reads::decode_trades_page(&reader, reply.raw());
            let page =
                page.map_err(|e| plain(format!("get_user_trades: {}", places::variant_name(&e))))?;
            after.trades += page.rows().len();
            for (n, order) in drop_orders.iter().enumerate() {
                after.naming[n] += page.rows().iter().filter(|r| order.named_by(r)).count();
            }
            for row in page.rows() {
                for member in &row.members {
                    let member = places::member_name(member);
                    if !after.fields.contains(&member) {
                        after.fields.push(member);
                    }
                }
            }
            if page.rows().len() < usize::from(ask.limit()) {
                break;
            }
            // A full page the same as the one before it, row for row: a venue ignoring the
            // offset, which would otherwise be paged for as long as the limiter had room. Rows
            // are compared whole, since a row may carry no trade id.
            if before.as_deref() == Some(page.rows()) {
                return Err(plain(
                    "a full get_user_trades page repeated the page before it".to_owned(),
                ));
            }
            before = Some(page.rows().to_vec());
            offset += u32::from(ask.limit());
        }
        after.trades_whole = true;
        let _ = self.close_read(opened).await;
        let mut offset = 0;
        let mut before: Option<Vec<account_reads::HistoricOrder>> = None;
        loop {
            let ask = account_reads::HistoricAsk::new(self.opts.page_size, offset);
            let ask = ask.map_err(|e| plain(e.to_string()))?;
            let ctx = self.ctx();
            let read = account_reads::historic_orders(&cfg, &reader, &ctx, ask);
            let (status, body) = self
                .rest(&read.map_err(|e| plain(e.to_string()))?)
                .await
                .map_err(plain)?;
            if matches!(status, 429 | 418) {
                let rest = places::RefusedOn::Rest;
                return Err((
                    format!("GET /user/historic-orders: HTTP {status}"),
                    Some(Stop::RateRefused { on: rest }),
                ));
            }
            let resp = HttpResponse {
                status,
                headers: &[],
                body: &body,
            };
            let page = account_reads::decode_historic(&resp);
            let page = page.map_err(|e| plain(format!("GET /user/historic-orders: {e}")))?;
            for (n, order) in drop_orders.iter().enumerate() {
                if let Some(status) = order.terminal_in(page.rows()) {
                    after.terminal[n] = Some(status);
                }
            }
            if page.rows().len() < usize::from(ask.limit()) {
                break;
            }
            // As for the trades, a full page the same as the one before it, which QFEX, declaring
            // no REST bucket, would otherwise let the read page without end. A row carries no
            // order id, and two orders' rows may be alike, so a page is compared whole.
            if before.as_deref() == Some(page.rows()) {
                return Err(plain(
                    "a full GET /user/historic-orders page repeated the page before it".to_owned(),
                ));
            }
            before = Some(page.rows().to_vec());
            offset += u32::from(ask.limit());
        }
        after.historic_whole = true;
        Ok(())
    }

    /// Counts `frame`, off the read socket of X and its trades, as a read socket's
    /// ([`Refusals::hear`]), holding the shared budget for a `RateLimited` err, which is a stop;
    /// so is every other refusal its count shows, one in a frame the journal keeps whole
    /// included ([`Refusals::shown`]), from which the adapter reads no hold (decision 0134 items
    /// 2 and 6, 0135 item 2).
    fn heard_x(
        &self,
        heard: &mut Refusals,
        reader: &ReadAuth,
        frame: &Data,
    ) -> Result<(), (String, Option<Stop>)> {
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        let shown = heard.shown();
        heard.hear(
            &refusals::kept(raw, &spans),
            &String::from_utf8_lossy(raw.bytes()),
        );
        let on = places::RefusedOn::ReadSocket;
        match account_reads::socket_err(reader, raw) {
            Some(SocketErr::RateLimited(hold)) => {
                let now = tokio::time::Instant::now();
                (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
                Err((
                    "a RateLimited err on the read socket of X and its trades".to_owned(),
                    Some(Stop::RateRefused { on }),
                ))
            }
            _ if heard.shown() > shown => Err((
                "a provider rate refusal on X's read socket".to_owned(),
                Some(Stop::RateRefused { on }),
            )),
            _ => Ok(()),
        }
    }
}

/// Where the account reads stood ([`Run::read_mark`]): the shared limiter's `rejected.user`
/// count, the REST refusals and the read sockets recorded.
#[derive(Copy, Clone, Debug)]
struct ReadMark {
    rejected: u64,
    rest: u64,
    sockets: usize,
}

/// What the after-session read of every named account showed ([`Run::read_after`]).
#[derive(Copy, Clone, Debug, Default)]
struct AfterReads {
    /// Whether an account held an open order (on the page, or counted by REST).
    resting: bool,
    /// Whether an account's page listed an order that is none of the drop test's, or a TWAP:
    /// what the streams did not show (decision 0148 item 8, FBC-77r8's review).
    unexpected: bool,
    /// Whether an account held a position that is not flat, or, after drop session two failed
    /// with no stop, a trade named X or Y.
    not_flat: bool,
    /// Whether X was listed on its account's page.
    x_listed: bool,
    /// Whether the drop test's account was read.
    read: bool,
    /// Whether the drop test's account was read, on a page that listed X or was its last.
    x_read: bool,
    /// Whether X's account was read and its position was flat.
    x_flat: bool,
    /// Whether the owner pressed Ctrl-C while they ran.
    pressed: bool,
    /// The provider rate refusal they met, if any: a stop ([`Run::read_after`]).
    refused: Option<Stop>,
}

/// The stop the reads after drop session two make, given `after` and whether the owner pressed
/// Ctrl-C during the reads after the drop, or during X's and Y's trades read after a failure
/// (`pressed`): [`read_stop`]'s, with [`Stop::ReadDisagrees`] for an order none of ours or a
/// TWAP after a position not flat and before an order of ours resting.
fn after_stop(after: &AfterReads, pressed: bool) -> Option<(Stop, &'static str)> {
    let pressed = pressed || after.pressed;
    // After a position not flat and before an order of ours still resting: an order or a TWAP
    // the streams did not show (decision 0148 item 8).
    let shown = read_stop(after.refused, pressed, after.not_flat, false);
    let unexpected = (after.unexpected).then_some((Stop::ReadDisagrees, "after drop session two"));
    (shown.or(unexpected)).or_else(|| read_stop(None, false, false, after.resting))
}

/// When the reads after drop session two met no stop of their own but did not read X's account
/// ([`Run::drop_sessions`]).
const UNREAD_AFTER_TWO: &str = "after drop session two, X's account not read";

/// Whether `read`, the stop the reads after drop session two make, outranks `judged`, the
/// verdict's, in the verdict's own order ([`crate::verdict::DropVerdict::stop`]): a position
/// not flat outranks an order or a TWAP not ours, an arm-time cancel QFEX did not report and a
/// test that only did not pass; an order or a TWAP not ours outranks the last two (FBC-77r8's
/// review). Nothing else outranks the verdict's stop.
fn outranks(read: Option<Stop>, judged: Stop) -> bool {
    let after = |s: Stop| matches!(s, Stop::ArmCancelUnreported | Stop::DropFailed);
    match read {
        Some(Stop::NotFlat) => judged == Stop::ReadDisagrees || after(judged),
        Some(Stop::ReadDisagrees) => after(judged),
        _ => false,
    }
}

/// The stop the reads after drop session two make, and when it was met, the first that applies:
/// the provider rate refusal they met, `refused`; the owner's Ctrl-C during them, `pressed`; a
/// position not flat; an order of ours still resting, for the owner's hand cancel (decision
/// 0132 item 7). Each is marked (0151 item 6). None when they show none of them.
pub fn read_stop(
    refused: Option<Stop>,
    pressed: bool,
    not_flat: bool,
    resting: bool,
) -> Option<(Stop, &'static str)> {
    const AFTER: &str = "after drop session two";
    if let Some(stop) = refused {
        return Some((stop, "in the reads after drop session two"));
    }
    match (pressed, not_flat, resting) {
        (true, ..) => Some((Stop::Interrupted, "during the reads after drop session two")),
        (false, true, _) => Some((Stop::NotFlat, AFTER)),
        (false, false, true) => Some((Stop::LeftResting, AFTER)),
        (false, false, false) => None,
    }
}

/// A comparison that made no read, since its socket could not be set up: `why` is not shown.
fn unread(step: &'static str, _why: String) -> crate::compare::Compared {
    crate::compare::Compared {
        step,
        reads: Vec::new(),
        first_agreed: None,
        ended: crate::compare::Ended::Unread,
        stop: None,
    }
}

/// What the streams of a drop session show of the account on `inst` ([`crate::compare`]): each
/// of `ours`, the orders its registry placed, by client id and venue id, and where it stands,
/// and the registry's position in the venue's units.
fn streams_of(
    link: &Link,
    inst: fbc_core::InstrumentId,
    found: &Discovered,
    ours: &[fbc_core::ClientOrderId],
) -> crate::compare::Streams {
    use crate::compare::{Ours, Seen, Streams};
    let reg = link.registry();
    let orders = (ours.iter().filter_map(|cid| reg.get(*cid)))
        .map(|r| Ours {
            cid: r.cid(),
            venue: r.vid().cloned(),
            seen: match r.state() {
                OrdState::PendingNew | OrdState::Unknown => Seen::InFlight,
                OrdState::Open | OrdState::PartiallyFilled => Seen::Open,
                OrdState::Terminal(_) => Seen::Ended,
            },
        })
        .collect();
    let step = found.spec.size_step.get();
    Streams {
        orders,
        position: rust_decimal::Decimal::from(reg.inventory(inst).0) * step,
    }
}
