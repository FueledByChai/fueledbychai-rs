//! The place stages' leases, client ids, stops, allowance and stop marker (FBC-23ch, a slice of
//! FBC-9sk2; decisions 0132 item 7, 0148 and 0151): what a place-stage run holds and spends
//! before and while it places, kept apart from the driver that opens its sessions.
//!
//! - **The lease directory** ([`RunLeases::open`]): `--lease-dir`, or production's default under
//!   `$HOME` (`probe_args::LEASE_DIR_NAME`), never the Paradex testnet samples' (refused as the
//!   command line is read). Its [`ENVIRONMENT`] file is written on first use, holding
//!   [`TAG`], and a directory whose tag names anything else is refused before anything connects
//!   (0151 item 1), its tag never shown.
//! - **The client ids** ([`Mint`]), as `paradex_testnet_acceptance` opens and mints: one
//!   `NamespaceLease` on `--namespace` held in the lease directory for the run, so a second run
//!   on that namespace there is refused; one `CidMint` for the run, across its accounts, keyed by
//!   [`MINT_ACCOUNT`]; and the high-water mark from `HighWater::at`, unchanged, written to the
//!   disk off the runtime's thread before any id minted goes out, so a later run, its wall clock
//!   stepped back or not, mints above every id this one used.
//! - **Start and Flatten** ([`start`], [`flatten`]): each under the `MarketLease` the registry's
//!   lease names give, taken in the lease directory. QFEX's nonce scope is `None`, so no account
//!   lease is taken.
//! - **The stops** ([`Stop`]; decisions 0132 item 7 and 0148 item 8): each reported by its name
//!   and what it is, never with an id or an amount. In a drop session only [`Stop::Interrupted`]
//!   and a fill of ours cancel (0151 item 2, [`Stop::cancels_in_drop`]): once a fill of ours
//!   reached the session, whichever stop came first.
//! - **The allowance** ([`Allowance`]; decision 0148 item 7): each registry's `MayPlace` list as
//!   counts left by kind, and one flatten budget of `--flatten-attempts` for the account across
//!   its registries. A place whose kind has none left is not built, and the run stops
//!   ([`Stop::BeyondList`]); nor is one the USD caps would not admit at the touch it is priced
//!   at ([`Stop::BeyondCaps`], decision 0113).
//! - **The stop marker** ([`write_marker`], [`RunLeases::open`]; 0151 item 6): on any stop but
//!   [`Stop::NoEcho`], the run writes [`MARKER`] in its lease directory with the stop's name and
//!   the run's start, and the stops of the marker it found there, unless it cleared them, so a
//!   marker it replaces loses no stop still unreconciled. A later place-stage run looks for one
//!   in its lease directory and in production's default, refuses to start while one exists
//!   without `--reconciled`, and refuses `--reconciled` when none exists. With it, the run reads every named account first and clears
//!   the marker only if every read is clean; the declaration is the owner's, which the probe does
//!   not check, and the report says so. Stage one and the hold neither read nor write it.
//! - **The drop test's record** ([`DropRun`], [`TwoRun`], [`Traded`]; FBC-23ch, FBC-77r8): each
//!   named account's two drop sessions, the reads after them and the verdict, as the report and
//!   the evidence show them ([`drop_markdown`], [`drop_to_json`]): names, flags, counts, status
//!   and member names, and times, never an id, a price or a size. A later session of a
//!   registry's watches only what follows its start ([`Watched::after`]).

use std::cell::RefCell;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use fbc_core::{
    AccountKey, CidMint, ClientOrderId, ConnKey, ConnState, Envelope, ExecEvent, InstrumentId,
    LeaseError, MarketLease, NamedLeaseError, Namespace, NamespaceLease, OrderCaps, SubmitHandle,
    SubmitOutcome, VenueFactory, VenueOrderSnapshot, VenueSymbol, WallNs,
};
use fbc_oms::{LeaseKeys, Leases, PositionCheck, Registry, ResyncReport};
use fbc_runtime::{ExecControl, ExecHandler};
use fbc_venue_qfex::Qfex;
use serde_json::{Value, json};

use crate::ledger::{FLATTEN, PlaceLists, Step};
use crate::link::{LinkHandler, Note};
use crate::probe_args::{Places, Reconciled};
use crate::testnet::{self, HighWater, off_thread_holding};

/// The file of the lease directory naming the environment its leases are for (0151 item 1).
pub const ENVIRONMENT: &str = "environment";

/// What [`ENVIRONMENT`] holds in a place-stage lease directory: every place-stage run is a
/// production run (0151 item 1).
pub const TAG: &str = "production";

/// The stop marker's file in the lease directory (0151 item 6).
pub const MARKER: &str = "stopped";

/// The account number the run's one namespace lease and high-water mark are keyed by: the mint
/// is one per run, across the run's accounts, and `HighWater::at` names this number.
pub const MINT_ACCOUNT: AccountKey = testnet::ACCT;

/// The reads of every named account before drop session one, as the report names them: with
/// `--reconciled`, the reads that clear the stop marker.
pub const DROP_ONE: &str = "drop session one";

/// The reads of every named account before drop session two, which admit X alone (decision
/// 0132 item 7, 0144 item 4), as the report names them, and drop session two's own name.
pub const DROP_TWO: &str = "drop session two";

/// Why the place stages stopped (decisions 0132 item 7 and 0148 item 8): each reported by name
/// ([`Stop::name`]) and by what it is ([`Stop::says`]), never with an id or an amount.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    /// An order event or a fill of ours without our client id.
    NotOurCid,
    /// An `order_response` naming no order of ours (0151 item 13). One naming an order of ours
    /// before any frame tied that order to us, with no client id of ours (an `ACK` of X with
    /// none, X's venue id not yet known), is this stop too: the tee cannot tell it from
    /// another's order.
    StrayOrder,
    /// A replayed fill the registry's ledger refused for its time.
    ReplayRefused(Replay),
    /// A resync that disagrees with the registry (`Desync`).
    ResyncDesync,
    /// A resync with a fill unsettled across it (`Unsettled`).
    ResyncUnsettled,
    /// The drop test failed: the reads after the drop did not show X and Y ended
    /// (`verdict::DropVerdict`).
    DropFailed,
    /// The read before the drop showed X cancelled by the arm with no report of it.
    ArmCancelUnreported,
    /// A session that ended with a position that is not flat, or a fill of ours seen after it
    /// with no session open (0151 item 9).
    NotFlat,
    /// An order of ours still resting after a session.
    LeftResting,
    /// A provider rate refusal, and where it was met.
    RateRefused { on: RefusedOn },
    /// A read showing an order or a position the streams did not.
    ReadDisagrees,
    /// The owner's interrupt (Ctrl-C).
    Interrupted,
    /// The registry's span bound.
    SpanBound,
    /// A session's connection ended unplanned: a server close, the codec's end or a transport
    /// error, with no second attempt.
    ConnectionEnded,
    /// A fill of ours other than the taker IOC's and the flatten IOCs' (0151 item 9).
    UnexpectedFill,
    /// A place beyond its registry's list, or a flatten IOC beyond the account's budget.
    BeyondList,
    /// A place the USD caps would not admit at the touch it is priced at: the registry's lot
    /// caps, fixed at the preflight's touch, above what either cap admits there, or the order
    /// itself above it (decision 0113). It is not sent, and never resized.
    BeyondCaps,
    /// No `ACK` in the run echoed one of our client ids byte for byte: not a fault (FBC-su9i).
    NoEcho,
}

/// The time a replayed fill was refused for (fbc-oms's `Admission`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Replay {
    Untimed,
    BeyondHorizon,
}

/// Where a provider rate refusal was met.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RefusedOn {
    /// On a session's connection.
    Session,
    /// On a read socket of a comparison or an account read.
    ReadSocket,
    /// On stage three's pager.
    Pager,
    /// On a REST read: HTTP 429 or 418.
    Rest,
}

impl Stop {
    /// Every stop, in the order the report lists them.
    pub const ALL: [Stop; 22] = [
        Stop::NotOurCid,
        Stop::StrayOrder,
        Stop::ReplayRefused(Replay::Untimed),
        Stop::ReplayRefused(Replay::BeyondHorizon),
        Stop::ResyncDesync,
        Stop::ResyncUnsettled,
        Stop::DropFailed,
        Stop::ArmCancelUnreported,
        Stop::NotFlat,
        Stop::LeftResting,
        Stop::RateRefused {
            on: RefusedOn::Session,
        },
        Stop::RateRefused {
            on: RefusedOn::ReadSocket,
        },
        Stop::RateRefused {
            on: RefusedOn::Pager,
        },
        Stop::RateRefused {
            on: RefusedOn::Rest,
        },
        Stop::ReadDisagrees,
        Stop::Interrupted,
        Stop::SpanBound,
        Stop::ConnectionEnded,
        Stop::UnexpectedFill,
        Stop::BeyondList,
        Stop::BeyondCaps,
        Stop::NoEcho,
    ];

    /// Its name, as the report, the evidence and the marker give it.
    pub fn name(self) -> &'static str {
        match self {
            Stop::NotOurCid => "NotOurCid",
            Stop::StrayOrder => "StrayOrder",
            Stop::ReplayRefused(Replay::Untimed) => "ReplayRefused(Untimed)",
            Stop::ReplayRefused(Replay::BeyondHorizon) => "ReplayRefused(BeyondHorizon)",
            Stop::ResyncDesync => "ResyncDesync",
            Stop::ResyncUnsettled => "ResyncUnsettled",
            Stop::DropFailed => "DropFailed",
            Stop::ArmCancelUnreported => "ArmCancelUnreported",
            Stop::NotFlat => "NotFlat",
            Stop::LeftResting => "LeftResting",
            Stop::RateRefused { on } => match on {
                RefusedOn::Session => "RateRefused(Session)",
                RefusedOn::ReadSocket => "RateRefused(ReadSocket)",
                RefusedOn::Pager => "RateRefused(Pager)",
                RefusedOn::Rest => "RateRefused(Rest)",
            },
            Stop::ReadDisagrees => "ReadDisagrees",
            Stop::Interrupted => "Interrupted",
            Stop::SpanBound => "SpanBound",
            Stop::ConnectionEnded => "ConnectionEnded",
            Stop::UnexpectedFill => "UnexpectedFill",
            Stop::BeyondList => "BeyondList",
            Stop::BeyondCaps => "BeyondCaps",
            Stop::NoEcho => "NoEcho",
        }
    }

    /// The stop named `name`, as [`Stop::name`] gives it.
    pub fn named(name: &str) -> Option<Stop> {
        Stop::ALL.into_iter().find(|stop| stop.name() == name)
    }

    /// What it is, with the decision it comes from.
    pub fn says(self) -> &'static str {
        match self {
            Stop::NotOurCid => {
                "an order event or a fill of ours without our client id (decision 0132 item 7)"
            }
            Stop::StrayOrder => {
                "an order_response naming no order of ours, on accounts --sole-trader declares \
                 ours alone (0151 item 13), or one of ours that no frame had yet tied to us, \
                 naming no client id of ours"
            }
            Stop::ReplayRefused(Replay::Untimed) => {
                "a replayed fill the registry's fill ledger refused Untimed (decision 0132 item 7)"
            }
            Stop::ReplayRefused(Replay::BeyondHorizon) => {
                "a replayed fill the registry's fill ledger refused BeyondHorizon (decision 0132 \
                 item 7)"
            }
            Stop::ResyncDesync => {
                "a resync whose orders or positions disagree with the registry's (decision 0132 \
                 item 7)"
            }
            Stop::ResyncUnsettled => {
                "a resync with a fill unsettled across it (decision 0132 item 7)"
            }
            Stop::DropFailed => {
                "the drop test failed: the reads after the drop did not show X and Y absent from \
                 get_user_orders, named by no get_user_trades row and listed by GET \
                 /user/historic-orders (decision 0132 items 4 and 7, 0151 item 13)"
            }
            Stop::ArmCancelUnreported => {
                "the read before the drop showed X cancelled by drop session two's arm with no \
                 CANCELLED report of it, recorded and stopped on after the drop (decision 0132 \
                 items 4 and 7)"
            }
            Stop::NotFlat => {
                "a session that ended with a position that is not flat, or a fill of ours seen \
                 after it with no session open, the position the owner's (decisions 0132 item 7, \
                 0151 item 9)"
            }
            Stop::LeftResting => {
                "an order of ours still resting after a session (decision 0132 item 7)"
            }
            Stop::RateRefused { on } => match on {
                RefusedOn::Session => {
                    "a provider rate refusal on a session's connection (decision 0148 item 8)"
                }
                RefusedOn::ReadSocket => {
                    "a provider rate refusal on a read socket (decision 0148 item 8)"
                }
                RefusedOn::Pager => {
                    "a provider rate refusal on stage three's pager (decision 0148 item 8)"
                }
                RefusedOn::Rest => {
                    "a provider rate refusal of a REST read, HTTP 429 or 418 (decision 0148 item \
                     8)"
                }
            },
            Stop::ReadDisagrees => {
                "a read showing an order or a position the streams did not (decision 0148 item 8)"
            }
            Stop::Interrupted => "the owner's interrupt, Ctrl-C (decision 0148 item 8)",
            Stop::SpanBound => "the registry's span bound (decision 0148 item 8)",
            Stop::ConnectionEnded => {
                "a session's connection ended unplanned, by a server close, the codec's end or a \
                 transport error, with no second attempt (decision 0148 item 7)"
            }
            Stop::UnexpectedFill => {
                "a fill of ours other than the taker IOC's and the flatten IOCs' (0151 item 9)"
            }
            Stop::BeyondList => {
                "a place beyond its registry's list, or a flatten IOC beyond the account's \
                 --flatten-attempts, which was not built (decision 0148 item 7)"
            }
            Stop::BeyondCaps => {
                "a place --resting-cap-usd or --inventory-cap-usd would not admit at the touch it \
                 is priced at, the registry's lot caps fixed at the preflight's touch: not sent \
                 and never resized (decision 0113)"
            }
            Stop::NoEcho => {
                "no ACK in this run echoed one of our client ids byte for byte, so no crossing \
                 order is placed (decision 0148 item 2): not a fault"
            }
        }
    }

    /// Whether the run writes the stop marker for it: every stop but [`Stop::NoEcho`], which is
    /// not a fault (0151 item 6).
    pub fn marks(self) -> bool {
        self != Stop::NoEcho
    }

    /// Whether the run, stopped by it, cancels our orders by venue id in a drop session: only at
    /// the owner's interrupt and a fill of ours, either of which ends the drop test's measurement
    /// (0151 item 2). `fill_of_ours` is whether a fill of ours reached the session, whatever
    /// stop it raised (a fill without our client id raises [`Stop::NotOurCid`]) and whichever
    /// stop came first, since the run's stop is the first one raised. Every other stop there
    /// sends no cancel and ends the session by a graceful close.
    pub fn cancels_in_drop(self, fill_of_ours: bool) -> bool {
        fill_of_ours || matches!(self, Stop::Interrupted | Stop::UnexpectedFill)
    }
}

/// What a place-stage registry's sessions may still place (decision 0148 item 7): each
/// registry's `MayPlace` list as counts left by kind, and the account's one flatten budget of
/// `--flatten-attempts`, which every list repeats and the account spends once across its
/// registries (`ledger::PlaceLists::flatten_budget`). One per named account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Allowance {
    left: Vec<(Step, &'static str, u64)>,
    flatten_left: u64,
}

impl Allowance {
    /// An account's allowance, every registry's list of `lists` and its flatten budget whole.
    pub fn new(lists: &PlaceLists) -> Allowance {
        let left = Step::ALL.into_iter().flat_map(|step| {
            let list = lists.list(step);
            list.into_iter()
                .map(move |may| (step, may.what, may.orders))
        });
        Allowance {
            left: left.collect(),
            flatten_left: lists.flatten_budget(),
        }
    }

    /// How many of `what` `step`'s registry may still place, the account's flatten budget
    /// bounding a flatten IOC's.
    pub fn left(&self, step: Step, what: &'static str) -> u64 {
        let listed = self.left.iter().find(|(s, w, _)| *s == step && *w == what);
        let listed = listed.map_or(0, |(_, _, n)| *n);
        match what == FLATTEN {
            true => listed.min(self.flatten_left),
            false => listed,
        }
    }

    /// Spends one `what` of `step`'s registry, before the place is built: refused, nothing
    /// spent, with [`Stop::BeyondList`] when its list has none left or, for a flatten IOC, when
    /// the account's budget is used up, in whichever registry asks.
    pub fn take(&mut self, step: Step, what: &'static str) -> Result<(), Stop> {
        if self.left(step, what) == 0 {
            return Err(Stop::BeyondList);
        }
        let entry = self
            .left
            .iter_mut()
            .find(|(s, w, _)| *s == step && *w == what);
        let (_, _, n) = entry.expect("a kind with one left is listed");
        *n -= 1;
        if what == FLATTEN {
            self.flatten_left -= 1;
        }
        Ok(())
    }
}

/// The run's client-id mint under its namespace lease, and the high-water mark kept beside it.
pub struct Mint {
    /// `None` once a write of the mark did not end within its wait: the mint went with it.
    mint: Option<CidMint>,
    hwm: HighWater,
    /// How long a write of the mark may take.
    wait: Duration,
}

impl Mint {
    /// Takes the namespace lease on `ns` in `dir`, refused while another holder has it, and
    /// builds the run's one mint above the mark kept there and the wall floor of `start`. Every
    /// named account reads with no open order before any session places (decision 0132 item 7),
    /// so no id of ours rests, and the mint counts no venue snapshot. Each write of the mark
    /// waits at most `wait`. A refusal never shows a path.
    pub fn open(dir: &Path, ns: Namespace, start: WallNs, wait: Duration) -> Result<Mint, String> {
        let lease = NamespaceLease::acquire(dir, MINT_ACCOUNT, ns).map_err(|e| match e {
            LeaseError::Held => "refused, nothing sent: the client-id namespace's lease is held: \
                                 another run holds --namespace in this lease directory (the \
                                 values are not shown)"
                .to_owned(),
            LeaseError::Io(io) => format!(
                "refused, nothing sent: the client-id namespace's lease cannot be taken: {} (the \
                 path is not shown)",
                io.source.kind()
            ),
        })?;
        let hwm = HighWater::at(dir, ns);
        let persisted = hwm.read().map_err(|_| {
            "refused, nothing sent: the client-id high-water mark in the lease directory cannot \
             be read (the path is not shown)"
                .to_owned()
        })?;
        Ok(Mint {
            mint: Some(CidMint::new(lease, persisted, 0, start)),
            hwm,
            wait,
        })
    }

    /// Mints `n` client ids and keeps the high-water mark on the disk, off the runtime's thread,
    /// before returning them, so none goes out before the mark covers it. A mint whose mark's
    /// write did not end within its wait is gone, and every later call is refused.
    pub async fn mint(&mut self, n: usize) -> Result<Vec<ClientOrderId>, String> {
        let mut mint = self
            .mint
            .take()
            .ok_or("no client-id mint: a write of its mark failed")?;
        let mut cids = Vec::with_capacity(n);
        for _ in 0..n {
            match mint.mint() {
                Ok(cid) => cids.push(cid),
                Err(e) => {
                    self.mint = Some(mint);
                    return Err(format!("no client id: {}", variant_name(&e)));
                }
            }
        }
        let (mark, hwm) = (mint.high_water(), self.hwm.clone());
        let kept = off_thread_holding(self.wait, mint, move || hwm.write(mark)).await;
        let unkept = "the client-id high-water mark could not be kept (the path is not shown)";
        match kept {
            Some((Ok(()), mint)) => {
                self.mint = Some(mint);
                Ok(cids)
            }
            Some((Err(_), mint)) => {
                self.mint = Some(mint);
                Err(unkept.to_owned())
            }
            None => Err(format!(
                "{unkept}: its write did not end within {} ms",
                self.wait.as_millis()
            )),
        }
    }

    /// The highest seq issued so far, the mark the next write keeps.
    #[allow(dead_code)] // the mint's tests read it
    pub fn high_water(&self) -> Option<u64> {
        self.mint.as_ref().map(CidMint::high_water)
    }
}

/// The lease names of `account`'s place-stage registries on the traded market `inst`, whose
/// venue symbol is `symbol`: QFEX's venue id, the account's name, its order caps' nonce scope.
pub fn lease_keys(
    account: &str,
    order: &OrderCaps,
    inst: InstrumentId,
    symbol: VenueSymbol,
) -> LeaseKeys {
    LeaseKeys::new(Qfex.id(), account, order).with_market(inst, symbol)
}

/// The owner's Start on `inst` of `reg`, under the market lease its lease names give, taken in
/// `dir` (Rules; decision 0013). No account lease: QFEX's nonce scope is `None`.
pub fn start(reg: &mut Registry, dir: &Path, inst: InstrumentId) -> Result<(), String> {
    let lease = market_lease(reg, dir, inst)?;
    let started = reg.start(inst, Leases::market(lease));
    started
        .map(|_| ())
        .map_err(|e| format!("Start was refused: {e}"))
}

/// The owner's Flatten on `inst` of `reg`, into Exit, under the market lease as [`start`] takes
/// it (decision 0148 item 5).
#[allow(dead_code)] // a flatten after a fill of ours takes it (FBC-ou9g)
pub fn flatten(reg: &mut Registry, dir: &Path, inst: InstrumentId) -> Result<(), String> {
    let lease = market_lease(reg, dir, inst)?;
    let flattened = reg.flatten(inst, Leases::market(lease));
    flattened
        .map(|_| ())
        .map_err(|e| format!("Flatten was refused: {e}"))
}

/// The market lease on `inst` that `reg`'s lease names give, taken in `dir`: refused while
/// another holder has it, never showing a path or a symbol.
fn market_lease(reg: &Registry, dir: &Path, inst: InstrumentId) -> Result<MarketLease, String> {
    let keys = reg.lease_keys().ok_or("the registry has no lease names")?;
    let symbol = keys
        .symbol(inst)
        .ok_or("the registry's lease names name no such market")?;
    MarketLease::acquire(dir, keys.venue(), keys.account(), symbol).map_err(|e| match e {
        NamedLeaseError::Held { .. } => "the market's lease is held: another run trades this \
                                         market on this account (the names are not shown)"
            .to_owned(),
        _ => "the market's lease cannot be taken (the path and the names are not shown)".to_owned(),
    })
}

/// Tags `dir` for production on its first use, creating it, or refuses it when its tag names
/// anything else (0151 item 1), never showing the path or the tag. The tag is written whole
/// beside its place and linked into it, so it is never half written, and two runs tagging one
/// directory at once leave one tag.
pub fn tag(dir: &Path) -> Result<(), String> {
    let refused =
        |what: &str| format!("refused, nothing sent: --lease-dir: {what} (the path is not shown)");
    fs::create_dir_all(dir).map_err(|e| refused(&format!("cannot be created: {}", e.kind())))?;
    let path = dir.join(ENVIRONMENT);
    if !path.exists() {
        let tmp = dir.join(format!("{ENVIRONMENT}.{}.tmp", std::process::id()));
        let written = (|| -> std::io::Result<()> {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(format!("{TAG}\n").as_bytes())?;
            file.sync_all()?;
            match fs::hard_link(&tmp, &path) {
                Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => Err(e),
                _ => Ok(()),
            }
        })();
        let _ = fs::remove_file(&tmp);
        written.map_err(|e| {
            refused(&format!(
                "its environment tag cannot be written: {}",
                e.kind()
            ))
        })?;
    }
    let read = fs::read_to_string(&path);
    let read =
        read.map_err(|e| refused(&format!("its environment tag cannot be read: {}", e.kind())))?;
    match read.trim() == TAG {
        true => Ok(()),
        false => Err(refused(
            "tagged for another environment than production: the place stages hold their \
             leases only in a directory tagged production, on its first use (0151 item 1; the \
             tag is not shown)",
        )),
    }
}

/// A stop marker found as the run starts: the earlier runs' stops it names, by name, in its
/// order, none when it names no stop this program writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub stops: Vec<&'static str>,
}

impl Found {
    /// The earlier stops as the report names them: `not readable` when it names none.
    pub fn names(&self) -> Vec<&'static str> {
        match self.stops.is_empty() {
            true => vec!["not readable"],
            false => self.stops.clone(),
        }
    }
}

/// The stops of the markers found, as a refusal and the report say them.
pub fn stops_said(names: &[&str]) -> String {
    match names {
        [one] => format!("its stop: {one}"),
        several => format!("its stops: {}", several.join("; ")),
    }
}

/// The marker in `dir`, when there is one. Only its stops' names are read back, and only as
/// [`Stop::ALL`]'s, so nothing else in the file reaches an output.
pub fn marker_in(dir: &Path) -> Result<Option<Found>, String> {
    let path = dir.join(MARKER);
    match fs::read_to_string(&path) {
        Ok(text) => {
            let named = text.lines().filter_map(|line| line.strip_prefix("stop: "));
            let stops = named.filter_map(Stop::named).map(Stop::name).collect();
            Ok(Some(Found { stops }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "refused, nothing sent: a lease directory's stop marker cannot be read: {} (the path \
             is not shown)",
            e.kind()
        )),
    }
}

/// Writes the stop marker in `dir`: `stop`'s name, then each of `earlier`, the stops of the
/// marker it replaces that are still unreconciled, once each, then `started`, the run's start,
/// written whole beside it, synced and renamed over it, so a later run finds it whole, every
/// stop in it named, or finds none (0151 item 6). A stop that is not a fault ([`Stop::marks`])
/// writes none and leaves the earlier marker as it is.
pub fn write_marker(
    dir: &Path,
    stop: Stop,
    earlier: &[&'static str],
    started: &str,
) -> Result<bool, String> {
    if !stop.marks() {
        return Ok(false);
    }
    let mut stops = vec![stop.name()];
    for name in earlier {
        if !stops.contains(name) {
            stops.push(name);
        }
    }
    let mut text: String = stops.iter().map(|s| format!("stop: {s}\n")).collect();
    let _ = writeln!(text, "started: {started}");
    let (path, tmp) = (dir.join(MARKER), dir.join(format!("{MARKER}.tmp")));
    let written = (|| -> std::io::Result<()> {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        fs::rename(&tmp, &path)?;
        fs::File::open(dir)?.sync_all()
    })();
    written.map(|()| true).map_err(|e| {
        format!(
            "the stop marker could not be written: {} (the path is not shown); the next \
             place-stage run will not see this stop",
            e.kind()
        )
    })
}

/// Removes the stop marker in each of `dirs`, as a run with `--reconciled` does once every
/// named account read clean (0151 item 6).
pub fn clear_markers(dirs: &[PathBuf]) -> Result<(), String> {
    for dir in dirs {
        match fs::remove_file(dir.join(MARKER)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "the stop marker could not be cleared: {} (the path is not shown)",
                    e.kind()
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// The stop markers a run found and `--reconciled` together (0151 item 6): refused when one was
/// found and the run has no `--reconciled`, and when the run has `--reconciled` and none was.
pub fn check_reconciled(found: &[Found], reconciled: Option<Reconciled>) -> Result<(), String> {
    let names: Vec<&str> = found.iter().flat_map(Found::names).collect();
    match (found.first(), reconciled) {
        (Some(_), None) => Err(format!(
            "refused, nothing sent: a stop marker from an earlier place-stage run ({}) is in the \
             lease directory or production's default: reconcile every named account by hand, \
             then give --reconciled <hand-cancel|hand-close|both|none>; the run then reads every \
             named account first and clears the marker only if every read is clean (0151 item \
             6)",
            stops_said(&names)
        )),
        (None, Some(_)) => Err(
            "refused, nothing sent: --reconciled, but no stop marker is in the lease directory \
             or production's default: give it only after a place-stage run that stopped (0151 \
             item 6)"
                .to_owned(),
        ),
        _ => Ok(()),
    }
}

/// What a place-stage run holds in its lease directory: the directory, the run's [`Mint`] under
/// its namespace lease, and the stop markers it found as it started.
pub struct RunLeases {
    pub dir: PathBuf,
    pub mint: Mint,
    /// The directories whose marker the run found: its lease directory, production's default,
    /// or both.
    pub marked: Vec<PathBuf>,
    /// The stops the marker found in its own lease directory names, which a marker it writes
    /// there over that one keeps unless the run cleared it.
    pub carried: Vec<&'static str>,
    pub marker: MarkerRun,
}

impl RunLeases {
    /// Opens `places`' lease directory before anything connects: tags it for production on its
    /// first use or refuses it tagged otherwise ([`tag`]); looks for a stop marker in it and in
    /// production's default and holds them to `--reconciled` ([`check_reconciled`]); then takes
    /// the namespace lease and builds the run's mint ([`Mint::open`]) at `start`, each write of
    /// its mark waiting at most `wait`. Every refusal says nothing was sent and shows no path.
    pub fn open(places: &Places, start: WallNs, wait: Duration) -> Result<RunLeases, String> {
        let dir = places.lease_dir.clone();
        tag(&dir)?;
        let mut dirs = vec![dir.clone()];
        let other = places.default_lease_dir.as_ref();
        dirs.extend(other.filter(|d| !same(d, &dir)).cloned());
        let (mut marked, mut found, mut carried) = (Vec::new(), Vec::new(), Vec::new());
        for d in dirs {
            if let Some(f) = marker_in(&d)? {
                if d == dir {
                    carried = f.stops.clone();
                }
                marked.push(d);
                found.push(f);
            }
        }
        check_reconciled(&found, places.reconciled)?;
        let ns = Namespace::new(places.namespace);
        let mint = Mint::open(&dir, ns, start, wait)?;
        let marker = MarkerRun {
            found: found.iter().flat_map(Found::names).collect(),
            reconciled: places.reconciled.map(Reconciled::name),
            ..MarkerRun::default()
        };
        Ok(RunLeases {
            dir,
            mint,
            marked,
            carried,
            marker,
        })
    }

    /// Whether the run reconciles: it found a marker and has `--reconciled`.
    pub fn reconciling(&self) -> bool {
        !self.marked.is_empty()
    }
}

/// Whether `a` and `b` are one directory, as written or once each symbolic link is resolved.
fn same(a: &Path, b: &Path) -> bool {
    let resolved = |p: &Path| fs::canonicalize(p).ok();
    a == b || resolved(a).is_some_and(|a| resolved(b) == Some(a))
}

/// What a place-stage run did with stop markers, for the report and the evidence: names and
/// flags alone, never a path.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkerRun {
    /// The markers found as it started: every earlier stop each names, by name.
    pub found: Vec<&'static str>,
    /// `--reconciled`, the owner's declaration, which the probe does not check.
    pub reconciled: Option<&'static str>,
    /// Whether it cleared them, after clean reads of every named account.
    pub cleared: bool,
    /// The stop its own marker names, when it wrote one.
    pub written: Option<&'static str>,
    /// Why a marker could not be cleared or written, when one could not.
    pub error: Option<String>,
}

/// The report's section on the stop marker (0151 item 6).
pub fn markdown(s: &mut String, run: &MarkerRun) {
    let found = match run.found.is_empty() {
        true => "No stop marker from an earlier place-stage run was found.".to_owned(),
        false => format!(
            "A stop marker from an earlier place-stage run was found ({}).",
            stops_said(&run.found)
        ),
    };
    let declared = match run.reconciled {
        Some(said) => format!(
            " The owner declared `--reconciled {said}`: the owner's own declaration of what was \
             done by hand, which the probe does not check (decision 0148 item 9)."
        ),
        None => String::new(),
    };
    let cleared = match (run.found.is_empty(), run.cleared) {
        (true, _) => String::new(),
        (false, true) => " It was cleared after every named account read clean.".to_owned(),
        (false, false) => {
            " It was kept: not every named account read clean before the run stopped.".to_owned()
        }
    };
    let written = match run.written {
        Some(stop) => format!(
            "This run stopped ({stop}) and wrote a stop marker in its lease directory: the next \
             place-stage run needs `--reconciled`."
        ),
        None => "This run wrote no stop marker.".to_owned(),
    };
    let error = (run.error.as_ref()).map_or(String::new(), |e| format!(" Error: {e}."));
    let _ = writeln!(
        s,
        "## The stop marker (0151 item 6)\n\n{found}{declared}{cleared} {written}{error}\n"
    );
}

/// The evidence's entry for the stop marker.
pub fn to_json(run: &MarkerRun) -> Value {
    json!({"found": run.found, "reconciled": run.reconciled, "declaration_checked": false,
        "cleared": run.cleared, "written": run.written, "error": run.error})
}

// ----- Drop session one (FBC-23ch; decision 0132 item 4, 0151 items 2, 9, 11 and 13) -----

/// What a drop session's handler saw ([`PlaceHandler`]), for the driver to wait on: the
/// instants the session reached, and the stops the handler raised, in order.
#[derive(Debug, Default)]
pub struct Watched {
    /// Whether the codec reported the stream authenticated.
    pub authenticated: bool,
    /// The outcome of the session's own request, its cancel-on-disconnect arm: the first one
    /// the session reported for a request not submitted through the `Link`.
    pub arm: Option<SubmitOutcome>,
    /// Whether the session's resync was applied to the registry.
    pub resynced: bool,
    /// Every stop the handler raised, in order: the first is the one it reports.
    pub stops: Vec<Stop>,
    /// Whether the driver is ending the session, so the end of its connection is planned.
    pub closing: bool,
    /// Whether the session's connection ended.
    pub ended: bool,
    /// Whether a fill of ours reached the session: in a drop session it lets a stop cancel
    /// (0151 item 2).
    pub fill_of_ours: bool,
    /// The `Link`'s notes and fill refusals read so far.
    read: usize,
    refused: usize,
}

impl Watched {
    /// What a later session of `link`'s registry watches (FBC-77r8: drop session two): nothing
    /// seen yet, the notes and fill refusals of the sessions before it already read, so none of
    /// theirs is taken for its own.
    pub fn after(link: &crate::link::Link) -> Watched {
        Watched {
            read: link.notes().len(),
            refused: link.fill_refusals().len(),
            ..Watched::default()
        }
    }

    /// The first stop the handler raised.
    pub fn stop(&self) -> Option<Stop> {
        self.stops.first().copied()
    }

    fn raise(&mut self, stop: Stop) {
        self.stops.push(stop);
    }
}

/// The stop a resync's report raises: [`Stop::ResyncDesync`] when it compared a position that
/// disagrees with the registry's, [`Stop::ResyncUnsettled`] when a fill across it could not be
/// placed against the seed (decision 0132 item 7), and none otherwise.
pub fn resync_stop(report: &ResyncReport) -> Option<Stop> {
    let desync = (report.checks.iter()).any(|c| matches!(c, PositionCheck::Desync { .. }));
    let unsettled = !report.unsettled.is_empty()
        || (report.checks.iter()).any(|c| matches!(c, PositionCheck::Unsettled(_)));
    match (desync, unsettled) {
        (true, _) => Some(Stop::ResyncDesync),
        (false, true) => Some(Stop::ResyncUnsettled),
        (false, false) => None,
    }
}

/// A drop session's handler: the shared glue's [`LinkHandler`], as `paradex_testnet_acceptance`'s
/// `Tap` wraps it, then what the driver waits on ([`Watched`]): authentication, the arm's
/// outcome, the resync, the connection's close, and the stops the session's events raise. A
/// resync the registry refused or that disagrees is [`Stop::ResyncDesync`] and one with a fill
/// unsettled across it [`Stop::ResyncUnsettled`] ([`resync_stop`]); a replayed fill the ledger
/// refused for its time ([`crate::link::Link::fill_refusals`]) is [`Stop::ReplayRefused`]; a fill
/// of ours is [`Stop::UnexpectedFill`] (0151 item 9: no fill of ours is expected in the drop test);
/// an end of the connection the driver did not ask for is [`Stop::ConnectionEnded`], and ends
/// the session, through its control, with no second attempt (decision 0148 item 7).
pub struct PlaceHandler {
    pub inner: LinkHandler,
    pub watched: Rc<RefCell<Watched>>,
    /// The session's stop control, which the driver's graceful close drops too.
    pub control: Rc<RefCell<Option<ExecControl>>>,
}

impl PlaceHandler {
    /// Reads what the `Link` noted since the last call into [`Watched`], then wakes the driver.
    fn judge(&self) {
        {
            let link = self.inner.link.borrow();
            let mut w = self.watched.borrow_mut();
            let notes = link.notes();
            for note in notes.get(w.read..).unwrap_or_default() {
                match note {
                    Note::Conn(ConnState::Authenticated) => w.authenticated = true,
                    Note::Outcome {
                        outcome,
                        ours: false,
                        ..
                    } if w.arm.is_none() => w.arm = Some(outcome.clone()),
                    Note::Resynced { report, .. } => {
                        w.resynced = true;
                        if let Some(stop) = resync_stop(report) {
                            w.raise(stop);
                        }
                    }
                    Note::ResyncRefused(_) => w.raise(Stop::ResyncDesync),
                    Note::Fill { ours: true, .. } => {
                        w.fill_of_ours = true;
                        w.raise(Stop::UnexpectedFill);
                    }
                    _ => {}
                }
            }
            w.read = notes.len();
            let refusals = link.fill_refusals();
            for refused in refusals.get(w.refused..).unwrap_or_default() {
                let replay = match refused.admission {
                    "Untimed" => Replay::Untimed,
                    _ => Replay::BeyondHorizon,
                };
                w.raise(Stop::ReplayRefused(replay));
            }
            w.refused = refusals.len();
        }
        self.inner.wake.notify_waiters();
    }
}

impl ExecHandler for PlaceHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.inner.on_exec(env);
        self.judge();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.inner.on_epoch_end(key);
        {
            let mut w = self.watched.borrow_mut();
            w.ended = true;
            if !w.closing {
                w.raise(Stop::ConnectionEnded);
            }
        }
        // A place stage's session has one connection attempt: its end ends the session.
        drop(self.control.borrow_mut().take());
        self.judge();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.inner.on_submitted(handle);
        self.judge();
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.inner.on_unprotected(orders);
        self.judge();
    }

    fn on_decode_error(&mut self, key: ConnKey) {
        self.inner.on_decode_error(key);
        self.judge();
    }
}

/// X, drop session one's order (decision 0132 item 4): what became of it, never an id, a price
/// or a size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct XRun {
    /// Whether it was authorized and handed to the session.
    pub submitted: bool,
    /// Whether an order event showed it resting.
    pub acked: bool,
    /// Whether the tee saw an `order_response` `ACK` echoing its client id byte for byte.
    pub exact_echo: bool,
    /// Its request's outcome, as the session reported it, when it did.
    pub outcome: Option<String>,
    /// From its send to the order event showing it resting.
    pub ack_after: Option<Duration>,
    /// Whether the registry held it Open or PartiallyFilled just before the session's close was
    /// sent: what a graceful close could cancel ([`DropRun::close_cancelled_x`]).
    pub rested_at_close: bool,
}

/// A stop's cancels in a drop session (0151 items 2 and 11): Kill, cancel everything by venue
/// id, the wait for each order to end up to `--rpc-timeout-ms`, and the disarm.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cancels {
    /// The cancels handed to the session.
    pub sent: usize,
    /// Whether any was refused locally or held by a provider rate refusal, so not every order
    /// could be seen to end: the session then ends by a graceful close (0151 item 11).
    pub held: bool,
    /// Whether every order the cancels named was seen to end within the wait.
    pub ended: bool,
    /// The owner's presses taken while they ran, each recorded, none ending them early.
    pub presses: u32,
}

/// The reads of X after drop session one (FBC-be57): its rows on `GET /user/historic-orders`
/// and the `get_user_trades` rows since its send, counts and status names alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct XAfter {
    /// The trades rows read since X's send, and those naming X by its venue or client id.
    pub trades: usize,
    pub trades_naming_x: usize,
    /// Whether every `get_user_trades` page since X's send was read, to a short page: what
    /// rules out a fill of X once no row since then is on the account (decision 0132 item 4).
    pub trades_read: bool,
    /// X's `terminal_status` on historic-orders when a row names its client id, as a status
    /// name (capitals, digits and underscores) or `(not a status name)`.
    pub terminal_status: Option<String>,
    /// The member names a `get_user_trades` row read carried, every row's, in order: names
    /// alone, never a value (decision 0132 item 5).
    pub fields: Vec<String>,
    /// Why a read could not be made, when one could not.
    pub error: Option<String>,
}

/// The reads of the drop test's orders after a session (FBC-be57, FBC-77r8): the
/// `get_user_trades` rows since X's send and `GET /user/historic-orders`, for X alone after drop
/// session one and for X then Y after the drop, counts and status names alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Traded {
    /// The trades rows read since X's send.
    pub trades: usize,
    /// The rows naming each order read, in order, by its venue or client id.
    pub naming: Vec<usize>,
    /// Each order's `terminal_status` on historic-orders when a row names its client id, as a
    /// status name ([`crate::verdict::status_name`]).
    pub terminal: Vec<Option<String>>,
    /// The member names a trades row read carried, every row's, in order.
    pub fields: Vec<String>,
    /// Whether every trades page was read, and every historic-orders page.
    pub trades_whole: bool,
    pub historic_whole: bool,
    /// Why a read could not be made, when one could not.
    pub error: Option<String>,
}

impl Traded {
    /// What it read of X, the first order, as drop session one's reads report it.
    pub fn x_after(&self) -> XAfter {
        XAfter {
            trades: self.trades,
            trades_naming_x: self.naming.first().copied().unwrap_or(0),
            trades_read: self.trades_whole,
            terminal_status: self.terminal.first().cloned().flatten(),
            fields: self.fields.clone(),
            error: self.error.clone(),
        }
    }
}

/// `member` as the report keeps a member name a `get_user_trades` row carries: a name of lower
/// case letters, digits and underscores, at most 32, or `(not a member name)`, so no other text
/// QFEX writes there reaches an output.
pub fn member_name(member: &str) -> String {
    let named = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_';
    match (1..=32).contains(&member.len()) && member.chars().all(named) {
        true => member.to_owned(),
        false => "(not a member name)".to_owned(),
    }
}

/// Drop session two (FBC-77r8; decision 0132 item 4), as the report and the evidence show it:
/// names, flags, counts and times alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TwoRun {
    pub authenticated: bool,
    /// The arm's outcome, as the session reported it.
    pub arm: Option<String>,
    pub resynced: bool,
    /// Whether an `order_response` naming X reported it `CANCELLED` in the arm's window: from
    /// the session's opening to the end of the read before the drop, since no report settles
    /// nothing until that read and nothing of ours cancels in the drop test (decision 0132 item
    /// 4).
    pub x_cancel_reported: bool,
    /// Whether an `order_response` naming X reported it `CANCELLED` in the session before the
    /// drop: then the drop did not end X, and its reach over X is not measured (decision 0132
    /// item 4).
    pub x_cancelled_before_drop: bool,
    /// Whether Y was still open in the registry as the connection dropped, with no `CANCELLED`
    /// of it heard in the session: the drop then had Y of its own to cancel (0151 item 13).
    /// Both are measured only once the session reached its drop: before it, each stays false,
    /// which the report and the evidence say is not measured.
    pub y_open_at_drop: bool,
    pub y: XRun,
    /// What the read before the drop listed, X then Y, on a socket that never arms; `None` when
    /// it was not made.
    pub before_drop: Option<[bool; 2]>,
    /// Whether that read showed what the streams did not: an order or a TWAP that is neither X
    /// nor Y, or a position the registry does not hold, a stop before the drop (decision 0148
    /// item 8, FBC-77r8's review).
    pub before_drop_disagrees: bool,
    /// The stop's cancels, when a stop that cancels in a drop session came (0151 item 2).
    pub cancels: Option<Cancels>,
    /// Each comparison of the streams with the reads: after the stop's cancels alone.
    pub compared: Vec<crate::compare::Compared>,
    /// How the session ended: the abrupt drop, a graceful close after a stop, or its
    /// connection's own end.
    pub ended_by: Option<&'static str>,
    /// The reads after the drop, until X and Y are absent or the wait ends (0151 item 4).
    pub after_drop: Option<crate::verdict::AfterDrop>,
    /// X's and Y's trades and historic-orders rows after the drop, or after drop session two
    /// failed with no stop, Y's once it was submitted.
    pub after: Option<Traded>,
    /// Whether an order rested on the account once the session ended, as the read of every
    /// named account after it showed, for the owner's hand cancel (decision 0132 item 7).
    pub resting: bool,
    /// Whether the account was read after the session.
    pub read: bool,
    /// Whether that read of X's account listed X, by its venue id once known: X resting when the
    /// run ended, for the owner's hand cancel (decision 0132 item 7).
    pub x_resting: bool,
    /// Whether X's account was read after the session on a page that listed X or was its last,
    /// so [`TwoRun::x_resting`] is known.
    pub x_read: bool,
    /// Why the session could not run its course, when that was no stop: never an id or value.
    pub error: Option<String>,
    /// How QFEX gave back our client id on the session, frame by frame ([`DropRun::echoes`]).
    pub echoes: Vec<String>,
    /// Whether Y's `add_order` went out as encoded (FBC-blcw); `None` when none was sent.
    pub as_encoded: Option<bool>,
}

/// One named account's drop test (FBC-23ch, FBC-77r8; decision 0132 item 4), as the report and
/// the evidence show it: drop session one's items here, drop session two's in
/// [`DropRun::two`], and the verdict, judged in this run on this account alone and kept in
/// memory (decision 0148 item 2). Names, flags, counts and times alone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DropRun {
    /// The account it ran on, as the configuration names it.
    pub account: &'static str,
    pub authenticated: bool,
    /// The arm's outcome, as the session reported it: what answers the arm.
    pub arm: Option<String>,
    pub resynced: bool,
    /// Whether the market was Started, under its market lease.
    pub started: bool,
    pub x: XRun,
    /// The stop's cancels, when a stop that cancels in a drop session came (0151 item 2).
    pub cancels: Option<Cancels>,
    /// Each comparison of the streams with the reads (0151 item 4).
    pub compared: Vec<crate::compare::Compared>,
    /// How the session ended: a graceful close, or its connection's own end.
    pub ended_by: Option<&'static str>,
    /// Whether the graceful close cancelled X ([`close_verdict`]): the read of X's own account
    /// after it no longer listing X, which rested before it with no cancel of ours, that account
    /// flat, and X's trades since its send read whole with no row since then (decision 0132 item
    /// 4); `None` when not measured, [`DropRun::close_not_measured`] saying why.
    pub close_cancelled_x: Option<bool>,
    /// Why [`DropRun::close_cancelled_x`] was not measured, when it was not.
    pub close_not_measured: Option<CloseUnmeasured>,
    /// Whether X still rested once the session ended, for the owner's hand cancel (decision
    /// 0132 item 7).
    pub x_resting: bool,
    /// Whether X's account was read after the session, so [`DropRun::x_resting`] is known.
    pub x_read: bool,
    /// The reads of X and its trades after the session.
    pub after: Option<XAfter>,
    /// Why the session could not run its course, when that was no stop: never an id or value.
    pub error: Option<String>,
    /// How each `order_response`, `fill_response` and echo of an `add_order` QFEX sent on the
    /// session gave back our client id, in arrival order, as the tee read it: a frame's kind and
    /// status name, and the client id's verdict, never the id ([`crate::tee`]).
    pub echoes: Vec<String>,
    /// Whether every `add_order` the session sent went out as encoded, carrying none of
    /// `take_profit`, `stop_loss` and `reduce_only` (FBC-blcw); `None` when none was sent.
    pub as_encoded: Option<bool>,
    /// Whether the read before drop session two listed X on its account: X open as that session
    /// armed; `None` when that read was not made.
    pub x_before_two: Option<bool>,
    /// Drop session two, once it was built.
    pub two: Option<TwoRun>,
    /// The drop test's verdict, once the reads after the drop were made (decision 0132 item 4,
    /// 0151 item 13).
    pub verdict: Option<crate::verdict::DropVerdict>,
    /// Whether every read of the other named accounts during this drop test was clean (no open
    /// order, no TWAP, flat), as [`OthersClean`] says.
    pub others_clean: OthersClean,
    /// The run's stop, by name, when one came during this drop test: nothing more is placed.
    pub stop: Option<&'static str>,
}

/// Whether the other named accounts stayed clean through a drop test (FBC-77r8), from the reads
/// of them made and those that could not be.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OthersClean {
    /// The configuration names no other account.
    #[default]
    NoneNamed,
    /// Every read of them was made, and each was clean.
    Clean,
    /// A read of one was not clean.
    NotClean,
    /// Every read made was clean, but a read of one could not be made, so whether they stayed
    /// clean is not known (FBC-77r8's review).
    NotKnown,
}

impl OthersClean {
    /// It, from `clean`, whether every read made was clean (`None` when no other account is
    /// named, as `stage::others_clean` gives it), and whether a read of one could not be made.
    pub fn of(clean: Option<bool>, unread: bool) -> OthersClean {
        match (clean, unread) {
            (None, _) => OthersClean::NoneNamed,
            (Some(false), _) => OthersClean::NotClean,
            (Some(true), true) => OthersClean::NotKnown,
            (Some(true), false) => OthersClean::Clean,
        }
    }

    /// As the evidence keeps it: `true` or `false`, `null` when no other account is named, and
    /// `"not known"`.
    fn to_json(self) -> Value {
        match self {
            OthersClean::NoneNamed => Value::Null,
            OthersClean::Clean => Value::Bool(true),
            OthersClean::NotClean => Value::Bool(false),
            OthersClean::NotKnown => Value::String("not known".to_owned()),
        }
    }
}

/// Why the graceful close's cancel of X was not measured ([`close_verdict`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CloseUnmeasured {
    /// The session's connection ended first ([`crate::stage::CONNECTION_ENDED`]), so no close
    /// was sent.
    NotClosed,
    /// The shared budget had no room for the close ([`crate::stage::NO_ROOM_TO_CLOSE`]), so it
    /// may not have been graceful (0151 item 11).
    NoRoomToClose,
    /// The session did not reach its close: it failed before it, or did not open.
    NoClose,
    /// A stop that cancels came (0151 item 2), its cancels sent, held or refused, so the close
    /// was not all that could end X.
    CancelStop,
    /// X was not held Open or PartiallyFilled as the close was sent.
    NotRested,
    /// X's account was not read after the session, or its page was full and did not list X.
    XUnread,
    /// X was absent, and a `get_user_trades` row names it: a fill of X, whole or in part.
    XTraded,
    /// X was absent, and a `get_user_trades` row since its send names another order or none,
    /// so a trade decision 0132 item 4 rules out came, or a fill of X its row does not name.
    OtherTrade,
    /// X was absent, and its account's position was not flat after the session.
    XNotFlat,
    /// X was absent, but its trades since its send were not read whole.
    TradesUnread,
}

impl CloseUnmeasured {
    /// Why, in the report's and the evidence's words.
    pub fn says(self) -> &'static str {
        match self {
            CloseUnmeasured::NotClosed => "the session's connection ended before a close was sent",
            CloseUnmeasured::NoRoomToClose => {
                "the shared budget had no room for the close, which may not have been graceful \
                 (0151 item 11)"
            }
            CloseUnmeasured::NoClose => "the session did not reach its close",
            CloseUnmeasured::CancelStop => {
                "a stop that cancels came before the close (0151 item 2), its cancels sent, held \
                 or refused"
            }
            CloseUnmeasured::NotRested => "X did not rest as the close was sent",
            CloseUnmeasured::XUnread => {
                "X's account was not read after it, or its page was full and did not list X"
            }
            CloseUnmeasured::XTraded => {
                "the read after it no longer listed X, but a get_user_trades row names X: a fill \
                 of X was seen, so its absence is not a cancel (decision 0132 item 4)"
            }
            CloseUnmeasured::OtherTrade => {
                "the read after it no longer listed X, but a get_user_trades row since X's send \
                 names another order or none: something traded on the account, so a fill of X \
                 is not ruled out (decision 0132 item 4)"
            }
            CloseUnmeasured::XNotFlat => {
                "the read after it no longer listed X, but X's account was not flat after it: \
                 something traded on the account, so a fill of X is not ruled out (decision 0132 \
                 item 4)"
            }
            CloseUnmeasured::TradesUnread => {
                "the read after it no longer listed X, but X's trades since its send were not \
                 read whole, so a fill of X is not ruled out (decision 0132 item 4)"
            }
        }
    }
}

/// The read of X's own account after drop session one, as [`close_verdict`] takes it.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct XAccountRead {
    /// Whether it was read, on a page that listed X or was its last.
    pub read: bool,
    /// Whether its page listed X.
    pub listed: bool,
    /// Whether its position was flat.
    pub flat: bool,
}

/// Whether drop session one's graceful close cancelled X (decision 0132 item 4), from `run` as
/// the session left it and `x`, the read of X's own account after it. Not measured, and why,
/// unless the session ended by a graceful close, with no stop that cancels before it, X rested
/// as the close was sent, and X's account was read. Then X listed was not cancelled by it, and
/// X absent was only when nothing else could have ended it: X's account flat, and X's trades
/// since its send read whole with no row on the account since then. A row naming X, a part
/// fill included, is a fill, not the close's cancel; any other row, or a position that moved,
/// is a trade 0132 item 4 rules out, which leaves the cause of X's absence unknown.
pub fn close_verdict(run: &DropRun, x: XAccountRead) -> Result<bool, CloseUnmeasured> {
    use crate::stage::{CONNECTION_ENDED, GRACEFUL, NO_ROOM_TO_CLOSE};
    match run.ended_by {
        Some(GRACEFUL) => {}
        Some(CONNECTION_ENDED) => return Err(CloseUnmeasured::NotClosed),
        Some(NO_ROOM_TO_CLOSE) => return Err(CloseUnmeasured::NoRoomToClose),
        _ => return Err(CloseUnmeasured::NoClose),
    }
    if run.cancels.is_some() {
        return Err(CloseUnmeasured::CancelStop);
    }
    if !run.x.rested_at_close {
        return Err(CloseUnmeasured::NotRested);
    }
    if !x.read {
        return Err(CloseUnmeasured::XUnread);
    }
    if x.listed {
        return Ok(false);
    }
    match &run.after {
        Some(a) if a.trades_naming_x > 0 => Err(CloseUnmeasured::XTraded),
        Some(a) if a.trades > a.trades_naming_x => Err(CloseUnmeasured::OtherTrade),
        _ if !x.flat => Err(CloseUnmeasured::XNotFlat),
        Some(a) if a.trades_read => Ok(true),
        _ => Err(CloseUnmeasured::TradesUnread),
    }
}

/// How each frame the tee heard from its `from`th on gave back our client id
/// ([`DropRun::echoes`]).
pub fn echoes(kept: &crate::tee::Kept, from: usize) -> Vec<String> {
    use crate::tee::{EchoCid, Heard};
    let echo = |cid: EchoCid| match cid {
        EchoCid::Exact => "exact",
        EchoCid::Differs => "not byte for byte",
        EchoCid::Absent => "absent",
    };
    (kept.heard.iter().skip(from))
        .map(|h| match h {
            Heard::Order { status, cid, .. } => format!("order_response {status}: {}", cid.name()),
            Heard::Fill { cid, .. } => format!("fill_response: {}", cid.name()),
            Heard::Echo { envelope, cid, .. } => format!("{envelope} of add_order: {}", echo(*cid)),
        })
        .collect()
}

/// Whether every `add_order` in `kept` from its `from`th on went out as encoded
/// ([`DropRun::as_encoded`]).
pub fn as_encoded(kept: &crate::tee::Kept, from: usize) -> Option<bool> {
    let sent = kept.sent().get(from..).unwrap_or_default();
    let carried = |s: &crate::tee::Sent| s.take_profit || s.stop_loss || s.reduce_only;
    (!sent.is_empty()).then(|| !sent.iter().any(carried))
}

/// What the report says of X resting after drop session one, for the owner (decision 0132 item
/// 7, on production's site as decision 0143 item 4 reads it).
pub const HAND_CANCEL: &str = "X still rests on the account: cancel it by hand on QFEX \
     production's site, then give the next place-stage run --reconciled hand-cancel (0151 item \
     6); the order is named by none of its ids here";

/// What a comparison came to, as the report and the evidence name it.
fn ended(e: crate::compare::Ended) -> &'static str {
    use crate::compare::Ended;
    match e {
        Ended::Agreed => "agreed",
        Ended::Lagged => "lagged: the last read still missed an order of ours",
        Ended::Disagreed => "disagreed: a stop (decision 0148 item 8)",
        Ended::Unread => "unread: no read was made",
        Ended::Refused => "refused: a provider rate refusal, a stop",
    }
}

/// What a session's request outcome is, as the report names it: its kind alone.
pub fn outcome_name(outcome: &SubmitOutcome) -> String {
    variant_name(outcome)
}

/// The name of the variant `value` is, as its `Debug` begins, and nothing of what it carries:
/// how a refusal of fbc-oms or the runtime reaches the report, the evidence and standard
/// output, since one may carry a client id, a cap or an amount (0151 item 10).
pub fn variant_name(value: &impl std::fmt::Debug) -> String {
    let named = format!("{value:?}");
    let end = named.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'));
    named[..end.unwrap_or(named.len())].to_owned()
}

/// What the report says of an order of ours, X or Y, resting once drop session two ended
/// short of its drop or after it, for the owner (decision 0132 item 7, on production's site as
/// decision 0143 item 4 reads it).
pub const HAND_CANCEL_ORDERS: &str = "an order rests on the account after drop session two: \
     cancel every order of ours (X, Y) by hand on QFEX production's site, then give the next \
     place-stage run --reconciled hand-cancel (0151 item 6); no order is named by its ids here";

/// What follows a drop test that did not stop the run: the next named account's.
pub const NEXT_ACCOUNT: &str = "the next named account's drop test, which starts only after \
     every named account reads clean again";

/// What follows the last named account's drop test that did not stop the run: the run's end.
pub const LAST_ACCOUNT: &str = "the run's end: stage two is not built yet (FBC-su9i builds it)";

/// What the report says of a fill a drop test's reads after the drop show (0151 item 9).
pub const OWNERS_POSITION: &str = "the run stops not flat, and the position is the owner's \
     (0151 item 9)";

/// What the report says of a fill a trades row after the drop names when the run stopped on a
/// stop before it in the verdict's order, a provider rate refusal (0151 item 9, decision 0148
/// item 8).
pub const OWNERS_POSITION_BESIDE: &str = "a get_user_trades row names X or Y, so the position \
     is the owner's (0151 item 9), though the run stopped first on the stop below";

fn yn(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

fn ms(d: Duration) -> String {
    format!("{} ms", d.as_millis())
}

/// The report's section on one named account's drop test: what it answers (decision 0148 item
/// 10), then drop session one, then drop session two. `last` is whether its account is the last
/// named.
pub fn drop_markdown(s: &mut String, run: &DropRun, last: bool) {
    let two = run.two.as_ref();
    let arm_two = two
        .and_then(|t| t.arm.as_deref())
        .unwrap_or("none reported");
    let enabling = match (&run.verdict, two) {
        (Some(v), _) => v.arm.says(),
        (None, Some(t)) if t.x_cancel_reported => {
            "yes, reported: a CANCELLED of X came in the arm's window".to_owned()
        }
        _ => "not measured: drop session two did not reach its drop".to_owned(),
    };
    let verdict = match &run.verdict {
        Some(v) => v.says(),
        None => "not judged: the drop test ended before its drop (decision 0132 item 4), so it \
                 did not pass"
            .to_owned(),
    };
    let reach = run.verdict.as_ref().map_or_else(
        || "not measured: the drop test ended before its drop".to_owned(),
        |v| v.reach.says(),
    );
    // A trades row naming X or Y is a fill of ours whatever stop came first (0151 item 9), so
    // the position is said to be the owner's from the verdict's failures, not from the stop's
    // name alone (FBC-77r8's review).
    let traded = run.verdict.as_ref().is_some_and(|v| {
        (v.failures.iter()).any(|f| matches!(f, crate::verdict::Failure::Traded(_)))
    });
    let fill = match run.stop {
        Some("UnexpectedFill" | "NotFlat") if run.verdict.is_some() => {
            format!("; {OWNERS_POSITION}")
        }
        _ if traded => format!("; {OWNERS_POSITION_BESIDE}"),
        _ => String::new(),
    };
    let others = match run.others_clean {
        OthersClean::NoneNamed => "no other account is named",
        OthersClean::Clean => "yes: every read of every other named account was clean",
        OthersClean::NotClean => {
            "no: a read of another named account was not clean (the reads below)"
        }
        OthersClean::NotKnown => {
            "not known: every read of another named account made was clean, but one could not \
             be made (the errors below)"
        }
    };
    // A status any read showed; otherwise "not listed" only when the last read of
    // historic-orders that could list it was read whole, and "not read" when it was not, or
    // none was made (FBC-77r8's review): Y only after drop session two, X after either session.
    let terminal = |n: usize| {
        let after = two.and_then(|t| t.after.as_ref());
        let status = after.and_then(|a| a.terminal.get(n).cloned().flatten());
        let one = run.after.as_ref().and_then(|a| a.terminal_status.clone());
        let status = if n == 0 { status.or(one) } else { status };
        let whole = match (after, n) {
            (Some(a), _) => a.historic_whole,
            (None, 0) => run.after.as_ref().is_some_and(|a| a.error.is_none()),
            (None, _) => false,
        };
        status.unwrap_or_else(|| match whole {
            true => "not listed".to_owned(),
            false => "not read".to_owned(),
        })
    };
    let mut fields: Vec<&str> = Vec::new();
    let read = run.after.iter().flat_map(|a| a.fields.iter());
    let read = read.chain(
        two.and_then(|t| t.after.as_ref())
            .into_iter()
            .flat_map(|a| &a.fields),
    );
    for field in read {
        if !fields.contains(&field.as_str()) {
            fields.push(field);
        }
    }
    let fields = match fields.is_empty() {
        true => "not shown: no row was read".to_owned(),
        false => fields.join(", "),
    };
    let followed = match (run.stop, last) {
        (Some(stop), _) => format!(
            "the run's stop, {stop}: nothing more is placed on this account, and no later named \
             account's drop test runs (decision 0132 item 7)"
        ),
        (None, false) => NEXT_ACCOUNT.to_owned(),
        (None, true) => LAST_ACCOUNT.to_owned(),
    };
    let _ = writeln!(
        s,
        "## The drop test on {} (FBC-23ch and FBC-77r8, decision 0132 item 4)\n\n\
         On {} (named by the run's configuration), after every named account before it. Its \
         verdict is judged in this run on this account's reads alone and kept in memory, never \
         carried to another account or run (decision 0148 item 2).\n\n\
         - What answers the arm: drop session one {}; drop session two {arm_two}\n\
         - Whether enabling cancel-on-disconnect cancels open orders, and whether QFEX reports \
           it: {enabling}\n\
         - The drop test: {verdict}{fill}\n\
         - The drop's reach over X, an order of an earlier connection: {reach}\n\
         - Whether a graceful close cancels: {}\n\
         - Whether the other named accounts stayed clean: {others}\n\
         - terminal_status on GET /user/historic-orders: X {}; Y {}\n\
         - The members a get_user_trades row carries: {fields}\n\
         - What followed: {followed}\n",
        run.account,
        run.account,
        run.arm.as_deref().unwrap_or("none reported"),
        close_said(run),
        terminal(0),
        terminal(1),
    );
    one_markdown(s, run);
    if let Some(two) = two {
        two_markdown(s, run, two);
    }
}

/// Whether drop session one's graceful close cancelled X ([`close_verdict`]), as the report
/// says it: not measured with its reason when it was not.
fn close_said(run: &DropRun) -> String {
    match (run.close_cancelled_x, run.close_not_measured) {
        (Some(true), _) => "yes: X rested before it, no cancel of ours was sent, and the read \
             after it no longer listed X, with X's account flat after it and X's trades since \
             its send read whole, no row since then on the account (decision 0132 item 4)"
            .to_owned(),
        (Some(false), _) => "no: X rested before it and still did after it".to_owned(),
        (None, why) => format!(
            "not measured: {}",
            why.map_or("the session did not reach its close", CloseUnmeasured::says)
        ),
    }
}

/// What a stop's cancels did, as the report says it.
fn cancels_said(cancels: Option<&Cancels>) -> String {
    match cancels {
        None => "none: no stop that cancels in a drop session came (0151 item 2)".to_owned(),
        Some(c) => format!(
            "Kill, then {} cancel{} by venue id; {}; {} press{} of Ctrl-C while they ran, each \
             recorded; then the market disarmed and a graceful close (0151 items 2 and 11)",
            c.sent,
            if c.sent == 1 { "" } else { "s" },
            match (c.held, c.ended) {
                (true, _) => "held or refused, so not every order was seen to end",
                (false, true) => "every order they named was seen to end",
                (false, false) => "not every order they named was seen to end in time",
            },
            c.presses,
            if c.presses == 1 { "" } else { "es" }
        ),
    }
}

/// An order's request, as the report says it: whether it was handed to the session, shown
/// resting, echoed byte for byte, its outcome and its time to its ACK.
fn placed_said(x: &XRun) -> String {
    format!(
        "handed to the session {}; shown resting by an order event {}; an ACK echoing its \
         client id byte for byte {}; its request's outcome {}; from its send to its ACK {}",
        yn(x.submitted),
        yn(x.acked),
        yn(x.exact_echo),
        x.outcome.as_deref().unwrap_or("none reported"),
        x.ack_after.map_or("not measured".to_owned(), ms),
    )
}

/// The frames' client ids as QFEX gave them back, as the report says them.
fn echoes_said(echoes: &[String]) -> String {
    match echoes.is_empty() {
        true => "none heard".to_owned(),
        false => echoes.join("; "),
    }
}

/// The comparisons' table.
fn compared_markdown(s: &mut String, compared: &[crate::compare::Compared]) {
    let _ = writeln!(
        s,
        "The comparisons of the streams with the reads (0151 item 4: a read at once, then each \
         --read-interval-ms after the last began, at most 10 within --agree-wait-ms):\n\n\
         | after | reads | first agreeing read after the step | ended |\n\
         | --- | --- | --- | --- |"
    );
    for c in compared {
        let first = c.first_agreed.map_or("none".to_owned(), ms);
        let _ = writeln!(
            s,
            "| {} | {} | {first} | {} |",
            c.step,
            c.reads.len(),
            ended(c.ended)
        );
    }
    let _ = writeln!(s);
}

/// The report's section on drop session one.
fn one_markdown(s: &mut String, run: &DropRun) {
    let arm = run.arm.as_deref().unwrap_or("none reported");
    let after = match &run.after {
        None => "not read".to_owned(),
        Some(a) => format!(
            "{} trade rows since X's send{}, {} naming X; X on GET /user/historic-orders: {}{}",
            a.trades,
            match a.trades_read {
                true => ", read whole",
                false => ", not read whole",
            },
            a.trades_naming_x,
            // Not listed only on a read that ended; a failed one, or none, says nothing of X.
            (a.terminal_status.as_deref()).unwrap_or(match a.error {
                Some(_) => "not known",
                None => "not listed",
            }),
            a.error
                .as_ref()
                .map_or(String::new(), |e| format!("; a read failed: {e}"))
        ),
    };
    let _ = writeln!(
        s,
        "### Drop session one (FBC-23ch)\n\n\
         Until an ACK echoes our client id, a session places at most one post-only GTC far from \
         the touch (decision 0132 item 7): X, at the instrument's minimum size, --away-bps \
         behind the touch (a fence met; no price or size is shown).\n\n\
         - Stream authenticated: {}; what answered the arm: {arm}; resync applied: {}; market \
           Started under its market lease: {}\n\
         - X: {}\n\
         - The stop's cancels: {}\n\
         - Ended by: {}\n\
         - Whether the graceful close cancelled X: {}\n\
         - X and its trades after the session: {after}\n\
         - X resting when the run ended: {}\n\
         - Our client id as QFEX gave it back, frame by frame (decision 0148 item 2; never the \
           id): {}\n\
         - add_order sent as encoded, with no take_profit, stop_loss or reduce_only (FBC-blcw): \
           {}\n\
         - Error: {}\n",
        yn(run.authenticated),
        yn(run.resynced),
        yn(run.started),
        placed_said(&run.x),
        cancels_said(run.cancels.as_ref()),
        run.ended_by.unwrap_or("it did not open"),
        close_said(run),
        x_at_end(run),
        echoes_said(&run.echoes),
        run.as_encoded.map_or("none sent", yn),
        run.error.as_deref().unwrap_or("none"),
    );
    compared_markdown(s, &run.compared);
}

/// Whether X rested when the run ended, as the report says it: from the read of X's account
/// after drop session two once that session was built, and otherwise after drop session one;
/// never from a session having been built (FBC-77r8's review).
fn x_at_end(run: &DropRun) -> String {
    if let Some(two) = &run.two {
        return match (two.x_resting, two.x_read) {
            (true, _) => format!(
                "yes, shown by the read of X's account after drop session two: {HAND_CANCEL}"
            ),
            (false, true) => {
                "no: the read of X's account after drop session two no longer listed it".to_owned()
            }
            (false, false) => "not known: X's account was not read after drop session two, or \
                               its page was full and did not list X; read it on QFEX's site \
                               before the next run"
                .to_owned(),
        };
    }
    match (run.x_resting, run.x.submitted, run.x_read) {
        (true, _, _) => HAND_CANCEL,
        (false, false, _) => "no: X was not placed",
        (false, true, true) => "no",
        (false, true, false) => {
            "not known: the account was not read after the session; read it on QFEX's site \
             before the next run"
        }
    }
    .to_owned()
}

/// The report's section on drop session two (FBC-77r8).
fn two_markdown(s: &mut String, run: &DropRun, two: &TwoRun) {
    let before_two = match run.x_before_two {
        Some(true) => "yes: X open on the account as drop session two armed",
        Some(false) => "no: X no longer open on the account",
        None => "not read",
    };
    let before_drop = match two.before_drop {
        Some([x, y]) => format!(
            "X listed {}; Y listed {}{}",
            yn(x),
            yn(y),
            match two.before_drop_disagrees {
                true =>
                    "; it showed an order, a TWAP or a position the streams did not, a stop \
                         before the drop (decision 0148 item 8)",
                false => "",
            }
        ),
        None => "not made".to_owned(),
    };
    let after_drop = match &two.after_drop {
        None => "not made: the session ended short of its drop".to_owned(),
        Some(a) => format!(
            "{} reads; {}{}",
            a.reads.len(),
            a.absent_after
                .map_or("none listed neither X nor Y".to_owned(), |d| {
                    format!(
                        "the first listing neither X nor Y ended {} after they began",
                        ms(d)
                    )
                }),
            match a.refused {
                Some(_) => "; cut short by a provider rate refusal (decision 0148 item 8)",
                None => "",
            }
        ),
    };
    // Read with no reads after the drop: after drop session two failed with no stop.
    let traded_when = match (&two.after_drop, &two.after) {
        (None, Some(_)) => "the session",
        _ => "the drop",
    };
    let after = match &two.after {
        None => "not read".to_owned(),
        Some(a) => format!(
            "{} trade rows since X's send, {} naming X, {} naming Y{}",
            a.trades,
            a.naming.first().copied().unwrap_or(0),
            a.naming.get(1).copied().unwrap_or(0),
            a.error
                .as_ref()
                .map_or(String::new(), |e| format!("; not read whole: {e}"))
        ),
    };
    // What the session showed of X and Y as it dropped, measured only once it reached its drop
    // (FBC-77r8's review).
    let dropped = two.ended_by == Some(crate::stage::DROPPED);
    let at_drop = |b: bool| match dropped {
        true => yn(b),
        false => "not measured: the session did not reach its drop",
    };
    let resting = match (two.resting, two.read) {
        (true, _) => HAND_CANCEL_ORDERS,
        (false, true) => "no",
        (false, false) => {
            "not known: the account was not read after the session; read it on QFEX's site \
             before the next run"
        }
    };
    let _ = writeln!(
        s,
        "### Drop session two (FBC-77r8)\n\n\
         After every named account read clean but for X, which may rest alone on this account as \
         one open order on its symbol (decision 0132 item 7, 0144 item 4): the same registry, \
         seeded nothing (decision 0148 item 3); Y, a post-only GTC at the instrument's minimum \
         size --away-bps behind the touch, once the arm was accepted; the read before the drop, \
         on a socket that never arms; then the abrupt drop, with no WebSocket close. From the \
         first session's arm to the last read, no cancel of X or Y, no reconnect and no later \
         arm (decision 0132 item 4).\n\n\
         - X listed by the read before drop session two: {before_two}\n\
         - Stream authenticated: {}; what answered the arm: {}; resync applied: {}\n\
         - A CANCELLED of X reported in the arm's window: {}\n\
         - Y: {}\n\
         - The read before the drop: {before_drop}\n\
         - A CANCELLED of X before the drop: {}; Y open as the connection dropped: {}\n\
         - The stop's cancels: {}\n\
         - Ended by: {}\n\
         - The reads after the drop: {after_drop}\n\
         - X's and Y's trades after {traded_when}: {after}\n\
         - An order resting on the account when the run ended: {resting}\n\
         - Our client id as QFEX gave it back, frame by frame: {}\n\
         - add_order sent as encoded (FBC-blcw): {}\n\
         - Error: {}\n",
        yn(two.authenticated),
        two.arm.as_deref().unwrap_or("none reported"),
        yn(two.resynced),
        yn(two.x_cancel_reported),
        placed_said(&two.y),
        at_drop(two.x_cancelled_before_drop),
        at_drop(two.y_open_at_drop),
        cancels_said(two.cancels.as_ref()),
        two.ended_by.unwrap_or("it did not open"),
        echoes_said(&two.echoes),
        two.as_encoded.map_or("none sent", yn),
        two.error.as_deref().unwrap_or("none"),
    );
    if !two.compared.is_empty() {
        compared_markdown(s, &two.compared);
    }
}

/// A comparison, as the evidence keeps it.
fn compared_json(c: &crate::compare::Compared) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let reads: Vec<Value> = (c.reads.iter())
        .map(|(at, took)| json!({"began_ms": ms(*at), "took": format!("{took:?}")}))
        .collect();
    json!({"after": c.step, "reads": reads,
        "first_agreed_ms": c.first_agreed.map(ms), "ended": ended(c.ended),
        "stop": c.stop.map(Stop::name)})
}

/// A stop's cancels, as the evidence keeps them.
fn cancels_json(c: Option<&Cancels>) -> Value {
    json!(
        c.map(|c| json!({"sent": c.sent, "held": c.held, "ended": c.ended,
        "presses": c.presses}))
    )
}

/// An order's request, as the evidence keeps it.
fn placed_json(x: &XRun) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    json!({"submitted": x.submitted, "acked": x.acked, "exact_echo": x.exact_echo,
        "outcome": x.outcome, "ack_after_ms": x.ack_after.map(ms)})
}

/// The evidence's entry for one named account's drop test: drop session one's items under
/// `session_one`, drop session two's under `session_two`, and the verdict.
pub fn drop_to_json(run: &DropRun) -> Value {
    let compared: Vec<Value> = run.compared.iter().map(compared_json).collect();
    let after = run.after.as_ref().map(|a| {
        json!({"trades": a.trades, "trades_naming_x": a.trades_naming_x,
            "trades_read": a.trades_read, "terminal_status": a.terminal_status,
            "trade_fields": a.fields, "error": a.error})
    });
    let one = json!({"account": run.account, "authenticated": run.authenticated,
        "arm": run.arm, "resynced": run.resynced, "started": run.started,
        "x": placed_json(&run.x),
        "cancels": cancels_json(run.cancels.as_ref()), "compared": compared,
        "ended_by": run.ended_by,
        "close_cancelled_x": run.close_cancelled_x,
        "close_not_measured": run.close_not_measured.map(CloseUnmeasured::says),
        "x_resting": run.x_resting,
        "x_read_after": run.x_read,
        "hand_cancel": (run.x_resting && run.two.is_none()).then_some(HAND_CANCEL),
        "after": after,
        "echoes": run.echoes, "add_order_as_encoded": run.as_encoded, "error": run.error});
    let two = run.two.as_ref().map(two_to_json);
    let verdict = run.verdict.as_ref().map(verdict_to_json);
    json!({"account": run.account, "session_one": one, "x_before_two": run.x_before_two,
        "session_two": two, "verdict": verdict, "others_clean": run.others_clean.to_json(),
        "stop": run.stop})
}

/// Drop session two's entry.
fn two_to_json(two: &TwoRun) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let compared: Vec<Value> = two.compared.iter().map(compared_json).collect();
    // Measured only once the session reached its drop: `null` otherwise.
    let dropped = two.ended_by == Some(crate::stage::DROPPED);
    let after_drop = two.after_drop.as_ref().map(|a| {
        let reads: Vec<Value> = (a.reads.iter())
            .map(|(at, shown)| json!({"began_ms": ms(*at), "shown": format!("{shown:?}")}))
            .collect();
        json!({"reads": reads, "absent_after_ms": a.absent_after.map(ms),
            "refused": a.refused.map(|on| Stop::RateRefused { on }.name()),
            "others": a.others})
    });
    let after = two.after.as_ref().map(|a| {
        json!({"trades": a.trades, "naming": a.naming, "terminal_status": a.terminal,
            "trade_fields": a.fields, "trades_whole": a.trades_whole,
            "historic_whole": a.historic_whole, "error": a.error})
    });
    json!({"authenticated": two.authenticated, "arm": two.arm, "resynced": two.resynced,
        "x_cancel_reported": two.x_cancel_reported,
        "x_cancelled_before_drop": dropped.then_some(two.x_cancelled_before_drop),
        "y_open_at_drop": dropped.then_some(two.y_open_at_drop), "y": placed_json(&two.y),
        "before_drop": two.before_drop.map(|[x, y]| json!({"x_listed": x, "y_listed": y})),
        "before_drop_disagrees": two.before_drop_disagrees,
        "cancels": cancels_json(two.cancels.as_ref()), "compared": compared,
        "ended_by": two.ended_by, "after_drop": after_drop, "after": after,
        "resting": two.resting, "read_after": two.read,
        "x_resting": two.x_resting, "x_read_after": two.x_read,
        "hand_cancel": two.resting.then_some(HAND_CANCEL_ORDERS),
        "echoes": two.echoes, "add_order_as_encoded": two.as_encoded, "error": two.error})
}

/// The verdict's entry: names and flags alone.
fn verdict_to_json(v: &crate::verdict::DropVerdict) -> Value {
    let failures: Vec<String> = v.failures.iter().map(|f| f.says()).collect();
    json!({"passed": v.passed, "failures": failures, "reach_over_x": v.reach.says(),
        "arm_cancels_open_orders": v.arm.says(), "terminal_status": v.terminal,
        "stop": v.stop().map(Stop::name)})
}
