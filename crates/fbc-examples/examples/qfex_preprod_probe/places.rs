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
//!   ([`Stop::BeyondList`]).
//! - **The stop marker** ([`write_marker`], [`RunLeases::open`]; 0151 item 6): on any stop but
//!   [`Stop::NoEcho`], the run writes [`MARKER`] in its lease directory with the stop's name and
//!   the run's start, and the stops of the marker it found there, unless it cleared them, so a
//!   marker it replaces loses no stop still unreconciled. A later place-stage run looks for one
//!   in its lease directory and in production's default, refuses to start while one exists
//!   without `--reconciled`, and refuses `--reconciled` when none exists. With it, the run reads every named account first and clears
//!   the marker only if every read is clean; the declaration is the owner's, which the probe does
//!   not check, and the report says so. Stage one and the hold neither read nor write it.

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
    /// A session that ended with a position that is not flat.
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
    pub const ALL: [Stop; 19] = [
        Stop::NotOurCid,
        Stop::StrayOrder,
        Stop::ReplayRefused(Replay::Untimed),
        Stop::ReplayRefused(Replay::BeyondHorizon),
        Stop::ResyncDesync,
        Stop::ResyncUnsettled,
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
            Stop::NotFlat => {
                "a session that ended with a position that is not flat (decision 0132 item 7)"
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
    /// X's `terminal_status` on historic-orders when a row names its client id, as a status
    /// name (capitals, digits and underscores) or `(not a status name)`.
    pub terminal_status: Option<String>,
    /// Why a read could not be made, when one could not.
    pub error: Option<String>,
}

/// Drop session one, as the report and the evidence show it (FBC-23ch): names, flags, counts and
/// times alone.
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
    /// Whether the graceful close cancelled X: the read of the account after it no longer
    /// listing X, which rested before it with no cancel of ours; `None` when not measured.
    pub close_cancelled_x: Option<bool>,
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
}

/// How each frame the tee heard gave back our client id ([`DropRun::echoes`]).
pub fn echoes(kept: &crate::tee::Kept) -> Vec<String> {
    use crate::tee::{EchoCid, Heard};
    let echo = |cid: EchoCid| match cid {
        EchoCid::Exact => "exact",
        EchoCid::Differs => "not byte for byte",
        EchoCid::Absent => "absent",
    };
    (kept.heard.iter())
        .map(|h| match h {
            Heard::Order { status, cid, .. } => format!("order_response {status}: {}", cid.name()),
            Heard::Fill { cid, .. } => format!("fill_response: {}", cid.name()),
            Heard::Echo { envelope, cid, .. } => format!("{envelope} of add_order: {}", echo(*cid)),
        })
        .collect()
}

/// Whether every `add_order` in `kept` went out as encoded ([`DropRun::as_encoded`]).
pub fn as_encoded(kept: &crate::tee::Kept) -> Option<bool> {
    let sent = kept.sent();
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

/// The report's section on drop session one.
pub fn drop_markdown(s: &mut String, run: &DropRun) {
    let yn = |b: bool| if b { "yes" } else { "no" };
    let arm = run.arm.as_deref().unwrap_or("none reported");
    let close = match run.close_cancelled_x {
        Some(true) => {
            "yes: X rested before it, no cancel of ours was sent, and the read after it \
                       no longer listed X"
        }
        Some(false) => "no: X rested before it and still did after it",
        None => {
            "not measured (X did not rest before the close, a cancel of ours was sent, or the \
                 read after it was not made)"
        }
    };
    let cancels = match &run.cancels {
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
    };
    let after = match &run.after {
        None => "not read".to_owned(),
        Some(a) => format!(
            "{} trade rows since X's send, {} naming X; X on GET /user/historic-orders: {}{}",
            a.trades,
            a.trades_naming_x,
            a.terminal_status.as_deref().unwrap_or("not listed"),
            a.error
                .as_ref()
                .map_or(String::new(), |e| format!("; not read whole: {e}"))
        ),
    };
    let _ = writeln!(
        s,
        "## Drop session one (FBC-23ch, decision 0132 item 4)\n\n\
         On {} (named by the run's configuration). Until an ACK echoes our client id, a \
         session places at most one post-only GTC far from the touch (decision 0132 item 7): \
         X, at the instrument's minimum size, --away-bps behind the touch (a fence met; no price \
         or size is shown).\n\n\
         - Stream authenticated: {}; what answered the arm: {arm}; resync applied: {}; market \
           Started under its market lease: {}\n\
         - X: handed to the session {}; shown resting by an order event {}; an ACK echoing its \
           client id byte for byte {}; its request's outcome {}; from its send to its ACK {}\n\
         - The stop's cancels: {cancels}\n\
         - Ended by: {}\n\
         - Whether the graceful close cancelled X: {close}\n\
         - X and its trades after the session: {after}\n\
         - X resting when the run ended: {}\n\
         - Our client id as QFEX gave it back, frame by frame (decision 0148 item 2; never the \
           id): {}\n\
         - add_order sent as encoded, with no take_profit, stop_loss or reduce_only (FBC-blcw): \
           {}\n\
         - Error: {}\n",
        run.account,
        yn(run.authenticated),
        yn(run.resynced),
        yn(run.started),
        yn(run.x.submitted),
        yn(run.x.acked),
        yn(run.x.exact_echo),
        run.x.outcome.as_deref().unwrap_or("none reported"),
        run.x
            .ack_after
            .map_or("not measured".to_owned(), |d| format!(
                "{} ms",
                d.as_millis()
            )),
        run.ended_by.unwrap_or("it did not open"),
        match (run.x_resting, run.x.submitted, run.x_read) {
            (true, _, _) => HAND_CANCEL,
            (false, false, _) => "no: X was not placed",
            (false, true, true) => "no",
            (false, true, false) => {
                "not known: the account was not read after the session; read it on QFEX's \
                 site before the next run"
            }
        },
        match run.echoes.is_empty() {
            true => "none heard".to_owned(),
            false => run.echoes.join("; "),
        },
        run.as_encoded.map_or("none sent", yn),
        run.error.as_deref().unwrap_or("none"),
    );
    let _ = writeln!(
        s,
        "The comparisons of the streams with the reads (0151 item 4: a read at once, then each \
         --read-interval-ms after the last began, at most 10 within --agree-wait-ms):\n\n\
         | after | reads | first agreeing read after the step | ended |\n\
         | --- | --- | --- | --- |"
    );
    for c in &run.compared {
        let first = c
            .first_agreed
            .map_or("none".to_owned(), |d| format!("{} ms", d.as_millis()));
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

/// The evidence's entry for drop session one.
pub fn drop_to_json(run: &DropRun) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let compared: Vec<Value> = (run.compared.iter())
        .map(|c| {
            let reads: Vec<Value> = (c.reads.iter())
                .map(|(at, took)| json!({"began_ms": ms(*at), "took": format!("{took:?}")}))
                .collect();
            json!({"after": c.step, "reads": reads,
                "first_agreed_ms": c.first_agreed.map(ms), "ended": ended(c.ended),
                "stop": c.stop.map(Stop::name)})
        })
        .collect();
    let cancels = run
        .cancels
        .as_ref()
        .map(|c| json!({"sent": c.sent, "held": c.held, "ended": c.ended, "presses": c.presses}));
    let after = run.after.as_ref().map(|a| {
        json!({"trades": a.trades, "trades_naming_x": a.trades_naming_x,
            "terminal_status": a.terminal_status, "error": a.error})
    });
    json!({"account": run.account, "authenticated": run.authenticated, "arm": run.arm,
        "resynced": run.resynced, "started": run.started,
        "x": {"submitted": run.x.submitted, "acked": run.x.acked,
            "exact_echo": run.x.exact_echo, "outcome": run.x.outcome,
            "ack_after_ms": run.x.ack_after.map(ms)},
        "cancels": cancels, "compared": compared, "ended_by": run.ended_by,
        "close_cancelled_x": run.close_cancelled_x, "x_resting": run.x_resting,
        "x_read_after": run.x_read,
        "hand_cancel": run.x_resting.then_some(HAND_CANCEL), "after": after,
        "echoes": run.echoes, "add_order_as_encoded": run.as_encoded, "error": run.error})
}
