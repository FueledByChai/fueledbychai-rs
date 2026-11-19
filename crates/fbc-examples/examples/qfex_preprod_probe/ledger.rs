//! Each registry's fill ledger and the bounds it is held to (decision 0132 item 5, FBC-5hif).
//!
//! The probe builds stage one's registries as decision 0132 item 7 says
//! (`Registry::for_testnet_run(TestnetRun::owner_assisted())`) on pre-production, and on production
//! without that declaration, which decision 0067 says a consumer trading a live venue never makes
//! (decision 0143 item 3: stage one seeds and arms no market, so it needs none). Each is bound to
//! the one account its sessions trade (decision 0082), and each has one fill ledger, kept across
//! every session the registry serves ([`RegistryLedger`]). A ledger forgets a fill once it is older
//! than its age bound or beyond its count bound, and each fill it forgets moves its horizon: to
//! `Horizon::Lost` when the forgotten fill had no matching-engine or publish time. A resync can
//! read again fills heard live (0132 item 5's start time), and the ledger judges a duplicate before
//! it judges time; a fill it forgot is judged by its time instead, and until FBC-jdj7 a replayed
//! fill has none, so it is refused `Untimed`, which stops the probe (0132 item 7). So the bounds
//! are the owner's, with no defaults, and refused before anything connects unless they outlast
//! every registry's sessions and fills ([`Bounds::new`]):
//!
//! - the **span bound** (`--registry-span`) is the longest from a registry's first session's
//!   start to its last session's end; a session that reaches it is stopped
//!   ([`RegistryLedger::deadline`]). Stage one's sessions place nothing, so the program stops
//!   them through their stop control;
//! - the **age bound** (`--ledger-max-age`) must be longer than the span bound, so no fill a
//!   registry's sessions applied ages out before the last of them ends;
//! - the **count bound** (`--ledger-max-entries`) must be above the **fill ceiling**
//!   ([`fill_ceiling`]): every order a registry's sessions may place ([`MayPlace`]), each at its
//!   largest size in size steps, since every fill moves at least one step. Stage one places
//!   nothing ([`stage_one`]), so its ceiling is zero.
//!
//! The report and the evidence state, for each registry, those bounds, and at each session's end
//! its ledger's horizon and replay counts ([`RegistryRun`]), so a run shows the bounds held.
//!
//! **The place stages' registries** (FBC-9sk2; decision 0148 items 3 and 7, FBC-5hif). On
//! production alone (0151 item 1), each named account gets four registries in run order, one per
//! [`Step`]: the drop test's, which serves both its sessions, the crossing post-only's, the taker
//! IOC's and stage three's. [`PlaceLists`] gives each its [`MayPlace`] list in size steps, every
//! order at the instrument's minimum size (0148 item 4): the lists' entries other than the
//! flatten IOCs sum to 0148 item 7's most per account, and every list carries
//! `--flatten-attempts` flatten IOCs, since an unexpected fill may need a flatten from any
//! registry (0151 item 9), while the account's flatten IOCs across its registries share one
//! budget of that many ([`PlaceLists::flatten_budget`]), which this file states and
//! `places::Allowance` spends (FBC-23ch): nothing here spends it. [`PlaceBounds::accept`] holds
//! the command line's bounds to each list's ceiling. [`PlaceRegistry`] builds each registry with
//! its caps, declared `CodProbeRun::owner_supervised_places` on its one market and never a
//! testnet run, under its lease names and bound to its account; gives the journal its
//! `CancelOnlyTrip` ([`PlaceRegistry::trip_on`], decision 0139); and hands the registry and its
//! one fill ledger to one `Link` (`src/link.rs`) for the registry's life
//! ([`PlaceRegistry::into_link`]). The `Link` routes every fill of the registry's sessions through
//! that ledger and lists each one the ledger refuses for its time (`Link::fill_refusals`), and
//! [`PlaceRun::session_ended`] reads the ledger back from it.
//!
//! What the place stages' driver does with them (FBC-23ch's and FBC-77r8's drop test, in
//! `stage.rs`): it spends each list and the shared flatten budget as it authorizes places, and
//! builds no place beyond them; it seeds each market flat after clean reads of every named account
//! and arms it under the market lease; it gives each registry's trip to the journal before any
//! market arms; it stops on a fill the `Link` lists as refused; and it records the ledger at each
//! session's end ([`PlaceRun::session_ended`]), which the report renders with its list and ceiling
//! as fences met ([`place_markdown`]). The drop test's two sessions share its account's drop
//! registry and one deadline (FBC-77r8). Still to come: the cancels by venue id begun one
//! `--rpc-timeout-ms` before the span bound outside the drop test, since the bound is when a
//! session is stopped and the session ends after it ([`AtEnd::past_span`] records by how much;
//! [`RegistryRun::held`] fails a registry whose session ended at or past the age bound).
//!
//! The span bound's floor, which the command line holds above a session's reach to its arm's
//! deadline, counts one session per registry: the drop test's two need it counted twice, with
//! the reads between them, which a place-stage run holds its span bound above before anything
//! connects (`probe_args::check_place_span`, FBC-c6kw). FBC-39ks's hold opens no session and
//! places nothing, so it builds no registry and adds nothing to the ceiling; held between two
//! sessions of one registry, it would count in that registry's span.

use std::fmt::Write as _;
use std::time::Duration;

use fbc_core::{AccountKey, FillEvent, InstrumentId, Lots, MonoNs, OrderCaps, WallNs};
use fbc_oms::{
    Admission, CodProbeRun, FillLedger, FillRouted, FillTime, Horizon, LadderConfig, LeaseKeys,
    LedgerConfig, OmsError, PreTradeCaps, Registry, ReplayCounts, TestnetRun,
};
use fbc_runtime::Journal;
use fbc_venue_qfex::probe::Environment;
use serde_json::{Value, json};

use crate::link::Link;

/// The longest `--registry-span` taken: a day.
pub const MAX_SPAN_S: u64 = 86_400;
/// The longest `--ledger-max-age` taken: a week.
pub const MAX_AGE_S: u64 = 604_800;
/// The largest `--ledger-max-entries` taken.
pub const MAX_ENTRIES: u64 = 1_000_000;

/// Orders a registry's sessions may place, each counted in the fill ceiling at its largest size
/// in size steps.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MayPlace {
    /// What they are, as the report names them.
    pub what: &'static str,
    /// How many.
    pub orders: u64,
    /// Each one's largest size, in the instrument's size steps.
    pub largest_steps: u64,
}

/// What stage one may place on a registry: nothing (decision 0132 item 7). The place stages'
/// lists are [`PlaceLists`]'.
pub fn stage_one() -> Vec<MayPlace> {
    Vec::new()
}

/// The most fills a registry's sessions can see when they may place `places`: the sum of each
/// order's largest size in size steps, since every fill moves at least one step. Refused when
/// the sum overflows.
pub fn fill_ceiling(places: &[MayPlace]) -> Result<u64, String> {
    places.iter().try_fold(0u64, |sum, p| {
        (p.orders.checked_mul(p.largest_steps))
            .and_then(|steps| sum.checked_add(steps))
            .ok_or_else(|| "the fill ceiling overflows: fewer or smaller orders".to_owned())
    })
}

/// A registry's ledger bounds, accepted ([`Bounds::new`]): the only way the program has them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bounds {
    max_age: Duration,
    max_entries: usize,
    span: Duration,
    places: Vec<MayPlace>,
    ceiling: u64,
}

impl Bounds {
    /// The bounds of a registry whose sessions may place `places`, refused naming the flag (never
    /// a value typed): a zero age, count or span, a span longer than [`MAX_SPAN_S`], an age not
    /// longer than the span, and a count not above the fill ceiling of `places`.
    pub fn new(
        max_age: Duration,
        max_entries: usize,
        span: Duration,
        places: &[MayPlace],
    ) -> Result<Bounds, String> {
        if span.is_zero() || span > Duration::from_secs(MAX_SPAN_S) {
            return Err(format!(
                "--registry-span: not from 1 to {MAX_SPAN_S} s (not shown)"
            ));
        }
        if max_age.is_zero() {
            return Err("--ledger-max-age: zero".to_owned());
        }
        if max_entries == 0 {
            return Err("--ledger-max-entries: zero".to_owned());
        }
        if max_age <= span {
            return Err(
                "--ledger-max-age must be longer than --registry-span, or a fill heard live \
                 could age out of the ledger before its registry's last session ends and be \
                 refused Untimed when a resync reads it again (decision 0132 item 5; the values \
                 are not shown)"
                    .to_owned(),
            );
        }
        let ceiling = fill_ceiling(places)?;
        if u64::try_from(max_entries).unwrap_or(u64::MAX) <= ceiling {
            return Err(format!(
                "--ledger-max-entries must be above the fill ceiling, {ceiling}: the fills the \
                 registry's sessions can see, each order they may place counted at its largest \
                 size in size steps (decision 0132 item 5; the value is not shown)"
            ));
        }
        Ok(Bounds {
            max_age,
            max_entries,
            span,
            places: places.to_vec(),
            ceiling,
        })
    }

    /// The span bound (`--registry-span`), which a place-stage run holds above the drop test's
    /// floor (`probe_args::check_place_span`, FBC-c6kw).
    pub fn span(&self) -> Duration {
        self.span
    }

    /// The ledger's configuration.
    pub fn config(&self) -> LedgerConfig {
        LedgerConfig {
            max_age: self.max_age,
            max_entries: self.max_entries,
        }
    }

    /// The fill ceiling the count bound is above: the orders it counts, in size steps. For the
    /// tests: the report reads the field.
    #[cfg(test)]
    pub fn ceiling(&self) -> u64 {
        self.ceiling
    }

    /// The orders the fill ceiling counts. For the tests: the report reads the field.
    #[cfg(test)]
    pub fn places(&self) -> &[MayPlace] {
        &self.places
    }
}

/// What became of a fill a registry's ledger was given: applied, and where the registry
/// counted it; refused by the registry; or not applied, and why (fbc-oms's `Admission`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Judged {
    Applied(FillRouted),
    Refused(OmsError),
    Duplicate,
    BeforeWatermark,
    BeyondHorizon,
    Untimed,
}

/// Gives `fill`, timed `time` by the venue, to `ledger` at `now` on the shard's monotonic
/// clock, and an accepted fill to `reg`, the one path a fill takes into a registry (decision
/// 0005, I3; the shared glue's fill path does the same).
pub fn judge(
    reg: &mut Registry,
    ledger: &mut FillLedger,
    fill: &FillEvent,
    time: Option<FillTime>,
    now: MonoNs,
) -> Judged {
    match ledger.admit(fill, time, now) {
        Admission::Apply(accepted) => match reg.apply_fill(accepted) {
            Ok(routed) => Judged::Applied(routed),
            Err(e) => Judged::Refused(e),
        },
        Admission::Duplicate => Judged::Duplicate,
        Admission::BeforeWatermark => Judged::BeforeWatermark,
        Admission::BeyondHorizon => Judged::BeyondHorizon,
        Admission::Untimed => Judged::Untimed,
    }
}

/// Now on the run's monotonic clock from `origin`, an instant on the runtime's clock (tokio's,
/// which a paused runtime stops and moves). The sessions' `IngestClock`, whose stamps a handler
/// gives the ledger as its `now`, reads that clock from an origin of its own: the run takes
/// `origin` just after it builds the `IngestClock`, so the two readings of one instant differ by
/// that moment alone, and the time between two instants is the same on both. That is what
/// [`AtEnd::after`] and the ledger's ages measure.
pub fn mono_since(origin: tokio::time::Instant) -> MonoNs {
    mono_at(origin, tokio::time::Instant::now())
}

/// `at` on the monotonic clock from `origin`.
fn mono_at(origin: tokio::time::Instant, at: tokio::time::Instant) -> MonoNs {
    let since = at.saturating_duration_since(origin);
    MonoNs(u64::try_from(since.as_nanos()).unwrap_or(u64::MAX))
}

/// When a registry's first session started, on each clock the registry reads.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Opened {
    /// On the run's monotonic clock ([`mono_since`]): the runtime's, as the ledger's ages are.
    pub mono: MonoNs,
    /// On the local wall clock: the ledger's session-start watermark.
    pub wall: WallNs,
    /// On the runtime's clock, which the span bound's timer runs on.
    pub at: tokio::time::Instant,
}

impl Opened {
    /// Now, as a registry's first session starts, the local wall clock reading `wall`: one
    /// reading of the runtime's clock, as the span bound's timer and on the run's monotonic
    /// clock from `origin`.
    pub fn now(origin: tokio::time::Instant, wall: WallNs) -> Opened {
        let at = tokio::time::Instant::now();
        Opened {
            mono: mono_at(origin, at),
            wall,
            at,
        }
    }
}

/// One registry, its one fill ledger, and the ledger's state at each of its sessions' ends.
pub struct RegistryLedger {
    run: RegistryRun,
    reg: Registry,
    ledger: FillLedger,
    opened: Opened,
}

impl RegistryLedger {
    /// The registry of `account` (the account key `acct`), for a run on `environment`, with an
    /// empty ledger under `bounds`, opened as its first session starts. On pre-production it is
    /// built as decision 0132 item 7 says, a declared owner-assisted testnet run; on production
    /// with no declaration (decisions 0067 and 0143 item 3). The watermark is that start: the
    /// reads before the session showed every account flat with no open order (0132 item 7), so
    /// the position it starts from holds every fill before it.
    pub fn new(
        account: &'static str,
        acct: AccountKey,
        bounds: &Bounds,
        opened: Opened,
        environment: Environment,
    ) -> RegistryLedger {
        let mut reg = match environment {
            Environment::PreProduction => {
                Registry::new().for_testnet_run(TestnetRun::owner_assisted())
            }
            Environment::Production => Registry::new(),
        };
        reg.bind_account(acct)
            .expect("a new registry binds to its account");
        let ledger = FillLedger::new(bounds.config(), opened.wall);
        let ledger = ledger.expect("accepted bounds hold no zero age or count");
        RegistryLedger {
            run: RegistryRun {
                account,
                testnet_run: reg.testnet_run(),
                bounds: bounds.clone(),
                ends: Vec::new(),
            },
            reg,
            ledger,
            opened,
        }
    }

    /// The instant on the runtime's clock its span bound ends at: a session still running then
    /// is stopped (the span bound is at most a day, so the instant exists).
    pub fn deadline(&self) -> tokio::time::Instant {
        self.opened.at + self.run.bounds.span
    }

    /// Gives `fill` to the ledger and, accepted, to the registry ([`judge`]): the path every
    /// fill of the registry's sessions takes. Stage one places nothing, so its codec, which
    /// decodes fills (FBC-jxau), is handed none of its own, and only its tests give one. A place
    /// stage's registry routes its fills through its `Link` instead ([`PlaceRegistry`]).
    #[allow(dead_code)]
    pub fn admit(&mut self, fill: &FillEvent, time: Option<FillTime>, now: MonoNs) -> Judged {
        judge(&mut self.reg, &mut self.ledger, fill, time, now)
    }

    /// Records the ledger's state as the session `session` ended, at `now` on the run's
    /// monotonic clock ([`mono_since`]).
    pub fn session_ended(&mut self, session: &'static str, now: MonoNs) {
        let end = AtEnd::of(&self.ledger, session, now - self.opened.mono);
        self.run.ends.push(end);
    }

    /// What the report and the evidence say of it.
    pub fn run(&self) -> &RegistryRun {
        &self.run
    }

    /// The registry, for the tests to read: stage one's carries no probe declaration on
    /// production, and is a declared testnet run on pre-production alone (decisions 0143 item 3,
    /// 0148 item 3).
    #[cfg(test)]
    pub fn registry(&self) -> &Registry {
        &self.reg
    }
}

/// A registry's ledger as a session of it ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtEnd {
    /// The session that ended.
    pub session: &'static str,
    /// How long after the registry's first session started.
    pub after: Duration,
    pub horizon: Horizon,
    /// The fills the ledger held.
    pub held: usize,
    pub replays: ReplayCounts,
}

impl AtEnd {
    /// `ledger` as the session `session` ended, `after` its registry's first session started.
    fn of(ledger: &FillLedger, session: &'static str, after: Duration) -> AtEnd {
        AtEnd {
            session,
            after,
            horizon: ledger.horizon(),
            held: ledger.len(),
            replays: ledger.replays(),
        }
    }

    /// By how much the session ended past the span bound `span`: the bound is when the program
    /// stops a session, which then takes a moment to end. `None` when it ended within it.
    pub fn past_span(&self, span: Duration) -> Option<Duration> {
        self.after.checked_sub(span).filter(|past| !past.is_zero())
    }
}

/// A measured time in whole milliseconds, as the report prints the time a session ended after
/// its first session's start and by how much it ended past the span bound. Whole milliseconds,
/// never `Duration`'s Debug form, so a measured time never reads as a decimal value.
fn whole_ms(d: Duration) -> String {
    format!("{} ms", d.as_millis())
}

/// One registry's ledger, as the report and the evidence show it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryRun {
    /// The account its sessions trade (`primary` or `subaccount`, never an id).
    pub account: &'static str,
    /// Whether it was built for a declared owner-assisted testnet run, as the registry reports
    /// it (`Registry::testnet_run`): on pre-production, never on production (decision 0143 item
    /// 3).
    pub testnet_run: bool,
    pub bounds: Bounds,
    /// Its ledger at each of its sessions' ends, in order.
    pub ends: Vec<AtEnd>,
}

impl RegistryRun {
    /// Whether the bounds held: at each of its sessions' ends the ledger's horizon was still full
    /// (it had forgotten no fill, by count or by age), and the session ended before the age
    /// bound from the registry's first session's start, so no fill its sessions applied, each at
    /// or after that start, could have aged out by then. The second is needed as well: fbc-oms
    /// forgets an aged fill only as it is given or records one, so a horizon read with no fill
    /// since is still full past the age bound, though a replay then would find the fill gone.
    pub fn held(&self) -> bool {
        let max_age = self.bounds.max_age;
        (self.ends.iter()).all(|end| end.horizon == Horizon::Full && end.after < max_age)
    }
}

// ----- The place stages' registries (FBC-9sk2; decision 0148 items 3 and 7, FBC-5hif) -----

/// The most flatten IOCs `--flatten-attempts` takes for an account in a run (decision 0148 item
/// 7).
pub const MAX_FLATTEN_ATTEMPTS: u64 = 3;

/// The drop test's first resting order (decision 0132 item 4), as a list names it.
pub const X: &str = "post-only GTC X";
/// The drop test's second resting order.
pub const Y: &str = "post-only GTC Y";
/// Stage two's post-only through the touch (decision 0132 item 7).
pub const THROUGH: &str = "post-only GTC through the touch";
/// Stage two's taker IOC.
pub const TAKER: &str = "taker limit IOC";
/// Stage three's resting orders, paged at size one.
pub const PAGED: &str = "stage-three post-only GTC";
/// A flatten's reducing limit IOC (decision 0148 item 5).
pub const FLATTEN: &str = "flatten limit IOC";

/// One place-stage registry of an account, in run order (0151 item 5): each
/// has its own [`MayPlace`] list, fill ledger and span bound.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Step {
    /// The drop test (decision 0132 item 4): X in its first session, Y in its second.
    Drop,
    /// Stage two's post-only through the touch.
    Cross,
    /// Stage two's taker IOC.
    Take,
    /// Stage three's resting orders and their pages.
    Pages,
}

impl Step {
    /// Every step, in run order.
    pub const ALL: [Step; 4] = [Step::Drop, Step::Cross, Step::Take, Step::Pages];

    /// The step as the report names its registry.
    pub fn name(self) -> &'static str {
        match self {
            Step::Drop => "drop",
            Step::Cross => "cross",
            Step::Take => "take",
            Step::Pages => "pages",
        }
    }

    /// The orders other than the flatten IOCs its sessions may place, each counted once: the
    /// step's share of decision 0148 item 7's most per account.
    fn orders(self) -> &'static [(&'static str, u64)] {
        match self {
            Step::Drop => &[(X, 1), (Y, 1)],
            Step::Cross => &[(THROUGH, 1)],
            Step::Take => &[(TAKER, 1)],
            Step::Pages => &[(PAGED, 3)],
        }
    }
}

/// The [`MayPlace`] list of each place-stage registry of an account, every order at the traded
/// instrument's minimum size, in size steps (decision 0148 items 4 and 7).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PlaceLists {
    /// The instrument's minimum size, in size steps: the size of every place.
    min_steps: u64,
    /// `--flatten-attempts`.
    attempts: u64,
}

impl PlaceLists {
    /// The lists for an instrument whose minimum size is `min_size` (in size steps, as this
    /// run's discovery gives it) and `--flatten-attempts` of `attempts`. Refused, naming the
    /// flag where a flag gives it and never a value: attempts not from 1 to
    /// [`MAX_FLATTEN_ATTEMPTS`], and a minimum size of zero, which no order is placed at and
    /// which would leave a ceiling that counts no fill.
    pub fn new(min_size: Lots, attempts: u64) -> Result<PlaceLists, String> {
        if !(1..=MAX_FLATTEN_ATTEMPTS).contains(&attempts) {
            return Err(format!(
                "--flatten-attempts: not from 1 to {MAX_FLATTEN_ATTEMPTS} (not shown)"
            ));
        }
        match u64::try_from(min_size.get()) {
            Ok(min_steps) if min_steps > 0 => Ok(PlaceLists {
                min_steps,
                attempts,
            }),
            _ => Err(
                "the instrument's minimum size is zero size steps: no order is placed at it"
                    .to_owned(),
            ),
        }
    }

    /// What `step`'s registry's sessions may place: its own orders, then `--flatten-attempts`
    /// flatten IOCs, since an unexpected fill may need a flatten from any registry (0151 item
    /// 9). Every order is one minimum size.
    pub fn list(&self, step: Step) -> Vec<MayPlace> {
        let may = |what, orders| MayPlace {
            what,
            orders,
            largest_steps: self.min_steps,
        };
        let own = step
            .orders()
            .iter()
            .map(|&(what, orders)| may(what, orders));
        own.chain([may(FLATTEN, self.attempts)]).collect()
    }

    /// The flatten IOCs an account may place in a run, across all of its registries: one
    /// budget of `--flatten-attempts`, which each registry's list repeats but which the account
    /// spends once (decision 0148 items 5 and 7). This states the budget and spends none of it:
    /// `places::Allowance` (FBC-23ch) holds the account's flatten IOCs across its registries to
    /// it, so the account's total never exceeds it.
    pub fn flatten_budget(&self) -> u64 {
        self.attempts
    }
}

/// The fill-ledger bounds of each place-stage registry, accepted against its list
/// ([`PlaceBounds::accept`]): the only way a [`PlaceRegistry`] gets them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceBounds {
    each: Vec<(Step, Bounds)>,
}

impl PlaceBounds {
    /// The bounds `parsed` (the command line's, which it accepts against stage one's ceiling
    /// alone) held to each list of `lists`: the age and span bounds as given, and the count
    /// bound above every list's fill ceiling, refused naming `--ledger-max-entries` and the
    /// registry, never a value (decision 0132 item 5). The ceiling is known only once the
    /// traded instrument's minimum size is, so this runs after discovery and before any
    /// account connects (0151 item 3).
    pub fn accept(parsed: &Bounds, lists: &PlaceLists) -> Result<PlaceBounds, String> {
        let bounds = |step: Step| {
            Bounds::new(
                parsed.max_age,
                parsed.max_entries,
                parsed.span,
                &lists.list(step),
            )
            .map(|b| (step, b))
            .map_err(|e| format!("the {} registry: {e}", step.name()))
        };
        let each = Step::ALL
            .into_iter()
            .map(bounds)
            .collect::<Result<_, _>>()?;
        Ok(PlaceBounds { each })
    }

    /// `step`'s registry's bounds.
    pub fn of(&self, step: Step) -> &Bounds {
        let found = self.each.iter().find(|(s, _)| *s == step);
        &found.expect("accepted for every step").1
    }
}

/// The declaration a place-stage registry carries, as the report names it (decision 0148 item
/// 3).
pub const PLACES_DECLARATION: &str = "CodProbeRun::owner_supervised_places";

/// One place-stage registry, built ([`PlaceRegistry::new`]) and not yet handed to its `Link`.
pub struct PlaceRegistry {
    run: PlaceRun,
    reg: Registry,
    ledger: FillLedger,
}

impl PlaceRegistry {
    /// `step`'s registry of `account` (the account key `acct`), on production alone (0151 item
    /// 1): built with `caps` (`Registry::with_caps`), declared
    /// `CodProbeRun::owner_supervised_places(inst)` on its one market `inst` (decision 0148 item
    /// 3) and never a testnet run, under the lease names `keys`, and bound to `acct` (decision
    /// 0082), with an empty ledger under its step's bounds of `bounds`, opened as its first
    /// session starts. The watermark is that start: the reads before the session show every
    /// named account flat with no open order (decision 0132 item 7), so the position it starts
    /// from holds every fill before it.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        step: Step,
        account: &'static str,
        acct: AccountKey,
        bounds: &PlaceBounds,
        opened: Opened,
        inst: InstrumentId,
        caps: PreTradeCaps,
        keys: LeaseKeys,
    ) -> PlaceRegistry {
        let mut reg = Registry::with_caps(caps)
            .for_cod_probe(CodProbeRun::owner_supervised_places(inst))
            .with_lease_keys(keys);
        reg.bind_account(acct)
            .expect("a new registry binds to its account");
        let bounds = bounds.of(step);
        let ledger = FillLedger::new(bounds.config(), opened.wall);
        let ledger = ledger.expect("accepted bounds hold no zero age or count");
        PlaceRegistry {
            run: PlaceRun {
                step,
                declaration: PLACES_DECLARATION,
                market: inst,
                registry: RegistryRun {
                    account,
                    testnet_run: reg.testnet_run(),
                    bounds: bounds.clone(),
                    ends: Vec::new(),
                },
                opened,
            },
            reg,
            ledger,
        }
    }

    /// The registry, to read before it goes to its `Link`.
    #[allow(dead_code)] // the registries' tests read it
    pub fn registry(&self) -> &Registry {
        &self.reg
    }

    /// What the report and the evidence say of it.
    #[allow(dead_code)] // the registries' tests read it
    pub fn run(&self) -> &PlaceRun {
        &self.run
    }

    /// Gives `journal` the registry's `CancelOnlyTrip`, so the journal's soft limit moves its
    /// market in Quoting to Cancel-only (decision 0139). The driver gives it before any market
    /// of the registry arms; the trip stays with the registry once it goes to its `Link`.
    pub fn trip_on(&self, journal: &Journal) {
        journal.cancel_only_at_soft_limit(self.reg.cancel_only_trip());
    }

    /// Hands the registry and its one fill ledger to one `Link` under the venue's order caps
    /// `order` and `ladder`, which the driver keeps for as long as the registry serves sessions,
    /// the drop test's two included, so every fill of its sessions goes through that ledger
    /// (FBC-5hif), and what the report says of it.
    pub fn into_link(self, order: OrderCaps, ladder: LadderConfig) -> (Link, PlaceRun) {
        (Link::new(self.reg, self.ledger, order, ladder), self.run)
    }
}

/// A place-stage registry as the report and the evidence show it: its step, its declaration and
/// its market beside its ledger's [`RegistryRun`].
///
/// Its [`RegistryRun`] is private, so [`markdown`] and [`to_json`] never render it: they print
/// each [`MayPlace`]'s count, the flatten IOCs' `--flatten-attempts` included, and a ceiling
/// that counts it, with stage one's prose. 0151 item 10 shows the attempts only as a fence met,
/// so the place stages' report writes its own rendering here, beside them, with the flatten
/// entry and the ceiling as fences met ([`place_markdown`], [`place_to_json`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceRun {
    pub step: Step,
    /// The probe declaration it carries ([`PLACES_DECLARATION`]).
    pub declaration: &'static str,
    /// The one market the declaration names.
    pub market: InstrumentId,
    /// Its bounds and its ledger at each of its sessions' ends.
    registry: RegistryRun,
    opened: Opened,
}

impl PlaceRun {
    /// The instant on the runtime's clock its span bound ends at, as
    /// [`RegistryLedger::deadline`]'s.
    pub fn deadline(&self) -> tokio::time::Instant {
        self.opened.at + self.registry.bounds.span
    }

    /// The account its sessions trade, as the configuration names it (FBC-ou9g's report of the
    /// declarations).
    pub fn account(&self) -> &'static str {
        self.registry.account
    }

    /// Whether its bounds held, as [`RegistryRun::held`] judges them.
    pub fn held(&self) -> bool {
        self.registry.held()
    }

    /// Records `link`'s ledger, the registry's, as the session `session` ended, at `now` on the
    /// run's monotonic clock ([`mono_since`]): its horizon, the fills it holds and its replay
    /// counts.
    pub fn session_ended(&mut self, link: &Link, session: &'static str, now: MonoNs) {
        let end = AtEnd::of(link.ledger(), session, now - self.opened.mono);
        self.registry.ends.push(end);
    }

    /// What decision 0132's list for its account says of it (`crate::answers`): its step,
    /// whether its bounds held, and, at each of its sessions' ends, whether its horizon was
    /// full and its replay counts, never a time or a value.
    pub fn said(&self) -> String {
        let r = &self.registry;
        let ends: Vec<String> = (r.ends.iter())
            .map(|e| {
                let (c, horizon) = (e.replays, horizon_word(e.horizon));
                format!(
                    "after {}: horizon {horizon}, {} fill{} held, replays applied {}, duplicate \
                     {}, before the watermark {}, beyond the horizon {}, untimed {}",
                    e.session,
                    e.held,
                    if e.held == 1 { "" } else { "s" },
                    c.applied,
                    c.duplicate,
                    c.before_watermark,
                    c.beyond_horizon,
                    c.untimed
                )
            })
            .collect();
        let ends = match ends.is_empty() {
            true => "no session of it ended".to_owned(),
            false => ends.join("; "),
        };
        format!(
            "the {} registry: its bounds held {}; {ends}",
            self.step.name(),
            if r.held() { "yes" } else { "NO" }
        )
    }

    /// Its bounds and its ledger at each of its sessions' ends, for the tests to read.
    #[cfg(test)]
    pub fn registry_run(&self) -> &RegistryRun {
        &self.registry
    }
}

/// What the report and the evidence say of a place-stage registry's count bound and fill
/// ceiling: a fence met, never a value, since the ceiling counts `--flatten-attempts` (0151 item
/// 10).
pub const PLACE_CEILING: &str = "above the registry's fill ceiling, every order its list may \
     place counted at the instrument's minimum size, its flatten IOCs --flatten-attempts (a \
     fence met; neither the ceiling nor the attempts is shown, 0151 item 10)";

/// The report's section on the place stages' registries (FBC-23ch; decision 0132 item 5, 0148
/// items 3 and 7, 0151 item 10): each registry's step, account, declaration, bounds and whether
/// they held, then its ledger at each of its sessions' ends. Its list and ceiling are fences met,
/// never values.
pub fn place_markdown(s: &mut String, runs: &[PlaceRun]) {
    if runs.is_empty() {
        return;
    }
    let _ = writeln!(
        s,
        "## The place stages' registries\n\n\
         Decision 0148 items 3 and 7: one registry per account and step, declared \
         `{PLACES_DECLARATION}` on its one market, never a testnet run (decision 0143 item 3), \
         bound to its account, its fills through one fill ledger across every session it serves \
         (decision 0132 item 5). Its `MayPlace` list holds one minimum size per order and the \
         account's flatten IOCs, a fence met. The bounds held for a registry when, at each of \
         its sessions' ends, its horizon was still full and the session ended within the age \
         bound of the registry's first session's start.\n\n\
         | registry | account | declaration | testnet run | max age | span bound | max entries \
         | held |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for run in runs {
        let r = &run.registry;
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {:?} | {:?} | {PLACE_CEILING} | {} |",
            run.step.name(),
            r.account,
            run.declaration,
            if r.testnet_run { "yes" } else { "no" },
            r.bounds.max_age,
            r.bounds.span,
            if r.held() { "yes" } else { "NO" }
        );
    }
    let _ = writeln!(
        s,
        "\n| registry | session ended | after its first session's start | past the span bound \
         | horizon | fills held | replays applied | duplicate | before watermark | beyond horizon \
         | untimed |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for run in runs {
        let r = &run.registry;
        for e in &r.ends {
            let c = e.replays;
            let past = (e.past_span(r.bounds.span)).map_or("no".to_owned(), whole_ms);
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                run.step.name(),
                e.session,
                whole_ms(e.after),
                past,
                horizon(e.horizon),
                e.held,
                c.applied,
                c.duplicate,
                c.before_watermark,
                c.beyond_horizon,
                c.untimed
            );
        }
    }
    let _ = writeln!(s);
}

/// The evidence's entry for the place stages' registries: as [`place_markdown`], the list and
/// the ceiling as fences met.
pub fn place_to_json(runs: &[PlaceRun]) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let runs: Vec<Value> = (runs.iter())
        .map(|run| {
            let r = &run.registry;
            let ends: Vec<Value> = (r.ends.iter())
                .map(|e| {
                    let c = e.replays;
                    json!({"session": e.session, "after_ms": ms(e.after),
                        "past_span_ms": e.past_span(r.bounds.span).map(ms),
                        "horizon": horizon(e.horizon), "fills_held": e.held,
                        "replays": {"applied": c.applied, "duplicate": c.duplicate,
                            "before_watermark": c.before_watermark,
                            "beyond_horizon": c.beyond_horizon, "untimed": c.untimed}})
                })
                .collect();
            json!({"step": run.step.name(), "account": r.account,
                "declaration": run.declaration, "testnet_run": r.testnet_run,
                "ledger_max_age_ms": ms(r.bounds.max_age), "registry_span_ms": ms(r.bounds.span),
                "ledger_max_entries": PLACE_CEILING, "held": r.held(), "session_ends": ends})
        })
        .collect();
    Value::Array(runs)
}

/// A horizon as the report and the evidence name it.
fn horizon(h: Horizon) -> String {
    match h {
        Horizon::Full => "full".to_owned(),
        Horizon::At(exch) => format!("at {} (venue ns)", exch.0),
        Horizon::Lost => "lost".to_owned(),
    }
}

/// A horizon as decision 0132's list says it: full, moved (its instant never shown) or lost.
pub fn horizon_word(h: Horizon) -> &'static str {
    match h {
        Horizon::Full => "full",
        Horizon::At(_) => "moved",
        Horizon::Lost => "lost",
    }
}

/// What the ceiling counts, as the report names it.
fn counted(b: &Bounds) -> String {
    match b.places.is_empty() {
        true => "no order: its sessions place none".to_owned(),
        false => (b.places.iter())
            .map(|p| {
                format!(
                    "{} {} of at most {} steps",
                    p.orders, p.what, p.largest_steps
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    }
}

/// The report's section on the registries' ledgers.
pub fn markdown(s: &mut String, registries: &[RegistryRun]) {
    let _ = writeln!(
        s,
        "## The fill ledger of each registry\n\n\
         Decision 0132 item 5: each registry keeps one fill ledger across every session it \
         serves, its age bound longer than the registry's span bound and its count bound above \
         the fills its sessions can see, so a fill heard live is still held, and judged a \
         duplicate, when a resync reads it again. A session that reaches the span bound is \
         stopped, and then takes a moment to end (past the span bound, below). The bounds held \
         for a registry when, at each of its sessions' ends, its horizon is still full and the \
         session ended within the age bound of the registry's first session's start: its ledger \
         forgot no fill, and none it applied could have aged out. A registry is a declared \
         owner-assisted testnet run (decision 0067) on pre-production, as decision 0132 item 7 \
         says, and never on production (decision 0143 item 3): stage one seeds and arms no \
         market.\n\n\
         | registry | max age | max entries | span bound | fill ceiling | what the ceiling counts | held | testnet run |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for r in registries {
        let b = &r.bounds;
        let _ = writeln!(
            s,
            "| {} | {:?} | {} | {:?} | {} | {} | {} | {} |",
            r.account,
            b.max_age,
            b.max_entries,
            b.span,
            b.ceiling,
            counted(b),
            if r.held() { "yes" } else { "NO" },
            if r.testnet_run { "yes" } else { "no" }
        );
    }
    let _ = writeln!(
        s,
        "\n| registry | session ended | after its first session's start | past the span bound \
         | horizon | fills held | replays applied | duplicate | before watermark | beyond horizon \
         | untimed |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for r in registries {
        for e in &r.ends {
            let c = e.replays;
            let past = (e.past_span(r.bounds.span)).map_or("no".to_owned(), whole_ms);
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                r.account,
                e.session,
                whole_ms(e.after),
                past,
                horizon(e.horizon),
                e.held,
                c.applied,
                c.duplicate,
                c.before_watermark,
                c.beyond_horizon,
                c.untimed
            );
        }
    }
    let _ = writeln!(s);
}

/// The evidence's entry for the registries' ledgers.
pub fn to_json(registries: &[RegistryRun]) -> Value {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let registries: Vec<Value> = (registries.iter())
        .map(|r| {
            let b = &r.bounds;
            let places: Vec<Value> = (b.places.iter())
                .map(|p| json!({"what": p.what, "orders": p.orders, "largest_steps": p.largest_steps}))
                .collect();
            let ends: Vec<Value> = (r.ends.iter())
                .map(|e| {
                    let c = e.replays;
                    json!({"session": e.session, "after_ms": ms(e.after),
                        "past_span_ms": e.past_span(b.span).map(ms),
                        "horizon": horizon(e.horizon), "fills_held": e.held,
                        "replays": {"applied": c.applied, "duplicate": c.duplicate,
                            "before_watermark": c.before_watermark,
                            "beyond_horizon": c.beyond_horizon, "untimed": c.untimed}})
                })
                .collect();
            json!({"account": r.account, "testnet_run": r.testnet_run,
                "ledger_max_age_ms": ms(b.max_age),
                "ledger_max_entries": b.max_entries, "registry_span_ms": ms(b.span),
                "fill_ceiling": b.ceiling, "ceiling_counts": places, "held": r.held(),
                "session_ends": ends})
        })
        .collect();
    Value::Array(registries)
}
