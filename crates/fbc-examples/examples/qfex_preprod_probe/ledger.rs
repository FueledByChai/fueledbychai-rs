//! Each registry's fill ledger and the bounds it is held to (decision 0132 item 5, FBC-5hif).
//!
//! The probe builds its registries as decision 0132 item 7 says
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
//! **Extension points.** FBC-9sk2's place stages add the orders their sessions may place to the
//! ceiling (its place limits and size caps in size steps, and its flattening attempts: one
//! [`MayPlace`] each, beside [`stage_one`]'s none), and build each registry's [`Bounds`] from its
//! own list, since the command line accepts one against [`stage_one`] alone. They serve the drop
//! test's two sessions from one [`RegistryLedger`] (one [`RegistryLedger::session_ended`] each,
//! one deadline for both), and at the deadline cancel their orders through fbc-oms before the
//! stop (0132 item 7). Their handler routes each fill through [`RegistryLedger::admit`]
//! ([`judge`]), and they need what this file does not yet give:
//!
//! - **the registry itself**, to authorize places and cancels through the registry that owns the
//!   ledger: [`RegistryLedger`] keeps both private, and the shared glue (`src/link.rs`) owns a
//!   registry and a ledger of its own and shows neither the ledger's horizon nor its replay
//!   counts, so either this type lends its registry or hands both to the glue, which then shows
//!   them for [`RegistryLedger::session_ended`];
//! - **the registry's `CancelOnlyTrip` given to the journal**
//!   (`Journal::cancel_only_at_soft_limit`, decision 0139), so a soft-limit drop moves its
//!   markets to Cancel-only: stage one places nothing and gives it none (`capture.rs` reads the
//!   journal so);
//! - **a stop that ends by the bound**: the span bound here is when the stop is issued, and a
//!   session ends after it ([`AtEnd::past_span`] records by how much). Stage one's stop is its
//!   control alone, so the overrun is the moment the session takes to end; FBC-9sk2's cancels by
//!   venue id at the deadline can take up to `--rpc-timeout-ms` more, so it begins them that much
//!   before the deadline, or requires `--ledger-max-age` above the span bound by that much.
//!   Either way [`RegistryRun::held`] fails a registry whose session ended at or past the age
//!   bound.
//!
//! The span bound's floor, which the command line holds above a session's reach to its arm's
//! deadline, counts one session per registry: the drop test's two need it counted twice, with
//! the read between them. FBC-39ks's hold opens no session and places nothing, so it builds no
//! registry and adds nothing to the ceiling; held between two sessions of one registry, it would
//! count in that registry's span.

use std::fmt::Write as _;
use std::time::Duration;

use fbc_core::{AccountKey, FillEvent, MonoNs, WallNs};
use fbc_oms::{
    Admission, FillLedger, FillRouted, FillTime, Horizon, LedgerConfig, OmsError, Registry,
    ReplayCounts, TestnetRun,
};
use fbc_venue_qfex::probe::Environment;
use serde_json::{Value, json};

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

/// What stage one may place on a registry: nothing (decision 0132 item 7). FBC-9sk2 adds its
/// stages' orders beside this.
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

    /// The ledger's configuration.
    pub fn config(&self) -> LedgerConfig {
        LedgerConfig {
            max_age: self.max_age,
            max_entries: self.max_entries,
        }
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
#[derive(Copy, Clone, Debug)]
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
    /// fill of the registry's sessions takes. Stage one places nothing and its codec decodes no
    /// fill until FBC-jxau, so only its tests give one; FBC-9sk2's handler routes each fill
    /// through it.
    #[allow(dead_code)]
    pub fn admit(&mut self, fill: &FillEvent, time: Option<FillTime>, now: MonoNs) -> Judged {
        judge(&mut self.reg, &mut self.ledger, fill, time, now)
    }

    /// Records the ledger's state as the session `session` ended, at `now` on the run's
    /// monotonic clock ([`mono_since`]).
    pub fn session_ended(&mut self, session: &'static str, now: MonoNs) {
        self.run.ends.push(AtEnd {
            session,
            after: now - self.opened.mono,
            horizon: self.ledger.horizon(),
            held: self.ledger.len(),
            replays: self.ledger.replays(),
        });
    }

    /// What the report and the evidence say of it.
    pub fn run(&self) -> &RegistryRun {
        &self.run
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
    /// By how much the session ended past the span bound `span`: the bound is when the program
    /// stops a session, which then takes a moment to end. `None` when it ended within it.
    pub fn past_span(&self, span: Duration) -> Option<Duration> {
        self.after.checked_sub(span).filter(|past| !past.is_zero())
    }
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

/// A horizon as the report and the evidence name it.
fn horizon(h: Horizon) -> String {
    match h {
        Horizon::Full => "full".to_owned(),
        Horizon::At(exch) => format!("at {} (venue ns)", exch.0),
        Horizon::Lost => "lost".to_owned(),
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
            let past = (e.past_span(r.bounds.span)).map_or("no".to_owned(), |d| format!("{d:?}"));
            let _ = writeln!(
                s,
                "| {} | {} | {:?} | {} | {} | {} | {} | {} | {} | {} | {} |",
                r.account,
                e.session,
                e.after,
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
