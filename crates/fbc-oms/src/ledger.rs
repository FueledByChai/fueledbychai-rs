//! The fill ledger: every fill the OMS applies passes it first, deduplicated by
//! [`FillEvent::key`] (decision 0005, I3).
//!
//! A live fill is applied when the ledger holds no fill under its key. A replayed or snapshot
//! fill ([`FillEvent::replay`]) only reconciles: it is applied only when the ledger holds no
//! fill under its key, its execution time is later than the session-start watermark, and the
//! ledger can still vouch that it never saw it. The ledger is bounded by an age and a count
//! from the consumer's configuration, so it forgets; each fill it forgets moves its retention
//! horizon to that fill's venue time, and a replayed fill at or before the horizon, which the
//! ledger may have applied and forgotten, is never applied: it is counted, so a fill evicted
//! and replayed after a long reconnect cannot move inventory twice.
//!
//! A fill applies only through the [`AcceptedFill`] the ledger hands back, which
//! [`Registry::apply_fill`](crate::Registry::apply_fill) consumes: no other path moves an
//! order's fill count or the inventory. The ledger records a fill only when the registry
//! counts it, so a fill the registry refuses or only flags is routed again when it is
//! delivered again, and never moves the retention horizon. The
//! accepted fill borrows the ledger until it is applied or dropped, the ledger cannot be
//! cloned, and a registry takes fills from one ledger only, so no fill is accepted twice.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{ExchNs, ExchTsKind, FillEvent, FillKey, MonoNs, WallNs};

/// How much the ledger remembers: the consumer's configuration, with no defaults.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct LedgerConfig {
    /// How long a fill is kept after it was applied, on the shard's monotonic clock.
    pub max_age: Duration,
    /// How many fills are kept at most; the oldest goes first.
    pub max_entries: usize,
}

/// A [`LedgerConfig`] the ledger refuses.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LedgerConfigError {
    /// `max_age` is zero: the ledger would forget every fill at once.
    ZeroAge,
    /// `max_entries` is zero: the ledger could hold no fill.
    ZeroEntries,
}

impl fmt::Display for LedgerConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LedgerConfigError::ZeroAge => write!(f, "the fill ledger's max_age is zero"),
            LedgerConfigError::ZeroEntries => write!(f, "the fill ledger's max_entries is zero"),
        }
    }
}

impl std::error::Error for LedgerConfigError {}

/// When the venue says a fill happened: the envelope's exchange timestamp and its kind, with
/// the same instant on the local wall clock as the connection's clock-skew estimate aligns it
/// (decision 0014), which is the clock of the session-start watermark.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct FillTime {
    /// The venue's timestamp.
    pub exch: ExchNs,
    /// Which instant it marks. Only a matching-engine time is a replayed fill's execution
    /// time; a publish time of a replay is when it was sent again.
    pub kind: ExchTsKind,
    /// `exch` on the local wall clock.
    pub aligned: WallNs,
}

/// How far back the ledger can vouch for the fills it applied.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Horizon {
    /// Nothing has been forgotten: the ledger holds every fill it applied.
    Full,
    /// Fills whose venue time is at or before this one may have been forgotten.
    At(ExchNs),
    /// A fill with no usable venue time was forgotten, so no replayed fill can be shown to
    /// differ from it.
    Lost,
}

/// What [`FillLedger::admit`] decided about the fill it was given.
#[derive(Debug)]
pub enum Admission<'l, 'f> {
    /// Apply it: hand it to [`Registry::apply_fill`](crate::Registry::apply_fill).
    Apply(AcceptedFill<'l, 'f>),
    /// The ledger holds a fill under its key.
    Duplicate,
    /// A replayed fill executed at or before the session-start watermark: the session's
    /// starting position already holds it.
    BeforeWatermark,
    /// A replayed fill at or before the retention horizon: the ledger may have applied it and
    /// forgotten it.
    BeyondHorizon,
    /// A replayed fill without a matching-engine time: nothing shows it is newer than the
    /// watermark.
    Untimed,
}

/// A fill the ledger accepted, to be applied once. It cannot be cloned or built outside the
/// ledger, holds the ledger until [`Registry::apply_fill`](crate::Registry::apply_fill)
/// consumes it, and is recorded in the ledger only then; dropped, it leaves nothing there.
#[derive(Debug)]
pub struct AcceptedFill<'l, 'f> {
    ledger: &'l mut FillLedger,
    fill: &'f FillEvent,
    time: Option<FillTime>,
    now: MonoNs,
}

impl<'f> AcceptedFill<'_, 'f> {
    /// The fill.
    pub fn fill(&self) -> &'f FillEvent {
        self.fill
    }

    /// Which ledger accepted it.
    pub(crate) fn ledger_id(&self) -> u64 {
        self.ledger.id
    }

    /// Records the fill in the ledger that accepted it: called once it is applied.
    pub(crate) fn commit(self) {
        self.ledger.record(self.fill, self.time, self.now);
    }
}

/// What became of the replayed fills the ledger was given.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ReplayCounts {
    /// Applied: absent and newer than the watermark and the horizon.
    pub applied: u64,
    /// Already held.
    pub duplicate: u64,
    /// At or before the session-start watermark.
    pub before_watermark: u64,
    /// At or before the retention horizon.
    pub beyond_horizon: u64,
    /// Without a matching-engine time.
    pub untimed: u64,
}

#[derive(Debug)]
struct Entry {
    key: FillKey,
    applied_at: MonoNs,
    /// The latest instant the fill can have happened at on the venue's clock, when known: a
    /// matching-engine or publish time, never earlier than the execution.
    bound: Option<ExchNs>,
}

/// The fills the OMS applied, deduplicated by [`FillEvent::key`] and bounded by age and count.
/// Not `Clone`: two copies could each accept the same fill once.
#[derive(Debug)]
pub struct FillLedger {
    /// This ledger, among every ledger the process made.
    id: u64,
    config: LedgerConfig,
    watermark: WallNs,
    keys: HashSet<FillKey>,
    entries: VecDeque<Entry>,
    horizon: Horizon,
    replays: ReplayCounts,
}

impl FillLedger {
    /// An empty ledger for a session that started at `watermark` (the instant on the local
    /// wall clock its starting position reflects). Refused when `config` would hold nothing.
    pub fn new(config: LedgerConfig, watermark: WallNs) -> Result<FillLedger, LedgerConfigError> {
        if config.max_age.is_zero() {
            return Err(LedgerConfigError::ZeroAge);
        }
        if config.max_entries == 0 {
            return Err(LedgerConfigError::ZeroEntries);
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Ok(FillLedger {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            config,
            watermark,
            keys: HashSet::new(),
            entries: VecDeque::new(),
            horizon: Horizon::Full,
            replays: ReplayCounts::default(),
        })
    }

    /// Decides whether `fill`, which the venue timed `time` (`None` when it sent no
    /// timestamp), is applied, at `now` on the shard's monotonic clock.
    ///
    /// A fill whose key the ledger holds is a duplicate. Otherwise a live fill is accepted. A
    /// replayed fill is accepted only when it has a matching-engine time later than the
    /// session-start watermark (aligned) and than the retention horizon (on the venue's
    /// clock); otherwise it is refused and counted, and the ledger keeps nothing of it. An
    /// accepted fill is recorded under its key when the registry applies it; then the fills
    /// older than the configured age, and the oldest beyond the configured count, are
    /// forgotten, each moving the horizon.
    pub fn admit<'l, 'f>(
        &'l mut self,
        fill: &'f FillEvent,
        time: Option<FillTime>,
        now: MonoNs,
    ) -> Admission<'l, 'f> {
        let key = fill.key();
        if self.keys.contains(&key) {
            if fill.replay {
                self.replays.duplicate += 1;
            }
            return Admission::Duplicate;
        }
        if fill.replay
            && let Some(refused) = self.refuse_replay(time)
        {
            return refused;
        }
        Admission::Apply(AcceptedFill {
            ledger: self,
            fill,
            time,
            now,
        })
    }

    /// Records an accepted fill once it is applied, forgetting what the age and count
    /// configured no longer keep.
    fn record(&mut self, fill: &FillEvent, time: Option<FillTime>, now: MonoNs) {
        if fill.replay {
            self.replays.applied += 1;
        }
        let key = fill.key();
        self.forget_older_than(now);
        self.keys.insert(key.clone());
        self.entries.push_back(Entry {
            key,
            applied_at: now,
            bound: time
                .filter(|t| matches!(t.kind, ExchTsKind::MatchingEngine | ExchTsKind::Publish))
                .map(|t| t.exch),
        });
        let mut excess = self.entries.len().saturating_sub(self.config.max_entries);
        while let Some(gone) = self.entries.pop_front_if(|_| excess > 0) {
            excess -= 1;
            self.forget(gone);
        }
    }

    /// Why a replayed fill timed `time`, absent from the ledger, is not applied; `None` when it
    /// is. Counts the refusal.
    fn refuse_replay(&mut self, time: Option<FillTime>) -> Option<Admission<'static, 'static>> {
        let Some(t) = time.filter(|t| t.kind == ExchTsKind::MatchingEngine) else {
            self.replays.untimed += 1;
            return Some(Admission::Untimed);
        };
        if t.aligned <= self.watermark {
            self.replays.before_watermark += 1;
            return Some(Admission::BeforeWatermark);
        }
        let beyond = match self.horizon {
            Horizon::Full => false,
            Horizon::At(h) => t.exch <= h,
            Horizon::Lost => true,
        };
        if beyond {
            self.replays.beyond_horizon += 1;
            return Some(Admission::BeyondHorizon);
        }
        None
    }

    fn forget_older_than(&mut self, now: MonoNs) {
        let max_age = self.config.max_age;
        while let Some(gone) = self.entries.pop_front_if(|e| now - e.applied_at >= max_age) {
            self.forget(gone);
        }
    }

    /// Forgets a fill taken off the ledger, moving the horizon to its venue time.
    fn forget(&mut self, gone: Entry) {
        self.keys.remove(&gone.key);
        self.horizon = match (self.horizon, gone.bound) {
            (Horizon::Lost, _) | (_, None) => Horizon::Lost,
            (Horizon::Full, Some(t)) => Horizon::At(t),
            (Horizon::At(h), Some(t)) => Horizon::At(h.max(t)),
        };
    }

    /// Whether the ledger holds a fill under `key`.
    pub fn contains(&self, key: &FillKey) -> bool {
        self.keys.contains(key)
    }

    /// How many fills it holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The session-start watermark.
    pub fn watermark(&self) -> WallNs {
        self.watermark
    }

    /// How far back it can vouch for what it applied.
    pub fn horizon(&self) -> Horizon {
        self.horizon
    }

    /// What became of the replayed fills it was given.
    pub fn replays(&self) -> ReplayCounts {
        self.replays
    }
}
