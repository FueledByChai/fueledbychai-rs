//! Reconnect pacing (decision 0023): how long a session waits before each connection attempt,
//! so an endpoint that keeps dropping cannot hammer the egress IP the deployment shares.
//!
//! Every number comes from the consumer's [`ReconnectPacing`]; there is no default in code
//! (0009). An attempt that has not opened by its deadline is abandoned and counts as failed.
//! The first attempt goes at once. After a connection that opened drops, the next
//! attempt waits the floor; each attempt that fails to open doubles the wait, up to the
//! ceiling. Independently, at most `budget` attempts start in any `window` (half-open, so two
//! attempts exactly `window` apart fall in different windows); an attempt the budget holds back
//! waits until the oldest of the last `budget` attempts leaves the window. A wait that would
//! pass the end of the clock never ends: the session waits for its control instead of panicking.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use tokio::time::Instant;

/// The consumer's reconnect pacing: exponential backoff between `floor` and `ceiling`, at most
/// `budget` connection attempts per `window`, and a `deadline` for each attempt to open.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ReconnectPacing {
    floor: Duration,
    ceiling: Duration,
    budget: NonZeroU32,
    window: Duration,
    deadline: Duration,
}

/// Why a [`ReconnectPacing`] was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum PacingError {
    /// A zero floor would reconnect at once after every drop and never back off.
    ZeroFloor,
    /// The ceiling is below the floor.
    CeilingBelowFloor,
    /// A budget of zero would never connect.
    ZeroBudget,
    /// A zero window bounds nothing.
    ZeroWindow,
    /// A zero deadline would abandon every attempt.
    ZeroDeadline,
}

impl fmt::Display for PacingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PacingError::ZeroFloor => "the reconnect floor is zero",
            PacingError::CeilingBelowFloor => "the reconnect ceiling is below the floor",
            PacingError::ZeroBudget => "the connection attempt budget is zero",
            PacingError::ZeroWindow => "the attempt budget's window is zero",
            PacingError::ZeroDeadline => "the connection attempt deadline is zero",
        })
    }
}

impl std::error::Error for PacingError {}

impl ReconnectPacing {
    /// Backoff from `floor` doubling to `ceiling`, at most `budget` attempts per `window`, and
    /// each attempt abandoned if it has not opened within `deadline`.
    pub fn new(
        floor: Duration,
        ceiling: Duration,
        budget: u32,
        window: Duration,
        deadline: Duration,
    ) -> Result<ReconnectPacing, PacingError> {
        if floor.is_zero() {
            return Err(PacingError::ZeroFloor);
        }
        if ceiling < floor {
            return Err(PacingError::CeilingBelowFloor);
        }
        let budget = NonZeroU32::new(budget).ok_or(PacingError::ZeroBudget)?;
        if window.is_zero() {
            return Err(PacingError::ZeroWindow);
        }
        if deadline.is_zero() {
            return Err(PacingError::ZeroDeadline);
        }
        Ok(ReconnectPacing {
            floor,
            ceiling,
            budget,
            window,
            deadline,
        })
    }

    pub fn floor(&self) -> Duration {
        self.floor
    }

    pub fn ceiling(&self) -> Duration {
        self.ceiling
    }

    pub fn budget(&self) -> u32 {
        self.budget.get()
    }

    pub fn window(&self) -> Duration {
        self.window
    }

    pub fn deadline(&self) -> Duration {
        self.deadline
    }

    /// The wait after `failures` consecutive attempts that did not open: the floor after a drop
    /// (no failure) or a first failure, doubling with each further one, capped at the ceiling.
    fn backoff(&self, failures: u32) -> Duration {
        let mut wait = self.floor;
        // Saturating, so the wait reaches any ceiling within about a hundred doublings.
        for _ in 1..failures {
            if wait >= self.ceiling {
                break;
            }
            wait = wait.saturating_mul(2);
        }
        wait.min(self.ceiling)
    }
}

/// One session's pacing state: when its recent attempts started and how many in a row failed.
#[derive(Debug)]
pub(crate) struct Pacer {
    pacing: ReconnectPacing,
    attempts: VecDeque<Instant>,
    failures: u32,
    ended: Option<Instant>,
}

impl Pacer {
    pub(crate) fn new(pacing: ReconnectPacing) -> Pacer {
        Pacer {
            pacing,
            attempts: VecDeque::new(),
            failures: 0,
            ended: None,
        }
    }

    /// When the next attempt may start: never before `now`, the backoff after the last attempt
    /// or connection ended, or the moment the budget frees a slot; `None` when that is past the
    /// end of the clock.
    pub(crate) fn next_attempt(&self, now: Instant) -> Option<Instant> {
        let mut at = now;
        if let Some(ended) = self.ended {
            at = at.max(ended.checked_add(self.pacing.backoff(self.failures))?);
        }
        if self.attempts.len() >= self.pacing.budget() as usize
            && let Some(oldest) = self.attempts.front()
        {
            at = at.max(oldest.checked_add(self.pacing.window)?);
        }
        Some(at)
    }

    /// How long an attempt may take to open.
    pub(crate) fn deadline(&self) -> Duration {
        self.pacing.deadline
    }

    /// An attempt started at `at`.
    pub(crate) fn attempted(&mut self, at: Instant) {
        self.attempts.push_back(at);
        while self.attempts.len() > self.pacing.budget() as usize {
            self.attempts.pop_front();
        }
    }

    /// The attempt failed at `at` without opening.
    pub(crate) fn failed(&mut self, at: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.ended = Some(at);
    }

    /// The attempt opened: the backoff starts again from the floor.
    pub(crate) fn opened(&mut self) {
        self.failures = 0;
    }

    /// The open connection ended at `at`.
    pub(crate) fn dropped(&mut self, at: Instant) {
        self.ended = Some(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn pacing(floor: u64, ceiling: u64, budget: u32, window: u64) -> ReconnectPacing {
        ReconnectPacing::new(ms(floor), ms(ceiling), budget, ms(window), ms(1)).unwrap()
    }

    #[test]
    fn every_number_is_the_consumers_and_a_useless_one_is_refused() {
        let p = pacing(100, 800, 3, 1_000);
        assert_eq!(
            (p.floor(), p.ceiling(), p.budget(), p.window(), p.deadline()),
            (ms(100), ms(800), 3, ms(1_000), ms(1))
        );
        let refused = |floor, ceiling, budget, window| {
            let err = ReconnectPacing::new(ms(floor), ms(ceiling), budget, ms(window), ms(1));
            (err.unwrap_err(), err.unwrap_err().to_string())
        };
        assert_eq!(
            refused(0, 1, 1, 1),
            (PacingError::ZeroFloor, "the reconnect floor is zero".into())
        );
        assert_eq!(
            refused(2, 1, 1, 1),
            (
                PacingError::CeilingBelowFloor,
                "the reconnect ceiling is below the floor".into()
            )
        );
        assert_eq!(
            refused(1, 1, 0, 1),
            (
                PacingError::ZeroBudget,
                "the connection attempt budget is zero".into()
            )
        );
        assert_eq!(
            refused(1, 1, 1, 0),
            (
                PacingError::ZeroWindow,
                "the attempt budget's window is zero".into()
            )
        );
        let err = ReconnectPacing::new(ms(1), ms(1), 1, ms(1), ms(0)).unwrap_err();
        assert_eq!(err, PacingError::ZeroDeadline);
        assert_eq!(err.to_string(), "the connection attempt deadline is zero");
    }

    #[test]
    fn the_backoff_doubles_from_the_floor_to_the_ceiling_and_never_overflows() {
        let p = pacing(100, 800, 1, 1);
        let waits: Vec<_> = (0..6).map(|n| p.backoff(n)).collect();
        assert_eq!(waits, [100, 100, 200, 400, 800, 800].map(ms));
        assert_eq!(p.backoff(u32::MAX), ms(800));
        // Past 2^31 floors the doubling goes on to the ceiling (Codex r4177068883).
        let ceiling = ms(1 << 40);
        let wide = ReconnectPacing::new(ms(1), ceiling, 1, ms(1), ms(1)).unwrap();
        assert_eq!(wide.backoff(33), ms(1 << 32));
        assert_eq!(wide.backoff(u32::MAX), ceiling);
        let unbounded = ReconnectPacing::new(ms(1), Duration::MAX, 1, ms(1), ms(1)).unwrap();
        assert_eq!(unbounded.backoff(u32::MAX), Duration::MAX);
    }

    #[test]
    fn a_wait_past_the_end_of_the_clock_never_ends_instead_of_panicking() {
        let t0 = Instant::now();
        let huge = Duration::MAX;
        let mut by_floor = Pacer::new(ReconnectPacing::new(huge, huge, 9, ms(1), ms(1)).unwrap());
        by_floor.attempted(t0);
        by_floor.opened();
        by_floor.dropped(t0);
        assert_eq!(by_floor.next_attempt(t0), None);
        let mut by_window = Pacer::new(ReconnectPacing::new(ms(1), ms(1), 1, huge, ms(1)).unwrap());
        by_window.attempted(t0);
        assert_eq!(by_window.next_attempt(t0), None);
    }

    #[test]
    fn the_first_attempt_goes_at_once_and_a_drop_waits_the_floor() {
        let t0 = Instant::now();
        let mut pacer = Pacer::new(pacing(100, 800, 10, 1_000));
        assert_eq!(pacer.next_attempt(t0), Some(t0));
        pacer.attempted(t0);
        pacer.failed(t0);
        pacer.attempted(t0 + ms(100));
        pacer.failed(t0 + ms(100));
        assert_eq!(pacer.next_attempt(t0 + ms(100)), Some(t0 + ms(300)));
        pacer.attempted(t0 + ms(300));
        pacer.opened();
        pacer.dropped(t0 + ms(5_000));
        assert_eq!(pacer.next_attempt(t0 + ms(5_000)), Some(t0 + ms(5_100)));
        assert_eq!(pacer.next_attempt(t0 + ms(9_000)), Some(t0 + ms(9_000)));
    }

    #[test]
    fn the_budget_holds_an_attempt_until_the_oldest_leaves_the_window() {
        let t0 = Instant::now();
        let mut pacer = Pacer::new(pacing(10, 10, 2, 1_000));
        for at in [0, 10] {
            pacer.attempted(t0 + ms(at));
            pacer.failed(t0 + ms(at));
        }
        assert_eq!(pacer.next_attempt(t0 + ms(10)), Some(t0 + ms(1_000)));
        pacer.attempted(t0 + ms(1_000));
        pacer.failed(t0 + ms(1_000));
        assert_eq!(pacer.next_attempt(t0 + ms(1_000)), Some(t0 + ms(1_010)));
    }
}
