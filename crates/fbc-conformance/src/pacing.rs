//! Holding the connection attempts a stub saw to the client's [`ReconnectPacing`] (decision
//! 0023): consecutive attempts are at least the floor apart, and no more than the budget start
//! in any half-open window.

use std::fmt;
use std::time::Duration;

use fbc_runtime::ReconnectPacing;
use tokio::time::Instant;

/// The first attempt that broke the pacing, numbered from 0.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PacingBreach {
    /// Attempt `attempt` started only `gap` after the one before it, under the floor.
    Floor { attempt: usize, gap: Duration },
    /// Attempt `attempt` started within the window of attempt `first`, though `budget` attempts
    /// had already started in it.
    Budget {
        attempt: usize,
        first: usize,
        budget: u32,
    },
}

impl fmt::Display for PacingBreach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacingBreach::Floor { attempt, gap } => {
                write!(
                    f,
                    "attempt {attempt} started {gap:?} after the last, under the floor"
                )
            }
            PacingBreach::Budget {
                attempt,
                first,
                budget,
            } => write!(
                f,
                "attempt {attempt} is past the budget of {budget} in the window of attempt {first}"
            ),
        }
    }
}

impl std::error::Error for PacingBreach {}

/// Checks the start instants of a client's connection attempts, in order, against `pacing`.
pub fn check_pacing(starts: &[Instant], pacing: &ReconnectPacing) -> Result<(), PacingBreach> {
    for (attempt, pair) in (1..).zip(starts.windows(2)) {
        let gap = pair[1].saturating_duration_since(pair[0]);
        if gap < pacing.floor() {
            return Err(PacingBreach::Floor { attempt, gap });
        }
    }
    let budget = pacing.budget();
    let span = usize::try_from(budget).unwrap_or(usize::MAX);
    for (first, window) in starts.windows(span.saturating_add(1)).enumerate() {
        if window[span].saturating_duration_since(window[0]) < pacing.window() {
            return Err(PacingBreach::Budget {
                attempt: first + span,
                first,
                budget,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn attempts_under_the_floor_or_past_the_budget_are_named() {
        let pacing = ReconnectPacing::new(s(1), s(4), 2, s(10), s(1)).unwrap();
        let t0 = Instant::now();
        let at = |secs: &[u64]| secs.iter().map(|n| t0 + s(*n)).collect::<Vec<_>>();
        assert_eq!(check_pacing(&at(&[0, 1, 10, 11, 20]), &pacing), Ok(()));
        assert_eq!(check_pacing(&[], &pacing), Ok(()));

        let floor = check_pacing(&at(&[0, 10, 10]), &pacing).unwrap_err();
        assert_eq!(
            floor,
            PacingBreach::Floor {
                attempt: 2,
                gap: s(0)
            }
        );
        assert_eq!(
            floor.to_string(),
            "attempt 2 started 0ns after the last, under the floor"
        );

        let budget = check_pacing(&at(&[0, 1, 10, 11, 12]), &pacing).unwrap_err();
        assert_eq!(
            budget,
            PacingBreach::Budget {
                attempt: 4,
                first: 2,
                budget: 2
            }
        );
        assert_eq!(
            budget.to_string(),
            "attempt 4 is past the budget of 2 in the window of attempt 2"
        );
    }
}
