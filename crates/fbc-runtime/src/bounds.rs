//! The bounds an order-entry session keeps on what it holds, each with a reserve for Safety
//! traffic (FBC-hwn, design §5.4, decision 0105).
//!
//! A [`ReservedBound`] is a number of Normal entries plus a reserve only Safety traffic (a
//! cancel, a reducing order) may use, both from the consumer's configuration with no default
//! (0009). An [`ExecSession`](crate::ExecSession) keeps two: the commands waiting for its
//! connection ([`ExecSessionConfig::outbound`](crate::ExecSessionConfig::outbound)), past which a
//! submission is `NotSent(Backpressure)`, and the requests in flight
//! ([`ExecSessionConfig::in_flight`](crate::ExecSessionConfig::in_flight), an [`InFlight`]
//! table the consumer may share between the sessions of one venue), past which a submission is
//! `NotSent(RateBudget)`. Normal traffic is admitted while fewer than the Normal bound are held,
//! Safety traffic while fewer than the Normal bound and the reserve together, as the rate
//! buckets' safety reserve is (0030, 0073).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use fbc_core::TrafficClass;

/// A bound of `normal` entries for Normal traffic plus `reserve` more that only Safety traffic
/// may use, from the consumer's configuration (decision 0105).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ReservedBound {
    normal: u32,
    reserve: u32,
}

/// Why a [`ReservedBound`] was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BoundError {
    /// A Normal bound of zero would refuse every place, amend and control command.
    NoNormal,
    /// The Normal bound and the reserve together exceed `u32::MAX`.
    TooLarge,
}

impl fmt::Display for BoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BoundError::NoNormal => "the normal bound is zero",
            BoundError::TooLarge => "the normal bound and the safety reserve exceed u32::MAX",
        })
    }
}

impl std::error::Error for BoundError {}

impl ReservedBound {
    /// `normal` entries for Normal traffic (at least one) and `reserve` more for Safety
    /// traffic only (none is allowed).
    pub fn new(normal: u32, reserve: u32) -> Result<ReservedBound, BoundError> {
        if normal == 0 {
            return Err(BoundError::NoNormal);
        }
        normal.checked_add(reserve).ok_or(BoundError::TooLarge)?;
        Ok(ReservedBound { normal, reserve })
    }

    /// The entries Normal traffic may fill.
    pub fn normal(self) -> u32 {
        self.normal
    }

    /// The entries beyond them only Safety traffic may fill.
    pub fn reserve(self) -> u32 {
        self.reserve
    }

    /// The Normal bound and the reserve together: what Safety traffic may fill.
    pub fn total(self) -> u32 {
        self.normal + self.reserve
    }

    /// Whether one more entry of `class` is admitted while `held` are held.
    pub(crate) fn admits(self, held: u64, class: TrafficClass) -> bool {
        let limit = match class {
            TrafficClass::Normal => self.normal,
            TrafficClass::Safety => self.total(),
        };
        held < u64::from(limit)
    }
}

/// A table of the order-entry requests in flight: written (or asked over HTTP) and neither
/// answered nor timed out, bounded by a [`ReservedBound`] (design §5.4's RPC table, decision
/// 0105). Clones share one count, so a consumer that hands a clone to every session of one
/// venue bounds the venue's requests together, and one that builds one per session bounds each
/// connection apart. A session admits a submitted command only while the table has room for its
/// class, and otherwise reports it `NotSent(RateBudget)` with nothing written; every other
/// request it writes (its own cancel-on-disconnect arm) is counted without being refused. A
/// session that ends gives back what it held.
#[derive(Clone, Debug)]
pub struct InFlight {
    bound: ReservedBound,
    held: Arc<AtomicU32>,
}

impl InFlight {
    /// An empty table bounded by `bound`.
    pub fn new(bound: ReservedBound) -> InFlight {
        InFlight {
            bound,
            held: Arc::default(),
        }
    }

    /// The table's bound.
    pub fn bound(&self) -> ReservedBound {
        self.bound
    }

    /// The requests in flight now, across every session sharing the table.
    pub fn held(&self) -> u32 {
        self.held.load(Ordering::Acquire)
    }

    /// Takes a place for a request of `class` if the bound admits it: true when taken.
    pub(crate) fn try_take(&self, class: TrafficClass) -> bool {
        let admit = |held: u32| {
            self.bound
                .admits(u64::from(held), class)
                .then(|| held.saturating_add(1))
        };
        self.held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, admit)
            .is_ok()
    }

    /// Takes a place whatever the bound: a request the session writes of its own.
    pub(crate) fn take(&self) {
        let more = |held: u32| Some(held.saturating_add(1));
        let _ = self
            .held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, more);
    }

    /// Gives back a place taken.
    pub(crate) fn release(&self) {
        let less = |held: u32| Some(held.saturating_sub(1));
        let _ = self
            .held
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, less);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bound_needs_a_normal_entry_and_a_total_that_fits() {
        assert_eq!(ReservedBound::new(0, 4), Err(BoundError::NoNormal));
        assert_eq!(ReservedBound::new(u32::MAX, 1), Err(BoundError::TooLarge));
        let bound = ReservedBound::new(8, 4).unwrap();
        assert_eq!((bound.normal(), bound.reserve(), bound.total()), (8, 4, 12));
        assert_eq!(ReservedBound::new(u32::MAX, 0).unwrap().total(), u32::MAX);
        assert_eq!(BoundError::NoNormal.to_string(), "the normal bound is zero");
        assert_eq!(
            BoundError::TooLarge.to_string(),
            "the normal bound and the safety reserve exceed u32::MAX"
        );
    }

    #[test]
    fn normal_traffic_stops_at_the_normal_bound_and_safety_at_the_total() {
        let bound = ReservedBound::new(2, 1).unwrap();
        let (normal, safety) = (TrafficClass::Normal, TrafficClass::Safety);
        assert!(bound.admits(1, normal) && !bound.admits(2, normal));
        assert!(bound.admits(2, safety) && !bound.admits(3, safety));
        // No reserve: Safety stops where Normal does.
        let bare = ReservedBound::new(1, 0).unwrap();
        assert!(!bare.admits(1, safety));
    }

    #[test]
    fn clones_of_a_table_share_one_count_and_a_forced_place_may_pass_the_bound() {
        let table = InFlight::new(ReservedBound::new(1, 1).unwrap());
        let other = table.clone();
        assert!(table.try_take(TrafficClass::Normal));
        assert!(!other.try_take(TrafficClass::Normal));
        assert!(other.try_take(TrafficClass::Safety));
        assert!(!table.try_take(TrafficClass::Safety));
        assert_eq!(other.held(), 2);
        // The session's own request is counted past the bound, and every place comes back.
        table.take();
        assert_eq!(table.held(), 3);
        for _ in 0..4 {
            other.release();
        }
        assert_eq!(table.held(), 0);
        assert_eq!(table.bound(), ReservedBound::new(1, 1).unwrap());
        assert!(format!("{table:?}").starts_with("InFlight"));
    }
}
