//! Clocks that cannot be mixed (decision 0004, design §4.1).
//!
//! Four kinds of instant, each its own type. Subtraction exists only within a kind:
//! `MonoNs - MonoNs` is a [`Duration`] for latency maths, and the three wall-clock kinds give a
//! signed nanosecond difference. There is no subtraction between kinds, so `WallNs - ExchNs`
//! does not compile (the `compile_fail` test proves every such pair). The only bridge between
//! exchange and local time is an explicit skew estimate, never an operator.

use core::ops::{Add, Sub};
use core::time::Duration;

/// Monotonic nanoseconds since an arbitrary process-local origin; for latency maths.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MonoNs(pub u64);

/// Wall-clock nanoseconds since the Unix epoch; for cross-process alignment.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WallNs(pub i64);

/// Venue-reported nanoseconds since the Unix epoch. Never synthesized from a local clock.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ExchNs(pub i64);

/// Which instant a venue's timestamp marks.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ExchTsKind {
    /// When the matching engine processed the event.
    MatchingEngine,
    /// When the venue published the message.
    Publish,
    /// The venue does not say.
    Unknown,
}

/// Kernel receive timestamp of the last packet of a frame (`SO_TIMESTAMPNS`, realtime clock).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct KernelRxNs(pub i64);

impl Sub for MonoNs {
    type Output = Duration;
    /// Elapsed time from `rhs` to `self`; zero when `rhs` is later.
    fn sub(self, rhs: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(rhs.0))
    }
}

impl Add<Duration> for MonoNs {
    type Output = MonoNs;
    /// The instant `rhs` after `self`, saturating at the end of the clock.
    fn add(self, rhs: Duration) -> MonoNs {
        let nanos = u64::try_from(rhs.as_nanos()).unwrap_or(u64::MAX);
        MonoNs(self.0.saturating_add(nanos))
    }
}

macro_rules! signed_difference {
    ($($kind:ident),*) => {$(
        impl Sub for $kind {
            /// Signed nanoseconds from `rhs` to `self`, saturating; negative when `rhs` is later.
            type Output = i64;
            fn sub(self, rhs: Self) -> i64 {
                self.0.saturating_sub(rhs.0)
            }
        }
    )*};
}
signed_difference!(WallNs, ExchNs, KernelRxNs);

/// One connection of a venue session and its epoch: the epoch rises on every reconnect, so
/// anything stamped with an older epoch is known to predate the current socket.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ConnKey {
    pub conn: u16,
    pub epoch: u32,
}

/// Where and when a frame arrived, recorded before it is decoded.
#[derive(Copy, Clone, Debug)]
pub struct Stamp {
    /// Shard-global consumption order, which is also replay order.
    pub ingest_seq: u64,
    /// Kernel receive time of the frame's last packet; `None` where the platform has none.
    pub kernel_rx: Option<KernelRxNs>,
    /// User-space frame completion, before decode.
    pub recv_mono: MonoNs,
    /// The same instant on the wall clock.
    pub recv_wall: WallNs,
    pub conn: ConnKey,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mono_difference_is_a_duration_and_saturates_at_zero() {
        assert_eq!(MonoNs(1_500) - MonoNs(500), Duration::from_nanos(1_000));
        assert_eq!(MonoNs(500) - MonoNs(1_500), Duration::ZERO);
    }

    #[test]
    fn mono_plus_duration_moves_forward_and_saturates() {
        assert_eq!(MonoNs(10) + Duration::from_nanos(5), MonoNs(15));
        assert_eq!(
            MonoNs(u64::MAX - 1) + Duration::from_secs(1),
            MonoNs(u64::MAX)
        );
        assert_eq!(MonoNs(0) + Duration::MAX, MonoNs(u64::MAX));
    }

    #[test]
    fn wall_kinds_give_a_signed_difference_within_their_kind() {
        assert_eq!(WallNs(10) - WallNs(25), -15);
        assert_eq!(ExchNs(25) - ExchNs(10), 15);
        assert_eq!(KernelRxNs(i64::MIN) - KernelRxNs(1), i64::MIN);
    }

    #[test]
    fn a_stamp_carries_its_connection_epoch() {
        let conn = ConnKey { conn: 2, epoch: 7 };
        let stamp = Stamp {
            ingest_seq: 42,
            kernel_rx: None,
            recv_mono: MonoNs(1),
            recv_wall: WallNs(2),
            conn,
        };
        assert_eq!(stamp.conn, ConnKey { conn: 2, epoch: 7 });
        assert_ne!(stamp.conn, ConnKey { conn: 2, epoch: 8 });
        assert_eq!(stamp.ingest_seq, 42);
    }
}
