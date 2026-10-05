//! Keeping a socket endpoint alive and noticing when it is not (FBC-djl, decision 0033).
//!
//! A session sends its codec's [`Keepalive`](fbc_core::Keepalive) at the interval the codec
//! declares, rotates a connection to its next epoch before the venue's
//! [`max_conn_lifetime`](fbc_core::MdCaps::max_conn_lifetime) by the consumer's margin, and
//! reports a stream that has received nothing within the consumer's silence window stale before
//! reconnecting it under a new epoch. The two numbers here are the consumer's; there is no
//! default in code (0009).

use std::fmt;
use std::time::Duration;

use fbc_core::Keepalive;
use tokio::time::Instant;

/// The consumer's liveness settings for a socket endpoint.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Liveness {
    silence: Duration,
    rotation_margin: Duration,
}

/// Why a [`Liveness`] was refused, or does not fit a venue.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LivenessError {
    /// A zero silence window would call every stream silent at once.
    ZeroSilence,
    /// A zero margin would race the venue's own close.
    ZeroMargin,
    /// The margin is as long as the venue's connection lifetime or longer, so a connection
    /// would rotate the moment it opened.
    MarginNotBelowLifetime,
}

impl fmt::Display for LivenessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LivenessError::ZeroSilence => "the silence window is zero",
            LivenessError::ZeroMargin => "the rotation margin is zero",
            LivenessError::MarginNotBelowLifetime => {
                "the rotation margin is not below the venue's connection lifetime"
            }
        })
    }
}

impl std::error::Error for LivenessError {}

impl Liveness {
    /// A stream that receives no frame, heartbeats included, within `silence` is reported
    /// stale and reconnected; a connection is rotated `rotation_margin` before the venue's
    /// lifetime ends.
    pub fn new(silence: Duration, rotation_margin: Duration) -> Result<Liveness, LivenessError> {
        if silence.is_zero() {
            return Err(LivenessError::ZeroSilence);
        }
        if rotation_margin.is_zero() {
            return Err(LivenessError::ZeroMargin);
        }
        Ok(Liveness {
            silence,
            rotation_margin,
        })
    }

    pub fn silence(&self) -> Duration {
        self.silence
    }

    pub fn rotation_margin(&self) -> Duration {
        self.rotation_margin
    }

    /// How long after it opens a connection to a venue whose connections live at most
    /// `lifetime` is rotated: `None` when the venue sets no limit.
    pub(crate) fn rotate_after(
        &self,
        lifetime: Option<Duration>,
    ) -> Result<Option<Duration>, LivenessError> {
        let Some(lifetime) = lifetime else {
            return Ok(None);
        };
        match lifetime.checked_sub(self.rotation_margin) {
            Some(after) if !after.is_zero() => Ok(Some(after)),
            _ => Err(LivenessError::MarginNotBelowLifetime),
        }
    }
}

/// One socket epoch's liveness: its keepalive and when the next falls due, when its silence
/// window runs out, and when it rotates.
pub(crate) struct Alive {
    pub(crate) keepalive: Option<Keepalive>,
    pub(crate) keepalive_at: Option<Instant>,
    pub(crate) silent_at: Option<Instant>,
    pub(crate) rotate_at: Option<Instant>,
    silence: Duration,
}

impl Alive {
    /// The liveness of an epoch opening now with the codec's `keepalive`, or none at all on a
    /// poll endpoint (`socket` false); true with it when the keepalive was refused for its
    /// zero interval. A deadline past the end of the clock never falls due.
    pub(crate) fn open(
        socket: bool,
        keepalive: Option<Keepalive>,
        silence: Duration,
        rotate_after: Option<Duration>,
    ) -> (Alive, bool) {
        let refused = socket && keepalive.as_ref().is_some_and(|k| k.interval.is_zero());
        let now = Instant::now();
        let mut alive = Alive {
            keepalive: keepalive.filter(|_| socket && !refused),
            keepalive_at: None,
            silent_at: None,
            rotate_at: None,
            silence,
        };
        if socket {
            alive.beat();
            alive.heard();
            alive.rotate_at = rotate_after.and_then(|after| now.checked_add(after));
        }
        (alive, refused)
    }

    /// A frame was heard now: the silence window starts again.
    pub(crate) fn heard(&mut self) {
        self.silent_at = Instant::now().checked_add(self.silence);
    }

    /// True when the silence window has run out by `now`; never on a poll endpoint.
    pub(crate) fn silent_by(&self, now: Instant) -> bool {
        self.silent_at.is_some_and(|at| at <= now)
    }

    /// A keepalive goes now: the next falls due an interval later.
    pub(crate) fn beat(&mut self) {
        let next = |k: &Keepalive| Instant::now().checked_add(k.interval);
        self.keepalive_at = self.keepalive.as_ref().and_then(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_has_run_out_from_its_deadline_on_and_never_on_a_poll_endpoint() {
        let s = Duration::from_secs;
        let (socket, _) = Alive::open(true, None, s(5), Some(s(5)));
        let at = socket.silent_at.unwrap();
        assert!(!socket.silent_by(at - Duration::from_nanos(1)));
        assert!(socket.silent_by(at));
        assert!(socket.silent_by(at + s(1)));
        let (poll, _) = Alive::open(false, None, s(5), Some(s(5)));
        assert!(!poll.silent_by(at + s(3_600)));
    }

    #[test]
    fn a_connection_rotates_its_margin_before_the_venues_lifetime() {
        let s = Duration::from_secs;
        let liveness = Liveness::new(s(1), s(60)).unwrap();
        assert_eq!(liveness.rotate_after(None), Ok(None));
        assert_eq!(liveness.rotate_after(Some(s(86_400))), Ok(Some(s(86_340))));
        for lifetime in [s(60), s(59)] {
            assert_eq!(
                liveness.rotate_after(Some(lifetime)),
                Err(LivenessError::MarginNotBelowLifetime)
            );
        }
    }
}
