//! The bound on an order-entry epoch's resync (FBC-nwpr, decisions 0013, 0053, 0058).
//!
//! An epoch takes no place or amend until the venue has accepted its cancel-on-disconnect arm
//! and its resync's `ResyncEnd` has reached the handler (0058). The arm has a deadline, its
//! request's own; the consumer's [`ResyncDeadline`] bounds the resync: once it has passed since
//! the session asked the codec's resync, an epoch whose `ResyncEnd` has not reached the handler
//! ends as a drop, counted, and the session reconnects through its
//! [`ReconnectPacing`](crate::ReconnectPacing). The deadline is the consumer's, with no default
//! in code (0009), and it is neither a request's timeout nor the attempt deadline.

use std::fmt;
use std::time::Duration;

/// The consumer's resync deadline: the longest an epoch's resync may run before the epoch ends.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ResyncDeadline {
    after: Duration,
}

/// Why a [`ResyncDeadline`] was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ResyncDeadlineError {
    /// A zero deadline would end every epoch as its resync was asked.
    Zero,
}

impl fmt::Display for ResyncDeadlineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ResyncDeadlineError::Zero => "the resync deadline is zero",
        })
    }
}

impl std::error::Error for ResyncDeadlineError {}

impl ResyncDeadline {
    /// An epoch whose resync has not ended `after` it was asked ends as a drop. A deadline past
    /// the end of the clock never falls due.
    pub fn new(after: Duration) -> Result<ResyncDeadline, ResyncDeadlineError> {
        if after.is_zero() {
            return Err(ResyncDeadlineError::Zero);
        }
        Ok(ResyncDeadline { after })
    }

    pub fn after(&self) -> Duration {
        self.after
    }
}
