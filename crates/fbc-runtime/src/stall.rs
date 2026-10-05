//! The bound on a socket write that a peer stopped reading (FBC-ha3, decision 0035).
//!
//! A session's write that has not completed within the consumer's [`WriteStall`] window is
//! abandoned: the epoch ends as a drop and the session reconnects through its
//! [`ReconnectPacing`](crate::ReconnectPacing). The window is the consumer's, with no default in
//! code (0009), and it is neither the attempt deadline nor the silence window.

use std::fmt;
use std::time::Duration;

/// The consumer's write-stall window: the longest one write may wait on its peer.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct WriteStall {
    window: Duration,
}

/// Why a [`WriteStall`] was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum WriteStallError {
    /// A zero window would abandon every write that did not complete at once.
    Zero,
}

impl fmt::Display for WriteStallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WriteStallError::Zero => "the write-stall window is zero",
        })
    }
}

impl std::error::Error for WriteStallError {}

impl WriteStall {
    /// A write not completed `window` after it began ends its epoch as a drop. A window past
    /// the end of the clock never runs out.
    pub fn new(window: Duration) -> Result<WriteStall, WriteStallError> {
        if window.is_zero() {
            return Err(WriteStallError::Zero);
        }
        Ok(WriteStall { window })
    }

    pub fn window(&self) -> Duration {
        self.window
    }
}
