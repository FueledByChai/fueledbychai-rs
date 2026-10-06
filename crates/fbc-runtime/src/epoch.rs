//! Connection epochs (decision 0002): one life of a stream's connection, and the gate that
//! drops whatever an older life left behind.
//!
//! A stream's [`Epochs`] numbers the lives of its connection. The first life is epoch 0, and a
//! close or a reconnect opens the next one ([`Epochs::advance`]); a codec lives for one epoch.
//! Every input the runtime attributes to a connection (a frame, a timer firing, an HTTP result,
//! an event a codec pushed) carries the [`ConnKey`] it was made under, and [`Epochs::admit`]
//! lets through only those of the current epoch: an older epoch's are dropped and counted per
//! [`Input`], so a dead connection's late arrivals never reach the current codec.
//!
//! This is logic only: no socket, no clock, no task.

use std::fmt;

use fbc_core::ConnKey;

/// A kind of input the runtime attributes to one connection epoch.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Input {
    /// A frame read from the socket.
    Frame,
    /// A timer the codec set, firing.
    Timer,
    /// The result of an HTTP request the codec asked for.
    Http,
    /// An event a codec pushed.
    Event,
}

impl Input {
    /// Every kind, in the order [`Epochs`] counts them.
    pub const ALL: [Input; 4] = [Input::Frame, Input::Timer, Input::Http, Input::Event];

    fn index(self) -> usize {
        match self {
            Input::Frame => 0,
            Input::Timer => 1,
            Input::Http => 2,
            Input::Event => 3,
        }
    }
}

/// What [`Epochs::admit`] decided about one input.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Admit {
    /// It belongs to the current epoch: pass it on.
    Current,
    /// It belongs to an older epoch: it was dropped and counted.
    Stale,
}

/// A rule of epoch numbering that a caller broke.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum EpochError {
    /// The input names another connection than this stream's: it was routed to the wrong
    /// stream.
    OtherConnection { stream: u16, input: u16 },
    /// The input names an epoch this stream has not opened yet.
    Unopened { current: u32, input: u32 },
    /// The stream is at the last epoch a `u32` can number; no further one can open.
    Exhausted { conn: u16 },
}

impl fmt::Display for EpochError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EpochError::OtherConnection { stream, input } => write!(
                f,
                "input of connection {input} reached the epochs of connection {stream}"
            ),
            EpochError::Unopened { current, input } => write!(
                f,
                "input of epoch {input} reached a connection still at epoch {current}"
            ),
            EpochError::Exhausted { conn } => {
                write!(f, "connection {conn} has no epoch left to open")
            }
        }
    }
}

impl std::error::Error for EpochError {}

/// The epochs of one stream's connection: the current one, and how many inputs of older ones
/// were dropped, by kind.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Epochs {
    current: ConnKey,
    stale: [u64; 4],
}

impl Epochs {
    /// The epochs of connection `conn`, at its first life, epoch 0.
    pub fn new(conn: u16) -> Epochs {
        Epochs {
            current: ConnKey { conn, epoch: 0 },
            stale: [0; 4],
        }
    }

    /// The current epoch.
    pub fn current(&self) -> ConnKey {
        self.current
    }

    /// The connection closed or reconnects: open the next epoch and return it. Everything
    /// attributed to an earlier epoch is stale from now on.
    pub fn advance(&mut self) -> Result<ConnKey, EpochError> {
        let conn = self.current.conn;
        self.current.epoch = self
            .current
            .epoch
            .checked_add(1)
            .ok_or(EpochError::Exhausted { conn })?;
        Ok(self.current)
    }

    /// Decide whether an input made under `key` passes: [`Admit::Current`] for the current
    /// epoch; [`Admit::Stale`] for an older one, which is dropped and counted under `input`.
    /// `Err` for an input of another connection or of an epoch not yet opened, which is a
    /// routing fault, not a late arrival, and is not counted.
    pub fn admit(&mut self, input: Input, key: ConnKey) -> Result<Admit, EpochError> {
        if key.conn != self.current.conn {
            return Err(EpochError::OtherConnection {
                stream: self.current.conn,
                input: key.conn,
            });
        }
        if key.epoch > self.current.epoch {
            return Err(EpochError::Unopened {
                current: self.current.epoch,
                input: key.epoch,
            });
        }
        if key.epoch == self.current.epoch {
            return Ok(Admit::Current);
        }
        let count = &mut self.stale[input.index()];
        *count = count.saturating_add(1);
        Ok(Admit::Stale)
    }

    /// Drops and counts an input of kind `input` that arrived after the current epoch ended
    /// but before the next opened: an event an order-entry codec pushes once its session
    /// stopped (decision 0053).
    pub(crate) fn drop_ended(&mut self, input: Input) {
        let count = &mut self.stale[input.index()];
        *count = count.saturating_add(1);
    }

    /// How many inputs of kind `input` were dropped as stale.
    pub fn stale(&self, input: Input) -> u64 {
        self.stale[input.index()]
    }

    /// How many inputs of every kind were dropped as stale.
    pub fn stale_total(&self) -> u64 {
        self.stale.iter().fold(0, |sum, n| sum.saturating_add(*n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(conn: u16, epoch: u32) -> ConnKey {
        ConnKey { conn, epoch }
    }

    #[test]
    fn a_close_or_reconnect_opens_the_next_epoch() {
        let mut epochs = Epochs::new(3);
        assert_eq!(epochs.current(), key(3, 0));
        assert_eq!(epochs.advance(), Ok(key(3, 1)));
        assert_eq!(epochs.advance(), Ok(key(3, 2)));
        assert_eq!(epochs.current(), key(3, 2));
    }

    #[test]
    fn each_input_of_an_older_epoch_is_dropped_and_counted_while_the_current_passes() {
        let mut epochs = Epochs::new(1);
        epochs.advance().unwrap();
        epochs.advance().unwrap();
        for input in Input::ALL {
            assert_eq!(
                epochs.admit(input, key(1, 2)),
                Ok(Admit::Current),
                "{input:?}"
            );
            assert_eq!(epochs.stale(input), 0, "{input:?}");
            assert_eq!(
                epochs.admit(input, key(1, 1)),
                Ok(Admit::Stale),
                "{input:?}"
            );
            assert_eq!(epochs.stale(input), 1, "{input:?}");
        }
        assert_eq!(epochs.stale_total(), 4);

        // Epoch 0 is two lives back: still stale, counted again under its own kind only.
        assert_eq!(epochs.admit(Input::Frame, key(1, 0)), Ok(Admit::Stale));
        assert_eq!(epochs.stale(Input::Frame), 2);
        assert_eq!(epochs.stale(Input::Timer), 1);
        assert_eq!(epochs.stale_total(), 5);
    }

    #[test]
    fn what_was_current_turns_stale_once_the_next_epoch_opens() {
        let mut epochs = Epochs::new(0);
        assert_eq!(epochs.admit(Input::Http, key(0, 0)), Ok(Admit::Current));
        epochs.advance().unwrap();
        assert_eq!(epochs.admit(Input::Http, key(0, 0)), Ok(Admit::Stale));
        assert_eq!(epochs.admit(Input::Http, key(0, 1)), Ok(Admit::Current));
        assert_eq!(epochs.stale(Input::Http), 1);
    }

    #[test]
    fn an_input_of_another_connection_is_a_routing_fault_and_is_not_counted() {
        let mut epochs = Epochs::new(4);
        let err = epochs.admit(Input::Frame, key(5, 0)).unwrap_err();
        assert_eq!(
            err,
            EpochError::OtherConnection {
                stream: 4,
                input: 5
            }
        );
        assert_eq!(
            err.to_string(),
            "input of connection 5 reached the epochs of connection 4"
        );
        assert_eq!(epochs.stale_total(), 0);
    }

    #[test]
    fn an_input_of_an_epoch_not_yet_opened_is_a_routing_fault_and_is_not_counted() {
        let mut epochs = Epochs::new(4);
        let err = epochs.admit(Input::Event, key(4, 1)).unwrap_err();
        assert_eq!(
            err,
            EpochError::Unopened {
                current: 0,
                input: 1
            }
        );
        assert_eq!(
            err.to_string(),
            "input of epoch 1 reached a connection still at epoch 0"
        );
        assert_eq!(epochs.stale_total(), 0);
    }

    #[test]
    fn the_last_epoch_refuses_to_advance_and_stays_current() {
        let mut epochs = Epochs::new(9);
        epochs.current.epoch = u32::MAX;
        let err = epochs.advance().unwrap_err();
        assert_eq!(err, EpochError::Exhausted { conn: 9 });
        assert_eq!(err.to_string(), "connection 9 has no epoch left to open");
        assert_eq!(epochs.current(), key(9, u32::MAX));
    }

    #[test]
    fn the_stale_count_saturates_instead_of_wrapping() {
        let mut epochs = Epochs::new(0);
        epochs.advance().unwrap();
        epochs.stale[Input::Timer.index()] = u64::MAX;
        assert_eq!(epochs.admit(Input::Timer, key(0, 0)), Ok(Admit::Stale));
        assert_eq!(epochs.stale(Input::Timer), u64::MAX);
        assert_eq!(epochs.admit(Input::Frame, key(0, 0)), Ok(Admit::Stale));
        assert_eq!(epochs.stale_total(), u64::MAX);
    }
}
