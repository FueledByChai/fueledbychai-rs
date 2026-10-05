//! Fault scripts as typed Rust values (decision 0025): a [`WsScript`] is one timeline of
//! [`Step`]s, each naming the connection it acts on by its accept order, so a script can drive
//! several connections and interleave them.

/// A WebSocket data frame, as the stub sends or received it. Pings, pongs and close frames are
/// protocol, not data, and are never recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

impl Frame {
    pub fn text(text: impl Into<String>) -> Frame {
        Frame::Text(text.into())
    }
}

/// One step of a script. A connection is named by its accept order: the first connection the
/// stub accepted is 0. A step naming a connection the script has not yet accepted fails it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Wait for the next connection and complete its WebSocket upgrade; it takes the next
    /// number.
    Accept,
    /// Wait for the next data frame from `conn` (every frame is recorded as it arrives, read or
    /// not). Reading a subscribe and then pushing a reply is how a script acknowledges it.
    Read { conn: usize },
    /// Send `frame` to `conn`.
    Push { conn: usize, frame: Frame },
    /// Send `conn` a close frame; the stub keeps recording until the client's close reply.
    Close { conn: usize },
    /// Stop reading from and writing to `conn`, holding it open until the stub is dropped.
    Silent { conn: usize },
}

/// A script the stub plays from its start, step by step.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WsScript {
    pub steps: Vec<Step>,
}

impl WsScript {
    pub fn new(steps: Vec<Step>) -> WsScript {
        WsScript { steps }
    }
}

/// The reconnect storm that desynced the Java stack's order map and stacked its duplicate
/// subscriptions: 340 reconnects in 8 minutes. The count matters, not the minutes.
pub const STORM_RECONNECTS: usize = 340;

/// A storm of `reconnects` forced reconnects: each of `reconnects + 1` connections is accepted,
/// `reads` frames are read from it (a client's open and subscribe frames), `push(n)` is sent to
/// connection `n`, and every connection but the last is then closed by the stub. The last stays
/// open, so the client should end the storm on one live connection.
pub fn reconnect_storm(
    reconnects: usize,
    reads: usize,
    mut push: impl FnMut(usize) -> Frame,
) -> WsScript {
    let mut steps = Vec::new();
    for conn in 0..=reconnects {
        steps.push(Step::Accept);
        steps.extend((0..reads).map(|_| Step::Read { conn }));
        steps.push(Step::Push {
            conn,
            frame: push(conn),
        });
        if conn < reconnects {
            steps.push(Step::Close { conn });
        }
    }
    WsScript::new(steps)
}

/// Duplicate subscription acknowledgements, behind the Java stack's duplicate subscriptions
/// after reconnects: one connection is accepted, `reads` frames are read from it (a client's
/// open and subscribe frames), every frame of `acks` is pushed twice, and each frame of `data`
/// once. The connection stays open.
pub fn duplicate_acks(reads: usize, acks: &[Frame], data: &[Frame]) -> WsScript {
    let push = |frame: &Frame| Step::Push {
        conn: 0,
        frame: frame.clone(),
    };
    let mut steps = vec![Step::Accept];
    steps.extend((0..reads).map(|_| Step::Read { conn: 0 }));
    steps.extend(acks.iter().flat_map(|ack| [push(ack), push(ack)]));
    steps.extend(data.iter().map(push));
    WsScript::new(steps)
}

/// Silence after subscription: connection 0 is accepted, `reads` frames are read from it, every
/// frame of `acks` is pushed once, and then nothing arrives on it, though it stays open. The
/// client should give up on it and reconnect: connection 1 is accepted, read and acknowledged
/// the same way, and stays open.
pub fn silence_after_ack(reads: usize, acks: &[Frame]) -> WsScript {
    let mut steps = Vec::new();
    for conn in 0..2 {
        steps.push(Step::Accept);
        steps.extend((0..reads).map(|_| Step::Read { conn }));
        steps.extend(acks.iter().map(|ack| Step::Push {
            conn,
            frame: ack.clone(),
        }));
        if conn == 0 {
            steps.push(Step::Silent { conn });
        }
    }
    WsScript::new(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_storm_closes_every_connection_but_the_last() {
        let script = reconnect_storm(1, 2, |n| Frame::text(format!("n={n}")));
        let read = |conn| Step::Read { conn };
        let push = |conn| Step::Push {
            conn,
            frame: Frame::text(format!("n={conn}")),
        };
        assert_eq!(
            script.steps,
            [
                Step::Accept,
                read(0),
                read(0),
                push(0),
                Step::Close { conn: 0 },
                Step::Accept,
                read(1),
                read(1),
                push(1),
            ]
        );
        let full = reconnect_storm(STORM_RECONNECTS, 0, |_| Frame::Binary(Vec::new()));
        let accepts = full.steps.iter().filter(|s| **s == Step::Accept).count();
        assert_eq!(accepts, STORM_RECONNECTS + 1);
        assert_eq!(WsScript::default(), WsScript::new(Vec::new()));
    }

    #[test]
    fn duplicate_acks_pushes_each_ack_twice_and_each_data_frame_once() {
        let (a, b, d) = (Frame::text("a"), Frame::text("b"), Frame::text("d"));
        let push = |frame: &Frame| Step::Push {
            conn: 0,
            frame: frame.clone(),
        };
        let script = duplicate_acks(1, &[a.clone(), b.clone()], std::slice::from_ref(&d));
        assert_eq!(
            script.steps,
            [
                Step::Accept,
                Step::Read { conn: 0 },
                push(&a),
                push(&a),
                push(&b),
                push(&b),
                push(&d),
            ]
        );
    }

    #[test]
    fn silence_after_ack_acknowledges_then_goes_silent_and_takes_the_reconnect() {
        let ack = Frame::text("ack");
        let push = |conn| Step::Push {
            conn,
            frame: ack.clone(),
        };
        let script = silence_after_ack(2, std::slice::from_ref(&ack));
        let read = |conn| Step::Read { conn };
        assert_eq!(
            script.steps,
            [
                Step::Accept,
                read(0),
                read(0),
                push(0),
                Step::Silent { conn: 0 },
                Step::Accept,
                read(1),
                read(1),
                push(1),
            ]
        );
    }
}
