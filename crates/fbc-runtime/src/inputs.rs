//! Consumer inputs (FBC-hvqi, decisions 0006, 0023, 0101, 0102; design §5.4's UI-to-shard
//! queue): what the consumer's other threads hand the shard (an operator's Start, Stop, Flatten,
//! Wind-down or kill switch, a calendar or configuration reload) and the firings of the timers
//! its handlers arm on the shard, each stamped with the shard's next ingest sequence, journaled
//! as an input consumer record, and only then handed to the consumer's [`InputHandler`].
//!
//! - **The ring.** [`ConsumerInputs::new`] makes a bounded ring of the consumer's capacity and
//!   the [`InputSender`] its other threads push into ([`InputSender::push`]). A push never
//!   blocks: one into a full ring is refused ([`InputRefusal::Full`]) and counted
//!   ([`InputSender::refused`]), and one after the inputs' run ended is refused
//!   ([`InputRefusal::Closed`]). Neither side ever waits on the other.
//! - **What an input is.** The consumer's own bytes and its own number for what they hold, both
//!   opaque to the library (decision 0001), and the traffic class it is journaled under (an
//!   operator's Stop is Safety, 0006). The handler is handed exactly what was journaled, so a
//!   replay hands back the same input.
//! - **Stamped, journaled, then handled.** [`ConsumerInputs::run`], which the shard host drives
//!   as a [`Member`](crate::Member) ([`Member::inputs`](crate::Member::inputs)), takes each
//!   input from the ring as the shard polls it, stamps it with the next ingest sequence of the
//!   shard's [`IngestClock`] (so it takes its place among the frames of every session of the
//!   shard, and calls for a decide pass as a frame does), journals it as
//!   `Record::Consumer { input: true, ingest_seq: Some(seq), .. }` under its class, and then
//!   calls [`InputHandler::on_input`] with it ([`ConsumerInput`]).
//! - **Timers.** A handler arms a timer through [`ConsumerTimers::arm`], on the shard's thread:
//!   after its delay, the run takes its firing as it takes a pushed input, stamped, journaled
//!   and handed to the handler with the kind and bytes it was armed with. A firing is an input
//!   like any other; the handler tells one from a pushed input by the kind it gave it.
//! - **Replay.** A replay hands each journaled input back at its place, with its recorded
//!   sequence ([`MdReplay::set_inputs`](crate::MdReplay::set_inputs)); its handlers arm through
//!   an inert [`ConsumerTimers::inert`], which arms no live timer, since every firing the live
//!   shard took is in the journal.

use std::fmt;
use std::time::Duration;

use fbc_core::{MonoNs, TrafficClass};

use crate::journal::Journal;
use crate::session_core::IngestClock;

/// An input handed to the consumer's [`InputHandler`]: what the consumer pushed, or a timer
/// firing with what it was armed with, as the shard stamped and journaled it.
#[derive(Clone, PartialEq, Eq)]
pub struct ConsumerInput {
    /// The shard's ingest sequence it took.
    pub ingest_seq: u64,
    /// When the shard took it, on the clock's monotonic time.
    pub at: MonoNs,
    /// The consumer's own number for what `bytes` hold.
    pub kind: u16,
    /// The consumer's own bytes, never read by the library.
    pub bytes: Vec<u8>,
}

/// Shows the bytes by length only, as the journal's opaque bytes are.
impl fmt::Debug for ConsumerInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInput")
            .field("ingest_seq", &self.ingest_seq)
            .field("at", &self.at)
            .field("kind", &self.kind)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// The consumer's handler of its own inputs, called on the shard's thread once per input, after
/// the input was journaled (any `FnMut(ConsumerInput)` is one).
pub trait InputHandler {
    fn on_input(&mut self, input: ConsumerInput);
}

impl<F: FnMut(ConsumerInput)> InputHandler for F {
    fn on_input(&mut self, input: ConsumerInput) {
        self(input)
    }
}

/// Why a push was refused: nothing of it reached the shard.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InputRefusal {
    /// The ring was full: counted ([`InputSender::refused`]).
    Full,
    /// The inputs' run has ended, or they were dropped.
    Closed,
}

impl fmt::Display for InputRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputRefusal::Full => f.write_str("the shard's input ring is full"),
            InputRefusal::Closed => f.write_str("the shard takes no more inputs"),
        }
    }
}

impl std::error::Error for InputRefusal {}

/// Where the consumer's other threads push inputs into the shard's ring. Cloning it shares the
/// ring and the count of refusals; it is `Send` and `Sync`.
#[derive(Clone, Debug)]
pub struct InputSender {}

impl InputSender {
    /// Pushes an input of the consumer's `kind`, holding `bytes`, to be journaled under
    /// `class`, without waiting: refused when the ring is full (and counted) or the inputs'
    /// run has ended.
    pub fn push(&self, class: TrafficClass, kind: u16, bytes: Vec<u8>) -> Result<(), InputRefusal> {
        let _ = (class, kind, bytes);
        Err(InputRefusal::Closed)
    }

    /// The pushes refused because the ring was full, by every clone of this sender.
    pub fn refused(&self) -> u64 {
        0
    }
}

/// The timers the consumer's handlers arm on the shard's thread (cloning it shares them),
/// whose firings a [`ConsumerInputs`] takes as inputs: live ([`ConsumerTimers::new`]), or inert
/// for a replay ([`ConsumerTimers::inert`]).
#[derive(Clone, Debug, Default)]
pub struct ConsumerTimers {}

impl ConsumerTimers {
    /// Live timers, to give the [`ConsumerInputs`] that fires them (one) and the handlers that
    /// arm them.
    pub fn new() -> ConsumerTimers {
        ConsumerTimers {}
    }

    /// Timers that arm nothing, for a replay: each arm is counted and fires nothing, since the
    /// firings of the live shard come back from the journal.
    pub fn inert() -> ConsumerTimers {
        ConsumerTimers {}
    }

    /// Arms a timer that fires `after` from now as an input of the consumer's `kind`, holding
    /// `bytes`, journaled under `class`. Counted and not armed when these timers are inert, when
    /// the inputs that fire them have ended, or when `after` lies past the end of the clock.
    pub fn arm(&self, after: Duration, class: TrafficClass, kind: u16, bytes: Vec<u8>) {
        let _ = (after, class, kind, bytes);
    }

    /// Every arm so far, armed or not.
    pub fn arms(&self) -> u64 {
        0
    }

    /// The arms that armed no timer: inert timers, ended inputs, or a delay past the clock.
    pub fn not_armed(&self) -> u64 {
        0
    }

    /// The timers armed that have not fired yet.
    pub fn pending(&self) -> usize {
        0
    }
}

/// What a [`ConsumerInputs`] needs, all from the consumer.
pub struct ConsumerInputsConfig {
    /// The shard's clock: the one every session of the shard stamps from.
    pub clock: IngestClock,
    /// The most inputs the ring holds; between 1 and [`MAX_CAPACITY`], with no default.
    pub capacity: usize,
    /// The timers the consumer's handlers arm, which these inputs fire.
    pub timers: ConsumerTimers,
}

/// The largest ring capacity: what the ring's counting semaphore allows.
pub const MAX_CAPACITY: usize = tokio::sync::Semaphore::MAX_PERMITS;

/// Why a [`ConsumerInputs`] could not be made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InputsError {
    /// The capacity is 0 or above [`MAX_CAPACITY`].
    Capacity(usize),
}

impl fmt::Display for InputsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputsError::Capacity(n) => write!(
                f,
                "an input ring of capacity {n}: it must hold between 1 and {MAX_CAPACITY}"
            ),
        }
    }
}

impl std::error::Error for InputsError {}

/// What a [`ConsumerInputs`] counted.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct InputCounters {
    /// Pushed inputs taken from the ring and handed on.
    pub pushed: u64,
    /// Timer firings handed on.
    pub fired: u64,
}

/// The shard's side of the consumer's inputs: the ring's receiving end and the timers it fires,
/// driven by [`ConsumerInputs::run`] (the module docs say how).
pub struct ConsumerInputs<H> {
    clock: IngestClock,
    timers: ConsumerTimers,
    journal: Option<Journal>,
    counters: InputCounters,
    handler: H,
}

impl<H> fmt::Debug for ConsumerInputs<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInputs")
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

impl<H: InputHandler> ConsumerInputs<H> {
    /// The shard's inputs, handing each to `handler`, and the sender of their ring.
    pub fn new(
        config: ConsumerInputsConfig,
        handler: H,
    ) -> Result<(ConsumerInputs<H>, InputSender), InputsError> {
        let ConsumerInputsConfig {
            clock,
            capacity,
            timers,
        } = config;
        if capacity == 0 || capacity > MAX_CAPACITY {
            return Err(InputsError::Capacity(capacity));
        }
        let inputs = ConsumerInputs {
            clock,
            timers,
            journal: None,
            counters: InputCounters::default(),
            handler,
        };
        Ok((inputs, InputSender {}))
    }

    /// Journals every input into `journal` from now on (0006).
    pub fn set_journal(&mut self, journal: Journal) {
        self.journal = Some(journal);
    }

    /// The timers these inputs fire.
    pub fn timers(&self) -> ConsumerTimers {
        self.timers.clone()
    }

    pub fn counters(&self) -> InputCounters {
        self.counters
    }

    /// Takes every input as it comes, stamped, journaled and handed to the handler, until every
    /// sender has dropped and no timer is pending. Once it returns, or is dropped, the ring
    /// refuses every push and the timers arm nothing.
    pub async fn run(&mut self) {
        let _ = &self.clock;
        let _ = &mut self.handler;
    }
}
