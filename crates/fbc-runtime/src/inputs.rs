//! Consumer inputs (FBC-hvqi, decisions 0006, 0023, 0101, 0103; design §5.4's UI-to-shard
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
//!   like any other; the handler tells one from a pushed input by the kind it gave it. An arm
//!   that arms nothing says why ([`ArmRefusal`]).
//! - **The end.** The run ends once every sender has dropped and no timer is pending; once it
//!   returns, or is dropped, the inputs have ended for good, and a restarted shard makes new
//!   ones, which may take the same timers.
//! - **Replay.** A replay hands each journaled input back at its place, with its recorded
//!   sequence ([`MdReplay::set_inputs`](crate::MdReplay::set_inputs)); its handlers arm through
//!   an inert [`ConsumerTimers::inert`], which arms no live timer, since every firing the live
//!   shard took is in the journal.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use fbc_core::{MonoNs, TrafficClass};
use fbc_journal::{Opaque, Record};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, Sleep};

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

/// Why an arm armed no timer: nothing of it will fire (Reviewer B's B1 on PR #172).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ArmRefusal {
    /// The inputs that fire these timers have ended (their run returned or was dropped), and no
    /// new [`ConsumerInputs`] has taken the timers since.
    Ended,
    /// The delay lies past the end of the clock.
    PastClock,
}

impl fmt::Display for ArmRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArmRefusal::Ended => f.write_str("the inputs that fire these timers have ended"),
            ArmRefusal::PastClock => {
                f.write_str("the timer's delay lies past the end of the clock")
            }
        }
    }
}

impl std::error::Error for ArmRefusal {}

/// An input as pushed, on its way through the ring.
struct Pushed {
    class: TrafficClass,
    kind: u16,
    bytes: Vec<u8>,
}

/// Where the consumer's other threads push inputs into the shard's ring. Cloning it shares the
/// ring and the count of refusals; it is `Send` and `Sync`.
#[derive(Clone)]
pub struct InputSender {
    tx: mpsc::Sender<Pushed>,
    refused: Arc<AtomicU64>,
}

impl fmt::Debug for InputSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputSender")
            .field("refused", &self.refused())
            .finish_non_exhaustive()
    }
}

impl InputSender {
    /// Pushes an input of the consumer's `kind`, holding `bytes`, to be journaled under
    /// `class`, without waiting: refused when the ring is full (and counted) or the inputs'
    /// run has ended.
    pub fn push(&self, class: TrafficClass, kind: u16, bytes: Vec<u8>) -> Result<(), InputRefusal> {
        let pushed = Pushed { class, kind, bytes };
        match self.tx.try_send(pushed) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                Err(InputRefusal::Full)
            }
            Err(TrySendError::Closed(_)) => Err(InputRefusal::Closed),
        }
    }

    /// The pushes refused because the ring was full, by every clone of this sender.
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }
}

/// The timers the consumer's handlers arm on the shard's thread (cloning it shares them),
/// whose firings a [`ConsumerInputs`] takes as inputs: live ([`ConsumerTimers::new`]), or inert
/// for a replay ([`ConsumerTimers::inert`]).
#[derive(Clone)]
pub struct ConsumerTimers {
    state: Rc<RefCell<Timers>>,
}

impl fmt::Debug for ConsumerTimers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerTimers")
            .field("arms", &self.arms())
            .field("not_armed", &self.not_armed())
            .field("pending", &self.pending())
            .finish()
    }
}

impl Default for ConsumerTimers {
    fn default() -> Self {
        ConsumerTimers::new()
    }
}

/// The timers' shared state.
struct Timers {
    /// Whether an arm arms a timer (false for a replay's).
    live: bool,
    /// Whether the inputs that fire them have ended, and no new ones have taken them since.
    ended: bool,
    /// Whether inputs that have not ended fire them (one at a time).
    bound: bool,
    /// Counts the inputs that have taken them, so inputs that ended end nothing of later ones.
    generation: u64,
    /// The order the next timer is armed in, which breaks a tie of deadlines.
    next: u64,
    /// The timers armed and not yet fired, by deadline then order armed.
    due: BTreeMap<(Instant, u64), Pushed>,
    /// The task of the inputs' run, woken when a timer is armed.
    waker: Option<Waker>,
    arms: u64,
    not_armed: u64,
}

impl ConsumerTimers {
    /// Live timers, to give the [`ConsumerInputs`] that fires them (one at a time: new inputs may
    /// take them once those have ended, as a restarted shard's do) and the handlers that arm them.
    pub fn new() -> ConsumerTimers {
        ConsumerTimers::with(true)
    }

    /// Timers that arm nothing, for a replay: each arm is counted and fires nothing, since the
    /// firings of the live shard come back from the journal.
    pub fn inert() -> ConsumerTimers {
        ConsumerTimers::with(false)
    }

    fn with(live: bool) -> ConsumerTimers {
        let state = Timers {
            live,
            ended: false,
            bound: false,
            generation: 0,
            next: 0,
            due: BTreeMap::new(),
            waker: None,
            arms: 0,
            not_armed: 0,
        };
        ConsumerTimers {
            state: Rc::new(RefCell::new(state)),
        }
    }

    /// Arms a timer that fires `after` from now as an input of the consumer's `kind`, holding
    /// `bytes`, journaled under `class`. Refused, counted and not armed when the inputs that
    /// fire these timers have ended ([`ArmRefusal::Ended`]) or `after` lies past the end of the
    /// clock ([`ArmRefusal::PastClock`]), so a lost timer is never silent (Reviewer B's B1 on PR
    /// #172). Inert timers take every arm and arm nothing (counted), as the live shard's firing
    /// comes back from the journal.
    #[must_use = "a refused arm fires nothing"]
    pub fn arm(
        &self,
        after: Duration,
        class: TrafficClass,
        kind: u16,
        bytes: Vec<u8>,
    ) -> Result<(), ArmRefusal> {
        let deadline = Instant::now().checked_add(after);
        let waker = {
            let mut st = self.state.borrow_mut();
            st.arms += 1;
            let refused = match deadline {
                _ if st.ended => Err(ArmRefusal::Ended),
                None => Err(ArmRefusal::PastClock),
                Some(deadline) => Ok(deadline),
            };
            let deadline = match refused {
                Ok(deadline) if st.live => deadline,
                refused => {
                    st.not_armed += 1;
                    // Inert timers take every arm: the live firing comes back from the journal.
                    return if st.live { refused.map(drop) } else { Ok(()) };
                }
            };
            let order = st.next;
            st.next += 1;
            let fired = Pushed { class, kind, bytes };
            st.due.insert((deadline, order), fired);
            st.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Every arm so far, armed or not.
    pub fn arms(&self) -> u64 {
        self.state.borrow().arms
    }

    /// The arms that armed no timer: inert timers, ended inputs, or a delay past the clock.
    pub fn not_armed(&self) -> u64 {
        self.state.borrow().not_armed
    }

    /// The timers armed that have not fired yet.
    pub fn pending(&self) -> usize {
        self.state.borrow().due.len()
    }

    /// The order the next timer will be armed in.
    fn horizon(&self) -> u64 {
        self.state.borrow().next
    }

    /// The first timer due at `now` that was armed before `horizon`.
    fn first_due(&self, now: Instant, horizon: u64) -> Option<(Instant, u64)> {
        let st = self.state.borrow();
        st.due
            .keys()
            .take_while(|(deadline, _)| *deadline <= now)
            .find(|(_, order)| *order < horizon)
            .copied()
    }

    /// Takes the timer armed at `key`.
    fn remove(&self, key: (Instant, u64)) -> Option<Pushed> {
        self.state.borrow_mut().due.remove(&key)
    }

    /// The earliest timer's deadline, and whether it was armed before `horizon`, once the run
    /// that fires them is woken on an arm.
    fn earliest(&self, waker: &Waker, horizon: u64) -> Option<(Instant, bool)> {
        let mut st = self.state.borrow_mut();
        st.waker = Some(waker.clone());
        let &(deadline, order) = st.due.keys().next()?;
        Some((deadline, order < horizon))
    }

    /// New inputs take these timers, and their generation: refused while other inputs that have
    /// not ended fire them.
    fn bind(&self) -> Option<u64> {
        let mut st = self.state.borrow_mut();
        if st.bound {
            return None;
        }
        st.bound = true;
        st.ended = false;
        st.generation += 1;
        Some(st.generation)
    }

    /// The inputs of `generation` that fire these timers ended: what is armed never fires and
    /// nothing more is, until new inputs take them. Inputs of an earlier generation end nothing.
    fn end(&self, generation: u64) {
        if let Ok(mut st) = self.state.try_borrow_mut()
            && st.generation == generation
        {
            st.bound = false;
            st.ended = true;
            st.due.clear();
            st.waker = None;
        }
    }
}

/// What a [`ConsumerInputs`] needs, all from the consumer.
pub struct ConsumerInputsConfig {
    /// The shard's clock: the one every session of the shard stamps from.
    pub clock: IngestClock,
    /// The most inputs the ring holds; between 1 and [`MAX_INPUT_CAPACITY`], with no default.
    pub capacity: usize,
    /// The timers the consumer's handlers arm, which these inputs fire.
    pub timers: ConsumerTimers,
}

/// The largest ring capacity: what the ring's counting semaphore allows.
pub const MAX_INPUT_CAPACITY: usize = tokio::sync::Semaphore::MAX_PERMITS;

/// Why a [`ConsumerInputs`] could not be made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InputsError {
    /// The capacity is 0 or above [`MAX_INPUT_CAPACITY`].
    Capacity(usize),
    /// The timers are fired by other inputs that have not ended.
    TimersInUse,
}

impl fmt::Display for InputsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputsError::Capacity(n) => write!(
                f,
                "an input ring of capacity {n}: it must hold between 1 and {MAX_INPUT_CAPACITY}"
            ),
            InputsError::TimersInUse => {
                f.write_str("the consumer's timers are fired by other inputs that have not ended")
            }
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
    /// Pushed inputs the ring still held when the inputs ended, never handed on.
    pub unhandled: u64,
}

/// The shard's side of the consumer's inputs: the ring's receiving end and the timers it fires,
/// driven by [`ConsumerInputs::run`] (the module docs say how).
pub struct ConsumerInputs<H> {
    ring: Ring,
    handler: H,
}

/// What a [`ConsumerInputs`] holds besides its handler, run without knowing its type.
struct Ring {
    clock: IngestClock,
    rx: mpsc::Receiver<Pushed>,
    /// Whether every sender has dropped, the ring emptied.
    closed: bool,
    timers: ConsumerTimers,
    /// The timers' generation these inputs took.
    generation: u64,
    /// Whether these inputs have ended, for good.
    ended: bool,
    journal: Option<Journal>,
    counters: InputCounters,
}

/// Inputs dropped unrun end too, so new ones may take their timers.
impl Drop for Ring {
    fn drop(&mut self) {
        self.end();
    }
}

impl<H> fmt::Debug for ConsumerInputs<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInputs")
            .field("counters", &self.ring.counters)
            .finish_non_exhaustive()
    }
}

impl<H: InputHandler> ConsumerInputs<H> {
    /// The shard's inputs, handing each to `handler`, and the sender of their ring.
    pub fn new(
        config: ConsumerInputsConfig,
        handler: H,
    ) -> Result<(ConsumerInputs<H>, InputSender), InputsError> {
        let (ring, sender) = Ring::new(config)?;
        Ok((ConsumerInputs { ring, handler }, sender))
    }

    /// Journals every input into `journal` from now on (0006).
    pub fn set_journal(&mut self, journal: Journal) {
        self.ring.journal = Some(journal);
    }

    pub fn counters(&self) -> InputCounters {
        self.ring.counters
    }

    /// Takes every input as it comes, stamped, journaled and handed to the handler, until every
    /// sender has dropped and no timer is pending. Once it returns, or is dropped, the inputs
    /// have ended for good, a run dropped before its first poll included (Codex P1 r4311000152
    /// on PR #172): the ring refuses every push, what it still held is counted
    /// ([`InputCounters::unhandled`]) and never handed on, the timers refuse every arm
    /// ([`ArmRefusal::Ended`]) until new inputs take them, and a second run returns at once
    /// handing nothing (Reviewer B's B2 on PR #172). A consumer whose handlers arm timers for
    /// the shard's whole life keeps a sender for it; a restarted shard makes new inputs, which
    /// may take the same timers.
    pub fn run(&mut self) -> impl Future<Output = ()> + '_ {
        let ConsumerInputs { ring, handler } = self;
        // Made before the future is, so dropping it unpolled ends the inputs too.
        let ending = Ending(ring);
        ending.run(handler)
    }
}

impl Ring {
    fn new(config: ConsumerInputsConfig) -> Result<(Ring, InputSender), InputsError> {
        let ConsumerInputsConfig {
            clock,
            capacity,
            timers,
        } = config;
        if capacity == 0 || capacity > MAX_INPUT_CAPACITY {
            return Err(InputsError::Capacity(capacity));
        }
        let generation = timers.bind().ok_or(InputsError::TimersInUse)?;
        let (tx, rx) = mpsc::channel(capacity);
        let ring = Ring {
            clock,
            rx,
            closed: false,
            timers,
            generation,
            ended: false,
            journal: None,
            counters: InputCounters::default(),
        };
        let sender = InputSender {
            tx,
            refused: Arc::new(AtomicU64::new(0)),
        };
        Ok((ring, sender))
    }

    /// Ends the inputs for good: the ring refuses every push from now on and what it still held
    /// is counted and dropped, never handed on; the timers arm nothing until new inputs take
    /// them (Reviewer B's B2 on PR #172).
    fn end(&mut self) {
        if self.ended {
            return;
        }
        self.ended = true;
        self.rx.close();
        while self.rx.try_recv().is_ok() {
            self.counters.unhandled += 1;
        }
        self.timers.end(self.generation);
    }

    /// Takes what is ready: a timer due that was armed before this poll began (so a handler
    /// that re-arms at once yields between firings), else what waits in the ring, until neither
    /// is; ready once the ring has closed and no timer is pending.
    fn poll_run(
        &mut self,
        handler: &mut dyn InputHandler,
        cx: &mut Context<'_>,
        sleep: &mut Option<Pin<Box<Sleep>>>,
    ) -> Poll<()> {
        if self.ended {
            // A second run of inputs that ended hands on nothing (Reviewer B's B2 on PR #172).
            return Poll::Ready(());
        }
        let horizon = self.timers.horizon();
        loop {
            if let Some(key) = self.timers.first_due(Instant::now(), horizon) {
                // Charged to the task's coop budget as the ring's inputs are, so a backlog of
                // due timers yields to the shard's other members (Codex P1 r4311000162 on PR
                // #172).
                std::task::ready!(tokio::task::coop::poll_proceed(cx)).made_progress();
                if let Some(fired) = self.timers.remove(key) {
                    self.counters.fired += 1;
                    self.take(handler, fired);
                }
                continue;
            }
            if !self.closed {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(pushed)) => {
                        self.counters.pushed += 1;
                        self.take(handler, pushed);
                        continue;
                    }
                    Poll::Ready(None) => self.closed = true,
                    Poll::Pending => {}
                }
            }
            match self.timers.earliest(cx.waker(), horizon) {
                None if self.closed => return Poll::Ready(()),
                None => return Poll::Pending,
                Some((deadline, true)) => std::task::ready!(wait_until(sleep, deadline, cx)),
                // The earliest was armed during this poll: the next poll takes it.
                Some((_, false)) => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }
    }

    /// Stamps `input` with the next ingest sequence, journals it as an input consumer record
    /// under its class, then hands it to `handler`.
    fn take(&mut self, handler: &mut dyn InputHandler, input: Pushed) {
        let Pushed { class, kind, bytes } = input;
        let (ingest_seq, at, wall) = self.clock.tick();
        if let Some(journal) = &self.journal {
            // An operator's command or a reload: rare and short, so copied.
            let record = Record::Consumer {
                at,
                kind,
                input: true,
                ingest_seq: Some(ingest_seq),
                bytes: Opaque(bytes.clone()),
            };
            journal.record(class, wall, &record);
        }
        handler.on_input(ConsumerInput {
            ingest_seq,
            at,
            kind,
            bytes,
        });
    }
}

/// Waits, in `sleep`, until `deadline`: ready once it has passed.
fn wait_until(
    sleep: &mut Option<Pin<Box<Sleep>>>,
    deadline: Instant,
    cx: &mut Context<'_>,
) -> Poll<()> {
    let wait = sleep.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
    if wait.deadline() != deadline {
        wait.as_mut().reset(deadline);
    }
    wait.as_mut().poll(cx)
}

/// A run that returns, or is dropped, ends the inputs ([`Ring::end`]).
struct Ending<'a>(&'a mut Ring);

impl Drop for Ending<'_> {
    fn drop(&mut self) {
        self.0.end();
    }
}

impl Ending<'_> {
    /// [`ConsumerInputs::run`], handing each input to `handler`.
    async fn run(self, handler: &mut dyn InputHandler) {
        let mut sleep = None;
        poll_fn(|cx| self.0.poll_run(handler, cx, &mut sleep)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::cell::Cell;

    type Seen = Rc<RefCell<Vec<ConsumerInput>>>;

    fn keep(seen: &Seen) -> impl FnMut(ConsumerInput) + use<> {
        let seen = seen.clone();
        move |input| seen.borrow_mut().push(input)
    }

    fn inputs(
        capacity: usize,
        timers: &ConsumerTimers,
        seen: &Seen,
    ) -> (
        ConsumerInputs<impl FnMut(ConsumerInput) + use<>>,
        InputSender,
    ) {
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity,
            timers: timers.clone(),
        };
        ConsumerInputs::new(config, keep(seen)).unwrap()
    }

    fn kinds(seen: &Seen) -> Vec<u16> {
        seen.borrow().iter().map(|i| i.kind).collect()
    }

    #[test]
    fn a_ring_of_no_capacity_or_past_the_semaphores_is_refused_by_name() {
        for capacity in [0, MAX_INPUT_CAPACITY + 1] {
            let config = ConsumerInputsConfig {
                clock: IngestClock::new(),
                capacity,
                timers: ConsumerTimers::new(),
            };
            let e = ConsumerInputs::new(config, |_: ConsumerInput| {}).unwrap_err();
            assert_eq!(e, InputsError::Capacity(capacity));
            let said = e.to_string();
            assert!(said.contains(&format!("capacity {capacity}:")), "{said}");
        }
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity: 1,
            timers: ConsumerTimers::new(),
        };
        assert!(ConsumerInputs::new(config, |_: ConsumerInput| {}).is_ok());
    }

    #[test]
    fn the_sender_is_send_and_sync_and_nothing_shows_an_inputs_bytes() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<InputSender>();
        let input = ConsumerInput {
            ingest_seq: 3,
            at: MonoNs(4),
            kind: 5,
            bytes: b"secret-ish".to_vec(),
        };
        let shown = format!("{input:?}");
        assert!(
            !shown.contains("secret") && shown.contains("len: 10"),
            "{shown}"
        );
        let (timers, seen) = (ConsumerTimers::default(), Seen::default());
        let (inputs, sender) = inputs(1, &timers, &seen);
        assert!(format!("{inputs:?}").starts_with("ConsumerInputs"));
        assert_eq!(format!("{sender:?}"), "InputSender { refused: 0, .. }");
        assert_eq!(
            format!("{timers:?}"),
            "ConsumerTimers { arms: 0, not_armed: 0, pending: 0 }"
        );
        assert_eq!(
            InputRefusal::Full.to_string(),
            "the shard's input ring is full"
        );
        assert_eq!(
            InputRefusal::Closed.to_string(),
            "the shard takes no more inputs"
        );
    }

    /// Without a journal the inputs still go to the handler, each stamped with the next ingest
    /// sequence of the clock, in the order pushed; the run ends once every sender has dropped
    /// and refuses every push after.
    #[tokio::test]
    async fn the_run_hands_on_what_was_pushed_in_order_and_ends_once_every_sender_dropped() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(4, &timers, &seen);
        let clock = inputs.ring.clock.clone();
        clock.tick();
        let other = sender.clone();
        sender
            .push(TrafficClass::Safety, 1, b"Start".to_vec())
            .unwrap();
        other.push(TrafficClass::Normal, 2, Vec::new()).unwrap();
        drop((sender, other));
        inputs.run().await;
        let seen = seen.take();
        let got: Vec<_> = seen.iter().map(|i| (i.ingest_seq, i.kind)).collect();
        assert_eq!(got, [(1, 1), (2, 2)]);
        assert_eq!(seen[0].bytes, b"Start");
        assert!(seen[0].at <= seen[1].at);
        assert_eq!(clock.next_seq(), 3);
        assert_eq!(
            inputs.counters(),
            InputCounters {
                pushed: 2,
                fired: 0,
                unhandled: 0
            }
        );
        // Ended: the timers arm nothing, and say so.
        let armed = timers.arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new());
        assert_eq!(armed, Err(ArmRefusal::Ended));
        assert_eq!(
            (timers.arms(), timers.not_armed(), timers.pending()),
            (1, 1, 0)
        );
    }

    /// A run dropped part way, as the shard host drops it when another member fails, ends the
    /// inputs: the ring refuses every push after (not counted as full), and what was armed never
    /// fires nor is anything armed again.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_run_ends_the_inputs() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        timers
            .arm(Duration::from_secs(5), TrafficClass::Normal, 7, Vec::new())
            .unwrap();
        assert_eq!(timers.pending(), 1);
        assert!(inputs.run().now_or_never().is_none());
        assert_eq!(timers.pending(), 0);
        assert_eq!(
            sender.push(TrafficClass::Safety, 1, Vec::new()),
            Err(InputRefusal::Closed)
        );
        assert_eq!(sender.refused(), 0);
        let armed = timers.arm(Duration::ZERO, TrafficClass::Normal, 7, Vec::new());
        assert_eq!(armed, Err(ArmRefusal::Ended));
        assert_eq!(
            (timers.arms(), timers.not_armed(), timers.pending()),
            (2, 1, 0)
        );
        assert!(seen.borrow().is_empty());
    }

    /// A full ring refuses the push, counted on every clone, and keeps what it holds.
    #[tokio::test]
    async fn a_full_ring_refuses_and_counts_and_keeps_what_it_holds() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        let other = sender.clone();
        sender.push(TrafficClass::Normal, 1, Vec::new()).unwrap();
        for s in [&sender, &other] {
            assert_eq!(
                s.push(TrafficClass::Safety, 2, Vec::new()),
                Err(InputRefusal::Full)
            );
        }
        assert_eq!((sender.refused(), other.refused()), (2, 2));
        drop((sender, other));
        inputs.run().await;
        assert_eq!(kinds(&seen), [1]);
    }

    /// Timers fire by deadline, a tie in the order armed; a delay past the end of the clock and
    /// an inert timer arm nothing; the run waits for a pending timer once every sender dropped.
    #[tokio::test(start_paused = true)]
    async fn timers_fire_by_deadline_then_order_armed_and_the_run_waits_for_them() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        drop(sender);
        let secs = Duration::from_secs;
        timers
            .arm(secs(2), TrafficClass::Normal, 20, Vec::new())
            .unwrap();
        timers
            .arm(secs(1), TrafficClass::Normal, 10, Vec::new())
            .unwrap();
        timers
            .arm(secs(2), TrafficClass::Safety, 21, b"x".to_vec())
            .unwrap();
        let past = timers.arm(Duration::MAX, TrafficClass::Normal, 99, Vec::new());
        assert_eq!(past, Err(ArmRefusal::PastClock));
        assert_eq!(
            (timers.arms(), timers.not_armed(), timers.pending()),
            (4, 1, 3)
        );
        let began = Instant::now();
        inputs.run().await;
        assert_eq!(kinds(&seen), [10, 20, 21]);
        assert_eq!(Instant::now() - began, secs(2));
        assert_eq!(inputs.counters().fired, 3);
        let seqs: Vec<u64> = seen.borrow().iter().map(|i| i.ingest_seq).collect();
        assert_eq!(seqs, [0, 1, 2]);

        // An inert arm is taken (the live firing comes back from the journal) and arms nothing.
        let inert = ConsumerTimers::inert();
        let armed = inert.arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new());
        assert_eq!(armed, Ok(()));
        assert_eq!(
            (inert.arms(), inert.not_armed(), inert.pending()),
            (1, 1, 0)
        );
    }

    /// A handler that re-arms a timer of no delay on every firing does not hold the run in one
    /// poll: a firing armed during a poll is taken by the next, so the other tasks of the
    /// thread run between two.
    #[tokio::test(start_paused = true)]
    async fn a_timer_armed_while_the_run_is_polled_fires_on_its_next_poll() {
        let timers = ConsumerTimers::new();
        let stop = Rc::new(Cell::new(false));
        let fired = Rc::new(Cell::new(0u32));
        let (re_arm, halt, count) = (timers.clone(), stop.clone(), fired.clone());
        let handler = move |input: ConsumerInput| {
            count.set(count.get() + 1);
            if !halt.get() {
                let kind = input.kind;
                re_arm
                    .arm(Duration::ZERO, TrafficClass::Normal, kind, Vec::new())
                    .unwrap();
            }
        };
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity: 1,
            timers: timers.clone(),
        };
        let (mut inputs, sender) = ConsumerInputs::new(config, handler).unwrap();
        timers
            .arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new())
            .unwrap();
        let other = async {
            tokio::task::yield_now().await;
            stop.set(true);
            drop(sender);
        };
        tokio::join!(inputs.run(), other);
        assert!(fired.get() >= 2 && fired.get() < 10, "{}", fired.get());
        assert_eq!(timers.pending(), 0);
    }

    /// Codex P1 r4311000162 on PR #172: a backlog of timers due at once is charged to the task's
    /// coop budget like the ring's inputs, so one poll of the run takes only part of it and the
    /// shard host's other members are polled before the rest.
    #[tokio::test(start_paused = true)]
    async fn a_backlog_of_due_timers_is_taken_within_the_tasks_coop_budget() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        drop(sender);
        for kind in 0..300 {
            timers
                .arm(Duration::ZERO, TrafficClass::Normal, kind, Vec::new())
                .unwrap();
        }
        let mut run = std::pin::pin!(inputs.run());
        let first = poll_fn(|cx| Poll::Ready(run.as_mut().poll(cx))).await;
        assert!(first.is_pending());
        let taken = seen.borrow().len();
        assert!(taken > 0 && taken < 300, "{taken} in one poll");
        run.await;
        assert_eq!(kinds(&seen), (0..300).collect::<Vec<u16>>());
    }

    /// A timer armed while the run waits on an empty ring wakes it, so it fires at its deadline.
    #[tokio::test(start_paused = true)]
    async fn arming_a_timer_wakes_a_waiting_run() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        let armer = timers.clone();
        let other = async move {
            tokio::task::yield_now().await;
            armer
                .arm(
                    Duration::from_millis(30),
                    TrafficClass::Normal,
                    5,
                    Vec::new(),
                )
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            // An earlier one armed while the run sleeps toward the first is taken first.
            armer
                .arm(
                    Duration::from_millis(5),
                    TrafficClass::Normal,
                    4,
                    Vec::new(),
                )
                .unwrap();
            drop(sender);
        };
        tokio::join!(inputs.run(), other);
        assert_eq!(kinds(&seen), [4, 5]);
    }

    /// Reviewer B's B1 on PR #172: an arm that arms nothing says why, so a consumer never loses
    /// a timer (a force-close escalation, a resume hold) without knowing: once the inputs have
    /// ended, and for a delay past the end of the clock.
    #[tokio::test(start_paused = true)]
    async fn an_arm_that_arms_nothing_is_refused_by_name() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        let past = timers.arm(Duration::MAX, TrafficClass::Normal, 1, Vec::new());
        assert_eq!(past, Err(ArmRefusal::PastClock));
        drop(sender);
        inputs.run().await;
        let ended = timers.arm(Duration::ZERO, TrafficClass::Safety, 2, Vec::new());
        assert_eq!(ended, Err(ArmRefusal::Ended));
        assert_eq!(
            (timers.arms(), timers.not_armed(), timers.pending()),
            (2, 2, 0)
        );
        assert_eq!(
            ArmRefusal::Ended.to_string(),
            "the inputs that fire these timers have ended"
        );
        assert_eq!(
            ArmRefusal::PastClock.to_string(),
            "the timer's delay lies past the end of the clock"
        );
    }

    /// Reviewer B's B1 on PR #172: a shard restarted with new inputs on the timers its handlers
    /// hold fires them again, and the ended inputs, dropped later, end nothing of the new.
    #[tokio::test(start_paused = true)]
    async fn new_inputs_on_the_timers_of_ended_ones_fire_them_again() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut first, sender) = inputs(1, &timers, &seen);
        drop(sender);
        first.run().await;
        let (mut second, sender) = inputs(1, &timers, &seen);
        drop(first);
        timers
            .arm(Duration::from_secs(1), TrafficClass::Normal, 3, Vec::new())
            .unwrap();
        drop(sender);
        second.run().await;
        assert_eq!(kinds(&seen), [3]);
        assert_eq!(second.counters().fired, 1);
    }

    /// Two inputs never fire one set of timers: new inputs on timers whose inputs have not ended
    /// are refused by name, and once those are dropped unrun the timers are free again.
    #[test]
    fn timers_other_inputs_still_fire_are_refused_by_name() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (first, _sender) = inputs(1, &timers, &seen);
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity: 1,
            timers: timers.clone(),
        };
        let e = ConsumerInputs::new(config, |_: ConsumerInput| {}).unwrap_err();
        assert_eq!(e, InputsError::TimersInUse);
        assert_eq!(
            e.to_string(),
            "the consumer's timers are fired by other inputs that have not ended"
        );
        drop(first);
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity: 1,
            timers: timers.clone(),
        };
        assert!(ConsumerInputs::new(config, |_: ConsumerInput| {}).is_ok());
    }

    /// Reviewer B's B2 on PR #172: inputs that ended stay ended. What the ring held when they
    /// ended is counted and never handed on, and a second run, as a consumer re-running its
    /// shard on them would make, returns at once handing nothing, even once new inputs have
    /// taken the timers.
    #[tokio::test(start_paused = true)]
    async fn a_second_run_hands_on_nothing_and_what_the_ring_held_is_counted() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(2, &timers, &seen);
        sender
            .push(TrafficClass::Safety, 1, b"Start".to_vec())
            .unwrap();
        drop(inputs.run());
        assert_eq!(inputs.counters().unhandled, 1);
        assert_eq!(
            sender.push(TrafficClass::Safety, 2, b"Stop".to_vec()),
            Err(InputRefusal::Closed)
        );
        let (_fresh, _fresh_sender) = self::inputs(1, &timers, &seen);
        timers
            .arm(Duration::ZERO, TrafficClass::Normal, 3, Vec::new())
            .unwrap();
        assert_eq!(inputs.run().now_or_never(), Some(()));
        assert!(seen.borrow().is_empty());
        assert_eq!(timers.pending(), 1);
        assert_eq!(
            inputs.counters(),
            InputCounters {
                pushed: 0,
                fired: 0,
                unhandled: 1
            }
        );
    }
}
