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
//!   like any other; the handler tells one from a pushed input by the kind it gave it.
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
    /// Whether the inputs that fire them have ended.
    ended: bool,
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
    /// Live timers, to give the [`ConsumerInputs`] that fires them (one) and the handlers that
    /// arm them.
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
    /// `bytes`, journaled under `class`. Counted and not armed when these timers are inert, when
    /// the inputs that fire them have ended, or when `after` lies past the end of the clock.
    pub fn arm(&self, after: Duration, class: TrafficClass, kind: u16, bytes: Vec<u8>) {
        let deadline = Instant::now().checked_add(after);
        let waker = {
            let mut st = self.state.borrow_mut();
            st.arms += 1;
            match deadline {
                Some(deadline) if st.live && !st.ended => {
                    let order = st.next;
                    st.next += 1;
                    let fired = Pushed { class, kind, bytes };
                    st.due.insert((deadline, order), fired);
                    st.waker.take()
                }
                _ => {
                    st.not_armed += 1;
                    None
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
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

    /// Takes the first timer due at `now` that was armed before `horizon`.
    fn take_due(&self, now: Instant, horizon: u64) -> Option<Pushed> {
        let mut st = self.state.borrow_mut();
        let key = st
            .due
            .keys()
            .take_while(|(deadline, _)| *deadline <= now)
            .find(|(_, order)| *order < horizon)
            .copied()?;
        st.due.remove(&key)
    }

    /// The earliest timer's deadline, and whether it was armed before `horizon`, once the run
    /// that fires them is woken on an arm.
    fn earliest(&self, waker: &Waker, horizon: u64) -> Option<(Instant, bool)> {
        let mut st = self.state.borrow_mut();
        st.waker = Some(waker.clone());
        let &(deadline, order) = st.due.keys().next()?;
        Some((deadline, order < horizon))
    }

    /// The inputs that fire these timers ended: what is armed never fires and nothing more is.
    fn end(&self) {
        if let Ok(mut st) = self.state.try_borrow_mut() {
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
}

impl fmt::Display for InputsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputsError::Capacity(n) => write!(
                f,
                "an input ring of capacity {n}: it must hold between 1 and {MAX_INPUT_CAPACITY}"
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
    rx: mpsc::Receiver<Pushed>,
    /// Whether every sender has dropped, the ring emptied.
    closed: bool,
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

impl<H> ConsumerInputs<H> {
    /// Ends the inputs: the ring refuses every push from now on, and the timers arm nothing.
    fn end(&mut self) {
        self.rx.close();
        self.timers.end();
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
        if capacity == 0 || capacity > MAX_INPUT_CAPACITY {
            return Err(InputsError::Capacity(capacity));
        }
        let (tx, rx) = mpsc::channel(capacity);
        let inputs = ConsumerInputs {
            clock,
            rx,
            closed: false,
            timers,
            journal: None,
            counters: InputCounters::default(),
            handler,
        };
        let sender = InputSender {
            tx,
            refused: Arc::new(AtomicU64::new(0)),
        };
        Ok((inputs, sender))
    }

    /// Journals every input into `journal` from now on (0006).
    pub fn set_journal(&mut self, journal: Journal) {
        self.journal = Some(journal);
    }

    pub fn counters(&self) -> InputCounters {
        self.counters
    }

    /// Takes every input as it comes, stamped, journaled and handed to the handler, until every
    /// sender has dropped and no timer is pending. Once it returns, or is dropped, the ring
    /// refuses every push and the timers arm nothing.
    pub async fn run(&mut self) {
        let ending = Ending(self);
        let mut sleep = None;
        let inputs = &mut *ending.0;
        poll_fn(|cx| inputs.poll_run(cx, &mut sleep)).await;
    }

    /// Takes what is ready: a timer due that was armed before this poll began (so a handler
    /// that re-arms at once yields between firings), else what waits in the ring, until neither
    /// is; ready once the ring has closed and no timer is pending.
    fn poll_run(&mut self, cx: &mut Context<'_>, sleep: &mut Option<Pin<Box<Sleep>>>) -> Poll<()> {
        let horizon = self.timers.horizon();
        loop {
            if let Some(fired) = self.timers.take_due(Instant::now(), horizon) {
                self.counters.fired += 1;
                self.take(fired);
                continue;
            }
            if !self.closed {
                match self.rx.poll_recv(cx) {
                    Poll::Ready(Some(pushed)) => {
                        self.counters.pushed += 1;
                        self.take(pushed);
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
    /// under its class, then hands it to the handler.
    fn take(&mut self, input: Pushed) {
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
        self.handler.on_input(ConsumerInput {
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

/// A run that returns, or is dropped, ends the inputs ([`ConsumerInputs::end`]).
struct Ending<'a, H>(&'a mut ConsumerInputs<H>);

impl<H> Drop for Ending<'_, H> {
    fn drop(&mut self) {
        self.0.end();
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
        let clock = inputs.clock.clone();
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
                fired: 0
            }
        );
        // Ended: the timers arm nothing.
        timers.arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new());
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
        timers.arm(Duration::from_secs(5), TrafficClass::Normal, 7, Vec::new());
        assert_eq!(timers.pending(), 1);
        assert!(inputs.run().now_or_never().is_none());
        assert_eq!(timers.pending(), 0);
        assert_eq!(
            sender.push(TrafficClass::Safety, 1, Vec::new()),
            Err(InputRefusal::Closed)
        );
        assert_eq!(sender.refused(), 0);
        timers.arm(Duration::ZERO, TrafficClass::Normal, 7, Vec::new());
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
        timers.arm(secs(2), TrafficClass::Normal, 20, Vec::new());
        timers.arm(secs(1), TrafficClass::Normal, 10, Vec::new());
        timers.arm(secs(2), TrafficClass::Safety, 21, b"x".to_vec());
        timers.arm(Duration::MAX, TrafficClass::Normal, 99, Vec::new());
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

        let inert = ConsumerTimers::inert();
        inert.arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new());
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
                re_arm.arm(Duration::ZERO, TrafficClass::Normal, input.kind, Vec::new());
            }
        };
        let config = ConsumerInputsConfig {
            clock: IngestClock::new(),
            capacity: 1,
            timers: timers.clone(),
        };
        let (mut inputs, sender) = ConsumerInputs::new(config, handler).unwrap();
        timers.arm(Duration::ZERO, TrafficClass::Normal, 1, Vec::new());
        let other = async {
            tokio::task::yield_now().await;
            stop.set(true);
            drop(sender);
        };
        tokio::join!(inputs.run(), other);
        assert!(fired.get() >= 2 && fired.get() < 10, "{}", fired.get());
        assert_eq!(timers.pending(), 0);
    }

    /// A timer armed while the run waits on an empty ring wakes it, so it fires at its deadline.
    #[tokio::test(start_paused = true)]
    async fn arming_a_timer_wakes_a_waiting_run() {
        let (timers, seen) = (ConsumerTimers::new(), Seen::default());
        let (mut inputs, sender) = inputs(1, &timers, &seen);
        let armer = timers.clone();
        let other = async move {
            tokio::task::yield_now().await;
            armer.arm(
                Duration::from_millis(30),
                TrafficClass::Normal,
                5,
                Vec::new(),
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            // An earlier one armed while the run sleeps toward the first is taken first.
            armer.arm(
                Duration::from_millis(5),
                TrafficClass::Normal,
                4,
                Vec::new(),
            );
            drop(sender);
        };
        tokio::join!(inputs.run(), other);
        assert_eq!(kinds(&seen), [4, 5]);
    }
}
