//! The shard host (FBC-ct0, decisions 0002, 0023, 0057, 0100): one current-thread loop per
//! group of markets that drains every input its sessions have ready, journals a `Cycle` record,
//! runs the consumer's decide pass, and submits the commands that pass decided one at a time,
//! polling every session between two, so a protective cancel a handler issues meanwhile waits
//! for at most the one command in progress and goes ahead of the rest (design §5.1-§5.2).
//!
//! The host drives the runs of the shard's sessions as its [`Member`]s
//! ([`MdSession::run`], [`MdVenue::run`], [`ExecSession::run`]) on the one task that awaits
//! [`ShardHost::run`], polling each itself, and spawns no task. Every session and the host share
//! one [`IngestClock`]: every input a session takes is stamped from it, which is how the host
//! sees one taken.
//!
//! **A pass.** Once an input has been stamped since the last pass, the host drains: it yields
//! to the runtime, which polls its I/O and time drivers before resuming the host, then polls
//! every member once, until such a round stamps no input. It then journals
//! [`Record::Cycle`](fbc_journal::Record::Cycle) under Normal, with the last ingest sequence the
//! clock gave and the instruments the consumer marked through [`ShardHost::changed`] since the
//! last pass, in the order first marked, and calls [`Decide::decide`] with those instruments
//! once. The events of every input the pass covers have reached the consumer's handlers
//! (`MdHandler::on_md`, `ExecHandler::on_exec`) inline, as they always do (0023, 0053).
//!
//! **The ladder.** The decide pass pushes its commands onto a [`Ladder`]: each an fbc-oms
//! [`Authorization`] for an order-entry session's [`ExecOrders`], or a [`ControlCommand`], in the
//! order pushed. The host submits one, lets the members run until that session holds nothing
//! (the command encoded, signed and written, or reported not sent), yields to the runtime and
//! polls every member once (the between-command poll: frames the kernel has delivered are read
//! and handed to the handlers), and only then submits the next. A command a handler submits
//! through `ExecOrders` meanwhile is therefore taken before the ladder's next one, and every
//! ladder command is re-checked at its encode (`Authorization::check_at_submit`, 0060, 0066): a
//! kill switch a handler turns on between two commands leaves each remaining place, batch and
//! amend `NotSent(StaleAuthorization)`, nothing written, while cancels still go out.
//!
//! **Attribution.** A command a handler submits while it handles an input is attributed to that
//! input, whichever session took it (the clock keeps the stamp of the input being handled), for
//! FBC-2y3's tick-to-wire; a ladder command is attributed to no input.
//!
//! **Ends.** A member that ends without an error is polled no more, and the run returns once
//! every member has ended. A member's error ends the run at once and is returned, its other
//! members' runs dropped (their connections close, which per-connection cancel-on-disconnect
//! protects, 0058). A ladder command never submitted, because its session had ended or the run
//! ended before its turn, is handed back to the consumer ([`Decide::unsubmitted`]) and counted
//! ([`ShardHost::unsubmitted`]); nothing of it reached a session.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use fbc_core::{InstrumentId, TrafficClass};
use fbc_journal::Record;
use fbc_oms::{Authorization, ControlCommand};

use crate::exec_orders::ExecOrders;
use crate::exec_session::{ExecHandler, ExecSession, ExecSessionError};
use crate::journal::Journal;
use crate::md_venue::MdVenue;
use crate::session::{MdHandler, MdSession, SessionError};
use crate::session_core::IngestClock;

/// Why a shard's run ended in an error: the error of the member that failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardError {
    /// A market-data session or venue failed.
    Md(SessionError),
    /// An order-entry session failed.
    Exec(ExecSessionError),
}

impl fmt::Display for ShardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardError::Md(e) => write!(f, "a market-data session failed: {e}"),
            ShardError::Exec(e) => write!(f, "an order-entry session failed: {e}"),
        }
    }
}

impl std::error::Error for ShardError {}

/// A member's run, until it ends.
type Run<'a> = Pin<Box<dyn Future<Output = Result<(), ShardError>> + 'a>>;

/// One session of the shard, whose run the host drives ([`ShardHost::run`]).
pub struct Member<'a> {
    run: Run<'a>,
}

impl fmt::Debug for Member<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Member")
    }
}

impl<'a> Member<'a> {
    /// A market-data session: its [`MdSession::run`].
    pub fn md<H: MdHandler + 'a>(session: &'a mut MdSession<H>) -> Member<'a> {
        let run = async move { session.run().await.map_err(ShardError::Md) };
        Member { run: Box::pin(run) }
    }

    /// A venue's market data: its [`MdVenue::run`].
    pub fn md_venue<H: MdHandler + 'static>(venue: &'a mut MdVenue<H>) -> Member<'a> {
        let run = async move { venue.run().await.map_err(ShardError::Md) };
        Member { run: Box::pin(run) }
    }

    /// An order-entry session: its [`ExecSession::run`].
    pub fn exec<H: ExecHandler + 'a>(session: &'a mut ExecSession<H>) -> Member<'a> {
        let run = async move { session.run().await.map_err(ShardError::Exec) };
        Member { run: Box::pin(run) }
    }
}

/// The instruments the consumer marks changed between two passes, which the next pass decides
/// for, in the order first marked ([`ShardHost::changed`]). Cloning it shares it.
#[derive(Clone, Debug, Default)]
pub struct Changed {
    marked: Rc<RefCell<Vec<InstrumentId>>>,
}

impl Changed {
    /// Marks `inst` changed since the last pass; marking it again changes nothing.
    pub fn mark(&self, inst: InstrumentId) {
        let mut marked = self.marked.borrow_mut();
        if !marked.contains(&inst) {
            marked.push(inst);
        }
    }

    /// The instruments marked since the last take, in the order first marked.
    fn take(&self) -> Vec<InstrumentId> {
        std::mem::take(&mut *self.marked.borrow_mut())
    }
}

/// A command a decide pass planned and the host never submitted ([`Decide::unsubmitted`]):
/// nothing of it reached a session.
#[derive(Debug)]
pub enum Unsubmitted {
    /// An order-affecting command fbc-oms authorized, its authorization unspent.
    Authorized(Authorization),
    /// A command that affects no order.
    Control(ControlCommand),
}

/// A planned command and the session it goes to.
struct Planned {
    orders: ExecOrders,
    command: Unsubmitted,
}

impl Planned {
    /// Submits the command to its session, which takes it: the ladder took only an authorization
    /// for the session's account, and the host submits only to a session that has not ended.
    fn submit(self) {
        let submitted = match self.command {
            Unsubmitted::Authorized(auth) => self.orders.submit(auth),
            Unsubmitted::Control(cmd) => self.orders.submit_control(cmd),
        };
        debug_assert!(submitted.is_ok(), "{submitted:?}");
    }
}

/// The commands one decide pass planned, which the host submits one at a time, in the order
/// they were pushed, polling every session between two (decision 0100).
#[derive(Default)]
pub struct Ladder {
    planned: VecDeque<Planned>,
}

impl fmt::Debug for Ladder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ladder")
            .field("len", &self.planned.len())
            .finish()
    }
}

impl Ladder {
    /// Plans the order-affecting command fbc-oms authorized, for the session `orders` reaches.
    /// An authorization for another account than that session's, which the session would refuse
    /// ([`SubmitRefusal::OtherAccount`](crate::SubmitRefusal::OtherAccount)), is handed back,
    /// with nothing planned.
    pub fn push(
        &mut self,
        orders: &ExecOrders,
        auth: Authorization,
    ) -> Result<(), Box<Authorization>> {
        if auth.account() != orders.account() {
            return Err(Box::new(auth));
        }
        let command = Unsubmitted::Authorized(auth);
        let orders = orders.clone();
        self.planned.push_back(Planned { orders, command });
        Ok(())
    }

    /// Plans a command that affects no order, for the session `orders` reaches.
    pub fn push_control(&mut self, orders: &ExecOrders, cmd: ControlCommand) {
        let command = Unsubmitted::Control(cmd);
        let orders = orders.clone();
        self.planned.push_back(Planned { orders, command });
    }

    /// How many commands are planned.
    pub fn len(&self) -> usize {
        self.planned.len()
    }

    /// Whether no command is planned.
    pub fn is_empty(&self) -> bool {
        self.planned.is_empty()
    }
}

/// The consumer's decide pass (design §4.8): called once per pass, after every ready input has
/// been handed to the handlers, with the instruments marked changed since the last pass, in the
/// order first marked (none when nothing was marked). The commands it decides go on `ladder`.
pub trait Decide {
    fn decide(&mut self, instruments: &[InstrumentId], ladder: &mut Ladder);

    /// A ladder command the host never submitted: its session had ended when its turn came, or
    /// the run ended first (every member ended, or one failed). Nothing of it reached a session,
    /// so no `on_submitted` reports it; an authorization comes back unspent. Nothing by default.
    fn unsubmitted(&mut self, command: Unsubmitted) {
        let _ = command;
    }
}

impl<F: FnMut(&[InstrumentId], &mut Ladder)> Decide for F {
    fn decide(&mut self, instruments: &[InstrumentId], ladder: &mut Ladder) {
        self(instruments, ladder)
    }
}

/// One shard's loop over its sessions (decision 0100), driven by [`ShardHost::run`].
pub struct ShardHost {
    clock: IngestClock,
    changed: Changed,
    journal: Option<Journal>,
    passes: u64,
    unsubmitted: u64,
}

impl fmt::Debug for ShardHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardHost")
            .field("passes", &self.passes)
            .field("unsubmitted", &self.unsubmitted)
            .finish_non_exhaustive()
    }
}

impl ShardHost {
    /// A host whose sessions stamp their inputs from `clock`: the same clock every member's
    /// session was configured with.
    pub fn new(clock: IngestClock) -> ShardHost {
        ShardHost {
            clock,
            changed: Changed::default(),
            journal: None,
            passes: 0,
            unsubmitted: 0,
        }
    }

    /// Where the consumer marks the instruments its handlers saw change, which the next pass
    /// decides for.
    pub fn changed(&self) -> Changed {
        self.changed.clone()
    }

    /// Journals each pass's `Cycle` record into `journal` from now on (0006), under Normal.
    pub fn set_journal(&mut self, journal: Journal) {
        self.journal = Some(journal);
    }

    /// The decide passes run so far.
    pub fn passes(&self) -> u64 {
        self.passes
    }

    /// The ladder commands handed back unsubmitted ([`Decide::unsubmitted`]).
    pub fn unsubmitted(&self) -> u64 {
        self.unsubmitted
    }

    /// Drives `members` until every one has ended, or one fails, running a decide pass through
    /// `decide` after each drain and submitting what it decides one command at a time (the
    /// module docs say how).
    pub async fn run<D: Decide + ?Sized>(
        &mut self,
        members: Vec<Member<'_>>,
        decide: &mut D,
    ) -> Result<(), ShardError> {
        let mut running = Running(members.into_iter().map(|m| Some(m.run)).collect());
        // The next ingest sequence as the last pass began: an input stamped since calls for one.
        let mut decided = self.clock.next_seq();
        loop {
            let clock = &self.clock;
            if !running.until(|| clock.next_seq() != decided).await? {
                return Ok(());
            }
            if !running.drain(clock).await? {
                return Ok(());
            }
            decided = self.clock.next_seq();
            let mut ladder = self.pass(decide, decided);
            let climbed = self.climb(&mut running, &mut ladder, decide).await;
            // What the run's end left of the ladder goes back to the consumer.
            if !matches!(climbed, Ok(true)) {
                for planned in ladder.planned.drain(..) {
                    self.hand_back(decide, planned);
                }
            }
            if !climbed? {
                return Ok(());
            }
        }
    }

    /// Submits `ladder`'s commands one at a time, each once its session holds nothing, polling
    /// every member between two: true once every one was taken, false (with what is left still
    /// in `ladder`) once every member has ended, or the first error.
    async fn climb<D: Decide + ?Sized>(
        &mut self,
        running: &mut Running<'_>,
        ladder: &mut Ladder,
        decide: &mut D,
    ) -> Result<bool, ShardError> {
        while let Some(planned) = ladder.planned.pop_front() {
            let orders = planned.orders.clone();
            // What a handler submitted is taken first.
            let idle = running.until(|| orders.idle()).await;
            if !matches!(idle, Ok(true)) {
                ladder.planned.push_front(planned);
                return idle;
            }
            if orders.ended() {
                self.hand_back(decide, planned);
                continue;
            }
            // A ladder command is attributed to no input.
            self.clock.handled();
            planned.submit();
            if !running.until(|| orders.idle()).await? {
                return Ok(false);
            }
            // Between two commands: what the kernel has delivered is read and handed on.
            tokio::task::yield_now().await;
            if !running.once().await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Hands `planned` back to the consumer, never submitted.
    fn hand_back<D: Decide + ?Sized>(&mut self, decide: &mut D, planned: Planned) {
        self.unsubmitted += 1;
        decide.unsubmitted(planned.command);
    }

    /// Journals the pass whose last input is the one before ingest sequence `next`, and runs the
    /// decide pass for the instruments marked since the last: its ladder.
    fn pass<D: Decide + ?Sized>(&mut self, decide: &mut D, next: u64) -> Ladder {
        let instruments = self.changed.take();
        if let Some(journal) = &self.journal {
            let (_, wall) = self.clock.now();
            let record = Record::Cycle {
                last_ingest_seq: next.wrapping_sub(1),
                instruments: instruments.clone(),
            };
            journal.record(TrafficClass::Normal, wall, &record);
        }
        // What the decide pass submits itself is attributed to no input.
        self.clock.handled();
        let mut ladder = Ladder::default();
        decide.decide(&instruments, &mut ladder);
        self.passes += 1;
        ladder
    }
}

/// The members still running.
struct Running<'a>(Vec<Option<Run<'a>>>);

impl Running<'_> {
    /// Polls every member still running once: false once none is, or the first error.
    fn poll_all(&mut self, cx: &mut Context<'_>) -> Result<bool, ShardError> {
        for slot in &mut self.0 {
            if let Some(run) = slot
                && let Poll::Ready(ended) = run.as_mut().poll(cx)
            {
                *slot = None;
                ended?;
            }
        }
        Ok(self.0.iter().any(Option::is_some))
    }

    /// Polls the members until `done` holds after a round: true then, false once every member
    /// has ended, or the first error.
    async fn until(&mut self, mut done: impl FnMut() -> bool) -> Result<bool, ShardError> {
        poll_fn(|cx| match self.poll_all(cx) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(false) => Poll::Ready(Ok(false)),
            Ok(true) if done() => Poll::Ready(Ok(true)),
            Ok(true) => Poll::Pending,
        })
        .await
    }

    /// Polls every member once: false once none is running, or the first error.
    async fn once(&mut self) -> Result<bool, ShardError> {
        poll_fn(|cx| Poll::Ready(self.poll_all(cx))).await
    }

    /// Yields to the runtime and polls every member once, until such a round stamps no input
    /// from `clock`: false once none is running, or the first error.
    async fn drain(&mut self, clock: &IngestClock) -> Result<bool, ShardError> {
        loop {
            let before = clock.next_seq();
            tokio::task::yield_now().await;
            if !self.once().await? {
                return Ok(false);
            }
            if clock.next_seq() == before {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instrument_marked_again_keeps_its_first_place_and_a_take_empties_the_marks() {
        let changed = Changed::default();
        let shared = changed.clone();
        let (a, b) = (InstrumentId::new(1), InstrumentId::new(2));
        changed.mark(b);
        shared.mark(a);
        changed.mark(b);
        assert_eq!(changed.take(), [b, a]);
        assert!(shared.take().is_empty());
    }

    #[test]
    fn a_shard_error_reads_as_the_failed_members() {
        let md = ShardError::Md(SessionError::NoConnectionLeft);
        assert_eq!(
            md.to_string(),
            "a market-data session failed: the venue has no connection number left for an \
             endpoint"
        );
        let exec = ShardError::Exec(ExecSessionError::Ended);
        assert_eq!(
            exec.to_string(),
            "an order-entry session failed: the session has run; it runs once"
        );
        let host = ShardHost::new(IngestClock::new());
        assert!(format!("{host:?}").starts_with("ShardHost"));
        assert_eq!(format!("{:?}", Ladder::default()), "Ladder { len: 0 }");
    }
}
