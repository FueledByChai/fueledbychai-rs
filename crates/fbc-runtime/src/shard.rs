//! The shard host (FBC-ct0, decisions 0002, 0023, 0057, 0100): one current-thread loop per
//! group of markets that drains every input its sessions have ready, journals a `Cycle` record,
//! runs the consumer's decide pass, and submits the commands that pass decided one at a time,
//! polling every session between two, so a protective cancel a handler issues meanwhile waits
//! for at most the one command in progress and goes ahead of the rest (design §5.1-§5.2).
//!
//! The host drives the runs of the shard's sessions as its [`Member`]s
//! ([`MdSession::run`], [`MdVenue::run`], [`ExecSession::run`], and the consumer's own inputs'
//! [`ConsumerInputs::run`], FBC-hvqi) on the one task that awaits
//! [`ShardHost::run`], polling each itself, and spawns no task. Every session and the host share
//! one [`IngestClock`]: every input a session takes is stamped from it, which is how the host
//! sees one taken.
//!
//! **A pass.** Once an input has been stamped since the last pass, the host drains: it yields
//! to the runtime, which polls its I/O and time drivers before resuming the host, then polls
//! every member once (again while such a round ends with the task's coop budget spent, since
//! the members share one task poll's budget), until such a round stamps no input. It then journals
//! [`Record::Cycle`](fbc_journal::Record::Cycle) under Normal, with the last ingest sequence the
//! clock gave and the instruments the consumer marked through [`ShardHost::changed`] since the
//! last pass, in the order first marked, and calls [`Decide::decide`] with those instruments
//! once. The events of every input the pass covers have reached the consumer's handlers
//! (`MdHandler::on_md`, `ExecHandler::on_exec`) inline, as they always do (0023, 0053).
//!
//! **The ladder.** The decide pass pushes its commands onto a [`Ladder`]: each an fbc-oms
//! [`Authorization`] for an order-entry session's [`ExecOrders`], or a [`ControlCommand`], in the
//! order pushed. The host submits one once every order-entry member's session holds nothing,
//! lets the members run until they again hold nothing (the command encoded, signed and written,
//! or reported not sent), yields to the runtime and
//! polls every member once, again while the round ends with the coop budget spent (the
//! between-command poll: frames the kernel has delivered are read and handed to the handlers),
//! and only then submits the next. A command a handler submits
//! through `ExecOrders` meanwhile is therefore taken before the ladder's next one, and every
//! ladder command is re-checked at its encode (`Authorization::check_at_submit`, 0060, 0066): a
//! kill switch a handler turns on between two commands leaves each remaining place, batch and
//! amend `NotSent(StaleAuthorization)`, nothing written, while cancels still go out.
//!
//! **Attribution.** A command a handler submits while it is handed an input's events is
//! attributed to that input, whichever session takes it (the clock names that input for the
//! length of the call), for FBC-2y3's tick-to-wire; a ladder command is attributed to no input.
//!
//! **Ends.** A member that ends without an error is polled no more, and the run returns once
//! every member has ended. A member's error ends the run at once and is returned, its other
//! members' runs dropped (their connections close, which per-connection cancel-on-disconnect
//! protects, 0058) and their orders ended, so nothing submitted to them afterwards is taken;
//! the run's return or drop ends them too. A ladder command never submitted, because its
//! session had ended or the run ended before its turn, or submitted and withdrawn because the
//! run or its session ended before the session encoded it (a session failing after taking it,
//! as on a short nonce reservation, included), is handed back to the consumer
//! ([`Decide::unsubmitted`]) and counted ([`ShardHost::unsubmitted`]); nothing of it reached a
//! codec. A consumer that drops the run while a ladder goes out is handed back the same.
//!
//! **Fairness.** The members share the coop budget of the task poll they are polled in. When it
//! runs out, the next poll of every member starts at the first member it ran out before, so a
//! member that spends every budget it is given starves no other; the drain's and the
//! between-command rounds go on until every member has had a poll that left budget, or one
//! with the budget whole.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use fbc_core::{InstrumentId, RpcId, TrafficClass};
use fbc_journal::Record;
use fbc_oms::{Authorization, ControlCommand};

use crate::exec_orders::{ExecOrders, Submitted};
use crate::exec_session::{ExecHandler, ExecSession, ExecSessionError};
use crate::inputs::{ConsumerInputs, InputHandler};
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
    /// An order-entry member's orders: the host waits for what its handlers submitted there.
    orders: Option<ExecOrders>,
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
        let orders = None;
        Member {
            run: Box::pin(run),
            orders,
        }
    }

    /// A venue's market data: its [`MdVenue::run`].
    pub fn md_venue<H: MdHandler + 'static>(venue: &'a mut MdVenue<H>) -> Member<'a> {
        let run = async move { venue.run().await.map_err(ShardError::Md) };
        let orders = None;
        Member {
            run: Box::pin(run),
            orders,
        }
    }

    /// The consumer's own inputs: [`ConsumerInputs::run`], which never fails.
    pub fn inputs<H: InputHandler + 'a>(inputs: &'a mut ConsumerInputs<H>) -> Member<'a> {
        // Called now, not on the first poll: a member dropped unpolled ends the inputs too
        // (Codex P1 r4311000152 on PR #172).
        let run = inputs.run();
        let run = async move {
            run.await;
            Ok(())
        };
        let orders = None;
        Member {
            run: Box::pin(run),
            orders,
        }
    }

    /// An order-entry session: its [`ExecSession::run`].
    pub fn exec<H: ExecHandler + 'a>(session: &'a mut ExecSession<H>) -> Member<'a> {
        let orders = Some(session.orders());
        let run = async move { session.run().await.map_err(ShardError::Exec) };
        Member {
            run: Box::pin(run),
            orders,
        }
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

/// A ladder command submitted and not yet known taken: its request id, and the control command
/// it was, kept to hand back should it be withdrawn.
struct InFlight {
    rpc: RpcId,
    control: Option<ControlCommand>,
}

impl Planned {
    /// Submits the command to its session, which takes it: the ladder took only an authorization
    /// for the session's account, and the host submits only to a session that has not ended.
    fn submit(self) -> Option<InFlight> {
        let (submitted, control) = match self.command {
            Unsubmitted::Authorized(auth) => (self.orders.submit(auth), None),
            Unsubmitted::Control(cmd) => (self.orders.submit_control(cmd.clone()), Some(cmd)),
        };
        debug_assert!(submitted.is_ok(), "{submitted:?}");
        let rpc = submitted.ok()?;
        // The session keeps it for the host to take back, should it end before encoding it.
        self.orders.keep(rpc);
        Some(InFlight { rpc, control })
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
    /// the run ended first (every member ended, or one failed); or one it submitted and took
    /// back, its session or the run ending before the session encoded it. Nothing of it reached
    /// a codec, so no `on_submitted` reports it; an authorization comes back unspent. Nothing by
    /// default.
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
        let ShardHost {
            clock,
            changed,
            journal,
            passes,
            unsubmitted,
        } = self;
        // Declared before the members' runs, so it drops after them: what it hands back on a
        // drop meets orders already ended.
        let mut climb = Climb {
            decide,
            unsubmitted,
            ladder: Ladder::default(),
            sent: None,
        };
        let execs = members.iter().filter_map(|m| m.orders.clone()).collect();
        let runs = members.into_iter().map(|m| Some(m.run)).collect();
        let mut running = Running {
            runs,
            execs,
            start: 0,
        };
        // The next ingest sequence as the last pass began: an input stamped since calls for one.
        let mut decided = clock.next_seq();
        loop {
            let clock = &*clock;
            if !running.until(|_| clock.next_seq() != decided).await? {
                return Ok(());
            }
            if !running.drain(clock).await? {
                return Ok(());
            }
            decided = clock.next_seq();
            let instruments = changed.take();
            if let Some(journal) = &*journal {
                let (_, wall) = clock.now();
                let record = Record::Cycle {
                    last_ingest_seq: decided.wrapping_sub(1),
                    instruments: instruments.clone(),
                };
                journal.record(TrafficClass::Normal, wall, &record);
            }
            climb.decide.decide(&instruments, &mut climb.ladder);
            *passes += 1;
            let climbed = climb.climb(&mut running).await;
            // What the run's end left of the ladder goes back to the consumer.
            if !matches!(climbed, Ok(true)) {
                climb.give_back();
            }
            if !climbed? {
                return Ok(());
            }
        }
    }
}

/// The ladder of the pass going out, and the command of it submitted and not yet known taken:
/// what is left of it goes back to the consumer however the run ends, the consumer dropping the
/// run included (Codex P1 r4309505789 on PR #164).
struct Climb<'d, D: Decide + ?Sized> {
    decide: &'d mut D,
    unsubmitted: &'d mut u64,
    ladder: Ladder,
    /// The command submitted, with its session's orders, until the session took it.
    sent: Option<(ExecOrders, InFlight)>,
}

impl<D: Decide + ?Sized> Climb<'_, D> {
    /// Submits the ladder's commands one at a time, each once its session holds nothing, polling
    /// every member between two: true once every one was taken, false (with what is left still
    /// in the ladder) once every member has ended, or the first error.
    async fn climb(&mut self, running: &mut Running<'_>) -> Result<bool, ShardError> {
        while let Some(planned) = self.ladder.planned.pop_front() {
            let orders = planned.orders.clone();
            // What a handler submitted, on any order-entry session of the shard, is taken first
            // (Codex P1 r4309119894 on PR #164).
            let quiet = |execs: &[ExecOrders]| execs.iter().all(ExecOrders::idle) && orders.idle();
            let idle = running.until(quiet).await;
            if !matches!(idle, Ok(true)) {
                self.ladder.planned.push_front(planned);
                return idle;
            }
            if orders.ended() {
                self.hand_back(planned.command);
                continue;
            }
            let Some(sent) = planned.submit() else {
                continue;
            };
            self.sent = Some((orders.clone(), sent));
            let taken = running.until(quiet).await;
            if !matches!(taken, Ok(true)) || orders.ended() {
                // The run, or its session, ends before the session took it: it is taken back,
                // nothing of it sent (Codex P1 r4309119897, r4309207253 on PR #164).
                self.withdraw();
                if !matches!(taken, Ok(true)) {
                    return taken;
                }
            }
            self.sent = None;
            // Between two commands: what the kernel has delivered is read and handed on.
            if !running.round().await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Takes back the command submitted, if its session has not taken it, and hands it back.
    fn withdraw(&mut self) {
        let Some((orders, sent)) = self.sent.take() else {
            return;
        };
        let item = orders.withdraw(sent.rpc);
        let command = item.and_then(|item| match item {
            Submitted::Authorized(auth) => Some(Unsubmitted::Authorized(auth)),
            Submitted::Control(_) => sent.control.map(Unsubmitted::Control),
        });
        if let Some(command) = command {
            self.hand_back(command);
        }
    }

    /// Hands back the command submitted and not taken, and every one not yet submitted.
    fn give_back(&mut self) {
        self.withdraw();
        while let Some(planned) = self.ladder.planned.pop_front() {
            self.hand_back(planned.command);
        }
    }

    /// Hands `command` back to the consumer, never sent.
    fn hand_back(&mut self, command: Unsubmitted) {
        *self.unsubmitted += 1;
        self.decide.unsubmitted(command);
    }
}

/// A run the consumer drops hands back what is left of its ladder; not as a panic unwinds,
/// since the consumer's callback may have panicked itself.
impl<D: Decide + ?Sized> Drop for Climb<'_, D> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.give_back();
        }
    }
}

/// The members still running, and the orders of every order-entry member.
struct Running<'a> {
    runs: Vec<Option<Run<'a>>>,
    execs: Vec<ExecOrders>,
    /// The member the next poll of every member starts at ([`Running::poll_all`]).
    start: usize,
}

impl Running<'_> {
    /// Polls every member still running once, from the one at `start`: false once none is, or
    /// the first error, every other member's run dropped then ([`Running::abort`]).
    ///
    /// The members share the task poll's coop budget. When it runs out, the next poll starts
    /// at the first member polled with none left, or, when the first member polled spent it,
    /// at the member after that one, so a member that spends every budget it is given starves
    /// no other (Codex P1 r4309505785 on PR #164). `served` marks each member whose poll left
    /// budget, or that was polled first, its budget the poll's whole one.
    fn poll_all(
        &mut self,
        cx: &mut Context<'_>,
        mut served: Option<&mut [bool]>,
    ) -> Result<bool, ShardError> {
        let n = self.runs.len();
        let mut first = true;
        let mut next = None;
        for k in 0..n {
            let at = (self.start + k) % n;
            let Some(run) = &mut self.runs[at] else {
                continue;
            };
            let polled = run.as_mut().poll(cx);
            let left = tokio::task::coop::has_budget_remaining();
            if let Some(served) = served.as_deref_mut()
                && (left || first)
            {
                served[at] = true;
            }
            if !left && next.is_none() {
                next = Some(if first { (at + 1) % n } else { at });
            }
            first = false;
            if let Poll::Ready(ended) = polled {
                self.runs[at] = None;
                if let Err(e) = ended {
                    self.abort();
                    return Err(e);
                }
            }
        }
        if let Some(next) = next {
            self.start = next;
        }
        Ok(self.runs.iter().any(Option::is_some))
    }

    /// Drops every member's run and ends every order-entry member's orders: a session runs
    /// once, so one whose run is dropped never sends what waits in it, and its orders take no
    /// more (Codex P1 r4309350874, Reviewer B RB-ct0-3 on PR #164). What the host keeps for
    /// withdrawal stays withdrawable.
    fn abort(&mut self) {
        self.runs.clear();
        for orders in &self.execs {
            orders.end();
        }
    }

    /// Polls the members until `done`, given the order-entry members' orders, holds after a
    /// round: true then, false once every member has ended, or the first error.
    async fn until(
        &mut self,
        mut done: impl FnMut(&[ExecOrders]) -> bool,
    ) -> Result<bool, ShardError> {
        poll_fn(|cx| match self.poll_all(cx, None) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(false) => Poll::Ready(Ok(false)),
            Ok(true) if done(&self.execs) => Poll::Ready(Ok(true)),
            Ok(true) => Poll::Pending,
        })
        .await
    }

    /// A round: yields to the runtime, which polls its I/O and time drivers, then polls every
    /// member once, and again until every member still running has had a poll that left coop
    /// budget, or one that began the task poll, its budget whole. The members share the budget
    /// of the one task poll, so a member with a backlog can spend it and leave a later member's
    /// resource reporting nothing ready though its frame was delivered; a member whose poll
    /// left budget read all it had ready (Reviewer B RB-ct0-1 on PR #164), and one given a
    /// whole budget had its share, so a member that never runs dry ends no round late (Codex
    /// P1 r4309505785). False once none is running, or the first error.
    async fn round(&mut self) -> Result<bool, ShardError> {
        let mut served = vec![false; self.runs.len()];
        loop {
            tokio::task::yield_now().await;
            let polled = poll_fn(|cx| Poll::Ready(self.poll_all(cx, Some(&mut served))));
            if !polled.await? {
                return Ok(false);
            }
            let runs = self.runs.iter().zip(&served);
            if runs
                .into_iter()
                .all(|(run, served)| run.is_none() || *served)
            {
                return Ok(true);
            }
        }
    }

    /// Rounds ([`Running::round`]) until one stamps no input from `clock`: false once none is
    /// running, or the first error.
    async fn drain(&mut self, clock: &IngestClock) -> Result<bool, ShardError> {
        loop {
            let before = clock.next_seq();
            if !self.round().await? {
                return Ok(false);
            }
            if clock.next_seq() == before {
                return Ok(true);
            }
        }
    }
}

/// A run that returns, or is dropped, drops what is left of its members' runs and ends their
/// orders ([`Running::abort`]).
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_orders::{RpcIds, Shared};
    use fbc_core::{AccountKey, ConnKey, RpcId};
    use std::cell::Cell;

    /// What a scripted member does each time it is polled.
    #[derive(Copy, Clone, Debug)]
    enum Act {
        /// Stamps an input and waits.
        Stamp,
        /// Waits.
        Wait,
        /// Takes the command waiting in the session and waits.
        Take,
        /// Ends the session, which takes no more commands, and waits.
        Close,
        /// Ends.
        End,
        /// Fails.
        Fail,
    }

    /// A member that does `acts`, one per poll, waking itself after each wait, and waits
    /// forever once they run out.
    fn scripted(clock: &IngestClock, shared: &Rc<Shared>, acts: &[Act]) -> Member<'static> {
        scripted_as("", &Taken::default(), clock, shared, acts)
    }

    /// The commands scripted members took, by member name, in order.
    type Taken = Rc<RefCell<Vec<(&'static str, RpcId)>>>;

    /// [`scripted`], named `name`, logging each command it takes in `taken`.
    fn scripted_as(
        name: &'static str,
        taken: &Taken,
        clock: &IngestClock,
        shared: &Rc<Shared>,
        acts: &[Act],
    ) -> Member<'static> {
        let (clock, shared, taken) = (clock.clone(), Rc::clone(shared), Rc::clone(taken));
        let acts = acts.to_vec();
        let next = Cell::new(0);
        let run = poll_fn(move |cx| {
            let act = acts.get(next.get()).copied();
            next.set(next.get() + 1);
            match act {
                Some(Act::End) => return Poll::Ready(Ok(())),
                Some(Act::Fail) => {
                    return Poll::Ready(Err(ShardError::Md(SessionError::NoConnectionLeft)));
                }
                Some(Act::Stamp) => {
                    clock.stamp(ConnKey { conn: 1, epoch: 0 }, None);
                }
                Some(Act::Take) => {
                    if let Some(queued) = shared.pop() {
                        taken.borrow_mut().push((name, queued.rpc));
                    }
                }
                Some(Act::Close) => shared.end(),
                Some(Act::Wait) | None => {}
            }
            if act.is_some() {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        });
        let orders = None;
        Member {
            run: Box::pin(run),
            orders,
        }
    }

    /// The orders of a session for account `acct` that stamps from `clock`.
    fn session_orders(acct: u16, clock: &IngestClock) -> (Rc<Shared>, ExecOrders) {
        let ids = RpcIds::default();
        let shared = Shared::new(AccountKey::new(acct), ids, true, false, true, clock.clone());
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        (shared, orders)
    }

    /// Codex P1 r4309119894 on PR #164: a command a handler left waiting on one order-entry
    /// session goes out before the ladder's next command on another, so a protective cancel
    /// waits behind at most the one command in progress whichever session it is for.
    #[tokio::test]
    async fn a_command_waiting_on_one_session_goes_before_the_ladders_next_on_another() {
        use Act::{Stamp, Take, Wait};
        let clock = IngestClock::new();
        let (a_shared, a) = session_orders(2, &clock);
        let (b_shared, b) = session_orders(3, &clock);
        let taken = Taken::default();
        // Session A takes what waits on it only at its fifth poll: the wait for an input, the
        // drain's round, then the ladder's. B takes at once.
        let a_acts = [Stamp, Wait, Wait, Wait, Take];
        let mut a_member = scripted_as("A", &taken, &clock, &a_shared, &a_acts);
        a_member.orders = Some(a.clone());
        let mut b_member = scripted_as("B", &taken, &clock, &b_shared, &[Take; 8]);
        b_member.orders = Some(b.clone());
        let mut host = ShardHost::new(clock.clone());
        let handler = Rc::new(Cell::new(None));
        let sent = Rc::clone(&handler);
        let mut decide = |_: &[InstrumentId], ladder: &mut Ladder| {
            if sent.get().is_none() {
                // As a handler would: a cancel submitted straight to A.
                sent.set(Some(a.submit_control(ControlCommand::FeeQuery).unwrap()));
                ladder.push_control(&b, ControlCommand::FeeQuery);
            }
        };
        let members = vec![a_member, b_member];
        let run = host.run(members, &mut decide);
        let mut run = Box::pin(run);
        // Until both commands were taken.
        let mut polls = 0;
        while taken.borrow().len() < 2 {
            assert!(futures_util::FutureExt::now_or_never(&mut run).is_none());
            tokio::task::yield_now().await;
            polls += 1;
            assert!(polls < 1_000, "{:?}", taken.borrow());
        }
        let order: Vec<_> = taken.borrow().iter().map(|(name, _)| *name).collect();
        assert_eq!(order, ["A", "B"]);
    }

    /// What one scripted run showed: its result, the passes and the ladder commands handed back.
    async fn scripted_run(acts: &[Act], planned: usize) -> (Result<(), ShardError>, u64, u64) {
        let clock = IngestClock::new();
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            clock.clone(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let mut host = ShardHost::new(clock.clone());
        // The ladder's commands, each a control command for a session that never takes one by
        // itself; a member taking one stands in for it.
        let mut decide = |_: &[InstrumentId], ladder: &mut Ladder| {
            for _ in 0..planned {
                ladder.push_control(&orders, ControlCommand::FeeQuery);
            }
        };
        let member = scripted(&clock, &shared, acts);
        assert_eq!(format!("{member:?}"), "Member");
        let ran = host.run(vec![member], &mut decide).await;
        (ran, host.passes(), host.unsubmitted())
    }

    #[tokio::test]
    async fn a_member_ending_or_failing_as_the_shard_drains_ends_the_run_before_any_pass() {
        let (ran, passes, back) = scripted_run(&[Act::Stamp, Act::End], 1).await;
        assert_eq!((ran, passes, back), (Ok(()), 0, 0));
        let (ran, passes, _) = scripted_run(&[Act::Stamp, Act::Fail], 1).await;
        assert_eq!(ran, Err(ShardError::Md(SessionError::NoConnectionLeft)));
        assert_eq!(passes, 0);
    }

    /// Codex P1 r4311000152 on PR #172: the consumer's inputs, dropped unpolled because a member
    /// polled before them failed at once, are ended all the same: the ring refuses every push
    /// after the run and the timers arm nothing, so nothing reports taken what no shard reads.
    #[tokio::test]
    async fn inputs_a_failing_member_drops_before_their_first_poll_are_ended() {
        use crate::inputs::{ConsumerInputs, ConsumerInputsConfig, ConsumerTimers, InputRefusal};
        let clock = IngestClock::new();
        let (shared, _orders) = session_orders(1, &clock);
        let timers = ConsumerTimers::new();
        let config = ConsumerInputsConfig {
            clock: clock.clone(),
            capacity: 4,
            timers: timers.clone(),
        };
        let (mut inputs, sender) = ConsumerInputs::new(config, |_: _| {}).unwrap();
        let mut host = ShardHost::new(clock.clone());
        let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
        let failing = scripted(&clock, &shared, &[Act::Fail]);
        let members = vec![failing, Member::inputs(&mut inputs)];
        let ran = host.run(members, &mut decide).await;
        assert_eq!(ran, Err(ShardError::Md(SessionError::NoConnectionLeft)));
        assert_eq!(
            sender.push(TrafficClass::Safety, 1, b"Stop".to_vec()),
            Err(InputRefusal::Closed)
        );
        timers.arm(
            std::time::Duration::ZERO,
            TrafficClass::Normal,
            2,
            Vec::new(),
        );
        assert_eq!((timers.not_armed(), timers.pending()), (1, 0));
        // A run of the inputs dropped before its first poll ends them too.
        let (mut inputs, sender) = ConsumerInputs::new(
            ConsumerInputsConfig {
                clock: clock.clone(),
                capacity: 4,
                timers: ConsumerTimers::new(),
            },
            |_: _| {},
        )
        .unwrap();
        drop(inputs.run());
        let refused = sender.push(TrafficClass::Safety, 1, Vec::new());
        assert_eq!(refused, Err(InputRefusal::Closed));
    }

    /// The pass's ladder of two: the member ends or fails at the poll the host makes before the
    /// first command's turn, after it was submitted, or between the two; every command not yet
    /// submitted is handed back, through the decide pass's default `unsubmitted`.
    #[tokio::test]
    async fn the_runs_end_before_a_ladder_commands_turn_hands_it_back() {
        use Act::{End, Fail, Stamp, Take, Wait};
        // Polled: the wait for an input, the drain's round, then the ladder's.
        let cases: [(&[Act], Result<(), ShardError>, u64); 5] = [
            (&[Stamp, Wait, End], Ok(()), 2),
            (
                &[Stamp, Wait, Fail],
                Err(ShardError::Md(SessionError::NoConnectionLeft)),
                2,
            ),
            // A command submitted that its session had not taken when the run ended is
            // withdrawn and handed back too (Codex P1 r4309119897 on PR #164).
            (&[Stamp, Wait, Wait, End], Ok(()), 2),
            (
                &[Stamp, Wait, Wait, Fail],
                Err(ShardError::Md(SessionError::NoConnectionLeft)),
                2,
            ),
            (&[Stamp, Wait, Wait, Take, End], Ok(()), 1),
        ];
        for (acts, want, back) in cases {
            let shown = scripted_run(acts, 2).await;
            assert_eq!(shown, (want, 1, back), "{acts:?}");
        }
        // The session ends with the command still queued, the run going on: it is handed back
        // too, and the next, whose session has ended, with it (Codex P1 r4309207253 on PR #164).
        let acts = [Stamp, Wait, Wait, Act::Close, Wait, Wait, End];
        assert_eq!(scripted_run(&acts, 2).await, (Ok(()), 1, 2));
        // Between the two, an error ends it too.
        let acts = [Stamp, Wait, Wait, Take, Fail];
        let (ran, _, back) = scripted_run(&acts, 2).await;
        assert_eq!((ran.is_err(), back), (true, 1));
    }

    /// Codex P1 r4309350874 and Reviewer B RB-ct0-3 on PR #164: a member's error ends the
    /// orders of every order-entry member whose run the host drops, and so does dropping the
    /// host's run, so nothing submitted to them afterwards is taken and never sent.
    #[tokio::test]
    async fn a_members_error_or_the_runs_drop_ends_the_orders_of_every_order_entry_member_dropped()
    {
        let clock = IngestClock::new();
        let (shared, orders) = session_orders(2, &clock);
        let mut exec = scripted(&clock, &shared, &[Act::Wait]);
        exec.orders = Some(orders.clone());
        orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let failing = scripted(&clock, &shared, &[Act::Stamp, Act::Fail]);
        let mut host = ShardHost::new(clock.clone());
        let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
        let ran = host.run(vec![exec, failing], &mut decide).await;
        assert!(ran.is_err());
        let refused = orders.submit_control(ControlCommand::FeeQuery);
        assert_eq!(refused, Err(crate::SubmitRefusal::Ended));
        assert!(orders.idle() && !shared.waiting());

        // The consumer drops the run while the member still runs.
        let (shared, orders) = session_orders(2, &clock);
        let mut exec = scripted(&clock, &shared, &[Act::Wait]);
        exec.orders = Some(orders.clone());
        let mut run = Box::pin(host.run(vec![exec], &mut decide));
        assert!(futures_util::FutureExt::now_or_never(&mut run).is_none());
        drop(run);
        let refused = orders.submit_control(ControlCommand::FeeQuery);
        assert_eq!(refused, Err(crate::SubmitRefusal::Ended));
    }

    /// What the members of the coop-budget tests and their decide pass did, in order.
    type Log = Rc<RefCell<Vec<&'static str>>>;

    /// A member that reads every message waiting on `rx`, each a ready operation that spends
    /// the task's coop budget, logging `name` and stamping an input for each when it has one;
    /// with `refill`, it then sends itself more than a budget's worth again, never running dry.
    fn reader(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        named: Option<(&'static str, &Log, &IngestClock)>,
        refill: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    ) -> Member<'static> {
        let named = named.map(|(name, log, clock)| (name, Rc::clone(log), clock.clone()));
        let run = poll_fn(move |cx| {
            while let Poll::Ready(Some(())) = rx.poll_recv(cx) {
                if let Some((name, log, clock)) = &named {
                    log.borrow_mut().push(name);
                    clock.stamp(ConnKey { conn: 2, epoch: 0 }, None);
                }
            }
            if let Some(tx) = &refill {
                while rx.len() < 512 {
                    tx.send(()).unwrap();
                }
            }
            Poll::Pending
        });
        let orders = None;
        Member {
            run: Box::pin(run),
            orders,
        }
    }

    /// A busy member's backlog: more messages than one task poll's coop budget reads.
    const BACKLOG: usize = 4096;

    /// Runs a shard of a busy member, a reference member and an order-entry member (in that
    /// order), with `busy` and `reference` messages waiting at the start; the order-entry member
    /// stamps an input at its first poll and takes each command at once, and on its first take
    /// fills the busy member's backlog and delivers one reference message. The first decide pass
    /// plans `planned` commands. What happened, once the log has `until` entries.
    async fn budgeted(
        busy: usize,
        reference: usize,
        planned: usize,
        until: usize,
        firehose: bool,
    ) -> Vec<&'static str> {
        let clock = IngestClock::new();
        let log = Log::default();
        let (busy_tx, busy_rx) = tokio::sync::mpsc::unbounded_channel();
        let (ref_tx, ref_rx) = tokio::sync::mpsc::unbounded_channel();
        for _ in 0..busy {
            busy_tx.send(()).unwrap();
        }
        for _ in 0..reference {
            ref_tx.send(()).unwrap();
        }
        let refill = firehose.then(|| busy_tx.clone());
        let (shared, orders) = session_orders(2, &clock);
        let (exec_log, exec_clock) = (Rc::clone(&log), clock.clone());
        let mut first = true;
        let run = poll_fn(move |_| {
            if std::mem::take(&mut first) {
                exec_clock.stamp(ConnKey { conn: 1, epoch: 0 }, None);
            }
            if shared.pop().is_some() {
                let mut log = exec_log.borrow_mut();
                if !log.contains(&"cmd") {
                    for _ in 0..BACKLOG {
                        busy_tx.send(()).unwrap();
                    }
                    ref_tx.send(()).unwrap();
                }
                log.push("cmd");
            }
            Poll::Pending
        });
        let exec = Member {
            run: Box::pin(run),
            orders: Some(orders.clone()),
        };
        let members = vec![
            reader(busy_rx, None, refill),
            reader(ref_rx, Some(("ref", &log, &clock)), None),
            exec,
        ];
        let (pass_log, mut decided) = (Rc::clone(&log), false);
        let mut decide = move |_: &[InstrumentId], ladder: &mut Ladder| {
            pass_log.borrow_mut().push("pass");
            if !std::mem::replace(&mut decided, true) {
                for _ in 0..planned {
                    ladder.push_control(&orders, ControlCommand::FeeQuery);
                }
            }
        };
        let mut host = ShardHost::new(clock.clone());
        let mut run = Box::pin(host.run(members, &mut decide));
        let mut polls = 0;
        while log.borrow().len() < until {
            assert!(futures_util::FutureExt::now_or_never(&mut run).is_none());
            tokio::task::yield_now().await;
            polls += 1;
            assert!(polls < 10_000, "{:?}", log.borrow());
        }
        drop(run);
        log.borrow().clone()
    }

    /// Reviewer B RB-ct0-1 on PR #164: the members polled in one task poll share tokio's coop
    /// budget, so a busy member early in the shard can spend it all and leave a later member's
    /// delivered frame unread in a round. The host's round between two ladder commands goes on
    /// until a round leaves budget, so the reference message delivered as the first command was
    /// taken is read before the second is submitted (decision 0100, point 4).
    #[tokio::test]
    async fn a_busy_member_spending_the_coop_budget_leaves_no_delivered_frame_unread_between_two_commands()
     {
        let log = budgeted(0, 0, 2, 4, false).await;
        assert_eq!(log[..4], ["pass", "cmd", "ref", "cmd"]);
    }

    /// Reviewer B RB-ct0-1 on PR #164: the drain goes on while a round spent the coop budget, so
    /// an input waiting behind a busy member's backlog is taken before the pass (point 2).
    #[tokio::test]
    async fn a_busy_member_spending_the_coop_budget_leaves_no_ready_input_out_of_the_pass() {
        let log = budgeted(BACKLOG, 1, 0, 2, false).await;
        assert_eq!(log[..2], ["ref", "pass"]);
    }

    /// Codex P1 r4309505785 on PR #164: a member that spends every fresh coop budget it is
    /// given, first in the shard, starves no later member: the next poll starts at the first
    /// member the budget ran out before, and a round ends once every member has had a poll that
    /// left budget, or one with the budget fresh. The pass still comes, and so do the ladder's
    /// commands, the reference message read between them.
    #[tokio::test]
    async fn a_member_that_always_spends_the_coop_budget_starves_no_later_member() {
        let log = budgeted(0, 1, 0, 2, true).await;
        assert_eq!(log[..2], ["ref", "pass"]);
        let log = budgeted(0, 0, 2, 4, true).await;
        assert_eq!(log[..4], ["pass", "cmd", "ref", "cmd"]);
    }

    /// Codex P1 r4309505789 on PR #164: a consumer that drops the run while a ladder goes out
    /// is handed back the command its session had not taken and every one not yet submitted.
    #[tokio::test]
    async fn dropping_the_run_while_a_ladder_goes_out_hands_back_what_was_not_taken() {
        /// Plans two commands on its first pass, counting what it is handed back.
        struct Plan(ExecOrders, bool, Rc<Cell<usize>>);
        impl Decide for Plan {
            fn decide(&mut self, _: &[InstrumentId], ladder: &mut Ladder) {
                if !std::mem::replace(&mut self.1, true) {
                    ladder.push_control(&self.0, ControlCommand::FeeQuery);
                    ladder.push_control(&self.0, ControlCommand::RefreshDeadMan);
                }
            }
            fn unsubmitted(&mut self, _: Unsubmitted) {
                self.2.set(self.2.get() + 1);
            }
        }
        let clock = IngestClock::new();
        let (shared, orders) = session_orders(2, &clock);
        let mut exec = scripted(&clock, &shared, &[Act::Stamp]);
        exec.orders = Some(orders.clone());
        let back = Rc::new(Cell::new(0));
        let mut decide = Plan(orders.clone(), false, Rc::clone(&back));
        let mut host = ShardHost::new(clock.clone());
        let mut run = Box::pin(host.run(vec![exec], &mut decide));
        let mut polls = 0;
        while !shared.waiting() {
            assert!(futures_util::FutureExt::now_or_never(&mut run).is_none());
            tokio::task::yield_now().await;
            polls += 1;
            assert!(polls < 1_000);
        }
        drop(run);
        assert_eq!(back.get(), 2);
        assert_eq!(host.unsubmitted(), 2);
    }

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
