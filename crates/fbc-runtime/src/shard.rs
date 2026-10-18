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
//! order pushed. The host submits one once every order-entry member's session holds nothing,
//! lets the members run until they again hold nothing (the command encoded, signed and written,
//! or reported not sent), yields to the runtime and
//! polls every member once (the between-command poll: frames the kernel has delivered are read
//! and handed to the handlers), and only then submits the next. A command a handler submits
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
//! protects, 0058). A ladder command never submitted, because its session had ended or the run
//! ended before its turn, or submitted and withdrawn because the run ended before its session
//! took it, is handed back to the consumer ([`Decide::unsubmitted`]) and counted
//! ([`ShardHost::unsubmitted`]); nothing of it reached a codec.

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
        let execs = members.iter().filter_map(|m| m.orders.clone()).collect();
        let runs = members.into_iter().map(|m| Some(m.run)).collect();
        let mut running = Running { runs, execs };
        // The next ingest sequence as the last pass began: an input stamped since calls for one.
        let mut decided = self.clock.next_seq();
        loop {
            let clock = &self.clock;
            if !running.until(|_| clock.next_seq() != decided).await? {
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
            // What a handler submitted, on any order-entry session of the shard, is taken first
            // (Codex P1 r4309119894 on PR #164).
            let quiet = |execs: &[ExecOrders]| execs.iter().all(ExecOrders::idle) && orders.idle();
            let idle = running.until(quiet).await;
            if !matches!(idle, Ok(true)) {
                ladder.planned.push_front(planned);
                return idle;
            }
            if orders.ended() {
                self.hand_back(decide, planned);
                continue;
            }
            let Some(sent) = planned.submit() else {
                continue;
            };
            let taken = running.until(quiet).await;
            if !matches!(taken, Ok(true)) {
                // The run ends before its session took it: it is taken back, nothing of it sent
                // (Codex P1 r4309119897 on PR #164).
                let item = orders.withdraw(sent.rpc);
                let command = item.and_then(|item| match item {
                    Submitted::Authorized(auth) => Some(Unsubmitted::Authorized(auth)),
                    Submitted::Control(_) => sent.control.map(Unsubmitted::Control),
                });
                if let Some(command) = command {
                    self.unsubmitted += 1;
                    decide.unsubmitted(command);
                }
                return taken;
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
        let mut ladder = Ladder::default();
        decide.decide(&instruments, &mut ladder);
        self.passes += 1;
        ladder
    }
}

/// The members still running, and the orders of every order-entry member.
struct Running<'a> {
    runs: Vec<Option<Run<'a>>>,
    execs: Vec<ExecOrders>,
}

impl Running<'_> {
    /// Polls every member still running once: false once none is, or the first error.
    fn poll_all(&mut self, cx: &mut Context<'_>) -> Result<bool, ShardError> {
        for slot in &mut self.runs {
            if let Some(run) = slot
                && let Poll::Ready(ended) = run.as_mut().poll(cx)
            {
                *slot = None;
                ended?;
            }
        }
        Ok(self.runs.iter().any(Option::is_some))
    }

    /// Polls the members until `done`, given the order-entry members' orders, holds after a
    /// round: true then, false once every member has ended, or the first error.
    async fn until(
        &mut self,
        mut done: impl FnMut(&[ExecOrders]) -> bool,
    ) -> Result<bool, ShardError> {
        poll_fn(|cx| match self.poll_all(cx) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(false) => Poll::Ready(Ok(false)),
            Ok(true) if done(&self.execs) => Poll::Ready(Ok(true)),
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
