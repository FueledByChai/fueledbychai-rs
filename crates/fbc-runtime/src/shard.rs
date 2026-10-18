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
//! protects, 0058). A ladder command whose session has ended is refused there `Ended`, as any
//! submission to an ended session is (0057), and counted ([`ShardHost::refused`]).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use fbc_core::InstrumentId;
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
    #[cfg_attr(not(test), allow(dead_code))]
    fn take(&self) -> Vec<InstrumentId> {
        std::mem::take(&mut *self.marked.borrow_mut())
    }
}

/// A command a decide pass planned.
enum Command {
    Authorized(Authorization),
    Control(ControlCommand),
}

/// A planned command and the session it goes to.
struct Planned {
    orders: ExecOrders,
    command: Command,
}

impl Planned {
    /// Submits the command to its session: false when the session refused it (it has ended).
    #[allow(dead_code)]
    fn submit(self) -> bool {
        let submitted = match self.command {
            Command::Authorized(auth) => self.orders.submit(auth),
            Command::Control(cmd) => self.orders.submit_control(cmd),
        };
        submitted.is_ok()
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
        let command = Command::Authorized(auth);
        let orders = orders.clone();
        self.planned.push_back(Planned { orders, command });
        Ok(())
    }

    /// Plans a command that affects no order, for the session `orders` reaches.
    pub fn push_control(&mut self, orders: &ExecOrders, cmd: ControlCommand) {
        let command = Command::Control(cmd);
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
    refused: u64,
}

impl fmt::Debug for ShardHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardHost")
            .field("passes", &self.passes)
            .field("refused", &self.refused)
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
            refused: 0,
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

    /// The ladder commands whose session refused them because it had ended.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// Drives `members` until every one has ended, or one fails. Still to come (FBC-ct0): the
    /// passes, their `Cycle` records and the ladder; this only runs the members.
    pub async fn run<D: Decide + ?Sized>(
        &mut self,
        members: Vec<Member<'_>>,
        decide: &mut D,
    ) -> Result<(), ShardError> {
        let _ = (decide, &self.clock, &self.changed, &self.journal);
        let mut running = Running(members.into_iter().map(|m| Some(m.run)).collect());
        poll_fn(|cx| match running.poll_all(cx) {
            Err(e) => Poll::Ready(Err(e)),
            Ok(false) => Poll::Ready(Ok(())),
            Ok(true) => Poll::Pending,
        })
        .await
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
