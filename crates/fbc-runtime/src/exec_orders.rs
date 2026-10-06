//! What an order-entry session takes commands through, and the deadlines of the requests it
//! wrote (FBC-0ga, decision 0057).
//!
//! [`ExecOrders`] is the only way a command reaches an [`ExecSession`](crate::ExecSession)'s
//! codec: an order-affecting one (a place, an amend, a batch of places, a cancel, a cancel-many
//! or an instrument cancel-all) only as the [`Authorization`] fbc-oms issued for it, which the
//! submission consumes (0013 rule 2, 0045), and one that affects no order as a
//! [`ControlCommand`]. Neither takes a [`VenueCommand`]
//! (`tests/submit_compile_fail.rs`). A submission is given its [`RpcId`] at once and waits for
//! the session's next turn on its thread, which encodes it and tells the handler what became of
//! it ([`ExecHandler::on_submitted`](crate::ExecHandler::on_submitted)).
//!
//! [`Rpcs`] holds each written request's deadline until the first event that answers it
//! ([`ExecEvent::answers`](fbc_core::ExecEvent::answers)); one still unanswered at its deadline
//! is handed to the codec's `on_rpc_timeout`, once, which reports it `Unknown` (0005, 0014
//! item 3).

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::fmt;
use std::rc::Rc;

use fbc_core::{AccountKey, RpcCall, RpcId, VenueCommand};
use fbc_oms::{Authorization, ControlCommand};
use tokio::sync::Notify;
use tokio::time::Instant;

/// Why [`ExecOrders`] took no command. Nothing of it reached the session.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SubmitRefusal {
    /// The authorization is for another account than the session's.
    OtherAccount,
    /// The session has run, or was dropped: it takes no more commands.
    Ended,
}

impl fmt::Display for SubmitRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SubmitRefusal::OtherAccount => "the authorization is for another account",
            SubmitRefusal::Ended => "the order-entry session has ended",
        })
    }
}

impl std::error::Error for SubmitRefusal {}

/// A command waiting for the session's next turn.
pub(crate) struct Queued {
    pub(crate) rpc: RpcId,
    pub(crate) cmd: VenueCommand,
    /// The epoch whose stream was authenticated when it was submitted, if one was: it is sent
    /// only on that epoch.
    pub(crate) epoch: Option<u32>,
}

/// What [`ExecOrders`] and its session share, on the session's thread.
pub(crate) struct Shared {
    acct: AccountKey,
    /// The last request id given; the first is 1.
    last: Cell<u64>,
    queue: RefCell<VecDeque<Queued>>,
    /// Wakes the session when a command is submitted.
    pub(crate) wake: Notify,
    /// The epoch whose stream the codec reported authenticated, while it lasts.
    ready: Cell<Option<u32>>,
    ended: Cell<bool>,
}

impl Shared {
    pub(crate) fn new(acct: AccountKey) -> Rc<Shared> {
        Rc::new(Shared {
            acct,
            last: Cell::new(0),
            queue: RefCell::default(),
            wake: Notify::new(),
            ready: Cell::new(None),
            ended: Cell::new(false),
        })
    }

    /// Gives `cmd` the next request id and queues it for the session's next turn.
    fn push(&self, cmd: VenueCommand) -> Result<RpcId, SubmitRefusal> {
        if self.ended.get() {
            return Err(SubmitRefusal::Ended);
        }
        let rpc = self
            .last
            .get()
            .checked_add(1)
            .expect("a session gives fewer than u64::MAX request ids");
        self.last.set(rpc);
        let rpc = RpcId(rpc);
        let epoch = self.ready.get();
        self.queue
            .borrow_mut()
            .push_back(Queued { rpc, cmd, epoch });
        self.wake.notify_one();
        Ok(rpc)
    }

    /// The command submitted first of those waiting.
    pub(crate) fn pop(&self) -> Option<Queued> {
        self.queue.borrow_mut().pop_front()
    }

    /// Whether a command waits.
    pub(crate) fn waiting(&self) -> bool {
        !self.queue.borrow().is_empty()
    }

    /// The epoch whose stream is authenticated, if one is.
    pub(crate) fn ready(&self) -> Option<u32> {
        self.ready.get()
    }

    pub(crate) fn set_ready(&self, epoch: Option<u32>) {
        self.ready.set(epoch);
    }

    /// The session has run or dropped: no more commands, and none still waiting is sent.
    pub(crate) fn end(&self) {
        self.ended.set(true);
        self.ready.set(None);
        self.queue.borrow_mut().clear();
    }
}

/// Submits commands to one account's order-entry session ([`ExecSession::orders`]
/// (crate::ExecSession::orders)), on the thread that drives it. Each submission is given its
/// request id at once; the session encodes it on its next turn, with an
/// [`EncodeCtx`](fbc_core::EncodeCtx) holding one nonce per item reserved from the consumer's
/// source, and tells the handler what became of it
/// ([`ExecHandler::on_submitted`](crate::ExecHandler::on_submitted)): sent, with the nonces
/// it used, or not sent and why. A sent request is never written again: one the venue leaves
/// unanswered is reported `Unknown` at its deadline (decision 0057).
#[derive(Clone)]
pub struct ExecOrders {
    pub(crate) shared: Rc<Shared>,
}

impl fmt::Debug for ExecOrders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecOrders")
            .field("acct", &self.shared.acct)
            .finish_non_exhaustive()
    }
}

impl ExecOrders {
    /// The account the session trades.
    pub fn account(&self) -> AccountKey {
        self.shared.acct
    }

    /// Submits the order-affecting command fbc-oms authorized, spending the authorization: its
    /// request id, or why nothing was submitted (an authorization for another account, or a
    /// session that has ended).
    pub fn submit(&self, auth: Authorization) -> Result<RpcId, SubmitRefusal> {
        if auth.account() != self.shared.acct {
            return Err(SubmitRefusal::OtherAccount);
        }
        self.shared.push(auth.command().clone())
    }

    /// Submits a command that affects no order: its request id, or [`SubmitRefusal::Ended`].
    pub fn submit_control(&self, cmd: ControlCommand) -> Result<RpcId, SubmitRefusal> {
        self.shared.push(cmd.into_command())
    }
}

/// The deadlines of the requests a session wrote, each until the first event that answers it.
#[derive(Default)]
pub(crate) struct Rpcs {
    due: BinaryHeap<Reverse<(Instant, RpcId)>>,
    /// The requests written and neither answered nor timed out.
    live: HashSet<RpcId>,
}

impl Rpcs {
    /// Request `call` is written at `now`: its deadline is `call.timeout` from then. A request
    /// already waiting keeps the deadline of its first frame; one whose deadline is past the end
    /// of the clock never falls due.
    pub(crate) fn sent(&mut self, call: RpcCall, now: Instant) {
        if self.live.insert(call.id)
            && let Some(at) = now.checked_add(call.timeout)
        {
            self.due.push(Reverse((at, call.id)));
        }
    }

    /// Request `rpc` was answered: it reaches no deadline.
    pub(crate) fn answered(&mut self, rpc: RpcId) {
        self.live.remove(&rpc);
    }

    /// When the earliest deadline falls due; it may be an answered request's, which falls due
    /// into nothing.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.due.peek().map(|Reverse((at, _))| *at)
    }

    /// The requests still unanswered whose deadlines fell due by `now`, in deadline order, each
    /// given once.
    pub(crate) fn take_due(&mut self, now: Instant) -> Vec<RpcId> {
        let mut due = Vec::new();
        while let Some(Reverse((at, rpc))) = self.due.peek().copied()
            && at <= now
        {
            self.due.pop();
            if self.live.remove(&rpc) {
                due.push(rpc);
            }
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn call(id: u64, ms: u64) -> RpcCall {
        RpcCall {
            id: RpcId(id),
            timeout: Duration::from_millis(ms),
        }
    }

    #[test]
    fn a_deadline_falls_due_once_unless_answered_first() {
        let now = Instant::now();
        let ms = Duration::from_millis;
        let mut rpcs = Rpcs::default();
        rpcs.sent(call(1, 20), now);
        rpcs.sent(call(2, 10), now);
        // A second frame of request 1 keeps its first deadline.
        rpcs.sent(call(1, 5), now);
        rpcs.sent(call(3, 30), now);
        rpcs.answered(RpcId(3));
        assert_eq!(rpcs.next_deadline(), Some(now + ms(10)));
        assert!(rpcs.take_due(now + ms(9)).is_empty());
        assert_eq!(rpcs.take_due(now + ms(25)), [RpcId(2), RpcId(1)]);
        // The answered one falls due into nothing, and nothing falls due twice.
        assert_eq!(rpcs.take_due(now + ms(60)), []);
        assert_eq!(rpcs.next_deadline(), None);
    }

    #[test]
    fn a_deadline_past_the_end_of_the_clock_never_falls_due() {
        let mut rpcs = Rpcs::default();
        let never = RpcCall {
            id: RpcId(1),
            timeout: Duration::MAX,
        };
        rpcs.sent(never, Instant::now());
        assert_eq!(rpcs.next_deadline(), None);
    }

    #[test]
    fn a_refusal_reads_as_its_cause() {
        assert_eq!(
            SubmitRefusal::OtherAccount.to_string(),
            "the authorization is for another account"
        );
        assert_eq!(
            SubmitRefusal::Ended.to_string(),
            "the order-entry session has ended"
        );
    }

    #[test]
    fn orders_give_increasing_request_ids_and_none_once_ended() {
        let shared = Shared::new(AccountKey::new(2));
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        assert_eq!(orders.account(), AccountKey::new(2));
        assert!(format!("{orders:?}").starts_with("ExecOrders"));
        shared.set_ready(Some(3));
        let first = orders.submit_control(ControlCommand::FeeQuery);
        let second = orders.submit_control(ControlCommand::RefreshDeadMan);
        assert_eq!((first, second), (Ok(RpcId(1)), Ok(RpcId(2))));
        assert!(shared.waiting());
        let queued = shared.pop().unwrap();
        assert_eq!((queued.rpc, queued.epoch), (RpcId(1), Some(3)));
        assert_eq!(queued.cmd, VenueCommand::FeeQuery);
        shared.end();
        assert!(!shared.waiting());
        assert_eq!(shared.ready(), None);
        assert_eq!(
            orders.submit_control(ControlCommand::FeeQuery),
            Err(SubmitRefusal::Ended)
        );
    }
}
