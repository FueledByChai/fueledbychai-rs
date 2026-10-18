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
//! item 3). A request asked as an HTTP request is also cleared by the call to the codec's
//! `on_http` for it, and its deadline stands only once the epoch that asked has ended, since
//! until then its result always comes back to `on_http` (FBC-m8vm, decision 0081).

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{AccountKey, InstrumentId, RpcCall, RpcId, Stamp, VenueCommand, VenueOrderSnapshot};
use fbc_oms::{Authorization, ControlCommand};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::exec_gate::Gate;
use crate::session_core::IngestClock;

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

/// What was submitted: the authorization fbc-oms issued for an order-affecting command, kept
/// until the command is encoded so fbc-oms's submit-time re-check (decision 0060) runs then, in
/// `ExecSession::send` just before the encode, not at queue time (decisions 0057, 0062; PR #87
/// Reviewer B B7), or a command that affects no order.
pub(crate) enum Submitted {
    Authorized(Authorization),
    Control(VenueCommand),
}

impl Submitted {
    /// The command to encode.
    pub(crate) fn command(&self) -> &VenueCommand {
        match self {
            Submitted::Authorized(auth) => auth.command(),
            Submitted::Control(cmd) => cmd,
        }
    }
}

/// A command waiting for the session's next turn.
pub(crate) struct Queued {
    pub(crate) rpc: RpcId,
    pub(crate) item: Submitted,
    /// The epoch whose stream was authenticated when it was submitted, if one was: it is sent
    /// only on that epoch.
    pub(crate) epoch: Option<u32>,
    /// The input being handled on the shard when it was submitted, if one was, which its write
    /// is attributed to (decision 0100).
    pub(crate) origin: Option<Stamp>,
}

/// The request ids one account's order-entry sessions give, counted up across them: the
/// consumer builds one per account and hands a clone to each session it builds for the account
/// ([`ExecSessionConfig::rpc_ids`](crate::ExecSessionConfig::rpc_ids)), so an answer or an
/// `Unknown` for one session's request is never matched to an earlier session's request whose
/// fate fbc-oms still waits on (decision 0057; PR #87 Reviewer B B2).
#[derive(Clone, Debug, Default)]
pub struct RpcIds {
    /// The last id given; the first is one past the counter's start.
    last: Arc<AtomicU64>,
}

impl RpcIds {
    /// Ids from one past `last` on: for a consumer that resumes the account's ids from where its
    /// journal left them. [`RpcIds::default`] gives ids from 1.
    pub fn after(last: RpcId) -> RpcIds {
        RpcIds {
            last: Arc::new(AtomicU64::new(last.0)),
        }
    }

    /// The next id.
    fn next(&self) -> RpcId {
        let give = |last: u64| last.checked_add(1);
        let last = self
            .last
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, give);
        RpcId(last.expect("an account gives fewer than u64::MAX request ids") + 1)
    }
}

/// What [`ExecOrders`] and its session share, on the session's thread.
pub(crate) struct Shared {
    acct: AccountKey,
    /// Where request ids are given from.
    ids: RpcIds,
    queue: RefCell<VecDeque<Queued>>,
    /// What still waited when the session ended, never to be sent: kept so the shard host can
    /// take back a ladder command of its own (decision 0100).
    left: RefCell<Vec<Queued>>,
    /// Wakes the session when a command is submitted.
    pub(crate) wake: Notify,
    /// The epoch whose stream the codec reported authenticated, while it lasts.
    ready: Cell<Option<u32>>,
    /// Whether that epoch takes places and amends yet: its cancel-on-disconnect arm and its
    /// resync (FBC-w19, decision 0058).
    pub(crate) gate: RefCell<Gate>,
    ended: Cell<bool>,
    /// Whether the session is taking the commands of a turn: one popped may still be encoding or
    /// writing (decision 0100).
    sending: Cell<bool>,
    /// The shard clock, whose input being handled a submission is attributed to.
    clock: IngestClock,
}

impl Shared {
    /// What the session for `acct` shares with its orders, its request ids from `ids`, its
    /// venue's protection per connection, re-armed on each when `rearm` and covering the
    /// orders already open when `covers_open`, its venue's amends keeping the venue id (or
    /// none taken) when `one_id`, each submission attributed to the input `clock` says is being
    /// handled.
    pub(crate) fn new(
        acct: AccountKey,
        ids: RpcIds,
        rearm: bool,
        covers_open: bool,
        one_id: bool,
        clock: IngestClock,
    ) -> Rc<Shared> {
        Rc::new(Shared {
            acct,
            ids,
            queue: RefCell::default(),
            left: RefCell::default(),
            wake: Notify::new(),
            ready: Cell::new(None),
            gate: RefCell::new(Gate::new(rearm, covers_open, one_id)),
            ended: Cell::new(false),
            sending: Cell::new(false),
            clock,
        })
    }

    /// The next request id, for a request the session sends of its own (its arm).
    pub(crate) fn next_rpc(&self) -> RpcId {
        self.ids.next()
    }

    /// Gives `item` the next request id and queues it for the session's next turn.
    fn push(&self, item: Submitted) -> Result<RpcId, SubmitRefusal> {
        if self.ended.get() {
            return Err(SubmitRefusal::Ended);
        }
        let rpc = self.ids.next();
        let epoch = self.ready.get();
        let origin = self.clock.handling();
        let queued = Queued {
            rpc,
            item,
            epoch,
            origin,
        };
        self.queue.borrow_mut().push_back(queued);
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

    /// One turn per command waiting now: a turn takes only those, so what is submitted while
    /// it runs waits for the next.
    pub(crate) fn waiting_now(&self) -> std::iter::RepeatN<()> {
        std::iter::repeat_n((), self.queue.borrow().len())
    }

    /// The session is taking a turn's commands (`true`), or has finished it.
    pub(crate) fn set_sending(&self, sending: bool) {
        self.sending.set(sending);
    }

    /// Whether the session holds no command: none waits, and none popped is still encoding or
    /// writing; or it has ended.
    pub(crate) fn idle(&self) -> bool {
        self.ended.get() || (!self.sending.get() && !self.waiting())
    }

    /// Takes back the command submitted as request `rpc` while it still waits for the session's
    /// turn, or was left waiting when the session ended: nothing of it reached the codec
    /// (decision 0100; Codex P1 r4309207253 on PR #164).
    pub(crate) fn withdraw(&self, rpc: RpcId) -> Option<Submitted> {
        let mut queue = self.queue.borrow_mut();
        if let Some(at) = queue.iter().position(|queued| queued.rpc == rpc) {
            return queue.remove(at).map(|queued| queued.item);
        }
        let mut left = self.left.borrow_mut();
        let at = left.iter().position(|queued| queued.rpc == rpc)?;
        Some(left.swap_remove(at).item)
    }

    /// The shard clock, which names the input whose events are being handed on.
    pub(crate) fn clock(&self) -> &IngestClock {
        &self.clock
    }

    /// The epoch whose stream is authenticated, if one is.
    pub(crate) fn ready(&self) -> Option<u32> {
        self.ready.get()
    }

    pub(crate) fn set_ready(&self, epoch: Option<u32>) {
        self.ready.set(epoch);
    }

    /// The session has run or dropped: no more commands, and none still waiting is sent; those
    /// are kept only for [`Shared::withdraw`].
    pub(crate) fn end(&self) {
        self.ended.set(true);
        self.ready.set(None);
        let unsent = std::mem::take(&mut *self.queue.borrow_mut());
        self.left.borrow_mut().extend(unsent);
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

    /// Whether the session holds no command submitted through it (decision 0100).
    pub(crate) fn idle(&self) -> bool {
        self.shared.idle()
    }

    /// Whether the session has run or dropped, taking no more commands.
    pub(crate) fn ended(&self) -> bool {
        self.shared.ended.get()
    }

    /// Takes back request `rpc` while it still waits for the session's turn ([`Shared::withdraw`]).
    pub(crate) fn withdraw(&self, rpc: RpcId) -> Option<Submitted> {
        self.shared.withdraw(rpc)
    }

    /// Submits the order-affecting command fbc-oms authorized, spending the authorization: its
    /// request id, or why nothing was submitted (an authorization for another account, or a
    /// session that has ended). The session keeps the authorization until it encodes the
    /// command, where fbc-oms's submit-time re-check runs: one it refuses is reported
    /// `NotSent(StaleAuthorization)`, nothing reserved or written (decisions 0057, 0060, 0062).
    pub fn submit(&self, auth: Authorization) -> Result<RpcId, SubmitRefusal> {
        if auth.account() != self.shared.acct {
            return Err(SubmitRefusal::OtherAccount);
        }
        self.shared.push(Submitted::Authorized(auth))
    }

    /// Submits a command that affects no order: its request id, or [`SubmitRefusal::Ended`].
    pub fn submit_control(&self, cmd: ControlCommand) -> Result<RpcId, SubmitRefusal> {
        self.shared.push(Submitted::Control(cmd.into_command()))
    }

    /// Whether the session's current epoch takes places and amends (FBC-w19, decision 0058):
    /// the codec reported its stream authenticated, the venue finally accepted the
    /// cancel-on-disconnect arm the session sent then (or, where the protection outlives a
    /// connection, one on an earlier epoch), and the resync the session asked for then has
    /// pushed its `ResyncEnd` to the handler. It is true already while the handler is handed the
    /// last of those two, and a place submitted then goes out once the handler returns. Until then a place, a batch of places or an amend is
    /// `NotSent(Disconnected)`, with no nonce reserved and nothing written; a cancel, a
    /// cancel-many, an instrument cancel-all and a control command are not held.
    pub fn may_place(&self) -> bool {
        let gate = self.shared.gate.borrow();
        self.shared.ready().is_some_and(|epoch| gate.placing(epoch))
    }

    /// Whether the session's current epoch takes places and amends on `inst`: it
    /// [may place](ExecOrders::may_place), and no order of ours on `inst` that its resync
    /// showed resting rests still unprotected (FBC-nvxn, decision 0080). Until then a place, a
    /// batch with an item on `inst`, or an amend on `inst` is `NotSent(Disconnected)`, counted
    /// ([`ExecCounters::unprotected_refusals`](crate::ExecCounters::unprotected_refusals)).
    pub fn may_place_on(&self, inst: InstrumentId) -> bool {
        self.may_place() && !self.shared.gate.borrow().unprotected_on(inst)
    }

    /// The orders of ours this or an earlier epoch's resync showed resting that rest still
    /// unprotected: placed on an earlier epoch, so the epoch's cancel-on-disconnect arm may not
    /// cover them, on a venue that does not declare that it does (`covers_open_orders`). Each
    /// holds its market until an order event or an order query's answer shows it ended; the
    /// consumer cancels them through fbc-oms ([`ExecHandler::on_unprotected`]
    /// (crate::ExecHandler::on_unprotected)). Empty before the resync has ended.
    pub fn unprotected(&self) -> Vec<VenueOrderSnapshot> {
        let gate = self.shared.gate.borrow();
        let ready = self.shared.ready().is_some_and(|epoch| gate.placing(epoch));
        if ready {
            gate.unprotected()
        } else {
            Vec::new()
        }
    }
}

/// The deadlines of the requests a session wrote, each until the first event that answers it.
#[derive(Default)]
pub(crate) struct Rpcs {
    due: BinaryHeap<Reverse<(Instant, RpcId)>>,
    /// The requests written and neither answered nor timed out.
    live: HashSet<RpcId>,
    /// The requests asked as HTTP requests whose epoch has not ended: the epoch that asked and
    /// the deadline, which stands once that epoch ends (decision 0081).
    held: HashMap<RpcId, (u32, Option<Instant>)>,
}

impl Rpcs {
    /// Request `call` is written at `now`: its deadline is `call.timeout` from then. A request
    /// already waiting keeps the deadline of its first frame; one whose deadline is past the end
    /// of the clock never falls due, which is why the session refuses to send one (PR #87
    /// Reviewer B B9).
    pub(crate) fn sent(&mut self, call: RpcCall, now: Instant) {
        if self.live.insert(call.id)
            && let Some(at) = now.checked_add(call.timeout)
        {
            self.due.push(Reverse((at, call.id)));
        }
    }

    /// Request `call` is asked as an HTTP request of epoch `epoch` at `now`: its deadline is
    /// `call.timeout` from then, standing only once that epoch has ended ([`Self::ended`]),
    /// since until then its result, or the failure that stands for one, always reaches the
    /// codec's `on_http` (decision 0081). A request already waiting keeps its first deadline.
    pub(crate) fn asked(&mut self, call: RpcCall, epoch: u32, now: Instant) {
        if self.live.insert(call.id) {
            let at = now.checked_add(call.timeout);
            self.held.insert(call.id, (epoch, at));
        }
    }

    /// Epoch `epoch` ended: the deadlines of the HTTP requests it asked stand, since a result
    /// coming back now is dropped and never reaches the codec.
    pub(crate) fn ended(&mut self, epoch: u32) {
        let ended: Vec<RpcId> = self
            .held
            .iter()
            .filter(|(_, (asked, _))| *asked == epoch)
            .map(|(rpc, _)| *rpc)
            .collect();
        for rpc in ended {
            if let Some((_, Some(at))) = self.held.remove(&rpc) {
                self.due.push(Reverse((at, rpc)));
            }
        }
    }

    /// Request `rpc` was answered, or its HTTP result handed to the codec: it reaches no
    /// deadline.
    pub(crate) fn answered(&mut self, rpc: RpcId) {
        self.live.remove(&rpc);
        self.held.remove(&rpc);
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
    fn an_http_requests_deadline_stands_only_once_its_epoch_ended_and_never_once_handed_over() {
        let now = Instant::now();
        let ms = Duration::from_millis;
        let mut rpcs = Rpcs::default();
        // Asked on epoch 0: two still out, one handed over, one of epoch 1.
        rpcs.asked(call(1, 10), 0, now);
        rpcs.asked(call(2, 20), 0, now);
        rpcs.asked(call(3, 10), 0, now);
        rpcs.asked(call(4, 10), 1, now);
        // A second ask of request 1 keeps its first deadline; a frame of it sets none.
        rpcs.asked(call(1, 5), 0, now);
        rpcs.sent(call(1, 5), now);
        rpcs.answered(RpcId(3));
        // While epoch 0 lasts, nothing falls due.
        assert_eq!(rpcs.next_deadline(), None);
        assert!(rpcs.take_due(now + ms(60)).is_empty());
        rpcs.ended(0);
        assert_eq!(rpcs.next_deadline(), Some(now + ms(10)));
        assert_eq!(rpcs.take_due(now + ms(25)), [RpcId(1), RpcId(2)]);
        // Epoch 1's still waits, and one whose deadline the clock cannot hold never falls due.
        let never = RpcCall {
            id: RpcId(5),
            timeout: Duration::MAX,
        };
        rpcs.asked(never, 1, now);
        assert!(rpcs.take_due(now + ms(60)).is_empty());
        rpcs.ended(1);
        assert_eq!(rpcs.take_due(now + ms(60)), [RpcId(4)]);
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
    fn request_ids_count_up_across_the_sessions_sharing_them_from_where_they_start() {
        let ids = RpcIds::default();
        let (first, second) = (
            Shared::new(
                AccountKey::new(2),
                ids.clone(),
                true,
                false,
                true,
                IngestClock::new(),
            ),
            Shared::new(
                AccountKey::new(2),
                ids,
                true,
                false,
                true,
                IngestClock::new(),
            ),
        );
        let fee = || Submitted::Control(VenueCommand::FeeQuery);
        assert_eq!(first.push(fee()), Ok(RpcId(1)));
        assert_eq!(second.push(fee()), Ok(RpcId(2)));
        assert_eq!(RpcIds::after(RpcId(41)).next(), RpcId(42));
        assert!(format!("{:?}", RpcIds::default()).starts_with("RpcIds"));
    }

    /// Decision 0100: a submission is attributed to the input the shard is handling as it is
    /// submitted, on whichever session that input came, and to none once it was handled; the
    /// session holds a command from its submission until its turn ends.
    #[test]
    fn a_submission_is_attributed_to_the_input_being_handled_and_held_until_its_turn_ends() {
        use fbc_core::ConnKey;
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
        assert!(orders.idle());
        // A frame of another session of the shard, being handled.
        let frame = clock.stamp(ConnKey { conn: 9, epoch: 1 }, None);
        clock.attributing(frame, || {
            orders.submit_control(ControlCommand::FeeQuery).unwrap()
        });
        assert!(!orders.idle());
        // Once its events were handed on, a submission names no input.
        orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let (first, second) = (shared.pop().unwrap(), shared.pop().unwrap());
        assert_eq!((first.origin, second.origin), (Some(frame), None));
        // Popped, but its turn still going: held until the turn ends.
        shared.set_sending(true);
        assert!(!orders.idle());
        shared.set_sending(false);
        assert!(orders.idle());
        // An ended session holds nothing.
        shared.set_sending(true);
        shared.end();
        assert!(orders.idle());
    }

    /// A command still waiting, or left waiting when the session ended, can be taken back by
    /// its request id, once; one the session took cannot (decision 0100).
    #[test]
    fn a_waiting_command_is_withdrawn_by_its_request_id_before_and_after_the_session_ends() {
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            IngestClock::new(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let fee = |orders: &ExecOrders| orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let (taken, waiting, left) = (fee(&orders), fee(&orders), fee(&orders));
        assert_eq!(shared.pop().map(|q| q.rpc), Some(taken));
        assert!(orders.withdraw(taken).is_none());
        let back = orders.withdraw(waiting).map(|item| item.command().clone());
        assert_eq!(back, Some(VenueCommand::FeeQuery));
        assert!(orders.withdraw(waiting).is_none());
        shared.end();
        assert!(!shared.waiting() && orders.ended());
        assert!(orders.withdraw(left).is_some());
        assert!(orders.withdraw(left).is_none());
    }

    #[test]
    #[should_panic(expected = "fewer than u64::MAX request ids")]
    fn request_ids_never_wrap() {
        RpcIds::after(RpcId(u64::MAX)).next();
    }

    #[test]
    fn orders_give_increasing_request_ids_and_none_once_ended() {
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            IngestClock::new(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        assert_eq!(orders.account(), AccountKey::new(2));
        assert!(format!("{orders:?}").starts_with("ExecOrders"));
        // No epoch authenticated: no market takes places, and nothing is unprotected.
        assert!(!orders.may_place_on(InstrumentId::new(1)));
        assert!(orders.unprotected().is_empty());
        shared.set_ready(Some(3));
        let first = orders.submit_control(ControlCommand::FeeQuery);
        let second = orders.submit_control(ControlCommand::RefreshDeadMan);
        assert_eq!((first, second), (Ok(RpcId(1)), Ok(RpcId(2))));
        assert!(shared.waiting());
        let queued = shared.pop().unwrap();
        assert_eq!((queued.rpc, queued.epoch), (RpcId(1), Some(3)));
        assert_eq!(queued.item.command(), &VenueCommand::FeeQuery);
        shared.end();
        assert!(!shared.waiting());
        assert_eq!(shared.ready(), None);
        assert_eq!(
            orders.submit_control(ControlCommand::FeeQuery),
            Err(SubmitRefusal::Ended)
        );
    }
}
