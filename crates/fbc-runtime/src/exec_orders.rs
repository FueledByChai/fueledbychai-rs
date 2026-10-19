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
//! The commands waiting are bounded by the consumer's [`ReservedBound`], a Normal bound plus a
//! reserve for Safety traffic; one submitted past it is refused as it is submitted,
//! [`SubmitRefusal::Backpressure`], nothing of it kept (decision 0104).
//!
//! [`Rpcs`] holds each written request's deadline until the first event that answers it
//! ([`ExecEvent::answers`](fbc_core::ExecEvent::answers)); one still unanswered at its deadline
//! is handed to the codec's `on_rpc_timeout`, once, which reports it `Unknown` (0005, 0014
//! item 3). A request asked as an HTTP request is also cleared by the call to the codec's
//! `on_http` for it, and its deadline stands only once the epoch that asked has ended, since
//! until then its result always comes back to `on_http` (FBC-m8vm, decision 0081).
//! It also holds each such request's place in the consumer's [`InFlight`] table, taken when a
//! submitted command is admitted at its encode and given back when the request is settled, not
//! written, or the session ends (decision 0104).

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{
    AccountKey, ConnKey, InstrumentId, NotSentReason, RpcCall, RpcId, Stamp, TrafficClass,
    VenueCommand, VenueOrderSnapshot,
};
use fbc_journal::{Record, WriteRes};
use fbc_oms::{Authorization, ControlCommand};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::bounds::{InFlight, ReservedBound};
use crate::exec_gate::Gate;
use crate::journal::Journal;
use crate::session_core::IngestClock;

/// Why [`ExecOrders`] took no command. Nothing of it reached the session.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SubmitRefusal {
    /// The authorization is for another account than the session's.
    OtherAccount,
    /// The session has run, or was dropped: it takes no more commands.
    Ended,
    /// Not sent, `NotSent(Backpressure)`: the commands already waiting for the connection fill
    /// the bound its class may use (design §5.4, decision 0104). Nothing of it reached the
    /// session and no request id was given; the refusal is journaled as a write result. The
    /// authorization was dropped unspent: the consumer settles what fbc-oms holds for it as for
    /// any command not sent (a place or batch item by `NotSent(Backpressure)` through
    /// `Registry::on_outcome`, an amend by `Registry::amend_not_submitted`) and plans again.
    Backpressure,
}

impl fmt::Display for SubmitRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SubmitRefusal::OtherAccount => "the authorization is for another account",
            SubmitRefusal::Ended => "the order-entry session has ended",
            SubmitRefusal::Backpressure => {
                "the commands waiting for the connection fill the bound its class may use"
            }
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

    /// The traffic class its frames are charged as (decision 0073) and the session's bounds
    /// admit it under (decision 0104): an authorized order command's own
    /// ([`VenueCommand::traffic_class`]), so cancels and reducing orders may use the safety
    /// reserves; every control command normal traffic, whatever its label. The consumer submits
    /// control commands without authorization or count (the Unknown ladder's order queries, an
    /// arm, a dead-man refresh, the fee query), so they stop at each Normal bound and never
    /// take what cancels and reducing orders need (design §4.10 step 7; Reviewer B RB-e8i-1 on
    /// PR #113). The session's own arm, one per epoch, is charged its label.
    pub(crate) fn class(&self) -> TrafficClass {
        match self {
            Submitted::Authorized(auth) => auth.command().traffic_class(),
            Submitted::Control(_) => TrafficClass::Normal,
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
    /// The bound on the commands waiting, Safety traffic having a reserve beyond its Normal
    /// part (decision 0104).
    bound: ReservedBound,
    /// The submissions refused for want of room in the queue.
    backpressured: Cell<u64>,
    /// Where such a refusal is journaled as a write result, once the consumer sets a journal.
    journal: RefCell<Option<Journal>>,
    /// The connection epoch the session is in, or waits to open: the one a refusal is journaled
    /// under.
    conn: Cell<ConnKey>,
    /// The request the shard host submitted from its ladder and may take back ([`Shared::keep`]).
    kept: Cell<Option<RpcId>>,
    /// That request, never to be sent, when it still waited as the session ended or was popped
    /// and never encoded: kept so the host can take it back (decision 0100). No other command
    /// is kept once the session ends (Codex P2 r4309350878 on PR #164).
    left: RefCell<Option<Queued>>,
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
    /// handled, and the commands waiting bounded by `bound`.
    pub(crate) fn new(
        acct: AccountKey,
        ids: RpcIds,
        rearm: bool,
        covers_open: bool,
        one_id: bool,
        clock: IngestClock,
        bound: ReservedBound,
    ) -> Rc<Shared> {
        Rc::new(Shared {
            acct,
            ids,
            queue: RefCell::default(),
            bound,
            backpressured: Cell::new(0),
            journal: RefCell::new(None),
            conn: Cell::new(ConnKey { conn: 0, epoch: 0 }),
            kept: Cell::new(None),
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

    /// Gives `item` the next request id and queues it for the session's next turn, if the
    /// commands waiting leave room for its class (decision 0104): a Normal one while fewer than
    /// the bound's Normal part wait, a Safety one while fewer than the whole bound do. Otherwise
    /// it is refused, [`SubmitRefusal::Backpressure`], with no request id given and nothing of
    /// it kept (an authorization is dropped unspent), the refusal counted and journaled as a
    /// write result under its class.
    fn push(&self, item: Submitted) -> Result<RpcId, SubmitRefusal> {
        if self.ended.get() {
            return Err(SubmitRefusal::Ended);
        }
        let class = item.class();
        let waiting = u64::try_from(self.queue.borrow().len()).unwrap_or(u64::MAX);
        if !self.bound.admits(waiting, class) {
            self.backpressured.set(self.backpressured.get() + 1);
            self.journal_refusal(class);
            return Err(SubmitRefusal::Backpressure);
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

    /// Journals a submission of `class` refused for want of room as its write result, on the
    /// epoch the session is in or waits to open, at the shard clock's time; it names no request,
    /// since none was given.
    fn journal_refusal(&self, class: TrafficClass) {
        if let Some(journal) = &*self.journal.borrow() {
            let (at, wall) = self.clock.now();
            let record = Record::WriteResult {
                at,
                conn: self.conn.get(),
                rpc: None,
                result: WriteRes::NotSent(NotSentReason::Backpressure),
            };
            journal.record(class, wall, &record);
        }
    }

    /// The journal a refusal is recorded in from now on.
    pub(crate) fn set_journal(&self, journal: Journal) {
        *self.journal.borrow_mut() = Some(journal);
    }

    /// The connection epoch the session is in, or waits to open, from now on.
    pub(crate) fn set_conn(&self, conn: ConnKey) {
        self.conn.set(conn);
    }

    /// The command submitted first of those waiting.
    pub(crate) fn pop(&self) -> Option<Queued> {
        self.queue.borrow_mut().pop_front()
    }

    /// The submissions refused because the commands waiting filled their bound.
    pub(crate) fn backpressured(&self) -> u64 {
        self.backpressured.get()
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
    /// turn, or, when it is the request [kept](Shared::keep), was left waiting when the session
    /// ended or popped and never encoded: nothing of it reached the codec (decision 0100; Codex
    /// P1 r4309207253, r4309350872 on PR #164).
    pub(crate) fn withdraw(&self, rpc: RpcId) -> Option<Submitted> {
        let mut queue = self.queue.borrow_mut();
        if let Some(at) = queue.iter().position(|queued| queued.rpc == rpc) {
            return queue.remove(at).map(|queued| queued.item);
        }
        let mut left = self.left.borrow_mut();
        let found = left.as_ref().is_some_and(|queued| queued.rpc == rpc);
        found
            .then(|| left.take())
            .flatten()
            .map(|queued| queued.item)
    }

    /// Request `rpc` is the shard host's to take back should the session end before encoding it
    /// ([`Shared::withdraw`]); it replaces the request kept before, which the host is done with.
    pub(crate) fn keep(&self, rpc: RpcId) {
        self.kept.set(Some(rpc));
    }

    /// Keeps `queued`, popped and never encoded or reported because the session failed first,
    /// for withdrawal when it is the request [kept](Shared::keep); any other is dropped, as
    /// decision 0057 has it (Codex P1 r4309350872 on PR #164).
    pub(crate) fn unsent(&self, queued: Queued) {
        if self.kept.get() == Some(queued.rpc) {
            *self.left.borrow_mut() = Some(queued);
        }
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

    /// The session has run or dropped, or its run was dropped: no more commands, none still
    /// waiting is sent, and none is in progress; of those waiting only the request
    /// [kept](Shared::keep) is kept, for [`Shared::withdraw`].
    pub(crate) fn end(&self) {
        self.ended.set(true);
        self.ready.set(None);
        self.sending.set(false);
        let unsent = std::mem::take(&mut *self.queue.borrow_mut());
        for queued in unsent {
            self.unsent(queued);
        }
    }
}

/// Submits commands to one account's order-entry session ([`ExecSession::orders`]
/// (crate::ExecSession::orders)), on the thread that drives it. Each submission is given its
/// request id at once; the session encodes it on its next turn, with an
/// [`EncodeCtx`](fbc_core::EncodeCtx) holding one nonce per item reserved from the consumer's
/// source, and tells the handler what became of it
/// ([`ExecHandler::on_submitted`](crate::ExecHandler::on_submitted)): sent, with the nonces
/// it used, or not sent and why. A sent request is never written again: one the venue leaves
/// unanswered is reported `Unknown` at its deadline (decision 0057). A submission the commands
/// already waiting leave no room for, under the consumer's outbound bound for its class, is
/// refused at once, [`SubmitRefusal::Backpressure`], nothing of it kept (decision 0104).
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

    /// Keeps request `rpc` for withdrawal ([`Shared::keep`]).
    pub(crate) fn keep(&self, rpc: RpcId) {
        self.shared.keep(rpc);
    }

    /// The session's run was dropped, so it never sends what waits ([`Shared::end`]).
    pub(crate) fn end(&self) {
        self.shared.end();
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

/// The deadlines of the requests a session wrote, each until the first event that answers it,
/// and the places they hold in the consumer's table of requests in flight (decision 0104).
pub(crate) struct Rpcs {
    due: BinaryHeap<Reverse<(Instant, RpcId)>>,
    /// The requests written and neither answered nor timed out.
    live: HashSet<RpcId>,
    /// The requests asked as HTTP requests whose epoch has not ended: the epoch that asked and
    /// the deadline, which stands once that epoch ends (decision 0081).
    held: HashMap<RpcId, (u32, Option<Instant>)>,
    /// The table of requests in flight the session counts against.
    table: InFlight,
    /// The requests holding a place in it: each live one, and one admitted and not yet written.
    places: HashSet<RpcId>,
}

impl Rpcs {
    /// No request yet, the places counted in `table`.
    pub(crate) fn new(table: InFlight) -> Rpcs {
        Rpcs {
            due: BinaryHeap::new(),
            live: HashSet::new(),
            held: HashMap::new(),
            table,
            places: HashSet::new(),
        }
    }

    /// Admits submitted request `rpc` of `class` into the table of requests in flight, if the
    /// table has room for its class: true when it took a place, which it holds until it is
    /// answered or times out, or, never written, until [`Self::unsent`] (decision 0104).
    pub(crate) fn admit(&mut self, rpc: RpcId, class: TrafficClass) -> bool {
        let taken = self.table.try_take(class);
        if taken {
            self.places.insert(rpc);
        }
        taken
    }

    /// Request `rpc`, admitted, may have gone unwritten: unless it is live, its place is given
    /// back.
    pub(crate) fn unsent(&mut self, rpc: RpcId) {
        if !self.live.contains(&rpc) {
            self.give_back(rpc);
        }
    }

    /// Request `rpc` went live: it holds a place, taken now, past the bound, when it was not
    /// admitted (a request the session writes of its own).
    fn hold(&mut self, rpc: RpcId) {
        if self.places.insert(rpc) {
            self.table.take();
        }
    }

    /// Gives back the place request `rpc` holds, if it holds one.
    fn give_back(&mut self, rpc: RpcId) {
        if self.places.remove(&rpc) {
            self.table.release();
        }
    }

    /// The session has ended: no request it wrote is reported any more, and every place one
    /// holds is given back.
    pub(crate) fn clear(&mut self) {
        self.due.clear();
        self.live.clear();
        self.held.clear();
        for _ in self.places.drain() {
            self.table.release();
        }
    }

    /// Request `call` is written at `now`: its deadline is `call.timeout` from then. A request
    /// already waiting keeps the deadline of its first frame; one whose deadline is past the end
    /// of the clock never falls due, which is why the session refuses to send one (PR #87
    /// Reviewer B B9).
    pub(crate) fn sent(&mut self, call: RpcCall, now: Instant) {
        if self.live.insert(call.id) {
            self.hold(call.id);
            if let Some(at) = now.checked_add(call.timeout) {
                self.due.push(Reverse((at, call.id)));
            }
        }
    }

    /// Request `call` is asked as an HTTP request of epoch `epoch` at `now`: its deadline is
    /// `call.timeout` from then, standing only once that epoch has ended ([`Self::ended`]),
    /// since until then its result, or the failure that stands for one, always reaches the
    /// codec's `on_http` (decision 0081). A request already waiting keeps its first deadline.
    pub(crate) fn asked(&mut self, call: RpcCall, epoch: u32, now: Instant) {
        if self.live.insert(call.id) {
            self.hold(call.id);
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
        self.give_back(rpc);
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
                self.give_back(rpc);
                due.push(rpc);
            }
        }
        due
    }
}

/// A session dropped, its run dropped with it, gives back the places its requests held.
impl Drop for Rpcs {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A table no test here fills.
    fn table() -> InFlight {
        InFlight::new(ReservedBound::new(64, 64).unwrap())
    }

    /// A queue no test here fills.
    fn roomy() -> ReservedBound {
        ReservedBound::new(64, 64).unwrap()
    }

    /// The command that waited first.
    fn queued(waiting: Option<Queued>) -> Queued {
        waiting.expect("a command waits")
    }

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
        let mut rpcs = Rpcs::new(table());
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
        let mut rpcs = Rpcs::new(table());
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
        let mut rpcs = Rpcs::new(table());
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
                roomy(),
            ),
            Shared::new(
                AccountKey::new(2),
                ids,
                true,
                false,
                true,
                IngestClock::new(),
                roomy(),
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
            roomy(),
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
        let (first, second) = (queued(shared.pop()), queued(shared.pop()));
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
            roomy(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let fee = |orders: &ExecOrders| orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let (taken, waiting, left) = (fee(&orders), fee(&orders), fee(&orders));
        assert_eq!(queued(shared.pop()).rpc, taken);
        assert!(orders.withdraw(taken).is_none());
        let back = orders.withdraw(waiting).map(|item| item.command().clone());
        assert_eq!(back, Some(VenueCommand::FeeQuery));
        assert!(orders.withdraw(waiting).is_none());
        orders.keep(left);
        shared.end();
        assert!(!shared.waiting() && orders.ended());
        assert!(orders.withdraw(left).is_some());
        assert!(orders.withdraw(left).is_none());
    }

    /// Codex P2 r4309350878 on PR #164: an ended session keeps only the command the shard host
    /// keeps for withdrawal, and none of the others still waiting; Codex P1 r4309350872: one the
    /// session popped and could not encode is kept as well when it is that command.
    #[test]
    fn an_ended_session_keeps_only_the_command_kept_for_withdrawal_waiting_or_popped_unsent() {
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            IngestClock::new(),
            roomy(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let fee = |orders: &ExecOrders| orders.submit_control(ControlCommand::FeeQuery).unwrap();
        let (first, kept, last) = (fee(&orders), fee(&orders), fee(&orders));
        orders.keep(kept);
        shared.end();
        assert!(orders.withdraw(first).is_none());
        assert!(orders.withdraw(last).is_none());
        assert!(orders.withdraw(kept).is_some());

        // Popped, then not encoded: kept only when it is the command kept.
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            IngestClock::new(),
            roomy(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let (other, kept) = (fee(&orders), fee(&orders));
        orders.keep(kept);
        shared.unsent(queued(shared.pop()));
        shared.unsent(queued(shared.pop()));
        shared.end();
        assert!(orders.withdraw(other).is_none());
        let back = orders.withdraw(kept).map(|item| item.command().clone());
        assert_eq!(back, Some(VenueCommand::FeeQuery));
    }

    /// Decision 0104: past the Normal bound, a Normal submission is refused at once, given no
    /// request id and kept nowhere, and journaled as a write result under the epoch the session
    /// is in; a turn taking a command makes room again.
    #[test]
    fn a_submission_past_the_bound_its_class_may_use_is_refused_and_journaled() {
        use fbc_core::WallNs;
        use fbc_journal::{JournalSink, Recorded};
        #[derive(Default)]
        struct Kept(Vec<(TrafficClass, Record)>);
        impl JournalSink for Kept {
            fn record(&mut self, class: TrafficClass, _: WallNs, record: &Record) -> Recorded {
                self.0.push((class, record.clone()));
                Recorded::Ok
            }
            fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
                Recorded::Ok
            }
        }
        let shared = Shared::new(
            AccountKey::new(2),
            RpcIds::default(),
            true,
            false,
            true,
            IngestClock::new(),
            ReservedBound::new(1, 1).unwrap(),
        );
        let orders = ExecOrders {
            shared: Rc::clone(&shared),
        };
        let fee = || orders.submit_control(ControlCommand::FeeQuery);
        // Refused before a journal is set: counted, nothing recorded.
        let taken = fee().unwrap();
        assert_eq!(fee(), Err(SubmitRefusal::Backpressure));
        let sink = Rc::new(RefCell::new(Kept::default()));
        shared.set_journal(Journal::new(Rc::clone(&sink)));
        let conn = ConnKey { conn: 3, epoch: 2 };
        shared.set_conn(conn);
        // A control command is Normal traffic: the reserve is not its to use.
        assert_eq!(fee(), Err(SubmitRefusal::Backpressure));
        assert_eq!(shared.backpressured(), 2);
        let refused = WriteRes::NotSent(NotSentReason::Backpressure);
        let journaled = |record: &Record| matches!(record, Record::WriteResult { conn: c, rpc: None, result, .. } if *c == conn && *result == refused);
        let records = &sink.borrow().0;
        assert_eq!(records.len(), 1);
        assert!(records[0].0 == TrafficClass::Normal && journaled(&records[0].1));
        // Room again once a turn took the command; the next id follows the last one given.
        assert_eq!(queued(shared.pop()).rpc, taken);
        assert_eq!(fee(), Ok(RpcId(taken.0 + 1)));
        assert_eq!(
            SubmitRefusal::Backpressure.to_string(),
            "the commands waiting for the connection fill the bound its class may use"
        );
    }

    /// Decision 0104: an admitted request holds its place in the table until it is answered,
    /// times out or goes unwritten; a request the session writes of its own takes one past the
    /// bound; an HTTP query holds one too; and the end of the session gives back every place.
    #[test]
    fn a_request_holds_its_place_in_flight_until_it_is_settled_unwritten_or_the_session_ends() {
        let now = Instant::now();
        let ms = Duration::from_millis;
        let table = InFlight::new(ReservedBound::new(1, 1).unwrap());
        let mut rpcs = Rpcs::new(table.clone());
        let (normal, safety) = (TrafficClass::Normal, TrafficClass::Safety);
        assert!(rpcs.admit(RpcId(1), normal));
        assert!(!rpcs.admit(RpcId(2), normal));
        assert!(rpcs.admit(RpcId(3), safety));
        assert!(!rpcs.admit(RpcId(4), safety));
        // Written: its place is the one it took; a second frame takes none.
        rpcs.sent(call(1, 10), now);
        rpcs.sent(call(1, 10), now);
        rpcs.unsent(RpcId(1));
        assert_eq!(table.held(), 2);
        // Never written: given back.
        rpcs.unsent(RpcId(3));
        assert_eq!(table.held(), 1);
        // The session's own request, past the bound.
        rpcs.sent(call(5, 20), now);
        assert_eq!(table.held(), 2);
        // An answer, then a deadline, give theirs back.
        rpcs.answered(RpcId(1));
        assert_eq!(table.held(), 1);
        assert_eq!(rpcs.take_due(now + ms(25)), [RpcId(5)]);
        assert_eq!(table.held(), 0);
        // An HTTP query holds one, and the end gives back every place still held.
        rpcs.asked(call(6, 10), 0, now);
        assert!(rpcs.admit(RpcId(7), safety));
        assert_eq!(table.held(), 2);
        rpcs.clear();
        assert_eq!(table.held(), 0);
        assert_eq!(rpcs.next_deadline(), None);
        // A session dropped gives back its places too.
        let mut dropped = Rpcs::new(table.clone());
        dropped.sent(call(8, 10), now);
        assert_eq!(table.held(), 1);
        drop(dropped);
        assert_eq!(table.held(), 0);
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
            roomy(),
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
        let first = queued(shared.pop());
        assert_eq!((first.rpc, first.epoch), (RpcId(1), Some(3)));
        assert_eq!(first.item.command(), &VenueCommand::FeeQuery);
        shared.end();
        assert!(!shared.waiting());
        assert_eq!(shared.ready(), None);
        assert_eq!(
            orders.submit_control(ControlCommand::FeeQuery),
            Err(SubmitRefusal::Ended)
        );
    }
}
