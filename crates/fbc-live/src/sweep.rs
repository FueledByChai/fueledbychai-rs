//! The sweep of a market's orders of ours (decision 0155 items 2, 5 and 8): what a consumer runs
//! at Start and after each reconnect before it places on the market, and at Stop after the kill
//! switch, so no order of ours rests there unseen.
//!
//! - **What it sends.** [`Sweep::start`] sends fbc-oms's cancel everything of the market
//!   ([`Registry::cancel_everything`](fbc_oms::Registry::cancel_everything), decision 0012): an
//!   instrument cancel-all under 0005's I7 guard, which needs a trustworthy resync (decision
//!   0094), with the explicit cancels beside it of our orders the venue may not hold yet. When
//!   fbc-oms builds none, or once the link noted a cancel not ours
//!   ([`Link::cancels_not_ours_seen`]: another program may be on the account, and an instrument
//!   cancel-all would cancel its orders too, FBC-aoh5), it sends explicit cancels instead, built
//!   by [`Registry::cancel_many`](fbc_oms::Registry::cancel_many), of each order of ours the link
//!   holds live on the market ([`Link::live_on`], in the order the link learnt them) with no
//!   cancel in flight or awaiting its acknowledgement, or gone Unknown. It never sends an account
//!   cancel-all (decision 0045). Each [`Sweep::poll`] sends the cancels of the market's orders
//!   that waited for their order's acknowledgement once it landed
//!   ([`Registry::cancels_due`](fbc_oms::Registry::cancels_due), each order's market as the
//!   registry records it); another market's stay due in the registry, for the consumer or that
//!   market's own sweep, since a sweep is of one market (decision 0155 item 5). A round that
//!   stops short at a cancel not handed over ([`Round::failed`]) leaves the requests it sent
//!   before to the consumer: the sweep neither waits on nor lists them ([`Sweep::rpcs`]).
//! - **Refused for its rate.** A cancel of ours on the market that the venue refuses for its rate
//!   makes the cancel of everything due again, whatever its order's state and whoever sent it
//!   (the sweep, the consumer, or the link's Safety cancel of an unprotected order): a poll sends
//!   it once the consumer's [`RateHold`] no longer holds (Codex r4306449947 on PR #158), so what
//!   the sweep waits for is sent again rather than waited out while the orders rest. A cancel on
//!   the market is one of the sweep's own, an explicit cancel or cancel-many naming at least one
//!   order of ours on it, or an instrument cancel-all of it. A place's refusal makes nothing due,
//!   nor does a refusal of a cancel naming no order on the market. The sweep reads from the link
//!   which requests are cancels on its market ([`Link::is_cancel_on`]), and the link forgets a
//!   request once the consumer releases it ([`Link::release`]), or at the consumer's next
//!   [`Link::forget`] after the one that took the last note of a request answered in full
//!   (FBC-mu68), so the consumer hands each note to the sweep before it releases the note's
//!   request and before it forgets notes again. The hold is the consumer's: it records each
//!   refusal for its rate it reads, a place's too, which holds for the venue's retry time or,
//!   when the venue states none, for the consumer's wait ([`RateHold::refused`]).
//! - **How it ends** ([`SweepEnd`]). At Start and after a reconnect
//!   ([`SweepEnd::BeforePlaces`]), once every order of ours the link holds live on the market has
//!   ended and the session takes places there ([`ExecOrders::may_place_on`]). A cancel of the
//!   sweep's refused because its order already ended or is not known (`AlreadyTerminal`,
//!   `NotFound`) waits for that end; one refused for any other reason but its rate fails the
//!   sweep ([`SweepState::Failed`]). A refusal of a request not the sweep's fails nothing: the
//!   consumer judges its own. An order fbc-oms puts on the Unknown ladder, as it does on
//!   any `NotFound`, may never end here, since the sweep queries no order (FBC-2cv0). No resync
//!   holds it past the session's resync deadline, which ends the epoch (FBC-nwpr). At Stop
//!   ([`SweepEnd::AtStop`]), once every such order has ended and every request the sweep sent is
//!   answered, `may_place_on` unread: no refusal ends it early, and the consumer judges each
//!   answer ([`Sweep::outcome`]).
//! - **The consumer's part.** While a sweep runs it hands the sweep every note it reads from the
//!   link, and the link with it, before it releases the note's request or forgets notes again
//!   ([`Sweep::observe`]), and polls it at each wake and once the hold has passed
//!   ([`SweepState::Waiting`]). It places nothing on the market before a `BeforePlaces` sweep
//!   ended. The sweep keeps no timeout and reads no clock: each poll takes `now` from its caller,
//!   on the shard's monotonic clock ([`Link::now`], decision 0155 item 3), and the consumer bounds
//!   its wait.
//! - **Stop's order**, which the consumer runs: the kill switch
//!   ([`Registry::kill`](fbc_oms::Registry::kill)), the sweep, the comparison of the venue's
//!   positions with the registry's while the market lease is still held, so no other process
//!   takes the market while the consumer compares against its registry (Codex r4305979062 on PR
//!   #157), and then the disarm ([`Registry::disarm`](fbc_oms::Registry::disarm)), which releases
//!   the market's leases.
//! - **Bounds.** The hold's wait for a venue that states no retry time is the consumer's, refused
//!   zero, with no default (decision 0155 item 8).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::Duration;

use fbc_core::{
    AccountKey, ClientOrderId, InstrumentId, MonoNs, OrderCaps, Reject, RejectKind, RpcId,
    SubmitOutcome,
};
use fbc_oms::{
    CancelAllRefusal, CancelEverything, Intent, IssueRefusal, OrdState, PermittedCommand,
};
use fbc_runtime::ExecOrders;

use crate::link::{Link, Note};

/// The hold a request of ours refused for its rate puts on the consumer: nothing it sends for
/// the rate's sake, the sweep's cancel of everything sent again included, goes before the hold
/// passes. It reads no clock: each call takes `now` from its caller.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RateHold {
    /// How long a refusal holds when the venue states no retry time.
    wait: Duration,
    /// When the latest hold ends, once a refusal was recorded.
    until: Option<MonoNs>,
}

/// A [`RateHold`] refused: its wait for a venue that states no retry time is zero, which would
/// send a refused request again at once.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ZeroWait;

impl fmt::Display for ZeroWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the rate hold's wait when the venue states no retry time is zero")
    }
}

impl std::error::Error for ZeroWait {}

impl RateHold {
    /// A hold of `wait` for each refusal for its rate whose venue states no retry time, the
    /// consumer's (decision 0155 item 8). Refused when it is zero.
    pub fn new(wait: Duration) -> Result<RateHold, ZeroWait> {
        if wait.is_zero() {
            return Err(ZeroWait);
        }
        Ok(RateHold { wait, until: None })
    }

    /// The consumer's wait for a venue that states no retry time.
    pub fn wait(&self) -> Duration {
        self.wait
    }

    /// A request was refused for its rate at `now`: the hold lasts until `retry_after` from then,
    /// the venue's retry time, or the consumer's wait when the venue states none, and never ends
    /// sooner than a hold recorded before it. The time it holds from `now`.
    pub fn refused(&mut self, retry_after: Option<Duration>, now: MonoNs) -> Duration {
        let wait = retry_after.unwrap_or(self.wait);
        let ends = MonoNs(now.0.saturating_add(nanos(wait)));
        self.until = Some(self.until.map_or(ends, |at| at.max(ends)));
        wait
    }

    /// Whether the hold holds at `now`.
    pub fn held(&self, now: MonoNs) -> bool {
        self.until.is_some_and(|at| now < at)
    }

    /// When the latest hold ends, `None` before any refusal was recorded.
    pub fn until(&self) -> Option<MonoNs> {
        self.until
    }
}

/// `d` in nanoseconds, the largest count past `u64`'s.
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// When a sweep ends (module documentation).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SweepEnd {
    /// At Start and after a reconnect: once every order of ours on the market ended and the
    /// session takes places there, a refusal of one of its own cancels other than for its rate,
    /// `AlreadyTerminal` or `NotFound` failing it.
    BeforePlaces,
    /// At Stop: once every order of ours on the market ended and every request the sweep sent
    /// is answered, whatever the answers. Its requests are all on its market, so another
    /// market's cancel, answered or not, never holds it.
    AtStop,
}

/// How a cancel of everything cancelled.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum How {
    /// fbc-oms's instrument cancel-all of the market, with the explicit cancels beside it of our
    /// orders the venue may not hold yet.
    CancelAll,
    /// Explicit cancels of our orders on the market, and why no cancel-all was sent.
    Explicit(Why),
}

/// Why a cancel of everything sent no instrument cancel-all.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Why {
    /// The link noted a cancel not ours ([`Link::cancels_not_ours_seen`]): another program may
    /// be on the account, and an instrument cancel-all would cancel its orders too.
    NotOursSeen,
    /// fbc-oms built none, for this reason.
    Refused(CancelAllRefusal),
}

/// A request the sweep sent: its request id, and the orders it names, in its items' order (none
/// for an instrument cancel-all).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub rpc: RpcId,
    pub names: Vec<ClientOrderId>,
}

/// Why a cancel fbc-oms built was not handed to the session.
#[derive(Debug)]
pub enum SendError {
    /// fbc-oms did not authorize it for the sweep's account.
    Unauthorized(IssueRefusal),
    /// The link did not submit it: the session refused it, or the link models no such command.
    Unsubmitted(String),
}

/// What one cancel of everything sent: the requests, in order, how it cancelled, and why it
/// stopped short when a cancel was not handed over (nothing after it was sent, and the sweep
/// neither waits on nor lists the requests sent before it).
#[derive(Debug)]
pub struct Round {
    pub sent: Vec<Sent>,
    pub how: How,
    pub failed: Option<SendError>,
}

/// Where a sweep stands after a poll.
#[derive(Clone, Debug, PartialEq)]
pub enum SweepState {
    /// It ended ([`SweepEnd`]).
    Ended,
    /// It waits; with the cancel of everything due again, until `resend_at`, when the hold ends.
    Waiting { resend_at: Option<MonoNs> },
    /// Before places, a cancel of the sweep's was refused, neither for its rate nor because its
    /// order already ended or is not known: the first such refusal. Nothing more is sent.
    Failed(Reject),
}

/// What one poll sent, and where the sweep stands.
#[derive(Debug)]
pub struct Polled {
    /// The cancels of the sweep's market's orders that waited for their order's
    /// acknowledgement, sent, in client id order.
    pub due: Vec<Sent>,
    /// Those of them not handed over.
    pub due_failed: Vec<SendError>,
    /// The cancel of everything sent again, once a refusal for its rate made it due and the hold
    /// passed.
    pub resent: Option<Round>,
    pub state: SweepState,
}

/// The answers to one request the sweep sent.
#[derive(Debug)]
struct Answers {
    /// How many items it has (one for an instrument cancel-all).
    items: usize,
    /// The whole request's first answer, a report that it was not sent included.
    whole: Option<SubmitOutcome>,
    /// Each item's first answer.
    by_item: BTreeMap<u16, SubmitOutcome>,
}

impl Answers {
    /// What came of the request once it is answered: the whole request's answer, or, item by
    /// item, the first not accepted, else the first; `None` while an item is unanswered, so a
    /// batch is never judged by its first item alone.
    fn outcome(&self) -> Option<SubmitOutcome> {
        if let Some(whole) = &self.whole {
            return Some(whole.clone());
        }
        if self.by_item.len() < self.items {
            return None;
        }
        let mut answers = self.by_item.values();
        let refused = answers
            .clone()
            .find(|o| !matches!(o, SubmitOutcome::Accepted { .. }));
        refused.or_else(|| answers.next()).cloned()
    }
}

/// One sweep of a market's orders of ours (module documentation).
#[derive(Debug)]
pub struct Sweep {
    inst: InstrumentId,
    acct: AccountKey,
    end: SweepEnd,
    /// The requests it sent, in order, but a round's that stopped short.
    sent: Vec<RpcId>,
    answers: HashMap<RpcId, Answers>,
    /// A cancel on its market was refused for its rate since the last cancel of everything.
    resend_due: bool,
    /// Before places, the first refusal that failed it.
    failed: Option<Reject>,
}

impl Sweep {
    /// Starts the sweep of `inst` that ends as `end` says: the cancel of everything of ours on
    /// it (module documentation), each cancel authorized by the link's registry for `acct` and
    /// submitted through the link to the session `orders` submits to.
    pub fn start(
        link: &mut Link,
        orders: &ExecOrders,
        acct: AccountKey,
        inst: InstrumentId,
        end: SweepEnd,
    ) -> (Sweep, Round) {
        let mut sweep = Sweep {
            inst,
            acct,
            end,
            sent: Vec::new(),
            answers: HashMap::new(),
            resend_due: false,
            failed: None,
        };
        let round = sweep.cancel_everything(link, orders);
        (sweep, round)
    }

    /// The requests the sweep sent, in order, but those of a round that stopped short at a
    /// cancel not handed over ([`Round::failed`]), which it leaves to the consumer.
    pub fn rpcs(&self) -> &[RpcId] {
        &self.sent
    }

    /// What came of request `rpc` of the sweep's once every item is answered: the whole
    /// request's answer (a report that it was not sent included), or, item by item, the first
    /// not accepted, else the first. `None` while an item is unanswered, so a batch is never
    /// judged by its first item alone, or for a request the sweep did not send.
    pub fn outcome(&self, rpc: RpcId) -> Option<SubmitOutcome> {
        self.answers.get(&rpc)?.outcome()
    }

    /// Takes `note`, read from `link`, into the sweep: an answer to one of its requests, or a
    /// report that one was not sent, is kept; a refusal for its rate of one of them, or of any
    /// request the link says is a cancel on the sweep's market ([`Link::is_cancel_on`]: the
    /// consumer's, or a Safety cancel the link sent), makes the cancel of everything due again,
    /// and a place's, or a cancel's naming no order on the market, makes nothing due; and before
    /// places a refusal of one of the sweep's for another reason than that its order already
    /// ended or is not known fails it. The note's request must not be released yet
    /// ([`Link::release`]), nor the link's notes forgotten again since the [`Link::forget`] that
    /// handed the note over: the link then no longer says it is a cancel.
    pub fn observe(&mut self, link: &Link, note: &Note) {
        match note {
            Note::Submitted {
                rpc,
                sent: Err(reason),
            } => {
                if let Some(answers) = self.answers.get_mut(rpc) {
                    answers.whole.get_or_insert(SubmitOutcome::NotSent(*reason));
                }
            }
            Note::Outcome {
                rpc,
                item,
                outcome,
                ours: true,
            } => self.answered(link, *rpc, *item, outcome),
            _ => {}
        }
    }

    /// Request `rpc`, submitted through `link`, was answered `outcome` for item `item` (`None`:
    /// the whole request).
    fn answered(&mut self, link: &Link, rpc: RpcId, item: Option<u16>, outcome: &SubmitOutcome) {
        let own = match self.answers.get_mut(&rpc) {
            Some(answers) => {
                match item {
                    Some(idx) => answers.by_item.entry(idx).or_insert(outcome.clone()),
                    None => answers.whole.get_or_insert(outcome.clone()),
                };
                true
            }
            None => false,
        };
        let SubmitOutcome::Rejected(reject) = outcome else {
            return;
        };
        match reject.kind {
            RejectKind::RateLimited { .. } => {
                self.resend_due |= own || link.is_cancel_on(rpc, self.inst);
            }
            RejectKind::AlreadyTerminal(_) | RejectKind::NotFound => {}
            _ if own && self.end == SweepEnd::BeforePlaces => {
                self.failed.get_or_insert_with(|| reject.clone());
            }
            _ => {}
        }
    }

    /// The sweep at `now` on the shard's monotonic clock, under the consumer's `hold`: the
    /// cancels of its market's orders due on an acknowledgement sent, another market's left due
    /// in the registry, the cancel of everything sent again when a refusal for its rate of a
    /// cancel on its market made it due and the hold no longer holds, and where it stands. A
    /// sweep that failed sends nothing more.
    pub fn poll(
        &mut self,
        link: &mut Link,
        orders: &ExecOrders,
        hold: &RateHold,
        now: MonoNs,
    ) -> Polled {
        let mut polled = Polled {
            due: Vec::new(),
            due_failed: Vec::new(),
            resent: None,
            state: SweepState::Ended,
        };
        if let Some(reject) = &self.failed {
            polled.state = SweepState::Failed(reject.clone());
            return polled;
        }
        let caps = link.caps().clone();
        // The sweep's market's alone, by the market the registry records for each order: fbc-oms
        // lists every market's, and reading them clears nothing, so another market's stay due
        // for the consumer or that market's own sweep (decision 0155 item 5).
        let inst = self.inst;
        let due: Vec<ClientOrderId> = {
            let reg = link.registry();
            let on_market =
                |cid: &ClientOrderId| reg.get(*cid).is_some_and(|r| r.placed().inst == inst);
            reg.cancels_due(&caps)
                .into_iter()
                .filter(on_market)
                .collect()
        };
        // Each a single cancel, as the order's cancel permit builds it under the venue's caps: a
        // batch cancel of none of them, which fbc-oms would build under a declared batch. fbc-oms
        // lists an order due once its cancel can name it, so its permit builds the cancel; an
        // order it no longer permits a cancel of has ended, and needs none.
        let singles = OrderCaps {
            batch_cancel: None,
            ..caps.clone()
        };
        let plan = link.reg().cancel_many(&due, &singles);
        for cmd in plan.commands {
            match self.send(link, orders, cmd) {
                Ok(sent) => polled.due.push(sent),
                Err(failed) => polled.due_failed.push(failed),
            }
        }
        if self.resend_due && !hold.held(now) {
            self.resend_due = false;
            polled.resent = Some(self.cancel_everything(link, orders));
        }
        let gone = link.live_on(self.inst).is_empty();
        let ended = gone
            && match self.end {
                SweepEnd::BeforePlaces => orders.may_place_on(self.inst),
                SweepEnd::AtStop => self.sent.iter().all(|rpc| self.outcome(*rpc).is_some()),
            };
        if !ended {
            let resend_at = if self.resend_due { hold.until() } else { None };
            polled.state = SweepState::Waiting { resend_at };
        }
        polled
    }

    /// The cancel of everything of ours on the market (module documentation), sent.
    fn cancel_everything(&mut self, link: &mut Link, orders: &ExecOrders) -> Round {
        let caps = link.caps().clone();
        let not_ours = link.cancels_not_ours_seen();
        let everything = (!not_ours).then(|| link.reg().cancel_everything(self.inst, &caps));
        let (cmds, how) = match everything {
            Some(CancelEverything::CancelAll {
                command,
                unanswered,
            }) => {
                let mut cmds = vec![command];
                cmds.extend(unanswered.commands);
                (cmds, How::CancelAll)
            }
            // fbc-oms's own explicit cancels are dropped: building them cleared the wait of
            // each order whose acknowledgement landed, so that order is among the sweep's.
            Some(CancelEverything::Explicit { why, .. }) => {
                (self.explicit(link, &caps), How::Explicit(Why::Refused(why)))
            }
            None => (self.explicit(link, &caps), How::Explicit(Why::NotOursSeen)),
        };
        let mut round = Round {
            sent: Vec::new(),
            how,
            failed: None,
        };
        for cmd in cmds {
            match self.send(link, orders, cmd) {
                Ok(sent) => round.sent.push(sent),
                Err(failed) => {
                    round.failed = Some(failed);
                    break;
                }
            }
        }
        if round.failed.is_some() {
            // Left to the consumer, which handles the round's failure (module documentation).
            for sent in &round.sent {
                self.sent.retain(|rpc| *rpc != sent.rpc);
                self.answers.remove(&sent.rpc);
            }
        }
        round
    }

    /// The explicit cancels of each order of ours the link holds live on the market with no
    /// cancel in flight or awaiting its acknowledgement, or gone Unknown (a cancel answered so is
    /// sent again), in the order the link learnt them, as fbc-oms builds them. fbc-oms refuses a
    /// cancel's permit only for an order it does not hold or that ended, and each order here is
    /// one it holds that has not ended, so none is refused.
    fn explicit(&self, link: &mut Link, caps: &OrderCaps) -> Vec<PermittedCommand> {
        let open: Vec<ClientOrderId> = {
            let reg = link.registry();
            let free = |cid: &ClientOrderId| {
                reg.get(*cid).is_some_and(|r| {
                    let unknown = r.state() == OrdState::Unknown || r.unknown_since().is_some();
                    !r.cancel_awaits_ack() && (r.intent() == Intent::None || unknown)
                })
            };
            link.live_on(self.inst).into_iter().filter(free).collect()
        };
        link.reg().cancel_many(&open, caps).commands
    }

    /// Authorizes `cmd` for the sweep's account and submits it through the link: what was sent.
    fn send(
        &mut self,
        link: &mut Link,
        orders: &ExecOrders,
        cmd: PermittedCommand,
    ) -> Result<Sent, SendError> {
        let auth = link
            .reg()
            .authorize(self.acct, cmd)
            .map_err(SendError::Unauthorized)?;
        let rpc = link.submit(orders, auth).map_err(SendError::Unsubmitted)?;
        let names = link.cids_of(rpc);
        let answers = Answers {
            items: names.len().max(1),
            whole: None,
            by_item: BTreeMap::new(),
        };
        self.answers.insert(rpc, answers);
        self.sent.push(rpc);
        Ok(Sent { rpc, names })
    }
}
