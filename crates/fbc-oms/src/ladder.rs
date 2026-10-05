//! The Unknown ladder (decision 0005's I5 and I9; design §4.9): how the OMS resolves an order
//! whose fate it does not know, without ever placing or amending it again.
//!
//! An order goes on the ladder when it is Unknown (an unanswered placement, one the venue does
//! not know, or a per-item Unknown; [`OrderRecord::on_outcome`](crate::OrderRecord::on_outcome)),
//! when an amend or cancel on it goes unanswered or the venue does not know it, or when the
//! ladder's pass finds a command on it older than the intent timeout: a placement sent and
//! unanswered that long moves to Unknown, and an amend or cancel in flight that long escalates.
//! While on the ladder, an order counts as fully resting for every cap (an Unknown order's
//! [`resting`](crate::OrderRecord::resting) is its whole unfilled total), gets no
//! [`Live`](crate::Live) permit, so it is never amended, and its client id is never placed
//! again ([`Registry::insert`]).
//!
//! On the ladder:
//! 1. It is queried once, by the reference the venue's `query_refs` declare, and the answer
//!    is awaited for the intent timeout at most
//!    ([`QueryOrder::reference`]); a client id always exists. An answer showing the order
//!    resting with nothing in flight, or ended, resolves it ([`LadderResolution::Resolved`]),
//!    an open order still counting against the caps; one naming another order than the one
//!    queried applies nothing ([`LadderResolution::TargetMismatch`]). An Unknown order shown
//!    resting with a cancel still in flight stays on the ladder.
//! 2. A query that cannot be built, goes unanswered or is inconclusive (the venue does not
//!    find the order, or shows it with a command still in flight) leaves it to resyncs, and the
//!    ladder asks for one on every pass ([`LadderPlan::resync`]). A resync showing the order
//!    applies there as the query's answer would. With a [`SnapshotSource::Trustworthy`] source,
//!    an order absent from the configured number of snapshots in a row, each with a watermark
//!    at least its sent time plus the settle time, ends [`TerminalKind::Lost`] and is counted
//!    ([`Registry::lost`]). An order whose sent time was never recorded
//!    ([`Registry::placement_sent`]), or whose venue id an unconfirmed amend may have replaced
//!    on a venue whose amend gives a new id, is never found absent: a snapshot may show it
//!    under an id the record does not know.
//! 3. Still on the ladder after the configured maximum, a tombstone cancel names it by client
//!    id, and again each maximum after; accepted for good it ends the order Canceled, refused
//!    because the order already ended, Lost ([`Registry::tombstone_sent`]). A venue whose
//!    cancels cannot name a client id gets none ([`LadderPlan::no_tombstone`]).
//!
//! Every number is the consumer's ([`LadderConfig`], decision 0009). The OMS reads no clock:
//! the consumer runs [`Registry::ladder`] on its journaled timer with the timer's time.

use std::fmt;
use std::time::Duration;

use fbc_core::{
    CidMatch, ClientOrderId, MonoNs, OrderCaps, OrderUpdate, QueryAnswer, QueryOrder, RefKind,
    RpcId, SnapshotSource, SubmitOutcome, VenueOrderSnapshot, WallNs,
};

use crate::gateway::ControlCommand;
use crate::permit::{self, PermittedCommand};
use crate::record::{Intent, LadderStep, OrdState, OrderKey, OrderRecord, TerminalKind};
use crate::registry::{OmsError, Registry};

/// The Unknown ladder's numbers: the consumer's configuration, with no defaults.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct LadderConfig {
    intent_timeout: Duration,
    settle: Duration,
    max_unknown: Duration,
    absent_snapshots: u8,
}

/// A [`LadderConfig`] the OMS refuses.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LadderConfigError {
    /// The intent timeout is zero: every command would escalate the moment it is sent.
    ZeroIntentTimeout,
    /// The maximum time on the ladder is zero: every order would be tombstoned at once.
    ZeroMaxUnknown,
    /// No snapshot would be needed to declare an order lost.
    ZeroAbsentSnapshots,
}

impl fmt::Display for LadderConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LadderConfigError::ZeroIntentTimeout => {
                write!(f, "the ladder's intent timeout is zero")
            }
            LadderConfigError::ZeroMaxUnknown => {
                write!(f, "the ladder's maximum time unknown is zero")
            }
            LadderConfigError::ZeroAbsentSnapshots => {
                write!(
                    f,
                    "the ladder needs no absent snapshot to declare an order lost"
                )
            }
        }
    }
}

impl std::error::Error for LadderConfigError {}

impl LadderConfig {
    /// The ladder's numbers: how long a command may stay in flight before it escalates
    /// (`intent_timeout`), how long after an order was sent a snapshot's watermark must be for
    /// its absence to count (`settle`), how long an order stays on the ladder before a
    /// tombstone cancel (`max_unknown`), and how many such snapshots in a row must miss it
    /// before it is lost (`absent_snapshots`). Refused when one of them would make the ladder
    /// act at once.
    pub fn new(
        intent_timeout: Duration,
        settle: Duration,
        max_unknown: Duration,
        absent_snapshots: u8,
    ) -> Result<LadderConfig, LadderConfigError> {
        if intent_timeout.is_zero() {
            return Err(LadderConfigError::ZeroIntentTimeout);
        }
        if max_unknown.is_zero() {
            return Err(LadderConfigError::ZeroMaxUnknown);
        }
        if absent_snapshots == 0 {
            return Err(LadderConfigError::ZeroAbsentSnapshots);
        }
        Ok(LadderConfig {
            intent_timeout,
            settle,
            max_unknown,
            absent_snapshots,
        })
    }

    /// How long a command may stay in flight before it escalates to the ladder.
    pub fn intent_timeout(&self) -> Duration {
        self.intent_timeout
    }

    /// How long after an order was sent a snapshot's watermark must be for its absence to
    /// count.
    pub fn settle(&self) -> Duration {
        self.settle
    }

    /// How long an order stays on the ladder before a tombstone cancel, and between them.
    pub fn max_unknown(&self) -> Duration {
        self.max_unknown
    }

    /// How many trustworthy snapshots in a row must miss an order before it is lost.
    pub fn absent_snapshots(&self) -> u8 {
        self.absent_snapshots
    }
}

/// What one pass of the ladder ([`Registry::ladder`]) asks the consumer to send: queries and
/// tombstone cancels, and whether a resync is wanted. Nothing in it places or amends an order.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LadderPlan {
    /// The orders this pass put on the ladder: a placement or a command in flight past the
    /// intent timeout.
    pub escalated: Vec<ClientOrderId>,
    /// One query per order newly on the ladder, by a reference the venue's `query_refs`
    /// declare: its request is recorded with [`Registry::query_sent`].
    pub queries: Vec<(ClientOrderId, ControlCommand)>,
    /// An order on the ladder waits for a resync.
    pub resync: bool,
    /// The tombstone cancels due, each naming its order by client id: each is recorded with
    /// [`Registry::tombstone_sent`] once sent.
    pub tombstones: Vec<(ClientOrderId, PermittedCommand)>,
    /// The orders a tombstone is due for, on a venue whose single cancel cannot name a client
    /// id: none is built.
    pub no_tombstone: Vec<ClientOrderId>,
}

/// What an answer to a ladder query did.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LadderResolution {
    /// The order left the ladder, in this state: the venue showed it resting with nothing in
    /// flight, or ended.
    Resolved(OrdState),
    /// The venue did not settle it: resyncs decide ([`LadderPlan::resync`]).
    Inconclusive,
    /// The answer carries the query's request but names another order than the one queried
    /// (another client id, or a venue id another order of ours holds): nothing of it applies,
    /// the query is spent and resyncs decide.
    TargetMismatch,
    /// Not the answer to a ladder query the registry holds, or its order is no longer on the
    /// ladder: nothing changed.
    Ignored,
}

/// What a resync did to the orders on the ladder ([`Registry::on_resync`]).
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ResyncApplied {
    /// The orders the snapshot resolved, with their state.
    pub resolved: Vec<(ClientOrderId, OrdState)>,
    /// The orders it ended Lost.
    pub lost: Vec<ClientOrderId>,
}

impl Registry {
    /// Records when the placement of `cid` was sent: `at` on the shard's monotonic clock (the
    /// intent timeout runs from it) and `wall` on the wall clock snapshot watermarks are
    /// aligned to (the settle time runs from it). The same instants again change nothing;
    /// others are refused.
    pub fn placement_sent(
        &mut self,
        cid: ClientOrderId,
        at: MonoNs,
        wall: WallNs,
    ) -> Result<(), OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        match rec.sent_at() {
            Some(known) if known != (at, wall) => Err(OmsError::SentRecorded(cid)),
            _ => {
                rec.set_sent(at, wall);
                Ok(())
            }
        }
    }

    /// One pass of the Unknown ladder at `now`, the consumer's journaled timer's time.
    ///
    /// A PendingNew order sent at least the intent timeout ago moves to Unknown, and an order
    /// with an amend or cancel in flight that long goes on the ladder ([`LadderPlan::escalated`]).
    /// Each order on the ladder then takes its step: a query when one is due, a resync while
    /// resyncs decide, and a tombstone cancel by client id once it has been on the ladder the
    /// maximum time, and again each maximum after. Orders in client id order.
    pub fn ladder(&mut self, cfg: &LadderConfig, caps: &OrderCaps, now: MonoNs) -> LadderPlan {
        let mut plan = LadderPlan::default();
        // Forget the queries no order awaits any more: it left the ladder (an event resolved
        // it, or it ended) or gave its query up, and the result may never come.
        let orders = &self.orders;
        self.queries
            .retain(|rpc, cid| orders[cid].query_rpc() == Some(*rpc));
        let mut cids: Vec<ClientOrderId> = self
            .orders
            .iter()
            .filter(|(_, rec)| !rec.state().is_terminal())
            .map(|(cid, _)| *cid)
            .collect();
        cids.sort();
        for cid in cids {
            let rec = self.orders.get_mut(&cid).expect("listed above");
            if rec.unknown_since().is_none() {
                if !escalate(rec, cfg, now) {
                    continue;
                }
                plan.escalated.push(cid);
            }
            step(rec, cfg, caps, now, &mut plan);
            let since = rec
                .tombstone_at()
                .or(rec.unknown_since())
                .expect("the order is on the ladder");
            if now >= since + cfg.max_unknown {
                rec.set_tombstone_at(now);
                match permit::tombstone_of(rec, caps) {
                    Some(cmd) => plan.tombstones.push((cid, cmd)),
                    None => plan.no_tombstone.push(cid),
                }
            }
        }
        plan
    }

    /// Records the request the ladder's query for `cid` was sent under, which its answer or
    /// outcome names.
    pub fn query_sent(&mut self, cid: ClientOrderId, rpc: RpcId) -> Result<(), OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        rec.set_query_rpc(Some(rpc));
        self.queries.insert(rpc, cid);
        Ok(())
    }

    /// Applies the answer to a ladder query, under the key the consumer gives its envelope.
    /// The order the venue shows applies as its order update would
    /// ([`OrderRecord::apply_update`](crate::OrderRecord::apply_update)); resting with nothing
    /// in flight, or ended, the order leaves the ladder. Not found, or shown with a command
    /// still in flight, it is left to resyncs.
    pub fn on_query_answer(&mut self, answer: &QueryAnswer, key: OrderKey) -> LadderResolution {
        let Some(cid) = self.ladder_query(answer.rpc()) else {
            return LadderResolution::Ignored;
        };
        // Our queries always name the order by our client id, with a venue id it has.
        let target = answer.target();
        let names_it = target.client() == Some(cid)
            && target
                .venue()
                .is_none_or(|v| self.cid_of(v).is_none_or(|by_vid| by_vid == cid));
        if !names_it {
            self.with_record(cid, |rec| rec.set_ladder_step(LadderStep::Resync));
            return LadderResolution::TargetMismatch;
        }
        match answer.found() {
            Some(snap) => {
                let u = update_of(snap);
                self.with_record(cid, |rec| {
                    rec.apply_update(&u, key);
                    resolve(rec).map_or(LadderResolution::Inconclusive, LadderResolution::Resolved)
                })
            }
            None => self.with_record(cid, |rec| {
                rec.set_ladder_step(LadderStep::Resync);
                LadderResolution::Inconclusive
            }),
        }
    }

    /// Applies the outcome of a ladder query's request: unanswered, not sent or refused, the
    /// query was inconclusive and resyncs decide; accepted, its answer is still to come.
    pub fn on_query_outcome(&mut self, rpc: RpcId, outcome: &SubmitOutcome) -> LadderResolution {
        if matches!(outcome, SubmitOutcome::Accepted { .. }) {
            return LadderResolution::Ignored;
        }
        let Some(cid) = self.ladder_query(rpc) else {
            return LadderResolution::Ignored;
        };
        self.with_record(cid, |rec| rec.set_ladder_step(LadderStep::Resync));
        LadderResolution::Inconclusive
    }

    /// Applies a resync snapshot of the venue's open orders, taken at `watermark` and
    /// delivered under `key`, to the orders on the ladder.
    ///
    /// An order on the ladder the snapshot shows (by our client id, or by a venue id it had)
    /// applies there as an order update would; resting with nothing in flight, or ended, it
    /// leaves the ladder. An order whose client id and venue id the snapshot names apart is
    /// only counted as shown. With a [`SnapshotSource::Trustworthy`] source, an order on the
    /// ladder the snapshot does not show, sent at least the settle time before the watermark,
    /// counts one absence, and the configured number in a row ends it Lost, counted; a
    /// snapshot showing it starts the count again. An order with no recorded sent time, or
    /// whose venue id an unconfirmed amend may have replaced, never counts as absent.
    pub fn on_resync(
        &mut self,
        cfg: &LadderConfig,
        caps: &OrderCaps,
        watermark: WallNs,
        orders: &[VenueOrderSnapshot],
        key: OrderKey,
    ) -> ResyncApplied {
        let mut applied = ResyncApplied::default();
        let mut shown = std::collections::HashSet::new();
        for snap in orders {
            let by_vid = self.cid_of(&snap.vid);
            let by_cid = match snap.cid {
                // Another namespace's or a non-canonical client id names no order of ours, as
                // for an order update: nothing of it applies by its venue id, though an order
                // of ours holding that id is not counted absent.
                Some(CidMatch::Foreign(_) | CidMatch::Unparseable) => {
                    shown.extend(by_vid);
                    continue;
                }
                Some(CidMatch::Ours(cid)) if self.orders.contains_key(&cid) => Some(cid),
                Some(CidMatch::Ours(_)) | None => None,
            };
            shown.extend(by_cid);
            shown.extend(by_vid);
            let cid = match (by_cid, by_vid) {
                (Some(a), Some(b)) if a != b => continue,
                (Some(cid), _) | (None, Some(cid)) => cid,
                (None, None) => continue,
            };
            if self.orders[&cid].unknown_since().is_none() {
                continue;
            }
            let u = update_of(snap);
            if let Some(state) = self.with_record(cid, |rec| {
                rec.apply_update(&u, key);
                resolve(rec)
            }) {
                applied.resolved.push((cid, state));
            }
        }
        let mut cids: Vec<ClientOrderId> = self.orders.keys().copied().collect();
        cids.sort();
        for cid in cids {
            let rec = self.orders.get_mut(&cid).expect("listed above");
            if rec.unknown_since().is_none() || rec.state().is_terminal() {
                continue;
            }
            if shown.contains(&cid) {
                rec.shown_in_snapshot();
                continue;
            }
            let settled = rec.sent_at().is_some_and(|(_, sent)| {
                let settle = i64::try_from(cfg.settle.as_nanos()).unwrap_or(i64::MAX);
                watermark >= WallNs(sent.0.saturating_add(settle))
            });
            if caps.snapshot_source == SnapshotSource::Trustworthy
                && settled
                && !rec.id_may_have_moved(caps)
                && rec.absent_from_snapshot(cfg.absent_snapshots)
            {
                self.lost += 1;
                applied.lost.push(cid);
            }
        }
        applied
    }

    /// Records the tombstone cancel of `cid` the ladder built, sent at `now` under `rpc`, as
    /// [`Registry::cancel_sent`] records a cancel: its outcome then resolves the order
    /// ([`OutcomeApplied::TombstoneResolved`](crate::OutcomeApplied::TombstoneResolved)).
    /// False once the order is terminal.
    pub fn tombstone_sent(
        &mut self,
        cid: ClientOrderId,
        rpc: RpcId,
        now: MonoNs,
    ) -> Result<bool, OmsError> {
        let rec = self.orders.get_mut(&cid).ok_or(OmsError::UnknownCid(cid))?;
        Ok(rec.tombstone_sent(rpc, now))
    }

    /// How many orders ended Lost: absent from the trustworthy snapshots, or their tombstone
    /// refused because they had already ended.
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// The order the ladder query `rpc` was for, forgotten once answered; `None` when it is no
    /// ladder query of ours or the order is no longer on the ladder.
    fn ladder_query(&mut self, rpc: RpcId) -> Option<ClientOrderId> {
        let cid = self.queries.remove(&rpc)?;
        let rec = self
            .orders
            .get_mut(&cid)
            .expect("a query is recorded for a held order");
        if rec.unknown_since().is_none() || rec.query_rpc() != Some(rpc) {
            return None;
        }
        rec.set_query_rpc(None);
        Some(cid)
    }

    /// How many ladder queries are awaited: sent, and their order still on the ladder waiting
    /// for them. A query whose order left the ladder, or was given up, is forgotten at the
    /// ladder's next pass.
    pub fn queries_awaited(&self) -> usize {
        self.queries.len()
    }
}

/// Puts on the ladder at `now` an order whose placement, sent and unanswered, or whose amend or
/// cancel in flight, is at least the intent timeout old: a placement moves it to Unknown.
/// Whether it did.
fn escalate(rec: &mut OrderRecord, cfg: &LadderConfig, now: MonoNs) -> bool {
    let (since, cause) = match (rec.state(), rec.intent(), rec.sent_at()) {
        (
            _,
            Intent::PendingAmend { since, rpc, .. } | Intent::PendingCancel { since, rpc, .. },
            _,
        ) => (since, Some(rpc)),
        (OrdState::PendingNew, Intent::None, Some((at, _))) => (at, None),
        _ => return false,
    };
    if now < since + cfg.intent_timeout {
        return false;
    }
    if rec.state() == OrdState::PendingNew {
        rec.time_out_placement(now);
    } else {
        rec.enter_ladder(now, cause);
    }
    true
}

/// The step an order on the ladder takes in a pass at `now`: its query when one is due (or
/// straight to resyncs when the venue's queries name no reference it has), a resync while
/// they decide. A query built the intent timeout ago and still unanswered (an acknowledgement
/// clears its request's deadline, and the result may never come) is given up for resyncs.
fn step(
    rec: &mut OrderRecord,
    cfg: &LadderConfig,
    caps: &OrderCaps,
    now: MonoNs,
    plan: &mut LadderPlan,
) {
    match rec.ladder_step() {
        Some(LadderStep::Query) => {
            let query = QueryOrder {
                target: rec.order_ref(caps),
                inst: rec.placed().inst,
                placement_nonce: rec.placement_nonce(),
            };
            if query.reference(caps.query_refs).is_some() {
                rec.set_ladder_step(LadderStep::Querying);
                rec.set_queried_at(now);
                plan.queries.push((rec.cid(), ControlCommand::Query(query)));
            } else {
                rec.set_ladder_step(LadderStep::Resync);
                plan.resync = true;
            }
        }
        Some(LadderStep::Querying)
            if rec
                .queried_at()
                .is_some_and(|built| now >= built + cfg.intent_timeout) =>
        {
            // No longer awaited: a late result is ignored, resyncs decide.
            rec.set_ladder_step(LadderStep::Resync);
            rec.set_query_rpc(None);
            plan.resync = true;
        }
        Some(LadderStep::Resync) => plan.resync = true,
        Some(LadderStep::Querying) | None => {}
    }
}

/// Takes `rec` off the ladder when what the venue showed settles it: ended, or resting with
/// nothing in flight; otherwise resyncs decide. The state it was resolved in, if it was.
fn resolve(rec: &mut OrderRecord) -> Option<OrdState> {
    let state = rec.state();
    // Resting is Open or PartiallyFilled: a PendingNew order (a cancel of it unanswered) is
    // no more settled than an Unknown one.
    let settled = state.is_terminal() || (state.rank() > 0 && rec.intent() == Intent::None);
    if settled {
        rec.leave_ladder();
        Some(state)
    } else {
        rec.set_ladder_step(LadderStep::Resync);
        None
    }
}

/// The order update a snapshot of an order states.
pub(crate) fn update_of(snap: &VenueOrderSnapshot) -> OrderUpdate {
    OrderUpdate {
        cid: snap.cid,
        vid: Some(snap.vid.clone()),
        inst: snap.inst,
        side: snap.side,
        state: snap.state.clone(),
        cum_filled: snap.cum_filled,
        px: snap.px,
        qty: Some(snap.qty),
        post_only: snap.post_only,
        reduce_only: snap.reduce_only,
    }
}

impl OrderRecord {
    /// Whether, on a venue whose amend gives the order a new id, an amend not yet confirmed may
    /// have replaced its venue id, or one confirmed without naming the new id did: a snapshot
    /// may then show it under an id the record does not know.
    pub(crate) fn id_may_have_moved(&self, caps: &OrderCaps) -> bool {
        caps.amend.is_some_and(|a| !a.keeps_venue_id)
            && (self.amend_unconfirmed() || self.vid_retired())
    }
}

/// Whether the venue's single cancel can name our client id.
pub(crate) fn cancels_by_client(caps: &OrderCaps) -> bool {
    caps.cancel_refs.contains(RefKind::Client)
}

/// Whether `state` is the Lost ending the registry counts.
pub(crate) fn is_lost(state: OrdState) -> bool {
    state == OrdState::Terminal(TerminalKind::Lost)
}
