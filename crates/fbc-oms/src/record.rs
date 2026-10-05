//! One order's record and the lattice it moves through (decision 0005; design §4.9 with 0014's
//! refinements).

use fbc_core::{
    CancelReason, ClientOrderId, Lots, MonoNs, NewOrder, NotSentReason, OrderKind, OrderUpdate,
    RejectKind, RpcId, SubmitOutcome, Ticks, VenueOrderId, VenueOrderState,
};

/// Where an order stands. The states are ranked ([`OrdState::rank`]) and a record's rank never
/// falls; a terminal state is absorbing.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrdState {
    /// Sent (or about to be), with no answer yet.
    PendingNew,
    /// Sent with no answer before its deadline, or not known to the venue: counted as fully
    /// resting and resolved by the Unknown ladder, never resent (decision 0005, I5).
    Unknown,
    /// Resting with nothing filled.
    Open,
    /// Resting with part of it filled.
    PartiallyFilled,
    /// Done: nothing can move it again.
    Terminal(TerminalKind),
}

impl OrdState {
    /// The state's rank: PendingNew and Unknown 0, Open 1, PartiallyFilled 2, Terminal 3.
    pub const fn rank(self) -> u8 {
        match self {
            OrdState::PendingNew | OrdState::Unknown => 0,
            OrdState::Open => 1,
            OrdState::PartiallyFilled => 2,
            OrdState::Terminal(_) => 3,
        }
    }

    /// Whether the state is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, OrdState::Terminal(_))
    }
}

/// How an order ended.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TerminalKind {
    /// Filled completely.
    Filled,
    /// Cancelled, for the reason given.
    Canceled(CancelReason),
    /// Refused by the venue: the order never rested.
    Rejected(RejectKind),
    /// Expired.
    Expired,
    /// Never sent: no byte of the placement reached a socket buffer.
    NotSent(NotSentReason),
}

/// The order an update arrived in: the venue's ordering key when the venue gives one
/// ([`OrderingKey`](fbc_core::OrderingKey)), with the shard's ingest order as tiebreak. It
/// orders only non-terminal updates; a terminal update applies whatever its key.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct OrderKey {
    /// The venue's ordering key, `None` when the venue gives none.
    pub venue: Option<u64>,
    /// The shard's ingest order.
    pub ingest: u64,
}

impl OrderKey {
    /// Whether `self` is strictly older than `than`: by the venue keys when both have one, the
    /// ingest order breaking a tie; by the ingest order alone otherwise.
    pub fn is_older_than(self, than: OrderKey) -> bool {
        match (self.venue, than.venue) {
            (Some(a), Some(b)) => (a, self.ingest) < (b, than.ingest),
            _ => self.ingest < than.ingest,
        }
    }
}

/// A command in flight on a live order, until an event or outcome resolves it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Intent {
    /// Nothing in flight.
    None,
    /// A cancel was sent.
    PendingCancel {
        /// The cancel's request.
        rpc: RpcId,
        /// When it was sent.
        since: MonoNs,
    },
    /// An amend to `px` and `qty` (the total, filled part included) was sent.
    PendingAmend {
        px: Ticks,
        qty: Lots,
        /// The amend's request.
        rpc: RpcId,
        /// When it was sent.
        since: MonoNs,
    },
}

/// What [`OrderRecord::apply_update`] did with an update.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Applied {
    /// Applied: the state may have moved.
    Advanced,
    /// An amended update applied: the price, total and venue id may have changed.
    Amended,
    /// The order is terminal; nothing moves it.
    IgnoredLate,
    /// A non-terminal update strictly older than the last one applied.
    IgnoredStale,
    /// The update names a venue id an amend replaced.
    IgnoredSupersededVid,
}

/// The kind of command an outcome answers.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderOp {
    Place,
    Amend,
    Cancel,
}

/// What [`OrderRecord::on_outcome`] did with an item's outcome. None of these is a command:
/// an outcome never makes the OMS send anything, and an Unknown order is never resent.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OutcomeApplied {
    /// The placement was accepted: the order rests, Open (or PartiallyFilled when an update
    /// already reported a fill).
    Opened,
    /// The placement was not sent or was refused: the order is terminal.
    Ended,
    /// The placement went unanswered (or the venue does not know it): the order is Unknown.
    MovedToUnknown,
    /// The amend or cancel went unanswered, or the venue does not know the order: the order is
    /// left to the Unknown ladder ([`OrderRecord::unknown_since`]).
    AwaitingLadder,
    /// The amend or cancel was not sent or was refused: the order is as it was, nothing in
    /// flight; Filled if its fills then cover every total the venue may hold.
    IntentCleared,
    /// Nothing changed: the order is terminal, or the outcome waits for an order event.
    Unchanged,
}

/// What a fill the ledger accepted did to its order
/// ([`Registry::apply_fill`](crate::Registry::apply_fill)).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillApplied {
    /// Counted; the order rests, PartiallyFilled (a PendingNew or Unknown order is promoted).
    Live,
    /// Counted, and the fills alone now cover the order's total: it is Filled.
    Completed,
    /// Counted on an order already terminal, whose state does not move.
    AfterEnd,
}

/// One of our orders, as the OMS knows it.
///
/// The fields are private and change only through [`apply_update`](OrderRecord::apply_update),
/// [`on_outcome`](OrderRecord::on_outcome), the sent intents and the fills the ledger accepted
/// ([`Registry::apply_fill`](crate::Registry::apply_fill)), so the lattice only moves forward.
#[derive(Clone, Debug)]
pub struct OrderRecord {
    placed: NewOrder,
    vid: Option<VenueOrderId>,
    /// Which venue id an amend replaced with which: every source is superseded.
    replaced: Vec<(VenueOrderId, VenueOrderId)>,
    px: Option<Ticks>,
    qty: Lots,
    cum_venue: Lots,
    cum_fills: Lots,
    /// The largest total of the amends a later command replaced in flight, before anything
    /// tied a confirmation to them: any of them may rest at the venue.
    unsettled: Option<Lots>,
    /// The latest venue ordering key applied when an amend was replaced in flight, or since
    /// while a command was in flight: only a total stated under a later key, with no command
    /// in flight, settles `unsettled`.
    unsettled_bar: Option<u64>,
    state: OrdState,
    intent: Intent,
    last_key: Option<OrderKey>,
    unknown_since: Option<MonoNs>,
}

impl OrderRecord {
    /// The record of a placement about to be sent: PendingNew, at the placement's price and
    /// quantity.
    pub fn new(placed: NewOrder) -> OrderRecord {
        let px = match placed.kind {
            OrderKind::Limit { px } => Some(px),
            OrderKind::Market => None,
        };
        OrderRecord {
            qty: placed.qty,
            placed,
            vid: None,
            replaced: Vec::new(),
            px,
            cum_venue: Lots::ZERO,
            cum_fills: Lots::ZERO,
            unsettled: None,
            unsettled_bar: None,
            state: OrdState::PendingNew,
            intent: Intent::None,
            last_key: None,
            unknown_since: None,
        }
    }

    /// Our id for the order.
    pub fn cid(&self) -> ClientOrderId {
        self.placed.cid
    }

    /// The placement as it was sent.
    pub fn placed(&self) -> &NewOrder {
        &self.placed
    }

    /// The venue's current id for the order, once known.
    pub fn vid(&self) -> Option<&VenueOrderId> {
        self.vid.as_ref()
    }

    /// The venue ids an amend replaced.
    pub fn superseded_vids(&self) -> impl Iterator<Item = &VenueOrderId> {
        self.replaced.iter().map(|(old, _)| old)
    }

    /// Whether an amend replaced `vid`.
    pub fn is_superseded(&self, vid: &VenueOrderId) -> bool {
        self.replaced.iter().any(|(old, _)| old == vid)
    }

    /// The limit price (`None` for a market order the venue has not priced).
    pub fn px(&self) -> Option<Ticks> {
        self.px
    }

    /// The total quantity, filled part included.
    pub fn qty(&self) -> Lots {
        self.qty
    }

    /// The largest cumulative fill the venue reported on an order update.
    pub fn cum_venue(&self) -> Lots {
        self.cum_venue
    }

    /// The sum of the order's fills the ledger accepted: each fill counted once.
    pub fn cum_fills(&self) -> Lots {
        self.cum_fills
    }

    /// The filled quantity: the larger of the venue's cumulative count and the deduplicated
    /// fill sum, never their sum, since both count the same executions (decision 0005, I3).
    pub fn filled(&self) -> Lots {
        self.cum_venue.max(self.cum_fills)
    }

    /// The quantity still resting: nothing once terminal; otherwise the largest total the
    /// venue may hold less the filled part, so a PendingNew or Unknown order counts as fully
    /// resting, and an amend to a larger total counts from when it is sent until the venue
    /// confirms an amend sent at or after it, or states the total once no command is in flight
    /// under a venue ordering key later than any applied while the amend was on its way, or the
    /// amend is refused while it is the only one unconfirmed, or the order ends (I6).
    pub fn resting(&self) -> Lots {
        if self.state.is_terminal() {
            Lots::ZERO
        } else {
            self.ceiling()
                .checked_sub(self.filled())
                .unwrap_or(Lots::ZERO)
        }
    }

    /// The largest total the venue may hold: the order's total, the amend in flight's and
    /// those of the amends replaced in flight before they were confirmed.
    fn ceiling(&self) -> Lots {
        let pending = match self.intent {
            Intent::PendingAmend { qty, .. } => qty,
            _ => Lots::ZERO,
        };
        self.qty
            .max(pending)
            .max(self.unsettled.unwrap_or(Lots::ZERO))
    }

    /// Where the order stands.
    pub fn state(&self) -> OrdState {
        self.state
    }

    /// The command in flight on it, if any.
    pub fn intent(&self) -> Intent {
        self.intent
    }

    /// The key of the last update applied.
    pub fn last_key(&self) -> Option<OrderKey> {
        self.last_key
    }

    /// When the order, or the command in flight on it, became unknown: the Unknown ladder's
    /// starting point. Cleared when the order leaves Unknown or ends.
    pub fn unknown_since(&self) -> Option<MonoNs> {
        self.unknown_since
    }

    /// Records an amend to `px` and `qty` sent at `now` under `rpc`. Refused (false) once the
    /// order is terminal.
    pub fn amend_sent(&mut self, px: Ticks, qty: Lots, rpc: RpcId, now: MonoNs) -> bool {
        self.set_intent(Intent::PendingAmend {
            px,
            qty,
            rpc,
            since: now,
        })
    }

    /// Records a cancel sent at `now` under `rpc`. Refused (false) once the order is terminal.
    pub fn cancel_sent(&mut self, rpc: RpcId, now: MonoNs) -> bool {
        self.set_intent(Intent::PendingCancel { rpc, since: now })
    }

    fn set_intent(&mut self, intent: Intent) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        if let Intent::PendingAmend { qty, .. } = self.intent {
            // Replaced before anything confirmed it: the amend may still reach the venue.
            self.unsettled = Some(self.unsettled.map_or(qty, |u| u.max(qty)));
            self.unsettled_bar = self.unsettled_bar.max(self.last_key.and_then(|k| k.venue));
        }
        self.intent = intent;
        true
    }

    /// Applies one venue order update arriving under `key`.
    ///
    /// A terminal order ignores it. An update naming a superseded venue id is ignored. A
    /// terminal update applies whatever its key, and names the order's last venue id. A
    /// non-terminal update strictly older than the last applied ([`OrderKey::is_older_than`])
    /// is ignored, though an amend's replacement of one venue id by another, a fact whenever
    /// it arrives, is still recorded. The venue's price and total apply when the update states
    /// them; `cum_venue` keeps the largest cumulative fill. An Open update moves the order to
    /// Open, or PartiallyFilled once something is filled; an amended update moves no state.
    /// An update leaving the order at the price and total of the amend in flight confirms it.
    /// An amended update stating neither also confirms it, the amend's price and total then
    /// standing, when it is tied to that amend: no earlier amend was replaced in flight
    /// unconfirmed, and it carries a venue ordering key later than the last update's. Without
    /// that it may be a duplicate, or a late notice, of an older amend's confirmation (a new
    /// venue id first seen here included), and the amend stays in flight, counted
    /// ([`Self::resting`]). A total stated while no command is in flight settles the amends
    /// replaced in flight before it, but only under a venue ordering key later than every one
    /// applied when they were replaced or while a command was in flight since: a delayed or
    /// duplicate update, or one on a feed without venue keys, may predate them. Either kind of update ends the order Filled when its
    /// fills alone then cover every total the venue may hold.
    pub fn apply_update(&mut self, u: &OrderUpdate, key: OrderKey) -> Applied {
        if self.state.is_terminal() {
            return Applied::IgnoredLate;
        }
        if u.vid.as_ref().is_some_and(|v| self.is_superseded(v)) {
            return Applied::IgnoredSupersededVid;
        }
        if let (None, Some(v)) = (&self.vid, &u.vid) {
            self.vid = Some(self.follow(v));
        }
        let later = key.venue.is_some_and(|k| {
            self.last_key
                .is_none_or(|last| last.venue.is_some_and(|l| k > l))
        });
        if let VenueOrderState::Amended { new_vid: Some(nv) } = &u.state {
            self.replace(u.vid.as_ref(), nv);
        }
        let ends = terminal_kind(&u.state);
        if ends.is_none() && self.last_key.is_some_and(|last| key.is_older_than(last)) {
            return Applied::IgnoredStale;
        }
        self.cum_venue = self.cum_venue.max(u.cum_filled);
        if let Some(px) = u.px {
            self.px = Some(px);
        }
        if let Some(qty) = u.qty {
            self.qty = qty;
            if self.intent == Intent::None
                && key
                    .venue
                    .is_some_and(|k| self.unsettled_bar.is_some_and(|bar| k > bar))
            {
                // The venue states the total with no command in flight, under a key later than
                // any applied while the amends replaced in flight could still be on their way:
                // they are behind this total. An update under an earlier key, or none, may
                // predate them (a delayed or duplicate one), and settles nothing.
                self.settle();
            }
        }
        if self.unsettled.is_some() && self.intent != Intent::None {
            self.unsettled_bar = self.unsettled_bar.max(key.venue);
        }
        self.last_key = Some(key);
        if let Some(kind) = ends {
            if let Some(v) = &u.vid {
                self.vid = Some(v.clone());
            }
            self.end(kind);
            return Applied::Advanced;
        }
        if let VenueOrderState::Amended { .. } = u.state {
            if let Intent::PendingAmend { px, qty, .. } = self.intent
                && self.unsettled.is_none()
                && later
            {
                // Tied to the amend in flight: what the venue does not echo is what was sent.
                self.px = Some(u.px.unwrap_or(px));
                self.qty = u.qty.unwrap_or(qty);
            }
            self.confirm_if_stated();
            self.complete_if_covered();
            return Applied::Amended;
        }
        if self.state == OrdState::Unknown {
            self.unknown_since = None;
        }
        self.state = self.live_state();
        self.confirm_if_stated();
        self.complete_if_covered();
        Applied::Advanced
    }

    /// Applies the outcome of one command item for this order: `op` is the command's kind,
    /// `vid` the venue id the item names (for an accepted placement, the id the venue
    /// assigned), `now` when the outcome arrived.
    ///
    /// A terminal order is unchanged. A placement not sent ends the order
    /// ([`TerminalKind::NotSent`]); accepted, it opens a PendingNew or Unknown order under the
    /// item's venue id; refused, it ends it ([`TerminalKind::Rejected`]), unless the venue
    /// says it does not know the order (`NotFound`), which, like no answer, moves a PendingNew
    /// order to Unknown. An amend or cancel not sent or refused leaves the order as it was
    /// with nothing in flight, except a cancel refused because the order already ended, which
    /// waits for the order's terminal event; unanswered or not known to the venue, it is left
    /// to the Unknown ladder.
    pub fn on_outcome(
        &mut self,
        op: OrderOp,
        vid: Option<&VenueOrderId>,
        outcome: &SubmitOutcome,
        now: MonoNs,
    ) -> OutcomeApplied {
        if self.state.is_terminal() {
            return OutcomeApplied::Unchanged;
        }
        match (op, outcome) {
            (OrderOp::Place, SubmitOutcome::NotSent(reason)) => {
                self.end(TerminalKind::NotSent(*reason));
                OutcomeApplied::Ended
            }
            (OrderOp::Place, SubmitOutcome::Accepted { .. }) => {
                if let (None, Some(v)) = (&self.vid, vid) {
                    self.vid = Some(self.follow(v));
                }
                if self.state.rank() > 0 {
                    return OutcomeApplied::Unchanged;
                }
                self.state = self.live_state();
                self.unknown_since = None;
                OutcomeApplied::Opened
            }
            (OrderOp::Place, SubmitOutcome::Rejected(r)) if r.kind != RejectKind::NotFound => {
                self.end(TerminalKind::Rejected(r.kind));
                OutcomeApplied::Ended
            }
            (OrderOp::Place, SubmitOutcome::Rejected(_) | SubmitOutcome::Unknown) => {
                if self.state != OrdState::PendingNew {
                    return OutcomeApplied::Unchanged;
                }
                self.state = OrdState::Unknown;
                self.unknown_since = Some(now);
                OutcomeApplied::MovedToUnknown
            }
            (_, SubmitOutcome::Accepted { .. }) => OutcomeApplied::Unchanged,
            (OrderOp::Cancel, SubmitOutcome::Rejected(r))
                if matches!(r.kind, RejectKind::AlreadyTerminal(_)) =>
            {
                OutcomeApplied::Unchanged
            }
            (_, SubmitOutcome::Rejected(r)) if r.kind == RejectKind::NotFound => {
                self.await_ladder(now)
            }
            (_, SubmitOutcome::NotSent(_) | SubmitOutcome::Rejected(_)) => {
                self.intent = Intent::None;
                self.complete_if_covered();
                OutcomeApplied::IntentCleared
            }
            (_, SubmitOutcome::Unknown) => self.await_ladder(now),
        }
    }

    /// The order's fill sum once a fill of `qty` is counted; `None` when that overflows.
    pub(crate) fn cum_fills_with(&self, qty: Lots) -> Option<Lots> {
        self.cum_fills.checked_add(qty)
    }

    /// Counts a fill the ledger accepted, naming the venue id `vid` and reporting the order's
    /// cumulative quantity `cum_after`, the order's fill sum then being `cum_fills` (checked by
    /// [`Self::cum_fills_with`]).
    ///
    /// The fill counts whatever the order's state, and `cum_after`, when reported, raises
    /// `cum_venue`. A terminal order does not move. Otherwise
    /// the record learns the fill's venue id when it has none; a PendingNew or Unknown order is
    /// promoted; and the order is Filled only when the fills alone cover every total the venue
    /// may hold (an amend's in flight included), never on the venue's cumulative count alone.
    pub(crate) fn apply_fill(
        &mut self,
        vid: Option<&VenueOrderId>,
        cum_after: Option<Lots>,
        cum_fills: Lots,
    ) -> FillApplied {
        self.cum_fills = cum_fills;
        if let Some(cum) = cum_after {
            self.cum_venue = self.cum_venue.max(cum);
        }
        if self.state.is_terminal() {
            return FillApplied::AfterEnd;
        }
        if let (None, Some(v)) = (&self.vid, vid) {
            self.vid = Some(self.follow(v));
        }
        if self.complete_if_covered() {
            return FillApplied::Completed;
        }
        if self.state == OrdState::Unknown {
            self.unknown_since = None;
        }
        self.state = self.live_state();
        FillApplied::Live
    }

    /// Resolves the amend in flight once the order stands at its price and total: the venue
    /// applied it, and with it every amend sent before it.
    fn confirm_if_stated(&mut self) {
        if let Intent::PendingAmend { px, qty, .. } = self.intent
            && self.px == Some(px)
            && self.qty == qty
        {
            self.intent = Intent::None;
            self.settle();
        }
    }

    /// Retires the totals of the amends replaced in flight.
    fn settle(&mut self) {
        self.unsettled = None;
        self.unsettled_bar = None;
    }

    /// Ends a live order Filled when its fills alone cover every total the venue may hold
    /// (its own, the amend in flight's and any unconfirmed earlier amend's); whether it did.
    /// Checked whenever the fills, the total or the amends in flight change.
    fn complete_if_covered(&mut self) -> bool {
        let covered = !self.state.is_terminal() && self.cum_fills >= self.ceiling();
        if covered {
            self.end(TerminalKind::Filled);
        }
        covered
    }

    /// The state of an order the venue shows resting: PartiallyFilled once something is
    /// filled, Open before.
    fn live_state(&self) -> OrdState {
        if self.filled() > Lots::ZERO {
            OrdState::PartiallyFilled
        } else {
            OrdState::Open
        }
    }

    fn await_ladder(&mut self, now: MonoNs) -> OutcomeApplied {
        self.unknown_since.get_or_insert(now);
        OutcomeApplied::AwaitingLadder
    }

    fn end(&mut self, kind: TerminalKind) {
        self.state = OrdState::Terminal(kind);
        self.intent = Intent::None;
        self.settle();
        self.unknown_since = None;
    }

    /// Records that an amend replaced `old` (the record's current id when the update names
    /// none) by `new`, unless that would make the replacements circular, and moves the
    /// current id to the end of its chain of replacements.
    fn replace(&mut self, old: Option<&VenueOrderId>, new: &VenueOrderId) {
        if let Some(from) = old.or(self.vid.as_ref()).cloned()
            && self.follow(new) != from
        {
            self.replaced.push((from, new.clone()));
        }
        let current = self.vid.as_ref().unwrap_or(new);
        self.vid = Some(self.follow(current));
    }

    /// The id `vid` was last replaced by, through every recorded replacement; `vid` itself
    /// when nothing replaced it. The replacements never form a cycle ([`Self::replace`]), and
    /// each id is replaced at most once, since an update naming a replaced id is ignored.
    fn follow(&self, vid: &VenueOrderId) -> VenueOrderId {
        let mut at = vid;
        while let Some((_, next)) = self.replaced.iter().find(|(old, _)| old == at) {
            at = next;
        }
        at.clone()
    }
}

/// The terminal kind an update's state ends an order with, `None` for a live state.
fn terminal_kind(state: &VenueOrderState) -> Option<TerminalKind> {
    match state {
        VenueOrderState::Open | VenueOrderState::Amended { .. } => None,
        VenueOrderState::Filled => Some(TerminalKind::Filled),
        VenueOrderState::Canceled(reason) => Some(TerminalKind::Canceled(*reason)),
        VenueOrderState::Rejected(reject) => Some(TerminalKind::Rejected(reject.kind())),
        VenueOrderState::Expired => Some(TerminalKind::Expired),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use fbc_core::{
        AccountKey, Channel, CidMint, InstrumentId, Namespace, NamespaceLease, NotAmendable,
        Reject, Side, Tif, WallNs,
    };

    use super::*;

    fn lots(n: i64) -> Lots {
        Lots::new(n).unwrap()
    }

    fn key(ingest: u64) -> OrderKey {
        OrderKey {
            venue: None,
            ingest,
        }
    }

    /// A venue update in `state` with cumulative fill `cum`, naming no ids.
    fn update(state: VenueOrderState, cum: i64) -> OrderUpdate {
        OrderUpdate {
            cid: None,
            vid: None,
            inst: InstrumentId::new(1),
            side: Side::Buy,
            state,
            cum_filled: lots(cum),
            px: None,
            qty: None,
            post_only: None,
            reduce_only: None,
        }
    }

    /// A pending limit buy of `qty`, under a client id minted in a lease of its own.
    fn order(qty: i64) -> OrderRecord {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir: PathBuf =
            std::env::temp_dir().join(format!("fbc-oms-record-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), Namespace::new(3)).unwrap();
        let cid = CidMint::new(lease, 0, 0, WallNs(0)).mint().unwrap();
        OrderRecord::new(NewOrder {
            cid,
            inst: InstrumentId::new(1),
            side: Side::Buy,
            qty: lots(qty),
            kind: OrderKind::Limit { px: Ticks(100) },
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        })
    }

    #[test]
    fn an_amend_to_a_larger_total_in_flight_keeps_a_covered_order_resting() {
        let mut rec = order(4);
        assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(4)), FillApplied::Live);
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        // The amend may already rest at the venue: its four more lots count as resting.
        assert_eq!(rec.resting(), lots(4));
        // Once the fills cover the amended total too, the order is filled.
        assert_eq!(rec.apply_fill(None, None, lots(8)), FillApplied::Completed);
        assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
    }

    #[test]
    fn a_covered_order_is_filled_once_the_larger_amend_fails() {
        let refusal = Reject {
            kind: RejectKind::NotAmendable(NotAmendable::Other),
            venue_code: None,
            raw: "refused".into(),
        };
        let fails = [
            SubmitOutcome::NotSent(NotSentReason::Unencodable),
            SubmitOutcome::Rejected(refusal),
        ];
        for outcome in fails {
            let mut rec = order(5);
            assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
            assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
            rec.on_outcome(OrderOp::Amend, None, &outcome, MonoNs(3));
            assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
            assert_eq!(rec.resting(), Lots::ZERO);
        }
    }

    #[test]
    fn a_covered_order_is_filled_once_an_update_resolves_the_amend_at_or_below_its_fills() {
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        // The venue amended it to eight: still resting three.
        let mut amended = update(VenueOrderState::Amended { new_vid: None }, 5);
        (amended.px, amended.qty) = (Some(Ticks(101)), Some(lots(8)));
        rec.apply_update(&amended, key(1));
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        assert_eq!(rec.resting(), lots(3));
        // A later update reports a total the fills already cover.
        let mut open = update(VenueOrderState::Open, 5);
        open.qty = Some(lots(5));
        rec.apply_update(&open, key(2));
        assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
    }

    fn keyed(venue: u64, ingest: u64) -> OrderKey {
        OrderKey {
            venue: Some(venue),
            ingest,
        }
    }

    fn refused() -> SubmitOutcome {
        SubmitOutcome::Rejected(Reject {
            kind: RejectKind::NotAmendable(NotAmendable::Other),
            venue_code: None,
            raw: "refused".into(),
        })
    }

    #[test]
    fn an_amend_confirmed_later_than_anything_applied_takes_the_values_that_were_sent() {
        let mut rec = order(5);
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(1, 1));
        assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        // The venue confirms the amend under a newer ordering key, echoing neither price nor
        // total.
        let amended = update(VenueOrderState::Amended { new_vid: None }, 5);
        assert_eq!(rec.apply_update(&amended, keyed(2, 2)), Applied::Amended);
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(8)));
        assert_eq!(rec.resting(), lots(3));
        assert_eq!(rec.intent(), Intent::None);
    }

    #[test]
    fn an_amend_confirmed_with_nothing_to_tie_it_to_the_amend_in_flight_counts_the_larger_total() {
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        // No venue ordering key, no new venue id, no price or total: this may be a duplicate
        // of an older confirmation, so the amend stays in flight and its total counts.
        let amended = update(VenueOrderState::Amended { new_vid: None }, 5);
        assert_eq!(rec.apply_update(&amended, key(1)), Applied::Amended);
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(100)), lots(5)));
        assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
        assert_eq!(rec.resting(), lots(3));
        // An update stating the amend's price and total resolves it.
        let mut stated = update(VenueOrderState::Amended { new_vid: None }, 5);
        (stated.px, stated.qty) = (Some(Ticks(101)), Some(lots(8)));
        assert_eq!(rec.apply_update(&stated, key(2)), Applied::Amended);
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!(rec.resting(), lots(3));
    }

    #[test]
    fn a_duplicate_of_an_older_amend_confirmation_does_not_resolve_a_newer_amend() {
        // Codex r4182353444: the venue keeps its order id.
        let mut rec = order(5);
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(1, 1));
        assert!(rec.amend_sent(Ticks(101), lots(6), RpcId(3), MonoNs(2)));
        let a1 = update(VenueOrderState::Amended { new_vid: None }, 0);
        assert_eq!(rec.apply_update(&a1, keyed(2, 2)), Applied::Amended);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(6)));
        assert!(rec.amend_sent(Ticks(102), lots(9), RpcId(4), MonoNs(3)));
        // A1's confirmation again: same venue key, later ingest.
        assert_eq!(rec.apply_update(&a1, keyed(2, 3)), Applied::Amended);
        assert!(matches!(
            rec.intent(),
            Intent::PendingAmend { qty, .. } if qty == lots(9)
        ));
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(6)));
        assert_eq!(rec.resting(), lots(9), "A2 may rest");
        // A2 is refused: the order is A1's.
        rec.on_outcome(OrderOp::Amend, None, &refused(), MonoNs(4));
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(6)));
        assert_eq!(rec.resting(), lots(6));
    }

    #[test]
    fn an_amend_replaced_in_flight_counts_until_a_later_amend_is_confirmed() {
        let mut rec = order(5);
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(1, 1));
        // A1 to nine, then A2 to seven before anything answers A1.
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.amend_sent(Ticks(102), lots(7), RpcId(4), MonoNs(3)));
        assert_eq!(rec.resting(), lots(9), "A1 may rest");
        // A newer bare confirmation could be A1's: it resolves neither.
        let bare = update(VenueOrderState::Amended { new_vid: None }, 0);
        assert_eq!(rec.apply_update(&bare, keyed(2, 2)), Applied::Amended);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(100)), lots(5)));
        assert_eq!(rec.resting(), lots(9));
        // A2 is refused: A1 may still rest, so its total still counts, and fills short of
        // it do not complete the order.
        rec.on_outcome(OrderOp::Amend, None, &refused(), MonoNs(4));
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!(rec.resting(), lots(9));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        assert_eq!(rec.resting(), lots(4));
        // A3 to six, confirmed with its values: the venue applied it after A1.
        assert!(rec.amend_sent(Ticks(103), lots(6), RpcId(5), MonoNs(5)));
        let mut a3 = update(VenueOrderState::Amended { new_vid: None }, 5);
        (a3.px, a3.qty) = (Some(Ticks(103)), Some(lots(6)));
        assert_eq!(rec.apply_update(&a3, keyed(3, 3)), Applied::Amended);
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!(rec.resting(), lots(1));
    }

    #[test]
    fn a_total_stated_later_once_no_amend_is_in_flight_settles_the_amends_replaced_in_flight() {
        // Codex r4182657487, under r4182986303's rule.
        let mut rec = order(5);
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(1, 1));
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.amend_sent(Ticks(102), lots(7), RpcId(4), MonoNs(3)));
        // An update applied while A2 is in flight raises the bar to its key.
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(2, 2));
        rec.on_outcome(OrderOp::Amend, None, &refused(), MonoNs(4));
        assert_eq!(rec.resting(), lots(9), "A1 may rest");
        // A total stated under the bar's key may predate A1: it settles nothing.
        let mut open = update(VenueOrderState::Open, 0);
        (open.px, open.qty) = (Some(Ticks(100)), Some(lots(5)));
        assert_eq!(rec.apply_update(&open, keyed(2, 3)), Applied::Advanced);
        assert_eq!(rec.resting(), lots(9));
        // An update stating no total settles nothing.
        assert_eq!(
            rec.apply_update(&update(VenueOrderState::Open, 0), keyed(3, 4)),
            Applied::Advanced
        );
        assert_eq!(rec.resting(), lots(9));
        // Stated under a later key: A1 did not take.
        assert_eq!(rec.apply_update(&open, keyed(4, 5)), Applied::Advanced);
        assert_eq!(rec.resting(), lots(5));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Completed);
    }

    #[test]
    fn a_total_stated_after_an_amend_replaced_with_no_venue_key_applied_settles_nothing() {
        // No venue key was applied when A1 was replaced, so nothing shows a total is later.
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.amend_sent(Ticks(102), lots(7), RpcId(4), MonoNs(3)));
        rec.on_outcome(OrderOp::Amend, None, &refused(), MonoNs(4));
        let mut open = update(VenueOrderState::Open, 0);
        open.qty = Some(lots(5));
        rec.apply_update(&open, keyed(1, 1));
        assert_eq!(rec.resting(), lots(9));
        // The order's end retires it.
        rec.apply_update(&update(VenueOrderState::Filled, 9), keyed(2, 2));
        assert_eq!(rec.resting(), Lots::ZERO);
    }

    #[test]
    fn a_total_stated_while_an_amend_is_in_flight_settles_nothing_replaced_before_it() {
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.amend_sent(Ticks(102), lots(7), RpcId(4), MonoNs(3)));
        // Stating the original total while A2 is in flight: A1 may yet reach the venue.
        let mut open = update(VenueOrderState::Open, 0);
        open.qty = Some(lots(5));
        rec.apply_update(&open, key(1));
        rec.on_outcome(OrderOp::Amend, None, &refused(), MonoNs(4));
        assert_eq!(rec.resting(), lots(9));
    }

    #[test]
    fn a_cancel_sent_over_an_amend_in_flight_keeps_the_amends_total_counted() {
        let mut rec = order(5);
        rec.apply_update(&update(VenueOrderState::Open, 0), keyed(1, 1));
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.cancel_sent(RpcId(4), MonoNs(3)));
        assert_eq!(rec.resting(), lots(9));
        // Codex r4182907131: a total stated while the cancel is in flight may predate the
        // amend; it settles nothing, and five fills do not complete the order.
        let mut open = update(VenueOrderState::Open, 0);
        open.qty = Some(lots(5));
        rec.apply_update(&open, keyed(2, 2));
        assert_eq!(rec.resting(), lots(9));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        assert_eq!(rec.resting(), lots(4));
        // The cancel is refused; then the venue states the total under a later key than any
        // applied while the cancel was in flight: the amend is behind it.
        rec.on_outcome(OrderOp::Cancel, None, &refused(), MonoNs(4));
        rec.apply_update(&open, keyed(3, 3));
        assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
    }

    #[test]
    fn a_stale_total_after_a_refused_cancel_does_not_settle_the_amend_it_replaced() {
        // Codex r4182986303: the amend to nine is replaced by a cancel, the cancel is refused,
        // and a duplicate of the update from before the amend states five.
        let mut rec = order(5);
        let mut open = update(VenueOrderState::Open, 0);
        (open.px, open.qty) = (Some(Ticks(100)), Some(lots(5)));
        rec.apply_update(&open, keyed(1, 1));
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.cancel_sent(RpcId(4), MonoNs(3)));
        rec.on_outcome(OrderOp::Cancel, None, &refused(), MonoNs(4));
        assert_eq!(rec.apply_update(&open, keyed(1, 2)), Applied::Advanced);
        assert_eq!(rec.resting(), lots(9), "the amend may rest");
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        assert_eq!(rec.resting(), lots(4));
        // Nor does a total on a feed without venue ordering keys: it may be as old.
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(9), RpcId(3), MonoNs(2)));
        assert!(rec.cancel_sent(RpcId(4), MonoNs(3)));
        rec.on_outcome(OrderOp::Cancel, None, &refused(), MonoNs(4));
        rec.apply_update(&open, key(1));
        assert_eq!(rec.resting(), lots(9));
    }

    #[test]
    fn an_amend_to_a_smaller_total_in_flight_does_not_hold_a_covered_order_open() {
        let mut rec = order(6);
        assert!(rec.amend_sent(Ticks(101), lots(3), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(6)), FillApplied::Completed);
        assert_eq!(rec.intent(), Intent::None);
    }
}
