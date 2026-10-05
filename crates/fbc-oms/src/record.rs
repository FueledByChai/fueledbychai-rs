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
    /// flight; Filled if its fills cover its total once no larger amend is in flight.
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

    /// The quantity still resting: nothing once terminal; otherwise the total less the filled
    /// part, so a PendingNew or Unknown order counts as fully resting.
    pub fn resting(&self) -> Lots {
        if self.state.is_terminal() {
            Lots::ZERO
        } else {
            self.qty.checked_sub(self.filled()).unwrap_or(Lots::ZERO)
        }
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
    /// Open, or PartiallyFilled once something is filled; an amended update moves no state,
    /// and resolves an amend in flight, whose price and total stand where the update does not
    /// state them. Either ends the order Filled when its fills alone then cover its total with no amend to
    /// a larger total in flight.
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
            if let Intent::PendingAmend { px, qty, .. } = self.intent {
                // The venue confirmed the amend in flight: what it does not echo is what was
                // sent.
                self.px = Some(u.px.unwrap_or(px));
                self.qty = u.qty.unwrap_or(qty);
                self.intent = Intent::None;
            }
            self.complete_if_covered();
            return Applied::Amended;
        }
        if self.state == OrdState::Unknown {
            self.unknown_since = None;
        }
        self.state = self.live_state();
        if let Intent::PendingAmend { px, qty, .. } = self.intent
            && self.px == Some(px)
            && self.qty == qty
        {
            self.intent = Intent::None;
        }
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
    /// promoted; and the order is Filled only when the fills alone cover its total and no amend
    /// to a larger total is in flight, never on the venue's cumulative count alone.
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

    /// Ends a live order Filled when its fills alone cover its total and no amend to a larger
    /// total is in flight; whether it did. Checked whenever the fills, the total or the amend
    /// in flight change.
    fn complete_if_covered(&mut self) -> bool {
        let growing =
            matches!(self.intent, Intent::PendingAmend { qty, .. } if qty > self.cum_fills);
        let covered = !self.state.is_terminal() && self.cum_fills >= self.qty && !growing;
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
        assert_eq!(rec.resting(), Lots::ZERO);
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
        amended.qty = Some(lots(8));
        rec.apply_update(&amended, key(1));
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        assert_eq!(rec.resting(), lots(3));
        // A later update reports a total the fills already cover.
        let mut open = update(VenueOrderState::Open, 5);
        open.qty = Some(lots(5));
        rec.apply_update(&open, key(2));
        assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
    }

    #[test]
    fn an_amend_confirmed_without_its_total_takes_the_total_that_was_sent() {
        let mut rec = order(5);
        assert!(rec.amend_sent(Ticks(101), lots(8), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(5)), FillApplied::Live);
        // The venue confirms the amend but echoes neither price nor total.
        let amended = update(VenueOrderState::Amended { new_vid: None }, 5);
        assert_eq!(rec.apply_update(&amended, key(1)), Applied::Amended);
        assert_eq!(rec.state(), OrdState::PartiallyFilled);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(8)));
        assert_eq!(rec.resting(), lots(3));
        assert_eq!(rec.intent(), Intent::None);
    }

    #[test]
    fn an_amend_to_a_smaller_total_in_flight_does_not_hold_a_covered_order_open() {
        let mut rec = order(6);
        assert!(rec.amend_sent(Ticks(101), lots(3), RpcId(3), MonoNs(2)));
        assert_eq!(rec.apply_fill(None, None, lots(6)), FillApplied::Completed);
        assert_eq!(rec.intent(), Intent::None);
    }
}
