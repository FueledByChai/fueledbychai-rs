//! Whether an order-entry session's epoch takes places and amends yet (FBC-w19, decision 0058).
//!
//! An epoch whose stream the codec reported authenticated owes two things before it places or
//! amends anything: the venue's acceptance of `ArmCancelOnDisconnect(true)`, which the session
//! sends then, so every order it rests is cancelled if the connection drops, and the end of the
//! codec's resync, which the session asks for then, so fbc-oms has applied the venue's open
//! orders and positions before it adds to them (0013 rule 1). Until both, [`Gate::admits`]
//! refuses every place, batch of places and amend; a cancel, a cancel-many, an instrument
//! cancel-all and every control command go through. An arm the venue rejects, that is not sent,
//! or that is unanswered at its deadline fails the epoch, which the session then ends as a drop.
//! Only a final acceptance accepts the arm: a two-phase venue's provisional one leaves it
//! pending under its deadline, which still stands, since the venue may yet reject it.
//! Where the venue's protection outlives a connection (`rearm_on_reconnect: false`), the first
//! accepted arm covers every later epoch, which still resyncs.
//!
//! **Orders resting from an earlier epoch (FBC-nvxn, decision 0080).** An arm may protect only
//! the orders placed on its connection after it. Unless the venue declares that an accepted arm
//! also covers the orders already open (`covers_open_orders`), every order of ours
//! ([`CidMatch::Ours`]) that the resync of an epoch whose arm was sent on it shows resting was
//! placed on an earlier epoch, since the epoch holds every place until its resync has ended, and
//! is unprotected. Once the epoch is [placing](Gate::placing), [`Gate::admits`] still refuses a
//! place, a batch with an item, or an amend on the market of such an order, until an order event
//! or an order query's answer of the epoch shows it ended (filled, cancelled, rejected or
//! expired); an amend the venue reports moves it to its new venue id. The consumer is told them
//! once ([`Gate::notice`]) and cancels them through fbc-oms's authorizations; nothing else
//! releases a market before the next epoch's resync reads the venue again.

use fbc_core::{
    AckLevel, CidMatch, ExecEvent, InstrumentId, RpcId, SubmitOutcome, VenueCommand, VenueOrderId,
    VenueOrderSnapshot, VenueOrderState,
};

/// What an event settles for its epoch once it reaches the handler.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Settled {
    /// The venue answered the arm: accepted, or not (rejected, or `Unknown`).
    Arm { accepted: bool },
    /// The resync ended.
    Resync,
}

/// Where an epoch's arm stands.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Arm {
    /// To be sent.
    Due,
    /// Sent as this request, not yet answered.
    Pending(RpcId),
    Accepted,
    /// Rejected, not sent, or unanswered at its deadline.
    Failed,
}

/// Where an epoch's resync stands.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Resync {
    Due,
    Asked,
    /// Its `ResyncEnd` reached the handler.
    Ended,
}

/// One authenticated epoch's arm and resync.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Epoch {
    epoch: u32,
    arm: Arm,
    resync: Resync,
    /// Its arm is sent on it and covers only what is placed after it, so the orders of ours
    /// its resync shows resting are unprotected.
    checks: bool,
    /// The consumer was told the unprotected orders.
    told: bool,
}

/// Why [`Gate::admits`] holds a command.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Hold {
    /// The epoch is not yet armed and resynced.
    Unready,
    /// An order of ours on its market rests unprotected from an earlier epoch.
    Unprotected,
}

/// The arm and resync of the latest authenticated epoch.
#[derive(Debug)]
pub(crate) struct Gate {
    /// The venue's protection lapses with each connection, so every epoch arms it.
    rearm: bool,
    /// An accepted arm also covers the orders already open when it is accepted.
    covers_open: bool,
    /// An arm was accepted on some epoch.
    armed_once: bool,
    current: Option<Epoch>,
    /// The orders of ours the latest epoch's resync showed resting, while no event of the
    /// epoch has shown them ended.
    unprotected: Vec<VenueOrderSnapshot>,
}

impl Gate {
    /// A gate for a venue whose protection is per connection, re-armed on each when `rearm`,
    /// an accepted arm covering the orders already open when `covers_open`.
    pub(crate) fn new(rearm: bool, covers_open: bool) -> Gate {
        Gate {
            rearm,
            covers_open,
            armed_once: false,
            current: None,
            unprotected: Vec::new(),
        }
    }

    /// The state of `epoch`, if it is the latest authenticated one.
    fn of(&self, epoch: u32) -> Option<Epoch> {
        self.current.filter(|e| e.epoch == epoch)
    }

    fn update(&mut self, epoch: u32, f: impl FnOnce(&mut Epoch)) {
        if let Some(e) = self.current.as_mut().filter(|e| e.epoch == epoch) {
            f(e);
        }
    }

    /// The codec reported `epoch`'s stream authenticated: its arm (unless an earlier one still
    /// covers it) and its resync are due, once per epoch.
    pub(crate) fn authenticated(&mut self, epoch: u32) {
        if self.of(epoch).is_some() {
            return;
        }
        let arm = if self.rearm || !self.armed_once {
            Arm::Due
        } else {
            Arm::Accepted
        };
        self.current = Some(Epoch {
            epoch,
            arm,
            resync: Resync::Due,
            checks: arm == Arm::Due && !self.covers_open,
            told: false,
        });
        self.unprotected.clear();
    }

    /// Whether `epoch`'s arm is to be sent.
    pub(crate) fn arm_due(&self, epoch: u32) -> bool {
        self.of(epoch).is_some_and(|e| e.arm == Arm::Due)
    }

    /// Whether `epoch`'s resync is to be asked for.
    pub(crate) fn resync_due(&self, epoch: u32) -> bool {
        self.of(epoch).is_some_and(|e| e.resync == Resync::Due)
    }

    /// `epoch`'s arm was written as request `rpc`.
    pub(crate) fn arm_sent(&mut self, epoch: u32, rpc: RpcId) {
        self.update(epoch, |e| e.arm = Arm::Pending(rpc));
    }

    /// `epoch`'s arm could not be sent.
    pub(crate) fn arm_not_sent(&mut self, epoch: u32) {
        self.update(epoch, |e| e.arm = Arm::Failed);
    }

    /// `epoch`'s resync was asked for.
    pub(crate) fn resync_asked(&mut self, epoch: u32) {
        self.update(epoch, |e| e.resync = Resync::Asked);
    }

    /// What `ev`, of `epoch`, settles as it is handed to the handler: the answer to the epoch's
    /// arm, or the end of its resync; `None` for anything else, a provisional acceptance of the
    /// arm included, read without copying `ev`.
    pub(crate) fn settles(&self, epoch: u32, ev: &ExecEvent) -> Option<Settled> {
        let e = self.of(epoch)?;
        match ev {
            ExecEvent::Outcome { rpc, outcome, .. } if e.arm == Arm::Pending(*rpc) => {
                let accepted = match outcome {
                    SubmitOutcome::Accepted {
                        ack: AckLevel::Provisional,
                    } => return None,
                    SubmitOutcome::Accepted {
                        ack: AckLevel::Final,
                    } => true,
                    _ => false,
                };
                Some(Settled::Arm { accepted })
            }
            ExecEvent::ResyncEnd if e.resync == Resync::Asked => Some(Settled::Resync),
            _ => None,
        }
    }

    /// Whether `ev` provisionally accepts the pending arm: it does not answer the arm, whose
    /// deadline still stands until the venue's final answer (PR #90 Reviewer A A2).
    pub(crate) fn provisional_arm(&self, ev: &ExecEvent) -> bool {
        let provisional = SubmitOutcome::Accepted {
            ack: AckLevel::Provisional,
        };
        match (ev, self.current) {
            (ExecEvent::Outcome { rpc, outcome, .. }, Some(e)) => {
                e.arm == Arm::Pending(*rpc) && *outcome == provisional
            }
            _ => false,
        }
    }

    /// What an event of `epoch` settled is being handed to the handler.
    pub(crate) fn settle(&mut self, epoch: u32, settled: Settled) {
        match settled {
            Settled::Arm { accepted } => {
                self.armed_once |= accepted;
                let arm = if accepted { Arm::Accepted } else { Arm::Failed };
                self.update(epoch, |e| e.arm = arm);
            }
            Settled::Resync => self.update(epoch, |e| e.resync = Resync::Ended),
        }
    }

    /// Request `rpc`'s deadline fell due unanswered: when it is the latest epoch's arm, that
    /// arm failed, whatever the codec reports for it.
    pub(crate) fn timed_out(&mut self, rpc: RpcId) {
        if let Some(e) = self.current.as_mut()
            && e.arm == Arm::Pending(rpc)
        {
            e.arm = Arm::Failed;
        }
    }

    /// Whether `epoch`'s arm failed, so the epoch is to end.
    pub(crate) fn failed(&self, epoch: u32) -> bool {
        self.of(epoch).is_some_and(|e| e.arm == Arm::Failed)
    }

    /// Whether `epoch` takes places and amends: its arm accepted and its resync ended.
    pub(crate) fn placing(&self, epoch: u32) -> bool {
        self.of(epoch)
            .is_some_and(|e| e.arm == Arm::Accepted && e.resync == Resync::Ended)
    }

    /// Whether an order of ours on `inst` rests unprotected from an earlier epoch.
    pub(crate) fn unprotected_on(&self, inst: InstrumentId) -> bool {
        self.unprotected.iter().any(|o| o.inst == inst)
    }

    /// The orders of ours resting unprotected from an earlier epoch, while no event of the
    /// latest epoch has shown them ended.
    pub(crate) fn unprotected(&self) -> &[VenueOrderSnapshot] {
        &self.unprotected
    }

    /// Whether `cmd` may be sent on `epoch`, or why it is held: a place, a batch of places or
    /// an amend only once the epoch is [placing](Gate::placing) and no order of ours on its
    /// market (on any item's, for a batch) rests unprotected; anything else always.
    pub(crate) fn admits(&self, cmd: &VenueCommand, epoch: u32) -> Result<(), Hold> {
        let insts: Vec<InstrumentId> = match cmd {
            VenueCommand::Place(o) => vec![o.inst],
            VenueCommand::PlaceBatch(orders) => orders.iter().map(|o| o.inst).collect(),
            VenueCommand::Amend(a) => vec![a.inst],
            _ => return Ok(()),
        };
        if !self.placing(epoch) {
            Err(Hold::Unready)
        } else if insts.into_iter().any(|inst| self.unprotected_on(inst)) {
            Err(Hold::Unprotected)
        } else {
            Ok(())
        }
    }

    /// What `ev`, of `epoch`, shows of the orders resting from an earlier epoch, as it is
    /// handed to the handler: an order of ours the resync asked for on an epoch that checks
    /// shows resting is unprotected; an order event or an order query's answer showing one
    /// ended releases it, and one showing it amended under a new venue id moves it there.
    pub(crate) fn observe(&mut self, epoch: u32, ev: &ExecEvent) {
        let Some(e) = self.of(epoch) else {
            return;
        };
        match ev {
            ExecEvent::ResyncOrder(snap)
                if e.checks
                    && e.resync == Resync::Asked
                    && snap.state == VenueOrderState::Open
                    && matches!(snap.cid, Some(CidMatch::Ours(_))) =>
            {
                self.unprotected.retain(|o| o.vid != snap.vid);
                self.unprotected.push(snap.clone());
            }
            ExecEvent::Order(u) => self.shown(u.cid, u.vid.as_ref(), &u.state),
            ExecEvent::QueryResult(answer) => {
                if let Some(found) = answer.found() {
                    self.shown(found.cid, Some(&found.vid), &found.state);
                }
            }
            _ => {}
        }
    }

    /// An event of the latest epoch showed the order of ours `cid`, or `vid`, in `state`.
    fn shown(
        &mut self,
        cid: Option<CidMatch>,
        vid: Option<&VenueOrderId>,
        state: &VenueOrderState,
    ) {
        let ours = cid.filter(|c| matches!(c, CidMatch::Ours(_)));
        let names =
            |o: &VenueOrderSnapshot| vid == Some(&o.vid) || (ours.is_some() && ours == o.cid);
        match state {
            VenueOrderState::Open | VenueOrderState::Amended { new_vid: None } => {}
            VenueOrderState::Amended {
                new_vid: Some(new_vid),
            } => self
                .unprotected
                .iter_mut()
                .filter(|o| names(o))
                .for_each(|o| o.vid = new_vid.clone()),
            VenueOrderState::Filled
            | VenueOrderState::Canceled(_)
            | VenueOrderState::Rejected(_)
            | VenueOrderState::Expired => self.unprotected.retain(|o| !names(o)),
        }
    }

    /// The orders of ours resting unprotected on `epoch`, once, when it has just become
    /// placing and there are any: the consumer is told them so it cancels them.
    pub(crate) fn notice(&mut self, epoch: u32) -> Option<Vec<VenueOrderSnapshot>> {
        let due = self.placing(epoch) && !self.unprotected.is_empty();
        let e = self
            .current
            .as_mut()
            .filter(|e| e.epoch == epoch && due && !e.told)?;
        e.told = true;
        Some(self.unprotected.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{
        AccountKey, AckLevel, AmendOrder, CancelOrder, CancelReason, CancelScope, Channel, CidMint,
        ClientOrderId, ItemRef, Lots, Namespace, NamespaceLease, NewOrder, OrderKind, OrderRef,
        OrderUpdate, QueryAnswer, QueryOrder, Reject, RejectKind, Side, TerminalReject, Ticks, Tif,
        WallNs,
    };

    const INST: InstrumentId = InstrumentId::new(7);

    /// One of our client ids, minted once under a namespace lease held in a directory of its
    /// own.
    fn cid() -> ClientOrderId {
        static CID: std::sync::OnceLock<ClientOrderId> = std::sync::OnceLock::new();
        *CID.get_or_init(|| {
            let name = format!("fbc-runtime-gate-{}", std::process::id());
            let dir = std::env::temp_dir().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let ns = Namespace::new(5);
            let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), ns).unwrap();
            let cid = CidMint::new(lease, 0, 0, WallNs(1)).mint().unwrap();
            let _ = std::fs::remove_dir_all(&dir);
            cid
        })
    }

    fn order(cid: ClientOrderId) -> NewOrder {
        NewOrder {
            cid,
            inst: INST,
            side: Side::Buy,
            kind: OrderKind::Limit { px: Ticks(10) },
            qty: Lots::new(1).unwrap(),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        }
    }

    fn cancel(cid: ClientOrderId) -> CancelOrder {
        CancelOrder {
            target: OrderRef::Client(cid),
            inst: INST,
            side: Side::Buy,
            placement_nonce: None,
        }
    }

    /// Every command kind, with whether it adds an order: a place, a batch or an amend.
    fn commands() -> Vec<(VenueCommand, bool)> {
        let cid = cid();
        let amend = AmendOrder {
            target: OrderRef::Client(cid),
            inst: INST,
            side: Side::Buy,
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(11),
            qty: Lots::new(1).unwrap(),
            cum_filled: Lots::ZERO,
        };
        let query = QueryOrder {
            target: OrderRef::Client(cid),
            inst: INST,
            placement_nonce: None,
        };
        vec![
            (VenueCommand::Place(order(cid)), true),
            (VenueCommand::PlaceBatch(vec![order(cid), order(cid)]), true),
            (VenueCommand::Amend(amend), true),
            (VenueCommand::Cancel(cancel(cid)), false),
            (
                VenueCommand::CancelMany(vec![cancel(cid), cancel(cid)]),
                false,
            ),
            (
                VenueCommand::CancelAll(CancelScope::Instrument(INST)),
                false,
            ),
            (VenueCommand::ArmCancelOnDisconnect(true), false),
            (VenueCommand::RefreshDeadMan, false),
            (VenueCommand::Query(query), false),
            (VenueCommand::FeeQuery, false),
        ]
    }

    /// Whether `gate` admits on `epoch` exactly what adds no order, or everything.
    fn admits_all(gate: &Gate, epoch: u32) -> Option<bool> {
        let admitted: Vec<_> = commands()
            .into_iter()
            .map(|(cmd, adds)| (gate.admits(&cmd, epoch).is_ok(), adds))
            .collect();
        if admitted.iter().all(|(admits, _)| *admits) {
            Some(true)
        } else if admitted.iter().all(|(admits, adds)| *admits != *adds) {
            Some(false)
        } else {
            None
        }
    }

    impl Gate {
        /// `ev`, of `epoch`, reached the handler.
        fn heard(&mut self, epoch: u32, ev: &ExecEvent) {
            if let Some(settled) = self.settles(epoch, ev) {
                self.settle(epoch, settled);
            }
        }
    }

    fn outcome(rpc: u64, outcome: SubmitOutcome) -> ExecEvent {
        let item = Some(ItemRef {
            idx: 0,
            cid: None,
            vid: None,
        });
        ExecEvent::Outcome {
            rpc: RpcId(rpc),
            item,
            outcome,
        }
    }

    fn accepted(rpc: u64) -> ExecEvent {
        let ack = AckLevel::Final;
        outcome(rpc, SubmitOutcome::Accepted { ack })
    }

    #[test]
    fn places_and_amends_wait_for_the_epochs_arm_and_resync_and_nothing_else_waits() {
        let mut gate = Gate::new(true, false);
        // Before any authentication only what adds no order goes.
        assert_eq!(admits_all(&gate, 0), Some(false));
        gate.authenticated(0);
        assert!(gate.arm_due(0) && gate.resync_due(0));
        assert_eq!(admits_all(&gate, 0), Some(false));
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        assert!(!gate.arm_due(0) && !gate.resync_due(0));
        // Another request's answer, an end of no resync of this epoch's, and an event of another
        // epoch change nothing.
        gate.heard(0, &accepted(2));
        gate.heard(1, &accepted(1));
        gate.heard(1, &ExecEvent::ResyncEnd);
        assert_eq!(admits_all(&gate, 0), Some(false));
        gate.heard(0, &accepted(1));
        assert_eq!(admits_all(&gate, 0), Some(false));
        gate.heard(0, &ExecEvent::ResyncEnd);
        assert!(gate.placing(0));
        assert_eq!(admits_all(&gate, 0), Some(true));
        // A second Authenticated on the epoch changes nothing; another epoch is not placing.
        gate.authenticated(0);
        assert!(gate.placing(0));
        assert_eq!(admits_all(&gate, 1), Some(false));
        // The next epoch arms again and resyncs again.
        gate.authenticated(1);
        assert!(gate.arm_due(1) && gate.resync_due(1));
        assert_eq!(admits_all(&gate, 0), Some(false));
        assert_eq!(admits_all(&gate, 1), Some(false));
    }

    /// The resync may end before the venue answers the arm (the order PR #119's CI run saw):
    /// the epoch still holds every place and amend until the arm's final acceptance, which then
    /// opens it.
    #[test]
    fn a_resync_that_ends_before_the_arm_is_answered_holds_places_until_the_arm_is_accepted() {
        let mut gate = Gate::new(true, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        gate.heard(0, &ExecEvent::ResyncEnd);
        assert!(!gate.placing(0));
        assert_eq!(admits_all(&gate, 0), Some(false));
        gate.heard(0, &accepted(1));
        assert!(gate.placing(0));
        assert_eq!(admits_all(&gate, 0), Some(true));
    }

    #[test]
    fn a_resync_ended_before_it_was_asked_for_on_the_epoch_does_not_count() {
        let mut gate = Gate::new(true, false);
        gate.authenticated(0);
        gate.heard(0, &ExecEvent::ResyncEnd);
        gate.arm_sent(0, RpcId(1));
        gate.heard(0, &accepted(1));
        assert!(!gate.placing(0));
        assert!(gate.resync_due(0));
    }

    #[test]
    fn an_arm_rejected_unanswered_or_not_sent_fails_the_epoch_for_good() {
        let reject = Reject {
            kind: RejectKind::Margin,
            venue_code: None,
            raw: "".into(),
        };
        let failures: [&dyn Fn(&mut Gate); 4] = [
            &|g| g.heard(0, &outcome(1, SubmitOutcome::Rejected(reject.clone()))),
            &|g| g.heard(0, &outcome(1, SubmitOutcome::Unknown)),
            &|g| g.timed_out(RpcId(1)),
            &|g| g.arm_not_sent(0),
        ];
        for fail in failures {
            let mut gate = Gate::new(true, false);
            gate.authenticated(0);
            assert!(!gate.failed(0));
            gate.arm_sent(0, RpcId(1));
            gate.resync_asked(0);
            gate.timed_out(RpcId(2));
            assert!(!gate.failed(0));
            fail(&mut gate);
            assert!(gate.failed(0));
            // Neither a late acceptance nor the resync's end makes it place.
            gate.heard(0, &accepted(1));
            gate.heard(0, &ExecEvent::ResyncEnd);
            assert!(gate.failed(0));
            assert_eq!(admits_all(&gate, 0), Some(false));
            // Another epoch is not failed.
            assert!(!gate.failed(1));
        }
        // A deadline with no epoch authenticated fails nothing.
        let mut gate = Gate::new(true, false);
        gate.timed_out(RpcId(1));
        assert!(!gate.failed(0));
    }

    #[test]
    fn a_provisional_acceptance_leaves_the_arm_pending_until_its_final_answer_or_deadline() {
        let provisional = outcome(
            1,
            SubmitOutcome::Accepted {
                ack: AckLevel::Provisional,
            },
        );
        let reject = Reject {
            kind: RejectKind::Margin,
            venue_code: None,
            raw: "".into(),
        };
        let rejected = outcome(1, SubmitOutcome::Rejected(reject));
        // Each answer that may follow it, and whether the epoch then takes places.
        type Then<'a> = (&'a dyn Fn(&mut Gate), bool);
        let after: [Then<'_>; 3] = [
            (&|g| g.heard(0, &accepted(1)), true),
            (&|g| g.heard(0, &rejected), false),
            (&|g| g.timed_out(RpcId(1)), false),
        ];
        for (answer, placing) in after {
            let mut gate = Gate::new(true, false);
            gate.authenticated(0);
            assert!(!gate.provisional_arm(&provisional));
            gate.arm_sent(0, RpcId(1));
            gate.resync_asked(0);
            gate.heard(0, &ExecEvent::ResyncEnd);
            assert!(gate.provisional_arm(&provisional));
            assert!(!gate.provisional_arm(&accepted(1)));
            assert!(!gate.provisional_arm(&ExecEvent::ResyncEnd));
            gate.heard(0, &provisional);
            assert!(!gate.placing(0) && !gate.failed(0));
            assert!(gate.provisional_arm(&provisional));
            answer(&mut gate);
            assert_eq!((gate.placing(0), gate.failed(0)), (placing, !placing));
            assert!(!gate.provisional_arm(&provisional));
        }
    }

    // -----------------------------------------------------------------------------------------
    // Orders resting from an earlier epoch (FBC-nvxn).
    // -----------------------------------------------------------------------------------------

    const OTHER: InstrumentId = InstrumentId::new(8);

    fn vid(wire: &str) -> VenueOrderId {
        crate::toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
    }

    /// An order on `inst` under `wire` with client id `cid`, in `state`.
    fn snap(
        cid: Option<CidMatch>,
        wire: &str,
        inst: InstrumentId,
        state: VenueOrderState,
    ) -> VenueOrderSnapshot {
        VenueOrderSnapshot {
            cid,
            vid: vid(wire),
            inst,
            side: Side::Buy,
            state,
            px: Some(Ticks(10)),
            qty: Lots::new(2).unwrap(),
            cum_filled: Lots::ZERO,
            post_only: None,
            reduce_only: None,
        }
    }

    fn ours(wire: &str) -> VenueOrderSnapshot {
        snap(
            Some(CidMatch::Ours(cid())),
            wire,
            INST,
            VenueOrderState::Open,
        )
    }

    /// An order event for `cid` or `wire` in `state`.
    fn update(cid: Option<CidMatch>, wire: Option<&str>, state: VenueOrderState) -> ExecEvent {
        ExecEvent::Order(OrderUpdate {
            cid,
            vid: wire.map(vid),
            inst: INST,
            side: Side::Buy,
            state,
            cum_filled: Lots::ZERO,
            px: None,
            qty: None,
            post_only: None,
            reduce_only: None,
        })
    }

    /// A gate whose epoch 0 is armed (request 1) and resynced, its resync showing `shown`.
    fn resynced(gate: &mut Gate, shown: &[VenueOrderSnapshot]) {
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        for order in shown {
            gate.observe(0, &ExecEvent::ResyncOrder(order.clone()));
        }
        gate.heard(0, &accepted(1));
        gate.heard(0, &ExecEvent::ResyncEnd);
        assert!(gate.placing(0));
    }

    fn place_on(inst: InstrumentId) -> VenueCommand {
        VenueCommand::Place(NewOrder {
            inst,
            ..order(cid())
        })
    }

    #[test]
    fn an_order_of_ours_a_resync_shows_resting_holds_its_market_until_an_event_shows_it_ended() {
        let mut gate = Gate::new(true, false);
        let foreign = Some(CidMatch::Foreign(Namespace::new(9)));
        let not_ours = [
            snap(foreign, "F-1", OTHER, VenueOrderState::Open),
            snap(
                Some(CidMatch::Unparseable),
                "F-2",
                OTHER,
                VenueOrderState::Open,
            ),
            snap(None, "F-3", OTHER, VenueOrderState::Open),
            snap(
                Some(CidMatch::Ours(cid())),
                "F-4",
                OTHER,
                VenueOrderState::Filled,
            ),
        ];
        let mut shown = not_ours.to_vec();
        // Shown twice: it is held once.
        shown.extend([ours("V-1"), ours("V-1")]);
        resynced(&mut gate, &shown);
        assert_eq!(gate.unprotected(), [ours("V-1")]);
        // The consumer is told once.
        assert_eq!(gate.notice(0), Some(vec![ours("V-1")]));
        assert_eq!(gate.notice(0), None);
        assert_eq!(gate.notice(1), None);
        // Its market takes no place, batch with an item on it, or amend; another market does,
        // and every command that adds no order goes.
        assert_eq!(gate.admits(&place_on(INST), 0), Err(Hold::Unprotected));
        assert_eq!(gate.admits(&place_on(OTHER), 0), Ok(()));
        let batch = VenueCommand::PlaceBatch(vec![
            order(cid()),
            NewOrder {
                inst: OTHER,
                ..order(cid())
            },
        ]);
        assert_eq!(gate.admits(&batch, 0), Err(Hold::Unprotected));
        for (cmd, adds) in commands() {
            let held = gate.admits(&cmd, 0).is_err();
            assert_eq!(held, adds, "{cmd:?}");
        }
        // On another epoch, the epoch is not ready first.
        assert_eq!(gate.admits(&place_on(INST), 1), Err(Hold::Unready));
        // An amend under a new venue id moves it there: an end shown under the old id, an event
        // still showing it open, an amend naming no new id, or an event of another epoch,
        // releases nothing.
        let mine = Some(CidMatch::Ours(cid()));
        let amended = VenueOrderState::Amended {
            new_vid: Some(vid("V-9")),
        };
        gate.observe(0, &update(None, Some("V-1"), amended));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(None, Some("V-1"), cancelled.clone()));
        gate.observe(0, &update(mine, Some("V-9"), VenueOrderState::Open));
        let unnamed = VenueOrderState::Amended { new_vid: None };
        gate.observe(0, &update(mine, Some("V-9"), unnamed));
        gate.observe(1, &update(mine, Some("V-9"), cancelled));
        assert_eq!(gate.unprotected().len(), 1);
        assert_eq!(gate.unprotected()[0].vid, vid("V-9"));
        // An order query's answer showing it filled releases it.
        let missing = QueryAnswer::new(RpcId(7), OrderRef::Venue(vid("V-9")), None).unwrap();
        gate.observe(0, &ExecEvent::QueryResult(missing));
        assert!(gate.unprotected_on(INST));
        let filled = snap(mine, "V-9", INST, VenueOrderState::Filled);
        let answer = QueryAnswer::new(RpcId(7), OrderRef::Venue(vid("V-9")), Some(filled));
        gate.observe(0, &ExecEvent::QueryResult(answer.unwrap()));
        assert!(gate.unprotected().is_empty());
        assert_eq!(gate.admits(&place_on(INST), 0), Ok(()));
        // Told once, and nothing is left to tell.
        assert_eq!(gate.notice(0), None);
    }

    #[test]
    fn an_order_event_naming_only_our_client_id_ends_it_and_each_end_state_releases() {
        let mine = Some(CidMatch::Ours(cid()));
        let ends = [
            VenueOrderState::Filled,
            VenueOrderState::Canceled(CancelReason::Disconnect),
            VenueOrderState::Rejected(TerminalReject::new(RejectKind::Margin).unwrap()),
            VenueOrderState::Expired,
        ];
        for end in ends {
            let mut gate = Gate::new(true, false);
            resynced(&mut gate, &[ours("V-1")]);
            // Another namespace's order event under no venue id names nothing of ours.
            let foreign = Some(CidMatch::Foreign(Namespace::new(9)));
            gate.observe(0, &update(foreign, None, end.clone()));
            assert!(gate.unprotected_on(INST));
            gate.observe(0, &update(mine, None, end));
            assert!(!gate.unprotected_on(INST));
        }
    }

    #[test]
    fn nothing_is_unprotected_where_the_arm_covers_open_orders_or_was_not_sent_on_the_epoch() {
        // The venue's arm covers the orders already open.
        let mut gate = Gate::new(true, true);
        resynced(&mut gate, &[ours("V-1")]);
        assert!(gate.unprotected().is_empty());
        assert_eq!(gate.notice(0), None);
        // Protection that outlives the connection, accepted on epoch 0: epoch 1 arms nothing,
        // and what its resync shows was placed under that arm.
        let mut gate = Gate::new(false, false);
        resynced(&mut gate, &[]);
        gate.authenticated(1);
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.heard(1, &ExecEvent::ResyncEnd);
        assert!(gate.placing(1));
        assert!(gate.unprotected().is_empty());
        // An order shown before the epoch's resync was asked for, or by another epoch, counts
        // for nothing.
        let mut gate = Gate::new(true, false);
        gate.authenticated(0);
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        assert!(gate.unprotected().is_empty());
    }

    #[test]
    fn the_next_epoch_reads_its_own_resync_and_holds_nothing_until_it_is_placing() {
        let mut gate = Gate::new(true, false);
        resynced(&mut gate, &[ours("V-1")]);
        assert!(gate.unprotected_on(INST));
        gate.authenticated(1);
        assert!(gate.unprotected().is_empty());
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-2")));
        // Not placing yet: the place waits for the arm, and the consumer is told nothing.
        assert_eq!(gate.admits(&place_on(INST), 1), Err(Hold::Unready));
        assert_eq!(gate.notice(1), None);
        gate.heard(1, &ExecEvent::ResyncEnd);
        gate.heard(1, &accepted(2));
        assert_eq!(gate.notice(1), Some(vec![ours("V-2")]));
    }

    #[test]
    fn protection_that_outlives_a_connection_is_armed_once_accepted_and_each_epoch_resyncs() {
        let mut gate = Gate::new(false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.heard(0, &outcome(1, SubmitOutcome::Unknown));
        // Not accepted: the next epoch arms again.
        gate.authenticated(1);
        assert!(gate.arm_due(1));
        gate.arm_sent(1, RpcId(2));
        gate.heard(1, &accepted(2));
        gate.resync_asked(1);
        gate.heard(1, &ExecEvent::ResyncEnd);
        assert!(gate.placing(1));
        // Accepted once: later epochs only resync.
        gate.authenticated(2);
        assert!(!gate.arm_due(2));
        assert!(gate.resync_due(2));
        assert!(!gate.placing(2));
        gate.resync_asked(2);
        gate.heard(2, &ExecEvent::ResyncEnd);
        assert!(gate.placing(2));
    }
}
