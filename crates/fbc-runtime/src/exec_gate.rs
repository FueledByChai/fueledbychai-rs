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
//! Where the venue's protection outlives a connection (`rearm_on_reconnect: false`), the first
//! accepted arm covers every later epoch, which still resyncs.

use fbc_core::{ExecEvent, RpcId, SubmitOutcome, VenueCommand};

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
}

/// The arm and resync of the latest authenticated epoch.
#[derive(Debug)]
pub(crate) struct Gate {
    /// The venue's protection lapses with each connection, so every epoch arms it.
    rearm: bool,
    /// An arm was accepted on some epoch.
    armed_once: bool,
    current: Option<Epoch>,
}

impl Gate {
    /// A gate for a venue whose protection is per connection, re-armed on each when `rearm`.
    pub(crate) fn new(rearm: bool) -> Gate {
        Gate {
            rearm,
            armed_once: false,
            current: None,
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
        let resync = Resync::Due;
        self.current = Some(Epoch { epoch, arm, resync });
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

    /// What `ev`, of `epoch`, settles once it reaches the handler: the answer to the epoch's
    /// arm, or the end of its resync; `None` for anything else, read without copying `ev`.
    pub(crate) fn settles(&self, epoch: u32, ev: &ExecEvent) -> Option<Settled> {
        let e = self.of(epoch)?;
        match ev {
            ExecEvent::Outcome { rpc, outcome, .. } if e.arm == Arm::Pending(*rpc) => {
                let accepted = matches!(outcome, SubmitOutcome::Accepted { .. });
                Some(Settled::Arm { accepted })
            }
            ExecEvent::ResyncEnd if e.resync == Resync::Asked => Some(Settled::Resync),
            _ => None,
        }
    }

    /// What an event of `epoch` settled has reached the handler.
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
        let ready = Epoch {
            epoch,
            arm: Arm::Accepted,
            resync: Resync::Ended,
        };
        self.of(epoch) == Some(ready)
    }

    /// Whether `cmd` may be sent on `epoch`: a place, a batch of places or an amend only once
    /// the epoch is [placing](Gate::placing); anything else always.
    pub(crate) fn admits(&self, cmd: &VenueCommand, epoch: u32) -> bool {
        let adds = matches!(
            cmd,
            VenueCommand::Place(_) | VenueCommand::PlaceBatch(_) | VenueCommand::Amend(_)
        );
        !adds || self.placing(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{
        AccountKey, AckLevel, AmendOrder, CancelOrder, CancelScope, Channel, CidMint,
        ClientOrderId, InstrumentId, ItemRef, Lots, Namespace, NamespaceLease, NewOrder, OrderKind,
        OrderRef, QueryOrder, Reject, RejectKind, Side, Ticks, Tif, WallNs,
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
            .map(|(cmd, adds)| (gate.admits(&cmd, epoch), adds))
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
        let mut gate = Gate::new(true);
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

    #[test]
    fn a_resync_ended_before_it_was_asked_for_on_the_epoch_does_not_count() {
        let mut gate = Gate::new(true);
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
            let mut gate = Gate::new(true);
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
        let mut gate = Gate::new(true);
        gate.timed_out(RpcId(1));
        assert!(!gate.failed(0));
    }

    #[test]
    fn protection_that_outlives_a_connection_is_armed_once_accepted_and_each_epoch_resyncs() {
        let mut gate = Gate::new(false);
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
