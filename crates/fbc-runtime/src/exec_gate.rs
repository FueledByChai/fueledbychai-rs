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
//! place, a batch with an item, or an amend on the market of such an order until it has ended
//! under every venue id it was seen under. For each, the gate keeps the venue ids it was seen
//! under (the snapshot's, each id an amend moved it to, each id an event or a query answer
//! naming our client id showed) and those an order event or a query answer showed ended
//! (filled, cancelled, rejected or expired) or replaced by an amend. No evidence removes an id,
//! so the order in which it arrives does not matter (Reviewer B RB-nvxn-4 on PR #127). Evidence
//! naming only our client id adds the id it shows and ends nothing, since it may report an order
//! an amend superseded, unless the venue's amends keep the venue id: our client id then names
//! one venue order, and an end naming it ends the order. What events and query answers showed
//! of venue ids is kept by market: while a resync runs, for every market, and after it, for the
//! markets an order is held on, so what was heard before the snapshot, or before the amend that
//! links an id to the order, is applied when they arrive. A market's is dropped once no order is
//! held on it as a resync ends or as its last held order is released after the resync, so an
//! order held for good keeps only its own market's (Reviewer B RB-nvxn-5 on PR #127, FBC-48j7,
//! decision 0088). What an epoch that drops before its resync ends heard is kept until a later
//! epoch's resync ends, since that epoch's snapshot may still need it (Reviewer B RB-48j7-1 on
//! PR #140). The consumer
//! is told the orders once per epoch ([`Gate::notice`]) and cancels them through fbc-oms's
//! authorizations, or queries one its registry holds ended. A later epoch keeps every order an
//! earlier one found unprotected, since no later arm covers it and a later snapshot may omit an
//! order still resting, and adds what its own resync shows.

use std::collections::{HashMap, HashSet};

use fbc_core::{
    AckLevel, CidMatch, ClientOrderId, ExecEvent, InstrumentId, RpcId, SubmitOutcome, VenueCommand,
    VenueOrderId, VenueOrderSnapshot, VenueOrderState,
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
    /// The venue's amends keep the venue id (or it amends nothing), so our client id names one
    /// venue order.
    one_id: bool,
    /// An arm was accepted on some epoch.
    armed_once: bool,
    current: Option<Epoch>,
    /// The orders of ours a resync showed resting unprotected, while they may rest under one
    /// of the venue ids they were seen under, kept across epochs.
    unprotected: Vec<Held>,
    /// What events and query answers showed of venue ids while a resync ran or an order was
    /// held on their market (Codex P1 r4219106981 on PR #127), by market, kept after the window
    /// in which it was heard only for the markets an order is held on (Reviewer B RB-nvxn-5 on
    /// PR #127).
    facts: HashMap<InstrumentId, Facts>,
}

/// An order of ours resting unprotected.
#[derive(Debug)]
struct Held {
    cid: ClientOrderId,
    /// As the latest snapshot showed it.
    order: VenueOrderSnapshot,
    /// Every venue id it was seen under.
    ids: Vec<VenueOrderId>,
    /// Those shown ended or replaced by an amend.
    ended: Vec<VenueOrderId>,
}

/// Adds `id` to `ids` unless it is there.
fn add(ids: &mut Vec<VenueOrderId>, id: &VenueOrderId) {
    if !ids.contains(id) {
        ids.push(id.clone());
    }
}

impl Held {
    /// Our order `cid`, as a snapshot shows it.
    fn new(cid: ClientOrderId, order: &VenueOrderSnapshot) -> Held {
        Held {
            cid,
            order: order.clone(),
            ids: vec![order.vid.clone()],
            ended: Vec::new(),
        }
    }

    /// It may rest under an id it was seen under that is not shown ended.
    fn live(&self) -> bool {
        self.ids.iter().any(|id| !self.ended.contains(id))
    }

    /// It as the consumer is told it: under the latest id it was seen under not shown ended.
    fn told(&self) -> VenueOrderSnapshot {
        let live = self.ids.iter().rfind(|id| !self.ended.contains(id));
        VenueOrderSnapshot {
            vid: live.unwrap_or(&self.order.vid).clone(),
            ..self.order.clone()
        }
    }

    /// Adds what `facts` show of it: the ids evidence naming our client id showed it under,
    /// then, for each of its ids (those amends add included), whether it ended and the ids
    /// amends replaced it by. Each id is visited once.
    fn learn(&mut self, facts: &Facts, one_id: bool) {
        for id in facts.by_cid.get(&self.cid).into_iter().flatten() {
            add(&mut self.ids, id);
        }
        let cid_ended = one_id && facts.ended_cids.contains(&self.cid);
        let mut i = 0;
        while let Some(id) = self.ids.get(i).cloned() {
            if cid_ended || facts.ended.contains(&id) {
                add(&mut self.ended, &id);
            }
            for next in facts.next.get(&id).into_iter().flatten() {
                add(&mut self.ids, next);
            }
            i += 1;
        }
    }
}

/// What events and query answers showed of venue ids.
#[derive(Debug, Default)]
struct Facts {
    /// The venue ids shown ended or replaced by an amend.
    ended: HashSet<VenueOrderId>,
    /// The venue ids amends replaced each venue id by.
    next: HashMap<VenueOrderId, Vec<VenueOrderId>>,
    /// The venue ids evidence naming our client id showed it under.
    by_cid: HashMap<ClientOrderId, Vec<VenueOrderId>>,
    /// Our client ids an end naming them showed ended, kept where our client id names one
    /// venue order.
    ended_cids: HashSet<ClientOrderId>,
}

impl Gate {
    /// A gate for a venue whose protection is per connection, re-armed on each when `rearm`,
    /// an accepted arm covering the orders already open when `covers_open`, whose amends keep
    /// the venue id (or that amends nothing) when `one_id`.
    pub(crate) fn new(rearm: bool, covers_open: bool, one_id: bool) -> Gate {
        Gate {
            rearm,
            covers_open,
            one_id,
            armed_once: false,
            current: None,
            unprotected: Vec::new(),
            facts: HashMap::new(),
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
        // What the epoch it replaces heard while its resync ran is kept: if it dropped before
        // its resync ended, this epoch's snapshot may still need it (Reviewer B RB-48j7-1 on
        // PR #140). It is dropped when a resync ends.
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
        // What an earlier epoch found unprotected stays so until events or query answers end
        // it: no later arm covers it, and a later snapshot may omit an order still resting
        // (Codex P1 r4219106969, r4219551549 on PR #127). The epoch's resync adds what it shows.
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
            Settled::Resync => {
                self.update(epoch, |e| e.resync = Resync::Ended);
                self.prune();
            }
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
        self.unprotected.iter().any(|o| o.order.inst == inst)
    }

    /// The orders of ours resting unprotected from an earlier epoch, while they may rest under
    /// a venue id they were seen under that no order event or query answer has shown ended, of
    /// those the gate kept for their market (decision 0088).
    pub(crate) fn unprotected(&self) -> Vec<VenueOrderSnapshot> {
        self.unprotected.iter().map(Held::told).collect()
    }

    /// Whether `cmd` may be sent on `epoch`, or why it is held: a place, a batch of places or
    /// an amend only once the epoch is [placing](Gate::placing) and no order of ours on its
    /// market (on any item's, for a batch) rests unprotected; anything else always.
    /// It allocates nothing, and with nothing unprotected looks at no item (Reviewer B
    /// RB-nvxn-3 on PR #127): it runs on the send path of every place and amend.
    pub(crate) fn admits(&self, cmd: &VenueCommand, epoch: u32) -> Result<(), Hold> {
        let held = |inst| self.unprotected_on(inst);
        let unprotected = match cmd {
            VenueCommand::Place(o) => held(o.inst),
            VenueCommand::PlaceBatch(orders) => {
                !self.unprotected.is_empty() && orders.iter().any(|o| held(o.inst))
            }
            VenueCommand::Amend(a) => held(a.inst),
            _ => return Ok(()),
        };
        if !self.placing(epoch) {
            Err(Hold::Unready)
        } else if unprotected {
            Err(Hold::Unprotected)
        } else {
            Ok(())
        }
    }

    /// What `ev`, of `epoch`, shows of the orders resting from an earlier epoch, as it is
    /// handed to the handler: an order of ours the resync asked for on an epoch that checks
    /// shows resting is unprotected; an order event or an order query's answer adds what it
    /// shows of venue ids, and an order no longer resting under any id it was seen under
    /// releases its market.
    pub(crate) fn observe(&mut self, epoch: u32, ev: &ExecEvent) {
        let Some(e) = self.of(epoch) else {
            return;
        };
        let early = e.resync != Resync::Ended;
        match ev {
            ExecEvent::ResyncOrder(snap) if e.checks && e.resync == Resync::Asked => {
                let (VenueOrderState::Open, Some(CidMatch::Ours(cid))) = (&snap.state, snap.cid)
                else {
                    return;
                };
                // A later snapshot of a held order is matched to it by our client id (Codex P2
                // r4220412802 on PR #127).
                match self.unprotected.iter_mut().find(|o| o.cid == cid) {
                    Some(known) => {
                        known.order = snap.clone();
                        add(&mut known.ids, &snap.vid);
                    }
                    None => self.unprotected.push(Held::new(cid, snap)),
                }
                self.learn();
            }
            ExecEvent::Order(u) => self.shown(u.cid, u.vid.as_ref(), &u.state, u.inst, early),
            // An answer to a query by our client id names the order by it (Codex P2 r4219551529
            // on PR #127).
            ExecEvent::QueryResult(answer) => {
                if let Some(found) = answer.found() {
                    let cid = answer.target().client().map(CidMatch::Ours).or(found.cid);
                    self.shown(cid, Some(&found.vid), &found.state, found.inst, early);
                }
            }
            _ => {}
        }
    }

    /// An event or a query answer of the latest epoch showed an order on `inst` in `state`,
    /// under `vid` and naming `cid`, where they are given: kept under `inst` while a resync
    /// runs (`early`) or an order is held on `inst`, and applied to the orders held on `inst`
    /// (Reviewer B RB-48j7-2 on PR #140).
    fn shown(
        &mut self,
        cid: Option<CidMatch>,
        vid: Option<&VenueOrderId>,
        state: &VenueOrderState,
        inst: InstrumentId,
        early: bool,
    ) {
        if !early && !self.unprotected_on(inst) {
            return;
        }
        let ours = match cid {
            Some(CidMatch::Ours(cid)) => Some(cid),
            _ => None,
        };
        let facts = self.facts.entry(inst).or_default();
        if let Some(cid) = ours {
            let ids = facts.by_cid.entry(cid).or_default();
            vid.into_iter().for_each(|vid| add(ids, vid));
        }
        match state {
            VenueOrderState::Open | VenueOrderState::Amended { new_vid: None } => {}
            VenueOrderState::Amended {
                new_vid: Some(new_vid),
            } => {
                if let Some(cid) = ours {
                    add(facts.by_cid.entry(cid).or_default(), new_vid);
                }
                // An amend under a new venue id replaces the order it names.
                if let Some(old) = vid.filter(|old| *old != new_vid) {
                    facts.ended.insert(old.clone());
                    add(facts.next.entry(old.clone()).or_default(), new_vid);
                }
            }
            VenueOrderState::Filled
            | VenueOrderState::Canceled(_)
            | VenueOrderState::Rejected(_)
            | VenueOrderState::Expired => {
                if let Some(vid) = vid {
                    facts.ended.insert(vid.clone());
                }
                if let Some(cid) = ours.filter(|_| self.one_id) {
                    facts.ended_cids.insert(cid);
                }
            }
        }
        if self.learn() && !early {
            self.prune();
        }
    }

    /// Applies what is known of venue ids on its market to every held order, and releases each
    /// that rests under none of the ids it was seen under: whether it released any.
    fn learn(&mut self) -> bool {
        for held in &mut self.unprotected {
            if let Some(facts) = self.facts.get(&held.order.inst) {
                held.learn(facts, self.one_id);
            }
        }
        let before = self.unprotected.len();
        self.unprotected.retain(Held::live);
        self.unprotected.len() < before
    }

    /// Drops what is known of venue ids on every market no order is held on, as a resync ends
    /// or the last order held on the market is released after the resync. What a held order
    /// may yet need is on its own market, so what is kept is bounded by the evidence heard on
    /// the markets held and that heard since the latest resync ended, not by the reconnects
    /// whose resyncs end (Reviewer B RB-nvxn-5 on PR #127). An epoch replaced before its
    /// resync ends drops nothing: the next epoch's snapshot may need what it heard (Reviewer
    /// B RB-48j7-1 on PR #140).
    fn prune(&mut self) {
        let held = &self.unprotected;
        self.facts
            .retain(|inst, _| held.iter().any(|o| o.order.inst == *inst));
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
        Some(self.unprotected())
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

    /// Our client ids, one per order, minted once under a namespace lease held in a directory
    /// of its own.
    fn cids() -> &'static [ClientOrderId; 4] {
        static CIDS: std::sync::OnceLock<[ClientOrderId; 4]> = std::sync::OnceLock::new();
        CIDS.get_or_init(|| {
            let name = format!("fbc-runtime-gate-{}", std::process::id());
            let dir = std::env::temp_dir().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let ns = Namespace::new(5);
            let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), ns).unwrap();
            let mut mint = CidMint::new(lease, 0, 0, WallNs(1));
            let cids = std::array::from_fn(|_| mint.mint().unwrap());
            let _ = std::fs::remove_dir_all(&dir);
            cids
        })
    }

    fn cid() -> ClientOrderId {
        cids()[0]
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
        let mut gate = Gate::new(true, false, false);
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
        let mut gate = Gate::new(true, false, false);
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
        let mut gate = Gate::new(true, false, false);
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
            let mut gate = Gate::new(true, false, false);
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
        let mut gate = Gate::new(true, false, false);
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
            let mut gate = Gate::new(true, false, false);
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

    /// Our order `n` resting under `wire`.
    fn ours_as(n: usize, wire: &str) -> VenueOrderSnapshot {
        snap(
            Some(CidMatch::Ours(cids()[n])),
            wire,
            INST,
            VenueOrderState::Open,
        )
    }

    /// Our order 0 resting under `wire`.
    fn ours(wire: &str) -> VenueOrderSnapshot {
        ours_as(0, wire)
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
        let mut gate = Gate::new(true, false, false);
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

    /// Where the venue's amends keep the venue id, our client id names one venue order, so an
    /// end naming it ends the order; where they issue new ids, it may report an order an amend
    /// superseded, and ends nothing (Reviewer B RB-nvxn-4 on PR #127).
    #[test]
    fn an_order_event_naming_only_our_client_id_ends_it_where_amends_keep_the_venue_id() {
        let mine = Some(CidMatch::Ours(cid()));
        let ends = [
            VenueOrderState::Filled,
            VenueOrderState::Canceled(CancelReason::Disconnect),
            VenueOrderState::Rejected(TerminalReject::new(RejectKind::Margin).unwrap()),
            VenueOrderState::Expired,
        ];
        for end in ends {
            for one_id in [true, false] {
                let mut gate = Gate::new(true, false, one_id);
                resynced(&mut gate, &[ours("V-1")]);
                // Another namespace's order event under no venue id names nothing of ours.
                let foreign = Some(CidMatch::Foreign(Namespace::new(9)));
                gate.observe(0, &update(foreign, None, end.clone()));
                assert!(gate.unprotected_on(INST));
                gate.observe(0, &update(mine, None, end.clone()));
                assert_eq!(gate.unprotected_on(INST), !one_id);
            }
        }
    }

    #[test]
    fn nothing_is_unprotected_where_the_arm_covers_open_orders_or_was_not_sent_on_the_epoch() {
        // The venue's arm covers the orders already open.
        let mut gate = Gate::new(true, true, false);
        resynced(&mut gate, &[ours("V-1")]);
        assert!(gate.unprotected().is_empty());
        assert_eq!(gate.notice(0), None);
        // Protection that outlives the connection, accepted on epoch 0: epoch 1 arms nothing,
        // and what its resync shows was placed under that arm.
        let mut gate = Gate::new(false, false, false);
        resynced(&mut gate, &[]);
        gate.authenticated(1);
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.heard(1, &ExecEvent::ResyncEnd);
        assert!(gate.placing(1));
        assert!(gate.unprotected().is_empty());
        // An order shown before the epoch's resync was asked for, or by another epoch, counts
        // for nothing.
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        assert!(gate.unprotected().is_empty());
    }

    /// Codex P1 r4219106969 on PR #127: where the protection outlives a connection, an order
    /// the first arm did not cover is never covered, so a reconnect that sends no arm keeps it
    /// held and tells the consumer again.
    #[test]
    fn an_order_no_arm_covered_stays_held_across_a_reconnect_that_sends_no_arm() {
        let mut gate = Gate::new(false, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        assert_eq!(gate.notice(0), Some(vec![ours("V-1")]));
        gate.authenticated(1);
        assert!(!gate.arm_due(1));
        gate.resync_asked(1);
        // Its resync shows it again, and an order placed under the persistent arm.
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-2")));
        gate.heard(1, &ExecEvent::ResyncEnd);
        assert!(gate.placing(1));
        assert_eq!(gate.admits(&place_on(INST), 1), Err(Hold::Unprotected));
        assert_eq!(gate.notice(1), Some(vec![ours("V-1")]));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(1, &update(None, Some("V-1"), cancelled));
        assert_eq!(gate.admits(&place_on(INST), 1), Ok(()));
    }

    /// Codex P1 r4219106981 on PR #127: an order event of the epoch that ends an order, or moves
    /// it to a new venue id, before the resync's older snapshot of it is read is not undone by
    /// that snapshot.
    #[test]
    fn an_end_or_a_move_heard_before_the_snapshot_shows_the_order_is_not_undone_by_it() {
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(None, Some("V-1"), cancelled.clone()));
        let moved = VenueOrderState::Amended {
            new_vid: Some(vid("V-5")),
        };
        gate.observe(0, &update(None, Some("V-2"), moved));
        let moved_again = VenueOrderState::Amended {
            new_vid: Some(vid("V-6")),
        };
        gate.observe(0, &update(None, Some("V-5"), moved_again));
        gate.observe(
            0,
            &update(
                None,
                Some("V-3"),
                VenueOrderState::Amended {
                    new_vid: Some(vid("V-7")),
                },
            ),
        );
        gate.observe(0, &update(None, Some("V-7"), cancelled));
        for (n, wire) in ["V-1", "V-2", "V-3", "V-4"].into_iter().enumerate() {
            gate.observe(0, &ExecEvent::ResyncOrder(ours_as(n, wire)));
        }
        gate.heard(0, &accepted(1));
        gate.heard(0, &ExecEvent::ResyncEnd);
        // V-1 and V-3 (moved to V-7) ended; V-2 rests as V-6; V-4 rests as shown.
        let vids: Vec<_> = gate.unprotected().iter().map(|o| o.vid.clone()).collect();
        assert_eq!(vids, [vid("V-6"), vid("V-4")]);

        // Codex P1 r4219340075 on PR #127: an end heard late under the id an amend superseded,
        // before the snapshot, ends nothing: the order rests under its new id.
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(None, Some("V-1"), to_v2));
        gate.observe(0, &update(None, Some("V-1"), cancelled));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        let vids: Vec<_> = gate.unprotected().iter().map(|o| o.vid.clone()).collect();
        assert_eq!(vids, [vid("V-2")]);

        // An end naming neither a venue id nor a client id names nothing, and one naming only
        // our client id, where amends issue new venue ids, ends nothing.
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &update(None, None, canceled()));
        let other = Some(CidMatch::Ours(cids()[1]));
        gate.observe(0, &update(other, None, canceled()));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-8")));
        gate.observe(0, &ExecEvent::ResyncOrder(ours_as(1, "V-1")));
        assert_eq!(gate.unprotected(), [ours("V-8"), ours_as(1, "V-1")]);
    }

    /// Codex P1 r4219106988 on PR #127: an event naming a venue id names that order only, so an
    /// end reported late under the id an amend superseded does not release the amended order,
    /// whatever client id it carries; an event with no venue id names the order by our client id.
    #[test]
    fn an_end_under_a_superseded_venue_id_does_not_release_the_amended_order() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        let mine = Some(CidMatch::Ours(cid()));
        let moved = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(mine, Some("V-1"), moved));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(mine, Some("V-1"), cancelled.clone()));
        assert!(gate.unprotected_on(INST));
        assert_eq!(gate.unprotected()[0].vid, vid("V-2"));
        gate.observe(0, &update(mine, Some("V-2"), cancelled));
        assert!(!gate.unprotected_on(INST));
    }

    #[test]
    /// Codex P1 r4219551549 on PR #127: a re-armed epoch's resync adds what it shows, and keeps
    /// what an earlier epoch found unprotected even when it does not show it (an untrustworthy
    /// snapshot may omit an order still resting), until an event or a query answer ends it.
    fn the_next_epoch_adds_its_own_resync_keeps_what_was_unprotected_and_holds_nothing_until_it_is_placing()
     {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        assert!(gate.unprotected_on(INST));
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours_as(1, "V-2")));
        // Not placing yet: the place waits for the arm, and the consumer is told nothing.
        assert_eq!(gate.admits(&place_on(INST), 1), Err(Hold::Unready));
        assert_eq!(gate.notice(1), None);
        gate.heard(1, &ExecEvent::ResyncEnd);
        gate.heard(1, &accepted(2));
        assert_eq!(gate.notice(1), Some(vec![ours("V-1"), ours_as(1, "V-2")]));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(1, &update(None, Some("V-2"), cancelled));
        assert!(gate.unprotected_on(INST));
        let filled = snap(
            Some(CidMatch::Ours(cid())),
            "V-1",
            INST,
            VenueOrderState::Filled,
        );
        let answer = QueryAnswer::new(RpcId(9), OrderRef::Venue(vid("V-1")), Some(filled));
        gate.observe(1, &ExecEvent::QueryResult(answer.unwrap()));
        assert!(!gate.unprotected_on(INST));
    }

    /// Codex P2 r4219551529 on PR #127: an answer to a query by our client id names the order by
    /// that id, under whatever venue id it shows. Where amends keep the venue id, it showing the
    /// order ended ends it; where they issue new ids, the order may still rest under the id the
    /// snapshot showed (Reviewer B RB-nvxn-4 on PR #127), until that id is shown ended too.
    #[test]
    fn a_query_by_our_client_id_ends_the_order_under_its_new_venue_id() {
        for one_id in [true, false] {
            let mut gate = Gate::new(true, false, one_id);
            resynced(&mut gate, &[ours("V-1")]);
            let mine = Some(CidMatch::Ours(cid()));
            let filled = snap(mine, "V-2", INST, VenueOrderState::Filled);
            let answer = QueryAnswer::new(RpcId(9), OrderRef::Client(cid()), Some(filled));
            gate.observe(0, &ExecEvent::QueryResult(answer.unwrap()));
            assert_eq!(gate.unprotected_on(INST), !one_id);
            gate.observe(0, &by_cid("V-1", canceled()));
            assert!(!gate.unprotected_on(INST));
        }
    }

    /// Codex P2 r4219551539 on PR #127: an amend heard before the snapshot that names our client
    /// id and its new venue id but not the old one adds that id to the order the snapshot shows,
    /// which the consumer is told under it.
    #[test]
    fn an_early_amend_naming_only_our_client_id_moves_the_order_the_snapshot_shows() {
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        let mine = Some(CidMatch::Ours(cid()));
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(None, None, to_v2.clone()));
        gate.observe(0, &update(mine, None, to_v2));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        let vids: Vec<_> = gate.unprotected().iter().map(|o| o.vid.clone()).collect();
        assert_eq!(vids, [vid("V-2")]);
        // A snapshot already showing the new id, after such an amend and an end naming only our
        // client id (which may report the superseded order), shows it resting there.
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(mine, None, to_v2));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(mine, None, cancelled));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-2")));
        assert_eq!(gate.unprotected(), [ours("V-2")]);
    }

    /// An answer to a query by our client id.
    fn by_cid(wire: &str, state: VenueOrderState) -> ExecEvent {
        let found = snap(Some(CidMatch::Ours(cid())), wire, INST, state);
        let answer = QueryAnswer::new(RpcId(9), OrderRef::Client(cid()), Some(found));
        ExecEvent::QueryResult(answer.unwrap())
    }

    /// Codex P2 r4219760825 on PR #127: an answer to a query by our client id showing the order
    /// open under a venue id the gate did not know adds it, and the consumer is told the order
    /// under it. The snapshot's id may still be the newer one (Reviewer B RB-nvxn-4 on PR #127),
    /// so the market is released once the order has ended under both.
    #[test]
    fn a_query_by_our_client_id_showing_the_order_open_under_a_new_venue_id_adds_that_id() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        assert_eq!(gate.unprotected(), [ours("V-2")]);
        assert_eq!(held_after_ends(&mut gate, &["V-2", "V-1"]), [true, false]);
    }

    /// Reviewer B RB-nvxn-1 on PR #127: once an amend moved the order from a venue id, neither
    /// an answer to a query by our client id showing that superseded id ended, nor an end
    /// naming only our client id (which may report the superseded order), releases it.
    #[test]
    fn an_end_under_a_superseded_venue_id_or_naming_only_our_client_id_after_an_amend_releases_nothing()
     {
        let mine = Some(CidMatch::Ours(cid()));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.observe(0, &update(None, Some("V-1"), to_v2.clone()));
        gate.observe(0, &by_cid("V-1", cancelled.clone()));
        gate.observe(0, &update(mine, None, cancelled.clone()));
        // A late report of the amend, or one naming only our client id that moves the order
        // back to an id it was amended from, moves nothing.
        gate.observe(
            0,
            &update(
                mine,
                None,
                VenueOrderState::Amended {
                    new_vid: Some(vid("V-1")),
                },
            ),
        );
        assert!(gate.unprotected_on(INST));
        assert_eq!(gate.unprotected()[0].vid, vid("V-2"));
        // The answer showing it ended under the id it rests under releases it.
        gate.observe(0, &by_cid("V-2", cancelled.clone()));
        assert!(!gate.unprotected_on(INST));

        // Heard before the snapshot: an end naming only our client id after an amend ends
        // nothing.
        let mut gate = Gate::new(true, false, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        gate.observe(0, &update(None, Some("V-1"), to_v2.clone()));
        gate.observe(0, &update(mine, None, cancelled.clone()));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        let vids: Vec<_> = gate.unprotected().iter().map(|o| o.vid.clone()).collect();
        assert_eq!(vids, [vid("V-2")]);
        // An answer showing the superseded id ended releases nothing either.
        gate.heard(0, &accepted(1));
        gate.heard(0, &ExecEvent::ResyncEnd);
        gate.observe(0, &by_cid("V-1", cancelled));
        assert!(gate.unprotected_on(INST));
    }

    /// A later epoch's lagging snapshot showing an order under a venue id an amend moved it
    /// from adds nothing: the order is held under the id it rests under.
    #[test]
    fn a_later_snapshot_under_a_superseded_venue_id_adds_nothing() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(None, Some("V-1"), to_v2));
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-1")));
        let vids: Vec<_> = gate.unprotected().iter().map(|o| o.vid.clone()).collect();
        assert_eq!(vids, [vid("V-2")]);
    }

    /// A gate whose epoch 0 is armed (request 1) and has asked for its resync, not yet ended.
    fn resyncing(gate: &mut Gate) {
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
    }

    fn ended(gate: &mut Gate, epoch: u32, rpc: u64) {
        gate.heard(epoch, &accepted(rpc));
        gate.heard(epoch, &ExecEvent::ResyncEnd);
        assert!(gate.placing(epoch));
    }

    /// Codex P1 r4220079804 on PR #127: a later snapshot of a held order under the id it rests
    /// under, or a snapshot showing the id an amend heard before it moved the order to, keeps
    /// the order's amend history, so an end naming only our client id still releases nothing.
    #[test]
    fn a_snapshot_of_an_amended_order_keeps_its_amend_history() {
        let mine = Some(CidMatch::Ours(cid()));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        // A later epoch's snapshot under the id the order was moved to.
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.observe(0, &update(None, Some("V-1"), to_v2.clone()));
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-2")));
        ended(&mut gate, 1, 2);
        gate.observe(1, &update(mine, None, cancelled.clone()));
        gate.observe(1, &by_cid("V-1", cancelled.clone()));
        assert_eq!(gate.unprotected(), [ours("V-2")]);

        // A snapshot already showing the id an amend heard before it moved the order to.
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &update(None, Some("V-1"), to_v2));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-2")));
        ended(&mut gate, 0, 1);
        gate.observe(0, &update(mine, None, cancelled.clone()));
        gate.observe(0, &by_cid("V-1", cancelled.clone()));
        assert_eq!(gate.unprotected(), [ours("V-2")]);

        // An amend naming only our client id, heard before a snapshot showing its new id.
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(mine, None, to_v2));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-2")));
        ended(&mut gate, 0, 1);
        gate.observe(0, &update(mine, None, cancelled));
        assert_eq!(gate.unprotected(), [ours("V-2")]);
    }

    /// Codex P2 r4220079832 on PR #127: an answer to a query by our client id heard before the
    /// snapshot is not undone by an older snapshot of the order: an id it shows open is added
    /// (held until the order has ended under both, Reviewer B RB-nvxn-4 on PR #127); one it
    /// shows ended ends the order where amends keep the venue id; and one an amend moved the
    /// order from adds nothing live.
    #[test]
    fn a_query_answer_by_our_client_id_heard_before_the_snapshot_is_not_undone_by_it() {
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.unprotected(), [ours("V-2")]);
        assert_eq!(held_after_ends(&mut gate, &["V-2", "V-1"]), [true, false]);

        for one_id in [true, false] {
            let mut gate = Gate::new(true, false, one_id);
            resyncing(&mut gate);
            gate.observe(0, &by_cid("V-2", VenueOrderState::Filled));
            gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
            assert_eq!(gate.unprotected().is_empty(), one_id);
        }

        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &update(None, Some("V-1"), to("V-2")));
        gate.observe(0, &by_cid("V-1", canceled()));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        assert_eq!(gate.unprotected(), [ours("V-2")]);
    }

    /// Codex P2 r4220412802 on PR #127: a later snapshot of a held order under a new venue id
    /// no event reported (amended while disconnected) is the same order, by our client id: it
    /// moves the held order there rather than holding a second one.
    #[test]
    fn a_later_snapshot_of_a_held_order_under_a_new_venue_id_moves_it_there() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.observe(1, &ExecEvent::ResyncOrder(ours("V-2")));
        ended(&mut gate, 1, 2);
        assert_eq!(gate.unprotected(), [ours("V-2")]);
        // An end naming only our client id may report V-1, and a query answer showing V-1 ended
        // shows the superseded order: neither releases it.
        let mine = Some(CidMatch::Ours(cid()));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(1, &update(mine, None, cancelled.clone()));
        gate.observe(1, &by_cid("V-1", cancelled.clone()));
        assert!(gate.unprotected_on(INST));
        gate.observe(1, &update(None, Some("V-2"), cancelled));
        assert!(gate.unprotected().is_empty());
    }

    /// Codex P2 r4220412819 on PR #127: an amend heard after an answer to a query by our client
    /// id, both before the snapshot, follows the order on from the id the answer showed; the
    /// snapshot's id stays until shown ended (Reviewer B RB-nvxn-4 on PR #127).
    #[test]
    fn an_amend_heard_after_an_early_client_id_query_answer_moves_the_order_on() {
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        gate.observe(0, &update(None, Some("V-2"), to("V-3")));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.unprotected(), [ours("V-3")]);
        let held = held_after_ends(&mut gate, &["V-2", "V-3", "V-1"]);
        assert_eq!(held, [true, true, false]);
    }

    /// Codex P1 r4220699833 on PR #127: an early answer to a query by our client id showing the
    /// order ended under an id an amend then moved it from shows the superseded order, so the
    /// order rests on from that id, following the amends heard from it.
    #[test]
    fn an_early_client_id_query_answer_showing_an_end_an_amend_superseded_starts_the_amend_chain() {
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", cancelled.clone()));
        let to_v3 = VenueOrderState::Amended {
            new_vid: Some(vid("V-3")),
        };
        gate.observe(0, &update(None, Some("V-2"), to_v3));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.unprotected()[0].vid, vid("V-3"));
        gate.observe(0, &update(None, Some("V-1"), cancelled.clone()));
        assert!(gate.unprotected_on(INST));
        gate.observe(0, &update(None, Some("V-3"), cancelled));
        assert!(gate.unprotected().is_empty());
    }

    fn canceled() -> VenueOrderState {
        VenueOrderState::Canceled(CancelReason::Requested)
    }

    fn to(wire: &str) -> VenueOrderState {
        VenueOrderState::Amended {
            new_vid: Some(vid(wire)),
        }
    }

    /// Whether the market is still held after an end under each of `wires`, in turn.
    fn held_after_ends(gate: &mut Gate, wires: &[&str]) -> Vec<bool> {
        wires
            .iter()
            .map(|wire| {
                gate.observe(0, &update(None, Some(wire), canceled()));
                gate.unprotected_on(INST)
            })
            .collect()
    }

    /// Reviewer B RB-nvxn-4 (1) on PR #127: an answer to a query by our client id showing an
    /// order ended under a venue id the gate never heard of may show a predecessor of the order
    /// the snapshot shows; it releases nothing.
    #[test]
    fn a_client_id_answer_showing_an_unheard_of_id_ended_releases_nothing() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-2")]);
        gate.observe(0, &by_cid("V-1", canceled()));
        assert!(gate.unprotected_on(INST));
        assert_eq!(held_after_ends(&mut gate, &["V-2"]), [false]);
    }

    /// Reviewer B RB-nvxn-4 (2) and (3) on PR #127: an amend naming only our client id, or an
    /// answer to a query by it showing the order open, heard before a snapshot showing the order
    /// under another venue id, may be older or newer than it: the market stays held until the
    /// order has ended under both ids.
    #[test]
    fn early_client_id_evidence_and_a_snapshot_under_another_id_hold_until_both_ended() {
        let mine = Some(CidMatch::Ours(cid()));
        let early = [
            update(mine, None, to("V-2")),
            by_cid("V-2", VenueOrderState::Open),
        ];
        for ev in early {
            for order in [["V-2", "V-3"], ["V-3", "V-2"]] {
                let mut gate = Gate::new(true, false, false);
                resyncing(&mut gate);
                gate.observe(0, &ev);
                gate.observe(0, &ExecEvent::ResyncOrder(ours("V-3")));
                ended(&mut gate, 0, 1);
                assert_eq!(held_after_ends(&mut gate, &order), [true, false], "{ev:?}");
            }
        }
    }

    /// Codex P1 r4220956623 on PR #127: an early answer to a query by our client id showing the
    /// order open, then an amend naming only our client id, then a snapshot under a third id:
    /// the market stays held until the order has ended under every one of them.
    #[test]
    fn an_early_client_id_answer_then_a_client_id_amend_hold_until_every_id_ended() {
        let mine = Some(CidMatch::Ours(cid()));
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        gate.observe(0, &update(mine, None, to("V-3")));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        let held = held_after_ends(&mut gate, &["V-2", "V-3", "V-1"]);
        assert_eq!(held, [true, true, false]);
    }

    /// Codex P1 r4220956605 on PR #127: two amends naming only our client id heard before the
    /// snapshot, and a late report of the first, leave the market held until the order has ended
    /// under the last.
    #[test]
    fn a_late_report_of_the_first_of_two_early_client_id_amends_releases_nothing() {
        let mine = Some(CidMatch::Ours(cid()));
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        gate.observe(0, &update(mine, None, to("V-2")));
        gate.observe(0, &update(mine, None, to("V-3")));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        gate.observe(0, &update(mine, None, to("V-2")));
        gate.observe(0, &update(None, Some("V-1"), to("V-2")));
        assert_eq!(held_after_ends(&mut gate, &["V-2", "V-3"]), [true, false]);
    }

    /// Codex P1 r4220956631 on PR #127: amends reported out of order after the snapshot (the
    /// second before the first) follow the order to its last venue id.
    #[test]
    fn amends_reported_out_of_order_follow_the_order_to_its_last_id() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.observe(0, &update(None, Some("V-2"), to("V-3")));
        gate.observe(0, &update(None, Some("V-1"), to("V-2")));
        assert_eq!(held_after_ends(&mut gate, &["V-2", "V-3"]), [true, false]);
    }

    impl Gate {
        /// How many venue-id facts the gate keeps, over every market.
        fn known(&self) -> usize {
            self.facts.values().map(Facts::len).sum()
        }
    }

    impl Facts {
        fn len(&self) -> usize {
            let next: usize = self.next.values().map(Vec::len).sum();
            let by_cid: usize = self.by_cid.values().map(Vec::len).sum();
            self.ended.len() + next + by_cid + self.ended_cids.len()
        }
    }

    /// An order event on `inst` for `cid` or `wire` in `state`.
    fn update_on(
        inst: InstrumentId,
        cid: Option<CidMatch>,
        wire: Option<&str>,
        state: VenueOrderState,
    ) -> ExecEvent {
        let ExecEvent::Order(u) = update(cid, wire, state) else {
            unreachable!()
        };
        ExecEvent::Order(OrderUpdate { inst, ..u })
    }

    /// Reviewer B RB-nvxn-5 on PR #127 (FBC-48j7): while one order stays held on INST, every
    /// reconnect's resync window hears ends, amends and client-id evidence on another market;
    /// what it heard there is dropped when a resync ends, so the facts stay bounded and the
    /// held order is still released by its own end. What an epoch dropped before its resync
    /// ended heard is carried into the next epoch's window, and only that (Reviewer B
    /// RB-48j7-1 and Reviewer A on PR #140).
    #[test]
    fn facts_stay_bounded_across_many_reconnects_while_one_order_stays_held() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[ours("V-1")]);
        let mine = Some(CidMatch::Ours(cids()[1]));
        let mut rpc = 1;
        for epoch in 1..=40u32 {
            rpc += 1;
            gate.authenticated(epoch);
            // Ten amends then ends on OTHER, four facts each, from the epoch dropped before.
            let carried = if epoch % 2 == 0 { 40 } else { 0 };
            assert_eq!(gate.known(), carried, "epoch {epoch}");
            gate.arm_sent(epoch, RpcId(rpc));
            gate.resync_asked(epoch);
            for n in 0..10 {
                let wire = format!("O-{epoch}-{n}");
                let next = format!("O-{epoch}-{n}-b");
                gate.observe(epoch, &update_on(OTHER, None, Some(&wire), to(&next)));
                gate.observe(epoch, &update_on(OTHER, mine, Some(&next), canceled()));
            }
            assert_eq!(gate.known(), carried + 40, "epoch {epoch}");
            // Every other epoch drops before its resync ends.
            if epoch % 2 == 0 {
                gate.observe(epoch, &ExecEvent::ResyncOrder(ours("V-1")));
                ended(&mut gate, epoch, rpc);
                assert_eq!(gate.known(), 0, "epoch {epoch}");
                // After the resync, another market's evidence is not kept.
                gate.observe(epoch, &update_on(OTHER, None, Some("O-x"), canceled()));
                assert_eq!(gate.known(), 0, "epoch {epoch}");
            }
        }
        assert!(gate.unprotected_on(INST));
        gate.observe(40, &update(None, Some("V-1"), canceled()));
        assert!(gate.unprotected().is_empty());
        assert_eq!(gate.known(), 0);
    }

    /// FBC-48j7: what was heard on a market an order is held on is kept across the resync's
    /// end and later epochs, and dropped once its last order there is released, while another
    /// market stays held.
    #[test]
    fn facts_are_kept_for_a_held_market_and_dropped_once_it_is_released() {
        let mut gate = Gate::new(true, false, false);
        resyncing(&mut gate);
        // Heard before the snapshot on each market: the second of two amends on INST, and an
        // end of an unrelated id on OTHER.
        gate.observe(0, &update(None, Some("V-2"), to("V-3")));
        gate.observe(0, &update_on(OTHER, None, Some("W-9"), canceled()));
        let other = snap(
            Some(CidMatch::Ours(cids()[1])),
            "W-1",
            OTHER,
            VenueOrderState::Open,
        );
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        gate.observe(0, &ExecEvent::ResyncOrder(other));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.known(), 3);
        // A reconnect that shows both again keeps what was heard on both held markets.
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        ended(&mut gate, 1, 2);
        assert_eq!(gate.known(), 3);
        // The first amend, heard late, follows the order to V-3 through the kept fact.
        gate.observe(1, &update(None, Some("V-1"), to("V-2")));
        assert_eq!(gate.unprotected()[0].vid, vid("V-3"));
        // OTHER's order ends: what was heard there is dropped; INST stays held, its facts kept.
        let before = gate.known();
        gate.observe(1, &update_on(OTHER, None, Some("W-1"), canceled()));
        assert!(!gate.unprotected_on(OTHER) && gate.unprotected_on(INST));
        assert_eq!(gate.known(), before - 1);
        assert_eq!(
            held_after_ends_on(&mut gate, 1, &["V-1", "V-2", "V-3"]),
            [true, true, false]
        );
        assert_eq!(gate.known(), 0);
    }

    /// Reviewer B RB-48j7-1 on PR #140: what was heard while an epoch's resync ran is kept
    /// when that epoch, and the next, drop before their resyncs end, so a later epoch's
    /// snapshot (here under the id the order had before it moved) still learns the id our
    /// client id was shown under, and an end of the snapshot's id alone releases nothing.
    #[test]
    fn evidence_heard_in_resyncs_of_dropped_epochs_is_kept_for_a_later_snapshot() {
        let mut gate = Gate::new(true, false, false);
        let mine = Some(CidMatch::Ours(cid()));
        resyncing(&mut gate);
        gate.observe(0, &update(mine, Some("V-2"), VenueOrderState::Open));
        // Epochs 0 and 1 drop before a snapshot shows the order.
        gate.authenticated(1);
        gate.arm_sent(1, RpcId(2));
        gate.resync_asked(1);
        gate.authenticated(2);
        gate.arm_sent(2, RpcId(3));
        gate.resync_asked(2);
        gate.observe(2, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 2, 3);
        assert_eq!(
            held_after_ends_on(&mut gate, 2, &["V-1", "V-2"]),
            [true, false]
        );
    }

    /// Whether INST is still held after an end on `epoch` under each of `wires`, in turn.
    fn held_after_ends_on(gate: &mut Gate, epoch: u32, wires: &[&str]) -> Vec<bool> {
        wires
            .iter()
            .map(|wire| {
                gate.observe(epoch, &update(None, Some(wire), canceled()));
                gate.unprotected_on(INST)
            })
            .collect()
    }

    /// RB-nvxn-3 on PR #127 (FBC-48j7): with nothing unprotected, every place, batch and amend
    /// on a placing epoch is admitted, a batch of any length included.
    #[test]
    fn with_nothing_unprotected_every_place_batch_and_amend_is_admitted() {
        let mut gate = Gate::new(true, false, false);
        resynced(&mut gate, &[]);
        assert_eq!(admits_all(&gate, 0), Some(true));
        let batch = VenueCommand::PlaceBatch((0..64).map(|_| order(cid())).collect());
        assert_eq!(gate.admits(&batch, 0), Ok(()));
        assert_eq!(
            gate.admits(&VenueCommand::PlaceBatch(Vec::new()), 0),
            Ok(())
        );
        assert_eq!(
            gate.admits(&VenueCommand::PlaceBatch(Vec::new()), 1),
            Err(Hold::Unready)
        );
    }

    #[test]
    fn protection_that_outlives_a_connection_is_armed_once_accepted_and_each_epoch_resyncs() {
        let mut gate = Gate::new(false, false, false);
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
