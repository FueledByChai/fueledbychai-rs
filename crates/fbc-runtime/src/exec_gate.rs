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
//! expired); an amend the venue reports moves it to its new venue id. An event naming a venue
//! id names that order only; one naming none names our order by its client id, unless an amend
//! moved it, since the end may then report the order the amend superseded. An answer to a query
//! by our client id is the order's state now: it names the order by that id and moves it to the
//! venue id it shows, unless an amend moved it away from that id. What the epoch's events and
//! client-id query answers showed before its resync ended is kept, so an older snapshot of an
//! order does not undo it, and a later snapshot of a held order, matched by our client id,
//! keeps what was heard of its amends. The consumer is told them once per epoch
//! ([`Gate::notice`]) and cancels them through fbc-oms's authorizations, or queries one its
//! registry holds ended. Nothing else releases a market: a later epoch keeps every order an
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
    /// An arm was accepted on some epoch.
    armed_once: bool,
    current: Option<Epoch>,
    /// The orders of ours a resync showed resting unprotected, while no event or query answer
    /// has shown them ended, kept across epochs.
    unprotected: Vec<Held>,
    /// What the latest epoch's events showed before its resync ended, so an older snapshot of
    /// an order does not undo it (Codex P1 r4219106981 on PR #127): the venue ids shown ended,
    /// our client ids shown ended by an event naming no venue id, and each venue id an amend
    /// moved an order from, to the id it moved it to.
    early: Early,
}

/// An order of ours resting unprotected.
#[derive(Debug)]
struct Held {
    order: VenueOrderSnapshot,
    /// The venue ids amends moved it from, so a late report under one names nothing (Reviewer
    /// B RB-nvxn-1 on PR #127). There are no more of them than amends heard of it.
    superseded: Vec<VenueOrderId>,
    /// An amend of it was heard, even one naming no old venue id: an end naming only our client
    /// id may then report the order it superseded, and ends nothing.
    amended: bool,
}

impl Held {
    /// It is our order `cid`.
    fn is(&self, cid: ClientOrderId) -> bool {
        self.order.cid == Some(CidMatch::Ours(cid))
    }

    /// It rests now under `vid`, unless an amend moved it away from `vid`.
    fn now_under(&mut self, vid: &VenueOrderId) {
        if self.order.vid != *vid && !self.superseded.contains(vid) {
            let old = std::mem::replace(&mut self.order.vid, vid.clone());
            self.superseded.push(old);
            self.amended = true;
        }
    }

    /// A later snapshot of the same order, by our client id: what it shows, keeping what was
    /// heard of its amends (Codex P1 r4220079804 on PR #127); under a new venue id, the order
    /// was amended meanwhile (Codex P2 r4220412802 on PR #127); under an id an amend moved it
    /// from, it lags what was heard and changes nothing.
    fn refresh(&mut self, newer: Held) {
        if self.superseded.contains(&newer.order.vid) {
            return;
        }
        let older = std::mem::replace(self, newer);
        for old in older.superseded.into_iter().chain([older.order.vid]) {
            if old != self.order.vid && !self.superseded.contains(&old) {
                self.superseded.push(old);
            }
        }
        self.amended |= older.amended || !self.superseded.is_empty();
    }
}

/// What an epoch's events showed of orders before its resync ended.
#[derive(Debug, Default)]
struct Early {
    ended: HashSet<VenueOrderId>,
    ended_cids: HashSet<ClientOrderId>,
    moved: HashMap<VenueOrderId, VenueOrderId>,
    /// The venue id an amend naming our client id and no old venue id moved the order to.
    moved_cids: HashMap<ClientOrderId, VenueOrderId>,
    /// What an answer to a query by our client id showed: the venue id the order had then, and
    /// whether it had ended (Codex P2 r4220079832 on PR #127).
    queried: HashMap<ClientOrderId, (VenueOrderId, bool)>,
}

impl Early {
    /// Where our order `cid`, shown as `snap`, rests now, with the venue ids amends moved it
    /// from, or `None` when an event or a query answer showed it ended.
    fn now(&self, snap: &VenueOrderSnapshot, cid: ClientOrderId) -> Option<Held> {
        let mut superseded = Vec::new();
        let mut vid = snap.vid.clone();
        // Codex P2 r4219551539 on PR #127.
        let moved_cid = self.moved_cids.get(&cid);
        if let Some(moved) = moved_cid
            && *moved != vid
        {
            superseded.push(std::mem::replace(&mut vid, moved.clone()));
        }
        // An answer to a query by our client id is the order's state when it was given (Codex
        // P2 r4220079832 on PR #127): open, the order rests on from the id it showed, following
        // the amends heard from it (Codex P2 r4220412819 on PR #127); ended, it ended, unless an
        // amend moved the order from the id it showed, so it showed the superseded order.
        match self.queried.get(&cid) {
            Some((at, false)) if *at != vid => {
                superseded.push(std::mem::replace(&mut vid, at.clone()));
            }
            Some((at, true)) if !self.moved.contains_key(at) => return None,
            _ => {}
        }
        // Each step follows one amend; there are no more steps than amends heard. An amend away
        // from an id comes before an end heard under it, which names the superseded order
        // (Codex P1 r4219340075 on PR #127). A cycle of amends no venue reports leaves the order
        // resting where the steps stop.
        for _ in 0..=self.moved.len() {
            match self.moved.get(&vid) {
                Some(next) => superseded.push(std::mem::replace(&mut vid, next.clone())),
                None if self.ended.contains(&vid) => return None,
                None => break,
            }
        }
        // The ids amends heard moved the order from to an id it had, the snapshot's included
        // (Codex P1 r4220079804 on PR #127); each pass adds at least one, or stops.
        superseded.push(snap.vid.clone());
        for _ in 0..self.moved.len() {
            let before: Vec<_> = self
                .moved
                .iter()
                .filter(|(from, to)| superseded.contains(to) && !superseded.contains(from))
                .map(|(from, _)| from.clone())
                .collect();
            if before.is_empty() {
                break;
            }
            superseded.extend(before);
        }
        let mut unique = Vec::with_capacity(superseded.len());
        for old in superseded {
            if old != vid && !unique.contains(&old) {
                unique.push(old);
            }
        }
        let superseded = unique;
        // An end naming only our client id ends the order only where no amend of it was heard:
        // after one, it may report the order the amend superseded (Reviewer B RB-nvxn-1 on
        // PR #127).
        let amended = !superseded.is_empty() || moved_cid.is_some();
        if !amended && self.ended_cids.contains(&cid) {
            return None;
        }
        Some(Held {
            order: VenueOrderSnapshot {
                vid,
                ..snap.clone()
            },
            superseded,
            amended,
        })
    }
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
            early: Early::default(),
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
        // What an earlier epoch found unprotected stays so until an event or a query answer
        // ends it: no later arm covers it, and a later snapshot may omit an order still resting
        // (Codex P1 r4219106969, r4219551549 on PR #127). The epoch's resync adds what it shows.
        self.early = Early::default();
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
        self.unprotected.iter().any(|o| o.order.inst == inst)
    }

    /// The orders of ours resting unprotected from an earlier epoch, while no event of the
    /// latest epoch has shown them ended.
    pub(crate) fn unprotected(&self) -> Vec<VenueOrderSnapshot> {
        self.unprotected.iter().map(|o| o.order.clone()).collect()
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
        let early = e.resync != Resync::Ended;
        match ev {
            ExecEvent::ResyncOrder(snap) if e.checks && e.resync == Resync::Asked => {
                let (VenueOrderState::Open, Some(CidMatch::Ours(cid))) = (&snap.state, snap.cid)
                else {
                    return;
                };
                if let Some(held) = self.early.now(snap, cid) {
                    match self.unprotected.iter_mut().find(|o| o.is(cid)) {
                        Some(known) => known.refresh(held),
                        None => self.unprotected.push(held),
                    }
                }
            }
            ExecEvent::Order(u) => self.shown(u.cid, u.vid.as_ref(), &u.state, early),
            // An answer to a query by our client id names the order by it: it is the order's
            // state now, under the venue id it shows (Codex P2 r4219551529, r4219760825 on PR
            // #127), unless an amend moved the order away from that id, when it shows the order
            // the amend superseded (Reviewer B RB-nvxn-1 on PR #127).
            ExecEvent::QueryResult(answer) => match (answer.found(), answer.target().client()) {
                (Some(found), Some(cid)) => {
                    for held in self.unprotected.iter_mut().filter(|o| o.is(cid)) {
                        held.now_under(&found.vid);
                    }
                    if early {
                        let ended = !matches!(
                            found.state,
                            VenueOrderState::Open | VenueOrderState::Amended { .. }
                        );
                        self.early.queried.insert(cid, (found.vid.clone(), ended));
                    }
                    let ours = Some(CidMatch::Ours(cid));
                    self.shown(ours, Some(&found.vid), &found.state, early);
                }
                (Some(found), None) => self.shown(found.cid, Some(&found.vid), &found.state, early),
                (None, _) => {}
            },
            _ => {}
        }
    }

    /// An event of the latest epoch showed an order in `state`: the order `vid` when it names a
    /// venue id, so an end reported under an id an amend superseded releases nothing (Codex P1
    /// r4219106988 on PR #127), and otherwise our order `cid`, if no amend moved it (an end may
    /// report the superseded order; an amend to an id it was moved from is a late report).
    /// Before the epoch's resync has ended, it is also kept for the snapshots still to come
    /// (`early`).
    fn shown(
        &mut self,
        cid: Option<CidMatch>,
        vid: Option<&VenueOrderId>,
        state: &VenueOrderState,
        early: bool,
    ) {
        let ours = match cid {
            Some(CidMatch::Ours(cid)) => Some(cid),
            _ => None,
        };
        let names = |o: &Held| match vid {
            Some(vid) => *vid == o.order.vid,
            None => ours.is_some_and(|cid| o.is(cid) && !o.amended),
        };
        match state {
            VenueOrderState::Open | VenueOrderState::Amended { new_vid: None } => {}
            VenueOrderState::Amended {
                new_vid: Some(new_vid),
            } => {
                if early {
                    match (vid, ours) {
                        (Some(vid), _) => {
                            self.early.moved.insert(vid.clone(), new_vid.clone());
                        }
                        (None, Some(cid)) => {
                            self.early.moved_cids.insert(cid, new_vid.clone());
                        }
                        (None, None) => {}
                    }
                }
                // One naming only our client id moves the order unless it moves it back to an id
                // an amend moved it from.
                let moves = |o: &Held| match vid {
                    Some(vid) => *vid == o.order.vid,
                    None => ours.is_some_and(|cid| o.is(cid) && !o.superseded.contains(new_vid)),
                };
                self.unprotected
                    .iter_mut()
                    .filter(|o| moves(o))
                    .for_each(|o| o.now_under(new_vid));
            }
            VenueOrderState::Filled
            | VenueOrderState::Canceled(_)
            | VenueOrderState::Rejected(_)
            | VenueOrderState::Expired => {
                if early {
                    match (vid, ours) {
                        (Some(vid), _) => {
                            self.early.ended.insert(vid.clone());
                        }
                        (None, Some(cid)) => {
                            self.early.ended_cids.insert(cid);
                        }
                        (None, None) => {}
                    }
                }
                self.unprotected.retain(|o| !names(o));
            }
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

    /// Codex P1 r4219106969 on PR #127: where the protection outlives a connection, an order
    /// the first arm did not cover is never covered, so a reconnect that sends no arm keeps it
    /// held and tells the consumer again.
    #[test]
    fn an_order_no_arm_covered_stays_held_across_a_reconnect_that_sends_no_arm() {
        let mut gate = Gate::new(false, false);
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
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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

        // An end naming no venue id ends our order by its client id where no amend of it was
        // heard; one naming neither names nothing. Amends no venue reports, in a cycle, leave
        // the order resting, and an end naming only our client id does not end it.
        let mut gate = Gate::new(true, false);
        gate.authenticated(0);
        gate.arm_sent(0, RpcId(1));
        gate.resync_asked(0);
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(None, None, cancelled.clone()));
        let to = |wire: &str| VenueOrderState::Amended {
            new_vid: Some(vid(wire)),
        };
        gate.observe(0, &update(None, Some("V-8"), to("V-9")));
        gate.observe(0, &update(None, Some("V-9"), to("V-8")));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-8")));
        assert_eq!(gate.unprotected().len(), 1);
        let other = Some(CidMatch::Ours(cids()[1]));
        gate.observe(0, &update(other, None, cancelled));
        gate.observe(0, &ExecEvent::ResyncOrder(ours_as(1, "V-1")));
        assert_eq!(gate.unprotected().len(), 1);
        assert_ne!(gate.unprotected()[0].vid, vid("V-1"));
    }

    /// Codex P1 r4219106988 on PR #127: an event naming a venue id names that order only, so an
    /// end reported late under the id an amend superseded does not release the amended order,
    /// whatever client id it carries; an event with no venue id names the order by our client id.
    #[test]
    fn an_end_under_a_superseded_venue_id_does_not_release_the_amended_order() {
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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
    /// that id, under whatever venue id an amend gave it meanwhile.
    #[test]
    fn a_query_by_our_client_id_ends_the_order_under_its_new_venue_id() {
        let mut gate = Gate::new(true, false);
        resynced(&mut gate, &[ours("V-1")]);
        let mine = Some(CidMatch::Ours(cid()));
        let filled = snap(mine, "V-2", INST, VenueOrderState::Filled);
        let answer = QueryAnswer::new(RpcId(9), OrderRef::Client(cid()), Some(filled));
        gate.observe(0, &ExecEvent::QueryResult(answer.unwrap()));
        assert!(!gate.unprotected_on(INST));
    }

    /// Codex P2 r4219551539 on PR #127: an amend heard before the snapshot that names our client
    /// id and its new venue id but not the old one still moves the order the snapshot shows.
    #[test]
    fn an_early_amend_naming_only_our_client_id_moves_the_order_the_snapshot_shows() {
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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
    /// open under a venue id the gate does not track moves it there, so an end under that id
    /// then releases it.
    #[test]
    fn a_query_by_our_client_id_showing_the_order_open_under_a_new_venue_id_moves_it_there() {
        let mut gate = Gate::new(true, false);
        resynced(&mut gate, &[ours("V-1")]);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        assert_eq!(gate.unprotected()[0].vid, vid("V-2"));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(None, Some("V-2"), cancelled));
        assert!(!gate.unprotected_on(INST));
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
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
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
        let mut gate = Gate::new(true, false);
        resyncing(&mut gate);
        gate.observe(0, &update(None, Some("V-1"), to_v2));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-2")));
        ended(&mut gate, 0, 1);
        gate.observe(0, &update(mine, None, cancelled.clone()));
        gate.observe(0, &by_cid("V-1", cancelled.clone()));
        assert_eq!(gate.unprotected(), [ours("V-2")]);

        // An amend naming only our client id, heard before a snapshot showing its new id.
        let mut gate = Gate::new(true, false);
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
    /// snapshot is the order's state then, so an older snapshot of the order does not undo it:
    /// open under a new id moves it there, ended ends it, and one showing an id an amend moved
    /// the order from changes nothing.
    #[test]
    fn a_query_answer_by_our_client_id_heard_before_the_snapshot_is_not_undone_by_it() {
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        let mut gate = Gate::new(true, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.unprotected()[0].vid, vid("V-2"));
        gate.observe(0, &update(None, Some("V-2"), cancelled.clone()));
        assert!(gate.unprotected().is_empty());

        let mut gate = Gate::new(true, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Filled));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        assert!(gate.unprotected().is_empty());

        let mut gate = Gate::new(true, false);
        resyncing(&mut gate);
        let to_v2 = VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        };
        gate.observe(0, &update(None, Some("V-1"), to_v2));
        gate.observe(0, &by_cid("V-1", cancelled));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        assert_eq!(gate.unprotected()[0].vid, vid("V-2"));
    }

    /// Codex P2 r4220412802 on PR #127: a later snapshot of a held order under a new venue id
    /// no event reported (amended while disconnected) is the same order, by our client id: it
    /// moves the held order there rather than holding a second one.
    #[test]
    fn a_later_snapshot_of_a_held_order_under_a_new_venue_id_moves_it_there() {
        let mut gate = Gate::new(true, false);
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
    /// id, both before the snapshot, moves the order on from the id the answer showed.
    #[test]
    fn an_amend_heard_after_an_early_client_id_query_answer_moves_the_order_on() {
        let mut gate = Gate::new(true, false);
        resyncing(&mut gate);
        gate.observe(0, &by_cid("V-2", VenueOrderState::Open));
        let to_v3 = VenueOrderState::Amended {
            new_vid: Some(vid("V-3")),
        };
        gate.observe(0, &update(None, Some("V-2"), to_v3));
        gate.observe(0, &ExecEvent::ResyncOrder(ours("V-1")));
        ended(&mut gate, 0, 1);
        assert_eq!(gate.unprotected()[0].vid, vid("V-3"));
        let cancelled = VenueOrderState::Canceled(CancelReason::Requested);
        gate.observe(0, &update(None, Some("V-3"), cancelled));
        assert!(gate.unprotected().is_empty());
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
