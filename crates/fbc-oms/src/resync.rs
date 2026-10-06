//! Resync snapshots (decision 0013 rule 1, 0005's I3; the rules are decision 0055's): the
//! venue's open orders and positions, re-read without breaking I3 (inventory changes only
//! through deduplicated fills).
//!
//! **The seed.** A market's position is unknown until a resync seeds it, once per process
//! ([`Registry::resync`]): until then [`Registry::position`] is `None` and the pre-trade caps
//! admit no place or amend on it. Only a resync from a trustworthy snapshot source seeds; one
//! that can be stale or incomplete, or none, seeds nothing ([`ResyncReport::untrustworthy`]).
//! Every resync registers our namespace's open orders the snapshot shows on a market not
//! seeded yet that the registry does not hold (an earlier run's; never amended, only
//! cancelled: their placement is not known), whether or not it seeds the market, so a cancel
//! of every order the registry holds reaches them. The seed sets the position to the
//! snapshot's, plus the fills accepted before the seed that the snapshot does not hold.
//!
//! **A fill straddling the snapshot** (executed after the request, reported before or after the
//! answer) is counted exactly once. The snapshot holds a fill when:
//! 1. the fill arrived before the resync was requested;
//! 2. the snapshot shows the fill's order with a cumulative fill at least the fill's
//!    `cum_after` (when the fill reports none: its matching-engine time is at or before the
//!    watermark). This needs the snapshot's open orders and positions read at one instant
//!    ([`ResyncSnapshot`]): a fill landing between two reads would show in the one and not the
//!    other;
//! 3. the snapshot does not show the fill's order, and its matching-engine time is at or before
//!    the watermark, or the order was at the venue before the request: it was not open when
//!    the venue read the account, so every fill of it came before. An order the registry held
//!    before the seed, unless a resync registered it from its snapshot, may have reached the
//!    venue after the read, so only its time places it (one an earlier snapshot showed too:
//!    stricter than needed, on the safe side); an earlier run's order the registry does not
//!    hold, or one a resync registered from its snapshot, was there (0013's
//!    cancel-on-disconnect ends an earlier run's orders in flight).
//!
//! It does not hold a fill of an order it shows with a cumulative fill below the fill's
//! `cum_after`, nor any fill of an order placed after the seed. A fill none of these rules
//! places is unsettled: before the seed, the market is not seeded and stays unknown until a
//! later resync places it (requested after the fill arrived, rule 1 does); after the seed, the
//! market's position is unknown for the rest of the process ([`FillRouted::Unsettled`](crate::FillRouted::Unsettled)).
//! A fill the seed holds is kept in the ledger and counted on its order (an order the snapshot
//! shows already counts the cumulative fill it showed), never on the inventory
//! ([`FillRouted::InSnapshot`](crate::FillRouted::InSnapshot)).
//!
//! **Later resyncs** never seed or overwrite the inventory: each seeded market's position is
//! compared with the inventory as of the snapshot's watermark (the fills that arrived before the
//! request, and those after it with a matching-engine time at or before the watermark), and a
//! difference is reported ([`PositionCheck::Desync`]). Every resync reconciles the orders the
//! registry holds as order updates (the snapshot's cumulative fill raises `cum_venue` only),
//! the Unknown ladder's included ([`Registry::on_resync`]).

use std::collections::{HashMap, HashSet};
use std::fmt;

use fbc_core::{
    CidMatch, ClientOrderId, ExchTsKind, FillKey, InstrumentId, Lots, MonoNs, OrderCaps,
    SignedLots, SnapshotSource, VenueOrderId, VenueOrderSnapshot, VenueOrderState, WallNs,
};

use crate::ladder::{LadderConfig, ResyncApplied, update_of};
use crate::ledger::FillTime;
use crate::record::{OrderKey, OrderRecord};
use crate::registry::Registry;

/// One resync's answer, as the consumer collected it from the `Resync*` events.
///
/// Its open orders and positions must be one read of the account, at one instant: the seed
/// compares an order's shown cumulative fill with the position read beside it (rule 2 in the
/// module documentation; decision 0055). A venue whose resync reads them in two requests can
/// have a fill land between the reads, shown in the one and not in the other, and the seed then
/// count it never or twice. Such a venue does not meet this yet; FBC-k7t7 makes it declared.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ResyncSnapshot {
    /// `ResyncBegin`'s watermark: the request's `EncodeCtx.wall` (decision 0014).
    pub watermark: WallNs,
    /// The request's `EncodeCtx.mono`, on the shard's monotonic clock the fill ledger's `now`
    /// is on: every fill applied at or before it reached us before the resync was asked.
    pub requested_at: MonoNs,
    /// The open orders (`ResyncOrder`).
    pub orders: Vec<VenueOrderSnapshot>,
    /// The positions (`ResyncPosition`); an instrument not listed is flat (decision 0049).
    pub positions: Vec<(InstrumentId, SignedLots)>,
}

/// A resync the registry refused: nothing of it applied.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ResyncError {
    /// The snapshot lists this instrument's position twice.
    DuplicatePosition(InstrumentId),
    /// The snapshot lists our order under this client id twice.
    DuplicateOrder(ClientOrderId),
    /// Seeding this market overflows its position or an order's fill count.
    Overflow(InstrumentId),
}

impl fmt::Display for ResyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResyncError::DuplicatePosition(inst) => {
                write!(f, "the resync lists the position on {inst:?} twice")
            }
            ResyncError::DuplicateOrder(cid) => write!(f, "the resync lists {cid:?} twice"),
            ResyncError::Overflow(inst) => {
                write!(f, "seeding {inst:?} overflows its position or a fill count")
            }
        }
    }
}

impl std::error::Error for ResyncError {}

/// How a seeded market's position compares with the snapshot of a later resync.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum PositionCheck {
    /// The venue's position is the inventory as of the watermark.
    Agrees {
        inst: InstrumentId,
        position: SignedLots,
    },
    /// They differ: reported, the inventory unchanged. A fill executed between the request and
    /// the venue's read, or one still on its way, shows here too, so the consumer judges a
    /// desync over more than one resync.
    /// `ledger` is `None` when the inventory as of the watermark does not fit a lot count.
    Desync {
        inst: InstrumentId,
        venue: SignedLots,
        ledger: Option<SignedLots>,
    },
    /// Not compared: the resync was requested before one the registry already compared, so
    /// the inventory as of its watermark is no longer kept.
    Stale(InstrumentId),
    /// Not compared: a fill after the seed could not be placed against it, so the market's
    /// position is unknown.
    Unsettled(InstrumentId),
}

/// What a resync did ([`Registry::resync`]).
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct ResyncReport {
    /// What it did to the orders on the Unknown ladder.
    pub ladder: ResyncApplied,
    /// The markets it seeded, with their position.
    pub seeded: Vec<(InstrumentId, SignedLots)>,
    /// Our open orders it registered from the snapshot, on markets not seeded before it,
    /// whether it seeded them or not.
    pub registered: Vec<ClientOrderId>,
    /// The markets it could not seed, each with a fill accepted before the seed that the
    /// snapshot neither holds nor shows to be after it: they stay unknown.
    pub unsettled: Vec<(InstrumentId, FillKey)>,
    /// Each market seeded before it, compared.
    pub checks: Vec<PositionCheck>,
    /// Open orders under our namespace on a market seeded before it that the registry does not
    /// hold (0005's I7 orphans).
    pub untracked: Vec<(ClientOrderId, VenueOrderId)>,
    /// The venue's snapshot source is not trustworthy (it can be stale or incomplete, or the
    /// venue offers none): the resync seeded no market, so their positions stay unknown and
    /// nothing is built on them (decision 0055).
    pub untrustworthy: bool,
}

/// A market's position, as the registry knows it.
#[derive(Debug)]
pub(crate) enum MarketState {
    /// Not seeded: the fills accepted so far, for the seed to place.
    Unseeded(Vec<Held>),
    /// Seeded.
    Seeded(Seed),
    /// Seeded, and then a fill could not be placed against the seed: unknown.
    Unsettled,
}

/// A fill accepted before its market was seeded.
#[derive(Debug)]
pub(crate) struct Held {
    pub(crate) key: FillKey,
    pub(crate) fill: Placed,
    pub(crate) signed: SignedLots,
}

/// What places a fill against a seed: its order (our client id, and the venue id it names),
/// its cumulative fill, its matching-engine time and when it arrived.
#[derive(Clone, Debug)]
pub(crate) struct Placed {
    pub(crate) cid: ClientOrderId,
    pub(crate) vid: Option<VenueOrderId>,
    pub(crate) qty: Lots,
    pub(crate) cum_after: Option<Lots>,
    pub(crate) exec: Option<WallNs>,
    pub(crate) arrived: MonoNs,
}

impl Placed {
    pub(crate) fn of(
        cid: ClientOrderId,
        vid: Option<VenueOrderId>,
        qty: Lots,
        cum_after: Option<Lots>,
        time: Option<FillTime>,
        arrived: MonoNs,
    ) -> Placed {
        Placed {
            cid,
            vid,
            qty,
            cum_after,
            exec: time
                .filter(|t| t.kind == ExchTsKind::MatchingEngine)
                .map(|t| t.aligned),
            arrived,
        }
    }
}

/// A seeded market.
#[derive(Debug)]
pub(crate) struct Seed {
    /// The snapshot that seeded it; `None` when the consumer seeded it by hand
    /// ([`Registry::seed_position`]), so every later fill counts.
    reference: Option<Box<SeedRef>>,
    /// The inventory as of `folded_to`.
    base: i128,
    /// Every fill counted on the inventory since then arrived after it.
    folded_to: MonoNs,
    /// The fills counted on the inventory since `folded_to`: when each arrived, its
    /// matching-engine time and what it moved.
    log: Vec<(MonoNs, Option<WallNs>, i128)>,
}

impl Seed {
    pub(crate) fn by_hand(pos: SignedLots) -> Seed {
        Seed {
            reference: None,
            base: i128::from(pos.0),
            folded_to: MonoNs(0),
            log: Vec::new(),
        }
    }
}

/// What a seed's snapshot showed.
#[derive(Debug)]
struct SeedRef {
    watermark: WallNs,
    requested_at: MonoNs,
    /// The orders the registry held on the market before the seed, except those a resync
    /// registered from its snapshot: one may have reached the venue after it read the account,
    /// so the snapshot not showing it does not place its fills (Reviewer B's P1 on PR #80).
    /// One an earlier snapshot showed is among them too: stricter than needed, the safe side.
    unsure: HashSet<ClientOrderId>,
    /// Our orders it showed, with their cumulative fill.
    shown: HashMap<ClientOrderId, Lots>,
    /// Every order it showed, by venue id, with its cumulative fill: an order shown without
    /// our client id is still shown.
    shown_vids: HashMap<VenueOrderId, Lots>,
    /// The orders the registry held on the market when it was seeded: any other was placed
    /// after it.
    known: HashSet<ClientOrderId>,
}

/// Where a fill stands against a seed.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Placement {
    /// Count it on the inventory.
    After,
    /// The seeded position holds it; `shown` when the snapshot showed its order, whose fill
    /// count already holds it too.
    InSnapshot { shown: bool },
    /// Nothing places it.
    Unsettled,
}

impl SeedRef {
    fn place(&self, f: &Placed) -> Placement {
        let before_watermark = f.exec.is_some_and(|t| t <= self.watermark);
        let shown = self
            .shown
            .get(&f.cid)
            .or_else(|| f.vid.as_ref().and_then(|v| self.shown_vids.get(v)));
        let held = f.arrived <= self.requested_at
            || match (shown, f.cum_after) {
                (Some(&shown), Some(cum)) => {
                    if cum > shown {
                        return Placement::After;
                    }
                    true
                }
                (Some(_), None) => before_watermark,
                (None, _) => before_watermark || !self.unsure.contains(&f.cid),
            };
        if held {
            Placement::InSnapshot {
                shown: shown.is_some(),
            }
        } else {
            Placement::Unsettled
        }
    }
}

impl Registry {
    /// The position on `inst`: `None` until a resync seeds it ([`Registry::resync`]), and once a
    /// fill after the seed could not be placed against it.
    pub fn position(&self, inst: InstrumentId) -> Option<SignedLots> {
        match self.markets.get(&inst) {
            Some(MarketState::Seeded(_)) => Some(self.inventory(inst)),
            _ => None,
        }
    }

    /// Where a fill of `inst` the ledger accepted stands, `registered` when its order is one
    /// the registry holds.
    pub(crate) fn placement(&self, inst: InstrumentId, registered: bool, f: &Placed) -> Placement {
        match self.markets.get(&inst) {
            Some(MarketState::Seeded(Seed {
                reference: Some(r), ..
            })) if !registered || r.known.contains(&f.cid) => r.place(f),
            _ => Placement::After,
        }
    }

    /// Records a fill of `inst` counted as `placement` says, moving the inventory by `signed`
    /// unless the seed holds it.
    pub(crate) fn fill_counted(
        &mut self,
        inst: InstrumentId,
        key: FillKey,
        f: Placed,
        signed: SignedLots,
        placement: Placement,
    ) {
        let state = self
            .markets
            .entry(inst)
            .or_insert_with(|| MarketState::Unseeded(Vec::new()));
        match (state, placement) {
            (MarketState::Unseeded(held), _) => held.push(Held {
                key,
                fill: f,
                signed,
            }),
            (state, Placement::Unsettled) => *state = MarketState::Unsettled,
            (MarketState::Seeded(seed), Placement::After) => {
                seed.log.push((f.arrived, f.exec, i128::from(signed.0)));
            }
            _ => {}
        }
    }

    /// Applies a resync's snapshot, delivered under `key` (decision 0055; the module
    /// documentation gives the rules).
    ///
    /// The Unknown ladder's orders apply first ([`Registry::on_resync`], with `cfg` and
    /// `caps`; call this instead of it, not beside it), then every other order of ours the
    /// snapshot shows as its order update would. Our open orders it shows on a market not yet
    /// seeded that the registry does not hold are registered. From a trustworthy snapshot
    /// source, each market not yet seeded that the caps configure, the snapshot names or a fill
    /// moved is then seeded, unless a fill it holds is unsettled; from any other, none is
    /// ([`ResyncReport::untrustworthy`]). Each market seeded before is compared.
    /// Refused whole, nothing applied, when the snapshot lists a position or one of our orders
    /// twice, or a seed overflows.
    pub fn resync(
        &mut self,
        cfg: &LadderConfig,
        caps: &OrderCaps,
        snap: &ResyncSnapshot,
        key: OrderKey,
    ) -> Result<ResyncReport, ResyncError> {
        let mut positions = HashMap::new();
        for &(inst, pos) in &snap.positions {
            if positions.insert(inst, pos).is_some() {
                return Err(ResyncError::DuplicatePosition(inst));
            }
        }
        let mut ours: HashMap<ClientOrderId, &VenueOrderSnapshot> = HashMap::new();
        for o in &snap.orders {
            if let Some(CidMatch::Ours(cid)) = o.cid
                && ours.insert(cid, o).is_some()
            {
                return Err(ResyncError::DuplicateOrder(cid));
            }
        }
        let mut report = ResyncReport {
            untrustworthy: caps.snapshot_source != SnapshotSource::Trustworthy,
            ..ResyncReport::default()
        };
        let seeds = if report.untrustworthy {
            Vec::new()
        } else {
            self.plan_seeds(snap, &positions, &ours, &mut report)?
        };

        let ladder: HashSet<ClientOrderId> = self
            .orders
            .iter()
            .filter(|(_, rec)| rec.unknown_since().is_some())
            .map(|(cid, _)| *cid)
            .collect();
        report.ladder = self.on_resync(cfg, caps, snap.watermark, &snap.orders, key);
        for o in &snap.orders {
            if let Some(cid) = self.held_order(o)
                && !ladder.contains(&cid)
            {
                let u = update_of(o);
                self.with_record(cid, |rec| rec.apply_update(&u, key));
            }
        }

        let mut checked: Vec<InstrumentId> = self
            .markets
            .iter()
            .filter(|(_, s)| !matches!(s, MarketState::Unseeded(_)))
            .map(|(inst, _)| *inst)
            .collect();
        checked.sort();
        for inst in checked {
            let venue = positions.get(&inst).copied().unwrap_or(SignedLots(0));
            report.checks.push(self.check(inst, snap, venue));
        }
        for (&cid, o) in &ours {
            if self.held_order(o).is_none()
                && matches!(
                    self.markets.get(&o.inst),
                    Some(MarketState::Seeded(_) | MarketState::Unsettled)
                )
            {
                report.untracked.push((cid, o.vid.clone()));
            }
        }
        report.untracked.sort_by_key(|(cid, _)| *cid);

        self.register_shown(snap, key, &mut report);
        for plan in seeds {
            self.seed(plan, &mut report);
        }
        report.registered.sort();
        Ok(report)
    }

    /// The order of ours the registry holds that a snapshot order names: by our client id, or
    /// by a venue id it had; `None` when they name different orders, or another namespace's
    /// or a non-canonical client id names it.
    fn held_order(&self, o: &VenueOrderSnapshot) -> Option<ClientOrderId> {
        let by_vid = self.cid_of(&o.vid);
        match o.cid {
            Some(CidMatch::Foreign(_) | CidMatch::Unparseable) => None,
            Some(CidMatch::Ours(cid)) if self.orders.contains_key(&cid) => {
                by_vid.is_none_or(|v| v == cid).then_some(cid)
            }
            Some(CidMatch::Ours(_)) | None => by_vid,
        }
    }

    /// Registers our open orders the snapshot shows on a market not seeded yet that the
    /// registry does not hold (an earlier run's), whether this resync seeds the market or not,
    /// so a cancel of every order the registry holds reaches them (Reviewer B's P2 on PR #80).
    /// Each counts as sent at the resync's request, so the Unknown ladder's settle and absence
    /// rules apply to it (Reviewer B's RB80-9). One whose venue id names another order of ours
    /// is not registered.
    fn register_shown(&mut self, snap: &ResyncSnapshot, key: OrderKey, report: &mut ResyncReport) {
        for o in &snap.orders {
            if let Some(CidMatch::Ours(cid)) = o.cid
                && o.state == VenueOrderState::Open
                && !self.orders.contains_key(&cid)
                && self.cid_of(&o.vid).is_none()
                && matches!(
                    self.markets.get(&o.inst),
                    None | Some(MarketState::Unseeded(_))
                )
            {
                self.orders.insert(
                    cid,
                    OrderRecord::seeded_from(cid, o, snap.requested_at, snap.watermark),
                );
                let u = update_of(o);
                self.with_record(cid, |rec| rec.apply_update(&u, key));
                report.registered.push(cid);
            }
        }
    }

    /// The seeds this resync makes from a trustworthy snapshot, decided before anything
    /// applies: each market not yet seeded that the caps configure, the snapshot names or a
    /// fill moved, whose held fills the snapshot places.
    fn plan_seeds(
        &self,
        snap: &ResyncSnapshot,
        positions: &HashMap<InstrumentId, SignedLots>,
        ours: &HashMap<ClientOrderId, &VenueOrderSnapshot>,
        report: &mut ResyncReport,
    ) -> Result<Vec<SeedPlan>, ResyncError> {
        let mut markets: Vec<InstrumentId> = self
            .caps()
            .markets()
            .chain(positions.keys().copied())
            .chain(ours.values().map(|o| o.inst))
            .chain(self.markets.keys().copied())
            .filter(|inst| {
                matches!(
                    self.markets.get(inst),
                    None | Some(MarketState::Unseeded(_))
                )
            })
            .collect();
        markets.sort();
        markets.dedup();
        let mut plans = Vec::new();
        'market: for inst in markets {
            let mut reference = SeedRef {
                watermark: snap.watermark,
                requested_at: snap.requested_at,
                unsure: self
                    .orders
                    .values()
                    .filter(|rec| rec.placed().inst == inst && !rec.from_snapshot())
                    .map(OrderRecord::cid)
                    .collect(),
                shown: HashMap::new(),
                shown_vids: HashMap::new(),
                known: HashSet::new(),
            };
            // Our orders it shows: one the registry holds by its id (an order shown under our
            // client id and another order's venue id by neither), or one to register.
            for o in snap.orders.iter().filter(|o| o.inst == inst) {
                reference.shown_vids.insert(o.vid.clone(), o.cum_filled);
                if let Some(cid) = self.held_order(o) {
                    reference.shown.insert(cid, o.cum_filled);
                } else if let Some(CidMatch::Ours(cid)) = o.cid
                    && !self.orders.contains_key(&cid)
                {
                    reference.shown.insert(cid, o.cum_filled);
                }
            }
            let overflow = ResyncError::Overflow(inst);
            let mut pos = i128::from(positions.get(&inst).map_or(0, |p| p.0));
            let mut folded_to = snap.requested_at;
            let mut after: HashMap<ClientOrderId, i128> = HashMap::new();
            let held: &[Held] = match self.markets.get(&inst) {
                Some(MarketState::Unseeded(held)) => held,
                _ => &[],
            };
            for h in held {
                match reference.place(&h.fill) {
                    Placement::Unsettled => {
                        report.unsettled.push((inst, h.key.clone()));
                        continue 'market;
                    }
                    Placement::InSnapshot { .. } => {}
                    Placement::After => {
                        pos += i128::from(h.signed.0);
                        folded_to = folded_to.max(h.fill.arrived);
                        *after.entry(h.fill.cid).or_insert(0) += i128::from(h.fill.qty.get());
                    }
                }
            }
            let pos = i64::try_from(pos).map_err(|_| overflow.clone())?;
            let mut baselines = Vec::new();
            for (&cid, &cum) in &reference.shown {
                let extra = after.get(&cid).copied().unwrap_or(0);
                let total = i64::try_from(i128::from(cum.get()) + extra)
                    .ok()
                    .and_then(Lots::new)
                    .ok_or(overflow.clone())?;
                baselines.push((cid, total));
            }
            baselines.sort();
            plans.push(SeedPlan {
                inst,
                pos: SignedLots(pos),
                folded_to,
                baselines,
                reference,
            });
        }
        Ok(plans)
    }

    /// Applies one planned seed: sets the fill count of each order it shows that the registry
    /// holds (those [`Registry::register_shown`] registered included) to the cumulative fill it
    /// showed plus the fills after it, and the position.
    fn seed(&mut self, plan: SeedPlan, report: &mut ResyncReport) {
        let SeedPlan {
            inst,
            pos,
            folded_to,
            baselines,
            mut reference,
        } = plan;
        for (cid, cum) in baselines {
            if self.orders.contains_key(&cid) {
                self.with_record(cid, |rec| rec.set_cum_fills(cum));
            }
        }
        reference.known = self
            .orders
            .values()
            .filter(|rec| rec.placed().inst == inst)
            .map(OrderRecord::cid)
            .collect();
        self.inventory.insert(inst, pos);
        self.markets.insert(
            inst,
            MarketState::Seeded(Seed {
                reference: Some(Box::new(reference)),
                base: i128::from(pos.0),
                folded_to,
                log: Vec::new(),
            }),
        );
        report.seeded.push((inst, pos));
    }

    /// Compares a seeded market's position with the venue's `venue` in `snap`.
    fn check(
        &mut self,
        inst: InstrumentId,
        snap: &ResyncSnapshot,
        venue: SignedLots,
    ) -> PositionCheck {
        let Some(MarketState::Seeded(seed)) = self.markets.get_mut(&inst) else {
            return PositionCheck::Unsettled(inst);
        };
        if snap.requested_at < seed.folded_to {
            return PositionCheck::Stale(inst);
        }
        let mut kept = Vec::new();
        for (arrived, exec, signed) in seed.log.drain(..) {
            if arrived <= snap.requested_at {
                seed.base += signed;
            } else {
                kept.push((arrived, exec, signed));
            }
        }
        seed.log = kept;
        seed.folded_to = snap.requested_at;
        let as_of = seed.base
            + seed
                .log
                .iter()
                .filter(|(_, exec, _)| exec.is_some_and(|t| t <= snap.watermark))
                .map(|(_, _, signed)| signed)
                .sum::<i128>();
        if as_of == i128::from(venue.0) {
            PositionCheck::Agrees {
                inst,
                position: venue,
            }
        } else {
            PositionCheck::Desync {
                inst,
                venue,
                ledger: i64::try_from(as_of).ok().map(SignedLots),
            }
        }
    }
}

/// A seed decided before anything applies.
struct SeedPlan {
    inst: InstrumentId,
    pos: SignedLots,
    folded_to: MonoNs,
    baselines: Vec<(ClientOrderId, Lots)>,
    reference: SeedRef,
}
