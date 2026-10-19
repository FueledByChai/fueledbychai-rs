//! Orphans (0005's I7; decision 0101): open orders under our namespace that the registry does
//! not hold, found by a resync on a market a resync (or a hand seed) already seeded: the
//! library side of the fix for the Hibachi orphan blowout.
//!
//! **Found.** [`Registry::resync`] reports each such order ([`ResyncReport::untracked`](crate::ResyncReport))
//! and acts on it: from a trustworthy snapshot source on every one, from any other only on one
//! a live order event or fill under its client id also showed while the registry did not hold
//! it, and no terminal event withdrew since; the rest stay in `untracked` only. Acting registers it as an order from the snapshot
//! ([`OrderRecord::from_snapshot`](crate::OrderRecord::from_snapshot)): never amended, counted
//! against both pre-trade caps as it rests, and cancelled by the kill switch's cancel everything
//! too. Shown open after the seed, the seed holds a fill of it only by the fill's arrival before
//! the seed's request or its matching-engine time at or before the seed's watermark; any other
//! moves the inventory, on either read mode (`src/resync.rs`). An order
//! not ours (another namespace's, a non-canonical client id, or one nothing attributes) is
//! never one (I4).
//!
//! **Suppressed.** From the resync that finds orphans on a market, and again from each resync
//! while any is not terminal, the market's adding side is suppressed: a place, a batch item or
//! an amend is built only on the side that reduces the known, non-zero position, with what the
//! side may still move it by, the order included, within the position's size (so on a flat
//! market, or one whose position is unknown, none is). The cooldown waits
//! ([`Cooldown::Pending`]) until the consumer's [`Registry::cancel_orphans`] at `now` starts it
//! ([`Cooldown::Until`], `now` plus the consumer's [`OrphanConfig::cooldown`]), and it ends
//! only when the consumer's [`Registry::end_orphan_cooldowns`] is given a time at or past it.
//! A suppression beginning advances the market's state generation, so a place or amend
//! authorized before it is refused at submit. The position sizes the build, so the suppression
//! is judged again at authorization ([`IssueRefusal::OrphanCooldown`](crate::IssueRefusal)),
//! and a command built during the cooldown is refused at submit once the inventory moved since
//! its build ([`StaleAuthorization::PositionMoved`](crate::StaleAuthorization)), as an Exit
//! command is (0066). Cancels are never suppressed.
//!
//! **Cancelled.** [`Registry::cancel_orphans`] builds, in every market state, the cancels of
//! the market's orphans that are not terminal, by explicit reference in a cancel-many
//! ([`Registry::cancel_many`]), a cancel in flight included. Above the consumer's level limit
//! ([`OrphanConfig::level_limit`]) it escalates to an instrument cancel-all, only under the
//! same guard as the kill switch's cancel everything (the venue has one, the registry holds the
//! market's exclusive lease, a trustworthy resync has shown the account's open orders and no
//! order not ours is in view on the market, a snapshot's included); otherwise it says why and
//! builds the explicit cancels. Each resync names the markets that still hold orphans
//! ([`ResyncReport::orphans`](crate::ResyncReport)), so the consumer builds them again: the
//! explicit cancels repeat on each resync. An account cancel-all is never built.
//!
//! The OMS reads no clock: every time here is the consumer's, on the shard's monotonic clock.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use fbc_core::{ClientOrderId, InstrumentId, MonoNs, OrderCaps, VenueOrderState};

use crate::caps::Exposure;
use crate::permit::{CancelPlan, PermittedCommand};
use crate::registry::Registry;
use crate::sweep::CancelAllRefusal;

/// The consumer's orphan settings (0005's I7), with no defaults.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct OrphanConfig {
    cooldown: Duration,
    level_limit: u32,
}

/// An [`OrphanConfig`] the OMS refuses.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrphanConfigError {
    /// The cooldown is zero: the adding side would not be suppressed at all.
    ZeroCooldown,
    /// The level limit is zero: every orphan would escalate to an instrument cancel-all.
    ZeroLevelLimit,
}

impl fmt::Display for OrphanConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OrphanConfigError::ZeroCooldown => "the orphan cooldown is zero",
            OrphanConfigError::ZeroLevelLimit => "the orphan level limit is zero",
        })
    }
}

impl std::error::Error for OrphanConfigError {}

impl OrphanConfig {
    /// How long a market's adding side stays suppressed once [`Registry::cancel_orphans`]
    /// starts its cooldown (`cooldown`), and how many orphans a market may hold before their
    /// cancel escalates to an instrument cancel-all (`level_limit`). Refused when either is
    /// zero.
    pub fn new(cooldown: Duration, level_limit: u32) -> Result<OrphanConfig, OrphanConfigError> {
        if cooldown.is_zero() {
            return Err(OrphanConfigError::ZeroCooldown);
        }
        if level_limit == 0 {
            return Err(OrphanConfigError::ZeroLevelLimit);
        }
        Ok(OrphanConfig {
            cooldown,
            level_limit,
        })
    }

    /// How long the adding side stays suppressed once the cooldown starts.
    pub fn cooldown(&self) -> Duration {
        self.cooldown
    }

    /// How many orphans a market may hold before their cancel escalates.
    pub fn level_limit(&self) -> u32 {
        self.level_limit
    }
}

/// A market's orphan cooldown: its adding side is suppressed while it lasts.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Cooldown {
    /// A resync found orphans on the market, and the consumer's [`Registry::cancel_orphans`]
    /// has not started the cooldown since.
    Pending,
    /// Until this time on the consumer's monotonic clock.
    Until(MonoNs),
}

/// What [`Registry::cancel_orphans`] built for one market.
#[derive(Eq, PartialEq, Debug)]
pub struct OrphanPlan {
    /// The market.
    pub market: InstrumentId,
    /// Its orphans that are not terminal, in client id order.
    pub orphans: Vec<ClientOrderId>,
    /// Their cancels.
    pub cancels: OrphanCancels,
    /// The market's cooldown after the call: `None` once none suppresses it.
    pub cooldown: Option<Cooldown>,
}

/// The cancels of a market's orphans.
#[derive(Eq, PartialEq, Debug)]
pub enum OrphanCancels {
    /// The orphans by explicit reference, as [`Registry::cancel_many`] built them; with
    /// `escalation`, why no instrument cancel-all was built for orphans above the level limit
    /// (`None` at or below it).
    Explicit {
        plan: CancelPlan,
        escalation: Option<CancelAllRefusal>,
    },
    /// Above the level limit, under 0005's I7 guard: the instrument cancel-all, with the
    /// explicit cancels of our orders on the market the venue may not hold yet (PendingNew or
    /// Unknown), which it can miss.
    CancelAll {
        command: PermittedCommand,
        unanswered: CancelPlan,
    },
}

/// The registry's orphans, the markets whose adding side they suppress, and the client ids of
/// ours a live event showed while the registry did not hold them.
#[derive(Debug, Default)]
pub(crate) struct Orphans {
    /// The orphans registered, per market; terminal ones are dropped as they are read.
    by_market: HashMap<InstrumentId, BTreeSet<ClientOrderId>>,
    cooldowns: HashMap<InstrumentId, Cooldown>,
    /// Our client ids a live order event or fill showed while the registry did not hold
    /// them, and no terminal event withdrew since: what confirms an untrustworthy snapshot.
    live: HashSet<ClientOrderId>,
}

impl Orphans {
    /// A live order event in `state` under our client id `cid`, which the registry does not
    /// hold: a terminal one withdraws an earlier sighting.
    pub(crate) fn seen_event(&mut self, cid: ClientOrderId, state: &VenueOrderState) {
        match state {
            VenueOrderState::Open | VenueOrderState::Amended { .. } => {
                self.live.insert(cid);
            }
            VenueOrderState::Filled
            | VenueOrderState::Canceled(_)
            | VenueOrderState::Rejected(_)
            | VenueOrderState::Expired => {
                self.live.remove(&cid);
            }
        }
    }

    /// A live fill under our client id `cid`, which the registry does not hold.
    pub(crate) fn seen_fill(&mut self, cid: ClientOrderId) {
        self.live.insert(cid);
    }

    /// Whether a live event showed `cid`.
    pub(crate) fn seen_live(&self, cid: ClientOrderId) -> bool {
        self.live.contains(&cid)
    }

    /// A trustworthy resync: the live sightings confirm nothing it does not show already.
    pub(crate) fn forget_live(&mut self) {
        self.live.clear();
    }

    /// Registers the orphan `cid` on `market`.
    pub(crate) fn add(&mut self, market: InstrumentId, cid: ClientOrderId) {
        self.live.remove(&cid);
        self.by_market.entry(market).or_default().insert(cid);
    }

    /// Suppresses `market`'s adding side until the consumer's next cancel of its orphans
    /// starts the cooldown; true when it was not suppressed before.
    pub(crate) fn suppress(&mut self, market: InstrumentId) -> bool {
        self.cooldowns.insert(market, Cooldown::Pending).is_none()
    }

    pub(crate) fn cooldown(&self, market: InstrumentId) -> Option<Cooldown> {
        self.cooldowns.get(&market).copied()
    }
}

impl Registry {
    /// The orphans on `market` that are not terminal, in client id order (0005's I7).
    pub fn orphans(&self, market: InstrumentId) -> Vec<ClientOrderId> {
        self.orphans
            .by_market
            .get(&market)
            .into_iter()
            .flatten()
            .copied()
            .filter(|cid| {
                self.orders
                    .get(cid)
                    .is_some_and(|rec| !rec.state().is_terminal())
            })
            .collect()
    }

    /// The markets holding orphans that are not terminal, in id order; drops the terminal ones.
    pub(crate) fn orphan_markets(&mut self) -> Vec<InstrumentId> {
        let orders = &self.orders;
        self.orphans.by_market.retain(|_, cids| {
            cids.retain(|cid| orders.get(cid).is_some_and(|r| !r.state().is_terminal()));
            !cids.is_empty()
        });
        let mut markets: Vec<InstrumentId> = self.orphans.by_market.keys().copied().collect();
        markets.sort();
        markets
    }

    /// `market`'s orphan cooldown, while one suppresses its adding side.
    pub fn orphan_cooldown(&self, market: InstrumentId) -> Option<Cooldown> {
        self.orphans.cooldown(market)
    }

    /// Ends every orphan cooldown that ran out by `now`, the consumer's time on the shard's
    /// monotonic clock, and names those markets, in id order: their adding side is no longer
    /// suppressed. A cooldown still waiting for the cancel of its orphans never ends here.
    pub fn end_orphan_cooldowns(&mut self, now: MonoNs) -> Vec<InstrumentId> {
        let mut ended: Vec<InstrumentId> = self
            .orphans
            .cooldowns
            .iter()
            .filter(|(_, c)| matches!(c, Cooldown::Until(until) if *until <= now))
            .map(|(m, _)| *m)
            .collect();
        ended.sort();
        for m in &ended {
            self.orphans.cooldowns.remove(m);
        }
        ended
    }

    /// Whether an order on `exposure`'s side bringing `adds` to what the side may still move the
    /// position by is refused while the market's adding side is suppressed: unless it is on
    /// the side that reduces the known, non-zero position and the side's total stays within
    /// the position's size.
    pub(crate) fn orphan_suppressed(&self, exposure: &Exposure, adds: fbc_core::Lots) -> bool {
        self.orphans.cooldown(exposure.inst).is_some() && exposure.admit_exit(true, adds).is_err()
    }

    /// The cancels of `market`'s orphans on a venue whose order caps are `caps` (0005's I7),
    /// built in every market state, at `now`, the consumer's time on the shard's monotonic
    /// clock: by explicit reference in a cancel-many, the orphans with a cancel in flight
    /// included; above `cfg`'s level limit an instrument cancel-all instead, only under the
    /// kill switch's guard (the venue has one, the registry holds the market's exclusive lease,
    /// a trustworthy resync has shown the account's open orders, and no order not ours is in
    /// view on the market), with the explicit cancels of our orders on it the venue may not
    /// hold yet. Never an account cancel-all, and never an order not ours.
    ///
    /// Starts the market's cooldown when it waits for this call, or holds orphans: until `now`
    /// plus `cfg`'s cooldown, or later if it already ran later.
    pub fn cancel_orphans(
        &mut self,
        market: InstrumentId,
        cfg: &OrphanConfig,
        caps: &OrderCaps,
        now: MonoNs,
    ) -> OrphanPlan {
        self.reap_dropped();
        let orphans = self.orphans(market);
        let cooldown = self.orphans.cooldown(market);
        if !orphans.is_empty() || cooldown == Some(Cooldown::Pending) {
            let from_now = MonoNs(
                now.0
                    .saturating_add(u64::try_from(cfg.cooldown.as_nanos()).unwrap_or(u64::MAX)),
            );
            let until = match cooldown {
                Some(Cooldown::Until(later)) if later > from_now => later,
                _ => from_now,
            };
            self.orphans
                .cooldowns
                .insert(market, Cooldown::Until(until));
        }
        let above = u32::try_from(orphans.len()).map_or(true, |n| n > cfg.level_limit);
        let cancels = match (above, above.then(|| self.cancel_all_refusal(market, caps))) {
            (true, Some(None)) => {
                let (command, unanswered) = self.instrument_cancel_all(market, caps);
                OrphanCancels::CancelAll {
                    command,
                    unanswered,
                }
            }
            (_, escalation) => OrphanCancels::Explicit {
                plan: self.cancel_many(&orphans, caps),
                escalation: escalation.flatten(),
            },
        };
        OrphanPlan {
            market,
            orphans,
            cancels,
            cooldown: self.orphans.cooldown(market),
        }
    }
}
