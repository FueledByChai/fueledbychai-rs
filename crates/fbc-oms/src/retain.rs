//! The retention of terminal orders (decision 0157): the one pass that forgets an order the
//! registry holds, [`Registry::retain_ended`], run by the consumer on its journaled timer at its
//! time on the shard's monotonic clock, under its [`RetentionConfig`].
//!
//! The pass stamps each terminal record it sees for the first time with its time, then keeps
//! every record that is not terminal, every record a pin holds (item 3: ended Lost, ended by an
//! accepted tombstone, owing a fill, owing an answer, named by a command or a planner slot, on a
//! market not seeded, inserted after a resync's seed, or sharing a venue id with another order
//! the registry holds), and every record whose stamp is not yet the venue's snapshot lag bound
//! plus the consumer's resync bound old (settling). Of the others, the eligible records, it
//! forgets each at least the consumer's age old, then the oldest by stamp and client id while
//! more than the consumer's count remain. A record is forgotten whole, with every index entry
//! naming it (item 4).
//!
//! Nothing is kept of a forgotten order but two spans per namespace (item 5): the lowest and
//! highest sequence of the forgotten orders this registry built, and of every forgotten order.
//! A late fill under a client id the first span covers is placed against its market's seed as
//! its held record's fill would be (item 6); an outcome or a sent report under a client id the
//! second covers gets a terminal record's answer; a place or an insert under a client id the
//! first covers is refused (item 7). A registry whose consumer never runs the pass keeps every
//! order, as before. The OMS reads no clock.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use fbc_core::{ClientOrderId, MonoNs, Namespace, OrderCaps, VenueOrderId};

use crate::ladder;
use crate::registry::Registry;

/// The retention pass's numbers: the consumer's configuration, with no defaults (decision 0157
/// item 2).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RetentionConfig {
    max_ended: usize,
    max_age: Duration,
    resync_within: Duration,
}

/// A [`RetentionConfig`] the OMS refuses.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RetentionConfigError {
    /// The count is zero: every eligible record would be forgotten at once.
    ZeroEnded,
    /// The age is zero: every eligible record would be forgotten at once.
    ZeroAge,
    /// The resync bound is zero: no resync applied after its request could be covered.
    ZeroResync,
}

impl fmt::Display for RetentionConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetentionConfigError::ZeroEnded => {
                write!(f, "the retention's count of ended orders is zero")
            }
            RetentionConfigError::ZeroAge => write!(f, "the retention's age is zero"),
            RetentionConfigError::ZeroResync => {
                write!(f, "the retention's resync bound is zero")
            }
        }
    }
}

impl std::error::Error for RetentionConfigError {}

impl RetentionConfig {
    /// The retention's numbers: how many eligible ended records a pass keeps at most
    /// (`max_ended`), the age at which a pass forgets an eligible one (`max_age`, from when a
    /// pass first saw it terminal), and the longest time from a resync snapshot's
    /// `requested_at` to the [`Registry::resync`] call that applies it (`resync_within`).
    /// Refused when one of them is zero.
    ///
    /// The resync bound, with the venue's snapshot lag bound, floors the age at which an ended
    /// record may be forgotten (decision 0157 item 3). For fbc-live's link, whose snapshots take
    /// the receive time of the socket's authentication as their `requested_at`, it covers the
    /// sum of three times (item 8): the rest of the authenticating turn up to the session's ask
    /// for the resync (the handler's work on that input, the codec's frames and the arm, and
    /// the frames the effects below add, each write bounded by the session's `WriteStall`
    /// window; the HTTP results and timer firings taken while a write waits, which reach the
    /// codec and the handler; and, on a shard host, the host's other work while a write waits,
    /// its decide pass included); the session's `ResyncDeadline`; and how long the turn under
    /// way when that deadline passes may still run before its check, with the HTTP results and
    /// timer firings it takes while one of its writes waits and, on a shard host, the host's
    /// other work it waits on, since a `ResyncEnd` it hands on first, from its input or from
    /// one of those results, ends the resync. A bound set short may register a forgotten order
    /// as an orphan, which fails closed.
    pub fn new(
        max_ended: usize,
        max_age: Duration,
        resync_within: Duration,
    ) -> Result<RetentionConfig, RetentionConfigError> {
        if max_ended == 0 {
            return Err(RetentionConfigError::ZeroEnded);
        }
        if max_age.is_zero() {
            return Err(RetentionConfigError::ZeroAge);
        }
        if resync_within.is_zero() {
            return Err(RetentionConfigError::ZeroResync);
        }
        Ok(RetentionConfig {
            max_ended,
            max_age,
            resync_within,
        })
    }

    /// How many eligible ended records a pass keeps at most.
    pub fn max_ended(&self) -> usize {
        self.max_ended
    }

    /// The age at which a pass forgets an eligible ended record.
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// The longest time from a resync snapshot's request to the call that applies it.
    pub fn resync_within(&self) -> Duration {
        self.resync_within
    }
}

/// What one retention pass did ([`Registry::retain_ended`]).
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct Retained {
    /// The orders it forgot, in client id order.
    pub forgotten: Vec<ClientOrderId>,
    /// The eligible ended records it kept: at most the configuration's count.
    pub kept: usize,
    /// The ended records it kept as settling: no pin holds them, and their stamp is not yet
    /// the venue's snapshot lag bound plus the resync bound old.
    pub settling: usize,
    /// How many of the records it forgot the count forgot before they reached the age.
    pub early: usize,
    /// The ended records a pin kept, by the first reason that applies.
    pub pinned: Pinned,
}

/// The ended records a retention pass kept for a pin, by reason (decision 0157 item 3).
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct Pinned {
    /// Ended Lost (a).
    pub lost: usize,
    /// Ended Canceled by an accepted tombstone (b).
    pub tombstoned: usize,
    /// The venue reported fills on it the ledger has not counted (c).
    pub fills_owed: usize,
    /// An answer to its place, or to an amend, a cancel or a tombstone reported sent, is owed
    /// (d).
    pub answer_owed: usize,
    /// A command, an authorization or a handed authorization naming it, or a planner slot
    /// holding it, lives (d and e).
    pub named: usize,
    /// Its market is not seeded (f).
    pub unseeded: usize,
    /// Registered by an insert after its market's resync seed (g).
    pub inserted: usize,
    /// A venue id the index gives to one order the registry holds is known by another (h).
    pub shared_vid: usize,
}

/// Why a retention pass keeps an ended record (decision 0157 item 3), in the order checked.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum PinReason {
    Lost,
    Tombstoned,
    FillsOwed,
    AnswerOwed,
    Named,
    Unseeded,
    Inserted,
    SharedVid,
}

impl Pinned {
    fn count(&mut self, reason: PinReason) {
        let counter = match reason {
            PinReason::Lost => &mut self.lost,
            PinReason::Tombstoned => &mut self.tombstoned,
            PinReason::FillsOwed => &mut self.fills_owed,
            PinReason::AnswerOwed => &mut self.answer_owed,
            PinReason::Named => &mut self.named,
            PinReason::Unseeded => &mut self.unseeded,
            PinReason::Inserted => &mut self.inserted,
            PinReason::SharedVid => &mut self.shared_vid,
        };
        *counter += 1;
    }
}

/// A span of sequences in one namespace: the lowest and the highest.
pub(crate) type Spans = HashMap<Namespace, (u64, u64)>;

/// Whether `cid`'s sequence lies within its namespace's span in `spans`.
fn within(spans: &Spans, cid: ClientOrderId) -> bool {
    spans
        .get(&cid.namespace())
        .is_some_and(|&(low, high)| (low..=high).contains(&cid.seq()))
}

/// Whether `now` is at least `after` past `stamp` on the monotonic clock. An instant past the
/// clock's end is never reached, so a floor or an age that would end there holds for good,
/// even at the clock's last instant.
fn reached(now: MonoNs, stamp: MonoNs, after: Duration) -> bool {
    u64::try_from(after.as_nanos())
        .ok()
        .and_then(|after| stamp.0.checked_add(after))
        .is_some_and(|at| now.0 >= at)
}

/// Widens `cid`'s namespace's span in `spans` to its sequence.
fn widen(spans: &mut Spans, cid: ClientOrderId) {
    let seq = cid.seq();
    spans
        .entry(cid.namespace())
        .and_modify(|(low, high)| {
            *low = (*low).min(seq);
            *high = (*high).max(seq);
        })
        .or_insert((seq, seq));
}

impl Registry {
    /// One retention pass at `now`, the consumer's time on the shard's monotonic clock, under
    /// the consumer's `cfg` and the venue's `caps` (decision 0157): the only call that forgets
    /// an order. Like every mutating call, it first applies the Cancel-only trips (decision
    /// 0139) and releases what dropped commands and authorizations handed back (decisions
    /// 0084, 0152).
    ///
    /// It stamps each terminal record it sees for the first time with `now`. A terminal record
    /// is eligible when no pin holds it and its stamp is at least the venue's snapshot lag bound
    /// (`caps.snapshot_lag_bound`) plus `cfg`'s resync bound before `now`. Of the eligible
    /// records, it forgets each at least `cfg`'s age old, then, while more than `cfg`'s count
    /// remain, the oldest by stamp, then by client id. A floor or an age that would end past
    /// the monotonic clock's end is never reached: the record stays settling, or young. A
    /// record that is not terminal, pinned or settling is never forgotten. Each forgotten
    /// record goes in one step with every venue id, ladder query and orphan entry naming it,
    /// and widens its namespace's spans (the module documentation). The consumer runs it on a
    /// journaled timer, after reading the notes of the events before it, and records each
    /// amend's, cancel's and tombstone's send report before it
    /// ([`OrderGateway::submit`](crate::OrderGateway::submit)).
    pub fn retain_ended(
        &mut self,
        cfg: &RetentionConfig,
        caps: &OrderCaps,
        now: MonoNs,
    ) -> Retained {
        self.reap_dropped();
        for rec in self.orders.values_mut() {
            if rec.state().is_terminal() {
                rec.stamp_ended(now);
            }
        }
        let shared = self.shared_vid_pins();
        let mut report = Retained::default();
        let mut eligible: Vec<(MonoNs, ClientOrderId)> = Vec::new();
        for rec in self.orders.values() {
            let Some(stamp) = rec.ended_seen().filter(|_| rec.state().is_terminal()) else {
                continue;
            };
            let cid = rec.cid();
            let inst = rec.placed().inst;
            let reason = rec.pin().or_else(|| {
                if !self.market_seeded(inst) {
                    Some(PinReason::Unseeded)
                } else if !rec.built_here() && self.seed_lacks(inst, cid) {
                    Some(PinReason::Inserted)
                } else if shared.contains(&cid) {
                    Some(PinReason::SharedVid)
                } else {
                    None
                }
            });
            if let Some(reason) = reason {
                report.pinned.count(reason);
                continue;
            }
            let floor = caps.snapshot_lag_bound.checked_add(cfg.resync_within);
            if floor.is_some_and(|floor| reached(now, stamp, floor)) {
                eligible.push((stamp, cid));
            } else {
                report.settling += 1;
            }
        }
        eligible.sort();
        let mut gone: HashSet<ClientOrderId> = HashSet::new();
        let mut young = Vec::new();
        for (stamp, cid) in eligible {
            if reached(now, stamp, cfg.max_age) {
                gone.insert(cid);
            } else {
                young.push(cid);
            }
        }
        let excess = young.len().saturating_sub(cfg.max_ended);
        for &cid in &young[..excess] {
            gone.insert(cid);
        }
        report.early = excess;
        report.kept = young.len() - excess;
        self.forget_whole(&gone);
        report.forgotten = gone.into_iter().collect();
        report.forgotten.sort();
        report
    }

    /// The ended records pin h holds (decision 0157 item 3h): each a venue id the index gives
    /// it is known by another record the registry holds, and each knowing a venue id the index
    /// gives to another order the registry holds that has not ended, ended Lost, or is still
    /// among its market's orphans.
    fn shared_vid_pins(&self) -> HashSet<ClientOrderId> {
        let mut knowers: HashMap<&VenueOrderId, Vec<ClientOrderId>> = HashMap::new();
        for rec in self.orders.values() {
            for vid in rec.known_vids() {
                knowers.entry(vid).or_default().push(rec.cid());
            }
        }
        let mut shared = HashSet::new();
        let indexed = knowers
            .into_iter()
            .filter_map(|(vid, cids)| Some((self.cid_of(vid)?, cids)));
        for (holder, cids) in indexed {
            for cid in cids.into_iter().filter(|&c| c != holder) {
                // The index gives `vid` to `holder`, which `cid` knows too.
                shared.insert(holder);
                let rec = &self.orders[&holder];
                let state = rec.state();
                if !state.is_terminal()
                    || ladder::is_lost(state)
                    || self.orphans.holds(rec.placed().inst, holder)
                {
                    shared.insert(cid);
                }
            }
        }
        shared
    }

    /// Forgets every order in `gone` in one step (decision 0157 item 4): its record, every venue
    /// id the index maps to it, every ladder query naming it and its orphan entry; and widens
    /// its namespace's spans with its sequence (item 5).
    fn forget_whole(&mut self, gone: &HashSet<ClientOrderId>) {
        if gone.is_empty() {
            return;
        }
        for cid in gone {
            let rec = self.orders.remove(cid).expect("a record forgotten is held");
            widen(&mut self.answer_span, *cid);
            if rec.built_here() {
                widen(&mut self.built_span, *cid);
            }
        }
        self.unindex(gone);
        self.queries.retain(|_, cid| !gone.contains(cid));
        self.orphans.forget(gone);
    }

    /// Whether `cid` is no order the registry holds and lies within its namespace's built span:
    /// an order this registry built and forgot, by every client id it registered (decision 0157
    /// items 5 to 7).
    pub(crate) fn forgot_here(&self, cid: ClientOrderId) -> bool {
        !self.orders.contains_key(&cid) && within(&self.built_span, cid)
    }

    /// Whether `cid` is no order the registry holds and lies within its namespace's answer span:
    /// an outcome or a sent report naming it gets a terminal record's answer (decision 0157 item
    /// 6).
    pub(crate) fn forgot_any(&self, cid: ClientOrderId) -> bool {
        !self.orders.contains_key(&cid) && within(&self.answer_span, cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retention_config_refuses_zero_for_each_number_and_keeps_what_it_is_given() {
        let s = Duration::from_secs(1);
        assert_eq!(
            RetentionConfig::new(0, s, s),
            Err(RetentionConfigError::ZeroEnded)
        );
        assert_eq!(
            RetentionConfig::new(1, Duration::ZERO, s),
            Err(RetentionConfigError::ZeroAge)
        );
        assert_eq!(
            RetentionConfig::new(1, s, Duration::ZERO),
            Err(RetentionConfigError::ZeroResync)
        );
        let cfg = RetentionConfig::new(3, s, 2 * s).unwrap();
        assert_eq!(
            (cfg.max_ended(), cfg.max_age(), cfg.resync_within()),
            (3, s, 2 * s)
        );
        for err in [
            RetentionConfigError::ZeroEnded,
            RetentionConfigError::ZeroAge,
            RetentionConfigError::ZeroResync,
        ] {
            assert!(!err.to_string().is_empty());
        }
    }

    /// Decision 0157 item 4: an orphan forgotten leaves no orphan entry naming it.
    #[test]
    fn a_forgotten_orphan_leaves_no_orphan_entry() {
        use fbc_core::{
            CidMatch, InstrumentId, Lots, Side, SignedLots, Ticks, VenueOrderSnapshot,
            VenueOrderState, WallNs,
        };

        use crate::common::{canceled, cid, order_caps, update, vid};
        use crate::{LadderConfig, OrderKey, ResyncSnapshot};

        const INST: InstrumentId = InstrumentId::new(1);
        let key = |ingest| OrderKey {
            venue: None,
            ingest,
        };
        let ladder = LadderConfig::new(
            Duration::from_secs(1),
            Duration::ZERO,
            Duration::from_secs(10),
            1,
        )
        .unwrap();
        let caps = order_caps();
        let snap = |at: u64, orders| ResyncSnapshot {
            watermark: WallNs(i64::try_from(at).unwrap()),
            requested_at: MonoNs(at),
            orders,
            positions: vec![(INST, SignedLots(0))],
        };
        let mut reg = Registry::new();
        reg.resync(&ladder, &caps, &snap(1, vec![]), key(1))
            .unwrap();
        let c = cid();
        let row = VenueOrderSnapshot {
            cid: Some(CidMatch::Ours(c)),
            vid: vid("o-1"),
            inst: INST,
            side: Side::Buy,
            state: VenueOrderState::Open,
            px: Some(Ticks(100)),
            qty: Lots::new(1).unwrap(),
            cum_filled: Lots::ZERO,
            post_only: None,
            reduce_only: None,
        };
        reg.resync(&ladder, &caps, &snap(10, vec![row]), key(2))
            .unwrap();
        assert!(reg.orphans.holds(INST, c));
        reg.apply_update(&update(Some(c), canceled(), 0), key(3));
        let cfg =
            RetentionConfig::new(10, Duration::from_millis(1), Duration::from_millis(1)).unwrap();
        reg.retain_ended(&cfg, &caps, MonoNs(100));
        let r = reg.retain_ended(&cfg, &caps, MonoNs(100) + Duration::from_millis(2));
        assert_eq!(r.forgotten, vec![c]);
        assert!(!reg.orphans.holds(INST, c));
    }

    #[test]
    fn a_span_holds_every_sequence_between_its_ends_in_its_namespace_alone() {
        let mut spans = Spans::new();
        let (a, b) = (crate::common::cid(), crate::common::cid());
        assert!(!within(&spans, a));
        widen(&mut spans, b);
        assert!(within(&spans, b) && !within(&spans, a));
        widen(&mut spans, a);
        assert!(within(&spans, a) && within(&spans, b));
        assert_eq!(spans.len(), 1);
    }
}
