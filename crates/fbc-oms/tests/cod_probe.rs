//! An owner-supervised cancel-on-disconnect probe arms its one market on a flat hand seed
//! (decision 0140 item 6, the owner's option B of 2026-10-21, augmenting 0067 and 0055).
//!
//! A registry built with [`Registry::for_cod_probe`] from
//! [`CodProbeRun::owner_supervised`]`(market)` no longer refuses Start, Flatten and Wind-down on
//! that market as [`ArmRefusal::SeededByHand`] when its hand seed, as seeded, was flat, whatever
//! the fills moved it to since. Nothing else changes: a hand seed of any other position, or on
//! any other market, is still refused, on a registry declared a testnet run too ("not a testnet
//! run"); a registry keeps its first declaration, so it names one market; an unknown position,
//! the kill switch and the leases refuse as before; a registry without the declaration refuses a
//! flat hand seed; [`Registry::for_testnet_run`] alone arms as it did; and a later resync only
//! compares the seed, reporting a desync, under a trustworthy source and under the
//! untrustworthy, separate reads the probe's codec declares (decisions 0055, 0140 item 7).

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{leases, market_lease, named, seed};
use common::{lots, no_fee, venue_fill};
use fbc_core::{
    InstrumentId, MonoNs, OrderCaps, PxExact, Side, SignedLots, SnapshotReads, SnapshotSource,
    VenueFillKind, WallNs,
};
use fbc_oms::{
    Admission, ArmRefusal, CodProbeRun, EntryState, ExitKind, FillLedger, FillRouted, LadderConfig,
    Leases, LedgerConfig, MarketCapsConfig, MarketEntry, OrderKey, PositionCheck, PreTradeCaps,
    Registry, ResyncReport, ResyncSnapshot, TestnetRun,
};

/// The market the probe declares.
const INST: InstrumentId = InstrumentId::new(1);
/// Another market the registry's lease names cover.
const OTHER: InstrumentId = InstrumentId::new(2);

type Arm = fn(&mut Registry, InstrumentId, Leases) -> Result<MarketEntry, ArmRefusal>;

/// Each arming call with the state it arms into.
fn arming_calls() -> [(&'static str, Arm, EntryState); 3] {
    [
        ("start", Registry::start, EntryState::Quoting),
        (
            "flatten",
            Registry::flatten,
            EntryState::Exit(ExitKind::Flatten),
        ),
        (
            "wind-down",
            Registry::wind_down,
            EntryState::Exit(ExitKind::WindDown),
        ),
    ]
}

fn caps() -> PreTradeCaps {
    [INST, OTHER]
        .into_iter()
        .fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(50)),
                    resting: Some(lots(1_000)),
                },
            )
            .unwrap()
        })
}

/// A named registry declared the probe's, on `INST`.
fn probe() -> Registry {
    named(Registry::with_caps(caps()).for_cod_probe(CodProbeRun::owner_supervised(INST)))
}

/// A named registry with no declaration.
fn live() -> Registry {
    named(Registry::with_caps(caps()))
}

/// A named registry declared an owner-assisted testnet run.
fn testnet() -> Registry {
    named(Registry::with_caps(caps()).for_testnet_run(TestnetRun::owner_assisted()))
}

fn seen(e: MarketEntry) -> (bool, EntryState, u64) {
    (e.armed(), e.state(), e.generation().get())
}

/// Calls `call` on `market` with `given` and asserts it refused with `why`, changing nothing.
fn refused_with(
    reg: &mut Registry,
    market: InstrumentId,
    name: &str,
    call: Arm,
    given: Leases,
    why: ArmRefusal,
) {
    let before = seen(reg.entry(market));
    assert_eq!(call(reg, market, given), Err(why), "{name}");
    assert_eq!(seen(reg.entry(market)), before, "{name}");
    assert!(!before.0, "{name}");
}

/// Calls `call` on `market` with its leases and asserts it refused with `why`, changing
/// nothing.
fn refused(reg: &mut Registry, market: InstrumentId, name: &str, call: Arm, why: ArmRefusal) {
    let now = leases(reg, market);
    refused_with(reg, market, name, call, now, why);
}

/// Calls `call` on `market` with its leases and asserts it armed the market into `to`.
fn arms(reg: &mut Registry, market: InstrumentId, name: &str, call: Arm, to: EntryState) {
    let now = leases(reg, market);
    let entry = call(reg, market, now).unwrap();
    assert!(entry.armed(), "{name}");
    assert_eq!(entry.state(), to, "{name}");
}

/// The probe codec's snapshot caps (decision 0140 item 7): untrustworthy, read separately.
fn probe_caps() -> OrderCaps {
    OrderCaps {
        snapshot_source: SnapshotSource::Untrustworthy,
        snapshot_reads: SnapshotReads::Separate,
        ..common::order_caps()
    }
}

/// A resync answered by a trustworthy snapshot of no orders showing `pos` on `INST`.
fn resync(reg: &mut Registry, at: u64, pos: i64) -> ResyncReport {
    resync_under(reg, &common::order_caps(), at, pos)
}

/// A resync under `caps`' snapshot source answered by a snapshot of no orders showing `pos` on
/// `INST`.
fn resync_under(reg: &mut Registry, caps: &OrderCaps, at: u64, pos: i64) -> ResyncReport {
    reg.resync(
        &LadderConfig::new(
            Duration::from_secs(1),
            Duration::ZERO,
            Duration::from_secs(10),
            1,
        )
        .unwrap(),
        caps,
        &ResyncSnapshot {
            watermark: WallNs(at as i64),
            requested_at: MonoNs(at),
            orders: vec![],
            positions: vec![(INST, SignedLots(pos))],
        },
        OrderKey {
            venue: None,
            ingest: at,
        },
    )
    .unwrap()
}

/// A liquidation of `qty` lots on `side` on `INST`, named `fid`, at 61000.0 with no fee: a fill
/// the venue made itself, on no order (decision 0120).
fn liquidation(fid: &str, side: Side, qty: i64) -> fbc_core::FillEvent {
    venue_fill(
        VenueFillKind::Liquidation,
        fid,
        None,
        side,
        qty,
        PxExact::new(6_100_000_000_000, -8),
        no_fee(),
    )
}

/// Hands `fill` to a fresh ledger at `now` and applies it, asserting it moved the inventory.
fn apply_venue_fill(reg: &mut Registry, fill: &fbc_core::FillEvent, now: u64) {
    let mut ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap();
    match ledger.admit(fill, None, MonoNs(now)) {
        Admission::Apply(a) => assert!(matches!(
            reg.apply_fill(a).unwrap(),
            FillRouted::Venue {
                kind: VenueFillKind::Liquidation,
                ..
            }
        )),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// What `report` agreed or found a desync in on `INST` (the caps' other market, which the
/// first trustworthy resync seeds as flat, is compared too).
fn on_inst(report: &ResyncReport) -> Vec<PositionCheck> {
    report
        .checks
        .iter()
        .filter(|c| {
            matches!(c, PositionCheck::Agrees { inst, .. } | PositionCheck::Desync { inst, .. }
                if *inst == INST)
        })
        .cloned()
        .collect()
}

#[test]
fn the_declaration_names_one_market_and_is_not_a_testnet_run() {
    let run = CodProbeRun::owner_supervised(INST);
    assert_eq!(run.market(), INST);
    assert_eq!(run, CodProbeRun::owner_supervised(INST));
    assert_ne!(run, CodProbeRun::owner_supervised(OTHER));
    let reg = probe();
    assert_eq!(reg.cod_probe(), Some(INST));
    assert!(!reg.testnet_run());
    assert_eq!(live().cod_probe(), None);
    assert_eq!(testnet().cod_probe(), None);
}

#[test]
fn a_flat_hand_seed_on_the_declared_market_arms_at_start_flatten_and_wind_down() {
    for (name, call, to) in arming_calls() {
        let mut reg = probe();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        assert_eq!(reg.position(INST), Some(SignedLots(0)), "{name}");
        arms(&mut reg, INST, name, call, to);
        assert_eq!(reg.position(INST), Some(SignedLots(0)), "{name}");
    }
}

#[test]
fn a_hand_seed_on_the_declared_market_that_is_not_flat_is_refused() {
    for (name, call, _) in arming_calls() {
        for pos in [1, -1, 20] {
            let mut reg = probe();
            reg.seed_position(INST, SignedLots(pos)).unwrap();
            refused(&mut reg, INST, name, call, ArmRefusal::SeededByHand(INST));
        }
    }
}

#[test]
fn a_flat_hand_seed_on_another_market_is_refused() {
    for (name, call, to) in arming_calls() {
        let mut reg = probe();
        reg.seed_position(OTHER, SignedLots(0)).unwrap();
        refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));
        // The declared market arms on its own flat seed; the other stays refused.
        reg.seed_position(INST, SignedLots(0)).unwrap();
        arms(&mut reg, INST, name, call, to);
        refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));
    }
}

#[test]
fn a_market_a_trustworthy_resync_seeded_arms_as_before_under_the_declaration() {
    for (name, call, to) in arming_calls() {
        let mut reg = probe();
        seed(&mut reg, &[(INST, 20), (OTHER, -4)]);
        arms(&mut reg, INST, name, call, to);
        arms(&mut reg, OTHER, name, call, to);
    }
}

#[test]
fn an_unknown_position_is_refused_as_before() {
    for (name, call, _) in arming_calls() {
        let mut reg = probe();
        refused(
            &mut reg,
            INST,
            name,
            call,
            ArmRefusal::PositionUnknown(INST),
        );
    }
}

#[test]
fn the_kill_switch_refuses_a_flat_probe_seed_as_before() {
    for (name, call, _) in arming_calls() {
        let mut reg = probe();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        reg.kill(INST);
        refused(&mut reg, INST, name, call, ArmRefusal::Killed(INST));
        assert_eq!(reg.entry(INST).state(), EntryState::Killed, "{name}");
    }
}

#[test]
fn missing_or_wrong_leases_refuse_a_flat_probe_seed_as_before() {
    for (name, call, _) in arming_calls() {
        // No lease names at all.
        let mut reg =
            Registry::with_caps(caps()).for_cod_probe(CodProbeRun::owner_supervised(INST));
        reg.seed_position(INST, SignedLots(0)).unwrap();
        refused_with(
            &mut reg,
            INST,
            name,
            call,
            Leases::none(),
            ArmRefusal::NotNamed(INST),
        );

        let mut reg = probe();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        // No market lease.
        refused_with(
            &mut reg,
            INST,
            name,
            call,
            Leases::none(),
            ArmRefusal::NoMarketLease(INST),
        );
        // Another market's lease.
        let other = Leases::market(market_lease(&reg, OTHER));
        refused_with(
            &mut reg,
            INST,
            name,
            call,
            other,
            ArmRefusal::WrongMarketLease(INST),
        );
        // The market's lease without the account's, which per-account nonces need.
        let alone = Leases::market(market_lease(&reg, INST));
        refused_with(
            &mut reg,
            INST,
            name,
            call,
            alone,
            ArmRefusal::NoAccountLease(INST),
        );
    }
}

#[test]
fn a_registry_without_the_declaration_refuses_a_flat_hand_seed() {
    for (name, call, _) in arming_calls() {
        let mut reg = live();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        refused(&mut reg, INST, name, call, ArmRefusal::SeededByHand(INST));
    }
}

#[test]
fn for_testnet_run_is_unchanged() {
    for (name, call, to) in arming_calls() {
        // Any hand seed, on any market, arms in a declared testnet run.
        let mut reg = testnet();
        reg.seed_position(INST, SignedLots(-3)).unwrap();
        reg.seed_position(OTHER, SignedLots(7)).unwrap();
        arms(&mut reg, INST, name, call, to);
        arms(&mut reg, OTHER, name, call, to);
        // A testnet run still never arms an unknown position.
        let mut reg = testnet();
        refused(
            &mut reg,
            INST,
            name,
            call,
            ArmRefusal::PositionUnknown(INST),
        );
    }
}

/// A named registry declared both the probe's, on `INST`, and a testnet run's, the testnet
/// declaration given first when `testnet_first`.
fn probe_and_testnet(testnet_first: bool) -> Registry {
    let reg = Registry::with_caps(caps());
    named(if testnet_first {
        reg.for_testnet_run(TestnetRun::owner_assisted())
            .for_cod_probe(CodProbeRun::owner_supervised(INST))
    } else {
        reg.for_cod_probe(CodProbeRun::owner_supervised(INST))
            .for_testnet_run(TestnetRun::owner_assisted())
    })
}

/// Decision 0140 item 6's "not a testnet run": on a registry declared a probe's, a testnet
/// declaration, given before or after, arms no hand seed the probe's narrowing refuses.
#[test]
fn a_testnet_declaration_widens_nothing_on_a_probe_registry() {
    for testnet_first in [true, false] {
        for (name, call, to) in arming_calls() {
            let mut reg = probe_and_testnet(testnet_first);
            assert!(reg.testnet_run(), "{name}");
            assert_eq!(reg.cod_probe(), Some(INST), "{name}");
            // A hand seed that is not flat, and one on another market, flat or not, are refused.
            reg.seed_position(INST, SignedLots(5)).unwrap();
            refused(&mut reg, INST, name, call, ArmRefusal::SeededByHand(INST));
            reg.seed_position(OTHER, SignedLots(-5)).unwrap();
            refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));

            // The declared market's flat seed arms; another market's flat seed stays refused.
            let mut reg = probe_and_testnet(testnet_first);
            reg.seed_position(INST, SignedLots(0)).unwrap();
            reg.seed_position(OTHER, SignedLots(0)).unwrap();
            arms(&mut reg, INST, name, call, to);
            refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));
        }
    }
}

/// A registry keeps the first probe declaration it is given, so it names one market for its
/// life and arms a hand seed on that market alone.
#[test]
fn a_second_declaration_changes_nothing() {
    for (name, call, to) in arming_calls() {
        // Given again before anything is seeded.
        let mut reg = named(
            Registry::with_caps(caps())
                .for_cod_probe(CodProbeRun::owner_supervised(INST))
                .for_cod_probe(CodProbeRun::owner_supervised(OTHER)),
        );
        assert_eq!(reg.cod_probe(), Some(INST), "{name}");
        reg.seed_position(OTHER, SignedLots(0)).unwrap();
        refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));
        reg.seed_position(INST, SignedLots(0)).unwrap();
        arms(&mut reg, INST, name, call, to);

        // Given again once the declared market is armed: the other market is still refused.
        let mut reg = probe();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        arms(&mut reg, INST, name, call, to);
        let mut reg = std::mem::take(&mut reg).for_cod_probe(CodProbeRun::owner_supervised(OTHER));
        assert_eq!(reg.cod_probe(), Some(INST), "{name}");
        assert!(reg.entry(INST).armed(), "{name}");
        reg.seed_position(OTHER, SignedLots(0)).unwrap();
        refused(&mut reg, OTHER, name, call, ArmRefusal::SeededByHand(OTHER));
    }
}

/// "Flat" is the hand seed as it was seeded, not the inventory now: a fill since moves the
/// inventory and leaves the seed's origin as it was (decision 0067).
#[test]
fn the_seed_is_judged_as_it_was_seeded_whatever_the_fills_moved_it_to() {
    for (name, call, to) in arming_calls() {
        // Seeded flat, then a liquidation of one lot: still armable.
        let mut reg = probe();
        reg.seed_position(INST, SignedLots(0)).unwrap();
        apply_venue_fill(&mut reg, &liquidation("liq-1", Side::Buy, 1), 10);
        assert_eq!(reg.position(INST), Some(SignedLots(1)), "{name}");
        arms(&mut reg, INST, name, call, to);

        // Seeded at one lot, then a liquidation back to flat: still refused.
        let mut reg = probe();
        reg.seed_position(INST, SignedLots(1)).unwrap();
        apply_venue_fill(&mut reg, &liquidation("liq-2", Side::Sell, 1), 10);
        assert_eq!(reg.position(INST), Some(SignedLots(0)), "{name}");
        refused(&mut reg, INST, name, call, ArmRefusal::SeededByHand(INST));
    }
}

/// Under the probe codec's untrustworthy, separately read snapshot (decision 0140 item 7) a
/// resync seeds nothing and still compares the flat probe seed, reporting a desync; the
/// inventory is unchanged.
#[test]
fn an_untrustworthy_resync_compares_a_flat_probe_seed_and_reports_a_desync() {
    let mut reg = probe();
    reg.seed_position(INST, SignedLots(0)).unwrap();
    arms(
        &mut reg,
        INST,
        "start",
        Registry::start,
        EntryState::Quoting,
    );
    let report = resync_under(&mut reg, &probe_caps(), 1_000, 0);
    assert!(report.untrustworthy);
    assert!(report.seeded.is_empty());
    assert_eq!(
        on_inst(&report),
        vec![PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(0)
        }]
    );
    let report = resync_under(&mut reg, &probe_caps(), 2_000, 2);
    assert!(report.untrustworthy);
    assert!(report.seeded.is_empty());
    assert_eq!(
        on_inst(&report),
        vec![PositionCheck::Desync {
            inst: INST,
            venue: SignedLots(2),
            ledger: Some(SignedLots(0))
        }]
    );
    assert_eq!(reg.position(INST), Some(SignedLots(0)));
    assert_eq!(reg.inventory(INST), SignedLots(0));
    // `OTHER`, which nothing seeded, stays unknown.
    assert_eq!(reg.position(OTHER), None);
    assert!(reg.entry(INST).armed());
}

#[test]
fn a_later_resync_compares_a_flat_probe_seed_and_reports_a_desync() {
    let mut reg = probe();
    reg.seed_position(INST, SignedLots(0)).unwrap();
    arms(
        &mut reg,
        INST,
        "start",
        Registry::start,
        EntryState::Quoting,
    );
    // A venue showing flat agrees and does not seed the market again (a market it lists as
    // flat that nothing seeded, `OTHER`, it seeds).
    let report = resync(&mut reg, 1_000, 0);
    assert!(report.seeded.iter().all(|&(m, _)| m != INST));
    assert_eq!(
        on_inst(&report),
        vec![PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(0)
        }]
    );
    // A venue showing 2 lots is a desync, reported; the inventory is unchanged.
    let report = resync(&mut reg, 2_000, 2);
    assert!(report.seeded.iter().all(|&(m, _)| m != INST));
    assert_eq!(
        on_inst(&report),
        vec![PositionCheck::Desync {
            inst: INST,
            venue: SignedLots(2),
            ledger: Some(SignedLots(0))
        }]
    );
    assert_eq!(reg.position(INST), Some(SignedLots(0)));
    assert_eq!(reg.inventory(INST), SignedLots(0));
    // The seed's origin is fixed for the process (decision 0067): still the flat hand seed, so
    // the market arms again once disarmed, and a non-flat hand seed stays refused after a resync
    // that agrees with it.
    reg.disarm(INST);
    arms(
        &mut reg,
        INST,
        "start",
        Registry::start,
        EntryState::Quoting,
    );
    let mut reg = probe();
    reg.seed_position(INST, SignedLots(3)).unwrap();
    let report = resync(&mut reg, 1_000, 3);
    assert!(report.seeded.iter().all(|&(m, _)| m != INST));
    refused(
        &mut reg,
        INST,
        "start",
        Registry::start,
        ArmRefusal::SeededByHand(INST),
    );
}

#[test]
fn the_refusal_names_the_probe_run_as_a_way_out() {
    let text = ArmRefusal::SeededByHand(INST).to_string();
    assert!(text.contains("by hand"), "{text}");
    assert!(text.contains("testnet"), "{text}");
    assert!(text.contains("flat seed"), "{text}");
    assert!(text.contains("probe run"), "{text}");
}
